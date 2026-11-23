//! Source-produced admission, committed by the existing unified SQL owner.
//! Wire commands alone never confer census provenance or permission to send.
use crate::admission_failure::{AdmissionFailure, AdmissionStage};
use crate::native_finance_risk::NativeFinanceRisk;
use okx_trade_execution::account_worker::{
    AdmissionIntentId, AdmissionReservationKey, AdmissionRunId, CensusObservedAtMs,
    CensusReadFloorMs, NativePostDecision, NativePostRequest, NativeReserveDecision,
    NativeReserveRequest, ReportedRiskCensus,
};
use okx_trade_execution::risk::{PostAccountRead, PostCausalProof};
use okx_trade_execution::{CentralReceipt, CommittedVenueSend};
use okx_ipc_contracts::risk_authority::*;
use sha2::{Digest, Sha256};

// A denial is not a lost reply, and a grant always includes its actual durable proof.
#[expect(
    clippy::large_enum_variant,
    reason = "Retain the existing bounded result and receipt inline without allocating on every admitted order."
)]
pub(crate) enum ReserveAdmission {
    Held {
        result: RiskResult,
        receipt: CentralReceipt,
    },
    Denied {
        reason: String,
    },
}
pub(crate) enum PostAdmission {
    Authorized {
        result: RiskResult,
        claim: CommittedVenueSend,
    },
    Denied {
        result: RiskResult,
    },
}

pub(crate) fn reported(census: &NativeRiskCensus) -> Result<ReportedRiskCensus, &'static str> {
    if !census.valid() {
        return Err("NATIVE_ADMISSION_CENSUS_INVALID");
    }
    Ok(ReportedRiskCensus {
        source_digest: census.source_digest.clone(),
        generated_at_ms: CensusObservedAtMs::new(
            i64::try_from(census.generated_at_ms).map_err(|_| "NATIVE_ADMISSION_TIME_INVALID")?,
        )
        .map_err(|_| "NATIVE_ADMISSION_CENSUS_INVALID")?,
        read_floor_ms: CensusReadFloorMs::new(
            i64::try_from(census.source_read_floor_ms)
                .map_err(|_| "NATIVE_ADMISSION_TIME_INVALID")?,
        )
        .map_err(|_| "NATIVE_ADMISSION_CENSUS_INVALID")?,
        occupied: census
            .occupied
            .iter()
            .map(|a| (a.account_key.clone(), a.occupied_usdt.clone()))
            .collect(),
        cumulative_loss_usd: census
            .occupied
            .iter()
            .map(|a| (a.account_key.clone(), a.cumulative_loss_usd.clone()))
            .collect(),
    })
}

fn result(command: &RiskCommand, status: RiskDecisionStatus) -> RiskResult {
    RiskResult {
        schema: RISK_RESULT_SCHEMA_V1.into(),
        command_id: command.command_id.clone(),
        status,
        reason: None,
        reservation_key: None,
        risk_book_sequence: None,
        policy_revision: None,
        existing_modelled_loss_usdt: None,
        reservation_state: None,
        budget: None,
        account_budget: None,
    }
}

fn checked(result: RiskResult) -> Result<RiskResult, &'static str> {
    if result.valid() {
        Ok(result)
    } else {
        Err("NATIVE_ADMISSION_RESULT_INVALID")
    }
}

impl NativeFinanceRisk {
    pub(crate) async fn reserve_produced(
        &self,
        packet: crate::trade_service::ProducedReserve,
    ) -> Result<ReserveAdmission, AdmissionFailure> {
        let input = |code| AdmissionFailure::local(AdmissionStage::ReserveInput, code);
        let output = |code| AdmissionFailure::local(AdmissionStage::ReserveResult, code);
        let (command, native_incarnation, native_loss_usd) = packet.into_parts();
        if !command.valid() || command.installation_id != self.installation {
            return Err(input("NATIVE_RESERVE_PRODUCER_INVALID"));
        }
        let RiskCommandAction::Reserve {
            venue,
            account_uid,
            run_id,
            intent_id,
            strategy_id,
            instrument_id,
            plan_sha256,
            requested_loss_usdt,
            requested_loss_usd: _,
            entry_upper_usd,
            entry_proof_digest,
            usd_conversion,
            policy_revision,
            census,
        } = &command.action
        else {
            return Err(input("NATIVE_RESERVE_COMMAND_REQUIRED"));
        };
        let request = NativeReserveRequest {
            venue: venue.clone(),
            uid: account_uid.clone(),
            run_id: AdmissionRunId::new(run_id.clone())
                .map_err(|_| input("NATIVE_ADMISSION_IDENTITY_REQUIRED"))?,
            intent_id: AdmissionIntentId::new(intent_id.clone())
                .map_err(|_| input("NATIVE_ADMISSION_IDENTITY_REQUIRED"))?,
            strategy_id: strategy_id.clone(),
            instrument_id: instrument_id.clone(),
            plan_sha256: plan_sha256.clone(),
            entry_proof_digest: entry_proof_digest
                .clone()
                .ok_or_else(|| input("NATIVE_RESERVE_ENTRY_PROOF_REQUIRED"))?,
            conversion_digest: format!(
                "{:x}",
                Sha256::digest(
                    serde_json::to_vec(usd_conversion)
                        .map_err(|_| input("NATIVE_RESERVE_CONVERSION_INVALID"))?
                )
            ),
            conversion_rate: usd_conversion.usd_per_usdt.clone(),
            conversion_observed_at_ms: i64::try_from(usd_conversion.observed_at_ms)
                .map_err(|_| input("NATIVE_ADMISSION_TIME_INVALID"))?,
            loss_usdt: okx_trade_execution::risk::UsdtLoss::parse(requested_loss_usdt)
                .map_err(|_| input("NATIVE_ADMISSION_MONEY_INVALID"))?,
            loss_usd: native_loss_usd,
            entry_upper_usd: okx_trade_execution::risk::UsdEntryUpper::parse(
                entry_upper_usd
                    .as_deref()
                    .ok_or_else(|| input("NATIVE_RESERVE_ENTRY_BOUND_REQUIRED"))?,
            )
            .map_err(|_| input("NATIVE_ADMISSION_MONEY_INVALID"))?,
            native_incarnation,
            policy_revision: *policy_revision,
        };
        let decision = self
            .owner
            .reserve_reported(
                self.campaign.clone(),
                request,
                reported(census).map_err(input)?,
            )
            .await
            .map_err(|e| AdmissionFailure::store(AdmissionStage::ReserveOwner, e))?;
        match decision {
            NativeReserveDecision::Held {
                receipt,
                book_sequence,
                policy_revision,
            } => {
                let mut held = result(&command, RiskDecisionStatus::Held);
                held.reservation_key = Some(receipt.reservation_key.clone());
                held.risk_book_sequence = Some(book_sequence);
                held.policy_revision = Some(policy_revision);
                Ok(ReserveAdmission::Held {
                    result: checked(held).map_err(output)?,
                    receipt,
                })
            }
            NativeReserveDecision::Denied { reason } => {
                let mut denied = result(&command, RiskDecisionStatus::Denied);
                denied.reason = Some(reason);
                Ok(ReserveAdmission::Denied {
                    reason: checked(denied)
                        .map_err(output)?
                        .reason
                        .ok_or_else(|| output("NATIVE_RESERVE_DENIAL_REASON_MISSING"))?,
                })
            }
        }
    }

    pub(crate) async fn post_produced(
        &self,
        packet: crate::trade_service::ProducedPost,
    ) -> Result<PostAdmission, AdmissionFailure> {
        let input = |code| AdmissionFailure::local(AdmissionStage::PostInput, code);
        let output = |code| AdmissionFailure::local(AdmissionStage::PostResult, code);
        let command = packet.into_parts();
        if !command.valid() || command.installation_id != self.installation {
            return Err(input("NATIVE_POST_PRODUCER_INVALID"));
        }
        let RiskCommandAction::BeginPost {
            reservation_key,
            plan_sha256,
            policy_revision,
            expected_risk_book_sequence,
            source_proof_sha256,
            census,
            causal_proof,
        } = &command.action
        else {
            return Err(input("NATIVE_POST_COMMAND_REQUIRED"));
        };
        let request = NativePostRequest {
            reservation_key: AdmissionReservationKey::new(reservation_key.clone())
                .map_err(|_| input("NATIVE_ADMISSION_IDENTITY_REQUIRED"))?,
            plan_sha256: plan_sha256.clone(),
            source_proof_digest: source_proof_sha256.clone(),
            policy_revision: *policy_revision,
            expected_sequence: *expected_risk_book_sequence,
            census: reported(census).map_err(input)?,
            causal: PostCausalProof {
                reserve_anchor: causal_proof.reserve_anchor.clone(),
                native_incarnation: causal_proof.native_incarnation.clone(),
                elapsed_to_proof_ms: causal_proof.elapsed_to_proof_ms,
                account_reads: causal_proof
                    .account_reads
                    .iter()
                    .map(|a| PostAccountRead {
                        account_key: a.account_key.clone(),
                        private_generation: a.private_generation,
                        source_read_id: a.source_read_id.clone(),
                        content_digest: a.content_digest.clone(),
                        started_after_held_ms: a.started_after_held_ms,
                        completed_after_held_ms: a.completed_after_held_ms,
                    })
                    .collect(),
            },
        };
        let decision = self
            .owner
            .begin_post_reported(self.campaign.clone(), request)
            .await
            .map_err(|e| AdmissionFailure::store(AdmissionStage::PostOwner, e))?;
        match decision {
            NativePostDecision::Authorized {
                claim,
                book_sequence,
                existing_modelled_loss_usdt,
            } => {
                let mut authorized = result(&command, RiskDecisionStatus::PostAuthorized);
                authorized.reservation_key = Some(reservation_key.clone());
                authorized.risk_book_sequence = Some(book_sequence);
                authorized.policy_revision = Some(*policy_revision);
                authorized.existing_modelled_loss_usdt = Some(existing_modelled_loss_usdt);
                Ok(PostAdmission::Authorized {
                    result: checked(authorized).map_err(output)?,
                    claim,
                })
            }
            NativePostDecision::Denied { reason } => {
                let mut denied = result(&command, RiskDecisionStatus::Denied);
                denied.reason = Some(reason);
                Ok(PostAdmission::Denied {
                    result: checked(denied).map_err(output)?,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn census() -> NativeRiskCensus {
        let account = format!("okx:real:{}", "a".repeat(64));
        NativeRiskCensus {
            account_keys: vec![account.clone()],
            occupied: vec![RiskAccountOccupancy {
                account_key: account,
                occupied_usdt: "1.230000000".into(),
                cumulative_loss_usd: "0.120000000".into(),
            }],
            generated_at_ms: 10,
            source_read_floor_ms: 9,
            source_digest: "b".repeat(64),
            content_digest: "c".repeat(64),
            complete: true,
            native_account_evidence_complete: true,
            native_order_evidence_complete: true,
            native_ledger_evidence_complete: true,
        }
    }

    #[test]
    fn reported_census_retains_exact_occupancy_loss_and_source_times() {
        let wire = census();
        let mapped = reported(&wire).unwrap();
        assert_eq!(mapped.source_digest, wire.source_digest);
        assert_eq!(mapped.generated_at_ms.raw_millis(), 10);
        assert_eq!(mapped.read_floor_ms.raw_millis(), 9);
        assert_eq!(mapped.occupied[&wire.account_keys[0]], "1.230000000");
        assert_eq!(
            mapped.cumulative_loss_usd[&wire.account_keys[0]],
            "0.120000000"
        );
    }

    #[test]
    fn reported_census_rejects_missing_evidence_and_unrepresentable_time() {
        let mut wire = census();
        wire.native_ledger_evidence_complete = false;
        assert_eq!(
            reported(&wire).err(),
            Some("NATIVE_ADMISSION_CENSUS_INVALID")
        );
        wire.native_ledger_evidence_complete = true;
        wire.generated_at_ms = u64::MAX;
        assert_eq!(reported(&wire).err(), Some("NATIVE_ADMISSION_TIME_INVALID"));
    }
}

//! Terminal reservation settlement and release for a reconciled intent.
use super::reconcile::{verified_order_readback, verified_terminal_order_update_time};
use super::{AccountResources, StoredIntent, TradeService, deadline, digest, err, exact};
use okx_execution::{AccountStoreHandle, CentralReceipt, IntentState, SourceCursor};
use okx_protocol::risk_authority::{
    RiskCommandAction, RiskDecisionStatus, RiskReservationState, RiskResolution,
    TerminalRiskLedgerProof,
};
use rust_decimal::Decimal;
use serde::Serialize;

/// The reached owner boundary, captured at execution time rather than inferred
/// from error strings. These values describe a failure, never grant release.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum TerminalReleaseStage {
    DurableSnapshot,
    Authority,
    Reservation,
    Exposure,
    FillCoverage,
    VenueReadback,
    BillCoverage,
    LocalAcknowledgement,
    Inspection,
    LateSettlement,
    SettlementProof,
    CentralSettlement,
    Census,
    Release,
    SqlAcknowledgement,
}

#[derive(Debug, Serialize)]
pub(super) struct TerminalReleaseError {
    pub stage: TerminalReleaseStage,
    pub reason_code: String,
    pub account: String,
    pub intent_id: String,
    pub intent_revision: i64,
    pub reservation_key: Option<String>,
}

/// Read the actual account owner. An absent grant is not a proof of capacity
/// for an opening intent, but remains inapplicable for a reducing operation.
async fn terminal_receipt(
    store: &AccountStoreHandle,
    intent_id: &str,
    risk_increase: bool,
) -> Result<Option<CentralReceipt>, String> {
    let id = intent_id.to_owned();
    let receipt = store
        .call(deadline(), move |s| s.central_receipt(&id))
        .await
        .map_err(err)?;
    if receipt.is_none() && risk_increase {
        return Err("TERMINAL_CENTRAL_RESERVATION_UNVERIFIED".into());
    }
    Ok(receipt)
}

fn source_covers_terminal(source: &SourceCursor, venue_updated_at: i64) -> bool {
    source.generation > 0
        && source.cursor.is_empty()
        && source.coverage_gaps.is_empty()
        && source.watermark >= venue_updated_at
}

#[cfg(test)]
mod tests;

impl TradeService {
    pub(super) async fn release_terminal(
        &self,
        r: &AccountResources,
        i: &StoredIntent,
        order: Option<&okx_api::TradeOrderDetails>,
        venue_flat_verified: bool,
        invocation: Option<crate::diagnostics::ProtectionCleanupInvocation<'_>>,
    ) -> Result<(), TerminalReleaseError> {
        let mut stage = TerminalReleaseStage::Authority;
        let mut reservation_key = None;
        let attempt = r.terminal_release_trace.begin();
        let mut facts = super::terminal_release_trace::ReleaseFacts::default();
        let observe = |stage,
                       reservation: Option<&str>,
                       outcome,
                       reason: Option<&str>,
                       facts: &super::terminal_release_trace::ReleaseFacts| {
            r.terminal_release_trace.observe_facts(
                &i.intent_id,
                i.revision,
                attempt,
                stage,
                reservation,
                outcome,
                reason,
                invocation,
                facts,
            );
        };
        observe(stage, None, "running", None, &facts);
        let result: Result<(), String> = async {
            if r.configuration.account_key.environment != okx_protocol::accounts::Environment::Real
            {
                return Ok(());
            }
            let state = self
                .governance
                .read()
                .await
                .clone()
                .ok_or("AUTHORITY_UNAVAILABLE")?;
            stage = TerminalReleaseStage::Reservation;
            observe(stage, reservation_key.as_deref(), "running", None, &facts);
            let Some(receipt) =
                terminal_receipt(&r.execution.store, &i.intent_id, i.risk_increase).await?
            else {
                return Ok(());
            };
            reservation_key = Some(receipt.reservation_key.clone());
            let local_reduction = receipt.reservation_key.starts_with("local:");
            stage = TerminalReleaseStage::Exposure;
            observe(stage, reservation_key.as_deref(), "running", None, &facts);
            let instrument = i.instrument_id.clone();
            let lots = r
                .execution
                .store
                .call(deadline(), move |s| s.active_lots(&instrument))
                .await
                .map_err(err)?;
            let remaining_exposure = lots.iter().any(|l| {
                (l.intent_id.as_deref() == Some(&i.intent_id) || local_reduction)
                    && l.remaining_qty > Decimal::ZERO
            });
            facts.remaining_exposure = Some(remaining_exposure);
            if remaining_exposure {
                return Err("INTENT_EXPOSURE_REMAINS".into());
            }
            stage = TerminalReleaseStage::FillCoverage;
            observe(stage, reservation_key.as_deref(), "running", None, &facts);
            let partition = if i.instrument_id.ends_with("-SWAP") {
                "SWAP"
            } else {
                "FUTURES"
            };
            let complete = r
                .execution
                .store
                .call(deadline(), move |s| s.cursor("fills", partition))
                .await
                .map_err(err)?
                .ok_or("FILL_COVERAGE_MISSING")?;
            facts.fills = Some((&complete).into());
            stage = TerminalReleaseStage::VenueReadback;
            observe(stage, reservation_key.as_deref(), "running", None, &facts);
            let venue_updated_at = verified_terminal_order_update_time(i, order)?;
            facts.terminal_order_updated_at_ms = Some(venue_updated_at);
            stage = TerminalReleaseStage::FillCoverage;
            observe(stage, reservation_key.as_deref(), "running", None, &facts);
            if !source_covers_terminal(&complete, venue_updated_at) {
                return Err("FILL_COVERAGE_INCOMPLETE".into());
            }
            if local_reduction {
                stage = TerminalReleaseStage::BillCoverage;
                observe(stage, reservation_key.as_deref(), "running", None, &facts);
                let bills = r
                    .execution
                    .store
                    .call(deadline(), |s| s.cursor("bills", "account"))
                    .await
                    .map_err(err)?
                    .ok_or("BILLS_COVERAGE_MISSING")?;
                facts.bills = Some((&bills).into());
                if !source_covers_terminal(&bills, venue_updated_at) {
                    return Err("FINANCIAL_COVERAGE_INCOMPLETE".into());
                }
                // Reduce-only has NO central RiskBook grant. Its SQL receipt must
                // nevertheless be completed after its own terminal venue/fill/bill
                // readback. Leaving it TERMINAL_PENDING_CENTRAL causes the next
                // full cohort census to block the opening intent's legitimate
                // central RELEASE forever.
                stage = TerminalReleaseStage::VenueReadback;
                observe(stage, reservation_key.as_deref(), "running", None, &facts);
                if i.risk_increase || !venue_flat_verified {
                    return Err("LOCAL_CLOSE_FLAT_POSITION_UNPROVEN".into());
                }
                let venue = order.ok_or("LOCAL_CLOSE_ORDER_READBACK_MISSING")?;
                if !matches!(venue.state.as_str(), "filled" | "canceled" | "mmp_canceled") {
                    return Err("LOCAL_CLOSE_VENUE_NOT_TERMINAL".into());
                }
                let filled = verified_order_readback(i, venue)?;
                if filled <= Decimal::ZERO || filled != i.plan.quantity {
                    return Err("LOCAL_CLOSE_FILL_QUANTITY_UNPROVEN".into());
                }
                stage = TerminalReleaseStage::LocalAcknowledgement;
                observe(stage, reservation_key.as_deref(), "running", None, &facts);
                let id = i.intent_id.clone();
                let order_id = venue.order_id.clone();
                r.execution
                    .store
                    .call(deadline(), move |s| {
                        s.acknowledge_local_terminal_close(&id, &order_id, filled, venue_updated_at)
                    })
                    .await
                    .map_err(err)?;
                return Ok(());
            }
            stage = TerminalReleaseStage::Inspection;
            observe(stage, reservation_key.as_deref(), "running", None, &facts);
            let observed = self
                .risk
                .request(self.risk_command(
                    &state,
                    format!("inspect_{}", &i.client_order_id),
                    RiskCommandAction::Inspect {
                        reservation_key: receipt.reservation_key.clone(),
                        plan_sha256: i.plan_hash.clone(),
                    },
                ))
                .await
                .map_err(str::to_owned)?;
            facts.worker_policy_revision = observed.policy_revision;
            facts.worker_sequence = observed.risk_book_sequence;
            facts.worker_released = observed
                .reservation_state
                .map(|v| v == RiskReservationState::Released);
            // INSPECT describes the immutable reservation's policy, not the
            // installation's latest policy. Rotation must preserve old replay.
            if observed.status != RiskDecisionStatus::Observed {
                return Err(observed
                    .reason
                    .unwrap_or("TERMINAL_INSPECTION_NOT_OBSERVED".into()));
            }
            if observed.reservation_key.as_deref() != Some(receipt.reservation_key.as_str())
                || u64::try_from(receipt.policy_revision).ok() != observed.policy_revision
                || observed.policy_revision.is_none()
                || observed.risk_book_sequence.is_none()
            {
                return Err("TERMINAL_INSPECTION_IDENTITY_MISMATCH".into());
            }
            // A response already in flight when the authenticated session
            // changes cannot acknowledge local money in the replacement session.
            if self.governance.read().await.as_ref().is_none_or(|latest| {
                latest.session_generation != state.session_generation
                    || latest.state.installation_id != state.state.installation_id
            }) {
                return Err("TERMINAL_INSPECTION_SESSION_CHANGED".into());
            }
            if observed.reservation_state == Some(RiskReservationState::Released) {
                // A late bill may already be captured but not yet acknowledged
                // by central SETTLE_ENTRY. Never overwrite that pending marker
                // with the original terminal release acknowledgement.
                stage = TerminalReleaseStage::LateSettlement;
                observe(stage, reservation_key.as_deref(), "running", None, &facts);
                let id = i.intent_id.clone();
                if r.execution
                    .store
                    .call(deadline(), move |s| s.entry_late_pending(&id))
                    .await
                    .map_err(err)?
                {
                    return Err("LATE_ENTRY_SETTLEMENT_PENDING".into());
                }
                stage = TerminalReleaseStage::SqlAcknowledgement;
                observe(stage, reservation_key.as_deref(), "running", None, &facts);
                let k = receipt.reservation_key.clone();
                let e = digest(&observed)?;
                r.execution
                    .store
                    .call(deadline(), move |s| s.acknowledge_central_release(&k, &e))
                    .await
                    .map_err(err)?;
                return Ok(());
            }
            stage = TerminalReleaseStage::BillCoverage;
            observe(stage, reservation_key.as_deref(), "running", None, &facts);
            let (through, loss) = r
                .execution
                .store
                .call(deadline(), |s| {
                    Ok((
                        s.cursor("bills", "account")?
                            .ok_or(okx_execution::StoreError::Invalid("BILLS_COVERAGE_MISSING"))?,
                        s.policy()?.4,
                    ))
                })
                .await
                .map_err(err)?;
            facts.bills = Some((&through).into());
            if !source_covers_terminal(&through, venue_updated_at) {
                return Err("FINANCIAL_COVERAGE_INCOMPLETE".into());
            }
            // The account-wide fills and bills cursors can advance independently
            // because their native SQL page ingests are separate atomic writes.
            // A newer bill watermark is NOT itself proof that fills have advanced
            // equally far. Use their common proven frontier, exactly as the
            // incremental SETTLE_ENTRY path already does, without downgrading
            // either source's completeness or the venue uTime requirement.
            let financial_through = complete.watermark.min(through.watermark);
            if financial_through < venue_updated_at {
                return Err("FINANCIAL_COVERAGE_INCOMPLETE".into());
            }
            // A terminal POST may free entry capacity only after the independent
            // venue order and all immutable native fill/fee bills agree. The
            // SQL owner commits the exact final cumulative proof BEFORE any WSS
            // SETTLE_ENTRY. A lost central reply reuses that same receipt.
            stage = TerminalReleaseStage::VenueReadback;
            observe(stage, reservation_key.as_deref(), "running", None, &facts);
            let venue_order = order.ok_or("TERMINAL_ENTRY_VENUE_ORDER_UNAVAILABLE")?;
            if !matches!(
                venue_order.state.as_str(),
                "filled" | "canceled" | "mmp_canceled"
            ) {
                return Err("TERMINAL_ENTRY_VENUE_STATE_UNVERIFIED".into());
            }
            let filled = verified_order_readback(i, venue_order)?;
            // A truly canceled zero-fill order uses the same terminal USD
            // settlement/release path but requires separate SQL no-money proof.
            // Any missing stream, unmatched bill, or existing partial fill denies.
            if filled == Decimal::ZERO && i.state != IntentState::Cancelled {
                return Err("TERMINAL_ZERO_FILL_STATE_UNVERIFIED".into());
            }
            stage = TerminalReleaseStage::SettlementProof;
            observe(stage, reservation_key.as_deref(), "running", None, &facts);
            let id = i.intent_id.clone();
            let settlement = r
                .execution
                .store
                .call(deadline(), move |s| {
                    s.bind_terminal_entry_settlement(
                        &id,
                        venue_updated_at,
                        filled,
                        financial_through,
                    )
                })
                .await
                .map_err(err)?;
            if settlement.reservation_key != receipt.reservation_key
                || settlement.plan_hash != i.plan_hash
                || settlement.venue_order_id != venue_order.order_id
            {
                return Err("TERMINAL_ENTRY_SETTLEMENT_IDENTITY_MISMATCH".into());
            }
            stage = TerminalReleaseStage::CentralSettlement;
            observe(stage, reservation_key.as_deref(), "running", None, &facts);
            let settled = self
                .risk
                .request(self.risk_command(
                    &state,
                    format!("settle_{}", i.client_order_id),
                    RiskCommandAction::SettleEntry {
                        reservation_key: settlement.reservation_key.clone(),
                        plan_sha256: settlement.plan_hash.clone(),
                        expected_risk_book_sequence:
                            observed.risk_book_sequence.ok_or("RISK_SEQUENCE_MISSING")?,
                        cumulative_entry_usd: exact(settlement.cumulative_entry_usd),
                        settlement_revision: settlement.settlement_revision,
                        source_digest: settlement.source_digest.clone(),
                    },
                ))
                .await
                .map_err(str::to_owned)?;
            if settled.status != RiskDecisionStatus::Updated {
                return Err(settled
                    .reason
                    .unwrap_or("CENTRAL_ENTRY_SETTLEMENT_PENDING".into()));
            }
            let settlement_sequence = settled
                .risk_book_sequence
                .ok_or("ENTRY_SETTLEMENT_SEQUENCE_MISSING")?;
            // The central SETTLE_ENTRY is now confirmed, but SQL must retain
            // the terminal intent's pending reservation until this RELEASE is
            // durably acknowledged. Construct a fresh full-cohort census with
            // exactly this settled reservation scoped for terminal resolution.
            // Every other terminal-pending / unknown intent still denies release.
            stage = TerminalReleaseStage::Census;
            observe(stage, reservation_key.as_deref(), "running", None, &facts);
            let account_key = r.configuration.account_key.wire();
            let census = self
                .census_for_terminal_release(
                    &state,
                    &account_key,
                    &i.intent_id,
                    &receipt.reservation_key,
                    &i.plan_hash,
                )
                .await?;
            stage = TerminalReleaseStage::Release;
            observe(stage, reservation_key.as_deref(), "running", None, &facts);
            let proof = TerminalRiskLedgerProof {
                account_uid: r.configuration.account_key.verified_uid.clone(),
                run_id: "continuous_engine".into(),
                intent_id: i.intent_id.clone(),
                state: match i.state {
                    IntentState::Rejected => "REJECTED",
                    IntentState::Cancelled => "CANCELED",
                    _ => "FILLED",
                }
                .into(),
                record_digest: digest(&(i, order, loss, through))?,
                cumulative_loss_usd: exact(loss),
                remaining_exposure_usdt: "0".into(),
                // The previous SETTLE_ENTRY committed the entire observed
                // amount. Terminal RELEASE must not create another revision or
                // claim a new debit; Worker compares these exact immutable
                // SQL-backed fields to the existing settled RiskBook entry.
                cumulative_entry_usd: Some(exact(settlement.cumulative_entry_usd)),
                entry_settlement_revision: Some(settlement.settlement_revision),
                entry_settlement_digest: Some(settlement.source_digest.clone()),
            };
            let command = self.risk_command(
                &state,
                format!("release_{}", &i.client_order_id),
                RiskCommandAction::Resolve {
                    reservation_key: receipt.reservation_key.clone(),
                    plan_sha256: i.plan_hash.clone(),
                    expected_risk_book_sequence: settlement_sequence,
                    next_state: RiskResolution::Released,
                    source_proof_sha256: digest(&proof)?,
                    terminal_census: Some(census),
                    terminal_ledger: Some(proof),
                },
            );
            let reservation = receipt.reservation_key.clone();
            let result = self.risk.request(command).await.map_err(str::to_owned)?;
            if result.status != RiskDecisionStatus::Updated {
                return Err(result.reason.unwrap_or("CENTRAL_RELEASE_DENIED".into()));
            }
            stage = TerminalReleaseStage::SqlAcknowledgement;
            observe(stage, reservation_key.as_deref(), "running", None, &facts);
            let e = digest(&result)?;
            r.execution
                .store
                .call(deadline(), move |s| {
                    s.acknowledge_central_release(&reservation, &e)
                })
                .await
                .map_err(err)?;
            Ok(())
        }
        .await;
        observe(
            stage,
            reservation_key.as_deref(),
            if result.is_ok() {
                "returned_ok"
            } else {
                "blocked"
            },
            result.as_ref().err().map(String::as_str),
            &facts,
        );
        result.map_err(|reason_code| TerminalReleaseError {
            stage,
            reason_code,
            account: r.configuration.account_key.wire(),
            intent_id: i.intent_id.clone(),
            intent_revision: i.revision,
            reservation_key,
        })
    }
}

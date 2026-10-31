//! One bounded native requester over the existing authenticated WS ownership chain.
//! Timeout is UNKNOWN. Never retry a risk mutation or an exchange POST.
use okx_protocol::risk_authority::{
    RiskCommand, RiskCommandAction, RiskDecisionStatus, RiskResult,
};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

pub struct OutboundRiskCommand {
    pub command: RiskCommand,
    pub return_to: oneshot::Sender<RiskResult>,
}
#[derive(Clone)]
pub struct NativeRiskClient {
    tx: mpsc::Sender<OutboundRiskCommand>,
}
impl NativeRiskClient {
    pub fn new(tx: mpsc::Sender<OutboundRiskCommand>) -> Self {
        Self { tx }
    }
    pub async fn request(&self, command: RiskCommand) -> Result<RiskResult, &'static str> {
        if !command.valid() {
            return Err("INVALID_RISK_COMMAND");
        }
        let id = command.command_id.clone();
        let (reply, rx) = oneshot::channel();
        self.tx
            .try_send(OutboundRiskCommand {
                command,
                return_to: reply,
            })
            .map_err(|_| "RISK_TRANSPORT_UNAVAILABLE")?;
        let response = tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| "RISK_OUTCOME_UNKNOWN_DO_NOT_REPLAY")?
            .map_err(|_| "RISK_TRANSPORT_LOST_DO_NOT_REPLAY")?;
        if !response.valid() || response.command_id != id {
            return Err("RISK_RESPONSE_IDENTITY_MISMATCH");
        }
        Ok(response)
    }
    /// Two-phase native admission using the same authenticated WS owner.
    /// The supplier MUST make a fresh source read after receiving HELD;
    /// it may not reuse the reserve-time census.
    ///
    /// Returns an authorization receipt, not a venue order acknowledgement.
    /// The executor must durably enter its single-POST mutation state and
    /// must never retry on UNKNOWN after receiving this receipt.
    pub async fn reserve_then_fence<F, Fut>(
        &self,
        reserve: RiskCommand,
        fresh_post_command: F,
    ) -> Result<RiskResult, &'static str>
    where
        F: FnOnce(&RiskResult) -> Fut,
        Fut: std::future::Future<Output = Result<RiskCommand, &'static str>>,
    {
        let (plan, revision, reserve_at_ms, reserve_digest, reserve_read_floor) =
            match &reserve.action {
                RiskCommandAction::Reserve {
                    plan_sha256,
                    policy_revision,
                    census,
                    ..
                } => (
                    plan_sha256.clone(),
                    *policy_revision,
                    census.generated_at_ms,
                    census.source_digest.clone(),
                    census.source_read_floor_ms,
                ),
                _ => return Err("RISK_RESERVE_COMMAND_REQUIRED"),
            };
        let held = self.request(reserve).await?;
        if held.status != RiskDecisionStatus::Held
            || held.policy_revision != Some(revision)
            || held.reservation_key.as_deref().is_none_or(str::is_empty)
        {
            return Err("RISK_RESERVATION_NOT_HELD");
        }
        let post = fresh_post_command(&held).await?;
        let correct = matches!(
            &post.action,
            RiskCommandAction::BeginPost {
                reservation_key,
                plan_sha256,
                policy_revision,
                expected_risk_book_sequence,
                census,
                ..
            } if Some(reservation_key.as_str()) == held.reservation_key.as_deref()
                && plan_sha256 == &plan
                && *policy_revision == revision
                && Some(*expected_risk_book_sequence) == held.risk_book_sequence
                && census.generated_at_ms > reserve_at_ms
                && census.source_read_floor_ms > reserve_read_floor
                && census.source_digest != reserve_digest
        );
        if !correct {
            return Err("POST_FENCE_IDENTITY_OR_SOURCE_NOT_FRESH");
        }
        let authorized = self.request(post).await?;
        if authorized.status != RiskDecisionStatus::PostAuthorized
            || authorized.reservation_key != held.reservation_key
            || authorized.policy_revision != held.policy_revision
            || authorized.risk_book_sequence <= held.risk_book_sequence
        {
            return Err("POST_FENCE_NOT_AUTHORIZED");
        }
        Ok(authorized)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_protocol::risk_authority::{
        NativeRiskCensus, RISK_COMMAND_SCHEMA_V1, RISK_RESULT_SCHEMA_V1, RiskAccountOccupancy,
        RiskCommandAction, RiskDecisionStatus,
    };
    fn census() -> NativeRiskCensus {
        let uid = "a".repeat(64);
        NativeRiskCensus {
            account_keys: vec![format!("okx:real:{uid}")],
            occupied: vec![RiskAccountOccupancy {
                account_key: format!("okx:real:{uid}"),
                occupied_usdt: "0".to_owned(),
                cumulative_loss_usd: "0".into(),
            }],
            generated_at_ms: 1_000,
            source_digest: "c".repeat(64),
            content_digest: "f".repeat(64),
            source_read_floor_ms: 1_000,
            complete: true,
            native_account_evidence_complete: true,
            native_order_evidence_complete: true,
            native_ledger_evidence_complete: true,
        }
    }
    fn reserve() -> RiskCommand {
        RiskCommand {
            schema: RISK_COMMAND_SCHEMA_V1.to_owned(),
            command_id: "risk_reserve_0001".to_owned(),
            installation_id: "installation-test".into(),
            action: RiskCommandAction::Reserve {
                venue: "okx".to_owned(),
                account_uid: "a".repeat(64),
                run_id: "run_accepted_001".to_owned(),
                intent_id: "intent_accepted_001".to_owned(),
                strategy_id: "alpha_strategy".to_owned(),
                instrument_id: "BTC-USDT-SWAP".to_owned(),
                plan_sha256: "b".repeat(64),
                requested_loss_usdt: "1".to_owned(),
                requested_loss_usd: "1.001".into(),
                entry_upper_usd: Some("2".into()),
                entry_proof_digest: Some("d".repeat(64)),
                usd_conversion: okx_protocol::risk_authority::UsdConversion {
                    usd_per_usdt: "1.001".into(),
                    source_digest: "f".repeat(64),
                    observed_at_ms: 1000,
                },
                policy_revision: 1,
                census: census(),
            },
        }
    }
    fn reply(command_id: String, status: RiskDecisionStatus, seq: u64) -> RiskResult {
        RiskResult {
            schema: RISK_RESULT_SCHEMA_V1.to_owned(),
            command_id,
            status,
            reason: None,
            reservation_key: Some("reservation_identity".to_owned()),
            risk_book_sequence: Some(seq),
            policy_revision: Some(1),
            existing_modelled_loss_usdt: if status == RiskDecisionStatus::PostAuthorized {
                Some("0".to_owned())
            } else {
                None
            },
            reservation_state: None,
            budget: None,
            account_budget: None,
        }
    }
    fn post(held: &RiskResult) -> RiskCommand {
        let mut fresh = census();
        fresh.generated_at_ms = 1_001;
        fresh.source_read_floor_ms = 1_001;
        fresh.source_digest = "e".repeat(64);
        RiskCommand {
            schema: RISK_COMMAND_SCHEMA_V1.to_owned(),
            command_id: "risk_post_000001".to_owned(),
            installation_id: "installation-test".into(),
            action: RiskCommandAction::BeginPost {
                reservation_key: held.reservation_key.clone().unwrap(),
                plan_sha256: "b".repeat(64),
                policy_revision: 1,
                expected_risk_book_sequence: held.risk_book_sequence.unwrap(),
                source_proof_sha256: "d".repeat(64),
                causal_proof: okx_protocol::risk_authority::CausalPostProof {
                    reserve_anchor: held.reservation_key.clone().unwrap(),
                    native_incarnation: "a".repeat(32),
                    elapsed_to_proof_ms: 25,
                    account_reads: vec![okx_protocol::risk_authority::PostAccountReadProof {
                        account_key: format!("okx:real:{}", "a".repeat(64)),
                        private_generation: 1,
                        source_read_id: "b".repeat(64),
                        content_digest: "f".repeat(64),
                        started_after_held_ms: 2,
                        completed_after_held_ms: 15,
                    }],
                },
                census: fresh,
            },
        }
    }
    #[tokio::test]
    async fn fresh_two_phase_receipt_fences_one_post() {
        let (tx, mut rx) = mpsc::channel(4);
        let client = NativeRiskClient::new(tx);
        let server = tokio::spawn(async move {
            let r1 = rx.recv().await.unwrap();
            let _ = r1
                .return_to
                .send(reply(r1.command.command_id, RiskDecisionStatus::Held, 1));
            let r2 = rx.recv().await.unwrap();
            assert!(matches!(
                r2.command.action,
                RiskCommandAction::BeginPost { .. }
            ));
            let _ = r2.return_to.send(reply(
                r2.command.command_id,
                RiskDecisionStatus::PostAuthorized,
                2,
            ));
        });
        let accepted = client
            .reserve_then_fence(reserve(), |held| {
                let next = post(held);
                async move { Ok(next) }
            })
            .await
            .unwrap();
        assert_eq!(accepted.status, RiskDecisionStatus::PostAuthorized);
        server.await.unwrap();
    }
    #[tokio::test]
    async fn denied_reservation_never_sends_post() {
        let (tx, mut rx) = mpsc::channel(4);
        let client = NativeRiskClient::new(tx);
        let server = tokio::spawn(async move {
            let req = rx.recv().await.unwrap();
            let _ =
                req.return_to
                    .send(reply(req.command.command_id, RiskDecisionStatus::Denied, 1));
            assert!(rx.try_recv().is_err());
        });
        let result = client
            .reserve_then_fence(reserve(), |held| {
                let p = post(held);
                async move { Ok(p) }
            })
            .await;
        assert!(matches!(result, Err("RISK_RESERVATION_NOT_HELD")));
        server.await.unwrap();
    }
    #[tokio::test]
    async fn reserve_time_census_cannot_enter_begin_post() {
        let (tx, mut rx) = mpsc::channel(4);
        let client = NativeRiskClient::new(tx);
        let server = tokio::spawn(async move {
            let req = rx.recv().await.unwrap();
            let _ = req
                .return_to
                .send(reply(req.command.command_id, RiskDecisionStatus::Held, 1));
            assert!(rx.try_recv().is_err());
        });
        let result = client
            .reserve_then_fence(reserve(), |held| {
                let mut p = post(held);
                if let RiskCommandAction::BeginPost { census, .. } = &mut p.action {
                    census.generated_at_ms = 1_000;
                    census.source_digest = "c".repeat(64);
                }
                async move { Ok(p) }
            })
            .await;
        assert!(matches!(
            result,
            Err("POST_FENCE_IDENTITY_OR_SOURCE_NOT_FRESH")
        ));
        server.await.unwrap();
    }
    #[tokio::test]
    async fn later_envelope_timestamp_without_all_account_re_read_cannot_post() {
        let (tx, mut rx) = mpsc::channel(4);
        let client = NativeRiskClient::new(tx);
        let server = tokio::spawn(async move {
            let r = rx.recv().await.unwrap();
            let _ = r
                .return_to
                .send(reply(r.command.command_id, RiskDecisionStatus::Held, 1));
            assert!(rx.try_recv().is_err());
        });
        let result = client
            .reserve_then_fence(reserve(), |held| {
                let mut p = post(held);
                if let RiskCommandAction::BeginPost { census, .. } = &mut p.action {
                    census.source_read_floor_ms = 1_000;
                }
                async move { Ok(p) }
            })
            .await;
        assert!(matches!(
            result,
            Err("POST_FENCE_IDENTITY_OR_SOURCE_NOT_FRESH")
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn changed_plan_cannot_turn_held_into_post() {
        let (tx, mut rx) = mpsc::channel(4);
        let client = NativeRiskClient::new(tx);
        let server = tokio::spawn(async move {
            let req = rx.recv().await.unwrap();
            let _ = req
                .return_to
                .send(reply(req.command.command_id, RiskDecisionStatus::Held, 1));
            assert!(rx.try_recv().is_err());
        });
        let result = client
            .reserve_then_fence(reserve(), |held| {
                let mut p = post(held);
                if let RiskCommandAction::BeginPost { plan_sha256, .. } = &mut p.action {
                    *plan_sha256 = "f".repeat(64);
                }
                async move { Ok(p) }
            })
            .await;
        assert!(matches!(
            result,
            Err("POST_FENCE_IDENTITY_OR_SOURCE_NOT_FRESH")
        ));
        server.await.unwrap();
    }
}

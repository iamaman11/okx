use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{
    DurableExecutionLedger, ExchangeOrderState, ExecutionAction, ExecutionLedgerEntry,
    ExecutionLedgerError, ExecutionLineageBinding, ExecutionPlan, ExecutionState,
    ExecutionSubmissionTimingEvidence, OrderSide, OrderType, PositionSide, PrepareDisposition,
    ProtectiveOrderLink, ReverseContinuation, ReverseLeg, TradeMode,
};

pub const EXECUTION_STATUS_SCHEMA_V1: &str = "okx.execution-status/v1";
pub const EXECUTION_STATUS_SCHEMA_V2: &str = "okx.execution-status/v2";
pub const EXECUTION_STATUS_SCHEMA_V3: &str = "okx.execution-status/v3";
pub const EXECUTION_STATUS_CORE_SCHEMA_V2: &str = "okx.execution-status-core/v2";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrepareOutcome {
    Created(ExecutionLedgerEntry),
    Existing(ExecutionLedgerEntry),
    Deferred(PrepareDeferral),
    Rejected(PrepareRejection),
    Failed(PrepareFailure),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrepareDeferral {
    InstrumentBusy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrepareRejection {
    IntentConflict,
    ClientOrderIdCollision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrepareFailure {
    CapacityExceeded { limit: usize },
}

pub fn classify_prepare_result(
    result: Result<PrepareDisposition, ExecutionLedgerError>,
) -> Result<PrepareOutcome, ExecutionLedgerError> {
    match result {
        Ok(PrepareDisposition::Created(entry)) => Ok(PrepareOutcome::Created(entry)),
        Ok(PrepareDisposition::Existing(entry)) => Ok(PrepareOutcome::Existing(entry)),
        Err(ExecutionLedgerError::IntentConflict) => {
            Ok(PrepareOutcome::Rejected(PrepareRejection::IntentConflict))
        }
        Err(ExecutionLedgerError::InstrumentBusy) => {
            Ok(PrepareOutcome::Deferred(PrepareDeferral::InstrumentBusy))
        }
        Err(ExecutionLedgerError::ClientOrderIdCollision) => Ok(PrepareOutcome::Rejected(
            PrepareRejection::ClientOrderIdCollision,
        )),
        Err(ExecutionLedgerError::CapacityExceeded(limit)) => {
            Ok(PrepareOutcome::Failed(PrepareFailure::CapacityExceeded {
                limit,
            }))
        }
        Err(error @ ExecutionLedgerError::Io(_)) => Err(error),
        Err(error @ ExecutionLedgerError::Json(_)) => Err(error),
        Err(error @ ExecutionLedgerError::Corrupt(_)) => Err(error),
        Err(error @ ExecutionLedgerError::InvalidTimestamp) => Err(error),
        Err(error @ ExecutionLedgerError::IntentNotFound(_)) => Err(error),
        Err(error @ ExecutionLedgerError::ReverseMismatch) => Err(error),
        Err(error @ ExecutionLedgerError::ReverseNotReady) => Err(error),
        Err(error @ ExecutionLedgerError::ReverseAborted) => Err(error),
        Err(error @ ExecutionLedgerError::Transition(_)) => Err(error),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionStatusSnapshot {
    pub schema: &'static str,
    pub intent_id: String,
    pub plan_fingerprint: String,
    pub client_order_id: String,
    pub instrument_id: String,
    pub state: ExecutionState,
    pub action: ExecutionAction,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub trade_mode: TradeMode,
    pub order_type: OrderType,
    pub size: String,
    pub price: String,
    pub effective_size: String,
    pub effective_price: String,
    pub order_id_present: bool,
    pub exchange_state: Option<ExchangeOrderState>,
    pub rejection_code: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReverseExecutionStage {
    Closing,
    AwaitingFreshOpen,
    Opening,
    Completed,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReverseExecutionStatus {
    pub root_intent_id: String,
    pub open_intent_id: String,
    pub target_position_side: PositionSide,
    pub stage: ReverseExecutionStage,
    pub close_state: ExecutionState,
    pub open_state: Option<ExecutionState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionSubmissionTimingStatus {
    pub basis: &'static str,
    pub request_exchange_time_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub okx_in_time_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub okx_out_time_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub okx_gateway_processing_us: Option<u64>,
}

impl From<&ExecutionSubmissionTimingEvidence> for ExecutionSubmissionTimingStatus {
    fn from(value: &ExecutionSubmissionTimingEvidence) -> Self {
        let okx_gateway_processing_us = value
            .okx_in_time_us
            .zip(value.okx_out_time_us)
            .map(|(incoming, outgoing)| outgoing - incoming);
        Self {
            basis: "exchange_adjusted_request_ms+okx_gateway_in_out_us",
            request_exchange_time_ms: value.request_exchange_time_ms,
            okx_in_time_us: value.okx_in_time_us,
            okx_out_time_us: value.okx_out_time_us,
            okx_gateway_processing_us,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionStatusEnvelope {
    pub schema: &'static str,
    pub execution: ExecutionStatusSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reverse: Option<ReverseExecutionStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lineage: Option<ExecutionLineageBinding>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protection: Option<ProtectiveOrderLink>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub submission_timing: Option<ExecutionSubmissionTimingStatus>,
}

pub fn execution_status(
    entry: &ExecutionLedgerEntry,
) -> Result<ExecutionStatusSnapshot, serde_json::Error> {
    let plan = &entry.record.plan;
    Ok(ExecutionStatusSnapshot {
        schema: EXECUTION_STATUS_CORE_SCHEMA_V2,
        intent_id: plan.intent_id.clone(),
        plan_fingerprint: plan_fingerprint(plan)?,
        client_order_id: plan.client_order_id.clone(),
        instrument_id: plan.instrument_id.clone(),
        state: entry.record.state,
        action: plan.action,
        side: plan.side,
        position_side: plan.position_side,
        trade_mode: plan.trade_mode,
        order_type: plan.order_type,
        size: plan.size.clone(),
        price: plan.price.clone(),
        effective_size: entry.record.effective_size().to_owned(),
        effective_price: entry.record.effective_price().to_owned(),
        order_id_present: entry
            .record
            .order_id
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty()),
        exchange_state: entry.record.exchange_state,
        rejection_code: entry.record.rejection_code.clone(),
        created_at_ms: entry.created_at_ms,
        updated_at_ms: entry.updated_at_ms,
    })
}

pub fn execution_status_with_ledger(
    ledger: &DurableExecutionLedger,
    intent_id: &str,
) -> Result<Option<ExecutionStatusEnvelope>, serde_json::Error> {
    let Some(entry) = ledger.get(intent_id) else {
        return Ok(None);
    };
    let reverse = reverse_status(ledger, entry);
    Ok(Some(ExecutionStatusEnvelope {
        schema: EXECUTION_STATUS_SCHEMA_V3,
        execution: execution_status(entry)?,
        reverse,
        lineage: entry.record.lineage.clone(),
        protection: entry.record.protection.clone(),
        submission_timing: entry
            .record
            .submission_timing
            .as_ref()
            .map(ExecutionSubmissionTimingStatus::from),
    }))
}

fn reverse_status(
    ledger: &DurableExecutionLedger,
    entry: &ExecutionLedgerEntry,
) -> Option<ReverseExecutionStatus> {
    let link = entry.record.reverse.as_ref()?;
    let (root, open) = match link.leg {
        ReverseLeg::Close => (entry, ledger.get(&link.open_intent_id)),
        ReverseLeg::Open => (ledger.get(&link.root_intent_id)?, Some(entry)),
    };
    let root_link = root.record.reverse.as_ref()?;
    let open_state = open.map(|value| value.record.state);
    let stage = if root_link.continuation == ReverseContinuation::Aborted {
        ReverseExecutionStage::Aborted
    } else if root.record.state != ExecutionState::Filled {
        if root.record.state.is_terminal() {
            ReverseExecutionStage::Aborted
        } else {
            ReverseExecutionStage::Closing
        }
    } else {
        match open_state {
            None => ReverseExecutionStage::AwaitingFreshOpen,
            Some(ExecutionState::Filled) => ReverseExecutionStage::Completed,
            Some(ExecutionState::Canceled | ExecutionState::Rejected) => {
                ReverseExecutionStage::Aborted
            }
            Some(_) => ReverseExecutionStage::Opening,
        }
    };
    Some(ReverseExecutionStatus {
        root_intent_id: root_link.root_intent_id.clone(),
        open_intent_id: root_link.open_intent_id.clone(),
        target_position_side: root_link.target_position_side,
        stage,
        close_state: root.record.state,
        open_state,
    })
}

fn plan_fingerprint(plan: &ExecutionPlan) -> Result<String, serde_json::Error> {
    let payload = serde_json::to_vec(plan)?;
    let digest = Sha256::digest(payload);
    Ok(format!("sha256:{digest:x}"))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use okx_analysis::{
        HARD_RISK_POLICY_SCHEMA_V1, HardRiskPolicy, RiskDegradedMode, RiskMinimumQuality,
        TRADING_MANDATE_SCHEMA_V1, TradingMandate,
    };

    use crate::{
        EXECUTION_PLAN_SCHEMA_V1, ExecutionRecord, ExecutionRiskBinding, ExecutionTransitionError,
        derive_client_order_id,
    };

    fn plan(intent_id: &str) -> ExecutionPlan {
        ExecutionPlan {
            schema: EXECUTION_PLAN_SCHEMA_V1.to_owned(),
            intent_id: intent_id.to_owned(),
            client_order_id: derive_client_order_id(intent_id),
            reference_generation: "sha256:reference".to_owned(),
            account_generation: "sha256:account".to_owned(),
            account_uid_fingerprint: "uid-fingerprint".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            trade_mode: TradeMode::Cross,
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            action: ExecutionAction::Open,
            order_type: OrderType::Limit,
            size: "0.05".to_owned(),
            price: "0.09317".to_owned(),
            open_risk: None,
            risk_binding: None,
        }
    }

    fn entry(intent_id: &str) -> ExecutionLedgerEntry {
        ExecutionLedgerEntry {
            record: ExecutionRecord::new(plan(intent_id)),
            created_at_ms: 100,
            updated_at_ms: 100,
        }
    }

    #[test]
    fn deterministic_prepare_results_are_domain_outcomes() {
        let created = entry("intent_created_01234567");
        assert!(matches!(
            classify_prepare_result(Ok(PrepareDisposition::Created(created))),
            Ok(PrepareOutcome::Created(_))
        ));

        let existing = entry("intent_existing_012345");
        assert!(matches!(
            classify_prepare_result(Ok(PrepareDisposition::Existing(existing))),
            Ok(PrepareOutcome::Existing(_))
        ));

        assert_eq!(
            classify_prepare_result(Err(ExecutionLedgerError::IntentConflict))
                .expect("terminal conflict"),
            PrepareOutcome::Rejected(PrepareRejection::IntentConflict)
        );
        assert_eq!(
            classify_prepare_result(Err(ExecutionLedgerError::ClientOrderIdCollision))
                .expect("terminal collision"),
            PrepareOutcome::Rejected(PrepareRejection::ClientOrderIdCollision)
        );
        assert_eq!(
            classify_prepare_result(Err(ExecutionLedgerError::CapacityExceeded(10_000)))
                .expect("terminal capacity"),
            PrepareOutcome::Failed(PrepareFailure::CapacityExceeded { limit: 10_000 })
        );
    }

    #[test]
    fn infrastructure_and_invariant_prepare_errors_remain_internal() {
        let json_error = serde_json::from_str::<serde_json::Value>("{")
            .expect_err("malformed JSON produces parse error");
        let errors = [
            ExecutionLedgerError::Io(std::io::Error::other("disk unavailable")),
            ExecutionLedgerError::Json(json_error),
            ExecutionLedgerError::Corrupt("tampered"),
            ExecutionLedgerError::InvalidTimestamp,
            ExecutionLedgerError::IntentNotFound("missing".to_owned()),
            ExecutionLedgerError::Transition(ExecutionTransitionError::InvalidTransition {
                from: ExecutionState::Prepared,
                to: ExecutionState::Acknowledged,
            }),
        ];

        for error in errors {
            assert!(classify_prepare_result(Err(error)).is_err());
        }
    }

    #[test]
    fn replay_restart_and_idempotency_matrix_is_fail_closed() {
        let root = std::env::temp_dir().join(format!(
            "okx-execution-contract-matrix-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let store = crate::ExecutionLedgerStore::at(root.join("ledger.json"));
        let mut ledger = crate::DurableExecutionLedger::open(store.clone(), 100).expect("open");

        let original = plan("intent_matrix_0123456789");
        assert!(matches!(
            classify_prepare_result(ledger.prepare(original.clone(), 101)).expect("created"),
            PrepareOutcome::Created(_)
        ));
        assert!(matches!(
            classify_prepare_result(ledger.prepare(original.clone(), 102)).expect("existing"),
            PrepareOutcome::Existing(_)
        ));

        let mut conflict = original.clone();
        conflict.price = "0.09318".to_owned();
        assert_eq!(
            classify_prepare_result(ledger.prepare(conflict, 103)).expect("conflict"),
            PrepareOutcome::Rejected(PrepareRejection::IntentConflict)
        );

        ledger
            .begin_submission(&original.intent_id, 104)
            .expect("persist submitting");
        drop(ledger);

        let reopened = crate::DurableExecutionLedger::open(store.clone(), 200).expect("reopen");
        let recovered = reopened.get(&original.intent_id).expect("recovered");
        assert_eq!(recovered.record.state, ExecutionState::UnknownSubmission);
        assert!(!recovered.record.can_submit());

        let status = execution_status(recovered).expect("status");
        assert_eq!(status.state, ExecutionState::UnknownSubmission);
        assert_eq!(status.intent_id, original.intent_id);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reverse_status_is_derived_from_durable_close_and_open_records() {
        let root = std::env::temp_dir().join(format!(
            "okx-execution-reverse-status-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let store = crate::ExecutionLedgerStore::at(root.join("ledger.json"));
        let mut ledger = crate::DurableExecutionLedger::open(store, 100).expect("open");
        let root_intent_id = "intent_reverse_status_0123";

        let mut close = plan(root_intent_id);
        close.action = ExecutionAction::Close;
        close.position_side = PositionSide::Long;
        close.side = OrderSide::Sell;
        close.open_risk = None;
        ledger
            .prepare_reverse_close(close, PositionSide::Short, 101)
            .expect("prepare reverse close");

        let closing = execution_status_with_ledger(&ledger, root_intent_id)
            .expect("status")
            .expect("root");
        assert_eq!(closing.schema, EXECUTION_STATUS_SCHEMA_V3);
        assert_eq!(
            closing.reverse.expect("reverse").stage,
            ReverseExecutionStage::Closing
        );

        ledger
            .begin_submission(root_intent_id, 102)
            .expect("submit close");
        ledger
            .acknowledge(root_intent_id, "close-order", 103)
            .expect("ack close");
        ledger
            .reconcile_found(
                root_intent_id,
                "close-order",
                ExchangeOrderState::Filled,
                104,
            )
            .expect("close filled");

        let awaiting = execution_status_with_ledger(&ledger, root_intent_id)
            .expect("status")
            .expect("root");
        let awaiting_reverse = awaiting.reverse.expect("reverse");
        assert_eq!(
            awaiting_reverse.stage,
            ReverseExecutionStage::AwaitingFreshOpen
        );
        assert_eq!(awaiting_reverse.open_state, None);

        let open_intent_id = crate::derive_reverse_open_intent_id(root_intent_id);
        let mut open = plan(&open_intent_id);
        open.position_side = PositionSide::Short;
        open.side = OrderSide::Sell;
        ledger
            .prepare_reverse_open(root_intent_id, open, 105)
            .expect("prepare reverse open");

        let opening = execution_status_with_ledger(&ledger, root_intent_id)
            .expect("status")
            .expect("root");
        assert_eq!(
            opening.reverse.expect("reverse").stage,
            ReverseExecutionStage::Opening
        );

        ledger
            .begin_submission(&open_intent_id, 106)
            .expect("submit open");
        ledger
            .acknowledge(&open_intent_id, "open-order", 107)
            .expect("ack open");
        ledger
            .reconcile_found(
                &open_intent_id,
                "open-order",
                ExchangeOrderState::Filled,
                108,
            )
            .expect("open filled");

        let completed = execution_status_with_ledger(&ledger, root_intent_id)
            .expect("status")
            .expect("root");
        let completed_reverse = completed.reverse.expect("reverse");
        assert_eq!(completed_reverse.stage, ReverseExecutionStage::Completed);
        assert_eq!(completed_reverse.open_state, Some(ExecutionState::Filled));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn execution_status_is_compact_stable_and_does_not_expose_order_id() {
        let mut entry = entry("intent_status_01234567");
        entry.record.order_id = Some("exchange-order-id".to_owned());
        entry.record.state = ExecutionState::Acknowledged;
        entry.updated_at_ms = 101;

        let status = execution_status(&entry).expect("status");
        assert_eq!(status.schema, EXECUTION_STATUS_CORE_SCHEMA_V2);
        assert_eq!(status.state, ExecutionState::Acknowledged);
        assert!(status.order_id_present);
        assert!(status.plan_fingerprint.starts_with("sha256:"));
        assert_eq!(status.plan_fingerprint.len(), 71);

        let json = serde_json::to_string(&status).expect("json");
        assert!(!json.contains("exchange-order-id"));
        assert!(json.contains("DOGE-USDT-SWAP"));
    }

    fn risk_binding(policy_version: &str) -> ExecutionRiskBinding {
        ExecutionRiskBinding {
            mandate: TradingMandate {
                schema: TRADING_MANDATE_SCHEMA_V1.to_owned(),
                version: "mandate/v1".to_owned(),
                capital_base_usd: "100".to_owned(),
                decision_horizon_hours: 24,
                benchmark: Some("none/v1".to_owned()),
                allowed_instruments: vec!["DOGE-USDT-SWAP".to_owned()],
                max_drawdown_ratio: "1".to_owned(),
                leverage_ceiling: "5".to_owned(),
                minimum_liquidity_notional_usd: "0".to_owned(),
                max_turnover_ratio: "5".to_owned(),
            },
            policy: HardRiskPolicy {
                schema: HARD_RISK_POLICY_SCHEMA_V1.to_owned(),
                version: policy_version.to_owned(),
                max_account_gross_notional_usd: "1000".to_owned(),
                max_instrument_gross_notional_usd: "1000".to_owned(),
                max_margin_utilization_ratio: "1".to_owned(),
                max_loss_per_trade_usd: "100".to_owned(),
                max_daily_realized_loss_usd: "100".to_owned(),
                max_drawdown_ratio: "1".to_owned(),
                max_leverage: "5".to_owned(),
                allowed_instruments: vec!["DOGE-USDT-SWAP".to_owned()],
                minimum_quality: RiskMinimumQuality::Fresh,
                degraded_mode: RiskDegradedMode::Reject,
                correlated_clusters: Vec::new(),
            },
        }
    }

    #[test]
    fn plan_fingerprint_binds_the_exact_risk_policy_version() {
        let mut first = plan("intent_policy_fingerprint_01");
        first.risk_binding = Some(risk_binding("policy/v1"));
        let mut second = first.clone();
        second.risk_binding = Some(risk_binding("policy/v2"));

        let first = plan_fingerprint(&first).expect("first");
        let second = plan_fingerprint(&second).expect("second");
        assert_ne!(first, second);
    }

    #[test]
    fn plan_fingerprint_changes_when_immutable_plan_changes() {
        let first = entry("intent_fingerprint_0123");
        let mut second = first.clone();
        second.record.plan.price = "0.09318".to_owned();

        let first = execution_status(&first).expect("first");
        let second = execution_status(&second).expect("second");
        assert_ne!(first.plan_fingerprint, second.plan_fingerprint);
    }
}

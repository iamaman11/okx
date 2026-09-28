use std::str::FromStr;

use okx_api::{
    ApiOrderSide, ApiOrderType, ApiPositionSide, ApiTradeMode, OrderOperationAck,
    PlaceOrderRequest, TradeOrderDetails, TradeResponse,
};
use rust_decimal::Decimal;
use thiserror::Error;

use crate::{
    DurableExecutionLedger, ExchangeOrderState, ExecutionLedgerEntry, ExecutionLedgerError,
    ExecutionPlan, ExecutionState, ExecutionTransitionError, OrderSide, OrderType, PositionSide,
    TradeMode, require_live_trading_enabled,
};

#[derive(Debug)]
pub struct OrderExecutor {
    ledger: DurableExecutionLedger,
    live_trading_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlaceResponseDisposition {
    Acknowledged(ExecutionLedgerEntry),
    Rejected(ExecutionLedgerEntry),
    UnknownSubmission(ExecutionLedgerEntry),
}

#[derive(Debug, Error)]
pub enum OrderExecutorError {
    #[error(transparent)]
    Ledger(#[from] ExecutionLedgerError),

    #[error(transparent)]
    Transition(#[from] ExecutionTransitionError),

    #[error("execution intent '{0}' is not present in the ledger")]
    IntentNotFound(String),

    #[error("execution intent is not PREPARED")]
    NotPrepared,

    #[error("exchange order detail does not match the durable execution plan: {0}")]
    ReconciliationIdentity(&'static str),

    #[error("exchange order state '{0}' is unsupported")]
    UnsupportedExchangeState(String),

    #[error("exchange numeric field '{field}' is invalid: '{value}'")]
    InvalidExchangeDecimal { field: &'static str, value: String },
}

impl OrderExecutor {
    pub fn new(ledger: DurableExecutionLedger, live_trading_enabled: bool) -> Self {
        Self {
            ledger,
            live_trading_enabled,
        }
    }

    pub fn live_trading_enabled(&self) -> bool {
        self.live_trading_enabled
    }

    pub fn ledger(&self) -> &DurableExecutionLedger {
        &self.ledger
    }

    pub fn begin_place(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<PlaceOrderRequest, OrderExecutorError> {
        require_live_trading_enabled(self.live_trading_enabled)?;

        let entry = self
            .ledger
            .get(intent_id)
            .ok_or_else(|| OrderExecutorError::IntentNotFound(intent_id.to_owned()))?;
        if entry.record.state != ExecutionState::Prepared {
            return Err(OrderExecutorError::NotPrepared);
        }
        let request = place_request(&entry.record.plan);

        self.ledger.begin_submission(intent_id, observed_at_ms)?;
        Ok(request)
    }

    pub fn observe_place_response(
        &mut self,
        intent_id: &str,
        response: &TradeResponse<OrderOperationAck>,
        observed_at_ms: u64,
    ) -> Result<PlaceResponseDisposition, OrderExecutorError> {
        let entry = self
            .ledger
            .get(intent_id)
            .ok_or_else(|| OrderExecutorError::IntentNotFound(intent_id.to_owned()))?;
        if entry.record.state != ExecutionState::Submitting {
            return Err(OrderExecutorError::NotPrepared);
        }

        if response.code != "0" {
            if response.code.trim().is_empty() {
                let entry = self
                    .ledger
                    .mark_unknown_submission(intent_id, observed_at_ms)?;
                return Ok(PlaceResponseDisposition::UnknownSubmission(entry));
            }
            let entry = self.ledger.reject_known(
                intent_id,
                format!("top:{}", response.code),
                observed_at_ms,
            )?;
            return Ok(PlaceResponseDisposition::Rejected(entry));
        }

        if response.data.len() != 1 {
            let entry = self
                .ledger
                .mark_unknown_submission(intent_id, observed_at_ms)?;
            return Ok(PlaceResponseDisposition::UnknownSubmission(entry));
        }

        let ack = &response.data[0];
        if ack.status_code != "0" {
            if ack.status_code.trim().is_empty() {
                let entry = self
                    .ledger
                    .mark_unknown_submission(intent_id, observed_at_ms)?;
                return Ok(PlaceResponseDisposition::UnknownSubmission(entry));
            }
            let entry =
                self.ledger
                    .reject_known(intent_id, ack.status_code.clone(), observed_at_ms)?;
            return Ok(PlaceResponseDisposition::Rejected(entry));
        }

        let plan = &self
            .ledger
            .get(intent_id)
            .expect("intent existence checked above")
            .record
            .plan;
        if ack.order_id.trim().is_empty() || ack.client_order_id != plan.client_order_id {
            let entry = self
                .ledger
                .mark_unknown_submission(intent_id, observed_at_ms)?;
            return Ok(PlaceResponseDisposition::UnknownSubmission(entry));
        }

        let entry =
            self.ledger
                .acknowledge(intent_id, ack.order_id.clone(), observed_at_ms)?;
        Ok(PlaceResponseDisposition::Acknowledged(entry))
    }

    pub fn observe_place_transport_ambiguity(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, OrderExecutorError> {
        Ok(self
            .ledger
            .mark_unknown_submission(intent_id, observed_at_ms)?)
    }

    pub fn reconcile_order_details(
        &mut self,
        intent_id: &str,
        details: &TradeOrderDetails,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, OrderExecutorError> {
        let entry = self
            .ledger
            .get(intent_id)
            .ok_or_else(|| OrderExecutorError::IntentNotFound(intent_id.to_owned()))?;
        validate_order_identity(&entry.record.plan, details)?;
        let exchange_state = exchange_state(details.state.as_str())?;
        Ok(self.ledger.reconcile_found(
            intent_id,
            details.order_id.clone(),
            exchange_state,
            observed_at_ms,
        )?)
    }
}

fn place_request(plan: &ExecutionPlan) -> PlaceOrderRequest {
    PlaceOrderRequest {
        instrument_id: plan.instrument_id.clone(),
        trade_mode: match plan.trade_mode {
            TradeMode::Cross => ApiTradeMode::Cross,
            TradeMode::Isolated => ApiTradeMode::Isolated,
        },
        client_order_id: plan.client_order_id.clone(),
        side: match plan.side {
            OrderSide::Buy => ApiOrderSide::Buy,
            OrderSide::Sell => ApiOrderSide::Sell,
        },
        position_side: match plan.position_side {
            PositionSide::Long => ApiPositionSide::Long,
            PositionSide::Short => ApiPositionSide::Short,
        },
        order_type: match plan.order_type {
            OrderType::Limit => ApiOrderType::Limit,
            OrderType::PostOnly => ApiOrderType::PostOnly,
            OrderType::Fok => ApiOrderType::Fok,
            OrderType::Ioc => ApiOrderType::Ioc,
        },
        size: plan.size.clone(),
        price: plan.price.clone(),
    }
}

fn validate_order_identity(
    plan: &ExecutionPlan,
    details: &TradeOrderDetails,
) -> Result<(), OrderExecutorError> {
    if details.order_id.trim().is_empty() {
        return Err(OrderExecutorError::ReconciliationIdentity("missing ordId"));
    }
    if details.instrument_id != plan.instrument_id {
        return Err(OrderExecutorError::ReconciliationIdentity("instId"));
    }
    if details.client_order_id != plan.client_order_id {
        return Err(OrderExecutorError::ReconciliationIdentity("clOrdId"));
    }
    if details.side != order_side_text(plan.side) {
        return Err(OrderExecutorError::ReconciliationIdentity("side"));
    }
    if details.position_side != position_side_text(plan.position_side) {
        return Err(OrderExecutorError::ReconciliationIdentity("posSide"));
    }
    if details.trade_mode != trade_mode_text(plan.trade_mode) {
        return Err(OrderExecutorError::ReconciliationIdentity("tdMode"));
    }
    if details.order_type != order_type_text(plan.order_type) {
        return Err(OrderExecutorError::ReconciliationIdentity("ordType"));
    }
    if decimal("px", &details.price)? != decimal("plan.price", &plan.price)? {
        return Err(OrderExecutorError::ReconciliationIdentity("px"));
    }
    if decimal("sz", &details.size)? != decimal("plan.size", &plan.size)? {
        return Err(OrderExecutorError::ReconciliationIdentity("sz"));
    }

    let filled = decimal("accFillSz", &details.accumulated_fill_size)?;
    let size = decimal("sz", &details.size)?;
    if filled < Decimal::ZERO || filled > size {
        return Err(OrderExecutorError::ReconciliationIdentity("accFillSz"));
    }

    Ok(())
}

fn exchange_state(value: &str) -> Result<ExchangeOrderState, OrderExecutorError> {
    match value {
        "live" => Ok(ExchangeOrderState::Live),
        "partially_filled" => Ok(ExchangeOrderState::PartiallyFilled),
        "filled" => Ok(ExchangeOrderState::Filled),
        "canceled" => Ok(ExchangeOrderState::Canceled),
        other => Err(OrderExecutorError::UnsupportedExchangeState(
            other.to_owned(),
        )),
    }
}

fn decimal(field: &'static str, value: &str) -> Result<Decimal, OrderExecutorError> {
    Decimal::from_str(value).map_err(|_| OrderExecutorError::InvalidExchangeDecimal {
        field,
        value: value.to_owned(),
    })
}

const fn order_side_text(value: OrderSide) -> &'static str {
    match value {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

const fn position_side_text(value: PositionSide) -> &'static str {
    match value {
        PositionSide::Long => "long",
        PositionSide::Short => "short",
    }
}

const fn trade_mode_text(value: TradeMode) -> &'static str {
    match value {
        TradeMode::Cross => "cross",
        TradeMode::Isolated => "isolated",
    }
}

const fn order_type_text(value: OrderType) -> &'static str {
    match value {
        OrderType::Limit => "limit",
        OrderType::PostOnly => "post_only",
        OrderType::Fok => "fok",
        OrderType::Ioc => "ioc",
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use crate::{
        EXECUTION_PLAN_SCHEMA_V1, ExecutionAction, ExecutionLedgerStore, ExecutionRecord,
        ExecutionState, OrderSide, PositionSide,
    };

    use super::*;

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "okx-order-executor-{name}-{}",
            std::process::id()
        ))
    }

    fn plan() -> ExecutionPlan {
        ExecutionPlan {
            schema: EXECUTION_PLAN_SCHEMA_V1.to_owned(),
            intent_id: "intent_0123456789abcdef".to_owned(),
            client_order_id: "okx01234567890123456789012345678".to_owned(),
            reference_generation: "sha256:reference".to_owned(),
            account_generation: "sha256:account".to_owned(),
            account_uid_fingerprint: "uid-fingerprint".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            trade_mode: TradeMode::Cross,
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            action: ExecutionAction::Open,
            order_type: OrderType::Limit,
            size: "4.95".to_owned(),
            price: "0.10000".to_owned(),
            open_risk: None,
        }
    }

    fn executor(name: &str, enabled: bool) -> (PathBuf, OrderExecutor) {
        let root = temp_root(name);
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let mut ledger = DurableExecutionLedger::open(store, 100).expect("ledger");
        ledger.prepare(plan(), 101).expect("prepare");
        (root, OrderExecutor::new(ledger, enabled))
    }

    fn accepted_response() -> TradeResponse<OrderOperationAck> {
        TradeResponse {
            code: "0".to_owned(),
            message: String::new(),
            data: vec![OrderOperationAck {
                order_id: "12345".to_owned(),
                client_order_id: "okx01234567890123456789012345678".to_owned(),
                request_id: String::new(),
                timestamp_ms: "1790000000000".to_owned(),
                status_code: "0".to_owned(),
                status_message: String::new(),
            }],
            in_time_us: "1790000000000000".to_owned(),
            out_time_us: "1790000000001000".to_owned(),
        }
    }

    fn live_details() -> TradeOrderDetails {
        TradeOrderDetails {
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            order_id: "12345".to_owned(),
            client_order_id: "okx01234567890123456789012345678".to_owned(),
            side: "buy".to_owned(),
            position_side: "long".to_owned(),
            trade_mode: "cross".to_owned(),
            order_type: "limit".to_owned(),
            price: "0.1".to_owned(),
            size: "4.950".to_owned(),
            accumulated_fill_size: "0".to_owned(),
            average_fill_price: String::new(),
            state: "live".to_owned(),
            creation_time_ms: "1790000000000".to_owned(),
            update_time_ms: "1790000000001".to_owned(),
        }
    }

    #[test]
    fn disabled_gate_never_enters_submitting() {
        let (root, mut executor) = executor("disabled", false);
        assert!(matches!(
            executor.begin_place("intent_0123456789abcdef", 102),
            Err(OrderExecutorError::Transition(
                ExecutionTransitionError::LiveTradingDisabled
            ))
        ));
        assert_eq!(
            executor
                .ledger()
                .get("intent_0123456789abcdef")
                .expect("entry")
                .record
                .state,
            ExecutionState::Prepared
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn enabled_begin_persists_submitting_before_returning_request() {
        let (root, mut executor) = executor("begin", true);
        let request = executor
            .begin_place("intent_0123456789abcdef", 102)
            .expect("begin");

        assert_eq!(request.instrument_id, "DOGE-USDT-SWAP");
        assert_eq!(
            request.client_order_id,
            "okx01234567890123456789012345678"
        );
        assert_eq!(request.side, ApiOrderSide::Buy);
        assert_eq!(request.position_side, ApiPositionSide::Long);
        assert_eq!(
            executor
                .ledger()
                .get("intent_0123456789abcdef")
                .expect("entry")
                .record
                .state,
            ExecutionState::Submitting
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn definitive_ack_and_rejection_are_persisted() {
        let (root, mut executor) = executor("response", true);
        executor
            .begin_place("intent_0123456789abcdef", 102)
            .expect("begin");
        let disposition = executor
            .observe_place_response(
                "intent_0123456789abcdef",
                &accepted_response(),
                103,
            )
            .expect("ack");

        assert!(matches!(
            disposition,
            PlaceResponseDisposition::Acknowledged(_)
        ));
        assert_eq!(
            executor
                .ledger()
                .get("intent_0123456789abcdef")
                .expect("entry")
                .record
                .state,
            ExecutionState::Acknowledged
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn malformed_success_response_becomes_unknown_not_retryable() {
        let (root, mut executor) = executor("unknown", true);
        executor
            .begin_place("intent_0123456789abcdef", 102)
            .expect("begin");
        let mut response = accepted_response();
        response.data.clear();

        let disposition = executor
            .observe_place_response("intent_0123456789abcdef", &response, 103)
            .expect("unknown");

        assert!(matches!(
            disposition,
            PlaceResponseDisposition::UnknownSubmission(_)
        ));
        let entry = executor
            .ledger()
            .get("intent_0123456789abcdef")
            .expect("entry");
        assert_eq!(entry.record.state, ExecutionState::UnknownSubmission);
        assert!(!entry.record.can_submit());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn unknown_submission_adopts_exact_exchange_order_by_client_id() {
        let (root, mut executor) = executor("reconcile", true);
        executor
            .begin_place("intent_0123456789abcdef", 102)
            .expect("begin");
        executor
            .observe_place_transport_ambiguity("intent_0123456789abcdef", 103)
            .expect("unknown");

        let reconciled = executor
            .reconcile_order_details(
                "intent_0123456789abcdef",
                &live_details(),
                104,
            )
            .expect("reconcile");

        assert_eq!(reconciled.record.state, ExecutionState::Live);
        assert_eq!(reconciled.record.order_id.as_deref(), Some("12345"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reconciliation_rejects_same_client_id_with_mismatched_order_shape() {
        let (root, mut executor) = executor("collision", true);
        executor
            .begin_place("intent_0123456789abcdef", 102)
            .expect("begin");
        executor
            .observe_place_transport_ambiguity("intent_0123456789abcdef", 103)
            .expect("unknown");

        let mut details = live_details();
        details.side = "sell".to_owned();
        assert!(matches!(
            executor.reconcile_order_details(
                "intent_0123456789abcdef",
                &details,
                104,
            ),
            Err(OrderExecutorError::ReconciliationIdentity("side"))
        ));
        assert_eq!(
            executor
                .ledger()
                .get("intent_0123456789abcdef")
                .expect("entry")
                .record
                .state,
            ExecutionState::UnknownSubmission
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn numeric_normalization_does_not_create_false_identity_mismatch() {
        let plan = plan();
        let details = live_details();
        validate_order_identity(&plan, &details).expect("same numeric values");
    }
}

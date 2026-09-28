use async_trait::async_trait;
use okx_api::{
    ApiOrderSide, ApiOrderType, ApiPositionSide, ApiTradeMode, OkxError, OrderOperationAck,
    PlaceOrderRequest, TradeApi, TradeOrderDetails, TradeResponse,
};
use thiserror::Error;

use crate::{
    DurableExecutionLedger, ExchangeOrderState, ExecutionLedgerEntry, ExecutionLedgerError,
    ExecutionPlan, ExecutionState, ExecutionTransitionError, OrderSide, OrderType, PositionSide,
    PrepareDisposition, TradeMode, require_live_trading_enabled,
};

#[async_trait]
pub trait ExecutionGateway: Send + Sync {
    async fn place_order(
        &self,
        request: PlaceOrderRequest,
        exp_time_ms: u64,
    ) -> Result<TradeResponse<OrderOperationAck>, OkxError>;

    async fn order_by_client_id(
        &self,
        instrument_id: String,
        client_order_id: String,
    ) -> Result<TradeOrderDetails, OkxError>;
}

#[async_trait]
impl ExecutionGateway for TradeApi {
    async fn place_order(
        &self,
        request: PlaceOrderRequest,
        exp_time_ms: u64,
    ) -> Result<TradeResponse<OrderOperationAck>, OkxError> {
        TradeApi::place_order(self, &request, Some(exp_time_ms)).await
    }

    async fn order_by_client_id(
        &self,
        instrument_id: String,
        client_order_id: String,
    ) -> Result<TradeOrderDetails, OkxError> {
        TradeApi::order_by_client_id(self, &instrument_id, &client_order_id).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitDisposition {
    Acknowledged(ExecutionLedgerEntry),
    Rejected(ExecutionLedgerEntry),
    UnknownSubmission(ExecutionLedgerEntry),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileDisposition {
    Found(ExecutionLedgerEntry),
    Unavailable(ExecutionLedgerEntry),
}

#[derive(Debug, Error)]
pub enum OrderExecutorError {
    #[error(transparent)]
    Ledger(#[from] ExecutionLedgerError),

    #[error(transparent)]
    Transition(#[from] ExecutionTransitionError),

    #[error("execution record is not PREPARED; current state is {0:?}")]
    NotPrepared(ExecutionState),

    #[error("execution record state {0:?} is not reconcilable")]
    NotReconcilable(ExecutionState),

    #[error("submission expTime must be a non-zero Unix millisecond timestamp")]
    InvalidExpiry,

    #[error("exchange order details do not match the execution plan identity")]
    ReconciliationIdentityMismatch,

    #[error("unsupported exchange order state '{0}'")]
    UnsupportedExchangeState(String),
}

pub struct OrderExecutor<G> {
    ledger: DurableExecutionLedger,
    gateway: G,
    live_trading_enabled: bool,
}

impl<G> OrderExecutor<G>
where
    G: ExecutionGateway,
{
    pub fn new(ledger: DurableExecutionLedger, gateway: G) -> Self {
        Self {
            ledger,
            gateway,
            live_trading_enabled: false,
        }
    }

    pub fn ledger(&self) -> &DurableExecutionLedger {
        &self.ledger
    }

    pub fn prepare(
        &mut self,
        plan: ExecutionPlan,
        observed_at_ms: u64,
    ) -> Result<PrepareDisposition, OrderExecutorError> {
        Ok(self.ledger.prepare(plan, observed_at_ms)?)
    }

    pub async fn submit_prepared(
        &mut self,
        intent_id: &str,
        exp_time_ms: u64,
        observed_at_ms: u64,
    ) -> Result<SubmitDisposition, OrderExecutorError> {
        if exp_time_ms == 0 {
            return Err(OrderExecutorError::InvalidExpiry);
        }

        // The production constructor is intentionally fail-closed. The gate is
        // evaluated before SUBMITTING is persisted, so a disabled runtime does
        // not create false ambiguous-submission evidence.
        require_live_trading_enabled(self.live_trading_enabled)?;

        let entry = self
            .ledger
            .get(intent_id)
            .cloned()
            .ok_or_else(|| ExecutionLedgerError::IntentNotFound(intent_id.to_owned()))?;

        if entry.record.state != ExecutionState::Prepared {
            return Err(OrderExecutorError::NotPrepared(entry.record.state));
        }

        let request = place_request(&entry.record.plan);
        self.ledger.begin_submission(intent_id, observed_at_ms)?;

        match self.gateway.place_order(request, exp_time_ms).await {
            Err(_) => {
                let entry = self
                    .ledger
                    .mark_unknown_submission(intent_id, observed_at_ms)?;
                Ok(SubmitDisposition::UnknownSubmission(entry))
            }
            Ok(response) => match classify_place_response(&entry.record.plan, response) {
                PlaceResponse::Acknowledged(order_id) => {
                    let entry = self
                        .ledger
                        .acknowledge(intent_id, order_id, observed_at_ms)?;
                    Ok(SubmitDisposition::Acknowledged(entry))
                }
                PlaceResponse::Rejected(code) => {
                    let entry = self.ledger.reject_known(intent_id, code, observed_at_ms)?;
                    Ok(SubmitDisposition::Rejected(entry))
                }
                PlaceResponse::Ambiguous => {
                    let entry = self
                        .ledger
                        .mark_unknown_submission(intent_id, observed_at_ms)?;
                    Ok(SubmitDisposition::UnknownSubmission(entry))
                }
            },
        }
    }

    pub async fn reconcile(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<ReconcileDisposition, OrderExecutorError> {
        let entry = self
            .ledger
            .get(intent_id)
            .cloned()
            .ok_or_else(|| ExecutionLedgerError::IntentNotFound(intent_id.to_owned()))?;

        if !matches!(
            entry.record.state,
            ExecutionState::Acknowledged
                | ExecutionState::UnknownSubmission
                | ExecutionState::Live
                | ExecutionState::PartiallyFilled
        ) {
            return Err(OrderExecutorError::NotReconcilable(entry.record.state));
        }

        let plan = &entry.record.plan;
        let order = match self
            .gateway
            .order_by_client_id(plan.instrument_id.clone(), plan.client_order_id.clone())
            .await
        {
            Ok(order) => order,
            Err(_) => return Ok(ReconcileDisposition::Unavailable(entry)),
        };

        if order.instrument_id != plan.instrument_id
            || order.client_order_id != plan.client_order_id
            || order.order_id.trim().is_empty()
        {
            return Err(OrderExecutorError::ReconciliationIdentityMismatch);
        }

        let state = map_exchange_state(&order.state)?;
        let entry =
            self.ledger
                .reconcile_found(intent_id, order.order_id, state, observed_at_ms)?;
        Ok(ReconcileDisposition::Found(entry))
    }

    #[cfg(test)]
    fn enabled_for_test(ledger: DurableExecutionLedger, gateway: G) -> Self {
        Self {
            ledger,
            gateway,
            live_trading_enabled: true,
        }
    }

    #[cfg(test)]
    fn gateway(&self) -> &G {
        &self.gateway
    }
}

enum PlaceResponse {
    Acknowledged(String),
    Rejected(String),
    Ambiguous,
}

fn classify_place_response(
    plan: &ExecutionPlan,
    response: TradeResponse<OrderOperationAck>,
) -> PlaceResponse {
    if !response.top_level_success() {
        return if response.code.trim().is_empty() {
            PlaceResponse::Ambiguous
        } else {
            PlaceResponse::Rejected(response.code)
        };
    }

    let [item] = response.data.as_slice() else {
        return PlaceResponse::Ambiguous;
    };

    if item.client_order_id != plan.client_order_id {
        return PlaceResponse::Ambiguous;
    }
    if !item.accepted() {
        return if item.status_code.trim().is_empty() {
            PlaceResponse::Ambiguous
        } else {
            PlaceResponse::Rejected(item.status_code.clone())
        };
    }
    if item.order_id.trim().is_empty() {
        return PlaceResponse::Ambiguous;
    }

    PlaceResponse::Acknowledged(item.order_id.clone())
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

fn map_exchange_state(value: &str) -> Result<ExchangeOrderState, OrderExecutorError> {
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

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        fs,
        path::PathBuf,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use okx_api::{OrderOperationAck, TradeOrderDetails, TradeResponse};

    use super::*;
    use crate::{
        EXECUTION_PLAN_SCHEMA_V1, ExecutionAction, ExecutionLedgerStore, OrderSide, PositionSide,
    };

    struct MockGateway {
        place_calls: AtomicUsize,
        lookup_calls: AtomicUsize,
        place_results: Mutex<VecDeque<Result<TradeResponse<OrderOperationAck>, OkxError>>>,
        lookup_results: Mutex<VecDeque<Result<TradeOrderDetails, OkxError>>>,
    }

    impl MockGateway {
        fn new(
            place_results: Vec<Result<TradeResponse<OrderOperationAck>, OkxError>>,
            lookup_results: Vec<Result<TradeOrderDetails, OkxError>>,
        ) -> Self {
            Self {
                place_calls: AtomicUsize::new(0),
                lookup_calls: AtomicUsize::new(0),
                place_results: Mutex::new(place_results.into()),
                lookup_results: Mutex::new(lookup_results.into()),
            }
        }

        fn place_calls(&self) -> usize {
            self.place_calls.load(Ordering::SeqCst)
        }

        fn lookup_calls(&self) -> usize {
            self.lookup_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ExecutionGateway for MockGateway {
        async fn place_order(
            &self,
            _request: PlaceOrderRequest,
            _exp_time_ms: u64,
        ) -> Result<TradeResponse<OrderOperationAck>, OkxError> {
            self.place_calls.fetch_add(1, Ordering::SeqCst);
            self.place_results
                .lock()
                .expect("place queue")
                .pop_front()
                .expect("place result")
        }

        async fn order_by_client_id(
            &self,
            _instrument_id: String,
            _client_order_id: String,
        ) -> Result<TradeOrderDetails, OkxError> {
            self.lookup_calls.fetch_add(1, Ordering::SeqCst);
            self.lookup_results
                .lock()
                .expect("lookup queue")
                .pop_front()
                .expect("lookup result")
        }
    }

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("okx-order-executor-{name}-{}", std::process::id()))
    }

    fn ledger(name: &str) -> (PathBuf, DurableExecutionLedger) {
        let root = temp_root(name);
        let _ = fs::remove_dir_all(&root);
        let ledger =
            DurableExecutionLedger::open(ExecutionLedgerStore::at(root.join("ledger.json")), 100)
                .expect("ledger");
        (root, ledger)
    }

    fn plan() -> ExecutionPlan {
        ExecutionPlan {
            schema: EXECUTION_PLAN_SCHEMA_V1.to_owned(),
            intent_id: "intent_0123456789abcdef".to_owned(),
            client_order_id: crate::derive_client_order_id("intent_0123456789abcdef"),
            reference_generation: "sha256:reference".to_owned(),
            account_generation: "sha256:account".to_owned(),
            account_uid_fingerprint: "uid-fingerprint".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            trade_mode: TradeMode::Cross,
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            action: ExecutionAction::Open,
            order_type: OrderType::Limit,
            size: "1".to_owned(),
            price: "0.1".to_owned(),
            open_risk: None,
        }
    }

    fn accepted_response(plan: &ExecutionPlan) -> TradeResponse<OrderOperationAck> {
        TradeResponse {
            code: "0".to_owned(),
            message: String::new(),
            data: vec![OrderOperationAck {
                order_id: "ord-1".to_owned(),
                client_order_id: plan.client_order_id.clone(),
                request_id: String::new(),
                timestamp_ms: "1790000000000".to_owned(),
                status_code: "0".to_owned(),
                status_message: String::new(),
            }],
            in_time_us: "1790000000000000".to_owned(),
            out_time_us: "1790000000001000".to_owned(),
        }
    }

    fn order_details(plan: &ExecutionPlan, state: &str) -> TradeOrderDetails {
        TradeOrderDetails {
            instrument_id: plan.instrument_id.clone(),
            order_id: "ord-1".to_owned(),
            client_order_id: plan.client_order_id.clone(),
            side: "buy".to_owned(),
            position_side: "long".to_owned(),
            trade_mode: "cross".to_owned(),
            order_type: "limit".to_owned(),
            price: plan.price.clone(),
            size: plan.size.clone(),
            accumulated_fill_size: "0".to_owned(),
            average_fill_price: String::new(),
            state: state.to_owned(),
            creation_time_ms: "1790000000000".to_owned(),
            update_time_ms: "1790000001000".to_owned(),
        }
    }

    #[tokio::test]
    async fn production_constructor_is_disabled_before_ledger_submission_or_network_send() {
        let (root, mut ledger) = ledger("disabled");
        let plan = plan();
        ledger.prepare(plan.clone(), 101).expect("prepare");
        let gateway = MockGateway::new(vec![Ok(accepted_response(&plan))], vec![]);
        let mut executor = OrderExecutor::new(ledger, gateway);

        let error = executor
            .submit_prepared(&plan.intent_id, 200, 102)
            .await
            .expect_err("disabled");

        assert!(matches!(
            error,
            OrderExecutorError::Transition(ExecutionTransitionError::LiveTradingDisabled)
        ));
        assert_eq!(executor.gateway().place_calls(), 0);
        assert_eq!(
            executor
                .ledger()
                .get(&plan.intent_id)
                .expect("entry")
                .record
                .state,
            ExecutionState::Prepared
        );

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn accepted_place_is_persisted_as_acknowledged() {
        let (root, ledger) = ledger("ack");
        let plan = plan();
        let gateway = MockGateway::new(vec![Ok(accepted_response(&plan))], vec![]);
        let mut executor = OrderExecutor::enabled_for_test(ledger, gateway);
        executor.prepare(plan.clone(), 101).expect("prepare");

        let result = executor
            .submit_prepared(&plan.intent_id, 200, 102)
            .await
            .expect("submit");

        assert!(matches!(result, SubmitDisposition::Acknowledged(_)));
        let entry = executor.ledger().get(&plan.intent_id).expect("entry");
        assert_eq!(entry.record.state, ExecutionState::Acknowledged);
        assert_eq!(entry.record.order_id.as_deref(), Some("ord-1"));
        assert_eq!(executor.gateway().place_calls(), 1);

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn transport_uncertainty_becomes_unknown_and_never_blindly_resubmits() {
        let (root, ledger) = ledger("unknown");
        let plan = plan();
        let gateway = MockGateway::new(
            vec![Err(OkxError::Response("transport uncertain".to_owned()))],
            vec![],
        );
        let mut executor = OrderExecutor::enabled_for_test(ledger, gateway);
        executor.prepare(plan.clone(), 101).expect("prepare");

        let first = executor
            .submit_prepared(&plan.intent_id, 200, 102)
            .await
            .expect("unknown");
        assert!(matches!(first, SubmitDisposition::UnknownSubmission(_)));
        assert_eq!(executor.gateway().place_calls(), 1);

        let second = executor
            .submit_prepared(&plan.intent_id, 201, 103)
            .await
            .expect_err("must not resubmit");
        assert!(matches!(
            second,
            OrderExecutorError::NotPrepared(ExecutionState::UnknownSubmission)
        ));
        assert_eq!(executor.gateway().place_calls(), 1);

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn per_item_exchange_rejection_is_durable_known_rejection() {
        let (root, ledger) = ledger("reject");
        let plan = plan();
        let response = TradeResponse {
            code: "0".to_owned(),
            message: String::new(),
            data: vec![OrderOperationAck {
                order_id: String::new(),
                client_order_id: plan.client_order_id.clone(),
                request_id: String::new(),
                timestamp_ms: "1790000000000".to_owned(),
                status_code: "51008".to_owned(),
                status_message: "insufficient balance".to_owned(),
            }],
            in_time_us: String::new(),
            out_time_us: String::new(),
        };
        let gateway = MockGateway::new(vec![Ok(response)], vec![]);
        let mut executor = OrderExecutor::enabled_for_test(ledger, gateway);
        executor.prepare(plan.clone(), 101).expect("prepare");

        let result = executor
            .submit_prepared(&plan.intent_id, 200, 102)
            .await
            .expect("rejected");
        assert!(matches!(result, SubmitDisposition::Rejected(_)));
        let entry = executor.ledger().get(&plan.intent_id).expect("entry");
        assert_eq!(entry.record.state, ExecutionState::Rejected);
        assert_eq!(entry.record.rejection_code.as_deref(), Some("51008"));

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn malformed_success_ack_is_ambiguous_not_success() {
        let (root, ledger) = ledger("malformed");
        let plan = plan();
        let mut response = accepted_response(&plan);
        response.data[0].client_order_id = "different".to_owned();
        let gateway = MockGateway::new(vec![Ok(response)], vec![]);
        let mut executor = OrderExecutor::enabled_for_test(ledger, gateway);
        executor.prepare(plan.clone(), 101).expect("prepare");

        let result = executor
            .submit_prepared(&plan.intent_id, 200, 102)
            .await
            .expect("ambiguous");
        assert!(matches!(result, SubmitDisposition::UnknownSubmission(_)));
        assert_eq!(
            executor
                .ledger()
                .get(&plan.intent_id)
                .expect("entry")
                .record
                .state,
            ExecutionState::UnknownSubmission
        );

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn unknown_submission_reconciles_only_when_exact_order_is_found() {
        let (root, mut ledger) = ledger("reconcile");
        let plan = plan();
        ledger.prepare(plan.clone(), 101).expect("prepare");
        ledger
            .begin_submission(&plan.intent_id, 102)
            .expect("submitting");
        ledger
            .mark_unknown_submission(&plan.intent_id, 103)
            .expect("unknown");

        let gateway = MockGateway::new(vec![], vec![Ok(order_details(&plan, "live"))]);
        let mut executor = OrderExecutor::enabled_for_test(ledger, gateway);
        let outcome = executor
            .reconcile(&plan.intent_id, 104)
            .await
            .expect("reconcile");

        assert!(matches!(outcome, ReconcileDisposition::Found(_)));
        assert_eq!(
            executor
                .ledger()
                .get(&plan.intent_id)
                .expect("entry")
                .record
                .state,
            ExecutionState::Live
        );
        assert_eq!(executor.gateway().lookup_calls(), 1);

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn unavailable_reconciliation_keeps_unknown_without_retry_authority() {
        let (root, mut ledger) = ledger("unavailable");
        let plan = plan();
        ledger.prepare(plan.clone(), 101).expect("prepare");
        ledger
            .begin_submission(&plan.intent_id, 102)
            .expect("submitting");
        ledger
            .mark_unknown_submission(&plan.intent_id, 103)
            .expect("unknown");

        let gateway = MockGateway::new(
            vec![],
            vec![Err(OkxError::Response("query unavailable".to_owned()))],
        );
        let mut executor = OrderExecutor::enabled_for_test(ledger, gateway);
        let outcome = executor
            .reconcile(&plan.intent_id, 104)
            .await
            .expect("unavailable");

        assert!(matches!(outcome, ReconcileDisposition::Unavailable(_)));
        assert_eq!(
            executor
                .ledger()
                .get(&plan.intent_id)
                .expect("entry")
                .record
                .state,
            ExecutionState::UnknownSubmission
        );

        let _ = fs::remove_dir_all(root);
    }
}

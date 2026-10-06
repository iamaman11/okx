use std::str::FromStr;

use async_trait::async_trait;
use okx_api::{
    AmendOrderRequest, ApiOrderSide, ApiOrderType, ApiPositionSide, ApiTradeMode,
    CancelOrderRequest, MutationTiming, OkxError, OrderOperationAck, PlaceOrderRequest,
    RateDecision, RateRequestPlan, RateThrottleEvidence, TradeApi, TradeOrderDetails,
    TradeResponse,
};
use rust_decimal::Decimal;
use thiserror::Error;

use crate::{
    DurableExecutionLedger, ExchangeOrderState, ExecutionLedgerEntry, ExecutionLedgerError,
    ExecutionPlan, ExecutionRecord, ExecutionState, ExecutionTransitionError,
    MutationPrepareDisposition, OrderMutationKind, OrderMutationRecord, OrderMutationResolution,
    OrderMutationState, OrderSide, OrderType, PositionSide, PrepareOutcome, TradeMode,
    classify_prepare_result, derive_amend_request_id, require_live_trading_enabled,
};

#[async_trait]
pub trait ExecutionGateway: Send + Sync {
    fn admit_place_order(
        &self,
        _request: &PlaceOrderRequest,
    ) -> Result<Option<RateRequestPlan>, OkxError> {
        Ok(None)
    }

    async fn place_order(
        &self,
        request: PlaceOrderRequest,
        timing: MutationTiming,
        rate_plan: Option<RateRequestPlan>,
    ) -> Result<TradeResponse<OrderOperationAck>, OkxError>;

    fn admit_amend_order(
        &self,
        _request: &AmendOrderRequest,
    ) -> Result<Option<RateRequestPlan>, OkxError> {
        Ok(None)
    }

    async fn amend_order(
        &self,
        _request: AmendOrderRequest,
        _timing: MutationTiming,
        _rate_plan: Option<RateRequestPlan>,
    ) -> Result<TradeResponse<OrderOperationAck>, OkxError> {
        Err(OkxError::Config(
            "execution gateway does not support amend_order".to_owned(),
        ))
    }

    fn admit_cancel_order(
        &self,
        _request: &CancelOrderRequest,
    ) -> Result<Option<RateRequestPlan>, OkxError> {
        Ok(None)
    }

    async fn cancel_order(
        &self,
        _request: CancelOrderRequest,
        _timing: MutationTiming,
        _rate_plan: Option<RateRequestPlan>,
    ) -> Result<TradeResponse<OrderOperationAck>, OkxError> {
        Err(OkxError::Config(
            "execution gateway does not support cancel_order".to_owned(),
        ))
    }

    async fn order_by_client_id(
        &self,
        instrument_id: String,
        client_order_id: String,
    ) -> Result<TradeOrderDetails, OkxError>;
}

#[async_trait]
impl ExecutionGateway for TradeApi {
    fn admit_place_order(
        &self,
        request: &PlaceOrderRequest,
    ) -> Result<Option<RateRequestPlan>, OkxError> {
        TradeApi::admit_place_order(self, request).map(Some)
    }

    async fn place_order(
        &self,
        request: PlaceOrderRequest,
        timing: MutationTiming,
        rate_plan: Option<RateRequestPlan>,
    ) -> Result<TradeResponse<OrderOperationAck>, OkxError> {
        match rate_plan {
            Some(rate_plan) => {
                TradeApi::place_order_after_admission(self, &request, &timing, &rate_plan).await
            }
            None => TradeApi::place_order(self, &request, &timing).await,
        }
    }

    fn admit_amend_order(
        &self,
        request: &AmendOrderRequest,
    ) -> Result<Option<RateRequestPlan>, OkxError> {
        TradeApi::admit_amend_order(self, request).map(Some)
    }

    async fn amend_order(
        &self,
        request: AmendOrderRequest,
        timing: MutationTiming,
        rate_plan: Option<RateRequestPlan>,
    ) -> Result<TradeResponse<OrderOperationAck>, OkxError> {
        match rate_plan {
            Some(rate_plan) => {
                TradeApi::amend_order_after_admission(self, &request, &timing, &rate_plan).await
            }
            None => TradeApi::amend_order(self, &request, &timing).await,
        }
    }

    fn admit_cancel_order(
        &self,
        request: &CancelOrderRequest,
    ) -> Result<Option<RateRequestPlan>, OkxError> {
        TradeApi::admit_cancel_order(self, request).map(Some)
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
        timing: MutationTiming,
        rate_plan: Option<RateRequestPlan>,
    ) -> Result<TradeResponse<OrderOperationAck>, OkxError> {
        match rate_plan {
            Some(rate_plan) => {
                TradeApi::cancel_order_after_admission(self, &request, &timing, &rate_plan).await
            }
            None => TradeApi::cancel_order(self, &request, &timing).await,
        }
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
    RateRejected {
        entry: ExecutionLedgerEntry,
        evidence: RateThrottleEvidence,
    },
    UnknownSubmission(ExecutionLedgerEntry),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationSubmitDisposition {
    Acknowledged(ExecutionLedgerEntry),
    Rejected(ExecutionLedgerEntry),
    RateRejected {
        entry: ExecutionLedgerEntry,
        evidence: RateThrottleEvidence,
    },
    Unknown(ExecutionLedgerEntry),
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

    #[error("exchange order details do not match the execution plan identity")]
    ReconciliationIdentityMismatch,

    #[error("unsupported exchange order state '{0}'")]
    UnsupportedExchangeState(String),

    #[error("mutation locally deferred by rate/backpressure policy: {evidence:?}")]
    RateDeferred { evidence: RateThrottleEvidence },

    #[error("pre-submit exchange request admission failed: {0}")]
    PreSubmit(OkxError),

    #[error("order mutation input '{0}' is invalid")]
    InvalidMutationInput(&'static str),
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

    pub const fn live_trading_enabled(&self) -> bool {
        self.live_trading_enabled
    }

    pub fn prepare(
        &mut self,
        plan: ExecutionPlan,
        observed_at_ms: u64,
    ) -> Result<PrepareOutcome, OrderExecutorError> {
        Ok(classify_prepare_result(
            self.ledger.prepare(plan, observed_at_ms),
        )?)
    }

    pub fn prepare_amend(
        &mut self,
        intent_id: &str,
        mutation_id: &str,
        new_size: Option<String>,
        new_price: Option<String>,
        observed_at_ms: u64,
    ) -> Result<MutationPrepareDisposition, OrderExecutorError> {
        let entry = self
            .ledger
            .get(intent_id)
            .cloned()
            .ok_or_else(|| ExecutionLedgerError::IntentNotFound(intent_id.to_owned()))?;
        let new_size = normalize_optional_positive_decimal("new_size", new_size)?;
        let new_price = normalize_optional_positive_decimal("new_price", new_price)?;
        if new_size.is_none() && new_price.is_none() {
            return Err(OrderExecutorError::InvalidMutationInput(
                "new_size/new_price",
            ));
        }
        if new_size.as_deref() == Some(entry.record.effective_size())
            && new_price.as_deref() == Some(entry.record.effective_price())
        {
            return Err(OrderExecutorError::InvalidMutationInput(
                "amend must change size and/or price",
            ));
        }
        let request_id = derive_amend_request_id(intent_id, mutation_id);
        let mutation = OrderMutationRecord::amend(
            mutation_id,
            request_id,
            new_size,
            new_price,
        )?;
        Ok(self
            .ledger
            .prepare_order_mutation(intent_id, mutation, observed_at_ms)?)
    }

    pub fn prepare_cancel(
        &mut self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<MutationPrepareDisposition, OrderExecutorError> {
        let mutation = OrderMutationRecord::cancel(mutation_id)?;
        Ok(self
            .ledger
            .prepare_order_mutation(intent_id, mutation, observed_at_ms)?)
    }

    pub async fn submit_prepared(
        &mut self,
        intent_id: &str,
        timing: MutationTiming,
        observed_at_ms: u64,
    ) -> Result<SubmitDisposition, OrderExecutorError> {
        // The production constructor is intentionally fail-closed. The hard
        // gate is the first operation: disabled execution must not validate,
        // persist SUBMITTING, or call the exchange gateway.
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
        let rate_plan = match self.gateway.admit_place_order(&request) {
            Ok(value) => value,
            Err(OkxError::RateLimited { evidence }) if !evidence.request_sent => {
                return Err(OrderExecutorError::RateDeferred {
                    evidence: *evidence,
                });
            }
            Err(error) => return Err(OrderExecutorError::PreSubmit(error)),
        };

        self.ledger.begin_submission(intent_id, observed_at_ms)?;

        match self.gateway.place_order(request, timing, rate_plan).await {
            Err(OkxError::RateLimited { evidence }) if evidence.request_sent => {
                let mut evidence = *evidence;
                evidence.decision = RateDecision::Rejected;
                evidence.retryable = false;
                let rejection_code = evidence
                    .exchange_code
                    .clone()
                    .unwrap_or_else(|| "RATE_LIMIT".to_owned());
                let entry = self
                    .ledger
                    .reject_known(intent_id, rejection_code, observed_at_ms)?;
                Ok(SubmitDisposition::RateRejected { entry, evidence })
            }
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

    pub async fn submit_order_mutation(
        &mut self,
        intent_id: &str,
        mutation_id: &str,
        timing: MutationTiming,
        observed_at_ms: u64,
    ) -> Result<MutationSubmitDisposition, OrderExecutorError> {
        require_live_trading_enabled(self.live_trading_enabled)?;

        let entry = self
            .ledger
            .get(intent_id)
            .cloned()
            .ok_or_else(|| ExecutionLedgerError::IntentNotFound(intent_id.to_owned()))?;
        let mutation = entry
            .record
            .mutations
            .iter()
            .find(|mutation| mutation.mutation_id == mutation_id)
            .cloned()
            .ok_or_else(|| ExecutionTransitionError::MutationNotFound(mutation_id.to_owned()))?;
        if mutation.state != OrderMutationState::Prepared {
            return Err(ExecutionTransitionError::InvalidMutationTransition {
                from: mutation.state,
                to: OrderMutationState::Submitting,
            }
            .into());
        }

        let request = order_mutation_request(&entry.record, &mutation)?;
        let rate_plan = match &request {
            OrderMutationRequest::Amend(request) => self.gateway.admit_amend_order(request),
            OrderMutationRequest::Cancel(request) => self.gateway.admit_cancel_order(request),
        };
        let rate_plan = match rate_plan {
            Ok(value) => value,
            Err(OkxError::RateLimited { evidence }) if !evidence.request_sent => {
                return Err(OrderExecutorError::RateDeferred {
                    evidence: *evidence,
                });
            }
            Err(error) => return Err(OrderExecutorError::PreSubmit(error)),
        };

        self.ledger.begin_order_mutation_submission(
            intent_id,
            mutation_id,
            observed_at_ms,
        )?;

        let result = match request {
            OrderMutationRequest::Amend(request) => {
                self.gateway.amend_order(request, timing, rate_plan).await
            }
            OrderMutationRequest::Cancel(request) => {
                self.gateway.cancel_order(request, timing, rate_plan).await
            }
        };

        match result {
            Err(OkxError::RateLimited { evidence }) if evidence.request_sent => {
                let mut evidence = *evidence;
                evidence.decision = RateDecision::Rejected;
                evidence.retryable = false;
                let rejection_code = evidence
                    .exchange_code
                    .clone()
                    .unwrap_or_else(|| "RATE_LIMIT".to_owned());
                let entry = self.ledger.reject_order_mutation(
                    intent_id,
                    mutation_id,
                    rejection_code,
                    observed_at_ms,
                )?;
                Ok(MutationSubmitDisposition::RateRejected { entry, evidence })
            }
            Err(_) => {
                let entry = self.ledger.mark_order_mutation_unknown(
                    intent_id,
                    mutation_id,
                    observed_at_ms,
                )?;
                Ok(MutationSubmitDisposition::Unknown(entry))
            }
            Ok(response) => match classify_mutation_response(&entry.record, &mutation, response) {
                MutationResponse::Acknowledged => {
                    let entry = self.ledger.acknowledge_order_mutation(
                        intent_id,
                        mutation_id,
                        observed_at_ms,
                    )?;
                    Ok(MutationSubmitDisposition::Acknowledged(entry))
                }
                MutationResponse::Rejected(code) => {
                    let entry = self.ledger.reject_order_mutation(
                        intent_id,
                        mutation_id,
                        code,
                        observed_at_ms,
                    )?;
                    Ok(MutationSubmitDisposition::Rejected(entry))
                }
                MutationResponse::Ambiguous => {
                    let entry = self.ledger.mark_order_mutation_unknown(
                        intent_id,
                        mutation_id,
                        observed_at_ms,
                    )?;
                    Ok(MutationSubmitDisposition::Unknown(entry))
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

        let state = map_exchange_state(&order.state)?;
        let mutation_resolution = validate_order_identity(&entry.record, &order, state)?;
        let entry = self.ledger.reconcile_found_with_mutation_resolution(
            intent_id,
            order.order_id,
            state,
            mutation_resolution,
            observed_at_ms,
        )?;
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

enum OrderMutationRequest {
    Amend(AmendOrderRequest),
    Cancel(CancelOrderRequest),
}

enum MutationResponse {
    Acknowledged,
    Rejected(String),
    Ambiguous,
}

fn order_mutation_request(
    record: &ExecutionRecord,
    mutation: &OrderMutationRecord,
) -> Result<OrderMutationRequest, OrderExecutorError> {
    match mutation.kind {
        OrderMutationKind::Amend => Ok(OrderMutationRequest::Amend(AmendOrderRequest {
            instrument_id: record.plan.instrument_id.clone(),
            client_order_id: record.plan.client_order_id.clone(),
            request_id: mutation
                .request_id
                .clone()
                .ok_or(OrderExecutorError::InvalidMutationInput("request_id"))?,
            cancel_on_fail: false,
            new_size: mutation.new_size.clone(),
            new_price: mutation.new_price.clone(),
        })),
        OrderMutationKind::Cancel => Ok(OrderMutationRequest::Cancel(CancelOrderRequest {
            instrument_id: record.plan.instrument_id.clone(),
            client_order_id: record.plan.client_order_id.clone(),
        })),
    }
}

fn classify_mutation_response(
    record: &ExecutionRecord,
    mutation: &OrderMutationRecord,
    response: TradeResponse<OrderOperationAck>,
) -> MutationResponse {
    if !response.top_level_success() {
        return if response.code.trim().is_empty() {
            MutationResponse::Ambiguous
        } else {
            MutationResponse::Rejected(response.code)
        };
    }
    let [item] = response.data.as_slice() else {
        return MutationResponse::Ambiguous;
    };
    if item.client_order_id != record.plan.client_order_id {
        return MutationResponse::Ambiguous;
    }
    if mutation.kind == OrderMutationKind::Amend
        && item.request_id
            != mutation.request_id.as_deref().unwrap_or_default()
    {
        return MutationResponse::Ambiguous;
    }
    if !item.accepted() {
        return if item.status_code.trim().is_empty() {
            MutationResponse::Ambiguous
        } else {
            MutationResponse::Rejected(item.status_code.clone())
        };
    }
    MutationResponse::Acknowledged
}

fn normalize_optional_positive_decimal(
    field: &'static str,
    value: Option<String>,
) -> Result<Option<String>, OrderExecutorError> {
    value
        .map(|value| {
            let parsed = Decimal::from_str(value.trim())
                .map_err(|_| OrderExecutorError::InvalidMutationInput(field))?;
            if parsed <= Decimal::ZERO {
                return Err(OrderExecutorError::InvalidMutationInput(field));
            }
            Ok(parsed.normalize().to_string())
        })
        .transpose()
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
    record: &ExecutionRecord,
    order: &TradeOrderDetails,
    exchange_state: ExchangeOrderState,
) -> Result<OrderMutationResolution, OrderExecutorError> {
    let plan = &record.plan;
    if order.order_id.trim().is_empty()
        || order.instrument_id != plan.instrument_id
        || order.client_order_id != plan.client_order_id
        || order.side != order_side_text(plan.side)
        || order.position_side != position_side_text(plan.position_side)
        || order.trade_mode != trade_mode_text(plan.trade_mode)
        || order.order_type != order_type_text(plan.order_type)
    {
        return Err(OrderExecutorError::ReconciliationIdentityMismatch);
    }

    let order_price = Decimal::from_str(&order.price)
        .map_err(|_| OrderExecutorError::ReconciliationIdentityMismatch)?;
    let order_size = Decimal::from_str(&order.size)
        .map_err(|_| OrderExecutorError::ReconciliationIdentityMismatch)?;
    let filled = Decimal::from_str(&order.accumulated_fill_size)
        .map_err(|_| OrderExecutorError::ReconciliationIdentityMismatch)?;

    let current_price = Decimal::from_str(record.effective_price())
        .map_err(|_| OrderExecutorError::ReconciliationIdentityMismatch)?;
    let current_size = Decimal::from_str(record.effective_size())
        .map_err(|_| OrderExecutorError::ReconciliationIdentityMismatch)?;
    if filled < Decimal::ZERO || filled > order_size {
        return Err(OrderExecutorError::ReconciliationIdentityMismatch);
    }

    let Some(mutation) = record.active_mutation() else {
        if order_price != current_price || order_size != current_size {
            return Err(OrderExecutorError::ReconciliationIdentityMismatch);
        }
        return Ok(OrderMutationResolution::Pending);
    };

    if mutation.kind == OrderMutationKind::Cancel {
        if order_price != current_price || order_size != current_size {
            return Err(OrderExecutorError::ReconciliationIdentityMismatch);
        }
        return Ok(match exchange_state {
            ExchangeOrderState::Canceled => OrderMutationResolution::Applied,
            ExchangeOrderState::Filled => OrderMutationResolution::Superseded,
            ExchangeOrderState::Live | ExchangeOrderState::PartiallyFilled => {
                OrderMutationResolution::Pending
            }
        });
    }

    if mutation.state == OrderMutationState::Prepared {
        if order_price != current_price || order_size != current_size {
            return Err(OrderExecutorError::ReconciliationIdentityMismatch);
        }
        return Ok(if matches!(
            exchange_state,
            ExchangeOrderState::Filled | ExchangeOrderState::Canceled
        ) {
            OrderMutationResolution::Superseded
        } else {
            OrderMutationResolution::Pending
        });
    }

    let requested_price = mutation
        .new_price
        .as_deref()
        .map(Decimal::from_str)
        .transpose()
        .map_err(|_| OrderExecutorError::ReconciliationIdentityMismatch)?
        .unwrap_or(current_price);
    let requested_size = mutation
        .new_size
        .as_deref()
        .map(Decimal::from_str)
        .transpose()
        .map_err(|_| OrderExecutorError::ReconciliationIdentityMismatch)?
        .unwrap_or(current_size);

    if order_price == requested_price && order_size == requested_size {
        Ok(OrderMutationResolution::Applied)
    } else if order_price == current_price && order_size == current_size {
        Ok(if matches!(
            exchange_state,
            ExchangeOrderState::Filled | ExchangeOrderState::Canceled
        ) {
            OrderMutationResolution::Superseded
        } else {
            OrderMutationResolution::Pending
        })
    } else {
        Err(OrderExecutorError::ReconciliationIdentityMismatch)
    }
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

    use okx_api::{
        GENERAL_RATE_LIMIT_CODE, OrderOperationAck, RateDecision, RateDomainEvidence,
        RateDomainKind, RateOperationClass, RateThrottleEvidence, RateThrottleSource,
        TradeOrderDetails, TradeResponse,
    };

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
            _timing: MutationTiming,
            _rate_plan: Option<RateRequestPlan>,
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

    struct LocalDeferredGateway {
        place_calls: AtomicUsize,
    }

    #[async_trait]
    impl ExecutionGateway for LocalDeferredGateway {
        fn admit_place_order(
            &self,
            _request: &PlaceOrderRequest,
        ) -> Result<Option<RateRequestPlan>, OkxError> {
            Err(OkxError::RateLimited {
                evidence: Box::new(local_rate_throttle()),
            })
        }

        async fn place_order(
            &self,
            _request: PlaceOrderRequest,
            _timing: MutationTiming,
            _rate_plan: Option<RateRequestPlan>,
        ) -> Result<TradeResponse<OrderOperationAck>, OkxError> {
            self.place_calls.fetch_add(1, Ordering::SeqCst);
            panic!("locally deferred mutation must not reach gateway send")
        }

        async fn order_by_client_id(
            &self,
            _instrument_id: String,
            _client_order_id: String,
        ) -> Result<TradeOrderDetails, OkxError> {
            panic!("not used")
        }
    }

    fn local_rate_throttle() -> RateThrottleEvidence {
        RateThrottleEvidence {
            schema: okx_api::RATE_THROTTLE_SCHEMA_V1,
            source: RateThrottleSource::LocalBudget,
            exchange_code: None,
            operation: RateOperationClass::PlaceOrder,
            domain: Box::new(RateDomainEvidence {
                kind: RateDomainKind::TradePlaceInstrument,
                endpoint: Some("/api/v5/trade/order".to_owned()),
                scope: Some("DOGE-USDT-SWAP".to_owned()),
                local_max_requests: 60,
                local_window_ms: 2_000,
            }),
            attempt_count: 1,
            local_defer_ms: 250,
            server_retry_after_ms: None,
            request_sent: false,
            retryable: true,
            decision: RateDecision::Deferred,
        }
    }

    fn exchange_rate_throttle() -> RateThrottleEvidence {
        RateThrottleEvidence {
            schema: okx_api::RATE_THROTTLE_SCHEMA_V1,
            source: RateThrottleSource::Exchange,
            exchange_code: Some(GENERAL_RATE_LIMIT_CODE.to_owned()),
            operation: RateOperationClass::PlaceOrder,
            domain: Box::new(RateDomainEvidence {
                kind: RateDomainKind::TradePlaceInstrument,
                endpoint: Some("/api/v5/trade/order".to_owned()),
                scope: Some("DOGE-USDT-SWAP".to_owned()),
                local_max_requests: 60,
                local_window_ms: 2_000,
            }),
            attempt_count: 1,
            local_defer_ms: 2_000,
            server_retry_after_ms: None,
            request_sent: true,
            retryable: true,
            decision: RateDecision::Deferred,
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
            risk_binding: None,
        }
    }

    fn timing() -> MutationTiming {
        MutationTiming::from_exchange_time_ms(1790000000000, 5_000).expect("timing")
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
            .submit_prepared(&plan.intent_id, timing(), 102)
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
    async fn local_rate_defer_keeps_prepared_and_never_sends() {
        let (root, ledger) = ledger("local-rate-defer");
        let plan = plan();
        let gateway = LocalDeferredGateway {
            place_calls: AtomicUsize::new(0),
        };
        let mut executor = OrderExecutor::enabled_for_test(ledger, gateway);
        executor.prepare(plan.clone(), 101).expect("prepare");

        let error = executor
            .submit_prepared(&plan.intent_id, timing(), 102)
            .await
            .expect_err("local defer");

        let OrderExecutorError::RateDeferred { evidence } = error else {
            panic!("expected typed local rate defer");
        };
        assert!(!evidence.request_sent);
        assert!(evidence.retryable);
        assert_eq!(evidence.decision, RateDecision::Deferred);
        assert_eq!(executor.gateway().place_calls.load(Ordering::SeqCst), 0);
        let entry = executor.ledger().get(&plan.intent_id).expect("entry");
        assert_eq!(entry.record.state, ExecutionState::Prepared);
        assert_eq!(entry.updated_at_ms, 101);

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn explicit_exchange_rate_rejection_is_known_and_never_unknown_submission() {
        let (root, ledger) = ledger("exchange-rate-reject");
        let plan = plan();
        let gateway = MockGateway::new(
            vec![Err(OkxError::RateLimited {
                evidence: Box::new(exchange_rate_throttle()),
            })],
            vec![],
        );
        let mut executor = OrderExecutor::enabled_for_test(ledger, gateway);
        executor.prepare(plan.clone(), 101).expect("prepare");

        let result = executor
            .submit_prepared(&plan.intent_id, timing(), 102)
            .await
            .expect("known rate rejection");

        let SubmitDisposition::RateRejected { entry, evidence } = result else {
            panic!("expected rate rejection");
        };
        assert_eq!(entry.record.state, ExecutionState::Rejected);
        assert_eq!(
            entry.record.rejection_code.as_deref(),
            Some(GENERAL_RATE_LIMIT_CODE)
        );
        assert_eq!(evidence.decision, RateDecision::Rejected);
        assert!(!evidence.retryable);
        assert!(evidence.request_sent);
        assert_eq!(
            evidence.exchange_code.as_deref(),
            Some(GENERAL_RATE_LIMIT_CODE)
        );
        assert_eq!(executor.gateway().place_calls(), 1);

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
            .submit_prepared(&plan.intent_id, timing(), 102)
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
            .submit_prepared(&plan.intent_id, timing(), 102)
            .await
            .expect("unknown");
        assert!(matches!(first, SubmitDisposition::UnknownSubmission(_)));
        assert_eq!(executor.gateway().place_calls(), 1);

        let second = executor
            .submit_prepared(&plan.intent_id, timing(), 103)
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
            .submit_prepared(&plan.intent_id, timing(), 102)
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
            .submit_prepared(&plan.intent_id, timing(), 102)
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
    async fn reconciliation_rejects_same_client_id_with_mismatched_order_shape() {
        let (root, mut ledger) = ledger("identity-mismatch");
        let plan = plan();
        ledger.prepare(plan.clone(), 101).expect("prepare");
        ledger
            .begin_submission(&plan.intent_id, 102)
            .expect("submitting");
        ledger
            .mark_unknown_submission(&plan.intent_id, 103)
            .expect("unknown");

        let mut mismatched = order_details(&plan, "live");
        mismatched.side = "sell".to_owned();
        let gateway = MockGateway::new(vec![], vec![Ok(mismatched)]);
        let mut executor = OrderExecutor::enabled_for_test(ledger, gateway);

        let error = executor
            .reconcile(&plan.intent_id, 104)
            .await
            .expect_err("identity mismatch");
        assert!(matches!(
            error,
            OrderExecutorError::ReconciliationIdentityMismatch
        ));
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
    async fn reconciliation_compares_price_and_size_numerically() {
        let (root, mut ledger) = ledger("numeric-identity");
        let plan = plan();
        ledger.prepare(plan.clone(), 101).expect("prepare");
        ledger
            .begin_submission(&plan.intent_id, 102)
            .expect("submitting");
        ledger
            .mark_unknown_submission(&plan.intent_id, 103)
            .expect("unknown");

        let mut details = order_details(&plan, "live");
        details.price = "0.1000".to_owned();
        details.size = "1.000".to_owned();
        let gateway = MockGateway::new(vec![], vec![Ok(details)]);
        let mut executor = OrderExecutor::enabled_for_test(ledger, gateway);

        let outcome = executor
            .reconcile(&plan.intent_id, 104)
            .await
            .expect("numeric equivalence");
        assert!(matches!(outcome, ReconcileDisposition::Found(_)));

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

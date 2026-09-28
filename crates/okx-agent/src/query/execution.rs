use okx_analysis::{
    CandidateOrderAssumptions, LiquidityRole as AnalysisLiquidityRole, PositionDirection,
    analyze_candidate_order,
};
use okx_execution::{
    EXECUTION_STATUS_SCHEMA_V1, ExecutionAction, ExecutionIntent, ExecutionTransitionError,
    OrderExecutorError, OrderType, PositionSide as ExecutionPositionSide, PrepareFailure,
    PrepareOutcome, PrepareRejection, TradeMode, prepare_execution, revalidate_execution_plan,
};
use okx_protocol::{
    ExecutionOrderType, ExecutionTradeMode, LiquidityRole as ProtocolLiquidityRole,
    PositionSide as ProtocolPositionSide,
};

use super::*;
use crate::execution_runtime::{
    EXECUTION_PREPARED_SCHEMA_V1, PreparedDisposition, PreparedExecutionResult,
};

pub const EXECUTION_PREFLIGHT_REJECTED_CODE: &str = "EXECUTION_PREFLIGHT_REJECTED";
pub const EXECUTION_PREFLIGHT_UNAVAILABLE_CODE: &str = "EXECUTION_PREFLIGHT_UNAVAILABLE";
pub const EXECUTION_RUNTIME_UNAVAILABLE_CODE: &str = "EXECUTION_RUNTIME_UNAVAILABLE";
pub const EXECUTION_ACCOUNT_NOT_FRESH_CODE: &str = "EXECUTION_ACCOUNT_NOT_FRESH";
pub const EXECUTION_INPUT_INCONSISTENT_CODE: &str = "EXECUTION_INPUT_INCONSISTENT";
pub const EXECUTION_RECORD_NOT_FOUND_CODE: &str = "EXECUTION_RECORD_NOT_FOUND";
pub const EXECUTION_INTENT_CONFLICT_CODE: &str = "EXECUTION_INTENT_CONFLICT";
pub const EXECUTION_IDEMPOTENCY_COLLISION_CODE: &str = "EXECUTION_IDEMPOTENCY_COLLISION";
pub const EXECUTION_LEDGER_CAPACITY_EXHAUSTED_CODE: &str = "EXECUTION_LEDGER_CAPACITY_EXHAUSTED";
pub const LIVE_TRADING_DISABLED_CODE: &str = "LIVE_TRADING_DISABLED";
pub const EXECUTION_GATE_INVARIANT_CODE: &str = "EXECUTION_GATE_INVARIANT_VIOLATION";

pub(super) async fn dispatch(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
) -> AgentResult<AgentResponse> {
    match &request.operation {
        AgentOperation::ExecutorPreflight => {
            executor_preflight(request, context, generated_at).await
        }
        AgentOperation::PrepareOpenExecution {
            intent_id,
            instrument,
            trade_mode,
            position_side,
            order_type,
            entry_price,
            stop_price,
            max_settle_notional,
            max_loss_settle,
            target_rr,
            entry_liquidity_role,
            exit_liquidity_role,
        } => {
            let Some(execution) = context.execution else {
                return Ok(execution_unavailable(request, generated_at));
            };
            let account = match fresh_account(request, context, generated_at).await? {
                FreshAccount::Ready(value) => value,
                FreshAccount::Response(response) => return Ok(*response),
            };
            let preflight = match executor_preflight_check(
                request,
                generated_at,
                execution,
                &account,
            )
            .await?
            {
                ExecutorPreflightCheck::Ready(value) => value,
                ExecutorPreflightCheck::Response(response) => return Ok(*response),
            };
            if !preflight.accepted {
                return Ok(preflight_rejected(request, generated_at));
            }
            let Some(rules) = current_rules(context, instrument).await else {
                return Ok(reference_not_found(request, generated_at, instrument));
            };
            let Some(observer) = context.account_fallback else {
                return Ok(execution_unavailable(request, generated_at));
            };
            let fees = match observer.fee_schedule(&rules).await {
                Ok(value) => value,
                Err(error) => return Ok(fee_schedule_failure(request, generated_at, error)),
            };
            let direction = match position_side {
                ProtocolPositionSide::Long => PositionDirection::Long,
                ProtocolPositionSide::Short => PositionDirection::Short,
            };
            let candidate = match analyze_candidate_order(
                &rules,
                &fees,
                &CandidateOrderAssumptions {
                    direction,
                    entry_price: entry_price.clone(),
                    stop_price: stop_price.clone(),
                    max_settle_notional: max_settle_notional.clone(),
                    max_loss_settle: max_loss_settle.clone(),
                    target_rr: target_rr.clone(),
                    entry_liquidity_role: analysis_liquidity_role(*entry_liquidity_role),
                    exit_liquidity_role: analysis_liquidity_role(*exit_liquidity_role),
                },
            ) {
                Ok(value) => value,
                Err(error) => {
                    return Ok(analysis_failure(
                        request,
                        generated_at,
                        AgentResponseStatus::Rejected,
                        error,
                    ));
                }
            };

            let intent = ExecutionIntent {
                intent_id: intent_id.clone(),
                expected_reference_generation: rules.reference_generation.clone(),
                expected_account_generation: account.account_generation.clone(),
                instrument_id: instrument.clone(),
                trade_mode: execution_trade_mode(*trade_mode),
                position_side: execution_position_side(*position_side),
                action: ExecutionAction::Open,
                order_type: execution_order_type(*order_type),
                size: candidate.contracts.clone(),
                price: candidate.entry_price.clone(),
            };
            let plan = match prepare_execution(&intent, &rules, &account, Some(&candidate)) {
                Ok(value) => value,
                Err(error) => return Ok(validation_failure(request, generated_at, error)),
            };
            let outcome = execution.prepare(plan, utc_now_ms()).await?;
            prepare_outcome_response(request, generated_at, outcome)
        }
        AgentOperation::PrepareCloseExecution {
            intent_id,
            instrument,
            trade_mode,
            position_side,
            order_type,
            size,
            price,
        } => {
            let Some(execution) = context.execution else {
                return Ok(execution_unavailable(request, generated_at));
            };
            let account = match fresh_account(request, context, generated_at).await? {
                FreshAccount::Ready(value) => value,
                FreshAccount::Response(response) => return Ok(*response),
            };
            let preflight = match executor_preflight_check(
                request,
                generated_at,
                execution,
                &account,
            )
            .await?
            {
                ExecutorPreflightCheck::Ready(value) => value,
                ExecutorPreflightCheck::Response(response) => return Ok(*response),
            };
            if !preflight.accepted {
                return Ok(preflight_rejected(request, generated_at));
            }
            let Some(rules) = current_rules(context, instrument).await else {
                return Ok(reference_not_found(request, generated_at, instrument));
            };
            let intent = ExecutionIntent {
                intent_id: intent_id.clone(),
                expected_reference_generation: rules.reference_generation.clone(),
                expected_account_generation: account.account_generation.clone(),
                instrument_id: instrument.clone(),
                trade_mode: execution_trade_mode(*trade_mode),
                position_side: execution_position_side(*position_side),
                action: ExecutionAction::Close,
                order_type: execution_order_type(*order_type),
                size: size.clone(),
                price: price.clone(),
            };
            let plan = match prepare_execution(&intent, &rules, &account, None) {
                Ok(value) => value,
                Err(error) => return Ok(validation_failure(request, generated_at, error)),
            };
            let outcome = execution.prepare(plan, utc_now_ms()).await?;
            prepare_outcome_response(request, generated_at, outcome)
        }
        AgentOperation::SubmitPreparedExecution { intent_id } => {
            submit_prepared(request, context, generated_at, intent_id).await
        }
        AgentOperation::ExecutionStatus { intent_id } => {
            execution_status_response(request, context, generated_at, intent_id).await
        }
        _ => unreachable!("execution dispatcher received unsupported operation"),
    }
}

enum ExecutorPreflightCheck {
    Ready(crate::execution_preflight::ExecutorCredentialPreflight),
    Response(Box<AgentResponse>),
}

async fn executor_preflight_check(
    request: &AgentRequest,
    generated_at: &str,
    execution: &crate::execution_runtime::ExecutionRuntime,
    account: &AccountSnapshot,
) -> AgentResult<ExecutorPreflightCheck> {
    match execution.preflight(account).await {
        Ok(value) => Ok(ExecutorPreflightCheck::Ready(value)),
        Err(crate::AgentError::Okx(error)) => Ok(ExecutorPreflightCheck::Response(Box::new(
            failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                EXECUTION_PREFLIGHT_UNAVAILABLE_CODE,
                error.to_string(),
                true,
            ),
        ))),
        Err(error) => Err(error),
    }
}

async fn executor_preflight(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
) -> AgentResult<AgentResponse> {
    let Some(execution) = context.execution else {
        return Ok(execution_unavailable(request, generated_at));
    };
    let account = match fresh_account(request, context, generated_at).await? {
        FreshAccount::Ready(value) => value,
        FreshAccount::Response(response) => return Ok(*response),
    };
    let evidence =
        match executor_preflight_check(request, generated_at, execution, &account).await? {
            ExecutorPreflightCheck::Ready(value) => value,
            ExecutorPreflightCheck::Response(response) => return Ok(*response),
        };
    Ok(completed(
        request,
        generated_at,
        crate::execution_preflight::EXECUTOR_CREDENTIAL_PREFLIGHT_SCHEMA_V1,
        serde_json::to_value(evidence)?,
    ))
}

async fn submit_prepared(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    intent_id: &str,
) -> AgentResult<AgentResponse> {
    let Some(execution) = context.execution else {
        return Ok(execution_unavailable(request, generated_at));
    };
    let Some(entry) = execution.entry(intent_id).await else {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_RECORD_NOT_FOUND_CODE,
            "prepared execution record was not found".to_owned(),
            false,
        ));
    };

    let account = match fresh_account(request, context, generated_at).await? {
        FreshAccount::Ready(value) => value,
        FreshAccount::Response(response) => return Ok(*response),
    };
    let preflight =
        match executor_preflight_check(request, generated_at, execution, &account).await? {
            ExecutorPreflightCheck::Ready(value) => value,
            ExecutorPreflightCheck::Response(response) => return Ok(*response),
        };
    if !preflight.accepted {
        return Ok(preflight_rejected(request, generated_at));
    }
    let plan = entry.record.plan;
    let Some(rules) = current_rules(context, &plan.instrument_id).await else {
        return Ok(reference_not_found(
            request,
            generated_at,
            &plan.instrument_id,
        ));
    };

    let current_fee_generation = if plan.action == ExecutionAction::Open {
        let Some(observer) = context.account_fallback else {
            return Ok(execution_unavailable(request, generated_at));
        };
        let fees = match observer.fee_schedule(&rules).await {
            Ok(value) => value,
            Err(error) => return Ok(fee_schedule_failure(request, generated_at, error)),
        };
        Some(fees.fee_generation)
    } else {
        None
    };

    if let Err(error) =
        revalidate_execution_plan(&plan, &rules, &account, current_fee_generation.as_deref())
    {
        return Ok(validation_failure(request, generated_at, error));
    }

    let observed_at_ms = utc_now_ms();
    let exp_time_ms = observed_at_ms.saturating_add(5_000);
    match execution
        .submit_prepared(intent_id, exp_time_ms, observed_at_ms)
        .await
    {
        Err(OrderExecutorError::Transition(ExecutionTransitionError::LiveTradingDisabled)) => {
            Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                LIVE_TRADING_DISABLED_CODE,
                "live trading is disabled before SUBMITTING persistence and before exchange send"
                    .to_owned(),
                false,
            ))
        }
        Err(error) => Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_INPUT_INCONSISTENT_CODE,
            error.to_string(),
            false,
        )),
        Ok(_) => Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            EXECUTION_GATE_INVARIANT_CODE,
            "production execution unexpectedly passed the hard disabled gate".to_owned(),
            false,
        )),
    }
}

enum FreshAccount {
    Ready(Box<AccountSnapshot>),
    Response(Box<AgentResponse>),
}

async fn fresh_account(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
) -> AgentResult<FreshAccount> {
    let assembled = match assemble_account_snapshot(context).await {
        Ok(value) => value,
        Err(error) => {
            return Ok(FreshAccount::Response(Box::new(account_query_failure(
                request,
                generated_at,
                error,
            ))));
        }
    };
    if assembled.quality != DataQuality::Fresh {
        return Ok(FreshAccount::Response(Box::new(account_not_fresh(
            request,
            generated_at,
        ))));
    }
    Ok(FreshAccount::Ready(Box::new(assembled.snapshot)))
}

async fn current_rules(
    context: ObservationQueryContext<'_>,
    instrument: &str,
) -> Option<InstrumentRulesSnapshot> {
    match context.public_ws {
        Some(public_ws) => public_ws.instrument_rules(instrument).await,
        None => None,
    }
}

fn completed(
    request: &AgentRequest,
    generated_at: &str,
    result_schema: &str,
    result: serde_json::Value,
) -> AgentResponse {
    AgentResponse {
        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
        request_id: request.request_id.clone(),
        status: AgentResponseStatus::Completed,
        generated_at: generated_at.to_owned(),
        quality: DataQuality::Fresh,
        result_schema: Some(result_schema.to_owned()),
        result: Some(result),
        failure: None,
        warnings: Vec::new(),
    }
}

fn execution_unavailable(request: &AgentRequest, generated_at: &str) -> AgentResponse {
    failure_response(
        request,
        generated_at,
        AgentResponseStatus::Rejected,
        EXECUTION_RUNTIME_UNAVAILABLE_CODE,
        "executor credential/runtime is not provisioned".to_owned(),
        false,
    )
}

fn account_not_fresh(request: &AgentRequest, generated_at: &str) -> AgentResponse {
    failure_response(
        request,
        generated_at,
        AgentResponseStatus::Rejected,
        EXECUTION_ACCOUNT_NOT_FRESH_CODE,
        "execution requires a private-WS-converged current account snapshot".to_owned(),
        true,
    )
}

fn preflight_rejected(request: &AgentRequest, generated_at: &str) -> AgentResponse {
    failure_response(
        request,
        generated_at,
        AgentResponseStatus::Rejected,
        EXECUTION_PREFLIGHT_REJECTED_CODE,
        "executor credential/account preflight did not satisfy the production safety contract"
            .to_owned(),
        false,
    )
}

fn validation_failure(
    request: &AgentRequest,
    generated_at: &str,
    error: okx_execution::ExecutionValidationError,
) -> AgentResponse {
    failure_response(
        request,
        generated_at,
        AgentResponseStatus::Rejected,
        EXECUTION_INPUT_INCONSISTENT_CODE,
        error.to_string(),
        false,
    )
}

fn prepare_outcome_response(
    request: &AgentRequest,
    generated_at: &str,
    outcome: PrepareOutcome,
) -> AgentResult<AgentResponse> {
    match outcome {
        PrepareOutcome::Created(entry) => Ok(completed(
            request,
            generated_at,
            EXECUTION_PREPARED_SCHEMA_V1,
            serde_json::to_value(PreparedExecutionResult {
                schema: EXECUTION_PREPARED_SCHEMA_V1,
                disposition: PreparedDisposition::Created,
                entry,
            })?,
        )),
        PrepareOutcome::Existing(entry) => Ok(completed(
            request,
            generated_at,
            EXECUTION_PREPARED_SCHEMA_V1,
            serde_json::to_value(PreparedExecutionResult {
                schema: EXECUTION_PREPARED_SCHEMA_V1,
                disposition: PreparedDisposition::Existing,
                entry,
            })?,
        )),
        PrepareOutcome::Rejected(PrepareRejection::IntentConflict) => Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_INTENT_CONFLICT_CODE,
            "intent_id already exists with a different immutable execution plan".to_owned(),
            false,
        )),
        PrepareOutcome::Rejected(PrepareRejection::ClientOrderIdCollision) => Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_IDEMPOTENCY_COLLISION_CODE,
            "derived client_order_id collides with an existing execution record".to_owned(),
            false,
        )),
        PrepareOutcome::Failed(PrepareFailure::CapacityExceeded { limit }) => Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            EXECUTION_LEDGER_CAPACITY_EXHAUSTED_CODE,
            format!("execution ledger capacity of {limit} records is exhausted"),
            false,
        )),
    }
}

async fn execution_status_response(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    intent_id: &str,
) -> AgentResult<AgentResponse> {
    let Some(execution) = context.execution else {
        return Ok(execution_unavailable(request, generated_at));
    };
    let Some(status) = execution.status(intent_id).await? else {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_RECORD_NOT_FOUND_CODE,
            "execution record was not found".to_owned(),
            false,
        ));
    };

    Ok(completed(
        request,
        generated_at,
        EXECUTION_STATUS_SCHEMA_V1,
        serde_json::to_value(status)?,
    ))
}

const fn execution_trade_mode(value: ExecutionTradeMode) -> TradeMode {
    match value {
        ExecutionTradeMode::Cross => TradeMode::Cross,
        ExecutionTradeMode::Isolated => TradeMode::Isolated,
    }
}

const fn execution_position_side(value: ProtocolPositionSide) -> ExecutionPositionSide {
    match value {
        ProtocolPositionSide::Long => ExecutionPositionSide::Long,
        ProtocolPositionSide::Short => ExecutionPositionSide::Short,
    }
}

const fn execution_order_type(value: ExecutionOrderType) -> OrderType {
    match value {
        ExecutionOrderType::Limit => OrderType::Limit,
        ExecutionOrderType::PostOnly => OrderType::PostOnly,
        ExecutionOrderType::Fok => OrderType::Fok,
        ExecutionOrderType::Ioc => OrderType::Ioc,
    }
}

const fn analysis_liquidity_role(value: ProtocolLiquidityRole) -> AnalysisLiquidityRole {
    match value {
        ProtocolLiquidityRole::Maker => AnalysisLiquidityRole::Maker,
        ProtocolLiquidityRole::Taker => AnalysisLiquidityRole::Taker,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_execution::{
        EXECUTION_PLAN_SCHEMA_V1, ExecutionLedgerEntry, ExecutionRecord, OrderSide,
        derive_client_order_id,
    };
    use okx_protocol::AGENT_REQUEST_SCHEMA_V1;

    const GENERATED_AT: &str = "2026-09-29T00:00:00.000Z";

    fn request() -> AgentRequest {
        AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_prepare_outcome_012345".to_owned(),
            operation: AgentOperation::ExecutionStatus {
                intent_id: "intent_prepare_outcome_01".to_owned(),
            },
        }
    }

    fn entry() -> ExecutionLedgerEntry {
        let intent_id = "intent_prepare_outcome_01";
        ExecutionLedgerEntry {
            record: ExecutionRecord::new(okx_execution::ExecutionPlan {
                schema: EXECUTION_PLAN_SCHEMA_V1.to_owned(),
                intent_id: intent_id.to_owned(),
                client_order_id: derive_client_order_id(intent_id),
                reference_generation: "sha256:reference".to_owned(),
                account_generation: "sha256:account".to_owned(),
                account_uid_fingerprint: "uid-fingerprint".to_owned(),
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                trade_mode: TradeMode::Cross,
                side: OrderSide::Buy,
                position_side: ExecutionPositionSide::Long,
                action: ExecutionAction::Open,
                order_type: OrderType::Limit,
                size: "0.05".to_owned(),
                price: "0.09317".to_owned(),
                open_risk: None,
            }),
            created_at_ms: 100,
            updated_at_ms: 100,
        }
    }

    #[test]
    fn every_prepare_domain_outcome_maps_to_terminal_response() {
        let cases = [
            (
                PrepareOutcome::Rejected(PrepareRejection::IntentConflict),
                AgentResponseStatus::Rejected,
                EXECUTION_INTENT_CONFLICT_CODE,
            ),
            (
                PrepareOutcome::Rejected(PrepareRejection::ClientOrderIdCollision),
                AgentResponseStatus::Rejected,
                EXECUTION_IDEMPOTENCY_COLLISION_CODE,
            ),
            (
                PrepareOutcome::Failed(PrepareFailure::CapacityExceeded { limit: 10_000 }),
                AgentResponseStatus::Failed,
                EXECUTION_LEDGER_CAPACITY_EXHAUSTED_CODE,
            ),
        ];

        for (outcome, expected_status, expected_code) in cases {
            let response =
                prepare_outcome_response(&request(), GENERATED_AT, outcome).expect("terminal");
            assert_eq!(response.status, expected_status);
            let failure = response.failure.expect("failure");
            assert_eq!(failure.code, expected_code);
            assert!(!failure.retryable);
        }
    }

    #[test]
    fn created_and_existing_prepare_outcomes_remain_completed() {
        for outcome in [
            PrepareOutcome::Created(entry()),
            PrepareOutcome::Existing(entry()),
        ] {
            let response =
                prepare_outcome_response(&request(), GENERATED_AT, outcome).expect("completed");
            assert_eq!(response.status, AgentResponseStatus::Completed);
            assert_eq!(
                response.result_schema.as_deref(),
                Some(EXECUTION_PREPARED_SCHEMA_V1)
            );
            assert!(response.failure.is_none());
        }
    }
}

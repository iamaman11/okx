use okx_analysis::{
    CandidateOrderAssumptions, HARD_RISK_POLICY_SCHEMA_V1, LiquidityRole as AnalysisLiquidityRole,
    PortfolioCandidate, PositionDirection, TRADING_MANDATE_SCHEMA_V1, analyze_candidate_order,
    analyze_portfolio_risk,
};
use okx_api::{MUTATION_REQUEST_TTL_MS, MarginMode};
use okx_execution::{
    EXECUTION_LINEAGE_SCHEMA_V1, EXECUTION_STATUS_SCHEMA_V2, ExecutionAction,
    ExecutionDecisionReference, ExecutionIntent, ExecutionLedgerError, ExecutionLineageBinding,
    ExecutionRiskBinding, ExecutionState, ExecutionTransitionError, OrderExecutorError, OrderType,
    PositionSide as ExecutionPositionSide, PrepareDeferral, PrepareFailure, PrepareOutcome,
    PrepareRejection, ReverseContinuation, ReverseLeg, TradeMode, prepare_execution,
    revalidate_execution_plan, revalidate_hard_risk_policy, revalidate_venue_execution,
};
use okx_protocol::{
    ExecutionEntryRequest, ExecutionLineageRequest, ExecutionOrderType, ExecutionPrepareSpec,
    ExecutionReferencePriceBasis, ExecutionRiskBindingRequest, ExecutionTradeMode,
    LiquidityRole as ProtocolLiquidityRole, PositionSide as ProtocolPositionSide,
};

use super::*;
use crate::execution_runtime::{
    EXECUTION_PREPARED_SCHEMA_V1, PreparedDisposition, PreparedExecutionResult,
};

pub const EXECUTION_PREFLIGHT_REJECTED_CODE: &str = "EXECUTION_PREFLIGHT_REJECTED";
pub const EXECUTION_PREFLIGHT_UNAVAILABLE_CODE: &str = "EXECUTION_PREFLIGHT_UNAVAILABLE";
pub const EXECUTION_CLOCK_UNAVAILABLE_CODE: &str = "EXECUTION_CLOCK_UNAVAILABLE";
pub const EXECUTION_CLOCK_UNSAFE_CODE: &str = "EXECUTION_CLOCK_UNSAFE";
pub const EXECUTION_RUNTIME_UNAVAILABLE_CODE: &str = "EXECUTION_RUNTIME_UNAVAILABLE";
pub const EXECUTION_ACCOUNT_NOT_FRESH_CODE: &str = "EXECUTION_ACCOUNT_NOT_FRESH";
pub const EXECUTION_REFERENCE_NOT_FRESH_CODE: &str = "EXECUTION_REFERENCE_NOT_FRESH";
pub const EXECUTION_VENUE_UNAVAILABLE_CODE: &str = "EXECUTION_VENUE_UNAVAILABLE";
pub const EXECUTION_INPUT_INCONSISTENT_CODE: &str = "EXECUTION_INPUT_INCONSISTENT";
pub const EXECUTION_RECORD_NOT_FOUND_CODE: &str = "EXECUTION_RECORD_NOT_FOUND";
pub const EXECUTION_INTENT_CONFLICT_CODE: &str = "EXECUTION_INTENT_CONFLICT";
pub const EXECUTION_IDEMPOTENCY_COLLISION_CODE: &str = "EXECUTION_IDEMPOTENCY_COLLISION";
pub const EXECUTION_INSTRUMENT_BUSY_CODE: &str = "EXECUTION_INSTRUMENT_BUSY";
pub const EXECUTION_LEDGER_CAPACITY_EXHAUSTED_CODE: &str = "EXECUTION_LEDGER_CAPACITY_EXHAUSTED";
pub const LIVE_TRADING_DISABLED_CODE: &str = "LIVE_TRADING_DISABLED";
pub const EXECUTION_GATE_INVARIANT_CODE: &str = "EXECUTION_GATE_INVARIANT_VIOLATION";
pub const EXECUTION_RISK_POLICY_REQUIRED_CODE: &str = "EXECUTION_RISK_POLICY_REQUIRED";
pub const EXECUTION_RISK_POLICY_REJECTED_CODE: &str = "EXECUTION_RISK_POLICY_REJECTED";
pub const EXECUTION_RISK_EVIDENCE_UNAVAILABLE_CODE: &str = "EXECUTION_RISK_EVIDENCE_UNAVAILABLE";
pub const EXECUTION_RISK_EVIDENCE_NOT_FRESH_CODE: &str = "EXECUTION_RISK_EVIDENCE_NOT_FRESH";

pub(super) async fn dispatch(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
) -> AgentResult<AgentResponse> {
    match &request.operation {
        AgentOperation::ExecutorPreflight => {
            executor_preflight(request, context, generated_at).await
        }
        AgentOperation::PrepareExecution {
            intent_id,
            instrument,
            trade_mode,
            order_type,
            spec,
            risk,
            lineage,
        } => {
            prepare_generic_execution(
                request,
                context,
                generated_at,
                PrepareCommon {
                    intent_id,
                    instrument,
                    trade_mode: *trade_mode,
                    order_type: *order_type,
                    risk: risk.as_deref(),
                    lineage: lineage.as_deref(),
                },
                spec,
            )
            .await
        }
        AgentOperation::SubmitPreparedExecution { intent_id } => {
            submit_prepared(request, context, generated_at, intent_id).await
        }
        AgentOperation::AbortReverseExecution { intent_id } => {
            abort_reverse_execution(request, context, generated_at, intent_id).await
        }
        AgentOperation::ExecutionStatus { intent_id } => {
            execution_status_response(request, context, generated_at, intent_id).await
        }
        _ => unreachable!("execution dispatcher received unsupported operation"),
    }
}

#[derive(Debug, Clone)]
enum PrepareTarget {
    Normal,
    ReverseClose {
        target_position_side: ExecutionPositionSide,
    },
    ReverseOpen {
        root_intent_id: String,
    },
}

#[derive(Clone, Copy)]
struct PrepareCommon<'a> {
    intent_id: &'a str,
    instrument: &'a str,
    trade_mode: ExecutionTradeMode,
    order_type: ExecutionOrderType,
    risk: Option<&'a ExecutionRiskBindingRequest>,
    lineage: Option<&'a ExecutionLineageRequest>,
}

struct EntryPrepare<'a> {
    common: PrepareCommon<'a>,
    position_side: ProtocolPositionSide,
    entry: &'a ExecutionEntryRequest,
    action: ExecutionAction,
    target: PrepareTarget,
}

struct SizedPrepare<'a> {
    common: PrepareCommon<'a>,
    position_side: ProtocolPositionSide,
    size: &'a str,
    price: &'a str,
    action: ExecutionAction,
    target: PrepareTarget,
}

async fn prepare_generic_execution(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    common: PrepareCommon<'_>,
    spec: &ExecutionPrepareSpec,
) -> AgentResult<AgentResponse> {
    match spec {
        ExecutionPrepareSpec::Open {
            position_side,
            entry,
        } => {
            prepare_risk_increasing(
                request,
                context,
                generated_at,
                EntryPrepare {
                    common,
                    position_side: *position_side,
                    entry,
                    action: ExecutionAction::Open,
                    target: PrepareTarget::Normal,
                },
            )
            .await
        }
        ExecutionPrepareSpec::Add {
            position_side,
            entry,
        } => {
            prepare_risk_increasing(
                request,
                context,
                generated_at,
                EntryPrepare {
                    common,
                    position_side: *position_side,
                    entry,
                    action: ExecutionAction::Add,
                    target: PrepareTarget::Normal,
                },
            )
            .await
        }
        ExecutionPrepareSpec::Hedge {
            position_side,
            entry,
        } => {
            prepare_risk_increasing(
                request,
                context,
                generated_at,
                EntryPrepare {
                    common,
                    position_side: *position_side,
                    entry,
                    action: ExecutionAction::Hedge,
                    target: PrepareTarget::Normal,
                },
            )
            .await
        }
        ExecutionPrepareSpec::Reduce {
            position_side,
            size,
            price,
        } => {
            prepare_risk_reducing(
                request,
                context,
                generated_at,
                SizedPrepare {
                    common,
                    position_side: *position_side,
                    size,
                    price,
                    action: ExecutionAction::Reduce,
                    target: PrepareTarget::Normal,
                },
            )
            .await
        }
        ExecutionPrepareSpec::Close {
            position_side,
            size,
            price,
        } => {
            prepare_risk_reducing(
                request,
                context,
                generated_at,
                SizedPrepare {
                    common,
                    position_side: *position_side,
                    size,
                    price,
                    action: ExecutionAction::Close,
                    target: PrepareTarget::Normal,
                },
            )
            .await
        }
        ExecutionPrepareSpec::Reverse {
            position_side,
            size,
            price,
        } => {
            let target_position_side = match position_side {
                ProtocolPositionSide::Long => ExecutionPositionSide::Short,
                ProtocolPositionSide::Short => ExecutionPositionSide::Long,
            };
            prepare_risk_reducing(
                request,
                context,
                generated_at,
                SizedPrepare {
                    common,
                    position_side: *position_side,
                    size,
                    price,
                    action: ExecutionAction::Close,
                    target: PrepareTarget::ReverseClose {
                        target_position_side,
                    },
                },
            )
            .await
        }
        ExecutionPrepareSpec::ContinueReverse { entry } => {
            prepare_reverse_open(request, context, generated_at, common, entry).await
        }
    }
}

async fn prepare_risk_increasing(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    input: EntryPrepare<'_>,
) -> AgentResult<AgentResponse> {
    let EntryPrepare {
        common,
        position_side,
        entry,
        action,
        target,
    } = input;
    debug_assert!(action.is_risk_increasing());
    let Some(execution) = context.execution else {
        return Ok(execution_unavailable(request, generated_at));
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
    let Some(rules) = current_rules(context, common.instrument).await else {
        return Ok(reference_not_found(
            request,
            generated_at,
            common.instrument,
        ));
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
            entry_price: entry.entry_price.clone(),
            stop_price: entry.stop_price.clone(),
            max_settle_notional: entry.max_settle_notional.clone(),
            max_loss_settle: entry.max_loss_settle.clone(),
            target_rr: entry.target_rr.clone(),
            entry_liquidity_role: analysis_liquidity_role(entry.entry_liquidity_role),
            exit_liquidity_role: analysis_liquidity_role(entry.exit_liquidity_role),
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
        intent_id: common.intent_id.to_owned(),
        expected_reference_generation: rules.reference_generation.clone(),
        expected_account_generation: account.account_generation.clone(),
        instrument_id: common.instrument.to_owned(),
        trade_mode: execution_trade_mode(common.trade_mode),
        position_side: execution_position_side(position_side),
        action,
        order_type: execution_order_type(common.order_type),
        size: candidate.contracts.clone(),
        price: candidate.entry_price.clone(),
    };
    let mut plan = match prepare_execution(&intent, &rules, &account, Some(&candidate)) {
        Ok(value) => value,
        Err(error) => return Ok(validation_failure(request, generated_at, error)),
    };
    plan.risk_binding = common.risk.map(execution_risk_binding);
    let lineage = common.lineage.map(execution_lineage_binding);
    commit_prepared(request, generated_at, execution, plan, lineage, target).await
}

async fn prepare_risk_reducing(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    input: SizedPrepare<'_>,
) -> AgentResult<AgentResponse> {
    let SizedPrepare {
        common,
        position_side,
        size,
        price,
        action,
        target,
    } = input;
    debug_assert!(action.is_risk_reducing());
    let Some(execution) = context.execution else {
        return Ok(execution_unavailable(request, generated_at));
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
    let Some(rules) = current_rules(context, common.instrument).await else {
        return Ok(reference_not_found(
            request,
            generated_at,
            common.instrument,
        ));
    };
    let intent = ExecutionIntent {
        intent_id: common.intent_id.to_owned(),
        expected_reference_generation: rules.reference_generation.clone(),
        expected_account_generation: account.account_generation.clone(),
        instrument_id: common.instrument.to_owned(),
        trade_mode: execution_trade_mode(common.trade_mode),
        position_side: execution_position_side(position_side),
        action,
        order_type: execution_order_type(common.order_type),
        size: size.to_owned(),
        price: price.to_owned(),
    };
    let mut plan = match prepare_execution(&intent, &rules, &account, None) {
        Ok(value) => value,
        Err(error) => return Ok(validation_failure(request, generated_at, error)),
    };
    plan.risk_binding = common.risk.map(execution_risk_binding);
    let lineage = common.lineage.map(execution_lineage_binding);
    commit_prepared(request, generated_at, execution, plan, lineage, target).await
}

async fn prepare_reverse_open(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    common: PrepareCommon<'_>,
    entry: &ExecutionEntryRequest,
) -> AgentResult<AgentResponse> {
    let root_intent_id = common.intent_id;
    let Some(execution) = context.execution else {
        return Ok(execution_unavailable(request, generated_at));
    };
    let Some(root) = execution.entry(root_intent_id).await else {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_RECORD_NOT_FOUND_CODE,
            "reverse root execution record was not found".to_owned(),
            false,
        ));
    };
    let Some(reverse) = root.record.reverse.as_ref() else {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_INPUT_INCONSISTENT_CODE,
            "execution record is not a reverse root".to_owned(),
            false,
        ));
    };
    if reverse.leg != ReverseLeg::Close
        || reverse.continuation != ReverseContinuation::Required
        || root.record.state != ExecutionState::Filled
        || root.record.plan.instrument_id != common.instrument
        || root.record.plan.trade_mode != execution_trade_mode(common.trade_mode)
    {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_INPUT_INCONSISTENT_CODE,
            "reverse root is not ready for a fresh opposite open".to_owned(),
            true,
        ));
    }
    let protocol_side = match reverse.target_position_side {
        ExecutionPositionSide::Long => ProtocolPositionSide::Long,
        ExecutionPositionSide::Short => ProtocolPositionSide::Short,
    };
    prepare_risk_increasing(
        request,
        context,
        generated_at,
        EntryPrepare {
            common: PrepareCommon {
                intent_id: &reverse.open_intent_id,
                ..common
            },
            position_side: protocol_side,
            entry,
            action: ExecutionAction::Open,
            target: PrepareTarget::ReverseOpen {
                root_intent_id: root_intent_id.to_owned(),
            },
        },
    )
    .await
}

async fn commit_prepared(
    request: &AgentRequest,
    generated_at: &str,
    execution: &crate::execution_runtime::ExecutionRuntime,
    plan: okx_execution::ExecutionPlan,
    lineage: Option<ExecutionLineageBinding>,
    target: PrepareTarget,
) -> AgentResult<AgentResponse> {
    let observed_at_ms = utc_now_ms();
    let result = match target {
        PrepareTarget::Normal => {
            execution
                .prepare_with_lineage(plan, lineage, observed_at_ms)
                .await
        }
        PrepareTarget::ReverseClose {
            target_position_side,
        } => {
            execution
                .prepare_reverse_close_with_lineage(
                    plan,
                    target_position_side,
                    lineage,
                    observed_at_ms,
                )
                .await
        }
        PrepareTarget::ReverseOpen { root_intent_id } => {
            debug_assert!(lineage.is_none());
            execution
                .prepare_reverse_open(&root_intent_id, plan, observed_at_ms)
                .await
        }
    };
    match result {
        Ok(outcome) => prepare_outcome_response(request, generated_at, outcome),
        Err(OrderExecutorError::Ledger(ExecutionLedgerError::ReverseNotReady)) => {
            Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                EXECUTION_INPUT_INCONSISTENT_CODE,
                "reverse execution is not ready for its next leg".to_owned(),
                true,
            ))
        }
        Err(OrderExecutorError::Ledger(ExecutionLedgerError::ReverseMismatch)) => {
            Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                EXECUTION_INPUT_INCONSISTENT_CODE,
                "reverse continuation does not match the durable root".to_owned(),
                false,
            ))
        }
        Err(OrderExecutorError::Ledger(ExecutionLedgerError::ReverseAborted)) => {
            Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                EXECUTION_INPUT_INCONSISTENT_CODE,
                "reverse continuation was explicitly aborted".to_owned(),
                false,
            ))
        }
        Err(error) => Err(error.into()),
    }
}

async fn abort_reverse_execution(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    intent_id: &str,
) -> AgentResult<AgentResponse> {
    let Some(execution) = context.execution else {
        return Ok(execution_unavailable(request, generated_at));
    };
    match execution.abort_reverse(intent_id, utc_now_ms()).await {
        Ok(_) => execution_status_response(request, context, generated_at, intent_id).await,
        Err(OrderExecutorError::Ledger(ExecutionLedgerError::IntentNotFound(_))) => {
            Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                EXECUTION_RECORD_NOT_FOUND_CODE,
                "reverse root execution record was not found".to_owned(),
                false,
            ))
        }
        Err(OrderExecutorError::Transition(ExecutionTransitionError::InvalidReverseTransition)) => {
            Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                EXECUTION_INPUT_INCONSISTENT_CODE,
                "reverse continuation cannot be aborted in the current state".to_owned(),
                false,
            ))
        }
        Err(error) => Err(error.into()),
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
    let credential =
        match executor_preflight_check(request, generated_at, execution, &account).await? {
            ExecutorPreflightCheck::Ready(value) => value,
            ExecutorPreflightCheck::Response(response) => return Ok(*response),
        };
    let clock = match execution.clock_evidence().await {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                EXECUTION_CLOCK_UNAVAILABLE_CODE,
                error.to_string(),
                true,
            ));
        }
    };
    let account_rate_limit = match execution.account_rate_limit_evidence().await {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                EXECUTION_PREFLIGHT_UNAVAILABLE_CODE,
                error.to_string(),
                true,
            ));
        }
    };
    let rate_budget = execution.rate_budget_snapshot();
    let evidence = crate::execution_preflight::ExecutorPreflightSnapshot::new(
        credential,
        clock.snapshot(),
        account_rate_limit,
        rate_budget,
    );
    Ok(completed(
        request,
        generated_at,
        crate::execution_preflight::EXECUTOR_PREFLIGHT_SCHEMA_V3,
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

    let current_fee_generation = if plan.action.is_risk_increasing() {
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

    let Some(risk_binding) = plan.risk_binding.as_ref() else {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_RISK_POLICY_REQUIRED_CODE,
            "prepared execution is missing the immutable mandate/hard-risk policy binding"
                .to_owned(),
            false,
        ));
    };
    if risk_binding.mandate.schema != TRADING_MANDATE_SCHEMA_V1
        || risk_binding.policy.schema != HARD_RISK_POLICY_SCHEMA_V1
    {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_RISK_POLICY_REQUIRED_CODE,
            "prepared execution carries an unsupported mandate/hard-risk policy schema".to_owned(),
            false,
        ));
    }
    let Some(observer) = context.account_fallback else {
        return Ok(execution_unavailable(request, generated_at));
    };
    let Some(private_ws) = context.private_ws else {
        return Ok(account_not_fresh(request, generated_at));
    };
    let risk_cursor = match private_ws.convergence_cursor().await {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                EXECUTION_RISK_EVIDENCE_NOT_FRESH_CODE,
                format!("private account convergence cursor unavailable before risk read: {error}"),
                true,
            ));
        }
    };
    let risk_facts = match observer.ledger_facts(&account).await {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                EXECUTION_RISK_EVIDENCE_UNAVAILABLE_CODE,
                error.to_string(),
                true,
            ));
        }
    };
    if risk_facts
        .summary
        .history_coverage
        .iter()
        .any(|coverage| !coverage.complete_within_bound)
    {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_RISK_EVIDENCE_NOT_FRESH_CODE,
            "bounded account-history coverage is incomplete for pre-mutation risk".to_owned(),
            true,
        ));
    }

    let configured_leverage = if plan.action.is_risk_increasing() {
        let margin_mode = match plan.trade_mode {
            TradeMode::Cross => MarginMode::Cross,
            TradeMode::Isolated => MarginMode::Isolated,
        };
        match observer
            .configured_leverage(
                &plan.instrument_id,
                margin_mode,
                plan.position_side.as_str(),
            )
            .await
        {
            Ok(value) => Some(value),
            Err(error) => {
                return Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Rejected,
                    EXECUTION_RISK_EVIDENCE_UNAVAILABLE_CODE,
                    format!("configured leverage is not uniquely available: {error}"),
                    true,
                ));
            }
        }
    } else {
        None
    };

    match private_ws.convergence_window(risk_cursor).await {
        Ok(window) if window.events.is_empty() => {}
        Ok(window) => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                EXECUTION_RISK_EVIDENCE_NOT_FRESH_CODE,
                format!(
                    "{} private account event(s) arrived while pre-mutation risk evidence was read",
                    window.events.len()
                ),
                true,
            ));
        }
        Err(error) => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                EXECUTION_RISK_EVIDENCE_NOT_FRESH_CODE,
                format!("private account coherence changed during pre-mutation risk read: {error}"),
                true,
            ));
        }
    }

    let Some(rules_after_risk) = current_rules(context, &plan.instrument_id).await else {
        return Ok(reference_not_fresh(request, generated_at));
    };
    if rules_after_risk.reference_generation != rules.reference_generation {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_REFERENCE_NOT_FRESH_CODE,
            format!(
                "reference generation changed during pre-mutation risk read: expected {}, observed {}",
                rules.reference_generation, rules_after_risk.reference_generation
            ),
            true,
        ));
    }

    let risk_candidate = if plan.action.is_risk_increasing() {
        let Some(open_risk) = plan.open_risk.as_ref() else {
            return Ok(validation_failure(
                request,
                generated_at,
                okx_execution::ExecutionValidationError::MissingOpenRiskEvidence,
            ));
        };
        let settle_currency = rules
            .instrument
            .settle_currency
            .as_deref()
            .unwrap_or_default();
        if !matches!(settle_currency, "USD" | "USDT" | "USDC" | "USDG") {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                EXECUTION_RISK_EVIDENCE_UNAVAILABLE_CODE,
                format!(
                    "pre-mutation candidate USD equivalence is unsupported for settlement currency '{settle_currency}'"
                ),
                false,
            ));
        }
        Some(PortfolioCandidate {
            instrument: plan.instrument_id.clone(),
            direction: match plan.position_side {
                ExecutionPositionSide::Long => PositionDirection::Long,
                ExecutionPositionSide::Short => PositionDirection::Short,
            },
            notional_usd: open_risk.entry_settle_notional.clone(),
            worst_case_loss_usd: open_risk.stop_loss_settle.clone(),
            leverage: configured_leverage
                .clone()
                .expect("risk-increasing execution acquired configured leverage"),
        })
    } else {
        None
    };

    let risk_analysis = match analyze_portfolio_risk(
        &account,
        &risk_facts.summary,
        risk_binding.mandate.clone(),
        risk_binding.policy.clone(),
        risk_candidate,
        true,
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
    if let Err(error) = revalidate_hard_risk_policy(
        &plan,
        &risk_analysis,
        &account.account_generation,
        configured_leverage.as_deref(),
    ) {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_RISK_POLICY_REJECTED_CODE,
            error.to_string(),
            false,
        ));
    }

    let Some(public_ws) = context.public_ws else {
        return Ok(reference_not_fresh(request, generated_at));
    };
    let public_state = public_ws.state();
    if public_state.read().await.connection_state() != okx_runtime::PublicConnectionState::Connected
    {
        return Ok(reference_not_fresh(request, generated_at));
    }

    let venue = match execution.venue_execution_evidence(&plan, &rules).await {
        Ok(value) => value,
        Err(crate::AgentError::Okx(error)) => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                EXECUTION_VENUE_UNAVAILABLE_CODE,
                error.to_string(),
                true,
            ));
        }
        Err(crate::AgentError::Reference(error)) => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                EXECUTION_INPUT_INCONSISTENT_CODE,
                error.to_string(),
                false,
            ));
        }
        Err(error) => return Err(error),
    };

    // Venue REST evidence can take long enough to consume the clock-evidence age budget.
    // Sample exchange time only after all pre-mutation venue I/O is complete.
    let clock = match execution.clock_evidence().await {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                EXECUTION_CLOCK_UNAVAILABLE_CODE,
                error.to_string(),
                true,
            ));
        }
    };

    let timing = match clock.mutation_timing(MUTATION_REQUEST_TTL_MS) {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                EXECUTION_CLOCK_UNSAFE_CODE,
                error.to_string(),
                true,
            ));
        }
    };
    if let Err(error) = revalidate_venue_execution(&plan, &rules, &venue, timing.exp_time_ms()) {
        return Ok(validation_failure(request, generated_at, error));
    }

    // Risk evidence must still be current after all pre-mutation venue/clock I/O.
    match private_ws.convergence_window(risk_cursor).await {
        Ok(window) if window.events.is_empty() => {}
        Ok(window) => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                EXECUTION_RISK_EVIDENCE_NOT_FRESH_CODE,
                format!(
                    "{} private account event(s) arrived after risk evaluation and before mutation",
                    window.events.len()
                ),
                true,
            ));
        }
        Err(error) => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                EXECUTION_RISK_EVIDENCE_NOT_FRESH_CODE,
                format!(
                    "private account coherence changed after risk evaluation and before mutation: {error}"
                ),
                true,
            ));
        }
    }
    let Some(final_rules) = current_rules(context, &plan.instrument_id).await else {
        return Ok(reference_not_fresh(request, generated_at));
    };
    if final_rules.reference_generation != rules.reference_generation {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_REFERENCE_NOT_FRESH_CODE,
            format!(
                "reference generation changed after risk evaluation and before mutation: expected {}, observed {}",
                rules.reference_generation, final_rules.reference_generation
            ),
            true,
        ));
    }

    let observed_at_ms = timing.request_time_ms();
    match execution
        .submit_prepared(intent_id, timing, observed_at_ms)
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

fn reference_not_fresh(request: &AgentRequest, generated_at: &str) -> AgentResponse {
    failure_response(
        request,
        generated_at,
        AgentResponseStatus::Rejected,
        EXECUTION_REFERENCE_NOT_FRESH_CODE,
        "execution requires a currently connected public reference owner before mutation"
            .to_owned(),
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
        PrepareOutcome::Deferred(PrepareDeferral::InstrumentBusy) => Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_INSTRUMENT_BUSY_CODE,
            "another nonterminal managed execution already owns this instrument".to_owned(),
            true,
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
        EXECUTION_STATUS_SCHEMA_V2,
        serde_json::to_value(status)?,
    ))
}

fn execution_lineage_binding(value: &ExecutionLineageRequest) -> ExecutionLineageBinding {
    ExecutionLineageBinding {
        schema: EXECUTION_LINEAGE_SCHEMA_V1.to_owned(),
        origin_evidence_id: value.origin_evidence_id.clone(),
        origin_schema: value.origin_schema.clone(),
        origin_version: value.origin_version.clone(),
        authority_evidence_id: value.authority_evidence_id.clone(),
        decision_reference: ExecutionDecisionReference {
            decision_time_ms: value.decision_reference.decision_time_ms,
            price: value.decision_reference.price.clone(),
            price_basis: match value.decision_reference.price_basis {
                ExecutionReferencePriceBasis::DecisionPrice => {
                    okx_execution::TcaReferencePriceBasis::DecisionPrice
                }
                ExecutionReferencePriceBasis::ArrivalMid => {
                    okx_execution::TcaReferencePriceBasis::ArrivalMid
                }
                ExecutionReferencePriceBasis::Mark => okx_execution::TcaReferencePriceBasis::Mark,
                ExecutionReferencePriceBasis::Index => {
                    okx_execution::TcaReferencePriceBasis::Index
                }
                ExecutionReferencePriceBasis::Last => okx_execution::TcaReferencePriceBasis::Last,
                ExecutionReferencePriceBasis::LimitPrice => {
                    okx_execution::TcaReferencePriceBasis::LimitPrice
                }
            },
            price_policy_version: value.decision_reference.price_policy_version.clone(),
        },
    }
}

fn execution_risk_binding(value: &ExecutionRiskBindingRequest) -> ExecutionRiskBinding {
    ExecutionRiskBinding {
        mandate: trading_mandate(&value.mandate),
        policy: hard_risk_policy(&value.policy),
    }
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
                risk_binding: None,
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

use okx_analysis::{
    CandidateOrderAssumptions, ExecutionTcaReport, HARD_RISK_POLICY_SCHEMA_V1,
    LiquidityRole as AnalysisLiquidityRole, PortfolioCandidate, PositionDirection,
    TRADING_MANDATE_SCHEMA_V1, TcaFillOutcome, TcaReference, TcaSide, analyze_candidate_order,
    analyze_execution_tca_report, analyze_portfolio_risk,
};
use okx_api::{InstrumentType, MUTATION_REQUEST_TTL_MS, MarginMode, MutationTiming};
use okx_execution::{
    EXECUTION_LINEAGE_SCHEMA_V2, EXECUTION_STATUS_SCHEMA_V3, ExecutionAction,
    ExecutionDecisionReference, ExecutionIntent, ExecutionLedgerEntry, ExecutionLedgerError,
    ExecutionLineageBinding, ExecutionRiskBinding, ExecutionState, ExecutionTcaInstrumentType,
    ExecutionTcaMechanicsBinding, ExecutionTransitionError, OrderExecutorError,
    OrderSide as ExecutionOrderSide, OrderType, PositionSide as ExecutionPositionSide,
    PrepareDeferral, PrepareFailure, PrepareOutcome, PrepareRejection, ReverseContinuation,
    ReverseLeg, TradeMode, prepare_execution, revalidate_execution_plan,
    revalidate_hard_risk_policy, revalidate_venue_execution,
};
use okx_protocol::{
    ExecutionEntryRequest, ExecutionLineageRequest, ExecutionMutationRequest, ExecutionOrderType,
    ExecutionPrepareSpec, ExecutionReferencePriceBasis, ExecutionRiskBindingRequest,
    ExecutionTradeMode, LiquidityRole as ProtocolLiquidityRole,
    PositionSide as ProtocolPositionSide,
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
pub const EXECUTION_RISK_STOP_PERSISTENCE_FAILED_CODE: &str =
    "EXECUTION_RISK_STOP_PERSISTENCE_FAILED";
pub const EXECUTION_RECONCILIATION_FAILED_CODE: &str = "EXECUTION_RECONCILIATION_FAILED";
pub const EXECUTION_RECONCILIATION_UNAVAILABLE_CODE: &str = "EXECUTION_RECONCILIATION_UNAVAILABLE";
pub const EXECUTION_MUTATION_UNSAFE_CODE: &str = "EXECUTION_MUTATION_UNSAFE";

fn daily_history_complete_at(
    coverage: &[okx_observation::AccountHistoryCoverage],
    day_start: Option<&str>,
    day_end: Option<&str>,
    observed_at_ms: u64,
) -> bool {
    let (Some(start), Some(end)) = (day_start, day_end) else {
        return false;
    };
    let (Ok(start), Ok(end)) = (start.parse::<u64>(), end.parse::<u64>()) else {
        return false;
    };
    if end.checked_sub(start) != Some(86_400_000) || observed_at_ms < start || observed_at_ms >= end
    {
        return false;
    }
    let mut seen = false;
    for row in coverage {
        if row.resource.starts_with("positions_history:") {
            seen = true;
            if !row.complete_within_bound {
                return false;
            }
        }
    }
    seen
}

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
        AgentOperation::MutateExecution {
            intent_id,
            mutation,
        } => mutate_execution(request, context, generated_at, intent_id, mutation).await,
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
    let lineage = common
        .lineage
        .map(|value| execution_lineage_binding(value, &rules));
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
    let lineage = common
        .lineage
        .map(|value| execution_lineage_binding(value, &rules));
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
    match execution.preflight_for_runtime(account).await {
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

    let plan = entry.record.plan;
    let admission =
        match pre_mutation_admission(request, context, generated_at, execution, plan).await? {
            PreMutationAdmissionResult::Ready(value) => value,
            PreMutationAdmissionResult::Response(response) => return Ok(*response),
        };
    let preflight = admission.preflight;
    let timing = admission.timing;

    let observed_at_ms = timing.request_time_ms();
    let demo_acceptance = execution.demo_mutation_acceptance_requested();
    let submit_result = if demo_acceptance {
        match execution
            .submit_prepared_demo_authorized(&preflight, intent_id, timing, observed_at_ms)
            .await?
        {
            Some(result) => result,
            None => return Ok(preflight_rejected(request, generated_at)),
        }
    } else {
        execution
            .submit_prepared(intent_id, timing, observed_at_ms)
            .await
    };

    match submit_result {
        Ok(_) if demo_acceptance => {
            execution_status_response(request, context, generated_at, intent_id).await
        }
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

enum MutationReconciliationCheck {
    Ready(Box<ExecutionLedgerEntry>),
    Response(Box<AgentResponse>),
}

async fn reconcile_for_mutation(
    request: &AgentRequest,
    generated_at: &str,
    execution: &crate::execution_runtime::ExecutionRuntime,
    intent_id: &str,
) -> AgentResult<MutationReconciliationCheck> {
    match execution.reconcile_once(intent_id, utc_now_ms()).await {
        Ok(crate::execution_runtime::ExecutionReconciliation::NotRequired)
        | Ok(crate::execution_runtime::ExecutionReconciliation::Reconciled) => {}
        Ok(crate::execution_runtime::ExecutionReconciliation::Unavailable) => {
            return Ok(MutationReconciliationCheck::Response(Box::new(
                failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Rejected,
                    EXECUTION_RECONCILIATION_UNAVAILABLE_CODE,
                    "exact OKX order state is unavailable; mutation is blocked until exchange truth can be reconciled"
                        .to_owned(),
                    true,
                ),
            )));
        }
        Err(OrderExecutorError::Ledger(ExecutionLedgerError::IntentNotFound(_))) => {
            return Ok(MutationReconciliationCheck::Response(Box::new(
                failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Rejected,
                    EXECUTION_RECORD_NOT_FOUND_CODE,
                    "execution record was not found".to_owned(),
                    false,
                ),
            )));
        }
        Err(
            error @ (OrderExecutorError::ReconciliationIdentityMismatch
            | OrderExecutorError::ProtectionIdentityMismatch
            | OrderExecutorError::UnsupportedExchangeState(_)),
        ) => {
            return Ok(MutationReconciliationCheck::Response(Box::new(
                failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Failed,
                    EXECUTION_RECONCILIATION_FAILED_CODE,
                    error.to_string(),
                    false,
                ),
            )));
        }
        Err(error) => return Err(error.into()),
    }

    let Some(entry) = execution.entry(intent_id).await else {
        return Ok(MutationReconciliationCheck::Response(Box::new(
            failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                EXECUTION_RECORD_NOT_FOUND_CODE,
                "execution record was not found after reconciliation".to_owned(),
                false,
            ),
        )));
    };
    Ok(MutationReconciliationCheck::Ready(Box::new(entry)))
}

async fn cancel_mutation_admission(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    execution: &crate::execution_runtime::ExecutionRuntime,
) -> AgentResult<PreMutationAdmissionResult> {
    let account = match fresh_account(request, context, generated_at).await? {
        FreshAccount::Ready(value) => value,
        FreshAccount::Response(response) => {
            return Ok(PreMutationAdmissionResult::Response(response));
        }
    };
    let preflight =
        match executor_preflight_check(request, generated_at, execution, &account).await? {
            ExecutorPreflightCheck::Ready(value) => value,
            ExecutorPreflightCheck::Response(response) => {
                return Ok(PreMutationAdmissionResult::Response(response));
            }
        };
    if !preflight.accepted {
        return Ok(PreMutationAdmissionResult::Response(Box::new(
            preflight_rejected(request, generated_at),
        )));
    }

    let clock = match execution.clock_evidence().await {
        Ok(value) => value,
        Err(error) => {
            return Ok(PreMutationAdmissionResult::Response(Box::new(
                failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Failed,
                    EXECUTION_CLOCK_UNAVAILABLE_CODE,
                    error.to_string(),
                    true,
                ),
            )));
        }
    };
    let timing = match clock.mutation_timing(MUTATION_REQUEST_TTL_MS) {
        Ok(value) => value,
        Err(error) => {
            return Ok(PreMutationAdmissionResult::Response(Box::new(
                failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Failed,
                    EXECUTION_CLOCK_UNSAFE_CODE,
                    error.to_string(),
                    true,
                ),
            )));
        }
    };
    Ok(PreMutationAdmissionResult::Ready(PreMutationAdmission {
        preflight,
        timing,
    }))
}

async fn mutate_execution(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    intent_id: &str,
    mutation: &ExecutionMutationRequest,
) -> AgentResult<AgentResponse> {
    let Some(execution) = context.execution else {
        return Ok(execution_unavailable(request, generated_at));
    };
    if !execution.demo_mutation_acceptance_requested() {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            LIVE_TRADING_DISABLED_CODE,
            "execution mutation is disabled; only explicit OKX Demo acceptance mode can admit Stage 4C mutations"
                .to_owned(),
            false,
        ));
    }

    if let ExecutionMutationRequest::CancelProtection { mutation_id } = mutation {
        return cancel_owned_protection(
            request,
            context,
            generated_at,
            execution,
            intent_id,
            mutation_id,
        )
        .await;
    }

    let entry = match reconcile_for_mutation(request, generated_at, execution, intent_id).await? {
        MutationReconciliationCheck::Ready(value) => value,
        MutationReconciliationCheck::Response(response) => return Ok(*response),
    };

    let (mutation_id, admission) = match mutation {
        ExecutionMutationRequest::Amend {
            mutation_id,
            new_size,
            new_price,
        } => {
            let shadow_plan = match execution
                .amend_revalidation_plan(intent_id, new_size.clone(), new_price.clone())
                .await
            {
                Ok(value) => value,
                Err(error) => {
                    return Ok(failure_response(
                        request,
                        generated_at,
                        AgentResponseStatus::Rejected,
                        EXECUTION_MUTATION_UNSAFE_CODE,
                        error.to_string(),
                        false,
                    ));
                }
            };
            let admission = match pre_mutation_admission(
                request,
                context,
                generated_at,
                execution,
                shadow_plan,
            )
            .await?
            {
                PreMutationAdmissionResult::Ready(value) => value,
                PreMutationAdmissionResult::Response(response) => return Ok(*response),
            };
            (mutation_id.as_str(), admission)
        }
        ExecutionMutationRequest::CancelProtection { .. } => {
            unreachable!("protective cleanup is routed before ordinary order mutation")
        }
        ExecutionMutationRequest::Cancel { mutation_id } => {
            let admission =
                match cancel_mutation_admission(request, context, generated_at, execution).await? {
                    PreMutationAdmissionResult::Ready(value) => value,
                    PreMutationAdmissionResult::Response(response) => return Ok(*response),
                };
            (mutation_id.as_str(), admission)
        }
    };

    let observed_at_ms = admission
        .timing
        .request_time_ms()
        .max(entry.updated_at_ms)
        .max(utc_now_ms());
    let prepared = match mutation {
        ExecutionMutationRequest::Amend {
            new_size,
            new_price,
            ..
        } => {
            execution
                .prepare_amend(
                    intent_id,
                    mutation_id,
                    new_size.clone(),
                    new_price.clone(),
                    observed_at_ms,
                )
                .await
        }
        ExecutionMutationRequest::CancelProtection { .. } => {
            unreachable!("protective cleanup has a distinct durable state")
        }
        ExecutionMutationRequest::Cancel { .. } => {
            execution
                .prepare_cancel(intent_id, mutation_id, observed_at_ms)
                .await
        }
    };
    if let Err(error) = prepared {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_INPUT_INCONSISTENT_CODE,
            error.to_string(),
            false,
        ));
    }

    let submit = match execution
        .submit_order_mutation_demo_authorized(
            &admission.preflight,
            intent_id,
            mutation_id,
            admission.timing,
            observed_at_ms,
        )
        .await?
    {
        Some(result) => result,
        None => return Ok(preflight_rejected(request, generated_at)),
    };

    match submit {
        Ok(_) => execution_status_response(request, context, generated_at, intent_id).await,
        Err(OrderExecutorError::Transition(ExecutionTransitionError::LiveTradingDisabled)) => {
            Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                LIVE_TRADING_DISABLED_CODE,
                "mutation authority was disabled before mutation persistence/send".to_owned(),
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
    }
}

async fn cancel_owned_protection(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    execution: &crate::execution_runtime::ExecutionRuntime,
    intent_id: &str,
    mutation_id: &str,
) -> AgentResult<AgentResponse> {
    let Some(mut entry) = execution.entry(intent_id).await else {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_RECORD_NOT_FOUND_CODE,
            "managed parent intent does not exist".to_owned(),
            false,
        ));
    };
    let admission =
        match cancel_mutation_admission(request, context, generated_at, execution).await? {
            PreMutationAdmissionResult::Ready(value) => value,
            PreMutationAdmissionResult::Response(value) => return Ok(*value),
        };
    // Re-read the account after admission: flat means no exchange positions,
    // ordinary pending orders, or non-owning residual risk. This is Demo only.
    let account = match fresh_account(request, context, generated_at).await? {
        FreshAccount::Ready(value) => value,
        FreshAccount::Response(value) => return Ok(*value),
    };
    if !account
        .positions
        .iter()
        .all(|position| position.position.trim() == "0")
        || !account.pending_orders.is_empty()
    {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_MUTATION_UNSAFE_CODE,
            "protective cleanup requires fresh flat account and zero ordinary pending orders"
                .to_owned(),
            false,
        ));
    }
    let Some(observer) = context.account_fallback else {
        return Ok(execution_unavailable(request, generated_at));
    };
    let inventory = match observer.pending_protective_algos().await {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                EXECUTION_RECONCILIATION_UNAVAILABLE_CODE,
                error.to_string(),
                true,
            ));
        }
    };
    if !inventory.complete_within_bound {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_RECONCILIATION_UNAVAILABLE_CODE,
            "pending protective algo inventory is truncated; no absence or cancel proof".to_owned(),
            true,
        ));
    }
    let Some(protection) = entry.record.protection.as_ref() else {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_MUTATION_UNSAFE_CODE,
            "parent has no managed protection".to_owned(),
            false,
        ));
    };
    let owned_algo = protection.algo_order_id.clone().unwrap_or_default();
    let owned_client = protection.algo_client_order_id.clone();

    if let Some(previous) = protection.cleanup.as_ref() {
        if previous.mutation_id != mutation_id {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                EXECUTION_MUTATION_UNSAFE_CODE,
                "a different durable protective cleanup owns this parent".to_owned(),
                false,
            ));
        }
        if previous.state == okx_execution::ProtectiveCleanupState::ConfirmedAbsent {
            return Ok(completed(
                request,
                generated_at,
                "okx.protective-cleanup/v1",
                serde_json::json!({
                    "intent_id": intent_id,
                    "mutation_id": mutation_id,
                    "state": "CONFIRMED_ABSENT",
                    "exchange_post_replayed": false,
                }),
            ));
        }
        if previous.state == okx_execution::ProtectiveCleanupState::Submitting {
            // The process may have died after writing SUBMITTING. This is
            // uncertain exchange effect; NEVER send the cancellation again.
            entry = execution
                .mark_protective_cleanup_unknown_after_restart(
                    intent_id,
                    mutation_id,
                    utc_now_ms().max(entry.updated_at_ms),
                )
                .await?;
        }
        if matches!(
            entry
                .record
                .protection
                .as_ref()
                .and_then(|p| p.cleanup.as_ref())
                .map(|c| c.state),
            Some(okx_execution::ProtectiveCleanupState::Unknown)
                | Some(okx_execution::ProtectiveCleanupState::Acknowledged)
        ) {
            if inventory.rows == 0 {
                let confirmed = execution
                    .confirm_protective_cleanup_absent(
                        intent_id,
                        mutation_id,
                        utc_now_ms().max(entry.updated_at_ms),
                    )
                    .await?;
                return Ok(completed(
                    request,
                    generated_at,
                    "okx.protective-cleanup/v1",
                    serde_json::json!({
                        "intent_id": intent_id,
                        "mutation_id": mutation_id,
                        "state": "CONFIRMED_ABSENT",
                        "exchange_post_replayed": false,
                        "record_updated_at_ms": confirmed.updated_at_ms,
                    }),
                ));
            }
            return Ok(failure_response(
                request, generated_at, AgentResponseStatus::Rejected,
                EXECUTION_RECONCILIATION_UNAVAILABLE_CODE,
                "previous protective cancel result remains uncertain or pending; POST must not be replayed".to_owned(),
                true,
            ));
        }
    }

    // At the time of a first one-shot cancel, this acceptance path requires
    // exactly one pending protective algo across its explicitly bounded scope.
    if inventory.rows != 1
        || inventory.samples.len() != 1
        || inventory.samples[0].instrument_id != entry.record.plan.instrument_id
        || inventory.samples[0].algo_order_id != owned_algo
        || inventory.samples[0].client_order_id != owned_client
    {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_MUTATION_UNSAFE_CODE,
            "single pending exchange algo does not exactly match managed parent ownership"
                .to_owned(),
            false,
        ));
    }
    let observed_at_ms = utc_now_ms().max(entry.updated_at_ms);
    if let Err(error) = execution
        .prepare_protective_cleanup(intent_id, mutation_id, observed_at_ms)
        .await
    {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_MUTATION_UNSAFE_CODE,
            error.to_string(),
            false,
        ));
    }
    let outcome = match execution
        .submit_protective_cleanup_demo_authorized(
            &admission.preflight,
            intent_id,
            mutation_id,
            admission.timing,
            observed_at_ms,
        )
        .await?
    {
        Some(result) => result,
        None => return Ok(preflight_rejected(request, generated_at)),
    };
    match outcome {
        Ok(result) => {
            let state = match result {
                okx_execution::MutationSubmitDisposition::Acknowledged(_) => "ACKNOWLEDGED",
                okx_execution::MutationSubmitDisposition::Unknown(_) => "UNKNOWN",
                okx_execution::MutationSubmitDisposition::Rejected(_) => "REJECTED",
                okx_execution::MutationSubmitDisposition::RateRejected { .. } => "RATE_REJECTED",
            };
            let response = completed(
                request,
                generated_at,
                "okx.protective-cleanup/v1",
                serde_json::json!({
                    "intent_id": intent_id,
                    "mutation_id": mutation_id,
                    "state": state,
                    "exchange_effect_terminally_verified": false,
                    "exchange_post_replayed": false,
                }),
            );
            Ok(AgentResponse {
                quality: DataQuality::Degraded,
                warnings: vec![
                    "protective cancellation is not terminally verified; independently reconcile exact algo absence before accepting flat"
                        .to_owned(),
                ],
                ..response
            })
        }
        Err(error) => Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_MUTATION_UNSAFE_CODE,
            error.to_string(),
            false,
        )),
    }
}

struct PreMutationAdmission {
    preflight: crate::execution_preflight::ExecutorCredentialPreflight,
    timing: MutationTiming,
}

enum PreMutationAdmissionResult {
    Ready(PreMutationAdmission),
    Response(Box<AgentResponse>),
}

async fn pre_mutation_admission(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    execution: &crate::execution_runtime::ExecutionRuntime,
    plan: okx_execution::ExecutionPlan,
) -> AgentResult<PreMutationAdmissionResult> {
    macro_rules! admission_response {
        ($response:expr) => {
            return Ok(PreMutationAdmissionResult::Response(Box::new($response)))
        };
    }

    let account = match fresh_account(request, context, generated_at).await? {
        FreshAccount::Ready(value) => value,
        FreshAccount::Response(response) => admission_response!(*response),
    };
    let preflight =
        match executor_preflight_check(request, generated_at, execution, &account).await? {
            ExecutorPreflightCheck::Ready(value) => value,
            ExecutorPreflightCheck::Response(response) => admission_response!(*response),
        };
    if !preflight.accepted {
        admission_response!(preflight_rejected(request, generated_at));
    }
    let Some(rules) = current_rules(context, &plan.instrument_id).await else {
        admission_response!(reference_not_found(
            request,
            generated_at,
            &plan.instrument_id,
        ));
    };

    let current_fee_generation = if plan.action.is_risk_increasing() {
        let Some(observer) = context.account_fallback else {
            admission_response!(execution_unavailable(request, generated_at));
        };
        let fees = match observer.fee_schedule(&rules).await {
            Ok(value) => value,
            Err(error) => admission_response!(fee_schedule_failure(request, generated_at, error)),
        };
        Some(fees.fee_generation)
    } else {
        None
    };

    if let Err(error) =
        revalidate_execution_plan(&plan, &rules, &account, current_fee_generation.as_deref())
    {
        admission_response!(validation_failure(request, generated_at, error));
    }

    let Some(risk_binding) = plan.risk_binding.as_ref() else {
        admission_response!(failure_response(
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
        admission_response!(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_RISK_POLICY_REQUIRED_CODE,
            "prepared execution carries an unsupported mandate/hard-risk policy schema".to_owned(),
            false,
        ));
    }
    let Some(observer) = context.account_fallback else {
        admission_response!(execution_unavailable(request, generated_at));
    };
    let Some(private_ws) = context.private_ws else {
        admission_response!(account_not_fresh(request, generated_at));
    };
    let risk_cursor = match private_ws.convergence_cursor().await {
        Ok(value) => value,
        Err(error) => {
            admission_response!(failure_response(
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
            admission_response!(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                EXECUTION_RISK_EVIDENCE_UNAVAILABLE_CODE,
                error.to_string(),
                true,
            ));
        }
    };
    let daily_history_complete = daily_history_complete_at(
        &risk_facts.summary.history_coverage,
        risk_facts
            .summary
            .daily_realized_pnl_utc_day_start_ms
            .as_deref(),
        risk_facts
            .summary
            .daily_realized_pnl_utc_day_end_ms
            .as_deref(),
        utc_now_ms(),
    );
    // Opening new exposure requires complete bounded history. Reducing a
    // verified position must not be trapped by unrelated historical gaps.
    if plan.action.is_risk_increasing()
        && (!daily_history_complete
            || risk_facts
                .summary
                .history_coverage
                .iter()
                .any(|coverage| !coverage.complete_within_bound))
    {
        admission_response!(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_RISK_EVIDENCE_NOT_FRESH_CODE,
            "UTC-day or bounded account-history coverage is incomplete for new risk".to_owned(),
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
                admission_response!(failure_response(
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
            admission_response!(failure_response(
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
            admission_response!(failure_response(
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
        admission_response!(reference_not_fresh(request, generated_at));
    };
    if rules_after_risk.reference_generation != rules.reference_generation {
        admission_response!(failure_response(
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
            admission_response!(validation_failure(
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
            admission_response!(failure_response(
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
            admission_response!(analysis_failure(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                error,
            ));
        }
    };
    let risk_disposition = revalidate_hard_risk_policy(
        &plan,
        &risk_analysis,
        &account.account_generation,
        configured_leverage.as_deref(),
    );
    // Never persist a global stop based on a mismatched immutable mandate,
    // stale account generation or invalid candidate binding. Actual accepted
    // risk analyses with an account-wide violation may latch before send.
    if matches!(
        &risk_disposition,
        Ok(_) | Err(okx_execution::ExecutionValidationError::HardRiskPolicyRejected(_))
    ) {
        if let Err(error) = execution
            .latch_account_risk_stop(&risk_analysis, daily_history_complete, utc_now_ms())
            .await
        {
            admission_response!(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                EXECUTION_RISK_STOP_PERSISTENCE_FAILED_CODE,
                format!("unable to persist account risk stop: {error}"),
                false,
            ));
        }
    }
    if let Err(error) = risk_disposition {
        admission_response!(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_RISK_POLICY_REJECTED_CODE,
            error.to_string(),
            false,
        ));
    }

    let Some(public_ws) = context.public_ws else {
        admission_response!(reference_not_fresh(request, generated_at));
    };
    let public_state = public_ws.state();
    if public_state.read().await.connection_state() != okx_runtime::PublicConnectionState::Connected
    {
        admission_response!(reference_not_fresh(request, generated_at));
    }

    let venue = match execution.venue_execution_evidence(&plan, &rules).await {
        Ok(value) => value,
        Err(crate::AgentError::Okx(error)) => {
            admission_response!(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                EXECUTION_VENUE_UNAVAILABLE_CODE,
                error.to_string(),
                true,
            ));
        }
        Err(crate::AgentError::Reference(error)) => {
            admission_response!(failure_response(
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
            admission_response!(failure_response(
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
            admission_response!(failure_response(
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
        admission_response!(validation_failure(request, generated_at, error));
    }

    // Risk evidence must still be current after all pre-mutation venue/clock I/O.
    match private_ws.convergence_window(risk_cursor).await {
        Ok(window) if window.events.is_empty() => {}
        Ok(window) => {
            admission_response!(failure_response(
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
            admission_response!(failure_response(
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
        admission_response!(reference_not_fresh(request, generated_at));
    };
    if final_rules.reference_generation != rules.reference_generation {
        admission_response!(failure_response(
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

    Ok(PreMutationAdmissionResult::Ready(PreMutationAdmission {
        preflight,
        timing,
    }))
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

const EXECUTION_TCA_SOURCE_V1: &str = "okx.trade.fills-history/exact-order/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum ExecutionTcaAvailability {
    Available,
    Provisional,
    Unavailable,
}

#[derive(Debug, serde::Serialize)]
struct ExecutionTcaSummary {
    requested_contracts: String,
    filled_contracts: String,
    unfilled_contracts: String,
    fill_ratio: String,
    fill_outcome: TcaFillOutcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    fill_vwap: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    maker_contracts: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    taker_contracts: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    slippage_bps: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    gross_slippage_settle: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    settle_fee_cost: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    net_execution_cost_settle: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reference_to_first_fill_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reference_to_last_fill_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    implementation_shortfall_settle: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    implementation_shortfall_unavailable_reason: Option<&'static str>,
}

impl From<ExecutionTcaReport> for ExecutionTcaSummary {
    fn from(value: ExecutionTcaReport) -> Self {
        let observed = value.observed_execution.as_ref();
        Self {
            requested_contracts: value.requested_contracts,
            filled_contracts: value.filled_contracts,
            unfilled_contracts: value.unfilled_contracts,
            fill_ratio: value.fill_ratio,
            fill_outcome: value.fill_outcome,
            fill_vwap: observed.map(|item| item.fill_vwap.clone()),
            maker_contracts: observed.map(|item| item.maker_contracts.clone()),
            taker_contracts: observed.map(|item| item.taker_contracts.clone()),
            slippage_bps: observed.map(|item| item.slippage_bps.clone()),
            gross_slippage_settle: observed.map(|item| item.gross_slippage_settle.clone()),
            settle_fee_cost: observed.and_then(|item| item.settle_fee_cost.clone()),
            net_execution_cost_settle: observed
                .and_then(|item| item.net_execution_cost_settle.clone()),
            reference_to_first_fill_ms: observed.map(|item| item.reference_to_first_fill_ms),
            reference_to_last_fill_ms: observed.map(|item| item.reference_to_last_fill_ms),
            implementation_shortfall_settle: value.implementation_shortfall_settle,
            implementation_shortfall_unavailable_reason: value
                .implementation_shortfall_unavailable_reason,
        }
    }
}

#[derive(Debug, serde::Serialize)]
struct ExecutionTcaStatus {
    availability: ExecutionTcaAvailability,
    #[serde(skip_serializing_if = "Option::is_none")]
    finality: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    history_pages: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    history_complete_within_bound: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fill_rows: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: Option<ExecutionTcaSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unavailable_reason: Option<&'static str>,
}

impl ExecutionTcaStatus {
    fn unavailable(reason: &'static str) -> Self {
        Self {
            availability: ExecutionTcaAvailability::Unavailable,
            finality: None,
            source: None,
            history_pages: None,
            history_complete_within_bound: None,
            fill_rows: None,
            summary: None,
            unavailable_reason: Some(reason),
        }
    }

    fn with_history(
        availability: ExecutionTcaAvailability,
        finality: &'static str,
        pages: usize,
        complete: bool,
        fill_rows: usize,
        summary: Option<ExecutionTcaSummary>,
        unavailable_reason: Option<&'static str>,
    ) -> Self {
        Self {
            availability,
            finality: Some(finality),
            source: Some(EXECUTION_TCA_SOURCE_V1),
            history_pages: Some(pages),
            history_complete_within_bound: Some(complete),
            fill_rows: Some(fill_rows),
            summary,
            unavailable_reason,
        }
    }
}

#[derive(Debug, serde::Serialize)]
struct ExecutionUnattributedAnalytics {
    post_fill_markout: &'static str,
    funding: &'static str,
    counterfactual: &'static str,
}

#[derive(Debug, serde::Serialize)]
struct ExecutionStatusResult {
    #[serde(flatten)]
    status: okx_execution::ExecutionStatusEnvelope,
    tca: ExecutionTcaStatus,
    unattributed_analytics: ExecutionUnattributedAnalytics,
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

    let mut warnings = Vec::new();
    match execution.reconcile_once(intent_id, utc_now_ms()).await {
        Ok(crate::execution_runtime::ExecutionReconciliation::NotRequired)
        | Ok(crate::execution_runtime::ExecutionReconciliation::Reconciled) => {}
        Ok(crate::execution_runtime::ExecutionReconciliation::Unavailable) => {
            warnings.push(
                "exact OKX order reconciliation was unavailable; durable execution state is returned without inventing venue progress"
                    .to_owned(),
            );
        }
        Err(OrderExecutorError::Ledger(ExecutionLedgerError::IntentNotFound(_))) => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                EXECUTION_RECORD_NOT_FOUND_CODE,
                "execution record was not found".to_owned(),
                false,
            ));
        }
        Err(
            error @ (OrderExecutorError::ReconciliationIdentityMismatch
            | OrderExecutorError::ProtectionIdentityMismatch
            | OrderExecutorError::UnsupportedExchangeState(_)),
        ) => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                EXECUTION_RECONCILIATION_FAILED_CODE,
                error.to_string(),
                false,
            ));
        }
        Err(error) => return Err(error.into()),
    }

    let Some((entry, status)) = execution.status_with_entry(intent_id).await? else {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            EXECUTION_RECORD_NOT_FOUND_CODE,
            "execution record was not found".to_owned(),
            false,
        ));
    };

    let (tca, tca_warnings) = execution_tca_status(context, &entry).await;
    warnings.extend(tca_warnings);
    let result = ExecutionStatusResult {
        status,
        tca,
        unattributed_analytics: ExecutionUnattributedAnalytics {
            post_fill_markout: "unavailable_no_versioned_post_fill_market_evidence",
            funding: "unavailable_no_per_intent_funding_attribution",
            counterfactual: "unavailable_no_versioned_counterfactual_evidence",
        },
    };
    let mut response = completed(
        request,
        generated_at,
        EXECUTION_STATUS_SCHEMA_V3,
        serde_json::to_value(result)?,
    );
    if !warnings.is_empty() {
        response.quality = DataQuality::Degraded;
        response.warnings = warnings;
    }
    Ok(response)
}

async fn execution_tca_status(
    context: ObservationQueryContext<'_>,
    entry: &ExecutionLedgerEntry,
) -> (ExecutionTcaStatus, Vec<String>) {
    let record = &entry.record;
    let Some(lineage) = record.lineage.as_ref() else {
        return (
            ExecutionTcaStatus::unavailable("lineage_missing"),
            Vec::new(),
        );
    };
    let Some(order_id) = record.order_id.as_deref() else {
        return (
            ExecutionTcaStatus::unavailable("exchange_order_id_missing"),
            Vec::new(),
        );
    };
    let Some(mechanics) = lineage.tca_mechanics.as_ref() else {
        return (
            ExecutionTcaStatus::unavailable("tca_mechanics_unavailable"),
            Vec::new(),
        );
    };
    if mechanics.source_reference_generation != record.plan.reference_generation
        || mechanics.contract_type != "linear"
    {
        return (
            ExecutionTcaStatus::unavailable("tca_mechanics_inconsistent"),
            Vec::new(),
        );
    }
    let instrument_type = match mechanics.instrument_type {
        ExecutionTcaInstrumentType::Swap => InstrumentType::Swap,
        ExecutionTcaInstrumentType::Futures => InstrumentType::Futures,
    };
    let Some(observer) = context.account_fallback else {
        return (
            ExecutionTcaStatus::unavailable("account_observer_unavailable"),
            Vec::new(),
        );
    };

    let evidence = match observer
        .execution_fills(
            instrument_type,
            &record.plan.instrument_id,
            order_id,
            &record.plan.client_order_id,
        )
        .await
    {
        Ok(value) => value,
        Err(error) => {
            return (
                ExecutionTcaStatus::unavailable("fills_history_unavailable"),
                vec![format!("execution TCA fill evidence unavailable: {error}")],
            );
        }
    };
    let finality = if record.state.is_terminal() {
        "final"
    } else {
        "provisional"
    };
    if !evidence.complete_within_bound {
        return (
            ExecutionTcaStatus::with_history(
                ExecutionTcaAvailability::Unavailable,
                finality,
                evidence.pages,
                false,
                evidence.fills.len(),
                None,
                Some("fills_history_bound_incomplete"),
            ),
            Vec::new(),
        );
    }
    if evidence
        .fills
        .iter()
        .any(|fill| fill.position_side != record.plan.position_side.as_str())
    {
        return (
            ExecutionTcaStatus::with_history(
                ExecutionTcaAvailability::Unavailable,
                finality,
                evidence.pages,
                true,
                evidence.fills.len(),
                None,
                Some("position_side_mismatch"),
            ),
            vec!["execution TCA fill position side does not match the durable plan".to_owned()],
        );
    }
    if evidence.fills.is_empty() && record.state == ExecutionState::Filled {
        return (
            ExecutionTcaStatus::with_history(
                ExecutionTcaAvailability::Unavailable,
                finality,
                evidence.pages,
                true,
                0,
                None,
                Some("filled_order_has_no_fills"),
            ),
            vec!["filled execution has no exact fills in complete bounded history".to_owned()],
        );
    }
    if evidence.fills.is_empty() && !record.state.is_terminal() {
        return (
            ExecutionTcaStatus::with_history(
                ExecutionTcaAvailability::Provisional,
                finality,
                evidence.pages,
                true,
                0,
                None,
                Some("no_fill_observed_yet"),
            ),
            Vec::new(),
        );
    }

    let report = match analyze_execution_tca_report(
        &record.plan.instrument_id,
        match record.plan.side {
            ExecutionOrderSide::Buy => TcaSide::Buy,
            ExecutionOrderSide::Sell => TcaSide::Sell,
        },
        &mechanics.contract_value,
        &mechanics.settle_currency,
        record.effective_size(),
        &TcaReference {
            price: lineage.decision_reference.price.clone(),
            reference_time_ms: lineage.decision_reference.decision_time_ms,
            price_basis: lineage.decision_reference.price_basis,
            price_policy_version: lineage.decision_reference.price_policy_version.clone(),
        },
        &evidence.fills,
    ) {
        Ok(value) => value,
        Err(error) => {
            return (
                ExecutionTcaStatus::with_history(
                    ExecutionTcaAvailability::Unavailable,
                    finality,
                    evidence.pages,
                    true,
                    evidence.fills.len(),
                    None,
                    Some("tca_analysis_inconsistent"),
                ),
                vec![format!("execution TCA analysis rejected evidence: {error}")],
            );
        }
    };

    if record.state == ExecutionState::Filled && report.fill_outcome != TcaFillOutcome::Complete {
        return (
            ExecutionTcaStatus::with_history(
                ExecutionTcaAvailability::Unavailable,
                finality,
                evidence.pages,
                true,
                evidence.fills.len(),
                Some(report.into()),
                Some("filled_state_completion_mismatch"),
            ),
            vec!["filled execution does not reconcile to the effective requested size".to_owned()],
        );
    }

    (
        ExecutionTcaStatus::with_history(
            if record.state.is_terminal() {
                ExecutionTcaAvailability::Available
            } else {
                ExecutionTcaAvailability::Provisional
            },
            finality,
            evidence.pages,
            true,
            evidence.fills.len(),
            Some(report.into()),
            None,
        ),
        Vec::new(),
    )
}

fn execution_lineage_binding(
    value: &ExecutionLineageRequest,
    rules: &InstrumentRulesSnapshot,
) -> ExecutionLineageBinding {
    let instrument = &rules.instrument;
    let tca_mechanics = match (
        instrument.contract_type.as_deref(),
        instrument.contract_value.as_deref(),
        instrument.settle_currency.as_deref(),
    ) {
        (Some("linear"), Some(contract_value), Some(settle_currency))
            if !contract_value.trim().is_empty() && !settle_currency.trim().is_empty() =>
        {
            Some(ExecutionTcaMechanicsBinding {
                source_reference_generation: rules.reference_generation.clone(),
                instrument_type: match instrument.instrument_type {
                    InstrumentType::Swap => ExecutionTcaInstrumentType::Swap,
                    InstrumentType::Futures => ExecutionTcaInstrumentType::Futures,
                },
                contract_type: "linear".to_owned(),
                contract_value: contract_value.to_owned(),
                settle_currency: settle_currency.to_owned(),
            })
        }
        _ => None,
    };
    ExecutionLineageBinding {
        schema: EXECUTION_LINEAGE_SCHEMA_V2.to_owned(),
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
                ExecutionReferencePriceBasis::Index => okx_execution::TcaReferencePriceBasis::Index,
                ExecutionReferencePriceBasis::Last => okx_execution::TcaReferencePriceBasis::Last,
                ExecutionReferencePriceBasis::LimitPrice => {
                    okx_execution::TcaReferencePriceBasis::LimitPrice
                }
            },
            price_policy_version: value.decision_reference.price_policy_version.clone(),
        },
        tca_mechanics,
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
    #[test]
    fn daily_loss_window_requires_current_complete_position_history() {
        let start = (1_790_000_000_000_u64 / 86_400_000) * 86_400_000;
        let end = start + 86_400_000;
        let row = |resource: &str, complete| okx_observation::AccountHistoryCoverage {
            resource: resource.to_owned(),
            documented_window: "last_3_months",
            instrument_count: 0,
            sample_instruments: Vec::new(),
            rows: 0,
            pages: 1,
            complete_within_bound: complete,
            newest_event_time_ms: None,
            oldest_event_time_ms: None,
        };
        let coverage = [
            row("positions_history:SWAP", true),
            row("positions_history:FUTURES", true),
        ];
        let from = start.to_string();
        let until = end.to_string();
        assert!(daily_history_complete_at(
            &coverage,
            Some(&from),
            Some(&until),
            start + 1,
        ));
        assert!(!daily_history_complete_at(
            &coverage,
            Some(&from),
            Some(&until),
            end,
        ));
        assert!(!daily_history_complete_at(
            &coverage,
            None,
            Some(&until),
            start + 1,
        ));
        assert!(!daily_history_complete_at(
            &[],
            Some(&from),
            Some(&until),
            start + 1,
        ));
        assert!(!daily_history_complete_at(
            &[row("positions_history:SWAP", false)],
            Some(&from),
            Some(&until),
            start + 1,
        ));
        assert!(!daily_history_complete_at(
            &[row("orders_history:SWAP", true)],
            Some(&from),
            Some(&until),
            start + 1,
        ));
    }

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
    fn full_execution_status_v3_stays_within_compact_transport_budget() {
        let mut entry = entry();
        entry.record.lineage = Some(okx_execution::ExecutionLineageBinding {
            schema: EXECUTION_LINEAGE_SCHEMA_V2.to_owned(),
            origin_evidence_id: format!("sha256:{}", "a".repeat(64)),
            origin_schema: "o".repeat(128),
            origin_version: "v".repeat(128),
            authority_evidence_id: Some(format!("sha256:{}", "b".repeat(64))),
            decision_reference: okx_execution::ExecutionDecisionReference {
                decision_time_ms: 1_791_300_000_000,
                price: "0.123456789012345678901234567890".to_owned(),
                price_basis: okx_execution::TcaReferencePriceBasis::DecisionPrice,
                price_policy_version: "p".repeat(128),
            },
            tca_mechanics: Some(okx_execution::ExecutionTcaMechanicsBinding {
                source_reference_generation: "sha256:reference".to_owned(),
                instrument_type: okx_execution::ExecutionTcaInstrumentType::Swap,
                contract_type: "linear".to_owned(),
                contract_value: "0.001".to_owned(),
                settle_currency: "USDT".to_owned(),
            }),
        });
        entry.record.protection = Some(okx_execution::ProtectiveOrderLink {
            policy_version: "okx.protective-order/mark-market-v1".to_owned(),
            algo_client_order_id: "prx01234567890123456789012345678".to_owned(),
            trigger_price_basis: okx_execution::ProtectiveTriggerPriceBasis::Mark,
            status: okx_execution::ProtectiveOrderStatus::Active,
            algo_order_id: Some("12345678901234567890".to_owned()),
            covered_size: Some("1234567890.123456789012345678".to_owned()),
            failure_code: None,
            cleanup: None,
        });
        entry.record.submission_timing = Some(okx_execution::ExecutionSubmissionTimingEvidence {
            request_exchange_time_ms: 1_791_300_000_000,
            okx_in_time_us: Some(1_791_300_000_100_000),
            okx_out_time_us: Some(1_791_300_000_100_999),
        });

        let status = okx_execution::ExecutionStatusEnvelope {
            schema: EXECUTION_STATUS_SCHEMA_V3,
            execution: okx_execution::execution_status(&entry).expect("status"),
            reverse: None,
            lineage: entry.record.lineage.clone(),
            protection: entry.record.protection.as_ref().map(|value| {
                okx_execution::ProtectiveExecutionStatus {
                    policy_version: value.policy_version.clone(),
                    algo_client_order_id: value.algo_client_order_id.clone(),
                    trigger_price_basis: value.trigger_price_basis,
                    status: value.status,
                    algo_order_id_present: value.algo_order_id.is_some(),
                    covered_size: value.covered_size.clone(),
                    failure_code: value.failure_code.clone(),
                }
            }),
            submission_timing: entry
                .record
                .submission_timing
                .as_ref()
                .map(okx_execution::ExecutionSubmissionTimingStatus::from),
        };
        let tca = ExecutionTcaStatus::with_history(
            ExecutionTcaAvailability::Available,
            "final",
            1,
            true,
            100,
            Some(ExecutionTcaSummary {
                requested_contracts: "1234567890.123456789012345678".to_owned(),
                filled_contracts: "1234567890.123456789012345678".to_owned(),
                unfilled_contracts: "0".to_owned(),
                fill_ratio: "1".to_owned(),
                fill_outcome: TcaFillOutcome::Complete,
                fill_vwap: Some("1234567890.123456789012345678".to_owned()),
                maker_contracts: Some("617283945.061728394506172839".to_owned()),
                taker_contracts: Some("617283945.061728394506172839".to_owned()),
                slippage_bps: Some("-1234567890.123456789012345678".to_owned()),
                gross_slippage_settle: Some("-1234567890.123456789012345678".to_owned()),
                settle_fee_cost: Some("1234567890.123456789012345678".to_owned()),
                net_execution_cost_settle: Some("1234567890.123456789012345678".to_owned()),
                reference_to_first_fill_ms: Some(u64::MAX),
                reference_to_last_fill_ms: Some(u64::MAX),
                implementation_shortfall_settle: Some("1234567890.123456789012345678".to_owned()),
                implementation_shortfall_unavailable_reason: None,
            }),
            None,
        );
        let response = completed(
            &request(),
            GENERATED_AT,
            EXECUTION_STATUS_SCHEMA_V3,
            serde_json::to_value(ExecutionStatusResult {
                status,
                tca,
                unattributed_analytics: ExecutionUnattributedAnalytics {
                    post_fill_markout: "unavailable_no_versioned_post_fill_market_evidence",
                    funding: "unavailable_no_per_intent_funding_attribution",
                    counterfactual: "unavailable_no_versioned_counterfactual_evidence",
                },
            })
            .expect("result"),
        );
        let bytes = serde_json::to_vec(&response).expect("response");
        assert!(
            bytes.len() < 8 * 1024,
            "execution status response must stay below compact budget, got {} bytes",
            bytes.len()
        );
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

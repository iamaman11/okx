mod contract;
mod exchange_evidence;
mod executor;
mod ledger;
mod model;
mod reconciliation;
mod state;
mod strategy_finance;
mod strategy_lifecycle;
mod validation;

pub use contract::{
    EXECUTION_STATUS_CORE_SCHEMA_V2, EXECUTION_STATUS_SCHEMA_V1, EXECUTION_STATUS_SCHEMA_V2,
    EXECUTION_STATUS_SCHEMA_V3, ExecutionStatusEnvelope, ExecutionStatusSnapshot,
    ExecutionSubmissionTimingStatus, PrepareDeferral, PrepareFailure, PrepareOutcome,
    PrepareRejection, ProtectiveExecutionStatus, ProtectiveSizeAmendStatus, ReverseExecutionStage,
    ReverseExecutionStatus, classify_prepare_result, execution_status,
    execution_status_with_ledger,
};
pub use exchange_evidence::{
    DurableExchangeEvidence, EXCHANGE_EVIDENCE_SCHEMA_V1, ExchangeEvidenceSummary,
};
pub use executor::{
    ExecutionGateway, FencedProtectiveShrinkRequest, MutationAuthority, MutationSubmitDisposition,
    OrderExecutor, OrderExecutorError, ReconcileDisposition, SubmitDisposition,
};
pub use ledger::{
    DurableExecutionLedger, EXECUTION_LEDGER_SCHEMA_V1, EXECUTION_LEDGER_SCHEMA_V2,
    EXECUTION_LEDGER_SCHEMA_V3, EXECUTION_LEDGER_SCHEMA_V4, EXECUTION_LEDGER_SCHEMA_V5,
    EXECUTION_LEDGER_SCHEMA_V6, EXECUTION_LEDGER_SCHEMA_V7, EXECUTION_LEDGER_SCHEMA_V8,
    EXECUTION_RISK_STOP_SCHEMA_V1, ExecutionLedgerEntry, ExecutionLedgerError,
    ExecutionLedgerStore, ExecutionRiskStop, MAX_EXECUTION_LEDGER_RECORDS,
    MutationPrepareDisposition, PrepareDisposition, STRATEGY_CAPITAL_RESERVATION_SCHEMA_V1,
    StrategyAllocationOpening, StrategyCapitalReservation,
};
pub use model::{
    EXECUTION_LINEAGE_SCHEMA_V1, EXECUTION_LINEAGE_SCHEMA_V2, EXECUTION_PLAN_SCHEMA_V1,
    ExecutionAction, ExecutionDecisionReference, ExecutionIntent, ExecutionLineageBinding,
    ExecutionPlan, ExecutionRiskBinding, ExecutionTcaInstrumentType, ExecutionTcaMechanicsBinding,
    OpenRiskEvidence, OrderSide, OrderType, PositionSide, TradeMode, derive_amend_request_id,
    derive_client_order_id, derive_protective_algo_client_id, derive_reverse_open_intent_id,
};
pub use okx_analysis::TcaReferencePriceBasis;
pub use reconciliation::{
    ACCOUNT_LEDGER_RECONCILIATION_SCHEMA_V1, AccountLedgerReconciliation,
    AccountLedgerReconciliationError, LinkedCurrencyAmount, MANAGED_EXECUTION_INVENTORY_SCHEMA_V1,
    MAX_MANAGED_EXECUTION_INVENTORY_ROWS, ManagedExecutionIdentity, ManagedExecutionInventory,
    PositionAttributionDiagnostic, STRATEGY_ATTRIBUTION_SCHEMA_V1,
    STRATEGY_NAV_ASSESSMENT_SCHEMA_V1, StrategyExchangeAttribution, StrategyNavAssessment,
    managed_execution_inventory, reconcile_account_ledger,
};
pub use state::{
    ALLOW_LIVE_TRADING_DEFAULT, ExchangeOrderState, ExecutionRecord, ExecutionState,
    ExecutionSubmissionTimingEvidence, ExecutionTransitionError, MAX_ORDER_MUTATIONS_PER_EXECUTION,
    OrderMutationKind, OrderMutationRecord, OrderMutationResolution, OrderMutationState,
    PROTECTIVE_ORDER_POLICY_V1, ProtectiveCleanupRecord, ProtectiveCleanupState,
    ProtectiveOrderLink, ProtectiveOrderResolution, ProtectiveOrderStatus,
    ProtectiveSizeAmendRecord, ProtectiveSizeAmendState, ProtectiveTriggerPriceBasis,
    ReverseContinuation, ReverseExecutionLink, ReverseLeg, require_live_trading_enabled,
};
pub use strategy_finance::{
    ProductionMarkEvidence, RunOwnedPosition, STRATEGY_FINANCIAL_PERIOD_SCHEMA_V1,
    StrategyFinancialPeriod, assess_persisted_financial_period,
    assess_persisted_financial_period_with_production_marks,
    assess_persisted_financial_period_with_verified_source,
};
pub use strategy_lifecycle::{
    ARCHIVED_STRATEGY_PROOF_SCHEMA_V1, ArchivedStrategyProof, ArchivedStrategyRun,
    MAX_ARCHIVED_STRATEGY_RUNS, MAX_STRATEGY_NAV_POINTS, STRATEGY_FINALIZATION_SCHEMA_V1,
    STRATEGY_NAV_POINT_SCHEMA_V1, StrategyFinalization, StrategyNavPoint, StrategyPeriodHistory,
};
pub use validation::{
    ExecutionValidationError, PreMutationRiskDisposition, prepare_execution,
    revalidate_amend_execution_plan, revalidate_execution_plan, revalidate_hard_risk_policy,
    revalidate_venue_execution,
};

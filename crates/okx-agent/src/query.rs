use chrono::Utc;
use okx_analysis::{
    AnalysisError, CANDIDATE_ORDER_ANALYSIS_SCHEMA_V1, COST_ANALYSIS_SCHEMA_V1,
    CandidateOrderAssumptions, CorrelatedClusterLimit, DATED_FUTURE_BASIS_SCHEMA_V1,
    HARD_RISK_POLICY_SCHEMA_V1, HISTORY_BEHAVIOR_SCHEMA_V1, HardRiskPolicy,
    LiquidityRole as AnalysisLiquidityRole, MARKET_INTELLIGENCE_ANALYSIS_SCHEMA_V1,
    PORTFOLIO_RISK_ANALYSIS_SCHEMA_V3, POSITION_SCENARIO_SCHEMA_V1, PortfolioCandidate,
    PortfolioStatisticsAnalysis, PositionDirection, PositionScenarioAssumptions,
    RiskDegradedMode as AnalysisRiskDegradedMode, RiskMinimumQuality as AnalysisRiskMinimumQuality,
    ScenarioExitAssumption, StatisticalExposure, TRADING_MANDATE_SCHEMA_V1, TradingMandate,
    analyze_basis_difference_bps, analyze_candidate_order, analyze_cost,
    analyze_dated_future_basis, analyze_history_behavior, analyze_mark_index_basis_bps,
    analyze_market_intelligence, analyze_portfolio_risk, analyze_portfolio_statistics,
    analyze_position_scenario, compare_account_position_risk_oracle,
};
use okx_github::{ISSUE_POLL_TELEMETRY_SCHEMA_V1, IssuePollTelemetryStatus};
use okx_observation::{
    ACCOUNT_SNAPSHOT_SCHEMA_V1, ACCOUNT_SNAPSHOT_SCHEMA_V2, AccountError, AccountSnapshot,
    FundingHistorySnapshot, INSTRUMENT_RULES_SCHEMA_V1, INSTRUMENT_SEARCH_SCHEMA_V1,
    InstrumentRulesSnapshot, MARKET_HISTORY_SCHEMA_V1, MARKET_SNAPSHOT_SCHEMA_V1, MarketError,
    MarketHistoryError, MarketHistorySnapshot, MarketReadiness, MarketSnapshot,
    MarketTradesSnapshot, OpenInterestHistorySnapshot, ReferenceRegistry,
    SNAPSHOT_QUALITY_SCHEMA_V1, SnapshotQualityReport, TRADING_CAPABILITIES_SCHEMA_V1,
    market_research_source_generation,
};
use okx_protocol::{
    AGENT_RESPONSE_SCHEMA_V1, AgentFailure, AgentOperation, AgentRequest, AgentResponse,
    AgentResponseStatus, DataQuality, HardRiskPolicyRequest, InstrumentTypeFilter,
    LiquidityRole as ProtocolLiquidityRole, PortfolioMandateRequest, PositionSide,
    RiskDegradedMode as ProtocolRiskDegradedMode, RiskMinimumQuality as ProtocolRiskMinimumQuality,
};
use okx_runtime::{
    PUBLIC_SNAPSHOT_QUALITY_SCHEMA_V2, PrivateConvergenceError, PrivateWsHandle,
    PublicQualitySnapshot, PublicWsHandle,
};

use crate::{
    AgentResult,
    account_bootstrap::{
        AccountBootstrapError, AccountBootstrapper, AccountLedgerBootstrapError,
        FeeScheduleBootstrapError, TradingCapabilitiesBootstrapError,
    },
    execution_runtime::ExecutionRuntime,
    market_bootstrap::{MarketBootstrapError, MarketBootstrapper},
};

mod account;
mod analysis;
mod execution;
mod market;
mod universal;

pub const P1_NOT_AVAILABLE_CODE: &str = "P1_OPERATION_NOT_AVAILABLE";
pub const REFERENCE_INSTRUMENT_NOT_FOUND_CODE: &str = "REFERENCE_INSTRUMENT_NOT_FOUND";
pub const MARKET_REFERENCE_INCOMPLETE_CODE: &str = "MARKET_REFERENCE_INCOMPLETE";
pub const MARKET_PUBLIC_API_UNAVAILABLE_CODE: &str = "MARKET_PUBLIC_API_UNAVAILABLE";
pub const MARKET_BOOTSTRAP_INCONSISTENT_CODE: &str = "MARKET_BOOTSTRAP_INCONSISTENT";
pub const MARKET_INSTRUMENT_NOT_LIVE_CODE: &str = "MARKET_INSTRUMENT_NOT_LIVE";
pub const MARKET_OVERVIEW_INCONSISTENT_CODE: &str = "MARKET_OVERVIEW_INCONSISTENT";
pub const MARKET_INTELLIGENCE_NOT_READY_CODE: &str = "MARKET_INTELLIGENCE_NOT_READY";
pub const MARKET_INTELLIGENCE_INCONSISTENT_CODE: &str = "MARKET_INTELLIGENCE_INCONSISTENT";
pub const MARKET_HISTORY_INCONSISTENT_CODE: &str = "MARKET_HISTORY_INCONSISTENT";
pub const MARKET_RESEARCH_INCONSISTENT_CODE: &str = "MARKET_RESEARCH_INCONSISTENT";
pub const ACCOUNT_OBSERVER_CREDENTIAL_UNAVAILABLE_CODE: &str =
    "ACCOUNT_OBSERVER_CREDENTIAL_UNAVAILABLE";
pub const ACCOUNT_OBSERVER_PERMISSION_REJECTED_CODE: &str = "ACCOUNT_OBSERVER_PERMISSION_REJECTED";
pub const ACCOUNT_PRIVATE_API_UNAVAILABLE_CODE: &str = "ACCOUNT_PRIVATE_API_UNAVAILABLE";
pub const ACCOUNT_BOOTSTRAP_INCONSISTENT_CODE: &str = "ACCOUNT_BOOTSTRAP_INCONSISTENT";
pub const ACCOUNT_TRADING_CAPABILITIES_INCONSISTENT_CODE: &str =
    "ACCOUNT_TRADING_CAPABILITIES_INCONSISTENT";
pub const ACCOUNT_LEDGER_INCONSISTENT_CODE: &str = "ACCOUNT_LEDGER_INCONSISTENT";
pub const PORTFOLIO_RISK_ORACLE_MISMATCH_CODE: &str = "PORTFOLIO_RISK_ORACLE_MISMATCH";
pub const PORTFOLIO_RISK_REFERENCE_INCONSISTENT_CODE: &str =
    "PORTFOLIO_RISK_REFERENCE_INCONSISTENT";
pub const PORTFOLIO_RISK_POLICY_REJECTED_CODE: &str = "PORTFOLIO_RISK_POLICY_REJECTED";
pub const ACCOUNT_SUMMARY_SCHEMA_V1: &str = "okx.account-summary/v1";
pub const PORTFOLIO_RISK_SCHEMA_V3: &str = "okx.portfolio-risk/v3";
pub const PORTFOLIO_RISK_SCHEMA_V4: &str = "okx.portfolio-risk/v4";
pub const PORTFOLIO_RISK_SOURCE_TIME_INCONSISTENT_CODE: &str =
    "PORTFOLIO_RISK_SOURCE_TIME_INCONSISTENT";
pub const ANALYSIS_INPUT_INCONSISTENT_CODE: &str = "ANALYSIS_INPUT_INCONSISTENT";
pub const ANALYSIS_EXACT_FEE_UNAVAILABLE_CODE: &str = "ANALYSIS_EXACT_FEE_UNAVAILABLE";
pub const MARKET_OVERVIEW_SCHEMA_V1: &str = "okx.market-overview/v1";
pub const MARKET_INTELLIGENCE_SCHEMA_V1: &str = "okx.market-intelligence/v1";
pub const MARKET_RESEARCH_SCHEMA_V3: &str = "okx.market-research/v3";

const REFERENCE_BOOTSTRAP_WARNING: &str =
    "reference data is REST-bootstrap only; live instruments continuity is not connected until M3";
const MARKET_REST_BOOTSTRAP_WARNING: &str = "market data is bounded public REST bootstrap; persistent WebSocket continuity is not connected until M3";
const REFERENCE_RUNTIME_WARNING: &str = "instrument rules come from the live ReferenceRegistry; market FRESH readiness is reported separately";
const MARKET_HISTORY_UNCONFIRMED_WARNING: &str =
    "OKX history response contains at least one unconfirmed candlestick";
const ACCOUNT_REST_BOOTSTRAP_WARNING: &str = "private account state is a bounded authenticated REST bootstrap; private WebSocket convergence is not ready";
const ACCOUNT_WS_GENERATION_CHANGED_WARNING: &str = "private WebSocket generation changed during REST bootstrap; returning coherent REST snapshot only";
const ACCOUNT_WS_JOURNAL_GAP_WARNING: &str = "private WebSocket delta journal advanced beyond the REST bootstrap cursor; returning coherent REST snapshot only";
const CANDIDATE_EXPLICIT_ASSUMPTIONS_WARNING: &str = "candidate analysis uses explicit hypothetical entry/stop prices and exact account fee evidence; it does not assume a current fill price, funding event, slippage, spread, margin or FX conversion";
const POSITION_SCENARIO_EXPLICIT_ASSUMPTIONS_WARNING: &str = "position scenario uses explicit hypothetical entry/exit assumptions and exact account fee evidence; funding, slippage, spread, margin, FX conversion and execution price are not included";
const CURRENT_COST_MARK_OBSERVATION_WARNING: &str = "current cost uses the current mark/reference market snapshot and exact account fee evidence; it is not a promised execution price, and funding is current event evidence only with no holding horizon";
pub const PUBLIC_MARKET_MAX_AGE_MS: u64 = 120_000;
pub const MARKET_INTELLIGENCE_MAX_AGE_MS: u64 = 60_000;

#[derive(serde::Serialize)]
struct MarketOverviewResult {
    instrument_rules: InstrumentRulesSnapshot,
    market: MarketSnapshot,
    quality: PublicQualitySnapshot,
    market_source: &'static str,
}

#[derive(Clone, Copy)]
pub struct ObservationQueryContext<'a> {
    standalone_reference: Option<&'a ReferenceRegistry>,
    market_fallback: Option<&'a MarketBootstrapper>,
    public_ws: Option<&'a PublicWsHandle>,
    mailbox_telemetry: Option<&'a IssuePollTelemetryStatus>,
    account_fallback: Option<&'a AccountBootstrapper>,
    private_ws: Option<&'a PrivateWsHandle>,
    execution: Option<&'a ExecutionRuntime>,
}

impl<'a> ObservationQueryContext<'a> {
    pub const fn unavailable() -> Self {
        Self {
            standalone_reference: None,
            market_fallback: None,
            public_ws: None,
            mailbox_telemetry: None,
            account_fallback: None,
            private_ws: None,
            execution: None,
        }
    }

    pub const fn standalone(
        reference: &'a ReferenceRegistry,
        market: &'a MarketBootstrapper,
    ) -> Self {
        Self::standalone_with_account(reference, market, None)
    }

    pub const fn standalone_with_account(
        reference: &'a ReferenceRegistry,
        market: &'a MarketBootstrapper,
        account_fallback: Option<&'a AccountBootstrapper>,
    ) -> Self {
        Self {
            standalone_reference: Some(reference),
            market_fallback: Some(market),
            public_ws: None,
            mailbox_telemetry: None,
            account_fallback,
            private_ws: None,
            execution: None,
        }
    }

    pub const fn live(
        public_ws: &'a PublicWsHandle,
        market_fallback: &'a MarketBootstrapper,
    ) -> Self {
        Self::live_with_private(public_ws, market_fallback, None, None, None)
    }

    pub const fn live_with_mailbox_telemetry(
        public_ws: &'a PublicWsHandle,
        market_fallback: &'a MarketBootstrapper,
        mailbox_telemetry: Option<&'a IssuePollTelemetryStatus>,
    ) -> Self {
        Self::live_with_private(public_ws, market_fallback, mailbox_telemetry, None, None)
    }

    pub const fn live_with_private(
        public_ws: &'a PublicWsHandle,
        market_fallback: &'a MarketBootstrapper,
        mailbox_telemetry: Option<&'a IssuePollTelemetryStatus>,
        account_fallback: Option<&'a AccountBootstrapper>,
        private_ws: Option<&'a PrivateWsHandle>,
    ) -> Self {
        Self::live_with_execution(
            public_ws,
            market_fallback,
            mailbox_telemetry,
            account_fallback,
            private_ws,
            None,
        )
    }

    pub const fn live_with_execution(
        public_ws: &'a PublicWsHandle,
        market_fallback: &'a MarketBootstrapper,
        mailbox_telemetry: Option<&'a IssuePollTelemetryStatus>,
        account_fallback: Option<&'a AccountBootstrapper>,
        private_ws: Option<&'a PrivateWsHandle>,
        execution: Option<&'a ExecutionRuntime>,
    ) -> Self {
        Self {
            standalone_reference: None,
            market_fallback: Some(market_fallback),
            public_ws: Some(public_ws),
            mailbox_telemetry,
            account_fallback,
            private_ws,
            execution,
        }
    }
}

fn trading_mandate(value: &PortfolioMandateRequest) -> TradingMandate {
    TradingMandate {
        schema: TRADING_MANDATE_SCHEMA_V1.to_owned(),
        version: value.version.clone(),
        capital_base_usd: value.capital_base_usd.clone(),
        decision_horizon_hours: value.decision_horizon_hours,
        benchmark: value.benchmark.clone(),
        allowed_instruments: value.allowed_instruments.clone(),
        max_drawdown_ratio: value.max_drawdown_ratio.clone(),
        leverage_ceiling: value.leverage_ceiling.clone(),
        minimum_liquidity_notional_usd: value.minimum_liquidity_notional_usd.clone(),
        max_turnover_ratio: value.max_turnover_ratio.clone(),
    }
}

fn hard_risk_policy(value: &HardRiskPolicyRequest) -> HardRiskPolicy {
    HardRiskPolicy {
        schema: HARD_RISK_POLICY_SCHEMA_V1.to_owned(),
        version: value.version.clone(),
        max_account_gross_notional_usd: value.max_account_gross_notional_usd.clone(),
        max_instrument_gross_notional_usd: value.max_instrument_gross_notional_usd.clone(),
        max_margin_utilization_ratio: value.max_margin_utilization_ratio.clone(),
        max_loss_per_trade_usd: value.max_loss_per_trade_usd.clone(),
        max_daily_realized_loss_usd: value.max_daily_realized_loss_usd.clone(),
        max_drawdown_ratio: value.max_drawdown_ratio.clone(),
        max_leverage: value.max_leverage.clone(),
        allowed_instruments: value.allowed_instruments.clone(),
        minimum_quality: match value.minimum_quality {
            ProtocolRiskMinimumQuality::Fresh => AnalysisRiskMinimumQuality::Fresh,
            ProtocolRiskMinimumQuality::Degraded => AnalysisRiskMinimumQuality::Degraded,
        },
        degraded_mode: match value.degraded_mode {
            ProtocolRiskDegradedMode::Reject => AnalysisRiskDegradedMode::Reject,
            ProtocolRiskDegradedMode::AllowReadOnly => AnalysisRiskDegradedMode::AllowReadOnly,
        },
        correlated_clusters: value
            .correlated_clusters
            .iter()
            .map(|cluster| CorrelatedClusterLimit {
                id: cluster.id.clone(),
                instruments: cluster.instruments.clone(),
                max_gross_notional_usd: cluster.max_gross_notional_usd.clone(),
            })
            .collect(),
    }
}

pub(crate) async fn dispatch(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
) -> AgentResult<AgentResponse> {
    match &request.operation {
        AgentOperation::MarketSnapshot { .. }
        | AgentOperation::InstrumentRules { .. }
        | AgentOperation::FindInstruments { .. }
        | AgentOperation::MarketOverview { .. }
        | AgentOperation::MarketIntelligence { .. }
        | AgentOperation::MarketResearch { .. }
        | AgentOperation::MarketHistory { .. }
        | AgentOperation::HistoryBehavior { .. }
        | AgentOperation::SnapshotQuality { .. } => {
            market::dispatch(request, context, generated_at).await
        }
        AgentOperation::QueryCapabilities | AgentOperation::Query { .. } => {
            universal::dispatch(request, context, generated_at).await
        }
        AgentOperation::AccountSnapshot
        | AgentOperation::AccountSummary
        | AgentOperation::PortfolioRisk { .. }
        | AgentOperation::TradingCapabilities { .. } => {
            account::dispatch(request, context, generated_at).await
        }
        AgentOperation::ExecutorPreflight
        | AgentOperation::PrepareOpenExecution { .. }
        | AgentOperation::PrepareCloseExecution { .. }
        | AgentOperation::SubmitPreparedExecution { .. }
        | AgentOperation::ExecutionStatus { .. } => {
            execution::dispatch(request, context, generated_at).await
        }
        AgentOperation::CurrentCost { .. }
        | AgentOperation::PositionScenario { .. }
        | AgentOperation::AnalyzeCandidateOrder { .. } => {
            analysis::dispatch(request, context, generated_at).await
        }
        AgentOperation::MailboxTelemetry => {
            let Some(telemetry) = context.mailbox_telemetry else {
                return Ok(unavailable(request, generated_at));
            };
            Ok(AgentResponse {
                schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                request_id: request.request_id.clone(),
                status: AgentResponseStatus::Completed,
                generated_at: generated_at.to_owned(),
                quality: DataQuality::Fresh,
                result_schema: Some(ISSUE_POLL_TELEMETRY_SCHEMA_V1.to_owned()),
                result: Some(serde_json::to_value(telemetry)?),
                failure: None,
                warnings: Vec::new(),
            })
        }
    }
}

struct AssembledCurrentMarket {
    rules: InstrumentRulesSnapshot,
    snapshot: MarketSnapshot,
    source: &'static str,
    quality: DataQuality,
    warnings: Vec<String>,
}

enum CurrentMarketAssembly {
    Ready(Box<AssembledCurrentMarket>),
    Response(Box<AgentResponse>),
    Unavailable,
}

async fn assemble_current_market(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    instrument: &str,
) -> AgentResult<CurrentMarketAssembly> {
    if let Some(public_ws) = context.public_ws {
        let Some(rules) = public_ws.instrument_rules(instrument).await else {
            return Ok(CurrentMarketAssembly::Response(Box::new(
                reference_not_found(request, generated_at, instrument),
            )));
        };
        public_ws.demand_instrument(instrument.to_owned()).await?;

        let now_ms = utc_now_ms();
        let quality = public_ws
            .quality_snapshot(instrument, now_ms, PUBLIC_MARKET_MAX_AGE_MS, true)
            .await?;

        if quality.quality == MarketReadiness::Fresh {
            let live = public_ws
                .fresh_snapshot(
                    instrument,
                    now_ms,
                    PUBLIC_MARKET_MAX_AGE_MS,
                    generated_at.to_owned(),
                )
                .await?;
            return Ok(CurrentMarketAssembly::Ready(Box::new(
                AssembledCurrentMarket {
                    rules,
                    snapshot: live.market,
                    source: "websocket",
                    quality: DataQuality::Fresh,
                    warnings: Vec::new(),
                },
            )));
        }

        let Some(market) = context.market_fallback else {
            return Ok(CurrentMarketAssembly::Response(Box::new(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                MARKET_PUBLIC_API_UNAVAILABLE_CODE,
                format!(
                    "persistent WebSocket state is not FRESH: {}",
                    quality.reason
                ),
                true,
            ))));
        };
        let reference = public_ws.reference_snapshot().await;
        return match market.snapshot(&reference, instrument).await {
            Ok(snapshot) => Ok(CurrentMarketAssembly::Ready(Box::new(
                AssembledCurrentMarket {
                    rules,
                    snapshot,
                    source: "rest_fallback",
                    quality: DataQuality::Degraded,
                    warnings: vec![format!(
                        "persistent WebSocket state is not FRESH ({}); returned bounded public REST fallback",
                        quality.reason
                    )],
                },
            ))),
            Err(error) => Ok(CurrentMarketAssembly::Response(Box::new(market_failure(
                request,
                generated_at,
                error,
            )))),
        };
    }

    let (Some(reference), Some(market)) = (context.standalone_reference, context.market_fallback)
    else {
        return Ok(CurrentMarketAssembly::Unavailable);
    };
    let Some(rules) = reference.instrument_rules(instrument) else {
        return Ok(CurrentMarketAssembly::Response(Box::new(
            reference_not_found(request, generated_at, instrument),
        )));
    };

    match market.snapshot(reference, instrument).await {
        Ok(snapshot) => Ok(CurrentMarketAssembly::Ready(Box::new(
            AssembledCurrentMarket {
                rules,
                snapshot,
                source: "rest_bootstrap",
                quality: DataQuality::Degraded,
                warnings: vec![MARKET_REST_BOOTSTRAP_WARNING.to_owned()],
            },
        ))),
        Err(error) => Ok(CurrentMarketAssembly::Response(Box::new(market_failure(
            request,
            generated_at,
            error,
        )))),
    }
}

struct AssembledMarketHistory {
    snapshot: MarketHistorySnapshot,
    quality: DataQuality,
    warnings: Vec<String>,
}

async fn assemble_market_history(
    context: ObservationQueryContext<'_>,
    instrument: &str,
    bar: &str,
    requested_limit: u16,
) -> Result<Option<AssembledMarketHistory>, MarketBootstrapError> {
    let Some(market) = context.market_fallback else {
        return Ok(None);
    };
    let reference = if let Some(public_ws) = context.public_ws {
        public_ws.reference_snapshot().await
    } else if let Some(reference) = context.standalone_reference {
        reference.clone()
    } else {
        return Ok(None);
    };

    let snapshot = market
        .history(&reference, instrument, bar, requested_limit)
        .await?;
    let all_confirmed = snapshot.all_confirmed;
    Ok(Some(AssembledMarketHistory {
        snapshot,
        quality: if all_confirmed {
            DataQuality::Fresh
        } else {
            DataQuality::Degraded
        },
        warnings: if all_confirmed {
            Vec::new()
        } else {
            vec![MARKET_HISTORY_UNCONFIRMED_WARNING.to_owned()]
        },
    }))
}

struct AssembledOpenInterestHistory {
    snapshot: OpenInterestHistorySnapshot,
    quality: DataQuality,
}

async fn assemble_open_interest_history(
    context: ObservationQueryContext<'_>,
    instrument: &str,
    period: &str,
    requested_limit: u16,
) -> Result<Option<AssembledOpenInterestHistory>, MarketBootstrapError> {
    let Some(market) = context.market_fallback else {
        return Ok(None);
    };
    let reference = if let Some(public_ws) = context.public_ws {
        public_ws.reference_snapshot().await
    } else if let Some(reference) = context.standalone_reference {
        reference.clone()
    } else {
        return Ok(None);
    };

    let snapshot = market
        .open_interest_history(&reference, instrument, period, requested_limit)
        .await?;
    Ok(Some(AssembledOpenInterestHistory {
        snapshot,
        quality: DataQuality::Fresh,
    }))
}

struct AssembledMarketTrades {
    snapshot: MarketTradesSnapshot,
    quality: DataQuality,
}

async fn assemble_recent_trades(
    context: ObservationQueryContext<'_>,
    instrument: &str,
    requested_limit: u16,
) -> Result<Option<AssembledMarketTrades>, MarketBootstrapError> {
    let Some(market) = context.market_fallback else {
        return Ok(None);
    };
    let reference = if let Some(public_ws) = context.public_ws {
        public_ws.reference_snapshot().await
    } else if let Some(reference) = context.standalone_reference {
        reference.clone()
    } else {
        return Ok(None);
    };

    let snapshot = market
        .recent_trades(&reference, instrument, requested_limit)
        .await?;
    Ok(Some(AssembledMarketTrades {
        snapshot,
        quality: DataQuality::Fresh,
    }))
}

struct AssembledFundingHistory {
    snapshot: FundingHistorySnapshot,
    quality: DataQuality,
}

async fn assemble_funding_history(
    context: ObservationQueryContext<'_>,
    instrument: &str,
    requested_limit: u16,
) -> Result<Option<AssembledFundingHistory>, MarketBootstrapError> {
    let Some(market) = context.market_fallback else {
        return Ok(None);
    };
    let reference = if let Some(public_ws) = context.public_ws {
        public_ws.reference_snapshot().await
    } else if let Some(reference) = context.standalone_reference {
        reference.clone()
    } else {
        return Ok(None);
    };

    let snapshot = market
        .funding_history(&reference, instrument, requested_limit)
        .await?;
    Ok(Some(AssembledFundingHistory {
        snapshot,
        quality: DataQuality::Fresh,
    }))
}

async fn resolve_instrument_rules(
    context: ObservationQueryContext<'_>,
    instrument: &str,
) -> Option<InstrumentRulesSnapshot> {
    if let Some(public_ws) = context.public_ws {
        public_ws.instrument_rules(instrument).await
    } else {
        context
            .standalone_reference
            .and_then(|reference| reference.instrument_rules(instrument))
    }
}

struct AssembledAccountSnapshot {
    snapshot: AccountSnapshot,
    quality: DataQuality,
    result_schema: &'static str,
    warnings: Vec<String>,
}

enum AccountQueryError {
    CredentialUnavailable,
    Bootstrap(AccountBootstrapError),
    Convergence(AccountError),
}

async fn assemble_account_snapshot(
    context: ObservationQueryContext<'_>,
) -> Result<AssembledAccountSnapshot, AccountQueryError> {
    let account = context
        .account_fallback
        .ok_or(AccountQueryError::CredentialUnavailable)?;
    let convergence_cursor = match context.private_ws {
        Some(private_ws) => private_ws.convergence_cursor().await.ok(),
        None => None,
    };
    let rest = account
        .snapshot()
        .await
        .map_err(AccountQueryError::Bootstrap)?;

    if let (Some(private_ws), Some(cursor)) = (context.private_ws, convergence_cursor) {
        match private_ws.convergence_window(cursor).await {
            Ok(window) => {
                if let (Some(connection_fingerprint), Some(last_inbound_ms)) = (
                    window.status.connection_id_fingerprint.as_deref(),
                    window.status.last_inbound_ms,
                ) {
                    let snapshot = rest
                        .converge_private_ws(
                            window.generation,
                            connection_fingerprint,
                            last_inbound_ms,
                            &window.events,
                        )
                        .map_err(AccountQueryError::Convergence)?;
                    return Ok(AssembledAccountSnapshot {
                        snapshot,
                        quality: DataQuality::Fresh,
                        result_schema: ACCOUNT_SNAPSHOT_SCHEMA_V2,
                        warnings: Vec::new(),
                    });
                }
            }
            Err(PrivateConvergenceError::GenerationChanged) => {
                return Ok(AssembledAccountSnapshot {
                    snapshot: rest,
                    quality: DataQuality::Degraded,
                    result_schema: ACCOUNT_SNAPSHOT_SCHEMA_V1,
                    warnings: vec![ACCOUNT_WS_GENERATION_CHANGED_WARNING.to_owned()],
                });
            }
            Err(PrivateConvergenceError::JournalGap) => {
                return Ok(AssembledAccountSnapshot {
                    snapshot: rest,
                    quality: DataQuality::Degraded,
                    result_schema: ACCOUNT_SNAPSHOT_SCHEMA_V1,
                    warnings: vec![ACCOUNT_WS_JOURNAL_GAP_WARNING.to_owned()],
                });
            }
            Err(PrivateConvergenceError::NotReady) => {}
        }
    }

    Ok(AssembledAccountSnapshot {
        snapshot: rest,
        quality: DataQuality::Degraded,
        result_schema: ACCOUNT_SNAPSHOT_SCHEMA_V1,
        warnings: vec![ACCOUNT_REST_BOOTSTRAP_WARNING.to_owned()],
    })
}

fn account_query_failure(
    request: &AgentRequest,
    generated_at: &str,
    error: AccountQueryError,
) -> AgentResponse {
    match error {
        AccountQueryError::CredentialUnavailable => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            ACCOUNT_OBSERVER_CREDENTIAL_UNAVAILABLE_CODE,
            "OKX observer credential is not provisioned in native secret storage".to_owned(),
            false,
        ),
        AccountQueryError::Bootstrap(error) => account_failure(request, generated_at, error),
        AccountQueryError::Convergence(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            ACCOUNT_BOOTSTRAP_INCONSISTENT_CODE,
            error.to_string(),
            false,
        ),
    }
}

fn trading_capabilities_failure(
    request: &AgentRequest,
    generated_at: &str,
    error: TradingCapabilitiesBootstrapError,
) -> AgentResponse {
    match error {
        TradingCapabilitiesBootstrapError::PermissionRejected => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            ACCOUNT_OBSERVER_PERMISSION_REJECTED_CODE,
            "OKX observer API key must have read_only permission only".to_owned(),
            false,
        ),
        TradingCapabilitiesBootstrapError::Api(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            ACCOUNT_PRIVATE_API_UNAVAILABLE_CODE,
            error.to_string(),
            true,
        ),
        TradingCapabilitiesBootstrapError::Fee(error) => {
            fee_schedule_failure(request, generated_at, error)
        }
        TradingCapabilitiesBootstrapError::Normalize(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            ACCOUNT_TRADING_CAPABILITIES_INCONSISTENT_CODE,
            error.to_string(),
            false,
        ),
    }
}

fn fee_schedule_failure(
    request: &AgentRequest,
    generated_at: &str,
    error: FeeScheduleBootstrapError,
) -> AgentResponse {
    match error {
        FeeScheduleBootstrapError::PermissionRejected => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            ACCOUNT_OBSERVER_PERMISSION_REJECTED_CODE,
            "OKX observer API key must have read_only permission only".to_owned(),
            false,
        ),
        FeeScheduleBootstrapError::Api(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            ACCOUNT_PRIVATE_API_UNAVAILABLE_CODE,
            error.to_string(),
            true,
        ),
        FeeScheduleBootstrapError::ReferenceIncomplete(field) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            ANALYSIS_EXACT_FEE_UNAVAILABLE_CODE,
            format!("instrument reference is missing required fee selector '{field}'"),
            false,
        ),
        FeeScheduleBootstrapError::ResponseInconsistent(message) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            ANALYSIS_EXACT_FEE_UNAVAILABLE_CODE,
            message,
            true,
        ),
        FeeScheduleBootstrapError::Normalize(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            ANALYSIS_INPUT_INCONSISTENT_CODE,
            error.to_string(),
            false,
        ),
    }
}

fn analysis_failure(
    request: &AgentRequest,
    generated_at: &str,
    status: AgentResponseStatus,
    error: AnalysisError,
) -> AgentResponse {
    failure_response(
        request,
        generated_at,
        status,
        ANALYSIS_INPUT_INCONSISTENT_CODE,
        error.to_string(),
        false,
    )
}

const fn analysis_liquidity_role(role: ProtocolLiquidityRole) -> AnalysisLiquidityRole {
    match role {
        ProtocolLiquidityRole::Maker => AnalysisLiquidityRole::Maker,
        ProtocolLiquidityRole::Taker => AnalysisLiquidityRole::Taker,
    }
}

fn data_quality(quality: MarketReadiness) -> DataQuality {
    match quality {
        MarketReadiness::NotReady => DataQuality::NotReady,
        MarketReadiness::Fresh => DataQuality::Fresh,
        MarketReadiness::Stale => DataQuality::Stale,
        MarketReadiness::Degraded => DataQuality::Degraded,
    }
}

fn utc_now_ms() -> u64 {
    Utc::now().timestamp_millis().max(0) as u64
}

fn account_failure(
    request: &AgentRequest,
    generated_at: &str,
    error: AccountBootstrapError,
) -> AgentResponse {
    match error {
        AccountBootstrapError::PermissionRejected => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            ACCOUNT_OBSERVER_PERMISSION_REJECTED_CODE,
            "OKX observer API key must have read_only permission only".to_owned(),
            false,
        ),
        AccountBootstrapError::Api(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            ACCOUNT_PRIVATE_API_UNAVAILABLE_CODE,
            error.to_string(),
            true,
        ),
        AccountBootstrapError::Normalize(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            ACCOUNT_BOOTSTRAP_INCONSISTENT_CODE,
            error.to_string(),
            false,
        ),
    }
}

fn market_failure(
    request: &AgentRequest,
    generated_at: &str,
    error: MarketBootstrapError,
) -> AgentResponse {
    match error {
        MarketBootstrapError::ReferenceInstrumentNotFound(instrument) => {
            reference_not_found(request, generated_at, &instrument)
        }
        MarketBootstrapError::MissingUnderlying(instrument) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            MARKET_REFERENCE_INCOMPLETE_CODE,
            format!("instrument '{instrument}' has no underlying/index id in reference data"),
            false,
        ),
        MarketBootstrapError::UnknownFundingRequirement {
            instrument_id,
            rule_type,
        } => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            MARKET_REFERENCE_INCOMPLETE_CODE,
            format!(
                "instrument '{instrument_id}' has unknown funding semantics for ruleType '{rule_type}'"
            ),
            false,
        ),
        MarketBootstrapError::Api(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            MARKET_PUBLIC_API_UNAVAILABLE_CODE,
            error.to_string(),
            true,
        ),
        MarketBootstrapError::Normalize(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            MARKET_BOOTSTRAP_INCONSISTENT_CODE,
            error.to_string(),
            true,
        ),
        MarketBootstrapError::HistoryNormalize(MarketHistoryError::InstrumentNotFound(
            instrument,
        )) => reference_not_found(request, generated_at, &instrument),
        MarketBootstrapError::HistoryNormalize(MarketHistoryError::InstrumentNotLive(
            instrument,
        )) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            MARKET_INSTRUMENT_NOT_LIVE_CODE,
            format!("instrument '{instrument}' is not live"),
            false,
        ),
        MarketBootstrapError::HistoryNormalize(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            MARKET_HISTORY_INCONSISTENT_CODE,
            error.to_string(),
            true,
        ),
    }
}

fn reference_not_found(
    request: &AgentRequest,
    generated_at: &str,
    instrument: &str,
) -> AgentResponse {
    failure_response(
        request,
        generated_at,
        AgentResponseStatus::Rejected,
        REFERENCE_INSTRUMENT_NOT_FOUND_CODE,
        format!("instrument '{instrument}' is not present in the reference registry"),
        false,
    )
}

fn failure_response(
    request: &AgentRequest,
    generated_at: &str,
    status: AgentResponseStatus,
    code: &str,
    message: String,
    retryable: bool,
) -> AgentResponse {
    AgentResponse {
        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
        request_id: request.request_id.clone(),
        status,
        generated_at: generated_at.to_owned(),
        quality: DataQuality::NotReady,
        result_schema: None,
        result: None,
        failure: Some(AgentFailure {
            code: code.to_owned(),
            message,
            retryable,
        }),
        warnings: Vec::new(),
    }
}

fn unavailable(request: &AgentRequest, generated_at: &str) -> AgentResponse {
    failure_response(
        request,
        generated_at,
        AgentResponseStatus::Rejected,
        P1_NOT_AVAILABLE_CODE,
        "typed request accepted by the P1 runtime shell; domain operation is not connected yet"
            .to_owned(),
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_api::PublicInstrument;
    use okx_protocol::AGENT_REQUEST_SCHEMA_V1;

    #[tokio::test]
    async fn instrument_rules_uses_reference_registry_and_reports_bootstrap_quality() {
        let request = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_rules_0123456789".to_owned(),
            operation: AgentOperation::InstrumentRules {
                instrument: "DOGE-USDT-SWAP".to_owned(),
            },
        };
        let registry = reference();

        let response = dispatch(
            &request,
            ObservationQueryContext {
                standalone_reference: Some(&registry),
                market_fallback: None,
                public_ws: None,
                mailbox_telemetry: None,
                account_fallback: None,
                private_ws: None,
                execution: None,
            },
            "2026-09-27T00:00:01.000Z",
        )
        .await
        .expect("response");

        assert_eq!(response.status, AgentResponseStatus::Completed);
        assert_eq!(response.quality, DataQuality::Degraded);
        assert_eq!(
            response.result_schema.as_deref(),
            Some(INSTRUMENT_RULES_SCHEMA_V1)
        );
        assert!(response.failure.is_none());
        assert_eq!(response.warnings, vec![REFERENCE_BOOTSTRAP_WARNING]);
    }

    #[tokio::test]
    async fn find_instruments_uses_reference_registry_without_guessing_ids() {
        let request = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_find_012345678901".to_owned(),
            operation: AgentOperation::FindInstruments {
                asset: "DOGE".to_owned(),
                settle_currency: Some("USDT".to_owned()),
                instrument_type: Some(InstrumentTypeFilter::Swap),
            },
        };
        let registry = reference();

        let response = dispatch(
            &request,
            ObservationQueryContext {
                standalone_reference: Some(&registry),
                market_fallback: None,
                public_ws: None,
                mailbox_telemetry: None,
                account_fallback: None,
                private_ws: None,
                execution: None,
            },
            "2026-09-27T00:00:01.000Z",
        )
        .await
        .expect("response");

        assert_eq!(response.status, AgentResponseStatus::Completed);
        assert_eq!(
            response.result_schema.as_deref(),
            Some(INSTRUMENT_SEARCH_SCHEMA_V1)
        );
        let result = response.result.expect("result");
        assert_eq!(
            result["instruments"].as_array().expect("instruments").len(),
            1
        );
        assert_eq!(result["instruments"][0]["instrument_id"], "DOGE-USDT-SWAP");
    }

    #[tokio::test]
    async fn snapshot_quality_explains_rest_only_m2_state() {
        let request = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_quality_012345678".to_owned(),
            operation: AgentOperation::SnapshotQuality {
                instrument: "DOGE-USDT-SWAP".to_owned(),
            },
        };
        let registry = reference();

        let response = dispatch(
            &request,
            ObservationQueryContext {
                standalone_reference: Some(&registry),
                market_fallback: None,
                public_ws: None,
                mailbox_telemetry: None,
                account_fallback: None,
                private_ws: None,
                execution: None,
            },
            "2026-09-27T00:00:01.000Z",
        )
        .await
        .expect("response");

        assert_eq!(response.status, AgentResponseStatus::Completed);
        assert_eq!(response.quality, DataQuality::Degraded);
        assert_eq!(
            response.result_schema.as_deref(),
            Some(SNAPSHOT_QUALITY_SCHEMA_V1)
        );
        assert_eq!(
            response.result.expect("result")["reason"],
            "M2_REST_BOOTSTRAP_ONLY"
        );
    }

    fn reference() -> ReferenceRegistry {
        ReferenceRegistry::from_public("2026-09-27T00:00:00.000Z", vec![swap()]).expect("registry")
    }

    fn swap() -> PublicInstrument {
        PublicInstrument {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            instrument_family: "DOGE-USDT".to_owned(),
            underlying: "DOGE-USDT".to_owned(),
            state: "live".to_owned(),
            rule_type: "normal".to_owned(),
            base_currency: String::new(),
            quote_currency: String::new(),
            settle_currency: "USDT".to_owned(),
            tick_size: "0.00001".to_owned(),
            lot_size: "0.01".to_owned(),
            min_size: "0.01".to_owned(),
            max_limit_size: "1000000".to_owned(),
            max_market_size: "100000".to_owned(),
            max_limit_amount: String::new(),
            max_market_amount: String::new(),
            contract_type: "linear".to_owned(),
            contract_value: "1000".to_owned(),
            contract_value_currency: "DOGE".to_owned(),
            fee_group_id: "4".to_owned(),
            lever: "50".to_owned(),
            list_time: "1700000000000".to_owned(),
            expiry_time: String::new(),
            initial_price_limit_pct: "0.05".to_owned(),
            floating_price_limit_pct: "0.03".to_owned(),
            maximum_price_limit_pct: "0.15".to_owned(),
            upcoming_parameter_changes: Vec::new(),
        }
    }
}

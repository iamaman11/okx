use std::str::FromStr;

use okx_analysis::{
    BASELINE_STRATEGY_VERSION_V1, BarDecisionInput, BaselineStrategyKind, CandidateRiskContext,
    HARD_RISK_POLICY_SCHEMA_V1, HardRiskPolicy, LiquidityRole, PortfolioCandidate,
    PositionDirection, PositionScenarioAssumptions, PositionScenarioMechanics, RiskDegradedMode,
    RiskMinimumQuality, RiskPolicyDecision, ScenarioExitAssumption, StrategyDecision,
    TRADING_MANDATE_SCHEMA_V1, TradingMandate, analyze_position_scenario_values,
    evaluate_baseline_strategy, evaluate_candidate_risk, funding_user_cost_quote,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    DatasetManifest, ReferenceCoverageStatus, ResearchCandle, ResearchError, ResearchFundingEvent,
    ResearchInstrumentReference, canonical_sha256,
};

pub const HYPOTHESIS_SCHEMA_V1: &str = "okx.research.hypothesis/v1";
pub const EXPERIMENT_SPEC_SCHEMA_V1: &str = "okx.research.experiment-spec/v1";
pub const EXPERIMENT_RESULT_SCHEMA_V1: &str = "okx.research.experiment-result/v1";
pub const REPLAY_EXECUTION_MODEL_VERSION_V1: &str = "okx.research.replay-execution/2026-10-04.1";
pub const REPLAY_DATASET_ARTIFACT_SCHEMA_V1: &str = "okx.research.replay-dataset/v1";

const ONE_HOUR_MS: u64 = 3_600_000;
const ONE_DAY_MS: u64 = 86_400_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayDatasetArtifact {
    pub schema: String,
    pub manifest: DatasetManifest,
    pub candles: Vec<ResearchCandle>,
    pub funding: Vec<ResearchFundingEvent>,
    pub reference: Option<ResearchInstrumentReference>,
}

impl ReplayDatasetArtifact {
    pub fn build(
        manifest: DatasetManifest,
        candles: Vec<ResearchCandle>,
        funding: Vec<ResearchFundingEvent>,
        reference: Option<ResearchInstrumentReference>,
    ) -> Result<Self, ResearchError> {
        if candles.is_empty() {
            return Err(ResearchError::ReplayMissingField("replay_dataset.candles"));
        }
        if reference
            .as_ref()
            .is_some_and(|value| value.instrument_id != manifest.instrument_id)
        {
            return Err(ResearchError::ReplayDatasetMismatch);
        }
        let artifact = Self {
            schema: REPLAY_DATASET_ARTIFACT_SCHEMA_V1.to_owned(),
            manifest,
            candles,
            funding,
            reference,
        };
        validate_replay_dataset_artifact(&artifact)?;
        Ok(artifact)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReplayMechanicsProvenance {
    HistoricalObserved,
    DeclaredCounterfactual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReplayEvidenceClass {
    DataOnly,
    HistoricalObserved,
    CounterfactualMechanics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReplayStatus {
    Completed,
    InsufficientData,
    InsufficientReferenceHistory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hypothesis {
    pub schema: String,
    pub hypothesis_id: String,
    pub statement: String,
    pub falsification_rule: String,
    pub strategy: BaselineStrategyKind,
    pub strategy_version: String,
}

#[derive(Serialize)]
struct HypothesisIdentity<'a> {
    schema: &'static str,
    statement: &'a str,
    falsification_rule: &'a str,
    strategy: BaselineStrategyKind,
    strategy_version: &'a str,
}

impl Hypothesis {
    pub fn build(
        statement: impl Into<String>,
        falsification_rule: impl Into<String>,
        strategy: BaselineStrategyKind,
        strategy_version: impl Into<String>,
    ) -> Result<Self, ResearchError> {
        let statement = statement.into();
        let falsification_rule = falsification_rule.into();
        let strategy_version = strategy_version.into();
        required("hypothesis.statement", &statement)?;
        required("hypothesis.falsification_rule", &falsification_rule)?;
        required("hypothesis.strategy_version", &strategy_version)?;
        let hypothesis_id = canonical_sha256(&HypothesisIdentity {
            schema: HYPOTHESIS_SCHEMA_V1,
            statement: &statement,
            falsification_rule: &falsification_rule,
            strategy,
            strategy_version: &strategy_version,
        })?;
        Ok(Self {
            schema: HYPOTHESIS_SCHEMA_V1.to_owned(),
            hypothesis_id,
            statement,
            falsification_rule,
            strategy,
            strategy_version,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayExecutionModel {
    pub version: String,
    pub mechanics: PositionScenarioMechanics,
    pub mechanics_provenance: ReplayMechanicsProvenance,
    pub fee_provenance: String,
    pub funding_provenance: String,
    pub contracts: String,
    pub leverage: String,
    pub candidate_worst_case_loss_usd: String,
    pub entry_liquidity_role: LiquidityRole,
    pub exit_liquidity_role: LiquidityRole,
    pub funding_notional_basis: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentSpec {
    pub schema: String,
    pub experiment_spec_id: String,
    pub hypothesis_id: String,
    pub dataset_id: String,
    pub instrument_id: String,
    pub bar: String,
    pub strategy: BaselineStrategyKind,
    pub strategy_version: String,
    pub deterministic_seed: u64,
    pub execution: ReplayExecutionModel,
    pub mandate: TradingMandate,
    pub policy: HardRiskPolicy,
    pub initial_risk_context: CandidateRiskContext,
}

#[derive(Serialize)]
struct ExperimentSpecIdentity<'a> {
    schema: &'static str,
    hypothesis_id: &'a str,
    dataset_id: &'a str,
    instrument_id: &'a str,
    bar: &'a str,
    strategy: BaselineStrategyKind,
    strategy_version: &'a str,
    deterministic_seed: u64,
    execution: &'a ReplayExecutionModel,
    mandate: &'a TradingMandate,
    policy: &'a HardRiskPolicy,
    initial_risk_context: &'a CandidateRiskContext,
}

impl ExperimentSpec {
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        hypothesis: &Hypothesis,
        dataset_id: impl Into<String>,
        instrument_id: impl Into<String>,
        bar: impl Into<String>,
        deterministic_seed: u64,
        execution: ReplayExecutionModel,
        mandate: TradingMandate,
        policy: HardRiskPolicy,
        initial_risk_context: CandidateRiskContext,
    ) -> Result<Self, ResearchError> {
        let dataset_id = dataset_id.into();
        let instrument_id = instrument_id.into();
        let bar = bar.into();
        required("experiment.dataset_id", &dataset_id)?;
        required("experiment.instrument_id", &instrument_id)?;
        required("experiment.bar", &bar)?;
        validate_execution_model(&execution)?;
        let experiment_spec_id = canonical_sha256(&ExperimentSpecIdentity {
            schema: EXPERIMENT_SPEC_SCHEMA_V1,
            hypothesis_id: &hypothesis.hypothesis_id,
            dataset_id: &dataset_id,
            instrument_id: &instrument_id,
            bar: &bar,
            strategy: hypothesis.strategy,
            strategy_version: &hypothesis.strategy_version,
            deterministic_seed,
            execution: &execution,
            mandate: &mandate,
            policy: &policy,
            initial_risk_context: &initial_risk_context,
        })?;
        Ok(Self {
            schema: EXPERIMENT_SPEC_SCHEMA_V1.to_owned(),
            experiment_spec_id,
            hypothesis_id: hypothesis.hypothesis_id.clone(),
            dataset_id,
            instrument_id,
            bar,
            strategy: hypothesis.strategy,
            strategy_version: hypothesis.strategy_version.clone(),
            deterministic_seed,
            execution,
            mandate,
            policy,
            initial_risk_context,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReplayDecisionTrace {
    pub signal_open_time_ms: String,
    pub signal_available_time_ms: String,
    pub earliest_execution_time_ms: String,
    pub previous_close: String,
    pub signal_close: String,
    pub decision: StrategyDecision,
    pub risk_decision: Option<RiskPolicyDecision>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReplayTrade {
    pub direction: PositionDirection,
    pub signal_available_time_ms: String,
    pub entry_time_ms: String,
    pub exit_time_ms: String,
    pub entry_price_role: &'static str,
    pub exit_price_role: &'static str,
    pub entry_price: String,
    pub exit_price: String,
    pub contracts: String,
    pub leverage: String,
    pub entry_notional_quote: String,
    pub gross_pnl_quote: String,
    pub trading_cost_quote: String,
    pub funding_cost_quote: String,
    pub net_pnl_quote: String,
    pub funding_event_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExperimentResult {
    pub schema: &'static str,
    pub experiment_id: String,
    pub experiment_spec_id: String,
    pub hypothesis_id: String,
    pub dataset_id: String,
    pub source_tree: String,
    pub status: ReplayStatus,
    pub evidence_class: ReplayEvidenceClass,
    pub blocker: Option<&'static str>,
    pub signal_price_role: &'static str,
    pub execution_price_role: &'static str,
    pub candles_processed: usize,
    pub decision_count: usize,
    pub trade_count: usize,
    pub rejected_candidate_count: usize,
    pub gross_pnl_quote: String,
    pub trading_cost_quote: String,
    pub funding_cost_quote: String,
    pub net_pnl_quote: String,
    pub decisions: Vec<ReplayDecisionTrace>,
    pub trades: Vec<ReplayTrade>,
}

#[derive(Serialize)]
struct ExperimentResultIdentity<'a> {
    schema: &'static str,
    experiment_spec_id: &'a str,
    hypothesis_id: &'a str,
    dataset_id: &'a str,
    source_tree: &'a str,
    status: ReplayStatus,
    evidence_class: ReplayEvidenceClass,
    blocker: &'a Option<&'static str>,
    signal_price_role: &'static str,
    execution_price_role: &'static str,
    candles_processed: usize,
    decision_count: usize,
    trade_count: usize,
    rejected_candidate_count: usize,
    gross_pnl_quote: &'a str,
    trading_cost_quote: &'a str,
    funding_cost_quote: &'a str,
    net_pnl_quote: &'a str,
    decisions: &'a [ReplayDecisionTrace],
    trades: &'a [ReplayTrade],
}

struct TerminalResultParts {
    status: ReplayStatus,
    evidence_class: ReplayEvidenceClass,
    blocker: Option<&'static str>,
    candles_processed: usize,
    decisions: Vec<ReplayDecisionTrace>,
    trades: Vec<ReplayTrade>,
    rejected_candidate_count: usize,
    gross_pnl: Decimal,
    trading_cost: Decimal,
    funding_cost: Decimal,
}

pub fn build_baseline_experiment(
    dataset: &ReplayDatasetArtifact,
    strategy: BaselineStrategyKind,
    mechanics_provenance: ReplayMechanicsProvenance,
) -> Result<(Hypothesis, ExperimentSpec), ResearchError> {
    let reference = dataset
        .reference
        .as_ref()
        .ok_or(ResearchError::ReplayMissingField("replay_dataset.reference"))?;
    let contract_value = reference
        .contract_value
        .as_deref()
        .ok_or(ResearchError::ReplayMissingField(
            "replay_dataset.reference.contract_value",
        ))?;
    let contract_value_currency = reference
        .contract_value_currency
        .as_deref()
        .ok_or(ResearchError::ReplayMissingField(
            "replay_dataset.reference.contract_value_currency",
        ))?;
    let settle_currency = reference
        .settle_currency
        .as_deref()
        .ok_or(ResearchError::ReplayMissingField(
            "replay_dataset.reference.settle_currency",
        ))?;

    let hypothesis = Hypothesis::build(
        match strategy {
            BaselineStrategyKind::NoTrade => "NO_TRADE null baseline",
            BaselineStrategyKind::CloseMomentum => {
                "1H completed-bar close momentum predicts the next one-hour price move"
            }
        },
        match strategy {
            BaselineStrategyKind::NoTrade => {
                "any trade, fee, funding cashflow, or non-zero PnL falsifies the null accounting baseline"
            }
            BaselineStrategyKind::CloseMomentum => {
                "replay records deterministic next-open decisions and net PnL without same-close execution"
            }
        },
        strategy,
        BASELINE_STRATEGY_VERSION_V1,
    )?;

    let instrument = dataset.manifest.instrument_id.clone();
    let mandate = TradingMandate {
        schema: TRADING_MANDATE_SCHEMA_V1.to_owned(),
        version: "okx.research.baseline-mandate/2026-10-04.1".to_owned(),
        capital_base_usd: "100".to_owned(),
        decision_horizon_hours: 1,
        benchmark: Some("NO_EXTERNAL_BENCHMARK".to_owned()),
        allowed_instruments: vec![instrument.clone()],
        max_drawdown_ratio: "0.5".to_owned(),
        leverage_ceiling: "5".to_owned(),
        minimum_liquidity_notional_usd: "0".to_owned(),
        max_turnover_ratio: "100".to_owned(),
    };
    let policy = HardRiskPolicy {
        schema: HARD_RISK_POLICY_SCHEMA_V1.to_owned(),
        version: "okx.research.baseline-policy/2026-10-04.1".to_owned(),
        max_account_gross_notional_usd: "1000".to_owned(),
        max_instrument_gross_notional_usd: "1000".to_owned(),
        max_margin_utilization_ratio: "0.9".to_owned(),
        max_loss_per_trade_usd: "10".to_owned(),
        max_daily_realized_loss_usd: "50".to_owned(),
        max_drawdown_ratio: "0.5".to_owned(),
        max_leverage: "5".to_owned(),
        allowed_instruments: vec![instrument.clone()],
        minimum_quality: RiskMinimumQuality::Fresh,
        degraded_mode: RiskDegradedMode::Reject,
        correlated_clusters: Vec::new(),
    };
    let initial_risk_context = CandidateRiskContext {
        total_equity_usd: "100".to_owned(),
        account_gross_notional_usd: "0".to_owned(),
        instrument_gross_notional_usd: "0".to_owned(),
        account_initial_margin_usd: "0".to_owned(),
        capital_base_drawdown_ratio: "0".to_owned(),
        daily_realized_loss_usd: "0".to_owned(),
        account_is_fresh: true,
    };
    let execution = ReplayExecutionModel {
        version: REPLAY_EXECUTION_MODEL_VERSION_V1.to_owned(),
        mechanics: PositionScenarioMechanics {
            settle_currency: settle_currency.to_owned(),
            contract_value_currency: contract_value_currency.to_owned(),
            contract_value: contract_value.to_owned(),
            tick_size: reference.tick_size.clone(),
            entry_fee_rate: "-0.0005".to_owned(),
            exit_fee_rate: "-0.0005".to_owned(),
        },
        mechanics_provenance,
        fee_provenance: "DECLARED_COUNTERFACTUAL_TAKER_5_BPS_EACH_SIDE".to_owned(),
        funding_provenance: "HISTORICAL_OBSERVED".to_owned(),
        contracts: reference.min_size.clone(),
        leverage: "2".to_owned(),
        candidate_worst_case_loss_usd: "2".to_owned(),
        entry_liquidity_role: LiquidityRole::Taker,
        exit_liquidity_role: LiquidityRole::Taker,
        funding_notional_basis: "ENTRY_NOTIONAL_COUNTERFACTUAL".to_owned(),
    };
    let spec = ExperimentSpec::build(
        &hypothesis,
        dataset.manifest.dataset_id.clone(),
        instrument,
        "1H",
        0,
        execution,
        mandate,
        policy,
        initial_risk_context,
    )?;
    Ok((hypothesis, spec))
}

pub fn replay_experiment(
    dataset: &DatasetManifest,
    candles: &[ResearchCandle],
    funding: &[ResearchFundingEvent],
    spec: &ExperimentSpec,
) -> Result<ExperimentResult, ResearchError> {
    validate_replay_inputs(dataset, candles, spec)?;

    if !dataset.gaps.is_empty() {
        return terminal_result(
            dataset,
            spec,
            TerminalResultParts {
                status: ReplayStatus::InsufficientData,
                evidence_class: evidence_class(dataset, spec),
                blocker: Some("INSUFFICIENT_DATA"),
                candles_processed: candles.len(),
                decisions: Vec::new(),
                trades: Vec::new(),
                rejected_candidate_count: 0,
                gross_pnl: Decimal::ZERO,
                trading_cost: Decimal::ZERO,
                funding_cost: Decimal::ZERO,
            },
        );
    }

    if spec.strategy != BaselineStrategyKind::NoTrade
        && spec.execution.mechanics_provenance == ReplayMechanicsProvenance::HistoricalObserved
        && dataset.reference_coverage != ReferenceCoverageStatus::Complete
    {
        return terminal_result(
            dataset,
            spec,
            TerminalResultParts {
                status: ReplayStatus::InsufficientReferenceHistory,
                evidence_class: ReplayEvidenceClass::HistoricalObserved,
                blocker: Some("INSUFFICIENT_REFERENCE_HISTORY"),
                candles_processed: candles.len(),
                decisions: Vec::new(),
                trades: Vec::new(),
                rejected_candidate_count: 0,
                gross_pnl: Decimal::ZERO,
                trading_cost: Decimal::ZERO,
                funding_cost: Decimal::ZERO,
            },
        );
    }

    if spec.strategy == BaselineStrategyKind::NoTrade {
        let mut decisions = Vec::new();
        for pair in candles.windows(2) {
            let signal = &pair[1];
            decisions.push(ReplayDecisionTrace {
                signal_open_time_ms: signal.open_time_ms.clone(),
                signal_available_time_ms: signal.available_time_ms.clone(),
                earliest_execution_time_ms: signal.available_time_ms.clone(),
                previous_close: pair[0].close.clone(),
                signal_close: signal.close.clone(),
                decision: StrategyDecision::Hold,
                risk_decision: None,
            });
        }
        return terminal_result(
            dataset,
            spec,
            TerminalResultParts {
                status: ReplayStatus::Completed,
                evidence_class: ReplayEvidenceClass::DataOnly,
                blocker: None,
                candles_processed: candles.len(),
                decisions,
                trades: Vec::new(),
                rejected_candidate_count: 0,
                gross_pnl: Decimal::ZERO,
                trading_cost: Decimal::ZERO,
                funding_cost: Decimal::ZERO,
            },
        );
    }

    if candles.len() < 4 {
        return terminal_result(
            dataset,
            spec,
            TerminalResultParts {
                status: ReplayStatus::InsufficientData,
                evidence_class: evidence_class(dataset, spec),
                blocker: Some("INSUFFICIENT_DATA"),
                candles_processed: candles.len(),
                decisions: Vec::new(),
                trades: Vec::new(),
                rejected_candidate_count: 0,
                gross_pnl: Decimal::ZERO,
                trading_cost: Decimal::ZERO,
                funding_cost: Decimal::ZERO,
            },
        );
    }

    let initial_equity = decimal(
        "initial_total_equity_usd",
        &spec.initial_risk_context.total_equity_usd,
    )?;
    let capital_base = decimal("mandate_capital_base_usd", &spec.mandate.capital_base_usd)?;
    let mut equity = initial_equity;
    let mut daily_loss = decimal(
        "initial_daily_realized_loss_usd",
        &spec.initial_risk_context.daily_realized_loss_usd,
    )?;
    let mut active_day = None::<u64>;

    let mut decisions = Vec::new();
    let mut trades = Vec::new();
    let mut rejected = 0usize;
    let mut gross_total = Decimal::ZERO;
    let mut trading_cost_total = Decimal::ZERO;
    let mut funding_cost_total = Decimal::ZERO;

    for signal_index in 1..(candles.len() - 2) {
        let previous = &candles[signal_index - 1];
        let signal = &candles[signal_index];
        let entry = &candles[signal_index + 1];
        let exit = &candles[signal_index + 2];

        let signal_available = timestamp("signal_available_time_ms", &signal.available_time_ms)?;
        let entry_time = timestamp("entry_open_time_ms", &entry.open_time_ms)?;
        let exit_time = timestamp("exit_open_time_ms", &exit.open_time_ms)?;
        if entry_time < signal_available || exit_time <= entry_time {
            return Err(ResearchError::ReplayCausalityViolation);
        }

        let decision = evaluate_baseline_strategy(
            spec.strategy,
            &BarDecisionInput {
                previous_close: previous.close.clone(),
                signal_close: signal.close.clone(),
            },
        )?;
        if decision == StrategyDecision::Hold {
            decisions.push(ReplayDecisionTrace {
                signal_open_time_ms: signal.open_time_ms.clone(),
                signal_available_time_ms: signal.available_time_ms.clone(),
                earliest_execution_time_ms: entry.open_time_ms.clone(),
                previous_close: previous.close.clone(),
                signal_close: signal.close.clone(),
                decision,
                risk_decision: None,
            });
            continue;
        }

        let direction = match decision {
            StrategyDecision::EnterLong => PositionDirection::Long,
            StrategyDecision::EnterShort => PositionDirection::Short,
            StrategyDecision::Hold => unreachable!("hold handled above"),
        };

        let scenario = analyze_position_scenario_values(
            &spec.instrument_id,
            &dataset.dataset_id,
            &spec.execution.fee_provenance,
            &spec.execution.mechanics,
            &PositionScenarioAssumptions {
                direction,
                contracts: spec.execution.contracts.clone(),
                entry_price: entry.open.clone(),
                exit: ScenarioExitAssumption::Price {
                    price: exit.open.clone(),
                },
                entry_liquidity_role: spec.execution.entry_liquidity_role,
                exit_liquidity_role: spec.execution.exit_liquidity_role,
            },
        )?;

        let day = entry_time / ONE_DAY_MS;
        if active_day != Some(day) {
            active_day = Some(day);
            daily_loss = Decimal::ZERO;
        }
        let drawdown = if capital_base > Decimal::ZERO && equity < capital_base {
            (capital_base - equity) / capital_base
        } else {
            Decimal::ZERO
        };
        let mut risk_context = spec.initial_risk_context.clone();
        risk_context.total_equity_usd = equity.max(Decimal::ZERO).normalize().to_string();
        risk_context.capital_base_drawdown_ratio = drawdown.normalize().to_string();
        risk_context.daily_realized_loss_usd = daily_loss.normalize().to_string();

        let risk_gate = evaluate_candidate_risk(
            &spec.mandate,
            &spec.policy,
            &PortfolioCandidate {
                instrument: spec.instrument_id.clone(),
                direction,
                notional_usd: scenario.entry_settle_notional.clone(),
                worst_case_loss_usd: spec.execution.candidate_worst_case_loss_usd.clone(),
                leverage: spec.execution.leverage.clone(),
            },
            &risk_context,
        )?;

        decisions.push(ReplayDecisionTrace {
            signal_open_time_ms: signal.open_time_ms.clone(),
            signal_available_time_ms: signal.available_time_ms.clone(),
            earliest_execution_time_ms: entry.open_time_ms.clone(),
            previous_close: previous.close.clone(),
            signal_close: signal.close.clone(),
            decision,
            risk_decision: Some(risk_gate.decision),
        });
        if risk_gate.decision == RiskPolicyDecision::Rejected {
            rejected += 1;
            continue;
        }

        let entry_notional = decimal("entry_settle_notional", &scenario.entry_settle_notional)?;
        let gross = decimal("gross_pnl_settle", &scenario.gross_pnl_settle)?;
        let entry_cost = decimal(
            "entry_trading_cost_settle",
            &scenario.entry_trading_cost_settle,
        )?;
        let exit_cost = decimal(
            "exit_trading_cost_settle",
            &scenario.exit_trading_cost_settle,
        )?;
        let trading_cost = entry_cost + exit_cost;

        let mut funding_cost = Decimal::ZERO;
        let mut funding_event_count = 0usize;
        for event in funding {
            let event_time = timestamp("funding_time_ms", &event.funding_time_ms)?;
            if event_time < entry_time || event_time >= exit_time {
                continue;
            }
            let rate = event
                .realized_rate
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(&event.funding_rate);
            funding_cost += decimal(
                "funding_user_cost_quote",
                &funding_user_cost_quote(&entry_notional.normalize().to_string(), rate, direction)?,
            )?;
            funding_event_count += 1;
        }

        let net = gross - trading_cost - funding_cost;
        gross_total += gross;
        trading_cost_total += trading_cost;
        funding_cost_total += funding_cost;
        equity += net;
        if net < Decimal::ZERO {
            daily_loss += -net;
        }

        trades.push(ReplayTrade {
            direction,
            signal_available_time_ms: signal.available_time_ms.clone(),
            entry_time_ms: entry.open_time_ms.clone(),
            exit_time_ms: exit.open_time_ms.clone(),
            entry_price_role: "NEXT_BAR_OPEN",
            exit_price_role: "FOLLOWING_BAR_OPEN",
            entry_price: entry.open.clone(),
            exit_price: exit.open.clone(),
            contracts: spec.execution.contracts.clone(),
            leverage: spec.execution.leverage.clone(),
            entry_notional_quote: scenario.entry_settle_notional,
            gross_pnl_quote: gross.normalize().to_string(),
            trading_cost_quote: trading_cost.normalize().to_string(),
            funding_cost_quote: funding_cost.normalize().to_string(),
            net_pnl_quote: net.normalize().to_string(),
            funding_event_count,
        });
    }

    terminal_result(
        dataset,
        spec,
        TerminalResultParts {
            status: ReplayStatus::Completed,
            evidence_class: evidence_class(dataset, spec),
            blocker: None,
            candles_processed: candles.len(),
            decisions,
            trades,
            rejected_candidate_count: rejected,
            gross_pnl: gross_total,
            trading_cost: trading_cost_total,
            funding_cost: funding_cost_total,
        },
    )
}

fn terminal_result(
    dataset: &DatasetManifest,
    spec: &ExperimentSpec,
    parts: TerminalResultParts,
) -> Result<ExperimentResult, ResearchError> {
    let TerminalResultParts {
        status,
        evidence_class,
        blocker,
        candles_processed,
        decisions,
        trades,
        rejected_candidate_count,
        gross_pnl,
        trading_cost,
        funding_cost,
    } = parts;
    let net_pnl = gross_pnl - trading_cost - funding_cost;
    let gross_pnl_quote = gross_pnl.normalize().to_string();
    let trading_cost_quote = trading_cost.normalize().to_string();
    let funding_cost_quote = funding_cost.normalize().to_string();
    let net_pnl_quote = net_pnl.normalize().to_string();
    let identity = ExperimentResultIdentity {
        schema: EXPERIMENT_RESULT_SCHEMA_V1,
        experiment_spec_id: &spec.experiment_spec_id,
        hypothesis_id: &spec.hypothesis_id,
        dataset_id: &dataset.dataset_id,
        source_tree: &dataset.source_tree,
        status,
        evidence_class,
        blocker: &blocker,
        signal_price_role: "COMPLETED_BAR_CLOSE",
        execution_price_role: "NEXT_BAR_OPEN_THEN_FOLLOWING_BAR_OPEN",
        candles_processed,
        decision_count: decisions.len(),
        trade_count: trades.len(),
        rejected_candidate_count,
        gross_pnl_quote: &gross_pnl_quote,
        trading_cost_quote: &trading_cost_quote,
        funding_cost_quote: &funding_cost_quote,
        net_pnl_quote: &net_pnl_quote,
        decisions: &decisions,
        trades: &trades,
    };
    let experiment_id = canonical_sha256(&identity)?;
    Ok(ExperimentResult {
        schema: EXPERIMENT_RESULT_SCHEMA_V1,
        experiment_id,
        experiment_spec_id: spec.experiment_spec_id.clone(),
        hypothesis_id: spec.hypothesis_id.clone(),
        dataset_id: dataset.dataset_id.clone(),
        source_tree: dataset.source_tree.clone(),
        status,
        evidence_class,
        blocker,
        signal_price_role: "COMPLETED_BAR_CLOSE",
        execution_price_role: "NEXT_BAR_OPEN_THEN_FOLLOWING_BAR_OPEN",
        candles_processed,
        decision_count: decisions.len(),
        trade_count: trades.len(),
        rejected_candidate_count,
        gross_pnl_quote,
        trading_cost_quote,
        funding_cost_quote,
        net_pnl_quote,
        decisions,
        trades,
    })
}

fn validate_replay_dataset_artifact(artifact: &ReplayDatasetArtifact) -> Result<(), ResearchError> {
    let begin = artifact.manifest.range.begin()?;
    let end = artifact.manifest.range.end()?;
    let mut previous = None::<u64>;
    for candle in &artifact.candles {
        let open = timestamp("replay_dataset.candle.open_time_ms", &candle.open_time_ms)?;
        let available = timestamp(
            "replay_dataset.candle.available_time_ms",
            &candle.available_time_ms,
        )?;
        if open < begin || open >= end || available != open.saturating_add(ONE_HOUR_MS) {
            return Err(ResearchError::ReplayCausalityViolation);
        }
        if previous.is_some_and(|value| open <= value) {
            return Err(ResearchError::ReplayEventOrdering);
        }
        previous = Some(open);
    }
    for event in &artifact.funding {
        let event_time = timestamp("replay_dataset.funding_time_ms", &event.funding_time_ms)?;
        if event_time < begin || event_time >= end {
            return Err(ResearchError::ReplayDatasetMismatch);
        }
    }
    Ok(())
}

fn validate_replay_inputs(
    dataset: &DatasetManifest,
    candles: &[ResearchCandle],
    spec: &ExperimentSpec,
) -> Result<(), ResearchError> {
    if spec.dataset_id != dataset.dataset_id
        || spec.instrument_id != dataset.instrument_id
        || dataset.bar.as_deref() != Some(spec.bar.as_str())
    {
        return Err(ResearchError::ReplayDatasetMismatch);
    }
    if spec.bar != "1H" {
        return Err(ResearchError::ReplayUnsupportedBar(spec.bar.clone()));
    }
    if candles.len() < 2 {
        return Ok(());
    }
    let begin = dataset.range.begin()?;
    let end = dataset.range.end()?;
    let mut previous_open = None::<u64>;
    for candle in candles {
        let open = timestamp("candle.open_time_ms", &candle.open_time_ms)?;
        let available = timestamp("candle.available_time_ms", &candle.available_time_ms)?;
        if open < begin || open >= end || available != open.saturating_add(ONE_HOUR_MS) {
            return Err(ResearchError::ReplayCausalityViolation);
        }
        if previous_open.is_some_and(|previous| open <= previous) {
            return Err(ResearchError::ReplayEventOrdering);
        }
        previous_open = Some(open);
    }
    Ok(())
}

fn validate_execution_model(model: &ReplayExecutionModel) -> Result<(), ResearchError> {
    required("execution.version", &model.version)?;
    required("execution.fee_provenance", &model.fee_provenance)?;
    required("execution.funding_provenance", &model.funding_provenance)?;
    required("execution.contracts", &model.contracts)?;
    required("execution.leverage", &model.leverage)?;
    required(
        "execution.candidate_worst_case_loss_usd",
        &model.candidate_worst_case_loss_usd,
    )?;
    if model.funding_notional_basis != "ENTRY_NOTIONAL_COUNTERFACTUAL" {
        return Err(ResearchError::ReplayInvalidExecutionModel(
            "funding_notional_basis".to_owned(),
        ));
    }
    Ok(())
}

fn evidence_class(dataset: &DatasetManifest, spec: &ExperimentSpec) -> ReplayEvidenceClass {
    if spec.strategy == BaselineStrategyKind::NoTrade {
        ReplayEvidenceClass::DataOnly
    } else if spec.execution.mechanics_provenance == ReplayMechanicsProvenance::HistoricalObserved
        && dataset.reference_coverage == ReferenceCoverageStatus::Complete
        && spec.execution.fee_provenance == "HISTORICAL_OBSERVED"
        && spec.execution.funding_provenance == "HISTORICAL_OBSERVED"
    {
        ReplayEvidenceClass::HistoricalObserved
    } else {
        ReplayEvidenceClass::CounterfactualMechanics
    }
}

fn decimal(field: &'static str, value: &str) -> Result<Decimal, ResearchError> {
    Decimal::from_str(value).map_err(|_| ResearchError::ReplayInvalidDecimal {
        field,
        value: value.to_owned(),
    })
}

fn timestamp(field: &'static str, value: &str) -> Result<u64, ResearchError> {
    value
        .parse::<u64>()
        .map_err(|_| ResearchError::InvalidTimestamp {
            field,
            value: value.to_owned(),
        })
}

fn required(field: &'static str, value: &str) -> Result<(), ResearchError> {
    if value.trim().is_empty() {
        Err(ResearchError::ReplayMissingField(field))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use okx_analysis::{
        BASELINE_STRATEGY_VERSION_V1, HARD_RISK_POLICY_SCHEMA_V1, RiskDegradedMode,
        RiskMinimumQuality, TRADING_MANDATE_SCHEMA_V1,
    };

    use super::*;
    use crate::{DataGap, ReferenceCoverageStatus, ResearchRange, ResearchTier};

    fn candle(open_ms: u64, open: &str, close: &str) -> ResearchCandle {
        ResearchCandle {
            schema: crate::RESEARCH_CANDLE_SCHEMA_V1.to_owned(),
            open_time_ms: open_ms.to_string(),
            available_time_ms: (open_ms + ONE_HOUR_MS).to_string(),
            open: open.to_owned(),
            high: "999999".to_owned(),
            low: "0.0001".to_owned(),
            close: close.to_owned(),
            volume: "1".to_owned(),
            volume_currency: "1".to_owned(),
            volume_quote: Some("1".to_owned()),
        }
    }

    fn candles() -> Vec<ResearchCandle> {
        vec![
            candle(0, "100", "100"),
            candle(ONE_HOUR_MS, "100", "110"),
            candle(2 * ONE_HOUR_MS, "111", "112"),
            candle(3 * ONE_HOUR_MS, "113", "114"),
            candle(4 * ONE_HOUR_MS, "115", "116"),
        ]
    }

    fn dataset(reference_coverage: ReferenceCoverageStatus) -> DatasetManifest {
        DatasetManifest {
            schema: crate::DATASET_MANIFEST_SCHEMA_V1.to_owned(),
            dataset_id: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            tier: ResearchTier::TierA,
            instrument_id: "BTC-USDT-SWAP".to_owned(),
            bar: Some("1H".to_owned()),
            range: ResearchRange::new("0", (5 * ONE_HOUR_MS).to_string()).expect("range"),
            reference_coverage,
            reference_window: None,
            chunk_ids: vec![
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_owned(),
            ],
            gaps: Vec::new(),
            parser_version: "parser/v1".to_owned(),
            normalization_version: "normalizer/v1".to_owned(),
            source_tree: "tree-v1".to_owned(),
            created_at_ms: "1".to_owned(),
        }
    }

    fn mandate() -> TradingMandate {
        TradingMandate {
            schema: TRADING_MANDATE_SCHEMA_V1.to_owned(),
            version: "mandate/v1".to_owned(),
            capital_base_usd: "100".to_owned(),
            decision_horizon_hours: 1,
            benchmark: None,
            allowed_instruments: vec!["BTC-USDT-SWAP".to_owned()],
            max_drawdown_ratio: "0.5".to_owned(),
            leverage_ceiling: "5".to_owned(),
            minimum_liquidity_notional_usd: "0".to_owned(),
            max_turnover_ratio: "100".to_owned(),
        }
    }

    fn policy() -> HardRiskPolicy {
        HardRiskPolicy {
            schema: HARD_RISK_POLICY_SCHEMA_V1.to_owned(),
            version: "policy/v1".to_owned(),
            max_account_gross_notional_usd: "1000".to_owned(),
            max_instrument_gross_notional_usd: "1000".to_owned(),
            max_margin_utilization_ratio: "0.9".to_owned(),
            max_loss_per_trade_usd: "10".to_owned(),
            max_daily_realized_loss_usd: "50".to_owned(),
            max_drawdown_ratio: "0.5".to_owned(),
            max_leverage: "5".to_owned(),
            allowed_instruments: vec!["BTC-USDT-SWAP".to_owned()],
            minimum_quality: RiskMinimumQuality::Fresh,
            degraded_mode: RiskDegradedMode::Reject,
            correlated_clusters: Vec::new(),
        }
    }

    fn risk_context() -> CandidateRiskContext {
        CandidateRiskContext {
            total_equity_usd: "100".to_owned(),
            account_gross_notional_usd: "0".to_owned(),
            instrument_gross_notional_usd: "0".to_owned(),
            account_initial_margin_usd: "0".to_owned(),
            capital_base_drawdown_ratio: "0".to_owned(),
            daily_realized_loss_usd: "0".to_owned(),
            account_is_fresh: true,
        }
    }

    fn execution(fee_rate: &str) -> ReplayExecutionModel {
        ReplayExecutionModel {
            version: REPLAY_EXECUTION_MODEL_VERSION_V1.to_owned(),
            mechanics: PositionScenarioMechanics {
                settle_currency: "USDT".to_owned(),
                contract_value_currency: "BTC".to_owned(),
                contract_value: "1".to_owned(),
                tick_size: "1".to_owned(),
                entry_fee_rate: fee_rate.to_owned(),
                exit_fee_rate: fee_rate.to_owned(),
            },
            mechanics_provenance: ReplayMechanicsProvenance::DeclaredCounterfactual,
            fee_provenance: "DECLARED_COUNTERFACTUAL".to_owned(),
            funding_provenance: "OBSERVED_EVENT_RATE".to_owned(),
            contracts: "1".to_owned(),
            leverage: "2".to_owned(),
            candidate_worst_case_loss_usd: "2".to_owned(),
            entry_liquidity_role: LiquidityRole::Taker,
            exit_liquidity_role: LiquidityRole::Taker,
            funding_notional_basis: "ENTRY_NOTIONAL_COUNTERFACTUAL".to_owned(),
        }
    }

    fn spec(strategy: BaselineStrategyKind, fee_rate: &str) -> ExperimentSpec {
        let hypothesis = Hypothesis::build(
            "baseline hypothesis",
            "baseline falsification",
            strategy,
            BASELINE_STRATEGY_VERSION_V1,
        )
        .expect("hypothesis");
        ExperimentSpec::build(
            &hypothesis,
            dataset(ReferenceCoverageStatus::InsufficientReferenceHistory).dataset_id,
            "BTC-USDT-SWAP",
            "1H",
            7,
            execution(fee_rate),
            mandate(),
            policy(),
            risk_context(),
        )
        .expect("spec")
    }

    #[test]
    fn no_trade_baseline_has_exact_zero_accounting() {
        let data = dataset(ReferenceCoverageStatus::InsufficientReferenceHistory);
        let result = replay_experiment(
            &data,
            &candles(),
            &[],
            &spec(BaselineStrategyKind::NoTrade, "-0.001"),
        )
        .expect("replay");

        assert_eq!(result.status, ReplayStatus::Completed);
        assert_eq!(result.evidence_class, ReplayEvidenceClass::DataOnly);
        assert_eq!(result.trade_count, 0);
        assert_eq!(result.gross_pnl_quote, "0");
        assert_eq!(result.trading_cost_quote, "0");
        assert_eq!(result.funding_cost_quote, "0");
        assert_eq!(result.net_pnl_quote, "0");
    }

    #[test]
    fn completed_bar_signal_executes_at_next_open_not_known_close() {
        let data = dataset(ReferenceCoverageStatus::InsufficientReferenceHistory);
        let result = replay_experiment(
            &data,
            &candles(),
            &[],
            &spec(BaselineStrategyKind::CloseMomentum, "-0.001"),
        )
        .expect("replay");

        assert_eq!(result.status, ReplayStatus::Completed);
        assert!(!result.trades.is_empty());
        let first = &result.trades[0];
        assert_eq!(
            first.signal_available_time_ms,
            (2 * ONE_HOUR_MS).to_string()
        );
        assert_eq!(first.entry_time_ms, (2 * ONE_HOUR_MS).to_string());
        assert_eq!(first.entry_price, "111");
        assert_ne!(first.entry_price, "110");
    }

    #[test]
    fn intrabar_high_low_poison_does_not_change_next_open_replay() {
        let data = dataset(ReferenceCoverageStatus::InsufficientReferenceHistory);
        let base = candles();
        let mut poisoned = base.clone();
        poisoned[1].high = "999999999999".to_owned();
        poisoned[1].low = "0.00000001".to_owned();

        let a = replay_experiment(
            &data,
            &base,
            &[],
            &spec(BaselineStrategyKind::CloseMomentum, "-0.001"),
        )
        .expect("base");
        let b = replay_experiment(
            &data,
            &poisoned,
            &[],
            &spec(BaselineStrategyKind::CloseMomentum, "-0.001"),
        )
        .expect("poisoned");
        assert_eq!(a.experiment_id, b.experiment_id);
    }

    #[test]
    fn future_value_poison_cannot_change_earlier_trade() {
        let data = dataset(ReferenceCoverageStatus::InsufficientReferenceHistory);
        let base = candles();
        let mut poisoned = base.clone();
        poisoned[4].close = "999999".to_owned();

        let a = replay_experiment(
            &data,
            &base,
            &[],
            &spec(BaselineStrategyKind::CloseMomentum, "-0.001"),
        )
        .expect("base");
        let b = replay_experiment(
            &data,
            &poisoned,
            &[],
            &spec(BaselineStrategyKind::CloseMomentum, "-0.001"),
        )
        .expect("poisoned");

        assert_eq!(a.trades.first(), b.trades.first());
        assert_eq!(a.decisions.first(), b.decisions.first());
    }

    #[test]
    fn observed_funding_events_are_applied_only_inside_holding_interval() {
        let data = dataset(ReferenceCoverageStatus::InsufficientReferenceHistory);
        let funding = vec![
            ResearchFundingEvent {
                schema: crate::RESEARCH_FUNDING_SCHEMA_V1.to_owned(),
                funding_time_ms: (2 * ONE_HOUR_MS + 10).to_string(),
                available_time_ms: (2 * ONE_HOUR_MS + 10).to_string(),
                funding_rate: "0.001".to_owned(),
                realized_rate: Some("0.001".to_owned()),
                formula_type: None,
                method: None,
            },
            ResearchFundingEvent {
                schema: crate::RESEARCH_FUNDING_SCHEMA_V1.to_owned(),
                funding_time_ms: (3 * ONE_HOUR_MS + 10).to_string(),
                available_time_ms: (3 * ONE_HOUR_MS + 10).to_string(),
                funding_rate: "0.5".to_owned(),
                realized_rate: Some("0.5".to_owned()),
                formula_type: None,
                method: None,
            },
        ];
        let result = replay_experiment(
            &data,
            &candles(),
            &funding,
            &spec(BaselineStrategyKind::CloseMomentum, "-0.001"),
        )
        .expect("replay");
        let first = result.trades.first().expect("first trade");
        assert_eq!(first.funding_event_count, 1);
        assert_eq!(first.funding_cost_quote, "0.111");
    }

    #[test]
    fn higher_declared_fees_cannot_improve_identical_gross_pnl() {
        let data = dataset(ReferenceCoverageStatus::InsufficientReferenceHistory);
        let low = replay_experiment(
            &data,
            &candles(),
            &[],
            &spec(BaselineStrategyKind::CloseMomentum, "-0.001"),
        )
        .expect("low fee");
        let high = replay_experiment(
            &data,
            &candles(),
            &[],
            &spec(BaselineStrategyKind::CloseMomentum, "-0.002"),
        )
        .expect("high fee");
        assert_eq!(low.gross_pnl_quote, high.gross_pnl_quote);
        assert!(
            decimal("low_net", &low.net_pnl_quote).expect("low")
                > decimal("high_net", &high.net_pnl_quote).expect("high")
        );
    }

    #[test]
    fn same_inputs_produce_same_experiment_identity() {
        let data = dataset(ReferenceCoverageStatus::InsufficientReferenceHistory);
        let experiment = spec(BaselineStrategyKind::CloseMomentum, "-0.001");
        let a = replay_experiment(&data, &candles(), &[], &experiment).expect("a");
        let b = replay_experiment(&data, &candles(), &[], &experiment).expect("b");
        assert_eq!(a, b);
        assert_eq!(a.experiment_id, b.experiment_id);
    }

    #[test]
    fn policy_rejection_blocks_trade_without_bypassing_risk_owner() {
        let data = dataset(ReferenceCoverageStatus::InsufficientReferenceHistory);
        let mut experiment = spec(BaselineStrategyKind::CloseMomentum, "-0.001");
        experiment.policy.allowed_instruments = vec!["ETH-USDT-SWAP".to_owned()];
        experiment.experiment_spec_id = canonical_sha256(&ExperimentSpecIdentity {
            schema: EXPERIMENT_SPEC_SCHEMA_V1,
            hypothesis_id: &experiment.hypothesis_id,
            dataset_id: &experiment.dataset_id,
            instrument_id: &experiment.instrument_id,
            bar: &experiment.bar,
            strategy: experiment.strategy,
            strategy_version: &experiment.strategy_version,
            deterministic_seed: experiment.deterministic_seed,
            execution: &experiment.execution,
            mandate: &experiment.mandate,
            policy: &experiment.policy,
            initial_risk_context: &experiment.initial_risk_context,
        })
        .expect("rehash");
        let result = replay_experiment(&data, &candles(), &[], &experiment).expect("replay");
        assert_eq!(result.trade_count, 0);
        assert!(result.rejected_candidate_count > 0);
        assert!(
            result
                .decisions
                .iter()
                .any(|row| row.risk_decision == Some(RiskPolicyDecision::Rejected))
        );
    }

    #[test]
    fn gaps_fail_closed_before_strategy_replay() {
        let mut data = dataset(ReferenceCoverageStatus::InsufficientReferenceHistory);
        data.gaps.push(DataGap {
            begin_ms: ONE_HOUR_MS.to_string(),
            end_ms: (2 * ONE_HOUR_MS).to_string(),
            reason: "missing_fixed_interval_events".to_owned(),
        });
        let result = replay_experiment(
            &data,
            &candles(),
            &[],
            &spec(BaselineStrategyKind::CloseMomentum, "-0.001"),
        )
        .expect("replay");
        assert_eq!(result.status, ReplayStatus::InsufficientData);
        assert_eq!(result.blocker, Some("INSUFFICIENT_DATA"));
        assert_eq!(result.trade_count, 0);
    }

    #[test]
    fn historical_mechanics_require_point_in_time_reference_coverage() {
        let data = dataset(ReferenceCoverageStatus::InsufficientReferenceHistory);
        let mut experiment = spec(BaselineStrategyKind::CloseMomentum, "-0.001");
        experiment.execution.mechanics_provenance = ReplayMechanicsProvenance::HistoricalObserved;
        experiment.experiment_spec_id = canonical_sha256(&ExperimentSpecIdentity {
            schema: EXPERIMENT_SPEC_SCHEMA_V1,
            hypothesis_id: &experiment.hypothesis_id,
            dataset_id: &experiment.dataset_id,
            instrument_id: &experiment.instrument_id,
            bar: &experiment.bar,
            strategy: experiment.strategy,
            strategy_version: &experiment.strategy_version,
            deterministic_seed: experiment.deterministic_seed,
            execution: &experiment.execution,
            mandate: &experiment.mandate,
            policy: &experiment.policy,
            initial_risk_context: &experiment.initial_risk_context,
        })
        .expect("rehash");
        let result = replay_experiment(&data, &candles(), &[], &experiment).expect("replay");
        assert_eq!(result.status, ReplayStatus::InsufficientReferenceHistory);
        assert_eq!(result.trade_count, 0);
    }
}

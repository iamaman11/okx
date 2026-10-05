use okx_analysis::{
    BarDecisionInput, BaselineStrategyKind, CandidateRiskContext, PortfolioCandidate,
    PositionDirection, StrategyDecision, baseline_strategy_research_metadata,
    baseline_strategy_version, evaluate_baseline_strategy, evaluate_candidate_risk,
    linear_contract_notional_usd,
};
use okx_observation::MarketHistorySnapshot;
use serde::{Deserialize, Serialize};

use crate::{BUILD_SOURCE_TREE, ExperimentSpec, ResearchError, canonical_sha256};

pub const LIVE_RESEARCH_DECISION_SCHEMA_V1: &str = "okx.research.live-decision/v1";
pub const LIVE_RESEARCH_DECISION_ALGORITHM_V1: &str =
    "okx.research.live-decision/2026-10-05.1";
const ONE_HOUR_MS: u64 = 3_600_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LiveResearchDisposition {
    Hold,
    RiskRejected,
    WouldSubmit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveResearchDecisionEvidence {
    pub schema: String,
    pub decision_evidence_id: String,
    pub algorithm_version: String,
    pub experiment_spec_id: String,
    pub hypothesis_id: String,
    pub experiment_source_tree: String,
    pub live_source_tree: String,
    pub instrument_id: String,
    pub bar: String,
    pub strategy: BaselineStrategyKind,
    pub strategy_version: String,
    pub reference_generation: String,
    pub history_generation: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub antecedent_close: Option<String>,
    pub previous_close: String,
    pub signal_close: String,
    pub signal_open_time_ms: String,
    pub signal_available_time_ms: String,
    pub entry_open_time_ms: String,
    pub entry_price: String,
    pub strategy_decision: StrategyDecision,
    pub disposition: LiveResearchDisposition,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<PositionDirection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub candidate_notional_usd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub candidate_worst_case_loss_usd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub candidate_leverage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub risk_decision: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub risk_violation_codes: Vec<String>,
    pub risk_context: CandidateRiskContext,
    pub exchange_mutation_authority: bool,
}

#[derive(Serialize)]
struct LiveResearchDecisionIdentity<'a> {
    schema: &'static str,
    algorithm_version: &'static str,
    experiment_spec_id: &'a str,
    hypothesis_id: &'a str,
    experiment_source_tree: &'a str,
    live_source_tree: &'static str,
    instrument_id: &'a str,
    bar: &'a str,
    strategy: BaselineStrategyKind,
    strategy_version: &'a str,
    reference_generation: &'a str,
    history_generation: &'a str,
    antecedent_close: &'a Option<String>,
    previous_close: &'a str,
    signal_close: &'a str,
    signal_open_time_ms: &'a str,
    signal_available_time_ms: &'a str,
    entry_open_time_ms: &'a str,
    entry_price: &'a str,
    strategy_decision: StrategyDecision,
    disposition: LiveResearchDisposition,
    direction: &'a Option<PositionDirection>,
    candidate_notional_usd: &'a Option<String>,
    candidate_worst_case_loss_usd: &'a Option<String>,
    candidate_leverage: &'a Option<String>,
    risk_decision: &'a Option<String>,
    risk_violation_codes: &'a [String],
    risk_context: &'a CandidateRiskContext,
    exchange_mutation_authority: bool,
}

pub fn evaluate_live_research_decision(
    spec: &ExperimentSpec,
    history: &MarketHistorySnapshot,
    risk_context: &CandidateRiskContext,
) -> Result<LiveResearchDecisionEvidence, ResearchError> {
    if BUILD_SOURCE_TREE == "UNAVAILABLE" {
        return Err(ResearchError::MissingField("live_decision.source_tree"));
    }
    if spec.instrument_id != history.instrument_id {
        return Err(ResearchError::InstrumentMismatch {
            expected: spec.instrument_id.clone(),
            actual: history.instrument_id.clone(),
        });
    }
    if spec.bar != "1H" || history.bar != spec.bar {
        return Err(ResearchError::InvalidLiveDecision(
            "live decision currently requires the frozen 1H bar",
        ));
    }
    if spec.strategy_version != baseline_strategy_version(spec.strategy) {
        return Err(ResearchError::InvalidLiveDecision(
            "strategy version does not match the live strategy implementation",
        ));
    }

    let metadata = baseline_strategy_research_metadata(spec.strategy);
    let confirmed_required = usize::from(metadata.signal_lookback_bars)
        .saturating_add(1)
        .max(1);
    let total_required = confirmed_required.saturating_add(1);
    if history.candles.len() < total_required {
        return Err(ResearchError::InvalidLiveDecision(
            "insufficient causal candle window",
        ));
    }

    let start = history.candles.len() - total_required;
    let window = &history.candles[start..];
    let entry = window
        .last()
        .ok_or(ResearchError::InvalidLiveDecision("missing entry candle"))?;
    if entry.confirmed {
        return Err(ResearchError::InvalidLiveDecision(
            "next-bar entry candle must still be unconfirmed",
        ));
    }
    let confirmed = &window[..window.len() - 1];
    if confirmed.iter().any(|candle| !candle.confirmed) {
        return Err(ResearchError::InvalidLiveDecision(
            "signal window contains an unconfirmed candle",
        ));
    }

    let times = window
        .iter()
        .map(|candle| {
            candle
                .open_time_ms
                .parse::<u64>()
                .map_err(|_| ResearchError::InvalidTimestamp {
                    field: "live_decision.candle_open_time_ms",
                    value: candle.open_time_ms.clone(),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    for pair in times.windows(2) {
        if pair[1].checked_sub(pair[0]) != Some(ONE_HOUR_MS) {
            return Err(ResearchError::InvalidLiveDecision(
                "live decision candle window is not contiguous 1H history",
            ));
        }
    }

    let signal = confirmed
        .last()
        .ok_or(ResearchError::InvalidLiveDecision("missing signal candle"))?;
    let signal_open_time_ms = signal
        .open_time_ms
        .parse::<u64>()
        .map_err(|_| ResearchError::InvalidTimestamp {
            field: "live_decision.signal_open_time_ms",
            value: signal.open_time_ms.clone(),
        })?;
    let signal_available_time_ms = signal_open_time_ms
        .checked_add(ONE_HOUR_MS)
        .ok_or(ResearchError::InvalidLiveDecision(
            "signal availability timestamp overflow",
        ))?;
    if signal_available_time_ms.to_string() != entry.open_time_ms {
        return Err(ResearchError::ReplayCausalityViolation);
    }

    let (antecedent_close, previous_close) = match spec.strategy {
        BaselineStrategyKind::NoTrade => (None, signal.close.clone()),
        BaselineStrategyKind::CloseMomentum => {
            let previous = confirmed
                .get(confirmed.len().saturating_sub(2))
                .ok_or(ResearchError::InvalidLiveDecision(
                    "close momentum previous candle is missing",
                ))?;
            (None, previous.close.clone())
        }
        BaselineStrategyKind::TwoBarMomentum => {
            let previous = confirmed
                .get(confirmed.len().saturating_sub(2))
                .ok_or(ResearchError::InvalidLiveDecision(
                    "two-bar momentum previous candle is missing",
                ))?;
            let antecedent = confirmed
                .get(confirmed.len().saturating_sub(3))
                .ok_or(ResearchError::InvalidLiveDecision(
                    "two-bar momentum antecedent candle is missing",
                ))?;
            (Some(antecedent.close.clone()), previous.close.clone())
        }
    };

    let strategy_decision = evaluate_baseline_strategy(
        spec.strategy,
        &BarDecisionInput {
            antecedent_close: antecedent_close.clone(),
            previous_close: previous_close.clone(),
            signal_close: signal.close.clone(),
        },
    )?;

    let mut direction = None;
    let mut candidate_notional_usd = None;
    let mut candidate_worst_case_loss_usd = None;
    let mut candidate_leverage = None;
    let mut risk_decision = None;
    let mut risk_violation_codes = Vec::new();

    let disposition = match strategy_decision {
        StrategyDecision::Hold => LiveResearchDisposition::Hold,
        StrategyDecision::EnterLong | StrategyDecision::EnterShort => {
            let candidate_direction = match strategy_decision {
                StrategyDecision::EnterLong => PositionDirection::Long,
                StrategyDecision::EnterShort => PositionDirection::Short,
                StrategyDecision::Hold => unreachable!("hold handled above"),
            };
            let notional = linear_contract_notional_usd(
                &spec.execution.contracts,
                &spec.execution.mechanics.contract_value,
                &entry.open,
            )?;
            let candidate = PortfolioCandidate {
                instrument: spec.instrument_id.clone(),
                direction: candidate_direction,
                notional_usd: notional.clone(),
                worst_case_loss_usd: spec.execution.candidate_worst_case_loss_usd.clone(),
                leverage: spec.execution.leverage.clone(),
            };
            let gate =
                evaluate_candidate_risk(&spec.mandate, &spec.policy, &candidate, risk_context)?;
            direction = Some(candidate_direction);
            candidate_notional_usd = Some(notional);
            candidate_worst_case_loss_usd =
                Some(spec.execution.candidate_worst_case_loss_usd.clone());
            candidate_leverage = Some(spec.execution.leverage.clone());
            risk_decision = Some(match gate.decision {
                okx_analysis::RiskPolicyDecision::Accepted => "accepted".to_owned(),
                okx_analysis::RiskPolicyDecision::Rejected => "rejected".to_owned(),
            });
            risk_violation_codes = gate
                .violations
                .iter()
                .map(|violation| violation.code.to_owned())
                .collect();
            match gate.decision {
                okx_analysis::RiskPolicyDecision::Accepted => {
                    LiveResearchDisposition::WouldSubmit
                }
                okx_analysis::RiskPolicyDecision::Rejected => {
                    LiveResearchDisposition::RiskRejected
                }
            }
        }
    };

    let mut evidence = LiveResearchDecisionEvidence {
        schema: LIVE_RESEARCH_DECISION_SCHEMA_V1.to_owned(),
        decision_evidence_id: String::new(),
        algorithm_version: LIVE_RESEARCH_DECISION_ALGORITHM_V1.to_owned(),
        experiment_spec_id: spec.experiment_spec_id.clone(),
        hypothesis_id: spec.hypothesis_id.clone(),
        experiment_source_tree: spec.replay_source_tree.clone(),
        live_source_tree: BUILD_SOURCE_TREE.to_owned(),
        instrument_id: spec.instrument_id.clone(),
        bar: spec.bar.clone(),
        strategy: spec.strategy,
        strategy_version: spec.strategy_version.clone(),
        reference_generation: history.reference_generation.clone(),
        history_generation: history.history_generation.clone(),
        antecedent_close,
        previous_close,
        signal_close: signal.close.clone(),
        signal_open_time_ms: signal.open_time_ms.clone(),
        signal_available_time_ms: signal_available_time_ms.to_string(),
        entry_open_time_ms: entry.open_time_ms.clone(),
        entry_price: entry.open.clone(),
        strategy_decision,
        disposition,
        direction,
        candidate_notional_usd,
        candidate_worst_case_loss_usd,
        candidate_leverage,
        risk_decision,
        risk_violation_codes,
        risk_context: risk_context.clone(),
        exchange_mutation_authority: false,
    };

    evidence.decision_evidence_id = canonical_sha256(&LiveResearchDecisionIdentity {
        schema: LIVE_RESEARCH_DECISION_SCHEMA_V1,
        algorithm_version: LIVE_RESEARCH_DECISION_ALGORITHM_V1,
        experiment_spec_id: &evidence.experiment_spec_id,
        hypothesis_id: &evidence.hypothesis_id,
        experiment_source_tree: &evidence.experiment_source_tree,
        live_source_tree: BUILD_SOURCE_TREE,
        instrument_id: &evidence.instrument_id,
        bar: &evidence.bar,
        strategy: evidence.strategy,
        strategy_version: &evidence.strategy_version,
        reference_generation: &evidence.reference_generation,
        history_generation: &evidence.history_generation,
        antecedent_close: &evidence.antecedent_close,
        previous_close: &evidence.previous_close,
        signal_close: &evidence.signal_close,
        signal_open_time_ms: &evidence.signal_open_time_ms,
        signal_available_time_ms: &evidence.signal_available_time_ms,
        entry_open_time_ms: &evidence.entry_open_time_ms,
        entry_price: &evidence.entry_price,
        strategy_decision: evidence.strategy_decision,
        disposition: evidence.disposition,
        direction: &evidence.direction,
        candidate_notional_usd: &evidence.candidate_notional_usd,
        candidate_worst_case_loss_usd: &evidence.candidate_worst_case_loss_usd,
        candidate_leverage: &evidence.candidate_leverage,
        risk_decision: &evidence.risk_decision,
        risk_violation_codes: &evidence.risk_violation_codes,
        risk_context: &evidence.risk_context,
        exchange_mutation_authority: false,
    })?;

    Ok(evidence)
}

#[cfg(test)]
mod tests {
    use okx_analysis::{
        HARD_RISK_POLICY_SCHEMA_V1, HardRiskPolicy, LiquidityRole, RiskDegradedMode,
        RiskMinimumQuality, TRADING_MANDATE_SCHEMA_V1, TradingMandate,
    };
    use okx_observation::{HistoryCandle, MARKET_HISTORY_SCHEMA_V1};

    use super::*;
    use crate::{EXPERIMENT_SPEC_SCHEMA_V1, ReplayExecutionModel, ReplayMechanicsProvenance};

    fn spec() -> ExperimentSpec {
        ExperimentSpec {
            schema: EXPERIMENT_SPEC_SCHEMA_V1.to_owned(),
            experiment_spec_id: "sha256:spec".to_owned(),
            hypothesis_id: "sha256:hypothesis".to_owned(),
            dataset_id: "sha256:dataset".to_owned(),
            replay_source_tree: "research-tree".to_owned(),
            instrument_id: "BTC-USDT-SWAP".to_owned(),
            bar: "1H".to_owned(),
            strategy: BaselineStrategyKind::TwoBarMomentum,
            strategy_version: baseline_strategy_version(BaselineStrategyKind::TwoBarMomentum)
                .to_owned(),
            deterministic_seed: 0,
            execution: ReplayExecutionModel {
                version: "execution/v1".to_owned(),
                mechanics: okx_analysis::PositionScenarioMechanics {
                    settle_currency: "USDT".to_owned(),
                    contract_value_currency: "BTC".to_owned(),
                    contract_value: "0.01".to_owned(),
                    tick_size: "0.1".to_owned(),
                    entry_fee_rate: "-0.0005".to_owned(),
                    exit_fee_rate: "-0.0005".to_owned(),
                },
                mechanics_provenance: ReplayMechanicsProvenance::DeclaredCounterfactual,
                fee_provenance: "declared".to_owned(),
                funding_provenance: "observed".to_owned(),
                contracts: "0.01".to_owned(),
                leverage: "2".to_owned(),
                candidate_worst_case_loss_usd: "2".to_owned(),
                entry_liquidity_role: LiquidityRole::Taker,
                exit_liquidity_role: LiquidityRole::Taker,
                funding_notional_basis: "ENTRY_NOTIONAL_COUNTERFACTUAL".to_owned(),
            },
            mandate: TradingMandate {
                schema: TRADING_MANDATE_SCHEMA_V1.to_owned(),
                version: "mandate/v1".to_owned(),
                capital_base_usd: "100".to_owned(),
                decision_horizon_hours: 1,
                benchmark: Some("NO_EXTERNAL_BENCHMARK".to_owned()),
                allowed_instruments: vec!["BTC-USDT-SWAP".to_owned()],
                max_drawdown_ratio: "0.5".to_owned(),
                leverage_ceiling: "5".to_owned(),
                minimum_liquidity_notional_usd: "0".to_owned(),
                max_turnover_ratio: "100".to_owned(),
            },
            policy: HardRiskPolicy {
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
            },
            initial_risk_context: risk_context(true),
        }
    }

    fn risk_context(fresh: bool) -> CandidateRiskContext {
        CandidateRiskContext {
            total_equity_usd: "100".to_owned(),
            account_gross_notional_usd: "0".to_owned(),
            instrument_gross_notional_usd: "0".to_owned(),
            account_initial_margin_usd: "0".to_owned(),
            capital_base_drawdown_ratio: "0".to_owned(),
            daily_realized_loss_usd: "0".to_owned(),
            account_is_fresh: fresh,
        }
    }

    fn candle(hour: u64, close: &str, confirmed: bool) -> HistoryCandle {
        HistoryCandle {
            open_time_ms: (hour * ONE_HOUR_MS).to_string(),
            open: close.to_owned(),
            high: close.to_owned(),
            low: close.to_owned(),
            close: close.to_owned(),
            volume: "1".to_owned(),
            volume_currency: "1".to_owned(),
            volume_quote: Some("1".to_owned()),
            confirmed,
        }
    }

    fn history(closes: [&str; 4]) -> MarketHistorySnapshot {
        let candles = vec![
            candle(0, closes[0], true),
            candle(1, closes[1], true),
            candle(2, closes[2], true),
            candle(3, closes[3], false),
        ];
        MarketHistorySnapshot {
            schema: MARKET_HISTORY_SCHEMA_V1.to_owned(),
            instrument_id: "BTC-USDT-SWAP".to_owned(),
            bar: "1H".to_owned(),
            requested_limit: 4,
            reference_generation: "sha256:reference".to_owned(),
            source: "okx_public_rest_history".to_owned(),
            source_received_at: "2026-10-05T00:00:00.000Z".to_owned(),
            history_generation: "sha256:history".to_owned(),
            all_confirmed: false,
            oldest_open_time_ms: "0".to_owned(),
            newest_open_time_ms: (3 * ONE_HOUR_MS).to_string(),
            candles,
        }
    }

    #[test]
    fn two_bar_live_signal_reuses_strategy_and_risk_without_lookahead() {
        let spec = spec();
        let risk = risk_context(true);
        let history = history(["100", "101", "102", "103"]);
        let result =
            evaluate_live_research_decision(&spec, &history, &risk).expect("live decision");

        assert_eq!(result.strategy_decision, StrategyDecision::EnterLong);
        assert_eq!(result.disposition, LiveResearchDisposition::WouldSubmit);
        assert_eq!(result.direction, Some(PositionDirection::Long));
        assert_eq!(result.entry_open_time_ms, (3 * ONE_HOUR_MS).to_string());
        assert_eq!(result.entry_price, "103");
        assert_eq!(result.candidate_notional_usd.as_deref(), Some("0.0103"));
        assert_eq!(result.candidate_leverage.as_deref(), Some("2"));
        assert_eq!(result.risk_decision.as_deref(), Some("accepted"));
        assert!(!result.exchange_mutation_authority);

        let direct_strategy = evaluate_baseline_strategy(
            BaselineStrategyKind::TwoBarMomentum,
            &BarDecisionInput {
                antecedent_close: Some("100".to_owned()),
                previous_close: "101".to_owned(),
                signal_close: "102".to_owned(),
            },
        )
        .expect("direct strategy");
        assert_eq!(result.strategy_decision, direct_strategy);

        let direct_notional = linear_contract_notional_usd("0.01", "0.01", "103")
            .expect("direct notional");
        assert_eq!(result.candidate_notional_usd.as_deref(), Some(direct_notional.as_str()));
    }

    #[test]
    fn risk_rejection_is_preserved_as_read_only_would_not_submit() {
        let result = evaluate_live_research_decision(
            &spec(),
            &history(["100", "101", "102", "103"]),
            &risk_context(false),
        )
        .expect("risk decision");
        assert_eq!(result.strategy_decision, StrategyDecision::EnterLong);
        assert_eq!(result.disposition, LiveResearchDisposition::RiskRejected);
        assert_eq!(result.risk_decision.as_deref(), Some("rejected"));
        assert!(
            result
                .risk_violation_codes
                .iter()
                .any(|code| code == "MINIMUM_DATA_QUALITY")
        );
    }

    #[test]
    fn hold_does_not_construct_a_candidate() {
        let result = evaluate_live_research_decision(
            &spec(),
            &history(["100", "101", "100", "100"]),
            &risk_context(true),
        )
        .expect("hold");
        assert_eq!(result.strategy_decision, StrategyDecision::Hold);
        assert_eq!(result.disposition, LiveResearchDisposition::Hold);
        assert!(result.candidate_notional_usd.is_none());
        assert!(result.risk_decision.is_none());
    }

    #[test]
    fn live_window_fails_closed_on_confirmed_entry_or_gap() {
        let mut confirmed_entry = history(["100", "101", "102", "103"]);
        confirmed_entry.candles[3].confirmed = true;
        assert!(matches!(
            evaluate_live_research_decision(&spec(), &confirmed_entry, &risk_context(true)),
            Err(ResearchError::InvalidLiveDecision(_))
        ));

        let mut gap = history(["100", "101", "102", "103"]);
        gap.candles[2].open_time_ms = (3 * ONE_HOUR_MS).to_string();
        gap.candles[3].open_time_ms = (4 * ONE_HOUR_MS).to_string();
        assert!(matches!(
            evaluate_live_research_decision(&spec(), &gap, &risk_context(true)),
            Err(ResearchError::InvalidLiveDecision(_))
        ));
    }

    #[test]
    fn identical_live_input_has_identical_evidence_identity() {
        let left = evaluate_live_research_decision(
            &spec(),
            &history(["100", "101", "102", "103"]),
            &risk_context(true),
        )
        .expect("left");
        let right = evaluate_live_research_decision(
            &spec(),
            &history(["100", "101", "102", "103"]),
            &risk_context(true),
        )
        .expect("right");
        assert_eq!(left.decision_evidence_id, right.decision_evidence_id);
        assert_eq!(left, right);
    }
}

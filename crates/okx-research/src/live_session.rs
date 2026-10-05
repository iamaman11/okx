use std::str::FromStr;

use okx_analysis::{
    PositionDirection, PositionScenarioAssumptions, ScenarioExitAssumption,
    analyze_position_scenario_values, funding_user_cost_quote,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    BUILD_SOURCE_TREE, ExperimentSpec, PROMOTION_BUNDLE_SCHEMA_V1, PromotionBundle,
    PromotionTransition, ResearchArtifactStore, ResearchError, ResearchFundingEvent,
    ResearchPromotionState, canonical_sha256, load_accepted_promotion_transition,
};

pub const LIVE_RESEARCH_SESSION_CONFIG_SCHEMA_V1: &str =
    "okx.research.live-session-config/v1";
pub const LIVE_RESEARCH_SESSION_CHECKPOINT_SCHEMA_V1: &str =
    "okx.research.live-session-checkpoint/v1";
pub const PAPER_VIRTUAL_TRADE_SCHEMA_V1: &str = "okx.research.paper-virtual-trade/v1";
pub const LIVE_RESEARCH_SESSION_ALGORITHM_V1: &str =
    "okx.research.live-session/2026-10-05.1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LiveResearchSessionMode {
    Paper,
    Shadow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LiveResearchSessionStatus {
    Active,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveResearchSessionConfig {
    pub schema: String,
    pub session_id: String,
    pub algorithm_version: String,
    pub mode: LiveResearchSessionMode,
    pub promotion_transition_artifact_id: String,
    pub promotion_transition_id: String,
    pub promotion_bundle_artifact_id: String,
    pub hypothesis_id: String,
    pub experiment_spec_artifact_id: String,
    pub experiment_spec_id: String,
    pub instrument_id: String,
    pub bar: String,
    pub strategy: okx_analysis::BaselineStrategyKind,
    pub strategy_version: String,
    pub session_source_tree: String,
    pub exchange_mutation_authority: bool,
}

#[derive(Serialize)]
struct LiveResearchSessionConfigIdentity<'a> {
    schema: &'static str,
    algorithm_version: &'static str,
    mode: LiveResearchSessionMode,
    promotion_transition_artifact_id: &'a str,
    promotion_transition_id: &'a str,
    promotion_bundle_artifact_id: &'a str,
    hypothesis_id: &'a str,
    experiment_spec_artifact_id: &'a str,
    experiment_spec_id: &'a str,
    instrument_id: &'a str,
    bar: &'a str,
    strategy: okx_analysis::BaselineStrategyKind,
    strategy_version: &'a str,
    session_source_tree: &'static str,
    exchange_mutation_authority: bool,
}

impl LiveResearchSessionConfig {
    pub fn validate(&self) -> Result<(), ResearchError> {
        if self.schema != LIVE_RESEARCH_SESSION_CONFIG_SCHEMA_V1
            || self.algorithm_version != LIVE_RESEARCH_SESSION_ALGORITHM_V1
            || self.exchange_mutation_authority
            || self.session_source_tree != BUILD_SOURCE_TREE
        {
            return Err(ResearchError::ArtifactIdentityMismatch);
        }
        let expected = canonical_sha256(&LiveResearchSessionConfigIdentity {
            schema: LIVE_RESEARCH_SESSION_CONFIG_SCHEMA_V1,
            algorithm_version: LIVE_RESEARCH_SESSION_ALGORITHM_V1,
            mode: self.mode,
            promotion_transition_artifact_id: &self.promotion_transition_artifact_id,
            promotion_transition_id: &self.promotion_transition_id,
            promotion_bundle_artifact_id: &self.promotion_bundle_artifact_id,
            hypothesis_id: &self.hypothesis_id,
            experiment_spec_artifact_id: &self.experiment_spec_artifact_id,
            experiment_spec_id: &self.experiment_spec_id,
            instrument_id: &self.instrument_id,
            bar: &self.bar,
            strategy: self.strategy,
            strategy_version: &self.strategy_version,
            session_source_tree: BUILD_SOURCE_TREE,
            exchange_mutation_authority: false,
        })?;
        if expected != self.session_id {
            return Err(ResearchError::ArtifactIdentityMismatch);
        }
        Ok(())
    }
}

pub fn load_live_research_session_config(
    store: &ResearchArtifactStore,
    promotion_transition_artifact_id: &str,
    experiment_spec_artifact_id: &str,
) -> Result<(LiveResearchSessionConfig, ExperimentSpec), ResearchError> {
    if BUILD_SOURCE_TREE == "UNAVAILABLE" {
        return Err(ResearchError::MissingField("live_session.source_tree"));
    }

    let transition =
        load_accepted_promotion_transition(store, promotion_transition_artifact_id)?;
    let bundle: PromotionBundle =
        store.read_evidence_json(&transition.promotion_bundle_artifact_id)?;
    if bundle.schema != PROMOTION_BUNDLE_SCHEMA_V1
        || bundle.final_oos.experiment_spec_artifact_id != experiment_spec_artifact_id
    {
        return Err(ResearchError::ArtifactIdentityMismatch);
    }

    let spec: ExperimentSpec = store.read_evidence_json(experiment_spec_artifact_id)?;
    validate_live_session_binding(&transition, &bundle, &spec)?;

    let mode = match transition.to {
        ResearchPromotionState::Paper => LiveResearchSessionMode::Paper,
        ResearchPromotionState::Shadow => LiveResearchSessionMode::Shadow,
        ResearchPromotionState::Research | ResearchPromotionState::Backtested => {
            return Err(ResearchError::InvalidLiveResearchSession(
                "promotion does not authorize PAPER or SHADOW",
            ));
        }
    };

    let session_id = canonical_sha256(&LiveResearchSessionConfigIdentity {
        schema: LIVE_RESEARCH_SESSION_CONFIG_SCHEMA_V1,
        algorithm_version: LIVE_RESEARCH_SESSION_ALGORITHM_V1,
        mode,
        promotion_transition_artifact_id,
        promotion_transition_id: &transition.transition_id,
        promotion_bundle_artifact_id: &transition.promotion_bundle_artifact_id,
        hypothesis_id: &transition.hypothesis_id,
        experiment_spec_artifact_id,
        experiment_spec_id: &spec.experiment_spec_id,
        instrument_id: &spec.instrument_id,
        bar: &spec.bar,
        strategy: spec.strategy,
        strategy_version: &spec.strategy_version,
        session_source_tree: BUILD_SOURCE_TREE,
        exchange_mutation_authority: false,
    })?;
    let config = LiveResearchSessionConfig {
        schema: LIVE_RESEARCH_SESSION_CONFIG_SCHEMA_V1.to_owned(),
        session_id,
        algorithm_version: LIVE_RESEARCH_SESSION_ALGORITHM_V1.to_owned(),
        mode,
        promotion_transition_artifact_id: promotion_transition_artifact_id.to_owned(),
        promotion_transition_id: transition.transition_id,
        promotion_bundle_artifact_id: transition.promotion_bundle_artifact_id,
        hypothesis_id: transition.hypothesis_id,
        experiment_spec_artifact_id: experiment_spec_artifact_id.to_owned(),
        experiment_spec_id: spec.experiment_spec_id.clone(),
        instrument_id: spec.instrument_id.clone(),
        bar: spec.bar.clone(),
        strategy: spec.strategy,
        strategy_version: spec.strategy_version.clone(),
        session_source_tree: BUILD_SOURCE_TREE.to_owned(),
        exchange_mutation_authority: false,
    };
    config.validate()?;
    Ok((config, spec))
}

fn validate_live_session_binding(
    transition: &PromotionTransition,
    bundle: &PromotionBundle,
    spec: &ExperimentSpec,
) -> Result<(), ResearchError> {
    if spec.hypothesis_id != transition.hypothesis_id
        || spec.hypothesis_id != bundle.hypothesis_id
        || spec.strategy != transition.strategy
        || spec.strategy != bundle.strategy
        || spec.strategy_version != transition.strategy_version
        || spec.strategy_version != bundle.strategy_version
        || spec.instrument_id != "BTC-USDT-SWAP"
        || spec.bar != "1H"
    {
        return Err(ResearchError::ArtifactIdentityMismatch);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperVirtualPosition {
    pub decision_evidence_artifact_id: String,
    pub direction: PositionDirection,
    pub entry_time_ms: String,
    pub entry_price: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperVirtualTradeEvidence {
    pub schema: String,
    pub trade_evidence_id: String,
    pub algorithm_version: String,
    pub session_id: String,
    pub decision_evidence_artifact_id: String,
    pub direction: PositionDirection,
    pub entry_time_ms: String,
    pub exit_time_ms: String,
    pub entry_price: String,
    pub exit_price: String,
    pub contracts: String,
    pub leverage: String,
    pub gross_pnl_quote: String,
    pub trading_cost_quote: String,
    pub funding_cost_quote: String,
    pub net_pnl_quote: String,
    pub funding_event_count: usize,
    pub source_tree: String,
}

#[derive(Serialize)]
struct PaperVirtualTradeIdentity<'a> {
    schema: &'static str,
    algorithm_version: &'static str,
    session_id: &'a str,
    decision_evidence_artifact_id: &'a str,
    direction: PositionDirection,
    entry_time_ms: &'a str,
    exit_time_ms: &'a str,
    entry_price: &'a str,
    exit_price: &'a str,
    contracts: &'a str,
    leverage: &'a str,
    gross_pnl_quote: &'a str,
    trading_cost_quote: &'a str,
    funding_cost_quote: &'a str,
    net_pnl_quote: &'a str,
    funding_event_count: usize,
    source_tree: &'static str,
}

pub fn settle_paper_virtual_position(
    session_id: &str,
    spec: &ExperimentSpec,
    position: &PaperVirtualPosition,
    exit_time_ms: &str,
    exit_price: &str,
    funding: &[ResearchFundingEvent],
) -> Result<PaperVirtualTradeEvidence, ResearchError> {
    if session_id.trim().is_empty() || BUILD_SOURCE_TREE == "UNAVAILABLE" {
        return Err(ResearchError::InvalidLiveResearchSession(
            "PAPER settlement requires bound session identity",
        ));
    }
    let entry_time = timestamp("paper.entry_time_ms", &position.entry_time_ms)?;
    let exit_time = timestamp("paper.exit_time_ms", exit_time_ms)?;
    if exit_time <= entry_time {
        return Err(ResearchError::ReplayCausalityViolation);
    }

    let scenario = analyze_position_scenario_values(
        &spec.instrument_id,
        "PAPER_LIVE_REFERENCE",
        &spec.execution.fee_provenance,
        &spec.execution.mechanics,
        &PositionScenarioAssumptions {
            direction: position.direction,
            contracts: spec.execution.contracts.clone(),
            entry_price: position.entry_price.clone(),
            exit: ScenarioExitAssumption::Price {
                price: exit_price.to_owned(),
            },
            entry_liquidity_role: spec.execution.entry_liquidity_role,
            exit_liquidity_role: spec.execution.exit_liquidity_role,
        },
    )?;

    let entry_notional = decimal("paper.entry_notional", &scenario.entry_settle_notional)?;
    let gross = decimal("paper.gross_pnl", &scenario.gross_pnl_settle)?;
    let entry_cost = decimal(
        "paper.entry_trading_cost",
        &scenario.entry_trading_cost_settle,
    )?;
    let exit_cost = decimal(
        "paper.exit_trading_cost",
        &scenario.exit_trading_cost_settle,
    )?;
    let trading_cost = entry_cost + exit_cost;
    let mut funding_cost = Decimal::ZERO;
    let mut funding_event_count = 0usize;
    for event in funding {
        let event_time = timestamp("paper.funding_time_ms", &event.funding_time_ms)?;
        if event_time < entry_time || event_time >= exit_time {
            continue;
        }
        let rate = event
            .realized_rate
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(&event.funding_rate);
        funding_cost += decimal(
            "paper.funding_cost",
            &funding_user_cost_quote(
                &entry_notional.normalize().to_string(),
                rate,
                position.direction,
            )?,
        )?;
        funding_event_count += 1;
    }
    let net = gross - trading_cost - funding_cost;
    let gross_pnl_quote = gross.normalize().to_string();
    let trading_cost_quote = trading_cost.normalize().to_string();
    let funding_cost_quote = funding_cost.normalize().to_string();
    let net_pnl_quote = net.normalize().to_string();

    let trade_evidence_id = canonical_sha256(&PaperVirtualTradeIdentity {
        schema: PAPER_VIRTUAL_TRADE_SCHEMA_V1,
        algorithm_version: LIVE_RESEARCH_SESSION_ALGORITHM_V1,
        session_id,
        decision_evidence_artifact_id: &position.decision_evidence_artifact_id,
        direction: position.direction,
        entry_time_ms: &position.entry_time_ms,
        exit_time_ms,
        entry_price: &position.entry_price,
        exit_price,
        contracts: &spec.execution.contracts,
        leverage: &spec.execution.leverage,
        gross_pnl_quote: &gross_pnl_quote,
        trading_cost_quote: &trading_cost_quote,
        funding_cost_quote: &funding_cost_quote,
        net_pnl_quote: &net_pnl_quote,
        funding_event_count,
        source_tree: BUILD_SOURCE_TREE,
    })?;

    Ok(PaperVirtualTradeEvidence {
        schema: PAPER_VIRTUAL_TRADE_SCHEMA_V1.to_owned(),
        trade_evidence_id,
        algorithm_version: LIVE_RESEARCH_SESSION_ALGORITHM_V1.to_owned(),
        session_id: session_id.to_owned(),
        decision_evidence_artifact_id: position.decision_evidence_artifact_id.clone(),
        direction: position.direction,
        entry_time_ms: position.entry_time_ms.clone(),
        exit_time_ms: exit_time_ms.to_owned(),
        entry_price: position.entry_price.clone(),
        exit_price: exit_price.to_owned(),
        contracts: spec.execution.contracts.clone(),
        leverage: spec.execution.leverage.clone(),
        gross_pnl_quote,
        trading_cost_quote,
        funding_cost_quote,
        net_pnl_quote,
        funding_event_count,
        source_tree: BUILD_SOURCE_TREE.to_owned(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveResearchSessionCheckpoint {
    pub schema: String,
    pub checkpoint_id: String,
    pub algorithm_version: String,
    pub session_id: String,
    pub config_artifact_id: String,
    pub status: LiveResearchSessionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_checkpoint_artifact_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_evaluated_entry_open_time_ms: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_decision_artifact_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_paper_trade_artifact_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_blocker: Option<String>,
    pub decision_count: u64,
    pub blocked_count: u64,
    pub would_submit_count: u64,
    pub paper_trade_count: u64,
    pub paper_realized_net_pnl_quote: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paper_open_position: Option<PaperVirtualPosition>,
    pub source_tree: String,
}

#[derive(Serialize)]
struct LiveResearchSessionCheckpointIdentity<'a> {
    schema: &'static str,
    algorithm_version: &'static str,
    session_id: &'a str,
    config_artifact_id: &'a str,
    status: LiveResearchSessionStatus,
    parent_checkpoint_artifact_id: &'a Option<String>,
    last_evaluated_entry_open_time_ms: &'a Option<String>,
    latest_decision_artifact_id: &'a Option<String>,
    latest_paper_trade_artifact_id: &'a Option<String>,
    last_blocker: &'a Option<String>,
    decision_count: u64,
    blocked_count: u64,
    would_submit_count: u64,
    paper_trade_count: u64,
    paper_realized_net_pnl_quote: &'a str,
    paper_open_position: &'a Option<PaperVirtualPosition>,
    source_tree: &'static str,
}

impl LiveResearchSessionCheckpoint {
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        session_id: impl Into<String>,
        config_artifact_id: impl Into<String>,
        status: LiveResearchSessionStatus,
        parent_checkpoint_artifact_id: Option<String>,
        last_evaluated_entry_open_time_ms: Option<String>,
        latest_decision_artifact_id: Option<String>,
        latest_paper_trade_artifact_id: Option<String>,
        last_blocker: Option<String>,
        decision_count: u64,
        blocked_count: u64,
        would_submit_count: u64,
        paper_trade_count: u64,
        paper_realized_net_pnl_quote: impl Into<String>,
        paper_open_position: Option<PaperVirtualPosition>,
    ) -> Result<Self, ResearchError> {
        let session_id = session_id.into();
        let config_artifact_id = config_artifact_id.into();
        let paper_realized_net_pnl_quote = paper_realized_net_pnl_quote.into();
        if session_id.trim().is_empty() || config_artifact_id.trim().is_empty() {
            return Err(ResearchError::InvalidLiveResearchSession(
                "checkpoint requires session and config identity",
            ));
        }
        decimal(
            "paper_realized_net_pnl_quote",
            &paper_realized_net_pnl_quote,
        )?;
        if let Some(value) = last_evaluated_entry_open_time_ms.as_deref() {
            timestamp("last_evaluated_entry_open_time_ms", value)?;
        }
        let checkpoint_id = canonical_sha256(&LiveResearchSessionCheckpointIdentity {
            schema: LIVE_RESEARCH_SESSION_CHECKPOINT_SCHEMA_V1,
            algorithm_version: LIVE_RESEARCH_SESSION_ALGORITHM_V1,
            session_id: &session_id,
            config_artifact_id: &config_artifact_id,
            status,
            parent_checkpoint_artifact_id: &parent_checkpoint_artifact_id,
            last_evaluated_entry_open_time_ms: &last_evaluated_entry_open_time_ms,
            latest_decision_artifact_id: &latest_decision_artifact_id,
            latest_paper_trade_artifact_id: &latest_paper_trade_artifact_id,
            last_blocker: &last_blocker,
            decision_count,
            blocked_count,
            would_submit_count,
            paper_trade_count,
            paper_realized_net_pnl_quote: &paper_realized_net_pnl_quote,
            paper_open_position: &paper_open_position,
            source_tree: BUILD_SOURCE_TREE,
        })?;
        Ok(Self {
            schema: LIVE_RESEARCH_SESSION_CHECKPOINT_SCHEMA_V1.to_owned(),
            checkpoint_id,
            algorithm_version: LIVE_RESEARCH_SESSION_ALGORITHM_V1.to_owned(),
            session_id,
            config_artifact_id,
            status,
            parent_checkpoint_artifact_id,
            last_evaluated_entry_open_time_ms,
            latest_decision_artifact_id,
            latest_paper_trade_artifact_id,
            last_blocker,
            decision_count,
            blocked_count,
            would_submit_count,
            paper_trade_count,
            paper_realized_net_pnl_quote,
            paper_open_position,
            source_tree: BUILD_SOURCE_TREE.to_owned(),
        })
    }

    pub fn validate(&self) -> Result<(), ResearchError> {
        if self.schema != LIVE_RESEARCH_SESSION_CHECKPOINT_SCHEMA_V1
            || self.algorithm_version != LIVE_RESEARCH_SESSION_ALGORITHM_V1
            || self.source_tree != BUILD_SOURCE_TREE
        {
            return Err(ResearchError::ArtifactIdentityMismatch);
        }
        let rebuilt = Self::build(
            self.session_id.clone(),
            self.config_artifact_id.clone(),
            self.status,
            self.parent_checkpoint_artifact_id.clone(),
            self.last_evaluated_entry_open_time_ms.clone(),
            self.latest_decision_artifact_id.clone(),
            self.latest_paper_trade_artifact_id.clone(),
            self.last_blocker.clone(),
            self.decision_count,
            self.blocked_count,
            self.would_submit_count,
            self.paper_trade_count,
            self.paper_realized_net_pnl_quote.clone(),
            self.paper_open_position.clone(),
        )?;
        if rebuilt.checkpoint_id != self.checkpoint_id {
            return Err(ResearchError::ArtifactIdentityMismatch);
        }
        Ok(())
    }
}

pub fn paper_realized_pnl_after_trade(
    current_realized_net_pnl_quote: &str,
    trade: &PaperVirtualTradeEvidence,
) -> Result<String, ResearchError> {
    let current = decimal(
        "paper_realized_net_pnl_quote",
        current_realized_net_pnl_quote,
    )?;
    let trade_net = decimal("paper_trade.net_pnl_quote", &trade.net_pnl_quote)?;
    Ok((current + trade_net).normalize().to_string())
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

#[cfg(test)]
mod tests {
    use okx_analysis::{
        TWO_BAR_MOMENTUM_STRATEGY_VERSION_V1, CandidateRiskContext, HardRiskPolicy,
        LiquidityRole, PositionScenarioMechanics, RiskDegradedMode, RiskMinimumQuality,
        TradingMandate,
    };

    use super::*;
    use crate::ReplayExecutionModel;

    fn spec() -> ExperimentSpec {
        ExperimentSpec {
            schema: crate::EXPERIMENT_SPEC_SCHEMA_V1.to_owned(),
            experiment_spec_id: "sha256:spec".to_owned(),
            hypothesis_id: "sha256:hypothesis".to_owned(),
            dataset_id: "sha256:dataset".to_owned(),
            replay_source_tree: "research-tree".to_owned(),
            instrument_id: "BTC-USDT-SWAP".to_owned(),
            bar: "1H".to_owned(),
            strategy: okx_analysis::BaselineStrategyKind::TwoBarMomentum,
            strategy_version: BASELINE_STRATEGY_VERSION_TWO_BAR_MOMENTUM_V1.to_owned(),
            deterministic_seed: 0,
            execution: ReplayExecutionModel {
                version: "execution/v1".to_owned(),
                mechanics: PositionScenarioMechanics {
                    settle_currency: "USDT".to_owned(),
                    contract_value_currency: "BTC".to_owned(),
                    contract_value: "0.01".to_owned(),
                    tick_size: "0.1".to_owned(),
                    entry_fee_rate: "-0.0005".to_owned(),
                    exit_fee_rate: "-0.0005".to_owned(),
                },
                mechanics_provenance: crate::ReplayMechanicsProvenance::DeclaredCounterfactual,
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
                schema: okx_analysis::TRADING_MANDATE_SCHEMA_V1.to_owned(),
                version: "mandate/v1".to_owned(),
                capital_base_usd: "100".to_owned(),
                decision_horizon_hours: 1,
                benchmark: None,
                allowed_instruments: vec!["BTC-USDT-SWAP".to_owned()],
                max_drawdown_ratio: "0.5".to_owned(),
                leverage_ceiling: "5".to_owned(),
                minimum_liquidity_notional_usd: "0".to_owned(),
                max_turnover_ratio: "100".to_owned(),
            },
            policy: HardRiskPolicy {
                schema: okx_analysis::HARD_RISK_POLICY_SCHEMA_V1.to_owned(),
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
            initial_risk_context: CandidateRiskContext {
                total_equity_usd: "100".to_owned(),
                account_gross_notional_usd: "0".to_owned(),
                instrument_gross_notional_usd: "0".to_owned(),
                account_initial_margin_usd: "0".to_owned(),
                capital_base_drawdown_ratio: "0".to_owned(),
                daily_realized_loss_usd: "0".to_owned(),
                account_is_fresh: true,
            },
        }
    }

    #[test]
    fn checkpoint_identity_is_deterministic() {
        let first = LiveResearchSessionCheckpoint::build(
            "sha256:session",
            "sha256:config",
            LiveResearchSessionStatus::Active,
            None,
            Some("3600000".to_owned()),
            Some("sha256:decision".to_owned()),
            None,
            None,
            1,
            0,
            1,
            0,
            "0",
            None,
        )
        .expect("checkpoint");
        let second = LiveResearchSessionCheckpoint::build(
            "sha256:session",
            "sha256:config",
            LiveResearchSessionStatus::Active,
            None,
            Some("3600000".to_owned()),
            Some("sha256:decision".to_owned()),
            None,
            1,
            0,
            1,
            0,
            "0",
            None,
        )
        .expect("checkpoint");
        assert_eq!(first.checkpoint_id, second.checkpoint_id);
        first.validate().expect("valid");
    }

    #[test]
    fn paper_settlement_reuses_scenario_and_funding_math() {
        let position = PaperVirtualPosition {
            decision_evidence_artifact_id: "sha256:decision".to_owned(),
            direction: PositionDirection::Long,
            entry_time_ms: "0".to_owned(),
            entry_price: "100".to_owned(),
        };
        let funding = vec![ResearchFundingEvent {
            schema: crate::RESEARCH_FUNDING_SCHEMA_V1.to_owned(),
            funding_time_ms: "1800000".to_owned(),
            available_time_ms: "1800000".to_owned(),
            funding_rate: "0.001".to_owned(),
            realized_rate: Some("0.001".to_owned()),
            formula_type: None,
            method: None,
        }];

        let trade = settle_paper_virtual_position(
            "sha256:session",
            &spec(),
            &position,
            "3600000",
            "110",
            &funding,
        )
        .expect("paper trade");

        assert_eq!(trade.gross_pnl_quote, "0.001");
        assert_eq!(trade.trading_cost_quote, "0.0000105");
        assert_eq!(trade.funding_cost_quote, "0.00001");
        assert_eq!(trade.net_pnl_quote, "0.0009795");
        assert_eq!(trade.funding_event_count, 1);
    }
}

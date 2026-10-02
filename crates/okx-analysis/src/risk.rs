use std::collections::{BTreeMap, BTreeSet};

use okx_observation::{
    AccountLedgerSummary, AccountPositionState, AccountSnapshot, CurrencyAggregate,
};
use rust_decimal::Decimal;
use serde::Serialize;

use super::{AnalysisError, PositionDirection, decimal, positive_decimal};

pub const ACCOUNT_RISK_ANALYSIS_SCHEMA_V1: &str = "okx.account-risk-analysis/v1";
pub const PORTFOLIO_RISK_ANALYSIS_SCHEMA_V2: &str = "okx.portfolio-risk-analysis/v2";
pub const TRADING_MANDATE_SCHEMA_V1: &str = "okx.trading-mandate/v1";
pub const HARD_RISK_POLICY_SCHEMA_V1: &str = "okx.hard-risk-policy/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PositionRiskAnalysis {
    pub instrument_type: String,
    pub instrument_id: String,
    pub direction: PositionDirection,
    pub margin_mode: String,
    pub position_contracts: String,
    pub position_notional_usd: String,
    pub signed_notional_usd: String,
    pub gross_concentration_ratio: String,
    pub configured_leverage: Option<String>,
    pub initial_margin_requirement_usd: Option<String>,
    pub maintenance_margin_requirement_usd: Option<String>,
    pub exchange_margin_ratio: Option<String>,
    pub mark_price: Option<String>,
    pub estimated_liquidation_price: Option<String>,
    pub estimated_liquidation_distance_ratio: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountRiskAnalysis {
    pub schema: String,
    pub account_generation: String,
    pub account_source: String,
    pub account_quality_reason: String,
    pub account_level: String,
    pub position_mode: String,
    pub total_equity_usd: String,
    pub adjusted_equity_usd: Option<String>,
    pub exchange_gross_notional_usd: Option<String>,
    pub gross_position_notional_usd: String,
    pub directional_net_position_notional_usd: String,
    pub gross_exposure_to_equity_ratio: Option<String>,
    pub directional_net_exposure_to_equity_ratio: Option<String>,
    pub initial_margin_requirement_usd: Option<String>,
    pub maintenance_margin_requirement_usd: Option<String>,
    pub exchange_margin_ratio: Option<String>,
    pub source_position_record_count: usize,
    pub open_position_count: usize,
    pub pending_order_count: usize,
    pub positions: Vec<PositionRiskAnalysis>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TradingMandate {
    pub schema: &'static str,
    pub version: String,
    pub capital_base_usd: String,
    pub decision_horizon_hours: u32,
    pub benchmark: Option<String>,
    pub allowed_instruments: Vec<String>,
    pub max_drawdown_ratio: String,
    pub leverage_ceiling: String,
    pub minimum_liquidity_notional_usd: String,
    pub max_turnover_ratio: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CorrelatedClusterLimit {
    pub id: String,
    pub instruments: Vec<String>,
    pub max_gross_notional_usd: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskMinimumQuality {
    Fresh,
    Degraded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskDegradedMode {
    Reject,
    AllowReadOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HardRiskPolicy {
    pub schema: &'static str,
    pub version: String,
    pub max_account_gross_notional_usd: String,
    pub max_instrument_gross_notional_usd: String,
    pub max_margin_utilization_ratio: String,
    pub max_loss_per_trade_usd: String,
    pub max_daily_realized_loss_usd: String,
    pub max_drawdown_ratio: String,
    pub max_leverage: String,
    pub allowed_instruments: Vec<String>,
    pub minimum_quality: RiskMinimumQuality,
    pub degraded_mode: RiskDegradedMode,
    pub correlated_clusters: Vec<CorrelatedClusterLimit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PortfolioCandidate {
    pub instrument: String,
    pub direction: PositionDirection,
    pub notional_usd: String,
    pub worst_case_loss_usd: String,
    pub leverage: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExposureAggregate {
    pub key: String,
    pub gross_notional_usd: String,
    pub signed_net_notional_usd: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClusterExposure {
    pub cluster_id: String,
    pub gross_notional_usd: String,
    pub max_gross_notional_usd: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CandidateProjection {
    pub instrument: String,
    pub direction: PositionDirection,
    pub additive_new_risk: bool,
    pub notional_usd: String,
    pub worst_case_loss_usd: String,
    pub leverage: String,
    pub projected_account_gross_notional_usd: String,
    pub projected_instrument_gross_notional_usd: String,
    pub projected_margin_utilization_ratio: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RiskPolicyViolation {
    pub code: &'static str,
    pub scope: String,
    pub observed: String,
    pub limit: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RiskOracleComparison {
    pub schema: &'static str,
    pub oracle_source: &'static str,
    pub oracle_timestamp_ms: String,
    pub local_gross_notional_usd: String,
    pub oracle_gross_notional_usd: String,
    pub gross_notional_residual_usd: String,
    pub local_adjusted_equity_usd: Option<String>,
    pub oracle_adjusted_equity_usd: Option<String>,
    pub adjusted_equity_residual_usd: Option<String>,
    pub consistent: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskPolicyDecision {
    Accepted,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PortfolioRiskAnalysis {
    pub schema: &'static str,
    pub valuation_basis: &'static str,
    pub account: AccountRiskAnalysis,
    pub instrument_exposure: Vec<ExposureAggregate>,
    pub settlement_exposure: Vec<ExposureAggregate>,
    pub correlated_cluster_exposure: Vec<ClusterExposure>,
    pub margin_utilization_ratio: Option<String>,
    pub capital_base_drawdown_ratio: String,
    pub daily_realized_pnl_utc_basis: &'static str,
    pub daily_realized_pnl_utc_day_start_ms: Option<String>,
    pub daily_realized_pnl_utc_day_end_ms: Option<String>,
    pub daily_realized_pnl_utc: Vec<CurrencyAggregate>,
    pub daily_realized_loss_usd_equivalent: Option<String>,
    pub mandate: TradingMandate,
    pub policy: HardRiskPolicy,
    pub candidate: Option<CandidateProjection>,
    pub policy_decision: RiskPolicyDecision,
    pub violations: Vec<RiskPolicyViolation>,
}

struct PositionWork {
    output: PositionRiskAnalysis,
    notional: Decimal,
}

pub fn analyze_account_risk(
    account: &AccountSnapshot,
) -> Result<AccountRiskAnalysis, AnalysisError> {
    match account.account_level.as_str() {
        "2" | "3" | "4" => {}
        other => return Err(AnalysisError::UnsupportedAccountMode(other.to_owned())),
    }
    match account.position_mode.as_str() {
        "net_mode" | "long_short_mode" => {}
        other => return Err(AnalysisError::UnsupportedPositionMode(other.to_owned())),
    }

    let total_equity = decimal("total_equity_usd", &account.balance.total_equity_usd)?;
    let adjusted_equity = optional_decimal(
        "adjusted_equity_usd",
        account.balance.adjusted_equity_usd.as_deref(),
    )?;
    let exchange_gross_notional = optional_non_negative_decimal(
        "exchange_gross_notional_usd",
        account.balance.notional_usd.as_deref(),
    )?;
    let account_imr = optional_non_negative_decimal(
        "initial_margin_requirement_usd",
        account.balance.initial_margin_requirement_usd.as_deref(),
    )?;
    let account_mmr = optional_non_negative_decimal(
        "maintenance_margin_requirement_usd",
        account
            .balance
            .maintenance_margin_requirement_usd
            .as_deref(),
    )?;
    let account_margin_ratio = optional_non_negative_decimal(
        "exchange_margin_ratio",
        account.balance.margin_ratio.as_deref(),
    )?;

    let mut gross = Decimal::ZERO;
    let mut net = Decimal::ZERO;
    let mut work = Vec::new();

    for position in &account.positions {
        let size = decimal("position_contracts", &position.position)?;
        if size == Decimal::ZERO {
            continue;
        }

        match position.instrument_type.as_str() {
            "SWAP" | "FUTURES" => {}
            other => {
                return Err(AnalysisError::UnsupportedPositionType {
                    instrument_id: position.instrument_id.clone(),
                    instrument_type: other.to_owned(),
                });
            }
        }

        let direction = direction_for(position, &account.position_mode, size)?;
        let notional_text = position.notional_usd.as_deref().ok_or_else(|| {
            AnalysisError::MissingPositionNotional(position.instrument_id.clone())
        })?;
        let notional = positive_decimal("position_notional_usd", notional_text)?;

        let signed_notional = match direction {
            PositionDirection::Long => notional,
            PositionDirection::Short => -notional,
        };
        gross += notional;
        net += signed_notional;

        let configured_leverage =
            optional_positive_decimal("configured_leverage", position.leverage.as_deref())?;
        let imr = optional_non_negative_decimal(
            "position_initial_margin_requirement_usd",
            position.initial_margin_requirement.as_deref(),
        )?;
        let mmr = optional_non_negative_decimal(
            "position_maintenance_margin_requirement_usd",
            position.maintenance_margin_requirement.as_deref(),
        )?;
        let margin_ratio = optional_non_negative_decimal(
            "position_exchange_margin_ratio",
            position.margin_ratio.as_deref(),
        )?;
        let mark_price =
            optional_positive_decimal("position_mark_price", position.mark_price.as_deref())?;
        let liquidation_price = optional_positive_decimal(
            "estimated_liquidation_price",
            position.liquidation_price.as_deref(),
        )?;
        let liquidation_distance = estimated_liquidation_distance(
            &position.instrument_id,
            direction,
            mark_price,
            liquidation_price,
        )?;

        work.push(PositionWork {
            output: PositionRiskAnalysis {
                instrument_type: position.instrument_type.clone(),
                instrument_id: position.instrument_id.clone(),
                direction,
                margin_mode: position.margin_mode.clone(),
                position_contracts: size.normalize().to_string(),
                position_notional_usd: notional.normalize().to_string(),
                signed_notional_usd: signed_notional.normalize().to_string(),
                gross_concentration_ratio: String::new(),
                configured_leverage: normalized(configured_leverage),
                initial_margin_requirement_usd: normalized(imr),
                maintenance_margin_requirement_usd: normalized(mmr),
                exchange_margin_ratio: normalized(margin_ratio),
                mark_price: normalized(mark_price),
                estimated_liquidation_price: normalized(liquidation_price),
                estimated_liquidation_distance_ratio: normalized(liquidation_distance),
            },
            notional,
        });
    }

    for item in &mut work {
        item.output.gross_concentration_ratio = if gross > Decimal::ZERO {
            (item.notional / gross).normalize().to_string()
        } else {
            "0".to_owned()
        };
    }

    let gross_to_equity =
        (total_equity > Decimal::ZERO).then(|| (gross / total_equity).normalize().to_string());
    let net_to_equity =
        (total_equity > Decimal::ZERO).then(|| (net / total_equity).normalize().to_string());

    let positions = work.into_iter().map(|item| item.output).collect::<Vec<_>>();

    Ok(AccountRiskAnalysis {
        schema: ACCOUNT_RISK_ANALYSIS_SCHEMA_V1.to_owned(),
        account_generation: account.account_generation.clone(),
        account_source: account.source.clone(),
        account_quality_reason: account.quality_reason.clone(),
        account_level: account.account_level.clone(),
        position_mode: account.position_mode.clone(),
        total_equity_usd: total_equity.normalize().to_string(),
        adjusted_equity_usd: normalized(adjusted_equity),
        exchange_gross_notional_usd: normalized(exchange_gross_notional),
        gross_position_notional_usd: gross.normalize().to_string(),
        directional_net_position_notional_usd: net.normalize().to_string(),
        gross_exposure_to_equity_ratio: gross_to_equity,
        directional_net_exposure_to_equity_ratio: net_to_equity,
        initial_margin_requirement_usd: normalized(account_imr),
        maintenance_margin_requirement_usd: normalized(account_mmr),
        exchange_margin_ratio: normalized(account_margin_ratio),
        source_position_record_count: account.positions.len(),
        open_position_count: positions.len(),
        pending_order_count: account.pending_orders.len(),
        positions,
    })
}

pub fn analyze_portfolio_risk(
    account: &AccountSnapshot,
    ledger: &AccountLedgerSummary,
    mandate: TradingMandate,
    policy: HardRiskPolicy,
    candidate: Option<PortfolioCandidate>,
    account_is_fresh: bool,
) -> Result<PortfolioRiskAnalysis, AnalysisError> {
    let account_risk = analyze_account_risk(account)?;
    let total_equity = decimal("total_equity_usd", &account_risk.total_equity_usd)?;
    let capital_base = positive_decimal("capital_base_usd", &mandate.capital_base_usd)?;
    let margin_utilization = match account_risk.initial_margin_requirement_usd.as_deref() {
        Some(imr) if total_equity > Decimal::ZERO => Some(
            (decimal("initial_margin_requirement_usd", imr)? / total_equity)
                .normalize()
                .to_string(),
        )
        _ => None,
    };
    let drawdown = if total_equity < capital_base {
        ((capital_base - total_equity) / capital_base).normalize()
    } else {
        Decimal::ZERO
    };

    let mut instrument = BTreeMap::<String, (Decimal, Decimal)>::new();
    let mut settlement = BTreeMap::<String, (Decimal, Decimal)>::new();
    for position in &account_risk.positions {
        let gross = positive_decimal("position_notional_usd", &position.position_notional_usd)?;
        let signed = decimal("signed_notional_usd", &position.signed_notional_usd)?;
        let entry = instrument
            .entry(position.instrument_id.clone())
            .or_default();
        entry.0 += gross;
        entry.1 += signed;

        let source = account.positions.iter().find(|row| {
            row.instrument_id == position.instrument_id
                && match position.direction {
                    PositionDirection::Long => {
                        row.position_side == "long"
                            || (row.position_side == "net" && !row.position.starts_with('-'))
                    }
                    PositionDirection::Short => {
                        row.position_side == "short"
                            || (row.position_side == "net" && row.position.starts_with('-'))
                    }
                }
        });
        let settle = source
            .and_then(|row| row.margin_currency.clone())
            .unwrap_or_else(|| "UNKNOWN".to_owned());
        let entry = settlement.entry(settle).or_default();
        entry.0 += gross;
        entry.1 += signed;
    }

    let instrument_exposure = finish_exposure(instrument);
    let settlement_exposure = finish_exposure(settlement);

    let mut cluster_exposure = Vec::new();
    for cluster in &policy.correlated_clusters {
        let members = cluster.instruments.iter().collect::<BTreeSet<_>>();
        let gross = instrument_exposure
            .iter()
            .filter(|row| members.contains(&row.key))
            .try_fold(Decimal::ZERO, |acc, row| {
                Ok::<_, AnalysisError>(acc + decimal("cluster_gross", &row.gross_notional_usd)?)
            })?;
        cluster_exposure.push(ClusterExposure {
            cluster_id: cluster.id.clone(),
            gross_notional_usd: gross.normalize().to_string(),
            max_gross_notional_usd: cluster.max_gross_notional_usd.clone(),
        });
    }

    let daily_loss = usd_equivalent_daily_loss(&ledger.daily_realized_pnl_utc)?;
    let mut violations = Vec::new();

    if matches!(policy.minimum_quality, RiskMinimumQuality::Fresh) && !account_is_fresh {
        violations.push(RiskPolicyViolation {
            code: "MINIMUM_DATA_QUALITY",
            scope: "account".to_owned(),
            observed: "degraded".to_owned(),
            limit: "fresh".to_owned(),
        });
    }
    if !account_is_fresh && matches!(policy.degraded_mode, RiskDegradedMode::Reject) {
        violations.push(RiskPolicyViolation {
            code: "DEGRADED_MODE_REJECT",
            scope: "account".to_owned(),
            observed: "degraded".to_owned(),
            limit: "reject".to_owned(),
        });
    }

    let gross = decimal(
        "gross_position_notional_usd",
        &account_risk.gross_position_notional_usd,
    )?;
    compare_limit(
        &mut violations,
        "MAX_ACCOUNT_GROSS_NOTIONAL",
        "account",
        gross,
        &policy.max_account_gross_notional_usd,
    )?;
    if let Some(utilization) = margin_utilization.as_deref() {
        compare_limit(
            &mut violations,
            "MAX_MARGIN_UTILIZATION",
            "account",
            decimal("margin_utilization_ratio", utilization)?,
            &policy.max_margin_utilization_ratio,
        )?;
    }
    compare_limit(
        &mut violations,
        "MAX_DRAWDOWN",
        "capital_base",
        drawdown,
        &policy.max_drawdown_ratio,
    )?;
    if let Some(loss) = daily_loss {
        compare_limit(&mut violations, "MAX_DAILY_REALIZED_LOSS", "utc_day", loss, &policy.max_daily_realized_loss_usd)?;
    }

    for row in &instrument_exposure {
        compare_limit(
            &mut violations,
            "MAX_INSTRUMENT_GROSS_NOTIONAL",
            &row.key,
            decimal("instrument_gross", &row.gross_notional_usd)?,
            &policy.max_instrument_gross_notional_usd,
        )?;
        if !policy.allowed_instruments.is_empty() && !policy.allowed_instruments.contains(&row.key)
        {
            violations.push(RiskPolicyViolation {
                code: "INSTRUMENT_NOT_ALLOWED",
                scope: row.key.clone(),
                observed: "present".to_owned(),
                limit: "allowed_instruments".to_owned(),
            });
        }
    }
    for row in &cluster_exposure {
        compare_limit(
            &mut violations,
            "MAX_CORRELATED_CLUSTER_GROSS_NOTIONAL",
            &row.cluster_id,
            decimal("cluster_gross", &row.gross_notional_usd)?,
            &row.max_gross_notional_usd,
        )?;
    }
    for position in &account_risk.positions {
        if let Some(leverage) = position.configured_leverage.as_deref() {
            compare_limit(
                &mut violations,
                "MAX_LEVERAGE",
                &position.instrument_id,
                decimal("configured_leverage", leverage)?,
                &policy.max_leverage,
            )?;
        }
    }

    let candidate_projection = if let Some(candidate) = candidate {
        if !mandate.allowed_instruments.is_empty()
            && !mandate.allowed_instruments.contains(&candidate.instrument)
        {
            violations.push(RiskPolicyViolation {
                code: "MANDATE_INSTRUMENT_NOT_ALLOWED",
                scope: candidate.instrument.clone(),
                observed: "candidate".to_owned(),
                limit: "mandate.allowed_instruments".to_owned(),
            });
        }
        if !policy.allowed_instruments.is_empty()
            && !policy.allowed_instruments.contains(&candidate.instrument)
        {
            violations.push(RiskPolicyViolation {
                code: "INSTRUMENT_NOT_ALLOWED",
                scope: candidate.instrument.clone(),
                observed: "candidate".to_owned(),
                limit: "policy.allowed_instruments".to_owned(),
            });
        }
        let notional = positive_decimal("candidate_notional_usd", &candidate.notional_usd)?;
        let candidate_loss = decimal(
            "candidate_worst_case_loss_usd",
            &candidate.worst_case_loss_usd,
        )?
        .abs();
        let leverage = positive_decimal("candidate_leverage", &candidate.leverage)?;
        compare_limit(
            &mut violations,
            "MAX_LOSS_PER_TRADE",
            &candidate.instrument,
            candidate_loss,
            &policy.max_loss_per_trade_usd,
        )?;
        compare_limit(
            &mut violations,
            "MAX_LEVERAGE",
            &candidate.instrument,
            leverage,
            &policy.max_leverage,
        )?;
        compare_limit(
            &mut violations,
            "MANDATE_LEVERAGE_CEILING",
            &candidate.instrument,
            leverage,
            &mandate.leverage_ceiling,
        )?;

        let current_instrument = instrument_exposure.iter()
            .find(|row| row.key == candidate.instrument)
            .map(|row| decimal("instrument_gross", &row.gross_notional_usd))
            .transpose()?
            .unwrap_or(Decimal::ZERO);
        let projected_gross = gross + notional;
        let projected_instrument = current_instrument + notional;
        compare_limit(
            &mut violations,
            "MAX_ACCOUNT_GROSS_NOTIONAL_PROJECTED",
            "account",
            projected_gross,
            &policy.max_account_gross_notional_usd,
        )?;
        compare_limit(
            &mut violations,
            "MAX_INSTRUMENT_GROSS_NOTIONAL_PROJECTED",
            &candidate.instrument,
            projected_instrument,
            &policy.max_instrument_gross_notional_usd,
        )?;

        let projected_margin = if total_equity > Decimal::ZERO {
            let current_imr = account_risk
                .initial_margin_requirement_usd
                .as_deref()
                .map(|value| decimal("initial_margin_requirement_usd", value))
                .transpose()?
                .unwrap_or(Decimal::ZERO);
            Some(
                ((current_imr + notional / leverage) / total_equity)
                    .normalize()
                    .to_string(),
            )
        } else {
            None
        };
        if let Some(value) = projected_margin.as_deref() {
            compare_limit(
                &mut violations,
                "MAX_MARGIN_UTILIZATION_PROJECTED",
                "account",
                decimal("projected_margin_utilization", value)?,
                &policy.max_margin_utilization_ratio,
            )?;
        }

        Some(CandidateProjection {
            instrument: candidate.instrument,
            direction: candidate.direction,
            additive_new_risk: true,
            notional_usd: notional.normalize().to_string(),
            worst_case_loss_usd: candidate_loss.normalize().to_string(),
            leverage: leverage.normalize().to_string(),
            projected_account_gross_notional_usd: projected_gross.normalize().to_string(),
            projected_instrument_gross_notional_usd: projected_instrument.normalize().to_string(),
            projected_margin_utilization_ratio: projected_margin,
        })
    } else {
        None
    };

    let policy_decision = if violations.is_empty() {
        RiskPolicyDecision::Accepted
    } else {
        RiskPolicyDecision::Rejected
    };

    Ok(PortfolioRiskAnalysis {
        schema: PORTFOLIO_RISK_ANALYSIS_SCHEMA_V2,
        valuation_basis: "exchange position notionalUsd / mark-price semantics; candidate notional is explicit USD input",
        account: account_risk,
        instrument_exposure,
        settlement_exposure,
        correlated_cluster_exposure: cluster_exposure,
        margin_utilization_ratio: margin_utilization,
        capital_base_drawdown_ratio: drawdown.normalize().to_string(),
        daily_realized_pnl_utc_basis: ledger.daily_realized_pnl_utc_basis,
        daily_realized_pnl_utc_day_start_ms: ledger.daily_realized_pnl_utc_day_start_ms.clone(),
        daily_realized_pnl_utc_day_end_ms: ledger.daily_realized_pnl_utc_day_end_ms.clone(),
        daily_realized_pnl_utc: ledger.daily_realized_pnl_utc.clone(),
        daily_realized_loss_usd_equivalent: daily_loss.map(|value| value.normalize().to_string()),
        mandate,
        policy,
        candidate: candidate_projection,
        policy_decision,
        violations,
    })
}

pub fn compare_account_position_risk_oracle(
    local: &PortfolioRiskAnalysis,
    oracle_timestamp_ms: &str,
    oracle_adjusted_equity_usd: Option<&str>,
    oracle_position_notionals_usd: &[&str],
) -> Result<RiskOracleComparison, AnalysisError> {
    let local_gross = decimal(
        "local_gross_notional_usd",
        &local.account.gross_position_notional_usd,
    )?;
    let oracle_gross = oracle_position_notionals_usd.iter().try_fold(
        Decimal::ZERO,
        |acc, value| -> Result<Decimal, AnalysisError> {
            if value.trim().is_empty() {
                return Ok(acc);
            }
            Ok(acc + decimal("oracle_position_notional_usd", value)?.abs())
        },
    )?;
    let gross_residual = local_gross - oracle_gross;

    let local_adjusted = local
        .account
        .adjusted_equity_usd
        .as_deref()
        .map(|value| decimal("local_adjusted_equity_usd", value))
        .transpose()?;
    let oracle_adjusted = oracle_adjusted_equity_usd
        .filter(|value| !value.trim().is_empty())
        .map(|value| decimal("oracle_adjusted_equity_usd", value))
        .transpose()?;
    let adjusted_residual = match (local_adjusted, oracle_adjusted) {
        (Some(local_value), Some(oracle_value)) => Some(local_value - oracle_value),
        _ => None,
    };
    let consistent = gross_residual.is_zero()
        && adjusted_residual.map(|value| value.is_zero()).unwrap_or(true);

    Ok(RiskOracleComparison {
        schema: "okx.risk-oracle-comparison/v1",
        oracle_source: "GET /api/v5/account/account-position-risk",
        oracle_timestamp_ms: oracle_timestamp_ms.to_owned(),
        local_gross_notional_usd: local_gross.normalize().to_string(),
        oracle_gross_notional_usd: oracle_gross.normalize().to_string(),
        gross_notional_residual_usd: gross_residual.normalize().to_string(),
        local_adjusted_equity_usd: local_adjusted.map(|value| value.normalize().to_string()),
        oracle_adjusted_equity_usd: oracle_adjusted.map(|value| value.normalize().to_string()),
        adjusted_equity_residual_usd: adjusted_residual.map(|value| value.normalize().to_string()),
        consistent,
    })
}

fn finish_exposure(values: BTreeMap<String, (Decimal, Decimal)>) -> Vec<ExposureAggregate> {
    values
        .into_iter()
        .map(|(key, (gross, net))| ExposureAggregate {
            key,
            gross_notional_usd: gross.normalize().to_string(),
            signed_net_notional_usd: net.normalize().to_string(),
        })
        .collect()
}

fn usd_equivalent_daily_loss(values: &[CurrencyAggregate]) -> Result<Option<Decimal>, AnalysisError> {
    let mut net = Decimal::ZERO;
    for row in values {
        if !matches!(row.currency.as_str(), "USD" | "USDT" | "USDC" | "USDG") {
            return Ok(None);
        }
        net += decimal("daily_realized_pnl", &row.amount)?;
    }
    Ok(Some(if net < Decimal::ZERO { -net } else { Decimal::ZERO }))
}

fn compare_limit(
    violations: &mut Vec<RiskPolicyViolation>,
    code: &'static str,
    scope: &str,
    observed: Decimal,
    limit: &str,
) -> Result<(), AnalysisError> {
    let limit_value = non_negative_decimal("risk_limit", limit)?;
    if observed > limit_value {
        violations.push(RiskPolicyViolation {
            code,
            scope: scope.to_owned(),
            observed: observed.normalize().to_string(),
            limit: limit_value.normalize().to_string(),
        });
    }
    Ok(())
}

fn direction_for(
    position: &AccountPositionState,
    account_position_mode: &str,
    size: Decimal,
) -> Result<PositionDirection, AnalysisError> {
    match (account_position_mode, position.position_side.as_str()) {
        ("net_mode", "net") => {
            if size > Decimal::ZERO {
                Ok(PositionDirection::Long)
            } else {
                Ok(PositionDirection::Short)
            }
        }
        ("long_short_mode", "long") if size > Decimal::ZERO => Ok(PositionDirection::Long),
        ("long_short_mode", "short") if size > Decimal::ZERO => Ok(PositionDirection::Short),
        ("long_short_mode", "long" | "short") => {
            Err(AnalysisError::InconsistentPositionDirection {
                instrument_id: position.instrument_id.clone(),
                position_side: position.position_side.clone(),
                position: position.position.clone(),
            })
        }
        (_, side) => Err(AnalysisError::UnsupportedPositionSide {
            instrument_id: position.instrument_id.clone(),
            position_side: side.to_owned(),
        }),
    }
}

fn estimated_liquidation_distance(
    instrument_id: &str,
    direction: PositionDirection,
    mark_price: Option<Decimal>,
    liquidation_price: Option<Decimal>,
) -> Result<Option<Decimal>, AnalysisError> {
    let (Some(mark), Some(liquidation)) = (mark_price, liquidation_price) else {
        return Ok(None);
    };

    let adverse_distance = match direction {
        PositionDirection::Long => mark - liquidation,
        PositionDirection::Short => liquidation - mark,
    };
    if adverse_distance < Decimal::ZERO {
        return Err(AnalysisError::InconsistentLiquidationPrice(
            instrument_id.to_owned(),
        ));
    }

    Ok(Some(adverse_distance / mark))
}

fn optional_decimal(
    field: &'static str,
    value: Option<&str>,
) -> Result<Option<Decimal>, AnalysisError> {
    value.map(|value| decimal(field, value)).transpose()
}

fn optional_positive_decimal(
    field: &'static str,
    value: Option<&str>,
) -> Result<Option<Decimal>, AnalysisError> {
    value
        .map(|value| positive_decimal(field, value))
        .transpose()
}

fn non_negative_decimal(field: &'static str, value: &str) -> Result<Decimal, AnalysisError> {
    let value = decimal(field, value)?;
    if value < Decimal::ZERO {
        return Err(AnalysisError::Negative(field));
    }
    Ok(value)
}

fn optional_non_negative_decimal(
    field: &'static str,
    value: Option<&str>,
) -> Result<Option<Decimal>, AnalysisError> {
    value
        .map(|value| {
            let value = decimal(field, value)?;
            if value < Decimal::ZERO {
                return Err(AnalysisError::Negative(field));
            }
            Ok(value)
        })
        .transpose()
}

fn normalized(value: Option<Decimal>) -> Option<String> {
    value.map(|value| value.normalize().to_string())
}

#[cfg(test)]
mod tests {
    use okx_observation::{
        AccountAuthorityEvidence, AccountBalanceState, AccountLedgerSummary, AccountPositionState,
        AccountSnapshot, CurrencyAggregate,
    };

    use super::*;

    fn account(position_mode: &str, positions: Vec<AccountPositionState>) -> AccountSnapshot {
        AccountSnapshot {
            schema: "okx.account-snapshot/v2".to_owned(),
            source: "okx_private_rest_plus_ws".to_owned(),
            source_received_at: "2026-09-27T20:00:00Z".to_owned(),
            account_generation: "sha256:test".to_owned(),
            quality_reason: "M4_PRIVATE_REST_WS_CONVERGED".to_owned(),
            private_ws_connected: true,
            private_ws_generation: Some(7),
            private_ws_connection_fingerprint: Some("fingerprint".to_owned()),
            private_ws_last_inbound_ms: Some(1),
            private_ws_events_applied: Some(1),
            account_level: "3".to_owned(),
            position_mode: position_mode.to_owned(),
            account_type: "0".to_owned(),
            account_uid_fingerprint: "uid".to_owned(),
            api_key_permissions: vec!["read_only".to_owned()],
            balance: AccountBalanceState {
                total_equity_usd: "500".to_owned(),
                adjusted_equity_usd: Some("480".to_owned()),
                isolated_equity_usd: None,
                initial_margin_requirement_usd: Some("100".to_owned()),
                maintenance_margin_requirement_usd: Some("50".to_owned()),
                margin_ratio: Some("9.6".to_owned()),
                notional_usd: Some("1000".to_owned()),
                update_time_ms: Some("1".to_owned()),
                details: Vec::new(),
            },
            positions,
            pending_orders: Vec::new(),
        }
    }

    fn position(
        instrument_id: &str,
        instrument_type: &str,
        side: &str,
        size: &str,
        notional: Option<&str>,
        mark: Option<&str>,
        liquidation: Option<&str>,
    ) -> AccountPositionState {
        AccountPositionState {
            instrument_type: instrument_type.to_owned(),
            instrument_id: instrument_id.to_owned(),
            position: size.to_owned(),
            position_side: side.to_owned(),
            margin_mode: "cross".to_owned(),
            average_price: Some("90".to_owned()),
            mark_price: mark.map(ToOwned::to_owned),
            liquidation_price: liquidation.map(ToOwned::to_owned),
            unrealized_pnl: Some("0".to_owned()),
            unrealized_pnl_ratio: Some("0".to_owned()),
            leverage: Some("5".to_owned()),
            margin: None,
            initial_margin_requirement: Some("10".to_owned()),
            maintenance_margin_requirement: Some("5".to_owned()),
            margin_ratio: Some("10".to_owned()),
            notional_usd: notional.map(ToOwned::to_owned),
            margin_currency: Some("USDT".to_owned()),
            creation_time_ms: Some("1".to_owned()),
            update_time_ms: Some("2".to_owned()),
        }
    }

    #[test]
    fn long_short_mode_aggregates_gross_net_concentration_and_liquidation_distance() {
        let snapshot = account(
            "long_short_mode",
            vec![
                position(
                    "BTC-USDT-SWAP",
                    "SWAP",
                    "long",
                    "2",
                    Some("600"),
                    Some("100"),
                    Some("80"),
                ),
                position(
                    "ETH-USDT-SWAP",
                    "SWAP",
                    "short",
                    "3",
                    Some("400"),
                    Some("100"),
                    Some("125"),
                ),
            ],
        );

        let risk = analyze_account_risk(&snapshot).expect("risk");

        assert_eq!(risk.gross_position_notional_usd, "1000");
        assert_eq!(risk.directional_net_position_notional_usd, "200");
        assert_eq!(risk.gross_exposure_to_equity_ratio.as_deref(), Some("2"));
        assert_eq!(
            risk.directional_net_exposure_to_equity_ratio.as_deref(),
            Some("0.4")
        );
        assert_eq!(risk.positions[0].gross_concentration_ratio, "0.6");
        assert_eq!(risk.positions[1].gross_concentration_ratio, "0.4");
        assert_eq!(
            risk.positions[0]
                .estimated_liquidation_distance_ratio
                .as_deref(),
            Some("0.2")
        );
        assert_eq!(
            risk.positions[1]
                .estimated_liquidation_distance_ratio
                .as_deref(),
            Some("0.25")
        );
    }

    #[test]
    fn net_mode_uses_position_sign_for_direction() {
        let snapshot = account(
            "net_mode",
            vec![position(
                "DOGE-USDT-SWAP",
                "SWAP",
                "net",
                "-4",
                Some("300"),
                Some("0.2"),
                Some("0.3"),
            )],
        );

        let risk = analyze_account_risk(&snapshot).expect("risk");

        assert_eq!(risk.positions[0].direction, PositionDirection::Short);
        assert_eq!(risk.directional_net_position_notional_usd, "-300");
        assert_eq!(
            risk.positions[0]
                .estimated_liquidation_distance_ratio
                .as_deref(),
            Some("0.5")
        );
    }

    #[test]
    fn unsupported_non_zero_position_type_fails_closed() {
        let snapshot = account(
            "net_mode",
            vec![position(
                "BTC-USD-OPTION",
                "OPTION",
                "net",
                "1",
                Some("100"),
                Some("10"),
                None,
            )],
        );

        assert!(matches!(
            analyze_account_risk(&snapshot),
            Err(AnalysisError::UnsupportedPositionType { .. })
        ));
    }

    #[test]
    fn missing_notional_for_non_zero_position_fails_closed() {
        let snapshot = account(
            "net_mode",
            vec![position(
                "DOGE-USDT-SWAP",
                "SWAP",
                "net",
                "1",
                None,
                Some("0.2"),
                None,
            )],
        );

        assert!(matches!(
            analyze_account_risk(&snapshot),
            Err(AnalysisError::MissingPositionNotional(_))
        ));
    }

    fn ledger(daily_amount: &str) -> AccountLedgerSummary {
        AccountLedgerSummary {
            schema: "okx.account-ledger-summary/v1",
            source_received_at: "2026-10-03T00:00:01Z".to_owned(),
            current_account_as_of_ms: Some("1790985600000".to_owned()),
            account_generation: "sha256:test".to_owned(),
            authority: AccountAuthorityEvidence {
                scope: "authenticated_account_only",
                account_type: "0".to_owned(),
                is_subaccount: true,
                account_uid_fingerprint: "uid".to_owned(),
                main_account_uid_fingerprint: Some("main".to_owned()),
                api_key_permissions: vec!["read_only".to_owned()],
                multi_account_inventory_complete: false,
            },
            total_equity_usd: "500".to_owned(),
            trading_equity_detail_usd_sum: "500".to_owned(),
            trading_equity_residual_usd: "0".to_owned(),
            funding_balances: Vec::new(),
            open_positions: 0,
            pending_orders: 0,
            current_unrealized_pnl: Vec::new(),
            history_coverage: Vec::new(),
            realized_pnl_basis: "positions-history.realizedPnl",
            realized_pnl: Vec::new(),
            daily_realized_pnl_utc_basis: "positions-history.realizedPnl filtered by UTC day",
            daily_realized_pnl_utc_day_start_ms: Some("1790985600000".to_owned()),
            daily_realized_pnl_utc_day_end_ms: Some("1791072000000".to_owned()),
            daily_realized_pnl_utc: if daily_amount == "0" {
                Vec::new()
            } else {
                vec![CurrencyAggregate {
                    currency: "USDT".to_owned(),
                    amount: daily_amount.to_owned(),
                    events: 1,
                }]
            },
            trade_fee_basis: "fills-history.fee",
            trade_fees: Vec::new(),
            funding_basis: "bills-archive funding",
            funding: Vec::new(),
            position_pnl_identity_rows_checked: 0,
            fill_order_links_checked: 0,
            fill_order_links_unresolved_due_to_truncation: 0,
        }
    }

    fn mandate() -> TradingMandate {
        TradingMandate {
            schema: TRADING_MANDATE_SCHEMA_V1,
            version: "test-mandate/v1".to_owned(),
            capital_base_usd: "500".to_owned(),
            decision_horizon_hours: 24,
            benchmark: None,
            allowed_instruments: vec![
                "BTC-USDT-SWAP".to_owned(),
                "ETH-USDT-SWAP".to_owned(),
            ],
            max_drawdown_ratio: "0.5".to_owned(),
            leverage_ceiling: "10".to_owned(),
            minimum_liquidity_notional_usd: "0".to_owned(),
            max_turnover_ratio: "10".to_owned(),
        }
    }

    fn policy() -> HardRiskPolicy {
        HardRiskPolicy {
            schema: HARD_RISK_POLICY_SCHEMA_V1,
            version: "test-policy/v1".to_owned(),
            max_account_gross_notional_usd: "2000".to_owned(),
            max_instrument_gross_notional_usd: "1200".to_owned(),
            max_margin_utilization_ratio: "0.5".to_owned(),
            max_loss_per_trade_usd: "100".to_owned(),
            max_daily_realized_loss_usd: "100".to_owned(),
            max_drawdown_ratio: "0.5".to_owned(),
            max_leverage: "10".to_owned(),
            allowed_instruments: vec![
                "BTC-USDT-SWAP".to_owned(),
                "ETH-USDT-SWAP".to_owned(),
            ],
            minimum_quality: RiskMinimumQuality::Fresh,
            degraded_mode: RiskDegradedMode::Reject,
            correlated_clusters: vec![CorrelatedClusterLimit {
                id: "majors".to_owned(),
                instruments: vec![
                    "BTC-USDT-SWAP".to_owned(),
                    "ETH-USDT-SWAP".to_owned(),
                ],
                max_gross_notional_usd: "1500".to_owned(),
            }],
        }
    }

    #[test]
    fn hedge_mode_same_instrument_long_short_preserves_gross_and_offsets_net() {
        let snapshot = account(
            "long_short_mode",
            vec![
                position(
                    "BTC-USDT-SWAP",
                    "SWAP",
                    "long",
                    "1",
                    Some("600"),
                    Some("100"),
                    Some("80"),
                ),
                position(
                    "BTC-USDT-SWAP",
                    "SWAP",
                    "short",
                    "1",
                    Some("400"),
                    Some("100"),
                    Some("125"),
                ),
            ],
        );
        let result = analyze_portfolio_risk(
            &snapshot,
            &ledger("0"),
            mandate(),
            policy(),
            None,
            true,
        )
        .expect("portfolio risk");

        assert_eq!(result.account.gross_position_notional_usd, "1000");
        assert_eq!(result.account.directional_net_position_notional_usd, "200");
        assert_eq!(result.instrument_exposure.len(), 1);
        assert_eq!(result.instrument_exposure[0].gross_notional_usd, "1000");
        assert_eq!(result.instrument_exposure[0].signed_net_notional_usd, "200");
        assert_eq!(result.policy_decision, RiskPolicyDecision::Accepted);
    }

    #[test]
    fn candidate_projection_and_daily_loss_fail_closed_with_typed_violations() {
        let snapshot = account("long_short_mode", Vec::new());
        let mut strict = policy();
        strict.max_daily_realized_loss_usd = "20".to_owned();
        strict.max_loss_per_trade_usd = "25".to_owned();
        strict.max_account_gross_notional_usd = "100".to_owned();
        let result = analyze_portfolio_risk(
            &snapshot,
            &ledger("-30"),
            mandate(),
            strict,
            Some(PortfolioCandidate {
                instrument: "BTC-USDT-SWAP".to_owned(),
                direction: PositionDirection::Long,
                notional_usd: "150".to_owned(),
                worst_case_loss_usd: "40".to_owned(),
                leverage: "5".to_owned(),
            }),
            true,
        )
        .expect("portfolio risk");

        assert_eq!(result.policy_decision, RiskPolicyDecision::Rejected);
        let codes = result
            .violations
            .iter()
            .map(|violation| violation.code)
            .collect::<BTreeSet<_>>();
        assert!(codes.contains("MAX_DAILY_REALIZED_LOSS"));
        assert!(codes.contains("MAX_LOSS_PER_TRADE"));
        assert!(codes.contains("MAX_ACCOUNT_GROSS_NOTIONAL_PROJECTED"));
        assert_eq!(
            result
                .candidate
                .as_ref()
                .expect("candidate")
                .projected_account_gross_notional_usd,
            "150"
        );
    }

    #[test]
    fn degraded_account_is_rejected_when_policy_requires_fresh() {
        let snapshot = account("long_short_mode", Vec::new());
        let result = analyze_portfolio_risk(
            &snapshot,
            &ledger("0"),
            mandate(),
            policy(),
            None,
            false,
        )
        .expect("portfolio risk");

        assert_eq!(result.policy_decision, RiskPolicyDecision::Rejected);
        assert!(
            result
                .violations
                .iter()
                .any(|violation| violation.code == "MINIMUM_DATA_QUALITY")
        );
        assert!(
            result
                .violations
                .iter()
                .any(|violation| violation.code == "DEGRADED_MODE_REJECT")
        );
    }

    #[test]
    fn correlated_cluster_limit_uses_gross_not_net_exposure() {
        let snapshot = account(
            "long_short_mode",
            vec![
                position(
                    "BTC-USDT-SWAP",
                    "SWAP",
                    "long",
                    "1",
                    Some("800"),
                    Some("100"),
                    Some("80"),
                ),
                position(
                    "ETH-USDT-SWAP",
                    "SWAP",
                    "short",
                    "1",
                    Some("800"),
                    Some("100"),
                    Some("125"),
                ),
            ],
        );
        let mut bounded = policy();
        bounded.max_account_gross_notional_usd = "2000".to_owned();
        bounded.correlated_clusters[0].max_gross_notional_usd = "1500".to_owned();

        let result = analyze_portfolio_risk(
            &snapshot,
            &ledger("0"),
            mandate(),
            bounded,
            None,
            true,
        )
        .expect("portfolio risk");

        assert!(
            result.violations.iter().any(|violation| {
                violation.code == "MAX_CORRELATED_CLUSTER_GROSS_NOTIONAL"
                    && violation.observed == "1600"
            })
        );
    }

    #[test]
    fn account_position_risk_oracle_residual_is_explicit() {
        let snapshot = account(
            "long_short_mode",
            vec![position(
                "BTC-USDT-SWAP",
                "SWAP",
                "long",
                "1",
                Some("600"),
                Some("100"),
                Some("80"),
            )],
        );
        let local = analyze_portfolio_risk(
            &snapshot,
            &ledger("0"),
            mandate(),
            policy(),
            None,
            true,
        )
        .expect("local");

        let exact = compare_account_position_risk_oracle(
            &local,
            "1790985600000",
            Some("480"),
            &["600"],
        )
        .expect("oracle");
        assert!(exact.consistent);
        assert_eq!(exact.gross_notional_residual_usd, "0");

        let mismatch = compare_account_position_risk_oracle(
            &local,
            "1790985600000",
            Some("479"),
            &["590"],
        )
        .expect("oracle mismatch");
        assert!(!mismatch.consistent);
        assert_eq!(mismatch.gross_notional_residual_usd, "10");
        assert_eq!(mismatch.adjusted_equity_residual_usd.as_deref(), Some("1"));
    }

    #[test]
    fn zero_size_funded_record_is_allowed_without_notional() {
        let snapshot = account(
            "net_mode",
            vec![position(
                "DOGE-USDT-SWAP",
                "SWAP",
                "net",
                "0",
                None,
                None,
                None,
            )],
        );

        let risk = analyze_account_risk(&snapshot).expect("risk");
        assert_eq!(risk.source_position_record_count, 1);
        assert_eq!(risk.open_position_count, 0);
        assert_eq!(risk.gross_position_notional_usd, "0");
        assert!(risk.positions.is_empty());
    }
}

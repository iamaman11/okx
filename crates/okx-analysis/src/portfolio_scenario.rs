//! Pure virtual portfolio stress calculations over validated caller-supplied data.
//! Wire representations are converted by Research Service, not by this crate.
use rust_decimal::Decimal;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScenarioSide {
    Long,
    Short,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScenarioPosition {
    pub instrument: String,
    pub side: ScenarioSide,
    pub notional_usd: Decimal,
    pub margin_usd: Decimal,
    pub price_shock_bps: i32,
    pub volatility_bps: u32,
    pub liquidity_cost_bps: u32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScenarioCorrelation {
    pub left: usize,
    pub right: usize,
    pub rho: f64,
}

/// Once constructed, a scenario has valid bounds and a complete correlation grid.
#[derive(Debug, Clone, PartialEq)]
pub struct PortfolioScenarioInput {
    equity_usd: Decimal,
    positions: Vec<ScenarioPosition>,
    correlations: Vec<ScenarioCorrelation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ScenarioError {
    #[error("INVALID_PORTFOLIO_SCENARIO")]
    InvalidInput,
    #[error("SCENARIO_OVERFLOW")]
    Overflow,
    #[error("CORRELATION_NOT_PSD")]
    CorrelationNotPsd,
}

impl PortfolioScenarioInput {
    pub fn new(
        equity_usd: Decimal,
        positions: Vec<ScenarioPosition>,
        correlations: Vec<ScenarioCorrelation>,
    ) -> Result<Self, ScenarioError> {
        let size = positions.len();
        if equity_usd <= Decimal::ZERO
            || !(1..=8).contains(&size)
            || correlations.len() != size * (size - 1) / 2
        {
            return Err(ScenarioError::InvalidInput);
        }
        for position in &positions {
            if position.instrument.is_empty()
                || position.instrument.len() > 128
                || !position
                    .instrument
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b':'))
                || position.notional_usd <= Decimal::ZERO
                || position.margin_usd <= Decimal::ZERO
                || !(-10_000..=10_000).contains(&position.price_shock_bps)
                || position.volatility_bps > 10_000
                || position.liquidity_cost_bps > 10_000
            {
                return Err(ScenarioError::InvalidInput);
            }
        }
        let mut pairs = BTreeSet::new();
        for correlation in &correlations {
            if correlation.left >= correlation.right
                || correlation.right >= size
                || !correlation.rho.is_finite()
                || !(-1.0..=1.0).contains(&correlation.rho)
                || !pairs.insert((correlation.left, correlation.right))
            {
                return Err(ScenarioError::InvalidInput);
            }
        }
        Ok(Self {
            equity_usd,
            positions,
            correlations,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScenarioPositionResult {
    pub instrument: String,
    pub side: ScenarioSide,
    pub price_pnl_usd: Decimal,
    pub liquidity_loss_usd: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PortfolioScenarioResult {
    pub gross_notional_usd: Decimal,
    pub margin_usd: Decimal,
    pub stress_price_pnl_usd: Decimal,
    pub liquidity_loss_usd: Decimal,
    pub net_stress_pnl_usd: Decimal,
    pub stressed_equity_usd: Decimal,
    pub drawdown_ratio: Decimal,
    pub gross_leverage_ratio: Decimal,
    pub correlated_volatility_usd: f64,
    pub positions: Vec<ScenarioPositionResult>,
}

pub fn evaluate(input: &PortfolioScenarioInput) -> Result<PortfolioScenarioResult, ScenarioError> {
    let (mut gross, mut margin, mut move_pnl, mut liquidity) =
        (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);
    let mut vols = Vec::new();
    let mut positions = Vec::new();
    for position in &input.positions {
        let notional = position.notional_usd;
        let sign: i64 = if position.side == ScenarioSide::Long {
            1
        } else {
            -1
        };
        let pnl = notional
            .checked_mul(Decimal::from(sign * i64::from(position.price_shock_bps)))
            .and_then(|value| value.checked_div(Decimal::from(10_000)))
            .ok_or(ScenarioError::Overflow)?;
        let impact = notional
            .checked_mul(Decimal::from(position.liquidity_cost_bps))
            .and_then(|value| value.checked_div(Decimal::from(10_000)))
            .ok_or(ScenarioError::Overflow)?;
        let vol = notional
            .checked_mul(Decimal::from(position.volatility_bps))
            .and_then(|value| value.checked_div(Decimal::from(10_000)))
            .ok_or(ScenarioError::Overflow)?;
        gross = gross.checked_add(notional).ok_or(ScenarioError::Overflow)?;
        margin = margin
            .checked_add(position.margin_usd)
            .ok_or(ScenarioError::Overflow)?;
        move_pnl = move_pnl.checked_add(pnl).ok_or(ScenarioError::Overflow)?;
        liquidity = liquidity
            .checked_add(impact)
            .ok_or(ScenarioError::Overflow)?;
        vols.push(
            vol.to_string()
                .parse::<f64>()
                .map_err(|_| ScenarioError::Overflow)?,
        );
        positions.push(ScenarioPositionResult {
            instrument: position.instrument.clone(),
            side: position.side,
            price_pnl_usd: pnl,
            liquidity_loss_usd: impact,
        });
    }
    let size = vols.len();
    let mut correlation = vec![vec![0.0; size]; size];
    for (index, row) in correlation.iter_mut().enumerate() {
        row[index] = 1.0;
    }
    for pair in &input.correlations {
        correlation[pair.left][pair.right] = pair.rho;
        correlation[pair.right][pair.left] = pair.rho;
    }
    let mut root = vec![vec![0.0_f64; size]; size];
    for i in 0..size {
        for j in 0..=i {
            let mut value = correlation[i][j];
            for (left, right) in root[i][..j].iter().zip(root[j][..j].iter()) {
                value -= left * right;
            }
            if i == j {
                if value < -1e-8 {
                    return Err(ScenarioError::CorrelationNotPsd);
                }
                root[i][j] = value.max(0.0).sqrt();
            } else if root[j][j] > 1e-10 {
                root[i][j] = value / root[j][j];
            } else if value.abs() > 1e-8 {
                return Err(ScenarioError::CorrelationNotPsd);
            }
        }
    }
    let mut variance = 0.0;
    for i in 0..size {
        for j in 0..size {
            variance += vols[i] * vols[j] * correlation[i][j];
        }
    }
    if !variance.is_finite() || variance < -1e-7 {
        return Err(ScenarioError::CorrelationNotPsd);
    }
    let net = move_pnl
        .checked_sub(liquidity)
        .ok_or(ScenarioError::Overflow)?;
    let stressed = input
        .equity_usd
        .checked_add(net)
        .ok_or(ScenarioError::Overflow)?;
    let loss = if net < Decimal::ZERO {
        -net
    } else {
        Decimal::ZERO
    };
    Ok(PortfolioScenarioResult {
        gross_notional_usd: gross,
        margin_usd: margin,
        stress_price_pnl_usd: move_pnl,
        liquidity_loss_usd: liquidity,
        net_stress_pnl_usd: net,
        stressed_equity_usd: stressed,
        drawdown_ratio: loss
            .checked_div(input.equity_usd)
            .ok_or(ScenarioError::Overflow)?,
        gross_leverage_ratio: gross
            .checked_div(input.equity_usd)
            .ok_or(ScenarioError::Overflow)?,
        correlated_volatility_usd: variance.max(0.0).sqrt(),
        positions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn position() -> ScenarioPosition {
        ScenarioPosition {
            instrument: "BTC-USDT-SWAP".into(),
            side: ScenarioSide::Long,
            notional_usd: Decimal::from(100),
            margin_usd: Decimal::from(10),
            price_shock_bps: -1_000,
            volatility_bps: 200,
            liquidity_cost_bps: 50,
        }
    }

    #[test]
    fn virtual_risk_is_typed_and_has_no_exchange_side_effects() {
        let input = PortfolioScenarioInput::new(
            Decimal::from(50),
            vec![position(), position()],
            vec![ScenarioCorrelation {
                left: 0,
                right: 1,
                rho: 0.6,
            }],
        )
        .unwrap();
        let result = evaluate(&input).unwrap();
        assert_eq!(result.stress_price_pnl_usd, Decimal::from(-20));
        assert_eq!(result.margin_usd, Decimal::from(20));
        assert!((result.correlated_volatility_usd - 3.577708764).abs() < 1e-8);
    }

    #[test]
    fn invalid_matrix_and_non_psd_are_distinct_typed_errors() {
        let negative = PortfolioScenarioInput::new(
            Decimal::from(50),
            vec![position(), position()],
            vec![ScenarioCorrelation {
                left: 0,
                right: 1,
                rho: 1.2,
            }],
        );
        assert_eq!(negative.unwrap_err(), ScenarioError::InvalidInput);

        let invalid = PortfolioScenarioInput::new(
            Decimal::from(50),
            vec![position(), position(), position()],
            vec![
                ScenarioCorrelation {
                    left: 0,
                    right: 1,
                    rho: 0.9,
                },
                ScenarioCorrelation {
                    left: 0,
                    right: 2,
                    rho: 0.9,
                },
                ScenarioCorrelation {
                    left: 1,
                    right: 2,
                    rho: -0.9,
                },
            ],
        )
        .unwrap();
        assert_eq!(evaluate(&invalid), Err(ScenarioError::CorrelationNotPsd));
    }

    #[test]
    fn typed_inputs_reject_invalid_bounds_without_wire_validation() {
        let check = |equity, position: ScenarioPosition| {
            PortfolioScenarioInput::new(equity, vec![position], Vec::new()).unwrap_err()
        };
        assert_eq!(
            check(Decimal::ZERO, position()),
            ScenarioError::InvalidInput
        );
        let mut bounded = position();
        bounded.margin_usd = Decimal::ZERO;
        assert_eq!(
            check(Decimal::from(10), bounded.clone()),
            ScenarioError::InvalidInput
        );
        bounded.margin_usd = Decimal::from(10);
        bounded.price_shock_bps = -10_001;
        assert_eq!(
            check(Decimal::from(10), bounded.clone()),
            ScenarioError::InvalidInput
        );
        bounded.price_shock_bps = 0;
        bounded.volatility_bps = 10_001;
        assert_eq!(
            check(Decimal::from(10), bounded.clone()),
            ScenarioError::InvalidInput
        );
        bounded.volatility_bps = 0;
        bounded.liquidity_cost_bps = 10_001;
        assert_eq!(
            check(Decimal::from(10), bounded.clone()),
            ScenarioError::InvalidInput
        );
        bounded.liquidity_cost_bps = 0;
        bounded.instrument = "BTC/USDT".into();
        assert_eq!(
            check(Decimal::from(10), bounded),
            ScenarioError::InvalidInput
        );
    }
}

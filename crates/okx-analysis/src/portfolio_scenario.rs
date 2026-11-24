//! Virtual portfolio stress calculations over caller-supplied data.
use okx_ipc_contracts::trading::{PortfolioScenarioRequest, PositionSide};
use serde_json::{Value, json};

pub fn evaluate(r: &PortfolioScenarioRequest) -> Result<Value, String> {
    use rust_decimal::Decimal;
    if !r.valid() {
        return Err("INVALID_PORTFOLIO_SCENARIO".into());
    }
    let decimal = |v: &str| {
        v.parse::<Decimal>()
            .map_err(|_| "INVALID_DECIMAL".to_owned())
    };
    let equity = decimal(&r.equity_usd)?;
    let (mut gross, mut margin, mut move_pnl, mut liquidity) =
        (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);
    let mut vols = Vec::new();
    let mut rows = Vec::new();
    for p in &r.positions {
        let n = decimal(&p.notional_usd)?;
        let m = decimal(&p.margin_usd)?;
        let side: i64 = if p.side == PositionSide::Long { 1 } else { -1 };
        let pnl = n
            .checked_mul(Decimal::from(side * i64::from(p.price_shock_bps)))
            .and_then(|x| x.checked_div(Decimal::from(10000)))
            .ok_or("SCENARIO_OVERFLOW")?;
        let impact = n
            .checked_mul(Decimal::from(p.liquidity_cost_bps))
            .and_then(|x| x.checked_div(Decimal::from(10000)))
            .ok_or("SCENARIO_OVERFLOW")?;
        let vol = n
            .checked_mul(Decimal::from(p.volatility_bps))
            .and_then(|x| x.checked_div(Decimal::from(10000)))
            .ok_or("SCENARIO_OVERFLOW")?;
        gross = gross.checked_add(n).ok_or("SCENARIO_OVERFLOW")?;
        margin = margin.checked_add(m).ok_or("SCENARIO_OVERFLOW")?;
        move_pnl = move_pnl.checked_add(pnl).ok_or("SCENARIO_OVERFLOW")?;
        liquidity = liquidity.checked_add(impact).ok_or("SCENARIO_OVERFLOW")?;
        vols.push(
            vol.to_string()
                .parse::<f64>()
                .map_err(|_| "SCENARIO_OVERFLOW")?,
        );
        rows.push(json!({"instrument":p.instrument,"side":p.side,"price_pnl_usd":pnl.normalize().to_string(),
    "liquidity_loss_usd":impact.normalize().to_string(),"source":"caller_supplied"}));
    }
    let size = vols.len();
    let mut correlation = vec![vec![0.0; size]; size];
    for (i, row) in correlation.iter_mut().enumerate() {
        row[i] = 1.0
    }
    for c in &r.correlations {
        correlation[c.left][c.right] = c.rho;
        correlation[c.right][c.left] = c.rho
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
                    return Err("CORRELATION_NOT_PSD".into());
                }
                root[i][j] = value.max(0.0).sqrt()
            } else if root[j][j] > 1e-10 {
                root[i][j] = value / root[j][j]
            } else if value.abs() > 1e-8 {
                return Err("CORRELATION_NOT_PSD".into());
            }
        }
    }
    let mut variance = 0.0;
    for i in 0..size {
        for j in 0..size {
            variance += vols[i] * vols[j] * correlation[i][j]
        }
    }
    if !variance.is_finite() || variance < -1e-7 {
        return Err("CORRELATION_NOT_PSD".into());
    }
    let net = move_pnl.checked_sub(liquidity).ok_or("SCENARIO_OVERFLOW")?;
    let stressed = equity.checked_add(net).ok_or("SCENARIO_OVERFLOW")?;
    let loss = if net < Decimal::ZERO {
        -net
    } else {
        Decimal::ZERO
    };
    Ok(
        json!({"schema":"okx.portfolio.scenario/v1","quality":"assumed","execution":"virtual_read_only",
  "exchange_post":false,"venue_margin_verified":false,"provenance":"caller_supplied",
  "gross_notional_usd":gross.normalize().to_string(),"margin_usd":margin.normalize().to_string(),
  "stress_price_pnl_usd":move_pnl.normalize().to_string(),"liquidity_loss_usd":liquidity.normalize().to_string(),
  "net_stress_pnl_usd":net.normalize().to_string(),"stressed_equity_usd":stressed.normalize().to_string(),
  "drawdown_ratio":loss.checked_div(equity).ok_or("SCENARIO_OVERFLOW")?.normalize().to_string(),
  "gross_leverage_ratio":gross.checked_div(equity).ok_or("SCENARIO_OVERFLOW")?.normalize().to_string(),
  "correlated_volatility_usd":variance.max(0.0).sqrt(),"correlation_basis":"caller_supplied_psd",
  "positions":rows}),
    )
}
#[cfg(test)]
mod scenario_tests {
    use super::*;
    use okx_ipc_contracts::trading::{
        PortfolioScenarioRequest, PositionSide, ScenarioCorrelation, ScenarioPosition,
    };
    #[test]
    fn virtual_risk_has_no_exchange_side_effects_and_requires_psd() {
        let p = ScenarioPosition {
            instrument: "BTC-USDT-SWAP".into(),
            side: PositionSide::Long,
            notional_usd: "100".into(),
            margin_usd: "10".into(),
            price_shock_bps: -1000,
            volatility_bps: 200,
            liquidity_cost_bps: 50,
        };
        let mut r = PortfolioScenarioRequest {
            equity_usd: "50".into(),
            positions: vec![p.clone(), p],
            correlations: vec![ScenarioCorrelation {
                left: 0,
                right: 1,
                rho: 0.6,
            }],
        };
        let v = evaluate(&r).unwrap();
        assert_eq!(v["stress_price_pnl_usd"], "-20");
        assert_eq!(v["exchange_post"], false);
        r.correlations[0].rho = 1.2;
        assert!(evaluate(&r).is_err());
    }
}

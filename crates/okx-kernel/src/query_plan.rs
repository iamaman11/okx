//! Typed K7 analysis plan, independent of storage, transport and venue.
use crate::{
    operators::{Operator, registry},
    time_series::ClosedRange,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const QUERY_PLAN_VERSION: &str = "okx.query-plan/448.1";
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    Candles,
    Ticker,
    Funding,
    OpenInterest,
    Request,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Unit {
    Price,
    Ratio,
    AnnualizedRatio,
    Coefficient,
    Index,
    Bps,
    Quantity,
    QuoteCurrency,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Input {
    pub name: String,
    pub source: Source,
    pub unit: Unit,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorDescriptor {
    pub id: Operator,
    pub version: u16,
    pub inputs: Vec<Input>,
    pub output_unit: Unit,
    pub warmup_bars: u64,
    pub null_policy: String,
    pub deterministic: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Filter {
    pub operator: Operator,
    pub min: Option<f64>,
    pub max: Option<f64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rank {
    pub operator: Operator,
    pub descending: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryPlan {
    pub version: String,
    pub universe: Vec<String>,
    pub dataset_refs: Vec<String>,
    pub range: ClosedRange,
    pub timeframe_ms: u64,
    pub warmup_bars: u64,
    pub sources: Vec<Source>,
    pub operators: Vec<OperatorDescriptor>,
    pub filters: Vec<Filter>,
    pub ranking: Option<Rank>,
    pub result_projection: Vec<Operator>,
    pub parameters: BTreeMap<String, f64>,
    pub assumptions: Vec<String>,
    pub resource_budget_ref: String,
}
fn input(name: &str) -> Result<Input, String> {
    let (source, unit) = match name {
        "close" | "high" | "low" | "peer_close" => (Source::Candles, Unit::Price),
        "volume" => (Source::Candles, Unit::Quantity),
        "bid" | "ask" => (Source::Ticker, Unit::Price),
        "funding" => (Source::Funding, Unit::Ratio),
        "open_interest" => (Source::OpenInterest, Unit::Quantity),
        "budget" | "notional" => (Source::Request, Unit::QuoteCurrency),
        "price" => (Source::Request, Unit::Price),
        "stop_distance" => (Source::Request, Unit::Ratio),
        "fee_bps" | "slippage_bps" => (Source::Request, Unit::Bps),
        _ => return Err(format!("operators.inputs:UNKNOWN:{name}")),
    };
    Ok(Input {
        name: name.into(),
        source,
        unit,
    })
}
fn output_unit(op: Operator) -> Result<Unit, String> {
    Ok(match registry(op).unit {
        "price" => Unit::Price,
        "ratio" => Unit::Ratio,
        "annualized_ratio" => Unit::AnnualizedRatio,
        "coefficient" => Unit::Coefficient,
        "index" => Unit::Index,
        "bps" => Unit::Bps,
        "base_quantity" => Unit::Quantity,
        "quote_currency" => Unit::QuoteCurrency,
        other => return Err(format!("operators.output_unit:UNKNOWN:{other}")),
    })
}
impl QueryPlan {
    pub fn compile(
        universe: Vec<String>,
        range: ClosedRange,
        operators: Vec<Operator>,
        parameters: BTreeMap<String, f64>,
        filters: Vec<Filter>,
        ranking: Option<Rank>,
    ) -> Result<Self, String> {
        if operators.is_empty() || operators.len() > 14 {
            return Err("operators:COUNT_LIMIT".into());
        }
        if range.expected_bars > 1_000_000 {
            return Err("range:LIMIT_BARS".into());
        }
        if universe.len() > 100 || universe.iter().any(String::is_empty) {
            return Err("universe:INVALID".into());
        }
        let selected = operators.iter().copied().collect::<BTreeSet<_>>();
        if selected.len() != operators.len() {
            return Err("operators:DUPLICATE".into());
        }
        for (name, value) in &parameters {
            let valid = value.is_finite()
                && match name.as_str() {
                    "ema_period" | "atr_period" | "rsi_period" => {
                        *value >= 1.0 && *value <= 1000.0 && value.fract() == 0.0
                    }
                    "budget" | "price" => *value > 0.0,
                    "stop_distance" => *value > 0.0 && *value < 1.0,
                    "notional" => *value >= 0.0,
                    "fee_bps" | "slippage_bps" => *value >= 0.0 && *value <= 10000.0,
                    _ => false,
                };
            if !valid {
                return Err(format!("parameters.{name}:INVALID"));
            }
        }
        for (idx, filter) in filters.iter().enumerate() {
            if !selected.contains(&filter.operator) {
                return Err(format!("filters[{idx}].operator:NOT_PROJECTED"));
            }
            if filter.min.is_some_and(|v| !v.is_finite())
                || filter.max.is_some_and(|v| !v.is_finite())
                || filter.min.zip(filter.max).is_some_and(|(a, b)| a > b)
            {
                return Err(format!("filters[{idx}]:INVALID_BOUNDS"));
            }
        }
        if ranking
            .as_ref()
            .is_some_and(|r| !selected.contains(&r.operator))
        {
            return Err("ranking.operator:NOT_PROJECTED".into());
        }
        let mut sources = BTreeSet::new();
        let mut bindings = Vec::new();
        let mut warmup_bars = 0;
        for op in &operators {
            let inputs = registry(*op)
                .inputs
                .iter()
                .map(|s| input(s))
                .collect::<Result<Vec<_>, _>>()?;
            sources.extend(inputs.iter().map(|i| i.source));
            let warmup = match op {
                Operator::Ema => parameters.get("ema_period").copied().unwrap_or(14.0) as u64,
                Operator::Atr => parameters.get("atr_period").copied().unwrap_or(14.0) as u64,
                Operator::Rsi => parameters.get("rsi_period").copied().unwrap_or(14.0) as u64,
                Operator::PearsonCorrelation => 30,
                Operator::RealizedVolatility | Operator::DownsideVolatility => 3,
                _ => 0,
            };
            warmup_bars = warmup_bars.max(warmup);
            bindings.push(OperatorDescriptor {
                id: *op,
                version: 1,
                inputs,
                output_unit: output_unit(*op)?,
                warmup_bars: warmup,
                null_policy: "propagate_missing".into(),
                deterministic: true,
            });
        }
        Ok(Self {
            version: QUERY_PLAN_VERSION.into(),
            universe,
            dataset_refs: Vec::new(),
            timeframe_ms: range.timeframe_ms,
            range,
            warmup_bars,
            sources: sources.into_iter().collect(),
            operators: bindings,
            result_projection: operators,
            parameters,
            filters,
            ranking,
            assumptions: vec!["closed_bars_half_open_utc".into(), "no_forward_fill".into()],
            resource_budget_ref: "research/default".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (Vec<String>, ClosedRange, Vec<Operator>) {
        (
            vec!["BTC-USDT-SWAP".into()],
            ClosedRange::strict(0, 20 * 3_600_000, 3_600_000).unwrap(),
            vec![Operator::Returns, Operator::SpreadBps],
        )
    }
    #[test]
    fn k7_compiled_plan_has_typed_sources_units_and_deterministic_provenance() {
        let (universe, range, ops) = fixture();
        let plan = QueryPlan::compile(
            universe,
            range,
            ops,
            BTreeMap::new(),
            Vec::new(),
            Some(Rank {
                operator: Operator::Returns,
                descending: true,
            }),
        )
        .unwrap();
        assert_eq!(plan.sources, vec![Source::Candles, Source::Ticker]);
        assert_eq!(plan.operators[0].output_unit, Unit::Ratio);
        assert_eq!(plan.operators[1].output_unit, Unit::Bps);
        assert!(plan.operators.iter().all(|o| o.deterministic));
        assert_eq!(plan.range.expected_bars, 20);
    }
    #[test]
    fn k7_invalid_operator_bounds_or_parameter_fail_closed() {
        let (universe, range, ops) = fixture();
        let params = BTreeMap::from([("unrecognized_parameter".to_string(), 1.0)]);
        assert_eq!(
            QueryPlan::compile(
                universe.clone(),
                range,
                ops.clone(),
                params,
                Vec::new(),
                None
            )
            .unwrap_err(),
            "parameters.unrecognized_parameter:INVALID"
        );
        let filters = vec![Filter {
            operator: Operator::Rsi,
            min: None,
            max: Some(40.0),
        }];
        assert_eq!(
            QueryPlan::compile(
                universe.clone(),
                range,
                ops.clone(),
                BTreeMap::new(),
                filters,
                None
            )
            .unwrap_err(),
            "filters[0].operator:NOT_PROJECTED"
        );
        assert_eq!(
            QueryPlan::compile(
                universe,
                range,
                ops,
                BTreeMap::new(),
                Vec::new(),
                Some(Rank {
                    operator: Operator::Atr,
                    descending: false
                }),
            )
            .unwrap_err(),
            "ranking.operator:NOT_PROJECTED"
        );
    }
}

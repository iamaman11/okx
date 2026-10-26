use okx_analysis::operators::{Operator, registry};
use okx_analysis::query_plan::{Filter, QueryPlan, Rank};
use okx_protocol::analytics::MarketRequest;
use std::collections::BTreeSet;
#[derive(Debug, Clone)]
pub struct AnalysisPlan {
    pub sources: BTreeSet<&'static str>,
    pub range: (u64, u64),
    pub alignment: &'static str,
    pub operators: Vec<Operator>,
    pub projection: Vec<String>,
}
impl AnalysisPlan {
    /// Compiles every executable market request to the pure typed operator
    /// plan before source allocation or the first venue request.
    pub fn compile_market(request: &MarketRequest) -> Result<QueryPlan, String> {
        let parse = |name: &str| {
            serde_json::from_value::<Operator>(serde_json::Value::String(name.into()))
                .map_err(|_| format!("operators:UNKNOWN_FIELD:{name}"))
        };
        let operators = request
            .fields
            .iter()
            .map(|f| parse(f))
            .collect::<Result<Vec<_>, _>>()?;
        let filters = request
            .filters
            .iter()
            .map(|f| {
                Ok(Filter {
                    operator: parse(&f.field)?,
                    min: f.min,
                    max: f.max,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let ranking = request
            .sort
            .as_ref()
            .map(|s| {
                Ok::<Rank, String>(Rank {
                    operator: parse(&s.field)?,
                    descending: s.descending,
                })
            })
            .transpose()?;
        let range = okx_analysis::time_series::ClosedRange::strict(
            request.start_ms,
            request.end_ms,
            request.timeframe_ms,
        )
        .map_err(|e| format!("range:{e}"))?;
        QueryPlan::compile(
            request.instruments.clone(),
            range,
            operators,
            request.parameters.clone(),
            filters,
            ranking,
        )
    }
    pub fn build(fields: &[String], start: u64, end: u64) -> Result<Self, String> {
        if start >= end {
            return Err("INVALID_RANGE".into());
        }
        let mut operators = Vec::new();
        let mut sources = BTreeSet::new();
        for f in fields {
            let op: Operator = serde_json::from_value(serde_json::Value::String(f.clone()))
                .map_err(|_| format!("UNKNOWN_FIELD:{f}"))?;
            for input in registry(op).inputs {
                sources.insert(match *input {
                    "close" | "high" | "low" | "volume" | "peer_close" => "candles",
                    "bid" | "ask" => "ticker",
                    "funding" => "funding",
                    "open_interest" => "open_interest",
                    _ => "request",
                });
            }
            operators.push(op)
        }
        Ok(Self {
            sources,
            range: (start, end),
            alignment: "utc_inner_join_minimum_30",
            operators,
            projection: fields.to_vec(),
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn plan_dependencies_exclude_unused_sources() {
        let p = AnalysisPlan::build(&["returns".into(), "spread_bps".into()], 0, 100).unwrap();
        assert_eq!(p.sources, BTreeSet::from(["candles", "ticker"]));
        assert!(!p.sources.contains("funding"));
        assert!(!p.sources.contains("open_interest"));
    }
    #[test]
    fn market_plan_rejects_unknown_operator_before_any_source_read() {
        let mut request: MarketRequest = serde_json::from_value(serde_json::json!({
            "action":"screen","instruments":["BTC-USDT-SWAP"],
            "fields":["returns","spread_bps"],"timeframe_ms":3600000,
            "start_ms":3600000,"end_ms":72000000,"limit":10
        }))
        .unwrap();
        let plan = AnalysisPlan::compile_market(&request).unwrap();
        assert_eq!(plan.result_projection.len(), 2);
        assert_eq!(plan.sources.len(), 2);
        request.fields = vec!["unknown_indicator".into()];
        assert_eq!(
            AnalysisPlan::compile_market(&request).unwrap_err(),
            "operators:UNKNOWN_FIELD:unknown_indicator"
        );
    }

    #[test]
    fn arbitrary_operator_is_rejected() {
        assert!(AnalysisPlan::build(&["execute_shell".into()], 0, 100).is_err());
    }
}

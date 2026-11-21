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
        if request.graph.is_some() && request.composition.is_some() {
            return Err("graph:AMBIGUOUS_REQUEST".into());
        }
        let composition = if let Some(graph) = &request.graph {
            Some(super::graph_resolver::resolve(graph)?)
        } else {
            request.composition.as_ref().map(|c| (**c).clone())
        };
        if composition.as_ref().is_some_and(|composition| {
            composition
                .result_steps
                .iter()
                .any(|id| request.fields.contains(id))
        }) {
            return Err("composition.result_steps:COLLIDES_WITH_LEGACY_FIELD".into());
        }
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
        )?
        .with_composition(composition)
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

    #[test]
    fn public_graph_resolves_only_in_research_before_source_allocation() {
        let mut request: MarketRequest = serde_json::from_value(serde_json::json!({
            "action":"screen", "instruments":["BTC-USDT-SWAP"],
            "fields":["returns"], "timeframe_ms":60000,
            "start_ms":60000, "end_ms":1200000,
            "graph":{
                "version":"okx.graph/v1",
                "steps":[
                    {"id":"r1","op":"returns_series","op_version":1,
                     "inputs":{"input":"close"}},
                    {"id":"w1","op":"rolling_mean","op_version":1,
                     "inputs":{"input":"r1"},"parameters":{"period":2}}
                ],
                "result_steps":["w1"]
            }
        })).unwrap();
        assert!(request.valid());
        assert!(AnalysisPlan::compile_market(&request).unwrap().composition.is_some());
        request.graph.as_mut().unwrap().steps[0].op = "new_primitive".into();
        // Agent can forward the structurally valid graph without knowing op IDs.
        assert!(request.valid());
        assert_eq!(AnalysisPlan::compile_market(&request).unwrap_err(),
                   "graph.r1:UNKNOWN_OPERATOR");
        request.graph.as_mut().unwrap().steps[0].op = "returns_series".into();
        request.composition = Some(Box::new(serde_json::from_value(serde_json::json!({
            "version":"okx.market.composition/v1",
            "steps":[{"id":"old","op":"returns_series","input":"close"}],
            "result_steps":["old"]
        })).unwrap()));
        assert!(!request.valid());
        assert_eq!(AnalysisPlan::compile_market(&request).unwrap_err(),
                   "graph:AMBIGUOUS_REQUEST");
    }
}

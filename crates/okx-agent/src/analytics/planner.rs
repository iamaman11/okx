use okx_analysis::operators::{Operator, registry};
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
    fn arbitrary_operator_is_rejected() {
        assert!(AnalysisPlan::build(&["execute_shell".into()], 0, 100).is_err());
    }
}

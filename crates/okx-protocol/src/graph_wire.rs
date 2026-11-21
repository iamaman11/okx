//! Versioned, operator-agnostic graph transport.
//!
//! Agent and MCP validate only bounded structure. Research owns operator
//! identifiers, mathematical versions, units, causal rules and execution.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub const GRAPH_WIRE_VERSION: &str = "okx.graph/v1";
pub const GRAPH_WIRE_MAX_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphEnvelope {
    pub version: String,
    pub steps: Vec<GraphStep>,
    pub result_steps: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphStep {
    pub id: String,
    /// Open semantic ID, resolved by the installed Research computation library.
    pub op: String,
    /// The semantic version of this particular mathematical operator.
    pub op_version: u16,
    /// Named input ports. Values are closed-candle sources or prior step IDs.
    pub inputs: BTreeMap<String, String>,
    /// Bounded JSON scalar values, decoded into operation-owned Rust types.
    #[serde(default)]
    pub parameters: BTreeMap<String, Value>,
}

fn identifier(value: &str) -> bool {
    (1..=32).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

impl GraphEnvelope {
    /// Safe to run in the Agent before IPC: no operator registry or math here.
    pub fn structurally_valid(&self) -> bool {
        if self.version != GRAPH_WIRE_VERSION
            || !(1..=32).contains(&self.steps.len())
            || !(1..=14).contains(&self.result_steps.len())
            || !serde_json::to_vec(self).is_ok_and(|bytes| bytes.len() <= GRAPH_WIRE_MAX_BYTES)
        {
            return false;
        }
        let mut ids = BTreeSet::new();
        for step in &self.steps {
            if !identifier(&step.id)
                || !ids.insert(step.id.as_str())
                || !identifier(&step.op)
                || step.op_version == 0
                || !(1..=4).contains(&step.inputs.len())
                || step.parameters.len() > 8
                || !step
                    .inputs
                    .iter()
                    .all(|(port, source)| identifier(port) && identifier(source))
                || !step.parameters.iter().all(|(name, value)| {
                    identifier(name)
                        && match value {
                            Value::Null | Value::Bool(_) | Value::Number(_) => true,
                            Value::String(v) => v.len() <= 128,
                            Value::Array(_) | Value::Object(_) => false,
                        }
                })
            {
                return false;
            }
        }
        self.result_steps
            .iter()
            .all(|id| identifier(id) && ids.contains(id.as_str()))
            && self.result_steps.iter().collect::<BTreeSet<_>>().len() == self.result_steps.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn example() -> GraphEnvelope {
        serde_json::from_value(json!({
            "version": GRAPH_WIRE_VERSION,
            "steps": [
                {"id":"change","op":"returns_series","op_version":1,
                 "inputs":{"input":"close"}},
                {"id":"smooth","op":"rolling_mean","op_version":1,
                 "inputs":{"input":"change"},"parameters":{"period":3}}
            ],
            "result_steps":["smooth"]
        }))
        .unwrap()
    }

    #[test]
    fn structural_transport_does_not_whitelist_mathematical_operators() {
        let mut graph = example();
        assert!(graph.structurally_valid());
        graph.steps[0].op = "brand_new_math".into();
        graph.steps[0].op_version = 2;
        assert!(graph.structurally_valid());
        assert_eq!(
            serde_json::from_value::<GraphEnvelope>(json!({
                "version":GRAPH_WIRE_VERSION,"steps":[],"result_steps":[],"unexpected":1
            }))
            .unwrap_err()
            .classify(),
            serde_json::error::Category::Data
        );
    }

    #[test]
    fn structure_is_bounded_and_rejects_collisions_and_executable_payloads() {
        let mut graph = example();
        graph.steps[1].id = "change".into();
        assert!(!graph.structurally_valid());
        let mut graph = example();
        graph.steps[1]
            .parameters
            .insert("script".into(), json!(["execute"]));
        assert!(!graph.structurally_valid());
        let mut graph = example();
        graph.result_steps.push("absent".into());
        assert!(!graph.structurally_valid());
        let mut graph = example();
        graph.steps[0].op_version = 0;
        assert!(!graph.structurally_valid());
        let mut graph = example();
        graph.steps[0].op = "x".repeat(100);
        assert!(!graph.structurally_valid());
    }
}

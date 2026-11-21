//! Versioned, operator-agnostic graph transport.
//!
//! Agent and MCP validate only bounded structure. Research owns operator
//! identifiers, mathematical versions, units, causal rules and execution.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub const GRAPH_WIRE_VERSION: &str = "okx.graph/v1";
// The public request limit is 64 KiB; no smaller, undocumented graph cap.
pub const GRAPH_WIRE_MAX_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphEnvelope {
    pub schema: String,
    pub steps: Vec<GraphStep>,
    pub outputs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphStep {
    pub id: String,
    pub op: GraphOperatorRef,
    /// Ordered sources or prior step references. The operator owns port semantics.
    pub inputs: Vec<GraphInputRef>,
    /// Bounded JSON scalar values, decoded into operation-owned Rust types.
    #[serde(default)]
    pub params: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphOperatorRef {
    /// Open semantic ID, resolved only by Research.
    pub id: String,
    /// Positive semantic version of this particular mathematical operator.
    pub version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphSourceRef {
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphStepRef {
    pub step: String,
}

/// Exactly one of {"source":"close"} or {"step":"prior_id"}.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GraphInputRef {
    Source(GraphSourceRef),
    Step(GraphStepRef),
}

impl GraphInputRef {
    pub fn name(&self) -> &str {
        match self {
            Self::Source(reference) => &reference.source,
            Self::Step(reference) => &reference.step,
        }
    }
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
        if self.schema != GRAPH_WIRE_VERSION
            || !(1..=32).contains(&self.steps.len())
            || !(1..=14).contains(&self.outputs.len())
            || !serde_json::to_vec(self).is_ok_and(|bytes| bytes.len() <= GRAPH_WIRE_MAX_BYTES)
        {
            return false;
        }
        let declared_ids = self
            .steps
            .iter()
            .map(|step| step.id.as_str())
            .collect::<BTreeSet<_>>();
        let mut ids = BTreeSet::new();
        for step in &self.steps {
            if !identifier(&step.id)
                || !ids.insert(step.id.as_str())
                || !identifier(&step.op.id)
                || step.op.version == 0
                || !(1..=4).contains(&step.inputs.len())
                || step.params.len() > 8
                || !step.inputs.iter().all(|input| {
                    identifier(input.name())
                        && match input {
                            // A source reference must not silently resolve as
                            // a similarly named graph step in Research.
                            GraphInputRef::Source(reference) => {
                                !declared_ids.contains(reference.source.as_str())
                            }
                            GraphInputRef::Step(reference) => {
                                // Only already declared steps may be read.
                                // The current step is not yet inserted.
                                ids.contains(reference.step.as_str())
                                    && reference.step != step.id
                            }
                        }
                })
                || !step.params.iter().all(|(name, value)| {
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
        self.outputs
            .iter()
            .all(|id| identifier(id) && ids.contains(id.as_str()))
            && self.outputs.iter().collect::<BTreeSet<_>>().len() == self.outputs.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn example() -> GraphEnvelope {
        serde_json::from_value(json!({
            "schema": GRAPH_WIRE_VERSION,
            "steps": [
                {"id":"change","op":{"id":"returns_series","version":1},
                 "inputs":[{"source":"close"}]},
                {"id":"smooth","op":{"id":"rolling_mean","version":1},
                 "inputs":[{"step":"change"}],"params":{"period":3}}
            ],
            "outputs":["smooth"]
        }))
        .unwrap()
    }

    #[test]
    fn structural_transport_does_not_whitelist_mathematical_operators() {
        let mut graph = example();
        assert!(graph.structurally_valid());
        graph.steps[0].op.id = "brand_new_math".into();
        graph.steps[0].op.version = 2;
        assert!(graph.structurally_valid());
        assert_eq!(
            serde_json::from_value::<GraphEnvelope>(json!({
                "schema":GRAPH_WIRE_VERSION,"steps":[],"outputs":[],"unexpected":1
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
            .params
            .insert("script".into(), json!(["execute"]));
        assert!(!graph.structurally_valid());
        let mut graph = example();
        graph.outputs.push("absent".into());
        assert!(!graph.structurally_valid());
        let mut graph = example();
        graph.steps[0].op.version = 0;
        assert!(!graph.structurally_valid());
        let mut graph = example();
        graph.steps[0].op.id = "x".repeat(100);
        assert!(!graph.structurally_valid());
    }

    #[test]
    fn exact_issue_fixture_is_canonical_and_rejects_old_draft_wire_shape() {
        let target = json!({
            "schema":"okx.graph/v1",
            "steps":[{"id":"mean_close","op":{"id":"rolling_mean","version":1},
                      "inputs":[{"source":"close"}],"params":{"period":20}}],
            "outputs":["mean_close"]
        });
        let decoded: GraphEnvelope = serde_json::from_value(target.clone()).unwrap();
        assert!(decoded.structurally_valid());
        assert_eq!(serde_json::to_value(&decoded).unwrap(), target);
        assert!(
            serde_json::from_value::<GraphEnvelope>(json!({
                "version":"okx.graph/v1","steps":[{"id":"mean_close",
                "op":"rolling_mean","op_version":1,"inputs":{"input":"close"}}],
                "result_steps":["mean_close"]
            }))
            .is_err()
        );
    }

    #[test]
    fn references_are_exclusive_ordered_and_non_cyclic() {
        let graph = example();
        assert!(graph.structurally_valid());
        let mut candidate = serde_json::to_value(&graph).unwrap();
        candidate["steps"][0]["inputs"] = json!([{"step":"smooth"}]);
        let forward: GraphEnvelope = serde_json::from_value(candidate).unwrap();
        assert!(!forward.structurally_valid());
        let mut candidate = serde_json::to_value(&graph).unwrap();
        candidate["steps"][1]["inputs"] = json!([{"source":"close","step":"change"}]);
        assert!(serde_json::from_value::<GraphEnvelope>(candidate).is_err());
        let mut candidate = serde_json::to_value(&graph).unwrap();
        candidate["steps"][1]["inputs"] = json!([{"step":"smooth"}]);
        assert!(
            !serde_json::from_value::<GraphEnvelope>(candidate)
                .unwrap()
                .structurally_valid()
        );
        let mut candidate = serde_json::to_value(&graph).unwrap();
        candidate["steps"][0]["op"]["version"] = json!(65536);
        assert!(
            serde_json::from_value::<GraphEnvelope>(candidate)
                .unwrap()
                .structurally_valid()
        );
        let mut candidate = serde_json::to_value(&graph).unwrap();
        candidate["steps"][0]["op"]["version"] = json!(u64::from(u32::MAX) + 1);
        assert!(serde_json::from_value::<GraphEnvelope>(candidate).is_err());
        let mut candidate = serde_json::to_value(&graph).unwrap();
        candidate["steps"][1]["inputs"] = json!([{"source":"change"}]);
        assert!(
            !serde_json::from_value::<GraphEnvelope>(candidate)
                .unwrap()
                .structurally_valid()
        );
    }
}

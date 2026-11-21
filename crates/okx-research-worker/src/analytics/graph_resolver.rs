//! Mathematical graph resolution lives in Research, never Agent or MCP.
//! This adapter accepts old v1 operations while the independent evaluator is
//! extracted. New operators are registered here with their implementation.
use okx_analysis::query_plan::{COMPOSITION_VERSION, CompositionRequest, SeriesOp, SeriesStep};
use okx_protocol::graph_wire::GraphEnvelope;
use serde_json::Value;

pub fn resolve(graph: &GraphEnvelope) -> Result<CompositionRequest, String> {
    if !graph.structurally_valid() {
        return Err("graph:INVALID_STRUCTURE".into());
    }
    let mut steps = Vec::with_capacity(graph.steps.len());
    for step in &graph.steps {
        // This lookup is deliberately NOT performed in the public protocol.
        let op: SeriesOp = serde_json::from_value(Value::String(step.op.id.clone())).map_err(|_| {
            format!(
                "graph.{}:UNKNOWN_OPERATOR:{}@{}",
                step.id, step.op.id, step.op.version
            )
        })?;
        if step.op.version != 1 {
            return Err(format!(
                "graph.{}:UNSUPPORTED_OPERATOR_VERSION:{}@{}",
                step.id, step.op.id, step.op.version
            ));
        }
        if step.inputs.len() > 2 {
            return Err(format!("graph.{}:TOO_MANY_INPUTS", step.id));
        }
        if step
            .params
            .keys()
            .any(|key| !matches!(key.as_str(), "period" | "threshold"))
        {
            return Err(format!("graph.{}:UNKNOWN_PARAMETER", step.id));
        }
        let input = step
            .inputs
            .first()
            .ok_or_else(|| format!("graph.{}:INPUT_REQUIRED", step.id))?
            .name()
            .to_owned();
        let period = step
            .params
            .get("period")
            .map(|v| serde_json::from_value::<u16>(v.clone()))
            .transpose()
            .map_err(|_| format!("graph.{}:INVALID_PERIOD_TYPE", step.id))?;
        let threshold = step
            .params
            .get("threshold")
            .map(|v| serde_json::from_value::<f64>(v.clone()))
            .transpose()
            .map_err(|_| format!("graph.{}:INVALID_THRESHOLD_TYPE", step.id))?;
        // Input order and arity are operator-owned semantics, not wire policy.
        let second = step
            .inputs
            .get(1)
            .map(|reference| reference.name().to_owned());
        let (when, other) = if op == SeriesOp::ForwardReturnLabel {
            (second, None)
        } else {
            (None, second)
        };
        steps.push(SeriesStep {
            id: step.id.clone(),
            op,
            input,
            period,
            threshold,
            when,
            other,
        });
    }
    Ok(CompositionRequest {
        version: COMPOSITION_VERSION.into(),
        steps,
        result_steps: graph.outputs.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_analysis::query_plan::CheckedComposition;
    use serde_json::json;

    fn graph() -> GraphEnvelope {
        serde_json::from_value(json!({
            "schema":"okx.graph/v1",
            "steps":[
                {"id":"change","op":{"id":"returns_series","version":1},
                 "inputs":[{"source":"close"}]},
                {"id":"smooth","op":{"id":"rolling_mean","version":1},
                 "inputs":[{"step":"change"}],"params":{"period":2}}
            ],
            "outputs":["smooth"]
        }))
        .unwrap()
    }

    #[test]
    fn wire_graph_and_legacy_composition_share_one_numerical_implementation() {
        let graph = graph();
        let compiled = CheckedComposition::compile(resolve(&graph).unwrap(), 4).unwrap();
        let legacy: CompositionRequest = serde_json::from_value(json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"change","op":"returns_series","input":"close"},
                {"id":"smooth","op":"rolling_mean","input":"change","period":2}
            ],
            "result_steps":["smooth"]
        }))
        .unwrap();
        let legacy = CheckedComposition::compile(legacy, 4).unwrap();
        let closes = [100.0, 110.0, 121.0, 133.1];
        assert_eq!(compiled.evaluate(&closes), legacy.evaluate(&closes));
    }

    #[test]
    fn owner_rejects_unsupported_math_and_types_after_transport_validation() {
        let mut candidate = graph();
        candidate.steps[0].op.id = "future_primitive".into();
        assert!(candidate.structurally_valid());
        assert_eq!(
            resolve(&candidate).unwrap_err(),
            "graph.change:UNKNOWN_OPERATOR:future_primitive@1"
        );
        let mut candidate = graph();
        candidate.steps[0].op.version = 2;
        assert!(candidate.structurally_valid());
        assert_eq!(
            resolve(&candidate).unwrap_err(),
            "graph.change:UNSUPPORTED_OPERATOR_VERSION:returns_series@2"
        );
        let mut candidate = graph();
        candidate.steps[1]
            .params
            .insert("period".into(), json!("two"));
        assert_eq!(
            resolve(&candidate).unwrap_err(),
            "graph.smooth:INVALID_PERIOD_TYPE"
        );
        let mut candidate = graph();
        candidate.steps[1]
            .params
            .insert("period".into(), json!(0));
        assert!(CheckedComposition::compile(resolve(&candidate).unwrap(), 4).is_err());
    }

    #[test]
    fn ordered_references_preserve_binary_and_outcome_operator_ports() {
        let graph: GraphEnvelope = serde_json::from_value(json!({
            "schema":"okx.graph/v1",
            "steps":[
                {"id":"returns","op":{"id":"returns_series","version":1},"inputs":[{"source":"close"}]},
                {"id":"mean","op":{"id":"rolling_mean","version":1},"inputs":[{"step":"returns"}],"params":{"period":2}},
                {"id":"difference","op":{"id":"subtract_series","version":1},"inputs":[{"step":"returns"},{"step":"mean"}]},
                {"id":"positive","op":{"id":"greater_than","version":1},"inputs":[{"step":"difference"}],"params":{"threshold":0.0}},
                {"id":"entry","op":{"id":"rising_edge","version":1},"inputs":[{"step":"positive"}]},
                {"id":"outcome","op":{"id":"forward_return_label","version":1},"inputs":[{"source":"close"},{"step":"entry"}],"params":{"period":2}}
            ],
            "outputs":["difference","outcome"]
        })).unwrap();
        assert!(graph.structurally_valid());
        let decoded = resolve(&graph).unwrap();
        assert_eq!(decoded.steps[2].other.as_deref(), Some("mean"));
        assert_eq!(decoded.steps[2].when, None);
        assert_eq!(decoded.steps[5].when.as_deref(), Some("entry"));
        assert_eq!(decoded.steps[5].other, None);
        assert!(CheckedComposition::compile(decoded, 16).is_ok());
    }
}

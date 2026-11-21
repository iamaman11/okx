//! Mathematical graph resolution lives in Research, never Agent or MCP.
//! This adapter accepts old v1 operations while the independent evaluator is
//! extracted. New operators are registered here with their implementation.
use okx_analysis::query_plan::{CompositionRequest, SeriesOp, SeriesStep, COMPOSITION_VERSION};
use okx_protocol::graph_wire::GraphEnvelope;
use serde_json::Value;

pub fn resolve(graph: &GraphEnvelope) -> Result<CompositionRequest, String> {
    if !graph.structurally_valid() {
        return Err("graph:INVALID_STRUCTURE".into());
    }
    let mut steps = Vec::with_capacity(graph.steps.len());
    for step in &graph.steps {
        // This lookup is deliberately NOT performed in the public protocol.
        let op: SeriesOp = serde_json::from_value(Value::String(step.op.clone()))
            .map_err(|_| format!("graph.{}:UNKNOWN_OPERATOR", step.id))?;
        if step.op_version != 1 {
            return Err(format!("graph.{}:UNSUPPORTED_OPERATOR_VERSION", step.id));
        }
        if step
            .inputs
            .keys()
            .any(|key| !matches!(key.as_str(), "input" | "other" | "when"))
        {
            return Err(format!("graph.{}:UNKNOWN_INPUT_PORT", step.id));
        }
        if step
            .parameters
            .keys()
            .any(|key| !matches!(key.as_str(), "period" | "threshold"))
        {
            return Err(format!("graph.{}:UNKNOWN_PARAMETER", step.id));
        }
        let input = step
            .inputs
            .get("input")
            .ok_or_else(|| format!("graph.{}:INPUT_REQUIRED", step.id))?
            .clone();
        let period = step
            .parameters
            .get("period")
            .map(|v| serde_json::from_value::<u16>(v.clone()))
            .transpose()
            .map_err(|_| format!("graph.{}:INVALID_PERIOD_TYPE", step.id))?;
        let threshold = step
            .parameters
            .get("threshold")
            .map(|v| serde_json::from_value::<f64>(v.clone()))
            .transpose()
            .map_err(|_| format!("graph.{}:INVALID_THRESHOLD_TYPE", step.id))?;
        steps.push(SeriesStep {
            id: step.id.clone(),
            op,
            input,
            period,
            threshold,
            when: step.inputs.get("when").cloned(),
            other: step.inputs.get("other").cloned(),
        });
    }
    Ok(CompositionRequest {
        version: COMPOSITION_VERSION.into(),
        steps,
        result_steps: graph.result_steps.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_analysis::query_plan::CheckedComposition;
    use serde_json::json;

    fn graph() -> GraphEnvelope {
        serde_json::from_value(json!({
            "version":"okx.graph/v1",
            "steps":[
                {"id":"change","op":"returns_series","op_version":1,
                 "inputs":{"input":"close"}},
                {"id":"smooth","op":"rolling_mean","op_version":1,
                 "inputs":{"input":"change"},"parameters":{"period":2}}
            ],
            "result_steps":["smooth"]
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
        candidate.steps[0].op = "future_primitive".into();
        assert!(candidate.structurally_valid());
        assert_eq!(resolve(&candidate).unwrap_err(), "graph.change:UNKNOWN_OPERATOR");
        let mut candidate = graph();
        candidate.steps[0].op_version = 2;
        assert!(candidate.structurally_valid());
        assert_eq!(resolve(&candidate).unwrap_err(), "graph.change:UNSUPPORTED_OPERATOR_VERSION");
        let mut candidate = graph();
        candidate.steps[1].parameters.insert("period".into(), json!("two"));
        assert_eq!(resolve(&candidate).unwrap_err(), "graph.smooth:INVALID_PERIOD_TYPE");
        let mut candidate = graph();
        candidate.steps[1].parameters.insert("period".into(), json!(0));
        assert!(CheckedComposition::compile(resolve(&candidate).unwrap(), 4).is_err());
    }
}

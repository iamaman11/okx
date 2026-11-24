use okx_strategy_core::{
    query_plan::{CheckedComposition, CompositionRequest, CompositionValues},
    stream::{CausalGraphStream, ClosedBar},
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputBar {
    pub timestamp_ms: i64,
    pub close: Option<f64>,
}

#[derive(Serialize)]
pub struct ResultBar {
    timestamp_ms: i64,
    values: CompositionValues,
}

pub fn replay(graph: CompositionRequest, bars: Vec<InputBar>) -> Result<Vec<ResultBar>, String> {
    if bars.is_empty() {
        return Err("replay requires at least one closed bar".into());
    }
    let plan = CheckedComposition::compile(graph, bars.len() as u64)?;
    let mut stream = CausalGraphStream::new(&plan)?;
    bars.into_iter()
        .map(|bar| {
            let values = stream.on_closed_bar(ClosedBar {
                timestamp_ms: bar.timestamp_ms,
                close: bar.close,
            })?;
            Ok(ResultBar {
                timestamp_ms: bar.timestamp_ms,
                values,
            })
        })
        .collect()
}

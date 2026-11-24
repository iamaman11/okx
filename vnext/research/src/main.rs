use okx_strategy_core::{
    query_plan::{CheckedComposition, CompositionRequest},
    stream::{CausalGraphStream, ClosedBar},
};
use serde::Deserialize;
use std::io::{self, Write};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Replay {
    graph: CompositionRequest,
    bars: Vec<InputBar>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InputBar {
    timestamp_ms: i64,
    close: Option<f64>,
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let request: Replay = serde_json::from_reader(io::stdin().lock())?;
    if request.bars.is_empty() {
        return Err("replay requires at least one closed bar".into());
    }
    let plan = CheckedComposition::compile(request.graph, request.bars.len().try_into()?)?;
    let mut stream = CausalGraphStream::new(&plan)?;
    let output = request
        .bars
        .into_iter()
        .map(|bar| {
            let values = stream.on_closed_bar(ClosedBar {
                timestamp_ms: bar.timestamp_ms,
                close: bar.close,
            })?;
            Ok(serde_json::json!({"timestamp_ms": bar.timestamp_ms, "values": values}))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, &output)?;
    writeln!(stdout)?;
    Ok(())
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::FAILURE
        }
    }
}

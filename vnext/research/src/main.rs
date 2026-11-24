mod adapters;
mod application;

use adapters::okx_candles;
use application::replay;

use okx_strategy_core::query_plan::CompositionRequest;
use serde::Deserialize;
use std::io::{self, Write};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedReplay {
    graph: CompositionRequest,
    timeframe_ms: replay::TimeframeMs,
    bars: Vec<replay::InputBar>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicReplay {
    graph: CompositionRequest,
    instrument: String,
    interval: okx_candles::Interval,
    limit: u16,
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    let input = io::stdin().lock();
    let mut stdout = io::stdout().lock();
    match args.as_slice() {
        [] => {
            let request: RecordedReplay = serde_json::from_reader(input)?;
            serde_json::to_writer(
                &mut stdout,
                &replay::replay(request.graph, request.timeframe_ms, request.bars)?,
            )?;
        }
        [mode] if mode == "--okx" => {
            let request: PublicReplay = serde_json::from_reader(input)?;
            let timeframe = replay::TimeframeMs::try_from(request.interval.milliseconds())?;
            let (source, bars) =
                okx_candles::fetch(&request.instrument, request.interval, request.limit)?;
            let result = replay::replay(request.graph, timeframe, bars)?;
            serde_json::to_writer(
                &mut stdout,
                &serde_json::json!({"source": source, "bars": result}),
            )?;
        }
        _ => return Err("usage: okx-research-replay [--okx]".into()),
    }
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

//! T13: independently reproduce a small immutable replay on read, using the
//! original recorded inputs. This has no venue, catalog-write or trade path.
//! It must never turn export into an unbounded second grid computation.
use super::result::read_verified_result;
use okx_analysis::operators::StrategyParameters;
use okx_research::{
    ResearchArtifactStore,
    economic_audit::{REPLAY_ECONOMIC_AUDIT_VERSION, audit_graph_replay},
    strategy_registry::{self as replay, Bar, GraphReplaySpec, ReplayExecution, ReplaySpec},
};
use serde_json::{Value, json};

const MAX_BARS: u64 = 8_192;
const MAX_REPLAY_WORK_UNITS: u64 = 65_536;
const PROOF_SCHEMA: &str = "okx.research.immutable-reexecution/v1";

fn unverified(reason: &str) -> Value {
    json!({"schema":PROOF_SCHEMA,"quality":"not_verified","reason_code":reason,
        "max_bars":MAX_BARS,"max_work_units":MAX_REPLAY_WORK_UNITS})
}

fn mismatch() -> String {
    "EXPORT_REEXECUTION_MISMATCH".into()
}

fn recorded_pages<T: serde::de::DeserializeOwned>(
    store: &ResearchArtifactStore,
    manifest: &Value,
    key: &str,
) -> Result<Vec<T>, String> {
    let mut rows = Vec::new();
    for id in manifest[key].as_array().ok_or_else(mismatch)? {
        let id = id.as_str().ok_or_else(mismatch)?;
        let mut chunk: Vec<T> = read_verified_result(store, id)?;
        rows.append(&mut chunk);
    }
    Ok(rows)
}

/// Verify actual output semantics, not just the existence of source hashes.
/// Legacy/future replay versions and oversized workloads retain their existing
/// read/export behavior and do not inherit a claim that this proof ran.
pub(super) fn verify(
    store: &ResearchArtifactStore,
    inputs: &Value,
    manifest: &Value,
) -> Result<Value, String> {
    let Some(audit) = manifest.get("economic_audit").filter(|v| !v.is_null()) else {
        return Ok(unverified("LEGACY_REPLAY_AUDIT_NOT_RETAINED"));
    };
    if audit["schema"] != REPLAY_ECONOMIC_AUDIT_VERSION {
        return Ok(unverified("REPLAY_AUDIT_VERSION_UNSUPPORTED"));
    }
    if inputs["code_version"] != replay::CAUSAL_REPLAY_VERSION
        || manifest["code_version"] != replay::CAUSAL_REPLAY_VERSION
    {
        return Ok(unverified("REPLAY_CODE_VERSION_UNSUPPORTED"));
    }
    let count = inputs["bar_count"].as_u64().ok_or_else(mismatch)?;
    let variants = inputs["variants"].as_array().ok_or_else(mismatch)?;
    let is_grid = manifest["grid"].is_object();
    let graph = inputs.get("graph_spec").filter(|value| value.is_object());
    let candidates = if is_grid { variants.len() as u64 } else { 1 };
    // Grid scores train, holdout, and double-fee sensitivity separately.
    // Graph replay also executes every declared step on every closed bar.
    // Check multiplication before allocating or opening dataset pages.
    let graph_steps = graph.map_or(1, |value| {
        value["graph"]["graph"]["steps"]
            .as_array()
            .map_or(u64::MAX, |steps| steps.len() as u64)
    });
    let work = count
        .checked_mul(candidates)
        .and_then(|n| n.checked_mul(if is_grid { 4 } else { graph_steps }));
    if count > MAX_BARS || candidates > 64 || work.is_none_or(|n| n > MAX_REPLAY_WORK_UNITS) {
        return Ok(unverified("REEXECUTION_BUDGET_EXCEEDED"));
    }
    if count == 0 || (graph.is_none() && variants.is_empty()) {
        return Err(mismatch());
    }
    let dataset_id = inputs["dataset_artifact"].as_str().ok_or_else(mismatch)?;
    let dataset: Value = read_verified_result(store, dataset_id)?;
    let bars: Vec<Bar> = recorded_pages(store, &dataset, "chunks")?;
    if bars.len() as u64 != count {
        return Err(mismatch());
    }
    let (result, score_start, execution) = if let Some(graph) = graph {
        if is_grid || !variants.is_empty() || inputs.get("spec").is_some() {
            return Err(mismatch());
        }
        let spec: GraphReplaySpec =
            serde_json::from_value(graph.clone()).map_err(|_| mismatch())?;
        if manifest["graph_signal"] != json!(spec.graph) {
            return Err(mismatch());
        }
        let start = okx_research::graph_signal::preflight_graph(&spec.graph, bars.len() as u64)
            .map_err(|_| mismatch())?;
        if inputs["graph_warmup_bars"] != json!(start) {
            return Err(mismatch());
        }
        let result = replay::replay_graph(&bars, &spec, start, bars.len(), &|| false)
            .map_err(|_| mismatch())?;
        (result, start, spec.execution)
    } else {
        let spec: ReplaySpec =
            serde_json::from_value(inputs["spec"].clone()).map_err(|_| mismatch())?;
        let parameters: Vec<StrategyParameters> =
            serde_json::from_value(inputs["variants"].clone()).map_err(|_| mismatch())?;
        if spec.parameters != parameters[0] {
            return Err(mismatch());
        }
        let execution = ReplayExecution::from(&spec);
        if is_grid {
            let grid = replay::grid_with_observer(&bars, &spec, &parameters, &|| false, &|_| {})
                .map_err(|_| mismatch())?;
            let stored = &manifest["grid"];
            if stored["selected"] != json!(grid.selected)
                || stored["split_index"] != json!(grid.split_index)
                || stored["train_dataset_hash"] != json!(grid.train_dataset_hash)
                || stored["train"] != json!(grid.train)
                || stored["variants"] != json!(grid.variants)
                || stored["warmup_bars"] != json!(grid.train_score_start_index)
            {
                return Err(mismatch());
            }
            (grid.test, grid.split_index, execution)
        } else {
            if parameters.len() != 1 || !manifest["grid"].is_null() {
                return Err(mismatch());
            }
            let start = spec
                .parameters
                .warmup_for(&spec.strategy_id)
                .map_err(|_| mismatch())?;
            let result = replay::replay(&bars, &spec, start, bars.len(), &|| false)
                .map_err(|_| mismatch())?;
            (result, start, execution)
        }
    };
    if manifest["hash"] != result.hash
        || manifest["dataset_hash"] != result.dataset_hash
        || manifest["summary"] != json!(result.summary)
        || json!(recorded_pages::<Value>(store, manifest, "trade_pages")?) != json!(result.trades)
        || recorded_pages::<(u64, f64)>(store, manifest, "equity_curve_pages")? != result.equity
    {
        return Err(mismatch());
    }
    let currency = inputs["economic_basis"]["quote_currency"]
        .as_str()
        .ok_or_else(mismatch)?;
    let recalculated = audit_graph_replay(
        &bars,
        &execution,
        &result,
        currency,
        score_start,
        bars.len(),
        &|| false,
    )
    .map_err(|_| mismatch())?;
    if json!(recalculated) != *audit {
        return Err(mismatch());
    }
    Ok(json!({
        "schema":PROOF_SCHEMA,"quality":"verified_reexecution",
        "dataset_hash":result.dataset_hash,"result_hash":result.hash,
        "bars_rehydrated":bars.len(),"work_units_upper_bound":work,
        "strategy_and_grid_verified":true,"trades_and_equity_verified":true,
        "exact_cashflow_reaudited":true,
        "historical_rules_verified":false,"historical_fees_verified":false,
        "historical_publication_time_verified":false
    }))
}

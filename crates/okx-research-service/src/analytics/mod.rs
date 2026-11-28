mod benchmark;
mod budget;
pub const RESEARCH_MEMORY_ADMISSION_MIB: u32 = budget::MEMORY_TOTAL_MIB;
pub mod cache;
pub mod candidate_progress;
pub mod cpu_executor;
mod cursor;
mod decision;
mod economic_scenario;
pub mod jobs;
mod market;
pub mod planner;
mod research;
mod resource_gate;
mod resource_observation;
mod result;
mod result_reexecution;
pub mod sources;
mod strategy_signal;

use futures_util::{StreamExt, stream};
use okx_research::{
    ResearchArtifactStore,
    strategy_registry::{self as replay, Costs, Mechanics, ReplaySpec},
};
use okx_research_protocol::analytics::*;
use okx_research_protocol::research_ipc::{SignalDecision, SignalPosition, SignalRequest};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[cfg(test)]
use result::verify_export_manifest;

struct RunnerGuard(Arc<AtomicUsize>);
impl RunnerGuard {
    fn new(count: Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(count)
    }
}
impl Drop for RunnerGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
fn drain_count(durable: u64, runners: &AtomicUsize) -> u64 {
    durable.max(runners.load(Ordering::SeqCst) as u64)
}
#[derive(Clone)]
pub struct AnalyticsService {
    sources: sources::Sources,
    cpu: cpu_executor::CpuExecutor,
    jobs: jobs::Jobs,
    producer: Option<crate::producer::Producer>,
    artifacts: ResearchArtifactStore,
    heavy: Arc<tokio::sync::Semaphore>,
    research_memory: Arc<tokio::sync::Semaphore>,
    runners: Arc<AtomicUsize>,
    signal_slots: Arc<tokio::sync::Semaphore>,
}
impl AnalyticsService {
    /// Legacy signal execution on the existing bounded Research CPU executor.
    /// No account state, risk admission or order sender is available here.
    pub async fn evaluate_signal(&self, request: SignalRequest) -> Result<SignalDecision, String> {
        if !request.valid() {
            return Err("INVALID_SIGNAL_REQUEST".into());
        }
        self.cpu
            .run(Default::default(), move |_| {
                let mut kernel = okx_analysis::operators::StrategyKernel::from_id(
                    &request.strategy_id,
                    request.parameters,
                )?;
                let mut signal = None;
                for bar in &request.closes {
                    signal = kernel.push_closed(bar.close)?;
                }
                let position = match signal {
                    Some(1) => SignalPosition::Long,
                    Some(-1) => SignalPosition::Short,
                    Some(0) => SignalPosition::Flat,
                    None => SignalPosition::Warmup,
                    _ => return Err("INVALID_SIGNAL_OUTPUT".into()),
                };
                Ok(SignalDecision {
                    schema: SignalDecision::SCHEMA.into(),
                    last_closed_ms: request
                        .closes
                        .last()
                        .expect("validated nonempty history")
                        .time_ms,
                    observed_bars: request.closes.len(),
                    position,
                })
            })
            .await
    }
    /// Local metadata projection only: never enters the catalog SQL owner.
    pub fn diagnostic_job(&self, id: &str) -> Value {
        self.jobs.diagnostic_readback(id)
    }
    /// Read-only Edge request lookup; unknown outside the retained job window.
    pub fn diagnostic_request(&self, invocation: &str) -> Value {
        self.jobs.diagnostic_by_invocation(invocation)
    }
    #[cfg(test)]
    pub fn new(
        root: PathBuf,
        bootstrapper: impl Into<crate::source_api::PublicSourceClient>,
    ) -> Result<Self, String> {
        let jobs = jobs::Jobs::open(root.join("research-catalog.sqlite"))?;
        Self::with_jobs(root, bootstrapper.into(), jobs)
    }
    pub fn new_scoped(
        root: PathBuf,
        bootstrapper: impl Into<crate::source_api::PublicSourceClient>,
        scope: &okx_research_protocol::research_ipc::ResearchScope,
    ) -> Result<Self, String> {
        let jobs = jobs::Jobs::open_scoped(root.join("research-catalog.sqlite"), scope)?;
        Self::with_jobs(root, bootstrapper.into(), jobs)
    }
    pub fn new_scoped_with_producer(
        root: PathBuf,
        bootstrapper: impl Into<crate::source_api::PublicSourceClient>,
        scope: &okx_research_protocol::research_ipc::ResearchScope,
        producer: crate::producer::Producer,
    ) -> Result<Self, String> {
        let jobs = jobs::Jobs::open_scoped(root.join("research-catalog.sqlite"), scope)?
            .with_producer(Some(producer.clone()));
        let mut service = Self::with_jobs(root, bootstrapper.into(), jobs)?;
        service.producer = Some(producer);
        Ok(service)
    }
    pub fn new_existing_scoped_with_producer(
        root: PathBuf,
        bootstrapper: impl Into<crate::source_api::PublicSourceClient>,
        scope: &okx_research_protocol::research_ipc::ResearchScope,
        producer: crate::producer::Producer,
    ) -> Result<Self, String> {
        let jobs = jobs::Jobs::open_existing_scoped(root.join("research-catalog.sqlite"), scope)?
            .with_producer(Some(producer.clone()));
        let mut service = Self::with_jobs(root, bootstrapper.into(), jobs)?;
        service.producer = Some(producer);
        Ok(service)
    }
    fn with_jobs(
        root: PathBuf,
        bootstrapper: crate::source_api::PublicSourceClient,
        jobs: jobs::Jobs,
    ) -> Result<Self, String> {
        let service = Self {
            sources: sources::Sources {
                excluded: Default::default(),
                cache: Default::default(),
                bootstrap: bootstrapper,
            },
            cpu: Default::default(),
            jobs,
            producer: None,
            runners: Arc::new(AtomicUsize::new(0)),
            signal_slots: Arc::new(tokio::sync::Semaphore::new(8)),
            artifacts: ResearchArtifactStore::at(root),
            heavy: Arc::new(tokio::sync::Semaphore::new(2)),
            research_memory: Arc::new(tokio::sync::Semaphore::new(
                budget::MEMORY_TOTAL_MIB as usize,
            )),
        };
        service.apply_retention(jiff::Timestamp::now().as_millisecond().max(0) as u64)?;
        Ok(service)
    }
    pub async fn begin_drain(
        &self,
    ) -> Result<okx_research_protocol::research_ipc::DrainStatus, String> {
        let jobs = self.jobs.clone();
        let active_jobs = tokio::task::spawn_blocking(move || jobs.begin_drain())
            .await
            .map_err(|_| "RESEARCH_DRAIN_FAILED")??;
        Ok(okx_research_protocol::research_ipc::DrainStatus {
            active_jobs: drain_count(active_jobs, &self.runners),
        })
    }
    pub async fn research(&self, request: ResearchRequest) -> Result<Value, String> {
        // Graph structural failure must retain its typed path before any
        // job/source I/O. Other Research operations keep their established
        // request admission and error contract.
        if matches!(&request, ResearchRequest::GraphBacktest { .. }) {
            request.validate_graph_backtest()?;
        } else if !request.valid() {
            return Err("INVALID_RESEARCH_REQUEST".into());
        }
        match request {
            request @ (ResearchRequest::RecordDecision { .. }
            | ResearchRequest::Decision { .. }) => {
                let service = self.clone();
                self.cpu
                    .run(Default::default(), move |_| {
                        service.research_decision(request)
                    })
                    .await
            }
            ResearchRequest::Capabilities {} => Ok(research_capabilities()),
            ResearchRequest::Economics { cases, data_mode } => {
                economic_scenario::evaluate(cases, data_mode)
            }
            ResearchRequest::Validate {
                data_mode,
                instruments,
                timeframe_ms,
                start_ms,
                end_ms,
                range_mode,
                ..
            } => {
                if matches!(data_mode, ResearchDataMode::HistoricalObserved) {
                    return Ok(
                        json!({"schema":"okx.research.validation/v1","status":"blocked",
                        "reason_code":"HISTORICAL_RULES_AND_FEES_UNAVAILABLE",
                        "side_effects":"none","job_created":false,"quality":"partial"}),
                    );
                }
                // Validate the canonical window before any venue I/O.
                let validation_range = match range_mode {
                    RangeMode::Strict => okx_analysis::time_series::ClosedRange::strict(
                        start_ms,
                        end_ms,
                        timeframe_ms,
                    ),
                    RangeMode::Normalize => okx_analysis::time_series::ClosedRange::normalize(
                        start_ms,
                        end_ms,
                        timeframe_ms,
                    ),
                }
                .map_err(str::to_owned)?;
                let last_closed = jiff::Timestamp::now().as_millisecond().max(0) as u64
                    / timeframe_ms
                    * timeframe_ms;
                if validation_range.end_ms > last_closed {
                    return Err("RANGE_NOT_CLOSED".into());
                }
                if validation_range.expected_bars > 2000 {
                    return Err("VALIDATION_RANGE_LIMIT:2000_CANDLES_PER_INSTRUMENT".into());
                }
                let mut rows = Vec::new();
                let mut complete = true;
                for symbol in instruments {
                    let result = self
                        .sources
                        .candles(
                            &symbol,
                            timeframe_ms,
                            validation_range.start_ms,
                            validation_range.end_ms,
                            false,
                        )
                        .await;
                    match result {
                        Ok(bars) => {
                            let evidence =
                                validation_range.coverage(bars.iter().map(|bar| bar.time));
                            if !evidence.complete {
                                complete = false;
                            }
                            rows.push(json!({"instrument":symbol,"source":"confirmed_closed_candles",
                                "first_candle_ms":bars.first().map(|b|b.time),
                                "last_candle_ms":bars.last().map(|b|b.time),
                                "observed_bars":bars.len(),"expected_bars":validation_range.expected_bars,
                                "coverage":if evidence.complete{"complete"}else{"partial"},
                                "reason_code":if evidence.complete{None}else{Some("CANDLE_RANGE_INCOMPLETE_OR_GAPPED")},
                                "time_evidence":evidence}));
                        }
                        Err(e) => {
                            complete = false;
                            rows.push(json!({"instrument":symbol,"coverage":"unavailable",
                                "reason_code":e}));
                        }
                    }
                }
                Ok(json!({"schema":"okx.research.validation/v1",
                    "status":if complete{"validated"}else{"blocked"},
                    "quality":if complete{"complete"}else{"partial"},
                    "data_mode":"counterfactual","historical_rules_and_fees":"not_observed",
                    "source_coverage":rows,"timeframe_ms":timeframe_ms,
                    "start_ms":validation_range.start_ms,
                    "end_ms":validation_range.end_ms,
                    "requested_range":{"start_ms":start_ms,"end_ms":end_ms},
                    "range_mode":range_mode,
                    "range_adjusted":start_ms != validation_range.start_ms
                        || end_ms != validation_range.end_ms,
                    "expected_bars":validation_range.expected_bars,
                    "side_effects":"none",
                    "job_created":false}))
            }
            ResearchRequest::Status { job_id } => {
                serde_json::to_value(self.jobs.get_async(job_id).await?).map_err(|e| e.to_string())
            }
            ResearchRequest::List { limit, cursor } => {
                let catalog = self.jobs.clone();
                tokio::task::spawn_blocking(move || catalog.list(limit, cursor.as_deref()))
                    .await
                    .map_err(|_| "RESEARCH_CATALOG_READ_FAILED".to_owned())?
            }
            ResearchRequest::Cancel {
                job_id,
                idempotency_key,
            } => serde_json::to_value(self.jobs.cancel_async(job_id, idempotency_key).await?)
                .map_err(|e| e.to_string()),
            ResearchRequest::Result {
                job_id,
                cursor,
                limit,
                view,
            } => {
                if limit == 0 || limit > 50 {
                    return Err("RESULT_PAGE_LIMIT:1..50".into());
                }
                let service = self.clone();
                self.cpu
                    .run(Default::default(), move |_| {
                        service.result_page(job_id, cursor, limit, view)
                    })
                    .await
            }
            other => self.submit(other).await,
        }
    }
}

struct ReplayPublication<'a> {
    grid: Option<Value>,
    experiment: Option<&'a str>,
    graph_signal: Option<&'a okx_research::graph_signal::GraphSignalSpec>,
    economic_audit: Option<Value>,
    producer: Option<&'a crate::producer::Producer>,
}

fn publish_replay(
    store: &ResearchArtifactStore,
    result: &replay::ReplayResult,
    dataset: &str,
    publication: ReplayPublication<'_>,
    cancel: &impl Fn() -> bool,
) -> Result<String, String> {
    let ReplayPublication {
        grid,
        experiment,
        graph_signal,
        economic_audit,
        producer,
    } = publication;
    let mut trade_pages = Vec::new();
    let mut curve_pages = Vec::new();
    for chunk in result.trades.chunks(50) {
        if cancel() {
            return Err("CANCELLED".into());
        }
        trade_pages.push(store.publish_evidence(&chunk).map_err(|e| e.to_string())?.0);
    }
    for chunk in result.equity.chunks(4096) {
        if cancel() {
            return Err("CANCELLED".into());
        }
        curve_pages.push(store.publish_evidence(&chunk).map_err(|e| e.to_string())?.0);
    }
    let mut manifest = json!({"summary":result.summary,"dataset_hash":result.dataset_hash,"hash":result.hash,"code_version":result.code_version,"dataset_artifact":dataset,"experiment_artifact":experiment,"economic_audit":economic_audit,"producer":producer,"grid":grid,"trade_pages":trade_pages,"equity_curve_pages":curve_pages,"total_equity_points":result.equity.len(),"total_trades":result.trades.len()});
    if let Some(signal) = graph_signal {
        manifest["graph_signal"] = json!(signal);
    }
    Ok(store
        .publish_evidence(&manifest)
        .map_err(|e| e.to_string())?
        .0)
}
impl AnalyticsService {
    /// Seven-day default retention. Active jobs protect their complete working set.
    pub fn apply_retention(&self, now_ms: u64) -> Result<usize, String> {
        // A bounded SQL candidate page, not a full historical jobs snapshot.
        // Keep shared evidence safe when a root is pinned or still referenced.
        if self.jobs.any_running()? {
            return Ok(0);
        }
        let cutoff = now_ms.saturating_sub(7 * 86400000);
        let expired = self.jobs.retention_candidates(cutoff, 128)?;
        if expired.is_empty() {
            return Ok(0);
        }
        let expired_roots = expired
            .iter()
            .flat_map(|j| {
                [j.artifact.clone(), j.decision_artifact.clone()]
                    .into_iter()
                    .flatten()
            })
            .collect::<std::collections::BTreeSet<_>>();
        // The candidates alone define a bounded garbage set. Scan current
        // reference roots in small indexed pages; never materialize every job
        // or every historical artifact in memory just to expire one result.
        let mut garbage = std::collections::BTreeSet::new();
        for root in &expired_roots {
            self.artifact_graph(root, &mut garbage)?;
        }
        let mut cursor: Option<String> = None;
        loop {
            let roots = self.jobs.artifact_roots_page(cursor.as_deref(), 64)?;
            if roots.is_empty() {
                break;
            }
            for root in &roots {
                let retiring_refs = expired
                    .iter()
                    .filter(|job| {
                        job.artifact.as_deref() == Some(root)
                            || job.decision_artifact.as_deref() == Some(root)
                    })
                    .count() as u64;
                // The same content-addressed artifact can be owned by
                // multiple jobs. Retiring one must not delete a live twin.
                if self.jobs.artifact_reference_count(root)? > retiring_refs {
                    let mut shared = std::collections::BTreeSet::new();
                    self.artifact_graph(root, &mut shared)?;
                    garbage.retain(|id| !shared.contains(id));
                }
            }
            cursor = roots.last().cloned();
            if roots.len() < 64 || garbage.is_empty() {
                break;
            }
        }
        // Mark jobs expired FIRST. Crashing before file deletion can leak
        // collectable files, but never leave an accepted job referring to a
        // deleted immutable result.
        for job in &expired {
            let artifact = job.artifact.as_deref().ok_or("RESEARCH_ARTIFACT_MISSING")?;
            self.jobs.expire_terminal_artifact(&job.id, artifact)?;
        }
        for id in &garbage {
            self.artifacts
                .remove_evidence(id)
                .map_err(|e| e.to_string())?;
        }
        Ok(expired.len())
    }
    fn artifact_graph(
        &self,
        id: &str,
        seen: &mut std::collections::BTreeSet<String>,
    ) -> Result<(), String> {
        if !seen.insert(id.into()) {
            return Ok(());
        }
        let value: Value = self
            .artifacts
            .read_evidence_json(id)
            .map_err(|e| e.to_string())?;
        for key in [
            "trade_pages",
            "equity_curve_pages",
            "chunks",
            "row_pages",
            "instrument_artifacts",
        ] {
            if let Some(ids) = value.get(key).and_then(Value::as_array) {
                for child in ids.iter().filter_map(Value::as_str) {
                    self.artifact_graph(child, seen)?;
                }
            }
        }
        // Market row pages are arrays, not object manifests. Their nested
        // capture references must be traversed or retention can erase source
        // witnesses of a still-pinned/referenced completed market result.
        let rows = value
            .as_array()
            .or_else(|| value.get("rows").and_then(Value::as_array));
        if let Some(rows) = rows {
            for row in rows {
                if let Some(captures) = row.get("source_captures").and_then(Value::as_object) {
                    for capture in captures.values() {
                        if let Some(child) =
                            capture.get("evidence_artifact").and_then(Value::as_str)
                        {
                            self.artifact_graph(child, seen)?;
                        }
                    }
                }
            }
        }
        for key in [
            "dataset_artifact",
            "result_artifact",
            "experiment_artifact",
            "previous_decision_artifact",
        ] {
            if let Some(child) = value.get(key).and_then(Value::as_str) {
                self.artifact_graph(child, seen)?;
            }
        }
        for key in ["reference_capture_artifact", "funding_capture_artifact"] {
            if let Some(child) = value
                .pointer(&format!("/economic_basis/{key}"))
                .and_then(Value::as_str)
            {
                self.artifact_graph(child, seen)?;
            }
        }
        Ok(())
    }
    pub fn pin_job(&self, id: &str, pinned: bool) -> Result<(), String> {
        self.jobs.pin(id, pinned)
    }
}
#[cfg(test)]
#[path = "tests/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/artifact_job_fixtures.rs"]
mod artifact_job_fixtures;

fn with_universe_page(mut result: Value, page: Option<&Value>) -> Result<Value, String> {
    if let Some(page) = page {
        result
            .as_object_mut()
            .ok_or("INVALID_MARKET_RESULT")?
            .insert("universe_page".into(), page.clone());
    }
    Ok(result)
}

/** Resolve rank-ordered pages from one immutable source generation and result. */
fn global_rank_page(
    store: &ResearchArtifactStore,
    mut root: Value,
    cursor: &str,
    job_id: &str,
) -> Result<Value, String> {
    let offset = cursor.parse::<usize>().map_err(|_| "INVALID_CURSOR")?;
    let total = root["total_rows"].as_u64().ok_or("INVALID_MARKET_RESULT")? as usize;
    if offset == 0 || offset >= total {
        return Err(if root["universe_page"]["global_ranking"] == true {
            "GLOBAL_RANK_CURSOR_OUT_OF_RANGE"
        } else {
            "MARKET_CURSOR_OUT_OF_RANGE"
        }
        .into());
    }
    let root_id = root["result_artifact"]
        .as_str()
        .ok_or("MARKET_ARTIFACT_MISSING")?;
    let manifest: Value = store
        .read_evidence_json(root_id)
        .map_err(|e| e.to_string())?;
    let chunks = manifest["row_pages"]
        .as_array()
        .ok_or("MARKET_ARTIFACT_INVALID")?;
    let limit = root["page_limit"]
        .as_u64()
        .ok_or("MARKET_ARTIFACT_INVALID")? as usize;
    if !(1..=50).contains(&limit) {
        return Err("MARKET_ARTIFACT_INVALID".into());
    }
    let mut rows = Vec::new();
    for (idx, chunk) in chunks.iter().enumerate().skip(offset / 50) {
        let artifact = chunk.as_str().ok_or("MARKET_ARTIFACT_INVALID")?;
        let values: Vec<Value> = store
            .read_evidence_json(artifact)
            .map_err(|e| e.to_string())?;
        for (index, row) in values.into_iter().enumerate() {
            if idx * 50 + index >= offset && rows.len() < limit {
                rows.push(row);
            }
        }
        if rows.len() >= limit {
            break;
        }
    }
    while rows.len() > 1
        && serde_json::to_vec(&rows)
            .map_err(|_| "SERIALIZATION_FAILED")?
            .len()
            > 6500
    {
        rows.pop();
    }
    let count = rows.len();
    if count == 0 {
        return Err("MARKET_ARTIFACT_INVALID".into());
    }
    let next = (offset + count < total).then(|| format!("{job_id}:{}", offset + count));
    root["rows"] = json!(rows);
    root["returned_rows"] = json!(count);
    root["next_cursor"] = json!(next);
    if root["universe_page"].is_object() {
        root["universe_page"]["has_more"] = json!(next.is_some());
        root["universe_page"]["page_count"] = json!(count);
    }
    Ok(root)
}
fn market_result_with_context(
    mut result: Value,
    job_id: &str,
    universe_page: Option<&Value>,
) -> Result<Value, String> {
    result
        .as_object_mut()
        .ok_or("INVALID_MARKET_RESULT")?
        .insert("job_id".into(), json!(job_id));
    // All market cursors refer to persisted job ID + next row offset.
    let count = result["returned_rows"].as_u64().unwrap_or(0);
    result["page_limit"] = json!(count);
    if let Some(offset) = result["next_cursor"].as_str() {
        result["next_cursor"] = json!(format!("{job_id}:{offset}"));
    }
    if universe_page.is_some_and(|v| v["global_ranking"] == true) {
        let mut page = universe_page.cloned().ok_or("UNIVERSE_PAGE_MISSING")?;
        page["has_more"] = json!(result["next_cursor"].is_string());
        page["page_count"] = json!(count);
        return with_universe_page(result, Some(&page));
    }
    with_universe_page(result, universe_page)
}

#[cfg(test)]
#[path = "tests/universe_artifact_tests.rs"]
mod universe_artifact_tests;

fn research_capabilities() -> Value {
    // Report only what the current public product can actually execute.
    // HistoricalObserved remains unavailable until authenticated historical
    // venue mechanics + fees are captured, not merely a validation enum.
    let contracts = okx_analysis::query_plan::series_operation_contracts();
    let supported_operators = contracts
        .iter()
        .filter(|contract| contract.graph_backtest_supported)
        .map(|contract| contract.op)
        .collect::<Vec<_>>();
    let unavailable_operators = contracts
        .iter()
        .filter(|contract| !contract.graph_backtest_supported)
        .map(|contract| contract.op)
        .collect::<Vec<_>>();
    json!({
        "schema":"okx.research.capabilities/v1",
        "quality":"fresh",
        "strategy_ids":["ema_cross_v1","ema_momentum_confirm_v1","close_breakout_v1"],
        "graph_backtest":{
            "action":"graph_backtest","version":"okx.graph-signal/v1",
            "input":"okx.market.composition/v1","sources":["close"],
            "execution":"same_next_open_costs_funding_and_decimal_ledger_as_registered_backtest",
            "supported_operators":supported_operators,
            "unavailable_operators":unavailable_operators,
            "max_steps":32,"max_result_steps":14,"signal_unit":"boolean_or_event",
            "missing_policies":["hold","flat"],"historical_observed":false,
            "grid_selection":"not_available_for_graph_backtest","trading_authority":false,
            "example_graph_signal":{
                "version":"okx.graph-signal/v1",
                "graph":{"version":"okx.market.composition/v1","steps":[
                    {"id":"returns","op":"returns_series","input":"close"},
                    {"id":"positive","op":"greater_than","input":"returns","threshold":0}
                ],"result_steps":["positive"]},
                "signal_step":"positive","target_if_true":1,"target_if_false":0,
                "missing":"hold"
            }
        },
        "strategy_compositions":{"ema_momentum_confirm_v1":{"version":1,"kernels":["ema_cross_v1","close_momentum"],"parameters":"ema_fast_slow","warmup":"slow_closed_bars","disagreement":"flat","execution":"next_open_counterfactual"}},
        "strategy_definitions":{
            "ema_cross_v1":{"version":1,"parameters":{"fast":{"min":1,"max":999},"slow":{"min":2,"max":1000}},"constraint":"fast<slow","warmup":"slow_closed_bars"},
            "ema_momentum_confirm_v1":{"version":1,"parameters":{"fast":{"min":1,"max":999},"slow":{"min":2,"max":1000}},"constraint":"fast<slow","warmup":"slow_closed_bars"},
            "close_breakout_v1":{"version":1,"parameters":{"lookback":{"min":2,"max":1000}},"signal":"closed_price_vs_prior_lookback_closes","warmup":"lookback_closed_bars","execution":"next_open_counterfactual"}
        },
        "supported_data_modes":["counterfactual"],
        "unavailable_data_modes":[{"mode":"historical_observed",
            "reason_code":"HISTORICAL_RULES_AND_FEES_UNAVAILABLE"}],
        "timeframes_ms":[60000,180000,300000,900000,1800000,3600000,14400000,86400000],
        "limits":{"max_instruments":8,"max_bars_total":1000000,"max_grid_variants":64,"research_memory_total_mib":budget::MEMORY_TOTAL_MIB,"memory_admission":"conservative_pre_source_shared",
            "max_result_page_rows":50,"max_heavy_concurrent_jobs":2},
        "resource_proof":{"memory_admission":"shared_estimated_mib_permit_lease_through_terminal_sql_attempt",
            "cancel_while_waiting":"both_heavy_and_memory_permits_released",
            "benchmark_process_memory":"windows_psapi_or_linux_procfs_rss_high_watermark",
            "memory_measurement_scope":"whole_process_not_per_job_and_not_hard_enforcement",
            "other_platform_measurement":"not_evaluated",
            "stop_under_load":"two_actual_causal_grids_with_cooperative_cancel",
            "mcp_and_finance_under_load":"not_evaluated"},
        "ipc_isolation":{"scheduling":"shared_typed_command_lane_in_protocol_agent_and_worker",
            "critical_in_flight":okx_research_protocol::research_ipc::CRITICAL_IN_FLIGHT,
            "observation_in_flight":okx_research_protocol::research_ipc::OBSERVATION_IN_FLIGHT,
            "work_in_flight":okx_research_protocol::research_ipc::MAX_IN_FLIGHT,
            "critical_commands":["cancel","drain","shutdown"],
            "observation_commands":["explain","capabilities","status","list"],
            "read_only_observation":"no_venue_trade_or_finance_authority",
            "worker_ipc_under_saturated_work":"covered_by_real_child_process_test",
            "engine_and_finance_slo_under_load":"not_evaluated"},
        "result_views":["summary","trades","variants","equity","export"],
        "decisions":{"statuses":["accepted","rejected","deferred"],"single_experiment_only":true,"max_revisions":256,"max_annotation_json_bytes":6144,"trading_authority":false},
        "oos":{"grid_train_test_split":true,"selected_variant_from_train_only":true},
        "economics":{
            "replay_products":["spot","linear_swap"],
            "counterfactual_oracle":{"action":"economics","schema":"okx.research.economics/v1",
                "products":["spot","linear_swap","inverse_swap"],
                "option_policy":"blocked_without_explicit_valuation_model",
                "position_units":{"spot":"base_asset","linear_swap":"contracts",
                    "inverse_swap":"contracts"},
                "settlement_units":{"spot":"quote","linear_swap":"quote",
                    "inverse_swap":"base"},
                "input_basis":"caller_supplied_unverified_current_and_historical",
                "money":"exact_decimal_strings","max_cases":8,
                "independent_currency_totals":true,"fx_conversion":false,
                "cashflow_attribution":{"per_case":["gross","fees","funding","net"],
                    "identity":"gross_minus_fees_minus_signed_funding_equals_net",
                    "grouping":"same_settlement_currency_exact_no_fx",
                    "blocked_reason_counts":true,"signed_funding":true},
                "joint_portfolio_risk":false,"market_reads":false,"job_created":false,
                "trading_authority":false},
            "unsupported_products":["inverse_swap","option","unverified_contract"],
            "linear_contract_units":"base_asset_per_contract_with_quote_settlement_verified_from_current_reference",
            "replay_money_precision":"decimal_ledger_with_f64_price_inputs",
            "observed_funding":{
                "products":["linear_swap"],
                "settlement_join":"funding_event_at_preceding_closed_bar_end_no_forward_fill",
                "bar_observation_schema":"okx.research.funding-bar-observation/v1",
                "observation_semantics":"observed_zero_distinct_from_absent_settlement",
                "replay_arithmetic":"absent_settlement_contributes_zero_cashflow",
                "export_evidence":"capture_counts_and_every_replay_bar_projection_verified",
                "valuation_price":"candle_close_proxy_not_venue_mark_price",
                "historical_cadence_verified":false,
                "publication_time_verified":false,
                "expected_events":null,
                "max_event_gap_guard_ms":okx_research::economics::MAX_FUNDING_EVENT_GAP_MS,
                "source_capture":"normalized_realized_events_immutable_artifact"
            },
            "batch_aggregation":"independent_exact_pnl_totals_grouped_by_quote_currency_no_fx_no_portfolio_risk"
        },
        "source_evidence":{"closed_candles_only":true,"no_forward_fill":true,
            "linear_reference_capture":true,"funding_event_capture_when_requested":true}
    })
}
#[cfg(test)]
#[path = "tests/research_capabilities_contract.rs"]
mod research_capabilities_contract;

#[cfg(test)]
mod compact_batch_summary_tests {
    use super::*;

    #[test]
    fn batch_summary_excludes_bulky_per_instrument_grid_and_preserves_cursor() {
        let (root, service) = super::artifact_job_fixtures::service();
        let row = json!({
            "index": 0,
            "instrument": "BTC-USDT-SWAP",
            "state": "completed",
            "artifact": "sha256:fixture",
            "trades_cursor": "instrument:0:0",
            "summary": {"bulky_grid": "x".repeat(12_000)}
        });
        let page_id = service
            .artifacts
            .publish_evidence(&vec![row.clone()])
            .unwrap()
            .0;
        let result_id = service
            .artifacts
            .publish_evidence(&json!({
                "kind": "research_batch",
                "instruments": [row],
                "row_pages": [page_id],
                "total_rows": 1,
                "summary": {"quality": "complete", "completed": 1, "failed": 0}
            }))
            .unwrap()
            .0;
        let (job, new, _) = service
            .jobs
            .enqueue("k9-batch-summary-fixture".into(), "b".repeat(64))
            .unwrap();
        assert!(new);
        service
            .jobs
            .update(&job.id, jobs::State::Completed, Some(result_id), None)
            .unwrap();
        let summary = service
            .result_page(job.id, None, 1, ResearchResultView::Summary)
            .unwrap();
        assert_eq!(summary["view"], "summary");
        assert_eq!(summary["rows"][0]["state"], "completed");
        assert_eq!(summary["rows"][0]["trades_cursor"], "instrument:0:0");
        assert_eq!(summary["returned_rows"], 1);
        assert!(!summary.to_string().contains("bulky_grid"));
        assert!(summary.to_string().len() < 6500);
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }
}

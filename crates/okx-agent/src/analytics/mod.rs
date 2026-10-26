pub mod cache;
mod cursor;
pub mod executor;
pub mod jobs;
mod market;
pub mod planner;
pub mod sources;

pub(crate) fn is_retryable_market_source_defer(error: &str) -> bool {
    jobs::is_transient_rate_defer(Some(error))
}

use futures_util::{StreamExt, stream};
use okx_protocol::analytics::*;
use okx_research::{
    ResearchArtifactStore,
    strategy_registry::{self as replay, Costs, Mechanics, ReplaySpec},
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
};

fn read_verified_result<T: DeserializeOwned>(
    store: &ResearchArtifactStore,
    artifact: &str,
) -> Result<T, String> {
    store
        .read_evidence_json(artifact)
        .map_err(|error| match error {
            okx_research::ResearchError::ArtifactIdentityMismatch
            | okx_research::ResearchError::InvalidArtifactId => "ARTIFACT_CORRUPT".into(),
            okx_research::ResearchError::Io(ref source)
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                "ARTIFACT_CORRUPT".into()
            }
            other => other.to_string(),
        })
}
#[derive(Clone)]
pub struct AnalyticsService {
    sources: sources::Sources,
    cpu: executor::CpuExecutor,
    jobs: jobs::Jobs,
    artifacts: ResearchArtifactStore,
    heavy: Arc<tokio::sync::Semaphore>,
}
impl AnalyticsService {
    pub fn new(
        root: PathBuf,
        bootstrapper: crate::market_bootstrap::MarketBootstrapper,
    ) -> Result<Self, String> {
        let service = Self {
            sources: sources::Sources {
                excluded: Default::default(),
                cache: Default::default(),
                bootstrap: bootstrapper,
            },
            cpu: Default::default(),
            jobs: jobs::Jobs::open(root.join("research-catalog.sqlite"))?,
            artifacts: ResearchArtifactStore::at(root),
            heavy: Arc::new(tokio::sync::Semaphore::new(2)),
        };
        service.apply_retention(chrono::Utc::now().timestamp_millis().max(0) as u64)?;
        Ok(service)
    }
    pub async fn market(
        &self,
        request: MarketRequest,
        universe_page: Option<Value>,
        resolved_instruments: Option<Vec<String>>,
    ) -> Result<Value, String> {
        if !request.valid() {
            return Err("INVALID_MARKET_REQUEST".into());
        }
        // K7 source, type and range validation precedes every venue read.
        let query_plan = planner::AnalysisPlan::compile_market(&request)?;
        let window = query_plan.range;
        let closed_end = chrono::Utc::now().timestamp_millis().max(0) as u64 / request.timeframe_ms
            * request.timeframe_ms;
        if window.end_ms > closed_end {
            return Err("RANGE_NOT_CLOSED".into());
        }
        let global_rank = request.universe.as_ref().is_some_and(|u| u.rank_globally);
        if global_rank && resolved_instruments.is_none() {
            return Err("GLOBAL_RANK_UNIVERSE_REQUIRED".into());
        }
        let mut compute_request = request.clone();
        if let Some(names) = resolved_instruments {
            compute_request.instruments = names;
            compute_request.universe = None;
        }
        let mut hash_request = request.clone();
        // A page selector is not a separate computation.
        let cursor = hash_request.cursor.take();
        // Unlike the time bucket, this request fingerprint is immutable
        // across pages, but binds the exact typed query and reference generation.
        let rank_fingerprint = if global_rank {
            Some(
                okx_research::canonical_sha256(&(
                    "market/398.global-v1",
                    &hash_request,
                    &universe_page,
                ))
                .map_err(|e| e.to_string())?,
            )
        } else {
            None
        };
        let request_fingerprint = okx_research::canonical_sha256(&(
            "market/448.cursor-v1",
            &hash_request,
            &universe_page,
        ))
        .map_err(|e| e.to_string())?;
        let bucket = if request
            .fields
            .iter()
            .any(|f| matches!(f.as_str(), "spread_bps" | "oi_change"))
        {
            chrono::Utc::now().timestamp_millis() / 1000
        } else if request.end_ms > chrono::Utc::now().timestamp_millis().max(0) as u64 {
            chrono::Utc::now().timestamp_millis() / request.timeframe_ms as i64
        } else {
            0
        };
        let hash = okx_research::canonical_sha256(&(
            "market/448.2",
            &hash_request,
            &universe_page,
            bucket,
        ))
        .map_err(|e| e.to_string())?;
        let hash = hash.strip_prefix("sha256:").unwrap_or(&hash).to_owned();
        if let Some(cursor) = cursor.as_deref() {
            // Reuse the original completed immutable artifact even if the
            // live source bucket has advanced since the preceding page.
            let (job_id, offset) = cursor.split_once(':').ok_or("MARKET_CURSOR_INVALID")?;
            if job_id.len() != 64 || !job_id.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err("MARKET_CURSOR_INVALID".into());
            }
            let job = self
                .jobs
                .get_async(job_id.into())
                .await
                .map_err(|_| "MARKET_CURSOR_NOT_FOUND")?;
            let artifact = job.artifact.ok_or("MARKET_CURSOR_NOT_READY")?;
            let store = self.artifacts.clone();
            let offset = offset.to_owned();
            let job_id = job_id.to_owned();
            return self
                .cpu
                .run(Default::default(), move |_| {
                    let value: Value = store
                        .read_evidence_json(&artifact)
                        .map_err(|e| e.to_string())?;
                    if value["market_request_fingerprint"] != request_fingerprint
                        || value["job_id"] != job_id
                    {
                        return Err("MARKET_CURSOR_REQUEST_MISMATCH".into());
                    }
                    global_rank_page(&store, value, &offset, &job_id)
                })
                .await;
        }
        let (job, new, cancel) = self
            .jobs
            .enqueue_async(format!("market-{hash}"), hash.clone())
            .await?;
        if !new {
            if let Some(artifact) = job.artifact {
                let store = self.artifacts.clone();
                return self
                    .cpu
                    .run(Default::default(), move |_| {
                        store
                            .read_evidence_json(&artifact)
                            .map_err(|e| e.to_string())
                    })
                    .await;
            }
            return with_universe_page(json!(job), universe_page.as_ref());
        }
        let queued_page = universe_page.clone();
        let this = self.clone();
        let mut task = tokio::spawn(async move {
            this.jobs
                .update_async(hash.clone(), jobs::State::Running, None, None)
                .await?;
            // Two global scans at most, using the existing analytics semaphore.
            let _permit = if global_rank {
                Some(
                    this.heavy
                        .acquire()
                        .await
                        .map_err(|_| "ANALYTICS_QUEUE_CLOSED")?,
                )
            } else {
                None
            };
            let mut outcome = this
                .market_execute(compute_request, cancel.clone(), global_rank)
                .await;
            outcome = outcome.and_then(|mut value| {
                value["query_plan"] =
                    serde_json::to_value(&query_plan).map_err(|e| e.to_string())?;
                if let Some(fingerprint) = &rank_fingerprint {
                    value["rank_request_fingerprint"] = json!(fingerprint);
                }
                value["market_request_fingerprint"] = json!(request_fingerprint);
                market_result_with_context(value, &hash, universe_page.as_ref())
            });
            match &outcome {
                Ok(value) => {
                    let store = this.artifacts.clone();
                    let value = value.clone();
                    let artifact = this
                        .cpu
                        .run(cancel.clone(), move |_| {
                            Ok(store.publish_evidence(&value).map_err(|e| e.to_string())?.0)
                        })
                        .await?;
                    this.jobs
                        .update_async(hash.clone(), jobs::State::Completed, Some(artifact), None)
                        .await?;
                }
                Err(e) => {
                    this.jobs
                        .update_async(
                            hash.clone(),
                            if cancel.load(Ordering::Acquire) {
                                jobs::State::Cancelled
                            } else {
                                jobs::State::Failed
                            },
                            None,
                            Some(e.clone()),
                        )
                        .await?;
                }
            }
            outcome
        });
        match tokio::time::timeout(std::time::Duration::from_secs(2), &mut task).await {
            Ok(result) => result.map_err(|e| e.to_string())?,
            Err(_) => with_universe_page(json!(job), queued_page.as_ref()),
        }
    }
    async fn market_execute(
        &self,
        request: MarketRequest,
        cancel: Arc<std::sync::atomic::AtomicBool>,
        global_rank: bool,
    ) -> Result<Value, String> {
        // The public request was validated before expanding the immutable
        // venue reference generation. No >100 explicit input is accepted.
        if request.instruments.is_empty()
            || request.instruments.len() > if global_rank { 512 } else { 100 }
            || request.limit == 0
            || request.limit > 50
        {
            return Err("MARKET_LIMIT:BOUNDED_INSTRUMENTS_AND_ROWS".into());
        }
        let plan = planner::AnalysisPlan::build(&request.fields, request.start_ms, request.end_ms)?;
        let (rows, correlation_matrix) = self
            .market_rows(&request, cancel.clone(), global_rank)
            .await?;
        let store = self.artifacts.clone();
        self.cpu.run(cancel,move |cancel| {
        if cancel.load(Ordering::Acquire){return Err("CANCELLED".into())}
        // Only the first page computes; subsequent pages read exact stored
        // pages rather than reading new venue data or accepting bare offsets.
        let offset = 0usize;
        let total = rows.len();
        let mut row_pages = Vec::new();
        for chunk in rows.chunks(50) {
            row_pages.push(
                store
                    .publish_evidence(&chunk)
                    .map_err(|e| e.to_string())?
                    .0,
            );
        }
        let result_artifact=store.publish_evidence(&json!({"kind":"market_pages","row_pages":row_pages,"total_rows":total,"sources":plan.sources,"correlation_matrix":correlation_matrix})).map_err(|e|e.to_string())?.0;
        let mut page = rows
            .into_iter()
            .skip(offset)
            .take(request.limit as usize)
            .collect::<Vec<_>>();
        while page.len() > 1 && serde_json::to_vec(&page).map_err(|e| e.to_string())?.len() > 6500 {
            page.pop();
        }
        let returned = page.len();
        Ok(
            json!({"kind":"market","result_artifact":result_artifact,"rows":page,"returned_rows":returned,"total_rows":total,"next_cursor":if offset+returned<total{Some((offset+returned).to_string())}else{None},"sources":plan.sources,"correlation_matrix":correlation_matrix}),
        )
        }).await
    }
    pub fn market_capabilities(&self) -> Value {
        let operators = okx_analysis::operators::SUPPORTED_OPERATORS
            .iter()
            .map(|&operator| {
                let info = okx_analysis::operators::registry(operator);
                json!({"name":operator,"unit":info.unit,"inputs":info.inputs})
            })
            .collect::<Vec<_>>();
        json!({"schema":"okx.market.capabilities/v1",
            "actions":["screen","compare","instruments","snapshot","microstructure","capabilities"],
            "timeframes_ms":[60000,180000,300000,900000,1800000,3600000,14400000,86400000],
            "operators":operators,
            "limits":{"max_explicit_instruments":100,"max_page_rows":50,
                "max_correlation_instruments":8,"max_candles_total":1000000},
            "universe":{"modes":["explicit_instruments","venue_reference_registry"],
                "whole_market_paged":true,"globally_ranked":true,
                "global_rank_requires_explicit_opt_in":true,
                "global_rank_max_instruments":512,
                "global_rank_source_budget_bars":1000000,
                "global_rank_uses_immutable_artifact_cursor":true,
                "max_instruments_per_page":100},
            "quality":"complete"})
    }
    pub async fn research(&self, request: ResearchRequest) -> Result<Value, String> {
        if !request.valid() {
            return Err("INVALID_RESEARCH_REQUEST".into());
        }
        match request {
            ResearchRequest::Capabilities {} => Ok(research_capabilities()),
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
                let last_closed = chrono::Utc::now().timestamp_millis().max(0) as u64
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
    fn result_page(
        &self,
        job_id: String,
        cursor: Option<String>,
        limit: u16,
        view: ResearchResultView,
    ) -> Result<Value, String> {
        let j = self.jobs.get(&job_id)?;
        if j.error.as_deref() == Some("RESULT_EXPIRED_BY_7_DAY_RETENTION") {
            return Err("RESULT_EXPIRED".into());
        }
        let artifact = j.artifact.ok_or("RESULT_NOT_READY")?;
        if j.state != jobs::State::Completed {
            return Err("RESULT_NOT_READY".into());
        }
        let signing_key = self.jobs.cursor_signing_key()?;
        let signed = cursor
            .as_deref()
            .filter(|s| s.starts_with("r1."))
            .map(|s| cursor::Cursor::decode(s, &signing_key))
            .transpose()?;
        // Explicit instrument:INDEX:0 is only a first-page selector for a
        // batch. Continuation always requires an authenticated page token.
        let selector = match cursor.as_deref() {
            Some(value) if value.starts_with("r1.") => None,
            Some(value) if value.starts_with("instrument:") => {
                let parts = value.split(':').collect::<Vec<_>>();
                if parts.len() != 3 || parts[2] != "0" {
                    return Err("INVALID_CURSOR".into());
                }
                Some(parts[1].parse::<u64>().map_err(|_| "INVALID_CURSOR")?)
            }
            Some(_) => return Err("INVALID_CURSOR".into()),
            None => None,
        };
        let mut leaf_artifact = artifact.clone();
        let mut value: Value = read_verified_result(&self.artifacts, &artifact)?;
        if let Some(id) = value.get("result_artifact").and_then(Value::as_str) {
            leaf_artifact = id.to_owned();
            value = read_verified_result(&self.artifacts, id)?;
        }
        let instrument_index = signed.as_ref().and_then(|s| s.instrument).or(selector);
        if let Some(index) = instrument_index {
            let row = value
                .get("instruments")
                .and_then(Value::as_array)
                .and_then(|rows| rows.iter().find(|r| r["index"].as_u64() == Some(index)))
                .ok_or("INVALID_CURSOR")?;
            let id = row["artifact"]
                .as_str()
                .ok_or("INSTRUMENT_RESULT_UNAVAILABLE")?;
            leaf_artifact = id.to_owned();
            value = read_verified_result(&self.artifacts, id)?;
        }
        if let Some(signed) = &signed {
            signed.matches(&job_id, &artifact, &leaf_artifact, view)?;
        }
        let offset = signed.as_ref().map_or(0, |s| s.offset);
        let metadata = json!({
            "job_id":job_id,"artifact":artifact,
            "summary":value.get("summary").or_else(||value.pointer("/test/summary")),
            "grid":value.get("grid").map(|g| json!({
                "selected":g.get("selected"),"train":g.get("train"),
                "test":value.get("summary"),"split_index":g.get("split_index"),
                "train_dataset_hash":g.get("train_dataset_hash"),"train_range":g.get("train_range"),
                "test_range":g.get("test_range"),"warmup_bars":g.get("warmup_bars"),
                "seed":g.get("seed"),"strategy_id":g.get("strategy_id"),
                "selection_warning":g.get("selection_warning")
            })),
            "dataset_hash":value.get("dataset_hash"),
            "code_version":value.get("code_version"),
            "result_hash":value.get("hash")
        });
        if matches!(view, ResearchResultView::Summary) && value.get("instruments").is_none() {
            if cursor.is_some() {
                return Err("INVALID_CURSOR".into());
            }
            return Ok(
                json!({"view":"summary","metadata":metadata,"rows":[],"total_rows":0,
                "returned_rows":0,"next_cursor":null}),
            );
        }
        if matches!(
            view,
            ResearchResultView::Variants | ResearchResultView::Equity
        ) {
            let mut output = Vec::new();
            let total;
            if matches!(view, ResearchResultView::Variants) {
                let variants = value
                    .pointer("/grid/variants")
                    .and_then(Value::as_array)
                    .ok_or("VARIANTS_UNAVAILABLE")?;
                total = variants.len();
                output.extend(variants.iter().skip(offset).take(limit as usize).cloned());
            } else {
                let pages = value
                    .get("equity_curve_pages")
                    .and_then(Value::as_array)
                    .ok_or("EQUITY_RESULT_UNAVAILABLE")?;
                total = value
                    .get("total_equity_points")
                    .and_then(Value::as_u64)
                    .ok_or("EQUITY_RESULT_UNAVAILABLE")? as usize;
                let mut position = offset;
                while output.len() < limit as usize && position < total {
                    let id = pages
                        .get(position / 4096)
                        .and_then(Value::as_str)
                        .ok_or("EQUITY_PAGE_MISSING")?;
                    let points: Vec<Value> = read_verified_result(&self.artifacts, id)?;
                    if position % 4096 >= points.len() {
                        return Err("EQUITY_PAGE_MISSING".into());
                    }
                    let take = (limit as usize - output.len()).min(points.len() - position % 4096);
                    output.extend(points.into_iter().skip(position % 4096).take(take));
                    position += take;
                }
            }
            if cursor.is_some() && offset >= total {
                return Err("RESULT_CURSOR_OUT_OF_RANGE".into());
            }
            while output.len() > 1
                && serde_json::to_vec(&output)
                    .map_err(|e| e.to_string())?
                    .len()
                    > 6500
            {
                output.pop();
            }
            if output.len() == 1
                && serde_json::to_vec(&output)
                    .map_err(|e| e.to_string())?
                    .len()
                    > 6500
            {
                return Err("RESULT_ROW_TOO_LARGE".into());
            }
            let next = if offset + output.len() < total {
                Some(
                    cursor::Cursor::new(
                        &job_id,
                        &artifact,
                        &leaf_artifact,
                        view,
                        offset + output.len(),
                        instrument_index,
                    )
                    .encode(&signing_key)?,
                )
            } else {
                None
            };
            return Ok(
                json!({"view":if matches!(view,ResearchResultView::Equity){"equity"}else{"variants"},
                "metadata":metadata,"rows":output,"returned_rows":output.len(),
                "total_rows":total,"next_cursor":next}),
            );
        }
        let pages = value
            .get("trade_pages")
            .or_else(|| value.get("row_pages"))
            .and_then(Value::as_array)
            .ok_or("RESULT_MANIFEST_INVALID")?;
        let total = value
            .get("total_trades")
            .or_else(|| value.get("total_rows"))
            .and_then(Value::as_u64)
            .unwrap_or(0) as usize;
        if cursor.is_some() && offset >= total {
            return Err("RESULT_CURSOR_OUT_OF_RANGE".into());
        }
        let mut page = Vec::new();
        let mut position = offset;
        while page.len() < limit as usize && position < total {
            let id = pages
                .get(position / 50)
                .and_then(Value::as_str)
                .ok_or("RESULT_PAGE_MISSING")?;
            let rows: Vec<Value> = read_verified_result(&self.artifacts, id)?;
            if rows.len() <= position % 50 {
                // A missing, empty or prematurely short page must never
                // spin at the same cursor or pretend the result is complete.
                return Err("RESULT_PAGE_TRUNCATED".into());
            }
            let take = (limit as usize - page.len()).min(rows.len() - position % 50);
            page.extend(rows.into_iter().skip(position % 50).take(take));
            position += take;
        }
        while page.len() > 1 && serde_json::to_vec(&page).map_err(|e| e.to_string())?.len() > 6500 {
            page.pop();
        }
        if page.len() == 1 && serde_json::to_vec(&page).map_err(|e| e.to_string())?.len() > 6500 {
            return Err("RESULT_ROW_TOO_LARGE".into());
        }
        let next = if offset + page.len() < total {
            Some(
                cursor::Cursor::new(
                    &job_id,
                    &artifact,
                    &leaf_artifact,
                    view,
                    offset + page.len(),
                    instrument_index,
                )
                .encode(&signing_key)?,
            )
        } else {
            None
        };
        Ok(json!({
            "view":"trades","metadata":metadata,
            "assumptions":value.pointer("/summary/assumptions").or_else(||value.pointer("/test/summary/assumptions")),
            "job_id":job_id,"artifact":artifact,"summary":value.get("summary").or_else(||value.pointer("/test/summary")),
            "rows":page,"returned_rows":page.len(),"total_rows":total,"next_cursor":next
        }))
    }
    async fn submit(&self, request: ResearchRequest) -> Result<Value, String> {
        let hash = okx_research::canonical_sha256(&("analytics/448.1", &request))
            .map_err(|e| e.to_string())?;
        let hash = hash.strip_prefix("sha256:").unwrap_or(&hash).to_owned();
        let key = match &request {
            ResearchRequest::Backtest {
                idempotency_key, ..
            }
            | ResearchRequest::Grid {
                idempotency_key, ..
            } => idempotency_key.clone(),
            _ => return Err("INVALID_JOB_REQUEST".into()),
        };
        let (job, new, cancel) = self.jobs.enqueue_async(key, hash.clone()).await?;
        if !new {
            return Ok(json!(job));
        }
        let this = self.clone();
        tokio::spawn(async move {
            let outcome = async {
                // Bounded SQL admission happens at submit. Accepted queued
                // jobs wait for an existing heavy permit instead of becoming
                // FAILED because two other research jobs are in progress.
                let _heavy = tokio::select! {
                    result = this.heavy.clone().acquire_owned() =>
                        result.map_err(|_| "ANALYTICS_QUEUE_CLOSED")?,
                    _ = executor::cancelled(cancel.clone()) =>
                        return Err("CANCELLED".into()),
                };
                this.jobs.update_async(hash.clone(), jobs::State::Running, None, None).await?;
                let instruments = match &request { ResearchRequest::Backtest{instruments,..}|ResearchRequest::Grid{instruments,..}=>instruments.clone(),_=>unreachable!() };
                if instruments.len()==1 { return this.execute_one(request,cancel.clone()).await; }
                let mut work=stream::iter(instruments.into_iter().enumerate().map(|(index,instrument)|{
                    let service=this.clone();let mut single=request.clone();let token=cancel.clone();
                    match &mut single {ResearchRequest::Backtest{instruments,..}|ResearchRequest::Grid{instruments,..}=>*instruments=vec![instrument.clone()],_=>unreachable!()};
                    async move {(index,instrument,service.execute_one(single,token).await)}
                })).buffer_unordered(4);
                let mut rows=Vec::new();let mut artifacts=Vec::new();
                while let Some((index,instrument,outcome))=work.next().await {
                    if cancel.load(Ordering::Acquire){return Err("CANCELLED".into())}
                    let store=this.artifacts.clone();
                    let previous_rows=rows;let previous_artifacts=artifacts;
                    let (next_rows,next_artifacts,manifest)=this.cpu.run(cancel.clone(),move |_|{
                        let mut rows=previous_rows;let mut artifacts=previous_artifacts;
                    let row=match outcome {Ok(artifact)=>{let result:Value=store.read_evidence_json(&artifact).map_err(|e|e.to_string())?;artifacts.push(artifact.clone());json!({"index":index,"instrument":instrument,"state":"completed","artifact":artifact,"summary":result["summary"],"dataset_hash":result["dataset_hash"],"grid":result["grid"],"trades_cursor":format!("instrument:{index}:0")})},Err(error)=>json!({"index":index,"instrument":instrument,"state":"failed","reason_code":error})};
                    rows.push(row);rows.sort_by_key(|r|r["index"].as_u64().unwrap_or(0));
                    let page=store.publish_evidence(&rows).map_err(|e|e.to_string())?.0;
                    let manifest=store.publish_evidence(&json!({"kind":"research_batch","summary":{"semantics":"independent_instruments","quality":if artifacts.len()==rows.len(){"complete"}else{"partial"},"completed":artifacts.len(),"failed":rows.len()-artifacts.len()},"row_pages":[page],"total_rows":rows.len(),"instrument_artifacts":artifacts,"instruments":rows})).map_err(|e|e.to_string())?.0;

                        Ok((rows,artifacts,manifest))
                    }).await?;
                    rows=next_rows;artifacts=next_artifacts;
                    this.jobs.update_async(hash.clone(),jobs::State::Running,Some(manifest),None).await?;
                }
                this.jobs.get_async(hash.clone()).await?.artifact.ok_or_else(||"EMPTY_BATCH".into())
            }
            .await;
            match outcome {
                Ok(a) => {
                    let _ = this
                        .jobs
                        .update_async(hash.clone(), jobs::State::Completed, Some(a), None)
                        .await;
                }
                Err(e) => {
                    let state = if cancel.load(Ordering::Acquire) {
                        jobs::State::Cancelled
                    } else {
                        jobs::State::Failed
                    };
                    let artifact = this
                        .jobs
                        .get_async(hash.clone())
                        .await
                        .ok()
                        .and_then(|j| j.artifact);
                    let _ = this
                        .jobs
                        .update_async(hash.clone(), state, artifact, Some(e))
                        .await;
                }
            }
        });
        Ok(json!(job))
    }
    async fn execute_one(
        &self,
        request: ResearchRequest,
        cancel: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<String, String> {
        let (instruments, ms, start, end, strategy, variants, costs, mode, seed, is_grid) =
            match request {
                ResearchRequest::Backtest {
                    instruments,
                    timeframe_ms,
                    start_ms,
                    end_ms,
                    strategy_id,
                    parameters,
                    costs,
                    data_mode,
                    seed,
                    ..
                } => (
                    instruments,
                    timeframe_ms,
                    start_ms,
                    end_ms,
                    strategy_id,
                    vec![parameters],
                    costs,
                    data_mode,
                    seed,
                    false,
                ),
                ResearchRequest::Grid {
                    instruments,
                    timeframe_ms,
                    start_ms,
                    end_ms,
                    strategy_id,
                    variants,
                    costs,
                    data_mode,
                    seed,
                    ..
                } => (
                    instruments,
                    timeframe_ms,
                    start_ms,
                    end_ms,
                    strategy_id,
                    variants,
                    costs,
                    data_mode,
                    seed,
                    true,
                ),
                _ => unreachable!(),
            };
        if instruments.len() != 1 {
            return Err("REPLAY_REQUIRES_EXACTLY_ONE_INSTRUMENT".into());
        }
        if !matches!(
            strategy.as_str(),
            "ema_cross_v1" | "no_trade" | "close_momentum" | "two_bar_momentum"
        ) {
            return Err("UNSUPPORTED_STRATEGY".into());
        }
        if matches!(mode, ResearchDataMode::HistoricalObserved) {
            return Err("HISTORICAL_RULES_AND_FEES_UNAVAILABLE".into());
        }
        let mut bars = tokio::select! {loaded=self.sources.candles(&instruments[0],ms,start,end,false)=>loaded?,_=executor::cancelled(cancel.clone())=>return Err("CANCELLED".into())};
        let expected_start = start.div_ceil(ms).saturating_mul(ms);
        let expected_end = end / ms * ms;
        if bars.first().is_none_or(|b| b.time > expected_start)
            || bars
                .last()
                .is_none_or(|b| b.time.saturating_add(ms) < expected_end)
        {
            return Err("CANDLE_RANGE_INCOMPLETE".into());
        }
        if bars
            .windows(2)
            .any(|pair| pair[1].time != pair[0].time.saturating_add(ms))
        {
            return Err("CANDLE_INTERIOR_GAP".into());
        }
        if bars.len().saturating_mul(512) > 256 * 1024 * 1024 {
            return Err("RESEARCH_MEMORY_LIMIT".into());
        }
        let variants = variants
            .into_iter()
            .map(|p| {
                okx_analysis::operators::EmaParameters {
                    fast: p.fast,
                    slow: p.slow,
                }
                .validate()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let first = *variants.first().ok_or("EMPTY_VARIANTS")?;
        let mechanics = if instruments[0].ends_with("-SWAP") {
            let reference = self
                .sources
                .bootstrap
                .research_current_reference(&instruments[0])
                .await
                .map_err(|e| e.to_string())?;
            if reference.instrument.contract_type.as_deref() != Some("linear") {
                return Err("UNSUPPORTED_INVERSE_CONTRACT".into());
            }
            let multiplier = reference
                .instrument
                .contract_value
                .as_deref()
                .ok_or("CONTRACT_MULTIPLIER_MISSING")?
                .parse::<f64>()
                .map_err(|e| e.to_string())?;
            Mechanics::Linear {
                contract_multiplier: multiplier,
            }
        } else if instruments[0].split('-').count() == 2 {
            Mechanics::Spot
        } else {
            return Err("UNSUPPORTED_INSTRUMENT_TYPE".into());
        };
        if matches!(costs.funding_mode, FundingMode::Observed) {
            let mut owned = (*bars).clone();
            if matches!(mechanics, Mechanics::Spot) {
                for b in &mut owned {
                    b.funding_rate = Some(0.0);
                }
            } else {
                let funding = self.sources.funding(&instruments[0], start, end).await?;
                if funding
                    .first()
                    .is_none_or(|v| v.0 > start.saturating_add(28_800_000))
                    || funding
                        .last()
                        .is_none_or(|v| v.0.saturating_add(28_800_000) < end)
                {
                    return Err("FUNDING_RANGE_INCOMPLETE".into());
                }
                for b in &mut owned {
                    b.funding_rate = Some(
                        funding
                            .iter()
                            .filter(|(t, _)| *t >= b.time && *t < b.time + ms)
                            .map(|(_, r)| r)
                            .sum(),
                    );
                }
            }
            bars = Arc::new(owned);
        }
        let spec = ReplaySpec {
            strategy_id: strategy,
            parameters: first,
            mechanics,
            costs: Costs {
                maker_fee_bps: costs.maker_fee_bps,
                taker_fee_bps: costs.taker_fee_bps,
                slippage_bps: costs.slippage_bps,
                observed_funding: matches!(costs.funding_mode, FundingMode::Observed),
            },
            timeframe_ms: ms,
            historical_observed: matches!(mode, ResearchDataMode::HistoricalObserved),
            historical_rules: false,
            historical_fees: false,
            seed,
            stop_fraction: None,
            take_profit_fraction: None,
        };
        let store = self.artifacts.clone();
        let artifact=self.cpu.run(cancel.clone(),move|c|{
                    let cancelled=||c.load(Ordering::Acquire);
                    let mut chunks=Vec::new();for chunk in bars.chunks(4096){if cancelled(){return Err("CANCELLED".into())}chunks.push(store.publish_evidence(&chunk).map_err(|e|e.to_string())?.0);}
                    let dataset=store.publish_evidence(&json!({"schema":"okx.research.dataset/398.1","instrument":instruments[0],"timeframe_ms":ms,"start_ms":start,"end_ms":end,"chunks":chunks})).map_err(|e|e.to_string())?.0;
                    let(result,grid)=if is_grid{let g=replay::grid(&bars,&spec,&variants,&cancelled)?;(g.test,Some(json!({"selected":g.selected,"split_index":g.split_index,"train_dataset_hash":g.train_dataset_hash,"train_range":{"start_ms":bars[g.train_score_start_index].time,"end_ms":bars[g.split_index].time},"warmup_bars":g.train_score_start_index,"seed":spec.seed,"strategy_id":spec.strategy_id,"test_range":{"start_ms":bars[g.split_index].time,"end_ms":end},"train":g.train,"variants":g.variants,"selection_warning":g.selection_warning})))}else{(replay::replay(&bars,&spec,first.slow,bars.len(),&cancelled)?,None)};
                    publish_replay(&store,&result,&dataset,grid,&cancelled)
                }).await?;
        Ok::<_, String>(artifact)
    }
}

fn publish_replay(
    store: &ResearchArtifactStore,
    result: &replay::ReplayResult,
    dataset: &str,
    grid: Option<Value>,
    cancel: &impl Fn() -> bool,
) -> Result<String, String> {
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
    let manifest = json!({"summary":result.summary,"dataset_hash":result.dataset_hash,"hash":result.hash,"code_version":result.code_version,"dataset_artifact":dataset,"grid":grid,"trade_pages":trade_pages,"equity_curve_pages":curve_pages,"total_equity_points":result.equity.len(),"total_trades":result.trades.len()});
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
            .filter_map(|j| j.artifact.clone())
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
                    .filter(|job| job.artifact.as_deref() == Some(root))
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
        for key in ["dataset_artifact", "result_artifact"] {
            if let Some(child) = value.get(key).and_then(Value::as_str) {
                self.artifact_graph(child, seen)?;
            }
        }
        Ok(())
    }
    pub fn pin_job(&self, id: &str, pinned: bool) -> Result<(), String> {
        self.jobs.pin(id, pinned)
    }
}
impl AnalyticsService {
    /// Shared closed-bar loader for the live signal runner; close time is the decision key.
    pub async fn load_closed_closes(
        &self,
        instrument: &str,
        timeframe_ms: u64,
        limit: usize,
    ) -> Result<Vec<(u64, f64)>, String> {
        if limit == 0 || limit > 10000 {
            return Err("LIVE_WARMUP_LIMIT:1..10000".into());
        }
        sources::timeframe(timeframe_ms)?;
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let end = now / timeframe_ms * timeframe_ms;
        let start = end.saturating_sub((limit as u64).saturating_mul(timeframe_ms));
        let bars = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.sources
                .candles(instrument, timeframe_ms, start, end, false),
        )
        .await
        .map_err(|_| "LIVE_SOURCE_DEADLINE")??;
        Ok(bars
            .iter()
            .map(|b| (b.time + timeframe_ms, b.close))
            .collect())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn market_capabilities_discloses_supported_operator_names_and_units() {
        let (root, service) = super::artifact_job_fixtures::service();
        let capabilities = service.market_capabilities();
        let operators = capabilities["operators"].as_array().unwrap();
        assert_eq!(
            operators.len(),
            okx_analysis::operators::SUPPORTED_OPERATORS.len()
        );
        assert!(
            operators
                .iter()
                .any(|operator| operator["name"] == "returns" && operator["unit"] == "ratio")
        );
        assert!(
            operators
                .iter()
                .all(|operator| operator["inputs"].is_array())
        );
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn grid_result_includes_selected_train_test_and_equity() {
        let (root, service) = super::artifact_job_fixtures::service();
        let dataset = service
            .artifacts
            .publish_evidence(&json!({"chunks":[]}))
            .unwrap()
            .0;
        let summary = replay::Summary {
            pnl: 1.0,
            pnl_exact: "1".into(),
            fees_exact: "0.1".into(),
            funding_exact: "0".into(),
            max_drawdown: 0.05,
            sharpe: 1.2,
            trades: 0,
            turnover: 2.0,
            fees: 0.1,
            funding: 0.0,
            benchmark: 0.0,
            assumptions: vec!["COUNTERFACTUAL".into()],
        };
        let result = replay::ReplayResult {
            dataset_hash: "dataset-398".into(),
            code_version: "test".into(),
            hash: "result-398".into(),
            summary: summary.clone(),
            equity: (0..100).map(|i| (i, 10000.0 + i as f64)).collect(),
            trades: vec![],
        };
        let grid = json!({"selected":{"fast":3,"slow":8},"train":summary,
            "split_index":70,"selection_warning":"Counterfactual only",
            "variants":[{"parameters":{"fast":3,"slow":8},"train":summary}]});
        let manifest =
            publish_replay(&service.artifacts, &result, &dataset, Some(grid), &|| false).unwrap();
        let id = "c".repeat(64);
        service
            .jobs
            .enqueue("f05-proof".into(), id.clone())
            .unwrap();
        service
            .jobs
            .update(&id, jobs::State::Completed, Some(manifest), None)
            .unwrap();
        let get = |view, cursor: Option<String>| ResearchRequest::Result {
            job_id: id.clone(),
            cursor,
            limit: 5,
            view,
        };
        let summary_view = service
            .research(get(ResearchResultView::Summary, None))
            .await
            .unwrap();
        assert_eq!(summary_view["metadata"]["grid"]["selected"]["fast"], 3);
        assert_eq!(summary_view["metadata"]["grid"]["train"]["pnl"], 1.0);
        assert_eq!(summary_view["metadata"]["grid"]["test"]["pnl"], 1.0);
        assert_eq!(summary_view["metadata"]["summary"]["pnl"], 1.0);
        assert_eq!(summary_view["returned_rows"], 0);
        let variants = service
            .research(get(ResearchResultView::Variants, None))
            .await
            .unwrap();
        assert_eq!(variants["total_rows"], 1);
        assert_eq!(variants["rows"][0]["parameters"]["slow"], 8);
        let first = service
            .research(get(ResearchResultView::Equity, None))
            .await
            .unwrap();
        assert_eq!(first["total_rows"], 100);
        assert_eq!(first["returned_rows"], 5);
        assert!(first["next_cursor"].as_str().unwrap().starts_with("r1."));
        let next = service
            .research(get(
                ResearchResultView::Equity,
                first["next_cursor"].as_str().map(str::to_owned),
            ))
            .await
            .unwrap();
        assert_eq!(next["rows"][0][0], 5);
        assert!(
            service
                .research(get(ResearchResultView::Equity, Some("invalid".into())))
                .await
                .is_err()
        );
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
mod artifact_job_fixtures {
    use super::*;
    pub(super) fn service() -> (PathBuf, AnalyticsService) {
        let root = std::env::temp_dir().join(format!(
            "okx-analytics-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        let client = okx_api::OkxPublicClient::new(okx_api::OkxEnvironment::new(
            okx_api::Region::Global,
            false,
        ))
        .unwrap();
        let service = AnalyticsService::new(
            root.clone(),
            crate::market_bootstrap::MarketBootstrapper::new(client),
        )
        .unwrap();
        (root, service)
    }
    #[tokio::test(flavor = "current_thread")]
    async fn market_artifact_publication_and_cached_result_roundtrip() {
        let (root, s) = service();
        let bars = (0..100)
            .map(|i| replay::Bar {
                time: i * 60000,
                open: 100.0 + i as f64,
                high: 102.0 + i as f64,
                low: 99.0 + i as f64,
                close: 101.0 + i as f64,
                volume: 1.0,
                funding_rate: None,
            })
            .collect();
        s.sources
            .cache
            .load(
                cache::SourceKey {
                    source: "candles".into(),
                    instrument: "BTC-USDT".into(),
                    timeframe_ms: 60000,
                    start_ms: 0,
                    end_ms: 6000000,
                    closed_only: true,
                    schema: 1,
                },
                || async { Ok(bars) },
            )
            .await
            .unwrap();
        let request = MarketRequest {
            action: MarketAction::Screen,
            instruments: vec!["BTC-USDT".into()],
            universe: None,
            fields: vec!["returns".into()],
            timeframe_ms: 60000,
            start_ms: 0,
            end_ms: 6000000,
            limit: 50,
            cursor: None,
            parameters: Default::default(),
            filters: vec![],
            sort: None,
        };
        let result = s.market(request.clone(), None, None).await.unwrap();
        assert_eq!(result["returned_rows"], 1);
        assert_eq!(s.market(request, None, None).await.unwrap(), result);
        let page = s
            .research(ResearchRequest::Result {
                job_id: result["job_id"].as_str().unwrap().into(),
                cursor: None,
                limit: 1,
                view: ResearchResultView::Trades,
            })
            .await
            .unwrap();
        assert_eq!(page["rows"], result["rows"]);
        assert!(
            root.canonicalize()
                .unwrap()
                .starts_with(std::env::temp_dir().canonicalize().unwrap())
        );
        drop(s);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test(flavor = "current_thread")]
    async fn ordinary_market_cursors_resume_one_job_and_reject_other_queries() {
        let (root, service) = service();
        let instruments = ["BTC-USDT", "ETH-USDT", "SOL-USDT"];
        for instrument in instruments {
            let bars = (0..100)
                .map(|i| replay::Bar {
                    time: i * 60000,
                    open: 100.0 + i as f64,
                    high: 102.0 + i as f64,
                    low: 99.0 + i as f64,
                    close: 101.0 + i as f64,
                    volume: 1.0,
                    funding_rate: None,
                })
                .collect::<Vec<_>>();
            service
                .sources
                .cache
                .load(
                    cache::SourceKey {
                        source: "candles".into(),
                        instrument: instrument.into(),
                        timeframe_ms: 60000,
                        start_ms: 0,
                        end_ms: 6000000,
                        closed_only: true,
                        schema: 1,
                    },
                    || async { Ok(bars) },
                )
                .await
                .unwrap();
        }
        let mut query = MarketRequest {
            action: MarketAction::Screen,
            instruments: instruments.iter().map(|s| (*s).into()).collect(),
            universe: None,
            fields: vec!["returns".into()],
            timeframe_ms: 60000,
            start_ms: 0,
            end_ms: 6000000,
            limit: 1,
            cursor: None,
            parameters: Default::default(),
            filters: vec![],
            sort: None,
        };
        let first = service.market(query.clone(), None, None).await.unwrap();
        assert_eq!(first["rows"][0]["instrument"], instruments[0]);
        let job = first["job_id"].as_str().unwrap().to_owned();
        assert_eq!(first["next_cursor"], format!("{job}:1"));
        query.cursor = first["next_cursor"].as_str().map(str::to_owned);
        let second = service.market(query.clone(), None, None).await.unwrap();
        assert_eq!(second["rows"][0]["instrument"], instruments[1]);
        assert_eq!(second["job_id"], job);
        assert_eq!(second["next_cursor"], format!("{job}:2"));
        query.cursor = second["next_cursor"].as_str().map(str::to_owned);
        let third = service.market(query.clone(), None, None).await.unwrap();
        assert_eq!(third["rows"][0]["instrument"], instruments[2]);
        assert!(third["next_cursor"].is_null());
        assert_eq!(service.jobs.snapshot().len(), 1);
        query.fields = vec!["realized_volatility".into()];
        assert_eq!(
            service.market(query, None, None).await.unwrap_err(),
            "MARKET_CURSOR_REQUEST_MISMATCH"
        );
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn two_asset_batch_grid_has_independent_artifacts_and_cursors() {
        let (root, s) = service();
        for (symbol, direction) in [("BTC-USDT", 1.0), ("ETH-USDT", -1.0)] {
            let bars = (0..400)
                .map(|i| {
                    let close = 1000.0 + direction * i as f64;
                    replay::Bar {
                        time: i * 60000,
                        open: close,
                        high: close + 1.0,
                        low: close - 1.0,
                        close,
                        volume: 10.0,
                        funding_rate: None,
                    }
                })
                .collect();
            s.sources
                .cache
                .load(
                    cache::SourceKey {
                        source: "candles".into(),
                        instrument: symbol.into(),
                        timeframe_ms: 60000,
                        start_ms: 0,
                        end_ms: 24000000,
                        closed_only: true,
                        schema: 1,
                    },
                    || async { Ok(bars) },
                )
                .await
                .unwrap();
        }
        let request = ResearchRequest::Grid {
            idempotency_key: "two-assets-grid".into(),
            instruments: vec!["BTC-USDT".into(), "ETH-USDT".into()],
            timeframe_ms: 60000,
            start_ms: 0,
            end_ms: 24000000,
            strategy_id: "ema_cross_v1".into(),
            variants: vec![
                EmaParameters { fast: 2, slow: 5 },
                EmaParameters { fast: 3, slow: 8 },
            ],
            costs: ResearchCosts {
                maker_fee_bps: 0.0,
                taker_fee_bps: 1.0,
                slippage_bps: 1.0,
                funding_mode: FundingMode::ZeroAssumption,
            },
            data_mode: ResearchDataMode::Counterfactual,
            seed: 7,
        };
        assert!(request.valid());
        let job = s.research(request.clone()).await.unwrap();
        let id = job["id"].as_str().unwrap().to_owned();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let j = s.jobs.get(&id).unwrap();
                if j.state == jobs::State::Completed {
                    break;
                }
                assert_ne!(j.state, jobs::State::Failed, "{:?}", j.error);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await
            }
        })
        .await
        .unwrap();
        assert_eq!(s.research(request).await.unwrap()["id"], id);
        let result = s
            .research(ResearchRequest::Result {
                job_id: id.clone(),
                cursor: None,
                limit: 50,
                view: ResearchResultView::Trades,
            })
            .await
            .unwrap();
        assert_eq!(result["total_rows"], 2);
        assert_eq!(result["summary"]["semantics"], "independent_instruments");
        assert_ne!(
            result["rows"][0]["dataset_hash"],
            result["rows"][1]["dataset_hash"]
        );
        assert_ne!(
            result["rows"][0]["summary"]["pnl"],
            result["rows"][1]["summary"]["pnl"]
        );
        assert!(result["rows"][0]["grid"]["selected"].is_object());
        let trades = s
            .research(ResearchRequest::Result {
                job_id: id.clone(),
                cursor: Some("instrument:0:0".into()),
                limit: 1,
                view: ResearchResultView::Trades,
            })
            .await
            .unwrap();
        assert_eq!(trades["returned_rows"], 1);
        assert!(
            s.research(ResearchRequest::Result {
                job_id: id,
                cursor: Some("instrument:8:0".into()),
                limit: 1,
                view: ResearchResultView::Trades,
            })
            .await
            .is_err()
        );
        assert_eq!(s.jobs.snapshot().len(), 1);
        assert!(
            root.canonicalize()
                .unwrap()
                .starts_with(std::env::temp_dir().canonicalize().unwrap())
        );
        drop(s);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn c17_cursor_survives_catalog_restart_and_is_not_transferable() {
        let (root, service) = super::artifact_job_fixtures::service();
        let page = service
            .artifacts
            .publish_evidence(&vec![json!({"row":1}), json!({"row":2})])
            .unwrap()
            .0;
        let manifest = service
            .artifacts
            .publish_evidence(&json!({
                "trade_pages":[page],"total_trades":2
            }))
            .unwrap()
            .0;
        let id = "e".repeat(64);
        service
            .jobs
            .enqueue("c17-signed".into(), id.clone())
            .unwrap();
        service
            .jobs
            .update(&id, jobs::State::Completed, Some(manifest.clone()), None)
            .unwrap();
        let first = service
            .result_page(id.clone(), None, 1, ResearchResultView::Trades)
            .unwrap();
        let token = first["next_cursor"].as_str().unwrap().to_owned();
        assert!(token.starts_with("r1."));
        let decoded =
            cursor::Cursor::decode(&token, &service.jobs.cursor_signing_key().unwrap()).unwrap();
        assert_eq!(decoded.offset, 1);
        assert_eq!(
            service
                .result_page(
                    id.clone(),
                    Some(token.clone()),
                    1,
                    ResearchResultView::Equity
                )
                .unwrap_err(),
            "CURSOR_MISMATCH"
        );
        let other = "f".repeat(64);
        service
            .jobs
            .enqueue("c17-other".into(), other.clone())
            .unwrap();
        service
            .jobs
            .update(&other, jobs::State::Completed, Some(manifest), None)
            .unwrap();
        assert_eq!(
            service
                .result_page(other, Some(token.clone()), 1, ResearchResultView::Trades)
                .unwrap_err(),
            "CURSOR_MISMATCH"
        );
        let mut forged = token.clone().into_bytes();
        let last = forged.len() - 1;
        forged[last] = if forged[last] == b'A' { b'B' } else { b'A' };
        assert_eq!(
            service
                .result_page(
                    id.clone(),
                    Some(String::from_utf8(forged).unwrap()),
                    1,
                    ResearchResultView::Trades
                )
                .unwrap_err(),
            "INVALID_CURSOR"
        );
        let restarted = jobs::Jobs::open(root.join("research-catalog.sqlite")).unwrap();
        assert_eq!(
            restarted.cursor_signing_key().unwrap(),
            service.jobs.cursor_signing_key().unwrap()
        );
        let second = service
            .result_page(id, Some(token), 1, ResearchResultView::Trades)
            .unwrap();
        assert_eq!(second["rows"][0]["row"], 2);
        drop(restarted);
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn c17_empty_or_short_manifest_page_fails_without_unbounded_scan() {
        let (root, service) = super::artifact_job_fixtures::service();
        let page_id = service
            .artifacts
            .publish_evidence(&Vec::<Value>::new())
            .unwrap()
            .0;
        let manifest = service
            .artifacts
            .publish_evidence(&json!({
                "trade_pages":[page_id],"total_trades":2
            }))
            .unwrap()
            .0;
        let id = "d".repeat(64);
        service
            .jobs
            .enqueue("c17-empty".into(), id.clone())
            .unwrap();
        service
            .jobs
            .update(&id, jobs::State::Completed, Some(manifest.clone()), None)
            .unwrap();
        let err = service
            .result_page(id.clone(), None, 5, ResearchResultView::Trades)
            .unwrap_err();
        assert_eq!(err, "RESULT_PAGE_TRUNCATED");
        let token = cursor::Cursor::new(
            &id,
            &manifest,
            &manifest,
            ResearchResultView::Trades,
            2,
            None,
        )
        .encode(&service.jobs.cursor_signing_key().unwrap())
        .unwrap();
        let err = service
            .result_page(id, Some(token), 5, ResearchResultView::Trades)
            .unwrap_err();
        assert_eq!(err, "RESULT_CURSOR_OUT_OF_RANGE");
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn immutable_result_pages_and_restart_catalog() {
        let (root, s) = service();
        let data = s
            .artifacts
            .publish_evidence(&json!({"chunks":[]}))
            .unwrap()
            .0;
        let result = replay::ReplayResult {
            dataset_hash: "fixture-hash".into(),
            code_version: "fixture-code".into(),
            hash: "fixture-result".into(),
            summary: replay::Summary {
                pnl: 0.0,
                pnl_exact: "0".into(),
                fees_exact: "1".into(),
                funding_exact: "0".into(),
                max_drawdown: 0.0,
                sharpe: 0.0,
                trades: 120,
                turnover: 120.0,
                fees: 1.0,
                funding: 0.0,
                benchmark: 0.0,
                assumptions: vec!["COUNTERFACTUAL fixture".into()],
            },
            equity: (0..9000).map(|i| (i, 10000.0)).collect(),
            trades: (0..120)
                .map(|i| replay::Trade {
                    time: i,
                    side: 1,
                    price: 1.0,
                    quantity: 1.0,
                    fee: 0.01,
                    reason: "fixture".into(),
                })
                .collect(),
        };
        let artifact = publish_replay(&s.artifacts, &result, &data, None, &|| false).unwrap();
        let id = "a".repeat(64);
        s.jobs
            .enqueue("fixture-request".into(), id.clone())
            .unwrap();
        s.jobs
            .update(&id, jobs::State::Completed, Some(artifact.clone()), None)
            .unwrap();
        let page = s
            .research(ResearchRequest::Result {
                job_id: id.clone(),
                cursor: Some(
                    cursor::Cursor::new(
                        &id,
                        &artifact,
                        &artifact,
                        ResearchResultView::Trades,
                        47,
                        None,
                    )
                    .encode(&s.jobs.cursor_signing_key().unwrap())
                    .unwrap(),
                ),
                limit: 5,
                view: ResearchResultView::Trades,
            })
            .await
            .unwrap();
        assert_eq!(page["returned_rows"], 5);
        assert_eq!(page["total_rows"], 120);
        assert_eq!(page["rows"][0]["time"], 47);
        assert!(page["next_cursor"].as_str().unwrap().starts_with("r1."));
        let reloaded = jobs::Jobs::open(root.join("research-catalog.sqlite")).unwrap();
        assert_eq!(reloaded.get(&id).unwrap().state, jobs::State::Completed);
        assert!(
            reloaded
                .enqueue("fixture-request".into(), "b".repeat(64))
                .unwrap_err()
                .contains("IDEMPOTENCY_CONFLICT")
        );
        s.pin_job(&id, true).unwrap();
        assert_eq!(
            s.apply_retention(chrono::Utc::now().timestamp_millis() as u64 + 8 * 86400000)
                .unwrap(),
            0
        );
        s.pin_job(&id, false).unwrap();
        assert_eq!(
            s.apply_retention(chrono::Utc::now().timestamp_millis() as u64 + 8 * 86400000)
                .unwrap(),
            1
        );
        assert!(s.jobs.get(&id).unwrap().artifact.is_none());
        assert!(
            root.canonicalize()
                .unwrap()
                .starts_with(std::env::temp_dir().canonicalize().unwrap())
        );
        drop(reloaded);
        drop(s);
        std::fs::remove_dir_all(root).unwrap();
    }
}

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
mod universe_artifact_tests {
    use super::*;

    #[test]
    fn global_rank_cursor_uses_persisted_pages_without_refetch() {
        let (root, service) = super::artifact_job_fixtures::service();
        let scores = (0..57)
            .map(|i| json!({"instrument":format!("A{i:03}"),"metrics":{"returns":57-i}}))
            .collect::<Vec<_>>();
        let first = service
            .artifacts
            .publish_evidence(&scores[..50].to_vec())
            .unwrap()
            .0;
        let second = service
            .artifacts
            .publish_evidence(&scores[50..].to_vec())
            .unwrap()
            .0;
        let manifest = service
            .artifacts
            .publish_evidence(&json!({"row_pages":[first,second],"total_rows":57}))
            .unwrap()
            .0;
        let result = json!({"result_artifact":manifest,"total_rows":57,"returned_rows":12,
            "page_limit":12,"rows":scores[..12],"next_cursor":"12",
            "universe_page":{"global_ranking":true,"has_more":true,"page_count":12}});
        let page = global_rank_page(
            &service.artifacts,
            result.clone(),
            "48",
            "f".repeat(64).as_str(),
        )
        .unwrap();
        assert_eq!(page["rows"][0]["instrument"], "A048");
        assert_eq!(page["returned_rows"], 9);
        assert_eq!(page["next_cursor"], Value::Null);
        assert_eq!(page["universe_page"]["has_more"], false);
        assert_eq!(
            global_rank_page(&service.artifacts, result, "57", "f".repeat(64).as_str())
                .unwrap_err(),
            "GLOBAL_RANK_CURSOR_OUT_OF_RANGE"
        );
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn ordinary_market_cursor_reads_exact_persisted_pages() {
        let (root, service) = super::artifact_job_fixtures::service();
        let scores = (0..105)
            .map(|i| json!({"instrument":format!("I{i:03}")}))
            .collect::<Vec<_>>();
        let mut pages = Vec::new();
        for chunk in scores.chunks(50) {
            pages.push(service.artifacts.publish_evidence(&chunk).unwrap().0);
        }
        let manifest = service
            .artifacts
            .publish_evidence(&json!({"row_pages":pages,"total_rows":105}))
            .unwrap()
            .0;
        let job = "a".repeat(64);
        let first = market_result_with_context(
            json!({"kind":"market","result_artifact":manifest,"rows":scores[..13],
                "total_rows":105,"returned_rows":13,"next_cursor":"13"}),
            &job,
            None,
        )
        .unwrap();
        assert_eq!(first["next_cursor"], format!("{job}:13"));
        assert_eq!(first["page_limit"], 13);
        let next = global_rank_page(&service.artifacts, first, "49", &job).unwrap();
        assert_eq!(next["rows"][0]["instrument"], "I049");
        assert_eq!(next["rows"][1]["instrument"], "I050");
        assert_eq!(next["returned_rows"], 13);
        assert_eq!(next["next_cursor"], format!("{job}:62"));
        assert!(next["universe_page"].is_null());
        assert_eq!(
            global_rank_page(&service.artifacts, next, "105", &job).unwrap_err(),
            "MARKET_CURSOR_OUT_OF_RANGE"
        );
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn market_universe_metadata_persisted_with_final_artifact_and_queue() {
        let page = json!({
            "reference_generation":"generation-a",
            "next_after_instrument":"BTC-USDT-SWAP",
            "has_more":true,
            "global_ranking":false
        });
        let complete = market_result_with_context(
            json!({"kind":"market","rows":[],"total_rows":0}),
            "fixture-job",
            Some(&page),
        )
        .unwrap();
        assert_eq!(complete["universe_page"], page);
        assert_eq!(complete["job_id"], "fixture-job");
        let (root, service) = super::artifact_job_fixtures::service();
        let artifact = service.artifacts.publish_evidence(&complete).unwrap().0;
        let persisted: Value = service.artifacts.read_evidence_json(&artifact).unwrap();
        assert_eq!(
            persisted["universe_page"]["reference_generation"],
            "generation-a"
        );
        assert_eq!(
            persisted["universe_page"]["next_after_instrument"],
            "BTC-USDT-SWAP"
        );
        assert_eq!(
            with_universe_page(json!({"state":"queued"}), Some(&page)).unwrap()["universe_page"],
            page
        );
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }
}

fn research_capabilities() -> Value {
    // Report only what the current public product can actually execute.
    // HistoricalObserved remains unavailable until authenticated historical
    // venue mechanics + fees are captured, not merely a validation enum.
    json!({
        "schema":"okx.research.capabilities/v1",
        "quality":"fresh",
        "strategy_ids":["ema_cross_v1"],
        "supported_data_modes":["counterfactual"],
        "unavailable_data_modes":[{"mode":"historical_observed",
            "reason_code":"HISTORICAL_RULES_AND_FEES_UNAVAILABLE"}],
        "timeframes_ms":[60000,180000,300000,900000,1800000,3600000,14400000,86400000],
        "limits":{"max_instruments":8,"max_bars_total":1000000,"max_grid_variants":64,
            "max_result_page_rows":50,"max_heavy_concurrent_jobs":2},
        "result_views":["summary","trades","variants","equity"],
        "oos":{"grid_train_test_split":true,"selected_variant_from_train_only":true},
        "source_evidence":{"closed_candles_only":true,"no_forward_fill":true}
    })
}
#[cfg(test)]
mod research_capabilities_contract {
    use super::*;
    #[test]
    fn native_capabilities_do_not_advertise_unavailable_historical_mode() {
        let report = research_capabilities();
        assert_eq!(report["supported_data_modes"], json!(["counterfactual"]));
        assert_eq!(
            report["unavailable_data_modes"][0]["reason_code"],
            "HISTORICAL_RULES_AND_FEES_UNAVAILABLE"
        );
        assert_eq!(report["limits"]["max_grid_variants"], 64);
    }
}

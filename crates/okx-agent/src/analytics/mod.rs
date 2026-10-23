pub mod cache;
pub mod executor;
pub mod jobs;
mod market;
pub mod planner;
pub mod sources;
use futures_util::{StreamExt, stream};
use okx_protocol::analytics::*;
use okx_research::{
    ResearchArtifactStore,
    strategy_registry::{self as replay, Costs, Mechanics, ReplaySpec},
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
};
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
            jobs: jobs::Jobs::open(root.join("research-catalog.json"))?,
            artifacts: ResearchArtifactStore::at(root),
            heavy: Arc::new(tokio::sync::Semaphore::new(2)),
        };
        service.apply_retention(chrono::Utc::now().timestamp_millis().max(0) as u64)?;
        Ok(service)
    }
    pub async fn market(&self, request: MarketRequest) -> Result<Value, String> {
        if !request.valid() {
            return Err("INVALID_MARKET_REQUEST".into());
        }
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
        let hash = okx_research::canonical_sha256(&("market/398.1", &request, bucket))
            .map_err(|e| e.to_string())?;
        let hash = hash.strip_prefix("sha256:").unwrap_or(&hash).to_owned();
        let (job, new, cancel) = self.jobs.enqueue(format!("market-{hash}"), hash.clone())?;
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
            return Ok(json!(job));
        }
        let this = self.clone();
        let mut task = tokio::spawn(async move {
            this.jobs.update(&hash, jobs::State::Running, None, None)?;
            let mut outcome = this.market_execute(request, cancel.clone()).await;
            if let Ok(value) = &mut outcome {
                value
                    .as_object_mut()
                    .ok_or("INVALID_MARKET_RESULT")?
                    .insert("job_id".into(), json!(hash));
            }
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
                        .update(&hash, jobs::State::Completed, Some(artifact), None)?;
                }
                Err(e) => {
                    this.jobs.update(
                        &hash,
                        if cancel.load(Ordering::Acquire) {
                            jobs::State::Cancelled
                        } else {
                            jobs::State::Failed
                        },
                        None,
                        Some(e.clone()),
                    )?;
                }
            }
            outcome
        });
        match tokio::time::timeout(std::time::Duration::from_secs(2), &mut task).await {
            Ok(result) => result.map_err(|e| e.to_string())?,
            Err(_) => Ok(json!(job)),
        }
    }
    async fn market_execute(
        &self,
        request: MarketRequest,
        cancel: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Value, String> {
        if !request.valid() {
            return Err("INVALID_MARKET_REQUEST".into());
        }
        if request.instruments.is_empty()
            || request.instruments.len() > 100
            || request.limit == 0
            || request.limit > 50
        {
            return Err("MARKET_LIMIT:1..100 instruments,1..50 rows".into());
        }
        let plan = planner::AnalysisPlan::build(&request.fields, request.start_ms, request.end_ms)?;
        let (rows, correlation_matrix) = self.market_rows(&request, cancel.clone()).await?;
        let store = self.artifacts.clone();
        self.cpu.run(cancel,move |cancel| {
        if cancel.load(Ordering::Acquire){return Err("CANCELLED".into())}
        let offset = request
            .cursor
            .as_deref()
            .unwrap_or("0")
            .parse::<usize>()
            .map_err(|_| "INVALID_CURSOR")?;
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
    pub async fn research(&self, request: ResearchRequest) -> Result<Value, String> {
        if !request.valid() {
            return Err("INVALID_RESEARCH_REQUEST".into());
        }
        match request {
            ResearchRequest::Status { job_id } => {
                serde_json::to_value(self.jobs.get(&job_id)?).map_err(|e| e.to_string())
            }
            ResearchRequest::Cancel {
                job_id,
                idempotency_key,
            } => serde_json::to_value(self.jobs.cancel(&job_id, &idempotency_key)?)
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
        let artifact = j.artifact.ok_or("RESULT_NOT_READY")?;
        let mut value: Value = self
            .artifacts
            .read_evidence_json(&artifact)
            .map_err(|e| e.to_string())?;
        if let Some(id) = value.get("result_artifact").and_then(Value::as_str) {
            value = self
                .artifacts
                .read_evidence_json(id)
                .map_err(|e| e.to_string())?;
        }
        let mut page_cursor = cursor.clone();
        let mut instrument_prefix = None;
        if let Some(raw) = cursor.as_deref().filter(|c| c.starts_with("instrument:")) {
            let parts = raw.split(':').collect::<Vec<_>>();
            if parts.len() != 3 {
                return Err("INVALID_CURSOR".into());
            }
            let index = parts[1].parse::<u64>().map_err(|_| "INVALID_CURSOR")?;
            let row = value
                .get("instruments")
                .and_then(Value::as_array)
                .and_then(|rows| rows.iter().find(|r| r["index"].as_u64() == Some(index)))
                .ok_or("INVALID_CURSOR")?;
            let id = row["artifact"]
                .as_str()
                .ok_or("INSTRUMENT_RESULT_UNAVAILABLE")?;
            value = self
                .artifacts
                .read_evidence_json(id)
                .map_err(|e| e.to_string())?;
            page_cursor = Some(parts[2].into());
            instrument_prefix = Some(format!("instrument:{index}:"));
        }
        let metadata = json!({
            "job_id":job_id,"artifact":artifact,
            "summary":value.get("summary").or_else(||value.pointer("/test/summary")),
            "grid":value.get("grid").map(|g| json!({
                "selected":g.get("selected"),"train":g.get("train"),"split_index":g.get("split_index"),
                "selection_warning":g.get("selection_warning")
            })),
            "dataset_hash":value.get("dataset_hash"),
            "code_version":value.get("code_version"),
            "result_hash":value.get("hash")
        });
        if matches!(view, ResearchResultView::Summary) && value.get("instruments").is_none() {
            return Ok(json!({"view":"summary","metadata":metadata,"rows":[],"total_rows":0,
                "returned_rows":0,"next_cursor":null}));
        }
        if matches!(view, ResearchResultView::Variants | ResearchResultView::Equity) {
            let offset=page_cursor.as_deref().unwrap_or("0").parse::<usize>()
                .map_err(|_|"INVALID_CURSOR")?;
            let mut output=Vec::new();
            let total;
            if matches!(view, ResearchResultView::Variants) {
                let variants=value.pointer("/grid/variants").and_then(Value::as_array)
                    .ok_or("VARIANTS_UNAVAILABLE")?;
                total=variants.len();
                output.extend(variants.iter().skip(offset).take(limit as usize).cloned());
            } else {
                let pages=value.get("equity_curve_pages").and_then(Value::as_array)
                    .ok_or("EQUITY_RESULT_UNAVAILABLE")?;
                total=value.get("total_equity_points").and_then(Value::as_u64)
                    .ok_or("EQUITY_RESULT_UNAVAILABLE")? as usize;
                let mut position=offset;
                while output.len()<limit as usize && position<total {
                    let id=pages.get(position/4096).and_then(Value::as_str)
                        .ok_or("EQUITY_PAGE_MISSING")?;
                    let points:Vec<Value>=self.artifacts.read_evidence_json(id)
                        .map_err(|e|e.to_string())?;
                    if position%4096>=points.len(){return Err("EQUITY_PAGE_MISSING".into())}
                    let take=(limit as usize-output.len()).min(points.len()-position%4096);
                    output.extend(points.into_iter().skip(position%4096).take(take));
                    position+=take;
                }
            }
            while output.len()>1 && serde_json::to_vec(&output)
                .map_err(|e|e.to_string())?.len()>6500{output.pop();}
            let next=if offset+output.len()<total {
                Some(format!("{}{}",instrument_prefix.as_deref().unwrap_or(""),offset+output.len()))
            }else{None};
            return Ok(json!({"view":if matches!(view,ResearchResultView::Equity){"equity"}else{"variants"},
                "metadata":metadata,"rows":output,"returned_rows":output.len(),
                "total_rows":total,"next_cursor":next}));
        }
        let offset = page_cursor
            .as_deref()
            .unwrap_or("0")
            .parse::<usize>()
            .map_err(|_| "INVALID_CURSOR")?;
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
        let mut page = Vec::new();
        let mut position = offset;
        while page.len() < limit as usize && position < total {
            let id = pages
                .get(position / 50)
                .and_then(Value::as_str)
                .ok_or("RESULT_PAGE_MISSING")?;
            let rows: Vec<Value> = self
                .artifacts
                .read_evidence_json(id)
                .map_err(|e| e.to_string())?;
            let take = (limit as usize - page.len()).min(rows.len() - position % 50);
            page.extend(rows.into_iter().skip(position % 50).take(take));
            position += take;
        }
        while page.len() > 1 && serde_json::to_vec(&page).map_err(|e| e.to_string())?.len() > 6500 {
            page.pop();
        }
        Ok(
            json!({"view":"trades","metadata":metadata,"assumptions":value.pointer("/summary/assumptions").or_else(||value.pointer("/test/summary/assumptions")),"job_id":job_id,"artifact":artifact,"summary":value.get("summary").or_else(||value.pointer("/test/summary")),"rows":page,"returned_rows":page.len(),"total_rows":total,"next_cursor":if offset+page.len()<total{Some(format!("{}{}",instrument_prefix.as_deref().unwrap_or(""),offset+page.len()))}else{None}}),
        )
    }
    async fn submit(&self, request: ResearchRequest) -> Result<Value, String> {
        let hash = okx_research::canonical_sha256(&("analytics-v1", &request))
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
        let (job, new, cancel) = self.jobs.enqueue(key, hash.clone())?;
        if !new {
            return Ok(json!(job));
        }
        let this = self.clone();
        tokio::spawn(async move {
            let outcome = async {
                let _heavy = this
                    .heavy
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| "HEAVY_JOBS_LIMIT:2")?;
                this.jobs.update(&hash, jobs::State::Running, None, None)?;
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
                    this.jobs.update(&hash,jobs::State::Running,Some(manifest),None)?;
                }
                this.jobs.get(&hash)?.artifact.ok_or_else(||"EMPTY_BATCH".into())
            }
            .await;
            match outcome {
                Ok(a) => {
                    let _ = this
                        .jobs
                        .update(&hash, jobs::State::Completed, Some(a), None);
                }
                Err(e) => {
                    let state = if cancel.load(Ordering::Acquire) {
                        jobs::State::Cancelled
                    } else {
                        jobs::State::Failed
                    };
                    let _ = this.jobs.update(
                        &hash,
                        state,
                        this.jobs.get(&hash).ok().and_then(|j| j.artifact),
                        Some(e),
                    );
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
        let mut bars = tokio::select! {loaded=self.sources.candles(&instruments[0],ms,start,end)=>loaded?,_=executor::cancelled(cancel.clone())=>return Err("CANCELLED".into())};
        let expected_start = start.div_ceil(ms).saturating_mul(ms);
        let expected_end = end / ms * ms;
        if bars.first().is_none_or(|b| b.time > expected_start)
            || bars
                .last()
                .is_none_or(|b| b.time.saturating_add(ms) < expected_end)
        {
            return Err("CANDLE_RANGE_INCOMPLETE".into());
        }
        if bars.windows(2).any(|pair| pair[1].time != pair[0].time.saturating_add(ms)) {
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
                    let(result,grid)=if is_grid{let g=replay::grid(&bars,&spec,&variants,&cancelled)?;(g.test,Some(json!({"selected":g.selected,"split_index":g.split_index,"train":g.train,"variants":g.variants,"selection_warning":g.selection_warning})))}else{(replay::replay(&bars,&spec,first.slow,bars.len(),&cancelled)?,None)};
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
        let snapshot = self.jobs.snapshot();
        if snapshot
            .iter()
            .any(|j| matches!(j.state, jobs::State::Queued | jobs::State::Running))
        {
            return Ok(0);
        }
        let expired = snapshot
            .iter()
            .filter(|j| {
                !j.pinned
                    && j.created_ms.saturating_add(7 * 86400000) < now_ms
                    && j.artifact.is_some()
            })
            .collect::<Vec<_>>();
        let mut protected = std::collections::BTreeSet::new();
        for j in &snapshot {
            if !expired.iter().any(|old| old.id == j.id)
                && let Some(id) = &j.artifact
            {
                self.artifact_graph(id, &mut protected)?;
            }
        }
        let mut garbage = std::collections::BTreeSet::new();
        for j in &expired {
            self.artifact_graph(j.artifact.as_deref().unwrap(), &mut garbage)?;
        }
        for id in garbage.difference(&protected) {
            self.artifacts
                .remove_evidence(id)
                .map_err(|e| e.to_string())?;
        }
        for j in &expired {
            self.jobs.update(
                &j.id,
                j.state.clone(),
                None,
                Some("RESULT_EXPIRED_BY_7_DAY_RETENTION".into()),
            )?;
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
            self.sources.candles(instrument, timeframe_ms, start, end),
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
mod artifact_job_fixtures {
    use super::*;
    fn service() -> (PathBuf, AnalyticsService) {
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
        let result = s.market(request.clone()).await.unwrap();
        assert_eq!(result["returned_rows"], 1);
        assert_eq!(s.market(request).await.unwrap(), result);
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
            .update(&id, jobs::State::Completed, Some(artifact), None)
            .unwrap();
        let page = s
            .research(ResearchRequest::Result {
                job_id: id.clone(),
                cursor: Some("47".into()),
                limit: 5,
                view: ResearchResultView::Trades,
            })
            .await
            .unwrap();
        assert_eq!(page["returned_rows"], 5);
        assert_eq!(page["total_rows"], 120);
        assert_eq!(page["rows"][0]["time"], 47);
        assert_eq!(page["next_cursor"], "52");
        let reloaded = jobs::Jobs::open(root.join("research-catalog.json")).unwrap();
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
        std::fs::remove_dir_all(root).unwrap();
    }
}

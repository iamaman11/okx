use super::*;
use okx_analysis::operators::{Operator, registry, scalar};
use replay::Bar;
struct Loaded {
    instrument: String,
    bars: Result<Arc<Vec<Bar>>, String>,
    ticker: Option<Result<okx_api::PublicTicker, String>>,
    funding: Option<Result<Vec<(u64, f64)>, String>>,
    oi: Option<Result<Vec<f64>, String>>,
    excluded: Option<usize>,
}

/// A public rate defer is transient source capacity, not durable analytical evidence.
fn market_source_rate_deferred(loaded: &[Loaded]) -> bool {
    let deferred =
        |error: Option<&String>| jobs::is_transient_rate_defer(error.map(String::as_str));
    loaded.iter().any(|row| {
        deferred(row.bars.as_ref().err())
            || deferred(row.ticker.as_ref().and_then(|value| value.as_ref().err()))
            || deferred(row.funding.as_ref().and_then(|value| value.as_ref().err()))
            || deferred(row.oi.as_ref().and_then(|value| value.as_ref().err()))
    })
}

impl AnalyticsService {
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
    pub(super) async fn market_rows(
        &self,
        request: &MarketRequest,
        cancel: Arc<std::sync::atomic::AtomicBool>,
        global_rank: bool,
    ) -> Result<(Vec<Value>, Option<Value>), String> {
        let plan = planner::AnalysisPlan::compile_market(request)?;
        let need_candles = plan
            .sources
            .contains(&okx_analysis::query_plan::Source::Candles);
        let count = (request.end_ms - request.start_ms) / request.timeframe_ms;
        if need_candles && count.saturating_mul(request.instruments.len() as u64) > 1_000_000 {
            return Err("LIMIT_TOTAL_BARS:1000000".into());
        }
        let loading = stream::iter(request.instruments.iter().cloned().map(|instrument| {
            let sources = self.sources.clone();
            let fields = request.fields.clone();
            let (start, end, ms) = (request.start_ms, request.end_ms, request.timeframe_ms);
            async move {
                let bars = if need_candles {
                    sources
                        .candles(&instrument, ms, start, end, global_rank)
                        .await
                } else {
                    Ok(Arc::new(Vec::new()))
                };
                let key = cache::SourceKey {
                    source: "candles".into(),
                    instrument: instrument.clone(),
                    timeframe_ms: ms,
                    start_ms: start,
                    end_ms: end.min(chrono::Utc::now().timestamp_millis().max(0) as u64 / ms * ms),
                    closed_only: true,
                    schema: 1,
                };
                let excluded = sources.excluded.lock().unwrap().get(&key).copied();
                let ticker = if fields.iter().any(|f| f == "spread_bps") {
                    let _p = sources.cache.permit().await;
                    Some(
                        sources
                            .bootstrap
                            .analytics_ticker(&instrument)
                            .await
                            .map_err(|e| e.to_string()),
                    )
                } else {
                    None
                };
                let funding = if fields.iter().any(|f| f == "funding_sum") {
                    Some(sources.funding(&instrument, start, end).await)
                } else {
                    None
                };
                let oi = if fields.iter().any(|f| f == "oi_change") {
                    let _p = sources.cache.permit().await;
                    Some(
                        async {
                            let period = sources::timeframe(ms)?;
                            let raw = sources
                                .bootstrap
                                .analytics_oi(&instrument, period)
                                .await
                                .map_err(|e| e.to_string())?;
                            let mut rows = std::collections::BTreeMap::new();
                            for r in raw {
                                let t = r.ts.parse::<u64>().map_err(|e| e.to_string())?;
                                if t >= start && t < end {
                                    let v = r.oi.parse::<f64>().map_err(|e| e.to_string())?;
                                    if !v.is_finite() {
                                        return Err("NONFINITE_OI".into());
                                    }
                                    rows.insert(t, v);
                                }
                            }
                            if rows
                                .first_key_value()
                                .is_none_or(|(t, _)| *t > start.saturating_add(ms))
                                || rows
                                    .last_key_value()
                                    .is_none_or(|(t, _)| t.saturating_add(ms) < end)
                            {
                                return Err("OI_RANGE_INCOMPLETE".into());
                            }
                            Ok(rows.into_values().collect())
                        }
                        .await,
                    )
                } else {
                    None
                };
                Loaded {
                    instrument,
                    bars,
                    ticker,
                    funding,
                    oi,
                    excluded,
                }
            }
        }))
        .buffered(4)
        .collect::<Vec<_>>();
        let loaded = tokio::select! {data=loading=>data,_=super::executor::cancelled(cancel.clone())=>return Err("CANCELLED".into())};
        if market_source_rate_deferred(&loaded) {
            // No immutable partial artifact: Jobs::enqueue requeues this same request.
            return Err("OKX rate/backpressure defer: MARKET_SOURCE_RETRYABLE".into());
        }
        if global_rank
            && need_candles
            && loaded.iter().any(|row| {
                !row.bars.as_ref().is_ok_and(|bars| {
                    complete_rank_history(
                        bars,
                        request.start_ms,
                        request.end_ms,
                        request.timeframe_ms,
                    )
                })
            })
        {
            return Err("GLOBAL_RANK_CANDLE_COVERAGE_INCOMPLETE".into());
        }
        let request = request.clone();
        self.cpu
            .run(cancel, move |cancel| {
                let matrix = if matches!(request.action, MarketAction::Compare)
                    && request
                        .fields
                        .iter()
                        .any(|field| field == "pearson_correlation")
                {
                    if loaded.len() > 8 {
                        return Err("CORRELATION_MATRIX_LIMIT:8".into());
                    }
                    Some(pairwise_matrix(&loaded, request.timeframe_ms))
                } else {
                    None
                };
                let peer = loaded
                    .first()
                    .and_then(|row| row.bars.as_ref().ok())
                    .cloned();
                let mut rows = Vec::new();
                for row in loaded {
                    if cancel.load(Ordering::Acquire) {
                        return Err("CANCELLED".into());
                    }
                    rows.push(compute_row(row, &request, peer.as_deref())?);
                }
                if global_rank {
                    // Never drop deficient candidates via a metric filter and
                    // pretend the surviving rows prove a full-venue ranking.
                    sort_market_rows(&mut rows, request.sort.as_ref(), true)?;
                }
                for filter in &request.filters {
                    rows.retain(|row| {
                        row["metrics"][&filter.field].as_f64().is_some_and(|v| {
                            filter.min.is_none_or(|min| v >= min)
                                && filter.max.is_none_or(|max| v <= max)
                        })
                    });
                }
                if !global_rank {
                    sort_market_rows(&mut rows, request.sort.as_ref(), false)?;
                }
                Ok((rows, matrix))
            })
            .await
    }
}
/// Full-universe scores require identical *closed* time coverage for all candidates.
fn complete_rank_history(bars: &[Bar], start: u64, end: u64, step: u64) -> bool {
    okx_analysis::time_series::ClosedRange::strict(start, end, step)
        .is_ok_and(|window| window.coverage(bars.iter().map(|bar| bar.time)).complete)
}
/// Rank is globally valid only when every candidate has complete evidence.
fn sort_market_rows(
    rows: &mut [Value],
    sort: Option<&MarketSort>,
    global_rank: bool,
) -> Result<(), String> {
    if global_rank
        && (sort.is_none()
            || rows.iter().any(|row| {
                row["quality"] != "complete"
                    || sort.is_some_and(|s| {
                        row["metrics"][&s.field]
                            .as_f64()
                            .is_none_or(|n| !n.is_finite())
                    })
            }))
    {
        return Err("GLOBAL_RANK_SOURCE_INCOMPLETE".into());
    }
    if let Some(sort) = sort {
        rows.sort_by(|a, b| {
            let order = a["metrics"][&sort.field]
                .as_f64()
                .partial_cmp(&b["metrics"][&sort.field].as_f64())
                .unwrap_or(std::cmp::Ordering::Equal);
            let primary = if sort.descending {
                order.reverse()
            } else {
                order
            };
            primary.then_with(|| a["instrument"].as_str().cmp(&b["instrument"].as_str()))
        });
    }
    Ok(())
}
fn compute_row(
    row: Loaded,
    request: &MarketRequest,
    peer: Option<&Vec<Bar>>,
) -> Result<Value, String> {
    let mut gaps = Vec::new();
    let bars = match row.bars {
        Ok(b) => b,
        Err(e) => {
            gaps.push(e);
            Arc::new(Vec::new())
        }
    };
    let close_column =
        okx_analysis::operators::NumericColumn::from_values(bars.iter().map(|b| b.close));
    let close = &close_column.values;
    let high = bars.iter().map(|b| b.high).collect::<Vec<_>>();
    let low = bars.iter().map(|b| b.low).collect::<Vec<_>>();
    let volume = bars.iter().map(|b| b.volume).collect::<Vec<_>>();
    let candle_coverage = okx_analysis::time_series::ClosedRange::strict(
        request.start_ms,
        request.end_ms,
        request.timeframe_ms,
    )
    .ok()
    .map(|range| range.coverage(bars.iter().map(|bar| bar.time)));
    let incomplete_candles = candle_coverage
        .as_ref()
        .is_none_or(|coverage| !coverage.complete);
    if !bars.is_empty() && incomplete_candles {
        gaps.push("CANDLE_RANGE_INCOMPLETE_OR_GAPPED".into());
        // Preserve the precise public reason alongside the new typed evidence.
        if bars
            .windows(2)
            .any(|pair| pair[0].time.checked_add(request.timeframe_ms) != Some(pair[1].time))
        {
            gaps.push("CANDLE_INTERIOR_GAP".into());
        }
    }
    let mut metrics = serde_json::Map::new();
    let mut units = serde_json::Map::new();
    for field in &request.fields {
        let op: Operator = serde_json::from_value(json!(field)).map_err(|e| e.to_string())?;
        let param =
            |name: &str, default: f64| request.parameters.get(name).copied().unwrap_or(default);
        let value = match op {
            Operator::SpreadBps => {
                row.ticker
                    .as_ref()
                    .and_then(|t| t.as_ref().ok())
                    .and_then(|t| {
                        let bid = t.bid_price.parse::<f64>().ok()?;
                        let ask = t.ask_price.parse::<f64>().ok()?;
                        (bid.is_finite() && ask.is_finite() && bid > 0.0 && ask >= bid)
                            .then_some((ask - bid) / ((ask + bid) / 2.0) * 10000.0)
                    })
            }
            Operator::FundingSum => row
                .funding
                .as_ref()
                .and_then(|v| v.as_ref().ok())
                .map(|v| v.iter().map(|(_, v)| v).sum()),
            Operator::PearsonCorrelation => peer.and_then(|peer| {
                let a = log_returns(&bars, request.timeframe_ms)?;
                let b = log_returns(peer, request.timeframe_ms)?;
                okx_analysis::operators::correlation(&a, &b)
            }),
            Operator::VolumeChange => {
                scalar(op, close, &high, &low, &volume, 14, request.timeframe_ms)?
            }
            Operator::OiChange => row
                .oi
                .as_ref()
                .and_then(|r| r.as_ref().ok())
                .and_then(|r| r.first().zip(r.last()))
                .filter(|(a, _)| **a > 0.0)
                .map(|(a, b)| b / a - 1.0),
            Operator::PositionSize => {
                let aux = [
                    param("budget", f64::NAN),
                    param("price", f64::NAN),
                    param("stop_distance", f64::NAN),
                ];
                if aux.iter().any(|v| v.is_nan()) {
                    None
                } else {
                    scalar(op, &[1.0], &[], &[], &aux, 14, request.timeframe_ms)?
                }
            }
            Operator::CostEstimate => {
                let aux = [
                    param("notional", f64::NAN),
                    param("fee_bps", f64::NAN),
                    param("slippage_bps", f64::NAN),
                ];
                if aux.iter().any(|v| v.is_nan()) {
                    None
                } else {
                    scalar(op, &[1.0], &[], &[], &aux, 14, request.timeframe_ms)?
                }
            }
            _ => {
                let period = match op {
                    Operator::Ema => param("ema_period", 14.0),
                    Operator::Atr => param("atr_period", 14.0),
                    Operator::Rsi => param("rsi_period", 14.0),
                    _ => 14.0,
                };
                scalar(
                    op,
                    close,
                    &high,
                    &low,
                    &[],
                    period as usize,
                    request.timeframe_ms,
                )?
            }
        };
        let value = if incomplete_candles
            && !matches!(
                op,
                Operator::SpreadBps
                    | Operator::FundingSum
                    | Operator::OiChange
                    | Operator::PositionSize
                    | Operator::CostEstimate
            ) {
            None
        } else {
            value
        };
        if value.is_none() {
            gaps.push(format!("MISSING_INPUT_OR_MINIMUM_SAMPLES:{field}"));
        }
        metrics.insert(field.clone(), value.map_or(Value::Null, |v| json!(v)));
        units.insert(field.clone(), json!(registry(op).unit));
    }
    Ok(
        json!({"instrument":row.instrument,"metrics":metrics,"units":units,"parameters":{"ema_period":request.parameters.get("ema_period").unwrap_or(&14.0),"atr_period":request.parameters.get("atr_period").unwrap_or(&14.0),"rsi_period":request.parameters.get("rsi_period").unwrap_or(&14.0)},"source_start_ms":bars.first().map(|b|b.time),"source_end_ms":bars.last().map(|b|b.time),"unclosed_candles_excluded":row.excluded,"candle_coverage":candle_coverage,"quality":if gaps.is_empty(){"complete"}else{"partial"},"gaps":gaps}),
    )
}

/// Closed-bar log returns indexed by the *later* candle. Missing, duplicate,
/// out-of-order, nonpositive and nonfinite prices invalidate the series.
fn log_returns(bars: &[Bar], timeframe_ms: u64) -> Option<Vec<(u64, f64)>> {
    if bars.len() < 2 || timeframe_ms == 0 {
        return None;
    }
    let mut out = Vec::with_capacity(bars.len() - 1);
    for pair in bars.windows(2) {
        if pair[1].time != pair[0].time.checked_add(timeframe_ms)?
            || !pair[0].close.is_finite()
            || !pair[1].close.is_finite()
            || pair[0].close <= 0.0
            || pair[1].close <= 0.0
        {
            return None;
        }
        out.push((pair[1].time, (pair[1].close / pair[0].close).ln()));
    }
    Some(out)
}
fn aligned_samples(left: &[(u64, f64)], right: &[(u64, f64)]) -> usize {
    let (mut a, mut b, mut count) = (0, 0, 0);
    while a < left.len() && b < right.len() {
        match left[a].0.cmp(&right[b].0) {
            std::cmp::Ordering::Less => a += 1,
            std::cmp::Ordering::Greater => b += 1,
            std::cmp::Ordering::Equal => {
                count += 1;
                a += 1;
                b += 1;
            }
        }
    }
    count
}
fn pairwise_matrix(loaded: &[Loaded], timeframe_ms: u64) -> Value {
    let series = loaded
        .iter()
        .map(|row| {
            row.bars
                .as_ref()
                .ok()
                .and_then(|bars| log_returns(bars, timeframe_ms))
        })
        .collect::<Vec<_>>();
    let mut values = Vec::with_capacity(loaded.len());
    let mut counts = Vec::with_capacity(loaded.len());
    let mut complete = true;
    for left in &series {
        let mut row = Vec::with_capacity(series.len());
        let mut samples = Vec::with_capacity(series.len());
        for right in &series {
            let (correlation, n) = match (left, right) {
                (Some(a), Some(b)) => (
                    okx_analysis::operators::correlation(a, b),
                    aligned_samples(a, b),
                ),
                _ => (None, 0),
            };
            if correlation.is_none() {
                complete = false;
            }
            row.push(correlation);
            samples.push(n);
        }
        values.push(row);
        counts.push(samples);
    }
    json!({"basis":"aligned_closed_bar_log_returns","instruments":loaded.iter()
        .map(|r|r.instrument.clone()).collect::<Vec<_>>(),"values":values,
        "sample_counts":counts,"quality":if complete{"complete"}else{"partial"}})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_market_source_defer_is_not_cacheable() {
        let mut rate_limited = row();
        rate_limited.bars =
            Err("public OKX market API error: OKX rate/backpressure defer: LocalBudget".into());
        assert!(market_source_rate_deferred(&[rate_limited]));
        assert!(!market_source_rate_deferred(&[row()]));
        let mut invalid = row();
        invalid.bars = Err("CANDLE_INTERIOR_GAP".into());
        assert!(!market_source_rate_deferred(&[invalid]));
    }
    #[test]
    fn correlation_3x3_log_returns() {
        let mut series = Vec::new();
        for (k, id) in ["BTC-USDT", "ETH-USDT", "SOL-USDT"].iter().enumerate() {
            let mut price = 100.0;
            let bars = (0..100)
                .map(|i| {
                    if i > 0 {
                        price *= (0.002
                            + ((i % 7) as f64 - 3.0) * 0.0007 * if k == 2 { -1.0 } else { 1.0 })
                        .exp();
                    }
                    Bar {
                        time: i * 60000,
                        open: price,
                        high: price * 1.01,
                        low: price * 0.99,
                        close: price,
                        volume: 1.0,
                        funding_rate: None,
                    }
                })
                .collect();
            series.push(Loaded {
                instrument: id.to_string(),
                bars: Ok(Arc::new(bars)),
                ticker: None,
                funding: None,
                oi: None,
                excluded: None,
            });
        }
        let m = pairwise_matrix(&series, 60000);
        assert_eq!(m["basis"], "aligned_closed_bar_log_returns");
        assert_eq!(m["instruments"].as_array().unwrap().len(), 3);
        assert_eq!(m["sample_counts"][1][2], 99);
        assert_eq!(m["values"][0][1], m["values"][1][0]);
        assert_eq!(m["values"][1][2], m["values"][2][1]);
        assert!(m["values"][0][0].as_f64().unwrap() > 0.99);
        assert!(m["values"][0][2].as_f64().unwrap() < 0.0);
    }
    #[test]
    fn interior_gap_never_imputed() {
        let mut missing = row();
        Arc::make_mut(missing.bars.as_mut().unwrap()).remove(43);
        let v = compute_row(missing, &request(vec!["returns"]), None).unwrap();
        assert_eq!(v["quality"], "partial");
        assert_eq!(v["metrics"]["returns"], Value::Null);
        assert!(
            v["gaps"]
                .as_array()
                .unwrap()
                .contains(&json!("CANDLE_INTERIOR_GAP"))
        );
    }

    fn request(fields: Vec<&str>) -> MarketRequest {
        serde_json::from_value(json!({"action":"screen","instruments":["BTC-USDT"],"fields":fields,"timeframe_ms":60000,"start_ms":0,"end_ms":6000000})).unwrap()
    }
    fn row() -> Loaded {
        Loaded {
            instrument: "BTC-USDT".into(),
            bars: Ok(Arc::new(
                (0..100)
                    .map(|i| Bar {
                        time: i * 60000,
                        open: 100.0,
                        high: 102.0,
                        low: 99.0,
                        close: 100.0 + i as f64,
                        volume: 1.0,
                        funding_rate: None,
                    })
                    .collect(),
            )),
            ticker: None,
            funding: None,
            oi: None,
            excluded: Some(1),
        }
    }
    #[test]
    fn new_downside_operator_uses_existing_market_plan_and_projection() {
        let query = request(vec!["downside_volatility"]);
        assert!(query.valid());
        let plan =
            planner::AnalysisPlan::build(&query.fields, query.start_ms, query.end_ms).unwrap();
        assert_eq!(plan.sources, std::collections::BTreeSet::from(["candles"]));
        let output = compute_row(row(), &query, None).unwrap();
        assert_eq!(output["metrics"]["downside_volatility"], 0.0);
        assert_eq!(output["units"]["downside_volatility"], "annualized_ratio");
        let mut missing = row();
        missing.bars = Ok(Arc::new(Vec::new()));
        let output = compute_row(missing, &query, None).unwrap();
        assert_eq!(output["quality"], "partial");
        assert!(output["metrics"]["downside_volatility"].is_null());
    }
    #[test]
    fn unused_missing_source_does_not_degrade() {
        let r = compute_row(row(), &request(vec!["returns"]), None).unwrap();
        assert_eq!(r["quality"], "complete");
        assert_eq!(r["metrics"]["returns"], json!(0.99));
    }
    #[test]
    fn closed_candle_filter_preserves_quality() {
        let r = compute_row(row(), &request(vec!["returns"]), None).unwrap();
        assert_eq!(r["unclosed_candles_excluded"], 1);
        assert_eq!(r["quality"], "complete");
    }
    #[test]
    fn missing_requested_source_is_partial_not_zero() {
        let r = compute_row(row(), &request(vec!["spread_bps"]), None).unwrap();
        assert_eq!(r["quality"], "partial");
        assert!(r["metrics"]["spread_bps"].is_null());
    }
}
#[cfg(test)]
mod performance_fixtures {
    use super::*;
    #[test]
    fn prepared_screen_8x100_latency_and_response_budget() {
        let request:MarketRequest=serde_json::from_value(json!({"action":"screen","instruments":["BTC-USDT"],"fields":["returns","realized_volatility","spread_bps"],"timeframe_ms":60000,"start_ms":0,"end_ms":6000000})).unwrap();
        let run = || {
            let mut rows = Vec::new();
            for symbol in 0..8 {
                let bars = (0..100)
                    .map(|i| Bar {
                        time: i * 60000,
                        open: 100.0,
                        high: 102.0,
                        low: 99.0,
                        close: 100.0 + i as f64,
                        volume: 1.0,
                        funding_rate: None,
                    })
                    .collect();
                let ticker: okx_api::PublicTicker = serde_json::from_value(
                    json!({"instId":format!("fixture-{symbol}"),"bidPx":"100","askPx":"100.1"}),
                )
                .unwrap();
                rows.push(
                    compute_row(
                        Loaded {
                            instrument: format!("fixture-{symbol}"),
                            bars: Ok(Arc::new(bars)),
                            ticker: Some(Ok(ticker)),
                            funding: None,
                            oi: None,
                            excluded: Some(1),
                        },
                        &request,
                        None,
                    )
                    .unwrap(),
                );
            }
            serde_json::to_vec(
                &json!({"rows":rows,"returned_rows":8,"total_rows":8,"next_cursor":null}),
            )
            .unwrap()
        };
        for _ in 0..5 {
            run();
        }
        let mut times = Vec::new();
        let mut bytes = 0;
        for _ in 0..30 {
            let t = std::time::Instant::now();
            bytes = run().len();
            times.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        times.sort_by(f64::total_cmp);
        assert!(times[28] <= 1000.0);
        assert!(bytes <= 8192);
        println!(
            "ANALYTICS_PERF_JSON:{}",
            json!({"screen_8x100":{"build":if cfg!(debug_assertions){"debug"}else{"release"},"stage":"native fixture compute/projection/serialization; HTTP and physical MCP separate","warmups":5,"measurements":30,"p50_ms":times[14],"p95_ms":times[28],"max_ms":times[29],"summary_bytes":bytes}})
        );
    }
}

#[cfg(test)]
mod global_rank_tests {
    use super::*;
    #[test]
    fn global_rank_rejects_first_last_and_interior_candle_gaps() {
        let bar = |t| Bar {
            time: t,
            open: 1.0,
            high: 1.0,
            low: 1.0,
            close: 1.0,
            volume: 1.0,
            funding_rate: None,
        };
        let full = vec![bar(0), bar(900_000), bar(1_800_000)];
        assert!(complete_rank_history(&full, 0, 2_700_000, 900_000));
        assert!(!complete_rank_history(&full[1..], 0, 2_700_000, 900_000));
        assert!(!complete_rank_history(&full[..2], 0, 2_700_000, 900_000));
        assert!(!complete_rank_history(
            &[bar(0), bar(1_800_000)],
            0,
            2_700_000,
            900_000
        ));
    }
    #[test]
    fn sorted_global_rank_has_stable_ties_and_no_page_local_shortcut() {
        let mut rows = vec![
            json!({"instrument":"AA","metrics":{"returns":0.05},"quality":"complete"}),
            json!({"instrument":"ZZ","metrics":{"returns":0.09},"quality":"complete"}),
            json!({"instrument":"BB","metrics":{"returns":0.09},"quality":"complete"}),
            json!({"instrument":"CC","metrics":{"returns":-0.02},"quality":"complete"}),
        ];
        let sort = MarketSort {
            field: "returns".into(),
            descending: true,
        };
        sort_market_rows(&mut rows, Some(&sort), true).unwrap();
        assert_eq!(
            rows.iter()
                .map(|r| r["instrument"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["BB", "ZZ", "AA", "CC"]
        );
        rows[3]["quality"] = json!("partial");
        assert_eq!(
            sort_market_rows(&mut rows, Some(&sort), true).unwrap_err(),
            "GLOBAL_RANK_SOURCE_INCOMPLETE"
        );
    }
}

use super::*;
use okx_analysis::operators::{Operator, registry, scalar};
use replay::Bar;
use serde::{Serialize, de::DeserializeOwned, ser::SerializeSeq};
use sha2::Digest;
use std::{cell::Cell, io::Write, marker::PhantomData};
use tracing::Instrument;
#[derive(Clone)]
struct Loaded {
    instrument: String,
    bars: Result<Arc<Vec<Bar>>, String>,
    ticker: Option<Result<okx_research_protocol::research_ipc::ResearchTicker, String>>,
    funding: Option<Result<Vec<(u64, f64)>, String>>,
    // Keep OI event timestamps; endpoint values alone cannot prove coverage.
    oi: Option<Result<Vec<(u64, f64)>, String>>,
    excluded: Option<usize>,
}

#[path = "market_replay_proof.rs"]
mod replay_proof;
#[cfg(test)]
#[path = "tests/market_source_evidence_tests.rs"]
mod source_evidence_tests;

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

// Completed market jobs survive Worker releases. Each changed result contract
// needs a new namespace; older immutable jobs remain readable by their IDs.
// This revision adds per-job source I/O evidence to every new Market result.
fn market_job_schema(fields: &[String]) -> &'static str {
    let funding = fields.iter().any(|field| field == "funding_sum");
    let correlation = fields.iter().any(|field| field == "pearson_correlation");
    if funding && correlation {
        "market/518.11.source-io-funding-correlation-v1"
    } else if correlation {
        "market/518.11.source-io-correlation-v1"
    } else if funding {
        "market/518.11.source-io-funding-v1"
    } else {
        "market/518.11.source-io-flat-v1"
    }
}

// Composition retains immutable normalized observations and per-source storage
// proof. It has a distinct namespace from legacy and current flat queries.
fn market_request_schema(
    fields: &[String],
    has_composition: bool,
    oi_composition: bool,
    funding_composition: bool,
) -> &'static str {
    if has_composition || oi_composition || funding_composition {
        "market/518.11.source-io-composition-v1"
    } else {
        market_job_schema(fields)
    }
}

fn validate_composition_source_products(
    instruments: &[String],
    oi_composition: bool,
    funding_composition: bool,
) -> Result<(), String> {
    if oi_composition && instruments.iter().any(|id| !id.ends_with("-SWAP")) {
        return Err("composition.sources.open_interest:UNSUPPORTED_PRODUCT".into());
    }
    if funding_composition && instruments.iter().any(|id| !id.ends_with("-SWAP")) {
        return Err("composition.sources.funding_rate:UNSUPPORTED_PRODUCT".into());
    }
    Ok(())
}

fn market_request_fingerprint(
    request: &MarketRequest,
    page: &Option<Value>,
) -> Result<String, String> {
    okx_research::canonical_sha256(&("market/448.cursor-v1", request, page))
        .map_err(|e| e.to_string())
}

fn recorded_universe_input(
    result: &Value,
    request: &MarketRequest,
) -> Result<Option<Value>, String> {
    let mut page = result
        .get("universe_page")
        .filter(|page| page.is_object())
        .cloned()
        .ok_or("MARKET_CURSOR_REQUEST_MISMATCH")?;
    if page["global_ranking"] == true {
        // Completion adjusted these two display fields. Reconstruct the input
        // values used by both historical venue registry and paging contracts.
        let size = page["universe_size"]
            .as_u64()
            .ok_or("MARKET_CURSOR_REQUEST_MISMATCH")?;
        page["has_more"] = json!(size > u64::from(request.limit));
        page["page_count"] = json!(size.min(u64::from(request.limit)));
    }
    Ok(Some(page))
}

fn remove_universe_receipt_time(page: &mut Option<Value>) {
    if let Some(fields) = page.as_mut().and_then(Value::as_object_mut) {
        fields.remove("reference_received_at");
    }
}

impl AnalyticsService {
    pub async fn market(
        &self,
        request: MarketRequest,
        universe_page: Option<Value>,
        resolved_instruments: Option<Vec<String>>,
    ) -> Result<Value, String> {
        self.run_market(request, universe_page, resolved_instruments, None)
            .await
    }

    pub(crate) async fn market_standalone(
        &self,
        request: MarketRequest,
        source: &crate::independent_source::IndependentOkxPublicSource,
    ) -> Result<Value, String> {
        self.run_market(request, None, None, Some(source)).await
    }

    async fn run_market(
        &self,
        request: MarketRequest,
        mut universe_page: Option<Value>,
        mut resolved_instruments: Option<Vec<String>>,
        standalone_source: Option<&crate::independent_source::IndependentOkxPublicSource>,
    ) -> Result<Value, String> {
        if !request.valid() {
            return Err("INVALID_MARKET_REQUEST".into());
        }
        // K7 source, type and range validation precedes every venue read.
        let query_plan = planner::AnalysisPlan::compile_market(&request)?;
        let oi_composition = query_plan.composition.as_ref().is_some_and(|plan| {
            plan.sources()
                .contains(&okx_analysis::query_plan::Source::OpenInterest)
        });
        let funding_composition = query_plan.composition.as_ref().is_some_and(|plan| {
            plan.sources()
                .contains(&okx_analysis::query_plan::Source::Funding)
        });
        let window = query_plan.range;
        let closed_end = jiff::Timestamp::now().as_millisecond().max(0) as u64
            / request.timeframe_ms
            * request.timeframe_ms;
        if window.end_ms > closed_end {
            return Err("RANGE_NOT_CLOSED".into());
        }
        let mut hash_request = request.clone();
        let cursor = hash_request.cursor.take();
        let standalone_universe = standalone_source.is_some() && request.universe.is_some();
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
            if job.state != jobs::State::Completed {
                return Err("MARKET_CURSOR_NOT_READY".into());
            }
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
                    let mut page = if standalone_universe {
                        recorded_universe_input(&value, &hash_request)?
                    } else {
                        universe_page
                    };
                    let expected = &value["market_request_fingerprint"];
                    let mut matches =
                        *expected == market_request_fingerprint(&hash_request, &page)?;
                    if standalone_universe && !matches {
                        // Standalone scans use reference generation as content identity.
                        // Older artifacts also used the original receipt timestamp.
                        remove_universe_receipt_time(&mut page);
                        matches = *expected == market_request_fingerprint(&hash_request, &page)?;
                    }
                    if !matches || value["job_id"] != job_id {
                        return Err("MARKET_CURSOR_REQUEST_MISMATCH".into());
                    }
                    // A queued request may finish after its live-source time
                    // bucket expires. Let the caller resume page zero of that
                    // exact completed job, without admitting another job or
                    // fetching the same venue sources a second time.
                    if offset == "0" {
                        Ok(value)
                    } else {
                        global_rank_page(&store, value, &offset, &job_id)
                    }
                })
                .await;
        }
        if let Some(source) = standalone_source
            && request.universe.is_some()
        {
            let (page, instruments) =
                crate::public_catalog::resolve_universe(source, &request).await?;
            universe_page = Some(page);
            resolved_instruments = Some(instruments);
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
        validate_composition_source_products(
            &compute_request.instruments,
            oi_composition,
            funding_composition,
        )?;
        let mut identity_page = universe_page.clone();
        if standalone_universe {
            remove_universe_receipt_time(&mut identity_page);
        }
        let rank_fingerprint = if global_rank {
            Some(
                okx_research::canonical_sha256(&(
                    "market/398.global-v1",
                    &hash_request,
                    &identity_page,
                ))
                .map_err(|e| e.to_string())?,
            )
        } else {
            None
        };
        let request_fingerprint = market_request_fingerprint(&hash_request, &identity_page)?;
        let bucket = if request
            .fields
            .iter()
            .any(|f| matches!(f.as_str(), "spread_bps" | "oi_change"))
            || oi_composition
        {
            jiff::Timestamp::now().as_millisecond() / 1000
        } else if request.end_ms > jiff::Timestamp::now().as_millisecond().max(0) as u64 {
            jiff::Timestamp::now().as_millisecond() / request.timeframe_ms as i64
        } else {
            0
        };
        let hash = okx_research::canonical_sha256(&(
            market_request_schema(
                &hash_request.fields,
                hash_request.composition.is_some(),
                oi_composition,
                funding_composition,
            ),
            &hash_request,
            &identity_page,
            bucket,
        ))
        .map_err(|e| e.to_string())?;
        let hash = hash.strip_prefix("sha256:").unwrap_or(&hash).to_owned();
        // A cursor reads its immutable original result. Only a new job needs
        // to bind the resolved venue instruments before durable admission.
        // The checked operation IDs/parameters remain those of the first plan.
        let execution_plan = query_plan
            .clone()
            .with_resolved_universe(compute_request.instruments.clone(), global_rank)?;
        let (job, new, cancel) = self
            .jobs
            .enqueue_async(
                okx_research_protocol::research_ipc::IdempotencyKey::new(format!("market-{hash}"))
                    .map_err(str::to_owned)?,
                hash.clone(),
            )
            .await?;
        tracing::info!(job_id = %job.id, stage = "admission", state = if new { "queued" } else { "replayed" }, "market job admission");
        if !new {
            // A retained partial artifact is NOT a completed response. In
            // particular a cancelled/interrupted job must never be mistaken
            // for a successful cached market query after reconnect.
            if let Some(artifact) = job
                .artifact
                .as_ref()
                .filter(|_| job.state == jobs::State::Completed)
                .cloned()
            {
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
            tracing::info!(job_id = %hash, stage = "execution", state = "running", "market job running");
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
                .market_execute(compute_request, cancel.clone(), global_rank, execution_plan)
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
                    let committed = this.jobs
                        .update_async(hash.clone(), jobs::State::Completed, Some(artifact), None)
                        .await?;
                    if !committed {
                        tracing::info!(job_id = %hash, stage = "terminal", state = "cancelled", "market completion lost cancellation race");
                        return Err("CANCELLED".into());
                    }
                    tracing::info!(job_id = %hash, stage = "terminal", state = "completed", "market job completed");
                }
                Err(e) => {
                    let reason_code = jobs::job_diagnostics::safe_reason(e);
                    let state = if cancel.load(Ordering::Acquire) { "cancelled" } else { "failed" };
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
                    tracing::info!(job_id = %hash, stage = "terminal", state, reason_code, "market job ended");
                }
            }
            outcome
        }.instrument(tracing::Span::current()));
        match tokio::time::timeout(std::time::Duration::from_secs(2), &mut task).await {
            Ok(result) => result.map_err(|e| e.to_string())?,
            Err(_) => with_universe_page(json!(job), queued_page.as_ref()),
        }
    }
    async fn market_execute(
        &self,
        request: MarketRequest,
        cancel: Arc<super::cpu_executor::Cancel>,
        global_rank: bool,
        plan: okx_analysis::query_plan::QueryPlan,
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
        let meter = Arc::new(crate::source_io::SourceIoMeter::default());
        let (rows, correlation_matrix) = crate::source_io::observe(
            meter.clone(),
            self.market_rows(&request, cancel.clone(), global_rank, plan.clone()),
        )
        .await?;
        let source_io = meter.snapshot();
        let store = self.artifacts.clone();
        let producer = self.producer.clone();
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
        let result_artifact=store.publish_evidence(&json!({"kind":"market_pages","producer":producer,"row_pages":row_pages,"total_rows":total,"sources":plan.sources,"correlation_matrix":correlation_matrix,"source_io":source_io})).map_err(|e|e.to_string())?.0;
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
            json!({"kind":"market","result_artifact":result_artifact,"rows":page,"returned_rows":returned,"total_rows":total,"next_cursor":if offset+returned<total{Some((offset+returned).to_string())}else{None},"sources":plan.sources,"correlation_matrix":correlation_matrix,"source_io":source_io}),
        )
        }).await
    }
    pub fn market_capabilities(&self) -> Value {
        let correlation_matrix = json!({"input":"closed_candle_log_returns",
            "minimum_common_pairs":30,
            "join":"exact_event_time_no_forward_fill",
            "availability":"after_candle_close",
            "source_coverage":"correlation_matrix.source_coverage",
            "pair_coverage":"correlation_matrix.pair_diagnostics",
            "pair_layout":"upper_triangle_without_diagonal",
            "pair_reason_codes":["AVAILABLE","INSUFFICIENT_COMMON_PAIRS",
                "ZERO_VARIANCE_OR_NONFINITE_CORRELATION",
                "SOURCE_SERIES_UNAVAILABLE_OR_GAPPED",
                "ALIGNMENT_DUPLICATE_OR_UNORDERED_TIME","ALIGNMENT_INVALID_AVAILABILITY"]});
        let operators = okx_analysis::operators::SUPPORTED_OPERATORS
            .iter()
            .map(|&operator| {
                let info = okx_analysis::operators::registry(operator);
                let typed_inputs = info
                    .inputs
                    .iter()
                    .map(|name| {
                        okx_analysis::query_plan::input(name).expect("registered operator input")
                    })
                    .collect::<Vec<_>>();
                json!({"name":operator,"version":info.version,"status":"code_supported",
                    "route_status":"public_market_screen_compare",
                    "data_availability":info.data_availability,"time_basis":info.time_basis,
                    "output_shape":info.output_shape,"unit":info.output_unit,
                    "effects":info.effects,"inputs":info.inputs,
                    "typed_inputs":typed_inputs})
            })
            .collect::<Vec<_>>();
        // The pure Rust owner publishes this one finite, typed catalog. No
        // manually maintained worker JSON list may drift from the compiler.
        let composition_operations = okx_analysis::query_plan::series_operation_contracts();
        let mut capabilities = json!({"schema":"okx.market.capabilities/v1",
            "route_owner":"okx-research-service","compute_owner":"okx-strategy-core",
            "portfolio_scenario":true,
            "strategy_signal_outbox":true,
            "availability_policy":"actual_data_and_rights_checked_per_request",
            "effects":{"read_sources":true,"write_research_jobs":true,
                "write_finance":false,"write_governance":false,"venue_post":false},
            "actions":["screen","compare","instruments","snapshot","microstructure","capabilities"],
            "timeframes_ms":[60000,180000,300000,900000,1800000,3600000,14400000,86400000],
            "operators":operators,
            "composition":{"version":okx_analysis::query_plan::COMPOSITION_VERSION,
                "available_for":"screen","input_sources":["open","high","low","close","volume","open_interest","funding_rate"],
                "source_units":{"open":"price","high":"price","low":"price","close":"price","volume":"native_volume","open_interest":"contracts","funding_rate":"ratio"},
                "ohlc_policy":"same_instrument_closed_candle_grid_no_forward_fill",
                "volume_policy":"venue_native_instrument_volume_spot_base_or_derivative_contracts_no_implicit_conversion",
                "open_interest_policy":"exact_timestamps_no_forward_fill_publication_time_unverified",
                "funding_rate_policy":"sparse_realized_settlement_events_exact_timestamp_no_forward_fill_publication_time_unverified_swap_only",
                "funding_event_alignment":{"location":"funding_event_alignment",
                    "expected_events":null,"join_semantics":"exact_event_timestamp_no_forward_fill",
                    "first_off_grid_ms":"earliest_event_not_on_closed_bar_grid",
                    "first_empty_grid_ms":"not_a_funding_coverage_gap_sparse_events_are_expected",
                    "publication_time_verified":false},
                "source_evidence":{"location":"source_captures.<source>.evidence_artifact",
                    "schema":"okx.market.normalized-source/v1",
                    "chunk_size":4096,"storage":"immutable_content_addressed",
                    "capture_hash":"canonical_source_tuple_sha256",
                    "integrity_location":"source_integrity",
                    "integrity_schema":"okx.market.composition-source-integrity/v1",
                    "verification":"bounded_streaming_reconstructed_canonical_capture_hash",
                    "metric_reexecution":"verified_source_rehydration_and_semantic_row_or_explicit_not_verified",
                    "max_metric_reexecution_observations":65536,
                    "retrieval":"retained_with_completed_market_job",
                    "scope":"composition_sources_only",
                    "publication_time_verified_for_oi_and_funding":false},
                "null_diagnostics":{"location":"composition_quality.<result>.issues",
                    "origin_step":true,
                    "origin_event_ms":true,
                    "origin_event_time_is_not_publication_time":true,
                    "reason_codes":["SOURCE_OBSERVATION_MISSING","WARMUP",
                        "DIVISION_BY_ZERO","NON_FINITE_RESULT","NON_POSITIVE_RETURN_INPUT",
                        "EVENT_NOT_TRIGGERED","OUTCOME_UNCONFIRMED","ZERO_VARIANCE"]},
                "source_alignment":{"location":"source_alignment",
                    "when":"composition_depends_on_open_interest",
                    "join_semantics":"exact_closed_bar_timestamp_no_forward_fill",
                    "counts":["expected_observations","observed_candle_observations",
                        "observed_oi_observations","aligned_observations",
                        "paired_return_observations","oi_off_grid_events"],
                    "publication_time_verified":false},
                "operations":composition_operations,
                "limits":{"max_steps":32,"max_depth":8,"max_result_steps":14,"max_cells":8000000,"max_work_units":64000000},
                "result_projection":"last_closed_observation","outcome_policy":"unconfirmed_without_future_bars_never_a_signal"},
            "limits":{"max_explicit_instruments":100,"max_page_rows":50,
                "max_correlation_instruments":8,"max_candles_total":1000000},
            "universe":{"modes":["explicit_instruments","venue_reference_registry"],
                "whole_market_paged":true,"globally_ranked":true,
                "global_rank_requires_explicit_opt_in":true,
                "global_rank_max_instruments":512,
                "global_rank_source_budget_bars":1000000,
                "global_rank_uses_immutable_artifact_cursor":true,
                "max_instruments_per_page":100},
            "quality":"complete"});
        capabilities["correlation_matrix"] = correlation_matrix;
        capabilities
    }
    pub(super) async fn market_rows(
        &self,
        request: &MarketRequest,
        cancel: Arc<super::cpu_executor::Cancel>,
        global_rank: bool,
        plan: okx_analysis::query_plan::QueryPlan,
    ) -> Result<(Vec<Value>, Option<Value>), String> {
        let need_candles = plan
            .sources
            .contains(&okx_analysis::query_plan::Source::Candles);
        let need_oi = plan
            .sources
            .contains(&okx_analysis::query_plan::Source::OpenInterest);
        let need_funding = plan
            .sources
            .contains(&okx_analysis::query_plan::Source::Funding);
        let count = (request.end_ms - request.start_ms) / request.timeframe_ms;
        let total_bars = count
            .checked_mul(request.instruments.len() as u64)
            .ok_or("LIMIT_TOTAL_BARS:OVERFLOW")?;
        if (need_candles || need_oi || need_funding) && total_bars > 1_000_000 {
            return Err("LIMIT_TOTAL_BARS:1000000".into());
        }
        // The QueryPlan checked the same source and graph-wide work budget
        // against the resolved universe before durable job admission. Do not
        // recompile user step descriptions after planning; this second guard
        // protects direct internal calls to market_rows.
        if let Some(composition) = &plan.composition {
            composition.validate_global_budget(total_bars)?;
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
                    end_ms: end
                        .min(jiff::Timestamp::now().as_millisecond().max(0) as u64 / ms * ms),
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
                let funding = if need_funding {
                    Some(sources.funding(&instrument, start, end).await)
                } else {
                    None
                };
                let oi = if need_oi {
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
                                    if rows.insert(t, v).is_some() {
                                        return Err("OI_DUPLICATE_TIMESTAMP".into());
                                    }
                                }
                            }
                            // Preserve all time keys for the strict per-bar
                            // coverage check in compute_row_checked.
                            Ok(rows.into_iter().collect())
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
        let loaded = tokio::select! {data=loading=>data,_=super::cpu_executor::cancelled(cancel.clone())=>return Err("CANCELLED".into())};
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
        let store = self.artifacts.clone();
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
                    Some(pairwise_matrix(
                        &loaded,
                        request.timeframe_ms,
                        request.end_ms,
                    ))
                } else {
                    None
                };
                let peer = loaded
                    .first()
                    .and_then(|row| row.bars.as_ref().ok())
                    .cloned();
                let mut rows = Vec::new();
                // Only surviving result rows own source evidence. Publishing
                // while computing would leak permanent unreferenced blobs for
                // rejected global ranks and ordinary metric filters.
                let mut source_inputs = Vec::new();
                for row in loaded {
                    if cancel.load(Ordering::Acquire) {
                        return Err("CANCELLED".into());
                    }
                    if plan.composition.is_some() {
                        let source_index = source_inputs.len();
                        source_inputs.push(row.clone());
                        let mut result = compute_row_planned(
                            row,
                            &request,
                            peer.as_deref(),
                            &plan,
                            need_candles,
                        )?;
                        result["__evidence_source_index"] = json!(source_index);
                        rows.push(result);
                    } else {
                        rows.push(compute_row_planned(
                            row,
                            &request,
                            peer.as_deref(),
                            &plan,
                            need_candles,
                        )?);
                    }
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
                if plan.composition.is_some() {
                    for result in &mut rows {
                        if cancel.load(Ordering::Acquire) {
                            return Err("CANCELLED".into());
                        }
                        let index = result
                            .as_object_mut()
                            .and_then(|r| r.remove("__evidence_source_index"))
                            .and_then(|v| v.as_u64())
                            .ok_or("COMPOSITION_SOURCE_INDEX_MISSING")?
                            as usize;
                        let inputs = source_inputs
                            .get(index)
                            .ok_or("COMPOSITION_SOURCE_INDEX_INVALID")?;
                        let evidence = publish_composition_source_evidence(
                            &store, inputs, &request, &plan, &cancel,
                        )?;
                        let mut linked =
                            attach_composition_source_evidence(std::mem::take(result), evidence)?;
                        linked["source_integrity"] = verify_composition_source_pages(
                            &store, &linked, &request, &plan, &cancel,
                        )?;
                        let proof = replay_proof::verify_persisted_market_metrics(
                            &store,
                            &linked,
                            &request,
                            &plan,
                            need_candles,
                            &cancel,
                        )?;
                        linked["source_integrity"]["replay_metric_reexecution_verified"] =
                            json!(proof["quality"] == "verified_reexecution");
                        linked["source_integrity"]["metric_reexecution"] = proof;
                        *result = linked;
                    }
                }
                Ok((rows, matrix))
            })
            .await
    }
}

// The evidence is the exact normalized input to the checked composition,
// published in bounded pages. The existing source-capture hashes remain the
// domain-separated witnesses over the full ordered source, not over pages.
// Neither a missing source nor a failed source read creates a fabricated page.
const MARKET_SOURCE_EVIDENCE_CHUNK: usize = 4096;
type PublishedMarketSource = (&'static str, String, String);

/// Stream a source's content-addressed pages through the same canonical JSON
/// tuple as the original capture. No 1M-row reconstruction or second
/// full-series buffer is required. This verifies the *recorded* observations,
/// not unavailable venue publication times or a separate execution oracle.
struct VerifiedSourcePages<'a, T> {
    store: &'a ResearchArtifactStore,
    chunks: &'a [String],
    expected: usize,
    cancel: &'a super::cpu_executor::Cancel,
    successful_file_reads: &'a Cell<u64>,
    bytes_read: &'a Cell<u64>,
    row: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned> Serialize for VerifiedSourcePages<'_, T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.expected))?;
        let mut observed = 0usize;
        for (index, id) in self.chunks.iter().enumerate() {
            if self.cancel.load(Ordering::Acquire) {
                return Err(serde::ser::Error::custom("CANCELLED"));
            }
            let bytes = self
                .store
                .read_evidence(id)
                .map_err(serde::ser::Error::custom)?;
            self.successful_file_reads
                .set(self.successful_file_reads.get() + 1);
            self.bytes_read
                .set(self.bytes_read.get() + bytes.len() as u64);
            let rows: Vec<T> = serde_json::from_slice(&bytes).map_err(serde::ser::Error::custom)?;
            if rows.is_empty()
                || rows.len() > MARKET_SOURCE_EVIDENCE_CHUNK
                || (index + 1 < self.chunks.len() && rows.len() != MARKET_SOURCE_EVIDENCE_CHUNK)
            {
                return Err(serde::ser::Error::custom(
                    "COMPOSITION_SOURCE_PAGE_SIZE_INVALID",
                ));
            }
            observed = observed
                .checked_add(rows.len())
                .ok_or_else(|| serde::ser::Error::custom("COMPOSITION_SOURCE_COUNT_OVERFLOW"))?;
            if observed > self.expected {
                return Err(serde::ser::Error::custom(
                    "COMPOSITION_SOURCE_COUNT_MISMATCH",
                ));
            }
            for row in &rows {
                sequence.serialize_element(row)?;
            }
        }
        if observed != self.expected {
            return Err(serde::ser::Error::custom(
                "COMPOSITION_SOURCE_COUNT_MISMATCH",
            ));
        }
        sequence.end()
    }
}

struct SourceHashWriter(sha2::Sha256);

impl Write for SourceHashWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn verify_one_source_pages<T: Serialize + DeserializeOwned>(
    store: &ResearchArtifactStore,
    request: &MarketRequest,
    instrument: &str,
    capture: &Value,
    source_spec: (&str, &str, &str),
    cancel: &super::cpu_executor::Cancel,
) -> Result<Value, String> {
    let (source, hash_domain, time_basis) = source_spec;
    let invalid = || "COMPOSITION_SOURCE_EVIDENCE_MISMATCH".to_owned();
    let manifest_id = capture["evidence_artifact"].as_str().ok_or_else(invalid)?;
    let manifest_bytes = store.read_evidence(manifest_id).map_err(|_| invalid())?;
    let manifest: Value = serde_json::from_slice(&manifest_bytes).map_err(|_| invalid())?;
    let expected = manifest["observed_points"].as_u64().ok_or_else(invalid)?;
    if expected > 1_000_000
        || manifest["schema"] != "okx.market.normalized-source/v1"
        || manifest["source"] != source
        || manifest["instrument"] != instrument
        || manifest["requested_start_ms"] != request.start_ms
        || manifest["requested_end_ms"] != request.end_ms
        || manifest["timeframe_ms"] != request.timeframe_ms
        || manifest["hash_domain"] != hash_domain
        || manifest["time_basis"] != time_basis
        || manifest["publication_time_verified"] != false
        || manifest["availability_assumption"]
            != (if source == "candles" {
                "after_closed_bar"
            } else {
                "historical_event_only"
            })
        || manifest["chunk_size"] != MARKET_SOURCE_EVIDENCE_CHUNK
        || manifest["capture_hash"] != capture["capture_hash"]
        || capture["source"]
            != (if source == "candles" {
                "okx/closed_candles"
            } else if source == "open_interest" {
                "okx/open_interest"
            } else {
                "okx/funding-rate-history"
            })
    {
        return Err(invalid());
    }
    let expected = expected as usize;
    let chunks = manifest["chunks"].as_array().ok_or_else(invalid)?;
    if chunks.len() != expected.div_ceil(MARKET_SOURCE_EVIDENCE_CHUNK) {
        return Err(invalid());
    }
    let ids = chunks
        .iter()
        .map(|value| value.as_str().map(str::to_owned).ok_or_else(invalid))
        .collect::<Result<Vec<_>, _>>()?;
    // These counters represent successful artifact fs::read operations during
    // source hash reconstruction, not physical disk reads or all job storage I/O.
    let file_reads = Cell::new(1u64);
    let bytes_read = Cell::new(manifest_bytes.len() as u64);
    let rows = VerifiedSourcePages::<T> {
        store,
        chunks: &ids,
        expected,
        cancel,
        successful_file_reads: &file_reads,
        bytes_read: &bytes_read,
        row: PhantomData,
    };
    let mut writer = SourceHashWriter(sha2::Sha256::new());
    serde_json::to_writer(
        &mut writer,
        &(
            hash_domain,
            instrument,
            request.start_ms,
            request.end_ms,
            request.timeframe_ms,
            rows,
        ),
    )
    .map_err(|_| {
        if cancel.load(Ordering::Acquire) {
            "CANCELLED".into()
        } else {
            invalid()
        }
    })?;
    let reconstructed_hash = format!("sha256:{:x}", writer.0.finalize());
    if capture["capture_hash"] != reconstructed_hash
        || (source == "candles" && capture["observed_points"] != expected)
        || (source == "open_interest" && capture["observed_points"] != expected)
        || (source == "funding_rate" && capture["observed_events"] != expected)
    {
        return Err(invalid());
    }
    Ok(
        json!({"source":source,"quality":"verified_normalized_capture",
        "pages":ids.len(),"observed_points":expected,"capture_hash":reconstructed_hash,
        "publication_time_verified":false,
        "verified_storage_io":{
            "scope":"source_hash_reconstruction_only",
            "successful_artifact_file_reads":file_reads.get(),
            "artifact_bytes_read":bytes_read.get(),
            "os_physical_disk_reads":null
        }}),
    )
}

fn verify_composition_source_pages(
    store: &ResearchArtifactStore,
    row: &Value,
    request: &MarketRequest,
    plan: &okx_analysis::query_plan::QueryPlan,
    cancel: &super::cpu_executor::Cancel,
) -> Result<Value, String> {
    use okx_analysis::query_plan::Source;
    plan.composition
        .as_ref()
        .ok_or("COMPOSITION_EVIDENCE_WITHOUT_PLAN")?;
    // A composed market response can include legacy flat projections alongside
    // graph outputs. Their normalized sources are also part of its evidence.
    let required = &plan.sources;
    let mut verified = Vec::new();
    let mut unavailable = Vec::new();
    let instrument = row["instrument"]
        .as_str()
        .ok_or("COMPOSITION_INSTRUMENT_MISSING")?;
    let sources = [
        (
            "candles",
            Source::Candles,
            "okx.market.candle-source-capture/v1",
            "closed_candle_time",
        ),
        (
            "open_interest",
            Source::OpenInterest,
            "okx.market.oi-source-capture/v1",
            "observation_event_time_publication_unverified",
        ),
        (
            "funding_rate",
            Source::Funding,
            "okx.market.funding-realized-event-source-capture/v1",
            "realized_settlement_event_time_publication_unverified",
        ),
    ];
    for (name, kind, domain, time_basis) in sources {
        if !required.contains(&kind) {
            continue;
        }
        let capture = &row["source_captures"][name];
        if capture.is_null() {
            unavailable.push(name);
            continue;
        }
        verified.push(match kind {
            Source::Candles => verify_one_source_pages::<Bar>(
                store,
                request,
                instrument,
                capture,
                (name, domain, time_basis),
                cancel,
            )?,
            _ => verify_one_source_pages::<(u64, f64)>(
                store,
                request,
                instrument,
                capture,
                (name, domain, time_basis),
                cancel,
            )?,
        });
    }
    Ok(
        json!({"schema":"okx.market.composition-source-integrity/v1",
        "quality":if unavailable.is_empty(){"verified_normalized_sources"}else{"partial_source_unavailable"},
        "verified_sources":verified,"unavailable_sources":unavailable,
        "replay_metric_reexecution_verified":false,
        "historical_publication_time_verified":false,
        "proof":"bounded_chunk_read_identity_and_reconstructed_canonical_capture_hash"}),
    )
}

fn attach_composition_source_evidence(
    mut result: Value,
    evidence: Vec<PublishedMarketSource>,
) -> Result<Value, String> {
    for (source, capture_hash, artifact) in evidence {
        let capture = result["source_captures"][source]
            .as_object_mut()
            .ok_or("COMPOSITION_SOURCE_CAPTURE_MISSING")?;
        if capture.get("capture_hash") != Some(&json!(capture_hash)) {
            return Err("COMPOSITION_SOURCE_CAPTURE_HASH_MISMATCH".into());
        }
        capture.insert("evidence_artifact".into(), json!(artifact));
    }
    Ok(result)
}

fn publish_normalized_source<T: Serialize>(
    store: &ResearchArtifactStore,
    request: &MarketRequest,
    instrument: &str,
    source: &'static str,
    hash_domain: &'static str,
    observations: &[T],
    cancel: &super::cpu_executor::Cancel,
) -> Result<PublishedMarketSource, String> {
    let capture_hash = okx_research::canonical_sha256(&(
        hash_domain,
        instrument,
        request.start_ms,
        request.end_ms,
        request.timeframe_ms,
        observations,
    ))
    .map_err(|e| e.to_string())?;
    let mut chunks = Vec::new();
    for chunk in observations.chunks(MARKET_SOURCE_EVIDENCE_CHUNK) {
        if cancel.load(Ordering::Acquire) {
            return Err("CANCELLED".into());
        }
        chunks.push(store.publish_evidence(&chunk).map_err(|e| e.to_string())?.0);
    }
    if cancel.load(Ordering::Acquire) {
        return Err("CANCELLED".into());
    }
    let manifest = store
        .publish_evidence(&json!({
            "schema":"okx.market.normalized-source/v1",
            "source":source,
            "instrument":instrument,
            "requested_start_ms":request.start_ms,
            "requested_end_ms":request.end_ms,
            "timeframe_ms":request.timeframe_ms,
            "capture_hash":capture_hash,
            "hash_domain":hash_domain,
            "observed_points":observations.len(),
            "chunk_size":MARKET_SOURCE_EVIDENCE_CHUNK,
            "chunks":chunks,
            "time_basis":if source == "funding_rate" {
                "realized_settlement_event_time_publication_unverified"
            } else if source == "open_interest" {
                "observation_event_time_publication_unverified"
            } else {
                "closed_candle_time"
            },
            "publication_time_verified":false,
            "availability_assumption":if source == "candles" {
                "after_closed_bar"
            } else {
                "historical_event_only"
            },
        }))
        .map_err(|e| e.to_string())?
        .0;
    Ok((source, capture_hash, manifest))
}

fn publish_composition_source_evidence(
    store: &ResearchArtifactStore,
    row: &Loaded,
    request: &MarketRequest,
    plan: &okx_analysis::query_plan::QueryPlan,
    cancel: &super::cpu_executor::Cancel,
) -> Result<Vec<PublishedMarketSource>, String> {
    use okx_analysis::query_plan::Source;
    plan.composition
        .as_ref()
        .ok_or("COMPOSITION_EVIDENCE_WITHOUT_PLAN")?;
    let mut evidence = Vec::with_capacity(3);
    if plan.sources.contains(&Source::Candles)
        && let Ok(bars) = &row.bars
    {
        evidence.push(publish_normalized_source(
            store,
            request,
            &row.instrument,
            "candles",
            "okx.market.candle-source-capture/v1",
            bars,
            cancel,
        )?);
    }
    if plan.sources.contains(&Source::OpenInterest)
        && let Some(Ok(points)) = &row.oi
    {
        evidence.push(publish_normalized_source(
            store,
            request,
            &row.instrument,
            "open_interest",
            "okx.market.oi-source-capture/v1",
            points,
            cancel,
        )?);
    }
    if plan.sources.contains(&Source::Funding)
        && let Some(Ok(events)) = &row.funding
    {
        evidence.push(publish_normalized_source(
            store,
            request,
            &row.instrument,
            "funding_rate",
            "okx.market.funding-realized-event-source-capture/v1",
            events,
            cancel,
        )?);
    }
    Ok(evidence)
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
#[cfg(test)]
fn compute_row(
    row: Loaded,
    request: &MarketRequest,
    peer: Option<&Vec<Bar>>,
) -> Result<Value, String> {
    let checked = request
        .composition
        .clone()
        .map(|c| {
            let count = (request.end_ms - request.start_ms) / request.timeframe_ms;
            okx_analysis::query_plan::CheckedComposition::compile(*c, count)
        })
        .transpose()?;
    let needs_candles =
        checked.as_ref().is_some_and(|plan| {
            plan.sources()
                .contains(&okx_analysis::query_plan::Source::Candles)
        }) || planner::AnalysisPlan::build(&request.fields, request.start_ms, request.end_ms)?
            .sources
            .contains("candles");
    compute_row_checked(row, request, peer, checked.as_ref(), needs_candles)
}
#[cfg(test)]
fn compute_row_checked(
    row: Loaded,
    request: &MarketRequest,
    peer: Option<&Vec<Bar>>,
    composition: Option<&okx_analysis::query_plan::CheckedComposition>,
    needs_candles: bool,
) -> Result<Value, String> {
    let mut checked = planner::AnalysisPlan::compile_market(request)?;
    checked.composition = composition.cloned();
    compute_row_planned(row, request, peer, &checked, needs_candles)
}
fn compute_row_planned(
    row: Loaded,
    request: &MarketRequest,
    peer: Option<&Vec<Bar>>,
    plan: &okx_analysis::query_plan::QueryPlan,
    needs_candles: bool,
) -> Result<Value, String> {
    if plan.operators.len() != request.fields.len()
        || plan.result_projection.len() != request.fields.len()
        || plan
            .operators
            .iter()
            .zip(&plan.result_projection)
            .zip(&request.fields)
            .any(|((descriptor, projection), field)| {
                descriptor.id != *projection
                    || serde_json::to_value(descriptor.id).ok().as_ref() != Some(&json!(field))
            })
    {
        return Err("COMPILED_PLAN_PROJECTION_MISMATCH".into());
    }
    let composition = plan.composition.as_ref();
    let mut gaps = Vec::new();
    // A source-specific metric does not imply that candle observations exist.
    // Preserve candle coverage for mixed queries and composed price series,
    // but report N/A (null) when the typed plan excludes candle sources.
    let candles_available = row.bars.is_ok();
    let bars = match row.bars {
        Ok(b) => b,
        Err(e) => {
            if needs_candles {
                gaps.push(e);
            }
            Arc::new(Vec::new())
        }
    };
    let close_column =
        okx_analysis::operators::NumericColumn::from_values(bars.iter().map(|b| b.close));
    let close = &close_column.values;
    let high = bars.iter().map(|b| b.high).collect::<Vec<_>>();
    let low = bars.iter().map(|b| b.low).collect::<Vec<_>>();
    let volume = bars.iter().map(|b| b.volume).collect::<Vec<_>>();
    let candle_coverage = needs_candles
        .then(|| {
            okx_analysis::time_series::ClosedRange::strict(
                request.start_ms,
                request.end_ms,
                request.timeframe_ms,
            )
            .ok()
            .map(|range| range.coverage(bars.iter().map(|bar| bar.time)))
        })
        .flatten();
    let incomplete_candles = needs_candles
        && candle_coverage
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
    let oi_coverage = row
        .oi
        .as_ref()
        .and_then(|source| source.as_ref().ok())
        .and_then(|points| {
            okx_analysis::time_series::ClosedRange::strict(
                request.start_ms,
                request.end_ms,
                request.timeframe_ms,
            )
            .ok()
            .map(|range| range.coverage(points.iter().map(|(time, _)| *time)))
        });
    let composition_oi = composition.is_some_and(|plan| {
        plan.sources()
            .contains(&okx_analysis::query_plan::Source::OpenInterest)
    });
    let composition_funding = composition.is_some_and(|plan| {
        plan.sources()
            .contains(&okx_analysis::query_plan::Source::Funding)
    });
    let capture_oi = composition.is_some()
        && plan
            .sources
            .contains(&okx_analysis::query_plan::Source::OpenInterest);
    let capture_funding = composition.is_some()
        && plan
            .sources
            .contains(&okx_analysis::query_plan::Source::Funding);
    if request.fields.iter().any(|field| field == "oi_change") || composition_oi {
        if let Some(Err(_)) = &row.oi {
            gaps.push("OI_SOURCE_UNAVAILABLE".into());
        } else if oi_coverage
            .as_ref()
            .is_none_or(|coverage| !coverage.complete)
        {
            gaps.push("OI_RANGE_INCOMPLETE_OR_GAPPED".into());
        }
        if composition_oi {
            gaps.push("OI_PUBLICATION_TIME_UNVERIFIED".into());
        }
    }
    let funding_requested = request.fields.iter().any(|field| field == "funding_sum");
    if funding_requested || composition_funding {
        // Funding history exposes event time and an eventual realized rate,
        // but not the moment that value became publicly available. Preserve
        // the retrospective sum while refusing to certify it as causal input.
        match &row.funding {
            Some(Ok(_)) => gaps.push("FUNDING_PUBLICATION_TIME_UNVERIFIED".into()),
            Some(Err(reason))
                if reason.starts_with("FUNDING_REALIZED_RATE_")
                    || reason == "FUNDING_CONFLICTING_EVENT_TIMESTAMP" =>
            {
                gaps.push(reason.clone());
                gaps.push("FUNDING_SOURCE_UNAVAILABLE".into());
            }
            Some(Err(_)) | None => gaps.push("FUNDING_SOURCE_UNAVAILABLE".into()),
        }
    }
    let spread_requested = request.fields.iter().any(|field| field == "spread_bps");
    if spread_requested {
        // ResearchTicker has bid/ask but no observation/publication timestamp.
        // A current quote must never masquerade as historical interval data.
        gaps.push("SPREAD_CURRENT_SNAPSHOT_NOT_HISTORICAL".into());
    }
    let mut metrics = serde_json::Map::new();
    let mut units = serde_json::Map::new();
    for (field, descriptor) in request.fields.iter().zip(&plan.operators) {
        let op = descriptor.id;
        let param =
            |name: &str, default: f64| plan.parameters.get(name).copied().unwrap_or(default);
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
                let (pairs, _) =
                    okx_analysis::time_series::align_exact_at(&a, &b, request.end_ms).ok()?;
                okx_analysis::operators::correlation_pairs(&pairs)
            }),
            Operator::VolumeChange => {
                scalar(op, close, &high, &low, &volume, 14, request.timeframe_ms)?
            }
            Operator::OiChange => row
                .oi
                .as_ref()
                .and_then(|r| r.as_ref().ok())
                .filter(|_| {
                    oi_coverage
                        .as_ref()
                        .is_some_and(|coverage| coverage.complete)
                })
                .and_then(|r| r.first().zip(r.last()))
                .filter(|(a, _)| a.1 > 0.0)
                .map(|(a, b)| b.1 / a.1 - 1.0),
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
    let mut composition_quality = serde_json::Map::new();
    let mut source_alignment = None;
    let mut funding_event_alignment = None;
    if let Some(composition) = composition {
        let source_dependencies = composition.result_sources();
        let composition_ohlcv = ["open", "high", "low", "volume"]
            .iter()
            .any(|name| composition.uses_source(name));
        // A missing candle in a close-only graph also requires exact-grid
        // evaluation. Compacting the remaining bars would hide the null's
        // origin and shift rolling windows across the missing timestamp.
        let evaluated = if composition_oi
            || composition_funding
            || composition_ohlcv
            || incomplete_candles
        {
            let count = ((request.end_ms - request.start_ms) / request.timeframe_ms) as usize;
            let mut prices = vec![f64::NAN; count];
            // Allocate only projected dependencies. Unused OHLC branches do
            // not consume buffers or cause any extra source reads.
            let mut opens = composition
                .uses_source("open")
                .then(|| vec![f64::NAN; count]);
            let mut highs = composition
                .uses_source("high")
                .then(|| vec![f64::NAN; count]);
            let mut lows = composition
                .uses_source("low")
                .then(|| vec![f64::NAN; count]);
            let mut volumes = composition
                .uses_source("volume")
                .then(|| vec![f64::NAN; count]);
            let range = okx_analysis::time_series::ClosedRange::strict(
                request.start_ms,
                request.end_ms,
                request.timeframe_ms,
            )?;
            for bar in bars.iter() {
                if let Some(offset) = bar.time.checked_sub(request.start_ms)
                    && offset.is_multiple_of(request.timeframe_ms)
                {
                    let index = (offset / request.timeframe_ms) as usize;
                    if let Some(slot) = prices.get_mut(index) {
                        *slot = bar.close;
                        if let Some(column) = &mut opens {
                            column[index] = bar.open;
                        }
                        if let Some(column) = &mut highs {
                            column[index] = bar.high;
                        }
                        if let Some(column) = &mut lows {
                            column[index] = bar.low;
                        }
                        if let Some(column) = &mut volumes {
                            column[index] = bar.volume;
                        }
                    }
                }
            }
            let oi_grid = if composition_oi {
                let points = row
                    .oi
                    .as_ref()
                    .and_then(|source| source.as_ref().ok())
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);
                Some(range.align_event_values(points.iter().copied())?)
            } else {
                None
            };
            let open_interest = oi_grid
                .as_ref()
                .map(|grid| grid.values.as_slice())
                .unwrap_or(&[]);
            let funding_grid = if composition_funding {
                let events = row
                    .funding
                    .as_ref()
                    .and_then(|source| source.as_ref().ok())
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);
                Some(range.align_event_values(events.iter().copied())?)
            } else {
                None
            };
            let funding_rate = funding_grid
                .as_ref()
                .map(|grid| grid.values.as_slice())
                .unwrap_or(&[]);
            if composition_funding {
                let grid = funding_grid.as_ref().expect("required funding grid");
                funding_event_alignment = Some(json!({
                    "join_semantics":"exact_event_timestamp_no_forward_fill",
                    "rate_basis":"realized_settlement_rate",
                    "observed_events":row.funding.as_ref().and_then(|v|v.as_ref().ok()).map(|v|v.len()),
                    "aligned_events":grid.aligned_events,
                    "off_grid_events":grid.off_grid_events,
                    "first_off_grid_ms":grid.first_off_grid_ms,
                    "first_empty_grid_ms":grid.first_empty_grid_ms,
                    "expected_events":null,
                    "publication_time_verified":false,
                    "last_bar_has_event":funding_rate.last().is_some_and(Option::is_some)
                }));
            }
            if composition_oi {
                let (mut candle_points, mut oi_points, mut aligned, mut paired_returns) =
                    (0usize, 0usize, 0usize, 0usize);
                let (mut first_unaligned_ms, mut prior_return_pair) = (None, false);
                for (i, (&price, &oi)) in prices.iter().zip(open_interest).enumerate() {
                    let candle_present = price.is_finite();
                    let oi_present = oi.is_some();
                    candle_points += usize::from(candle_present);
                    oi_points += usize::from(oi_present);
                    aligned += usize::from(candle_present && oi_present);
                    if !(candle_present && oi_present) && first_unaligned_ms.is_none() {
                        first_unaligned_ms =
                            Some(request.start_ms + i as u64 * request.timeframe_ms);
                    }
                    let return_pair =
                        price.is_finite() && price > 0.0 && oi.is_some_and(|v| v > 0.0);
                    if i > 0 && prior_return_pair && return_pair {
                        paired_returns += 1;
                    }
                    prior_return_pair = return_pair;
                }
                source_alignment = Some(json!({
                    "join_semantics":"exact_closed_bar_timestamp_no_forward_fill",
                    "expected_observations":count,
                    "observed_candle_observations":candle_points,
                    "observed_oi_observations":oi_points,
                    "aligned_observations":aligned,
                    "paired_return_observations":paired_returns,
                    "first_unaligned_ms":first_unaligned_ms,
                    "oi_off_grid_events":oi_grid.as_ref().map(|grid| grid.off_grid_events),
                    "oi_first_off_grid_ms":oi_grid.as_ref().and_then(|grid| grid.first_off_grid_ms),
                    "oi_publication_time_verified":false
                }));
            }
            composition.evaluate_with_candle_sources_diagnostics(
                &prices,
                okx_analysis::query_plan::CandlePriceInputs {
                    open: opens.as_deref(),
                    high: highs.as_deref(),
                    low: lows.as_deref(),
                    volume: volumes.as_deref(),
                },
                composition_oi.then_some(open_interest),
                composition_funding.then_some(funding_rate),
            )?
        } else {
            composition.evaluate_with_diagnostics(close)
        };
        let (evaluated, null_causes) = evaluated;
        for (id, (value, unit)) in evaluated {
            let dependencies = &source_dependencies[&id];
            let origin_event_ms = null_causes.get(&id).and_then(|cause| {
                (cause.origin_index as u64)
                    .checked_mul(request.timeframe_ms)
                    .and_then(|offset| request.start_ms.checked_add(offset))
            });
            let depends_on_oi =
                dependencies.contains(&okx_analysis::query_plan::Source::OpenInterest);
            let depends_on_funding =
                dependencies.contains(&okx_analysis::query_plan::Source::Funding);
            let missing_candles = incomplete_candles
                && dependencies.contains(&okx_analysis::query_plan::Source::Candles);
            let missing_oi = depends_on_oi
                && oi_coverage
                    .as_ref()
                    .is_none_or(|coverage| !coverage.complete);
            let missing_funding = depends_on_funding && !matches!(&row.funding, Some(Ok(_)));
            // Event-time alone does not prove a historical OI observation was
            // available at decision time. Never certify an OI-derived signal.
            let unavailable_causal_oi = depends_on_oi
                && matches!(
                    unit,
                    okx_analysis::query_plan::Unit::Boolean
                        | okx_analysis::query_plan::Unit::Event
                        | okx_analysis::query_plan::Unit::OutcomeRatio
                );
            let unavailable_causal_funding = depends_on_funding
                && matches!(
                    unit,
                    okx_analysis::query_plan::Unit::Boolean
                        | okx_analysis::query_plan::Unit::Event
                        | okx_analysis::query_plan::Unit::OutcomeRatio
                );
            let value = if missing_candles
                || missing_oi
                || missing_funding
                || unavailable_causal_oi
                || unavailable_causal_funding
            {
                None
            } else {
                value
            };
            let mut issues = Vec::new();
            if missing_candles {
                issues.push(json!({
                    "source":"candles","reason":"CANDLE_RANGE_INCOMPLETE_OR_GAPPED",
                    "first_missing_ms":candle_coverage.as_ref().and_then(|c|c.first_missing_ms),
                    "origin_step":null_causes.get(&id).map(|c|c.origin_step.as_str()),
                    "origin_event_ms":origin_event_ms,
                    "causal_null_reason":null_causes.get(&id).map(|c|c.reason)
                }));
            }
            if missing_oi {
                issues.push(json!({
                    "source":"open_interest","reason":"OI_RANGE_INCOMPLETE_OR_GAPPED",
                    "first_missing_ms":oi_coverage.as_ref().and_then(|c|c.first_missing_ms),
                    "origin_step":null_causes.get(&id).map(|c|c.origin_step.as_str()),
                    "origin_event_ms":origin_event_ms,
                    "causal_null_reason":null_causes.get(&id).map(|c|c.reason)
                }));
            }
            if missing_funding {
                issues.push(json!({
                    "source":"funding_rate","reason":"FUNDING_SOURCE_UNAVAILABLE",
                    "origin_step":null_causes.get(&id).map(|c|c.origin_step.as_str()),
                    "origin_event_ms":origin_event_ms,
                    "causal_null_reason":null_causes.get(&id).map(|c|c.reason)
                }));
            }
            if unavailable_causal_oi {
                gaps.push(format!("CAUSAL_AVAILABILITY_UNVERIFIED:{id}"));
                issues.push(json!({
                    "source":"open_interest","reason":"CAUSAL_PUBLICATION_TIME_UNVERIFIED"
                }));
            }
            if unavailable_causal_funding {
                gaps.push(format!("CAUSAL_AVAILABILITY_UNVERIFIED:{id}"));
                issues.push(json!({
                    "source":"funding_rate","reason":"CAUSAL_PUBLICATION_TIME_UNVERIFIED"
                }));
            }
            if value.is_none() {
                gaps.push(if unit == okx_analysis::query_plan::Unit::OutcomeRatio {
                    format!("OUTCOME_UNCONFIRMED:{id}")
                } else {
                    format!("MISSING_INPUT_OR_MINIMUM_SAMPLES:{id}")
                });
                if issues.is_empty() {
                    if let Some(cause) = null_causes.get(&id) {
                        issues.push(json!({
                            "reason":cause.reason,
                            "origin_step":cause.origin_step,
                            "origin_event_ms":origin_event_ms
                        }));
                    } else {
                        issues.push(json!({
                            "reason":if unit == okx_analysis::query_plan::Unit::OutcomeRatio {
                                "OUTCOME_UNCONFIRMED"
                            } else {"MISSING_INPUT_OR_WARMUP"}
                        }));
                    }
                }
            }
            let status = if value.is_none() {
                "unavailable"
            } else if depends_on_oi || depends_on_funding {
                "retrospective_publication_unverified"
            } else {
                "available_at_closed_bar"
            };
            composition_quality.insert(
                id.clone(),
                json!({
                    "status":status,"dependencies":dependencies,"issues":issues
                }),
            );
            let metric = match unit {
                okx_analysis::query_plan::Unit::Boolean | okx_analysis::query_plan::Unit::Event => {
                    value.map_or(Value::Null, |v| json!(v == 1.0))
                }
                _ => value.map_or(Value::Null, |v| json!(v)),
            };
            metrics.insert(id.clone(), metric);
            let unit_name = match unit {
                okx_analysis::query_plan::Unit::Price => "price",
                okx_analysis::query_plan::Unit::Contracts => "contracts",
                okx_analysis::query_plan::Unit::NativeVolume => "native_volume",
                okx_analysis::query_plan::Unit::Ratio => "ratio",
                okx_analysis::query_plan::Unit::Coefficient => "coefficient",
                okx_analysis::query_plan::Unit::Boolean => "boolean",
                okx_analysis::query_plan::Unit::Event => "event",
                okx_analysis::query_plan::Unit::OutcomeRatio => "outcome_ratio",
                _ => return Err("composition:UNEXPECTED_UNIT".into()),
            };
            units.insert(id, json!(unit_name));
        }
    }
    let source_captures = if composition.is_some() {
        let mut captures = serde_json::Map::new();
        if capture_oi {
            let oi_capture = row
                .oi
                .as_ref()
                .and_then(|source| source.as_ref().ok())
                .map(|points| -> Result<Value, String> {
                    let hash = okx_research::canonical_sha256(&(
                        "okx.market.oi-source-capture/v1",
                        &row.instrument,
                        request.start_ms,
                        request.end_ms,
                        request.timeframe_ms,
                        points,
                    ))
                    .map_err(|error| error.to_string())?;
                    Ok(json!({
                        "source":"okx/open_interest",
                        "capture_hash":hash,
                        "requested_start_ms":request.start_ms,
                        "requested_end_ms":request.end_ms,
                        "timeframe_ms":request.timeframe_ms,
                        "observed_points":points.len(),
                        "first_event_ms":points.first().map(|(time, _)| time),
                        "last_event_ms":points.last().map(|(time, _)| time),
                        "publication_time_verified":false,
                    }))
                })
                .transpose()?;
            captures.insert("open_interest".into(), oi_capture.unwrap_or(Value::Null));
        }
        if capture_funding {
            let funding_capture = row
                .funding
                .as_ref()
                .and_then(|source| source.as_ref().ok())
                .map(|events| -> Result<Value, String> {
                    let hash = okx_research::canonical_sha256(&(
                        "okx.market.funding-realized-event-source-capture/v1",
                        &row.instrument,
                        request.start_ms,
                        request.end_ms,
                        request.timeframe_ms,
                        events,
                    ))
                    .map_err(|error| error.to_string())?;
                    Ok(json!({
                        "source":"okx/funding-rate-history",
                        "capture_hash":hash,
                        "requested_start_ms":request.start_ms,
                        "requested_end_ms":request.end_ms,
                        "timeframe_ms":request.timeframe_ms,
                        "observed_events":events.len(),
                        "first_event_ms":events.first().map(|(time, _)| time),
                        "last_event_ms":events.last().map(|(time, _)| time),
                        "rate_basis":"realized_settlement_rate",
                        "publication_time_verified":false,
                        "expected_events":null
                    }))
                })
                .transpose()?;
            captures.insert(
                "funding_rate".into(),
                funding_capture.unwrap_or(Value::Null),
            );
        }
        let candle_capture = (needs_candles && candles_available)
            .then(|| -> Result<Value, String> {
                let hash = okx_research::canonical_sha256(&(
                    "okx.market.candle-source-capture/v1",
                    &row.instrument,
                    request.start_ms,
                    request.end_ms,
                    request.timeframe_ms,
                    bars.as_ref(),
                ))
                .map_err(|error| error.to_string())?;
                Ok(json!({
                    "source":"okx/closed_candles",
                    "capture_hash":hash,
                    "observed_points":bars.len(),
                    "availability":"after_close",
                }))
            })
            .transpose()?;
        captures.insert("candles".into(), candle_capture.unwrap_or(Value::Null));
        Some(Value::Object(captures))
    } else {
        None
    };
    let mut result = json!({"instrument":row.instrument,"metrics":metrics,"units":units,"parameters":{"ema_period":plan.parameters.get("ema_period"),"atr_period":plan.parameters.get("atr_period"),"rsi_period":plan.parameters.get("rsi_period")},"source_start_ms":bars.first().map(|b|b.time),"source_end_ms":bars.last().map(|b|b.time),"unclosed_candles_excluded":row.excluded,"candle_coverage":candle_coverage,"quality":if gaps.is_empty(){"complete"}else{"partial"},"gaps":gaps});
    if composition.is_some() {
        result["composition_quality"] = Value::Object(composition_quality);
    }
    if request.fields.iter().any(|field| field == "oi_change") || composition_oi {
        result["oi_coverage"] = json!(oi_coverage);
    }
    if let Some(source_captures) = source_captures {
        result["source_captures"] = source_captures;
    }
    if let Some(source_alignment) = source_alignment {
        result["source_alignment"] = source_alignment;
    }
    if let Some(funding_event_alignment) = funding_event_alignment {
        result["funding_event_alignment"] = funding_event_alignment;
    }
    let mut metric_time_basis = serde_json::Map::new();
    if let Some(composition) = composition {
        for (id, sources) in composition.result_sources() {
            if sources.contains(&okx_analysis::query_plan::Source::Funding) {
                metric_time_basis.insert(id, json!("funding_event_time_publication_unverified"));
            } else if sources.contains(&okx_analysis::query_plan::Source::OpenInterest) {
                metric_time_basis.insert(id, json!("oi_event_time_publication_unverified"));
            }
        }
    }
    if spread_requested {
        metric_time_basis.insert("spread_bps".into(), json!("current_snapshot_unaligned"));
    }
    if funding_requested {
        metric_time_basis.insert(
            "funding_sum".into(),
            json!("funding_event_time_publication_unverified"),
        );
        result["funding_observations"] = row
            .funding
            .as_ref()
            .and_then(|source| source.as_ref().ok())
            .map(|events| {
                json!({
                    "observed_events": events.len(),
                    "first_event_ms": events.first().map(|(time, _)| time),
                    "last_event_ms": events.last().map(|(time, _)| time),
                    "rate_basis": "realized_settlement_rate",
                    "publication_time_verified": false,
                    "expected_events": null
                })
            })
            .unwrap_or(Value::Null);
    }
    if !metric_time_basis.is_empty() {
        result["metric_time_basis"] = Value::Object(metric_time_basis);
    }
    Ok(result)
}

/// Closed-bar log returns indexed by the *later* candle. Missing, duplicate,
/// out-of-order, nonpositive and nonfinite prices invalidate the series.
fn log_returns(
    bars: &[Bar],
    timeframe_ms: u64,
) -> Option<Vec<okx_analysis::time_series::TimedValue>> {
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
        out.push(okx_analysis::time_series::TimedValue {
            time_ms: pair[1].time,
            // Candle time is its OPEN; its return is not known until CLOSE.
            available_at_ms: pair[1].time.checked_add(timeframe_ms)?,
            value: (pair[1].close / pair[0].close).ln(),
        });
    }
    Some(out)
}
fn pairwise_matrix(loaded: &[Loaded], timeframe_ms: u64, end_ms: u64) -> Value {
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
    // Upper triangle only: source coverage already describes the diagonal,
    // and the other half has the same timestamps with reversed ownership.
    let mut pair_diagnostics = Vec::new();
    let mut complete = true;
    for (left_idx, left) in series.iter().enumerate() {
        let mut row = Vec::with_capacity(series.len());
        let mut samples = Vec::with_capacity(series.len());
        for (right_idx, right) in series.iter().enumerate() {
            let (correlation, n) = match (left, right) {
                (Some(a), Some(b)) => match okx_analysis::time_series::align_exact_at(a, b, end_ms)
                {
                    Ok((pairs, alignment)) => {
                        let correlation = okx_analysis::operators::correlation_pairs(&pairs);
                        if left_idx < right_idx {
                            pair_diagnostics.push(json!({
                                "left_index":left_idx,"right_index":right_idx,
                                "matched":alignment.matched,
                                "left_unmatched":alignment.left_unmatched,
                                "right_unmatched":alignment.right_unmatched,
                                "first_unmatched_ms":alignment.first_unmatched_ms,
                                "left_unavailable_at_cutoff":alignment.left_unavailable,
                                "right_unavailable_at_cutoff":alignment.right_unavailable,
                                "first_unavailable_ms":alignment.first_unavailable_ms,
                                "reason":if correlation.is_some() {"AVAILABLE"}
                                    else if alignment.matched < 30 {"INSUFFICIENT_COMMON_PAIRS"}
                                    else {"ZERO_VARIANCE_OR_NONFINITE_CORRELATION"}
                            }));
                        }
                        (correlation, alignment.matched)
                    }
                    Err(reason) => {
                        if left_idx < right_idx {
                            pair_diagnostics.push(json!({
                                "left_index":left_idx,"right_index":right_idx,
                                "matched":0,"reason":reason
                            }));
                        }
                        (None, 0)
                    }
                },
                _ => {
                    if left_idx < right_idx {
                        pair_diagnostics.push(json!({
                            "left_index":left_idx,"right_index":right_idx,
                            "matched":0,"reason":"SOURCE_SERIES_UNAVAILABLE_OR_GAPPED"
                        }));
                    }
                    (None, 0)
                }
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
    let per_source = series
        .iter()
        .map(|samples| {
            samples.as_ref().and_then(|s| {
                okx_analysis::time_series::align_exact_at(s, s, end_ms)
                    .ok()
                    .map(|(_, counts)| counts)
            })
        })
        .collect::<Vec<_>>();
    let usable_samples = per_source
        .iter()
        .map(|counts| counts.map(|counts| counts.left_usable))
        .collect::<Vec<_>>();
    let source_coverage = loaded
        .iter()
        .zip(series.iter())
        .zip(per_source.iter())
        .map(|((source, returns), counts)| match (returns, counts) {
            (Some(returns), Some(counts)) => json!({
                "instrument":source.instrument,
                "source_state":"valid_closed_return_series",
                "return_observations":counts.left_total,
                "usable_at_cutoff":counts.left_usable,
                "not_available_at_cutoff":counts.left_unavailable,
                "nonfinite_observations":counts.left_nonfinite,
                "first_return_event_ms":returns.first().map(|v|v.time_ms),
                "last_return_event_ms":returns.last().map(|v|v.time_ms),
            }),
            _ => json!({
                "instrument":source.instrument,
                "source_state": if source.bars.is_err() {"source_unavailable"}
                    else {"invalid_or_gapped_closed_candles"},
                "return_observations":0,
                "usable_at_cutoff":0,
                "not_available_at_cutoff":0,
                "nonfinite_observations":0,
                "first_return_event_ms":null,
                "last_return_event_ms":null,
            }),
        })
        .collect::<Vec<_>>();
    json!({"basis":"aligned_closed_bar_log_returns","instruments":loaded.iter()
        .map(|r|r.instrument.clone()).collect::<Vec<_>>(),"values":values,
        "sample_counts":counts,"usable_samples":usable_samples,
        "source_coverage":source_coverage,"pair_diagnostics":pair_diagnostics,
        "join_semantics":"exact_event_time_no_forward_fill",
        "availability_rule":"candle_close_boundary","decision_cutoff_ms":end_ms,
        "quality":if complete{"complete"}else{"partial"}})
}

#[cfg(test)]
#[path = "tests/market.rs"]
mod tests;
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
                let ticker: okx_research_protocol::research_ipc::ResearchTicker =
                    serde_json::from_value(
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
#[path = "tests/market_global_rank.rs"]
mod global_rank_tests;

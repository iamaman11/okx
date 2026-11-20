use super::*;
use okx_analysis::operators::{Operator, registry, scalar};
use replay::Bar;
use serde::{Serialize, de::DeserializeOwned, ser::SerializeSeq};
use sha2::Digest;
use std::{io::Write, marker::PhantomData};
use tracing::Instrument;
#[derive(Clone)]
struct Loaded {
    instrument: String,
    bars: Result<Arc<Vec<Bar>>, String>,
    ticker: Option<Result<okx_protocol::research_ipc::ResearchTicker, String>>,
    funding: Option<Result<Vec<(u64, f64)>, String>>,
    // Keep OI event timestamps; endpoint values alone cannot prove coverage.
    oi: Option<Result<Vec<(u64, f64)>, String>>,
    excluded: Option<usize>,
}

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

// Completed market jobs survive Worker releases. A changed result contract must
// use a new job namespace, or the old immutable evidence is replayed instead
// of being recalculated. Limit the T06 bump to funding consumers so unrelated
// historical market jobs keep their existing cache entries.
fn market_job_schema(fields: &[String]) -> &'static str {
    let funding = fields.iter().any(|field| field == "funding_sum");
    let correlation = fields.iter().any(|field| field == "pearson_correlation");
    if funding && correlation {
        "market/518.2.funding-correlation-pair-diagnostics-v1"
    } else if correlation {
        "market/518.2.correlation-pair-diagnostics-v1"
    } else if funding {
        "market/448.4.funding-realized-only-v1"
    } else {
        "market/448.2"
    }
}

// Composed results now retain immutable normalized observations as well as
// their capture hashes. Completed old jobs cannot provide this witness, so
// isolate the new contract without invalidating legacy flat queries.
fn market_request_schema(
    fields: &[String],
    has_composition: bool,
    oi_composition: bool,
    funding_composition: bool,
) -> &'static str {
    if has_composition || oi_composition || funding_composition {
        "market/518.8.verified-source-pages-v1"
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
        // A cursor reads its immutable original result. Only a new job needs
        // to bind the resolved venue instruments before durable admission.
        // The checked operation IDs/parameters remain those of the first plan.
        let execution_plan = query_plan
            .clone()
            .with_resolved_universe(compute_request.instruments.clone(), global_rank)?;
        let (job, new, cancel) = self
            .jobs
            .enqueue_async(
                okx_protocol::research_ipc::IdempotencyKey::new(format!("market-{hash}"))
                    .map_err(str::to_owned)?,
                hash.clone(),
            )
            .await?;
        tracing::info!(job_id = %job.id, stage = "admission", state = if new { "queued" } else { "replayed" }, "market job admission");
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
                    this.jobs
                        .update_async(hash.clone(), jobs::State::Completed, Some(artifact), None)
                        .await?;
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
        cancel: Arc<super::executor::Cancel>,
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
        let (rows, correlation_matrix) = self
            .market_rows(&request, cancel.clone(), global_rank, plan.clone())
            .await?;
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
        let result_artifact=store.publish_evidence(&json!({"kind":"market_pages","producer":producer,"row_pages":row_pages,"total_rows":total,"sources":plan.sources,"correlation_matrix":correlation_matrix})).map_err(|e|e.to_string())?.0;
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
                let (time_basis, data_availability) = match operator {
                    Operator::SpreadBps => (
                        "current_ticker_snapshot_not_historical",
                        "live_snapshot_timestamp_unverified",
                    ),
                    Operator::FundingSum => (
                        "realized_funding_event_time_not_publication_time",
                        "historical_publication_time_unverified",
                    ),
                    Operator::OiChange => (
                        "open_interest_event_time_not_publication_time",
                        "historical_publication_time_unverified",
                    ),
                    Operator::PearsonCorrelation => (
                        "exact_closed_candle_join",
                        "requires_thirty_common_eligible_pairs",
                    ),
                    Operator::PositionSize | Operator::CostEstimate => {
                        ("request_parameters", "requires_typed_request_arguments")
                    }
                    _ => ("closed_candles", "checked_per_request"),
                };
                let typed_inputs = info
                    .inputs
                    .iter()
                    .map(|name| {
                        okx_analysis::query_plan::input(name).expect("registered operator input")
                    })
                    .collect::<Vec<_>>();
                json!({"name":operator,"version":1,"status":"code_supported",
                    "route_status":"public_market_screen_compare",
                    "data_availability":data_availability,"time_basis":time_basis,
                    "output_shape":"scalar","unit":info.unit,"inputs":info.inputs,
                    "typed_inputs":typed_inputs})
            })
            .collect::<Vec<_>>();
        // Keep the primitive catalog separate from the enclosing capability
        // object: a single giant json! expansion hits Rust's macro recursion
        // limit as the finite typed operator set grows.
        let composition_operations = json!([
            {"op":"returns_series","input_unit":"price_or_contracts_or_native_volume","output_unit":"ratio"},
            {"op":"rolling_mean","input_unit":"price_or_ratio_or_contracts_or_native_volume_or_coefficient","output_unit":"same_as_input","period_min":1,"period_max":1000,"compute_cost":"linear_in_series_length"},
            {"op":"rolling_max","input_unit":"price_or_ratio_or_contracts_or_native_volume_or_coefficient","output_unit":"same_as_input","period_min":1,"period_max":1000,"null_policy":"propagate_missing"},
            {"op":"rolling_min","input_unit":"price_or_ratio_or_contracts_or_native_volume_or_coefficient","output_unit":"same_as_input","period_min":1,"period_max":1000,"null_policy":"propagate_missing"},
            {"op":"rolling_pearson","input_unit":"ratio","other_unit":"ratio","output_unit":"coefficient","period_min":30,"period_max":1000,"null_policy":"strict_exact_pairs","compute_cost":"linear_in_series_length","minimum_pairs":30},
            {"op":"rolling_zscore","version":1,"input_unit":"price_or_ratio_or_contracts_or_native_volume_or_coefficient","output_unit":"coefficient","period_min":2,"period_max":1000,"variance":"population_inclusive_window","null_policy":"strict_window_or_zero_variance","compute_cost":"linear_in_series_length","causal":true},
            {"op":"rolling_stddev","version":1,"input_unit":"price_or_ratio_or_contracts_or_native_volume_or_coefficient","output_unit":"same_as_input","period_min":2,"period_max":1000,"variance":"population_inclusive_window","null_policy":"strict_window_zero_for_constant","compute_cost":"linear_in_series_length","causal":true},
            {"op":"add_series","input_unit":"ratio","other_unit":"ratio","output_unit":"ratio","null_policy":"propagate_missing"},
            {"op":"subtract_series","input_unit":"ratio","other_unit":"ratio","output_unit":"ratio","null_policy":"propagate_missing","numeric_policy":"finite_only"},
            {"op":"divide_series","input_unit":"ratio","other_unit":"ratio","output_unit":"ratio","null_policy":"propagate_missing_or_zero_denominator","numeric_policy":"finite_only"},
            {"op":"and","input_unit":"boolean","other_unit":"boolean","output_unit":"boolean","null_policy":"propagate_unknown","causal":true},
            {"op":"or","input_unit":"boolean","other_unit":"boolean","output_unit":"boolean","null_policy":"propagate_unknown","causal":true},
            {"op":"lag","input_unit":"causal_series","output_unit":"same_as_input","period_min":1,"period_max":1000},
            {"op":"greater_than","input_unit":"price_or_ratio_or_native_volume_or_coefficient","output_unit":"boolean","requires":"finite_threshold"},
            {"op":"greater_than_series","input_unit":"price_or_ratio_or_contracts_or_native_volume_or_coefficient","other_unit":"same_as_input","output_unit":"boolean","null_policy":"propagate_unknown","causal":true},
            {"op":"less_than","input_unit":"price_or_ratio_or_native_volume_or_coefficient","output_unit":"boolean","requires":"finite_threshold"},
            {"op":"less_than_series","input_unit":"price_or_ratio_or_contracts_or_native_volume_or_coefficient","other_unit":"same_as_input","output_unit":"boolean","null_policy":"propagate_unknown","causal":true},
            {"op":"rising_edge","input_unit":"boolean","output_unit":"event","trigger":"false_to_true"},
            {"op":"falling_edge","input_unit":"boolean","output_unit":"event","trigger":"true_to_false"},
            {"op":"forward_return_label","input_unit":"price","output_unit":"outcome_ratio","requires":"prior_event_step_in_when","period_min":1,"period_max":1000,"causal_feature":false}
        ]);
        let mut capabilities = json!({"schema":"okx.market.capabilities/v1",
            "route_owner":"okx-research-worker","compute_owner":"okx-kernel",
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
                    "metric_reexecution_verified":false,
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
        cancel: Arc<super::executor::Cancel>,
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
    cancel: &'a super::executor::Cancel,
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
            let rows: Vec<T> = self
                .store
                .read_evidence_json(id)
                .map_err(serde::ser::Error::custom)?;
            if rows.is_empty()
                || rows.len() > MARKET_SOURCE_EVIDENCE_CHUNK
                || (index + 1 < self.chunks.len() && rows.len() != MARKET_SOURCE_EVIDENCE_CHUNK)
            {
                return Err(serde::ser::Error::custom("COMPOSITION_SOURCE_PAGE_SIZE_INVALID"));
            }
            observed = observed
                .checked_add(rows.len())
                .ok_or_else(|| serde::ser::Error::custom("COMPOSITION_SOURCE_COUNT_OVERFLOW"))?;
            if observed > self.expected {
                return Err(serde::ser::Error::custom("COMPOSITION_SOURCE_COUNT_MISMATCH"));
            }
            for row in &rows {
                sequence.serialize_element(row)?;
            }
        }
        if observed != self.expected {
            return Err(serde::ser::Error::custom("COMPOSITION_SOURCE_COUNT_MISMATCH"));
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
    source: &str,
    hash_domain: &str,
    time_basis: &str,
    cancel: &super::executor::Cancel,
) -> Result<Value, String> {
    let invalid = || "COMPOSITION_SOURCE_EVIDENCE_MISMATCH".to_owned();
    let manifest_id = capture["evidence_artifact"].as_str().ok_or_else(invalid)?;
    let manifest: Value = store.read_evidence_json(manifest_id).map_err(|_| invalid())?;
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
    let rows = VerifiedSourcePages::<T> {
        store,
        chunks: &ids,
        expected,
        cancel,
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
    Ok(json!({"source":source,"quality":"verified_normalized_capture",
        "pages":ids.len(),"observed_points":expected,"capture_hash":reconstructed_hash,
        "publication_time_verified":false}))
}

fn verify_composition_source_pages(
    store: &ResearchArtifactStore,
    row: &Value,
    request: &MarketRequest,
    plan: &okx_analysis::query_plan::QueryPlan,
    cancel: &super::executor::Cancel,
) -> Result<Value, String> {
    use okx_analysis::query_plan::Source;
    let graph = plan
        .composition
        .as_ref()
        .ok_or("COMPOSITION_EVIDENCE_WITHOUT_PLAN")?;
    let required = graph.sources();
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
                store, request, instrument, capture, name, domain, time_basis, cancel,
            )?,
            _ => verify_one_source_pages::<(u64, f64)>(
                store, request, instrument, capture, name, domain, time_basis, cancel,
            )?,
        });
    }
    Ok(json!({"schema":"okx.market.composition-source-integrity/v1",
        "quality":if unavailable.is_empty(){"verified_normalized_sources"}else{"partial_source_unavailable"},
        "verified_sources":verified,"unavailable_sources":unavailable,
        "replay_metric_reexecution_verified":false,
        "historical_publication_time_verified":false,
        "proof":"bounded_chunk_read_identity_and_reconstructed_canonical_capture_hash"}))
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
    cancel: &super::executor::Cancel,
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
    cancel: &super::executor::Cancel,
) -> Result<Vec<PublishedMarketSource>, String> {
    use okx_analysis::query_plan::Source;
    let composition_sources = plan
        .composition
        .as_ref()
        .ok_or("COMPOSITION_EVIDENCE_WITHOUT_PLAN")?
        .sources();
    let mut evidence = Vec::with_capacity(3);
    if plan.sources.contains(&Source::Candles) {
        if let Ok(bars) = &row.bars {
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
    }
    if composition_sources.contains(&Source::OpenInterest) {
        if let Some(Ok(points)) = &row.oi {
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
    }
    if composition_sources.contains(&Source::Funding) {
        if let Some(Ok(events)) = &row.funding {
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
            let mut opens = composition.uses_source("open").then(|| vec![f64::NAN; count]);
            let mut highs = composition.uses_source("high").then(|| vec![f64::NAN; count]);
            let mut lows = composition.uses_source("low").then(|| vec![f64::NAN; count]);
            let mut volumes = composition.uses_source("volume").then(|| vec![f64::NAN; count]);
            let range = okx_analysis::time_series::ClosedRange::strict(
                request.start_ms,
                request.end_ms,
                request.timeframe_ms,
            )?;
            for bar in bars.iter() {
                if let Some(offset) = bar.time.checked_sub(request.start_ms)
                    && offset % request.timeframe_ms == 0
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
        if composition_oi {
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
        if composition_funding {
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
mod tests {
    use super::*;

    #[test]
    fn t02_funding_composition_rejects_non_swap_before_source_reads() {
        let spot = vec!["BTC-USDT-SWAP".to_owned(), "ETH-USDT".to_owned()];
        assert_eq!(
            validate_composition_source_products(&spot, false, true).unwrap_err(),
            "composition.sources.funding_rate:UNSUPPORTED_PRODUCT"
        );
        assert_eq!(
            validate_composition_source_products(&spot, true, true).unwrap_err(),
            "composition.sources.open_interest:UNSUPPORTED_PRODUCT"
        );
        assert!(
            validate_composition_source_products(&["BTC-USDT-SWAP".into()], true, true).is_ok()
        );
        assert!(validate_composition_source_products(&spot, false, false).is_ok());
    }

    #[test]
    fn t13_composition_evidence_uses_a_new_namespace_without_invalidating_legacy_results() {
        let fields = vec!["returns".to_owned()];
        let oi_fields = vec!["pearson_correlation".to_owned()];
        assert_eq!(
            market_request_schema(&fields, false, false, false),
            "market/448.2"
        );
        let composed = market_request_schema(&fields, true, false, false);
        let oi_composed = market_request_schema(&fields, true, true, false);
        let oi_correlated = market_request_schema(&oi_fields, true, true, false);
        let funding_composed = market_request_schema(&fields, true, false, true);
        assert_eq!(composed, "market/518.8.verified-source-pages-v1");
        assert_eq!(oi_composed, composed);
        assert_ne!(composed, "market/518.2.composition-quality-v1");
        assert_ne!(oi_composed, "market/518.2.oi-composition-quality-v1");
        assert_ne!(
            composed,
            market_request_schema(&fields, false, false, false)
        );
        assert_eq!(oi_correlated, oi_composed);
        assert_eq!(
            funding_composed,
            "market/518.8.verified-source-pages-v1"
        );
        assert_eq!(
            funding_composed,
            market_request_schema(&fields, true, true, true)
        );
        assert_eq!(
            market_request_schema(&oi_fields, false, false, false),
            "market/518.2.correlation-pair-diagnostics-v1"
        );
    }

    #[test]
    fn t06_funding_result_schema_invalidates_only_funding_jobs() {
        let funding = request(vec!["funding_sum"]);
        let mixed = request(vec!["returns", "funding_sum", "spread_bps"]);
        let returns = request(vec!["returns"]);
        let correlation = request(vec!["pearson_correlation"]);
        let combined = request(vec!["funding_sum", "pearson_correlation"]);
        assert_eq!(
            market_job_schema(&funding.fields),
            market_job_schema(&mixed.fields)
        );
        assert_eq!(market_job_schema(&returns.fields), "market/448.2");
        assert_eq!(
            market_job_schema(&correlation.fields),
            "market/518.2.correlation-pair-diagnostics-v1"
        );
        assert_ne!(
            market_job_schema(&combined.fields),
            market_job_schema(&funding.fields)
        );
        assert_ne!(
            market_job_schema(&combined.fields),
            market_job_schema(&correlation.fields)
        );

        // Reproduce the exact persisted pre-T06 key for a closed historical
        // query. The new key must not replay that completed stale artifact.
        let old = okx_research::canonical_sha256(&(
            "market/448.2",
            &funding,
            &Option::<Value>::None,
            0_i64,
        ))
        .unwrap();
        let new = okx_research::canonical_sha256(&(
            market_job_schema(&funding.fields),
            &funding,
            &Option::<Value>::None,
            0_i64,
        ))
        .unwrap();
        assert_ne!(old, new);

        // #525 used this domain. Its old completed funding artifacts may
        // include a forecast fallback and cannot be reused after this change.
        let previous = okx_research::canonical_sha256(&(
            "market/448.3.funding-publication-v1",
            &funding,
            &Option::<Value>::None,
            0_i64,
        ))
        .unwrap();
        assert_ne!(previous, new);

        // Non-funding research retains its original deterministic job key.
        let legacy_returns = okx_research::canonical_sha256(&(
            "market/448.2",
            &returns,
            &Option::<Value>::None,
            0_i64,
        ))
        .unwrap();
        let current_returns = okx_research::canonical_sha256(&(
            market_job_schema(&returns.fields),
            &returns,
            &Option::<Value>::None,
            0_i64,
        ))
        .unwrap();
        assert_eq!(legacy_returns, current_returns);
    }

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
        let m = pairwise_matrix(&series, 60000, 6_000_000);
        assert_eq!(m["basis"], "aligned_closed_bar_log_returns");
        assert_eq!(m["instruments"].as_array().unwrap().len(), 3);
        assert_eq!(m["sample_counts"][1][2], 99);
        assert_eq!(m["values"][0][1], m["values"][1][0]);
        assert_eq!(m["values"][1][2], m["values"][2][1]);
        assert!(m["values"][0][0].as_f64().unwrap() > 0.99);
        assert!(m["values"][0][2].as_f64().unwrap() < 0.0);
    }
    #[test]
    fn t06_pearson_needs_thirty_known_aligned_pairs_not_future_or_shifted_data() {
        let mut rows = Vec::new();
        for (instrument, factor) in [("BTC-USDT", 1.0), ("ETH-USDT", 2.0)] {
            let mut price = 100.0;
            let bars = (0..31)
                .map(|i| {
                    if i > 0 {
                        price *= (((i % 7) as f64 - 3.0) * 0.002 * factor).exp();
                    }
                    Bar {
                        time: i * 60_000,
                        open: price,
                        high: price,
                        low: price,
                        close: price,
                        volume: 1.0,
                        funding_rate: None,
                    }
                })
                .collect();
            rows.push(Loaded {
                instrument: instrument.into(),
                bars: Ok(Arc::new(bars)),
                ticker: None,
                funding: None,
                oi: None,
                excluded: None,
            });
        }
        let full = pairwise_matrix(&rows, 60_000, 31 * 60_000);
        assert_eq!(full["join_semantics"], "exact_event_time_no_forward_fill");
        assert_eq!(full["sample_counts"][0][1], 30);
        assert_eq!(full["usable_samples"], json!([30, 30]));
        assert_eq!(full["source_coverage"][0]["return_observations"], 30);
        assert_eq!(full["source_coverage"][1]["usable_at_cutoff"], 30);
        assert_eq!(full["pair_diagnostics"][0]["reason"], "AVAILABLE");
        assert_eq!(full["pair_diagnostics"][0]["matched"], 30);
        assert_eq!(full["pair_diagnostics"].as_array().unwrap().len(), 1);
        assert_eq!(full["source_coverage"][0]["not_available_at_cutoff"], 0);
        assert!((full["values"][0][1].as_f64().unwrap() - 1.0).abs() < 1e-9);

        // Last bar was observed in the recorded fixture but cannot be used
        // before its close. No post-decision look-ahead is permitted.
        let early = pairwise_matrix(&rows, 60_000, 30 * 60_000);
        assert_eq!(early["sample_counts"][0][1], 29);
        assert_eq!(early["usable_samples"], json!([29, 29]));
        assert_eq!(early["source_coverage"][0]["not_available_at_cutoff"], 1);
        assert_eq!(early["source_coverage"][1]["usable_at_cutoff"], 29);
        assert_eq!(
            early["pair_diagnostics"][0]["reason"],
            "INSUFFICIENT_COMMON_PAIRS"
        );
        assert_eq!(
            early["pair_diagnostics"][0]["first_unavailable_ms"],
            1_800_000
        );
        assert!(early["values"][0][1].is_null());

        // Equal array lengths cannot substitute for equal timestamps.
        let mut shifted = rows;
        for bar in Arc::make_mut(shifted[1].bars.as_mut().unwrap()).iter_mut() {
            bar.time += 60_000;
        }
        let unaligned = pairwise_matrix(&shifted, 60_000, 32 * 60_000);
        assert_eq!(unaligned["sample_counts"][0][1], 29);
        assert_eq!(unaligned["usable_samples"], json!([30, 30]));
        assert_eq!(unaligned["pair_diagnostics"][0]["left_unmatched"], 1);
        assert_eq!(unaligned["pair_diagnostics"][0]["right_unmatched"], 1);
        assert_eq!(
            unaligned["pair_diagnostics"][0]["first_unmatched_ms"],
            60_000
        );
        assert!(unaligned["values"][0][1].is_null());
        shifted[1].bars = Err("SOURCE_UNAVAILABLE".into());
        let absent = pairwise_matrix(&shifted, 60_000, 32 * 60_000);
        assert_eq!(
            absent["source_coverage"][1]["source_state"],
            "source_unavailable"
        );
        assert!(absent["usable_samples"][1].is_null());
        assert_eq!(absent["sample_counts"][0][1], 0);
        assert_eq!(
            absent["pair_diagnostics"][0]["reason"],
            "SOURCE_SERIES_UNAVAILABLE_OR_GAPPED"
        );
    }
    #[test]
    fn t06_oi_interior_gap_is_exposed_and_never_accepted_as_endpoint_only() {
        let mut query = request(vec!["oi_change"]);
        query.timeframe_ms = 300_000;
        query.end_ms = 1_500_000;
        let mut source = row();
        source.bars = Ok(Arc::new(Vec::new()));
        source.oi = Some(Ok(vec![
            (0, 10.0),
            (300_000, 12.0),
            (900_000, 16.0),
            (1_200_000, 18.0),
        ]));
        let partial = compute_row(source, &query, None).unwrap();
        assert_eq!(partial["quality"], "partial");
        assert_eq!(partial["metrics"]["oi_change"], Value::Null);
        assert!(partial["candle_coverage"].is_null());
        assert_eq!(partial["oi_coverage"]["expected_bars"], 5);
        assert_eq!(partial["oi_coverage"]["observed_unique_bars"], 4);
        assert_eq!(partial["oi_coverage"]["first_missing_ms"], 600_000);
        assert!(
            partial["gaps"]
                .as_array()
                .unwrap()
                .contains(&json!("OI_RANGE_INCOMPLETE_OR_GAPPED"))
        );

        let mut full_source = row();
        full_source.bars = Ok(Arc::new(Vec::new()));
        full_source.oi = Some(Ok(vec![
            (0, 10.0),
            (300_000, 12.0),
            (600_000, 14.0),
            (900_000, 16.0),
            (1_200_000, 18.0),
        ]));
        let full = compute_row(full_source, &query, None).unwrap();
        assert_eq!(full["quality"], "complete");
        assert!(full["candle_coverage"].is_null());
        assert_eq!(full["oi_coverage"]["complete"], true);
        assert!((full["metrics"]["oi_change"].as_f64().unwrap() - 0.8).abs() < 1e-12);
    }
    #[test]
    fn t06_current_ticker_is_not_marked_as_historical_spread() {
        let mut source = row();
        source.ticker = Some(Ok(okx_protocol::research_ipc::ResearchTicker {
            bid_price: "100".into(),
            ask_price: "101".into(),
        }));
        let response = compute_row(source, &request(vec!["spread_bps"]), None).unwrap();
        assert!(response["metrics"]["spread_bps"].is_number());
        assert_eq!(
            response["metric_time_basis"]["spread_bps"],
            "current_snapshot_unaligned"
        );
        assert!(response["candle_coverage"].is_null());
        assert_eq!(response["quality"], "partial");
        assert!(
            response["gaps"]
                .as_array()
                .unwrap()
                .contains(&json!("SPREAD_CURRENT_SNAPSHOT_NOT_HISTORICAL"))
        );
    }
    #[test]
    fn t06_funding_event_time_does_not_prove_publication_time() {
        let mut source = row();
        source.funding = Some(Ok(vec![(60_000, 0.01), (120_000, -0.002)]));
        let response = compute_row(source, &request(vec!["funding_sum"]), None).unwrap();
        assert!((response["metrics"]["funding_sum"].as_f64().unwrap() - 0.008).abs() < 1e-12);
        assert!(response["candle_coverage"].is_null());
        assert_eq!(response["quality"], "partial");
        assert_eq!(
            response["metric_time_basis"]["funding_sum"],
            "funding_event_time_publication_unverified"
        );
        assert_eq!(response["funding_observations"]["observed_events"], 2);
        assert_eq!(response["funding_observations"]["first_event_ms"], 60_000);
        assert_eq!(response["funding_observations"]["last_event_ms"], 120_000);
        assert_eq!(
            response["funding_observations"]["rate_basis"],
            "realized_settlement_rate"
        );
        assert_eq!(
            response["funding_observations"]["publication_time_verified"],
            false
        );
        assert!(response["funding_observations"]["expected_events"].is_null());
        assert!(
            response["gaps"]
                .as_array()
                .unwrap()
                .contains(&json!("FUNDING_PUBLICATION_TIME_UNVERIFIED"))
        );
    }
    #[test]
    fn t06_mixed_funding_ticker_and_candles_keep_distinct_time_evidence() {
        let mut source = row();
        source.funding = Some(Ok(vec![(60_000, 0.001)]));
        source.ticker = Some(Ok(okx_protocol::research_ipc::ResearchTicker {
            bid_price: "100".into(),
            ask_price: "101".into(),
        }));
        let response = compute_row(
            source,
            &request(vec!["returns", "funding_sum", "spread_bps"]),
            None,
        )
        .unwrap();
        assert_eq!(response["candle_coverage"]["complete"], true);
        assert!(response["metrics"]["returns"].is_number());
        assert_eq!(response["metrics"]["funding_sum"], json!(0.001));
        assert_eq!(
            response["metric_time_basis"]["funding_sum"],
            "funding_event_time_publication_unverified"
        );
        assert_eq!(
            response["metric_time_basis"]["spread_bps"],
            "current_snapshot_unaligned"
        );
        assert_eq!(response["quality"], "partial");
    }
    #[test]
    fn t06_missing_funding_source_has_explicit_first_failure() {
        let mut source = row();
        source.funding = Some(Err("SOURCE_TIMEOUT".into()));
        let response = compute_row(source, &request(vec!["funding_sum"]), None).unwrap();
        assert!(response["metrics"]["funding_sum"].is_null());
        assert!(response["funding_observations"].is_null());
        assert!(
            response["gaps"]
                .as_array()
                .unwrap()
                .contains(&json!("FUNDING_SOURCE_UNAVAILABLE"))
        );
        assert_eq!(response["quality"], "partial");
    }
    #[test]
    fn t06_missing_realized_rate_blocks_historical_sum_with_specific_reason() {
        let mut source = row();
        source.funding = Some(Err("FUNDING_REALIZED_RATE_UNAVAILABLE".into()));
        let response = compute_row(source, &request(vec!["funding_sum"]), None).unwrap();
        assert!(response["metrics"]["funding_sum"].is_null());
        assert!(response["funding_observations"].is_null());
        assert_eq!(response["quality"], "partial");
        assert_eq!(response["gaps"][0], "FUNDING_REALIZED_RATE_UNAVAILABLE");
        assert!(
            response["gaps"]
                .as_array()
                .unwrap()
                .contains(&json!("FUNDING_SOURCE_UNAVAILABLE"))
        );
    }
    #[test]
    fn source_specific_metrics_do_not_hide_candle_coverage_in_mixed_requests() {
        let response = compute_row(row(), &request(vec!["returns", "oi_change"]), None).unwrap();
        assert_eq!(response["candle_coverage"]["complete"], true);
        assert!(response["oi_coverage"].is_null());
        assert_eq!(response["quality"], "partial");
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
    fn c20_rolling_mean_reuses_existing_market_pipeline_without_new_routing() {
        let query = request(vec!["rolling_mean"]);
        assert!(query.valid());
        let plan =
            planner::AnalysisPlan::build(&query.fields, query.start_ms, query.end_ms).unwrap();
        assert_eq!(plan.sources, std::collections::BTreeSet::from(["candles"]));
        let response = compute_row(row(), &query, None).unwrap();
        assert_eq!(response["units"]["rolling_mean"], "price");
        assert!(response["metrics"]["rolling_mean"].is_number());
        let mut missing = row();
        missing.bars = Ok(Arc::new(Vec::new()));
        let output = compute_row(missing, &query, None).unwrap();
        assert_eq!(output["quality"], "partial");
        assert!(output["metrics"]["rolling_mean"].is_null());
    }
    #[test]
    fn t02_t03_public_row_uses_checked_operator_and_parameter_binding() {
        let query = request(vec!["ema", "returns"]);
        let checked = planner::AnalysisPlan::compile_market(&query).unwrap();
        assert_eq!(checked.parameters["ema_period"], 14.0);
        let result = compute_row_planned(row(), &query, None, &checked, true).unwrap();
        let direct = compute_row(row(), &query, None).unwrap();
        assert_eq!(result["metrics"], direct["metrics"]);
        assert_eq!(result["parameters"]["ema_period"], 14.0);
        let mut changed = checked.clone();
        changed.parameters.insert("ema_period".into(), 3.0);
        let value = compute_row_planned(row(), &query, None, &changed, true).unwrap();
        assert_ne!(value["metrics"]["ema"], result["metrics"]["ema"]);
        assert_eq!(value["parameters"]["ema_period"], 3.0);
        let mut mismatched = query.clone();
        mismatched.fields.reverse();
        assert_eq!(
            compute_row_planned(row(), &mismatched, None, &checked, true).unwrap_err(),
            "COMPILED_PLAN_PROJECTION_MISMATCH"
        );
    }
    #[test]
    fn t07_t15_public_boolean_composition_uses_existing_candle_execution() {
        let mut request = request(vec!["returns"]);
        request.composition = Some(Box::new(
            serde_json::from_value(json!({
                "version":okx_analysis::query_plan::COMPOSITION_VERSION,
                "steps":[
                    {"id":"returns_series","op":"returns_series","input":"close"},
                    {"id":"positive","op":"greater_than","input":"returns_series","threshold":0},
                    {"id":"previous","op":"lag","input":"positive","period":1},
                    {"id":"both","op":"and","input":"positive","other":"previous"},
                    {"id":"either","op":"or","input":"positive","other":"previous"},
                    {"id":"event","op":"rising_edge","input":"both"}
                ],
                "result_steps":["both","either","event"]
            }))
            .unwrap(),
        ));
        let plan = planner::AnalysisPlan::compile_market(&request).unwrap();
        assert_eq!(
            plan.sources,
            vec![okx_analysis::query_plan::Source::Candles]
        );
        let result = compute_row_planned(row(), &request, None, &plan, true).unwrap();
        assert_eq!(result["metrics"]["both"], true);
        assert_eq!(result["metrics"]["either"], true);
        assert_eq!(result["metrics"]["event"], false);
        assert_eq!(result["units"]["both"], "boolean");
        assert_eq!(
            result["composition_quality"]["both"]["status"],
            "available_at_closed_bar"
        );
        let mut missing = row();
        Arc::make_mut(missing.bars.as_mut().unwrap()).remove(42);
        let partial = compute_row_planned(missing, &request, None, &plan, true).unwrap();
        assert!(partial["metrics"]["both"].is_null());
        assert_eq!(
            partial["composition_quality"]["both"]["issues"][0]["first_missing_ms"],
            42 * 60_000
        );
    }
    #[test]
    fn t03_t07_t15_public_rolling_price_cross_is_one_composed_market_request() {
        let mut request = request(vec!["returns"]);
        request.composition = Some(Box::new(serde_json::from_value(json!({
            "version":okx_analysis::query_plan::COMPOSITION_VERSION,
            "steps":[
                {"id":"fast","op":"rolling_mean","input":"close","period":1},
                {"id":"slow","op":"rolling_mean","input":"close","period":3},
                {"id":"above","op":"greater_than_series","input":"fast","other":"slow"},
                {"id":"cross","op":"rising_edge","input":"above"}
            ],
            "result_steps":["above","cross"]
        })).unwrap()));
        let plan = planner::AnalysisPlan::compile_market(&request).unwrap();
        assert_eq!(plan.sources, vec![okx_analysis::query_plan::Source::Candles]);
        let mut loaded = row();
        let bars = Arc::make_mut(loaded.bars.as_mut().unwrap());
        for bar in &mut bars[96..99] {
            bar.open = 100.0;
            bar.high = 102.0;
            bar.low = 99.0;
            bar.close = 100.0;
        }
        bars[99].open = 100.0;
        bars[99].low = 99.0;
        bars[99].high = 112.0;
        bars[99].close = 110.0;
        let result = compute_row_planned(loaded, &request, None, &plan, true).unwrap();
        assert_eq!(result["metrics"]["above"], true);
        assert_eq!(result["metrics"]["cross"], true);
        assert_eq!(result["units"]["cross"], "event");
        assert_eq!(result["composition_quality"]["cross"]["status"], "available_at_closed_bar");
        assert!(result["source_captures"]["candles"]["capture_hash"].is_string());
    }
    #[test]
    fn t07_t15_public_bidirectional_exit_and_population_risk_share_the_checked_market_route() {
        let mut request = request(vec!["returns"]);
        request.composition = Some(Box::new(serde_json::from_value(json!({
            "version":okx_analysis::query_plan::COMPOSITION_VERSION,
            "steps":[
                {"id":"fast","op":"rolling_mean","input":"close","period":2},
                {"id":"slow","op":"rolling_mean","input":"close","period":3},
                {"id":"below","op":"less_than_series","input":"fast","other":"slow"},
                {"id":"exit","op":"falling_edge","input":"below"},
                {"id":"risk","op":"rolling_stddev","input":"close","period":2},
                {"id":"quiet","op":"less_than","input":"risk","threshold":4.0}
            ],
            "result_steps":["below","exit","risk","quiet"]
        })).unwrap()));
        let plan = planner::AnalysisPlan::compile_market(&request).unwrap();
        assert_eq!(plan.sources, vec![okx_analysis::query_plan::Source::Candles]);
        let mut loaded = row();
        let bars = Arc::make_mut(loaded.bars.as_mut().unwrap());
        for (index, close) in [(95, 100.0), (96, 99.0), (97, 97.0), (98, 95.0), (99, 102.0)] {
            bars[index].close = close;
            bars[index].open = close;
            bars[index].high = close + 1.0;
            bars[index].low = close - 1.0;
        }
        let result = compute_row_planned(loaded.clone(), &request, None, &plan, true).unwrap();
        assert_eq!(result["metrics"]["below"], false);
        assert_eq!(result["metrics"]["exit"], true);
        assert_eq!(result["metrics"]["risk"], 3.5);
        assert_eq!(result["metrics"]["quiet"], true);
        assert_eq!(result["units"]["risk"], "price");
        assert_eq!(result["units"]["exit"], "event");
        assert_eq!(result["composition_quality"]["risk"]["status"], "available_at_closed_bar");
        assert!(result["source_captures"]["candles"]["capture_hash"].is_string());
        Arc::make_mut(loaded.bars.as_mut().unwrap()).remove(98);
        let missing = compute_row_planned(loaded, &request, None, &plan, true).unwrap();
        assert!(missing["metrics"]["risk"].is_null());
        assert!(missing["metrics"]["exit"].is_null());
        assert_eq!(missing["composition_quality"]["risk"]["issues"][0]["reason"], "CANDLE_RANGE_INCOMPLETE_OR_GAPPED");
        assert_eq!(missing["composition_quality"]["risk"]["issues"][0]["source"], "candles");
        assert_eq!(missing["composition_quality"]["risk"]["issues"][0]["causal_null_reason"], "SOURCE_OBSERVATION_MISSING");
        assert_eq!(missing["composition_quality"]["risk"]["issues"][0]["origin_step"], "close");
        assert_eq!(missing["composition_quality"]["risk"]["issues"][0]["origin_event_ms"], 98 * 60_000);
        assert_eq!(missing["composition_quality"]["risk"]["issues"][0]["first_missing_ms"], 98 * 60_000);
    }
    #[test]
    fn t02_t04_public_ohlc_graph_diagnoses_a_missing_high_without_forward_fill() {
        let mut request = request(vec!["returns"]);
        request.composition = Some(Box::new(serde_json::from_value(json!({
            "version":okx_analysis::query_plan::COMPOSITION_VERSION,
            "steps":[
                {"id":"body","op":"greater_than_series","input":"close","other":"open"},
                {"id":"range","op":"greater_than_series","input":"high","other":"low"},
                {"id":"confirmed","op":"and","input":"body","other":"range"}
            ],
            "result_steps":["confirmed"]
        })).unwrap()));
        let plan = planner::AnalysisPlan::compile_market(&request).unwrap();
        let mut loaded = row();
        let bars = Arc::make_mut(loaded.bars.as_mut().unwrap());
        bars[99].open = 190.0;
        bars[99].low = 189.0;
        bars[99].high = 201.0;
        bars[99].close = 199.0;
        let present = compute_row_planned(loaded.clone(), &request, None, &plan, true).unwrap();
        assert_eq!(present["metrics"]["confirmed"], true);
        assert_eq!(present["units"]["confirmed"], "boolean");
        let bars = Arc::make_mut(loaded.bars.as_mut().unwrap());
        bars[99].high = f64::NAN;
        let missing = compute_row_planned(loaded, &request, None, &plan, true).unwrap();
        assert!(missing["metrics"]["confirmed"].is_null());
        assert_eq!(missing["composition_quality"]["confirmed"]["issues"][0]["origin_step"], "high");
        assert_eq!(missing["composition_quality"]["confirmed"]["issues"][0]["origin_event_ms"], 99 * 60_000);
        assert_eq!(missing["composition_quality"]["confirmed"]["issues"][0]["reason"], "SOURCE_OBSERVATION_MISSING");
    }
    #[test]
    fn t02_t04_t07_t13_native_volume_composition_preserves_units_and_source_capture() {
        let mut request = request(vec!["returns"]);
        request.composition = Some(Box::new(serde_json::from_value(json!({
            "version":okx_analysis::query_plan::COMPOSITION_VERSION,
            "steps":[
                {"id":"fast","op":"rolling_mean","input":"volume","period":2},
                {"id":"slow","op":"rolling_mean","input":"volume","period":3},
                {"id":"lift","op":"greater_than_series","input":"fast","other":"slow"},
                {"id":"entry","op":"rising_edge","input":"lift"}
            ],
            "result_steps":["fast","lift","entry"]
        })).unwrap()));
        let plan = planner::AnalysisPlan::compile_market(&request).unwrap();
        assert_eq!(plan.sources, vec![okx_analysis::query_plan::Source::Candles]);
        let mut loaded = row();
        let bars = Arc::make_mut(loaded.bars.as_mut().unwrap());
        for bar in &mut bars[95..99] {
            bar.volume = 10.0;
        }
        bars[99].volume = 30.0;
        let result = compute_row_planned(loaded.clone(), &request, None, &plan, true).unwrap();
        assert_eq!(result["metrics"]["fast"], 20.0);
        assert_eq!(result["units"]["fast"], "native_volume");
        assert_eq!(result["metrics"]["lift"], true);
        assert_eq!(result["metrics"]["entry"], true);
        assert_eq!(result["composition_quality"]["entry"]["status"], "available_at_closed_bar");
        let capture = result["source_captures"]["candles"]["capture_hash"].clone();
        assert!(capture.is_string());
        Arc::make_mut(loaded.bars.as_mut().unwrap())[99].volume = 31.0;
        let revised = compute_row_planned(loaded.clone(), &request, None, &plan, true).unwrap();
        assert_ne!(revised["source_captures"]["candles"]["capture_hash"], capture);
        Arc::make_mut(loaded.bars.as_mut().unwrap())[99].volume = -1.0;
        assert_eq!(compute_row_planned(loaded, &request, None, &plan, true).unwrap_err(),
            "composition.sources.volume:INVALID_NON_NEGATIVE_VOLUME");
    }
    #[test]
    fn t10_unprojected_oi_and_funding_branches_never_require_sources_on_spot() {
        let mut request = request(vec!["returns"]);
        request.composition = Some(Box::new(serde_json::from_value(json!({
            "version":okx_analysis::query_plan::COMPOSITION_VERSION,
            "steps":[
                {"id":"r","op":"returns_series","input":"close"},
                {"id":"projected","op":"rolling_mean","input":"r","period":2},
                {"id":"unneeded_oi","op":"returns_series","input":"open_interest"},
                {"id":"unneeded_funding","op":"rolling_mean","input":"funding_rate","period":2},
                {"id":"unneeded_pair","op":"subtract_series","input":"unneeded_oi","other":"unneeded_funding"}
            ],
            "result_steps":["projected"]
        })).unwrap()));
        let plan = planner::AnalysisPlan::compile_market(&request).unwrap();
        assert_eq!(
            plan.sources,
            vec![okx_analysis::query_plan::Source::Candles]
        );
        assert!(
            !plan
                .composition
                .as_ref()
                .unwrap()
                .sources()
                .contains(&okx_analysis::query_plan::Source::OpenInterest)
        );
        assert!(validate_composition_source_products(&request.instruments, false, false).is_ok());
        // No OI/funding values exist in this spot fixture. An unreachable
        // branch must not downgrade the projected result or invent evidence.
        let result = compute_row_planned(row(), &request, None, &plan, true).unwrap();
        assert!(result["metrics"]["projected"].is_number());
        assert_eq!(
            result["composition_quality"]["projected"]["status"],
            "available_at_closed_bar"
        );
        assert!(result["source_captures"].get("open_interest").is_none());
        assert!(result["source_captures"].get("funding_rate").is_none());
        assert_eq!(result["quality"], "complete");
    }
    #[test]
    fn t05_composition_reuses_closed_candles_and_projects_a_distinct_ratio() {
        let mut request = request(vec!["returns"]);
        request.start_ms = 0;
        request.end_ms = 180_000;
        request.composition = Some(Box::new(okx_analysis::query_plan::CompositionRequest {
            version: okx_analysis::query_plan::COMPOSITION_VERSION.into(),
            steps: vec![
                okx_analysis::query_plan::SeriesStep {
                    id: "r1".into(),
                    op: okx_analysis::query_plan::SeriesOp::ReturnsSeries,
                    input: "close".into(),
                    period: None,
                    threshold: None,
                    when: None,
                    other: None,
                },
                okx_analysis::query_plan::SeriesStep {
                    id: "w1".into(),
                    op: okx_analysis::query_plan::SeriesOp::RollingMean,
                    input: "r1".into(),
                    period: Some(2),
                    threshold: None,
                    when: None,
                    other: None,
                },
            ],
            result_steps: vec!["w1".into()],
        }));
        let mut fixture = row();
        fixture.bars = Ok(Arc::new(vec![
            Bar {
                time: 0,
                open: 100.0,
                high: 100.0,
                low: 100.0,
                close: 100.0,
                volume: 1.0,
                funding_rate: None,
            },
            Bar {
                time: 60_000,
                open: 110.0,
                high: 110.0,
                low: 110.0,
                close: 110.0,
                volume: 1.0,
                funding_rate: None,
            },
            Bar {
                time: 120_000,
                open: 99.0,
                high: 99.0,
                low: 99.0,
                close: 99.0,
                volume: 1.0,
                funding_rate: None,
            },
        ]));
        let checked = planner::AnalysisPlan::compile_market(&request).unwrap();
        let composition = checked.composition.as_ref();
        let mut partial_fixture = row();
        partial_fixture.bars = fixture.bars.clone();
        let output = compute_row_checked(fixture, &request, None, composition, true).unwrap();
        assert!((output["metrics"]["returns"].as_f64().unwrap() + 0.01).abs() < 1e-12);
        assert!(output["metrics"]["w1"].as_f64().unwrap().abs() < 1e-12);
        assert_eq!(output["units"]["w1"], "ratio");
        Arc::make_mut(partial_fixture.bars.as_mut().unwrap()).remove(1);
        let partial =
            compute_row_checked(partial_fixture, &request, None, composition, true).unwrap();
        assert_eq!(partial["metrics"]["w1"], Value::Null);
        assert_eq!(partial["quality"], "partial");
    }
    #[test]
    fn t15_public_window_extrema_share_candle_source_and_preserve_units() {
        let mut request = request(vec!["returns"]);
        request.start_ms = 0;
        request.end_ms = 180_000;
        request.composition = Some(Box::new(
            serde_json::from_value(json!({
                "version":"okx.market.composition/v1",
                "steps":[
                    {"id":"upper","op":"rolling_max","input":"close","period":3},
                    {"id":"lower","op":"rolling_min","input":"close","period":3}
                ],
                "result_steps":["upper","lower"]
            }))
            .unwrap(),
        ));
        let mut fixture = row();
        fixture.bars = Ok(Arc::new(
            [100.0, 110.0, 99.0]
                .into_iter()
                .enumerate()
                .map(|(i, price)| Bar {
                    time: i as u64 * 60_000,
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                    volume: 1.0,
                    funding_rate: None,
                })
                .collect(),
        ));
        let checked = planner::AnalysisPlan::compile_market(&request).unwrap();
        assert_eq!(
            checked.sources,
            vec![okx_analysis::query_plan::Source::Candles]
        );
        let mut partial_fixture = row();
        partial_fixture.bars = fixture.bars.clone();
        let out = compute_row_checked(fixture, &request, None, checked.composition.as_ref(), true)
            .unwrap();
        assert_eq!(out["metrics"]["upper"], 110.0);
        assert_eq!(out["metrics"]["lower"], 99.0);
        assert_eq!(out["units"]["upper"], "price");
        assert_eq!(out["units"]["lower"], "price");
        Arc::make_mut(partial_fixture.bars.as_mut().unwrap()).remove(1);
        let partial = compute_row_checked(
            partial_fixture,
            &request,
            None,
            checked.composition.as_ref(),
            true,
        )
        .unwrap();
        assert_eq!(partial["quality"], "partial");
        assert!(partial["metrics"]["upper"].is_null());
        assert!(partial["metrics"]["lower"].is_null());
    }
    #[test]
    fn t07_event_graph_exposes_boolean_metrics_but_not_unconfirmed_future_outcomes() {
        let mut query = request(vec!["returns"]);
        query.start_ms = 0;
        query.end_ms = 300_000;
        query.composition = Some(Box::new(serde_json::from_value(json!({
            "version":"okx.market.composition/v1",
            "steps":[
                {"id":"r1","op":"returns_series","input":"close"},
                {"id":"above","op":"greater_than","input":"r1","threshold":0.0},
                {"id":"entry","op":"rising_edge","input":"above"},
                {"id":"previous","op":"lag","input":"close","period":1},
                {"id":"label","op":"forward_return_label","input":"close","period":2,"when":"entry"}
            ],
            "result_steps":["above","entry","previous","label"]
        })).unwrap()));
        let mut fixture = row();
        fixture.bars = Ok(Arc::new(
            [100.0, 90.0, 95.0, 99.0, 120.0]
                .iter()
                .enumerate()
                .map(|(i, &close)| Bar {
                    time: i as u64 * 60_000,
                    open: close,
                    high: close,
                    low: close,
                    close,
                    volume: 1.0,
                    funding_rate: None,
                })
                .collect(),
        ));
        let checked = planner::AnalysisPlan::compile_market(&query).unwrap();
        let result =
            compute_row_checked(fixture, &query, None, checked.composition.as_ref(), true).unwrap();
        assert_eq!(result["metrics"]["above"], true);
        assert_eq!(result["units"]["above"], "boolean");
        assert_eq!(result["metrics"]["entry"], false);
        assert_eq!(result["units"]["entry"], "event");
        assert_eq!(result["metrics"]["previous"], 99.0);
        assert_eq!(result["metrics"]["label"], Value::Null);
        assert_eq!(result["units"]["label"], "outcome_ratio");
        assert!(
            result["gaps"]
                .as_array()
                .unwrap()
                .iter()
                .any(|gap| gap.as_str() == Some("OUTCOME_UNCONFIRMED:label"))
        );
        assert_eq!(result["quality"], "partial");
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
                let ticker: okx_protocol::research_ipc::ResearchTicker = serde_json::from_value(
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
    #[test]
    fn t06_t07_t13_oi_provenance_is_stable_and_does_not_certify_a_signal() {
        let mut query: MarketRequest = serde_json::from_value(json!({
            "action":"screen",
            "instruments":["BTC-USDT-SWAP"],
            "fields":["returns"],
            "timeframe_ms":60000,
            "start_ms":0,
            "end_ms":6000000
        }))
        .unwrap();
        query.composition = Some(Box::new(
            serde_json::from_value(json!({
                "version":"okx.market.composition/v1",
                "steps":[
                    {"id":"oi_return","op":"returns_series","input":"open_interest"},
                    {"id":"positive","op":"greater_than","input":"oi_return","threshold":0.0},
                    {"id":"event","op":"rising_edge","input":"positive"}
                ],
                "result_steps":["oi_return","event"]
            }))
            .unwrap(),
        ));
        let checked = planner::AnalysisPlan::compile_market(&query).unwrap();
        let points = (0..100)
            .map(|i| (i * 60_000, 10.0 + i as f64))
            .collect::<Vec<_>>();
        let fixture = || Loaded {
            instrument: "BTC-USDT-SWAP".into(),
            bars: Ok(Arc::new(
                (0..100)
                    .map(|i| Bar {
                        time: i * 60_000,
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
            oi: Some(Ok(points.clone())),
            excluded: None,
        };
        let first =
            compute_row_checked(fixture(), &query, None, checked.composition.as_ref(), true)
                .unwrap();
        let repeat =
            compute_row_checked(fixture(), &query, None, checked.composition.as_ref(), true)
                .unwrap();
        assert_eq!(first["source_captures"], repeat["source_captures"]);
        assert!(first["metrics"]["oi_return"].is_number());
        assert!(first["metrics"]["event"].is_null());
        assert_eq!(
            first["composition_quality"]["oi_return"]["status"],
            "retrospective_publication_unverified"
        );
        assert_eq!(
            first["composition_quality"]["event"]["issues"][0]["reason"],
            "CAUSAL_PUBLICATION_TIME_UNVERIFIED"
        );
        assert_eq!(
            first["metric_time_basis"]["event"],
            "oi_event_time_publication_unverified"
        );
        assert!(
            first["gaps"]
                .as_array()
                .unwrap()
                .contains(&json!("CAUSAL_AVAILABILITY_UNVERIFIED:event"))
        );
        let mut changed = fixture();
        changed
            .oi
            .as_mut()
            .unwrap()
            .as_mut()
            .unwrap()
            .last_mut()
            .unwrap()
            .1 += 1.0;
        let after =
            compute_row_checked(changed, &query, None, checked.composition.as_ref(), true).unwrap();
        assert_ne!(
            first["source_captures"]["open_interest"]["capture_hash"],
            after["source_captures"]["open_interest"]["capture_hash"]
        );
        let mut gap = fixture();
        gap.oi.as_mut().unwrap().as_mut().unwrap().remove(42);
        let partial =
            compute_row_checked(gap, &query, None, checked.composition.as_ref(), true).unwrap();
        assert!(partial["metrics"]["oi_return"].is_null());
        assert_eq!(
            partial["composition_quality"]["oi_return"]["issues"][0]["first_missing_ms"],
            42 * 60_000
        );
    }

    #[test]
    fn t02_t06_funding_events_join_price_returns_with_provenance_not_causal_signals() {
        let query: MarketRequest = serde_json::from_value(json!({
            "action":"screen","instruments":["BTC-USDT-SWAP"],"fields":["returns"],
            "timeframe_ms":60000,"start_ms":0,"end_ms":6000000,
            "composition":{"version":"okx.market.composition/v1","steps":[
                {"id":"price_return","op":"returns_series","input":"close"},
                {"id":"net_return","op":"subtract_series","input":"price_return","other":"funding_rate"},
                {"id":"positive","op":"greater_than","input":"net_return","threshold":0.0}
            ],"result_steps":["net_return","positive"]}
        })).unwrap();
        let plan = planner::AnalysisPlan::compile_market(&query).unwrap();
        assert!(
            plan.sources
                .contains(&okx_analysis::query_plan::Source::Funding)
        );
        let fixture = || Loaded {
            instrument: "BTC-USDT-SWAP".into(),
            bars: Ok(Arc::new(
                (0..100)
                    .map(|i| Bar {
                        time: i * 60_000,
                        open: 100.0,
                        high: 200.0,
                        low: 99.0,
                        close: 100.0 + i as f64,
                        volume: 1.0,
                        funding_rate: None,
                    })
                    .collect(),
            )),
            ticker: None,
            funding: Some(Ok(vec![
                (60_000, 0.001),
                (60_001, 0.006),
                (99 * 60_000, -0.002),
            ])),
            oi: None,
            excluded: None,
        };
        let first = compute_row_planned(fixture(), &query, None, &plan, true).unwrap();
        let repeated = compute_row_planned(fixture(), &query, None, &plan, true).unwrap();
        assert_eq!(first["source_captures"], repeated["source_captures"]);
        assert!(
            (first["metrics"]["net_return"].as_f64().unwrap() - (199.0 / 198.0 - 1.0 + 0.002))
                .abs()
                < 1e-12
        );
        assert!(first["metrics"]["positive"].is_null());
        assert_eq!(
            first["composition_quality"]["net_return"]["status"],
            "retrospective_publication_unverified"
        );
        assert_eq!(
            first["composition_quality"]["positive"]["issues"][0]["reason"],
            "CAUSAL_PUBLICATION_TIME_UNVERIFIED"
        );
        assert_eq!(
            first["metric_time_basis"]["net_return"],
            "funding_event_time_publication_unverified"
        );
        assert_eq!(first["funding_event_alignment"]["observed_events"], 3);
        assert_eq!(first["funding_event_alignment"]["aligned_events"], 2);
        assert_eq!(first["funding_event_alignment"]["off_grid_events"], 1);
        assert_eq!(first["funding_event_alignment"]["first_off_grid_ms"], 60_001);
        assert_eq!(first["funding_event_alignment"]["first_empty_grid_ms"], 0);
        assert_eq!(
            first["funding_event_alignment"]["expected_events"],
            Value::Null
        );
        assert_eq!(
            first["funding_event_alignment"]["publication_time_verified"],
            false
        );
        assert_eq!(
            first["source_captures"]["funding_rate"]["observed_events"],
            3
        );
        assert_eq!(first["source_captures"]["candles"]["observed_points"], 100);
        assert!(
            first["gaps"]
                .as_array()
                .unwrap()
                .contains(&json!("CAUSAL_AVAILABILITY_UNVERIFIED:positive"))
        );

        let mut changed = fixture();
        changed
            .funding
            .as_mut()
            .unwrap()
            .as_mut()
            .unwrap()
            .last_mut()
            .unwrap()
            .1 = -0.003;
        let after = compute_row_planned(changed, &query, None, &plan, true).unwrap();
        assert_ne!(
            first["source_captures"]["funding_rate"]["capture_hash"],
            after["source_captures"]["funding_rate"]["capture_hash"]
        );
        let mut absent = fixture();
        absent.funding.as_mut().unwrap().as_mut().unwrap().pop();
        let missing = compute_row_planned(absent, &query, None, &plan, true).unwrap();
        assert!(missing["metrics"]["net_return"].is_null());
        assert_eq!(
            missing["composition_quality"]["net_return"]["issues"][0],
            json!({"reason":"SOURCE_OBSERVATION_MISSING","origin_step":"funding_rate", "origin_event_ms":99 * 60_000})
        );
        assert_eq!(
            missing["funding_event_alignment"]["last_bar_has_event"],
            false
        );
        let mut unavailable = fixture();
        unavailable.funding = Some(Err("FUNDING_RANGE_UNAVAILABLE".into()));
        let failed = compute_row_planned(unavailable, &query, None, &plan, true).unwrap();
        assert_eq!(
            failed["composition_quality"]["net_return"]["issues"][0]["reason"],
            "FUNDING_SOURCE_UNAVAILABLE"
        );
        assert!(failed["source_captures"]["funding_rate"].is_null());
    }

    #[test]
    fn t06_shifted_oi_event_cannot_fill_a_candle_gap_or_certify_a_signal() {
        let mut request: MarketRequest = serde_json::from_value(json!({
            "action":"screen",
            "instruments":["BTC-USDT-SWAP"],
            "fields":["returns"],
            "timeframe_ms":60000,
            "start_ms":0,
            "end_ms":6000000
        }))
        .unwrap();
        request.composition = Some(Box::new(serde_json::from_value(json!({
            "version":okx_analysis::query_plan::COMPOSITION_VERSION,
            "steps":[
                {"id":"oi_change","op":"returns_series","input":"open_interest"},
                {"id":"positive","op":"greater_than","input":"oi_change","threshold":0.0}
            ],
            "result_steps":["oi_change","positive"]
        })).unwrap()));
        let plan = planner::AnalysisPlan::compile_market(&request).unwrap();
        let mut loaded = Loaded {
            instrument: "BTC-USDT-SWAP".into(),
            bars: Ok(Arc::new(
                (0..100)
                    .map(|index| Bar {
                        time: index * 60_000,
                        open: 100.0,
                        high: 200.0,
                        low: 99.0,
                        close: 100.0 + index as f64,
                        volume: 1.0,
                        funding_rate: None,
                    })
                    .collect(),
            )),
            ticker: None,
            funding: None,
            oi: None,
            excluded: None,
        };
        let mut oi = (0..100).map(|index| (index * 60_000, 20.0 + index as f64))
            .collect::<Vec<_>>();
        // A one-millisecond shift is not the closed-bar event at 42 * 60,000.
        // A nearest/as-of join would fabricate a usable point here.
        oi[42].0 += 1;
        loaded.oi = Some(Ok(oi));
        let result = compute_row_planned(loaded, &request, None, &plan, true).unwrap();
        assert_eq!(result["source_alignment"]["observed_oi_observations"], 99);
        assert_eq!(result["source_alignment"]["aligned_observations"], 99);
        assert_eq!(result["source_alignment"]["oi_off_grid_events"], 1);
        assert_eq!(result["source_alignment"]["oi_first_off_grid_ms"], 42 * 60_000 + 1);
        assert_eq!(result["source_alignment"]["first_unaligned_ms"], 42 * 60_000);
        assert_eq!(result["oi_coverage"]["first_missing_ms"], 42 * 60_000);
        assert!(result["metrics"]["oi_change"].is_null());
        assert!(result["metrics"]["positive"].is_null());
        assert_eq!(result["composition_quality"]["oi_change"]["issues"][0]["reason"],
            "OI_RANGE_INCOMPLETE_OR_GAPPED");
        assert_eq!(result["composition_quality"]["positive"]["issues"][1]["reason"],
            "CAUSAL_PUBLICATION_TIME_UNVERIFIED");
        assert_eq!(result["source_captures"]["open_interest"]["publication_time_verified"],
            false);
    }

    #[test]
    fn t06_funding_only_graph_does_not_require_or_invent_candles() {
        let query: MarketRequest = serde_json::from_value(json!({
            "action":"screen","instruments":["ETH-USDT-SWAP"],"fields":["funding_sum"],
            "timeframe_ms":60000,"start_ms":0,"end_ms":6000000,
            "composition":{"version":"okx.market.composition/v1","steps":[
                {"id":"rate","op":"rolling_mean","input":"funding_rate","period":1}
            ],"result_steps":["rate"]}
        }))
        .unwrap();
        let plan = planner::AnalysisPlan::compile_market(&query).unwrap();
        assert!(
            !plan
                .sources
                .contains(&okx_analysis::query_plan::Source::Candles)
        );
        let loaded = Loaded {
            instrument: "ETH-USDT-SWAP".into(),
            bars: Ok(Arc::new(vec![])),
            ticker: None,
            funding: Some(Ok(vec![(99 * 60_000, -0.001)])),
            oi: None,
            excluded: None,
        };
        let result = compute_row_planned(loaded, &query, None, &plan, false).unwrap();
        assert_eq!(result["metrics"]["funding_sum"], json!(-0.001));
        assert_eq!(result["metrics"]["rate"], json!(-0.001));
        assert_eq!(result["funding_event_alignment"]["aligned_events"], 1);
        assert_eq!(result["source_captures"]["candles"], Value::Null);
        assert_eq!(result["candle_coverage"], Value::Null);
        assert_eq!(
            result["composition_quality"]["rate"]["status"],
            "retrospective_publication_unverified"
        );
    }
    #[test]
    fn t15_public_zscore_composes_price_deviation_into_a_causal_condition() {
        let query: MarketRequest = serde_json::from_value(json!({
            "action":"screen","instruments":["BTC-USDT"],"fields":["returns"],
            "timeframe_ms":60000,"start_ms":0,"end_ms":6000000,
            "composition":{"version":"okx.market.composition/v1","steps":[
                {"id":"deviation","op":"rolling_zscore","input":"close","period":3},
                {"id":"above","op":"greater_than","input":"deviation","threshold":1.0}
            ],"result_steps":["deviation","above"]}
        }))
        .unwrap();
        let plan = planner::AnalysisPlan::compile_market(&query).unwrap();
        let fixture = |flat: bool| Loaded {
            instrument: "BTC-USDT".into(),
            bars: Ok(Arc::new(
                (0..100)
                    .map(|i| Bar {
                        time: i * 60_000,
                        open: 100.0,
                        high: 201.0,
                        low: 99.0,
                        close: if flat { 100.0 } else { 100.0 + i as f64 },
                        volume: 1.0,
                        funding_rate: None,
                    })
                    .collect(),
            )),
            ticker: None,
            funding: None,
            oi: None,
            excluded: None,
        };
        let output = compute_row_planned(fixture(false), &query, None, &plan, true).unwrap();
        assert!(
            (output["metrics"]["deviation"].as_f64().unwrap() - (1.0 / (2.0f64 / 3.0).sqrt()))
                .abs()
                < 1e-10
        );
        assert_eq!(output["metrics"]["above"], true);
        assert_eq!(output["units"]["deviation"], "coefficient");
        assert_eq!(
            output["composition_quality"]["above"]["status"],
            "available_at_closed_bar"
        );
        assert_eq!(output["quality"], "complete");
        let flat = compute_row_planned(fixture(true), &query, None, &plan, true).unwrap();
        assert!(flat["metrics"]["deviation"].is_null());
        assert_eq!(
            flat["composition_quality"]["deviation"]["issues"][0]["reason"],
            "ZERO_VARIANCE"
        );
        assert!(flat["metrics"]["above"].is_null());
        assert_eq!(
            flat["composition_quality"]["above"]["issues"][0]["origin_step"],
            "deviation"
        );
    }
    #[test]
    fn t02_t06_funding_and_open_interest_keep_independent_source_evidence() {
        let query: MarketRequest = serde_json::from_value(json!({
            "action":"screen","instruments":["BTC-USDT-SWAP"],"fields":["funding_sum"],
            "timeframe_ms":60000,"start_ms":0,"end_ms":6000000,
            "composition":{"version":"okx.market.composition/v1","steps":[
                {"id":"oi_return","op":"returns_series","input":"open_interest"},
                {"id":"joint","op":"add_series","input":"funding_rate","other":"oi_return"}
            ],"result_steps":["joint"]}
        }))
        .unwrap();
        let plan = planner::AnalysisPlan::compile_market(&query).unwrap();
        assert!(
            !plan
                .sources
                .contains(&okx_analysis::query_plan::Source::Candles)
        );
        let loaded = Loaded {
            instrument: "BTC-USDT-SWAP".into(),
            bars: Ok(Arc::new(vec![])),
            ticker: None,
            funding: Some(Ok(vec![(99 * 60_000, 0.001)])),
            oi: Some(Ok((0..100)
                .map(|i| (i * 60_000, 100.0 + i as f64))
                .collect())),
            excluded: None,
        };
        let output = compute_row_planned(loaded, &query, None, &plan, false).unwrap();
        assert!(
            (output["metrics"]["joint"].as_f64().unwrap() - (0.001 + 1.0 / 198.0)).abs() < 1e-12
        );
        assert!(output["source_captures"]["open_interest"]["capture_hash"].is_string());
        assert!(output["source_captures"]["funding_rate"]["capture_hash"].is_string());
        assert_eq!(output["source_alignment"]["observed_oi_observations"], 100);
        assert_eq!(output["funding_event_alignment"]["aligned_events"], 1);
        assert_eq!(
            output["metric_time_basis"]["joint"],
            "funding_event_time_publication_unverified"
        );
        assert_eq!(
            output["composition_quality"]["joint"]["status"],
            "retrospective_publication_unverified"
        );
    }
    #[test]
    fn t02_t05_t06_public_ratio_divergence_uses_exact_oi_grid_and_null_denominator() {
        let mut query: MarketRequest = serde_json::from_value(json!({
            "action":"screen","instruments":["BTC-USDT-SWAP"],"fields":["returns"],
            "timeframe_ms":60000,"start_ms":0,"end_ms":6000000
        }))
        .unwrap();
        query.composition = Some(Box::new(
            serde_json::from_value(json!({
                "version":"okx.market.composition/v1",
                "steps":[
                    {"id":"price_return","op":"returns_series","input":"close"},
                    {"id":"oi_return","op":"returns_series","input":"open_interest"},
                    {"id":"difference","op":"subtract_series","input":"price_return","other":"oi_return"},
                    {"id":"relative","op":"divide_series","input":"price_return","other":"oi_return"},
                    {"id":"delayed_relative","op":"lag","input":"relative","period":1}
                ],"result_steps":["difference","relative","delayed_relative"]
            }))
            .unwrap(),
        ));
        let plan = planner::AnalysisPlan::compile_market(&query).unwrap();
        let fixture = |flat_oi: bool| Loaded {
            instrument: "BTC-USDT-SWAP".into(),
            bars: Ok(Arc::new(
                (0..100)
                    .map(|i| Bar {
                        time: i * 60_000,
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
            oi: Some(Ok((0..100)
                .map(|i| (i * 60_000, if flat_oi { 10.0 } else { 10.0 + i as f64 }))
                .collect())),
            excluded: None,
        };
        let output = compute_row_planned(fixture(false), &query, None, &plan, true).unwrap();
        let price_return = 199.0 / 198.0 - 1.0;
        let oi_return = 109.0 / 108.0 - 1.0;
        assert!(
            (output["metrics"]["difference"].as_f64().unwrap() - (price_return - oi_return)).abs()
                < 1e-12
        );
        assert!(
            (output["metrics"]["relative"].as_f64().unwrap() - price_return / oi_return).abs()
                < 1e-12
        );
        assert_eq!(output["units"]["difference"], "ratio");
        assert_eq!(output["source_alignment"]["expected_observations"], 100);
        assert_eq!(output["source_alignment"]["aligned_observations"], 100);
        assert_eq!(output["source_alignment"]["paired_return_observations"], 99);
        assert_eq!(
            output["source_alignment"]["first_unaligned_ms"],
            Value::Null
        );
        assert_eq!(
            output["source_alignment"]["oi_publication_time_verified"],
            false
        );
        assert_eq!(
            output["composition_quality"]["relative"]["status"],
            "retrospective_publication_unverified"
        );
        let zero = compute_row_planned(fixture(true), &query, None, &plan, true).unwrap();
        assert!(zero["metrics"]["relative"].is_null());
        assert!(zero["metrics"]["difference"].is_number());
        assert_eq!(
            zero["composition_quality"]["relative"]["status"],
            "unavailable"
        );
        assert_eq!(
            zero["composition_quality"]["relative"]["issues"][0],
            json!({"reason":"DIVISION_BY_ZERO","origin_step":"relative", "origin_event_ms":99 * 60_000})
        );
        assert_eq!(
            zero["composition_quality"]["delayed_relative"]["issues"][0],
            json!({"reason":"DIVISION_BY_ZERO","origin_step":"relative", "origin_event_ms":98 * 60_000})
        );
        assert!(zero["metrics"]["delayed_relative"].is_null());
        let mut interrupted = fixture(false);
        interrupted
            .oi
            .as_mut()
            .unwrap()
            .as_mut()
            .unwrap()
            .remove(50);
        let partial = compute_row_planned(interrupted, &query, None, &plan, true).unwrap();
        assert!(partial["metrics"]["difference"].is_null());
        assert_eq!(partial["source_alignment"]["aligned_observations"], 99);
        assert_eq!(
            partial["source_alignment"]["paired_return_observations"],
            97
        );
        assert_eq!(
            partial["source_alignment"]["first_unaligned_ms"],
            50 * 60_000
        );
        assert_eq!(
            zero["source_captures"]["open_interest"]["publication_time_verified"],
            false
        );
    }

    #[test]
    fn t06_public_rolling_pearson_uses_thirty_exact_return_pairs_without_oi_lookahead() {
        let mut query: MarketRequest = serde_json::from_value(json!({
            "action":"screen","instruments":["BTC-USDT-SWAP"],"fields":["returns"],
            "timeframe_ms":60000,"start_ms":0,"end_ms":6000000,
            "composition":{"version":"okx.market.composition/v1","steps":[
                {"id":"returns","op":"returns_series","input":"close"},
                {"id":"oi_return","op":"returns_series","input":"open_interest"},
                {"id":"relationship","op":"rolling_pearson","input":"returns","other":"oi_return","period":30}
            ],"result_steps":["relationship"]}
        }))
        .unwrap();
        let plan = planner::AnalysisPlan::compile_market(&query).unwrap();
        let fixture = |flat_oi: bool| {
            let (mut price, mut oi) = (100.0, 10.0);
            let mut bars = Vec::new();
            let mut oi_points = Vec::new();
            for i in 0..100 {
                if i > 0 {
                    let change = (((i * 7) % 11) as f64 - 5.0) * 0.0005;
                    price *= 1.0 + change;
                    if !flat_oi {
                        oi *= 1.0 + 2.0 * change;
                    }
                }
                bars.push(Bar {
                    time: i * 60_000,
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                    volume: 1.0,
                    funding_rate: None,
                });
                oi_points.push((i * 60_000, oi));
            }
            Loaded {
                instrument: "BTC-USDT-SWAP".into(),
                bars: Ok(Arc::new(bars)),
                ticker: None,
                funding: None,
                oi: Some(Ok(oi_points)),
                excluded: None,
            }
        };
        let complete = compute_row_planned(fixture(false), &query, None, &plan, true).unwrap();
        assert!((complete["metrics"]["relationship"].as_f64().unwrap() - 1.0).abs() < 1e-8);
        assert_eq!(complete["units"]["relationship"], "coefficient");
        assert_eq!(
            complete["source_alignment"]["paired_return_observations"],
            99
        );
        assert_eq!(
            complete["composition_quality"]["relationship"]["status"],
            "retrospective_publication_unverified"
        );
        let no_variance = compute_row_planned(fixture(true), &query, None, &plan, true).unwrap();
        assert!(no_variance["metrics"]["relationship"].is_null());
        assert_eq!(
            no_variance["composition_quality"]["relationship"]["issues"][0]["reason"],
            "ZERO_VARIANCE"
        );
        let mut missing = fixture(false);
        missing.oi.as_mut().unwrap().as_mut().unwrap().remove(80);
        let partial = compute_row_planned(missing, &query, None, &plan, true).unwrap();
        assert!(partial["metrics"]["relationship"].is_null());
        assert_eq!(
            partial["source_alignment"]["paired_return_observations"],
            97
        );
        assert_eq!(
            partial["source_alignment"]["first_unaligned_ms"],
            80 * 60_000
        );
        assert_eq!(
            partial["composition_quality"]["relationship"]["issues"][0]["reason"],
            "OI_RANGE_INCOMPLETE_OR_GAPPED"
        );
        // The same graph cannot be used to create a historical causal signal
        // from OI publication times that have not been independently attested.
        query.composition.as_mut().unwrap().steps.push(
            serde_json::from_value(
                json!({"id":"signal","op":"greater_than","input":"relationship","threshold":0.9}),
            )
            .unwrap(),
        );
        query.composition.as_mut().unwrap().result_steps = vec!["signal".into()];
        let causal_plan = planner::AnalysisPlan::compile_market(&query).unwrap();
        let rejected =
            compute_row_planned(fixture(false), &query, None, &causal_plan, true).unwrap();
        assert!(rejected["metrics"]["signal"].is_null());
        assert_eq!(
            rejected["composition_quality"]["signal"]["issues"][0]["reason"],
            "CAUSAL_PUBLICATION_TIME_UNVERIFIED"
        );
    }
}

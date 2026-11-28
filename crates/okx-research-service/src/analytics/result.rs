//! Native Research result presentation: artifact integrity, export verification,
//! bounded paging, and signed cursors. No SQL mutation or research execution.
use super::{AnalyticsService, cursor, jobs, result_reexecution};
use okx_research::{
    ResearchArtifactStore,
    strategy_registry::{Bar, ChunkedDataHash},
};
use okx_research_protocol::analytics::ResearchResultView;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

pub(super) fn read_verified_result<T: DeserializeOwned>(
    store: &ResearchArtifactStore,
    artifact: &str,
) -> Result<T, String> {
    store
        .read_evidence_json(artifact)
        .map_err(|error| match error {
            okx_research::ResearchError::ArtifactIdentityMismatch
            | okx_research::ResearchError::InvalidArtifactId
            | okx_research::ResearchError::Serialization(_) => "ARTIFACT_CORRUPT".into(),
            okx_research::ResearchError::Io(ref source)
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                "ARTIFACT_CORRUPT".into()
            }
            other => other.to_string(),
        })
}
/// Traverse all immutable pages, checking SHA and exact count without
/// materializing the complete result. Row export uses the paged result view.
fn verify_export_pages(
    store: &ResearchArtifactStore,
    manifest: &Value,
    refs_key: &str,
    count_key: &str,
    capacity: usize,
) -> Result<Option<Value>, String> {
    let Some(ids) = manifest.get(refs_key) else {
        return Ok(None);
    };
    let pages = ids.as_array().ok_or("EXPORT_MANIFEST_INVALID")?;
    let declared = manifest
        .get(count_key)
        .and_then(Value::as_u64)
        .ok_or("EXPORT_MANIFEST_INVALID")?;
    if pages.len() as u64 > declared.div_ceil(capacity as u64) {
        return Err("EXPORT_ROW_COUNT_MISMATCH".into());
    }
    let mut count = 0u64;
    for (index, reference) in pages.iter().enumerate() {
        let id = reference.as_str().ok_or("EXPORT_MANIFEST_INVALID")?;
        let rows: Vec<Value> = read_verified_result(store, id)?;
        if rows.is_empty()
            || rows.len() > capacity
            || (index + 1 < pages.len() && rows.len() != capacity)
        {
            return Err("EXPORT_PAGE_SIZE_INVALID".into());
        }
        count = count
            .checked_add(rows.len() as u64)
            .ok_or("EXPORT_ROW_COUNT_MISMATCH")?;
        if count > declared {
            return Err("EXPORT_ROW_COUNT_MISMATCH".into());
        }
    }
    if count != declared {
        return Err("EXPORT_ROW_COUNT_MISMATCH".into());
    }
    Ok(Some(json!({
        "rows":count,
        "pages":pages.len(),
        "page_refs_sha256":okx_research::canonical_sha256(ids).map_err(|e| e.to_string())?
    })))
}

pub(super) fn verify_export_manifest(
    store: &ResearchArtifactStore,
    manifest: &Value,
) -> Result<Value, String> {
    let experiment = match manifest
        .get("experiment_artifact")
        .filter(|value| !value.is_null())
    {
        Some(reference) => {
            let id = reference.as_str().ok_or("EXPORT_MANIFEST_INVALID")?;
            let inputs: Value = read_verified_result(store, id)?;
            if inputs["schema"] != "okx.research.experiment-inputs/v1"
                || inputs["dataset_artifact"].as_str().is_none()
                || inputs["dataset_artifact"] != manifest["dataset_artifact"]
                || inputs["code_version"].as_str().is_none()
                || inputs["code_version"] != manifest["code_version"]
            {
                return Err("EXPERIMENT_PROVENANCE_MISMATCH".into());
            }
            let basis = &inputs["economic_basis"];
            let reference_id = basis["reference_capture_artifact"].as_str();
            if basis["product"] == "linear_swap" && reference_id.is_none() {
                return Err("EXPERIMENT_PROVENANCE_MISMATCH".into());
            }
            if let Some(reference_id) = reference_id {
                let captured: Value = read_verified_result(store, reference_id)?;
                if captured["schema"] != "okx.research.reference-capture/v1"
                    || captured["instrument"] != inputs["instrument"]
                    || captured["reference_generation"] != basis["reference_generation"]
                    || captured["acquired_at_ms"] != basis["reference_acquired_at_ms"]
                    || captured["contract_value_currency"] != basis["source_ct_val_ccy"]
                    || captured["contract_type"] != basis["source_ct_type"]
                    || captured["settle_currency"] != basis["source_settle_currency"]
                    || captured["contract_value"] != basis["source_ct_val"]
                {
                    return Err("EXPERIMENT_PROVENANCE_MISMATCH".into());
                }
                let raw: Vec<u8> = serde_json::from_value(captured["raw_body"].clone())
                    .map_err(|_| "EXPERIMENT_PROVENANCE_MISMATCH")?;
                let recomputed =
                    okx_research::canonical_sha256(&("okx.public.reference.raw/v1", &raw))
                        .map_err(|error| error.to_string())?;
                if basis["reference_capture_hash"] != recomputed {
                    return Err("EXPERIMENT_PROVENANCE_MISMATCH".into());
                }
            }
            // Rebuild the event-to-bar projection from the captured source,
            // then compare it to every replay input bar as it is streamed and
            // hash-verified. A valid capture alone is not proof of attribution.
            let (funding_capture, expected_funding) = verify_experiment_funding(store, &inputs)?;
            let dataset =
                verify_experiment_dataset(store, &inputs, manifest, expected_funding.as_deref())?;
            Some(
                json!({"artifact":id,"checksum_verified":true,"references_match":true,
                "reference_capture_verified":reference_id.is_some(),
                "funding_replay_attribution_verified":expected_funding.is_some(),
                "dataset":dataset,"funding_capture":funding_capture}),
            )
        }
        None => None,
    };
    let trades = verify_export_pages(store, manifest, "trade_pages", "total_trades", 50)?;
    let rows = verify_export_pages(store, manifest, "row_pages", "total_rows", 50)?;
    let equity = verify_export_pages(
        store,
        manifest,
        "equity_curve_pages",
        "total_equity_points",
        4096,
    )?;
    if trades.is_none() && rows.is_none() {
        return Err("EXPORT_MANIFEST_INVALID".into());
    }
    let dataset_verified = experiment
        .as_ref()
        .is_some_and(|e| e["dataset"]["quality"] == "verified");
    let funding_verified = experiment.as_ref().is_some_and(|e| {
        matches!(
            e["funding_capture"]["quality"].as_str(),
            Some("verified" | "not_applicable")
        )
    });
    let reexecution = if let Some(id) = manifest["experiment_artifact"].as_str() {
        let inputs: Value = read_verified_result(store, id)?;
        result_reexecution::verify(store, &inputs, manifest)?
    } else {
        json!({"schema":"okx.research.immutable-reexecution/v1", "quality":"not_verified",
            "reason_code":"LEGACY_EXPERIMENT_INPUTS_NOT_RETAINED"})
    };
    Ok(json!({
        "quality":if dataset_verified && funding_verified {"complete"} else {"partial"},
        "evidence_scope":if dataset_verified && funding_verified {"result_pages_and_replay_sources"} else {"result_pages_or_partial_sources"},
        "counts_verified":true,
        "checksums_verified":true,"dataset_verified":dataset_verified,
        "funding_capture_verified":funding_verified,
        "trades":trades,"rows":rows,"equity":equity,"experiment":experiment,
        "reexecution":reexecution
    }))
}

/// A replay's historical funding is a retrospective settlement input, not a
/// publication-time-certified signal. Verify its normalized capture against
/// the experiment while keeping availability and mark-price caveats explicit.
type FundingVerification = Result<(Value, Option<Vec<Option<f64>>>), String>;

fn verify_experiment_funding(store: &ResearchArtifactStore, inputs: &Value) -> FundingVerification {
    let basis = &inputs["economic_basis"];
    if !basis["funding_event_alignment"].is_object() {
        return Ok((json!({"quality":"not_applicable"}), None));
    }
    let alignment = &basis["funding_event_alignment"];
    let attribution_v1 =
        alignment["allocation_schema"] == "okx.research.funding-bar-observation/v1";
    if !alignment["allocation_schema"].is_null() && !attribution_v1 {
        return Err("EXPERIMENT_FUNDING_MISMATCH".into());
    }
    let Some(id) = basis["funding_capture_artifact"].as_str() else {
        if attribution_v1 {
            return Err("EXPERIMENT_FUNDING_MISMATCH".into());
        }
        return Ok((
            json!({"quality":"unverified_legacy",
            "reason_code":"FUNDING_CAPTURE_NOT_RETAINED"}),
            None,
        ));
    };
    let captured: Value = read_verified_result(store, id)?;
    let reason = "EXPERIMENT_FUNDING_MISMATCH";
    let end = inputs["effective_range"]["end_ms"]
        .as_u64()
        .and_then(|value| value.checked_add(1))
        .ok_or(reason)?;
    let start = inputs["requested_range"]["start_ms"]
        .as_u64()
        .ok_or(reason)?;
    if captured["schema"] != "okx.research.funding-event-capture/v1"
        || captured["instrument"] != inputs["instrument"]
        || captured["source"] != "okx/funding-rate-history"
        || captured["rate_basis"] != "realized_settlement_rate"
        || captured["start_ms"] != start
        || captured["end_exclusive_ms"] != end
    {
        return Err(reason.into());
    }
    let events: Vec<(u64, f64)> =
        serde_json::from_value(captured["events"].clone()).map_err(|_| reason)?;
    let mut previous = None;
    for &(time, rate) in &events {
        if time < start
            || time >= end
            || !rate.is_finite()
            || previous.is_some_and(|prior| prior >= time)
        {
            return Err(reason.into());
        }
        previous = Some(time);
    }
    let hash = okx_research::canonical_sha256(&(
        "okx.research.realized-funding-events/v1",
        inputs["instrument"].as_str().ok_or(reason)?,
        start,
        end,
        &events,
    ))
    .map_err(|error| error.to_string())?;
    if captured["normalized_capture_hash"] != hash
        || alignment["normalized_source_capture_hash"] != hash
        || alignment["observed_events"] != events.len()
    {
        return Err(reason.into());
    }
    if !attribution_v1 {
        // Older immutable experiments can prove event capture only, never
        // claim to have passed the newer bar-by-bar attribution check.
        return Ok((
            json!({"artifact":id,"quality":"verified",
            "observed_events":events.len(),"normalized_capture_hash":hash,
            "replay_attribution_verified":false,
            "publication_time_verified":false,"mark_price_verified":false}),
            None,
        ));
    }
    let bar_count = inputs["bar_count"].as_u64().ok_or(reason)?;
    let bar_count = usize::try_from(bar_count).map_err(|_| reason)?;
    let timeframe = inputs["timeframe_ms"].as_u64().ok_or(reason)?;
    let first_bar = inputs["effective_range"]["start_ms"]
        .as_u64()
        .ok_or(reason)?;
    let last_close = inputs["effective_range"]["end_ms"].as_u64().ok_or(reason)?;
    if bar_count == 0
        || bar_count > okx_research::economics::MAX_FUNDING_ALLOCATION_BARS
        || timeframe == 0
        || first_bar.checked_add((bar_count as u64).checked_mul(timeframe).ok_or(reason)?)
            != Some(last_close)
        || alignment["join_semantics"]
            != "funding_event_at_preceding_closed_bar_end_no_forward_fill"
        || alignment["source"] != "okx/funding-rate-history"
        || alignment["rate_basis"] != "realized_settlement_rate"
        || alignment["valuation_price"] != "candle_close_proxy_not_venue_mark_price"
        || alignment["absence_semantics"] != "no_observed_settlement_event_not_observed_zero_rate"
        || alignment["arithmetic_projection"]
            != "unobserved_settlement_contributes_zero_cashflow_not_zero_rate_evidence"
        || !alignment["expected_events"].is_null()
        || alignment["publication_time_verified"] != false
        || alignment["mark_price_verified"] != false
        || alignment["historical_cadence_verified"] != false
    {
        return Err(reason.into());
    }
    let bar_opens = (0..bar_count)
        .map(|index| first_bar + index as u64 * timeframe)
        .collect::<Vec<_>>();
    let allocation =
        okx_research::economics::allocate_funding_at_bar_close(&bar_opens, timeframe, &events)
            .map_err(|_| reason)?;
    if alignment["attributed_events"] != allocation.attributed_events
        || alignment["pre_window_events_ignored"] != allocation.pre_window_events
        || alignment["observed_zero_rate_events"] != allocation.observed_zero_rate_events
        || alignment["bars_without_settlement"] != allocation.bars_without_settlement
        || alignment["first_event_ms"] != json!(allocation.first_event_ms)
        || alignment["last_event_ms"] != json!(allocation.last_event_ms)
        || alignment["max_observed_event_gap_ms"] != json!(allocation.max_observed_gap_ms)
        || alignment["max_allowed_gap_ms"] != okx_research::economics::MAX_FUNDING_EVENT_GAP_MS
    {
        return Err(reason.into());
    }
    Ok((
        json!({"artifact":id,"quality":"verified",
        "observed_events":events.len(),"normalized_capture_hash":hash,
        "replay_attribution_verified":true,
        "attributed_events":allocation.attributed_events,
        "observed_zero_rate_events":allocation.observed_zero_rate_events,
        "bars_without_settlement":allocation.bars_without_settlement,
        "publication_time_verified":false,"mark_price_verified":false}),
        Some(allocation.observed_bar_rates),
    ))
}

/// Verify the exact input bars used by replay from the original immutable
/// pages. Never materialize the entire dataset; the same 4096-row hashing
/// implementation that computed ReplayResult.dataset_hash is used here.
/// Older experiment formats stay readable but never claim verified inputs.
fn verify_experiment_dataset(
    store: &ResearchArtifactStore,
    inputs: &Value,
    result: &Value,
    expected_funding: Option<&[Option<f64>]>,
) -> Result<Value, String> {
    let id = inputs["dataset_artifact"]
        .as_str()
        .ok_or("EXPERIMENT_PROVENANCE_MISMATCH")?;
    let dataset: Value = read_verified_result(store, id)?;
    if dataset["schema"] != "okx.research.dataset/398.1" {
        if expected_funding.is_some() {
            return Err("EXPERIMENT_FUNDING_BAR_MISMATCH".into());
        }
        return Ok(json!({"artifact":id,"quality":"unverified_legacy",
            "reason_code":"DATASET_SCHEMA_NOT_VERIFIABLE"}));
    }
    let invalid = || "EXPERIMENT_DATASET_MISMATCH".to_owned();
    let timeframe = dataset["timeframe_ms"].as_u64().ok_or_else(invalid)?;
    let start = dataset["start_ms"].as_u64().ok_or_else(invalid)?;
    let end = dataset["end_ms"].as_u64().ok_or_else(invalid)?;
    let expected = inputs["bar_count"].as_u64().ok_or_else(invalid)?;
    if timeframe == 0
        || end <= start
        || expected == 0
        || expected > 1_000_000
        || dataset["instrument"] != inputs["instrument"]
        || dataset["timeframe_ms"] != inputs["timeframe_ms"]
        || dataset["start_ms"] != inputs["requested_range"]["start_ms"]
        || dataset["end_ms"] != inputs["requested_range"]["end_ms"]
    {
        return Err(invalid());
    }
    let expected = usize::try_from(expected).map_err(|_| invalid())?;
    if expected_funding.is_some_and(|rates| rates.len() != expected) {
        return Err("EXPERIMENT_FUNDING_BAR_MISMATCH".into());
    }
    let pages = dataset["chunks"].as_array().ok_or_else(invalid)?;
    if pages.len() != expected.div_ceil(4096) {
        return Err("DATASET_PAGE_COUNT_MISMATCH".into());
    }
    let mut digest = ChunkedDataHash::new(expected);
    let mut previous = None;
    let mut first = None;
    let mut observed_bars = 0usize;
    for (index, reference) in pages.iter().enumerate() {
        let reference = reference.as_str().ok_or_else(invalid)?;
        let bars: Vec<Bar> = read_verified_result(store, reference)?;
        if bars.is_empty() || bars.len() > 4096 || (index + 1 < pages.len() && bars.len() != 4096) {
            return Err("DATASET_PAGE_SIZE_INVALID".into());
        }
        for bar in &bars {
            if bar.time < start
                || bar.time >= end
                || !bar.time.is_multiple_of(timeframe)
                || previous.is_some_and(|time: u64| time.checked_add(timeframe) != Some(bar.time))
            {
                return Err("DATASET_TIME_GRID_MISMATCH".into());
            }
            if let Some(projection) = expected_funding {
                // A missing event is an arithmetic zero, but the event's
                // presence is retained in `projection` for evidence.
                if bar.funding_rate
                    != projection
                        .get(observed_bars)
                        .map(|rate| Some(rate.unwrap_or(0.0)))
                        .unwrap_or(None)
                {
                    return Err("EXPERIMENT_FUNDING_BAR_MISMATCH".into());
                }
            }
            observed_bars += 1;
            first.get_or_insert(bar.time);
            previous = Some(bar.time);
        }
        digest.update(&bars)?;
    }
    let computed = digest.finish()?;
    if observed_bars != expected {
        return Err("DATASET_PAGE_SIZE_INVALID".into());
    }
    let last_closed = previous.and_then(|time| time.checked_add(timeframe));
    if inputs["effective_range"]["start_ms"].as_u64() != first
        || inputs["effective_range"]["end_ms"].as_u64() != last_closed
        || result["dataset_hash"].as_str() != Some(computed.as_str())
    {
        return Err("EXPERIMENT_DATASET_MISMATCH".into());
    }
    Ok(
        json!({"artifact":id,"quality":"verified","bar_count":expected,
        "pages":pages.len(),"first_bar_ms":first,"last_closed_ms":last_closed,
        "dataset_hash":computed,"hash_algorithm":"okx.typed.chunks/4096/v1",
        "funding_bar_projection_verified":expected_funding.is_some()}),
    )
}

impl AnalyticsService {
    pub(super) fn result_page(
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
        let batch = value.get("instruments").and_then(Value::as_array).is_some();
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
        // A batch is an index of independent instrument results, not a single
        // trades/equity/variants artifact. Read summary and select its explicit
        // instrument:<index>:0 cursor. Unselected export remains a truthful
        // aggregate/root diagnostic; signed cursors still validate first.
        if batch
            && matches!(
                view,
                ResearchResultView::Trades | ResearchResultView::Variants | ResearchResultView::Equity
            )
            && instrument_index.is_none()
        {
            return Err("BATCH_INSTRUMENT_SELECTION_REQUIRED".into());
        }
        let offset = signed.as_ref().map_or(0, |s| s.offset);
        if matches!(view, ResearchResultView::Export) {
            if signed.is_some() {
                return Err("INVALID_CURSOR".into());
            }
            return Ok(json!({
                "view":"export","schema":"okx.research.export/448.1",
                "job_id":job_id,"result_id":artifact,"leaf_id":leaf_artifact,
                "expires_at_ms":j.expires_at_ms,
                "integrity":verify_export_manifest(&self.artifacts, &value)?,
                "dataset_hash":value.get("dataset_hash"),
                "result_hash":value.get("hash"),
                "economic_audit":value.get("economic_audit"),
                "code_version":value.get("code_version"),
                "producer":value.get("producer"),
                "experiment_artifact":value.get("experiment_artifact"),
                "read_data_via":["trades","equity"],"side_effects":"none"
            }));
        }
        let mut metadata = json!({
            "job_id":job_id,"artifact":artifact,
            "expires_at_ms":j.expires_at_ms,
            "summary":value.get("summary").or_else(||value.pointer("/test/summary")),
            "economic_audit":value.get("economic_audit"),
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
            "producer":value.get("producer"),
            "result_hash":value.get("hash"),
            "experiment_artifact":value.get("experiment_artifact")
        });
        // A researcher reopening a graph job needs the actual typed recipe,
        // not just an opaque artifact ID. Named replay projections stay byte
        // compatible; only graph results include this immutable input.
        if let Some(signal) = value.get("graph_signal") {
            metadata["graph_signal"] = signal.clone();
        }
        // A batch summary is a compact overview, NOT a trade-page of bulky
        // per-instrument grid payloads. Drill down via an instrument cursor.
        if matches!(view, ResearchResultView::Summary) && value.get("instruments").is_some() {
            if cursor.is_some() {
                return Err("INVALID_CURSOR".into());
            }
            let items = value["instruments"]
                .as_array()
                .ok_or("RESULT_MANIFEST_INVALID")?;
            let rows = items
                .iter()
                .map(|row| {
                    json!({
                        "index": row.get("index"),
                        "instrument": row.get("instrument"),
                        "product": row.get("product"),
                        "quote_currency": row.get("quote_currency"),
                        "net_pnl_exact": row.pointer("/summary/pnl_exact"),
                        "economic_audit": row.get("economic_audit"),
                        "state": row.get("state"),
                        "reason_code": row.get("reason_code"),
                        "artifact": row.get("artifact"),
                        "dataset_hash": row.get("dataset_hash"),
                        "trades_cursor": row.get("trades_cursor")
                    })
                })
                .collect::<Vec<_>>();
            return Ok(json!({
                "view": "summary",
                "metadata": metadata,
                "rows": rows,
                "total_rows": rows.len(),
                "returned_rows": rows.len(),
                "next_cursor": null
            }));
        }
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
}

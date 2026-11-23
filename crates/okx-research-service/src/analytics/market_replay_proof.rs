//! Re-execute a bounded, source-complete Market row from the immutable input
//! pages. This is a separate read of persisted evidence, not a second pass
//! over the in-memory Loaded values used for the original calculation.
use super::*;
use okx_analysis::query_plan::Source;

// Re-execution is optional evidence, never an unbudgeted second research job.
// The public 1M-bar limit remains unchanged; larger results retain verified
// source pages but explicitly do not claim independent metric re-execution.
const MAX_REEXECUTION_OBSERVATIONS: u64 = 65_536;

fn not_verified(reason: &str) -> Value {
    json!({
        "schema":"okx.market.composition-metric-reexecution/v1",
        "quality":"not_verified",
        "reason":reason,
        "max_observations":MAX_REEXECUTION_OBSERVATIONS,
    })
}

fn load_pages<T: DeserializeOwned>(
    store: &ResearchArtifactStore,
    capture: &Value,
    cancel: &super::super::cpu_executor::Cancel,
) -> Result<Vec<T>, String> {
    let invalid = || "COMPOSITION_SOURCE_REEXECUTION_MISMATCH".to_owned();
    let manifest_id = capture["evidence_artifact"].as_str().ok_or_else(invalid)?;
    let manifest: Value = store
        .read_evidence_json(manifest_id)
        .map_err(|_| invalid())?;
    let expected = manifest["observed_points"].as_u64().ok_or_else(invalid)?;
    if expected > MAX_REEXECUTION_OBSERVATIONS {
        return Err(invalid());
    }
    let mut restored = Vec::with_capacity(expected as usize);
    for id in manifest["chunks"].as_array().ok_or_else(invalid)? {
        if cancel.load(Ordering::Acquire) {
            return Err("CANCELLED".into());
        }
        let id = id.as_str().ok_or_else(invalid)?;
        let mut page: Vec<T> = store.read_evidence_json(id).map_err(|_| invalid())?;
        restored.append(&mut page);
        if restored.len() as u64 > expected {
            return Err(invalid());
        }
    }
    if restored.len() as u64 != expected {
        return Err(invalid());
    }
    Ok(restored)
}

fn semantic_row(mut row: Value) -> Result<Value, String> {
    let record = row
        .as_object_mut()
        .ok_or("COMPOSITION_REEXECUTION_ROW_INVALID")?;
    record.remove("source_integrity");
    if let Some(captures) = record
        .get_mut("source_captures")
        .and_then(Value::as_object_mut)
    {
        for capture in captures.values_mut() {
            if let Some(source) = capture.as_object_mut() {
                source.remove("evidence_artifact");
            }
        }
    }
    Ok(row)
}

pub(super) fn verify_persisted_market_metrics(
    store: &ResearchArtifactStore,
    linked: &Value,
    request: &MarketRequest,
    plan: &okx_analysis::query_plan::QueryPlan,
    need_candles: bool,
    cancel: &super::super::cpu_executor::Cancel,
) -> Result<Value, String> {
    if linked["source_integrity"]["quality"] != "verified_normalized_sources" {
        return Ok(not_verified("SOURCE_UNAVAILABLE"));
    }
    // Ticker has no historical capture timestamp, while the cross-instrument
    // Pearson peer is external to a single row. Neither may be silently
    // represented by current observations or a caller-supplied peer.
    if plan.sources.contains(&Source::Ticker) {
        return Ok(not_verified("UNPERSISTED_TICKER_SNAPSHOT"));
    }
    if request
        .fields
        .iter()
        .any(|field| field == "pearson_correlation")
    {
        return Ok(not_verified("UNPERSISTED_CORRELATION_PEER"));
    }

    let required = [
        (Source::Candles, "candles"),
        (Source::OpenInterest, "open_interest"),
        (Source::Funding, "funding_rate"),
    ];
    let mut total = 0u64;
    for (kind, name) in required {
        if !plan.sources.contains(&kind) {
            continue;
        }
        let capture = &linked["source_captures"][name];
        if capture["evidence_artifact"].as_str().is_none() {
            return Ok(not_verified("UNPERSISTED_SOURCE_INPUT"));
        }
        let observed = linked["source_integrity"]["verified_sources"]
            .as_array()
            .and_then(|sources| sources.iter().find(|s| s["source"] == name))
            .and_then(|source| source["observed_points"].as_u64())
            .ok_or("COMPOSITION_SOURCE_REEXECUTION_MISMATCH")?;
        total = total
            .checked_add(observed)
            .ok_or("COMPOSITION_SOURCE_REEXECUTION_MISMATCH")?;
    }
    if total > MAX_REEXECUTION_OBSERVATIONS {
        return Ok(not_verified("REEXECUTION_OBSERVATION_BUDGET_EXCEEDED"));
    }
    if cancel.load(Ordering::Acquire) {
        return Err("CANCELLED".into());
    }

    let bars = if need_candles {
        Arc::new(load_pages::<Bar>(
            store,
            &linked["source_captures"]["candles"],
            cancel,
        )?)
    } else {
        Arc::new(Vec::new())
    };
    let oi = if plan.sources.contains(&Source::OpenInterest) {
        Some(Ok(load_pages::<(u64, f64)>(
            store,
            &linked["source_captures"]["open_interest"],
            cancel,
        )?))
    } else {
        None
    };
    let funding = if plan.sources.contains(&Source::Funding) {
        Some(Ok(load_pages::<(u64, f64)>(
            store,
            &linked["source_captures"]["funding_rate"],
            cancel,
        )?))
    } else {
        None
    };
    let restored = Loaded {
        instrument: linked["instrument"]
            .as_str()
            .ok_or("COMPOSITION_REEXECUTION_INSTRUMENT_MISSING")?
            .to_owned(),
        bars: Ok(bars),
        ticker: None,
        oi,
        funding,
        // Excluded unclosed bars are source-adapter metadata, not a historical
        // venue observation. The proof binds to this recorded count only.
        excluded: linked["unclosed_candles_excluded"]
            .as_u64()
            .map(|count| count as usize),
    };
    let replayed = compute_row_planned(restored, request, None, plan, need_candles)?;
    let recorded = semantic_row(linked.clone())?;
    if replayed != recorded {
        return Err("COMPOSITION_SOURCE_REEXECUTION_MISMATCH".into());
    }
    let hash =
        okx_research::canonical_sha256(&("okx.market.composition-semantic-row/v1", &replayed))
            .map_err(|error| error.to_string())?;
    Ok(json!({
        "schema":"okx.market.composition-metric-reexecution/v1",
        "quality":"verified_reexecution",
        "semantic_row_hash":hash,
        "observations_rehydrated":total,
        "max_observations":MAX_REEXECUTION_OBSERVATIONS,
        "input":"independently_reloaded_verified_pages",
        "excluded_unclosed_count":"recorded_source_adapter_metadata",
        "historical_publication_time_verified":false,
    }))
}

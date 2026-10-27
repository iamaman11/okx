//! Native Research result presentation: artifact integrity, export verification,
//! bounded paging, and signed cursors. No SQL mutation or research execution.
use super::{AnalyticsService, cursor, jobs};
use okx_protocol::analytics::ResearchResultView;
use okx_research::ResearchArtifactStore;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

fn read_verified_result<T: DeserializeOwned>(
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
    Ok(json!({
        "quality":"complete",
        "counts_verified":true,
        "checksums_verified":true,
        "trades":trades,"rows":rows,"equity":equity
    }))
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
                "code_version":value.get("code_version"),
                "read_data_via":["trades","equity"],"side_effects":"none"
            }));
        }
        let metadata = json!({
            "job_id":job_id,"artifact":artifact,
            "expires_at_ms":j.expires_at_ms,
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

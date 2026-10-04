use std::collections::BTreeMap;

use okx_protocol::{
    AGENT_RESPONSE_SCHEMA_V1, AgentOperation, AgentRequest, AgentResponse, AgentResponseStatus,
    DataQuality, RESEARCH_CATALOG_VERSION_V1, ResearchRequest,
};
use okx_research::{
    BUILD_SOURCE_TREE, DatasetManifest, ReferenceCoverageStatus, ResearchArtifactStore,
    ResearchRange, ResearchSourceKind, ResearchTier, SourceRequest, build_candle_chunk,
    build_funding_chunk, build_reference_chunk, detect_fixed_interval_gaps,
};
use serde::Serialize;

use super::{
    MARKET_PUBLIC_API_UNAVAILABLE_CODE, ObservationQueryContext, failure_response, unavailable,
};
use crate::{AgentResult, market_bootstrap::MarketBootstrapError};

pub const RESEARCH_CAPABILITIES_SCHEMA_V1: &str = "okx.research-capabilities/v1";
pub const RESEARCH_DATA_INSPECTION_SCHEMA_V1: &str = "okx.research-data-inspection/v1";
pub const RESEARCH_ARTIFACT_FAILURE_CODE: &str = "RESEARCH_ARTIFACT_FAILURE";
pub const INSUFFICIENT_REFERENCE_HISTORY_CODE: &str = "INSUFFICIENT_REFERENCE_HISTORY";
const PARSER_VERSION_V1: &str = "okx.public-history-parser/v1";
const NORMALIZATION_VERSION_V1: &str = "okx.research-normalizer/v1";
const ONE_HOUR_MS: u64 = 3_600_000;
const NORMAL_RESULT_TARGET_BYTES: u64 = 12_288;

#[derive(Serialize)]
struct ResearchCapabilitiesResult {
    schema: &'static str,
    catalog_version: &'static str,
    stage: &'static str,
    tier_a_instruments: [&'static str; 3],
    tier_a_bars: [&'static str; 1],
    candle_page_limit_max: u16,
    funding_page_limit_max: u16,
    normal_result_target_bytes: u64,
    source_tree: &'static str,
    source_tree_bound: bool,
    reference_history_policy: &'static str,
    bulk_history_over_mcp: bool,
    exchange_mutation_authority: bool,
}

#[derive(Serialize)]
struct ResearchChunkSummary {
    kind: ResearchSourceKind,
    chunk_id: String,
    artifact_id: String,
    raw_sha256: String,
    normalized_sha256: String,
    normalized_row_count: u64,
    oldest_event_time_ms: Option<String>,
    newest_event_time_ms: Option<String>,
}

#[derive(Serialize)]
struct ResearchDataInspectionResult {
    schema: &'static str,
    catalog_version: &'static str,
    stage: &'static str,
    tier: ResearchTier,
    instrument: String,
    bar: String,
    dataset_id: String,
    dataset_artifact_id: String,
    range: ResearchRange,
    reference_coverage: ReferenceCoverageStatus,
    strategy_ready: bool,
    blocker: Option<&'static str>,
    gaps: Vec<okx_research::DataGap>,
    chunks: Vec<ResearchChunkSummary>,
    source_tree: &'static str,
    evidence_store: &'static str,
    source_cache: &'static str,
}

pub(crate) async fn dispatch(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
) -> AgentResult<AgentResponse> {
    match &request.operation {
        AgentOperation::ResearchCapabilities => capabilities(request, generated_at),
        AgentOperation::Research {
            request: ResearchRequest::InspectTierA {
                catalog_version: _,
                instrument,
                bar,
                candle_limit,
                funding_limit,
            },
        } => {
            inspect_tier_a(
                request,
                context,
                generated_at,
                instrument,
                bar,
                *candle_limit,
                *funding_limit,
            )
            .await
        }
        _ => Ok(unavailable(request, generated_at)),
    }
}

fn capabilities(request: &AgentRequest, generated_at: &str) -> AgentResult<AgentResponse> {
    let result = ResearchCapabilitiesResult {
        schema: RESEARCH_CAPABILITIES_SCHEMA_V1,
        catalog_version: RESEARCH_CATALOG_VERSION_V1,
        stage: "3A_V1",
        tier_a_instruments: [
            "BTC-USDT-SWAP",
            "ETH-USDT-SWAP",
            "DOGE-USDT-SWAP",
        ],
        tier_a_bars: ["1H"],
        candle_page_limit_max: 100,
        funding_page_limit_max: 400,
        normal_result_target_bytes: NORMAL_RESULT_TARGET_BYTES,
        source_tree: BUILD_SOURCE_TREE,
        source_tree_bound: BUILD_SOURCE_TREE != "UNAVAILABLE",
        reference_history_policy: "FAIL_CLOSED_POINT_IN_TIME_REQUIRED",
        bulk_history_over_mcp: false,
        exchange_mutation_authority: false,
    };
    Ok(AgentResponse {
        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
        request_id: request.request_id.clone(),
        status: AgentResponseStatus::Completed,
        generated_at: generated_at.to_owned(),
        quality: if result.source_tree_bound {
            DataQuality::Fresh
        } else {
            DataQuality::Degraded
        },
        result_schema: Some(RESEARCH_CAPABILITIES_SCHEMA_V1.to_owned()),
        result: Some(serde_json::to_value(result)?),
        failure: None,
        warnings: if BUILD_SOURCE_TREE == "UNAVAILABLE" {
            vec!["research build source tree is unavailable".to_owned()]
        } else {
            Vec::new()
        },
    })
}

async fn inspect_tier_a(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    instrument: &str,
    bar: &str,
    candle_limit: u16,
    funding_limit: u16,
) -> AgentResult<AgentResponse> {
    let Some(market) = context.market_fallback else {
        return Ok(unavailable(request, generated_at));
    };
    let Some(research_root) = context.research_root else {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            RESEARCH_ARTIFACT_FAILURE_CODE,
            "research artifact root is unavailable".to_owned(),
            false,
        ));
    };
    if BUILD_SOURCE_TREE == "UNAVAILABLE" {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            RESEARCH_ARTIFACT_FAILURE_CODE,
            "research source tree is not bound into this build".to_owned(),
            false,
        ));
    }

    let acquired = tokio::try_join!(
        market.research_candles_page(instrument, bar, None, None, candle_limit),
        market.research_funding_page(instrument, None, None, funding_limit),
        market.research_current_reference(instrument),
    );
    let (candles, funding, reference) = match acquired {
        Ok(value) => value,
        Err(error) => return Ok(source_failure(request, generated_at, error)),
    };

    let Some(first) = candles.rows.first() else {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            MARKET_PUBLIC_API_UNAVAILABLE_CODE,
            format!("OKX returned no research candles for '{instrument}'"),
            true,
        ));
    };
    let Some(last) = candles.rows.last() else {
        unreachable!("first row exists");
    };
    let range_end = match last
        .open_time_ms
        .parse::<u64>()
        .ok()
        .and_then(|value| value.checked_add(ONE_HOUR_MS))
    {
        Some(value) => value.to_string(),
        None => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                RESEARCH_ARTIFACT_FAILURE_CODE,
                "research candle range overflowed u64".to_owned(),
                false,
            ));
        }
    };
    let range = match ResearchRange::new(first.open_time_ms.clone(), range_end) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };

    let candle_source = SourceRequest {
        provider: "okx_public_rest".to_owned(),
        resource: "/api/v5/market/history-candles".to_owned(),
        instrument_id: instrument.to_owned(),
        bar: Some(bar.to_owned()),
        range: range.clone(),
        parameters: BTreeMap::from([
            ("limit".to_owned(), candle_limit.to_string()),
            ("selection".to_owned(), "latest_page".to_owned()),
        ]),
    };
    let candle_chunk = match build_candle_chunk(
        candle_source,
        candles.acquired_at_ms,
        &candles.raw_body,
        &candles.rows,
        ONE_HOUR_MS,
        PARSER_VERSION_V1,
        NORMALIZATION_VERSION_V1,
        BUILD_SOURCE_TREE,
    ) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };

    let funding_rows = funding
        .rows
        .into_iter()
        .filter(|event| {
            event
                .funding_time_ms
                .parse::<u64>()
                .ok()
                .is_some_and(|timestamp| {
                    timestamp >= range.begin().unwrap_or(u64::MAX)
                        && timestamp < range.end().unwrap_or(0)
                })
        })
        .collect::<Vec<_>>();
    let funding_source = SourceRequest {
        provider: "okx_public_rest".to_owned(),
        resource: "/api/v5/public/funding-rate-history".to_owned(),
        instrument_id: instrument.to_owned(),
        bar: None,
        range: range.clone(),
        parameters: BTreeMap::from([
            ("limit".to_owned(), funding_limit.to_string()),
            (
                "selection".to_owned(),
                "events_within_candle_range".to_owned(),
            ),
        ]),
    };
    let funding_chunk = match build_funding_chunk(
        funding_source,
        funding.acquired_at_ms,
        &funding.raw_body,
        &funding_rows,
        PARSER_VERSION_V1,
        NORMALIZATION_VERSION_V1,
        BUILD_SOURCE_TREE,
    ) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };

    let reference_source = SourceRequest {
        provider: "okx_public_rest".to_owned(),
        resource: "/api/v5/public/instruments".to_owned(),
        instrument_id: instrument.to_owned(),
        bar: None,
        range: range.clone(),
        parameters: BTreeMap::from([(
            "semantics".to_owned(),
            "current_snapshot_only".to_owned(),
        )]),
    };
    let (reference_chunk, reference_window) = match build_reference_chunk(
        reference_source,
        reference.acquired_at_ms.clone(),
        &reference.raw_body,
        &reference.instrument,
        reference.acquired_at_ms.clone(),
        reference.acquired_at_ms.clone(),
        reference.acquired_at_ms,
        reference.reference_generation,
        PARSER_VERSION_V1,
        NORMALIZATION_VERSION_V1,
        BUILD_SOURCE_TREE,
    ) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };

    let gaps = match detect_fixed_interval_gaps(
        &candle_chunk
            .rows
            .iter()
            .map(|row| row.open_time_ms.clone())
            .collect::<Vec<_>>(),
        ONE_HOUR_MS,
    ) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };

    let chunk_manifests = vec![
        candle_chunk.manifest.clone(),
        funding_chunk.manifest.clone(),
        reference_chunk.manifest.clone(),
    ];
    let manifest = match DatasetManifest::build(
        ResearchTier::TierA,
        instrument,
        Some(bar.to_owned()),
        range.clone(),
        Some(reference_window),
        &chunk_manifests,
        gaps,
        PARSER_VERSION_V1,
        NORMALIZATION_VERSION_V1,
        BUILD_SOURCE_TREE,
        utc_now_ms().to_string(),
    ) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };

    let store = ResearchArtifactStore::at(research_root.join("research"));
    let persisted = (|| {
        let raw_sources = [
            (&candle_chunk.manifest.raw_sha256, candles.raw_body.as_slice()),
            (&funding_chunk.manifest.raw_sha256, funding.raw_body.as_slice()),
            (&reference_chunk.manifest.raw_sha256, reference.raw_body.as_slice()),
        ];
        for (id, bytes) in raw_sources {
            store.publish_source_bytes(id, bytes)?;
        }

        let mut chunks = Vec::with_capacity(chunk_manifests.len());
        for chunk in &chunk_manifests {
            let (artifact_id, _) = store.publish_evidence(chunk)?;
            chunks.push(ResearchChunkSummary {
                kind: chunk.kind,
                chunk_id: chunk.chunk_id.clone(),
                artifact_id,
                raw_sha256: chunk.raw_sha256.clone(),
                normalized_sha256: chunk.normalized_sha256.clone(),
                normalized_row_count: chunk.normalized_row_count,
                oldest_event_time_ms: chunk.oldest_event_time_ms.clone(),
                newest_event_time_ms: chunk.newest_event_time_ms.clone(),
            });
        }
        let (dataset_artifact_id, _) = store.publish_evidence(&manifest)?;
        Ok::<_, okx_research::ResearchError>((dataset_artifact_id, chunks))
    })();
    let (dataset_artifact_id, chunks) = match persisted {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };

    let strategy_ready = manifest.reference_coverage == ReferenceCoverageStatus::Complete
        && manifest.gaps.is_empty();
    let blocker = (!strategy_ready).then_some(if manifest.reference_coverage
        != ReferenceCoverageStatus::Complete
    {
        INSUFFICIENT_REFERENCE_HISTORY_CODE
    } else {
        "INSUFFICIENT_DATA"
    });
    let quality = if strategy_ready {
        DataQuality::Fresh
    } else {
        DataQuality::Degraded
    };
    let warnings = blocker
        .map(|code| {
            vec![format!(
                "{code}: Stage 3A refuses to treat the current instrument snapshot as historical point-in-time coverage"
            )]
        })
        .unwrap_or_default();

    let result = ResearchDataInspectionResult {
        schema: RESEARCH_DATA_INSPECTION_SCHEMA_V1,
        catalog_version: RESEARCH_CATALOG_VERSION_V1,
        stage: "3A_V1",
        tier: ResearchTier::TierA,
        instrument: instrument.to_owned(),
        bar: bar.to_owned(),
        dataset_id: manifest.dataset_id.clone(),
        dataset_artifact_id,
        range,
        reference_coverage: manifest.reference_coverage,
        strategy_ready,
        blocker,
        gaps: manifest.gaps.clone(),
        chunks,
        source_tree: BUILD_SOURCE_TREE,
        evidence_store: "PINNED_CONTENT_ADDRESSED",
        source_cache: "CONTENT_ADDRESSED_REDOWNLOADABLE",
    };

    Ok(AgentResponse {
        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
        request_id: request.request_id.clone(),
        status: AgentResponseStatus::Completed,
        generated_at: generated_at.to_owned(),
        quality,
        result_schema: Some(RESEARCH_DATA_INSPECTION_SCHEMA_V1.to_owned()),
        result: Some(serde_json::to_value(result)?),
        failure: None,
        warnings,
    })
}

fn source_failure(
    request: &AgentRequest,
    generated_at: &str,
    error: MarketBootstrapError,
) -> AgentResponse {
    failure_response(
        request,
        generated_at,
        AgentResponseStatus::Failed,
        MARKET_PUBLIC_API_UNAVAILABLE_CODE,
        error.to_string(),
        true,
    )
}

fn research_failure(
    request: &AgentRequest,
    generated_at: &str,
    error: okx_research::ResearchError,
) -> AgentResponse {
    failure_response(
        request,
        generated_at,
        AgentResponseStatus::Failed,
        RESEARCH_ARTIFACT_FAILURE_CODE,
        error.to_string(),
        false,
    )
}

use std::collections::{BTreeMap, BTreeSet};

use okx_protocol::{
    AGENT_RESPONSE_SCHEMA_V1, AgentRequest, AgentResponse, AgentResponseStatus, DataQuality,
    RESEARCH_CATALOG_VERSION_V1,
};
use okx_research::{
    BUILD_SOURCE_TREE, DatasetManifest, ReferenceCoverageStatus, ReplayDatasetArtifact,
    ResearchArtifactStore, ResearchCandle, ResearchCheckpoint, ResearchCheckpointPage,
    ResearchCheckpointPhase, ResearchCheckpointTerminal, ResearchChunkArtifact,
    ResearchFundingEvent, ResearchRange, ResearchSourceKind, ResearchTier, SourceRequest,
    build_candle_chunk, build_chunk_artifact, build_funding_chunk, build_reference_chunk,
    detect_fixed_interval_gaps,
};
use serde::Serialize;

use super::{
    MARKET_PUBLIC_API_UNAVAILABLE_CODE, ObservationQueryContext, failure_response, utc_now_ms,
};
use crate::{AgentResult, market_bootstrap::MarketBootstrapError};

use super::research::{
    NORMALIZATION_VERSION_V1, ONE_HOUR_MS, PARSER_VERSION_V1, RESEARCH_ARTIFACT_FAILURE_CODE,
};

pub(super) const RESEARCH_VALIDATION_DATASET_SCHEMA_V1: &str = "okx.research-validation-dataset/v1";
pub(super) const VALIDATION_TARGET_CANDLES_MIN: u16 = 240;
pub(super) const VALIDATION_TARGET_CANDLES_MAX: u16 = 2400;
pub(super) const VALIDATION_PAGES_PER_CALL: usize = 2;

const CANDLE_PAGE_LIMIT: u16 = 100;
const FUNDING_PAGE_LIMIT: u16 = 400;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ValidationDatasetState {
    ContinuationRequired,
    Completed,
    InsufficientData,
}

#[derive(Serialize)]
struct ValidationDatasetPreparationResult {
    schema: &'static str,
    catalog_version: &'static str,
    stage: &'static str,
    state: ValidationDatasetState,
    instrument: String,
    bar: String,
    target_candle_count: u16,
    candle_count: usize,
    funding_event_count: usize,
    phase: ResearchCheckpointPhase,
    checkpoint_id: String,
    checkpoint_artifact_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    range: Option<ResearchRange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dataset_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dataset_artifact_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    replay_dataset_artifact_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reference_coverage: Option<ReferenceCoverageStatus>,
    gaps: Vec<okx_research::DataGap>,
    continuation_required: bool,
    bulk_rows_returned: bool,
    source_tree: &'static str,
    exchange_mutation_authority: bool,
}

pub(super) async fn prepare_validation_dataset(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    instrument: &str,
    bar: &str,
    target_candle_count: u16,
    checkpoint_artifact_id: Option<&str>,
) -> AgentResult<AgentResponse> {
    let Some(market) = context.market_fallback else {
        return Ok(super::unavailable(request, generated_at));
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

    let store = ResearchArtifactStore::at(research_root.join("research"));
    let mut checkpoint = match checkpoint_artifact_id {
        Some(id) => match store.read_evidence_json::<ResearchCheckpoint>(id) {
            Ok(value) => value,
            Err(error) => return Ok(research_failure(request, generated_at, error)),
        },
        None => match ResearchCheckpoint::build(
            None,
            instrument,
            bar,
            target_candle_count,
            ResearchCheckpointPhase::Candles,
            Vec::new(),
            None,
            None,
            None,
            BUILD_SOURCE_TREE,
            utc_now_ms().to_string(),
        ) {
            Ok(value) => value,
            Err(error) => return Ok(research_failure(request, generated_at, error)),
        },
    };

    if let Err(error) = checkpoint.validate() {
        return Ok(research_failure(request, generated_at, error));
    }
    if checkpoint.instrument_id != instrument
        || checkpoint.bar != bar
        || checkpoint.target_candle_count != target_candle_count
        || checkpoint.source_tree != BUILD_SOURCE_TREE
    {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            RESEARCH_ARTIFACT_FAILURE_CODE,
            "validation checkpoint does not match the requested immutable acquisition spec"
                .to_owned(),
            false,
        ));
    }

    if checkpoint.phase == ResearchCheckpointPhase::Complete {
        return completed_response_from_checkpoint(
            request,
            generated_at,
            &store,
            checkpoint,
            checkpoint_artifact_id.expect("complete checkpoint was loaded"),
        );
    }

    let parent_checkpoint_id = checkpoint_artifact_id.map(|_| checkpoint.checkpoint_id.clone());

    match checkpoint.phase {
        ResearchCheckpointPhase::Candles => {
            let mut current_rows = match load_candles(&store, &checkpoint.completed_pages) {
                Ok(value) => value.len(),
                Err(error) => return Ok(research_failure(request, generated_at, error)),
            };
            let mut exhausted = false;

            for _ in 0..VALIDATION_PAGES_PER_CALL {
                if current_rows >= usize::from(target_candle_count) {
                    break;
                }
                let remaining = usize::from(target_candle_count) - current_rows;
                let limit = u16::try_from(remaining.min(usize::from(CANDLE_PAGE_LIMIT)))
                    .expect("bounded page limit");
                let captured = match market
                    .research_candles_page(
                        instrument,
                        bar,
                        checkpoint.remaining_cursor.as_deref(),
                        None,
                        limit,
                    )
                    .await
                {
                    Ok(value) => value,
                    Err(error) => return Ok(source_failure(request, generated_at, error)),
                };
                let page_exhausted = captured.rows.len() < usize::from(limit);
                let confirmed = captured
                    .rows
                    .iter()
                    .filter(|row| row.confirmed)
                    .cloned()
                    .collect::<Vec<_>>();
                if confirmed.is_empty() {
                    exhausted = true;
                    break;
                }

                let (page_range, oldest) = match candle_page_range(&confirmed) {
                    Ok(value) => value,
                    Err(error) => return Ok(research_failure(request, generated_at, error)),
                };
                let mut parameters = BTreeMap::from([
                    ("limit".to_owned(), limit.to_string()),
                    (
                        "selection".to_owned(),
                        if checkpoint.remaining_cursor.is_some() {
                            "older_than_cursor".to_owned()
                        } else {
                            "latest_page".to_owned()
                        },
                    ),
                ]);
                if let Some(cursor) = checkpoint.remaining_cursor.as_ref() {
                    parameters.insert("after".to_owned(), cursor.clone());
                }
                let source = SourceRequest {
                    provider: "okx_public_rest".to_owned(),
                    resource: "/api/v5/market/history-candles".to_owned(),
                    instrument_id: instrument.to_owned(),
                    bar: Some(bar.to_owned()),
                    range: page_range,
                    parameters,
                };
                let chunk = match build_candle_chunk(
                    source,
                    captured.acquired_at_ms,
                    &captured.raw_body,
                    &confirmed,
                    ONE_HOUR_MS,
                    PARSER_VERSION_V1,
                    NORMALIZATION_VERSION_V1,
                    BUILD_SOURCE_TREE,
                ) {
                    Ok(value) => value,
                    Err(error) => return Ok(research_failure(request, generated_at, error)),
                };
                if let Err(error) =
                    store.publish_source_bytes(&chunk.manifest.raw_sha256, &captured.raw_body)
                {
                    return Ok(research_failure(request, generated_at, error));
                }
                let chunk_artifact = match build_chunk_artifact(&chunk) {
                    Ok(value) => value,
                    Err(error) => return Ok(research_failure(request, generated_at, error)),
                };
                let (artifact_id, _) = match store.publish_evidence(&chunk_artifact) {
                    Ok(value) => value,
                    Err(error) => return Ok(research_failure(request, generated_at, error)),
                };
                if checkpoint
                    .remaining_cursor
                    .as_deref()
                    .is_some_and(|cursor| {
                        cursor
                            .parse::<u64>()
                            .ok()
                            .zip(oldest.parse::<u64>().ok())
                            .is_none_or(|(previous, next)| next >= previous)
                    })
                {
                    return Ok(research_failure(
                        request,
                        generated_at,
                        okx_research::ResearchError::CursorDidNotAdvance,
                    ));
                }
                checkpoint
                    .completed_pages
                    .push(page_ref(&chunk_artifact, artifact_id));
                checkpoint.remaining_cursor = Some(oldest);
                current_rows = match load_candles(&store, &checkpoint.completed_pages) {
                    Ok(value) => value.len(),
                    Err(error) => return Ok(research_failure(request, generated_at, error)),
                };
                if page_exhausted {
                    exhausted = true;
                    break;
                }
            }

            if current_rows < usize::from(target_candle_count) {
                let state = if exhausted {
                    ValidationDatasetState::InsufficientData
                } else {
                    ValidationDatasetState::ContinuationRequired
                };
                let next = match ResearchCheckpoint::build(
                    parent_checkpoint_id,
                    instrument,
                    bar,
                    target_candle_count,
                    if exhausted {
                        ResearchCheckpointPhase::InsufficientData
                    } else {
                        ResearchCheckpointPhase::Candles
                    },
                    checkpoint.completed_pages,
                    if exhausted {
                        None
                    } else {
                        checkpoint.remaining_cursor
                    },
                    None,
                    None,
                    BUILD_SOURCE_TREE,
                    utc_now_ms().to_string(),
                ) {
                    Ok(value) => value,
                    Err(error) => return Ok(research_failure(request, generated_at, error)),
                };
                return checkpoint_response(
                    request,
                    generated_at,
                    &store,
                    next,
                    state,
                    Vec::new(),
                );
            }

            let candles = match load_candles(&store, &checkpoint.completed_pages) {
                Ok(value) => value,
                Err(error) => return Ok(research_failure(request, generated_at, error)),
            };
            let selected = select_latest_candles(candles, target_candle_count);
            let range = match replay_range(&selected) {
                Ok(value) => value,
                Err(error) => return Ok(research_failure(request, generated_at, error)),
            };
            let next = match ResearchCheckpoint::build(
                parent_checkpoint_id,
                instrument,
                bar,
                target_candle_count,
                ResearchCheckpointPhase::Funding,
                checkpoint.completed_pages,
                None,
                Some(range),
                None,
                BUILD_SOURCE_TREE,
                utc_now_ms().to_string(),
            ) {
                Ok(value) => value,
                Err(error) => return Ok(research_failure(request, generated_at, error)),
            };
            checkpoint_response(
                request,
                generated_at,
                &store,
                next,
                ValidationDatasetState::ContinuationRequired,
                Vec::new(),
            )
        }
        ResearchCheckpointPhase::Funding => {
            let range = checkpoint
                .dataset_range
                .clone()
                .expect("validated funding checkpoint has range");
            let range_begin = match range.begin() {
                Ok(value) => value,
                Err(error) => return Ok(research_failure(request, generated_at, error)),
            };
            let range_end = match range.end() {
                Ok(value) => value,
                Err(error) => return Ok(research_failure(request, generated_at, error)),
            };
            let mut done = false;

            for _ in 0..VALIDATION_PAGES_PER_CALL {
                let captured = match market
                    .research_funding_page(
                        instrument,
                        checkpoint.remaining_cursor.as_deref(),
                        None,
                        FUNDING_PAGE_LIMIT,
                    )
                    .await
                {
                    Ok(value) => value,
                    Err(error) => return Ok(source_failure(request, generated_at, error)),
                };
                if captured.rows.is_empty() {
                    done = true;
                    break;
                }
                let oldest_all = captured
                    .rows
                    .iter()
                    .filter_map(|row| row.funding_time_ms.parse::<u64>().ok())
                    .min()
                    .unwrap_or(u64::MAX);
                let page_exhausted = captured.rows.len() < usize::from(FUNDING_PAGE_LIMIT);
                let in_range = captured
                    .rows
                    .iter()
                    .filter(|row| {
                        row.funding_time_ms
                            .parse::<u64>()
                            .ok()
                            .is_some_and(|timestamp| {
                                timestamp >= range_begin && timestamp < range_end
                            })
                    })
                    .cloned()
                    .collect::<Vec<_>>();

                let mut parameters = BTreeMap::from([
                    ("limit".to_owned(), FUNDING_PAGE_LIMIT.to_string()),
                    (
                        "selection".to_owned(),
                        "events_within_parent_range".to_owned(),
                    ),
                ]);
                if let Some(cursor) = checkpoint.remaining_cursor.as_ref() {
                    parameters.insert("after".to_owned(), cursor.clone());
                }
                let source = SourceRequest {
                    provider: "okx_public_rest".to_owned(),
                    resource: "/api/v5/public/funding-rate-history".to_owned(),
                    instrument_id: instrument.to_owned(),
                    bar: None,
                    range: range.clone(),
                    parameters,
                };
                let chunk = match build_funding_chunk(
                    source,
                    captured.acquired_at_ms,
                    &captured.raw_body,
                    &in_range,
                    PARSER_VERSION_V1,
                    NORMALIZATION_VERSION_V1,
                    BUILD_SOURCE_TREE,
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        return Ok(research_failure(request, generated_at, error));
                    }
                };
                if let Err(error) =
                    store.publish_source_bytes(&chunk.manifest.raw_sha256, &captured.raw_body)
                {
                    return Ok(research_failure(request, generated_at, error));
                }
                let chunk_artifact = match build_chunk_artifact(&chunk) {
                    Ok(value) => value,
                    Err(error) => {
                        return Ok(research_failure(request, generated_at, error));
                    }
                };
                let (artifact_id, _) = match store.publish_evidence(&chunk_artifact) {
                    Ok(value) => value,
                    Err(error) => {
                        return Ok(research_failure(request, generated_at, error));
                    }
                };
                checkpoint
                    .completed_pages
                    .push(page_ref(&chunk_artifact, artifact_id));

                if checkpoint
                    .remaining_cursor
                    .as_deref()
                    .is_some_and(|cursor| {
                        cursor
                            .parse::<u64>()
                            .ok()
                            .is_none_or(|previous| oldest_all >= previous)
                    })
                {
                    return Ok(research_failure(
                        request,
                        generated_at,
                        okx_research::ResearchError::CursorDidNotAdvance,
                    ));
                }
                checkpoint.remaining_cursor = Some(oldest_all.to_string());
                if oldest_all <= range_begin || page_exhausted {
                    done = true;
                    break;
                }
            }

            if !done {
                let next = match ResearchCheckpoint::build(
                    parent_checkpoint_id,
                    instrument,
                    bar,
                    target_candle_count,
                    ResearchCheckpointPhase::Funding,
                    checkpoint.completed_pages,
                    checkpoint.remaining_cursor,
                    Some(range),
                    None,
                    BUILD_SOURCE_TREE,
                    utc_now_ms().to_string(),
                ) {
                    Ok(value) => value,
                    Err(error) => return Ok(research_failure(request, generated_at, error)),
                };
                return checkpoint_response(
                    request,
                    generated_at,
                    &store,
                    next,
                    ValidationDatasetState::ContinuationRequired,
                    Vec::new(),
                );
            }

            finalize_dataset(
                request,
                context,
                generated_at,
                &store,
                checkpoint,
                parent_checkpoint_id,
            )
            .await
        }
        ResearchCheckpointPhase::InsufficientData => checkpoint_response(
            request,
            generated_at,
            &store,
            checkpoint,
            ValidationDatasetState::InsufficientData,
            Vec::new(),
        ),
        ResearchCheckpointPhase::Complete => unreachable!("handled above"),
    }
}

async fn finalize_dataset(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    store: &ResearchArtifactStore,
    checkpoint: ResearchCheckpoint,
    parent_checkpoint_id: Option<String>,
) -> AgentResult<AgentResponse> {
    let Some(market) = context.market_fallback else {
        return Ok(super::unavailable(request, generated_at));
    };
    let mut candles = match load_candles(store, &checkpoint.completed_pages) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };
    candles = select_latest_candles(candles, checkpoint.target_candle_count);
    if candles.len() != usize::from(checkpoint.target_candle_count) {
        return checkpoint_response(
            request,
            generated_at,
            store,
            checkpoint,
            ValidationDatasetState::InsufficientData,
            Vec::new(),
        );
    }
    let range = match replay_range(&candles) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };
    let funding = match load_funding(store, &checkpoint.completed_pages, &range) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };
    let gaps = match detect_fixed_interval_gaps(
        &candles
            .iter()
            .map(|row| row.open_time_ms.clone())
            .collect::<Vec<_>>(),
        ONE_HOUR_MS,
    ) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };

    let reference = match market
        .research_current_reference(&checkpoint.instrument_id)
        .await
    {
        Ok(value) => value,
        Err(error) => return Ok(source_failure(request, generated_at, error)),
    };
    let observed_ms = match reference.acquired_at_ms.parse::<u64>() {
        Ok(value) => value,
        Err(_) => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                RESEARCH_ARTIFACT_FAILURE_CODE,
                "captured reference timestamp is invalid".to_owned(),
                false,
            ));
        }
    };
    let Some(observed_through) = observed_ms.checked_add(1) else {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            RESEARCH_ARTIFACT_FAILURE_CODE,
            "captured reference timestamp overflowed u64".to_owned(),
            false,
        ));
    };
    let reference_source = SourceRequest {
        provider: "okx_public_rest".to_owned(),
        resource: "/api/v5/public/instruments".to_owned(),
        instrument_id: checkpoint.instrument_id.clone(),
        bar: None,
        range: ResearchRange::new(observed_ms.to_string(), observed_through.to_string())
            .expect("one millisecond reference capture range"),
        parameters: BTreeMap::from([("semantics".to_owned(), "current_snapshot_only".to_owned())]),
    };
    let (reference_chunk, reference_window) = match build_reference_chunk(
        reference_source,
        reference.acquired_at_ms.clone(),
        &reference.raw_body,
        &reference.instrument,
        observed_ms.to_string(),
        observed_through.to_string(),
        observed_ms.to_string(),
        reference.reference_generation,
        PARSER_VERSION_V1,
        NORMALIZATION_VERSION_V1,
        BUILD_SOURCE_TREE,
    ) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };
    if let Err(error) =
        store.publish_source_bytes(&reference_chunk.manifest.raw_sha256, &reference.raw_body)
    {
        return Ok(research_failure(request, generated_at, error));
    }
    let reference_artifact = match build_chunk_artifact(&reference_chunk) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };
    let (reference_artifact_id, _) = match store.publish_evidence(&reference_artifact) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };

    let mut completed_pages = checkpoint.completed_pages;
    completed_pages.push(page_ref(&reference_artifact, reference_artifact_id));

    let chunk_manifests = match load_chunk_manifests(store, &completed_pages) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };
    let manifest = match DatasetManifest::build(
        ResearchTier::TierA,
        checkpoint.instrument_id.clone(),
        Some(checkpoint.bar.clone()),
        range.clone(),
        Some(reference_window),
        &chunk_manifests,
        gaps.clone(),
        PARSER_VERSION_V1,
        NORMALIZATION_VERSION_V1,
        BUILD_SOURCE_TREE,
        utc_now_ms().to_string(),
    ) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };
    let replay_dataset = match ReplayDatasetArtifact::build(
        manifest.clone(),
        candles.clone(),
        funding.clone(),
        reference_chunk.rows.first().cloned(),
    ) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };
    let (dataset_artifact_id, _) = match store.publish_evidence(&manifest) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };
    let (replay_dataset_artifact_id, _) = match store.publish_evidence(&replay_dataset) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };
    let terminal = ResearchCheckpointTerminal {
        dataset_artifact_id: dataset_artifact_id.clone(),
        replay_dataset_artifact_id: replay_dataset_artifact_id.clone(),
    };
    let complete = match ResearchCheckpoint::build(
        parent_checkpoint_id,
        checkpoint.instrument_id.clone(),
        checkpoint.bar.clone(),
        checkpoint.target_candle_count,
        ResearchCheckpointPhase::Complete,
        completed_pages,
        None,
        Some(range.clone()),
        Some(terminal),
        BUILD_SOURCE_TREE,
        utc_now_ms().to_string(),
    ) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };
    let (checkpoint_artifact_id, _) = match store.publish_evidence(&complete) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };

    let result = ValidationDatasetPreparationResult {
        schema: RESEARCH_VALIDATION_DATASET_SCHEMA_V1,
        catalog_version: RESEARCH_CATALOG_VERSION_V1,
        stage: "3C_V1_FOUNDATION",
        state: ValidationDatasetState::Completed,
        instrument: checkpoint.instrument_id,
        bar: checkpoint.bar,
        target_candle_count: checkpoint.target_candle_count,
        candle_count: candles.len(),
        funding_event_count: funding.len(),
        phase: ResearchCheckpointPhase::Complete,
        checkpoint_id: complete.checkpoint_id,
        checkpoint_artifact_id,
        range: Some(range),
        dataset_id: Some(manifest.dataset_id),
        dataset_artifact_id: Some(dataset_artifact_id),
        replay_dataset_artifact_id: Some(replay_dataset_artifact_id),
        reference_coverage: Some(manifest.reference_coverage),
        gaps,
        continuation_required: false,
        bulk_rows_returned: false,
        source_tree: BUILD_SOURCE_TREE,
        exchange_mutation_authority: false,
    };
    response(
        request,
        generated_at,
        if result.gaps.is_empty() {
            DataQuality::Fresh
        } else {
            DataQuality::Degraded
        },
        result,
        if manifest.reference_coverage == ReferenceCoverageStatus::Complete {
            Vec::new()
        } else {
            vec![
                "INSUFFICIENT_REFERENCE_HISTORY: current instrument reference is preserved as current-only evidence; declared-counterfactual replay remains possible, historical-observed mechanics remain blocked"
                    .to_owned(),
            ]
        },
    )
}

fn checkpoint_response(
    request: &AgentRequest,
    generated_at: &str,
    store: &ResearchArtifactStore,
    checkpoint: ResearchCheckpoint,
    state: ValidationDatasetState,
    warnings: Vec<String>,
) -> AgentResult<AgentResponse> {
    let candle_count = checkpoint
        .completed_pages
        .iter()
        .filter(|page| page.kind == ResearchSourceKind::Candle)
        .map(|page| page.row_count as usize)
        .sum();
    let funding_event_count = checkpoint
        .completed_pages
        .iter()
        .filter(|page| page.kind == ResearchSourceKind::Funding)
        .map(|page| page.row_count as usize)
        .sum();
    let (checkpoint_artifact_id, _) = match store.publish_evidence(&checkpoint) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };
    let result = ValidationDatasetPreparationResult {
        schema: RESEARCH_VALIDATION_DATASET_SCHEMA_V1,
        catalog_version: RESEARCH_CATALOG_VERSION_V1,
        stage: "3C_V1_FOUNDATION",
        state,
        instrument: checkpoint.instrument_id.clone(),
        bar: checkpoint.bar.clone(),
        target_candle_count: checkpoint.target_candle_count,
        candle_count,
        funding_event_count,
        phase: checkpoint.phase,
        checkpoint_id: checkpoint.checkpoint_id,
        checkpoint_artifact_id,
        range: checkpoint.dataset_range,
        dataset_id: None,
        dataset_artifact_id: None,
        replay_dataset_artifact_id: None,
        reference_coverage: None,
        gaps: Vec::new(),
        continuation_required: state == ValidationDatasetState::ContinuationRequired,
        bulk_rows_returned: false,
        source_tree: BUILD_SOURCE_TREE,
        exchange_mutation_authority: false,
    };
    response(
        request,
        generated_at,
        if state == ValidationDatasetState::InsufficientData {
            DataQuality::Degraded
        } else {
            DataQuality::Fresh
        },
        result,
        warnings,
    )
}

fn completed_response_from_checkpoint(
    request: &AgentRequest,
    generated_at: &str,
    store: &ResearchArtifactStore,
    checkpoint: ResearchCheckpoint,
    checkpoint_artifact_id: &str,
) -> AgentResult<AgentResponse> {
    let terminal = checkpoint
        .terminal
        .as_ref()
        .expect("validated complete checkpoint has terminal");
    let dataset: ReplayDatasetArtifact =
        match store.read_evidence_json(&terminal.replay_dataset_artifact_id) {
            Ok(value) => value,
            Err(error) => return Ok(research_failure(request, generated_at, error)),
        };
    let result = ValidationDatasetPreparationResult {
        schema: RESEARCH_VALIDATION_DATASET_SCHEMA_V1,
        catalog_version: RESEARCH_CATALOG_VERSION_V1,
        stage: "3C_V1_FOUNDATION",
        state: ValidationDatasetState::Completed,
        instrument: checkpoint.instrument_id,
        bar: checkpoint.bar,
        target_candle_count: checkpoint.target_candle_count,
        candle_count: dataset.candles.len(),
        funding_event_count: dataset.funding.len(),
        phase: ResearchCheckpointPhase::Complete,
        checkpoint_id: checkpoint.checkpoint_id,
        checkpoint_artifact_id: checkpoint_artifact_id.to_owned(),
        range: Some(dataset.manifest.range.clone()),
        dataset_id: Some(dataset.manifest.dataset_id.clone()),
        dataset_artifact_id: Some(terminal.dataset_artifact_id.clone()),
        replay_dataset_artifact_id: Some(terminal.replay_dataset_artifact_id.clone()),
        reference_coverage: Some(dataset.manifest.reference_coverage),
        gaps: dataset.manifest.gaps.clone(),
        continuation_required: false,
        bulk_rows_returned: false,
        source_tree: BUILD_SOURCE_TREE,
        exchange_mutation_authority: false,
    };
    response(
        request,
        generated_at,
        if dataset.manifest.gaps.is_empty() {
            DataQuality::Fresh
        } else {
            DataQuality::Degraded
        },
        result,
        Vec::new(),
    )
}

fn response(
    request: &AgentRequest,
    generated_at: &str,
    quality: DataQuality,
    result: ValidationDatasetPreparationResult,
    warnings: Vec<String>,
) -> AgentResult<AgentResponse> {
    Ok(AgentResponse {
        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
        request_id: request.request_id.clone(),
        status: AgentResponseStatus::Completed,
        generated_at: generated_at.to_owned(),
        quality,
        result_schema: Some(RESEARCH_VALIDATION_DATASET_SCHEMA_V1.to_owned()),
        result: Some(serde_json::to_value(result)?),
        failure: None,
        warnings,
    })
}

fn page_ref<T>(artifact: &ResearchChunkArtifact<T>, artifact_id: String) -> ResearchCheckpointPage {
    ResearchCheckpointPage {
        kind: artifact.manifest.kind,
        chunk_id: artifact.manifest.chunk_id.clone(),
        artifact_id,
        row_count: artifact.manifest.normalized_row_count,
        oldest_event_time_ms: artifact.manifest.oldest_event_time_ms.clone(),
        newest_event_time_ms: artifact.manifest.newest_event_time_ms.clone(),
    }
}

fn load_candles(
    store: &ResearchArtifactStore,
    pages: &[ResearchCheckpointPage],
) -> Result<Vec<ResearchCandle>, okx_research::ResearchError> {
    let mut rows = Vec::new();
    for page in pages
        .iter()
        .filter(|page| page.kind == ResearchSourceKind::Candle)
    {
        let artifact: ResearchChunkArtifact<ResearchCandle> =
            store.read_evidence_json(&page.artifact_id)?;
        artifact.validate()?;
        if artifact.manifest.chunk_id != page.chunk_id {
            return Err(okx_research::ResearchError::ArtifactIdentityMismatch);
        }
        rows.extend(artifact.rows);
    }
    rows.sort_by_key(|row| row.open_time_ms.parse::<u64>().unwrap_or_default());
    let mut seen = BTreeSet::new();
    for row in &rows {
        if !seen.insert(row.open_time_ms.clone()) {
            return Err(okx_research::ResearchError::DuplicateTimestamp(
                row.open_time_ms.clone(),
            ));
        }
    }
    Ok(rows)
}

fn load_funding(
    store: &ResearchArtifactStore,
    pages: &[ResearchCheckpointPage],
    range: &ResearchRange,
) -> Result<Vec<ResearchFundingEvent>, okx_research::ResearchError> {
    let begin = range.begin()?;
    let end = range.end()?;
    let mut rows = Vec::new();
    for page in pages
        .iter()
        .filter(|page| page.kind == ResearchSourceKind::Funding)
    {
        let artifact: ResearchChunkArtifact<ResearchFundingEvent> =
            store.read_evidence_json(&page.artifact_id)?;
        artifact.validate()?;
        if artifact.manifest.chunk_id != page.chunk_id {
            return Err(okx_research::ResearchError::ArtifactIdentityMismatch);
        }
        rows.extend(artifact.rows.into_iter().filter(|row| {
            row.funding_time_ms
                .parse::<u64>()
                .ok()
                .is_some_and(|timestamp| timestamp >= begin && timestamp < end)
        }));
    }
    rows.sort_by_key(|row| row.funding_time_ms.parse::<u64>().unwrap_or_default());
    let mut seen = BTreeSet::new();
    for row in &rows {
        if !seen.insert(row.funding_time_ms.clone()) {
            return Err(okx_research::ResearchError::DuplicateTimestamp(
                row.funding_time_ms.clone(),
            ));
        }
    }
    Ok(rows)
}

fn load_chunk_manifests(
    store: &ResearchArtifactStore,
    pages: &[ResearchCheckpointPage],
) -> Result<Vec<okx_research::ChunkManifest>, okx_research::ResearchError> {
    let mut manifests = Vec::new();
    for page in pages {
        let bytes = store.read_evidence(&page.artifact_id)?;
        let value: serde_json::Value = serde_json::from_slice(&bytes)?;
        let manifest: okx_research::ChunkManifest = serde_json::from_value(
            value
                .get("manifest")
                .cloned()
                .ok_or(okx_research::ResearchError::ArtifactIdentityMismatch)?,
        )?;
        if manifest.chunk_id != page.chunk_id {
            return Err(okx_research::ResearchError::ArtifactIdentityMismatch);
        }
        manifests.push(manifest);
    }
    Ok(manifests)
}

fn select_latest_candles(
    mut rows: Vec<ResearchCandle>,
    target_candle_count: u16,
) -> Vec<ResearchCandle> {
    rows.sort_by_key(|row| row.open_time_ms.parse::<u64>().unwrap_or_default());
    let target = usize::from(target_candle_count);
    if rows.len() > target {
        rows.drain(0..rows.len() - target);
    }
    rows
}

fn replay_range(rows: &[ResearchCandle]) -> Result<ResearchRange, okx_research::ResearchError> {
    let first = rows
        .first()
        .ok_or(okx_research::ResearchError::MissingField(
            "validation.candles",
        ))?;
    let last = rows
        .last()
        .ok_or(okx_research::ResearchError::MissingField(
            "validation.candles",
        ))?;
    let end = last
        .open_time_ms
        .parse::<u64>()
        .map_err(|_| okx_research::ResearchError::InvalidTimestamp {
            field: "validation.last_candle_open_time_ms",
            value: last.open_time_ms.clone(),
        })?
        .checked_add(ONE_HOUR_MS)
        .ok_or_else(|| okx_research::ResearchError::InvalidTimestamp {
            field: "validation.range_end_ms",
            value: last.open_time_ms.clone(),
        })?;
    ResearchRange::new(first.open_time_ms.clone(), end.to_string())
}

fn candle_page_range(
    rows: &[okx_observation::HistoryCandle],
) -> Result<(ResearchRange, String), okx_research::ResearchError> {
    let mut times = rows
        .iter()
        .map(|row| {
            row.open_time_ms.parse::<u64>().map_err(|_| {
                okx_research::ResearchError::InvalidTimestamp {
                    field: "validation.candle_page.open_time_ms",
                    value: row.open_time_ms.clone(),
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    times.sort_unstable();
    let oldest = *times
        .first()
        .ok_or(okx_research::ResearchError::MissingField(
            "validation.candle_page",
        ))?;
    let newest = *times.last().expect("non-empty page");
    let end = newest.checked_add(ONE_HOUR_MS).ok_or_else(|| {
        okx_research::ResearchError::InvalidTimestamp {
            field: "validation.candle_page.end_ms",
            value: newest.to_string(),
        }
    })?;
    Ok((
        ResearchRange::new(oldest.to_string(), end.to_string())?,
        oldest.to_string(),
    ))
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

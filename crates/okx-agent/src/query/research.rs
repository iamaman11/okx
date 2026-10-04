use std::collections::BTreeMap;

use okx_analysis::BaselineStrategyKind;
use okx_protocol::{
    AGENT_RESPONSE_SCHEMA_V1, AgentOperation, AgentRequest, AgentResponse, AgentResponseStatus,
    DataQuality, RESEARCH_CATALOG_VERSION_V1, ResearchReplayMechanicsProvenance,
    ResearchReplayStrategy, ResearchRequest,
};
use okx_research::{
    BUILD_SOURCE_TREE, DatasetManifest, ReferenceCoverageStatus, ReplayDatasetArtifact,
    ReplayEvidenceClass, ReplayMechanicsProvenance, ReplayStatus, ResearchArtifactStore,
    ResearchRange, ResearchSourceKind, ResearchTier, SourceRequest, build_baseline_experiment,
    build_candle_chunk, build_funding_chunk, build_reference_chunk, build_tier_b_trade_chunk,
    detect_fixed_interval_gaps, replay_experiment,
};
use serde::Serialize;

use super::{
    MARKET_PUBLIC_API_UNAVAILABLE_CODE, ObservationQueryContext, failure_response, unavailable,
    utc_now_ms,
};
use crate::{AgentResult, market_bootstrap::MarketBootstrapError};

pub const RESEARCH_CAPABILITIES_SCHEMA_V1: &str = "okx.research-capabilities/v1";
pub const RESEARCH_DATA_INSPECTION_SCHEMA_V1: &str = "okx.research-data-inspection/v1";
pub const RESEARCH_TIER_B_INSPECTION_SCHEMA_V1: &str = "okx.research-tier-b-inspection/v1";
pub const RESEARCH_REPLAY_RESULT_SCHEMA_V1: &str = "okx.research-replay-summary/v1";
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
    tier_b_instruments: [&'static str; 1],
    tier_b_sources: [&'static str; 1],
    candle_page_limit_max: u16,
    funding_page_limit_max: u16,
    trade_page_limit_max: u16,
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
    capture_id: String,
    artifact_id: String,
    raw_sha256: String,
    raw_size_bytes: u64,
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
    replay_dataset_artifact_id: String,
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

#[derive(Serialize)]
struct ResearchTierBInspectionResult {
    schema: &'static str,
    catalog_version: &'static str,
    stage: &'static str,
    tier: ResearchTier,
    instrument: String,
    source: &'static str,
    source_scope: &'static str,
    acquired_at_ms: String,
    range: ResearchRange,
    chunk: ResearchChunkSummary,
    availability_semantics: &'static str,
    continuity_semantics: &'static str,
    bulk_rows_returned: bool,
    source_tree: &'static str,
    evidence_store: &'static str,
    source_cache: &'static str,
    evidence_label: &'static str,
}

#[derive(Serialize)]
struct ResearchReplaySummary {
    schema: &'static str,
    catalog_version: &'static str,
    stage: &'static str,
    instrument: String,
    replay_dataset_artifact_id: String,
    dataset_id: String,
    hypothesis_id: String,
    hypothesis_artifact_id: String,
    experiment_spec_id: String,
    experiment_spec_artifact_id: String,
    experiment_id: String,
    experiment_result_artifact_id: String,
    strategy: BaselineStrategyKind,
    mechanics_provenance: ReplayMechanicsProvenance,
    status: ReplayStatus,
    evidence_class: ReplayEvidenceClass,
    blocker: Option<&'static str>,
    signal_price_role: &'static str,
    execution_price_role: &'static str,
    candles_processed: usize,
    decision_count: usize,
    trade_count: usize,
    rejected_candidate_count: usize,
    gross_pnl_quote: String,
    trading_cost_quote: String,
    funding_cost_quote: String,
    net_pnl_quote: String,
    bulk_events_returned: bool,
    source_tree: String,
    exchange_mutation_authority: bool,
}

pub(crate) async fn dispatch(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
) -> AgentResult<AgentResponse> {
    match &request.operation {
        AgentOperation::ResearchCapabilities => capabilities(request, generated_at),
        AgentOperation::Research {
            request:
                ResearchRequest::InspectTierA {
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
        AgentOperation::Research {
            request:
                ResearchRequest::InspectTierB {
                    catalog_version: _,
                    instrument,
                    trade_limit,
                },
        } => inspect_tier_b(request, context, generated_at, instrument, *trade_limit).await,
        AgentOperation::Research {
            request:
                ResearchRequest::RunReplay {
                    catalog_version: _,
                    instrument,
                    replay_dataset_artifact_id,
                    strategy,
                    mechanics_provenance,
                },
        } => {
            run_replay(
                request,
                context,
                generated_at,
                instrument,
                replay_dataset_artifact_id,
                *strategy,
                *mechanics_provenance,
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
        stage: "3B_V1",
        tier_a_instruments: ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "DOGE-USDT-SWAP"],
        tier_a_bars: ["1H"],
        tier_b_instruments: ["BTC-USDT-SWAP"],
        tier_b_sources: ["okx_public_rest_history_trades"],
        candle_page_limit_max: 100,
        funding_page_limit_max: 400,
        trade_page_limit_max: 100,
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

    let confirmed_candles = candles
        .rows
        .iter()
        .filter(|row| row.confirmed)
        .cloned()
        .collect::<Vec<_>>();
    let Some(first) = confirmed_candles.first() else {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            MARKET_PUBLIC_API_UNAVAILABLE_CODE,
            format!("OKX returned no confirmed research candles for '{instrument}'"),
            true,
        ));
    };
    let Some(last) = confirmed_candles.last() else {
        unreachable!("first confirmed row exists");
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
        &confirmed_candles,
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

    let reference_observed_ms = match reference.acquired_at_ms.parse::<u64>() {
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
    let reference_through_ms = match reference_observed_ms.checked_add(1) {
        Some(value) => value,
        None => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                RESEARCH_ARTIFACT_FAILURE_CODE,
                "captured reference timestamp overflowed u64".to_owned(),
                false,
            ));
        }
    };
    let reference_source = SourceRequest {
        provider: "okx_public_rest".to_owned(),
        resource: "/api/v5/public/instruments".to_owned(),
        instrument_id: instrument.to_owned(),
        bar: None,
        range: ResearchRange::new(
            reference_observed_ms.to_string(),
            reference_through_ms.to_string(),
        )
        .expect("one millisecond reference capture range"),
        parameters: BTreeMap::from([("semantics".to_owned(), "current_snapshot_only".to_owned())]),
    };
    let (reference_chunk, reference_window) = match build_reference_chunk(
        reference_source,
        reference.acquired_at_ms.clone(),
        &reference.raw_body,
        &reference.instrument,
        reference_observed_ms.to_string(),
        reference_through_ms.to_string(),
        reference_observed_ms.to_string(),
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

    let replay_dataset = match ReplayDatasetArtifact::build(
        manifest.clone(),
        candle_chunk.rows.clone(),
        funding_chunk.rows.clone(),
        reference_chunk.rows.first().cloned(),
    ) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };

    let store = ResearchArtifactStore::at(research_root.join("research"));
    let persisted = (|| {
        let raw_sources = [
            (
                &candle_chunk.manifest.raw_sha256,
                candles.raw_body.as_slice(),
            ),
            (
                &funding_chunk.manifest.raw_sha256,
                funding.raw_body.as_slice(),
            ),
            (
                &reference_chunk.manifest.raw_sha256,
                reference.raw_body.as_slice(),
            ),
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
                capture_id: chunk.capture_id.clone(),
                artifact_id,
                raw_sha256: chunk.raw_sha256.clone(),
                raw_size_bytes: chunk.raw_size_bytes,
                normalized_sha256: chunk.normalized_sha256.clone(),
                normalized_row_count: chunk.normalized_row_count,
                oldest_event_time_ms: chunk.oldest_event_time_ms.clone(),
                newest_event_time_ms: chunk.newest_event_time_ms.clone(),
            });
        }
        let (dataset_artifact_id, _) = store.publish_evidence(&manifest)?;
        let (replay_dataset_artifact_id, _) = store.publish_evidence(&replay_dataset)?;
        Ok::<_, okx_research::ResearchError>((
            dataset_artifact_id,
            replay_dataset_artifact_id,
            chunks,
        ))
    })();
    let (dataset_artifact_id, replay_dataset_artifact_id, chunks) = match persisted {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };

    let strategy_ready = manifest.reference_coverage == ReferenceCoverageStatus::Complete
        && manifest.gaps.is_empty();
    let blocker = (!strategy_ready).then_some(
        if manifest.reference_coverage != ReferenceCoverageStatus::Complete {
            INSUFFICIENT_REFERENCE_HISTORY_CODE
        } else {
            "INSUFFICIENT_DATA"
        },
    );
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
        replay_dataset_artifact_id,
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

async fn run_replay(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    instrument: &str,
    replay_dataset_artifact_id: &str,
    strategy: ResearchReplayStrategy,
    mechanics_provenance: ResearchReplayMechanicsProvenance,
) -> AgentResult<AgentResponse> {
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
    let dataset: ReplayDatasetArtifact = match store.read_evidence_json(replay_dataset_artifact_id)
    {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };
    if dataset.manifest.instrument_id != instrument {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            RESEARCH_ARTIFACT_FAILURE_CODE,
            "replay artifact instrument does not match request".to_owned(),
            false,
        ));
    }
    let strategy = match strategy {
        ResearchReplayStrategy::NoTrade => BaselineStrategyKind::NoTrade,
        ResearchReplayStrategy::CloseMomentum => BaselineStrategyKind::CloseMomentum,
    };
    let mechanics_provenance = match mechanics_provenance {
        ResearchReplayMechanicsProvenance::DeclaredCounterfactual => {
            ReplayMechanicsProvenance::DeclaredCounterfactual
        }
        ResearchReplayMechanicsProvenance::HistoricalObserved => {
            ReplayMechanicsProvenance::HistoricalObserved
        }
    };
    let (hypothesis, spec) =
        match build_baseline_experiment(&dataset, strategy, mechanics_provenance) {
            Ok(value) => value,
            Err(error) => return Ok(research_failure(request, generated_at, error)),
        };

    let result =
        match replay_experiment(&dataset.manifest, &dataset.candles, &dataset.funding, &spec) {
            Ok(value) => value,
            Err(error) => return Ok(research_failure(request, generated_at, error)),
        };

    let persisted = (|| {
        let (hypothesis_artifact_id, _) = store.publish_evidence(&hypothesis)?;
        let (experiment_spec_artifact_id, _) = store.publish_evidence(&spec)?;
        let (experiment_result_artifact_id, _) = store.publish_evidence(&result)?;
        Ok::<_, okx_research::ResearchError>((
            hypothesis_artifact_id,
            experiment_spec_artifact_id,
            experiment_result_artifact_id,
        ))
    })();
    let (hypothesis_artifact_id, experiment_spec_artifact_id, experiment_result_artifact_id) =
        match persisted {
            Ok(value) => value,
            Err(error) => return Ok(research_failure(request, generated_at, error)),
        };

    let quality = if result.status == ReplayStatus::Completed {
        DataQuality::Fresh
    } else {
        DataQuality::Degraded
    };
    let mut warnings = Vec::new();
    if result.evidence_class == ReplayEvidenceClass::CounterfactualMechanics {
        warnings.push(
            "COUNTERFACTUAL_MECHANICS: current instrument mechanics and declared 5 bps taker fees are replay assumptions, not historical reference/fee truth"
                .to_owned(),
        );
    }
    if let Some(blocker) = result.blocker {
        warnings.push(blocker.to_owned());
    }

    let summary = ResearchReplaySummary {
        schema: RESEARCH_REPLAY_RESULT_SCHEMA_V1,
        catalog_version: RESEARCH_CATALOG_VERSION_V1,
        stage: "3B_V1",
        instrument: instrument.to_owned(),
        replay_dataset_artifact_id: replay_dataset_artifact_id.to_owned(),
        dataset_id: result.dataset_id.clone(),
        hypothesis_id: result.hypothesis_id.clone(),
        hypothesis_artifact_id,
        experiment_spec_id: result.experiment_spec_id.clone(),
        experiment_spec_artifact_id,
        experiment_id: result.experiment_id.clone(),
        experiment_result_artifact_id,
        strategy,
        mechanics_provenance,
        status: result.status,
        evidence_class: result.evidence_class,
        blocker: result.blocker,
        signal_price_role: result.signal_price_role,
        execution_price_role: result.execution_price_role,
        candles_processed: result.candles_processed,
        decision_count: result.decision_count,
        trade_count: result.trade_count,
        rejected_candidate_count: result.rejected_candidate_count,
        gross_pnl_quote: result.gross_pnl_quote,
        trading_cost_quote: result.trading_cost_quote,
        funding_cost_quote: result.funding_cost_quote,
        net_pnl_quote: result.net_pnl_quote,
        bulk_events_returned: false,
        source_tree: result.source_tree,
        exchange_mutation_authority: false,
    };

    Ok(AgentResponse {
        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
        request_id: request.request_id.clone(),
        status: AgentResponseStatus::Completed,
        generated_at: generated_at.to_owned(),
        quality,
        result_schema: Some(RESEARCH_REPLAY_RESULT_SCHEMA_V1.to_owned()),
        result: Some(serde_json::to_value(summary)?),
        failure: None,
        warnings,
    })
}

async fn inspect_tier_b(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    instrument: &str,
    trade_limit: u16,
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

    let trades = match market
        .research_trades_page(instrument, None, None, trade_limit)
        .await
    {
        Ok(value) => value,
        Err(error) => return Ok(source_failure(request, generated_at, error)),
    };
    if trades.rows.len() < 2 {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            MARKET_PUBLIC_API_UNAVAILABLE_CODE,
            format!("OKX returned fewer than two historical trades for '{instrument}'"),
            true,
        ));
    }

    let first = trades.rows.first().expect("two or more historical trades");
    let last = trades.rows.last().expect("two or more historical trades");
    let range_end = match last
        .exchange_timestamp_ms
        .parse::<u64>()
        .ok()
        .and_then(|value| value.checked_add(1))
    {
        Some(value) => value.to_string(),
        None => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                RESEARCH_ARTIFACT_FAILURE_CODE,
                "Tier B trade range overflowed u64".to_owned(),
                false,
            ));
        }
    };
    let range = match ResearchRange::new(first.exchange_timestamp_ms.clone(), range_end) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };

    let source = SourceRequest {
        provider: "okx_public_rest".to_owned(),
        resource: "/api/v5/market/history-trades".to_owned(),
        instrument_id: instrument.to_owned(),
        bar: None,
        range: range.clone(),
        parameters: BTreeMap::from([
            ("limit".to_owned(), trade_limit.to_string()),
            ("pagination".to_owned(), "trade_id".to_owned()),
            ("type".to_owned(), "1".to_owned()),
        ]),
    };
    let chunk = match build_tier_b_trade_chunk(
        source,
        trades.acquired_at_ms.clone(),
        &trades.raw_body,
        &trades.rows,
        PARSER_VERSION_V1,
        NORMALIZATION_VERSION_V1,
        BUILD_SOURCE_TREE,
    ) {
        Ok(value) => value,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };

    let store = ResearchArtifactStore::at(research_root.join("research"));
    if let Err(error) = store.publish_source_bytes(&chunk.manifest.raw_sha256, &trades.raw_body) {
        return Ok(research_failure(request, generated_at, error));
    }
    let artifact_id = match store.publish_evidence(&chunk.manifest) {
        Ok((id, _)) => id,
        Err(error) => return Ok(research_failure(request, generated_at, error)),
    };

    let result = ResearchTierBInspectionResult {
        schema: RESEARCH_TIER_B_INSPECTION_SCHEMA_V1,
        catalog_version: RESEARCH_CATALOG_VERSION_V1,
        stage: "3A_V1",
        tier: ResearchTier::TierB,
        instrument: instrument.to_owned(),
        source: "/api/v5/market/history-trades",
        source_scope: "OKX historical trades; bounded latest page within the endpoint retention window",
        acquired_at_ms: trades.acquired_at_ms,
        range,
        chunk: ResearchChunkSummary {
            kind: chunk.manifest.kind,
            chunk_id: chunk.manifest.chunk_id,
            capture_id: chunk.manifest.capture_id,
            artifact_id,
            raw_sha256: chunk.manifest.raw_sha256,
            raw_size_bytes: chunk.manifest.raw_size_bytes,
            normalized_sha256: chunk.manifest.normalized_sha256,
            normalized_row_count: chunk.manifest.normalized_row_count,
            oldest_event_time_ms: chunk.manifest.oldest_event_time_ms,
            newest_event_time_ms: chunk.manifest.newest_event_time_ms,
        },
        availability_semantics: "exchange event time is a lower bound; retrospective research acquisition is recorded separately",
        continuity_semantics: "trade ids must be unique; this bounded page does not infer complete tick continuity outside returned events",
        bulk_rows_returned: false,
        source_tree: BUILD_SOURCE_TREE,
        evidence_store: "PINNED_CONTENT_ADDRESSED",
        source_cache: "CONTENT_ADDRESSED_REDOWNLOADABLE",
        evidence_label: "OBSERVED_OKX_HISTORICAL_TRADE_SOURCE",
    };

    Ok(AgentResponse {
        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
        request_id: request.request_id.clone(),
        status: AgentResponseStatus::Completed,
        generated_at: generated_at.to_owned(),
        quality: DataQuality::Fresh,
        result_schema: Some(RESEARCH_TIER_B_INSPECTION_SCHEMA_V1.to_owned()),
        result: Some(serde_json::to_value(result)?),
        failure: None,
        warnings: Vec::new(),
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

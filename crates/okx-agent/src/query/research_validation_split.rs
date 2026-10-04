use okx_analysis::BaselineStrategyKind;
use okx_protocol::{
    AGENT_RESPONSE_SCHEMA_V1, AgentRequest, AgentResponse, AgentResponseStatus, DataQuality,
    RESEARCH_CATALOG_VERSION_V1, ResearchReplayStrategy,
};
use okx_research::{
    BUILD_SOURCE_TREE, DerivedValidationSlice, ReplayDatasetArtifact, ResearchArtifactStore,
    ValidationPartitionRole, ValidationSpec, build_validation_spec_from_counts,
    derive_validation_slice,
};
use serde::Serialize;

use super::{ObservationQueryContext, failure_response};
use crate::AgentResult;

pub const RESEARCH_VALIDATION_SPLIT_SCHEMA_V1: &str = "okx.research-validation-split/v1";

#[derive(Debug, Serialize)]
struct ValidationSliceSummary {
    role: ValidationPartitionRole,
    declared_range: okx_research::ResearchRange,
    effective_range: okx_research::ResearchRange,
    candle_count: usize,
    funding_event_count: usize,
    dataset_id: String,
    replay_dataset_artifact_id: String,
}

#[derive(Debug, Serialize)]
struct ValidationSplitPreparationResult {
    schema: &'static str,
    catalog_version: &'static str,
    stage: &'static str,
    instrument: String,
    parent_replay_dataset_artifact_id: String,
    parent_dataset_id: String,
    validation_spec_id: String,
    validation_spec_artifact_id: String,
    strategy: BaselineStrategyKind,
    strategy_version: String,
    purge_bars: u16,
    embargo_bars: u16,
    slices: Vec<ValidationSliceSummary>,
    final_oos_sealed: bool,
    final_oos_consumed: bool,
    bulk_rows_returned: bool,
    source_tree: &'static str,
    exchange_mutation_authority: bool,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn prepare_validation_split(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    instrument: &str,
    parent_replay_dataset_artifact_id: &str,
    strategy: ResearchReplayStrategy,
    train_candle_count: u16,
    validation_candle_count: u16,
    final_oos_candle_count: u16,
) -> AgentResult<AgentResponse> {
    let Some(research_root) = context.research_root else {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            super::research::RESEARCH_ARTIFACT_FAILURE_CODE,
            "research artifact root is unavailable".to_owned(),
            false,
        ));
    };
    if BUILD_SOURCE_TREE == "UNAVAILABLE" {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            super::research::RESEARCH_ARTIFACT_FAILURE_CODE,
            "research source tree is not bound into this build".to_owned(),
            false,
        ));
    }

    let store = ResearchArtifactStore::at(research_root.join("research"));
    let parent: ReplayDatasetArtifact =
        match store.read_evidence_json(parent_replay_dataset_artifact_id) {
            Ok(value) => value,
            Err(error) => {
                return Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Failed,
                    super::research::RESEARCH_ARTIFACT_FAILURE_CODE,
                    error.to_string(),
                    false,
                ));
            }
        };
    if parent.manifest.instrument_id != instrument {
        return Ok(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            super::research::RESEARCH_ARTIFACT_FAILURE_CODE,
            "validation parent artifact instrument does not match request".to_owned(),
            false,
        ));
    }

    let strategy = match strategy {
        ResearchReplayStrategy::NoTrade => BaselineStrategyKind::NoTrade,
        ResearchReplayStrategy::CloseMomentum => BaselineStrategyKind::CloseMomentum,
    };
    let spec = match build_validation_spec_from_counts(
        parent_replay_dataset_artifact_id,
        &parent,
        strategy,
        train_candle_count,
        validation_candle_count,
        final_oos_candle_count,
    ) {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                super::research::RESEARCH_ARTIFACT_FAILURE_CODE,
                error.to_string(),
                false,
            ));
        }
    };

    let (validation_spec_artifact_id, _) = match store.publish_evidence(&spec) {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                super::research::RESEARCH_ARTIFACT_FAILURE_CODE,
                error.to_string(),
                false,
            ));
        }
    };

    let mut slices = Vec::with_capacity(3);
    for role in [
        ValidationPartitionRole::Train,
        ValidationPartitionRole::Validation,
        ValidationPartitionRole::FinalOos,
    ] {
        let derived: DerivedValidationSlice = match derive_validation_slice(&parent, &spec, role) {
            Ok(value) => value,
            Err(error) => {
                return Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Failed,
                    super::research::RESEARCH_ARTIFACT_FAILURE_CODE,
                    error.to_string(),
                    false,
                ));
            }
        };
        let (replay_dataset_artifact_id, _) =
            match store.publish_evidence(&derived.replay_dataset) {
                Ok(value) => value,
                Err(error) => {
                    return Ok(failure_response(
                        request,
                        generated_at,
                        AgentResponseStatus::Failed,
                        super::research::RESEARCH_ARTIFACT_FAILURE_CODE,
                        error.to_string(),
                        false,
                    ));
                }
            };
        slices.push(slice_summary(derived, replay_dataset_artifact_id));
    }

    let result = ValidationSplitPreparationResult {
        schema: RESEARCH_VALIDATION_SPLIT_SCHEMA_V1,
        catalog_version: RESEARCH_CATALOG_VERSION_V1,
        stage: "3C_V1_SPLIT",
        instrument: instrument.to_owned(),
        parent_replay_dataset_artifact_id: parent_replay_dataset_artifact_id.to_owned(),
        parent_dataset_id: parent.manifest.dataset_id,
        validation_spec_id: spec.validation_spec_id,
        validation_spec_artifact_id,
        strategy,
        strategy_version: spec.strategy_version,
        purge_bars: spec.purge_bars,
        embargo_bars: spec.embargo_bars,
        slices,
        final_oos_sealed: true,
        final_oos_consumed: false,
        bulk_rows_returned: false,
        source_tree: BUILD_SOURCE_TREE,
        exchange_mutation_authority: false,
    };

    Ok(AgentResponse {
        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
        request_id: request.request_id.clone(),
        status: AgentResponseStatus::Completed,
        generated_at: generated_at.to_owned(),
        quality: DataQuality::Fresh,
        result_schema: Some(RESEARCH_VALIDATION_SPLIT_SCHEMA_V1.to_owned()),
        result: Some(serde_json::to_value(result)?),
        failure: None,
        warnings: Vec::new(),
    })
}

fn slice_summary(
    derived: DerivedValidationSlice,
    replay_dataset_artifact_id: String,
) -> ValidationSliceSummary {
    ValidationSliceSummary {
        role: derived.role,
        declared_range: derived.declared_range,
        effective_range: derived.effective_range,
        candle_count: derived.replay_dataset.candles.len(),
        funding_event_count: derived.replay_dataset.funding.len(),
        dataset_id: derived.replay_dataset.manifest.dataset_id,
        replay_dataset_artifact_id,
    }
}

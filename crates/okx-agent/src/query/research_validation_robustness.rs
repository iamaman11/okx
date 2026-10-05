use okx_protocol::{
    AGENT_RESPONSE_SCHEMA_V1, AgentRequest, AgentResponse, AgentResponseStatus, DataQuality,
    RESEARCH_CATALOG_VERSION_V1,
};
use okx_research::{
    BUILD_SOURCE_TREE, ResearchArtifactStore, ValidationEvidenceReadiness,
    prepare_validation_robustness,
};
use serde::Serialize;

use super::{ObservationQueryContext, failure_response};
use crate::AgentResult;

pub const RESEARCH_VALIDATION_ROBUSTNESS_SCHEMA_V1: &str =
    "okx.research-validation-robustness/v1";

#[derive(Debug, Serialize)]
struct ValidationRobustnessResult {
    schema: &'static str,
    catalog_version: &'static str,
    stage: &'static str,
    instrument: String,
    robustness_artifact_id: String,
    evidence: okx_research::ValidationRobustnessEvidence,
    bulk_events_returned: bool,
    exchange_mutation_authority: bool,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn evaluate_validation_robustness(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    instrument: &str,
    validation_spec_artifact_id: &str,
    pre_holdout_evidence_artifact_id: &str,
    train_replay_dataset_artifact_id: &str,
    validation_replay_dataset_artifact_id: &str,
    validation_experiment_result_artifact_id: &str,
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
    let prepared = match prepare_validation_robustness(
        &store,
        instrument,
        validation_spec_artifact_id,
        pre_holdout_evidence_artifact_id,
        train_replay_dataset_artifact_id,
        validation_replay_dataset_artifact_id,
        validation_experiment_result_artifact_id,
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
    let warnings = prepared
        .evidence
        .blockers
        .iter()
        .map(|blocker| format!("VALIDATION_ROBUSTNESS_BLOCKER: {blocker}"))
        .collect::<Vec<_>>();
    let result = ValidationRobustnessResult {
        schema: RESEARCH_VALIDATION_ROBUSTNESS_SCHEMA_V1,
        catalog_version: RESEARCH_CATALOG_VERSION_V1,
        stage: "3C_V1_ROBUSTNESS",
        instrument: instrument.to_owned(),
        robustness_artifact_id: prepared.evidence_artifact_id,
        evidence: prepared.evidence,
        bulk_events_returned: false,
        exchange_mutation_authority: false,
    };

    Ok(AgentResponse {
        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
        request_id: request.request_id.clone(),
        status: AgentResponseStatus::Completed,
        generated_at: generated_at.to_owned(),
        quality: DataQuality::Fresh,
        result_schema: Some(RESEARCH_VALIDATION_ROBUSTNESS_SCHEMA_V1.to_owned()),
        result: Some(serde_json::to_value(result)?),
        failure: None,
        warnings,
    })
}

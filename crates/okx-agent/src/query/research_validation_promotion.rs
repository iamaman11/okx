use okx_protocol::{
    AGENT_RESPONSE_SCHEMA_V1, AgentRequest, AgentResponse, AgentResponseStatus, DataQuality,
    RESEARCH_CATALOG_VERSION_V1,
};
use okx_research::{BUILD_SOURCE_TREE, ResearchArtifactStore};
use serde::Serialize;

use super::{ObservationQueryContext, failure_response};
use crate::AgentResult;

pub const RESEARCH_VALIDATION_PROMOTION_SCHEMA_V1: &str = "okx.research-validation-promotion/v1";

#[derive(Debug, Serialize)]
struct ValidationPromotionResult {
    schema: &'static str,
    catalog_version: &'static str,
    stage: &'static str,
    instrument: String,
    consumption_intent_artifact_id: String,
    promotion_bundle_artifact_id: String,
    bundle: okx_research::PromotionBundle,
    bulk_events_returned: bool,
    exchange_mutation_authority: bool,
}

pub(super) fn consume_final_oos(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    instrument: &str,
    validation_spec_artifact_id: &str,
    validation_robustness_artifact_id: &str,
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
    let prepared = match okx_research::consume_final_oos(
        &store,
        instrument,
        validation_spec_artifact_id,
        validation_robustness_artifact_id,
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
        .bundle
        .decision_blockers
        .iter()
        .map(|blocker| format!("VALIDATION_PROMOTION_BLOCKER: {blocker}"))
        .collect::<Vec<_>>();
    let result = ValidationPromotionResult {
        schema: RESEARCH_VALIDATION_PROMOTION_SCHEMA_V1,
        catalog_version: RESEARCH_CATALOG_VERSION_V1,
        stage: "3C_V1_FINAL_GATE",
        instrument: instrument.to_owned(),
        consumption_intent_artifact_id: prepared.consumption_intent_artifact_id,
        promotion_bundle_artifact_id: prepared.bundle_artifact_id,
        bundle: prepared.bundle,
        bulk_events_returned: false,
        exchange_mutation_authority: false,
    };

    Ok(AgentResponse {
        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
        request_id: request.request_id.clone(),
        status: AgentResponseStatus::Completed,
        generated_at: generated_at.to_owned(),
        quality: DataQuality::Fresh,
        result_schema: Some(RESEARCH_VALIDATION_PROMOTION_SCHEMA_V1.to_owned()),
        result: Some(serde_json::to_value(result)?),
        failure: None,
        warnings,
    })
}

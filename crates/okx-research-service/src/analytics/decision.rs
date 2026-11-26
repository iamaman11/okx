//! Immutable researcher annotations. The existing catalog owns only the head;
//! result artifacts and their scientific evidence are never changed.
use super::{
    AnalyticsService, jobs,
    result::{read_verified_result, verify_export_manifest},
};
use okx_research_protocol::analytics::{ResearchDecisionStatus, ResearchRequest};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const MAX_REVISIONS: u64 = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Decision {
    schema: String,
    job_id: String,
    result_artifact: String,
    experiment_artifact: String,
    previous_decision_artifact: Option<String>,
    revision: u64,
    created_ms: u64,
    name: String,
    hypothesis: String,
    status: ResearchDecisionStatus,
    reason: String,
}

impl AnalyticsService {
    pub(super) fn research_decision(&self, request: ResearchRequest) -> Result<Value, String> {
        // Keep this boundary independently safe for direct callers as well as
        // the public dispatcher. Metadata uses the existing bounded artifact I/O executor, never submit/source/trade.
        if !request.valid() {
            return Err("INVALID_RESEARCH_REQUEST".into());
        }
        match request {
            ResearchRequest::RecordDecision {
                job_id,
                result_id,
                expected_decision_id,
                name,
                hypothesis,
                status,
                reason,
            } => {
                let job = self.jobs.get(&job_id)?;
                jobs::Jobs::require_decision_result(&job)?;
                if job.artifact.as_deref() != Some(&result_id) {
                    return Err("DECISION_RESULT_MISMATCH".into());
                }
                let experiment = self.decision_provenance(&job)?;
                let current = job
                    .decision_artifact
                    .as_deref()
                    .map(|head| self.read_decision(head, &job, &experiment))
                    .transpose()?;
                let same_payload = |value: &Decision| {
                    value.name == name
                        && value.hypothesis == hypothesis
                        && value.status == status
                        && value.reason == reason
                };
                let proposed = if current.as_ref().is_some_and(&same_payload) {
                    // Retry after a lost reply preserves server time and ID.
                    job.decision_artifact.clone().expect("current head")
                } else {
                    if expected_decision_id != job.decision_artifact {
                        return Err("DECISION_REVISION_CONFLICT".into());
                    }
                    let revision = current.as_ref().map_or(1, |value| value.revision + 1);
                    if revision > MAX_REVISIONS {
                        return Err("DECISION_REVISION_LIMIT:256".into());
                    }
                    let record = Decision {
                        schema: "okx.research.decision/v1".into(),
                        job_id: job_id.clone(),
                        result_artifact: result_id.clone(),
                        experiment_artifact: experiment.clone(),
                        previous_decision_artifact: job.decision_artifact.clone(),
                        revision,
                        created_ms: jiff::Timestamp::now().as_millisecond().max(0) as u64,
                        name: name.clone(),
                        hypothesis: hypothesis.clone(),
                        status,
                        reason: reason.clone(),
                    };
                    self.artifacts
                        .publish_evidence(&record)
                        .map(|(id, _)| id)
                        .map_err(|e| e.to_string())?
                };
                let decision_id = match self.jobs.commit_decision(
                    &job_id,
                    &result_id,
                    job.decision_artifact.as_deref(),
                    &proposed,
                ) {
                    Ok(()) => proposed,
                    Err(error) if error == "DECISION_REVISION_CONFLICT" => {
                        // Concurrent identical submissions converge without doing
                        // any disk I/O while the SQL owner is locked.
                        let latest = self.jobs.get(&job_id)?;
                        jobs::Jobs::require_decision_result(&latest)?;
                        let id = latest
                            .decision_artifact
                            .as_deref()
                            .ok_or("DECISION_REVISION_CONFLICT")?;
                        let record = self.read_decision(id, &latest, &experiment)?;
                        if !same_payload(&record) {
                            return Err(error);
                        }
                        id.to_owned()
                    }
                    Err(error) => return Err(error),
                };
                self.research_decision(ResearchRequest::Decision {
                    job_id,
                    decision_id: Some(decision_id),
                })
            }
            ResearchRequest::Decision {
                job_id,
                decision_id,
            } => {
                let job = self.jobs.get(&job_id)?;
                jobs::Jobs::require_decision_result(&job)?;
                let experiment = self.decision_provenance(&job)?;
                let head = job
                    .decision_artifact
                    .as_deref()
                    .ok_or("DECISION_NOT_FOUND")?;
                let target = decision_id.as_deref().unwrap_or(head);
                let mut next = Some(head.to_owned());
                let mut expected_revision = None;
                for _ in 0..MAX_REVISIONS {
                    let Some(id) = next else {
                        break;
                    };
                    let record = self.read_decision(&id, &job, &experiment)?;
                    if expected_revision.is_some_and(|expected| record.revision != expected) {
                        return Err("DECISION_CHAIN_INVALID".into());
                    }
                    if id == target {
                        let inputs: Value = read_verified_result(&self.artifacts, &experiment)?;
                        let mut response = json!({"decision_id":id,"decision":record,"experiment":inputs,"side_effects":"research_metadata_only","trading_authority":false});
                        if serde_json::to_vec(&response)
                            .map_err(|e| e.to_string())?
                            .len()
                            > 10240
                        {
                            response
                                .as_object_mut()
                                .expect("response object")
                                .remove("experiment");
                            response["experiment_body_omitted"] =
                                json!("RESPONSE_BUDGET_USE_IMMUTABLE_EXPERIMENT_REFERENCE");
                        }
                        if serde_json::to_vec(&response)
                            .map_err(|e| e.to_string())?
                            .len()
                            > 10240
                        {
                            return Err("DECISION_RESPONSE_LIMIT".into());
                        }
                        return Ok(response);
                    }
                    expected_revision = record.revision.checked_sub(1);
                    next = record.previous_decision_artifact;
                }
                Err("DECISION_NOT_FOUND".into())
            }
            _ => Err("INVALID_RESEARCH_REQUEST".into()),
        }
    }

    fn decision_provenance(&self, job: &jobs::Job) -> Result<String, String> {
        let artifact = job.artifact.as_deref().ok_or("RESULT_NOT_READY")?;
        let manifest: Value = read_verified_result(&self.artifacts, artifact)?;
        let experiment = manifest
            .get("experiment_artifact")
            .and_then(Value::as_str)
            .ok_or("DECISION_PROVENANCE_UNAVAILABLE")?;
        // Reuse the result owner's complete page-count and SHA verification,
        // including experiment↔dataset/code binding. No new export verifier.
        verify_export_manifest(&self.artifacts, &manifest)?;
        let dataset = manifest
            .get("dataset_artifact")
            .and_then(Value::as_str)
            .ok_or("DECISION_PROVENANCE_UNAVAILABLE")?;
        let dataset_manifest: Value = read_verified_result(&self.artifacts, dataset)?;
        let chunks = dataset_manifest
            .get("chunks")
            .and_then(Value::as_array)
            .ok_or("DECISION_PROVENANCE_UNAVAILABLE")?;
        if chunks.len() > 245 {
            return Err("DECISION_DATASET_LIMIT".into());
        }
        for chunk in chunks {
            let id = chunk.as_str().ok_or("DECISION_PROVENANCE_UNAVAILABLE")?;
            let _: Vec<Value> = read_verified_result(&self.artifacts, id)?;
        }
        Ok(experiment.to_owned())
    }

    fn read_decision(
        &self,
        id: &str,
        job: &jobs::Job,
        experiment: &str,
    ) -> Result<Decision, String> {
        let record: Decision = read_verified_result(&self.artifacts, id)?;
        if record.schema != "okx.research.decision/v1"
            || record.job_id != job.id
            || Some(&record.result_artifact) != job.artifact.as_ref()
            || record.experiment_artifact != experiment
            || record.revision == 0
            || record.revision > MAX_REVISIONS
            || (record.revision == 1) != record.previous_decision_artifact.is_none()
        {
            return Err("DECISION_PROVENANCE_MISMATCH".into());
        }
        Ok(record)
    }
}

#[cfg(test)]
#[path = "tests/decision.rs"]
mod tests;

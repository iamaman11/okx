//! Immutable researcher annotations. The existing catalog owns only the head;
//! result artifacts and their scientific evidence are never changed.
use super::{
    AnalyticsService, jobs,
    result::{read_verified_result, verify_export_manifest},
};
use okx_protocol::analytics::{ResearchDecisionStatus, ResearchRequest};
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
                        created_ms: chrono::Utc::now().timestamp_millis().max(0) as u64,
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
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    fn fixture(service: &AnalyticsService, digit: char) -> (String, String) {
        let dataset = service
            .artifacts
            .publish_evidence(&json!({"chunks":[]}))
            .unwrap()
            .0;
        let experiment = service
            .artifacts
            .publish_evidence(&json!({
                "schema":"okx.research.experiment-inputs/v1", "dataset_artifact":dataset,
                "code_version":"decision-fixture", "spec":{"seed":7,"costs":{"taker_fee_bps":2}},
                "variants":[{"fast":2,"slow":5}]
            }))
            .unwrap()
            .0;
        let result = service.artifacts.publish_evidence(&json!({
            "trade_pages":[],"total_trades":0,"equity_curve_pages":[],"total_equity_points":0,
            "dataset_artifact":dataset,"experiment_artifact":experiment,"code_version":"decision-fixture"
        })).unwrap().0;
        let job = digit.to_string().repeat(64);
        service
            .jobs
            .enqueue(format!("decision-{digit}"), job.clone())
            .unwrap();
        service
            .jobs
            .update(&job, jobs::State::Completed, Some(result.clone()), None)
            .unwrap();
        (job, result)
    }

    fn record(
        job: &str,
        result: &str,
        previous: Option<&str>,
        hypothesis: &str,
    ) -> ResearchRequest {
        ResearchRequest::RecordDecision {
            job_id: job.into(),
            result_id: result.into(),
            expected_decision_id: previous.map(str::to_owned),
            name: "Experiment A".into(),
            hypothesis: hypothesis.into(),
            status: ResearchDecisionStatus::Deferred,
            reason: "Await independent evidence".into(),
        }
    }
    fn read(job: &str, decision: Option<&str>) -> ResearchRequest {
        ResearchRequest::Decision {
            job_id: job.into(),
            decision_id: decision.map(str::to_owned),
        }
    }

    #[tokio::test]
    async fn service_decision_replay_revision_and_real_catalog_reopen_preserve_original_bytes() {
        let (root, service) = super::super::artifact_job_fixtures::service();
        let (job, result) = fixture(&service, 'a');
        let original_result = service.artifacts.read_evidence(&result).unwrap();
        // Closing the heavy admission paths makes accidental submit fail. The
        // metadata branch has no source loader, replay admission or TradeService.
        service.heavy.close();
        service.research_memory.close();
        let first_request = record(&job, &result, None, "Original hypothesis");
        let first = service.research(first_request.clone()).await.unwrap();
        let first_id = first["decision_id"].as_str().unwrap().to_owned();
        let original_decision = service.artifacts.read_evidence(&first_id).unwrap();
        assert_eq!(first["experiment"]["spec"]["seed"], 7);
        assert_eq!(first["experiment"]["spec"]["costs"]["taker_fee_bps"], 2);
        assert_eq!(first["trading_authority"], false);
        assert_eq!(service.research(first_request).await.unwrap(), first);
        let second = service
            .research(record(&job, &result, Some(&first_id), "Revised hypothesis"))
            .await
            .unwrap();
        assert_eq!(second["decision"]["revision"], 2);
        assert_eq!(second["decision"]["previous_decision_artifact"], first_id);
        assert_eq!(
            service
                .research(record(
                    &job,
                    &result,
                    Some(&first_id),
                    "Stale conflicting hypothesis"
                ))
                .await
                .unwrap_err(),
            "DECISION_REVISION_CONFLICT"
        );
        assert_eq!(
            service.research(read(&job, Some(&first_id))).await.unwrap(),
            first
        );
        assert_eq!(
            service.artifacts.read_evidence(&result).unwrap(),
            original_result
        );
        assert_eq!(
            service.artifacts.read_evidence(&first_id).unwrap(),
            original_decision
        );
        assert_eq!(service.jobs.snapshot().len(), 1);
        let bootstrap = service.sources.bootstrap.clone();
        drop(service);
        let reopened = AnalyticsService::new(root.clone(), bootstrap).unwrap();
        assert_eq!(reopened.research(read(&job, None)).await.unwrap(), second);
        assert_eq!(
            reopened
                .research(read(&job, Some(&first_id)))
                .await
                .unwrap(),
            first
        );
        drop(reopened);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn concurrent_decision_cas_commits_only_one_revision_and_replay_keeps_id() {
        let (root, service) = super::super::artifact_job_fixtures::service();
        let (job, result) = fixture(&service, 'b');
        let barrier = Arc::new(Barrier::new(3));
        let handles = ["Hypothesis one", "Hypothesis two"].map(|hypothesis| {
            let service = service.clone();
            let job = job.clone();
            let result = result.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                service.research_decision(record(&job, &result, None, hypothesis))
            })
        });
        barrier.wait();
        let outcomes = handles.map(|handle| handle.join().unwrap());
        assert_eq!(outcomes.iter().filter(|value| value.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|value| value
                    .as_ref()
                    .is_err_and(|e| e == "DECISION_REVISION_CONFLICT"))
                .count(),
            1
        );
        let winner = outcomes.into_iter().find_map(Result::ok).unwrap();
        assert_eq!(winner["decision"]["revision"], 1);
        let request = record(
            &job,
            &result,
            None,
            winner["decision"]["hypothesis"].as_str().unwrap(),
        );
        assert_eq!(service.research_decision(request).unwrap(), winner);
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn simultaneous_identical_decisions_return_one_id_and_first_revision() {
        let (root, service) = super::super::artifact_job_fixtures::service();
        let (job, result) = fixture(&service, 'b');
        let barrier = Arc::new(Barrier::new(3));
        let handles = [0, 1].map(|_| {
            let service = service.clone();
            let request = record(&job, &result, None, "Identical hypothesis");
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                service.research_decision(request).unwrap()
            })
        });
        barrier.wait();
        let [first, second] = handles.map(|handle| handle.join().unwrap());
        assert_eq!(first, second);
        assert_eq!(first["decision"]["revision"], 1);
        assert!(first["decision"]["previous_decision_artifact"].is_null());
        assert_eq!(service.research_decision(read(&job, None)).unwrap(), first);
        assert_eq!(service.jobs.snapshot().len(), 1);
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn query_dispatch_records_and_reads_decisions_with_scope_and_zero_external_effects() {
        use okx_api::{HttpSend, HttpTransport, OkxEnvironment, RateBudget};
        use okx_protocol::{
            AGENT_REQUEST_SCHEMA_V1, AgentOperation, AgentRequest, AgentResponseStatus,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct ForbiddenNetwork(AtomicUsize);
        impl HttpTransport for ForbiddenNetwork {
            fn send(&self, _: okx_api::HttpRequest, _: std::time::Instant) -> HttpSend<'_> {
                self.0.fetch_add(1, Ordering::Relaxed);
                Box::pin(async {
                    Err(okx_api::OkxError::Response(
                        "RESEARCH_NETWORK_FORBIDDEN".into(),
                    ))
                })
            }
        }
        let (root, mut service) = super::super::artifact_job_fixtures::service();
        let (job, result) = fixture(&service, 'a');
        let (other_job, _) = fixture(&service, 'b');
        let result_bytes = service.artifacts.read_evidence(&result).unwrap();
        let network = Arc::new(ForbiddenNetwork(AtomicUsize::new(0)));
        let environment = OkxEnvironment::new(okx_api::Region::Global, false);
        let market = crate::market_bootstrap::MarketBootstrapper::new(
            okx_api::OkxPublicClient::with_http_transport(
                environment,
                RateBudget::new(),
                network.clone(),
            )
            .unwrap(),
        );
        service.sources.bootstrap = market.clone();
        service.heavy.close();
        service.research_memory.close();
        let installation = root.join("installation");
        crate::accounts::initialize_installation(&installation, "decision-dispatch-installation")
            .unwrap();
        let accounts = Arc::new(
            crate::accounts::AccountRuntimeRegistry::open(&installation, RateBudget::new())
                .await
                .unwrap(),
        );
        let (coordinator, public_ws) = okx_runtime::PublicWsCoordinator::new_with_connector(
            environment,
            okx_observation::ReferenceRegistry::from_public("fixture", vec![]).unwrap(),
            RateBudget::new(),
            Arc::new(crate::accounts::recorded_support::OfflineSocket),
        );
        let (risk_tx, mut risk_rx) = tokio::sync::mpsc::channel(4);
        let trade = Arc::new(crate::trade_service::TradeService::new(
            accounts.clone(),
            public_ws.clone(),
            crate::risk_bridge::NativeRiskClient::new(risk_tx),
        ));
        let context = crate::query::DispatchContext {
            accounts,
            analytics: service,
            public_ws,
            trade,
            market,
        };
        let envelope = |request| AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.into(),
            request_id: "decision-dispatch-request-001".into(),
            account: None,
            operation: AgentOperation::Research { request },
        };
        let recorded = crate::query::dispatch(
            &context,
            &envelope(record(&job, &result, None, "Dispatch hypothesis")),
        )
        .await;
        assert_eq!(recorded.status, AgentResponseStatus::Completed);
        let first = recorded.result.unwrap();
        let decision_id = first["decision_id"].as_str().unwrap();
        let reopened =
            crate::query::dispatch(&context, &envelope(read(&job, Some(decision_id)))).await;
        assert_eq!(reopened.status, AgentResponseStatus::Completed);
        assert_eq!(reopened.result.unwrap(), first);
        context
            .analytics
            .research_decision(record(&other_job, &result, None, "Other job"))
            .unwrap();
        let foreign =
            crate::query::dispatch(&context, &envelope(read(&other_job, Some(decision_id)))).await;
        assert_eq!(foreign.status, AgentResponseStatus::Rejected);
        assert_eq!(foreign.failure.unwrap().code, "DECISION_NOT_FOUND");
        assert_eq!(
            context.analytics.artifacts.read_evidence(&result).unwrap(),
            result_bytes
        );
        assert_eq!(context.analytics.jobs.snapshot().len(), 2);
        assert_eq!(network.0.load(Ordering::Relaxed), 0);
        assert!(risk_rx.try_recv().is_err());
        context.accounts.shutdown().await;
        drop(context);
        drop(coordinator);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn decision_rejects_foreign_unreachable_corrupt_expired_and_incomplete_results() {
        let (root, service) = super::super::artifact_job_fixtures::service();
        let (a, result) = fixture(&service, 'a');
        let (b, _) = fixture(&service, 'b');
        let first = service
            .research_decision(record(&a, &result, None, "A"))
            .unwrap();
        let first_id = first["decision_id"].as_str().unwrap();
        // Same immutable result belongs to two jobs; history remains job-bound.
        service
            .research_decision(record(&b, &result, None, "B"))
            .unwrap();
        assert_eq!(
            service
                .research_decision(read(&b, Some(first_id)))
                .unwrap_err(),
            "DECISION_NOT_FOUND"
        );
        let orphan: Decision = serde_json::from_value(first["decision"].clone()).unwrap();
        let mut orphan = orphan;
        orphan.hypothesis = "Unreachable but well formed".into();
        let orphan_id = service.artifacts.publish_evidence(&orphan).unwrap().0;
        assert_eq!(
            service
                .research_decision(read(&a, Some(&orphan_id)))
                .unwrap_err(),
            "DECISION_NOT_FOUND"
        );
        let wrong_result = service
            .artifacts
            .publish_evidence(&json!({"other":true}))
            .unwrap()
            .0;
        assert_eq!(
            service
                .research_decision(record(&a, &wrong_result, Some(first_id), "Changed"))
                .unwrap_err(),
            "DECISION_RESULT_MISMATCH"
        );
        let pending = "c".repeat(64);
        service
            .jobs
            .enqueue("pending-decision".into(), pending.clone())
            .unwrap();
        assert_eq!(
            service
                .research_decision(record(&pending, &result, None, "Pending"))
                .unwrap_err(),
            "RESULT_NOT_READY"
        );
        let result_path = service
            .artifacts
            .evidence_dir()
            .join(format!("{}.json", result.strip_prefix("sha256:").unwrap()));
        let bytes = std::fs::read(&result_path).unwrap();
        std::fs::write(&result_path, b"corrupt").unwrap();
        assert_eq!(
            service
                .research_decision(record(&a, &result, Some(first_id), "Corrupt"))
                .unwrap_err(),
            "ARTIFACT_CORRUPT"
        );
        std::fs::write(&result_path, bytes).unwrap();
        service.jobs.expire_terminal_artifact(&a, &result).unwrap();
        assert_eq!(
            service.research_decision(read(&a, None)).unwrap_err(),
            "RESULT_EXPIRED"
        );
        assert_eq!(
            service
                .research_decision(record(&a, &result, None, "Expired"))
                .unwrap_err(),
            "RESULT_EXPIRED"
        );
        assert!(service.jobs.get(&a).unwrap().decision_artifact.is_none());
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn decision_rejects_legacy_and_batch_provenance_without_committing_metadata() {
        let (root, service) = super::super::artifact_job_fixtures::service();
        for (digit, manifest) in [
            ('a', json!({"trade_pages":[],"total_trades":0})),
            (
                'b',
                json!({"kind":"research_batch","row_pages":[],"total_rows":0,"instrument_artifacts":[]}),
            ),
        ] {
            let (job, _) = fixture(&service, digit);
            // Immutable legacy rows are fixture inserts, not overwritten results.
            let legacy = digit
                .to_string()
                .repeat(64)
                .replace(digit, if digit == 'a' { "c" } else { "d" });
            let result = service.artifacts.publish_evidence(&manifest).unwrap().0;
            service
                .jobs
                .enqueue(format!("legacy-{digit}"), legacy.clone())
                .unwrap();
            service
                .jobs
                .update(&legacy, jobs::State::Completed, Some(result.clone()), None)
                .unwrap();
            assert_eq!(
                service
                    .research_decision(record(&legacy, &result, None, "Legacy"))
                    .unwrap_err(),
                "DECISION_PROVENANCE_UNAVAILABLE"
            );
            assert!(
                service
                    .jobs
                    .get(&legacy)
                    .unwrap()
                    .decision_artifact
                    .is_none()
            );
            assert!(service.jobs.get(&job).unwrap().decision_artifact.is_none());
        }
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn pinned_decision_chain_survives_shared_result_retention_then_expires() {
        let (root, service) = super::super::artifact_job_fixtures::service();
        let (a, result) = fixture(&service, 'a');
        let (b, _) = fixture(&service, 'b');
        let first = service
            .research_decision(record(&a, &result, None, "First"))
            .unwrap();
        let first_id = first["decision_id"].as_str().unwrap();
        let second = service
            .research_decision(record(&a, &result, Some(first_id), "Second"))
            .unwrap();
        let second_id = second["decision_id"].as_str().unwrap();
        let other = service
            .research_decision(record(&b, &result, None, "Other job"))
            .unwrap();
        let other_id = other["decision_id"].as_str().unwrap();
        service.pin_job(&a, true).unwrap();
        let future = chrono::Utc::now().timestamp_millis().max(0) as u64 + 8 * 86_400_000;
        assert_eq!(service.apply_retention(future).unwrap(), 1);
        assert!(service.artifacts.read_evidence(other_id).is_err());
        assert!(service.artifacts.read_evidence(&result).is_ok());
        assert_eq!(
            service.research_decision(read(&a, Some(first_id))).unwrap(),
            first
        );
        assert_eq!(service.research_decision(read(&a, None)).unwrap(), second);
        assert_eq!(
            service.research_decision(read(&b, None)).unwrap_err(),
            "RESULT_EXPIRED"
        );
        service.pin_job(&a, false).unwrap();
        assert_eq!(service.apply_retention(future).unwrap(), 1);
        for id in [first_id, second_id, result.as_str()] {
            assert!(service.artifacts.read_evidence(id).is_err());
        }
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn pre_decision_sqlite_catalog_migrates_and_preserves_existing_result_and_head() {
        let (root, mut service) = super::super::artifact_job_fixtures::service();
        let (job, result) = fixture(&service, 'e');
        let legacy_path = root.join("legacy.sqlite");
        let legacy = rusqlite::Connection::open(&legacy_path).unwrap();
        legacy.execute_batch("CREATE TABLE research_jobs(
            id TEXT PRIMARY KEY,request_hash TEXT NOT NULL,idempotency_key TEXT NOT NULL UNIQUE,
            state TEXT NOT NULL,progress INTEGER NOT NULL,artifact TEXT,error TEXT,created_ms INTEGER NOT NULL,pinned INTEGER NOT NULL
        ) STRICT;").unwrap();
        legacy
            .execute(
                "INSERT INTO research_jobs VALUES(?1,?1,'legacy','completed',100,?2,NULL,?3,0)",
                rusqlite::params![job, result, chrono::Utc::now().timestamp_millis()],
            )
            .unwrap();
        drop(legacy);
        service.jobs = jobs::Jobs::open(legacy_path.clone()).unwrap();
        assert!(service.jobs.get(&job).unwrap().decision_artifact.is_none());
        let original_result = service.artifacts.read_evidence(&result).unwrap();
        let decision = service
            .research_decision(record(&job, &result, None, "Migrated"))
            .unwrap();
        // Existing whole-row lifecycle updates preserve the metadata head.
        assert!(
            service
                .jobs
                .update(&job, jobs::State::Completed, Some(result.clone()), None)
                .is_err()
        );
        assert_eq!(
            service.research_decision(read(&job, None)).unwrap(),
            decision
        );
        service.jobs = jobs::Jobs::open(legacy_path).unwrap();
        assert_eq!(
            service.research_decision(read(&job, None)).unwrap(),
            decision
        );
        assert_eq!(
            service.artifacts.read_evidence(&result).unwrap(),
            original_result
        );
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn accepted_rejected_deferred_and_tampered_dataset_or_decision_are_explicit() {
        let (root, service) = super::super::artifact_job_fixtures::service();
        let (job, result) = fixture(&service, 'f');
        let mut previous = None;
        for status in [
            ResearchDecisionStatus::Accepted,
            ResearchDecisionStatus::Rejected,
            ResearchDecisionStatus::Deferred,
        ] {
            let mut request = record(&job, &result, previous.as_deref(), "Status annotation");
            if let ResearchRequest::RecordDecision { status: value, .. } = &mut request {
                *value = status;
            }
            let response = service.research_decision(request).unwrap();
            assert_eq!(
                response["decision"]["status"],
                serde_json::to_value(status).unwrap()
            );
            assert_eq!(response["trading_authority"], false);
            previous = Some(response["decision_id"].as_str().unwrap().to_owned());
        }
        let head = previous.unwrap();
        let decision_path = service
            .artifacts
            .evidence_dir()
            .join(format!("{}.json", head.strip_prefix("sha256:").unwrap()));
        let decision_bytes = std::fs::read(&decision_path).unwrap();
        std::fs::write(&decision_path, b"tampered").unwrap();
        assert_eq!(
            service.research_decision(read(&job, None)).unwrap_err(),
            "ARTIFACT_CORRUPT"
        );
        std::fs::write(&decision_path, decision_bytes).unwrap();
        let manifest: Value = service.artifacts.read_evidence_json(&result).unwrap();
        let dataset = manifest["dataset_artifact"].as_str().unwrap();
        let dataset_path = service
            .artifacts
            .evidence_dir()
            .join(format!("{}.json", dataset.strip_prefix("sha256:").unwrap()));
        std::fs::write(dataset_path, b"tampered").unwrap();
        assert_eq!(
            service.research_decision(read(&job, None)).unwrap_err(),
            "ARTIFACT_CORRUPT"
        );
        assert_eq!(service.jobs.snapshot().len(), 1);
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn decision_response_keeps_full_annotation_and_references_within_public_packet_budget() {
        let (root, service) = super::super::artifact_job_fixtures::service();
        let dataset = service
            .artifacts
            .publish_evidence(&json!({"chunks":[]}))
            .unwrap()
            .0;
        let experiment = service
            .artifacts
            .publish_evidence(&json!({
                "schema":"okx.research.experiment-inputs/v1","dataset_artifact":dataset,
                "code_version":"budget-fixture","archived_note":"x".repeat(12000),
                "spec":{"seed":7,"costs":{"taker_fee_bps":2}}
            }))
            .unwrap()
            .0;
        let result = service.artifacts.publish_evidence(&json!({
            "dataset_artifact":dataset,"experiment_artifact":experiment,"code_version":"budget-fixture",
            "trade_pages":[],"total_trades":0
        })).unwrap().0;
        let job = "a".repeat(64);
        service
            .jobs
            .enqueue("budget-decision".into(), job.clone())
            .unwrap();
        service
            .jobs
            .update(&job, jobs::State::Completed, Some(result.clone()), None)
            .unwrap();
        // Legal UTF-8 bytes and near-maximum serialized annotation tuple.
        let mut request = record(&job, &result, None, &"я".repeat(2048));
        if let ResearchRequest::RecordDecision { reason, .. } = &mut request {
            *reason = "\"\\\t".repeat(300);
        }
        assert!(request.valid());
        let response = service.research_decision(request).unwrap();
        assert_eq!(response["decision"]["hypothesis"], "я".repeat(2048));
        assert_eq!(response["decision"]["experiment_artifact"], experiment);
        assert_eq!(
            response["experiment_body_omitted"],
            "RESPONSE_BUDGET_USE_IMMUTABLE_EXPERIMENT_REFERENCE"
        );
        assert!(response.get("experiment").is_none());
        let packet =
            json!({"schema":"okx.agent.response/v1","request_id":"a".repeat(64),"result":response});
        assert!(serde_json::to_vec(&packet).unwrap().len() <= 12288);
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }
}

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

fn record(job: &str, result: &str, previous: Option<&str>, hypothesis: &str) -> ResearchRequest {
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
    let scope =
        okx_research_protocol::research_ipc::ResearchScope::new("decision-scope".into()).unwrap();
    drop(AnalyticsService::new_scoped(root.clone(), bootstrap.clone(), &scope).unwrap());
    let producer = crate::producer::Producer::new("a".repeat(64), None, None).unwrap();
    let reopened = AnalyticsService::new_existing_scoped_with_producer(
        root.clone(),
        bootstrap,
        &scope,
        producer,
    )
    .unwrap();
    let status = crate::edge_runtime::dispatch_research_for_test(
        &reopened,
        ResearchRequest::Status {
            job_id: job.clone(),
        },
    )
    .await
    .unwrap();
    assert_eq!(status["state"], "completed");
    assert_eq!(status["artifact"], result);
    assert_eq!(
        crate::edge_runtime::dispatch_research_for_test(&reopened, read(&job, None))
            .await
            .unwrap(),
        second
    );
    assert_eq!(
        crate::edge_runtime::dispatch_research_for_test(
            &reopened,
            record(&job, &result, Some(&first_id), "Revised hypothesis")
        )
        .await
        .unwrap(),
        second
    );
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
    let future = jiff::Timestamp::now().as_millisecond().max(0) as u64 + 8 * 86_400_000;
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
            rusqlite::params![job, result, jiff::Timestamp::now().as_millisecond()],
        )
        .unwrap();
    drop(legacy);
    drop(service.jobs);
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
    drop(service.jobs);
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

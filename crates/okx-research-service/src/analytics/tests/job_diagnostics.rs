use super::super::{Jobs, State as JobState};
use super::*;
fn jobs(label: &str) -> (Jobs, std::path::PathBuf) {
    let path = std::env::temp_dir()
        .join(format!(
            "okx-job-diagnostic-{label}-{}-{}.sqlite",
            std::process::id(),
            jiff::Timestamp::now().as_nanosecond()
        ))
        .join("catalog.sqlite");
    (Jobs::open(path.clone()).unwrap(), path)
}
#[test]
fn job_diagnostic_is_local_while_catalog_owner_is_busy() {
    let (jobs, _) = jobs("busy");
    jobs.enqueue("private-key".into(), "a".repeat(64)).unwrap();
    jobs.update(&"a".repeat(64), JobState::Running, None, None)
        .unwrap();
    let before = jobs.diagnostic_readback(&"a".repeat(64));
    let lock = jobs.db.lock().unwrap();
    let reader = jobs.clone();
    let (send, recv) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        send.send(reader.diagnostic_readback(&"a".repeat(64)))
            .unwrap()
    });
    let observed = recv.recv_timeout(std::time::Duration::from_millis(200));
    drop(lock);
    thread.join().unwrap();
    let observed = observed.expect("diagnostic waited for catalog SQL mutex");
    assert_eq!(observed["job"]["state"], "running");
    assert_eq!(observed["job"]["progress"], 10);
    assert_eq!(observed["job"]["generation"], before["job"]["generation"]);
    assert!(!observed.to_string().contains("private-key"));
}
#[test]
fn job_diagnostic_contention_is_visible_and_does_not_change_sql_outcomes() {
    let (jobs, _) = jobs("contention");
    let id = "b".repeat(64);
    jobs.enqueue("enqueue".into(), id.clone()).unwrap();
    let held = jobs.diagnostic.state.write().unwrap();
    assert_eq!(
        jobs.diagnostic_readback(&id)["reason_code"],
        "JOB_TRACE_BUSY"
    );
    jobs.update(&id, JobState::Running, None, None).unwrap();
    drop(held);
    let partial = jobs.diagnostic_readback(&id);
    assert_eq!(partial["job"]["state"], "queued");
    assert_eq!(partial["dropped_events"], 1);
    assert_eq!(partial["history"], "UNKNOWN");
    jobs.update(
        &id,
        JobState::Failed,
        None,
        Some("credential=DO_NOT_LEAK".into()),
    )
    .unwrap();
    let current = jobs.diagnostic_readback(&id);
    assert_eq!(current["job"]["state"], "failed");
    assert_eq!(current["job"]["error_code"], "UNCLASSIFIED_RESEARCH_ERROR");
    assert!(!current.to_string().contains("DO_NOT_LEAK"));
    assert_eq!(jobs.get(&id).unwrap().state, JobState::Failed);
}
#[test]
fn job_diagnostic_tracks_cancel_pin_expiry_and_restart_durable_subset() {
    let (jobs, path) = jobs("mutations");
    let cancelled = "c".repeat(64);
    jobs.enqueue("cancel-input".into(), cancelled.clone())
        .unwrap();
    jobs.cancel(&cancelled, "private-cancel-key").unwrap();
    assert_eq!(
        jobs.diagnostic_readback(&cancelled)["job"]["state"],
        "cancelled"
    );
    let id = "d".repeat(64);
    let artifact = "e".repeat(64);
    jobs.enqueue("complete".into(), id.clone()).unwrap();
    jobs.update(&id, JobState::Completed, Some(artifact.clone()), None)
        .unwrap();
    jobs.pin(&id, true).unwrap();
    assert_eq!(jobs.diagnostic_readback(&id)["job"]["pinned"], true);
    jobs.pin(&id, false).unwrap();
    jobs.expire_terminal_artifact(&id, &artifact).unwrap();
    let expired = jobs.diagnostic_readback(&id);
    assert_eq!(expired["job"]["result_ref"], Value::Null);
    assert_eq!(
        expired["job"]["error_code"],
        "RESULT_EXPIRED_BY_7_DAY_RETENTION"
    );
    let active = "f".repeat(64);
    jobs.enqueue("restart-active".into(), active.clone())
        .unwrap();
    drop(jobs);
    let reopened = Jobs::open(path).unwrap();
    let restored = reopened.diagnostic_readback(&id);
    assert_eq!(restored["job"]["origin"], "durable_startup_subset");
    assert_eq!(restored["job"]["state"], "completed");
    assert_eq!(restored["history"], "UNKNOWN");
    assert_eq!(
        reopened.diagnostic_readback(&active)["job"]["state"],
        "interrupted"
    );
    assert_eq!(
        reopened.diagnostic_readback(&active)["job"]["error_code"],
        "RESEARCH_INTERRUPTED"
    );
    assert!(
        !reopened
            .diagnostic_readback(&cancelled)
            .to_string()
            .contains("private-cancel-key")
    );
}
#[test]
fn completed_sha256_result_reference_survives_restart_without_leaking_bad_references() {
    let (jobs, file) = jobs("canonical-result-ref");
    let completed = "1".repeat(64);
    let result = format!("sha256:{}", "a".repeat(64));
    jobs.enqueue("canonical-result".into(), completed.clone())
        .unwrap();
    jobs.update(&completed, JobState::Completed, Some(result.clone()), None)
        .unwrap();
    assert_eq!(
        jobs.diagnostic_readback(&completed)["job"]["result_ref"],
        result
    );

    let invalid = [
        format!("sha256:{}", "A".repeat(64)),
        format!("sha256:{}x", "a".repeat(63)),
        format!("sha256:{}", "a".repeat(65)),
        format!("sha256{}", "a".repeat(64)),
        "secret=DO_NOT_COPY".into(),
    ];
    for (index, artifact) in invalid.iter().enumerate() {
        let id = format!("{:064x}", index + 2);
        jobs.enqueue(format!("invalid-{index}"), id.clone())
            .unwrap();
        jobs.update(&id, JobState::Completed, Some(artifact.clone()), None)
            .unwrap();
        let projection = jobs.diagnostic_readback(&id);
        assert_eq!(projection["job"]["state"], "completed");
        assert!(projection["job"]["result_ref"].is_null());
        assert!(!projection.to_string().contains("DO_NOT_COPY"));
    }

    let interrupted = "f".repeat(64);
    jobs.enqueue("incomplete-result".into(), interrupted.clone())
        .unwrap();
    jobs.update(&interrupted, JobState::Running, Some(result.clone()), None)
        .unwrap();
    assert!(jobs.diagnostic_readback(&interrupted)["job"]["result_ref"].is_null());

    drop(jobs);
    let restored = Jobs::open(file).unwrap();
    assert_eq!(
        restored.get(&completed).unwrap().artifact,
        Some(result.clone())
    );
    let proof = restored.diagnostic_readback(&completed);
    assert_eq!(proof["job"]["origin"], "durable_startup_subset");
    assert_eq!(proof["job"]["result_ref"], result);
    for index in 0..invalid.len() {
        assert!(
            restored.diagnostic_readback(&format!("{:064x}", index + 2))["job"]["result_ref"]
                .is_null()
        );
    }
    let interrupted_proof = restored.diagnostic_readback(&interrupted);
    assert_eq!(interrupted_proof["job"]["state"], "interrupted");
    assert!(interrupted_proof["job"]["result_ref"].is_null());
}
#[test]
fn legacy_bare_result_ref_rejects_64_byte_non_hex_secret_across_restart() {
    let (jobs, path) = jobs("legacy-bare-result-guard");
    let valid_id = "7".repeat(64);
    let invalid_id = "8".repeat(64);
    let valid = "a".repeat(64);
    let marker = "DO_NOT_DISCLOSE_64_BYTE_SECRET";
    let invalid = format!("{marker}{}", "x".repeat(64 - marker.len()));
    assert_eq!(invalid.len(), 64);

    for (id, label, artifact) in [
        (&valid_id, "valid-legacy-bare-hex", &valid),
        (&invalid_id, "invalid-legacy-bare-hex", &invalid),
    ] {
        jobs.enqueue(label.into(), id.clone()).unwrap();
        jobs.update(id, JobState::Completed, Some(artifact.clone()), None)
            .unwrap();
    }

    let assert_projection = |jobs: &Jobs| {
        let accepted = jobs.diagnostic_readback(&valid_id);
        assert_eq!(accepted["job"]["state"], "completed");
        assert_eq!(accepted["job"]["result_ref"], valid);

        let rejected = jobs.diagnostic_readback(&invalid_id);
        assert_eq!(rejected["job"]["state"], "completed");
        assert!(rejected["job"]["result_ref"].is_null());
        assert!(!rejected.to_string().contains(marker));
        // The catalog still owns its original data; only the read-only
        // diagnostic projection is prohibited from exposing a false digest.
        assert_eq!(
            jobs.get(&invalid_id).unwrap().artifact.as_deref(),
            Some(invalid.as_str())
        );
    };

    assert_projection(&jobs);
    drop(jobs);
    let reopened = Jobs::open(path).unwrap();
    assert_projection(&reopened);
    assert_eq!(
        reopened.diagnostic_readback(&valid_id)["job"]["origin"],
        "durable_startup_subset"
    );
    assert_eq!(
        reopened.diagnostic_readback(&invalid_id)["job"]["origin"],
        "durable_startup_subset"
    );
}
#[test]
fn t14_partial_artifact_is_never_advertised_as_a_completed_result() {
    let (jobs, file) = jobs("partial-result");
    let pending_id = "a".repeat(64);
    let partial_artifact = "b".repeat(64);
    jobs.enqueue("pending-job".into(), pending_id.clone())
        .unwrap();
    jobs.update(
        &pending_id,
        JobState::Running,
        Some(partial_artifact.clone()),
        None,
    )
    .unwrap();
    assert_eq!(
        jobs.diagnostic_readback(&pending_id)["job"]["state"],
        "running"
    );
    assert!(jobs.diagnostic_readback(&pending_id)["job"]["result_ref"].is_null());
    drop(jobs);
    let restarted = Jobs::open(file).unwrap();
    let durable = restarted.get(&pending_id).unwrap();
    assert_eq!(durable.state, JobState::Interrupted);
    assert_eq!(durable.artifact, Some(partial_artifact));
    let observed = restarted.diagnostic_readback(&pending_id);
    assert_eq!(observed["job"]["state"], "interrupted");
    assert_eq!(observed["job"]["error_code"], "RESEARCH_INTERRUPTED");
    assert!(observed["job"]["result_ref"].is_null());
    assert_eq!(observed["history"], "UNKNOWN");
    assert_eq!(
        restarted
            .update(&pending_id, JobState::Completed, Some("c".repeat(64)), None)
            .unwrap_err(),
        "RESEARCH_TERMINAL_IMMUTABLE"
    );

    let completed_id = "d".repeat(64);
    let complete_artifact = "e".repeat(64);
    restarted
        .enqueue("complete-job".into(), completed_id.clone())
        .unwrap();
    restarted
        .update(
            &completed_id,
            JobState::Completed,
            Some(complete_artifact.clone()),
            None,
        )
        .unwrap();
    assert_eq!(
        restarted.diagnostic_readback(&completed_id)["job"]["result_ref"],
        complete_artifact
    );
}
#[test]
fn job_diagnostic_preserves_known_failure_codes_without_payloads() {
    let (jobs, _) = jobs("reasons");
    for (n, (raw, expected)) in [
        ("CANDLE_INTERIOR_GAP", "CANDLE_INTERIOR_GAP"),
        ("LIMIT_EXCEEDED:RESEARCH_MEMORY", "LIMIT_EXCEEDED"),
        ("RESOURCE_BUSY:RESEARCH_QUEUE_FULL", "RESOURCE_BUSY"),
        (
            "public OKX market API error: OKX rate/backpressure defer: LocalBudget",
            "MARKET_SOURCE_RETRYABLE",
        ),
        ("api_key=DO_NOT_LEAK", "UNCLASSIFIED_RESEARCH_ERROR"),
    ]
    .into_iter()
    .enumerate()
    {
        let id = format!("{n:064x}");
        jobs.enqueue(format!("key{n}"), id.clone()).unwrap();
        jobs.update(&id, JobState::Failed, None, Some(raw.into()))
            .unwrap();
        let observed = jobs.diagnostic_readback(&id);
        assert_eq!(observed["job"]["error_code"], expected);
        assert!(!observed.to_string().contains("DO_NOT_LEAK"));
    }
}
#[test]
fn p7_missing_dataset_job_diagnostic_survives_retry_and_restart_without_inventing_result() {
    use okx_research_protocol::research_ipc::{IdempotencyKey, InvocationId};

    let (jobs, file) = jobs("p7-missing-dataset");
    let original = InvocationId::new("req_missing_dataset_original".into()).unwrap();
    let retry = InvocationId::new("req_missing_dataset_retry".into()).unwrap();
    let key = IdempotencyKey::new("missing-dataset-owner-key".into()).unwrap();
    let id = "f".repeat(64);
    let (created, fresh, _) = jobs
        .enqueue_semantic_with_origin(
            key.clone(),
            id.clone(),
            "p7-dataset-semantic".into(),
            Some(original.clone()),
        )
        .unwrap();
    assert!(fresh);
    jobs.update(&id, JobState::Running, None, None).unwrap();
    jobs.update(
        &id,
        JobState::Failed,
        None,
        Some("CANDLE_RANGE_UNAVAILABLE".into()),
    )
    .unwrap();
    let (replayed, fresh, _) = jobs
        .enqueue_semantic_with_origin(
            key,
            id.clone(),
            "p7-dataset-semantic".into(),
            Some(retry.clone()),
        )
        .unwrap();
    assert!(!fresh);
    assert_eq!(replayed.id, created.id);
    assert_eq!(jobs.snapshot().len(), 1);

    for reference in [original.as_str(), retry.as_str()] {
        let observation = jobs.diagnostic_by_invocation(reference);
        assert_eq!(observation["state"], "OBSERVED");
        assert_eq!(observation["reference_kind"], "request_id");
        assert_eq!(observation["matched_request_id"], reference);
        assert_eq!(observation["job"]["id"], id);
        assert_eq!(observation["job"]["state"], "failed");
        assert_eq!(observation["job"]["error_code"], "CANDLE_RANGE_UNAVAILABLE");
        assert!(observation["job"]["result_ref"].is_null());
        assert_eq!(observation["history"], "UNKNOWN");
        assert_eq!(observation["next_action"], "inspect_job_snapshot");
    }
    assert_eq!(
        jobs.diagnostic_by_invocation("req_missing_dataset_unrelated")["state"],
        "NOT_OBSERVED"
    );
    assert_eq!(jobs.snapshot().len(), 1);

    drop(jobs);
    let restored = Jobs::open(file).unwrap();
    assert_eq!(restored.get(&id).unwrap().state, JobState::Failed);
    assert_eq!(restored.snapshot().len(), 1);
    for reference in [original.as_str(), retry.as_str()] {
        let observation = restored.diagnostic_by_invocation(reference);
        assert_eq!(observation["matched_request_id"], reference);
        assert_eq!(observation["job"]["id"], id);
        assert_eq!(observation["job"]["origin"], "durable_startup_subset");
        assert_eq!(observation["job"]["error_code"], "CANDLE_RANGE_UNAVAILABLE");
        assert!(observation["job"]["result_ref"].is_null());
    }
    let missing = restored.diagnostic_by_invocation("req_missing_dataset_unrelated");
    assert_eq!(missing["reason_code"], "UNKNOWN_NOT_RETAINED");
    assert!(missing.get("matched_request_id").is_none());
    assert_eq!(restored.snapshot().len(), 1);
}
#[test]
fn failed_startup_seed_is_explicit_and_does_not_invent_missing_history() {
    let db = Connection::open_in_memory().unwrap();
    let projection = Projection::default();
    // An unreadable/missing catalog cannot be reported as an empty proven catalog.
    projection.restore(&db);
    let observed = projection.read(&"a".repeat(64));
    assert_eq!(observed["state"], "NOT_OBSERVED");
    assert_eq!(observed["reason_code"], "UNKNOWN_NOT_RETAINED");
    assert_eq!(observed["startup_restore_failed"], true);
    assert_eq!(observed["dropped_events"], 1);
    assert_eq!(observed["history"], "UNKNOWN");
    assert!(observed["job"].is_null());
}
#[test]
fn poisoned_projection_is_unavailable_rather_than_contended() {
    let projection = Projection::default();
    let _ = std::panic::catch_unwind(|| {
        let _held = projection.state.write().unwrap();
        panic!("test projection poisoning");
    });
    let observed = projection.read(&"a".repeat(64));
    assert_eq!(observed["state"], "NOT_EVALUATED");
    assert_eq!(observed["reason_code"], "JOB_TRACE_UNAVAILABLE");
    assert_eq!(observed["history"], "UNKNOWN");
}
#[test]
fn job_diagnostic_retention_and_indexed_startup_are_bounded() {
    let (jobs, path) = jobs("bounded");
    for n in 0..130 {
        let id = format!("{n:064x}");
        jobs.enqueue(format!("key{n}"), id.clone()).unwrap();
        jobs.update(&id, JobState::Failed, None, None).unwrap();
    }
    let read = jobs.diagnostic_readback(&format!("{:064x}", 129));
    assert_eq!(read["retained"], 128);
    assert_eq!(read["evicted"], 2);
    assert_eq!(
        jobs.diagnostic_readback(&format!("{:064x}", 0))["reason_code"],
        "UNKNOWN_NOT_RETAINED"
    );
    {
        let db = jobs.db.lock().unwrap();
        let sql = format!(
            "SELECT {COLUMNS} FROM research_jobs ORDER BY created_ms DESC,id DESC LIMIT 129"
        );
        let plan = db
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .unwrap()
            .query_map([], |r| r.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
            .join(" ");
        assert!(
            plan.contains("idx_research_jobs_diagnostics_recent"),
            "{plan}"
        );
        assert!(!plan.contains("TEMP B-TREE"), "{plan}");
        assert_eq!(
            db.prepare(&sql)
                .unwrap()
                .query_map([], row)
                .unwrap()
                .count(),
            129
        );
    }
    drop(jobs);
    let reopened = Jobs::open(path).unwrap();
    assert_eq!(
        reopened.diagnostic_readback(&format!("{:064x}", 0))["retained"],
        128
    );
    assert_eq!(
        reopened.diagnostic_readback(&format!("{:064x}", 0))["startup_subset_truncated"],
        true
    );
    assert_eq!(
        reopened.diagnostic_readback(&format!("{:064x}", 129))["job"]["origin"],
        "durable_startup_subset"
    );
    assert_eq!(
        reopened.diagnostic_readback(&format!("{:064x}", 129))["job"]["state"],
        "failed"
    );
    assert_eq!(
        reopened.diagnostic_readback(&format!("{:064x}", 0))["state"],
        "NOT_OBSERVED"
    );
    assert_eq!(
        reopened.diagnostic_readback(&format!("{:064x}", 1))["state"],
        "NOT_OBSERVED"
    );
    assert_eq!(
        reopened.diagnostic_readback(&format!("{:064x}", 2))["job"]["state"],
        "failed"
    );
    assert_eq!(
        reopened.diagnostic_readback(&format!("{:064x}", 0))["startup_subset_selection"],
        "latest_created_ms_then_id"
    );
    // The bounded diagnostic is not the authority. Older jobs remain
    // available from the original durable catalog when queried by ID.
    assert_eq!(
        reopened.get(&format!("{:064x}", 0)).unwrap().state,
        JobState::Failed
    );
}

#[test]
fn restart_truncated_request_projection_never_invents_origin_or_retry_history() {
    use okx_research_protocol::research_ipc::{IdempotencyKey, InvocationId};

    let (jobs, file) = jobs("bounded-request-aliases");
    for n in 0..130 {
        let id = format!("{n:064x}");
        let key = IdempotencyKey::new(format!("retention-key-{n}")).unwrap();
        let origin = InvocationId::new(format!("req_retained_origin_{n}")).unwrap();
        let (created, fresh, _) = jobs
            .enqueue_semantic_with_origin(
                key.clone(),
                id.clone(),
                format!("semantic-{n}"),
                Some(origin),
            )
            .unwrap();
        assert!(fresh);
        assert_eq!(created.id, id);
        jobs.update(
            &id,
            JobState::Failed,
            None,
            Some("CANDLE_RANGE_UNAVAILABLE".into()),
        )
        .unwrap();
        if n == 0 || n == 129 {
            let retry = InvocationId::new(format!("req_retained_retry_{n}")).unwrap();
            let (same, fresh, _) = jobs
                .enqueue_semantic_with_origin(key, id.clone(), format!("semantic-{n}"), Some(retry))
                .unwrap();
            assert!(!fresh);
            assert_eq!(same.id, id);
        }
    }
    drop(jobs);
    let restored = Jobs::open(file).unwrap();
    // The durable catalog still owns the old job, but the bounded, read-only
    // startup trace cannot claim to remember an evicted request or its retry.
    for reference in [
        "req_retained_origin_0",
        "req_retained_retry_0",
        "req_never_seen_by_research",
    ] {
        let missing = restored.diagnostic_by_invocation(reference);
        assert_eq!(missing["state"], "NOT_OBSERVED");
        assert_eq!(missing["reason_code"], "UNKNOWN_NOT_RETAINED");
        assert_eq!(missing["quality"], "partial");
        assert_eq!(missing["startup_subset_truncated"], true);
        assert_eq!(missing["history"], "UNKNOWN");
        assert_eq!(missing["next_action"], "retry_read_only_diagnostic");
        assert!(missing.get("matched_request_id").is_none());
        assert!(missing["job"].is_null());
    }
    for reference in ["req_retained_origin_129", "req_retained_retry_129"] {
        let observed = restored.diagnostic_by_invocation(reference);
        assert_eq!(observed["state"], "OBSERVED");
        assert_eq!(observed["matched_request_id"], reference);
        assert_eq!(observed["job"]["id"], format!("{:064x}", 129));
        assert_eq!(observed["job"]["origin"], "durable_startup_subset");
        assert_eq!(observed["job"]["error_code"], "CANDLE_RANGE_UNAVAILABLE");
        assert!(observed["job"]["result_ref"].is_null());
    }
    assert_eq!(
        restored.get(&format!("{:064x}", 0)).unwrap().state,
        JobState::Failed
    );
}

#[test]
fn conflicting_origin_and_retry_alias_never_selects_a_phantom_job() {
    use okx_research_protocol::research_ipc::InvocationId;

    // Simulate a damaged/replayed bounded projection, not a permitted catalog
    // write: two different jobs claim the same external request identity.
    let shared = InvocationId::new("req_ambiguous_replayed_origin".into()).unwrap();
    let unrelated = InvocationId::new("req_other_job_original".into()).unwrap();
    let original_job = "a".repeat(64);
    let second_job = "b".repeat(64);
    let snapshot = |id: &str, origin: InvocationId, aliases: Vec<InvocationId>| Snapshot {
        id: id.to_owned(),
        state: "failed".into(),
        observed_stage: ObservedStage::Terminal,
        progress: 0,
        candidate_progress: None,
        origin_invocation: Some(origin),
        retry_invocations: aliases,
        producer: None,
        result_ref: None,
        error_code: Some("CANDLE_RANGE_UNAVAILABLE".into()),
        failure_class: Some(FailureClass::Source),
        pinned: false,
        observed_at_ms: 0,
        generation: 0,
        origin: "catalog_commit",
    };
    let projection = Projection::default();
    projection.publish(snapshot(&original_job, shared.clone(), vec![]));
    // Republishing the same job is an update, never a second conflicting job.
    projection.publish(snapshot(&original_job, shared.clone(), vec![]));
    let unambiguous = projection.read_by_invocation(shared.as_str());
    assert_eq!(unambiguous["state"], "OBSERVED");
    assert_eq!(unambiguous["matched_request_id"], shared.as_str());
    assert_eq!(unambiguous["retained"], 1);

    projection.publish(snapshot(
        &second_job,
        unrelated.clone(),
        vec![shared.clone()],
    ));
    let collision = projection.read_by_invocation(shared.as_str());
    assert_eq!(collision["state"], "NOT_EVALUATED");
    assert_eq!(collision["reason_code"], "AMBIGUOUS_REQUEST_ORIGIN");
    assert_eq!(collision["next_action"], "supply_exact_job_id");
    assert_eq!(collision["history"], "UNKNOWN");
    assert_eq!(collision["retained"], 2);
    assert!(collision.get("matched_request_id").is_none());
    assert!(collision["job"].is_null());
    // The exact local job IDs remain individually readable without exposing
    // an arbitrary winner for the colliding external request identity.
    assert_eq!(projection.read(&original_job)["job"]["id"], original_job);
    assert_eq!(projection.read(&second_job)["job"]["id"], second_job);

    // Replacing a snapshot must also replace its aliases. An old retry
    // identity cannot survive a catalog projection refresh as a phantom
    // collision; the other job's proven original identity stays intact.
    projection.publish(snapshot(&second_job, unrelated.clone(), vec![]));
    let restored = projection.read_by_invocation(shared.as_str());
    assert_eq!(restored["state"], "OBSERVED");
    assert_eq!(restored["reason_code"], "JOB_SNAPSHOT");
    assert_eq!(restored["matched_request_id"], shared.as_str());
    assert_eq!(restored["job"]["id"], original_job);
    assert_eq!(restored["retained"], 2);
    assert_eq!(
        projection.read_by_invocation(unrelated.as_str())["job"]["id"],
        second_job
    );
}

#[test]
fn terminal_reason_classification_matches_live_reason_and_durable_sql_projection() {
    // One stable reason contract across live Research/Market observations,
    // the bounded job projection, and a catalog restart. Exact matching also
    // prevents arbitrary secret-bearing prefixes from being reclassified as
    // resource failure or source backpressure.
    let (jobs, file) = jobs("reason-parity");
    let cases = [
        ("NONFINITE_SOURCE", "NONFINITE_SOURCE", "source"),
        ("INVALID_FUNDING", "INVALID_FUNDING", "source"),
        (
            "GLOBAL_RANK_SOURCE_INCOMPLETE",
            "GLOBAL_RANK_SOURCE_INCOMPLETE",
            "source",
        ),
        ("RESEARCH_MEMORY_LIMIT", "RESEARCH_MEMORY_LIMIT", "resource"),
        ("LIMIT_TOTAL_BARS:1000000", "LIMIT_TOTAL_BARS", "resource"),
        ("LIMIT_BARS:1000000", "LIMIT_BARS", "resource"),
        ("LIMIT_EXCEEDED:WORK_UNITS", "LIMIT_EXCEEDED", "resource"),
        ("RESOURCE_BUSY:QUEUE_FULL", "RESOURCE_BUSY", "resource"),
        (
            "UNSUPPORTED_INSTRUMENT_TYPE",
            "UNSUPPORTED_INSTRUMENT_TYPE",
            "contract",
        ),
        (
            "RESEARCH_BATCH_ALL_INSTRUMENTS_FAILED",
            "RESEARCH_BATCH_ALL_INSTRUMENTS_FAILED",
            "batch",
        ),
        (
            "RESEARCH_BATCH_SOURCE_RETRYABLE",
            "RESEARCH_BATCH_SOURCE_RETRYABLE",
            "batch",
        ),
        (
            "RESULT_EXPIRED_BY_7_DAY_RETENTION",
            "RESULT_EXPIRED_BY_7_DAY_RETENTION",
            "lifecycle",
        ),
        (
            "process restarted before completion",
            "RESEARCH_INTERRUPTED",
            "lifecycle",
        ),
        (
            "OKX rate/backpressure defer: limited",
            "MARKET_SOURCE_RETRYABLE",
            "source",
        ),
        (
            "public OKX market API error: OKX rate/backpressure defer: limited",
            "MARKET_SOURCE_RETRYABLE",
            "source",
        ),
        (
            "LIMIT_NOT_A_REAL_CODE:private",
            "UNCLASSIFIED_RESEARCH_ERROR",
            "unclassified",
        ),
        (
            "credential=OKX rate/backpressure defer: secret",
            "UNCLASSIFIED_RESEARCH_ERROR",
            "unclassified",
        ),
        (
            "limit_bars:private",
            "UNCLASSIFIED_RESEARCH_ERROR",
            "unclassified",
        ),
        (
            "RESOURCE-BUSY:private",
            "UNCLASSIFIED_RESEARCH_ERROR",
            "unclassified",
        ),
        (
            "okx rate/backpressure defer: secret",
            "UNCLASSIFIED_RESEARCH_ERROR",
            "unclassified",
        ),
    ];
    for (index, &(raw, code, class)) in cases.iter().enumerate() {
        assert_eq!(safe_reason(raw), code, "live classifier: {index}");
        let id = format!("{index:064x}");
        jobs.enqueue(format!("parity-{index}"), id.clone()).unwrap();
        jobs.update(&id, JobState::Failed, None, Some(raw.to_owned()))
            .unwrap();
        let observed = jobs.diagnostic_readback(&id);
        assert_eq!(observed["job"]["error_code"], code, "SQL: {index}");
        assert_eq!(observed["job"]["failure_class"], class, "class: {index}");
        assert_eq!(observed["job"]["observed_stage"], "terminal");
        assert!(!observed.to_string().contains("secret"));
        assert!(!observed.to_string().contains("private"));
    }

    let waiting = "f".repeat(64);
    jobs.enqueue("waiting-parity".into(), waiting.clone()).unwrap();
    assert_eq!(
        jobs.diagnostic_readback(&waiting)["job"]["observed_stage"],
        "admission"
    );
    jobs.update(&waiting, JobState::Running, None, None).unwrap();
    assert_eq!(
        jobs.diagnostic_readback(&waiting)["job"]["observed_stage"],
        "execution"
    );

    drop(jobs);
    let reopened = Jobs::open(file).unwrap();
    for (index, &(_, code, class)) in cases.iter().enumerate() {
        let observed = reopened.diagnostic_readback(&format!("{index:064x}"));
        assert_eq!(
            observed["job"]["error_code"], code,
            "restored code: {index}"
        );
        assert_eq!(
            observed["job"]["failure_class"], class,
            "restored class: {index}"
        );
        assert_eq!(observed["job"]["observed_stage"], "terminal");
        assert_eq!(observed["job"]["origin"], "durable_startup_subset");
    }
}

#[test]
fn durable_origin_and_verified_retry_collision_stays_ambiguous_after_restart() {
    use okx_research_protocol::research_ipc::{IdempotencyKey, InvocationId};

    // A real retry link can collide with a different job's original request
    // identity. Neither a catalog commit nor startup restoration may silently
    // pick the completed job (or disclose its result) for that request ID.
    let (jobs, file) = jobs("durable-origin-retry-collision");
    let shared = InvocationId::new("req_shared_across_jobs".into()).unwrap();
    let second_origin = InvocationId::new("req_second_job_original".into()).unwrap();
    let first_id = "a".repeat(64);
    let second_id = "b".repeat(64);
    let result_ref = format!("sha256:{}", "c".repeat(64));
    let first_key = IdempotencyKey::new("original-collision-key".into()).unwrap();
    let second_key = IdempotencyKey::new("retry-collision-key".into()).unwrap();

    let (first, fresh, _) = jobs
        .enqueue_semantic_with_origin(
            first_key,
            first_id.clone(),
            "semantic-first".into(),
            Some(shared.clone()),
        )
        .unwrap();
    assert!(fresh);
    jobs.update(
        &first.id,
        JobState::Completed,
        Some(result_ref.clone()),
        None,
    )
    .unwrap();
    let (second, fresh, _) = jobs
        .enqueue_semantic_with_origin(
            second_key.clone(),
            second_id.clone(),
            "semantic-second".into(),
            Some(second_origin.clone()),
        )
        .unwrap();
    assert!(fresh);
    jobs.update(
        &second.id,
        JobState::Failed,
        None,
        Some("private=DO_NOT_DISCLOSE".into()),
    )
    .unwrap();
    assert_eq!(
        jobs.diagnostic_by_invocation(shared.as_str())["job"]["id"],
        first_id
    );

    // The replay is proven by the same key and semantic hash, not by a
    // request-ID-only association or an artificially seeded Projection.
    let (replayed, fresh, _) = jobs
        .enqueue_semantic_with_origin(
            second_key,
            second_id.clone(),
            "semantic-second".into(),
            Some(shared.clone()),
        )
        .unwrap();
    assert!(!fresh);
    assert_eq!(replayed.id, second.id);
    assert_eq!(replayed.origin_invocation, Some(second_origin.clone()));
    let link_count: i64 = jobs
        .db
        .lock()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM research_invocation_links WHERE invocation=?1 AND job_id=?2",
            [shared.as_str(), second_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(link_count, 1);

    let assert_fail_closed = |jobs: &Jobs| {
        let collision = jobs.diagnostic_by_invocation(shared.as_str());
        assert_eq!(collision["state"], "NOT_EVALUATED");
        assert_eq!(collision["reason_code"], "AMBIGUOUS_REQUEST_ORIGIN");
        assert_eq!(collision["next_action"], "supply_exact_job_id");
        assert_eq!(collision["history"], "UNKNOWN");
        assert!(collision["job"].is_null());
        assert!(collision.get("matched_request_id").is_none());
        assert!(!collision.to_string().contains(result_ref.as_str()));
        assert!(!collision.to_string().contains("DO_NOT_DISCLOSE"));

        // Recovery is by exact job ID, with no fabricated winner or loss of
        // the two independent durable outcomes.
        assert_eq!(
            jobs.diagnostic_readback(&first_id)["job"]["result_ref"],
            result_ref
        );
        assert_eq!(
            jobs.diagnostic_readback(&second_id)["job"]["state"],
            "failed"
        );
        assert!(
            jobs.diagnostic_readback(&second_id)["job"]
                .get("retry_invocations")
                .is_none()
        );
        assert_eq!(
            jobs.diagnostic_by_invocation(second_origin.as_str())["job"]["id"],
            second_id
        );
    };
    assert_fail_closed(&jobs);
    drop(jobs);
    let restored = Jobs::open(file).unwrap();
    assert_fail_closed(&restored);
    assert_eq!(
        restored.diagnostic_readback(&first_id)["job"]["origin"],
        "durable_startup_subset"
    );
    assert_eq!(
        restored.diagnostic_readback(&second_id)["job"]["origin"],
        "durable_startup_subset"
    );
}

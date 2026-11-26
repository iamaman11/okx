use super::*;
fn path(label: &str) -> PathBuf {
    std::env::temp_dir()
        .join(format!(
            "okx-research-sqlite-{label}-{}-{}.sqlite",
            std::process::id(),
            jiff::Timestamp::now().as_nanosecond()
        ))
        .join("catalog.sqlite")
}
#[test]
fn admission_accepts_public_utf8_keys_and_rejects_invalid_durable_job_ids() {
    assert!(IdempotencyKey::new(String::new()).is_err());
    assert!(IdempotencyKey::new("é".repeat(65)).is_err());
    let jobs = Jobs::open(path("identity-validation")).unwrap();
    let key = IdempotencyKey::new("ключ / one".into()).unwrap();
    let hash = "a".repeat(64);
    let (first, _, _) = jobs
        .enqueue_semantic(key.clone(), hash.clone(), "semantic".into())
        .unwrap();
    let (again, fresh, _) = jobs.enqueue_semantic(key, hash, "semantic".into()).unwrap();
    assert!(!fresh);
    assert_eq!(first.id, again.id);
    let db = jobs.db.lock().unwrap();
    db.execute(
        "UPDATE research_jobs SET id='invalid id' WHERE id=?1",
        [&first.id],
    )
    .unwrap();
    assert!(Jobs::read(&db, "invalid id").is_err());
}
#[test]
fn admission_publishes_cancel_token_before_unlocking_catalog() {
    let file = path("cancel-admission");
    let jobs = Jobs::open(file.clone()).unwrap();
    let held = jobs.cancel.lock().unwrap();
    let submitter = jobs.clone();
    let submit = std::thread::spawn(move || submitter.enqueue("key".into(), "job".into()).unwrap());
    let reader = rusqlite::Connection::open(file).unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let count: i64 = reader
            .query_row(
                "SELECT COUNT(*) FROM research_jobs WHERE id='job'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        if count == 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "admission did not commit"
        );
        std::thread::yield_now();
    }
    assert!(
        jobs.db.try_lock().is_err(),
        "catalog unlocked before token publication"
    );
    let canceller = jobs.clone();
    let cancel = std::thread::spawn(move || canceller.cancel("job", "cancel-key").unwrap());
    drop(held);
    let (_, _, token) = submit.join().unwrap();
    assert_eq!(cancel.join().unwrap().state, State::Cancelled);
    assert!(token.load(Ordering::Acquire));
}
#[test]
fn drain_and_enqueue_share_admission_lock() {
    let file = path("drain-race");
    let jobs = Jobs::open(file).unwrap();
    let gate = Arc::new(std::sync::Barrier::new(3));
    let submit_jobs = jobs.clone();
    let submit_gate = gate.clone();
    let submit = std::thread::spawn(move || {
        submit_gate.wait();
        submit_jobs.enqueue("race-key".into(), "race-hash".into())
    });
    let drain_jobs = jobs.clone();
    let drain_gate = gate.clone();
    let drain = std::thread::spawn(move || {
        drain_gate.wait();
        drain_jobs.begin_drain()
    });
    gate.wait();
    let submitted = submit.join().unwrap();
    let count = drain.join().unwrap().unwrap();
    match submitted {
        Ok((job, _, _)) => {
            assert_eq!(count, 1);
            assert!(jobs.enqueue("race-key".into(), "race-hash".into()).is_ok());
            jobs.update(&job.id, State::Cancelled, None, None).unwrap();
            assert_eq!(jobs.begin_drain().unwrap(), 0);
        }
        Err(error) => {
            assert_eq!(error, "RESEARCH_UPDATING");
            assert_eq!(count, 0);
        }
    }
    assert!(
        matches!(jobs.enqueue("new-key".into(), "new-hash".into()), Err(error) if error == "RESEARCH_UPDATING")
    );
}
#[test]
fn scope_binds_legacy_catalog_and_mismatch_precedes_recovery() {
    let file = path("scope-binding");
    let legacy = Jobs::open(file.clone()).unwrap();
    let (job, _, _) = legacy
        .enqueue("scope-key".into(), "scope-hash".into())
        .unwrap();
    drop(legacy);
    let alice = okx_research_protocol::research_ipc::ResearchScope::new("alice".into()).unwrap();
    let bob = okx_research_protocol::research_ipc::ResearchScope::new("bob".into()).unwrap();
    let jobs = Jobs::open_scoped(file.clone(), &alice).unwrap();
    assert_eq!(jobs.get(&job.id).unwrap().id, job.id);
    jobs.db
        .lock()
        .unwrap()
        .execute(
            "UPDATE research_jobs SET state='running',error=NULL WHERE id=?1",
            [&job.id],
        )
        .unwrap();
    drop(jobs);
    assert!(
        matches!(Jobs::open_scoped(file.clone(), &bob), Err(e) if e == "RESEARCH_SCOPE_MISMATCH")
    );
    let db = Connection::open(&file).unwrap();
    let state: String = db
        .query_row(
            "SELECT state FROM research_jobs WHERE id=?1",
            [&job.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "running");
    drop(db);
    let reopened = Jobs::open_scoped(file, &alice).unwrap();
    assert_eq!(reopened.get(&job.id).unwrap().state, State::Interrupted);
}
#[test]
fn exclusive_root_and_semantic_identity_survive_reopen() {
    let file = path("exclusive-semantic");
    let jobs = Jobs::open(file.clone()).unwrap();
    let (first, _, _) = jobs
        .enqueue_semantic(
            IdempotencyKey::new("key".into()).unwrap(),
            "legacy-shaped-id".into(),
            "semantic-a".into(),
        )
        .unwrap();
    assert!(matches!(Jobs::open(file.clone()), Err(e) if e == "RESEARCH_ROOT_LOCKED"));
    drop(jobs);
    let jobs = Jobs::open(file).unwrap();
    let (replayed, fresh, _) = jobs
        .enqueue_semantic(
            IdempotencyKey::new("key".into()).unwrap(),
            "different-id".into(),
            "semantic-a".into(),
        )
        .unwrap();
    assert!(!fresh);
    assert_eq!(first.id, replayed.id);
    assert!(
        matches!(jobs.enqueue_semantic(IdempotencyKey::new("key".into()).unwrap(), "different-id".into(), "semantic-b".into()), Err(e) if e == "IDEMPOTENCY_CONFLICT")
    );
}
#[test]
fn k9_sql_busy_admission_is_bounded_and_does_not_commit() {
    let file = path("k9-busy-100ms");
    let jobs = Jobs::open(file.clone()).unwrap();
    let blocker = Connection::open(file).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let began = std::time::Instant::now();
    let blocked = jobs.enqueue("idempotent".into(), "new-job".into());
    let elapsed = began.elapsed();
    assert_eq!(blocked.unwrap_err(), "RESEARCH_SQL_BUSY");
    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "elapsed: {elapsed:?}"
    );
    blocker.execute_batch("ROLLBACK").unwrap();
    assert!(jobs.get("new-job").is_err());
    let (accepted, fresh, _) = jobs.enqueue("idempotent".into(), "new-job".into()).unwrap();
    assert!(fresh);
    assert_eq!(accepted.state, State::Queued);
}

#[test]
fn c16_two_active_and_thirty_two_queued_bound_admission() {
    let jobs = Jobs::open(path("c16-capacity")).unwrap();
    for i in 0..34 {
        let id = format!("c16-job-{i}");
        let (job, new, _) = jobs.enqueue(format!("idem-{i}"), id.clone()).unwrap();
        assert!(new);
        assert_eq!(job.id, id);
        if i < 2 {
            jobs.update(&id, State::Running, None, None).unwrap();
        }
    }
    assert_eq!(
        jobs.enqueue("overflow".into(), "c16-overflow".into())
            .unwrap_err(),
        "RESOURCE_BUSY:RESEARCH_QUEUE_FULL"
    );
    // Repeating an accepted idempotent request does not take another slot.
    assert!(!jobs.enqueue("idem-0".into(), "c16-job-0".into()).unwrap().1);
    jobs.cancel("c16-job-2", "cancel-c16-slot").unwrap();
    assert!(
        jobs.enqueue("overflow".into(), "c16-overflow".into())
            .unwrap()
            .1
    );
}

/// Runs only when selected by the parent OS-kill acceptance test.
/// The child genuinely holds an open SQLite WAL connection until killed.
#[test]
#[ignore]
fn c17_os_kill_child_process() {
    let Ok(file) = std::env::var("OKX_C17_CHILD_SQLITE") else {
        return;
    };
    let phase = std::env::var("OKX_C17_CHILD_PHASE").unwrap();
    let jobs = Jobs::open(PathBuf::from(&file)).unwrap();
    let id = format!("os-kill-{phase}");
    jobs.enqueue(id.clone(), id.clone()).unwrap();
    let evidence = okx_research::ResearchArtifactStore::at(
        PathBuf::from(&file).with_extension("evidence-root"),
    );
    let artifact = evidence
        .publish_evidence(&serde_json::json!({"phase":phase,"quality":"verified"}))
        .unwrap()
        .0;
    if phase == "running" || phase == "cancelled" {
        jobs.update(&id, State::Running, Some(artifact), None)
            .unwrap();
        if phase == "cancelled" {
            let cancelled = jobs.cancel(&id, "os-kill-cancel-key").unwrap();
            assert_eq!(cancelled.state, State::Cancelled);
        }
    } else if phase == "completed" {
        jobs.update(&id, State::Running, None, None).unwrap();
        jobs.update(&id, State::Completed, Some(artifact), None)
            .unwrap();
    }
    let ready = PathBuf::from(&file).with_extension(format!("{phase}.ready"));
    std::fs::write(ready, b"committed").unwrap();
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

#[test]
fn c17_actual_child_os_kill_recovers_wal_and_blocks_false_success() {
    use std::{
        process::{Command, Stdio},
        time::{Duration, Instant},
    };
    let file = path("c17-physical-kill");
    let exe = std::env::current_exe().unwrap();
    let phases = ["queued", "running", "cancelled", "completed"];
    for phase in phases {
        let ready = file.with_extension(format!("{phase}.ready"));
        let mut child = Command::new(&exe)
            .args([
                "--ignored",
                "--exact",
                "analytics::jobs::tests::c17_os_kill_child_process",
                "--nocapture",
            ])
            .env("OKX_C17_CHILD_SQLITE", file.as_os_str())
            .env("OKX_C17_CHILD_PHASE", phase)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(8);
        while !ready.exists() && Instant::now() < deadline {
            assert!(
                child.try_wait().unwrap().is_none(),
                "child exited before evidence commit"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        if !ready.exists() {
            let _ = child.kill();
            let _ = child.wait();
            panic!("OS-KILL_SETUP_TIMEOUT: {phase}");
        }
        // Real OS process termination (TerminateProcess on Windows,
        // SIGKILL on Linux); no cooperative runtime shutdown.
        child.kill().unwrap();
        child.wait().unwrap();
        let restarted = Jobs::open(file.clone()).unwrap();
        let id = format!("os-kill-{phase}");
        let job = restarted.get(&id).unwrap();
        if phase == "completed" {
            assert_eq!(job.state, State::Completed);
            let proof = job.artifact.clone().unwrap();
            let store =
                okx_research::ResearchArtifactStore::at(file.with_extension("evidence-root"));
            let readback: serde_json::Value = store.read_evidence_json(&proof).unwrap();
            assert_eq!(readback["phase"], "completed");
            assert_eq!(
                restarted
                    .update(&id, State::Completed, Some("forged".into()), None)
                    .unwrap_err(),
                "RESEARCH_TERMINAL_IMMUTABLE"
            );
        } else if phase == "cancelled" {
            assert_eq!(job.state, State::Cancelled);
            assert!(
                job.artifact.is_some(),
                "cancelled partial evidence retained"
            );
            restarted
                .update(&id, State::Completed, Some("forged".into()), None)
                .unwrap();
            assert_eq!(restarted.get(&id).unwrap().state, State::Cancelled);
            assert!(!restarted.enqueue(id.clone(), id.clone()).unwrap().1);
            assert_eq!(
                restarted.cancel(&id, "os-kill-cancel-key").unwrap().state,
                State::Cancelled
            );
        } else {
            assert_eq!(job.state, State::Interrupted);
            assert_eq!(
                restarted
                    .update(&id, State::Completed, Some("forged".into()), None)
                    .unwrap_err(),
                "RESEARCH_TERMINAL_IMMUTABLE"
            );
        }
        std::fs::remove_file(ready).unwrap();
    }
    println!(
        "C17_OS_KILL_JSON:{}",
        serde_json::json!({
            "runner_os":std::env::consts::OS,
            "test_binary":exe.file_name().and_then(|name|name.to_str()).unwrap_or("unknown"),
            "phases":phases,
            "kill_mode":"OS process kill + WAL reopen, no graceful shutdown",
            "assertions":"completed immutable; queued/running interrupted; cancelled terminal immutable after SIGKILL; SHA artifact readback",
            "trading_effects":"none"
        })
    );
}

#[test]
fn c17_terminal_manifest_survives_restart_and_rejects_late_overwrite() {
    let file = path("c17-terminal");
    let original = Jobs::open(file.clone()).unwrap();
    original
        .enqueue("c17-idem".into(), "c17-id".into())
        .unwrap();
    original
        .update("c17-id", State::Running, None, None)
        .unwrap();
    original
        .update(
            "c17-id",
            State::Completed,
            Some("sha256:verified".into()),
            None,
        )
        .unwrap();
    let saved = original.get("c17-id").unwrap();
    assert_eq!(saved.expires_at_ms, Some(saved.created_ms + 7 * 86_400_000));
    assert_eq!(
        original
            .update("c17-id", State::Running, None, None)
            .unwrap_err(),
        "RESEARCH_TERMINAL_IMMUTABLE"
    );
    assert_eq!(
        original
            .update(
                "c17-id",
                State::Completed,
                Some("other-result".into()),
                None
            )
            .unwrap_err(),
        "RESEARCH_TERMINAL_IMMUTABLE"
    );
    assert!(matches!(Jobs::open(file.clone()), Err(e) if e == "RESEARCH_ROOT_LOCKED"));
    drop(original);
    let restarted = Jobs::open(file).unwrap();
    assert_eq!(restarted.get("c17-id").unwrap().state, State::Completed);
    assert_eq!(restarted.get("c17-id").unwrap().artifact, saved.artifact);
    assert!(
        !restarted
            .enqueue("c17-idem".into(), "c17-id".into())
            .unwrap()
            .1
    );
    restarted.pin("c17-id", true).unwrap();
    assert!(restarted.get("c17-id").unwrap().expires_at_ms.is_none());
    restarted.pin("c17-id", false).unwrap();
    assert_eq!(
        restarted.get("c17-id").unwrap().expires_at_ms,
        saved.expires_at_ms
    );
    assert!(
        restarted
            .update("c17-id", State::Completed, None, None)
            .is_err()
    );
}

#[test]
fn c17_restart_interrupts_incomplete_without_resurrecting_it() {
    let file = path("c17-crash");
    let before = Jobs::open(file.clone()).unwrap();
    before
        .enqueue("c17-queued".into(), "queued".into())
        .unwrap();
    before
        .enqueue("c17-running".into(), "running".into())
        .unwrap();
    before
        .update(
            "running",
            State::Running,
            Some("partial-artifact".into()),
            None,
        )
        .unwrap();
    assert!(matches!(Jobs::open(file.clone()), Err(e) if e == "RESEARCH_ROOT_LOCKED"));
    drop(before);
    let after = Jobs::open(file).unwrap();
    assert_eq!(after.get("queued").unwrap().state, State::Interrupted);
    assert_eq!(after.get("running").unwrap().state, State::Interrupted);
    assert_eq!(
        after.get("running").unwrap().artifact.as_deref(),
        Some("partial-artifact")
    );
    assert_eq!(
        after
            .update(
                "running",
                State::Completed,
                Some("false-success".into()),
                None
            )
            .unwrap_err(),
        "RESEARCH_TERMINAL_IMMUTABLE"
    );
    assert!(
        !after
            .enqueue("c17-running".into(), "running".into())
            .unwrap()
            .1
    );
}

#[test]
fn restart_marks_job_interrupted() {
    let p = path("restart");
    let s = Jobs::open(p.clone()).unwrap();
    s.enqueue("fixture-key".into(), "fixture".into()).unwrap();
    s.update("fixture", State::Running, None, None).unwrap();
    drop(s);
    let t = Jobs::open(p.clone()).unwrap();
    assert_eq!(t.get("fixture").unwrap().state, State::Interrupted);
}
#[test]
fn t14_executor_cannot_forge_queue_recovery_or_inconsistent_terminal_state() {
    let jobs = Jobs::open(path("t14-transitions")).unwrap();
    let id = "f".repeat(64);
    jobs.enqueue("t14-key".into(), id.clone()).unwrap();
    for forbidden in [State::Queued, State::Interrupted] {
        assert_eq!(
            jobs.update(&id, forbidden, None, None).unwrap_err(),
            "RESEARCH_TRANSITION_INVALID"
        );
    }
    assert_eq!(
        jobs.update(
            &id,
            State::Completed,
            Some("a".repeat(64)),
            Some("error".into())
        )
        .unwrap_err(),
        "RESEARCH_COMPLETED_ERROR_UNEXPECTED"
    );
    assert_eq!(jobs.get(&id).unwrap().state, State::Queued);
    jobs.update(&id, State::Running, None, None).unwrap();
    jobs.update(&id, State::Completed, Some("a".repeat(64)), None)
        .unwrap();
    assert_eq!(jobs.get(&id).unwrap().state, State::Completed);
    assert_eq!(
        jobs.update(&id, State::Running, None, None).unwrap_err(),
        "RESEARCH_TERMINAL_IMMUTABLE"
    );
}
#[test]
fn wrapped_source_throttle_is_retryable() {
    assert!(is_transient_rate_defer(Some(
        "public OKX market API error: OKX rate/backpressure defer: LocalBudget"
    )));
    assert!(!is_transient_rate_defer(Some("CANDLE_INTERIOR_GAP")));
}
#[test]
fn transient_rate_defer_is_requeued_instead_of_cached_as_failed() {
    let p = path("retry");
    let s = Jobs::open(p).unwrap();
    s.enqueue("fixture-key".into(), "rate-defer".into())
        .unwrap();
    s.update(
        "rate-defer",
        State::Failed,
        None,
        Some("OKX rate/backpressure defer: fixture".into()),
    )
    .unwrap();
    let (job, new, _) = s
        .enqueue("fixture-key".into(), "rate-defer".into())
        .unwrap();
    assert!(new);
    assert_eq!(job.state, State::Queued);
    assert!(job.error.is_none());
    s.update(
        "rate-defer",
        State::Failed,
        None,
        Some("SOURCE_INVALID".into()),
    )
    .unwrap();
    assert!(
        !s.enqueue("fixture-key".into(), "rate-defer".into())
            .unwrap()
            .1
    );
}
#[test]
fn new_catalog_starts_without_old_json() {
    let p = path("strict");
    let obsolete = p.with_extension("json");
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(&obsolete, b"{malformed obsolete catalog").unwrap();
    let jobs = Jobs::open(p.clone()).unwrap();
    assert!(jobs.get("old").is_err());
    jobs.enqueue("new-idempotency".into(), "new-id".into())
        .unwrap();
    assert_eq!(jobs.get("new-id").unwrap().state, State::Queued);
    assert!(obsolete.is_file()); // not loaded, migrated or silently deleted
    let _ = std::fs::remove_file(obsolete);
}
#[test]
fn cancelled_partial_job_retention_tombstones_before_delete() {
    let catalog = Jobs::open(path("cancel-expire")).unwrap();
    catalog
        .enqueue("cancel-key".into(), "cancelled-job".into())
        .unwrap();
    catalog
        .update(
            "cancelled-job",
            State::Running,
            Some("partial-artifact".into()),
            None,
        )
        .unwrap();
    let cancelled = catalog.cancel("cancelled-job", "cancel-receipt").unwrap();
    assert_eq!(cancelled.state, State::Cancelled);
    assert_eq!(cancelled.artifact.as_deref(), Some("partial-artifact"));
    // General late async writes cannot mutate a cancelled job.
    catalog
        .update(
            "cancelled-job",
            State::Completed,
            Some("late-result".into()),
            None,
        )
        .unwrap();
    assert_eq!(
        catalog.get("cancelled-job").unwrap().state,
        State::Cancelled
    );
    // The dedicated terminal CAS is allowed to expire this reference.
    catalog
        .expire_terminal_artifact("cancelled-job", "partial-artifact")
        .unwrap();
    let after = catalog.get("cancelled-job").unwrap();
    assert_eq!(after.state, State::Cancelled);
    assert!(after.artifact.is_none());
    assert_eq!(
        after.error.as_deref(),
        Some("RESULT_EXPIRED_BY_7_DAY_RETENTION")
    );
    assert!(
        catalog
            .expire_terminal_artifact("cancelled-job", "partial-artifact")
            .is_err()
    );
}
#[test]
fn expired_result_returns_expired_not_rerun() {
    let p = path("expiry");
    let jobs = Jobs::open(p.clone()).unwrap();
    jobs.enqueue("key".into(), "hash".into()).unwrap();
    jobs.update(
        "hash",
        State::Completed,
        Some("immutable-result".into()),
        None,
    )
    .unwrap();
    jobs.expire_terminal_artifact("hash", "immutable-result")
        .unwrap();
    let (old, spawn, _) = jobs.enqueue("key".into(), "hash".into()).unwrap();
    assert!(!spawn);
    assert_eq!(
        old.error.as_deref(),
        Some("RESULT_EXPIRED_BY_7_DAY_RETENTION")
    );
    assert_eq!(old.artifact, None);
    assert_eq!(old.state, State::Completed);
}
#[tokio::test(flavor = "current_thread")]
async fn research_io_does_not_block_trade_loop() {
    let jobs = Jobs::open(path("nonblocking")).unwrap();
    jobs.enqueue("key".into(), "job".into()).unwrap();
    let holder = jobs.clone();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        let _lock = holder.db.lock().unwrap();
        ready_tx.send(()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(160));
    });
    ready_rx.recv().unwrap();
    let operation = jobs.get_async("job".into());
    tokio::pin!(operation);
    let timer = tokio::time::sleep(std::time::Duration::from_millis(25));
    tokio::pin!(timer);
    tokio::select! {
        _=&mut timer=>{},
        _=&mut operation=>panic!("SQLite lock unexpectedly ran on Tokio event-loop thread"),
    }
    thread.join().unwrap();
    assert_eq!(operation.await.unwrap().state, State::Queued);
}
#[test]
fn job_status_reads_bounded_rows_at_100k_jobs() {
    let p = path("load");
    let s = Jobs::open(p.clone()).unwrap();
    {
        let mut db = s.db.lock().unwrap();
        let tx = db.transaction().unwrap();
        {
            let mut stmt = tx.prepare_cached(
                    "INSERT INTO research_jobs(id,request_hash,idempotency_key,state,progress,created_ms,pinned)
                     VALUES(?1,?1,?2,'completed',100,0,0)",
                ).unwrap();
            for i in 0..100_000 {
                stmt.execute(params![format!("job_{i:06}"), format!("key_{i:06}")])
                    .unwrap();
            }
        }
        tx.commit().unwrap();
    }
    let connection = s.db.lock().unwrap();
    let mut stmt = connection.prepare(
            "EXPLAIN QUERY PLAN SELECT id,request_hash,idempotency_key,state,progress,artifact,error,created_ms,pinned,decision_artifact
             FROM research_jobs WHERE id=?1",
        ).unwrap();
    let plan = stmt
        .query_map(["job_099999"], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    drop(stmt);
    drop(connection);
    assert!(plan.iter().any(|detail| detail.contains("INDEX")));
    for i in [0, 1, 50_000, 99_999] {
        assert_eq!(
            s.get(&format!("job_{i:06}")).unwrap().state,
            State::Completed
        );
    }
    assert_eq!(s.retention_candidates(u64::MAX, 64).unwrap().len(), 0);
    {
        let db = s.db.lock().unwrap();
        let plan = db
            .prepare("EXPLAIN QUERY PLAN SELECT id,state FROM research_jobs ORDER BY created_ms DESC,id DESC LIMIT 129")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
            .join(" ");
        assert!(
            plan.contains("idx_research_jobs_diagnostics_recent"),
            "{plan}"
        );
        assert!(!plan.contains("TEMP B-TREE"), "{plan}");
    }
    drop(s);
    let reopened = Jobs::open(p).unwrap();
    let latest = reopened.diagnostic_readback("job_099999");
    assert_eq!(latest["job"]["state"], "completed");
    assert_eq!(latest["job"]["origin"], "durable_startup_subset");
    assert_eq!(latest["retained"], 128);
    assert_eq!(latest["startup_subset_truncated"], true);
    assert_eq!(
        reopened.diagnostic_readback("job_000000")["reason_code"],
        "UNKNOWN_NOT_RETAINED"
    );
}

#[test]
fn producer_and_origin_survive_replay_failure_and_reopen() {
    let file = path("durable-producer");
    let first =
        crate::producer::Producer::new("a".repeat(64), Some("b".repeat(40)), Some("c".repeat(40)))
            .unwrap();
    let second = crate::producer::Producer::new("d".repeat(64), None, None).unwrap();
    let origin =
        okx_research_protocol::research_ipc::InvocationId::new("first-invocation".into()).unwrap();
    let jobs = Jobs::open(file.clone())
        .unwrap()
        .with_producer(Some(first.clone()));
    let key = IdempotencyKey::new("producer-key".into()).unwrap();
    let (job, _, _) = jobs
        .enqueue_semantic_with_origin(
            key.clone(),
            "e".repeat(64),
            "semantic".into(),
            Some(origin.clone()),
        )
        .unwrap();
    assert_eq!(job.producer, Some(first.clone()));
    jobs.update(
        &job.id,
        State::Failed,
        None,
        Some("CANDLE_RANGE_UNAVAILABLE".into()),
    )
    .unwrap();
    drop(jobs);
    let jobs = Jobs::open(file).unwrap().with_producer(Some(second));
    let restored = jobs.diagnostic_readback(&job.id);
    assert_eq!(restored["job"]["origin_invocation"], origin.as_str());
    assert_eq!(
        restored["job"]["producer"],
        serde_json::to_value(&first).unwrap()
    );
    assert_eq!(restored["job"]["origin"], "durable_startup_subset");
    let (replayed, fresh, _) = jobs
        .enqueue_semantic_with_origin(key, job.id.clone(), "semantic".into(), None)
        .unwrap();
    assert!(!fresh);
    assert_eq!(replayed.producer, Some(first));
    assert_eq!(replayed.origin_invocation, Some(origin.clone()));
    assert_eq!(replayed.state, State::Failed);
    assert_eq!(replayed.error.as_deref(), Some("CANDLE_RANGE_UNAVAILABLE"));
    assert_eq!(
        jobs.diagnostic_readback(&job.id)["job"]["origin_invocation"],
        origin.as_str()
    );
}

#[test]
fn edge_request_diagnostic_is_bounded_nonblocking_and_ambiguous_fail_closed() {
    let file = path("edge-request-diagnostic");
    let origin =
        okx_research_protocol::research_ipc::InvocationId::new("req_edge_original_01".into())
            .unwrap();
    let jobs = Jobs::open(file.clone()).unwrap();
    let key = IdempotencyKey::new("origin-first".into()).unwrap();
    let (first, _, _) = jobs
        .enqueue_semantic_with_origin(
            key,
            "a".repeat(64),
            "semantic-1".into(),
            Some(origin.clone()),
        )
        .unwrap();
    jobs.update(
        &first.id,
        State::Failed,
        None,
        Some("secret=DO_NOT_DISCLOSE".into()),
    )
    .unwrap();
    assert_eq!(
        jobs.diagnostic_by_invocation(origin.as_str())["job"]["id"],
        first.id
    );
    assert_eq!(
        jobs.diagnostic_by_invocation(origin.as_str())["matched_request_id"],
        origin.as_str()
    );
    assert_eq!(
        jobs.diagnostic_by_invocation(origin.as_str())["job"]["error_code"],
        "UNCLASSIFIED_RESEARCH_ERROR"
    );
    let lock = jobs.db.lock().unwrap();
    let reader = jobs.clone();
    let request = origin.as_str().to_owned();
    let (tx, rx) = std::sync::mpsc::channel();
    let thread =
        std::thread::spawn(move || tx.send(reader.diagnostic_by_invocation(&request)).unwrap());
    let result = rx.recv_timeout(std::time::Duration::from_millis(200));
    drop(lock);
    thread.join().unwrap();
    assert_eq!(result.unwrap()["state"], "OBSERVED");

    // A request may have produced more than one job: no guessed winner.
    let (second, _, _) = jobs
        .enqueue_semantic_with_origin(
            IdempotencyKey::new("origin-second".into()).unwrap(),
            "b".repeat(64),
            "semantic-2".into(),
            Some(origin.clone()),
        )
        .unwrap();
    let ambiguous = jobs.diagnostic_by_invocation(origin.as_str());
    assert_eq!(ambiguous["state"], "NOT_EVALUATED");
    assert_eq!(ambiguous["reason_code"], "AMBIGUOUS_REQUEST_ORIGIN");
    assert!(ambiguous["job"].is_null());
    assert!(ambiguous.get("matched_request_id").is_none());
    assert_eq!(ambiguous["next_action"], "supply_exact_job_id");
    assert!(!ambiguous.to_string().contains("DO_NOT_DISCLOSE"));
    assert_eq!(jobs.diagnostic_readback(&second.id)["state"], "OBSERVED");
    drop(jobs);
    let restored = Jobs::open(file).unwrap();
    assert_eq!(
        restored.diagnostic_by_invocation(origin.as_str())["reason_code"],
        "AMBIGUOUS_REQUEST_ORIGIN"
    );
    assert_eq!(
        restored.diagnostic_by_invocation("req_never_observed")["reason_code"],
        "UNKNOWN_NOT_RETAINED"
    );
    assert!(
        restored
            .diagnostic_by_invocation("req_never_observed")
            .get("matched_request_id")
            .is_none()
    );
    assert!(
        restored
            .diagnostic_by_invocation("req/invalid")
            .get("matched_request_id")
            .is_none()
    );
}

#[test]
fn idempotent_retry_links_each_observed_request_without_rewriting_job_origin() {
    use okx_research_protocol::research_ipc::InvocationId;
    let file = path("retry-invocations");
    let first = InvocationId::new("req_before_lost_reply".into()).unwrap();
    let retry = InvocationId::new("req_after_lost_reply".into()).unwrap();
    let unrelated = InvocationId::new("req_conflicting_semantics".into()).unwrap();
    let jobs = Jobs::open(file.clone()).unwrap();
    let key = IdempotencyKey::new("retry-safe-key".into()).unwrap();
    let (original, started, _) = jobs
        .enqueue_semantic_with_origin(
            key.clone(),
            "c".repeat(64),
            "same-semantic".into(),
            Some(first.clone()),
        )
        .unwrap();
    assert!(started);
    let (replayed, restarted, _) = jobs
        .enqueue_semantic_with_origin(
            key.clone(),
            "c".repeat(64),
            "same-semantic".into(),
            Some(retry.clone()),
        )
        .unwrap();
    assert!(!restarted);
    assert_eq!(replayed.id, original.id);
    assert_eq!(replayed.origin_invocation, Some(first.clone()));
    assert_eq!(
        jobs.diagnostic_by_invocation(first.as_str())["job"]["id"],
        original.id
    );
    assert_eq!(
        jobs.diagnostic_by_invocation(first.as_str())["matched_request_id"],
        first.as_str()
    );
    let retried_trace = jobs.diagnostic_by_invocation(retry.as_str());
    assert_eq!(retried_trace["job"]["id"], original.id);
    assert_eq!(retried_trace["reference_kind"], "request_id");
    assert_eq!(retried_trace["matched_request_id"], retry.as_str());
    assert_eq!(retried_trace["job"]["origin_invocation"], first.as_str());
    assert!(
        jobs.diagnostic_readback(&original.id)
            .get("matched_request_id")
            .is_none()
    );
    assert!(matches!(
        jobs.enqueue_semantic_with_origin(
            key,
            "d".repeat(64),
            "different-semantic".into(),
            Some(unrelated.clone()),
        ),
        Err(error) if error == "IDEMPOTENCY_CONFLICT"
    ));
    assert_eq!(
        jobs.diagnostic_by_invocation(unrelated.as_str())["reason_code"],
        "UNKNOWN_NOT_RETAINED"
    );
    assert!(
        jobs.diagnostic_by_invocation(unrelated.as_str())
            .get("matched_request_id")
            .is_none()
    );
    drop(jobs);
    let reopened = Jobs::open(file).unwrap();
    assert_eq!(
        reopened.diagnostic_by_invocation(first.as_str())["job"]["id"],
        original.id
    );
    assert_eq!(
        reopened.diagnostic_by_invocation(first.as_str())["matched_request_id"],
        first.as_str()
    );
    assert_eq!(
        reopened.diagnostic_by_invocation(retry.as_str())["job"]["id"],
        original.id
    );
    assert_eq!(
        reopened.diagnostic_by_invocation(retry.as_str())["matched_request_id"],
        retry.as_str()
    );
    assert_eq!(
        reopened.get(&original.id).unwrap().origin_invocation,
        Some(first)
    );
}

#[test]
fn retry_request_alias_window_is_bounded_and_unknown_is_not_fabricated() {
    use okx_research_protocol::research_ipc::InvocationId;
    let file = path("bounded-retry-links");
    let jobs = Jobs::open(file.clone()).unwrap();
    let key = IdempotencyKey::new("bounded-alias-key".into()).unwrap();
    let hash = "e".repeat(64);
    jobs.enqueue_semantic_with_origin(
        key.clone(),
        hash.clone(),
        "semantic".into(),
        Some(InvocationId::new("req_original".into()).unwrap()),
    )
    .unwrap();
    for index in 0..20 {
        let invocation = InvocationId::new(format!("req_retry_{index:02}")).unwrap();
        let (_, is_new, _) = jobs
            .enqueue_semantic_with_origin(
                key.clone(),
                hash.clone(),
                "semantic".into(),
                Some(invocation),
            )
            .unwrap();
        assert!(!is_new);
    }
    assert_eq!(
        jobs.diagnostic_by_invocation("req_retry_00")["reason_code"],
        "UNKNOWN_NOT_RETAINED"
    );
    assert_eq!(
        jobs.diagnostic_by_invocation("req_retry_19")["job"]["id"],
        hash
    );
    let count: i64 = jobs
        .db
        .lock()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM research_invocation_links WHERE job_id=?1",
            [&hash],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, MAX_ORIGIN_ALIASES_PER_JOB);
    // Replaying a retained request refreshes its observation recency.
    for request in ["req_retry_04", "req_retry_20"] {
        let invocation = InvocationId::new(request.into()).unwrap();
        jobs.enqueue_semantic_with_origin(
            key.clone(),
            hash.clone(),
            "semantic".into(),
            Some(invocation),
        )
        .unwrap();
    }
    assert_eq!(
        jobs.diagnostic_by_invocation("req_retry_04")["job"]["id"],
        hash
    );
    assert_eq!(
        jobs.diagnostic_by_invocation("req_retry_05")["reason_code"],
        "UNKNOWN_NOT_RETAINED"
    );
    drop(jobs);
    let reopened = Jobs::open(file).unwrap();
    assert_eq!(
        reopened.diagnostic_by_invocation("req_original")["job"]["id"],
        hash
    );
    assert_eq!(
        reopened.diagnostic_by_invocation("req_retry_00")["reason_code"],
        "UNKNOWN_NOT_RETAINED"
    );
    assert_eq!(
        reopened.diagnostic_by_invocation("req_retry_19")["job"]["id"],
        hash
    );
    assert_eq!(
        reopened.diagnostic_by_invocation("req_retry_04")["job"]["id"],
        hash
    );
}

#[test]
fn historical_rows_keep_unknown_producer_on_replay() {
    let file = path("legacy-producer");
    std::fs::create_dir_all(file.parent().unwrap()).unwrap();
    let db = rusqlite::Connection::open(&file).unwrap();
    db.execute_batch("CREATE TABLE research_jobs(id TEXT PRIMARY KEY,request_hash TEXT NOT NULL,idempotency_key TEXT NOT NULL UNIQUE,state TEXT NOT NULL,progress INTEGER NOT NULL,artifact TEXT,error TEXT,created_ms INTEGER NOT NULL,pinned INTEGER NOT NULL) STRICT;").unwrap();
    let id = "f".repeat(64);
    db.execute("INSERT INTO research_jobs VALUES(?1,?1,'legacy-key','failed',0,NULL,'CANDLE_RANGE_UNAVAILABLE',1,0)", [&id]).unwrap();
    drop(db);
    let jobs = Jobs::open(file.clone()).unwrap();
    let job = jobs.get(&id).unwrap();
    assert!(job.producer.is_none());
    drop(jobs);
    let jobs = Jobs::open(file).unwrap().with_producer(Some(
        crate::producer::Producer::new("a".repeat(64), None, None).unwrap(),
    ));
    let (again, _, _) = jobs.enqueue("legacy-key".into(), job.id).unwrap();
    assert!(again.producer.is_none());
    assert!(again.origin_invocation.is_none());
    let diagnostic = jobs.diagnostic_readback(&again.id);
    assert!(diagnostic["job"]["producer"].is_null());
    assert!(diagnostic["job"]["origin_invocation"].is_null());
}

#[test]
fn existing_catalog_requires_matching_scope_before_recovery() {
    let file = path("existing-adoption");
    let alice = okx_research_protocol::research_ipc::ResearchScope::new("alice".into()).unwrap();
    let bob = okx_research_protocol::research_ipc::ResearchScope::new("bob".into()).unwrap();
    assert!(
        matches!(Jobs::open_existing_scoped(file.clone(), &alice), Err(e) if e == "RESEARCH_CATALOG_MISSING")
    );
    assert!(!file.exists());
    let legacy = Jobs::open(file.clone()).unwrap();
    let (job, _, _) = legacy.enqueue("key".into(), "hash".into()).unwrap();
    drop(legacy);
    assert!(
        matches!(Jobs::open_existing_scoped(file.clone(), &alice), Err(e) if e == "RESEARCH_CATALOG_SCOPE_UNBOUND")
    );
    let scoped = Jobs::open_scoped(file.clone(), &alice).unwrap();
    scoped
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE research_jobs SET state='running',error=NULL WHERE id=?1",
            [&job.id],
        )
        .unwrap();
    drop(scoped);
    assert!(
        matches!(Jobs::open_existing_scoped(file.clone(), &bob), Err(e) if e == "RESEARCH_SCOPE_MISMATCH")
    );
    let db = Connection::open(&file).unwrap();
    let state: String = db
        .query_row(
            "SELECT state FROM research_jobs WHERE id=?1",
            [&job.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "running");
    drop(db);
    let adopted = Jobs::open_existing_scoped(file.clone(), &alice).unwrap();
    assert_eq!(adopted.get(&job.id).unwrap().state, State::Interrupted);
    let (same, fresh, _) = adopted.enqueue("key".into(), "hash".into()).unwrap();
    assert_eq!(same.id, job.id);
    assert!(!fresh);
    drop(adopted);
    assert_eq!(
        Jobs::open_existing_scoped(file, &alice)
            .unwrap()
            .get(&job.id)
            .unwrap()
            .id,
        job.id
    );
}

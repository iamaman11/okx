//! Windows kernel Job acceptance for separate CONTROL-owned process lifetimes.
//! This uses harmless Rust test children, not an installed trading Engine or
//! real Research binary. It is CODE evidence, never physical installation proof.
#![cfg(windows)]

use okx_host_controller::{
    job::AgentJob,
    research_supervisor::ResearchSupervisor,
};
use okx_host_protocol::{ResearchCatalogMode, ResearchRuntimePhase, ResearchRuntimeStatus};
use std::{
    fs,
    io::{Read, Write},
    path::PathBuf,
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant, SystemTime},
};

struct IsolatedRoots {
    root: PathBuf,
    control: PathBuf,
    runtime: PathBuf,
}

impl IsolatedRoots {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "okx-research-owner-acceptance-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&root).unwrap();
        let control = root.join("control");
        let runtime = root.join("runtime");
        fs::create_dir(&control).unwrap();
        fs::create_dir(&runtime).unwrap();
        Self {
            root,
            control,
            runtime,
        }
    }
}

impl Drop for IsolatedRoots {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn harmless_job_child() {
    if std::env::var_os("OKX_RESEARCH_JOB_TEST_CHILD").is_some() {
        thread::sleep(Duration::from_secs(25));
    }
}

#[test]
fn gated_nested_job_child() {
    let Some(marker) = std::env::var_os("OKX_RESEARCH_NESTED_JOB_TEST_MARKER") else {
        return;
    };
    // The child must not even create its nested Windows Job before CONTROL
    // assigns the parent Job and writes its exact readiness receipt.
    let mut receipt = Vec::new();
    std::io::stdin().take(129).read_to_end(&mut receipt).unwrap();
    if receipt != b"RESEARCH_HOST_JOB_ASSIGNED\n" {
        return;
    }
    use windows_sys::Win32::{
        Foundation::CloseHandle,
        System::{
            JobObjects::{AssignProcessToJobObject, CreateJobObjectW},
            Threading::GetCurrentProcess,
        },
    };
    let nested_job = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
    assert!(!nested_job.is_null(), "nested test Job creation failed");
    let assigned = unsafe { AssignProcessToJobObject(nested_job, GetCurrentProcess()) };
    assert_ne!(assigned, 0, "nested Job assignment after parent gate failed");
    fs::write(PathBuf::from(marker), b"nested-job-ready").unwrap();
    thread::sleep(Duration::from_secs(25));
    unsafe { CloseHandle(nested_job) };
}

fn fixture_process_in_job() -> (AgentJob, Child) {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "harmless_job_child"])
        .env("OKX_RESEARCH_JOB_TEST_CHILD", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let job = AgentJob::new().unwrap();
    job.assign(&mut child).unwrap();
    (job, child)
}

fn exited_within(child: &mut Child, deadline: Duration) -> Option<ExitStatus> {
    let until = Instant::now() + deadline;
    loop {
        if let Some(outcome) = child.try_wait().unwrap() {
            return Some(outcome);
        }
        if Instant::now() >= until {
            return None;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn write_pointer_claiming_sha(runtime: &std::path::Path, expected: &str) {
    fs::write(
        runtime.join("installed-research.json"),
        serde_json::to_vec(&serde_json::json!({
            "schema": "okx.host-control.installed-research/v1",
            "repository_id": 1_388_071_566u64,
            "run_id": 1,
            "artifact_id": 1,
            "source_head_sha": "b".repeat(40),
            "source_tree": "c".repeat(40),
            "worker_sha256": expected,
            "protocol_version": 1,
            "storage_version": 1
        }))
        .unwrap(),
    )
    .unwrap();
}

#[test]
fn one_process_job_can_be_terminated_without_touching_the_other() {
    let (engine_job, mut engine_child) = fixture_process_in_job();
    let (research_job, mut research_child) = fixture_process_in_job();

    assert!(!engine_job.is_empty().unwrap());
    assert!(!research_job.is_empty().unwrap());

    research_job.terminate().unwrap();
    let research_exit = exited_within(&mut research_child, Duration::from_secs(5));
    assert!(research_exit.is_some(), "Research test child survived its Job termination");
    assert!(research_job.is_empty().unwrap());
    assert!(engine_child.try_wait().unwrap().is_none(), "Research Job terminated Engine Job child");

    engine_job.terminate().unwrap();
    let engine_exit = exited_within(&mut engine_child, Duration::from_secs(5));
    assert!(engine_exit.is_some(), "Engine test child survived its Job termination");
    assert!(engine_job.is_empty().unwrap());
}

#[test]
fn nested_job_assignment_waits_for_parent_ready_receipt() {
    let roots = IsolatedRoots::new();
    let marker = roots.control.join("nested-job-ready.txt");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "gated_nested_job_child"])
        .env("OKX_RESEARCH_NESTED_JOB_TEST_MARKER", &marker)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    assert!(!marker.exists(), "child bypassed the parent assignment gate");
    assert!(child.try_wait().unwrap().is_none());
    let parent_job = AgentJob::new().unwrap();
    parent_job.assign(&mut child).unwrap();
    assert!(!marker.exists(), "child created nested Job before host receipt");

    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(b"RESEARCH_HOST_JOB_ASSIGNED\n").unwrap();
    stdin.flush().unwrap();
    drop(stdin);

    let until = Instant::now() + Duration::from_secs(5);
    while !marker.exists() && Instant::now() < until {
        if let Some(exit) = child.try_wait().unwrap() {
            panic!("nested Job child exited before readiness, exit={exit}");
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(fs::read(&marker).unwrap(), b"nested-job-ready");
    assert!(!parent_job.is_empty().unwrap());

    parent_job.terminate().unwrap();
    assert!(exited_within(&mut child, Duration::from_secs(5)).is_some());
    assert!(parent_job.is_empty().unwrap());
}

#[test]
fn research_control_root_is_independent_and_start_requires_verified_installation() {
    let roots = IsolatedRoots::new();
    fs::write(roots.control.join("engine-state.pb"), b"invalid Engine state").unwrap();
    let financial_root = roots.runtime.join("analytics");
    fs::create_dir(&financial_root).unwrap();
    let financial_file = financial_root.join("finance-unchanged.sqlite");
    fs::write(&financial_file, b"engine-owned-sentinel").unwrap();

    let mut supervisor =
        ResearchSupervisor::at(roots.control.clone(), roots.runtime.clone()).unwrap();
    let desired_path = roots.control.join("research-state.json");
    let status: ResearchRuntimeStatus =
        serde_json::from_value(supervisor.status().unwrap()).unwrap();
    assert_eq!(status.phase, ResearchRuntimePhase::Stopped);
    assert_eq!(status.catalog_mode, ResearchCatalogMode::HostIndependent);
    assert!(!desired_path.exists(), "status must not install or create desired state");
    assert_eq!(
        supervisor.start(&"a".repeat(64)).unwrap_err().code(),
        "ARTIFACT_VERIFICATION_FAILED"
    );
    assert!(!desired_path.exists(), "failed image verification persisted desired running");
    supervisor.reconcile().unwrap();
    assert_eq!(fs::read(&financial_file).unwrap(), b"engine-owned-sentinel");
    assert!(!roots.runtime.join("research/edge-catalog").exists());
}

#[test]
fn corrupted_and_hash_mismatched_research_image_never_launch() {
    let roots = IsolatedRoots::new();
    let expected = "a".repeat(64);
    let mut supervisor =
        ResearchSupervisor::at(roots.control.clone(), roots.runtime.clone()).unwrap();

    fs::write(roots.runtime.join("installed-research.json"), b"not json").unwrap();
    assert!(supervisor.start(&expected).is_err());
    let corrupt: ResearchRuntimeStatus =
        serde_json::from_value(supervisor.status().unwrap()).unwrap();
    assert_eq!(corrupt.phase, ResearchRuntimePhase::InvalidProvenance);
    assert_eq!(corrupt.last_error.as_deref(), Some("RESEARCH_IMAGE_INVALID"));
    assert!(!corrupt.desired_running);
    assert!(corrupt.pid.is_none());

    let image_dir = roots.runtime.join("research/versions").join(&expected);
    fs::create_dir_all(&image_dir).unwrap();
    fs::write(image_dir.join("okx-research-service.exe"), b"hash mismatch").unwrap();
    write_pointer_claiming_sha(&roots.runtime, &expected);
    assert_eq!(
        supervisor.start(&expected).unwrap_err().code(),
        "ARTIFACT_HASH_MISMATCH"
    );
    let mismatch: ResearchRuntimeStatus =
        serde_json::from_value(supervisor.status().unwrap()).unwrap();
    assert_eq!(mismatch.phase, ResearchRuntimePhase::InvalidProvenance);
    assert_eq!(mismatch.last_error.as_deref(), Some("RESEARCH_IMAGE_INVALID"));
    assert!(!mismatch.desired_running);
    assert!(mismatch.pid.is_none());
    assert!(!roots.control.join("research-state.json").exists());
    assert!(!roots.runtime.join("research/edge-catalog").exists());
}

#[test]
fn standalone_research_status_is_typed_and_does_not_claim_edge_online() {
    let roots = IsolatedRoots::new();
    let mut supervisor =
        ResearchSupervisor::at(roots.control.clone(), roots.runtime.clone()).unwrap();
    let status: ResearchRuntimeStatus =
        serde_json::from_value(supervisor.status().unwrap()).unwrap();
    assert_eq!(status.phase, ResearchRuntimePhase::Stopped);
    assert_eq!(status.catalog_mode, ResearchCatalogMode::HostIndependent);
    assert!(!status.desired_running);
    assert!(status.pid.is_none());
    assert!(status.active_worker_sha256.is_none());
    assert_eq!(status.last_error.as_deref(), Some("RESEARCH_NOT_INSTALLED"));
    let status_wire = serde_json::to_value(&status).unwrap();
    assert!(status_wire.get("edge_online").is_none());
    assert!(status_wire.get("edge_handshake_accepted").is_none());

    fs::write(roots.control.join("engine-state.pb"), b"corrupt engine state").unwrap();
    supervisor.reconcile().unwrap();
    assert_eq!(
        supervisor.start(&"a".repeat(64)).unwrap_err().code(),
        "ARTIFACT_VERIFICATION_FAILED"
    );
    let stopped: ResearchRuntimeStatus =
        serde_json::from_value(supervisor.status().unwrap()).unwrap();
    assert!(!stopped.desired_running);
    assert!(stopped.pid.is_none());
    assert!(!roots.runtime.join("research/edge-catalog").exists());
    supervisor.shutdown().unwrap();
}

#[test]
fn standalone_research_lifecycle_requires_exact_image_and_process_generation() {
    let roots = IsolatedRoots::new();
    let expected = "a".repeat(64);
    let mut supervisor =
        ResearchSupervisor::at(roots.control.clone(), roots.runtime.clone()).unwrap();
    let pointer = roots.runtime.join("installed-research.json");

    fs::write(&pointer, b"{\"schema\":\"invalid\"}").unwrap();
    assert!(supervisor.start(&expected).is_err());
    let corrupted: ResearchRuntimeStatus =
        serde_json::from_value(supervisor.status().unwrap()).unwrap();
    assert_eq!(corrupted.phase, ResearchRuntimePhase::InvalidProvenance);
    assert_eq!(corrupted.last_error.as_deref(), Some("RESEARCH_IMAGE_INVALID"));
    assert!(corrupted.pid.is_none());

    let image_dir = roots.runtime.join("research/versions").join(&expected);
    fs::create_dir_all(&image_dir).unwrap();
    fs::write(
        image_dir.join("okx-research-service.exe"),
        b"not-a-real-research-executable",
    )
    .unwrap();
    write_pointer_claiming_sha(&roots.runtime, &expected);
    assert_eq!(
        supervisor.start(&"a".repeat(64)).unwrap_err().code(),
        "ARTIFACT_HASH_MISMATCH"
    );
    assert!(supervisor.status().unwrap()["pid"].is_null());
    assert!(!roots.runtime.join("research/edge-catalog").exists());

    // A process-generation stale Stop must never affect a newer lifetime.
    fs::write(
        roots.control.join("research-state.json"),
        serde_json::to_vec(&serde_json::json!({
            "schema":"okx.host-control.research-desired/v1",
            "desired_running":true,
            "expected_worker_sha256":"a".repeat(64),
            "host_scope":"d".repeat(64),
            "process_generation":7
        }))
        .unwrap(),
    )
    .unwrap();
    drop(supervisor);
    let mut restarted =
        ResearchSupervisor::at(roots.control.clone(), roots.runtime.clone()).unwrap();
    assert_eq!(
        restarted.stop(&"a".repeat(64), 6).unwrap_err().code(),
        "RESEARCH_IDENTITY_MISMATCH"
    );
    assert!(restarted.status().unwrap()["pid"].is_null());
}

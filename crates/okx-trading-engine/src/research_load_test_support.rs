//! Non-shipping, cross-owner T10 load boundary.
//!
//! Research owns the fixture executable and its typed Worker IPC. The Engine
//! tests only drive the external fixture's versioned JSONL control surface;
//! no Research Rust protocol, child lifetime code, or financial authority is
//! imported into this crate.

use serde_json::{Value, json};
use std::{path::PathBuf, process::Stdio, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex,
};

const SCHEMA: &str = "okx.research-load-fixture/v1";

struct Wire {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    sequence: u64,
}

#[derive(Clone)]
pub(crate) struct ResearchLoadFixture {
    wire: Arc<Mutex<Wire>>,
}

impl ResearchLoadFixture {
    pub(crate) async fn launch(root: PathBuf) -> Self {
        let fixture = PathBuf::from(
            std::env::var_os("OKX_RESEARCH_LOAD_FIXTURE_EXE")
                .expect("set OKX_RESEARCH_LOAD_FIXTURE_EXE to the non-shipping Research helper"),
        );
        let worker = PathBuf::from(
            std::env::var_os("OKX_RESEARCH_SERVICE_TEST_EXE")
                .expect("set OKX_RESEARCH_SERVICE_TEST_EXE to the actual Research Worker"),
        );
        assert!(
            fixture.is_file(),
            "Research load fixture executable is missing"
        );
        assert!(worker.is_file(), "Research Worker executable is missing");
        std::fs::create_dir_all(&root).expect("create disposable Research root");
        let mut child = Command::new(&fixture)
            .arg("--root")
            .arg(&root)
            .arg("--worker")
            .arg(&worker)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .expect("launch externally owned Research fixture executable");
        let stdin = child.stdin.take().expect("Research fixture stdin");
        let mut stdout =
            BufReader::new(child.stdout.take().expect("Research fixture stdout")).lines();
        let ready = tokio::time::timeout(Duration::from_secs(20), stdout.next_line())
            .await
            .expect("Research fixture READY deadline")
            .expect("read Research fixture READY")
            .expect("Research fixture exited before READY");
        let receipt: Value =
            serde_json::from_str(&ready).expect("Research fixture READY must be JSON");
        assert_eq!(
            receipt,
            json!({"schema":SCHEMA,"event":"ready"}),
            "Research fixture must produce exactly the independent READY receipt"
        );
        Self {
            wire: Arc::new(Mutex::new(Wire {
                child,
                stdin,
                stdout,
                sequence: 0,
            })),
        }
    }

    async fn call(
        &self,
        action: &str,
        job_id: Option<&str>,
        deadline: Duration,
    ) -> Result<Value, String> {
        let mut wire = self.wire.lock().await;
        wire.sequence += 1;
        let id = wire.sequence;
        let request = match job_id {
            Some(job_id) => json!({"schema":SCHEMA,"id":id,"action":action,"job_id":job_id}),
            None => json!({"schema":SCHEMA,"id":id,"action":action}),
        };
        let mut encoded = serde_json::to_vec(&request).expect("serialize Research fixture control");
        encoded.push(b'\n');
        tokio::time::timeout(deadline, wire.stdin.write_all(&encoded))
            .await
            .map_err(|_| "RESEARCH_FIXTURE_WRITE_TIMEOUT".to_owned())?
            .map_err(|_| "RESEARCH_FIXTURE_WRITE_FAILED".to_owned())?;
        let response = tokio::time::timeout(deadline, wire.stdout.next_line())
            .await
            .map_err(|_| "RESEARCH_FIXTURE_REPLY_TIMEOUT".to_owned())?
            .map_err(|_| "RESEARCH_FIXTURE_READ_FAILED".to_owned())?
            .ok_or_else(|| "RESEARCH_FIXTURE_EOF".to_owned())?;
        let decoded: Value = serde_json::from_str(&response)
            .map_err(|_| "RESEARCH_FIXTURE_INVALID_JSON".to_owned())?;
        if decoded["schema"] != SCHEMA || decoded["id"] != id {
            return Err("RESEARCH_FIXTURE_REPLY_BINDING_INVALID".into());
        }
        if decoded["ok"] == true {
            return decoded
                .get("data")
                .cloned()
                .ok_or_else(|| "RESEARCH_FIXTURE_MISSING_DATA".into());
        }
        if decoded["ok"] == false {
            return Err(decoded["error"]
                .as_str()
                .filter(|error| !error.is_empty())
                .unwrap_or("RESEARCH_FIXTURE_UNKNOWN_REFUSAL")
                .to_owned());
        }
        Err("RESEARCH_FIXTURE_MISSING_OUTCOME".into())
    }

    pub(crate) async fn start(&self) -> (String, String, Value) {
        let data = self
            .call("start", None, Duration::from_secs(90))
            .await
            .expect("actual Research grids must enter concurrent compute");
        let first = data["first"]
            .as_str()
            .expect("first real Research job ID")
            .to_owned();
        let second = data["second"]
            .as_str()
            .expect("second real Research job ID")
            .to_owned();
        assert_ne!(first, second, "concurrent Research jobs must be distinct");
        assert!(!first.is_empty() && !second.is_empty());
        assert_eq!(data["research_jobs_actual"], true);
        let worker_pid = data["worker_pid"]
            .as_u64()
            .expect("real external Research Worker PID");
        assert!(worker_pid > 0);
        assert_ne!(worker_pid, std::process::id() as u64);
        for (name, id) in [("first_status", &first), ("second_status", &second)] {
            let snapshot = &data[name];
            assert_eq!(snapshot["id"], id.as_str(), "real Worker job ID mismatch");
            assert_eq!(snapshot["state"], "running", "{name} must enter compute");
            assert!(
                snapshot["candidate_progress"]["completed_candidates"]
                    .as_u64()
                    .is_some_and(|count| count > 0),
                "{name} must have completed authentic Research candidate work"
            );
        }
        let boundary = data
            .get("boundary_evidence")
            .cloned()
            .expect("missing strict pre-source Research boundary witness");
        assert_eq!(
            boundary["schema"],
            "okx.518.t10-real-child-pre-source-boundaries/v1"
        );
        assert_eq!(boundary["route"], "research_owned_fixture_to_worker_ipc");
        assert_eq!(
            boundary["source_requests_before"], 0,
            "Research boundary must precede all source reads"
        );
        assert_eq!(
            boundary["source_requests_after"], 0,
            "invalid Research plans may not fetch source data"
        );
        assert_eq!(
            boundary["admission_only_not_executed"],
            json!([
                "one_million_bars_one_instrument_64",
                "eight_instruments_one_million_bars_64"
            ]),
            "both exact maximal plans must be admitted without execution"
        );
        let rejected = boundary["rejected_before_source"]
            .as_object()
            .expect("all three pre-source negative cases are mandatory");
        assert_eq!(rejected.len(), 3);
        for name in [
            "one_million_plus_one_bars",
            "nine_instruments",
            "sixty_five_variants",
        ] {
            assert_eq!(rejected[name], "INVALID_RESEARCH_REQUEST", "{name}");
        }
        assert_eq!(boundary["full_scale_execution"], "not_attempted");
        assert_eq!(boundary["side_effects"], "none");
        (first, second, boundary)
    }

    pub(crate) async fn status(&self, id: &str) -> Value {
        self.call("status", Some(id), Duration::from_secs(5))
            .await
            .expect("real Research Worker status")
    }

    pub(crate) async fn explain(&self, id: &str) -> Value {
        self.call("explain", Some(id), Duration::from_secs(2))
            .await
            .expect("real Research Worker bounded diagnostic")
    }

    pub(crate) async fn disconnect(&self) {
        self.call("disconnect", None, Duration::from_secs(15))
            .await
            .expect("fixture must actually stop its Research child");
    }

    pub(crate) async fn rejected_while_disconnected(&self, id: &str) {
        assert!(
            self.call("explain", Some(id), Duration::from_secs(2))
                .await
                .is_err(),
            "Research diagnostic must not succeed with Worker disconnected"
        );
    }

    pub(crate) async fn reopen_status(&self, id: &str) -> Value {
        self.call("reopen_status", Some(id), Duration::from_secs(15))
            .await
            .expect("Research Worker must reopen retained job state")
    }

    pub(crate) async fn source_stats(&self) -> (u64, u64) {
        let data = self
            .call("stats", None, Duration::from_secs(5))
            .await
            .expect("Research fixture source evidence");
        (
            data["successful_requests"]
                .as_u64()
                .expect("count actual public source requests"),
            data["returned_rows"]
                .as_u64()
                .expect("count returned source rows"),
        )
    }

    pub(crate) async fn shutdown(&self) {
        self.call("stop", None, Duration::from_secs(15))
            .await
            .expect("Research fixture bounded child cleanup");
        let mut wire = self.wire.lock().await;
        let exited = tokio::time::timeout(Duration::from_secs(15), wire.child.wait())
            .await
            .expect("Research fixture process did not terminate")
            .expect("Research fixture process wait failed");
        assert!(exited.success(), "Research fixture process failed to stop");
    }
}

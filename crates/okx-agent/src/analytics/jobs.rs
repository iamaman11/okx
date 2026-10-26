use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}
impl State {
    fn wire(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
        }
    }
    fn parse(value: &str) -> rusqlite::Result<Self> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "interrupted" => Ok(Self::Interrupted),
            _ => Err(rusqlite::Error::InvalidQuery),
        }
    }
}
pub(super) fn is_transient_rate_defer(error: Option<&str>) -> bool {
    error.is_some_and(|value| value.contains("OKX rate/backpressure defer:"))
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub request_hash: String,
    pub idempotency_key: String,
    #[serde(default)]
    pub cancel_keys: BTreeSet<String>,
    pub state: State,
    pub progress: u8,
    pub artifact: Option<String>,
    pub error: Option<String>,
    pub created_ms: u64,
    pub pinned: bool,
    /// Terminal artifact expiry; pinned results have no deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
}

/// The SQLite connection is the sole metadata owner. The in-memory map contains
/// only cancellers for currently running requests, never historical jobs.
#[derive(Clone)]
pub struct Jobs {
    db: Arc<Mutex<Connection>>,
    cancel: Arc<Mutex<BTreeMap<String, Arc<AtomicBool>>>>,
}
impl Jobs {
    pub fn open(path: PathBuf) -> Result<Self, String> {
        std::fs::create_dir_all(path.parent().ok_or("INVALID_CATALOG_PATH")?)
            .map_err(|e| e.to_string())?;
        if path
            .extension()
            .is_none_or(|extension| extension != "sqlite")
        {
            return Err("RESEARCH_SQLITE_PATH_REQUIRED".into());
        }
        let connection = Connection::open(path).map_err(|e| e.to_string())?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA foreign_keys=ON;
                 PRAGMA busy_timeout=5000;
                 CREATE TABLE IF NOT EXISTS research_jobs(
                    id TEXT PRIMARY KEY,
                    request_hash TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL CHECK(state IN('queued','running','completed','failed','cancelled','interrupted')),
                    progress INTEGER NOT NULL CHECK(progress BETWEEN 0 AND 100),
                    artifact TEXT,
                    error TEXT,
                    created_ms INTEGER NOT NULL,
                    pinned INTEGER NOT NULL CHECK(pinned IN(0,1))
                 ) STRICT;
                 CREATE INDEX IF NOT EXISTS idx_research_jobs_retention
                    ON research_jobs(created_ms,id) WHERE artifact IS NOT NULL AND pinned=0;
                 CREATE INDEX IF NOT EXISTS idx_research_jobs_active
                    ON research_jobs(state,id);
                 CREATE INDEX IF NOT EXISTS idx_research_jobs_artifact
                    ON research_jobs(artifact) WHERE artifact IS NOT NULL;
                 CREATE TABLE IF NOT EXISTS research_cancel_keys(
                    cancel_key TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL REFERENCES research_jobs(id)
                 ) STRICT;
                 CREATE TABLE IF NOT EXISTS research_cursor_signing (
                    name TEXT PRIMARY KEY,
                    key BLOB NOT NULL CHECK(length(key)=32)
                 ) STRICT;
                 INSERT INTO research_cursor_signing(name,key)
                    VALUES('research-result-v1',randomblob(32))
                    ON CONFLICT(name) DO NOTHING;
                 UPDATE research_jobs
                    SET state='interrupted',error='process restarted before completion'
                    WHERE state IN('queued','running');",
            )
            .map_err(|e| e.to_string())?;
        Ok(Self {
            db: Arc::new(Mutex::new(connection)),
            cancel: Default::default(),
        })
    }
    fn row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Job> {
        let state: String = row.get(3)?;
        let progress: i64 = row.get(4)?;
        let created_ms: i64 = row.get(7)?;
        let created_ms = u64::try_from(created_ms).map_err(|_| rusqlite::Error::InvalidQuery)?;
        let state = State::parse(&state)?;
        let pinned = row.get::<_, i64>(8)? != 0;
        let artifact: Option<String> = row.get(5)?;
        let expires_at_ms =
            if artifact.is_some() && !pinned && !matches!(state, State::Queued | State::Running) {
                Some(created_ms.saturating_add(7 * 86_400_000))
            } else {
                None
            };
        Ok(Job {
            id: row.get(0)?,
            request_hash: row.get(1)?,
            idempotency_key: row.get(2)?,
            cancel_keys: BTreeSet::new(),
            state,
            progress: u8::try_from(progress).map_err(|_| rusqlite::Error::InvalidQuery)?,
            artifact,
            error: row.get(6)?,
            created_ms,
            pinned,
            expires_at_ms,
        })
    }
    fn read(connection: &Connection, id: &str) -> Result<Option<Job>, String> {
        let mut job = connection
            .query_row(
                "SELECT id,request_hash,idempotency_key,state,progress,artifact,error,created_ms,pinned
                 FROM research_jobs WHERE id=?1",
                [id],
                Self::row,
            )
            .optional()
            .map_err(|e| e.to_string())?;
        if let Some(value) = &mut job {
            let mut stmt = connection
                .prepare("SELECT cancel_key FROM research_cancel_keys WHERE job_id=?1")
                .map_err(|e| e.to_string())?;
            value.cancel_keys = stmt
                .query_map([id], |row| row.get::<_, String>(0))
                .map_err(|e| e.to_string())?
                .collect::<rusqlite::Result<BTreeSet<_>>>()
                .map_err(|e| e.to_string())?;
        }
        Ok(job)
    }
    pub fn enqueue(
        &self,
        idempotency_key: String,
        hash: String,
    ) -> Result<(Job, bool, Arc<AtomicBool>), String> {
        let mut db = self.db.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| e.to_string())?;
        let owner: Option<String> = tx
            .query_row(
                "SELECT id FROM research_jobs WHERE idempotency_key=?1",
                [&idempotency_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| e.to_string())?;
        if owner.as_deref().is_some_and(|existing| existing != hash) {
            return Err("IDEMPOTENCY_CONFLICT".into());
        }
        let existing = Self::read(&tx, &hash)?;
        // One SQL transaction owns the bounded research/market admission.
        // Duplicate idempotent submissions do not consume new queue slots.
        if existing.is_none()
            || existing.as_ref().is_some_and(|j| {
                j.state == State::Failed && is_transient_rate_defer(j.error.as_deref())
            })
        {
            let admitted: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM research_jobs WHERE state IN ('queued','running')",
                    [],
                    |row| row.get(0),
                )
                .map_err(|e| e.to_string())?;
            if admitted >= 34 {
                return Err("RESOURCE_BUSY:RESEARCH_QUEUE_FULL".into());
            }
        }
        let is_new = if let Some(job) = &existing {
            if job.state == State::Failed && is_transient_rate_defer(job.error.as_deref()) {
                tx.execute(
                    "UPDATE research_jobs SET state='queued',progress=0,artifact=NULL,error=NULL WHERE id=?1",
                    [&hash],
                )
                .map_err(|e| e.to_string())?;
                true
            } else {
                false
            }
        } else {
            let now = chrono::Utc::now().timestamp_millis().max(0);
            tx.execute(
                "INSERT INTO research_jobs(id,request_hash,idempotency_key,state,progress,created_ms,pinned)
                 VALUES(?1,?1,?2,'queued',0,?3,0)",
                params![hash, idempotency_key, now],
            )
            .map_err(|e| e.to_string())?;
            true
        };
        tx.commit().map_err(|e| e.to_string())?;
        let job = Self::read(&db, &hash)?.ok_or("JOB_NOT_FOUND")?;
        drop(db);
        let mut running = self.cancel.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        let token = if is_new {
            let token = Arc::new(AtomicBool::new(false));
            running.insert(hash, token.clone());
            token
        } else {
            running.get(&hash).cloned().unwrap_or_default()
        };
        Ok((job, is_new, token))
    }
    /// Persisted per-catalog cursor authenticator. Distinct installations
    /// cannot replay each other's page selectors. Never exported or logged.
    pub fn cursor_signing_key(&self) -> Result<Vec<u8>, String> {
        let db = self.db.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        db.query_row(
            "SELECT key FROM research_cursor_signing WHERE name='research-result-v1'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| e.to_string())
    }

    pub fn get(&self, id: &str) -> Result<Job, String> {
        let db = self.db.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        Self::read(&db, id)?.ok_or("JOB_NOT_FOUND".into())
    }
    pub fn update(
        &self,
        id: &str,
        state: State,
        artifact: Option<String>,
        error: Option<String>,
    ) -> Result<(), String> {
        if state == State::Completed && artifact.is_none() {
            return Err("RESEARCH_COMPLETED_RESULT_REQUIRED".into());
        }
        let db = self.db.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        let updated = db
            .execute(
                "UPDATE research_jobs SET state=?2,
                    progress=CASE WHEN ?2='completed' THEN 100 WHEN ?2='running' THEN 10 ELSE progress END,
                    artifact=?3,error=?4
                 WHERE id=?1 AND state IN ('queued','running')",
                params![id, state.wire(), artifact, error],
            )
            .map_err(|e| e.to_string())?;
        if updated == 0 {
            match Self::read(&db, id)?.ok_or("JOB_NOT_FOUND")?.state {
                // A cancelled job cannot be resurrected by late compute.
                State::Cancelled => return Ok(()),
                _ => return Err("RESEARCH_TERMINAL_IMMUTABLE".into()),
            }
        }
        drop(db);
        if !matches!(state, State::Queued | State::Running) {
            self.cancel
                .lock()
                .map_err(|_| "CATALOG_LOCK_POISONED")?
                .remove(id);
        }
        Ok(())
    }
    pub fn cancel(&self, id: &str, key: &str) -> Result<Job, String> {
        let mut db = self.db.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| e.to_string())?;
        if Self::read(&tx, id)?.is_none() {
            return Err("JOB_NOT_FOUND".into());
        }
        let owner: Option<String> = tx
            .query_row(
                "SELECT job_id FROM research_cancel_keys WHERE cancel_key=?1",
                [key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| e.to_string())?;
        if owner.as_deref().is_some_and(|owner| owner != id) {
            return Err("IDEMPOTENCY_CONFLICT".into());
        }
        tx.execute(
            "INSERT INTO research_cancel_keys(cancel_key,job_id) VALUES(?1,?2)
             ON CONFLICT(cancel_key) DO NOTHING",
            params![key, id],
        )
        .map_err(|e| e.to_string())?;
        tx.execute(
            "UPDATE research_jobs SET state='cancelled'
             WHERE id=?1 AND state IN('queued','running')",
            [id],
        )
        .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())?;
        let job = Self::read(&db, id)?.ok_or("JOB_NOT_FOUND")?;
        drop(db);
        if job.state == State::Cancelled
            && let Some(token) = self
                .cancel
                .lock()
                .map_err(|_| "CATALOG_LOCK_POISONED")?
                .remove(id)
        {
            token.store(true, Ordering::Release);
        }
        Ok(job)
    }
    /// Legacy test-only snapshot. Production retention uses SQL window selection.
    #[cfg(test)]
    pub fn snapshot(&self) -> Vec<Job> {
        let db = self.db.lock().unwrap();
        let mut stmt = db
            .prepare("SELECT id FROM research_jobs ORDER BY id LIMIT 1000")
            .unwrap();
        stmt.query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|id| Self::read(&db, &id).ok().flatten())
            .collect()
    }
    pub fn retention_candidates(&self, now_ms: u64, limit: usize) -> Result<Vec<Job>, String> {
        let db = self.db.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        let mut stmt = db
            .prepare(
                "SELECT id FROM research_jobs WHERE pinned=0 AND artifact IS NOT NULL
             AND created_ms < ?1 AND state NOT IN ('queued','running')
             ORDER BY created_ms,id LIMIT ?2",
            )
            .map_err(|e| e.to_string())?;
        let ids = stmt
            .query_map(
                params![now_ms.min(i64::MAX as u64) as i64, limit as i64],
                |row| row.get::<_, String>(0),
            )
            .map_err(|e| e.to_string())?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| e.to_string())?;
        ids.iter()
            .map(|id| Self::read(&db, id)?.ok_or("JOB_NOT_FOUND".into()))
            .collect()
    }
    /// Atomically retire a terminal reference. Unlike update(), this permits
    /// a CANCELLED job's partial artifact to be tombstoned without reviving it.
    /// A failed compare-and-swap aborts garbage collection before file deletion.
    pub fn expire_terminal_artifact(&self, id: &str, artifact: &str) -> Result<(), String> {
        let db = self.db.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        let changed = db
            .execute(
                "UPDATE research_jobs SET artifact=NULL,error='RESULT_EXPIRED_BY_7_DAY_RETENTION'
             WHERE id=?1 AND artifact=?2 AND pinned=0
             AND state NOT IN ('queued','running')",
                params![id, artifact],
            )
            .map_err(|e| e.to_string())?;
        if changed != 1 {
            return Err("RESEARCH_RETENTION_CONFLICT".into());
        }
        Ok(())
    }
    pub fn artifact_roots_page(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>, String> {
        if limit == 0 || limit > 128 {
            return Err("RESEARCH_ROOT_PAGE_LIMIT:1..128".into());
        }
        let db = self.db.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        let mut stmt = db
            .prepare(
                "SELECT DISTINCT artifact FROM research_jobs
             WHERE artifact IS NOT NULL AND (?1 IS NULL OR artifact>?1)
             ORDER BY artifact LIMIT ?2",
            )
            .map_err(|e| e.to_string())?;
        stmt.query_map(params![after, limit as i64], |row| row.get::<_, String>(0))
            .map_err(|e| e.to_string())?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| e.to_string())
    }
    pub fn artifact_reference_count(&self, artifact: &str) -> Result<u64, String> {
        let db = self.db.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        db.query_row(
            "SELECT COUNT(*) FROM research_jobs WHERE artifact=?1",
            [artifact],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| e.to_string())
        .and_then(|count| {
            u64::try_from(count).map_err(|_| "RESEARCH_REFERENCE_COUNT_INVALID".into())
        })
    }
    pub fn any_running(&self) -> Result<bool, String> {
        let db = self.db.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        db.query_row(
            "SELECT EXISTS(SELECT 1 FROM research_jobs WHERE state IN ('queued','running'))",
            [],
            |row| row.get::<_, bool>(0),
        )
        .map_err(|e| e.to_string())
    }
    pub fn pin(&self, id: &str, pinned: bool) -> Result<(), String> {
        let db = self.db.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        if db
            .execute(
                "UPDATE research_jobs SET pinned=?2 WHERE id=?1",
                params![id, pinned],
            )
            .map_err(|e| e.to_string())?
            == 0
        {
            return Err("JOB_NOT_FOUND".into());
        }
        Ok(())
    }
}

impl Jobs {
    pub async fn enqueue_async(
        &self,
        idempotency_key: String,
        hash: String,
    ) -> Result<(Job, bool, Arc<AtomicBool>), String> {
        let owner = self.clone();
        tokio::task::spawn_blocking(move || owner.enqueue(idempotency_key, hash))
            .await
            .map_err(|_| "RESEARCH_SQL_WORKER_FAILED".to_owned())?
    }
    pub async fn get_async(&self, id: String) -> Result<Job, String> {
        let owner = self.clone();
        tokio::task::spawn_blocking(move || owner.get(&id))
            .await
            .map_err(|_| "RESEARCH_SQL_WORKER_FAILED".to_owned())?
    }
    pub async fn update_async(
        &self,
        id: String,
        state: State,
        artifact: Option<String>,
        error: Option<String>,
    ) -> Result<(), String> {
        let owner = self.clone();
        tokio::task::spawn_blocking(move || owner.update(&id, state, artifact, error))
            .await
            .map_err(|_| "RESEARCH_SQL_WORKER_FAILED".to_owned())?
    }
    pub async fn cancel_async(&self, id: String, key: String) -> Result<Job, String> {
        let owner = self.clone();
        tokio::task::spawn_blocking(move || owner.cancel(&id, &key))
            .await
            .map_err(|_| "RESEARCH_SQL_WORKER_FAILED".to_owned())?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "okx-research-sqlite-{label}-{}-{}.sqlite",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ))
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
        if phase == "running" {
            jobs.update(&id, State::Running, Some(artifact), None).unwrap();
        } else if phase == "completed" {
            jobs.update(&id, State::Running, None, None).unwrap();
            jobs.update(&id, State::Completed, Some(artifact), None).unwrap();
        }
        let ready = PathBuf::from(&file).with_extension(format!("{phase}.ready"));
        std::fs::write(ready, b"committed").unwrap();
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }

    #[test]
    fn c17_actual_child_os_kill_recovers_wal_and_blocks_false_success() {
        use std::{process::{Command, Stdio},time::{Duration, Instant}};
        let file = path("c17-physical-kill");
        let exe = std::env::current_exe().unwrap();
        let phases = ["queued", "running", "completed"];
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
                assert!(child.try_wait().unwrap().is_none(), "child exited before evidence commit");
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
                let store = okx_research::ResearchArtifactStore::at(
                    file.with_extension("evidence-root"),
                );
                let readback: serde_json::Value = store.read_evidence_json(&proof).unwrap();
                assert_eq!(readback["phase"], "completed");
                assert_eq!(
                    restarted.update(&id, State::Completed, Some("forged".into()), None)
                        .unwrap_err(),
                    "RESEARCH_TERMINAL_IMMUTABLE"
                );
            } else {
                assert_eq!(job.state, State::Interrupted);
                assert_eq!(
                    restarted.update(&id, State::Completed, Some("forged".into()), None)
                        .unwrap_err(),
                    "RESEARCH_TERMINAL_IMMUTABLE"
                );
            }
            std::fs::remove_file(ready).unwrap();
        }
        println!("C17_OS_KILL_JSON:{}", serde_json::json!({
            "runner_os":std::env::consts::OS,
            "test_binary":exe.file_name().and_then(|name|name.to_str()).unwrap_or("unknown"),
            "phases":phases,
            "kill_mode":"OS process kill + WAL reopen, no graceful shutdown",
            "assertions":"completed remains immutable; queued/running interrupted; SHA artifact readback",
            "trading_effects":"none"
        }));
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
        let t = Jobs::open(p.clone()).unwrap();
        assert_eq!(t.get("fixture").unwrap().state, State::Interrupted);
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
        let s = Jobs::open(p).unwrap();
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
            "EXPLAIN QUERY PLAN SELECT id,request_hash,idempotency_key,state,progress,artifact,error,created_ms,pinned
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
    }
}

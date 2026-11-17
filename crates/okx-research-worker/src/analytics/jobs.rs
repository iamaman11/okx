use okx_protocol::research_ipc::{IdempotencyKey, JobId};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
#[path = "job_diagnostics.rs"]
mod job_diagnostics;
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
    error.is_some_and(okx_protocol::research_ipc::is_retryable_source_defer)
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_progress: Option<super::candidate_progress::CandidateProgress>,
    pub artifact: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_artifact: Option<String>,
    pub error: Option<String>,
    pub created_ms: u64,
    pub pinned: bool,
    /// Terminal artifact expiry; pinned results have no deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
}

/// The SQLite connection is the sole metadata owner. The in-memory map contains
/// only cancellers for currently running requests. The diagnostic projection
/// retains bounded, non-authoritative observations, never the complete catalog.
#[derive(Clone)]
pub struct Jobs {
    db: Arc<Mutex<Connection>>,
    _root_lock: Arc<std::fs::File>,
    cancel: Arc<Mutex<BTreeMap<String, Arc<super::executor::Cancel>>>>,
    diagnostic: Arc<job_diagnostics::Projection>,
    draining: Arc<AtomicBool>,
}
impl Jobs {
    #[cfg(test)]
    pub fn open(path: PathBuf) -> Result<Self, String> {
        Self::open_impl(path, None)
    }
    pub fn open_scoped(
        path: PathBuf,
        scope: &okx_protocol::research_ipc::ResearchScope,
    ) -> Result<Self, String> {
        Self::open_impl(path, Some(scope))
    }
    fn open_impl(
        path: PathBuf,
        scope: Option<&okx_protocol::research_ipc::ResearchScope>,
    ) -> Result<Self, String> {
        std::fs::create_dir_all(path.parent().ok_or("INVALID_CATALOG_PATH")?)
            .map_err(|e| e.to_string())?;
        if path
            .extension()
            .is_none_or(|extension| extension != "sqlite")
        {
            return Err("RESEARCH_SQLITE_PATH_REQUIRED".into());
        }
        let lock_path = path
            .parent()
            .ok_or("INVALID_CATALOG_PATH")?
            .join("research-worker.lock");
        let root_lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)
            .map_err(|e| e.to_string())?;
        root_lock
            .try_lock()
            .map_err(|_| "RESEARCH_ROOT_LOCKED".to_string())?;
        let mut connection = Connection::open(path).map_err(|e| e.to_string())?;
        // The authorized host assigns a legacy unscoped catalog to its configured
        // scope once, under the root lease, before recovery or schema migration.
        if let Some(scope) = scope {
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|e| e.to_string())?;
            let present: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='research_catalog_metadata')", [], |row| row.get(0)).map_err(|e| e.to_string())?;
            if present {
                let stored: Option<String> = tx
                    .query_row(
                        "SELECT value FROM research_catalog_metadata WHERE name='scope'",
                        [],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|e| e.to_string())?;
                if stored.as_deref() != Some(scope.as_str()) {
                    return Err("RESEARCH_SCOPE_MISMATCH".into());
                }
            } else {
                tx.execute_batch("CREATE TABLE research_catalog_metadata(name TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;").map_err(|e| e.to_string())?;
                tx.execute(
                    "INSERT INTO research_catalog_metadata(name,value) VALUES('scope',?1)",
                    [scope.as_str()],
                )
                .map_err(|e| e.to_string())?;
            }
            tx.commit().map_err(|e| e.to_string())?;
        }
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA foreign_keys=ON;
                 PRAGMA busy_timeout=100;
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
        // Existing catalogs gain only a metadata reference; immutable result roots stay unchanged.
        let migration = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| e.to_string())?;
        let has_decision = migration
            .prepare("PRAGMA table_info(research_jobs)")
            .map_err(|e| e.to_string())?
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(|e| e.to_string())?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| e.to_string())?
            .iter()
            .any(|column| column == "decision_artifact");
        if !has_decision {
            migration
                .execute(
                    "ALTER TABLE research_jobs ADD COLUMN decision_artifact TEXT",
                    [],
                )
                .map_err(|e| e.to_string())?;
        }
        migration.execute("CREATE INDEX IF NOT EXISTS idx_research_jobs_decision_artifact ON research_jobs(decision_artifact) WHERE decision_artifact IS NOT NULL", []).map_err(|e| e.to_string())?;
        let has_progress = migration
            .prepare("PRAGMA table_info(research_jobs)")
            .map_err(|e| e.to_string())?
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(|e| e.to_string())?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| e.to_string())?
            .iter()
            .any(|column| column == "candidate_progress");
        if !has_progress {
            migration
                .execute(
                    "ALTER TABLE research_jobs ADD COLUMN candidate_progress TEXT",
                    [],
                )
                .map_err(|e| e.to_string())?;
        }
        let has_semantic = migration
            .prepare("PRAGMA table_info(research_jobs)")
            .map_err(|e| e.to_string())?
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(|e| e.to_string())?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| e.to_string())?
            .iter()
            .any(|column| column == "semantic_hash");
        if !has_semantic {
            migration
                .execute(
                    "ALTER TABLE research_jobs ADD COLUMN semantic_hash TEXT",
                    [],
                )
                .map_err(|e| e.to_string())?;
        }
        migration.commit().map_err(|e| e.to_string())?;
        let diagnostic = Arc::new(job_diagnostics::Projection::default());
        diagnostic.restore(&connection);
        Ok(Self {
            db: Arc::new(Mutex::new(connection)),
            _root_lock: Arc::new(root_lock),
            cancel: Default::default(),
            diagnostic,
            draining: Arc::new(AtomicBool::new(false)),
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
        let id: String = row.get(0)?;
        JobId::new(id.clone()).map_err(|_| rusqlite::Error::InvalidQuery)?;
        Ok(Job {
            id,
            request_hash: row.get(1)?,
            idempotency_key: row.get(2)?,
            cancel_keys: BTreeSet::new(),
            state,
            progress: u8::try_from(progress).map_err(|_| rusqlite::Error::InvalidQuery)?,
            candidate_progress: row
                .get::<_, Option<String>>(10)?
                .map(|value| {
                    serde_json::from_str::<super::candidate_progress::CandidateProgress>(&value)
                        .map_err(|_| rusqlite::Error::InvalidQuery)
                        .and_then(|value| {
                            if value.valid() {
                                Ok(value)
                            } else {
                                Err(rusqlite::Error::InvalidQuery)
                            }
                        })
                })
                .transpose()?,
            artifact,
            decision_artifact: row.get(9)?,
            error: row.get(6)?,
            created_ms,
            pinned,
            expires_at_ms,
        })
    }
    fn read(connection: &Connection, id: &str) -> Result<Option<Job>, String> {
        let mut job = connection
            .query_row(
                "SELECT id,request_hash,idempotency_key,state,progress,artifact,error,created_ms,pinned,decision_artifact,candidate_progress
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
    #[cfg(test)]
    pub fn enqueue(
        &self,
        idempotency_key: String,
        hash: String,
    ) -> Result<(Job, bool, Arc<super::executor::Cancel>), String> {
        self.enqueue_semantic(
            IdempotencyKey::new(idempotency_key).map_err(str::to_owned)?,
            hash.clone(),
            hash,
        )
    }
    pub fn enqueue_semantic(
        &self,
        idempotency_key: IdempotencyKey,
        hash: String,
        semantic_hash: String,
    ) -> Result<(Job, bool, Arc<super::executor::Cancel>), String> {
        let mut db = self.db.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| {
                if e.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy) {
                    "RESEARCH_SQL_BUSY".into()
                } else {
                    e.to_string()
                }
            })?;
        let owner: Option<(String, Option<String>)> = tx
            .query_row(
                "SELECT id,semantic_hash FROM research_jobs WHERE idempotency_key=?1",
                [idempotency_key.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|e| e.to_string())?;
        if owner.as_ref().is_some_and(|(id, digest)| {
            digest
                .as_deref()
                .map_or(id != &hash, |value| value != semantic_hash)
        }) {
            return Err("IDEMPOTENCY_CONFLICT".into());
        }
        let hash = owner.map(|(id, _)| id).unwrap_or(hash);
        let existing = Self::read(&tx, &hash)?;
        let would_start = existing.is_none()
            || existing.as_ref().is_some_and(|j| {
                j.state == State::Failed && is_transient_rate_defer(j.error.as_deref())
            });
        if would_start && self.draining.load(Ordering::Acquire) {
            return Err("RESEARCH_UPDATING".into());
        }
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
                    "UPDATE research_jobs SET state='queued',progress=0,candidate_progress=NULL,artifact=NULL,error=NULL WHERE id=?1",
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
                "INSERT INTO research_jobs(id,request_hash,idempotency_key,state,progress,created_ms,pinned,semantic_hash)
                 VALUES(?1,?1,?2,'queued',0,?3,0,?4)",
                params![hash, idempotency_key.as_str(), now, semantic_hash],
            )
            .map_err(|e| e.to_string())?;
            true
        };
        tx.commit().map_err(|e| e.to_string())?;
        let job = Self::read(&db, &hash)?.ok_or("JOB_NOT_FOUND")?;
        self.diagnostic.publish_id(&db, &hash);
        // Publish the token before releasing admission: cancellation takes the
        // same DB lock and must never observe a job without its token.
        let mut running = self.cancel.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        let token = if is_new {
            let token = Arc::new(super::executor::Cancel::default());
            running.insert(hash, token.clone());
            token
        } else {
            running.get(&hash).cloned().unwrap_or_default()
        };
        drop(running);
        drop(db);
        Ok((job, is_new, token))
    }
    /// Persisted per-catalog cursor authenticator. Distinct installations
    /// cannot replay each other's page selectors. Never exported or logged.
    /// Commit a prepublished immutable reference in a short SQL transaction.
    /// Artifact verification and file I/O happen before acquiring this owner.
    pub(super) fn commit_decision(
        &self,
        id: &str,
        result: &str,
        observed_head: Option<&str>,
        next: &str,
    ) -> Result<(), String> {
        let mut db = self.db.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| e.to_string())?;
        let job = Self::read(&tx, id)?.ok_or("JOB_NOT_FOUND")?;
        Self::require_decision_result(&job)?;
        if job.artifact.as_deref() != Some(result) {
            return Err("DECISION_RESULT_MISMATCH".into());
        }
        if job.decision_artifact.as_deref() != observed_head {
            return Err("DECISION_REVISION_CONFLICT".into());
        }
        let changed = tx.execute(
            "UPDATE research_jobs SET decision_artifact=?2 WHERE id=?1 AND artifact=?3 AND decision_artifact IS ?4 AND state='completed'",
            params![id, next, result, observed_head],
        ).map_err(|e| e.to_string())?;
        if changed != 1 {
            return Err("DECISION_REVISION_CONFLICT".into());
        }
        tx.commit().map_err(|e| e.to_string())?;
        Ok(())
    }
    pub(super) fn require_decision_result(job: &Job) -> Result<(), String> {
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        if job.error.as_deref() == Some("RESULT_EXPIRED_BY_7_DAY_RETENTION")
            || job.expires_at_ms.is_some_and(|expiry| expiry <= now)
        {
            return Err("RESULT_EXPIRED".into());
        }
        if job.state != State::Completed || job.artifact.is_none() {
            return Err("RESULT_NOT_READY".into());
        }
        Ok(())
    }
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
    /// Bounded read-only observation; does not acquire the catalog/SQL mutex.
    pub fn diagnostic_readback(&self, id: &str) -> serde_json::Value {
        self.diagnostic.read(id)
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
                    progress=CASE WHEN ?2='completed' THEN 100 WHEN ?2='running' AND candidate_progress IS NULL THEN 10 ELSE progress END,
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
        self.diagnostic.publish_id(&db, id);
        if !matches!(state, State::Queued | State::Running) {
            self.cancel
                .lock()
                .map_err(|_| "CATALOG_LOCK_POISONED")?
                .remove(id);
        }
        drop(db);
        Ok(())
    }
    /// Same catalog owner and terminal fence as job transitions. Late CPU
    /// observations may never mutate cancellation or terminal snapshots.
    pub(super) fn update_candidate_progress(
        &self,
        id: &str,
        progress: super::candidate_progress::CandidateProgress,
    ) -> Result<(), String> {
        if !progress.valid() {
            return Err("INVALID_CANDIDATE_PROGRESS".into());
        }
        let db = self.db.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        let percent = progress
            .completed_work_units
            .saturating_mul(99)
            .checked_div(progress.total_work_units)
            .unwrap_or(0)
            .min(99);
        let sql_counter = |value| i64::try_from(value).map_err(|_| "INVALID_CANDIDATE_PROGRESS");
        let percent = sql_counter(percent)?;
        let total_candidates = sql_counter(progress.total_candidates)?;
        let total_work_units = sql_counter(progress.total_work_units)?;
        let completed_candidates = sql_counter(progress.completed_candidates)?;
        let completed_work_units = sql_counter(progress.completed_work_units)?;
        db.execute(
            "UPDATE research_jobs SET candidate_progress=?2,progress=?3
            WHERE id=?1 AND state IN ('queued','running') AND (candidate_progress IS NULL OR (
                json_extract(candidate_progress,'$.total_candidates')=?4 AND
                json_extract(candidate_progress,'$.total_work_units')=?5 AND
                json_extract(candidate_progress,'$.completed_candidates')<=?6 AND
                json_extract(candidate_progress,'$.completed_work_units')<=?7))",
            params![
                id,
                serde_json::to_string(&progress).map_err(|e| e.to_string())?,
                percent,
                total_candidates,
                total_work_units,
                completed_candidates,
                completed_work_units
            ],
        )
        .map_err(|e| e.to_string())?;
        self.diagnostic.publish_id(&db, id);
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
        self.diagnostic.publish_id(&db, id);
        if job.state == State::Cancelled
            && let Some(token) = self
                .cancel
                .lock()
                .map_err(|_| "CATALOG_LOCK_POISONED")?
                .remove(id)
        {
            token.store(true, Ordering::Release);
        }
        drop(db);
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
                "UPDATE research_jobs SET artifact=NULL,decision_artifact=NULL,error='RESULT_EXPIRED_BY_7_DAY_RETENTION'
             WHERE id=?1 AND artifact=?2 AND pinned=0
             AND state NOT IN ('queued','running')",
                params![id, artifact],
            )
            .map_err(|e| e.to_string())?;
        if changed != 1 {
            return Err("RESEARCH_RETENTION_CONFLICT".into());
        }
        self.diagnostic.publish_id(&db, id);
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
                "SELECT artifact FROM research_jobs
             WHERE artifact IS NOT NULL AND (?1 IS NULL OR artifact>?1)
             UNION SELECT decision_artifact AS artifact FROM research_jobs
             WHERE decision_artifact IS NOT NULL AND (?1 IS NULL OR decision_artifact>?1)
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
            "SELECT COUNT(*) FROM research_jobs WHERE artifact=?1 OR decision_artifact=?1",
            [artifact],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| e.to_string())
        .and_then(|count| {
            u64::try_from(count).map_err(|_| "RESEARCH_REFERENCE_COUNT_INVALID".into())
        })
    }
    /// Seal admission and observe all accepted work under the same SQL owner lock.
    pub fn begin_drain(&self) -> Result<u64, String> {
        let db = self.db.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        self.draining.store(true, Ordering::Release);
        let count: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM research_jobs WHERE state IN ('queued','running')",
                [],
                |row| row.get(0),
            )
            .map_err(|e| e.to_string())?;
        u64::try_from(count).map_err(|_| "RESEARCH_COUNT_INVALID".into())
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
        self.diagnostic.publish_id(&db, id);
        Ok(())
    }
}

impl Jobs {
    pub async fn enqueue_semantic_async(
        &self,
        key: IdempotencyKey,
        hash: String,
        semantic: String,
    ) -> Result<(Job, bool, Arc<super::executor::Cancel>), String> {
        let owner = self.clone();
        tokio::task::spawn_blocking(move || owner.enqueue_semantic(key, hash, semantic))
            .await
            .map_err(|e| e.to_string())?
    }
    pub async fn enqueue_async(
        &self,
        idempotency_key: IdempotencyKey,
        hash: String,
    ) -> Result<(Job, bool, Arc<super::executor::Cancel>), String> {
        let owner = self.clone();
        tokio::task::spawn_blocking(move || {
            owner.enqueue_semantic(idempotency_key, hash.clone(), hash)
        })
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
#[path = "tests/jobs.rs"]
mod tests;

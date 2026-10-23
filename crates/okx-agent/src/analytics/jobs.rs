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
fn is_transient_rate_defer(error: Option<&str>) -> bool {
    error.is_some_and(|value| value.starts_with("OKX rate/backpressure defer:"))
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
                 CREATE TABLE IF NOT EXISTS research_cancel_keys(
                    cancel_key TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL REFERENCES research_jobs(id)
                 ) STRICT;
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
        Ok(Job {
            id: row.get(0)?,
            request_hash: row.get(1)?,
            idempotency_key: row.get(2)?,
            cancel_keys: BTreeSet::new(),
            state: State::parse(&state)?,
            progress: u8::try_from(progress).map_err(|_| rusqlite::Error::InvalidQuery)?,
            artifact: row.get(5)?,
            error: row.get(6)?,
            created_ms: u64::try_from(created_ms).map_err(|_| rusqlite::Error::InvalidQuery)?,
            pinned: row.get::<_, i64>(8)? != 0,
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
        let db = self.db.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        let updated = db
            .execute(
                "UPDATE research_jobs SET state=?2,
                    progress=CASE WHEN ?2='completed' THEN 100 WHEN ?2='running' THEN 10 ELSE progress END,
                    artifact=?3,error=?4
                 WHERE id=?1 AND state!='cancelled'",
                params![id, state.wire(), artifact, error],
            )
            .map_err(|e| e.to_string())?;
        if updated == 0 && Self::read(&db, id)?.is_none() {
            return Err("JOB_NOT_FOUND".into());
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
        if job.state == State::Cancelled {
            if let Some(token) = self
                .cancel
                .lock()
                .map_err(|_| "CATALOG_LOCK_POISONED")?
                .remove(id)
            {
                token.store(true, Ordering::Release);
            }
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
    pub fn artifact_roots(&self) -> Result<Vec<String>, String> {
        let db = self.db.lock().map_err(|_| "CATALOG_LOCK_POISONED")?;
        let mut stmt = db
            .prepare("SELECT DISTINCT artifact FROM research_jobs WHERE artifact IS NOT NULL")
            .map_err(|e| e.to_string())?;
        stmt.query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| e.to_string())?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| e.to_string())
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
    fn restart_marks_job_interrupted() {
        let p = path("restart");
        let s = Jobs::open(p.clone()).unwrap();
        s.enqueue("fixture-key".into(), "fixture".into()).unwrap();
        s.update("fixture", State::Running, None, None).unwrap();
        let t = Jobs::open(p.clone()).unwrap();
        assert_eq!(t.get("fixture").unwrap().state, State::Interrupted);
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

//! Bounded observations of the existing catalog owner, never a second job store.
use rusqlite::{Connection, OptionalExtension};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{
        RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

const LIMIT: usize = 128;
// Do not even copy raw error payloads/cancellation keys into the projection.
const COLUMNS: &str = "substr(id,1,129),state,progress,
    -- An interrupted, failed or cancelled job may retain a partial blob, but
    -- only the completed state can advertise a usable result reference.
    -- The durable result store emits sha256:<64 lowercase hex>. Retain the
    -- older bare-hex references already accepted by the catalog, but never
    -- copy arbitrary artifact text into this bounded read-only projection.
    CASE WHEN state='completed' AND (length(artifact)=64
        OR (length(artifact)=71 AND substr(artifact,1,7)='sha256:'))
      THEN artifact ELSE NULL END,
    CASE WHEN error IS NULL THEN NULL
      WHEN error IN ('RESULT_EXPIRED_BY_7_DAY_RETENTION','JOB_CANCELLED','CANCELLED',
        'SOURCE_INVALID','NONFINITE_SOURCE','CANDLE_RANGE_UNAVAILABLE','CANDLE_RANGE_INCOMPLETE',
        'CANDLE_INTERIOR_GAP','INVALID_FUNDING','FUNDING_RANGE_UNAVAILABLE','FUNDING_RANGE_INCOMPLETE',
        'GLOBAL_RANK_SOURCE_INCOMPLETE','RESEARCH_MEMORY_LIMIT','CPU_QUEUE_FULL','LIVE_SOURCE_DEADLINE',
        'UNSUPPORTED_STRATEGY','UNSUPPORTED_INVERSE_CONTRACT','UNSUPPORTED_INSTRUMENT_TYPE',
        'CONTRACT_MULTIPLIER_MISSING','EMPTY_VARIANTS','RANGE_NOT_CLOSED',
        'RESEARCH_BATCH_ALL_INSTRUMENTS_FAILED','RESEARCH_BATCH_SOURCE_RETRYABLE') THEN error
      WHEN error='process restarted before completion' THEN 'RESEARCH_INTERRUPTED'
      WHEN error LIKE 'LIMIT_EXCEEDED:%' THEN 'LIMIT_EXCEEDED'
      WHEN error LIKE 'LIMIT_TOTAL_BARS:%' THEN 'LIMIT_TOTAL_BARS'
      WHEN error LIKE 'LIMIT_BARS:%' THEN 'LIMIT_BARS'
      WHEN error LIKE 'RESOURCE_BUSY:%' THEN 'RESOURCE_BUSY'
      WHEN error LIKE 'OKX rate/backpressure defer:%'
        OR error LIKE 'public OKX market API error: OKX rate/backpressure defer:%' THEN 'MARKET_SOURCE_RETRYABLE'
      ELSE 'UNCLASSIFIED_RESEARCH_ERROR' END,pinned,
    CASE WHEN length(candidate_progress)<=512 THEN candidate_progress ELSE NULL END,
    CASE WHEN length(origin_invocation)<=256 THEN origin_invocation ELSE NULL END,
    CASE WHEN length(producer)<=512 THEN producer ELSE NULL END";

// Safe bounded event labels only; never emit raw source or request errors.
pub(in crate::analytics) fn safe_reason(error: &str) -> &'static str {
    match error {
        "CANCELLED" | "JOB_CANCELLED" => "CANCELLED",
        "SOURCE_INVALID" => "SOURCE_INVALID",
        "CANDLE_RANGE_UNAVAILABLE" => "CANDLE_RANGE_UNAVAILABLE",
        "CANDLE_RANGE_INCOMPLETE" => "CANDLE_RANGE_INCOMPLETE",
        "CANDLE_INTERIOR_GAP" => "CANDLE_INTERIOR_GAP",
        "FUNDING_RANGE_UNAVAILABLE" => "FUNDING_RANGE_UNAVAILABLE",
        "FUNDING_RANGE_INCOMPLETE" => "FUNDING_RANGE_INCOMPLETE",
        "RESEARCH_MEMORY_LIMIT" => "RESEARCH_MEMORY_LIMIT",
        "CPU_QUEUE_FULL" => "CPU_QUEUE_FULL",
        "LIVE_SOURCE_DEADLINE" => "LIVE_SOURCE_DEADLINE",
        "RESEARCH_BATCH_ALL_INSTRUMENTS_FAILED" => "RESEARCH_BATCH_ALL_INSTRUMENTS_FAILED",
        "RESEARCH_BATCH_SOURCE_RETRYABLE" => "RESEARCH_BATCH_SOURCE_RETRYABLE",
        "UNSUPPORTED_STRATEGY" => "UNSUPPORTED_STRATEGY",
        _ if okx_research_protocol::research_ipc::is_retryable_source_defer(error) => {
            "MARKET_SOURCE_RETRYABLE"
        }
        _ if error.starts_with("RESOURCE_BUSY:") => "RESOURCE_BUSY",
        _ if error.starts_with("LIMIT_") => "LIMIT_EXCEEDED",
        _ => "UNCLASSIFIED_RESEARCH_ERROR",
    }
}

#[derive(Clone, Serialize)]
struct Snapshot {
    id: String,
    state: String,
    progress: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    candidate_progress: Option<super::super::candidate_progress::CandidateProgress>,
    origin_invocation: Option<okx_research_protocol::research_ipc::InvocationId>,
    // Only bounded, validated retry identities participate in lookup. Do not
    // disclose the alias roster in the public snapshot.
    #[serde(skip)]
    retry_invocations: Vec<okx_research_protocol::research_ipc::InvocationId>,
    producer: Option<crate::producer::Producer>,
    result_ref: Option<String>,
    error_code: Option<String>,
    pinned: bool,
    observed_at_ms: i64,
    generation: u64,
    origin: &'static str,
}

fn valid_result_reference(value: &str) -> bool {
    // Existing catalog rows may contain a bare SHA-256. New persisted results
    // use the explicit sha256: form; require its canonical lowercase digest.
    // A malformed or oversized reference must never escape the projection.
    let hex = if let Some(hex) = value.strip_prefix("sha256:") {
        if !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return false;
        }
        hex
    } else {
        value
    };
    hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Snapshot> {
    let error: Option<String> = row.get(4)?;
    let artifact: Option<String> = row.get(3)?;
    Ok(Snapshot {
        id: row.get(0)?,
        state: row.get(1)?,
        progress: row.get(2)?,
        candidate_progress: row
            .get::<_, Option<String>>(6)?
            .map(|value| {
                serde_json::from_str::<super::super::candidate_progress::CandidateProgress>(&value)
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
        origin_invocation: row
            .get::<_, Option<String>>(7)?
            .and_then(|v| serde_json::from_str(&v).ok()),
        retry_invocations: Vec::new(),
        producer: row
            .get::<_, Option<String>>(8)?
            .and_then(|v| serde_json::from_str(&v).ok()),
        result_ref: artifact.filter(|v| valid_result_reference(v)),
        // SQL emits only the fixed whitelist above, never raw failure payloads.
        error_code: error,
        pinned: row.get(5)?,
        observed_at_ms: 0,
        generation: 0,
        origin: "catalog_commit",
    })
}

fn read_retry_invocations(
    db: &Connection,
    id: &str,
) -> rusqlite::Result<Vec<okx_research_protocol::research_ipc::InvocationId>> {
    let mut stmt = db.prepare(
        "SELECT invocation FROM research_invocation_links WHERE job_id=?1
         ORDER BY rowid DESC LIMIT ?2",
    )?;
    let rows = stmt.query_map(
        rusqlite::params![id, super::MAX_ORIGIN_ALIASES_PER_JOB],
        |row| row.get::<_, String>(0),
    )?;
    rows.filter_map(|row| match row {
        Ok(value) => okx_research_protocol::research_ipc::InvocationId::new(value)
            .ok()
            .map(Ok),
        Err(error) => Some(Err(error)),
    })
    .collect()
}
#[derive(Default)]
struct State {
    entries: VecDeque<Snapshot>,
    evicted: u64,
}
#[derive(Default)]
pub(super) struct Projection {
    state: RwLock<State>,
    generation: AtomicU64,
    dropped: AtomicU64,
    restore_partial: AtomicBool,
    restore_failed: AtomicBool,
}
impl Projection {
    fn publish(&self, mut snapshot: Snapshot) {
        if snapshot.id.len() > 128 {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let Ok(mut state) = self.state.try_write() else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        snapshot.generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        snapshot.observed_at_ms = jiff::Timestamp::now().as_millisecond();
        state.entries.retain(|old| old.id != snapshot.id);
        if state.entries.len() == LIMIT {
            state.entries.pop_front();
            state.evicted += 1;
        }
        state.entries.push_back(snapshot);
    }
    pub(super) fn publish_id(&self, db: &Connection, id: &str) {
        // Publication failure cannot alter the already committed SQL outcome.
        match db
            .query_row(
                &format!("SELECT {COLUMNS} FROM research_jobs WHERE id=?1"),
                [id],
                row,
            )
            .optional()
        {
            Ok(Some(mut snapshot)) => {
                match read_retry_invocations(db, id) {
                    Ok(aliases) => snapshot.retry_invocations = aliases,
                    Err(_) => {
                        self.dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
                self.publish(snapshot);
            }
            _ => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    pub(super) fn restore(&self, db: &Connection) {
        // Most recent committed rows, bounded by the research-owned SQLite
        // index. A lexical hash-ID prefix is not a useful recovery window:
        // after restart it can hide every job submitted most recently.
        // LIMIT 129 exposes truncation without scanning the remaining catalog.
        let restored = (|| -> rusqlite::Result<Vec<Snapshot>> {
            let mut statement = db.prepare(&format!(
                "SELECT {COLUMNS} FROM research_jobs ORDER BY created_ms DESC,id DESC LIMIT 129"
            ))?;
            let mut rows: Vec<Snapshot> = statement
                .query_map([], row)?
                .collect::<rusqlite::Result<_>>()?;
            for snapshot in &mut rows {
                match read_retry_invocations(db, &snapshot.id) {
                    Ok(aliases) => snapshot.retry_invocations = aliases,
                    Err(_) => {
                        // A damaged alias record must not hide the durable job
                        // itself or prevent all other snapshots from restoring.
                        self.dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Ok(rows)
        })();
        match restored {
            Ok(rows) => {
                self.restore_partial
                    .store(rows.len() > LIMIT, Ordering::Relaxed);
                // The projection's deque is oldest-to-newest. Reverse the
                // selected bounded window so the newest readback remains last.
                for mut snapshot in rows.into_iter().take(LIMIT).rev() {
                    snapshot.origin = "durable_startup_subset";
                    self.publish(snapshot);
                }
            }
            Err(_) => {
                self.restore_failed.store(true, Ordering::Relaxed);
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    pub(super) fn read(&self, id: &str) -> Value {
        self.read_matching(|snapshot| snapshot.id == id, false, None)
    }
    /// Search only the same bounded, read-only projection as read(job_id).
    /// A miss cannot prove that the Edge invocation never created a job.
    pub(super) fn read_by_invocation(&self, invocation: &str) -> Value {
        // Echo the exact queried identity only after a unique, observed match.
        // The Edge can then distinguish a proven retry alias from a foreign
        // original invocation without exposing the persisted alias roster.
        let verified_invocation =
            okx_research_protocol::research_ipc::InvocationId::new(invocation.to_owned()).ok();
        self.read_matching(
            |snapshot| {
                snapshot
                    .origin_invocation
                    .as_ref()
                    .is_some_and(|origin| origin.as_str() == invocation)
                    || snapshot
                        .retry_invocations
                        .iter()
                        .any(|origin| origin.as_str() == invocation)
            },
            true,
            verified_invocation.as_ref(),
        )
    }
    fn read_matching(
        &self,
        matches: impl Fn(&Snapshot) -> bool,
        by_invocation: bool,
        verified_invocation: Option<&okx_research_protocol::research_ipc::InvocationId>,
    ) -> Value {
        let dropped = self.dropped.load(Ordering::Relaxed);
        let partial = self.restore_partial.load(Ordering::Relaxed);
        let lock = self.state.try_read();
        let reason = match &lock {
            Err(std::sync::TryLockError::WouldBlock) => Some("JOB_TRACE_BUSY"),
            Err(std::sync::TryLockError::Poisoned(_)) => Some("JOB_TRACE_UNAVAILABLE"),
            Ok(_) => None,
        };
        if let Some(reason) = reason {
            return json!({"schema":"okx.research-job-diagnostics/v1","state":"NOT_EVALUATED",
                "reason_code":reason,"quality":"partial","history":"UNKNOWN",
                "dropped_events":dropped,"read_only":true});
        }
        let state = lock.expect("projection read lock checked above");
        let mut matching = state.entries.iter().filter(|snapshot| matches(snapshot));
        let first = matching.next();
        // Multiple jobs may share one invocation. Never select a winner.
        let ambiguous = by_invocation && matching.next().is_some();
        let snapshot = if ambiguous { None } else { first };
        let observed = snapshot.is_some();
        let mut result = json!({"schema":"okx.research-job-diagnostics/v1","owner":"research_job_catalog",
            "state":if ambiguous {"NOT_EVALUATED"}else if observed {"OBSERVED"}else{"NOT_OBSERVED"},
            "reason_code":if ambiguous {"AMBIGUOUS_REQUEST_ORIGIN"}else if observed {"JOB_SNAPSHOT"}else{"UNKNOWN_NOT_RETAINED"},
            "quality":"partial","history":"UNKNOWN","job":snapshot,
            "age_ms":snapshot.map(|s|jiff::Timestamp::now().as_millisecond().saturating_sub(s.observed_at_ms).max(0)),
            "retained":state.entries.len(),"retention_limit":LIMIT,"evicted":state.evicted,
            "dropped_events":dropped,"startup_subset_truncated":partial,
            "startup_subset_selection":"latest_created_ms_then_id",
            "startup_restore_failed":self.restore_failed.load(Ordering::Relaxed),"read_only":true,
            "reference_kind":if by_invocation {"request_id"}else{"job_id"},
            "next_action":if ambiguous {"supply_exact_job_id"}else if observed {"inspect_job_snapshot"}else{"retry_read_only_diagnostic"}});
        if by_invocation
            && observed
            && let Some(invocation) = verified_invocation
        {
            result["matched_request_id"] = json!(invocation.as_str());
        }
        result
    }
}

#[cfg(test)]
#[path = "tests/job_diagnostics.rs"]
mod tests;

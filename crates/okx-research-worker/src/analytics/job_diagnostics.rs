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
// Read a bounded prefix of the raw error only inside the SQLite row mapper.
// Neither the projection nor its JSON representation retains raw payloads.
const COLUMNS: &str = "substr(id,1,129),state,progress,
    -- An interrupted, failed or cancelled job may retain a partial blob, but
    -- only the completed state can advertise a usable result reference.
    CASE WHEN state='completed' AND length(artifact)=64 THEN artifact ELSE NULL END,
    substr(error,1,256),pinned,
    CASE WHEN length(candidate_progress)<=512 THEN candidate_progress ELSE NULL END,
    CASE WHEN length(origin_invocation)<=256 THEN origin_invocation ELSE NULL END,
    CASE WHEN length(producer)<=512 THEN producer ELSE NULL END";

/// The one closed set of safe diagnostic reasons shared by durable readback
/// and terminal tracing events. Raw error strings never cross this boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReasonCode {
    Cancelled,
    SourceInvalid,
    NonfiniteSource,
    CandleRangeUnavailable,
    CandleRangeIncomplete,
    CandleInteriorGap,
    InvalidFunding,
    FundingRangeUnavailable,
    FundingRangeIncomplete,
    GlobalRankSourceIncomplete,
    ResearchMemoryLimit,
    CpuQueueFull,
    LiveSourceDeadline,
    UnsupportedStrategy,
    UnsupportedInverseContract,
    UnsupportedInstrumentType,
    ContractMultiplierMissing,
    EmptyVariants,
    RangeNotClosed,
    ResultExpired,
    ResearchInterrupted,
    LimitExceeded,
    LimitTotalBars,
    LimitBars,
    ResourceBusy,
    MarketSourceRetryable,
    Unclassified,
}

impl ReasonCode {
    fn classify(error: &str) -> Self {
        match error {
            "CANCELLED" | "JOB_CANCELLED" => Self::Cancelled,
            "SOURCE_INVALID" => Self::SourceInvalid,
            "NONFINITE_SOURCE" => Self::NonfiniteSource,
            "CANDLE_RANGE_UNAVAILABLE" => Self::CandleRangeUnavailable,
            "CANDLE_RANGE_INCOMPLETE" => Self::CandleRangeIncomplete,
            "CANDLE_INTERIOR_GAP" => Self::CandleInteriorGap,
            "INVALID_FUNDING" => Self::InvalidFunding,
            "FUNDING_RANGE_UNAVAILABLE" => Self::FundingRangeUnavailable,
            "FUNDING_RANGE_INCOMPLETE" => Self::FundingRangeIncomplete,
            "GLOBAL_RANK_SOURCE_INCOMPLETE" => Self::GlobalRankSourceIncomplete,
            "RESEARCH_MEMORY_LIMIT" => Self::ResearchMemoryLimit,
            "CPU_QUEUE_FULL" => Self::CpuQueueFull,
            "LIVE_SOURCE_DEADLINE" => Self::LiveSourceDeadline,
            "UNSUPPORTED_STRATEGY" => Self::UnsupportedStrategy,
            "UNSUPPORTED_INVERSE_CONTRACT" => Self::UnsupportedInverseContract,
            "UNSUPPORTED_INSTRUMENT_TYPE" => Self::UnsupportedInstrumentType,
            "CONTRACT_MULTIPLIER_MISSING" => Self::ContractMultiplierMissing,
            "EMPTY_VARIANTS" => Self::EmptyVariants,
            "RANGE_NOT_CLOSED" => Self::RangeNotClosed,
            "RESULT_EXPIRED_BY_7_DAY_RETENTION" => Self::ResultExpired,
            "process restarted before completion" => Self::ResearchInterrupted,
            _ if error.starts_with("LIMIT_EXCEEDED:") => Self::LimitExceeded,
            _ if error.starts_with("LIMIT_TOTAL_BARS:") => Self::LimitTotalBars,
            _ if error.starts_with("LIMIT_BARS:") => Self::LimitBars,
            _ if error.starts_with("RESOURCE_BUSY:") => Self::ResourceBusy,
            _ if error.starts_with("OKX rate/backpressure defer:")
                || error.starts_with("public OKX market API error: OKX rate/backpressure defer:") =>
            {
                Self::MarketSourceRetryable
            }
            _ => Self::Unclassified,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Cancelled => "CANCELLED",
            Self::SourceInvalid => "SOURCE_INVALID",
            Self::NonfiniteSource => "NONFINITE_SOURCE",
            Self::CandleRangeUnavailable => "CANDLE_RANGE_UNAVAILABLE",
            Self::CandleRangeIncomplete => "CANDLE_RANGE_INCOMPLETE",
            Self::CandleInteriorGap => "CANDLE_INTERIOR_GAP",
            Self::InvalidFunding => "INVALID_FUNDING",
            Self::FundingRangeUnavailable => "FUNDING_RANGE_UNAVAILABLE",
            Self::FundingRangeIncomplete => "FUNDING_RANGE_INCOMPLETE",
            Self::GlobalRankSourceIncomplete => "GLOBAL_RANK_SOURCE_INCOMPLETE",
            Self::ResearchMemoryLimit => "RESEARCH_MEMORY_LIMIT",
            Self::CpuQueueFull => "CPU_QUEUE_FULL",
            Self::LiveSourceDeadline => "LIVE_SOURCE_DEADLINE",
            Self::UnsupportedStrategy => "UNSUPPORTED_STRATEGY",
            Self::UnsupportedInverseContract => "UNSUPPORTED_INVERSE_CONTRACT",
            Self::UnsupportedInstrumentType => "UNSUPPORTED_INSTRUMENT_TYPE",
            Self::ContractMultiplierMissing => "CONTRACT_MULTIPLIER_MISSING",
            Self::EmptyVariants => "EMPTY_VARIANTS",
            Self::RangeNotClosed => "RANGE_NOT_CLOSED",
            Self::ResultExpired => "RESULT_EXPIRED_BY_7_DAY_RETENTION",
            Self::ResearchInterrupted => "RESEARCH_INTERRUPTED",
            Self::LimitExceeded => "LIMIT_EXCEEDED",
            Self::LimitTotalBars => "LIMIT_TOTAL_BARS",
            Self::LimitBars => "LIMIT_BARS",
            Self::ResourceBusy => "RESOURCE_BUSY",
            Self::MarketSourceRetryable => "MARKET_SOURCE_RETRYABLE",
            Self::Unclassified => "UNCLASSIFIED_RESEARCH_ERROR",
        }
    }
}

// Event labels and the SQLite projection use the same typed classification.
pub(in crate::analytics) fn safe_reason(error: &str) -> &'static str {
    ReasonCode::classify(error).as_str()
}

#[derive(Clone, Serialize)]
struct Snapshot {
    id: String,
    state: String,
    progress: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    candidate_progress: Option<super::super::candidate_progress::CandidateProgress>,
    origin_invocation: Option<okx_protocol::research_ipc::InvocationId>,
    producer: Option<crate::producer::Producer>,
    result_ref: Option<String>,
    error_code: Option<String>,
    pinned: bool,
    observed_at_ms: i64,
    generation: u64,
    origin: &'static str,
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
        producer: row
            .get::<_, Option<String>>(8)?
            .and_then(|v| serde_json::from_str(&v).ok()),
        result_ref: artifact.filter(|v| v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit())),
        // Classify before publication: raw failures never enter Snapshot.
        error_code: error.as_deref().map(safe_reason).map(str::to_owned),
        pinned: row.get(5)?,
        observed_at_ms: 0,
        generation: 0,
        origin: "catalog_commit",
    })
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
            Ok(Some(snapshot)) => self.publish(snapshot),
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
            statement.query_map([], row)?.collect()
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
        let snapshot = state.entries.iter().find(|snapshot| snapshot.id == id);
        json!({"schema":"okx.research-job-diagnostics/v1","owner":"research_job_catalog",
            "state":if snapshot.is_some(){"OBSERVED"}else{"NOT_OBSERVED"},
            "reason_code":if snapshot.is_some(){"JOB_SNAPSHOT"}else{"UNKNOWN_NOT_RETAINED"},
            "quality":"partial","history":"UNKNOWN","job":snapshot,
            "age_ms":snapshot.map(|s|jiff::Timestamp::now().as_millisecond().saturating_sub(s.observed_at_ms).max(0)),
            "retained":state.entries.len(),"retention_limit":LIMIT,"evicted":state.evicted,
            "dropped_events":dropped,"startup_subset_truncated":partial,
            "startup_subset_selection":"latest_created_ms_then_id",
            "startup_restore_failed":self.restore_failed.load(Ordering::Relaxed),"read_only":true,
            "next_action":if snapshot.is_some(){"inspect_job_snapshot"}else{"retry_read_only_diagnostic"}})
    }
}

#[cfg(test)]
mod tests {
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
        let (jobs, path) = jobs("reasons");
        for (n, (raw, expected)) in [
            ("NONFINITE_SOURCE", "NONFINITE_SOURCE"),
            ("INVALID_FUNDING", "INVALID_FUNDING"),
            (
                "GLOBAL_RANK_SOURCE_INCOMPLETE",
                "GLOBAL_RANK_SOURCE_INCOMPLETE",
            ),
            (
                "UNSUPPORTED_INVERSE_CONTRACT",
                "UNSUPPORTED_INVERSE_CONTRACT",
            ),
            (
                "RESULT_EXPIRED_BY_7_DAY_RETENTION",
                "RESULT_EXPIRED_BY_7_DAY_RETENTION",
            ),
            ("process restarted before completion", "RESEARCH_INTERRUPTED"),
            ("CANDLE_INTERIOR_GAP", "CANDLE_INTERIOR_GAP"),
            ("LIMIT_EXCEEDED:RESEARCH_MEMORY", "LIMIT_EXCEEDED"),
            ("LIMIT_TOTAL_BARS:9001", "LIMIT_TOTAL_BARS"),
            ("LIMIT_BARS:9001", "LIMIT_BARS"),
            ("RESOURCE_BUSY:RESEARCH_QUEUE_FULL", "RESOURCE_BUSY"),
            (
                "public OKX market API error: OKX rate/backpressure defer: LocalBudget",
                "MARKET_SOURCE_RETRYABLE",
            ),
            (
                "LIMIT_PRIVATE_TOKEN:DO_NOT_LEAK",
                "UNCLASSIFIED_RESEARCH_ERROR",
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
            // The same reason is sent to tracing by both terminal paths.
            assert_eq!(safe_reason(raw), expected, "event reason for {raw}");
            assert_eq!(observed["job"]["error_code"], expected);
            assert!(!observed.to_string().contains("DO_NOT_LEAK"));
        }
        drop(jobs);
        let restored = Jobs::open(path).unwrap();
        assert_eq!(
            restored.diagnostic_readback(&format!("{:064x}", 0))["job"]["error_code"],
            "NONFINITE_SOURCE"
        );
        assert!(!restored
            .diagnostic_readback(&format!("{:064x}", 13))
            .to_string()
            .contains("DO_NOT_LEAK"));
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
}

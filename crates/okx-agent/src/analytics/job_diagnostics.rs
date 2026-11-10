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
    CASE WHEN length(artifact)=64 THEN artifact ELSE NULL END,
    CASE WHEN error IS NULL THEN NULL
      WHEN error IN ('RESULT_EXPIRED_BY_7_DAY_RETENTION','JOB_CANCELLED','CANCELLED',
        'SOURCE_INVALID','NONFINITE_SOURCE','CANDLE_RANGE_UNAVAILABLE','CANDLE_RANGE_INCOMPLETE',
        'CANDLE_INTERIOR_GAP','INVALID_FUNDING','FUNDING_RANGE_UNAVAILABLE','FUNDING_RANGE_INCOMPLETE',
        'GLOBAL_RANK_SOURCE_INCOMPLETE','RESEARCH_MEMORY_LIMIT','CPU_QUEUE_FULL','LIVE_SOURCE_DEADLINE',
        'UNSUPPORTED_STRATEGY','UNSUPPORTED_INVERSE_CONTRACT','UNSUPPORTED_INSTRUMENT_TYPE',
        'CONTRACT_MULTIPLIER_MISSING','EMPTY_VARIANTS','RANGE_NOT_CLOSED') THEN error
      WHEN error='process restarted before completion' THEN 'RESEARCH_INTERRUPTED'
      WHEN error LIKE 'LIMIT_EXCEEDED:%' THEN 'LIMIT_EXCEEDED'
      WHEN error LIKE 'LIMIT_TOTAL_BARS:%' THEN 'LIMIT_TOTAL_BARS'
      WHEN error LIKE 'LIMIT_BARS:%' THEN 'LIMIT_BARS'
      WHEN error LIKE 'RESOURCE_BUSY:%' THEN 'RESOURCE_BUSY'
      WHEN error LIKE 'OKX rate/backpressure defer:%'
        OR error LIKE 'public OKX market API error: OKX rate/backpressure defer:%' THEN 'MARKET_SOURCE_RETRYABLE'
      ELSE 'UNCLASSIFIED_RESEARCH_ERROR' END,pinned,
    CASE WHEN length(candidate_progress)<=512 THEN candidate_progress ELSE NULL END";

#[derive(Clone, Serialize)]
struct Snapshot {
    id: String,
    state: String,
    progress: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    candidate_progress: Option<super::super::candidate_progress::CandidateProgress>,
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
        result_ref: artifact.filter(|v| v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit())),
        // SQL emits only the fixed whitelist above, never raw failure payloads.
        error_code: error,
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
        snapshot.observed_at_ms = chrono::Utc::now().timestamp_millis();
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
        // PK-order subset, not a claim of most-recent jobs or complete history.
        let restored = (|| -> rusqlite::Result<Vec<Snapshot>> {
            let mut statement = db.prepare(&format!(
                "SELECT {COLUMNS} FROM research_jobs ORDER BY id LIMIT 129"
            ))?;
            statement.query_map([], row)?.collect()
        })();
        match restored {
            Ok(rows) => {
                self.restore_partial
                    .store(rows.len() > LIMIT, Ordering::Relaxed);
                for mut snapshot in rows.into_iter().take(LIMIT) {
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
            "age_ms":snapshot.map(|s|chrono::Utc::now().timestamp_millis().saturating_sub(s.observed_at_ms).max(0)),
            "retained":state.entries.len(),"retention_limit":LIMIT,"evicted":state.evicted,
            "dropped_events":dropped,"startup_subset_truncated":partial,
            "startup_restore_failed":self.restore_failed.load(Ordering::Relaxed),"read_only":true,
            "next_action":if snapshot.is_some(){"inspect_job_snapshot"}else{"retry_read_only_diagnostic"}})
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Jobs, State as JobState};
    use super::*;
    #[derive(Default)]
    struct ForbiddenNetwork(std::sync::atomic::AtomicUsize);
    impl okx_api::HttpTransport for ForbiddenNetwork {
        fn send(&self, _: okx_api::HttpRequest, _: std::time::Instant) -> okx_api::HttpSend<'_> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Box::pin(async {
                Err(okx_api::OkxError::Response(
                    "DIAGNOSTIC_NETWORK_FORBIDDEN".into(),
                ))
            })
        }
    }
    fn jobs(label: &str) -> (Jobs, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "okx-job-diagnostic-{label}-{}-{}.sqlite",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
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
    #[tokio::test]
    async fn public_job_diagnostic_completes_with_known_state_while_catalog_sql_is_locked() {
        use crate::{
            accounts::{AccountRuntimeRegistry, initialize_installation},
            analytics::AnalyticsService,
            market_bootstrap::MarketBootstrapper,
            query::DispatchContext,
            risk_bridge::NativeRiskClient,
            trade_service::TradeService,
        };
        use okx_api::{OkxEnvironment, OkxPublicClient, RateBudget, Region};
        use okx_observation::ReferenceRegistry;
        use okx_runtime::PublicWsCoordinator;
        use std::sync::Arc;
        let root = std::env::temp_dir().join(format!(
            "okx-public-job-diagnostic-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        initialize_installation(&root, "installation-diagnostic").unwrap();
        let budget = RateBudget::new();
        let accounts = Arc::new(
            AccountRuntimeRegistry::open(&root, budget.clone())
                .await
                .unwrap(),
        );
        let env = OkxEnvironment::new(Region::Global, false);
        let network = Arc::new(ForbiddenNetwork::default());
        let market = MarketBootstrapper::new(
            OkxPublicClient::with_http_transport(env, budget.clone(), network.clone()).unwrap(),
        );
        let reference = ReferenceRegistry::from_public("2026-10-31T00:00:00Z", vec![]).unwrap();
        let (_coordinator, public_ws) =
            PublicWsCoordinator::new_with_rate_budget(env, reference, budget);
        let analytics = AnalyticsService::new(root.join("analytics"), market.clone()).unwrap();
        let jobs = analytics.jobs.clone();
        let id = "a".repeat(64);
        jobs.enqueue("public-query".into(), id.clone()).unwrap();
        let (risk_tx, mut risk_rx) = tokio::sync::mpsc::channel(8);
        let trade = Arc::new(TradeService::new(
            accounts.clone(),
            public_ws.clone(),
            NativeRiskClient::new(risk_tx),
        ));
        let ctx = DispatchContext {
            accounts,
            analytics,
            public_ws,
            trade,
            market,
        };
        let request = okx_protocol::AgentRequest {
            schema: okx_protocol::AGENT_REQUEST_SCHEMA_V1.into(),
            request_id: "job_diagnostic_public".into(),
            account: None,
            operation: okx_protocol::AgentOperation::Diagnostics {
                request: okx_protocol::trading::DiagnosticsRequest {
                    reference: Some(id),
                    account: None,
                    view: Some(okx_protocol::trading::DiagnosticsView::Research),
                    cycle_id: None,
                    limit: None,
                },
            },
        };
        request.validate().unwrap();
        let held = jobs.db.lock().unwrap();
        let changes_before = held.total_changes();
        let (send, recv) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let response = runtime.block_on(crate::query::dispatch(&ctx, &request));
            let mut routed = request.clone();
            routed.account = Some("another-account".into());
            let refused = runtime.block_on(crate::query::dispatch(&ctx, &routed));
            assert_eq!(
                refused.failure.unwrap().code,
                "RESEARCH_DIAGNOSTICS_ACCOUNT_NOT_APPLICABLE"
            );
            let mut absent = request.clone();
            if let okx_protocol::AgentOperation::Diagnostics { request } = &mut absent.operation {
                request.reference = Some("b".repeat(64));
            }
            let unknown = runtime
                .block_on(crate::query::dispatch(&ctx, &absent))
                .result
                .unwrap();
            assert_eq!(unknown["reason_code"], "UNKNOWN_NOT_RETAINED");
            assert!(unknown["job"].is_null());
            send.send(response).unwrap();
        });
        let response = recv.recv_timeout(std::time::Duration::from_millis(500));
        assert_eq!(
            held.total_changes(),
            changes_before,
            "diagnostic cannot mutate catalog"
        );
        drop(held);
        thread.join().unwrap();
        let response = response.expect("public diagnostic waited behind catalog SQL");
        assert_eq!(
            response.status,
            okx_protocol::AgentResponseStatus::Completed
        );
        assert_eq!(response.quality, okx_protocol::DataQuality::Degraded);
        let result = response.result.unwrap();
        assert_eq!(result["job"]["state"], "queued");
        assert_eq!(result["job"]["progress"], 0);
        assert_eq!(result["history"], "UNKNOWN");
        assert_eq!(
            network.0.load(Ordering::Relaxed),
            0,
            "diagnostic cannot fetch remote facts"
        );
        assert!(matches!(
            risk_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
                | Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        ));
        drop(jobs);
        std::fs::remove_dir_all(root).unwrap();
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
            let sql = format!("SELECT {COLUMNS} FROM research_jobs ORDER BY id LIMIT 129");
            let plan = db
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap()
                .query_map([], |r| r.get::<_, String>(3))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
                .join(" ");
            assert!(plan.contains("sqlite_autoindex_research_jobs_1"), "{plan}");
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
            reopened.diagnostic_readback(&format!("{:064x}", 129))["state"],
            "NOT_OBSERVED"
        );
    }
}

//! Bounded diagnostic evidence only; no financial data, SQL text or second owner.
use serde::Serialize;
use std::{
    collections::VecDeque,
    sync::{Mutex, OnceLock},
};

const SUMMARY_MAX: usize = 2048;
const DETAIL_MAX: usize = 128;
const PHASE_MAX: usize = 10;
const SOURCE_PHASE_MAX: usize = 2;
const ERROR_CODE_MAX: usize = 64;
const STATEMENT_MAX: usize = 32;
const ACCOUNT_TRACE_BUDGET_BYTES: usize = 8 * 1024 * 1024;

// The incarnation is scoped to one process start; sequence IDs reset only on restart.
fn incarnation() -> &'static str {
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(|| {
        let started = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("p{}-t{started}", std::process::id())
    })
}

fn error_code(code: String) -> String {
    if !code.is_empty()
        && code.len() <= ERROR_CODE_MAX
        && code
            .bytes()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'_')
    {
        code
    } else {
        "UNCLASSIFIED_FINANCE_ERROR".into()
    }
}

// Conservative bound: includes inline VecDeque slots, heap phase buffers,
// bounded error-code strings and an active cycle. Excludes allocator metadata
// (small, allocator-dependent); margin is more than 7 MiB.
#[cfg(test)]
fn worst_case_allocated_bytes() -> usize {
    let cycle = std::mem::size_of::<Cycle>()
        + PHASE_MAX * std::mem::size_of::<Phase>()
        + SOURCE_PHASE_MAX * std::mem::size_of::<Phase>()
        + ERROR_CODE_MAX
        + STATEMENT_MAX * std::mem::size_of::<SqlStatement>();
    SUMMARY_MAX * (std::mem::size_of::<Brief>() + ERROR_CODE_MAX) + DETAIL_MAX * cycle + cycle
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct Phase {
    pub name: &'static str,
    pub elapsed_us: Option<u64>,
    pub outcome: &'static str,
}
#[derive(Clone, Debug, Serialize)]
pub(crate) struct Transaction {
    pub begin_us: u64,
    pub body_us: u64,
    pub commit_us: u64,
    pub commit_outcome: &'static str,
}
#[derive(Clone, Debug, Serialize)]
pub(crate) struct SqlStatement {
    pub statement_id: &'static str,
    pub count: u32,
    pub elapsed_us: u64,
    pub max_us: u64,
    pub rows: u64,
    pub vm_steps: u64,
    pub fullscan_steps: u64,
    pub sorts: u64,
    pub failures: u32,
}
#[derive(Clone, Debug, Serialize)]
pub(crate) struct Cycle {
    pub id: u64,
    pub started_ms: i64,
    pub finished_ms: Option<i64>,
    pub private_ws_generation: u64,
    pub outcome: &'static str,
    pub error_code: Option<String>,
    pub elapsed_ms: Option<u64>,
    pub queue_ms: Option<u64>,
    pub owner_ms: Option<u64>,
    pub phases: Vec<Phase>,
    pub source_phases: Vec<Phase>,
    pub sql_details: Vec<SqlStatement>,
    pub details_truncated: bool,
    pub transaction: Option<Transaction>,
}
#[derive(Clone, Debug, Serialize)]
pub(crate) struct Brief {
    pub id: u64,
    pub started_ms: i64,
    pub outcome: &'static str,
    pub error_code: Option<String>,
    pub elapsed_ms: Option<u64>,
    pub owner_ms: Option<u64>,
}
impl From<&Cycle> for Brief {
    fn from(v: &Cycle) -> Self {
        Self {
            id: v.id,
            started_ms: v.started_ms,
            outcome: v.outcome,
            error_code: v.error_code.clone(),
            elapsed_ms: v.elapsed_ms,
            owner_ms: v.owner_ms,
        }
    }
}
#[derive(Default)]
struct State {
    active: Option<Cycle>,
    recent: VecDeque<Brief>,
    detailed: VecDeque<Cycle>,
    evicted_recent: u64,
    evicted_detailed: u64,
}
#[derive(Default)]
pub(crate) struct FinanceTrace {
    state: Mutex<State>,
}
#[derive(Serialize)]
pub(crate) struct Readback {
    pub schema: &'static str,
    pub state: &'static str,
    pub reason_code: Option<&'static str>,
    pub history_complete: &'static str,
    pub process_incarnation: &'static str,
    pub memory_budget_bytes: usize,
    pub active: Option<Cycle>,
    pub recent: Vec<Brief>,
    pub problem_ids: Vec<u64>,
    pub selected: Option<Cycle>,
    pub selected_summary: Option<Brief>,
    pub selected_details_unavailable: bool,
    pub selected_not_found: bool,
    pub eviction_counts_known: bool,
    pub evicted_recent: u64,
    pub evicted_detailed: u64,
}
impl FinanceTrace {
    fn state<T>(&self, f: impl FnOnce(&mut State) -> T) -> T {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut s)
    }
    fn retain(s: &mut State, c: Cycle) {
        if c.outcome != "succeeded"
            || c.owner_ms.is_some_and(|x| x >= 500)
            || c.elapsed_ms.is_some_and(|x| x >= 1800)
        {
            if s.detailed.len() == DETAIL_MAX {
                s.detailed.pop_front();
                s.evicted_detailed += 1;
            }
            s.detailed.push_back(c.clone());
        }
        if s.recent.len() == SUMMARY_MAX {
            s.recent.pop_front();
            s.evicted_recent += 1;
        }
        s.recent.push_back(Brief::from(&c));
    }
    pub fn begin(&self, id: u64, started_ms: i64, generation: u64) {
        self.state(|s| {
            if let Some(mut old) = s.active.take() {
                old.outcome = "interrupted";
                old.error_code = Some("CAPTURE_INTERRUPTED".into());
                Self::retain(s, old);
            }
            s.active = Some(Cycle {
                id,
                started_ms,
                finished_ms: None,
                private_ws_generation: generation,
                outcome: "in_progress",
                error_code: None,
                elapsed_ms: None,
                queue_ms: None,
                owner_ms: None,
                phases: Vec::with_capacity(PHASE_MAX),
                source_phases: Vec::with_capacity(SOURCE_PHASE_MAX),
                sql_details: Vec::new(),
                details_truncated: false,
                transaction: None,
            });
        });
    }
    pub fn phase_started(&self, id: u64, name: &'static str) {
        self.state(|s| {
            if let Some(c) = s.active.as_mut().filter(|c| c.id == id) {
                let target = if name == "observer_and_cursors" || name == "history_and_ingest" {
                    &mut c.source_phases
                } else {
                    &mut c.phases
                };
                let cap = if name == "observer_and_cursors" || name == "history_and_ingest" {
                    SOURCE_PHASE_MAX
                } else {
                    PHASE_MAX
                };
                if target.len() < cap {
                    target.push(Phase {
                        name,
                        elapsed_us: None,
                        outcome: "in_progress",
                    });
                } else {
                    c.details_truncated = true;
                }
            }
        });
    }
    pub fn phase_finished(&self, id: u64, name: &'static str, elapsed_us: u64, ok: bool) {
        self.state(|s| {
            if let Some(c) = s.active.as_mut().filter(|c| c.id == id) {
                let phases = if name == "observer_and_cursors" || name == "history_and_ingest" {
                    &mut c.source_phases
                } else {
                    &mut c.phases
                };
                if let Some(p) = phases
                    .iter_mut()
                    .rev()
                    .find(|p| p.name == name && p.elapsed_us.is_none())
                {
                    p.elapsed_us = Some(elapsed_us);
                    p.outcome = if ok { "ok" } else { "error" };
                }
            }
        });
    }
    /// Aggregate fixed statement IDs only; never store SQL text or parameters.
    pub fn statement_finished(&self, id: u64, stat: okx_execution::AccountSqlStatementStat) {
        self.state(|s| {
            if let Some(c) = s.active.as_mut().filter(|c| c.id == id) {
                if let Some(v) = c
                    .sql_details
                    .iter_mut()
                    .find(|v| v.statement_id == stat.statement_id)
                {
                    v.count = v.count.saturating_add(1);
                    v.elapsed_us = v.elapsed_us.saturating_add(stat.elapsed_us);
                    v.max_us = v.max_us.max(stat.elapsed_us);
                    v.rows = v.rows.saturating_add(stat.rows);
                    v.vm_steps = v.vm_steps.saturating_add(stat.vm_steps);
                    v.fullscan_steps = v.fullscan_steps.saturating_add(stat.fullscan_steps);
                    v.sorts = v.sorts.saturating_add(stat.sorts);
                    v.failures = v.failures.saturating_add(u32::from(!stat.succeeded));
                } else if c.sql_details.len() < STATEMENT_MAX {
                    c.sql_details.push(SqlStatement {
                        statement_id: stat.statement_id,
                        count: 1,
                        elapsed_us: stat.elapsed_us,
                        max_us: stat.elapsed_us,
                        rows: stat.rows,
                        vm_steps: stat.vm_steps,
                        fullscan_steps: stat.fullscan_steps,
                        sorts: stat.sorts,
                        failures: u32::from(!stat.succeeded),
                    });
                } else {
                    c.details_truncated = true;
                }
            }
        });
    }

    /// The commit observation is published without joining another SQL job.
    pub fn transaction_finished(
        &self,
        id: u64,
        begin_us: u64,
        body_us: u64,
        commit_us: u64,
        commit_succeeded: bool,
    ) {
        self.state(|s| {
            if let Some(c) = s.active.as_mut().filter(|c| c.id == id) {
                c.transaction = Some(Transaction {
                    begin_us,
                    body_us,
                    commit_us,
                    commit_outcome: if commit_succeeded { "ok" } else { "error" },
                });
            }
        });
    }
    pub fn owner_finished(&self, id: u64, queue_ms: u64, owner_ms: u64) {
        self.state(|s| {
            if let Some(c) = s.active.as_mut().filter(|c| c.id == id) {
                c.queue_ms = Some(queue_ms);
                c.owner_ms = Some(owner_ms);
            }
        });
    }
    pub fn finish(&self, id: u64, finished_ms: i64, elapsed_ms: u64, error: Option<String>) {
        self.state(|s| {
            if let Some(mut c) = s.active.take() {
                if c.id != id {
                    s.active = Some(c);
                    return;
                }
                c.finished_ms = Some(finished_ms);
                c.elapsed_ms = Some(elapsed_ms);
                c.outcome = if error.is_some() {
                    "failed"
                } else {
                    "succeeded"
                };
                c.error_code = error.map(error_code);
                Self::retain(s, c);
            }
        });
    }
    /// Never queue a diagnostic behind an observer. Absence while busy is
    /// unevaluated, not proof that a cycle is missing or financial work failed.
    pub fn readback(&self, id: Option<u64>, limit: usize) -> Readback {
        let lock = self.state.try_lock();
        let failure = match &lock {
            Err(std::sync::TryLockError::WouldBlock) => Some("FINANCE_TRACE_BUSY"),
            Err(std::sync::TryLockError::Poisoned(_)) => Some("FINANCE_TRACE_UNAVAILABLE"),
            Ok(_) => None,
        };
        if let Some(reason) = failure {
            return Readback {
                schema: "okx.finance-cycle-trace/v1",
                state: "NOT_EVALUATED",
                reason_code: Some(reason),
                history_complete: "UNKNOWN",
                process_incarnation: incarnation(),
                memory_budget_bytes: ACCOUNT_TRACE_BUDGET_BYTES,
                active: None,
                recent: vec![],
                problem_ids: vec![],
                selected: None,
                selected_summary: None,
                selected_details_unavailable: false,
                selected_not_found: false,
                eviction_counts_known: false,
                evicted_recent: 0,
                evicted_detailed: 0,
            };
        }
        let s = lock.expect("trace lock checked above");
        let selected = id.and_then(|key| {
            s.active
                .iter()
                .chain(s.detailed.iter())
                .find(|c| c.id == key)
                .cloned()
        });
        let selected_summary =
            id.and_then(|key| s.recent.iter().find(|cycle| cycle.id == key).cloned());
        Readback {
            schema: "okx.finance-cycle-trace/v1",
            state: if s.active.is_none() && s.recent.is_empty() {
                "NOT_OBSERVED"
            } else {
                "OBSERVED"
            },
            reason_code: if s.active.is_none() && s.recent.is_empty() {
                Some("VOLATILE_PROJECTION_UNAVAILABLE")
            } else {
                None
            },
            history_complete: "UNKNOWN",
            process_incarnation: incarnation(),
            memory_budget_bytes: ACCOUNT_TRACE_BUDGET_BYTES,
            active: s.active.clone(),
            recent: s.recent.iter().rev().take(limit.min(4)).cloned().collect(),
            problem_ids: s.detailed.iter().rev().take(3).map(|c| c.id).collect(),
            selected_not_found: id.is_some() && selected.is_none() && selected_summary.is_none(),
            selected_details_unavailable: selected.is_none() && selected_summary.is_some(),
            selected,
            selected_summary,
            eviction_counts_known: true,
            evicted_recent: s.evicted_recent,
            evicted_detailed: s.evicted_detailed,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn public_finance_diagnostic_does_not_queue_behind_sql_or_trace() {
        use crate::{accounts, analytics::AnalyticsService, query, trade_service::TradeService};
        use okx_api::{OkxEnvironment, OkxPublicClient, RateBudget, Region};
        use okx_protocol::{
            AgentOperation, AgentRequest, DataQuality,
            trading::{DiagnosticsRequest, DiagnosticsView},
        };
        use std::sync::Arc;
        use std::time::{Duration, Instant};
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce).unwrap();
        let root =
            std::env::temp_dir().join(format!("okx-explain-{:032x}", u128::from_le_bytes(nonce)));
        accounts::initialize_installation(&root, "installation-diagnostic-test").unwrap();
        let budget = RateBudget::new();
        let registry = Arc::new(
            accounts::AccountRuntimeRegistry::open(&root, budget.clone())
                .await
                .unwrap(),
        );
        let feed = Arc::new(accounts::recorded_support::RecordedAccountHttp::default());
        let (config, clients) =
            accounts::recorded_support::prepared("a", feed.clone(), budget.clone());
        registry.connect_prepared(config, clients).await.unwrap();
        // Complete normal source teardown first: the subsequent counters concern
        // the diagnostic alone, rather than concurrent background capture.
        registry.shutdown().await;
        let resources = registry.resolve("fixture-a").await.unwrap();
        let env = OkxEnvironment::new(Region::Global, false);
        let market = crate::market_bootstrap::MarketBootstrapper::new(
            OkxPublicClient::with_rate_budget(env, budget.clone()).unwrap(),
        );
        let reference =
            okx_observation::ReferenceRegistry::from_public("2026-10-31T00:00:00Z", vec![])
                .unwrap();
        let (_public, public_ws) =
            okx_runtime::PublicWsCoordinator::new_with_rate_budget(env, reference, budget);
        let analytics = AnalyticsService::new(root.join("analytics"), market.clone()).unwrap();
        let (risk_tx, mut risk_rx) = tokio::sync::mpsc::channel(4);
        let trade = Arc::new(TradeService::new(
            registry.clone(),
            public_ws.clone(),
            crate::risk_bridge::NativeRiskClient::new(risk_tx),
        ));
        let ctx = query::DispatchContext {
            accounts: registry.clone(),
            analytics,
            public_ws,
            trade,
            market,
        };
        resources.finance_trace.begin(99, 100, 1);
        let request = AgentRequest {
            schema: okx_protocol::AGENT_REQUEST_SCHEMA_V1.into(),
            request_id: "finance-explain-001".into(),
            account: Some("fixture-a".into()),
            operation: AgentOperation::Diagnostics {
                request: DiagnosticsRequest {
                    account: Some("fixture-a".into()),
                    reference: None,
                    view: Some(DiagnosticsView::Finance),
                    cycle_id: Some(99),
                    limit: Some(4),
                },
            },
        };
        request.validate().unwrap();
        let healthy = query::dispatch(&ctx, &request).await.result.unwrap();
        assert_eq!(healthy["state"], "OBSERVED");
        assert_eq!(healthy["trace"]["selected"]["id"], 99);
        let storage_request = AgentRequest {
            operation: AgentOperation::Diagnostics {
                request: DiagnosticsRequest {
                    account: Some("fixture-a".into()),
                    reference: None,
                    view: Some(DiagnosticsView::Storage),
                    cycle_id: None,
                    limit: None,
                },
            },
            ..request.clone()
        };
        storage_request.validate().unwrap();
        resources
            .execution
            .store
            .call(Instant::now() + Duration::from_secs(3), |store| {
                store.apply_authority_policy(
                    1,
                    true,
                    "diagnostic-test-campaign",
                    rust_decimal::Decimal::from(10),
                )
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while resources
                .execution
                .store
                .diagnostic_storage_snapshot()
                .snapshot
                .as_ref()
                .is_none_or(|s| s.policy.revision != 1)
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let store = resources.execution.store.clone();
        let blocked_sql = tokio::spawn(async move {
            store
                .call(Instant::now() + Duration::from_secs(5), move |_| {
                    let _ = entered_tx.send(());
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(())
                })
                .await
        });
        entered_rx.await.unwrap();
        let (held_tx, held_rx) = tokio::sync::oneshot::channel();
        let (unlock_tx, unlock_rx) = std::sync::mpsc::channel();
        let holder_resources = resources.clone();
        let holder = std::thread::spawn(move || {
            let _held = holder_resources.finance_trace.state.lock().unwrap();
            let _ = held_tx.send(());
            unlock_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        held_rx.await.unwrap();
        let sql_before = resources.execution.store.sql_timing().observed_jobs;
        let http_before = feed.requests.lock().unwrap().len();
        let busy =
            tokio::time::timeout(Duration::from_millis(100), query::dispatch(&ctx, &request))
                .await
                .unwrap();
        assert_eq!(busy.quality, DataQuality::Degraded);
        let busy = busy.result.unwrap();
        assert_eq!(busy["state"], "NOT_EVALUATED");
        assert_eq!(busy["trace"]["reason_code"], "FINANCE_TRACE_BUSY");
        assert_eq!(busy["trace"]["selected_not_found"], false);
        // A useful storage image survives an occupied SQL owner. Source locks
        // are held too: the reader reports those gaps rather than waiting.
        let source_lock = resources.latest_snapshot.write().await;
        let health_lock = resources.source_capture_health.write().await;
        let mut samples_ns = Vec::with_capacity(1000);
        for _ in 0..1000 {
            let started = Instant::now();
            let response = query::dispatch(&ctx, &storage_request).await;
            let serialized = serde_json::to_vec(&response).unwrap();
            samples_ns.push(started.elapsed().as_nanos());
            let value = response.result.unwrap();
            assert_eq!(value["state"], "OBSERVED");
            assert_eq!(
                value["storage"]["snapshot"]["policy"]["campaign_limit_usd"],
                "10"
            );
            assert_eq!(value["sources"]["reason_code"], "SOURCE_SNAPSHOT_BUSY");
            assert_eq!(value["source_capture"]["reason_code"], "HEALTH_BUSY");
            assert!(!serialized.is_empty());
        }
        let mut sorted = samples_ns.clone();
        sorted.sort_unstable();
        println!(
            "STORAGE_PUBLIC_DIAGNOSTIC_BENCHMARK={}",
            serde_json::json!({
                "scope":"native_dispatch_with_blocked_sql_and_source_locks", "samples_ns":samples_ns,
                "p95_ns":sorted[949], "build":if cfg!(debug_assertions){"debug"}else{"release"},
                "external_calls":0,"sql_jobs":0,"not_a_full_k9_measurement":true
            })
        );
        assert!(sorted[949] <= 100_000_000);
        drop(health_lock);
        drop(source_lock);
        assert_eq!(
            resources.execution.store.sql_timing().observed_jobs,
            sql_before
        );
        assert_eq!(feed.requests.lock().unwrap().len(), http_before);
        assert!(risk_rx.try_recv().is_err());
        unlock_tx.send(()).unwrap();
        holder.join().unwrap();
        let restored = query::dispatch(&ctx, &request).await.result.unwrap();
        assert_eq!(restored["trace"]["selected"]["id"], 99);
        release_tx.send(()).unwrap();
        blocked_sql.await.unwrap().unwrap();
        let mut termination = resources.execution.store.termination_signal();
        drop(resources);
        drop(ctx);
        drop(registry);
        drop(_public);
        tokio::time::timeout(Duration::from_secs(5), termination.wait_for(|done| *done))
            .await
            .unwrap()
            .unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn contended_readback_never_waits_or_claims_a_missing_cycle() {
        let trace = FinanceTrace::default();
        trace.begin(1, 100, 7);
        let held = trace.state.lock().unwrap();
        let start = std::time::Instant::now();
        let busy = trace.readback(Some(1), usize::MAX);
        assert!(start.elapsed() < std::time::Duration::from_millis(100));
        assert_eq!(busy.state, "NOT_EVALUATED");
        assert_eq!(busy.reason_code, Some("FINANCE_TRACE_BUSY"));
        assert_eq!(busy.history_complete, "UNKNOWN");
        assert!(!busy.selected_not_found);
        assert!(!busy.eviction_counts_known);
        drop(held);
        assert!(trace.readback(Some(1), 4).selected.is_some());
        let restarted = FinanceTrace::default().readback(Some(1), 4);
        assert_eq!(restarted.history_complete, "UNKNOWN");
        assert!(restarted.selected_not_found);
    }

    #[test]
    fn healthy_cycle_summary_is_observed_even_when_detail_was_not_retained() {
        let trace = FinanceTrace::default();
        trace.begin(1, 100, 7);
        trace.finish(1, 110, 10, None);
        let retained = trace.readback(Some(1), usize::MAX);
        assert!(retained.selected.is_none());
        assert_eq!(retained.selected_summary.unwrap().id, 1);
        assert!(retained.selected_details_unavailable);
        assert!(!retained.selected_not_found);
        assert_eq!(retained.state, "OBSERVED");
        assert!(retained.eviction_counts_known);
    }
    #[test]
    fn failed_phase_remains_visible() {
        let t = FinanceTrace::default();
        t.begin(1, 100, 7);
        t.phase_started(1, "account_nav_commit");
        assert_eq!(
            t.readback(Some(1), 1).active.unwrap().phases[0].outcome,
            "in_progress"
        );
        t.phase_finished(1, "account_nav_commit", 4_200_000, false);
        t.owner_finished(1, 0, 4200);
        t.transaction_finished(1, 100, 4_100_000, 99_000, false);
        t.finish(1, 4400, 4300, Some("ACCOUNT_STORAGE_DEADLINE".into()));
        let r = t.readback(Some(1), 4);
        let selected = r.selected.unwrap();
        assert_eq!(selected.owner_ms, Some(4200));
        assert_eq!(selected.transaction.unwrap().commit_outcome, "error");
        assert_eq!(r.problem_ids, vec![1]);
    }
    #[test]
    fn external_source_failure_is_attributed_without_sql_owner() {
        let t = FinanceTrace::default();
        t.begin(10, 200, 2);
        t.phase_started(10, "observer_and_cursors");
        t.phase_finished(10, "observer_and_cursors", 11_000, true);
        t.phase_started(10, "history_and_ingest");
        t.phase_finished(10, "history_and_ingest", 33_000, false);
        t.finish(10, 244, 44, Some("SWAP_FILLS_SOURCE_UNAVAILABLE".into()));
        let cycle = t.readback(Some(10), 1).selected.unwrap();
        assert_eq!(cycle.source_phases.len(), 2);
        assert_eq!(cycle.source_phases[0].outcome, "ok");
        assert_eq!(cycle.source_phases[1].outcome, "error");
        assert_eq!(cycle.owner_ms, None);
    }

    #[test]
    fn readback_is_available_while_an_owner_phase_remains_in_progress() {
        let trace = std::sync::Arc::new(FinanceTrace::default());
        trace.begin(17, 100, 3);
        trace.phase_started(17, "account_nav_commit");
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let owner = std::sync::Arc::clone(&trace);
        let worker = std::thread::spawn(move || {
            entered_tx.send(()).unwrap();
            resume_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap();
            owner.phase_finished(17, "account_nav_commit", 20_000, true);
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        let active = trace.readback(Some(17), 4).active.unwrap();
        assert_eq!(active.phases[0].outcome, "in_progress");
        assert!(active.phases[0].elapsed_us.is_none());
        resume_tx.send(()).unwrap();
        worker.join().unwrap();
        assert_eq!(
            trace.readback(Some(17), 4).active.unwrap().phases[0].outcome,
            "ok"
        );
    }

    #[test]
    fn source_phases_and_sql_phases_do_not_exceed_contract() {
        let t = FinanceTrace::default();
        t.begin(1, 0, 1);
        for _ in 0..14 {
            t.phase_started(1, "account_nav_commit");
            t.phase_started(1, "history_and_ingest");
        }
        let active = t.readback(None, 4).active.unwrap();
        assert_eq!(active.phases.len(), PHASE_MAX);
        assert_eq!(active.source_phases.len(), SOURCE_PHASE_MAX);
        assert!(active.details_truncated);
    }

    #[test]
    fn memory_and_reply_are_bounded_with_maximal_history() {
        assert!(worst_case_allocated_bytes() < ACCOUNT_TRACE_BUDGET_BYTES);
        let t = FinanceTrace::default();
        for i in 1..=2050 {
            t.begin(i, i as i64, 1);
            for _ in 0..PHASE_MAX {
                t.phase_started(i, "account_nav_commit");
                t.phase_finished(i, "account_nav_commit", 9_999_999, true);
            }
            t.finish(i, i as i64 + 2000, 2000, Some("E".repeat(10_000)));
        }
        let summary = t.readback(None, 4);
        assert_eq!(summary.problem_ids.len(), 3);
        assert_eq!(
            summary.recent[0].error_code.as_deref(),
            Some("UNCLASSIFIED_FINANCE_ERROR")
        );
        assert!(serde_json::to_vec(&summary).unwrap().len() <= 12 * 1024);
        let detail = t.readback(Some(2050), 4);
        assert!(serde_json::to_vec(&detail).unwrap().len() <= 32 * 1024);
        assert_eq!(incarnation(), detail.process_incarnation);
    }

    #[test]
    fn fixed_sql_statement_metrics_aggregate_and_do_not_leak_parameters() {
        let t = FinanceTrace::default();
        t.begin(7, 10, 2);
        let stat = okx_execution::AccountSqlStatementStat {
            statement_id: "pending_fills_select",
            elapsed_us: 3000,
            rows: 2,
            vm_steps: 400,
            fullscan_steps: 0,
            sorts: 1,
            succeeded: true,
        };
        t.statement_finished(7, stat);
        t.statement_finished(
            7,
            okx_execution::AccountSqlStatementStat {
                succeeded: false,
                ..stat
            },
        );
        t.finish(7, 99, 89, Some("SQL_QUERY_FAILED".into()));
        let readback = t.readback(Some(7), 1);
        let details = &readback.selected.unwrap().sql_details;
        assert_eq!(details.len(), 1);
        assert_eq!(details[0].count, 2);
        assert_eq!(details[0].elapsed_us, 6000);
        assert_eq!(details[0].vm_steps, 800);
        assert_eq!(details[0].failures, 1);
    }

    #[test]
    fn bounded_history_and_stale_callback() {
        let t = FinanceTrace::default();
        for i in 1..=2060 {
            t.begin(i, i as i64, 1);
            t.finish(i, i as i64 + 5, 5, None);
        }
        t.begin(2061, 2061, 1);
        t.owner_finished(2000, 0, 9999);
        assert_eq!(t.readback(None, 4).active.unwrap().owner_ms, None);
        let r = t.readback(Some(1), 4);
        assert!(r.evicted_recent > 0 && r.recent.len() <= 4 && r.selected_not_found);
    }
}

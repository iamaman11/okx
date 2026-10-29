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
        && code.bytes().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'_')
    {
        code
    } else {
        "UNCLASSIFIED_FINANCE_ERROR".into()
    }
}

// Conservative bound: includes inline VecDeque slots, heap phase buffers,
// bounded error-code strings and an active cycle. Excludes allocator metadata
// (small, allocator-dependent); margin is more than 7 MiB.
fn worst_case_allocated_bytes() -> usize {
    let cycle = std::mem::size_of::<Cycle>()
        + PHASE_MAX * std::mem::size_of::<Phase>()
        + SOURCE_PHASE_MAX * std::mem::size_of::<Phase>()
        + ERROR_CODE_MAX;
    SUMMARY_MAX * (std::mem::size_of::<Brief>() + ERROR_CODE_MAX)
        + DETAIL_MAX * cycle + cycle
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
    pub process_incarnation: &'static str,
    pub memory_budget_bytes: usize,
    pub active: Option<Cycle>,
    pub recent: Vec<Brief>,
    pub problem_ids: Vec<u64>,
    pub selected: Option<Cycle>,
    pub selected_not_found: bool,
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
    /// Copy bounded data under lock; JSON serialization happens later.
    pub fn readback(&self, id: Option<u64>, limit: usize) -> Readback {
        self.state(|s| {
            let selected = id.and_then(|key| {
                s.active
                    .iter()
                    .chain(s.detailed.iter())
                    .find(|c| c.id == key)
                    .cloned()
            });
            Readback {
                schema: "okx.finance-cycle-trace/v1",
                process_incarnation: incarnation(),
                memory_budget_bytes: ACCOUNT_TRACE_BUDGET_BYTES,
                active: s.active.clone(),
                recent: s.recent.iter().rev().take(limit).cloned().collect(),
                problem_ids: s.detailed.iter().rev().take(3).map(|c| c.id).collect(),
                selected_not_found: id.is_some() && selected.is_none(),
                selected,
                evicted_recent: s.evicted_recent,
                evicted_detailed: s.evicted_detailed,
            }
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
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
        assert_eq!(summary.recent[0].error_code.as_deref(), Some("UNCLASSIFIED_FINANCE_ERROR"));
        assert!(serde_json::to_vec(&summary).unwrap().len() <= 12 * 1024);
        let detail = t.readback(Some(2050), 4);
        assert!(serde_json::to_vec(&detail).unwrap().len() <= 32 * 1024);
        assert_eq!(incarnation(), detail.process_incarnation);
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

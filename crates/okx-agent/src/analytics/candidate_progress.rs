//! Optional observations of real grid replay work; no catalog or CPU owner.
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Preparing,
    EvaluatingCandidates,
    Publishing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateProgress {
    pub phase: Phase,
    /// A candidate completes only after training, holdout and doubled-fee replay.
    pub completed_candidates: u64,
    pub total_candidates: u64,
    pub completed_work_units: u64,
    pub total_work_units: u64,
}
impl CandidateProgress {
    pub(super) fn valid(&self) -> bool {
        let instruments = self
            .total_work_units
            .checked_sub(self.total_candidates.saturating_mul(3));
        (1..=512).contains(&self.total_candidates)
            && instruments.is_some_and(|n| {
                (1..=8).contains(&n)
                    && self.total_candidates.is_multiple_of(n)
                    && self.total_candidates / n <= 64
            })
            && self.completed_candidates <= self.total_candidates
            && self.completed_work_units <= self.total_work_units
            && self.completed_work_units >= self.completed_candidates.saturating_mul(3)
            && match self.phase {
                Phase::Preparing => {
                    self.completed_work_units == 0 && self.completed_candidates == 0
                }
                Phase::Publishing => {
                    self.completed_work_units == self.total_work_units
                        && self.completed_candidates == self.total_candidates
                }
                Phase::EvaluatingCandidates => {
                    self.completed_work_units > 0
                        && self.completed_work_units < self.total_work_units
                }
            }
    }
}

pub(super) struct Tracker {
    slots: Vec<(AtomicU64, AtomicU64)>,
    variants: u64,
}
impl Tracker {
    pub(super) fn new(instruments: usize, variants: usize) -> Self {
        Self {
            slots: (0..instruments)
                .map(|_| (AtomicU64::new(0), AtomicU64::new(0)))
                .collect(),
            variants: variants as u64,
        }
    }
    /// CPU callback performs only bounded atomic stores. No SQL, allocation,
    /// channel send, locking, waiting, or publication occurs here.
    pub(super) fn observe(
        &self,
        instrument: usize,
        progress: okx_research::strategy_registry::GridProgress,
    ) {
        let (work, candidates) = &self.slots[instrument];
        work.store(progress.completed_work_units, Ordering::Release);
        candidates.store(progress.completed_candidates, Ordering::Release);
    }
    pub(super) fn snapshot(&self) -> CandidateProgress {
        let mut completed_candidates = 0;
        let mut completed_work_units = 0;
        for (work, candidates) in &self.slots {
            completed_candidates += candidates.load(Ordering::Acquire);
            completed_work_units += work.load(Ordering::Acquire);
        }
        let total_work_units = self.slots.len() as u64 * (3 * self.variants + 1);
        CandidateProgress {
            phase: if completed_work_units == total_work_units {
                Phase::Publishing
            } else if completed_work_units == 0 {
                Phase::Preparing
            } else {
                Phase::EvaluatingCandidates
            },
            completed_candidates,
            total_candidates: self.slots.len() as u64 * self.variants,
            completed_work_units,
            total_work_units,
        }
    }
}

pub(super) async fn publish(
    jobs: super::jobs::Jobs,
    id: String,
    value: CandidateProgress,
) -> Result<(), String> {
    tokio::task::spawn_blocking(move || jobs.update_candidate_progress(&id, value))
        .await
        .map_err(|_| "RESEARCH_SQL_TASK_FAILED".to_string())?
}

#[cfg(test)]
mod tests {
    use super::super::{
        jobs::{Jobs, State},
        replay,
    };
    use super::*;
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicBool, AtomicUsize},
    };

    fn financial_files(
        root: &std::path::Path,
    ) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
        let mut files = std::collections::BTreeMap::new();
        for entry in std::fs::read_dir(root).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                files.extend(financial_files(&path));
            } else {
                files.insert(path.clone(), std::fs::read(path).unwrap());
            }
        }
        files
    }

    fn fixture() -> (
        Vec<replay::Bar>,
        replay::ReplaySpec,
        Vec<okx_analysis::operators::EmaParameters>,
    ) {
        let bars = (0..200)
            .map(|i| replay::Bar {
                time: i * 60_000,
                open: 100.0 + i as f64,
                high: 102.0 + i as f64,
                low: 99.0 + i as f64,
                close: 101.0 + i as f64,
                volume: 1.0,
                funding_rate: Some(0.0),
            })
            .collect();
        let variants = (3..67)
            .map(|slow| okx_analysis::operators::EmaParameters { fast: 2, slow })
            .collect();
        let spec = replay::ReplaySpec {
            strategy_id: "ema_cross_v1".into(),
            parameters: okx_analysis::operators::EmaParameters { fast: 2, slow: 3 },
            mechanics: replay::Mechanics::Linear {
                contract_multiplier: 1.0,
            },
            costs: replay::Costs {
                maker_fee_bps: 1.0,
                taker_fee_bps: 2.0,
                slippage_bps: 1.0,
                observed_funding: true,
            },
            timeframe_ms: 60_000,
            historical_observed: false,
            historical_rules: false,
            historical_fees: false,
            seed: 7,
            stop_fraction: None,
            take_profit_fraction: None,
        };
        (bars, spec, variants)
    }

    #[tokio::test]
    async fn candidate_progress_64_real_candidates_public_status_barriers_and_zero_external_effects()
     {
        use okx_api::{HttpSend, HttpTransport, OkxEnvironment, RateBudget};
        use okx_protocol::{
            AGENT_REQUEST_SCHEMA_V1, AgentOperation, AgentRequest, AgentResponseStatus,
            analytics::ResearchRequest,
        };
        struct ForbiddenNetwork(AtomicUsize);
        impl HttpTransport for ForbiddenNetwork {
            fn send(&self, _: okx_api::HttpRequest, _: std::time::Instant) -> HttpSend<'_> {
                self.0.fetch_add(1, Ordering::Relaxed);
                Box::pin(async {
                    Err(okx_api::OkxError::Response(
                        "RESEARCH_NETWORK_FORBIDDEN".into(),
                    ))
                })
            }
        }
        let (root, mut service) = super::super::artifact_job_fixtures::service();
        let id = "a".repeat(64);
        service
            .jobs
            .enqueue("candidate-progress".into(), id.clone())
            .unwrap();
        service
            .jobs
            .update(&id, State::Running, None, None)
            .unwrap();
        let tracker = Arc::new(Tracker::new(1, 64));
        let barrier = Arc::new(Barrier::new(2));
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = {
            let tracker = tracker.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let (bars, spec, variants) = fixture();
                let observed =
                    replay::grid_with_observer(&bars, &spec, &variants, &|| false, &|value| {
                        tracker.observe(0, value);
                        if matches!(value.completed_work_units, 65 | 66) {
                            tx.send(value).unwrap();
                            barrier.wait();
                        }
                    })
                    .unwrap();
                let plain = replay::grid(&bars, &spec, &variants, &|| false).unwrap();
                assert_eq!(
                    serde_json::to_value(observed).unwrap(),
                    serde_json::to_value(plain).unwrap()
                );
            })
        };
        let network = Arc::new(ForbiddenNetwork(AtomicUsize::new(0)));
        let environment = OkxEnvironment::new(okx_api::Region::Global, false);
        let market = crate::market_bootstrap::MarketBootstrapper::new(
            okx_api::OkxPublicClient::with_http_transport(
                environment,
                RateBudget::new(),
                network.clone(),
            )
            .unwrap(),
        );
        service.sources.bootstrap = market.clone();
        let installation = root.join("installation");
        crate::accounts::initialize_installation(&installation, "candidate-progress-installation")
            .unwrap();
        let accounts = Arc::new(
            crate::accounts::AccountRuntimeRegistry::open(&installation, RateBudget::new())
                .await
                .unwrap(),
        );
        let (coordinator, public_ws) = okx_runtime::PublicWsCoordinator::new_with_connector(
            environment,
            okx_observation::ReferenceRegistry::from_public("fixture", vec![]).unwrap(),
            RateBudget::new(),
            Arc::new(crate::accounts::recorded_support::OfflineSocket),
        );
        let (risk_tx, mut risk_rx) = tokio::sync::mpsc::channel(4);
        let trade = Arc::new(crate::trade_service::TradeService::new(
            accounts.clone(),
            public_ws.clone(),
            crate::risk_bridge::NativeRiskClient::new(risk_tx),
        ));
        let context = crate::query::DispatchContext {
            accounts,
            analytics: service,
            public_ws,
            trade,
            market,
        };
        let before = financial_files(&installation);
        for (work, candidates) in [(65, 0), (66, 1)] {
            let event = rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
            assert_eq!(event.completed_work_units, work);
            publish(
                context.analytics.jobs.clone(),
                id.clone(),
                tracker.snapshot(),
            )
            .await
            .unwrap();
            let response = crate::query::dispatch(
                &context,
                &AgentRequest {
                    schema: AGENT_REQUEST_SCHEMA_V1.into(),
                    request_id: format!("candidate-progress-{work}"),
                    account: None,
                    operation: AgentOperation::Research {
                        request: ResearchRequest::Status { job_id: id.clone() },
                    },
                },
            )
            .await;
            assert_eq!(response.status, AgentResponseStatus::Completed);
            let status = response.result.unwrap();
            assert_eq!(status["state"], "running");
            assert_eq!(
                status["candidate_progress"]["completed_candidates"],
                candidates
            );
            assert_eq!(status["candidate_progress"]["total_candidates"], 64);
            assert_eq!(status["candidate_progress"]["completed_work_units"], work);
            assert_eq!(status["candidate_progress"]["total_work_units"], 193);
            assert!(status["progress"].as_u64().unwrap() < 100);
            barrier.wait();
        }
        worker.join().unwrap();
        publish(
            context.analytics.jobs.clone(),
            id.clone(),
            tracker.snapshot(),
        )
        .await
        .unwrap();
        let final_job = context.analytics.jobs.get(&id).unwrap();
        assert_eq!(
            final_job.candidate_progress.unwrap().completed_candidates,
            64
        );
        assert_eq!(final_job.progress, 99);
        let artifact = context
            .analytics
            .artifacts
            .publish_evidence(&serde_json::json!({"finished":true}))
            .unwrap()
            .0;
        context
            .analytics
            .jobs
            .update(&id, State::Completed, Some(artifact), None)
            .unwrap();
        assert_eq!(context.analytics.jobs.get(&id).unwrap().progress, 100);
        assert_eq!(network.0.load(Ordering::Relaxed), 0);
        assert!(risk_rx.try_recv().is_err());
        assert_eq!(before, financial_files(&installation));
        context.accounts.shutdown().await;
        drop(context);
        drop(coordinator);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn candidate_progress_cancel_restart_terminal_fence_and_multi_instrument_totals() {
        let (root, service) = super::super::artifact_job_fixtures::service();
        let jobs = &service.jobs;
        let tracker = Tracker::new(2, 64);
        tracker.observe(
            0,
            replay::GridProgress {
                completed_work_units: 66,
                completed_candidates: 1,
            },
        );
        tracker.observe(
            1,
            replay::GridProgress {
                completed_work_units: 64,
                completed_candidates: 0,
            },
        );
        let snapshot = tracker.snapshot();
        assert_eq!(
            (snapshot.total_candidates, snapshot.total_work_units),
            (128, 386)
        );
        assert_eq!(
            (snapshot.completed_candidates, snapshot.completed_work_units),
            (1, 130)
        );
        for id in ["running", "cancelled"] {
            jobs.enqueue(id.into(), id.into()).unwrap();
            jobs.update(id, State::Running, None, None).unwrap();
            jobs.update_candidate_progress(id, snapshot.clone())
                .unwrap();
        }
        jobs.update_candidate_progress("running", Tracker::new(2, 64).snapshot())
            .unwrap();
        assert_eq!(
            jobs.get("running").unwrap().candidate_progress,
            Some(snapshot.clone())
        );
        jobs.cancel("cancelled", "cancel-key").unwrap();
        tracker.observe(
            0,
            replay::GridProgress {
                completed_work_units: 193,
                completed_candidates: 64,
            },
        );
        jobs.update_candidate_progress("cancelled", tracker.snapshot())
            .unwrap();
        assert_eq!(
            jobs.get("cancelled").unwrap().candidate_progress,
            Some(snapshot.clone())
        );
        drop(service);
        let reopened = Jobs::open(root.join("research-catalog.sqlite")).unwrap();
        assert_eq!(reopened.get("running").unwrap().state, State::Interrupted);
        assert_eq!(
            reopened.get("running").unwrap().candidate_progress,
            Some(snapshot.clone())
        );
        assert_eq!(reopened.get("cancelled").unwrap().state, State::Cancelled);
        reopened
            .update_candidate_progress("running", tracker.snapshot())
            .unwrap();
        assert_eq!(
            reopened.get("running").unwrap().candidate_progress,
            Some(snapshot)
        );
        drop(reopened);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn candidate_progress_unfinished_replay_never_counts_candidate() {
        let (bars, spec, variants) = fixture();
        let cancelled = AtomicBool::new(false);
        let tracker = Tracker::new(1, 64);
        let result = replay::grid_with_observer(
            &bars,
            &spec,
            &variants,
            &|| cancelled.load(Ordering::Acquire),
            &|value| {
                tracker.observe(0, value);
                if value.completed_work_units == 65 {
                    cancelled.store(true, Ordering::Release);
                }
            },
        );
        assert_eq!(result.unwrap_err(), "CANCELLED");
        assert_eq!(tracker.snapshot().completed_candidates, 0);
        assert_eq!(tracker.snapshot().completed_work_units, 65);
    }

    #[test]
    fn candidate_progress_migrates_legacy_catalog_without_inventing_counts() {
        let root = std::env::temp_dir().join(format!(
            "candidate-progress-migration-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("research-catalog.sqlite");
        let db = rusqlite::Connection::open(&path).unwrap();
        db.execute_batch("CREATE TABLE research_jobs(id TEXT PRIMARY KEY,request_hash TEXT NOT NULL,idempotency_key TEXT NOT NULL UNIQUE,state TEXT NOT NULL,progress INTEGER NOT NULL,artifact TEXT,error TEXT,created_ms INTEGER NOT NULL,pinned INTEGER NOT NULL) STRICT;
            INSERT INTO research_jobs VALUES('old','old','old','running',10,NULL,NULL,1,0);").unwrap();
        drop(db);
        let jobs = Jobs::open(path.clone()).unwrap();
        let old = jobs.get("old").unwrap();
        assert_eq!(old.state, State::Interrupted);
        assert_eq!(old.progress, 10);
        assert!(old.candidate_progress.is_none());
        assert!(
            serde_json::to_value(old)
                .unwrap()
                .get("candidate_progress")
                .is_none()
        );
        drop(jobs);
        let reopened = Jobs::open(path).unwrap();
        assert!(reopened.get("old").unwrap().candidate_progress.is_none());
        drop(reopened);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn candidate_progress_actual_submitted_grid_aggregates_two_instruments_64_variants() {
        use okx_protocol::analytics::*;
        let (root, service) = super::super::artifact_job_fixtures::service();
        for symbol in ["BTC-USDT", "ETH-USDT"] {
            let (bars, _, _) = fixture();
            service
                .sources
                .cache
                .load(
                    super::super::cache::SourceKey {
                        source: "candles".into(),
                        instrument: symbol.into(),
                        timeframe_ms: 60_000,
                        start_ms: 0,
                        end_ms: 12_000_000,
                        closed_only: true,
                        schema: 1,
                    },
                    || async { Ok(bars) },
                )
                .await
                .unwrap();
        }
        let request = ResearchRequest::Grid {
            idempotency_key: "candidate-progress-real-grid".into(),
            instruments: vec!["BTC-USDT".into(), "ETH-USDT".into()],
            timeframe_ms: 60_000,
            start_ms: 0,
            end_ms: 12_000_000,
            strategy_id: "ema_cross_v1".into(),
            variants: (3..67)
                .map(|slow| EmaParameters { fast: 2, slow })
                .collect(),
            costs: ResearchCosts {
                maker_fee_bps: 1.0,
                taker_fee_bps: 2.0,
                slippage_bps: 1.0,
                funding_mode: FundingMode::ZeroAssumption,
            },
            data_mode: ResearchDataMode::Counterfactual,
            seed: 7,
        };
        let queued = service.research(request).await.unwrap();
        let id = queued["id"].as_str().unwrap().to_owned();
        let status = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let status = service
                    .research(ResearchRequest::Status { job_id: id.clone() })
                    .await
                    .unwrap();
                if status["state"] == "completed" {
                    break status;
                }
                assert!(
                    matches!(status["state"].as_str(), Some("queued" | "running")),
                    "{status}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(status["candidate_progress"]["completed_candidates"], 128);
        assert_eq!(status["candidate_progress"]["total_candidates"], 128);
        assert_eq!(status["candidate_progress"]["completed_work_units"], 386);
        assert_eq!(status["candidate_progress"]["total_work_units"], 386);
        assert_eq!(status["candidate_progress"]["phase"], "publishing");
        assert_eq!(status["progress"], 100);
        assert!(status["artifact"].as_str().is_some());
        drop(service);
        let reopened = Jobs::open(root.join("research-catalog.sqlite")).unwrap();
        assert_eq!(
            reopened
                .get(&id)
                .unwrap()
                .candidate_progress
                .unwrap()
                .completed_candidates,
            128
        );
        drop(reopened);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn candidate_progress_rejects_invalid_and_regressing_observations() {
        let (root, service) = super::super::artifact_job_fixtures::service();
        let jobs = &service.jobs;
        jobs.enqueue("valid".into(), "valid".into()).unwrap();
        let tracker = Tracker::new(1, 64);
        tracker.observe(
            0,
            replay::GridProgress {
                completed_work_units: 66,
                completed_candidates: 1,
            },
        );
        let good = tracker.snapshot();
        jobs.update_candidate_progress("valid", good.clone())
            .unwrap();
        for malformed in [
            CandidateProgress {
                completed_candidates: 65,
                ..good.clone()
            },
            CandidateProgress {
                total_work_units: 192,
                ..good.clone()
            },
            CandidateProgress {
                completed_work_units: 1,
                ..good.clone()
            },
            CandidateProgress {
                phase: Phase::Preparing,
                ..good.clone()
            },
            CandidateProgress {
                phase: Phase::Publishing,
                ..good.clone()
            },
            CandidateProgress {
                phase: Phase::EvaluatingCandidates,
                completed_candidates: 0,
                completed_work_units: 0,
                ..good.clone()
            },
            CandidateProgress {
                completed_candidates: 64,
                completed_work_units: 193,
                ..good.clone()
            },
        ] {
            assert_eq!(
                jobs.update_candidate_progress("valid", malformed)
                    .unwrap_err(),
                "INVALID_CANDIDATE_PROGRESS"
            );
            assert_eq!(
                jobs.get("valid").unwrap().candidate_progress,
                Some(good.clone())
            );
        }
        jobs.update_candidate_progress("valid", Tracker::new(1, 64).snapshot())
            .unwrap();
        jobs.update_candidate_progress("valid", Tracker::new(2, 64).snapshot())
            .unwrap();
        assert_eq!(jobs.get("valid").unwrap().candidate_progress, Some(good));
        // Projection does not copy arbitrary phases or unbounded counters from
        // a corrupt catalog into a public diagnostic snapshot.
        let db = rusqlite::Connection::open(root.join("research-catalog.sqlite")).unwrap();
        db.execute("UPDATE research_jobs SET candidate_progress=?1 WHERE id='valid'",[r#"{"phase":"secret","completed_candidates":0,"total_candidates":64,"completed_work_units":0,"total_work_units":193}"#]).unwrap();
        drop(db);
        assert!(jobs.get("valid").is_err());
        drop(service);
        let reopened = Jobs::open(root.join("research-catalog.sqlite")).unwrap();
        let diagnostic = reopened.diagnostic_readback("valid");
        assert!(diagnostic["job"].is_null());
        assert!(!diagnostic.to_string().contains("secret"));
        drop(reopened);
        std::fs::remove_dir_all(root).unwrap();
    }
}

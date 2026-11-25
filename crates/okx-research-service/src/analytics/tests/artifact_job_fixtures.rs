use super::*;
// Windows clock granularity is not a unique test-directory owner:
// concurrent Rust tests can observe the same timestamp and open one
// SQLite catalog simultaneously, producing a spurious DATABASE_BUSY.
static NEXT_FIXTURE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
fn unique_fixture_root() -> PathBuf {
    std::env::temp_dir().join(format!(
        "okx-analytics-{}-{}-{}",
        std::process::id(),
        jiff::Timestamp::now().as_nanosecond(),
        NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed),
    ))
}
#[tokio::test]
async fn drain_waits_for_runner_after_durable_cancellation() {
    let (_, service) = service();
    let guard = RunnerGuard::new(service.runners.clone());
    let (job, _, _) = service.jobs.enqueue("key".into(), "id".into()).unwrap();
    service.jobs.cancel(&job.id, "cancel").unwrap();
    assert_eq!(
        service.jobs.get(&job.id).unwrap().state,
        jobs::State::Cancelled
    );
    assert_eq!(service.begin_drain().await.unwrap().active_jobs, 1);
    drop(guard);
    assert_eq!(service.begin_drain().await.unwrap().active_jobs, 0);
}
pub(super) fn service() -> (PathBuf, AnalyticsService) {
    let transport = crate::independent_source::test_support::RecordedGet::new(Vec::new());
    service_with_source(transport.client())
}

pub(super) fn service_with_source(
    source: crate::source_api::PublicSourceClient,
) -> (PathBuf, AnalyticsService) {
    let root = unique_fixture_root();
    let service = AnalyticsService::new(root.clone(), source).unwrap();
    (root, service)
}
// market() promises a bounded synchronous response, NOT completion within
// two seconds under arbitrary simultaneous test CPU load. A queued/running
// job is a valid response; wait for its original durable terminal state,
// then read the same cached artifact. Failure is reported before row access.
async fn completed_market(
    service: &AnalyticsService,
    request: MarketRequest,
    first: Value,
) -> Value {
    if first["rows"].is_array() {
        return first;
    }
    let id = first["id"]
        .as_str()
        .expect("market result must be rows or a typed job")
        .to_owned();
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(10));
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            tick.tick().await;
            let job = service.jobs.get_async(id.clone()).await.unwrap();
            match job.state {
                jobs::State::Completed => break,
                jobs::State::Queued | jobs::State::Running => {}
                _ => panic!(
                    "market job ended before artifact: {:?} {:?}",
                    job.state, job.error
                ),
            }
        }
    })
    .await
    .expect("original market job must publish its artifact");
    let mut first_page = request;
    first_page.cursor = Some(format!("{id}:0"));
    let result = service.market(first_page, None, None).await.unwrap();
    assert_eq!(result["job_id"], id, "same completed market identity");
    result
}
#[test]
fn research_fixture_catalog_roots_are_distinct_even_with_parallel_starts() {
    let roots = (0..128)
        .map(|_| unique_fixture_root())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(roots.len(), 128);
}

#[tokio::test]
async fn t08_public_breakout_grid_persists_typed_selection_and_rejects_65_before_sources() {
    let (root, service) = service();
    let bars = (0..200)
        .map(|i| replay::Bar {
            time: i * 60_000,
            open: 100.0 + i as f64 * 0.2,
            high: 101.0 + i as f64 * 0.2,
            low: 99.0 + i as f64 * 0.2,
            close: 100.5 + i as f64 * 0.2,
            volume: 10.0,
            funding_rate: None,
        })
        .collect();
    service
        .sources
        .cache
        .load(
            cache::SourceKey {
                source: "candles".into(),
                instrument: "BTC-USDT".into(),
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

    let variants = (2..66)
        .map(|lookback| okx_research_protocol::analytics::BreakoutParameters { lookback }.into())
        .collect::<Vec<_>>();
    let mut request = ResearchRequest::Grid {
        idempotency_key: "typed-breakout-grid".into(),
        instruments: vec!["BTC-USDT".into()],
        timeframe_ms: 60_000,
        start_ms: 0,
        end_ms: 12_000_000,
        strategy_id: "close_breakout_v1".into(),
        variants,
        costs: ResearchCosts {
            maker_fee_bps: 0.0,
            taker_fee_bps: 0.0,
            slippage_bps: 0.0,
            funding_mode: FundingMode::ZeroAssumption,
        },
        data_mode: ResearchDataMode::Counterfactual,
        seed: 7,
    };
    assert!(request.valid());
    if let ResearchRequest::Grid { variants, .. } = &mut request {
        variants.push(okx_research_protocol::analytics::BreakoutParameters { lookback: 66 }.into());
    }
    assert!(!request.valid());
    assert_eq!(
        service.research(request.clone()).await.unwrap_err(),
        "INVALID_RESEARCH_REQUEST"
    );
    assert!(service.jobs.snapshot().is_empty());
    if let ResearchRequest::Grid { variants, .. } = &mut request {
        variants.pop();
    }
    let admitted = service.research(request.clone()).await.unwrap();
    let id = admitted["id"].as_str().unwrap().to_owned();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let state = service.jobs.get(&id).unwrap();
            if state.state == jobs::State::Completed {
                break;
            }
            assert_ne!(state.state, jobs::State::Failed, "{:?}", state.error);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(service.research(request).await.unwrap()["id"], id);
    let result = service
        .research(ResearchRequest::Result {
            job_id: id.clone(),
            cursor: None,
            limit: 50,
            view: ResearchResultView::Summary,
        })
        .await
        .unwrap();
    assert_eq!(
        result["metadata"]["economic_audit"]["schema"],
        okx_research::economic_audit::REPLAY_ECONOMIC_AUDIT_VERSION
    );
    assert_eq!(
        result["metadata"]["economic_audit"]["net_exact"],
        result["metadata"]["summary"]["pnl_exact"]
    );
    assert_eq!(
        result["metadata"]["economic_audit"]["historical_rules_verified"],
        false
    );
    let export = service
        .research(ResearchRequest::Result {
            job_id: id.clone(),
            cursor: None,
            limit: 50,
            view: ResearchResultView::Export,
        })
        .await
        .unwrap();
    assert_eq!(export["integrity"]["quality"], "complete");
    assert_eq!(
        export["integrity"]["reexecution"]["quality"],
        "verified_reexecution"
    );
    assert_eq!(export["integrity"]["reexecution"]["bars_rehydrated"], 200);
    assert_eq!(
        export["integrity"]["reexecution"]["exact_cashflow_reaudited"],
        true
    );
    let mut forged: Value = service
        .artifacts
        .read_evidence_json(&service.jobs.get(&id).unwrap().artifact.unwrap())
        .unwrap();
    forged["economic_audit"]["net_exact"] = json!("999999");
    assert_eq!(
        verify_export_manifest(&service.artifacts, &forged).unwrap_err(),
        "EXPORT_REEXECUTION_MISMATCH"
    );
    let mut changed_selection: Value = service
        .artifacts
        .read_evidence_json(&service.jobs.get(&id).unwrap().artifact.unwrap())
        .unwrap();
    changed_selection["grid"]["selected"] = json!({"lookback":999});
    assert_eq!(
        verify_export_manifest(&service.artifacts, &changed_selection).unwrap_err(),
        "EXPORT_REEXECUTION_MISMATCH"
    );
    assert!(result["metadata"]["grid"]["selected"]["lookback"].is_u64());
    assert_eq!(result["metadata"]["grid"]["split_index"], 140);
    assert_eq!(result["metadata"]["grid"]["warmup_bars"], 65);
    assert_eq!(
        result["metadata"]["grid"]["strategy_id"],
        "close_breakout_v1"
    );
    let artifact = result["metadata"]["experiment_artifact"].as_str().unwrap();
    let evidence: Value = service.artifacts.read_evidence_json(artifact).unwrap();
    assert_eq!(evidence["variants"].as_array().unwrap().len(), 64);
    assert_eq!(evidence["variants"][0], json!({"lookback":2}));
    assert_eq!(
        evidence["selection_policy"]["tie_break"][1],
        "lookback_ascending"
    );
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn t13_reexecution_budget_rejects_large_grids_before_loading_pages() {
    let (root, service) = service();
    let input = json!({
        "bar_count":1_000_000,
        "variants":[{"fast":3,"slow":9}],
        "code_version":replay::CAUSAL_REPLAY_VERSION
    });
    let result = json!({
        "grid":{"selected":{"fast":3,"slow":9}},
        "economic_audit":{"schema":okx_research::economic_audit::REPLAY_ECONOMIC_AUDIT_VERSION},
        "code_version":replay::CAUSAL_REPLAY_VERSION
    });
    let proof = super::result_reexecution::verify(&service.artifacts, &input, &result).unwrap();
    assert_eq!(proof["quality"], "not_verified");
    assert_eq!(proof["reason_code"], "REEXECUTION_BUDGET_EXCEEDED");
    assert_eq!(proof["max_work_units"], 65_536);
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn t13_replay_reexecution_restarts_preserves_old_reads_and_tracks_changed_seed() {
    let (root, service) = service();
    let bars = (0..240)
        .map(|index| {
            let center = 100.0 + (index as f64 / 11.0).sin() * 8.0 + index as f64 * 0.03;
            replay::Bar {
                time: index * 60_000,
                open: center,
                high: center + 1.0,
                low: center - 1.0,
                close: center + 0.2,
                volume: 10.0,
                funding_rate: None,
            }
        })
        .collect();
    service
        .sources
        .cache
        .load(
            cache::SourceKey {
                source: "candles".into(),
                instrument: "BTC-USDT".into(),
                timeframe_ms: 60_000,
                start_ms: 0,
                end_ms: 14_400_000,
                closed_only: true,
                schema: 1,
            },
            || async { Ok(bars) },
        )
        .await
        .unwrap();
    let template = ResearchRequest::Backtest {
        idempotency_key: "t13-repeat-original".into(),
        instruments: vec!["BTC-USDT".into()],
        timeframe_ms: 60_000,
        start_ms: 0,
        end_ms: 14_400_000,
        strategy_id: "ema_cross_v1".into(),
        parameters: EmaParameters { fast: 3, slow: 9 }.into(),
        costs: ResearchCosts {
            maker_fee_bps: 0.0,
            taker_fee_bps: 1.25,
            slippage_bps: 0.75,
            funding_mode: FundingMode::ZeroAssumption,
        },
        data_mode: ResearchDataMode::Counterfactual,
        seed: 73,
    };
    let mut recorded = Vec::new();
    for (key, seed) in [
        ("t13-repeat-original", 73),
        ("t13-repeat-separate", 73),
        ("t13-changed-seed", 74),
    ] {
        let mut request = template.clone();
        if let ResearchRequest::Backtest {
            idempotency_key,
            seed: replay_seed,
            ..
        } = &mut request
        {
            *idempotency_key = key.into();
            *replay_seed = seed;
        }
        let admitted = service.research(request.clone()).await.unwrap();
        let id = admitted["id"].as_str().unwrap().to_owned();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let job = service.jobs.get(&id).unwrap();
                match job.state {
                    jobs::State::Completed => break,
                    jobs::State::Queued | jobs::State::Running => {}
                    other => panic!("T13 replay {other:?}: {:?}", job.error),
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(service.research(request).await.unwrap()["id"], id);
        let export = service
            .research(ResearchRequest::Result {
                job_id: id.clone(),
                cursor: None,
                limit: 50,
                view: ResearchResultView::Export,
            })
            .await
            .unwrap();
        assert_eq!(
            export["integrity"]["reexecution"]["quality"],
            "verified_reexecution"
        );
        assert_eq!(export["integrity"]["reexecution"]["bars_rehydrated"], 240);
        assert_eq!(
            export["integrity"]["reexecution"]["exact_cashflow_reaudited"],
            true
        );
        recorded.push((id, export));
    }
    assert_ne!(recorded[0].0, recorded[1].0);
    assert_eq!(recorded[0].1["result_hash"], recorded[1].1["result_hash"]);
    assert_eq!(recorded[0].1["result_id"], recorded[1].1["result_id"]);
    assert_eq!(recorded[0].1["dataset_hash"], recorded[2].1["dataset_hash"]);
    assert_ne!(recorded[0].1["result_hash"], recorded[2].1["result_hash"]);
    assert_ne!(recorded[0].1["result_id"], recorded[2].1["result_id"]);
    let bootstrap = service.sources.bootstrap.clone();
    drop(service);
    let reopened = AnalyticsService::new(root.clone(), bootstrap).unwrap();
    for (id, original) in &recorded {
        let repeated = reopened
            .research(ResearchRequest::Result {
                job_id: id.clone(),
                cursor: None,
                limit: 50,
                view: ResearchResultView::Export,
            })
            .await
            .unwrap();
        assert_eq!(
            &repeated, original,
            "immutable export survives owner restart"
        );
        let summary = reopened
            .research(ResearchRequest::Result {
                job_id: id.clone(),
                cursor: None,
                limit: 50,
                view: ResearchResultView::Summary,
            })
            .await
            .unwrap();
        assert_eq!(summary["metadata"]["result_hash"], original["result_hash"]);
        let first_page = reopened
            .research(ResearchRequest::Result {
                job_id: id.clone(),
                cursor: None,
                limit: 1,
                view: ResearchResultView::Trades,
            })
            .await
            .unwrap();
        assert_eq!(
            first_page["metadata"]["result_hash"],
            original["result_hash"]
        );
        if let Some(cursor) = first_page["next_cursor"].as_str() {
            assert_eq!(
                reopened
                    .research(ResearchRequest::Result {
                        job_id: id.clone(),
                        cursor: Some(cursor.into()),
                        limit: 1,
                        view: ResearchResultView::Trades,
                    })
                    .await
                    .unwrap()["returned_rows"],
                1
            );
        }
    }
    drop(reopened);
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn market_artifact_publication_and_cached_result_roundtrip() {
    let (root, s) = service();
    let bars = (0..100)
        .map(|i| replay::Bar {
            time: i * 60000,
            open: 100.0 + i as f64,
            high: 102.0 + i as f64,
            low: 99.0 + i as f64,
            close: 101.0 + i as f64,
            volume: 1.0,
            funding_rate: None,
        })
        .collect();
    s.sources
        .cache
        .load(
            cache::SourceKey {
                source: "candles".into(),
                instrument: "BTC-USDT".into(),
                timeframe_ms: 60000,
                start_ms: 0,
                end_ms: 6000000,
                closed_only: true,
                schema: 1,
            },
            || async { Ok(bars) },
        )
        .await
        .unwrap();
    let request = MarketRequest {
        action: MarketAction::Screen,
        instruments: vec!["BTC-USDT".into()],
        universe: None,
        fields: vec!["returns".into()],
        timeframe_ms: 60000,
        start_ms: 0,
        end_ms: 6000000,
        limit: 50,
        cursor: None,
        parameters: Default::default(),
        filters: vec![],
        sort: None,
        composition: None,
    };
    // Reproduce the valid queued response deterministically: the REAL CPU
    // owner has no worker available until after market's synchronous budget.
    // No fake job, changed deadline or increased production timeout.
    let workers = s.cpu.hold_all_workers_for_test().await;
    let pending = s.market(request.clone(), None, None).await.unwrap();
    assert!(pending["rows"].is_null());
    assert!(pending["id"].is_string());
    drop(workers);
    let result = completed_market(&s, request.clone(), pending).await;
    assert_eq!(result["returned_rows"], 1);
    assert_eq!(result["source_io"]["schema"], "okx.research.source-io/v1");
    assert_eq!(result["source_io"]["candle_cache_hits"], 1);
    assert_eq!(result["source_io"]["source_calls"], json!({}));
    let manifest: Value = s
        .artifacts
        .read_evidence_json(result["result_artifact"].as_str().unwrap())
        .unwrap();
    assert_eq!(manifest["source_io"], result["source_io"]);
    assert_eq!(s.market(request, None, None).await.unwrap(), result);
    let page = s
        .research(ResearchRequest::Result {
            job_id: result["job_id"].as_str().unwrap().into(),
            cursor: None,
            limit: 1,
            view: ResearchResultView::Trades,
        })
        .await
        .unwrap();
    assert_eq!(page["rows"], result["rows"]);
    assert!(
        root.canonicalize()
            .unwrap()
            .starts_with(std::env::temp_dir().canonicalize().unwrap())
    );
    drop(s);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn t13_composed_market_route_persists_input_evidence_and_replays_same_job() {
    let (root, service) = service();
    let bars = (0..100)
        .map(|i| replay::Bar {
            time: i * 60000,
            open: 100.0,
            high: 103.0,
            low: 99.0,
            close: 100.0 + i as f64,
            volume: 1.0,
            funding_rate: None,
        })
        .collect::<Vec<_>>();
    service
        .sources
        .cache
        .load(
            cache::SourceKey {
                source: "candles".into(),
                instrument: "BTC-USDT".into(),
                timeframe_ms: 60000,
                start_ms: 0,
                end_ms: 6000000,
                closed_only: true,
                schema: 1,
            },
            || async { Ok(bars.clone()) },
        )
        .await
        .unwrap();
    let request: MarketRequest = serde_json::from_value(json!({
        "action":"screen", "instruments":["BTC-USDT"], "fields":["returns"],
        "timeframe_ms":60000, "start_ms":0, "end_ms":6000000,
        "composition":{"version":"okx.market.composition/v1", "steps":[
            {"id":"z","op":"rolling_zscore","input":"close","period":20}
        ],"result_steps":["z"]}
    }))
    .unwrap();
    let admitted = service.market(request.clone(), None, None).await.unwrap();
    let result = completed_market(&service, request.clone(), admitted).await;
    assert_eq!(result["rows"].as_array().unwrap().len(), 1);
    assert!(result["rows"][0]["metrics"]["z"].is_number());
    let captured = &result["rows"][0]["source_captures"]["candles"];
    let id = captured["evidence_artifact"].as_str().unwrap();
    let manifest: Value = service.artifacts.read_evidence_json(id).unwrap();
    assert_eq!(manifest["observed_points"], 100);
    assert_eq!(manifest["capture_hash"], captured["capture_hash"]);
    let reconstructed: Vec<replay::Bar> = service
        .artifacts
        .read_evidence_json(manifest["chunks"][0].as_str().unwrap())
        .unwrap();
    assert_eq!(reconstructed.len(), 100);
    assert_eq!(reconstructed.last().unwrap().close, 199.0);
    assert_eq!(
        result["rows"][0]["source_captures"]["open_interest"],
        Value::Null
    );
    assert_eq!(service.market(request, None, None).await.unwrap(), result);
    let mut graph = std::collections::BTreeSet::new();
    let job_artifact = service
        .jobs
        .get(result["job_id"].as_str().unwrap())
        .unwrap()
        .artifact
        .unwrap();
    service.artifact_graph(&job_artifact, &mut graph).unwrap();
    assert!(graph.contains(id));
    assert!(graph.contains(manifest["chunks"][0].as_str().unwrap()));
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn t10_filtered_composition_does_not_publish_unreachable_source_evidence() {
    let (root, service) = service();
    let bars = (0..100)
        .map(|i| replay::Bar {
            time: i * 60000,
            open: 100.0,
            high: 102.0,
            low: 99.0,
            close: 100.0 + i as f64,
            volume: 1.0,
            funding_rate: None,
        })
        .collect::<Vec<_>>();
    service
        .sources
        .cache
        .load(
            cache::SourceKey {
                source: "candles".into(),
                instrument: "BTC-USDT".into(),
                timeframe_ms: 60000,
                start_ms: 0,
                end_ms: 6000000,
                closed_only: true,
                schema: 1,
            },
            || async { Ok(bars) },
        )
        .await
        .unwrap();
    let request: MarketRequest = serde_json::from_value(json!({
        "action":"screen", "instruments":["BTC-USDT"], "fields":["returns"],
        "timeframe_ms":60000, "start_ms":0, "end_ms":6000000,
        "filters":[{"field":"returns","min":5.0}],
        "composition":{"version":"okx.market.composition/v1", "steps":[
            {"id":"z","op":"rolling_zscore","input":"close","period":20}
        ],"result_steps":["z"]}
    }))
    .unwrap();
    let admitted = service.market(request.clone(), None, None).await.unwrap();
    let result = completed_market(&service, request, admitted).await;
    assert_eq!(result["rows"], json!([]));
    let artifacts = std::fs::read_dir(service.artifacts.evidence_dir())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    // One empty market page-manifest and one completed job envelope;
    // rejected inputs do not leave unreferenced evidence blobs.
    assert_eq!(artifacts.len(), 2, "unexpected evidence: {artifacts:?}");
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn ordinary_market_cursors_resume_one_job_and_reject_other_queries() {
    let (root, service) = service();
    let instruments = ["BTC-USDT", "ETH-USDT", "SOL-USDT"];
    for instrument in instruments {
        let bars = (0..100)
            .map(|i| replay::Bar {
                time: i * 60000,
                open: 100.0 + i as f64,
                high: 102.0 + i as f64,
                low: 99.0 + i as f64,
                close: 101.0 + i as f64,
                volume: 1.0,
                funding_rate: None,
            })
            .collect::<Vec<_>>();
        service
            .sources
            .cache
            .load(
                cache::SourceKey {
                    source: "candles".into(),
                    instrument: instrument.into(),
                    timeframe_ms: 60000,
                    start_ms: 0,
                    end_ms: 6000000,
                    closed_only: true,
                    schema: 1,
                },
                || async { Ok(bars) },
            )
            .await
            .unwrap();
    }
    let mut query = MarketRequest {
        action: MarketAction::Screen,
        instruments: instruments.iter().map(|s| (*s).into()).collect(),
        universe: None,
        fields: vec!["returns".into()],
        timeframe_ms: 60000,
        start_ms: 0,
        end_ms: 6000000,
        limit: 1,
        cursor: None,
        parameters: Default::default(),
        filters: vec![],
        sort: None,
        composition: None,
    };
    let response = service.market(query.clone(), None, None).await.unwrap();
    let first = completed_market(&service, query.clone(), response).await;
    assert_eq!(first["rows"][0]["instrument"], instruments[0]);
    let job = first["job_id"].as_str().unwrap().to_owned();
    assert_eq!(first["next_cursor"], format!("{job}:1"));
    query.cursor = first["next_cursor"].as_str().map(str::to_owned);
    let second = service.market(query.clone(), None, None).await.unwrap();
    assert_eq!(second["rows"][0]["instrument"], instruments[1]);
    assert_eq!(second["job_id"], job);
    assert_eq!(second["next_cursor"], format!("{job}:2"));
    query.cursor = second["next_cursor"].as_str().map(str::to_owned);
    let third = service.market(query.clone(), None, None).await.unwrap();
    assert_eq!(third["rows"][0]["instrument"], instruments[2]);
    assert!(third["next_cursor"].is_null());
    assert_eq!(service.jobs.snapshot().len(), 1);
    query.fields = vec!["realized_volatility".into()];
    assert_eq!(
        service.market(query, None, None).await.unwrap_err(),
        "MARKET_CURSOR_REQUEST_MISMATCH"
    );
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn two_asset_batch_grid_has_independent_artifacts_and_cursors() {
    let (root, s) = service();
    for (symbol, direction) in [("BTC-USDT", 1.0), ("ETH-USD", -1.0)] {
        let bars = (0..400)
            .map(|i| {
                let close = 1000.0 + direction * i as f64;
                replay::Bar {
                    time: i * 60000,
                    open: close,
                    high: close + 1.0,
                    low: close - 1.0,
                    close,
                    volume: 10.0,
                    funding_rate: None,
                }
            })
            .collect();
        s.sources
            .cache
            .load(
                cache::SourceKey {
                    source: "candles".into(),
                    instrument: symbol.into(),
                    timeframe_ms: 60000,
                    start_ms: 0,
                    end_ms: 24000000,
                    closed_only: true,
                    schema: 1,
                },
                || async { Ok(bars) },
            )
            .await
            .unwrap();
    }
    let request = ResearchRequest::Grid {
        idempotency_key: "two-assets-grid".into(),
        instruments: vec!["BTC-USDT".into(), "ETH-USD".into()],
        timeframe_ms: 60000,
        start_ms: 0,
        end_ms: 24000000,
        strategy_id: "ema_cross_v1".into(),
        variants: vec![
            EmaParameters { fast: 2, slow: 5 }.into(),
            EmaParameters { fast: 3, slow: 8 }.into(),
        ],
        costs: ResearchCosts {
            maker_fee_bps: 0.0,
            taker_fee_bps: 1.0,
            slippage_bps: 1.0,
            funding_mode: FundingMode::ZeroAssumption,
        },
        data_mode: ResearchDataMode::Counterfactual,
        seed: 7,
    };
    assert!(request.valid());
    let job = s.research(request.clone()).await.unwrap();
    let id = job["id"].as_str().unwrap().to_owned();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let j = s.jobs.get(&id).unwrap();
            if j.state == jobs::State::Completed {
                break;
            }
            assert_ne!(j.state, jobs::State::Failed, "{:?}", j.error);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await
        }
    })
    .await
    .unwrap();
    assert_eq!(s.research(request).await.unwrap()["id"], id);
    let result = s
        .research(ResearchRequest::Result {
            job_id: id.clone(),
            cursor: None,
            limit: 50,
            view: ResearchResultView::Trades,
        })
        .await
        .unwrap();
    assert_eq!(result["total_rows"], 2);
    // The public bounded Trades response may split two independently large
    // grid rows at its byte cap; follow its authenticated cursor to compare
    // both original instrument summaries without weakening that limit.
    let mut batch_rows = result["rows"].as_array().unwrap().clone();
    let mut next = result["next_cursor"].as_str().map(str::to_owned);
    while let Some(cursor) = next {
        let page = s
            .research(ResearchRequest::Result {
                job_id: id.clone(),
                cursor: Some(cursor),
                limit: 50,
                view: ResearchResultView::Trades,
            })
            .await
            .unwrap();
        batch_rows.extend(page["rows"].as_array().unwrap().iter().cloned());
        assert!(batch_rows.len() <= 2);
        next = page["next_cursor"].as_str().map(str::to_owned);
    }
    assert_eq!(batch_rows.len(), 2);
    assert_eq!(result["summary"]["semantics"], "independent_instruments");
    assert_eq!(
        result["summary"]["economic_totals"]["fx_conversion"],
        "not_performed"
    );
    assert!(result["summary"]["economic_totals"]["cross_currency_total"].is_null());
    assert!(result["summary"]["economic_totals"]["portfolio_sharpe"].is_null());
    assert_eq!(
        result["summary"]["economic_totals"]["by_quote_currency"]
            .as_array()
            .unwrap()
            .iter()
            .map(|bucket| bucket["currency"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["USD", "USDT"]
    );
    for bucket in result["summary"]["economic_totals"]["by_quote_currency"]
        .as_array()
        .unwrap()
    {
        assert_eq!(bucket["independent_replay_count"], 1);
        assert!(bucket["net_pnl_exact"].is_string());
    }
    assert_eq!(
        result["summary"]["economic_totals"]["cashflow_quality"],
        "reconciled_from_replay_ledger"
    );
    let cashflows = result["summary"]["economic_totals"]["cashflow_by_settlement_currency"]
        .as_array()
        .unwrap();
    assert_eq!(cashflows.len(), 2);
    for row in cashflows {
        use std::str::FromStr;
        let money = [
            "gross_exact",
            "fees_exact",
            "funding_cost_exact",
            "net_exact",
        ]
        .map(|field| rust_decimal::Decimal::from_str(row[field].as_str().unwrap()).unwrap());
        assert_eq!(money[0] - money[1] - money[2], money[3]);
    }
    let summary = s
        .research(ResearchRequest::Result {
            job_id: id.clone(),
            cursor: None,
            limit: 50,
            view: ResearchResultView::Summary,
        })
        .await
        .unwrap();
    assert_eq!(
        summary["metadata"]["summary"]["economic_totals"],
        result["summary"]["economic_totals"]
    );
    assert_eq!(summary["rows"][0]["quote_currency"], "USDT");
    assert_eq!(summary["rows"][1]["quote_currency"], "USD");
    assert_eq!(
        summary["rows"][0]["economic_audit"]["settlement_currency"],
        "USDT"
    );
    assert_eq!(
        summary["rows"][1]["economic_audit"]["settlement_currency"],
        "USD"
    );
    assert_eq!(summary["rows"][0]["product"], "spot");
    assert_eq!(
        summary["rows"][1]["net_pnl_exact"],
        batch_rows[1]["summary"]["pnl_exact"]
    );
    assert_ne!(batch_rows[0]["dataset_hash"], batch_rows[1]["dataset_hash"]);
    assert_ne!(
        batch_rows[0]["summary"]["pnl"],
        batch_rows[1]["summary"]["pnl"]
    );
    assert!(batch_rows[0]["grid"]["selected"].is_object());
    let trades = s
        .research(ResearchRequest::Result {
            job_id: id.clone(),
            cursor: Some("instrument:0:0".into()),
            limit: 1,
            view: ResearchResultView::Trades,
        })
        .await
        .unwrap();
    assert_eq!(trades["returned_rows"], 1);
    let experiment_id = trades["metadata"]["experiment_artifact"].as_str().unwrap();
    let experiment: Value = s.artifacts.read_evidence_json(experiment_id).unwrap();
    assert_eq!(experiment["schema"], "okx.research.experiment-inputs/v1");
    assert_eq!(experiment["code_version"], replay::CAUSAL_REPLAY_VERSION);
    assert_eq!(experiment["spec"]["seed"], 7);
    assert_eq!(experiment["spec"]["costs"]["taker_fee_bps"], 1.0);
    assert_eq!(experiment["economic_basis"]["product"], "spot");
    assert_eq!(experiment["economic_basis"]["quantity_unit"], "base_asset");
    assert_eq!(
        experiment["economic_basis"]["replay_arithmetic"],
        "f64_approximation"
    );
    assert_eq!(
        experiment["economic_basis"]["historical_rules_verified"],
        false
    );
    assert_eq!(
        experiment["variants"],
        json!([{"fast":2,"slow":5},{"fast":3,"slow":8}])
    );
    assert_eq!(
        experiment["selection_policy"]["selection_metric"],
        "train_sharpe_descending"
    );
    let artifact = s.jobs.get(&id).unwrap().artifact.unwrap();
    let mut graph = std::collections::BTreeSet::new();
    s.artifact_graph(&artifact, &mut graph).unwrap();
    assert!(graph.contains(experiment_id));
    assert!(graph.contains(experiment["dataset_artifact"].as_str().unwrap()));
    assert!(
        s.research(ResearchRequest::Result {
            job_id: id,
            cursor: Some("instrument:8:0".into()),
            limit: 1,
            view: ResearchResultView::Trades,
        })
        .await
        .is_err()
    );
    assert_eq!(s.jobs.snapshot().len(), 1);
    assert!(
        root.canonicalize()
            .unwrap()
            .starts_with(std::env::temp_dir().canonicalize().unwrap())
    );
    drop(s);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn c17_verified_export_reads_all_page_checksums_and_exact_rows() {
    let (root, service) = super::artifact_job_fixtures::service();
    let first = (0..50).map(|i| json!({"row":i})).collect::<Vec<_>>();
    let second = vec![json!({"row":50})];
    let (page_a, path_a) = service.artifacts.publish_evidence(&first).unwrap();
    let page_b = service.artifacts.publish_evidence(&second).unwrap().0;
    let equity = service
        .artifacts
        .publish_evidence(&vec![(1, 100.0), (2, 101.0)])
        .unwrap()
        .0;
    let manifest = service
        .artifacts
        .publish_evidence(&json!({
            "trade_pages":[page_a,page_b],"total_trades":51,
            "equity_curve_pages":[equity],"total_equity_points":2,
            "dataset_hash":"fixture-dataset","code_version":"fixture"
        }))
        .unwrap()
        .0;
    let job = "9".repeat(64);
    service
        .jobs
        .enqueue("c17-export".into(), job.clone())
        .unwrap();
    service
        .jobs
        .update(&job, jobs::State::Completed, Some(manifest), None)
        .unwrap();
    let receipt = service
        .result_page(job.clone(), None, 20, ResearchResultView::Export)
        .unwrap();
    assert_eq!(receipt["integrity"]["quality"], "partial");
    assert_eq!(receipt["integrity"]["dataset_verified"], false);
    assert_eq!(receipt["integrity"]["trades"]["rows"], 51);
    assert_eq!(receipt["integrity"]["trades"]["pages"], 2);
    assert_eq!(receipt["integrity"]["equity"]["rows"], 2);
    assert_eq!(receipt["dataset_hash"], "fixture-dataset");
    let data = service
        .result_page(job.clone(), None, 50, ResearchResultView::Trades)
        .unwrap();
    assert_eq!(data["rows"].as_array().unwrap().len(), 50);
    std::fs::write(path_a, b"tampered-page").unwrap();
    assert_eq!(
        service
            .result_page(job, None, 20, ResearchResultView::Export)
            .unwrap_err(),
        "ARTIFACT_CORRUPT"
    );
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn c17_export_rejects_corrupt_or_mismatched_experiment_provenance() {
    let (root, service) = service();
    let dataset = service
        .artifacts
        .publish_evidence(&json!({
            "schema":"fixture.dataset/v1"
        }))
        .unwrap()
        .0;
    let input = json!({
        "schema":"okx.research.experiment-inputs/v1",
        "dataset_artifact":dataset,
        "code_version":replay::CAUSAL_REPLAY_VERSION
    });
    let experiment = service.artifacts.publish_evidence(&input).unwrap().0;
    let mut manifest = json!({
        "trade_pages":[],"total_trades":0,
        "dataset_artifact":dataset,
        "code_version":replay::CAUSAL_REPLAY_VERSION,
        "experiment_artifact":experiment
    });
    let export = verify_export_manifest(&service.artifacts, &manifest).unwrap();
    assert_eq!(export["experiment"]["checksum_verified"], true);
    assert_eq!(export["experiment"]["references_match"], true);
    assert_eq!(
        export["experiment"]["dataset"]["quality"],
        "unverified_legacy"
    );
    assert_eq!(export["quality"], "partial");
    manifest["dataset_artifact"] = json!("b".repeat(64));
    assert_eq!(
        verify_export_manifest(&service.artifacts, &manifest).unwrap_err(),
        "EXPERIMENT_PROVENANCE_MISMATCH"
    );
    manifest["dataset_artifact"] = json!(dataset);
    service.artifacts.remove_evidence(&experiment).unwrap();
    assert_eq!(
        verify_export_manifest(&service.artifacts, &manifest).unwrap_err(),
        "ARTIFACT_CORRUPT"
    );
    manifest
        .as_object_mut()
        .unwrap()
        .remove("experiment_artifact");
    assert!(verify_export_manifest(&service.artifacts, &manifest).unwrap()["experiment"].is_null());
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn t13_export_rechecks_all_replay_dataset_pages_and_exact_content_hash() {
    let (root, service) = service();
    let timeframe = 60_000u64;
    let bars = (0..4097u64)
        .map(|index| replay::Bar {
            time: index * timeframe,
            open: 100.0 + index as f64 / 10.0,
            high: 101.0 + index as f64 / 10.0,
            low: 99.0 + index as f64 / 10.0,
            close: 100.5 + index as f64 / 10.0,
            volume: 1.0,
            funding_rate: Some(0.0),
        })
        .collect::<Vec<_>>();
    let mut digest = replay::ChunkedDataHash::new(bars.len());
    let mut pages = Vec::new();
    for chunk in bars.chunks(4096) {
        digest.update(chunk).unwrap();
        pages.push(service.artifacts.publish_evidence(&chunk).unwrap().0);
    }
    let dataset_hash = digest.finish().unwrap();
    let end = bars.len() as u64 * timeframe;
    let make_dataset = |pages: Vec<String>| {
        service
            .artifacts
            .publish_evidence(&json!({
                "schema":"okx.research.dataset/398.1",
                "instrument":"BTC-USDT","start_ms":0,"end_ms":end,
                "timeframe_ms":timeframe,"chunks":pages
            }))
            .unwrap()
            .0
    };
    let dataset = make_dataset(pages.clone());
    let make_experiment = |dataset: String| {
        service
            .artifacts
            .publish_evidence(&json!({
                "schema":"okx.research.experiment-inputs/v1",
                "instrument":"BTC-USDT","dataset_artifact":dataset,
                "bar_count":bars.len(),"timeframe_ms":timeframe,
                "requested_range":{"start_ms":0,"end_ms":end},
                "effective_range":{"start_ms":0,"end_ms":end},
                "code_version":replay::CAUSAL_REPLAY_VERSION,
                "economic_basis":{"product":"spot"}
            }))
            .unwrap()
            .0
    };
    let experiment = make_experiment(dataset.clone());
    let mut manifest = json!({
        "trade_pages":[],"total_trades":0,
        "dataset_artifact":dataset,"dataset_hash":dataset_hash,
        "experiment_artifact":experiment,
        "code_version":replay::CAUSAL_REPLAY_VERSION
    });
    let verified = verify_export_manifest(&service.artifacts, &manifest).unwrap();
    assert_eq!(verified["quality"], "complete");
    assert_eq!(verified["dataset_verified"], true);
    assert_eq!(verified["experiment"]["dataset"]["bar_count"], 4097);
    assert_eq!(verified["experiment"]["dataset"]["pages"], 2);
    assert_eq!(verified["experiment"]["dataset"]["last_closed_ms"], end);

    // All page IDs can be valid yet refer to a different dataset: the replay
    // digest must bind the exact values, not just their count and time range.
    let mut changed = bars[4096].clone();
    changed.close += 1.0;
    let changed_page = service
        .artifacts
        .publish_evidence(&vec![changed])
        .unwrap()
        .0;
    let changed_dataset = make_dataset(vec![pages[0].clone(), changed_page]);
    manifest["dataset_artifact"] = json!(changed_dataset.clone());
    manifest["experiment_artifact"] = json!(make_experiment(changed_dataset));
    assert_eq!(
        verify_export_manifest(&service.artifacts, &manifest).unwrap_err(),
        "EXPERIMENT_DATASET_MISMATCH"
    );

    let mut gap = bars[4096].clone();
    gap.time += timeframe;
    let gap_page = service.artifacts.publish_evidence(&vec![gap]).unwrap().0;
    let gap_dataset = make_dataset(vec![pages[0].clone(), gap_page]);
    manifest["dataset_artifact"] = json!(gap_dataset.clone());
    manifest["experiment_artifact"] = json!(make_experiment(gap_dataset));
    assert_eq!(
        verify_export_manifest(&service.artifacts, &manifest).unwrap_err(),
        "DATASET_TIME_GRID_MISMATCH"
    );

    let missing_page_dataset = make_dataset(vec![pages[0].clone()]);
    manifest["dataset_artifact"] = json!(missing_page_dataset.clone());
    manifest["experiment_artifact"] = json!(make_experiment(missing_page_dataset));
    assert_eq!(
        verify_export_manifest(&service.artifacts, &manifest).unwrap_err(),
        "DATASET_PAGE_COUNT_MISMATCH"
    );

    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn t13_export_and_retention_verify_realized_funding_capture_without_lookahead_claims() {
    let (root, service) = service();
    let bar = replay::Bar {
        time: 0,
        open: 100.0,
        high: 101.0,
        low: 99.0,
        close: 100.5,
        volume: 1.0,
        funding_rate: Some(0.01),
    };
    let mut hasher = replay::ChunkedDataHash::new(1);
    hasher.update(std::slice::from_ref(&bar)).unwrap();
    let dataset_hash = hasher.finish().unwrap();
    let chunk = service.artifacts.publish_evidence(&vec![bar]).unwrap().0;
    let dataset = service
        .artifacts
        .publish_evidence(&json!({
            "schema":"okx.research.dataset/398.1",
            "instrument":"BTC-USDT-SWAP","start_ms":0,"end_ms":60_000,
            "timeframe_ms":60_000,"chunks":[chunk]
        }))
        .unwrap()
        .0;
    let events = vec![(60_000u64, 0.01f64)];
    let capture_hash = okx_research::canonical_sha256(&(
        "okx.research.realized-funding-events/v1",
        "BTC-USDT-SWAP",
        0u64,
        60_001u64,
        &events,
    ))
    .unwrap();
    let captured = json!({
        "schema":"okx.research.funding-event-capture/v1",
        "instrument":"BTC-USDT-SWAP","source":"okx/funding-rate-history",
        "start_ms":0,"end_exclusive_ms":60_001,"rate_basis":"realized_settlement_rate",
        "events":events,"normalized_capture_hash":capture_hash
    });
    let funding = service.artifacts.publish_evidence(&captured).unwrap().0;
    let make_experiment = |reference: Option<String>| {
        service
            .artifacts
            .publish_evidence(&json!({
                "schema":"okx.research.experiment-inputs/v1",
                "instrument":"BTC-USDT-SWAP","dataset_artifact":dataset,
                "bar_count":1,"timeframe_ms":60_000,
                "requested_range":{"start_ms":0,"end_ms":60_000},
                "effective_range":{"start_ms":0,"end_ms":60_000},
                "code_version":replay::CAUSAL_REPLAY_VERSION,
                "economic_basis":{"product":"spot",
                    "funding_capture_artifact":reference,
                    "funding_event_alignment":{"observed_events":1,
                        "normalized_source_capture_hash":capture_hash}}
            }))
            .unwrap()
            .0
    };
    let experiment = make_experiment(Some(funding.clone()));
    let mut manifest = json!({
        "trade_pages":[],"total_trades":0,
        "dataset_artifact":dataset,"dataset_hash":dataset_hash,
        "experiment_artifact":experiment,"code_version":replay::CAUSAL_REPLAY_VERSION
    });
    let verified = verify_export_manifest(&service.artifacts, &manifest).unwrap();
    assert_eq!(verified["quality"], "complete");
    assert_eq!(verified["funding_capture_verified"], true);
    assert_eq!(
        verified["experiment"]["funding_replay_attribution_verified"],
        false
    );
    assert_eq!(
        verified["experiment"]["funding_capture"]["observed_events"],
        1
    );
    assert_eq!(
        verified["experiment"]["funding_capture"]["publication_time_verified"],
        false
    );
    let mut graph = std::collections::BTreeSet::new();
    service.artifact_graph(&experiment, &mut graph).unwrap();
    assert!(
        graph.contains(&funding),
        "retention must retain its funding evidence"
    );

    let mut wrong = captured.clone();
    wrong["events"][0][1] = json!(0.02);
    let wrong_id = service.artifacts.publish_evidence(&wrong).unwrap().0;
    manifest["experiment_artifact"] = json!(make_experiment(Some(wrong_id)));
    assert_eq!(
        verify_export_manifest(&service.artifacts, &manifest).unwrap_err(),
        "EXPERIMENT_FUNDING_MISMATCH"
    );
    manifest["experiment_artifact"] = json!(make_experiment(None));
    let legacy = verify_export_manifest(&service.artifacts, &manifest).unwrap();
    assert_eq!(legacy["quality"], "partial");
    assert_eq!(
        legacy["experiment"]["funding_capture"]["reason_code"],
        "FUNDING_CAPTURE_NOT_RETAINED"
    );
    service.artifacts.remove_evidence(&funding).unwrap();
    manifest["experiment_artifact"] = json!(make_experiment(Some(funding)));
    assert_eq!(
        verify_export_manifest(&service.artifacts, &manifest).unwrap_err(),
        "ARTIFACT_CORRUPT"
    );
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn t09_t13_export_proves_funding_presence_and_every_replay_bar_from_source_events() {
    let (root, service) = service();
    let events = vec![(60_000u64, 0.0f64), (180_000, -0.01)];
    let capture_hash = okx_research::canonical_sha256(&(
        "okx.research.realized-funding-events/v1",
        "BTC-USDT-SWAP",
        0u64,
        240_001u64,
        &events,
    ))
    .unwrap();
    let capture = service
        .artifacts
        .publish_evidence(&json!({
            "schema":"okx.research.funding-event-capture/v1",
            "instrument":"BTC-USDT-SWAP","source":"okx/funding-rate-history",
            "start_ms":0,"end_exclusive_ms":240_001,
            "rate_basis":"realized_settlement_rate","events":events,
            "normalized_capture_hash":capture_hash
        }))
        .unwrap()
        .0;
    let alignment = json!({
        "allocation_schema":"okx.research.funding-bar-observation/v1",
        "source":"okx/funding-rate-history",
        "rate_basis":"realized_settlement_rate",
        "valuation_price":"candle_close_proxy_not_venue_mark_price",
        "join_semantics":"funding_event_at_preceding_closed_bar_end_no_forward_fill",
        "absence_semantics":"no_observed_settlement_event_not_observed_zero_rate",
        "arithmetic_projection":"unobserved_settlement_contributes_zero_cashflow_not_zero_rate_evidence",
        "observed_events":2,"attributed_events":2,"pre_window_events_ignored":0,
        "observed_zero_rate_events":1,"bars_without_settlement":2,
        "first_event_ms":60_000,"last_event_ms":180_000,
        "max_observed_event_gap_ms":120_000,
        "max_allowed_gap_ms":okx_research::economics::MAX_FUNDING_EVENT_GAP_MS,
        "expected_events":null,
        "historical_cadence_verified":false,"publication_time_verified":false,
        "mark_price_verified":false,"normalized_source_capture_hash":capture_hash
    });
    let publish_dataset = |rates: [Option<f64>; 4]| {
        let bars = rates
            .into_iter()
            .enumerate()
            .map(|(index, funding_rate)| replay::Bar {
                time: index as u64 * 60_000,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 1.0,
                funding_rate,
            })
            .collect::<Vec<_>>();
        let mut hasher = replay::ChunkedDataHash::new(bars.len());
        hasher.update(&bars).unwrap();
        let hash = hasher.finish().unwrap();
        let chunk = service.artifacts.publish_evidence(&bars).unwrap().0;
        let dataset = service
            .artifacts
            .publish_evidence(&json!({
                "schema":"okx.research.dataset/398.1",
                "instrument":"BTC-USDT-SWAP","start_ms":0,"end_ms":240_000,
                "timeframe_ms":60_000,"chunks":[chunk]
            }))
            .unwrap()
            .0;
        (dataset, hash)
    };
    let publish_experiment = |dataset: &str, capture: Option<&str>, alignment: &Value| {
        service
            .artifacts
            .publish_evidence(&json!({
                "schema":"okx.research.experiment-inputs/v1",
                "instrument":"BTC-USDT-SWAP","dataset_artifact":dataset,
                "bar_count":4,"timeframe_ms":60_000,
                "requested_range":{"start_ms":0,"end_ms":240_000},
                "effective_range":{"start_ms":0,"end_ms":240_000},
                "code_version":replay::CAUSAL_REPLAY_VERSION,
                "economic_basis":{"product":"funding_attribution_test_fixture",
                    "funding_capture_artifact":capture,"funding_event_alignment":alignment}
            }))
            .unwrap()
            .0
    };
    let (dataset, dataset_hash) = publish_dataset([Some(0.0), Some(0.0), Some(-0.01), Some(0.0)]);
    let experiment = publish_experiment(&dataset, Some(&capture), &alignment);
    let mut manifest = json!({
        "trade_pages":[],"total_trades":0,
        "dataset_artifact":dataset,"dataset_hash":dataset_hash,
        "experiment_artifact":experiment,"code_version":replay::CAUSAL_REPLAY_VERSION
    });
    let result = verify_export_manifest(&service.artifacts, &manifest).unwrap();
    assert_eq!(result["quality"], "complete");
    assert_eq!(
        result["experiment"]["funding_replay_attribution_verified"],
        true
    );
    assert_eq!(
        result["experiment"]["dataset"]["funding_bar_projection_verified"],
        true
    );
    assert_eq!(
        result["experiment"]["funding_capture"]["observed_zero_rate_events"],
        1
    );
    assert_eq!(
        result["experiment"]["funding_capture"]["bars_without_settlement"],
        2
    );
    assert_eq!(
        result["experiment"]["funding_capture"]["publication_time_verified"],
        false
    );

    // A malicious or buggy producer can write an internally consistent
    // dataset and result hash with the settlement applied one bar too late.
    // Both hashes pass, but the source-to-bar evidence must still fail.
    for bad_rates in [
        [Some(0.02), Some(0.0), Some(-0.01), Some(0.0)],
        [Some(0.0), Some(-0.01), Some(0.0), Some(0.0)],
        [None, Some(0.0), Some(-0.01), Some(0.0)],
    ] {
        let (bad_dataset, bad_hash) = publish_dataset(bad_rates);
        manifest["dataset_artifact"] = json!(bad_dataset);
        manifest["dataset_hash"] = json!(bad_hash);
        manifest["experiment_artifact"] =
            json!(publish_experiment(&bad_dataset, Some(&capture), &alignment,));
        assert_eq!(
            verify_export_manifest(&service.artifacts, &manifest).unwrap_err(),
            "EXPERIMENT_FUNDING_BAR_MISMATCH"
        );
    }
    manifest["dataset_artifact"] = json!(dataset);
    manifest["dataset_hash"] = json!(dataset_hash);
    let mut forged = alignment.clone();
    forged["observed_zero_rate_events"] = json!(0);
    manifest["experiment_artifact"] = json!(publish_experiment(&dataset, Some(&capture), &forged));
    assert_eq!(
        verify_export_manifest(&service.artifacts, &manifest).unwrap_err(),
        "EXPERIMENT_FUNDING_MISMATCH"
    );
    manifest["experiment_artifact"] = json!(publish_experiment(&dataset, None, &alignment));
    assert_eq!(
        verify_export_manifest(&service.artifacts, &manifest).unwrap_err(),
        "EXPERIMENT_FUNDING_MISMATCH"
    );
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn t13_linear_research_source_bytes_are_attested_exported_and_retained() {
    let (root, service) = service();
    let raw = vec![1u8, 2, 3];
    let reference = service
        .artifacts
        .publish_evidence(&json!({
            "schema":"okx.research.reference-capture/v1",
            "instrument":"BTC-USDT-SWAP",
            "raw_body":raw,
            "reference_generation":"sha256:fixture",
            "acquired_at_ms":"123456789",
            "contract_type":"linear",
            "contract_value":"0.01",
            "contract_value_currency":"BTC",
            "settle_currency":"USDT"
        }))
        .unwrap()
        .0;
    let source_hash =
        okx_research::canonical_sha256(&("okx.public.reference.raw/v1", &[1u8, 2, 3])).unwrap();
    let dataset = service
        .artifacts
        .publish_evidence(&json!({"schema":"fixture.dataset/v1"}))
        .unwrap()
        .0;
    let experiment = service
        .artifacts
        .publish_evidence(&json!({
            "schema":"okx.research.experiment-inputs/v1",
            "instrument":"BTC-USDT-SWAP",
            "dataset_artifact":dataset,
            "code_version":replay::CAUSAL_REPLAY_VERSION,
            "economic_basis":{
                "product":"linear_swap",
                "source_ct_val":"0.01",
                "source_ct_val_ccy":"BTC",
                "source_ct_type":"linear",
                "source_settle_currency":"USDT",
                "reference_generation":"sha256:fixture",
                "reference_acquired_at_ms":"123456789",
                "reference_capture_hash":source_hash,
                "reference_capture_artifact":reference
            }
        }))
        .unwrap()
        .0;
    let manifest = json!({
        "trade_pages":[],"total_trades":0,
        "dataset_artifact":dataset,
        "code_version":replay::CAUSAL_REPLAY_VERSION,
        "experiment_artifact":experiment
    });
    let integrity = verify_export_manifest(&service.artifacts, &manifest).unwrap();
    assert_eq!(integrity["experiment"]["reference_capture_verified"], true);
    let mut graph = std::collections::BTreeSet::new();
    service.artifact_graph(&experiment, &mut graph).unwrap();
    assert!(graph.contains(&reference));

    // Even a valid immutable artifact is not acceptable when its *content*
    // contradicts the reference declared in the experiment.
    let mut mismatched: Value = service.artifacts.read_evidence_json(&experiment).unwrap();
    mismatched["economic_basis"]["source_settle_currency"] = json!("USD");
    let bad_experiment = service.artifacts.publish_evidence(&mismatched).unwrap().0;
    let mut bad_manifest = manifest.clone();
    bad_manifest["experiment_artifact"] = json!(bad_experiment);
    assert_eq!(
        verify_export_manifest(&service.artifacts, &bad_manifest).unwrap_err(),
        "EXPERIMENT_PROVENANCE_MISMATCH"
    );
    service.artifacts.remove_evidence(&reference).unwrap();
    assert_eq!(
        verify_export_manifest(&service.artifacts, &manifest).unwrap_err(),
        "ARTIFACT_CORRUPT"
    );
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn c17_export_does_not_claim_complete_on_count_mismatch() {
    let (root, service) = super::artifact_job_fixtures::service();
    let page = service
        .artifacts
        .publish_evidence(&vec![json!({"row":1})])
        .unwrap()
        .0;
    let manifest = json!({"trade_pages":[page],"total_trades":2});
    assert_eq!(
        verify_export_manifest(&service.artifacts, &manifest).unwrap_err(),
        "EXPORT_ROW_COUNT_MISMATCH"
    );
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn c17_retention_keeps_shared_pinned_result_and_expires_only_unpinned_job() {
    let (root, service) = super::artifact_job_fixtures::service();
    let page = service
        .artifacts
        .publish_evidence(&vec![json!({"row":1})])
        .unwrap()
        .0;
    let experiment = service
        .artifacts
        .publish_evidence(&json!({
            "schema":"okx.research.experiment-inputs/v1",
            "selection_policy":{"selection_metric":"train_sharpe_descending"}
        }))
        .unwrap()
        .0;
    let manifest = service
        .artifacts
        .publish_evidence(&json!({
            "trade_pages":[page.clone()],"total_trades":1,"experiment_artifact":experiment
        }))
        .unwrap()
        .0;
    let first = "8".repeat(64);
    let second = "7".repeat(64);
    for (key, id) in [("shared-first", &first), ("shared-second", &second)] {
        service.jobs.enqueue(key.into(), id.clone()).unwrap();
        service
            .jobs
            .update(id, jobs::State::Completed, Some(manifest.clone()), None)
            .unwrap();
    }
    service.pin_job(&second, true).unwrap();
    let future = jiff::Timestamp::now().as_millisecond().max(0) as u64 + 8 * 86_400_000;
    assert_eq!(service.apply_retention(future).unwrap(), 1);
    assert_eq!(
        service
            .result_page(first, None, 1, ResearchResultView::Trades)
            .unwrap_err(),
        "RESULT_EXPIRED"
    );
    let still_live = service
        .result_page(second.clone(), None, 1, ResearchResultView::Trades)
        .unwrap();
    assert_eq!(still_live["rows"][0]["row"], 1);
    assert!(service.artifacts.read_evidence(&page).is_ok());
    assert!(service.artifacts.read_evidence(&experiment).is_ok());
    service.pin_job(&second, false).unwrap();
    assert_eq!(service.apply_retention(future).unwrap(), 1);
    assert!(service.artifacts.read_evidence(&page).is_err());
    assert!(service.artifacts.read_evidence(&manifest).is_err());
    assert!(service.artifacts.read_evidence(&experiment).is_err());
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn c17_cursor_survives_catalog_restart_and_is_not_transferable() {
    let (root, service) = super::artifact_job_fixtures::service();
    let page = service
        .artifacts
        .publish_evidence(&vec![json!({"row":1}), json!({"row":2})])
        .unwrap()
        .0;
    let manifest = service
        .artifacts
        .publish_evidence(&json!({
            "trade_pages":[page],"total_trades":2
        }))
        .unwrap()
        .0;
    let id = "e".repeat(64);
    service
        .jobs
        .enqueue("c17-signed".into(), id.clone())
        .unwrap();
    service
        .jobs
        .update(&id, jobs::State::Completed, Some(manifest.clone()), None)
        .unwrap();
    let first = service
        .result_page(id.clone(), None, 1, ResearchResultView::Trades)
        .unwrap();
    let token = first["next_cursor"].as_str().unwrap().to_owned();
    assert!(token.starts_with("r1."));
    let decoded =
        cursor::Cursor::decode(&token, &service.jobs.cursor_signing_key().unwrap()).unwrap();
    assert_eq!(decoded.offset, 1);
    assert_eq!(
        service
            .result_page(
                id.clone(),
                Some(token.clone()),
                1,
                ResearchResultView::Equity
            )
            .unwrap_err(),
        "CURSOR_MISMATCH"
    );
    let other = "f".repeat(64);
    service
        .jobs
        .enqueue("c17-other".into(), other.clone())
        .unwrap();
    service
        .jobs
        .update(&other, jobs::State::Completed, Some(manifest), None)
        .unwrap();
    assert_eq!(
        service
            .result_page(other, Some(token.clone()), 1, ResearchResultView::Trades)
            .unwrap_err(),
        "CURSOR_MISMATCH"
    );
    let mut forged = token.clone().into_bytes();
    let last = forged.len() - 1;
    forged[last] = if forged[last] == b'A' { b'B' } else { b'A' };
    assert_eq!(
        service
            .result_page(
                id.clone(),
                Some(String::from_utf8(forged).unwrap()),
                1,
                ResearchResultView::Trades
            )
            .unwrap_err(),
        "INVALID_CURSOR"
    );
    let signing_key = service.jobs.cursor_signing_key().unwrap();
    let bootstrap = service.sources.bootstrap.clone();
    assert!(
        matches!(jobs::Jobs::open(root.join("research-catalog.sqlite")), Err(e) if e == "RESEARCH_ROOT_LOCKED")
    );
    drop(service);
    let restarted = AnalyticsService::new(root.clone(), bootstrap).unwrap();
    assert_eq!(restarted.jobs.cursor_signing_key().unwrap(), signing_key);
    let second = restarted
        .result_page(id, Some(token), 1, ResearchResultView::Trades)
        .unwrap();
    assert_eq!(second["rows"][0]["row"], 2);
    drop(restarted);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn c17_empty_or_short_manifest_page_fails_without_unbounded_scan() {
    let (root, service) = super::artifact_job_fixtures::service();
    let page_id = service
        .artifacts
        .publish_evidence(&Vec::<Value>::new())
        .unwrap()
        .0;
    let manifest = service
        .artifacts
        .publish_evidence(&json!({
            "trade_pages":[page_id],"total_trades":2
        }))
        .unwrap()
        .0;
    let id = "d".repeat(64);
    service
        .jobs
        .enqueue("c17-empty".into(), id.clone())
        .unwrap();
    service
        .jobs
        .update(&id, jobs::State::Completed, Some(manifest.clone()), None)
        .unwrap();
    let err = service
        .result_page(id.clone(), None, 5, ResearchResultView::Trades)
        .unwrap_err();
    assert_eq!(err, "RESULT_PAGE_TRUNCATED");
    let token = cursor::Cursor::new(
        &id,
        &manifest,
        &manifest,
        ResearchResultView::Trades,
        2,
        None,
    )
    .encode(&service.jobs.cursor_signing_key().unwrap())
    .unwrap();
    let err = service
        .result_page(id, Some(token), 5, ResearchResultView::Trades)
        .unwrap_err();
    assert_eq!(err, "RESULT_CURSOR_OUT_OF_RANGE");
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn immutable_result_pages_and_restart_catalog() {
    let (root, s) = service();
    let data = s
        .artifacts
        .publish_evidence(&json!({"chunks":[]}))
        .unwrap()
        .0;
    let result = replay::ReplayResult {
        dataset_hash: "fixture-hash".into(),
        code_version: "fixture-code".into(),
        hash: "fixture-result".into(),
        summary: replay::Summary {
            pnl: 0.0,
            pnl_exact: "0".into(),
            fees_exact: "1".into(),
            funding_exact: "0".into(),
            max_drawdown: 0.0,
            sharpe: 0.0,
            trades: 120,
            turnover: 120.0,
            fees: 1.0,
            funding: 0.0,
            benchmark: 0.0,
            assumptions: vec!["COUNTERFACTUAL fixture".into()],
        },
        equity: (0..9000).map(|i| (i, 10000.0)).collect(),
        trades: (0..120)
            .map(|i| replay::Trade {
                time: i,
                side: 1,
                price: 1.0,
                quantity: 1.0,
                fee: 0.01,
                reason: "fixture".into(),
            })
            .collect(),
    };
    let artifact = publish_replay(
        &s.artifacts,
        &result,
        &data,
        ReplayPublication {
            grid: None,
            experiment: None,
            economic_audit: None,
            producer: None,
        },
        &|| false,
    )
    .unwrap();
    let id = "a".repeat(64);
    s.jobs
        .enqueue("fixture-request".into(), id.clone())
        .unwrap();
    s.jobs
        .update(&id, jobs::State::Completed, Some(artifact.clone()), None)
        .unwrap();
    let page = s
        .research(ResearchRequest::Result {
            job_id: id.clone(),
            cursor: Some(
                cursor::Cursor::new(
                    &id,
                    &artifact,
                    &artifact,
                    ResearchResultView::Trades,
                    47,
                    None,
                )
                .encode(&s.jobs.cursor_signing_key().unwrap())
                .unwrap(),
            ),
            limit: 5,
            view: ResearchResultView::Trades,
        })
        .await
        .unwrap();
    assert_eq!(page["returned_rows"], 5);
    assert_eq!(page["total_rows"], 120);
    assert_eq!(page["rows"][0]["time"], 47);
    assert!(page["next_cursor"].as_str().unwrap().starts_with("r1."));
    let bootstrap = s.sources.bootstrap.clone();
    assert!(
        matches!(jobs::Jobs::open(root.join("research-catalog.sqlite")), Err(e) if e == "RESEARCH_ROOT_LOCKED")
    );
    drop(s);
    let scope =
        okx_research_protocol::research_ipc::ResearchScope::new("fixture-scope".into()).unwrap();
    drop(AnalyticsService::new_scoped(root.clone(), bootstrap.clone(), &scope).unwrap());
    let producer = crate::producer::Producer::new("a".repeat(64), None, None).unwrap();
    let s = AnalyticsService::new_existing_scoped_with_producer(
        root.clone(),
        bootstrap,
        &scope,
        producer,
    )
    .unwrap();
    let adopted_page = crate::edge_runtime::dispatch_research_for_test(
        &s,
        ResearchRequest::Result {
            job_id: id.clone(),
            cursor: None,
            limit: 1,
            view: ResearchResultView::Trades,
        },
    )
    .await
    .unwrap();
    assert_eq!(adopted_page["total_rows"], 120);
    assert_eq!(s.jobs.get(&id).unwrap().state, jobs::State::Completed);
    let legacy = s
        .result_page(id.clone(), None, 1, ResearchResultView::Export)
        .unwrap();
    assert_eq!(legacy["integrity"]["quality"], "partial");
    assert_eq!(
        legacy["integrity"]["reexecution"]["quality"],
        "not_verified"
    );
    assert_eq!(
        legacy["integrity"]["reexecution"]["reason_code"],
        "LEGACY_EXPERIMENT_INPUTS_NOT_RETAINED"
    );
    assert!(
        s.jobs
            .enqueue("fixture-request".into(), "b".repeat(64))
            .unwrap_err()
            .contains("IDEMPOTENCY_CONFLICT")
    );
    s.pin_job(&id, true).unwrap();
    assert_eq!(
        s.apply_retention(jiff::Timestamp::now().as_millisecond() as u64 + 8 * 86400000)
            .unwrap(),
        0
    );
    s.pin_job(&id, false).unwrap();
    assert_eq!(
        s.apply_retention(jiff::Timestamp::now().as_millisecond() as u64 + 8 * 86400000)
            .unwrap(),
        1
    );
    assert!(s.jobs.get(&id).unwrap().artifact.is_none());
    assert!(
        root.canonicalize()
            .unwrap()
            .starts_with(std::env::temp_dir().canonicalize().unwrap())
    );
    drop(s);
    std::fs::remove_dir_all(root).unwrap();
}

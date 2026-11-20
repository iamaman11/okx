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
    let root = unique_fixture_root();
    let client =
        okx_api::OkxPublicClient::new(okx_api::OkxEnvironment::new(okx_api::Region::Global, false))
            .unwrap();
    let service = AnalyticsService::new(
        root.clone(),
        okx_runtime::market_bootstrap::MarketBootstrapper::new(client),
    )
    .unwrap();
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
    let result = service.market(request, None, None).await.unwrap();
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
        .map(|lookback| okx_protocol::analytics::BreakoutParameters { lookback }.into())
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
        variants.push(okx_protocol::analytics::BreakoutParameters { lookback: 66 }.into());
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
            job_id: id,
            cursor: None,
            limit: 50,
            view: ResearchResultView::Summary,
        })
        .await
        .unwrap();
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
    for (symbol, direction) in [("BTC-USDT", 1.0), ("ETH-USDT", -1.0)] {
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
        instruments: vec!["BTC-USDT".into(), "ETH-USDT".into()],
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
    assert_eq!(result["summary"]["semantics"], "independent_instruments");
    assert_ne!(
        result["rows"][0]["dataset_hash"],
        result["rows"][1]["dataset_hash"]
    );
    assert_ne!(
        result["rows"][0]["summary"]["pnl"],
        result["rows"][1]["summary"]["pnl"]
    );
    assert!(result["rows"][0]["grid"]["selected"].is_object());
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
    assert_eq!(receipt["integrity"]["quality"], "complete");
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
    let input = json!({
        "schema":"okx.research.experiment-inputs/v1",
        "dataset_artifact":"a".repeat(64),
        "code_version":replay::CAUSAL_REPLAY_VERSION
    });
    let experiment = service.artifacts.publish_evidence(&input).unwrap().0;
    let mut manifest = json!({
        "trade_pages":[],"total_trades":0,
        "dataset_artifact":"a".repeat(64),
        "code_version":replay::CAUSAL_REPLAY_VERSION,
        "experiment_artifact":experiment
    });
    let export = verify_export_manifest(&service.artifacts, &manifest).unwrap();
    assert_eq!(export["experiment"]["checksum_verified"], true);
    assert_eq!(export["experiment"]["references_match"], true);
    manifest["dataset_artifact"] = json!("b".repeat(64));
    assert_eq!(
        verify_export_manifest(&service.artifacts, &manifest).unwrap_err(),
        "EXPERIMENT_PROVENANCE_MISMATCH"
    );
    manifest["dataset_artifact"] = json!("a".repeat(64));
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
    let artifact =
        publish_replay(&s.artifacts, &result, &data, None, None, None, &|| false).unwrap();
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
    let s = AnalyticsService::new(root.clone(), bootstrap).unwrap();
    assert_eq!(s.jobs.get(&id).unwrap().state, jobs::State::Completed);
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

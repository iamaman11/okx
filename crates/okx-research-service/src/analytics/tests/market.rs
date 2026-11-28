use super::*;

#[tokio::test(flavor = "current_thread")]
async fn t14_market_retry_and_cursor_never_publish_cancelled_or_interrupted_partial_result() {
    let (root, service) = super::super::artifact_job_fixtures::service();
    let make_id = |request: &MarketRequest| {
        okx_research::canonical_sha256(&(
            market_request_schema(&request.fields, false, false, false),
            request,
            &Option::<Value>::None,
            0_i64,
        ))
        .unwrap()
        .trim_start_matches("sha256:")
        .to_owned()
    };
    let cancelled_request = request(vec!["returns"]);
    assert!(cancelled_request.valid());
    let cancelled_id = make_id(&cancelled_request);
    let fingerprint = okx_research::canonical_sha256(&(
        "market/448.cursor-v1",
        &cancelled_request,
        &Option::<Value>::None,
    ))
    .unwrap();
    // This is an authentic immutable page hierarchy, but the catalog
    // marks it as partial. Old Market retry/cursor paths checked only
    // artifact.is_some() and would serve it as a completed query.
    let row_page = service
        .artifacts
        .publish_evidence(&vec![
            json!({"instrument":"UNCOMMITTED"}),
            json!({"instrument":"UNCOMMITTED_2"}),
        ])
        .unwrap()
        .0;
    let manifest = service
        .artifacts
        .publish_evidence(&json!({"row_pages":[row_page],"total_rows":2}))
        .unwrap()
        .0;
    let partial = service
        .artifacts
        .publish_evidence(&json!({
            "job_id":cancelled_id,"market_request_fingerprint":fingerprint,
            "rows":[{"instrument":"UNCOMMITTED"}],"total_rows":2,
            "result_artifact":manifest,"page_limit":1,
            "next_cursor":format!("{}:1",cancelled_id)
        }))
        .unwrap()
        .0;
    let (created, fresh, _) = service
        .jobs
        .enqueue(format!("market-{cancelled_id}"), cancelled_id.clone())
        .unwrap();
    assert!(fresh);
    assert!(
        service
            .jobs
            .update(
                &created.id,
                jobs::State::Running,
                Some(partial.clone()),
                None
            )
            .unwrap()
    );
    service
        .jobs
        .cancel(&cancelled_id, "t14-cancel-key")
        .unwrap();
    assert_eq!(
        service.jobs.get(&cancelled_id).unwrap().state,
        jobs::State::Cancelled
    );
    let retry = service
        .market(cancelled_request.clone(), None, None)
        .await
        .unwrap();
    assert_eq!(retry["state"], "cancelled");
    assert!(
        retry["rows"].is_null(),
        "cancelled job cannot claim published market rows"
    );
    for offset in [0, 1] {
        let mut cancelled_cursor = cancelled_request.clone();
        cancelled_cursor.cursor = Some(format!("{cancelled_id}:{offset}"));
        assert_eq!(
            service
                .market(cancelled_cursor, None, None)
                .await
                .unwrap_err(),
            "MARKET_CURSOR_NOT_READY"
        );
    }
    assert_eq!(
        service
            .result_page(cancelled_id.clone(), None, 1, ResearchResultView::Trades)
            .unwrap_err(),
        "RESULT_NOT_READY"
    );
    assert!(
        !service
            .jobs
            .update(
                &cancelled_id,
                jobs::State::Completed,
                Some(partial.clone()),
                None
            )
            .unwrap()
    );
    assert_eq!(
        service.jobs.get(&cancelled_id).unwrap().state,
        jobs::State::Cancelled
    );

    // The second job is interrupted by reopening the same durable owner.
    let interrupted_request = request(vec!["rolling_mean"]);
    let interrupted_id = make_id(&interrupted_request);
    let (created, fresh, _) = service
        .jobs
        .enqueue(format!("market-{interrupted_id}"), interrupted_id.clone())
        .unwrap();
    assert!(fresh);
    assert!(
        service
            .jobs
            .update(&created.id, jobs::State::Running, Some(partial), None)
            .unwrap()
    );
    let bootstrap = service.sources.bootstrap.clone();
    drop(service);
    let reopened = AnalyticsService::new(root.clone(), bootstrap).unwrap();
    assert_eq!(
        reopened.jobs.get(&interrupted_id).unwrap().state,
        jobs::State::Interrupted
    );
    let retry = reopened
        .market(interrupted_request.clone(), None, None)
        .await
        .unwrap();
    assert_eq!(retry["state"], "interrupted");
    assert!(
        retry["rows"].is_null(),
        "interrupted job cannot claim completed rows"
    );
    let mut interrupted_cursor = interrupted_request;
    interrupted_cursor.cursor = Some(format!("{interrupted_id}:1"));
    assert_eq!(
        reopened
            .market(interrupted_cursor, None, None)
            .await
            .unwrap_err(),
        "MARKET_CURSOR_NOT_READY"
    );
    assert_eq!(
        reopened.diagnostic_job(&interrupted_id)["job"]["result_ref"],
        Value::Null
    );
    assert_eq!(
        reopened.diagnostic_job(&interrupted_id)["job"]["error_code"],
        "RESEARCH_INTERRUPTED"
    );
    drop(reopened);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn t02_funding_composition_rejects_non_swap_before_source_reads() {
    let spot = ["BTC-USDT-SWAP".to_owned(), "ETH-USDT".to_owned()];
    let range = okx_analysis::time_series::ClosedRange::strict(0, 180_000, 60_000).unwrap();
    use okx_analysis::query_plan::SeriesSource;
    assert_eq!(
        SeriesSource::FundingRate
            .bind(&spot[1], range, 1)
            .unwrap_err(),
        "composition.sources.funding_rate:UNSUPPORTED_PRODUCT"
    );
    assert_eq!(
        SeriesSource::OpenInterest
            .bind(&spot[1], range, 1)
            .unwrap_err(),
        "composition.sources.open_interest:UNSUPPORTED_PRODUCT"
    );
    assert!(SeriesSource::FundingRate.bind(&spot[0], range, 0).is_ok());
    assert!(SeriesSource::OpenInterest.bind(&spot[0], range, 0).is_ok());
    assert!(SeriesSource::Close.bind(&spot[1], range, 1).is_ok());
}

#[test]
fn t13_composition_evidence_uses_a_new_namespace_without_invalidating_legacy_results() {
    let fields = vec!["returns".to_owned()];
    let oi_fields = vec!["pearson_correlation".to_owned()];
    assert_eq!(
        market_request_schema(&fields, false, false, false),
        "market/518.11.source-io-flat-v1"
    );
    let composed = market_request_schema(&fields, true, false, false);
    let oi_composed = market_request_schema(&fields, true, true, false);
    let oi_correlated = market_request_schema(&oi_fields, true, true, false);
    let funding_composed = market_request_schema(&fields, true, false, true);
    assert_eq!(composed, "market/518.11.source-io-composition-v1");
    assert_eq!(oi_composed, composed);
    assert_ne!(composed, "market/518.2.composition-quality-v1");
    assert_ne!(oi_composed, "market/518.2.oi-composition-quality-v1");
    assert_ne!(
        composed,
        market_request_schema(&fields, false, false, false)
    );
    assert_eq!(oi_correlated, oi_composed);
    assert_eq!(funding_composed, "market/518.11.source-io-composition-v1");
    assert_eq!(
        funding_composed,
        market_request_schema(&fields, true, true, true)
    );
    assert_eq!(
        market_request_schema(&oi_fields, false, false, false),
        "market/518.11.source-io-correlation-v1"
    );
}

#[test]
fn t06_funding_result_schema_invalidates_only_funding_jobs() {
    let funding = request(vec!["funding_sum"]);
    let mixed = request(vec!["returns", "funding_sum", "spread_bps"]);
    let returns = request(vec!["returns"]);
    let correlation = request(vec!["pearson_correlation"]);
    let combined = request(vec!["funding_sum", "pearson_correlation"]);
    assert_eq!(
        market_job_schema(&funding.fields),
        market_job_schema(&mixed.fields)
    );
    assert_eq!(
        market_job_schema(&returns.fields),
        "market/518.11.source-io-flat-v1"
    );
    assert_eq!(
        market_job_schema(&correlation.fields),
        "market/518.11.source-io-correlation-v1"
    );
    assert_ne!(
        market_job_schema(&combined.fields),
        market_job_schema(&funding.fields)
    );
    assert_ne!(
        market_job_schema(&combined.fields),
        market_job_schema(&correlation.fields)
    );

    // Reproduce the exact persisted pre-T06 key for a closed historical
    // query. The new key must not replay that completed stale artifact.
    let old =
        okx_research::canonical_sha256(&("market/448.2", &funding, &Option::<Value>::None, 0_i64))
            .unwrap();
    let new = okx_research::canonical_sha256(&(
        market_job_schema(&funding.fields),
        &funding,
        &Option::<Value>::None,
        0_i64,
    ))
    .unwrap();
    assert_ne!(old, new);

    // #525 used this domain. Its old completed funding artifacts may
    // include a forecast fallback and cannot be reused after this change.
    let previous = okx_research::canonical_sha256(&(
        "market/448.3.funding-publication-v1",
        &funding,
        &Option::<Value>::None,
        0_i64,
    ))
    .unwrap();
    assert_ne!(previous, new);

    // Reading floats with exact round-trip precision can change any
    // calculation, not just funding. Preserve completed old job IDs for
    // result/status/export reads; new requests use the new namespace.
    let legacy_returns =
        okx_research::canonical_sha256(&("market/448.2", &returns, &Option::<Value>::None, 0_i64))
            .unwrap();
    let current_returns = okx_research::canonical_sha256(&(
        market_job_schema(&returns.fields),
        &returns,
        &Option::<Value>::None,
        0_i64,
    ))
    .unwrap();
    assert_ne!(legacy_returns, current_returns);
}

#[test]
fn transient_market_source_defer_is_not_cacheable() {
    let mut rate_limited = row();
    rate_limited.bars =
        Err("public OKX market API error: OKX rate/backpressure defer: LocalBudget".into());
    assert!(market_source_rate_deferred(&[rate_limited]));
    assert!(!market_source_rate_deferred(&[row()]));
    let mut invalid = row();
    invalid.bars = Err("CANDLE_INTERIOR_GAP".into());
    assert!(!market_source_rate_deferred(&[invalid]));
}
#[test]
fn correlation_3x3_log_returns() {
    let mut series = Vec::new();
    for (k, id) in ["BTC-USDT", "ETH-USDT", "SOL-USDT"].iter().enumerate() {
        let mut price = 100.0;
        let bars = (0..100)
            .map(|i| {
                if i > 0 {
                    price *= (0.002
                        + ((i % 7) as f64 - 3.0) * 0.0007 * if k == 2 { -1.0 } else { 1.0 })
                    .exp();
                }
                Bar {
                    time: i * 60000,
                    open: price,
                    high: price * 1.01,
                    low: price * 0.99,
                    close: price,
                    volume: 1.0,
                    funding_rate: None,
                }
            })
            .collect();
        series.push(Loaded {
            instrument: id.to_string(),
            bars: Ok(Arc::new(bars)),
            ticker: None,
            funding: None,
            oi: None,
            excluded: None,
        });
    }
    let m = pairwise_matrix(&series, 60000, 6_000_000);
    assert_eq!(m["basis"], "aligned_closed_bar_log_returns");
    assert_eq!(m["instruments"].as_array().unwrap().len(), 3);
    assert_eq!(m["sample_counts"][1][2], 99);
    assert_eq!(m["values"][0][1], m["values"][1][0]);
    assert_eq!(m["values"][1][2], m["values"][2][1]);
    assert!(m["values"][0][0].as_f64().unwrap() > 0.99);
    assert!(m["values"][0][2].as_f64().unwrap() < 0.0);
}
#[test]
fn t06_pearson_needs_thirty_known_aligned_pairs_not_future_or_shifted_data() {
    let mut rows = Vec::new();
    for (instrument, factor) in [("BTC-USDT", 1.0), ("ETH-USDT", 2.0)] {
        let mut price = 100.0;
        let bars = (0..31)
            .map(|i| {
                if i > 0 {
                    price *= (((i % 7) as f64 - 3.0) * 0.002 * factor).exp();
                }
                Bar {
                    time: i * 60_000,
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                    volume: 1.0,
                    funding_rate: None,
                }
            })
            .collect();
        rows.push(Loaded {
            instrument: instrument.into(),
            bars: Ok(Arc::new(bars)),
            ticker: None,
            funding: None,
            oi: None,
            excluded: None,
        });
    }
    let full = pairwise_matrix(&rows, 60_000, 31 * 60_000);
    assert_eq!(full["join_semantics"], "exact_event_time_no_forward_fill");
    assert_eq!(full["sample_counts"][0][1], 30);
    assert_eq!(full["usable_samples"], json!([30, 30]));
    assert_eq!(full["source_coverage"][0]["return_observations"], 30);
    assert_eq!(full["source_coverage"][1]["usable_at_cutoff"], 30);
    assert_eq!(full["pair_diagnostics"][0]["reason"], "AVAILABLE");
    assert_eq!(full["pair_diagnostics"][0]["matched"], 30);
    assert_eq!(full["pair_diagnostics"].as_array().unwrap().len(), 1);
    assert_eq!(full["source_coverage"][0]["not_available_at_cutoff"], 0);
    assert!((full["values"][0][1].as_f64().unwrap() - 1.0).abs() < 1e-9);

    // Last bar was observed in the recorded fixture but cannot be used
    // before its close. No post-decision look-ahead is permitted.
    let early = pairwise_matrix(&rows, 60_000, 30 * 60_000);
    assert_eq!(early["sample_counts"][0][1], 29);
    assert_eq!(early["usable_samples"], json!([29, 29]));
    assert_eq!(early["source_coverage"][0]["not_available_at_cutoff"], 1);
    assert_eq!(early["source_coverage"][1]["usable_at_cutoff"], 29);
    assert_eq!(
        early["pair_diagnostics"][0]["reason"],
        "INSUFFICIENT_COMMON_PAIRS"
    );
    assert_eq!(
        early["pair_diagnostics"][0]["first_unavailable_ms"],
        1_800_000
    );
    assert!(early["values"][0][1].is_null());

    // Equal array lengths cannot substitute for equal timestamps.
    let mut shifted = rows;
    for bar in Arc::make_mut(shifted[1].bars.as_mut().unwrap()).iter_mut() {
        bar.time += 60_000;
    }
    let unaligned = pairwise_matrix(&shifted, 60_000, 32 * 60_000);
    assert_eq!(unaligned["sample_counts"][0][1], 29);
    assert_eq!(unaligned["usable_samples"], json!([30, 30]));
    assert_eq!(unaligned["pair_diagnostics"][0]["left_unmatched"], 1);
    assert_eq!(unaligned["pair_diagnostics"][0]["right_unmatched"], 1);
    assert_eq!(
        unaligned["pair_diagnostics"][0]["first_unmatched_ms"],
        60_000
    );
    assert!(unaligned["values"][0][1].is_null());
    shifted[1].bars = Err("SOURCE_UNAVAILABLE".into());
    let absent = pairwise_matrix(&shifted, 60_000, 32 * 60_000);
    assert_eq!(
        absent["source_coverage"][1]["source_state"],
        "source_unavailable"
    );
    assert!(absent["usable_samples"][1].is_null());
    assert_eq!(absent["sample_counts"][0][1], 0);
    assert_eq!(
        absent["pair_diagnostics"][0]["reason"],
        "SOURCE_SERIES_UNAVAILABLE_OR_GAPPED"
    );
}
#[test]
fn t06_oi_interior_gap_is_exposed_and_never_accepted_as_endpoint_only() {
    let mut query = request(vec!["oi_change"]);
    query.timeframe_ms = 300_000;
    query.end_ms = 1_500_000;
    let mut source = row();
    source.bars = Ok(Arc::new(Vec::new()));
    source.oi = Some(Ok(vec![
        (0, 10.0),
        (300_000, 12.0),
        (900_000, 16.0),
        (1_200_000, 18.0),
    ]));
    let partial = compute_row(source, &query, None).unwrap();
    assert_eq!(partial["quality"], "partial");
    assert_eq!(partial["metrics"]["oi_change"], Value::Null);
    assert!(partial["candle_coverage"].is_null());
    assert_eq!(partial["oi_coverage"]["expected_bars"], 5);
    assert_eq!(partial["oi_coverage"]["observed_unique_bars"], 4);
    assert_eq!(partial["oi_coverage"]["first_missing_ms"], 600_000);
    assert!(
        partial["gaps"]
            .as_array()
            .unwrap()
            .contains(&json!("OI_RANGE_INCOMPLETE_OR_GAPPED"))
    );

    let mut full_source = row();
    full_source.bars = Ok(Arc::new(Vec::new()));
    full_source.oi = Some(Ok(vec![
        (0, 10.0),
        (300_000, 12.0),
        (600_000, 14.0),
        (900_000, 16.0),
        (1_200_000, 18.0),
    ]));
    let full = compute_row(full_source, &query, None).unwrap();
    assert_eq!(full["quality"], "complete");
    assert!(full["candle_coverage"].is_null());
    assert_eq!(full["oi_coverage"]["complete"], true);
    assert!((full["metrics"]["oi_change"].as_f64().unwrap() - 0.8).abs() < 1e-12);
}
#[test]
fn t06_current_ticker_is_not_marked_as_historical_spread() {
    let mut source = row();
    source.ticker = Some(Ok(okx_research_protocol::research_ipc::ResearchTicker {
        bid_price: "100".into(),
        ask_price: "101".into(),
    }));
    let response = compute_row(source, &request(vec!["spread_bps"]), None).unwrap();
    assert!(response["metrics"]["spread_bps"].is_number());
    assert_eq!(
        response["metric_time_basis"]["spread_bps"],
        "current_snapshot_unaligned"
    );
    assert!(response["candle_coverage"].is_null());
    assert_eq!(response["quality"], "partial");
    assert!(
        response["gaps"]
            .as_array()
            .unwrap()
            .contains(&json!("SPREAD_CURRENT_SNAPSHOT_NOT_HISTORICAL"))
    );
}
#[test]
fn t06_funding_event_time_does_not_prove_publication_time() {
    let mut source = row();
    source.funding = Some(Ok(vec![(60_000, 0.01), (120_000, -0.002)]));
    let response = compute_row(source, &request(vec!["funding_sum"]), None).unwrap();
    assert!((response["metrics"]["funding_sum"].as_f64().unwrap() - 0.008).abs() < 1e-12);
    assert!(response["candle_coverage"].is_null());
    assert_eq!(response["quality"], "partial");
    assert_eq!(
        response["metric_time_basis"]["funding_sum"],
        "funding_event_time_publication_unverified"
    );
    assert_eq!(response["funding_observations"]["observed_events"], 2);
    assert_eq!(response["funding_observations"]["first_event_ms"], 60_000);
    assert_eq!(response["funding_observations"]["last_event_ms"], 120_000);
    assert_eq!(
        response["funding_observations"]["rate_basis"],
        "realized_settlement_rate"
    );
    assert_eq!(
        response["funding_observations"]["publication_time_verified"],
        false
    );
    assert!(response["funding_observations"]["expected_events"].is_null());
    assert!(
        response["gaps"]
            .as_array()
            .unwrap()
            .contains(&json!("FUNDING_PUBLICATION_TIME_UNVERIFIED"))
    );
}
#[test]
fn t06_mixed_funding_ticker_and_candles_keep_distinct_time_evidence() {
    let mut source = row();
    source.funding = Some(Ok(vec![(60_000, 0.001)]));
    source.ticker = Some(Ok(okx_research_protocol::research_ipc::ResearchTicker {
        bid_price: "100".into(),
        ask_price: "101".into(),
    }));
    let response = compute_row(
        source,
        &request(vec!["returns", "funding_sum", "spread_bps"]),
        None,
    )
    .unwrap();
    assert_eq!(response["candle_coverage"]["complete"], true);
    assert!(response["metrics"]["returns"].is_number());
    assert_eq!(response["metrics"]["funding_sum"], json!(0.001));
    assert_eq!(
        response["metric_time_basis"]["funding_sum"],
        "funding_event_time_publication_unverified"
    );
    assert_eq!(
        response["metric_time_basis"]["spread_bps"],
        "current_snapshot_unaligned"
    );
    assert_eq!(response["quality"], "partial");
}
#[test]
fn t06_missing_funding_source_has_explicit_first_failure() {
    let mut source = row();
    source.funding = Some(Err("SOURCE_TIMEOUT".into()));
    let response = compute_row(source, &request(vec!["funding_sum"]), None).unwrap();
    assert!(response["metrics"]["funding_sum"].is_null());
    assert!(response["funding_observations"].is_null());
    assert!(
        response["gaps"]
            .as_array()
            .unwrap()
            .contains(&json!("FUNDING_SOURCE_UNAVAILABLE"))
    );
    assert_eq!(response["quality"], "partial");
}
#[test]
fn t06_missing_realized_rate_blocks_historical_sum_with_specific_reason() {
    let mut source = row();
    source.funding = Some(Err("FUNDING_REALIZED_RATE_UNAVAILABLE".into()));
    let response = compute_row(source, &request(vec!["funding_sum"]), None).unwrap();
    assert!(response["metrics"]["funding_sum"].is_null());
    assert!(response["funding_observations"].is_null());
    assert_eq!(response["quality"], "partial");
    assert_eq!(response["gaps"][0], "FUNDING_REALIZED_RATE_UNAVAILABLE");
    assert!(
        response["gaps"]
            .as_array()
            .unwrap()
            .contains(&json!("FUNDING_SOURCE_UNAVAILABLE"))
    );
}
#[test]
fn source_specific_metrics_do_not_hide_candle_coverage_in_mixed_requests() {
    let response = compute_row(row(), &request(vec!["returns", "oi_change"]), None).unwrap();
    assert_eq!(response["candle_coverage"]["complete"], true);
    assert!(response["oi_coverage"].is_null());
    assert_eq!(response["quality"], "partial");
}
#[test]
fn interior_gap_never_imputed() {
    let mut missing = row();
    Arc::make_mut(missing.bars.as_mut().unwrap()).remove(43);
    let v = compute_row(missing, &request(vec!["returns"]), None).unwrap();
    assert_eq!(v["quality"], "partial");
    assert_eq!(v["metrics"]["returns"], Value::Null);
    assert!(
        v["gaps"]
            .as_array()
            .unwrap()
            .contains(&json!("CANDLE_INTERIOR_GAP"))
    );
}

fn request(fields: Vec<&str>) -> MarketRequest {
    serde_json::from_value(json!({"action":"screen","instruments":["BTC-USDT"],"fields":fields,"timeframe_ms":60000,"start_ms":0,"end_ms":6000000})).unwrap()
}
fn row() -> Loaded {
    Loaded {
        instrument: "BTC-USDT".into(),
        bars: Ok(Arc::new(
            (0..100)
                .map(|i| Bar {
                    time: i * 60000,
                    open: 100.0,
                    high: 102.0,
                    low: 99.0,
                    close: 100.0 + i as f64,
                    volume: 1.0,
                    funding_rate: None,
                })
                .collect(),
        )),
        ticker: None,
        funding: None,
        oi: None,
        excluded: Some(1),
    }
}
#[test]
fn c20_rolling_mean_reuses_existing_market_pipeline_without_new_routing() {
    let query = request(vec!["rolling_mean"]);
    assert!(query.valid());
    let plan = planner::AnalysisPlan::build(&query.fields, query.start_ms, query.end_ms).unwrap();
    assert_eq!(plan.sources, std::collections::BTreeSet::from(["candles"]));
    let response = compute_row(row(), &query, None).unwrap();
    assert_eq!(response["units"]["rolling_mean"], "price");
    assert!(response["metrics"]["rolling_mean"].is_number());
    let mut missing = row();
    missing.bars = Ok(Arc::new(Vec::new()));
    let output = compute_row(missing, &query, None).unwrap();
    assert_eq!(output["quality"], "partial");
    assert!(output["metrics"]["rolling_mean"].is_null());
}
#[test]
fn t02_t03_public_row_uses_checked_operator_and_parameter_binding() {
    let query = request(vec!["ema", "returns"]);
    let checked = planner::AnalysisPlan::compile_market(&query).unwrap();
    assert_eq!(checked.parameters["ema_period"], 14.0);
    let result = compute_row_planned(row(), &query, None, &checked, true).unwrap();
    let direct = compute_row(row(), &query, None).unwrap();
    assert_eq!(result["metrics"], direct["metrics"]);
    assert_eq!(result["parameters"]["ema_period"], 14.0);
    let mut changed = checked.clone();
    changed.parameters.insert("ema_period".into(), 3.0);
    let value = compute_row_planned(row(), &query, None, &changed, true).unwrap();
    assert_ne!(value["metrics"]["ema"], result["metrics"]["ema"]);
    assert_eq!(value["parameters"]["ema_period"], 3.0);
    let mut mismatched = query.clone();
    mismatched.fields.reverse();
    assert_eq!(
        compute_row_planned(row(), &mismatched, None, &checked, true).unwrap_err(),
        "COMPILED_PLAN_PROJECTION_MISMATCH"
    );
}
#[test]
fn t07_t15_public_boolean_composition_uses_existing_candle_execution() {
    let mut request = request(vec!["returns"]);
    request.composition = Some(Box::new(
        serde_json::from_value(json!({
            "version":okx_analysis::query_plan::COMPOSITION_VERSION,
            "steps":[
                {"id":"returns_series","op":"returns_series","input":"close"},
                {"id":"positive","op":"greater_than","input":"returns_series","threshold":0},
                {"id":"previous","op":"lag","input":"positive","period":1},
                {"id":"both","op":"and","input":"positive","other":"previous"},
                {"id":"either","op":"or","input":"positive","other":"previous"},
                {"id":"event","op":"rising_edge","input":"both"}
            ],
            "result_steps":["both","either","event"]
        }))
        .unwrap(),
    ));
    let plan = planner::AnalysisPlan::compile_market(&request).unwrap();
    assert_eq!(
        plan.sources,
        vec![okx_analysis::query_plan::Source::Candles]
    );
    let result = compute_row_planned(row(), &request, None, &plan, true).unwrap();
    assert_eq!(result["metrics"]["both"], true);
    assert_eq!(result["metrics"]["either"], true);
    assert_eq!(result["metrics"]["event"], false);
    assert_eq!(result["units"]["both"], "boolean");
    assert_eq!(
        result["composition_quality"]["both"]["status"],
        "available_at_closed_bar"
    );
    let mut missing = row();
    Arc::make_mut(missing.bars.as_mut().unwrap()).remove(42);
    let partial = compute_row_planned(missing, &request, None, &plan, true).unwrap();
    assert!(partial["metrics"]["both"].is_null());
    assert_eq!(
        partial["composition_quality"]["both"]["issues"][0]["first_missing_ms"],
        42 * 60_000
    );
}
#[test]
fn t03_t07_t15_public_rolling_price_cross_is_one_composed_market_request() {
    let mut request = request(vec!["returns"]);
    request.composition = Some(Box::new(
        serde_json::from_value(json!({
            "version":okx_analysis::query_plan::COMPOSITION_VERSION,
            "steps":[
                {"id":"fast","op":"rolling_mean","input":"close","period":1},
                {"id":"slow","op":"rolling_mean","input":"close","period":3},
                {"id":"above","op":"greater_than_series","input":"fast","other":"slow"},
                {"id":"cross","op":"rising_edge","input":"above"}
            ],
            "result_steps":["above","cross"]
        }))
        .unwrap(),
    ));
    let plan = planner::AnalysisPlan::compile_market(&request).unwrap();
    assert_eq!(
        plan.sources,
        vec![okx_analysis::query_plan::Source::Candles]
    );
    let mut loaded = row();
    let bars = Arc::make_mut(loaded.bars.as_mut().unwrap());
    for bar in &mut bars[96..99] {
        bar.open = 100.0;
        bar.high = 102.0;
        bar.low = 99.0;
        bar.close = 100.0;
    }
    bars[99].open = 100.0;
    bars[99].low = 99.0;
    bars[99].high = 112.0;
    bars[99].close = 110.0;
    let result = compute_row_planned(loaded, &request, None, &plan, true).unwrap();
    assert_eq!(result["metrics"]["above"], true);
    assert_eq!(result["metrics"]["cross"], true);
    assert_eq!(result["units"]["cross"], "event");
    assert_eq!(
        result["composition_quality"]["cross"]["status"],
        "available_at_closed_bar"
    );
    assert!(result["source_captures"]["candles"]["capture_hash"].is_string());
}
#[test]
fn t07_t15_public_bidirectional_exit_and_population_risk_share_the_checked_market_route() {
    let mut request = request(vec!["returns"]);
    request.composition = Some(Box::new(
        serde_json::from_value(json!({
            "version":okx_analysis::query_plan::COMPOSITION_VERSION,
            "steps":[
                {"id":"fast","op":"rolling_mean","input":"close","period":2},
                {"id":"slow","op":"rolling_mean","input":"close","period":3},
                {"id":"below","op":"less_than_series","input":"fast","other":"slow"},
                {"id":"exit","op":"falling_edge","input":"below"},
                {"id":"risk","op":"rolling_stddev","input":"close","period":2},
                {"id":"quiet","op":"less_than","input":"risk","threshold":4.0}
            ],
            "result_steps":["below","exit","risk","quiet"]
        }))
        .unwrap(),
    ));
    let plan = planner::AnalysisPlan::compile_market(&request).unwrap();
    assert_eq!(
        plan.sources,
        vec![okx_analysis::query_plan::Source::Candles]
    );
    let mut loaded = row();
    let bars = Arc::make_mut(loaded.bars.as_mut().unwrap());
    for (index, close) in [(95, 100.0), (96, 99.0), (97, 97.0), (98, 95.0), (99, 102.0)] {
        bars[index].close = close;
        bars[index].open = close;
        bars[index].high = close + 1.0;
        bars[index].low = close - 1.0;
    }
    let result = compute_row_planned(loaded.clone(), &request, None, &plan, true).unwrap();
    assert_eq!(result["metrics"]["below"], false);
    assert_eq!(result["metrics"]["exit"], true);
    assert_eq!(result["metrics"]["risk"], 3.5);
    assert_eq!(result["metrics"]["quiet"], true);
    assert_eq!(result["units"]["risk"], "price");
    assert_eq!(result["units"]["exit"], "event");
    assert_eq!(
        result["composition_quality"]["risk"]["status"],
        "available_at_closed_bar"
    );
    assert!(result["source_captures"]["candles"]["capture_hash"].is_string());
    Arc::make_mut(loaded.bars.as_mut().unwrap()).remove(98);
    let missing = compute_row_planned(loaded, &request, None, &plan, true).unwrap();
    assert!(missing["metrics"]["risk"].is_null());
    assert!(missing["metrics"]["exit"].is_null());
    assert_eq!(
        missing["composition_quality"]["risk"]["issues"][0]["reason"],
        "CANDLE_RANGE_INCOMPLETE_OR_GAPPED"
    );
    assert_eq!(
        missing["composition_quality"]["risk"]["issues"][0]["source"],
        "candles"
    );
    assert_eq!(
        missing["composition_quality"]["risk"]["issues"][0]["causal_null_reason"],
        "SOURCE_OBSERVATION_MISSING"
    );
    assert_eq!(
        missing["composition_quality"]["risk"]["issues"][0]["origin_step"],
        "close"
    );
    assert_eq!(
        missing["composition_quality"]["risk"]["issues"][0]["origin_event_ms"],
        98 * 60_000
    );
    assert_eq!(
        missing["composition_quality"]["risk"]["issues"][0]["first_missing_ms"],
        98 * 60_000
    );
}
#[test]
fn t02_t04_public_ohlc_graph_diagnoses_a_missing_high_without_forward_fill() {
    let mut request = request(vec!["returns"]);
    request.composition = Some(Box::new(
        serde_json::from_value(json!({
            "version":okx_analysis::query_plan::COMPOSITION_VERSION,
            "steps":[
                {"id":"body","op":"greater_than_series","input":"close","other":"open"},
                {"id":"range","op":"greater_than_series","input":"high","other":"low"},
                {"id":"confirmed","op":"and","input":"body","other":"range"}
            ],
            "result_steps":["confirmed"]
        }))
        .unwrap(),
    ));
    let plan = planner::AnalysisPlan::compile_market(&request).unwrap();
    let mut loaded = row();
    let bars = Arc::make_mut(loaded.bars.as_mut().unwrap());
    bars[99].open = 190.0;
    bars[99].low = 189.0;
    bars[99].high = 201.0;
    bars[99].close = 199.0;
    let present = compute_row_planned(loaded.clone(), &request, None, &plan, true).unwrap();
    assert_eq!(present["metrics"]["confirmed"], true);
    assert_eq!(present["units"]["confirmed"], "boolean");
    let bars = Arc::make_mut(loaded.bars.as_mut().unwrap());
    bars[99].high = f64::NAN;
    let missing = compute_row_planned(loaded, &request, None, &plan, true).unwrap();
    assert!(missing["metrics"]["confirmed"].is_null());
    assert_eq!(
        missing["composition_quality"]["confirmed"]["issues"][0]["origin_step"],
        "high"
    );
    assert_eq!(
        missing["composition_quality"]["confirmed"]["issues"][0]["origin_event_ms"],
        99 * 60_000
    );
    assert_eq!(
        missing["composition_quality"]["confirmed"]["issues"][0]["reason"],
        "SOURCE_OBSERVATION_MISSING"
    );
}
#[test]
fn t02_t04_t07_t13_native_volume_composition_preserves_units_and_source_capture() {
    let mut request = request(vec!["returns"]);
    request.composition = Some(Box::new(
        serde_json::from_value(json!({
            "version":okx_analysis::query_plan::COMPOSITION_VERSION,
            "steps":[
                {"id":"fast","op":"rolling_mean","input":"volume","period":2},
                {"id":"slow","op":"rolling_mean","input":"volume","period":3},
                {"id":"lift","op":"greater_than_series","input":"fast","other":"slow"},
                {"id":"entry","op":"rising_edge","input":"lift"}
            ],
            "result_steps":["fast","lift","entry"]
        }))
        .unwrap(),
    ));
    let plan = planner::AnalysisPlan::compile_market(&request).unwrap();
    assert_eq!(
        plan.sources,
        vec![okx_analysis::query_plan::Source::Candles]
    );
    let mut loaded = row();
    let bars = Arc::make_mut(loaded.bars.as_mut().unwrap());
    for bar in &mut bars[95..99] {
        bar.volume = 10.0;
    }
    bars[99].volume = 30.0;
    let result = compute_row_planned(loaded.clone(), &request, None, &plan, true).unwrap();
    assert_eq!(result["metrics"]["fast"], 20.0);
    assert_eq!(result["units"]["fast"], "native_volume");
    assert_eq!(result["metrics"]["lift"], true);
    assert_eq!(result["metrics"]["entry"], true);
    assert_eq!(
        result["composition_quality"]["entry"]["status"],
        "available_at_closed_bar"
    );
    let capture = result["source_captures"]["candles"]["capture_hash"].clone();
    assert!(capture.is_string());
    Arc::make_mut(loaded.bars.as_mut().unwrap())[99].volume = 31.0;
    let revised = compute_row_planned(loaded.clone(), &request, None, &plan, true).unwrap();
    assert_ne!(
        revised["source_captures"]["candles"]["capture_hash"],
        capture
    );
    Arc::make_mut(loaded.bars.as_mut().unwrap())[99].volume = -1.0;
    assert_eq!(
        compute_row_planned(loaded, &request, None, &plan, true).unwrap_err(),
        "composition.sources.volume:INVALID_NON_NEGATIVE_VOLUME"
    );
}
#[test]
fn t10_unprojected_oi_and_funding_branches_never_require_sources_on_spot() {
    let mut request = request(vec!["returns"]);
    request.composition = Some(Box::new(serde_json::from_value(json!({
        "version":okx_analysis::query_plan::COMPOSITION_VERSION,
        "steps":[
            {"id":"r","op":"returns_series","input":"close"},
            {"id":"projected","op":"rolling_mean","input":"r","period":2},
            {"id":"unneeded_oi","op":"returns_series","input":"open_interest"},
            {"id":"unneeded_funding","op":"rolling_mean","input":"funding_rate","period":2},
            {"id":"unneeded_pair","op":"subtract_series","input":"unneeded_oi","other":"unneeded_funding"}
        ],
        "result_steps":["projected"]
    })).unwrap()));
    let plan = planner::AnalysisPlan::compile_market(&request).unwrap();
    assert_eq!(
        plan.sources,
        vec![okx_analysis::query_plan::Source::Candles]
    );
    assert!(
        !plan
            .composition
            .as_ref()
            .unwrap()
            .sources()
            .contains(&okx_analysis::query_plan::Source::OpenInterest)
    );
    assert_eq!(plan.bound_series_source_ports().unwrap().len(), 1);
    // No OI/funding values exist in this spot fixture. An unreachable
    // branch must not downgrade the projected result or invent evidence.
    let result = compute_row_planned(row(), &request, None, &plan, true).unwrap();
    assert!(result["metrics"]["projected"].is_number());
    assert_eq!(
        result["composition_quality"]["projected"]["status"],
        "available_at_closed_bar"
    );
    assert!(result["source_captures"].get("open_interest").is_none());
    assert!(result["source_captures"].get("funding_rate").is_none());
    assert_eq!(result["quality"], "complete");
}
#[test]
fn t05_composition_reuses_closed_candles_and_projects_a_distinct_ratio() {
    let mut request = request(vec!["returns"]);
    request.start_ms = 0;
    request.end_ms = 180_000;
    request.composition = Some(Box::new(okx_analysis::query_plan::CompositionRequest {
        version: okx_analysis::query_plan::COMPOSITION_VERSION.into(),
        steps: vec![
            okx_analysis::query_plan::SeriesStep {
                id: "r1".into(),
                op: okx_analysis::query_plan::SeriesOp::ReturnsSeries,
                input: "close".into(),
                period: None,
                threshold: None,
                when: None,
                other: None,
            },
            okx_analysis::query_plan::SeriesStep {
                id: "w1".into(),
                op: okx_analysis::query_plan::SeriesOp::RollingMean,
                input: "r1".into(),
                period: Some(2),
                threshold: None,
                when: None,
                other: None,
            },
        ],
        result_steps: vec!["w1".into()],
    }));
    let mut fixture = row();
    fixture.bars = Ok(Arc::new(vec![
        Bar {
            time: 0,
            open: 100.0,
            high: 100.0,
            low: 100.0,
            close: 100.0,
            volume: 1.0,
            funding_rate: None,
        },
        Bar {
            time: 60_000,
            open: 110.0,
            high: 110.0,
            low: 110.0,
            close: 110.0,
            volume: 1.0,
            funding_rate: None,
        },
        Bar {
            time: 120_000,
            open: 99.0,
            high: 99.0,
            low: 99.0,
            close: 99.0,
            volume: 1.0,
            funding_rate: None,
        },
    ]));
    let checked = planner::AnalysisPlan::compile_market(&request).unwrap();
    let composition = checked.composition.as_ref();
    let mut partial_fixture = row();
    partial_fixture.bars = fixture.bars.clone();
    let output = compute_row_checked(fixture, &request, None, composition, true).unwrap();
    assert!((output["metrics"]["returns"].as_f64().unwrap() + 0.01).abs() < 1e-12);
    assert!(output["metrics"]["w1"].as_f64().unwrap().abs() < 1e-12);
    assert_eq!(output["units"]["w1"], "ratio");
    Arc::make_mut(partial_fixture.bars.as_mut().unwrap()).remove(1);
    let partial = compute_row_checked(partial_fixture, &request, None, composition, true).unwrap();
    assert_eq!(partial["metrics"]["w1"], Value::Null);
    assert_eq!(partial["quality"], "partial");
}
#[test]
fn t15_public_window_extrema_share_candle_source_and_preserve_units() {
    let mut request = request(vec!["returns"]);
    request.start_ms = 0;
    request.end_ms = 180_000;
    request.composition = Some(Box::new(
        serde_json::from_value(json!({
            "version":"okx.market.composition/v1",
            "steps":[
                {"id":"upper","op":"rolling_max","input":"close","period":3},
                {"id":"lower","op":"rolling_min","input":"close","period":3}
            ],
            "result_steps":["upper","lower"]
        }))
        .unwrap(),
    ));
    let mut fixture = row();
    fixture.bars = Ok(Arc::new(
        [100.0, 110.0, 99.0]
            .into_iter()
            .enumerate()
            .map(|(i, price)| Bar {
                time: i as u64 * 60_000,
                open: price,
                high: price,
                low: price,
                close: price,
                volume: 1.0,
                funding_rate: None,
            })
            .collect(),
    ));
    let checked = planner::AnalysisPlan::compile_market(&request).unwrap();
    assert_eq!(
        checked.sources,
        vec![okx_analysis::query_plan::Source::Candles]
    );
    let mut partial_fixture = row();
    partial_fixture.bars = fixture.bars.clone();
    let out =
        compute_row_checked(fixture, &request, None, checked.composition.as_ref(), true).unwrap();
    assert_eq!(out["metrics"]["upper"], 110.0);
    assert_eq!(out["metrics"]["lower"], 99.0);
    assert_eq!(out["units"]["upper"], "price");
    assert_eq!(out["units"]["lower"], "price");
    Arc::make_mut(partial_fixture.bars.as_mut().unwrap()).remove(1);
    let partial = compute_row_checked(
        partial_fixture,
        &request,
        None,
        checked.composition.as_ref(),
        true,
    )
    .unwrap();
    assert_eq!(partial["quality"], "partial");
    assert!(partial["metrics"]["upper"].is_null());
    assert!(partial["metrics"]["lower"].is_null());
}
#[test]
fn t07_event_graph_exposes_boolean_metrics_but_not_unconfirmed_future_outcomes() {
    let mut query = request(vec!["returns"]);
    query.start_ms = 0;
    query.end_ms = 300_000;
    query.composition = Some(Box::new(
        serde_json::from_value(json!({
            "version":"okx.market.composition/v1",
            "steps":[
                {"id":"r1","op":"returns_series","input":"close"},
                {"id":"above","op":"greater_than","input":"r1","threshold":0.0},
                {"id":"entry","op":"rising_edge","input":"above"},
                {"id":"previous","op":"lag","input":"close","period":1},
                {"id":"label","op":"forward_return_label","input":"close","period":2,"when":"entry"}
            ],
            "result_steps":["above","entry","previous","label"]
        }))
        .unwrap(),
    ));
    let mut fixture = row();
    fixture.bars = Ok(Arc::new(
        [100.0, 90.0, 95.0, 99.0, 120.0]
            .iter()
            .enumerate()
            .map(|(i, &close)| Bar {
                time: i as u64 * 60_000,
                open: close,
                high: close,
                low: close,
                close,
                volume: 1.0,
                funding_rate: None,
            })
            .collect(),
    ));
    let checked = planner::AnalysisPlan::compile_market(&query).unwrap();
    let result =
        compute_row_checked(fixture, &query, None, checked.composition.as_ref(), true).unwrap();
    assert_eq!(result["metrics"]["above"], true);
    assert_eq!(result["units"]["above"], "boolean");
    assert_eq!(result["metrics"]["entry"], false);
    assert_eq!(result["units"]["entry"], "event");
    assert_eq!(result["metrics"]["previous"], 99.0);
    assert_eq!(result["metrics"]["label"], Value::Null);
    assert_eq!(result["units"]["label"], "outcome_ratio");
    assert!(
        result["gaps"]
            .as_array()
            .unwrap()
            .iter()
            .any(|gap| gap.as_str() == Some("OUTCOME_UNCONFIRMED:label"))
    );
    assert_eq!(result["quality"], "partial");
}
#[test]
fn new_downside_operator_uses_existing_market_plan_and_projection() {
    let query = request(vec!["downside_volatility"]);
    assert!(query.valid());
    let plan = planner::AnalysisPlan::build(&query.fields, query.start_ms, query.end_ms).unwrap();
    assert_eq!(plan.sources, std::collections::BTreeSet::from(["candles"]));
    let output = compute_row(row(), &query, None).unwrap();
    assert_eq!(output["metrics"]["downside_volatility"], 0.0);
    assert_eq!(output["units"]["downside_volatility"], "annualized_ratio");
    let mut missing = row();
    missing.bars = Ok(Arc::new(Vec::new()));
    let output = compute_row(missing, &query, None).unwrap();
    assert_eq!(output["quality"], "partial");
    assert!(output["metrics"]["downside_volatility"].is_null());
}
#[test]
fn unused_missing_source_does_not_degrade() {
    let r = compute_row(row(), &request(vec!["returns"]), None).unwrap();
    assert_eq!(r["quality"], "complete");
    assert_eq!(r["metrics"]["returns"], json!(0.99));
}
#[test]
fn closed_candle_filter_preserves_quality() {
    let r = compute_row(row(), &request(vec!["returns"]), None).unwrap();
    assert_eq!(r["unclosed_candles_excluded"], 1);
    assert_eq!(r["quality"], "complete");
}
#[test]
fn missing_requested_source_is_partial_not_zero() {
    let r = compute_row(row(), &request(vec!["spread_bps"]), None).unwrap();
    assert_eq!(r["quality"], "partial");
    assert!(r["metrics"]["spread_bps"].is_null());
}

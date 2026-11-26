use super::*;
#[test]
fn global_rank_rejects_first_last_and_interior_candle_gaps() {
    let bar = |t| Bar {
        time: t,
        open: 1.0,
        high: 1.0,
        low: 1.0,
        close: 1.0,
        volume: 1.0,
        funding_rate: None,
    };
    let full = vec![bar(0), bar(900_000), bar(1_800_000)];
    assert!(complete_rank_history(&full, 0, 2_700_000, 900_000));
    assert!(!complete_rank_history(&full[1..], 0, 2_700_000, 900_000));
    assert!(!complete_rank_history(&full[..2], 0, 2_700_000, 900_000));
    assert!(!complete_rank_history(
        &[bar(0), bar(1_800_000)],
        0,
        2_700_000,
        900_000
    ));
}
#[test]
fn sorted_global_rank_has_stable_ties_and_no_page_local_shortcut() {
    let mut rows = vec![
        json!({"instrument":"AA","metrics":{"returns":0.05},"quality":"complete"}),
        json!({"instrument":"ZZ","metrics":{"returns":0.09},"quality":"complete"}),
        json!({"instrument":"BB","metrics":{"returns":0.09},"quality":"complete"}),
        json!({"instrument":"CC","metrics":{"returns":-0.02},"quality":"complete"}),
    ];
    let sort = MarketSort {
        field: "returns".into(),
        descending: true,
    };
    sort_market_rows(&mut rows, Some(&sort), true).unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| r["instrument"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["BB", "ZZ", "AA", "CC"]
    );
    rows[3]["quality"] = json!("partial");
    assert_eq!(
        sort_market_rows(&mut rows, Some(&sort), true).unwrap_err(),
        "GLOBAL_RANK_SOURCE_INCOMPLETE"
    );
}
#[test]
fn t06_t07_t13_oi_provenance_is_stable_and_does_not_certify_a_signal() {
    let mut query: MarketRequest = serde_json::from_value(json!({
        "action":"screen",
        "instruments":["BTC-USDT-SWAP"],
        "fields":["returns"],
        "timeframe_ms":60000,
        "start_ms":0,
        "end_ms":6000000
    }))
    .unwrap();
    query.composition = Some(Box::new(
        serde_json::from_value(json!({
            "version":"okx.market.composition/v1",
            "steps":[
                {"id":"oi_return","op":"returns_series","input":"open_interest"},
                {"id":"positive","op":"greater_than","input":"oi_return","threshold":0.0},
                {"id":"event","op":"rising_edge","input":"positive"}
            ],
            "result_steps":["oi_return","event"]
        }))
        .unwrap(),
    ));
    let checked = planner::AnalysisPlan::compile_market(&query).unwrap();
    let points = (0..100)
        .map(|i| (i * 60_000, 10.0 + i as f64))
        .collect::<Vec<_>>();
    let fixture = || Loaded {
        instrument: "BTC-USDT-SWAP".into(),
        bars: Ok(Arc::new(
            (0..100)
                .map(|i| Bar {
                    time: i * 60_000,
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
        oi: Some(Ok(points.clone())),
        excluded: None,
    };
    let first =
        compute_row_checked(fixture(), &query, None, checked.composition.as_ref(), true)
            .unwrap();
    let repeat =
        compute_row_checked(fixture(), &query, None, checked.composition.as_ref(), true)
            .unwrap();
    assert_eq!(first["source_captures"], repeat["source_captures"]);
    assert!(first["metrics"]["oi_return"].is_number());
    assert!(first["metrics"]["event"].is_null());
    assert_eq!(
        first["composition_quality"]["oi_return"]["status"],
        "retrospective_publication_unverified"
    );
    assert_eq!(
        first["composition_quality"]["event"]["issues"][0]["reason"],
        "CAUSAL_PUBLICATION_TIME_UNVERIFIED"
    );
    assert_eq!(
        first["metric_time_basis"]["event"],
        "oi_event_time_publication_unverified"
    );
    assert!(
        first["gaps"]
            .as_array()
            .unwrap()
            .contains(&json!("CAUSAL_AVAILABILITY_UNVERIFIED:event"))
    );
    let mut changed = fixture();
    changed
        .oi
        .as_mut()
        .unwrap()
        .as_mut()
        .unwrap()
        .last_mut()
        .unwrap()
        .1 += 1.0;
    let after =
        compute_row_checked(changed, &query, None, checked.composition.as_ref(), true).unwrap();
    assert_ne!(
        first["source_captures"]["open_interest"]["capture_hash"],
        after["source_captures"]["open_interest"]["capture_hash"]
    );
    let mut gap = fixture();
    gap.oi.as_mut().unwrap().as_mut().unwrap().remove(42);
    let partial =
        compute_row_checked(gap, &query, None, checked.composition.as_ref(), true).unwrap();
    assert!(partial["metrics"]["oi_return"].is_null());
    assert_eq!(
        partial["composition_quality"]["oi_return"]["issues"][0]["first_missing_ms"],
        42 * 60_000
    );
}

#[test]
fn t02_t06_funding_events_join_price_returns_with_provenance_not_causal_signals() {
    let query: MarketRequest = serde_json::from_value(json!({
        "action":"screen","instruments":["BTC-USDT-SWAP"],"fields":["returns"],
        "timeframe_ms":60000,"start_ms":0,"end_ms":6000000,
        "composition":{"version":"okx.market.composition/v1","steps":[
            {"id":"price_return","op":"returns_series","input":"close"},
            {"id":"net_return","op":"subtract_series","input":"price_return","other":"funding_rate"},
            {"id":"positive","op":"greater_than","input":"net_return","threshold":0.0}
        ],"result_steps":["net_return","positive"]}
    })).unwrap();
    let plan = planner::AnalysisPlan::compile_market(&query).unwrap();
    assert!(
        plan.sources
            .contains(&okx_analysis::query_plan::Source::Funding)
    );
    let fixture = || Loaded {
        instrument: "BTC-USDT-SWAP".into(),
        bars: Ok(Arc::new(
            (0..100)
                .map(|i| Bar {
                    time: i * 60_000,
                    open: 100.0,
                    high: 200.0,
                    low: 99.0,
                    close: 100.0 + i as f64,
                    volume: 1.0,
                    funding_rate: None,
                })
                .collect(),
        )),
        ticker: None,
        funding: Some(Ok(vec![
            (60_000, 0.001),
            (60_001, 0.006),
            (99 * 60_000, -0.002),
        ])),
        oi: None,
        excluded: None,
    };
    let first = compute_row_planned(fixture(), &query, None, &plan, true).unwrap();
    let repeated = compute_row_planned(fixture(), &query, None, &plan, true).unwrap();
    assert_eq!(first["source_captures"], repeated["source_captures"]);
    assert!(
        (first["metrics"]["net_return"].as_f64().unwrap() - (199.0 / 198.0 - 1.0 + 0.002))
            .abs()
            < 1e-12
    );
    assert!(first["metrics"]["positive"].is_null());
    assert_eq!(
        first["composition_quality"]["net_return"]["status"],
        "retrospective_publication_unverified"
    );
    assert_eq!(
        first["composition_quality"]["positive"]["issues"][0]["reason"],
        "CAUSAL_PUBLICATION_TIME_UNVERIFIED"
    );
    assert_eq!(
        first["metric_time_basis"]["net_return"],
        "funding_event_time_publication_unverified"
    );
    assert_eq!(first["funding_event_alignment"]["observed_events"], 3);
    assert_eq!(first["funding_event_alignment"]["aligned_events"], 2);
    assert_eq!(first["funding_event_alignment"]["off_grid_events"], 1);
    assert_eq!(
        first["funding_event_alignment"]["first_off_grid_ms"],
        60_001
    );
    assert_eq!(first["funding_event_alignment"]["first_empty_grid_ms"], 0);
    assert_eq!(
        first["funding_event_alignment"]["expected_events"],
        Value::Null
    );
    assert_eq!(
        first["funding_event_alignment"]["publication_time_verified"],
        false
    );
    assert_eq!(
        first["source_captures"]["funding_rate"]["observed_events"],
        3
    );
    assert_eq!(first["source_captures"]["candles"]["observed_points"], 100);
    assert!(
        first["gaps"]
            .as_array()
            .unwrap()
            .contains(&json!("CAUSAL_AVAILABILITY_UNVERIFIED:positive"))
    );

    let mut changed = fixture();
    changed
        .funding
        .as_mut()
        .unwrap()
        .as_mut()
        .unwrap()
        .last_mut()
        .unwrap()
        .1 = -0.003;
    let after = compute_row_planned(changed, &query, None, &plan, true).unwrap();
    assert_ne!(
        first["source_captures"]["funding_rate"]["capture_hash"],
        after["source_captures"]["funding_rate"]["capture_hash"]
    );
    let mut absent = fixture();
    absent.funding.as_mut().unwrap().as_mut().unwrap().pop();
    let missing = compute_row_planned(absent, &query, None, &plan, true).unwrap();
    assert!(missing["metrics"]["net_return"].is_null());
    assert_eq!(
        missing["composition_quality"]["net_return"]["issues"][0],
        json!({"reason":"SOURCE_OBSERVATION_MISSING","origin_step":"funding_rate", "origin_event_ms":99 * 60_000})
    );
    assert_eq!(
        missing["funding_event_alignment"]["last_bar_has_event"],
        false
    );
    let mut unavailable = fixture();
    unavailable.funding = Some(Err("FUNDING_RANGE_UNAVAILABLE".into()));
    let failed = compute_row_planned(unavailable, &query, None, &plan, true).unwrap();
    assert_eq!(
        failed["composition_quality"]["net_return"]["issues"][0]["reason"],
        "FUNDING_SOURCE_UNAVAILABLE"
    );
    assert!(failed["source_captures"]["funding_rate"].is_null());
}

#[test]
fn t06_shifted_oi_event_cannot_fill_a_candle_gap_or_certify_a_signal() {
    let mut request: MarketRequest = serde_json::from_value(json!({
        "action":"screen",
        "instruments":["BTC-USDT-SWAP"],
        "fields":["returns"],
        "timeframe_ms":60000,
        "start_ms":0,
        "end_ms":6000000
    }))
    .unwrap();
    request.composition = Some(Box::new(
        serde_json::from_value(json!({
            "version":okx_analysis::query_plan::COMPOSITION_VERSION,
            "steps":[
                {"id":"oi_change","op":"returns_series","input":"open_interest"},
                {"id":"positive","op":"greater_than","input":"oi_change","threshold":0.0}
            ],
            "result_steps":["oi_change","positive"]
        }))
        .unwrap(),
    ));
    let plan = planner::AnalysisPlan::compile_market(&request).unwrap();
    let mut loaded = Loaded {
        instrument: "BTC-USDT-SWAP".into(),
        bars: Ok(Arc::new(
            (0..100)
                .map(|index| Bar {
                    time: index * 60_000,
                    open: 100.0,
                    high: 200.0,
                    low: 99.0,
                    close: 100.0 + index as f64,
                    volume: 1.0,
                    funding_rate: None,
                })
                .collect(),
        )),
        ticker: None,
        funding: None,
        oi: None,
        excluded: None,
    };
    let mut oi = (0..100)
        .map(|index| (index * 60_000, 20.0 + index as f64))
        .collect::<Vec<_>>();
    // A one-millisecond shift is not the closed-bar event at 42 * 60,000.
    // A nearest/as-of join would fabricate a usable point here.
    oi[42].0 += 1;
    loaded.oi = Some(Ok(oi));
    let result = compute_row_planned(loaded, &request, None, &plan, true).unwrap();
    assert_eq!(result["source_alignment"]["observed_oi_observations"], 99);
    assert_eq!(result["source_alignment"]["aligned_observations"], 99);
    assert_eq!(result["source_alignment"]["oi_off_grid_events"], 1);
    assert_eq!(
        result["source_alignment"]["oi_first_off_grid_ms"],
        42 * 60_000 + 1
    );
    assert_eq!(
        result["source_alignment"]["first_unaligned_ms"],
        42 * 60_000
    );
    assert_eq!(result["oi_coverage"]["first_missing_ms"], 42 * 60_000);
    assert!(result["metrics"]["oi_change"].is_null());
    assert!(result["metrics"]["positive"].is_null());
    assert_eq!(
        result["composition_quality"]["oi_change"]["issues"][0]["reason"],
        "OI_RANGE_INCOMPLETE_OR_GAPPED"
    );
    assert_eq!(
        result["composition_quality"]["positive"]["issues"][1]["reason"],
        "CAUSAL_PUBLICATION_TIME_UNVERIFIED"
    );
    assert_eq!(
        result["source_captures"]["open_interest"]["publication_time_verified"],
        false
    );
}

#[test]
fn t06_funding_only_graph_does_not_require_or_invent_candles() {
    let query: MarketRequest = serde_json::from_value(json!({
        "action":"screen","instruments":["ETH-USDT-SWAP"],"fields":["funding_sum"],
        "timeframe_ms":60000,"start_ms":0,"end_ms":6000000,
        "composition":{"version":"okx.market.composition/v1","steps":[
            {"id":"rate","op":"rolling_mean","input":"funding_rate","period":1}
        ],"result_steps":["rate"]}
    }))
    .unwrap();
    let plan = planner::AnalysisPlan::compile_market(&query).unwrap();
    assert!(
        !plan
            .sources
            .contains(&okx_analysis::query_plan::Source::Candles)
    );
    let loaded = Loaded {
        instrument: "ETH-USDT-SWAP".into(),
        bars: Ok(Arc::new(vec![])),
        ticker: None,
        funding: Some(Ok(vec![(99 * 60_000, -0.001)])),
        oi: None,
        excluded: None,
    };
    let result = compute_row_planned(loaded, &query, None, &plan, false).unwrap();
    assert_eq!(result["metrics"]["funding_sum"], json!(-0.001));
    assert_eq!(result["metrics"]["rate"], json!(-0.001));
    assert_eq!(result["funding_event_alignment"]["aligned_events"], 1);
    assert_eq!(result["source_captures"]["candles"], Value::Null);
    assert_eq!(result["candle_coverage"], Value::Null);
    assert_eq!(
        result["composition_quality"]["rate"]["status"],
        "retrospective_publication_unverified"
    );
}
#[test]
fn t15_range_extension_uses_existing_market_planner_and_user_composes_new_graph() {
    let mut query: MarketRequest = serde_json::from_value(json!({
        "action":"screen","instruments":["BTC-USDT"],"fields":["returns"],
        "timeframe_ms":60000,"start_ms":60000,"end_ms":6060000,
        "composition":{"version":"okx.market.composition/v1","steps":[
            {"id":"range_price","op":"rolling_range","input":"close","period":3},
            {"id":"wide","op":"greater_than","input":"range_price","threshold":5.0},
            {"id":"entry","op":"rising_edge","input":"wide"},
            {"id":"smooth","op":"rolling_mean","input":"range_price","period":2},
            {"id":"returns","op":"returns_series","input":"close"},
            {"id":"range_return","op":"rolling_range","input":"returns","period":2}
        ],"result_steps":["range_price","wide","entry","smooth","range_return"]}
    }))
    .unwrap();
    let loaded = || Loaded {
        instrument: "BTC-USDT".into(),
        bars: Ok(Arc::new(
            (0..100)
                .map(|i| Bar {
                    time: (i + 1) * 60_000,
                    open: 100.0,
                    high: 115.0,
                    low: 99.0,
                    close: if i == 99 {
                        110.0
                    } else if i == 98 {
                        101.0
                    } else {
                        100.0
                    },
                    volume: 1.0,
                    funding_rate: None,
                })
                .collect(),
        )),
        ticker: None,
        funding: None,
        oi: None,
        excluded: None,
    };
    let plan = planner::AnalysisPlan::compile_market(&query).unwrap();
    let result = compute_row_planned(loaded(), &query, None, &plan, true).unwrap();
    assert_eq!(result["metrics"]["range_price"], 10.0);
    assert_eq!(result["metrics"]["wide"], true);
    assert_eq!(result["metrics"]["entry"], true);
    assert_eq!(result["metrics"]["smooth"], 5.5);
    let return_range = 110.0 / 101.0 - 1.0 - 0.01;
    assert!((result["metrics"]["range_return"].as_f64().unwrap() - return_range).abs() < 1e-12);
    assert_eq!(result["units"]["range_price"], "price");
    assert_eq!(result["units"]["range_return"], "ratio");
    assert_eq!(
        result["composition_quality"]["entry"]["status"],
        "available_at_closed_bar"
    );
    assert_eq!(result["quality"], "complete");

    // Another user graph: same Worker build, unchanged source/route and
    // kernel executable; only the validated request threshold changes.
    query.composition.as_mut().unwrap().steps[1].threshold = Some(20.0);
    let changed_plan = planner::AnalysisPlan::compile_market(&query).unwrap();
    let changed = compute_row_planned(loaded(), &query, None, &changed_plan, true).unwrap();
    assert_eq!(
        changed["metrics"]["range_price"],
        result["metrics"]["range_price"]
    );
    assert_eq!(changed["metrics"]["wide"], false);
    assert_eq!(changed["metrics"]["entry"], false);
}

#[test]
fn t15_public_zscore_composes_price_deviation_into_a_causal_condition() {
    let query: MarketRequest = serde_json::from_value(json!({
        "action":"screen","instruments":["BTC-USDT"],"fields":["returns"],
        "timeframe_ms":60000,"start_ms":0,"end_ms":6000000,
        "composition":{"version":"okx.market.composition/v1","steps":[
            {"id":"deviation","op":"rolling_zscore","input":"close","period":3},
            {"id":"above","op":"greater_than","input":"deviation","threshold":1.0}
        ],"result_steps":["deviation","above"]}
    }))
    .unwrap();
    let plan = planner::AnalysisPlan::compile_market(&query).unwrap();
    let fixture = |flat: bool| Loaded {
        instrument: "BTC-USDT".into(),
        bars: Ok(Arc::new(
            (0..100)
                .map(|i| Bar {
                    time: i * 60_000,
                    open: 100.0,
                    high: 201.0,
                    low: 99.0,
                    close: if flat { 100.0 } else { 100.0 + i as f64 },
                    volume: 1.0,
                    funding_rate: None,
                })
                .collect(),
        )),
        ticker: None,
        funding: None,
        oi: None,
        excluded: None,
    };
    let output = compute_row_planned(fixture(false), &query, None, &plan, true).unwrap();
    assert!(
        (output["metrics"]["deviation"].as_f64().unwrap() - (1.0 / (2.0f64 / 3.0).sqrt()))
            .abs()
            < 1e-10
    );
    assert_eq!(output["metrics"]["above"], true);
    assert_eq!(output["units"]["deviation"], "coefficient");
    assert_eq!(
        output["composition_quality"]["above"]["status"],
        "available_at_closed_bar"
    );
    assert_eq!(output["quality"], "complete");
    let flat = compute_row_planned(fixture(true), &query, None, &plan, true).unwrap();
    assert!(flat["metrics"]["deviation"].is_null());
    assert_eq!(
        flat["composition_quality"]["deviation"]["issues"][0]["reason"],
        "ZERO_VARIANCE"
    );
    assert!(flat["metrics"]["above"].is_null());
    assert_eq!(
        flat["composition_quality"]["above"]["issues"][0]["origin_step"],
        "deviation"
    );
}
#[test]
fn t02_t06_funding_and_open_interest_keep_independent_source_evidence() {
    let query: MarketRequest = serde_json::from_value(json!({
        "action":"screen","instruments":["BTC-USDT-SWAP"],"fields":["funding_sum"],
        "timeframe_ms":60000,"start_ms":0,"end_ms":6000000,
        "composition":{"version":"okx.market.composition/v1","steps":[
            {"id":"oi_return","op":"returns_series","input":"open_interest"},
            {"id":"joint","op":"add_series","input":"funding_rate","other":"oi_return"}
        ],"result_steps":["joint"]}
    }))
    .unwrap();
    let plan = planner::AnalysisPlan::compile_market(&query).unwrap();
    assert!(
        !plan
            .sources
            .contains(&okx_analysis::query_plan::Source::Candles)
    );
    let loaded = Loaded {
        instrument: "BTC-USDT-SWAP".into(),
        bars: Ok(Arc::new(vec![])),
        ticker: None,
        funding: Some(Ok(vec![(99 * 60_000, 0.001)])),
        oi: Some(Ok((0..100)
            .map(|i| (i * 60_000, 100.0 + i as f64))
            .collect())),
        excluded: None,
    };
    let output = compute_row_planned(loaded, &query, None, &plan, false).unwrap();
    assert!(
        (output["metrics"]["joint"].as_f64().unwrap() - (0.001 + 1.0 / 198.0)).abs() < 1e-12
    );
    assert!(output["source_captures"]["open_interest"]["capture_hash"].is_string());
    assert!(output["source_captures"]["funding_rate"]["capture_hash"].is_string());
    assert_eq!(output["source_alignment"]["observed_oi_observations"], 100);
    assert_eq!(output["funding_event_alignment"]["aligned_events"], 1);
    assert_eq!(
        output["metric_time_basis"]["joint"],
        "funding_event_time_publication_unverified"
    );
    assert_eq!(
        output["composition_quality"]["joint"]["status"],
        "retrospective_publication_unverified"
    );
}
#[test]
fn t02_t05_t06_public_ratio_divergence_uses_exact_oi_grid_and_null_denominator() {
    let mut query: MarketRequest = serde_json::from_value(json!({
        "action":"screen","instruments":["BTC-USDT-SWAP"],"fields":["returns"],
        "timeframe_ms":60000,"start_ms":0,"end_ms":6000000
    }))
    .unwrap();
    query.composition = Some(Box::new(
        serde_json::from_value(json!({
            "version":"okx.market.composition/v1",
            "steps":[
                {"id":"price_return","op":"returns_series","input":"close"},
                {"id":"oi_return","op":"returns_series","input":"open_interest"},
                {"id":"difference","op":"subtract_series","input":"price_return","other":"oi_return"},
                {"id":"relative","op":"divide_series","input":"price_return","other":"oi_return"},
                {"id":"delayed_relative","op":"lag","input":"relative","period":1}
            ],"result_steps":["difference","relative","delayed_relative"]
        }))
        .unwrap(),
    ));
    let plan = planner::AnalysisPlan::compile_market(&query).unwrap();
    let fixture = |flat_oi: bool| Loaded {
        instrument: "BTC-USDT-SWAP".into(),
        bars: Ok(Arc::new(
            (0..100)
                .map(|i| Bar {
                    time: i * 60_000,
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
        oi: Some(Ok((0..100)
            .map(|i| (i * 60_000, if flat_oi { 10.0 } else { 10.0 + i as f64 }))
            .collect())),
        excluded: None,
    };
    let output = compute_row_planned(fixture(false), &query, None, &plan, true).unwrap();
    let price_return = 199.0 / 198.0 - 1.0;
    let oi_return = 109.0 / 108.0 - 1.0;
    assert!(
        (output["metrics"]["difference"].as_f64().unwrap() - (price_return - oi_return)).abs()
            < 1e-12
    );
    assert!(
        (output["metrics"]["relative"].as_f64().unwrap() - price_return / oi_return).abs()
            < 1e-12
    );
    assert_eq!(output["units"]["difference"], "ratio");
    assert_eq!(output["source_alignment"]["expected_observations"], 100);
    assert_eq!(output["source_alignment"]["aligned_observations"], 100);
    assert_eq!(output["source_alignment"]["paired_return_observations"], 99);
    assert_eq!(
        output["source_alignment"]["first_unaligned_ms"],
        Value::Null
    );
    assert_eq!(
        output["source_alignment"]["oi_publication_time_verified"],
        false
    );
    assert_eq!(
        output["composition_quality"]["relative"]["status"],
        "retrospective_publication_unverified"
    );
    let zero = compute_row_planned(fixture(true), &query, None, &plan, true).unwrap();
    assert!(zero["metrics"]["relative"].is_null());
    assert!(zero["metrics"]["difference"].is_number());
    assert_eq!(
        zero["composition_quality"]["relative"]["status"],
        "unavailable"
    );
    assert_eq!(
        zero["composition_quality"]["relative"]["issues"][0],
        json!({"reason":"DIVISION_BY_ZERO","origin_step":"relative", "origin_event_ms":99 * 60_000})
    );
    assert_eq!(
        zero["composition_quality"]["delayed_relative"]["issues"][0],
        json!({"reason":"DIVISION_BY_ZERO","origin_step":"relative", "origin_event_ms":98 * 60_000})
    );
    assert!(zero["metrics"]["delayed_relative"].is_null());
    let mut interrupted = fixture(false);
    interrupted
        .oi
        .as_mut()
        .unwrap()
        .as_mut()
        .unwrap()
        .remove(50);
    let partial = compute_row_planned(interrupted, &query, None, &plan, true).unwrap();
    assert!(partial["metrics"]["difference"].is_null());
    assert_eq!(partial["source_alignment"]["aligned_observations"], 99);
    assert_eq!(
        partial["source_alignment"]["paired_return_observations"],
        97
    );
    assert_eq!(
        partial["source_alignment"]["first_unaligned_ms"],
        50 * 60_000
    );
    assert_eq!(
        zero["source_captures"]["open_interest"]["publication_time_verified"],
        false
    );
}

#[test]
fn t06_public_rolling_pearson_uses_thirty_exact_return_pairs_without_oi_lookahead() {
    let mut query: MarketRequest = serde_json::from_value(json!({
        "action":"screen","instruments":["BTC-USDT-SWAP"],"fields":["returns"],
        "timeframe_ms":60000,"start_ms":0,"end_ms":6000000,
        "composition":{"version":"okx.market.composition/v1","steps":[
            {"id":"returns","op":"returns_series","input":"close"},
            {"id":"oi_return","op":"returns_series","input":"open_interest"},
            {"id":"relationship","op":"rolling_pearson","input":"returns","other":"oi_return","period":30}
        ],"result_steps":["relationship"]}
    }))
    .unwrap();
    let plan = planner::AnalysisPlan::compile_market(&query).unwrap();
    let fixture = |flat_oi: bool| {
        let (mut price, mut oi) = (100.0, 10.0);
        let mut bars = Vec::new();
        let mut oi_points = Vec::new();
        for i in 0..100 {
            if i > 0 {
                let change = (((i * 7) % 11) as f64 - 5.0) * 0.0005;
                price *= 1.0 + change;
                if !flat_oi {
                    oi *= 1.0 + 2.0 * change;
                }
            }
            bars.push(Bar {
                time: i * 60_000,
                open: price,
                high: price,
                low: price,
                close: price,
                volume: 1.0,
                funding_rate: None,
            });
            oi_points.push((i * 60_000, oi));
        }
        Loaded {
            instrument: "BTC-USDT-SWAP".into(),
            bars: Ok(Arc::new(bars)),
            ticker: None,
            funding: None,
            oi: Some(Ok(oi_points)),
            excluded: None,
        }
    };
    let complete = compute_row_planned(fixture(false), &query, None, &plan, true).unwrap();
    assert!((complete["metrics"]["relationship"].as_f64().unwrap() - 1.0).abs() < 1e-8);
    assert_eq!(complete["units"]["relationship"], "coefficient");
    assert_eq!(
        complete["source_alignment"]["paired_return_observations"],
        99
    );
    assert_eq!(
        complete["composition_quality"]["relationship"]["status"],
        "retrospective_publication_unverified"
    );
    let no_variance = compute_row_planned(fixture(true), &query, None, &plan, true).unwrap();
    assert!(no_variance["metrics"]["relationship"].is_null());
    assert_eq!(
        no_variance["composition_quality"]["relationship"]["issues"][0]["reason"],
        "ZERO_VARIANCE"
    );
    let mut missing = fixture(false);
    missing.oi.as_mut().unwrap().as_mut().unwrap().remove(80);
    let partial = compute_row_planned(missing, &query, None, &plan, true).unwrap();
    assert!(partial["metrics"]["relationship"].is_null());
    assert_eq!(
        partial["source_alignment"]["paired_return_observations"],
        97
    );
    assert_eq!(
        partial["source_alignment"]["first_unaligned_ms"],
        80 * 60_000
    );
    assert_eq!(
        partial["composition_quality"]["relationship"]["issues"][0]["reason"],
        "OI_RANGE_INCOMPLETE_OR_GAPPED"
    );
    // The same graph cannot be used to create a historical causal signal
    // from OI publication times that have not been independently attested.
    query.composition.as_mut().unwrap().steps.push(
        serde_json::from_value(
            json!({"id":"signal","op":"greater_than","input":"relationship","threshold":0.9}),
        )
        .unwrap(),
    );
    query.composition.as_mut().unwrap().result_steps = vec!["signal".into()];
    let causal_plan = planner::AnalysisPlan::compile_market(&query).unwrap();
    let rejected =
        compute_row_planned(fixture(false), &query, None, &causal_plan, true).unwrap();
    assert!(rejected["metrics"]["signal"].is_null());
    assert_eq!(
        rejected["composition_quality"]["signal"]["issues"][0]["reason"],
        "CAUSAL_PUBLICATION_TIME_UNVERIFIED"
    );
}

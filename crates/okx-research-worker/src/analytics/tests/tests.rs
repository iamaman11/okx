use super::*;
#[test]
fn market_capabilities_discloses_supported_operator_names_and_units() {
    let (root, service) = super::artifact_job_fixtures::service();
    let capabilities = service.market_capabilities();
    let operators = capabilities["operators"].as_array().unwrap();
    assert_eq!(operators.len(), 16);
    assert_eq!(
        operators.len(),
        okx_analysis::operators::SUPPORTED_OPERATORS.len()
    );
    let expected = okx_analysis::operators::SUPPORTED_OPERATORS
        .iter()
        .map(|operator| serde_json::to_value(operator).unwrap())
        .collect::<Vec<_>>();
    let names = operators
        .iter()
        .map(|operator| operator["name"].clone())
        .collect::<Vec<_>>();
    assert_eq!(names, expected);
    assert_eq!(capabilities["effects"]["venue_post"], false);
    assert_eq!(capabilities["effects"]["write_finance"], false);
    assert_eq!(capabilities["effects"]["write_governance"], false);
    assert_eq!(capabilities["composition"]["input_sources"],
        serde_json::json!(["open", "high", "low", "close", "volume", "open_interest", "funding_rate"]));
    for source in ["open", "high", "low", "close"] {
        assert_eq!(capabilities["composition"]["source_units"][source], "price");
    }
    assert_eq!(capabilities["composition"]["source_units"]["volume"], "native_volume");
    assert_eq!(capabilities["composition"]["ohlc_policy"],
        "same_instrument_closed_candle_grid_no_forward_fill");
    let volume_change = operators.iter().find(|item| item["name"] == "volume_change").unwrap();
    assert_eq!(volume_change["typed_inputs"][0]["unit"], "native_volume");
    assert_eq!(volume_change["typed_inputs"][0]["source"], "candles");
    let comparator = capabilities["composition"]["operations"]
        .as_array().unwrap().iter()
        .find(|item| item["op"] == "greater_than_series").unwrap();
    assert_eq!(comparator["other_unit"], "same_as_input");
    assert_eq!(comparator["output_unit"], "boolean");
    assert_eq!(comparator["causal"], true);
    let operations = capabilities["composition"]["operations"].as_array().unwrap();
    let stddev = operations.iter().find(|item| item["op"] == "rolling_stddev").unwrap();
    assert_eq!(stddev["output_unit"], "same_as_input");
    assert_eq!(stddev["variance"], "population_inclusive_window");
    assert_eq!(stddev["period_min"], 2);
    assert_eq!(stddev["null_policy"], "strict_window_zero_for_constant");
    let lower = operations.iter().find(|item| item["op"] == "less_than_series").unwrap();
    assert_eq!(lower["other_unit"], "same_as_input");
    assert_eq!(lower["null_policy"], "propagate_unknown");
    let falling = operations.iter().find(|item| item["op"] == "falling_edge").unwrap();
    assert_eq!(falling["input_unit"], "boolean");
    assert_eq!(falling["trigger"], "true_to_false");
    for op in ["and", "or"] {
        let operation = capabilities["composition"]["operations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["op"] == op)
            .unwrap();
        assert_eq!(operation["input_unit"], "boolean");
        assert_eq!(operation["other_unit"], "boolean");
        assert_eq!(operation["null_policy"], "propagate_unknown");
        assert_eq!(operation["causal"], true);
    }
    for op in ["subtract_series", "divide_series"] {
        let operation = capabilities["composition"]["operations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["op"] == op)
            .unwrap();
        assert_eq!(operation["input_unit"], "ratio");
        assert_eq!(operation["other_unit"], "ratio");
        assert_eq!(operation["output_unit"], "ratio");
        assert_eq!(operation["numeric_policy"], "finite_only");
    }
    let pearson = capabilities["composition"]["operations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|operation| operation["op"] == "rolling_pearson")
        .unwrap();
    assert_eq!(pearson["input_unit"], "ratio");
    assert_eq!(pearson["other_unit"], "ratio");
    assert_eq!(pearson["output_unit"], "coefficient");
    assert_eq!(pearson["period_min"], 30);
    assert_eq!(pearson["compute_cost"], "linear_in_series_length");
    assert_eq!(
        capabilities["composition"]["null_diagnostics"]["origin_step"],
        true
    );
    assert!(
        capabilities["composition"]["null_diagnostics"]["reason_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "DIVISION_BY_ZERO")
    );
    assert_eq!(
        capabilities["composition"]["source_alignment"]["publication_time_verified"],
        false
    );
    assert_eq!(
        capabilities["composition"]["source_alignment"]["join_semantics"],
        "exact_closed_bar_timestamp_no_forward_fill"
    );
    assert_eq!(
        capabilities["availability_policy"],
        "actual_data_and_rights_checked_per_request"
    );
    assert_eq!(
        names
            .iter()
            .map(|name| name.as_str().unwrap())
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        16
    );
    for operator in operators {
        assert_eq!(operator["version"], 1);
        assert_eq!(operator["status"], "code_supported");
        assert_eq!(operator["route_status"], "public_market_screen_compare");
        assert!(operator["data_availability"].is_string());
        assert!(operator["time_basis"].is_string());
        assert_eq!(operator["output_shape"], "scalar");
        assert_eq!(
            operator["typed_inputs"].as_array().unwrap().len(),
            operator["inputs"].as_array().unwrap().len()
        );
    }
    let position = operators
        .iter()
        .find(|operator| operator["name"] == "position_size")
        .unwrap();
    assert_eq!(
        position["typed_inputs"][0],
        json!({"name":"budget","source":"request","unit":"quote_currency"})
    );
    assert_eq!(position["time_basis"], "request_parameters");
    let spread = operators
        .iter()
        .find(|operator| operator["name"] == "spread_bps")
        .unwrap();
    assert_eq!(
        spread["data_availability"],
        "live_snapshot_timestamp_unverified"
    );
    let funding = operators
        .iter()
        .find(|operator| operator["name"] == "funding_sum")
        .unwrap();
    assert_eq!(
        funding["data_availability"],
        "historical_publication_time_unverified"
    );
    let pearson = operators
        .iter()
        .find(|operator| operator["name"] == "pearson_correlation")
        .unwrap();
    assert_eq!(pearson["time_basis"], "exact_closed_candle_join");
    assert!(
        operators
            .iter()
            .any(|operator| operator["name"] == "returns" && operator["unit"] == "ratio")
    );
    assert!(
        operators
            .iter()
            .all(|operator| operator["inputs"].is_array())
    );
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn grid_result_includes_selected_train_test_and_equity() {
    let (root, service) = super::artifact_job_fixtures::service();
    let dataset = service
        .artifacts
        .publish_evidence(&json!({"chunks":[]}))
        .unwrap()
        .0;
    let summary = replay::Summary {
        pnl: 1.0,
        pnl_exact: "1".into(),
        fees_exact: "0.1".into(),
        funding_exact: "0".into(),
        max_drawdown: 0.05,
        sharpe: 1.2,
        trades: 0,
        turnover: 2.0,
        fees: 0.1,
        funding: 0.0,
        benchmark: 0.0,
        assumptions: vec!["COUNTERFACTUAL".into()],
    };
    let result = replay::ReplayResult {
        dataset_hash: "dataset-398".into(),
        code_version: "test".into(),
        hash: "result-398".into(),
        summary: summary.clone(),
        equity: (0..100).map(|i| (i, 10000.0 + i as f64)).collect(),
        trades: vec![],
    };
    let grid = json!({"selected":{"fast":3,"slow":8},"train":summary,
            "split_index":70,"selection_warning":"Counterfactual only",
            "variants":[{"parameters":{"fast":3,"slow":8},"train":summary}]});
    let manifest = publish_replay(
        &service.artifacts,
        &result,
        &dataset,
        Some(grid),
        None,
        None,
        None,
        &|| false,
    )
    .unwrap();
    let id = "c".repeat(64);
    service
        .jobs
        .enqueue("f05-proof".into(), id.clone())
        .unwrap();
    service
        .jobs
        .update(&id, jobs::State::Completed, Some(manifest), None)
        .unwrap();
    let get = |view, cursor: Option<String>| ResearchRequest::Result {
        job_id: id.clone(),
        cursor,
        limit: 5,
        view,
    };
    let summary_view = service
        .research(get(ResearchResultView::Summary, None))
        .await
        .unwrap();
    assert_eq!(summary_view["metadata"]["grid"]["selected"]["fast"], 3);
    assert_eq!(summary_view["metadata"]["grid"]["train"]["pnl"], 1.0);
    assert_eq!(summary_view["metadata"]["grid"]["test"]["pnl"], 1.0);
    assert_eq!(summary_view["metadata"]["summary"]["pnl"], 1.0);
    assert_eq!(summary_view["returned_rows"], 0);
    let variants = service
        .research(get(ResearchResultView::Variants, None))
        .await
        .unwrap();
    assert_eq!(variants["total_rows"], 1);
    assert_eq!(variants["rows"][0]["parameters"]["slow"], 8);
    let first = service
        .research(get(ResearchResultView::Equity, None))
        .await
        .unwrap();
    assert_eq!(first["total_rows"], 100);
    assert_eq!(first["returned_rows"], 5);
    assert!(first["next_cursor"].as_str().unwrap().starts_with("r1."));
    let next = service
        .research(get(
            ResearchResultView::Equity,
            first["next_cursor"].as_str().map(str::to_owned),
        ))
        .await
        .unwrap();
    assert_eq!(next["rows"][0][0], 5);
    assert!(
        service
            .research(get(ResearchResultView::Equity, Some("invalid".into())))
            .await
            .is_err()
    );
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

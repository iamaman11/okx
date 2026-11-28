use super::*;

#[test]
fn graph_backtest_capabilities_follow_the_same_typed_catalog_as_market_and_compiler() {
    let capabilities = research_capabilities();
    let contracts = okx_analysis::query_plan::series_operation_contracts();
    let supported = contracts
        .iter()
        .filter(|contract| contract.graph_backtest_supported)
        .map(|contract| contract.op)
        .collect::<Vec<_>>();
    let unavailable = contracts
        .iter()
        .filter(|contract| !contract.graph_backtest_supported)
        .map(|contract| contract.op)
        .collect::<Vec<_>>();
    assert_eq!(
        capabilities["graph_backtest"]["supported_operators"],
        json!(supported)
    );
    assert_eq!(
        capabilities["graph_backtest"]["unavailable_operators"],
        json!(unavailable)
    );
    assert_eq!(supported.len(), 20);
    let graph_sources = okx_analysis::query_plan::series_source_contracts()
        .into_iter()
        .filter(|contract| contract.graph_backtest_supported)
        .map(|contract| contract.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        capabilities["graph_backtest"]["sources"],
        json!(graph_sources)
    );
    assert_eq!(graph_sources, vec!["close"]);
    assert_eq!(
        unavailable,
        vec![okx_research_protocol::strategy_contract::SeriesOp::ForwardReturnLabel]
    );
    assert_eq!(capabilities["graph_backtest"]["trading_authority"], false);
    assert_eq!(
        capabilities["graph_backtest"]["oos"]["split"],
        "train_70_holdout_30"
    );
    assert_eq!(
        capabilities["graph_backtest"]["oos"]["selection"],
        "fixed_graph_no_model_fitting"
    );
    assert_eq!(capabilities["oos"]["single_graph_train_holdout"], true);
    assert_eq!(capabilities["oos"]["single_graph_optimization"], false);
}

#[test]
fn t10_capabilities_separate_memory_permit_from_observed_rss_and_full_slo() {
    let caps = research_capabilities();
    assert_eq!(caps["limits"]["research_memory_total_mib"], 4096);
    assert_eq!(caps["limits"]["max_heavy_concurrent_jobs"], 2);
    assert_eq!(
        caps["resource_proof"]["memory_admission"],
        "shared_estimated_mib_permit_lease_through_terminal_sql_attempt"
    );
    assert_eq!(
        caps["resource_proof"]["cancel_while_waiting"],
        "both_heavy_and_memory_permits_released"
    );
    assert_eq!(
        caps["resource_proof"]["memory_measurement_scope"],
        "whole_process_not_per_job_and_not_hard_enforcement"
    );
    assert_eq!(
        caps["resource_proof"]["mcp_and_finance_under_load"],
        "not_evaluated"
    );
    assert_eq!(caps["ipc_isolation"]["critical_in_flight"], 2);
    assert_eq!(caps["ipc_isolation"]["observation_in_flight"], 4);
    assert_eq!(caps["ipc_isolation"]["work_in_flight"], 32);
    assert_eq!(
        caps["ipc_isolation"]["observation_commands"],
        json!(["explain", "capabilities", "status", "list"])
    );
    assert_eq!(
        caps["ipc_isolation"]["engine_and_finance_slo_under_load"],
        "not_evaluated"
    );
}
#[test]
fn native_capabilities_do_not_advertise_unavailable_historical_mode() {
    let report = research_capabilities();
    assert_eq!(report["supported_data_modes"], json!(["counterfactual"]));
    assert_eq!(
        report["unavailable_data_modes"][0]["reason_code"],
        "HISTORICAL_RULES_AND_FEES_UNAVAILABLE"
    );
    assert_eq!(report["limits"]["max_grid_variants"], 64);
    assert_eq!(report["strategy_ids"].as_array().unwrap().len(), 3);
    assert_eq!(
        report["strategy_definitions"]["close_breakout_v1"]["parameters"]["lookback"]["min"],
        2
    );
    assert_eq!(report["decisions"]["trading_authority"], false);
    assert_eq!(
        report["economics"]["replay_products"],
        json!(["spot", "linear_swap"])
    );
    assert_eq!(
        report["economics"]["counterfactual_oracle"]["cashflow_attribution"]["identity"],
        "gross_minus_fees_minus_signed_funding_equals_net"
    );
    assert_eq!(
        report["economics"]["counterfactual_oracle"]["cashflow_attribution"]["signed_funding"],
        true
    );
    assert_eq!(
        report["economics"]["counterfactual_oracle"]["fx_conversion"],
        false
    );
    assert_eq!(
        report["economics"]["observed_funding"]["settlement_join"],
        "funding_event_at_preceding_closed_bar_end_no_forward_fill"
    );
    assert_eq!(
        report["economics"]["observed_funding"]["publication_time_verified"],
        false
    );
    assert_eq!(
        report["economics"]["observed_funding"]["observation_semantics"],
        "observed_zero_distinct_from_absent_settlement"
    );
    assert_eq!(
        report["economics"]["observed_funding"]["export_evidence"],
        "capture_counts_and_every_replay_bar_projection_verified"
    );
    assert!(report["economics"]["observed_funding"]["expected_events"].is_null());
    assert_eq!(
        report["source_evidence"]["funding_event_capture_when_requested"],
        true
    );
}

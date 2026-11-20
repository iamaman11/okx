use super::*;
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
    assert_eq!(report["economics"]["counterfactual_oracle"]["cashflow_attribution"]["identity"],
        "gross_minus_fees_minus_signed_funding_equals_net");
    assert_eq!(report["economics"]["counterfactual_oracle"]["cashflow_attribution"]["signed_funding"], true);
    assert_eq!(report["economics"]["counterfactual_oracle"]["fx_conversion"], false);
    assert_eq!(
        report["economics"]["observed_funding"]["settlement_join"],
        "funding_event_at_preceding_closed_bar_end_no_forward_fill"
    );
    assert_eq!(
        report["economics"]["observed_funding"]["publication_time_verified"],
        false
    );
    assert!(report["economics"]["observed_funding"]["expected_events"].is_null());
    assert_eq!(
        report["source_evidence"]["funding_event_capture_when_requested"],
        true
    );
}

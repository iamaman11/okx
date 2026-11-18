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
}

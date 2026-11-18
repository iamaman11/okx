//! Properties of the production risk amount parser and canonical wire form.
use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn risk_amount_canonical_round_trip_and_scale_equivalence(
        whole in 0u32..1_000_000, fraction in 0u32..1_000_000,
    ) {
        let raw = format!("{whole}.{fraction:06}");
        let scaled = format!("{raw}00");
        let amount = RiskAmount::parse(&raw).unwrap();
        let canonical = amount.canonical().unwrap();
        prop_assert_eq!(RiskAmount::parse(&scaled).unwrap(), amount.clone());
        prop_assert_eq!(RiskAmount::parse(&canonical).unwrap(), amount);
        prop_assert_eq!(RiskAmount::parse(&canonical).unwrap().canonical().unwrap(), canonical);
    }

    #[test]
    fn risk_amount_rejects_out_of_range_and_leading_zero(
        leading in 1u8..10, digit in 0u8..10,
    ) {
        let oversized = format!("{leading}{}", digit.to_string().repeat(28));
        prop_assert_eq!(RiskAmount::parse(&oversized), Err(MoneyError::NumericRange));
        let noncanonical = format!("0{leading}");
        prop_assert_eq!(RiskAmount::parse(&noncanonical), Err(MoneyError::InvalidSyntax));
    }
}

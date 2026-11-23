//! Bounded properties of the production native reserve sizing boundary.
use super::*;
use proptest::prelude::*;
use rust_decimal::Decimal;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn native_reserved_loss_covers_plan_with_minimal_nine_place_quantum(
        cents in 1i64..1_000_000,
        rate_millionths in 1_000_000i64..2_000_001,
    ) {
        let planned = Decimal::new(cents, 2);
        let rate = Decimal::new(rate_millionths, 6);
        let sized = native_reserved_loss(planned, rate).unwrap();
        let usdt: Decimal = sized.usdt.checked_canonical().unwrap().parse().unwrap();
        let usd: Decimal = sized.usd.checked_canonical().unwrap().parse().unwrap();
        let quantum = Decimal::new(1, 9);
        let product = usdt * rate;

        // These generated values stay within Decimal's exact multiplication
        // range, so these inequalities do not reproduce the sizing algorithm.
        prop_assert!(product >= planned);
        prop_assert!(product - planned < quantum * rate);
        prop_assert_eq!(usdt % quantum, Decimal::ZERO);
        prop_assert_eq!(usd, product);
        prop_assert!(usd >= planned);
    }
}

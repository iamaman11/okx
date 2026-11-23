//! Semantic identity properties through the production TradeRequest API.
use super::*;
use proptest::prelude::*;

fn execute_with_size(size: &str, reordered: bool) -> TradeRequest {
    let raw = if reordered {
        format!(
            r#"{{"expires_in_ms":10000,"protection":{{"stop_price":"900.00"}},"order":{{"max_slippage_bps":5,"type":"market"}},"sizing":{{"max_loss_usd":"{size}"}},"side":"long","instrument":"BTC-USDT-SWAP","strategy_instance":"manual","operation":"open","idempotency_key":"property_request_01","action":"execute"}}"#
        )
    } else {
        format!(
            r#"{{"action":"execute","idempotency_key":"property_request_01","operation":"open","strategy_instance":"manual","instrument":"BTC-USDT-SWAP","side":"long","sizing":{{"max_loss_usd":"{size}"}},"order":{{"type":"market","max_slippage_bps":5}},"protection":{{"stop_price":"900"}},"expires_in_ms":10000}}"#
        )
    };
    serde_json::from_str(&raw).unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn trade_semantic_identity_preserves_scale_and_field_order(
        value in 1u32..1_000_000,
    ) {
        let plain = execute_with_size(&value.to_string(), false);
        let scaled = execute_with_size(&format!("{value}.000"), true);
        let first = plain.canonical_semantic_json("account-a").unwrap();
        let equivalent = scaled.canonical_semantic_json("account-a").unwrap();
        prop_assert_eq!(&first, &equivalent);
        let changed = execute_with_size(&(value + 1).to_string(), false);
        prop_assert_ne!(&first, &changed.canonical_semantic_json("account-a").unwrap());
        prop_assert_ne!(&first, &plain.canonical_semantic_json("account-b").unwrap());
    }
}

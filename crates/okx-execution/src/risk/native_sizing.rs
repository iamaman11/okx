//! Shared sizing for the native producer and original SQL-plan verification.
use crate::{StoreError, StoreResult};
use okx_kernel::risk::money::RiskAmount;
use rust_decimal::{Decimal, RoundingStrategy};

/// USDT keeps the production nine-place upward sizing quantum. USD uses
/// the RiskBook's exact 10^-28 multiplication, rather than a second ceil9.
pub fn native_reserved_loss(
    planned_loss_usd: Decimal,
    rate: Decimal,
) -> StoreResult<(String, String)> {
    if planned_loss_usd <= Decimal::ZERO || rate <= Decimal::ZERO {
        return Err(StoreError::Invalid("NATIVE_RESERVE_LOSS_SOURCE_INVALID"));
    }
    let usdt = planned_loss_usd
        .checked_div(rate)
        .ok_or(StoreError::Invalid("NATIVE_RESERVE_LOSS_OVERFLOW"))?
        .round_dp_with_strategy(9, RoundingStrategy::ToPositiveInfinity);
    let parse = |value: Decimal| {
        RiskAmount::parse(&value.to_string())
            .map_err(|_| StoreError::Invalid("NATIVE_RESERVE_LOSS_RANGE"))
    };
    let usdt = parse(usdt)?;
    let usd = usdt
        .checked_mul_ceil(&parse(rate)?)
        .map_err(|_| StoreError::Invalid("NATIVE_RESERVE_LOSS_OVERFLOW"))?;
    if usd < parse(planned_loss_usd)? {
        return Err(StoreError::Invalid("NATIVE_RESERVE_LOSS_UNDERSTATED"));
    }
    Ok((
        usdt.canonical()
            .map_err(|_| StoreError::Invalid("NATIVE_RESERVE_LOSS_RANGE"))?,
        usd.canonical()
            .map_err(|_| StoreError::Invalid("NATIVE_RESERVE_LOSS_RANGE"))?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn dec(value: &str) -> Decimal {
        value.parse().unwrap()
    }

    #[test]
    fn native_fx_keeps_exact_usd_product_and_covers_plan() {
        let (usdt, usd) = native_reserved_loss(dec("1"), dec("1.123456789")).unwrap();
        let loss = RiskAmount::parse(&usdt).unwrap();
        let expected = loss
            .checked_mul_ceil(&RiskAmount::parse("1.123456789").unwrap())
            .unwrap();
        assert_eq!(RiskAmount::parse(&usd).unwrap(), expected);
        assert!(expected >= RiskAmount::parse("1").unwrap());
        assert_eq!(usdt, "0.890109892");
        assert_eq!(usd, "1.000000001123456788");
    }

    #[test]
    fn invalid_plan_or_fx_cannot_issue_native_sizing() {
        assert!(native_reserved_loss(Decimal::ZERO, dec("1")).is_err());
        assert!(native_reserved_loss(dec("1"), Decimal::ZERO).is_err());
        assert!(native_reserved_loss(dec("-1"), dec("1")).is_err());
    }
}

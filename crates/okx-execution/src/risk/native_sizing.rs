//! Shared sizing for the native producer and original SQL-plan verification.
use crate::{StoreError, StoreResult};
use okx_kernel::risk::money::RiskAmount;
use rust_decimal::{Decimal, RoundingStrategy};

/// Exact reserved losses retain their units until the wire boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsdtLoss(RiskAmount);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsdLoss(RiskAmount);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeReservedLoss {
    pub usdt: UsdtLoss,
    pub usd: UsdLoss,
}

impl UsdtLoss {
    pub fn checked_canonical(&self) -> StoreResult<String> {
        self.0
            .canonical()
            .map_err(|_| StoreError::Invalid("NATIVE_RESERVE_LOSS_RANGE"))
    }
}
impl UsdLoss {
    pub fn checked_canonical(&self) -> StoreResult<String> {
        self.0
            .canonical()
            .map_err(|_| StoreError::Invalid("NATIVE_RESERVE_LOSS_RANGE"))
    }
}

/// The producer cannot pass the USDT factory output as a USD loss:
///
/// ```compile_fail
/// use okx_execution::risk::{native_reserved_loss, UsdLoss};
/// use rust_decimal::Decimal;
/// fn accepts_usd(_: UsdLoss) {}
/// let loss = native_reserved_loss(Decimal::ONE, Decimal::ONE).unwrap();
/// accepts_usd(loss.usdt);
/// ```
///
/// The corresponding USD output is accepted:
///
/// ```
/// use okx_execution::risk::{native_reserved_loss, UsdLoss};
/// use rust_decimal::Decimal;
/// fn accepts_usd(_: UsdLoss) {}
/// let loss = native_reserved_loss(Decimal::ONE, Decimal::ONE).unwrap();
/// accepts_usd(loss.usd);
/// ```
/// USDT keeps the production nine-place upward sizing quantum. USD uses
/// the RiskBook's exact 10^-28 multiplication, rather than a second ceil9.
pub fn native_reserved_loss(
    planned_loss_usd: Decimal,
    rate: Decimal,
) -> StoreResult<NativeReservedLoss> {
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
    // Retain the original factory's range checks before returning either value.
    usdt.canonical()
        .map_err(|_| StoreError::Invalid("NATIVE_RESERVE_LOSS_RANGE"))?;
    usd.canonical()
        .map_err(|_| StoreError::Invalid("NATIVE_RESERVE_LOSS_RANGE"))?;
    Ok(NativeReservedLoss {
        usdt: UsdtLoss(usdt),
        usd: UsdLoss(usd),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn dec(value: &str) -> Decimal {
        value.parse().unwrap()
    }

    #[test]
    fn native_fx_keeps_exact_usd_product_and_covers_plan() {
        let result = native_reserved_loss(dec("1"), dec("1.123456789")).unwrap();
        let usdt = result.usdt.checked_canonical().unwrap();
        let usd = result.usd.checked_canonical().unwrap();
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

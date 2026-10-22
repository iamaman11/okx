use crate::{StoreError, StoreResult};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdmissionEvidence {
    pub now: i64,
    pub deadline: i64,
    pub quote_at: i64,
    pub account_at: i64,
    pub generation: u64,
    pub quote_generation: u64,
    pub account_generation: u64,
    pub rules_generation: u64,
    pub policy_revision: i64,
    pub planned_policy_revision: i64,
    pub risk_increase: bool,
    pub paused: bool,
    pub stopped: bool,
    pub complete: bool,
    pub confirmed_reduce_qty: Decimal,
    pub requested_qty: Decimal,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct FreshnessLimits {
    pub quote_ms: i64,
    pub account_ms: i64,
}
impl Default for FreshnessLimits {
    fn default() -> Self {
        Self {
            quote_ms: 2000,
            account_ms: 5000,
        }
    }
}
pub fn evaluate_admission(e: &AdmissionEvidence, ttl: FreshnessLimits) -> StoreResult<()> {
    if e.now >= e.deadline {
        return Err(StoreError::Invalid("COMMAND_EXPIRED"));
    }
    if e.policy_revision != e.planned_policy_revision {
        return Err(StoreError::Invalid("POLICY_CHANGED"));
    }
    if [e.quote_generation, e.account_generation, e.rules_generation]
        .iter()
        .any(|g| *g != e.generation)
    {
        return Err(StoreError::Invalid("SOURCE_GENERATION_MISMATCH"));
    }
    if e.now < e.quote_at || e.now - e.quote_at > ttl.quote_ms {
        return Err(StoreError::Invalid("WAITING_FRESH_QUOTE"));
    }
    if e.now < e.account_at || e.now - e.account_at > ttl.account_ms || !e.complete {
        return Err(StoreError::Invalid("WAITING_ACCOUNT_EVIDENCE"));
    }
    if e.requested_qty <= Decimal::ZERO {
        return Err(StoreError::Invalid("INVALID_SIZE"));
    }
    if e.risk_increase {
        if e.paused || e.stopped {
            return Err(StoreError::Invalid("RISK_INCREASE_STOPPED"));
        }
    } else if e.requested_qty > e.confirmed_reduce_qty {
        return Err(StoreError::Invalid("CONFIRMED_REDUCTION_EXCEEDED"));
    }
    Ok(())
}
/// Central units are 1e-9 USD and round toward greater reserved cost.
pub fn reserve_units(amount: Decimal) -> StoreResult<i64> {
    if amount < Decimal::ZERO {
        return Err(StoreError::Invalid("NEGATIVE_RESERVE"));
    }
    let scaled = amount
        .checked_mul(Decimal::from(1_000_000_000u64))
        .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?
        .ceil();
    use rust_decimal::prelude::ToPrimitive;
    scaled.to_i64().ok_or(StoreError::Invalid("MONEY_OVERFLOW"))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn close_under_stop() {
        let e = AdmissionEvidence {
            now: 100,
            deadline: 101,
            quote_at: 100,
            account_at: 100,
            generation: 1,
            quote_generation: 1,
            account_generation: 1,
            rules_generation: 1,
            policy_revision: 1,
            planned_policy_revision: 1,
            risk_increase: false,
            paused: true,
            stopped: true,
            complete: true,
            confirmed_reduce_qty: Decimal::ONE,
            requested_qty: Decimal::ONE,
        };
        evaluate_admission(&e, FreshnessLimits::default()).unwrap();
    }
    #[test]
    fn reservation_rounds_up() {
        assert_eq!(reserve_units(Decimal::new(1, 10)).unwrap(), 1);
    }
}

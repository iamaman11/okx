use crate::{StoreError, StoreResult};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NavComponents {
    pub opening_equity: Decimal,
    pub trading_pnl: Decimal,
    pub fees: Decimal,
    pub funding: Decimal,
    pub external_flows: Decimal,
    pub valuation_effects: Decimal,
}
impl NavComponents {
    pub fn equity(&self) -> StoreResult<Decimal> {
        [
            self.trading_pnl,
            self.fees,
            self.funding,
            self.external_flows,
            self.valuation_effects,
        ]
        .into_iter()
        .try_fold(self.opening_equity, |a, b| {
            a.checked_add(b)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))
        })
    }
    pub fn profit(&self) -> StoreResult<Decimal> {
        [self.fees, self.funding, self.valuation_effects]
            .into_iter()
            .try_fold(self.trading_pnl, |a, b| {
                a.checked_add(b)
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))
            })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deposit_is_not_profit() {
        let n = NavComponents {
            opening_equity: Decimal::from(100),
            trading_pnl: Decimal::from(5),
            fees: Decimal::from(-1),
            funding: Decimal::ZERO,
            external_flows: Decimal::from(50),
            valuation_effects: Decimal::ZERO,
        };
        assert_eq!(n.equity().unwrap(), Decimal::from(154));
        assert_eq!(n.profit().unwrap(), Decimal::from(4));
    }
}

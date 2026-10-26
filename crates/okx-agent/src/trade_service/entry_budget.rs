//! Exact fee-inclusive linear entry sizing. Pure arithmetic shared by manual, strategy and preview.
use super::*;
use rust_decimal::RoundingStrategy;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct EntryQuote {
    pub(super) quantity: Decimal,
    pub(super) notional_usdt: Decimal,
    pub(super) fee_usdt: Decimal,
    pub(super) total_usdt: Decimal,
    pub(super) total_usd: Decimal,
}

pub(super) fn quote(
    qty: Decimal,
    ct: Decimal,
    price: Decimal,
    fee_rate: Decimal,
    fx: Decimal,
) -> Result<EntryQuote, String> {
    if qty <= Decimal::ZERO
        || ct <= Decimal::ZERO
        || price <= Decimal::ZERO
        || fee_rate < Decimal::ZERO
        || fx <= Decimal::ZERO
    {
        return Err("ENTRY_SOURCE_INVALID".into());
    }
    let notional_usdt = qty
        .checked_mul(ct)
        .and_then(|v| v.checked_mul(price))
        .ok_or("MONEY_OVERFLOW")?;
    let fee_usdt = notional_usdt
        .checked_mul(fee_rate)
        .ok_or("MONEY_OVERFLOW")?
        .round_dp_with_strategy(6, RoundingStrategy::ToPositiveInfinity);
    let total_usdt = notional_usdt
        .checked_add(fee_usdt)
        .ok_or("MONEY_OVERFLOW")?;
    let total_usd = total_usdt
        .checked_mul(fx)
        .ok_or("MONEY_OVERFLOW")?
        .round_dp_with_strategy(9, RoundingStrategy::ToPositiveInfinity);
    Ok(EntryQuote {
        quantity: qty,
        notional_usdt,
        fee_usdt,
        total_usdt,
        total_usd,
    })
}


/// The Real account policy owns the required ceiling. A command may only tighten it.
/// No cap is inferred from the independent campaign LOSS limit or USDT notional.
pub(super) fn enforced_entry_ceiling(
    increase: bool,
    real: bool,
    account_cap: Option<Decimal>,
    request_cap: Option<Decimal>,
) -> Result<Option<Decimal>, String> {
    if !increase {
        return Ok(None);
    }
    if real && account_cap.is_none() {
        return Err("ACCOUNT_ENTRY_CAP_UNCONFIGURED".into());
    }
    if account_cap.is_some_and(|cap| cap <= Decimal::ZERO)
        || request_cap.is_some_and(|cap| cap <= Decimal::ZERO)
    {
        return Err("ENTRY_BUDGET_INVALID".into());
    }
    Ok(match (account_cap, request_cap) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) | (None, Some(a)) => Some(a),
        (None, None) => None,
    })
}
#[allow(clippy::too_many_arguments)] // One bounded, pure pricing calculation; no owner or IO.
pub(super) fn size_order(
    sizing: &Sizing,
    increase: bool,
    lot: Decimal,
    minimum: Decimal,
    ct: Decimal,
    price: Decimal,
    per_contract_loss: Decimal,
    fx: Decimal,
    fee_rate: Decimal,
    entry_ceiling_usd: Option<Decimal>,
) -> Result<EntryQuote, String> {
    if lot <= Decimal::ZERO
        || minimum <= Decimal::ZERO
        || ct <= Decimal::ZERO
        || price <= Decimal::ZERO
        || fx <= Decimal::ZERO
        || fee_rate < Decimal::ZERO
    {
        return Err("ENTRY_SOURCE_INVALID".into());
    }
    let quantity = match sizing {
        Sizing::Contracts { contracts } => dec(contracts)?,
        Sizing::MaxLossUsd { max_loss_usd } => {
            if !increase || per_contract_loss <= Decimal::ZERO {
                return Err("LOSS_SIZING_REQUIRES_PROTECTED_INCREASE".into());
            }
            dec(max_loss_usd)? / fx / per_contract_loss
        }
        Sizing::EntryBudgetUsd { entry_budget_usd } => {
            if !increase {
                return Err("ENTRY_BUDGET_REQUIRES_INCREASE".into());
            }
            let divisor = ct
                .checked_mul(price)
                .and_then(|v| v.checked_mul(Decimal::ONE + fee_rate))
                .and_then(|v| v.checked_mul(fx))
                .ok_or("MONEY_OVERFLOW")?;
            dec(entry_budget_usd)? / divisor
        }
    };
    let mut qty = if matches!(sizing, Sizing::Contracts { .. }) {
        if (quantity / lot).fract() != Decimal::ZERO {
            return Err("QUANTITY_NOT_ON_VENUE_LOT".into());
        }
        quantity
    } else {
        (quantity / lot).floor() * lot
    };
    if let Some(cap) = entry_ceiling_usd.filter(|_| increase) {
        if cap <= Decimal::ZERO {
            return Err("ENTRY_BUDGET_INVALID".into());
        }
        let divisor = ct
            .checked_mul(price)
            .and_then(|v| v.checked_mul(Decimal::ONE + fee_rate))
            .and_then(|v| v.checked_mul(fx))
            .ok_or("MONEY_OVERFLOW")?;
        let cap_qty = (cap / divisor / lot).floor() * lot;
        if matches!(sizing, Sizing::Contracts { .. }) && qty > cap_qty {
            return Err("ENTRY_BUDGET_EXCEEDED".into());
        }
        qty = qty.min(cap_qty);
    }
    if qty < minimum || qty <= Decimal::ZERO {
        return Err(
            if matches!(sizing, Sizing::EntryBudgetUsd { .. }) || entry_ceiling_usd.is_some() {
                "ENTRY_BUDGET_BELOW_VENUE_MINIMUM"
            } else {
                "MINIMUM_SIZE_EXCEEDS_BUDGET"
            }
            .into(),
        );
    }
    let mut value = quote(
        qty,
        ct,
        price,
        if increase { fee_rate } else { Decimal::ZERO },
        fx,
    )?;
    if increase {
        if let Some(cap) = entry_ceiling_usd {
            while value.total_usd > cap && qty >= minimum + lot {
                qty -= lot;
                value = quote(qty, ct, price, fee_rate, fx)?;
            }
            if value.total_usd > cap {
                return Err("ENTRY_BUDGET_BELOW_VENUE_MINIMUM".into());
            }
        }
        if let Sizing::EntryBudgetUsd { entry_budget_usd } = sizing {
            let cap = dec(entry_budget_usd)?;
            while value.total_usd > cap && qty >= minimum + lot {
                qty -= lot;
                value = quote(qty, ct, price, fee_rate, fx)?;
            }
            if value.total_usd > cap {
                return Err("ENTRY_BUDGET_BELOW_VENUE_MINIMUM".into());
            }
        }
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn d(s: &str) -> Decimal {
        dec(s).unwrap()
    }
    #[test]
    fn policy_ceiling_is_mandatory_for_real_and_always_stricter() {
        assert_eq!(
            enforced_entry_ceiling(true, true, None, Some(d("10"))).unwrap_err(),
            "ACCOUNT_ENTRY_CAP_UNCONFIGURED"
        );
        assert_eq!(
            enforced_entry_ceiling(true, true, Some(d("10")), None).unwrap(),
            Some(d("10"))
        );
        assert_eq!(
            enforced_entry_ceiling(true, true, Some(d("10")), Some(d("8"))).unwrap(),
            Some(d("8"))
        );
        assert_eq!(
            enforced_entry_ceiling(true, true, Some(d("8")), Some(d("10"))).unwrap(),
            Some(d("8"))
        );
        assert_eq!(enforced_entry_ceiling(false, true, None, None).unwrap(), None);
        assert_eq!(
            enforced_entry_ceiling(true, false, None, Some(d("10"))).unwrap(),
            Some(d("10"))
        );
    }
    #[test]
    fn k6_v01_fee_inclusive_max_entry_and_minimum() {
        let size = Sizing::EntryBudgetUsd {
            entry_budget_usd: "10".into(),
        };
        let v = size_order(
            &size,
            true,
            d("0.01"),
            d("0.01"),
            d("1"),
            d("101"),
            d("1"),
            d("1"),
            d("0.0005"),
            Some(d("10")),
        )
        .unwrap();
        assert_eq!(v.quantity, d("0.09"));
        assert_eq!(v.notional_usdt, d("9.09"));
        assert_eq!(v.fee_usdt, d("0.004545"));
        assert_eq!(v.total_usd, d("9.094545"));
        assert_eq!(
            quote(d("0.10"), d("1"), d("101"), d("0.0005"), d("1"))
                .unwrap()
                .total_usd,
            d("10.10505")
        );
        assert_eq!(
            size_order(
                &size,
                true,
                d("0.01"),
                d("0.10"),
                d("1"),
                d("101"),
                d("1"),
                d("1"),
                d("0.0005"),
                Some(d("10"))
            )
            .unwrap_err(),
            "ENTRY_BUDGET_BELOW_VENUE_MINIMUM"
        );
    }
    #[test]
    fn cap_is_independent_of_modeled_loss_and_exact_qty_rejects_resize() {
        let loss = Sizing::MaxLossUsd {
            max_loss_usd: "1".into(),
        };
        let capped = size_order(
            &loss,
            true,
            d("0.01"),
            d("0.01"),
            d("0.01"),
            d("80000"),
            d("10"),
            d("1"),
            d("0.0005"),
            Some(d("10")),
        )
        .unwrap();
        assert_eq!(capped.quantity, d("0.01"));
        assert!(capped.total_usd <= d("10"));
        assert!(
            size_order(
                &Sizing::Contracts {
                    contracts: "0.02".into()
                },
                true,
                d("0.01"),
                d("0.01"),
                d("0.01"),
                d("80000"),
                d("10"),
                d("1"),
                d("0.0005"),
                Some(d("10"))
            )
            .is_err()
        );
        assert_eq!(
            size_order(
                &Sizing::Contracts {
                    contracts: "0.011".into()
                },
                true,
                d("0.01"),
                d("0.01"),
                d("0.01"),
                d("80000"),
                d("10"),
                d("1"),
                d("0.0005"),
                None
            )
            .unwrap_err(),
            "QUANTITY_NOT_ON_VENUE_LOT"
        );
    }
}

//! Cumulative entry budget transition; no SQL, transport or system clocks.
//! The SQL owner supplies current values and commits the returned changes atomically.
use super::money::{MoneyError, RiskAmount};

pub struct EntrySettlementInput<'a> {
    pub released: bool,
    pub upper: &'a RiskAmount,
    pub previous_cumulative: &'a RiskAmount,
    pub cumulative: &'a RiskAmount,
    pub previous_reserved: &'a RiskAmount,
    pub previous_overage: &'a RiskAmount,
    pub consumed: &'a RiskAmount,
    pub reserved: &'a RiskAmount,
    pub limit: &'a RiskAmount,
    pub breach: &'a RiskAmount,
}

#[derive(Debug, PartialEq, Eq)]
pub struct EntrySettlementChange {
    pub remaining_after: RiskAmount,
    pub consumed_after: RiskAmount,
    pub reserved_after: RiskAmount,
    pub current_over: RiskAmount,
    pub breach_after: RiskAmount,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum EntrySettlementError {
    #[error("ENTRY_RESERVED_PRIMARY_MISMATCH")]
    ReservedMismatch,
    #[error("{0}")]
    Money(#[from] MoneyError),
}

/// Amount exceeding an entry bound, using the canonical checked money semantics.
pub fn entry_overage(value: &RiskAmount, limit: &RiskAmount) -> Result<RiskAmount, MoneyError> {
    if value > limit {
        value.checked_sub(limit)
    } else {
        Ok(RiskAmount::zero())
    }
}
fn aggregate_overage(
    consumed: &RiskAmount,
    reserved: &RiskAmount,
    limit: &RiskAmount,
) -> Result<RiskAmount, MoneyError> {
    if RiskAmount::sum_exceeds(&[consumed, reserved], limit) {
        consumed.checked_add(reserved)?.checked_sub(limit)
    } else {
        Ok(RiskAmount::zero())
    }
}

/// Caller verifies source identity/revision and applies this result in the same transaction.
pub fn calculate_entry_settlement(
    input: &EntrySettlementInput<'_>,
) -> Result<EntrySettlementChange, EntrySettlementError> {
    let expected_remaining = if input.released {
        RiskAmount::zero()
    } else if input.upper > input.previous_cumulative {
        input.upper.checked_sub(input.previous_cumulative)?
    } else {
        RiskAmount::zero()
    };
    if &expected_remaining != input.previous_reserved {
        return Err(EntrySettlementError::ReservedMismatch);
    }
    let delta = input.cumulative.checked_sub(input.previous_cumulative)?;
    let remaining_after = if input.released {
        RiskAmount::zero()
    } else if input.upper > input.cumulative {
        input.upper.checked_sub(input.cumulative)?
    } else {
        RiskAmount::zero()
    };
    let reserve_debit = input.previous_reserved.checked_sub(&remaining_after)?;
    let consumed_after = input.consumed.checked_add(&delta)?;
    let reserved_after = input.reserved.checked_sub(&reserve_debit)?;
    let current_over = entry_overage(input.cumulative, input.upper)?;
    let individual_delta = current_over.checked_sub(input.previous_overage)?;
    let prior_aggregate = aggregate_overage(input.consumed, input.reserved, input.limit)?;
    let current_aggregate = aggregate_overage(&consumed_after, &reserved_after, input.limit)?;
    let aggregate_delta = if current_aggregate > prior_aggregate {
        current_aggregate.checked_sub(&prior_aggregate)?
    } else {
        RiskAmount::zero()
    };
    let breach_delta = if individual_delta > aggregate_delta {
        individual_delta
    } else {
        aggregate_delta
    };
    let breach_after = input.breach.checked_add(&breach_delta)?;

    Ok(EntrySettlementChange {
        remaining_after,
        consumed_after,
        reserved_after,
        current_over,
        breach_after,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn a(s: &str) -> RiskAmount {
        RiskAmount::parse(s).unwrap()
    }
    #[test]
    fn risk_book_contract_settlement_independent_golden_transitions() {
        // released, upper, before, cumulative, old reserve, old overage,
        // consumed, reserved, limit, breach -> remaining, consumed, reserved, overage, breach
        for (released, values, expected) in [
            (
                false,
                ["8", "0", "3", "8", "0", "0", "8", "10", "0"],
                ["5", "3", "5", "0", "0"],
            ),
            (
                false,
                ["8", "3", "9", "5", "0", "3", "5", "10", "0"],
                ["0", "9", "0", "1", "1"],
            ),
            (
                true,
                ["8", "9", "10", "0", "1", "9", "0", "10", "1"],
                ["0", "10", "0", "2", "2"],
            ),
            (
                true,
                ["8", "3", "4", "0", "0", "5", "2", "10", "0"],
                ["0", "6", "2", "0", "0"],
            ),
        ] {
            let v = values.map(a);
            let input = EntrySettlementInput {
                released,
                upper: &v[0],
                previous_cumulative: &v[1],
                cumulative: &v[2],
                previous_reserved: &v[3],
                previous_overage: &v[4],
                consumed: &v[5],
                reserved: &v[6],
                limit: &v[7],
                breach: &v[8],
            };
            let out = calculate_entry_settlement(&input).unwrap();
            assert_eq!(
                [
                    out.remaining_after,
                    out.consumed_after,
                    out.reserved_after,
                    out.current_over,
                    out.breach_after
                ]
                .map(|v| v.canonical().unwrap()),
                expected
            );
        }
    }
    #[test]
    fn risk_book_contract_settlement_rejects_corrupt_reserve_and_regression() {
        let zero = a("0");
        let one = a("1");
        let two = a("2");
        let mut input = EntrySettlementInput {
            released: false,
            upper: &two,
            previous_cumulative: &one,
            cumulative: &two,
            previous_reserved: &zero,
            previous_overage: &zero,
            consumed: &one,
            reserved: &one,
            limit: &two,
            breach: &zero,
        };
        assert_eq!(
            calculate_entry_settlement(&input),
            Err(EntrySettlementError::ReservedMismatch)
        );
        input.previous_reserved = &one;
        input.cumulative = &zero;
        assert_eq!(
            calculate_entry_settlement(&input),
            Err(EntrySettlementError::Money(MoneyError::Negative))
        );
    }
}

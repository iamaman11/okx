use crate::{StoreError, StoreResult};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionLot {
    pub contract_value: Decimal,
    pub inverse: bool,
    pub lot_id: String,
    pub instance_id: Option<String>,
    pub instrument_id: String,
    pub signed_qty: Decimal,
    pub unit_cost: Decimal,
    pub opened_at: i64,
}
pub fn position_pnl(
    quantity: Decimal,
    entry: Decimal,
    exit: Decimal,
    contract_value: Decimal,
    inverse: bool,
    direction: Decimal,
) -> StoreResult<Decimal> {
    if entry <= Decimal::ZERO || exit <= Decimal::ZERO || contract_value <= Decimal::ZERO {
        return Err(StoreError::Invalid("INVALID_CONTRACT_MECHANICS"));
    }
    let delta = if inverse {
        Decimal::ONE.checked_div(entry).and_then(|e| {
            Decimal::ONE
                .checked_div(exit)
                .and_then(|x| e.checked_sub(x))
        })
    } else {
        exit.checked_sub(entry)
    }
    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
    quantity
        .checked_mul(contract_value)
        .and_then(|q| q.checked_mul(delta))
        .and_then(|v| v.checked_mul(direction))
        .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LotReduction {
    pub lot_id: String,
    pub quantity: Decimal,
    pub realized_pnl: Decimal,
}
/// A reduction is scoped to the named instance. It cannot consume another owner's lot.
pub fn reduce_fifo(
    lots: &mut [PositionLot],
    instance: &str,
    instrument: &str,
    quantity: Decimal,
    exit_price: Decimal,
) -> StoreResult<Vec<LotReduction>> {
    if quantity <= Decimal::ZERO || exit_price <= Decimal::ZERO {
        return Err(StoreError::Invalid("INVALID_REDUCTION"));
    }
    let mut indices: Vec<usize> = lots
        .iter()
        .enumerate()
        .filter(|(_, l)| {
            l.instance_id.as_deref() == Some(instance)
                && l.instrument_id == instrument
                && l.signed_qty != Decimal::ZERO
        })
        .map(|(i, _)| i)
        .collect();
    indices.sort_by(|a, b| {
        (lots[*a].opened_at, &lots[*a].lot_id).cmp(&(lots[*b].opened_at, &lots[*b].lot_id))
    });
    let available = indices.iter().try_fold(Decimal::ZERO, |a, i| {
        a.checked_add(lots[*i].signed_qty.abs())
            .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))
    })?;
    if quantity > available {
        return Err(StoreError::Invalid("INSTANCE_POSITION_INSUFFICIENT"));
    }
    let mut remaining = quantity;
    let mut result = Vec::new();
    let mut updates = Vec::new();
    for i in indices {
        if remaining == Decimal::ZERO {
            break;
        }
        let l = &lots[i];
        let qty = remaining.min(l.signed_qty.abs());
        let direction = if l.signed_qty > Decimal::ZERO {
            Decimal::ONE
        } else {
            -Decimal::ONE
        };
        let pnl = position_pnl(
            qty,
            l.unit_cost,
            exit_price,
            l.contract_value,
            l.inverse,
            direction,
        )?;
        updates.push((
            i,
            l.signed_qty
                .checked_sub(qty * direction)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
        ));
        remaining -= qty;
        result.push(LotReduction {
            lot_id: l.lot_id.clone(),
            quantity: qty,
            realized_pnl: pnl,
        });
    }
    for (i, q) in updates {
        lots[i].signed_qty = q;
    }
    Ok(result)
}
/// Signed exposure allocation preserves the account amount; rounding is explicit.
pub fn allocate_funding(
    amount: Decimal,
    exposures: &[(String, Decimal)],
    scale: u32,
) -> StoreResult<Vec<(String, Decimal)>> {
    if scale > 28 {
        return Err(StoreError::Invalid("UNSUPPORTED_PRECISION"));
    }
    let total = exposures.iter().try_fold(Decimal::ZERO, |a, (_, q)| {
        a.checked_add(*q)
            .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))
    })?;
    if total == Decimal::ZERO {
        return Ok(vec![("unattributed".into(), amount)]);
    }
    let mut result = Vec::new();
    let mut allocated = Decimal::ZERO;
    for (id, q) in exposures {
        let a = amount
            .checked_mul(*q)
            .and_then(|v| v.checked_div(total))
            .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?
            .round_dp_with_strategy(scale, rust_decimal::RoundingStrategy::ToZero);
        allocated = allocated
            .checked_add(a)
            .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
        result.push((id.clone(), a));
    }
    result.push((
        "rounding_residual".into(),
        amount
            .checked_sub(allocated)
            .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
    ));
    Ok(result)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn two_instances_same_instrument_lot_attribution() {
        let mut lots = vec![
            PositionLot {
                contract_value: Decimal::ONE,
                inverse: false,
                lot_id: "b".into(),
                instance_id: Some("b".into()),
                instrument_id: "BTC".into(),
                signed_qty: Decimal::ONE,
                unit_cost: Decimal::from(10),
                opened_at: 1,
            },
            PositionLot {
                contract_value: Decimal::ONE,
                inverse: false,
                lot_id: "a".into(),
                instance_id: Some("a".into()),
                instrument_id: "BTC".into(),
                signed_qty: Decimal::from(2),
                unit_cost: Decimal::from(11),
                opened_at: 2,
            },
        ];
        let r = reduce_fifo(&mut lots, "a", "BTC", Decimal::ONE, Decimal::from(12)).unwrap();
        assert_eq!(r[0].lot_id, "a");
        assert_eq!(lots[0].signed_qty, Decimal::ONE);
        assert_eq!(r[0].realized_pnl, Decimal::ONE);
    }
    #[test]
    fn funding_allocation_sums_to_account() {
        let a = Decimal::ONE;
        let v = allocate_funding(
            a,
            &[("a".into(), Decimal::ONE), ("b".into(), Decimal::from(2))],
            2,
        )
        .unwrap();
        assert_eq!(v.iter().map(|(_, v)| *v).sum::<Decimal>(), a);
        assert_eq!(v.last().unwrap().0, "rounding_residual");
    }
}

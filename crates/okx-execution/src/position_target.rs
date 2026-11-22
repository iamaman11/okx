//! Pure target-position reconciliation for strategy-owned lots.
//!
//! Research may express an intention, but only Engine can decide whether an
//! order is necessary. This planner has no network, storage or order authority:
//! its output must still pass durable decision claiming, governance, risk
//! admission and execution idempotency before any venue mutation.

use crate::account_store::StoredLot;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetPosition {
    /// No new decision. In particular, this must never close a position.
    Hold,
    /// Intentionally remove the strategy's existing exposure.
    Flat,
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExposureSide {
    Long,
    Short,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetPlan {
    /// Target already satisfied, or the signal explicitly requested Hold.
    NoChange,
    /// No matching strategy lots exist; admission is still required to open.
    Open(ExposureSide),
    /// Close these unique existing intent references before attempting a flip.
    CloseExisting(Vec<String>),
    /// An order is already in flight; a fresh state read is required afterward.
    AwaitSettlement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetPlanError {
    InvalidScope,
    InvalidLot(String),
    MissingCloseReference(String),
}

/// Plan at most one reconciliation phase for one strategy and instrument.
///
/// `lots` is a snapshot, not an authoritative reservation. `in_flight` must
/// include outstanding opens *and* closes for this strategy/instrument. Callers
/// must re-read persisted state after each settlement before planning again.
pub fn plan_strategy_target(
    target: TargetPosition,
    instance_id: &str,
    instrument_id: &str,
    lots: &[StoredLot],
    in_flight: bool,
) -> Result<TargetPlan, TargetPlanError> {
    if instance_id.is_empty() || instrument_id.is_empty() {
        return Err(TargetPlanError::InvalidScope);
    }
    if target == TargetPosition::Hold {
        return Ok(TargetPlan::NoChange);
    }
    if in_flight {
        return Ok(TargetPlan::AwaitSettlement);
    }

    let mut has_matching_side = false;
    let mut references_to_close = BTreeSet::new();
    for lot in lots.iter().filter(|lot| {
        lot.instance_id.as_deref() == Some(instance_id) && lot.instrument_id == instrument_id
    }) {
        if lot.lot_id.is_empty() || lot.remaining_qty <= rust_decimal::Decimal::ZERO {
            return Err(TargetPlanError::InvalidLot(lot.lot_id.clone()));
        }
        let side = match lot.side.as_str() {
            "long" => ExposureSide::Long,
            "short" => ExposureSide::Short,
            _ => return Err(TargetPlanError::InvalidLot(lot.lot_id.clone())),
        };
        let matches_target = matches!(
            (target, side),
            (TargetPosition::Long, ExposureSide::Long)
                | (TargetPosition::Short, ExposureSide::Short)
        );
        if matches_target {
            has_matching_side = true;
        } else {
            let reference = lot
                .intent_id
                .as_deref()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| TargetPlanError::MissingCloseReference(lot.lot_id.clone()))?;
            references_to_close.insert(reference.to_owned());
        }
    }

    if !references_to_close.is_empty() {
        return Ok(TargetPlan::CloseExisting(
            references_to_close.into_iter().collect(),
        ));
    }
    if has_matching_side || target == TargetPosition::Flat {
        return Ok(TargetPlan::NoChange);
    }
    Ok(TargetPlan::Open(match target {
        TargetPosition::Long => ExposureSide::Long,
        TargetPosition::Short => ExposureSide::Short,
        TargetPosition::Hold | TargetPosition::Flat => unreachable!(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn lot(id: &str, owner: &str, instrument: &str, side: &str, intent: Option<&str>) -> StoredLot {
        StoredLot {
            lot_id: id.into(),
            instance_id: Some(owner.into()),
            intent_id: intent.map(str::to_owned),
            fill_id: Some(format!("fill-{id}")),
            instrument_id: instrument.into(),
            side: side.into(),
            remaining_qty: Decimal::ONE,
            cost: Decimal::ONE,
            opened_at: 1,
        }
    }

    fn plan(
        target: TargetPosition,
        lots: &[StoredLot],
        pending: bool,
    ) -> Result<TargetPlan, TargetPlanError> {
        plan_strategy_target(target, "strategy-a", "BTC-USDT-SWAP", lots, pending)
    }

    #[test]
    fn hold_is_not_flat_even_when_position_exists() {
        let lots = [lot(
            "a",
            "strategy-a",
            "BTC-USDT-SWAP",
            "long",
            Some("entry-a"),
        )];
        assert_eq!(
            plan(TargetPosition::Hold, &lots, false),
            Ok(TargetPlan::NoChange)
        );
        assert_eq!(
            plan(TargetPosition::Flat, &lots, false),
            Ok(TargetPlan::CloseExisting(vec!["entry-a".into()]))
        );
    }

    #[test]
    fn flat_without_exposure_is_no_change_not_open() {
        assert_eq!(
            plan(TargetPosition::Flat, &[], false),
            Ok(TargetPlan::NoChange)
        );
    }

    #[test]
    fn open_only_with_no_existing_lots() {
        assert_eq!(
            plan(TargetPosition::Long, &[], false),
            Ok(TargetPlan::Open(ExposureSide::Long))
        );
        assert_eq!(
            plan(TargetPosition::Short, &[], false),
            Ok(TargetPlan::Open(ExposureSide::Short))
        );
    }

    #[test]
    fn repeating_long_does_not_add_another_order() {
        let lots = [lot(
            "a",
            "strategy-a",
            "BTC-USDT-SWAP",
            "long",
            Some("entry-a"),
        )];
        assert_eq!(
            plan(TargetPosition::Long, &lots, false),
            Ok(TargetPlan::NoChange)
        );
    }

    #[test]
    fn opposite_side_is_closed_before_a_new_open() {
        let lots = [lot(
            "a",
            "strategy-a",
            "BTC-USDT-SWAP",
            "short",
            Some("entry-a"),
        )];
        assert_eq!(
            plan(TargetPosition::Long, &lots, false),
            Ok(TargetPlan::CloseExisting(vec!["entry-a".into()]))
        );
    }

    #[test]
    fn mixed_exposure_closes_opposite_side_first() {
        let lots = [
            lot("a", "strategy-a", "BTC-USDT-SWAP", "long", Some("entry-a")),
            lot("b", "strategy-a", "BTC-USDT-SWAP", "short", Some("entry-b")),
        ];
        assert_eq!(
            plan(TargetPosition::Long, &lots, false),
            Ok(TargetPlan::CloseExisting(vec!["entry-b".into()]))
        );
    }

    #[test]
    fn deduplicates_close_references_in_stable_order() {
        let lots = [
            lot("z", "strategy-a", "BTC-USDT-SWAP", "short", Some("entry-z")),
            lot("a", "strategy-a", "BTC-USDT-SWAP", "long", Some("entry-a")),
            lot(
                "z2",
                "strategy-a",
                "BTC-USDT-SWAP",
                "short",
                Some("entry-z"),
            ),
        ];
        assert_eq!(
            plan(TargetPosition::Flat, &lots, false),
            Ok(TargetPlan::CloseExisting(vec![
                "entry-a".into(),
                "entry-z".into()
            ]))
        );
    }

    #[test]
    fn only_exposure_owned_by_this_strategy_and_instrument_is_considered() {
        let lots = [
            lot("a", "strategy-b", "BTC-USDT-SWAP", "short", Some("foreign")),
            lot(
                "b",
                "strategy-a",
                "ETH-USDT-SWAP",
                "short",
                Some("different"),
            ),
        ];
        assert_eq!(
            plan(TargetPosition::Long, &lots, false),
            Ok(TargetPlan::Open(ExposureSide::Long))
        );
    }

    #[test]
    fn in_flight_order_defers_reconciliation() {
        assert_eq!(
            plan(TargetPosition::Long, &[], true),
            Ok(TargetPlan::AwaitSettlement)
        );
        assert_eq!(
            plan(TargetPosition::Flat, &[], true),
            Ok(TargetPlan::AwaitSettlement)
        );
        assert_eq!(
            plan(TargetPosition::Hold, &[], true),
            Ok(TargetPlan::NoChange)
        );
    }

    #[test]
    fn ambiguous_or_corrupt_lots_fail_closed() {
        let missing = [lot("a", "strategy-a", "BTC-USDT-SWAP", "short", None)];
        assert_eq!(
            plan(TargetPosition::Flat, &missing, false),
            Err(TargetPlanError::MissingCloseReference("a".into()))
        );
        let bad_side = [lot(
            "b",
            "strategy-a",
            "BTC-USDT-SWAP",
            "unknown",
            Some("entry-b"),
        )];
        assert_eq!(
            plan(TargetPosition::Long, &bad_side, false),
            Err(TargetPlanError::InvalidLot("b".into()))
        );
        let mut bad_quantity = lot("c", "strategy-a", "BTC-USDT-SWAP", "long", Some("entry-c"));
        bad_quantity.remaining_qty = Decimal::ZERO;
        assert_eq!(
            plan(TargetPosition::Long, &[bad_quantity], false),
            Err(TargetPlanError::InvalidLot("c".into()))
        );
    }

    #[test]
    fn scope_must_be_explicit() {
        assert_eq!(
            plan_strategy_target(TargetPosition::Long, "", "BTC-USDT-SWAP", &[], false),
            Err(TargetPlanError::InvalidScope)
        );
    }
}

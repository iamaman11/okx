//! Shared reconciliation evidence checks; command implementations have explicit owners below.
use super::*;
mod cancel;
mod close;
mod replace;
mod status;

// Source-backed protective policy has a hard wall-clock SLO independent of
// fee/fill capture. A timeout can cancel the I/O future after a durable BEGIN:
// this is an UNKNOWN operation, never permission to repeat the signed POST.
async fn bounded_protection_before_finance<F>(
    observed_at: Instant,
    bound: Duration,
    action: F,
) -> Value
where
    F: std::future::Future<Output = Result<Value, String>>,
{
    let until = observed_at + bound;
    let outcome = tokio::time::timeout_at(until.into(), action).await;
    let elapsed_ms = observed_at.elapsed().as_millis() as u64;
    match outcome {
        Ok(Ok(action)) => json!({
            "state":"attempted","phase":"before_financial_capture",
            "elapsed_ms":elapsed_ms,"action":action,"retry_create":false
        }),
        Ok(Err(reason)) => json!({
            "state":"blocked","phase":"before_financial_capture",
            "elapsed_ms":elapsed_ms,"reason_code":reason,
            "next_action":"trade.status","retry_create":false
        }),
        Err(_) => json!({
            "state":"blocked","phase":"before_financial_capture",
            "elapsed_ms":elapsed_ms,"reason_code":"EMERGENCY_ACTION_DEADLINE_OR_OUTCOME_UNKNOWN",
            "next_action":"trade.status","retry_create":false
        }),
    }
}

// An authenticated later `live` algo GET cannot silently undo a durable
// protection incident: the same algoId may be stale, reappear after an ACK,
// or no longer cover the late fill. Only committed remediation may resolve it;
// a signed flat-position GET alone is insufficient. No guessed success.
fn protection_incident_still_unresolved(reason: Option<&str>, remediation_committed: bool) -> bool {
    !remediation_committed
        && matches!(
            reason,
            Some("PROTECTION_PENDING_OR_FAILED" | "MANAGED_AGGREGATE_PROTECTION_UNVERIFIED")
        )
}

/// The exact remaining SQL lot quantity must equal this entry's cumulative
/// authenticated venue fill before an emergency Close (unless a separate
/// independently proven reduction has already changed the remaining lot).
/// This conservative path blocks partial coverage after late fills; a missing
/// or out-of-sync lot never authorizes a speculative reduce.
fn verified_emergency_owned_exposure<'a>(
    intent: &StoredIntent,
    venue_filled: Decimal,
    venue_position: Decimal,
    lots: impl IntoIterator<Item = (&'a str, Decimal)>,
) -> Result<Decimal, String> {
    if venue_filled <= Decimal::ZERO || venue_position <= Decimal::ZERO {
        return Err("EMERGENCY_EXPOSURE_SOURCE_UNVERIFIED".into());
    }
    let (side, owned) = verified_close_lot_exposure(lots)?;
    if !matches!(
        (side, intent.plan.position_side.as_str()),
        (PositionSide::Long, "long") | (PositionSide::Short, "short")
    ) || owned != venue_filled
        || owned > venue_position
    {
        return Err("EMERGENCY_EXPOSURE_SQL_VENUE_MISMATCH".into());
    }
    Ok(owned)
}

/// A successful source capture only proves that the API replied. It cannot
/// complete the financial lifecycle when no identity-bound fill+bill evidence
/// has been captured. A terminal verified zero-fill cancellation is separate:
/// its central release must still be proven by the existing pending gate.
fn entry_fill_finance_pending(
    risk_increase: bool,
    verified_zero_cancel: bool,
    evidence: &serde_json::Value,
) -> bool {
    risk_increase
        && !verified_zero_cancel
        && evidence["state"] != "verified_native_fill_and_bill_fee_sources"
}
/// Bind the native immutable fill sum to an independent, exact venue
/// readback. A cursor-covered subset is not necessarily the whole execution,
/// especially across partial/late fills or a lost POST response. This check
/// never proves all-in USD debit, fee currency finality, or settlement.
fn verify_venue_fill_coverage(
    intent: &StoredIntent,
    source: &okx_execution::IntentFillCostEvidence,
    observed: &okx_api::TradeOrderDetails,
) -> Result<(), String> {
    if source.intent_id != intent.intent_id
        || source.plan_hash != intent.plan_hash
        || source.exchange_order_id.is_empty()
        || source.exchange_order_id != observed.order_id
        || observed.client_order_id != intent.client_order_id
        || observed.instrument_id != intent.instrument_id
        || observed.side != intent.plan.side
    {
        return Err("ENTRY_FILL_VENUE_IDENTITY_MISMATCH".into());
    }
    let update = observed
        .update_time_ms
        .parse::<i64>()
        .map_err(|_| "ENTRY_FILL_VENUE_TIME_UNVERIFIED")?;
    if update <= 0 || source.covered_through < update {
        return Err("ENTRY_FILL_VENUE_TIME_UNCOVERED".into());
    }
    let venue_filled = dec(&observed.accumulated_fill_size)?;
    if venue_filled <= Decimal::ZERO
        || venue_filled > intent.plan.quantity
        || source.filled_quantity <= Decimal::ZERO
        || source.filled_quantity != venue_filled
    {
        return Err("ENTRY_FILL_VENUE_QUANTITY_MISMATCH".into());
    }
    if !matches!(
        observed.state.as_str(),
        "live" | "partially_filled" | "filled" | "canceled" | "mmp_canceled"
    ) {
        return Err("ENTRY_FILL_VENUE_STATE_UNKNOWN".into());
    }
    Ok(())
}

/// One authoritative exchange update timestamp is reused for both fills and
/// bills cursor checks. Malformed uTime may never bypass final reconciliation.
pub(super) fn terminal_order_update_time(
    order: Option<&okx_api::TradeOrderDetails>,
) -> Result<Option<i64>, String> {
    order
        .map(|o| {
            let at = o
                .update_time_ms
                .parse::<i64>()
                .map_err(|_| "TERMINAL_ORDER_UPDATE_TIME_INVALID")?;
            if at <= 0 {
                return Err("TERMINAL_ORDER_UPDATE_TIME_INVALID".into());
            }
            Ok(at)
        })
        .transpose()
}

/// One native source gate for active protective order evidence. A saved
/// SQL protection receipt or an algo ID in an inventory is not proof of a
/// live, correctly sized exchange stop. Shared by trade.status and restart
/// recovery; this performs no network or SQL calls of its own.
pub(super) fn verified_live_protection(
    intent: &StoredIntent,
    quantity: Decimal,
    source: &okx_api::TradeAlgoOrderDetails,
) -> Result<(), String> {
    let client_id = intent
        .client_order_id
        .get(..31)
        .ok_or("PROTECTION_CLIENT_ID_INVALID")?;
    let expected_client = format!("s{client_id}");
    let opposite = matches!(
        (intent.plan.side.as_str(), source.side.as_str()),
        ("buy", "sell") | ("sell", "buy")
    );
    if quantity <= Decimal::ZERO
        || source.instrument_id != intent.instrument_id
        || source.client_order_id != expected_client
        || source.algo_order_id.is_empty()
        || source.state != "live"
        || !matches!(source.position_side.as_str(), "net" | "long" | "short")
        || (source.position_side != "net" && source.position_side != intent.plan.position_side)
        || !source.failure_code.is_empty()
        || source.stop_loss_trigger_price_type != "mark"
        || !opposite
        || intent.plan.stop_price.is_none()
        || dec(&source.stop_loss_trigger_price).ok() != intent.plan.stop_price
        || dec(&source.size).ok() != Some(quantity)
    {
        return Err("PROTECTION_LIVE_SOURCE_UNVERIFIED".into());
    }
    Ok(())
}

/// Actual parsed venue positions, not an opening order's cumulative fills,
/// determine whether the first-test position is flat and whether protection
/// covers all current exposure. Invalid amounts may never count as zero.
fn verified_position_quantity<'a>(
    amounts: impl IntoIterator<Item = &'a str>,
) -> Result<Decimal, String> {
    amounts.into_iter().try_fold(Decimal::ZERO, |total, value| {
        let quantity = dec(value).map_err(|_| "POSITION_QUANTITY_UNVERIFIED")?;
        total
            .checked_add(quantity.abs())
            .ok_or_else(|| "POSITION_QUANTITY_OVERFLOW".into())
    })
}

/// The venue's net_mode position direction is encoded in the SIGNED
/// `pos` field, not the `posSide` label ("net"). Native SQL lots instead
/// use long/short. This is the sole normalization shared by status,
/// all-account census and recovery. Unknown or inconsistent sources cannot
/// certify a flat position or permit a new order.
pub(super) fn verified_venue_position_direction(
    position_side: &str,
    raw_position: &str,
) -> Result<Option<(&'static str, Decimal)>, String> {
    let signed = dec(raw_position).map_err(|_| "VENUE_POSITION_QUANTITY_UNVERIFIED")?;
    if signed == Decimal::ZERO {
        if !matches!(position_side, "net" | "long" | "short") {
            return Err("VENUE_POSITION_SIDE_UNVERIFIED".into());
        }
        return Ok(None);
    }
    let (direction, quantity) = match position_side {
        "net" if signed > Decimal::ZERO => ("long", signed),
        "net" => (
            "short",
            signed
                .checked_mul(-Decimal::ONE)
                .ok_or("VENUE_POSITION_QUANTITY_OVERFLOW")?,
        ),
        "long" if signed > Decimal::ZERO => ("long", signed),
        "short" => (
            "short",
            if signed < Decimal::ZERO {
                signed
                    .checked_mul(-Decimal::ONE)
                    .ok_or("VENUE_POSITION_QUANTITY_OVERFLOW")?
            } else {
                signed
            },
        ),
        "long" => return Err("VENUE_POSITION_SIDE_QUANTITY_MISMATCH".into()),
        _ => return Err("VENUE_POSITION_SIDE_UNVERIFIED".into()),
    };
    Ok(Some((direction, quantity)))
}

/// A one-intent status view must not present a net-mode opposite
/// position as zero exposure. Such a read is inconclusive for its stop,
/// so the existing reconcile/STOP route stays responsible.
fn verified_intent_position_quantity<'a>(
    expected_side: &str,
    positions: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<Decimal, String> {
    positions
        .into_iter()
        .try_fold(
            Decimal::ZERO,
            |total, (side, raw)| match verified_venue_position_direction(side, raw)? {
                None => Ok(total),
                Some((direction, quantity)) if direction == expected_side => total
                    .checked_add(quantity)
                    .ok_or_else(|| "VENUE_POSITION_QUANTITY_OVERFLOW".into()),
                Some(_) => Err("VENUE_POSITION_OPPOSITE_EXPOSURE_UNVERIFIED".into()),
            },
        )
}

/// Protection cleanup must inspect the whole instrument (both hedge
/// sides), not just the original side. A malformed or overflowing position
/// is UNKNOWN exposure and may never authorize removing a live stop.
pub(super) fn verify_instrument_is_flat<'a>(
    instrument: &str,
    positions: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<(), String> {
    let exposure = verified_position_quantity(
        positions
            .into_iter()
            .filter(|(id, _)| *id == instrument)
            .map(|(_, quantity)| quantity),
    )?;
    if exposure != Decimal::ZERO {
        return Err("PROTECTION_STILL_REQUIRED".into());
    }
    Ok(())
}

/// The first real campaign accepts protection only when the live order
/// covers exactly the remaining first-test position. Matching the original
/// fill count alone can leave an overprotective order after a partial close.
fn protection_covers_live_position(
    opening_side: &str,
    protection_side: &str,
    cumulative_fill: Decimal,
    venue_position: Decimal,
    protection_size: &str,
) -> bool {
    let opposite = matches!(
        (opening_side, protection_side),
        ("buy", "sell") | ("sell", "buy")
    );
    opposite
        && cumulative_fill > Decimal::ZERO
        && venue_position > Decimal::ZERO
        && cumulative_fill == venue_position
        && dec(protection_size).ok() == Some(venue_position)
}

/// An attached Add stop covers this intent's actually executed increment,
/// NOT the whole net-mode venue position already covered by older verified
/// stops. All older lots, stops and reservations are rechecked by the native
/// full-cohort census before this compound protection can be certified.
fn protection_covers_managed_increment(
    opening_side: &str,
    protection_side: &str,
    cumulative_fill: Decimal,
    venue_position: Decimal,
    protection_size: &str,
) -> bool {
    cumulative_fill > Decimal::ZERO
        && venue_position >= cumulative_fill
        && protection_covers_live_position(
            opening_side,
            protection_side,
            cumulative_fill,
            cumulative_fill,
            protection_size,
        )
}

/// A signed venue position smaller than the still-durable SQL lot is an
/// ordering disagreement, not by itself evidence that the attached stop is
/// missing. Only a separately authenticated, *exact* still-live old stop
/// permits a pending-source classification; anything else remains a
/// protection failure. This never proves a Reduce, changes a lot, confirms
/// current protection sizing, or permits a new RiskBook grant.
fn signed_position_below_sql_lot_with_live_old_stop(
    intent: &StoredIntent,
    sql_owned: Decimal,
    signed_position: Decimal,
    source: &okx_api::TradeAlgoOrderDetails,
) -> bool {
    sql_owned > Decimal::ZERO
        && signed_position > Decimal::ZERO
        && signed_position < sql_owned
        && verified_live_protection(intent, sql_owned, source).is_ok()
}

/// Every venue readback must refer to this durable intent before it can
/// change local state, protection requirements, or exchange-order identity.
/// An exchange lookup by client ID alone is a lookup, not an identity proof.
pub(super) fn verified_order_readback(
    intent: &StoredIntent,
    order: &okx_api::TradeOrderDetails,
) -> Result<Decimal, String> {
    if order.order_id.is_empty()
        || order.client_order_id != intent.client_order_id
        || order.instrument_id != intent.instrument_id
        || order.side != intent.plan.side
        || intent
            .exchange_order_id
            .as_deref()
            .is_some_and(|id| id != order.order_id)
    {
        return Err("ORDER_READBACK_IDENTITY_MISMATCH".into());
    }
    if dec(&order.size)? != intent.plan.quantity {
        return Err("ORDER_READBACK_PLAN_MISMATCH".into());
    }
    let filled =
        dec(&order.accumulated_fill_size).map_err(|_| "ORDER_READBACK_FILLED_SIZE_INVALID")?;
    if filled < Decimal::ZERO || filled > intent.plan.quantity {
        return Err("ORDER_READBACK_FILLED_SIZE_INVALID".into());
    }
    Ok(filled)
}

/// Replace is a NEW entry, not a retry of the canceled entry. Even after
/// a successful cancel ACK, an unavailable venue readback or ANY executed
/// quantity must never be treated as an empty old order.
fn verify_empty_canceled_order_for_replace(
    intent: &StoredIntent,
    order: &okx_api::TradeOrderDetails,
) -> Result<(), String> {
    if !matches!(order.state.as_str(), "canceled" | "mmp_canceled") {
        return Err("REPLACE_CANCELED_READBACK_UNPROVEN".into());
    }
    verified_terminal_order_update_time(intent, Some(order))
        .map_err(|_| "REPLACE_CANCELED_READBACK_UNPROVEN")?;
    let filled =
        verified_order_readback(intent, order).map_err(|_| "REPLACE_CANCELED_READBACK_UNPROVEN")?;
    if filled != Decimal::ZERO {
        return Err("REPLACE_PARTIAL_FILL_REQUIRES_EXPLICIT_CLOSE".into());
    }
    Ok(())
}

/// The authoritative SQL lots for one intent must agree on direction and
/// contain only positive remaining exposure. Never silently add opposite
/// sides, skip negative lots or overflow before constructing a close order.
fn verified_close_lot_exposure<'a>(
    lots: impl IntoIterator<Item = (&'a str, Decimal)>,
) -> Result<(PositionSide, Decimal), String> {
    let mut direction: Option<&str> = None;
    let mut quantity = Decimal::ZERO;
    for (side, remaining) in lots {
        if !matches!(side, "long" | "short") {
            return Err("CLOSE_LOT_SIDE_UNATTESTED".into());
        }
        if remaining <= Decimal::ZERO {
            return Err("CLOSE_LOT_QUANTITY_UNVERIFIED".into());
        }
        if direction.is_some_and(|old| old != side) {
            return Err("CLOSE_LOT_MIXED_DIRECTIONS".into());
        }
        direction = Some(side);
        quantity = quantity
            .checked_add(remaining)
            .ok_or("CLOSE_LOT_QUANTITY_OVERFLOW")?;
    }
    let side = match direction {
        Some("long") => PositionSide::Long,
        Some("short") => PositionSide::Short,
        _ => return Err("NO_ATTRIBUTED_POSITION_TO_CLOSE".into()),
    };
    Ok((side, quantity))
}

/// A terminal local state alone is not source proof that an exchange order is
/// terminal. No real release without matching exchange identity, covered final
/// update and a bounded executed quantity. In particular, missing readback is
/// UNKNOWN, not a zero-fill terminal outcome.
pub(super) fn verified_terminal_order_update_time(
    intent: &StoredIntent,
    order: Option<&okx_api::TradeOrderDetails>,
) -> Result<i64, String> {
    let o = order.ok_or("TERMINAL_ORDER_READBACK_MISSING")?;
    if intent.exchange_order_id.as_deref() != Some(o.order_id.as_str()) {
        return Err("TERMINAL_ORDER_IDENTITY_MISMATCH".into());
    }
    verified_order_readback(intent, o).map_err(|_| "TERMINAL_ORDER_IDENTITY_OR_SIZE_UNVERIFIED")?;
    if !matches!(o.state.as_str(), "filled" | "canceled" | "mmp_canceled") {
        return Err("TERMINAL_ORDER_STATE_UNVERIFIED".into());
    }
    terminal_order_update_time(Some(o))?.ok_or_else(|| "TERMINAL_ORDER_READBACK_MISSING".into())
}

/// The native SQL owner independently verifies both records on readback.
/// An observed price-only quote is NOT authority to submit. A fully sourced
/// admission is still cost evidence, not an exchange POST authorization.
fn entry_cost_status(
    stored: Option<okx_execution::EntryQuoteEvidence>,
    admission: Option<okx_execution::EntryAdmissionEvidence>,
) -> Result<Value, String> {
    let Some(quote) = stored else {
        return Ok(json!({
            "state": "not_proven",
            "all_in_entry_cost_proven": false,
            "entry_budget_upper_reporting": null,
            "reason_codes": ["ENTRY_QUOTE_NOT_RECORDED"],
            "next_action": "RECONCILE_ENTRY_COST_SOURCES"
        }));
    };
    let known = entry_budget::quote(
        quote.quantity,
        quote.contract_value,
        quote.limit_price,
        quote.fee_rate,
        quote.usd_per_usdt,
    )?;
    if known.notional_usdt != quote.notional_usdt
        || known.fee_usdt != quote.fee_usdt
        || known.total_usdt != quote.total_usdt
        || known.total_usd != quote.total_usd
    {
        return Err("ENTRY_QUOTE_READBACK_MISMATCH".into());
    }
    if let Some(cost) = admission {
        if cost.intent_id != quote.intent_id
            || cost.plan_hash != quote.plan_hash
            || cost.account_key != quote.account_key
            || cost.installation_id != quote.installation_id
            || cost.entry_upper_usd < quote.total_usd
            || cost.entry_upper_usd <= Decimal::ZERO
        {
            return Err("ENTRY_ADMISSION_READBACK_MISMATCH".into());
        }
        return Ok(json!({
            "schema":"okx.entry-admission/v2",
            "state":"source_bound_entry_cost",
            "known_notional_usdt":exact(quote.notional_usdt),
            "known_fee_usdt":exact(quote.fee_usdt),
            "known_entry_total_usd":exact(quote.total_usd),
            "all_in_entry_cost_proven":true,
            "entry_budget_upper_reporting":exact(cost.entry_upper_usd),
            "entry_budget_metric":"OpeningNotionalPlusEntryFees",
            "valuation_mode":"AdmissionSnapshot",
            "entry_proof_digest":cost.entry_proof_digest,
            "source":{"quote_digest":cost.quote_digest,"observed_at_ms":cost.observed_at},
            "authorization":"COST_PROOF_ONLY_NOT_POST_AUTHORITY",
        }));
    }
    let proof = entry_budget::EntryCostProof::unproven_linear_quote(known);
    Ok(json!({
        "schema": "okx.entry-known-quote/v1",
        "state": "historical_known_cost_only",
        "known_notional_usdt": exact(quote.notional_usdt),
        "known_fee_usdt": exact(quote.fee_usdt),
        "known_entry_total_usd": exact(quote.total_usd),
        "source": {
            "observed_at_ms": quote.observed_at,
            "fee_generation": quote.fee_generation,
            "reference_generation": quote.reference_generation,
            "conversion_source_digest": quote.conversion_digest,
        },
        "all_in_entry_cost_proven": false,
        "entry_budget_upper_reporting": null,
        "reason_codes": proof.missing_reasons(),
        "next_action": "RECONCILE_ENTRY_COST_SOURCES"
    }))
}

#[cfg(test)]
#[path = "reconcile/status_tests.rs"]
mod entry_cost_status_tests;

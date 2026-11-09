use super::*;
use okx_api::CancelOrderRequest;

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

impl TradeService {
    async fn find_intent(
        &self,
        r: &AccountResources,
        reference: &str,
    ) -> Result<StoredIntent, String> {
        let reference = reference.to_owned();
        r.execution
            .store
            .call(deadline(), move |s| {
                Ok(s.intent(&reference)?
                    .or(s.by_client_order_id(&reference)?)
                    .or(s.by_exchange_order_id(&reference)?)
                    .or(s.by_idempotency(&reference)?))
            })
            .await
            .map_err(err)?
            .ok_or("INTENT_NOT_FOUND".into())
    }
    /// Observe one original order through the real adapter and persist only a
    /// verified readback. Does not capture finance, release risk or resend.
    pub(crate) async fn observe_intent_order(
        &self,
        r: &Arc<AccountResources>,
        mut i: StoredIntent,
    ) -> Result<(StoredIntent, Option<okx_api::TradeOrderDetails>), String> {
        let order = if matches!(i.state, IntentState::Rejected) && i.exchange_order_id.is_none() {
            None
        } else {
            match r
                .execution
                .trade
                .order_by_client_id_until(&i.instrument_id, &i.client_order_id, deadline())
                .await
            {
                Ok(o) => Some(o),
                Err(_error) => {
                    // Test-only causal classification at the exact failure
                    // boundary: a local admission defer, missing signed raw
                    // GET, protocol response or deadline must never collapse
                    // into the same test diagnosis. No public identity, raw
                    // payload or credential is emitted. Production remains
                    // fail-closed with the same UNKNOWN/no-resubmit result.
                    #[cfg(test)]
                    if std::env::var("OKX_T09_8_DENIAL_PROBE").as_deref() == Ok("1") {
                        let (class, request_sent, local_defer_ms) = match &_error {
                            okx_api::OkxError::RateLimited { evidence } => (
                                "RATE_LIMITED",
                                Some(evidence.request_sent),
                                Some(evidence.local_defer_ms),
                            ),
                            okx_api::OkxError::Response(reason)
                                if reason.starts_with(
                                    "TRANSCRIPT_SOURCE_PAGE_UNEXPECTED_OR_EXHAUSTED",
                                ) =>
                            {
                                ("TRANSCRIPT_SOURCE_EXHAUSTED", None, None)
                            }
                            okx_api::OkxError::Response(reason)
                                if reason == "ORDER_READBACK_DEADLINE_EXCEEDED" =>
                            {
                                ("READBACK_DEADLINE", None, None)
                            }
                            okx_api::OkxError::Response(reason)
                                if reason.starts_with("TRANSCRIPT_") =>
                            {
                                ("TRANSCRIPT_SOURCE_OTHER", None, None)
                            }
                            okx_api::OkxError::Response(_) => {
                                ("VENUE_RESPONSE_UNVERIFIED", None, None)
                            }
                            okx_api::OkxError::Json(_) => ("VENUE_ORDER_DECODE", None, None),
                            okx_api::OkxError::Api { .. } => ("VENUE_API_DENIAL", None, None),
                            okx_api::OkxError::Clock(_) => ("CLOCK_SOURCE", None, None),
                            _ => ("OTHER_READBACK_FAILURE", None, None),
                        };
                        eprintln!(
                            "T09_6_ORDER_READBACK_CAUSE_V1:{}",
                            json!({
                                "schema":"okx.t09-6-order-readback-cause/v1",
                                "class":class,
                                "request_sent":request_sent,
                                "local_defer_ms":local_defer_ms,
                                "sql_state":i.state.as_str(),
                                "has_exchange_ack":i.exchange_order_id.is_some(),
                            })
                        );
                    }
                    return Err("ORDER_READBACK_INCOMPLETE_NO_RESUBMIT".into());
                }
            }
        };
        if let Some(o) = &order {
            let state = match o.state.as_str() {
                "live" => IntentState::Acknowledged,
                "partially_filled" => IntentState::PartiallyFilled,
                "filled" => IntentState::Filled,
                "canceled" | "mmp_canceled" => IntentState::Cancelled,
                _ => return Err("EXCHANGE_ORDER_STATE_UNKNOWN".into()),
            };
            let filled = verified_order_readback(&i, o)?;
            let id = i.intent_id.clone();
            let order_id = o.order_id.clone();
            if !i.state.terminal() {
                r.execution
                    .store
                    .call(deadline(), move |s| {
                        s.record_order_readback(&id, &order_id, state, filled, now())
                    })
                    .await
                    .map_err(err)?;
            }
            i = self.find_intent(r, &i.intent_id).await?;
        }
        Ok((i, order))
    }

    pub(super) async fn status_intent(
        &self,
        r: &Arc<AccountResources>,
        reference: &str,
        invocation: Option<crate::diagnostics::ProtectionCleanupInvocation<'_>>,
    ) -> Result<Value, String> {
        let mut i = self.find_intent(r, reference).await?;
        // Historical fee/notional is a recorded fact, not an authorized entry.
        let entry_costs = if i.risk_increase
            && r.configuration.account_key.environment == okx_protocol::accounts::Environment::Real
        {
            let id = i.intent_id.clone();
            let (quote, admission) = r
                .execution
                .store
                .call(deadline(), move |s| {
                    Ok((s.entry_quote(&id)?, s.entry_admission(&id)?))
                })
                .await
                .map_err(err)?;
            entry_cost_status(quote, admission)?
        } else {
            json!(null)
        };
        // Persisted incidents remain safety-active even when a later algo GET
        // reports live. An observed flat venue position is not an SQL resolution.
        let prior_protection_incident =
            protection_incident_still_unresolved(i.incident.as_deref(), false);
        let mut protection_failed = false;
        let mut protection_failure_observed_at = None;
        let mut protection_source_pending = false;
        let mut aggregate_protection_blocker: Option<String> = None;
        let mut venue_flat_verified = false;
        let mut signed_position_qty = None;
        if matches!(i.state, IntentState::Prepared | IntentState::Reserved) {
            if now() >= i.deadline {
                let id = i.intent_id.clone();
                r.execution
                    .store
                    .call(deadline(), move |s| {
                        s.reject_unsent(&id, "COMMAND_EXPIRED", now())
                    })
                    .await
                    .map_err(err)?;
                i = self.find_intent(r, &i.intent_id).await?;
            } else {
                return Ok(completed(
                    json!({"intent":i,"remaining_exposure":"not_evaluated","entry_costs":entry_costs}),
                ));
            }
        }
        let id = i.intent_id.clone();
        if r.execution
            .store
            .call(deadline(), move |s| s.declined_reserve(&id))
            .await
            .map_err(err)?
        {
            return Ok(completed(json!({
                "intent":i,"send_state":"NOT_STARTED",
                "entry_costs":entry_costs,
                "rejection_origin":"CENTRAL","primary_reason":i.incident,
                "central_release":{"state":"not_required","proof":"RESERVE_DENIED"}
            })));
        }
        // F1: no risk command was ever attempted. The SQL terminal receipt
        // and absent reserve marker jointly prove no-send / no central risk;
        // skip unrelated venue and financial I/O.
        let id = i.intent_id.clone();
        if r.execution
            .store
            .call(deadline(), move |s| s.unsent_before_reserve(&id))
            .await
            .map_err(err)?
        {
            return Ok(completed(json!({
                "intent":i,
                "send_state":"NOT_STARTED",
                "entry_costs":entry_costs,
                "rejection_origin":"LOCAL",
                "primary_reason":i.incident,
                "finance_state":"NOT_APPLICABLE",
                "central_release":{"state":"not_required","proof":"PRE_RESERVE_SQL"}
            })));
        }
        // A durable SQL REJECTED_UNSENT can release its central reservation
        // independently of exchange history/fees. Other rejected intents
        // still require the full authoritative reconciliation below.
        let id = i.intent_id.clone();
        let no_send = r
            .execution
            .store
            .call(deadline(), move |s| s.unsent_release_receipt(&id))
            .await
            .map_err(err)?
            .is_some();
        if no_send {
            // A completed native SQL REJECTED_UNSENT receipt is proof that
            // this intent never touched venue. It needs neither a fills
            // backfill nor an account-wide finance snapshot for release.
            let central_release = match self.release_unsent(r, &i).await {
                Ok(_) => json!({"state":"verified","proof":"SQL_NO_SEND"}),
                Err(failure) => json!({
                    "state":"pending","reason_code":failure.reason_code,
                    "failure":failure,
                    "scope":"intent","next_action":"trade.status"
                }),
            };
            return Ok(completed(json!({
                "intent":i,
                "send_state":"NOT_STARTED",
                "entry_costs":entry_costs,
                "rejection_origin":"LOCAL",
                "primary_reason":i.incident,
                "finance_state":"NOT_APPLICABLE",
                "central_release":central_release
            })));
        }
        let original_id = i.intent_id.clone();
        let (observed_intent, order) = match self.observe_intent_order(r, i).await {
            Ok(observed) => observed,
            Err(reason)
                if matches!(
                    reason.as_str(),
                    "ORDER_READBACK_INCOMPLETE_NO_RESUBMIT" | "EXCHANGE_ORDER_STATE_UNKNOWN"
                ) =>
            {
                return Ok(uncertain(&original_id, &reason));
            }
            Err(reason) => return Err(reason),
        };
        i = observed_intent;
        if let Some(o) = &order {
            let filled = verified_order_readback(&i, o)?;
            // Both entry and reduction status must reconcile against fresh
            // authenticated account facts. A private GET may be deferred by
            // the existing per-user RateBudget without ever reaching OKX.
            // Wait only for that LOCAL no-send read admission, within the
            // existing finite status deadline. No venue mutation is retried,
            // no stale snapshot is substituted, and terminal release still
            // requires independent finance/census proof.
            let snapshot = r.observer.snapshot_until(deadline()).await.map_err(err)?;
            let position_qty = verified_intent_position_quantity(
                &i.plan.position_side,
                snapshot
                    .positions
                    .iter()
                    .filter(|p| p.instrument_id == i.instrument_id)
                    .map(|p| (p.position_side.as_str(), p.position.as_str())),
            )?;
            signed_position_qty = Some(position_qty);
            let exposed = position_qty > Decimal::ZERO;
            // A reduction's local terminal marker is not discharged by an
            // order ACK. Require an authenticated flat-instrument readback.
            venue_flat_verified = verify_instrument_is_flat(
                &i.instrument_id,
                snapshot
                    .positions
                    .iter()
                    .map(|p| (p.instrument_id.as_str(), p.position.as_str())),
            )
            .is_ok();
            protection_cleanup::cleanup_flat_protection(
                protection_cleanup::CleanupDependencies {
                    store: &r.execution.store,
                    observer: &r.observer,
                    trade: &r.execution.trade,
                    clock: &r.execution.clock,
                    mutation_capacity: &r.mutation_capacity,
                    verified_uid: &r.configuration.account_key.verified_uid,
                    trace: &r.protection_operation_trace,
                },
                protection_cleanup::CleanupOrderFacts {
                    intent_id: &i.intent_id,
                    client_order_id: &i.client_order_id,
                    instrument_id: &i.instrument_id,
                    risk_increase: i.risk_increase,
                    signed_exposed: exposed,
                    venue_order_state: &o.state,
                    invocation,
                },
            )
            .await?;
            // An Add ACK with zero executed contracts does NOT own the
            // previously protected position observed on this net-mode
            // instrument. The pre-POST signed full-cohort census verified
            // that earlier exposure and its live stops. An unfilled Add
            // must retain its central reservation, but its attached stop
            // is not yet an active stop against the old lots. Never
            // emergency-cancel an unrelated managed position because a
            // new limit order has not filled.
            if i.risk_increase && exposed && filled > Decimal::ZERO && !prior_protection_incident {
                // Gross entry fills are historical; after a separately
                // proven partial Reduce the ONLY permitted smaller stop is
                // the still-owned SQL lot quantity with a verified durable
                // single-send amendment receipt. An unsigned/synthetic
                // smaller live stop is always rejected.
                let original_id = i.intent_id.clone();
                let original_inst = i.instrument_id.clone();
                let original_terminal = i.state.terminal();
                let original_plan_hash = i.plan_hash.clone();
                let original_order_id = i.exchange_order_id.clone();
                let effective_protected = r
                    .execution
                    .store
                    .call(deadline(), move |s| {
                        let lots = s.active_lots(&original_inst)?;
                        let own = lots
                            .iter()
                            .filter(|lot| lot.intent_id.as_deref() == Some(original_id.as_str()))
                            .fold(Decimal::ZERO, |sum, lot| sum + lot.remaining_qty);
                        // A later position on the same instrument does not
                        // belong to an already closed/released entry. Its late
                        // fees still need the ordinary financial reconciliation
                        // below, without resurrecting its former venue stop.
                        if own == Decimal::ZERO
                            && original_terminal
                            && s.active_central_receipt(&original_id)?.is_none()
                        {
                            let terminal_evidence = (
                                s.central_receipt(&original_id)?,
                                s.operation(&format!("entry-settlement:{original_id}"))?,
                            );
                            if let (Some(receipt), Some(terminal)) = terminal_evidence {
                                let proof = serde_json::from_str::<
                                    okx_execution::EntrySettlementEvidence,
                                >(&terminal.result_ref)
                                .map_err(|_| {
                                    okx_execution::StoreError::Invalid(
                                        "TERMINAL_ENTRY_PROOF_CORRUPT",
                                    )
                                })?;
                                let release = s.operation(&format!(
                                    "central-release:{}",
                                    receipt.reservation_key
                                ))?;
                                if terminal.kind == "ENTRY_SETTLEMENT_PROVEN"
                                    && proof.schema == "okx.entry-terminal-settlement/v1"
                                    && proof.intent_id == original_id
                                    && receipt.intent_id == original_id
                                    && original_order_id.as_deref()
                                        == Some(proof.venue_order_id.as_str())
                                    && proof.plan_hash == original_plan_hash
                                    && receipt.plan_hash == original_plan_hash
                                    && proof.account_key == receipt.account_key
                                    && proof.reservation_key == receipt.reservation_key
                                    && proof.venue_filled_quantity == filled
                                    && release.is_some_and(|ack| {
                                        ack.kind == "CENTRAL_RELEASE"
                                            && ack.request_hash == receipt.receipt_hash
                                            && !ack.result_ref.is_empty()
                                    })
                                {
                                    return Ok(None);
                                }
                            }
                        }
                        if own <= Decimal::ZERO || own > filled {
                            return Err(okx_execution::StoreError::Invalid(
                                "PROTECTION_ORIGINAL_SQL_LOT_MISMATCH",
                            ));
                        }
                        if own == filled {
                            return Ok(Some(own));
                        }
                        let receipt = s.protection(&original_id)?.ok_or(
                            okx_execution::StoreError::Invalid(
                                "PROTECTION_REDUCED_LOT_RECEIPT_MISSING",
                            ),
                        )?;
                        let key = format!("protection-shrink:{}:{}", original_id, exact(own));
                        let op = s.operation(&format!("{key}:verified"))?.ok_or(
                            okx_execution::StoreError::Invalid(
                                "PROTECTION_REDUCE_SOURCE_ACK_MISSING",
                            ),
                        )?;
                        if op.kind != "protection_shrink_verified"
                            || op.result_ref != "VERIFIED"
                            || receipt.quantity != own
                        {
                            return Err(okx_execution::StoreError::Invalid(
                                "PROTECTION_REDUCE_SOURCE_MISMATCH",
                            ));
                        }
                        Ok(Some(own))
                    })
                    .await
                    .map_err(err)?;
                if let Some(effective_protected) = effective_protected {
                    let algo_client = i
                        .client_order_id
                        .get(..31)
                        .map(|client| format!("s{client}"))
                        .ok_or("PROTECTION_CLIENT_ID_INVALID")?;
                    let p = r
                        .execution
                        .trade
                        .algo_order_by_client_id(&algo_client)
                        .await;
                    match p {
                        Ok(p)
                            if signed_position_below_sql_lot_with_live_old_stop(
                                &i,
                                effective_protected,
                                position_qty,
                                &p,
                            ) =>
                        {
                            // Position GET has moved ahead of the SQL lot
                            // projection (for example, after signed Reduce
                            // execution but before its native finance ingest).
                            // The exact original stop is still live. Remain
                            // fail-closed and retry after source convergence;
                            // do NOT create an irreversible incident merely
                            // because these independently owned observations
                            // have different timestamps.
                            protection_source_pending = true;
                        }
                        Ok(p)
                            if verified_live_protection(&i, effective_protected, &p).is_ok()
                                && protection_covers_managed_increment(
                                    &i.plan.side,
                                    &p.side,
                                    effective_protected,
                                    position_qty,
                                    &p.size,
                                ) =>
                        {
                            let source_digest = digest(&p)?;
                            let protection = okx_execution::ProtectionReceipt {
                                intent_id: i.intent_id.clone(),
                                algo_id: p.algo_order_id,
                                side: p.side,
                                quantity: effective_protected,
                                trigger_price: i.plan.stop_price.ok_or("STOP_PRICE_MISSING")?,
                                trigger_basis: "mark".into(),
                                verified_at: now(),
                                source_digest,
                            };
                            r.execution
                                .store
                                .call(deadline(), move |s| s.verify_protection(&protection))
                                .await
                                .map_err(err)?;
                            // If the live venue quantity exceeds this intent's
                            // fill, the previous position MUST still be covered by
                            // its own SQL lots, distinct venue stop(s) and active
                            // RiskBook reservation. Reuse the sole full-cohort
                            // signed census, never simply subtract the old qty.
                            if position_qty > effective_protected {
                                let aggregate_result = match self.governance.read().await.clone() {
                                    Some(state) => self
                                        .census_for_managed_protection_until(&state, deadline())
                                        .await
                                        .map(|_| ()),
                                    None => {
                                        Err(super::execute::ManagedProtectionCensusError::Denied(
                                            "AUTHORITY_UNAVAILABLE".into(),
                                        ))
                                    }
                                };
                                match aggregate_result {
                                Ok(()) => {}
                                Err(
                                    super::execute::ManagedProtectionCensusError::SourceBeforeLedgerFill,
                                ) => {
                                    // A signed venue position older than an already
                                    // committed SQL fill cannot prove a missing stop
                                    // or authorize an emergency Close. New-risk
                                    // census still fails closed until fresh source.
                                    protection_source_pending = true;
                                    eprintln!(
                                        "MANAGED_AGGREGATE_SOURCE_PENDING_V1:POSITION_BEFORE_SQL_FILL"
                                    );
                                }
                                Err(super::execute::ManagedProtectionCensusError::PendingPeerPost) => {
                                    // Another SQL intent is POSTING/UNKNOWN. Its venue
                                    // outcome and any attached protection are unresolved,
                                    // not a verified failure of this intent's live stop.
                                    // Keep the global new-risk census fail-closed, report
                                    // source pending, and do not create a speculative
                                    // safety_close for a different owned position.
                                    protection_source_pending = true;
                                    eprintln!(
                                        "MANAGED_AGGREGATE_SOURCE_PENDING_V1:COHORT_UNRESOLVED_POST_OUTCOME"
                                    );
                                }
                                Err(super::execute::ManagedProtectionCensusError::Denied(reason))
                                    if reason == "UNATTRIBUTED_EXCHANGE_POSITION" => {
                                    // This branch already verified this intent's exact
                                    // live stop and committed its protection receipt.
                                    // Additional, unowned venue exposure still denies
                                    // the complete new-risk census. It is not evidence
                                    // that this particular stop disappeared, and cannot
                                    // authorize a safety close of these owned lots.
                                    aggregate_protection_blocker = Some(reason);
                                }
                                Err(super::execute::ManagedProtectionCensusError::Denied(reason)) => {
                                    eprintln!("MANAGED_AGGREGATE_CENSUS_DENIAL_V1:{reason}");
                                    protection_failed = true;
                                    protection_failure_observed_at = Some(Instant::now());
                                    let id = i.intent_id.clone();
                                    r.execution
                                        .store
                                        .call(deadline(), move |s| {
                                            s.protection_incident(
                                                &id,
                                                "MANAGED_AGGREGATE_PROTECTION_UNVERIFIED",
                                                now(),
                                            )
                                        })
                                        .await
                                        .map_err(err)?;
                                }
                            }
                            }
                        }
                        _ => {
                            protection_failed = true;
                            protection_failure_observed_at = Some(Instant::now());
                            let id = i.intent_id.clone();
                            r.execution
                                .store
                                .call(deadline(), move |s| {
                                    s.protection_incident(
                                        &id,
                                        "PROTECTION_PENDING_OR_FAILED",
                                        now(),
                                    )
                                })
                                .await
                                .map_err(err)?;
                        }
                    }
                }
            }
        }
        // A confirmed failure of the attached stop must start the protective
        // policy BEFORE any possibly slow/failed finance capture. The durable
        // incident already blocks new risk. The only pre-finance venue mutation
        // allowed is a single ACK-bound cancel of this exact entry order.
        // A reduce/Close needs independently captured signed fills and SQL lots.
        let previously_unprotected_exposure = prior_protection_incident
            && signed_position_qty.is_some_and(|quantity| quantity > Decimal::ZERO);
        let protective = if protection_failed || previously_unprotected_exposure {
            Some(
                bounded_protection_before_finance(
                    protection_failure_observed_at.unwrap_or_else(Instant::now),
                    Duration::from_secs(2),
                    self.emergency_protection_before_finance(
                        r,
                        &i,
                        signed_position_qty.ok_or("EMERGENCY_SIGNED_POSITION_MISSING")?,
                    ),
                )
                .await,
            )
        } else {
            None
        };
        // No proven owned quantity or unresolved network outcome authorizes
        // any further mutation. Return the durable incident and an explicit
        // protective blocker immediately; the account background source
        // owner continues capture independently. A later trade.status may
        // perform a source-backed Close, never retry an unknown POST.
        if let Some(ref action) = protective
            && action["state"] == "blocked"
        {
            eprintln!(
                "T10_08_PROTECTIVE_BLOCKER_V1:{}",
                json!({
                    "intent_id":i.intent_id,
                    "elapsed_ms":action["elapsed_ms"],
                    "reason_code":action["reason_code"],
                    "phase":"before_financial_capture",
                    "retry_create":false
                })
            );
            let mut result = completed(json!({
                "intent":i,
                "remaining_exposure":null,
                "entry_costs":entry_costs,
                "entry_fill_evidence":{"state":"not_evaluated",
                    "all_in_entry_cost_proven":false},
                "protection_incident":true,
                "protection_source_pending":protection_source_pending,
                "aggregate_protection_blocker":aggregate_protection_blocker,
                "compensation":action,
                "financial_complete":false,
                "central_release":{"state":"blocked","reason_code":
                    "PROTECTION_INCIDENT_UNRESOLVED"},
                "next_action":"trade.status",
            }));
            result["status"] = json!("partial");
            result["quality"] = json!({
                "execution":"authenticated_readback",
                "financial":"incomplete",
                "entry_settlement":"pending",
                "protection":"incident"
            });
            result["warnings"] = json!(["EMERGENCY_PROTECTION_UNRESOLVED"]);
            return Ok(result);
        }
        // Even when a source-backed protective Close was attempted above,
        // settlement/central release remain subject to the ordinary signed
        // financial capture. Never interpret Close ACK as a finance proof.
        let financial_capture = crate::source_capture::capture_account_sources(
            &r.observer,
            &r.private_ws,
            &r.execution,
        )
        .await;
        let financial_error = financial_capture.as_ref().err().map(ToString::to_string);
        let financial = financial_capture.is_ok();
        let compensation = protective;
        // Count every independently captured partial fill + linked fee bill
        // against the original frozen admission before a terminal release.
        // A missing bill is PENDING, never zero or an invented risk refund.
        // This uses the same SQLite owner and central SETTLE_ENTRY command.
        let verified_zero_cancel = i.state == IntentState::Cancelled
            && order.as_ref().is_some_and(|venue| {
                verified_terminal_order_update_time(&i, Some(venue)).is_ok()
                    && verified_order_readback(&i, venue) == Ok(Decimal::ZERO)
            });
        let incremental_entry = if financial && i.risk_increase && verified_zero_cancel {
            // The terminal zero-fee proof and central settlement are owned
            // exclusively by release_terminal; a missing partial fill cannot
            // be treated as an incremental settlement failure.
            json!({"state":"not_applicable_zero_fill_terminal"})
        } else if financial
            && i.risk_increase
            && matches!(
                i.state,
                IntentState::PartiallyFilled | IntentState::Filled | IntentState::Cancelled
            )
        {
            match entry_settlement::settle_incremental_entry(
                &r.configuration.account_key,
                &i,
                order.as_ref(),
                &r.execution.store,
                &self.risk,
                &self.governance,
                deadline,
            )
            .await
            {
                Ok(_) => json!({"state":"verified_or_no_change"}),
                Err(reason) => json!({"state":"pending","reason_code":reason.reason_code}),
            }
        } else {
            json!({"state":"not_evaluated"})
        };
        let protection_adjustment = if financial
            && !i.risk_increase
            && i.plan.operation == "reduce"
            && i.state == IntentState::Filled
            && signed_position_qty.is_some_and(|qty| qty > Decimal::ZERO)
        {
            match self
                .shrink_stop_after_reduce(
                    r,
                    &i,
                    order.as_ref().ok_or("REDUCE_VENUE_READBACK_MISSING")?,
                    signed_position_qty.ok_or("REDUCE_SIGNED_POSITION_MISSING")?,
                )
                .await
            {
                Ok(state) => state,
                Err(reason) => json!({
                    "state":"blocked",
                    "reason_code":reason,
                    "retry_amend":false,
                    "next_action":"trade.status",
                }),
            }
        } else {
            json!({"state":"not_applicable"})
        };
        // Only a lost Worker settlement reply with a VERIFIED, still-unacked
        // immutable SQL revision may defer terminal RELEASE. A generic pending
        // source/finance state is NOT enough (T09.6/T10-09 regression).
        // This is one read-only query against the original account SQL owner.
        let settlement_reply_lost = matches!(
            incremental_entry["reason_code"].as_str(),
            Some("RISK_TRANSPORT_LOST_DO_NOT_REPLAY" | "RISK_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
        );
        let original_partial_unacked =
            if i.risk_increase && i.state.terminal() && financial && settlement_reply_lost {
                let id = i.intent_id.clone();
                r.execution
                    .store
                    .call(deadline(), move |s| s.partial_entry_ack_pending(&id))
                    .await
                    .map_err(err)?
            } else {
                false
            };
        let central_release = if original_partial_unacked {
            json!({"state":"pending","reason_code":"ENTRY_PARTIAL_SQL_ACK_PENDING",
                "scope":"intent","next_action":"trade.status"})
        } else if i.state.terminal() && financial {
            match self
                .release_terminal(r, &i, order.as_ref(), venue_flat_verified, invocation)
                .await
            {
                Ok(()) => json!({"state":"verified",
                    "diagnostic_reference":super::TerminalReleaseTrace::reference(&i.intent_id)}),
                Err(failure) => {
                    json!({"state":"pending","reason_code":failure.reason_code,"scope":"intent","next_action":"trade.status",
                        "diagnostic_reference":super::TerminalReleaseTrace::reference(&i.intent_id),
                        "diagnostic":failure})
                }
            }
        } else {
            json!({"state":"not_evaluated"})
        };
        let instrument = i.instrument_id.clone();
        let lots = r
            .execution
            .store
            .call(deadline(), move |s| s.active_lots(&instrument))
            .await
            .map_err(err)?;
        let exposure = lots
            .iter()
            .filter(|l| l.intent_id.as_deref() == Some(&i.intent_id))
            .fold(Decimal::ZERO, |s, l| s + l.remaining_qty);
        let id = i.intent_id.clone();
        let protection = r
            .execution
            .store
            .call(deadline(), move |s| s.protection(&id))
            .await
            .map_err(err)?;
        // Expose the immutable, cursor-covered source ledger for this intent,
        // not an estimate from venue order status or a permission to settle
        // central USD entry spend. Missing coverage remains explicitly blocked.
        let entry_fill_evidence =
            if financial && i.risk_increase {
                let evidence_id = i.intent_id.clone();
                match r
                    .execution
                    .store
                    .call(deadline(), move |s| {
                        let cursor = s.cursor("fills", "SWAP")?.ok_or(
                            okx_execution::StoreError::Invalid("ENTRY_FILL_COVERAGE_UNPROVEN"),
                        )?;
                        s.intent_fill_cost_evidence(&evidence_id, cursor.watermark)
                    })
                    .await
                {
                    Ok(source) => match order.as_ref() {
                        Some(observed) => match verify_venue_fill_coverage(&i, &source, observed) {
                            Ok(()) => {
                                // The independent venue order proves the
                                // filled quantity, not that the venue has
                                // published its final fee bill. Cross-check
                                // each raw fill's fee with the SAME account
                                // finance event linked by a real bill.
                                let bill_id = i.intent_id.clone();
                                let through = source.covered_through;
                                match r
                                    .execution
                                    .store
                                    .call(deadline(), move |s| {
                                        s.intent_fee_bill_evidence(&bill_id, through)
                                    })
                                    .await
                                {
                                    Ok(billed) => json!({
                                        "state":"verified_native_fill_and_bill_fee_sources",
                                        "scope":"USDT_fee_bill_source_link_only",
                                        "final_bill_and_margin_debit_proven":false,
                                        "all_in_entry_cost_proven":false,
                                        "source":billed,
                                    }),
                                    Err(reason) => json!({
                                        "state":"native_fills_verified_bill_pending",
                                        "scope":"USDT_fill_notional_and_fee_only",
                                        "all_in_entry_cost_proven":false,
                                        "reason_code":err(reason),
                                        "source":source,
                                    }),
                                }
                            }
                            Err(reason) => json!({"state":"incomplete",
                                "all_in_entry_cost_proven":false,
                                "reason_code":reason}),
                        },
                        None => json!({"state":"incomplete",
                            "all_in_entry_cost_proven":false,
                            "reason_code":"ENTRY_FILL_VENUE_ORDER_UNAVAILABLE"}),
                    },
                    Err(reason) => json!({"state":"incomplete",
                    "all_in_entry_cost_proven":false,
                    "reason_code":err(reason)}),
                }
            } else {
                json!({"state":"not_evaluated","all_in_entry_cost_proven":false})
            };
        let entry_fill_pending =
            entry_fill_finance_pending(i.risk_increase, verified_zero_cancel, &entry_fill_evidence);
        let settlement_pending = incremental_entry["state"] == "pending"
            || central_release["state"] == "pending"
            || entry_fill_pending;
        // A verified flat position alone cannot retire the durable SQL risk
        // stop. Final remediation requires independently complete finance,
        // Worker release, ZERO native lots and a NEW flat+orphan-free venue
        // readback, committed together by the SQL owner.
        let mut remediation_committed = false;
        let mut protection_remediation = json!({"state":"not_evaluated"});
        if prior_protection_incident
            && venue_flat_verified
            && financial
            && !settlement_pending
            && exposure == Decimal::ZERO
            && central_release["state"] == "verified"
        {
            // Bracket the signed risk-algo census with two account snapshots.
            // A changing position/order during this I/O interval cannot prove
            // stable flat state; a mismatched UID or incomplete algo page denies.
            let first = r.observer.snapshot_until(deadline()).await;
            let algos = r.observer.pending_derivative_risk_algos().await;
            let last = r.observer.snapshot_until(deadline()).await;
            match (first, algos, last) {
                (Ok(first), Ok(algos), Ok(last))
                    if first.account_uid_fingerprint == last.account_uid_fingerprint
                        && first.positions == last.positions
                        && first.pending_orders == last.pending_orders
                        && first
                            .balance
                            .same_values_ignoring_update_times(&last.balance)
                        && verify_instrument_is_flat(
                            &i.instrument_id,
                            last.positions
                                .iter()
                                .map(|p| (p.instrument_id.as_str(), p.position.as_str())),
                        )
                        .is_ok()
                        && algos.complete_within_bound
                        && !algos
                            .samples
                            .iter()
                            .any(|a| a.instrument_id == i.instrument_id) =>
                {
                    let source_digest = digest(&(
                        "protection-flat-source-proof/v1",
                        &i.intent_id,
                        &i.plan_hash,
                        &first.account_uid_fingerprint,
                        &last.account_uid_fingerprint,
                        &algos.samples,
                        &order,
                        &entry_fill_evidence,
                        &central_release,
                        now(),
                    ))?;
                    let id = i.intent_id.clone();
                    let proof = source_digest.clone();
                    let committed = r
                        .execution
                        .store
                        .call(deadline(), move |s| {
                            s.resolve_protection_incident_after_flat(&id, &proof, now())
                        })
                        .await
                        .map_err(err)?;
                    if committed {
                        i = self.find_intent(r, &i.intent_id).await?;
                        remediation_committed = true;
                        protection_remediation = json!({"state":"verified_flat_no_orphans","source_digest":source_digest});
                    }
                }
                _ => {
                    protection_remediation =
                        json!({"state":"pending","reason_code":"FLAT_OR_ALGO_SOURCE_UNVERIFIED"});
                }
            }
        }
        let prior_protection_incident_unresolved =
            protection_incident_still_unresolved(i.incident.as_deref(), remediation_committed);
        let live_protection_incident = protection_failed || prior_protection_incident_unresolved;
        let mut result = completed(
            json!({"intent":i,"remaining_exposure":if financial{Some(exact(exposure))}else{None},
                "entry_costs":entry_costs,
                "entry_fill_evidence":entry_fill_evidence,
                "incremental_entry_settlement":incremental_entry,
                "protection":protection,"central_release":central_release,
                "protection_adjustment":protection_adjustment,
                "protection_remediation":protection_remediation,
                "financial_error":financial_error,"financial_complete":financial && !settlement_pending,
                "protection_incident":live_protection_incident,
                "prior_protection_incident_unresolved":prior_protection_incident_unresolved,
                "protection_source_pending":protection_source_pending,
                "aggregate_protection_blocker":aggregate_protection_blocker,
                "compensation":compensation}),
        );
        result["quality"] = json!({
            "execution":"authenticated_readback",
            "financial":if financial && !settlement_pending {"complete"}else{"incomplete"},
            "entry_settlement":if settlement_pending {"pending"} else {"evaluated"},
            "protection":if live_protection_incident {
                "incident"
            } else if protection_source_pending {
                "pending_source_coherence"
            } else if aggregate_protection_blocker.is_some() {
                "own_verified_cohort_blocked"
            } else {
                "evaluated"
            }
        });
        if !financial
            || settlement_pending
            || protection_source_pending
            || aggregate_protection_blocker.is_some()
            || live_protection_incident
            || matches!(
                protection_adjustment["state"].as_str(),
                Some("blocked" | "ack_source_pending")
            )
        {
            result["status"] = json!("partial");
            result["warnings"] = json!([if live_protection_incident {
                "PROTECTION_INCIDENT_REMEDIATION_UNVERIFIED"
            } else if protection_source_pending {
                "PROTECTION_SOURCE_BEFORE_SQL_FILL"
            } else if aggregate_protection_blocker.is_some() {
                "UNATTRIBUTED_EXCHANGE_POSITION"
            } else if !financial || entry_fill_pending {
                "FINANCIAL_BACKFILL_PENDING"
            } else {
                "ENTRY_SETTLEMENT_PENDING"
            }]);
        }
        Ok(result)
    }
    /// Public Status, Execute rejection and maintenance share this operation.
    pub(super) async fn release_unsent(
        &self,
        r: &AccountResources,
        i: &StoredIntent,
    ) -> Result<unsent_release::UnsentReleaseOutcome, unsent_release::UnsentReleaseError> {
        unsent_release::release_unsent(
            r.configuration.account_key.environment,
            i,
            unsent_release::UnsentReleaseDependencies {
                store: &r.execution.store,
                risk: &self.risk,
                governance: &self.governance,
                sql_deadline: deadline,
            },
        )
        .await
    }
    /// Protect an already opened exact SQL intent independently of the full
    /// financial capture. For a pending entry, cancel its own venue order once
    /// (durable operation claim). For a filled/terminal entry, skip cancel.
    /// In both cases, any reduce uses the SINGLE existing public Close owner,
    /// with source-backed SQL lot quantity and signed venue position. Failure
    /// of those source/identity checks is an explicit bounded blocker.
    async fn emergency_protection_before_finance(
        &self,
        r: &Arc<AccountResources>,
        i: &StoredIntent,
        signed_position: Decimal,
    ) -> Result<Value, String> {
        let current = r
            .execution
            .trade
            .order_by_client_id(&i.instrument_id, &i.client_order_id)
            .await
            .map_err(err)?;
        let mut venue_filled = verified_order_readback(i, &current)?;
        let mut canceled = false;
        if !matches!(
            current.state.as_str(),
            "filled" | "canceled" | "mmp_canceled"
        ) {
            if !matches!(current.state.as_str(), "live" | "partially_filled") {
                return Err("SAFETY_CANCEL_ORDER_STATE_UNVERIFIED".into());
            }
            let permit = r
                .mutation_capacity
                .try_acquire()
                .map_err(|_| "ACCOUNT_MUTATION_BUSY")?;
            let request = CancelOrderRequest {
                instrument_id: i.instrument_id.clone(),
                client_order_id: i.client_order_id.clone(),
            };
            // LOCAL no-send admission precedes the durable possible-POST
            // claim. A timeout AFTER BEGIN must never authorize a retry.
            let timing = r
                .execution
                .clock
                .clock_evidence()
                .await
                .map_err(err)?
                .mutation_timing(5000)
                .map_err(err)?;
            let rate = r
                .execution
                .trade
                .admit_cancel_order(&request)
                .map_err(err)?;
            let key = format!("safety_cancel_{}", &i.client_order_id);
            let hash = digest(&(i.intent_id.clone(), "unprotected_entry_cancel"))?;
            let k = key.clone();
            let h = hash.clone();
            let fresh = r
                .execution
                .store
                .call(deadline(), move |s| {
                    s.begin_operation(&k, &h, "safety_cancel", now())
                })
                .await
                .map_err(err)?;
            if !fresh {
                return Err("SAFETY_CANCEL_OUTCOME_UNKNOWN_NO_RESUBMIT".into());
            }
            let response = r
                .execution
                .trade
                .cancel_order_after_admission(&request, &timing, &rate)
                .await
                .map_err(|_| "SAFETY_CANCEL_OUTCOME_UNKNOWN_NO_RESUBMIT")?;
            if !response.top_level_success()
                || response.data.len() != 1
                || !response.data[0].accepted()
            {
                return Err("SAFETY_CANCEL_NOT_CONFIRMED_NO_RESUBMIT".into());
            }
            let read = r
                .execution
                .trade
                .order_by_client_id(&i.instrument_id, &i.client_order_id)
                .await
                .map_err(err)?;
            venue_filled = verified_order_readback(i, &read)?;
            if !matches!(read.state.as_str(), "filled" | "canceled" | "mmp_canceled") {
                return Err("SAFETY_CANCEL_READBACK_PENDING_NO_RESUBMIT".into());
            }
            r.execution
                .store
                .call(deadline(), move |s| {
                    s.operation_receipt(&key, &hash, "safety_cancel", "TERMINAL_VERIFIED", now())
                })
                .await
                .map_err(err)?;
            drop(permit);
            canceled = true;
        }
        // Signed fill evidence was committed to SQL by the one financial
        // owner. A venue fill or net-mode position by itself is NOT an
        // attributed quantity and must not authorize a reduce.
        let instrument = i.instrument_id.clone();
        let intent_id = i.intent_id.clone();
        let owned_lots = r
            .execution
            .store
            .call(deadline(), move |s| {
                let lots = s.active_lots(&instrument)?;
                Ok(lots
                    .into_iter()
                    .filter(|lot| lot.intent_id.as_deref() == Some(&intent_id))
                    .map(|lot| (lot.side, lot.remaining_qty))
                    .collect::<Vec<_>>())
            })
            .await
            .map_err(err)?;
        let owned = verified_emergency_owned_exposure(
            i,
            venue_filled,
            signed_position,
            owned_lots
                .iter()
                .map(|(side, quantity)| (side.as_str(), *quantity)),
        )?;
        let key = format!("safety_close_{}", &i.client_order_id);
        let outcome = Box::pin(self.close(r, &key, &i.intent_id, 2000, None)).await?;
        Ok(json!({
            "kind":"source_bound_close",
            "intent_id":i.intent_id,
            "signed_position":exact(signed_position),
            "sql_owned_quantity":exact(owned),
            "cancel_verified":canceled,
            "outcome":outcome,
        }))
    }

    pub(super) async fn cancel(
        &self,
        r: &Arc<AccountResources>,
        key: &str,
        reference: &str,
        request: &TradeRequest,
    ) -> Result<Value, String> {
        let capacity = r
            .mutation_capacity
            .try_acquire()
            .map_err(|_| "ACCOUNT_MUTATION_BUSY")?;
        let i = self.find_intent(r, reference).await?;
        let hash = digest(&(r.configuration.account_key.wire(), request))?;
        let op = key.to_owned();
        let h = hash.clone();
        let fresh = r
            .execution
            .store
            .call(deadline(), move |s| {
                s.begin_operation(&op, &h, "cancel", now())
            })
            .await
            .map_err(err)?;
        if !fresh {
            // Status may legitimately clean up a now-flat protective order.
            // Do not hold this account's sole mutation permit across the
            // independent reconciliation and emergency safety path.
            drop(capacity);
            return self.status_intent(r, &i.intent_id, None).await;
        }
        if matches!(i.state, IntentState::Prepared | IntentState::Reserved) {
            let id = i.intent_id.clone();
            r.execution
                .store
                .call(deadline(), move |s| {
                    s.reject_unsent(&id, "USER_CANCELLED_UNSENT", now())
                })
                .await
                .map_err(err)?;
        } else if !i.state.terminal() {
            let timing = r
                .execution
                .clock
                .clock_evidence()
                .await
                .map_err(err)?
                .mutation_timing(5000)
                .map_err(err)?;
            let cancel = CancelOrderRequest {
                instrument_id: i.instrument_id.clone(),
                client_order_id: i.client_order_id.clone(),
            };
            let rate = r.execution.trade.admit_cancel_order(&cancel).map_err(err)?;
            // Cancels only the ordinary exact entry. Exchange-native protection is never canceled by this action.
            match r
                .execution
                .trade
                .cancel_order_after_admission(&cancel, &timing, &rate)
                .await
            {
                Ok(v) if v.top_level_success() && v.data.len() == 1 && v.data[0].accepted() => {}
                _ => return Ok(uncertain(&i.intent_id, "CANCEL_OUTCOME_UNKNOWN")),
            }
        }
        let op = key.to_owned();
        let h = hash;
        let result = i.intent_id.clone();
        r.execution
            .store
            .call(deadline(), move |s| {
                s.operation_receipt(&op, &h, "cancel", &result, now())
            })
            .await
            .map_err(err)?;
        // Cancel ACK is not a terminal financial proof. Reconciliation
        // must run after releasing the cancel mutation lease, so that its
        // independent protection cleanup/emergency owner can acquire it.
        drop(capacity);
        self.status_intent(r, &i.intent_id, None).await
    }
    pub(super) async fn replace(
        &self,
        r: &Arc<AccountResources>,
        request: &TradeRequest,
        governance: Option<&CentralGovernanceSnapshot>,
    ) -> Result<Value, String> {
        let TradeRequest::Execute {
            idempotency_key,
            order,
            instrument,
            strategy_instance,
            side,
            expires_in_ms,
            ..
        } = request
        else {
            return Err("EXECUTE_REQUIRED".into());
        };
        let begun = Instant::now();
        let hash = digest(&(r.configuration.account_key.wire(), request))?;
        let key = idempotency_key.clone();
        if let Some(old) = r
            .execution
            .store
            .call(deadline(), move |s| s.by_idempotency(&key))
            .await
            .map_err(err)?
        {
            if old.request_hash != hash {
                return Err("IDEMPOTENCY_CONFLICT".into());
            }
            return self.status_intent(r, &old.intent_id, None).await;
        }
        let reference = order
            .reference
            .as_ref()
            .ok_or("REPLACE_REFERENCE_REQUIRED")?;
        let target = self.find_intent(r, reference).await?;
        if target.instrument_id != *instrument
            || target.plan.strategy_instance != *strategy_instance
            || target.plan.position_side
                != if *side == PositionSide::Long {
                    "long"
                } else {
                    "short"
                }
        {
            return Err("REPLACE_SCOPE_MISMATCH".into());
        }
        let cancel_key = format!("replace_cancel_{}", &hash[..40]);
        let cancel = TradeRequest::Cancel {
            idempotency_key: cancel_key.clone(),
            reference: reference.clone(),
        };
        let _ = self.cancel(r, &cancel_key, reference, &cancel).await?;
        let after = self.find_intent(r, &target.intent_id).await?;
        if !matches!(after.state, IntentState::Cancelled | IntentState::Rejected) {
            return Ok(uncertain(&after.intent_id, "REPLACE_CANCEL_NOT_TERMINAL"));
        }
        // A timeout, not-found reply or incomplete venue order is UNKNOWN,
        // never evidence of a zero-fill cancel. No second POST on that path.
        let old_order = match r
            .execution
            .trade
            .order_by_client_id(&target.instrument_id, &target.client_order_id)
            .await
        {
            Ok(order) => order,
            Err(_) => {
                return Ok(uncertain(
                    &after.intent_id,
                    "REPLACE_ORDER_READBACK_UNKNOWN_NO_POST",
                ));
            }
        };
        verify_empty_canceled_order_for_replace(&after, &old_order)?;
        let remaining = expires_in_ms.saturating_sub(begun.elapsed().as_millis() as u64);
        if remaining == 0 {
            return Err("COMMAND_EXPIRED".into());
        }
        let mut replacement = request.clone();
        if let TradeRequest::Execute {
            operation,
            expires_in_ms,
            order,
            ..
        } = &mut replacement
        {
            *operation = TradeOperation::Open;
            *expires_in_ms = remaining;
            order.reference = None;
        }
        self.execute_bound(r, &replacement, governance, Some(hash))
            .await
    }
    pub(super) async fn close(
        &self,
        r: &Arc<AccountResources>,
        key: &str,
        reference: &str,
        expires: u64,
        governance: Option<&CentralGovernanceSnapshot>,
    ) -> Result<Value, String> {
        let public = TradeRequest::Close {
            idempotency_key: key.into(),
            reference: reference.into(),
            expires_in_ms: expires,
        };
        let bound_hash = digest(&(r.configuration.account_key.wire(), &public))?;
        let k = key.to_owned();
        if let Some(prior) = r
            .execution
            .store
            .call(deadline(), move |s| s.by_idempotency(&k))
            .await
            .map_err(err)?
        {
            if prior.request_hash != bound_hash {
                return Err("IDEMPOTENCY_CONFLICT".into());
            }
            return self.status_intent(r, &prior.intent_id, None).await;
        }
        let original = self.find_intent(r, reference).await?;
        let instrument = original.instrument_id.clone();
        let lots = r
            .execution
            .store
            .call(deadline(), move |s| s.active_lots(&instrument))
            .await
            .map_err(err)?;
        let mine = lots
            .iter()
            .filter(|l| l.intent_id.as_deref() == Some(&original.intent_id))
            .collect::<Vec<_>>();
        let (side, quantity) = verified_close_lot_exposure(
            mine.iter()
                .map(|lot| (lot.side.as_str(), lot.remaining_qty)),
        )?;
        let command = TradeRequest::Execute {
            idempotency_key: key.into(),
            operation: TradeOperation::Close,
            strategy_instance: original.plan.strategy_instance,
            instrument: original.instrument_id,
            side,
            sizing: Sizing::Contracts {
                contracts: exact(quantity),
            },
            order: OrderConstraints {
                order_type: "ioc".into(),
                limit_price: None,
                reference: None,
                max_entry_usd: None,
                max_slippage_bps: 10,
            },
            protection: None,
            expires_in_ms: expires,
        };
        self.execute_bound(r, &command, governance, Some(bound_hash))
            .await
    }
}

#[cfg(test)]
mod entry_cost_status_tests {
    use super::*;

    #[test]
    fn t10_08_later_live_stop_get_cannot_clear_durable_protection_incident() {
        for reason in [
            "PROTECTION_PENDING_OR_FAILED",
            "MANAGED_AGGREGATE_PROTECTION_UNVERIFIED",
        ] {
            assert!(protection_incident_still_unresolved(Some(reason), false));
            assert!(
                !protection_incident_still_unresolved(Some(reason), true),
                "independently signed flat position is a different proof"
            );
        }
        assert!(!protection_incident_still_unresolved(None, false));
        assert!(!protection_incident_still_unresolved(
            Some("UNRELATED_LOCAL_ADMISSION_DENIAL"),
            false
        ));
    }

    #[tokio::test]
    async fn t10_08_emergency_policy_is_bounded_before_financial_capture() {
        let first =
            bounded_protection_before_finance(Instant::now(), Duration::from_millis(50), async {
                Ok(json!({"kind":"source_bound_close"}))
            })
            .await;
        assert_eq!(first["state"], "attempted");
        assert_eq!(first["action"]["kind"], "source_bound_close");
        assert_eq!(first["phase"], "before_financial_capture");

        // A deferred source/operation cannot consume the whole finance
        // deadline before we report a fail-closed protective blocker.
        let began = Instant::now();
        let blocked = bounded_protection_before_finance(began, Duration::from_millis(25), async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok(json!({"kind":"source_bound_close"}))
        })
        .await;
        assert_eq!(blocked["state"], "blocked");
        assert_eq!(
            blocked["reason_code"],
            "EMERGENCY_ACTION_DEADLINE_OR_OUTCOME_UNKNOWN"
        );
        assert_eq!(blocked["retry_create"], false);
        assert!(began.elapsed() < Duration::from_millis(200));

        let local =
            bounded_protection_before_finance(Instant::now(), Duration::from_millis(50), async {
                Err("ACCOUNT_MUTATION_BUSY".to_owned())
            })
            .await;
        assert_eq!(local["state"], "blocked");
        assert_eq!(local["reason_code"], "ACCOUNT_MUTATION_BUSY");
        assert_eq!(local["retry_create"], false);
    }

    #[test]
    fn t10_08_emergency_close_requires_same_intent_signed_sql_lots() {
        let mut i = StoredIntent {
            intent_id: "intent-a".into(),
            client_order_id: "client-a".into(),
            instrument_id: "BTC-USDT-SWAP".into(),
            plan_hash: "a".repeat(64),
            idempotency_key: "key-a".into(),
            request_hash: "b".repeat(64),
            state: IntentState::Filled,
            revision: 2,
            deadline: 20,
            policy_revision: 1,
            risk_increase: true,
            exchange_order_id: Some("venue-a".into()),
            incident: Some("PROTECTION_PENDING_OR_FAILED".into()),
            plan: okx_execution::IntentPlan {
                quantity: Decimal::ONE,
                side: "buy".into(),
                position_side: "long".into(),
                ..Default::default()
            },
        };
        let one = Decimal::ONE;
        let half = Decimal::new(5, 1);
        assert_eq!(
            verified_emergency_owned_exposure(&i, one, one, [("long", half)]),
            Err("EMERGENCY_EXPOSURE_SQL_VENUE_MISMATCH".into()),
            "a late fill must be in the same intent's SQL lots before Close"
        );
        assert_eq!(
            verified_emergency_owned_exposure(&i, one, one, [("long", one)]),
            Ok(one)
        );
        assert_eq!(
            verified_emergency_owned_exposure(&i, one, one, std::iter::empty()),
            Err("NO_ATTRIBUTED_POSITION_TO_CLOSE".into())
        );
        assert_eq!(
            verified_emergency_owned_exposure(&i, half, one, [("long", one)]),
            Err("EMERGENCY_EXPOSURE_SQL_VENUE_MISMATCH".into())
        );
        assert_eq!(
            verified_emergency_owned_exposure(&i, one, half, [("long", one)]),
            Err("EMERGENCY_EXPOSURE_SQL_VENUE_MISMATCH".into())
        );
        assert_eq!(
            verified_emergency_owned_exposure(&i, one, one, [("short", half)]),
            Err("EMERGENCY_EXPOSURE_SQL_VENUE_MISMATCH".into())
        );
        assert_eq!(
            verified_emergency_owned_exposure(&i, one, Decimal::ZERO, [("long", half)]),
            Err("EMERGENCY_EXPOSURE_SOURCE_UNVERIFIED".into())
        );
        i.plan.position_side = "short".into();
        assert_eq!(
            verified_emergency_owned_exposure(&i, one, one, [("long", half)]),
            Err("EMERGENCY_EXPOSURE_SQL_VENUE_MISMATCH".into())
        );
    }

    #[test]
    fn t09_public_ack_without_fill_bill_evidence_cannot_complete_finance() {
        let missing = json!({"state":"incomplete","reason_code":"ENTRY_FILL_EVIDENCE_EMPTY"});
        let bill_pending = json!({"state":"native_fills_verified_bill_pending"});
        let verified = json!({"state":"verified_native_fill_and_bill_fee_sources"});
        assert!(entry_fill_finance_pending(true, false, &missing));
        assert!(entry_fill_finance_pending(true, false, &bill_pending));
        assert!(!entry_fill_finance_pending(true, false, &verified));
        // A venue-confirmed zero-fill cancellation is a distinct terminal
        // proof, and still needs the separate central_release pending gate.
        assert!(!entry_fill_finance_pending(true, true, &missing));
        assert!(!entry_fill_finance_pending(false, false, &missing));
    }

    #[test]
    fn e2_terminal_cursors_never_trust_invalid_exchange_order_time() {
        assert_eq!(terminal_order_update_time(None), Ok(None));
        let order = |t: &str| {
            serde_json::from_value::<okx_api::TradeOrderDetails>(json!({"uTime":t})).unwrap()
        };
        assert_eq!(terminal_order_update_time(Some(&order("16"))), Ok(Some(16)));
        for invalid in ["", "invalid", "0", "-1", "9223372036854775808"] {
            assert_eq!(
                terminal_order_update_time(Some(&order(invalid))),
                Err("TERMINAL_ORDER_UPDATE_TIME_INVALID".into())
            );
        }
    }

    #[test]
    fn e1_native_fills_require_independent_exact_venue_quantity_and_time() {
        let intent = StoredIntent {
            intent_id: "intent".into(),
            client_order_id: "client".into(),
            instrument_id: "BTC-USDT-SWAP".into(),
            plan_hash: "a".repeat(64),
            idempotency_key: "idem".into(),
            request_hash: "request".into(),
            state: IntentState::Filled,
            revision: 1,
            deadline: 15,
            policy_revision: 1,
            risk_increase: true,
            exchange_order_id: Some("venue-order".into()),
            incident: None,
            plan: okx_execution::IntentPlan {
                quantity: Decimal::ONE,
                side: "buy".into(),
                ..Default::default()
            },
        };
        let source = okx_execution::IntentFillCostEvidence {
            intent_id: intent.intent_id.clone(),
            plan_hash: intent.plan_hash.clone(),
            exchange_order_id: "venue-order".into(),
            reservation_key: "central-key".into(),
            fill_count: 2,
            filled_quantity: Decimal::ONE,
            covered_through: 10,
            gross_notional_usdt: Decimal::from(94),
            conservative_fee_usdt: Decimal::new(3, 2),
            source_digest: "b".repeat(64),
        };
        let order = okx_api::TradeOrderDetails {
            instrument_id: intent.instrument_id.clone(),
            order_id: source.exchange_order_id.clone(),
            client_order_id: intent.client_order_id.clone(),
            side: "buy".into(),
            position_side: "long".into(),
            trade_mode: "cross".into(),
            order_type: "limit".into(),
            price: "100".into(),
            size: "1".into(),
            accumulated_fill_size: "1".into(),
            average_fill_price: "94".into(),
            state: "filled".into(),
            creation_time_ms: "3".into(),
            update_time_ms: "10".into(),
        };
        assert!(verify_venue_fill_coverage(&intent, &source, &order).is_ok());
        assert_eq!(verified_order_readback(&intent, &order), Ok(Decimal::ONE));
        assert_eq!(
            verified_terminal_order_update_time(&intent, Some(&order)),
            Ok(10)
        );
        assert_eq!(
            verified_terminal_order_update_time(&intent, None),
            Err("TERMINAL_ORDER_READBACK_MISSING".into())
        );
        let mut changed = order.clone();
        changed.state = "partially_filled".into();
        assert_eq!(
            verified_terminal_order_update_time(&intent, Some(&changed)),
            Err("TERMINAL_ORDER_STATE_UNVERIFIED".into())
        );
        changed = order.clone();
        changed.order_id = "other".into();
        assert_eq!(
            verified_terminal_order_update_time(&intent, Some(&changed)),
            Err("TERMINAL_ORDER_IDENTITY_MISMATCH".into())
        );
        changed = order.clone();
        changed.client_order_id = "wrong-client".into();
        assert_eq!(
            verified_order_readback(&intent, &changed),
            Err("ORDER_READBACK_IDENTITY_MISMATCH".into())
        );
        changed = order.clone();
        changed.order_id = "other-venue-order".into();
        assert_eq!(
            verified_order_readback(&intent, &changed),
            Err("ORDER_READBACK_IDENTITY_MISMATCH".into())
        );
        changed = order.clone();
        changed.size = "2".into();
        assert_eq!(
            verified_order_readback(&intent, &changed),
            Err("ORDER_READBACK_PLAN_MISMATCH".into())
        );
        changed = order.clone();
        changed.accumulated_fill_size = "1.1".into();
        assert_eq!(
            verified_order_readback(&intent, &changed),
            Err("ORDER_READBACK_FILLED_SIZE_INVALID".into())
        );
        assert_eq!(
            verified_terminal_order_update_time(&intent, Some(&changed)),
            Err("TERMINAL_ORDER_IDENTITY_OR_SIZE_UNVERIFIED".into())
        );
        let mut changed = order.clone();
        changed.accumulated_fill_size = "0.9".into();
        assert_eq!(
            verify_venue_fill_coverage(&intent, &source, &changed),
            Err("ENTRY_FILL_VENUE_QUANTITY_MISMATCH".into())
        );
        changed = order.clone();
        changed.update_time_ms = "11".into();
        assert_eq!(
            verify_venue_fill_coverage(&intent, &source, &changed),
            Err("ENTRY_FILL_VENUE_TIME_UNCOVERED".into())
        );
        changed = order.clone();
        changed.order_id = "other".into();
        assert_eq!(
            verify_venue_fill_coverage(&intent, &source, &changed),
            Err("ENTRY_FILL_VENUE_IDENTITY_MISMATCH".into())
        );
        changed = order.clone();
        changed.client_order_id = "other".into();
        assert_eq!(
            verify_venue_fill_coverage(&intent, &source, &changed),
            Err("ENTRY_FILL_VENUE_IDENTITY_MISMATCH".into())
        );
        changed = order.clone();
        changed.update_time_ms = "invalid".into();
        assert_eq!(
            verify_venue_fill_coverage(&intent, &source, &changed),
            Err("ENTRY_FILL_VENUE_TIME_UNVERIFIED".into())
        );
        changed = order;
        changed.state = "unknown".into();
        assert_eq!(
            verify_venue_fill_coverage(&intent, &source, &changed),
            Err("ENTRY_FILL_VENUE_STATE_UNKNOWN".into())
        );
    }

    #[test]
    fn e2_protective_readback_requires_exact_venue_exposure_and_opposite_side() {
        let one = Decimal::ONE;
        let half = dec("0.5").unwrap();
        assert!(protection_covers_live_position(
            "buy", "sell", one, one, "1"
        ));
        assert!(protection_covers_live_position(
            "sell", "buy", one, one, "1"
        ));
        assert!(!protection_covers_live_position(
            "buy", "buy", one, one, "1"
        ));
        assert!(!protection_covers_live_position(
            "buy", "close", one, one, "1"
        ));
        assert!(!protection_covers_live_position(
            "buy", "sell", one, half, "1"
        ));
        assert!(!protection_covers_live_position(
            "buy", "sell", one, one, "0.5"
        ));
        assert!(!protection_covers_live_position(
            "buy", "sell", one, one, "invalid"
        ));
        assert!(!protection_covers_live_position(
            "buy",
            "sell",
            Decimal::ZERO,
            one,
            "1"
        ));
    }

    #[test]
    fn t09_4_add_attached_stop_must_cover_exact_increment_not_old_position() {
        let add = dec("0.2").unwrap();
        let net = dec("0.5").unwrap();
        assert!(protection_covers_managed_increment(
            "buy", "sell", add, net, "0.2"
        ));
        assert!(!protection_covers_managed_increment(
            "buy", "sell", add, net, "0.5"
        ));
        assert!(!protection_covers_managed_increment(
            "buy", "sell", add, net, "0.19"
        ));
        assert!(!protection_covers_managed_increment(
            "buy", "buy", add, net, "0.2"
        ));
        assert!(!protection_covers_managed_increment(
            "buy",
            "sell",
            Decimal::ZERO,
            net,
            "0.2"
        ));
        assert!(!protection_covers_managed_increment(
            "buy",
            "sell",
            dec("0.6").unwrap(),
            net,
            "0.6"
        ));
    }

    #[test]
    fn t10_09_position_ahead_of_sql_lot_requires_signed_old_stop_and_stays_pending() {
        // A causal partial Reduce can appear in venue positions before its
        // immutable fill/bill updates the SQL lot. No new protection defect
        // is proved by the difference alone; a missing or wrong stop still
        // fails closed and the existing durable incident is never cleared.
        let intent = StoredIntent {
            intent_id: "original".into(),
            client_order_id: "c".repeat(32),
            instrument_id: "BTC-USDT-SWAP".into(),
            plan_hash: "a".repeat(64),
            idempotency_key: "original-key".into(),
            request_hash: "original-request".into(),
            state: IntentState::Cancelled,
            revision: 3,
            deadline: 100,
            policy_revision: 1,
            risk_increase: true,
            exchange_order_id: Some("original-order".into()),
            incident: None,
            plan: okx_execution::IntentPlan {
                side: "buy".into(),
                position_side: "long".into(),
                quantity: dec("0.1").unwrap(),
                stop_price: Some(dec("900").unwrap()),
                ..Default::default()
            },
        };
        let original = okx_api::TradeAlgoOrderDetails {
            instrument_id: intent.instrument_id.clone(),
            algo_order_id: "900000004".into(),
            client_order_id: format!("s{}", "c".repeat(31)),
            state: "live".into(),
            stop_loss_trigger_price_type: "mark".into(),
            stop_loss_trigger_price: "900".into(),
            size: "0.04".into(),
            side: "sell".into(),
            position_side: "long".into(),
            ..serde_json::from_value::<okx_api::TradeAlgoOrderDetails>(json!({})).unwrap()
        };
        let sql_lot = dec("0.04").unwrap();
        let newer_venue_position = dec("0.02").unwrap();
        assert!(signed_position_below_sql_lot_with_live_old_stop(
            &intent,
            sql_lot,
            newer_venue_position,
            &original
        ));
        assert!(!protection_covers_managed_increment(
            "buy",
            "sell",
            sql_lot,
            newer_venue_position,
            &original.size
        ));
        for bad_size in ["0.02", "0.05", "invalid"] {
            let mut invalid = original.clone();
            invalid.size = bad_size.into();
            assert!(!signed_position_below_sql_lot_with_live_old_stop(
                &intent,
                sql_lot,
                newer_venue_position,
                &invalid
            ));
        }
        let mut missing = original.clone();
        missing.state = "canceled".into();
        assert!(!signed_position_below_sql_lot_with_live_old_stop(
            &intent,
            sql_lot,
            newer_venue_position,
            &missing
        ));
        let mut alien = original.clone();
        alien.client_order_id = "other-account-stop".into();
        assert!(!signed_position_below_sql_lot_with_live_old_stop(
            &intent,
            sql_lot,
            newer_venue_position,
            &alien
        ));
        assert!(!signed_position_below_sql_lot_with_live_old_stop(
            &intent,
            sql_lot,
            Decimal::ZERO,
            &original
        ));
        assert!(!signed_position_below_sql_lot_with_live_old_stop(
            &intent, sql_lot, sql_lot, &original
        ));
        assert!(!signed_position_below_sql_lot_with_live_old_stop(
            &intent,
            sql_lot,
            dec("0.05").unwrap(),
            &original
        ));
    }

    #[test]
    fn e2_shared_protection_check_is_strict_about_live_venue_state_and_size() {
        let intent = StoredIntent {
            intent_id: "intent".into(),
            client_order_id: "c".repeat(32),
            instrument_id: "BTC-USDT-SWAP".into(),
            plan_hash: "a".repeat(64),
            idempotency_key: "key".into(),
            request_hash: "req".into(),
            state: IntentState::Filled,
            revision: 1,
            deadline: 100,
            policy_revision: 1,
            risk_increase: true,
            exchange_order_id: Some("order".into()),
            incident: None,
            plan: okx_execution::IntentPlan {
                side: "buy".into(),
                position_side: "long".into(),
                quantity: Decimal::ONE,
                stop_price: Some(dec("96").unwrap()),
                ..Default::default()
            },
        };
        let p = okx_api::TradeAlgoOrderDetails {
            instrument_id: "BTC-USDT-SWAP".into(),
            algo_order_id: "algo".into(),
            client_order_id: format!("s{}", "c".repeat(31)),
            state: "live".into(),
            stop_loss_trigger_price_type: "mark".into(),
            stop_loss_trigger_price: "96".into(),
            size: "1".into(),
            side: "sell".into(),
            position_side: "net".into(),
            ..serde_json::from_value::<okx_api::TradeAlgoOrderDetails>(json!({})).unwrap()
        };
        assert!(verified_live_protection(&intent, Decimal::ONE, &p).is_ok());
        let mut changed = p.clone();
        changed.size = "0.5".into();
        assert_eq!(
            verified_live_protection(&intent, Decimal::ONE, &changed),
            Err("PROTECTION_LIVE_SOURCE_UNVERIFIED".into())
        );
        changed = p.clone();
        changed.state = "pause".into();
        assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_err());
        changed = p.clone();
        changed.side = "buy".into();
        assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_err());
        changed = p.clone();
        changed.stop_loss_trigger_price_type = "last".into();
        assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_err());
        changed = p.clone();
        changed.stop_loss_trigger_price = "95".into();
        assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_err());
        changed = p.clone();
        changed.client_order_id = "wrong".into();
        assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_err());
        changed = p.clone();
        changed.position_side = "short".into();
        assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_err());
        changed = p.clone();
        changed.position_side.clear();
        assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_err());
        changed = p.clone();
        changed.position_side = "long".into();
        assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_ok());
        changed = p.clone();
        changed.failure_code = "failure".into();
        assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_err());
        changed = p.clone();
        changed.algo_order_id.clear();
        assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_err());
        assert!(verified_live_protection(&intent, Decimal::ZERO, &p).is_err());
        let mut short_client = intent;
        short_client.client_order_id = "short".into();
        assert_eq!(
            verified_live_protection(&short_client, Decimal::ONE, &p),
            Err("PROTECTION_CLIENT_ID_INVALID".into())
        );
    }

    #[test]
    fn e2_position_source_does_not_treat_malformed_exposure_as_flat() {
        assert_eq!(
            verified_position_quantity(["0", "0"].into_iter()),
            Ok(Decimal::ZERO)
        );
        assert_eq!(
            verified_position_quantity(["0.5", "-0.25"].into_iter()),
            Ok(dec("0.75").unwrap())
        );
        assert_eq!(
            verified_position_quantity(["0", "unreadable"].into_iter()),
            Err("POSITION_QUANTITY_UNVERIFIED".into())
        );
        assert_eq!(
            verified_position_quantity(["79228162514264337593543950335", "1"].into_iter()),
            Err("POSITION_QUANTITY_OVERFLOW".into())
        );
    }

    #[test]
    fn e2_position_direction_is_identical_for_net_status_census_and_recovery() {
        let one = Decimal::ONE;
        let half = dec("0.5").unwrap();
        assert_eq!(
            verified_venue_position_direction("net", "1"),
            Ok(Some(("long", one)))
        );
        assert_eq!(
            verified_venue_position_direction("net", "-0.5"),
            Ok(Some(("short", half)))
        );
        assert_eq!(
            verified_intent_position_quantity("long", [("net", "0.25"), ("net", "0.75")]),
            Ok(one)
        );
        assert_eq!(
            verified_intent_position_quantity("short", [("net", "-0.5")]),
            Ok(half)
        );
        assert_eq!(
            verified_intent_position_quantity("long", [("net", "-0.5")]),
            Err("VENUE_POSITION_OPPOSITE_EXPOSURE_UNVERIFIED".into())
        );
        assert_eq!(
            verified_intent_position_quantity("short", [("long", "0")]),
            Ok(Decimal::ZERO)
        );
        assert_eq!(
            verified_venue_position_direction("long", "1"),
            Ok(Some(("long", one)))
        );
        assert_eq!(
            verified_venue_position_direction("short", "0.5"),
            Ok(Some(("short", half)))
        );
        for side in ["long", "short", "net"] {
            assert_eq!(verified_venue_position_direction(side, "0"), Ok(None));
        }
        assert_eq!(
            verified_venue_position_direction("long", "-1"),
            Err("VENUE_POSITION_SIDE_QUANTITY_MISMATCH".into())
        );
        assert_eq!(
            verified_venue_position_direction("unknown", "0"),
            Err("VENUE_POSITION_SIDE_UNVERIFIED".into())
        );
        assert_eq!(
            verified_venue_position_direction("unknown", "1"),
            Err("VENUE_POSITION_SIDE_UNVERIFIED".into())
        );
        assert_eq!(
            verified_venue_position_direction("net", "bad"),
            Err("VENUE_POSITION_QUANTITY_UNVERIFIED".into())
        );
    }

    #[test]
    fn e2_cleanup_does_not_remove_protection_for_any_unverified_hedge_side() {
        let instrument = "BTC-USDT-SWAP";
        assert!(
            verify_instrument_is_flat(
                instrument,
                [
                    (instrument, "0"),
                    (instrument, "0"),
                    ("ETH-USDT-SWAP", "20"),
                ]
            )
            .is_ok()
        );
        assert_eq!(
            verify_instrument_is_flat(instrument, [(instrument, "0"), (instrument, "-0.2"),]),
            Err("PROTECTION_STILL_REQUIRED".into())
        );
        assert_eq!(
            verify_instrument_is_flat(instrument, [(instrument, "unreadable")]),
            Err("POSITION_QUANTITY_UNVERIFIED".into())
        );
        assert_eq!(
            verify_instrument_is_flat(
                instrument,
                [
                    (instrument, "79228162514264337593543950335"),
                    (instrument, "1"),
                ]
            ),
            Err("POSITION_QUANTITY_OVERFLOW".into())
        );
    }

    #[test]
    fn e2_replace_requires_independent_terminal_zero_fill_order_readback() {
        let intent = StoredIntent {
            intent_id: "intent".into(),
            client_order_id: "client".into(),
            instrument_id: "BTC-USDT-SWAP".into(),
            plan_hash: "a".repeat(64),
            idempotency_key: "idem".into(),
            request_hash: "request".into(),
            state: IntentState::Cancelled,
            revision: 2,
            deadline: 40,
            policy_revision: 1,
            risk_increase: true,
            exchange_order_id: Some("venue".into()),
            incident: None,
            plan: okx_execution::IntentPlan {
                quantity: Decimal::ONE,
                side: "buy".into(),
                ..Default::default()
            },
        };
        let order = okx_api::TradeOrderDetails {
            instrument_id: intent.instrument_id.clone(),
            order_id: "venue".into(),
            client_order_id: "client".into(),
            side: "buy".into(),
            position_side: "long".into(),
            trade_mode: "cross".into(),
            order_type: "limit".into(),
            price: "100".into(),
            size: "1".into(),
            accumulated_fill_size: "0".into(),
            average_fill_price: "".into(),
            state: "canceled".into(),
            creation_time_ms: "1".into(),
            update_time_ms: "2".into(),
        };
        assert!(verify_empty_canceled_order_for_replace(&intent, &order).is_ok());
        let mut partial = order.clone();
        partial.accumulated_fill_size = "0.1".into();
        assert_eq!(
            verify_empty_canceled_order_for_replace(&intent, &partial),
            Err("REPLACE_PARTIAL_FILL_REQUIRES_EXPLICIT_CLOSE".into())
        );
        let mut nonterminal = order.clone();
        nonterminal.state = "live".into();
        assert_eq!(
            verify_empty_canceled_order_for_replace(&intent, &nonterminal),
            Err("REPLACE_CANCELED_READBACK_UNPROVEN".into())
        );
        let mut wrong_id = order.clone();
        wrong_id.order_id = "other".into();
        assert_eq!(
            verify_empty_canceled_order_for_replace(&intent, &wrong_id),
            Err("REPLACE_CANCELED_READBACK_UNPROVEN".into())
        );
        let mut missing_time = order.clone();
        missing_time.update_time_ms = "0".into();
        assert_eq!(
            verify_empty_canceled_order_for_replace(&intent, &missing_time),
            Err("REPLACE_CANCELED_READBACK_UNPROVEN".into())
        );
        let mut malformed = order;
        malformed.accumulated_fill_size = "unreadable".into();
        assert_eq!(
            verify_empty_canceled_order_for_replace(&intent, &malformed),
            Err("REPLACE_CANCELED_READBACK_UNPROVEN".into())
        );
    }

    #[test]
    fn e2_close_lots_require_one_side_positive_amounts_and_exact_sum() {
        assert_eq!(
            verified_close_lot_exposure([
                ("long", dec("0.2").unwrap()),
                ("long", dec("0.3").unwrap())
            ])
            .map(|(_, quantity)| quantity),
            Ok(dec("0.5").unwrap())
        );
        assert!(verified_close_lot_exposure([("short", Decimal::ONE)]).is_ok());
        assert_eq!(
            verified_close_lot_exposure(std::iter::empty()),
            Err("NO_ATTRIBUTED_POSITION_TO_CLOSE".into())
        );
        assert_eq!(
            verified_close_lot_exposure([("long", Decimal::ONE), ("short", Decimal::ONE)]),
            Err("CLOSE_LOT_MIXED_DIRECTIONS".into())
        );
        assert_eq!(
            verified_close_lot_exposure([("long", Decimal::ZERO)]),
            Err("CLOSE_LOT_QUANTITY_UNVERIFIED".into())
        );
        assert_eq!(
            verified_close_lot_exposure([("long", -Decimal::ONE)]),
            Err("CLOSE_LOT_QUANTITY_UNVERIFIED".into())
        );
        assert_eq!(
            verified_close_lot_exposure([("unknown", Decimal::ONE)]),
            Err("CLOSE_LOT_SIDE_UNATTESTED".into())
        );
        assert_eq!(
            verified_close_lot_exposure([("long", Decimal::MAX), ("long", Decimal::ONE)]),
            Err("CLOSE_LOT_QUANTITY_OVERFLOW".into())
        );
    }

    #[test]
    fn e1_status_never_promotes_known_quote_into_all_in_entry_permission() {
        let empty = entry_cost_status(None, None).unwrap();
        assert_eq!(empty["all_in_entry_cost_proven"], false);
        assert!(empty["entry_budget_upper_reporting"].is_null());
        assert_eq!(empty["reason_codes"][0], "ENTRY_QUOTE_NOT_RECORDED");
        let quote = okx_execution::EntryQuoteEvidence {
            schema: "okx.entry-known-quote/v1".into(),
            account_key: "okx:real:test".into(),
            installation_id: "install".into(),
            intent_id: "intent".into(),
            plan_hash: "a".repeat(64),
            policy_revision: 1,
            instrument_id: "BTC-USDT-SWAP".into(),
            quantity: dec("0.01").unwrap(),
            limit_price: dec("100").unwrap(),
            contract_value: Decimal::ONE,
            fee_rate: dec("0.0005").unwrap(),
            maker_rate: "-0.0002".into(),
            taker_rate: "0.0005".into(),
            fee_level: "Lv1".into(),
            fee_exchange_at: 1,
            usd_per_usdt: Decimal::ONE,
            notional_usdt: Decimal::ONE,
            fee_usdt: dec("0.0005").unwrap(),
            total_usdt: dec("1.0005").unwrap(),
            total_usd: dec("1.0005").unwrap(),
            fee_generation: format!("sha256:{}", "b".repeat(64)),
            reference_generation: "ref".into(),
            fee_received_at: 1,
            conversion_digest: "c".repeat(64),
            conversion_received_at: 1,
            observed_at: 1,
        };
        let view = entry_cost_status(Some(quote.clone()), None).unwrap();
        assert_eq!(view["state"], "historical_known_cost_only");
        assert_eq!(view["known_entry_total_usd"], "1.0005");
        assert_eq!(view["all_in_entry_cost_proven"], false);
        assert!(view["entry_budget_upper_reporting"].is_null());
        assert_eq!(view["reason_codes"].as_array().unwrap().len(), 5);
        let admitted = okx_execution::EntryAdmissionEvidence {
            schema: "okx.entry-admission/v2".into(),
            account_key: quote.account_key.clone(),
            installation_id: quote.installation_id.clone(),
            intent_id: quote.intent_id.clone(),
            plan_hash: quote.plan_hash.clone(),
            quote_digest: "c".repeat(64),
            admission_usd_per_usdt: Decimal::ONE,
            other_entry_costs_upper_usd: Decimal::ONE,
            entry_upper_usd: Decimal::new(20005, 4),
            entry_proof_digest: "d".repeat(64),
            observed_at: quote.observed_at,
        };
        let shown = entry_cost_status(Some(quote.clone()), Some(admitted.clone())).unwrap();
        assert_eq!(shown["all_in_entry_cost_proven"], true);
        assert_eq!(shown["entry_budget_upper_reporting"], "2.0005");
        assert_eq!(shown["authorization"], "COST_PROOF_ONLY_NOT_POST_AUTHORITY");
        let mut mismatched = admitted;
        mismatched.plan_hash = "different".into();
        assert_eq!(
            entry_cost_status(Some(quote.clone()), Some(mismatched)).unwrap_err(),
            "ENTRY_ADMISSION_READBACK_MISMATCH"
        );
        let mut tampered = quote;
        tampered.total_usd = Decimal::ONE;
        assert_eq!(
            entry_cost_status(Some(tampered), None).unwrap_err(),
            "ENTRY_QUOTE_READBACK_MISMATCH"
        );
    }
}

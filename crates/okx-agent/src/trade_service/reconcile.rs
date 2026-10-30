use super::*;
use okx_api::CancelOrderRequest;
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
fn terminal_order_update_time(
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
fn verify_instrument_is_flat<'a>(
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

/// Every venue readback must refer to this durable intent before it can
/// change local state, protection requirements, or exchange-order identity.
/// An exchange lookup by client ID alone is a lookup, not an identity proof.
fn verified_order_readback(
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
fn verified_terminal_order_update_time(
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
    pub(super) async fn status_intent(
        &self,
        r: &Arc<AccountResources>,
        reference: &str,
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
        let mut protection_failed = false;
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
                Ok(()) => json!({"state":"verified","proof":"SQL_NO_SEND"}),
                Err(reason) => json!({
                    "state":"pending","reason_code":reason,
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
        let order = if matches!(i.state, IntentState::Rejected) && i.exchange_order_id.is_none() {
            None
        } else {
            match r
                .execution
                .trade
                .order_by_client_id(&i.instrument_id, &i.client_order_id)
                .await
            {
                Ok(o) => Some(o),
                Err(_) => {
                    return Ok(uncertain(
                        &i.intent_id,
                        "ORDER_READBACK_INCOMPLETE_NO_RESUBMIT",
                    ));
                }
            }
        };
        if let Some(o) = &order {
            let state = match o.state.as_str() {
                "live" => IntentState::Acknowledged,
                "partially_filled" => IntentState::PartiallyFilled,
                "filled" => IntentState::Filled,
                "canceled" | "mmp_canceled" => IntentState::Cancelled,
                _ => return Ok(uncertain(&i.intent_id, "EXCHANGE_ORDER_STATE_UNKNOWN")),
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
            let snapshot = r.observer.snapshot().await.map_err(err)?;
            let position_qty = verified_intent_position_quantity(
                &i.plan.position_side,
                snapshot
                    .positions
                    .iter()
                    .filter(|p| p.instrument_id == i.instrument_id)
                    .map(|p| (p.position_side.as_str(), p.position.as_str())),
            )?;
            let exposed = position_qty > Decimal::ZERO;
            if !exposed && i.risk_increase {
                self.cleanup_flat_protection(r, &i).await?;
            }
            if i.risk_increase && exposed {
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
                        if verified_live_protection(&i, position_qty, &p).is_ok()
                            && protection_covers_live_position(
                                &i.plan.side,
                                &p.side,
                                filled,
                                position_qty,
                                &p.size,
                            ) =>
                    {
                        let source_digest = digest(&p)?;
                        let protection = okx_execution::ProtectionReceipt {
                            intent_id: i.intent_id.clone(),
                            algo_id: p.algo_order_id,
                            side: p.side,
                            quantity: position_qty,
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
                    }
                    _ => {
                        protection_failed = true;
                        let id = i.intent_id.clone();
                        r.execution
                            .store
                            .call(deadline(), move |s| {
                                s.protection_incident(&id, "PROTECTION_PENDING_OR_FAILED", now())
                            })
                            .await
                            .map_err(err)?;
                    }
                }
            }
        }
        // Capture money before releasing capacity; incomplete source pages never become zero costs.
        let financial_capture = crate::source_capture::capture_account_sources(
            &r.observer,
            &r.private_ws,
            &r.execution,
        )
        .await;
        let financial_error = financial_capture.as_ref().err().map(ToString::to_string);
        let financial = financial_capture.is_ok();
        let compensation = if protection_failed && financial {
            Some(Box::pin(self.emergency_exit(r, &i)).await?)
        } else {
            None
        };
        let central_release = if i.state.terminal() && financial {
            match self.release_terminal(r, &i, order.as_ref()).await {
                Ok(()) => json!({"state":"verified"}),
                Err(reason) => {
                    json!({"state":"pending","reason_code":reason,"scope":"intent","next_action":"trade.status"})
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
        let mut result = completed(
            json!({"intent":i,"remaining_exposure":if financial{Some(exact(exposure))}else{None},
                "entry_costs":entry_costs,
                "entry_fill_evidence":entry_fill_evidence,
                "protection":protection,"central_release":central_release,
                "financial_error":financial_error,"financial_complete":financial,
                "protection_incident":protection_failed,"compensation":compensation}),
        );
        result["quality"] = json!({"execution":"authenticated_readback","financial":if financial{"complete"}else{"incomplete"},"protection":if protection_failed{"incident"}else{"evaluated"}});
        if !financial {
            result["status"] = json!("partial");
            result["warnings"] = json!(["FINANCIAL_BACKFILL_PENDING"]);
        }
        Ok(result)
    }
    /// Retryable in the existing 1-second maintenance loop. INSPECT resolves
    /// lost central ACKs; a missing/rejected SQL witness never releases budget.
    pub(super) async fn release_unsent(
        &self,
        r: &AccountResources,
        i: &StoredIntent,
    ) -> Result<(), String> {
        if r.configuration.account_key.environment != okx_protocol::accounts::Environment::Real {
            return Ok(());
        }
        let id = i.intent_id.clone();
        let proof = r
            .execution
            .store
            .call(deadline(), move |s| s.unsent_release_receipt(&id))
            .await
            .map_err(err)?
            .ok_or("DURABLE_UNSENT_SQL_PROOF_MISSING")?;
        let state = self
            .governance
            .read()
            .await
            .clone()
            .ok_or("AUTHORITY_UNAVAILABLE")?;
        let reservation = proof.reservation_key.clone();
        if reservation.starts_with("local:") {
            return Ok(());
        }
        for _ in 0..4 {
            let observed = self
                .risk
                .request(self.risk_command(
                    &state,
                    format!("inspect_{}", i.client_order_id),
                    RiskCommandAction::Inspect {
                        reservation_key: reservation.clone(),
                        plan_sha256: proof.plan_hash.clone(),
                    },
                ))
                .await
                .map_err(str::to_owned)?;
            let absent = observed.status == RiskDecisionStatus::Denied
                && observed.reason.as_deref() == Some("RISK_RESERVATION_NOT_FOUND");
            if observed.status != RiskDecisionStatus::Observed && !absent {
                return Err(observed
                    .reason
                    .unwrap_or("CENTRAL_INSPECTION_FAILED".into()));
            }
            if observed.reservation_state == Some(RiskReservationState::Released) {
                let evidence = digest(&observed)?;
                let reservation = reservation.clone();
                r.execution
                    .store
                    .call(deadline(), move |s| {
                        s.acknowledge_central_release(&reservation, &evidence)
                    })
                    .await
                    .map_err(err)?;
                return Ok(());
            }
            if !absent
                && !matches!(
                    observed.reservation_state,
                    Some(RiskReservationState::Held | RiskReservationState::Posted)
                )
            {
                return Err("UNSENT_CENTRAL_STATE_UNRESOLVED".into());
            }
            let native_proof = UnsentRiskReleaseProof {
                reservation_key: proof.reservation_key.clone(),
                account_key: proof.account_key.clone(),
                intent_id: proof.intent_id.clone(),
                plan_hash: proof.plan_hash.clone(),
                request_hash: proof.request_hash.clone(),
                reason: proof.reason.clone(),
                record_digest: digest(&proof)?,
            };
            let result = self
                .risk
                .request(self.risk_command(
                    &state,
                    format!("release_{}", i.client_order_id),
                    RiskCommandAction::ReleaseUnsent {
                        reservation_key: reservation.clone(),
                        plan_sha256: proof.plan_hash.clone(),
                        expected_risk_book_sequence:
                            observed.risk_book_sequence.ok_or("RISK_SEQUENCE_MISSING")?,
                        proof: native_proof,
                    },
                ))
                .await
                .map_err(str::to_owned)?;
            if result.status == RiskDecisionStatus::Updated {
                let evidence = digest(&result)?;
                let reservation = reservation.clone();
                r.execution
                    .store
                    .call(deadline(), move |s| {
                        s.acknowledge_central_release(&reservation, &evidence)
                    })
                    .await
                    .map_err(err)?;
                return Ok(());
            }
            if result.reason.as_deref() != Some("STALE_SESSION_OR_BOOK_SEQUENCE") {
                return Err(result
                    .reason
                    .unwrap_or("CENTRAL_UNSENT_RELEASE_DENIED".into()));
            }
        }
        Err("CENTRAL_UNSENT_RELEASE_CAS_BUSY".into())
    }
    async fn release_terminal(
        &self,
        r: &AccountResources,
        i: &StoredIntent,
        order: Option<&okx_api::TradeOrderDetails>,
    ) -> Result<(), String> {
        if r.configuration.account_key.environment != okx_protocol::accounts::Environment::Real {
            return Ok(());
        }
        let state = self
            .governance
            .read()
            .await
            .clone()
            .ok_or("AUTHORITY_UNAVAILABLE")?;
        let id = i.intent_id.clone();
        let receipt = r
            .execution
            .store
            .call(deadline(), move |s| s.central_receipt(&id))
            .await
            .map_err(err)?;
        let Some(receipt) = receipt else {
            // Rejected PRE-RESERVE / centrally DENIED intents are handled by
            // their independent SQL witnesses before terminal reconciliation.
            // For any risk-increasing terminal intent with a possible POST,
            // absent local central receipt does not prove released capacity.
            return if i.risk_increase {
                Err("TERMINAL_CENTRAL_RESERVATION_UNVERIFIED".into())
            } else {
                Ok(())
            };
        };
        if receipt.reservation_key.starts_with("local:") {
            return Ok(());
        }
        let instrument = i.instrument_id.clone();
        let lots = r
            .execution
            .store
            .call(deadline(), move |s| s.active_lots(&instrument))
            .await
            .map_err(err)?;
        if lots.iter().any(|l| {
            l.intent_id.as_deref() == Some(&i.intent_id) && l.remaining_qty > Decimal::ZERO
        }) {
            return Err("INTENT_EXPOSURE_REMAINS".into());
        }
        let partition = if i.instrument_id.ends_with("-SWAP") {
            "SWAP"
        } else {
            "FUTURES"
        };
        let complete = r
            .execution
            .store
            .call(deadline(), move |s| s.cursor("fills", partition))
            .await
            .map_err(err)?
            .ok_or("FILL_COVERAGE_MISSING")?;
        let venue_updated_at = verified_terminal_order_update_time(i, order)?;
        if !complete.coverage_gaps.is_empty() || complete.watermark < venue_updated_at {
            return Err("FILL_COVERAGE_INCOMPLETE".into());
        }
        let observed = self
            .risk
            .request(self.risk_command(
                &state,
                format!("inspect_{}", &i.client_order_id),
                RiskCommandAction::Inspect {
                    reservation_key: receipt.reservation_key.clone(),
                    plan_sha256: i.plan_hash.clone(),
                },
            ))
            .await
            .map_err(str::to_owned)?;
        if observed.reservation_state == Some(RiskReservationState::Released) {
            let k = receipt.reservation_key.clone();
            let e = digest(&observed)?;
            r.execution
                .store
                .call(deadline(), move |s| s.acknowledge_central_release(&k, &e))
                .await
                .map_err(err)?;
            return Ok(());
        }
        let (through, loss) = r
            .execution
            .store
            .call(deadline(), |s| {
                Ok((
                    s.cursor("bills", "account")?
                        .ok_or(okx_execution::StoreError::Invalid("BILLS_COVERAGE_MISSING"))?,
                    s.policy()?.4,
                ))
            })
            .await
            .map_err(err)?;
        if !through.cursor.is_empty()
            || !through.coverage_gaps.is_empty()
            || through.watermark < venue_updated_at
        {
            return Err("FINANCIAL_COVERAGE_INCOMPLETE".into());
        }
        // A terminal POST may free entry capacity only after the independent
        // venue order and all immutable native fill/fee bills agree. The
        // SQL owner commits the exact final cumulative proof BEFORE any WSS
        // SETTLE_ENTRY. A lost central reply reuses that same receipt.
        let venue_order = order.ok_or("TERMINAL_ENTRY_VENUE_ORDER_UNAVAILABLE")?;
        if !matches!(
            venue_order.state.as_str(),
            "filled" | "canceled" | "mmp_canceled"
        ) {
            return Err("TERMINAL_ENTRY_VENUE_STATE_UNVERIFIED".into());
        }
        let filled = verified_order_readback(i, venue_order)?;
        if filled <= Decimal::ZERO {
            // A zero-fill release needs its own independently verified
            // no-fee proof; do not treat absence of fills as zero debit.
            return Err("TERMINAL_ZERO_FILL_ENTRY_FEES_UNPROVEN".into());
        }
        let id = i.intent_id.clone();
        let settlement = r
            .execution
            .store
            .call(deadline(), move |s| {
                s.bind_terminal_entry_settlement(&id, venue_updated_at, filled, through.watermark)
            })
            .await
            .map_err(err)?;
        if settlement.reservation_key != receipt.reservation_key
            || settlement.plan_hash != i.plan_hash
            || settlement.venue_order_id != venue_order.order_id
        {
            return Err("TERMINAL_ENTRY_SETTLEMENT_IDENTITY_MISMATCH".into());
        }
        let settled = self
            .risk
            .request(self.risk_command(
                &state,
                format!("settle_{}", i.client_order_id),
                RiskCommandAction::SettleEntry {
                    reservation_key: settlement.reservation_key.clone(),
                    plan_sha256: settlement.plan_hash.clone(),
                    expected_risk_book_sequence:
                        observed.risk_book_sequence.ok_or("RISK_SEQUENCE_MISSING")?,
                    cumulative_entry_usd: exact(settlement.cumulative_entry_usd),
                    settlement_revision: settlement.settlement_revision,
                    source_digest: settlement.source_digest.clone(),
                },
            ))
            .await
            .map_err(str::to_owned)?;
        if settled.status != RiskDecisionStatus::Updated {
            return Err(settled
                .reason
                .unwrap_or("CENTRAL_ENTRY_SETTLEMENT_PENDING".into()));
        }
        let settlement_sequence = settled
            .risk_book_sequence
            .ok_or("ENTRY_SETTLEMENT_SEQUENCE_MISSING")?;
        let census = self.census(&state).await?;
        let proof = TerminalRiskLedgerProof {
            account_uid: r.configuration.account_key.verified_uid.clone(),
            run_id: "continuous_engine".into(),
            intent_id: i.intent_id.clone(),
            state: match i.state {
                IntentState::Rejected => "REJECTED",
                IntentState::Cancelled => "CANCELED",
                _ => "FILLED",
            }
            .into(),
            record_digest: digest(&(i, order, loss, through))?,
            cumulative_loss_usd: exact(loss),
            remaining_exposure_usdt: "0".into(),
            // The previous SETTLE_ENTRY committed the entire observed
            // amount. Terminal RELEASE must not create another revision or
            // claim a new debit; Worker compares these exact immutable
            // SQL-backed fields to the existing settled RiskBook entry.
            cumulative_entry_usd: Some(exact(settlement.cumulative_entry_usd)),
            entry_settlement_revision: Some(settlement.settlement_revision),
            entry_settlement_digest: Some(settlement.source_digest.clone()),
        };
        let command = self.risk_command(
            &state,
            format!("release_{}", &i.client_order_id),
            RiskCommandAction::Resolve {
                reservation_key: receipt.reservation_key.clone(),
                plan_sha256: i.plan_hash.clone(),
                expected_risk_book_sequence: settlement_sequence,
                next_state: RiskResolution::Released,
                source_proof_sha256: digest(&proof)?,
                terminal_census: Some(census),
                terminal_ledger: Some(proof),
            },
        );
        let reservation = receipt.reservation_key.clone();
        let result = self.risk.request(command).await.map_err(str::to_owned)?;
        if result.status != RiskDecisionStatus::Updated {
            return Err(result.reason.unwrap_or("CENTRAL_RELEASE_DENIED".into()));
        }
        let e = digest(&result)?;
        r.execution
            .store
            .call(deadline(), move |s| {
                s.acknowledge_central_release(&reservation, &e)
            })
            .await
            .map_err(err)?;
        Ok(())
    }
    async fn emergency_exit(
        &self,
        r: &Arc<AccountResources>,
        i: &StoredIntent,
    ) -> Result<Value, String> {
        let current = r
            .execution
            .trade
            .order_by_client_id(&i.instrument_id, &i.client_order_id)
            .await
            .map_err(err)?;
        if !matches!(
            current.state.as_str(),
            "filled" | "canceled" | "mmp_canceled"
        ) {
            let key = format!("safety_cancel_{}", &i.client_order_id);
            let hash = digest(&(i.intent_id.clone(), "unprotected_entry_cancel"))?;
            let k = key.clone();
            let h = hash.clone();
            let permit = r
                .mutation_capacity
                .try_acquire()
                .map_err(|_| "ACCOUNT_MUTATION_BUSY")?;
            let fresh = r
                .execution
                .store
                .call(deadline(), move |s| {
                    s.begin_operation(&k, &h, "safety_cancel", now())
                })
                .await
                .map_err(err)?;
            if !fresh {
                return Ok(uncertain(&i.intent_id, "SAFETY_CANCEL_OUTCOME_UNKNOWN"));
            }
            let request = CancelOrderRequest {
                instrument_id: i.instrument_id.clone(),
                client_order_id: i.client_order_id.clone(),
            };
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
            let response = r
                .execution
                .trade
                .cancel_order_after_admission(&request, &timing, &rate)
                .await
                .map_err(|_| "SAFETY_CANCEL_OUTCOME_UNKNOWN")?;
            if !response.top_level_success()
                || response.data.len() != 1
                || !response.data[0].accepted()
            {
                return Ok(uncertain(&i.intent_id, "SAFETY_CANCEL_NOT_CONFIRMED"));
            }
            let read = r
                .execution
                .trade
                .order_by_client_id(&i.instrument_id, &i.client_order_id)
                .await
                .map_err(err)?;
            if !matches!(read.state.as_str(), "filled" | "canceled" | "mmp_canceled") {
                return Ok(uncertain(&i.intent_id, "SAFETY_CANCEL_READBACK_PENDING"));
            }
            r.execution
                .store
                .call(deadline(), move |s| {
                    s.operation_receipt(&key, &hash, "safety_cancel", "CANCELED_VERIFIED", now())
                })
                .await
                .map_err(err)?;
            drop(permit);
        }
        crate::source_capture::capture_account_sources(&r.observer, &r.private_ws, &r.execution)
            .await
            .map_err(err)?;
        let key = format!("safety_close_{}", &i.client_order_id);
        self.close(r, &key, &i.intent_id, 10000, None).await
    }
    async fn cleanup_flat_protection(
        &self,
        r: &AccountResources,
        i: &StoredIntent,
    ) -> Result<(), String> {
        let _capacity = r
            .mutation_capacity
            .try_acquire()
            .map_err(|_| "ACCOUNT_MUTATION_BUSY")?;
        let id = i.intent_id.clone();
        let saved = r
            .execution
            .store
            .call(deadline(), move |s| s.protection(&id))
            .await
            .map_err(err)?;
        let Some(saved) = saved else {
            return Ok(());
        };
        let inventory = r
            .observer
            .pending_derivative_risk_algos()
            .await
            .map_err(err)?;
        if !inventory.complete_within_bound {
            return Err("ALGO_INVENTORY_INCOMPLETE".into());
        }
        if !inventory
            .samples
            .iter()
            .any(|a| a.algo_order_id == saved.algo_id)
        {
            return Ok(());
        }
        let snapshot = r.observer.snapshot().await.map_err(err)?;
        verify_instrument_is_flat(
            &i.instrument_id,
            snapshot
                .positions
                .iter()
                .map(|position| (position.instrument_id.as_str(), position.position.as_str())),
        )?;
        let key = format!("cleanup_{}", &i.client_order_id);
        let hash = digest(&(
            i.intent_id.clone(),
            saved.algo_id.clone(),
            "flat_protection_cleanup",
        ))?;
        let k = key.clone();
        let h = hash.clone();
        let fresh = r
            .execution
            .store
            .call(deadline(), move |s| {
                s.begin_operation(&k, &h, "protection_cleanup", now())
            })
            .await
            .map_err(err)?;
        if !fresh {
            return Err("PROTECTION_CANCEL_OUTCOME_UNKNOWN_INSPECT".into());
        }
        let request = okx_api::CancelAlgoOrderRequest {
            instrument_id: i.instrument_id.clone(),
            algo_order_id: saved.algo_id.clone(),
        };
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
            .admit_cancel_algo_order(&request)
            .map_err(err)?;
        let response = r
            .execution
            .trade
            .cancel_algo_order_after_admission(&request, &timing, &rate)
            .await
            .map_err(|_| "PROTECTION_CANCEL_OUTCOME_UNKNOWN")?;
        if !response.top_level_success()
            || response.data.len() != 1
            || response.data[0].status_code != "0"
            || response.data[0].algo_order_id != saved.algo_id
        {
            return Err("PROTECTION_CANCEL_NOT_CONFIRMED".into());
        }
        let verified = r
            .observer
            .pending_derivative_risk_algos()
            .await
            .map_err(err)?;
        if !verified.complete_within_bound
            || verified
                .samples
                .iter()
                .any(|a| a.algo_order_id == saved.algo_id)
        {
            return Err("PROTECTION_CANCEL_READBACK_PENDING".into());
        }
        r.execution
            .store
            .call(deadline(), move |s| {
                s.operation_receipt(
                    &key,
                    &hash,
                    "protection_cleanup",
                    "VERIFIED_CANCELED",
                    now(),
                )
            })
            .await
            .map_err(err)?;
        Ok(())
    }
    pub(super) async fn cancel(
        &self,
        r: &Arc<AccountResources>,
        key: &str,
        reference: &str,
        request: &TradeRequest,
    ) -> Result<Value, String> {
        let _capacity = r
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
            return self.status_intent(r, &i.intent_id).await;
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
        self.status_intent(r, &i.intent_id).await
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
            return self.status_intent(r, &old.intent_id).await;
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
            return self.status_intent(r, &prior.intent_id).await;
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

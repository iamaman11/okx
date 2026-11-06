//! One bounded stop-size amendment after a source-verified partial Reduce.
//! No new trade engine: this uses the original Native SQLite operation fence,
//! the existing account mutation lane and the already typed OKX amend-algos.
//! An ACK is only permission to verify a new immutable venue source.
use super::*;
use okx_api::AmendAlgoOrderRequest;

impl TradeService {
    pub(super) async fn shrink_stop_after_reduce(
        &self,
        r: &Arc<AccountResources>,
        reduction: &StoredIntent,
        venue: &okx_api::TradeOrderDetails,
        position_qty: Decimal,
    ) -> Result<Value, String> {
        if reduction.risk_increase
            || reduction.plan.operation != "reduce"
            || reduction.state != IntentState::Filled
            || position_qty <= Decimal::ZERO
        {
            return Err("PROTECTION_SHRINK_REDUCTION_UNVERIFIED".into());
        }
        let update = super::reconcile::verified_terminal_order_update_time(reduction, Some(venue))?;
        let venue_filled = super::reconcile::verified_order_readback(reduction, venue)?;
        if venue_filled != reduction.plan.quantity || venue_filled <= Decimal::ZERO {
            return Err("PROTECTION_SHRINK_REDUCE_FILL_UNVERIFIED".into());
        }
        let instrument = reduction.instrument_id.clone();
        let id = reduction.intent_id.clone();
        let plan_hash = reduction.plan_hash.clone();
        let reduce_order_id = venue.order_id.clone();
        let reduce_instance = reduction.plan.strategy_instance.clone();
        let reduce_position_side = reduction.plan.position_side.clone();
        let account_key = r.configuration.account_key.wire();
        let (opening, old_stop, key, prior, verified) = r
            .execution
            .store
            .call(deadline(), move |s| {
                let (fee, fill_count, _) =
                    s.prove_local_partial_reduce(&id, &reduce_order_id, venue_filled, update)?;
                if fill_count == 0 || fee > Decimal::ZERO {
                    return Err(okx_execution::StoreError::Invalid(
                        "PROTECTION_SHRINK_FINANCIAL_PROOF_INCOMPLETE",
                    ));
                }
                let lots = s.active_lots(&instrument)?;
                if lots.len() != 1
                    || lots[0].remaining_qty != position_qty
                    || lots[0].side != reduce_position_side
                    || lots[0].instance_id.as_deref() != Some(reduce_instance.as_str())
                {
                    return Err(okx_execution::StoreError::Invalid(
                        "PROTECTION_SHRINK_OWNERSHIP_UNVERIFIED",
                    ));
                }
                let origin =
                    lots[0]
                        .intent_id
                        .as_ref()
                        .ok_or(okx_execution::StoreError::Invalid(
                            "PROTECTION_SHRINK_ORIGINAL_INTENT_UNATTRIBUTED",
                        ))?;
                if origin == &id {
                    return Err(okx_execution::StoreError::Invalid(
                        "PROTECTION_SHRINK_OWNERSHIP_UNVERIFIED",
                    ));
                }
                let opening = s.intent(origin)?.ok_or(okx_execution::StoreError::Invalid(
                    "PROTECTION_SHRINK_ORIGINAL_INTENT_MISSING",
                ))?;
                if !opening.risk_increase
                    || opening.instrument_id != instrument
                    || opening.plan.strategy_instance != reduce_instance
                    || opening.plan.position_side != reduce_position_side
                    || opening.incident.is_some()
                    || s.active_central_receipt(origin)?.is_none()
                {
                    return Err(okx_execution::StoreError::Invalid(
                        "PROTECTION_SHRINK_ORIGINAL_RISK_UNVERIFIED",
                    ));
                }
                let old = s
                    .protection(origin)?
                    .ok_or(okx_execution::StoreError::Invalid(
                        "PROTECTION_SHRINK_ORIGINAL_STOP_MISSING",
                    ))?;
                let covered =
                    s.cursor("fills", "SWAP")?
                        .ok_or(okx_execution::StoreError::Invalid(
                            "PROTECTION_SHRINK_ORIGINAL_FILL_CURSOR_MISSING",
                        ))?;
                let original_fill = s.intent_fill_cost_evidence(origin, covered.watermark)?;
                let original_bills = s.intent_fee_bill_evidence(origin, covered.watermark)?;
                if original_bills.linked_bill_count != original_fill.fill_count
                    || original_fill.filled_quantity
                        != position_qty.checked_add(venue_filled).ok_or(
                            okx_execution::StoreError::Invalid(
                                "PROTECTION_SHRINK_QUANTITY_OVERFLOW",
                            ),
                        )?
                {
                    return Err(okx_execution::StoreError::Invalid(
                        "PROTECTION_SHRINK_GROSS_NET_QUANTITY_UNVERIFIED",
                    ));
                }
                let key = format!("protection-shrink:{}:{}", origin, exact(position_qty));
                let op = s.operation(&key)?;
                let verified = s.operation(&format!("{key}:verified"))?;
                if old.quantity != original_fill.filled_quantity
                    && (old.quantity != position_qty || verified.is_none())
                {
                    return Err(okx_execution::StoreError::Invalid(
                        "PROTECTION_SHRINK_STOP_VOLUME_CHANGED",
                    ));
                }
                Ok((opening, old, key, op, verified))
            })
            .await
            .map_err(err)?;
        let hash = digest(&(
            "protection-shrink/v1",
            &account_key,
            &opening.intent_id,
            &opening.plan_hash,
            &plan_hash,
            &reduction.intent_id,
            &reduction.client_order_id,
            &old_stop.algo_id,
            exact(position_qty),
        ))?;
        // A strictly matching signed algo detail is required in BOTH the
        // pre-amend and post-ACK paths. A standalone SQL receipt or a reused
        // algoId without source cannot verify new protection.
        let client = opening
            .client_order_id
            .get(..31)
            .map(|cl| format!("s{cl}"))
            .ok_or("PROTECTION_SHRINK_CLIENT_ID_INVALID")?;
        let live = r
            .execution
            .trade
            .algo_order_by_client_id(&client)
            .await
            .map_err(err)?;
        if live.algo_order_id != old_stop.algo_id {
            return Err("PROTECTION_SHRINK_STOP_ID_MISMATCH".into());
        }
        if let Some(done) = verified {
            if done.kind != "protection_shrink_verified"
                || done.request_hash != hash
                || done.result_ref != "VERIFIED"
                || old_stop.quantity != position_qty
            {
                return Err("PROTECTION_SHRINK_VERIFIED_RECEIPT_MISMATCH".into());
            }
            super::reconcile::verified_live_protection(&opening, position_qty, &live)?;
            return Ok(json!({
                "state":"verified",
                "original_intent_id":opening.intent_id,
                "reduce_intent_id":reduction.intent_id,
                "algo_id":live.algo_order_id,
                "remaining_quantity":exact(position_qty),
                "no_second_post":true,
            }));
        }
        if let Some(saved) = prior {
            if saved.kind != "protection_shrink" || saved.request_hash != hash {
                return Err("PROTECTION_SHRINK_OPERATION_CONFLICT".into());
            }
            if saved.result_ref != "ACK_SOURCE_PENDING" {
                return Err("PROTECTION_SHRINK_OUTCOME_UNKNOWN_NO_RESUBMIT".into());
            }
            // The authenticated exchange response to the first POST is
            // durable, but a second independently signed GET is necessary.
            if super::reconcile::verified_live_protection(&opening, position_qty, &live).is_err() {
                return Ok(json!({
                    "state":"ack_source_pending",
                    "original_intent_id":opening.intent_id,
                    "remaining_quantity":exact(position_qty),
                    "retry_amend":false,
                }));
            }
            let new_protection = okx_execution::ProtectionReceipt {
                intent_id: opening.intent_id.clone(),
                algo_id: live.algo_order_id.clone(),
                side: live.side.clone(),
                quantity: position_qty,
                trigger_price: opening
                    .plan
                    .stop_price
                    .ok_or("PROTECTION_SHRINK_ORIGINAL_STOP_PRICE_MISSING")?,
                trigger_basis: "mark".into(),
                verified_at: now(),
                source_digest: digest(&live)?,
            };
            let verification = format!("{key}:verified");
            r.execution
                .store
                .call(deadline(), move |s| {
                    s.verify_protection(&new_protection)?;
                    s.operation_receipt(
                        &verification,
                        &hash,
                        "protection_shrink_verified",
                        "VERIFIED",
                        now(),
                    )?;
                    Ok(())
                })
                .await
                .map_err(err)?;
            return Ok(json!({
                "state":"verified",
                "original_intent_id":opening.intent_id,
                "reduce_intent_id":reduction.intent_id,
                "algo_id":live.algo_order_id,
                "remaining_quantity":exact(position_qty),
                "no_second_post":true,
            }));
        }
        if old_stop.quantity != position_qty + venue_filled {
            return Err("PROTECTION_SHRINK_ORIGINAL_VOLUME_MISMATCH".into());
        }
        super::reconcile::verified_live_protection(&opening, old_stop.quantity, &live)?;
        let request = AmendAlgoOrderRequest {
            instrument_id: opening.instrument_id.clone(),
            algo_order_id: old_stop.algo_id.clone(),
            request_id: reduction.client_order_id.clone(),
            new_size: exact(position_qty),
            cancel_on_fail: false,
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
            .admit_amend_algo_order(&request)
            .map_err(err)?;
        let _permit = r
            .mutation_capacity
            .try_acquire()
            .map_err(|_| "ACCOUNT_MUTATION_BUSY")?;
        let operation_key = key.clone();
        let operation_hash = hash.clone();
        let fresh = r
            .execution
            .store
            .call(deadline(), move |s| {
                s.begin_operation(&operation_key, &operation_hash, "protection_shrink", now())
            })
            .await
            .map_err(err)?;
        if !fresh {
            return Err("PROTECTION_SHRINK_OUTCOME_UNKNOWN_NO_RESUBMIT".into());
        }
        let ack = r
            .execution
            .trade
            .amend_algo_order_after_admission(&request, &timing, &rate)
            .await
            .map_err(|_| "PROTECTION_SHRINK_OUTCOME_UNKNOWN_NO_RESUBMIT")?;
        if !ack.top_level_success()
            || ack.data.len() != 1
            || ack.data[0].status_code != "0"
            || ack.data[0].algo_order_id != request.algo_order_id
            || ack.data[0].request_id != request.request_id
        {
            return Err("PROTECTION_SHRINK_ACK_UNVERIFIED_NO_RESUBMIT".into());
        }
        r.execution
            .store
            .call(deadline(), move |s| {
                s.operation_receipt(
                    &key,
                    &hash,
                    "protection_shrink",
                    "ACK_SOURCE_PENDING",
                    now(),
                )
            })
            .await
            .map_err(err)?;
        Ok(json!({
            "state":"ack_source_pending",
            "original_intent_id":opening.intent_id,
            "reduce_intent_id":reduction.intent_id,
            "algo_id":old_stop.algo_id,
            "remaining_quantity":exact(position_qty),
            "retry_amend":false,
        }))
    }
}

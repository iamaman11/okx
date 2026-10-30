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

/// Historical known pricing evidence is not an all-in entry debit proof.
fn entry_cost_status(stored: Option<okx_execution::EntryQuoteEvidence>) -> Result<Value, String> {
    let Some(quote) = stored else {
        return Ok(json!({
            "state": "not_proven",
            "all_in_entry_cost_proven": false,
            "entry_debit_upper_usd": null,
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
        "entry_debit_upper_usd": null,
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
            let quote = r
                .execution
                .store
                .call(deadline(), move |s| s.entry_quote(&id))
                .await
                .map_err(err)?;
            entry_cost_status(quote)?
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
            if dec(&o.size)? != i.plan.quantity
                || o.side != i.plan.side
                || o.instrument_id != i.instrument_id
            {
                return Err("ORDER_READBACK_PLAN_MISMATCH".into());
            }
            let id = i.intent_id.clone();
            let order_id = o.order_id.clone();
            let filled = dec(&o.accumulated_fill_size)?;
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
            let exposed = snapshot.positions.iter().any(|p| {
                p.instrument_id == i.instrument_id
                    && p.position_side == i.plan.position_side
                    && dec(&p.position).is_ok_and(|q| q != Decimal::ZERO)
            });
            if !exposed && i.risk_increase {
                self.cleanup_flat_protection(r, &i).await?;
            }
            if filled > Decimal::ZERO && i.risk_increase && exposed {
                let algo_client = format!("s{}", &i.client_order_id[..31]);
                let p = r
                    .execution
                    .trade
                    .algo_order_by_client_id(&algo_client)
                    .await;
                match p {
                    Ok(p)
                        if p.instrument_id == i.instrument_id
                            && p.client_order_id == algo_client
                            && p.state == "live"
                            && p.failure_code.is_empty()
                            && p.stop_loss_trigger_price_type == "mark"
                            && dec(&p.stop_loss_trigger_price).ok() == i.plan.stop_price
                            && dec(&p.size).ok() == Some(filled)
                            && p.side != i.plan.side =>
                    {
                        let source_digest = digest(&p)?;
                        let protection = okx_execution::ProtectionReceipt {
                            intent_id: i.intent_id.clone(),
                            algo_id: p.algo_order_id,
                            side: p.side,
                            quantity: filled,
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
                                match r.execution.store.call(deadline(), move |s| {
                                    s.intent_fee_bill_evidence(&bill_id, through)
                                }).await {
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
                            },
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
        let venue_updated_at = terminal_order_update_time(order)?;
        if !complete.coverage_gaps.is_empty()
            || venue_updated_at.is_some_and(|at| complete.watermark < at)
        {
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
        let census = self.census(&state).await?;
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
        if !through.coverage_gaps.is_empty()
            || venue_updated_at.is_some_and(|at| through.watermark < at)
        {
            return Err("FINANCIAL_COVERAGE_INCOMPLETE".into());
        }
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
            // Do not invent settled fees from modeled loss; final bill proof
            // must explicitly populate these before the E1 risk release.
            cumulative_entry_usd: None,
            entry_settlement_revision: None,
            entry_settlement_digest: None,
        };
        let command = self.risk_command(
            &state,
            format!("release_{}", &i.client_order_id),
            RiskCommandAction::Resolve {
                reservation_key: receipt.reservation_key.clone(),
                plan_sha256: i.plan_hash.clone(),
                expected_risk_book_sequence: observed
                    .risk_book_sequence
                    .ok_or("RISK_SEQUENCE_MISSING")?,
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
        if snapshot.positions.iter().any(|p| {
            p.instrument_id == i.instrument_id
                && p.position_side == i.plan.position_side
                && dec(&p.position).is_ok_and(|q| q != Decimal::ZERO)
        }) {
            return Err("PROTECTION_STILL_REQUIRED".into());
        }
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
        if let Ok(order) = r
            .execution
            .trade
            .order_by_client_id(&target.instrument_id, &target.client_order_id)
            .await
            && dec(&order.accumulated_fill_size)? > Decimal::ZERO
        {
            return Err("REPLACE_PARTIAL_FILL_REQUIRES_EXPLICIT_CLOSE".into());
        }
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
        let quantity = mine.iter().fold(Decimal::ZERO, |s, l| s + l.remaining_qty);
        if quantity <= Decimal::ZERO {
            return Err("NO_ATTRIBUTED_POSITION_TO_CLOSE".into());
        }
        let side = match mine.first().map(|l| l.side.as_str()) {
            Some("long") => PositionSide::Long,
            Some("short") => PositionSide::Short,
            _ => return Err("POSITION_SIDE_UNATTESTED".into()),
        };
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
    fn e1_status_never_promotes_known_quote_into_all_in_entry_permission() {
        let empty = entry_cost_status(None).unwrap();
        assert_eq!(empty["all_in_entry_cost_proven"], false);
        assert!(empty["entry_debit_upper_usd"].is_null());
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
        let view = entry_cost_status(Some(quote.clone())).unwrap();
        assert_eq!(view["state"], "historical_known_cost_only");
        assert_eq!(view["known_entry_total_usd"], "1.0005");
        assert_eq!(view["all_in_entry_cost_proven"], false);
        assert!(view["entry_debit_upper_usd"].is_null());
        assert_eq!(view["reason_codes"].as_array().unwrap().len(), 4);
        let mut tampered = quote;
        tampered.total_usd = Decimal::ONE;
        assert_eq!(
            entry_cost_status(Some(tampered)).unwrap_err(),
            "ENTRY_QUOTE_READBACK_MISMATCH"
        );
    }
}

//! Reconcile one intent from independent observations, including bounded corrective protection.
use super::*;
use okx_api::CancelOrderRequest;

impl TradeService {
    pub(super) async fn find_intent(
        &self,
        r: &AccountResources,
        reference: &str,
    ) -> Result<StoredIntent, String> {
        let reference = reference.to_owned();
        r.execution
            .store
            .lookup_intent(
                deadline(),
                crate::execution_store::IntentLookup::new(
                    okx_trade_execution::TradeIntentLookup::Reference(reference),
                )
                .map_err(err)?,
            )
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
                    .record_order_readback(deadline(), id, order_id, state, filled)
                    .await
                    .map_err(err)?;
            }
            i = self.find_intent(r, &i.intent_id).await?;
        }
        Ok((i, order))
    }

    pub(in crate::trade_service) async fn status_intent(
        &self,
        r: &Arc<AccountResources>,
        reference: &str,
        invocation: Option<crate::diagnostics::ProtectionCleanupInvocation<'_>>,
    ) -> Result<Value, String> {
        let mut i = self.find_intent(r, reference).await?;
        // Historical fee/notional is a recorded fact, not an authorized entry.
        let entry_costs = if i.risk_increase
            && r.configuration.account_key.environment
                == okx_ipc_contracts::accounts::Environment::Real
        {
            let id = i.intent_id.clone();
            let (quote, admission) = r
                .execution
                .store
                .entry_cost_evidence(deadline(), id)
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
                    .reject_unsent(deadline(), id, "COMMAND_EXPIRED".into())
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
        let declined_reserve = r
            .execution
            .store
            .declined_reserve(deadline(), id)
            .await
            .map_err(err)?;
        if declined_reserve {
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
        let unsent_before_reserve = r
            .execution
            .store
            .unsent_before_reserve(deadline(), id)
            .await
            .map_err(err)?;
        if unsent_before_reserve {
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
            .unsent_release_receipt(deadline(), id)
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
                let effective_protected = r
                    .execution
                    .store
                    .effective_protected_quantity(deadline(), i.intent_id.clone(), filled)
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
                            let protection = okx_trade_execution::ProtectionReceipt {
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
                                .verify_protection(deadline(), protection)
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
                                        .protection_incident(deadline(),id,"MANAGED_AGGREGATE_PROTECTION_UNVERIFIED".into())
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
                                .protection_incident(
                                    deadline(),
                                    id,
                                    "PROTECTION_PENDING_OR_FAILED".into(),
                                )
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
                Err(reason) => json!({"state":"pending","reason_code":reason.reason_code,
                    "stage":reason.stage,"kind":reason.kind,
                    "intent_id":reason.intent_id,"intent_revision":reason.intent_revision}),
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
                    .partial_entry_ack_pending(deadline(), id)
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
            .active_lots(deadline(), instrument)
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
            .protection(deadline(), id)
            .await
            .map_err(err)?;
        // Expose the immutable, cursor-covered source ledger for this intent,
        // not an estimate from venue order status or a permission to settle
        // central USD entry spend. Missing coverage remains explicitly blocked.
        let entry_fill_evidence = if financial && i.risk_increase {
            let evidence_id = i.intent_id.clone();
            let source_result = async {
                let cursor = r
                    .execution
                    .store
                    .cursor(deadline(), "fills".into(), "SWAP".into())
                    .await?
                    .ok_or(okx_trade_execution::StoreError::Invalid(
                        "ENTRY_FILL_COVERAGE_UNPROVEN",
                    ))?;
                r.execution
                    .store
                    .intent_fill_cost_evidence(deadline(), evidence_id, cursor.watermark)
                    .await
            }
            .await;
            match source_result {
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
                                .intent_fee_bill_evidence(deadline(), bill_id, through)
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
                        .resolve_protection_incident_after_flat(deadline(), id, proof)
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
    pub(in crate::trade_service) async fn release_unsent(
        &self,
        r: &AccountResources,
        i: &StoredIntent,
    ) -> Result<unsent_release::UnsentReleaseOutcome, unsent_release::UnsentReleaseError> {
        unsent_release::release_unsent(
            r.configuration.account_key.environment,
            i,
            unsent_release::UnsentReleaseDependencies {
                store: &r.execution.store,
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
            let key = format!("safety_cancel_{}", i.client_order_id);
            let hash = digest(&(i.intent_id.clone(), "unprotected_entry_cancel"))?;
            let k = key.clone();
            let h = hash.clone();
            let fresh = r
                .execution
                .store
                .begin_operation(
                    deadline(),
                    crate::execution_store::OperationId::new(k).map_err(err)?,
                    h,
                    "safety_cancel".into(),
                )
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
                .operation_receipt(
                    deadline(),
                    crate::execution_store::OperationId::new(key).map_err(err)?,
                    hash,
                    "safety_cancel".into(),
                    "TERMINAL_VERIFIED".into(),
                )
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
            .active_lots(deadline(), instrument)
            .await
            .map_err(err)?
            .into_iter()
            .filter(|lot| lot.intent_id.as_deref() == Some(&intent_id))
            .map(|lot| (lot.side, lot.remaining_qty))
            .collect::<Vec<_>>();
        let owned = verified_emergency_owned_exposure(
            i,
            venue_filled,
            signed_position,
            owned_lots
                .iter()
                .map(|(side, quantity)| (side.as_str(), *quantity)),
        )?;
        let key = format!("safety_close_{}", i.client_order_id);
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
}

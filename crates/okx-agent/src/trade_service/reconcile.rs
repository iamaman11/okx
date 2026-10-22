use super::*;
use okx_api::CancelOrderRequest;
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
                    .or(s.by_exchange_order_id(&reference)?))
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
                    json!({"intent":i,"remaining_exposure":"not_evaluated"}),
                ));
            }
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
        let mut result = completed(
            json!({"intent":i,"remaining_exposure":if financial{Some(exact(exposure))}else{None},"protection":protection,"central_release":central_release,"financial_error":financial_error,"financial_complete":financial,"protection_incident":protection_failed,"compensation":compensation}),
        );
        result["quality"] = json!({"execution":"authenticated_readback","financial":if financial{"complete"}else{"incomplete"},"protection":if protection_failed{"incident"}else{"evaluated"}});
        if !financial {
            result["status"] = json!("partial");
            result["warnings"] = json!(["FINANCIAL_BACKFILL_PENDING"]);
        }
        Ok(result)
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
            return Ok(());
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
        if !complete.coverage_gaps.is_empty()
            || order.is_some_and(|o| {
                o.update_time_ms
                    .parse::<i64>()
                    .is_ok_and(|at| complete.watermark < at)
            })
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
            || order.is_some_and(|o| {
                o.update_time_ms
                    .parse::<i64>()
                    .is_ok_and(|at| through.watermark < at)
            })
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
                max_slippage_bps: 10,
            },
            protection: None,
            expires_in_ms: expires,
        };
        self.execute_bound(r, &command, governance, Some(bound_hash))
            .await
    }
}

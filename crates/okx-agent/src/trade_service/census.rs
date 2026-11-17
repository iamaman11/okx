//! One owner for pre-reserve, post-HELD, protection and terminal-release
//! cohort census. Source proof is not permission to send an exchange order.
use super::execute::{
    ManagedProtectionCensusError, PostHeldSelected, classify_managed_protection_census_error,
    native_process_incarnation, position_mismatch_reason, verify_post_held_selected,
};
use super::*;

/// Assemble only signed venue objects from this original account scope.
fn census_request(
    snapshot: &okx_observation::AccountSnapshot,
    release_scope: Option<TerminalReleaseCensusScope<'_>>,
) -> Result<okx_execution::AccountCensusRequest, String> {
    let positions = snapshot
        .positions
        .iter()
        .filter_map(|p| {
            match super::reconcile::verified_venue_position_direction(&p.position_side, &p.position)
            {
                Ok(Some((direction, _))) => {
                    Some(Ok((p.instrument_id.clone(), direction.to_owned())))
                }
                Ok(None) => None,
                Err(reason) => Some(Err(reason)),
            }
        })
        .collect::<Result<Vec<_>, String>>()?;
    let client_order_ids = snapshot
        .pending_orders
        .iter()
        .map(|order| {
            order
                .client_order_id
                .clone()
                .ok_or("UNATTRIBUTED_PENDING_ORDER".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(okx_execution::AccountCensusRequest {
        positions,
        client_order_ids,
        limit: 100,
        terminal_peer_release_intent: release_scope.map(|scope| scope.intent_id.to_owned()),
    })
}

impl TradeService {
    pub(super) async fn census(
        &self,
        state: &CentralGovernanceSnapshot,
    ) -> Result<NativeRiskCensus, String> {
        self.census_with_reads(state, None, None, None, None, None)
            .await
            .map(|v| v.0)
    }

    /// PRE-RESERVE read-only cohort census, bounded by the SAME Execute
    /// deadline as PREPARED. Wait only for authenticated LOCAL, NOT-SENT
    /// private reads; never tolerate missing lots, pending POSTs or stops.
    pub(super) async fn census_until(
        &self,
        state: &CentralGovernanceSnapshot,
        until: Instant,
    ) -> Result<NativeRiskCensus, String> {
        tokio::time::timeout_at(
            until.into(),
            self.census_with_reads(state, None, None, Some(until), None, None),
        )
        .await
        .map_err(|_| "ENTRY_CENSUS_SOURCE_DEADLINE".to_owned())?
        .map(|v| v.0)
    }

    #[cfg(test)]
    fn b_census_probe_failure(phase: &str, account: &str, reason: &str, until: Option<Instant>) {
        if std::env::var("OKX_B_CENSUS_CAUSE_PROBE").as_deref() != Ok("1") {
            return;
        }
        let fingerprint = format!("{:x}", Sha256::digest(account.as_bytes()));
        let code = [
            "COHORT_FINANCIAL_SOURCE_INCOMPLETE",
            "COHORT_ALGO_SOURCE_STALE",
            "COHORT_EVIDENCE_INCOMPLETE",
            "COHORT_SOURCE_GENERATION_CHANGED",
            "ENTRY_CENSUS_SOURCE_DEADLINE",
            "TRANSCRIPT_SOURCE_PAGE_UNEXPECTED_OR_EXHAUSTED",
            "TRANSCRIPT_CLOCK_EXHAUSTED",
            "TRANSCRIPT_REPEAT_EXCEEDED",
            "RateLimited",
            "rate/backpressure defer",
            "SourceDeadline",
        ]
        .into_iter()
        .find(|s| reason.contains(s))
        .unwrap_or("OTHER_TYPED_FAILURE");
        let endpoint = [
            "/api/v5/account/config",
            "/api/v5/account/balance",
            "/api/v5/account/positions",
            "/api/v5/account/trade-fee",
            "/api/v5/trade/orders-algo-pending",
            "/api/v5/trade/orders-pending",
        ]
        .into_iter()
        .find(|s| reason.contains(s));
        eprintln!(
            "B_CENSUS_FIRST_FAILURE_V1:{}",
            json!({
              "schema":"okx.b-census-first-failure/v1","phase":phase,
              "account_digest_prefix":&fingerprint[..12],
              "reason_code":code,"endpoint":endpoint,
              "reason_sha256":format!("{:x}",Sha256::digest(reason.as_bytes())),
              "remaining_ms":until.map(|v|v.saturating_duration_since(Instant::now()).as_millis() as u64)
            })
        );
    }

    /// Only the protection reconciliation of an existing managed position
    /// may wait for LOCAL/NO-SEND private reads. The same complete cohort
    /// proof and freshness rules apply; failure remains fail-closed within
    /// the original bounded status deadline.
    pub(super) async fn census_for_managed_protection_until(
        &self,
        state: &CentralGovernanceSnapshot,
        until: Instant,
    ) -> Result<NativeRiskCensus, ManagedProtectionCensusError> {
        let result = tokio::time::timeout_at(
            until.into(),
            self.census_with_reads(state, None, None, Some(until), None, None),
        )
        .await
        .map_err(|_| ManagedProtectionCensusError::Denied("MANAGED_CENSUS_SOURCE_DEADLINE".into()))?
        .map(|v| v.0);
        result.map_err(classify_managed_protection_census_error)
    }

    /// Only the settlement-confirmed terminal-release path may supply this
    /// exact one-reservation exception. No alternate or cached census owner.
    pub(super) async fn census_for_terminal_release(
        &self,
        state: &CentralGovernanceSnapshot,
        account_key: &str,
        intent_id: &str,
        reservation_key: &str,
        plan_hash: &str,
    ) -> Result<NativeRiskCensus, String> {
        let scope = TerminalReleaseCensusScope {
            account_key,
            intent_id,
            reservation_key,
            plan_hash,
        };
        self.census_with_reads(state, None, None, None, None, Some(scope))
            .await
            .map(|v| v.0)
    }

    pub(super) async fn census_after_held(
        &self,
        state: &CentralGovernanceSnapshot,
        held_received: Instant,
        until: Instant,
        reservation: &str,
        reserved_usd_per_usdt: &str,
        selected: &PostHeldSelected,
    ) -> Result<(NativeRiskCensus, CausalPostProof), String> {
        let (census, account_reads) = self
            .census_with_reads(
                state,
                Some(held_received),
                Some((&selected.account_key, reserved_usd_per_usdt)),
                Some(until),
                Some(selected),
                None,
            )
            .await?;
        let proof = CausalPostProof {
            reserve_anchor: reservation.to_owned(),
            native_incarnation: native_process_incarnation()?.to_owned(),
            elapsed_to_proof_ms: held_received.elapsed().as_millis() as u64,
            account_reads,
        };
        if !proof.valid(&census) {
            return Err("POST_CAUSAL_READ_PROOF_INVALID".into());
        }
        Ok((census, proof))
    }

    async fn census_with_reads(
        &self,
        state: &CentralGovernanceSnapshot,
        held_received: Option<Instant>,
        fx_rate_check: Option<(&str, &str)>,
        until: Option<Instant>,
        selected: Option<&PostHeldSelected>,
        terminal_release: Option<TerminalReleaseCensusScope<'_>>,
    ) -> Result<(NativeRiskCensus, Vec<PostAccountReadProof>), String> {
        if !self.accounts.failures().await.is_empty() {
            return Err("ACCOUNT_COHORT_UNAVAILABLE".into());
        }
        let mut keys = Vec::new();
        let mut occupied = Vec::new();
        let mut evidence = Vec::new();
        let mut post_reads = Vec::new();
        let started_at = now();
        use futures_util::{StreamExt, TryStreamExt, stream};
        let cohort = state
            .state
            .accounts
            .iter()
            .filter(|a| a.environment == okx_protocol::account_governance::AccountEnvironment::Real)
            .cloned()
            .collect::<Vec<_>>();
        let pages = stream::iter(cohort)
            .map(|c| async move {
                // The timer starts before fetching any account/private/venue facts.
                let selected = selected.filter(|w| w.account_key == c.account_key);
                let release_scope =
                    terminal_release.filter(|scope| scope.account_key == c.account_key);
                let started = selected
                    .map(|w| w.started_after_held_ms)
                    .or_else(|| held_received.map(|tick| tick.elapsed().as_millis() as u64));
                let r = self.resources(&c.account_key).await?;
                if !r.source_identity_ready().await {
                    return Err("COHORT_SOURCE_GENERATION_UNVERIFIED".into());
                }
                let generation = r.private_ws.status().await.connection_generation;
                let report=|_phase: &str, reason: String| {
                    #[cfg(test)]
                    Self::b_census_probe_failure(_phase,&c.account_key,&reason,until);
                    reason
                };
                // Inventory can span several bounded REST scopes. Read the
                // authenticated account snapshot AFTER inventory so source
                // freshness is checked at the final census proof, not before
                // a potentially rate-admitted sequence of algo GETs.
                let algos = if held_received.is_none() {
                    if let Some(until) = until {
                        r.observer
                            .pending_derivative_risk_algos_until(until)
                            .await
                            .map_err(|e|report("ALGO_INVENTORY_DEADLINE_OWNED",err(e)))?
                    } else {
                        r.observer
                            .pending_derivative_risk_algos()
                            .await
                            .map_err(|e|report("ALGO_INVENTORY_NORMAL",err(e)))?
                    }
                } else {
                    // POST-HELD is still within the original Execute's
                    // monotonic deadline. Previously this branch made 16
                    // derivative-algo GETs with immediate local rate admission,
                    // turning a safe NO-SEND throttle into a definitive reject
                    // of an already durable HELD intent. Use the same bounded
                    // authenticated read admission as pre-reserve; a missing
                    // page, changed generation or expired deadline still
                    // prevents BEGIN_POST.
                    r.observer
                        .pending_derivative_risk_algos_until(
                            until.ok_or("POST_DEADLINE_MISSING")?
                        )
                        .await
                        .map_err(|e|report("ALGO_INVENTORY_POST_HELD",err(e)))?
                };
                let snapshot = if let Some(witness) = selected {
                    verify_post_held_selected(
                        witness,
                        &c.account_key,
                        &c.uid_fingerprint,
                        generation,
                        now(),
                    )?;
                    // The signed selected-account GET occurred on this very
                    // CAS attempt, after HELD; other accounts are read anew.
                    witness.snapshot.clone()
                } else if held_received.is_some() {
                    r.observer
                        .snapshot_after_held_until(until.ok_or("POST_DEADLINE_MISSING")?)
                        .await
                        .map_err(|e|report("ACCOUNT_SNAPSHOT_POST_HELD",err(e)))?
                } else if terminal_release.is_some() {
                    // All terminal census sources must follow the settlement
                    // frontier. snapshot_until retains the AUTHENTICATED time
                    // of a cached config observation; relabeling a cache hit
                    // as fresh would be unsafe and can put the census before
                    // settlement even when its balances were fetched later.
                    // Reuse the existing actual fresh-config snapshot ingress.
                    r.observer.snapshot_after_held_until(deadline()).await
                        .map_err(|e|report("ACCOUNT_SNAPSHOT_TERMINAL",err(e)))?
                } else if let Some(until) = until {
                    // The managed-protection census retries only authenticated
                    // LOCAL/NO-SEND read admission, bounded by its deadline.
                    // It still requires the complete current cohort evidence.
                    r.observer.snapshot_until(until).await
                        .map_err(|e|report("ACCOUNT_SNAPSHOT_PRE_RESERVE",err(e)))?
                } else {
                    r.observer.snapshot().await
                        .map_err(|e|report("ACCOUNT_SNAPSHOT_NORMAL",err(e)))?
                };
                // The selected account's fresh USD/USDT conversion must
                // still cover the identical reserved USD exposure. A rate
                // change cannot silently reuse a smaller prior risk grant.
                if let Some((selected, original_rate)) = fx_rate_check
                    && c.account_key == selected
                {
                    let fresh_rate = Self::usd_conversion(&snapshot)?.usd_per_usdt;
                    verify_post_held_fx(original_rate, &fresh_rate)?;
                }
                if !algos.complete_within_bound
                    || snapshot.account_uid_fingerprint != c.uid_fingerprint
                {
                    return Err("COHORT_EVIDENCE_INCOMPLETE".into());
                }
                // This is the earliest REST response across every pending
                // derivative-algo scope. Complete coverage is insufficient
                // if the underlying source timestamp is absent or stale.
                let algo_source_floor=verified_algo_source_time(algos.received_at_ms,now())
                    .map_err(|e|report("ALGO_SOURCE_FRESHNESS",e))?;
                let mut managed_algos = std::collections::BTreeSet::new();
                // Native reads all local obligations and financial facts in
                // ONE owner snapshot, then compares them to the signed venue
                // observation below. No per-position SQL hops or legacy owner.
                #[cfg(test)]
                let native_census = if let Some((owner, account)) = r.execution.store.native_scope() {
                    Some(
                        owner
                            .census_snapshot(
                                account.to_owned(),
                                census_request(&snapshot, release_scope)?,
                                until.unwrap_or_else(deadline),
                            )
                            .await
                            .map_err(err)?,
                    )
                } else {
                    None
                };
                #[cfg(not(test))]
                let native_census = {
                    let (owner, account) = r.execution.store.native_owner().map_err(err)?;
                    Some(
                        owner
                            .census_snapshot(
                                account.to_owned(),
                                census_request(&snapshot, release_scope)?,
                                until.unwrap_or_else(deadline),
                            )
                            .await
                            .map_err(err)?,
                    )
                };
                // Identifiers here are populated ONLY after the independently
                // observed venue position, SQL lots, central reserve and
                // verified protective algo have been matched.
                let mut verified_live_intents = std::collections::BTreeSet::new();
                let mut covered_position_count = 0_usize;
                for p in &snapshot.positions {
                    let Some((position_direction, actual_qty)) =
                        super::reconcile::verified_venue_position_direction(
                            &p.position_side,
                            &p.position,
                        )?
                    else {
                        continue;
                    };
                    covered_position_count += 1;
                    // The active lots, each independently verified stop and
                    // its central receipt must be observed in ONE SQLite
                    // owner turn. Multiple queue hops could age the signed
                    // venue snapshot before the final freshness gate.
                    let instrument = p.instrument_id.clone();
                    let direction = position_direction.to_owned();
                    let (lots, owned_proofs) = if let Some(census) = &native_census {
                        let position = census.positions.iter()
                            .find(|v| v.instrument == instrument && v.direction == direction)
                            .ok_or("COHORT_POSITION_SNAPSHOT_MISSING")?;
                        (position.lots.clone(), position.owned_proofs.clone())
                    } else {
                    #[cfg(test)]
                    { r
                        .execution
                        .store
                        .call(deadline(), move |s| {
                            let lots = s.active_lots(&instrument)?;
                            let proofs = lots
                                .iter()
                                .filter(|lot| lot.side == direction)
                                .map(|lot| {
                                    let id = lot.intent_id.as_ref().ok_or(
                                        okx_execution::StoreError::Invalid(
                                            "UNATTRIBUTED_POSITION_LOT",
                                        ),
                                    )?;
                                    Ok((
                                        id.clone(),
                                        s.protection(id)?,
                                        s.active_central_receipt(id)?,
                                    ))
                                })
                                .collect::<okx_execution::StoreResult<Vec<_>>>()?;
                            Ok((lots, proofs))
                        })
                        .await
                        .map_err(err)? }
                    #[cfg(not(test))]
                    {
                        return Err("NATIVE_CENSUS_POSITION_PROOFS_OWNER_REQUIRED".into());
                    }
                };
                    let covered = lots
                        .iter()
                        .filter(|l| l.side == position_direction)
                        .try_fold(Decimal::ZERO, |total, lot| {
                            if lot.remaining_qty <= Decimal::ZERO {
                                return Err("UNATTRIBUTED_EXCHANGE_POSITION".to_owned());
                            }
                            total
                                .checked_add(lot.remaining_qty)
                                .ok_or_else(|| "COHORT_POSITION_QUANTITY_OVERFLOW".to_owned())
                        })?;
                    if covered != actual_qty {
                        let newest_lot = lots
                            .iter()
                            .filter(|lot| lot.side == position_direction)
                            .map(|lot| lot.opened_at)
                            .max();
                        return Err(
                            position_mismatch_reason(p.update_time_ms.as_deref(), newest_lot)
                                .to_owned(),
                        );
                    }
                    for (owner_id, protection, receipt) in owned_proofs {
                        let protection = protection.ok_or("PROTECTION_UNVERIFIED")?;
                        let receipt = receipt.ok_or("CENTRAL_RESERVATION_UNVERIFIED")?;
                        let owned_qty = lots
                            .iter()
                            .filter(|l| {
                                l.side == position_direction
                                    && l.intent_id.as_deref() == Some(&owner_id)
                            })
                            .try_fold(Decimal::ZERO, |total, lot| {
                                if lot.remaining_qty <= Decimal::ZERO {
                                    return Err("COHORT_POSITION_LOT_QUANTITY_INVALID".to_owned());
                                }
                                total
                                    .checked_add(lot.remaining_qty)
                                    .ok_or_else(|| "COHORT_POSITION_QUANTITY_OVERFLOW".to_owned())
                            })?;
                        // An oversized stop can flip an already partially
                        // reduced position. Exact attributed lot coverage is
                        // mandatory for every new-risk cohort admission.
                        if protection.quantity != owned_qty
                            || receipt.plan_hash.is_empty()
                            || receipt.reservation_key.starts_with("local:")
                        {
                            return Err("PROTECTION_OR_RESERVATION_COVERAGE_INCOMPLETE".into());
                        }
                        managed_algos.insert(protection.algo_id);
                        verified_live_intents.insert(owner_id.clone());
                    }
                }
                // The same one-owner readback applies to all pending orders.
                // No missing client ID or unmatched SQL intent can disappear
                // in a batch; every venue order is still checked separately.
                let clients = snapshot.pending_orders
                    .iter()
                    .map(|order| order.client_order_id.clone()
                        .ok_or("UNATTRIBUTED_PENDING_ORDER"))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut covered_order_count = 0_usize;
                if !clients.is_empty() {
                    let known_orders = if let Some(census) = &native_census {
                        census.pending_orders.clone()
                    } else {
                    #[cfg(test)]
                    { r
                        .execution
                        .store
                        .call(deadline(), move |s| {
                            clients.iter().map(|client|
                                s.by_client_order_id(client)
                            ).collect::<okx_execution::StoreResult<Vec<_>>>()
                        })
                        .await
                        .map_err(err)? }
                    #[cfg(not(test))]
                    {
                        return Err("NATIVE_CENSUS_PENDING_ORDERS_OWNER_REQUIRED".into());
                    }
                };
                    for (order, known) in snapshot.pending_orders.iter().zip(known_orders) {
                        let known = known.ok_or("UNATTRIBUTED_PENDING_ORDER")?;
                        verify_pending_order_binding(order, &known)?;
                        covered_order_count += 1;
                    }
                }
                let observed_algos = algos
                    .samples
                    .iter()
                    .map(|a| a.algo_order_id.clone())
                    .collect::<std::collections::BTreeSet<_>>();
                if observed_algos != managed_algos {
                    // This is a HARD safety mismatch, never accepted as flat
                    // or retried with an expanded trading permission. Only
                    // the test transcript can emit source/SQL attribution.
                    #[cfg(test)]
                    if std::env::var("OKX_T09_4_SOURCE_COHERENCE_PROBE").as_deref() == Ok("1") {
                        let positions = snapshot.positions.iter().map(|p| json!({
                            "instrument":p.instrument_id,
                            "side":p.position_side,
                            "quantity":p.position,
                        })).collect::<Vec<_>>();
                        eprintln!("T09_4_CENSUS_ALGO_MISMATCH_V1:{}", json!({
                            "account":c.account_key,
                            "observed_algo_ids":observed_algos,
                            "sql_protection_algo_ids":managed_algos,
                            "positions":positions,
                            "algo_received_at_ms":algos.received_at_ms,
                            "snapshot_received_at":snapshot.source_received_at,
                            "generation":generation,
                            "held":held_received.is_some(),
                            "remaining_ms":until.map(|v|v.saturating_duration_since(Instant::now()).as_millis() as u64),
                        }));
                    }
                    return Err("UNATTRIBUTED_CONDITIONAL_ORDER".into());
                }
                // Active SQL intents are obligations too. A fresh venue
                // snapshot can omit a still-UNKNOWN POST, and a pending order
                // cannot be considered managed without its central receipt.
                // The 100th row is a positive truncation signal: never
                // misreport a bounded local scan as complete.
                // This proof is specific to terminal RELEASE, never normal
                // entry admission. A second terminal entry may still hold its
                // central reservation: verify its own immutable finance proof
                // in the SAME SQL owner before allowing the first to RELEASE.
                // A stale/unknown lot, live algo or pending order never qualifies.
                let venue_empty_for_terminal_peers = release_scope.is_some()
                    && algos.samples.is_empty()
                    && snapshot.pending_orders.is_empty()
                    && snapshot
                        .positions
                        .iter()
                        .all(|p| dec(&p.position).is_ok_and(|quantity| quantity == Decimal::ZERO));
                let releasing_id = release_scope.map(|scope| scope.intent_id.to_owned());
                let (local_intents, pending_central, verified_terminal_peers) = if let Some(census) = &native_census {
                    (census.intents.clone(), census.pending_central.clone(),
                     if venue_empty_for_terminal_peers {census.verified_terminal_peers.clone()}
                     else {std::collections::BTreeSet::new()})
                } else {
                    #[cfg(test)]
                    { r
                    .execution
                    .store
                    .call(deadline(), move |s| {
                        let (intents, pending) = s.risk_census_intents(100)?;
                        let mut peers = std::collections::BTreeSet::new();
                        if venue_empty_for_terminal_peers {
                            for intent in &intents {
                                if !intent.risk_increase
                                    || intent.state != IntentState::Filled
                                    || !intent.instrument_id.ends_with("-USDT-SWAP")
                                    || releasing_id.as_deref() == Some(intent.intent_id.as_str())
                                    || pending.binary_search(&intent.intent_id).is_err()
                                {
                                    continue;
                                }
                                let saved =
                                    s.operation(&format!("entry-settlement:{}", intent.intent_id))?;
                                let Some(saved) = saved.filter(|receipt| {
                                    receipt.kind == "ENTRY_SETTLEMENT_PROVEN"
                                        && receipt.request_hash == intent.plan_hash
                                }) else {
                                    continue;
                                };
                                let proof: okx_execution::EntrySettlementEvidence =
                                    serde_json::from_str(&saved.result_ref).map_err(|_| {
                                        okx_execution::StoreError::Invalid(
                                            "COHORT_TERMINAL_PEER_PROOF_CORRUPT",
                                        )
                                    })?;
                                if proof.schema != "okx.entry-terminal-settlement/v1"
                                    || proof.intent_id != intent.intent_id
                                    || proof.plan_hash != intent.plan_hash
                                    || s.entry_late_pending(&intent.intent_id)?
                                {
                                    continue;
                                }
                                let fills = s.cursor("fills", "SWAP")?;
                                let bills = s.cursor("bills", "account")?;
                                let (Some(fills), Some(bills)) = (fills, bills) else {
                                    continue;
                                };
                                if fills.generation <= 0
                                    || bills.generation <= 0
                                    || !fills.cursor.is_empty()
                                    || !bills.cursor.is_empty()
                                    || !fills.coverage_gaps.is_empty()
                                    || !bills.coverage_gaps.is_empty()
                                {
                                    continue;
                                }
                                let frontier = fills.watermark.min(bills.watermark);
                                if frontier < proof.venue_update_ms
                                    || frontier < proof.source_covered_through_ms
                                    || s.active_lots(&intent.instrument_id)?.iter().any(|lot| {
                                        lot.intent_id.as_deref() == Some(&intent.intent_id)
                                            && lot.remaining_qty > Decimal::ZERO
                                    })
                                {
                                    continue;
                                }
                                // Existing receipt means this is a read-only
                                // replay; the SQL source owner rechecks every
                                // original immutable bill, fill and digest.
                                let original = s.bind_terminal_entry_settlement(
                                    &intent.intent_id,
                                    proof.venue_update_ms,
                                    proof.venue_filled_quantity,
                                    frontier,
                                )?;
                                if original != proof {
                                    return Err(okx_execution::StoreError::Invalid(
                                        "COHORT_TERMINAL_PEER_SOURCE_CHANGED",
                                    ));
                                }
                                peers.insert(intent.intent_id.clone());
                            }
                        }
                        let covered = intents
                            .into_iter()
                            .map(|intent| {
                                let central = s.active_central_receipt(&intent.intent_id)?;
                                Ok((intent, central))
                            })
                            .collect::<okx_execution::StoreResult<Vec<_>>>()?;
                        Ok((covered, pending, peers))
                    })
                    .await
                    .map_err(err)? }
                    #[cfg(not(test))]
                    {
                        return Err("NATIVE_CENSUS_INTENT_RECEIPTS_OWNER_REQUIRED".into());
                    }
                };
                let local_intent_digest = verify_local_intent_coverage_with_managed(
                    &local_intents,
                    &pending_central,
                    &c.account_key,
                    release_scope,
                    &verified_live_intents,
                    &verified_terminal_peers,
                )?;
                if r.private_ws.status().await.connection_generation != generation
                    || !r.source_identity_ready().await
                    || !r
                        .execution
                        .verified_uid_matches_until(
                            &c.uid_fingerprint,
                            generation,
                            until.unwrap_or_else(deadline),
                        )
                        .await
                        .map_err(|e| report("COHORT_EXECUTOR_IDENTITY", err(e)))?
                {
                    return Err("COHORT_SOURCE_GENERATION_CHANGED".into());
                }
                let financial = if let Some(census) = &native_census {
                    (census.financial_risk.clone(), census.policy.campaign_loss)
                } else {
                    #[cfg(test)]
                    { r
                    .execution
                    .store
                    .call(deadline(), |s| {
                        Ok((s.financial_risk(now(), None)?, s.policy()?.4))
                    })
                    .await
                    .map_err(|e|report("FINANCIAL_SQL_OWNER",err(e)))? }
                    #[cfg(not(test))]
                    {
                        return Err("NATIVE_CENSUS_FINANCIAL_RISK_OWNER_REQUIRED".into());
                    }
                };
                #[cfg(test)]
                if std::env::var("OKX_B_CENSUS_CAUSE_PROBE").as_deref()==Ok("1")
                    && !financial.0.nav_complete
                {
                    let h=r.source_capture_health.read().await;
                    let fingerprint=format!("{:x}",Sha256::digest(c.account_key.as_bytes()));
                    eprintln!("B_CENSUS_FINANCIAL_GAP_V1:{}",json!({
                        "schema":"okx.b-census-financial-gap/v1",
                        "account_digest_prefix":&fingerprint[..12],
                        "nav_complete":financial.0.nav_complete,
                        "source_as_of":financial.0.source_as_of,
                        "snapshot_received_at":snapshot.source_received_at,
                        "algo_received_ms":algos.received_at_ms,
                        "capture_last_attempt_ms":h.last_attempt_ms,
                        "capture_last_success_ms":h.last_success_ms,
                        "capture_last_failure_ms":h.last_failure_ms,
                        "capture_reason_code":h.reason_code,
                        "remaining_ms":until.map(|v|v.saturating_duration_since(Instant::now()).as_millis() as u64)
                    }));
                }
                // A freshly constructed census is not proof that its underlying
                // account or ledger sources were fresh. Keep the actual oldest
                // source timestamp, not just the time at which we read it.
                let source_floor = census_source_floor(
                    &snapshot.source_received_at,
                    &snapshot.account_uid_fingerprint,
                    &c.uid_fingerprint,
                    financial.0.nav_complete,
                    financial.0.source_as_of,
                    now(),
                )
                .map_err(|e|report("FINANCIAL_SOURCE_FLOOR",e))?
                .min(algo_source_floor);
                // Bind SQL source quality and financial state to the same
                // content proof as the account and conditional-order reads.
                let data = digest(&(
                    &snapshot,
                    &algos,
                    &local_intent_digest,
                    financial.0.source_as_of,
                    &financial.0.daily_loss_usd,
                    &financial.0.drawdown_ratio,
                    &financial.1,
                ))?;
                // Exactly matched native positions, venue orders and
                // protection algos are already covered by independently held
                // central RiskBook reservations. External occupancy is zero
                // for this PROVEN managed set, not because the account is
                // flat; Worker adds central charged risk separately.
                // Any unmatched venue exposure fails closed above/below.
                let external_occupied = certified_managed_external_exposure(
                    snapshot.positions.iter().map(|p| p.position.as_str()),
                    covered_position_count,
                    snapshot.pending_orders.len(),
                    covered_order_count,
                    algos.samples.len(),
                    managed_algos.len(),
                )?;
                let occupancy = RiskAccountOccupancy {
                    account_key: c.account_key.clone(),
                    occupied_usdt: external_occupied.into(),
                    cumulative_loss_usd: exact(financial.1),
                };
                let read = started
                    .map(|started_after_held_ms| {
                        let clock = held_received.ok_or("POST_PROOF_CLOCK_MISSING")?;
                        let completed_after_held_ms = clock.elapsed().as_millis() as u64;
                        Ok::<_, String>(PostAccountReadProof {
                            account_key: c.account_key.clone(),
                            private_generation: generation,
                            source_read_id: digest(&(
                                &c.account_key,
                                generation,
                                &data,
                                started_after_held_ms,
                                completed_after_held_ms,
                            ))?,
                            content_digest: data.clone(),
                            started_after_held_ms,
                            completed_after_held_ms,
                        })
                    })
                    .transpose()?;
                Ok::<_, String>((c.account_key, occupancy, data, read, source_floor))
            })
            .buffer_unordered(8)
            .try_collect::<Vec<_>>()
            .await?;
        let mut source_floor = started_at;
        for (key, row, proof, read, account_floor) in pages {
            source_floor = source_floor.min(account_floor);
            evidence.push((key.clone(), proof));
            keys.push(key);
            occupied.push(row);
            if let Some(read) = read {
                post_reads.push(read);
            }
        }
        post_reads.sort_by(|a, b| a.account_key.cmp(&b.account_key));

        keys.sort();
        occupied.sort_by(|a, b| a.account_key.cmp(&b.account_key));
        let generated = now();
        if source_floor > generated
            || !crate::config::evidence_fresh(
                source_floor,
                generated,
                crate::config::ACCOUNT_MAX_AGE_MS,
            )
        {
            return Err("COHORT_EVIDENCE_STALE".into());
        }
        let content = canonical_census_content(evidence, &keys)?;
        Ok((
            NativeRiskCensus {
                account_keys: keys,
                occupied,
                generated_at_ms: generated as u64,
                source_digest: digest(&(content.clone(), source_floor, generated))?,
                content_digest: content,
                source_read_floor_ms: source_floor as u64,
                complete: true,
                native_account_evidence_complete: true,
                native_order_evidence_complete: true,
                native_ledger_evidence_complete: true,
            },
            post_reads,
        ))
    }
}
/// The pending derivative-algo census covers multiple private REST scopes.
/// A successful query or empty array without a fresh source timestamp is not
/// independently sufficient evidence that no unprotected order exists.
pub(super) fn verified_algo_source_time(
    source_received_at_ms: u64,
    observed_at_ms: i64,
) -> Result<i64, String> {
    let source =
        i64::try_from(source_received_at_ms).map_err(|_| "COHORT_ALGO_SOURCE_TIME_INVALID")?;
    if source <= 0
        || source > observed_at_ms
        || !crate::config::evidence_fresh(source, observed_at_ms, crate::config::ACCOUNT_MAX_AGE_MS)
    {
        return Err("COHORT_ALGO_SOURCE_STALE".into());
    }
    Ok(source)
}

/// A complete native census proves that all venue obligations are owned
/// by currently held central risk, instead of requiring a flat account.
/// Worker independently adds those central reservations to its risk book;
/// returning that amount as "external" would double count the same loss.
///
/// Only the exact linked objects may count as managed. Unproven positions,
/// orders or algos are rejected, never rounded down to zero.
pub(super) fn certified_managed_external_exposure<'a>(
    positions: impl IntoIterator<Item = &'a str>,
    covered_positions: usize,
    open_orders: usize,
    covered_orders: usize,
    conditional_orders: usize,
    covered_algos: usize,
) -> Result<&'static str, String> {
    let mut actual_positions = 0_usize;
    for amount in positions {
        if dec(amount)? != Decimal::ZERO {
            actual_positions = actual_positions
                .checked_add(1)
                .ok_or("COHORT_POSITION_COUNT_OVERFLOW")?;
        }
    }
    if actual_positions != covered_positions
        || open_orders != covered_orders
        || conditional_orders != covered_algos
    {
        return Err("COHORT_UNMANAGED_EXTERNAL_EXPOSURE".into());
    }
    Ok("0")
}

/// The reserved known quote is source-bound but NOT an all-in debit proof.
/// If any source changes after HELD, do not reuse the prior risk grant. Fail
/// rather than silently refresh the amount/cost or issue a second venue POST.
pub(super) fn verify_post_held_entry_sources(
    quote: &okx_execution::EntryQuoteEvidence,
    fee: &okx_observation::FeeScheduleSnapshot,
    reference_generation: &str,
    contract_value: Decimal,
    fx: &UsdConversion,
    observed_at_ms: i64,
) -> Result<(), String> {
    let current_fee = entry_budget::verified_entry_fee_rate(
        fee,
        &quote.instrument_id,
        reference_generation,
        observed_at_ms,
    )?;
    if quote.contract_value != contract_value
        || quote.reference_generation != reference_generation
        || quote.fee_generation != fee.fee_generation
        || quote.maker_rate != fee.maker_rate
        || quote.taker_rate != fee.taker_rate
        || quote.fee_level != fee.level
        || quote.fee_rate != current_fee
    {
        return Err("POST_ENTRY_FEE_OR_SPEC_CHANGED".into());
    }
    if !crate::config::evidence_fresh(
        quote.observed_at,
        observed_at_ms,
        crate::config::ACCOUNT_MAX_AGE_MS,
    ) {
        return Err("POST_ENTRY_QUOTE_EXPIRED".into());
    }
    if !fx.valid() {
        return Err("POST_ENTRY_FX_SOURCE_INVALID".into());
    }
    let fx_timestamp =
        i64::try_from(fx.observed_at_ms).map_err(|_| "POST_ENTRY_FX_TIMESTAMP_INVALID")?;
    if !crate::config::evidence_fresh(
        fx_timestamp,
        observed_at_ms,
        crate::config::ACCOUNT_MAX_AGE_MS,
    ) {
        return Err("POST_ENTRY_FX_STALE".into());
    }
    if quote.usd_per_usdt != dec(&fx.usd_per_usdt)? {
        return Err("POST_ENTRY_FX_CHANGED".into());
    }
    // The historical quote keeps its original source digest in SQL.
    // The authenticated post-HELD read can legitimately have a new account
    // generation: its FX source must be valid/fresh and its exact observed
    // conversion must still match the originally reserved rate.
    Ok(())
}

/// A terminal release may gather a cohort census while precisely ONE
/// SQL-backed reservation is still pending its central RELEASE. The caller
/// must already have validated the terminal order, fill and bill coverage,
/// zero attributed exposure, and a successful central SETTLE_ENTRY. This
/// scope authorizes no other unresolved, POSTING or UNKNOWN intent.
#[derive(Clone, Copy)]
pub(super) struct TerminalReleaseCensusScope<'a> {
    pub(super) account_key: &'a str,
    pub(super) intent_id: &'a str,
    pub(super) reservation_key: &'a str,
    pub(super) plan_hash: &'a str,
}

/// The SQL owner is the authority for the local send state. A missing venue
/// row does not prove no-send after POSTING/UNKNOWN; preserve those obligations
/// and deny further account risk until a terminal readback is reconciled.
#[cfg(test)]
pub(super) fn verify_local_intent_coverage(
    active: &[(StoredIntent, Option<CentralReceipt>)],
    unresolved: &[String],
    account_key: &str,
) -> Result<String, String> {
    verify_local_intent_coverage_scoped(active, unresolved, account_key, None)
}

/// The only exception to terminal-pending denial is the exact intent whose
/// central RELEASE is about to be constructed from a settled SQL proof.
/// The pending reservation remains included in the content digest and all
/// unrelated intents retain the normal fail-closed semantics.
#[cfg(test)]
pub(super) fn verify_local_intent_coverage_scoped(
    active: &[(StoredIntent, Option<CentralReceipt>)],
    unresolved: &[String],
    account_key: &str,
    terminal_release: Option<TerminalReleaseCensusScope<'_>>,
) -> Result<String, String> {
    verify_local_intent_coverage_with_managed(
        active,
        unresolved,
        account_key,
        terminal_release,
        &std::collections::BTreeSet::new(),
        &std::collections::BTreeSet::new(),
    )
}

/// A fully FILLED entry can still own LIVE managed exposure. Its central
/// reservation must remain OPEN, not be released as though the position were
/// flat. Only the existing source-bound venue-position/SQL-lot/protective-algo
/// check populates this set. The ordinary scoped release proof remains strict.
pub(super) fn verify_local_intent_coverage_with_managed(
    active: &[(StoredIntent, Option<CentralReceipt>)],
    unresolved: &[String],
    account_key: &str,
    terminal_release: Option<TerminalReleaseCensusScope<'_>>,
    verified_live_intents: &std::collections::BTreeSet<String>,
    verified_terminal_peers: &std::collections::BTreeSet<String>,
) -> Result<String, String> {
    const PAGE_LIMIT: usize = 100;
    if active.len() >= PAGE_LIMIT {
        return Err("COHORT_LOCAL_INTENTS_TRUNCATED".into());
    }
    let mut ids = std::collections::BTreeSet::new();
    let mut release_scope_matched = false;
    if unresolved.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err("COHORT_UNRESOLVED_INTENT_SCOPE_INVALID".into());
    }
    for (intent, receipt) in active {
        if !ids.insert(intent.intent_id.as_str())
            || intent.plan_hash.is_empty()
            || intent.plan.quantity <= Decimal::ZERO
        {
            return Err("COHORT_LOCAL_INTENT_CORRUPT".into());
        }
        if matches!(intent.state, IntentState::Posting | IntentState::Unknown) {
            return Err("COHORT_UNRESOLVED_POST_OUTCOME".into());
        }
        let central_pending = unresolved.binary_search(&intent.intent_id).is_ok();
        if intent.state.terminal() && central_pending {
            // FILLED is terminal for this ORDER, not necessarily for the
            // owned POSITION. This is only safe when the same census has
            // already proved the venue quantity == local lots, matched the
            // live stop and independently held central reservation.
            let managed_open_entry = intent.state == IntentState::Filled
                && intent.risk_increase
                && verified_live_intents.contains(&intent.intent_id)
                && receipt.as_ref().is_some_and(|r| {
                    r.account_key == account_key
                        && r.intent_id == intent.intent_id
                        && r.plan_hash == intent.plan_hash
                        && !r.reservation_key.starts_with("local:")
                        && !r.reservation_key.is_empty()
                });
            let matching_release = terminal_release.is_some_and(|scope| {
                scope.account_key == account_key
                    && scope.intent_id == intent.intent_id
                    && scope.plan_hash == intent.plan_hash
                    && receipt.as_ref().is_some_and(|r| {
                        r.account_key == scope.account_key
                            && r.intent_id == scope.intent_id
                            && r.plan_hash == scope.plan_hash
                            && r.reservation_key == scope.reservation_key
                    })
            });
            // Only the actual terminal release census can recognize a
            // separately source-proven flat terminal peer. That peer's
            // RiskBook receipt remains held; this releases ONLY the selected
            // reservation, never the peer or a new entry grant.
            let proved_flat_peer = terminal_release.is_some_and(|scope| {
                scope.account_key == account_key
                    && scope.intent_id != intent.intent_id
                    && intent.risk_increase
                    && intent.state == IntentState::Filled
                    && verified_terminal_peers.contains(&intent.intent_id)
                    && receipt.as_ref().is_some_and(|r| {
                        r.account_key == account_key
                            && r.intent_id == intent.intent_id
                            && r.plan_hash == intent.plan_hash
                            && !r.reservation_key.starts_with("local:")
                            && !r.reservation_key.is_empty()
                    })
            });
            if !matching_release && !managed_open_entry && !proved_flat_peer {
                return Err(match terminal_release {
                    Some(scope) if scope.intent_id == intent.intent_id => {
                        "COHORT_TERMINAL_RELEASE_RECEIPT_MISMATCH"
                    }
                    Some(_) => "COHORT_OTHER_TERMINAL_RISK_RELEASE_PENDING",
                    None => "COHORT_TERMINAL_RISK_RELEASE_PENDING",
                }
                .into());
            }
            if matching_release {
                release_scope_matched = true;
            }
        }
        if intent.state == IntentState::Prepared && central_pending {
            return Err("COHORT_UNRESOLVED_RESERVE_ATTEMPT".into());
        }
        if matches!(
            intent.state,
            IntentState::Acknowledged | IntentState::PartiallyFilled
        ) && intent
            .exchange_order_id
            .as_ref()
            .is_none_or(String::is_empty)
        {
            return Err("COHORT_EXCHANGE_ORDER_ID_UNPROVEN".into());
        }
        if intent.risk_increase {
            if intent.state != IntentState::Prepared && receipt.is_none() {
                return Err("COHORT_CENTRAL_RESERVATION_MISSING".into());
            }
            if let Some(receipt) = receipt
                && (receipt.account_key != account_key
                    || receipt.intent_id != intent.intent_id
                    || receipt.plan_hash != intent.plan_hash
                    || receipt.reservation_key.starts_with("local:")
                    || receipt.reservation_key.is_empty()
                    || receipt.policy_revision != intent.policy_revision)
            {
                return Err("COHORT_CENTRAL_RESERVATION_MISMATCH".into());
            }
        }
    }
    if unresolved.iter().any(|id| !ids.contains(id.as_str()))
        || verified_live_intents
            .iter()
            .any(|id| !ids.contains(id.as_str()))
        || verified_terminal_peers
            .iter()
            .any(|id| !ids.contains(id.as_str()))
    {
        return Err("COHORT_UNRESOLVED_INTENT_SCOPE_INVALID".into());
    }
    if terminal_release.is_some() && !release_scope_matched {
        return Err("COHORT_TERMINAL_RELEASE_SCOPE_UNPROVEN".into());
    }
    digest(&(
        active,
        unresolved,
        verified_live_intents,
        verified_terminal_peers,
    ))
}

/// Concurrent account reads must produce one deterministic, complete cohort
/// digest regardless of completion order. Duplicated/missing keys fail closed.
pub(super) fn canonical_census_content(
    mut evidence: Vec<(String, String)>,
    expected_keys: &[String],
) -> Result<String, String> {
    evidence.sort_by(|a, b| a.0.cmp(&b.0));
    if evidence.len() != expected_keys.len()
        || !expected_keys.windows(2).all(|pair| pair[0] < pair[1])
        || evidence
            .iter()
            .zip(expected_keys)
            .any(|((key, _), expected)| key != expected)
    {
        return Err("COHORT_EVIDENCE_SCOPE_MISMATCH".into());
    }
    digest(&evidence)
}

/// A matching client ID alone does not prove that a live venue obligation
/// belongs to this durable intent. Compare the actual order and saved plan.
pub(super) fn verify_pending_order_binding(
    order: &okx_observation::PendingOrderState,
    intent: &StoredIntent,
) -> Result<(), String> {
    if order.client_order_id.as_deref() != Some(intent.client_order_id.as_str())
        || order.order_id.is_empty()
        || intent.exchange_order_id.as_deref() != Some(order.order_id.as_str())
        || order.instrument_id != intent.instrument_id
        || order.side != intent.plan.side
        || !matches!(
            order.position_side.as_deref(),
            Some("net") | Some("long") | Some("short")
        )
        || (order.position_side.as_deref() != Some("net")
            && order.position_side.as_deref() != Some(intent.plan.position_side.as_str()))
        || (order.position_side.as_deref() == Some("net")
            && ((intent.risk_increase && order.reduce_only == Some(true))
                || (!intent.risk_increase && order.reduce_only != Some(true))))
        || !matches!(order.state.as_str(), "live" | "partially_filled")
        || intent.state.terminal()
    {
        return Err("PENDING_ORDER_INTENT_MISMATCH".into());
    }
    let qty = dec(&order.size)?;
    let filled = dec(&order.accumulated_fill_size)?;
    if qty <= Decimal::ZERO || qty != intent.plan.quantity || filled < Decimal::ZERO || filled > qty
    {
        return Err("PENDING_ORDER_QUANTITY_MISMATCH".into());
    }
    if intent.risk_increase {
        let price = dec(order
            .price
            .as_deref()
            .ok_or("PENDING_ORDER_PRICE_UNPROVEN")?)?;
        if price <= Decimal::ZERO
            || intent.plan.limit_price <= Decimal::ZERO
            || (intent.plan.side == "buy" && price > intent.plan.limit_price)
            || (intent.plan.side == "sell" && price < intent.plan.limit_price)
        {
            return Err("PENDING_ORDER_PRICE_BOUND_MISMATCH".into());
        }
    }
    Ok(())
}

/// Validate actual source time, not the timestamp at which a new census is
/// assembled. Both the venue account read and SQLite financial projection
/// must be current before a complete native risk witness may be issued.
pub(super) fn census_source_floor(
    account_received_at: &str,
    observed_uid: &str,
    expected_uid: &str,
    nav_complete: bool,
    financial_as_of: i64,
    observed_at: i64,
) -> Result<i64, String> {
    if observed_uid != expected_uid {
        return Err("COHORT_IDENTITY_MISMATCH".into());
    }
    let account_as_of = chrono::DateTime::parse_from_rfc3339(account_received_at)
        .map_err(|_| "COHORT_ACCOUNT_SOURCE_TIME_INVALID")?
        .timestamp_millis();
    if account_as_of <= 0
        || account_as_of > observed_at
        || !crate::config::evidence_fresh(
            account_as_of,
            observed_at,
            crate::config::ACCOUNT_MAX_AGE_MS,
        )
    {
        return Err("COHORT_ACCOUNT_SOURCE_STALE".into());
    }
    if !nav_complete {
        return Err("COHORT_FINANCIAL_SOURCE_INCOMPLETE".into());
    }
    if financial_as_of <= 0
        || financial_as_of > observed_at
        || !crate::config::evidence_fresh(
            financial_as_of,
            observed_at,
            crate::config::ACCOUNT_MAX_AGE_MS,
        )
    {
        return Err("COHORT_FINANCIAL_SOURCE_STALE".into());
    }
    Ok(account_as_of.min(financial_as_of))
}

/// Compare exact 9-decimal USD/USDT conversion values on native fresh
/// post-HELD snapshot; content equality is allowed only after a new read.
pub(super) fn verify_post_held_fx(reserved: &str, observed: &str) -> Result<(), String> {
    let before = dec(reserved)?;
    let after = dec(observed)?;
    if before <= Decimal::ZERO || after <= Decimal::ZERO {
        return Err("POST_FX_SOURCE_INVALID".into());
    }
    if before != after {
        return Err("POST_FX_SOURCE_CHANGED_REQUIRES_NEW_RESERVE".into());
    }
    Ok(())
}

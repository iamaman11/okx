use super::*;
use okx_api::{
    ApiOrderSide, ApiOrderType, ApiPositionSide, ApiTradeMode, ApiTriggerPriceType,
    AttachedAlgoOrderRequest, PlaceOrderRequest,
};
impl TradeService {
    pub(super) async fn execute(
        &self,
        r: &Arc<AccountResources>,
        request: &TradeRequest,
        supplied: Option<&CentralGovernanceSnapshot>,
    ) -> Result<Value, String> {
        self.execute_bound(r, request, supplied, None).await
    }
    pub(super) async fn execute_bound(
        &self,
        r: &Arc<AccountResources>,
        request: &TradeRequest,
        supplied: Option<&CentralGovernanceSnapshot>,
        bound_hash: Option<String>,
    ) -> Result<Value, String> {
        self.execute_inner(r, request, supplied, bound_hash, None, None)
            .await
    }
    pub(super) async fn execute_for_definition(
        &self,
        r: &Arc<AccountResources>,
        request: &TradeRequest,
        supplied: Option<&CentralGovernanceSnapshot>,
        version: String,
        decision_digest: String,
    ) -> Result<Value, String> {
        self.execute_inner(r, request, supplied, None, Some(version), Some(decision_digest))
            .await
    }
    async fn execute_inner(
        &self,
        r: &Arc<AccountResources>,
        request: &TradeRequest,
        supplied: Option<&CentralGovernanceSnapshot>,
        bound_hash: Option<String>,
        expected_definition: Option<String>,
        claimed_input_digest: Option<String>,
    ) -> Result<Value, String> {
        let TradeRequest::Execute {
            idempotency_key,
            operation,
            strategy_instance,
            instrument,
            side,
            sizing,
            order,
            protection,
            expires_in_ms,
        } = request
        else {
            return Err("EXECUTE_REQUIRED".into());
        };
        let started = Instant::now();
        let until = started + Duration::from_millis(*expires_in_ms);
        let _capacity = r
            .mutation_capacity
            .try_acquire()
            .map_err(|_| "ACCOUNT_MUTATION_BUSY")?;
        let body_hash =
            bound_hash.unwrap_or(digest(&(r.configuration.account_key.wire(), request))?);
        let key = idempotency_key.clone();
        let prior = r
            .execution
            .store
            .call(until, move |s| s.by_idempotency(&key))
            .await
            .map_err(err)?;
        if let Some(i) = prior {
            if i.request_hash != body_hash {
                return Err("IDEMPOTENCY_CONFLICT".into());
            }
            drop(_capacity);
            return self.status_intent(r, &i.intent_id).await;
        }
        let (state, control) = self.authority(r, supplied).await?;
        let increase = matches!(
            operation,
            TradeOperation::Open | TradeOperation::Add | TradeOperation::Replace
        );
        let plan = loop {
            match self
                .plan(
                    r,
                    &control,
                    *operation,
                    strategy_instance,
                    instrument,
                    *side,
                    sizing,
                    order,
                    protection.as_ref(),
                    until,
                )
                .await
            {
                Ok(plan) => break plan,
                Err(reason)
                    if Instant::now() + Duration::from_millis(100) < until
                        && [
                            "ACCOUNT_SOURCE_GENERATION_UNVERIFIED",
                            "ACCOUNT_EVIDENCE_EXPIRED_DURING_PLAN",
                            "FINANCIAL_SOURCE_INCOMPLETE_OR_STALE",
                            "PRIVATE_SOURCE_GENERATION_CHANGED",
                            "PUBLIC_SOURCE_GENERATION_CHANGED",
                        ]
                        .iter()
                        .any(|code| reason.contains(code)) =>
                {
                    let remaining = until.saturating_duration_since(Instant::now());
                    let _ = tokio::time::timeout(
                        remaining,
                        crate::source_capture::capture_account_sources(
                            &r.observer,
                            &r.private_ws,
                            &r.execution,
                        ),
                    )
                    .await;
                }
                Err(reason) => return Err(reason),
            }
        };
        let id = format!("intent_{}", &body_hash[..32]);
        let client_id = body_hash[..32].to_owned();
        let decision = Decision {
            decision_id: if claimed_input_digest.is_some() {
                format!(
                    "strategy:{}:{}:{}",
                    strategy_instance,
                    expected_definition.as_deref().ok_or("STRATEGY_DEFINITION_MISSING")?,
                    idempotency_key,
                )
            } else {
                format!("decision_{}", &body_hash[..32])
            },
            instance_id: Some(strategy_instance.clone()),
            decision_at: now(),
            input_digest: match claimed_input_digest {
                Some(digest) => digest,
                None => digest(&(body_hash.clone(), &plan))?,
            },
            policy_revision: state.state.revision as i64,
            code_version: crate::accounts::build_release().into(),
        };
        let intent = NewIntent {
            plan: plan.clone(),
            intent_id: id.clone(),
            idempotency_key: idempotency_key.clone(),
            request_hash: body_hash,
            client_order_id: client_id.clone(),
            deadline: now() + until.saturating_duration_since(Instant::now()).as_millis() as i64,
            plan_hash: digest(&plan)?,
            policy_revision: state.state.revision as i64,
            risk_increase: increase,
            instrument_id: instrument.clone(),
        };
        if expected_definition
            .as_ref()
            .is_some_and(|version| version != &intent.plan.strategy_definition_version)
        {
            return Err("STRATEGY_VERSION_CHANGED".into());
        }
        let manual_budget = dec(&state.state.campaign_loss_limit_usd)?;
        let owner_account = r.configuration.account_key.wire();
        let owner_uid = r.execution.uid().to_owned();
        let prepared = prepare_on_owner(
            &r.execution.store,
            until,
            &owner_account,
            &owner_uid,
            move |s| {
                if intent.plan.strategy_instance == "manual"
                    && !s.instances()?.iter().any(|i| i.instance_id == "manual")
                {
                    let manual = StrategyInstance {
                        instance_id: "manual".into(),
                        definition_id: "manual".into(),
                        definition_version: "1".into(),
                        parameters: "{}".into(),
                        enabled: false,
                        policy_revision: 1,
                        budget: manual_budget,
                        last_decision_key: None,
                    };
                    let h = format!(
                        "{:x}",
                        Sha256::digest(format!("{}:manual", s.meta().account_key).as_bytes())
                    );
                    s.mutate_instance(
                        "internal_create_manual",
                        &h,
                        "create",
                        &manual,
                        None,
                        now(),
                    )?;
                }
                s.prepare(&decision, &intent)
            },
        )
        .await?;
        if Instant::now() >= until {
            return Err("COMMAND_EXPIRED".into());
        }
        let mut central = None;
        if increase
            && r.configuration.account_key.environment == okx_protocol::accounts::Environment::Real
        {
            let census = self.census(&state).await?;
            let snapshot = r.observer.snapshot().await.map_err(err)?;
            let conversion = Self::usd_conversion(&snapshot)?;
            let usdt = (plan.planned_loss_usd / dec(&conversion.usd_per_usdt)?)
                .round_dp_with_strategy(9, RoundingStrategy::ToPositiveInfinity);
            let usd = (usdt * dec(&conversion.usd_per_usdt)?)
                .round_dp_with_strategy(9, RoundingStrategy::ToPositiveInfinity);
            let reserve = self.risk_command(
                &state,
                format!("reserve_{}", client_id),
                RiskCommandAction::Reserve {
                    venue: "okx".into(),
                    account_uid: r.configuration.account_key.verified_uid.clone(),
                    run_id: "continuous_engine".into(),
                    intent_id: id.clone(),
                    strategy_id: strategy_instance.clone(),
                    instrument_id: instrument.clone(),
                    plan_sha256: prepared.plan_hash.clone(),
                    requested_loss_usdt: exact(usdt),
                    requested_loss_usd: exact(usd),
                    usd_conversion: conversion.clone(),
                    policy_revision: state.state.revision,
                    census,
                },
            );
            let held = self
                .reserve_inspect_retry(&state, reserve, &prepared, r)
                .await?;
            let receipt = self.local_receipt(r, &prepared, &held, usd, &conversion)?;
            r.execution
                .store
                .call(until, move |s| s.reserve(&receipt, now()))
                .await
                .map_err(err)?;
            if Instant::now() >= until {
                return Err("COMMAND_EXPIRED".into());
            }
            match self
                .fence_inspect_retry(&state, &held, &prepared, until)
                .await
            {
                Ok(v) => central = Some(v),
                Err(reason) => return Ok(uncertain(&id, &reason)),
            }
        } else {
            // Demo and native-confirmed reductions use the same durable marker; no Real global capacity is minted.
            let receipt = CentralReceipt {
                reservation_key: format!("local:{}", id),
                account_key: r.configuration.account_key.wire(),
                installation_id: self.accounts.installation_id().into(),
                instance_id: Some(strategy_instance.clone()),
                intent_id: id.clone(),
                plan_hash: prepared.plan_hash.clone(),
                policy_revision: state.state.revision as i64,
                receipt_hash: digest(&prepared)?,
                requested_loss_usd: plan.planned_loss_usd,
                conversion_digest: digest(&plan)?,
            };
            r.execution
                .store
                .call(until, move |s| s.reserve(&receipt, now()))
                .await
                .map_err(err)?;
        }
        let revision = self
            .governance
            .read()
            .await
            .as_ref()
            .ok_or("AUTHORITY_UNAVAILABLE")?
            .state
            .revision;
        if revision != state.state.revision {
            return Err("POLICY_CHANGED".into());
        }
        let market = self
            .public_ws
            .fresh_snapshot(
                instrument,
                now() as u64,
                crate::config::QUOTE_MAX_AGE_MS,
                Utc::now().to_rfc3339(),
            )
            .await
            .map_err(err)?;
        validate_generation(plan.source_generation, market.generation as i64)?;
        if increase
            && (!r.source_identity_ready().await
                || r.private_ws.status().await.connection_generation as i64
                    != plan.private_source_generation)
        {
            return Err("ACCOUNT_SOURCE_GENERATION_UNVERIFIED".into());
        }
        if Instant::now() >= until {
            return Err("COMMAND_EXPIRED".into());
        }
        let clock = r.execution.clock.clock_evidence().await.map_err(err)?;
        let timing = clock
            .mutation_timing(
                until
                    .saturating_duration_since(Instant::now())
                    .as_millis()
                    .min(crate::config::ACCOUNT_MAX_AGE_MS as u128) as u64,
            )
            .map_err(err)?;
        let attached = plan
            .stop_price
            .map(|stop| AttachedAlgoOrderRequest {
                client_order_id: format!("s{}", &client_id[..31]),
                take_profit_trigger_price: String::new(),
                take_profit_trigger_price_type: ApiTriggerPriceType::Mark,
                take_profit_order_price: String::new(),
                stop_loss_trigger_price: exact(stop),
                stop_loss_trigger_price_type: ApiTriggerPriceType::Mark,
                stop_loss_order_price: "-1".into(),
            })
            .into_iter()
            .collect();
        let venue = PlaceOrderRequest {
            instrument_id: instrument.clone(),
            trade_mode: ApiTradeMode::Cross,
            client_order_id: client_id,
            side: if plan.side == "buy" {
                ApiOrderSide::Buy
            } else {
                ApiOrderSide::Sell
            },
            position_side: if *side == PositionSide::Long {
                ApiPositionSide::Long
            } else {
                ApiPositionSide::Short
            },
            order_type: match order.order_type.as_str() {
                "limit" => ApiOrderType::Limit,
                "post_only" => ApiOrderType::PostOnly,
                "ioc" | "market" => ApiOrderType::Ioc,
                _ => ApiOrderType::Fok,
            },
            size: exact(plan.quantity),
            price: exact(plan.limit_price),
            attached_algo_orders: if increase { attached } else { Vec::new() },
        };
        let rate = r.execution.trade.admit_place_order(&venue).map_err(err)?;
        let result = post_once(&r.execution.store, &prepared, until, || async {
            if self
                .governance
                .read()
                .await
                .as_ref()
                .is_none_or(|v| v.state.revision != prepared.policy_revision as u64)
            {
                return Err("POLICY_CHANGED_BEFORE_SEND".into());
            }
            if Instant::now() >= until {
                return Err("COMMAND_EXPIRED_BEFORE_SEND".into());
            }
            r.execution
                .trade
                .place_order_after_admission(&venue, &timing, &rate)
                .await
                .map_err(err)
        })
        .await?;
        drop(_capacity);
        match result {
            Some(response) => {
                if response.top_level_success()
                    && response.data.len() == 1
                    && response.data[0].accepted()
                    && response.data[0].client_order_id == venue.client_order_id
                {
                    let order_id = response.data[0].order_id.clone();
                    let intent_id = id.clone();
                    r.execution
                        .store
                        .call(deadline(), move |s| {
                            s.record_outcome(
                                &intent_id,
                                IntentState::Acknowledged,
                                Some(&order_id),
                                "exchange-ack",
                                now(),
                            )
                        })
                        .await
                        .map_err(err)?;
                    self.status_intent(r, &id).await
                } else {
                    let i = id.clone();
                    r.execution
                        .store
                        .call(deadline(), move |s| {
                            s.record_outcome(
                                &i,
                                IntentState::Rejected,
                                None,
                                "exchange-explicit-rejection",
                                now(),
                            )
                        })
                        .await
                        .map_err(err)?;
                    Ok(completed(
                        json!({"intent_id":id,"state":"REJECTED","exchange_code":response.code,"central_reservation":central}),
                    ))
                }
            }
            None => Ok(uncertain(&id, "EXCHANGE_POST_OUTCOME_UNKNOWN")),
        }
    }
    pub(super) fn risk_command(
        &self,
        state: &CentralGovernanceSnapshot,
        id: String,
        action: RiskCommandAction,
    ) -> RiskCommand {
        RiskCommand {
            schema: RISK_COMMAND_SCHEMA_V1.into(),
            command_id: id,
            installation_id: state.state.installation_id.clone(),
            action,
        }
    }
    pub(super) fn local_receipt(
        &self,
        r: &AccountResources,
        intent: &StoredIntent,
        result: &RiskResult,
        usd: Decimal,
        conversion: &UsdConversion,
    ) -> Result<CentralReceipt, String> {
        Ok(CentralReceipt {
            reservation_key: result
                .reservation_key
                .clone()
                .ok_or("RESERVATION_KEY_MISSING")?,
            account_key: r.configuration.account_key.wire(),
            installation_id: self.accounts.installation_id().into(),
            instance_id: Some(intent.plan.strategy_instance.clone()),
            intent_id: intent.intent_id.clone(),
            plan_hash: intent.plan_hash.clone(),
            policy_revision: intent.policy_revision,
            receipt_hash: digest(result)?,
            requested_loss_usd: usd,
            conversion_digest: digest(conversion)?,
        })
    }
    async fn reserve_inspect_retry(
        &self,
        state: &CentralGovernanceSnapshot,
        command: RiskCommand,
        intent: &StoredIntent,
        r: &AccountResources,
    ) -> Result<RiskResult, String> {
        match self.risk.request(command.clone()).await {
            Ok(v) if v.status == RiskDecisionStatus::Held => Ok(v),
            Ok(v) => Err(v.reason.unwrap_or("RESERVE_DENIED".into())),
            Err(_) => {
                let key = [
                    "okx",
                    "real",
                    r.configuration.account_key.verified_uid.as_str(),
                    "continuous_engine",
                    intent.intent_id.as_str(),
                ]
                .iter()
                .map(|x| format!("{}:{}", x.len(), x))
                .collect::<String>();
                let inspect = self.risk_command(
                    state,
                    format!("inspect_{}", &intent.plan_hash[..32]),
                    RiskCommandAction::Inspect {
                        reservation_key: key,
                        plan_sha256: intent.plan_hash.clone(),
                    },
                );
                let observed = self.risk.request(inspect).await.map_err(str::to_owned)?;
                if observed.status != RiskDecisionStatus::Observed
                    || observed.reservation_state != Some(RiskReservationState::Held)
                {
                    return Err("RESERVE_OUTCOME_UNKNOWN_NO_POST".into());
                }
                // Inspection confirms the one original reservation. A stale reserve-time
                // source is never replayed as a second grant after its transport timeout.
                Ok(observed)
            }
        }
    }
    async fn fence_inspect_retry(
        &self,
        state: &CentralGovernanceSnapshot,
        held: &RiskResult,
        intent: &StoredIntent,
        until: Instant,
    ) -> Result<RiskResult, String> {
        let key = held
            .reservation_key
            .clone()
            .ok_or("RESERVATION_KEY_MISSING")?;
        let mut sequence = held
            .risk_book_sequence
            .ok_or("RESERVATION_SEQUENCE_MISSING")?;
        for _ in 0..4 {
            if Instant::now() >= until {
                return Err("COMMAND_EXPIRED".into());
            }
            let census = self.census(state).await?;
            let command = self.risk_command(
                state,
                format!("fence_{}", intent.client_order_id),
                RiskCommandAction::BeginPost {
                    reservation_key: key.clone(),
                    plan_sha256: intent.plan_hash.clone(),
                    policy_revision: state.state.revision,
                    expected_risk_book_sequence: sequence,
                    source_proof_sha256: digest(&census)?,
                    census,
                },
            );
            match self.risk.request(command).await {
                Ok(v) if v.status == RiskDecisionStatus::PostAuthorized => return Ok(v),
                Ok(v) if v.reason.as_deref() != Some("STALE_RISK_BOOK_SEQUENCE") => {
                    return Err(v.reason.unwrap_or("CENTRAL_POST_FENCE_DENIED".into()));
                }
                _ => {
                    let inspected = self
                        .risk
                        .request(self.risk_command(
                            state,
                            format!("inspect_fence_{}", intent.client_order_id),
                            RiskCommandAction::Inspect {
                                reservation_key: key.clone(),
                                plan_sha256: intent.plan_hash.clone(),
                            },
                        ))
                        .await
                        .map_err(str::to_owned)?;
                    if inspected.reservation_state != Some(RiskReservationState::Held) {
                        return Err("POST_FENCE_OUTCOME_UNKNOWN_NO_POST".into());
                    }
                    sequence = inspected
                        .risk_book_sequence
                        .ok_or("RESERVATION_SEQUENCE_MISSING")?;
                }
            }
        }
        Err("CENTRAL_RISK_CAS_BUSY_NO_POST".into())
    }
    pub(super) async fn census(
        &self,
        state: &CentralGovernanceSnapshot,
    ) -> Result<NativeRiskCensus, String> {
        if !self.accounts.failures().await.is_empty() {
            return Err("ACCOUNT_COHORT_UNAVAILABLE".into());
        }
        let mut keys = Vec::new();
        let mut occupied = Vec::new();
        let mut evidence = Vec::new();
        let floor = now();
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
                let r = self.resources(&c.account_key).await?;
                if !r.source_identity_ready().await {
                    return Err("COHORT_SOURCE_GENERATION_UNVERIFIED".into());
                }
                let generation = r.private_ws.status().await.connection_generation;
                let snapshot = r.observer.snapshot().await.map_err(err)?;
                let algos = r
                    .observer
                    .pending_derivative_risk_algos()
                    .await
                    .map_err(err)?;
                if !algos.complete_within_bound
                    || snapshot.account_uid_fingerprint != c.uid_fingerprint
                {
                    return Err("COHORT_EVIDENCE_INCOMPLETE".into());
                }
                let mut managed_algos = std::collections::BTreeSet::new();
                for p in &snapshot.positions {
                    let actual_qty = dec(&p.position)?;
                    if actual_qty == Decimal::ZERO {
                        continue;
                    }
                    let instrument = p.instrument_id.clone();
                    let lots = r
                        .execution
                        .store
                        .call(deadline(), move |s| s.active_lots(&instrument))
                        .await
                        .map_err(err)?;
                    let covered = lots
                        .iter()
                        .filter(|l| l.side == p.position_side)
                        .fold(Decimal::ZERO, |s, l| s + l.remaining_qty);
                    if covered != dec(&p.position)?.abs() {
                        return Err("UNATTRIBUTED_EXCHANGE_POSITION".into());
                    }
                    for lot in lots.iter().filter(|l| l.side == p.position_side) {
                        let id = lot.intent_id.clone().ok_or("UNATTRIBUTED_POSITION_LOT")?;
                        let owner_id = id.clone();
                        let (protection, receipt) = r
                            .execution
                            .store
                            .call(deadline(), move |s| {
                                Ok((s.protection(&id)?, s.central_receipt(&id)?))
                            })
                            .await
                            .map_err(err)?;
                        let protection = protection.ok_or("PROTECTION_UNVERIFIED")?;
                        let receipt = receipt.ok_or("CENTRAL_RESERVATION_UNVERIFIED")?;
                        let owned_qty = lots
                            .iter()
                            .filter(|l| l.intent_id.as_deref() == Some(&owner_id))
                            .fold(Decimal::ZERO, |n, l| n + l.remaining_qty);
                        if protection.quantity < owned_qty
                            || receipt.plan_hash.is_empty()
                            || receipt.reservation_key.starts_with("local:")
                        {
                            return Err("PROTECTION_OR_RESERVATION_COVERAGE_INCOMPLETE".into());
                        }
                        managed_algos.insert(protection.algo_id);
                    }
                }
                for order in &snapshot.pending_orders {
                    let client = order
                        .client_order_id
                        .clone()
                        .ok_or("UNATTRIBUTED_PENDING_ORDER")?;
                    let known = r
                        .execution
                        .store
                        .call(deadline(), move |s| s.by_client_order_id(&client))
                        .await
                        .map_err(err)?;
                    if known.is_none() {
                        return Err("UNATTRIBUTED_PENDING_ORDER".into());
                    }
                }
                if algos
                    .samples
                    .iter()
                    .map(|a| a.algo_order_id.clone())
                    .collect::<std::collections::BTreeSet<_>>()
                    != managed_algos
                {
                    return Err("UNATTRIBUTED_CONDITIONAL_ORDER".into());
                }
                if r.private_ws.status().await.connection_generation != generation
                    || !r.source_identity_ready().await
                    || !r
                        .execution
                        .verified_uid_matches(&c.uid_fingerprint)
                        .await
                        .map_err(err)?
                {
                    return Err("COHORT_SOURCE_GENERATION_CHANGED".into());
                }
                let data = digest(&(snapshot, algos))?;
                let financial = r
                    .execution
                    .store
                    .call(deadline(), |s| {
                        Ok((s.financial_risk(now(), None)?, s.policy()?.4))
                    })
                    .await
                    .map_err(err)?;
                if !financial.0.nav_complete {
                    return Err("COHORT_FINANCIAL_SOURCE_INCOMPLETE".into());
                }
                let occupancy = RiskAccountOccupancy {
                    account_key: c.account_key.clone(),
                    occupied_usdt: "0".into(),
                    cumulative_loss_usd: exact(financial.1),
                };
                Ok::<_, String>((c.account_key, occupancy, data))
            })
            .buffer_unordered(8)
            .try_collect::<Vec<_>>()
            .await?;
        for (key, row, proof) in pages {
            keys.push(key);
            occupied.push(row);
            evidence.push(proof);
        }

        keys.sort();
        occupied.sort_by(|a, b| a.account_key.cmp(&b.account_key));
        let generated = now();
        if !crate::config::evidence_fresh(floor, generated, crate::config::ACCOUNT_MAX_AGE_MS) {
            return Err("COHORT_EVIDENCE_STALE".into());
        }
        let content = digest(&evidence)?;
        Ok(NativeRiskCensus {
            account_keys: keys,
            occupied,
            generated_at_ms: generated as u64,
            source_digest: digest(&(content.clone(), floor, generated))?,
            content_digest: content,
            source_read_floor_ms: floor as u64,
            complete: true,
            native_account_evidence_complete: true,
            native_order_evidence_complete: true,
            native_ledger_evidence_complete: true,
        })
    }
}
pub(super) async fn prepare_on_owner<T, F>(
    store: &AccountStoreHandle,
    until: Instant,
    account: &str,
    verified_uid: &str,
    prepare: F,
) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce(&mut okx_execution::AccountStore) -> okx_execution::StoreResult<T> + Send + 'static,
{
    let account = account.to_owned();
    let uid = verified_uid.to_owned();
    store
        .call(until, move |s| {
            if s.meta().account_key != account || account.rsplit(':').next() != Some(uid.as_str()) {
                return Err(okx_execution::StoreError::Invalid(
                    "VERIFIED_UID_OWNER_MISMATCH",
                ));
            }
            prepare(s)
        })
        .await
        .map_err(err)
}
fn validate_generation(expected: i64, observed: i64) -> Result<(), String> {
    if expected <= 0 || expected != observed {
        Err("SOURCE_GENERATION_CHANGED".into())
    } else {
        Ok(())
    }
}
/// The one durable send boundary, shared by real and fixture gateways.
pub(super) async fn post_once<T, F, Fut>(
    store: &AccountStoreHandle,
    intent: &StoredIntent,
    until: Instant,
    send: F,
) -> Result<Option<T>, String>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    if Instant::now() >= until {
        return Err("COMMAND_EXPIRED".into());
    }
    let id = intent.intent_id.clone();
    let revision = intent.policy_revision;
    if !store
        .call(until, move |s| s.begin_post(&id, revision, now()))
        .await
        .map_err(err)?
    {
        return Ok(None);
    }
    if Instant::now() >= until {
        let id = intent.intent_id.clone();
        store
            .call(deadline(), move |s| {
                s.record_outcome(
                    &id,
                    IntentState::Rejected,
                    None,
                    "monotonic-deadline-expired-owner-confirmed-unsent",
                    now(),
                )
            })
            .await
            .map_err(err)?;
        return Err("COMMAND_EXPIRED".into());
    }
    match send().await {
        Ok(v) => Ok(Some(v)),
        Err(reason)
            if reason == "COMMAND_EXPIRED_BEFORE_SEND"
                || reason == "POLICY_CHANGED_BEFORE_SEND" =>
        {
            let id = intent.intent_id.clone();
            let proof = reason.clone();
            store
                .call(deadline(), move |s| {
                    s.record_outcome(
                        &id,
                        IntentState::Rejected,
                        None,
                        &format!("owner-confirmed-unsent:{proof}"),
                        now(),
                    )
                })
                .await
                .map_err(err)?;
            Err(reason)
        }
        Err(_) => {
            let id = intent.intent_id.clone();
            store
                .call(deadline(), move |s| {
                    s.record_outcome(
                        &id,
                        IntentState::Unknown,
                        None,
                        "exchange-post-outcome-unknown",
                        now(),
                    )
                })
                .await
                .map_err(err)?;
            Ok(None)
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct FixtureGateway {
        posts: AtomicUsize,
        lose: bool,
    }
    impl FixtureGateway {
        async fn send(&self) -> Result<u32, String> {
            self.posts.fetch_add(1, Ordering::SeqCst);
            if self.lose {
                Err("reply_lost".into())
            } else {
                Ok(1)
            }
        }
    }
    async fn reserved(
        label: &str,
        increase: bool,
        paused: bool,
        expired: bool,
    ) -> (AccountStoreHandle, StoredIntent, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "okx-native-orchestration-{label}-{}-{}",
            std::process::id(),
            now()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let account = format!(
            "okx:real:{}",
            if label == "two_b" { "b" } else { "a" }.repeat(64)
        );
        let t0 = now() - 1000;
        let store = AccountStoreHandle::initialize_flat(
            root.join("account.db"),
            okx_execution::AccountMeta {
                account_key: account.clone(),
                installation_id: "installation_test".into(),
                initialized_at: t0,
                opening_source: "opening_fixture".into(),
                initialize_operation_id: "initialize_fixture".into(),
                release_id: "a".repeat(40),
            },
            okx_execution::OpeningSnapshot {
                source_ref: "opening_fixture".into(),
                observed_at: t0,
                opening_equity_usd: Decimal::from(100),
                positions: 0,
                orders: 0,
                conditional_orders: 0,
                liabilities: Decimal::ZERO,
                balances: vec![("USD".into(), Decimal::from(100))],
            },
        )
        .await
        .unwrap();
        let at = now();
        let definition = store
            .call(deadline(), |s| s.ensure_manual_instance(now()))
            .await
            .unwrap();
        let plan = IntentPlan {
            strategy_definition_id: definition.definition_id,
            strategy_definition_version: definition.version,
            strategy_parameters_digest: definition.parameters_digest,
            operation: if increase { "open" } else { "close" }.into(),
            side: "buy".into(),
            position_side: "long".into(),
            quantity: Decimal::ONE,
            limit_price: Decimal::from(100),
            stop_price: Some(Decimal::from(99)),
            strategy_instance: "manual".into(),
            planned_loss_usd: Decimal::ONE,
            source_generation: 1,
            private_source_generation: 1,
            contract_value: Decimal::ONE,
            inverse: false,
            settlement_currency: "USDT".into(),
        };
        let decision = Decision {
            decision_id: "decision_fixture".into(),
            instance_id: None,
            decision_at: at - 100,
            input_digest: "b".repeat(64),
            policy_revision: 1,
            code_version: "fixture".into(),
        };
        let intent = NewIntent {
            plan,
            intent_id: "intent_fixture".into(),
            idempotency_key: "fixture_command_01".into(),
            request_hash: "b".repeat(64),
            client_order_id: "b".repeat(32),
            deadline: if expired { at - 1 } else { at + 30000 },
            plan_hash: "c".repeat(64),
            policy_revision: 1,
            risk_increase: increase,
            instrument_id: "BTC-USDT-SWAP".into(),
        };
        let saved = store
            .call(deadline(), move |s| {
                s.apply_authority_policy(1, paused, "campaign_fixture", Decimal::from(20))?;
                s.prepare(&decision, &intent)
            })
            .await
            .unwrap();
        let receipt = CentralReceipt {
            reservation_key: "fixture_reservation".into(),
            account_key: account,
            installation_id: "installation_test".into(),
            instance_id: None,
            intent_id: saved.intent_id.clone(),
            plan_hash: saved.plan_hash.clone(),
            policy_revision: 1,
            receipt_hash: "d".repeat(64),
            requested_loss_usd: Decimal::ONE,
            conversion_digest: "e".repeat(64),
        };
        store
            .call(deadline(), move |s| s.reserve(&receipt, now()))
            .await
            .unwrap();
        (store, saved, root)
    }
    #[tokio::test]
    async fn duplicate_execute_one_post() {
        let (store, intent, _root) = reserved("duplicate", true, false, false).await;
        let gateway = FixtureGateway {
            posts: AtomicUsize::new(0),
            lose: false,
        };
        assert_eq!(
            post_once(&store, &intent, deadline(), || gateway.send())
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            post_once(&store, &intent, deadline(), || gateway.send())
                .await
                .unwrap(),
            None
        );
        assert_eq!(gateway.posts.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn stop_after_reserve_before_post() {
        let (store, intent, _root) = reserved("stop", true, false, false).await;
        store
            .call(deadline(), |s| {
                s.apply_authority_policy(2, true, "campaign_fixture", Decimal::from(20))
            })
            .await
            .unwrap();
        let gateway = FixtureGateway {
            posts: AtomicUsize::new(0),
            lose: false,
        };
        assert!(
            post_once(&store, &intent, deadline(), || gateway.send())
                .await
                .is_err()
        );
        assert_eq!(gateway.posts.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn lost_response_after_post_returns_unknown() {
        let (store, intent, _root) = reserved("lost", true, false, false).await;
        let gateway = FixtureGateway {
            posts: AtomicUsize::new(0),
            lose: true,
        };
        assert_eq!(
            post_once(&store, &intent, deadline(), || gateway.send())
                .await
                .unwrap(),
            None
        );
        let saved = store
            .call(deadline(), |s| s.intent("intent_fixture"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.state, IntentState::Unknown);
        assert_eq!(
            post_once(&store, &intent, deadline(), || gateway.send())
                .await
                .unwrap(),
            None
        );
        assert_eq!(gateway.posts.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn restart_posting_reconciles_without_resubmit() {
        let (store, intent, _root) = reserved("restart", true, false, false).await;
        store
            .call(deadline(), |s| {
                assert!(s.begin_post("intent_fixture", 1, now())?);
                s.recover_posting(now())?;
                s.record_order_readback(
                    "intent_fixture",
                    "exchange_fixture",
                    IntentState::Filled,
                    Decimal::ONE,
                    now(),
                )
            })
            .await
            .unwrap();
        let gateway = FixtureGateway {
            posts: AtomicUsize::new(0),
            lose: false,
        };
        assert_eq!(
            post_once(&store, &intent, deadline(), || gateway.send())
                .await
                .unwrap(),
            None
        );
        assert_eq!(gateway.posts.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn close_under_stop() {
        let (store, intent, _root) = reserved("close", false, true, false).await;
        let gateway = FixtureGateway {
            posts: AtomicUsize::new(0),
            lose: false,
        };
        assert_eq!(
            post_once(&store, &intent, deadline(), || gateway.send())
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(gateway.posts.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn expired_command_never_executes_later() {
        let (store, intent, _root) = reserved("expired", true, false, true).await;
        let gateway = FixtureGateway {
            posts: AtomicUsize::new(0),
            lose: false,
        };
        for _ in 0..2 {
            assert!(
                post_once(&store, &intent, deadline(), || gateway.send())
                    .await
                    .is_err()
            );
        }
        assert_eq!(gateway.posts.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn wrong_uid_rejected_before_post() {
        let (store, _intent, _root) = reserved("wrong_uid", true, false, false).await;
        let account = format!("okx:real:{}", "a".repeat(64));
        let gateway = FixtureGateway {
            posts: AtomicUsize::new(0),
            lose: false,
        };
        let validated = prepare_on_owner(&store, deadline(), &account, &"b".repeat(64), |s| {
            s.intent("intent_fixture")
        })
        .await;
        assert!(validated.is_err());
        assert_eq!(gateway.posts.load(Ordering::SeqCst), 0);
        assert_eq!(
            store
                .call(deadline(), |s| s.intent("intent_fixture"))
                .await
                .unwrap()
                .unwrap()
                .state,
            IntentState::Reserved
        );
    }
    #[tokio::test]
    async fn stale_generation_response_ignored() {
        let (store, intent, _root) = reserved("generation", true, false, false).await;
        let gateway = FixtureGateway {
            posts: AtomicUsize::new(0),
            lose: false,
        };
        if validate_generation(2, 1).is_ok() {
            let _ = post_once(&store, &intent, deadline(), || gateway.send()).await;
        }
        assert_eq!(gateway.posts.load(Ordering::SeqCst), 0);
        assert!(validate_generation(2, 2).is_ok());
    }
    #[tokio::test]
    async fn view_selection_does_not_reroute_intent() {
        let (store, intent, _root) = reserved("view_route", true, false, false).await;
        let bound = format!("okx:real:{}", "a".repeat(64));
        let selected = format!("okx:real:{}", "b".repeat(64));
        assert_ne!(bound, selected);
        let route = prepare_on_owner(&store, deadline(), &bound, &"a".repeat(64), |s| {
            Ok(s.meta().account_key.clone())
        })
        .await
        .unwrap();
        let gateway = FixtureGateway {
            posts: AtomicUsize::new(0),
            lose: false,
        };
        post_once(&store, &intent, deadline(), || gateway.send())
            .await
            .unwrap();
        assert_eq!(route, bound);
        assert_eq!(gateway.posts.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn two_accounts_two_instances() {
        let (a, ia, _ra) = reserved("two_a", true, false, false).await;
        let (b, ib, _rb) = reserved("two_b", true, false, false).await;
        for store in [&a, &b] {
            store
                .call(deadline(), |s| {
                    for id in ["alpha", "beta"] {
                        let instance = StrategyInstance {
                            instance_id: id.into(),
                            definition_id: "ema_cross_v1".into(),
                            definition_version: "1".into(),
                            parameters: "{\"fast\":2,\"slow\":3}".into(),
                            enabled: false,
                            policy_revision: 1,
                            budget: Decimal::from(5),
                            last_decision_key: None,
                        };
                        s.mutate_instance(
                            &format!("create_{id}"),
                            &format!("hash_{id}"),
                            "create",
                            &instance,
                            None,
                            now(),
                        )?;
                    }
                    Ok(())
                })
                .await
                .unwrap();
        }
        let ga = FixtureGateway {
            posts: AtomicUsize::new(0),
            lose: false,
        };
        let gb = FixtureGateway {
            posts: AtomicUsize::new(0),
            lose: false,
        };
        let (ra, rb) = tokio::join!(
            post_once(&a, &ia, deadline(), || ga.send()),
            post_once(&b, &ib, deadline(), || gb.send())
        );
        assert!(ra.unwrap().is_some() && rb.unwrap().is_some());
        assert_eq!(ga.posts.load(Ordering::SeqCst), 1);
        assert_eq!(gb.posts.load(Ordering::SeqCst), 1);
        for store in [&a, &b] {
            assert_eq!(
                store
                    .call(deadline(), |s| s.instances())
                    .await
                    .unwrap()
                    .into_iter()
                    .filter(|instance| instance.instance_id != "manual")
                    .count(),
                2
            );
        }
    }
    #[tokio::test]
    async fn deadline_expires_after_commit_before_gateway_send() {
        let (store, intent, _root) = reserved("commit_deadline", true, false, false).await;
        let gateway = FixtureGateway {
            posts: AtomicUsize::new(0),
            lose: false,
        };
        let until = Instant::now() + Duration::from_millis(100);
        let result = post_once(&store, &intent, until, || async {
            tokio::time::sleep(Duration::from_millis(150)).await;
            if Instant::now() >= until {
                return Err("COMMAND_EXPIRED_BEFORE_SEND".into());
            }
            gateway.send().await
        })
        .await;
        assert!(result.is_err());
        assert_eq!(gateway.posts.load(Ordering::SeqCst), 0);
        assert_eq!(
            store
                .call(deadline(), |s| s.intent("intent_fixture"))
                .await
                .unwrap()
                .unwrap()
                .state,
            IntentState::Rejected
        );
    }
}

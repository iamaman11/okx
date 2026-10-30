use super::*;
use okx_api::{
    ApiOrderSide, ApiOrderType, ApiPositionSide, ApiTradeMode, ApiTriggerPriceType,
    AttachedAlgoOrderRequest, PlaceOrderRequest,
};
/// Immutable provenance for checking the fresh post-HELD FX rate against the
/// exact native account that owns the central reservation.
struct PostFxFence<'a> {
    owner: &'a AccountResources,
    conversion: &'a UsdConversion,
}
impl TradeService {
    /// Native, completely classified USD-denominated spot opening spend.
    /// A buy consumes gross USD notional; any execution fee consumes
    /// additional USD, including on a sell. Rebates never replenish budget.
    /// No other instrument, conversion, bill type or currency is guessed.
    /// Completeness and fill↔order↔bill identity must already be established
    /// by AccountHistoryApi's authenticated campaign witness.
    fn historical_usd_spot_entry(
        history: &okx_api::CompleteCampaignAccountHistory,
    ) -> Result<Decimal, String> {
        use rust_decimal::RoundingStrategy;
        let orders = history
            .orders.iter()
            .map(|o| (o.order_id.as_str(), o))
            .collect::<std::collections::BTreeMap<_, _>>();
        let bills = history
            .bills.iter()
            .map(|b| (b.bill_id.as_str(), b))
            .collect::<std::collections::BTreeMap<_, _>>();
        if orders.len() != history.orders.len() || bills.len() != history.bills.len() {
            return Err("ENTRY_OPENING_DUPLICATE_VENUE_ID".into());
        }
        let mut observed_bills = std::collections::BTreeSet::new();
        let mut total = Decimal::ZERO;
        for fill in &history.fills {
            let order = orders.get(fill.order_id.as_str())
                .ok_or("ENTRY_OPENING_ORDER_UNPROVEN")?;
            let bill = bills.get(fill.bill_id.as_str())
                .ok_or("ENTRY_OPENING_BILL_UNPROVEN")?;
            if fill.instrument_type != "SPOT"
                || !fill.instrument_id.ends_with("-USD")
                || fill.fee_currency != "USD"
                || bill.ccy != "USD"
                || bill.bill_type != "2"
                || bill.instrument_id != fill.instrument_id
                || bill.order_id != fill.order_id
                || bill.trade_id != fill.trade_id
                || order.instrument_type != "SPOT"
                || order.instrument_id != fill.instrument_id
                || !matches!(fill.side.as_str(), "buy" | "sell")
                || order.side != fill.side
                || bill.fee != fill.fee
                || !observed_bills.insert(fill.bill_id.as_str())
            {
                return Err("ENTRY_OPENING_UNSUPPORTED_OR_UNCLASSIFIED_MONEY".into());
            }
            let qty = dec(&fill.fill_size)?;
            let price = dec(&fill.fill_price)?;
            let fee = dec(&fill.fee)?;
            if qty <= Decimal::ZERO || price <= Decimal::ZERO {
                return Err("ENTRY_OPENING_INVALID_FILL".into());
            }
            // Negative venue fee = debit; positive rebate is never a credit
            // to the spent entry budget. Sell proceeds never refund entry.
            let debit = if fill.side == "buy" {
                qty.checked_mul(price).ok_or("MONEY_OVERFLOW")?
            } else {
                Decimal::ZERO
            };
            let debit = debit.checked_add(fee.abs()).ok_or("MONEY_OVERFLOW")?;
            total = total.checked_add(debit).ok_or("MONEY_OVERFLOW")?;
        }
        if observed_bills.len() != history.bills.len() {
            return Err("ENTRY_OPENING_STANDALONE_BILL_UNCLASSIFIED".into());
        }
        Ok(total.round_dp_with_strategy(9, RoundingStrategy::ToPositiveInfinity))
    }

    /// Opening may be installed once for the entire authenticated cohort.
    /// Unknown venue events never imply zero. Non-USD historical instruments
    /// remain unsupported until a historical contract+FX source exists.
    async fn ensure_campaign_opening(
        &self,
        state: &CentralGovernanceSnapshot,
        until: Instant,
    ) -> Result<(), String> {
        let summary = self
            .risk
            .request(self.risk_command(
                state,
                format!("opening_summary_{}", now()),
                RiskCommandAction::Summary {},
            ))
            .await
            .map_err(str::to_owned)?;
        if summary.status != RiskDecisionStatus::Observed {
            return Err("ENTRY_OPENING_CENTRAL_SUMMARY_UNAVAILABLE".into());
        }
        let budget = summary
            .budget
            .ok_or("ENTRY_OPENING_CENTRAL_BUDGET_MISSING")?;
        if let Some(entry) = budget.entry_budget {
            // An imported opening is a historical snapshot, NOT perpetual
            // proof that nobody subsequently traded outside RiskBook. Until
            // native post-opening fill/bill settlement is independently
            // reconciled, any money event after that cut-off blocks new risk.
            // This deliberately rejects even own fills rather than silently
            // letting a second trade spend untracked campaign capacity.
            let from = i64::try_from(entry.opening_as_of_ms)
                .map_err(|_| "ENTRY_OPENING_CUTOFF_INVALID")?;
            let through = now();
            if through <= from {
                return Err("ENTRY_OPENING_CUTOFF_NOT_FRESH".into());
            }
            for account in state.state.accounts.iter().filter(|a| {
                a.environment == okx_protocol::account_governance::AccountEnvironment::Real
            }) {
                if Instant::now() >= until {
                    return Err("COMMAND_EXPIRED".into());
                }
                let resource = self.resources(&account.account_key).await?;
                if !resource.source_identity_ready().await {
                    return Err("ENTRY_OPENING_IDENTITY_UNVERIFIED".into());
                }
                let money = tokio::time::timeout(
                    until.saturating_duration_since(Instant::now()),
                    resource.execution.history.complete_campaign_money_history(
                        from, through, now(),
                    ),
                ).await.map_err(|_| "ENTRY_OPENING_POST_CUTOFF_HISTORY_DEADLINE")?
                    .map_err(|_| "ENTRY_OPENING_POST_CUTOFF_HISTORY_UNAVAILABLE")?;
                if money.from_ms != from || money.through_ms != through
                    || !money.fills.is_empty() || !money.bills.is_empty()
                {
                    return Err("ENTRY_OPENING_POST_CUTOFF_MONEY_UNRECONCILED".into());
                }
            }
            return Ok(());
        }
        let origin = budget
            .campaign_origin
            .ok_or("ENTRY_CAMPAIGN_ORIGIN_UNPROVEN")?;
        if origin.installation_id != self.accounts.installation_id()
            || origin.campaign_id != state.state.campaign_id
        {
            return Err("ENTRY_CAMPAIGN_ORIGIN_MISMATCH".into());
        }
        let from =
            i64::try_from(origin.started_at_ms).map_err(|_| "ENTRY_CAMPAIGN_ORIGIN_INVALID")?;
        let mut rows = Vec::new();
        let cohort = state
            .state
            .accounts
            .iter()
            .filter(|account| {
                account.environment == okx_protocol::account_governance::AccountEnvironment::Real
            })
            .collect::<Vec<_>>();
        if cohort.is_empty() || cohort.len() > 32 {
            return Err("ENTRY_OPENING_COHORT_INVALID".into());
        }
        if !self.accounts.failures().await.is_empty() {
            return Err("ENTRY_OPENING_COHORT_UNAVAILABLE".into());
        }
        let through = now();
        for account in cohort {
            if Instant::now() >= until {
                return Err("COMMAND_EXPIRED".into());
            }
            let resource = self.resources(&account.account_key).await?;
            if !resource.source_identity_ready().await {
                return Err("ENTRY_OPENING_IDENTITY_UNVERIFIED".into());
            }
            let history = tokio::time::timeout(
                until.saturating_duration_since(Instant::now()),
                resource
                    .execution
                    .history
                    .complete_campaign_account_history(from, through, now()),
            )
            .await
            .map_err(|_| "ENTRY_OPENING_HISTORY_DEADLINE")?
            .map_err(|_| "ENTRY_OPENING_HISTORY_UNAVAILABLE")?;
            if history.from_ms != from || history.through_ms != through {
                return Err("ENTRY_OPENING_HISTORY_SCOPE_MISMATCH".into());
            }
            let spent = Self::historical_usd_spot_entry(&history)?;
            let venue_fill_count = history.fills.len();
            let venue_bill_count = history.bills.len();
            let campaign = origin.campaign_id.clone();
            let source = history.source_digest;
            let source_digest = resource
                .execution
                .store
                .call(until, move |store| {
                    store.seal_campaign_entry_opening(
                        &campaign, from, through, &source,
                        venue_fill_count, venue_bill_count, spent,
                    )
                })
                .await
                .map_err(err)?;
            rows.push(CampaignOpeningAccount {
                account_key: account.account_key.clone(),
                history_from_ms: origin.started_at_ms,
                history_through_ms: u64::try_from(through)
                    .map_err(|_| "ENTRY_OPENING_TIME_INVALID")?,
                consumed_entry_usd: exact(spent),
                order_history_complete: true,
                fill_history_complete: true,
                bill_history_complete: true,
                finance_provenance_sealed: true,
                source_digest,
            });
        }
        rows.sort_by(|a, b| a.account_key.cmp(&b.account_key));
        let consumed = rows.iter().try_fold(Decimal::ZERO, |sum, account| {
            sum.checked_add(dec(&account.consumed_entry_usd)?)
                .ok_or_else(|| "MONEY_OVERFLOW".to_owned())
        })?;
        let limit_usd = state
            .state
            .campaign_entry_limit_usd
            .clone()
            .ok_or("ENTRY_CAMPAIGN_LIMIT_MISSING")?;
        let mut source = CampaignOpeningSource {
            campaign_id: origin.campaign_id,
            limit_usd,
            consumed_usd: exact(consumed),
            source_digest: String::new(),
            as_of_ms: u64::try_from(through).map_err(|_| "ENTRY_OPENING_TIME_INVALID")?,
            campaign_started_at_ms: origin.started_at_ms,
            accounts: rows,
        };
        source.source_digest = digest(&(
            "okx.campaign-entry-opening/v1",
            &source.campaign_id,
            &source.limit_usd,
            &source.consumed_usd,
            source.campaign_started_at_ms,
            source.as_of_ms,
            &source.accounts,
        ))?;
        if !source.valid() || Instant::now() >= until {
            return Err("ENTRY_OPENING_SOURCE_INVALID".into());
        }
        let result = self
            .risk
            .request(self.risk_command(
                state,
                format!("install_opening_{}", now()),
                RiskCommandAction::InstallEntryOpening {
                    source: source.clone(),
                },
            ))
            .await
            .map_err(str::to_owned)?;
        if result.status != RiskDecisionStatus::Observed
            || result
                .budget
                .as_ref()
                .and_then(|budget| budget.entry_budget.as_ref())
                .is_none_or(|opened| opened.opening_source_digest != source.source_digest)
        {
            return Err(result
                .reason
                .unwrap_or("ENTRY_OPENING_INSTALL_UNPROVEN".into()));
        }
        Ok(())
    }

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
        claimed_decision_id: String,
    ) -> Result<Value, String> {
        self.execute_inner(
            r,
            request,
            supplied,
            None,
            Some(version),
            Some((decision_digest, claimed_decision_id)),
        )
        .await
    }
    async fn execute_inner(
        &self,
        r: &Arc<AccountResources>,
        request: &TradeRequest,
        supplied: Option<&CentralGovernanceSnapshot>,
        bound_hash: Option<String>,
        expected_definition: Option<String>,
        claimed_decision: Option<(String, String)>,
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
        if increase
            && r.configuration.account_key.environment == okx_protocol::accounts::Environment::Real
        {
            self.ensure_campaign_opening(&state, until).await?;
        }
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
                    true,
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
            decision_id: claimed_decision
                .as_ref()
                .map(|(_, id)| id.clone())
                .unwrap_or_else(|| format!("decision_{}", &body_hash[..32])),
            instance_id: Some(strategy_instance.clone()),
            decision_at: now(),
            input_digest: match claimed_decision {
                Some((digest, _)) => digest,
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
            // E1 staging: persist exactly what the live venue price, fee and
            // conversion sources can prove before requesting central risk.
            // The receipt says UNPROVEN; it cannot authorize the exchange.
            let rules = self
                .public_ws
                .instrument_rules(instrument)
                .await
                .ok_or("ENTRY_INSTRUMENT_RULES_UNAVAILABLE")?;
            let fees = r.observer.fee_schedule(&rules).await.map_err(err)?;
            let quote_snapshot = r.observer.snapshot().await.map_err(err)?;
            let fx = Self::usd_conversion(&quote_snapshot)?;
            let (quote, rate) = entry_budget::verified_known_entry_quote(
                &plan,
                instrument,
                &rules,
                &fees,
                &fx,
                &quote_snapshot.account_uid_fingerprint,
                &r.configuration.account_key.verified_uid,
                now(),
            )?;
            let ceiling = entry_budget::enforced_entry_ceiling(
                true,
                true,
                control
                    .limits
                    .max_entry_usd
                    .as_deref()
                    .map(dec)
                    .transpose()?,
                order.max_entry_usd.as_deref().map(dec).transpose()?,
            )?
            .ok_or("ENTRY_ACCOUNT_CAP_MISSING")?;
            if quote.total_usd > ceiling {
                return Err("ENTRY_QUOTE_EXCEEDS_ACCOUNT_CAP".into());
            }
            if Instant::now() >= until {
                return Err("COMMAND_EXPIRED".into());
            }
            let fee_received_at = chrono::DateTime::parse_from_rfc3339(&fees.source_received_at)
                .map_err(err)?
                .timestamp_millis();
            let entry_receipt = okx_execution::EntryQuoteEvidence {
                schema: "okx.entry-known-quote/v1".into(),
                account_key: r.configuration.account_key.wire(),
                installation_id: self.accounts.installation_id().into(),
                intent_id: prepared.intent_id.clone(),
                plan_hash: prepared.plan_hash.clone(),
                policy_revision: prepared.policy_revision,
                instrument_id: instrument.clone(),
                quantity: plan.quantity,
                limit_price: plan.limit_price,
                contract_value: plan.contract_value,
                fee_rate: rate,
                maker_rate: fees.maker_rate.clone(),
                taker_rate: fees.taker_rate.clone(),
                fee_level: fees.level.clone(),
                fee_exchange_at: fees
                    .exchange_timestamp_ms
                    .parse::<i64>()
                    .map_err(|_| "ENTRY_FEE_EXCHANGE_TIME_INVALID")?,
                usd_per_usdt: dec(&fx.usd_per_usdt)?,
                notional_usdt: quote.notional_usdt,
                fee_usdt: quote.fee_usdt,
                total_usdt: quote.total_usdt,
                total_usd: quote.total_usd,
                fee_generation: fees.fee_generation.clone(),
                reference_generation: rules.reference_generation.clone(),
                fee_received_at,
                conversion_digest: fx.source_digest,
                conversion_received_at: fx.observed_at_ms as i64,
                observed_at: now(),
            };
            // This is the only bridge from immutable SQL pricing to
            // central entry-spend authority. Until authenticated venue
            // applicability and an independently bounded FX source exist,
            // unproven_linear_quote produces no upper and no proof digest.
            let entry_cost_proof =
                entry_budget::EntryCostProof::unproven_linear_quote(quote.clone());
            let verified_entry =
                entry_budget::reserve_entry_evidence(&entry_cost_proof, &entry_receipt, ceiling)?;
            let cost_components = entry_cost_proof.verified_fx_and_fixed_costs()?;
            if verified_entry.is_some() != cost_components.is_some() {
                return Err("ENTRY_PROOF_COMPONENTS_UNVERIFIED".into());
            }
            // The native source attestation and its Worker RESERVE upper
            // must survive a crash together with the same immutable quote.
            // No admission operation receipt exists for an Unknown cost.
            let entry_admission = verified_entry
                .as_ref()
                .zip(cost_components)
                .map(|((upper, source_digest), (fx_upper, fixed_upper))| {
                    let serialized = serde_json::to_vec(&entry_receipt)
                        .map_err(|_| "ENTRY_ADMISSION_QUOTE_SERIALIZATION_FAILED")?;
                    Ok::<okx_execution::EntryAdmissionEvidence, String>(
                        okx_execution::EntryAdmissionEvidence {
                            schema: "okx.entry-admission/v1".into(),
                            account_key: entry_receipt.account_key.clone(),
                            installation_id: entry_receipt.installation_id.clone(),
                            intent_id: entry_receipt.intent_id.clone(),
                            plan_hash: entry_receipt.plan_hash.clone(),
                            quote_digest: format!("{:x}", Sha256::digest(serialized)),
                            fx_upper_usd_per_usdt: fx_upper,
                            other_entry_costs_upper_usd: fixed_upper,
                            entry_upper_usd: *upper,
                            entry_proof_digest: source_digest.clone(),
                            observed_at: entry_receipt.observed_at,
                        },
                    )
                })
                .transpose()?;
            r.execution
                .store
                .call(until, move |s| {
                    s.bind_entry_quote(&entry_receipt)?;
                    if let Some(proof) = entry_admission {
                        s.bind_entry_admission(&proof)?;
                    }
                    Ok(())
                })
                .await
                .map_err(err)?;
            // An absent all-in proof is a native decision, not a reason to
            // create a fictitious central RESERVE attempt and send a WSS
            // request with null money. Finish the SQL intent as unsent,
            // preserving the immutable known quote for trader inspection.
            if verified_entry.is_none() {
                let rejected = prepared.intent_id.clone();
                r.execution
                    .store
                    .call(until, move |s| {
                        s.reject_unsent(&rejected, "ENTRY_ALL_IN_COST_UNPROVEN", now())?;
                        Ok(())
                    })
                    .await
                    .map_err(err)?;
                return Err("ENTRY_ALL_IN_COST_UNPROVEN".into());
            }
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
                    // Only the exact SQL-quote-bound ALL-IN proof may
                    // authorize the independent entry-spend budget. An
                    // Unknown dimension never substitutes zero or a quote.
                    entry_upper_usd: verified_entry.as_ref().map(|(upper, _)| exact(*upper)),
                    entry_proof_digest: verified_entry
                        .as_ref()
                        .map(|(_, proof_digest)| proof_digest.clone()),
                    usd_conversion: conversion.clone(),
                    policy_revision: state.state.revision,
                    census,
                },
            );
            let key = self.risk_reservation_key(r, &prepared);
            let intent_id = prepared.intent_id.clone();
            let reserve_key = key.clone();
            r.execution
                .store
                .call(until, move |s| {
                    s.mark_reserve_attempt(&intent_id, &reserve_key, now())
                })
                .await
                .map_err(err)?;
            let held = match self
                .reserve_inspect_retry(&state, reserve, &prepared, r)
                .await
            {
                Ok(held) => held,
                Err((reason, authoritative_denial)) => {
                    let id = prepared.intent_id.clone();
                    if authoritative_denial {
                        let owner_reason = reason.clone();
                        r.execution
                            .store
                            .call(deadline(), move |s| {
                                s.reject_denied_reserve(&id, &owner_reason, now())
                            })
                            .await
                            .map_err(err)?;
                        return Ok(completed(json!({
                            "intent_id":prepared.intent_id,
                            "state":"REJECTED","rejection_origin":"CENTRAL",
                            "send_state":"NOT_STARTED","primary_reason":reason,
                            "risk_release_state":"NOT_RESERVED"
                        })));
                    }
                    return match reject_prepost_denial(&r.execution.store, &prepared, &reason).await
                    {
                        Ok(mut result) => {
                            match self.release_unsent(r, &prepared).await {
                                Ok(()) => result["data"]["risk_release_state"] = json!("RELEASED"),
                                Err(release) => {
                                    result["data"]["risk_release_error"] = json!(release);
                                }
                            }
                            Ok(result)
                        }
                        Err(_) => Ok(uncertain(
                            &prepared.intent_id,
                            "RESERVE_LOCAL_COMMIT_UNCONFIRMED_NO_RESUBMIT",
                        )),
                    };
                }
            };
            let held_received = Instant::now();
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
                .fence_inspect_retry(
                    &state,
                    &held,
                    &prepared,
                    until,
                    held_received,
                    PostFxFence {
                        owner: r,
                        conversion: &conversion,
                    },
                )
                .await
            {
                Ok(v) => central = Some(v),
                Err(reason) => {
                    // BEGIN_POST failed before any native post claim or venue call.
                    // Preserve the central first cause in SQL, not a fabricated
                    // MAY_HAVE_SENT outcome. A failed local commit stays uncertain.
                    return match reject_prepost_denial(&r.execution.store, &prepared, &reason).await
                    {
                        Ok(mut result) => {
                            match self.release_unsent(r, &prepared).await {
                                Ok(()) => result["data"]["risk_release_state"] = json!("RELEASED"),
                                Err(release) => {
                                    result["data"]["risk_release_error"] = json!(release);
                                }
                            }
                            Ok(result)
                        }
                        Err(_) => Ok(uncertain(
                            &id,
                            "PREPOST_LOCAL_COMMIT_UNCONFIRMED_NO_RESUBMIT",
                        )),
                    };
                }
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
    fn risk_reservation_key(&self, r: &AccountResources, intent: &StoredIntent) -> String {
        [
            "okx",
            "real",
            r.configuration.account_key.verified_uid.as_str(),
            "continuous_engine",
            intent.intent_id.as_str(),
        ]
        .iter()
        .map(|x| format!("{}:{}", x.len(), x))
        .collect::<String>()
    }

    async fn reserve_inspect_retry(
        &self,
        state: &CentralGovernanceSnapshot,
        command: RiskCommand,
        intent: &StoredIntent,
        r: &AccountResources,
    ) -> Result<RiskResult, (String, bool)> {
        let first = self.risk.request(command).await;
        if let Ok(held) = &first
            && held.status == RiskDecisionStatus::Held
        {
            return Ok(held.clone());
        }
        // DENIED is not itself proof of no existing reservation: a replayed
        // identity may have HELD capacity in Worker already. INSPECT the exact
        // address before settling the SQL no-reservation marker.
        let inspect = self.risk_command(
            state,
            format!("inspect_{}", &intent.plan_hash[..32]),
            RiskCommandAction::Inspect {
                reservation_key: self.risk_reservation_key(r, intent),
                plan_sha256: intent.plan_hash.clone(),
            },
        );
        let observed = self.risk.request(inspect).await;
        let central_denied = matches!(&first,
            Ok(v) if v.status == RiskDecisionStatus::Denied);
        let reason = match &first {
            Ok(v) => v.reason.clone().unwrap_or("RESERVE_DENIED".into()),
            Err(_) => "RESERVE_OUTCOME_UNKNOWN_NO_POST".into(),
        };
        match observed {
            Ok(reply)
                if reply.status == RiskDecisionStatus::Observed
                    && reply.reservation_state == Some(RiskReservationState::Held)
                    && !central_denied =>
            {
                // One original reservation is known; never replay stale
                // reserve-time source as a second grant.
                Ok(reply)
            }
            Ok(reply)
                if central_denied
                    && reply.status == RiskDecisionStatus::Denied
                    && reply.reason.as_deref() == Some("RISK_RESERVATION_NOT_FOUND") =>
            {
                Err((reason, true))
            }
            _ => Err((reason, false)),
        }
    }
    async fn revalidate_held_entry_quote(
        &self,
        r: &AccountResources,
        intent: &StoredIntent,
        until: Instant,
    ) -> Result<(), String> {
        let id = intent.intent_id.clone();
        let quote = r
            .execution
            .store
            .call(until, move |s| s.entry_quote(&id))
            .await
            .map_err(err)?
            .ok_or("POST_ENTRY_QUOTE_MISSING")?;
        let rules = self
            .public_ws
            .instrument_rules(&intent.instrument_id)
            .await
            .ok_or("POST_ENTRY_RULES_UNAVAILABLE")?;
        plan::verify_entry_rules_match(&rules, &intent.instrument_id, &intent.plan)?;
        let ct = dec(rules
            .instrument
            .contract_value
            .as_deref()
            .ok_or("POST_ENTRY_CONTRACT_VALUE_MISSING")?)?;
        let fee = r.observer.fee_schedule(&rules).await.map_err(err)?;
        let snap = r.observer.snapshot_after_held().await.map_err(err)?;
        if snap.account_uid_fingerprint != r.configuration.account_key.verified_uid {
            return Err("POST_ENTRY_ACCOUNT_CHANGED".into());
        }
        let fx = Self::usd_conversion(&snap)?;
        verify_post_held_entry_sources(&quote, &fee, &rules.reference_generation, ct, &fx, now())?;
        if Instant::now() >= until {
            return Err("COMMAND_EXPIRED".into());
        }
        Ok(())
    }

    async fn fence_inspect_retry(
        &self,
        state: &CentralGovernanceSnapshot,
        held: &RiskResult,
        intent: &StoredIntent,
        until: Instant,
        held_received: Instant,
        fx: PostFxFence<'_>,
    ) -> Result<RiskResult, String> {
        let key = held
            .reservation_key
            .clone()
            .ok_or("RESERVATION_KEY_MISSING")?;
        let mut sequence = held
            .risk_book_sequence
            .ok_or("RESERVATION_SEQUENCE_MISSING")?;
        // One original attempt plus at most two bounded CAS retries.
        // Every attempt shares the original after-HELD five-second window.
        for _ in 0..3 {
            if Instant::now() >= until {
                return Err("COMMAND_EXPIRED".into());
            }
            // A fresh census and a valid FX fence do not revalidate tariff or
            // contract mechanics. Bind the same immutable SQL quote against
            // actual post-HELD source data on EACH CAS attempt before POST.
            self.revalidate_held_entry_quote(fx.owner, intent, until)
                .await?;
            let (census, causal_proof) = self
                .census_after_held(
                    state,
                    held_received,
                    &key,
                    &fx.owner.configuration.account_key.wire(),
                    &fx.conversion.usd_per_usdt,
                )
                .await?;
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
                    causal_proof,
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
        self.census_with_reads(state, None, None).await.map(|v| v.0)
    }

    async fn census_after_held(
        &self,
        state: &CentralGovernanceSnapshot,
        held_received: Instant,
        reservation: &str,
        selected_account: &str,
        reserved_usd_per_usdt: &str,
    ) -> Result<(NativeRiskCensus, CausalPostProof), String> {
        let (census, account_reads) = self
            .census_with_reads(
                state,
                Some(held_received),
                Some((selected_account, reserved_usd_per_usdt)),
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
                let started = held_received.map(|tick| tick.elapsed().as_millis() as u64);
                let r = self.resources(&c.account_key).await?;
                if !r.source_identity_ready().await {
                    return Err("COHORT_SOURCE_GENERATION_UNVERIFIED".into());
                }
                let generation = r.private_ws.status().await.connection_generation;
                let snapshot = if held_received.is_some() {
                    r.observer.snapshot_after_held().await.map_err(err)?
                } else {
                    r.observer.snapshot().await.map_err(err)?
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
                // This is the earliest REST response across every pending
                // derivative-algo scope. Complete coverage is insufficient
                // if the underlying source timestamp is absent or stale.
                let algo_source_floor = verified_algo_source_time(algos.received_at_ms, now())?;
                let mut managed_algos = std::collections::BTreeSet::new();
                for p in &snapshot.positions {
                    let Some((position_direction, actual_qty)) =
                        super::reconcile::verified_venue_position_direction(
                            &p.position_side,
                            &p.position,
                        )?
                    else {
                        continue;
                    };
                    let instrument = p.instrument_id.clone();
                    let lots = r
                        .execution
                        .store
                        .call(deadline(), move |s| s.active_lots(&instrument))
                        .await
                        .map_err(err)?;
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
                        return Err("UNATTRIBUTED_EXCHANGE_POSITION".to_owned());
                    }
                    for lot in lots.iter().filter(|l| l.side == position_direction) {
                        let id = lot.intent_id.clone().ok_or("UNATTRIBUTED_POSITION_LOT")?;
                        let owner_id = id.clone();
                        let (protection, receipt) = r
                            .execution
                            .store
                            .call(deadline(), move |s| {
                                Ok((s.protection(&id)?, s.active_central_receipt(&id)?))
                            })
                            .await
                            .map_err(err)?;
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
                    let known = known.ok_or("UNATTRIBUTED_PENDING_ORDER")?;
                    verify_pending_order_binding(order, &known)?;
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
                // Active SQL intents are obligations too. A fresh venue
                // snapshot can omit a still-UNKNOWN POST, and a pending order
                // cannot be considered managed without its central receipt.
                // The 100th row is a positive truncation signal: never
                // misreport a bounded local scan as complete.
                let local_intents = r
                    .execution
                    .store
                    .call(deadline(), |s| {
                        let (intents, pending) = s.risk_census_intents(100)?;
                        let covered = intents
                            .into_iter()
                            .map(|intent| {
                                let central = s.active_central_receipt(&intent.intent_id)?;
                                Ok((intent, central))
                            })
                            .collect::<okx_execution::StoreResult<Vec<_>>>()?;
                        Ok((covered, pending))
                    })
                    .await
                    .map_err(err)?;
                let (local_intents, pending_central) = local_intents;
                let local_intent_digest =
                    verify_local_intent_coverage(&local_intents, &pending_central, &c.account_key)?;
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
                let financial = r
                    .execution
                    .store
                    .call(deadline(), |s| {
                        Ok((s.financial_risk(now(), None)?, s.policy()?.4))
                    })
                    .await
                    .map_err(err)?;
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
                )?
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
                // No accepted monetary projection for independently occupied
                // exposure exists yet. The central DO accounts for its own
                // HELD grants separately, but a local open venue obligation
                // must never be silently invented as "zero external risk".
                // The first bounded acceptance supports a provably flat
                // account; managed open exposure remains NO-GO until native
                // position/order/stop monetary occupancy is source-proven.
                let flat_occupied = certified_flat_external_exposure(
                    snapshot.positions.iter().map(|p| p.position.as_str()),
                    snapshot.pending_orders.len(),
                    algos.samples.len(),
                )?;
                let occupancy = RiskAccountOccupancy {
                    account_key: c.account_key.clone(),
                    occupied_usdt: flat_occupied.into(),
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
fn verified_algo_source_time(
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

/// E2 strict first-test boundary: a zero external occupancy witness can
/// only be produced from a genuinely flat venue inventory. Matching SQL
/// intent/central receipt IDs alone do not prove the monetary risk of an
/// already-open position, active order or algo. Those require the full priced
/// occupancy projection before allowing concurrent increases.
fn certified_flat_external_exposure<'a>(
    positions: impl IntoIterator<Item = &'a str>,
    open_orders: usize,
    conditional_orders: usize,
) -> Result<&'static str, String> {
    if open_orders != 0 || conditional_orders != 0 {
        return Err("COHORT_NONFLAT_EXPOSURE_UNPRICED".into());
    }
    for amount in positions {
        if dec(amount)? != Decimal::ZERO {
            return Err("COHORT_NONFLAT_EXPOSURE_UNPRICED".into());
        }
    }
    Ok("0")
}

/// The reserved known quote is source-bound but NOT an all-in debit proof.
/// If any source changes after HELD, do not reuse the prior risk grant. Fail
/// rather than silently refresh the amount/cost or issue a second venue POST.
fn verify_post_held_entry_sources(
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

/// The SQL owner is the authority for the local send state. A missing venue
/// row does not prove no-send after POSTING/UNKNOWN; preserve those obligations
/// and deny further account risk until a terminal readback is reconciled.
fn verify_local_intent_coverage(
    active: &[(StoredIntent, Option<CentralReceipt>)],
    unresolved: &[String],
    account_key: &str,
) -> Result<String, String> {
    const PAGE_LIMIT: usize = 100;
    if active.len() >= PAGE_LIMIT {
        return Err("COHORT_LOCAL_INTENTS_TRUNCATED".into());
    }
    let mut ids = std::collections::BTreeSet::new();
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
            return Err("COHORT_TERMINAL_RISK_RELEASE_PENDING".into());
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
    if unresolved.iter().any(|id| !ids.contains(id.as_str())) {
        return Err("COHORT_UNRESOLVED_INTENT_SCOPE_INVALID".into());
    }
    digest(&(active, unresolved))
}

/// Concurrent account reads must produce one deterministic, complete cohort
/// digest regardless of completion order. Duplicated/missing keys fail closed.
fn canonical_census_content(
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
fn verify_pending_order_binding(
    order: &okx_observation::PendingOrderState,
    intent: &StoredIntent,
) -> Result<(), String> {
    if order.client_order_id.as_deref() != Some(intent.client_order_id.as_str())
        || order.order_id.is_empty()
        || intent.exchange_order_id.as_deref() != Some(order.order_id.as_str())
        || order.instrument_id != intent.instrument_id
        || order.side != intent.plan.side
        || order.position_side.as_deref() != Some(intent.plan.position_side.as_str())
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
fn census_source_floor(
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
fn verify_post_held_fx(reserved: &str, observed: &str) -> Result<(), String> {
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

fn native_process_incarnation() -> Result<&'static str, String> {
    static INSTANCE: std::sync::OnceLock<Result<String, String>> = std::sync::OnceLock::new();
    INSTANCE
        .get_or_init(|| {
            let mut bytes = [0u8; 16];
            getrandom::fill(&mut bytes).map_err(err)?;
            Ok(bytes.iter().map(|b| format!("{b:02x}")).collect::<String>())
        })
        .as_ref()
        .map(String::as_str)
        .map_err(Clone::clone)
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
/// A central fence denial before the SQL POST claim proves the venue was
/// never called. Keep the first reason and the reservation pending for
/// central release; do not confuse this with a lost exchange POST response.
async fn reject_prepost_denial(
    store: &AccountStoreHandle,
    intent: &StoredIntent,
    reason: &str,
) -> Result<Value, String> {
    let id = intent.intent_id.clone();
    let original = reason.to_owned();
    store
        .call(deadline(), move |s| s.reject_unsent(&id, &original, now()))
        .await
        .map_err(err)?;
    let id = intent.intent_id.clone();
    let saved = store
        .call(deadline(), move |s| s.intent(&id))
        .await
        .map_err(err)?
        .ok_or("PREPOST_INTENT_READBACK_MISSING")?;
    if saved.state != IntentState::Rejected {
        return Err("PREPOST_INTENT_REJECTION_UNCONFIRMED".into());
    }
    Ok(completed(json!({
        "intent_id":saved.intent_id,
        "state":"REJECTED",
        "rejection_origin":"LOCAL",
        "send_state":"NOT_STARTED",
        "primary_reason":saved.incident.as_deref().unwrap_or(reason),
        "risk_release_state":"PENDING",
        "next_action":"trade.status"
    })))
}
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

    #[test]
    fn e1_positive_historical_usd_spot_is_classified_without_usdt_fx_guess() {
        let order: okx_api::HistoricalOrder = serde_json::from_value(serde_json::json!({
            "instType":"SPOT","instId":"BTC-USD","ordId":"123","side":"buy"
        })).unwrap();
        let fill: okx_api::FillHistory = serde_json::from_value(serde_json::json!({
            "instType":"SPOT","instId":"BTC-USD","tradeId":"11","ordId":"123",
            "billId":"111","side":"buy","fillPx":"100","fillSz":"0.05",
            "fee":"-0.01","feeCcy":"USD"
        })).unwrap();
        let bill: okx_api::AccountBill = serde_json::from_value(serde_json::json!({
            "billId":"111","instType":"SPOT","instId":"BTC-USD","ordId":"123",
            "tradeId":"11","ccy":"USD","fee":"-0.01","type":"2"
        })).unwrap();
        let mut history = okx_api::CompleteCampaignAccountHistory {
            from_ms: 1, through_ms: 100, orders: vec![order], fills: vec![fill],
            bills: vec![bill], source_digest:"f".repeat(64),
        };
        assert_eq!(
            TradeService::historical_usd_spot_entry(&history).unwrap(),
            dec("5.01").unwrap()
        );
        history.fills[0].fee = "0.03".into();
        history.bills[0].fee = "0.03".into();
        assert_eq!(TradeService::historical_usd_spot_entry(&history).unwrap(),
            dec("5.03").unwrap(), "rebate never increases campaign budget");
        history.fills[0].instrument_type="SWAP".into();
        assert!(TradeService::historical_usd_spot_entry(&history).is_err(),
            "SWAP cannot be priced without historical contract unit");
        history.fills[0].instrument_type="SPOT".into();
        history.fills[0].fee_currency="USDT".into();
        assert!(TradeService::historical_usd_spot_entry(&history).is_err(),
            "no observed USD/USDT rate may be promoted into a guaranteed source");
        history.fills[0].fee_currency="USD".into();
        history.bills.push(serde_json::from_value(serde_json::json!({
            "billId":"112","instType":"SPOT","instId":"BTC-USD",
            "ccy":"USD","fee":"-1","type":"7"
        })).unwrap());
        assert!(TradeService::historical_usd_spot_entry(&history).is_err(),
            "standalone bills and borrow costs cannot be treated as zero");
    }

    #[test]
    fn e1_empty_history_is_zero_but_unclassified_bill_never_is() {
        let mut history = okx_api::CompleteCampaignAccountHistory {
            from_ms: 1, through_ms: 100, orders:vec![], fills:vec![], bills:vec![],
            source_digest:"f".repeat(64),
        };
        assert_eq!(TradeService::historical_usd_spot_entry(&history).unwrap(),
                   Decimal::ZERO);
        history.bills.push(serde_json::from_value(serde_json::json!({
            "billId":"999","ccy":"USD","fee":"-1","type":"2"
        })).unwrap());
        assert_eq!(TradeService::historical_usd_spot_entry(&history),
            Err("ENTRY_OPENING_STANDALONE_BILL_UNCLASSIFIED".into()));
    }

    #[test]
    fn e1_held_entry_sources_reject_tampering_and_stale_tariffs() {
        use okx_observation::{FeeScheduleInput, FeeScheduleSnapshot};
        let stamp = "2026-10-30T00:00:00Z";
        let t = chrono::DateTime::parse_from_rfc3339(stamp)
            .unwrap()
            .timestamp_millis();
        let fee = FeeScheduleSnapshot::from_input(FeeScheduleInput {
            instrument_id: "BTC-USDT-SWAP".into(),
            reference_generation: "reference-1".into(),
            source_received_at: stamp.into(),
            exchange_timestamp_ms: t.to_string(),
            level: "Lv1".into(),
            maker_rate: "-0.0002".into(),
            taker_rate: "0.0005".into(),
            exact_for_instrument: true,
        })
        .unwrap();
        let mut quote = okx_execution::EntryQuoteEvidence {
            schema: "okx.entry-known-quote/v1".into(),
            account_key: "okx:real:test".into(),
            installation_id: "test".into(),
            intent_id: "intent".into(),
            plan_hash: "a".repeat(64),
            policy_revision: 1,
            instrument_id: "BTC-USDT-SWAP".into(),
            quantity: dec("1").unwrap(),
            limit_price: dec("100").unwrap(),
            contract_value: dec("1").unwrap(),
            fee_rate: dec("0.0005").unwrap(),
            maker_rate: fee.maker_rate.clone(),
            taker_rate: fee.taker_rate.clone(),
            fee_level: fee.level.clone(),
            fee_exchange_at: t,
            usd_per_usdt: Decimal::ONE,
            notional_usdt: dec("100").unwrap(),
            fee_usdt: dec("0.05").unwrap(),
            total_usdt: dec("100.05").unwrap(),
            total_usd: dec("100.05").unwrap(),
            fee_generation: fee.fee_generation.clone(),
            reference_generation: fee.reference_generation.clone(),
            fee_received_at: t,
            conversion_digest: "b".repeat(64),
            conversion_received_at: t,
            observed_at: t,
        };
        let fx = UsdConversion {
            usd_per_usdt: "1".into(),
            source_digest: "b".repeat(64),
            observed_at_ms: t as u64,
        };
        let known = entry_budget::quote(
            quote.quantity,
            quote.contract_value,
            quote.limit_price,
            quote.fee_rate,
            quote.usd_per_usdt,
        )
        .unwrap();
        let unproven = entry_budget::EntryCostProof::unproven_linear_quote(known.clone());
        assert_eq!(
            entry_budget::reserve_entry_evidence(&unproven, &quote, dec("101").unwrap()),
            Ok(None),
            "a correctly priced SQL quote cannot invent margin, fees, borrowing or bounded FX"
        );
        let mut tampered = unproven;
        tampered.quote.total_usd += Decimal::ONE;
        assert_eq!(
            entry_budget::reserve_entry_evidence(&tampered, &quote, dec("101").unwrap()),
            Err("ENTRY_RESERVE_QUOTE_BINDING_MISMATCH".into())
        );
        assert!(
            verify_post_held_entry_sources(&quote, &fee, "reference-1", Decimal::ONE, &fx, t + 1)
                .is_ok()
        );
        assert_eq!(
            verify_post_held_entry_sources(&quote, &fee, "reference-2", Decimal::ONE, &fx, t + 1),
            Err("ENTRY_FEE_SOURCE_SCOPE_MISMATCH".into())
        );
        assert_eq!(
            verify_post_held_entry_sources(
                &quote,
                &fee,
                "reference-1",
                dec("2").unwrap(),
                &fx,
                t + 1
            ),
            Err("POST_ENTRY_FEE_OR_SPEC_CHANGED".into())
        );
        quote.fee_generation = "tampered".into();
        assert!(
            verify_post_held_entry_sources(&quote, &fee, "reference-1", Decimal::ONE, &fx, t + 1)
                .is_err()
        );
        quote.fee_generation = fee.fee_generation.clone();
        assert_eq!(
            verify_post_held_entry_sources(
                &quote,
                &fee,
                "reference-1",
                Decimal::ONE,
                &fx,
                t + crate::config::ACCOUNT_MAX_AGE_MS + 1
            ),
            Err("ENTRY_FEE_SOURCE_STALE".into())
        );
        let invalid_provenance = UsdConversion {
            source_digest: "invalid".into(),
            ..fx.clone()
        };
        assert_eq!(
            verify_post_held_entry_sources(
                &quote,
                &fee,
                "reference-1",
                Decimal::ONE,
                &invalid_provenance,
                t + 1
            ),
            Err("POST_ENTRY_FX_SOURCE_INVALID".into())
        );
        let refreshed_same_rate = UsdConversion {
            source_digest: "c".repeat(64),
            ..fx.clone()
        };
        assert!(
            verify_post_held_entry_sources(
                &quote,
                &fee,
                "reference-1",
                Decimal::ONE,
                &refreshed_same_rate,
                t + 1
            )
            .is_ok()
        );
        let modified_fx = UsdConversion {
            usd_per_usdt: "1.01".into(),
            ..fx
        };
        assert_eq!(
            verify_post_held_entry_sources(
                &quote,
                &fee,
                "reference-1",
                Decimal::ONE,
                &modified_fx,
                t + 1
            ),
            Err("POST_ENTRY_FX_CHANGED".into())
        );
    }

    #[test]
    fn k4_post_held_fx_same_rate_or_changed_rate() {
        assert!(verify_post_held_fx("0.999000000", "0.999").is_ok());
        assert_eq!(
            verify_post_held_fx("0.999", "0.998").unwrap_err(),
            "POST_FX_SOURCE_CHANGED_REQUIRES_NEW_RESERVE"
        );
        assert_eq!(
            verify_post_held_fx("0", "1").unwrap_err(),
            "POST_FX_SOURCE_INVALID"
        );
        assert!(verify_post_held_fx("1", "invalid").is_err());
    }
    #[test]
    fn e2_algo_census_source_age_is_part_of_cohort_proof() {
        let now_ms = 1_798_758_000_000_i64;
        assert_eq!(verified_algo_source_time(now_ms as u64, now_ms), Ok(now_ms));
        assert_eq!(
            verified_algo_source_time(0, now_ms),
            Err("COHORT_ALGO_SOURCE_STALE".into())
        );
        assert_eq!(
            verified_algo_source_time((now_ms + 1) as u64, now_ms),
            Err("COHORT_ALGO_SOURCE_STALE".into())
        );
        assert_eq!(
            verified_algo_source_time(
                (now_ms - crate::config::ACCOUNT_MAX_AGE_MS - 1) as u64,
                now_ms,
            ),
            Err("COHORT_ALGO_SOURCE_STALE".into())
        );
        assert_eq!(
            verified_algo_source_time(u64::MAX, now_ms),
            Err("COHORT_ALGO_SOURCE_TIME_INVALID".into())
        );
    }

    #[test]
    fn e2_census_zero_risk_is_only_attested_for_actual_venue_flat() {
        assert_eq!(
            certified_flat_external_exposure(["0", "0.000"].into_iter(), 0, 0),
            Ok("0")
        );
        assert_eq!(
            certified_flat_external_exposure(["0", "-0.01"].into_iter(), 0, 0),
            Err("COHORT_NONFLAT_EXPOSURE_UNPRICED".into())
        );
        assert_eq!(
            certified_flat_external_exposure(["0"].into_iter(), 1, 0),
            Err("COHORT_NONFLAT_EXPOSURE_UNPRICED".into())
        );
        assert_eq!(
            certified_flat_external_exposure(["0"].into_iter(), 0, 1),
            Err("COHORT_NONFLAT_EXPOSURE_UNPRICED".into())
        );
        assert!(certified_flat_external_exposure(["unknown"].into_iter(), 0, 0).is_err());
        assert_eq!(
            certified_flat_external_exposure(std::iter::empty::<&str>(), 0, 0),
            Ok("0")
        );
    }

    #[test]
    fn e2_census_canonical_digest_rejects_missing_duplicate_and_order_variation() {
        let keys = vec!["a".to_owned(), "b".to_owned()];
        let rows = vec![
            ("a".to_owned(), "proof_a".to_owned()),
            ("b".to_owned(), "proof_b".to_owned()),
        ];
        assert_eq!(
            canonical_census_content(rows.clone(), &keys),
            canonical_census_content(rows.iter().rev().cloned().collect(), &keys)
        );
        assert!(canonical_census_content(vec![rows[0].clone()], &keys).is_err());
        assert!(canonical_census_content(vec![rows[0].clone(), rows[0].clone()], &keys).is_err());
    }

    #[test]
    fn e2_active_real_intents_have_complete_bounded_central_coverage() {
        let plan = IntentPlan {
            side: "buy".into(),
            position_side: "long".into(),
            quantity: dec("1").unwrap(),
            limit_price: dec("100").unwrap(),
            ..IntentPlan::default()
        };
        let mut intent = StoredIntent {
            plan,
            intent_id: "intent_123".into(),
            idempotency_key: "key_123".into(),
            request_hash: "a".repeat(64),
            client_order_id: "client_123".into(),
            state: IntentState::Prepared,
            revision: 1,
            deadline: 123,
            plan_hash: "b".repeat(64),
            policy_revision: 3,
            risk_increase: true,
            instrument_id: "BTC-USDT-SWAP".into(),
            exchange_order_id: None,
            incident: None,
        };
        let account = format!("okx:real:{}", "a".repeat(64));
        let receipt = CentralReceipt {
            reservation_key: "central:receipt_123".into(),
            account_key: account.clone(),
            installation_id: "install".into(),
            instance_id: Some("manual".into()),
            intent_id: intent.intent_id.clone(),
            plan_hash: intent.plan_hash.clone(),
            policy_revision: 3,
            receipt_hash: "c".repeat(64),
            requested_loss_usd: dec("1").unwrap(),
            conversion_digest: "d".repeat(64),
        };
        assert!(verify_local_intent_coverage(&[(intent.clone(), None)], &[], &account).is_ok());
        intent.state = IntentState::Reserved;
        assert_eq!(
            verify_local_intent_coverage(&[(intent.clone(), None)], &[], &account),
            Err("COHORT_CENTRAL_RESERVATION_MISSING".into())
        );
        assert!(
            verify_local_intent_coverage(&[(intent.clone(), Some(receipt.clone()))], &[], &account)
                .is_ok()
        );
        let mut wrong = receipt.clone();
        wrong.account_key = "other".into();
        assert_eq!(
            verify_local_intent_coverage(&[(intent.clone(), Some(wrong))], &[], &account),
            Err("COHORT_CENTRAL_RESERVATION_MISMATCH".into())
        );
        intent.state = IntentState::Posting;
        assert_eq!(
            verify_local_intent_coverage(&[(intent.clone(), Some(receipt.clone()))], &[], &account),
            Err("COHORT_UNRESOLVED_POST_OUTCOME".into())
        );
        intent.state = IntentState::Unknown;
        assert!(
            verify_local_intent_coverage(&[(intent.clone(), Some(receipt.clone()))], &[], &account)
                .is_err()
        );
        intent.state = IntentState::Acknowledged;
        assert_eq!(
            verify_local_intent_coverage(&[(intent.clone(), Some(receipt.clone()))], &[], &account),
            Err("COHORT_EXCHANGE_ORDER_ID_UNPROVEN".into())
        );
        intent.exchange_order_id = Some("venue_abc".into());
        assert!(
            verify_local_intent_coverage(&[(intent.clone(), Some(receipt.clone()))], &[], &account)
                .is_ok()
        );
        let unresolved = vec![intent.intent_id.clone()];
        assert!(
            verify_local_intent_coverage(
                &[(intent.clone(), Some(receipt.clone()))],
                &unresolved,
                &account
            )
            .is_ok()
        );
        let mut terminal = intent.clone();
        terminal.state = IntentState::Filled;
        assert_eq!(
            verify_local_intent_coverage(
                &[(terminal, Some(receipt.clone()))],
                &unresolved,
                &account
            ),
            Err("COHORT_TERMINAL_RISK_RELEASE_PENDING".into())
        );
        let mut attempted = intent.clone();
        attempted.state = IntentState::Prepared;
        attempted.exchange_order_id = None;
        assert_eq!(
            verify_local_intent_coverage(&[(attempted, None)], &unresolved, &account),
            Err("COHORT_UNRESOLVED_RESERVE_ATTEMPT".into())
        );
        assert_eq!(
            verify_local_intent_coverage(
                &[
                    (intent.clone(), Some(receipt.clone())),
                    (intent.clone(), Some(receipt.clone()))
                ],
                &[],
                &account
            ),
            Err("COHORT_LOCAL_INTENT_CORRUPT".into())
        );
        let many = vec![(intent, Some(receipt)); 100];
        assert_eq!(
            verify_local_intent_coverage(&many, &[], &account),
            Err("COHORT_LOCAL_INTENTS_TRUNCATED".into())
        );
    }

    #[test]
    fn e2_pending_venue_order_cannot_steal_matching_client_id() {
        let plan = IntentPlan {
            side: "buy".into(),
            position_side: "long".into(),
            quantity: dec("2").unwrap(),
            limit_price: dec("100").unwrap(),
            ..IntentPlan::default()
        };
        let intent = StoredIntent {
            plan,
            intent_id: "intent_test".into(),
            idempotency_key: "test_id".into(),
            request_hash: "a".repeat(64),
            client_order_id: "client_test".into(),
            state: IntentState::Acknowledged,
            revision: 1,
            deadline: 123,
            plan_hash: "b".repeat(64),
            policy_revision: 1,
            risk_increase: true,
            instrument_id: "BTC-USDT-SWAP".into(),
            exchange_order_id: Some("exchange_test".into()),
            incident: None,
        };
        let order = okx_observation::PendingOrderState {
            order_id: "exchange_test".into(),
            client_order_id: Some("client_test".into()),
            instrument_type: "SWAP".into(),
            instrument_id: "BTC-USDT-SWAP".into(),
            side: "buy".into(),
            position_side: Some("long".into()),
            trade_mode: "cross".into(),
            order_type: "limit".into(),
            price: Some("99".into()),
            size: "2".into(),
            accumulated_fill_size: "0.5".into(),
            average_fill_price: None,
            state: "partially_filled".into(),
            reduce_only: Some(false),
            creation_time_ms: "1".into(),
            update_time_ms: "2".into(),
        };
        assert!(verify_pending_order_binding(&order, &intent).is_ok());
        let mut foreign = order.clone();
        foreign.order_id = "foreign_exchange_order".into();
        assert_eq!(
            verify_pending_order_binding(&foreign, &intent),
            Err("PENDING_ORDER_INTENT_MISMATCH".into())
        );
        let mut changed = order.clone();
        changed.side = "sell".into();
        assert!(verify_pending_order_binding(&changed, &intent).is_err());
        changed = order.clone();
        changed.size = "3".into();
        assert_eq!(
            verify_pending_order_binding(&changed, &intent),
            Err("PENDING_ORDER_QUANTITY_MISMATCH".into())
        );
        changed = order.clone();
        changed.accumulated_fill_size = "2.1".into();
        assert!(verify_pending_order_binding(&changed, &intent).is_err());
        changed = order.clone();
        changed.price = Some("101".into());
        assert_eq!(
            verify_pending_order_binding(&changed, &intent),
            Err("PENDING_ORDER_PRICE_BOUND_MISMATCH".into())
        );
        changed = order.clone();
        changed.price = None;
        assert_eq!(
            verify_pending_order_binding(&changed, &intent),
            Err("PENDING_ORDER_PRICE_UNPROVEN".into())
        );
        let mut terminal = intent;
        terminal.state = IntentState::Cancelled;
        assert!(verify_pending_order_binding(&order, &terminal).is_err());
    }

    #[test]
    fn e2_census_uses_oldest_verified_account_and_financial_source() {
        let received = "2026-10-30T00:00:00Z";
        let t = chrono::DateTime::parse_from_rfc3339(received)
            .unwrap()
            .timestamp_millis();
        assert_eq!(
            census_source_floor(received, "uid", "uid", true, t - 120, t + 300),
            Ok(t - 120)
        );
        assert_eq!(
            census_source_floor(received, "uid", "other", true, t, t),
            Err("COHORT_IDENTITY_MISMATCH".into())
        );
        assert_eq!(
            census_source_floor("invalid", "uid", "uid", true, t, t),
            Err("COHORT_ACCOUNT_SOURCE_TIME_INVALID".into())
        );
        assert_eq!(
            census_source_floor(received, "uid", "uid", true, t, t - 1),
            Err("COHORT_ACCOUNT_SOURCE_STALE".into())
        );
        assert_eq!(
            census_source_floor(
                received,
                "uid",
                "uid",
                true,
                t,
                t + crate::config::ACCOUNT_MAX_AGE_MS + 1,
            ),
            Err("COHORT_ACCOUNT_SOURCE_STALE".into())
        );
        assert_eq!(
            census_source_floor(received, "uid", "uid", false, t, t),
            Err("COHORT_FINANCIAL_SOURCE_INCOMPLETE".into())
        );
        assert_eq!(
            census_source_floor(
                received,
                "uid",
                "uid",
                true,
                t - crate::config::ACCOUNT_MAX_AGE_MS - 1,
                t,
            ),
            Err("COHORT_FINANCIAL_SOURCE_STALE".into())
        );
        assert_eq!(
            census_source_floor(received, "uid", "uid", true, t + 1, t),
            Err("COHORT_FINANCIAL_SOURCE_STALE".into())
        );
    }

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
    async fn prepost_fence_denial_is_durable_no_send_not_unknown() {
        let (store, intent, _root) = reserved("prepost_fence_denial", true, false, false).await;
        let result = reject_prepost_denial(&store, &intent, "POST_CENSUS_NOT_AFTER_RESERVE")
            .await
            .unwrap();
        assert_eq!(result["data"]["state"], "REJECTED");
        assert_eq!(result["data"]["send_state"], "NOT_STARTED");
        assert_eq!(result["data"]["risk_release_state"], "PENDING");
        assert_eq!(
            result["data"]["primary_reason"],
            "POST_CENSUS_NOT_AFTER_RESERVE"
        );
        let saved = store
            .call(deadline(), |s| s.intent("intent_fixture"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.state, IntentState::Rejected);
        assert_eq!(
            saved.incident.as_deref(),
            Some("POST_CENSUS_NOT_AFTER_RESERVE")
        );
        // Re-observation must not erase the original denial or mint a POST.
        let sql_proof = store
            .call(deadline(), |s| s.unsent_release_receipt("intent_fixture"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sql_proof.reason, "POST_CENSUS_NOT_AFTER_RESERVE");
        assert_eq!(sql_proof.intent_id, "intent_fixture");
        let replay = reject_prepost_denial(&store, &intent, "COMMAND_EXPIRED")
            .await
            .unwrap();
        assert_eq!(
            replay["data"]["primary_reason"],
            "POST_CENSUS_NOT_AFTER_RESERVE"
        );
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
        assert!(
            store
                .call(deadline(), |s| s.unsent_release_receipt("intent_fixture"))
                .await
                .unwrap()
                .is_none(),
            "a possible POST must never acquire an SQL no-send release witness"
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
    async fn k5_f5_f6_reopen_sql_never_reposts_unknown_or_invents_no_send() {
        // F5: durable POST claim, no gateway invocation before the owner dies.
        // F6: gateway accepted the call, but the reply was lost.
        // In both cases the restarted owner cannot infer absence from SQLite.
        for (label, reply_lost) in [
            ("k5_f5_claim_reopen", false),
            ("k5_f6_lost_reply_reopen", true),
        ] {
            let (store, intent, root) = reserved(label, true, false, false).await;
            let gateway = FixtureGateway {
                posts: AtomicUsize::new(0),
                lose: true,
            };
            if reply_lost {
                assert_eq!(
                    post_once(&store, &intent, deadline(), || gateway.send())
                        .await
                        .unwrap(),
                    None
                );
                assert_eq!(gateway.posts.load(Ordering::SeqCst), 1);
            } else {
                assert!(
                    store
                        .call(deadline(), |s| s.begin_post("intent_fixture", 1, now()))
                        .await
                        .unwrap()
                );
                assert_eq!(gateway.posts.load(Ordering::SeqCst), 0);
            }

            // Use the owner's native SQLite online backup to obtain an
            // immutable crash/recovery image. The live worker still owns its
            // writer lock until it terminates; reopening that same live file
            // would only test a race in test teardown, not durability.
            let recovery_image = root.join("recovery.db");
            store
                .call(deadline(), move |s| s.backup(&recovery_image))
                .await
                .unwrap();
            drop(store);
            let reopened = AccountStoreHandle::open_existing(
                root.join("recovery.db"),
                format!("okx:real:{}", "a".repeat(64)),
                "installation_test".into(),
            )
            .await
            .unwrap();
            let promoted = reopened
                .call(deadline(), |s| s.recover_posting(now()))
                .await
                .unwrap();
            assert_eq!(promoted, if reply_lost { 0 } else { 1 });
            let restored = reopened
                .call(deadline(), |s| s.intent("intent_fixture"))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(restored.state, IntentState::Unknown);
            assert!(
                reopened
                    .call(deadline(), |s| s.unsent_release_receipt("intent_fixture"))
                    .await
                    .unwrap()
                    .is_none(),
                "an unresolved post claim cannot authorize central release"
            );
            // Even with the original idempotency key/intent and a fresh owner,
            // recovery must never invoke place_order for a second time.
            let count_before = gateway.posts.load(Ordering::SeqCst);
            for _ in 0..2 {
                assert_eq!(
                    post_once(&reopened, &intent, deadline(), || gateway.send())
                        .await
                        .unwrap(),
                    None
                );
            }
            assert_eq!(gateway.posts.load(Ordering::SeqCst), count_before);
            assert_eq!(
                reopened
                    .call(deadline(), |s| s.intent("intent_fixture"))
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                IntentState::Unknown
            );

            if reply_lost {
                // Only an explicit simulated venue readback can resolve F6.
                reopened
                    .call(deadline(), |s| {
                        s.record_order_readback(
                            "intent_fixture",
                            "exchange_f6_readback",
                            IntentState::Filled,
                            Decimal::ONE,
                            now(),
                        )
                    })
                    .await
                    .unwrap();
                assert_eq!(
                    reopened
                        .call(deadline(), |s| s.intent("intent_fixture"))
                        .await
                        .unwrap()
                        .unwrap()
                        .state,
                    IntentState::Filled
                );
                assert_eq!(gateway.posts.load(Ordering::SeqCst), 1);
            }
        }
    }

    // Genuine OS-level termination on the Windows CI runner. The child keeps
    // the original SQLite writer lease open until TerminateProcess kills it.
    // This is not a Cloudflare/venue failpoint; the gateway is a file fixture.
    #[cfg(windows)]
    #[test]
    #[ignore = "called only as an isolated crash-test subprocess"]
    fn k5_windows_process_kill_child_fixture() {
        use std::io::Write as _;
        let role = std::env::var("OKX_K5_KILL_ROLE").expect("crash fixture role");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (store, intent, root) = reserved(&format!("kill_{role}"), true, false, false).await;
            match role.as_str() {
                "f5" => {
                    assert!(
                        store
                            .call(deadline(), |s| s.begin_post("intent_fixture", 1, now()))
                            .await
                            .unwrap()
                    );
                    println!("OKX_K5_READY={}", root.display());
                    std::io::stdout().flush().unwrap();
                    // Parent kills this OS process after observing the durable claim.
                    std::future::pending::<()>().await;
                }
                "f6" => {
                    let witness = root.join("venue-accepted.marker");
                    let ready_root = root.clone();
                    // The gateway fixture has a durable external acceptance
                    // witness but never supplies an ACK to the native owner.
                    let _ = post_once(
                        &store,
                        &intent,
                        Instant::now() + Duration::from_secs(60),
                        || async move {
                            let mut f = std::fs::File::create(witness).unwrap();
                            f.write_all(b"accepted_once").unwrap();
                            f.sync_all().unwrap();
                            println!("OKX_K5_READY={}", ready_root.display());
                            std::io::stdout().flush().unwrap();
                            std::future::pending::<Result<u32, String>>().await
                        },
                    )
                    .await;
                    panic!("unresolved F6 gateway returned instead of being killed");
                }
                _ => panic!("unrecognized crash fixture role: {role}"),
            }
        });
    }

    #[cfg(windows)]
    #[test]
    fn k5_windows_actual_process_kill_f5_f6_ten_times_each() {
        use std::io::BufRead as _;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        for role in ["f5", "f6"] {
            for run in 0..10 {
                let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--ignored",
                        "--exact",
                        "trade_service::execute::tests::k5_windows_process_kill_child_fixture",
                        "--nocapture",
                    ])
                    .env("OKX_K5_KILL_ROLE", role)
                    .stdin(std::process::Stdio::null())
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::inherit())
                    .spawn()
                    .unwrap();
                let stdout = child.stdout.take().unwrap();
                let (tx, rx) = std::sync::mpsc::sync_channel(1);
                let reader = std::thread::spawn(move || {
                    for line in std::io::BufReader::new(stdout).lines() {
                        let line = line.unwrap();
                        if let Some((_, root)) = line.split_once("OKX_K5_READY=") {
                            let _ = tx.send(std::path::PathBuf::from(root.trim()));
                            break;
                        }
                    }
                });
                // Event-driven readiness from the *durably committed* child,
                // with a bounded deadline rather than a blind sleep.
                let root = match rx.recv_timeout(Duration::from_secs(20)) {
                    Ok(root) => root,
                    Err(e) => {
                        let _ = child.kill();
                        let _ = child.wait();
                        reader.join().unwrap();
                        panic!("F{role} run {run} did not arm the crash point: {e}");
                    }
                };
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "child exited without actual OS termination"
                );
                child.kill().unwrap();
                let exit = child.wait().unwrap();
                reader.join().unwrap();
                assert!(!exit.success(), "forced-kill child exited successfully");

                let venue_witness = root.join("venue-accepted.marker");
                if role == "f6" {
                    assert_eq!(std::fs::read(&venue_witness).unwrap(), b"accepted_once");
                } else {
                    assert!(!venue_witness.exists(), "F5 must have zero sends");
                }

                // Reopen the actual crash-surviving WAL database, NOT an
                // online backup. Killing the child must free the writer lease.
                runtime.block_on(async {
                    let store = AccountStoreHandle::open_existing(
                        root.join("account.db"),
                        format!("okx:real:{}", "a".repeat(64)),
                        "installation_test".into(),
                    )
                    .await
                    .unwrap();
                    let changed = store
                        .call(deadline(), |s| s.recover_posting(now()))
                        .await
                        .unwrap();
                    assert_eq!(changed, 1, "the crashed child left a POSTING claim");
                    let intent = store
                        .call(deadline(), |s| s.intent("intent_fixture"))
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(intent.state, IntentState::Unknown);
                    assert!(
                        store
                            .call(deadline(), |s| {
                                s.unsent_release_receipt("intent_fixture")
                            })
                            .await
                            .unwrap()
                            .is_none(),
                        "crash must never fabricate an unsent release witness"
                    );

                    let gateway = FixtureGateway {
                        posts: AtomicUsize::new(0),
                        lose: false,
                    };
                    for _ in 0..2 {
                        assert_eq!(
                            post_once(&store, &intent, deadline(), || gateway.send())
                                .await
                                .unwrap(),
                            None,
                            "replaying an unknown original intent cannot place an order"
                        );
                    }
                    assert_eq!(gateway.posts.load(Ordering::SeqCst), 0);
                    if role == "f6" {
                        store
                            .call(deadline(), |s| {
                                s.record_order_readback(
                                    "intent_fixture",
                                    "fixture-venue-confirmed",
                                    IntentState::Filled,
                                    Decimal::ONE,
                                    now(),
                                )
                            })
                            .await
                            .unwrap();
                        assert_eq!(
                            store
                                .call(deadline(), |s| s.intent("intent_fixture"))
                                .await
                                .unwrap()
                                .unwrap()
                                .state,
                            IntentState::Filled
                        );
                        // One and only one externally witnessed acceptance.
                        assert_eq!(std::fs::read(&venue_witness).unwrap(), b"accepted_once");
                    } else {
                        assert_eq!(
                            store
                                .call(deadline(), |s| s.intent("intent_fixture"))
                                .await
                                .unwrap()
                                .unwrap()
                                .state,
                            IntentState::Unknown,
                            "F5 cannot be called no-send after a process crash"
                        );
                    }
                });
            }
        }
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
    /// Test the actual Rust single-POST boundary, not an extra mock engine.
    /// External source labels are synthetic and do not prove chat identity.
    /// Risk receipts are fixtures: the independent RiskBook CAS tests remain separate.
    #[tokio::test]
    async fn three_sources_share_post_once_and_unknown_gate() {
        let (store, human, _root) = reserved("three_sources", true, false, false).await;
        let (chat, native) = store
            .call(deadline(), |s| {
                let native_instance = StrategyInstance {
                    instance_id: "native_alpha".into(),
                    definition_id: "ema_cross_v1".into(),
                    definition_version: "1".into(),
                    parameters: r#"{"parameters":{"fast":2,"slow":3},"instruments":["BTC-USDT-SWAP"],"timeframe_ms":60000}"#.into(),
                    enabled: true,
                    policy_revision: 1,
                    budget: Decimal::from(2),
                    last_decision_key: None,
                };
                s.mutate_instance(
                    "three_source_create",
                    "three_source_create_body",
                    "create",
                    &native_instance,
                    None,
                    now(),
                )?;
                let claim = s
                    .claim_strategy_decision_with_id(
                        "native_alpha",
                        "bar_3sources",
                        "native_inputs",
                        1,
                        now(),
                    )?
                    .ok_or(okx_execution::StoreError::Invalid("STRATEGY_CLAIM_MISSING"))?;
                let prepare = |s: &mut okx_execution::AccountStore,
                               source: &str,
                               instance: &str,
                               decision_id: String,
                               inputs: &str|
                 -> okx_execution::StoreResult<StoredIntent> {
                    let definition = s.strategy_definition(instance)?;
                    let intent_id = format!("three_sources_{source}");
                    let decision = Decision {
                        decision_id,
                        instance_id: Some(instance.into()),
                        decision_at: now(),
                        input_digest: inputs.into(),
                        policy_revision: 1,
                        code_version: "three-sources-fixture".into(),
                    };
                    let new_intent = NewIntent {
                        plan: IntentPlan {
                            strategy_definition_id: definition.definition_id,
                            strategy_definition_version: definition.version,
                            strategy_parameters_digest: definition.parameters_digest,
                            contract_value: Decimal::ONE,
                            inverse: false,
                            settlement_currency: "USDT".into(),
                            position_side: "long".into(),
                            private_source_generation: 1,
                            operation: "open".into(),
                            side: "buy".into(),
                            quantity: Decimal::ONE,
                            limit_price: Decimal::from(100),
                            stop_price: Some(Decimal::from(90)),
                            strategy_instance: instance.into(),
                            planned_loss_usd: Decimal::ONE,
                            source_generation: 1,
                        },
                        intent_id: intent_id.clone(),
                        idempotency_key: format!("request_three_{source}"),
                        request_hash: format!("request_body_three_{source}"),
                        client_order_id: format!("client_three_{source}"),
                        deadline: now() + 30_000,
                        plan_hash: format!("plan_three_{source}"),
                        policy_revision: 1,
                        risk_increase: true,
                        instrument_id: "BTC-USDT-SWAP".into(),
                    };
                    let saved = s.prepare(&decision, &new_intent)?;
                    // Preserve the single SQL intent when the same command is replayed.
                    assert_eq!(s.prepare(&decision, &new_intent)?.intent_id, intent_id);
                    s.reserve(
                        &CentralReceipt {
                            reservation_key: format!("synthetic_reservation_{source}"),
                            account_key: s.meta().account_key.clone(),
                            installation_id: s.meta().installation_id.clone(),
                            instance_id: Some(instance.into()),
                            intent_id,
                            plan_hash: new_intent.plan_hash,
                            policy_revision: 1,
                            receipt_hash: format!("synthetic_receipt_{source}"),
                            requested_loss_usd: Decimal::ONE,
                            conversion_digest: "synthetic_conversion".into(),
                        },
                        now(),
                    )?;
                    Ok(saved)
                };
                let chat = prepare(s, "chat", "manual", "external_chat".into(), "unverified_external")?;
                let native = prepare(s, "native", "native_alpha", claim, "native_inputs")?;
                Ok((chat, native))
            })
            .await
            .unwrap();
        let gateway = FixtureGateway {
            posts: AtomicUsize::new(0),
            lose: false,
        };
        // All three compete for the same AccountStoreHandle / durable POST fence.
        let (first, second, third) = tokio::join!(
            post_once(&store, &human, deadline(), || gateway.send()),
            post_once(&store, &chat, deadline(), || gateway.send()),
            post_once(&store, &native, deadline(), || gateway.send())
        );
        let intents = [human, chat, native];
        let mut winner = None;
        let mut blocked = Vec::new();
        for (index, result) in [first, second, third].into_iter().enumerate() {
            match result {
                Ok(Some(1)) => {
                    assert!(winner.replace(index).is_none(), "only one source may POST");
                }
                Err(reason) if reason.contains("ACCOUNT_UNKNOWN_EXPOSURE") => blocked.push(index),
                other => panic!("unexpected POST admission result: {other:?}"),
            }
        }
        assert_eq!(blocked.len(), 2);
        assert_eq!(gateway.posts.load(Ordering::SeqCst), 1);
        let winner = winner.expect("one admitted source");
        let winner_id = intents[winner].intent_id.clone();
        store
            .call(deadline(), move |s| {
                s.record_outcome(
                    &winner_id,
                    IntentState::Filled,
                    Some("first_fixture_order"),
                    "verified-fixture-venue-readback",
                    now(),
                )
            })
            .await
            .unwrap();
        assert!(
            post_once(&store, &intents[winner], deadline(), || gateway.send())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(gateway.posts.load(Ordering::SeqCst), 1);

        let lost = FixtureGateway {
            posts: AtomicUsize::new(0),
            lose: true,
        };
        assert!(
            post_once(&store, &intents[blocked[0]], deadline(), || lost.send())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(lost.posts.load(Ordering::SeqCst), 1);
        let unknown_id = intents[blocked[0]].intent_id.clone();
        assert_eq!(
            store
                .call(deadline(), move |s| s.intent(&unknown_id))
                .await
                .unwrap()
                .unwrap()
                .state,
            IntentState::Unknown
        );
        assert!(
            post_once(&store, &intents[blocked[1]], deadline(), || gateway.send())
                .await
                .unwrap_err()
                .contains("ACCOUNT_UNKNOWN_EXPOSURE")
        );
        assert_eq!(gateway.posts.load(Ordering::SeqCst), 1);
        let resolved_id = intents[blocked[0]].intent_id.clone();
        store
            .call(deadline(), move |s| {
                s.record_outcome(
                    &resolved_id,
                    IntentState::Filled,
                    Some("resolved_fixture_order"),
                    "verified-fixture-reconciliation",
                    now(),
                )
            })
            .await
            .unwrap();
        assert_eq!(
            post_once(&store, &intents[blocked[1]], deadline(), || gateway.send())
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(gateway.posts.load(Ordering::SeqCst), 2);
        let last_id = intents[blocked[1]].intent_id.clone();
        store
            .call(deadline(), move |s| {
                s.record_outcome(
                    &last_id,
                    IntentState::Filled,
                    Some("last_fixture_order"),
                    "verified-fixture-venue-readback",
                    now(),
                )
            })
            .await
            .unwrap();
        for intent in &intents {
            let id = intent.intent_id.clone();
            let state = store
                .call(deadline(), move |s| s.intent(&id))
                .await
                .unwrap();
            assert_eq!(state.unwrap().state, IntentState::Filled);
        }
        // Two acknowledged fixture POSTs plus one lost-reply fixture POST.
        assert_eq!(
            gateway.posts.load(Ordering::SeqCst) + lost.posts.load(Ordering::SeqCst),
            3
        );
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

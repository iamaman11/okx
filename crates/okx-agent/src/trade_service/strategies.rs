use super::*;
use okx_protocol::trading::{CapitalDirection, StrategyConfigurationV1, StrategyParametersV1};
impl TradeService {
    pub async fn strategies(
        &self,
        account: &str,
        request: &StrategyRequest,
        supplied: Option<&CentralGovernanceSnapshot>,
    ) -> Result<Value, String> {
        if !request.valid() {
            return Err("INVALID_STRATEGY_REQUEST".into());
        }
        let r = self.resources(account).await?;
        if matches!(
            request,
            StrategyRequest::List {} | StrategyRequest::Status { .. }
        ) {
            let body = request.clone();
            let result = r
                .execution
                .store
                .call(deadline(), move |s| match body {
                    StrategyRequest::Status { instance_id } => {
                        s.strategy_status(&instance_id, now())
                    }
                    _ => Ok(json!({"instances":s.instances()?})),
                })
                .await
                .map_err(err)?;
            return Ok(completed(result));
        }

        let _capacity = r
            .mutation_capacity
            .try_acquire()
            .map_err(|_| "ACCOUNT_MUTATION_BUSY")?;
        let (state, control) = self.authority(&r, supplied).await?;
        let campaign_limit_usd = state.state.campaign_loss_limit_usd.clone();
        let request_hash = digest(&(r.configuration.account_key.wire(), request))?;
        if let StrategyRequest::CapitalAssign {
            idempotency_key, instance_id, direction, amount_usd,
            expected_authority_revision,
        } = request {
            let revision = i64::try_from(*expected_authority_revision)
                .map_err(|_| "CAPITAL_AUTHORITY_REVISION_INVALID")?;
            if i64::try_from(state.state.revision).ok() != Some(revision) {
                return Err("CAPITAL_AUTHORITY_REVISION_MISMATCH".into());
            }
            if *direction == CapitalDirection::Assign && !control.access_enabled() {
                return Err("ACCOUNT_NEW_RISK_PAUSED".into());
            }
            let amount=Decimal::from_str(amount_usd)
                .map_err(|_| "INVALID_CAPITAL_AMOUNT")?;
            let delta=if *direction==CapitalDirection::Assign {
                amount
            } else {
                Decimal::ZERO.checked_sub(amount)
                    .ok_or("INVALID_CAPITAL_AMOUNT")?
            };
            let verified_authority_receipt=digest(&state)?;
            let account_key=r.configuration.account_key.wire();
            let idempotency_key=idempotency_key.clone();
            let instance_id=instance_id.clone();
            let operation_hash=request_hash.clone();
            let observed_at=now();
            let result=r.execution.store.call(deadline(),move |s| {
                let source=s.capital_state(observed_at)?.nav_source_ref;
                let authorized=okx_execution::AuthorizedCapitalAssignment {
                    account_key,
                    installation_id:s.meta().installation_id.clone(),
                    nav_source_ref:source,
                    operation_id:idempotency_key,
                    request_hash:operation_hash,
                    instance_id,
                    delta_usd:delta,
                    authority_revision:revision,
                    authority_receipt:verified_authority_receipt,
                    time:observed_at,
                };
                let receipt=s.commit_authorized_capital_assignment(&authorized)?;
                let capital=s.capital_state(observed_at)?;
                Ok(json!({"receipt":receipt,"capital":capital}))
            }).await.map_err(err)?;
            return Ok(completed(result));
        }
        let body = request.clone();
        let result = r
            .execution
            .store
            .call(deadline(), move |s| {
                let operation = match &body {
                    StrategyRequest::Create {
                        idempotency_key, ..
                    }
                    | StrategyRequest::Update {
                        idempotency_key, ..
                    }
                    | StrategyRequest::Start {
                        idempotency_key, ..
                    }
                    | StrategyRequest::Stop {
                        idempotency_key, ..
                    } => idempotency_key,
                    _ => {
                        return Err(okx_execution::StoreError::Invalid(
                            "STRATEGY_MUTATION_REQUIRED",
                        ));
                    }
                };
                if let Some(old) = s.operation(operation)? {
                    if old.request_hash != request_hash {
                        return Err(okx_execution::StoreError::Invalid("IDEMPOTENCY_CONFLICT"));
                    }
                    return serde_json::from_str::<StrategyInstance>(&old.result_ref).map_err(
                        |_| okx_execution::StoreError::Invalid("STRATEGY_RECEIPT_CORRUPT"),
                    );
                }
                let instances = s.instances()?;
                let (operation, action, instance, expected) = match &body {
                    StrategyRequest::Create {
                        idempotency_key,
                        instance_id,
                        definition_id,
                        version,
                        parameters,
                        instruments,
                        timeframe_ms,
                        budget_usd,
                    } => {
                        if !matches!(
                            definition_id.as_str(),
                            "ema_cross_v1" | "no_trade" | "close_momentum" | "two_bar_momentum"
                        ) {
                            return Err(okx_execution::StoreError::Invalid(
                                "STRATEGY_DEFINITION_UNSUPPORTED",
                            ));
                        }
                        let config = StrategyConfigurationV1 {
                            parameters: StrategyParametersV1::parse(parameters).ok_or(
                                okx_execution::StoreError::Invalid("STRATEGY_PARAMETERS_INVALID"),
                            )?,
                            instruments: instruments.clone(),
                            timeframe_ms: *timeframe_ms,
                        };
                        if !config.valid() || *version != 1 {
                            return Err(okx_execution::StoreError::Invalid(
                                "STRATEGY_CONFIGURATION_INVALID",
                            ));
                        }
                        let parameters = serde_json::to_string(&config).map_err(|_| {
                            okx_execution::StoreError::Invalid("STRATEGY_CONFIGURATION_INVALID")
                        })?;
                        let instance = StrategyInstance {
                            instance_id: instance_id.clone(),
                            definition_id: definition_id.clone(),
                            definition_version: version.to_string(),
                            parameters,
                            enabled: false,
                            policy_revision: 1,
                            budget: Decimal::from_str(budget_usd).map_err(|_| {
                                okx_execution::StoreError::Invalid("INVALID_BUDGET")
                            })?,
                            last_decision_key: None,
                        };
                        (idempotency_key.clone(), "create", instance, None)
                    }
                    StrategyRequest::Update {
                        idempotency_key,
                        instance_id,
                        expected_revision,
                        parameters,
                        budget_usd,
                    } => {
                        let mut i = instances
                            .iter()
                            .find(|i| &i.instance_id == instance_id)
                            .cloned()
                            .ok_or(okx_execution::StoreError::Invalid(
                                "STRATEGY_INSTANCE_NOT_FOUND",
                            ))?;
                        let mut config: StrategyConfigurationV1 =
                            serde_json::from_str(&i.parameters).map_err(|_| {
                                okx_execution::StoreError::Invalid("STRATEGY_CONFIGURATION_CORRUPT")
                            })?;
                        config.parameters = StrategyParametersV1::parse(parameters).ok_or(
                            okx_execution::StoreError::Invalid("STRATEGY_PARAMETERS_INVALID"),
                        )?;
                        if !config.valid() {
                            return Err(okx_execution::StoreError::Invalid(
                                "STRATEGY_CONFIGURATION_INVALID",
                            ));
                        }
                        i.parameters = serde_json::to_string(&config).map_err(|_| {
                            okx_execution::StoreError::Invalid("STRATEGY_CONFIGURATION_INVALID")
                        })?;
                        i.budget = Decimal::from_str(budget_usd)
                            .map_err(|_| okx_execution::StoreError::Invalid("INVALID_BUDGET"))?;
                        i.policy_revision = (*expected_revision as i64) + 1;
                        (
                            idempotency_key.clone(),
                            "update",
                            i,
                            Some(*expected_revision as i64),
                        )
                    }
                    StrategyRequest::Start {
                        idempotency_key,
                        instance_id,
                    }
                    | StrategyRequest::Stop {
                        idempotency_key,
                        instance_id,
                    } => {
                        let mut i = instances
                            .iter()
                            .find(|i| &i.instance_id == instance_id)
                            .cloned()
                            .ok_or(okx_execution::StoreError::Invalid(
                                "STRATEGY_INSTANCE_NOT_FOUND",
                            ))?;
                        let expected = i.policy_revision;
                        i.policy_revision += 1;
                        i.enabled = matches!(&body, StrategyRequest::Start { .. });
                        (
                            idempotency_key.clone(),
                            if i.enabled { "start" } else { "stop" },
                            i,
                            Some(expected),
                        )
                    }
                    _ => {
                        return Err(okx_execution::StoreError::Invalid(
                            "STRATEGY_MUTATION_REQUIRED",
                        ));
                    }
                };
                if action == "start" && !control.access_enabled() {
                    return Err(okx_execution::StoreError::Invalid(
                        "ACCOUNT_NEW_RISK_PAUSED",
                    ));
                }
                // Strategy budget is a per-decision maximum modeled loss in USD, NOT a
                // notional allocation and NOT a central reservation. Only actual plans
                // reserve aggregate campaign capacity through RiskBook CAS.
                let ceiling = Decimal::from_str(&campaign_limit_usd)
                    .map_err(|_| okx_execution::StoreError::Invalid("INVALID_CAMPAIGN_BUDGET"))?;
                validate_strategy_risk_budget_usd(instance.budget, ceiling)?;
                let receipt = s.mutate_instance(
                    &operation,
                    &request_hash,
                    action,
                    &instance,
                    expected,
                    now(),
                )?;
                serde_json::from_str::<StrategyInstance>(&receipt.result_ref)
                    .map_err(|_| okx_execution::StoreError::Invalid("STRATEGY_RECEIPT_CORRUPT"))
            })
            .await
            .map_err(err)?;
        Ok(completed(json!({"instance":result})))
    }
    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut timer = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {_ = shutdown.changed()=>{if *shutdown.borrow(){break;}},_ = timer.tick()=>{
            let turn=self.maintenance_turn();tokio::pin!(turn);tokio::select!{_ = &mut turn=>{},_ = shutdown.changed()=>{if *shutdown.borrow(){let _=tokio::time::timeout(Duration::from_secs(10),&mut turn).await;break;}}}
            }}
        }
    }
    async fn maintenance_turn(&self) {
        use futures_util::{StreamExt, stream};
        stream::iter(self.accounts.list().await)
            .for_each_concurrent(4, |config| async move {
                let Some(r) = self.accounts.resolve(&config.alias).await else {
                    return;
                };
                let _ = self.reconcile_restored(&r).await;
                let reconcile = async {
                    let account = config.account_key.wire();
                    let mut offset = self.recovery_schedule.lock().await.page(&account);
                    let mut result = r
                        .execution
                        .store
                        .call(deadline(), move |s| {
                            s.reconcilable_intents_page(RECOVERY_BATCH, offset)
                        })
                        .await;
                    if result.as_ref().is_ok_and(Vec::is_empty) && offset != 0 {
                        // The candidate set shrank after successful recovery.
                        // Wrap rather than starving fresh lower-index entries.
                        offset = 0;
                        result = r
                            .execution
                            .store
                            .call(deadline(), move |s| {
                                s.reconcilable_intents_page(RECOVERY_BATCH, 0)
                            })
                            .await;
                    }
                    match result {
                        Ok(intents) => {
                            self.recovery_schedule
                                .lock()
                                .await
                                .advance_page(&account, offset, intents.len());
                            stream::iter(intents)
                                .for_each_concurrent(2, |intent| {
                                    let r = &r;
                                    let account = account.clone();
                                    async move {
                                        let scheduled = self
                                            .recovery_schedule
                                            .lock()
                                            .await
                                            .due(&account, &intent.intent_id, Instant::now());
                                        if !scheduled {
                                            return;
                                        }
                                        let result = self.status_intent(r, &intent.intent_id).await;
                                        let settled = result.as_ref().is_ok_and(|value| {
                                            value["data"]["central_release"]["state"] == "verified"
                                                || value["data"]["central_release"]["state"]
                                                    == "not_required"
                                        });
                                        let incident = {
                                            let mut schedule = self.recovery_schedule.lock().await;
                                            if settled {
                                                schedule.finish(&account, &intent.intent_id);
                                                false
                                            } else {
                                                schedule.overdue_incident(
                                                    &account,
                                                    &intent.intent_id,
                                                    intent.deadline,
                                                    now(),
                                                )
                                            }
                                        };
                                        if incident {
                                            let reason = match &result {
                                                Ok(value) => value["reason_code"]
                                                    .as_str()
                                                    .or_else(|| {
                                                        value["data"]["central_release"]
                                                            ["reason_code"]
                                                            .as_str()
                                                    })
                                                    .unwrap_or("RECONCILIATION_NOT_TERMINAL"),
                                                Err(_) => "RECONCILIATION_READBACK_FAILED",
                                            };
                                            eprintln!(
                                                "OKX_RECOVERY_INCIDENT_V1 account={} intent={} reason={} state=UNRESOLVED_OVER_30S",
                                                account, intent.intent_id, reason
                                            );
                                        }
                                    }
                                })
                                .await;
                        }
                        Err(_) => {
                            eprintln!(
                                "OKX_RECOVERY_SCAN_ERROR_V1 account={} state=SQL_READ_FAILED",
                                account
                            );
                        }
                    }
                };
                let decisions = async {
                    if let Ok(instances) =
                        r.execution.store.call(deadline(), |s| s.instances()).await
                    {
                        for instance in instances.into_iter().filter(|i| i.enabled) {
                            let _ = self.strategy_turn(&r, &instance).await;
                        }
                    }
                };
                tokio::join!(reconcile, decisions);
            })
            .await;
    }
    async fn strategy_turn(
        &self,
        r: &Arc<AccountResources>,
        instance: &StrategyInstance,
    ) -> Result<(), String> {
        let analytics = self
            .analytics
            .read()
            .await
            .clone()
            .ok_or("ANALYTICS_SOURCE_NOT_READY")?;
        let config: StrategyConfigurationV1 = serde_json::from_str(&instance.parameters)
            .map_err(|_| "STRATEGY_CONFIGURATION_CORRUPT")?;
        if !config.valid() {
            return Err("STRATEGY_CONFIGURATION_INVALID".into());
        }
        let (fast, slow) = (config.parameters.fast, config.parameters.slow);
        let timeframe = config.timeframe_ms;
        for instrument in &config.instruments {
            let bars = analytics
                .load_closed_closes(instrument, timeframe, (slow * 4).clamp(100, 10000))
                .await?;
            let last = bars.last().ok_or("STRATEGY_HISTORY_UNAVAILABLE")?.0;
            let decision_key = format!(
                "bar_{}",
                &digest(&(
                    r.configuration.account_key.wire(),
                    &instance.instance_id,
                    &instance.definition_id,
                    &instance.definition_version,
                    &instance.parameters,
                    instrument,
                    timeframe,
                    last
                ))?[..40]
            );
            let mut kernel = okx_analysis::operators::StrategyKernel::from_id(
                &instance.definition_id,
                okx_analysis::operators::EmaParameters { fast, slow },
            )?;
            let mut signal = None;
            for (_, close) in &bars {
                signal = kernel.push_closed(*close)?;
            }
            let decision_digest = digest(&(last, &bars))?;
            let (state, control) = self.authority(r, None).await?;
            if !control.access_enabled() {
                return Err("ACCOUNT_NEW_RISK_PAUSED".into());
            }
            let id = instance.instance_id.clone();
            let dk = decision_key.clone();
            let input_for_execute = decision_digest.clone();
            let revision = state.state.revision as i64;
            let expected_version = instance.definition_version.clone();
            let claimed = r
                .execution
                .store
                .call(deadline(), move |s| {
                    if s.strategy_definition(&id)?.version != expected_version {
                        return Err(okx_execution::StoreError::Invalid(
                            "STRATEGY_VERSION_CHANGED",
                        ));
                    }
                    s.claim_strategy_decision_with_id(&id, &dk, &decision_digest, revision, now())
                })
                .await
                .map_err(err)?;
            let Some(claimed_decision_id) = claimed else {
                continue;
            };
            let desired = signal.unwrap_or(0);
            if desired == 0 {
                let id = instance.instance_id.clone();
                let key = decision_key.clone();
                r.execution
                    .store
                    .call(deadline(), move |s| {
                        s.finish_strategy_no_trade(&id, &key, now())
                    })
                    .await
                    .map_err(err)?;
                continue;
            }
            let inst = instrument.to_owned();
            let instance_id = instance.instance_id.clone();
            let lots = r
                .execution
                .store
                .call(deadline(), move |s| {
                    Ok(s.active_lots(&inst)?
                        .into_iter()
                        .filter(|l| l.instance_id.as_deref() == Some(&instance_id))
                        .collect::<Vec<_>>())
                })
                .await
                .map_err(err)?;
            let desired_side = if desired > 0 { "long" } else { "short" };
            if lots.iter().any(|l| l.side != desired_side) {
                for lot in lots.iter().filter(|l| l.side != desired_side) {
                    if let Some(reference) = &lot.intent_id {
                        let close_key = format!(
                            "auto_close_{}",
                            &digest(&(decision_key.clone(), reference))?[..32]
                        );
                        let _ = self
                            .close(r, &close_key, reference, 10000, Some(&state))
                            .await?;
                    }
                }
                continue;
            }
            if !lots.is_empty() {
                let id = instance.instance_id.clone();
                let key = decision_key.clone();
                r.execution
                    .store
                    .call(deadline(), move |s| {
                        s.finish_strategy_no_trade(&id, &key, now())
                    })
                    .await
                    .map_err(err)?;
                continue;
            }
            let command = TradeRequest::Execute {
                idempotency_key: decision_key,
                operation: TradeOperation::Open,
                strategy_instance: instance.instance_id.clone(),
                instrument: instrument.into(),
                side: if desired > 0 {
                    PositionSide::Long
                } else {
                    PositionSide::Short
                },
                sizing: Sizing::MaxLossUsd {
                    max_loss_usd: exact(
                        instance
                            .budget
                            .min(dec(&state.state.campaign_loss_limit_usd)?),
                    ),
                },
                order: OrderConstraints {
                    order_type: "fok".into(),
                    limit_price: None,
                    reference: None,
                    max_entry_usd: None,
                    max_slippage_bps: 5,
                },
                protection: Some(Protection::Distance {
                    stop_distance_bps: 50,
                }),
                expires_in_ms: 10000,
            };
            let _ = self
                .execute_for_definition(
                    r,
                    &command,
                    Some(&state),
                    instance.definition_version.clone(),
                    input_for_execute,
                    claimed_decision_id,
                )
                .await?;
        }
        Ok(())
    }
}

fn validate_strategy_risk_budget_usd(
    budget_usd: Decimal,
    campaign_limit_usd: Decimal,
) -> Result<(), okx_execution::StoreError> {
    if budget_usd <= Decimal::ZERO || budget_usd > campaign_limit_usd {
        return Err(okx_execution::StoreError::Invalid(
            "STRATEGY_RISK_BUDGET_USD_OUT_OF_RANGE",
        ));
    }
    // This is a per-order modeled LOSS limit. It is not capital or notional,
    // and must never be compared to venue max_notional_usdt. Total occupied
    // risk remains exclusively bounded by central per-intent CAS reservations.
    Ok(())
}

#[cfg(test)]
mod budget_contract_tests {
    use super::*;
    #[test]
    fn strategy_risk_budget_not_notional() {
        let campaign_limit_usd = Decimal::from(10);
        let permitted_per_order_loss_usd = Decimal::from(2);
        // Notional is irrelevant to checking a USD loss budget. Notional and
        // leverage have their own venue rules at later plan admission.
        let _notional_usdt = Decimal::from(1000);
        assert!(
            validate_strategy_risk_budget_usd(permitted_per_order_loss_usd, campaign_limit_usd,)
                .is_ok()
        );
        assert!(validate_strategy_risk_budget_usd(Decimal::from(11), campaign_limit_usd,).is_err());
        assert!(validate_strategy_risk_budget_usd(Decimal::ZERO, campaign_limit_usd,).is_err());
    }
}

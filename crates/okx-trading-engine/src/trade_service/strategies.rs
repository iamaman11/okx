use super::*;
use okx_ipc_contracts::research_ipc::{
    ClosedPrice, SignalDefinitionId, SignalDefinitionVersion, SignalInstanceId, SignalInstrumentId,
    SignalPosition, SignalRequest, StrategySignalInput, StrategySignalProposal,
};
use okx_ipc_contracts::trading::{CapitalDirection, StrategyConfigurationV1, StrategyParametersV1};
#[cfg(test)]
#[path = "strategy_contract_tests.rs"]
mod strategy_contract_tests;

/// Research output has exactly one provenance path. A proposal is untrusted
/// until Engine checks it against its own source and authority; the legacy
/// decision path is available only when the child explicitly lacks the outbox.
enum ResearchSignal {
    UntrustedProposal(StrategySignalProposal),
    LegacyDecision(SignalPosition),
}

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
            let result = match request {
                StrategyRequest::Status { instance_id } => {
                    let instance_id = instance_id.clone();
                    r.execution
                        .store
                        .strategy_status(deadline(), instance_id, now())
                        .await
                        .map_err(err)?
                }
                _ => {
                    json!({"instances":r.execution.store.strategy_instances(deadline()).await.map_err(err)?})
                }
            };
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
            idempotency_key,
            instance_id,
            direction,
            amount_usd,
            expected_authority_revision,
        } = request
        {
            let revision = i64::try_from(*expected_authority_revision)
                .map_err(|_| "CAPITAL_AUTHORITY_REVISION_INVALID")?;
            if i64::try_from(state.state.revision).ok() != Some(revision) {
                return Err("CAPITAL_AUTHORITY_REVISION_MISMATCH".into());
            }
            if *direction == CapitalDirection::Assign && !control.access_enabled() {
                return Err("ACCOUNT_NEW_RISK_PAUSED".into());
            }
            let amount = Decimal::from_str(amount_usd).map_err(|_| "INVALID_CAPITAL_AMOUNT")?;
            let delta = if *direction == CapitalDirection::Assign {
                amount
            } else {
                Decimal::ZERO
                    .checked_sub(amount)
                    .ok_or("INVALID_CAPITAL_AMOUNT")?
            };
            let verified_authority_receipt = digest(&state)?;
            let account_key = r.configuration.account_key.wire();
            let idempotency_key = idempotency_key.clone();
            let instance_id = instance_id.clone();
            let operation_hash = request_hash.clone();
            let observed_at = now();
            let result = r
                .execution
                .store
                .assign_capital(
                    deadline(),
                    okx_trade_execution::CapitalAssignmentCommand {
                        account_key,
                        operation_id: idempotency_key,
                        request_hash: operation_hash,
                        instance_id,
                        delta_usd: delta,
                        authority_revision: revision,
                        authority_receipt: verified_authority_receipt,
                        time: observed_at,
                    },
                )
                .await
                .map_err(err)?;
            return Ok(completed(serde_json::to_value(result).map_err(err)?));
        }
        let body = request.clone();
        let result: okx_trade_execution::StoreResult<StrategyInstance> = async {
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
                    return Err(okx_trade_execution::StoreError::Invalid(
                        "STRATEGY_MUTATION_REQUIRED",
                    ));
                }
            };
            if let Some(old) = r
                .execution
                .store
                .operation(
                    deadline(),
                    crate::execution_store::OperationId::new(operation.clone())?,
                )
                .await?
            {
                if old.request_hash != request_hash {
                    return Err(okx_trade_execution::StoreError::Invalid(
                        "IDEMPOTENCY_CONFLICT",
                    ));
                }
                return serde_json::from_str::<StrategyInstance>(&old.result_ref).map_err(|_| {
                    okx_trade_execution::StoreError::Invalid("STRATEGY_RECEIPT_CORRUPT")
                });
            }
            let instances = r.execution.store.strategy_instances(deadline()).await?;
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
                        return Err(okx_trade_execution::StoreError::Invalid(
                            "STRATEGY_DEFINITION_UNSUPPORTED",
                        ));
                    }
                    let config = StrategyConfigurationV1 {
                        parameters: StrategyParametersV1::parse(parameters).ok_or(
                            okx_trade_execution::StoreError::Invalid("STRATEGY_PARAMETERS_INVALID"),
                        )?,
                        instruments: instruments.clone(),
                        timeframe_ms: *timeframe_ms,
                    };
                    if !config.valid() || *version != 1 {
                        return Err(okx_trade_execution::StoreError::Invalid(
                            "STRATEGY_CONFIGURATION_INVALID",
                        ));
                    }
                    let parameters = serde_json::to_string(&config).map_err(|_| {
                        okx_trade_execution::StoreError::Invalid("STRATEGY_CONFIGURATION_INVALID")
                    })?;
                    let instance = StrategyInstance {
                        instance_id: instance_id.clone(),
                        definition_id: definition_id.clone(),
                        definition_version: version.to_string(),
                        parameters,
                        enabled: false,
                        policy_revision: 1,
                        budget: Decimal::from_str(budget_usd).map_err(|_| {
                            okx_trade_execution::StoreError::Invalid("INVALID_BUDGET")
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
                        .ok_or(okx_trade_execution::StoreError::Invalid(
                            "STRATEGY_INSTANCE_NOT_FOUND",
                        ))?;
                    let mut config: StrategyConfigurationV1 = serde_json::from_str(&i.parameters)
                        .map_err(|_| {
                        okx_trade_execution::StoreError::Invalid("STRATEGY_CONFIGURATION_CORRUPT")
                    })?;
                    config.parameters = StrategyParametersV1::parse(parameters).ok_or(
                        okx_trade_execution::StoreError::Invalid("STRATEGY_PARAMETERS_INVALID"),
                    )?;
                    if !config.valid() {
                        return Err(okx_trade_execution::StoreError::Invalid(
                            "STRATEGY_CONFIGURATION_INVALID",
                        ));
                    }
                    i.parameters = serde_json::to_string(&config).map_err(|_| {
                        okx_trade_execution::StoreError::Invalid("STRATEGY_CONFIGURATION_INVALID")
                    })?;
                    i.budget = Decimal::from_str(budget_usd)
                        .map_err(|_| okx_trade_execution::StoreError::Invalid("INVALID_BUDGET"))?;
                    let expected = i64::try_from(*expected_revision).map_err(|_| {
                        okx_trade_execution::StoreError::Invalid("INSTANCE_REVISION_CONFLICT")
                    })?;
                    i.policy_revision =
                        expected
                            .checked_add(1)
                            .ok_or(okx_trade_execution::StoreError::Invalid(
                                "INSTANCE_REVISION_OVERFLOW",
                            ))?;
                    (idempotency_key.clone(), "update", i, Some(expected))
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
                        .ok_or(okx_trade_execution::StoreError::Invalid(
                            "STRATEGY_INSTANCE_NOT_FOUND",
                        ))?;
                    let expected = i.policy_revision;
                    i.policy_revision =
                        expected
                            .checked_add(1)
                            .ok_or(okx_trade_execution::StoreError::Invalid(
                                "INSTANCE_REVISION_OVERFLOW",
                            ))?;
                    i.enabled = matches!(&body, StrategyRequest::Start { .. });
                    (
                        idempotency_key.clone(),
                        if i.enabled { "start" } else { "stop" },
                        i,
                        Some(expected),
                    )
                }
                _ => {
                    return Err(okx_trade_execution::StoreError::Invalid(
                        "STRATEGY_MUTATION_REQUIRED",
                    ));
                }
            };
            if action == "start" && !control.access_enabled() {
                return Err(okx_trade_execution::StoreError::Invalid(
                    "ACCOUNT_NEW_RISK_PAUSED",
                ));
            }
            // Strategy budget is a per-decision maximum modeled loss in USD, NOT a
            // notional allocation and NOT a central reservation. Only actual plans
            // reserve aggregate campaign capacity through RiskBook CAS.
            let ceiling = Decimal::from_str(&campaign_limit_usd)
                .map_err(|_| okx_trade_execution::StoreError::Invalid("INVALID_CAMPAIGN_BUDGET"))?;
            validate_strategy_risk_budget_usd(instance.budget, ceiling)?;
            let receipt = r
                .execution
                .store
                .mutate_strategy_instance(
                    deadline(),
                    okx_trade_execution::account_worker::StrategyMutationCommand {
                        operation_id: operation,
                        request_hash: request_hash.clone(),
                        action: action.into(),
                        instance,
                        expected_revision: expected,
                        time: now(),
                    },
                )
                .await?;
            serde_json::from_str::<StrategyInstance>(&receipt.result_ref)
                .map_err(|_| okx_trade_execution::StoreError::Invalid("STRATEGY_RECEIPT_CORRUPT"))
        }
        .await;
        let result = result.map_err(err)?;
        Ok(completed(json!({"instance":result})))
    }
    pub async fn run(&self, shutdown: tokio::sync::watch::Receiver<bool>) {
        tokio::join!(
            run_periodic_turn(shutdown.clone(), || self.recovery_turn()),
            run_periodic_turn(shutdown, || self.strategy_decisions_turn()),
        );
    }

    async fn recovery_turn(&self) {
        // One existing maintenance owner. The recovery scheduler provides
        // bounded retry spacing for the campaign, while independent account
        // capture keeps advancing cursors every two seconds. No archive
        // history is downloaded from trade.execute or from a new daemon.
        if let Some(authority) = self.governance.read().await.clone()
            && authority.state.campaign_entry_limit_usd.is_some()
        {
            let due = self.recovery_schedule.lock().await.due(
                "campaign-opening",
                &authority.state.campaign_id,
                Instant::now(),
            );
            if due {
                let until = Instant::now() + Duration::from_secs(4);
                let result = tokio::time::timeout(Duration::from_secs(4), async {
                    // After authenticated account connection, the original
                    // retained policy may now have a complete SQL roster.

                    self.risk.apply_required_governance(&authority).await?;
                    self.install_captured_empty_campaign_opening(&authority, until)
                        .await
                })
                .await;
                match result {
                    Ok(Ok(())) => {
                        // A successfully reconciled source needs a fresh
                        // cheap readiness read on the next maintenance turn.
                        // Do not back off into 30s windows of stale admission.
                        self.recovery_schedule
                            .lock()
                            .await
                            .finish("campaign-opening", &authority.state.campaign_id);
                    }
                    Ok(Err(reason)) => {
                        eprintln!("OKX_ENTRY_BASELINE_V1 state=PENDING reason={reason}");
                    }
                    Err(_) => {
                        eprintln!("OKX_ENTRY_BASELINE_V1 state=PENDING reason=TIMEOUT");
                    }
                }
            }
        }
        use futures_util::{StreamExt, stream};
        stream::iter(self.accounts.list().await)
            .for_each_concurrent(4, |config| async move {

                let Some(r) = self.accounts.resolve(&config.alias).await else {
                    return;
                };
                let _ = self.reconcile_restored(&r).await;

                let account = config.account_key.wire();
                let mut offset = self.recovery_schedule.lock().await.page(&account);
                let mut result = r
                    .execution
                    .store
                    .reconcilable_intents_page(deadline(), RECOVERY_BATCH, offset)
                    .await;
                if result.as_ref().is_ok_and(Vec::is_empty) && offset != 0 {
                    // The candidate set shrank after successful recovery.
                    // Wrap rather than starving fresh lower-index entries.
                    offset = 0;
                    result = r
                        .execution
                        .store
                        .reconcilable_intents_page(deadline(), RECOVERY_BATCH, 0)
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

                                    let result = self.status_intent(r, &intent.intent_id, None).await;

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
            })
            .await;
    }

    async fn strategy_decisions_turn(&self) {
        use futures_util::{StreamExt, stream};
        stream::iter(self.accounts.list().await)
            .for_each_concurrent(4, |config| async move {
                let Some(r) = self.accounts.resolve(&config.alias).await else {
                    return;
                };
                if let Ok(instances) = r.execution.store.strategy_instances(deadline()).await {
                    for instance in instances.into_iter().filter(|i| i.enabled) {
                        let _ = self.strategy_turn(&r, &instance).await;
                    }
                }
            })
            .await;
    }

    async fn strategy_turn(
        &self,
        r: &Arc<AccountResources>,
        instance: &StrategyInstance,
    ) -> Result<(), String> {
        let live_source = self
            .live_source
            .read()
            .await
            .clone()
            .ok_or("ANALYTICS_SOURCE_NOT_READY")?;
        let config: StrategyConfigurationV1 = serde_json::from_str(&instance.parameters)
            .map_err(|_| "STRATEGY_CONFIGURATION_CORRUPT")?;
        if !config.valid() {
            return Err("STRATEGY_CONFIGURATION_INVALID".into());
        }
        let research = self
            .signal_research
            .read()
            .await
            .clone()
            .ok_or("STRATEGY_RESEARCH_NOT_READY")?;
        let (fast, slow) = (config.parameters.fast, config.parameters.slow);
        let timeframe = config.timeframe_ms;
        let use_outbox = research.supports_strategy_signal_outbox().await?;
        for instrument in &config.instruments {
            let bars = live_source
                .load_closed_closes(instrument, timeframe, (slow * 4).clamp(100, 10000))
                .await?;
            let last = bars.last().ok_or("STRATEGY_HISTORY_UNAVAILABLE")?.0;
            let prices = SignalRequest {
                strategy_id: instance.definition_id.clone(),
                parameters: okx_ipc_contracts::analytics::EmaParameters { fast, slow },
                closes: bars
                    .iter()
                    .map(|(time_ms, close)| ClosedPrice {
                        time_ms: *time_ms,
                        close: *close,
                    })
                    .collect(),
            };
            let research_signal = if use_outbox {
                let Some(proposal) = research
                    .stage_strategy_signal(StrategySignalInput {
                        instance_id: SignalInstanceId::new(instance.instance_id.clone())?,
                        definition_id: SignalDefinitionId::new(instance.definition_id.clone())?,
                        definition_version: SignalDefinitionVersion::new(
                            instance.definition_version.clone(),
                        )?,
                        instrument: SignalInstrumentId::new(instrument.to_owned())?,
                        timeframe_ms: timeframe,
                        available_at_ms: u64::try_from(now()).map_err(err)?,
                        prices,
                    })
                    .await?
                else {
                    // An outbox-capable child declining to emit a proposal is
                    // not a signal to downgrade to the legacy evaluation API.
                    continue;
                };
                ResearchSignal::UntrustedProposal(proposal)
            } else {
                ResearchSignal::LegacyDecision(research.evaluate_signal(prices).await?.position)
            };
            let decision_digest = digest(&(last, &bars))?;
            let (state, control) = self.authority(r, None).await?;
            if !control.access_enabled() {
                return Err("ACCOUNT_NEW_RISK_PAUSED".into());
            }
            let input_for_execute = decision_digest.clone();
            let revision = i64::try_from(state.state.revision)
                .map_err(|_| "STRATEGY_POLICY_REVISION_INVALID")?;
            let account = r.configuration.account_key.wire();
            let (claim, position) = match research_signal {
                ResearchSignal::UntrustedProposal(proposal) => {
                    strategy_signal::check_strategy_signal(
                        &proposal,
                        &strategy_signal::TrustedStrategySignalContext {
                            instance,
                            configuration: &config,
                            authenticated_account_key: &account,
                            instrument,
                            last_closed_ms: last,
                            source_digest: &decision_digest,
                            policy_revision: revision,
                            now_ms: now(),
                        },
                    )?
                    .into_claim()
                }
                ResearchSignal::LegacyDecision(position) => {
                    let decision_key = strategy_signal::scoped_decision_key(
                        &account, instance, instrument, timeframe, last,
                    )?;
                    (
                        okx_trade_execution::account_worker::StrategyDecisionClaimCommand {
                            instance_id: instance.instance_id.clone(),
                            decision_key,
                            input_digest: decision_digest,
                            policy_revision: revision,
                            expected_version: instance.definition_version.clone(),
                            time: now(),
                        },
                        position,
                    )
                }
            };
            let decision_key = claim.decision_key.clone();
            let claimed = r
                .execution
                .store
                .claim_strategy_decision(deadline(), claim)
                .await
                .map_err(err)?;
            let Some(claimed_decision_id) = claimed else {
                continue;
            };
            let desired = match position {
                SignalPosition::Long => 1,
                SignalPosition::Short => -1,
                SignalPosition::Flat | SignalPosition::Warmup => 0,
            };
            if desired == 0 {
                let id = instance.instance_id.clone();
                let key = decision_key.clone();
                r.execution
                    .store
                    .finish_strategy_no_trade(
                        deadline(),
                        id,
                        key,
                        instance.definition_version.clone(),
                        now(),
                    )
                    .await
                    .map_err(err)?;
                continue;
            }
            let inst = instrument.to_owned();
            let instance_id = instance.instance_id.clone();
            let lots = r
                .execution
                .store
                .active_lots(deadline(), inst)
                .await
                .map_err(err)?
                .into_iter()
                .filter(|lot| lot.instance_id.as_deref() == Some(&instance_id))
                .collect::<Vec<_>>();
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
                    .finish_strategy_no_trade(
                        deadline(),
                        id,
                        key,
                        instance.definition_version.clone(),
                        now(),
                    )
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

// Each lane owns at most one turn at a time. In particular, a Research
// response can delay new signals without delaying the next recovery scan.
async fn run_periodic_turn<F>(
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    mut start_turn: impl FnMut() -> F,
) where
    F: std::future::Future<Output = ()>,
{
    let mut timer = tokio::time::interval(Duration::from_secs(1));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let stopping = async {
        let _ = shutdown.wait_for(|stop| *stop).await;
    };
    tokio::pin!(stopping);
    loop {
        tokio::select! {
            biased;
            _ = &mut stopping => break,
            _ = timer.tick() => {}
        }
        let turn = start_turn();
        tokio::pin!(turn);
        tokio::select! {
            biased;
            _ = &mut stopping => {
                let _ = tokio::time::timeout(Duration::from_secs(10), &mut turn).await;
                break;
            }
            _ = &mut turn => {}
        }
    }
}

#[cfg(test)]
#[tokio::test(start_paused = true)]
async fn pending_strategy_turn_does_not_delay_a_second_recovery_turn() {
    use tokio::sync::{Notify, mpsc, watch};

    let (stop, shutdown) = watch::channel(false);
    let (events, mut recovery_turns) = mpsc::unbounded_channel();
    let strategy_started = Notify::new();
    let strategy_release = Notify::new();

    let recovery = run_periodic_turn(shutdown.clone(), || async {
        events.send(()).unwrap();
    });
    let strategies = run_periodic_turn(shutdown, || async {
        strategy_started.notify_one();
        strategy_release.notified().await;
    });
    let assert_progress = async {
        tokio::time::timeout(Duration::from_secs(3), async {
            strategy_started.notified().await;
            assert_eq!(recovery_turns.recv().await, Some(()));
            assert_eq!(recovery_turns.recv().await, Some(()));
        })
        .await
        .expect("recovery did not advance while the strategy turn was held");

        stop.send(true).unwrap();
        strategy_release.notify_one();
    };
    tokio::join!(recovery, strategies, assert_progress);
}

fn validate_strategy_risk_budget_usd(
    budget_usd: Decimal,
    campaign_limit_usd: Decimal,
) -> Result<(), okx_trade_execution::StoreError> {
    if budget_usd <= Decimal::ZERO || budget_usd > campaign_limit_usd {
        return Err(okx_trade_execution::StoreError::Invalid(
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

use super::*;
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
        let body = request.clone();
        let result=r.execution.store.call(deadline(),move|s|{
 let operation=match &body {StrategyRequest::Create{idempotency_key,..}|StrategyRequest::Update{idempotency_key,..}|StrategyRequest::Start{idempotency_key,..}|StrategyRequest::Stop{idempotency_key,..}=>idempotency_key,_=>return Err(okx_execution::StoreError::Invalid("STRATEGY_MUTATION_REQUIRED"))};
 if let Some(old)=s.operation(operation)? {if old.request_hash!=request_hash {return Err(okx_execution::StoreError::Invalid("IDEMPOTENCY_CONFLICT"));}return serde_json::from_str::<StrategyInstance>(&old.result_ref).map_err(|_|okx_execution::StoreError::Invalid("STRATEGY_RECEIPT_CORRUPT"));}
 let mut instances=s.instances()?;
 let (operation,action,instance,expected)=match &body{
 StrategyRequest::Create{idempotency_key,instance_id,definition_id,version,parameters,instruments,timeframe_ms,budget_usd}=>{
 if !matches!(definition_id.as_str(),"ema_cross_v1"|"no_trade"|"close_momentum"|"two_bar_momentum"){return Err(okx_execution::StoreError::Invalid("STRATEGY_DEFINITION_UNSUPPORTED"));}
 let parameters=serde_json::json!({"parameters":parameters,"instruments":instruments,"timeframe_ms":timeframe_ms});
 let instance=StrategyInstance{instance_id:instance_id.clone(),definition_id:definition_id.clone(),definition_version:version.to_string(),parameters:parameters.to_string(),enabled:false,policy_revision:1,budget:Decimal::from_str(budget_usd).map_err(|_|okx_execution::StoreError::Invalid("INVALID_BUDGET"))?,last_decision_key:None};
 (idempotency_key.clone(),"create",instance,None)
 },
 StrategyRequest::Update{idempotency_key,instance_id,expected_revision,parameters,budget_usd}=>{let mut i=instances.iter().find(|i|&i.instance_id==instance_id).cloned().ok_or(okx_execution::StoreError::Invalid("STRATEGY_INSTANCE_NOT_FOUND"))?;let mut config:Value=serde_json::from_str(&i.parameters).map_err(|_|okx_execution::StoreError::Invalid("STRATEGY_CONFIGURATION_CORRUPT"))?;config["parameters"]=parameters.clone();i.parameters=config.to_string();i.budget=Decimal::from_str(budget_usd).map_err(|_|okx_execution::StoreError::Invalid("INVALID_BUDGET"))?;i.policy_revision=(*expected_revision as i64)+1;(idempotency_key.clone(),"update",i,Some(*expected_revision as i64))},
 StrategyRequest::Start{idempotency_key,instance_id}|StrategyRequest::Stop{idempotency_key,instance_id}=>{let mut i=instances.iter().find(|i|&i.instance_id==instance_id).cloned().ok_or(okx_execution::StoreError::Invalid("STRATEGY_INSTANCE_NOT_FOUND"))?;let expected=i.policy_revision;i.policy_revision+=1;i.enabled=matches!(&body,StrategyRequest::Start{..});(idempotency_key.clone(),if i.enabled{"start"}else{"stop"},i,Some(expected))},_=>return Err(okx_execution::StoreError::Invalid("STRATEGY_MUTATION_REQUIRED"))
 };
 if action=="start"&&!control.access_enabled(){return Err(okx_execution::StoreError::Invalid("ACCOUNT_NEW_RISK_PAUSED"));}
 // Strategy budget is a per-decision maximum modeled loss in USD, NOT a
 // notional allocation and NOT a central reservation. Only actual plans
 // reserve aggregate campaign capacity through RiskBook CAS.
 let ceiling=Decimal::from_str(&campaign_limit_usd).map_err(|_|okx_execution::StoreError::Invalid("INVALID_CAMPAIGN_BUDGET"))?;
 if instance.budget<=Decimal::ZERO || instance.budget>ceiling {
     return Err(okx_execution::StoreError::Invalid("STRATEGY_RISK_BUDGET_USD_OUT_OF_RANGE"));
 }
 let receipt=s.mutate_instance(&operation,&request_hash,action,&instance,expected,now())?;
 serde_json::from_str::<StrategyInstance>(&receipt.result_ref).map_err(|_|okx_execution::StoreError::Invalid("STRATEGY_RECEIPT_CORRUPT"))
 }).await.map_err(err)?;
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
                    if let Ok(intents) = r
                        .execution
                        .store
                        .call(deadline(), |s| s.reconcilable_intents(16))
                        .await
                    {
                        stream::iter(intents)
                            .for_each_concurrent(2, |intent| {
                                let r = &r;
                                async move {
                                    let _ = self.status_intent(r, &intent.intent_id).await;
                                }
                            })
                            .await;
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
        let config: Value = serde_json::from_str(&instance.parameters).map_err(err)?;
        let fast = config["parameters"]["fast"].as_u64().unwrap_or(10) as usize;
        let slow = config["parameters"]["slow"].as_u64().unwrap_or(20) as usize;
        let timeframe = config["timeframe_ms"]
            .as_u64()
            .ok_or("STRATEGY_TIMEFRAME_INVALID")?;
        let instruments = config["instruments"]
            .as_array()
            .ok_or("STRATEGY_INSTRUMENTS_INVALID")?;
        for instrument in instruments {
            let instrument = instrument.as_str().ok_or("STRATEGY_INSTRUMENT_INVALID")?;
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
                    s.claim_strategy_decision(&id, &dk, &decision_digest, revision, now())
                })
                .await
                .map_err(err)?;
            if !claimed {
                continue;
            }
            let desired = signal.unwrap_or(0);
            if desired == 0 {
                let id = instance.instance_id.clone();
                let key = decision_key.clone();
                r.execution.store.call(deadline(), move |s| {
                    s.finish_strategy_no_trade(&id, &key, now())
                }).await.map_err(err)?;
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
                r.execution.store.call(deadline(), move |s| {
                    s.finish_strategy_no_trade(&id, &key, now())
                }).await.map_err(err)?;
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
                )
                .await?;
        }
        Ok(())
    }
}

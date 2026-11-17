use super::*;

impl AnalyticsService {
    pub(super) async fn submit(&self, request: ResearchRequest) -> Result<Value, String> {
        // Estimate the complete batch before SQL admission or fetching candles.
        // Large requests are rejected before any allocation or source I/O.
        let memory_mib = budget::estimated_mib(&request)?;
        let hash = okx_research::canonical_sha256(&("analytics/448.1", &request))
            .map_err(|e| e.to_string())?;
        let hash = hash.strip_prefix("sha256:").unwrap_or(&hash).to_owned();
        let key = match &request {
            ResearchRequest::Backtest {
                idempotency_key, ..
            }
            | ResearchRequest::Grid {
                idempotency_key, ..
            }
            | ResearchRequest::Benchmark { idempotency_key } => idempotency_key.clone(),
            _ => return Err("INVALID_JOB_REQUEST".into()),
        };
        let mut semantic = serde_json::to_value(&request).map_err(|e| e.to_string())?;
        semantic
            .as_object_mut()
            .ok_or("INVALID_JOB_REQUEST")?
            .remove("idempotency_key");
        let semantic_hash = okx_research::canonical_sha256(&("analytics/semantic/v1", semantic))
            .map_err(|e| e.to_string())?;
        let key = okx_protocol::research_ipc::IdempotencyKey::new(key).map_err(str::to_owned)?;
        let runner = RunnerGuard::new(self.runners.clone());
        let (job, new, cancel) = self
            .jobs
            .enqueue_semantic_async(key, hash.clone(), semantic_hash)
            .await?;
        let hash = job.id.clone();
        if !new {
            return Ok(json!(job));
        }
        let this = self.clone();
        tokio::spawn(async move {
            let _runner = runner;
            let progress = match &request {
                ResearchRequest::Grid {
                    instruments,
                    variants,
                    ..
                } => Some(Arc::new(candidate_progress::Tracker::new(
                    instruments.len(),
                    variants.len(),
                ))),
                _ => None,
            };
            if let Some(progress) = &progress {
                let _ = candidate_progress::publish(
                    this.jobs.clone(),
                    hash.clone(),
                    progress.snapshot(),
                )
                .await;
            }
            let outcome = async {
                // Bounded SQL admission happens at submit. Accepted queued
                // jobs wait for an existing heavy permit instead of becoming
                // FAILED because two other research jobs are in progress.
                let _heavy = tokio::select! {
                    result = this.heavy.clone().acquire_owned() =>
                        result.map_err(|_| "ANALYTICS_QUEUE_CLOSED")?,
                    _ = executor::cancelled(cancel.clone()) =>
                        return Err("CANCELLED".into()),
                };
                // Hold the entire batch reservation through artifact publish
                // and terminal state transition. Multiple heavy jobs share one
                // global 4 GiB estimate; wait cancellably, never oversubscribe.
                let _memory = tokio::select! {
                    permit = this.research_memory.clone().acquire_many_owned(memory_mib) =>
                        permit.map_err(|_| "RESEARCH_MEMORY_POOL_CLOSED")?,
                    _ = executor::cancelled(cancel.clone()) =>
                        return Err("CANCELLED".into()),
                };
                this.jobs
                    .update_async(hash.clone(), jobs::State::Running, None, None)
                    .await?;
                if matches!(request, ResearchRequest::Benchmark { .. }) {
                    return benchmark::run(
                        this.cpu.clone(),
                        this.artifacts.clone(),
                        cancel.clone(),
                    )
                    .await;
                }
                let instruments = match &request {
                    ResearchRequest::Backtest { instruments, .. }
                    | ResearchRequest::Grid { instruments, .. } => instruments.clone(),
                    _ => unreachable!(),
                };
                if instruments.len() == 1 {
                    return this
                        .execute_one_observed(
                            request,
                            cancel.clone(),
                            progress.clone().map(|p| (p, 0)),
                        )
                        .await;
                }
                let mut work = stream::iter(instruments.into_iter().enumerate().map(
                    |(index, instrument)| {
                        let service = this.clone();
                        let mut single = request.clone();
                        let token = cancel.clone();
                        let observation = progress.clone().map(|p| (p, index));
                        match &mut single {
                            ResearchRequest::Backtest { instruments, .. }
                            | ResearchRequest::Grid { instruments, .. } => {
                                *instruments = vec![instrument.clone()]
                            }
                            _ => unreachable!(),
                        };
                        async move {
                            (
                                index,
                                instrument,
                                service
                                    .execute_one_observed(single, token, observation)
                                    .await,
                            )
                        }
                    },
                ))
                .buffer_unordered(4);
                let mut rows = Vec::new();
                let mut artifacts = Vec::new();
                while let Some((index, instrument, outcome)) = work.next().await {
                    if cancel.load(Ordering::Acquire) {
                        return Err("CANCELLED".into());
                    }
                    let store = this.artifacts.clone();
                    let previous_rows = rows;
                    let previous_artifacts = artifacts;
                    let (next_rows,next_artifacts,manifest)=this.cpu.run(cancel.clone(),move |_|{
                        let mut rows=previous_rows;let mut artifacts=previous_artifacts;
                    let row=match outcome {Ok(artifact)=>{let result:Value=store.read_evidence_json(&artifact).map_err(|e|e.to_string())?;artifacts.push(artifact.clone());json!({"index":index,"instrument":instrument,"state":"completed","artifact":artifact,"summary":result["summary"],"dataset_hash":result["dataset_hash"],"grid":result["grid"],"trades_cursor":format!("instrument:{index}:0")})},Err(error)=>json!({"index":index,"instrument":instrument,"state":"failed","reason_code":error})};
                    rows.push(row);rows.sort_by_key(|r|r["index"].as_u64().unwrap_or(0));
                    let page=store.publish_evidence(&rows).map_err(|e|e.to_string())?.0;
                    let manifest=store.publish_evidence(&json!({"kind":"research_batch","summary":{"semantics":"independent_instruments","quality":if artifacts.len()==rows.len(){"complete"}else{"partial"},"completed":artifacts.len(),"failed":rows.len()-artifacts.len()},"row_pages":[page],"total_rows":rows.len(),"instrument_artifacts":artifacts,"instruments":rows})).map_err(|e|e.to_string())?.0;

                        Ok((rows,artifacts,manifest))
                    }).await?;
                    rows = next_rows;
                    artifacts = next_artifacts;
                    this.jobs
                        .update_async(hash.clone(), jobs::State::Running, Some(manifest), None)
                        .await?;
                }
                this.jobs
                    .get_async(hash.clone())
                    .await?
                    .artifact
                    .ok_or_else(|| "EMPTY_BATCH".into())
            };
            // Poll observations alongside the existing job future. One bounded
            // snapshot per tick; no detached consumer survives any exit path.
            tokio::pin!(outcome);
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut committed = None;
            let outcome = loop {
                tokio::select! {
                    result = &mut outcome => break result,
                    _ = tick.tick(), if progress.is_some() => {
                        let value = progress.as_ref().unwrap().snapshot();
                        if committed.as_ref() != Some(&value) && candidate_progress::publish(this.jobs.clone(), hash.clone(), value.clone()).await.is_ok() {
                            committed = Some(value);
                        }
                    }
                }
            };
            if let Some(progress) = &progress {
                let _ = candidate_progress::publish(
                    this.jobs.clone(),
                    hash.clone(),
                    progress.snapshot(),
                )
                .await;
            }
            match outcome {
                Ok(a) => {
                    let _ = this
                        .jobs
                        .update_async(hash.clone(), jobs::State::Completed, Some(a), None)
                        .await;
                }
                Err(e) => {
                    let state = if cancel.load(Ordering::Acquire) {
                        jobs::State::Cancelled
                    } else {
                        jobs::State::Failed
                    };
                    let artifact = this
                        .jobs
                        .get_async(hash.clone())
                        .await
                        .ok()
                        .and_then(|j| j.artifact);
                    let _ = this
                        .jobs
                        .update_async(hash.clone(), state, artifact, Some(e))
                        .await;
                }
            }
        });
        Ok(json!(job))
    }
    #[cfg(test)]
    pub(super) async fn execute_one(
        &self,
        request: ResearchRequest,
        cancel: Arc<executor::Cancel>,
    ) -> Result<String, String> {
        self.execute_one_observed(request, cancel, None).await
    }
    async fn execute_one_observed(
        &self,
        request: ResearchRequest,
        cancel: Arc<executor::Cancel>,
        observation: Option<(Arc<candidate_progress::Tracker>, usize)>,
    ) -> Result<String, String> {
        let (instruments, ms, start, end, strategy, variants, costs, mode, seed, is_grid) =
            match request {
                ResearchRequest::Backtest {
                    instruments,
                    timeframe_ms,
                    start_ms,
                    end_ms,
                    strategy_id,
                    parameters,
                    costs,
                    data_mode,
                    seed,
                    ..
                } => (
                    instruments,
                    timeframe_ms,
                    start_ms,
                    end_ms,
                    strategy_id,
                    vec![parameters],
                    costs,
                    data_mode,
                    seed,
                    false,
                ),
                ResearchRequest::Grid {
                    instruments,
                    timeframe_ms,
                    start_ms,
                    end_ms,
                    strategy_id,
                    variants,
                    costs,
                    data_mode,
                    seed,
                    ..
                } => (
                    instruments,
                    timeframe_ms,
                    start_ms,
                    end_ms,
                    strategy_id,
                    variants,
                    costs,
                    data_mode,
                    seed,
                    true,
                ),
                _ => unreachable!(),
            };
        if instruments.len() != 1 {
            return Err("REPLAY_REQUIRES_EXACTLY_ONE_INSTRUMENT".into());
        }
        if !matches!(
            strategy.as_str(),
            "ema_cross_v1"
                | "ema_momentum_confirm_v1"
                | "no_trade"
                | "close_momentum"
                | "two_bar_momentum"
        ) {
            return Err("UNSUPPORTED_STRATEGY".into());
        }
        if matches!(mode, ResearchDataMode::HistoricalObserved) {
            return Err("HISTORICAL_RULES_AND_FEES_UNAVAILABLE".into());
        }
        // K9: accepted research jobs use the existing central public-IP queued
        // admission (bounded to 15s per page). Preview/validation keep their
        // separate fail-fast paths; never replay entire instrument after a
        // local rate defer or bypass the native per-endpoint rate budget.
        let mut bars = tokio::select! {loaded=self.sources.candles(&instruments[0],ms,start,end,true)=>loaded?,_=executor::cancelled(cancel.clone())=>return Err("CANCELLED".into())};
        let expected_start = start.div_ceil(ms).saturating_mul(ms);
        let expected_end = end / ms * ms;
        if bars.first().is_none_or(|b| b.time > expected_start)
            || bars
                .last()
                .is_none_or(|b| b.time.saturating_add(ms) < expected_end)
        {
            return Err("CANDLE_RANGE_INCOMPLETE".into());
        }
        if bars
            .windows(2)
            .any(|pair| pair[1].time != pair[0].time.saturating_add(ms))
        {
            return Err("CANDLE_INTERIOR_GAP".into());
        }
        if bars.len().saturating_mul(512) > 256 * 1024 * 1024 {
            return Err("RESEARCH_MEMORY_LIMIT".into());
        }
        let variants = variants
            .into_iter()
            .map(|p| {
                okx_analysis::operators::EmaParameters {
                    fast: p.fast,
                    slow: p.slow,
                }
                .validate()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let first = *variants.first().ok_or("EMPTY_VARIANTS")?;
        let mechanics = if instruments[0].ends_with("-SWAP") {
            let reference = self
                .sources
                .bootstrap
                .research_current_reference(&instruments[0])
                .await
                .map_err(|e| e.to_string())?;
            if reference.instrument.contract_type.as_deref() != Some("linear") {
                return Err("UNSUPPORTED_INVERSE_CONTRACT".into());
            }
            let multiplier = reference
                .instrument
                .contract_value
                .as_deref()
                .ok_or("CONTRACT_MULTIPLIER_MISSING")?
                .parse::<f64>()
                .map_err(|e| e.to_string())?;
            Mechanics::Linear {
                contract_multiplier: multiplier,
            }
        } else if instruments[0].split('-').count() == 2 {
            Mechanics::Spot
        } else {
            return Err("UNSUPPORTED_INSTRUMENT_TYPE".into());
        };
        if matches!(costs.funding_mode, FundingMode::Observed) {
            let mut owned = (*bars).clone();
            if matches!(mechanics, Mechanics::Spot) {
                for b in &mut owned {
                    b.funding_rate = Some(0.0);
                }
            } else {
                let funding = self.sources.funding(&instruments[0], start, end).await?;
                if funding
                    .first()
                    .is_none_or(|v| v.0 > start.saturating_add(28_800_000))
                    || funding
                        .last()
                        .is_none_or(|v| v.0.saturating_add(28_800_000) < end)
                {
                    return Err("FUNDING_RANGE_INCOMPLETE".into());
                }
                for b in &mut owned {
                    b.funding_rate = Some(
                        funding
                            .iter()
                            .filter(|(t, _)| *t >= b.time && *t < b.time + ms)
                            .map(|(_, r)| r)
                            .sum(),
                    );
                }
            }
            bars = Arc::new(owned);
        }
        let spec = ReplaySpec {
            strategy_id: strategy,
            parameters: first,
            mechanics,
            costs: Costs {
                maker_fee_bps: costs.maker_fee_bps,
                taker_fee_bps: costs.taker_fee_bps,
                slippage_bps: costs.slippage_bps,
                observed_funding: matches!(costs.funding_mode, FundingMode::Observed),
            },
            timeframe_ms: ms,
            historical_observed: matches!(mode, ResearchDataMode::HistoricalObserved),
            historical_rules: false,
            historical_fees: false,
            seed,
            stop_fraction: None,
            take_profit_fraction: None,
        };
        let store = self.artifacts.clone();
        let artifact=self.cpu.run(cancel.clone(),move|c|{
                    let cancelled=||c.load(Ordering::Acquire);
                    let mut chunks=Vec::new();for chunk in bars.chunks(4096){if cancelled(){return Err("CANCELLED".into())}chunks.push(store.publish_evidence(&chunk).map_err(|e|e.to_string())?.0);}
                    let dataset=store.publish_evidence(&json!({"schema":"okx.research.dataset/398.1","instrument":instruments[0],"timeframe_ms":ms,"start_ms":start,"end_ms":end,"chunks":chunks})).map_err(|e|e.to_string())?.0;
                    // Persist the exact inputs and selection policy before any
                    // replay. This is immutable artifact metadata, not another
                    // job catalog, and its ID comes from the existing store.
                    let policy = if is_grid { Some(replay::grid_policy(bars.len(), &variants)?) } else { None };
                    let experiment = store.publish_evidence(&json!({
                        "schema":"okx.research.experiment-inputs/v1",
                        "dataset_artifact":dataset,"instrument":instruments[0],
                        "requested_range":{"start_ms":start,"end_ms":end},
                        "effective_range":{"start_ms":bars.first().map(|b|b.time),"end_ms":bars.last().map(|b|b.time+ms)},
                        "bar_count":bars.len(),"timeframe_ms":ms,
                        "code_version":replay::CAUSAL_REPLAY_VERSION,"spec":spec,
                        "variants":variants,"selection_policy":policy,
                        "result_role":"counterfactual_research_only"
                    })).map_err(|e|e.to_string())?.0;
                    let(result,grid)=if is_grid{let g=replay::grid_with_observer(&bars,&spec,&variants,&cancelled,&|value|{if let Some((tracker,index))=&observation {tracker.observe(*index,value)}})?;(g.test,Some(json!({"selected":g.selected,"split_index":g.split_index,"train_dataset_hash":g.train_dataset_hash,"train_range":{"start_ms":bars[g.train_score_start_index].time,"end_ms":bars[g.split_index].time},"warmup_bars":g.train_score_start_index,"seed":spec.seed,"strategy_id":spec.strategy_id,"test_range":{"start_ms":bars[g.split_index].time,"end_ms":end},"train":g.train,"variants":g.variants,"selection_warning":g.selection_warning})))}else{(replay::replay(&bars,&spec,first.slow,bars.len(),&cancelled)?,None)};
                    publish_replay(&store,&result,&dataset,grid,Some(&experiment),&cancelled)
                }).await?;
        Ok::<_, String>(artifact)
    }
}

use super::*;
use tracing::Instrument;

// A batch consists of independent counterfactual replays. Only net PnL in
// exactly the same quote currency may be added; Sharpe, drawdown, equity and
// cross-currency totals have no defensible aggregate without synchronized
// positions, historical conversion and an explicit portfolio model.
fn independent_replay_currency_totals(rows: &[Value]) -> Result<Value, String> {
    let mut values = Vec::new();
    for row in rows {
        if row["state"] != "completed" {
            continue;
        }
        let currency = row["quote_currency"]
            .as_str()
            .ok_or("REPLAY_CURRENCY_MISSING")?;
        let pnl = row["summary"]["pnl_exact"]
            .as_str()
            .ok_or("REPLAY_EXACT_PNL_MISSING")?;
        values.push(okx_research::economics::QuoteAmount::parse_exact(
            currency, pnl,
        )?);
    }
    let grouped = okx_research::economics::group_quote_by_currency(&values)?;
    let currencies = grouped
        .iter()
        .map(|total| {
            json!({
                "currency":total.currency,
                "net_pnl_exact":total.amount.normalize().to_string(),
                "independent_replay_count":total.contributors
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "basis":"independent_counterfactual_replay_net_pnl",
        "by_quote_currency":currencies,
        "completed_contributors":values.len(),
        "fx_conversion":"not_performed",
        "cross_currency_total":null,
        "portfolio_sharpe":null,
        "portfolio_drawdown":null,
        "portfolio_risk_reason":"JOINT_VALUATION_AND_FX_NOT_AVAILABLE"
    }))
}

fn research_job_versions(request: &ResearchRequest) -> (&'static str, &'static str) {
    let observed_funding = match request {
        ResearchRequest::Backtest { costs, .. } | ResearchRequest::Grid { costs, .. } => {
            matches!(costs.funding_mode, FundingMode::Observed)
        }
        _ => false,
    };
    if observed_funding {
        (
            "analytics/518.3.funding-presence-proof-v1",
            "analytics/semantic/518.3.funding-presence-proof-v1",
        )
    } else {
        ("analytics/448.1", "analytics/semantic/v1")
    }
}

impl AnalyticsService {
    pub(super) async fn submit(&self, request: ResearchRequest) -> Result<Value, String> {
        // Estimate the complete batch before SQL admission or fetching candles.
        // Large requests are rejected before any allocation or source I/O.
        let memory_mib = budget::estimated_mib(&request)?;
        // Preserve old immutable job IDs for unaffected research requests.
        // A funding-observed replay now attributes each settlement to its
        // preceding closed bar, so the same old request must not replay an
        // immutable result with the previous one-bar-shifted economics.
        let (job_version, semantic_version) = research_job_versions(&request);
        let hash =
            okx_research::canonical_sha256(&(job_version, &request)).map_err(|e| e.to_string())?;
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
        let semantic_hash = okx_research::canonical_sha256(&(semantic_version, semantic))
            .map_err(|e| e.to_string())?;
        let key = okx_protocol::research_ipc::IdempotencyKey::new(key).map_err(str::to_owned)?;
        let runner = RunnerGuard::new(self.runners.clone());
        let (job, new, cancel) = self
            .jobs
            .enqueue_semantic_async(key, hash.clone(), semantic_hash)
            .await?;
        let hash = job.id.clone();
        tracing::info!(job_id = %hash, stage = "admission", state = if new { "queued" } else { "replayed" }, "research job admission");
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
                tracing::info!(job_id = %hash, stage = "execution", state = "running", "research job running");
                if matches!(request, ResearchRequest::Benchmark { .. }) {
                    return benchmark::run(
                        this.cpu.clone(),
                        this.artifacts.clone(),
                        cancel.clone(),
                        this.producer.clone(),
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
                    let producer = this.producer.clone();
                    let previous_rows = rows;
                    let previous_artifacts = artifacts;
                    let (next_rows,next_artifacts,manifest)=this.cpu.run(cancel.clone(),move |_|{
                        let mut rows=previous_rows;let mut artifacts=previous_artifacts;
                    let row=match outcome {Ok(artifact)=>{
                        let result:Value=store.read_evidence_json(&artifact).map_err(|e|e.to_string())?;
                        let experiment_id=result["experiment_artifact"].as_str().ok_or("REPLAY_EXPERIMENT_MISSING")?;
                        let experiment:Value=store.read_evidence_json(experiment_id).map_err(|e|e.to_string())?;
                        if experiment["instrument"]!=instrument || experiment["economic_basis"]["instrument"]!=instrument {
                            return Err("REPLAY_ECONOMIC_INSTRUMENT_MISMATCH".into());
                        }
                        let quote=experiment["economic_basis"]["quote_currency"].as_str().ok_or("REPLAY_CURRENCY_MISSING")?;
                        let product=experiment["economic_basis"]["product"].as_str().ok_or("REPLAY_PRODUCT_MISSING")?;
                        // Verify exact money before acknowledging this successful
                        // instrument; malformed evidence must not enter a total.
                        okx_research::economics::QuoteAmount::parse_exact(quote,result["summary"]["pnl_exact"].as_str().ok_or("REPLAY_EXACT_PNL_MISSING")?)?;
                        artifacts.push(artifact.clone());
                        json!({"index":index,"instrument":instrument,"state":"completed","artifact":artifact,
                            "quote_currency":quote,"product":product,"summary":result["summary"],
                            "dataset_hash":result["dataset_hash"],"grid":result["grid"],
                            "trades_cursor":format!("instrument:{index}:0")})
                    },Err(error)=>json!({"index":index,"instrument":instrument,"state":"failed","reason_code":error})};
                    rows.push(row);rows.sort_by_key(|r|r["index"].as_u64().unwrap_or(0));
                    let economics=independent_replay_currency_totals(&rows)?;
                    let page=store.publish_evidence(&rows).map_err(|e|e.to_string())?.0;
                    let manifest=store.publish_evidence(&json!({"kind":"research_batch","schema":"okx.research.batch/518.1","producer":producer,
                        "summary":{"semantics":"independent_instruments","quality":if artifacts.len()==rows.len(){"complete"}else{"partial"},
                        "completed":artifacts.len(),"failed":rows.len()-artifacts.len(),"economic_totals":economics},
                        "row_pages":[page],"total_rows":rows.len(),"instrument_artifacts":artifacts,"instruments":rows})).map_err(|e|e.to_string())?.0;

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
                    if this.jobs.update_async(hash.clone(), jobs::State::Completed, Some(a), None).await.is_ok() {
                        tracing::info!(job_id = %hash, stage = "terminal", state = "completed", "research job completed");
                    }
                }
                Err(e) => {
                    let state = if cancel.load(Ordering::Acquire) {
                        jobs::State::Cancelled
                    } else {
                        jobs::State::Failed
                    };
                    let reason_code = jobs::job_diagnostics::safe_reason(&e);
                    let terminal_state = if state == jobs::State::Cancelled { "cancelled" } else { "failed" };
                    let artifact = this
                        .jobs
                        .get_async(hash.clone())
                        .await
                        .ok()
                        .and_then(|j| j.artifact);
                    if this.jobs.update_async(hash.clone(), state, artifact, Some(e)).await.is_ok() {
                        tracing::info!(job_id = %hash, stage = "terminal", state = terminal_state, reason_code, "research job ended");
                    }
                }
            }
        }.instrument(tracing::Span::current()));
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
                | okx_analysis::operators::BREAKOUT_STRATEGY_ID
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
            .map(|p| p.warmup_for(&strategy).map(|_| p))
            .collect::<Result<Vec<_>, _>>()?;
        let first = *variants.first().ok_or("EMPTY_VARIANTS")?;
        let (mechanics, economic_basis, reference_capture) = if instruments[0].ends_with("-SWAP") {
            let reference = self
                .sources
                .bootstrap
                .research_current_reference(&instruments[0])
                .await
                .map_err(|e| e.to_string())?;
            if reference.instrument.contract_type.as_deref() != Some("linear") {
                return Err("UNSUPPORTED_INVERSE_CONTRACT".into());
            }
            let contract = okx_research::economics::LinearContract::from_reference(
                &instruments[0],
                reference.instrument.contract_type.as_deref(),
                reference.instrument.contract_value.as_deref(),
                reference.instrument.contract_value_currency.as_deref(),
                reference.instrument.settle_currency.as_deref(),
            )?;
            let multiplier = contract.multiplier_f64()?;
            let source_hash = okx_research::canonical_sha256(&(
                "okx.public.reference.raw/v1",
                &reference.raw_body,
            ))
            .map_err(|error| error.to_string())?;
            // Keep the captured bytes; a detached digest alone cannot be
            // independently checked after the original venue response expires.
            let source_evidence = json!({
                "schema": "okx.research.reference-capture/v1",
                "instrument": instruments[0],
                "raw_body": &reference.raw_body,
                "reference_generation": &reference.reference_generation,
                "acquired_at_ms": &reference.acquired_at_ms,
                "contract_type": &reference.instrument.contract_type,
                "contract_value": &reference.instrument.contract_value,
                "contract_value_currency": &reference.instrument.contract_value_currency,
                "settle_currency": &reference.instrument.settle_currency,
            });
            (
                Mechanics::Linear {
                    contract_multiplier: multiplier,
                },
                json!({
                    "product": "linear_swap",
                    "instrument": contract.instrument,
                    "base_asset": contract.base_asset,
                    "quote_currency": contract.quote_currency,
                    "contract_value": contract.base_per_contract.to_string(),
                    "contract_value_unit": "base_asset_per_contract",
                    "source_ct_val": reference.instrument.contract_value,
                    "source_ct_val_ccy": reference.instrument.contract_value_currency,
                    "source_ct_type": reference.instrument.contract_type,
                    "source_settle_currency": reference.instrument.settle_currency,
                    "reference_generation": reference.reference_generation,
                    "reference_acquired_at_ms": reference.acquired_at_ms,
                    "reference_capture_hash": source_hash,
                    "reference_time_basis": "current_at_replay_not_historical",
                    "replay_arithmetic": "f64_approximation",
                    "historical_rules_verified": false,
                    "historical_fees_verified": false,
                }),
                Some(source_evidence),
            )
        } else if instruments[0].split('-').count() == 2 {
            let pair = okx_research::economics::SpotPair::from_instrument(&instruments[0])?;
            (
                Mechanics::Spot,
                json!({
                    "product": "spot",
                    "instrument": pair.instrument,
                    "base_asset": pair.base_asset,
                    "quote_currency": pair.quote_currency,
                    "quantity_unit": "base_asset",
                    "price_unit": "quote_currency_per_base_asset",
                    "reference_time_basis": "pair_name_only_no_reference_capture",
                    "replay_arithmetic": "f64_approximation",
                    "historical_rules_verified": false,
                    "historical_fees_verified": false,
                }),
                None,
            )
        } else {
            return Err("UNSUPPORTED_INSTRUMENT_TYPE".into());
        };
        let mut economic_basis = economic_basis;
        let mut funding_capture = None;
        if matches!(costs.funding_mode, FundingMode::Observed) {
            let mut owned = (*bars).clone();
            if matches!(mechanics, Mechanics::Spot) {
                for b in &mut owned {
                    b.funding_rate = Some(0.0);
                }
                economic_basis["funding_policy"] = json!("not_applicable_spot");
            } else {
                // Include the final *actual closed bar's* settlement, even
                // when the requested right boundary is not grid-aligned.
                // Source reads use a half-open event range.
                let last_close = owned
                    .last()
                    .and_then(|bar| bar.time.checked_add(ms))
                    .ok_or("FUNDING_INVALID_BAR_GRID")?;
                let funding_end = last_close.checked_add(1).ok_or("FUNDING_INVALID_RANGE")?;
                let funding = self
                    .sources
                    .funding(&instruments[0], start, funding_end)
                    .await?;
                let bar_opens = owned.iter().map(|bar| bar.time).collect::<Vec<_>>();
                let allocation = okx_research::economics::allocate_funding_at_bar_close(
                    &bar_opens, ms, &funding,
                )?;
                for (bar, rate) in owned.iter_mut().zip(&allocation.bar_rates) {
                    bar.funding_rate = Some(*rate);
                }
                let capture_hash = okx_research::canonical_sha256(&(
                    "okx.research.realized-funding-events/v1",
                    &instruments[0],
                    start,
                    funding_end,
                    &funding,
                ))
                .map_err(|e| e.to_string())?;
                economic_basis["funding_event_alignment"] = json!({
                    "source":"okx/funding-rate-history",
                    "rate_basis":"realized_settlement_rate",
                    "join_semantics":"funding_event_at_preceding_closed_bar_end_no_forward_fill",
                    "allocation_schema":"okx.research.funding-bar-observation/v1",
                    "absence_semantics":"no_observed_settlement_event_not_observed_zero_rate",
                    "arithmetic_projection":"unobserved_settlement_contributes_zero_cashflow_not_zero_rate_evidence",
                    "valuation_price":"candle_close_proxy_not_venue_mark_price",
                    "observed_events":allocation.observed_events,
                    "attributed_events":allocation.attributed_events,
                    "pre_window_events_ignored":allocation.pre_window_events,
                    "observed_zero_rate_events":allocation.observed_zero_rate_events,
                    "bars_without_settlement":allocation.bars_without_settlement,
                    "first_event_ms":allocation.first_event_ms,
                    "last_event_ms":allocation.last_event_ms,
                    "max_observed_event_gap_ms":allocation.max_observed_gap_ms,
                    "max_allowed_gap_ms":okx_research::economics::MAX_FUNDING_EVENT_GAP_MS,
                    "expected_events":null,
                    "historical_cadence_verified":false,
                    "publication_time_verified":false,
                    "mark_price_verified":false,
                    "normalized_source_capture_hash":capture_hash
                });
                funding_capture = Some(json!({
                    "schema":"okx.research.funding-event-capture/v1",
                    "instrument":instruments[0],
                    "source":"okx/funding-rate-history",
                    "start_ms":start,"end_exclusive_ms":funding_end,
                    "rate_basis":"realized_settlement_rate",
                    "events":funding,
                    "normalized_capture_hash":capture_hash
                }));
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
        let producer = self.producer.clone();
        let artifact=self.cpu.run(cancel.clone(),move|c|{
                    let cancelled=||c.load(Ordering::Acquire);
                    let mut chunks=Vec::new();for chunk in bars.chunks(4096){if cancelled(){return Err("CANCELLED".into())}chunks.push(store.publish_evidence(&chunk).map_err(|e|e.to_string())?.0);}
                    let dataset=store.publish_evidence(&json!({"schema":"okx.research.dataset/398.1","instrument":instruments[0],"timeframe_ms":ms,"start_ms":start,"end_ms":end,"chunks":chunks})).map_err(|e|e.to_string())?.0;
                    // Persist the exact inputs and selection policy before any
                    // replay. This is immutable artifact metadata, not another
                    // job catalog, and its ID comes from the existing store.
                    let policy = if is_grid { Some(replay::grid_policy(bars.len(), &spec.strategy_id, &variants)?) } else { None };
                    let mut economic_basis = economic_basis;
                    if let Some(reference_capture) = reference_capture {
                        let id = store.publish_evidence(&reference_capture).map_err(|e|e.to_string())?.0;
                        economic_basis["reference_capture_artifact"] = json!(id);
                    }
                    if let Some(funding_capture) = funding_capture {
                        let id = store.publish_evidence(&funding_capture).map_err(|e|e.to_string())?.0;
                        economic_basis["funding_capture_artifact"] = json!(id);
                    }
                    let experiment = store.publish_evidence(&json!({
                        "schema":"okx.research.experiment-inputs/v1",
                        "dataset_artifact":dataset,"instrument":instruments[0],
                        "requested_range":{"start_ms":start,"end_ms":end},
                        "effective_range":{"start_ms":bars.first().map(|b|b.time),"end_ms":bars.last().map(|b|b.time+ms)},
                        "bar_count":bars.len(),"timeframe_ms":ms,
                        "code_version":replay::CAUSAL_REPLAY_VERSION,"producer":producer,"spec":spec,
                        "variants":variants,"selection_policy":policy,"economic_basis":economic_basis,
                        "result_role":"counterfactual_research_only"
                    })).map_err(|e|e.to_string())?.0;
                    let(result,grid)=if is_grid{let g=replay::grid_with_observer(&bars,&spec,&variants,&cancelled,&|value|{if let Some((tracker,index))=&observation {tracker.observe(*index,value)}})?;(g.test,Some(json!({"selected":g.selected,"split_index":g.split_index,"train_dataset_hash":g.train_dataset_hash,"train_range":{"start_ms":bars[g.train_score_start_index].time,"end_ms":bars[g.split_index].time},"warmup_bars":g.train_score_start_index,"seed":spec.seed,"strategy_id":spec.strategy_id,"test_range":{"start_ms":bars[g.split_index].time,"end_ms":end},"train":g.train,"variants":g.variants,"selection_warning":g.selection_warning})))}else{(replay::replay(&bars,&spec,first.warmup_for(&spec.strategy_id)?,bars.len(),&cancelled)?,None)};
                    publish_replay(&store,&result,&dataset,grid,Some(&experiment),producer.as_ref(),&cancelled)
                }).await?;
        Ok::<_, String>(artifact)
    }
}

#[cfg(test)]
mod t09_currency_batch_tests {
    use super::*;

    #[test]
    fn t09_t13_funding_settlement_migration_is_scoped_to_observed_research_jobs() {
        let mut body = json!({
            "action":"backtest","idempotency_key":"funding-v2-proof",
            "instruments":["BTC-USDT-SWAP"],"timeframe_ms":60000,
            "start_ms":0,"end_ms":6000000,"strategy_id":"ema_cross_v1",
            "parameters":{"fast":2,"slow":4},
            "costs":{"maker_fee_bps":0,"taker_fee_bps":0,"slippage_bps":0,
                "funding_mode":"zero_assumption"},
            "data_mode":"counterfactual"
        });
        let previous: ResearchRequest = serde_json::from_value(body.clone()).unwrap();
        assert_eq!(
            research_job_versions(&previous),
            ("analytics/448.1", "analytics/semantic/v1")
        );
        body["costs"]["funding_mode"] = json!("observed");
        let migrated: ResearchRequest = serde_json::from_value(body.clone()).unwrap();
        assert_eq!(
            research_job_versions(&migrated),
            (
                "analytics/518.3.funding-presence-proof-v1",
                "analytics/semantic/518.3.funding-presence-proof-v1"
            )
        );
        body["action"] = json!("grid");
        body["variants"] = json!([{"fast":2,"slow":4}]);
        body.as_object_mut().unwrap().remove("parameters");
        let grid: ResearchRequest = serde_json::from_value(body).unwrap();
        assert_eq!(
            research_job_versions(&grid),
            research_job_versions(&migrated)
        );
    }

    #[test]
    fn partial_batch_aggregates_only_completed_exact_same_currency_pnl() {
        let rows = vec![
            json!({"state":"completed","quote_currency":"USDT","summary":{"pnl_exact":"1.25"}}),
            json!({"state":"failed","reason_code":"UNSUPPORTED_INVERSE_CONTRACT"}),
            json!({"state":"completed","quote_currency":"USDT","summary":{"pnl_exact":"-0.25"}}),
        ];
        let result = independent_replay_currency_totals(&rows).unwrap();
        assert_eq!(result["completed_contributors"], 2);
        assert_eq!(result["by_quote_currency"][0]["currency"], "USDT");
        assert_eq!(result["by_quote_currency"][0]["net_pnl_exact"], "1");
        assert_eq!(
            result["by_quote_currency"][0]["independent_replay_count"],
            2
        );
        assert!(result["portfolio_drawdown"].is_null());
        assert!(result["cross_currency_total"].is_null());
        let mut invalid = rows;
        invalid[0]["quote_currency"] = Value::Null;
        assert_eq!(
            independent_replay_currency_totals(&invalid).unwrap_err(),
            "REPLAY_CURRENCY_MISSING"
        );
        invalid[0]["quote_currency"] = json!("USDT");
        invalid[0]["summary"]["pnl_exact"] = json!("invalid");
        assert_eq!(
            independent_replay_currency_totals(&invalid).unwrap_err(),
            "INVALID_EXACT_AMOUNT"
        );
    }
}

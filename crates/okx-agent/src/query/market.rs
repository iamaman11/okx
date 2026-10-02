use std::collections::BTreeMap;

use super::*;

#[derive(serde::Serialize)]
struct MarketIntelligenceResult {
    schema: &'static str,
    as_of: String,
    market_source: &'static str,
    observed_evidence_label: &'static str,
    impact_evidence_label: &'static str,
    sequence_continuity_proven: bool,
    analysis_schema: &'static str,
    coherence: MarketIntelligenceCoherence,
    analysis: okx_analysis::MarketIntelligenceAnalysis,
}

#[derive(serde::Serialize)]
struct MarketIntelligenceCoherence {
    freshness_budget_ms: u64,
    connection_generation: u64,
    readiness_reason: String,
    reference_source_received_at: String,
    oldest_required_receive_ms: u64,
    oldest_required_age_ms: u64,
    exchange_as_of_ms: u64,
    exchange_timestamp_min_ms: u64,
    exchange_timestamp_max_ms: u64,
    exchange_timestamp_skew_ms: u64,
    source_exchange_timestamps_ms: MarketIntelligenceSourceTimestamps,
}

#[derive(serde::Serialize)]
struct MarketIntelligenceSourceTimestamps {
    ticker: u64,
    mark: u64,
    index: u64,
    funding: Option<u64>,
    open_interest: u64,
    order_book: u64,
}

#[derive(serde::Serialize)]
struct MarketResearchResult {
    schema: String,
    assembled_at: String,
    as_of_ms: u64,
    bar: String,
    history_limit: u16,
    feature_versions: MarketResearchFeatureVersions,
    reference: MarketResearchReferenceProvenance,
    instruments: Vec<MarketResearchInstrumentResult>,
    term_structure: Vec<MarketResearchTermStructure>,
}

#[derive(serde::Serialize)]
struct MarketResearchFeatureVersions {
    realized_volatility: &'static str,
    volume_change: &'static str,
    trade_flow: &'static str,
    funding_regime: &'static str,
    dated_future_basis: &'static str,
    term_structure: &'static str,
}

#[derive(serde::Serialize, PartialEq, Eq)]
struct MarketResearchReferenceProvenance {
    generation: String,
}

#[derive(serde::Serialize)]
struct MarketResearchInstrumentResult {
    instrument_id: String,
    mechanics: MarketResearchMechanics,
    market: MarketResearchMarket,
    behavior: MarketResearchBehavior,
    trade_flow: MarketResearchTradeFlow,
    funding_regime: Option<MarketResearchFundingRegime>,
    dated_basis: Option<MarketResearchDatedBasis>,
    provenance: MarketResearchProvenance,
    quality: MarketResearchQuality,
    diagnostics: Vec<MarketResearchDiagnostic>,
}

#[derive(serde::Serialize)]
struct MarketResearchMechanics {
    instrument_type: okx_api::InstrumentType,
    underlying: String,
    contract_value: Option<String>,
    contract_value_currency: Option<String>,
    settle_currency: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expiry_time_ms: Option<String>,
    funding_semantics: &'static str,
}

#[derive(serde::Serialize)]
struct MarketResearchMarket {
    bid: String,
    ask: String,
    last: String,
    mark: String,
    index: String,
    funding_rate: Option<String>,
    open_interest_contracts: String,
    open_interest_usd: Option<String>,
}

#[derive(serde::Serialize)]
struct MarketResearchBehavior {
    confirmed_count: usize,
    total_close_return_ratio: String,
    mean_absolute_close_return_ratio: String,
    max_absolute_close_return_ratio: String,
    max_close_drawdown_ratio: String,
    realized_volatility_ratio: String,
    first_confirmed_volume: String,
    last_confirmed_volume: String,
    volume_change_ratio: Option<String>,
}

#[derive(Clone, serde::Serialize)]
struct MarketResearchTradeFlow {
    trade_count: usize,
    oldest_exchange_timestamp_ms: Option<String>,
    newest_exchange_timestamp_ms: Option<String>,
    buy_contracts: String,
    sell_contracts: String,
    signed_taker_imbalance_ratio: Option<String>,
    vwap: Option<String>,
}

#[derive(serde::Serialize)]
struct MarketResearchFundingRegime {
    event_count: usize,
    oldest_funding_time_ms: Option<String>,
    newest_funding_time_ms: Option<String>,
    realized_rate_coverage_count: usize,
    rate_basis: &'static str,
    latest_rate: Option<String>,
    cumulative_rate_sum: Option<String>,
    mean_rate: Option<String>,
    min_rate: Option<String>,
    max_rate: Option<String>,
    regime: okx_analysis::FundingRegime,
}

#[derive(Clone, serde::Serialize)]
struct MarketResearchDatedBasis {
    expiry_time_ms: u64,
    time_to_expiry_ms: u64,
    basis_bps: String,
    annualized_basis_bps: String,
}

#[derive(serde::Serialize)]
struct MarketResearchTermStructure {
    underlying: String,
    cross_contract: bool,
    points: Vec<MarketResearchTermPoint>,
}

#[derive(serde::Serialize)]
struct MarketResearchTermPoint {
    instrument_id: String,
    expiry_time_ms: u64,
    basis_bps: String,
    annualized_basis_bps: String,
}

#[derive(serde::Serialize)]
struct MarketResearchProvenance {
    market_generation: String,
    market_received_at: String,
    history_generation: String,
    history_oldest_confirmed_open_time_ms: String,
    history_newest_confirmed_open_time_ms: String,
    trades_generation: String,
    trades_received_at: String,
    funding_generation: Option<String>,
    funding_received_at: Option<String>,
    market_source: &'static str,
}

#[derive(serde::Serialize)]
struct MarketResearchQuality {
    market: DataQuality,
    history: DataQuality,
    trades: DataQuality,
    funding: Option<DataQuality>,
}

#[derive(serde::Serialize)]
struct MarketResearchDiagnostic {
    code: &'static str,
    component: &'static str,
}

fn market_intelligence_coherence(
    quality: &PublicQualitySnapshot,
    market: &MarketSnapshot,
    order_book: &okx_observation::OrderBookSnapshot,
    now_ms: u64,
) -> Result<MarketIntelligenceCoherence, String> {
    let parse = |name: &'static str, value: &str| {
        value
            .parse::<u64>()
            .map_err(|_| format!("invalid {name} exchange timestamp"))
    };

    let ticker = parse("ticker", &market.ticker.exchange_timestamp_ms)?;
    let mark = parse("mark", &market.mark_price.exchange_timestamp_ms)?;
    let index = parse("index", &market.index_price.exchange_timestamp_ms)?;
    let funding = market
        .funding
        .as_ref()
        .map(|value| parse("funding", &value.exchange_timestamp_ms))
        .transpose()?;
    let open_interest = parse("open_interest", &market.open_interest.exchange_timestamp_ms)?;
    let order_book = parse(
        "order_book",
        order_book
            .exchange_timestamp_ms
            .as_deref()
            .ok_or_else(|| "missing order_book exchange timestamp".to_owned())?,
    )?;

    let mut timestamps = vec![ticker, mark, index, open_interest, order_book];
    if let Some(funding) = funding {
        timestamps.push(funding);
    }
    let min = *timestamps
        .iter()
        .min()
        .ok_or_else(|| "missing exchange timestamps".to_owned())?;
    let max = *timestamps
        .iter()
        .max()
        .ok_or_else(|| "missing exchange timestamps".to_owned())?;
    let oldest_required_receive_ms = quality
        .oldest_required_receive_ms
        .ok_or_else(|| "FRESH market intelligence is missing receive-age evidence".to_owned())?;
    let oldest_required_age_ms = now_ms.saturating_sub(oldest_required_receive_ms);
    if oldest_required_age_ms > MARKET_INTELLIGENCE_MAX_AGE_MS {
        return Err(format!(
            "FRESH market intelligence exceeded receive-age budget: age={oldest_required_age_ms}ms budget={}ms",
            MARKET_INTELLIGENCE_MAX_AGE_MS
        ));
    }

    Ok(MarketIntelligenceCoherence {
        freshness_budget_ms: MARKET_INTELLIGENCE_MAX_AGE_MS,
        connection_generation: quality.connection_generation,
        readiness_reason: quality.reason.clone(),
        reference_source_received_at: quality.reference_source_received_at.clone(),
        oldest_required_receive_ms,
        oldest_required_age_ms,
        exchange_as_of_ms: max,
        exchange_timestamp_min_ms: min,
        exchange_timestamp_max_ms: max,
        exchange_timestamp_skew_ms: max.saturating_sub(min),
        source_exchange_timestamps_ms: MarketIntelligenceSourceTimestamps {
            ticker,
            mark,
            index,
            funding,
            open_interest,
            order_book,
        },
    })
}

fn funding_semantics(requirement: okx_observation::FundingRequirement) -> &'static str {
    match requirement {
        okx_observation::FundingRequirement::Required => "required",
        okx_observation::FundingRequirement::NotApplicable => "not_applicable",
        okx_observation::FundingRequirement::Unknown => "unknown",
    }
}

fn research_diagnostics(
    market_source: &'static str,
    history_warnings: &[String],
) -> Vec<MarketResearchDiagnostic> {
    let mut diagnostics = Vec::with_capacity(2);
    match market_source {
        "rest_fallback" => diagnostics.push(MarketResearchDiagnostic {
            code: "REST_FALLBACK",
            component: "market",
        }),
        "rest_bootstrap" => diagnostics.push(MarketResearchDiagnostic {
            code: "REST_BOOTSTRAP",
            component: "market",
        }),
        _ => {}
    }
    if !history_warnings.is_empty() {
        diagnostics.push(MarketResearchDiagnostic {
            code: "UNCONFIRMED_LAST_CANDLE",
            component: "history",
        });
    }
    diagnostics
}

const fn research_quality(market: DataQuality, history: DataQuality) -> DataQuality {
    if matches!(market, DataQuality::Fresh) && matches!(history, DataQuality::Fresh) {
        DataQuality::Fresh
    } else {
        DataQuality::Degraded
    }
}

pub(super) async fn dispatch(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
) -> AgentResult<AgentResponse> {
    match &request.operation {
        AgentOperation::MarketSnapshot { instrument } => {
            match assemble_current_market(request, context, generated_at, instrument).await? {
                CurrentMarketAssembly::Ready(assembled) => {
                    let assembled = *assembled;
                    Ok(AgentResponse {
                        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                        request_id: request.request_id.clone(),
                        status: AgentResponseStatus::Completed,
                        generated_at: generated_at.to_owned(),
                        quality: assembled.quality,
                        result_schema: Some(MARKET_SNAPSHOT_SCHEMA_V1.to_owned()),
                        result: Some(serde_json::to_value(assembled.snapshot)?),
                        failure: None,
                        warnings: assembled.warnings,
                    })
                }
                CurrentMarketAssembly::Response(response) => Ok(*response),
                CurrentMarketAssembly::Unavailable => Ok(unavailable(request, generated_at)),
            }
        }
        AgentOperation::InstrumentRules { instrument } => {
            if let Some(public_ws) = context.public_ws {
                if let Some(result) = public_ws.instrument_rules(instrument).await {
                    return Ok(AgentResponse {
                        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                        request_id: request.request_id.clone(),
                        status: AgentResponseStatus::Completed,
                        generated_at: generated_at.to_owned(),
                        quality: DataQuality::Degraded,
                        result_schema: Some(INSTRUMENT_RULES_SCHEMA_V1.to_owned()),
                        result: Some(serde_json::to_value(result)?),
                        failure: None,
                        warnings: vec![REFERENCE_RUNTIME_WARNING.to_owned()],
                    });
                }
                return Ok(reference_not_found(request, generated_at, instrument));
            }

            let Some(reference) = context.standalone_reference else {
                return Ok(unavailable(request, generated_at));
            };

            if let Some(result) = reference.instrument_rules(instrument) {
                return Ok(AgentResponse {
                    schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                    request_id: request.request_id.clone(),
                    status: AgentResponseStatus::Completed,
                    generated_at: generated_at.to_owned(),
                    quality: DataQuality::Degraded,
                    result_schema: Some(INSTRUMENT_RULES_SCHEMA_V1.to_owned()),
                    result: Some(serde_json::to_value(result)?),
                    failure: None,
                    warnings: vec![REFERENCE_BOOTSTRAP_WARNING.to_owned()],
                });
            }

            Ok(reference_not_found(request, generated_at, instrument))
        }
        AgentOperation::FindInstruments {
            asset,
            settle_currency,
            instrument_type,
        } => {
            let reference = if let Some(public_ws) = context.public_ws {
                public_ws.reference_snapshot().await
            } else if let Some(reference) = context.standalone_reference {
                reference.clone()
            } else {
                return Ok(unavailable(request, generated_at));
            };

            let instrument_type = instrument_type.map(|kind| match kind {
                InstrumentTypeFilter::Swap => okx_api::InstrumentType::Swap,
                InstrumentTypeFilter::Futures => okx_api::InstrumentType::Futures,
            });
            let result =
                reference.find_instruments(asset, settle_currency.as_deref(), instrument_type);
            Ok(AgentResponse {
                schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                request_id: request.request_id.clone(),
                status: AgentResponseStatus::Completed,
                generated_at: generated_at.to_owned(),
                quality: DataQuality::Degraded,
                result_schema: Some(INSTRUMENT_SEARCH_SCHEMA_V1.to_owned()),
                result: Some(serde_json::to_value(result)?),
                failure: None,
                warnings: vec![REFERENCE_RUNTIME_WARNING.to_owned()],
            })
        }
        AgentOperation::MarketOverview { instrument } => {
            let Some(public_ws) = context.public_ws else {
                return Ok(unavailable(request, generated_at));
            };
            let Some(instrument_rules) = public_ws.instrument_rules(instrument).await else {
                return Ok(reference_not_found(request, generated_at, instrument));
            };

            public_ws.demand_instrument(instrument.clone()).await?;
            let now_ms = utc_now_ms();
            let quality = public_ws
                .quality_snapshot(instrument, now_ms, PUBLIC_MARKET_MAX_AGE_MS, true)
                .await?;

            if instrument_rules.reference_generation != quality.reference_generation {
                return Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Failed,
                    MARKET_OVERVIEW_INCONSISTENT_CODE,
                    "reference generation changed while building market overview".to_owned(),
                    true,
                ));
            }

            if quality.quality == MarketReadiness::Fresh {
                let live = public_ws
                    .fresh_snapshot(
                        instrument,
                        now_ms,
                        PUBLIC_MARKET_MAX_AGE_MS,
                        generated_at.to_owned(),
                    )
                    .await?;
                if live.market.reference_generation != quality.reference_generation {
                    return Ok(failure_response(
                        request,
                        generated_at,
                        AgentResponseStatus::Failed,
                        MARKET_OVERVIEW_INCONSISTENT_CODE,
                        "market snapshot references a different ReferenceRegistry generation"
                            .to_owned(),
                        true,
                    ));
                }

                let result = MarketOverviewResult {
                    instrument_rules,
                    market: live.market,
                    quality,
                    market_source: "websocket",
                };
                return Ok(AgentResponse {
                    schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                    request_id: request.request_id.clone(),
                    status: AgentResponseStatus::Completed,
                    generated_at: generated_at.to_owned(),
                    quality: DataQuality::Fresh,
                    result_schema: Some(MARKET_OVERVIEW_SCHEMA_V1.to_owned()),
                    result: Some(serde_json::to_value(result)?),
                    failure: None,
                    warnings: Vec::new(),
                });
            }

            let Some(market) = context.market_fallback else {
                return Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Failed,
                    MARKET_PUBLIC_API_UNAVAILABLE_CODE,
                    format!(
                        "persistent WebSocket state is not FRESH: {}",
                        quality.reason
                    ),
                    true,
                ));
            };
            let reference = public_ws.reference_snapshot().await;
            if reference.generation().as_str() != quality.reference_generation
                || instrument_rules.reference_generation != quality.reference_generation
            {
                return Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Failed,
                    MARKET_OVERVIEW_INCONSISTENT_CODE,
                    "reference generation changed before REST fallback".to_owned(),
                    true,
                ));
            }

            match market.snapshot(&reference, instrument).await {
                Ok(snapshot) => {
                    if snapshot.reference_generation != quality.reference_generation {
                        return Ok(failure_response(
                            request,
                            generated_at,
                            AgentResponseStatus::Failed,
                            MARKET_OVERVIEW_INCONSISTENT_CODE,
                            "REST fallback references a different ReferenceRegistry generation"
                                .to_owned(),
                            true,
                        ));
                    }
                    let reason = quality.reason.clone();
                    let result = MarketOverviewResult {
                        instrument_rules,
                        market: snapshot,
                        quality,
                        market_source: "rest_fallback",
                    };
                    Ok(AgentResponse {
                        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                        request_id: request.request_id.clone(),
                        status: AgentResponseStatus::Completed,
                        generated_at: generated_at.to_owned(),
                        quality: DataQuality::Degraded,
                        result_schema: Some(MARKET_OVERVIEW_SCHEMA_V1.to_owned()),
                        result: Some(serde_json::to_value(result)?),
                        failure: None,
                        warnings: vec![format!(
                            "persistent WebSocket state is not FRESH ({reason}); returned bounded public REST fallback"
                        )],
                    })
                }
                Err(error) => Ok(market_failure(request, generated_at, error)),
            }
        }
        AgentOperation::MarketIntelligence {
            instrument,
            impact_contracts,
            depth_levels,
        } => {
            let Some(public_ws) = context.public_ws else {
                return Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Failed,
                    MARKET_INTELLIGENCE_NOT_READY_CODE,
                    "market intelligence requires the live public WebSocket owner".to_owned(),
                    true,
                ));
            };
            let Some(rules) = public_ws.instrument_rules(instrument).await else {
                return Ok(reference_not_found(request, generated_at, instrument));
            };

            public_ws.demand_instrument(instrument.clone()).await?;
            let now_ms = utc_now_ms();
            let quality = public_ws
                .quality_snapshot(instrument, now_ms, MARKET_INTELLIGENCE_MAX_AGE_MS, false)
                .await?;
            if quality.quality != MarketReadiness::Fresh {
                return Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Failed,
                    MARKET_INTELLIGENCE_NOT_READY_CODE,
                    format!(
                        "sequence-contiguous FRESH WebSocket market intelligence is not ready: {}",
                        quality.reason
                    ),
                    true,
                ));
            }

            let live = public_ws
                .fresh_snapshot(
                    instrument,
                    now_ms,
                    MARKET_INTELLIGENCE_MAX_AGE_MS,
                    generated_at.to_owned(),
                )
                .await?;
            if rules.reference_generation != live.market.reference_generation
                || quality.reference_generation != live.market.reference_generation
                || live.order_book.generation != quality.connection_generation
            {
                return Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Failed,
                    MARKET_INTELLIGENCE_INCONSISTENT_CODE,
                    "reference/market/order-book generation changed while building market intelligence"
                        .to_owned(),
                    true,
                ));
            }

            let coherence = match market_intelligence_coherence(
                &quality,
                &live.market,
                &live.order_book,
                now_ms,
            ) {
                Ok(value) => value,
                Err(message) => {
                    return Ok(failure_response(
                        request,
                        generated_at,
                        AgentResponseStatus::Failed,
                        MARKET_INTELLIGENCE_INCONSISTENT_CODE,
                        message,
                        true,
                    ));
                }
            };

            let analysis = match analyze_market_intelligence(
                &rules.instrument.instrument_id,
                &rules.reference_generation,
                &live.market,
                &live.order_book,
                impact_contracts,
                *depth_levels,
            ) {
                Ok(value) => value,
                Err(error) => {
                    return Ok(analysis_failure(
                        request,
                        generated_at,
                        AgentResponseStatus::Failed,
                        error,
                    ));
                }
            };

            let result = MarketIntelligenceResult {
                schema: MARKET_INTELLIGENCE_SCHEMA_V1,
                as_of: generated_at.to_owned(),
                market_source: "websocket",
                observed_evidence_label: "OBSERVED",
                impact_evidence_label: "MODELLED",
                sequence_continuity_proven: quality.sequence_continuity_proven,
                analysis_schema: MARKET_INTELLIGENCE_ANALYSIS_SCHEMA_V1,
                coherence,
                analysis,
            };
            Ok(AgentResponse {
                schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                request_id: request.request_id.clone(),
                status: AgentResponseStatus::Completed,
                generated_at: generated_at.to_owned(),
                quality: DataQuality::Fresh,
                result_schema: Some(MARKET_INTELLIGENCE_SCHEMA_V1.to_owned()),
                result: Some(serde_json::to_value(result)?),
                failure: None,
                warnings: vec![
                    "impact is a deterministic sweep over the current observed book and is MODELLED, not a promised or observed fill".to_owned(),
                ],
            })
        }
        AgentOperation::MarketResearch {
            instruments,
            bar,
            limit,
        } => {
            let history_limit = limit.unwrap_or(100);
            let research_as_of_ms = utc_now_ms();
            let mut results = Vec::with_capacity(instruments.len());
            let mut response_quality = DataQuality::Fresh;
            let mut shared_reference = None;
            let mut term_points = BTreeMap::<String, Vec<MarketResearchTermPoint>>::new();

            for instrument in instruments {
                let current =
                    match assemble_current_market(request, context, generated_at, instrument)
                        .await?
                    {
                        CurrentMarketAssembly::Ready(value) => *value,
                        CurrentMarketAssembly::Response(response) => return Ok(*response),
                        CurrentMarketAssembly::Unavailable => {
                            return Ok(unavailable(request, generated_at));
                        }
                    };

                let history =
                    match assemble_market_history(context, instrument, bar, history_limit).await {
                        Ok(Some(value)) => value,
                        Ok(None) => return Ok(unavailable(request, generated_at)),
                        Err(error) => return Ok(market_failure(request, generated_at, error)),
                    };
                let trades =
                    match assemble_recent_trades(context, instrument, history_limit).await {
                        Ok(Some(value)) => value,
                        Ok(None) => return Ok(unavailable(request, generated_at)),
                        Err(error) => return Ok(market_failure(request, generated_at, error)),
                    };
                let funding_history =
                    if current.rules.instrument.funding_requirement
                        == okx_observation::FundingRequirement::Required
                    {
                        match assemble_funding_history(context, instrument, history_limit).await {
                            Ok(Some(value)) => Some(value),
                            Ok(None) => return Ok(unavailable(request, generated_at)),
                            Err(error) => return Ok(market_failure(request, generated_at, error)),
                        }
                    } else {
                        None
                    };

                if current.rules.reference_generation != history.snapshot.reference_generation
                    || current.snapshot.reference_generation
                        != history.snapshot.reference_generation
                    || trades.snapshot.reference_generation
                        != history.snapshot.reference_generation
                    || funding_history.as_ref().is_some_and(|funding| {
                        funding.snapshot.reference_generation
                            != history.snapshot.reference_generation
                    })
                {
                    return Ok(failure_response(
                        request,
                        generated_at,
                        AgentResponseStatus::Failed,
                        MARKET_RESEARCH_INCONSISTENT_CODE,
                        format!(
                            "reference generation changed while assembling market research for '{instrument}'"
                        ),
                        true,
                    ));
                }

                let reference = MarketResearchReferenceProvenance {
                    generation: current.rules.reference_generation.clone(),
                };
                match &shared_reference {
                    Some(existing) if existing != &reference => {
                        return Ok(failure_response(
                            request,
                            generated_at,
                            AgentResponseStatus::Failed,
                            MARKET_RESEARCH_INCONSISTENT_CODE,
                            format!(
                                "reference provenance changed between instruments while assembling market research for '{instrument}'"
                            ),
                            true,
                        ));
                    }
                    None => shared_reference = Some(reference),
                    _ => {}
                }

                let history_behavior = match analyze_history_behavior(&history.snapshot) {
                    Ok(value) => value,
                    Err(error) => {
                        return Ok(analysis_failure(
                            request,
                            generated_at,
                            AgentResponseStatus::Failed,
                            error,
                        ));
                    }
                };
                let trade_flow = match okx_analysis::analyze_trade_flow(&trades.snapshot) {
                    Ok(value) => value,
                    Err(error) => {
                        return Ok(analysis_failure(
                            request,
                            generated_at,
                            AgentResponseStatus::Failed,
                            error,
                        ));
                    }
                };
                let funding_regime = match funding_history.as_ref() {
                    Some(funding) => match okx_analysis::analyze_funding_regime(&funding.snapshot) {
                        Ok(value) => Some(value),
                        Err(error) => {
                            return Ok(analysis_failure(
                                request,
                                generated_at,
                                AgentResponseStatus::Failed,
                                error,
                            ));
                        }
                    },
                    None => None,
                };

                let quality = research_quality(current.quality, history.quality);
                if !matches!(quality, DataQuality::Fresh) {
                    response_quality = DataQuality::Degraded;
                }
                let diagnostics = research_diagnostics(current.source, &history.warnings);
                let ordinary_dated_future =
                    current.rules.instrument.instrument_type == okx_api::InstrumentType::Futures
                        && current.rules.instrument.funding_requirement
                            == okx_observation::FundingRequirement::NotApplicable;
                let dated_basis = if ordinary_dated_future {
                    match current.rules.instrument.expiry_time_ms.as_deref() {
                        Some(expiry_time_ms) => {
                            let analysis = match analyze_dated_future_basis(
                                &current.snapshot.mark_price.price,
                                &current.snapshot.index_price.price,
                                research_as_of_ms,
                                expiry_time_ms,
                            ) {
                                Ok(value) => value,
                                Err(error) => {
                                    return Ok(analysis_failure(
                                        request,
                                        generated_at,
                                        AgentResponseStatus::Failed,
                                        error,
                                    ));
                                }
                            };
                            let compact = MarketResearchDatedBasis {
                                expiry_time_ms: analysis.expiry_time_ms,
                                time_to_expiry_ms: analysis.time_to_expiry_ms,
                                basis_bps: analysis.basis_bps.clone(),
                                annualized_basis_bps: analysis.annualized_basis_bps.clone(),
                            };
                            term_points
                                .entry(current.snapshot.underlying.clone())
                                .or_default()
                                .push(MarketResearchTermPoint {
                                    instrument_id: instrument.clone(),
                                    expiry_time_ms: analysis.expiry_time_ms,
                                    basis_bps: analysis.basis_bps,
                                    annualized_basis_bps: analysis.annualized_basis_bps,
                                });
                            Some(compact)
                        }
                        None => None,
                    }
                } else {
                    None
                };

                results.push(MarketResearchInstrumentResult {
                    instrument_id: instrument.clone(),
                    mechanics: MarketResearchMechanics {
                        instrument_type: current.rules.instrument.instrument_type,
                        underlying: current.snapshot.underlying.clone(),
                        contract_value: current.rules.instrument.contract_value.clone(),
                        contract_value_currency: current
                            .rules
                            .instrument
                            .contract_value_currency
                            .clone(),
                        settle_currency: current.rules.instrument.settle_currency.clone(),
                        expiry_time_ms: current.rules.instrument.expiry_time_ms.clone(),
                        funding_semantics: funding_semantics(
                            current.rules.instrument.funding_requirement,
                        ),
                    },
                    market: MarketResearchMarket {
                        bid: current.snapshot.ticker.best_bid.clone(),
                        ask: current.snapshot.ticker.best_ask.clone(),
                        last: current.snapshot.ticker.last.clone(),
                        mark: current.snapshot.mark_price.price.clone(),
                        index: current.snapshot.index_price.price.clone(),
                        funding_rate: current
                            .snapshot
                            .funding
                            .as_ref()
                            .map(|funding| funding.rate.clone()),
                        open_interest_contracts: current.snapshot.open_interest.contracts.clone(),
                        open_interest_usd: current.snapshot.open_interest.usd.clone(),
                    },
                    behavior: MarketResearchBehavior {
                        confirmed_count: history_behavior.confirmed_candle_count,
                        total_close_return_ratio: history_behavior.total_close_return_ratio.clone(),
                        mean_absolute_close_return_ratio: history_behavior
                            .mean_absolute_close_return_ratio
                            .clone(),
                        max_absolute_close_return_ratio: history_behavior
                            .max_absolute_close_return_ratio
                            .clone(),
                        max_close_drawdown_ratio: history_behavior.max_close_drawdown_ratio.clone(),
                        realized_volatility_ratio: history_behavior
                            .realized_volatility_ratio
                            .clone(),
                        first_confirmed_volume: history_behavior.first_confirmed_volume.clone(),
                        last_confirmed_volume: history_behavior.last_confirmed_volume.clone(),
                        volume_change_ratio: history_behavior.volume_change_ratio.clone(),
                    },
                    trade_flow: MarketResearchTradeFlow {
                        trade_count: trade_flow.trade_count,
                        oldest_exchange_timestamp_ms: trade_flow.oldest_exchange_timestamp_ms,
                        newest_exchange_timestamp_ms: trade_flow.newest_exchange_timestamp_ms,
                        buy_contracts: trade_flow.buy_contracts,
                        sell_contracts: trade_flow.sell_contracts,
                        signed_taker_imbalance_ratio: trade_flow.signed_taker_imbalance_ratio,
                        vwap: trade_flow.vwap,
                    },
                    funding_regime: funding_regime.as_ref().map(|funding| {
                        MarketResearchFundingRegime {
                            event_count: funding.event_count,
                            oldest_funding_time_ms: funding.oldest_funding_time_ms.clone(),
                            newest_funding_time_ms: funding.newest_funding_time_ms.clone(),
                            realized_rate_coverage_count: funding.realized_rate_coverage_count,
                            rate_basis: funding.regime_rate_basis,
                            latest_rate: funding.latest_rate.clone(),
                            cumulative_rate_sum: funding.cumulative_rate_sum.clone(),
                            mean_rate: funding.mean_rate.clone(),
                            min_rate: funding.min_rate.clone(),
                            max_rate: funding.max_rate.clone(),
                            regime: funding.regime,
                        }
                    }),
                    dated_basis,
                    provenance: MarketResearchProvenance {
                        market_generation: current.snapshot.market_generation.clone(),
                        market_received_at: current.snapshot.source_received_at.clone(),
                        history_generation: history_behavior.history_generation.clone(),
                        history_oldest_confirmed_open_time_ms: history_behavior
                            .oldest_confirmed_open_time_ms
                            .clone(),
                        history_newest_confirmed_open_time_ms: history_behavior
                            .newest_confirmed_open_time_ms
                            .clone(),
                        trades_generation: trades.snapshot.trades_generation.clone(),
                        trades_received_at: trades.snapshot.source_received_at.clone(),
                        funding_generation: funding_history
                            .as_ref()
                            .map(|funding| funding.snapshot.funding_generation.clone()),
                        funding_received_at: funding_history
                            .as_ref()
                            .map(|funding| funding.snapshot.source_received_at.clone()),
                        market_source: current.source,
                    },
                    quality: MarketResearchQuality {
                        market: current.quality,
                        history: history.quality,
                        trades: trades.quality,
                        funding: funding_history.as_ref().map(|funding| funding.quality),
                    },
                    diagnostics,
                });
            }

            let mut term_structure = term_points
                .into_iter()
                .map(|(underlying, mut points)| {
                    points.sort_by_key(|point| point.expiry_time_ms);
                    MarketResearchTermStructure {
                        underlying,
                        cross_contract: points.len() >= 2,
                        points,
                    }
                })
                .collect::<Vec<_>>();
            term_structure.sort_by(|left, right| left.underlying.cmp(&right.underlying));

            let result = MarketResearchResult {
                schema: MARKET_RESEARCH_SCHEMA_V3.to_owned(),
                assembled_at: generated_at.to_owned(),
                as_of_ms: research_as_of_ms,
                bar: bar.clone(),
                history_limit,
                feature_versions: MarketResearchFeatureVersions {
                    realized_volatility: "realized_volatility/simple_return_rss/v1",
                    volume_change: "volume_change/confirmed_candle_contract_volume/v1",
                    trade_flow: okx_analysis::TRADE_FLOW_ANALYSIS_SCHEMA_V1,
                    funding_regime: okx_analysis::FUNDING_REGIME_ANALYSIS_SCHEMA_V1,
                    dated_future_basis: DATED_FUTURE_BASIS_SCHEMA_V1,
                    term_structure: "term_structure/dated_futures/v1",
                },
                reference: shared_reference
                    .expect("MarketResearch validation requires at least two instruments"),
                instruments: results,
                term_structure,
            };
            Ok(AgentResponse {
                schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                request_id: request.request_id.clone(),
                status: AgentResponseStatus::Completed,
                generated_at: generated_at.to_owned(),
                quality: response_quality,
                result_schema: Some(MARKET_RESEARCH_SCHEMA_V3.to_owned()),
                result: Some(serde_json::to_value(result)?),
                failure: None,
                warnings: Vec::new(),
            })
        }
        AgentOperation::MarketHistory {
            instrument,
            bar,
            limit,
        } => {
            let assembled =
                match assemble_market_history(context, instrument, bar, limit.unwrap_or(100)).await
                {
                    Ok(Some(value)) => value,
                    Ok(None) => return Ok(unavailable(request, generated_at)),
                    Err(error) => return Ok(market_failure(request, generated_at, error)),
                };
            Ok(AgentResponse {
                schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                request_id: request.request_id.clone(),
                status: AgentResponseStatus::Completed,
                generated_at: generated_at.to_owned(),
                quality: assembled.quality,
                result_schema: Some(MARKET_HISTORY_SCHEMA_V1.to_owned()),
                result: Some(serde_json::to_value(assembled.snapshot)?),
                failure: None,
                warnings: assembled.warnings,
            })
        }
        AgentOperation::HistoryBehavior {
            instrument,
            bar,
            limit,
        } => {
            let assembled =
                match assemble_market_history(context, instrument, bar, limit.unwrap_or(100)).await
                {
                    Ok(Some(value)) => value,
                    Ok(None) => return Ok(unavailable(request, generated_at)),
                    Err(error) => return Ok(market_failure(request, generated_at, error)),
                };
            let result = match analyze_history_behavior(&assembled.snapshot) {
                Ok(value) => value,
                Err(error) => {
                    return Ok(analysis_failure(
                        request,
                        generated_at,
                        AgentResponseStatus::Failed,
                        error,
                    ));
                }
            };
            Ok(AgentResponse {
                schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                request_id: request.request_id.clone(),
                status: AgentResponseStatus::Completed,
                generated_at: generated_at.to_owned(),
                quality: assembled.quality,
                result_schema: Some(HISTORY_BEHAVIOR_SCHEMA_V1.to_owned()),
                result: Some(serde_json::to_value(result)?),
                failure: None,
                warnings: assembled.warnings,
            })
        }
        AgentOperation::SnapshotQuality { instrument } => {
            if let Some(public_ws) = context.public_ws {
                if public_ws.instrument_rules(instrument).await.is_none() {
                    return Ok(reference_not_found(request, generated_at, instrument));
                }
                public_ws.demand_instrument(instrument.clone()).await?;
                let result = public_ws
                    .quality_snapshot(instrument, utc_now_ms(), PUBLIC_MARKET_MAX_AGE_MS, true)
                    .await?;
                return Ok(AgentResponse {
                    schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                    request_id: request.request_id.clone(),
                    status: AgentResponseStatus::Completed,
                    generated_at: generated_at.to_owned(),
                    quality: data_quality(result.quality),
                    result_schema: Some(PUBLIC_SNAPSHOT_QUALITY_SCHEMA_V2.to_owned()),
                    result: Some(serde_json::to_value(result)?),
                    failure: None,
                    warnings: Vec::new(),
                });
            }

            let Some(reference) = context.standalone_reference else {
                return Ok(unavailable(request, generated_at));
            };

            match SnapshotQualityReport::m2(reference, instrument) {
                Ok(result) => Ok(AgentResponse {
                    schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                    request_id: request.request_id.clone(),
                    status: AgentResponseStatus::Completed,
                    generated_at: generated_at.to_owned(),
                    quality: DataQuality::Degraded,
                    result_schema: Some(SNAPSHOT_QUALITY_SCHEMA_V1.to_owned()),
                    result: Some(serde_json::to_value(result)?),
                    failure: None,
                    warnings: vec![MARKET_REST_BOOTSTRAP_WARNING.to_owned()],
                }),
                Err(MarketError::InstrumentNotFound(_)) => {
                    Ok(reference_not_found(request, generated_at, instrument))
                }
                Err(MarketError::InstrumentNotLive(_)) => Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Rejected,
                    MARKET_INSTRUMENT_NOT_LIVE_CODE,
                    format!("instrument '{instrument}' is not live"),
                    false,
                )),
                Err(error) => Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Failed,
                    MARKET_BOOTSTRAP_INCONSISTENT_CODE,
                    error.to_string(),
                    false,
                )),
            }
        }
        _ => unreachable!("query domain dispatcher received unsupported operation"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(index: usize) -> MarketResearchInstrumentResult {
        let instrument_id = format!("ASSET{index:02}-USDT-SWAP");
        MarketResearchInstrumentResult {
            instrument_id,
            mechanics: MarketResearchMechanics {
                instrument_type: okx_api::InstrumentType::Swap,
                underlying: "ASSET-USDT".to_owned(),
                contract_value: Some("0.00000001".to_owned()),
                contract_value_currency: Some("ASSET".to_owned()),
                settle_currency: Some("USDT".to_owned()),
                expiry_time_ms: None,
                funding_semantics: "required",
            },
            market: MarketResearchMarket {
                bid: "12345.12345678901234".to_owned(),
                ask: "12345.12345678901235".to_owned(),
                last: "12345.12345678901234".to_owned(),
                mark: "12345.12345678901233".to_owned(),
                index: "12345.12345678901230".to_owned(),
                funding_rate: Some("0.000123456789012345".to_owned()),
                open_interest_contracts: "1234567890123456789".to_owned(),
                open_interest_usd: Some("1234567890123456789012345".to_owned()),
            },
            behavior: MarketResearchBehavior {
                confirmed_count: 100,
                total_close_return_ratio: "0.123456789012345678901234567890".to_owned(),
                mean_absolute_close_return_ratio: "0.012345678901234567890123456789".to_owned(),
                max_absolute_close_return_ratio: "0.045678901234567890123456789012".to_owned(),
                max_close_drawdown_ratio: "0.078901234567890123456789012345".to_owned(),
                realized_volatility_ratio: "0.098765432109876543210987654321".to_owned(),
                first_confirmed_volume: "123456789012345".to_owned(),
                last_confirmed_volume: "223456789012345".to_owned(),
                volume_change_ratio: Some("0.810000000000000000000000000000".to_owned()),
            },
            trade_flow: MarketResearchTradeFlow {
                trade_count: 100,
                oldest_exchange_timestamp_ms: Some("1790550000000".to_owned()),
                newest_exchange_timestamp_ms: Some("1790553600000".to_owned()),
                buy_contracts: "1234567890123456789".to_owned(),
                sell_contracts: "987654321098765432".to_owned(),
                signed_taker_imbalance_ratio: Some(
                    "0.123456789012345678901234567890".to_owned(),
                ),
                vwap: Some("12345.123456789012345678901234567890".to_owned()),
            },
            funding_regime: Some(MarketResearchFundingRegime {
                event_count: 100,
                oldest_funding_time_ms: Some("1790000000000".to_owned()),
                newest_funding_time_ms: Some("1790553600000".to_owned()),
                realized_rate_coverage_count: 100,
                rate_basis: "realized_rate",
                latest_rate: Some("0.000123456789012345678901234567".to_owned()),
                cumulative_rate_sum: Some("0.00123456789012345678901234567".to_owned()),
                mean_rate: Some("0.000012345678901234567890123456".to_owned()),
                min_rate: Some("-0.000123456789012345678901234567".to_owned()),
                max_rate: Some("0.000223456789012345678901234567".to_owned()),
                regime: okx_analysis::FundingRegime::Mixed,
            }),
            dated_basis: None,
            provenance: MarketResearchProvenance {
                market_generation:
                    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_owned(),
                market_received_at: "2026-09-28T00:00:01.000Z".to_owned(),
                history_generation:
                    "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                        .to_owned(),
                history_oldest_confirmed_open_time_ms: "1790467200000".to_owned(),
                history_newest_confirmed_open_time_ms: "1790553600000".to_owned(),
                trades_generation:
                    "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                        .to_owned(),
                trades_received_at: "2026-09-28T00:00:01.100Z".to_owned(),
                funding_generation: Some(
                    "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        .to_owned(),
                ),
                funding_received_at: Some("2026-09-28T00:00:01.200Z".to_owned()),
                market_source: "websocket",
            },
            quality: MarketResearchQuality {
                market: DataQuality::Degraded,
                history: DataQuality::Degraded,
                trades: DataQuality::Fresh,
                funding: Some(DataQuality::Fresh),
            },
            diagnostics: vec![
                MarketResearchDiagnostic {
                    code: "REST_FALLBACK",
                    component: "market",
                },
                MarketResearchDiagnostic {
                    code: "UNCONFIRMED_LAST_CANDLE",
                    component: "history",
                },
            ],
        }
    }

    fn projected_size(instrument_count: usize) -> usize {
        let result = MarketResearchResult {
            schema: MARKET_RESEARCH_SCHEMA_V3.to_owned(),
            assembled_at: "2026-09-28T00:00:02.000Z".to_owned(),
            as_of_ms: 1_790_553_602_000,
            bar: "1H".to_owned(),
            history_limit: 100,
            feature_versions: MarketResearchFeatureVersions {
                realized_volatility: "realized_volatility/simple_return_rss/v1",
                volume_change: "volume_change/confirmed_candle_contract_volume/v1",
                trade_flow: okx_analysis::TRADE_FLOW_ANALYSIS_SCHEMA_V1,
                funding_regime: okx_analysis::FUNDING_REGIME_ANALYSIS_SCHEMA_V1,
                dated_future_basis: DATED_FUTURE_BASIS_SCHEMA_V1,
                term_structure: "term_structure/dated_futures/v1",
            },
            reference: MarketResearchReferenceProvenance {
                generation:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_owned(),
            },
            instruments: (0..instrument_count).map(fixture).collect(),
            term_structure: Vec::new(),
        };
        let response = AgentResponse {
            schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
            request_id: "req_h1d_market_research_size_fixture_20260928a".to_owned(),
            status: AgentResponseStatus::Completed,
            generated_at: "2026-09-28T00:00:02.000Z".to_owned(),
            quality: DataQuality::Degraded,
            result_schema: Some(MARKET_RESEARCH_SCHEMA_V2.to_owned()),
            result: Some(serde_json::to_value(result).expect("serialize result")),
            failure: None,
            warnings: Vec::new(),
        };
        serde_json::to_vec(&response).expect("serialize").len()
    }

    fn market_intelligence_response_size() -> usize {
        let long_decimal = "12345678901234567890.123456789012345678901234567890".to_owned();
        let sweep = okx_analysis::BookSweepAnalysis {
            requested_contracts: long_decimal.clone(),
            available_contracts: long_decimal.clone(),
            filled_contracts: long_decimal.clone(),
            complete: false,
            vwap: Some(long_decimal.clone()),
            worst_price: Some(long_decimal.clone()),
            impact_bps_from_mid: Some(long_decimal.clone()),
        };
        let result = MarketIntelligenceResult {
            schema: MARKET_INTELLIGENCE_SCHEMA_V1,
            as_of: "2026-10-02T16:49:59.426Z".to_owned(),
            market_source: "websocket",
            observed_evidence_label: "OBSERVED",
            impact_evidence_label: "MODELLED",
            sequence_continuity_proven: true,
            analysis_schema: MARKET_INTELLIGENCE_ANALYSIS_SCHEMA_V1,
            coherence: MarketIntelligenceCoherence {
                freshness_budget_ms: MARKET_INTELLIGENCE_MAX_AGE_MS,
                connection_generation: u64::MAX,
                readiness_reason: "WS_CURRENT_GENERATION_COMPLETE".to_owned(),
                reference_source_received_at: "2026-10-02T16:49:00.000Z".to_owned(),
                oldest_required_receive_ms: 1_790_959_759_363,
                oldest_required_age_ms: MARKET_INTELLIGENCE_MAX_AGE_MS,
                exchange_as_of_ms: 1_790_959_799_999,
                exchange_timestamp_min_ms: 1_790_959_740_000,
                exchange_timestamp_max_ms: 1_790_959_799_999,
                exchange_timestamp_skew_ms: 59_999,
                source_exchange_timestamps_ms: MarketIntelligenceSourceTimestamps {
                    ticker: 1_790_959_799_990,
                    mark: 1_790_959_799_991,
                    index: 1_790_959_799_992,
                    funding: Some(1_790_959_740_000),
                    open_interest: 1_790_959_799_993,
                    order_book: 1_790_959_799_999,
                },
            },
            analysis: okx_analysis::MarketIntelligenceAnalysis {
                schema: MARKET_INTELLIGENCE_ANALYSIS_SCHEMA_V1.to_owned(),
                instrument_id: "ASSET-LONG-INSTRUMENT-ID-USDT-SWAP".to_owned(),
                reference_generation:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_owned(),
                market_generation:
                    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_owned(),
                order_book_generation: u64::MAX,
                order_book_seq_id: i64::MAX,
                order_book_exchange_timestamp_ms: "1790959799999".to_owned(),
                depth_levels: 50,
                best_bid: long_decimal.clone(),
                best_ask: long_decimal.clone(),
                mid_price: long_decimal.clone(),
                spread_price: long_decimal.clone(),
                spread_bps: long_decimal.clone(),
                microprice: long_decimal.clone(),
                depth_imbalance_ratio: long_decimal.clone(),
                bid_depth_contracts: long_decimal.clone(),
                ask_depth_contracts: long_decimal.clone(),
                buy_sweep: sweep.clone(),
                sell_sweep: sweep,
                last_price: long_decimal.clone(),
                mark_price: long_decimal.clone(),
                index_price: long_decimal.clone(),
                mark_index_basis_bps: long_decimal.clone(),
                last_mark_deviation_bps: long_decimal.clone(),
                funding_rate: Some(long_decimal.clone()),
                next_funding_time_ms: Some("1791014400000".to_owned()),
                open_interest_contracts: long_decimal.clone(),
                open_interest_usd: Some(long_decimal),
            },
        };
        let response = AgentResponse {
            schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
            request_id: "req_market_intelligence_size_budget_20261002a".to_owned(),
            status: AgentResponseStatus::Completed,
            generated_at: "2026-10-02T16:49:59.426Z".to_owned(),
            quality: DataQuality::Fresh,
            result_schema: Some(MARKET_INTELLIGENCE_SCHEMA_V1.to_owned()),
            result: Some(serde_json::to_value(result).expect("serialize result")),
            failure: None,
            warnings: vec![
                "impact is a deterministic sweep over the current observed book and is MODELLED, not a promised or observed fill".to_owned(),
            ],
        };
        serde_json::to_vec(&response)
            .expect("serialize response")
            .len()
    }

    #[test]
    fn market_intelligence_stays_inside_standard_fallback_budget() {
        let size = market_intelligence_response_size();
        assert!(
            size <= 16 * 1024,
            "market-intelligence projection is {size} bytes"
        );
        assert!(
            size <= 16 * 1024 - 1024,
            "market-intelligence projection leaves less than 1 KiB headroom: {size} bytes"
        );
    }

    #[test]
    fn compact_projection_stays_within_h1_targets() {
        let three = projected_size(3);
        let eight = projected_size(8);
        assert!(
            three <= 6 * 1024,
            "three-instrument projection is {three} bytes"
        );
        assert!(
            eight <= 12 * 1024,
            "eight-instrument projection is {eight} bytes"
        );
        assert!(
            eight <= 12 * 1024 - 512,
            "eight-instrument conservative fixture leaves less than 512 bytes of headroom: {eight} bytes"
        );
    }

    #[test]
    fn diagnostics_are_structured_and_bounded() {
        let diagnostics = research_diagnostics("rest_fallback", &[String::from("detail")]);
        let value = serde_json::to_value(diagnostics).expect("serialize");
        assert_eq!(value[0]["code"], "REST_FALLBACK");
        assert_eq!(value[0]["component"], "market");
        assert_eq!(value[1]["code"], "UNCONFIRMED_LAST_CANDLE");
        assert_eq!(value[1]["component"], "history");
    }
}

use super::*;

#[derive(serde::Serialize)]
struct MarketResearchResult {
    schema: String,
    bar: String,
    history_limit: u16,
    instruments: Vec<MarketResearchInstrumentResult>,
}

#[derive(serde::Serialize)]
struct MarketResearchInstrumentResult {
    instrument_id: String,
    instrument_rules: InstrumentRulesSnapshot,
    market: MarketSnapshot,
    market_source: &'static str,
    market_quality: DataQuality,
    market_warnings: Vec<String>,
    history_behavior: HistoryBehaviorAnalysis,
    history_quality: DataQuality,
    history_warnings: Vec<String>,
    quality: DataQuality,
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
        AgentOperation::MarketResearch {
            instruments,
            bar,
            limit,
        } => {
            let history_limit = limit.unwrap_or(100);
            let mut results = Vec::with_capacity(instruments.len());
            let mut response_quality = DataQuality::Fresh;
            let mut response_warnings = Vec::new();

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

                if current.rules.reference_generation != history.snapshot.reference_generation
                    || current.snapshot.reference_generation
                        != history.snapshot.reference_generation
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

                let quality = research_quality(current.quality, history.quality);
                if !matches!(quality, DataQuality::Fresh) {
                    response_quality = DataQuality::Degraded;
                }
                response_warnings.extend(
                    current
                        .warnings
                        .iter()
                        .map(|warning| format!("{instrument}: market: {warning}")),
                );
                response_warnings.extend(
                    history
                        .warnings
                        .iter()
                        .map(|warning| format!("{instrument}: history: {warning}")),
                );

                results.push(MarketResearchInstrumentResult {
                    instrument_id: instrument.clone(),
                    instrument_rules: current.rules,
                    market: current.snapshot,
                    market_source: current.source,
                    market_quality: current.quality,
                    market_warnings: current.warnings,
                    history_behavior,
                    history_quality: history.quality,
                    history_warnings: history.warnings,
                    quality,
                });
            }

            let result = MarketResearchResult {
                schema: MARKET_RESEARCH_SCHEMA_V1.to_owned(),
                bar: bar.clone(),
                history_limit,
                instruments: results,
            };
            Ok(AgentResponse {
                schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                request_id: request.request_id.clone(),
                status: AgentResponseStatus::Completed,
                generated_at: generated_at.to_owned(),
                quality: response_quality,
                result_schema: Some(MARKET_RESEARCH_SCHEMA_V1.to_owned()),
                result: Some(serde_json::to_value(result)?),
                failure: None,
                warnings: response_warnings,
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

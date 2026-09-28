use super::*;

#[derive(serde::Serialize)]
struct MarketResearchResult {
    schema: String,
    assembled_at: String,
    bar: String,
    history_limit: u16,
    reference: MarketResearchReferenceProvenance,
    instruments: Vec<MarketResearchInstrumentResult>,
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
    provenance: MarketResearchProvenance,
    quality: MarketResearchQuality,
    diagnostics: Vec<MarketResearchDiagnostic>,
}

#[derive(serde::Serialize)]
struct MarketResearchMechanics {
    instrument_type: okx_api::InstrumentType,
    contract_value: Option<String>,
    contract_value_currency: Option<String>,
    settle_currency: Option<String>,
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
}

#[derive(serde::Serialize)]
struct MarketResearchProvenance {
    market_generation: String,
    ticker_exchange_timestamp_ms: String,
    history_generation: String,
    history_oldest_confirmed_open_time_ms: String,
    history_newest_confirmed_open_time_ms: String,
    market_source: &'static str,
}

#[derive(serde::Serialize)]
struct MarketResearchQuality {
    market: DataQuality,
    history: DataQuality,
}

#[derive(serde::Serialize)]
struct MarketResearchDiagnostic {
    code: &'static str,
    component: &'static str,
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
        AgentOperation::MarketResearch {
            instruments,
            bar,
            limit,
        } => {
            let history_limit = limit.unwrap_or(100);
            let mut results = Vec::with_capacity(instruments.len());
            let mut response_quality = DataQuality::Fresh;
            let mut shared_reference = None;

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

                let quality = research_quality(current.quality, history.quality);
                if !matches!(quality, DataQuality::Fresh) {
                    response_quality = DataQuality::Degraded;
                }
                let diagnostics = research_diagnostics(current.source, &history.warnings);
                results.push(MarketResearchInstrumentResult {
                    instrument_id: instrument.clone(),
                    mechanics: MarketResearchMechanics {
                        instrument_type: current.rules.instrument.instrument_type,
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
                    },
                    provenance: MarketResearchProvenance {
                        market_generation: current.snapshot.market_generation.clone(),
                        ticker_exchange_timestamp_ms: current
                            .snapshot
                            .ticker
                            .exchange_timestamp_ms
                            .clone(),
                        history_generation: history_behavior.history_generation.clone(),
                        history_oldest_confirmed_open_time_ms: history_behavior
                            .oldest_confirmed_open_time_ms
                            .clone(),
                        history_newest_confirmed_open_time_ms: history_behavior
                            .newest_confirmed_open_time_ms
                            .clone(),
                        market_source: current.source,
                    },
                    quality: MarketResearchQuality {
                        market: current.quality,
                        history: history.quality,
                    },
                    diagnostics,
                });
            }

            let result = MarketResearchResult {
                schema: MARKET_RESEARCH_SCHEMA_V2.to_owned(),
                assembled_at: generated_at.to_owned(),
                bar: bar.clone(),
                history_limit,
                reference: shared_reference
                    .expect("MarketResearch validation requires at least two instruments"),
                instruments: results,
            };
            Ok(AgentResponse {
                schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                request_id: request.request_id.clone(),
                status: AgentResponseStatus::Completed,
                generated_at: generated_at.to_owned(),
                quality: response_quality,
                result_schema: Some(MARKET_RESEARCH_SCHEMA_V2.to_owned()),
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
            },
            provenance: MarketResearchProvenance {
                market_generation:
                    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_owned(),
                ticker_exchange_timestamp_ms: "1790553601000".to_owned(),
                history_generation:
                    "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                        .to_owned(),
                history_oldest_confirmed_open_time_ms: "1790467200000".to_owned(),
                history_newest_confirmed_open_time_ms: "1790553600000".to_owned(),
                market_source: "websocket",
            },
            quality: MarketResearchQuality {
                market: DataQuality::Fresh,
                history: DataQuality::Fresh,
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
            schema: MARKET_RESEARCH_SCHEMA_V2.to_owned(),
            assembled_at: "2026-09-28T00:00:02.000Z".to_owned(),
            bar: "1H".to_owned(),
            history_limit: 100,
            reference: MarketResearchReferenceProvenance {
                generation:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_owned(),
            },
            instruments: (0..instrument_count).map(fixture).collect(),
        };
        let response = AgentResponse {
            schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
            request_id: "req_h1d_market_research_size_fixture_20260928a".to_owned(),
            status: AgentResponseStatus::Completed,
            generated_at: "2026-09-28T00:00:02.000Z".to_owned(),
            quality: DataQuality::Fresh,
            result_schema: Some(MARKET_RESEARCH_SCHEMA_V2.to_owned()),
            result: Some(serde_json::to_value(result).expect("serialize result")),
            failure: None,
            warnings: Vec::new(),
        };
        serde_json::to_vec(&response).expect("serialize").len()
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

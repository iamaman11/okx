use super::*;

#[derive(serde::Serialize)]
struct AccountSummaryCoherence {
    account_snapshot_source_received_at: String,
    current_account_as_of_ms: Option<String>,
    history_source_received_at: String,
    history_read_duration_ms: u64,
    private_ws_generation: Option<u64>,
    events_during_history_read: Option<usize>,
    coherent: bool,
}

#[derive(serde::Serialize)]
struct AccountSummaryResult {
    schema: &'static str,
    account_ledger: okx_observation::AccountLedgerSummary,
    reconciliation: Option<okx_execution::AccountLedgerReconciliation>,
    coherence: AccountSummaryCoherence,
}

const PORTFOLIO_RISK_MAX_OBSERVATION_SKEW_MS: u64 = 30_000;

#[derive(serde::Serialize)]
struct PortfolioRiskCoherence {
    account_snapshot_source_received_at: String,
    account_generation: String,
    reference_generation: Option<String>,
    reference_source_received_at: Option<String>,
    reference_instruments: Vec<String>,
    ledger_source_received_at: String,
    oracle_timestamp_ms: String,
    account_oracle_observation_skew_ms: u64,
    ledger_oracle_observation_skew_ms: u64,
    max_observation_skew_ms: u64,
    private_ws_generation: Option<u64>,
    events_during_read: Option<usize>,
    coherent: bool,
}

#[derive(serde::Serialize)]
struct PortfolioRiskResult {
    schema: &'static str,
    as_of: String,
    observed_evidence_label: &'static str,
    modelled_evidence_label: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    counterfactual_evidence_label: Option<&'static str>,
    analysis_schema: &'static str,
    mandate_schema: &'static str,
    policy_schema: &'static str,
    coherence: PortfolioRiskCoherence,
    analysis: okx_analysis::PortfolioRiskAnalysis,
    #[serde(skip_serializing_if = "Option::is_none")]
    statistics: Option<PortfolioStatisticsAnalysis>,
    exchange_oracle: okx_analysis::RiskOracleComparison,
}

pub(super) async fn dispatch(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
) -> AgentResult<AgentResponse> {
    match &request.operation {
        AgentOperation::AccountSnapshot => {
            let assembled = match assemble_account_snapshot(context).await {
                Ok(value) => value,
                Err(error) => return Ok(account_query_failure(request, generated_at, error)),
            };
            Ok(AgentResponse {
                schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                request_id: request.request_id.clone(),
                status: AgentResponseStatus::Completed,
                generated_at: generated_at.to_owned(),
                quality: assembled.quality,
                result_schema: Some(assembled.result_schema.to_owned()),
                result: Some(serde_json::to_value(assembled.snapshot)?),
                failure: None,
                warnings: assembled.warnings,
            })
        }
        AgentOperation::AccountSummary => {
            let assembled = match assemble_account_snapshot(context).await {
                Ok(value) => value,
                Err(error) => return Ok(account_query_failure(request, generated_at, error)),
            };
            let Some(account) = context.account_fallback else {
                return Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Rejected,
                    ACCOUNT_OBSERVER_CREDENTIAL_UNAVAILABLE_CODE,
                    "OKX observer credential is not provisioned in native secret storage"
                        .to_owned(),
                    false,
                ));
            };

            let history_cursor = match context.private_ws {
                Some(private_ws) => private_ws.convergence_cursor().await.ok(),
                None => None,
            };

            let history_started = std::time::Instant::now();
            let facts = match account.ledger_facts(&assembled.snapshot).await {
                Ok(value) => value,
                Err(error) => {
                    return Ok(account_ledger_failure(request, generated_at, error));
                }
            };

            let history_read_duration_ms =
                u64::try_from(history_started.elapsed().as_millis()).unwrap_or(u64::MAX);
            let mut quality = assembled.quality;
            let mut warnings = assembled.warnings;
            if facts
                .summary
                .history_coverage
                .iter()
                .any(|coverage| !coverage.complete_within_bound)
            {
                quality = DataQuality::Degraded;
                warnings.push(
                    "one or more account history surfaces reached the bounded one-page limit; coverage is explicitly truncated"
                        .to_owned(),
                );
            }
            if facts.summary.fill_order_links_unresolved_due_to_truncation > 0 {
                quality = DataQuality::Degraded;
                warnings.push(
                    "some fill-to-order links are unresolved because bounded order history is truncated"
                        .to_owned(),
                );
            }
            if !facts.summary.authority.multi_account_inventory_complete {
                warnings.push(
                    "summary covers only the authenticated account; main + all-subaccounts inventory requires the separate master read credential"
                        .to_owned(),
                );
            }

            let mut coherence = AccountSummaryCoherence {
                account_snapshot_source_received_at: assembled.snapshot.source_received_at.clone(),
                current_account_as_of_ms: facts.summary.current_account_as_of_ms.clone(),
                history_source_received_at: facts.summary.source_received_at.clone(),
                history_read_duration_ms,
                private_ws_generation: assembled.snapshot.private_ws_generation,
                events_during_history_read: None,
                coherent: assembled.quality == DataQuality::Fresh,
            };
            if let (Some(private_ws), Some(cursor)) = (context.private_ws, history_cursor) {
                match private_ws.convergence_window(cursor).await {
                    Ok(window) => {
                        coherence.private_ws_generation = Some(window.generation);
                        coherence.events_during_history_read = Some(window.events.len());
                        if !window.events.is_empty() {
                            coherence.coherent = false;
                            quality = DataQuality::Degraded;
                            warnings.push(format!(
                                "{} private account event(s) arrived while history evidence was read; current account values are not silently joined as a coherent as-of",
                                window.events.len()
                            ));
                        }
                    }
                    Err(error) => {
                        coherence.coherent = false;
                        quality = DataQuality::Degraded;
                        warnings.push(format!(
                            "private account coherence changed while history evidence was read: {error}"
                        ));
                    }
                }
            }

            let reconciliation = match context.execution {
                Some(execution) => match execution.reconcile_account_ledger(&facts).await {
                    Ok(value) => {
                        if !value.consistent {
                            return Ok(failure_response(
                                request,
                                generated_at,
                                AgentResponseStatus::Failed,
                                ACCOUNT_LEDGER_INCONSISTENT_CODE,
                                format!(
                                    "durable execution ledger does not reconcile with bounded exchange evidence: unexpected_orders={}, unexpected_fills={}, identity_mismatches={}",
                                    value.unexpected_exchange_orders_for_non_submitted_intents,
                                    value.unexpected_exchange_fills_for_non_submitted_intents,
                                    value.identity_mismatches
                                ),
                                false,
                            ));
                        }
                        if value.managed_intents_unresolved_in_bounded_exchange_evidence > 0 {
                            quality = DataQuality::Degraded;
                            warnings.push(format!(
                                "{} managed intent(s) are not observable in the bounded exchange order window; no absence claim is made",
                                value.managed_intents_unresolved_in_bounded_exchange_evidence
                            ));
                        }
                        if value.position_attribution_residual_count > 0 {
                            quality = DataQuality::Degraded;
                            warnings.push(
                                "authoritative exchange position contains an unattributed residual relative to managed fills in the bounded history window; it is not credited to a managed strategy"
                                    .to_owned(),
                            );
                        }
                        if value.position_attribution_truncated {
                            quality = DataQuality::Degraded;
                            warnings.push(format!(
                                "position attribution diagnostics were truncated: {} total rows, bounded response includes {}",
                                value.position_attribution_total,
                                value.position_attribution.len()
                            ));
                        }
                        if value.position_attribution_unavailable_events > 0 {
                            quality = DataQuality::Degraded;
                            warnings.push(format!(
                                "{} position/fill event(s) could not be attributed because their position-side semantics are outside the supported hedge long/short projection",
                                value.position_attribution_unavailable_events
                            ));
                        }
                        Some(value)
                    }
                    Err(error) => {
                        return Ok(failure_response(
                            request,
                            generated_at,
                            AgentResponseStatus::Failed,
                            ACCOUNT_LEDGER_INCONSISTENT_CODE,
                            error.to_string(),
                            false,
                        ));
                    }
                },
                None => {
                    quality = DataQuality::Degraded;
                    warnings.push(
                        "durable execution-ledger reconciliation is unavailable because the execution owner is not configured"
                            .to_owned(),
                    );
                    None
                }
            };

            Ok(AgentResponse {
                schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                request_id: request.request_id.clone(),
                status: AgentResponseStatus::Completed,
                generated_at: generated_at.to_owned(),
                quality,
                result_schema: Some(ACCOUNT_SUMMARY_SCHEMA_V1.to_owned()),
                result: Some(serde_json::to_value(AccountSummaryResult {
                    schema: ACCOUNT_SUMMARY_SCHEMA_V1,
                    account_ledger: facts.summary,
                    reconciliation,
                    coherence,
                })?),
                failure: None,
                warnings,
            })
        }
        AgentOperation::TradingCapabilities {
            instrument,
            margin_mode,
        } => {
            let Some(account) = context.account_fallback else {
                return Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Rejected,
                    ACCOUNT_OBSERVER_CREDENTIAL_UNAVAILABLE_CODE,
                    "OKX observer credential is not provisioned in native secret storage"
                        .to_owned(),
                    false,
                ));
            };
            let Some(rules) = resolve_instrument_rules(context, instrument).await else {
                return Ok(reference_not_found(request, generated_at, instrument));
            };

            let margin_mode = match margin_mode {
                okx_protocol::TradingMarginMode::Cross => okx_api::MarginMode::Cross,
                okx_protocol::TradingMarginMode::Isolated => okx_api::MarginMode::Isolated,
            };
            let snapshot = match account.trading_capabilities(&rules, margin_mode).await {
                Ok(value) => value,
                Err(error) => {
                    return Ok(trading_capabilities_failure(request, generated_at, error));
                }
            };
            let mut warnings = snapshot.warnings.clone();
            warnings.push(REFERENCE_RUNTIME_WARNING.to_owned());
            Ok(AgentResponse {
                schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                request_id: request.request_id.clone(),
                status: AgentResponseStatus::Completed,
                generated_at: generated_at.to_owned(),
                quality: DataQuality::Degraded,
                result_schema: Some(TRADING_CAPABILITIES_SCHEMA_V1.to_owned()),
                result: Some(serde_json::to_value(snapshot)?),
                failure: None,
                warnings,
            })
        }
        AgentOperation::PortfolioRisk {
            mandate,
            policy,
            candidate,
            statistics,
        } => {
            let assembled = match assemble_account_snapshot(context).await {
                Ok(value) => value,
                Err(error) => return Ok(account_query_failure(request, generated_at, error)),
            };
            let Some(account) = context.account_fallback else {
                return Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Rejected,
                    ACCOUNT_OBSERVER_CREDENTIAL_UNAVAILABLE_CODE,
                    "OKX observer credential is not provisioned in native secret storage"
                        .to_owned(),
                    false,
                ));
            };

            let mut reference_instruments = mandate.allowed_instruments.clone();
            reference_instruments.extend(policy.allowed_instruments.iter().cloned());
            reference_instruments.extend(
                assembled
                    .snapshot
                    .positions
                    .iter()
                    .filter(|position| position.position != "0")
                    .map(|position| position.instrument_id.clone()),
            );
            if let Some(candidate) = candidate.as_ref() {
                reference_instruments.push(candidate.instrument.clone());
            }
            reference_instruments.sort();
            reference_instruments.dedup();

            let mut reference_generation: Option<String> = None;
            let mut reference_source_received_at: Option<String> = None;
            for instrument in &reference_instruments {
                let Some(rules) = resolve_instrument_rules(context, instrument).await else {
                    return Ok(reference_not_found(request, generated_at, instrument));
                };
                if let Some(existing) = reference_generation.as_deref()
                    && existing != rules.reference_generation
                {
                    return Ok(failure_response(
                        request,
                        generated_at,
                        AgentResponseStatus::Failed,
                        PORTFOLIO_RISK_REFERENCE_INCONSISTENT_CODE,
                        format!(
                            "portfolio risk reference generations differ inside one evaluation: expected {existing}, observed {} for {instrument}",
                            rules.reference_generation
                        ),
                        false,
                    ));
                }
                reference_generation = Some(rules.reference_generation.clone());
                reference_source_received_at = match reference_source_received_at {
                    Some(existing) if existing <= rules.source_received_at => Some(existing),
                    _ => Some(rules.source_received_at.clone()),
                };
            }

            let read_cursor = match context.private_ws {
                Some(private_ws) => private_ws.convergence_cursor().await.ok(),
                None => None,
            };
            let facts = match account.ledger_facts(&assembled.snapshot).await {
                Ok(value) => value,
                Err(error) => return Ok(account_ledger_failure(request, generated_at, error)),
            };
            let oracle = match account.account_position_risk_oracle().await {
                Ok(value) => value,
                Err(error) => return Ok(account_failure(request, generated_at, error)),
            };

            let account_oracle_observation_skew_ms =
                match observation_skew_ms(&assembled.snapshot.source_received_at, &oracle.ts) {
                    Ok(value) => value,
                    Err(error) => {
                        return Ok(failure_response(
                            request,
                            generated_at,
                            AgentResponseStatus::Failed,
                            PORTFOLIO_RISK_SOURCE_TIME_INCONSISTENT_CODE,
                            error,
                            false,
                        ));
                    }
                };
            let ledger_oracle_observation_skew_ms =
                match observation_skew_ms(&facts.summary.source_received_at, &oracle.ts) {
                    Ok(value) => value,
                    Err(error) => {
                        return Ok(failure_response(
                            request,
                            generated_at,
                            AgentResponseStatus::Failed,
                            PORTFOLIO_RISK_SOURCE_TIME_INCONSISTENT_CODE,
                            error,
                            false,
                        ));
                    }
                };
            let observation_skew_within_bound = account_oracle_observation_skew_ms
                <= PORTFOLIO_RISK_MAX_OBSERVATION_SKEW_MS
                && ledger_oracle_observation_skew_ms <= PORTFOLIO_RISK_MAX_OBSERVATION_SKEW_MS;

            let mut quality = assembled.quality;
            let mut warnings = assembled.warnings;
            if !observation_skew_within_bound {
                quality = DataQuality::Degraded;
                warnings.push(format!(
                    "portfolio account/oracle observation skew exceeded {} ms: account_oracle={} ms, ledger_oracle={} ms",
                    PORTFOLIO_RISK_MAX_OBSERVATION_SKEW_MS,
                    account_oracle_observation_skew_ms,
                    ledger_oracle_observation_skew_ms
                ));
            }
            if facts
                .summary
                .history_coverage
                .iter()
                .any(|coverage| !coverage.complete_within_bound)
            {
                quality = DataQuality::Degraded;
                warnings.push(
                    "portfolio risk history evidence reached a bounded account-history limit; daily realized-loss evidence may be incomplete"
                        .to_owned(),
                );
            }

            let mut coherence = PortfolioRiskCoherence {
                account_snapshot_source_received_at: assembled.snapshot.source_received_at.clone(),
                account_generation: assembled.snapshot.account_generation.clone(),
                reference_generation,
                reference_source_received_at,
                reference_instruments,
                ledger_source_received_at: facts.summary.source_received_at.clone(),
                oracle_timestamp_ms: oracle.ts.clone(),
                account_oracle_observation_skew_ms,
                ledger_oracle_observation_skew_ms,
                max_observation_skew_ms: PORTFOLIO_RISK_MAX_OBSERVATION_SKEW_MS,
                private_ws_generation: assembled.snapshot.private_ws_generation,
                events_during_read: None,
                coherent: assembled.quality == DataQuality::Fresh && observation_skew_within_bound,
            };
            if let (Some(private_ws), Some(cursor)) = (context.private_ws, read_cursor) {
                match private_ws.convergence_window(cursor).await {
                    Ok(window) => {
                        coherence.private_ws_generation = Some(window.generation);
                        coherence.events_during_read = Some(window.events.len());
                        if !window.events.is_empty() {
                            coherence.coherent = false;
                            quality = DataQuality::Degraded;
                            warnings.push(format!(
                                "{} private account event(s) arrived while portfolio ledger/oracle evidence was read; inputs are not silently treated as one coherent snapshot",
                                window.events.len()
                            ));
                        }
                    }
                    Err(error) => {
                        coherence.coherent = false;
                        quality = DataQuality::Degraded;
                        warnings.push(format!(
                            "private account coherence changed while portfolio risk evidence was read: {error}"
                        ));
                    }
                }
            }

            if let Some(expected_generation) = coherence.reference_generation.clone() {
                let instruments = coherence.reference_instruments.clone();
                for instrument in instruments {
                    let Some(rules) = resolve_instrument_rules(context, &instrument).await else {
                        return Ok(reference_not_found(request, generated_at, &instrument));
                    };
                    if rules.reference_generation != expected_generation {
                        return Ok(failure_response(
                            request,
                            generated_at,
                            AgentResponseStatus::Failed,
                            PORTFOLIO_RISK_REFERENCE_INCONSISTENT_CODE,
                            format!(
                                "portfolio risk reference generation changed during evaluation: expected {expected_generation}, observed {} for {instrument}",
                                rules.reference_generation
                            ),
                            false,
                        ));
                    }
                }
            }

            let analysis_mandate = TradingMandate {
                schema: TRADING_MANDATE_SCHEMA_V1,
                version: mandate.version.clone(),
                capital_base_usd: mandate.capital_base_usd.clone(),
                decision_horizon_hours: mandate.decision_horizon_hours,
                benchmark: mandate.benchmark.clone(),
                allowed_instruments: mandate.allowed_instruments.clone(),
                max_drawdown_ratio: mandate.max_drawdown_ratio.clone(),
                leverage_ceiling: mandate.leverage_ceiling.clone(),
                minimum_liquidity_notional_usd: mandate.minimum_liquidity_notional_usd.clone(),
                max_turnover_ratio: mandate.max_turnover_ratio.clone(),
            };
            let analysis_policy = HardRiskPolicy {
                schema: HARD_RISK_POLICY_SCHEMA_V1,
                version: policy.version.clone(),
                max_account_gross_notional_usd: policy.max_account_gross_notional_usd.clone(),
                max_instrument_gross_notional_usd: policy.max_instrument_gross_notional_usd.clone(),
                max_margin_utilization_ratio: policy.max_margin_utilization_ratio.clone(),
                max_loss_per_trade_usd: policy.max_loss_per_trade_usd.clone(),
                max_daily_realized_loss_usd: policy.max_daily_realized_loss_usd.clone(),
                max_drawdown_ratio: policy.max_drawdown_ratio.clone(),
                max_leverage: policy.max_leverage.clone(),
                allowed_instruments: policy.allowed_instruments.clone(),
                minimum_quality: match policy.minimum_quality {
                    ProtocolRiskMinimumQuality::Fresh => AnalysisRiskMinimumQuality::Fresh,
                    ProtocolRiskMinimumQuality::Degraded => AnalysisRiskMinimumQuality::Degraded,
                },
                degraded_mode: match policy.degraded_mode {
                    ProtocolRiskDegradedMode::Reject => AnalysisRiskDegradedMode::Reject,
                    ProtocolRiskDegradedMode::AllowReadOnly => {
                        AnalysisRiskDegradedMode::AllowReadOnly
                    }
                },
                correlated_clusters: policy
                    .correlated_clusters
                    .iter()
                    .map(|cluster| CorrelatedClusterLimit {
                        id: cluster.id.clone(),
                        instruments: cluster.instruments.clone(),
                        max_gross_notional_usd: cluster.max_gross_notional_usd.clone(),
                    })
                    .collect(),
            };
            let analysis_candidate = candidate.as_ref().map(|candidate| PortfolioCandidate {
                instrument: candidate.instrument.clone(),
                direction: match candidate.side {
                    PositionSide::Long => PositionDirection::Long,
                    PositionSide::Short => PositionDirection::Short,
                },
                notional_usd: candidate.notional_usd.clone(),
                worst_case_loss_usd: candidate.worst_case_loss_usd.clone(),
                leverage: candidate.leverage.clone(),
            });

            let analysis = match analyze_portfolio_risk(
                &assembled.snapshot,
                &facts.summary,
                analysis_mandate,
                analysis_policy,
                analysis_candidate,
                coherence.coherent && quality == DataQuality::Fresh,
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

            let statistics_analysis = if let Some(statistics_request) = statistics.as_ref() {
                if !coherence.coherent {
                    return Ok(failure_response(
                        request,
                        generated_at,
                        AgentResponseStatus::Failed,
                        PORTFOLIO_RISK_SOURCE_TIME_INCONSISTENT_CODE,
                        "portfolio statistics require a coherent account/ledger/oracle read window"
                            .to_owned(),
                        true,
                    ));
                }

                let exposures = analysis
                    .instrument_exposure
                    .iter()
                    .map(|row| StatisticalExposure {
                        instrument_id: row.key.clone(),
                        signed_notional_usd: row.signed_net_notional_usd.clone(),
                    })
                    .collect::<Vec<_>>();

                let value = if exposures.is_empty() {
                    analyze_portfolio_statistics(
                        &[],
                        &[],
                        statistics_request.parallel_scenario_move_ratio.as_deref(),
                    )
                } else if exposures.len() > 8 {
                    analyze_portfolio_statistics(
                        &exposures,
                        &[],
                        statistics_request.parallel_scenario_move_ratio.as_deref(),
                    )
                } else {
                    let expected_reference = match coherence.reference_generation.as_deref() {
                        Some(value) => value.to_owned(),
                        None => {
                            return Ok(failure_response(
                                request,
                                generated_at,
                                AgentResponseStatus::Failed,
                                PORTFOLIO_RISK_REFERENCE_INCONSISTENT_CODE,
                                "non-flat statistical portfolio is missing reference generation"
                                    .to_owned(),
                                false,
                            ));
                        }
                    };

                    let mut histories = Vec::with_capacity(exposures.len());
                    for exposure in &exposures {
                        let Some(rules) =
                            resolve_instrument_rules(context, &exposure.instrument_id).await
                        else {
                            return Ok(reference_not_found(
                                request,
                                generated_at,
                                &exposure.instrument_id,
                            ));
                        };
                        if rules.reference_generation != expected_reference {
                            return Ok(failure_response(
                                request,
                                generated_at,
                                AgentResponseStatus::Failed,
                                PORTFOLIO_RISK_REFERENCE_INCONSISTENT_CODE,
                                format!(
                                    "portfolio statistics reference generation changed before history acquisition: expected {expected_reference}, observed {} for {}",
                                    rules.reference_generation, exposure.instrument_id
                                ),
                                false,
                            ));
                        }
                        if rules.instrument.contract_type.as_deref() != Some("linear") {
                            return Ok(analysis_failure(
                                request,
                                generated_at,
                                AgentResponseStatus::Failed,
                                AnalysisError::UnsupportedContractMechanics(
                                    rules
                                        .instrument
                                        .contract_type
                                        .clone()
                                        .unwrap_or_else(|| "missing ctType".to_owned()),
                                ),
                            ));
                        }

                        let assembled_history = match assemble_market_history(
                            context,
                            &exposure.instrument_id,
                            &statistics_request.bar,
                            statistics_request.limit,
                        )
                        .await
                        {
                            Ok(Some(value)) => value,
                            Ok(None) => return Ok(unavailable(request, generated_at)),
                            Err(error) => {
                                return Ok(market_failure(request, generated_at, error));
                            }
                        };
                        if assembled_history.snapshot.reference_generation != expected_reference {
                            return Ok(failure_response(
                                request,
                                generated_at,
                                AgentResponseStatus::Failed,
                                PORTFOLIO_RISK_REFERENCE_INCONSISTENT_CODE,
                                format!(
                                    "portfolio statistics history for '{}' references generation {}, expected {}",
                                    exposure.instrument_id,
                                    assembled_history.snapshot.reference_generation,
                                    expected_reference
                                ),
                                false,
                            ));
                        }
                        histories.push(assembled_history.snapshot);
                    }

                    for exposure in &exposures {
                        let Some(rules) =
                            resolve_instrument_rules(context, &exposure.instrument_id).await
                        else {
                            return Ok(reference_not_found(
                                request,
                                generated_at,
                                &exposure.instrument_id,
                            ));
                        };
                        if rules.reference_generation != expected_reference {
                            return Ok(failure_response(
                                request,
                                generated_at,
                                AgentResponseStatus::Failed,
                                PORTFOLIO_RISK_REFERENCE_INCONSISTENT_CODE,
                                format!(
                                    "portfolio statistics reference generation changed during history acquisition: expected {expected_reference}, observed {} for {}",
                                    rules.reference_generation, exposure.instrument_id
                                ),
                                false,
                            ));
                        }
                    }

                    if let (Some(private_ws), Some(cursor)) = (context.private_ws, read_cursor) {
                        match private_ws.convergence_window(cursor).await {
                            Ok(window) if window.events.is_empty() => {}
                            Ok(window) => {
                                return Ok(failure_response(
                                    request,
                                    generated_at,
                                    AgentResponseStatus::Failed,
                                    PORTFOLIO_RISK_SOURCE_TIME_INCONSISTENT_CODE,
                                    format!(
                                        "{} private account event(s) arrived before statistical history acquisition completed",
                                        window.events.len()
                                    ),
                                    true,
                                ));
                            }
                            Err(error) => {
                                return Ok(failure_response(
                                    request,
                                    generated_at,
                                    AgentResponseStatus::Failed,
                                    PORTFOLIO_RISK_SOURCE_TIME_INCONSISTENT_CODE,
                                    format!(
                                        "private account coherence changed before statistical history acquisition completed: {error}"
                                    ),
                                    true,
                                ));
                            }
                        }
                    }

                    analyze_portfolio_statistics(
                        &exposures,
                        &histories,
                        statistics_request.parallel_scenario_move_ratio.as_deref(),
                    )
                };

                match value {
                    Ok(value) => Some(value),
                    Err(error) => {
                        return Ok(analysis_failure(
                            request,
                            generated_at,
                            AgentResponseStatus::Failed,
                            error,
                        ));
                    }
                }
            } else {
                None
            };

            let oracle_notional = oracle
                .positions
                .iter()
                .map(|position| position.notional_usd.as_str())
                .collect::<Vec<_>>();
            let oracle_comparison = match compare_account_position_risk_oracle(
                &analysis,
                &oracle.ts,
                (!oracle.adjusted_equity_usd.trim().is_empty())
                    .then_some(oracle.adjusted_equity_usd.as_str()),
                &oracle_notional,
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
            if coherence.coherent && !oracle_comparison.consistent {
                return Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Failed,
                    PORTFOLIO_RISK_ORACLE_MISMATCH_CODE,
                    format!(
                        "coherent local portfolio risk does not match OKX account-position-risk oracle: gross_residual_usd={}, adjusted_equity_residual_usd={}",
                        oracle_comparison.gross_notional_residual_usd,
                        oracle_comparison
                            .adjusted_equity_residual_usd
                            .as_deref()
                            .unwrap_or("<unavailable>")
                    ),
                    false,
                ));
            }
            if !coherence.coherent && !oracle_comparison.consistent {
                warnings.push(
                    "OKX account-position-risk differs from local account evidence across an incoherent read window; residual is reported but not classified as a correctness failure"
                        .to_owned(),
                );
            }

            let rejected = analysis.policy_decision == okx_analysis::RiskPolicyDecision::Rejected;
            let violation_codes = analysis
                .violations
                .iter()
                .map(|violation| violation.code)
                .collect::<Vec<_>>();
            let result_schema = if statistics_analysis.is_some() {
                PORTFOLIO_RISK_SCHEMA_V4
            } else {
                PORTFOLIO_RISK_SCHEMA_V3
            };
            let result = serde_json::to_value(PortfolioRiskResult {
                schema: result_schema,
                as_of: generated_at.to_owned(),
                observed_evidence_label: "OBSERVED",
                modelled_evidence_label: "MODELLED",
                counterfactual_evidence_label: statistics_analysis.as_ref().and_then(
                    |statistics| {
                        statistics
                            .parallel_scenario
                            .as_ref()
                            .map(|_| "COUNTERFACTUAL")
                    },
                ),
                analysis_schema: PORTFOLIO_RISK_ANALYSIS_SCHEMA_V3,
                mandate_schema: TRADING_MANDATE_SCHEMA_V1,
                policy_schema: HARD_RISK_POLICY_SCHEMA_V1,
                coherence,
                analysis,
                statistics: statistics_analysis,
                exchange_oracle: oracle_comparison,
            })?;

            Ok(AgentResponse {
                schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                request_id: request.request_id.clone(),
                status: if rejected {
                    AgentResponseStatus::Rejected
                } else {
                    AgentResponseStatus::Completed
                },
                generated_at: generated_at.to_owned(),
                quality,
                result_schema: Some(result_schema.to_owned()),
                result: Some(result),
                failure: rejected.then(|| AgentFailure {
                    code: PORTFOLIO_RISK_POLICY_REJECTED_CODE.to_owned(),
                    message: format!(
                        "portfolio hard-risk policy rejected the evaluated state/candidate: {}",
                        violation_codes.join(",")
                    ),
                    retryable: false,
                }),
                warnings,
            })
        }
        _ => unreachable!("query domain dispatcher received unsupported operation"),
    }
}

fn observation_skew_ms(source_received_at: &str, oracle_timestamp_ms: &str) -> Result<u64, String> {
    let source_ms = chrono::DateTime::parse_from_rfc3339(source_received_at)
        .map_err(|error| {
            format!(
                "invalid portfolio source observation timestamp '{source_received_at}': {error}"
            )
        })?
        .timestamp_millis();
    let oracle_ms = oracle_timestamp_ms.parse::<i64>().map_err(|error| {
        format!("invalid account-position-risk oracle timestamp '{oracle_timestamp_ms}': {error}")
    })?;
    Ok(source_ms.abs_diff(oracle_ms))
}

fn account_ledger_failure(
    request: &AgentRequest,
    generated_at: &str,
    error: AccountLedgerBootstrapError,
) -> AgentResponse {
    match error {
        AccountLedgerBootstrapError::PermissionRejected => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            ACCOUNT_OBSERVER_PERMISSION_REJECTED_CODE,
            "OKX observer credential must be strictly read-only".to_owned(),
            false,
        ),
        AccountLedgerBootstrapError::Api(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            ACCOUNT_PRIVATE_API_UNAVAILABLE_CODE,
            error.to_string(),
            true,
        ),
        AccountLedgerBootstrapError::Normalize(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            ACCOUNT_LEDGER_INCONSISTENT_CODE,
            error.to_string(),
            false,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn portfolio_observation_skew_is_explicit_and_bounded_in_milliseconds() {
        assert_eq!(
            observation_skew_ms("2026-10-03T00:00:05.000Z", "1790985600000").expect("skew"),
            5_000
        );
        assert!(observation_skew_ms("not-a-timestamp", "1790985600000").is_err());
        assert!(observation_skew_ms("2026-10-03T00:00:00.000Z", "not-millis").is_err());
    }
}

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
        AgentOperation::PortfolioRisk => {
            let assembled = match assemble_account_snapshot(context).await {
                Ok(value) => value,
                Err(error) => return Ok(account_query_failure(request, generated_at, error)),
            };
            let result = match analyze_account_risk(&assembled.snapshot) {
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
                result_schema: Some(ACCOUNT_RISK_ANALYSIS_SCHEMA_V1.to_owned()),
                result: Some(serde_json::to_value(result)?),
                failure: None,
                warnings: assembled.warnings,
            })
        }
        _ => unreachable!("query domain dispatcher received unsupported operation"),
    }
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

use super::*;

#[derive(serde::Serialize)]
struct AccountSummaryCoherence {
    account_snapshot_source_received_at: String,
    current_account_as_of_ms: Option<String>,
    history_source_received_at: String,
    history_read_duration_ms: u64,
    ledger_history_read_duration_ms: u64,
    protective_algo_read_duration_ms: u64,
    private_ws_generation: Option<u64>,
    events_during_history_read: Option<usize>,
    coherent: bool,
}

#[derive(serde::Serialize)]
struct StrategyOpeningSourcePreview {
    scope: &'static str,
    status: &'static str,
    blockers: Vec<&'static str>,
}

#[derive(serde::Serialize)]
struct AccountSummaryResult {
    schema: &'static str,
    account_ledger: okx_observation::AccountLedgerSummary,
    pending_protective_algos: okx_api::PendingProtectiveAlgoInventory,
    strategy_run_opening_source: StrategyOpeningSourcePreview,
    reconciliation: Option<okx_execution::AccountLedgerReconciliation>,
    managed_execution_intents: Option<okx_execution::ManagedExecutionInventory>,
    coherence: AccountSummaryCoherence,
}

const TRADER_SNAPSHOT_SCHEMA_V1: &str = "okx.trader-snapshot/v1";
const MAX_TRADER_PACKET_BYTES: usize = 12_288;

/// Deliberately *not* a mutation authority. This is one bounded projection of
/// account_summary, after the same source/coherence/ledger reconciliation.
/// It can prove NO_GO, but can NEVER mint a GO from read-only observations.
fn compact_trader_packet(
    full: &serde_json::Value,
    quality: &DataQuality,
    warnings_count: usize,
    source_read_duration_ms: u64,
) -> serde_json::Value {
    use serde_json::{Value, json};

    let get = |p: &str| full.pointer(p).cloned().unwrap_or(Value::Null);
    let has_stop = !get("/reconciliation/strategy_risk_stop").is_null();
    let finalized = !get("/reconciliation/finalized_run").is_null();
    let coherent = get("/coherence/coherent") == json!(true);
    let reconciled = get("/reconciliation/consistent") == json!(true);
    let complete_protection = get("/pending_protective_algos/complete_within_bound") == json!(true);
    let history_complete = full
        .pointer("/account_ledger/history_coverage")
        .and_then(Value::as_array)
        .is_some_and(|rows| {
            !rows.is_empty()
                && rows
                    .iter()
                    .all(|row| row.get("complete_within_bound") == Some(&json!(true)))
        });
    let mut reasons = Vec::new();
    if has_stop {
        reasons.push("DURABLE_STRATEGY_STOP");
    }
    if finalized {
        reasons.push("RUN_FINALIZED");
    }
    if get("/reconciliation/financial_period/state") == json!("INCOMPLETE") {
        reasons.push("STRATEGY_FINANCE_INCOMPLETE");
    }
    if get("/reconciliation/exchange_evidence/source_gap_over_two_hours") == json!(true) {
        reasons.push("FINANCIAL_SOURCE_GAP");
    }
    if get("/reconciliation/strategy_attribution/unallocated_funding_bills")
        .as_u64()
        .is_some_and(|count| count > 0)
    {
        reasons.push("FUNDING_NOT_ATTRIBUTED");
    }
    if !coherent {
        reasons.push("PRIVATE_SOURCE_NOT_COHERENT");
    }
    if !reconciled {
        reasons.push("LEDGER_RECONCILIATION_NOT_PROVEN");
    }
    if !complete_protection || !history_complete {
        reasons.push("BOUNDED_PRIVATE_SOURCE");
    }
    if !matches!(quality, DataQuality::Fresh) {
        reasons.push("SUMMARY_QUALITY_NOT_FRESH");
    }
    let blocked = !reasons.is_empty();
    // Even when no blocker was observed, this read-only packet cannot attest
    // an execution policy, risk budget, exact current venue or new order plan.
    reasons.push("EXECUTOR_RISK_ADMISSION_NOT_PERFORMED");
    let mut compact = json!({
        "schema": TRADER_SNAPSHOT_SCHEMA_V1,
        "decision": {
            "new_risk": if blocked { "NO_GO" } else { "NOT_ATTESTED" },
            "scope": "READ_ONLY_OBSERVER_NOT_TRADE_PERMISSION",
            "reasons": reasons,
            "risk_reducing_exit": "REQUIRES_NATIVE_EXECUTOR_REVALIDATION"
        },
        "account": {
            "uid_fingerprint": get("/account_ledger/authority/account_uid_fingerprint"),
            "scope": "AUTHENTICATED_ACCOUNT_ONLY",
            "account_equity_usd": get("/account_ledger/total_equity_usd"),
            "account_unrealized_pnl": get("/account_ledger/current_unrealized_pnl"),
            "equity_is_strategy_nav": false,
            "positions": get("/account_ledger/open_positions"),
            "pending_orders": get("/account_ledger/pending_orders"),
            "pending_protective_algos": get("/pending_protective_algos/rows"),
            "native_protection_complete": complete_protection,
            "private_history_complete_within_bound": history_complete
        },
        "strategy": {
            "active_run_id": get("/reconciliation/financial_period/run_id"),
            "period_state": get("/reconciliation/financial_period/state"),
            "current_nav_usdt": get("/reconciliation/financial_period/nav_usdt"),
            "finalized_run_id": get("/reconciliation/finalized_run/run_id"),
            "finalized_nav_usdt": get("/reconciliation/finalized_run/nav_usdt"),
            "finalized_realized_usdt": get("/reconciliation/finalized_run/realized_usdt"),
            "finalized_fees_usdt": get("/reconciliation/finalized_run/fees_usdt"),
            "finalized_checksum_sha256": get("/reconciliation/finalized_run/checksum_sha256"),
            "durable_stop": get("/reconciliation/strategy_risk_stop"),
            "archived_finalized": get("/reconciliation/archived_runs_count"),
            "archived_unverified": get("/reconciliation/unverified_runs_count"),
            "archive_free_slots": get("/reconciliation/archived_run_slots_remaining")
        },
        "funding": {
            "allocated_to_strategy_usdt": get("/reconciliation/financial_period/funding_usdt"),
            "unallocated_exchange_bills": get("/reconciliation/strategy_attribution/unallocated_funding_bills"),
            "classification": "UNOWNED_UNLESS_EXACT_POSITION_PROOF"
        },
        "evidence": {
            "quality": quality,
            "private_source_coherent": coherent,
            "current_account_asof_ms": get("/coherence/current_account_as_of_ms"),
            "account_source_received_at": get("/coherence/account_snapshot_source_received_at"),
            "history_source_received_at": get("/coherence/history_source_received_at"),
            "source_history_complete": history_complete,
            "source_gap_over_two_hours": get("/reconciliation/exchange_evidence/source_gap_over_two_hours"),
            "source_cursor_complete": get("/reconciliation/exchange_evidence/latest_source_complete"),
            "source_asof_ms": get("/reconciliation/exchange_evidence/latest_source_asof_ms"),
            "production_public_market_gate": "NOT_TESTED_BY_ACCOUNT_READ",
            "managed_intents_total": get("/managed_execution_intents/total"),
            "managed_intents_sampled": get("/managed_execution_intents/shown")
        },
        "transport": {
            "full_account_result_bytes": serde_json::to_vec(full).map_or(0, |v| v.len()),
            "native_source_read_ms": source_read_duration_ms,
            "ledger_history_read_ms": get("/coherence/ledger_history_read_duration_ms"),
            "protective_algo_read_ms": get("/coherence/protective_algo_read_duration_ms"),
            "warnings_count": warnings_count,
            "packet_bytes": 0
        }
    });
    // The packet byte count includes its own integer field, not just
    // the pre-metadata JSON. Stabilizes after at most a few digit changes.
    for _ in 0..4 {
        let bytes = serde_json::to_vec(&compact).map_or(usize::MAX, |v| v.len());
        if compact["transport"]["packet_bytes"] == json!(bytes) {
            break;
        }
        compact["transport"]["packet_bytes"] = json!(bytes);
    }
    compact
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
    #[serde(skip_serializing_if = "Option::is_none")]
    virtual_portfolio: Option<VirtualPortfolioProof>,
}

struct VirtualPortfolioProofInput<'a> {
    observer: &'a AccountBootstrapper,
    account_snapshot: &'a okx_observation::AccountSnapshot,
    request: &'a VirtualPortfolioRequest,
    statistics: Option<&'a PortfolioStatisticsRequest>,
    ledger: &'a okx_observation::AccountLedgerSummary,
    mandate: &'a TradingMandate,
    policy: &'a HardRiskPolicy,
    expected_reference: &'a str,
}

#[derive(serde::Serialize)]
struct FuturesVirtualPositionEvidence {
    constraint: VirtualPositionConstraintEvidence,
    reference_generation: String,
    market_generation: String,
    market_source: &'static str,
    market_source_received_at: String,
    mark_exchange_timestamp_ms: String,
}

#[derive(serde::Serialize)]
struct VirtualPortfolioProof {
    schema: &'static str,
    evidence_label: &'static str,
    account_mode: String,
    oracle_scope: &'static str,
    exchange_virtual_portfolio_oracle_available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    exchange_virtual_portfolio_oracle_reason: Option<String>,
    evidence_source_received_at: String,
    request: VirtualPortfolioRequest,
    analysis: okx_analysis::PortfolioRiskAnalysis,
    #[serde(skip_serializing_if = "Option::is_none")]
    statistics: Option<PortfolioStatisticsAnalysis>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    futures_constraints: Vec<FuturesVirtualPositionEvidence>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exchange_oracle: Option<okx_api::PositionBuilderSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    notional_oracle: Option<okx_analysis::VirtualNotionalOracleComparison>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VirtualPortfolioOracleRoute {
    FuturesLocalModel,
    PositionBuilder,
}

fn virtual_portfolio_oracle_route(
    account_level: &str,
) -> Result<VirtualPortfolioOracleRoute, AnalysisError> {
    match account_level {
        "2" => Ok(VirtualPortfolioOracleRoute::FuturesLocalModel),
        "3" | "4" => Ok(VirtualPortfolioOracleRoute::PositionBuilder),
        other => Err(AnalysisError::UnsupportedAccountMode(other.to_owned())),
    }
}

pub(super) async fn dispatch(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
) -> AgentResult<AgentResponse> {
    match &request.operation {
        AgentOperation::LedgerStorageAudit => {
            let Some(execution) = context.execution else {
                return Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Rejected,
                    ACCOUNT_LEDGER_INCONSISTENT_CODE,
                    "native single-owner execution ledger is not configured".to_owned(),
                    false,
                ));
            };
            match execution.attest_ledger_storage().await {
                Ok(proof) => Ok(AgentResponse {
                    schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                    request_id: request.request_id.clone(),
                    status: AgentResponseStatus::Completed,
                    generated_at: generated_at.to_owned(),
                    quality: if proof.on_disk_schema == okx_execution::EXECUTION_LEDGER_SCHEMA_V14 {
                        DataQuality::Fresh
                    } else {
                        DataQuality::Degraded
                    },
                    result_schema: Some(
                        okx_execution::LEDGER_STORAGE_ATTESTATION_SCHEMA_V1.to_owned(),
                    ),
                    result: Some(serde_json::to_value(proof)?),
                    failure: None,
                    warnings: Vec::new(),
                }),
                Err(error) => Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Failed,
                    ACCOUNT_LEDGER_INCONSISTENT_CODE,
                    format!("native on-disk ledger integrity audit failed: {error}"),
                    false,
                )),
            }
        }
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
        AgentOperation::AccountSummary | AgentOperation::TraderSnapshot => {
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

            // Both REST inventories are independent read-only evidence
            // surfaces. Start them together under the SAME private WS
            // convergence cursor, then inspect completion before using
            // either result. No snapshot cache, data fallback or extra owner.
            let history_started = std::time::Instant::now();
            let ledger_read = async {
                let started = std::time::Instant::now();
                let result = account.ledger_facts(&assembled.snapshot).await;
                let duration = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                (result, duration)
            };
            let protective_read = async {
                let started = std::time::Instant::now();
                let result = account.pending_protective_algos().await;
                let duration = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                (result, duration)
            };
            let ((facts_result, ledger_history_read_duration_ms),
                (protection_result, protective_algo_read_duration_ms)) =
                tokio::join!(ledger_read, protective_read);
            let history_read_duration_ms =
                u64::try_from(history_started.elapsed().as_millis()).unwrap_or(u64::MAX);
            let facts = match facts_result {
                Ok(value) => value,
                Err(error) => return Ok(account_ledger_failure(request, generated_at, error)),
            };
            let pending_protective_algos = match protection_result {
                Ok(value) => value,
                Err(error) => return Ok(account_ledger_failure(request, generated_at, error)),
            };
            let mut quality = assembled.quality;
            let mut warnings = assembled.warnings;
            if !pending_protective_algos.complete_within_bound {
                quality = DataQuality::Degraded;
                warnings.push(
                    "pending conditional/OCO protective algo order inventory reached its bounded one-page limit; orphan absence is not proven"
                        .to_owned(),
                );
            }
            if pending_protective_algos.rows > 0 && facts.summary.open_positions == 0 {
                quality = DataQuality::Degraded;
                warnings.push(format!(
                    "{} pending conditional/OCO algo order(s) remain with zero observed positions; flat/orphan-free is NOT proven",
                    pending_protective_algos.rows
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

            // OPEN NAV must be valued against the same single Production-
            // public market as all strategy signals. Demo-private position
            // marks/UPL are execution evidence only, never market-price proof.
            // No REST fallback or simulated feed is accepted for open NAV.
            // Mark reads are inside the existing private WS coherence fence.
            let mut production_marks = std::collections::BTreeMap::new();
            let mut mark_instruments_attempted = std::collections::BTreeSet::new();
            if context
                .market_fallback
                .is_some_and(|market| !market.source_is_simulated())
                && let Some(public_ws) = context.public_ws
            {
                let now_ms = super::utc_now_ms();
                for position in &facts.authoritative_positions {
                    if !position
                        .position
                        .parse::<rust_decimal::Decimal>()
                        .is_ok_and(|qty| qty != rust_decimal::Decimal::ZERO)
                        || !mark_instruments_attempted.insert(position.instrument_id.clone())
                    {
                        continue;
                    }
                    // Bound extra WS queries. Unquoted positions remain
                    // INCOMPLETE, never silently valued from Demo.
                    if mark_instruments_attempted.len() > 8 {
                        break;
                    }
                    let inst = &position.instrument_id;
                    if public_ws.demand_instrument(inst.clone()).await.is_err() {
                        continue;
                    }
                    let quality = public_ws.quality_snapshot(inst, now_ms, 30_000, true).await;
                    if !quality.is_ok_and(|status| status.quality == MarketReadiness::Fresh) {
                        continue;
                    }
                    if let Ok(live) = public_ws
                        .fresh_snapshot(inst, now_ms, 30_000, generated_at.to_owned())
                        .await
                        && let Ok(exchange_timestamp_ms) =
                            live.market.mark_price.exchange_timestamp_ms.parse::<u64>()
                    {
                        production_marks.insert(
                            inst.clone(),
                            okx_execution::ProductionMarkEvidence {
                                price: live.market.mark_price.price,
                                exchange_timestamp_ms,
                                received_at_ms: super::utc_now_ms(),
                            },
                        );
                    }
                }
            }

            let mut coherence = AccountSummaryCoherence {
                account_snapshot_source_received_at: assembled.snapshot.source_received_at.clone(),
                current_account_as_of_ms: facts.summary.current_account_as_of_ms.clone(),
                history_source_received_at: facts.summary.source_received_at.clone(),
                history_read_duration_ms,
                ledger_history_read_duration_ms,
                protective_algo_read_duration_ms,
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

            let (reconciliation, managed_execution_intents) = match context.execution {
                Some(execution) => match execution
                    .reconcile_account_ledger_with_intents(
                        &facts,
                        coherence.coherent,
                        &production_marks,
                    )
                    .await
                {
                    Ok((value, inventory)) => {
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
                        if !inventory.complete_within_bound {
                            quality = DataQuality::Degraded;
                            warnings.push(format!(
                                "managed execution intent identity sample contains {} of {} intents; no complete inventory claim is made",
                                inventory.shown, inventory.total
                            ));
                        }
                        // A durable intent from a different authenticated UID is
                        // never accepted as this account's reconciliation proof.
                        if inventory.rows.iter().any(|item| {
                            item.account_uid_fingerprint
                                != facts.summary.authority.account_uid_fingerprint
                        }) {
                            return Ok(failure_response(
                                request,
                                generated_at,
                                AgentResponseStatus::Failed,
                                ACCOUNT_LEDGER_INCONSISTENT_CODE,
                                "managed execution intent belongs to another authenticated account"
                                    .to_owned(),
                                false,
                            ));
                        }
                        if inventory.total != value.managed_intents {
                            return Ok(failure_response(
                                request,
                                generated_at,
                                AgentResponseStatus::Failed,
                                ACCOUNT_LEDGER_INCONSISTENT_CODE,
                                "managed intent inventory and exchange reconciliation disagree on source ledger count"
                                    .to_owned(),
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
                        (Some(value), Some(inventory))
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
                    (None, None)
                }
            };

            // Same native source predicates as the Demo BEGIN operation,
            // but this is only read-only observer proof, never trading authority.
            let mut opening_blockers = super::execution::strategy_opening_source_blockers(
                &facts,
                &pending_protective_algos,
                &assembled.snapshot.account_generation,
                &assembled.snapshot.account_uid_fingerprint,
            );
            if !coherence.coherent {
                opening_blockers.push("PRIVATE_WS_NOT_COHERENT");
            }
            let strategy_run_opening_source = StrategyOpeningSourcePreview {
                scope: "OBSERVER_SOURCE_ONLY_NO_TRADE_PERMISSION",
                status: if opening_blockers.is_empty() {
                    "SOURCE_READY"
                } else {
                    "BLOCKED"
                },
                blockers: opening_blockers,
            };

            let compact = matches!(request.operation, AgentOperation::TraderSnapshot);
            let full = serde_json::to_value(AccountSummaryResult {
                schema: ACCOUNT_SUMMARY_SCHEMA_V1,
                account_ledger: facts.summary,
                pending_protective_algos,
                strategy_run_opening_source,
                reconciliation,
                managed_execution_intents,
                coherence,
            })?;
            let result = if compact {
                let packet = compact_trader_packet(
                    &full,
                    &quality,
                    warnings.len(),
                    history_read_duration_ms,
                );
                if serde_json::to_vec(&packet)?.len() > MAX_TRADER_PACKET_BYTES {
                    return Ok(failure_response(
                        request,
                        generated_at,
                        AgentResponseStatus::Failed,
                        ACCOUNT_LEDGER_INCONSISTENT_CODE,
                        "compact trader snapshot exceeds bounded transport payload".to_owned(),
                        false,
                    ));
                }
                packet
            } else {
                full
            };
            Ok(AgentResponse {
                schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                request_id: request.request_id.clone(),
                status: AgentResponseStatus::Completed,
                generated_at: generated_at.to_owned(),
                quality,
                result_schema: Some(
                    if compact {
                        TRADER_SNAPSHOT_SCHEMA_V1
                    } else {
                        ACCOUNT_SUMMARY_SCHEMA_V1
                    }
                    .to_owned(),
                ),
                result: Some(result),
                failure: None,
                warnings: if compact { Vec::new() } else { warnings },
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
            virtual_portfolio,
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
            if let Some(virtual_portfolio) = virtual_portfolio.as_ref() {
                reference_instruments.extend(
                    virtual_portfolio
                        .positions
                        .iter()
                        .map(|position| position.instrument.clone()),
                );
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

            let analysis_mandate = trading_mandate(mandate);
            let analysis_policy = hard_risk_policy(policy);
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
                analysis_mandate.clone(),
                analysis_policy.clone(),
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
                // Exact USD equality compares two separately marked REST reads.
                // Preserve the explicit residual, but only classify it as an
                // impossible account mismatch when exact lot identity differs.
                // A moving valuation is reported DEGRADED, never silently FRESH.
                let oracle_lots = oracle
                    .positions
                    .iter()
                    .map(|position| okx_analysis::PositionOracleEvidence {
                        instrument_type: position.instrument_type.clone(),
                        instrument_id: position.instrument_id.clone(),
                        position_side: position.position_side.clone(),
                        margin_mode: position.margin_mode.clone(),
                        contracts: position.pos.clone(),
                        notional_usd: position.notional_usd.clone(),
                    })
                    .collect::<Vec<_>>();
                if let Err(error) = okx_analysis::conservative_account_with_position_oracle(
                    &assembled.snapshot,
                    &oracle_lots,
                    Some(&oracle.adjusted_equity_usd),
                ) {
                    return Ok(failure_response(
                        request,
                        generated_at,
                        AgentResponseStatus::Failed,
                        PORTFOLIO_RISK_ORACLE_MISMATCH_CODE,
                        format!(
                            "position-risk oracle lot identity, quantity or valuation invalid: {error}"
                        ),
                        false,
                    ));
                }
                quality = DataQuality::Degraded;
                warnings.push(format!(
                    "separately observed account-position-risk has exact matching lots but asynchronously different USD valuation: gross_residual_usd={}, adjusted_equity_residual_usd={}; exact USD equivalence not claimed; new-risk admission uses the conservative upper per-lot notional",
                    oracle_comparison.gross_notional_residual_usd,
                    oracle_comparison.adjusted_equity_residual_usd.as_deref().unwrap_or("<unavailable>"),
                ));
            }
            if !coherence.coherent && !oracle_comparison.consistent {
                warnings.push(
                    "OKX account-position-risk differs from local account evidence across an incoherent read window; residual is reported but not classified as a correctness failure"
                        .to_owned(),
                );
            }

            let virtual_portfolio_proof = if let Some(virtual_request) = virtual_portfolio.as_ref()
            {
                let expected_reference = match coherence.reference_generation.as_deref() {
                    Some(value) => value,
                    None => {
                        return Ok(failure_response(
                            request,
                            generated_at,
                            AgentResponseStatus::Failed,
                            PORTFOLIO_RISK_REFERENCE_INCONSISTENT_CODE,
                            "virtual portfolio proof is missing a reference generation".to_owned(),
                            false,
                        ));
                    }
                };
                match build_virtual_portfolio_proof(
                    request,
                    context,
                    generated_at,
                    VirtualPortfolioProofInput {
                        observer: account,
                        account_snapshot: &assembled.snapshot,
                        request: virtual_request,
                        statistics: statistics.as_ref(),
                        ledger: &facts.summary,
                        mandate: &analysis_mandate,
                        policy: &analysis_policy,
                        expected_reference,
                    },
                )
                .await
                {
                    Ok(value) => Some(value),
                    Err(response) => return Ok(response),
                }
            } else {
                None
            };

            if virtual_portfolio_proof.is_some()
                && let (Some(private_ws), Some(cursor)) = (context.private_ws, read_cursor)
            {
                match private_ws.convergence_window(cursor).await {
                    Ok(window) if window.events.is_empty() => {}
                    Ok(window) => {
                        return Ok(failure_response(
                            request,
                            generated_at,
                            AgentResponseStatus::Failed,
                            PORTFOLIO_RISK_SOURCE_TIME_INCONSISTENT_CODE,
                            format!(
                                "{} private account event(s) arrived before virtual portfolio proof completed",
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
                                "private account coherence changed before virtual portfolio proof completed: {error}"
                            ),
                            true,
                        ));
                    }
                }
            }

            let rejected = analysis.policy_decision == okx_analysis::RiskPolicyDecision::Rejected;
            let violation_codes = analysis
                .violations
                .iter()
                .map(|violation| violation.code)
                .collect::<Vec<_>>();
            let result_schema = if virtual_portfolio_proof.is_some() {
                PORTFOLIO_RISK_SCHEMA_V6
            } else if statistics_analysis.is_some() {
                PORTFOLIO_RISK_SCHEMA_V4
            } else {
                PORTFOLIO_RISK_SCHEMA_V3
            };
            let result = serde_json::to_value(PortfolioRiskResult {
                schema: result_schema,
                as_of: generated_at.to_owned(),
                observed_evidence_label: "OBSERVED",
                modelled_evidence_label: "MODELLED",
                counterfactual_evidence_label: if virtual_portfolio_proof.is_some()
                    || statistics_analysis
                        .as_ref()
                        .and_then(|statistics| statistics.parallel_scenario.as_ref())
                        .is_some()
                {
                    Some("COUNTERFACTUAL")
                } else {
                    None
                },
                analysis_schema: PORTFOLIO_RISK_ANALYSIS_SCHEMA_V3,
                mandate_schema: TRADING_MANDATE_SCHEMA_V1,
                policy_schema: HARD_RISK_POLICY_SCHEMA_V1,
                coherence,
                analysis,
                statistics: statistics_analysis,
                exchange_oracle: oracle_comparison,
                virtual_portfolio: virtual_portfolio_proof,
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

async fn build_virtual_portfolio_proof(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    input: VirtualPortfolioProofInput<'_>,
) -> Result<VirtualPortfolioProof, AgentResponse> {
    match virtual_portfolio_oracle_route(&input.account_snapshot.account_level) {
        Ok(VirtualPortfolioOracleRoute::FuturesLocalModel) => {
            build_futures_virtual_portfolio_proof(request, context, generated_at, input).await
        }
        Ok(VirtualPortfolioOracleRoute::PositionBuilder) => {
            build_position_builder_virtual_portfolio_proof(request, context, generated_at, input)
                .await
        }
        Err(error) => Err(analysis_failure(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            error,
        )),
    }
}

async fn build_position_builder_virtual_portfolio_proof(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    input: VirtualPortfolioProofInput<'_>,
) -> Result<VirtualPortfolioProof, AgentResponse> {
    let VirtualPortfolioProofInput {
        observer,
        account_snapshot,
        request: virtual_request,
        statistics: statistics_request,
        ledger,
        mandate,
        policy,
        expected_reference,
    } = input;
    let builder_request = okx_api::PositionBuilderRequest {
        account_level: account_snapshot.account_level.clone(),
        include_real_positions_and_equity: false,
        positions: virtual_request
            .positions
            .iter()
            .map(|position| okx_api::PositionBuilderSimPosition {
                instrument_id: position.instrument.clone(),
                contracts: position.contracts.clone(),
                average_price: position.average_price.clone(),
                leverage: position.leverage.clone(),
            })
            .collect(),
        assets: vec![okx_api::PositionBuilderSimAsset {
            currency: "USDT".to_owned(),
            amount: virtual_request.collateral_usdt.clone(),
        }],
    };
    let oracle = match observer.position_builder_oracle(&builder_request).await {
        Ok(value) => value,
        Err(error) => {
            if let Some(message) = position_builder_account_unavailable(&error) {
                return Err(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Failed,
                    POSITION_BUILDER_ACCOUNT_UNAVAILABLE_CODE,
                    message,
                    false,
                ));
            }
            return Err(account_failure(request, generated_at, error));
        }
    };
    let oracle_source_received_at = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

    if oracle.positions.len() != virtual_request.positions.len()
        || oracle
            .positions
            .iter()
            .any(|position| position.is_real_position)
    {
        return Err(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            VIRTUAL_PORTFOLIO_ORACLE_MISMATCH_CODE,
            format!(
                "position-builder returned {} positions for {} requested virtual positions or included a real position",
                oracle.positions.len(),
                virtual_request.positions.len()
            ),
            false,
        ));
    }

    let mut rules_by_instrument = Vec::with_capacity(virtual_request.positions.len());
    let mut notional_inputs = Vec::with_capacity(virtual_request.positions.len());
    for requested in &virtual_request.positions {
        let matching = oracle
            .positions
            .iter()
            .filter(|position| position.instrument_id == requested.instrument)
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            return Err(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                VIRTUAL_PORTFOLIO_ORACLE_MISMATCH_CODE,
                format!(
                    "position-builder returned {} rows for requested instrument '{}'",
                    matching.len(),
                    requested.instrument
                ),
                false,
            ));
        }
        let position = matching[0];
        if !matches!(position.instrument_type.as_str(), "SWAP" | "FUTURES") {
            return Err(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                VIRTUAL_PORTFOLIO_ORACLE_MISMATCH_CODE,
                format!(
                    "position-builder returned unsupported instrument type '{}' for '{}'",
                    position.instrument_type, requested.instrument
                ),
                false,
            ));
        }
        if !virtual_position_matches_request(requested, position) {
            return Err(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                VIRTUAL_PORTFOLIO_ORACLE_MISMATCH_CODE,
                format!(
                    "position-builder virtual position does not match requested contracts/direction for '{}'",
                    requested.instrument
                ),
                false,
            ));
        }
        if position.leverage.trim().is_empty() {
            return Err(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                VIRTUAL_PORTFOLIO_ORACLE_MISMATCH_CODE,
                format!(
                    "position-builder leverage is unavailable for '{}'",
                    requested.instrument
                ),
                false,
            ));
        }

        let Some(rules) = resolve_instrument_rules(context, &requested.instrument).await else {
            return Err(reference_not_found(
                request,
                generated_at,
                &requested.instrument,
            ));
        };
        if rules.reference_generation != expected_reference {
            return Err(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                PORTFOLIO_RISK_REFERENCE_INCONSISTENT_CODE,
                format!(
                    "virtual portfolio reference generation changed: expected {expected_reference}, observed {} for {}",
                    rules.reference_generation, requested.instrument
                ),
                false,
            ));
        }
        if rules.instrument.contract_type.as_deref() != Some("linear") {
            return Err(analysis_failure(
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
        let settle_currency = rules
            .instrument
            .settle_currency
            .as_deref()
            .unwrap_or_default();
        if !matches!(settle_currency, "USD" | "USDT" | "USDC" | "USDG") {
            return Err(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                VIRTUAL_PORTFOLIO_ORACLE_MISMATCH_CODE,
                format!(
                    "virtual portfolio proof requires USD-family settlement, observed '{settle_currency}' for {}",
                    requested.instrument
                ),
                false,
            ));
        }
        let Some(contract_value) = rules.instrument.contract_value.as_deref() else {
            return Err(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                VIRTUAL_PORTFOLIO_ORACLE_MISMATCH_CODE,
                format!(
                    "reference contract value is missing for '{}'",
                    requested.instrument
                ),
                false,
            ));
        };
        notional_inputs.push(VirtualNotionalOracleInput {
            instrument_id: requested.instrument.clone(),
            contracts: requested.contracts.clone(),
            contract_value: contract_value.to_owned(),
            mark_price: position.mark_price.clone(),
            oracle_notional_usd: position.notional_usd.clone(),
        });
        rules_by_instrument.push((requested.instrument.clone(), rules));
    }

    let notional_oracle = match compare_virtual_position_builder_notional(&notional_inputs) {
        Ok(value) => value,
        Err(error) => {
            return Err(analysis_failure(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                error,
            ));
        }
    };
    if !notional_oracle.consistent {
        return Err(failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            VIRTUAL_PORTFOLIO_ORACLE_MISMATCH_CODE,
            format!(
                "local linear-contract notional differs from OKX position-builder: gross residual USD {}",
                notional_oracle.gross_notional_residual_usd
            ),
            false,
        ));
    }

    let positions = oracle
        .positions
        .iter()
        .map(|position| {
            let rules = rules_by_instrument
                .iter()
                .find(|(instrument, _)| instrument == &position.instrument_id)
                .map(|(_, rules)| rules)
                .expect("validated position-builder instrument");
            let local_notional = notional_oracle
                .positions
                .iter()
                .find(|row| row.instrument_id == position.instrument_id)
                .expect("validated local notional row");
            okx_observation::AccountPositionState {
                instrument_type: position.instrument_type.clone(),
                instrument_id: position.instrument_id.clone(),
                position: virtual_request
                    .positions
                    .iter()
                    .find(|requested| requested.instrument == position.instrument_id)
                    .expect("validated requested virtual position")
                    .contracts
                    .clone(),
                position_side: "net".to_owned(),
                margin_mode: "cross".to_owned(),
                average_price: non_empty_option(&position.average_price),
                mark_price: non_empty_option(&position.mark_price),
                liquidation_price: None,
                unrealized_pnl: non_empty_option(&position.floating_pnl),
                unrealized_pnl_ratio: None,
                leverage: non_empty_option(&position.leverage),
                margin: None,
                initial_margin_requirement: non_empty_option(
                    &position.initial_margin_requirement_usd,
                ),
                maintenance_margin_requirement: None,
                margin_ratio: non_empty_option(&position.margin_ratio),
                notional_usd: Some(local_notional.local_notional_usd.clone()),
                margin_currency: rules.instrument.settle_currency.clone(),
                creation_time_ms: None,
                update_time_ms: None,
            }
        })
        .collect::<Vec<_>>();

    let virtual_snapshot = okx_observation::AccountSnapshot {
        schema: ACCOUNT_SNAPSHOT_SCHEMA_V2.to_owned(),
        source: "okx_position_builder_counterfactual".to_owned(),
        source_received_at: oracle_source_received_at.clone(),
        account_generation: format!("counterfactual/position-builder-v1/{expected_reference}"),
        quality_reason: "COUNTERFACTUAL_POSITION_BUILDER_READ_ORACLE".to_owned(),
        private_ws_connected: false,
        private_ws_generation: None,
        private_ws_connection_fingerprint: None,
        private_ws_last_inbound_ms: None,
        private_ws_events_applied: None,
        account_level: account_snapshot.account_level.clone(),
        position_mode: "net_mode".to_owned(),
        account_type: "counterfactual".to_owned(),
        account_uid_fingerprint: "counterfactual".to_owned(),
        api_key_permissions: vec!["read_only".to_owned()],
        balance: okx_observation::AccountBalanceState {
            total_equity_usd: oracle.equity_usd.clone(),
            adjusted_equity_usd: non_empty_option(&oracle.equity_usd),
            isolated_equity_usd: None,
            initial_margin_requirement_usd: non_empty_option(
                &oracle.initial_margin_requirement_usd,
            ),
            maintenance_margin_requirement_usd: non_empty_option(
                &oracle.maintenance_margin_requirement_usd,
            ),
            margin_ratio: non_empty_option(&oracle.margin_ratio),
            notional_usd: Some(notional_oracle.local_gross_notional_usd.clone()),
            update_time_ms: non_empty_option(&oracle.ts),
            details: Vec::new(),
        },
        positions,
        pending_orders: Vec::new(),
    };

    let analysis = match analyze_portfolio_risk(
        &virtual_snapshot,
        ledger,
        mandate.clone(),
        policy.clone(),
        None,
        true,
    ) {
        Ok(value) => value,
        Err(error) => {
            return Err(analysis_failure(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                error,
            ));
        }
    };

    let statistics = build_virtual_portfolio_statistics(
        request,
        context,
        generated_at,
        &analysis,
        statistics_request,
        expected_reference,
    )
    .await?;

    Ok(VirtualPortfolioProof {
        schema: VIRTUAL_PORTFOLIO_PROOF_SCHEMA_V2,
        evidence_label: "COUNTERFACTUAL",
        account_mode: match account_snapshot.account_level.as_str() {
            "3" => "multi_currency_margin",
            "4" => "portfolio_margin",
            _ => "unknown",
        }
        .to_owned(),
        oracle_scope: "okx_position_builder",
        exchange_virtual_portfolio_oracle_available: true,
        exchange_virtual_portfolio_oracle_reason: None,
        evidence_source_received_at: oracle_source_received_at,
        request: virtual_request.clone(),
        analysis,
        statistics,
        futures_constraints: Vec::new(),
        exchange_oracle: Some(oracle),
        notional_oracle: Some(notional_oracle),
    })
}

async fn build_futures_virtual_portfolio_proof(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    input: VirtualPortfolioProofInput<'_>,
) -> Result<VirtualPortfolioProof, AgentResponse> {
    let VirtualPortfolioProofInput {
        observer: _,
        account_snapshot,
        request: virtual_request,
        statistics: statistics_request,
        ledger,
        mandate,
        policy,
        expected_reference,
    } = input;

    if !matches!(
        account_snapshot.position_mode.as_str(),
        "net_mode" | "long_short_mode"
    ) {
        return Err(analysis_failure(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            AnalysisError::UnsupportedPositionMode(account_snapshot.position_mode.clone()),
        ));
    }

    let mut positions = Vec::with_capacity(virtual_request.positions.len());
    let mut futures_constraints = Vec::with_capacity(virtual_request.positions.len());
    let mut evidence_source_received_at = String::new();

    for requested in &virtual_request.positions {
        let market =
            match assemble_current_market(request, context, generated_at, &requested.instrument)
                .await
            {
                Ok(CurrentMarketAssembly::Ready(value)) => *value,
                Ok(CurrentMarketAssembly::Response(response)) => return Err(*response),
                Ok(CurrentMarketAssembly::Unavailable) => {
                    return Err(unavailable(request, generated_at));
                }
                Err(error) => {
                    return Err(failure_response(
                        request,
                        generated_at,
                        AgentResponseStatus::Failed,
                        MARKET_PUBLIC_API_UNAVAILABLE_CODE,
                        error.to_string(),
                        true,
                    ));
                }
            };

        if market.quality != DataQuality::Fresh {
            return Err(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                VIRTUAL_PORTFOLIO_MARKET_NOT_FRESH_CODE,
                format!(
                    "Futures-mode counterfactual requires FRESH mark/reference evidence for '{}', observed {:?}",
                    requested.instrument, market.quality
                ),
                true,
            ));
        }
        if market.rules.reference_generation != expected_reference
            || market.snapshot.reference_generation != expected_reference
        {
            return Err(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                PORTFOLIO_RISK_REFERENCE_INCONSISTENT_CODE,
                format!(
                    "Futures-mode virtual portfolio reference generation changed: expected {expected_reference}, rules={}, market={} for {}",
                    market.rules.reference_generation,
                    market.snapshot.reference_generation,
                    requested.instrument
                ),
                false,
            ));
        }
        if market.rules.instrument.contract_type.as_deref() != Some("linear") {
            return Err(analysis_failure(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                AnalysisError::UnsupportedContractMechanics(
                    market
                        .rules
                        .instrument
                        .contract_type
                        .clone()
                        .unwrap_or_else(|| "missing ctType".to_owned()),
                ),
            ));
        }

        let settle_currency = market
            .rules
            .instrument
            .settle_currency
            .as_deref()
            .unwrap_or_default();
        if !matches!(settle_currency, "USD" | "USDT" | "USDC" | "USDG") {
            return Err(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                VIRTUAL_PORTFOLIO_CONSTRAINT_REJECTED_CODE,
                format!(
                    "Futures-mode virtual portfolio requires USD-family settlement, observed '{settle_currency}' for {}",
                    requested.instrument
                ),
                false,
            ));
        }

        let Some(contract_value) = market.rules.instrument.contract_value.as_deref() else {
            return Err(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                VIRTUAL_PORTFOLIO_CONSTRAINT_REJECTED_CODE,
                format!(
                    "reference contract value is missing for '{}'",
                    requested.instrument
                ),
                false,
            ));
        };
        let Some(max_size) = market.rules.instrument.max_limit_size.as_deref() else {
            return Err(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                VIRTUAL_PORTFOLIO_CONSTRAINT_REJECTED_CODE,
                format!(
                    "reference maxLmtSz is missing for '{}'",
                    requested.instrument
                ),
                false,
            ));
        };
        let Some(max_leverage) = market.rules.instrument.max_leverage.as_deref() else {
            return Err(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                VIRTUAL_PORTFOLIO_CONSTRAINT_REJECTED_CODE,
                format!(
                    "reference max leverage is missing for '{}'",
                    requested.instrument
                ),
                false,
            ));
        };

        let constraint = match validate_virtual_linear_position(&VirtualPositionConstraintInput {
            instrument_id: requested.instrument.clone(),
            contracts: requested.contracts.clone(),
            contract_value: contract_value.to_owned(),
            mark_price: market.snapshot.mark_price.price.clone(),
            lot_size: market.rules.instrument.lot_size.clone(),
            min_size: market.rules.instrument.min_size.clone(),
            max_size: max_size.to_owned(),
            leverage: requested.leverage.clone(),
            max_leverage: max_leverage.to_owned(),
        }) {
            Ok(value) => value,
            Err(error) => {
                return Err(analysis_failure(
                    request,
                    generated_at,
                    AgentResponseStatus::Rejected,
                    error,
                ));
            }
        };

        if !constraint.constraints_satisfied {
            return Err(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Rejected,
                VIRTUAL_PORTFOLIO_CONSTRAINT_REJECTED_CODE,
                format!(
                    "virtual position '{}' violates exchange-published Futures constraints: lot_aligned={}, min_size={}, max_size={}, leverage={}",
                    requested.instrument,
                    constraint.lot_aligned,
                    constraint.minimum_size_satisfied,
                    constraint.maximum_size_satisfied,
                    constraint.leverage_satisfied
                ),
                false,
            ));
        }

        let (position, position_side) = match account_snapshot.position_mode.as_str() {
            "net_mode" => (constraint.signed_contracts.clone(), "net".to_owned()),
            "long_short_mode" => (
                constraint.absolute_contracts.clone(),
                if constraint.signed_contracts.starts_with('-') {
                    "short".to_owned()
                } else {
                    "long".to_owned()
                },
            ),
            _ => unreachable!("position mode validated"),
        };

        if evidence_source_received_at < market.snapshot.source_received_at {
            evidence_source_received_at = market.snapshot.source_received_at.clone();
        }

        positions.push(okx_observation::AccountPositionState {
            instrument_type: market.rules.instrument.instrument_type.to_string(),
            instrument_id: requested.instrument.clone(),
            position,
            position_side,
            margin_mode: "cross".to_owned(),
            average_price: Some(requested.average_price.clone()),
            mark_price: Some(market.snapshot.mark_price.price.clone()),
            liquidation_price: None,
            unrealized_pnl: None,
            unrealized_pnl_ratio: None,
            leverage: Some(constraint.requested_leverage.clone()),
            margin: None,
            initial_margin_requirement: Some(constraint.initial_margin_usd.clone()),
            maintenance_margin_requirement: None,
            margin_ratio: None,
            notional_usd: Some(constraint.local_notional_usd.clone()),
            margin_currency: market.rules.instrument.settle_currency.clone(),
            creation_time_ms: None,
            update_time_ms: Some(market.snapshot.mark_price.exchange_timestamp_ms.clone()),
        });
        futures_constraints.push(FuturesVirtualPositionEvidence {
            constraint,
            reference_generation: market.rules.reference_generation,
            market_generation: market.snapshot.market_generation,
            market_source: market.source,
            market_source_received_at: market.snapshot.source_received_at,
            mark_exchange_timestamp_ms: market.snapshot.mark_price.exchange_timestamp_ms,
        });
    }

    let initial_margin_requirement_usd = match virtual_portfolio_initial_margin_usd(
        &futures_constraints
            .iter()
            .map(|evidence| evidence.constraint.clone())
            .collect::<Vec<_>>(),
    ) {
        Ok(value) => value,
        Err(error) => {
            return Err(analysis_failure(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                error,
            ));
        }
    };

    let virtual_snapshot = okx_observation::AccountSnapshot {
        schema: ACCOUNT_SNAPSHOT_SCHEMA_V2.to_owned(),
        source: "okx_futures_counterfactual_local_model".to_owned(),
        source_received_at: generated_at.to_owned(),
        account_generation: format!("counterfactual/futures-v1/{expected_reference}"),
        quality_reason: "COUNTERFACTUAL_FUTURES_FRESH_MARK_REFERENCE".to_owned(),
        private_ws_connected: false,
        private_ws_generation: None,
        private_ws_connection_fingerprint: None,
        private_ws_last_inbound_ms: None,
        private_ws_events_applied: None,
        account_level: account_snapshot.account_level.clone(),
        position_mode: account_snapshot.position_mode.clone(),
        account_type: "counterfactual".to_owned(),
        account_uid_fingerprint: "counterfactual".to_owned(),
        api_key_permissions: vec!["read_only".to_owned()],
        balance: okx_observation::AccountBalanceState {
            total_equity_usd: virtual_request.collateral_usdt.clone(),
            adjusted_equity_usd: Some(virtual_request.collateral_usdt.clone()),
            isolated_equity_usd: None,
            initial_margin_requirement_usd: Some(initial_margin_requirement_usd),
            maintenance_margin_requirement_usd: None,
            margin_ratio: None,
            notional_usd: None,
            update_time_ms: None,
            details: Vec::new(),
        },
        positions,
        pending_orders: Vec::new(),
    };

    let analysis = match analyze_portfolio_risk(
        &virtual_snapshot,
        ledger,
        mandate.clone(),
        policy.clone(),
        None,
        true,
    ) {
        Ok(value) => value,
        Err(error) => {
            return Err(analysis_failure(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                error,
            ));
        }
    };

    let statistics = build_virtual_portfolio_statistics(
        request,
        context,
        generated_at,
        &analysis,
        statistics_request,
        expected_reference,
    )
    .await?;

    Ok(VirtualPortfolioProof {
        schema: VIRTUAL_PORTFOLIO_PROOF_SCHEMA_V2,
        evidence_label: "COUNTERFACTUAL",
        account_mode: "futures".to_owned(),
        oracle_scope: "okx_fresh_mark_reference_and_contract_constraints",
        exchange_virtual_portfolio_oracle_available: false,
        exchange_virtual_portfolio_oracle_reason: Some(
            "OKX Position Builder models Multi-currency/Portfolio margin (acctLv 3/4); no documented read-only Futures-mode endpoint simulates arbitrary synthetic collateral plus multi-instrument positions. Futures counterfactual therefore remains a local deterministic model over fresh OKX mark/reference facts while observed account risk is independently checked against account-position-risk."
                .to_owned(),
        ),
        evidence_source_received_at,
        request: virtual_request.clone(),
        analysis,
        statistics,
        futures_constraints,
        exchange_oracle: None,
        notional_oracle: None,
    })
}

async fn build_virtual_portfolio_statistics(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
    analysis: &okx_analysis::PortfolioRiskAnalysis,
    statistics_request: Option<&PortfolioStatisticsRequest>,
    expected_reference: &str,
) -> Result<Option<PortfolioStatisticsAnalysis>, AgentResponse> {
    let Some(statistics_request) = statistics_request else {
        return Ok(None);
    };

    let exposures = analysis
        .instrument_exposure
        .iter()
        .map(|row| StatisticalExposure {
            instrument_id: row.key.clone(),
            signed_notional_usd: row.signed_net_notional_usd.clone(),
        })
        .collect::<Vec<_>>();
    let mut histories = Vec::with_capacity(exposures.len());

    for exposure in &exposures {
        let Some(rules) = resolve_instrument_rules(context, &exposure.instrument_id).await else {
            return Err(reference_not_found(
                request,
                generated_at,
                &exposure.instrument_id,
            ));
        };
        if rules.reference_generation != expected_reference {
            return Err(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                PORTFOLIO_RISK_REFERENCE_INCONSISTENT_CODE,
                format!(
                    "virtual statistical reference generation changed before history acquisition: expected {expected_reference}, observed {} for {}",
                    rules.reference_generation, exposure.instrument_id
                ),
                false,
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
            Ok(None) => return Err(unavailable(request, generated_at)),
            Err(error) => return Err(market_failure(request, generated_at, error)),
        };
        if assembled_history.snapshot.reference_generation != expected_reference {
            return Err(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                PORTFOLIO_RISK_REFERENCE_INCONSISTENT_CODE,
                format!(
                    "virtual statistical history for '{}' references generation {}, expected {}",
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
        let Some(rules) = resolve_instrument_rules(context, &exposure.instrument_id).await else {
            return Err(reference_not_found(
                request,
                generated_at,
                &exposure.instrument_id,
            ));
        };
        if rules.reference_generation != expected_reference {
            return Err(failure_response(
                request,
                generated_at,
                AgentResponseStatus::Failed,
                PORTFOLIO_RISK_REFERENCE_INCONSISTENT_CODE,
                format!(
                    "virtual statistical reference generation changed during history acquisition: expected {expected_reference}, observed {} for {}",
                    rules.reference_generation, exposure.instrument_id
                ),
                false,
            ));
        }
    }

    analyze_portfolio_statistics(
        &exposures,
        &histories,
        statistics_request.parallel_scenario_move_ratio.as_deref(),
    )
    .map(Some)
    .map_err(|error| analysis_failure(request, generated_at, AgentResponseStatus::Failed, error))
}

fn position_builder_account_unavailable(error: &AccountBootstrapError) -> Option<String> {
    match error {
        AccountBootstrapError::Api(okx_api::OkxError::Api { code, message }) if code == "50008" => {
            Some(format!(
                "OKX Position Builder is unavailable for the authenticated account: API {code}: {message}"
            ))
        }
        _ => None,
    }
}

fn virtual_position_matches_request(
    requested: &okx_protocol::VirtualPortfolioPositionRequest,
    observed: &okx_api::PositionBuilderPosition,
) -> bool {
    let Ok(requested_absolute) =
        okx_analysis::linear_contract_notional_usd(&requested.contracts, "1", "1")
    else {
        return false;
    };
    let Ok(observed_absolute) =
        okx_analysis::linear_contract_notional_usd(&observed.contracts, "1", "1")
    else {
        return false;
    };
    if requested_absolute != observed_absolute {
        return false;
    }

    let requested_negative = requested.contracts.trim_start().starts_with('-');
    let observed_negative = observed.contracts.trim_start().starts_with('-');
    match observed.position_side.as_str() {
        "net" => requested_negative == observed_negative,
        "long" => !requested_negative,
        "short" => requested_negative,
        _ => false,
    }
}

fn non_empty_option(value: &str) -> Option<String> {
    (!value.trim().is_empty()).then(|| value.to_owned())
}

pub(super) fn observation_skew_ms(
    source_received_at: &str,
    oracle_timestamp_ms: &str,
) -> Result<u64, String> {
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
    fn trader_packet_is_bounded_never_mints_go_and_keeps_exact_finance_proof() {
        use serde_json::json;
        let mut full = json!({
            "account_ledger": {
                "authority": {"account_uid_fingerprint": "account-sha"},
                "open_positions": 0,
                "pending_orders": 0,
                "total_equity_usd": "5000",
                "current_unrealized_pnl": "0",
                "history_coverage": [{"complete_within_bound": true}]
            },
            "pending_protective_algos": {
                "rows": 0,
                "complete_within_bound": true
            },
            "coherence": {
                "coherent": true,
                "current_account_as_of_ms": "1792024604593",
                "account_snapshot_source_received_at": "2026-10-15T01:00:00Z",
                "history_source_received_at": "2026-10-15T01:00:01Z",
                "history_read_duration_ms": 125,
                "ledger_history_read_duration_ms": 120,
                "protective_algo_read_duration_ms": 95
            },
            "reconciliation": {
                "consistent": true,
                "strategy_risk_stop": {"reason": "STRATEGY_UTC_DAILY_GROSS_LOSS"},
                "financial_period": {
                    "run_id": "stage5b_physical_test_run",
                    "state": "FLAT_RECONCILED",
                    "nav_usdt": "99.996587799",
                    "funding_usdt": null
                },
                "finalized_run": {
                    "run_id": "stage5b_physical_test_run",
                    "nav_usdt": "99.996587799",
                    "realized_usdt": "0.004762",
                    "fees_usdt": "-0.008174201",
                    "checksum_sha256": "542337bbb39787dba9f128c7828716e12f95cc3954358400e644fb28f0fe75ce"
                },
                "archived_runs_count": 4,
                "unverified_runs_count": 1,
                "archived_run_slots_remaining": 251,
                "strategy_attribution": {"unallocated_funding_bills": 2},
                "exchange_evidence": {
                    "source_gap_over_two_hours": false,
                    "latest_source_complete": true,
                    "latest_source_asof_ms": 1792023166014u64
                }
            },
            "managed_execution_intents": {
                "total": 45,
                "shown": 16,
                "rows": vec!["big diagnostic content".repeat(500); 16]
            }
        });
        let p = compact_trader_packet(&full, &DataQuality::Degraded, 2, 125);
        assert_eq!(p["schema"], TRADER_SNAPSHOT_SCHEMA_V1);
        assert_eq!(p["decision"]["new_risk"], "NO_GO");
        assert_eq!(
            p["decision"]["scope"],
            "READ_ONLY_OBSERVER_NOT_TRADE_PERMISSION"
        );
        assert_eq!(p["strategy"]["finalized_nav_usdt"], "99.996587799");
        assert_eq!(p["account"]["account_equity_usd"], "5000");
        assert_eq!(p["account"]["equity_is_strategy_nav"], false);
        assert_eq!(
            p["strategy"]["finalized_checksum_sha256"],
            full["reconciliation"]["finalized_run"]["checksum_sha256"]
        );
        assert_eq!(p["funding"]["unallocated_exchange_bills"], 2);
        // Performance telemetry must reflect independently measured source
        // windows without altering the financial/account proof or inventing
        // FRESH from parallel REST completion.
        assert_eq!(p["transport"]["native_source_read_ms"], 125);
        assert_eq!(p["transport"]["ledger_history_read_ms"], 120);
        assert_eq!(p["transport"]["protective_algo_read_ms"], 95);
        assert_eq!(p["evidence"]["current_account_asof_ms"], "1792024604593");
        assert_eq!(
            p["evidence"]["account_source_received_at"],
            "2026-10-15T01:00:00Z"
        );
        assert!(p["funding"]["allocated_to_strategy_usdt"].is_null());
        assert!(serde_json::to_vec(&p).unwrap().len() <= MAX_TRADER_PACKET_BYTES);
        assert_eq!(
            p["transport"]["packet_bytes"].as_u64().unwrap() as usize,
            serde_json::to_vec(&p).unwrap().len()
        );
        assert!(
            p["transport"]["full_account_result_bytes"]
                .as_u64()
                .unwrap()
                > 5 * p["transport"]["packet_bytes"].as_u64().unwrap()
        );
        // A healthy observer still cannot authorize new risk without a typed
        // native executor + explicit risk/mandate; not a false green GO.
        full["reconciliation"]["strategy_risk_stop"] = json!(null);
        full["reconciliation"]["finalized_run"] = json!(null);
        full["reconciliation"]["strategy_attribution"]["unallocated_funding_bills"] = json!(0);
        let seemingly_ready = compact_trader_packet(&full, &DataQuality::Fresh, 0, 1);
        assert_eq!(seemingly_ready["decision"]["new_risk"], "NOT_ATTESTED");
        full["reconciliation"]["financial_period"]["state"] = json!("INCOMPLETE");
        let incomplete = compact_trader_packet(&full, &DataQuality::Fresh, 0, 1);
        assert_eq!(incomplete["decision"]["new_risk"], "NO_GO");
        full["reconciliation"]["financial_period"]["state"] = json!("FLAT_RECONCILED");
        full["reconciliation"]["strategy_attribution"]["unallocated_funding_bills"] = json!(1);
        let unfunded = compact_trader_packet(&full, &DataQuality::Fresh, 0, 1);
        assert_eq!(unfunded["decision"]["new_risk"], "NO_GO");
        assert_ne!(seemingly_ready["decision"]["new_risk"], "GO");
        let absent = compact_trader_packet(&json!({}), &DataQuality::Fresh, 0, 1);
        assert_eq!(absent["decision"]["new_risk"], "NO_GO");
        assert!(absent["account"]["positions"].is_null());
    }

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

#[cfg(test)]
mod position_builder_contract_tests {
    use super::*;

    #[test]
    fn virtual_portfolio_oracle_route_follows_authenticated_account_mode() {
        assert_eq!(
            virtual_portfolio_oracle_route("2").expect("futures"),
            VirtualPortfolioOracleRoute::FuturesLocalModel
        );
        assert_eq!(
            virtual_portfolio_oracle_route("3").expect("multi currency"),
            VirtualPortfolioOracleRoute::PositionBuilder
        );
        assert_eq!(
            virtual_portfolio_oracle_route("4").expect("portfolio margin"),
            VirtualPortfolioOracleRoute::PositionBuilder
        );
        assert!(matches!(
            virtual_portfolio_oracle_route("1"),
            Err(AnalysisError::UnsupportedAccountMode(mode)) if mode == "1"
        ));
    }

    #[test]
    fn position_builder_50008_is_typed_non_retryable_eligibility() {
        let error = AccountBootstrapError::Api(okx_api::OkxError::Api {
            code: "50008".to_owned(),
            message: "User doesn't exist.".to_owned(),
        });
        assert_eq!(
            position_builder_account_unavailable(&error).as_deref(),
            Some(
                "OKX Position Builder is unavailable for the authenticated account: API 50008: User doesn't exist."
            )
        );

        let transient = AccountBootstrapError::Api(okx_api::OkxError::Api {
            code: "50011".to_owned(),
            message: "Rate limit reached".to_owned(),
        });
        assert!(position_builder_account_unavailable(&transient).is_none());
    }
}

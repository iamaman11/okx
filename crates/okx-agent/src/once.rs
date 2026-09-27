use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{SecondsFormat, Utc};
use okx_analysis::{
    ACCOUNT_RISK_ANALYSIS_SCHEMA_V1, AnalysisError, CANDIDATE_ORDER_ANALYSIS_SCHEMA_V1,
    CandidateOrderAssumptions, LiquidityRole as AnalysisLiquidityRole, PositionDirection,
    analyze_account_risk, analyze_candidate_order,
};
use okx_github::{ISSUE_POLL_TELEMETRY_SCHEMA_V1, IssuePollTelemetryStatus};
use okx_observation::{
    ACCOUNT_SNAPSHOT_SCHEMA_V1, ACCOUNT_SNAPSHOT_SCHEMA_V2, AccountError, AccountSnapshot,
    INSTRUMENT_RULES_SCHEMA_V1, INSTRUMENT_SEARCH_SCHEMA_V1, InstrumentRulesSnapshot,
    MARKET_HISTORY_SCHEMA_V1, MARKET_SNAPSHOT_SCHEMA_V1, MarketError, MarketHistoryError,
    MarketReadiness, MarketSnapshot, ReferenceRegistry, SNAPSHOT_QUALITY_SCHEMA_V1,
    SnapshotQualityReport,
};
use okx_protocol::{
    AGENT_REQUEST_SCHEMA_V1, AGENT_RESPONSE_SCHEMA_V1, AgentFailure, AgentOperation, AgentRequest,
    AgentResponse, AgentResponseStatus, DataQuality, InstrumentTypeFilter,
    LiquidityRole as ProtocolLiquidityRole, MAILBOX_ENVELOPE_SCHEMA_V1, MailboxDirection,
    MailboxEnvelope, PositionSide,
    crypto::{decrypt, derive_directional_key, encrypt, shared_secret},
};
use okx_runtime::{
    PUBLIC_SNAPSHOT_QUALITY_SCHEMA_V2, PrivateConvergenceError, PrivateWsHandle,
    PublicQualitySnapshot, PublicWsHandle,
};

use crate::{
    AgentError, AgentResult,
    account_bootstrap::{AccountBootstrapError, AccountBootstrapper, FeeScheduleBootstrapError},
    market_bootstrap::{MarketBootstrapError, MarketBootstrapper},
};

pub const P1_NOT_AVAILABLE_CODE: &str = "P1_OPERATION_NOT_AVAILABLE";
pub const REFERENCE_INSTRUMENT_NOT_FOUND_CODE: &str = "REFERENCE_INSTRUMENT_NOT_FOUND";
pub const MARKET_REFERENCE_INCOMPLETE_CODE: &str = "MARKET_REFERENCE_INCOMPLETE";
pub const MARKET_PUBLIC_API_UNAVAILABLE_CODE: &str = "MARKET_PUBLIC_API_UNAVAILABLE";
pub const MARKET_BOOTSTRAP_INCONSISTENT_CODE: &str = "MARKET_BOOTSTRAP_INCONSISTENT";
pub const MARKET_INSTRUMENT_NOT_LIVE_CODE: &str = "MARKET_INSTRUMENT_NOT_LIVE";
pub const MARKET_OVERVIEW_INCONSISTENT_CODE: &str = "MARKET_OVERVIEW_INCONSISTENT";
pub const MARKET_HISTORY_INCONSISTENT_CODE: &str = "MARKET_HISTORY_INCONSISTENT";
pub const ACCOUNT_OBSERVER_CREDENTIAL_UNAVAILABLE_CODE: &str =
    "ACCOUNT_OBSERVER_CREDENTIAL_UNAVAILABLE";
pub const ACCOUNT_OBSERVER_PERMISSION_REJECTED_CODE: &str = "ACCOUNT_OBSERVER_PERMISSION_REJECTED";
pub const ACCOUNT_PRIVATE_API_UNAVAILABLE_CODE: &str = "ACCOUNT_PRIVATE_API_UNAVAILABLE";
pub const ACCOUNT_BOOTSTRAP_INCONSISTENT_CODE: &str = "ACCOUNT_BOOTSTRAP_INCONSISTENT";
pub const ANALYSIS_INPUT_INCONSISTENT_CODE: &str = "ANALYSIS_INPUT_INCONSISTENT";
pub const ANALYSIS_EXACT_FEE_UNAVAILABLE_CODE: &str = "ANALYSIS_EXACT_FEE_UNAVAILABLE";
pub const MARKET_OVERVIEW_SCHEMA_V1: &str = "okx.market-overview/v1";

const REFERENCE_BOOTSTRAP_WARNING: &str =
    "reference data is REST-bootstrap only; live instruments continuity is not connected until M3";
const MARKET_REST_BOOTSTRAP_WARNING: &str = "market data is bounded public REST bootstrap; persistent WebSocket continuity is not connected until M3";
const REFERENCE_RUNTIME_WARNING: &str = "instrument rules come from the live ReferenceRegistry; market FRESH readiness is reported separately";
const MARKET_HISTORY_UNCONFIRMED_WARNING: &str =
    "OKX history response contains at least one unconfirmed candlestick";
const ACCOUNT_REST_BOOTSTRAP_WARNING: &str = "private account state is a bounded authenticated REST bootstrap; private WebSocket convergence is not ready";
const ACCOUNT_WS_GENERATION_CHANGED_WARNING: &str = "private WebSocket generation changed during REST bootstrap; returning coherent REST snapshot only";
const ACCOUNT_WS_JOURNAL_GAP_WARNING: &str = "private WebSocket delta journal advanced beyond the REST bootstrap cursor; returning coherent REST snapshot only";
const CANDIDATE_EXPLICIT_ASSUMPTIONS_WARNING: &str = "candidate analysis uses explicit hypothetical entry/stop prices and exact account fee evidence; it does not assume a current fill price, funding event, slippage, spread, margin or FX conversion";
pub const PUBLIC_MARKET_MAX_AGE_MS: u64 = 120_000;

#[derive(serde::Serialize)]
struct MarketOverviewResult {
    instrument_rules: InstrumentRulesSnapshot,
    market: MarketSnapshot,
    quality: PublicQualitySnapshot,
    market_source: &'static str,
}

#[derive(Clone, Copy)]
pub struct ObservationQueryContext<'a> {
    standalone_reference: Option<&'a ReferenceRegistry>,
    market_fallback: Option<&'a MarketBootstrapper>,
    public_ws: Option<&'a PublicWsHandle>,
    mailbox_telemetry: Option<&'a IssuePollTelemetryStatus>,
    account_fallback: Option<&'a AccountBootstrapper>,
    private_ws: Option<&'a PrivateWsHandle>,
}

impl<'a> ObservationQueryContext<'a> {
    pub const fn unavailable() -> Self {
        Self {
            standalone_reference: None,
            market_fallback: None,
            public_ws: None,
            mailbox_telemetry: None,
            account_fallback: None,
            private_ws: None,
        }
    }

    pub const fn standalone(
        reference: &'a ReferenceRegistry,
        market: &'a MarketBootstrapper,
    ) -> Self {
        Self::standalone_with_account(reference, market, None)
    }

    pub const fn standalone_with_account(
        reference: &'a ReferenceRegistry,
        market: &'a MarketBootstrapper,
        account_fallback: Option<&'a AccountBootstrapper>,
    ) -> Self {
        Self {
            standalone_reference: Some(reference),
            market_fallback: Some(market),
            public_ws: None,
            mailbox_telemetry: None,
            account_fallback,
            private_ws: None,
        }
    }

    pub const fn live(
        public_ws: &'a PublicWsHandle,
        market_fallback: &'a MarketBootstrapper,
    ) -> Self {
        Self::live_with_private(public_ws, market_fallback, None, None, None)
    }

    pub const fn live_with_mailbox_telemetry(
        public_ws: &'a PublicWsHandle,
        market_fallback: &'a MarketBootstrapper,
        mailbox_telemetry: Option<&'a IssuePollTelemetryStatus>,
    ) -> Self {
        Self::live_with_private(public_ws, market_fallback, mailbox_telemetry, None, None)
    }

    pub const fn live_with_private(
        public_ws: &'a PublicWsHandle,
        market_fallback: &'a MarketBootstrapper,
        mailbox_telemetry: Option<&'a IssuePollTelemetryStatus>,
        account_fallback: Option<&'a AccountBootstrapper>,
        private_ws: Option<&'a PrivateWsHandle>,
    ) -> Self {
        Self {
            standalone_reference: None,
            market_fallback: Some(market_fallback),
            public_ws: Some(public_ws),
            mailbox_telemetry,
            account_fallback,
            private_ws,
        }
    }
}

pub async fn process_once(
    envelope: &MailboxEnvelope,
    expected_key_id: &str,
    agent_private_key: &[u8; 32],
    context: ObservationQueryContext<'_>,
    response_nonce: [u8; 12],
    generated_at: &str,
) -> AgentResult<MailboxEnvelope> {
    envelope.validate(MailboxDirection::ClientToAgent)?;

    if envelope.agent_key_id != expected_key_id {
        return Err(AgentError::AgentKeyMismatch {
            expected: expected_key_id.to_owned(),
            actual: envelope.agent_key_id.clone(),
        });
    }

    let client_public_key = decode_fixed::<32>(&envelope.client_ephemeral_public_key)?;
    let request_nonce = decode_fixed::<12>(&envelope.nonce)?;
    let shared = shared_secret(*agent_private_key, client_public_key)?;
    let request_key = derive_directional_key(
        &shared,
        &envelope.request_id,
        expected_key_id,
        MailboxDirection::ClientToAgent,
    )?;
    let ciphertext = STANDARD.decode(&envelope.ciphertext)?;
    let aad = envelope.aad()?;
    let plaintext = decrypt(&request_key, &request_nonce, aad.as_bytes(), &ciphertext)?;

    let request: AgentRequest = serde_json::from_slice(&plaintext)?;
    request.validate()?;
    if request.request_id != envelope.request_id {
        return Err(AgentError::RequestIdMismatch);
    }
    debug_assert_eq!(request.schema, AGENT_REQUEST_SCHEMA_V1);

    let response = response_for(&request, context, generated_at).await?;
    response.validate()?;
    let response_plaintext = serde_json::to_vec(&response)?;
    let response_key = derive_directional_key(
        &shared,
        &envelope.request_id,
        expected_key_id,
        MailboxDirection::AgentToClient,
    )?;

    let mut response_envelope = MailboxEnvelope {
        schema: MAILBOX_ENVELOPE_SCHEMA_V1.to_owned(),
        request_id: envelope.request_id.clone(),
        direction: MailboxDirection::AgentToClient,
        agent_key_id: expected_key_id.to_owned(),
        client_ephemeral_public_key: envelope.client_ephemeral_public_key.clone(),
        nonce: STANDARD.encode(response_nonce),
        ciphertext: String::new(),
    };
    let response_aad = response_envelope.aad()?;
    let response_ciphertext = encrypt(
        &response_key,
        &response_nonce,
        response_aad.as_bytes(),
        &response_plaintext,
    )?;
    response_envelope.ciphertext = STANDARD.encode(response_ciphertext);

    Ok(response_envelope)
}

pub async fn process_once_now(
    envelope: &MailboxEnvelope,
    expected_key_id: &str,
    agent_private_key: &[u8; 32],
    context: ObservationQueryContext<'_>,
) -> AgentResult<MailboxEnvelope> {
    let mut nonce = [0_u8; 12];
    getrandom::fill(&mut nonce).map_err(|error| AgentError::Random(error.to_string()))?;
    let generated_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);

    process_once(
        envelope,
        expected_key_id,
        agent_private_key,
        context,
        nonce,
        &generated_at,
    )
    .await
}

async fn response_for(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
) -> AgentResult<AgentResponse> {
    match &request.operation {
        AgentOperation::MarketSnapshot { instrument } => {
            if let Some(public_ws) = context.public_ws {
                if public_ws.instrument_rules(instrument).await.is_none() {
                    return Ok(reference_not_found(request, generated_at, instrument));
                }
                public_ws.demand_instrument(instrument.clone()).await?;

                let now_ms = utc_now_ms();
                let quality = public_ws
                    .quality_snapshot(instrument, now_ms, PUBLIC_MARKET_MAX_AGE_MS, true)
                    .await?;

                if quality.quality == MarketReadiness::Fresh {
                    let live = public_ws
                        .fresh_snapshot(
                            instrument,
                            now_ms,
                            PUBLIC_MARKET_MAX_AGE_MS,
                            generated_at.to_owned(),
                        )
                        .await?;
                    return Ok(AgentResponse {
                        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                        request_id: request.request_id.clone(),
                        status: AgentResponseStatus::Completed,
                        generated_at: generated_at.to_owned(),
                        quality: DataQuality::Fresh,
                        result_schema: Some(MARKET_SNAPSHOT_SCHEMA_V1.to_owned()),
                        result: Some(serde_json::to_value(live.market)?),
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
                return match market.snapshot(&reference, instrument).await {
                    Ok(result) => Ok(AgentResponse {
                        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                        request_id: request.request_id.clone(),
                        status: AgentResponseStatus::Completed,
                        generated_at: generated_at.to_owned(),
                        quality: DataQuality::Degraded,
                        result_schema: Some(MARKET_SNAPSHOT_SCHEMA_V1.to_owned()),
                        result: Some(serde_json::to_value(result)?),
                        failure: None,
                        warnings: vec![format!(
                            "persistent WebSocket state is not FRESH ({}); returned bounded public REST fallback",
                            quality.reason
                        )],
                    }),
                    Err(error) => Ok(market_failure(request, generated_at, error)),
                };
            }

            let (Some(reference), Some(market)) =
                (context.standalone_reference, context.market_fallback)
            else {
                return Ok(unavailable(request, generated_at));
            };

            match market.snapshot(reference, instrument).await {
                Ok(result) => Ok(AgentResponse {
                    schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                    request_id: request.request_id.clone(),
                    status: AgentResponseStatus::Completed,
                    generated_at: generated_at.to_owned(),
                    quality: DataQuality::Degraded,
                    result_schema: Some(MARKET_SNAPSHOT_SCHEMA_V1.to_owned()),
                    result: Some(serde_json::to_value(result)?),
                    failure: None,
                    warnings: vec![MARKET_REST_BOOTSTRAP_WARNING.to_owned()],
                }),
                Err(error) => Ok(market_failure(request, generated_at, error)),
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
        AgentOperation::MarketHistory {
            instrument,
            bar,
            limit,
        } => {
            let Some(market) = context.market_fallback else {
                return Ok(unavailable(request, generated_at));
            };
            let reference = if let Some(public_ws) = context.public_ws {
                public_ws.reference_snapshot().await
            } else if let Some(reference) = context.standalone_reference {
                reference.clone()
            } else {
                return Ok(unavailable(request, generated_at));
            };

            let requested_limit = limit.unwrap_or(100);
            match market
                .history(&reference, instrument, bar, requested_limit)
                .await
            {
                Ok(result) => {
                    let all_confirmed = result.all_confirmed;
                    Ok(AgentResponse {
                        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                        request_id: request.request_id.clone(),
                        status: AgentResponseStatus::Completed,
                        generated_at: generated_at.to_owned(),
                        quality: if all_confirmed {
                            DataQuality::Fresh
                        } else {
                            DataQuality::Degraded
                        },
                        result_schema: Some(MARKET_HISTORY_SCHEMA_V1.to_owned()),
                        result: Some(serde_json::to_value(result)?),
                        failure: None,
                        warnings: if all_confirmed {
                            Vec::new()
                        } else {
                            vec![MARKET_HISTORY_UNCONFIRMED_WARNING.to_owned()]
                        },
                    })
                }
                Err(error) => Ok(market_failure(request, generated_at, error)),
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
        AgentOperation::AnalyzeCandidateOrder {
            instrument,
            side,
            entry_price,
            stop_price,
            max_settle_notional,
            max_loss_settle,
            target_rr,
            entry_liquidity_role,
            exit_liquidity_role,
        } => {
            let rules = if let Some(public_ws) = context.public_ws {
                let Some(rules) = public_ws.instrument_rules(instrument).await else {
                    return Ok(reference_not_found(request, generated_at, instrument));
                };
                rules
            } else if let Some(reference) = context.standalone_reference {
                let Some(rules) = reference.instrument_rules(instrument) else {
                    return Ok(reference_not_found(request, generated_at, instrument));
                };
                rules
            } else {
                return Ok(unavailable(request, generated_at));
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
            let fees = match account.fee_schedule(&rules).await {
                Ok(value) => value,
                Err(error) => return Ok(fee_schedule_failure(request, generated_at, error)),
            };
            let assumptions = CandidateOrderAssumptions {
                direction: match side {
                    PositionSide::Long => PositionDirection::Long,
                    PositionSide::Short => PositionDirection::Short,
                },
                entry_price: entry_price.clone(),
                stop_price: stop_price.clone(),
                max_settle_notional: max_settle_notional.clone(),
                max_loss_settle: max_loss_settle.clone(),
                target_rr: target_rr.clone(),
                entry_liquidity_role: analysis_liquidity_role(*entry_liquidity_role),
                exit_liquidity_role: analysis_liquidity_role(*exit_liquidity_role),
            };
            let result = match analyze_candidate_order(&rules, &fees, &assumptions) {
                Ok(value) => value,
                Err(error) => {
                    return Ok(analysis_failure(
                        request,
                        generated_at,
                        AgentResponseStatus::Rejected,
                        error,
                    ));
                }
            };
            Ok(AgentResponse {
                schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                request_id: request.request_id.clone(),
                status: AgentResponseStatus::Completed,
                generated_at: generated_at.to_owned(),
                quality: DataQuality::Degraded,
                result_schema: Some(CANDIDATE_ORDER_ANALYSIS_SCHEMA_V1.to_owned()),
                result: Some(serde_json::to_value(result)?),
                failure: None,
                warnings: vec![CANDIDATE_EXPLICIT_ASSUMPTIONS_WARNING.to_owned()],
            })
        }
        AgentOperation::MailboxTelemetry => {
            let Some(telemetry) = context.mailbox_telemetry else {
                return Ok(unavailable(request, generated_at));
            };
            Ok(AgentResponse {
                schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                request_id: request.request_id.clone(),
                status: AgentResponseStatus::Completed,
                generated_at: generated_at.to_owned(),
                quality: DataQuality::Fresh,
                result_schema: Some(ISSUE_POLL_TELEMETRY_SCHEMA_V1.to_owned()),
                result: Some(serde_json::to_value(telemetry)?),
                failure: None,
                warnings: Vec::new(),
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
    }
}

struct AssembledAccountSnapshot {
    snapshot: AccountSnapshot,
    quality: DataQuality,
    result_schema: &'static str,
    warnings: Vec<String>,
}

enum AccountQueryError {
    CredentialUnavailable,
    Bootstrap(AccountBootstrapError),
    Convergence(AccountError),
}

async fn assemble_account_snapshot(
    context: ObservationQueryContext<'_>,
) -> Result<AssembledAccountSnapshot, AccountQueryError> {
    let account = context
        .account_fallback
        .ok_or(AccountQueryError::CredentialUnavailable)?;
    let convergence_cursor = match context.private_ws {
        Some(private_ws) => private_ws.convergence_cursor().await.ok(),
        None => None,
    };
    let rest = account
        .snapshot()
        .await
        .map_err(AccountQueryError::Bootstrap)?;

    if let (Some(private_ws), Some(cursor)) = (context.private_ws, convergence_cursor) {
        match private_ws.convergence_window(cursor).await {
            Ok(window) => {
                if let (Some(connection_fingerprint), Some(last_inbound_ms)) = (
                    window.status.connection_id_fingerprint.as_deref(),
                    window.status.last_inbound_ms,
                ) {
                    let snapshot = rest
                        .converge_private_ws(
                            window.generation,
                            connection_fingerprint,
                            last_inbound_ms,
                            &window.events,
                        )
                        .map_err(AccountQueryError::Convergence)?;
                    return Ok(AssembledAccountSnapshot {
                        snapshot,
                        quality: DataQuality::Fresh,
                        result_schema: ACCOUNT_SNAPSHOT_SCHEMA_V2,
                        warnings: Vec::new(),
                    });
                }
            }
            Err(PrivateConvergenceError::GenerationChanged) => {
                return Ok(AssembledAccountSnapshot {
                    snapshot: rest,
                    quality: DataQuality::Degraded,
                    result_schema: ACCOUNT_SNAPSHOT_SCHEMA_V1,
                    warnings: vec![ACCOUNT_WS_GENERATION_CHANGED_WARNING.to_owned()],
                });
            }
            Err(PrivateConvergenceError::JournalGap) => {
                return Ok(AssembledAccountSnapshot {
                    snapshot: rest,
                    quality: DataQuality::Degraded,
                    result_schema: ACCOUNT_SNAPSHOT_SCHEMA_V1,
                    warnings: vec![ACCOUNT_WS_JOURNAL_GAP_WARNING.to_owned()],
                });
            }
            Err(PrivateConvergenceError::NotReady) => {}
        }
    }

    Ok(AssembledAccountSnapshot {
        snapshot: rest,
        quality: DataQuality::Degraded,
        result_schema: ACCOUNT_SNAPSHOT_SCHEMA_V1,
        warnings: vec![ACCOUNT_REST_BOOTSTRAP_WARNING.to_owned()],
    })
}

fn account_query_failure(
    request: &AgentRequest,
    generated_at: &str,
    error: AccountQueryError,
) -> AgentResponse {
    match error {
        AccountQueryError::CredentialUnavailable => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            ACCOUNT_OBSERVER_CREDENTIAL_UNAVAILABLE_CODE,
            "OKX observer credential is not provisioned in native secret storage".to_owned(),
            false,
        ),
        AccountQueryError::Bootstrap(error) => account_failure(request, generated_at, error),
        AccountQueryError::Convergence(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            ACCOUNT_BOOTSTRAP_INCONSISTENT_CODE,
            error.to_string(),
            false,
        ),
    }
}

fn fee_schedule_failure(
    request: &AgentRequest,
    generated_at: &str,
    error: FeeScheduleBootstrapError,
) -> AgentResponse {
    match error {
        FeeScheduleBootstrapError::PermissionRejected => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            ACCOUNT_OBSERVER_PERMISSION_REJECTED_CODE,
            "OKX observer API key must have read_only permission only".to_owned(),
            false,
        ),
        FeeScheduleBootstrapError::Api(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            ACCOUNT_PRIVATE_API_UNAVAILABLE_CODE,
            error.to_string(),
            true,
        ),
        FeeScheduleBootstrapError::ReferenceIncomplete(field) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            ANALYSIS_EXACT_FEE_UNAVAILABLE_CODE,
            format!("instrument reference is missing required fee selector '{field}'"),
            false,
        ),
        FeeScheduleBootstrapError::ResponseInconsistent(message) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            ANALYSIS_EXACT_FEE_UNAVAILABLE_CODE,
            message,
            true,
        ),
        FeeScheduleBootstrapError::Normalize(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            ANALYSIS_INPUT_INCONSISTENT_CODE,
            error.to_string(),
            false,
        ),
    }
}

fn analysis_failure(
    request: &AgentRequest,
    generated_at: &str,
    status: AgentResponseStatus,
    error: AnalysisError,
) -> AgentResponse {
    failure_response(
        request,
        generated_at,
        status,
        ANALYSIS_INPUT_INCONSISTENT_CODE,
        error.to_string(),
        false,
    )
}

const fn analysis_liquidity_role(role: ProtocolLiquidityRole) -> AnalysisLiquidityRole {
    match role {
        ProtocolLiquidityRole::Maker => AnalysisLiquidityRole::Maker,
        ProtocolLiquidityRole::Taker => AnalysisLiquidityRole::Taker,
    }
}

fn data_quality(quality: MarketReadiness) -> DataQuality {
    match quality {
        MarketReadiness::NotReady => DataQuality::NotReady,
        MarketReadiness::Fresh => DataQuality::Fresh,
        MarketReadiness::Stale => DataQuality::Stale,
        MarketReadiness::Degraded => DataQuality::Degraded,
    }
}

fn utc_now_ms() -> u64 {
    Utc::now().timestamp_millis().max(0) as u64
}

fn account_failure(
    request: &AgentRequest,
    generated_at: &str,
    error: AccountBootstrapError,
) -> AgentResponse {
    match error {
        AccountBootstrapError::PermissionRejected => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            ACCOUNT_OBSERVER_PERMISSION_REJECTED_CODE,
            "OKX observer API key must have read_only permission only".to_owned(),
            false,
        ),
        AccountBootstrapError::Api(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            ACCOUNT_PRIVATE_API_UNAVAILABLE_CODE,
            error.to_string(),
            true,
        ),
        AccountBootstrapError::Normalize(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            ACCOUNT_BOOTSTRAP_INCONSISTENT_CODE,
            error.to_string(),
            false,
        ),
    }
}

fn market_failure(
    request: &AgentRequest,
    generated_at: &str,
    error: MarketBootstrapError,
) -> AgentResponse {
    match error {
        MarketBootstrapError::ReferenceInstrumentNotFound(instrument) => {
            reference_not_found(request, generated_at, &instrument)
        }
        MarketBootstrapError::MissingUnderlying(instrument) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            MARKET_REFERENCE_INCOMPLETE_CODE,
            format!("instrument '{instrument}' has no underlying/index id in reference data"),
            false,
        ),
        MarketBootstrapError::UnknownFundingRequirement {
            instrument_id,
            rule_type,
        } => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            MARKET_REFERENCE_INCOMPLETE_CODE,
            format!(
                "instrument '{instrument_id}' has unknown funding semantics for ruleType '{rule_type}'"
            ),
            false,
        ),
        MarketBootstrapError::Api(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            MARKET_PUBLIC_API_UNAVAILABLE_CODE,
            error.to_string(),
            true,
        ),
        MarketBootstrapError::Normalize(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            MARKET_BOOTSTRAP_INCONSISTENT_CODE,
            error.to_string(),
            true,
        ),
        MarketBootstrapError::HistoryNormalize(MarketHistoryError::InstrumentNotFound(
            instrument,
        )) => reference_not_found(request, generated_at, &instrument),
        MarketBootstrapError::HistoryNormalize(MarketHistoryError::InstrumentNotLive(
            instrument,
        )) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            MARKET_INSTRUMENT_NOT_LIVE_CODE,
            format!("instrument '{instrument}' is not live"),
            false,
        ),
        MarketBootstrapError::HistoryNormalize(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            MARKET_HISTORY_INCONSISTENT_CODE,
            error.to_string(),
            true,
        ),
    }
}

fn reference_not_found(
    request: &AgentRequest,
    generated_at: &str,
    instrument: &str,
) -> AgentResponse {
    failure_response(
        request,
        generated_at,
        AgentResponseStatus::Rejected,
        REFERENCE_INSTRUMENT_NOT_FOUND_CODE,
        format!("instrument '{instrument}' is not present in the reference registry"),
        false,
    )
}

fn failure_response(
    request: &AgentRequest,
    generated_at: &str,
    status: AgentResponseStatus,
    code: &str,
    message: String,
    retryable: bool,
) -> AgentResponse {
    AgentResponse {
        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
        request_id: request.request_id.clone(),
        status,
        generated_at: generated_at.to_owned(),
        quality: DataQuality::NotReady,
        result_schema: None,
        result: None,
        failure: Some(AgentFailure {
            code: code.to_owned(),
            message,
            retryable,
        }),
        warnings: Vec::new(),
    }
}

fn unavailable(request: &AgentRequest, generated_at: &str) -> AgentResponse {
    failure_response(
        request,
        generated_at,
        AgentResponseStatus::Rejected,
        P1_NOT_AVAILABLE_CODE,
        "typed request accepted by the P1 runtime shell; domain operation is not connected yet"
            .to_owned(),
        false,
    )
}

fn decode_fixed<const N: usize>(value: &str) -> AgentResult<[u8; N]> {
    let bytes = STANDARD.decode(value)?;
    bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| AgentError::InvalidPrivateKeyLength(bytes.len()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_api::PublicInstrument;
    use okx_protocol::crypto::{
        derive_directional_key, encrypt, public_key_from_private, shared_secret,
    };

    #[tokio::test]
    async fn local_once_round_trip_returns_authenticated_terminal_response() {
        let agent_private = [1_u8; 32];
        let client_private = [2_u8; 32];
        let agent_public = public_key_from_private(agent_private);
        let client_public = public_key_from_private(client_private);
        let shared = shared_secret(client_private, agent_public).expect("shared");
        assert_eq!(
            shared_secret(agent_private, client_public).expect("shared"),
            shared
        );

        let request = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_0123456789abcdef".to_owned(),
            operation: AgentOperation::MarketSnapshot {
                instrument: "DOGE-USDT-SWAP".to_owned(),
            },
        };
        let request_plaintext = serde_json::to_vec(&request).expect("request json");
        let request_nonce = [3_u8; 12];
        let request_key = derive_directional_key(
            &shared,
            &request.request_id,
            "agent-key-1",
            MailboxDirection::ClientToAgent,
        )
        .expect("request key");

        let mut request_envelope = MailboxEnvelope {
            schema: MAILBOX_ENVELOPE_SCHEMA_V1.to_owned(),
            request_id: request.request_id.clone(),
            direction: MailboxDirection::ClientToAgent,
            agent_key_id: "agent-key-1".to_owned(),
            client_ephemeral_public_key: STANDARD.encode(client_public),
            nonce: STANDARD.encode(request_nonce),
            ciphertext: String::new(),
        };
        let aad = request_envelope.aad().expect("request aad");
        request_envelope.ciphertext = STANDARD.encode(
            encrypt(
                &request_key,
                &request_nonce,
                aad.as_bytes(),
                &request_plaintext,
            )
            .expect("encrypt request"),
        );

        let response_nonce = [4_u8; 12];
        let response_envelope = process_once(
            &request_envelope,
            "agent-key-1",
            &agent_private,
            ObservationQueryContext::unavailable(),
            response_nonce,
            "2026-09-26T18:00:00.000Z",
        )
        .await
        .expect("process once");

        let response_key = derive_directional_key(
            &shared,
            &request.request_id,
            "agent-key-1",
            MailboxDirection::AgentToClient,
        )
        .expect("response key");
        let response_aad = response_envelope.aad().expect("response aad");
        let response_ciphertext = STANDARD
            .decode(&response_envelope.ciphertext)
            .expect("response ciphertext");
        let response_plaintext = decrypt(
            &response_key,
            &response_nonce,
            response_aad.as_bytes(),
            &response_ciphertext,
        )
        .expect("decrypt response");
        let response: AgentResponse =
            serde_json::from_slice(&response_plaintext).expect("response json");

        assert_eq!(response.request_id, request.request_id);
        assert_eq!(response.status, AgentResponseStatus::Rejected);
        assert_eq!(response.quality, DataQuality::NotReady);
        assert_eq!(
            response.failure.expect("failure").code,
            P1_NOT_AVAILABLE_CODE
        );
    }

    #[tokio::test]
    async fn instrument_rules_uses_reference_registry_and_reports_bootstrap_quality() {
        let request = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_rules_0123456789".to_owned(),
            operation: AgentOperation::InstrumentRules {
                instrument: "DOGE-USDT-SWAP".to_owned(),
            },
        };
        let registry = reference();

        let response = response_for(
            &request,
            ObservationQueryContext {
                standalone_reference: Some(&registry),
                market_fallback: None,
                public_ws: None,
                mailbox_telemetry: None,
                account_fallback: None,
                private_ws: None,
            },
            "2026-09-27T00:00:01.000Z",
        )
        .await
        .expect("response");

        assert_eq!(response.status, AgentResponseStatus::Completed);
        assert_eq!(response.quality, DataQuality::Degraded);
        assert_eq!(
            response.result_schema.as_deref(),
            Some(INSTRUMENT_RULES_SCHEMA_V1)
        );
        assert!(response.failure.is_none());
        assert_eq!(response.warnings, vec![REFERENCE_BOOTSTRAP_WARNING]);
    }

    #[tokio::test]
    async fn find_instruments_uses_reference_registry_without_guessing_ids() {
        let request = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_find_012345678901".to_owned(),
            operation: AgentOperation::FindInstruments {
                asset: "DOGE".to_owned(),
                settle_currency: Some("USDT".to_owned()),
                instrument_type: Some(InstrumentTypeFilter::Swap),
            },
        };
        let registry = reference();

        let response = response_for(
            &request,
            ObservationQueryContext {
                standalone_reference: Some(&registry),
                market_fallback: None,
                public_ws: None,
                mailbox_telemetry: None,
                account_fallback: None,
                private_ws: None,
            },
            "2026-09-27T00:00:01.000Z",
        )
        .await
        .expect("response");

        assert_eq!(response.status, AgentResponseStatus::Completed);
        assert_eq!(
            response.result_schema.as_deref(),
            Some(INSTRUMENT_SEARCH_SCHEMA_V1)
        );
        let result = response.result.expect("result");
        assert_eq!(
            result["instruments"].as_array().expect("instruments").len(),
            1
        );
        assert_eq!(result["instruments"][0]["instrument_id"], "DOGE-USDT-SWAP");
    }

    #[tokio::test]
    async fn snapshot_quality_explains_rest_only_m2_state() {
        let request = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_quality_012345678".to_owned(),
            operation: AgentOperation::SnapshotQuality {
                instrument: "DOGE-USDT-SWAP".to_owned(),
            },
        };
        let registry = reference();

        let response = response_for(
            &request,
            ObservationQueryContext {
                standalone_reference: Some(&registry),
                market_fallback: None,
                public_ws: None,
                mailbox_telemetry: None,
                account_fallback: None,
                private_ws: None,
            },
            "2026-09-27T00:00:01.000Z",
        )
        .await
        .expect("response");

        assert_eq!(response.status, AgentResponseStatus::Completed);
        assert_eq!(response.quality, DataQuality::Degraded);
        assert_eq!(
            response.result_schema.as_deref(),
            Some(SNAPSHOT_QUALITY_SCHEMA_V1)
        );
        assert_eq!(
            response.result.expect("result")["reason"],
            "M2_REST_BOOTSTRAP_ONLY"
        );
    }

    fn reference() -> ReferenceRegistry {
        ReferenceRegistry::from_public("2026-09-27T00:00:00.000Z", vec![swap()]).expect("registry")
    }

    fn swap() -> PublicInstrument {
        PublicInstrument {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            instrument_family: "DOGE-USDT".to_owned(),
            underlying: "DOGE-USDT".to_owned(),
            state: "live".to_owned(),
            rule_type: "normal".to_owned(),
            base_currency: String::new(),
            quote_currency: String::new(),
            settle_currency: "USDT".to_owned(),
            tick_size: "0.00001".to_owned(),
            lot_size: "0.01".to_owned(),
            min_size: "0.01".to_owned(),
            max_limit_size: "1000000".to_owned(),
            max_market_size: "100000".to_owned(),
            max_limit_amount: String::new(),
            max_market_amount: String::new(),
            contract_type: "linear".to_owned(),
            contract_value: "1000".to_owned(),
            contract_value_currency: "DOGE".to_owned(),
            fee_group_id: "4".to_owned(),
            lever: "50".to_owned(),
            list_time: "1700000000000".to_owned(),
            expiry_time: String::new(),
        }
    }
}

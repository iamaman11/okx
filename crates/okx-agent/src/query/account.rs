use super::*;

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

use super::*;

pub(super) async fn dispatch(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
) -> AgentResult<AgentResponse> {
    match &request.operation {
        AgentOperation::PositionScenario {
            instrument,
            side,
            contracts,
            entry_price,
            exit_price,
            entry_move_ratio,
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
            let exit = match (exit_price, entry_move_ratio) {
                (Some(exit_price), None) => ScenarioExitAssumption::Price {
                    price: exit_price.clone(),
                },
                (None, Some(entry_move_ratio)) => ScenarioExitAssumption::EntryMoveRatio {
                    ratio: entry_move_ratio.clone(),
                },
                _ => {
                    return Ok(failure_response(
                        request,
                        generated_at,
                        AgentResponseStatus::Rejected,
                        ANALYSIS_INPUT_INCONSISTENT_CODE,
                        "position scenario requires exactly one exit_price or entry_move_ratio"
                            .to_owned(),
                        false,
                    ));
                }
            };
            let assumptions = PositionScenarioAssumptions {
                direction: match side {
                    PositionSide::Long => PositionDirection::Long,
                    PositionSide::Short => PositionDirection::Short,
                },
                contracts: contracts.clone(),
                entry_price: entry_price.clone(),
                exit,
                entry_liquidity_role: analysis_liquidity_role(*entry_liquidity_role),
                exit_liquidity_role: analysis_liquidity_role(*exit_liquidity_role),
            };
            let result = match analyze_position_scenario(&rules, &fees, &assumptions) {
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
                result_schema: Some(POSITION_SCENARIO_SCHEMA_V1.to_owned()),
                result: Some(serde_json::to_value(result)?),
                failure: None,
                warnings: vec![POSITION_SCENARIO_EXPLICIT_ASSUMPTIONS_WARNING.to_owned()],
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
        _ => unreachable!("query domain dispatcher received unsupported operation"),
    }
}

use std::str::FromStr;

use okx_analysis::{
    CandidateOrderAnalysis, PortfolioRiskAnalysis, PositionDirection, RiskPolicyDecision,
};
use okx_observation::{
    ACCOUNT_SNAPSHOT_SCHEMA_V2, AccountSnapshot, InstrumentRulesSnapshot, VenueExecutionEvidence,
};
use rust_decimal::Decimal;
use thiserror::Error;

use crate::{
    EXECUTION_PLAN_SCHEMA_V1, ExecutionAction, ExecutionIntent, ExecutionPlan, OpenRiskEvidence,
    OrderSide, PositionSide, derive_client_order_id,
    model::{order_side, valid_intent_id},
};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExecutionValidationError {
    #[error("invalid execution intent id")]
    InvalidIntentId,

    #[error("execution intent instrument does not match reference rules")]
    InstrumentMismatch,

    #[error("reference generation changed")]
    ReferenceGenerationMismatch,

    #[error("account generation changed")]
    AccountGenerationMismatch,

    #[error("account snapshot is not private-WS converged")]
    AccountNotConverged,

    #[error("execution account UID fingerprint is missing")]
    MissingAccountIdentity,

    #[error("execution account identity changed")]
    AccountIdentityMismatch,

    #[error("execution plan side does not match action/position side")]
    PlanSideMismatch,

    #[error("opening execution fee schedule changed")]
    FeeGenerationMismatch,

    #[error("opening execution requires current exact fee evidence")]
    CurrentFeeEvidenceUnavailable,

    #[error("unsupported account level '{0}', expected Futures mode level 2")]
    UnsupportedAccountLevel(String),

    #[error("unsupported position mode '{0}', expected long_short_mode")]
    UnsupportedPositionMode(String),

    #[error("instrument '{0}' is not trade-ready live")]
    InstrumentNotLive(String),

    #[error("fresh public instrument evidence differs from the live reference generation")]
    FreshReferenceMismatch,

    #[error("fresh venue evidence refers to a different instrument")]
    VenueEvidenceInstrumentMismatch,

    #[error("account instrument '{0}' is not currently live")]
    AccountInstrumentNotLive(String),

    #[error(
        "upcoming exchange rule '{param}' becomes effective at {effective_time_ms} before mutation deadline {mutation_deadline_ms}"
    )]
    UpcomingRuleChangeInsideMutationWindow {
        param: String,
        effective_time_ms: u64,
        mutation_deadline_ms: u64,
    },

    #[error(
        "OKX reports {0} ongoing system status event(s); venue mutation eligibility is not proven"
    )]
    OngoingSystemStatus(usize),

    #[error("buy price '{price}' exceeds current OKX buy limit '{limit}'")]
    PriceAboveCurrentLimit { price: String, limit: String },

    #[error("sell price '{price}' is below current OKX sell limit '{limit}'")]
    PriceBelowCurrentLimit { price: String, limit: String },

    #[error("opening execution requires current OKX maximum-order-size evidence")]
    MissingCurrentMaxOrderSize,

    #[error("opening size '{size}' exceeds current OKX maximum '{max_size}' for the order side")]
    ExceedsCurrentMaxOrderSize { size: String, max_size: String },

    #[error("invalid decimal field '{field}': '{value}'")]
    InvalidDecimal { field: &'static str, value: String },

    #[error("decimal field '{0}' must be positive")]
    NonPositive(&'static str),

    #[error("execution size '{size}' is below minimum '{min_size}'")]
    BelowMinimumSize { size: String, min_size: String },

    #[error("execution size '{size}' is not aligned to lot size '{lot_size}'")]
    SizeNotLotAligned { size: String, lot_size: String },

    #[error("reference rules are missing the applicable maximum order size")]
    MissingMaximumSize,

    #[error("execution size '{size}' exceeds maximum '{max_size}'")]
    ExceedsMaximumSize { size: String, max_size: String },

    #[error("execution price '{price}' is not aligned to tick size '{tick_size}'")]
    PriceNotTickAligned { price: String, tick_size: String },

    #[error("opening execution requires accepted CandidateOrderAnalysis")]
    MissingOpenRiskEvidence,

    #[error("closing execution must not carry opening CandidateOrderAnalysis")]
    UnexpectedOpenRiskEvidence,

    #[error("candidate order evidence does not match execution intent")]
    CandidateMismatch,

    #[error("candidate order risk invariants are not satisfied")]
    CandidateRiskInvariantViolation,

    #[error("no matching open position exists for close execution")]
    NoClosablePosition,

    #[error("account position value is invalid for close execution")]
    InvalidPositionValue,

    #[error("pending close order state is invalid")]
    InvalidPendingCloseOrder,

    #[error("requested close size '{requested}' exceeds unreserved position size '{available}'")]
    CloseSizeExceedsAvailable {
        requested: String,
        available: String,
    },

    #[error("prepared execution is missing an immutable hard-risk policy binding")]
    MissingRiskBinding,

    #[error("hard-risk policy rejected risk-increasing mutation: {0}")]
    HardRiskPolicyRejected(String),

    #[error("portfolio risk analysis does not match the immutable execution risk binding")]
    RiskBindingMismatch,

    #[error("portfolio risk candidate does not match the immutable execution plan/current leverage")]
    RiskCandidateMismatch,

    #[error("portfolio risk analysis does not match the current account generation")]
    RiskAccountGenerationMismatch,

    #[error("portfolio risk decision/violation set is internally inconsistent")]
    RiskAnalysisInconsistent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreMutationRiskDisposition {
    Accepted,
    AcceptedRiskReducingClose,
}

pub fn prepare_execution(
    intent: &ExecutionIntent,
    rules: &InstrumentRulesSnapshot,
    account: &AccountSnapshot,
    candidate: Option<&CandidateOrderAnalysis>,
) -> Result<ExecutionPlan, ExecutionValidationError> {
    if !valid_intent_id(&intent.intent_id) {
        return Err(ExecutionValidationError::InvalidIntentId);
    }

    validate_current_authorities(
        &intent.instrument_id,
        &intent.expected_reference_generation,
        Some(&intent.expected_account_generation),
        None,
        rules,
        account,
    )?;
    let (size, price) = validate_order_mechanics(&intent.size, &intent.price, rules)?;

    let side = order_side(intent.action, intent.position_side);
    let open_risk = match intent.action {
        ExecutionAction::Open => Some(validate_candidate(
            intent,
            rules,
            candidate.ok_or(ExecutionValidationError::MissingOpenRiskEvidence)?,
            size,
            price,
        )?),
        ExecutionAction::Close => {
            if candidate.is_some() {
                return Err(ExecutionValidationError::UnexpectedOpenRiskEvidence);
            }
            validate_close_capacity(intent, account, size, side)?;
            None
        }
    };

    Ok(ExecutionPlan {
        schema: EXECUTION_PLAN_SCHEMA_V1.to_owned(),
        intent_id: intent.intent_id.clone(),
        client_order_id: derive_client_order_id(&intent.intent_id),
        reference_generation: rules.reference_generation.clone(),
        account_generation: account.account_generation.clone(),
        account_uid_fingerprint: account.account_uid_fingerprint.clone(),
        instrument_id: intent.instrument_id.clone(),
        trade_mode: intent.trade_mode,
        side,
        position_side: intent.position_side,
        action: intent.action,
        order_type: intent.order_type,
        size: normalized(size),
        price: normalized(price),
        open_risk,
        risk_binding: None,
    })
}

pub fn revalidate_hard_risk_policy(
    plan: &ExecutionPlan,
    analysis: &PortfolioRiskAnalysis,
    current_account_generation: &str,
    current_configured_leverage: Option<&str>,
) -> Result<PreMutationRiskDisposition, ExecutionValidationError> {
    let binding = plan
        .risk_binding
        .as_ref()
        .ok_or(ExecutionValidationError::MissingRiskBinding)?;
    if analysis.mandate != binding.mandate || analysis.policy != binding.policy {
        return Err(ExecutionValidationError::RiskBindingMismatch);
    }
    if analysis.account.account_generation != current_account_generation {
        return Err(ExecutionValidationError::RiskAccountGenerationMismatch);
    }

    match (analysis.policy_decision, analysis.violations.is_empty()) {
        (RiskPolicyDecision::Accepted, true) | (RiskPolicyDecision::Rejected, false) => {}
        _ => return Err(ExecutionValidationError::RiskAnalysisInconsistent),
    }

    match (plan.action, analysis.candidate.as_ref()) {
        (ExecutionAction::Open, Some(candidate)) => {
            let open_risk = plan
                .open_risk
                .as_ref()
                .ok_or(ExecutionValidationError::MissingOpenRiskEvidence)?;
            let current_leverage = current_configured_leverage
                .filter(|value| !value.trim().is_empty())
                .ok_or(ExecutionValidationError::RiskCandidateMismatch)?;
            let expected_direction = match plan.position_side {
                PositionSide::Long => PositionDirection::Long,
                PositionSide::Short => PositionDirection::Short,
            };
            if candidate.instrument != plan.instrument_id
                || candidate.direction != expected_direction
                || !candidate.additive_new_risk
                || decimal("risk_candidate.notional_usd", &candidate.notional_usd)?
                    != decimal(
                        "open_risk.entry_settle_notional",
                        &open_risk.entry_settle_notional,
                    )?
                || decimal(
                    "risk_candidate.worst_case_loss_usd",
                    &candidate.worst_case_loss_usd,
                )? != decimal("open_risk.stop_loss_settle", &open_risk.stop_loss_settle)?
                || decimal("risk_candidate.leverage", &candidate.leverage)?
                    != decimal("current_configured_leverage", current_leverage)?
            {
                return Err(ExecutionValidationError::RiskCandidateMismatch);
            }
        }
        (ExecutionAction::Close, None) => {}
        _ => return Err(ExecutionValidationError::RiskCandidateMismatch),
    }

    let codes = analysis
        .violations
        .iter()
        .map(|violation| violation.code)
        .collect::<Vec<_>>()
        .join(",");
    hard_risk_disposition(plan.action, analysis.policy_decision, codes)
}

fn hard_risk_disposition(
    action: ExecutionAction,
    decision: RiskPolicyDecision,
    violation_codes: String,
) -> Result<PreMutationRiskDisposition, ExecutionValidationError> {
    match action {
        ExecutionAction::Open if decision == RiskPolicyDecision::Rejected => Err(
            ExecutionValidationError::HardRiskPolicyRejected(violation_codes),
        ),
        ExecutionAction::Open => Ok(PreMutationRiskDisposition::Accepted),
        ExecutionAction::Close => Ok(PreMutationRiskDisposition::AcceptedRiskReducingClose),
    }
}

pub fn revalidate_execution_plan(
    plan: &ExecutionPlan,
    rules: &InstrumentRulesSnapshot,
    account: &AccountSnapshot,
    current_fee_generation: Option<&str>,
) -> Result<(), ExecutionValidationError> {
    validate_current_authorities(
        &plan.instrument_id,
        &plan.reference_generation,
        None,
        Some(&plan.account_uid_fingerprint),
        rules,
        account,
    )?;

    if plan.side != order_side(plan.action, plan.position_side) {
        return Err(ExecutionValidationError::PlanSideMismatch);
    }

    let (size, _) = validate_order_mechanics(&plan.size, &plan.price, rules)?;

    match plan.action {
        ExecutionAction::Open => {
            let evidence = plan
                .open_risk
                .as_ref()
                .ok_or(ExecutionValidationError::MissingOpenRiskEvidence)?;
            let current_fee_generation = current_fee_generation
                .filter(|value| !value.trim().is_empty())
                .ok_or(ExecutionValidationError::CurrentFeeEvidenceUnavailable)?;
            if evidence.fee_generation != current_fee_generation {
                return Err(ExecutionValidationError::FeeGenerationMismatch);
            }
        }
        ExecutionAction::Close => {
            if plan.open_risk.is_some() {
                return Err(ExecutionValidationError::UnexpectedOpenRiskEvidence);
            }
            let intent = ExecutionIntent {
                intent_id: plan.intent_id.clone(),
                expected_reference_generation: plan.reference_generation.clone(),
                expected_account_generation: plan.account_generation.clone(),
                instrument_id: plan.instrument_id.clone(),
                trade_mode: plan.trade_mode,
                position_side: plan.position_side,
                action: plan.action,
                order_type: plan.order_type,
                size: plan.size.clone(),
                price: plan.price.clone(),
            };
            validate_close_capacity(&intent, account, size, plan.side)?;
        }
    }

    Ok(())
}

pub fn revalidate_venue_execution(
    plan: &ExecutionPlan,
    rules: &InstrumentRulesSnapshot,
    evidence: &VenueExecutionEvidence,
    mutation_deadline_ms: u64,
) -> Result<(), ExecutionValidationError> {
    if !evidence.ongoing_system_statuses.is_empty() {
        return Err(ExecutionValidationError::OngoingSystemStatus(
            evidence.ongoing_system_statuses.len(),
        ));
    }

    if evidence.public_instrument.instrument_id != plan.instrument_id
        || evidence.price_limit.instrument_id != plan.instrument_id
        || evidence
            .max_order_size
            .as_ref()
            .is_some_and(|value| value.instrument_id != plan.instrument_id)
    {
        return Err(ExecutionValidationError::VenueEvidenceInstrumentMismatch);
    }

    if evidence.public_instrument != rules.instrument {
        return Err(ExecutionValidationError::FreshReferenceMismatch);
    }

    for change in &evidence.public_instrument.upcoming_rule_changes {
        let effective_time_ms = change.effective_time_ms.parse::<u64>().map_err(|_| {
            ExecutionValidationError::InvalidDecimal {
                field: "upcoming_rule_change.effective_time_ms",
                value: change.effective_time_ms.clone(),
            }
        })?;
        if effective_time_ms <= mutation_deadline_ms {
            return Err(
                ExecutionValidationError::UpcomingRuleChangeInsideMutationWindow {
                    param: change.param.clone(),
                    effective_time_ms,
                    mutation_deadline_ms,
                },
            );
        }
    }

    if evidence.account_instrument.state != "live" {
        return Err(ExecutionValidationError::AccountInstrumentNotLive(
            plan.instrument_id.clone(),
        ));
    }

    let price = positive_decimal("price", &plan.price)?;
    match plan.side {
        OrderSide::Buy => {
            let limit = positive_decimal("buy_limit", &evidence.price_limit.buy_limit)?;
            if price > limit {
                return Err(ExecutionValidationError::PriceAboveCurrentLimit {
                    price: normalized(price),
                    limit: normalized(limit),
                });
            }
        }
        OrderSide::Sell => {
            let limit = positive_decimal("sell_limit", &evidence.price_limit.sell_limit)?;
            if price < limit {
                return Err(ExecutionValidationError::PriceBelowCurrentLimit {
                    price: normalized(price),
                    limit: normalized(limit),
                });
            }
        }
    }

    if plan.action == ExecutionAction::Open {
        let size = positive_decimal("size", &plan.size)?;
        let max_order_size = evidence
            .max_order_size
            .as_ref()
            .ok_or(ExecutionValidationError::MissingCurrentMaxOrderSize)?;
        let max_size_text = match plan.side {
            OrderSide::Buy => &max_order_size.max_buy,
            OrderSide::Sell => &max_order_size.max_sell,
        };
        let max_size = decimal("current_max_order_size", max_size_text)?;
        if max_size < Decimal::ZERO {
            return Err(ExecutionValidationError::InvalidDecimal {
                field: "current_max_order_size",
                value: max_size_text.clone(),
            });
        }
        if size > max_size {
            return Err(ExecutionValidationError::ExceedsCurrentMaxOrderSize {
                size: normalized(size),
                max_size: normalized(max_size),
            });
        }
    }

    Ok(())
}

fn validate_current_authorities(
    instrument_id: &str,
    expected_reference_generation: &str,
    expected_account_generation: Option<&str>,
    expected_account_uid_fingerprint: Option<&str>,
    rules: &InstrumentRulesSnapshot,
    account: &AccountSnapshot,
) -> Result<(), ExecutionValidationError> {
    if instrument_id != rules.instrument.instrument_id {
        return Err(ExecutionValidationError::InstrumentMismatch);
    }
    if expected_reference_generation != rules.reference_generation {
        return Err(ExecutionValidationError::ReferenceGenerationMismatch);
    }
    if let Some(expected_account_generation) = expected_account_generation
        && expected_account_generation != account.account_generation
    {
        return Err(ExecutionValidationError::AccountGenerationMismatch);
    }
    if account.schema != ACCOUNT_SNAPSHOT_SCHEMA_V2 || !account.private_ws_connected {
        return Err(ExecutionValidationError::AccountNotConverged);
    }
    if account.account_uid_fingerprint.trim().is_empty() {
        return Err(ExecutionValidationError::MissingAccountIdentity);
    }
    if let Some(expected) = expected_account_uid_fingerprint
        && expected != account.account_uid_fingerprint
    {
        return Err(ExecutionValidationError::AccountIdentityMismatch);
    }
    if account.account_level != "2" {
        return Err(ExecutionValidationError::UnsupportedAccountLevel(
            account.account_level.clone(),
        ));
    }
    if account.position_mode != "long_short_mode" {
        return Err(ExecutionValidationError::UnsupportedPositionMode(
            account.position_mode.clone(),
        ));
    }
    if rules.instrument.state != "live" {
        return Err(ExecutionValidationError::InstrumentNotLive(
            rules.instrument.instrument_id.clone(),
        ));
    }
    Ok(())
}

fn validate_order_mechanics(
    size_text: &str,
    price_text: &str,
    rules: &InstrumentRulesSnapshot,
) -> Result<(Decimal, Decimal), ExecutionValidationError> {
    let size = positive_decimal("size", size_text)?;
    let lot_size = positive_decimal("lot_size", &rules.instrument.lot_size)?;
    let min_size = positive_decimal("min_size", &rules.instrument.min_size)?;
    if size < min_size {
        return Err(ExecutionValidationError::BelowMinimumSize {
            size: normalized(size),
            min_size: normalized(min_size),
        });
    }
    if size % lot_size != Decimal::ZERO {
        return Err(ExecutionValidationError::SizeNotLotAligned {
            size: normalized(size),
            lot_size: normalized(lot_size),
        });
    }

    let max_size_text = rules
        .instrument
        .max_limit_size
        .as_deref()
        .ok_or(ExecutionValidationError::MissingMaximumSize)?;
    let max_size = positive_decimal("max_limit_size", max_size_text)?;
    if size > max_size {
        return Err(ExecutionValidationError::ExceedsMaximumSize {
            size: normalized(size),
            max_size: normalized(max_size),
        });
    }

    let price = positive_decimal("price", price_text)?;
    let tick_size = positive_decimal("tick_size", &rules.instrument.tick_size)?;
    if price % tick_size != Decimal::ZERO {
        return Err(ExecutionValidationError::PriceNotTickAligned {
            price: normalized(price),
            tick_size: normalized(tick_size),
        });
    }

    Ok((size, price))
}

fn validate_candidate(
    intent: &ExecutionIntent,
    rules: &InstrumentRulesSnapshot,
    candidate: &CandidateOrderAnalysis,
    size: Decimal,
    price: Decimal,
) -> Result<OpenRiskEvidence, ExecutionValidationError> {
    let expected_direction = match intent.position_side {
        PositionSide::Long => PositionDirection::Long,
        PositionSide::Short => PositionDirection::Short,
    };

    let candidate_size = decimal("candidate.contracts", &candidate.contracts)?;
    let candidate_entry = decimal("candidate.entry_price", &candidate.entry_price)?;

    if candidate.instrument_id != intent.instrument_id
        || candidate.reference_generation != rules.reference_generation
        || candidate.direction != expected_direction
        || candidate_size != size
        || candidate_entry != price
    {
        return Err(ExecutionValidationError::CandidateMismatch);
    }

    let entry_notional = positive_decimal(
        "candidate.entry_settle_notional",
        &candidate.entry_settle_notional,
    )?;
    let max_notional = positive_decimal(
        "candidate.requested_max_settle_notional",
        &candidate.requested_max_settle_notional,
    )?;
    let stop_loss = positive_decimal("candidate.stop_loss_settle", &candidate.stop_loss_settle)?;
    let max_loss = positive_decimal(
        "candidate.requested_max_loss_settle",
        &candidate.requested_max_loss_settle,
    )?;
    let actual_rr = positive_decimal("candidate.actual_target_rr", &candidate.actual_target_rr)?;
    let requested_rr = positive_decimal(
        "candidate.requested_target_rr",
        &candidate.requested_target_rr,
    )?;

    if entry_notional > max_notional || stop_loss > max_loss || actual_rr < requested_rr {
        return Err(ExecutionValidationError::CandidateRiskInvariantViolation);
    }

    Ok(OpenRiskEvidence {
        fee_generation: candidate.fee_generation.clone(),
        requested_max_settle_notional: candidate.requested_max_settle_notional.clone(),
        requested_max_loss_settle: candidate.requested_max_loss_settle.clone(),
        requested_target_rr: candidate.requested_target_rr.clone(),
        stop_price: candidate.stop_price.clone(),
        target_price: candidate.target_price.clone(),
        entry_settle_notional: candidate.entry_settle_notional.clone(),
        stop_loss_settle: candidate.stop_loss_settle.clone(),
        actual_target_rr: candidate.actual_target_rr.clone(),
    })
}

fn validate_close_capacity(
    intent: &ExecutionIntent,
    account: &AccountSnapshot,
    requested: Decimal,
    closing_side: OrderSide,
) -> Result<(), ExecutionValidationError> {
    let matching_positions = account
        .positions
        .iter()
        .filter(|position| {
            position.instrument_id == intent.instrument_id
                && position.position_side == intent.position_side.as_str()
                && position.margin_mode == intent.trade_mode.as_str()
        })
        .collect::<Vec<_>>();

    if matching_positions.len() != 1 {
        return Err(ExecutionValidationError::NoClosablePosition);
    }

    let position = decimal("position", &matching_positions[0].position)?;
    if position <= Decimal::ZERO {
        return Err(ExecutionValidationError::InvalidPositionValue);
    }

    let mut reserved = Decimal::ZERO;
    for order in account.pending_orders.iter().filter(|order| {
        order.instrument_id == intent.instrument_id
            && order.position_side.as_deref() == Some(intent.position_side.as_str())
            && order.trade_mode == intent.trade_mode.as_str()
            && order.side == closing_side.as_str()
    }) {
        let order_size = decimal("pending_order.size", &order.size)?;
        let filled = decimal(
            "pending_order.accumulated_fill_size",
            &order.accumulated_fill_size,
        )?;
        if order_size < Decimal::ZERO || filled < Decimal::ZERO || filled > order_size {
            return Err(ExecutionValidationError::InvalidPendingCloseOrder);
        }
        reserved += order_size - filled;
    }

    let available = (position - reserved).max(Decimal::ZERO);
    if requested > available {
        return Err(ExecutionValidationError::CloseSizeExceedsAvailable {
            requested: normalized(requested),
            available: normalized(available),
        });
    }
    Ok(())
}

fn decimal(field: &'static str, value: &str) -> Result<Decimal, ExecutionValidationError> {
    if value.is_empty() || value.len() > 64 {
        return Err(ExecutionValidationError::InvalidDecimal {
            field,
            value: value.to_owned(),
        });
    }
    Decimal::from_str(value).map_err(|_| ExecutionValidationError::InvalidDecimal {
        field,
        value: value.to_owned(),
    })
}

fn positive_decimal(field: &'static str, value: &str) -> Result<Decimal, ExecutionValidationError> {
    let value = decimal(field, value)?;
    if value <= Decimal::ZERO {
        Err(ExecutionValidationError::NonPositive(field))
    } else {
        Ok(value)
    }
}

fn normalized(value: Decimal) -> String {
    value.normalize().to_string()
}

#[cfg(test)]
mod tests {
    use okx_analysis::{
        CandidateOrderAssumptions, HARD_RISK_POLICY_SCHEMA_V1, HardRiskPolicy, LiquidityRole,
        PortfolioCandidate, RiskDegradedMode, RiskMinimumQuality, TRADING_MANDATE_SCHEMA_V1,
        TradingMandate, analyze_candidate_order, analyze_portfolio_risk,
    };
    use okx_api::InstrumentType;
    use okx_observation::{
        ACCOUNT_CONVERGED_SOURCE_V2, ACCOUNT_SNAPSHOT_SCHEMA_V2, AccountAuthorityEvidence,
        AccountBalanceState, AccountInstrumentExecutionLimits, AccountLedgerSummary,
        AccountPositionState, FeeScheduleInput, FeeScheduleSnapshot, InstrumentSpec,
        M4_REST_WS_CONVERGED_REASON, MaxOrderSizeEvidence, PendingOrderState, PriceLimitEvidence,
        SystemStatusEvidence, VENUE_EXECUTION_EVIDENCE_SCHEMA_V1, VenueExecutionEvidence,
    };

    use super::*;
    use crate::{ExecutionAction, OrderType, PositionSide, TradeMode};

    #[test]
    fn hard_risk_gate_blocks_rejected_open_but_never_traps_risk_reducing_close() {
        assert!(matches!(
            hard_risk_disposition(
                ExecutionAction::Open,
                RiskPolicyDecision::Rejected,
                "MAX_DRAWDOWN".to_owned(),
            ),
            Err(ExecutionValidationError::HardRiskPolicyRejected(codes))
                if codes == "MAX_DRAWDOWN"
        ));
        assert_eq!(
            hard_risk_disposition(
                ExecutionAction::Open,
                RiskPolicyDecision::Accepted,
                String::new(),
            )
            .expect("accepted open"),
            PreMutationRiskDisposition::Accepted
        );
        assert_eq!(
            hard_risk_disposition(
                ExecutionAction::Close,
                RiskPolicyDecision::Rejected,
                "MAX_DAILY_REALIZED_LOSS".to_owned(),
            )
            .expect("risk-reducing close"),
            PreMutationRiskDisposition::AcceptedRiskReducingClose
        );
    }

    fn execution_risk_binding() -> crate::ExecutionRiskBinding {
        crate::ExecutionRiskBinding {
            mandate: TradingMandate {
                schema: TRADING_MANDATE_SCHEMA_V1.to_owned(),
                version: "execution-mandate/v1".to_owned(),
                capital_base_usd: "10000".to_owned(),
                decision_horizon_hours: 24,
                benchmark: Some("none/v1".to_owned()),
                allowed_instruments: vec!["DOGE-USDT-SWAP".to_owned()],
                max_drawdown_ratio: "1".to_owned(),
                leverage_ceiling: "10".to_owned(),
                minimum_liquidity_notional_usd: "0".to_owned(),
                max_turnover_ratio: "10".to_owned(),
            },
            policy: HardRiskPolicy {
                schema: HARD_RISK_POLICY_SCHEMA_V1.to_owned(),
                version: "execution-policy/v1".to_owned(),
                max_account_gross_notional_usd: "100000".to_owned(),
                max_instrument_gross_notional_usd: "100000".to_owned(),
                max_margin_utilization_ratio: "1".to_owned(),
                max_loss_per_trade_usd: "1000".to_owned(),
                max_daily_realized_loss_usd: "1000".to_owned(),
                max_drawdown_ratio: "1".to_owned(),
                max_leverage: "10".to_owned(),
                allowed_instruments: vec!["DOGE-USDT-SWAP".to_owned()],
                minimum_quality: RiskMinimumQuality::Fresh,
                degraded_mode: RiskDegradedMode::Reject,
                correlated_clusters: Vec::new(),
            },
        }
    }

    fn risk_ledger(account: &AccountSnapshot) -> AccountLedgerSummary {
        AccountLedgerSummary {
            schema: "okx.account-ledger-summary/v1",
            source_received_at: "2026-10-03T00:00:01Z".to_owned(),
            current_account_as_of_ms: Some("1790985600000".to_owned()),
            account_generation: account.account_generation.clone(),
            authority: AccountAuthorityEvidence {
                scope: "authenticated_account_only",
                account_type: account.account_type.clone(),
                is_subaccount: true,
                account_uid_fingerprint: account.account_uid_fingerprint.clone(),
                main_account_uid_fingerprint: Some("main".to_owned()),
                api_key_permissions: vec!["read_only".to_owned()],
                multi_account_inventory_complete: false,
            },
            total_equity_usd: account.balance.total_equity_usd.clone(),
            trading_equity_detail_usd_sum: account.balance.total_equity_usd.clone(),
            trading_equity_residual_usd: "0".to_owned(),
            funding_balances: Vec::new(),
            open_positions: account.positions.len(),
            pending_orders: account.pending_orders.len(),
            current_unrealized_pnl: Vec::new(),
            history_coverage: Vec::new(),
            realized_pnl_basis: "positions-history.realizedPnl",
            realized_pnl: Vec::new(),
            daily_realized_pnl_utc_basis: "positions-history.realizedPnl filtered by UTC day",
            daily_realized_pnl_utc_day_start_ms: Some("1790985600000".to_owned()),
            daily_realized_pnl_utc_day_end_ms: Some("1791072000000".to_owned()),
            daily_realized_pnl_utc: Vec::new(),
            trade_fee_basis: "fills-history.fee",
            trade_fees: Vec::new(),
            funding_basis: "bills-archive funding",
            funding: Vec::new(),
            position_pnl_identity_rows_checked: 0,
            fill_order_links_checked: 0,
            fill_order_links_unresolved_due_to_truncation: 0,
        }
    }

    #[test]
    fn hard_risk_revalidation_rejects_analysis_from_another_binding_or_candidate() {
        let rules = rules();
        let account = account();
        let candidate = open_candidate(&rules, PositionDirection::Long);
        let intent = open_intent(&rules, &account, &candidate, PositionSide::Long);
        let mut plan =
            prepare_execution(&intent, &rules, &account, Some(&candidate)).expect("execution plan");
        let binding = execution_risk_binding();
        plan.risk_binding = Some(binding.clone());

        let analysis = analyze_portfolio_risk(
            &account,
            &risk_ledger(&account),
            binding.mandate.clone(),
            binding.policy.clone(),
            Some(PortfolioCandidate {
                instrument: plan.instrument_id.clone(),
                direction: PositionDirection::Long,
                notional_usd: plan
                    .open_risk
                    .as_ref()
                    .expect("open risk")
                    .entry_settle_notional
                    .clone(),
                worst_case_loss_usd: plan
                    .open_risk
                    .as_ref()
                    .expect("open risk")
                    .stop_loss_settle
                    .clone(),
                leverage: "5".to_owned(),
            }),
            true,
        )
        .expect("portfolio risk");

        assert_eq!(
            revalidate_hard_risk_policy(
                &plan,
                &analysis,
                &account.account_generation,
                Some("5"),
            )
            .expect("matching analysis"),
            PreMutationRiskDisposition::Accepted
        );

        let mut wrong_binding = analysis.clone();
        wrong_binding.policy.version = "execution-policy/v2".to_owned();
        assert_eq!(
            revalidate_hard_risk_policy(
                &plan,
                &wrong_binding,
                &account.account_generation,
                Some("5"),
            ),
            Err(ExecutionValidationError::RiskBindingMismatch)
        );

        let mut wrong_candidate = analysis.clone();
        wrong_candidate
            .candidate
            .as_mut()
            .expect("candidate")
            .direction = PositionDirection::Short;
        assert_eq!(
            revalidate_hard_risk_policy(
                &plan,
                &wrong_candidate,
                &account.account_generation,
                Some("5"),
            ),
            Err(ExecutionValidationError::RiskCandidateMismatch)
        );

        let mut inconsistent = analysis;
        inconsistent.policy_decision = RiskPolicyDecision::Rejected;
        assert_eq!(
            revalidate_hard_risk_policy(
                &plan,
                &inconsistent,
                &account.account_generation,
                Some("5"),
            ),
            Err(ExecutionValidationError::RiskAnalysisInconsistent)
        );
    }

    fn rules() -> InstrumentRulesSnapshot {
        InstrumentRulesSnapshot {
            reference_generation: "sha256:reference".to_owned(),
            source_received_at: "2026-09-28T18:00:00Z".to_owned(),
            instrument: InstrumentSpec {
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                instrument_type: InstrumentType::Swap,
                instrument_family: Some("DOGE-USDT".to_owned()),
                underlying: Some("DOGE-USDT".to_owned()),
                state: "live".to_owned(),
                rule_type: Some("normal".to_owned()),
                funding_requirement: okx_observation::FundingRequirement::Required,
                base_currency: None,
                quote_currency: None,
                settle_currency: Some("USDT".to_owned()),
                tick_size: "0.00001".to_owned(),
                lot_size: "0.01".to_owned(),
                min_size: "0.01".to_owned(),
                max_limit_size: Some("1000".to_owned()),
                max_market_size: Some("1000".to_owned()),
                max_limit_amount: None,
                max_market_amount: None,
                contract_type: Some("linear".to_owned()),
                contract_value: Some("1000".to_owned()),
                contract_value_currency: Some("DOGE".to_owned()),
                fee_group_id: Some("1".to_owned()),
                max_leverage: Some("20".to_owned()),
                list_time_ms: None,
                expiry_time_ms: None,
                initial_price_limit_pct: Some("0.05".to_owned()),
                floating_price_limit_pct: Some("0.03".to_owned()),
                maximum_price_limit_pct: Some("0.15".to_owned()),
                upcoming_rule_changes: Vec::new(),
            },
        }
    }

    fn venue(rules: &InstrumentRulesSnapshot) -> VenueExecutionEvidence {
        VenueExecutionEvidence {
            schema: VENUE_EXECUTION_EVIDENCE_SCHEMA_V1,
            source_received_at: "2026-10-01T20:00:00Z".to_owned(),
            public_instrument: rules.instrument.clone(),
            account_instrument: AccountInstrumentExecutionLimits {
                state: "live".to_owned(),
                max_limit_size: Some("1000".to_owned()),
                max_market_size: Some("1000".to_owned()),
                position_limit_amount_usd: Some("100000".to_owned()),
                position_limit_pct: Some("30".to_owned()),
                platform_open_interest_limit_usd: Some("10000000".to_owned()),
                platform_open_interest_limit_coin: Some("100000000".to_owned()),
                long_position_remaining_quota_usd: Some("50000".to_owned()),
                short_position_remaining_quota_usd: Some("50000".to_owned()),
            },
            price_limit: PriceLimitEvidence {
                instrument_id: rules.instrument.instrument_id.clone(),
                buy_limit: "0.12000".to_owned(),
                sell_limit: "0.08000".to_owned(),
                exchange_timestamp_ms: "1790884800000".to_owned(),
            },
            max_order_size: Some(MaxOrderSizeEvidence {
                instrument_id: rules.instrument.instrument_id.clone(),
                max_buy: "1000".to_owned(),
                max_sell: "1000".to_owned(),
            }),
            ongoing_system_statuses: Vec::new(),
        }
    }

    fn account() -> AccountSnapshot {
        AccountSnapshot {
            schema: ACCOUNT_SNAPSHOT_SCHEMA_V2.to_owned(),
            source: ACCOUNT_CONVERGED_SOURCE_V2.to_owned(),
            source_received_at: "2026-09-28T18:00:00Z".to_owned(),
            account_generation: "sha256:account".to_owned(),
            quality_reason: M4_REST_WS_CONVERGED_REASON.to_owned(),
            private_ws_connected: true,
            private_ws_generation: Some(7),
            private_ws_connection_fingerprint: Some("connection-fingerprint".to_owned()),
            private_ws_last_inbound_ms: Some(1_790_000_000_000),
            private_ws_events_applied: Some(3),
            account_level: "2".to_owned(),
            position_mode: "long_short_mode".to_owned(),
            account_type: "1".to_owned(),
            account_uid_fingerprint: "uid-fingerprint".to_owned(),
            api_key_permissions: vec!["read_only".to_owned()],
            balance: AccountBalanceState {
                total_equity_usd: "10000".to_owned(),
                adjusted_equity_usd: Some("10000".to_owned()),
                isolated_equity_usd: None,
                initial_margin_requirement_usd: Some("0".to_owned()),
                maintenance_margin_requirement_usd: Some("0".to_owned()),
                margin_ratio: None,
                notional_usd: Some("0".to_owned()),
                update_time_ms: Some("1790000000000".to_owned()),
                details: Vec::new(),
            },
            positions: Vec::new(),
            pending_orders: Vec::new(),
        }
    }

    fn fees(rules: &InstrumentRulesSnapshot) -> FeeScheduleSnapshot {
        FeeScheduleSnapshot::from_input(FeeScheduleInput {
            instrument_id: rules.instrument.instrument_id.clone(),
            reference_generation: rules.reference_generation.clone(),
            source_received_at: "2026-09-28T18:00:00Z".to_owned(),
            exchange_timestamp_ms: "1790000000000".to_owned(),
            level: "Lv1".to_owned(),
            maker_rate: "-0.0002".to_owned(),
            taker_rate: "-0.0005".to_owned(),
            exact_for_instrument: true,
        })
        .expect("fees")
    }

    fn open_candidate(
        rules: &InstrumentRulesSnapshot,
        direction: PositionDirection,
    ) -> CandidateOrderAnalysis {
        let stop = match direction {
            PositionDirection::Long => "0.09000",
            PositionDirection::Short => "0.11000",
        };
        analyze_candidate_order(
            rules,
            &fees(rules),
            &CandidateOrderAssumptions {
                direction,
                entry_price: "0.10000".to_owned(),
                stop_price: stop.to_owned(),
                max_settle_notional: "1000".to_owned(),
                max_loss_settle: "50".to_owned(),
                target_rr: "2".to_owned(),
                entry_liquidity_role: LiquidityRole::Taker,
                exit_liquidity_role: LiquidityRole::Taker,
            },
        )
        .expect("candidate")
    }

    fn open_intent(
        rules: &InstrumentRulesSnapshot,
        account: &AccountSnapshot,
        candidate: &CandidateOrderAnalysis,
        position_side: PositionSide,
    ) -> ExecutionIntent {
        ExecutionIntent {
            intent_id: "intent_0123456789abcdef".to_owned(),
            expected_reference_generation: rules.reference_generation.clone(),
            expected_account_generation: account.account_generation.clone(),
            instrument_id: rules.instrument.instrument_id.clone(),
            trade_mode: TradeMode::Cross,
            position_side,
            action: ExecutionAction::Open,
            order_type: OrderType::Limit,
            size: candidate.contracts.clone(),
            price: candidate.entry_price.clone(),
        }
    }

    #[test]
    fn accepted_open_intent_reuses_candidate_risk_evidence() {
        let rules = rules();
        let account = account();
        let candidate = open_candidate(&rules, PositionDirection::Long);
        let intent = open_intent(&rules, &account, &candidate, PositionSide::Long);

        let plan =
            prepare_execution(&intent, &rules, &account, Some(&candidate)).expect("execution plan");

        assert_eq!(plan.side, OrderSide::Buy);
        assert_eq!(plan.position_side, PositionSide::Long);
        assert_eq!(plan.size, candidate.contracts);
        assert_eq!(plan.price, candidate.entry_price);
        assert!(plan.open_risk.is_some());
        assert_eq!(plan.client_order_id.len(), 32);
    }

    #[test]
    fn venue_revalidation_accepts_only_current_exchange_constraints() {
        let rules = rules();
        let account = account();
        let candidate = open_candidate(&rules, PositionDirection::Long);
        let intent = open_intent(&rules, &account, &candidate, PositionSide::Long);
        let plan =
            prepare_execution(&intent, &rules, &account, Some(&candidate)).expect("execution plan");
        let evidence = venue(&rules);

        revalidate_venue_execution(&plan, &rules, &evidence, 1_790_884_805_000)
            .expect("current venue evidence");
    }

    #[test]
    fn venue_revalidation_rejects_rule_change_status_price_and_account_limits() {
        let rules = rules();
        let account = account();
        let candidate = open_candidate(&rules, PositionDirection::Long);
        let intent = open_intent(&rules, &account, &candidate, PositionSide::Long);
        let plan =
            prepare_execution(&intent, &rules, &account, Some(&candidate)).expect("execution plan");

        let mut changed = venue(&rules);
        changed.public_instrument.tick_size = "0.0001".to_owned();
        assert_eq!(
            revalidate_venue_execution(&plan, &rules, &changed, 1_790_884_805_000),
            Err(ExecutionValidationError::FreshReferenceMismatch)
        );

        let mut blocked = venue(&rules);
        blocked.ongoing_system_statuses.push(SystemStatusEvidence {
            id: "maintenance".to_owned(),
            state: "ongoing".to_owned(),
            service_type: "trading".to_owned(),
            system: "trading".to_owned(),
            maintenance_type: "2".to_owned(),
            environment: "1".to_owned(),
            begin_ms: "1790884800000".to_owned(),
            end_ms: String::new(),
        });
        assert_eq!(
            revalidate_venue_execution(&plan, &rules, &blocked, 1_790_884_805_000),
            Err(ExecutionValidationError::OngoingSystemStatus(1))
        );

        let mut price_blocked = venue(&rules);
        price_blocked.price_limit.buy_limit = "0.09000".to_owned();
        assert_eq!(
            revalidate_venue_execution(&plan, &rules, &price_blocked, 1_790_884_805_000),
            Err(ExecutionValidationError::PriceAboveCurrentLimit {
                price: "0.1".to_owned(),
                limit: "0.09".to_owned(),
            })
        );

        let mut quota_blocked = venue(&rules);
        quota_blocked
            .max_order_size
            .as_mut()
            .expect("max-size")
            .max_buy = "0.01".to_owned();
        assert!(matches!(
            revalidate_venue_execution(&plan, &rules, &quota_blocked, 1_790_884_805_000),
            Err(ExecutionValidationError::ExceedsCurrentMaxOrderSize { .. })
        ));

        let mut zero_capacity = venue(&rules);
        zero_capacity
            .max_order_size
            .as_mut()
            .expect("max-size")
            .max_buy = "0".to_owned();
        assert_eq!(
            revalidate_venue_execution(&plan, &rules, &zero_capacity, 1_790_884_805_000),
            Err(ExecutionValidationError::ExceedsCurrentMaxOrderSize {
                size: "4.95".to_owned(),
                max_size: "0".to_owned(),
            })
        );

        let mut account_blocked = venue(&rules);
        account_blocked.account_instrument.state = "suspend".to_owned();
        assert!(matches!(
            revalidate_venue_execution(&plan, &rules, &account_blocked, 1_790_884_805_000),
            Err(ExecutionValidationError::AccountInstrumentNotLive(_))
        ));
    }

    #[test]
    fn upcoming_rule_change_inside_mutation_deadline_fails_closed() {
        let rules = rules();
        let account = account();
        let candidate = open_candidate(&rules, PositionDirection::Long);
        let intent = open_intent(&rules, &account, &candidate, PositionSide::Long);
        let plan =
            prepare_execution(&intent, &rules, &account, Some(&candidate)).expect("execution plan");

        let mut rules_with_change = rules.clone();
        rules_with_change.instrument.upcoming_rule_changes.push(
            okx_observation::UpcomingRuleChange {
                param: "tickSz".to_owned(),
                new_value: "0.000001".to_owned(),
                effective_time_ms: "1790884804000".to_owned(),
            },
        );
        let evidence = venue(&rules_with_change);

        assert_eq!(
            revalidate_venue_execution(&plan, &rules_with_change, &evidence, 1_790_884_805_000,),
            Err(
                ExecutionValidationError::UpcomingRuleChangeInsideMutationWindow {
                    param: "tickSz".to_owned(),
                    effective_time_ms: 1_790_884_804_000,
                    mutation_deadline_ms: 1_790_884_805_000,
                }
            )
        );
    }

    #[test]
    fn stale_reference_or_account_generation_fails_closed() {
        let rules = rules();
        let account = account();
        let candidate = open_candidate(&rules, PositionDirection::Long);
        let mut intent = open_intent(&rules, &account, &candidate, PositionSide::Long);

        intent.expected_reference_generation = "sha256:stale".to_owned();
        assert_eq!(
            prepare_execution(&intent, &rules, &account, Some(&candidate)),
            Err(ExecutionValidationError::ReferenceGenerationMismatch)
        );

        intent.expected_reference_generation = rules.reference_generation.clone();
        intent.expected_account_generation = "sha256:stale".to_owned();
        assert_eq!(
            prepare_execution(&intent, &rules, &account, Some(&candidate)),
            Err(ExecutionValidationError::AccountGenerationMismatch)
        );
    }

    #[test]
    fn candidate_direction_size_and_price_must_match_intent() {
        let rules = rules();
        let account = account();
        let candidate = open_candidate(&rules, PositionDirection::Long);
        let mut intent = open_intent(&rules, &account, &candidate, PositionSide::Long);

        intent.position_side = PositionSide::Short;
        assert_eq!(
            prepare_execution(&intent, &rules, &account, Some(&candidate)),
            Err(ExecutionValidationError::CandidateMismatch)
        );

        intent.position_side = PositionSide::Long;
        intent.size = "0.01".to_owned();
        assert_eq!(
            prepare_execution(&intent, &rules, &account, Some(&candidate)),
            Err(ExecutionValidationError::CandidateMismatch)
        );
    }

    #[test]
    fn price_and_size_exchange_mechanics_fail_closed() {
        let rules = rules();
        let account = account();
        let candidate = open_candidate(&rules, PositionDirection::Long);
        let mut intent = open_intent(&rules, &account, &candidate, PositionSide::Long);

        intent.price = "0.100001".to_owned();
        assert!(matches!(
            prepare_execution(&intent, &rules, &account, Some(&candidate)),
            Err(ExecutionValidationError::PriceNotTickAligned { .. })
        ));

        intent.price = candidate.entry_price.clone();
        intent.size = "0.001".to_owned();
        assert!(matches!(
            prepare_execution(&intent, &rules, &account, Some(&candidate)),
            Err(ExecutionValidationError::BelowMinimumSize { .. })
        ));
    }

    #[test]
    fn close_reserves_existing_pending_close_quantity() {
        let rules = rules();
        let mut account = account();
        account.positions.push(AccountPositionState {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            position: "5".to_owned(),
            position_side: "long".to_owned(),
            margin_mode: "cross".to_owned(),
            average_price: Some("0.1".to_owned()),
            mark_price: Some("0.1".to_owned()),
            liquidation_price: None,
            unrealized_pnl: Some("0".to_owned()),
            unrealized_pnl_ratio: Some("0".to_owned()),
            leverage: Some("5".to_owned()),
            margin: Some("100".to_owned()),
            initial_margin_requirement: Some("100".to_owned()),
            maintenance_margin_requirement: Some("50".to_owned()),
            margin_ratio: None,
            notional_usd: Some("500".to_owned()),
            margin_currency: Some("USDT".to_owned()),
            creation_time_ms: Some("1790000000000".to_owned()),
            update_time_ms: Some("1790000001000".to_owned()),
        });
        account.pending_orders.push(PendingOrderState {
            order_id: "ord-existing-close".to_owned(),
            client_order_id: Some("existingclose1".to_owned()),
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            side: "sell".to_owned(),
            position_side: Some("long".to_owned()),
            trade_mode: "cross".to_owned(),
            order_type: "limit".to_owned(),
            price: Some("0.11".to_owned()),
            size: "2".to_owned(),
            accumulated_fill_size: "0.5".to_owned(),
            average_fill_price: None,
            state: "live".to_owned(),
            reduce_only: None,
            creation_time_ms: "1790000000000".to_owned(),
            update_time_ms: "1790000001000".to_owned(),
        });

        let intent = ExecutionIntent {
            intent_id: "intent_close_0123456789".to_owned(),
            expected_reference_generation: rules.reference_generation.clone(),
            expected_account_generation: account.account_generation.clone(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            trade_mode: TradeMode::Cross,
            position_side: PositionSide::Long,
            action: ExecutionAction::Close,
            order_type: OrderType::Limit,
            size: "3.5".to_owned(),
            price: "0.10000".to_owned(),
        };

        let plan = prepare_execution(&intent, &rules, &account, None).expect("close plan");
        assert_eq!(plan.side, OrderSide::Sell);
        assert!(plan.open_risk.is_none());

        let mut too_large = intent;
        too_large.size = "3.51".to_owned();
        assert_eq!(
            prepare_execution(&too_large, &rules, &account, None),
            Err(ExecutionValidationError::CloseSizeExceedsAvailable {
                requested: "3.51".to_owned(),
                available: "3.5".to_owned(),
            })
        );
    }

    #[test]
    fn prepared_plan_revalidation_uses_semantic_authorities_not_generation_equality() {
        let rules = rules();
        let account = account();
        let candidate = open_candidate(&rules, PositionDirection::Long);
        let intent = open_intent(&rules, &account, &candidate, PositionSide::Long);
        let plan =
            prepare_execution(&intent, &rules, &account, Some(&candidate)).expect("execution plan");

        revalidate_execution_plan(&plan, &rules, &account, Some(&candidate.fee_generation))
            .expect("fresh plan");

        let mut generation_only_change = account.clone();
        generation_only_change.account_generation = "sha256:new-observation".to_owned();
        revalidate_execution_plan(
            &plan,
            &rules,
            &generation_only_change,
            Some(&candidate.fee_generation),
        )
        .expect("generation provenance change alone is not a semantic rejection");

        let mut changed_account = account.clone();
        changed_account.account_uid_fingerprint = "different".to_owned();
        assert_eq!(
            revalidate_execution_plan(
                &plan,
                &rules,
                &changed_account,
                Some(&candidate.fee_generation),
            ),
            Err(ExecutionValidationError::AccountIdentityMismatch)
        );

        assert_eq!(
            revalidate_execution_plan(&plan, &rules, &account, Some("changed-fee-generation")),
            Err(ExecutionValidationError::FeeGenerationMismatch)
        );
    }

    #[test]
    fn non_converged_or_wrong_account_mode_fails_closed() {
        let rules = rules();
        let mut account = account();
        let candidate = open_candidate(&rules, PositionDirection::Long);
        let intent = open_intent(&rules, &account, &candidate, PositionSide::Long);

        account.private_ws_connected = false;
        assert_eq!(
            prepare_execution(&intent, &rules, &account, Some(&candidate)),
            Err(ExecutionValidationError::AccountNotConverged)
        );

        account.private_ws_connected = true;
        account.position_mode = "net_mode".to_owned();
        assert_eq!(
            prepare_execution(&intent, &rules, &account, Some(&candidate)),
            Err(ExecutionValidationError::UnsupportedPositionMode(
                "net_mode".to_owned()
            ))
        );
    }
}

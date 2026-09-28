use std::str::FromStr;

use okx_analysis::{CandidateOrderAnalysis, PositionDirection};
use okx_observation::{ACCOUNT_SNAPSHOT_SCHEMA_V2, AccountSnapshot, InstrumentRulesSnapshot};
use rust_decimal::Decimal;
use thiserror::Error;

use crate::{
    EXECUTION_PLAN_SCHEMA_V1, ExecutionAction, ExecutionIntent, ExecutionPlan, OpenRiskEvidence,
    OrderSide, PositionSide, derive_client_order_id, model::order_side,
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

    #[error("unsupported account level '{0}', expected Futures mode level 2")]
    UnsupportedAccountLevel(String),

    #[error("unsupported position mode '{0}', expected long_short_mode")]
    UnsupportedPositionMode(String),

    #[error("instrument '{0}' is not trade-ready live")]
    InstrumentNotLive(String),

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
}

pub fn prepare_execution(
    intent: &ExecutionIntent,
    rules: &InstrumentRulesSnapshot,
    account: &AccountSnapshot,
    candidate: Option<&CandidateOrderAnalysis>,
) -> Result<ExecutionPlan, ExecutionValidationError> {
    validate_intent_id(&intent.intent_id)?;

    if intent.instrument_id != rules.instrument.instrument_id {
        return Err(ExecutionValidationError::InstrumentMismatch);
    }
    if intent.expected_reference_generation != rules.reference_generation {
        return Err(ExecutionValidationError::ReferenceGenerationMismatch);
    }
    if intent.expected_account_generation != account.account_generation {
        return Err(ExecutionValidationError::AccountGenerationMismatch);
    }
    if account.schema != ACCOUNT_SNAPSHOT_SCHEMA_V2 || !account.private_ws_connected {
        return Err(ExecutionValidationError::AccountNotConverged);
    }
    if account.account_uid_fingerprint.trim().is_empty() {
        return Err(ExecutionValidationError::MissingAccountIdentity);
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

    let size = positive_decimal("size", &intent.size)?;
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

    let price = positive_decimal("price", &intent.price)?;
    let tick_size = positive_decimal("tick_size", &rules.instrument.tick_size)?;
    if price % tick_size != Decimal::ZERO {
        return Err(ExecutionValidationError::PriceNotTickAligned {
            price: normalized(price),
            tick_size: normalized(tick_size),
        });
    }

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
    })
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

fn validate_intent_id(value: &str) -> Result<(), ExecutionValidationError> {
    if (16..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Ok(())
    } else {
        Err(ExecutionValidationError::InvalidIntentId)
    }
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
        CandidateOrderAssumptions, LiquidityRole, PositionDirection, analyze_candidate_order,
    };
    use okx_api::InstrumentType;
    use okx_observation::{
        ACCOUNT_CONVERGED_SOURCE_V2, ACCOUNT_SNAPSHOT_SCHEMA_V2, AccountBalanceState,
        AccountPositionState, FeeScheduleInput, FeeScheduleSnapshot, InstrumentSpec,
        M4_REST_WS_CONVERGED_REASON, PendingOrderState,
    };

    use super::*;
    use crate::{ExecutionAction, OrderType, PositionSide, TradeMode};

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
            },
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

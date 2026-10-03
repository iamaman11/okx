use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::ExecutionPlan;

pub const ALLOW_LIVE_TRADING_DEFAULT: bool = false;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionState {
    Prepared,
    Submitting,
    Acknowledged,
    UnknownSubmission,
    Live,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
}

impl ExecutionState {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Filled | Self::Canceled | Self::Rejected)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeOrderState {
    Live,
    PartiallyFilled,
    Filled,
    Canceled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRecord {
    pub plan: ExecutionPlan,
    pub state: ExecutionState,
    pub order_id: Option<String>,
    pub exchange_state: Option<ExchangeOrderState>,
    pub rejection_code: Option<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExecutionTransitionError {
    #[error("live trading is disabled")]
    LiveTradingDisabled,

    #[error("execution transition from {from:?} to {to:?} is not allowed")]
    InvalidTransition {
        from: ExecutionState,
        to: ExecutionState,
    },

    #[error("exchange order id must not be empty")]
    EmptyOrderId,

    #[error("exchange order id changed from '{existing}' to '{incoming}'")]
    OrderIdMismatch { existing: String, incoming: String },

    #[error("rejection code must not be empty")]
    EmptyRejectionCode,
}

impl ExecutionRecord {
    pub fn new(plan: ExecutionPlan) -> Self {
        Self {
            plan,
            state: ExecutionState::Prepared,
            order_id: None,
            exchange_state: None,
            rejection_code: None,
        }
    }

    pub fn begin_submission(&mut self) -> Result<(), ExecutionTransitionError> {
        self.transition(ExecutionState::Prepared, ExecutionState::Submitting)
    }

    pub fn acknowledge(
        &mut self,
        order_id: impl Into<String>,
    ) -> Result<(), ExecutionTransitionError> {
        if self.state != ExecutionState::Submitting {
            return Err(ExecutionTransitionError::InvalidTransition {
                from: self.state,
                to: ExecutionState::Acknowledged,
            });
        }
        let order_id = validated_order_id(order_id.into())?;
        self.order_id = Some(order_id);
        self.state = ExecutionState::Acknowledged;
        Ok(())
    }

    pub fn mark_unknown_submission(&mut self) -> Result<(), ExecutionTransitionError> {
        self.transition(
            ExecutionState::Submitting,
            ExecutionState::UnknownSubmission,
        )
    }

    pub fn reject_known(
        &mut self,
        code: impl Into<String>,
    ) -> Result<(), ExecutionTransitionError> {
        if self.state != ExecutionState::Submitting {
            return Err(ExecutionTransitionError::InvalidTransition {
                from: self.state,
                to: ExecutionState::Rejected,
            });
        }
        let code = code.into();
        if code.trim().is_empty() {
            return Err(ExecutionTransitionError::EmptyRejectionCode);
        }
        self.rejection_code = Some(code);
        self.state = ExecutionState::Rejected;
        Ok(())
    }

    pub fn reconcile_found(
        &mut self,
        order_id: impl Into<String>,
        exchange_state: ExchangeOrderState,
    ) -> Result<(), ExecutionTransitionError> {
        if !matches!(
            self.state,
            ExecutionState::Acknowledged
                | ExecutionState::UnknownSubmission
                | ExecutionState::Live
                | ExecutionState::PartiallyFilled
        ) {
            return Err(ExecutionTransitionError::InvalidTransition {
                from: self.state,
                to: execution_state(exchange_state),
            });
        }

        let incoming = validated_order_id(order_id.into())?;
        if let Some(existing) = self.order_id.as_ref()
            && existing != &incoming
        {
            return Err(ExecutionTransitionError::OrderIdMismatch {
                existing: existing.clone(),
                incoming,
            });
        }

        self.order_id = Some(incoming);
        self.exchange_state = Some(exchange_state);
        self.state = execution_state(exchange_state);
        Ok(())
    }

    pub const fn can_submit(&self) -> bool {
        matches!(self.state, ExecutionState::Prepared)
    }

    fn transition(
        &mut self,
        from: ExecutionState,
        to: ExecutionState,
    ) -> Result<(), ExecutionTransitionError> {
        if self.state != from {
            return Err(ExecutionTransitionError::InvalidTransition {
                from: self.state,
                to,
            });
        }
        self.state = to;
        Ok(())
    }
}

pub fn require_live_trading_enabled(enabled: bool) -> Result<(), ExecutionTransitionError> {
    if enabled {
        Ok(())
    } else {
        Err(ExecutionTransitionError::LiveTradingDisabled)
    }
}

fn validated_order_id(order_id: String) -> Result<String, ExecutionTransitionError> {
    if order_id.trim().is_empty() {
        Err(ExecutionTransitionError::EmptyOrderId)
    } else {
        Ok(order_id)
    }
}

const fn execution_state(exchange_state: ExchangeOrderState) -> ExecutionState {
    match exchange_state {
        ExchangeOrderState::Live => ExecutionState::Live,
        ExchangeOrderState::PartiallyFilled => ExecutionState::PartiallyFilled,
        ExchangeOrderState::Filled => ExecutionState::Filled,
        ExchangeOrderState::Canceled => ExecutionState::Canceled,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        EXECUTION_PLAN_SCHEMA_V1, ExecutionAction, OrderSide, OrderType, PositionSide, TradeMode,
    };

    fn plan() -> ExecutionPlan {
        ExecutionPlan {
            schema: EXECUTION_PLAN_SCHEMA_V1.to_owned(),
            intent_id: "intent_0123456789abcdef".to_owned(),
            client_order_id: "okx01234567890123456789012345678".to_owned(),
            reference_generation: "sha256:reference".to_owned(),
            account_generation: "sha256:account".to_owned(),
            account_uid_fingerprint: "uid-fingerprint".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            trade_mode: TradeMode::Cross,
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            action: ExecutionAction::Open,
            order_type: OrderType::Limit,
            size: "1".to_owned(),
            price: "0.1".to_owned(),
            open_risk: None,
            risk_binding: None,
        }
    }

    #[test]
    fn default_live_trading_gate_is_fail_closed() {
        assert_eq!(
            require_live_trading_enabled(ALLOW_LIVE_TRADING_DEFAULT),
            Err(ExecutionTransitionError::LiveTradingDisabled)
        );
    }

    #[test]
    fn unknown_submission_cannot_blindly_resubmit() {
        let mut record = ExecutionRecord::new(plan());
        record.begin_submission().expect("submit");
        record.mark_unknown_submission().expect("unknown");

        assert!(!record.can_submit());
        assert!(matches!(
            record.begin_submission(),
            Err(ExecutionTransitionError::InvalidTransition {
                from: ExecutionState::UnknownSubmission,
                ..
            })
        ));

        record
            .reconcile_found("ord-1", ExchangeOrderState::Live)
            .expect("reconcile");
        assert_eq!(record.state, ExecutionState::Live);
        assert_eq!(record.order_id.as_deref(), Some("ord-1"));
    }

    #[test]
    fn acknowledged_order_reconciles_to_terminal_state() {
        let mut record = ExecutionRecord::new(plan());
        record.begin_submission().expect("submit");
        record.acknowledge("ord-1").expect("ack");
        record
            .reconcile_found("ord-1", ExchangeOrderState::PartiallyFilled)
            .expect("partial");
        assert_eq!(record.state, ExecutionState::PartiallyFilled);

        record
            .reconcile_found("ord-1", ExchangeOrderState::Filled)
            .expect("filled");
        assert!(record.state.is_terminal());
    }

    #[test]
    fn exchange_order_id_is_immutable_after_ack() {
        let mut record = ExecutionRecord::new(plan());
        record.begin_submission().expect("submit");
        record.acknowledge("ord-1").expect("ack");

        assert!(matches!(
            record.reconcile_found("ord-2", ExchangeOrderState::Live),
            Err(ExecutionTransitionError::OrderIdMismatch { .. })
        ));
    }
}

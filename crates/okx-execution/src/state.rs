use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ExecutionLineageBinding, ExecutionPlan, PositionSide, derive_protective_algo_client_id,
    model::{valid_intent_id, valid_mutation_id},
};

pub const ALLOW_LIVE_TRADING_DEFAULT: bool = false;
pub const MAX_ORDER_MUTATIONS_PER_EXECUTION: usize = 64;
pub const PROTECTIVE_ORDER_POLICY_V1: &str = "okx.protective-order/mark-market-v1";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderMutationKind {
    Amend,
    Cancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderMutationState {
    Prepared,
    Submitting,
    Acknowledged,
    Unknown,
    Applied,
    Superseded,
    Rejected,
}

impl OrderMutationState {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Applied | Self::Superseded | Self::Rejected)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderMutationResolution {
    Pending,
    Applied,
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderMutationRecord {
    pub mutation_id: String,
    pub kind: OrderMutationKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_size: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_price: Option<String>,
    pub state: OrderMutationState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejection_code: Option<String>,
}

impl OrderMutationRecord {
    pub fn amend(
        mutation_id: impl Into<String>,
        request_id: impl Into<String>,
        new_size: Option<String>,
        new_price: Option<String>,
    ) -> Result<Self, ExecutionTransitionError> {
        let mutation_id = mutation_id.into();
        let request_id = request_id.into();
        if !valid_mutation_id(&mutation_id) {
            return Err(ExecutionTransitionError::InvalidMutationId);
        }
        if request_id.trim().is_empty() {
            return Err(ExecutionTransitionError::InvalidMutationRequest);
        }
        if new_size.is_none() && new_price.is_none() {
            return Err(ExecutionTransitionError::InvalidMutationRequest);
        }
        Ok(Self {
            mutation_id,
            kind: OrderMutationKind::Amend,
            request_id: Some(request_id),
            new_size,
            new_price,
            state: OrderMutationState::Prepared,
            rejection_code: None,
        })
    }

    pub fn cancel(mutation_id: impl Into<String>) -> Result<Self, ExecutionTransitionError> {
        let mutation_id = mutation_id.into();
        if !valid_mutation_id(&mutation_id) {
            return Err(ExecutionTransitionError::InvalidMutationId);
        }
        Ok(Self {
            mutation_id,
            kind: OrderMutationKind::Cancel,
            request_id: None,
            new_size: None,
            new_price: None,
            state: OrderMutationState::Prepared,
            rejection_code: None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtectiveTriggerPriceBasis {
    Mark,
}

impl ProtectiveTriggerPriceBasis {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mark => "mark",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProtectiveOrderStatus {
    Pending,
    Active,
    NotActivated,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtectiveOrderLink {
    pub policy_version: String,
    pub algo_client_order_id: String,
    pub trigger_price_basis: ProtectiveTriggerPriceBasis,
    pub status: ProtectiveOrderStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub algo_order_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub covered_size: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_code: Option<String>,
}

impl ProtectiveOrderLink {
    fn from_plan(plan: &ExecutionPlan) -> Option<Self> {
        if !plan.action.is_risk_increasing() || plan.open_risk.is_none() {
            return None;
        }
        Some(Self {
            policy_version: PROTECTIVE_ORDER_POLICY_V1.to_owned(),
            algo_client_order_id: derive_protective_algo_client_id(&plan.intent_id),
            trigger_price_basis: ProtectiveTriggerPriceBasis::Mark,
            status: ProtectiveOrderStatus::Pending,
            algo_order_id: None,
            covered_size: None,
            failure_code: None,
        })
    }

    pub const fn blocks_new_managed_intent(&self) -> bool {
        matches!(
            self.status,
            ProtectiveOrderStatus::Pending | ProtectiveOrderStatus::Failed
        )
    }

    pub const fn requires_reconciliation(&self) -> bool {
        matches!(self.status, ProtectiveOrderStatus::Pending)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtectiveOrderResolution {
    Pending,
    Active {
        algo_order_id: String,
        covered_size: String,
    },
    NotActivated,
    Failed {
        code: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReverseLeg {
    Close,
    Open,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReverseContinuation {
    Required,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReverseExecutionLink {
    pub root_intent_id: String,
    pub open_intent_id: String,
    pub target_position_side: PositionSide,
    pub leg: ReverseLeg,
    pub continuation: ReverseContinuation,
}

impl ReverseExecutionLink {
    pub fn close(
        root_intent_id: impl Into<String>,
        open_intent_id: impl Into<String>,
        target_position_side: PositionSide,
    ) -> Result<Self, ExecutionTransitionError> {
        let root_intent_id = root_intent_id.into();
        let open_intent_id = open_intent_id.into();
        if !valid_intent_id(&root_intent_id)
            || !valid_intent_id(&open_intent_id)
            || root_intent_id == open_intent_id
        {
            return Err(ExecutionTransitionError::InvalidReverseLink);
        }
        Ok(Self {
            root_intent_id,
            open_intent_id,
            target_position_side,
            leg: ReverseLeg::Close,
            continuation: ReverseContinuation::Required,
        })
    }

    pub fn open_from(close: &Self) -> Result<Self, ExecutionTransitionError> {
        if close.leg != ReverseLeg::Close {
            return Err(ExecutionTransitionError::InvalidReverseLink);
        }
        Ok(Self {
            root_intent_id: close.root_intent_id.clone(),
            open_intent_id: close.open_intent_id.clone(),
            target_position_side: close.target_position_side,
            leg: ReverseLeg::Open,
            continuation: ReverseContinuation::Required,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRecord {
    pub plan: ExecutionPlan,
    pub state: ExecutionState,
    pub order_id: Option<String>,
    pub exchange_state: Option<ExchangeOrderState>,
    pub rejection_code: Option<String>,
    #[serde(default)]
    pub mutations: Vec<OrderMutationRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<ExecutionLineageBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protection: Option<ProtectiveOrderLink>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reverse: Option<ReverseExecutionLink>,
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

    #[error("order mutation id is invalid")]
    InvalidMutationId,

    #[error("order mutation request is invalid")]
    InvalidMutationRequest,

    #[error("order mutation '{0}' conflicts with an existing durable mutation")]
    MutationConflict(String),

    #[error("execution already has a nonterminal order mutation")]
    MutationAlreadyActive,

    #[error("order mutation is not allowed while execution state is {0:?}")]
    MutationNotAllowed(ExecutionState),

    #[error("order mutation '{0}' is not present")]
    MutationNotFound(String),

    #[error("order mutation capacity is exhausted")]
    MutationCapacityExceeded,

    #[error("order mutation transition from {from:?} to {to:?} is not allowed")]
    InvalidMutationTransition {
        from: OrderMutationState,
        to: OrderMutationState,
    },

    #[error("execution lineage can only be bound idempotently while PREPARED")]
    InvalidLineageTransition,

    #[error("protective order linkage is invalid")]
    InvalidProtectionLink,

    #[error("protective order transition is invalid")]
    InvalidProtectionTransition,

    #[error("reverse execution linkage is invalid")]
    InvalidReverseLink,

    #[error("reverse execution transition is invalid")]
    InvalidReverseTransition,
}

impl ExecutionRecord {
    pub fn new(plan: ExecutionPlan) -> Self {
        let protection = ProtectiveOrderLink::from_plan(&plan);
        Self {
            plan,
            state: ExecutionState::Prepared,
            order_id: None,
            exchange_state: None,
            rejection_code: None,
            mutations: Vec::new(),
            lineage: None,
            protection,
            reverse: None,
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
        if self.protection.is_some() {
            self.resolve_protection(ProtectiveOrderResolution::NotActivated)?;
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
        self.reconcile_found_with_protection(order_id, exchange_state, None)
    }

    pub fn reconcile_found_with_protection(
        &mut self,
        order_id: impl Into<String>,
        exchange_state: ExchangeOrderState,
        protection_resolution: Option<ProtectiveOrderResolution>,
    ) -> Result<(), ExecutionTransitionError> {
        let target = execution_state(exchange_state);
        if !matches!(
            self.state,
            ExecutionState::Acknowledged
                | ExecutionState::UnknownSubmission
                | ExecutionState::Live
                | ExecutionState::PartiallyFilled
        ) && !(self.state.is_terminal() && self.state == target)
        {
            return Err(ExecutionTransitionError::InvalidTransition {
                from: self.state,
                to: target,
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

        if let Some(resolution) = protection_resolution {
            self.resolve_protection(resolution)?;
        }

        self.order_id = Some(incoming);
        self.exchange_state = Some(exchange_state);
        self.state = target;
        Ok(())
    }

    pub fn protection_requires_reconciliation(&self) -> bool {
        self.protection
            .as_ref()
            .is_some_and(ProtectiveOrderLink::requires_reconciliation)
    }

    pub fn protection_blocks_new_managed_intent(&self) -> bool {
        self.protection
            .as_ref()
            .is_some_and(ProtectiveOrderLink::blocks_new_managed_intent)
    }

    pub fn resolve_protection(
        &mut self,
        resolution: ProtectiveOrderResolution,
    ) -> Result<(), ExecutionTransitionError> {
        let protection = self
            .protection
            .as_mut()
            .ok_or(ExecutionTransitionError::InvalidProtectionLink)?;

        match resolution {
            ProtectiveOrderResolution::Pending => {
                if protection.status != ProtectiveOrderStatus::Pending {
                    return Err(ExecutionTransitionError::InvalidProtectionTransition);
                }
            }
            ProtectiveOrderResolution::NotActivated => {
                if !matches!(
                    protection.status,
                    ProtectiveOrderStatus::Pending | ProtectiveOrderStatus::NotActivated
                ) {
                    return Err(ExecutionTransitionError::InvalidProtectionTransition);
                }
                protection.status = ProtectiveOrderStatus::NotActivated;
                protection.algo_order_id = None;
                protection.covered_size = None;
                protection.failure_code = None;
            }
            ProtectiveOrderResolution::Failed { code } => {
                if code.trim().is_empty()
                    || !matches!(
                        protection.status,
                        ProtectiveOrderStatus::Pending | ProtectiveOrderStatus::Failed
                    )
                {
                    return Err(ExecutionTransitionError::InvalidProtectionTransition);
                }
                if protection.status == ProtectiveOrderStatus::Failed
                    && protection.failure_code.as_deref() != Some(code.as_str())
                {
                    return Err(ExecutionTransitionError::InvalidProtectionTransition);
                }
                protection.status = ProtectiveOrderStatus::Failed;
                protection.algo_order_id = None;
                protection.covered_size = None;
                protection.failure_code = Some(code);
            }
            ProtectiveOrderResolution::Active {
                algo_order_id,
                covered_size,
            } => {
                let covered = Decimal::from_str(covered_size.trim())
                    .ok()
                    .filter(|value| *value > Decimal::ZERO)
                    .ok_or(ExecutionTransitionError::InvalidProtectionLink)?;
                if algo_order_id.trim().is_empty() {
                    return Err(ExecutionTransitionError::InvalidProtectionLink);
                }
                let covered_size = covered.normalize().to_string();
                if protection.status == ProtectiveOrderStatus::Active {
                    if protection.algo_order_id.as_deref() != Some(algo_order_id.as_str())
                        || protection.covered_size.as_deref() != Some(covered_size.as_str())
                    {
                        return Err(ExecutionTransitionError::InvalidProtectionTransition);
                    }
                    return Ok(());
                }
                if protection.status != ProtectiveOrderStatus::Pending {
                    return Err(ExecutionTransitionError::InvalidProtectionTransition);
                }
                protection.status = ProtectiveOrderStatus::Active;
                protection.algo_order_id = Some(algo_order_id);
                protection.covered_size = Some(covered_size);
                protection.failure_code = None;
            }
        }
        Ok(())
    }

    pub fn bind_lineage(
        &mut self,
        lineage: ExecutionLineageBinding,
    ) -> Result<(), ExecutionTransitionError> {
        if self.state != ExecutionState::Prepared {
            return Err(ExecutionTransitionError::InvalidLineageTransition);
        }
        if self
            .lineage
            .as_ref()
            .is_some_and(|existing| existing != &lineage)
        {
            return Err(ExecutionTransitionError::InvalidLineageTransition);
        }
        self.lineage = Some(lineage);
        Ok(())
    }

    pub fn attach_reverse(
        &mut self,
        reverse: ReverseExecutionLink,
    ) -> Result<(), ExecutionTransitionError> {
        if self
            .reverse
            .as_ref()
            .is_some_and(|existing| existing != &reverse)
        {
            return Err(ExecutionTransitionError::InvalidReverseLink);
        }
        self.reverse = Some(reverse);
        Ok(())
    }

    pub fn abort_reverse_continuation(&mut self) -> Result<(), ExecutionTransitionError> {
        let reverse = self
            .reverse
            .as_mut()
            .ok_or(ExecutionTransitionError::InvalidReverseLink)?;
        if reverse.leg != ReverseLeg::Close
            || self.state != ExecutionState::Filled
            || reverse.continuation != ReverseContinuation::Required
        {
            return Err(ExecutionTransitionError::InvalidReverseTransition);
        }
        reverse.continuation = ReverseContinuation::Aborted;
        Ok(())
    }

    pub const fn can_submit(&self) -> bool {
        matches!(self.state, ExecutionState::Prepared)
    }

    pub fn active_mutation(&self) -> Option<&OrderMutationRecord> {
        self.mutations
            .iter()
            .rev()
            .find(|mutation| !mutation.state.is_terminal())
    }

    pub fn effective_size(&self) -> &str {
        let mut value = self.plan.size.as_str();
        for mutation in &self.mutations {
            if mutation.kind == OrderMutationKind::Amend
                && mutation.state == OrderMutationState::Applied
                && let Some(size) = mutation.new_size.as_deref()
            {
                value = size;
            }
        }
        value
    }

    pub fn effective_price(&self) -> &str {
        let mut value = self.plan.price.as_str();
        for mutation in &self.mutations {
            if mutation.kind == OrderMutationKind::Amend
                && mutation.state == OrderMutationState::Applied
                && let Some(price) = mutation.new_price.as_deref()
            {
                value = price;
            }
        }
        value
    }

    pub fn prepare_mutation(
        &mut self,
        mutation: OrderMutationRecord,
    ) -> Result<bool, ExecutionTransitionError> {
        if let Some(existing) = self
            .mutations
            .iter()
            .find(|existing| existing.mutation_id == mutation.mutation_id)
        {
            return if existing == &mutation {
                Ok(false)
            } else {
                Err(ExecutionTransitionError::MutationConflict(
                    mutation.mutation_id,
                ))
            };
        }
        if self.active_mutation().is_some() {
            return Err(ExecutionTransitionError::MutationAlreadyActive);
        }
        if !matches!(
            self.state,
            ExecutionState::Live | ExecutionState::PartiallyFilled
        ) {
            return Err(ExecutionTransitionError::MutationNotAllowed(self.state));
        }
        if self.mutations.len() >= MAX_ORDER_MUTATIONS_PER_EXECUTION {
            return Err(ExecutionTransitionError::MutationCapacityExceeded);
        }
        self.mutations.push(mutation);
        Ok(true)
    }

    pub fn begin_mutation_submission(
        &mut self,
        mutation_id: &str,
    ) -> Result<(), ExecutionTransitionError> {
        self.mutation_transition(
            mutation_id,
            OrderMutationState::Prepared,
            OrderMutationState::Submitting,
        )
    }

    pub fn acknowledge_mutation(
        &mut self,
        mutation_id: &str,
    ) -> Result<(), ExecutionTransitionError> {
        self.mutation_transition(
            mutation_id,
            OrderMutationState::Submitting,
            OrderMutationState::Acknowledged,
        )
    }

    pub fn mark_mutation_unknown(
        &mut self,
        mutation_id: &str,
    ) -> Result<(), ExecutionTransitionError> {
        self.mutation_transition(
            mutation_id,
            OrderMutationState::Submitting,
            OrderMutationState::Unknown,
        )
    }

    pub fn reject_mutation(
        &mut self,
        mutation_id: &str,
        code: impl Into<String>,
    ) -> Result<(), ExecutionTransitionError> {
        let mutation = self.mutation_mut(mutation_id)?;
        if mutation.state != OrderMutationState::Submitting {
            return Err(ExecutionTransitionError::InvalidMutationTransition {
                from: mutation.state,
                to: OrderMutationState::Rejected,
            });
        }
        let code = code.into();
        if code.trim().is_empty() {
            return Err(ExecutionTransitionError::EmptyRejectionCode);
        }
        mutation.rejection_code = Some(code);
        mutation.state = OrderMutationState::Rejected;
        Ok(())
    }

    pub fn recover_inflight_mutation(&mut self) -> bool {
        let Some(mutation) = self
            .mutations
            .iter_mut()
            .rev()
            .find(|mutation| mutation.state == OrderMutationState::Submitting)
        else {
            return false;
        };
        mutation.state = OrderMutationState::Unknown;
        true
    }

    pub fn resolve_active_mutation(
        &mut self,
        resolution: OrderMutationResolution,
    ) -> Result<(), ExecutionTransitionError> {
        if resolution == OrderMutationResolution::Pending {
            return Ok(());
        }
        let mutation = self
            .mutations
            .iter_mut()
            .rev()
            .find(|mutation| !mutation.state.is_terminal())
            .ok_or_else(|| ExecutionTransitionError::MutationNotFound("active".to_owned()))?;
        let allowed = matches!(
            mutation.state,
            OrderMutationState::Acknowledged | OrderMutationState::Unknown
        ) || (mutation.state == OrderMutationState::Prepared
            && resolution == OrderMutationResolution::Superseded);
        if !allowed {
            return Err(ExecutionTransitionError::InvalidMutationTransition {
                from: mutation.state,
                to: match resolution {
                    OrderMutationResolution::Applied => OrderMutationState::Applied,
                    OrderMutationResolution::Superseded => OrderMutationState::Superseded,
                    OrderMutationResolution::Pending => unreachable!(),
                },
            });
        }
        mutation.state = match resolution {
            OrderMutationResolution::Applied => OrderMutationState::Applied,
            OrderMutationResolution::Superseded => OrderMutationState::Superseded,
            OrderMutationResolution::Pending => unreachable!(),
        };
        Ok(())
    }

    fn mutation_transition(
        &mut self,
        mutation_id: &str,
        from: OrderMutationState,
        to: OrderMutationState,
    ) -> Result<(), ExecutionTransitionError> {
        let mutation = self.mutation_mut(mutation_id)?;
        if mutation.state != from {
            return Err(ExecutionTransitionError::InvalidMutationTransition {
                from: mutation.state,
                to,
            });
        }
        mutation.state = to;
        Ok(())
    }

    fn mutation_mut(
        &mut self,
        mutation_id: &str,
    ) -> Result<&mut OrderMutationRecord, ExecutionTransitionError> {
        self.mutations
            .iter_mut()
            .find(|mutation| mutation.mutation_id == mutation_id)
            .ok_or_else(|| ExecutionTransitionError::MutationNotFound(mutation_id.to_owned()))
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
        EXECUTION_PLAN_SCHEMA_V1, ExecutionAction, OpenRiskEvidence, OrderSide, OrderType,
        PositionSide, TradeMode, derive_protective_algo_client_id,
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

    fn protected_plan() -> ExecutionPlan {
        let mut value = plan();
        value.open_risk = Some(OpenRiskEvidence {
            fee_generation: "sha256:fee".to_owned(),
            requested_max_settle_notional: "10".to_owned(),
            requested_max_loss_settle: "1".to_owned(),
            requested_target_rr: "2".to_owned(),
            stop_price: "0.09".to_owned(),
            target_price: "0.12".to_owned(),
            entry_settle_notional: "10".to_owned(),
            stop_loss_settle: "1".to_owned(),
            actual_target_rr: "2".to_owned(),
        });
        value
    }

    #[test]
    fn protection_is_derived_without_copying_stop_or_target_prices() {
        let record = ExecutionRecord::new(protected_plan());
        let protection = record.protection.as_ref().expect("protection");
        assert_eq!(
            protection.algo_client_order_id,
            derive_protective_algo_client_id(&record.plan.intent_id)
        );
        assert_eq!(protection.policy_version, PROTECTIVE_ORDER_POLICY_V1);
        assert_eq!(
            protection.trigger_price_basis,
            ProtectiveTriggerPriceBasis::Mark
        );
        assert_eq!(protection.status, ProtectiveOrderStatus::Pending);
    }

    #[test]
    fn terminal_pending_protection_can_resolve_later_idempotently() {
        let mut record = ExecutionRecord::new(protected_plan());
        record.begin_submission().expect("submit");
        record.acknowledge("ord-1").expect("ack");
        record
            .reconcile_found_with_protection(
                "ord-1",
                ExchangeOrderState::Canceled,
                Some(ProtectiveOrderResolution::Pending),
            )
            .expect("terminal pending");
        assert!(record.state.is_terminal());
        assert!(record.protection_requires_reconciliation());

        record
            .reconcile_found_with_protection(
                "ord-1",
                ExchangeOrderState::Canceled,
                Some(ProtectiveOrderResolution::Active {
                    algo_order_id: "algo-1".to_owned(),
                    covered_size: "0.4".to_owned(),
                }),
            )
            .expect("late active proof");
        assert_eq!(
            record.protection.as_ref().expect("protection").status,
            ProtectiveOrderStatus::Active
        );
        assert!(!record.protection_requires_reconciliation());
    }

    #[test]
    fn reverse_link_is_validated_and_idempotent() {
        let mut record = ExecutionRecord::new(plan());
        let link = ReverseExecutionLink::close(
            "intent_reverse_01234567",
            "rvx00000000000000000000000000001",
            PositionSide::Short,
        )
        .expect("reverse link");
        record.attach_reverse(link.clone()).expect("attach");
        record.attach_reverse(link).expect("idempotent");
        assert_eq!(
            record.reverse.as_ref().expect("reverse").leg,
            ReverseLeg::Close
        );
    }

    #[test]
    fn mutation_is_durable_idempotent_and_fail_closed() {
        let mut record = ExecutionRecord::new(plan());
        record.begin_submission().expect("submit");
        record.acknowledge("ord-1").expect("ack");
        record
            .reconcile_found("ord-1", ExchangeOrderState::Live)
            .expect("live");

        let mutation = OrderMutationRecord::amend(
            "mutation_01234567",
            "amend000000000000000000000000001",
            None,
            Some("0.11".to_owned()),
        )
        .expect("mutation");
        assert!(record.prepare_mutation(mutation.clone()).expect("created"));
        assert!(!record.prepare_mutation(mutation).expect("idempotent"));

        record
            .begin_mutation_submission("mutation_01234567")
            .expect("submit mutation");
        assert!(record.recover_inflight_mutation());
        assert_eq!(
            record.active_mutation().expect("active").state,
            OrderMutationState::Unknown
        );
        assert!(matches!(
            record.begin_mutation_submission("mutation_01234567"),
            Err(ExecutionTransitionError::InvalidMutationTransition { .. })
        ));
    }

    #[test]
    fn applied_amend_updates_effective_terms_without_rewriting_plan() {
        let mut record = ExecutionRecord::new(plan());
        record.begin_submission().expect("submit");
        record.acknowledge("ord-1").expect("ack");
        record
            .reconcile_found("ord-1", ExchangeOrderState::Live)
            .expect("live");
        let original_price = record.plan.price.clone();

        record
            .prepare_mutation(
                OrderMutationRecord::amend(
                    "mutation_01234567",
                    "amend000000000000000000000000001",
                    Some("2".to_owned()),
                    Some("0.11".to_owned()),
                )
                .expect("mutation"),
            )
            .expect("prepare");
        record
            .begin_mutation_submission("mutation_01234567")
            .expect("submit mutation");
        record
            .acknowledge_mutation("mutation_01234567")
            .expect("ack mutation");
        record
            .resolve_active_mutation(OrderMutationResolution::Applied)
            .expect("applied");

        assert_eq!(record.plan.price, original_price);
        assert_eq!(record.effective_size(), "2");
        assert_eq!(record.effective_price(), "0.11");
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

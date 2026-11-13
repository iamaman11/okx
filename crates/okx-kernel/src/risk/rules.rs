//! Pure capacity decisions ported from the same legacy RiskBook predicates.
//! The owner must re-read identity, policy, census and revisions in its SQL transaction.
use super::model::{CampaignCapacity, RiskProposal, ScopeCapacity};
use super::money::RiskAmount;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RiskRuleError {
    #[error("BUDGET_BOUND_BREACH")]
    BoundBreach,
    #[error("CAMPAIGN_ENTRY_BUDGET_EXHAUSTED")]
    EntryBudgetExhausted,
    #[error("CAMPAIGN_LOSS_BUDGET_EXHAUSTED")]
    LossBudgetExhausted,
    #[error("ORDER_OR_STRATEGY_RISK_LIMIT")]
    OrderOrStrategyLimit,
    #[error("STRATEGY_AGGREGATE_RISK_LIMIT")]
    StrategyAggregateLimit,
    #[error("ACCOUNT_ALREADY_OVER_LIMIT")]
    AccountAlreadyOverLimit,
    #[error("INSUFFICIENT_AGGREGATE_OR_ACCOUNT_RISK")]
    InsufficientAggregateOrAccountRisk,
}

/// Called only after the persisted policy, opening proof and cohort are validated.
pub fn evaluate_campaign_capacity(
    budget: &CampaignCapacity,
    request: &RiskProposal,
) -> Result<(), RiskRuleError> {
    if !budget.entry_bound_breach_usd.is_zero() {
        return Err(RiskRuleError::BoundBreach);
    }
    if RiskAmount::sum_exceeds(
        &[
            &budget.entry_consumed_usd,
            &budget.entry_reserved_usd,
            &request.entry_upper_usd,
        ],
        &budget.entry_limit_usd,
    ) {
        return Err(RiskRuleError::EntryBudgetExhausted);
    }
    if RiskAmount::sum_exceeds(
        &[
            &budget.loss_consumed_usd,
            &budget.loss_reserved_usd,
            &request.loss_usd,
        ],
        &budget.loss_limit_usd,
    ) {
        return Err(RiskRuleError::LossBudgetExhausted);
    }
    Ok(())
}

/// Caller must include all accounts' source-attested occupancy and central reservations.
pub fn evaluate_scope_capacity(
    scope: &ScopeCapacity,
    request: &RiskProposal,
) -> Result<(), RiskRuleError> {
    if request.loss_usdt > scope.order_limit_usdt || request.loss_usdt > scope.strategy_limit_usdt {
        return Err(RiskRuleError::OrderOrStrategyLimit);
    }
    if RiskAmount::sum_exceeds(
        &[&scope.strategy_occupied_usdt, &request.loss_usdt],
        &scope.strategy_limit_usdt,
    ) {
        return Err(RiskRuleError::StrategyAggregateLimit);
    }
    if scope.account_occupied_usdt > scope.account_limit_usdt {
        return Err(RiskRuleError::AccountAlreadyOverLimit);
    }
    if RiskAmount::sum_exceeds(
        &[&scope.global_occupied_usdt, &request.loss_usdt],
        &scope.global_limit_usdt,
    ) || RiskAmount::sum_exceeds(
        &[&scope.account_occupied_usdt, &request.loss_usdt],
        &scope.account_limit_usdt,
    ) {
        return Err(RiskRuleError::InsufficientAggregateOrAccountRisk);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::risk::model::{CampaignCapacity, RiskProposal, ScopeCapacity};
    use crate::risk::money::RiskAmount;

    fn amount(value: &str) -> RiskAmount {
        RiskAmount::parse(value).unwrap()
    }

    fn proposal() -> RiskProposal {
        RiskProposal {
            loss_usdt: amount("5"),
            loss_usd: amount("5.005"),
            entry_upper_usd: amount("2.5"),
        }
    }

    #[test]
    fn risk_book_contract_independent_entry_and_loss_budgets() {
        let budget = CampaignCapacity {
            loss_limit_usd: amount("9"),
            loss_consumed_usd: amount("2"),
            loss_reserved_usd: amount("2"),
            entry_limit_usd: amount("5"),
            entry_consumed_usd: amount("2"),
            entry_reserved_usd: amount("1"),
            entry_bound_breach_usd: amount("0"),
        };
        assert_eq!(
            evaluate_campaign_capacity(&budget, &proposal()),
            Err(RiskRuleError::EntryBudgetExhausted)
        );
        let new_budget = CampaignCapacity {
            entry_limit_usd: amount("6"),
            ..budget.clone()
        };
        assert_eq!(
            evaluate_campaign_capacity(&new_budget, &proposal()),
            Err(RiskRuleError::LossBudgetExhausted)
        );
        let permitted = CampaignCapacity {
            loss_limit_usd: amount("10.01"),
            ..new_budget.clone()
        };
        assert_eq!(evaluate_campaign_capacity(&permitted, &proposal()), Ok(()));
        let breached = CampaignCapacity {
            entry_bound_breach_usd: amount("0.001"),
            ..permitted
        };
        assert_eq!(
            evaluate_campaign_capacity(&breached, &proposal()),
            Err(RiskRuleError::BoundBreach)
        );
    }

    #[test]
    fn risk_book_contract_second_account_cannot_overspend_shared_limit() {
        let scope = ScopeCapacity {
            global_limit_usdt: amount("10"),
            global_occupied_usdt: amount("6"),
            account_limit_usdt: amount("10"),
            account_occupied_usdt: amount("0"),
            strategy_limit_usdt: amount("10"),
            strategy_occupied_usdt: amount("0"),
            order_limit_usdt: amount("10"),
        };
        assert_eq!(
            evaluate_scope_capacity(&scope, &proposal()),
            Err(RiskRuleError::InsufficientAggregateOrAccountRisk)
        );
        let available = ScopeCapacity {
            global_occupied_usdt: amount("5"),
            ..scope.clone()
        };
        assert_eq!(evaluate_scope_capacity(&available, &proposal()), Ok(()));
        let account_over = ScopeCapacity {
            account_limit_usdt: amount("4"),
            account_occupied_usdt: amount("5"),
            ..scope
        };
        assert_eq!(
            evaluate_scope_capacity(&account_over, &proposal()),
            Err(RiskRuleError::AccountAlreadyOverLimit)
        );
    }
}

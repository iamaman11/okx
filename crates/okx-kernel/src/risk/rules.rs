//! Pure capacity decisions ported from the same legacy RiskBook predicates.
//! The owner must re-read identity, policy, census and revisions in its SQL transaction.
use super::model::{CampaignCapacity, PostCausalProof, RiskProposal, ScopeCapacity};
use super::money::{MoneyError, RiskAmount};
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
    #[error("ACCOUNT_ENTRY_UPPER_LIMIT_EXCEEDED")]
    AccountEntryLimitExceeded,
    #[error("POST_CAUSAL_PROOF_UNPROVEN")]
    PostCausalProofUnproven,
}

/// Pure shape/causality check for the complete registered cohort.
/// A valid shape is NOT an attestation that the sources were actually read.
/// That trust boundary is enforced separately by the native account producers.
pub fn validate_post_causality(
    proof: &PostCausalProof,
    reservation_key: &str,
    original_incarnation: &str,
    policy_account_keys: &[String],
) -> Result<(), RiskRuleError> {
    let hex = |s: &str, len: usize| {
        s.len() == len
            && s.bytes()
                .all(|ch| ch.is_ascii_digit() || (b'a'..=b'f').contains(&ch))
    };
    if proof.reserve_anchor != reservation_key
        || !hex(&proof.native_incarnation, 32)
        || proof.native_incarnation != original_incarnation
        || proof.elapsed_to_proof_ms > 5_000
        || policy_account_keys.is_empty()
        || proof.account_reads.len() != policy_account_keys.len()
        || proof
            .account_reads
            .iter()
            .zip(policy_account_keys)
            .any(|(read, member)| {
                read.account_key != *member
                    || read.private_generation == 0
                    || !hex(&read.source_read_id, 64)
                    || !hex(&read.content_digest, 64)
                    || read.started_after_held_ms > read.completed_after_held_ms
                    || read.completed_after_held_ms > proof.elapsed_to_proof_ms
            })
    {
        return Err(RiskRuleError::PostCausalProofUnproven);
    }
    Ok(())
}

/// Remaining budget is a read-only statement about one committed book.
/// Historical obligations can exceed a lowered limit; never return a
/// negative number or reinterpret that debt as newly available capacity.
pub fn remaining_budget(
    limit: &RiskAmount,
    consumed: &RiskAmount,
    reserved: &RiskAmount,
) -> Result<RiskAmount, MoneyError> {
    if RiskAmount::sum_exceeds(&[consumed, reserved], limit) {
        return Ok(RiskAmount::zero());
    }
    limit.checked_sub(consumed)?.checked_sub(reserved)
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

/// Every member of an authorized campaign cohort must respect its individual cap,
/// including occupancy established before this particular account's request.
pub fn validate_account_occupancy(
    limit: &RiskAmount,
    occupied: &RiskAmount,
) -> Result<(), RiskRuleError> {
    if occupied > limit {
        Err(RiskRuleError::AccountAlreadyOverLimit)
    } else {
        Ok(())
    }
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
    validate_account_occupancy(&scope.account_limit_usdt, &scope.account_occupied_usdt)?;
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
    fn risk_book_contract_causal_post_requires_exact_cohort_and_incarnation() {
        use crate::risk::model::{PostAccountRead, PostCausalProof};
        let accounts = vec![
            "okx:real:".to_owned() + &"a".repeat(64),
            "okx:real:".to_owned() + &"b".repeat(64),
        ];
        let proof = PostCausalProof {
            reserve_anchor: "original-reservation".into(),
            native_incarnation: "f".repeat(32),
            elapsed_to_proof_ms: 3,
            account_reads: accounts
                .iter()
                .map(|key| PostAccountRead {
                    account_key: key.clone(),
                    private_generation: 7,
                    source_read_id: "d".repeat(64),
                    content_digest: "c".repeat(64),
                    started_after_held_ms: 0,
                    completed_after_held_ms: 2,
                })
                .collect(),
        };
        assert_eq!(
            validate_post_causality(&proof, "original-reservation", &"f".repeat(32), &accounts),
            Ok(())
        );
        let mut missing = proof.clone();
        missing.account_reads.pop();
        assert_eq!(
            validate_post_causality(&missing, "original-reservation", &"f".repeat(32), &accounts),
            Err(RiskRuleError::PostCausalProofUnproven)
        );
        let mut old_session = proof.clone();
        old_session.native_incarnation = "e".repeat(32);
        assert_eq!(
            validate_post_causality(
                &old_session,
                "original-reservation",
                &"f".repeat(32),
                &accounts
            ),
            Err(RiskRuleError::PostCausalProofUnproven)
        );
        let mut bad_ordering = proof.clone();
        bad_ordering.account_reads[1].started_after_held_ms = 4;
        assert_eq!(
            validate_post_causality(
                &bad_ordering,
                "original-reservation",
                &"f".repeat(32),
                &accounts
            ),
            Err(RiskRuleError::PostCausalProofUnproven)
        );
        let mut late = proof;
        late.elapsed_to_proof_ms = 5_001;
        assert_eq!(
            validate_post_causality(&late, "original-reservation", &"f".repeat(32), &accounts),
            Err(RiskRuleError::PostCausalProofUnproven)
        );
    }

    #[test]
    fn risk_book_contract_exact_remaining_budget_never_mints_headroom() {
        assert_eq!(
            remaining_budget(&amount("10.1"), &amount("2.05"), &amount("3.005"))
                .unwrap()
                .canonical()
                .unwrap(),
            "5.045"
        );
        assert_eq!(
            remaining_budget(&amount("4"), &amount("3"), &amount("2"))
                .unwrap()
                .canonical()
                .unwrap(),
            "0"
        );
        // Intermediate sum may exceed legacy 28-digit serialization capacity,
        // but a safe zero-remaining decision must still be representable.
        assert_eq!(
            remaining_budget(
                &amount("1"),
                &amount("9999999999999999999999999999"),
                &amount("9999999999999999999999999999")
            )
            .unwrap()
            .canonical()
            .unwrap(),
            "0"
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

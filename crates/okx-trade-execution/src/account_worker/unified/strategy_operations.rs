//! Typed strategy commands on the existing installation SQL owner.
use super::UnifiedFinanceHandle;
use crate::{AccountStore, OperationReceipt, StoreError, StoreResult, StrategyInstance};
use std::time::Instant;

#[derive(Clone)]
pub struct StrategyMutationCommand {
    pub operation_id: String,
    pub request_hash: String,
    pub action: String,
    pub instance: StrategyInstance,
    pub expected_revision: Option<i64>,
    pub confirmed_configuration_digest: Option<String>,
    pub restore_revision: Option<i64>,
    pub time: i64,
}
#[derive(Clone)]
pub struct StrategyDecisionClaimCommand {
    pub instance_id: String,
    pub decision_key: String,
    pub input_digest: String,
    pub policy_revision: i64,
    pub expected_version: String,
    pub activation_revision: i64,
    pub time: i64,
}
impl UnifiedFinanceHandle {
    pub async fn strategy_configuration_at_revision(
        &self,
        deadline: Instant,
        account: String,
        instance_id: String,
        revision: i64,
    ) -> StoreResult<StrategyInstance> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            AccountStore::strategy_configuration_at_revision_sql(
                db,
                (&installation, &account),
                &instance_id,
                revision,
            )
        })
        .await
    }
    pub async fn strategy_status(
        &self,
        deadline: Instant,
        account: String,
        instance: String,
        time: i64,
    ) -> StoreResult<serde_json::Value> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let result = AccountStore::strategy_status_sql(
                &tx,
                Some((&installation, &account)),
                &instance,
                time,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }
    pub async fn mutate_strategy_instance(
        &self,
        deadline: Instant,
        account: String,
        command: StrategyMutationCommand,
    ) -> StoreResult<OperationReceipt> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            super::finance_authority::require_preparation_authority(&tx, &installation)?;
            let mut command = command;
            let scope = Some((installation.as_str(), account.as_str()));
            let replay = AccountStore::operation_sql(&tx, scope, &command.operation_id)?.is_some();
            if !replay && command.action == "start" {
                let current =
                    AccountStore::instance_sql(&tx, scope, &command.instance.instance_id)?
                        .ok_or(StoreError::Invalid("STRATEGY_INSTANCE_NOT_FOUND"))?;
                if Some(current.policy_revision) != command.expected_revision {
                    return Err(StoreError::Invalid("INSTANCE_REVISION_CONFLICT"));
                }
                if command.confirmed_configuration_digest.as_deref()
                    != Some(current.configuration_digest(&account)?.as_str())
                {
                    return Err(StoreError::Invalid(
                        "STRATEGY_CONFIGURATION_DIGEST_MISMATCH",
                    ));
                }
                if current.enabled {
                    return Err(StoreError::Invalid("STRATEGY_ALREADY_ACTIVE"));
                }
            } else if !replay && command.action == "restore_configuration" {
                let source = command
                    .restore_revision
                    .filter(|revision| *revision > 0 && Some(*revision) < command.expected_revision)
                    .ok_or(StoreError::Invalid("STRATEGY_RESTORE_REVISION_INVALID"))?;
                let snapshot = AccountStore::strategy_configuration_at_revision_sql(
                    &tx,
                    (&installation, &account),
                    &command.instance.instance_id,
                    source,
                )?;
                let next = command.instance.policy_revision;
                command.instance = snapshot;
                command.instance.policy_revision = next;
            } else if !replay && command.action == "stop" {
                // Protective stop targets the current instance in the writer turn.
                command.instance = AccountStore::instance_sql(
                    &tx,
                    Some((&installation, &account)),
                    &command.instance.instance_id,
                )?
                .ok_or(StoreError::Invalid("STRATEGY_INSTANCE_NOT_FOUND"))?;
                command.expected_revision = Some(command.instance.policy_revision);
                command.instance.policy_revision = command
                    .instance
                    .policy_revision
                    .checked_add(1)
                    .ok_or(StoreError::Invalid("INSTANCE_REVISION_OVERFLOW"))?;
            }
            let result = AccountStore::mutate_instance_sql(
                &tx,
                Some((&installation, &account)),
                &command.operation_id,
                &command.request_hash,
                &command.action,
                &command.instance,
                command.expected_revision,
                command.time,
            )?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(result)
        })
        .await
    }
    pub async fn claim_strategy_decision(
        &self,
        deadline: Instant,
        account: String,
        command: StrategyDecisionClaimCommand,
    ) -> StoreResult<Option<String>> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            super::finance_authority::require_preparation_authority(&tx, &installation)?;
            AccountStore::verify_strategy_activation_sql(
                &tx,
                (&installation, &account),
                &command.instance_id,
                command.activation_revision,
            )?;
            let result = AccountStore::claim_strategy_decision_sql(
                &tx,
                Some((&installation, &account)),
                &command.instance_id,
                &command.decision_key,
                &command.input_digest,
                command.policy_revision,
                Some(&command.expected_version),
                command.time,
            )?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(result)
        })
        .await
    }
    pub async fn finish_strategy_no_trade(
        &self,
        deadline: Instant,
        account: String,
        instance: String,
        key: String,
        expected_version: String,
        time: i64,
    ) -> StoreResult<()> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            super::finance_authority::require_preparation_authority(&tx, &installation)?;
            AccountStore::finish_strategy_no_trade_sql(
                &tx,
                Some((&installation, &account)),
                &instance,
                &key,
                Some(&expected_version),
                time,
            )?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(())
        })
        .await
    }
}

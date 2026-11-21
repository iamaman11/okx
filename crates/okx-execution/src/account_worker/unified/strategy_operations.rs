//! Typed strategy commands on the existing installation SQL owner.
use super::UnifiedFinanceHandle;
use crate::{
    AccountStore, OperationReceipt, StoreError, StoreResult, StrategyInstance,
    StrategyPositionSnapshot,
};
use std::time::Instant;

#[derive(Clone)]
pub struct StrategyMutationCommand {
    pub operation_id: String,
    pub request_hash: String,
    pub action: String,
    pub instance: StrategyInstance,
    pub expected_revision: Option<i64>,
    pub time: i64,
}
#[derive(Clone)]
pub struct StrategyDecisionClaimCommand {
    pub instance_id: String,
    pub decision_key: String,
    pub input_digest: String,
    pub policy_revision: i64,
    pub expected_version: String,
    pub time: i64,
}
impl UnifiedFinanceHandle {
    pub async fn strategy_position_snapshot(
        &self,
        deadline: Instant,
        account: String,
        instance_id: String,
        instrument_id: String,
    ) -> StoreResult<StrategyPositionSnapshot> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let snapshot = AccountStore::strategy_position_snapshot_sql(
                &tx,
                Some((&installation, &account)),
                &instance_id,
                &instrument_id,
            )?;
            tx.commit()?;
            Ok(snapshot)
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
            // Start/Stop carry no caller revision. Read their current revision
            // inside the writer turn, as the original serialized command did.
            let mut command = command;
            if matches!(command.action.as_str(), "start" | "stop")
                && AccountStore::operation_sql(
                    &tx,
                    Some((&installation, &account)),
                    &command.operation_id,
                )?
                .is_none()
            {
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

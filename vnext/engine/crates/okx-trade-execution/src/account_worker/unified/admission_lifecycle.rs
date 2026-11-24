//! Typed admission operations on the single installation SQL writer.
//! Every mutation retains the real activation gate and original ledger rules.
use super::*;
use crate::{
    AccountStore, Decision, EntryAdmissionEvidence, EntryQuoteEvidence, NewIntent, StoreError,
    StoreResult, StoredIntent, StrategyDefinitionVersion,
};
use std::time::Instant;

impl UnifiedFinanceHandle {
    pub async fn prepare_admission(
        &self,
        deadline: Instant,
        account: String,
        decision: Decision,
        intent: NewIntent,
    ) -> StoreResult<StoredIntent> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            super::finance_authority::require_preparation_authority(&tx, &installation)?;
            if intent.plan.strategy_instance == "manual" {
                AccountStore::ensure_manual_instance_sql(
                    &tx,
                    Some((&installation, &account)),
                    super::opening_ingress::owner_now_ms()?,
                )?;
            }
            let receipt = apply_original_unified_finance_transaction(
                &tx,
                &installation,
                UnifiedFinanceWrite::Prepare {
                    account: account.clone(),
                    decision,
                    intent,
                },
            )?;
            let UnifiedFinanceWriteReceipt::Prepared { intent_id, .. } = receipt else {
                return Err(StoreError::Invalid("UNIFIED_PREPARE_RECEIPT_UNEXPECTED"));
            };
            let stored = AccountStore::find_intent_sql(
                &tx,
                Some((&installation, &account)),
                "intent_id",
                &intent_id,
            )?
            .ok_or(StoreError::Invalid("UNIFIED_PREPARED_INTENT_MISSING"))?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(stored)
        })
        .await
    }
    pub async fn ensure_manual_instance(
        &self,
        deadline: Instant,
        account: String,
    ) -> StoreResult<StrategyDefinitionVersion> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            super::finance_authority::require_preparation_authority(&tx, &installation)?;
            let result = AccountStore::ensure_manual_instance_sql(
                &tx,
                Some((&installation, &account)),
                super::opening_ingress::owner_now_ms()?,
            )?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(result)
        })
        .await
    }
    pub async fn bind_entry_evidence(
        &self,
        deadline: Instant,
        account: String,
        quote: EntryQuoteEvidence,
        admission: Option<EntryAdmissionEvidence>,
    ) -> StoreResult<()> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            super::finance_authority::require_preparation_authority(&tx, &installation)?;
            if admission
                .as_ref()
                .is_some_and(|proof| quote.intent_id != proof.intent_id)
            {
                return Err(StoreError::Invalid("ENTRY_ADMISSION_INTENT_MISMATCH"));
            }
            let scope = Some((installation.as_str(), account.as_str()));
            AccountStore::bind_entry_quote_sql(&tx, scope, &quote)?;
            if let Some(proof) = admission {
                AccountStore::bind_entry_admission_sql(&tx, scope, &proof)?;
            }
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(())
        })
        .await
    }
    pub async fn mark_reserve_attempt(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
        reservation_key: String,
    ) -> StoreResult<()> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            super::finance_authority::require_preparation_authority(&tx, &installation)?;
            AccountStore::mark_reserve_attempt_sql(
                &tx,
                Some((&installation, &account)),
                &intent,
                &reservation_key,
                super::opening_ingress::owner_now_ms()?,
            )?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(())
        })
        .await
    }
    pub async fn reject_unsent(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
        reason: String,
    ) -> StoreResult<bool> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            super::finance_authority::require_preparation_authority(&tx, &installation)?;
            let result = AccountStore::reject_unsent_sql(
                &tx,
                Some((&installation, &account)),
                &intent,
                &reason,
                super::opening_ingress::owner_now_ms()?,
                false,
            )?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(result)
        })
        .await
    }
    pub async fn reject_denied_reserve(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
        reason: String,
    ) -> StoreResult<()> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline),move |db| {
            let tx=db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            super::finance_authority::require_preparation_authority(&tx,&installation)?;
            // Native denial is independently addressable in the SAME SQL;
            // a caller-supplied reason cannot deny an existing reservation.
            let reserved:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM risk_entries WHERE installation_id=?1 AND account_key=?2 AND intent_id=?3)",rusqlite::params![installation,account,intent],|r|r.get(0))?;
            if reserved {return Err(StoreError::Invalid("NATIVE_RESERVE_DENIAL_UNPROVEN"));}
            AccountStore::reject_unsent_sql(&tx,Some((&installation,&account)),&intent,&reason,super::opening_ingress::owner_now_ms()?,true)?;
            tx.commit().map_err(|_|StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY"))?;
            Ok(())
        }).await
    }
    pub async fn unsent_before_reserve(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
    ) -> StoreResult<bool> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let result = AccountStore::unsent_before_reserve_sql(
                &tx,
                Some((&installation, &account)),
                &intent,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }
    pub async fn strategy_instances(
        &self,
        deadline: Instant,
        account: String,
    ) -> StoreResult<Vec<crate::StrategyInstance>> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let result = AccountStore::instances_sql(&tx, Some((&installation, &account)))?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }
    pub async fn plan_definition(
        &self,
        deadline: Instant,
        account: String,
        instance: String,
        record_risk_state: bool,
    ) -> StoreResult<StrategyDefinitionVersion> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let behavior = if record_risk_state && instance == "manual" {
                rusqlite::TransactionBehavior::Immediate
            } else {
                rusqlite::TransactionBehavior::Deferred
            };
            let tx = db.transaction_with_behavior(behavior)?;
            if record_risk_state {
                super::finance_authority::require_preparation_authority(&tx, &installation)?;
            }
            let result = AccountStore::plan_definition_sql(
                &tx,
                Some((&installation, &account)),
                &instance,
                record_risk_state,
                super::opening_ingress::owner_now_ms()?,
            )?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(result)
        })
        .await
    }
}

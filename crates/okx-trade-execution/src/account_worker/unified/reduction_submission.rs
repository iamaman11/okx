//! Local reductions retain the original account CAS without minting global risk.
use super::{UnifiedFinanceHandle, require_verified_live_finance_authority};
use crate::{
    AccountStore, CentralReceipt, CentralReceiptView, CommittedVenueSend, StoreError, StoreResult,
    TradeIntentLookup,
};
use std::time::Instant;

fn reduction_intent(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    account: &str,
    intent: &str,
) -> StoreResult<crate::StoredIntent> {
    let original = AccountStore::lookup_trade_intent_sql(
        tx,
        Some((installation, account)),
        &TradeIntentLookup::IntentId(intent.into()),
    )?
    .ok_or(StoreError::Invalid("LOCAL_REDUCTION_INTENT_MISSING"))?;
    if original.risk_increase || !matches!(original.plan.operation.as_str(), "close" | "reduce") {
        return Err(StoreError::Invalid("LOCAL_REDUCTION_ONLY"));
    }
    if original.plan.quantity <= rust_decimal::Decimal::ZERO
        || original.plan.planned_loss_usd != rust_decimal::Decimal::ZERO
    {
        return Err(StoreError::Invalid("LOCAL_REDUCTION_PLAN_INVALID"));
    }
    Ok(original)
}

fn reserve_reduction_sql(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    account: &str,
    receipt: &CentralReceipt,
    now: i64,
) -> StoreResult<()> {
    reduction_intent(tx, installation, account, &receipt.intent_id)?;
    if receipt.installation_id != installation
        || receipt.account_key != account
        || receipt.reservation_key != format!("local:{}", receipt.intent_id)
        || receipt.requested_loss_usd != rust_decimal::Decimal::ZERO
    {
        return Err(StoreError::Invalid("LOCAL_REDUCTION_RECEIPT_MISMATCH"));
    }
    AccountStore::reserve_sql_transaction(tx, Some((installation, account)), receipt, now)
}

fn claim_reduction_sql(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    account: &str,
    intent: &str,
    plan: &str,
    revision: i64,
    now: i64,
) -> StoreResult<bool> {
    let original = reduction_intent(tx, installation, account, intent)?;
    if original.plan_hash != plan {
        return Err(StoreError::Invalid("POST_CLAIM_PLAN_MISMATCH"));
    }
    // A repeated call reads the original non-RESERVED state and cannot mint a
    // capability; inspecting POSTING/UNKNOWN is never permission to resend.
    if original.state != crate::IntentState::Reserved {
        return Ok(false);
    }
    let receipt = AccountStore::reservation_receipt_sql(
        tx,
        Some((installation, account)),
        intent,
        CentralReceiptView::Retained,
    )?
    .ok_or(StoreError::Invalid("LOCAL_REDUCTION_RECEIPT_MISSING"))?;
    let local = format!("local:{intent}");
    if receipt.reservation_key != local || receipt.plan_hash != plan {
        return Err(StoreError::Invalid("LOCAL_REDUCTION_RECEIPT_MISMATCH"));
    }
    AccountStore::begin_post_sql_transaction(
        tx,
        Some((installation, account)),
        intent,
        revision,
        now,
        Some(&local),
    )
}

impl UnifiedFinanceHandle {
    pub async fn reserve_local_reduction(
        &self,
        deadline: Instant,
        account: String,
        receipt: CentralReceipt,
    ) -> StoreResult<()> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_verified_live_finance_authority(&tx, &installation)?;
            reserve_reduction_sql(
                &tx,
                &installation,
                &account,
                &receipt,
                super::opening_ingress::owner_now_ms()?,
            )?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(())
        })
        .await
    }

    pub async fn claim_local_reduction_send(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
        plan: String,
        revision: i64,
    ) -> StoreResult<Option<CommittedVenueSend>> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_verified_live_finance_authority(&tx, &installation)?;
            let claimed = claim_reduction_sql(
                &tx,
                &installation,
                &account,
                &intent,
                &plan,
                revision,
                super::opening_ingress::owner_now_ms()?,
            )?;
            if !claimed {
                return Ok(None);
            }
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(Some(CommittedVenueSend::after_commit(
                installation,
                account,
                intent,
                plan,
            )))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Decision, IntentPlan, NewIntent};
    use rusqlite::{Connection, TransactionBehavior, params};
    use rust_decimal::Decimal;

    fn fixture(operation: &str, increasing_b: bool, automated: bool) -> Connection {
        let mut db = Connection::open_in_memory().unwrap();
        db.execute_batch(include_str!("../../../schema/finance-unified-v1.sql"))
            .unwrap();
        db.pragma_update(None, "foreign_keys", "ON").unwrap();
        for account in ["A", "B"] {
            db.execute(
                "INSERT INTO account_meta VALUES(1,?1,'I',2,1,'source','initialize','release')",
                [account],
            )
            .unwrap();
            db.execute(
                "INSERT INTO account_policy VALUES('I',?1,1,1,1,'campaign','10','0')",
                [account],
            )
            .unwrap();
            let tx = db
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            let scope = Some(("I", account));
            let instance_id = if automated { "automated" } else { "manual" };
            let definition = if automated {
                let instance = crate::StrategyInstance {
                    instance_id: instance_id.into(),
                    definition_id: "no_trade".into(),
                    definition_version: "1".into(),
                    parameters: "{}".into(),
                    enabled: true,
                    policy_revision: 1,
                    budget: Decimal::ONE,
                    last_decision_key: None,
                };
                AccountStore::mutate_instance_sql(
                    &tx, scope, "create", "create", "create", &instance, None, 1,
                )
                .unwrap();
                AccountStore::strategy_definition_sql(&tx, scope, instance_id).unwrap()
            } else {
                AccountStore::ensure_manual_instance_sql(&tx, scope, 1).unwrap()
            };
            AccountStore::prepare_sql_transaction(
                &tx,
                Some(("I", account)),
                &Decision {
                    decision_id: "decision".into(),
                    instance_id: automated.then(|| instance_id.into()),
                    decision_at: 2,
                    input_digest: "inputs".into(),
                    policy_revision: 1,
                    code_version: "test".into(),
                },
                &NewIntent {
                    intent_id: "intent".into(),
                    idempotency_key: "request".into(),
                    request_hash: "body".into(),
                    client_order_id: "client".into(),
                    deadline: 100,
                    plan_hash: "plan".into(),
                    policy_revision: 1,
                    risk_increase: increasing_b && account == "B",
                    instrument_id: "BTC-USDT-SWAP".into(),
                    plan: IntentPlan {
                        operation: operation.into(),
                        side: "sell".into(),
                        quantity: Decimal::ONE,
                        limit_price: Decimal::from(100),
                        stop_price: None,
                        strategy_instance: instance_id.into(),
                        planned_loss_usd: Decimal::ZERO,
                        source_generation: 1,
                        position_side: "long".into(),
                        private_source_generation: 1,
                        contract_value: Decimal::ONE,
                        inverse: false,
                        settlement_currency: "USDT".into(),
                        strategy_definition_id: definition.definition_id,
                        strategy_definition_version: definition.version,
                        strategy_parameters_digest: definition.parameters_digest,
                    },
                },
            )
            .unwrap();
            if automated {
                let intent = AccountStore::find_intent_sql(&tx, scope, "intent_id", "intent")
                    .unwrap()
                    .unwrap();
                AccountStore::bind_strategy_send_sql(&tx, ("I", account), &intent, 1, 2).unwrap();
            }
            tx.commit().unwrap();
        }
        db
    }
    fn receipt(account: &str) -> CentralReceipt {
        CentralReceipt {
            reservation_key: "local:intent".into(),
            account_key: account.into(),
            installation_id: "I".into(),
            instance_id: None,
            intent_id: "intent".into(),
            plan_hash: "plan".into(),
            policy_revision: 1,
            receipt_hash: "receipt".into(),
            requested_loss_usd: Decimal::ZERO,
            conversion_digest: "local".into(),
        }
    }
    #[test]
    fn risk_book_contract_reduction_reserve_and_post_use_original_scoped_once_cas() {
        let mut db = fixture("close", false, false);
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        assert!(reserve_reduction_sql(&tx, "I", "B", &receipt("A"), 3).is_err());
        reserve_reduction_sql(&tx, "I", "A", &receipt("A"), 3).unwrap();
        assert!(!claim_reduction_sql(&tx, "I", "B", "intent", "plan", 1, 4).unwrap());
        assert!(claim_reduction_sql(&tx, "I", "A", "intent", "wrong", 1, 4).is_err());
        assert!(claim_reduction_sql(&tx, "I", "A", "intent", "plan", 1, 4).unwrap());
        tx.commit().unwrap();
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        assert!(!claim_reduction_sql(&tx, "I", "A", "intent", "plan", 1, 5).unwrap());
        let state:String = tx.query_row("SELECT state FROM intents WHERE installation_id='I' AND account_key='B' AND intent_id='intent'",[],|r|r.get(0)).unwrap();
        assert_eq!(state, "PREPARED");
        tx.commit().unwrap();
    }
    #[test]
    fn risk_book_contract_reduction_rejects_increase_and_rolls_back_uncommitted_claim() {
        let mut db = fixture("close", true, false);
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        assert!(matches!(
            reserve_reduction_sql(&tx, "I", "B", &receipt("B"), 3),
            Err(StoreError::Invalid("LOCAL_REDUCTION_ONLY"))
        ));
        assert!(matches!(
            claim_reduction_sql(&tx, "I", "B", "intent", "plan", 1, 4),
            Err(StoreError::Invalid("LOCAL_REDUCTION_ONLY"))
        ));
        reserve_reduction_sql(&tx, "I", "A", &receipt("A"), 3).unwrap();
        assert!(claim_reduction_sql(&tx, "I", "A", "intent", "plan", 1, 4).unwrap());
        tx.rollback().unwrap();
        let state: String = db
            .query_row(
                "SELECT state FROM intents WHERE installation_id=?1 AND account_key=?2",
                params!["I", "A"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(state, "PREPARED");
    }
    #[test]
    fn risk_book_contract_reduction_rejects_open_with_forged_nonincreasing_flag() {
        let mut db = fixture("open", false, false);
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        assert!(matches!(
            reserve_reduction_sql(&tx, "I", "A", &receipt("A"), 3),
            Err(StoreError::Invalid("LOCAL_REDUCTION_ONLY"))
        ));
        assert!(matches!(
            claim_reduction_sql(&tx, "I", "A", "intent", "plan", 1, 4),
            Err(StoreError::Invalid("LOCAL_REDUCTION_ONLY"))
        ));
        tx.rollback().unwrap();
    }
    #[test]
    fn risk_book_contract_partial_reduce_uses_same_original_cas() {
        let mut db = fixture("reduce", false, false);
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        reserve_reduction_sql(&tx, "I", "A", &receipt("A"), 3).unwrap();
        assert!(claim_reduction_sql(&tx, "I", "A", "intent", "plan", 1, 4).unwrap());
        tx.commit().unwrap();
    }

    #[test]
    fn strategy_stop_and_send_are_ordered_by_the_same_writer() {
        let mut db = fixture("close", false, true);
        let change = |tx: &rusqlite::Transaction<'_>, account, action, revision, now| {
            let scope = Some(("I", account));
            let mut instance = AccountStore::instance_sql(tx, scope, "automated")
                .unwrap()
                .unwrap();
            instance.policy_revision = revision + 1;
            AccountStore::mutate_instance_sql(
                tx,
                scope,
                action,
                action,
                action,
                &instance,
                Some(revision),
                now,
            )
            .unwrap();
        };
        let tx = db.transaction().unwrap();
        for account in ["A", "B"] {
            let strategy_receipt = CentralReceipt {
                instance_id: Some("automated".into()),
                ..receipt(account)
            };
            reserve_reduction_sql(&tx, "I", account, &strategy_receipt, 3).unwrap();
        }
        assert!(claim_reduction_sql(&tx, "I", "B", "intent", "plan", 1, 4).unwrap());
        tx.commit().unwrap();

        let tx = db.transaction().unwrap();
        change(&tx, "A", "stop", 1, 5);
        change(&tx, "B", "stop", 1, 5);
        tx.commit().unwrap();

        let tx = db.transaction().unwrap();
        assert!(matches!(
            claim_reduction_sql(&tx, "I", "A", "intent", "plan", 1, 6),
            Err(StoreError::Invalid("STRATEGY_STOPPED"))
        ));
        assert!(!claim_reduction_sql(&tx, "I", "B", "intent", "plan", 1, 6).unwrap());
        change(&tx, "A", "start", 2, 7);
        assert!(matches!(
            claim_reduction_sql(&tx, "I", "A", "intent", "plan", 1, 8),
            Err(StoreError::Invalid("STRATEGY_ACTIVATION_CHANGED"))
        ));
        let unsent: Option<i64> = tx.query_row(
            "SELECT begin_post_at FROM intents WHERE installation_id='I' AND account_key='A' AND intent_id='intent'",
            [], |row| row.get(0),
        ).unwrap();
        assert_eq!(unsent, None);
        tx.commit().unwrap();
    }
}

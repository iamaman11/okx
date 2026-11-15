//! Typed trading reads on the one installation SQL FIFO; no mutation capability.
use super::UnifiedFinanceHandle;
use crate::{AccountStore, StoreResult, StoredIntent, StoredLot, TradeIntentLookup};
use std::time::Instant;

impl UnifiedFinanceHandle {
    /// Bounded recovery scan on the original account-scoped SQL snapshot.
    pub async fn reconcilable_intents_page(
        &self,
        account: String,
        limit: usize,
        offset: usize,
        deadline: Instant,
    ) -> StoreResult<Vec<StoredIntent>> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let intents = AccountStore::reconcilable_intents_page_sql(
                &tx,
                Some((&installation, &account)),
                limit,
                offset,
                super::opening_ingress::owner_now_ms()?,
            )?;
            tx.commit()?;
            Ok(intents)
        })
        .await
    }

    pub async fn lookup_intent(
        &self,
        account: String,
        lookup: TradeIntentLookup,
        deadline: Instant,
    ) -> StoreResult<Option<StoredIntent>> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let intent = AccountStore::lookup_trade_intent_sql(
                &tx,
                Some((&installation, &account)),
                &lookup,
            )?;
            tx.commit()?;
            Ok(intent)
        })
        .await
    }

    pub async fn active_lots(
        &self,
        account: String,
        instrument: String,
        deadline: Instant,
    ) -> StoreResult<Vec<StoredLot>> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let lots =
                AccountStore::active_lots_sql(&tx, Some((&installation, &account)), &instrument)?;
            tx.commit()?;
            Ok(lots)
        })
        .await
    }
}

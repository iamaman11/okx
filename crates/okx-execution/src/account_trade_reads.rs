//! Typed identity and lot reads, shared by both storage layouts.
use super::*;
#[cfg(any(test, feature = "legacy-account-worker-fixtures"))]
use crate::AccountStoreHandle;
#[cfg(any(test, feature = "legacy-account-worker-fixtures"))]
use std::time::Instant;

#[derive(Debug, Clone)]
pub enum TradeIntentLookup {
    IntentId(String),
    IdempotencyKey(String),
    ClientOrderId(String),
    ExchangeOrderId(String),
    /// Preserve the original Status precedence and eager validation of all keys.
    Reference(String),
}

impl AccountStore {
    pub fn lookup_trade_intent(
        &self,
        lookup: &TradeIntentLookup,
    ) -> StoreResult<Option<StoredIntent>> {
        Self::lookup_trade_intent_sql(&self.connection, None, lookup)
    }

    pub(crate) fn lookup_trade_intent_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        lookup: &TradeIntentLookup,
    ) -> StoreResult<Option<StoredIntent>> {
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
        }
        let find = |column, key: &str| Self::find_intent_sql(db, scope, column, key);
        match lookup {
            TradeIntentLookup::IntentId(id) => find("intent_id", id),
            TradeIntentLookup::IdempotencyKey(key) => find("idempotency_key", key),
            TradeIntentLookup::ClientOrderId(key) => find("client_order_id", key),
            TradeIntentLookup::ExchangeOrderId(key) => find("exchange_order_id", key),
            TradeIntentLookup::Reference(key) => Ok(find("intent_id", key)?
                .or(find("client_order_id", key)?)
                .or(find("exchange_order_id", key)?)
                .or(find("idempotency_key", key)?)),
        }
    }
}

#[cfg(any(test, feature = "legacy-account-worker-fixtures"))]
impl AccountStoreHandle {
    pub async fn lookup_intent(
        &self,
        deadline: Instant,
        lookup: TradeIntentLookup,
    ) -> StoreResult<Option<StoredIntent>> {
        self.call(deadline, move |store| store.lookup_trade_intent(&lookup))
            .await
    }

    pub async fn active_lots(
        &self,
        deadline: Instant,
        instrument: String,
    ) -> StoreResult<Vec<StoredLot>> {
        self.call(deadline, move |store| store.active_lots(&instrument))
            .await
    }
}

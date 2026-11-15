//! Source ownership is a scoped durable identity lookup, never a send grant.
use super::super::*;
impl AccountStore {
    pub fn source_order_owner(
        &self,
        client: &str,
        venue: &str,
    ) -> StoreResult<Option<StoredIntent>> {
        Self::source_order_owner_sql(&self.connection, None, client, venue)
    }
    pub(crate) fn source_order_owner_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        client: &str,
        venue: &str,
    ) -> StoreResult<Option<StoredIntent>> {
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
        }
        let by_client = if client.is_empty() {
            None
        } else {
            Self::find_intent_sql(db, scope, "client_order_id", client)?
        };
        let by_venue = if venue.is_empty() || venue == "0" {
            None
        } else {
            Self::find_intent_sql(db, scope, "exchange_order_id", venue)?
        };
        if let (Some(a), Some(b)) = (&by_client, &by_venue) {
            if a.intent_id != b.intent_id {
                return Err(StoreError::Invalid("SOURCE_ORDER_IDENTITY_CONFLICT"));
            }
        }
        if let Some(known) = &by_client {
            if !venue.is_empty()
                && venue != "0"
                && known
                    .exchange_order_id
                    .as_deref()
                    .is_some_and(|id| id != venue)
            {
                return Err(StoreError::Invalid("SOURCE_ORDER_IDENTITY_CONFLICT"));
            }
        }
        if let Some(known) = &by_venue {
            if !client.is_empty() && known.client_order_id != client {
                return Err(StoreError::Invalid("SOURCE_ORDER_IDENTITY_CONFLICT"));
            }
        }
        Ok(by_client.or(by_venue))
    }
}

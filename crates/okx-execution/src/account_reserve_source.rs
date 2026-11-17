//! Original source snapshot for reserve admission, not a risk or POST grant.
use super::*;
#[derive(Debug, Clone)]
pub struct AccountReserveSource {
    pub intent: StoredIntent,
    pub quote: EntryQuoteEvidence,
    pub admission: EntryAdmissionEvidence,
}
impl AccountStore {
    pub fn reserve_source(&mut self, intent: &str) -> StoreResult<AccountReserveSource> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)?;
        let source = Self::reserve_source_sql(&tx, None, intent)?;
        tx.commit()?;
        Ok(source)
    }
    pub(crate) fn reserve_source_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
    ) -> StoreResult<AccountReserveSource> {
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
        }
        let intent = Self::find_intent_sql(db, scope, "intent_id", id)?
            .ok_or(StoreError::Invalid("RESERVE_SOURCE_INTENT_MISSING"))?;
        let quote = Self::entry_quote_sql(db, scope, id)?
            .ok_or(StoreError::Invalid("ENTRY_ADMISSION_QUOTE_MISSING"))?;
        let admission = Self::entry_admission_sql(db, scope, id)?
            .ok_or(StoreError::Invalid("ENTRY_ALL_IN_COST_UNPROVEN"))?;
        Ok(AccountReserveSource {
            intent,
            quote,
            admission,
        })
    }
}

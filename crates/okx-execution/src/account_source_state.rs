//! One coherent read-only frontier for the source collector; never a grant.
use super::*;
#[derive(Debug, Clone, Default)]
pub struct AccountSourceCursors {
    pub spot: Option<SourceCursor>,
    pub margin: Option<SourceCursor>,
    pub swap: Option<SourceCursor>,
    pub futures: Option<SourceCursor>,
    pub options: Option<SourceCursor>,
    pub bills: Option<SourceCursor>,
}
#[derive(Debug, Clone)]
pub struct AccountSourceState {
    pub account_key: String,
    pub initialized_at: i64,
    pub generation_base: u64,
    pub cursors: AccountSourceCursors,
}
impl AccountStore {
    pub fn source_capture_state(&mut self) -> StoreResult<AccountSourceState> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)?;
        let state = Self::source_capture_state_sql(&tx, None)?;
        tx.commit()?;
        Ok(state)
    }
    pub(crate) fn source_capture_state_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
    ) -> StoreResult<AccountSourceState> {
        let (account_key, initialized_at): (String, i64) = match scope {
            None => db.query_row(
                "SELECT account_key,initialized_at FROM account_meta WHERE singleton=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?,
            Some((installation, account)) => {
                Self::verify_source_account_sql(db, installation, account)?;
                db.query_row("SELECT account_key,initialized_at FROM account_meta WHERE installation_id=?1 AND account_key=?2 AND singleton=1",params![installation,account],|r|Ok((r.get(0)?,r.get(1)?)))?
            }
        };
        let generation_base = Self::source_generation_base_sql(db, scope)?;
        let query = match scope {
            None => {
                "SELECT source,stream_partition,cursor,watermark,generation,coverage_gaps FROM source_cursors WHERE (source='fills' AND stream_partition IN ('SPOT','MARGIN','SWAP','FUTURES','OPTION')) OR (source='bills' AND stream_partition='account')"
            }
            Some(_) => {
                "SELECT source,stream_partition,cursor,watermark,generation,coverage_gaps FROM source_cursors WHERE installation_id=?1 AND account_key=?2 AND ((source='fills' AND stream_partition IN ('SPOT','MARGIN','SWAP','FUTURES','OPTION')) OR (source='bills' AND stream_partition='account'))"
            }
        };
        let mut statement = db.prepare(query)?;
        let mut rows = match scope {
            None => statement.query([])?,
            Some((installation, account)) => statement.query(params![installation, account])?,
        };
        let mut cursors = AccountSourceCursors::default();
        while let Some(row) = rows.next()? {
            let cursor = SourceCursor {
                source: row.get(0)?,
                stream_partition: row.get(1)?,
                cursor: row.get(2)?,
                watermark: row.get(3)?,
                generation: row.get(4)?,
                coverage_gaps: row.get(5)?,
            };
            match (cursor.source.as_str(), cursor.stream_partition.as_str()) {
                ("fills", "SPOT") => cursors.spot = Some(cursor),
                ("fills", "MARGIN") => cursors.margin = Some(cursor),
                ("fills", "SWAP") => cursors.swap = Some(cursor),
                ("fills", "FUTURES") => cursors.futures = Some(cursor),
                ("fills", "OPTION") => cursors.options = Some(cursor),
                ("bills", "account") => cursors.bills = Some(cursor),
                _ => return Err(StoreError::Invalid("SOURCE_CAPTURE_PARTITION_INVALID")),
            }
        }
        Ok(AccountSourceState {
            account_key,
            initialized_at,
            generation_base,
            cursors,
        })
    }
}

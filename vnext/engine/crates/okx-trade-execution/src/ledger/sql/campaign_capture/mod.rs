//! One original campaign-source implementation for legacy and scoped SQLite.
//! The borrow cannot open a writer, COMMIT, grant risk or submit a venue order.
use super::super::*;

// Compile-time SELECT projections preserve original rowids and restrict EVERY
// side of the original financial JOINs, including nested receipt subqueries.
// These are ordinary CTE reads of primary tables, not TEMP views, copies,
// runtime SQL rewriting, or a second financial model. Writes below use
// explicit namespace columns, conflict keys and WHERE predicates instead.
macro_rules! campaign_read {
    ($query:literal) => {
        concat!(
            "WITH
             fills AS (SELECT rowid AS rowid,* FROM main.fills WHERE installation_id=?1 AND account_key=?2),
             bills AS (SELECT rowid AS rowid,* FROM main.bills WHERE installation_id=?1 AND account_key=?2),
             finance_entries AS (SELECT rowid AS rowid,* FROM main.finance_entries WHERE installation_id=?1 AND account_key=?2),
             cash_flows AS (SELECT rowid AS rowid,* FROM main.cash_flows WHERE installation_id=?1 AND account_key=?2),
             intents AS (SELECT rowid AS rowid,* FROM main.intents WHERE installation_id=?1 AND account_key=?2),
             intent_plans AS (SELECT rowid AS rowid,* FROM main.intent_plans WHERE installation_id=?1 AND account_key=?2),
             risk_receipts AS (SELECT rowid AS rowid,* FROM main.risk_receipts WHERE installation_id=?1 AND account_key=?2),
             operation_receipts AS (SELECT rowid AS rowid,* FROM main.operation_receipts WHERE installation_id=?1 AND account_key=?2),
             position_lots AS (SELECT rowid AS rowid,* FROM main.position_lots WHERE installation_id=?1 AND account_key=?2),
             source_cursors AS (SELECT rowid AS rowid,* FROM main.source_cursors WHERE installation_id=?1 AND account_key=?2),
             finance_source_links AS (SELECT rowid AS rowid,* FROM main.finance_source_links WHERE installation_id=?1 AND account_key=?2),
             campaign_entry_readiness AS (SELECT rowid AS rowid,* FROM main.campaign_entry_readiness WHERE installation_id=?1 AND account_key=?2),
             campaign_source_epoch AS (SELECT rowid AS rowid,* FROM main.campaign_source_epoch WHERE installation_id=?1 AND account_key=?2),
             campaign_late_head_epoch AS (SELECT rowid AS rowid,* FROM main.campaign_late_head_epoch WHERE installation_id=?1 AND account_key=?2),
             campaign_entry_applied AS (SELECT rowid AS rowid,* FROM main.campaign_entry_applied WHERE installation_id=?1 AND account_key=?2),
             campaign_entry_totals AS (SELECT rowid AS rowid,* FROM main.campaign_entry_totals WHERE installation_id=?1 AND account_key=?2),
             campaign_entry_window_proofs AS (SELECT rowid AS rowid,* FROM main.campaign_entry_window_proofs WHERE installation_id=?1 AND account_key=?2),
             campaign_receipt_events AS (SELECT rowid AS rowid,* FROM main.campaign_receipt_events WHERE installation_id=?1 AND account_key=?2),
             campaign_receipt_cursors AS (SELECT rowid AS rowid,* FROM main.campaign_receipt_cursors WHERE installation_id=?1 AND account_key=?2)
             ",
            $query
        )
    };
}

mod baseline;
mod continuity;
mod deltas;
mod readiness;
mod sources;
#[cfg(test)]
mod tests;

struct CampaignIdentity {
    account_key: String,
    installation_id: String,
    initialized_at: i64,
}

pub(super) struct CampaignCapture<'db> {
    connection: &'db Connection,
    scope: Option<(&'db str, &'db str)>,
    meta: CampaignIdentity,
}

impl<'db> CampaignCapture<'db> {
    pub(super) fn legacy(connection: &'db Connection, meta: &AccountMeta) -> Self {
        Self {
            connection,
            scope: None,
            meta: CampaignIdentity {
                account_key: meta.account_key.clone(),
                installation_id: meta.installation_id.clone(),
                initialized_at: meta.initialized_at,
            },
        }
    }

    fn from_scope(
        connection: &'db Connection,
        scope: Option<(&'db str, &'db str)>,
    ) -> StoreResult<Self> {
        let meta = match scope {
            Some((installation, account)) => {
                AccountStore::verify_source_account_sql(connection, installation, account)?;
                connection.query_row(
                    "SELECT account_key,installation_id,initialized_at FROM account_meta
                     WHERE installation_id=?1 AND account_key=?2 AND singleton=1",
                    params![installation, account],
                    |r| Ok(CampaignIdentity { account_key: r.get(0)?, installation_id: r.get(1)?, initialized_at: r.get(2)? }),
                )?
            }
            None => connection.query_row(
                "SELECT account_key,installation_id,initialized_at FROM account_meta WHERE singleton=1",
                [],
                |r| Ok(CampaignIdentity { account_key: r.get(0)?, installation_id: r.get(1)?, initialized_at: r.get(2)? }),
            )?,
        };
        Ok(Self {
            connection,
            scope,
            meta,
        })
    }

    fn sql(&self, legacy: &'static str, scoped: &'static str) -> &'static str {
        if self.scope.is_some() { scoped } else { legacy }
    }

    fn params<'a>(
        &'a self,
        original: &[&'a dyn rusqlite::ToSql],
    ) -> rusqlite::ParamsFromIter<Vec<&'a dyn rusqlite::ToSql>> {
        let mut values: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(original.len() + 2);
        if let Some((installation, account)) = &self.scope {
            values.push(installation);
            values.push(account);
        }
        values.extend_from_slice(original);
        rusqlite::params_from_iter(values)
    }

    fn cursor(&self, source: &str, partition: &str) -> StoreResult<Option<SourceCursor>> {
        if let Some((installation, account)) = self.scope {
            return AccountStore::scoped_source_cursor_sql(
                self.connection,
                installation,
                account,
                source,
                partition,
            );
        }
        Ok(self
            .connection
            .query_row(
                "SELECT cursor,watermark,generation,coverage_gaps FROM source_cursors
             WHERE source=?1 AND stream_partition=?2",
                params![source, partition],
                |r| {
                    Ok(SourceCursor {
                        source: source.into(),
                        stream_partition: partition.into(),
                        cursor: r.get(0)?,
                        watermark: r.get(1)?,
                        generation: r.get(2)?,
                        coverage_gaps: r.get(3)?,
                    })
                },
            )
            .optional()?)
    }

    fn operation(&self, id: &str) -> StoreResult<Option<OperationReceipt>> {
        AccountStore::operation_sql(self.connection, self.scope, id)
    }
}

impl AccountStore {
    pub(crate) fn verify_campaign_entry_readiness_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        campaign: &str,
        from: i64,
        through: i64,
    ) -> StoreResult<()> {
        CampaignCapture::from_scope(db, scope)?
            .verify_campaign_entry_readiness(campaign, from, through)
    }
    pub(crate) fn refresh_campaign_entry_readiness_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        campaign: &str,
        from: i64,
        through: i64,
    ) -> StoreResult<()> {
        CampaignCapture::from_scope(db, scope)?
            .refresh_campaign_entry_readiness(campaign, from, through)
    }
    pub(crate) fn seal_captured_campaign_baseline_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        campaign: &str,
        from: i64,
        through: i64,
    ) -> StoreResult<(Decimal, String)> {
        CampaignCapture::from_scope(db, scope)?
            .seal_captured_campaign_baseline(campaign, from, through)
    }
}

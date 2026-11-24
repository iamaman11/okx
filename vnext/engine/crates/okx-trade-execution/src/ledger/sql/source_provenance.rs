//! Finance verification watermark. Never seals unverified pre-existing history.
use super::super::*;
impl AccountStore {
    pub(crate) fn finance_rowid_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
    ) -> StoreResult<i64> {
        Ok(match scope {
            None => db.query_row(
                "SELECT COALESCE(MAX(rowid),0) FROM finance_entries",
                [],
                |r| r.get(0),
            )?,
            Some((installation, account)) => {
                Self::verify_source_account_sql(db, installation, account)?;
                db.query_row("SELECT COALESCE(MAX(rowid),0) FROM finance_entries WHERE installation_id=?1 AND account_key=?2",params![installation,account],|r|r.get(0))?
            }
        })
    }
    pub(crate) fn provenance_watermark_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
    ) -> StoreResult<Option<i64>> {
        Ok(match scope {
            None=>db.query_row("SELECT watermark FROM source_cursors WHERE source='finance_provenance' AND stream_partition='account' AND cursor='' AND coverage_gaps='' AND generation=0",[],|r|r.get(0)).optional()?,
            Some((installation,account))=>{
                Self::verify_source_account_sql(db,installation,account)?;
                db.query_row("SELECT watermark FROM source_cursors WHERE installation_id=?1 AND account_key=?2 AND source='finance_provenance' AND stream_partition='account' AND cursor='' AND coverage_gaps='' AND generation=0",params![installation,account],|r|r.get(0)).optional()?
            }
        })
    }
    pub(crate) fn finance_proof_sealed_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
    ) -> StoreResult<bool> {
        let latest = Self::finance_rowid_sql(db, scope)?;
        let prior = Self::provenance_watermark_sql(db, scope)?;
        Ok(prior == Some(latest) || (latest == 0 && prior.is_none()))
    }
    pub(crate) fn advance_finance_provenance_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        previous: i64,
    ) -> StoreResult<()> {
        let prior = Self::provenance_watermark_sql(tx, scope)?;
        if prior != Some(previous) && !(previous == 0 && prior.is_none()) {
            return Ok(());
        }
        let latest = Self::finance_rowid_sql(tx, scope)?;
        if latest != previous {
            match scope {
                None => tx.execute("INSERT INTO source_cursors(source,stream_partition,cursor,watermark,generation,coverage_gaps) VALUES('finance_provenance','account','',?1,0,'') ON CONFLICT(source,stream_partition) DO UPDATE SET cursor='',watermark=excluded.watermark,generation=0,coverage_gaps=''", [latest])?,
                Some((installation,account)) => tx.execute("INSERT INTO source_cursors(installation_id,account_key,source,stream_partition,cursor,watermark,generation,coverage_gaps) VALUES(?1,?2,'finance_provenance','account','',?3,0,'') ON CONFLICT(installation_id,account_key,source,stream_partition) DO UPDATE SET cursor='',watermark=excluded.watermark,generation=0,coverage_gaps=''",params![installation,account,latest])?,
            };
        }
        Ok(())
    }
}

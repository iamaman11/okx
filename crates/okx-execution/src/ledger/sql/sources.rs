use super::super::*;
impl AccountStore {
    pub(crate) fn upsert_source_cursor_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        c: &SourceCursor,
    ) -> StoreResult<()> {
        let prior: Option<(i64,i64)> = match scope {
            None => tx.query_row("SELECT generation,watermark FROM source_cursors WHERE source=?1 AND stream_partition=?2",
                params![c.source,c.stream_partition],|r|Ok((r.get(0)?,r.get(1)?))).optional()?,
            Some((installation,account)) => {
                Self::verify_source_account_sql(tx,installation,account)?;
                tx.query_row("SELECT generation,watermark FROM source_cursors WHERE installation_id=?1 AND account_key=?2 AND source=?3 AND stream_partition=?4",
                    params![installation,account,c.source,c.stream_partition],|r|Ok((r.get(0)?,r.get(1)?))).optional()?
            }
        };
        if prior.is_some_and(|(g, w)| c.generation < g || (c.generation == g && c.watermark < w)) {
            return Err(StoreError::Invalid("SOURCE_CURSOR_REGRESSION"));
        }
        match scope {
            None => tx.execute("INSERT INTO source_cursors VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(source,stream_partition) DO UPDATE SET cursor=excluded.cursor,watermark=excluded.watermark,generation=excluded.generation,coverage_gaps=excluded.coverage_gaps",
                params![c.source,c.stream_partition,c.cursor,c.watermark,c.generation,c.coverage_gaps])?,
            Some((installation,account)) => tx.execute("INSERT INTO source_cursors VALUES(?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(installation_id,account_key,source,stream_partition) DO UPDATE SET cursor=excluded.cursor,watermark=excluded.watermark,generation=excluded.generation,coverage_gaps=excluded.coverage_gaps",
                params![installation,account,c.source,c.stream_partition,c.cursor,c.watermark,c.generation,c.coverage_gaps])?,
        };
        Ok(())
    }

    pub fn cursor(&self, source: &str, partition: &str) -> StoreResult<Option<SourceCursor>> {
        Ok(self.connection.query_row("SELECT cursor,watermark,generation,coverage_gaps FROM source_cursors WHERE source=?1 AND stream_partition=?2",params![source,partition],|r|Ok(SourceCursor{source:source.into(),stream_partition:partition.into(),cursor:r.get(0)?,watermark:r.get(1)?,generation:r.get(2)?,coverage_gaps:r.get(3)?})).optional()?)
    }

    /// A WebSocket connection starts at generation zero after each process restart.
    /// Its persistent source generation must resume AFTER all durable SQL cursors,
    /// without resetting any watermark or weakening cursor-regression enforcement.
    pub fn source_generation_base(&self) -> StoreResult<u64> {
        Self::source_generation_base_sql(&self.connection, None)
    }

    pub(crate) fn source_generation_base_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
    ) -> StoreResult<u64> {
        let generation: i64 = match scope {
            Some((installation, account)) => {
                Self::verify_source_account_sql(db, installation, account)?;
                db.query_row(
                    "SELECT COALESCE(MAX(generation),0) FROM source_cursors
                     WHERE installation_id=?1 AND account_key=?2",
                    params![installation, account],
                    |r| r.get(0),
                )?
            }
            None => db.query_row(
                "SELECT COALESCE(MAX(generation),0) FROM source_cursors",
                [],
                |r| r.get(0),
            )?,
        };
        if generation < 0 || generation == i64::MAX {
            return Err(StoreError::Invalid("SOURCE_GENERATION_INVALID"));
        }
        Ok(generation as u64)
    }

    pub(crate) fn verify_source_account_sql(
        db: &Connection,
        installation: &str,
        account: &str,
    ) -> StoreResult<()> {
        let found: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM account_meta
             WHERE installation_id=?1 AND account_key=?2)",
            params![installation, account],
            |r| r.get(0),
        )?;
        if !found {
            return Err(StoreError::Invalid("UNIFIED_SOURCE_ACCOUNT_NOT_FOUND"));
        }
        Ok(())
    }

    pub(crate) fn scoped_source_cursor_sql(
        db: &Connection,
        installation: &str,
        account: &str,
        source: &str,
        partition: &str,
    ) -> StoreResult<Option<SourceCursor>> {
        Self::verify_source_account_sql(db, installation, account)?;
        Ok(db
            .query_row(
                "SELECT cursor,watermark,generation,coverage_gaps FROM source_cursors
             WHERE installation_id=?1 AND account_key=?2 AND source=?3 AND stream_partition=?4",
                params![installation, account, source, partition],
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
}

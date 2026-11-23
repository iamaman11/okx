//! Original campaign sources rules on a scoped, caller-owned SQL snapshot.
use super::*;
impl CampaignCapture<'_> {
    pub(in crate::account_store::sql) fn captured_complete_frontiers(
        &self,
        from_ms: i64,
        through_ms: i64,
    ) -> StoreResult<Vec<(&'static str, &'static str, i64, i64)>> {
        if from_ms <= 0 || through_ms <= from_ms {
            return Err(StoreError::Invalid("ENTRY_OPENING_WINDOW_INVALID"));
        }
        if from_ms < self.meta.initialized_at {
            return Err(StoreError::Invalid("BASELINE_RECONCILIATION_REQUIRED"));
        }
        let mut frontiers = Vec::new();
        for (source, partition) in [
            ("fills", "SPOT"),
            ("fills", "MARGIN"),
            ("fills", "SWAP"),
            ("fills", "FUTURES"),
            ("fills", "OPTION"),
            ("bills", "account"),
        ] {
            let cursor = self
                .cursor(source, partition)?
                .ok_or(StoreError::Invalid("ENTRY_BASELINE_CAPTURE_PENDING"))?;
            if cursor.generation <= 0
                || !cursor.cursor.is_empty()
                || !cursor.coverage_gaps.is_empty()
                || cursor.watermark < through_ms
            {
                return Err(StoreError::Invalid("ENTRY_BASELINE_CAPTURE_PENDING"));
            }
            frontiers.push((source, partition, cursor.generation, cursor.watermark));
        }
        if !AccountStore::finance_proof_sealed_sql(self.connection, self.scope)? {
            return Err(StoreError::Invalid(
                "ENTRY_OPENING_FINANCE_PROVENANCE_UNSEALED",
            ));
        }
        Ok(frontiers)
    }

    pub(in crate::account_store::sql) fn captured_empty_frontiers(
        &self,
        from_ms: i64,
        through_ms: i64,
    ) -> StoreResult<Vec<(&'static str, &'static str, i64, i64)>> {
        let frontiers = self.captured_complete_frontiers(from_ms, through_ms)?;
        let (fills, bills, unresolved, lots): (i64, i64, i64, i64) = self.connection.query_row(
            self.sql(
                "SELECT (SELECT COUNT(*) FROM fills WHERE time>?1 AND time<=?2),
                        (SELECT COUNT(*) FROM bills
                         WHERE time>?1 AND time<=?2
                           AND kind NOT IN ('1:11','1:12','8:173','8:174')
                           AND kind NOT LIKE '7:%'),
                        (SELECT COUNT(*) FROM intents
                         WHERE state NOT IN ('REJECTED','CANCELLED','FILLED')),
                        (SELECT COUNT(*) FROM position_lots WHERE remaining_qty!='0')",
                campaign_read!(
                    "SELECT (SELECT COUNT(*) FROM fills WHERE time>?3 AND time<=?4),
                        (SELECT COUNT(*) FROM bills
                         WHERE time>?3 AND time<=?4
                           AND kind NOT IN ('1:11','1:12','8:173','8:174')
                           AND kind NOT LIKE '7:%'),
                        (SELECT COUNT(*) FROM intents
                         WHERE state NOT IN ('REJECTED','CANCELLED','FILLED')),
                        (SELECT COUNT(*) FROM position_lots WHERE remaining_qty!='0')"
                ),
            ),
            self.params(params![from_ms, through_ms]),
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        if fills != 0 || bills != 0 {
            // Recognized transfer/funding/interest bills were already
            // classified and ingested by the existing finance owner. They
            // do not constitute opening turnover. Any other bill, including
            // an unknown category, remains budget-relevant until classified.
            // Nonempty sources require the historical linear-contract,
            // opening/closing classification and event-time FX policy.
            return Err(StoreError::Invalid("BASELINE_RECONCILIATION_REQUIRED"));
        }
        if unresolved != 0 || lots != 0 {
            return Err(StoreError::Invalid(
                "ENTRY_OPENING_UNRESOLVED_NATIVE_EXPOSURE",
            ));
        }
        Ok(frontiers)
    }

    pub(in crate::account_store::sql) fn campaign_source_epoch(&self) -> StoreResult<i64> {
        self.connection
            .query_row(
                self.sql(
                    "SELECT revision FROM campaign_source_epoch WHERE id=1",
                    campaign_read!("SELECT revision FROM campaign_source_epoch WHERE id=1"),
                ),
                self.params(&[]),
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub(in crate::account_store::sql) fn campaign_source_generations(
        frontiers: &[(&str, &str, i64, i64)],
    ) -> String {
        frontiers
            .iter()
            .map(|(source, partition, generation, _)| format!("{source}/{partition}:{generation}"))
            .collect::<Vec<_>>()
            .join("|")
    }

    pub(in crate::account_store::sql) fn campaign_source_rowids(
        &self,
    ) -> StoreResult<(i64, i64, i64, i64, i64, i64)> {
        self.connection
            .query_row(
                self.sql(
                    "SELECT
               (SELECT COALESCE(MAX(rowid),0) FROM fills),
               (SELECT COALESCE(MAX(rowid),0) FROM bills),
               (SELECT COALESCE(MAX(rowid),0) FROM operation_receipts),
               (SELECT COALESCE(MAX(rowid),0) FROM finance_entries),
               (SELECT COALESCE(MAX(rowid),0) FROM cash_flows),
               (SELECT revision FROM campaign_late_head_epoch WHERE id=1)",
                    campaign_read!(
                        "SELECT
               (SELECT COALESCE(MAX(rowid),0) FROM fills),
               (SELECT COALESCE(MAX(rowid),0) FROM bills),
               (SELECT COALESCE(MAX(rowid),0) FROM operation_receipts),
               (SELECT COALESCE(MAX(rowid),0) FROM finance_entries),
               (SELECT COALESCE(MAX(rowid),0) FROM cash_flows),
               (SELECT revision FROM campaign_late_head_epoch WHERE id=1)"
                    ),
                ),
                self.params(&[]),
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                    ))
                },
            )
            .map_err(Into::into)
    }

    pub(in crate::account_store::sql) fn campaign_bounded_next_rowid(
        &self,
        table: &str,
        floor: i64,
        end: i64,
    ) -> StoreResult<i64> {
        // Table identifiers are compile-time constants, never caller input.
        let sql = match table {
            "fills" => self.sql("SELECT rowid FROM fills WHERE rowid>?1 ORDER BY rowid LIMIT 1 OFFSET 4095", campaign_read!("SELECT rowid FROM fills WHERE rowid>?3 ORDER BY rowid LIMIT 1 OFFSET 4095")),
            "bills" => self.sql("SELECT rowid FROM bills WHERE rowid>?1 ORDER BY rowid LIMIT 1 OFFSET 4095", campaign_read!("SELECT rowid FROM bills WHERE rowid>?3 ORDER BY rowid LIMIT 1 OFFSET 4095")),
            "operation_receipts" => {
                self.sql("SELECT rowid FROM operation_receipts WHERE rowid>?1 ORDER BY rowid LIMIT 1 OFFSET 4095", campaign_read!("SELECT rowid FROM operation_receipts WHERE rowid>?3 ORDER BY rowid LIMIT 1 OFFSET 4095"))
            }
            _ => return Err(StoreError::Invalid("CAMPAIGN_DELTA_SOURCE_INVALID")),
        };
        let page_end: Option<i64> = self
            .connection
            .query_row(sql, self.params(params![floor]), |r| r.get(0))
            .optional()?;
        Ok(page_end.unwrap_or(end))
    }

    pub(in crate::account_store::sql) fn campaign_receipt_cursor(
        &self,
        campaign_id: &str,
    ) -> StoreResult<Option<i64>> {
        self.connection
            .query_row(
                self.sql(
                    "SELECT reviewed_event_seq FROM campaign_receipt_cursors
             WHERE campaign_id=?1",
                    campaign_read!(
                        "SELECT reviewed_event_seq FROM campaign_receipt_cursors
             WHERE campaign_id=?3"
                    ),
                ),
                self.params(params![campaign_id]),
                |r| r.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub(in crate::account_store::sql) fn save_campaign_receipt_cursor(
        &self,
        campaign_id: &str,
        event_seq: i64,
    ) -> StoreResult<()> {
        self.connection.execute(
            self.sql(
                "INSERT INTO campaign_receipt_cursors VALUES(?1,?2)
             ON CONFLICT(campaign_id) DO UPDATE SET
                reviewed_event_seq=MAX(reviewed_event_seq,excluded.reviewed_event_seq)",
                "INSERT INTO campaign_receipt_cursors VALUES(?1,?2,?3,?4)
             ON CONFLICT(installation_id,account_key,campaign_id) DO UPDATE SET
                reviewed_event_seq=MAX(reviewed_event_seq,excluded.reviewed_event_seq)",
            ),
            self.params(params![campaign_id, event_seq]),
        )?;
        Ok(())
    }

    pub(in crate::account_store::sql) fn latest_campaign_receipt_event(&self) -> StoreResult<i64> {
        self.connection
            .query_row(
                self.sql(
                    "SELECT COALESCE(MAX(event_seq),0) FROM campaign_receipt_events",
                    campaign_read!(
                        "SELECT COALESCE(MAX(event_seq),0) FROM campaign_receipt_events"
                    ),
                ),
                self.params(&[]),
                |r| r.get(0),
            )
            .map_err(Into::into)
    }

    pub(in crate::account_store::sql) fn new_campaign_receipt_events(
        &self,
        after_seq: i64,
    ) -> StoreResult<(Vec<String>, i64, bool)> {
        let mut statement = self.connection.prepare(self.sql(
            "SELECT event_seq,operation_id,kind
             FROM campaign_receipt_events WHERE event_seq>?1
             ORDER BY event_seq LIMIT 4097",
            campaign_read!(
                "SELECT event_seq,operation_id,kind
             FROM campaign_receipt_events WHERE event_seq>?3
             ORDER BY event_seq LIMIT 4097"
            ),
        ))?;
        let rows = statement
            .query_map(self.params(params![after_seq]), |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let pending = rows.len() > 4096;
        let page = &rows[..rows.len().min(4096)];
        let new_seq = page.last().map(|r| r.0).unwrap_or(after_seq);
        let mut terminal = std::collections::BTreeSet::new();
        for (_, key, kind) in page {
            let id = match kind.as_str() {
                "ENTRY_SETTLEMENT_PROVEN" => key.strip_prefix("entry-settlement:"),
                "ENTRY_PARTIAL_SETTLED" => {
                    // Unlike a mutable HEAD, the Worker accepted one exact
                    // immutable revision and this event is append-only.
                    let raw = key.strip_prefix("entry-partial-settled:");
                    let id = raw.and_then(|s| s.rsplit_once(":r").map(|(i, _)| i));
                    if id.is_none() {
                        return Err(StoreError::Invalid("CAMPAIGN_PARTIAL_EVENT_INVALID"));
                    }
                    terminal.insert(key.clone());
                    continue;
                }
                "ENTRY_LATE_HEAD" => key.strip_prefix("entry-late-head:"),
                "ENTRY_LATE_SETTLEMENT_PROVEN" => key
                    .strip_prefix("entry-late:")
                    .and_then(|v| v.rsplit_once(":r").map(|(id, _)| id)),
                "ENTRY_LATE_SETTLED" => key
                    .strip_prefix("entry-late-settled:")
                    .and_then(|v| v.rsplit_once(":r").map(|(id, _)| id)),
                _ => None,
            }
            .filter(|v| !v.is_empty())
            .ok_or(StoreError::Invalid("CAMPAIGN_RECEIPT_EVENT_CORRUPT"))?;
            terminal.insert(format!("entry-settlement:{id}"));
        }
        Ok((terminal.into_iter().collect(), new_seq, pending))
    }
}

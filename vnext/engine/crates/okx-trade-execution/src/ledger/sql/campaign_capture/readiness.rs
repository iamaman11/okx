//! Original campaign readiness rules on a scoped, caller-owned SQL snapshot.
use super::*;
impl CampaignCapture<'_> {
    pub(in crate::account_store::sql) fn mark_campaign_readiness(
        &self,
        campaign_id: &str,
        from_ms: i64,
        through_ms: i64,
        frontiers: &[(&str, &str, i64, i64)],
    ) -> StoreResult<()> {
        // A watermark only proves coverage through a cutoff. Never mark
        // future event-time money as reviewed and then silently skip it
        // when the wall clock catches up without another INSERT.
        let future: bool = self.connection.query_row(
            self.sql(
                "SELECT EXISTS(SELECT 1 FROM fills WHERE time>?1)
              OR EXISTS(SELECT 1 FROM bills WHERE time>?1)
              OR EXISTS(SELECT 1 FROM operation_receipts
                  WHERE kind IN (
                    'ENTRY_SETTLEMENT_PROVEN','ENTRY_LATE_HEAD',
                    'ENTRY_LATE_SETTLEMENT_PROVEN','ENTRY_PARTIAL_SETTLED')
                    AND committed_at>?1)",
                campaign_read!(
                    "SELECT EXISTS(SELECT 1 FROM fills WHERE time>?3)
              OR EXISTS(SELECT 1 FROM bills WHERE time>?3)
              OR EXISTS(SELECT 1 FROM operation_receipts
                  WHERE kind IN (
                    'ENTRY_SETTLEMENT_PROVEN','ENTRY_LATE_HEAD',
                    'ENTRY_LATE_SETTLEMENT_PROVEN','ENTRY_PARTIAL_SETTLED')
                    AND committed_at>?3)"
                ),
            ),
            self.params(params![through_ms]),
            |r| r.get(0),
        )?;
        if future {
            return Err(StoreError::Invalid("CAMPAIGN_DELTA_EVENT_NOT_COVERED"));
        }
        let epoch = self.campaign_source_epoch()?;
        let generations = Self::campaign_source_generations(frontiers);
        let rows = self.campaign_source_rowids()?;
        // The economic event cursor advances only after independently
        // verified money was applied. Both writes use the sole SQL owner.
        self.save_campaign_receipt_cursor(campaign_id, self.latest_campaign_receipt_event()?)?;
        self.connection.execute(
            self.sql(
                "INSERT INTO campaign_entry_readiness VALUES(
                ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
             ON CONFLICT(campaign_id) DO UPDATE SET
                 from_ms=excluded.from_ms,
                 covered_through_ms=excluded.covered_through_ms,
                 source_revision=excluded.source_revision,
                 source_generations=excluded.source_generations,
                 reviewed_fill_rowid=excluded.reviewed_fill_rowid,
                 reviewed_bill_rowid=excluded.reviewed_bill_rowid,
                 reviewed_receipt_rowid=excluded.reviewed_receipt_rowid,
                 reviewed_finance_rowid=excluded.reviewed_finance_rowid,
                 reviewed_flow_rowid=excluded.reviewed_flow_rowid,
                 reviewed_late_epoch=excluded.reviewed_late_epoch",
                "INSERT INTO campaign_entry_readiness VALUES(?1,?2,
                ?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)
             ON CONFLICT(installation_id,account_key,campaign_id) DO UPDATE SET
                 from_ms=excluded.from_ms,
                 covered_through_ms=excluded.covered_through_ms,
                 source_revision=excluded.source_revision,
                 source_generations=excluded.source_generations,
                 reviewed_fill_rowid=excluded.reviewed_fill_rowid,
                 reviewed_bill_rowid=excluded.reviewed_bill_rowid,
                 reviewed_receipt_rowid=excluded.reviewed_receipt_rowid,
                 reviewed_finance_rowid=excluded.reviewed_finance_rowid,
                 reviewed_flow_rowid=excluded.reviewed_flow_rowid,
                 reviewed_late_epoch=excluded.reviewed_late_epoch",
            ),
            self.params(params![
                campaign_id,
                from_ms,
                through_ms,
                epoch,
                generations,
                rows.0,
                rows.1,
                rows.2,
                rows.3,
                rows.4,
                rows.5
            ]),
        )?;
        Ok(())
    }

    pub(in crate::account_store::sql) fn verify_campaign_entry_readiness(
        &self,
        campaign_id: &str,
        from_ms: i64,
        through_ms: i64,
    ) -> StoreResult<()> {
        let ready: Option<(i64, i64, i64, String)> = self
            .connection
            .query_row(
                self.sql(
                    "SELECT from_ms,covered_through_ms,source_revision,source_generations
                 FROM campaign_entry_readiness WHERE campaign_id=?1",
                    campaign_read!(
                        "SELECT from_ms,covered_through_ms,source_revision,source_generations
                 FROM campaign_entry_readiness WHERE campaign_id=?3"
                    ),
                ),
                self.params(params![campaign_id]),
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let Some((origin, covered, revision, generations)) = ready else {
            return Err(StoreError::Invalid("CAMPAIGN_READINESS_NOT_RECONCILED"));
        };
        // Execute's target is now-2000ms; an independently scheduled
        // maintenance cycle cannot attest a future cutoff. Accept at most
        // another 3000ms delay (total age <= ACCOUNT_MAX_AGE_MS=5000).
        // A changed economic epoch or capture generation is still an
        // unconditional refusal even within this freshness allowance.
        let frontiers = self.captured_complete_frontiers(from_ms, covered)?;
        if origin != from_ms
            || covered > through_ms
            || through_ms.saturating_sub(covered) > 3_000
            || revision != self.campaign_source_epoch()?
            || generations != Self::campaign_source_generations(&frontiers)
        {
            return Err(StoreError::Invalid("CAMPAIGN_READINESS_STALE"));
        }
        // An attested capture with no durable economic total cannot
        // mint a fresh RiskBook opening or authorize another admission.
        let exists: bool = self.connection.query_row(
            self.sql(
                "SELECT EXISTS(
                SELECT 1 FROM campaign_entry_totals
                WHERE campaign_id=?1 AND from_ms=?2
                  AND covered_through_ms<=?3)",
                campaign_read!(
                    "SELECT EXISTS(
                SELECT 1 FROM campaign_entry_totals
                WHERE campaign_id=?3 AND from_ms=?4
                  AND covered_through_ms<=?5)"
                ),
            ),
            self.params(params![campaign_id, from_ms, covered]),
            |row| row.get(0),
        )?;
        if !exists {
            return Err(StoreError::Invalid("CAMPAIGN_APPLIED_FRONTIER_MISSING"));
        }
        Ok(())
    }

    pub(in crate::account_store::sql) fn refresh_campaign_entry_readiness(
        &self,
        campaign_id: &str,
        from_ms: i64,
        through_ms: i64,
    ) -> StoreResult<()> {
        let frontiers = self.captured_complete_frontiers(from_ms, through_ms)?;
        let epoch = self.campaign_source_epoch()?;
        let generations = Self::campaign_source_generations(&frontiers);
        // A named checkpoint keeps one owner for the ten source facts
        // without embedding an opaque, error-prone ten-element tuple.
        struct Ready {
            origin: i64,
            covered: i64,
            revision: i64,
            prior_generations: String,
            fill_floor: i64,
            bill_floor: i64,
            receipt_floor: i64,
            finance_floor: i64,
            flow_floor: i64,
            late_floor: i64,
        }
        let previous: Option<Ready> = self
            .connection
            .query_row(
                self.sql(
                    "SELECT from_ms,covered_through_ms,source_revision,source_generations,
                    reviewed_fill_rowid,reviewed_bill_rowid,reviewed_receipt_rowid,
                    reviewed_finance_rowid,reviewed_flow_rowid,reviewed_late_epoch
             FROM campaign_entry_readiness WHERE campaign_id=?1",
                    campaign_read!(
                        "SELECT from_ms,covered_through_ms,source_revision,source_generations,
                    reviewed_fill_rowid,reviewed_bill_rowid,reviewed_receipt_rowid,
                    reviewed_finance_rowid,reviewed_flow_rowid,reviewed_late_epoch
             FROM campaign_entry_readiness WHERE campaign_id=?3"
                    ),
                ),
                self.params(params![campaign_id]),
                |r| {
                    Ok(Ready {
                        origin: r.get(0)?,
                        covered: r.get(1)?,
                        revision: r.get(2)?,
                        prior_generations: r.get(3)?,
                        fill_floor: r.get(4)?,
                        bill_floor: r.get(5)?,
                        receipt_floor: r.get(6)?,
                        finance_floor: r.get(7)?,
                        flow_floor: r.get(8)?,
                        late_floor: r.get(9)?,
                    })
                },
            )
            .optional()?;
        if let Some(Ready {
            origin,
            covered,
            revision,
            prior_generations,
            fill_floor,
            bill_floor,
            receipt_floor,
            finance_floor: _finance_floor,
            flow_floor: _flow_floor,
            late_floor,
        }) = previous
            && origin == from_ms
            && covered <= through_ms
            && prior_generations == generations
        {
            if revision == epoch {
                // Cursor-only advance: the immutable economic input is unchanged.
                self.connection.execute(
                    self.sql("UPDATE campaign_entry_readiness SET covered_through_ms=?2
                     WHERE campaign_id=?1 AND source_revision=?3", "UPDATE campaign_entry_readiness SET covered_through_ms=?4
                     WHERE campaign_id=?3 AND source_revision=?5 AND installation_id=?1 AND account_key=?2"),
                    self.params(params![campaign_id, through_ms, epoch]),
                )?;
                return Ok(());
            }
            let rows = self.campaign_source_rowids()?;
            let (fill_max, bill_max, receipt_max, finance_max, flow_max, late_epoch) = rows;
            if late_epoch == late_floor {
                let fill_end = self.campaign_bounded_next_rowid("fills", fill_floor, fill_max)?;
                let bill_end = self.campaign_bounded_next_rowid("bills", bill_floor, bill_max)?;
                let receipt_end = self.campaign_bounded_next_rowid(
                    "operation_receipts",
                    receipt_floor,
                    receipt_max,
                )?;
                // A future event cannot be marked reviewed before a complete
                // capture covers its exchange event-time.
                let future: bool = self.connection.query_row(
                    self.sql(
                        "SELECT EXISTS(SELECT 1 FROM fills
                       WHERE rowid>?1 AND rowid<=?2 AND time>?5)
                     OR EXISTS(SELECT 1 FROM bills
                       WHERE rowid>?3 AND rowid<=?4 AND time>?5)",
                        campaign_read!(
                            "SELECT EXISTS(SELECT 1 FROM fills
                       WHERE rowid>?3 AND rowid<=?4 AND time>?7)
                     OR EXISTS(SELECT 1 FROM bills
                       WHERE rowid>?5 AND rowid<=?6 AND time>?7)"
                        ),
                    ),
                    self.params(params![
                        fill_floor, fill_end, bill_floor, bill_end, through_ms
                    ]),
                    |r| r.get(0),
                )?;
                if future {
                    return Err(StoreError::Invalid("CAMPAIGN_DELTA_EVENT_NOT_COVERED"));
                }
                // Native terminal and late receipts change consumed USD; they
                // must pass the existing full amount/source/FX verifier.
                // Do not shortcut this with a cursor or a money guess.
                let monetary_receipt: bool = self.connection.query_row(
                    self.sql("SELECT EXISTS(SELECT 1 FROM operation_receipts
                     WHERE rowid>?1 AND rowid<=?2
                       AND kind IN (
                         'ENTRY_SETTLEMENT_PROVEN','ENTRY_LATE_HEAD',
                         'ENTRY_LATE_SETTLEMENT_PROVEN','ENTRY_LATE_SETTLED')
                     OR (kind='ENTRY_PARTIAL_SETTLED' AND EXISTS (
                       SELECT 1 FROM operation_receipts partial
                       JOIN intents i ON i.intent_id=json_extract(partial.result_ref,'$.intent_id')
                       JOIN risk_receipts r ON r.intent_id=i.intent_id
                       WHERE partial.operation_id=replace(operation_receipts.operation_id,
                                 'entry-partial-settled:','entry-partial:')
                         AND partial.kind='ENTRY_PARTIAL_SETTLEMENT_PROVEN'
                         AND partial.request_hash=operation_receipts.request_hash
                         AND json_extract(partial.result_ref,'$.source_digest')=operation_receipts.result_ref
                         AND json_extract(partial.result_ref,'$.plan_hash')=i.plan_hash
                         AND json_extract(partial.result_ref,'$.reservation_key')=r.reservation_key
                         AND json_extract(partial.result_ref,'$.venue_order_id')=i.exchange_order_id
                         AND r.plan_hash=i.plan_hash AND r.state IN ('OPEN','RELEASED')
                         AND operation_receipts.committed_at<=?3
                     ))
                    )", campaign_read!("SELECT EXISTS(SELECT 1 FROM operation_receipts
                     WHERE rowid>?3 AND rowid<=?4
                       AND kind IN (
                         'ENTRY_SETTLEMENT_PROVEN','ENTRY_LATE_HEAD',
                         'ENTRY_LATE_SETTLEMENT_PROVEN','ENTRY_LATE_SETTLED')
                     OR (kind='ENTRY_PARTIAL_SETTLED' AND EXISTS (
                       SELECT 1 FROM operation_receipts partial
                       JOIN intents i ON i.intent_id=json_extract(partial.result_ref,'$.intent_id')
                       JOIN risk_receipts r ON r.intent_id=i.intent_id
                       WHERE partial.operation_id=replace(operation_receipts.operation_id,
                                 'entry-partial-settled:','entry-partial:')
                         AND partial.kind='ENTRY_PARTIAL_SETTLEMENT_PROVEN'
                         AND partial.request_hash=operation_receipts.request_hash
                         AND json_extract(partial.result_ref,'$.source_digest')=operation_receipts.result_ref
                         AND json_extract(partial.result_ref,'$.plan_hash')=i.plan_hash
                         AND json_extract(partial.result_ref,'$.reservation_key')=r.reservation_key
                         AND json_extract(partial.result_ref,'$.venue_order_id')=i.exchange_order_id
                         AND r.plan_hash=i.plan_hash AND r.state IN ('OPEN','RELEASED')
                         AND operation_receipts.committed_at<=?5
                     ))
                    )")),
                    self.params(params![receipt_floor, receipt_end, through_ms]),
                    |r| r.get(0),
                )?;
                if !monetary_receipt {
                    // Finance or cash-flow rows are durable economic facts.
                    // A captured fill/bill cursor alone does not prove that
                    // newly projected fees, funding or liabilities have been
                    // reconciled to the independent central budget. The native
                    // terminal/late settlement verifier remains mandatory
                    // before admitting risk based on those changed sources.
                    if finance_max > _finance_floor {
                        // External capital transfers and realized PnL are
                        // already owned by account NAV/loss accounting. Only
                        // previously unpriced entry-related fees require a
                        // terminal/late settlement receipt before the
                        // independent ENTRY budget can be used again.
                        let unpriced_fee: bool = self.connection.query_row(
                            self.sql(
                                "SELECT EXISTS(
                                SELECT 1 FROM finance_entries
                                WHERE rowid>?1 AND rowid<=?2
                                  AND component IN ('FEE','LIQUIDATION_COST')
                            )",
                                campaign_read!(
                                    "SELECT EXISTS(
                                SELECT 1 FROM finance_entries
                                WHERE rowid>?3 AND rowid<=?4
                                  AND component IN ('FEE','LIQUIDATION_COST')
                            )"
                                ),
                            ),
                            self.params(params![_finance_floor, finance_max]),
                            |r| r.get(0),
                        )?;
                        if unpriced_fee {
                            return Err(StoreError::Invalid(
                                "CAMPAIGN_NEW_FINANCE_REQUIRES_SETTLEMENT",
                            ));
                        }
                    }
                    // Cash flows change NAV and are independently checked by
                    // the fresh account/post-HELD census. They are not another
                    // purchase and must not consume the ENTRY budget.
                    let _flow_changed = flow_max > _flow_floor;
                    self.verify_captured_campaign_continuity_rows(
                        from_ms, through_ms, fill_floor, bill_floor, fill_end, bill_end,
                    )?;
                    // Only the reviewed page is committed. A partial page
                    // NEVER advances readiness/source revision, so the next
                    // execution remains fail-closed while more rows await work.
                    self.connection.execute(
                        self.sql(
                            "UPDATE campaign_entry_readiness SET
                            reviewed_fill_rowid=?2,reviewed_bill_rowid=?3,
                            reviewed_receipt_rowid=?4
                         WHERE campaign_id=?1",
                            "UPDATE campaign_entry_readiness SET
                            reviewed_fill_rowid=?4,reviewed_bill_rowid=?5,
                            reviewed_receipt_rowid=?6
                         WHERE campaign_id=?3 AND installation_id=?1 AND account_key=?2",
                        ),
                        self.params(params![campaign_id, fill_end, bill_end, receipt_end]),
                    )?;
                    if fill_end < fill_max || bill_end < bill_max || receipt_end < receipt_max {
                        return Err(StoreError::Invalid("CAMPAIGN_DELTA_BATCH_PENDING"));
                    }
                    self.mark_campaign_readiness(campaign_id, from_ms, through_ms, &frontiers)?;
                    return Ok(());
                }
            }
        }
        // Initial, mismatched, or financially-settled source: the original
        // monetary sealer remains the only authority for frozen-USD spend.
        self.seal_captured_campaign_baseline(campaign_id, from_ms, through_ms)?;
        Ok(())
    }
}

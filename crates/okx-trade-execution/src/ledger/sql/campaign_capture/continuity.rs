//! Original campaign continuity rules on a scoped, caller-owned SQL snapshot.
use super::*;
impl CampaignCapture<'_> {
    pub(in crate::account_store::sql) fn verify_captured_campaign_continuity(
        &self,
        from_ms: i64,
        through_ms: i64,
    ) -> StoreResult<()> {
        self.verify_captured_campaign_continuity_rows(from_ms, through_ms, 0, 0, i64::MAX, i64::MAX)
    }

    pub(in crate::account_store::sql) fn verify_captured_campaign_continuity_rows(
        &self,
        from_ms: i64,
        through_ms: i64,
        fill_after: i64,
        bill_after: i64,
        fill_end: i64,
        bill_end: i64,
    ) -> StoreResult<()> {
        self.captured_complete_frontiers(from_ms, through_ms)?;
        let unknown_fill: i64 = self.connection.query_row(
            self.sql("SELECT EXISTS(
               SELECT 1 FROM fills f
               LEFT JOIN intents i ON i.intent_id=f.intent_id
               LEFT JOIN intent_plans p ON p.intent_id=i.intent_id
               LEFT JOIN risk_receipts r ON r.intent_id=i.intent_id
               LEFT JOIN operation_receipts e
                 ON e.operation_id='entry-settlement:'||i.intent_id
               WHERE f.time>?1 AND f.time<=?2
               AND f.rowid>?3 AND f.rowid<=?4
               AND (i.intent_id IS NULL OR p.intent_id IS NULL
                 OR i.instrument_id!=f.instrument_id
                 OR i.exchange_order_id IS NULL
                 OR i.exchange_order_id!=f.exchange_order_id
                 OR i.begin_post_at IS NULL OR i.begin_post_at>f.time
                 OR p.side!=f.side
                 OR (i.risk_increase=1 AND
                     (r.state IS NULL OR r.plan_hash!=i.plan_hash
                      OR (
                        i.state IN ('FILLED','CANCELLED') AND
                        (r.state!='RELEASED'
                         OR e.kind IS NULL OR e.kind!='ENTRY_SETTLEMENT_PROVEN'
                         OR e.request_hash!=i.plan_hash OR e.committed_at>?2)
                        AND NOT (
                          i.state='FILLED' AND r.state='OPEN'
                          AND EXISTS (
                            SELECT 1
                            FROM operation_receipts head
                            JOIN operation_receipts partial
                              ON partial.operation_id='entry-partial:'||
                                i.intent_id||':r'||json_extract(
                                  head.result_ref,'$.settlement_revision')
                             AND partial.kind='ENTRY_PARTIAL_SETTLEMENT_PROVEN'
                             AND partial.request_hash=i.plan_hash
                             AND partial.result_ref=head.result_ref
                            WHERE head.operation_id='entry-partial-head:'||i.intent_id
                              AND head.kind='ENTRY_PARTIAL_HEAD'
                              AND head.request_hash=i.plan_hash
                              AND json_extract(head.result_ref,'$.schema')
                                ='okx.entry-partial-settlement/v1'
                              AND json_extract(head.result_ref,'$.account_key')=?5
                              AND json_extract(head.result_ref,'$.intent_id')=i.intent_id
                              AND json_extract(head.result_ref,'$.plan_hash')=i.plan_hash
                              AND json_extract(head.result_ref,'$.reservation_key')=r.reservation_key
                              AND json_extract(head.result_ref,'$.venue_order_id')=i.exchange_order_id
                              AND json_extract(head.result_ref,'$.source_covered_through_ms')>=f.time
                              AND partial.committed_at<=?2
                          )
                        )
                      )
                      OR (
                        i.state IN ('ACKNOWLEDGED','PARTIALLY_FILLED') AND
                        (r.state NOT IN ('POSTED','OPEN') OR NOT EXISTS(
                           SELECT 1 FROM operation_receipts a
                           WHERE a.operation_id='entry-admission:'||i.intent_id
                             AND a.kind='ENTRY_ADMISSION_PROVEN'
                             AND a.request_hash=i.plan_hash))
                      )
                      OR i.state NOT IN
                         ('FILLED','CANCELLED','ACKNOWLEDGED','PARTIALLY_FILLED')))
                 OR (i.risk_increase=0 AND i.state NOT IN
                     ('FILLED','CANCELLED','ACKNOWLEDGED','PARTIALLY_FILLED')))
               LIMIT 1)", campaign_read!("SELECT EXISTS(
               SELECT 1 FROM fills f
               LEFT JOIN intents i ON i.intent_id=f.intent_id
               LEFT JOIN intent_plans p ON p.intent_id=i.intent_id
               LEFT JOIN risk_receipts r ON r.intent_id=i.intent_id
               LEFT JOIN operation_receipts e
                 ON e.operation_id='entry-settlement:'||i.intent_id
               WHERE f.time>?3 AND f.time<=?4
               AND f.rowid>?5 AND f.rowid<=?6
               AND (i.intent_id IS NULL OR p.intent_id IS NULL
                 OR i.instrument_id!=f.instrument_id
                 OR i.exchange_order_id IS NULL
                 OR i.exchange_order_id!=f.exchange_order_id
                 OR i.begin_post_at IS NULL OR i.begin_post_at>f.time
                 OR p.side!=f.side
                 OR (i.risk_increase=1 AND
                     (r.state IS NULL OR r.plan_hash!=i.plan_hash
                      OR (
                        i.state IN ('FILLED','CANCELLED') AND
                        (r.state!='RELEASED'
                         OR e.kind IS NULL OR e.kind!='ENTRY_SETTLEMENT_PROVEN'
                         OR e.request_hash!=i.plan_hash OR e.committed_at>?4)
                        AND NOT (
                          i.state='FILLED' AND r.state='OPEN'
                          AND EXISTS (
                            SELECT 1
                            FROM operation_receipts head
                            JOIN operation_receipts partial
                              ON partial.operation_id='entry-partial:'||
                                i.intent_id||':r'||json_extract(
                                  head.result_ref,'$.settlement_revision')
                             AND partial.kind='ENTRY_PARTIAL_SETTLEMENT_PROVEN'
                             AND partial.request_hash=i.plan_hash
                             AND partial.result_ref=head.result_ref
                            WHERE head.operation_id='entry-partial-head:'||i.intent_id
                              AND head.kind='ENTRY_PARTIAL_HEAD'
                              AND head.request_hash=i.plan_hash
                              AND json_extract(head.result_ref,'$.schema')
                                ='okx.entry-partial-settlement/v1'
                              AND json_extract(head.result_ref,'$.account_key')=?7
                              AND json_extract(head.result_ref,'$.intent_id')=i.intent_id
                              AND json_extract(head.result_ref,'$.plan_hash')=i.plan_hash
                              AND json_extract(head.result_ref,'$.reservation_key')=r.reservation_key
                              AND json_extract(head.result_ref,'$.venue_order_id')=i.exchange_order_id
                              AND json_extract(head.result_ref,'$.source_covered_through_ms')>=f.time
                              AND partial.committed_at<=?4
                          )
                        )
                      )
                      OR (
                        i.state IN ('ACKNOWLEDGED','PARTIALLY_FILLED') AND
                        (r.state NOT IN ('POSTED','OPEN') OR NOT EXISTS(
                           SELECT 1 FROM operation_receipts a
                           WHERE a.operation_id='entry-admission:'||i.intent_id
                             AND a.kind='ENTRY_ADMISSION_PROVEN'
                             AND a.request_hash=i.plan_hash))
                      )
                      OR i.state NOT IN
                         ('FILLED','CANCELLED','ACKNOWLEDGED','PARTIALLY_FILLED')))
                 OR (i.risk_increase=0 AND i.state NOT IN
                     ('FILLED','CANCELLED','ACKNOWLEDGED','PARTIALLY_FILLED')))
               LIMIT 1)")),
            self.params(params![from_ms, through_ms, fill_after, fill_end, self.meta.account_key]),
            |row| row.get(0),
        )?;
        if unknown_fill != 0 {
            return Err(StoreError::Invalid("ENTRY_POST_OPENING_FILL_NOT_SETTLED"));
        }
        let unknown_bill: i64 = self.connection.query_row(
            self.sql(
                "SELECT EXISTS(
               SELECT 1 FROM bills b
               WHERE b.time>?1 AND b.time<=?2
               AND b.rowid>?3 AND b.rowid<=?4
               AND b.kind NOT IN ('1:11','1:12','8:173','8:174')
               AND b.kind NOT LIKE '7:%'
               AND NOT EXISTS(
                 SELECT 1 FROM finance_entries e
                 JOIN finance_source_links l ON l.entry_id=e.entry_id
                 JOIN fills f ON l.source_ref='fill:'||f.fill_id
                 WHERE e.economic_event_key=b.economic_event_key
                   AND e.component='FEE'
                   AND f.time>?1 AND f.time<=?2
               )
               AND NOT EXISTS(
                 SELECT 1 FROM operation_receipts own
                 JOIN risk_receipts rr
                   ON rr.intent_id=json_extract(own.result_ref,'$.intent_id')
                     AND rr.state='RELEASED'
                 JOIN operation_receipts terminal
                   ON terminal.operation_id='entry-settlement:'||rr.intent_id
                     AND terminal.kind='ENTRY_SETTLEMENT_PROVEN'
                 LEFT JOIN operation_receipts late
                   ON late.operation_id='entry-late-head:'||rr.intent_id
                     AND late.kind='ENTRY_LATE_HEAD'
                 LEFT JOIN operation_receipts late_ack
                   ON late_ack.operation_id='entry-late-settled:'||
                     rr.intent_id||':r'||json_extract(late.result_ref,'$.settlement_revision')
                     AND late_ack.kind='ENTRY_LATE_SETTLED'
                     AND late_ack.result_ref=json_extract(late.result_ref,'$.source_digest')
                 WHERE own.operation_id='entry-bill-owner:'||b.bill_id
                   AND own.kind='ENTRY_ORDER_FEE_INDEX'
                   AND own.request_hash=rr.plan_hash
                   AND b.time<=COALESCE(
                     json_extract(late.result_ref,'$.source_covered_through_ms'),
                     json_extract(terminal.result_ref,'$.source_covered_through_ms'))
                   AND (late.operation_id IS NULL OR late_ack.operation_id IS NOT NULL)
               ) LIMIT 1)",
                campaign_read!(
                    "SELECT EXISTS(
               SELECT 1 FROM bills b
               WHERE b.time>?3 AND b.time<=?4
               AND b.rowid>?5 AND b.rowid<=?6
               AND b.kind NOT IN ('1:11','1:12','8:173','8:174')
               AND b.kind NOT LIKE '7:%'
               AND NOT EXISTS(
                 SELECT 1 FROM finance_entries e
                 JOIN finance_source_links l ON l.entry_id=e.entry_id
                 JOIN fills f ON l.source_ref='fill:'||f.fill_id
                 WHERE e.economic_event_key=b.economic_event_key
                   AND e.component='FEE'
                   AND f.time>?3 AND f.time<=?4
               )
               AND NOT EXISTS(
                 SELECT 1 FROM operation_receipts own
                 JOIN risk_receipts rr
                   ON rr.intent_id=json_extract(own.result_ref,'$.intent_id')
                     AND rr.state='RELEASED'
                 JOIN operation_receipts terminal
                   ON terminal.operation_id='entry-settlement:'||rr.intent_id
                     AND terminal.kind='ENTRY_SETTLEMENT_PROVEN'
                 LEFT JOIN operation_receipts late
                   ON late.operation_id='entry-late-head:'||rr.intent_id
                     AND late.kind='ENTRY_LATE_HEAD'
                 LEFT JOIN operation_receipts late_ack
                   ON late_ack.operation_id='entry-late-settled:'||
                     rr.intent_id||':r'||json_extract(late.result_ref,'$.settlement_revision')
                     AND late_ack.kind='ENTRY_LATE_SETTLED'
                     AND late_ack.result_ref=json_extract(late.result_ref,'$.source_digest')
                 WHERE own.operation_id='entry-bill-owner:'||b.bill_id
                   AND own.kind='ENTRY_ORDER_FEE_INDEX'
                   AND own.request_hash=rr.plan_hash
                   AND b.time<=COALESCE(
                     json_extract(late.result_ref,'$.source_covered_through_ms'),
                     json_extract(terminal.result_ref,'$.source_covered_through_ms'))
                   AND (late.operation_id IS NULL OR late_ack.operation_id IS NOT NULL)
               ) LIMIT 1)"
                ),
            ),
            self.params(params![from_ms, through_ms, bill_after, bill_end]),
            |row| row.get(0),
        )?;
        if unknown_bill != 0 {
            return Err(StoreError::Invalid(
                "ENTRY_POST_OPENING_BILL_NOT_RECONCILED",
            ));
        }
        // This is financial-source continuity, NOT a flat-account gate.
        // Known posted/partially filled exposure remains covered by the
        // original central reservation; the post-HELD native/venue census
        // independently checks positions, orders, protection and total risk.
        // PREPARED is unsent, RESERVED holds known risk. Neither is an
        // unknown external economic event. POSTING/UNKNOWN cannot be
        // presumed sent or not sent and block new risk until reconciliation.
        let uncertain: i64 = self.connection.query_row(
            self.sql(
                "SELECT EXISTS(
               SELECT 1 FROM intents i
               LEFT JOIN risk_receipts r ON r.intent_id=i.intent_id
               WHERE i.state IN ('POSTING','UNKNOWN')
                  OR (i.state='RESERVED' AND
                      (r.state IS NULL OR r.state!='HELD'
                       OR r.plan_hash!=i.plan_hash))
                  OR (i.state IN ('ACKNOWLEDGED','PARTIALLY_FILLED') AND
                      i.risk_increase=1 AND
                      (r.state IS NULL OR r.state NOT IN ('POSTED','OPEN')
                       OR r.plan_hash!=i.plan_hash))
             )",
                campaign_read!(
                    "SELECT EXISTS(
               SELECT 1 FROM intents i
               LEFT JOIN risk_receipts r ON r.intent_id=i.intent_id
               WHERE i.state IN ('POSTING','UNKNOWN')
                  OR (i.state='RESERVED' AND
                      (r.state IS NULL OR r.state!='HELD'
                       OR r.plan_hash!=i.plan_hash))
                  OR (i.state IN ('ACKNOWLEDGED','PARTIALLY_FILLED') AND
                      i.risk_increase=1 AND
                      (r.state IS NULL OR r.state NOT IN ('POSTED','OPEN')
                       OR r.plan_hash!=i.plan_hash))
             )"
                ),
            ),
            self.params(&[]),
            |row| row.get(0),
        )?;
        if uncertain != 0 {
            return Err(StoreError::Invalid(
                "ENTRY_POST_OPENING_NATIVE_EXPOSURE_UNRESOLVED",
            ));
        }
        Ok(())
    }
}

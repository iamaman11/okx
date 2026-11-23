//! Original campaign baseline rules on a scoped, caller-owned SQL snapshot.
use super::*;
impl CampaignCapture<'_> {
    pub(in crate::account_store::sql) fn seal_captured_campaign_baseline(
        &self,
        campaign_id: &str,
        from_ms: i64,
        through_ms: i64,
    ) -> StoreResult<(Decimal, String)> {
        use sha2::{Digest, Sha256};
        if !campaign_id.starts_with("sha256:")
            || campaign_id.len() != 71
            || !campaign_id[7..].bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(StoreError::Invalid("ENTRY_OPENING_WINDOW_INVALID"));
        }
        let frontiers = self.captured_complete_frontiers(from_ms, through_ms)?;
        let reviewed: Option<(i64, i64, i64, i64, i64, String, i64)> = self
            .connection
            .query_row(
                self.sql(
                    "SELECT from_ms,covered_through_ms,reviewed_fill_rowid,
                        reviewed_bill_rowid,reviewed_receipt_rowid,
                        source_generations,reviewed_late_epoch
                 FROM campaign_entry_readiness WHERE campaign_id=?1",
                    campaign_read!(
                        "SELECT from_ms,covered_through_ms,reviewed_fill_rowid,
                        reviewed_bill_rowid,reviewed_receipt_rowid,
                        source_generations,reviewed_late_epoch
                 FROM campaign_entry_readiness WHERE campaign_id=?3"
                    ),
                ),
                self.params(params![campaign_id]),
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                    ))
                },
            )
            .optional()?;
        let older_verified =
            reviewed
                .as_ref()
                .filter(|(origin, covered, _, _, _, generations, _)| {
                    *origin == from_ms
                        && *covered <= through_ms
                        && generations == &Self::campaign_source_generations(&frontiers)
                });
        if let Some((_, _, fill, bill, _receipt, _, _)) = older_verified {
            self.verify_captured_campaign_continuity_rows(
                from_ms,
                through_ms,
                *fill,
                *bill,
                i64::MAX,
                i64::MAX,
            )?;
        } else {
            self.verify_captured_campaign_continuity(from_ms, through_ms)?;
        }
        // The campaign projection is authoritative only for the *previously
        // completed* and already source-verified frontier. Scan the new
        // interval, not the whole campaign on every post-trade preview.
        // A late cumulative revision is a NEW economic delta even when its
        // original terminal order belongs to an older interval.
        let checkpoint: Option<(i64, i64, String)> = self
            .connection
            .query_row(
                self.sql(
                    "SELECT from_ms,covered_through_ms,consumed_usd
             FROM campaign_entry_totals WHERE campaign_id=?1",
                    campaign_read!(
                        "SELECT from_ms,covered_through_ms,consumed_usd
             FROM campaign_entry_totals WHERE campaign_id=?3"
                    ),
                ),
                self.params(params![campaign_id]),
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let event_cursor = self.campaign_receipt_cursor(campaign_id)?;
        // An earlier partial batch can have durable monetary deltas but
        // deliberately NO completed immutable window proof yet.
        let window_sealed: bool = self.connection.query_row(
            self.sql(
                "SELECT EXISTS(
                SELECT 1 FROM campaign_entry_window_proofs
                WHERE campaign_id=?1 AND covered_through_ms=?2
            )",
                campaign_read!(
                    "SELECT EXISTS(
                SELECT 1 FROM campaign_entry_window_proofs
                WHERE campaign_id=?3 AND covered_through_ms=?4
            )"
                ),
            ),
            self.params(params![campaign_id, through_ms]),
            |r| r.get(0),
        )?;
        if let Some((origin, cut, amount)) = checkpoint.as_ref()
            && *origin == from_ms
            && *cut == through_ms
            && window_sealed
        {
            // A backfilled settlement or late fee can arrive with an
            // event timestamp inside an already sealed interval. Its
            // original cursor cut-off is NOT permission to return the
            // previous money proof unchanged. Refuse same-frontier
            // replay until a new fully captured interval is available.
            let newer: bool = if let Some(cursor) = event_cursor {
                // Durable event sequence catches both fresh terminals and
                // in-place late-head updates, even at an old exchange time.
                self.latest_campaign_receipt_event()? > cursor
            } else {
                // An existing ledger with a completed old-version money
                // checkpoint but no indexed migration cursor must re-import
                // at a NEW source frontier. It cannot replay the old proof
                // after a pre-migration late-head update.
                true
            };
            if newer {
                return Err(StoreError::Invalid(
                    "CAMPAIGN_FRONTIER_STALE_ECONOMIC_SOURCE",
                ));
            }
            let prior: Option<(String, String)> = self
                .connection
                .query_row(
                    self.sql(
                        "SELECT consumed_usd,source_digest
                 FROM campaign_entry_window_proofs
                 WHERE campaign_id=?1 AND covered_through_ms=?2",
                        campaign_read!(
                            "SELECT consumed_usd,source_digest
                 FROM campaign_entry_window_proofs
                 WHERE campaign_id=?3 AND covered_through_ms=?4"
                        ),
                    ),
                    self.params(params![campaign_id, through_ms]),
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            let (old_money, proof) =
                prior.ok_or(StoreError::Invalid("CAMPAIGN_OPENING_PROOF_MISSING"))?;
            if old_money != *amount {
                return Err(StoreError::Invalid("CAMPAIGN_OPENING_TOTAL_CONFLICT"));
            }
            let key = format!("campaign_entry_opening:{campaign_id}:{proof}");
            let receipt = self
                .operation(&key)?
                .ok_or(StoreError::Invalid("CAMPAIGN_OPENING_RECEIPT_MISSING"))?;
            if receipt.kind != "CAMPAIGN_ENTRY_OPENING_SEALED"
                || receipt.request_hash != proof
                || receipt.result_ref != proof
                || receipt.committed_at != through_ms
            {
                return Err(StoreError::Invalid("CAMPAIGN_OPENING_RECEIPT_CONFLICT"));
            }
            self.mark_campaign_readiness(campaign_id, from_ms, through_ms, &frontiers)?;
            return Ok((decimal(old_money)?, proof));
        }
        let (_since, mut total) = match checkpoint.as_ref() {
            Some((origin, cut, amount)) if *origin == from_ms && *cut <= through_ms => {
                (*cut, decimal(amount.clone())?)
            }
            Some(_) => return Err(StoreError::Invalid("CAMPAIGN_APPLIED_FRONTIER_REGRESSION")),
            None => (from_ms, Decimal::ZERO),
        };
        // Each change to terminal settlement OR mutable late-head is
        // indexed when committed by the same SQL owner. The derived queue is
        // only a lookup index: original immutable terminal/late/central
        // receipts remain the sole authority for USD money.
        let (event_json, batch_seq, batch_pending) = if let Some(cursor) = event_cursor {
            let (ids, seq, pending) = self.new_campaign_receipt_events(cursor)?;
            let json = serde_json::to_string(&ids)
                .map_err(|_| StoreError::Invalid("CAMPAIGN_RECEIPT_EVENTS_SERIALIZATION"))?;
            (Some(json), seq, pending)
        } else {
            (None, 0, false)
        };
        let indexed = self.sql("SELECT t.operation_id,
                       CASE WHEN t.kind='ENTRY_PARTIAL_SETTLED' THEN p.result_ref ELSE t.result_ref END,
                       CASE WHEN t.kind='ENTRY_PARTIAL_SETTLED' THEN p.committed_at ELSE t.committed_at END
                       FROM json_each(?1) e
                       LEFT JOIN operation_receipts t ON t.operation_id=e.value
                       LEFT JOIN operation_receipts p ON p.operation_id=
                         replace(t.operation_id,'entry-partial-settled:','entry-partial:')
                          AND p.kind='ENTRY_PARTIAL_SETTLEMENT_PROVEN'
                       ORDER BY t.operation_id", campaign_read!("SELECT t.operation_id,
                       CASE WHEN t.kind='ENTRY_PARTIAL_SETTLED' THEN p.result_ref ELSE t.result_ref END,
                       CASE WHEN t.kind='ENTRY_PARTIAL_SETTLED' THEN p.committed_at ELSE t.committed_at END
                       FROM json_each(?3) e
                       LEFT JOIN operation_receipts t ON t.operation_id=e.value
                       LEFT JOIN operation_receipts p ON p.operation_id=
                         replace(t.operation_id,'entry-partial-settled:','entry-partial:')
                          AND p.kind='ENTRY_PARTIAL_SETTLEMENT_PROVEN'
                       ORDER BY t.operation_id"));
        let initial = self.sql("SELECT t.operation_id,
                       CASE WHEN t.kind='ENTRY_PARTIAL_SETTLED' THEN p.result_ref ELSE t.result_ref END,
                       CASE WHEN t.kind='ENTRY_PARTIAL_SETTLED' THEN p.committed_at ELSE t.committed_at END
                       FROM operation_receipts t
                       LEFT JOIN operation_receipts p ON p.operation_id=
                         replace(t.operation_id,'entry-partial-settled:','entry-partial:')
                         AND p.kind='ENTRY_PARTIAL_SETTLEMENT_PROVEN'
                       WHERE t.kind IN ('ENTRY_SETTLEMENT_PROVEN','ENTRY_PARTIAL_SETTLED')
                         AND t.committed_at>?1 AND t.committed_at<=?2
                       ORDER BY t.operation_id LIMIT 10001", campaign_read!("SELECT t.operation_id,
                       CASE WHEN t.kind='ENTRY_PARTIAL_SETTLED' THEN p.result_ref ELSE t.result_ref END,
                       CASE WHEN t.kind='ENTRY_PARTIAL_SETTLED' THEN p.committed_at ELSE t.committed_at END
                       FROM operation_receipts t
                       LEFT JOIN operation_receipts p ON p.operation_id=
                         replace(t.operation_id,'entry-partial-settled:','entry-partial:')
                         AND p.kind='ENTRY_PARTIAL_SETTLEMENT_PROVEN'
                       WHERE t.kind IN ('ENTRY_SETTLEMENT_PROVEN','ENTRY_PARTIAL_SETTLED')
                         AND t.committed_at>?3 AND t.committed_at<=?4
                       ORDER BY t.operation_id LIMIT 10001"));
        let mut stmt = self.connection.prepare(if event_json.is_some() {
            indexed
        } else {
            initial
        })?;
        let mut rows = if let Some(json) = event_json.as_ref() {
            stmt.query(self.params(params![json]))?
        } else {
            stmt.query(self.params(params![from_ms, through_ms]))?
        };
        let mut witnesses = Vec::new();
        // A single capture batch may include multiple cumulative revisions
        // for the SAME intent (partial r1, partial r2, terminal r3).
        // Read the prior SQL checkpoint once, then advance this in-memory
        // cursor without writing any extra ledger or double counting money.
        let mut in_batch = std::collections::BTreeMap::<String, (i64, Decimal, String)>::new();
        while let Some(row) = rows.next()? {
            if witnesses.len() >= 10_000 {
                return Err(StoreError::Invalid("ENTRY_OPENING_HISTORY_TOO_LARGE"));
            }
            let operation_id: String = row.get(0)?;
            let encoded: String = row.get(1)?;
            let time: i64 = row.get(2)?;
            if time > through_ms {
                return Err(StoreError::Invalid("CAMPAIGN_RECEIPT_EVENT_NOT_COVERED"));
            }
            if time <= from_ms {
                continue;
            }
            let terminal: EntrySettlementEvidence = serde_json::from_str(&encoded)
                .map_err(|_| StoreError::Invalid("ENTRY_OPENING_SETTLEMENT_CORRUPT"))?;
            let partial = operation_id.starts_with("entry-partial-settled:");
            if (partial
                && (operation_id
                    != format!(
                        "entry-partial-settled:{}:r{}",
                        terminal.intent_id, terminal.settlement_revision
                    )
                    || terminal.schema != "okx.entry-partial-settlement/v1"))
                || (!partial
                    && (operation_id != format!("entry-settlement:{}", terminal.intent_id)
                        || terminal.schema != "okx.entry-terminal-settlement/v1"))
                || terminal.account_key != self.meta.account_key
                || terminal.venue_update_ms != time
            {
                return Err(StoreError::Invalid("ENTRY_OPENING_SETTLEMENT_INVALID"));
            }
            // Additional fee bills are cumulative revisions, never a
            // second historical entry. Only the exact central-settled latest
            // revision may replace the terminal amount in an opening proof.
            if partial {
                let ack = self
                    .operation(&operation_id)?
                    .ok_or(StoreError::Invalid("CAMPAIGN_PARTIAL_ACK_MISSING"))?;
                let immutable = self
                    .operation(&format!(
                        "entry-partial:{}:r{}",
                        terminal.intent_id, terminal.settlement_revision
                    ))?
                    .ok_or(StoreError::Invalid("CAMPAIGN_PARTIAL_IMMUTABLE_MISSING"))?;
                let encoded = serde_json::to_string(&terminal)
                    .map_err(|_| StoreError::Invalid("CAMPAIGN_PARTIAL_SERIALIZATION"))?;
                if terminal.source_covered_through_ms > through_ms {
                    return Err(StoreError::Invalid("CAMPAIGN_DELTA_EVENT_NOT_COVERED"));
                }
                if ack.kind != "ENTRY_PARTIAL_SETTLED"
                    || ack.request_hash != terminal.plan_hash
                    || ack.result_ref != terminal.source_digest
                    || immutable.kind != "ENTRY_PARTIAL_SETTLEMENT_PROVEN"
                    || immutable.request_hash != terminal.plan_hash
                    || immutable.result_ref != encoded
                {
                    return Err(StoreError::Invalid("CAMPAIGN_PARTIAL_ACK_INVALID"));
                }
            }
            let latest: Option<String> = if partial {
                None
            } else {
                self.connection
                    .query_row(
                        self.sql(
                            "SELECT result_ref FROM operation_receipts
                 WHERE operation_id='entry-late-head:'||?1 AND kind='ENTRY_LATE_HEAD'",
                            campaign_read!(
                                "SELECT result_ref FROM operation_receipts
                 WHERE operation_id='entry-late-head:'||?3 AND kind='ENTRY_LATE_HEAD'"
                            ),
                        ),
                        self.params(params![&terminal.intent_id]),
                        |r| r.get(0),
                    )
                    .optional()?
            };
            let value: EntrySettlementEvidence = if let Some(revision) = latest {
                let late: EntrySettlementEvidence = serde_json::from_str(&revision)
                    .map_err(|_| StoreError::Invalid("ENTRY_OPENING_LATE_CORRUPT"))?;
                let immutable: Option<String> = self
                    .connection
                    .query_row(
                        self.sql(
                            "SELECT result_ref FROM operation_receipts
                     WHERE operation_id=?1 AND kind='ENTRY_LATE_SETTLEMENT_PROVEN'
                       AND request_hash=?2",
                            campaign_read!(
                                "SELECT result_ref FROM operation_receipts
                     WHERE operation_id=?3 AND kind='ENTRY_LATE_SETTLEMENT_PROVEN'
                       AND request_hash=?4"
                            ),
                        ),
                        self.params(params![
                            format!(
                                "entry-late:{}:r{}",
                                late.intent_id, late.settlement_revision
                            ),
                            late.plan_hash
                        ]),
                        |r| r.get(0),
                    )
                    .optional()?;
                let proof: Option<String> = self
                    .connection
                    .query_row(
                        self.sql(
                            "SELECT result_ref FROM operation_receipts
                     WHERE operation_id=?1 AND kind='ENTRY_LATE_SETTLED'
                       AND request_hash=?2",
                            campaign_read!(
                                "SELECT result_ref FROM operation_receipts
                     WHERE operation_id=?3 AND kind='ENTRY_LATE_SETTLED'
                       AND request_hash=?4"
                            ),
                        ),
                        self.params(params![
                            format!(
                                "entry-late-settled:{}:r{}",
                                late.intent_id, late.settlement_revision
                            ),
                            late.plan_hash
                        ]),
                        |r| r.get(0),
                    )
                    .optional()?;
                if late.schema != "okx.entry-late-settlement/v1"
                    || late.account_key != terminal.account_key
                    || late.intent_id != terminal.intent_id
                    || late.plan_hash != terminal.plan_hash
                    || late.reservation_key != terminal.reservation_key
                    || late.venue_order_id != terminal.venue_order_id
                    || late.venue_update_ms != terminal.venue_update_ms
                    || late.settlement_revision <= terminal.settlement_revision
                    || late.cumulative_entry_usd < terminal.cumulative_entry_usd
                    || immutable.as_deref() != Some(revision.as_str())
                    || proof.as_deref() != Some(late.source_digest.as_str())
                    || late.source_covered_through_ms > through_ms
                {
                    return Err(StoreError::Invalid("ENTRY_OPENING_LATE_UNSETTLED"));
                }
                late
            } else {
                terminal
            };
            if value.cumulative_entry_usd < Decimal::ZERO
                || (value.cumulative_entry_usd == Decimal::ZERO
                    && (value.venue_filled_quantity != Decimal::ZERO
                        || value.booked_notional_and_fee_usdt != Decimal::ZERO
                        || value.settlement_revision != 1
                        || value.schema != "okx.entry-terminal-settlement/v1"))
                || value.source_digest.len() != 64
                || !value.source_digest.bytes().all(|b| b.is_ascii_hexdigit())
            {
                return Err(StoreError::Invalid("ENTRY_OPENING_SETTLEMENT_INVALID"));
            }
            let native: Option<(String, i64, String, String)> = self
                .connection
                .query_row(
                    self.sql(
                        "SELECT i.plan_hash,i.begin_post_at,r.state,r.reservation_key
                 FROM intents i JOIN risk_receipts r ON r.intent_id=i.intent_id
                 WHERE i.intent_id=?1 AND i.exchange_order_id=?2
                   AND i.risk_increase=1
                   AND i.state IN ('FILLED','CANCELLED','PARTIALLY_FILLED')
                   AND r.plan_hash=i.plan_hash",
                        campaign_read!(
                            "SELECT i.plan_hash,i.begin_post_at,r.state,r.reservation_key
                 FROM intents i JOIN risk_receipts r ON r.intent_id=i.intent_id
                 WHERE i.intent_id=?3 AND i.exchange_order_id=?4
                   AND i.risk_increase=1
                   AND i.state IN ('FILLED','CANCELLED','PARTIALLY_FILLED')
                   AND r.plan_hash=i.plan_hash"
                        ),
                    ),
                    self.params(params![value.intent_id, value.venue_order_id]),
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
                .optional()?;
            if native
                .as_ref()
                .is_none_or(|(hash, post, state, reservation)| {
                    hash != &value.plan_hash
                        || *post < from_ms
                        || (if partial {
                            state != "OPEN"
                                && !(state == "RELEASED"
                                    && self
                                        .operation(&format!("entry-settlement:{}", value.intent_id))
                                        .ok()
                                        .flatten()
                                        .is_some_and(|x| {
                                            x.kind == "ENTRY_SETTLEMENT_PROVEN"
                                                && x.request_hash == value.plan_hash
                                        }))
                        } else {
                            state != "RELEASED"
                        })
                        || reservation != &value.reservation_key
                })
            {
                return Err(StoreError::Invalid("ENTRY_OPENING_SETTLEMENT_NOT_RELEASED"));
            }
            let admission: Option<String> = self
                .connection
                .query_row(
                    self.sql(
                        "SELECT result_ref FROM operation_receipts
                 WHERE operation_id='entry-admission:'||?1
                   AND kind='ENTRY_ADMISSION_PROVEN'",
                        campaign_read!(
                            "SELECT result_ref FROM operation_receipts
                 WHERE operation_id='entry-admission:'||?3
                   AND kind='ENTRY_ADMISSION_PROVEN'"
                        ),
                    ),
                    self.params(params![value.intent_id.as_str()]),
                    |r| r.get(0),
                )
                .optional()?;
            let admission: EntryAdmissionEvidence = serde_json::from_str(
                admission
                    .as_deref()
                    .ok_or(StoreError::Invalid("ENTRY_OPENING_ADMISSION_MISSING"))?,
            )
            .map_err(|_| StoreError::Invalid("ENTRY_OPENING_ADMISSION_INVALID"))?;
            if admission.account_key != value.account_key
                || admission.installation_id != self.meta.installation_id
                || admission.intent_id != value.intent_id
                || admission.plan_hash != value.plan_hash
                || value.cumulative_entry_usd > admission.entry_upper_usd
            {
                // An original debit breach remains a STOP; a reconstructed
                // opening must not erase it by replacing the central book.
                return Err(StoreError::Invalid(
                    "ENTRY_OPENING_BOUND_BREACH_REQUIRES_STOP",
                ));
            }
            // A late source revision replaces its previous cumulative
            // amount; the campaign only consumes the positive difference.
            let applied: Option<(i64, String, String)> = self
                .connection
                .query_row(
                    self.sql(
                        "SELECT settlement_revision,cumulative_usd,source_digest
                 FROM campaign_entry_applied
                 WHERE campaign_id=?1 AND intent_id=?2
                 ORDER BY settlement_revision DESC LIMIT 1",
                        campaign_read!(
                            "SELECT settlement_revision,cumulative_usd,source_digest
                 FROM campaign_entry_applied
                 WHERE campaign_id=?3 AND intent_id=?4
                 ORDER BY settlement_revision DESC LIMIT 1"
                        ),
                    ),
                    self.params(params![campaign_id, value.intent_id]),
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            let applied = if let Some((revision, amount, source)) = in_batch.get(&value.intent_id) {
                Some((*revision, amount.to_string(), source.clone()))
            } else {
                applied
            };
            let previous = if let Some((rev, amount, digest)) = applied {
                let amount = decimal(amount)?;
                let revision = i64::try_from(value.settlement_revision)
                    .map_err(|_| StoreError::Invalid("CAMPAIGN_APPLIED_REVISION_INVALID"))?;
                if rev > revision {
                    // The same bounded source scan may revisit an older
                    // Worker-ACKed immutable revision after a newer one was
                    // durably applied. Only the exact historical money AND
                    // digest may replay; it contributes no new campaign debit.
                    let historical: Option<(String, String)> = self
                        .connection
                        .query_row(
                            self.sql("SELECT cumulative_usd,source_digest FROM campaign_entry_applied
                         WHERE campaign_id=?1 AND intent_id=?2
                           AND settlement_revision=?3", campaign_read!("SELECT cumulative_usd,source_digest FROM campaign_entry_applied
                         WHERE campaign_id=?3 AND intent_id=?4
                           AND settlement_revision=?5")),
                            self.params(params![campaign_id, value.intent_id, revision]),
                            |row| Ok((row.get(0)?, row.get(1)?)),
                        )
                        .optional()?;
                    if historical.as_ref().is_some_and(|(money, hash)| {
                        decimal(money.clone()).ok() == Some(value.cumulative_entry_usd)
                            && hash == &value.source_digest
                    }) {
                        continue;
                    }
                    return Err(StoreError::Invalid("CAMPAIGN_APPLIED_MONOTONICITY_BREACH"));
                }
                if amount > value.cumulative_entry_usd {
                    return Err(StoreError::Invalid("CAMPAIGN_APPLIED_MONOTONICITY_BREACH"));
                }
                if rev == revision {
                    if amount != value.cumulative_entry_usd || digest != value.source_digest {
                        return Err(StoreError::Invalid("CAMPAIGN_APPLIED_SOURCE_CONFLICT"));
                    }
                    continue;
                }
                amount
            } else {
                Decimal::ZERO
            };
            let delta = value
                .cumulative_entry_usd
                .checked_sub(previous)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            total = total
                .checked_add(delta)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            in_batch.insert(
                value.intent_id.clone(),
                (
                    i64::try_from(value.settlement_revision)
                        .map_err(|_| StoreError::Invalid("CAMPAIGN_APPLIED_REVISION_INVALID"))?,
                    value.cumulative_entry_usd,
                    value.source_digest.clone(),
                ),
            );
            witnesses.push((
                value.intent_id,
                value.source_digest,
                value.cumulative_entry_usd,
                value.settlement_revision,
                value.venue_update_ms,
            ));
        }
        // A partial source page applies validated cumulative money but
        // never seals the campaign window or allows a new order. The cursor
        // moves forward only AFTER the existing SQL owner persisted the
        // checked source-backed monetary deltas.
        drop(rows);
        drop(stmt);
        if batch_pending {
            self.apply_captured_campaign_deltas(
                campaign_id,
                from_ms,
                through_ms,
                &witnesses,
                total,
                None,
            )?;
            self.save_campaign_receipt_cursor(campaign_id, batch_seq)?;
            return Err(StoreError::Invalid("CAMPAIGN_RECEIPT_BATCH_PENDING"));
        }
        // The initial zero is proved before any final window can be sealed.
        if witnesses.is_empty() && checkpoint.is_none() {
            // Compute from already verified capture facts without writing
            // an independent operation receipt ahead of the aggregate.
            // apply_captured_campaign_deltas persists both atomically.
            let zero = self.captured_empty_campaign_proof(campaign_id, from_ms, through_ms)?;
            self.apply_captured_campaign_deltas(
                campaign_id,
                from_ms,
                through_ms,
                &witnesses,
                Decimal::ZERO,
                Some(&zero),
            )?;
            self.mark_campaign_readiness(campaign_id, from_ms, through_ms, &frontiers)?;
            return Ok((Decimal::ZERO, zero));
        }
        let canonical = serde_json::to_vec(&(
            "okx.campaign-source-backed-baseline/v1",
            self.meta.account_key.as_str(),
            self.meta.installation_id.as_str(),
            campaign_id,
            from_ms,
            through_ms,
            &frontiers,
            &witnesses,
            total.to_string(),
        ))
        .map_err(|_| StoreError::Invalid("ENTRY_OPENING_SERIALIZATION_FAILED"))?;
        let proof = format!("{:x}", Sha256::digest(canonical));
        self.apply_captured_campaign_deltas(
            campaign_id,
            from_ms,
            through_ms,
            &witnesses,
            total,
            Some(&proof),
        )?;
        self.connection.execute_batch(
            "CREATE TRIGGER IF NOT EXISTS immutable_campaign_opening_update
             BEFORE UPDATE ON operation_receipts
             WHEN OLD.kind='CAMPAIGN_ENTRY_OPENING_SEALED'
             BEGIN SELECT RAISE(ABORT,'IMMUTABLE_CAMPAIGN_ENTRY_OPENING'); END;
             CREATE TRIGGER IF NOT EXISTS immutable_campaign_opening_delete
             BEFORE DELETE ON operation_receipts
             WHEN OLD.kind='CAMPAIGN_ENTRY_OPENING_SEALED'
             BEGIN SELECT RAISE(ABORT,'IMMUTABLE_CAMPAIGN_ENTRY_OPENING'); END;",
        )?;
        let key = format!("campaign_entry_opening:{campaign_id}:{proof}");
        self.connection.execute(
            self.sql(
                "INSERT OR IGNORE INTO operation_receipts
             (operation_id,kind,request_hash,result_ref,committed_at)
             VALUES(?1,'CAMPAIGN_ENTRY_OPENING_SEALED',?2,?2,?3)",
                "INSERT OR IGNORE INTO operation_receipts
             (installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at)
             VALUES(?1,?2,?3,'CAMPAIGN_ENTRY_OPENING_SEALED',?4,?4,?5)",
            ),
            self.params(params![key, proof, through_ms]),
        )?;
        let received: (String, String, String, i64) = self.connection.query_row(
            self.sql(
                "SELECT kind,request_hash,result_ref,committed_at FROM operation_receipts
             WHERE operation_id=?1",
                campaign_read!(
                    "SELECT kind,request_hash,result_ref,committed_at FROM operation_receipts
             WHERE operation_id=?3"
                ),
            ),
            self.params(params![key]),
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?;
        if received
            != (
                "CAMPAIGN_ENTRY_OPENING_SEALED".into(),
                proof.clone(),
                proof.clone(),
                through_ms,
            )
        {
            return Err(StoreError::Invalid("ENTRY_OPENING_SQL_RECEIPT_CONFLICT"));
        }
        self.mark_campaign_readiness(campaign_id, from_ms, through_ms, &frontiers)?;
        Ok((total, proof))
    }

    pub(in crate::account_store::sql) fn captured_empty_campaign_proof(
        &self,
        campaign_id: &str,
        from_ms: i64,
        through_ms: i64,
    ) -> StoreResult<String> {
        use sha2::{Digest, Sha256};
        if !campaign_id.starts_with("sha256:")
            || campaign_id.len() != 71
            || !campaign_id[7..].bytes().all(|b| b.is_ascii_hexdigit())
            || from_ms <= 0
            || through_ms <= from_ms
        {
            return Err(StoreError::Invalid("ENTRY_OPENING_WINDOW_INVALID"));
        }
        let frontiers = self.captured_empty_frontiers(from_ms, through_ms)?;
        let (fills, bills) = (0_i64, 0_i64);
        let bytes = serde_json::to_vec(&(
            "okx.campaign-captured-zero/v1",
            self.meta.account_key.as_str(),
            self.meta.installation_id.as_str(),
            campaign_id,
            from_ms,
            through_ms,
            &frontiers,
            fills,
            bills,
        ))
        .map_err(|_| StoreError::Invalid("ENTRY_OPENING_SERIALIZATION_FAILED"))?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

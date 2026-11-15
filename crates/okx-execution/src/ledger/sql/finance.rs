use super::super::*;
use super::campaign_capture::CampaignCapture;

/* F1: provenance is verified at the event boundary, not by replaying the
 * immutable finance journal for every two-second NAV. The existing cursor
 * table stores the highest fully verified finance rowid; a missing/stale
 * cursor is NOT evidence of financial completeness. */
/// Signed fee: debit (<0) consumes entry, rebate (>0) does not free cap.
fn entry_fee_debit(fee: Decimal) -> Decimal {
    if fee < Decimal::ZERO {
        -fee
    } else {
        Decimal::ZERO
    }
}

fn finance_rowid(db: &Connection) -> StoreResult<i64> {
    AccountStore::finance_rowid_sql(db, None)
}
fn finance_proof_sealed(db: &Connection) -> StoreResult<bool> {
    AccountStore::finance_proof_sealed_sql(db, None)
}
fn advance_finance_provenance(tx: &rusqlite::Transaction<'_>, previous: i64) -> StoreResult<()> {
    AccountStore::advance_finance_provenance_sql(tx, None, previous)
}

/// Start from the bounded set of unapplied fills, never from immutable history.
/// SQLite otherwise chooses idx_fills_time to satisfy ORDER BY and scans all
/// historical fills even when fill_projection_pending is empty.
pub(super) const PENDING_FILL_PROJECTION_SQL: &str = "SELECT f.fill_id,f.instrument_id,f.trade_id,f.exchange_order_id,f.intent_id,f.time,f.qty,f.price,f.fee,f.currency,p.side
    FROM fill_projection_pending p CROSS JOIN fills f ON f.fill_id=p.fill_id
    ORDER BY f.time,f.fill_id LIMIT ?1";

/// Report the transaction phase even if an early validation/SQL error returns
/// before COMMIT. The observer is diagnostic-only and never changes rollback.
struct IngestTimingGuard<O: FnOnce(IngestTransactionTiming)> {
    observe: Option<O>,
    begin_us: u64,
    body_started: std::time::Instant,
    body_us: Option<u64>,
    commit_us: u64,
    commit_succeeded: bool,
}

impl<O: FnOnce(IngestTransactionTiming)> Drop for IngestTimingGuard<O> {
    fn drop(&mut self) {
        if let Some(observe) = self.observe.take() {
            observe(IngestTransactionTiming {
                begin_us: self.begin_us,
                body_us: self.body_us.unwrap_or_else(|| {
                    u64::try_from(self.body_started.elapsed().as_micros()).unwrap_or(u64::MAX)
                }),
                commit_us: self.commit_us,
                commit_succeeded: self.commit_succeeded,
            });
        }
    }
}

/// Account-owned, monotonic source cursor update shared by ingestion and NAV.
fn upsert_source_cursor(tx: &rusqlite::Transaction<'_>, c: &SourceCursor) -> StoreResult<()> {
    AccountStore::upsert_source_cursor_sql(tx, None, c)
}
impl AccountStore {
    /// Seal an externally classified USD-denominated campaign monetary
    /// window. Only the native authenticated adapter can call this after
    /// exhausting venue orders, fills and bills and classifying every item.
    /// The SQL writer refuses local-ledger divergence (including unposted
    /// risks) and stores an immutable account-scoped receipt. UNKNOWN money
    /// cannot be imported as zero.
    /// A SQL receipt is immutable and scoped to this installation/account.
    // This immutable SQL witness carries the complete source scope as distinct
    // attested dimensions; preserve the existing call sites and wire semantics.
    #[expect(
        clippy::too_many_arguments,
        reason = "source-proven campaign opening dimensions"
    )]
    pub fn seal_campaign_entry_opening(
        &self,
        campaign_id: &str,
        from_ms: i64,
        through_ms: i64,
        venue_source_digest: &str,
        venue_fill_count: usize,
        venue_bill_count: usize,
        consumed_entry_usd: Decimal,
    ) -> StoreResult<String> {
        use sha2::{Digest, Sha256};
        if !campaign_id.starts_with("sha256:")
            || campaign_id.len() != 71
            || !campaign_id[7..].bytes().all(|b| b.is_ascii_hexdigit())
            || venue_source_digest.len() != 64
            || !venue_source_digest.bytes().all(|b| b.is_ascii_hexdigit())
            || from_ms <= 0
            || through_ms < from_ms
            || venue_fill_count != venue_bill_count
            || consumed_entry_usd < Decimal::ZERO
            || (venue_fill_count == 0 && consumed_entry_usd != Decimal::ZERO)
            || (venue_fill_count > 0 && consumed_entry_usd <= Decimal::ZERO)
        {
            return Err(StoreError::Invalid("ENTRY_OPENING_NONEMPTY_OR_INVALID"));
        }
        if !finance_proof_sealed(&self.connection)? {
            return Err(StoreError::Invalid(
                "ENTRY_OPENING_FINANCE_PROVENANCE_UNSEALED",
            ));
        }
        // Current native ledger must still be at its authenticated flat
        // opening. The venue may report historical activity before native
        // initialization; duplicate post-initialization money is not
        // automatically reconciled or counted twice. Any divergence blocks.
        let (fills, bills, finance, cash_debits, unresolved, lots): (i64, i64, i64, i64, i64, i64) =
            self.connection.query_row(
                "SELECT (SELECT COUNT(*) FROM fills),
                        (SELECT COUNT(*) FROM bills),
                        (SELECT COUNT(*) FROM finance_entries),
                        (SELECT COUNT(*) FROM cash_flows WHERE kind!='OPENING'),
                        (SELECT COUNT(*) FROM intents
                          WHERE state NOT IN ('REJECTED','CANCELLED','FILLED')),
                        (SELECT COUNT(*) FROM position_lots WHERE remaining_qty!='0')",
                [],
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
            )?;
        if fills != 0 || bills != 0 || finance != 0 || cash_debits != 0 {
            return Err(StoreError::Invalid("ENTRY_OPENING_LOCAL_LEDGER_NOT_EMPTY"));
        }
        if unresolved != 0 || lots != 0 {
            return Err(StoreError::Invalid(
                "ENTRY_OPENING_UNRESOLVED_NATIVE_EXPOSURE",
            ));
        }
        let canonical = serde_json::to_vec(&(
            "okx.campaign-entry-opening-sql/v2",
            self.meta.account_key.as_str(),
            self.meta.installation_id.as_str(),
            campaign_id,
            from_ms,
            through_ms,
            venue_source_digest,
            venue_fill_count,
            venue_bill_count,
            consumed_entry_usd.to_string(),
        ))
        .map_err(|_| StoreError::Invalid("ENTRY_OPENING_SERIALIZATION_FAILED"))?;
        let proof = format!("{:x}", Sha256::digest(canonical));
        // Install the immutable receipt guards even for a database created
        // before this release. These are the existing single SQL owner and
        // operation_receipts table, not a second financial ledger.
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
        // Only RiskBook owns the FINAL campaign balance. SQL owns immutable
        // per-attempt witnesses: a lost WSS reply or expired freshness window
        // must not permanently pin the account to a superseded cut-off.
        // Each fully verified retry gets its own immutable receipt, while an
        // identical source/cut-off is an idempotent readback.
        let key = format!("campaign_entry_opening:{campaign_id}:{proof}");
        self.connection.execute(
            "INSERT OR IGNORE INTO operation_receipts
             (operation_id,kind,request_hash,result_ref,committed_at)
             VALUES(?1,'CAMPAIGN_ENTRY_OPENING_SEALED',?2,?2,?3)",
            params![key, proof, through_ms],
        )?;
        let stored: (String, String, String, i64) = self.connection.query_row(
            "SELECT kind,request_hash,result_ref,committed_at
             FROM operation_receipts WHERE operation_id=?1",
            [key],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?;
        if stored.0 != "CAMPAIGN_ENTRY_OPENING_SEALED"
            || stored.1 != proof
            || stored.2 != proof
            || stored.3 != through_ms
        {
            return Err(StoreError::Invalid("ENTRY_OPENING_SQL_RECEIPT_CONFLICT"));
        }
        Ok(proof)
    }

    /// Read-only, source-cursor-backed absence witness. Used both during
    /// bootstrap and immediately before a new risk admission. No archive
    /// request, ledger mutation or fake gap-free state is performed here.
    fn captured_complete_frontiers(
        &self,
        from_ms: i64,
        through_ms: i64,
    ) -> StoreResult<Vec<(&'static str, &'static str, i64, i64)>> {
        CampaignCapture::legacy(&self.connection, &self.meta)
            .captured_complete_frontiers(from_ms, through_ms)
    }

    fn captured_empty_frontiers(
        &self,
        from_ms: i64,
        through_ms: i64,
    ) -> StoreResult<Vec<(&'static str, &'static str, i64, i64)>> {
        CampaignCapture::legacy(&self.connection, &self.meta)
            .captured_empty_frontiers(from_ms, through_ms)
    }

    pub fn verify_captured_empty_window(&self, from_ms: i64, through_ms: i64) -> StoreResult<()> {
        self.captured_empty_frontiers(from_ms, through_ms)
            .map(|_| ())
    }

    fn campaign_source_epoch(&self) -> StoreResult<i64> {
        CampaignCapture::legacy(&self.connection, &self.meta).campaign_source_epoch()
    }

    fn campaign_source_generations(frontiers: &[(&str, &str, i64, i64)]) -> String {
        CampaignCapture::campaign_source_generations(frontiers)
    }

    /// Only the indexed derived changes; no JOIN on historical fills/bills.
    /// The caller must validate the original terminal and latest late
    /// settlement from the immutable SQL owner before advancing this cursor.

    /// The POST path reads only an addressable campaign receipt, a mutation
    /// epoch and six source cursors. It NEVER joins archived fills/bills.
    /// An unseen bill, UNKNOWN transition or recaptured source generation
    /// invalidates this witness until maintenance reconciles it.
    pub fn verify_campaign_entry_readiness(
        &self,
        campaign_id: &str,
        from_ms: i64,
        through_ms: i64,
    ) -> StoreResult<()> {
        CampaignCapture::legacy(&self.connection, &self.meta).verify_campaign_entry_readiness(
            campaign_id,
            from_ms,
            through_ms,
        )
    }

    /// The ORIGINAL AccountStore's sealed campaign baseline, read under the
    /// installation's single SQL transaction. This validates persisted
    /// source evidence, not a WS caller's reported spend or completeness flags.
    pub(crate) fn verify_unified_campaign_entry_source_sql(
        tx: &rusqlite::Transaction<'_>,
        installation: &str,
        account: &str,
        campaign: &str,
        from_ms: i64,
        through_ms: i64,
        amount: Decimal,
        digest: &str,
    ) -> StoreResult<()> {
        const BAD: &str = "UNIFIED_OPENING_ORIGINAL_SOURCE_UNVERIFIED";
        if installation.is_empty()
            || account.is_empty()
            || campaign.is_empty()
            || from_ms <= 0
            || through_ms <= from_ms
            || amount < Decimal::ZERO
            || digest.len() != 64
            || !digest.bytes().all(|v| v.is_ascii_hexdigit())
        {
            return Err(StoreError::Invalid(BAD));
        }
        let init: Option<i64> = tx
            .query_row(
                "SELECT initialized_at FROM account_meta
             WHERE installation_id=?1 AND account_key=?2 AND singleton=1",
                params![installation, account],
                |r| r.get(0),
            )
            .optional()?;
        if init.is_none_or(|at| at > from_ms) {
            return Err(StoreError::Invalid(BAD));
        }

        let mut generations = Vec::new();
        for (source, partition) in [
            ("fills", "SPOT"),
            ("fills", "MARGIN"),
            ("fills", "SWAP"),
            ("fills", "FUTURES"),
            ("fills", "OPTION"),
            ("bills", "account"),
        ] {
            let row: Option<(String, i64, i64, String)> = tx
                .query_row(
                    "SELECT cursor,watermark,generation,coverage_gaps
                 FROM source_cursors WHERE installation_id=?1 AND account_key=?2
                   AND source=?3 AND stream_partition=?4",
                    params![installation, account, source, partition],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
                .optional()?;
            if !matches!(row, Some((ref cursor,watermark,generation,ref gaps))
                if cursor.is_empty() && gaps.is_empty()
                    && generation>0 && watermark>=through_ms)
            {
                return Err(StoreError::Invalid(BAD));
            }
            let generation = row.unwrap().2;
            generations.push(format!("{source}/{partition}:{generation}"));
        }
        let readiness: Option<(i64, i64, i64, String, i64)> = tx
            .query_row(
                "SELECT from_ms,covered_through_ms,source_revision,
                    source_generations,reviewed_late_epoch
             FROM campaign_entry_readiness WHERE installation_id=?1
               AND account_key=?2 AND campaign_id=?3",
                params![installation, account, campaign],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .optional()?;
        let epoch: Option<i64> = tx
            .query_row(
                "SELECT revision FROM campaign_source_epoch
             WHERE installation_id=?1 AND account_key=?2 AND id=1",
                params![installation, account],
                |r| r.get(0),
            )
            .optional()?;
        let late_epoch: i64 = tx.query_row(
            "SELECT COALESCE(MAX(revision),0) FROM campaign_late_head_epoch
             WHERE installation_id=?1 AND account_key=?2 AND id=1",
            params![installation, account],
            |r| r.get(0),
        )?;
        if !matches!(readiness,Some((f,t,e,ref saved_generations,late))
            if f==from_ms && t==through_ms && epoch==Some(e)
                && saved_generations==&generations.join("|") && late==late_epoch)
        {
            return Err(StoreError::Invalid(BAD));
        }

        // Recheck the ORIGINAL sealed finance provenance, scoped by account.
        let highest: i64 = tx.query_row(
            "SELECT COALESCE(MAX(rowid),0) FROM finance_entries
             WHERE installation_id=?1 AND account_key=?2",
            params![installation, account],
            |r| r.get(0),
        )?;
        let sealed: Option<i64> = tx
            .query_row(
                "SELECT watermark FROM source_cursors WHERE installation_id=?1
                AND account_key=?2 AND source='finance_provenance'
                AND stream_partition='account' AND cursor=''
                AND coverage_gaps='' AND generation=0",
                params![installation, account],
                |r| r.get(0),
            )
            .optional()?;
        if sealed != Some(highest) && !(highest == 0 && sealed.is_none()) {
            return Err(StoreError::Invalid(BAD));
        }

        let totals: Option<(i64, i64, String)> = tx
            .query_row(
                "SELECT from_ms,covered_through_ms,consumed_usd
             FROM campaign_entry_totals WHERE installation_id=?1
                AND account_key=?2 AND campaign_id=?3",
                params![installation, account, campaign],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let window: Option<(String, String)> = tx
            .query_row(
                "SELECT consumed_usd,source_digest FROM campaign_entry_window_proofs
             WHERE installation_id=?1 AND account_key=?2
               AND campaign_id=?3 AND covered_through_ms=?4",
                params![installation, account, campaign, through_ms],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if !matches!(totals,Some((f,t,ref money))
            if f==from_ms && t==through_ms
                && decimal(money.clone()).ok()==Some(amount))
            || !matches!(window,Some((ref money,ref saved))
                if saved==digest && decimal(money.clone()).ok()==Some(amount))
        {
            return Err(StoreError::Invalid(BAD));
        }
        let operation = format!("campaign_entry_opening:{campaign}:{digest}");
        let receipt: Option<(String, String, String, i64)> = tx
            .query_row(
                "SELECT kind,request_hash,result_ref,committed_at FROM operation_receipts
             WHERE installation_id=?1 AND account_key=?2 AND operation_id=?3",
                params![installation, account, operation],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        if !matches!(receipt,Some((ref kind,ref hash,ref result,t))
            if kind=="CAMPAIGN_ENTRY_OPENING_SEALED" && hash==digest
                && result==digest && t==through_ms)
        {
            return Err(StoreError::Invalid(BAD));
        }
        let latest: i64 = tx.query_row(
            "SELECT COALESCE(MAX(event_seq),0) FROM campaign_receipt_events
             WHERE installation_id=?1 AND account_key=?2",
            params![installation, account],
            |r| r.get(0),
        )?;
        let cursor: Option<i64> = tx
            .query_row(
                "SELECT reviewed_event_seq FROM campaign_receipt_cursors
             WHERE installation_id=?1 AND account_key=?2 AND campaign_id=?3",
                params![installation, account, campaign],
                |r| r.get(0),
            )
            .optional()?;
        if cursor != Some(latest) {
            return Err(StoreError::Invalid(BAD));
        }

        // No unaccounted pending orders, risk receipts or original open lots.
        let pending: i64 = tx.query_row(
            "SELECT (SELECT COUNT(*) FROM intents WHERE installation_id=?1
                AND account_key=?2 AND state NOT IN('REJECTED','CANCELLED','FILLED'))
             + (SELECT COUNT(*) FROM risk_receipts WHERE installation_id=?1
                AND account_key=?2 AND state<>'RELEASED')
             + (SELECT COUNT(*) FROM position_lots WHERE installation_id=?1
                AND account_key=?2 AND remaining_qty!='0')",
            params![installation, account],
            |r| r.get(0),
        )?;
        if pending != 0 {
            return Err(StoreError::Invalid("UNIFIED_OPENING_UNRESOLVED_EXPOSURE"));
        }
        Ok(())
    }

    /// Read-only breakdown of the campaign gate in debug test builds. This
    /// grants no readiness, advances no cursor and discloses no account keys.
    #[cfg(debug_assertions)]
    pub fn campaign_readiness_diagnostic(
        &self,
        campaign_id: &str,
        from_ms: i64,
        through_ms: i64,
    ) -> StoreResult<String> {
        let saved: Option<(i64, i64, i64, String)> = self
            .connection
            .query_row(
                "SELECT from_ms,covered_through_ms,source_revision,source_generations
                 FROM campaign_entry_readiness WHERE campaign_id=?1",
                [campaign_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        let Some((origin, covered, revision, generations)) = saved else {
            return Ok("readiness=NOT_RECONCILED".into());
        };
        let epoch = self.campaign_source_epoch()?;
        let frontiers = self.captured_complete_frontiers(from_ms, covered);
        let source = match frontiers {
            Ok(frontiers) => format!(
                "frontiers=COMPLETE,generations_match={}",
                generations == Self::campaign_source_generations(&frontiers)
            ),
            Err(reason) => format!("frontiers={reason}"),
        };
        // The saved witness can still have complete old cursors while the
        // requested admission frontier has no capture. Report that separate
        // obligation instead of explaining only the already reviewed cutoff.
        let requested = self.captured_complete_frontiers(from_ms, through_ms);
        let requested_status = match requested {
            Ok(_) => "COMPLETE".to_owned(),
            Err(reason) => reason.to_string(),
        };
        let mut cursors = Vec::new();
        for (name, partition) in [
            ("fills", "SPOT"),
            ("fills", "MARGIN"),
            ("fills", "SWAP"),
            ("fills", "FUTURES"),
            ("fills", "OPTION"),
            ("bills", "account"),
        ] {
            cursors.push(match self.cursor(name, partition)? {
                Some(cursor) => format!(
                    "{name}/{partition}:lag_ms={},generation={},page_pending={},gaps={}",
                    through_ms.saturating_sub(cursor.watermark),
                    cursor.generation,
                    !cursor.cursor.is_empty(),
                    !cursor.coverage_gaps.is_empty()
                ),
                None => format!("{name}/{partition}:MISSING"),
            });
        }
        Ok(format!(
            "origin_match={},age_ms={},covered_ahead={},epoch_match={},requested_frontiers={},{},cursors=[{}]",
            origin == from_ms,
            through_ms.saturating_sub(covered),
            covered > through_ms,
            revision == epoch,
            requested_status,
            source,
            cursors.join(";")
        ))
    }

    /// Existing maintenance owner only; the fast path can advance source
    /// coverage when no economic/native fact or source generation changed.
    /// A revision change must revalidate the economic source before attesting.
    pub fn refresh_campaign_entry_readiness(
        &self,
        campaign_id: &str,
        from_ms: i64,
        through_ms: i64,
    ) -> StoreResult<()> {
        let tx = self.connection.unchecked_transaction()?;
        let outcome = CampaignCapture::legacy(&tx, &self.meta).refresh_campaign_entry_readiness(
            campaign_id,
            from_ms,
            through_ms,
        );
        if outcome.is_ok()
            || matches!(
                &outcome,
                Err(StoreError::Invalid(
                    "CAMPAIGN_DELTA_BATCH_PENDING" | "CAMPAIGN_RECEIPT_BATCH_PENDING"
                ))
            )
        {
            tx.commit()?;
        }
        outcome
    }

    /// After a fully imported zero opening, a *completed previous* trade
    /// must not make the next risk-increasing order impossible. The only
    /// permissible post-opening monetary rows are terminal native intents
    /// with exact exchange identity and (for entries) durable all-in native
    /// settlement AND central release readback. Unknown/external fills and
    /// unlinked fee bills remain fail-closed, without a hot-path archive scan.
    /// This is an indexed SQL read; RiskBook remains the owner of spent money.
    pub fn verify_captured_campaign_continuity(
        &self,
        from_ms: i64,
        through_ms: i64,
    ) -> StoreResult<()> {
        CampaignCapture::legacy(&self.connection, &self.meta)
            .verify_captured_campaign_continuity(from_ms, through_ms)
    }

    /// The native SQL owner's monotone campaign economic-event fold.
    /// Each new terminal or late cumulative revision adds only its delta,
    /// never the full amount again. A completed account-wide source witness
    /// is independently recomputed before this transaction; any mismatch
    /// rolls back event IDs, money and the applied frontier together.
    pub(crate) fn apply_captured_campaign_deltas(
        &self,
        campaign_id: &str,
        from_ms: i64,
        through_ms: i64,
        receipts: &[(String, String, Decimal, u64, i64)],
        witnessed_total: Decimal,
        opening_proof: Option<&str>,
    ) -> StoreResult<()> {
        let tx = self.connection.unchecked_transaction()?;
        let outcome = CampaignCapture::legacy(&tx, &self.meta).apply_captured_campaign_deltas(
            campaign_id,
            from_ms,
            through_ms,
            receipts,
            witnessed_total,
            opening_proof,
        );
        if outcome.is_ok()
            || matches!(
                &outcome,
                Err(StoreError::Invalid(
                    "CAMPAIGN_DELTA_BATCH_PENDING" | "CAMPAIGN_RECEIPT_BATCH_PENDING"
                ))
            )
        {
            tx.commit()?;
        }
        outcome
    }

    /// One campaign source for both empty and historical OWN native entries.
    /// Previously settled linear SWAP entries carry admission-time frozen USD,
    /// complete fill/bill provenance, and central RELEASE in this SQL owner.
    /// They are counted once per immutable settlement, not at today's FX.
    /// Foreign, unreconciled or breached historical money cannot be reset to zero.
    pub fn seal_captured_campaign_baseline(
        &self,
        campaign_id: &str,
        from_ms: i64,
        through_ms: i64,
    ) -> StoreResult<(Decimal, String)> {
        let tx = self.connection.unchecked_transaction()?;
        let outcome = CampaignCapture::legacy(&tx, &self.meta).seal_captured_campaign_baseline(
            campaign_id,
            from_ms,
            through_ms,
        );
        if outcome.is_ok()
            || matches!(
                &outcome,
                Err(StoreError::Invalid(
                    "CAMPAIGN_DELTA_BATCH_PENDING" | "CAMPAIGN_RECEIPT_BATCH_PENDING"
                ))
            )
        {
            tx.commit()?;
        }
        outcome
    }

    /// Source-backed zero opening from the EXISTING incremental account writer.
    /// A complete source cursor is required for every venue money stream;
    /// missing pages, an older campaign than the initial capture, or any
    /// potentially budget-relevant event remains pending, never zero.
    /// This permits opening even after unrelated pre-campaign activity.
    fn captured_empty_campaign_proof(
        &self,
        campaign_id: &str,
        from_ms: i64,
        through_ms: i64,
    ) -> StoreResult<String> {
        CampaignCapture::legacy(&self.connection, &self.meta).captured_empty_campaign_proof(
            campaign_id,
            from_ms,
            through_ms,
        )
    }

    /// Readback-compatible standalone zero opening receipt. The composed
    /// campaign sealer uses captured_empty_campaign_proof directly so its
    /// receipt and monetary frontier commit in one SQL transaction.
    pub fn seal_captured_empty_campaign(
        &self,
        campaign_id: &str,
        from_ms: i64,
        through_ms: i64,
    ) -> StoreResult<String> {
        let proof = self.captured_empty_campaign_proof(campaign_id, from_ms, through_ms)?;
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
            "INSERT OR IGNORE INTO operation_receipts
             (operation_id,kind,request_hash,result_ref,committed_at)
             VALUES(?1,'CAMPAIGN_ENTRY_OPENING_SEALED',?2,?2,?3)",
            params![key, proof, through_ms],
        )?;
        let saved: (String, String, String, i64) = self.connection.query_row(
            "SELECT kind,request_hash,result_ref,committed_at
             FROM operation_receipts WHERE operation_id=?1",
            [key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        if saved
            != (
                "CAMPAIGN_ENTRY_OPENING_SEALED".into(),
                proof.clone(),
                proof.clone(),
                through_ms,
            )
        {
            return Err(StoreError::Invalid("ENTRY_OPENING_SQL_RECEIPT_CONFLICT"));
        }
        Ok(proof)
    }

    /// Read bounded native fill evidence for one persisted risk-increasing
    /// intent. The source coverage cursor is mandatory: a partial page is
    /// NOT an absence-of-fills proof. This does not price margin, non-USDT
    /// fees, borrowing or FX and cannot authorize a reserve or venue POST.
    pub fn intent_fill_cost_evidence(
        &self,
        intent_id: &str,
        through: i64,
    ) -> StoreResult<IntentFillCostEvidence> {
        use sha2::{Digest, Sha256};
        if through < self.meta.initialized_at {
            return Err(StoreError::Invalid("ENTRY_FILL_WINDOW_INVALID"));
        }
        let cursor = self
            .cursor("fills", "SWAP")?
            .ok_or(StoreError::Invalid("ENTRY_FILL_COVERAGE_UNPROVEN"))?;
        if !cursor.cursor.is_empty()
            || !cursor.coverage_gaps.is_empty()
            || cursor.generation <= 0
            || cursor.watermark < through
        {
            return Err(StoreError::Invalid("ENTRY_FILL_COVERAGE_UNPROVEN"));
        }
        let (
            plan_hash,
            instrument,
            contract_value,
            inverse,
            currency,
            increase,
            side,
            planned_quantity,
            bound_price,
        ): (
            String,
            String,
            String,
            bool,
            String,
            bool,
            String,
            String,
            String,
        ) = self
            .connection
            .query_row(
                "SELECT i.plan_hash,i.instrument_id,p.contract_value,p.inverse,
                        p.settlement_currency,i.risk_increase,p.side,p.quantity,p.limit_price
                 FROM intents i JOIN intent_plans p ON p.intent_id=i.intent_id
                 WHERE i.intent_id=?1",
                [intent_id],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                        r.get(7)?,
                        r.get(8)?,
                    ))
                },
            )
            .optional()?
            .ok_or(StoreError::Invalid("ENTRY_FILL_INTENT_NOT_FOUND"))?;
        // Addressable PK-backed read of the only authorized SQL order and
        // central receipt, separate from the immutable contract mechanics.
        let (exchange_order_id, begin_post_at, reservation_key): (
            Option<String>,
            Option<i64>,
            Option<String>,
        ) = self.connection.query_row(
            "SELECT i.exchange_order_id,i.begin_post_at,r.reservation_key
                 FROM intents i LEFT JOIN risk_receipts r
                  ON r.intent_id=i.intent_id AND r.plan_hash=i.plan_hash
                   AND r.account_key=?2 AND r.installation_id=?3
                 WHERE i.intent_id=?1",
            params![intent_id, self.meta.account_key, self.meta.installation_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        if !increase || inverse || !instrument.ends_with("-USDT-SWAP") || currency != "USDT" {
            return Err(StoreError::Invalid("ENTRY_FILL_INSTRUMENT_UNSUPPORTED"));
        }
        // A fill row's intent_id is attribution, not proof that the exchange
        // actually accepted *this* order. Resolve the durable POST fence,
        // central reservation and exact order ID before monetary evaluation.
        let (Some(exchange_order_id), Some(posted_at), Some(reservation_key)) =
            (exchange_order_id, begin_post_at, reservation_key)
        else {
            return Err(StoreError::Invalid("ENTRY_FILL_ORDER_UNVERIFIED"));
        };
        if exchange_order_id.is_empty() || reservation_key.is_empty() || posted_at <= 0 {
            return Err(StoreError::Invalid("ENTRY_FILL_ORDER_UNVERIFIED"));
        }
        let ct = decimal(contract_value.clone())?;
        let planned_qty = decimal(planned_quantity.clone())?;
        let price_bound = decimal(bound_price.clone())?;
        if !matches!(side.as_str(), "buy" | "sell")
            || planned_qty <= Decimal::ZERO
            || price_bound <= Decimal::ZERO
        {
            return Err(StoreError::Invalid("ENTRY_FILL_PLAN_INVALID"));
        }
        if ct <= Decimal::ZERO {
            return Err(StoreError::Invalid("ENTRY_FILL_CONTRACT_INVALID"));
        }
        // A cursor watermark or private generation can advance after an
        // immutable venue event. Check source coverage above on EVERY read,
        // but hash only the chosen covered-through cut-off and economic facts.
        // Otherwise a lost Worker ACK changes digest on ordinary recapture.
        let mut hasher = Sha256::new();
        let header = serde_json::to_vec(&(
            "okx.intent-fill-cost-evidence/v1",
            self.meta.account_key.as_str(),
            self.meta.installation_id.as_str(),
            intent_id,
            plan_hash.as_str(),
            instrument.as_str(),
            contract_value.as_str(),
            side.as_str(),
            planned_quantity.as_str(),
            bound_price.as_str(),
            exchange_order_id.as_str(),
            reservation_key.as_str(),
            posted_at,
            through,
        ))
        .map_err(|_| StoreError::Invalid("ENTRY_FILL_DIGEST_INVALID"))?;
        hasher.update(&header);
        let mut statement = self.connection.prepare(
            "SELECT fill_id,instrument_id,trade_id,exchange_order_id,time,qty,price,fee,currency,side
             FROM fills WHERE intent_id=?1 AND time<=?2 ORDER BY fill_id LIMIT 10001",
        )?;
        let mut rows = statement.query(params![intent_id, through])?;
        let mut count = 0u64;
        let mut notional = Decimal::ZERO;
        let mut fees = Decimal::ZERO;
        let mut filled_qty = Decimal::ZERO;
        while let Some(row) = rows.next()? {
            count += 1;
            if count > 10000 {
                return Err(StoreError::Invalid("ENTRY_FILL_HISTORY_TOO_LARGE"));
            }
            let source: (
                String,
                String,
                String,
                String,
                i64,
                String,
                String,
                String,
                String,
                String,
            ) = (
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
                row.get(9)?,
            );
            if source.1 != instrument
                || source.4 < self.meta.initialized_at
                || source.4 < posted_at
                || source.8 != "USDT"
                || source.9 != side
                || source.0.is_empty()
                || source.2.is_empty()
                || source.3.is_empty()
            {
                return Err(StoreError::Invalid("ENTRY_FILL_SOURCE_INVALID"));
            }
            if source.3 != exchange_order_id {
                return Err(StoreError::Invalid("ENTRY_FILL_EXCHANGE_ORDER_MISMATCH"));
            }
            let qty = decimal(source.5.clone())?;
            let price = decimal(source.6.clone())?;
            let fee = decimal(source.7.clone())?;
            if qty <= Decimal::ZERO || price <= Decimal::ZERO {
                return Err(StoreError::Invalid("ENTRY_FILL_SOURCE_INVALID"));
            }
            // A fill attributed to an intent is not enough: prove that it
            // could have resulted from the exact persisted bounded order.
            if (side == "buy" && price > price_bound) || (side == "sell" && price < price_bound) {
                return Err(StoreError::Invalid("ENTRY_FILL_PRICE_BOUND_VIOLATED"));
            }
            filled_qty = filled_qty
                .checked_add(qty)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            if filled_qty > planned_qty {
                return Err(StoreError::Invalid("ENTRY_FILL_QUANTITY_EXCEEDED"));
            }
            let gross = qty
                .checked_mul(ct)
                .and_then(|v| v.checked_mul(price))
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            notional = notional
                .checked_add(gross)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            fees = fees
                .checked_add(entry_fee_debit(fee))
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            // Length-delimited immutable venue row; independent of ingestion order.
            let bytes = serde_json::to_vec(&source)
                .map_err(|_| StoreError::Invalid("ENTRY_FILL_DIGEST_INVALID"))?;
            hasher.update((bytes.len() as u64).to_le_bytes());
            hasher.update(bytes);
        }
        if count == 0 {
            return Err(StoreError::Invalid("ENTRY_FILL_EVIDENCE_EMPTY"));
        }
        Ok(IntentFillCostEvidence {
            intent_id: intent_id.to_owned(),
            plan_hash,
            exchange_order_id,
            reservation_key,
            fill_count: count,
            filled_quantity: filled_qty,
            covered_through: through,
            gross_notional_usdt: notional,
            conservative_fee_usdt: fees,
            source_digest: format!("{:x}", hasher.finalize()),
        })
    }

    /// Extra order-attested debit bills are distinct from the fill's first
    /// canonical fee. Every linked invoice has independent source identity;
    /// a newer bill never rewrites a previous receipt or returns capacity.
    /// The existing account writer reads bounded PK-prefix rows, no archives.
    pub fn intent_additional_entry_fee_evidence(
        &self,
        id: &str,
        through: i64,
    ) -> StoreResult<(Decimal, String)> {
        use sha2::{Digest, Sha256};
        let covered = self
            .cursor("bills", "account")?
            .ok_or(StoreError::Invalid("ENTRY_EXTRA_BILLS_UNCOVERED"))?;
        if covered.watermark < through
            || !covered.cursor.is_empty()
            || !covered.coverage_gaps.is_empty()
            || covered.generation <= 0
        {
            return Err(StoreError::Invalid("ENTRY_EXTRA_BILLS_UNCOVERED"));
        }
        let intent = self
            .intent(id)?
            .ok_or(StoreError::Invalid("ENTRY_BILL_INTENT_MISSING"))?;
        let prefix = format!("entry-bill:{id}:");
        let bound = format!("{prefix}~");
        let mut q = self.connection.prepare(
            "SELECT operation_id,kind,request_hash,result_ref,committed_at
             FROM operation_receipts WHERE operation_id>=?1 AND operation_id<?2
             ORDER BY operation_id LIMIT 10001",
        )?;
        let mut rows = q.query(params![prefix, bound])?;
        let mut extras = Decimal::ZERO;
        let mut digest = Sha256::new();
        digest.update(b"okx.intent-extra-entry-fee/v1");
        digest.update(id.as_bytes());
        digest.update(through.to_le_bytes());
        let mut count = 0_u64;
        while let Some(row) = rows.next()? {
            count += 1;
            if count > 10000 {
                return Err(StoreError::Invalid("ENTRY_EXTRA_BILLS_TOO_MANY"));
            }
            let (key, kind, hash, encoded, at): (String, String, String, String, i64) = (
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            );
            let link: EntryOrderFeeLink = serde_json::from_str(&encoded)
                .map_err(|_| StoreError::Invalid("ENTRY_EXTRA_BILL_RECEIPT_CORRUPT"))?;
            if kind != "ENTRY_ORDER_FEE_PROVEN"
                || key != format!("entry-bill:{id}:{}", link.bill_id)
                || hash != intent.plan_hash
                || link.intent_id != id
                || link.instrument_id != intent.instrument_id
                || Some(link.exchange_order_id.as_str()) != intent.exchange_order_id.as_deref()
                || link.bill_at != at
                || link.fee_usdt >= Decimal::ZERO
            {
                return Err(StoreError::Invalid("ENTRY_EXTRA_BILL_IDENTITY_MISMATCH"));
            }
            if at > through {
                continue;
            }
            let source: Option<(i64, String, String, String, bool)> = self
                .connection
                .query_row(
                    "SELECT b.time,b.currency,e.amount,e.entry_id,
                    EXISTS(SELECT 1 FROM finance_source_links lf
                        JOIN fills f ON lf.source_ref='fill:'||f.fill_id
                        WHERE lf.entry_id=e.entry_id AND f.intent_id=?2)
                 FROM bills b JOIN finance_entries e
                    ON e.economic_event_key=b.economic_event_key
                       AND e.component='FEE' AND e.allocation_leg='account'
                 JOIN finance_source_links lb ON lb.entry_id=e.entry_id
                       AND lb.source_ref='bill:'||b.bill_id
                 WHERE b.bill_id=?1",
                    params![link.bill_id, id],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .optional()?;
            let (bill_at, ccy, charge, event_id, first_fee) =
                source.ok_or(StoreError::Invalid("ENTRY_EXTRA_BILL_SOURCE_UNVERIFIED"))?;
            if bill_at != link.bill_at || ccy != "USDT" || decimal(charge.clone())? != link.fee_usdt
            {
                return Err(StoreError::Invalid("ENTRY_EXTRA_BILL_AMOUNT_CHANGED"));
            }
            if !first_fee {
                extras = extras
                    .checked_add(entry_fee_debit(link.fee_usdt))
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
                let bytes = serde_json::to_vec(&(link, bill_at, ccy, charge, event_id))
                    .map_err(|_| StoreError::Invalid("ENTRY_EXTRA_BILL_DIGEST_FAILED"))?;
                digest.update((bytes.len() as u64).to_le_bytes());
                digest.update(bytes);
            }
        }
        Ok((
            extras,
            if extras > Decimal::ZERO {
                format!("{:x}", digest.finalize())
            } else {
                String::new()
            },
        ))
    }

    /// Read-only F1/E1 source cross-check. Native fill fee and bill fee must
    /// resolve to the SAME existing finance event; the account bill stream
    /// must be covered through the requested time. Missing bill linkage is
    /// UNKNOWN, not a made-up zero fee or release permission. This is run on
    /// the sole SQL owner, off the NAV hot path.
    pub fn intent_fee_bill_evidence(
        &self,
        intent_id: &str,
        through: i64,
    ) -> StoreResult<IntentFeeBillEvidence> {
        use sha2::{Digest, Sha256};
        #[derive(serde::Serialize)]
        struct FeeBillSource {
            entry_id: String,
            economic_event_key: String,
            amount: String,
            currency: String,
            allocation_leg: String,
            finance_time: i64,
            reversal: Option<String>,
            bill_id: Option<String>,
            bill_time: Option<i64>,
            bill_kind: Option<String>,
            bill_currency: Option<String>,
            bill_linked: bool,
            // Preserve the canonical digest of legacy authenticated
            // fee receipts that predate raw bill order provenance capture.
            #[serde(skip_serializing_if = "Option::is_none")]
            bill_order_id: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            bill_instrument_id: Option<String>,
        }
        let fill = self.intent_fill_cost_evidence(intent_id, through)?;
        let intent_instrument = self
            .intent(intent_id)?
            .ok_or(StoreError::Invalid("ENTRY_BILL_INTENT_MISSING"))?
            .instrument_id;
        if !finance_proof_sealed(&self.connection)? {
            return Err(StoreError::Invalid("ENTRY_FINANCE_PROVENANCE_UNSEALED"));
        }
        let bills = self
            .cursor("bills", "account")?
            .ok_or(StoreError::Invalid("ENTRY_BILLS_COVERAGE_UNPROVEN"))?;
        if !bills.cursor.is_empty()
            || !bills.coverage_gaps.is_empty()
            || bills.generation <= 0
            || bills.watermark < through
        {
            return Err(StoreError::Invalid("ENTRY_BILLS_COVERAGE_UNPROVEN"));
        }
        // A later source generation/watermark cannot change the source
        // digest of the same fixed through-window and immutable fee bills.
        let mut hasher = Sha256::new();
        hasher.update(
            serde_json::to_vec(&(
                "okx.intent-fee-bill-evidence/v1",
                self.meta.account_key.as_str(),
                self.meta.installation_id.as_str(),
                intent_id,
                fill.source_digest.as_str(),
                through,
            ))
            .map_err(|_| StoreError::Invalid("ENTRY_FEE_BILL_DIGEST_INVALID"))?,
        );
        let mut fee_rows = self.connection.prepare(
            "SELECT fill_id,fee,currency,time FROM fills
             WHERE intent_id=?1 AND time<=?2 ORDER BY fill_id LIMIT 10001",
        )?;
        // Source-ref first index makes each immutable fee lookup addressable.
        // The canonical finance entry may have first arrived from bill history
        // or from fills. Its source_links must include BOTH original sources.
        let mut linked = self.connection.prepare(
            "SELECT e.entry_id,e.economic_event_key,e.amount,e.currency,
                    e.allocation_leg,e.time,e.reverses_entry_id,b.bill_id,b.time,b.kind,b.currency,
                    EXISTS(SELECT 1 FROM finance_source_links lb
                      WHERE lb.entry_id=e.entry_id
                        AND lb.source_ref='bill:'||b.bill_id),
                    origin.exchange_order_id,origin.instrument_id
             FROM finance_source_links fl
             JOIN finance_entries e ON e.entry_id=fl.entry_id AND e.component='FEE'
             LEFT JOIN bills b ON b.economic_event_key=e.economic_event_key
                              AND e.economic_event_key='bill:'||b.bill_id
             LEFT JOIN bill_order_sources origin ON origin.bill_id=b.bill_id
             WHERE fl.source_ref=?1 ORDER BY e.entry_id LIMIT 2",
        )?;
        let mut scanned = fee_rows.query(params![intent_id, through])?;
        let mut count = 0_u64;
        let mut total = Decimal::ZERO;
        while let Some(row) = scanned.next()? {
            count += 1;
            if count > 10000 {
                return Err(StoreError::Invalid("ENTRY_FILL_HISTORY_TOO_LARGE"));
            }
            let id: String = row.get(0)?;
            let fee: String = row.get(1)?;
            let currency: String = row.get(2)?;
            let fill_time: i64 = row.get(3)?;
            let fee = decimal(fee)?;
            if fee == Decimal::ZERO {
                // No finance fee event exists for an actual zero-fee fill.
                // A zero amount in fills alone does NOT prove the final bill.
                return Err(StoreError::Invalid("ENTRY_ZERO_FEE_BILL_UNPROVEN"));
            }
            let fee_source = format!("fill:{id}");
            let mut rows = linked.query([fee_source.as_str()])?;
            let Some(source) = rows.next()? else {
                return Err(StoreError::Invalid("ENTRY_FEE_BILL_LINK_MISSING"));
            };
            let proof = FeeBillSource {
                entry_id: source.get(0)?,
                economic_event_key: source.get(1)?,
                amount: source.get(2)?,
                currency: source.get(3)?,
                allocation_leg: source.get(4)?,
                finance_time: source.get(5)?,
                reversal: source.get(6)?,
                bill_id: source.get(7)?,
                bill_time: source.get(8)?,
                bill_kind: source.get(9)?,
                bill_currency: source.get(10)?,
                bill_linked: source.get(11)?,
                bill_order_id: source.get(12)?,
                bill_instrument_id: source.get(13)?,
            };
            if rows.next()?.is_some() {
                return Err(StoreError::Invalid("ENTRY_FEE_BILL_LINK_AMBIGUOUS"));
            }
            // A fill without billId can acquire a fee link from bill-history
            // later. If another matching bill arrives AFTER the first link,
            // checking just finance_source_links would falsely retain success.
            // Use the original per-account SQL facts, not a second ledger.
            let late_swap: bool = self.connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM swap_fill_sources
                 WHERE fill_id=?1 AND bill_id='')",
                [&id],
                |r| r.get(0),
            )?;
            if late_swap {
                let mut candidates = self.connection.prepare(
                    "SELECT b.bill_id,e.amount,e.currency,b.time
                     FROM fills f
                     JOIN bill_order_sources o
                       ON o.instrument_id=f.instrument_id
                      AND o.exchange_order_id=f.exchange_order_id
                      AND o.trade_id=f.trade_id
                     JOIN bills b ON b.bill_id=o.bill_id
                     JOIN finance_entries e ON e.economic_event_key=b.economic_event_key
                       AND e.component='FEE' AND e.allocation_leg='account'
                     WHERE f.fill_id=?1 AND b.time<=?2
                     ORDER BY b.bill_id LIMIT 65",
                )?;
                let mut bill_rows = candidates.query(params![&id, through])?;
                let mut scanned = 0_u32;
                let mut matched: Option<String> = None;
                while let Some(bill_row) = bill_rows.next()? {
                    scanned += 1;
                    if scanned > 64 {
                        return Err(StoreError::Invalid("LATE_SWAP_FEE_BILLS_UNBOUNDED"));
                    }
                    let bill_amount: String = bill_row.get(1)?;
                    let bill_currency: String = bill_row.get(2)?;
                    let bill_time: i64 = bill_row.get(3)?;
                    if decimal(bill_amount)? != fee
                        || bill_currency != currency
                        || bill_time < fill_time
                    {
                        continue;
                    }
                    if matched.is_some() {
                        return Err(StoreError::Invalid("ENTRY_FEE_BILL_LINK_AMBIGUOUS"));
                    }
                    matched = Some(bill_row.get(0)?);
                }
                if matched.as_deref() != proof.bill_id.as_deref() {
                    return Err(StoreError::Invalid("ENTRY_FEE_BILL_SOURCE_MISMATCH"));
                }
            }
            // A linked fee still cannot settle an entry if the bill
            // predates the fill, the finance event is outside the
            // claimed covered window, or the link belongs to a non-account
            // allocation. Do not equate a source-link match with money proof.
            if proof.reversal.is_some()
                || proof.allocation_leg != "account"
                || fill_time <= 0
                || proof.finance_time < fill_time
                || proof.finance_time > through
                || !proof.economic_event_key.starts_with("bill:")
                || proof.bill_id.as_deref() != proof.economic_event_key.strip_prefix("bill:")
                || proof
                    .bill_time
                    .is_none_or(|t| t < fill_time || t > through || t > bills.watermark)
                || proof.bill_kind.as_deref().is_none_or(|kind| {
                    kind.is_empty()
                        || kind.starts_with("7:")
                        || kind == "1:11"
                        || kind == "1:12"
                        || kind.ends_with(":173")
                        || kind.ends_with(":174")
                })
                || proof.bill_currency.as_deref() != Some(currency.as_str())
                || !proof.bill_linked
                || proof
                    .bill_order_id
                    .as_deref()
                    .is_some_and(|v| v != fill.exchange_order_id.as_str())
                || proof
                    .bill_instrument_id
                    .as_deref()
                    .is_some_and(|v| v != intent_instrument.as_str())
                || proof.currency != currency
                || decimal(proof.amount.clone())? != fee
            {
                return Err(StoreError::Invalid("ENTRY_FEE_BILL_SOURCE_MISMATCH"));
            }
            total = total
                .checked_add(entry_fee_debit(fee))
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            let data = serde_json::to_vec(&(id, fee_source, currency, fill_time, proof))
                .map_err(|_| StoreError::Invalid("ENTRY_FEE_BILL_DIGEST_INVALID"))?;
            hasher.update((data.len() as u64).to_le_bytes());
            hasher.update(data);
        }
        if count != fill.fill_count || count == 0 || total != fill.conservative_fee_usdt {
            return Err(StoreError::Invalid("ENTRY_FEE_BILL_TOTAL_MISMATCH"));
        }
        Ok(IntentFeeBillEvidence {
            fill,
            linked_bill_count: count,
            bills_covered_through: bills.watermark,
            conservative_verified_fee_usdt: total,
            source_digest: format!("{:x}", hasher.finalize()),
        })
    }

    /// Account-backed principal, never PnL or independently priced strategy NAV.
    /// All returned balances are exact USD decimals within this AccountStore.
    pub fn capital_state(&self, now: i64) -> StoreResult<AccountCapitalState> {
        let cached_assignment = *self.latest_capital_assignment_time.borrow();
        let latest_assignment = if let Some(cached) = cached_assignment {
            cached
        } else {
            let loaded = Self::latest_capital_assignment_time_sql(&self.connection, None)?;
            *self.latest_capital_assignment_time.borrow_mut() = Some(loaded);
            loaded
        };
        Self::capital_state_sql(&self.connection, None, now, latest_assignment)
    }

    /// One-time existing-v2 migration fence. No second database or schema
    /// version: audit immutable historical evidence on open, never in NAV.
    /// Invalid old rows leave the proof unsealed and risk fails closed.
    pub(super) fn audit_existing_finance_provenance(db: &mut Connection) -> StoreResult<()> {
        if finance_proof_sealed(db)? {
            return Ok(());
        }
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let last = finance_rowid(&tx)?;
        {
            let mut stmt = tx.prepare(
                "SELECT entry_id,economic_event_key,component,allocation_leg,amount,currency,instance_id,source_ref,time,reverses_entry_id
                 FROM finance_entries
                 WHERE instance_id IS NOT NULL AND instance_id<>'manual'
                 ORDER BY rowid",
            )?;
            let mut rows = stmt.query([])?;
            while let Some(r) = rows.next()? {
                let entry = FinanceEntry {
                    entry_id: r.get(0)?,
                    economic_event_key: r.get(1)?,
                    component: r.get(2)?,
                    allocation_leg: r.get(3)?,
                    amount: decimal(r.get(4)?)?,
                    currency: r.get(5)?,
                    instance_id: r.get(6)?,
                    source_ref: r.get(7)?,
                    time: r.get(8)?,
                    reverses_entry_id: r.get(9)?,
                };
                Self::verify_strategy_finance_source_sql(&tx, None, &entry)?;
            }
        }
        tx.execute(
            "INSERT INTO source_cursors(source,stream_partition,cursor,watermark,generation,coverage_gaps)
             VALUES('finance_provenance','account','',?1,0,'')
             ON CONFLICT(source,stream_partition)
             DO UPDATE SET cursor='',watermark=excluded.watermark,generation=0,coverage_gaps=''",
            [last],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Verify and price known strategy capital without creating a NAV point or
    /// unlocking risk. Only a converged exchange snapshot and five complete
    /// post-T0 history streams may supply a COMPLETE calculation.
    pub fn reconcile_strategy_valuation(
        &self,
        evidence: &StrategyValuationEvidence,
    ) -> StoreResult<StrategyValuationReconciliation> {
        Self::reconcile_strategy_valuation_sql(&self.connection, None, evidence)
    }

    /// Valuation needs a stronger fence than finance-history pagination:
    /// a zero-fee fill or a venue bill can change economic exposure without
    /// appending a finance entry. Only immutable source tables participate;
    /// risk-budget allocations and valuation/readback rows do not.
    pub fn strategy_valuation_revision(&self) -> StoreResult<String> {
        self.strategy_valuation_revision_as_of(i64::MAX)
    }

    /// Late financial events invalidate a historical period only when their
    /// exchange timestamp belongs to that period. Future market moves must
    /// not erase earlier, already verified financial reports.
    pub fn strategy_valuation_revision_as_of(&self, through: i64) -> StoreResult<String> {
        Self::strategy_valuation_revision_sql(&self.connection, None, through)
    }

    /// Atomically publish only a previously source-verified estimate to the
    /// existing NAV points and immutable operation receipts. A matching finance
    /// revision fences replay after a late fill, fee, bill or correction.
    pub fn commit_reconciled_strategy_nav(
        &mut self,
        evidence: &StrategyValuationEvidence,
    ) -> StoreResult<StrategyValuationReconciliation> {
        self.commit_reconciled_strategy_nav_with_optional_cursor(evidence, None)
    }

    /// NAV and COMPLETE source coverage become visible atomically.
    pub fn commit_reconciled_strategy_nav_with_cursor(
        &mut self,
        evidence: &StrategyValuationEvidence,
        cursor: &SourceCursor,
    ) -> StoreResult<StrategyValuationReconciliation> {
        self.commit_reconciled_strategy_nav_with_optional_cursor(evidence, Some(cursor))
    }

    fn commit_reconciled_strategy_nav_with_optional_cursor(
        &mut self,
        evidence: &StrategyValuationEvidence,
        cursor: Option<&SourceCursor>,
    ) -> StoreResult<StrategyValuationReconciliation> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let summary = Self::commit_reconciled_strategy_nav_sql(&tx, None, evidence, cursor)?;
        tx.commit()?;
        Ok(summary)
    }

    /// Atomic two-leg, account-backed book assignment. The caller is the
    /// already-authorized Agent governance owner, not an arbitrary ingestion
    /// stream. No venue transfer, risk reservation, NAV point or highwater
    /// is created here. This cannot authorize new trading risk.
    pub fn commit_authorized_capital_assignment(
        &mut self,
        request: &AuthorizedCapitalAssignment,
    ) -> StoreResult<OperationReceipt> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let receipt = Self::commit_authorized_capital_assignment_sql(&tx, None, request)?;
        tx.commit()?;
        *self.latest_capital_assignment_time.borrow_mut() = None;
        Ok(receipt)
    }

    pub fn nav_summary(
        &self,
        from: i64,
        through: i64,
        instances: &[String],
        currencies: &[String],
    ) -> StoreResult<Vec<NavWindow>> {
        Self::nav_summary_sql(&self.connection, None, from, through, instances, currencies)
    }
    pub fn opening_equity_usd(&self) -> StoreResult<Decimal> {
        Self::opening_equity_usd_sql(&self.connection, None)
    }
    pub fn usd_component_total(&self, component: &str) -> StoreResult<Decimal> {
        Self::usd_component_total_sql(&self.connection, None, component)
    }
    pub fn financial_risk(&self, now: i64, instance: Option<&str>) -> StoreResult<FinancialRisk> {
        Self::financial_risk_sql(&self.connection, None, now, instance)
    }
    pub fn attribute_pending_funding(
        &mut self,
        fx: &UsdConversionEvidence,
        limit: usize,
    ) -> StoreResult<usize> {
        let rows = Self::pending_funding_sources_sql(&self.connection, None, limit)?;
        let count = rows.len();
        for event in rows {
            let projection = Self::project_funding_source_sql(&self.connection, None, &event)?;
            self.ingest_page_with_usd(&projection.page, fx)?;
            if projection.unknown {
                self.latch_stop(
                    "funding-attribution-incomplete",
                    "account",
                    "FUNDING_ATTRIBUTION_INCOMPLETE",
                    event.time,
                )?;
            }
        }
        Ok(count)
    }
    pub fn ingest_page_with_usd(
        &mut self,
        page: &IngestPage,
        fx: &UsdConversionEvidence,
    ) -> StoreResult<()> {
        if fx.source_digest.is_empty() || fx.observed_at <= 0 {
            return Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE"));
        }
        let normalized = match Self::normalize_source_page_usd_sql(&self.connection, None, page, fx)
        {
            Ok(page) => page,
            Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE")) => {
                self.latch_stop(
                    "usd-conversion-unavailable",
                    "account",
                    "USD_CONVERSION_SOURCE_UNAVAILABLE",
                    fx.observed_at,
                )?;
                return Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE"));
            }
            Err(error) => return Err(error),
        };
        self.ingest_page(&normalized)
    }
    #[allow(clippy::too_many_arguments)] // Public bounded pagination/filter facade.
    pub fn finance_history_filtered(
        &self,
        from: i64,
        through: i64,
        after_time: i64,
        after_id: &str,
        limit: usize,
        instances: &[String],
        currencies: &[String],
    ) -> StoreResult<Vec<FinanceEntry>> {
        Self::finance_history_filtered_sql(
            &self.connection,
            None,
            from,
            through,
            after_time,
            after_id,
            limit,
            instances,
            currencies,
        )
    }
    /// Cheap monotonic revision for the append-only, immutable finance log.
    /// Includes late event-time arrivals. MAX(rowid) uses the table btree,
    /// avoiding a full historical scan on every history page.
    pub fn finance_revision(&self) -> StoreResult<i64> {
        Ok(self.connection.query_row(
            "SELECT COALESCE(MAX(rowid),0) FROM finance_entries",
            [],
            |r| r.get(0),
        )?)
    }

    /// Exact per-instance economic facts from the canonical account ledger.
    /// None means the account's unattributed leg, not a second copy of money.
    pub fn finance_attribution_filtered(
        &self,
        from: i64,
        through: i64,
        instances: &[String],
        currencies: &[String],
    ) -> StoreResult<FinanceAttribution> {
        Self::finance_attribution_filtered_sql(
            &self.connection,
            None,
            from,
            through,
            instances,
            currencies,
        )
    }

    pub fn finance_totals_filtered(
        &self,
        from: i64,
        through: i64,
        instances: &[String],
        currencies: &[String],
    ) -> StoreResult<std::collections::BTreeMap<(String, String), Decimal>> {
        let (filter, mut args) = finance_filter(instances, currencies, 2)?;
        let mut values = vec![from.into(), through.into()];
        values.append(&mut args);
        let query = format!(
            "SELECT currency,component,amount FROM finance_entries WHERE time>=?1 AND time<=?2{filter} ORDER BY time,entry_id"
        );
        let mut s = self.connection.prepare(&query)?;
        let mut rows = s.query(rusqlite::params_from_iter(values))?;
        let mut totals = std::collections::BTreeMap::new();
        while let Some(row) = rows.next()? {
            let key = (row.get::<_, String>(0)?, row.get::<_, String>(1)?);
            let amount = decimal(row.get(2)?)?;
            let old = *totals.get(&key).unwrap_or(&Decimal::ZERO);
            totals.insert(
                key,
                old.checked_add(amount)
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
            );
        }
        Ok(totals)
    }
    pub fn cash_flow_totals(
        &self,
    ) -> StoreResult<std::collections::BTreeMap<(String, String), Decimal>> {
        self.cash_flow_totals_observed(|_| {})
    }

    pub fn cash_flow_totals_observed(
        &self,
        observe: impl FnOnce(AccountSqlStatementStat),
    ) -> StoreResult<std::collections::BTreeMap<(String, String), Decimal>> {
        if let Some(cached) = self.cash_flow_totals_cache.borrow().clone() {
            return Ok(cached);
        }
        let result = Self::cash_flow_totals_sql(&self.connection, None, observe);
        let totals = result?;
        *self.cash_flow_totals_cache.borrow_mut() = Some(totals.clone());
        Ok(totals)
    }
    pub fn project_pending_fills(&mut self, limit: usize) -> StoreResult<usize> {
        self.project_pending_fills_observed(limit, |_| {})
    }

    /// The measured SELECT is bounded and distinct from the projection loop.
    pub fn project_pending_fills_observed(
        &mut self,
        limit: usize,
        observe: impl FnOnce(AccountSqlStatementStat),
    ) -> StoreResult<usize> {
        let rows = Self::pending_fill_sources_sql(&self.connection, None, limit, observe)?;
        let count = rows.len();
        for fill in rows {
            let effect = Self::pending_fill_effect_sql(&self.connection, None, &fill)?;
            self.ingest_attributed_fill(fill, effect, None)?;
        }
        Ok(count)
    }
    pub fn ingest_attributed_fill(
        &mut self,
        fill: Fill,
        effect: FillEffect,
        cursor: Option<SourceCursor>,
    ) -> StoreResult<()> {
        let page = Self::project_attributed_fill_sql(&self.connection, None, fill, effect, cursor)?;
        self.ingest_page(&page)
    }

    pub fn finance_totals(
        &self,
        from: i64,
        through: i64,
        instance: Option<&str>,
    ) -> StoreResult<std::collections::BTreeMap<(String, String), Decimal>> {
        let mut statement=self.connection.prepare("SELECT currency,component,amount FROM finance_entries WHERE time>=?1 AND time<=?2 AND (?3 IS NULL OR instance_id=?3) ORDER BY time,entry_id")?;
        let mut rows = statement.query(params![from, through, instance])?;
        let mut totals = std::collections::BTreeMap::new();
        while let Some(row) = rows.next()? {
            let key = (row.get::<_, String>(0)?, row.get::<_, String>(1)?);
            let amount = decimal(row.get(2)?)?;
            let old = *totals.get(&key).unwrap_or(&Decimal::ZERO);
            totals.insert(
                key,
                old.checked_add(amount)
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
            );
        }
        Ok(totals)
    }
    pub fn ingest_page(&mut self, page: &IngestPage) -> StoreResult<()> {
        self.ingest_page_observed(page, |_| {})
    }

    /// The observer is diagnostic-only and runs on the existing SQL owner.
    /// No transaction, cursor, durability, or finance semantics change.
    pub fn ingest_page_observed(
        &mut self,
        page: &IngestPage,
        observe: impl FnOnce(IngestTransactionTiming),
    ) -> StoreResult<()> {
        let begin_started = std::time::Instant::now();
        let tx_result = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate);
        let begin_us = u64::try_from(begin_started.elapsed().as_micros()).unwrap_or(u64::MAX);
        let tx = match tx_result {
            Ok(tx) => tx,
            Err(error) => {
                observe(IngestTransactionTiming {
                    begin_us,
                    body_us: 0,
                    commit_us: 0,
                    commit_succeeded: false,
                });
                return Err(error.into());
            }
        };
        let mut timing = IngestTimingGuard {
            observe: Some(observe),
            begin_us,
            body_started: std::time::Instant::now(),
            body_us: None,
            commit_us: 0,
            commit_succeeded: false,
        };
        if page
            .cursor
            .as_ref()
            .is_some_and(|c| c.source == "finance_provenance")
        {
            return Err(StoreError::Invalid("FINANCE_PROVENANCE_CURSOR_INTERNAL"));
        }
        let finance_rowid_before = finance_rowid(&tx)?;
        Self::record_source_fills_sql(&tx, None, &page.fills)?;
        Self::record_swap_sources_sql(&tx, None, &page.swap_fill_sources)?;
        Self::record_source_bills_sql(&tx, None, &page.bills, &page.bill_order_sources)?;
        Self::record_funding_sources_sql(&tx, None, &page.funding_events)?;
        let cash_flows_changed =
            Self::record_source_flows_sql(&tx, None, &page.cash_flows, &page.allocations)?;
        let added_loss = Self::record_finance_entries_sql(&tx, None, &page.finance)?;
        Self::link_page_swap_fees_sql(&tx, None, page)?;
        if added_loss > Decimal::ZERO {
            let old: String =
                tx.query_row("SELECT campaign_loss FROM account_policy", [], |r| r.get(0))?;
            let total = decimal(old)?
                .checked_add(added_loss)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            tx.execute("UPDATE account_policy SET campaign_loss=?1", [exact(total)])?;
        }
        Self::record_entry_fee_links_sql(&tx, None, &page.entry_order_fee_links)?;
        Self::record_position_lots_sql(&tx, None, &page.lots)?;
        Self::record_nav_points_sql(&tx, None, &page.nav)?;
        if let Some(c) = &page.cursor {
            upsert_source_cursor(&tx, c)?;
        }
        Self::acknowledge_projected_fills_sql(&tx, None, &page.projected_fill_ids)?;
        Self::acknowledge_funding_sources_sql(&tx, None, &page.attributed_funding_ids)?;
        advance_finance_provenance(&tx, finance_rowid_before)?;
        timing.body_us =
            Some(u64::try_from(timing.body_started.elapsed().as_micros()).unwrap_or(u64::MAX));
        let commit_started = std::time::Instant::now();
        let commit_result = tx.commit();
        timing.commit_us = u64::try_from(commit_started.elapsed().as_micros()).unwrap_or(u64::MAX);
        timing.commit_succeeded = commit_result.is_ok();
        let commit_ms = timing.commit_us / 1000;
        if commit_ms >= 250 || commit_result.is_err() {
            // Redacted, fixed-label operational evidence. Does not write to
            // SQLite, change the transaction boundary or weaken durability.
            eprintln!(
                "ACCOUNT_FINANCE_SQL_COMMIT_V1 phase=ingest_page elapsed_ms={} outcome={}",
                commit_ms,
                if commit_result.is_ok() { "ok" } else { "error" }
            );
        }
        commit_result?;
        // Only a newly committed fact invalidates the memory view; pure
        // immutable-source replay leaves the existing cache untouched.
        if cash_flows_changed {
            *self.cash_flow_totals_cache.borrow_mut() = None;
        }
        Ok(())
    }
    pub fn active_lots(&self, instrument: &str) -> StoreResult<Vec<StoredLot>> {
        Self::active_lots_sql(&self.connection, None, instrument)
    }

    pub fn finance_history(
        &self,
        from: i64,
        through: i64,
        after_time: i64,
        after_id: &str,
        limit: usize,
    ) -> StoreResult<Vec<FinanceEntry>> {
        let mut s=self.connection.prepare("SELECT entry_id,economic_event_key,component,allocation_leg,amount,currency,instance_id,source_ref,time,reverses_entry_id FROM finance_entries WHERE time>=?1 AND time<=?2 AND (time,entry_id)>(?3,?4) ORDER BY time,entry_id LIMIT ?5")?;
        s.query_map(
            params![from, through, after_time, after_id, limit.min(1000) as i64],
            |r| {
                Ok((
                    FinanceEntry {
                        entry_id: r.get(0)?,
                        economic_event_key: r.get(1)?,
                        component: r.get(2)?,
                        allocation_leg: r.get(3)?,
                        amount: Decimal::ZERO,
                        currency: r.get(5)?,
                        instance_id: r.get(6)?,
                        source_ref: r.get(7)?,
                        time: r.get(8)?,
                        reverses_entry_id: r.get(9)?,
                    },
                    r.get::<_, String>(4)?,
                ))
            },
        )?
        .map(|v| {
            let (mut e, a) = v?;
            e.amount = decimal(a)?;
            Ok(e)
        })
        .collect()
    }
}
pub(super) fn finance_filter(
    instances: &[String],
    currencies: &[String],
    offset: usize,
) -> StoreResult<(String, Vec<rusqlite::types::Value>)> {
    if instances.len() > 32 || currencies.len() > 32 {
        return Err(StoreError::Invalid("FINANCE_FILTER_LIMIT"));
    }
    let mut filter = String::new();
    let mut args = Vec::new();
    for (column, items) in [("instance_id", instances), ("currency", currencies)] {
        if items.is_empty() {
            continue;
        }
        let mut placeholders = Vec::new();
        for item in items {
            if item.is_empty() {
                return Err(StoreError::Invalid("FINANCE_FILTER_INVALID"));
            }
            args.push(item.clone().into());
            placeholders.push(format!("?{}", offset + args.len()));
        }
        filter.push_str(&format!(" AND {column} IN({})", placeholders.join(",")));
    }
    Ok((filter, args))
}

#[cfg(test)]
mod pending_fill_join_order_tests {
    use super::*;

    #[test]
    fn nav_pending_fill_query_reads_pending_before_immutable_history() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE fills(
                fill_id TEXT PRIMARY KEY,
                instrument_id TEXT,
                trade_id TEXT,
                exchange_order_id TEXT,
                intent_id TEXT,
                time INTEGER,
                qty TEXT,
                price TEXT,
                fee TEXT,
                currency TEXT,
                side TEXT
            );
            CREATE INDEX idx_fills_time ON fills(time,fill_id);
            CREATE TABLE fill_projection_pending(fill_id TEXT PRIMARY KEY,side TEXT);
            INSERT INTO fills VALUES
                ('old','BTC-USDT-SWAP','t1','o1',NULL,10,'1','100','0','USDT','buy'),
                ('new','BTC-USDT-SWAP','t2','o2',NULL,30,'1','101','0','USDT','sell'),
                ('middle','BTC-USDT-SWAP','t3','o3',NULL,20,'1','102','0','USDT','buy');",
        )
        .unwrap();

        // EXPLAIN validates the actual production query, including its ORDER BY.
        // Without CROSS JOIN SQLite chooses SCAN f USING idx_fills_time on an
        // empty pending set; this grows with the lifetime immutable ledger.
        let plan = db
            .prepare(&format!("EXPLAIN QUERY PLAN {PENDING_FILL_PROJECTION_SQL}"))
            .unwrap()
            .query_map([1000_i64], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            plan.iter().any(|detail| detail.starts_with("SCAN p")),
            "{plan:?}"
        );
        assert!(
            !plan.iter().any(|detail| detail.starts_with("SCAN f")),
            "{plan:?}"
        );

        let collect_ids = |limit: i64| {
            db.prepare(PENDING_FILL_PROJECTION_SQL)
                .unwrap()
                .query_map([limit], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        assert!(collect_ids(1000).is_empty());

        db.execute_batch(
            "INSERT INTO fill_projection_pending VALUES('new','sell');
             INSERT INTO fill_projection_pending VALUES('middle','buy');",
        )
        .unwrap();
        assert_eq!(collect_ids(1000), vec!["middle", "new"]);
        assert_eq!(collect_ids(1), vec!["middle"]);
    }
}

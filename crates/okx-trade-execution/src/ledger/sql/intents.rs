use super::super::*;

/// Older manual SQL commands persist NULL as the decision/receipt owner while
/// their plan is explicitly manual. Preserve that representation and its replay;
/// NULL is never an implicit owner for a named strategy's new intent or grant.
fn strategy_owner_matches_plan(owner: Option<&str>, plan_owner: &str) -> bool {
    match owner {
        Some(owner) => owner == plan_owner,
        None => plan_owner == "manual",
    }
}

// The existing state/intent index bounds candidates BEFORE identity joins.
// This is an ordered subset, not a latest-history or completeness claim.
const TERMINAL_RELEASE_DIAGNOSTIC_SQL: &str = "
WITH candidates AS MATERIALIZED (
    SELECT * FROM risk_receipts INDEXED BY idx_risk_receipts_active
    WHERE state IN ('TERMINAL_PENDING_CENTRAL','RELEASED','LATE_SETTLEMENT_PENDING')
    ORDER BY state,intent_id
    LIMIT ?1
)
SELECT i.intent_id,i.revision,i.state,r.reservation_key,r.policy_revision,r.state,
       ack.committed_at
FROM candidates AS r
JOIN intents AS i ON i.intent_id=r.intent_id
    AND i.plan_hash=r.plan_hash AND i.policy_revision=r.policy_revision
LEFT JOIN operation_receipts AS ack ON ack.operation_id='central-release:'||r.reservation_key
    AND ack.kind='CENTRAL_RELEASE' AND ack.request_hash=r.receipt_hash
    AND length(ack.result_ref)>0 AND ack.committed_at>0
WHERE r.account_key=?2 AND r.installation_id=?3
    AND length(r.reservation_key)>0 AND length(r.receipt_hash)>0
    AND i.state IN ('FILLED','CANCELLED','REJECTED')";

// Native: scope installation/account inside the indexed candidate bound.
// The historical local account path retains its original candidate semantics.
const TERMINAL_RELEASE_DIAGNOSTIC_SCOPED_SQL: &str = "
WITH candidates AS MATERIALIZED (
    SELECT * FROM risk_receipts INDEXED BY idx_risk_receipts_active
    WHERE account_key=?2 AND installation_id=?3
      AND state IN ('TERMINAL_PENDING_CENTRAL','RELEASED','LATE_SETTLEMENT_PENDING')
    ORDER BY state,intent_id
    LIMIT ?1
)
SELECT i.intent_id,i.revision,i.state,r.reservation_key,r.policy_revision,r.state,
       ack.committed_at
FROM candidates AS r
JOIN intents AS i ON i.intent_id=r.intent_id
    AND i.installation_id=r.installation_id AND i.account_key=r.account_key
    AND i.plan_hash=r.plan_hash AND i.policy_revision=r.policy_revision
LEFT JOIN operation_receipts AS ack ON ack.operation_id='central-release:'||r.reservation_key
    AND ack.installation_id=r.installation_id AND ack.account_key=r.account_key
    AND ack.kind='CENTRAL_RELEASE' AND ack.request_hash=r.receipt_hash
    AND length(ack.result_ref)>0 AND ack.committed_at>0
WHERE r.account_key=?2 AND r.installation_id=?3
    AND length(r.reservation_key)>0 AND length(r.receipt_hash)>0
    AND i.state IN ('FILLED','CANCELLED','REJECTED')";

impl AccountStore {
    /// Return at most 128 validated terminal reservation facts from a bounded
    /// state/intent ordered subset of the original ledger. Invalid scopes are
    /// skipped within that candidate budget, so fewer rows do not imply that
    /// all history has been inspected. No financial receipt is changed.
    pub fn recent_terminal_release_diagnostics(
        &self,
        limit: usize,
    ) -> StoreResult<Vec<TerminalReleaseDiagnosticReceipt>> {
        Self::terminal_release_diagnostics_query(
            &self.connection,
            (&self.meta.installation_id, &self.meta.account_key),
            limit,
            TERMINAL_RELEASE_DIAGNOSTIC_SQL,
        )
    }

    /// Read the same original bounded terminal facts under an explicit
    /// installation/account scope. This never edits or creates a receipt.
    pub(crate) fn recent_terminal_release_diagnostics_sql(
        db: &rusqlite::Connection,
        scope: (&str, &str),
        limit: usize,
    ) -> StoreResult<Vec<TerminalReleaseDiagnosticReceipt>> {
        Self::terminal_release_diagnostics_query(
            db,
            scope,
            limit,
            TERMINAL_RELEASE_DIAGNOSTIC_SCOPED_SQL,
        )
    }

    fn terminal_release_diagnostics_query(
        db: &rusqlite::Connection,
        scope: (&str, &str),
        limit: usize,
        sql: &str,
    ) -> StoreResult<Vec<TerminalReleaseDiagnosticReceipt>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        Self::verify_source_account_sql(db, scope.0, scope.1)?;
        let mut statement = db.prepare(sql)?;
        let rows =
            statement.query_map(params![limit.min(128) as i64, scope.1, scope.0], |row| {
                Ok((
                    TerminalReleaseDiagnosticReceipt {
                        intent_id: row.get(0)?,
                        intent_revision: row.get(1)?,
                        intent_state: IntentState::Prepared,
                        reservation_key: row.get(3)?,
                        policy_revision: row.get(4)?,
                        receipt_state: row.get(5)?,
                        central_release_committed_at: row.get(6)?,
                    },
                    row.get::<_, String>(2)?,
                ))
            })?;
        rows.map(|row| {
            let (mut receipt, state) = row?;
            receipt.intent_state = IntentState::parse(&state)?;
            Ok(receipt)
        })
        .collect()
    }
    /// No second ledger/table: one immutable, idempotent record in the existing
    /// operation_receipts journal, bound to the original saved intent/plan.
    /// Its kind explicitly denies any claim of ALL-IN cost authorization.
    pub fn bind_entry_quote(&mut self, quote: &EntryQuoteEvidence) -> StoreResult<()> {
        Self::bind_entry_quote_sql(&self.connection, None, quote)
    }

    pub fn entry_quote(&self, intent_id: &str) -> StoreResult<Option<EntryQuoteEvidence>> {
        Self::entry_quote_sql(&self.connection, None, intent_id)
    }
    /// Validate the independently produced native cost proof against the
    /// immutable quote on EVERY read, including after a Windows restart.
    /// The digest here is a binding to the native source witness, not a
    /// cryptographic claim that arbitrary user-supplied evidence is trusted.
    pub fn bind_entry_admission(&mut self, proof: &EntryAdmissionEvidence) -> StoreResult<()> {
        Self::bind_entry_admission_sql(&self.connection, None, proof)
    }

    pub fn entry_admission(&self, id: &str) -> StoreResult<Option<EntryAdmissionEvidence>> {
        Self::entry_admission_sql(&self.connection, None, id)
    }

    /// A source-complete venue cancellation with ZERO executed quantity.
    /// No data absence is inferred from a venue ACK: both history streams
    /// must be fully captured beyond the terminal update and SQL must contain
    /// no fill, order-linked fee, or unclassified bill in the causal window.
    /// Later independently sourced fees are still charged by the normal
    /// append-only late-settlement revision.
    pub fn bind_zero_fill_terminal_entry_settlement(
        &mut self,
        id: &str,
        venue_updated_at: i64,
        covered_through: i64,
    ) -> StoreResult<EntrySettlementEvidence> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = Self::bind_zero_fill_terminal_entry_settlement_sql(
            &tx,
            None,
            id,
            venue_updated_at,
            covered_through,
        )?;
        tx.commit()?;
        Ok(result)
    }

    /// E1.4 final source: only a terminal, independently read-back exchange
    /// order whose complete fills and fee bills exist in the sole SQL owner
    /// can produce a cumulative USD charge. Partial/zero-fill or stale pages
    /// do NOT create a terminal accounting claim. The up-front source-bound
    /// Frozen admission FX and separately verified entry fees come from the SAME durable admission
    /// used for Worker RESERVE; no observed spot FX is promoted here.
    pub fn bind_terminal_entry_settlement(
        &mut self,
        id: &str,
        venue_updated_at: i64,
        venue_filled_quantity: Decimal,
        covered_through: i64,
    ) -> StoreResult<EntrySettlementEvidence> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = Self::bind_terminal_entry_settlement_sql(
            &tx,
            None,
            id,
            venue_updated_at,
            venue_filled_quantity,
            covered_through,
        )?;
        tx.commit()?;
        Ok(result)
    }

    /// Incremental fills and their independently linked bills are settled
    /// BEFORE terminal release. No new journal, scheduler or funding formula:
    /// this is the same admission FX and source ledger as terminal settlement.
    pub fn bind_partial_entry_settlement(
        &mut self,
        id: &str,
        venue_updated_at: i64,
        venue_filled_quantity: Decimal,
        covered_through: i64,
    ) -> StoreResult<EntrySettlementEvidence> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = Self::bind_partial_entry_settlement_sql(
            &tx,
            None,
            id,
            venue_updated_at,
            venue_filled_quantity,
            covered_through,
        )?;
        tx.commit()?;
        Ok(result)
    }

    /// Existing original AccountStore partial-entry receipt. v5 and v6
    /// share the same verification rule, with the exact owner identity
    /// selected by the source SQLite scope.
    pub fn partial_entry_settlement_receipt(
        &self,
        id: &str,
    ) -> StoreResult<Option<EntrySettlementEvidence>> {
        let result = Self::partial_entry_settlement_receipt_sql(
            &self.connection,
            None,
            &self.meta.account_key,
            id,
        )?;
        if result.is_some() {
            self.partial_entry_ack_pending(id)?;
        }
        Ok(result)
    }

    /// Original immutable entry/fee/fill source-linked revision readback,
    /// performed ON THE CALLER'S SQLite transaction for installation v6.
    /// Every join and operation ID is scoped to both installation+account:
    /// external IDs/intent IDs from a second account can never authorize
    /// settlement in this one. Read-only; source receipts are constructed
    /// by the original AccountStore fill/bill producer, never by caller flags.
    pub(crate) fn partial_entry_settlement_receipt_sql(
        db: &rusqlite::Connection,
        installation: Option<&str>,
        account: &str,
        id: &str,
    ) -> StoreResult<Option<EntrySettlementEvidence>> {
        use rusqlite::{params_from_iter, types::Value};
        if account.is_empty() || id.is_empty() || installation.is_some_and(str::is_empty) {
            return Err(StoreError::Invalid(
                "ENTRY_PARTIAL_SQL_SOURCE_SCOPE_INVALID",
            ));
        }
        let read_operation =
            |operation_id: String| -> StoreResult<Option<(String, String, String)>> {
                let mut args = vec![Value::Text(operation_id)];
                if let Some(scope) = installation {
                    args.push(Value::Text(scope.to_owned()));
                    args.push(Value::Text(account.to_owned()));
                }
                db.query_row(
                    if installation.is_some() {
                        "SELECT kind,request_hash,result_ref FROM operation_receipts
                     WHERE operation_id=?1 AND installation_id=?2 AND account_key=?3"
                    } else {
                        "SELECT kind,request_hash,result_ref FROM operation_receipts
                     WHERE operation_id=?1"
                    },
                    params_from_iter(args.iter()),
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
                .map_err(StoreError::from)
            };
        let Some(head) = read_operation(format!("entry-partial-head:{id}"))? else {
            return Ok(None);
        };
        let proof: EntrySettlementEvidence = serde_json::from_str(&head.2)
            .map_err(|_| StoreError::Invalid("ENTRY_PARTIAL_HEAD_CORRUPT"))?;
        let immutable =
            read_operation(format!("entry-partial:{id}:r{}", proof.settlement_revision,))?
                .ok_or(StoreError::Invalid("ENTRY_PARTIAL_REVISION_MISSING"))?;
        let mut args = vec![Value::Text(id.to_owned())];
        if let Some(scope) = installation {
            args.extend([
                Value::Text(scope.to_owned()),
                Value::Text(account.to_owned()),
            ]);
        }
        let intent: Option<(String, Option<String>)> = db
            .query_row(
                if installation.is_some() {
                    "SELECT plan_hash,exchange_order_id FROM intents
                 WHERE intent_id=?1 AND installation_id=?2 AND account_key=?3"
                } else {
                    "SELECT plan_hash,exchange_order_id FROM intents WHERE intent_id=?1"
                },
                params_from_iter(args.iter()),
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let (plan, order_id) = intent.ok_or(StoreError::Invalid("ENTRY_PARTIAL_INTENT_MISSING"))?;
        let reservation: Option<(String, String)> = db
            .query_row(
                if installation.is_some() {
                    "SELECT reservation_key,account_key FROM risk_receipts
                 WHERE intent_id=?1 AND installation_id=?2 AND account_key=?3"
                } else {
                    "SELECT reservation_key,account_key FROM risk_receipts WHERE intent_id=?1"
                },
                params_from_iter(args.iter()),
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let (reservation_key, reservation_account) =
            reservation.ok_or(StoreError::Invalid("ENTRY_PARTIAL_RESERVATION_MISSING"))?;
        if proof.schema != "okx.entry-partial-settlement/v1"
            || proof.account_key != account
            || proof.intent_id != id
            || proof.plan_hash != plan
            || proof.reservation_key != reservation_key
            || proof.account_key != reservation_account
            || proof.venue_order_id.is_empty()
            || order_id.as_deref() != Some(proof.venue_order_id.as_str())
            || proof.settlement_revision == 0
            || proof.cumulative_entry_usd <= Decimal::ZERO
            || proof.source_digest.len() != 64
            || !proof
                .source_digest
                .bytes()
                .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v))
            || head.0 != "ENTRY_PARTIAL_HEAD"
            || head.1 != proof.plan_hash
            || immutable.0 != "ENTRY_PARTIAL_SETTLEMENT_PROVEN"
            || immutable.1 != proof.plan_hash
            || immutable.2 != head.2
        {
            return Err(StoreError::Invalid("ENTRY_PARTIAL_SOURCE_UNPROVEN"));
        }
        // An ACK is not required for a first financial debit, but a
        // conflicting or forged ACK can never authorize an additional debit.
        if let Some(ack) = read_operation(format!(
            "entry-partial-settled:{id}:r{}",
            proof.settlement_revision,
        ))? && (ack.0 != "ENTRY_PARTIAL_SETTLED"
            || ack.1 != proof.plan_hash
            || ack.2 != proof.source_digest)
        {
            return Err(StoreError::Invalid("PARTIAL_ENTRY_ACK_CONFLICT"));
        }
        Ok(Some(proof))
    }

    /// Read the ORIGINAL immutable terminal/late SQL money producer on the
    /// same scoped financial snapshot. The caller supplies ONLY the address
    /// and kind, never a debit amount, fake fee, zero-fill flag or JSON proof.
    /// A late revision is bound to the original terminal receipt and its
    /// immutable revision; mutable head pointers cannot mint new money.
    pub(crate) fn final_entry_settlement_receipt_sql(
        db: &rusqlite::Connection,
        installation: &str,
        account: &str,
        id: &str,
        late: bool,
    ) -> StoreResult<Option<EntrySettlementEvidence>> {
        if installation.is_empty() || account.is_empty() || id.is_empty() {
            return Err(StoreError::Invalid("UNIFIED_FINAL_SOURCE_SCOPE_INVALID"));
        }
        let read = |operation: &str| -> StoreResult<Option<(String, String, String, i64)>> {
            db.query_row(
                "SELECT kind,request_hash,result_ref,committed_at
                 FROM operation_receipts
                 WHERE installation_id=?1 AND account_key=?2 AND operation_id=?3",
                params![installation, account, operation],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(StoreError::from)
        };
        let terminal_operation = format!("entry-settlement:{id}");
        let Some(terminal_row) = read(&terminal_operation)? else {
            return Ok(None);
        };
        let terminal: EntrySettlementEvidence = serde_json::from_str(&terminal_row.2)
            .map_err(|_| StoreError::Invalid("UNIFIED_TERMINAL_SOURCE_CORRUPT"))?;
        if terminal_row.0 != "ENTRY_SETTLEMENT_PROVEN"
            || terminal.schema != "okx.entry-terminal-settlement/v1"
            || terminal.account_key != account
            || terminal.intent_id != id
            || terminal_row.1 != terminal.plan_hash
            || terminal_row.3 != terminal.venue_update_ms
            || terminal.settlement_revision == 0
            || terminal.source_covered_through_ms < terminal.venue_update_ms
            || terminal.cumulative_entry_usd < Decimal::ZERO
        {
            return Err(StoreError::Invalid("UNIFIED_TERMINAL_SOURCE_INVALID"));
        }
        let (proof, head, immutable) = if late {
            let Some(head) = read(&format!("entry-late-head:{id}"))? else {
                return Ok(None);
            };
            let proof: EntrySettlementEvidence = serde_json::from_str(&head.2)
                .map_err(|_| StoreError::Invalid("UNIFIED_LATE_HEAD_CORRUPT"))?;
            let immutable = read(&format!("entry-late:{id}:r{}", proof.settlement_revision))?
                .ok_or(StoreError::Invalid("UNIFIED_LATE_REVISION_MISSING"))?;
            if head.0 != "ENTRY_LATE_HEAD"
                || immutable.0 != "ENTRY_LATE_SETTLEMENT_PROVEN"
                || proof.schema != "okx.entry-late-settlement/v1"
                || head.1 != terminal.plan_hash
                || immutable.1 != head.1
                || immutable.2 != head.2
                || immutable.3 != proof.source_covered_through_ms
                || head.3 != proof.source_covered_through_ms
                || proof.account_key != terminal.account_key
                || proof.intent_id != terminal.intent_id
                || proof.plan_hash != terminal.plan_hash
                || proof.reservation_key != terminal.reservation_key
                || proof.venue_order_id != terminal.venue_order_id
                || proof.venue_update_ms != terminal.venue_update_ms
                || proof.venue_filled_quantity != terminal.venue_filled_quantity
                || proof.settlement_revision <= terminal.settlement_revision
                || proof.source_covered_through_ms <= terminal.source_covered_through_ms
                || proof.cumulative_entry_usd <= terminal.cumulative_entry_usd
            {
                return Err(StoreError::Invalid("UNIFIED_LATE_SOURCE_INVALID"));
            }
            (proof, head, immutable)
        } else {
            (terminal.clone(), terminal_row.clone(), terminal_row)
        };
        if head.2 != immutable.2
            || proof.account_key != account
            || proof.intent_id != id
            || proof.reservation_key.is_empty()
            || proof.reservation_key.starts_with("local:")
            || proof.venue_order_id.is_empty()
            || proof.venue_update_ms <= 0
            || proof.source_covered_through_ms < proof.venue_update_ms
            || proof.settlement_revision == 0
            || proof.source_digest.len() != 64
            || !proof
                .source_digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || proof.plan_hash.len() != 64
            || !proof
                .plan_hash
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(StoreError::Invalid("UNIFIED_FINAL_SOURCE_INVALID"));
        }
        let intent: Option<(String, Option<String>, String, i64)> = db
            .query_row(
                "SELECT plan_hash,exchange_order_id,state,risk_increase
                 FROM intents
                 WHERE installation_id=?1 AND account_key=?2 AND intent_id=?3",
                params![installation, account, id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let receipt: Option<(String, String)> = db
            .query_row(
                "SELECT reservation_key,plan_hash FROM risk_receipts
                 WHERE installation_id=?1 AND account_key=?2 AND intent_id=?3",
                params![installation, account, id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if !matches!(
            intent,
            Some((ref plan, Some(ref venue), ref state, 1))
                if *plan == proof.plan_hash
                    && *venue == proof.venue_order_id
                    && matches!(state.as_str(), "FILLED" | "CANCELLED")
        ) || !matches!(receipt,
            Some((ref reservation, ref plan))
                if *reservation == proof.reservation_key && *plan == proof.plan_hash)
        {
            return Err(StoreError::Invalid("UNIFIED_FINAL_INTENT_SOURCE_UNPROVEN"));
        }
        Ok(Some(proof))
    }

    /// The RiskBook debit and this ORIGINAL native ACK live in the SAME
    /// installation transaction. A late fee cannot clear a newer pending
    /// bill marker by merely updating the old SQL head.
    pub(crate) fn acknowledge_unified_final_entry_sql(
        tx: &rusqlite::Transaction<'_>,
        installation: &str,
        account: &str,
        accepted: &EntrySettlementEvidence,
        late: bool,
    ) -> StoreResult<()> {
        let stored = Self::final_entry_settlement_receipt_sql(
            tx,
            installation,
            account,
            &accepted.intent_id,
            late,
        )?
        .ok_or(StoreError::Invalid("UNIFIED_FINAL_ACK_SOURCE_MISSING"))?;
        if stored != *accepted {
            return Err(StoreError::Invalid("UNIFIED_FINAL_ACK_SOURCE_CHANGED"));
        }
        let state: String = tx.query_row(
            "SELECT state FROM risk_receipts
             WHERE installation_id=?1 AND account_key=?2
               AND intent_id=?3 AND plan_hash=?4 AND reservation_key=?5",
            params![
                installation,
                account,
                &accepted.intent_id,
                &accepted.plan_hash,
                &accepted.reservation_key
            ],
            |row| row.get(0),
        )?;
        if (late && !matches!(state.as_str(), "LATE_SETTLEMENT_PENDING" | "RELEASED"))
            || (!late
                && !matches!(
                    state.as_str(),
                    "OPEN" | "POSTED" | "UNKNOWN" | "TERMINAL_PENDING_CENTRAL" | "RELEASED"
                ))
        {
            return Err(StoreError::Invalid("UNIFIED_FINAL_ACK_STATE_INVALID"));
        }
        let (operation, kind) = if late {
            (
                format!(
                    "entry-late-settled:{}:r{}",
                    accepted.intent_id, accepted.settlement_revision
                ),
                "ENTRY_LATE_SETTLED",
            )
        } else {
            (
                format!(
                    "entry-terminal-settled:{}:r{}",
                    accepted.intent_id, accepted.settlement_revision
                ),
                "ENTRY_TERMINAL_SETTLED",
            )
        };
        tx.execute(
            "INSERT INTO operation_receipts(
                 installation_id,account_key,operation_id,kind,
                 request_hash,result_ref,committed_at)
             VALUES(?1,?2,?3,?4,?5,?6,?7)
             ON CONFLICT(installation_id,account_key,operation_id) DO NOTHING",
            params![
                installation,
                account,
                &operation,
                kind,
                &accepted.plan_hash,
                &accepted.source_digest,
                wall_time_ms()
            ],
        )?;
        let observed: (String, String, String) = tx.query_row(
            "SELECT kind,request_hash,result_ref FROM operation_receipts
             WHERE installation_id=?1 AND account_key=?2 AND operation_id=?3",
            params![installation, account, &operation],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        if observed
            != (
                kind.to_owned(),
                accepted.plan_hash.clone(),
                accepted.source_digest.clone(),
            )
        {
            return Err(StoreError::Invalid("UNIFIED_FINAL_ACK_CONFLICT"));
        }
        // The original post-release late marker is cleared only if the
        // campaign itself is ALREADY released in this same transaction.
        // A pending terminal exposure can never be discharged by a fee ACK.
        if late && state == "LATE_SETTLEMENT_PENDING" {
            let entry: Option<String> = tx
                .query_row(
                    "SELECT state FROM risk_entries
                     WHERE installation_id=?1 AND account_key=?2
                       AND intent_id=?3 AND plan_sha256=?4
                       AND reservation_key=?5",
                    params![
                        installation,
                        account,
                        &accepted.intent_id,
                        &accepted.plan_hash,
                        &accepted.reservation_key
                    ],
                    |row| row.get(0),
                )
                .optional()?;
            if entry.as_deref() == Some("RELEASED") {
                tx.execute(
                    "UPDATE risk_receipts SET state='RELEASED'
                     WHERE installation_id=?1 AND account_key=?2
                       AND intent_id=?3 AND state='LATE_SETTLEMENT_PENDING'",
                    params![installation, account, &accepted.intent_id],
                )?;
            }
        }
        Ok(())
    }

    /// The ORIGINAL AccountStore ACK is written in the SAME installation
    /// SQL transaction as the RiskBook budget debit. The existing immutable
    /// source/head/intent/receipt verification is rerun on this same snapshot,
    /// and any ACK conflict fails the entire caller-owned transaction.
    /// The legacy two-SQL method remains only for the pre-cutover owner.
    pub(crate) fn acknowledge_unified_partial_entry_sql(
        tx: &rusqlite::Transaction<'_>,
        installation: &str,
        account: &str,
        accepted: &EntrySettlementEvidence,
    ) -> StoreResult<()> {
        if installation.is_empty() || account.is_empty() || accepted.account_key != account {
            return Err(StoreError::Invalid("UNIFIED_PARTIAL_ACK_IDENTITY_INVALID"));
        }
        let stored = Self::partial_entry_settlement_receipt_sql(
            tx,
            Some(installation),
            account,
            &accepted.intent_id,
        )?
        .ok_or(StoreError::Invalid("UNIFIED_PARTIAL_ACK_SOURCE_MISSING"))?;
        if stored != *accepted || stored.reservation_key.starts_with("local:") {
            return Err(StoreError::Invalid("UNIFIED_PARTIAL_ACK_SOURCE_MISMATCH"));
        }
        let state: Option<String> = tx
            .query_row(
                "SELECT state FROM risk_receipts
             WHERE installation_id=?1 AND account_key=?2
               AND intent_id=?3 AND plan_hash=?4 AND reservation_key=?5",
                rusqlite::params![
                    installation,
                    account,
                    &accepted.intent_id,
                    &accepted.plan_hash,
                    &accepted.reservation_key,
                ],
                |row| row.get(0),
            )
            .optional()?;
        // A lost venue ACK leaves this original receipt UNKNOWN, even when
        // independently captured immutable fills/bills prove a real partial
        // debit. Preserve the risk obligation and accept only that exact
        // AccountStore source revision; UNKNOWN itself grants no second POST.
        if !matches!(state.as_deref(), Some("OPEN" | "POSTED" | "UNKNOWN")) {
            return Err(StoreError::Invalid("UNIFIED_PARTIAL_ACK_RESERVATION_STATE"));
        }
        let operation = format!(
            "entry-partial-settled:{}:r{}",
            accepted.intent_id, accepted.settlement_revision,
        );
        tx.execute(
            "INSERT INTO operation_receipts(
                 installation_id,account_key,operation_id,kind,
                 request_hash,result_ref,committed_at)
             VALUES(?1,?2,?3,'ENTRY_PARTIAL_SETTLED',?4,?5,?6)
             ON CONFLICT(installation_id,account_key,operation_id) DO NOTHING",
            rusqlite::params![
                installation,
                account,
                &operation,
                &accepted.plan_hash,
                &accepted.source_digest,
                wall_time_ms(),
            ],
        )?;
        let ack: (String, String, String) = tx.query_row(
            "SELECT kind,request_hash,result_ref FROM operation_receipts
             WHERE installation_id=?1 AND account_key=?2 AND operation_id=?3",
            rusqlite::params![installation, account, &operation],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        if ack
            != (
                "ENTRY_PARTIAL_SETTLED".to_owned(),
                accepted.plan_hash.clone(),
                accepted.source_digest.clone(),
            )
        {
            return Err(StoreError::Invalid("UNIFIED_PARTIAL_ACK_CONFLICT"));
        }
        Ok(())
    }

    /// Read-only check of the latest durable partial settlement. A lost
    /// Worker reply must not let a terminal release overtake the ORIGINAL
    /// immutable SQL revision. No head means no partial claim to reconcile.
    /// A corrupted head/revision/ACK never becomes permission to release.
    pub fn partial_entry_ack_pending(&self, id: &str) -> StoreResult<bool> {
        Self::partial_entry_ack_pending_sql(&self.connection, None, id)
    }

    /// The sole Native SQL owner records the exact cumulative revision only
    /// AFTER the persisted Worker RiskBook has accepted SETTLE_ENTRY.  This
    /// is an ACK of the original immutable venue/bill/FX witness, not a second
    /// monetary producer. Crashes before ACK remain fail closed and replayable.
    pub fn acknowledge_partial_entry_settlement(
        &mut self,
        accepted: &EntrySettlementEvidence,
    ) -> StoreResult<()> {
        if accepted.schema != "okx.entry-partial-settlement/v1"
            || accepted.settlement_revision == 0
            || accepted.account_key != self.meta.account_key
            || accepted.source_digest.len() != 64
            || !accepted
                .source_digest
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
        {
            return Err(StoreError::Invalid("PARTIAL_ENTRY_ACK_IDENTITY_INVALID"));
        }
        let revision_key = format!(
            "entry-partial:{}:r{}",
            accepted.intent_id, accepted.settlement_revision
        );
        let immutable = self
            .operation(&revision_key)?
            .ok_or(StoreError::Invalid("PARTIAL_ENTRY_ACK_SOURCE_MISSING"))?;
        let saved: EntrySettlementEvidence = serde_json::from_str(&immutable.result_ref)
            .map_err(|_| StoreError::Invalid("PARTIAL_ENTRY_ACK_SOURCE_CORRUPT"))?;
        let i = self
            .intent(&accepted.intent_id)?
            .ok_or(StoreError::Invalid("PARTIAL_ENTRY_ACK_INTENT_MISSING"))?;
        let r = self
            .central_receipt(&accepted.intent_id)?
            .ok_or(StoreError::Invalid("PARTIAL_ENTRY_ACK_RESERVATION_MISSING"))?;
        let receipt_state: String = self
            .connection
            .query_row(
                "SELECT state FROM risk_receipts WHERE intent_id=?1 AND plan_hash=?2",
                params![accepted.intent_id, accepted.plan_hash],
                |row| row.get(0),
            )
            .optional()?
            .ok_or(StoreError::Invalid(
                "PARTIAL_ENTRY_ACK_RESERVATION_STATE_MISSING",
            ))?;
        if saved != *accepted
            || immutable.kind != "ENTRY_PARTIAL_SETTLEMENT_PROVEN"
            || immutable.request_hash != accepted.plan_hash
            || i.plan_hash != accepted.plan_hash
            || i.exchange_order_id.as_deref() != Some(accepted.venue_order_id.as_str())
            || r.plan_hash != accepted.plan_hash
            || r.reservation_key != accepted.reservation_key
            || r.reservation_key.starts_with("local:")
            || !matches!(receipt_state.as_str(), "OPEN" | "POSTED")
        {
            return Err(StoreError::Invalid("PARTIAL_ENTRY_ACK_SOURCE_MISMATCH"));
        }
        let key = format!(
            "entry-partial-settled:{}:r{}",
            accepted.intent_id, accepted.settlement_revision
        );
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO operation_receipts
                (operation_id,kind,request_hash,result_ref,committed_at)
             VALUES(?1,'ENTRY_PARTIAL_SETTLED',?2,?3,?4)
             ON CONFLICT(operation_id) DO NOTHING",
            params![
                key,
                accepted.plan_hash,
                accepted.source_digest,
                wall_time_ms()
            ],
        )?;
        let verified: (String, String, String) = tx.query_row(
            "SELECT kind,request_hash,result_ref FROM operation_receipts
             WHERE operation_id=?1",
            [key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        if verified
            != (
                "ENTRY_PARTIAL_SETTLED".into(),
                accepted.plan_hash.clone(),
                accepted.source_digest.clone(),
            )
        {
            return Err(StoreError::Invalid("PARTIAL_ENTRY_ACK_CONFLICT"));
        }
        tx.commit()?;
        Ok(())
    }

    /// One additional authenticated order-fee bill after terminal RELEASE.
    /// The first terminal receipt is immutable; every later fee grows an
    /// independent append-only cumulative revision under the SAME reserve.
    pub fn bind_late_entry_settlement(
        &mut self,
        id: &str,
        venue_updated_at: i64,
        venue_filled_quantity: Decimal,
        covered_through: i64,
    ) -> StoreResult<EntrySettlementEvidence> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = Self::bind_late_entry_settlement_sql(
            &tx,
            None,
            id,
            venue_updated_at,
            venue_filled_quantity,
            covered_through,
        )?;
        tx.commit()?;
        Ok(result)
    }

    /// One indexed read for the existing maintenance owner. The marker is
    /// installed atomically with a previously unseen independent fee bill.
    pub fn entry_late_pending(&self, id: &str) -> StoreResult<bool> {
        Self::entry_late_pending_sql(&self.connection, None, id)
    }

    /// Only after central SETTLE_ENTRY has accepted this exact revision can
    /// SQL clear the pending late-fee marker. A lost ACK retries without a
    /// second debit or an exchange POST.
    pub fn acknowledge_late_entry_settlement(
        &mut self,
        settlement: &EntrySettlementEvidence,
    ) -> StoreResult<()> {
        let head = self
            .operation(&format!("entry-late-head:{}", settlement.intent_id))?
            .ok_or(StoreError::Invalid("LATE_ENTRY_HEAD_MISSING"))?;
        let saved: EntrySettlementEvidence = serde_json::from_str(&head.result_ref)
            .map_err(|_| StoreError::Invalid("LATE_ENTRY_HEAD_CORRUPT"))?;
        if saved != *settlement {
            return Err(StoreError::Invalid("LATE_ENTRY_ACK_SOURCE_CHANGED"));
        }
        // Source capture can add ANOTHER independent fee between the native
        // proof and central SETTLE_ENTRY response. Do not clear the account
        // maintenance marker for a newer unbooked bill or lost cursor.
        let current = self
            .cursor("bills", "account")?
            .ok_or(StoreError::Invalid("LATE_ENTRY_BILLS_UNCOVERED"))?;
        let (accepted_fee, _) = self.intent_additional_entry_fee_evidence(
            &settlement.intent_id,
            settlement.source_covered_through_ms,
        )?;
        let (latest_fee, _) =
            self.intent_additional_entry_fee_evidence(&settlement.intent_id, current.watermark)?;
        if latest_fee > accepted_fee {
            return Err(StoreError::Invalid("LATE_ENTRY_NEW_BILL_PENDING"));
        }
        let key = format!(
            "entry-late-settled:{}:r{}",
            settlement.intent_id, settlement.settlement_revision
        );
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO operation_receipts(operation_id,kind,request_hash,result_ref,committed_at)
             VALUES(?1,'ENTRY_LATE_SETTLED',?2,?3,?4)
             ON CONFLICT(operation_id) DO NOTHING",
            params![
                key,
                settlement.plan_hash,
                settlement.source_digest,
                wall_time_ms()
            ],
        )?;
        let proof: (String, String, String) = tx.query_row(
            "SELECT kind,request_hash,result_ref FROM operation_receipts WHERE operation_id=?1",
            [key],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        if proof
            != (
                "ENTRY_LATE_SETTLED".into(),
                settlement.plan_hash.clone(),
                settlement.source_digest.clone(),
            )
        {
            return Err(StoreError::Invalid("LATE_ENTRY_ACK_CONFLICT"));
        }
        tx.execute(
            "UPDATE risk_receipts SET state='RELEASED'
             WHERE intent_id=?1 AND state IN ('LATE_SETTLEMENT_PENDING','RELEASED')",
            [&settlement.intent_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Durable address of a central reservation attempt, committed before
    /// any WSS RESERVE. A crash or lost ACK cannot orphan the Worker grant.
    pub fn mark_reserve_attempt(&mut self, id: &str, key: &str, now: i64) -> StoreResult<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::mark_reserve_attempt_sql(&tx, None, id, key, now)?;
        tx.commit()?;
        Ok(())
    }

    /// An explicit central DENIED plus an authenticated address-specific
    /// NOT_FOUND readback proves this attempt minted no reservation. Both
    /// the original reason and its no-risk marker commit atomically.
    pub fn reject_denied_reserve(&mut self, id: &str, reason: &str, now: i64) -> StoreResult<()> {
        self.reject_unsent_inner(id, reason, now, true).map(|_| ())
    }

    pub fn declined_reserve(&self, id: &str) -> StoreResult<bool> {
        Ok(self.connection.query_row(
            "SELECT EXISTS(
               SELECT 1 FROM intents i
               JOIN operation_receipts o ON o.operation_id='risk-reserve:'||i.intent_id
               JOIN operation_receipts t ON t.operation_id='terminal:'||i.intent_id
               WHERE i.intent_id=?1 AND i.state='REJECTED' AND i.begin_post_at IS NULL
                 AND o.kind='RISK_RESERVE_DENIED'
                 AND o.request_hash=i.request_hash
                 AND t.kind='REJECTED_UNSENT' AND t.request_hash=i.request_hash)",
            [id],
            |r| r.get(0),
        )?)
    }

    /// F1: the prepare transaction committed, but no central RESERVE was
    /// attempted. A matching terminal SQL receipt proves no native POST.
    pub fn unsent_before_reserve(&self, id: &str) -> StoreResult<bool> {
        Self::unsent_before_reserve_sql(&self.connection, None, id)
    }

    pub fn reconcilable_intents(&self, limit: usize) -> StoreResult<Vec<StoredIntent>> {
        self.reconcilable_intents_page(limit, 0)
    }

    /// Bounded, addressable paging prevents a failed early intent from starving
    /// later account intents. Offset is a scheduling cursor, not a second owner.
    pub fn reconcilable_intents_page(
        &self,
        limit: usize,
        offset: usize,
    ) -> StoreResult<Vec<StoredIntent>> {
        Self::reconcilable_intents_page_sql(&self.connection, None, limit, offset, wall_time_ms())
    }
    /// A reduce-only local: reservation never reached Worker RiskBook.
    /// Its SQL no-send obligation must still leave risk_census_intents;
    /// success without a durable release ACK is not settlement.
    pub fn acknowledge_local_unsent_release(&mut self, id: &str) -> StoreResult<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::acknowledge_local_unsent_release_sql(
            &tx,
            None,
            id,
            &self.meta.account_key,
            wall_time_ms(),
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn acknowledge_central_release(&mut self, key: &str, evidence: &str) -> StoreResult<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::acknowledge_central_release_sql(&tx, None, key, evidence, wall_time_ms())?;
        tx.commit()?;
        Ok(())
    }

    pub fn local_terminal_close_evidence(
        &self,
        id: &str,
        order_id: &str,
        venue_filled: Decimal,
        venue_updated_at: i64,
    ) -> StoreResult<(Decimal, u64, u64, Decimal, String)> {
        Self::local_terminal_close_evidence_sql(
            &self.connection,
            None,
            id,
            order_id,
            venue_filled,
            venue_updated_at,
        )
    }

    /// The single production mutation uses the SAME read-only local Close
    /// witness as scenario diagnostics. No test can manufacture a release.
    pub fn acknowledge_local_terminal_close(
        &mut self,
        id: &str,
        order_id: &str,
        venue_filled: Decimal,
        venue_updated_at: i64,
    ) -> StoreResult<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::acknowledge_local_terminal_close_sql(
            &tx,
            None,
            id,
            order_id,
            venue_filled,
            venue_updated_at,
            wall_time_ms(),
        )?;
        tx.commit()?;
        Ok(())
    }
    pub fn reject_unsent(&mut self, id: &str, reason: &str, now: i64) -> StoreResult<bool> {
        self.reject_unsent_inner(id, reason, now, false)
    }

    fn reject_unsent_inner(
        &mut self,
        id: &str,
        reason: &str,
        now: i64,
        proven_not_reserved: bool,
    ) -> StoreResult<bool> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = Self::reject_unsent_sql(&tx, None, id, reason, now, proven_not_reserved)?;
        tx.commit()?;
        Ok(result)
    }

    /// A no-send release requires the ORIGINAL durable rejection receipt,
    /// matching the request with no committed POST claim. The same query
    /// and rule now support both the legacy single-account v2 DB and the
    /// installation-wide v6 account scope. Caller data is NEVER a receipt.
    pub fn unsent_release_receipt(&self, id: &str) -> StoreResult<Option<UnsentSqlReceipt>> {
        Self::unsent_release_receipt_sql(&self.connection, None, &self.meta.account_key, id)
    }

    /// Read the original AccountStore proof from the SAME caller-owned SQL
    /// snapshot. v6 must bind installation+account on every joined table,
    /// not simply filter the final row by intent_id.
    pub(crate) fn unsent_release_receipt_sql(
        db: &rusqlite::Connection,
        installation: Option<&str>,
        account: &str,
        id: &str,
    ) -> StoreResult<Option<UnsentSqlReceipt>> {
        use rusqlite::{params_from_iter, types::Value};
        if account.is_empty() || id.is_empty() || installation.is_some_and(str::is_empty) {
            return Err(StoreError::Invalid("UNSENT_SQL_SOURCE_SCOPE_INVALID"));
        }
        let original_sql = "SELECT COALESCE(r.reservation_key,a.result_ref),
                        COALESCE(r.account_key,?2),
                        i.intent_id,i.plan_hash,i.request_hash,i.incident
                 FROM intents i
                 LEFT JOIN risk_receipts r ON r.intent_id=i.intent_id
                 LEFT JOIN operation_receipts a ON a.operation_id='risk-reserve:'||i.intent_id
                    AND a.kind='RISK_RESERVE_ATTEMPT' AND a.request_hash=i.request_hash
                 JOIN operation_receipts o ON o.operation_id='terminal:'||i.intent_id
                 WHERE i.intent_id=?1 AND i.state='REJECTED'
                   AND i.begin_post_at IS NULL AND i.exchange_order_id IS NULL
                   AND (r.state IN('UNSENT_PENDING_CENTRAL','RELEASED') OR
                        (r.reservation_key IS NULL AND a.result_ref IS NOT NULL))
                   AND o.kind='REJECTED_UNSENT'
                   AND o.request_hash=i.request_hash AND o.result_ref=i.incident
                   AND (r.reservation_key IS NULL OR
                        (r.account_key=?2 AND r.plan_hash=i.plan_hash))";
        let scoped_sql = "SELECT COALESCE(r.reservation_key,a.result_ref),
                        COALESCE(r.account_key,?2),
                        i.intent_id,i.plan_hash,i.request_hash,i.incident
                 FROM intents i
                 LEFT JOIN risk_receipts r
                   ON r.intent_id=i.intent_id
                  AND r.installation_id=i.installation_id AND r.account_key=i.account_key
                 LEFT JOIN operation_receipts a
                   ON a.operation_id='risk-reserve:'||i.intent_id
                  AND a.installation_id=i.installation_id AND a.account_key=i.account_key
                  AND a.kind='RISK_RESERVE_ATTEMPT' AND a.request_hash=i.request_hash
                 JOIN operation_receipts o
                   ON o.operation_id='terminal:'||i.intent_id
                  AND o.installation_id=i.installation_id AND o.account_key=i.account_key
                 WHERE i.intent_id=?1 AND i.account_key=?2 AND i.installation_id=?3
                   AND i.state='REJECTED'
                   AND i.begin_post_at IS NULL AND i.exchange_order_id IS NULL
                   AND (r.state IN('UNSENT_PENDING_CENTRAL','RELEASED') OR
                        (r.reservation_key IS NULL AND a.result_ref IS NOT NULL))
                   AND o.kind='REJECTED_UNSENT'
                   AND o.request_hash=i.request_hash AND o.result_ref=i.incident
                   AND (r.reservation_key IS NULL OR
                        (r.account_key=?2 AND r.plan_hash=i.plan_hash))";
        let mut args = vec![Value::Text(id.to_owned()), Value::Text(account.to_owned())];
        if let Some(installation) = installation {
            args.push(Value::Text(installation.to_owned()));
        }
        db.query_row(
            if installation.is_some() {
                scoped_sql
            } else {
                original_sql
            },
            params_from_iter(args.iter()),
            |r| {
                Ok(UnsentSqlReceipt {
                    reservation_key: r.get(0)?,
                    account_key: r.get(1)?,
                    intent_id: r.get(2)?,
                    plan_hash: r.get(3)?,
                    request_hash: r.get(4)?,
                    reason: r.get(5)?,
                })
            },
        )
        .optional()
        .map_err(StoreError::from)
    }
    pub fn central_receipt(&self, id: &str) -> StoreResult<Option<CentralReceipt>> {
        self.reservation_receipt(id, CentralReceiptView::Retained)
    }
    /// RELEASED receipts remain observable, but cannot cover exposure.
    pub fn active_central_receipt(&self, id: &str) -> StoreResult<Option<CentralReceipt>> {
        self.reservation_receipt(id, CentralReceiptView::ActiveExposure)
    }
    pub fn by_exchange_order_id(&self, key: &str) -> StoreResult<Option<StoredIntent>> {
        self.find_intent("exchange_order_id", key)
    }
    /// The ORIGINAL AccountStore PREPARE rules, on either legacy account-v2
    /// or the installation-wide v6 finance transaction. All original intent,
    /// decision, plan and strategy-version reads/writes share the caller's
    /// snapshot. In v6 every lookup and key is scoped to this exact account.
    /// This method never opens a second connection or commits a transaction.
    pub(crate) fn prepare_sql_transaction(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        decision: &Decision,
        intent: &NewIntent,
    ) -> StoreResult<bool> {
        if intent.plan.contract_value <= Decimal::ZERO
            || intent.plan.settlement_currency.is_empty()
            || !matches!(intent.plan.position_side.as_str(), "long" | "short")
        {
            return Err(StoreError::Invalid("INVALID_CONTRACT_MECHANICS"));
        }
        if let Some((installation, account)) = scope {
            if installation.is_empty() || account.is_empty() {
                return Err(StoreError::Invalid("UNIFIED_PREPARE_SCOPE_INVALID"));
            }
            let known: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM account_meta
                 WHERE installation_id=?1 AND account_key=?2)",
                params![installation, account],
                |row| row.get(0),
            )?;
            if !known {
                return Err(StoreError::Invalid("UNIFIED_PREPARE_ACCOUNT_MISSING"));
            }
        }
        let existing: Option<String> = match scope {
            Some((installation, account)) => tx
                .query_row(
                    "SELECT request_hash FROM intents
                     WHERE installation_id=?1 AND account_key=?2 AND idempotency_key=?3",
                    params![installation, account, intent.idempotency_key],
                    |row| row.get(0),
                )
                .optional()?,
            None => tx
                .query_row(
                    "SELECT request_hash FROM intents WHERE idempotency_key=?1",
                    [&intent.idempotency_key],
                    |row| row.get(0),
                )
                .optional()?,
        };
        if let Some(hash) = existing {
            if hash != intent.request_hash {
                return Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"));
            }
            return Ok(false);
        }
        if intent.deadline <= decision.decision_at
            || intent.policy_revision != decision.policy_revision
        {
            return Err(StoreError::Invalid("INVALID_INTENT_DEADLINE_OR_POLICY"));
        }
        if intent.plan.quantity <= Decimal::ZERO
            || intent.plan.limit_price <= Decimal::ZERO
            || intent.plan.planned_loss_usd < Decimal::ZERO
            || !matches!(intent.plan.side.as_str(), "buy" | "sell")
            || intent.plan.stop_price.is_some_and(|p| p <= Decimal::ZERO)
        {
            return Err(StoreError::Invalid("INVALID_INTENT_PLAN"));
        }
        let definition: (String, String, String) = match scope {
            Some((installation, account)) => tx.query_row(
                "SELECT v.definition_id,v.version,v.parameters_digest
                 FROM strategy_instances i JOIN strategy_definition_versions v
                   ON v.installation_id=i.installation_id
                  AND v.account_key=i.account_key
                  AND v.definition_id=i.definition_id
                  AND v.version=i.definition_version
                  AND v.parameters_digest=i.parameters_digest
                 WHERE i.installation_id=?1 AND i.account_key=?2 AND i.instance_id=?3",
                params![installation, account, intent.plan.strategy_instance],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?,
            None => tx.query_row(
                "SELECT v.definition_id,v.version,v.parameters_digest
                 FROM strategy_instances i JOIN strategy_definition_versions v
                   ON v.definition_id=i.definition_id
                  AND v.version=i.definition_version
                  AND v.parameters_digest=i.parameters_digest
                 WHERE i.instance_id=?1",
                [&intent.plan.strategy_instance],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?,
        };
        if definition.0 != intent.plan.strategy_definition_id
            || definition.1 != intent.plan.strategy_definition_version
            || definition.2 != intent.plan.strategy_parameters_digest
        {
            return Err(StoreError::Invalid("STRATEGY_VERSION_CHANGED"));
        }
        if !strategy_owner_matches_plan(
            decision.instance_id.as_deref(),
            &intent.plan.strategy_instance,
        ) {
            return Err(StoreError::Invalid("STRATEGY_OWNER_MISMATCH"));
        }
        let claimed_strategy = decision.decision_id.starts_with("strategy:");
        if claimed_strategy {
            let prior: Option<(Option<String>, String, i64)> = match scope {
                Some((installation, account)) => tx.query_row(
                    "SELECT instance_id,input_digest,policy_revision FROM decisions
                     WHERE installation_id=?1 AND account_key=?2 AND decision_id=?3",
                    params![installation, account, decision.decision_id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                ).optional()?,
                None => tx.query_row(
                    "SELECT instance_id,input_digest,policy_revision FROM decisions WHERE decision_id=?1",
                    [&decision.decision_id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                ).optional()?,
            };
            let (owner, digest, revision) =
                prior.ok_or(StoreError::Invalid("STRATEGY_CLAIM_MISSING"))?;
            if owner != decision.instance_id
                || digest != decision.input_digest
                || revision != decision.policy_revision
            {
                return Err(StoreError::Invalid("STRATEGY_CLAIM_MISMATCH"));
            }
            let already_bound: bool = match scope {
                Some((installation, account)) => tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM intents
                     WHERE installation_id=?1 AND account_key=?2 AND decision_id=?3)",
                    params![installation, account, decision.decision_id],
                    |r| r.get(0),
                )?,
                None => tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM intents WHERE decision_id=?1)",
                    [&decision.decision_id],
                    |r| r.get(0),
                )?,
            };
            if already_bound {
                return Err(StoreError::Invalid("STRATEGY_DECISION_ALREADY_BOUND"));
            }
        } else {
            match scope {
                Some((installation, account)) => {
                    tx.execute(
                        "INSERT INTO decisions(installation_id,account_key,decision_id,instance_id,decision_at,input_digest,policy_revision,code_version)
                         VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                        params![
                            installation, account, decision.decision_id, decision.instance_id,
                            decision.decision_at, decision.input_digest,
                            decision.policy_revision, decision.code_version
                        ],
                    )?;
                }
                None => {
                    tx.execute(
                        "INSERT INTO decisions VALUES(?1,?2,?3,?4,?5,?6)",
                        params![
                            decision.decision_id,
                            decision.instance_id,
                            decision.decision_at,
                            decision.input_digest,
                            decision.policy_revision,
                            decision.code_version
                        ],
                    )?;
                }
            }
        }
        match scope {
            Some((installation, account)) => {
                tx.execute(
                    "INSERT INTO intents(
                     installation_id,account_key,intent_id,idempotency_key,request_hash,decision_id,
                     state,revision,client_order_id,deadline,plan_hash,policy_revision,
                     risk_increase,instrument_id,created_at,updated_at)
                     VALUES(?1,?2,?3,?4,?5,?6,'PREPARED',0,?7,?8,?9,?10,?11,?12,?13,?13)",
                    params![
                        installation,
                        account,
                        intent.intent_id,
                        intent.idempotency_key,
                        intent.request_hash,
                        decision.decision_id,
                        intent.client_order_id,
                        intent.deadline,
                        intent.plan_hash,
                        intent.policy_revision,
                        intent.risk_increase,
                        intent.instrument_id,
                        decision.decision_at
                    ],
                )?;
                tx.execute(
                    "INSERT INTO intent_plans VALUES(
                     ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)",
                    params![
                        installation,
                        account,
                        intent.intent_id,
                        intent.plan.operation,
                        intent.plan.side,
                        exact(intent.plan.quantity),
                        exact(intent.plan.limit_price),
                        intent.plan.stop_price.map(exact),
                        intent.plan.strategy_instance,
                        exact(intent.plan.planned_loss_usd),
                        intent.plan.source_generation,
                        intent.plan.position_side,
                        intent.plan.private_source_generation,
                        exact(intent.plan.contract_value),
                        intent.plan.inverse,
                        intent.plan.settlement_currency,
                        intent.plan.strategy_definition_id,
                        intent.plan.strategy_definition_version,
                        intent.plan.strategy_parameters_digest
                    ],
                )?;
            }
            None => {
                tx.execute(
                    "INSERT INTO intents(intent_id,idempotency_key,request_hash,decision_id,state,revision,client_order_id,deadline,plan_hash,policy_revision,risk_increase,instrument_id,created_at,updated_at)
                     VALUES(?1,?2,?3,?4,'PREPARED',0,?5,?6,?7,?8,?9,?10,?11,?11)",
                    params![
                        intent.intent_id, intent.idempotency_key, intent.request_hash,
                        decision.decision_id, intent.client_order_id, intent.deadline,
                        intent.plan_hash, intent.policy_revision, intent.risk_increase,
                        intent.instrument_id, decision.decision_at
                    ],
                )?;
                tx.execute(
                    "INSERT INTO intent_plans VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
                    params![
                        intent.intent_id, intent.plan.operation, intent.plan.side,
                        exact(intent.plan.quantity), exact(intent.plan.limit_price),
                        intent.plan.stop_price.map(exact), intent.plan.strategy_instance,
                        exact(intent.plan.planned_loss_usd), intent.plan.source_generation,
                        intent.plan.position_side, intent.plan.private_source_generation,
                        exact(intent.plan.contract_value), intent.plan.inverse,
                        intent.plan.settlement_currency, intent.plan.strategy_definition_id,
                        intent.plan.strategy_definition_version, intent.plan.strategy_parameters_digest
                    ],
                )?;
            }
        }
        if claimed_strategy {
            match scope {
                Some((installation, account)) => {
                    tx.execute(
                        "UPDATE strategy_instances SET last_decision_key=?4
                         WHERE installation_id=?1 AND account_key=?2 AND instance_id=?3",
                        params![
                            installation,
                            account,
                            intent.plan.strategy_instance,
                            intent.idempotency_key
                        ],
                    )?;
                }
                None => {
                    tx.execute(
                        "UPDATE strategy_instances SET last_decision_key=?2 WHERE instance_id=?1",
                        params![intent.plan.strategy_instance, intent.idempotency_key],
                    )?;
                }
            }
        }
        Ok(true)
    }

    pub fn prepare(
        &mut self,
        decision: &Decision,
        intent: &NewIntent,
    ) -> StoreResult<StoredIntent> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let inserted = Self::prepare_sql_transaction(&tx, None, decision, intent)?;
        tx.commit()?;
        // The original idempotency contract returns the FIRST stored intent,
        // even when a repeated request uses a different proposed intent_id.
        if inserted {
            self.intent(&intent.intent_id)?
        } else {
            self.by_idempotency(&intent.idempotency_key)?
        }
        .ok_or(StoreError::Invalid("INTENT_MISSING"))
    }
    pub fn intent(&self, id: &str) -> StoreResult<Option<StoredIntent>> {
        self.find_intent("intent_id", id)
    }
    pub fn by_idempotency(&self, key: &str) -> StoreResult<Option<StoredIntent>> {
        self.find_intent("idempotency_key", key)
    }
    pub fn by_client_order_id(&self, key: &str) -> StoreResult<Option<StoredIntent>> {
        self.find_intent("client_order_id", key)
    }
    pub fn operation(&self, id: &str) -> StoreResult<Option<OperationReceipt>> {
        Self::operation_sql(&self.connection, None, id)
    }

    /// Return a bounded, read-only projection of this account's existing
    /// protection-cleanup receipts. The kind/time/id index keeps the scan
    /// within the cleanup receipt range; the LEFT JOIN preserves orphaned
    /// receipts instead of hiding an identity mismatch.
    pub fn recent_cleanup_diagnostics(
        &self,
        limit: usize,
    ) -> StoreResult<Vec<ProtectionCleanupOperationReceipt>> {
        Self::recent_cleanup_diagnostics_sql(&self.connection, None, limit)
    }
    // Durable marker is committed before any cancel/amend exchange mutation.
    pub fn begin_operation(
        &mut self,
        id: &str,
        hash: &str,
        kind: &str,
        now: i64,
    ) -> StoreResult<bool> {
        Self::begin_operation_sql(&self.connection, None, id, hash, kind, now)
    }
    pub fn operation_receipt(
        &mut self,
        id: &str,
        hash: &str,
        kind: &str,
        result: &str,
        now: i64,
    ) -> StoreResult<OperationReceipt> {
        Self::operation_receipt_sql(&self.connection, None, id, hash, kind, result, now)
    }
    pub(super) fn find_intent(&self, column: &str, key: &str) -> StoreResult<Option<StoredIntent>> {
        Self::find_intent_sql(&self.connection, None, column, key)
    }

    /// Decode the ORIGINAL stored intent and its full original SQL plan,
    /// under the same v2 or v6 account scope as the caller's transaction.
    /// A partial row projection is not sufficient for an outcome/POST fence.
    pub(crate) fn find_intent_sql(
        db: &rusqlite::Connection,
        scope: Option<(&str, &str)>,
        column: &str,
        key: &str,
    ) -> StoreResult<Option<StoredIntent>> {
        if !matches!(
            column,
            "intent_id" | "idempotency_key" | "client_order_id" | "exchange_order_id"
        ) {
            return Err(StoreError::Invalid("INTENT_IDENTITY_COLUMN_INVALID"));
        }
        let sql = if scope.is_some() {
            format!(
                "SELECT intent_id,idempotency_key,request_hash,client_order_id,state,revision,deadline,plan_hash,policy_revision,risk_increase,instrument_id,exchange_order_id,incident
                 FROM intents WHERE {column}=?1 AND installation_id=?2 AND account_key=?3"
            )
        } else {
            format!(
                "SELECT intent_id,idempotency_key,request_hash,client_order_id,state,revision,deadline,plan_hash,policy_revision,risk_increase,instrument_id,exchange_order_id,incident
                 FROM intents WHERE {column}=?1"
            )
        };
        let mut args = vec![rusqlite::types::Value::Text(key.to_owned())];
        if let Some((installation, account)) = scope {
            if installation.is_empty() || account.is_empty() {
                return Err(StoreError::Invalid("UNIFIED_INTENT_SCOPE_INVALID"));
            }
            args.push(rusqlite::types::Value::Text(installation.to_owned()));
            args.push(rusqlite::types::Value::Text(account.to_owned()));
        }
        let raw = db
            .query_row(&sql, rusqlite::params_from_iter(args.iter()), |r| {
                Ok((
                    StoredIntent {
                        plan: IntentPlan::default(),
                        intent_id: r.get(0)?,
                        idempotency_key: r.get(1)?,
                        request_hash: r.get(2)?,
                        client_order_id: r.get(3)?,
                        state: IntentState::Prepared,
                        revision: r.get(5)?,
                        deadline: r.get(6)?,
                        plan_hash: r.get(7)?,
                        policy_revision: r.get(8)?,
                        risk_increase: r.get(9)?,
                        instrument_id: r.get(10)?,
                        exchange_order_id: r.get(11)?,
                        incident: r.get(12)?,
                    },
                    r.get::<_, String>(4)?,
                ))
            })
            .optional()?;
        raw.map(|(mut intent, state)| {
            intent.state = IntentState::parse(&state)?;
            intent.plan = Self::plan_sql(db, scope, &intent.intent_id)?;
            Ok(intent)
        })
        .transpose()
    }
    // The original native plan decoder, shared by per-account v2 and scoped v6
    // SQL. Reserve validates the entire original plan under its caller's lock.
    pub(crate) fn plan_sql(
        db: &rusqlite::Connection,
        scope: Option<(&str, &str)>,
        id: &str,
    ) -> StoreResult<IntentPlan> {
        use rusqlite::{params_from_iter, types::Value};
        let sql = if scope.is_some() {
            "SELECT operation,side,quantity,limit_price,stop_price,strategy_instance,planned_loss_usd,source_generation,position_side,private_source_generation,contract_value,inverse,settlement_currency,strategy_definition_id,strategy_definition_version,strategy_parameters_digest FROM intent_plans WHERE intent_id=?1 AND installation_id=?2 AND account_key=?3"
        } else {
            "SELECT operation,side,quantity,limit_price,stop_price,strategy_instance,planned_loss_usd,source_generation,position_side,private_source_generation,contract_value,inverse,settlement_currency,strategy_definition_id,strategy_definition_version,strategy_parameters_digest FROM intent_plans WHERE intent_id=?1"
        };
        let mut values = vec![Value::Text(id.to_owned())];
        if let Some((installation, account)) = scope {
            values.push(Value::Text(installation.to_owned()));
            values.push(Value::Text(account.to_owned()));
        }
        let (mut p, q, price, stop, loss, cv) =
            db.query_row(sql, params_from_iter(values.iter()), |r| {
                Ok((
                    IntentPlan {
                        operation: r.get(0)?,
                        side: r.get(1)?,
                        strategy_instance: r.get(5)?,
                        source_generation: r.get(7)?,
                        position_side: r.get(8)?,
                        private_source_generation: r.get(9)?,
                        inverse: r.get(11)?,
                        settlement_currency: r.get(12)?,
                        strategy_definition_id: r.get(13)?,
                        strategy_definition_version: r.get(14)?,
                        strategy_parameters_digest: r.get(15)?,
                        ..Default::default()
                    },
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, Option<String>>(4)?,
                    r.get::<_, String>(6)?,
                    r.get::<_, String>(10)?,
                ))
            })?;
        p.contract_value = decimal(cv)?;
        p.quantity = decimal(q)?;
        p.limit_price = decimal(price)?;
        p.stop_price = stop.map(decimal).transpose()?;
        p.planned_loss_usd = decimal(loss)?;
        Ok(p)
    }

    pub fn reserve(&mut self, receipt: &CentralReceipt, now: i64) -> StoreResult<()> {
        if receipt.account_key != self.meta.account_key
            || receipt.installation_id != self.meta.installation_id
        {
            return Err(StoreError::Invalid("CENTRAL_RECEIPT_MISMATCH"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::reserve_sql_transaction(&tx, None, receipt, now)?;
        tx.commit()?;
        Ok(())
    }

    /// The ORIGINAL AccountStore receipt, strategy and intent state machine,
    /// executed by the caller's single SQL transaction for either the
    /// legacy v2 account database or one scope in the v6 financial database.
    /// Does not create a Connection, independently COMMIT, or grant venue POST.
    pub(crate) fn reserve_sql_transaction(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        receipt: &CentralReceipt,
        now: i64,
    ) -> StoreResult<()> {
        use rusqlite::{params_from_iter, types::Value};
        if let Some((installation, account)) = scope
            && (installation.is_empty()
                || account.is_empty()
                || receipt.installation_id != installation
                || receipt.account_key != account)
        {
            return Err(StoreError::Invalid("CENTRAL_RECEIPT_MISMATCH"));
        }
        let scoped_values = |mut values: Vec<Value>| {
            if let Some((installation, account)) = scope {
                values.push(Value::Text(installation.to_owned()));
                values.push(Value::Text(account.to_owned()));
            }
            values
        };
        let intent_sql = if scope.is_some() {
            "SELECT state,plan_hash,policy_revision FROM intents
             WHERE intent_id=?1 AND installation_id=?2 AND account_key=?3"
        } else {
            "SELECT state,plan_hash,policy_revision FROM intents WHERE intent_id=?1"
        };
        let values = scoped_values(vec![Value::Text(receipt.intent_id.clone())]);
        let original: Option<(String, String, i64)> = tx
            .query_row(intent_sql, params_from_iter(values.iter()), |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .optional()?;
        let (state, plan_hash, policy_revision) =
            original.ok_or(StoreError::Invalid("INTENT_MISSING"))?;
        let state = IntentState::parse(&state)?;
        let plan = Self::plan_sql(tx, scope, &receipt.intent_id)?;
        if receipt.plan_hash != plan_hash
            || receipt.policy_revision != policy_revision
            || receipt.receipt_hash.is_empty()
            || receipt.requested_loss_usd < Decimal::ZERO
            || receipt.conversion_digest.is_empty()
        {
            return Err(StoreError::Invalid("CENTRAL_RECEIPT_MISMATCH"));
        }
        if state == IntentState::Reserved {
            let hash_sql = if scope.is_some() {
                "SELECT receipt_hash FROM risk_receipts
                 WHERE reservation_key=?1 AND installation_id=?2 AND account_key=?3"
            } else {
                "SELECT receipt_hash FROM risk_receipts WHERE reservation_key=?1"
            };
            let values = scoped_values(vec![Value::Text(receipt.reservation_key.clone())]);
            let saved: String =
                tx.query_row(hash_sql, params_from_iter(values.iter()), |r| r.get(0))?;
            return if saved == receipt.receipt_hash {
                Ok(())
            } else {
                Err(StoreError::Invalid("CENTRAL_RECEIPT_CONFLICT"))
            };
        }
        if state != IntentState::Prepared {
            return Err(StoreError::Invalid("INVALID_INTENT_TRANSITION"));
        }
        let decision_sql = if scope.is_some() {
            "SELECT d.instance_id FROM decisions d JOIN intents i
                ON d.decision_id=i.decision_id
               AND d.installation_id=i.installation_id
               AND d.account_key=i.account_key
             WHERE i.intent_id=?1 AND i.installation_id=?2 AND i.account_key=?3"
        } else {
            "SELECT d.instance_id FROM decisions d JOIN intents i USING(decision_id)
             WHERE i.intent_id=?1"
        };
        let owner: Option<String> = tx
            .query_row(decision_sql, params_from_iter(values.iter()), |r| {
                r.get::<_, Option<String>>(0)
            })
            .optional()?
            .flatten();
        if owner != receipt.instance_id
            || !strategy_owner_matches_plan(owner.as_deref(), &plan.strategy_instance)
        {
            return Err(StoreError::Invalid("CENTRAL_INSTANCE_MISMATCH"));
        }
        let insert_sql = if scope.is_some() {
            "INSERT INTO risk_receipts(
             installation_id,account_key,reservation_key,intent_id,instance_id,plan_hash,
             policy_revision,receipt_hash,requested_loss_usd,conversion_digest,state)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,'HELD')"
        } else {
            "INSERT INTO risk_receipts(
             reservation_key,intent_id,account_key,installation_id,instance_id,plan_hash,
             policy_revision,receipt_hash,requested_loss_usd,conversion_digest,state)
             VALUES (?3,?4,?2,?1,?5,?6,?7,?8,?9,?10,'HELD')"
        };
        tx.execute(
            insert_sql,
            params![
                receipt.installation_id,
                receipt.account_key,
                receipt.reservation_key,
                receipt.intent_id,
                receipt.instance_id,
                receipt.plan_hash,
                receipt.policy_revision,
                receipt.receipt_hash,
                exact(receipt.requested_loss_usd),
                receipt.conversion_digest
            ],
        )?;
        let update_sql = if scope.is_some() {
            "UPDATE intents SET state='RESERVED',revision=revision+1,updated_at=?2
             WHERE intent_id=?1 AND state='PREPARED'
               AND installation_id=?3 AND account_key=?4"
        } else {
            "UPDATE intents SET state='RESERVED',revision=revision+1,updated_at=?2
             WHERE intent_id=?1 AND state='PREPARED'"
        };
        let values = scoped_values(vec![
            Value::Text(receipt.intent_id.clone()),
            Value::Integer(now),
        ]);
        if tx.execute(update_sql, params_from_iter(values.iter()))? != 1 {
            return Err(StoreError::Invalid("INVALID_INTENT_TRANSITION"));
        }
        Ok(())
    }
    /// Read-only proof of the durable BEGIN_POST fence and its coherent
    /// SQL state. A venue ACK moves the reservation from POSTED to OPEN;
    /// requiring POSTED *after* that transition would reject a real ACK.
    /// Neither ACK nor OPEN by itself proves that BEGIN_POST was committed.
    pub fn post_fence_evidence(&self, id: &str) -> StoreResult<(i64, String)> {
        Self::post_fence_evidence_sql(
            &self.connection,
            &self.meta.installation_id,
            &self.meta.account_key,
            id,
            None,
        )
    }

    /// Existing ORIGINAL AccountStore durable POST witness for both old
    /// account-local v2 and unified v6 finance. V6 binds all identity scopes
    /// including reservation; never infer authorization from a different
    /// account's identical venue or intent ID.
    pub(crate) fn post_fence_evidence_sql(
        db: &rusqlite::Connection,
        installation: &str,
        account: &str,
        id: &str,
        reservation: Option<&str>,
    ) -> StoreResult<(i64, String)> {
        let recorded: Option<(Option<i64>, String, String, Option<String>)> =
            if let Some(reservation) = reservation {
                db.query_row(
                    "SELECT i.begin_post_at,r.state,i.state,i.exchange_order_id
                     FROM intents i JOIN risk_receipts r
                       ON r.installation_id=i.installation_id AND r.account_key=i.account_key
                      AND r.intent_id=i.intent_id AND r.plan_hash=i.plan_hash
                     WHERE i.installation_id=?1 AND i.account_key=?2 AND i.intent_id=?3
                       AND r.reservation_key=?4 AND i.risk_increase=1",
                    params![installation, account, id, reservation],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?
            } else {
                db.query_row(
                    "SELECT i.begin_post_at,r.state,i.state,i.exchange_order_id
                     FROM intents i JOIN risk_receipts r
                       ON r.intent_id=i.intent_id AND r.plan_hash=i.plan_hash
                      AND r.account_key=?2 AND r.installation_id=?3
                     WHERE i.intent_id=?1 AND i.risk_increase=1",
                    params![id, account, installation],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?
            };
        match recorded {
            Some((Some(at), state, intent_state, order_id))
                if verified_durable_post_fence(at, &state, &intent_state, order_id.as_deref()) =>
            {
                Ok((at, state))
            }
            _ => Err(StoreError::Invalid("POST_FENCE_NOT_DURABLY_COMMITTED")),
        }
    }
    // Returns true exactly once. Only the owner commits; neither a queued
    // result nor an uncommitted RiskBook decision permits venue submission.
    pub fn begin_post(&mut self, id: &str, policy_revision: i64, now: i64) -> StoreResult<bool> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let permitted =
            Self::begin_post_sql_transaction(&tx, None, id, policy_revision, now, None)?;
        if permitted {
            tx.commit()?;
        }
        Ok(permitted)
    }

    /// SAME production AccountStore validation and durable receipt transition
    /// for v2 single-account and v6 scoped multi-account SQLite.
    ///
    /// A caller owns the BEGIN IMMEDIATE/COMMIT; this function never opens
    /// another connection or transaction. It is NOT venue permission until
    /// the finance owner has committed *both* account and campaign states.
    pub(crate) fn begin_post_sql_transaction(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        id: &str,
        policy_revision: i64,
        now: i64,
        required_reservation: Option<&str>,
    ) -> StoreResult<bool> {
        use rusqlite::{params_from_iter, types::Value};

        // Legacy account-local SQL intentionally has no installation/account
        // columns. Finance-v6 always requires BOTH original scope keys.
        fn bind(mut base: Vec<Value>, scope: Option<(&str, &str)>) -> Vec<Value> {
            if let Some((installation, account)) = scope {
                base.push(Value::Text(installation.to_owned()));
                base.push(Value::Text(account.to_owned()));
            }
            base
        }
        if id.is_empty()
            || now <= 0
            || policy_revision <= 0
            || (scope.is_some()
                && (required_reservation.is_none()
                    || scope.is_some_and(|(installation, account)| {
                        installation.is_empty() || account.is_empty()
                    })))
        {
            return Err(StoreError::Invalid("ACCOUNT_POST_INPUT_INVALID"));
        }
        let intent_sql = if scope.is_some() {
            "SELECT state,deadline,policy_revision,risk_increase FROM intents
             WHERE intent_id=?1 AND installation_id=?2 AND account_key=?3"
        } else {
            "SELECT state,deadline,policy_revision,risk_increase FROM intents WHERE intent_id=?1"
        };
        let intent_bind = bind(vec![Value::Text(id.to_owned())], scope);
        let (state, deadline, revision, increase): (String, i64, i64, bool) =
            tx.query_row(intent_sql, params_from_iter(intent_bind.iter()), |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })?;
        if state != "RESERVED" {
            return Ok(false);
        }
        if now >= deadline {
            return Err(StoreError::Invalid("COMMAND_EXPIRED"));
        }
        let policy_sql = if scope.is_some() {
            "SELECT revision,paused FROM account_policy
             WHERE singleton=1 AND installation_id=?1 AND account_key=?2"
        } else {
            "SELECT revision,paused FROM account_policy WHERE singleton=1"
        };
        let scope_bind = bind(Vec::new(), scope);
        let (current, paused): (i64, bool) =
            tx.query_row(policy_sql, params_from_iter(scope_bind.iter()), |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?;
        if revision != policy_revision || current != revision {
            return Err(StoreError::Invalid("POLICY_CHANGED"));
        }
        if increase {
            let strategy_sql = if scope.is_some() {
                "SELECT EXISTS(SELECT 1 FROM intent_plans p
                 JOIN strategy_instances i
                   ON i.installation_id=p.installation_id
                  AND i.account_key=p.account_key
                  AND i.instance_id=p.strategy_instance
                 WHERE p.intent_id=?1 AND p.installation_id=?2 AND p.account_key=?3
                   AND i.definition_id=p.strategy_definition_id
                   AND i.definition_version=p.strategy_definition_version
                   AND i.parameters_digest=p.strategy_parameters_digest
                   AND (i.enabled=1 OR i.instance_id='manual'))"
            } else {
                "SELECT EXISTS(SELECT 1 FROM intent_plans p
                 JOIN strategy_instances i ON i.instance_id=p.strategy_instance
                 WHERE p.intent_id=?1 AND i.definition_id=p.strategy_definition_id
                   AND i.definition_version=p.strategy_definition_version
                   AND i.parameters_digest=p.strategy_parameters_digest
                   AND (i.enabled=1 OR i.instance_id='manual'))"
            };
            let same: bool =
                tx.query_row(strategy_sql, params_from_iter(intent_bind.iter()), |r| {
                    r.get(0)
                })?;
            if !same {
                return Err(StoreError::Invalid("STRATEGY_VERSION_CHANGED"));
            }
            let stop_sql = if scope.is_some() {
                "SELECT COUNT(*) FROM risk_stops WHERE resolution IS NULL
                 AND installation_id=?1 AND account_key=?2"
            } else {
                "SELECT COUNT(*) FROM risk_stops WHERE resolution IS NULL"
            };
            let stops: i64 =
                tx.query_row(stop_sql, params_from_iter(scope_bind.iter()), |r| r.get(0))?;
            if paused || stops > 0 {
                return Err(StoreError::Invalid("RISK_INCREASE_STOPPED"));
            }
            let recovery_sql = if scope.is_some() {
                "SELECT requires_reconcile FROM storage_recovery WHERE singleton=1
                 AND installation_id=?1 AND account_key=?2"
            } else {
                "SELECT requires_reconcile FROM storage_recovery"
            };
            let recovery: bool =
                tx.query_row(recovery_sql, params_from_iter(scope_bind.iter()), |r| {
                    r.get(0)
                })?;
            if recovery {
                return Err(StoreError::Invalid("STORAGE_RECOVERY_INCOMPLETE"));
            }
            let unknown_sql = if scope.is_some() {
                "SELECT COUNT(*) FROM intents WHERE state IN('POSTING','UNKNOWN')
                 AND installation_id=?1 AND account_key=?2"
            } else {
                "SELECT COUNT(*) FROM intents WHERE state IN('POSTING','UNKNOWN')"
            };
            let unknown: i64 =
                tx.query_row(unknown_sql, params_from_iter(scope_bind.iter()), |r| {
                    r.get(0)
                })?;
            if unknown > 0 {
                return Err(StoreError::Invalid("ACCOUNT_UNKNOWN_EXPOSURE"));
            }
        }
        if let Some(reservation) = required_reservation {
            // Scoped finance requires the ORIGINAL AccountStore receipt,
            // never an unattested RiskBook-only HELD row.
            let sql = "SELECT COUNT(*) FROM risk_receipts
                       WHERE installation_id=?1 AND account_key=?2
                         AND intent_id=?3 AND reservation_key=?4 AND state='HELD'
                         AND plan_hash=(SELECT plan_hash FROM intents
                           WHERE installation_id=?1 AND account_key=?2 AND intent_id=?3)";
            let (installation, account) =
                scope.ok_or(StoreError::Invalid("ACCOUNT_POST_SCOPE_REQUIRED"))?;
            let count: i64 = tx.query_row(
                sql,
                rusqlite::params![installation, account, id, reservation],
                |r| r.get(0),
            )?;
            if count != 1 {
                return Err(StoreError::Invalid("ACCOUNT_POST_ORIGINAL_RECEIPT_MISSING"));
            }
        }
        let update_sql = if scope.is_some() {
            "UPDATE intents SET state='POSTING',begin_post_at=?2,updated_at=?2,
             revision=revision+1 WHERE intent_id=?1 AND state='RESERVED'
             AND begin_post_at IS NULL AND installation_id=?3 AND account_key=?4"
        } else {
            "UPDATE intents SET state='POSTING',begin_post_at=?2,updated_at=?2,
             revision=revision+1 WHERE intent_id=?1 AND state='RESERVED'
             AND begin_post_at IS NULL"
        };
        let update_bind = bind(vec![Value::Text(id.to_owned()), Value::Integer(now)], scope);
        if tx.execute(update_sql, params_from_iter(update_bind.iter()))? != 1 {
            return Err(StoreError::Invalid("ACCOUNT_POST_INTENT_CAS_FAILED"));
        }
        let receipt_sql = if scope.is_some() {
            "UPDATE risk_receipts SET state='POSTED'
             WHERE intent_id=?1 AND installation_id=?2 AND account_key=?3
               AND state='HELD'"
        } else {
            "UPDATE risk_receipts SET state='POSTED' WHERE intent_id=?1 AND state='HELD'"
        };
        if tx.execute(receipt_sql, params_from_iter(intent_bind.iter()))? != 1 {
            return Err(StoreError::Invalid("ACCOUNT_POST_RECEIPT_CAS_FAILED"));
        }
        Ok(true)
    }
    // Exchange readback is authoritative; a timeout or negative lookup is UNKNOWN.
    pub fn record_outcome(
        &mut self,
        id: &str,
        state: IntentState,
        exchange_order_id: Option<&str>,
        evidence: &str,
        now: i64,
    ) -> StoreResult<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::record_outcome_sql_transaction(
            &tx,
            None,
            id,
            state,
            exchange_order_id,
            evidence,
            now,
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Original state transition + venue readback + terminal receipt, using
    /// the same typed source decoder and ONE caller-owned SQL COMMIT.
    /// Scope every SQL row on installation-v6; no venue call occurs here.
    pub(crate) fn record_outcome_sql_transaction(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        id: &str,
        state: IntentState,
        exchange_order_id: Option<&str>,
        evidence: &str,
        now: i64,
    ) -> StoreResult<()> {
        let original = Self::find_intent_sql(tx, scope, "intent_id", id)?
            .ok_or(StoreError::Invalid("INTENT_MISSING"))?;
        let allowed = match original.state {
            IntentState::Posting | IntentState::Unknown => matches!(
                state,
                IntentState::Unknown
                    | IntentState::Acknowledged
                    | IntentState::PartiallyFilled
                    | IntentState::Filled
                    | IntentState::Cancelled
                    | IntentState::Rejected
            ),
            IntentState::Acknowledged => matches!(
                state,
                IntentState::Acknowledged
                    | IntentState::PartiallyFilled
                    | IntentState::Filled
                    | IntentState::Cancelled
            ),
            IntentState::PartiallyFilled => matches!(
                state,
                IntentState::PartiallyFilled | IntentState::Filled | IntentState::Cancelled
            ),
            _ => original.state == state,
        };
        if !allowed || evidence.is_empty() {
            return Err(StoreError::Invalid("INVALID_INTENT_TRANSITION"));
        }
        if original
            .exchange_order_id
            .as_deref()
            .is_some_and(|old| Some(old) != exchange_order_id)
        {
            return Err(StoreError::Invalid("EXCHANGE_ID_CONFLICT"));
        }
        let changed = match scope {
            Some((installation, account)) => tx.execute(
                "UPDATE intents SET state=?2,exchange_order_id=COALESCE(exchange_order_id,?3),
                 updated_at=?4,revision=revision+1
                 WHERE intent_id=?1 AND installation_id=?5 AND account_key=?6",
                params![
                    id,
                    state.as_str(),
                    exchange_order_id,
                    now,
                    installation,
                    account
                ],
            )?,
            None => tx.execute(
                "UPDATE intents SET state=?2,exchange_order_id=COALESCE(exchange_order_id,?3),
                 updated_at=?4,revision=revision+1 WHERE intent_id=?1",
                params![id, state.as_str(), exchange_order_id, now],
            )?,
        };
        if changed != 1 {
            return Err(StoreError::Invalid("ACCOUNT_OUTCOME_INTENT_CAS_FAILED"));
        }
        let risk_state = if state == IntentState::Unknown {
            "UNKNOWN"
        } else if original.risk_increase
            && matches!(state, IntentState::Filled | IntentState::PartiallyFilled)
        {
            "OPEN"
        } else if state.terminal() {
            "TERMINAL_PENDING_CENTRAL"
        } else {
            "OPEN"
        };
        let risk_rows = match scope {
            Some((installation, account)) => tx.execute(
                "UPDATE risk_receipts SET state=?2
                 WHERE intent_id=?1 AND installation_id=?3 AND account_key=?4",
                params![id, risk_state, installation, account],
            )?,
            None => tx.execute(
                "UPDATE risk_receipts SET state=?2 WHERE intent_id=?1",
                params![id, risk_state],
            )?,
        };
        // A v6 increase with no ORIGINAL AccountStore risk receipt must never
        // acknowledge an order while the campaign budget is unproven.
        if scope.is_some() && original.risk_increase && risk_rows != 1 {
            return Err(StoreError::Invalid("UNIFIED_OUTCOME_RISK_RECEIPT_MISSING"));
        }
        if state.terminal() {
            let operation = format!("terminal:{id}");
            match scope {
                Some((installation, account)) => {
                    let previous: Option<(String, String, String)> = tx
                        .query_row(
                            "SELECT kind,request_hash,result_ref FROM operation_receipts
                         WHERE installation_id=?1 AND account_key=?2 AND operation_id=?3",
                            params![installation, account, operation],
                            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                        )
                        .optional()?;
                    if let Some((kind, hash, result)) = previous {
                        if kind != "TERMINAL" || hash != original.request_hash || result != evidence
                        {
                            return Err(StoreError::Invalid("UNIFIED_TERMINAL_RECEIPT_CONFLICT"));
                        }
                    } else {
                        tx.execute(
                            "INSERT INTO operation_receipts(
                             installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at)
                             VALUES(?1,?2,?3,'TERMINAL',?4,?5,?6)",
                            params![
                                installation, account, operation,
                                original.request_hash, evidence, now
                            ],
                        )?;
                    }
                }
                None => {
                    tx.execute(
                        "INSERT INTO operation_receipts VALUES(?1,'TERMINAL',?2,?3,?4)
                         ON CONFLICT(operation_id) DO NOTHING",
                        params![operation, original.request_hash, evidence, now],
                    )?;
                }
            }
        }
        Ok(())
    }

    pub fn recover_posting(&mut self, now: i64) -> StoreResult<usize> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let recovered = Self::recover_posting_sql_transaction(&tx, None, now)?;
        tx.commit()?;
        Ok(recovered)
    }

    /// Native recovery: only POSTING rows under the requested account become
    /// UNKNOWN. Verify the ORIGINAL durable POST fence and matching source
    /// receipt BEFORE any change. Never treat a missing/corrupt receipt as
    /// empty budget or silently overwrite an older UNKNOWN/terminal receipt.
    /// The ledger intent and original receipt transition in ONE SQL COMMIT.
    pub(crate) fn recover_posting_sql_transaction(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        now: i64,
    ) -> StoreResult<usize> {
        let changed = match scope {
            Some((installation, account)) => {
                if installation.is_empty() || account.is_empty() {
                    return Err(StoreError::Invalid("UNIFIED_RECOVERY_SCOPE_INVALID"));
                }
                // A POSTING intent means there was exactly one potential
                // exchange POST. A missing SQL fence or original receipt is
                // an incident, not permission to recover/release the budget.
                let broken: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM intents i
                     LEFT JOIN risk_receipts r
                       ON r.installation_id=i.installation_id
                      AND r.account_key=i.account_key
                      AND r.intent_id=i.intent_id
                     WHERE i.installation_id=?1 AND i.account_key=?2
                       AND i.state='POSTING'
                       AND (i.begin_post_at IS NULL OR i.begin_post_at<=0
                         OR (i.risk_increase=1 AND (
                              r.intent_id IS NULL OR r.state!='POSTED'
                              OR r.plan_hash!=i.plan_hash
                              OR r.policy_revision!=i.policy_revision)))",
                    params![installation, account],
                    |r| r.get(0),
                )?;
                if broken != 0 {
                    return Err(StoreError::Invalid(
                        "UNIFIED_RECOVERY_ORIGINAL_POST_UNPROVEN",
                    ));
                }
                let originals: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM intents WHERE installation_id=?1
                     AND account_key=?2 AND state='POSTING' AND risk_increase=1",
                    params![installation, account],
                    |r| r.get(0),
                )?;
                // Only receipts for intents that are CURRENTLY POSTING may
                // transition. Previously UNKNOWN/terminal entries retain
                // their exact durable evidence; repeated recovery is no-op.
                let updated_receipts = tx.execute(
                    "UPDATE risk_receipts SET state='UNKNOWN'
                     WHERE installation_id=?1 AND account_key=?2 AND state='POSTED'
                       AND intent_id IN (
                         SELECT intent_id FROM intents
                         WHERE installation_id=?1 AND account_key=?2
                           AND state='POSTING' AND risk_increase=1
                       )",
                    params![installation, account],
                )?;
                if i64::try_from(updated_receipts).ok() != Some(originals) {
                    return Err(StoreError::Invalid("UNIFIED_RECOVERY_RECEIPT_CAS_FAILED"));
                }
                tx.execute(
                    "UPDATE intents SET state='UNKNOWN',updated_at=?1,revision=revision+1
                     WHERE state='POSTING' AND installation_id=?2 AND account_key=?3",
                    params![now, installation, account],
                )?
            }
            None => {
                // Keep the exact existing account-v2 algorithm until cutover.
                // Its SQL owner contains only one account and uses local IDs.
                let n = tx.execute(
                    "UPDATE intents SET state='UNKNOWN',updated_at=?1,revision=revision+1
                     WHERE state='POSTING'",
                    [now],
                )?;
                tx.execute(
                    "UPDATE risk_receipts SET state='UNKNOWN'
                     WHERE intent_id IN(SELECT intent_id FROM intents WHERE state='UNKNOWN')",
                    [],
                )?;
                n
            }
        };
        Ok(changed)
    }

    /// Recover every installed account in ONE financial SQLite transaction.
    /// Installation scope comes from the ORIGINAL account_meta inventory in
    /// the same DB, never from caller-provided account lists or Cloudflare.
    /// Any invalid original POST fence fails the entire installation batch:
    /// the caller's one transaction rollback preserves every other account.
    pub(crate) fn recover_installation_posting_sql_transaction(
        tx: &rusqlite::Transaction<'_>,
        installation: &str,
        now: i64,
    ) -> StoreResult<usize> {
        if installation.is_empty() {
            return Err(StoreError::Invalid("UNIFIED_RECOVERY_SCOPE_INVALID"));
        }
        // Savepoint is within the ONE existing BEGIN IMMEDIATE, not another
        // database or commit. On account B's failure, even a caller that
        // mistakenly commits its outer transaction cannot persist A's
        // recovery or partially clear the original budget receipt.
        tx.execute_batch("SAVEPOINT okx_installation_post_recovery")?;
        let attempted = (|| -> StoreResult<usize> {
            let mut query = tx.prepare(
                "SELECT account_key FROM account_meta
                 WHERE installation_id=?1 ORDER BY account_key",
            )?;
            let accounts = query
                .query_map([installation], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            drop(query);
            if accounts.is_empty() {
                return Err(StoreError::Invalid("UNIFIED_RECOVERY_ACCOUNT_ROSTER_EMPTY"));
            }
            let mut total = 0_usize;
            for account in accounts {
                total = total
                    .checked_add(Self::recover_posting_sql_transaction(
                        tx,
                        Some((installation, &account)),
                        now,
                    )?)
                    .ok_or(StoreError::Invalid("UNIFIED_RECOVERY_COUNT_OVERFLOW"))?;
            }
            Ok(total)
        })();
        match attempted {
            Ok(total) => {
                tx.execute_batch("RELEASE SAVEPOINT okx_installation_post_recovery")?;
                Ok(total)
            }
            Err(error) => {
                tx.execute_batch(
                    "ROLLBACK TO SAVEPOINT okx_installation_post_recovery;
                     RELEASE SAVEPOINT okx_installation_post_recovery",
                )?;
                Err(error)
            }
        }
    }

    /// Bounded combined local account obligations, including terminal
    /// intents whose RiskBook reservations have NOT yet been released.
    pub fn risk_census_intents(
        &self,
        limit: usize,
    ) -> StoreResult<(Vec<StoredIntent>, Vec<String>)> {
        Self::risk_census_intents_sql(&self.connection, None, limit, wall_time_ms())
    }

    pub fn active_intents(&self, limit: usize) -> StoreResult<Vec<StoredIntent>> {
        Self::active_intents_sql(&self.connection, None, limit)
    }
    // Cache an already-authorized revision. This grants no new financial authority.
}

// A transient POSTED receipt is superseded by OPEN after authenticated ACK.
// Both states are admissible ONLY with an earlier persisted BEGIN_POST.
fn verified_durable_post_fence(
    begin_post_at: i64,
    receipt_state: &str,
    intent_state: &str,
    exchange_order_id: Option<&str>,
) -> bool {
    if begin_post_at <= 0 {
        return false;
    }
    match (receipt_state, intent_state) {
        ("POSTED", "POSTING") => exchange_order_id.is_none(),
        ("OPEN", "ACKNOWLEDGED") => exchange_order_id.is_some_and(|id| !id.is_empty()),
        _ => false,
    }
}

/// SQL risk receipt state is read under the existing single AccountStore owner.
#[cfg(test)]
fn risk_receipt_covers_exposure(state: &str) -> bool {
    AccountStore::receipt_covers_exposure(state)
}

#[cfg(test)]
mod active_receipt_tests {
    use super::*;

    #[test]
    fn terminal_diagnostics_native_plan_scopes_before_candidate_limit() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection
            .execute_batch(include_str!("../../../schema/finance-unified-v1.sql"))
            .unwrap();
        let mut statement = connection
            .prepare(&format!(
                "EXPLAIN QUERY PLAN {TERMINAL_RELEASE_DIAGNOSTIC_SCOPED_SQL}"
            ))
            .unwrap();
        let plan = statement
            .query_map(
                params![128, "okx:real:account", "installation_native"],
                |row| row.get::<_, String>(3),
            )
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join(" ");
        assert!(
            TERMINAL_RELEASE_DIAGNOSTIC_SCOPED_SQL
                .contains("WHERE account_key=?2 AND installation_id=?3")
        );
        assert!(plan.contains("MATERIALIZE candidates"), "{plan}");
        assert!(plan.contains("idx_risk_receipts_active"), "{plan}");
        assert!(!plan.contains("SCAN intents"), "{plan}");
    }

    #[test]
    fn terminal_diagnostics_native_join_never_crosses_account_for_reused_ids() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection
            .execute_batch(include_str!("../../../schema/finance-unified-v1.sql"))
            .unwrap();
        // A deliberately independent SQL fixture: same intent and release
        // operation IDs are legal in separate scoped account namespaces.
        connection
            .execute_batch(
                "PRAGMA foreign_keys=OFF;
                 INSERT INTO intents(installation_id,account_key,intent_id,idempotency_key,
                    request_hash,decision_id,state,revision,client_order_id,deadline,
                    plan_hash,policy_revision,risk_increase,instrument_id,created_at,updated_at)
                 VALUES
                  ('installation','account-a','same','key-a','hash','decision-a','FILLED',1,'client-a',1000,'plan',1,1,'BTC-USDT-SWAP',100,100),
                  ('installation','account-b','same','key-b','hash','decision-b','FILLED',1,'client-b',1000,'plan',1,1,'BTC-USDT-SWAP',100,100);
                 INSERT INTO risk_receipts(reservation_key,intent_id,account_key,installation_id,
                    plan_hash,policy_revision,receipt_hash,requested_loss_usd,conversion_digest,state)
                 VALUES
                  ('reserve','same','account-a','installation','plan',1,'receipt-hash','1','proof','RELEASED'),
                  ('reserve','same','account-b','installation','plan',1,'receipt-hash','1','proof','RELEASED');
                 INSERT INTO operation_receipts(installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at)
                 VALUES
                  ('installation','account-a','central-release:reserve','CENTRAL_RELEASE','receipt-hash','ACK',111),
                  ('installation','account-b','central-release:reserve','CENTRAL_RELEASE','receipt-hash','ACK',222);",
            )
            .unwrap();
        let read = |account: &str| -> Vec<(String, Option<i64>)> {
            let mut statement = connection
                .prepare(TERMINAL_RELEASE_DIAGNOSTIC_SCOPED_SQL)
                .unwrap();
            statement
                .query_map(params![128, account, "installation"], |row| {
                    Ok((row.get(0)?, row.get(6)?))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        assert_eq!(read("account-a"), vec![("same".into(), Some(111))]);
        assert_eq!(read("account-b"), vec![("same".into(), Some(222))]);
        assert!(read("account-c").is_empty());
    }

    #[test]
    fn terminal_diagnostics_query_plan_bounds_original_index_before_joins() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection
            .execute_batch(include_str!("../../../schema/account-v2.sql"))
            .unwrap();
        let mut statement = connection
            .prepare(&format!(
                "EXPLAIN QUERY PLAN {TERMINAL_RELEASE_DIAGNOSTIC_SQL}"
            ))
            .unwrap();
        let plan = statement
            .query_map(params![128, "account", "installation"], |row| {
                row.get::<_, String>(3)
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join(" ");
        assert!(plan.contains("MATERIALIZE candidates"), "{plan}");
        assert!(plan.contains("idx_risk_receipts_active"), "{plan}");
        assert!(plan.contains("SEARCH i USING INDEX"), "{plan}");
        assert!(plan.contains("SEARCH ack USING INDEX"), "{plan}");
        assert!(!plan.contains("SCAN intents"), "{plan}");
        assert!(!plan.contains("TEMP B-TREE"), "{plan}");
    }

    #[test]
    fn post_fence_proof_requires_durable_timestamp_and_coherent_ack_transition() {
        assert!(verified_durable_post_fence(42, "POSTED", "POSTING", None));
        assert!(verified_durable_post_fence(
            42,
            "OPEN",
            "ACKNOWLEDGED",
            Some("recorded-venue-order")
        ));
        for (at, receipt, intent, order) in [
            (0, "POSTED", "POSTING", None),
            (0, "OPEN", "ACKNOWLEDGED", Some("venue")),
            (42, "OPEN", "ACKNOWLEDGED", None),
            (42, "OPEN", "ACKNOWLEDGED", Some("")),
            (42, "HELD", "ACKNOWLEDGED", Some("venue")),
            (42, "POSTED", "ACKNOWLEDGED", Some("venue")),
            (42, "OPEN", "POSTING", None),
            (42, "RELEASED", "ACKNOWLEDGED", Some("venue")),
            (42, "UNKNOWN", "UNKNOWN", Some("venue")),
        ] {
            assert!(
                !verified_durable_post_fence(at, receipt, intent, order),
                "false BEGIN_POST proof: {at}/{receipt}/{intent}/{order:?}"
            );
        }
    }

    #[test]
    fn released_or_no_send_receipt_never_covers_open_exposure() {
        for state in [
            "HELD",
            "POSTED",
            "UNKNOWN",
            "OPEN",
            "TERMINAL_PENDING_CENTRAL",
        ] {
            assert!(risk_receipt_covers_exposure(state), "{state}");
        }
        for state in [
            "RELEASED",
            "UNSENT_PENDING_CENTRAL",
            "",
            "CORRUPT",
            "PREPARED",
        ] {
            assert!(!risk_receipt_covers_exposure(state), "{state}");
        }
    }
}

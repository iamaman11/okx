//! In-process SQLite WAL persistence for the existing execution ledger.
//! Explicitly selected; Production v14 stays authoritative until a cutover.
use super::*;
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::{Serialize, de::DeserializeOwned};

pub(super) const SCHEMA: &str = "okx.execution-ledger.sqlite/v1";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredState {
    version: u32,
    stop: Option<ExecutionRiskStop>,
    reservation: Option<StrategyCapitalReservation>,
    source_run_id: Option<String>,
    periods: StrategyPeriodHistory,
    leverage: Vec<LeverageChangeAttempt>,
}

pub(super) fn io(e: impl std::error::Error + Send + Sync + 'static) -> ExecutionLedgerError {
    ExecutionLedgerError::Io(std::io::Error::other(e))
}
pub(super) fn encode<T: Serialize>(x: &T) -> Result<Vec<u8>, ExecutionLedgerError> {
    rmp_serde::to_vec_named(x).map_err(io)
}
pub(super) fn decode<T: DeserializeOwned>(b: &[u8]) -> Result<T, ExecutionLedgerError> {
    rmp_serde::from_slice(b).map_err(io)
}
pub(super) fn checksum(b: &[u8]) -> String {
    format!("{:x}", Sha256::digest(b))
}

fn connect(path: &Path, create: bool) -> Result<Connection, ExecutionLedgerError> {
    connect_with_integrity(path, create, true)
}

/// Indexed hot-path reads validate the selected rows and pinned account
/// identity. Full PRAGMA integrity_check remains mandatory at cold owner
/// attestation, not on each paginated UI/history query.
fn connect_indexed_read(path: &Path) -> Result<Connection, ExecutionLedgerError> {
    if !path.exists() {
        return Err(ExecutionLedgerError::Corrupt(
            "SQLite account ledger missing",
        ));
    }
    connect_with_integrity(path, false, false)
}

fn connect_with_integrity(
    path: &Path,
    create: bool,
    verify_all_pages: bool,
) -> Result<Connection, ExecutionLedgerError> {
    let mut connection = if create {
        fs::create_dir_all(
            path.parent()
                .ok_or(ExecutionLedgerError::Corrupt("SQLite parent missing"))?,
        )?;
        Connection::open(path).map_err(io)?
    } else {
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(io)?
    };
    connection
        .busy_timeout(std::time::Duration::from_secs(5))
        .map_err(io)?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(io)?;
    if create {
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(io)?;
    }
    let current_version: i64 = connection
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(io)?;
    if current_version != 0 && current_version != 1 {
        return Err(ExecutionLedgerError::Corrupt(
            "unsupported SQLite account ledger schema",
        ));
    }
    if create && current_version == 0 {
        // A missing DB is initialized ONLY in the explicit native SAVE path.
        // No production read ever manufactures a blank trading ledger.
        // The accepted relational schema and native projections are published
        // as one transaction; a crash cannot leave a user_version=1 half DB.
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(io)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(io)?;
        tx.execute_batch(include_str!("../../schema/account-ledger-v1.sql"))
            .map_err(io)?;
        tx.execute_batch(super::sqlite_source::TABLES).map_err(io)?;
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS ledger_state (
                key INTEGER PRIMARY KEY CHECK(key=1), format INTEGER NOT NULL,
                uid TEXT NOT NULL, digest TEXT NOT NULL, bytes BLOB NOT NULL
             ) STRICT;
             CREATE TABLE IF NOT EXISTS ledger_entries (
                intent_id TEXT PRIMARY KEY,
                client_order_id TEXT NOT NULL UNIQUE, uid TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                digest TEXT NOT NULL, bytes BLOB NOT NULL
             ) STRICT;
             CREATE INDEX IF NOT EXISTS idx_ledger_entries_uid
                ON ledger_entries(uid, updated_at_ms);
             CREATE TABLE IF NOT EXISTS native_guarded_intents (
                intent_id TEXT PRIMARY KEY REFERENCES ledger_entries(intent_id),
                instrument_id TEXT NOT NULL,
                entry_digest TEXT NOT NULL
             ) STRICT;
             CREATE INDEX IF NOT EXISTS idx_native_guard_instrument
                ON native_guarded_intents(instrument_id, intent_id);",
        )
        .map_err(io)?;
        tx.commit().map_err(io)?;
    }
    let ver: i64 = connection
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(io)?;
    let journal: String = connection
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .map_err(io)?;
    let foreign_keys: i64 = connection
        .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
        .map_err(io)?;
    if ver != 1 || journal != "wal" || foreign_keys != 1 {
        return Err(ExecutionLedgerError::Corrupt(
            "SQLite schema/WAL/FK mismatch",
        ));
    }
    if create {
        let sync: i64 = connection
            .query_row("PRAGMA synchronous", [], |r| r.get(0))
            .map_err(io)?;
        if sync != 2 {
            return Err(ExecutionLedgerError::Corrupt(
                "SQLite synchronous FULL unavailable",
            ));
        }
    } else if verify_all_pages {
        // Cold read only; per-intent indexed reads do not scan all DB pages.
        let ok: String = connection
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .map_err(io)?;
        if ok != "ok" {
            return Err(ExecutionLedgerError::Corrupt("SQLite integrity failed"));
        }
    }
    Ok(connection)
}

/// Every finance/indexed read checks the SAME durable account-state
/// digest and permanent UID identity. Staging reports may precede native
/// binding; live indexed intent reads always require that binding.
fn require_account_read_scope(
    c: &Connection,
    expected_uid: &str,
    bound_required: bool,
) -> Result<(), ExecutionLedgerError> {
    if expected_uid.is_empty()
        || (bound_required
            && (expected_uid.len() != 64
                || !expected_uid
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))))
    {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    let row: Option<(String, String, Vec<u8>, Option<String>)> = c
        .query_row(
            "SELECT s.uid,s.digest,s.bytes,i.uid_fingerprint
             FROM ledger_state s LEFT JOIN account_identity i ON i.singleton=1
             WHERE s.key=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()
        .map_err(io)?;
    let Some((state_uid, digest, bytes, identity_uid)) = row else {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    };
    if state_uid != expected_uid
        || checksum(&bytes) != digest
        || identity_uid
            .as_deref()
            .is_some_and(|bound| bound != expected_uid)
        || (bound_required && identity_uid.is_none())
    {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    Ok(())
}

fn require_indexed_scope(c: &Connection, expected_uid: &str) -> Result<(), ExecutionLedgerError> {
    require_account_read_scope(c, expected_uid, true)
}

/// A published native account's immutable UID and authenticated state
/// digest can be checked without scanning its historical intents. A full
/// cold row/projection attestation was required before owner activation.
pub(super) fn validate_account_scope(
    path: &Path,
    expected_uid: &str,
) -> Result<(), ExecutionLedgerError> {
    let c = connect_indexed_read(path)?;
    require_indexed_scope(&c, expected_uid)
}

/// Finance diagnostics are allowed for unbound historical staging, but a
/// published account binding must NEVER be bypassed or cross-read.
pub(super) fn require_finance_read_scope(
    c: &Connection,
    expected_uid: &str,
) -> Result<(), ExecutionLedgerError> {
    require_account_read_scope(c, expected_uid, false)
}

/// Use the same read-only WAL/schema/foreign-key validation for financial
/// reporting as for indexed intent queries. No blank-ledger creation.
pub(super) fn open_finance_read(path: &Path) -> Result<Connection, ExecutionLedgerError> {
    connect_indexed_read(path)
}

/// A live guard is a derived projection, never a second reservation
/// authority. Its membership is defined entirely by the native state
/// machine. Historical terminal intents without blocking protection or
/// unfinished reverse continuation are excluded from trading hot reads.
pub(super) fn needs_instrument_guard(entry: &ExecutionLedgerEntry) -> bool {
    let record = &entry.record;
    !record.state.is_terminal()
        || record.active_mutation().is_some()
        || record.protection_blocks_new_managed_intent()
        || record.reverse.as_ref().is_some_and(|link| {
            link.leg == ReverseLeg::Close
                && link.continuation == ReverseContinuation::Required
                && record.state == ExecutionState::Filled
        })
}

/// Read only material instrument guards, validating every row against the
/// authoritative immutable plan + transition record. The guard projection
/// is also fully verified on cold load before any exchange admission.
pub(super) fn indexed_instrument_guards(
    path: &Path,
    expected_uid: &str,
    instrument_id: &str,
) -> Result<Vec<ExecutionLedgerEntry>, ExecutionLedgerError> {
    if instrument_id.is_empty() {
        return Err(ExecutionLedgerError::Corrupt("missing guard instrument"));
    }
    let c = connect_indexed_read(path)?;
    require_indexed_scope(&c, expected_uid)?;
    let sql = "SELECT e.intent_id,e.client_order_id,e.uid,e.digest,e.bytes,
                i.instrument_id,i.requested_action,i.plan_sha256,i.current_state,i.created_at_ms,
                g.entry_digest
         FROM native_guarded_intents g
         JOIN ledger_entries e ON e.intent_id=g.intent_id
         LEFT JOIN intents i ON i.intent_id=e.intent_id
         WHERE g.instrument_id=?1 ORDER BY g.intent_id";
    let mut stmt = c.prepare(sql).map_err(io)?;
    let mut rows = stmt.query(params![instrument_id]).map_err(io)?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().map_err(io)? {
        let digest: String = row.get(3).map_err(io)?;
        let guard_digest: String = row.get(10).map_err(io)?;
        if digest != guard_digest {
            return Err(ExecutionLedgerError::Corrupt(
                "native instrument guard digest mismatch",
            ));
        }
        let entry = verified_indexed_entry(row, expected_uid)?;
        if entry.record.plan.instrument_id != instrument_id || !needs_instrument_guard(&entry) {
            return Err(ExecutionLedgerError::Corrupt(
                "native instrument guard projection mismatch",
            ));
        }
        result.push(entry);
    }
    Ok(result)
}

/// Read every currently material account obligation, not every terminal
/// historic intent. A matching guard is only an indexed projection: its row,
/// digest, immutable plan and current state must all match native truth.
/// The selected account identity is pinned before any answer is returned.
pub(super) fn indexed_account_guards(
    path: &Path,
    expected_uid: &str,
) -> Result<Vec<ExecutionLedgerEntry>, ExecutionLedgerError> {
    let c = connect_indexed_read(path)?;
    require_indexed_scope(&c, expected_uid)?;
    let mut stmt = c
        .prepare(
            "SELECT e.intent_id,e.client_order_id,e.uid,e.digest,e.bytes,
                    i.instrument_id,i.requested_action,i.plan_sha256,i.current_state,i.created_at_ms,
                    g.entry_digest
             FROM native_guarded_intents g
             JOIN ledger_entries e ON e.intent_id=g.intent_id
             LEFT JOIN intents i ON i.intent_id=e.intent_id
             ORDER BY g.intent_id",
        )
        .map_err(io)?;
    let mut rows = stmt.query([]).map_err(io)?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().map_err(io)? {
        let digest: String = row.get(3).map_err(io)?;
        let guard_digest: String = row.get(10).map_err(io)?;
        if digest != guard_digest {
            return Err(ExecutionLedgerError::Corrupt(
                "native account guard digest mismatch",
            ));
        }
        let entry = verified_indexed_entry(row, expected_uid)?;
        if !needs_instrument_guard(&entry) {
            return Err(ExecutionLedgerError::Corrupt(
                "native account guard projection mismatch",
            ));
        }
        result.push(entry);
    }
    Ok(result)
}

/// Only active guards can contain a lost native BEGIN_POST or an in-flight
/// order mutation. The caller holds the same account writer lease used to
/// apply UNKNOWN transitions; no status here authorizes a second venue POST.
///
/// Staging tests may be unbound before physical owner cutover, but immutable
/// ledger UID and state digest are still verified. Published accounts use
/// the same UID and stricter bound-owner checks at their cold boundary.
pub(super) fn indexed_recovery_candidates(
    path: &Path,
    expected_uid: &str,
) -> Result<Vec<ExecutionLedgerEntry>, ExecutionLedgerError> {
    let c = connect_indexed_read(path)?;
    require_account_read_scope(&c, expected_uid, false)?;
    let sql = "SELECT e.intent_id,e.client_order_id,e.uid,e.digest,e.bytes,
                      i.instrument_id,i.requested_action,i.plan_sha256,i.current_state,i.created_at_ms,
                      g.entry_digest
               FROM native_guarded_intents g
               JOIN ledger_entries e ON e.intent_id=g.intent_id
               LEFT JOIN intents i ON i.intent_id=e.intent_id
               ORDER BY g.intent_id";
    let mut stmt = c.prepare(sql).map_err(io)?;
    let mut rows = stmt.query([]).map_err(io)?;
    let mut pending = Vec::new();
    while let Some(row) = rows.next().map_err(io)? {
        let digest: String = row.get(3).map_err(io)?;
        let guard_digest: String = row.get(10).map_err(io)?;
        if digest != guard_digest {
            return Err(ExecutionLedgerError::Corrupt(
                "native recovery guard digest mismatch",
            ));
        }
        let entry = verified_indexed_entry(row, expected_uid)?;
        if !needs_instrument_guard(&entry) {
            return Err(ExecutionLedgerError::Corrupt(
                "native recovery guard projection mismatch",
            ));
        }
        if entry.record.state == ExecutionState::Submitting
            || entry
                .record
                .mutations
                .iter()
                .any(|mutation| mutation.state == OrderMutationState::Submitting)
        {
            pending.push(entry);
        }
    }
    Ok(pending)
}

/// A bounded caller-proved batch can retire only ALL currently unsent
/// PREPARED intents of this account. Read the active material SQL projection
/// (rather than its unbounded historical journal), validate each BLOB and
/// preserve a fail-closed no-UNKNOWN rule under the account's writer lease.
pub(super) fn indexed_exact_prepared_batch(
    path: &Path,
    expected_uid: &str,
    expected_ids: &[String],
) -> Result<Vec<ExecutionLedgerEntry>, ExecutionLedgerError> {
    if expected_ids.is_empty()
        || expected_ids.len() > 32
        || !expected_ids.windows(2).all(|pair| pair[0] < pair[1])
    {
        return Err(ExecutionLedgerError::Corrupt(
            "invalid native exact prepared inventory",
        ));
    }
    let c = connect_indexed_read(path)?;
    require_account_read_scope(&c, expected_uid, false)?;
    let mut stmt = c
        .prepare(
            "SELECT e.intent_id,e.client_order_id,e.uid,e.digest,e.bytes,
                    i.instrument_id,i.requested_action,i.plan_sha256,i.current_state,i.created_at_ms,
                    g.entry_digest
             FROM native_guarded_intents g
             JOIN ledger_entries e ON e.intent_id=g.intent_id
             LEFT JOIN intents i ON i.intent_id=e.intent_id
             ORDER BY g.intent_id",
        )
        .map_err(io)?;
    let mut rows = stmt.query([]).map_err(io)?;
    let mut found = Vec::with_capacity(expected_ids.len());
    let mut next_expected = 0;
    while let Some(row) = rows.next().map_err(io)? {
        let digest: String = row.get(3).map_err(io)?;
        let guard_digest: String = row.get(10).map_err(io)?;
        if digest != guard_digest {
            return Err(ExecutionLedgerError::Corrupt(
                "native prepared guard digest mismatch",
            ));
        }
        let entry = verified_indexed_entry(row, expected_uid)?;
        if !needs_instrument_guard(&entry) {
            return Err(ExecutionLedgerError::Corrupt(
                "native prepared inventory index mismatch",
            ));
        }
        if matches!(
            entry.record.state,
            ExecutionState::Submitting | ExecutionState::UnknownSubmission
        ) {
            return Err(ExecutionLedgerError::Corrupt(
                "ledger account or unresolved POST mismatch",
            ));
        }
        if entry.record.state == ExecutionState::Prepared {
            if expected_ids.get(next_expected).map(String::as_str)
                != Some(entry.record.plan.intent_id.as_str())
            {
                return Err(ExecutionLedgerError::Corrupt("prepared inventory mismatch"));
            }
            found.push(entry);
            next_expected += 1;
        }
    }
    if next_expected != expected_ids.len() {
        return Err(ExecutionLedgerError::Corrupt("prepared inventory mismatch"));
    }
    Ok(found)
}

/// The authoritative ledger_entries UNIQUE index checks client IDs without
/// searching all historical intents. The caller retains its account lease.
pub(super) fn client_order_id_exists(
    path: &Path,
    expected_uid: &str,
    client_order_id: &str,
) -> Result<bool, ExecutionLedgerError> {
    let c = connect_indexed_read(path)?;
    require_indexed_scope(&c, expected_uid)?;
    let prior_uid: Option<String> = c
        .query_row(
            "SELECT uid FROM ledger_entries WHERE client_order_id=?1",
            params![client_order_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(io)?;
    if prior_uid.as_deref().is_some_and(|uid| uid != expected_uid) {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    Ok(prior_uid.is_some())
}

/// One exact persisted entry is checked against its BLOB digest, immutable
/// plan, and indexed relational projection. The left join exposes missing
/// projections as corruption rather than silently returning incomplete rows.
fn verified_indexed_entry(
    row: &rusqlite::Row<'_>,
    expected_uid: &str,
) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
    let intent_id: String = row.get(0).map_err(io)?;
    let client_order_id: String = row.get(1).map_err(io)?;
    let uid: String = row.get(2).map_err(io)?;
    let digest: String = row.get(3).map_err(io)?;
    let payload: Vec<u8> = row.get(4).map_err(io)?;
    let instrument_id: Option<String> = row.get(5).map_err(io)?;
    let requested_action: Option<String> = row.get(6).map_err(io)?;
    let plan_sha256: Option<String> = row.get(7).map_err(io)?;
    let current_state: Option<String> = row.get(8).map_err(io)?;
    let created_at_ms: Option<i64> = row.get(9).map_err(io)?;
    if uid != expected_uid || checksum(&payload) != digest {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    let entry: ExecutionLedgerEntry = decode(&payload)?;
    validate_entry(&entry)?;
    if entry.record.plan.intent_id != intent_id
        || entry.record.plan.client_order_id != client_order_id
        || entry.record.plan.account_uid_fingerprint != uid
        || instrument_id.as_deref() != Some(entry.record.plan.instrument_id.as_str())
        || requested_action.as_deref() != Some(format!("{:?}", entry.record.plan.action).as_str())
        || plan_sha256.as_deref() != Some(checksum(&encode(&entry.record.plan)?).as_str())
        || current_state.as_deref() != Some(format!("{:?}", entry.record.state).as_str())
        || created_at_ms != i64::try_from(entry.created_at_ms).ok()
    {
        return Err(ExecutionLedgerError::Corrupt(
            "indexed intent projection mismatch",
        ));
    }
    Ok(entry)
}

const INDEXED_INTENT_COLUMNS: &str = "
 SELECT e.intent_id,e.client_order_id,e.uid,e.digest,e.bytes,
        i.instrument_id,i.requested_action,i.plan_sha256,i.current_state,i.created_at_ms
 FROM ledger_entries e LEFT JOIN intents i ON i.intent_id=e.intent_id";

pub(super) fn indexed_intent(
    path: &Path,
    expected_uid: &str,
    intent_id: &str,
) -> Result<Option<ExecutionLedgerEntry>, ExecutionLedgerError> {
    if intent_id.is_empty() || intent_id.len() > 128 {
        return Err(ExecutionLedgerError::Corrupt("invalid indexed intent ID"));
    }
    let c = connect_indexed_read(path)?;
    require_indexed_scope(&c, expected_uid)?;
    let sql = format!("{INDEXED_INTENT_COLUMNS} WHERE e.intent_id=?1");
    let mut stmt = c.prepare(&sql).map_err(io)?;
    let mut rows = stmt.query(params![intent_id]).map_err(io)?;
    let entry = rows
        .next()
        .map_err(io)?
        .map(|row| verified_indexed_entry(row, expected_uid))
        .transpose()?;
    if rows.next().map_err(io)?.is_some() {
        return Err(ExecutionLedgerError::Corrupt("indexed intent duplicate"));
    }
    Ok(entry)
}

pub(super) fn indexed_intent_page(
    path: &Path,
    expected_uid: &str,
    after_intent_id: Option<&str>,
    limit: usize,
) -> Result<SqliteIntentPage, ExecutionLedgerError> {
    if !(1..=256).contains(&limit) || after_intent_id.is_some_and(|s| s.is_empty() || s.len() > 128)
    {
        return Err(ExecutionLedgerError::Corrupt("invalid indexed intent page"));
    }
    let c = connect_indexed_read(path)?;
    require_indexed_scope(&c, expected_uid)?;
    let sql =
        format!("{INDEXED_INTENT_COLUMNS} WHERE e.intent_id>?1 ORDER BY e.intent_id LIMIT ?2");
    let mut stmt = c.prepare(&sql).map_err(io)?;
    let mut rows = stmt
        .query(params![after_intent_id.unwrap_or(""), (limit + 1) as i64])
        .map_err(io)?;
    let mut entries = Vec::with_capacity(limit + 1);
    while let Some(row) = rows.next().map_err(io)? {
        entries.push(verified_indexed_entry(row, expected_uid)?);
    }
    let has_more = entries.len() > limit;
    entries.truncate(limit);
    let next_after = if has_more {
        entries.last().map(|e| e.record.plan.intent_id.clone())
    } else {
        None
    };
    Ok(SqliteIntentPage {
        entries,
        next_after,
    })
}

/// Read one exact authoritative record for the single writer. Unlike
/// user-facing history reads, a staged native owner can be not yet bound to
/// its credential UID, but the durable manifest and every indexed projection
/// MUST already agree. Never manufacture a missing account or empty intent.
pub(super) fn owner_intent(
    path: &Path,
    intent_id: &str,
) -> Result<Option<ExecutionLedgerEntry>, ExecutionLedgerError> {
    if intent_id.is_empty() || intent_id.len() > 128 {
        return Err(ExecutionLedgerError::Corrupt("invalid owner intent ID"));
    }
    let c = connect_indexed_read(path)?;
    let uid: String = c
        .query_row("SELECT uid FROM ledger_state WHERE key=1", [], |row| {
            row.get(0)
        })
        .map_err(io)?;
    require_account_read_scope(&c, &uid, false)?;
    let sql = format!("{INDEXED_INTENT_COLUMNS} WHERE e.intent_id=?1");
    let mut stmt = c.prepare(&sql).map_err(io)?;
    let mut rows = stmt.query(params![intent_id]).map_err(io)?;
    let found = rows
        .next()
        .map_err(io)?
        .map(|row| verified_indexed_entry(row, &uid))
        .transpose()?;
    if rows.next().map_err(io)?.is_some() {
        return Err(ExecutionLedgerError::Corrupt("duplicate owner intent"));
    }
    Ok(found)
}

/// Read a risk predicate over intents CREATED during the current
/// run without building a historical BTreeMap or selecting all terminal
/// account operations. Every row is authenticated against its exact BLOB
/// and immutable relational projection before the predicate is evaluated.
///
/// updated_at >= started_at is a *safe indexed superset* of
/// created_at >= started_at because native transitions never move time
/// backward. The actual created-at test uses the verified authoritative
/// record. Do not trust the material projection alone for a negative gate.
pub(super) fn any_entry_created_since<F>(
    path: &Path,
    expected_uid: &str,
    since_ms: u64,
    mut predicate: F,
) -> Result<bool, ExecutionLedgerError>
where
    F: FnMut(&ExecutionLedgerEntry) -> bool,
{
    let mut matched = false;
    visit_entries_created_since(path, expected_uid, since_ms, |entry| {
        if predicate(entry) {
            matched = true;
            false
        } else {
            true
        }
    })?;
    Ok(matched)
}

/// A single indexed, authenticated cursor serves risk predicates and bounded
/// release candidates. Return false from the callback to stop early. The
/// callback does not gain SQLite write access or a mutation authority.
pub(super) fn visit_entries_created_since<F>(
    path: &Path,
    expected_uid: &str,
    since_ms: u64,
    mut visit: F,
) -> Result<(), ExecutionLedgerError>
where
    F: FnMut(&ExecutionLedgerEntry) -> bool,
{
    if since_ms == 0 {
        return Err(ExecutionLedgerError::InvalidTimestamp);
    }
    let since = i64::try_from(since_ms).map_err(|_| ExecutionLedgerError::InvalidTimestamp)?;
    let c = connect_indexed_read(path)?;
    require_indexed_scope(&c, expected_uid)?;
    let sql = format!(
        "{INDEXED_INTENT_COLUMNS}
         WHERE e.uid=?1 AND e.updated_at_ms>=?2
         ORDER BY e.updated_at_ms,e.intent_id"
    );
    let mut stmt = c.prepare(&sql).map_err(io)?;
    let mut rows = stmt.query(params![expected_uid, since]).map_err(io)?;
    while let Some(row) = rows.next().map_err(io)? {
        let entry = verified_indexed_entry(row, expected_uid)?;
        if entry.created_at_ms >= since_ms && !visit(&entry) {
            break;
        }
    }
    Ok(())
}

/// Source-exact capital epoch opening must not infer a flat account
/// merely because completed historical intents were evicted from RAM.
/// Read the material guards and created-since interval from the native
/// durable owner; support the explicit initially-empty account only.
pub(super) fn clear_for_capital_opening(
    path: &Path,
    expected_uid: &str,
    started_at_ms: u64,
) -> Result<bool, ExecutionLedgerError> {
    if started_at_ms == 0 || expected_uid.is_empty() {
        return Err(ExecutionLedgerError::CapitalReservationUnattested);
    }
    let started = i64::try_from(started_at_ms)
        .map_err(|_| ExecutionLedgerError::CapitalReservationUnattested)?;
    let c = connect_indexed_read(path)?;
    let uid: String = c
        .query_row("SELECT uid FROM ledger_state WHERE key=1", [], |row| {
            row.get(0)
        })
        .map_err(io)?;
    if uid.is_empty() {
        let count: i64 = c
            .query_row("SELECT COUNT(*) FROM ledger_entries", [], |r| r.get(0))
            .map_err(io)?;
        if count != 0 {
            return Err(ExecutionLedgerError::AccountScopeMismatch);
        }
        return Ok(true);
    }
    if uid != expected_uid {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    require_account_read_scope(&c, expected_uid, false)?;
    let guards: i64 = c
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM native_guarded_intents LIMIT 1)",
            [],
            |r| r.get(0),
        )
        .map_err(io)?;
    if guards != 0 {
        return Ok(false);
    }
    let sql = format!(
        "{INDEXED_INTENT_COLUMNS}
         WHERE e.uid=?1 AND e.updated_at_ms>=?2
         ORDER BY e.updated_at_ms,e.intent_id"
    );
    let mut stmt = c.prepare(&sql).map_err(io)?;
    let mut rows = stmt.query(params![expected_uid, started]).map_err(io)?;
    while let Some(row) = rows.next().map_err(io)? {
        let entry = verified_indexed_entry(row, expected_uid)?;
        if entry.created_at_ms >= started_at_ms {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Count exact authoritative rows from this account's WAL owner without
/// visiting, decoding or retaining the historical intent payloads. Caller
/// must separately prove row/guard parity at the cold owner boundary.
pub(super) fn count_account_intents(path: &Path) -> Result<usize, ExecutionLedgerError> {
    let c = connect_indexed_read(path)?;
    let uid: String = c
        .query_row("SELECT uid FROM ledger_state WHERE key=1", [], |r| r.get(0))
        .map_err(io)?;
    require_account_read_scope(&c, &uid, false)?;
    let rows: i64 = c
        .query_row("SELECT COUNT(*) FROM ledger_entries", [], |r| r.get(0))
        .map_err(io)?;
    usize::try_from(rows)
        .map_err(|_| ExecutionLedgerError::Corrupt("native inventory overflow"))
}

/// A binding is created only by an authenticated native owner for an
/// ALREADY VALID, nonempty, UID-scoped ledger. This is not a registration
/// endpoint or a separate authorizer.
pub(super) fn bind_account_identity(
    path: &Path,
    identity: &SqliteAccountIdentity,
) -> Result<(), ExecutionLedgerError> {
    identity.validate()?;
    // Validate the complete authoritative historical state once at binding,
    // not on every hot-path order transition.
    let (entries, _, reservation, source, _, _) = load(path, usize::MAX)?;
    let expected = identity.uid_fingerprint.as_str();
    if entries
        .values()
        .any(|entry| entry.record.plan.account_uid_fingerprint != expected)
        || reservation
            .as_ref()
            .is_some_and(|r| r.account_uid_fingerprint != expected)
        || source.as_ref().is_some_and(|e| e.account_uid != expected)
        || (entries.is_empty() && reservation.is_none() && source.is_none())
    {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }

    let mut connection = connect(path, true)?;
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io)?;
    let persisted_uid: String = tx
        .query_row("SELECT uid FROM ledger_state WHERE key=1", [], |r| r.get(0))
        .map_err(io)?;
    if persisted_uid != expected {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    let previous: Option<(String, String, String, String)> = tx
        .query_row(
            "SELECT venue,environment,account_id,uid_fingerprint
             FROM account_identity WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()
        .map_err(io)?;
    let next = (
        identity.venue_key().to_owned(),
        identity.environment_key().to_owned(),
        identity.account_id.clone(),
        expected.to_owned(),
    );
    if let Some(prior) = previous {
        if prior != next {
            return Err(ExecutionLedgerError::AccountScopeMismatch);
        }
    } else {
        tx.execute(
            "INSERT INTO account_identity
             (singleton,venue,environment,account_id,uid_fingerprint,created_at_ms,schema_version)
             VALUES (1,?1,?2,?3,?4,?5,1)",
            params![
                identity.venue_key(),
                identity.environment_key(),
                identity.account_id,
                expected,
                identity.bound_at_ms,
            ],
        )
        .map_err(io)?;
    }
    tx.commit().map_err(io)?;
    Ok(())
}

/// An indexed, bounded identity read; does not load historical intents.
pub(super) fn bound_account_identity(
    path: &Path,
) -> Result<Option<SqliteAccountIdentity>, ExecutionLedgerError> {
    if !path.exists() {
        return Err(ExecutionLedgerError::Corrupt(
            "SQLite account ledger missing",
        ));
    }
    let connection = connect(path, false)?;
    let row: Option<(String, String, String, String, i64)> = connection
        .query_row(
            "SELECT venue,environment,account_id,uid_fingerprint,created_at_ms
             FROM account_identity WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .optional()
        .map_err(io)?;
    let Some((venue, environment, account_id, uid_fingerprint, bound_at)) = row else {
        return Ok(None);
    };
    let scope = SqliteAccountIdentity {
        venue: match venue.as_str() {
            "okx" => LedgerVenue::Okx,
            "bybit" => LedgerVenue::Bybit,
            _ => return Err(ExecutionLedgerError::AccountScopeMismatch),
        },
        environment: match environment.as_str() {
            "production" => LedgerEnvironment::Production,
            "demo" => LedgerEnvironment::Demo,
            _ => return Err(ExecutionLedgerError::AccountScopeMismatch),
        },
        account_id,
        uid_fingerprint,
        bound_at_ms: u64::try_from(bound_at)
            .map_err(|_| ExecutionLedgerError::AccountScopeMismatch)?,
    };
    scope.validate()?;
    let ledger_uid: String = connection
        .query_row("SELECT uid FROM ledger_state WHERE key=1", [], |r| r.get(0))
        .map_err(io)?;
    if ledger_uid != scope.uid_fingerprint {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    Ok(Some(scope))
}

/// Startup reads authoritative history by STREAMING attestation and only
/// retains current material obligations in Rust. Old terminal rows remain
/// searchable by immutable indexed ID, but no terminal BTreeMap is created.
/// This is the canonical native runtime load, NOT the v14 migration loader.
///
/// The cold attestation enforces the entire original snapshot, post fences
/// and guard inventory before these bounded-state views may be used.
/// The same account writer lease must remain held by the calling Engine.
pub(super) fn load_operational(path: &Path) -> Result<LoadedExecutionLedger, ExecutionLedgerError> {
    attest_snapshot(path, None)?;
    let c = connect(path, false)?;
    let (uid, digest, bytes): (String, String, Vec<u8>) = c
        .query_row(
            "SELECT uid,digest,bytes FROM ledger_state WHERE key=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(io)?;
    if checksum(&bytes) != digest {
        return Err(ExecutionLedgerError::Corrupt(
            "operational SQLite state digest mismatch",
        ));
    }
    let state: StoredState = decode(&bytes)?;
    if state.version != 1
        || state
            .reservation
            .as_ref()
            .is_some_and(|r| r.account_uid_fingerprint != uid)
    {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    require_account_read_scope(&c, &uid, false)?;
    let source = state
        .source_run_id
        .as_deref()
        .map(|run_id| super::sqlite_source::read(&c, run_id, &uid))
        .transpose()?;
    state
        .periods
        .validate(state.reservation.as_ref(), source.as_ref())?;
    validate_leverage_attempts(&state.leverage, state.reservation.as_ref())?;

    let mut statement = c
        .prepare(
            "SELECT e.intent_id,e.client_order_id,e.uid,e.digest,e.bytes,
                    i.instrument_id,i.requested_action,i.plan_sha256,i.current_state,i.created_at_ms,
                    g.entry_digest
             FROM native_guarded_intents g
             JOIN ledger_entries e ON e.intent_id=g.intent_id
             LEFT JOIN intents i ON i.intent_id=e.intent_id
             ORDER BY g.intent_id",
        )
        .map_err(io)?;
    let mut rows = statement.query([]).map_err(io)?;
    let mut active = BTreeMap::new();
    while let Some(row) = rows.next().map_err(io)? {
        let digest: String = row.get(3).map_err(io)?;
        let guard_digest: String = row.get(10).map_err(io)?;
        if digest != guard_digest {
            return Err(ExecutionLedgerError::Corrupt(
                "operational guard checksum mismatch",
            ));
        }
        let entry = verified_indexed_entry(row, &uid)?;
        if !needs_instrument_guard(&entry) {
            return Err(ExecutionLedgerError::Corrupt("operational guard mismatch"));
        }
        let id = entry.record.plan.intent_id.clone();
        if active.insert(id, entry).is_some() {
            return Err(ExecutionLedgerError::Corrupt(
                "duplicate operational intent",
            ));
        }
    }
    drop(rows);
    drop(statement);
    Ok((
        active,
        state.stop,
        state.reservation,
        source,
        state.periods,
        state.leverage,
    ))
}

pub(super) fn load(path: &Path, max: usize) -> Result<LoadedExecutionLedger, ExecutionLedgerError> {
    // Only explicit staging creation may introduce an empty SQLite file.
    // A missing live account ledger can never silently reopen as an empty one.
    if !path.exists() {
        return Err(ExecutionLedgerError::Corrupt(
            "SQLite account ledger missing",
        ));
    }
    let c = connect(path, false)?;
    let meta: Option<(String, String, Vec<u8>)> = c
        .query_row(
            "SELECT uid,digest,bytes FROM ledger_state WHERE key=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(io)?;
    let bound_uid: Option<String> = c
        .query_row(
            "SELECT uid_fingerprint FROM account_identity WHERE singleton=1",
            [],
            |r| r.get(0),
        )
        .optional()
        .map_err(io)?;
    if bound_uid
        .as_deref()
        .is_some_and(|bound| meta.as_ref().is_none_or(|(uid, _, _)| uid != bound))
    {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    // One indexed streaming join verifies the authoritative BLOB and its
    // relational projection in the SAME row. Avoid N extra SQLite queries
    // on every restart (the in-memory engine still keeps full history until
    // risk/finance lookups are migrated to indexed reads).
    let sql = format!("{INDEXED_INTENT_COLUMNS} ORDER BY e.intent_id");
    let mut statement = c.prepare(&sql).map_err(io)?;
    let mut rows = statement.query([]).map_err(io)?;
    let mut entries = BTreeMap::new();
    let mut known_uid: Option<String> = None;
    while let Some(row) = rows.next().map_err(io)? {
        let uid: String = row.get(2).map_err(io)?;
        if entries.len() >= max || known_uid.as_ref().is_some_and(|v| v != &uid) {
            return Err(ExecutionLedgerError::Corrupt(
                "SQLite entry integrity/scope",
            ));
        }
        let entry = verified_indexed_entry(row, &uid)?;
        let id = entry.record.plan.intent_id.clone();
        known_uid = Some(uid);
        if entries.insert(id, entry).is_some() {
            return Err(ExecutionLedgerError::Corrupt("SQLite duplicate intent"));
        }
    }

    // A cold read proves the projection has no extra orphan rows. The join
    // above already proved that every authoritative row has its projection.
    let count: i64 = c
        .query_row("SELECT COUNT(*) FROM intents", [], |row| row.get(0))
        .map_err(io)?;
    if count != entries.len() as i64 {
        return Err(ExecutionLedgerError::Corrupt(
            "SQLite intent projection count mismatch",
        ));
    }
    // A live guard must never disappear from the SQL projection after a
    // crash or tampering: absence could incorrectly admit another intent.
    // Cold verification walks all authoritative rows once, while hot
    // admission reads only the small indexed material guard set.
    {
        let mut stmt = c
            .prepare(
                "SELECT intent_id,instrument_id,entry_digest
                 FROM native_guarded_intents ORDER BY intent_id",
            )
            .map_err(io)?;
        let mut rows = stmt.query([]).map_err(io)?;
        let mut expected_guards = entries.iter().filter(|(_, e)| needs_instrument_guard(e));
        while let Some(row) = rows.next().map_err(io)? {
            let id: String = row.get(0).map_err(io)?;
            let instrument: String = row.get(1).map_err(io)?;
            let digest: String = row.get(2).map_err(io)?;
            let Some((expected_id, entry)) = expected_guards.next() else {
                return Err(ExecutionLedgerError::Corrupt(
                    "extra native instrument guard",
                ));
            };
            if id != *expected_id
                || instrument != entry.record.plan.instrument_id
                || digest != checksum(&encode(entry)?)
            {
                return Err(ExecutionLedgerError::Corrupt(
                    "native instrument guard inventory mismatch",
                ));
            }
        }
        if expected_guards.next().is_some() {
            return Err(ExecutionLedgerError::Corrupt(
                "missing native instrument guard",
            ));
        }
    }
    // Reconcile all native POST fences against the immutable event chain.
    // There may be legacy imported submissions with no native BEGIN_POST;
    // absence of a native fence is never upgraded to affirmative proof.
    {
        let mut query = c
            .prepare(
                "SELECT p.intent_id,p.client_order_id,p.begin_post_at_ms,
                    e.event_type,e.subject_id,e.effective_at_ms
             FROM post_attempts p
             JOIN domain_events e ON e.event_id=p.begin_post_event_id",
            )
            .map_err(io)?;
        let mut rows = query.query([]).map_err(io)?;
        while let Some(row) = rows.next().map_err(io)? {
            let id: String = row.get(0).map_err(io)?;
            let client: String = row.get(1).map_err(io)?;
            let at: i64 = row.get(2).map_err(io)?;
            let kind: String = row.get(3).map_err(io)?;
            let subject: String = row.get(4).map_err(io)?;
            let effective: i64 = row.get(5).map_err(io)?;
            let entry = entries
                .get(&id)
                .ok_or(ExecutionLedgerError::Corrupt("orphan native post fence"))?;
            if client != entry.record.plan.client_order_id
                || kind != "BEGIN_POST"
                || subject != id
                || effective != at
                || entry.record.state == ExecutionState::Prepared
            {
                return Err(ExecutionLedgerError::Corrupt("native post fence mismatch"));
            }
        }
    }
    let state = if let Some((uid, sha, bytes)) = meta {
        if checksum(&bytes) != sha || known_uid.as_deref().is_some_and(|v| v != uid) {
            return Err(ExecutionLedgerError::Corrupt(
                "SQLite manifest integrity/UID",
            ));
        }
        let s: StoredState = decode(&bytes)?;
        if s.version != 1 {
            return Err(ExecutionLedgerError::Corrupt("SQLite data version"));
        }
        let expected_uid = known_uid
            .as_deref()
            .or_else(|| {
                s.reservation
                    .as_ref()
                    .map(|r| r.account_uid_fingerprint.as_str())
            })
            .unwrap_or("");
        if uid != expected_uid {
            return Err(ExecutionLedgerError::Corrupt(
                "SQLite state account UID mismatch",
            ));
        }
        s
    } else if !entries.is_empty() {
        return Err(ExecutionLedgerError::Corrupt("SQLite orphaned entries"));
    } else {
        return Err(ExecutionLedgerError::Corrupt(
            "SQLite account state missing",
        ));
    };
    if let Some(s) = &state.stop {
        validate_risk_stop(s)?;
    }
    if let Some(r) = &state.reservation {
        validate_capital_reservation(r)?;
    }
    let source = state
        .source_run_id
        .as_deref()
        .map(|run_id| {
            super::sqlite_source::read(
                &c,
                run_id,
                known_uid
                    .as_deref()
                    .or_else(|| {
                        state
                            .reservation
                            .as_ref()
                            .map(|r| r.account_uid_fingerprint.as_str())
                    })
                    .unwrap_or(""),
            )
        })
        .transpose()?;
    state
        .periods
        .validate(state.reservation.as_ref(), source.as_ref())?;
    validate_leverage_attempts(&state.leverage, state.reservation.as_ref())?;
    for e in entries.values() {
        validate_strategy_run_binding(
            e,
            state.reservation.as_ref(),
            &state.periods.archived_runs,
            &state.periods.unverified_runs,
        )?;
    }
    Ok((
        entries,
        state.stop,
        state.reservation,
        source,
        state.periods,
        state.leverage,
    ))
}

struct SqliteWriteState<'a> {
    stop: Option<&'a ExecutionRiskStop>,
    reservation: Option<&'a StrategyCapitalReservation>,
    source: Option<&'a DurableExchangeEvidence>,
    periods: &'a StrategyPeriodHistory,
    leverage: &'a [LeverageChangeAttempt],
}

pub(super) fn save(
    path: &Path,
    entries: &BTreeMap<String, ExecutionLedgerEntry>,
    stop: Option<&ExecutionRiskStop>,
    reservation: Option<&StrategyCapitalReservation>,
    source: Option<&DurableExchangeEvidence>,
    periods: &StrategyPeriodHistory,
    leverage: &[LeverageChangeAttempt],
) -> Result<(), ExecutionLedgerError> {
    save_snapshot(
        path,
        entries,
        SqliteWriteState {
            stop,
            reservation,
            source,
            periods,
            leverage,
        },
        true,
    )
}

/// Update STOP, capital, financial source and epoch state in ONE local
/// SQLite WAL commit without rewriting historical intent or POST rows.
pub(super) fn save_metadata(
    path: &Path,
    stop: Option<&ExecutionRiskStop>,
    reservation: Option<&StrategyCapitalReservation>,
    source: Option<&DurableExchangeEvidence>,
    periods: &StrategyPeriodHistory,
    leverage: &[LeverageChangeAttempt],
) -> Result<(), ExecutionLedgerError> {
    if !path.exists() {
        return Err(ExecutionLedgerError::Corrupt(
            "native metadata mutation has no account DB",
        ));
    }
    if let Some(value) = stop {
        validate_risk_stop(value)?;
    }
    if let Some(value) = reservation {
        validate_capital_reservation(value)?;
    }
    if let Some(value) = source {
        value.validate()?;
        if reservation.is_none_or(|run| {
            value.account_uid != run.account_uid_fingerprint || value.run_id != run.allocation_id
        }) {
            return Err(ExecutionLedgerError::AccountScopeMismatch);
        }
    }
    periods.validate(reservation, source)?;
    validate_leverage_attempts(leverage, reservation)?;
    let mut c = connect(path, true)?;
    let tx = c
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io)?;
    let (uid, digest, old_bytes): (String, String, Vec<u8>) = tx
        .query_row(
            "SELECT uid,digest,bytes FROM ledger_state WHERE key=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(io)?;
    if checksum(&old_bytes) != digest {
        return Err(ExecutionLedgerError::Corrupt(
            "native metadata checksum mismatch",
        ));
    }
    let old: StoredState = decode(&old_bytes)?;
    if old.version != 1 {
        return Err(ExecutionLedgerError::Corrupt(
            "native metadata schema mismatch",
        ));
    }
    // Account identity never changes when rotating a strategy epoch or
    // temporarily having no current source/capital reservation.
    let expected = reservation
        .map(|r| r.account_uid_fingerprint.as_str())
        .or_else(|| source.map(|s| s.account_uid.as_str()))
        .or_else(|| {
            old.reservation
                .as_ref()
                .map(|r| r.account_uid_fingerprint.as_str())
        })
        .unwrap_or(uid.as_str());
    let bound: Option<String> = tx
        .query_row(
            "SELECT uid_fingerprint FROM account_identity WHERE singleton=1",
            [],
            |r| r.get(0),
        )
        .optional()
        .map_err(io)?;
    // One previously empty, unbound account can acquire its first verified
    // UID from the typed capital opening. Never rebind populated history.
    let first_uid = if uid.is_empty()
        && !expected.is_empty()
        && bound.is_none()
        && old.reservation.is_none()
        && old.source_run_id.is_none()
    {
        let count: i64 = tx
            .query_row("SELECT COUNT(*) FROM ledger_entries", [], |r| r.get(0))
            .map_err(io)?;
        count == 0
    } else {
        false
    };
    if (uid != expected && !first_uid) || bound.as_deref().is_some_and(|v| v != uid) {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    let bytes = encode(&StoredState {
        version: 1,
        stop: stop.cloned(),
        reservation: reservation.cloned(),
        source_run_id: source.map(|s| s.run_id.clone()),
        periods: periods.clone(),
        leverage: leverage.to_vec(),
    })?;
    let changed = tx
        .execute(
            "UPDATE ledger_state SET uid=?1,digest=?2,bytes=?3
             WHERE key=1 AND uid=?4 AND digest=?5",
            params![expected, checksum(&bytes), bytes, uid, digest],
        )
        .map_err(io)?;
    if changed != 1 {
        return Err(ExecutionLedgerError::Corrupt("native metadata CAS lost"));
    }
    // Source observations and derived immutable financial rows are
    // committed with their state pointer, never as a competing write owner.
    if let Some(evidence) = source {
        super::sqlite_source::persist(&tx, evidence)?;
    }
    tx.commit().map_err(io)?;
    Ok(())
}

/// A single-intent mutation persists only this record and its projections.
/// Other account intents remain immutable in this transaction. Exact identity,
/// durable POST fencing, and all stop/source proof are still enforced.
pub(super) fn save_one(
    path: &Path,
    entry: &ExecutionLedgerEntry,
    stop: Option<&ExecutionRiskStop>,
    reservation: Option<&StrategyCapitalReservation>,
    source: Option<&DurableExchangeEvidence>,
    periods: &StrategyPeriodHistory,
    leverage: &[LeverageChangeAttempt],
) -> Result<(), ExecutionLedgerError> {
    // A delta must never initialize a new account ledger. Only a verified
    // first snapshot / one-owner migration can create its storage.
    if !path.exists() {
        return Err(ExecutionLedgerError::Corrupt(
            "native intent delta has no account DB",
        ));
    }
    let id = entry.record.plan.intent_id.clone();
    let changed = BTreeMap::from([(id, entry.clone())]);
    save_snapshot(
        path,
        &changed,
        SqliteWriteState {
            stop,
            reservation,
            source,
            periods,
            leverage,
        },
        false,
    )
}

/// Persist only the intents whose crash-recovery states actually changed.
/// All transitions and POST fences commit in a SINGLE SQLite WAL transaction;
/// never rewrite source/capital/archives merely because an Agent restarted.
pub(super) fn save_recovered(
    path: &Path,
    changed: &BTreeMap<String, ExecutionLedgerEntry>,
    stop: Option<&ExecutionRiskStop>,
    reservation: Option<&StrategyCapitalReservation>,
    source: Option<&DurableExchangeEvidence>,
    periods: &StrategyPeriodHistory,
    leverage: &[LeverageChangeAttempt],
) -> Result<(), ExecutionLedgerError> {
    if changed.is_empty() || !path.exists() {
        return Err(ExecutionLedgerError::Corrupt(
            "native recovery requires existing account and changed intents",
        ));
    }
    save_snapshot(
        path,
        changed,
        SqliteWriteState {
            stop,
            reservation,
            source,
            periods,
            leverage,
        },
        false,
    )
}

fn save_snapshot(
    path: &Path,
    entries: &BTreeMap<String, ExecutionLedgerEntry>,
    state: SqliteWriteState<'_>,
    complete_snapshot: bool,
) -> Result<(), ExecutionLedgerError> {
    let SqliteWriteState {
        stop,
        reservation,
        source,
        periods,
        leverage,
    } = state;
    let mut c = connect(path, true)?;
    let tx = c
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io)?;
    let uid = entries
        .values()
        .next()
        .map(|e| e.record.plan.account_uid_fingerprint.as_str())
        .or_else(|| reservation.map(|r| r.account_uid_fingerprint.as_str()))
        .or_else(|| source.map(|e| e.account_uid.as_str()))
        .unwrap_or("");
    let old_uid: Option<String> = tx
        .query_row("SELECT uid FROM ledger_state WHERE key=1", [], |r| r.get(0))
        .optional()
        .map_err(io)?;
    // An authenticated account binding is immutable once present, including
    // on full-snapshot writes. Staging without such a binding is still legal.
    let bound_uid: Option<String> = tx
        .query_row(
            "SELECT uid_fingerprint FROM account_identity WHERE singleton=1",
            [],
            |r| r.get(0),
        )
        .optional()
        .map_err(io)?;
    if bound_uid.as_deref().is_some_and(|bound| bound != uid) {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    if uid.is_empty() && (!entries.is_empty() || reservation.is_some() || source.is_some()) {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    if entries
        .values()
        .any(|e| e.record.plan.account_uid_fingerprint != uid)
        || reservation.is_some_and(|r| r.account_uid_fingerprint != uid)
        || source.is_some_and(|e| e.account_uid != uid)
    {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    if old_uid
        .as_deref()
        .is_some_and(|u| !u.is_empty() && u != uid)
    {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    // A delta is legal only after a fully persisted, account-bound snapshot
    // already exists. Otherwise it could silently open a new empty account.
    if !complete_snapshot && old_uid.as_deref() != Some(uid) {
        return Err(ExecutionLedgerError::Corrupt(
            "native delta requires existing owner state",
        ));
    }
    if complete_snapshot {
        let payload = encode(&StoredState {
            version: 1,
            stop: stop.cloned(),
            reservation: reservation.cloned(),
            source_run_id: source.map(|s| s.run_id.clone()),
            periods: periods.clone(),
            leverage: leverage.to_vec(),
        })?;
        tx.execute(
            "INSERT INTO ledger_state VALUES(1,1,?1,?2,?3)
         ON CONFLICT(key) DO UPDATE SET uid=excluded.uid,
           digest=excluded.digest,bytes=excluded.bytes
         WHERE ledger_state.digest<>excluded.digest",
            params![uid, checksum(&payload), payload],
        )
        .map_err(io)?;
    }
    {
        let mut q = tx
            .prepare(
                "INSERT INTO ledger_entries VALUES(?1,?2,?3,?4,?5,?6)
             ON CONFLICT(intent_id) DO UPDATE SET
               updated_at_ms=excluded.updated_at_ms,
               digest=excluded.digest,bytes=excluded.bytes
             WHERE ledger_entries.digest<>excluded.digest
               AND ledger_entries.client_order_id=excluded.client_order_id
               AND ledger_entries.uid=excluded.uid",
            )
            .map_err(io)?;
        for (id, e) in entries {
            // The Rust owner supplies a complete snapshot for now. Compare
            // indexed digests and fetch the old BLOB only for a changed
            // entry; a steady-state trade must not re-read every payload.
            let bytes = encode(e)?;
            let entry_digest = checksum(&bytes);
            let existing: Option<(String, String, String, i64)> = tx
                .query_row(
                    "SELECT client_order_id,uid,digest,updated_at_ms
                     FROM ledger_entries WHERE intent_id=?1",
                    params![id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()
                .map_err(io)?;
            if existing
                .as_ref()
                .is_some_and(|(client, existing_uid, _, _)| {
                    client != &e.record.plan.client_order_id
                        || existing_uid != &e.record.plan.account_uid_fingerprint
                })
            {
                return Err(ExecutionLedgerError::IntentConflict);
            }
            if existing
                .as_ref()
                .is_some_and(|(_, _, digest, _)| digest == &entry_digest)
            {
                continue;
            }
            if existing.as_ref().is_some_and(|(_, _, _, prior_time)| {
                i64::try_from(e.updated_at_ms)
                    .ok()
                    .is_none_or(|t| t <= *prior_time)
            }) {
                return Err(ExecutionLedgerError::InvalidTimestamp);
            }
            let prior_state = if let Some((_, _, prior_digest, _)) = &existing {
                let prior_bytes: Vec<u8> = tx
                    .query_row(
                        "SELECT bytes FROM ledger_entries WHERE intent_id=?1",
                        params![id],
                        |row| row.get(0),
                    )
                    .map_err(io)?;
                if checksum(&prior_bytes) != *prior_digest {
                    return Err(ExecutionLedgerError::Corrupt(
                        "prior intent digest mismatch",
                    ));
                }
                let old: ExecutionLedgerEntry = decode(&prior_bytes)?;
                // BEGIN_POST and every later transition retain the EXACT
                // immutable prepared plan, including risk mandate and
                // instrument. A matching client-order-id alone is not a
                // legitimate plan identity: never relabel old venue risk.
                if old.record.plan != e.record.plan || old.created_at_ms != e.created_at_ms {
                    return Err(ExecutionLedgerError::IntentConflict);
                }
                Some(old.record.state)
            } else {
                None
            };
            let is_begin_post = prior_state == Some(ExecutionState::Prepared)
                && e.record.state == ExecutionState::Submitting;
            // Existing post records are durable fences: PREPARED cannot
            // re-enter after BEGIN_POST, even through an erroneous caller.
            let posted: Option<String> = tx
                .query_row(
                    "SELECT client_order_id FROM post_attempts WHERE intent_id=?1",
                    params![id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(io)?;
            if posted
                .as_deref()
                .is_some_and(|client| client != e.record.plan.client_order_id)
                || posted.is_some() && e.record.state == ExecutionState::Prepared
                || posted.is_some() && is_begin_post
            {
                return Err(ExecutionLedgerError::IntentConflict);
            }
            let written = q
                .execute(params![
                    id,
                    &e.record.plan.client_order_id,
                    &e.record.plan.account_uid_fingerprint,
                    e.updated_at_ms,
                    &entry_digest,
                    bytes
                ])
                .map_err(io)?;
            if written != 1 {
                return Err(ExecutionLedgerError::Corrupt(
                    "SQLite intent transition lost",
                ));
            }
            let plan_hash = checksum(&encode(&e.record.plan)?);
            let stage = format!("{:?}", e.record.state);
            let action = format!("{:?}", e.record.plan.action);
            let projection = tx.execute(
                "INSERT INTO intents
                   (intent_id,epoch_id,decision_id,instrument_id,requested_action,plan_sha256,current_state,created_at_ms)
                 VALUES (?1,NULL,NULL,?2,?3,?4,?5,?6)
                 ON CONFLICT(intent_id) DO UPDATE SET current_state=excluded.current_state
                 WHERE intents.instrument_id=excluded.instrument_id
                   AND intents.requested_action=excluded.requested_action
                   AND intents.plan_sha256=excluded.plan_sha256
                   AND intents.created_at_ms=excluded.created_at_ms",
                params![id, &e.record.plan.instrument_id, action, plan_hash, stage, e.created_at_ms],
            ).map_err(io)?;
            if projection != 1 {
                return Err(ExecutionLedgerError::Corrupt(
                    "SQLite intent projection lost",
                ));
            }
            // Same WAL transaction as the authoritative intent and its
            // relational plan projection. A terminal historical intent
            // immediately leaves this active-only indexed inventory.
            if needs_instrument_guard(e) {
                tx.execute(
                    "INSERT INTO native_guarded_intents
                     (intent_id,instrument_id,entry_digest) VALUES(?1,?2,?3)
                     ON CONFLICT(intent_id) DO UPDATE SET
                         instrument_id=excluded.instrument_id,
                         entry_digest=excluded.entry_digest",
                    params![id, &e.record.plan.instrument_id, &entry_digest],
                )
                .map_err(io)?;
            } else {
                tx.execute(
                    "DELETE FROM native_guarded_intents WHERE intent_id=?1",
                    params![id],
                )
                .map_err(io)?;
            }
            let last: Option<(i64, String)> = tx
                .query_row(
                    "SELECT seq,event_sha256 FROM domain_events ORDER BY seq DESC LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(io)?;
            let event_sequence = last.as_ref().map_or(1, |(seq, _)| seq + 1);
            let event_id = format!("sqlite-intent-{event_sequence}");
            let prev_hash = last.as_ref().map(|(_, hash)| hash.as_str());
            let event_hash = checksum(&encode(&(
                "okx.sqlite.intent-transition/v1",
                prev_hash.as_ref(),
                &event_id,
                id,
                &entry_digest,
                e.updated_at_ms,
            ))?);
            tx.execute(
                "INSERT INTO domain_events
                 (event_id,event_type,event_version,subject_kind,subject_id,
                  effective_at_ms,recorded_at_ms,previous_event_sha256,event_sha256)
                 VALUES (?1,?6,1,'INTENT',?2,?3,?3,?4,?5)",
                params![
                    event_id,
                    id,
                    e.updated_at_ms,
                    prev_hash,
                    event_hash,
                    if is_begin_post {
                        "BEGIN_POST"
                    } else {
                        "INTENT_TRANSITION"
                    }
                ],
            )
            .map_err(io)?;
            if is_begin_post {
                // This row proves that the old PREPARED record existed
                // before a native durable SUBMITTING transition. Imported
                // historical SUBMITTING/UNKNOWN rows do not fabricate it.
                tx.execute(
                    "INSERT INTO post_attempts
                     (intent_id,client_order_id,begin_post_event_id,
                      begin_post_at_ms,delivery_outcome)
                     VALUES(?1,?2,?3,?4,'BEGIN_POST_UNRESOLVED')",
                    params![
                        id,
                        &e.record.plan.client_order_id,
                        event_id,
                        e.updated_at_ms
                    ],
                )
                .map_err(io)?;
            }
        }
    }
    // A full import/snapshot must prove it has not silently dropped an old
    // intent. A native per-intent delta never claims to replace the entire
    // account, so counting all historical rows on every POST is unnecessary.
    // Existing delta rows are independently validated by exact ID/digest
    // and must not mutate unrelated history.
    if complete_snapshot {
        let persisted: i64 = tx
            .query_row("SELECT COUNT(*) FROM ledger_entries", [], |row| row.get(0))
            .map_err(io)?;
        if usize::try_from(persisted).ok() != Some(entries.len()) {
            return Err(ExecutionLedgerError::Corrupt(
                "SQLite historical intent deletion",
            ));
        }
    }
    // Existing intents are inserted before projecting fill attribution, so a
    // foreign-key-bound source_intent_id is proved in this same transaction.
    if complete_snapshot && let Some(evidence) = source {
        super::sqlite_source::persist(&tx, evidence)?;
    }
    tx.commit().map_err(io)?;
    Ok(())
}

/// Immutable, source-bound origin proof for one staged JSON-v14 -> SQLite
/// transfer. It does NOT authorize or perform a production owner cutover.
pub(super) fn seal_staging_origin(
    path: &Path,
    expected_uid: &str,
    source_sha256: &str,
) -> Result<(), ExecutionLedgerError> {
    if expected_uid.is_empty()
        || source_sha256.len() != 64
        || !source_sha256
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(ExecutionLedgerError::Corrupt(
            "invalid staging origin identity",
        ));
    }
    let mut c = connect(path, true)?;
    let tx = c
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io)?;
    let (uid, digest, bytes): (String, String, Vec<u8>) = tx
        .query_row(
            "SELECT uid,digest,bytes FROM ledger_state WHERE key=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(io)?;
    if uid != expected_uid || checksum(&bytes) != digest {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS native_migration_origin(
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            uid TEXT NOT NULL,
            original_v14_sha256 TEXT NOT NULL
         ) STRICT;
         CREATE TRIGGER IF NOT EXISTS immutable_native_origin_update
         BEFORE UPDATE ON native_migration_origin BEGIN
           SELECT RAISE(ABORT,'IMMUTABLE_ORIGIN'); END;
         CREATE TRIGGER IF NOT EXISTS immutable_native_origin_delete
         BEFORE DELETE ON native_migration_origin BEGIN
           SELECT RAISE(ABORT,'IMMUTABLE_ORIGIN'); END;",
    )
    .map_err(io)?;
    tx.execute(
        "INSERT INTO native_migration_origin VALUES(1,?1,?2)
         ON CONFLICT(singleton) DO NOTHING",
        params![expected_uid, source_sha256],
    )
    .map_err(io)?;
    let (recorded_uid, recorded_hash): (String, String) = tx
        .query_row(
            "SELECT uid,original_v14_sha256 FROM native_migration_origin WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(io)?;
    if recorded_uid != expected_uid || recorded_hash != source_sha256 {
        return Err(ExecutionLedgerError::Corrupt("migration origin conflict"));
    }
    tx.commit().map_err(io)?;
    Ok(())
}

/// The immutable source hash survives subsequent writes to the same SQLite
/// Ledger; unlike a live manifest digest it remains stable after OPEN/CLOSE.
pub(super) fn read_staging_origin(path: &Path) -> Result<(String, String), ExecutionLedgerError> {
    let c = connect(path, false)?;
    let (uid, source): (String, String) = c
        .query_row(
            "SELECT uid,original_v14_sha256 FROM native_migration_origin WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(io)?;
    if uid.is_empty() || source.len() != 64 {
        return Err(ExecutionLedgerError::Corrupt(
            "missing staging origin proof",
        ));
    }
    Ok((uid, source))
}

/// One consistent WAL snapshot feeds both independent read-only
/// attestation and CONTROL handoff inventory. Never pair counts from a
/// newer SQL transaction with an older account digest.
/// Published-owner verification streams all historical facts and then
/// checks the immutable account identity without materializing old intents.
pub(super) fn attest_published_owner(
    path: &Path,
    expected_uid: &str,
) -> Result<(), ExecutionLedgerError> {
    attest_snapshot(path, None)?;
    let c = connect_indexed_read(path)?;
    let (uid, digest, bytes): (String, String, Vec<u8>) = c
        .query_row(
            "SELECT uid,digest,bytes FROM ledger_state WHERE key=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(io)?;
    if uid != expected_uid || checksum(&bytes) != digest {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    let state: StoredState = decode(&bytes)?;
    let present: i64 = c
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM ledger_entries LIMIT 1)",
            [],
            |r| r.get(0),
        )
        .map_err(io)?;
    if state.reservation.is_none() && state.source_run_id.is_none() && present == 0 {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    Ok(())
}

pub(super) fn assess_profile(
    path: &Path,
    account_uid: &str,
) -> Result<ProfileHandoffLedgerAssessment, ExecutionLedgerError> {
    let (_, profile) = attest_snapshot(path, Some(account_uid))?;
    profile.ok_or(ExecutionLedgerError::Corrupt(
        "native profile snapshot absent",
    ))
}

pub(super) fn attest(path: &Path) -> Result<LedgerStorageAttestation, ExecutionLedgerError> {
    Ok(attest_snapshot(path, None)?.0)
}

/// Exhaustive read-only attestation without a second full historical
/// BTreeMap. The snapshot also carries exact operational counters when
/// called for native CONTROL handoff.
fn attest_snapshot(
    path: &Path,
    expected_uid: Option<&str>,
) -> Result<
    (
        LedgerStorageAttestation,
        Option<ProfileHandoffLedgerAssessment>,
    ),
    ExecutionLedgerError,
> {
    let c = connect(path, false)?; // FULL integrity_check at cold boundary
    // Hold one consistent WAL snapshot through manifest, source and fences.
    c.execute_batch("BEGIN DEFERRED TRANSACTION").map_err(io)?;
    let (uid, digest, bytes): (String, String, Vec<u8>) = c
        .query_row(
            "SELECT uid,digest,bytes FROM ledger_state WHERE key=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(io)?;
    if checksum(&bytes) != digest {
        return Err(ExecutionLedgerError::Corrupt(
            "SQLite state digest mismatch",
        ));
    }
    let state: StoredState = decode(&bytes)?;
    if state.version != 1 {
        return Err(ExecutionLedgerError::Corrupt("SQLite data version"));
    }
    let bound: Option<String> = c
        .query_row(
            "SELECT uid_fingerprint FROM account_identity WHERE singleton=1",
            [],
            |r| r.get(0),
        )
        .optional()
        .map_err(io)?;
    if bound.as_deref().is_some_and(|value| value != uid)
        || expected_uid.is_some_and(|value| value != uid || bound.as_deref() != Some(value))
    {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    if let Some(stop) = &state.stop {
        validate_risk_stop(stop)?;
    }
    if let Some(reservation) = &state.reservation {
        validate_capital_reservation(reservation)?;
    }
    if state
        .reservation
        .as_ref()
        .is_some_and(|r| r.account_uid_fingerprint != uid)
    {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    let source = state
        .source_run_id
        .as_deref()
        .map(|run_id| super::sqlite_source::read(&c, run_id, &uid))
        .transpose()?;
    state
        .periods
        .validate(state.reservation.as_ref(), source.as_ref())?;
    validate_leverage_attempts(&state.leverage, state.reservation.as_ref())?;

    let mut owner_digest = Sha256::new();
    owner_digest.update(b"okx.sqlite.ledger/v1");
    owner_digest.update(digest.as_bytes());
    if let Some(source) = &source {
        owner_digest.update(source.checksum_sha256.as_bytes());
    }
    let sql = format!("{INDEXED_INTENT_COLUMNS} ORDER BY e.intent_id");
    let mut statement = c.prepare(&sql).map_err(io)?;
    let mut rows = statement.query([]).map_err(io)?;
    let mut guards_stmt = c
        .prepare(
            "SELECT intent_id,instrument_id,entry_digest
             FROM native_guarded_intents ORDER BY intent_id",
        )
        .map_err(io)?;
    let mut guards = guards_stmt.query([]).map_err(io)?;
    let mut count: i64 = 0;
    // [total, nonterminal, unresolved mutation, unresolved protection].
    // No second SQL pass and no BTreeMap allocation for the same account.
    let mut profile_counts = expected_uid.map(|_| [0usize; 4]);
    while let Some(row) = rows.next().map_err(io)? {
        let entry = verified_indexed_entry(row, &uid)?;
        validate_strategy_run_binding(
            &entry,
            state.reservation.as_ref(),
            &state.periods.archived_runs,
            &state.periods.unverified_runs,
        )?;
        if let Some(counts) = &mut profile_counts {
            counts[0] += 1;
            if !entry.record.state.is_terminal() {
                counts[1] += 1;
            }
            counts[2] += entry
                .record
                .mutations
                .iter()
                .filter(|change| !change.state.is_terminal())
                .count();
            if let Some(protective) = entry.record.protection.as_ref()
                && (!matches!(
                    protective.status,
                    ProtectiveOrderStatus::CleanedUp | ProtectiveOrderStatus::NotActivated
                ) || protective
                    .cleanup
                    .as_ref()
                    .is_some_and(|c| c.state != ProtectiveCleanupState::ConfirmedAbsent)
                    || protective.size_amend.as_ref().is_some_and(|amend| {
                        !matches!(
                            amend.state,
                            ProtectiveSizeAmendState::Confirmed
                                | ProtectiveSizeAmendState::RetiredAbsent
                        )
                    }))
            {
                counts[3] += 1;
            }
        }
        let id = &entry.record.plan.intent_id;
        let payload = encode(&entry)?;
        if needs_instrument_guard(&entry) {
            let guard = guards
                .next()
                .map_err(io)?
                .ok_or(ExecutionLedgerError::Corrupt(
                    "missing native instrument guard",
                ))?;
            let guard_id: String = guard.get(0).map_err(io)?;
            let instrument: String = guard.get(1).map_err(io)?;
            let digest: String = guard.get(2).map_err(io)?;
            if guard_id != *id
                || instrument != entry.record.plan.instrument_id
                || digest != checksum(&payload)
            {
                return Err(ExecutionLedgerError::Corrupt(
                    "native instrument guard mismatch",
                ));
            }
        }
        owner_digest.update((id.len() as u64).to_le_bytes());
        owner_digest.update(id.as_bytes());
        owner_digest.update((payload.len() as u64).to_le_bytes());
        owner_digest.update(&payload);
        count = count
            .checked_add(1)
            .ok_or(ExecutionLedgerError::Corrupt("SQLite inventory overflow"))?;
    }
    if guards.next().map_err(io)?.is_some() {
        return Err(ExecutionLedgerError::Corrupt(
            "extra native instrument guard",
        ));
    }
    drop(guards);
    drop(guards_stmt);
    drop(rows);
    drop(statement);
    let (indexed_count, entry_count): (i64, i64) = c
        .query_row(
            "SELECT (SELECT COUNT(*) FROM intents),
                    (SELECT COUNT(*) FROM ledger_entries)",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(io)?;
    if count != entry_count || count != indexed_count {
        return Err(ExecutionLedgerError::Corrupt(
            "SQLite historical intent projection inventory",
        ));
    }
    // An empty database is legal only as an explicitly created staging
    // snapshot. The *published owner* transition independently requires
    // a source-proven UID and nonempty v14 evidence; attestation itself is
    // a read-only integrity check, never trading admission.
    // Every native BEGIN_POST fence must remain linked to an exact intent,
    // original client ID and append-only event, including after crashes.
    // One streaming SQL join replaces a historical in-memory index.
    let mut fence_stmt = c
        .prepare(
            "SELECT e.intent_id,e.client_order_id,e.uid,e.digest,e.bytes,
                    i.instrument_id,i.requested_action,i.plan_sha256,i.current_state,i.created_at_ms,
                    p.client_order_id,p.begin_post_at_ms,
                    d.event_type,d.subject_id,d.effective_at_ms
             FROM post_attempts p
             JOIN domain_events d ON d.event_id=p.begin_post_event_id
             JOIN ledger_entries e ON e.intent_id=p.intent_id
             LEFT JOIN intents i ON i.intent_id=e.intent_id",
        )
        .map_err(io)?;
    let mut fence_rows = fence_stmt.query([]).map_err(io)?;
    let mut fence_count: i64 = 0;
    while let Some(row) = fence_rows.next().map_err(io)? {
        let entry = verified_indexed_entry(row, &uid)?;
        let client: String = row.get(10).map_err(io)?;
        let at: i64 = row.get(11).map_err(io)?;
        let kind: String = row.get(12).map_err(io)?;
        let subject: String = row.get(13).map_err(io)?;
        let effective: i64 = row.get(14).map_err(io)?;
        if client != entry.record.plan.client_order_id
            || kind != "BEGIN_POST"
            || subject != entry.record.plan.intent_id
            || at != effective
            || entry.record.state == ExecutionState::Prepared
        {
            return Err(ExecutionLedgerError::Corrupt("native post fence mismatch"));
        }
        fence_count += 1;
    }
    drop(fence_rows);
    drop(fence_stmt);
    let all_fences: i64 = c
        .query_row("SELECT COUNT(*) FROM post_attempts", [], |r| r.get(0))
        .map_err(io)?;
    if fence_count != all_fences {
        return Err(ExecutionLedgerError::Corrupt(
            "native post fence has missing source",
        ));
    }
    let full_manifest = format!("{:x}", owner_digest.finalize());
    let physical = LedgerStorageAttestation {
        schema: LEDGER_STORAGE_ATTESTATION_SCHEMA_V1,
        on_disk_schema: SCHEMA.to_owned(),
        integrity: "VALIDATED_COLD_READ",
        migration: "SQLITE_NATIVE_OWNER",
        manifest_sha256: full_manifest,
        manifest_bytes: fs::metadata(path)?.len(),
        sealed_finalized_runs: state.periods.archived_runs.len(),
        sealed_unverified_runs: state.periods.unverified_runs.len(),
        verified_archive_bodies: state.periods.archived_runs.len()
            + state.periods.unverified_runs.len(),
        original_snapshot: "SQLITE_NO_LEGACY_FALLBACK",
        original_snapshot_sha256: None,
        active_risk_stop_reason: state.stop.as_ref().map(|v| v.reason.clone()),
        finalized_run_checksum_sha256: state
            .periods
            .finalized_run
            .as_ref()
            .map(|f| f.checksum_sha256.clone()),
        backup_scope: "SQLITE_WAL_AND_IMMUTABLE_SOURCE",
        trading_admission: "READ_ONLY_NOT_TRADING_AUTHORITY",
    };
    let profile = profile_counts.map(|counts| ProfileHandoffLedgerAssessment {
        schema: PROFILE_HANDOFF_LEDGER_ASSESSMENT_SCHEMA_V1,
        managed_intents_total: counts[0],
        nonterminal_intents: counts[1],
        unresolved_order_mutations: counts[2],
        unresolved_protective_lifecycle: counts[3],
        account_uid_mismatches: 0,
        source_ledger_manifest_sha256: physical.manifest_sha256.clone(),
        source_ledger_format: physical.on_disk_schema.clone(),
        immutable_stop_reason: physical.active_risk_stop_reason.clone(),
        finalized_run_checksum_sha256: physical.finalized_run_checksum_sha256.clone(),
    });
    Ok((physical, profile))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_root(label: &str) -> PathBuf {
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("okx-sqlite-{label}-{}-{n}", std::process::id()))
    }

    #[test]
    fn streaming_attestation_matches_canonical_full_history_manifest() {
        let root = test_root("streaming-attestation");
        let path = root.join("account.sqlite");
        let uid = "a".repeat(64);
        let mut entries = BTreeMap::new();
        for index in 0..384_u64 {
            let id = format!("historical_attestation_intent_{index:06}");
            let plan = ExecutionPlan {
                schema: EXECUTION_PLAN_SCHEMA_V1.to_owned(),
                intent_id: id.clone(),
                client_order_id: derive_client_order_id(&id),
                reference_generation: "sha256:reference".to_owned(),
                account_generation: "sha256:account".to_owned(),
                account_uid_fingerprint: uid.clone(),
                instrument_id: "BTC-USDT-SWAP".to_owned(),
                trade_mode: crate::TradeMode::Cross,
                side: crate::OrderSide::Buy,
                position_side: PositionSide::Long,
                action: ExecutionAction::Open,
                order_type: crate::OrderType::Limit,
                size: "1".to_owned(),
                price: "0.1".to_owned(),
                open_risk: None,
                risk_binding: None,
            };
            entries.insert(
                id,
                ExecutionLedgerEntry {
                    record: ExecutionRecord::new(plan),
                    created_at_ms: index + 1000,
                    updated_at_ms: index + 1000,
                },
            );
        }
        save(
            &path,
            &entries,
            None,
            None,
            None,
            &StrategyPeriodHistory::default(),
            &[],
        )
        .unwrap();
        let store = ExecutionLedgerStore::sqlite_at(&path);
        let scope = SqliteAccountIdentity {
            venue: LedgerVenue::Okx,
            environment: LedgerEnvironment::Demo,
            account_id: "demo".to_owned(),
            uid_fingerprint: uid.clone(),
            bound_at_ms: 2000,
        };
        store.bind_native_account_identity(&scope).unwrap();
        let state = Connection::open(&path).unwrap();
        let state_digest: String = state
            .query_row("SELECT digest FROM ledger_state WHERE key=1", [], |r| {
                r.get(0)
            })
            .unwrap();
        drop(state);
        let mut expected = Sha256::new();
        expected.update(b"okx.sqlite.ledger/v1");
        expected.update(state_digest.as_bytes());
        for (id, entry) in &entries {
            let payload = encode(entry).unwrap();
            expected.update((id.len() as u64).to_le_bytes());
            expected.update(id.as_bytes());
            expected.update((payload.len() as u64).to_le_bytes());
            expected.update(&payload);
        }
        let full = store.attest_storage().unwrap();
        assert_eq!(full.manifest_sha256, format!("{:x}", expected.finalize()));
        assert_eq!(store.load().unwrap().0, entries);
        assert_eq!(
            store.attest_storage().unwrap().manifest_sha256,
            full.manifest_sha256
        );
        // Account handoff also streams the same verified SQL rows rather than
        // counting the Engine's historical BTreeMap.
        let ledger = DurableExecutionLedger::open(store.clone(), 3000).unwrap();
        let report = ledger.profile_handoff_ledger_assessment(&uid).unwrap();
        assert_eq!(report.managed_intents_total, 384);
        assert_eq!(report.nonterminal_intents, 384);
        assert_eq!(report.unresolved_order_mutations, 0);
        assert_eq!(report.unresolved_protective_lifecycle, 0);
        assert_eq!(report.account_uid_mismatches, 0);
        assert_eq!(report.source_ledger_manifest_sha256, full.manifest_sha256);
        assert!(
            ledger
                .profile_handoff_ledger_assessment(&"b".repeat(64))
                .is_err()
        );
        drop(ledger);

        // A forged index cannot be papered over by hashing only the
        // authoritative BLOB or only a subset of historical rows.
        let c = Connection::open(&path).unwrap();
        c.execute(
            "UPDATE intents SET current_state='FAKE' WHERE intent_id=?1",
            params!["historical_attestation_intent_000211"],
        )
        .unwrap();
        drop(c);
        assert!(store.attest_storage().is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn sqlite_intent_plan_is_immutable_after_first_persist() {
        let root = test_root("plan-immutable");
        let db = root.join("account.sqlite");
        let uid = "a".repeat(64);
        let id = "intent_sqlite_plan_immutability_0123";
        let plan = ExecutionPlan {
            schema: EXECUTION_PLAN_SCHEMA_V1.to_owned(),
            intent_id: id.to_owned(),
            client_order_id: derive_client_order_id(id),
            reference_generation: "sha256:reference".to_owned(),
            account_generation: "sha256:account".to_owned(),
            account_uid_fingerprint: uid.clone(),
            instrument_id: "BTC-USDT-SWAP".to_owned(),
            trade_mode: crate::TradeMode::Cross,
            side: crate::OrderSide::Buy,
            position_side: PositionSide::Long,
            action: ExecutionAction::Open,
            order_type: crate::OrderType::Limit,
            size: "1".to_owned(),
            price: "0.1".to_owned(),
            open_risk: None,
            risk_binding: None,
        };
        let original = ExecutionLedgerEntry {
            record: ExecutionRecord::new(plan),
            created_at_ms: 1000,
            updated_at_ms: 1000,
        };
        save(
            &db,
            &BTreeMap::from([(id.to_owned(), original.clone())]),
            None,
            None,
            None,
            &StrategyPeriodHistory::default(),
            &[],
        )
        .unwrap();
        let store = ExecutionLedgerStore::sqlite_at(&db);
        store
            .bind_native_account_identity(&SqliteAccountIdentity {
                venue: LedgerVenue::Okx,
                environment: LedgerEnvironment::Demo,
                account_id: "demo".to_owned(),
                uid_fingerprint: uid.clone(),
                bound_at_ms: 1001,
            })
            .unwrap();
        let mut changed = original.clone();
        changed.record.plan.instrument_id = "ETH-USDT-SWAP".to_owned();
        changed.updated_at_ms += 2;
        assert!(matches!(
            save_one(
                &db,
                &changed,
                None,
                None,
                None,
                &StrategyPeriodHistory::default(),
                &[],
            ),
            Err(ExecutionLedgerError::IntentConflict)
        ));
        assert_eq!(
            store.indexed_intent(&uid, id).unwrap(),
            Some(original.clone())
        );

        changed = original.clone();
        changed.created_at_ms += 1;
        changed.updated_at_ms += 2;
        assert!(matches!(
            save_one(
                &db,
                &changed,
                None,
                None,
                None,
                &StrategyPeriodHistory::default(),
                &[],
            ),
            Err(ExecutionLedgerError::IntentConflict)
        ));
        assert_eq!(
            store.indexed_intent(&uid, id).unwrap(),
            Some(original.clone())
        );

        // A forged projection is rejected in the SAME transaction that
        // attempts to advance an otherwise valid authoritative intent.
        let c = Connection::open(&db).unwrap();
        c.execute(
            "UPDATE intents SET plan_sha256=?1 WHERE intent_id=?2",
            params!["f".repeat(64), id],
        )
        .unwrap();
        drop(c);
        changed = original.clone();
        changed.updated_at_ms += 2;
        assert!(
            save_one(
                &db,
                &changed,
                None,
                None,
                None,
                &StrategyPeriodHistory::default(),
                &[],
            )
            .is_err()
        );
        let c = Connection::open(&db).unwrap();
        let old_time: i64 = c
            .query_row(
                "SELECT updated_at_ms FROM ledger_entries WHERE intent_id=?1",
                params![id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(old_time, 1000);
        drop(c);
        assert!(store.indexed_intent(&uid, id).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn sqlite_native_account_identity_is_single_permanent_and_uid_scoped() {
        let root = test_root("account-identity");
        let path = root.join("account.sqlite");
        let uid = "b".repeat(64);
        let reservation = StrategyCapitalReservation {
            schema: STRATEGY_CAPITAL_RESERVATION_SCHEMA_V1.to_owned(),
            allocation_id: "account_identity_test_run_000001".to_owned(),
            account_uid_fingerprint: uid.clone(),
            origin_schema: "test/v1".to_owned(),
            origin_version: "v1".to_owned(),
            mandate_version: "1".to_owned(),
            allocated_usdt: "100".to_owned(),
            opening_account_usdt_equity: "127".to_owned(),
            account_generation: "g1".to_owned(),
            operator_authority_id: format!("sha256:{}", "a".repeat(64)),
            started_at_ms: 1,
        };
        let store = ExecutionLedgerStore::sqlite_at(&path);
        assert!(store.bound_native_account_identity().is_err());
        let identity = SqliteAccountIdentity {
            venue: LedgerVenue::Okx,
            environment: LedgerEnvironment::Production,
            account_id: "real".to_owned(),
            uid_fingerprint: uid.clone(),
            bound_at_ms: 1000,
        };
        assert!(store.bind_native_account_identity(&identity).is_err());
        save(
            &path,
            &BTreeMap::new(),
            None,
            Some(&reservation),
            None,
            &StrategyPeriodHistory::default(),
            &[],
        )
        .unwrap();
        assert!(store.bound_native_account_identity().unwrap().is_none());
        store.bind_native_account_identity(&identity).unwrap();
        assert_eq!(
            store.bound_native_account_identity().unwrap(),
            Some(identity.clone())
        );
        let mut same = identity.clone();
        same.bound_at_ms += 1; // Repeated native verification never edits history.
        store.bind_native_account_identity(&same).unwrap();
        assert_eq!(
            store.bound_native_account_identity().unwrap(),
            Some(identity.clone())
        );

        let mut foreign = identity.clone();
        foreign.account_id = "subaccount".to_owned();
        assert!(store.bind_native_account_identity(&foreign).is_err());
        foreign = identity.clone();
        foreign.environment = LedgerEnvironment::Demo;
        assert!(store.bind_native_account_identity(&foreign).is_err());
        foreign = identity.clone();
        foreign.venue = LedgerVenue::Bybit;
        assert!(store.bind_native_account_identity(&foreign).is_err());
        foreign = identity.clone();
        foreign.uid_fingerprint = "c".repeat(64);
        assert!(store.bind_native_account_identity(&foreign).is_err());
        assert!(
            ExecutionLedgerStore::at(root.join("v14.json"))
                .bind_native_account_identity(&identity)
                .is_err()
        );
        let sql = Connection::open(&path).unwrap();
        assert!(
            sql.execute("DELETE FROM account_identity WHERE singleton=1", [])
                .is_err()
        );
        assert!(
            sql.execute(
                "UPDATE account_identity SET account_id='foreign' WHERE singleton=1",
                []
            )
            .is_err()
        );
        drop(sql);
        assert_eq!(
            store.bound_native_account_identity().unwrap(),
            Some(identity)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Historical pages are keyset indexed, fixed-size and UID-bound.
    /// Two DBs have independent native writer leases without a shared
    /// account-global lock or another controller/transport.
    #[test]
    fn bounded_indexed_history_across_independent_native_accounts() {
        let root = test_root("indexed-account-roots");
        for (name, c) in [("real", 'a'), ("demo", 'b')] {
            let account_root = root.join(name);
            let db = account_root.join("execution-ledger.sqlite");
            let uid = c.to_string().repeat(64);
            let mut entries = BTreeMap::new();
            for index in 0..131_u64 {
                let id = format!("indexed_{name}_intent_{index:04}");
                let plan = ExecutionPlan {
                    schema: EXECUTION_PLAN_SCHEMA_V1.to_owned(),
                    intent_id: id.clone(),
                    client_order_id: derive_client_order_id(&id),
                    reference_generation: "sha256:reference".to_owned(),
                    account_generation: "sha256:account".to_owned(),
                    account_uid_fingerprint: uid.clone(),
                    instrument_id: "BTC-USDT-SWAP".to_owned(),
                    trade_mode: crate::TradeMode::Cross,
                    side: crate::OrderSide::Buy,
                    position_side: PositionSide::Long,
                    action: ExecutionAction::Open,
                    order_type: crate::OrderType::Limit,
                    size: "1".to_owned(),
                    price: "0.1".to_owned(),
                    open_risk: None,
                    risk_binding: None,
                };
                entries.insert(
                    id,
                    ExecutionLedgerEntry {
                        record: ExecutionRecord::new(plan),
                        created_at_ms: 1000 + index,
                        updated_at_ms: 1000 + index,
                    },
                );
            }
            let store = ExecutionLedgerStore::sqlite_at(&db);
            store
                .save(
                    &entries,
                    None,
                    None,
                    None,
                    &StrategyPeriodHistory::default(),
                    &[],
                )
                .unwrap();
            let scope = SqliteAccountIdentity {
                venue: LedgerVenue::Okx,
                environment: if name == "demo" {
                    LedgerEnvironment::Demo
                } else {
                    LedgerEnvironment::Production
                },
                account_id: name.to_owned(),
                uid_fingerprint: uid.clone(),
                bound_at_ms: 1111,
            };
            store.bind_native_account_identity(&scope).unwrap();
            assert_eq!(store.indexed_intent(&uid, "not_present").unwrap(), None);
            assert!(
                store
                    .indexed_intent(&"f".repeat(64), "not_present")
                    .is_err()
            );
            assert!(store.indexed_intent_page(&uid, None, 0).is_err());
            assert!(store.indexed_intent_page(&uid, None, 257).is_err());
            let mut cursor: Option<String> = None;
            let mut all = Vec::new();
            for expected_len in [64, 64, 3] {
                let page = store
                    .indexed_intent_page(&uid, cursor.as_deref(), 64)
                    .unwrap();
                assert_eq!(page.entries.len(), expected_len);
                for entry in &page.entries {
                    let id = &entry.record.plan.intent_id;
                    assert_eq!(
                        store.indexed_intent(&uid, id).unwrap().as_ref(),
                        Some(entry)
                    );
                    all.push(id.clone());
                }
                cursor = page.next_after;
            }
            assert_eq!(all, entries.keys().cloned().collect::<Vec<_>>());
            assert!(cursor.is_none());
        }
        let real_root = root.join("real");
        let demo_root = root.join("demo");
        let first = ExecutionLedgerStore::acquire_writer_lease(&real_root).unwrap();
        let second = ExecutionLedgerStore::acquire_writer_lease(&demo_root).unwrap();
        assert!(ExecutionLedgerStore::acquire_writer_lease(&real_root).is_err());
        assert!(ExecutionLedgerStore::acquire_writer_lease(&demo_root).is_err());
        drop(first);
        drop(second);
        // Even a relational-index corruption must never fabricate a valid
        // historical result (and does not authorize an exchange POST).
        let db = real_root.join("execution-ledger.sqlite");
        let sql = Connection::open(&db).unwrap();
        sql.execute(
            "UPDATE intents SET current_state='FORGED' WHERE intent_id='indexed_real_intent_0000'",
            [],
        )
        .unwrap();
        drop(sql);
        assert!(
            ExecutionLedgerStore::sqlite_at(&db)
                .indexed_intent(&"a".repeat(64), "indexed_real_intent_0000")
                .is_err()
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn native_sqlite_transition_survives_evicted_memory_without_replaying_post() {
        let root = test_root("native-cached-intent-eviction");
        let db = root.join("account.sqlite");
        let id = "cache_eviction_native_intent_000001";
        let plan = ExecutionPlan {
            schema: EXECUTION_PLAN_SCHEMA_V1.to_owned(),
            intent_id: id.to_owned(),
            client_order_id: derive_client_order_id(id),
            reference_generation: "sha256:reference".to_owned(),
            account_generation: "sha256:account".to_owned(),
            account_uid_fingerprint: "a".repeat(64),
            instrument_id: "BTC-USDT-SWAP".to_owned(),
            trade_mode: crate::TradeMode::Cross,
            side: crate::OrderSide::Buy,
            position_side: PositionSide::Long,
            action: ExecutionAction::Open,
            order_type: crate::OrderType::Limit,
            size: "1".to_owned(),
            price: "0.1".to_owned(),
            open_risk: None,
            risk_binding: None,
        };
        let prepared = ExecutionLedgerEntry {
            record: ExecutionRecord::new(plan),
            created_at_ms: 1000,
            updated_at_ms: 1000,
        };
        save(
            &db,
            &BTreeMap::from([(id.to_owned(), prepared.clone())]),
            None,
            None,
            None,
            &StrategyPeriodHistory::default(),
            &[],
        )
        .unwrap();
        let store = ExecutionLedgerStore::sqlite_at(&db);
        let mut engine = DurableExecutionLedger::open(store.clone(), 1001).unwrap();
        // The SQLite row, not an in-memory historical BTreeMap, must own
        // both the precise transition and the original immutable plan.
        engine.entries.clear();
        assert!(engine.get(id).is_none());
        assert_eq!(owner_intent(&db, id).unwrap(), Some(prepared));
        let submitting = engine.begin_submission(id, 1002).unwrap();
        assert_eq!(submitting.record.state, ExecutionState::Submitting);
        assert_eq!(engine.get(id), Some(&submitting));
        drop(engine);
        let mut reopened = DurableExecutionLedger::open(store, 1003).unwrap();
        assert_eq!(
            reopened.get(id).unwrap().record.state,
            ExecutionState::UnknownSubmission
        );
        let c = Connection::open(&db).unwrap();
        let post_count: i64 = c
            .query_row("SELECT COUNT(*) FROM post_attempts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(post_count, 1);
        assert!(reopened.begin_submission(id, 1004).is_err());
        // Windows keeps the SQLite file locked while any connection or
        // prepared owner is alive. Explicitly release both before cleanup.
        drop(c);
        drop(reopened);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn native_runtime_cold_open_does_not_materialize_terminal_history() {
        let root = test_root("bounded-native-runtime-open");
        let db = root.join("account.sqlite");
        let uid = "a".repeat(64);
        let mut entries = BTreeMap::new();
        for index in 0..512_u64 {
            let id = format!("bounded_historical_intent_{index:06}");
            let plan = ExecutionPlan {
                schema: EXECUTION_PLAN_SCHEMA_V1.to_owned(),
                intent_id: id.clone(),
                client_order_id: derive_client_order_id(&id),
                reference_generation: "sha256:reference".to_owned(),
                account_generation: "sha256:account".to_owned(),
                account_uid_fingerprint: uid.clone(),
                instrument_id: "BTC-USDT-SWAP".to_owned(),
                trade_mode: crate::TradeMode::Cross,
                side: crate::OrderSide::Buy,
                position_side: PositionSide::Long,
                action: ExecutionAction::Open,
                order_type: crate::OrderType::Limit,
                size: "1".to_owned(),
                price: "0.1".to_owned(),
                open_risk: None,
                risk_binding: None,
            };
            let mut record = ExecutionRecord::new(plan);
            if index < 511 {
                record.abandon_prepared().unwrap();
            }
            entries.insert(
                id,
                ExecutionLedgerEntry {
                    record,
                    created_at_ms: 1000 + index,
                    updated_at_ms: 1000 + index,
                },
            );
        }
        save(
            &db,
            &entries,
            None,
            None,
            None,
            &StrategyPeriodHistory::default(),
            &[],
        )
        .unwrap();
        let store = ExecutionLedgerStore::sqlite_at(&db);
        store
            .bind_native_account_identity(&SqliteAccountIdentity {
                venue: LedgerVenue::Okx,
                environment: LedgerEnvironment::Demo,
                account_id: "demo".to_owned(),
                uid_fingerprint: uid.clone(),
                bound_at_ms: 2000,
            })
            .unwrap();
        let owner = DurableExecutionLedger::open(store.clone(), 2000).unwrap();
        assert_eq!(owner.entries.len(), 1);
        assert_eq!(
            owner.entries.values().next().unwrap().record.state,
            ExecutionState::Prepared
        );
        assert_eq!(store.load().unwrap().0, entries);
        assert_eq!(
            owner
                .profile_handoff_ledger_assessment(&uid)
                .unwrap()
                .managed_intents_total,
            512
        );
        assert_eq!(
            owner
                .account_intent(&uid, "bounded_historical_intent_000001")
                .unwrap(),
            entries.get("bounded_historical_intent_000001").cloned()
        );
        drop(owner);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn native_guard_index_keeps_only_material_intents_and_cannot_lose_one() {
        let root = test_root("active-guard-inventory");
        let db = root.join("account.sqlite");
        let uid = "a".repeat(64);
        let mut entries = BTreeMap::new();
        for index in 0..64_u64 {
            let id = format!("guarded_intent_{index:06}");
            let plan = ExecutionPlan {
                schema: EXECUTION_PLAN_SCHEMA_V1.to_owned(),
                intent_id: id.clone(),
                client_order_id: derive_client_order_id(&id),
                reference_generation: "sha256:reference".to_owned(),
                account_generation: "sha256:account".to_owned(),
                account_uid_fingerprint: uid.clone(),
                instrument_id: "BTC-USDT-SWAP".to_owned(),
                trade_mode: crate::TradeMode::Cross,
                side: crate::OrderSide::Buy,
                position_side: PositionSide::Long,
                action: ExecutionAction::Open,
                order_type: crate::OrderType::Limit,
                size: "1".to_owned(),
                price: "0.1".to_owned(),
                open_risk: None,
                risk_binding: None,
            };
            let mut record = ExecutionRecord::new(plan);
            if index < 63 {
                record.abandon_prepared().unwrap();
            }
            entries.insert(
                id,
                ExecutionLedgerEntry {
                    record,
                    created_at_ms: index + 1000,
                    updated_at_ms: index + 1100,
                },
            );
        }
        save(
            &db,
            &entries,
            None,
            None,
            None,
            &StrategyPeriodHistory::default(),
            &[],
        )
        .unwrap();
        let store = ExecutionLedgerStore::sqlite_at(&db);
        let identity = SqliteAccountIdentity {
            venue: LedgerVenue::Okx,
            environment: LedgerEnvironment::Demo,
            account_id: "demo".to_owned(),
            uid_fingerprint: uid.clone(),
            bound_at_ms: 2000,
        };
        store.bind_native_account_identity(&identity).unwrap();
        let guards = indexed_instrument_guards(&db, &uid, "BTC-USDT-SWAP").unwrap();
        let account_guards = indexed_account_guards(&db, &uid).unwrap();
        assert_eq!(account_guards, guards);
        assert!(indexed_account_guards(&db, &"b".repeat(64)).is_err());
        assert_eq!(guards.len(), 1);
        assert_eq!(guards[0].record.plan.intent_id, "guarded_intent_000063");
        let verified_engine = DurableExecutionLedger::open(store.clone(), 2500).unwrap();
        assert!(verified_engine.account_scope_matches(&uid).unwrap());
        // The indexed temporal query checks authoritative created_at_ms,
        // not only updated_at_ms (older intents can be updated later).
        assert!(
            verified_engine
                .any_entry_created_since(&uid, 1060, |entry| {
                    entry.record.plan.intent_id == "guarded_intent_000063"
                })
                .unwrap()
        );
        assert!(
            !verified_engine
                .any_entry_created_since(&uid, 2000, |_| true)
                .unwrap()
        );
        assert!(
            verified_engine
                .any_entry_created_since(&"b".repeat(64), 1060, |_| false)
                .is_err()
        );
        let selected = verified_engine
            .bounded_run_entries(&uid, 1060, 2, |entry| {
                Some(entry.record.plan.intent_id.clone())
            })
            .unwrap();
        assert_eq!(
            selected,
            vec![
                "guarded_intent_000060".to_owned(),
                "guarded_intent_000061".to_owned()
            ]
        );
        assert!(
            verified_engine
                .bounded_run_entries(&uid, 1060, 33, |_| Some(()))
                .is_err()
        );
        assert!(
            verified_engine
                .account_scope_matches(&"b".repeat(64))
                .is_err()
        );
        assert_eq!(
            verified_engine
                .account_intent(&uid, "guarded_intent_000010")
                .unwrap(),
            entries.get("guarded_intent_000010").cloned()
        );
        assert!(
            verified_engine
                .account_intent(&"b".repeat(64), "guarded_intent_000010")
                .is_err()
        );
        drop(verified_engine);
        let batch = vec!["guarded_intent_000063".to_owned()];
        assert_eq!(
            indexed_exact_prepared_batch(&db, &uid, &batch).unwrap(),
            guards
        );
        assert!(
            indexed_exact_prepared_batch(&db, &uid, &["guarded_intent_000000".to_owned()]).is_err()
        );
        assert!(
            client_order_id_exists(&db, &uid, &derive_client_order_id("guarded_intent_000001"))
                .unwrap()
        );
        assert!(
            !client_order_id_exists(&db, &uid, &derive_client_order_id("guarded_intent_new"))
                .unwrap()
        );
        assert_eq!(load(&db, usize::MAX).unwrap().0, entries);
        // Exercise the actual native prepare path, not just the SQL reader.
        // The old completed BTC history is ignored, but the one pending BTC
        // intent blocks a second BTC submission under the same Rust rule.
        let mut engine = DurableExecutionLedger::open(store.clone(), 3000).unwrap();
        let mut blocked = entries
            .get("guarded_intent_000063")
            .unwrap()
            .record
            .plan
            .clone();
        blocked.intent_id = "guarded_intent_new_000064".to_owned();
        blocked.client_order_id = derive_client_order_id(&blocked.intent_id);
        assert!(matches!(
            engine.prepare(blocked, 4000),
            Err(ExecutionLedgerError::InstrumentBusy)
        ));
        let mut independent = entries
            .get("guarded_intent_000063")
            .unwrap()
            .record
            .plan
            .clone();
        independent.intent_id = "guarded_intent_eth_000065".to_owned();
        independent.client_order_id = derive_client_order_id(&independent.intent_id);
        independent.instrument_id = "ETH-USDT-SWAP".to_owned();
        assert!(matches!(
            engine.prepare(independent, 4001),
            Ok(PrepareDisposition::Created(_))
        ));
        drop(engine);
        let c = Connection::open(&db).unwrap();
        c.execute(
            "DELETE FROM native_guarded_intents WHERE intent_id='guarded_intent_000063'",
            [],
        )
        .unwrap();
        drop(c);
        // Missing an active guard can otherwise cause a duplicate order.
        // Cold ownership checks MUST fail rather than trust an empty index.
        assert!(load(&db, usize::MAX).is_err());
        assert!(store.attest_storage().is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn metadata_wal_commit_preserves_all_historical_intents_and_guard_proofs() {
        let root = test_root("metadata-only");
        let db = root.join("account.sqlite");
        let uid = "a".repeat(64);
        let id = "intent_metadata_000001";
        let plan = ExecutionPlan {
            schema: EXECUTION_PLAN_SCHEMA_V1.to_owned(),
            intent_id: id.to_owned(),
            client_order_id: derive_client_order_id(id),
            reference_generation: "sha256:reference".to_owned(),
            account_generation: "sha256:account".to_owned(),
            account_uid_fingerprint: uid.clone(),
            instrument_id: "BTC-USDT-SWAP".to_owned(),
            trade_mode: crate::TradeMode::Cross,
            side: crate::OrderSide::Buy,
            position_side: PositionSide::Long,
            action: ExecutionAction::Open,
            order_type: crate::OrderType::Limit,
            size: "1".to_owned(),
            price: "0.1".to_owned(),
            open_risk: None,
            risk_binding: None,
        };
        let entry = ExecutionLedgerEntry {
            record: ExecutionRecord::new(plan),
            created_at_ms: 1000,
            updated_at_ms: 1000,
        };
        let entries = BTreeMap::from([(id.to_owned(), entry)]);
        save(
            &db,
            &entries,
            None,
            None,
            None,
            &StrategyPeriodHistory::default(),
            &[],
        )
        .unwrap();
        let original = Connection::open(&db).unwrap();
        let (intent_bytes, guard_digest): (Vec<u8>, String) = original
            .query_row(
                "SELECT e.bytes,g.entry_digest FROM ledger_entries e
                 JOIN native_guarded_intents g ON g.intent_id=e.intent_id
                 WHERE e.intent_id=?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let event_count: i64 = original
            .query_row("SELECT COUNT(*) FROM domain_events", [], |row| row.get(0))
            .unwrap();
        drop(original);
        let stop = ExecutionRiskStop {
            schema: EXECUTION_RISK_STOP_SCHEMA_V1.to_owned(),
            stopped_at_ms: 2000,
            reason: "TEST_DURABLE_STOP".to_owned(),
        };
        save_metadata(
            &db,
            Some(&stop),
            None,
            None,
            &StrategyPeriodHistory::default(),
            &[],
        )
        .unwrap();
        let stored = load(&db, usize::MAX).unwrap();
        assert_eq!(stored.0, entries);
        assert_eq!(stored.1, Some(stop));
        let c = Connection::open(&db).unwrap();
        let (actual, guard): (Vec<u8>, String) = c
            .query_row(
                "SELECT e.bytes,g.entry_digest FROM ledger_entries e
                 JOIN native_guarded_intents g ON g.intent_id=e.intent_id
                 WHERE e.intent_id=?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let after_events: i64 = c
            .query_row("SELECT COUNT(*) FROM domain_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            (actual, guard, after_events),
            (intent_bytes, guard_digest, event_count)
        );
        drop(c);
        assert!(
            save_metadata(
                &root.join("missing.sqlite"),
                None,
                None,
                None,
                &StrategyPeriodHistory::default(),
                &[],
            )
            .is_err()
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn sqlite_owner_persists_and_cold_reads_without_json() {
        let root = test_root("cold");
        let p = root.join("account.sqlite");
        let e = BTreeMap::new();
        save(
            &p,
            &e,
            None,
            None,
            None,
            &StrategyPeriodHistory::default(),
            &[],
        )
        .unwrap();
        let snapshot = load(&p, 10).unwrap();
        assert!(snapshot.0.is_empty());
        assert_eq!(snapshot.4, StrategyPeriodHistory::default());
        let a = attest(&p).unwrap();
        assert_eq!(a.integrity, "VALIDATED_COLD_READ");
        assert_eq!(a.on_disk_schema, SCHEMA);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn offline_v14_to_sqlite_staging_keeps_original_and_is_not_replayable() {
        let root = test_root("migration");
        let original = root.join("source.json");
        let stage = root.join("prepared.sqlite");
        let legacy = ExecutionLedgerStore::at(&original);
        legacy
            .save(
                &BTreeMap::new(),
                None,
                None,
                None,
                &StrategyPeriodHistory::default(),
                &[],
            )
            .unwrap();
        let old = std::fs::read(&original).unwrap();
        let proof = legacy.prepare_sqlite_staging(&stage).unwrap();
        assert_eq!(proof.integrity, "VALIDATED_COLD_READ");
        assert_eq!(std::fs::read(&original).unwrap(), old);
        assert!(legacy.prepare_sqlite_staging(&stage).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn corrupt_sqlite_does_not_create_empty_ledger() {
        let root = test_root("corrupt");
        std::fs::create_dir_all(&root).unwrap();
        let p = root.join("account.sqlite");
        std::fs::write(&p, b"not a sqlite database").unwrap();
        assert!(load(&p, 10).is_err());
        assert!(attest(&p).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn sealed_exchange_sources_roundtrip_with_no_mutation_or_replay() {
        let root = test_root("exchange-evidence");
        let p = root.join("account.sqlite");
        let run = StrategyCapitalReservation {
            schema: STRATEGY_CAPITAL_RESERVATION_SCHEMA_V1.to_owned(),
            allocation_id: "sqlite_test_run_000001".to_owned(),
            account_uid_fingerprint: "uid-test-one".to_owned(),
            origin_schema: "okx.manual-research/v1".to_owned(),
            origin_version: "1".to_owned(),
            mandate_version: "1".to_owned(),
            allocated_usdt: "100".to_owned(),
            opening_account_usdt_equity: "127".to_owned(),
            account_generation: "g1".to_owned(),
            operator_authority_id: format!("sha256:{}", "a".repeat(64)),
            started_at_ms: 1,
        };
        let mut source = DurableExchangeEvidence::new(&run).unwrap();
        source.events.insert(
            "event-1".to_owned(),
            crate::exchange_evidence::StoredExchangeEvent {
                payload: serde_json::json!({
                    "source":"OKX_REST_BILL","billId":"bill-1",
                    "instId":"BTC-USDT-SWAP","tradeId":"trade-1","ccy":"USDT",
                    "type":"2","subType":"5","balChg":"-0.12","ts":1000
                }),
                source_intent_id: None,
                strategy_run_id: None,
                allocation: "UNALLOCATED".to_owned(),
            },
        );
        source.events.insert(
            "fill-1".to_owned(),
            crate::exchange_evidence::StoredExchangeEvent {
                payload: serde_json::json!({
                    "source":"OKX_REST_FILL","instId":"BTC-USDT-SWAP",
                    "tradeId":"trade-1","ordId":"order-1",
                    "side":"buy","posSide":"net","fillPx":"100",
                    "fillSz":"0.01","fee":"-0.12","feeCcy":"USDT","ts":1000
                }),
                source_intent_id: None,
                strategy_run_id: None,
                allocation: "UNALLOCATED".to_owned(),
            },
        );
        source.events.insert(
            "legacy-bill-1".to_owned(),
            crate::exchange_evidence::StoredExchangeEvent {
                payload: serde_json::json!({
                    "source":"OKX_REST_BILL","billId":"legacy-bill-1",
                    "instId":"BTC-USDT-SWAP","ccy":"USDT",
                    "type":null,"subType":"173","balChg":null,"ts":1100
                }),
                source_intent_id: None,
                strategy_run_id: None,
                allocation: "UNALLOCATED".to_owned(),
            },
        );
        source.cursors.insert(
            "receipt-1".to_owned(),
            crate::exchange_evidence::ExchangeCoverageCursor {
                source: "OKX_REST".to_owned(),
                source_received_at: "2026-10-20T12:00:00Z".to_owned(),
                generation: "g1".to_owned(),
                account_as_of_ms: 1000,
                order_complete: true,
                fill_complete: true,
                bill_complete: true,
                covered_ranges: serde_json::json!({}),
            },
        );
        source.checksum_sha256 = checksum(
            &serde_json::to_vec(&(
                &source.schema,
                &source.account_uid,
                &source.run_id,
                &source.events,
                &source.cursors,
            ))
            .unwrap(),
        );
        source.validate().unwrap();
        let history = StrategyPeriodHistory::default();
        save(
            &p,
            &BTreeMap::new(),
            None,
            Some(&run),
            Some(&source),
            &history,
            &[],
        )
        .unwrap();
        let first = load(&p, 10).unwrap();
        assert_eq!(first.3.as_ref(), Some(&source));
        assert_eq!(first.2.as_ref(), Some(&run));
        // The metadata-only path must preserve the exact same source
        // and financial projections as a full original save.
        save_metadata(&p, None, Some(&run), Some(&source), &history, &[]).unwrap();
        assert_eq!(load(&p, 10).unwrap(), first);
        save(
            &p,
            &BTreeMap::new(),
            None,
            Some(&run),
            Some(&source),
            &history,
            &[],
        )
        .unwrap();
        let c = Connection::open(&p).unwrap();
        let count: i64 = c
            .query_row("SELECT COUNT(*) FROM native_source_events", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(count, 3);
        let fill_count: i64 = c
            .query_row("SELECT COUNT(*) FROM fills", [], |r| r.get(0))
            .unwrap();
        let bill_count: i64 = c
            .query_row("SELECT COUNT(*) FROM bills", [], |r| r.get(0))
            .unwrap();
        let gap_count: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM source_gaps WHERE materiality='FINANCIAL'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let delta: String = c
            .query_row(
                "SELECT delta_decimal FROM bills WHERE bill_id='bill-1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!((fill_count, bill_count, gap_count), (1, 1, 1));
        assert_eq!(delta, "-0.12");
        drop(c);
        let snapshot =
            super::sqlite_finance::finance_window(&p, "uid-test-one", 1, 100_000).unwrap();
        assert_eq!(snapshot.bill_rows, 1);
        assert_eq!(snapshot.fill_rows, 1);
        assert_eq!(snapshot.financial_gaps, 1);
        assert_eq!(snapshot.financial_completeness, "INCOMPLETE");
        assert!(
            snapshot
                .buckets
                .iter()
                .any(|v| v.currency == "USDT" && v.exact_balance_delta == "-0.12")
        );
        let read_as_wrong_account =
            super::sqlite_finance::finance_window(&p, "uid-other", 1, 100_000);
        assert!(matches!(
            read_as_wrong_account,
            Err(ExecutionLedgerError::AccountScopeMismatch)
        ));
        let at = crate::exchange_evidence::source_receipt_ms("2026-10-20T12:00:00Z").unwrap();
        let points =
            super::sqlite_finance::finance_window(&p, "uid-test-one", at - 1, at + 1).unwrap();
        assert_eq!(
            (
                points.order_source_receipts,
                points.fill_source_receipts,
                points.bill_source_receipts
            ),
            (1, 1, 1)
        );
        assert_eq!(points.bill_rows, 0);
        assert_eq!(
            points.financial_completeness,
            "UNVERIFIED_WITHOUT_CONTIGUOUS_SOURCE_PROOF"
        );
        let c = Connection::open(&p).unwrap();
        let linked_receipts: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM source_receipts r JOIN evidence_objects e
             ON e.evidence_sha256=r.evidence_sha256
             WHERE r.scope_key='uid-test-one'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(linked_receipts, 3);
        drop(c);

        let original = source.clone();
        source.events.get_mut("event-1").unwrap().payload =
            serde_json::json!({"source":"OKX_REST_BILL","billId":"forged"});
        source.checksum_sha256 = checksum(
            &serde_json::to_vec(&(
                &source.schema,
                &source.account_uid,
                &source.run_id,
                &source.events,
                &source.cursors,
            ))
            .unwrap(),
        );
        assert!(
            save(
                &p,
                &BTreeMap::new(),
                None,
                Some(&run),
                Some(&source),
                &history,
                &[]
            )
            .is_err()
        );
        // No partial financial evidence is committed by a failed
        // account-state/source transaction, and the original manifest
        // remains the only live proof.
        assert!(save_metadata(&p, None, Some(&run), Some(&source), &history, &[]).is_err());
        assert_eq!(load(&p, 10).unwrap().3, Some(original));
        // Financial reports must not accept a forged ledger-state digest,
        // even when the account UID and individual bill rows still look valid.
        let c = Connection::open(&p).unwrap();
        c.execute("UPDATE ledger_state SET digest='forged' WHERE key=1", [])
            .unwrap();
        drop(c);
        assert!(super::sqlite_finance::finance_window(&p, "uid-test-one", 1, 100_000).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn native_begin_post_is_one_durable_fence_and_unknown_is_not_replayable() {
        let root = test_root("post-fence");
        let db = root.join("one-owner.sqlite");
        let id = "native_begin_post_test_000001";
        let plan = ExecutionPlan {
            schema: EXECUTION_PLAN_SCHEMA_V1.to_owned(),
            intent_id: id.to_owned(),
            client_order_id: derive_client_order_id(id),
            reference_generation: "sha256:reference".to_owned(),
            account_generation: "sha256:account".to_owned(),
            account_uid_fingerprint: "uid-fingerprint".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            trade_mode: crate::TradeMode::Cross,
            side: crate::OrderSide::Buy,
            position_side: PositionSide::Long,
            action: ExecutionAction::Open,
            order_type: crate::OrderType::Limit,
            size: "1".to_owned(),
            price: "0.1".to_owned(),
            open_risk: None,
            risk_binding: None,
        };
        let mut entries = BTreeMap::new();
        let mut entry = ExecutionLedgerEntry {
            record: ExecutionRecord::new(plan),
            created_at_ms: 1000,
            updated_at_ms: 1000,
        };
        entries.insert(id.to_owned(), entry.clone());
        let periods = StrategyPeriodHistory::default();
        save(&db, &entries, None, None, None, &periods, &[]).unwrap();
        let conn = Connection::open(&db).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM post_attempts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
        drop(conn);

        entry.record.begin_submission_with_timing(None).unwrap();
        entry.updated_at_ms = 1001;
        entries.insert(id.to_owned(), entry.clone());
        save(&db, &entries, None, None, None, &periods, &[]).unwrap();
        let conn = Connection::open(&db).unwrap();
        let (count, correct): (i64, i64) = (
            conn.query_row("SELECT COUNT(*) FROM post_attempts", [], |r| r.get(0))
                .unwrap(),
            conn.query_row(
                "SELECT COUNT(*) FROM post_attempts p
                 JOIN domain_events e ON e.event_id=p.begin_post_event_id
                 WHERE p.intent_id=?1 AND p.client_order_id=?2
                   AND e.event_type='BEGIN_POST' AND e.subject_id=?1
                   AND p.delivery_outcome='BEGIN_POST_UNRESOLVED'",
                params![id, entry.record.plan.client_order_id],
                |r| r.get(0),
            )
            .unwrap(),
        );
        assert_eq!((count, correct), (1, 1));
        drop(conn);

        entry.record.mark_unknown_submission().unwrap();
        entry.updated_at_ms = 1002;
        entries.insert(id.to_owned(), entry.clone());
        save(&db, &entries, None, None, None, &periods, &[]).unwrap();
        save(&db, &entries, None, None, None, &periods, &[]).unwrap();
        assert_eq!(load(&db, 10).unwrap().0.get(id), Some(&entry));

        let conn = Connection::open(&db).unwrap();
        let post_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM post_attempts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(post_count, 1);
        drop(conn);

        let mut forged = entry.clone();
        forged.record.state = ExecutionState::Prepared;
        forged.updated_at_ms = 1003;
        entries.insert(id.to_owned(), forged);
        assert!(save(&db, &entries, None, None, None, &periods, &[]).is_err());
        assert_eq!(load(&db, 10).unwrap().0.get(id), Some(&entry));
        // An obsolete/incomplete caller snapshot cannot remove a historic
        // intent, nor erase the outstanding UNKNOWN POST identity.
        assert!(save(&db, &BTreeMap::new(), None, None, None, &periods, &[]).is_err());
        assert_eq!(load(&db, 10).unwrap().0.get(id), Some(&entry));
        assert_eq!(
            super::ExecutionLedgerStore::sqlite_at(&db).max_records,
            usize::MAX
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn sealed_manifest_keeps_original_archive_directory() {
        let root = test_root("archive-dir");
        let active = ExecutionLedgerStore::at(root.join("execution-ledger.json"));
        let sealed = ExecutionLedgerStore::at(root.join("execution-ledger.v14.sealed"));
        let name = "archived_run_integrity_000001";
        assert_eq!(
            active
                .archive_path(ArchivedRunKind::Finalized, name)
                .unwrap(),
            sealed
                .archive_path(ArchivedRunKind::Finalized, name)
                .unwrap()
        );
    }

    #[test]
    fn native_cutover_publishes_only_after_full_v14_parity_and_is_idempotent() {
        let root = test_root("cutover-commit");
        let v14 = root.join("execution-ledger.json");
        let sqlite = root.join("execution-ledger.sqlite");
        let uid = "b".repeat(64);
        let capital = StrategyCapitalReservation {
            schema: STRATEGY_CAPITAL_RESERVATION_SCHEMA_V1.to_owned(),
            allocation_id: "native_cutover_test_000001".to_owned(),
            account_uid_fingerprint: uid.clone(),
            origin_schema: "test/v1".to_owned(),
            origin_version: "v1".to_owned(),
            mandate_version: "1".to_owned(),
            allocated_usdt: "100".to_owned(),
            opening_account_usdt_equity: "127".to_owned(),
            account_generation: "g1".to_owned(),
            operator_authority_id: format!("sha256:{}", "a".repeat(64)),
            started_at_ms: 1,
        };
        let stop = ExecutionRiskStop {
            schema: EXECUTION_RISK_STOP_SCHEMA_V1.to_owned(),
            stopped_at_ms: 100,
            reason: "test stop".to_owned(),
        };
        let source = ExecutionLedgerStore::at(&v14);
        source
            .save(
                &BTreeMap::new(),
                Some(&stop),
                Some(&capital),
                None,
                &StrategyPeriodHistory::default(),
                &[],
            )
            .unwrap();
        let original = std::fs::read(&v14).unwrap();
        let digest = checksum(&original);
        source.prepare_sqlite_staging(&sqlite).unwrap();
        assert!(ExecutionLedgerStore::select_published_owner(&root).is_err());
        assert!(
            ExecutionLedgerStore::migrate_to_sqlite_offline(&root, &"f".repeat(64), &digest)
                .is_err()
        );
        assert!(
            ExecutionLedgerStore::migrate_to_sqlite_offline(&root, &uid, &"0".repeat(64)).is_err()
        );
        assert!(!root.join("execution-ledger.owner").exists());
        let scope = SqliteAccountIdentity {
            venue: LedgerVenue::Okx,
            environment: LedgerEnvironment::Production,
            account_id: "real".to_owned(),
            uid_fingerprint: uid.clone(),
            bound_at_ms: 101,
        };
        let chosen = ExecutionLedgerStore::migrate_to_sqlite_offline_for_account(
            &root, &uid, &digest, &scope,
        )
        .unwrap();
        assert_eq!(
            chosen.bound_native_account_identity().unwrap(),
            Some(scope.clone())
        );
        assert_eq!(chosen.path(), sqlite.as_path());
        assert_eq!(
            chosen.attest_storage().unwrap().integrity,
            "VALIDATED_COLD_READ"
        );
        let duplicate = ExecutionLedgerStore::migrate_to_sqlite_offline_for_account(
            &root, &uid, &digest, &scope,
        )
        .unwrap();
        assert_eq!(duplicate.path(), sqlite.as_path());
        assert_eq!(
            duplicate.bound_native_account_identity().unwrap(),
            Some(scope)
        );
        let sealed = root.join("execution-ledger.v14.sealed");
        assert!(!v14.exists());
        assert_eq!(std::fs::read(&sealed).unwrap(), original);
        std::fs::remove_file(root.join("execution-ledger.owner")).unwrap();
        assert!(ExecutionLedgerStore::select_published_owner(&root).is_err());
        std::fs::remove_file(&sqlite).unwrap();
        assert!(ExecutionLedgerStore::select_published_owner(&root).is_err());
        // A sealed legacy source must never recreate a possibly newer
        // missing SQLite file (and thereby lose legitimate recent trades).
        assert!(ExecutionLedgerStore::migrate_to_sqlite_offline(&root, &uid, &digest).is_err());
        // Even the total loss of archived body, receipt and SQLite does
        // not turn this proven cutover account into a fresh JSON ledger.
        std::fs::remove_file(&sealed).unwrap();
        assert!(ExecutionLedgerStore::select_published_owner(&root).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn owner_receipt_requires_sealed_source_and_never_falls_back_after_failure() {
        let root = test_root("owner-proof");
        let v14 = root.join("execution-ledger.json");
        let sqlite = root.join("execution-ledger.sqlite");
        let legacy = ExecutionLedgerStore::at(&v14);
        legacy
            .save(
                &BTreeMap::new(),
                None,
                None,
                None,
                &StrategyPeriodHistory::default(),
                &[],
            )
            .unwrap();
        let witness_sha = checksum(&std::fs::read(&v14).unwrap());
        let uid = "a".repeat(64);
        assert_eq!(
            ExecutionLedgerStore::select_published_owner(&root)
                .unwrap()
                .path(),
            v14.as_path()
        );
        let capital = StrategyCapitalReservation {
            schema: STRATEGY_CAPITAL_RESERVATION_SCHEMA_V1.to_owned(),
            allocation_id: "sqlite_owner_test_000001".to_owned(),
            account_uid_fingerprint: uid.clone(),
            origin_schema: "test/v1".to_owned(),
            origin_version: "v1".to_owned(),
            mandate_version: "1".to_owned(),
            allocated_usdt: "100".to_owned(),
            opening_account_usdt_equity: "127".to_owned(),
            account_generation: "g1".to_owned(),
            operator_authority_id: format!("sha256:{}", "a".repeat(64)),
            started_at_ms: 1,
        };
        save(
            &sqlite,
            &BTreeMap::new(),
            None,
            Some(&capital),
            None,
            &StrategyPeriodHistory::default(),
            &[],
        )
        .unwrap();
        seal_staging_origin(&sqlite, &uid, &witness_sha).unwrap();
        // A candidate in the canonical location without its owner receipt is
        // ambiguous, not proof that the original v14 writer can resume.
        assert!(ExecutionLedgerStore::select_published_owner(&root).is_err());
        // The published receipt is owned by CONTROL; tests simulate it only
        // after source and target are independently materialized.
        let receipt = root.join("execution-ledger.owner");
        std::fs::write(
            &receipt,
            format!(
                "okx.ledger-owner/v1\nSQLITE\n{}\n{}\n",
                "0".repeat(64),
                witness_sha
            ),
        )
        .unwrap();
        assert!(ExecutionLedgerStore::select_published_owner(&root).is_err());
        std::fs::write(
            &receipt,
            format!("okx.ledger-owner/v1\nSQLITE\n{}\n{}\n", uid, witness_sha),
        )
        .unwrap();
        assert!(ExecutionLedgerStore::select_published_owner(&root).is_err());
        std::fs::rename(&v14, root.join("execution-ledger.v14.sealed")).unwrap();
        // A forged receipt with exact historical parity is insufficient:
        // CONTROL must have durably retired the previous writer.
        assert!(ExecutionLedgerStore::select_published_owner(&root).is_err());
        let mut writer = ExecutionLedgerStore::acquire_writer_lease(&root).unwrap();
        ExecutionLedgerStore::mark_sqlite_cutover(&mut writer).unwrap();
        drop(writer);
        assert_eq!(
            ExecutionLedgerStore::select_published_owner(&root)
                .unwrap()
                .path(),
            sqlite.as_path()
        );
        // Losing the receipt after cutover cannot silently reactivate v14.
        std::fs::remove_file(&receipt).unwrap();
        assert!(ExecutionLedgerStore::select_published_owner(&root).is_err());
        std::fs::write(
            &receipt,
            format!("okx.ledger-owner/v1\nSQLITE\n{}\n{}\n", uid, witness_sha),
        )
        .unwrap();
        assert_eq!(
            ExecutionLedgerStore::select_published_owner(&root)
                .unwrap()
                .path(),
            sqlite.as_path()
        );
        // Even if the SQLite target disappears after receipt publication,
        // boot must fail and never reopen an independent v14 writer.
        std::fs::remove_file(&sqlite).unwrap();
        assert!(ExecutionLedgerStore::select_published_owner(&root).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn interrupted_v14_retirement_resumes_without_unsealing_or_replaying() {
        let root = test_root("interrupted-retirement");
        let v14 = root.join("execution-ledger.json");
        let sqlite = root.join("execution-ledger.sqlite");
        let sealed = root.join("execution-ledger.v14.sealed");
        let uid = "c".repeat(64);
        let capital = StrategyCapitalReservation {
            schema: STRATEGY_CAPITAL_RESERVATION_SCHEMA_V1.to_owned(),
            allocation_id: "native_cutover_resume_000001".to_owned(),
            account_uid_fingerprint: uid.clone(),
            origin_schema: "test/v1".to_owned(),
            origin_version: "v1".to_owned(),
            mandate_version: "1".to_owned(),
            allocated_usdt: "100".to_owned(),
            opening_account_usdt_equity: "127".to_owned(),
            account_generation: "g1".to_owned(),
            operator_authority_id: format!("sha256:{}", "a".repeat(64)),
            started_at_ms: 1,
        };
        let legacy = ExecutionLedgerStore::at(&v14);
        legacy
            .save(
                &BTreeMap::new(),
                None,
                Some(&capital),
                None,
                &StrategyPeriodHistory::default(),
                &[],
            )
            .unwrap();
        let digest = checksum(&std::fs::read(&v14).unwrap());
        legacy.prepare_sqlite_staging(&sqlite).unwrap();
        // Crash after retiring the old writer but before receipt publication.
        std::fs::rename(&v14, &sealed).unwrap();
        assert!(ExecutionLedgerStore::select_published_owner(&root).is_err());
        let chosen = ExecutionLedgerStore::migrate_to_sqlite_offline(&root, &uid, &digest).unwrap();
        assert_eq!(chosen.path(), sqlite.as_path());
        assert!(!v14.exists());
        assert!(sealed.exists());
        assert_eq!(
            ExecutionLedgerStore::migrate_to_sqlite_offline(&root, &uid, &digest)
                .unwrap()
                .path(),
            sqlite.as_path()
        );
        // A resurrected legacy writer is never accepted.
        std::fs::copy(&sealed, &v14).unwrap();
        assert!(ExecutionLedgerStore::select_published_owner(&root).is_err());
        assert!(ExecutionLedgerStore::migrate_to_sqlite_offline(&root, &uid, &digest).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn native_cutover_accepts_historical_intents_without_active_strategy() {
        let root = test_root("intent-only-cutover");
        let legacy = ExecutionLedgerStore::at(root.join("execution-ledger.json"));
        let uid = "d".repeat(64);
        let id = "historical_only_intent_000001";
        let plan = ExecutionPlan {
            schema: EXECUTION_PLAN_SCHEMA_V1.to_owned(),
            intent_id: id.to_owned(),
            client_order_id: derive_client_order_id(id),
            reference_generation: "sha256:reference".to_owned(),
            account_generation: "sha256:account".to_owned(),
            account_uid_fingerprint: uid.clone(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            trade_mode: crate::TradeMode::Cross,
            side: crate::OrderSide::Buy,
            position_side: PositionSide::Long,
            action: ExecutionAction::Open,
            order_type: crate::OrderType::Limit,
            size: "1".to_owned(),
            price: "0.1".to_owned(),
            open_risk: None,
            risk_binding: None,
        };
        let entries = BTreeMap::from([(
            id.to_owned(),
            ExecutionLedgerEntry {
                record: ExecutionRecord::new(plan),
                created_at_ms: 1000,
                updated_at_ms: 1000,
            },
        )]);
        legacy
            .save(
                &entries,
                None,
                None,
                None,
                &StrategyPeriodHistory::default(),
                &[],
            )
            .unwrap();
        let hash = checksum(&std::fs::read(legacy.path()).unwrap());
        let sqlite = ExecutionLedgerStore::migrate_to_sqlite_offline(&root, &uid, &hash).unwrap();
        assert_eq!(sqlite.load().unwrap().0, entries);
        assert_eq!(
            ExecutionLedgerStore::select_published_owner(&root)
                .unwrap()
                .path(),
            sqlite.path()
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn invalid_cutover_preflight_never_stages_or_disables_v14() {
        let root = test_root("source-preflight");
        let legacy = ExecutionLedgerStore::at(root.join("execution-ledger.json"));
        let uid = "e".repeat(64);
        let capital = StrategyCapitalReservation {
            schema: STRATEGY_CAPITAL_RESERVATION_SCHEMA_V1.to_owned(),
            allocation_id: "preflight_strategy_run_000001".to_owned(),
            account_uid_fingerprint: uid.clone(),
            origin_schema: "test/v1".to_owned(),
            origin_version: "v1".to_owned(),
            mandate_version: "1".to_owned(),
            allocated_usdt: "100".to_owned(),
            opening_account_usdt_equity: "127".to_owned(),
            account_generation: "g1".to_owned(),
            operator_authority_id: format!("sha256:{}", "a".repeat(64)),
            started_at_ms: 1,
        };
        legacy
            .save(
                &BTreeMap::new(),
                None,
                Some(&capital),
                None,
                &StrategyPeriodHistory::default(),
                &[],
            )
            .unwrap();
        let digest = checksum(&std::fs::read(legacy.path()).unwrap());
        assert!(
            ExecutionLedgerStore::migrate_to_sqlite_offline(&root, &"f".repeat(64), &digest)
                .is_err()
        );
        assert!(
            ExecutionLedgerStore::migrate_to_sqlite_offline(&root, &uid, &"0".repeat(64)).is_err()
        );
        assert!(!root.join("execution-ledger.sqlite").exists());
        assert!(!root.join("execution-ledger.owner").exists());
        assert!(!root.join("execution-ledger.v14.sealed").exists());
        assert_eq!(
            ExecutionLedgerStore::select_published_owner(&root)
                .unwrap()
                .path(),
            legacy.path()
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn altered_cutover_watermark_never_authorizes_sqlite_owner() {
        let root = test_root("cutover-marker-corrupt");
        std::fs::create_dir_all(&root).unwrap();
        let lock = root.join("execution-ledger.writer-lock");
        let valid = b"okx.sqlite-cutover/v1\n";
        // Same length as a legitimate watermark, but different content.
        std::fs::write(&lock, b"okx.sqlite-cutover/v2\n").unwrap();
        assert_eq!(std::fs::metadata(&lock).unwrap().len(), valid.len() as u64);
        assert!(ExecutionLedgerStore::select_published_owner(&root).is_err());
        assert!(
            ExecutionLedgerStore::migrate_to_sqlite_offline(
                &root,
                &"a".repeat(64),
                &"b".repeat(64)
            )
            .is_err()
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn recovery_updates_only_changed_intents_without_rewriting_history() {
        let root = test_root("recovery-delta");
        let path = root.join("execution-ledger.sqlite");
        let make = |id: &str| {
            let plan = ExecutionPlan {
                schema: EXECUTION_PLAN_SCHEMA_V1.to_owned(),
                intent_id: id.to_owned(),
                client_order_id: derive_client_order_id(id),
                reference_generation: "sha256:reference".to_owned(),
                account_generation: "sha256:account".to_owned(),
                account_uid_fingerprint: "account-uid-test".to_owned(),
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                trade_mode: crate::TradeMode::Cross,
                side: crate::OrderSide::Buy,
                position_side: PositionSide::Long,
                action: ExecutionAction::Open,
                order_type: crate::OrderType::Limit,
                size: "1".to_owned(),
                price: "0.1".to_owned(),
                open_risk: None,
                risk_binding: None,
            };
            ExecutionLedgerEntry {
                record: ExecutionRecord::new(plan),
                created_at_ms: 1000,
                updated_at_ms: 1000,
            }
        };
        let historic_id = "old_historical_intent_00001";
        let inflight_id = "inflight_post_intent_00001";
        let mut historic = make(historic_id);
        historic.record.abandon_prepared().unwrap();
        let mut inflight = make(inflight_id);
        let periods = StrategyPeriodHistory::default();
        let entries = BTreeMap::from([
            (historic_id.to_owned(), historic.clone()),
            (inflight_id.to_owned(), inflight.clone()),
        ]);
        save(&path, &entries, None, None, None, &periods, &[]).unwrap();
        inflight.record.begin_submission_with_timing(None).unwrap();
        inflight.updated_at_ms = 1001;
        save_one(&path, &inflight, None, None, None, &periods, &[]).unwrap();
        let prior = Connection::open(&path).unwrap();
        let before: (String, String) = prior
            .query_row(
                "SELECT digest,hex(bytes) FROM ledger_entries WHERE intent_id=?1",
                params![historic_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        let before_events: i64 = prior
            .query_row("SELECT COUNT(*) FROM domain_events", [], |r| r.get(0))
            .unwrap();
        drop(prior);
        let candidates = indexed_recovery_candidates(&path, "account-uid-test").unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0], inflight);
        let opened =
            DurableExecutionLedger::open(ExecutionLedgerStore::sqlite_at(&path), 1002).unwrap();
        assert!(
            indexed_recovery_candidates(&path, "account-uid-test")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            opened.get(inflight_id).unwrap().record.state,
            ExecutionState::UnknownSubmission
        );
        assert!(opened.get(historic_id).is_none());
        assert_eq!(opened.read_intent(historic_id).unwrap(), Some(historic));
        let after = Connection::open(&path).unwrap();
        let same: (String, String) = after
            .query_row(
                "SELECT digest,hex(bytes) FROM ledger_entries WHERE intent_id=?1",
                params![historic_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(before, same);
        let after_events: i64 = after
            .query_row("SELECT COUNT(*) FROM domain_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(after_events, before_events + 1);
        let post_count: i64 = after
            .query_row("SELECT COUNT(*) FROM post_attempts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(post_count, 1);
        drop(after);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn native_writer_lease_excludes_concurrent_process_owners() {
        let root = test_root("writer-lease");
        let mut first = ExecutionLedgerStore::acquire_writer_lease(&root).unwrap();
        // The existing owner must read the marker from its locked handle.
        // This is especially important on Windows, where a second open of
        // the same locked file fails even from the same process.
        assert_eq!(
            ExecutionLedgerStore::select_published_owner_under_writer_lease(&root, &mut first)
                .unwrap()
                .path(),
            root.join("execution-ledger.json").as_path()
        );
        assert!(ExecutionLedgerStore::acquire_writer_lease(&root).is_err());
        drop(first);
        let second = ExecutionLedgerStore::acquire_writer_lease(&root).unwrap();
        drop(second);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn native_single_intent_delta_preserves_unrelated_history_and_post_fence() {
        let root = test_root("single-intent-delta");
        let db = root.join("account.sqlite");
        let build = |id: &str| {
            let plan = ExecutionPlan {
                schema: EXECUTION_PLAN_SCHEMA_V1.to_owned(),
                intent_id: id.to_owned(),
                client_order_id: derive_client_order_id(id),
                reference_generation: "sha256:reference".to_owned(),
                account_generation: "sha256:account".to_owned(),
                account_uid_fingerprint: "uid-fingerprint".to_owned(),
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                trade_mode: crate::TradeMode::Cross,
                side: crate::OrderSide::Buy,
                position_side: PositionSide::Long,
                action: ExecutionAction::Open,
                order_type: crate::OrderType::Limit,
                size: "1".to_owned(),
                price: "0.1".to_owned(),
                open_risk: None,
                risk_binding: None,
            };
            ExecutionLedgerEntry {
                record: ExecutionRecord::new(plan),
                created_at_ms: 1000,
                updated_at_ms: 1000,
            }
        };
        let changed_id = "delta_trade_intent_00001";
        let historic_id = "historic_trade_intent_00002";
        let mut changed = build(changed_id);
        let historic = build(historic_id);
        let original = BTreeMap::from([
            (changed_id.to_owned(), changed.clone()),
            (historic_id.to_owned(), historic.clone()),
        ]);
        let periods = StrategyPeriodHistory::default();
        let missing = root.join("missing.sqlite");
        assert!(save_one(&missing, &changed, None, None, None, &periods, &[]).is_err());
        assert!(!missing.exists());
        save(&db, &original, None, None, None, &periods, &[]).unwrap();
        changed.record.begin_submission_with_timing(None).unwrap();
        changed.updated_at_ms = 1001;
        // Delta write includes ONE entry. Previous source, other intent and
        // original writer history MUST remain unchanged, not deleted.
        save_one(&db, &changed, None, None, None, &periods, &[]).unwrap();
        let recovered = load(&db, usize::MAX).unwrap().0;
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered.get(changed_id), Some(&changed));
        assert_eq!(recovered.get(historic_id), Some(&historic));
        let dbconn = Connection::open(&db).unwrap();
        let n: i64 = dbconn
            .query_row("SELECT COUNT(*) FROM post_attempts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1);
        drop(dbconn);
        changed.record.mark_unknown_submission().unwrap();
        changed.updated_at_ms = 1002;
        save_one(&db, &changed, None, None, None, &periods, &[]).unwrap();
        save_one(&db, &changed, None, None, None, &periods, &[]).unwrap();
        let mut forged = changed.clone();
        forged.record.state = ExecutionState::Prepared;
        forged.updated_at_ms = 1003;
        assert!(save_one(&db, &forged, None, None, None, &periods, &[]).is_err());
        let recovered = load(&db, usize::MAX).unwrap().0;
        assert_eq!(recovered.get(changed_id), Some(&changed));
        assert_eq!(recovered.get(historic_id), Some(&historic));
        std::fs::remove_dir_all(root).unwrap();
    }
}

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
                ON ledger_entries(uid, updated_at_ms);",
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
    } else {
        // Cold read only, not the hot mutation path.
        let ok: String = connection
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .map_err(io)?;
        if ok != "ok" {
            return Err(ExecutionLedgerError::Corrupt("SQLite integrity failed"));
        }
    }
    Ok(connection)
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
    let mut statement=c.prepare(
        "SELECT intent_id,client_order_id,uid,digest,bytes FROM ledger_entries ORDER BY intent_id"
    ).map_err(io)?;
    let mut rows = statement.query([]).map_err(io)?;
    let mut entries = BTreeMap::new();
    let mut known_uid: Option<String> = None;
    while let Some(row) = rows.next().map_err(io)? {
        let id: String = row.get(0).map_err(io)?;
        let client: String = row.get(1).map_err(io)?;
        let uid: String = row.get(2).map_err(io)?;
        let sha: String = row.get(3).map_err(io)?;
        let bytes: Vec<u8> = row.get(4).map_err(io)?;
        if entries.len() >= max
            || checksum(&bytes) != sha
            || known_uid.as_ref().is_some_and(|v| v != &uid)
        {
            return Err(ExecutionLedgerError::Corrupt(
                "SQLite entry integrity/scope",
            ));
        }
        let entry: ExecutionLedgerEntry = decode(&bytes)?;
        if entry.record.plan.intent_id != id
            || entry.record.plan.client_order_id != client
            || entry.record.plan.account_uid_fingerprint != uid
        {
            return Err(ExecutionLedgerError::Corrupt("SQLite entry identity"));
        }
        validate_entry(&entry)?;
        known_uid = Some(uid);
        entries.insert(id, entry);
    }

    // A cold read proves that the indexed SQL projection is not stale or
    // silently incomplete relative to the authoritatively stored intents.
    let count: i64 = c
        .query_row("SELECT COUNT(*) FROM intents", [], |row| row.get(0))
        .map_err(io)?;
    if count != entries.len() as i64 {
        return Err(ExecutionLedgerError::Corrupt(
            "SQLite intent projection count mismatch",
        ));
    }
    for (intent_id, entry) in &entries {
        let projected: (String, String, String, String, i64) = c
            .query_row(
                "SELECT instrument_id,requested_action,plan_sha256,current_state,created_at_ms
             FROM intents WHERE intent_id=?1",
                params![intent_id],
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
            .map_err(io)?;
        let expected_plan_hash = checksum(&encode(&entry.record.plan)?);
        if projected.0 != entry.record.plan.instrument_id
            || projected.1 != format!("{:?}", entry.record.plan.action)
            || projected.2 != expected_plan_hash
            || projected.3 != format!("{:?}", entry.record.state)
            || i64::try_from(entry.created_at_ms).ok() != Some(projected.4)
        {
            return Err(ExecutionLedgerError::Corrupt(
                "SQLite intent projection mismatch",
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
                 ON CONFLICT(intent_id) DO UPDATE SET current_state=excluded.current_state",
                params![id, &e.record.plan.instrument_id, action, plan_hash, stage, e.created_at_ms],
            ).map_err(io)?;
            if projection != 1 {
                return Err(ExecutionLedgerError::Corrupt(
                    "SQLite intent projection lost",
                ));
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
    // An old intent may not disappear: a count alone is sufficient here
    // because the upsert loop proved every input intent has one SQL row.
    // Unexpected historical rows increase the count and roll back all writes.
    let persisted: i64 = tx
        .query_row("SELECT COUNT(*) FROM ledger_entries", [], |row| row.get(0))
        .map_err(io)?;
    if complete_snapshot && usize::try_from(persisted).ok() != Some(entries.len()) {
        return Err(ExecutionLedgerError::Corrupt(
            "SQLite historical intent deletion",
        ));
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

pub(super) fn attest(path: &Path) -> Result<LedgerStorageAttestation, ExecutionLedgerError> {
    let c = connect(path, false)?;
    let digest: String = c
        .query_row("SELECT digest FROM ledger_state WHERE key=1", [], |r| {
            r.get(0)
        })
        .map_err(io)?;
    let (records, stop, _capital, source, periods, _leverage) = load(path, usize::MAX)?;
    let n: i64 = c
        .query_row("SELECT COUNT(*) FROM ledger_entries", [], |r| r.get(0))
        .map_err(io)?;
    if n != records.len() as i64 {
        return Err(ExecutionLedgerError::Corrupt("SQLite inventory"));
    }
    // The attestation hashes all ledger records in a stable key order and the
    // state manifest. Hashing the state row alone would miss amended intents.
    let mut owner_digest = Sha256::new();
    owner_digest.update(b"okx.sqlite.ledger/v1");
    owner_digest.update(digest.as_bytes());
    if let Some(source) = &source {
        owner_digest.update(source.checksum_sha256.as_bytes());
    }
    for (id, entry) in &records {
        let payload = encode(entry)?;
        owner_digest.update((id.len() as u64).to_le_bytes());
        owner_digest.update(id.as_bytes());
        owner_digest.update((payload.len() as u64).to_le_bytes());
        owner_digest.update(&payload);
    }
    let full_manifest = format!("{:x}", owner_digest.finalize());
    Ok(LedgerStorageAttestation {
        schema: LEDGER_STORAGE_ATTESTATION_SCHEMA_V1,
        on_disk_schema: SCHEMA.to_owned(),
        integrity: "VALIDATED_COLD_READ",
        migration: "SQLITE_NATIVE_OWNER",
        manifest_sha256: full_manifest,
        manifest_bytes: fs::metadata(path)?.len(),
        sealed_finalized_runs: periods.archived_runs.len(),
        sealed_unverified_runs: periods.unverified_runs.len(),
        verified_archive_bodies: periods.archived_runs.len() + periods.unverified_runs.len(),
        original_snapshot: "SQLITE_NO_LEGACY_FALLBACK",
        original_snapshot_sha256: None,
        active_risk_stop_reason: stop.as_ref().map(|v| v.reason.clone()),
        finalized_run_checksum_sha256: periods
            .finalized_run
            .as_ref()
            .map(|f| f.checksum_sha256.clone()),
        backup_scope: "SQLITE_WAL_AND_IMMUTABLE_SOURCE",
        trading_admission: "READ_ONLY_NOT_TRADING_AUTHORITY",
    })
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
        assert_eq!(load(&p, 10).unwrap().3, Some(original));
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
        assert_eq!(
            ExecutionLedgerStore::select_published_owner(&root)
                .unwrap()
                .path(),
            v14.as_path()
        );
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

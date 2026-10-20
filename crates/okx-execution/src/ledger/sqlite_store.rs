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
    source: Option<DurableExchangeEvidence>,
    periods: StrategyPeriodHistory,
    leverage: Vec<LeverageChangeAttempt>,
}

fn io(e: impl std::error::Error + Send + Sync + 'static) -> ExecutionLedgerError {
    ExecutionLedgerError::Io(std::io::Error::other(e))
}
fn encode<T: Serialize>(x: &T) -> Result<Vec<u8>, ExecutionLedgerError> {
    rmp_serde::to_vec_named(x).map_err(io)
}
fn decode<T: DeserializeOwned>(b: &[u8]) -> Result<T, ExecutionLedgerError> {
    rmp_serde::from_slice(b).map_err(io)
}
fn checksum(b: &[u8]) -> String {
    format!("{:x}", Sha256::digest(b))
}

fn connect(path: &Path, create: bool) -> Result<Connection, ExecutionLedgerError> {
    let connection = if create {
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
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(io)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(io)?;
        let mode: String = connection
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .map_err(io)?;
        let sync: i64 = connection
            .query_row("PRAGMA synchronous", [], |r| r.get(0))
            .map_err(io)?;
        if mode != "wal" || sync != 2 {
            return Err(ExecutionLedgerError::Corrupt("SQLite WAL/FULL unavailable"));
        }
        let current_version: i64 = connection
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .map_err(io)?;
        if current_version != 0 && current_version != 1 {
            return Err(ExecutionLedgerError::Corrupt("unsupported future SQLite schema"));
        }
        if current_version == 0 {
            connection
                .execute_batch(include_str!("../../schema/account-ledger-v1.sql"))
                .map_err(io)?;
        }
        connection
            .execute_batch(
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
    }
    let ver: i64 = connection
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(io)?;
    if ver != 1 {
        return Err(ExecutionLedgerError::Corrupt(
            "SQLite schema version mismatch",
        ));
    }
    if !create {
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
        return Err(ExecutionLedgerError::Corrupt("SQLite account ledger missing"));
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
    let count: i64 = c.query_row("SELECT COUNT(*) FROM intents",[],|row|row.get(0)).map_err(io)?;
    if count != entries.len() as i64 {
        return Err(ExecutionLedgerError::Corrupt("SQLite intent projection count mismatch"));
    }
    for (intent_id, entry) in &entries {
        let projected: (String,String,String,String,i64) = c.query_row(
            "SELECT instrument_id,requested_action,plan_sha256,current_state,created_at_ms
             FROM intents WHERE intent_id=?1", params![intent_id],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?))
        ).map_err(io)?;
        let expected_plan_hash = checksum(&encode(&entry.record.plan)?);
        if projected.0 != entry.record.plan.instrument_id
            || projected.1 != format!("{:?}",entry.record.plan.action)
            || projected.2 != expected_plan_hash
            || projected.3 != format!("{:?}",entry.record.state)
            || i64::try_from(entry.created_at_ms).ok() != Some(projected.4) {
            return Err(ExecutionLedgerError::Corrupt("SQLite intent projection mismatch"));
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
            .or_else(|| s.source.as_ref().map(|e| e.account_uid.as_str()))
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
        return Err(ExecutionLedgerError::Corrupt("SQLite account state missing"));
    };
    if let Some(s) = &state.stop {
        validate_risk_stop(s)?;
    }
    if let Some(r) = &state.reservation {
        validate_capital_reservation(r)?;
    }
    if let Some(e) = &state.source {
        e.validate()?;
    }
    state
        .periods
        .validate(state.reservation.as_ref(), state.source.as_ref())?;
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
        state.source,
        state.periods,
        state.leverage,
    ))
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
    if entries.values().any(|e|e.record.plan.account_uid_fingerprint != uid)
        || reservation.is_some_and(|r|r.account_uid_fingerprint != uid)
        || source.is_some_and(|e|e.account_uid != uid) {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    if old_uid
        .as_deref()
        .is_some_and(|u| !u.is_empty() && u != uid)
    {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    let payload = encode(&StoredState {
        version: 1,
        stop: stop.cloned(),
        reservation: reservation.cloned(),
        source: source.cloned(),
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
    let mut prior = Vec::new();
    {
        let mut statement = tx
            .prepare("SELECT intent_id FROM ledger_entries")
            .map_err(io)?;
        for item in statement
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(io)?
        {
            prior.push(item.map_err(io)?);
        }
    }
    if prior.iter().any(|id| !entries.contains_key(id)) {
        return Err(ExecutionLedgerError::Corrupt(
            "SQLite intent deletion forbidden",
        ));
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
            let existing: Option<(String, String, String, i64)> = tx
                .query_row(
                    "SELECT client_order_id,uid,digest,updated_at_ms FROM ledger_entries WHERE intent_id=?1",
                    params![id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()
                .map_err(io)?;
            if existing.as_ref().is_some_and(|(client, existing_uid, _, _)| {
                client != &e.record.plan.client_order_id
                    || existing_uid != &e.record.plan.account_uid_fingerprint
            }) {
                return Err(ExecutionLedgerError::IntentConflict);
            }
            let bytes = encode(e)?;
            let entry_digest = checksum(&bytes);
            if existing.as_ref().is_some_and(|(_, _, prior_digest, _)| prior_digest == &entry_digest) {
                continue;
            }
            if existing.as_ref().is_some_and(|(_, _, _, prior_time)| {
                i64::try_from(e.updated_at_ms).ok().is_none_or(|t| t <= *prior_time)
            }) {
                return Err(ExecutionLedgerError::InvalidTimestamp);
            }
            let written = q.execute(params![
                id,
                &e.record.plan.client_order_id,
                &e.record.plan.account_uid_fingerprint,
                e.updated_at_ms,
                &entry_digest,
                bytes
            ]).map_err(io)?;
            if written != 1 {
                return Err(ExecutionLedgerError::Corrupt("SQLite intent transition lost"));
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
                return Err(ExecutionLedgerError::Corrupt("SQLite intent projection lost"));
            }
            let last: Option<(i64,String)> = tx.query_row(
                "SELECT seq,event_sha256 FROM domain_events ORDER BY seq DESC LIMIT 1",
                [], |row| Ok((row.get(0)?, row.get(1)?))
            ).optional().map_err(io)?;
            let event_sequence = last.as_ref().map_or(1, |(seq, _)| seq + 1);
            let event_id = format!("sqlite-intent-{event_sequence}");
            let prev_hash = last.as_ref().map(|(_,hash)| hash.as_str());
            let event_hash = checksum(&encode(&(
                "okx.sqlite.intent-transition/v1",prev_hash.as_ref(),&event_id,id,
                &entry_digest,e.updated_at_ms
            ))?);
            tx.execute(
                "INSERT INTO domain_events
                 (event_id,event_type,event_version,subject_kind,subject_id,
                  effective_at_ms,recorded_at_ms,previous_event_sha256,event_sha256)
                 VALUES (?1,'INTENT_TRANSITION',1,'INTENT',?2,?3,?3,?4,?5)",
                params![event_id,id,e.updated_at_ms,prev_hash,event_hash]
            ).map_err(io)?;
        }
    }
    tx.commit().map_err(io)?;
    Ok(())
}

pub(super) fn attest(path: &Path) -> Result<LedgerStorageAttestation, ExecutionLedgerError> {
    let c = connect(path, false)?;
    let digest: String = c
        .query_row("SELECT digest FROM ledger_state WHERE key=1", [], |r| {
            r.get(0)
        })
        .map_err(io)?;
    let (records, stop, _capital, _source, periods, _leverage) =
        load(path, MAX_EXECUTION_LEDGER_RECORDS)?;
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
}

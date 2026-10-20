//! In-process SQLite WAL persistence for the existing execution ledger.
//! Explicitly selected; Production v14 stays authoritative until a cutover.
use super::*;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use serde::{de::DeserializeOwned, Serialize};

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
fn checksum(b: &[u8]) -> String { format!("{:x}", Sha256::digest(b)) }

fn connect(path: &Path, create: bool) -> Result<Connection, ExecutionLedgerError> {
    let connection = if create {
        fs::create_dir_all(path.parent().ok_or(ExecutionLedgerError::Corrupt("SQLite parent missing"))?)?;
        Connection::open(path).map_err(io)?
    } else {
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(io)?
    };
    connection.busy_timeout(std::time::Duration::from_secs(5)).map_err(io)?;
    connection.pragma_update(None, "foreign_keys", "ON").map_err(io)?;
    if create {
        connection.pragma_update(None, "journal_mode", "WAL").map_err(io)?;
        connection.pragma_update(None, "synchronous", "FULL").map_err(io)?;
        let mode: String=connection.query_row("PRAGMA journal_mode",[],|r|r.get(0)).map_err(io)?;
        let sync: i64=connection.query_row("PRAGMA synchronous",[],|r|r.get(0)).map_err(io)?;
        if mode!="wal" || sync!=2 {
            return Err(ExecutionLedgerError::Corrupt("SQLite WAL/FULL unavailable"));
        }
        connection.execute_batch(
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
             PRAGMA user_version=1;"
        ).map_err(io)?;
    }
    let ver: i64=connection.query_row("PRAGMA user_version",[],|r|r.get(0)).map_err(io)?;
    if ver!=1 { return Err(ExecutionLedgerError::Corrupt("SQLite schema version mismatch")); }
    if !create {
        let ok: String=connection.query_row("PRAGMA integrity_check",[],|r|r.get(0)).map_err(io)?;
        if ok!="ok" { return Err(ExecutionLedgerError::Corrupt("SQLite integrity failed")); }
    }
    Ok(connection)
}

pub(super) fn load(path:&Path, max:usize) -> Result<LoadedExecutionLedger, ExecutionLedgerError> {
    if !path.exists() {
        return Ok((BTreeMap::new(),None,None,None,StrategyPeriodHistory::default(),Vec::new()));
    }
    let c=connect(path,false)?;
    let meta: Option<(String,String,Vec<u8>)>=c.query_row(
        "SELECT uid,digest,bytes FROM ledger_state WHERE key=1",
        [], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))
    ).optional().map_err(io)?;
    let mut statement=c.prepare(
        "SELECT intent_id,client_order_id,uid,digest,bytes FROM ledger_entries ORDER BY intent_id"
    ).map_err(io)?;
    let mut rows=statement.query([]).map_err(io)?;
    let mut entries=BTreeMap::new();
    let mut known_uid:Option<String>=None;
    while let Some(row)=rows.next().map_err(io)? {
        let id:String=row.get(0).map_err(io)?;
        let client:String=row.get(1).map_err(io)?;
        let uid:String=row.get(2).map_err(io)?;
        let sha:String=row.get(3).map_err(io)?;
        let bytes:Vec<u8>=row.get(4).map_err(io)?;
        if entries.len()>=max || checksum(&bytes)!=sha
           || known_uid.as_ref().is_some_and(|v|v!=&uid) {
            return Err(ExecutionLedgerError::Corrupt("SQLite entry integrity/scope"));
        }
        let entry:ExecutionLedgerEntry=decode(&bytes)?;
        if entry.record.plan.intent_id!=id || entry.record.plan.client_order_id!=client
           || entry.record.plan.account_uid_fingerprint!=uid {
            return Err(ExecutionLedgerError::Corrupt("SQLite entry identity"));
        }
        validate_entry(&entry)?;
        known_uid=Some(uid);
        entries.insert(id,entry);
    }
    let state=if let Some((uid,sha,bytes))=meta {
        if checksum(&bytes)!=sha || known_uid.as_deref().is_some_and(|v|v!=uid) {
            return Err(ExecutionLedgerError::Corrupt("SQLite manifest integrity/UID"));
        }
        let s:StoredState=decode(&bytes)?;
        if s.version!=1{return Err(ExecutionLedgerError::Corrupt("SQLite data version"));}
        s
    } else if !entries.is_empty() {
        return Err(ExecutionLedgerError::Corrupt("SQLite orphaned entries"));
    } else {
        StoredState {version:1,stop:None,reservation:None,source:None,
          periods:StrategyPeriodHistory::default(),leverage:Vec::new()}
    };
    if let Some(s)=&state.stop { validate_risk_stop(s)?; }
    if let Some(r)=&state.reservation { validate_capital_reservation(r)?; }
    if let Some(e)=&state.source { e.validate()?; }
    state.periods.validate(state.reservation.as_ref(),state.source.as_ref())?;
    validate_leverage_attempts(&state.leverage,state.reservation.as_ref())?;
    for e in entries.values() {
        validate_strategy_run_binding(e,state.reservation.as_ref(),
            &state.periods.archived_runs,&state.periods.unverified_runs)?;
    }
    Ok((entries,state.stop,state.reservation,state.source,state.periods,state.leverage))
}

pub(super) fn save(path:&Path, entries:&BTreeMap<String,ExecutionLedgerEntry>,
    stop:Option<&ExecutionRiskStop>, reservation:Option<&StrategyCapitalReservation>,
    source:Option<&DurableExchangeEvidence>, periods:&StrategyPeriodHistory,
    leverage:&[LeverageChangeAttempt]) -> Result<(),ExecutionLedgerError>
{
    let mut c=connect(path,true)?;
    let tx=c.transaction_with_behavior(TransactionBehavior::Immediate).map_err(io)?;
    let uid=entries.values().next().map(|e|e.record.plan.account_uid_fingerprint.as_str())
        .or_else(||reservation.map(|r|r.account_uid_fingerprint.as_str()))
        .or_else(||source.map(|e|e.account_uid.as_str())).unwrap_or("");
    let old_uid:Option<String>=tx.query_row(
        "SELECT uid FROM ledger_state WHERE key=1",[],|r|r.get(0)
    ).optional().map_err(io)?;
    if old_uid.as_deref().is_some_and(|u|!u.is_empty() && u!=uid) {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    let payload=encode(&StoredState {version:1,stop:stop.cloned(),
        reservation:reservation.cloned(),source:source.cloned(),
        periods:periods.clone(),leverage:leverage.to_vec()})?;
    tx.execute(
        "INSERT INTO ledger_state VALUES(1,1,?1,?2,?3)
         ON CONFLICT(key) DO UPDATE SET uid=excluded.uid,
           digest=excluded.digest,bytes=excluded.bytes
         WHERE ledger_state.digest<>excluded.digest",
        params![uid,checksum(&payload),payload]
    ).map_err(io)?;
    let prior:Vec<String>={
        let mut statement=tx.prepare("SELECT intent_id FROM ledger_entries").map_err(io)?;
        statement.query_map([],|r|r.get(0)).map_err(io)?
          .collect::<Result<Vec<String>,_>>().map_err(io)?
    };
    if prior.iter().any(|id|!entries.contains_key(id)) {
        return Err(ExecutionLedgerError::Corrupt("SQLite intent deletion forbidden"));
    }
    {
        let mut q=tx.prepare(
            "INSERT INTO ledger_entries VALUES(?1,?2,?3,?4,?5,?6)
             ON CONFLICT(intent_id) DO UPDATE SET
               updated_at_ms=excluded.updated_at_ms,
               digest=excluded.digest,bytes=excluded.bytes
             WHERE ledger_entries.digest<>excluded.digest
               AND ledger_entries.client_order_id=excluded.client_order_id
               AND ledger_entries.uid=excluded.uid"
        ).map_err(io)?;
        for (id,e) in entries {
            let bytes=encode(e)?;
            q.execute(params![id,&e.record.plan.client_order_id,
               &e.record.plan.account_uid_fingerprint,e.updated_at_ms,
               checksum(&bytes),bytes]).map_err(io)?;
        }
    }
    tx.commit().map_err(io)?;
    Ok(())
}

pub(super) fn attest(path:&Path) -> Result<LedgerStorageAttestation,ExecutionLedgerError> {
    let c=connect(path,false)?;
    let digest:String=c.query_row(
        "SELECT digest FROM ledger_state WHERE key=1",[],|r|r.get(0)
    ).map_err(io)?;
    let (records,stop,_capital,_source,periods,_leverage)=load(path,MAX_EXECUTION_LEDGER_RECORDS)?;
    let n:i64=c.query_row("SELECT COUNT(*) FROM ledger_entries",[],|r|r.get(0)).map_err(io)?;
    if n!=records.len() as i64 {return Err(ExecutionLedgerError::Corrupt("SQLite inventory"));}
    Ok(LedgerStorageAttestation {
        schema:LEDGER_STORAGE_ATTESTATION_SCHEMA_V1,
        on_disk_schema:SCHEMA.to_owned(),integrity:"VALIDATED_COLD_READ",
        migration:"SQLITE_NATIVE_OWNER",manifest_sha256:digest,
        manifest_bytes:fs::metadata(path)?.len(),
        sealed_finalized_runs:periods.archived_runs.len(),
        sealed_unverified_runs:periods.unverified_runs.len(),
        verified_archive_bodies:periods.archived_runs.len()+periods.unverified_runs.len(),
        original_snapshot:"SQLITE_NO_LEGACY_FALLBACK",original_snapshot_sha256:None,
        active_risk_stop_reason:stop.as_ref().map(|v|v.reason.clone()),
        finalized_run_checksum_sha256:periods.finalized_run.as_ref().map(|f|f.checksum_sha256.clone()),
        backup_scope:"SQLITE_WAL_AND_IMMUTABLE_SOURCE",
        trading_admission:"READ_ONLY_NOT_TRADING_AUTHORITY"
    })
}

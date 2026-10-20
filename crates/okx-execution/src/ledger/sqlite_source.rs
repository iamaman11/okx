//! Indexed and immutable exchange source evidence in the SAME account SQLite transaction.
//! Exchange payload bytes are encoded as MessagePack; original legacy JSON evidence
//! stays inside the separately sealed and unmodified pre-cutover v14 archive.
use super::sqlite_store::{checksum, decode, encode, io};
use super::*;
use crate::exchange_evidence::{ExchangeCoverageCursor, StoredExchangeEvent};
use rusqlite::{Connection, OptionalExtension, Transaction, params};

pub(super) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS native_source_runs (
    run_id TEXT PRIMARY KEY,
    uid TEXT NOT NULL,
    source_schema TEXT NOT NULL,
    proof_digest TEXT NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS native_source_events (
    run_id TEXT NOT NULL REFERENCES native_source_runs(run_id),
    event_key TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    payload BLOB NOT NULL,
    PRIMARY KEY(run_id,event_key)
) STRICT;
CREATE TABLE IF NOT EXISTS native_source_cursors (
    run_id TEXT NOT NULL REFERENCES native_source_runs(run_id),
    cursor_key TEXT NOT NULL,
    receipt_ms INTEGER NOT NULL,
    payload_digest TEXT NOT NULL,
    payload BLOB NOT NULL,
    PRIMARY KEY(run_id,cursor_key)
) STRICT;
CREATE INDEX IF NOT EXISTS idx_source_receipt ON
 native_source_cursors(run_id,receipt_ms);
CREATE TRIGGER IF NOT EXISTS immutable_native_event_update
BEFORE UPDATE ON native_source_events BEGIN SELECT RAISE(ABORT,'IMMUTABLE_SOURCE'); END;
CREATE TRIGGER IF NOT EXISTS immutable_native_event_delete
BEFORE DELETE ON native_source_events BEGIN SELECT RAISE(ABORT,'IMMUTABLE_SOURCE'); END;
CREATE TRIGGER IF NOT EXISTS immutable_native_cursor_update
BEFORE UPDATE ON native_source_cursors BEGIN SELECT RAISE(ABORT,'IMMUTABLE_SOURCE'); END;
CREATE TRIGGER IF NOT EXISTS immutable_native_cursor_delete
BEFORE DELETE ON native_source_cursors BEGIN SELECT RAISE(ABORT,'IMMUTABLE_SOURCE'); END;
";

pub(super) fn persist(
    tx: &Transaction<'_>,
    source: &DurableExchangeEvidence,
) -> Result<(), ExecutionLedgerError> {
    source.validate()?;
    let previous: Option<(String, String)> = tx
        .query_row(
            "SELECT uid,source_schema FROM native_source_runs WHERE run_id=?1",
            params![source.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(io)?;
    if previous.is_some_and(|(uid, schema)| uid != source.account_uid || schema != source.schema) {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    tx.execute(
        "INSERT INTO native_source_runs VALUES(?1,?2,?3,?4)
         ON CONFLICT(run_id) DO UPDATE SET proof_digest=excluded.proof_digest",
        params![
            source.run_id,
            source.account_uid,
            source.schema,
            source.checksum_sha256
        ],
    )
    .map_err(io)?;

    let mut old_events = BTreeMap::<String, String>::new();
    {
        let mut q = tx
            .prepare("SELECT event_key,payload_digest FROM native_source_events WHERE run_id=?1")
            .map_err(io)?;
        let rows = q
            .query_map(params![source.run_id], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(io)?;
        for row in rows {
            let (key, hash) = row.map_err(io)?;
            old_events.insert(key, hash);
        }
    }
    if old_events
        .keys()
        .any(|key| !source.events.contains_key(key))
    {
        return Err(ExecutionLedgerError::Corrupt(
            "exchange source event deletion",
        ));
    }
    for (key, event) in &source.events {
        if !old_events.contains_key(key) {
            let body = encode(event)?;
            tx.execute(
                "INSERT INTO native_source_events VALUES(?1,?2,?3,?4)",
                params![source.run_id, key, checksum(&body), body],
            )
            .map_err(io)?;
            super::sqlite_finance::persist_event(tx, &source.run_id, key, event)?;
        } else if old_events.get(key) != Some(&checksum(&encode(event)?)) {
            return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
        }
    }
    let mut old_cursors = BTreeMap::<String, String>::new();
    {
        let mut q = tx
            .prepare("SELECT cursor_key,payload_digest FROM native_source_cursors WHERE run_id=?1")
            .map_err(io)?;
        let rows = q
            .query_map(params![source.run_id], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(io)?;
        for row in rows {
            let (key, hash) = row.map_err(io)?;
            old_cursors.insert(key, hash);
        }
    }
    if old_cursors
        .keys()
        .any(|key| !source.cursors.contains_key(key))
    {
        return Err(ExecutionLedgerError::Corrupt(
            "exchange source cursor deletion",
        ));
    }
    for (key, cursor) in &source.cursors {
        if !old_cursors.contains_key(key) {
            let body = encode(cursor)?;
            let time = crate::exchange_evidence::source_receipt_ms(&cursor.source_received_at)
                .ok_or(ExecutionLedgerError::Corrupt("invalid REST receipt time"))?;
            tx.execute(
                "INSERT INTO native_source_cursors VALUES(?1,?2,?3,?4,?5)",
                params![source.run_id, key, time, checksum(&body), body],
            )
            .map_err(io)?;
            super::sqlite_finance::persist_source_receipt(tx, &source.account_uid, cursor)?;
        } else if old_cursors.get(key) != Some(&checksum(&encode(cursor)?)) {
            return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
        }
    }
    Ok(())
}

pub(super) fn read(
    connection: &Connection,
    run_id: &str,
    expected_uid: &str,
) -> Result<DurableExchangeEvidence, ExecutionLedgerError> {
    let (uid, schema, proof): (String, String, String) = connection
        .query_row(
            "SELECT uid,source_schema,proof_digest FROM native_source_runs WHERE run_id=?1",
            params![run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(io)?;
    if uid != expected_uid {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    let mut events = BTreeMap::new();
    {
        let mut q = connection
            .prepare(
                "SELECT event_key,payload_digest,payload FROM native_source_events WHERE run_id=?1",
            )
            .map_err(io)?;
        let mut rows = q.query(params![run_id]).map_err(io)?;
        while let Some(row) = rows.next().map_err(io)? {
            let key: String = row.get(0).map_err(io)?;
            let hash: String = row.get(1).map_err(io)?;
            let bytes: Vec<u8> = row.get(2).map_err(io)?;
            if checksum(&bytes) != hash {
                return Err(ExecutionLedgerError::Corrupt(
                    "exchange source event checksum",
                ));
            }
            events.insert(key, decode::<StoredExchangeEvent>(&bytes)?);
        }
    }
    let mut cursors = BTreeMap::new();
    {
        let mut q = connection
            .prepare(
                "SELECT cursor_key,receipt_ms,payload_digest,payload
             FROM native_source_cursors WHERE run_id=?1",
            )
            .map_err(io)?;
        let mut rows = q.query(params![run_id]).map_err(io)?;
        while let Some(row) = rows.next().map_err(io)? {
            let key: String = row.get(0).map_err(io)?;
            let received: i64 = row.get(1).map_err(io)?;
            let hash: String = row.get(2).map_err(io)?;
            let bytes: Vec<u8> = row.get(3).map_err(io)?;
            let cursor: ExchangeCoverageCursor = decode(&bytes)?;
            if checksum(&bytes) != hash
                || u64::try_from(received).ok()
                    != crate::exchange_evidence::source_receipt_ms(&cursor.source_received_at)
            {
                return Err(ExecutionLedgerError::Corrupt(
                    "exchange source cursor checksum",
                ));
            }
            cursors.insert(key, cursor);
        }
    }
    let result = DurableExchangeEvidence {
        schema,
        account_uid: uid,
        run_id: run_id.to_owned(),
        events,
        cursors,
        checksum_sha256: proof,
    };
    result.validate()?;
    Ok(result)
}

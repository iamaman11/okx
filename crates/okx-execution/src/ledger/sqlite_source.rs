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

/// Merge source keys in their canonical index order: prior rows must
/// reappear byte-for-byte, and only NEW keys are held temporarily. Avoid
/// loading a duplicate BTreeMap of all historical exchange receipts.
#[derive(Clone, Copy)]
enum IndexedSourceKind {
    Events,
    Cursors,
}

fn new_source_keys<'a, T: serde::Serialize>(
    tx: &Transaction<'_>,
    run_id: &str,
    kind: IndexedSourceKind,
    current: &'a BTreeMap<String, T>,
) -> Result<Vec<&'a str>, ExecutionLedgerError> {
    let query = match kind {
        IndexedSourceKind::Events => {
            "SELECT event_key,payload_digest FROM native_source_events WHERE run_id=?1 ORDER BY event_key"
        }
        IndexedSourceKind::Cursors => {
            "SELECT cursor_key,payload_digest FROM native_source_cursors WHERE run_id=?1 ORDER BY cursor_key"
        }
    };
    let mut stmt = tx.prepare(query).map_err(io)?;
    let mut rows = stmt.query(params![run_id]).map_err(io)?;
    let mut incoming = current.iter();
    let mut next = incoming.next();
    let mut added = Vec::new();
    while let Some(row) = rows.next().map_err(io)? {
        let prior_key: String = row.get(0).map_err(io)?;
        let prior_hash: String = row.get(1).map_err(io)?;
        while next.is_some_and(|(key, _)| key.as_str() < prior_key.as_str()) {
            let (key, _) = next.expect("checked source next");
            added.push(key.as_str());
            next = incoming.next();
        }
        let Some((key, body)) = next else {
            return Err(ExecutionLedgerError::Corrupt(
                "native exchange source key was removed",
            ));
        };
        if key != &prior_key {
            return Err(ExecutionLedgerError::Corrupt(
                "native exchange source key was removed",
            ));
        }
        if checksum(&encode(body)?) != prior_hash {
            return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
        }
        next = incoming.next();
    }
    while let Some((key, _)) = next {
        added.push(key.as_str());
        next = incoming.next();
    }
    Ok(added)
}

pub(super) fn persist(
    tx: &Transaction<'_>,
    source: &DurableExchangeEvidence,
) -> Result<(), ExecutionLedgerError> {
    source.validate()?;
    let previous: Option<(String, String, String)> = tx
        .query_row(
            "SELECT uid,source_schema,proof_digest FROM native_source_runs WHERE run_id=?1",
            params![source.run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(io)?;
    if let Some((uid, schema, proof_digest)) = previous {
        if uid != source.account_uid || schema != source.schema {
            return Err(ExecutionLedgerError::AccountScopeMismatch);
        }
        // Durable source checksum validates all original events/cursors.
        // If identical, no source rows, indices or history scans are touched.
        if proof_digest == source.checksum_sha256 {
            return Ok(());
        }
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

    let new_events = new_source_keys(
        tx,
        &source.run_id,
        IndexedSourceKind::Events,
        &source.events,
    )?;
    for key in new_events {
        let event = source
            .events
            .get(key)
            .ok_or(ExecutionLedgerError::ExchangeEvidenceConflict)?;
        let body = encode(event)?;
        tx.execute(
            "INSERT INTO native_source_events VALUES(?1,?2,?3,?4)",
            params![source.run_id, key, checksum(&body), body],
        )
        .map_err(io)?;
        super::sqlite_finance::persist_event(tx, &source.run_id, key, event)?;
    }
    let new_cursors = new_source_keys(
        tx,
        &source.run_id,
        IndexedSourceKind::Cursors,
        &source.cursors,
    )?;
    for key in new_cursors {
        let cursor = source
            .cursors
            .get(key)
            .ok_or(ExecutionLedgerError::ExchangeEvidenceConflict)?;
        let body = encode(cursor)?;
        let time = crate::exchange_evidence::source_receipt_ms(&cursor.source_received_at)
            .ok_or(ExecutionLedgerError::Corrupt("invalid REST receipt time"))?;
        tx.execute(
            "INSERT INTO native_source_cursors VALUES(?1,?2,?3,?4,?5)",
            params![source.run_id, key, time, checksum(&body), body],
        )
        .map_err(io)?;
        super::sqlite_finance::persist_source_receipt(
            tx,
            &source.account_uid,
            &source.run_id,
            key,
            cursor,
        )?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexed_source_merge_only_returns_new_keys_and_rejects_loss_or_rewrite() {
        let mut connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(TABLES).unwrap();
        connection
            .execute(
                "INSERT INTO native_source_runs VALUES('run','uid','source','digest')",
                [],
            )
            .unwrap();
        for (key, value) in [("a", "prior-a"), ("c", "prior-c")] {
            let payload = encode(&value.to_owned()).unwrap();
            connection
                .execute(
                    "INSERT INTO native_source_events VALUES('run',?1,?2,?3)",
                    params![key, checksum(&payload), payload],
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO native_source_cursors VALUES('run',?1,1,?2,?3)",
                    params![key, checksum(&payload), payload],
                )
                .unwrap();
        }
        let tx = connection.transaction().unwrap();
        let current = BTreeMap::from([
            ("0".to_owned(), "new-zero".to_owned()),
            ("a".to_owned(), "prior-a".to_owned()),
            ("b".to_owned(), "new-b".to_owned()),
            ("c".to_owned(), "prior-c".to_owned()),
            ("z".to_owned(), "new-z".to_owned()),
        ]);
        for kind in [IndexedSourceKind::Events, IndexedSourceKind::Cursors] {
            let added = new_source_keys(&tx, "run", kind, &current).unwrap();
            assert_eq!(added, vec!["0", "b", "z"]);
            let deleted = BTreeMap::from([("c".to_owned(), "prior-c".to_owned())]);
            assert!(matches!(
                new_source_keys(&tx, "run", kind, &deleted),
                Err(ExecutionLedgerError::Corrupt(_))
            ));
            let mut rewritten = current.clone();
            rewritten.insert("a".to_owned(), "altered".to_owned());
            assert!(matches!(
                new_source_keys(&tx, "run", kind, &rewritten),
                Err(ExecutionLedgerError::ExchangeEvidenceConflict)
            ));
        }
        tx.rollback().unwrap();
    }
}

//! Indexed and immutable exchange source evidence in the SAME account SQLite transaction.
//! Exchange payload bytes are encoded as MessagePack. No v14 history is imported
//! into a freshly initialized account.
use super::sqlite_store::{checksum, decode, encode, io};
use super::*;
use crate::exchange_evidence::{ExchangeCoverageCursor, StoredExchangeEvent};
use rusqlite::{Connection, OptionalExtension, Transaction, params};

/// Reject an excessively large individual SQLite BLOB before Row::get
/// allocates it. This applies even when the row digest is self-consistent.
fn source_row_bytes(
    row: &rusqlite::Row<'_>,
    length_column: usize,
) -> Result<Vec<u8>, ExecutionLedgerError> {
    let len: i64 = row.get(length_column).map_err(io)?;
    if len <= 0 {
        return Err(ExecutionLedgerError::Corrupt("empty indexed source BLOB"));
    }
    let len = usize::try_from(len).map_err(|_| ExecutionLedgerError::ExchangeEvidenceCapacity)?;
    if len > crate::exchange_evidence::MAX_BYTES {
        return Err(ExecutionLedgerError::ExchangeEvidenceCapacity);
    }
    row.get(length_column + 1).map_err(io)
}

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
CREATE TABLE IF NOT EXISTS native_source_finance_order (
    run_id TEXT NOT NULL,
    event_key TEXT NOT NULL,
    event_kind TEXT NOT NULL CHECK(event_kind IN ('ORDER','FILL','BILL')),
    exchange_time_ms INTEGER NOT NULL CHECK(exchange_time_ms > 0),
    payload_digest TEXT NOT NULL,
    PRIMARY KEY(run_id,event_key),
    FOREIGN KEY(run_id,event_key) REFERENCES native_source_events(run_id,event_key)
) STRICT;
CREATE INDEX IF NOT EXISTS idx_native_financial_chrono
 ON native_source_finance_order(run_id,event_kind,exchange_time_ms,event_key);
CREATE TRIGGER IF NOT EXISTS immutable_finance_order_update
 BEFORE UPDATE ON native_source_finance_order BEGIN SELECT RAISE(ABORT,'IMMUTABLE_FINANCIAL_SOURCE'); END;
CREATE TRIGGER IF NOT EXISTS immutable_finance_order_delete
 BEFORE DELETE ON native_source_finance_order BEGIN SELECT RAISE(ABORT,'IMMUTABLE_FINANCIAL_SOURCE'); END;

CREATE TRIGGER IF NOT EXISTS immutable_native_event_update
BEFORE UPDATE ON native_source_events BEGIN SELECT RAISE(ABORT,'IMMUTABLE_SOURCE'); END;
CREATE TRIGGER IF NOT EXISTS immutable_native_event_delete
BEFORE DELETE ON native_source_events BEGIN SELECT RAISE(ABORT,'IMMUTABLE_SOURCE'); END;
CREATE TRIGGER IF NOT EXISTS immutable_native_cursor_update
BEFORE UPDATE ON native_source_cursors BEGIN SELECT RAISE(ABORT,'IMMUTABLE_SOURCE'); END;
CREATE TRIGGER IF NOT EXISTS immutable_native_cursor_delete
BEFORE DELETE ON native_source_cursors BEGIN SELECT RAISE(ABORT,'IMMUTABLE_SOURCE'); END;
";

/// An append-only SQL ordering projection, derived from the SAME sealed
/// source fact and inserted in the SAME WAL transaction. It is never an
/// independent execution or financial authority.
fn index_financial_source_event(
    tx: &Transaction<'_>,
    run_id: &str,
    event_key: &str,
    event: &StoredExchangeEvent,
) -> Result<(), ExecutionLedgerError> {
    let source = event
        .payload
        .get("source")
        .and_then(serde_json::Value::as_str)
        .ok_or(ExecutionLedgerError::Corrupt("missing native event source"))?;
    let (kind, field) = match source {
        "OKX_REST_ORDER" => ("ORDER", "uTime"),
        "OKX_REST_FILL" => ("FILL", "ts"),
        "OKX_REST_BILL" => ("BILL", "ts"),
        _ => {
            return Err(ExecutionLedgerError::Corrupt(
                "unknown native financial event source",
            ));
        }
    };
    let ts = event
        .payload
        .get(field)
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|s| s.parse::<i64>().ok()))
        })
        .filter(|n| *n > 0)
        .ok_or(ExecutionLedgerError::Corrupt(
            "invalid native indexed financial time",
        ))?;
    let digest = checksum(&encode(event)?);
    let inserted = tx
        .execute(
            "INSERT INTO native_source_finance_order
         (run_id,event_key,event_kind,exchange_time_ms,payload_digest)
         VALUES(?1,?2,?3,?4,?5) ON CONFLICT(run_id,event_key) DO NOTHING",
            params![run_id, event_key, kind, ts, &digest],
        )
        .map_err(io)?;
    if inserted == 0 {
        let saved: (String, i64, String) = tx
            .query_row(
                "SELECT event_kind,exchange_time_ms,payload_digest
             FROM native_source_finance_order WHERE run_id=?1 AND event_key=?2",
                params![run_id, event_key],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(io)?;
        if saved.0 != kind || saved.1 != ts || saved.2 != digest {
            return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
        }
    }
    Ok(())
}

/// Every active finance read consumes ordered, typed SQL rows. Historical
/// events remain sealed on disk; the callback never stores a full source map.
pub(super) fn visit_financial_kind<F>(
    connection: &Connection,
    run_id: &str,
    expected_uid: &str,
    kind: &str,
    mut visit: F,
) -> Result<(), ExecutionLedgerError>
where
    F: FnMut(&str, &StoredExchangeEvent) -> Result<(), ExecutionLedgerError>,
{
    let uid: String = connection
        .query_row(
            "SELECT uid FROM native_source_runs WHERE run_id=?1",
            params![run_id],
            |r| r.get(0),
        )
        .map_err(io)?;
    if uid != expected_uid || !matches!(kind, "FILL" | "BILL") {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    let mut stmt = connection
        .prepare(
            "SELECT e.event_key,e.payload_digest,length(e.payload),e.payload,
                i.payload_digest,i.exchange_time_ms
         FROM native_source_finance_order i
         JOIN native_source_events e ON e.run_id=i.run_id AND e.event_key=i.event_key
         WHERE i.run_id=?1 AND i.event_kind=?2
           AND (?2<>'BILL' OR (i.event_key>='bill:' AND i.event_key<'bill;'))
         ORDER BY i.exchange_time_ms,i.event_key",
        )
        .map_err(io)?;
    let mut rows = stmt.query(params![run_id, kind]).map_err(io)?;
    while let Some(row) = rows.next().map_err(io)? {
        let key: String = row.get(0).map_err(io)?;
        let digest: String = row.get(1).map_err(io)?;
        let bytes = source_row_bytes(row, 2)?;
        let indexed_digest: String = row.get(4).map_err(io)?;
        let indexed_time: i64 = row.get(5).map_err(io)?;
        if checksum(&bytes) != digest || digest != indexed_digest {
            return Err(ExecutionLedgerError::Corrupt(
                "native ordered finance source digest",
            ));
        }
        let event: StoredExchangeEvent = decode(&bytes)?;
        let expected = if kind == "FILL" {
            "OKX_REST_FILL"
        } else {
            "OKX_REST_BILL"
        };
        let actual_time = event.payload.get("ts").and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
        });
        if event.payload["source"] != expected || actual_time != Some(indexed_time) {
            return Err(ExecutionLedgerError::Corrupt(
                "native ordered finance source identity",
            ));
        }
        let event = if kind == "BILL" {
            effective_bill_event(connection, run_id, &key, event)?
        } else {
            event
        };
        visit(&key, &event)?;
    }
    Ok(())
}

/// Cold, read-only inventory verification: both directions are checked.
/// The primary event is authoritative; every index row must match its
/// source hash, and every source must have exactly one projected row.
pub(super) fn audit_financial_index_inventory(c: &Connection) -> Result<(), ExecutionLedgerError> {
    let missing: i64 = c
        .query_row(
            "SELECT COUNT(*) FROM native_source_events e
         LEFT JOIN native_source_finance_order i
           ON i.run_id=e.run_id AND i.event_key=e.event_key
         WHERE i.event_key IS NULL OR i.payload_digest<>e.payload_digest",
            [],
            |row| row.get(0),
        )
        .map_err(io)?;
    let orphan: i64 = c
        .query_row(
            "SELECT COUNT(*) FROM native_source_finance_order i
         LEFT JOIN native_source_events e
           ON e.run_id=i.run_id AND e.event_key=i.event_key
         WHERE e.event_key IS NULL OR i.payload_digest<>e.payload_digest",
            [],
            |row| row.get(0),
        )
        .map_err(io)?;
    if missing != 0 || orphan != 0 {
        return Err(ExecutionLedgerError::Corrupt(
            "native chronological financial index/source mismatch",
        ));
    }
    Ok(())
}

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
        index_financial_source_event(tx, &source.run_id, key, event)?;
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

/// A newer authenticated REST bill may add a previously unavailable exact
/// balance change. The original source event never changes. Only the missing
/// optional financial fields may be completed, and only once.
fn additive_bill_detail(
    original_key: &str,
    prior: &StoredExchangeEvent,
    latest: &StoredExchangeEvent,
) -> bool {
    let Some(bill_id) = original_key.strip_prefix("bill:") else {
        return false;
    };
    if prior.payload["source"] != "OKX_REST_BILL"
        || latest.payload["source"] != "OKX_REST_BILL"
        || prior.payload["billId"].as_str() != Some(bill_id)
        || latest.payload["billId"].as_str() != Some(bill_id)
        || prior.payload["balChg"].as_str().is_some()
        || latest.payload["balChg"]
            .as_str()
            .is_none_or(|s| s.parse::<rust_decimal::Decimal>().is_err())
    {
        return false;
    }
    let mut normalized = latest.clone();
    let (Some(prior_fields), Some(current_fields)) =
        (prior.payload.as_object(), normalized.payload.as_object_mut())
    else {
        return false;
    };
    if prior_fields.contains_key("balChg") {
        current_fields.insert("balChg".to_owned(), serde_json::Value::Null);
    } else {
        current_fields.remove("balChg");
    }
    if !prior_fields.contains_key("type") {
        current_fields.remove("type");
    }
    &normalized == prior
}

/// An indexed lookup of the only allowed additive financial witness.
/// The original event and the new immutable bytes are independently hashed.
pub(super) fn effective_bill_event(
    connection: &Connection,
    run_id: &str,
    key: &str,
    original: StoredExchangeEvent,
) -> Result<StoredExchangeEvent, ExecutionLedgerError> {
    let Some(id) = key.strip_prefix("bill:") else {
        return Ok(original);
    };
    let proof_key = format!("bill-proof:{id}");
    let proof: Option<(String, Vec<u8>)> = connection
        .query_row(
            "SELECT payload_digest,payload FROM native_source_events
             WHERE run_id=?1 AND event_key=?2",
            params![run_id, proof_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(io)?;
    let Some((digest, bytes)) = proof else {
        return Ok(original);
    };
    if bytes.is_empty()
        || bytes.len() > crate::exchange_evidence::MAX_BYTES
        || checksum(&bytes) != digest
    {
        return Err(ExecutionLedgerError::Corrupt(
            "additive bill witness digest invalid",
        ));
    }
    let latest: StoredExchangeEvent = decode(&bytes)?;
    if !additive_bill_detail(key, &original, &latest) {
        return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
    }
    Ok(latest)
}

/// Append ONLY facts observed in the current authenticated venue window.
/// Previously sealed facts stay in SQLite and are never materialized into a
/// Rust event map. Every insert, financial projection and source proof shares
/// the caller's one immediate WAL transaction.
pub(super) fn append_window_delta(
    tx: &Transaction<'_>,
    delta: &DurableExchangeEvidence,
) -> Result<bool, ExecutionLedgerError> {
    delta.validate()?;
    let old: Option<(String, String)> = tx
        .query_row(
            "SELECT uid,source_schema FROM native_source_runs WHERE run_id=?1",
            params![&delta.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(io)?;
    if let Some((uid, schema)) = &old {
        if uid != &delta.account_uid || schema != &delta.schema {
            return Err(ExecutionLedgerError::AccountScopeMismatch);
        }
        // A prior sealed source cannot be overwritten to repair missing rows.
        attest_indexed(tx, &delta.run_id, &delta.account_uid)?;
    } else {
        tx.execute(
            "INSERT INTO native_source_runs(run_id,uid,source_schema,proof_digest) VALUES(?1,?2,?3,?4)",
            params![&delta.run_id, &delta.account_uid, &delta.schema, &delta.checksum_sha256],
        )
        .map_err(io)?;
    }
    let mut changed = old.is_none();
    for (key, event) in &delta.events {
        let existing: Option<(String, Vec<u8>)> = tx
            .query_row(
                "SELECT payload_digest,payload FROM native_source_events WHERE run_id=?1 AND event_key=?2",
                params![&delta.run_id, key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(io)?;
        if let Some((prior_digest, body)) = existing {
            if checksum(&body) != prior_digest {
                return Err(ExecutionLedgerError::Corrupt(
                    "native source event digest mismatch",
                ));
            }
            let earlier: StoredExchangeEvent = decode(&body)?;
            if earlier != *event {
                if !additive_bill_detail(key, &earlier, event) {
                    return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
                }
                // One canonical additive witness is enough. If the venue
                // returns it again, compare exact bytes; a conflicting second
                // observation cannot replace the first or silently change PnL.
                let proof_key = format!(
                    "bill-proof:{}",
                    key.strip_prefix("bill:").ok_or(
                        ExecutionLedgerError::ExchangeEvidenceConflict
                    )?
                );
                let proof: Option<(String, Vec<u8>)> = tx.query_row(
                    "SELECT payload_digest,payload FROM native_source_events
                     WHERE run_id=?1 AND event_key=?2",
                    params![&delta.run_id, &proof_key],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                ).optional().map_err(io)?;
                if let Some((digest, bytes)) = proof {
                    if checksum(&bytes) != digest || decode::<StoredExchangeEvent>(&bytes)? != *event {
                        return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
                    }
                } else {
                    let enriched = encode(event)?;
                    tx.execute(
                        "INSERT INTO native_source_events VALUES(?1,?2,?3,?4)",
                        params![&delta.run_id, &proof_key, checksum(&enriched), enriched],
                    ).map_err(io)?;
                    super::sqlite_finance::persist_event(tx, &delta.run_id, &proof_key, event)?;
                    index_financial_source_event(tx, &delta.run_id, &proof_key, event)?;
                    changed = true;
                }
            }
            continue;
        }
        let body = encode(event)?;
        tx.execute(
            "INSERT INTO native_source_events VALUES(?1,?2,?3,?4)",
            params![&delta.run_id, key, checksum(&body), body],
        )
        .map_err(io)?;
        super::sqlite_finance::persist_event(tx, &delta.run_id, key, event)?;
        index_financial_source_event(tx, &delta.run_id, key, event)?;
        changed = true;
    }
    for (key, cursor) in &delta.cursors {
        let prior: Option<(String, Vec<u8>)> = tx
            .query_row(
                "SELECT payload_digest,payload FROM native_source_cursors WHERE run_id=?1 AND cursor_key=?2",
                params![&delta.run_id, key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(io)?;
        if let Some((digest, body)) = prior {
            if checksum(&body) != digest || decode::<ExchangeCoverageCursor>(&body)? != *cursor {
                return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
            }
            continue;
        }
        let body = encode(cursor)?;
        let receipt = crate::exchange_evidence::source_receipt_ms(&cursor.source_received_at)
            .ok_or(ExecutionLedgerError::Corrupt(
                "native cursor receipt time missing",
            ))?;
        tx.execute(
            "INSERT INTO native_source_cursors VALUES(?1,?2,?3,?4,?5)",
            params![&delta.run_id, key, receipt, checksum(&body), body],
        )
        .map_err(io)?;
        super::sqlite_finance::persist_source_receipt(
            tx,
            &delta.account_uid,
            &delta.run_id,
            key,
            cursor,
        )?;
        changed = true;
    }
    if changed {
        // One streaming pass recalculates the original cryptographic v1 tuple.
        // No complete historical event object or duplicate RAM index exists.
        let identity = scan_indexed(tx, &delta.run_id, &delta.account_uid, false)?;
        let written = tx
            .execute(
                "UPDATE native_source_runs SET proof_digest=?1 WHERE run_id=?2",
                params![identity.checksum_sha256, &delta.run_id],
            )
            .map_err(io)?;
        if written != 1 {
            return Err(ExecutionLedgerError::Corrupt(
                "native source proof publication lost",
            ));
        }
    }
    Ok(changed)
}

/// Exact, bounded-memory native source summary. Cursors are scanned in
/// receipt order and individually authenticated; old events remain indexed
/// in SQLite rather than being reconstructed as a historical Rust map.
/// Cold owner attestation separately verifies the complete source digest.
pub(super) fn summary(
    connection: &Connection,
    run_id: &str,
    expected_uid: &str,
) -> Result<ExchangeEvidenceSummary, ExecutionLedgerError> {
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
    if schema != crate::exchange_evidence::EXCHANGE_EVIDENCE_SCHEMA_V1
        || proof.len() != 64
        || !proof
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(ExecutionLedgerError::Corrupt(
            "native source summary identity",
        ));
    }
    let event_rows: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM native_source_events WHERE run_id=?1",
            params![run_id],
            |row| row.get(0),
        )
        .map_err(io)?;
    let events = usize::try_from(event_rows)
        .map_err(|_| ExecutionLedgerError::Corrupt("native event inventory overflow"))?;
    let mut stmt = connection
        .prepare(
            "SELECT cursor_key,receipt_ms,payload_digest,length(payload),payload
             FROM native_source_cursors WHERE run_id=?1
             ORDER BY receipt_ms,cursor_key",
        )
        .map_err(io)?;
    let mut rows = stmt.query(params![run_id]).map_err(io)?;
    let mut cursors = 0usize;
    let mut oldest_asof = None::<u64>;
    let mut latest_asof = None::<u64>;
    let mut previous_receipt = None::<u64>;
    let mut largest_gap = None::<u64>;
    let mut latest_complete = false;
    while let Some(row) = rows.next().map_err(io)? {
        let _key: String = row.get(0).map_err(io)?;
        let stored_receipt: i64 = row.get(1).map_err(io)?;
        let digest: String = row.get(2).map_err(io)?;
        let bytes = source_row_bytes(row, 3)?;
        if checksum(&bytes) != digest {
            return Err(ExecutionLedgerError::Corrupt(
                "native source summary checksum",
            ));
        }
        let cursor: ExchangeCoverageCursor = decode(&bytes)?;
        let receipt = crate::exchange_evidence::source_receipt_ms(&cursor.source_received_at)
            .ok_or(ExecutionLedgerError::Corrupt(
                "invalid native source receipt",
            ))?;
        if i64::try_from(receipt).ok() != Some(stored_receipt)
            || previous_receipt.is_some_and(|prior| receipt < prior)
        {
            return Err(ExecutionLedgerError::Corrupt(
                "native source receipt index mismatch",
            ));
        }
        // The v1 source proof computes gaps over DISTINCT receipt epochs.
        // Two valid source receipts at the same timestamp are not a new gap.
        if let Some(prior) = previous_receipt
            && receipt > prior
        {
            let gap = receipt - prior;
            largest_gap = Some(largest_gap.unwrap_or(0).max(gap));
        }
        previous_receipt = Some(receipt);
        oldest_asof =
            Some(oldest_asof.map_or(cursor.account_as_of_ms, |v| v.min(cursor.account_as_of_ms)));
        latest_asof =
            Some(latest_asof.map_or(cursor.account_as_of_ms, |v| v.max(cursor.account_as_of_ms)));
        latest_complete = cursor.order_complete && cursor.fill_complete && cursor.bill_complete;
        cursors += 1;
    }
    Ok(ExchangeEvidenceSummary {
        schema: crate::exchange_evidence::EXCHANGE_EVIDENCE_SCHEMA_V1,
        run_id: run_id.to_owned(),
        events,
        cursors,
        checksum_sha256: proof,
        latest_source_complete: previous_receipt.is_some() && latest_complete,
        oldest_source_asof_ms: oldest_asof,
        latest_source_asof_ms: latest_asof,
        latest_source_received_at_ms: previous_receipt,
        largest_persisted_gap_ms: largest_gap,
        source_gap_over_two_hours: largest_gap
            .is_some_and(|gap| gap > crate::exchange_evidence::MAX_SOURCE_COVERAGE_GAP_MS),
        // Native histories have no fixed number-of-rows limit.
        // Retain v1 observer-counter semantics for wire compatibility.
        // A zero here is not a SQL persistence limit; native epochs have
        // no historic row-count capacity ceiling.
        remaining_source_cursors: crate::exchange_evidence::MAX_CURSORS.saturating_sub(cursors),
        remaining_events: crate::exchange_evidence::MAX_EVENTS.saturating_sub(events),
        next_source_coverage_deadline_ms: previous_receipt.and_then(|last| {
            last.checked_add(crate::exchange_evidence::MAX_SOURCE_COVERAGE_GAP_MS)
        }),
    })
}

/// Stream the original v1 canonical JSON tuple through SHA256 without
/// materializing either exchange map. Row digests, source identity and the
/// exact historical tuple digest must all agree before a native owner can
/// report a valid cold attestation.
struct SourceHashWriter {
    digest: Sha256,
    tuple_bytes: usize,
}

impl SourceHashWriter {
    fn new() -> Self {
        Self {
            digest: Sha256::new(),
            tuple_bytes: 0,
        }
    }

    fn push(&mut self, bytes: &[u8]) {
        self.digest.update(bytes);
        self.tuple_bytes = self.tuple_bytes.saturating_add(bytes.len());
    }

    fn json<T: serde::Serialize>(&mut self, value: &T) -> Result<(), ExecutionLedgerError> {
        serde_json::to_writer(&mut *self, value)?;
        Ok(())
    }

    fn finish(self) -> (String, usize) {
        (format!("{:x}", self.digest.finalize()), self.tuple_bytes)
    }
}

impl std::io::Write for SourceHashWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.push(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The original hash covers a JSON tuple, while the legacy 24 MiB capacity
/// rule covers the JSON struct, including its checksum field. Both have
/// identical serialized map entries; the envelope size delta is constant.
/// Serialize ONLY empty envelopes to preserve exact legacy byte accounting.
fn source_struct_overhead(
    schema: &str,
    uid: &str,
    run_id: &str,
    proof: &str,
) -> Result<usize, ExecutionLedgerError> {
    let empty = DurableExchangeEvidence {
        schema: schema.to_owned(),
        account_uid: uid.to_owned(),
        run_id: run_id.to_owned(),
        events: BTreeMap::new(),
        cursors: BTreeMap::new(),
        checksum_sha256: proof.to_owned(),
    };
    let struct_len = serde_json::to_vec(&empty)?.len();
    let tuple_len = serde_json::to_vec(&(
        &empty.schema,
        &empty.account_uid,
        &empty.run_id,
        &empty.events,
        &empty.cursors,
    ))?
    .len();
    struct_len
        .checked_sub(tuple_len)
        .ok_or(ExecutionLedgerError::Corrupt("source JSON envelope length"))
}

pub(super) fn attest_indexed(
    connection: &Connection,
    run_id: &str,
    expected_uid: &str,
) -> Result<crate::strategy_lifecycle::SourceIdentity, ExecutionLedgerError> {
    scan_indexed(connection, run_id, expected_uid, true)
}

/// The same canonical streaming digest validates both cold attestation and
/// the new append-only source image before its proof is published.
fn scan_indexed(
    connection: &Connection,
    run_id: &str,
    expected_uid: &str,
    enforce_previous_proof: bool,
) -> Result<crate::strategy_lifecycle::SourceIdentity, ExecutionLedgerError> {
    let (uid, schema, proof): (String, String, String) = connection
        .query_row(
            "SELECT uid,source_schema,proof_digest FROM native_source_runs WHERE run_id=?1",
            params![run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(io)?;
    if uid != expected_uid || schema != crate::EXCHANGE_EVIDENCE_SCHEMA_V1 {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    if uid.trim().is_empty() || run_id.trim().is_empty() {
        return Err(ExecutionLedgerError::Corrupt(
            "indexed exchange source identity missing",
        ));
    }
    let mut writer = SourceHashWriter::new();
    writer.push(b"[");
    writer.json(&schema)?;
    writer.push(b",");
    writer.json(&uid)?;
    writer.push(b",");
    writer.json(&run_id)?;
    writer.push(b",{");

    let mut stmt = connection
        .prepare(
            "SELECT event_key,payload_digest,length(payload),payload
             FROM native_source_events WHERE run_id=?1 ORDER BY event_key",
        )
        .map_err(io)?;
    let mut rows = stmt.query(params![run_id]).map_err(io)?;
    let mut count = 0usize;
    while let Some(row) = rows.next().map_err(io)? {
        let key: String = row.get(0).map_err(io)?;
        let saved_hash: String = row.get(1).map_err(io)?;
        let bytes = source_row_bytes(row, 2)?;
        if checksum(&bytes) != saved_hash {
            return Err(ExecutionLedgerError::Corrupt("indexed source event digest"));
        }
        let event: StoredExchangeEvent = decode(&bytes)?;
        if !matches!(event.allocation.as_str(), "RUN_OWNED" | "UNALLOCATED")
            || (event.allocation == "RUN_OWNED"
                && (event.strategy_run_id.as_deref() != Some(run_id)
                    || event.source_intent_id.is_none()))
            || (event.allocation == "UNALLOCATED" && event.strategy_run_id.is_some())
        {
            return Err(ExecutionLedgerError::Corrupt("indexed source allocation"));
        }
        if count != 0 {
            writer.push(b",");
        }
        writer.json(&key)?;
        writer.push(b":");
        writer.json(&event)?;
        count += 1;
    }
    drop(rows);
    drop(stmt);
    writer.push(b"},{");

    let mut stmt = connection
        .prepare(
            "SELECT cursor_key,receipt_ms,payload_digest,length(payload),payload
             FROM native_source_cursors WHERE run_id=?1 ORDER BY cursor_key",
        )
        .map_err(io)?;
    let mut rows = stmt.query(params![run_id]).map_err(io)?;
    let mut count = 0usize;
    while let Some(row) = rows.next().map_err(io)? {
        let key: String = row.get(0).map_err(io)?;
        let receipt_ms: i64 = row.get(1).map_err(io)?;
        let saved_hash: String = row.get(2).map_err(io)?;
        let bytes = source_row_bytes(row, 3)?;
        if checksum(&bytes) != saved_hash {
            return Err(ExecutionLedgerError::Corrupt(
                "indexed source cursor digest",
            ));
        }
        let cursor: ExchangeCoverageCursor = decode(&bytes)?;
        if i64::try_from(
            crate::exchange_evidence::source_receipt_ms(&cursor.source_received_at).ok_or(
                ExecutionLedgerError::Corrupt("invalid indexed source receipt"),
            )?,
        )
        .ok()
            != Some(receipt_ms)
        {
            return Err(ExecutionLedgerError::Corrupt(
                "indexed source receipt index",
            ));
        }
        if count != 0 {
            writer.push(b",");
        }
        writer.json(&key)?;
        writer.push(b":");
        writer.json(&cursor)?;
        count += 1;
    }
    drop(rows);
    drop(stmt);
    writer.push(b"}]");
    let (actual, tuple_len) = writer.finish();
    if enforce_previous_proof && actual != proof {
        return Err(ExecutionLedgerError::Corrupt(
            "indexed source canonical digest mismatch",
        ));
    }
    let proof = actual;
    let serialized_bytes = tuple_len
        .checked_add(source_struct_overhead(&schema, &uid, run_id, &proof)?)
        .ok_or(ExecutionLedgerError::ExchangeEvidenceCapacity)?;
    // Canonical hash/bytes are streamed. Total historical bytes are not
    // subject to the old v14 document cap; each row stays size-limited.
    let _authenticated_source_bytes = serialized_bytes;
    let summary = summary(connection, run_id, expected_uid)?;
    Ok(crate::strategy_lifecycle::SourceIdentity {
        account_uid: uid,
        run_id: run_id.to_owned(),
        checksum_sha256: proof,
        source_gap_over_two_hours: summary.source_gap_over_two_hours,
    })
}

/// Exact key lookup for NAV/source lineage after a full immutable
/// streaming digest has attested the same WAL read snapshot.
pub(super) fn indexed_cursor(
    connection: &Connection,
    run_id: &str,
    key: &str,
) -> Result<Option<ExchangeCoverageCursor>, ExecutionLedgerError> {
    let mut stmt = connection
        .prepare(
            "SELECT receipt_ms,payload_digest,length(payload),payload
             FROM native_source_cursors WHERE run_id=?1 AND cursor_key=?2",
        )
        .map_err(io)?;
    let mut rows = stmt.query(params![run_id, key]).map_err(io)?;
    let Some(row) = rows.next().map_err(io)? else {
        return Ok(None);
    };
    let receipt: i64 = row.get(0).map_err(io)?;
    let hash: String = row.get(1).map_err(io)?;
    let bytes = source_row_bytes(row, 2)?;
    if checksum(&bytes) != hash {
        return Err(ExecutionLedgerError::Corrupt(
            "indexed NAV source cursor checksum",
        ));
    }
    let cursor: ExchangeCoverageCursor = decode(&bytes)?;
    if i64::try_from(
        crate::exchange_evidence::source_receipt_ms(&cursor.source_received_at)
            .ok_or(ExecutionLedgerError::Corrupt("invalid NAV source receipt"))?,
    )
    .ok()
        != Some(receipt)
    {
        return Err(ExecutionLedgerError::Corrupt(
            "indexed NAV source cursor timestamp",
        ));
    }
    Ok(Some(cursor))
}

/// Stream one account-scoped native source without constructing a
/// historical Rust BTreeMap. Each immutable row is hash-verified, decoded,
/// handed to the caller and discarded before advancing the WAL cursor.
/// The caller must pin one consistent WAL snapshot for related queries.
pub(super) fn visit_events<F>(
    connection: &Connection,
    run_id: &str,
    expected_uid: &str,
    visit: F,
) -> Result<(), ExecutionLedgerError>
where
    F: FnMut(&str, &StoredExchangeEvent) -> Result<(), ExecutionLedgerError>,
{
    visit_events_scoped(connection, run_id, expected_uid, false, visit)
}

/// NAV and daily STOP need exact native bills, not the complete historical
/// order/fill source. The (run_id,event_key) primary key provides a bounded
/// ordered seek by the canonical immutable "bill:" key namespace.
pub(super) fn visit_bill_events<F>(
    connection: &Connection,
    run_id: &str,
    expected_uid: &str,
    visit: F,
) -> Result<(), ExecutionLedgerError>
where
    F: FnMut(&str, &StoredExchangeEvent) -> Result<(), ExecutionLedgerError>,
{
    visit_events_scoped(connection, run_id, expected_uid, true, visit)
}

fn visit_events_scoped<F>(
    connection: &Connection,
    run_id: &str,
    expected_uid: &str,
    only_bills: bool,
    mut visit: F,
) -> Result<(), ExecutionLedgerError>
where
    F: FnMut(&str, &StoredExchangeEvent) -> Result<(), ExecutionLedgerError>,
{
    let (uid, schema): (String, String) = connection
        .query_row(
            "SELECT uid,source_schema FROM native_source_runs WHERE run_id=?1",
            params![run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(io)?;
    if run_id.is_empty()
        || uid.is_empty()
        || uid != expected_uid
        || schema != crate::exchange_evidence::EXCHANGE_EVIDENCE_SCHEMA_V1
    {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    let statement = if only_bills {
        "SELECT event_key,payload_digest,length(payload),payload
         FROM native_source_events
         WHERE run_id=?1 AND event_key>='bill:' AND event_key<'bill;'
         ORDER BY event_key"
    } else {
        "SELECT event_key,payload_digest,length(payload),payload
         FROM native_source_events WHERE run_id=?1 ORDER BY event_key"
    };
    let mut stmt = connection.prepare(statement).map_err(io)?;
    let mut rows = stmt.query(params![run_id]).map_err(io)?;
    while let Some(row) = rows.next().map_err(io)? {
        let key: String = row.get(0).map_err(io)?;
        let digest: String = row.get(1).map_err(io)?;
        let bytes = source_row_bytes(row, 2)?;
        if checksum(&bytes) != digest {
            return Err(ExecutionLedgerError::Corrupt(
                "indexed event stream digest mismatch",
            ));
        }
        let event: StoredExchangeEvent = decode(&bytes)?;
        if only_bills
            && (!key.starts_with("bill:")
                || event
                    .payload
                    .get("source")
                    .and_then(serde_json::Value::as_str)
                    != Some("OKX_REST_BILL"))
        {
            return Err(ExecutionLedgerError::Corrupt(
                "indexed bill source kind mismatch",
            ));
        }
        let event = if only_bills {
            effective_bill_event(connection, run_id, &key, event)?
        } else {
            event
        };
        visit(&key, &event)?;
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
    visit_events(connection, run_id, expected_uid, |key, event| {
        if events.insert(key.to_owned(), event.clone()).is_some() {
            return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
        }
        Ok(())
    })?;
    let mut cursors = BTreeMap::new();
    {
        let mut q = connection
            .prepare(
                "SELECT cursor_key,receipt_ms,payload_digest,length(payload),payload
             FROM native_source_cursors WHERE run_id=?1",
            )
            .map_err(io)?;
        let mut rows = q.query(params![run_id]).map_err(io)?;
        while let Some(row) = rows.next().map_err(io)? {
            let key: String = row.get(0).map_err(io)?;
            let received: i64 = row.get(1).map_err(io)?;
            let hash: String = row.get(2).map_err(io)?;
            let bytes = source_row_bytes(row, 3)?;
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
    fn native_sql_window_deltas_match_exact_full_source_and_reject_rewrites() {
        let mut c = Connection::open_in_memory().unwrap();
        c.execute_batch(include_str!("../../schema/account-ledger-v1.sql"))
            .unwrap();
        c.execute_batch(TABLES).unwrap();
        let mut empty = DurableExchangeEvidence {
            schema: crate::EXCHANGE_EVIDENCE_SCHEMA_V1.to_owned(),
            account_uid: "a".repeat(64),
            run_id: "sqlite_native_delta_period_001".to_owned(),
            events: BTreeMap::new(),
            cursors: BTreeMap::new(),
            checksum_sha256: String::new(),
        };
        empty.seal().unwrap();
        let order = |id: &str, time: &str| StoredExchangeEvent {
            payload: serde_json::json!({
                "source": "OKX_REST_ORDER",
                "ordId": id,
                "instId": "BTC-USDT-SWAP",
                "clOrdId": null,
                "state": "live",
                "uTime": time
            }),
            source_intent_id: None,
            strategy_run_id: None,
            allocation: "UNALLOCATED".to_owned(),
        };
        let fill = |trade: &str, order: &str, time: u64| StoredExchangeEvent {
            payload: serde_json::json!({
                "source": "OKX_REST_FILL",
                "instType": "SWAP", "instId": "BTC-USDT-SWAP",
                "tradeId": trade, "ordId": order,
                "side": "buy", "posSide": "long",
                "fillPx": "100", "fillSz": "0.01",
                "fee": "-0.01", "feeCcy": "USDT", "ts": time
            }),
            source_intent_id: None,
            strategy_run_id: None,
            allocation: "UNALLOCATED".to_owned(),
        };
        let cursor = |receipt: &str| ExchangeCoverageCursor {
            source: "OKX_REST_ACCOUNT_HISTORY".to_owned(),
            source_received_at: receipt.to_owned(),
            generation: receipt.to_owned(),
            account_as_of_ms: 1000,
            order_complete: true,
            fill_complete: true,
            bill_complete: true,
            covered_ranges: serde_json::json!({}),
        };
        let mut first = empty.clone();
        first.events.insert(
            "order:external-1:1000".to_owned(),
            order("external-1", "1000"),
        );
        first.events.insert(
            "fill:SWAP:BTC-USDT-SWAP:fill-later".to_owned(),
            fill("fill-later", "external-1", 2500),
        );
        first
            .cursors
            .insert("receipt-1".to_owned(), cursor("2026-10-20T12:00:00Z"));
        first.seal().unwrap();
        {
            let tx = c.transaction().unwrap();
            assert!(append_window_delta(&tx, &first).unwrap());
            tx.commit().unwrap();
        }
        assert_eq!(
            attest_indexed(&c, &empty.run_id, &empty.account_uid)
                .unwrap()
                .checksum_sha256,
            first.checksum_sha256
        );
        let mut second = empty.clone();
        second.events.insert(
            "order:external-2:2000".to_owned(),
            order("external-2", "2000"),
        );
        second.events.insert(
            "fill:SWAP:BTC-USDT-SWAP:fill-earlier".to_owned(),
            fill("fill-earlier", "external-2", 1500),
        );
        second
            .cursors
            .insert("receipt-2".to_owned(), cursor("2026-10-20T12:05:00Z"));
        second.events.insert(
            "bill:bill-native-1".to_owned(),
            StoredExchangeEvent {
                payload: serde_json::json!({
                    "source": "OKX_REST_BILL",
                    "billId": "bill-native-1",
                    "instId": "BTC-USDT-SWAP",
                    "tradeId": null,
                    "ordId": null,
                    "clOrdId": null,
                    "ccy": "USDT",
                    "type": "2",
                    "subType": "5",
                    "balChg": "-0.02",
                    "fee": "-0.02",
                    "pnl": "0",
                    "ts": 2000
                }),
                source_intent_id: None,
                strategy_run_id: None,
                allocation: "UNALLOCATED".to_owned(),
            },
        );
        second.seal().unwrap();
        {
            let tx = c.transaction().unwrap();
            assert!(append_window_delta(&tx, &second).unwrap());
            tx.commit().unwrap();
        }
        let mut combined = first.clone();
        combined.events.extend(second.events.clone());
        combined.cursors.extend(second.cursors.clone());
        combined.seal().unwrap();
        assert_eq!(
            attest_indexed(&c, &empty.run_id, &empty.account_uid)
                .unwrap()
                .checksum_sha256,
            combined.checksum_sha256,
            "SQL incremental writes must preserve original v1 canonical hash"
        );
        {
            let tx = c.transaction().unwrap();
            assert!(!append_window_delta(&tx, &first).unwrap());
            tx.commit().unwrap();
        }
        let mut conflict = first.clone();
        conflict
            .events
            .get_mut("order:external-1:1000")
            .unwrap()
            .payload["state"] = serde_json::json!("canceled");
        conflict.seal().unwrap();
        {
            let tx = c.transaction().unwrap();
            assert!(matches!(
                append_window_delta(&tx, &conflict),
                Err(ExecutionLedgerError::ExchangeEvidenceConflict)
            ));
        }
        assert_eq!(
            attest_indexed(&c, &empty.run_id, &empty.account_uid)
                .unwrap()
                .checksum_sha256,
            combined.checksum_sha256
        );
        let count: i64 = c
            .query_row("SELECT COUNT(*) FROM exchange_orders", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);
        let mut seen_times = Vec::new();
        visit_financial_kind(&c, &empty.run_id, &empty.account_uid, "FILL", |_, event| {
            seen_times.push(event.payload["ts"].as_u64().unwrap());
            Ok(())
        })
        .unwrap();
        assert_eq!(
            seen_times,
            vec![1500, 2500],
            "out-of-order REST arrivals must be processed chronologically"
        );
        let mut bills = 0usize;
        visit_bill_events(&c, &empty.run_id, &empty.account_uid, |_, _| {
            bills += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(bills, 1, "only the canonical bill event is visited");
        let persisted_bills: i64 = c
            .query_row("SELECT COUNT(*) FROM bills", [], |r| r.get(0))
            .unwrap();
        assert_eq!(persisted_bills, 1, "bill and source must commit together");
    }

    #[test]
    fn native_source_summary_matches_exact_legacy_receipt_semantics_and_denies_tampering() {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(TABLES).unwrap();
        let uid = "uid";
        let run_id = "run";
        let mut source = DurableExchangeEvidence {
            schema: crate::exchange_evidence::EXCHANGE_EVIDENCE_SCHEMA_V1.to_owned(),
            account_uid: uid.to_owned(),
            run_id: run_id.to_owned(),
            events: BTreeMap::new(),
            cursors: BTreeMap::new(),
            checksum_sha256: String::new(),
        };
        let cursor = ExchangeCoverageCursor {
            source: "OKX_REST_ACCOUNT_HISTORY".to_owned(),
            source_received_at: "2026-10-20T12:00:00Z".to_owned(),
            generation: "g1".to_owned(),
            account_as_of_ms: 1000,
            order_complete: true,
            fill_complete: true,
            bill_complete: true,
            covered_ranges: serde_json::json!({}),
        };
        source
            .cursors
            .insert("receipt-1".to_owned(), cursor.clone());
        // Independent cursors at an identical receipt time count as one
        // coverage epoch, never an invented 0ms coverage gap.
        source.cursors.insert("receipt-2".to_owned(), cursor);
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
        c.execute(
            "INSERT INTO native_source_runs VALUES(?1,?2,?3,?4)",
            params![run_id, uid, &source.schema, &source.checksum_sha256],
        )
        .unwrap();
        for (key, cursor) in &source.cursors {
            let bytes = encode(cursor).unwrap();
            let at =
                crate::exchange_evidence::source_receipt_ms(&cursor.source_received_at).unwrap();
            c.execute(
                "INSERT INTO native_source_cursors VALUES(?1,?2,?3,?4,?5)",
                params![run_id, key, at, checksum(&bytes), bytes],
            )
            .unwrap();
        }
        let indexed = attest_indexed(&c, run_id, uid).unwrap();
        assert_eq!(indexed.checksum_sha256, source.checksum_sha256);
        assert_eq!(indexed.run_id, run_id);
        let cursor = indexed_cursor(&c, run_id, "receipt-1").unwrap().unwrap();
        assert_eq!(cursor.source, "OKX_REST_ACCOUNT_HISTORY");
        assert!(indexed_cursor(&c, run_id, "missing").unwrap().is_none());
        let mut visited = 0usize;
        visit_events(&c, run_id, uid, |_, _| {
            visited += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(visited, 0);
        assert!(matches!(
            visit_events(&c, run_id, "foreign-uid", |_, _| Ok(())),
            Err(ExecutionLedgerError::AccountScopeMismatch)
        ));
        let exact = summary(&c, run_id, uid).unwrap();
        assert_eq!(exact.largest_persisted_gap_ms, None);
        assert_eq!(
            serde_json::to_value(&exact).unwrap(),
            serde_json::to_value(source.summary()).unwrap()
        );
        assert!(matches!(
            summary(&c, run_id, "different-uid"),
            Err(ExecutionLedgerError::AccountScopeMismatch)
        ));
        c.execute_batch("DROP TRIGGER immutable_native_cursor_update;")
            .unwrap();
        c.execute(
            "UPDATE native_source_cursors SET payload_digest='forged' WHERE cursor_key='receipt-1'",
            [],
        )
        .unwrap();
        assert!(summary(&c, run_id, uid).is_err());
        assert!(attest_indexed(&c, run_id, uid).is_err());
    }

    #[test]
    fn additive_native_bill_witness_closes_financial_gap_without_rewriting_source() {
        let mut c = Connection::open_in_memory().unwrap();
        c.pragma_update(None, "foreign_keys", "ON").unwrap();
        c.execute_batch(include_str!("../../schema/account-ledger-v1.sql"))
            .unwrap();
        c.execute_batch(TABLES).unwrap();
        let mut sample = DurableExchangeEvidence {
            schema: crate::EXCHANGE_EVIDENCE_SCHEMA_V1.to_owned(),
            account_uid: "a".repeat(64),
            run_id: "native_additive_bill_001".to_owned(),
            events: BTreeMap::new(),
            cursors: BTreeMap::new(),
            checksum_sha256: String::new(),
        };
        let original = StoredExchangeEvent {
            payload: serde_json::json!({
                "source":"OKX_REST_BILL", "billId":"bill-19",
                "instId":"BTC-USDT-SWAP", "tradeId":null,
                "ccy":"USDT", "type":"2", "subType":"5",
                "balChg":null, "ts":2000
            }),
            source_intent_id: None,
            strategy_run_id: None,
            allocation: "UNALLOCATED".to_owned(),
        };
        let key = "bill:bill-19";
        sample.events.insert(key.to_owned(), original.clone());
        sample.seal().unwrap();
        {
            let tx = c.transaction().unwrap();
            assert!(append_window_delta(&tx, &sample).unwrap());
            tx.commit().unwrap();
        }
        let original_digest: String = c.query_row(
            "SELECT payload_digest FROM native_source_events
             WHERE run_id=?1 AND event_key=?2",
            params![&sample.run_id,key],
            |r| r.get(0),
        ).unwrap();
        let unresolved: i64 = c.query_row(
            "SELECT COUNT(*) FROM source_gaps
             WHERE gap_id='bill-financial-missing:bill-19'
             AND resolution_evidence_sha256 IS NULL",[],|r|r.get(0)
        ).unwrap();
        assert_eq!(unresolved, 1);
        let mut complete = original.clone();
        complete.payload["balChg"] = serde_json::json!("-0.25");
        let mut later = sample.clone();
        later.events.insert(key.to_owned(), complete.clone());
        later.seal().unwrap();
        {
            let tx = c.transaction().unwrap();
            assert!(append_window_delta(&tx, &later).unwrap());
            tx.commit().unwrap();
        }
        let preserved: String = c.query_row(
            "SELECT payload_digest FROM native_source_events
             WHERE run_id=?1 AND event_key=?2",
            params![&sample.run_id,key],
            |r| r.get(0),
        ).unwrap();
        assert_eq!(preserved, original_digest);
        let updated: (String, String) = c.query_row(
            "SELECT b.delta_decimal,g.resolution_evidence_sha256 FROM bills b
             JOIN source_gaps g ON g.gap_id='bill-financial-missing:bill-19'
             WHERE b.bill_id='bill-19'",[],|r|Ok((r.get(0)?,r.get(1)?))
        ).unwrap();
        assert_eq!(updated.0,"-0.25");
        assert_eq!(updated.1.len(),64);
        assert_ne!(updated.1,original_digest);
        let mut observed = Vec::new();
        visit_bill_events(&c,&sample.run_id,&sample.account_uid,|k,v|{
            observed.push((k.to_owned(),v.payload["balChg"].as_str().unwrap_or("").to_owned()));
            Ok(())
        }).unwrap();
        assert_eq!(observed, vec![(key.to_owned(),"-0.25".to_owned())]);
        let mut from_finance = Vec::new();
        visit_financial_kind(&c,&sample.run_id,&sample.account_uid,"BILL",|k,v|{
            from_finance.push((k.to_owned(),v.payload["balChg"].as_str().unwrap_or("").to_owned()));
            Ok(())
        }).unwrap();
        assert_eq!(from_finance, observed);
        audit_financial_index_inventory(&c).unwrap();
        assert!(attest_indexed(&c,&sample.run_id,&sample.account_uid).is_ok());
        {
            let tx = c.transaction().unwrap();
            assert!(!append_window_delta(&tx, &later).unwrap());
            tx.commit().unwrap();
        }
        let mut conflicting = later.clone();
        conflicting.events.get_mut(key).unwrap().payload["balChg"] =
            serde_json::json!("-0.5");
        conflicting.seal().unwrap();
        {
            let tx = c.transaction().unwrap();
            assert!(matches!(
                append_window_delta(&tx,&conflicting),
                Err(ExecutionLedgerError::ExchangeEvidenceConflict)
            ));
        }
    }

    #[test]
    fn native_source_ignores_legacy_cumulative_bytes_but_bounds_each_row() {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(TABLES).unwrap();
        let mut source = DurableExchangeEvidence {
            schema: crate::EXCHANGE_EVIDENCE_SCHEMA_V1.to_owned(),
            account_uid: "uid-byte-bound".to_owned(),
            run_id: "run-byte-bound".to_owned(),
            events: BTreeMap::new(),
            cursors: BTreeMap::new(),
            checksum_sha256: String::new(),
        };
        let proof_of = |s: &DurableExchangeEvidence| {
            checksum(
                &serde_json::to_vec(&(&s.schema, &s.account_uid, &s.run_id, &s.events, &s.cursors))
                    .unwrap(),
            )
        };
        source.checksum_sha256 = proof_of(&source);
        let tuple_len = serde_json::to_vec(&(
            &source.schema,
            &source.account_uid,
            &source.run_id,
            &source.events,
            &source.cursors,
        ))
        .unwrap()
        .len();
        assert_eq!(
            tuple_len
                + source_struct_overhead(
                    &source.schema,
                    &source.account_uid,
                    &source.run_id,
                    &source.checksum_sha256,
                )
                .unwrap(),
            serde_json::to_vec(&source).unwrap().len(),
        );
        c.execute(
            "INSERT INTO native_source_runs VALUES(?1,?2,?3,?4)",
            params![
                &source.run_id,
                &source.account_uid,
                &source.schema,
                &source.checksum_sha256
            ],
        )
        .unwrap();
        assert!(attest_indexed(&c, &source.run_id, &source.account_uid).is_ok());

        // v14 has a cumulative document limit, while the SQLite-native
        // historical account ledger must scale beyond it. Each row
        // stays individually bounded and the full source digest is
        // still verified by the streaming authenticator.
        for i in 0..2 {
            let event = StoredExchangeEvent {
                payload: serde_json::json!({
                    "blob": "x".repeat(crate::exchange_evidence::MAX_BYTES / 2 + 1024)
                }),
                source_intent_id: None,
                strategy_run_id: None,
                allocation: "UNALLOCATED".to_owned(),
            };
            let key = format!("large-{i}");
            source.events.insert(key.clone(), event.clone());
            let bytes = encode(&event).unwrap();
            assert!(bytes.len() < crate::exchange_evidence::MAX_BYTES);
            c.execute(
                "INSERT INTO native_source_events VALUES(?1,?2,?3,?4)",
                params![&source.run_id, key, checksum(&bytes), bytes],
            )
            .unwrap();
        }
        source.checksum_sha256 = proof_of(&source);
        assert!(source.validate().is_err());
        c.execute(
            "UPDATE native_source_runs SET proof_digest=?1 WHERE run_id=?2",
            params![&source.checksum_sha256, &source.run_id],
        )
        .unwrap();
        let authenticated = attest_indexed(&c, &source.run_id, &source.account_uid).unwrap();
        assert_eq!(authenticated.checksum_sha256, source.checksum_sha256);
        assert_eq!(authenticated.run_id, source.run_id);
    }

    #[test]
    fn streaming_source_identity_cannot_accept_empty_legacy_owner() {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(TABLES).unwrap();
        c.execute(
            "INSERT INTO native_source_runs VALUES('','',?1,?2)",
            params![crate::EXCHANGE_EVIDENCE_SCHEMA_V1, "0".repeat(64)],
        )
        .unwrap();
        assert!(matches!(
            attest_indexed(&c, "", ""),
            Err(ExecutionLedgerError::Corrupt(
                "indexed exchange source identity missing"
            ))
        ));
    }

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

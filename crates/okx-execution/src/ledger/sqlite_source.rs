//! Indexed and immutable exchange source evidence in the SAME account SQLite transaction.
//! Exchange payload bytes are encoded as MessagePack; original legacy JSON evidence
//! stays inside the separately sealed and unmodified pre-cutover v14 archive.
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
            if earlier != *event
                && !DurableExchangeEvidence::same_legacy_bill_source(&earlier, event)
            {
                return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
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
    if events > crate::exchange_evidence::MAX_EVENTS {
        return Err(ExecutionLedgerError::ExchangeEvidenceCapacity);
    }
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
        if cursors > crate::exchange_evidence::MAX_CURSORS {
            return Err(ExecutionLedgerError::ExchangeEvidenceCapacity);
        }
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
        remaining_source_cursors: crate::exchange_evidence::MAX_CURSORS - cursors,
        remaining_events: crate::exchange_evidence::MAX_EVENTS - events,
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
        if count > crate::exchange_evidence::MAX_EVENTS {
            return Err(ExecutionLedgerError::ExchangeEvidenceCapacity);
        }
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
        if count > crate::exchange_evidence::MAX_CURSORS {
            return Err(ExecutionLedgerError::ExchangeEvidenceCapacity);
        }
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
    if serialized_bytes > crate::exchange_evidence::MAX_BYTES {
        return Err(ExecutionLedgerError::ExchangeEvidenceCapacity);
    }
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
    let mut count = 0usize;
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
                || event.payload.get("source").and_then(serde_json::Value::as_str)
                    != Some("OKX_REST_BILL"))
        {
            return Err(ExecutionLedgerError::Corrupt("indexed bill source kind mismatch"));
        }
        count += 1;
        if count > crate::exchange_evidence::MAX_EVENTS {
            return Err(ExecutionLedgerError::ExchangeEvidenceCapacity);
        }
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
        second
            .cursors
            .insert("receipt-2".to_owned(), cursor("2026-10-20T12:05:00Z"));
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
        let mut bills = 0usize;
        visit_bill_events(&c, &empty.run_id, &empty.account_uid, |_, _| {
            bills += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(bills, 0, "order events are excluded by the bill key index");
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
    fn indexed_source_exact_legacy_struct_byte_limit() {
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

        // Individually bounded BLOBs are allowed to be read, but together
        // exceed the same legacy WHOLE-source serialized JSON byte limit.
        // This proves we enforce cumulative size, not only row size.
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
        assert!(matches!(
            attest_indexed(&c, &source.run_id, &source.account_uid),
            Err(ExecutionLedgerError::ExchangeEvidenceCapacity)
        ));
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

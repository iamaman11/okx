//! Typed account financial fact projections from immutable native OKX REST evidence.
//! Projection is transactional and rebuildable from the single source ledger.
//! UNKNOWN legacy cash changes become explicit material source gaps, never zero PnL.
use super::sqlite_store::{
    INDEXED_INTENT_COLUMNS, StoredState, checksum, connect_indexed_read, decode, encode, io,
    require_finance_read_scope, verified_indexed_entry,
};
use super::*;
use crate::exchange_evidence::StoredExchangeEvent;
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;

fn field<'a>(v: &'a Value, name: &str) -> Result<&'a str, ExecutionLedgerError> {
    v.get(name)
        .and_then(Value::as_str)
        .filter(|v| !v.trim().is_empty())
        .ok_or(ExecutionLedgerError::Corrupt(
            "missing native financial field",
        ))
}

fn nullable<'a>(v: &'a Value, name: &str) -> Result<Option<&'a str>, ExecutionLedgerError> {
    match v.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.as_str())),
        _ => Err(ExecutionLedgerError::Corrupt(
            "invalid native optional field",
        )),
    }
}

fn amount<'a>(v: &'a Value, name: &str) -> Result<&'a str, ExecutionLedgerError> {
    let s = field(v, name)?;
    if Decimal::from_str(s).is_err() {
        return Err(ExecutionLedgerError::Corrupt(
            "invalid exact financial decimal",
        ));
    }
    Ok(s)
}

fn optional_amount<'a>(v: &'a Value, name: &str) -> Result<Option<&'a str>, ExecutionLedgerError> {
    let opt = nullable(v, name)?;
    if opt.is_some_and(|s| Decimal::from_str(s).is_err()) {
        return Err(ExecutionLedgerError::Corrupt(
            "invalid optional exact financial decimal",
        ));
    }
    Ok(opt)
}

fn ms(v: &Value) -> Result<i64, ExecutionLedgerError> {
    let value = v.get("ts").ok_or(ExecutionLedgerError::Corrupt(
        "missing exchange event timestamp",
    ))?;
    let n = match value {
        Value::String(s) => s.parse::<u64>().ok(),
        Value::Number(n) => n.as_u64(),
        _ => None,
    }
    .and_then(|n| i64::try_from(n).ok());
    n.filter(|n| *n > 0).ok_or(ExecutionLedgerError::Corrupt(
        "invalid exchange event timestamp",
    ))
}

fn assert_evidence_object(
    tx: &Transaction<'_>,
    run_id: &str,
    event_key: &str,
    original: &StoredExchangeEvent,
) -> Result<String, ExecutionLedgerError> {
    let bytes = encode(original)?;
    let digest = checksum(&bytes);
    // codec identifies a same-DB logical reference; not a claim that a
    // separate filesystem object has been created.
    let locator = format!("native_source_events/{run_id}/{event_key}");
    tx.execute(
        "INSERT INTO evidence_objects
          (evidence_sha256,evidence_kind,codec,relative_path,size_bytes,native_source_time_ms)
         VALUES(?1,'AUTHENTICATED_EXCHANGE_EVENT','sqlite-msgpack',?2,?3,NULL)
         ON CONFLICT(evidence_sha256) DO NOTHING",
        params![&digest, &locator, bytes.len() as i64],
    )
    .map_err(io)?;
    let (kind, codec, len): (String, String, i64) = tx
        .query_row(
            "SELECT evidence_kind,codec,size_bytes FROM evidence_objects WHERE evidence_sha256=?1",
            params![&digest],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(io)?;
    if kind != "AUTHENTICATED_EXCHANGE_EVENT"
        || codec != "sqlite-msgpack"
        || len != bytes.len() as i64
    {
        return Err(ExecutionLedgerError::Corrupt(
            "financial evidence locator collision",
        ));
    }
    Ok(digest)
}

/// Transactional current-order projection from the SAME immutable source
/// event that proves exchange order identity. Historical order transitions
/// remain sealed in native_source_events; this row is only an indexed view.
///
/// A later partial REST window must not move exchange order state backwards.
/// Equal-timestamp disagreements and reused order IDs fail the whole WAL
/// transaction, including fills, bills, cursors and STOP metadata.
fn insert_order(
    tx: &Transaction<'_>,
    event: &StoredExchangeEvent,
) -> Result<(), ExecutionLedgerError> {
    let v = &event.payload;
    let order_id = field(v, "ordId")?;
    let instrument = field(v, "instId")?;
    // Manual and external OKX orders can legitimately have an empty clOrdId.
    // Only a ledger-owned order must prove its immutable client identity.
    let client_order_id = nullable(v, "clOrdId")?.unwrap_or("");
    if event.source_intent_id.is_some() && client_order_id.is_empty() {
        return Err(ExecutionLedgerError::Corrupt(
            "managed native order missing client identity",
        ));
    }
    let state = field(v, "state")?;
    let update_at_ms = field(v, "uTime")?
        .parse::<i64>()
        .ok()
        .filter(|ms| *ms > 0)
        .ok_or(ExecutionLedgerError::Corrupt(
            "invalid native order timestamp",
        ))?;
    let current: Option<(Option<String>, String, String, String, i64)> = tx
        .query_row(
            "SELECT intent_id,instrument_id,client_order_id,status,last_exchange_update_ms
             FROM exchange_orders WHERE venue_order_id=?1",
            params![order_id],
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
        .optional()
        .map_err(io)?;
    if let Some((prior_intent, prior_instrument, prior_client, prior_state, prior_time)) = current {
        if prior_intent.as_deref() != event.source_intent_id.as_deref()
            || prior_instrument != instrument
            || prior_client != client_order_id
            || (prior_time == update_at_ms && prior_state != state)
        {
            return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
        }
        if update_at_ms > prior_time {
            let changed = tx
                .execute(
                    "UPDATE exchange_orders SET status=?1,last_exchange_update_ms=?2
                     WHERE venue_order_id=?3 AND last_exchange_update_ms=?4",
                    params![state, update_at_ms, order_id, prior_time],
                )
                .map_err(io)?;
            if changed != 1 {
                return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
            }
        }
    } else {
        tx.execute(
            "INSERT INTO exchange_orders
             (venue_order_id,intent_id,instrument_id,client_order_id,status,last_exchange_update_ms)
             VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                order_id,
                event.source_intent_id.as_deref(),
                instrument,
                client_order_id,
                state,
                update_at_ms,
            ],
        )
        .map_err(io)?;
    }
    Ok(())
}

struct IndexedFill {
    order_id: Option<String>,
    intent_id: Option<String>,
    exchange_time_ms: i64,
    side: String,
    position_side: String,
    price: String,
    size: String,
    fee: Option<String>,
    fee_currency: Option<String>,
}

fn insert_fill(
    tx: &Transaction<'_>,
    v: &Value,
    event: &StoredExchangeEvent,
    proof: &str,
) -> Result<(), ExecutionLedgerError> {
    let inst = field(v, "instId")?;
    let trade = field(v, "tradeId")?;
    let side = field(v, "side")?;
    let pos = field(v, "posSide")?;
    let price = amount(v, "fillPx")?;
    let contracts = amount(v, "fillSz")?;
    if Decimal::from_str(price)
        .ok()
        .is_none_or(|d| d <= Decimal::ZERO)
        || Decimal::from_str(contracts)
            .ok()
            .is_none_or(|d| d <= Decimal::ZERO)
    {
        return Err(ExecutionLedgerError::Corrupt("nonpositive fill price/size"));
    }
    let fee = optional_amount(v, "fee")?;
    let ccy = nullable(v, "feeCcy")?;
    let ord = nullable(v, "ordId")?;
    let intent = event.source_intent_id.as_deref();
    let timestamp = ms(v)?;
    tx.execute(
        "INSERT INTO fills
         (instrument_id,trade_id,venue_order_id,intent_id,exchange_time_ms,side,
          position_side,price_decimal,contracts_decimal,fee_decimal,fee_currency,source_evidence_sha256)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)
         ON CONFLICT(instrument_id,trade_id) DO NOTHING",
        params![inst,trade,ord,intent,timestamp,side,pos,price,contracts,fee,ccy,proof],
    ).map_err(io)?;
    let stored: IndexedFill = tx
        .query_row(
            "SELECT venue_order_id,intent_id,exchange_time_ms,side,position_side,
                    price_decimal,contracts_decimal,fee_decimal,fee_currency
             FROM fills WHERE instrument_id=?1 AND trade_id=?2",
            params![inst, trade],
            |r| {
                Ok(IndexedFill {
                    order_id: r.get(0)?,
                    intent_id: r.get(1)?,
                    exchange_time_ms: r.get(2)?,
                    side: r.get(3)?,
                    position_side: r.get(4)?,
                    price: r.get(5)?,
                    size: r.get(6)?,
                    fee: r.get(7)?,
                    fee_currency: r.get(8)?,
                })
            },
        )
        .map_err(io)?;
    if stored.order_id.as_deref() != ord
        || stored.intent_id.as_deref() != intent
        || stored.exchange_time_ms != timestamp
        || stored.side != side
        || stored.position_side != pos
        || stored.price != price
        || stored.size != contracts
        || stored.fee.as_deref() != fee
        || stored.fee_currency.as_deref() != ccy
    {
        return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
    }
    Ok(())
}

struct IndexedFinancialGap {
    stream: String,
    scope: String,
    from: i64,
    through: i64,
    materiality: String,
    reason: String,
    resolution: Option<String>,
}

struct IndexedBillFact {
    instrument: Option<String>,
    trade: Option<String>,
    timestamp: i64,
    currency: String,
    kind: String,
    delta: String,
}

/// A financial gap is resolved only by a later authenticated, exact bill
/// fact with the SAME native bill identity. Both immutable source proofs
/// remain present. A point receipt never grants continuous-history coverage.
fn match_bill_gap(
    tx: &Transaction<'_>,
    bill: &str,
    timestamp: i64,
    resolution: Option<&str>,
) -> Result<(), ExecutionLedgerError> {
    let gap_id = format!("bill-financial-missing:{bill}");
    let prior: Option<IndexedFinancialGap> = tx
        .query_row(
            "SELECT stream_kind,scope_key,from_ms,through_ms,materiality,reason_code,
                    resolution_evidence_sha256
             FROM source_gaps WHERE gap_id=?1",
            params![&gap_id],
            |row| {
                Ok(IndexedFinancialGap {
                    stream: row.get(0)?,
                    scope: row.get(1)?,
                    from: row.get(2)?,
                    through: row.get(3)?,
                    materiality: row.get(4)?,
                    reason: row.get(5)?,
                    resolution: row.get(6)?,
                })
            },
        )
        .optional()
        .map_err(io)?;
    let Some(prior) = prior else {
        return Ok(());
    };
    if prior.stream != "BILLS"
        || prior.scope != bill
        || prior.from != timestamp
        || prior.through
            != timestamp
                .checked_add(1)
                .ok_or(ExecutionLedgerError::Corrupt("bill time overflow"))?
        || prior.materiality != "FINANCIAL"
        || prior.reason != "MISSING_EXACT_BALANCE_CHANGE"
    {
        return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
    }
    if let Some(proof) = resolution {
        // Resolution is one-way. A duplicate later receipt with the same
        // exact cash fact does not replace the first resolving evidence.
        if prior.resolution.is_none() {
            let changed = tx
                .execute(
                    "UPDATE source_gaps SET resolution_evidence_sha256=?1
                     WHERE gap_id=?2 AND resolution_evidence_sha256 IS NULL",
                    params![proof, gap_id],
                )
                .map_err(io)?;
            if changed != 1 {
                return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
            }
        }
    } else if prior.resolution.is_some() {
        // A later incomplete REST observation cannot revoke a proven exact
        // bill or resurrect an already resolved financial gap.
        return Ok(());
    }
    Ok(())
}

fn insert_bill(tx: &Transaction<'_>, v: &Value, proof: &str) -> Result<(), ExecutionLedgerError> {
    let bill = field(v, "billId")?;
    let timestamp = ms(v)?;
    let currency = field(v, "ccy")?;
    let inst = nullable(v, "instId")?;
    let trade = nullable(v, "tradeId")?;
    let bill_type = nullable(v, "type")?.unwrap_or("UNKNOWN_LEGACY");
    let subtype = nullable(v, "subType")?.unwrap_or("UNKNOWN_LEGACY");
    let kind = format!("{bill_type}/{subtype}");
    let delta = optional_amount(v, "balChg")?;
    let existing: Option<IndexedBillFact> = tx
        .query_row(
            "SELECT instrument_id,trade_id,exchange_time_ms,currency,bill_kind,delta_decimal
             FROM bills WHERE bill_id=?1",
            params![bill],
            |r| {
                Ok(IndexedBillFact {
                    instrument: r.get(0)?,
                    trade: r.get(1)?,
                    timestamp: r.get(2)?,
                    currency: r.get(3)?,
                    kind: r.get(4)?,
                    delta: r.get(5)?,
                })
            },
        )
        .optional()
        .map_err(io)?;
    // An incomplete duplicate must not reopen a financial gap after an
    // earlier complete observation. Reused bill IDs with changed identity
    // always fail closed, even when balChg is missing.
    if let Some(prior) = existing.as_ref() {
        if prior.instrument.as_deref() != inst
            || prior.trade.as_deref() != trade
            || prior.timestamp != timestamp
            || prior.currency != currency
            || prior.kind != kind
            || delta.is_some_and(|exact| prior.delta != exact)
        {
            return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
        }
        if delta.is_none() {
            match_bill_gap(tx, bill, timestamp, None)?;
            return Ok(());
        }
    }
    let Some(exact) = delta else {
        let gap_id = format!("bill-financial-missing:{bill}");
        let through = timestamp
            .checked_add(1)
            .ok_or(ExecutionLedgerError::Corrupt("bill time overflow"))?;
        tx.execute(
            "INSERT INTO source_gaps
             (gap_id,stream_kind,scope_key,from_ms,through_ms,materiality,reason_code,
              opening_evidence_sha256,resolution_evidence_sha256)
             VALUES(?1,'BILLS',?2,?3,?4,'FINANCIAL','MISSING_EXACT_BALANCE_CHANGE',?5,NULL)
             ON CONFLICT(gap_id) DO NOTHING",
            params![gap_id, bill, timestamp, through, proof],
        )
        .map_err(io)?;
        match_bill_gap(tx, bill, timestamp, None)?;
        return Ok(());
    };
    tx.execute(
        "INSERT INTO bills
         (bill_id,instrument_id,trade_id,exchange_time_ms,currency,bill_kind,delta_decimal,source_evidence_sha256)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8)
         ON CONFLICT(bill_id) DO NOTHING",
        params![bill,inst,trade,timestamp,currency,kind,exact,proof],
    ).map_err(io)?;
    let stored: IndexedBillFact = tx
        .query_row(
            "SELECT instrument_id,trade_id,exchange_time_ms,currency,bill_kind,delta_decimal
             FROM bills WHERE bill_id=?1",
            params![bill],
            |r| {
                Ok(IndexedBillFact {
                    instrument: r.get(0)?,
                    trade: r.get(1)?,
                    timestamp: r.get(2)?,
                    currency: r.get(3)?,
                    kind: r.get(4)?,
                    delta: r.get(5)?,
                })
            },
        )
        .map_err(io)?;
    if stored.instrument.as_deref() != inst
        || stored.trade.as_deref() != trade
        || stored.timestamp != timestamp
        || stored.currency != currency
        || stored.kind != kind
        || stored.delta != exact
    {
        return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
    }
    match_bill_gap(tx, bill, timestamp, Some(proof))?;
    Ok(())
}

pub(super) fn persist_event(
    tx: &Transaction<'_>,
    run_id: &str,
    event_key: &str,
    event: &StoredExchangeEvent,
) -> Result<(), ExecutionLedgerError> {
    match event.payload.get("source").and_then(Value::as_str) {
        Some("OKX_REST_FILL") => {
            let evidence = assert_evidence_object(tx, run_id, event_key, event)?;
            insert_fill(tx, &event.payload, event, &evidence)?;
        }
        Some("OKX_REST_BILL") => {
            let evidence = assert_evidence_object(tx, run_id, event_key, event)?;
            insert_bill(tx, &event.payload, &evidence)?;
        }
        Some("OKX_REST_ORDER") => {
            assert_evidence_object(tx, run_id, event_key, event)?;
            insert_order(tx, event)?;
        }
        _ => {
            return Err(ExecutionLedgerError::Corrupt(
                "unknown native financial source kind",
            ));
        }
    }
    Ok(())
}

/// Authenticated snapshot of observed account CASH movements, never strategy NAV/PnL.
/// A bill's balance delta may represent a transfer, fee, funding, or trade; it
/// MUST NOT be interpreted as strategy return without independent attribution.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct AccountCashflowBucket {
    pub currency: String,
    pub bill_kind: String,
    pub rows: u64,
    pub exact_balance_delta: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct AccountFinanceWindow {
    pub schema: &'static str,
    pub account_uid_fingerprint: String,
    pub from_ms: u64,
    pub through_ms: u64,
    pub scope: &'static str,
    pub buckets: Vec<AccountCashflowBucket>,
    pub fill_rows: u64,
    pub bill_rows: u64,
    pub financial_gaps: u64,
    pub execution_gaps: u64,
    pub unclassified_gaps: u64,
    pub bill_source_receipts: u64,
    pub fill_source_receipts: u64,
    pub order_source_receipts: u64,
    pub financial_completeness: &'static str,
}

/// Pin every cashflow, fill, gap and source-receipt query to one SQLite WAL
/// read snapshot. Separate autocommit SELECTs can otherwise observe different
/// commits and report internally inconsistent financial facts.
fn begin_finance_snapshot(
    connection: &mut rusqlite::Connection,
) -> Result<Transaction<'_>, ExecutionLedgerError> {
    connection
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .map_err(io)
}

/// One read-only, bounded-by-request financial view. Returns observed money
/// movement and explicit uncertainty; it never authorizes an exchange POST.
/// The caller supplies the natively verified UID, not a user-facing selector.
/// One WAL snapshot supplies the exact NAV receipt and all native bill facts.
/// Unlike legacy NAV this never creates a historical DurableExchangeEvidence
/// or an all-run map of intents. Every source row and referenced intent is
/// authenticated, and the canonical source hash matches the financial report.
/// An unattributed bill can never silently enter the run's daily loss.
pub(super) fn nav_cursor_and_daily_loss(
    path: &Path,
    run: &StrategyCapitalReservation,
    report: &StrategyFinancialPeriod,
    day: u64,
    as_of: u64,
) -> Result<
    Option<(
        String,
        crate::exchange_evidence::ExchangeCoverageCursor,
        u64,
        Decimal,
    )>,
    ExecutionLedgerError,
> {
    let c = connect_indexed_read(path)?;
    c.execute_batch("BEGIN DEFERRED TRANSACTION").map_err(io)?;
    let (uid, digest, bytes): (String, String, Vec<u8>) = c
        .query_row(
            "SELECT uid,digest,bytes FROM ledger_state WHERE key=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(io)?;
    require_finance_read_scope(&c, &uid)?;
    if checksum(&bytes) != digest {
        return Err(ExecutionLedgerError::Corrupt("native NAV state digest"));
    }
    let state: StoredState = decode(&bytes)?;
    if state.version != 1
        || uid != run.account_uid_fingerprint
        || state
            .reservation
            .as_ref()
            .is_none_or(|active| active != run)
        || state.source_run_id.as_deref() != Some(run.allocation_id.as_str())
        || report.run_id != run.allocation_id
    {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    let source = super::sqlite_source::attest_indexed(&c, &run.allocation_id, &uid)?;
    if source.checksum_sha256 != report.evidence_sha256 {
        return Ok(None);
    }
    let cursor: Option<(String, i64)> = c
        .query_row(
            "SELECT cursor_key,receipt_ms FROM native_source_cursors
             WHERE run_id=?1 AND receipt_ms<=?2
             ORDER BY receipt_ms DESC,cursor_key DESC LIMIT 1",
            params![
                run.allocation_id,
                i64::try_from(as_of).map_err(|_| ExecutionLedgerError::InvalidTimestamp)?
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(io)?;
    let Some((key, receipt_ms)) = cursor else {
        return Err(ExecutionLedgerError::ExchangeSourceIncomplete);
    };
    let receipt_ms = u64::try_from(receipt_ms)
        .map_err(|_| ExecutionLedgerError::Corrupt("invalid NAV receipt index"))?;
    let proof = super::sqlite_source::indexed_cursor(&c, &run.allocation_id, &key)?
        .ok_or(ExecutionLedgerError::ExchangeSourceIncomplete)?;
    if crate::exchange_evidence::source_receipt_ms(&proof.source_received_at) != Some(receipt_ms) {
        return Err(ExecutionLedgerError::Corrupt(
            "native NAV cursor index mismatch",
        ));
    }
    if as_of - receipt_ms > crate::exchange_evidence::MAX_SOURCE_COVERAGE_GAP_MS {
        return Ok(None);
    }

    let sql = format!("{INDEXED_INTENT_COLUMNS} WHERE e.intent_id=?1");
    let mut stmt = c.prepare(&sql).map_err(io)?;
    let mut loss = crate::strategy_finance::DailyStrategyLossAccumulator::default();
    super::sqlite_source::visit_bill_events(&c, &run.allocation_id, &uid, |_, event| {
        if event.payload["source"] != "OKX_REST_BILL" {
            return Ok(());
        }
        let Some(intent) = event.source_intent_id.as_deref() else {
            return Ok(());
        };
        let entry = {
            let mut rows = stmt.query(params![intent]).map_err(io)?;
            let entry = rows
                .next()
                .map_err(io)?
                .map(|row| verified_indexed_entry(row, &uid))
                .transpose()?;
            if rows.next().map_err(io)?.is_some() {
                return Err(ExecutionLedgerError::Corrupt(
                    "duplicate NAV bill intent identity",
                ));
            }
            entry
        };
        let Some(entry) = entry else {
            return Ok(());
        };
        if entry.created_at_ms <= run.started_at_ms
            || entry.record.plan.account_uid_fingerprint != uid
        {
            return Ok(());
        }
        let direct_run = event.strategy_run_id.as_deref() == Some(run.allocation_id.as_str())
            && entry.record.strategy_run_id.as_deref() == Some(run.allocation_id.as_str());
        let run_reducer = entry.record.plan.action.is_risk_reducing()
            && event.strategy_run_id.is_none()
            && entry.record.strategy_run_id.is_none();
        if !direct_run && !run_reducer {
            return Ok(());
        }
        loss.accept(&event.payload, run, day, as_of)?;
        Ok(())
    })?;
    let Some(gross_loss) = loss.finish(run, report, day)? else {
        return Ok(None);
    };
    Ok(Some((key, proof, receipt_ms, gross_loss)))
}

pub(super) fn finance_window(
    path: &Path,
    expected_uid: &str,
    from_ms: u64,
    through_ms: u64,
) -> Result<AccountFinanceWindow, ExecutionLedgerError> {
    if expected_uid.is_empty() || from_ms == 0 || through_ms <= from_ms {
        return Err(ExecutionLedgerError::Corrupt(
            "invalid account finance window",
        ));
    }
    // Same validated SQLite connection as indexed intent reads, with one
    // consistent WAL snapshot covering account identity and every fact.
    let mut connection = super::sqlite_store::open_finance_read(path)?;
    let snapshot = begin_finance_snapshot(&mut connection)?;
    super::sqlite_store::require_finance_read_scope(&snapshot, expected_uid)?;
    let first = i64::try_from(from_ms).map_err(|_| ExecutionLedgerError::InvalidTimestamp)?;
    let end = i64::try_from(through_ms).map_err(|_| ExecutionLedgerError::InvalidTimestamp)?;
    let mut accum = BTreeMap::<(String, String), (u64, Decimal)>::new();
    {
        let mut stmt = snapshot
            .prepare(
                "SELECT bill_id,currency,bill_kind,delta_decimal FROM bills
             WHERE exchange_time_ms>=?1 AND exchange_time_ms<?2
             ORDER BY currency,bill_kind,bill_id",
            )
            .map_err(io)?;
        let mut rows = stmt.query(params![first, end]).map_err(io)?;
        while let Some(row) = rows.next().map_err(io)? {
            let ccy: String = row.get(1).map_err(io)?;
            let kind: String = row.get(2).map_err(io)?;
            let decimal: String = row.get(3).map_err(io)?;
            let value = Decimal::from_str(&decimal).map_err(io)?;
            let entry = accum.entry((ccy, kind)).or_insert((0, Decimal::ZERO));
            entry.0 = entry
                .0
                .checked_add(1)
                .ok_or(ExecutionLedgerError::Corrupt("bill count overflow"))?;
            entry.1 = entry
                .1
                .checked_add(value)
                .ok_or(ExecutionLedgerError::Corrupt(
                    "account cashflow decimal overflow",
                ))?;
        }
    }
    let buckets = accum
        .into_iter()
        .map(
            |((currency, bill_kind), (rows, value))| AccountCashflowBucket {
                currency,
                bill_kind,
                rows,
                exact_balance_delta: value.to_string(),
            },
        )
        .collect::<Vec<_>>();
    let count = |table: &'static str| -> Result<u64, ExecutionLedgerError> {
        // SQL constants only, not interpolated user input.
        let query = match table {
            "fills" => {
                "SELECT COUNT(*) FROM fills WHERE exchange_time_ms>=?1 AND exchange_time_ms<?2"
            }
            "bills" => {
                "SELECT COUNT(*) FROM bills WHERE exchange_time_ms>=?1 AND exchange_time_ms<?2"
            }
            _ => {
                return Err(ExecutionLedgerError::Corrupt(
                    "unknown account finance table",
                ));
            }
        };
        let n: i64 = snapshot
            .query_row(query, params![first, end], |r| r.get(0))
            .map_err(io)?;
        u64::try_from(n).map_err(|_| ExecutionLedgerError::Corrupt("negative finance count"))
    };
    let fill_rows = count("fills")?;
    let bill_rows = count("bills")?;
    let gap_count = |materiality: &str| -> Result<u64, ExecutionLedgerError> {
        let n: i64 = snapshot
            .query_row(
                "SELECT COUNT(*) FROM source_gaps
             WHERE materiality=?1 AND from_ms<?2 AND through_ms>?3
               AND resolution_evidence_sha256 IS NULL",
                params![materiality, end, first],
                |r| r.get(0),
            )
            .map_err(io)?;
        u64::try_from(n).map_err(|_| ExecutionLedgerError::Corrupt("negative financial gap count"))
    };
    let financial_gaps = gap_count("FINANCIAL")?;
    let execution_gaps = gap_count("EXECUTION_ONLY")?;
    let unclassified_gaps = gap_count("UNKNOWN")? + gap_count("EXPOSURE")?;
    let receipt_count = |stream: &str| -> Result<u64, ExecutionLedgerError> {
        // A source receipt is proof of ONE authenticated observation, not
        // necessarily of continuous historical coverage across the window.
        let n: i64 = snapshot
            .query_row(
                "SELECT COUNT(*) FROM source_receipts
             WHERE stream_kind=?1 AND scope_key=?2
               AND received_at_ms>=?3 AND received_at_ms<?4
               AND completeness='COMPLETE'",
                params![stream, expected_uid, first, end],
                |r| r.get(0),
            )
            .map_err(io)?;
        u64::try_from(n).map_err(|_| ExecutionLedgerError::Corrupt("negative receipt count"))
    };
    let bill_source_receipts = receipt_count("BILLS")?;
    let fill_source_receipts = receipt_count("FILLS")?;
    let order_source_receipts = receipt_count("ORDERS")?;
    let completeness = if financial_gaps > 0 || unclassified_gaps > 0 {
        "INCOMPLETE"
    } else {
        // No closed interval proof is encoded by the point receipts; absence
        // of detected gaps is NOT evidence of complete strategy finance.
        "UNVERIFIED_WITHOUT_CONTIGUOUS_SOURCE_PROOF"
    };
    let window = AccountFinanceWindow {
        schema: "okx.account-finance-window/v1",
        account_uid_fingerprint: expected_uid.to_owned(),
        from_ms,
        through_ms,
        scope: "ACCOUNT_OBSERVED_CASH_MOVEMENTS_NOT_STRATEGY_PNL",
        buckets,
        fill_rows,
        bill_rows,
        financial_gaps,
        execution_gaps,
        unclassified_gaps,
        bill_source_receipts,
        fill_source_receipts,
        order_source_receipts,
        financial_completeness: completeness,
    };
    snapshot.commit().map_err(io)?;
    Ok(window)
}

/// Separate source coverage facts per stream. Every inserted interval is a
/// POINT observation; it never fabricates continuity across missing history.
pub(super) fn persist_source_receipt(
    tx: &Transaction<'_>,
    account_uid: &str,
    run_id: &str,
    cursor_key: &str,
    cursor: &crate::exchange_evidence::ExchangeCoverageCursor,
) -> Result<(), ExecutionLedgerError> {
    let at = crate::exchange_evidence::source_receipt_ms(&cursor.source_received_at)
        .and_then(|n| i64::try_from(n).ok())
        .ok_or(ExecutionLedgerError::Corrupt(
            "invalid source receipt timestamp",
        ))?;
    if account_uid.is_empty() {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    let body = encode(cursor)?;
    let digest = checksum(&body);
    let locator = format!("native_source_cursors/{run_id}/{cursor_key}");
    tx.execute(
        "INSERT INTO evidence_objects
         (evidence_sha256,evidence_kind,codec,relative_path,size_bytes,native_source_time_ms)
         VALUES(?1,'AUTHENTICATED_SOURCE_RECEIPT','sqlite-msgpack',?2,?3,?4)
         ON CONFLICT(evidence_sha256) DO NOTHING",
        params![&digest, &locator, body.len() as i64, at],
    )
    .map_err(io)?;
    let (kind, codec, len, observed): (String, String, i64, Option<i64>) = tx
        .query_row(
            "SELECT evidence_kind,codec,size_bytes,native_source_time_ms
         FROM evidence_objects WHERE evidence_sha256=?1",
            params![&digest],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .map_err(io)?;
    if kind != "AUTHENTICATED_SOURCE_RECEIPT"
        || codec != "sqlite-msgpack"
        || len != body.len() as i64
        || observed != Some(at)
    {
        return Err(ExecutionLedgerError::Corrupt(
            "source receipt evidence collision",
        ));
    }
    for (kind, complete) in [
        ("ORDERS", cursor.order_complete),
        ("FILLS", cursor.fill_complete),
        ("BILLS", cursor.bill_complete),
    ] {
        let key = checksum(&encode(&("okx.source-point/v1", kind, account_uid, &body))?);
        tx.execute(
            "INSERT INTO source_receipts
             (receipt_sha256,stream_kind,scope_key,interval_start_ms,interval_end_ms,
              received_at_ms,completeness,evidence_sha256)
             VALUES(?1,?2,?3,?4,?4,?4,?5,?6)
             ON CONFLICT(receipt_sha256) DO NOTHING",
            params![
                key,
                kind,
                account_uid,
                at,
                if complete { "COMPLETE" } else { "PARTIAL" },
                &digest
            ],
        )
        .map_err(io)?;
    }
    Ok(())
}

/// Native financial source is one pinned account WAL read transaction.
/// Each intent, order and bill is looked up by exact immutable identity.
pub(super) struct NativeFinanceReader {
    connection: rusqlite::Connection,
    run: StrategyCapitalReservation,
    identity: crate::strategy_lifecycle::SourceIdentity,
}
impl NativeFinanceReader {
    pub(super) fn open(
        path: &Path,
        run: &StrategyCapitalReservation,
    ) -> Result<Self, ExecutionLedgerError> {
        let connection = connect_indexed_read(path)?;
        connection
            .execute_batch("BEGIN DEFERRED TRANSACTION")
            .map_err(io)?;
        require_finance_read_scope(&connection, &run.account_uid_fingerprint)?;
        let (uid, digest, payload): (String, String, Vec<u8>) = connection
            .query_row(
                "SELECT uid,digest,bytes FROM ledger_state WHERE key=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(io)?;
        if uid != run.account_uid_fingerprint || checksum(&payload) != digest {
            return Err(ExecutionLedgerError::AccountScopeMismatch);
        }
        let state: StoredState = decode(&payload)?;
        if state.version != 1
            || state.reservation.as_ref() != Some(run)
            || state.source_run_id.as_deref() != Some(run.allocation_id.as_str())
        {
            return Err(ExecutionLedgerError::AccountScopeMismatch);
        }
        let identity = super::sqlite_source::attest_indexed(&connection, &run.allocation_id, &uid)?;
        let authoritative: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM native_source_events WHERE run_id=?1",
                params![&run.allocation_id],
                |r| r.get(0),
            )
            .map_err(io)?;
        let indexed: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM native_source_finance_order WHERE run_id=?1",
                params![&run.allocation_id],
                |r| r.get(0),
            )
            .map_err(io)?;
        if authoritative != indexed {
            return Err(ExecutionLedgerError::Corrupt(
                "native finance/source index inventory mismatch",
            ));
        }
        Ok(Self {
            connection,
            run: run.clone(),
            identity,
        })
    }
    fn source_event(&self, key: &str) -> Result<Option<StoredExchangeEvent>, ExecutionLedgerError> {
        let row:Option<(String,Vec<u8>)> = self.connection.query_row(
            "SELECT payload_digest,payload FROM native_source_events WHERE run_id=?1 AND event_key=?2",
            params![&self.run.allocation_id,key], |r| Ok((r.get(0)?,r.get(1)?))
        ).optional().map_err(io)?;
        row.map(|(digest, bytes)| {
            if checksum(&bytes) != digest {
                return Err(ExecutionLedgerError::Corrupt(
                    "indexed financial event digest",
                ));
            }
            let original = decode(&bytes)?;
            if key.starts_with("bill:") {
                super::sqlite_source::effective_bill_event(
                    &self.connection,
                    &self.run.allocation_id,
                    key,
                    original,
                )
            } else {
                Ok(original)
            }
        })
        .transpose()
    }
    fn original_orders(
        &self,
        order_id: &str,
    ) -> Result<Vec<(String, StoredExchangeEvent)>, ExecutionLedgerError> {
        // The current execution venue supports SWAP; an unsupported
        // source kind fails closed instead of searching by instrument alone.
        let prefix = format!("order:SWAP:{order_id}:");
        let limit = format!("order:SWAP:{order_id};");
        let mut stmt = self
            .connection
            .prepare(
                "SELECT event_key,payload_digest,payload FROM native_source_events
             WHERE run_id=?1 AND event_key>=?2 AND event_key<?3 ORDER BY event_key",
            )
            .map_err(io)?;
        let mut rows = stmt
            .query(params![&self.run.allocation_id, prefix, limit])
            .map_err(io)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(io)? {
            let key: String = row.get(0).map_err(io)?;
            let digest: String = row.get(1).map_err(io)?;
            let bytes: Vec<u8> = row.get(2).map_err(io)?;
            if checksum(&bytes) != digest {
                return Err(ExecutionLedgerError::Corrupt("native order source digest"));
            }
            let event: StoredExchangeEvent = decode(&bytes)?;
            if event.payload["source"] != "OKX_REST_ORDER"
                || event.payload["ordId"].as_str() != Some(order_id)
            {
                return Err(ExecutionLedgerError::Corrupt(
                    "native order source identity",
                ));
            }
            out.push((key, event));
        }
        Ok(out)
    }
}
impl crate::strategy_finance::FinanceSourceReader for NativeFinanceReader {
    fn audit(&self) -> Result<crate::strategy_finance::FinanceSourceAudit, ExecutionLedgerError> {
        let mut times = Vec::new();
        let mut incomplete = false;
        let mut stmt = self
            .connection
            .prepare(
                "SELECT receipt_ms,payload_digest,payload FROM native_source_cursors
             WHERE run_id=?1 ORDER BY receipt_ms,cursor_key",
            )
            .map_err(io)?;
        let mut rows = stmt.query(params![&self.run.allocation_id]).map_err(io)?;
        while let Some(row) = rows.next().map_err(io)? {
            let timestamp: i64 = row.get(0).map_err(io)?;
            let hash: String = row.get(1).map_err(io)?;
            let bytes: Vec<u8> = row.get(2).map_err(io)?;
            if checksum(&bytes) != hash {
                return Err(ExecutionLedgerError::Corrupt("native finance cursor hash"));
            }
            let cursor: crate::exchange_evidence::ExchangeCoverageCursor = decode(&bytes)?;
            let received = crate::exchange_evidence::source_receipt_ms(&cursor.source_received_at)
                .ok_or(ExecutionLedgerError::ExchangeSourceIncomplete)?;
            if i64::try_from(received).ok() != Some(timestamp) {
                return Err(ExecutionLedgerError::Corrupt(
                    "finance source receipt mismatch",
                ));
            }
            times.push(received);
            incomplete |= !cursor.order_complete || !cursor.fill_complete || !cursor.bill_complete;
        }
        let latest = times.iter().max().copied();
        Ok(crate::strategy_finance::FinanceSourceAudit {
            uid: self.identity.account_uid.clone(),
            run_id: self.identity.run_id.clone(),
            digest: self.identity.checksum_sha256.clone(),
            receipt_times: times,
            latest_receipt: latest,
            truncated: incomplete,
        })
    }
    fn visit_fills(
        &self,
        visit: &mut dyn FnMut(&StoredExchangeEvent) -> Result<(), ExecutionLedgerError>,
    ) -> Result<(), ExecutionLedgerError> {
        super::sqlite_source::visit_financial_kind(
            &self.connection,
            &self.run.allocation_id,
            &self.run.account_uid_fingerprint,
            "FILL",
            |_, event| visit(event),
        )
    }
    fn bill(&self, key: &str) -> Result<Option<StoredExchangeEvent>, ExecutionLedgerError> {
        self.source_event(key)
    }
    fn visit_bills(
        &self,
        visitor: &mut dyn FnMut(&str, &StoredExchangeEvent) -> Result<(), ExecutionLedgerError>,
    ) -> Result<(), ExecutionLedgerError> {
        super::sqlite_source::visit_financial_kind(
            &self.connection,
            &self.run.allocation_id,
            &self.run.account_uid_fingerprint,
            "BILL",
            |key, event| visitor(key, event),
        )
    }
    fn entry(
        &self,
        id: &str,
        _entries: &[ExecutionLedgerEntry],
    ) -> Result<Option<ExecutionLedgerEntry>, ExecutionLedgerError> {
        if id.is_empty() || id.len() > 128 {
            return Err(ExecutionLedgerError::Corrupt(
                "bad native financial intent ID",
            ));
        }
        let sql = format!("{INDEXED_INTENT_COLUMNS} WHERE e.intent_id=?1");
        let mut stmt = self.connection.prepare(&sql).map_err(io)?;
        let mut rows = stmt.query(params![id]).map_err(io)?;
        let entry = rows
            .next()
            .map_err(io)?
            .map(|r| verified_indexed_entry(r, &self.run.account_uid_fingerprint))
            .transpose()?;
        if rows.next().map_err(io)?.is_some() {
            return Err(ExecutionLedgerError::Corrupt(
                "duplicate indexed financial intent",
            ));
        }
        Ok(entry)
    }
    fn exact_order(
        &self,
        order: &str,
        client: &str,
        inst: &str,
        intent: &str,
    ) -> Result<bool, ExecutionLedgerError> {
        Ok(self.original_orders(order)?.iter().any(|(_, e)| {
            e.payload["clOrdId"].as_str() == Some(client)
                && e.payload["instId"].as_str() == Some(inst)
                && e.source_intent_id.as_deref() == Some(intent)
        }))
    }
    fn validate_entry(
        &self,
        entry: &ExecutionLedgerEntry,
        since: u64,
        reasons: &mut std::collections::BTreeSet<&'static str>,
    ) -> Result<(), ExecutionLedgerError> {
        if entry.created_at_ms <= since {
            return Ok(());
        }
        let Some(order_id) = entry.record.order_id.as_deref() else {
            return Ok(());
        };
        let mut source = DurableExchangeEvidence::new(&self.run)?;
        for (key, order) in self.original_orders(order_id)? {
            source.events.insert(key, order);
        }
        let mut stmt=self.connection.prepare(
            "SELECT instrument_id,trade_id FROM fills WHERE intent_id=?1 AND venue_order_id=?2 ORDER BY exchange_time_ms,trade_id"
        ).map_err(io)?;
        let mut rows = stmt
            .query(params![&entry.record.plan.intent_id, order_id])
            .map_err(io)?;
        while let Some(row) = rows.next().map_err(io)? {
            let inst: String = row.get(0).map_err(io)?;
            let trade: String = row.get(1).map_err(io)?;
            let key = format!("fill:SWAP:{inst}:{trade}");
            let fill = self
                .source_event(&key)?
                .ok_or(ExecutionLedgerError::Corrupt(
                    "missing source-indexed native fill",
                ))?;
            if fill.payload["ordId"].as_str() != Some(order_id) {
                return Err(ExecutionLedgerError::Corrupt(
                    "native financial fill/order mismatch",
                ));
            }
            source.events.insert(key, fill);
        }
        let index = crate::strategy_finance::SourceOrderIndex::new(&source);
        index.validate_entry(entry, since, reasons);
        Ok(())
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;

    #[test]
    fn source_order_projection_preserves_identity_and_monotonic_exchange_time() {
        let mut c = rusqlite::Connection::open_in_memory().unwrap();
        c.execute_batch(
            "CREATE TABLE exchange_orders (
                venue_order_id TEXT PRIMARY KEY,
                intent_id TEXT,
                instrument_id TEXT NOT NULL,
                client_order_id TEXT NOT NULL,
                status TEXT NOT NULL,
                last_exchange_update_ms INTEGER NOT NULL
            ) STRICT;",
        )
        .unwrap();
        let event = |state: &str, ts: &str, client: &str| StoredExchangeEvent {
            payload: serde_json::json!({
                "source":"OKX_REST_ORDER",
                "ordId":"order-123",
                "clOrdId":client,
                "instId":"BTC-USDT-SWAP",
                "state":state,
                "uTime":ts,
            }),
            source_intent_id: None,
            strategy_run_id: None,
            allocation: "UNALLOCATED".to_owned(),
        };
        {
            let tx = c.transaction().unwrap();
            insert_order(&tx, &event("live", "1000", "client-1")).unwrap();
            insert_order(&tx, &event("filled", "2000", "client-1")).unwrap();
            // Out-of-order windows cannot regress the current state.
            insert_order(&tx, &event("live", "1000", "client-1")).unwrap();
            assert!(matches!(
                insert_order(&tx, &event("canceled", "2000", "client-1")),
                Err(ExecutionLedgerError::ExchangeEvidenceConflict)
            ));
            assert!(matches!(
                insert_order(&tx, &event("filled", "3000", "client-2")),
                Err(ExecutionLedgerError::ExchangeEvidenceConflict)
            ));
            tx.commit().unwrap();
        }
        let (state, updated): (String, i64) = c
            .query_row(
                "SELECT status,last_exchange_update_ms FROM exchange_orders
                 WHERE venue_order_id='order-123'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, "filled");
        assert_eq!(updated, 2000);

        // Another account-owned or manual venue order may have no client
        // order ID. It remains a real immutable exchange fact, never an
        // invented managed intent.
        let external = StoredExchangeEvent {
            payload: serde_json::json!({
                "source":"OKX_REST_ORDER",
                "ordId":"unmanaged-456",
                "clOrdId":"",
                "instId":"BTC-USDT-SWAP",
                "state":"canceled",
                "uTime":"4000",
            }),
            source_intent_id: None,
            strategy_run_id: None,
            allocation: "UNALLOCATED".to_owned(),
        };
        {
            let tx = c.transaction().unwrap();
            insert_order(&tx, &external).unwrap();
            tx.commit().unwrap();
        }
        let client_id: String = c
            .query_row(
                "SELECT client_order_id FROM exchange_orders
                 WHERE venue_order_id='unmanaged-456'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(client_id.is_empty());
    }

    /// The same deferred read transaction used by finance_window must pin
    /// all constituent counts to one point, even when another owner commits
    /// a new bill after the first SELECT.
    /// A previously incomplete bill is not permanently unaccountable when
    /// fresh exchange evidence supplies the exact balance change. Original
    /// and resolving proofs both survive; bad duplicates fail as one WAL tx.
    #[test]
    fn exact_later_bill_resolves_only_its_matching_financial_gap() {
        let mut c = rusqlite::Connection::open_in_memory().unwrap();
        c.pragma_update(None, "foreign_keys", "ON").unwrap();
        c.execute_batch(include_str!("../../schema/account-ledger-v1.sql"))
            .unwrap();
        let first = "a".repeat(64);
        let second = "b".repeat(64);
        let third = "c".repeat(64);
        for (proof, location) in [
            (&first, "source/initial"),
            (&second, "source/resolution"),
            (&third, "source/duplicate"),
        ] {
            c.execute(
                "INSERT INTO evidence_objects
                 (evidence_sha256,evidence_kind,codec,relative_path,size_bytes)
                 VALUES(?1,'AUTHENTICATED_EXCHANGE_EVENT','sqlite-msgpack',?2,1)",
                params![proof, location],
            )
            .unwrap();
        }
        let incomplete = serde_json::json!({
            "billId":"bill-17","ts":"5000","ccy":"USDT",
            "instId":"BTC-USDT-SWAP","tradeId":"trade-17",
            "type":"2","subType":"1"
        });
        let mut exact = incomplete.clone();
        exact["balChg"] = serde_json::json!("-0.25");
        {
            let tx = c.transaction().unwrap();
            insert_bill(&tx, &incomplete, &first).unwrap();
            tx.commit().unwrap();
        }
        let read = |conn: &rusqlite::Connection| -> (Option<String>, Option<String>, i64) {
            let gap: (Option<String>, Option<String>) = conn
                .query_row(
                    "SELECT opening_evidence_sha256,resolution_evidence_sha256
                 FROM source_gaps WHERE gap_id='bill-financial-missing:bill-17'",
                    [],
                    |r| Ok((r.get(0).unwrap(), r.get(1).unwrap())),
                )
                .unwrap();
            let n: i64 = conn
                .query_row("SELECT COUNT(*) FROM bills", [], |r| r.get(0))
                .unwrap();
            (gap.0, gap.1, n)
        };
        assert_eq!(read(&c), (Some(first.clone()), None, 0));
        {
            let tx = c.transaction().unwrap();
            insert_bill(&tx, &exact, &second).unwrap();
            tx.commit().unwrap();
        }
        assert_eq!(read(&c), (Some(first.clone()), Some(second.clone()), 1));
        {
            let tx = c.transaction().unwrap();
            insert_bill(&tx, &exact, &third).unwrap();
            insert_bill(&tx, &incomplete, &first).unwrap();
            tx.commit().unwrap();
        }
        assert_eq!(read(&c), (Some(first.clone()), Some(second.clone()), 1));
        let mut conflicting = exact.clone();
        conflicting["balChg"] = serde_json::json!("-0.5");
        {
            let tx = c.transaction().unwrap();
            assert!(insert_bill(&tx, &conflicting, &third).is_err());
        }
        assert_eq!(read(&c), (Some(first.clone()), Some(second.clone()), 1));
        let mut wrong_identity = incomplete.clone();
        wrong_identity["ts"] = serde_json::json!("6000");
        {
            let tx = c.transaction().unwrap();
            assert!(insert_bill(&tx, &wrong_identity, &third).is_err());
        }
        assert_eq!(read(&c), (Some(first), Some(second), 1));
    }

    #[test]
    fn finance_read_snapshot_does_not_mix_concurrent_commits() {
        let root = std::env::temp_dir().join(format!(
            "okx-finance-snapshot-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("account.sqlite");
        let mut reader = rusqlite::Connection::open(&path).unwrap();
        reader.pragma_update(None, "journal_mode", "WAL").unwrap();
        reader
            .execute_batch(
                "CREATE TABLE financial_facts (id INTEGER PRIMARY KEY);
                INSERT INTO financial_facts VALUES (1);",
            )
            .unwrap();
        let writer = rusqlite::Connection::open(&path).unwrap();

        let snapshot = begin_finance_snapshot(&mut reader).unwrap();
        let before: i64 = snapshot
            .query_row("SELECT COUNT(*) FROM financial_facts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(before, 1);

        // A WAL writer must not wait for the report reader, and the report
        // must not silently adopt this newer committed generation.
        writer
            .execute("INSERT INTO financial_facts VALUES (2)", [])
            .unwrap();
        let pinned: i64 = snapshot
            .query_row("SELECT COUNT(*) FROM financial_facts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(pinned, before);
        snapshot.commit().unwrap();

        let latest: i64 = reader
            .query_row("SELECT COUNT(*) FROM financial_facts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(latest, 2);
        drop(writer);
        drop(reader);
        std::fs::remove_dir_all(root).unwrap();
    }
}

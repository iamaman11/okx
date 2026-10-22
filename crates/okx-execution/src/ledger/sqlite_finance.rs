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

fn insert_bill(tx: &Transaction<'_>, v: &Value, proof: &str) -> Result<(), ExecutionLedgerError> {
    let bill = field(v, "billId")?;
    let timestamp = ms(v)?;
    let currency = field(v, "ccy")?;
    let inst = nullable(v, "instId")?;
    let trade = nullable(v, "tradeId")?;
    let bill_type = nullable(v, "type")?.unwrap_or("UNKNOWN_LEGACY");
    let subtype = nullable(v, "subType")?.unwrap_or("UNKNOWN_LEGACY");
    let kind = format!("{bill_type}/{subtype}");
    let Some(delta) = optional_amount(v, "balChg")? else {
        let from = timestamp;
        let through = timestamp
            .checked_add(1)
            .ok_or(ExecutionLedgerError::Corrupt("bill time overflow"))?;
        let gap_id = format!("bill-financial-missing:{bill}");
        tx.execute(
            "INSERT INTO source_gaps
             (gap_id,stream_kind,scope_key,from_ms,through_ms,materiality,reason_code,
              opening_evidence_sha256,resolution_evidence_sha256)
             VALUES(?1,'BILLS',?2,?3,?4,'FINANCIAL','MISSING_EXACT_BALANCE_CHANGE',?5,NULL)
             ON CONFLICT(gap_id) DO NOTHING",
            params![gap_id, bill, from, through, proof],
        )
        .map_err(io)?;
        return Ok(());
    };
    tx.execute(
        "INSERT INTO bills
         (bill_id,instrument_id,trade_id,exchange_time_ms,currency,bill_kind,delta_decimal,source_evidence_sha256)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8)
         ON CONFLICT(bill_id) DO NOTHING",
        params![bill,inst,trade,timestamp,currency,kind,delta,proof],
    ).map_err(io)?;
    let stored: (Option<String>, Option<String>, i64, String, String, String) = tx
        .query_row(
            "SELECT instrument_id,trade_id,exchange_time_ms,currency,bill_kind,delta_decimal
         FROM bills WHERE bill_id=?1",
            params![bill],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                ))
            },
        )
        .map_err(io)?;
    if stored.0.as_deref() != inst
        || stored.1.as_deref() != trade
        || stored.2 != timestamp
        || stored.3 != currency
        || stored.4 != kind
        || stored.5 != delta
    {
        return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
    }
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
    let mut linked = 0usize;
    let mut fees = Decimal::ZERO;
    let mut realized = Decimal::ZERO;
    let mut gross_loss = Decimal::ZERO;
    let mut source_ahead = false;
    super::sqlite_source::visit_events(&c, &run.allocation_id, &uid, |_, event| {
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
        if event.payload["ccy"] != "USDT" {
            return Err(ExecutionLedgerError::ExchangeSourceIncomplete);
        }
        let fee = event.payload["fee"]
            .as_str()
            .and_then(|v| Decimal::from_str(v).ok())
            .ok_or(ExecutionLedgerError::ExchangeSourceIncomplete)?;
        let pnl = event.payload["pnl"]
            .as_str()
            .and_then(|v| Decimal::from_str(v).ok())
            .ok_or(ExecutionLedgerError::ExchangeSourceIncomplete)?;
        let ts = event.payload["ts"]
            .as_u64()
            .ok_or(ExecutionLedgerError::ExchangeSourceIncomplete)?;
        if ts < run.started_at_ms {
            return Err(ExecutionLedgerError::ExchangeSourceIncomplete);
        }
        if ts > as_of {
            source_ahead = true;
            return Ok(());
        }
        linked = linked
            .checked_add(1)
            .ok_or(ExecutionLedgerError::ExchangeEvidenceCapacity)?;
        fees += fee;
        realized += pnl;
        if ts >= day {
            if fee < Decimal::ZERO {
                gross_loss -= fee;
            }
            if pnl < Decimal::ZERO {
                gross_loss -= pnl;
            }
        }
        Ok(())
    })?;
    if source_ahead {
        return Ok(None);
    }
    if linked != report.linked_bills
        || Decimal::from_str(&report.fees_usdt).ok() != Some(fees)
        || Decimal::from_str(&report.realized_usdt).ok() != Some(realized)
    {
        return Err(ExecutionLedgerError::ExchangeSourceIncomplete);
    }
    if run.started_at_ms >= day && report.state == "FLAT_RECONCILED" {
        let allocation = Decimal::from_str(&run.allocated_usdt)
            .map_err(|_| ExecutionLedgerError::CapitalReservationUnattested)?;
        let nav = Decimal::from_str(
            report
                .nav_usdt
                .as_deref()
                .ok_or(ExecutionLedgerError::ExchangeSourceIncomplete)?,
        )
        .map_err(|_| ExecutionLedgerError::ExchangeSourceIncomplete)?;
        if gross_loss < (allocation - nav).max(Decimal::ZERO) {
            return Err(ExecutionLedgerError::ExchangeSourceIncomplete);
        }
    }
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

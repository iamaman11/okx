//! Typed account financial fact projections from immutable native OKX REST evidence.
//! Projection is transactional and rebuildable from the single source ledger.
//! UNKNOWN legacy cash changes become explicit material source gaps, never zero PnL.
use super::sqlite_store::{checksum, encode, io};
use super::*;
use crate::exchange_evidence::StoredExchangeEvent;
use rusqlite::{Transaction, params};
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
        Some("OKX_REST_ORDER") => {}
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

/// One read-only, bounded-by-request financial view. Returns observed money
/// movement and explicit uncertainty; it never authorizes an exchange POST.
/// The caller supplies the natively verified UID, not a user-facing selector.
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
    // Read-only opening cannot create or initialize a missing account.
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(io)?;
    connection
        .busy_timeout(std::time::Duration::from_secs(5))
        .map_err(io)?;
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(io)?;
    if version != 1 {
        return Err(ExecutionLedgerError::Corrupt(
            "finance SQLite schema mismatch",
        ));
    }
    let account_uid: String = connection
        .query_row("SELECT uid FROM ledger_state WHERE key=1", [], |row| {
            row.get(0)
        })
        .map_err(io)?;
    if account_uid != expected_uid {
        return Err(ExecutionLedgerError::AccountScopeMismatch);
    }
    let first = i64::try_from(from_ms).map_err(|_| ExecutionLedgerError::InvalidTimestamp)?;
    let end = i64::try_from(through_ms).map_err(|_| ExecutionLedgerError::InvalidTimestamp)?;
    let mut accum = BTreeMap::<(String, String), (u64, Decimal)>::new();
    {
        let mut stmt = connection
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
        let n: i64 = connection
            .query_row(query, params![first, end], |r| r.get(0))
            .map_err(io)?;
        u64::try_from(n).map_err(|_| ExecutionLedgerError::Corrupt("negative finance count"))
    };
    let fill_rows = count("fills")?;
    let bill_rows = count("bills")?;
    let gap_count = |materiality: &str| -> Result<u64, ExecutionLedgerError> {
        let n: i64 = connection
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
        let n: i64 = connection
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
    Ok(AccountFinanceWindow {
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
    })
}

/// Separate source coverage facts per stream. Every inserted interval is a
/// POINT observation; it never fabricates continuity across missing history.
pub(super) fn persist_source_receipt(
    tx: &Transaction<'_>,
    account_uid: &str,
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
             VALUES(?1,?2,?3,?4,?4,?4,?5,NULL)
             ON CONFLICT(receipt_sha256) DO NOTHING",
            params![
                key,
                kind,
                account_uid,
                at,
                if complete { "COMPLETE" } else { "PARTIAL" }
            ],
        )
        .map_err(io)?;
    }
    Ok(())
}

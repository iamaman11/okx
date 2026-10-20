//! Typed account financial fact projections from immutable native OKX REST evidence.
//! Projection is transactional and rebuildable from the single source ledger.
//! UNKNOWN legacy cash changes become explicit material source gaps, never zero PnL.
use super::sqlite_store::{checksum, encode, io};
use super::*;
use crate::exchange_evidence::{DurableExchangeEvidence, StoredExchangeEvent};
use rusqlite::{OptionalExtension, Transaction, params};
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
    let stored: (
        Option<String>,
        Option<String>,
        i64,
        String,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
    ) = tx
        .query_row(
            "SELECT venue_order_id,intent_id,exchange_time_ms,side,position_side,
                    price_decimal,contracts_decimal,fee_decimal,fee_currency
             FROM fills WHERE instrument_id=?1 AND trade_id=?2",
            params![inst, trade],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                    r.get(8)?,
                ))
            },
        )
        .map_err(io)?;
    if stored.0.as_deref() != ord
        || stored.1.as_deref() != intent
        || stored.2 != timestamp
        || stored.3 != side
        || stored.4 != pos
        || stored.5 != price
        || stored.6 != contracts
        || stored.7.as_deref() != fee
        || stored.8.as_deref() != ccy
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

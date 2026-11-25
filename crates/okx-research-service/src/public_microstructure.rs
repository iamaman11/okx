//! Standalone public REST book. This is observational evidence, not a WS trading book.
use crate::independent_source::{IndependentOkxPublicSource, one};
use okx_domain_core::observation_model::BookLevel;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Value, json};
use std::time::{SystemTime, UNIX_EPOCH};

const BOOK_PATH: &str = "/api/v5/market/books";
const MAX_AGE_MS: u64 = 2_000;

pub(crate) async fn market_microstructure(
    source: &IndependentOkxPublicSource,
    instrument: &str,
    size_contracts: Option<&str>,
) -> Result<Value, String> {
    validate_request(instrument, size_contracts)?;
    let bytes = source
        .get_bytes(
            BOOK_PATH,
            vec![("instId", instrument.to_owned()), ("sz", "10".into())],
        )
        .await?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "CLOCK_INVALID")?
        .as_millis();
    let now = u64::try_from(now).map_err(|_| "CLOCK_INVALID")?;
    summarize(&bytes, instrument, size_contracts, now)
}

fn validate_request(instrument: &str, size: Option<&str>) -> Result<(), String> {
    if instrument.is_empty()
        || instrument.len() > 64
        || !instrument
            .bytes()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'-')
        || instrument.starts_with('-')
        || instrument.ends_with('-')
        || instrument.contains("--")
    {
        return Err("INSTRUMENT_INVALID".into());
    }
    if let Some(size) = size {
        if size.len() > 64
            || !Decimal::from_str_exact(size)
                .is_ok_and(|n| n > Decimal::ZERO && n <= Decimal::from(1_000_000))
        {
            return Err("SIZE_INVALID".into());
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct RestBook {
    asks: Vec<[String; 4]>,
    bids: Vec<[String; 4]>,
    ts: String,
}

fn side(
    rows: &[[String; 4]],
    ascending: bool,
) -> Result<Vec<(Decimal, Decimal, BookLevel)>, String> {
    if rows.is_empty() || rows.len() > 10 {
        return Err("BOOK_LEVEL_INVALID".into());
    }
    let mut out: Vec<(Decimal, Decimal, BookLevel)> = Vec::new();
    for row in rows {
        let price = row[0]
            .parse::<Decimal>()
            .map_err(|_| "BOOK_PRICE_INVALID")?;
        let size = row[1].parse::<Decimal>().map_err(|_| "BOOK_SIZE_INVALID")?;
        if price <= Decimal::ZERO || size <= Decimal::ZERO {
            return Err("BOOK_LEVEL_INVALID".into());
        }
        if let Some((prev, _, _)) = out.last() {
            if (ascending && price <= *prev) || (!ascending && price >= *prev) {
                return Err("BOOK_LEVEL_INVALID".into());
            }
        }
        let count = row[3].parse::<u64>().map_err(|_| "BOOK_LEVEL_INVALID")?;
        if count == 0 || row[2] != "0" {
            return Err("BOOK_LEVEL_INVALID".into());
        }
        out.push((
            price,
            size,
            BookLevel {
                price: row[0].clone(),
                size: row[1].clone(),
                order_count: Some(row[3].clone()),
            },
        ));
    }
    Ok(out)
}

fn sweep(levels: &[(Decimal, Decimal, BookLevel)], qty: Decimal) -> Result<Decimal, String> {
    let mut filled = Decimal::ZERO;
    let mut cost = Decimal::ZERO;
    for (price, size, _) in levels {
        let take = (qty - filled).min(*size);
        filled = filled.checked_add(take).ok_or("BOOK_OVERFLOW")?;
        cost = cost
            .checked_add(price.checked_mul(take).ok_or("BOOK_OVERFLOW")?)
            .ok_or("BOOK_OVERFLOW")?;
        if filled == qty {
            return cost.checked_div(filled).ok_or("BOOK_OVERFLOW".into());
        }
    }
    Err("INSUFFICIENT_BOOK_DEPTH".into())
}

fn summarize(
    bytes: &[u8],
    instrument: &str,
    size: Option<&str>,
    now: u64,
) -> Result<Value, String> {
    validate_request(instrument, size)?;
    let book: RestBook = one(bytes, "BOOK_RESPONSE_INVALID")?;
    let ts = book
        .ts
        .parse::<u64>()
        .map_err(|_| "BOOK_TIMESTAMP_INVALID")?;
    if ts == 0 || ts > now || now - ts > MAX_AGE_MS {
        return Err("MICROSTRUCTURE_FRESH_BOOK_REQUIRED".into());
    }
    let asks = side(&book.asks, true)?;
    let bids = side(&book.bids, false)?;
    let ask = asks[0].0;
    let bid = bids[0].0;
    if ask <= bid {
        return Err("BOOK_SPREAD_INVALID".into());
    }
    let spread = ask
        .checked_sub(bid)
        .and_then(|v| v.checked_mul(Decimal::from(20_000)))
        .and_then(|v| bid.checked_add(ask).and_then(|sum| v.checked_div(sum)))
        .ok_or("BOOK_OVERFLOW")?
        .normalize()
        .to_string();
    let sweep_result = size
        .map(|size| {
            let qty = Decimal::from_str_exact(size).map_err(|_| "SIZE_INVALID")?;
            let buy = sweep(&asks, qty)?;
            let sell = sweep(&bids, qty)?;
            let impact = |delta: Decimal, base: Decimal| {
                delta
                    .checked_mul(Decimal::from(10_000))
                    .and_then(|value| value.checked_div(base))
                    .map(|value| value.normalize().to_string())
                    .ok_or("BOOK_OVERFLOW")
            };
            Ok::<_, String>(json!({
                "size_contracts": size,
                "buy_vwap": buy.normalize().to_string(),
                "sell_vwap": sell.normalize().to_string(),
                "buy_impact_bps": impact(buy - ask, ask)?,
                "sell_impact_bps": impact(bid - sell, bid)?,
            }))
        })
        .transpose()?;
    let levels = |items: &[(Decimal, Decimal, BookLevel)]| {
        items
            .iter()
            .map(|(_, _, level)| level.clone())
            .collect::<Vec<_>>()
    };
    Ok(json!({
        "schema": "okx.market.microstructure/v1",
        "instrument": instrument,
        "quality": "degraded",
        "source": "okx_public_rest_snapshot",
        "evidence": "standalone_snapshot_not_contiguous_ws",
        "reference_generation": null,
        "generation": null,
        "book_sequence": null,
        "book_exchange_timestamp_ms": ts.to_string(),
        "spread_bps": spread,
        "asks": levels(&asks),
        "bids": levels(&bids),
        "sweep": sweep_result,
        "mark_price": null,
        "index_price": null,
        "funding": null,
        "open_interest": null,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> Vec<u8> {
        br#"{"code":"0","data":[{"ts":"10000","asks":[["101","2","0","1"],["102","2","0","1"]],"bids":[["99","2","0","1"],["98","2","0","1"]]}]}"#.to_vec()
    }
    #[test]
    fn standalone_snapshot_and_sweep_are_honest() {
        let v = summarize(&fixture(), "BTC-USDT-SWAP", Some("3"), 10000).unwrap();
        assert_eq!(v["quality"], "degraded");
        assert!(v["book_sequence"].is_null());
        assert_eq!(v["spread_bps"], "200");
        assert_eq!(v["sweep"]["buy_vwap"], "101.33333333333333333333333333");
        assert!(summarize(&fixture(), "BTC-USDT-SWAP", Some("5"), 10000).is_err());
    }
    #[test]
    fn rejects_stale_invalid_and_broken_books() {
        assert!(summarize(&fixture(), "BTC-USDT-SWAP", None, 12001).is_err());
        assert!(summarize(&fixture(), "BTC-USDT-SWAP", Some("0"), 10000).is_err());
        assert!(summarize(&fixture(), "../BTC", None, 10000).is_err());
        let crossed = String::from_utf8(fixture())
            .unwrap()
            .replace("\"99\"", "\"101\"");
        assert!(summarize(crossed.as_bytes(), "BTC-USDT-SWAP", None, 10000).is_err());
        let unsorted = String::from_utf8(fixture())
            .unwrap()
            .replace("\"102\"", "\"100\"");
        assert!(summarize(unsorted.as_bytes(), "BTC-USDT-SWAP", None, 10000).is_err());
    }
}

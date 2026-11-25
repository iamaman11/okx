//! Bounded, standalone REST projection of the public market overview.
use crate::independent_source::{IndependentOkxPublicSource, one};
use okx_research_domain::observation_model::{
    FundingState, IndexPriceState, InstrumentType, MarkPriceState, MarketSnapshot,
    OpenInterestState, TickerState,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

const ERROR: &str = "MARKET_SOURCE_UNAVAILABLE";

#[derive(serde::Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct VenueRow {
    inst_id: String,
    inst_type: String,
    state: String,
    uly: String,
    rule_type: String,
    last: String,
    last_sz: String,
    ask_px: String,
    ask_sz: String,
    bid_px: String,
    bid_sz: String,
    open24h: String,
    high24h: String,
    low24h: String,
    vol_ccy24h: String,
    vol24h: String,
    mark_px: String,
    idx_px: String,
    oi: String,
    oi_ccy: String,
    oi_usd: String,
    funding_rate: String,
    funding_time: String,
    next_funding_time: String,
    premium: String,
    min_funding_rate: String,
    max_funding_rate: String,
    ts: String,
}
fn row(raw: &[u8]) -> Result<VenueRow, String> {
    one(raw, ERROR)
}
fn required(v: &str) -> Result<String, String> {
    if v.trim().is_empty() {
        Err(ERROR.into())
    } else {
        Ok(v.to_owned())
    }
}
fn optional(v: &str) -> Option<String> {
    if v.trim().is_empty() {
        None
    } else {
        Some(v.to_owned())
    }
}
fn identity(actual: &str, expected: &str) -> Result<(), String> {
    if actual != expected {
        return Err(ERROR.into());
    }
    Ok(())
}
fn timestamp(s: &str, now: u64) -> Result<String, String> {
    let ts = s.parse::<u64>().map_err(|_| ERROR)?;
    if ts == 0 || ts > now.saturating_add(5_000) || now.saturating_sub(ts) > 300_000 {
        return Err(ERROR.into());
    }
    Ok(s.to_owned())
}
fn number(s: &str, positive: bool) -> Result<String, String> {
    let value = s.parse::<rust_decimal::Decimal>().map_err(|_| ERROR)?;
    if (positive && value <= rust_decimal::Decimal::ZERO)
        || (!positive && value < rust_decimal::Decimal::ZERO)
    {
        return Err(ERROR.into());
    }
    Ok(s.to_owned())
}
fn signed_number(s: &str) -> Result<String, String> {
    s.parse::<rust_decimal::Decimal>().map_err(|_| ERROR)?;
    Ok(s.to_owned())
}
fn event_time(s: &str) -> Result<String, String> {
    if s.parse::<u64>().map_err(|_| ERROR)? == 0 {
        return Err(ERROR.into());
    }
    Ok(s.to_owned())
}
fn digest(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

pub(crate) async fn market_overview(
    source: &IndependentOkxPublicSource,
    instrument: &str,
) -> Result<Value, String> {
    let inst_type = if instrument.ends_with("-SWAP") {
        "SWAP"
    } else {
        "FUTURES"
    };
    let params = || vec![("instId", instrument.to_owned())];
    let reference_raw = source
        .get_bytes(
            "/api/v5/public/instruments",
            vec![
                ("instType", inst_type.into()),
                ("instId", instrument.into()),
            ],
        )
        .await?;
    let reference = row(&reference_raw)?;
    identity(&reference.inst_id, instrument)?;
    identity(&reference.inst_type, inst_type)?;
    identity(&reference.state, "live")?;
    let underlying = required(&reference.uly)?;
    let funding_required = if inst_type == "SWAP" {
        true
    } else {
        match required(&reference.rule_type)?.as_str() {
            "normal" => false,
            "xperp" | "pre_market" => true,
            _ => return Err(ERROR.into()),
        }
    };
    let (ticker, mark, index, oi) = tokio::try_join!(
        source.get_bytes("/api/v5/market/ticker", params()),
        source.get_bytes(
            "/api/v5/public/mark-price",
            vec![
                ("instType", inst_type.into()),
                ("instId", instrument.into())
            ]
        ),
        source.get_bytes(
            "/api/v5/market/index-tickers",
            vec![("instId", underlying.clone())]
        ),
        source.get_bytes(
            "/api/v5/public/open-interest",
            vec![
                ("instType", inst_type.into()),
                ("instId", instrument.into())
            ]
        ),
    )?;
    let funding = if funding_required {
        Some(
            source
                .get_bytes("/api/v5/public/funding-rate", params())
                .await?,
        )
    } else {
        None
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ERROR)?
        .as_millis()
        .try_into()
        .map_err(|_| ERROR)?;
    let t = row(&ticker)?;
    let m = row(&mark)?;
    let i = row(&index)?;
    let o = row(&oi)?;
    for v in [&t, &m, &o] {
        identity(&v.inst_id, instrument)?;
    }
    identity(&i.inst_id, &underlying)?;
    let funding = funding
        .map(|raw| -> Result<FundingState, String> {
            let f = row(&raw)?;
            identity(&f.inst_id, instrument)?;
            Ok(FundingState {
                rate: signed_number(&f.funding_rate)?,
                funding_time_ms: event_time(&f.funding_time)?,
                next_funding_time_ms: event_time(&f.next_funding_time)?,
                premium: optional(&f.premium),
                min_rate: optional(&f.min_funding_rate),
                max_rate: optional(&f.max_funding_rate),
                exchange_timestamp_ms: timestamp(&f.ts, now)?,
            })
        })
        .transpose()?;
    let mut market = MarketSnapshot {
        schema: "okx.market-snapshot/v1".into(),
        instrument_id: instrument.into(),
        instrument_type: if inst_type == "SWAP" {
            InstrumentType::Swap
        } else {
            InstrumentType::Futures
        },
        underlying: underlying.clone(),
        reference_generation: digest(&reference_raw),
        market_generation: String::new(),
        source_received_at: format!("{:.3}", jiff::Timestamp::now()),
        ticker: TickerState {
            last: number(&t.last, true)?,
            last_size: number(&t.last_sz, false)?,
            best_ask: number(&t.ask_px, true)?,
            best_ask_size: number(&t.ask_sz, false)?,
            best_bid: number(&t.bid_px, true)?,
            best_bid_size: number(&t.bid_sz, false)?,
            open_24h: optional(&t.open24h),
            high_24h: optional(&t.high24h),
            low_24h: optional(&t.low24h),
            volume_currency_24h: optional(&t.vol_ccy24h),
            volume_24h: optional(&t.vol24h),
            exchange_timestamp_ms: timestamp(&t.ts, now)?,
        },
        mark_price: MarkPriceState {
            price: number(&m.mark_px, true)?,
            exchange_timestamp_ms: timestamp(&m.ts, now)?,
        },
        index_price: IndexPriceState {
            index_id: underlying,
            price: number(&i.idx_px, true)?,
            exchange_timestamp_ms: timestamp(&i.ts, now)?,
        },
        funding,
        open_interest: OpenInterestState {
            contracts: number(&o.oi, false)?,
            currency: optional(&o.oi_ccy),
            usd: optional(&o.oi_usd),
            exchange_timestamp_ms: timestamp(&o.ts, now)?,
        },
    };
    // Match the observation contract: receipt time is not part of the content identity.
    #[derive(serde::Serialize)]
    struct Generation<'a> {
        schema: &'a str,
        instrument_id: &'a str,
        instrument_type: InstrumentType,
        underlying: &'a str,
        reference_generation: &'a str,
        ticker: &'a TickerState,
        mark_price: &'a MarkPriceState,
        index_price: &'a IndexPriceState,
        funding: &'a Option<FundingState>,
        open_interest: &'a OpenInterestState,
    }
    let generation = Generation {
        schema: &market.schema,
        instrument_id: &market.instrument_id,
        instrument_type: market.instrument_type,
        underlying: &market.underlying,
        reference_generation: &market.reference_generation,
        ticker: &market.ticker,
        mark_price: &market.mark_price,
        index_price: &market.index_price,
        funding: &market.funding,
        open_interest: &market.open_interest,
    };
    market.market_generation = digest(&serde_json::to_vec(&generation).map_err(|_| ERROR)?);
    Ok(json!({
        "schema": "okx.market.overview/v1",
        "market": market,
        "source_mode": "rest",
        "quality": "degraded",
        "reason": "STANDALONE_REST_NO_PUBLIC_WS",
        "reference_generation_scope": "single_instrument_rest_response",
        "public_ws_quality": {"reason_code": "PUBLIC_WS_QUALITY_UNAVAILABLE"},
    }))
}

pub(crate) fn websocket_overview(snapshot: &crate::public_ws::Snapshot) -> Value {
    json!({
        "schema": "okx.market.overview/v1",
        "source_mode": "websocket",
        "quality": "complete",
        "market": snapshot.market,
        "generation": snapshot.generation,
        "book_sequence": snapshot.book_sequence,
        "book_exchange_timestamp_ms": snapshot.book_exchange_timestamp_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn envelopes_and_identity_are_strict() {
        assert!(row(br#"{"code":"0","msg":"","data":[]}"#).is_err());
        assert!(row(br#"{"code":"1","msg":"","data":[{}]}"#).is_err());
        let r = row(br#"{"code":"0","msg":"","data":[{"instId":"BTC-USDT-SWAP"}]}"#).unwrap();
        assert!(identity(&r.inst_id, "ETH-USDT-SWAP").is_err());
    }
    #[test]
    fn prices_and_quantities_are_decimal_and_nonnegative() {
        assert!(number("1.25", true).is_ok());
        assert!(number("0", true).is_err());
        assert!(number("0", false).is_ok());
        assert!(number("-1", false).is_err());
        assert!(number("NaN", true).is_err());
    }
    #[test]
    fn rejects_stale_future_and_invalid_timestamps() {
        for ts in ["0", "bad", "1", "1006000"] {
            assert!(timestamp(ts, 1_000_000).is_err());
        }
        assert!(timestamp("999999", 1_000_000).is_ok());
    }
}

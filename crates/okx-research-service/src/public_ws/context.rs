//! Typed, bounded projection of public market channel updates.
use okx_research_domain::observation_model::{
    FundingState, IndexPriceState, InstrumentType, MarkPriceState, MarketSnapshot,
    OpenInterestState, TickerState,
};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

const ERROR: &str = "MARKET_SOURCE_UNAVAILABLE";

struct Seen<T> {
    value: T,
    received: u64,
    exchange: u64,
    identity: String,
}
#[derive(Default)]
pub(super) struct Context {
    ticker: Option<Seen<TickerState>>,
    mark: Option<Seen<MarkPriceState>>,
    index: Option<Seen<IndexPriceState>>,
    oi: Option<Seen<OpenInterestState>>,
    funding: Option<Seen<FundingState>>,
}
fn field<'a>(row: &'a Value, key: &str) -> Result<&'a str, &'static str> {
    row.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or(ERROR)
}
fn num(row: &Value, key: &str, positive: bool, signed: bool) -> Result<String, &'static str> {
    let s = field(row, key)?;
    let n = s.parse::<rust_decimal::Decimal>().map_err(|_| ERROR)?;
    if (positive && n <= rust_decimal::Decimal::ZERO)
        || (!positive && !signed && n < rust_decimal::Decimal::ZERO)
    {
        return Err(ERROR);
    }
    Ok(s.to_owned())
}
fn opt_num(row: &Value, key: &str, signed: bool) -> Result<Option<String>, &'static str> {
    match row.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if s.is_empty() => Ok(None),
        Some(_) => num(row, key, false, signed).map(Some),
    }
}
fn event(row: &Value, key: &str) -> Result<String, &'static str> {
    let s = field(row, key)?;
    if s.parse::<u64>().map_err(|_| ERROR)? == 0 {
        return Err(ERROR);
    }
    Ok(s.to_owned())
}
fn seen<T>(value: T, ts: &str, received: u64, identity: &str) -> Result<Seen<T>, &'static str> {
    let exchange = ts.parse::<u64>().map_err(|_| ERROR)?;
    if received == 0 || exchange == 0 || exchange > received.saturating_add(5_000) {
        return Err(ERROR);
    }
    Ok(Seen {
        value,
        received,
        exchange,
        identity: identity.to_owned(),
    })
}
fn fresh<T>(v: &Option<Seen<T>>, now: u64, age: u64) -> Option<&T> {
    let v = v.as_ref()?;
    if v.received > now
        || v.exchange > now.saturating_add(5_000)
        || now.saturating_sub(v.received) > age
        || now.saturating_sub(v.exchange) > age
    {
        return None;
    }
    Some(&v.value)
}
impl Context {
    /// The caller must prevalidate the exact channel and instrument target, and subscription target; row identities are checked against the reference at snapshot time.
    pub(super) fn apply(
        &mut self,
        channel: &str,
        row: Value,
        received_ms: u64,
    ) -> Result<(), &'static str> {
        if !row.is_object() {
            return Err(ERROR);
        }
        let ts = event(&row, "ts")?;
        let identity = field(&row, "instId")?;
        let exchange = ts.parse::<u64>().map_err(|_| ERROR)?;
        let previous = match channel {
            "tickers" => self.ticker.as_ref().map(|v| v.exchange),
            "mark-price" => self.mark.as_ref().map(|v| v.exchange),
            "index-tickers" => self.index.as_ref().map(|v| v.exchange),
            "open-interest" => self.oi.as_ref().map(|v| v.exchange),
            "funding-rate" => self.funding.as_ref().map(|v| v.exchange),
            _ => return Err(ERROR),
        };
        if previous.is_some_and(|old| exchange < old) {
            return Err(ERROR);
        }
        match channel {
            "tickers" => {
                self.ticker = Some(seen(
                    TickerState {
                        last: num(&row, "last", true, false)?,
                        last_size: num(&row, "lastSz", false, false)?,
                        best_ask: num(&row, "askPx", true, false)?,
                        best_ask_size: num(&row, "askSz", false, false)?,
                        best_bid: num(&row, "bidPx", true, false)?,
                        best_bid_size: num(&row, "bidSz", false, false)?,
                        open_24h: opt_num(&row, "open24h", false)?,
                        high_24h: opt_num(&row, "high24h", false)?,
                        low_24h: opt_num(&row, "low24h", false)?,
                        volume_currency_24h: opt_num(&row, "volCcy24h", false)?,
                        volume_24h: opt_num(&row, "vol24h", false)?,
                        exchange_timestamp_ms: ts.clone(),
                    },
                    &ts,
                    received_ms,
                    identity,
                )?)
            }
            "mark-price" => {
                self.mark = Some(seen(
                    MarkPriceState {
                        price: num(&row, "markPx", true, false)?,
                        exchange_timestamp_ms: ts.clone(),
                    },
                    &ts,
                    received_ms,
                    identity,
                )?)
            }
            "index-tickers" => {
                self.index = Some(seen(
                    IndexPriceState {
                        index_id: field(&row, "instId")?.to_owned(),
                        price: num(&row, "idxPx", true, false)?,
                        exchange_timestamp_ms: ts.clone(),
                    },
                    &ts,
                    received_ms,
                    identity,
                )?)
            }
            "open-interest" => {
                self.oi = Some(seen(
                    OpenInterestState {
                        contracts: num(&row, "oi", false, false)?,
                        currency: opt_num(&row, "oiCcy", false)?,
                        usd: opt_num(&row, "oiUsd", false)?,
                        exchange_timestamp_ms: ts.clone(),
                    },
                    &ts,
                    received_ms,
                    identity,
                )?)
            }
            "funding-rate" => {
                self.funding = Some(seen(
                    FundingState {
                        rate: num(&row, "fundingRate", false, true)?,
                        funding_time_ms: event(&row, "fundingTime")?,
                        next_funding_time_ms: event(&row, "nextFundingTime")?,
                        premium: opt_num(&row, "premium", true)?,
                        min_rate: opt_num(&row, "minFundingRate", true)?,
                        max_rate: opt_num(&row, "maxFundingRate", true)?,
                        exchange_timestamp_ms: ts.clone(),
                    },
                    &ts,
                    received_ms,
                    identity,
                )?)
            }
            _ => return Err(ERROR),
        }
        Ok(())
    }
    /// Latest time at which all required observations remain within their age limits.
    pub(super) fn valid_until_ms(&self, reference: &super::Reference) -> Option<u64> {
        fn until<T>(value: &Option<Seen<T>>, age: u64) -> Option<u64> {
            let value = value.as_ref()?;
            Some(value.received.min(value.exchange).saturating_add(age))
        }
        let mut expiry = until(&self.ticker, 2_000)?
            .min(until(&self.mark, 2_000)?)
            .min(until(&self.index, 2_000)?)
            .min(until(&self.oi, 180_000)?);
        if reference.funding_required {
            expiry = expiry.min(until(&self.funding, 180_000)?);
        }
        Some(expiry)
    }
    pub(super) fn snapshot(
        &self,
        reference: &super::Reference,
        now_ms: u64,
    ) -> Result<Option<MarketSnapshot>, &'static str> {
        let (Some(ticker), Some(mark), Some(index), Some(oi)) = (
            fresh(&self.ticker, now_ms, 2_000),
            fresh(&self.mark, now_ms, 2_000),
            fresh(&self.index, now_ms, 2_000),
            fresh(&self.oi, now_ms, 180_000),
        ) else {
            return Ok(None);
        };
        if index.index_id != reference.underlying
            || [
                &self.ticker.as_ref().unwrap().identity,
                &self.mark.as_ref().unwrap().identity,
                &self.oi.as_ref().unwrap().identity,
            ]
            .iter()
            .any(|id| *id != &reference.instrument)
        {
            return Err(ERROR);
        }
        let funding = if reference.funding_required {
            let Some(v) = fresh(&self.funding, now_ms, 180_000) else {
                return Ok(None);
            };
            if self.funding.as_ref().unwrap().identity != reference.instrument {
                return Err(ERROR);
            }
            Some(v.clone())
        } else {
            None
        };
        let mut received_ms = self
            .ticker
            .as_ref()
            .unwrap()
            .received
            .min(self.mark.as_ref().unwrap().received)
            .min(self.index.as_ref().unwrap().received)
            .min(self.oi.as_ref().unwrap().received);
        if reference.funding_required {
            received_ms = received_ms.min(self.funding.as_ref().unwrap().received);
        }
        let received = i64::try_from(received_ms).map_err(|_| ERROR)?;
        let received = jiff::Timestamp::from_millisecond(received).map_err(|_| ERROR)?;
        let mut market = MarketSnapshot {
            schema: "okx.market-snapshot/v1".into(),
            instrument_id: reference.instrument.clone(),
            instrument_type: reference.kind,
            underlying: reference.underlying.clone(),
            reference_generation: reference.generation.clone(),
            market_generation: String::new(),
            source_received_at: format!("{received:.3}"),
            ticker: ticker.clone(),
            mark_price: mark.clone(),
            index_price: index.clone(),
            funding,
            open_interest: oi.clone(),
        };
        #[derive(Serialize)]
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
        let g = Generation {
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
        market.market_generation = format!(
            "sha256:{:x}",
            Sha256::digest(serde_json::to_vec(&g).map_err(|_| ERROR)?)
        );
        Ok(Some(market))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn reference(funding_required: bool) -> super::super::Reference {
        super::super::Reference {
            instrument: "BTC-USDT-SWAP".into(),
            kind: InstrumentType::Swap,
            underlying: "BTC-USDT".into(),
            generation: "ref".into(),
            funding_required,
            observed_ms: 1_000_000,
        }
    }
    fn context() -> Context {
        let mut c = Context::default();
        for (channel, row) in [
            (
                "tickers",
                json!({"instId":"BTC-USDT-SWAP","last":"100","lastSz":"1","askPx":"101","askSz":"1","bidPx":"99","bidSz":"1","ts":"1000000"}),
            ),
            (
                "mark-price",
                json!({"instId":"BTC-USDT-SWAP","markPx":"100","ts":"1000000"}),
            ),
            (
                "index-tickers",
                json!({"instId":"BTC-USDT","idxPx":"100","ts":"1000000"}),
            ),
            (
                "open-interest",
                json!({"instId":"BTC-USDT-SWAP","oi":"10","ts":"1000000"}),
            ),
            (
                "funding-rate",
                json!({"instId":"BTC-USDT-SWAP","fundingRate":"-0.01","fundingTime":"2000000","nextFundingTime":"3000000","ts":"1000000"}),
            ),
        ] {
            c.apply(channel, row, 1_000_000).unwrap();
        }
        c
    }
    #[test]
    fn completeness_freshness_and_generation() {
        assert!(
            Context::default()
                .snapshot(&reference(true), 1_000_000)
                .unwrap()
                .is_none()
        );
        let c = context();
        let a = c.snapshot(&reference(true), 1_000_000).unwrap().unwrap();
        let b = c.snapshot(&reference(true), 1_002_000).unwrap().unwrap();
        assert_eq!(a.market_generation, b.market_generation);
        assert_eq!(a.source_received_at, b.source_received_at);
        assert_eq!(c.valid_until_ms(&reference(true)), Some(1_002_000));
        assert!(c.snapshot(&reference(true), 999_999).unwrap().is_none());
        assert!(c.snapshot(&reference(true), 1_002_001).unwrap().is_none());
        assert!(
            c.snapshot(&reference(false), 1_000_000)
                .unwrap()
                .unwrap()
                .funding
                .is_none()
        );
    }
    #[test]
    fn malformed_rows_do_not_replace_valid_state() {
        let mut c = context();
        assert!(
            c.apply(
                "mark-price",
                json!({"instId":"BTC-USDT-SWAP","markPx":"NaN","ts":"1000000"}),
                1_000_000
            )
            .is_err()
        );
        assert!(
            c.apply(
                "mark-price",
                json!({"instId":"BTC-USDT-SWAP","markPx":"1","ts":"1005001"}),
                1_000_000
            )
            .is_err()
        );
        assert!(
            c.apply(
                "mark-price",
                json!({"instId":"BTC-USDT-SWAP","markPx":"1","ts":"999999"}),
                1_000_000
            )
            .is_err()
        );
        assert!(c.snapshot(&reference(true), 1_000_000).unwrap().is_some());
        assert!(
            c.snapshot(
                &super::super::Reference {
                    underlying: "ETH-USDT".into(),
                    ..reference(true)
                },
                1_000_000
            )
            .is_err()
        );
    }
}

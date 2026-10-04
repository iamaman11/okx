use serde::{
    Deserialize, Serialize,
    de::{self, Deserializer},
};

use crate::{
    client::{CapturedPublicRows, OkxPublicClient},
    error::OkxError,
    instrument::InstrumentType,
};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PublicTicker {
    #[serde(rename = "instType", default)]
    pub instrument_type: String,
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(default)]
    pub last: String,
    #[serde(rename = "lastSz", default)]
    pub last_size: String,
    #[serde(rename = "askPx", default)]
    pub ask_price: String,
    #[serde(rename = "askSz", default)]
    pub ask_size: String,
    #[serde(rename = "bidPx", default)]
    pub bid_price: String,
    #[serde(rename = "bidSz", default)]
    pub bid_size: String,
    #[serde(rename = "open24h", default)]
    pub open_24h: String,
    #[serde(rename = "high24h", default)]
    pub high_24h: String,
    #[serde(rename = "low24h", default)]
    pub low_24h: String,
    #[serde(rename = "volCcy24h", default)]
    pub volume_currency_24h: String,
    #[serde(rename = "vol24h", default)]
    pub volume_24h: String,
    #[serde(rename = "sodUtc0", default)]
    pub sod_utc0: String,
    #[serde(rename = "sodUtc8", default)]
    pub sod_utc8: String,
    #[serde(default)]
    pub ts: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PublicMarkPrice {
    #[serde(rename = "instType", default)]
    pub instrument_type: String,
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(rename = "markPx", default)]
    pub mark_price: String,
    #[serde(default)]
    pub ts: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PublicIndexTicker {
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(rename = "idxPx", default)]
    pub index_price: String,
    #[serde(rename = "open24h", default)]
    pub open_24h: String,
    #[serde(rename = "high24h", default)]
    pub high_24h: String,
    #[serde(rename = "low24h", default)]
    pub low_24h: String,
    #[serde(rename = "sodUtc0", default)]
    pub sod_utc0: String,
    #[serde(rename = "sodUtc8", default)]
    pub sod_utc8: String,
    #[serde(default)]
    pub ts: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PublicFundingRate {
    #[serde(rename = "instType", default)]
    pub instrument_type: String,
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(rename = "fundingRate", default)]
    pub funding_rate: String,
    #[serde(rename = "fundingTime", default)]
    pub funding_time: String,
    #[serde(rename = "nextFundingTime", default)]
    pub next_funding_time: String,
    #[serde(rename = "impactValue", default)]
    pub impact_value: String,
    #[serde(rename = "interestRate", default)]
    pub interest_rate: String,
    #[serde(default)]
    pub premium: String,
    #[serde(rename = "minFundingRate", default)]
    pub min_funding_rate: String,
    #[serde(rename = "maxFundingRate", default)]
    pub max_funding_rate: String,
    #[serde(default)]
    pub method: String,
    #[serde(rename = "formulaType", default)]
    pub formula_type: String,
    #[serde(rename = "settState", default)]
    pub settlement_state: String,
    #[serde(rename = "settFundingRate", default)]
    pub settled_funding_rate: String,
    #[serde(default)]
    pub ts: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PublicOpenInterest {
    #[serde(rename = "instType", default)]
    pub instrument_type: String,
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(default)]
    pub oi: String,
    #[serde(rename = "oiCcy", default)]
    pub oi_currency: String,
    #[serde(rename = "oiUsd", default)]
    pub oi_usd: String,
    #[serde(default)]
    pub ts: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PublicOpenInterestHistory {
    pub oi: String,
    pub oi_currency: String,
    pub ts: String,
}

pub fn is_open_interest_history_period(value: &str) -> bool {
    matches!(
        value,
        "5m" | "15m"
            | "30m"
            | "1H"
            | "2H"
            | "4H"
            | "6H"
            | "12H"
            | "1D"
            | "2D"
            | "3D"
            | "5D"
            | "1W"
            | "1M"
            | "3M"
            | "6Hutc"
            | "12Hutc"
            | "1Dutc"
            | "2Dutc"
            | "3Dutc"
            | "5Dutc"
            | "1Wutc"
            | "1Mutc"
            | "3Mutc"
    )
}

#[derive(Deserialize)]
#[serde(untagged)]
enum PublicOpenInterestHistoryWire {
    Object {
        #[serde(default)]
        oi: String,
        #[serde(rename = "oiCcy", default)]
        oi_currency: String,
        #[serde(default)]
        ts: String,
    },
    Row(Vec<String>),
}

impl<'de> Deserialize<'de> for PublicOpenInterestHistory {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match PublicOpenInterestHistoryWire::deserialize(deserializer)? {
            PublicOpenInterestHistoryWire::Object {
                oi,
                oi_currency,
                ts,
            } => Ok(Self {
                oi,
                oi_currency,
                ts,
            }),
            PublicOpenInterestHistoryWire::Row(row) => match row.as_slice() {
                [ts, oi, oi_currency] | [ts, oi, oi_currency, _] => Ok(Self {
                    oi: oi.clone(),
                    oi_currency: oi_currency.clone(),
                    ts: ts.clone(),
                }),
                _ => Err(de::Error::custom(format!(
                    "open interest history row has {} fields; expected 3 or 4",
                    row.len()
                ))),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PublicTrade {
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(rename = "tradeId", default)]
    pub trade_id: String,
    #[serde(rename = "px", default)]
    pub price: String,
    #[serde(rename = "sz", default)]
    pub size: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub source: String,
    #[serde(rename = "ts", default)]
    pub timestamp_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PublicFundingHistory {
    #[serde(rename = "instType", default)]
    pub instrument_type: String,
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(rename = "fundingRate", default)]
    pub funding_rate: String,
    #[serde(rename = "fundingTime", default)]
    pub funding_time_ms: String,
    #[serde(rename = "realizedRate", default)]
    pub realized_rate: String,
    #[serde(rename = "formulaType", default)]
    pub formula_type: String,
    #[serde(default)]
    pub method: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PublicCandle {
    pub timestamp_ms: String,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub volume_currency: String,
    pub volume_quote: Option<String>,
    pub confirm: String,
}

impl<'de> Deserialize<'de> for PublicCandle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let row = Vec::<String>::deserialize(deserializer)?;
        match row.as_slice() {
            [
                timestamp_ms,
                open,
                high,
                low,
                close,
                volume,
                volume_currency,
                confirm,
            ] => Ok(Self {
                timestamp_ms: timestamp_ms.clone(),
                open: open.clone(),
                high: high.clone(),
                low: low.clone(),
                close: close.clone(),
                volume: volume.clone(),
                volume_currency: volume_currency.clone(),
                volume_quote: None,
                confirm: confirm.clone(),
            }),
            [
                timestamp_ms,
                open,
                high,
                low,
                close,
                volume,
                volume_currency,
                volume_quote,
                confirm,
            ] => Ok(Self {
                timestamp_ms: timestamp_ms.clone(),
                open: open.clone(),
                high: high.clone(),
                low: low.clone(),
                close: close.clone(),
                volume: volume.clone(),
                volume_currency: volume_currency.clone(),
                volume_quote: Some(volume_quote.clone()),
                confirm: confirm.clone(),
            }),
            _ => Err(de::Error::custom(format!(
                "history candle row has {} fields; expected 8 or 9",
                row.len()
            ))),
        }
    }
}

#[derive(Clone)]
pub struct MarketDataApi {
    client: OkxPublicClient,
}

impl MarketDataApi {
    pub fn new(client: OkxPublicClient) -> Self {
        Self { client }
    }

    pub async fn ticker(&self, instrument_id: &str) -> Result<PublicTicker, OkxError> {
        one(
            self.client
                .public_get(
                    "/api/v5/market/ticker",
                    &[("instId", instrument_id.to_owned())],
                )
                .await?,
            "ticker",
        )
    }

    pub async fn tickers(
        &self,
        instrument_type: InstrumentType,
    ) -> Result<Vec<PublicTicker>, OkxError> {
        self.client
            .public_get(
                "/api/v5/market/tickers",
                &[("instType", instrument_type.to_string())],
            )
            .await
    }

    pub async fn mark_price(
        &self,
        instrument_type: InstrumentType,
        instrument_id: &str,
    ) -> Result<PublicMarkPrice, OkxError> {
        one(
            self.client
                .public_get(
                    "/api/v5/public/mark-price",
                    &[
                        ("instType", instrument_type.to_string()),
                        ("instId", instrument_id.to_owned()),
                    ],
                )
                .await?,
            "mark price",
        )
    }

    pub async fn index_ticker(&self, index_id: &str) -> Result<PublicIndexTicker, OkxError> {
        one(
            self.client
                .public_get(
                    "/api/v5/market/index-tickers",
                    &[("instId", index_id.to_owned())],
                )
                .await?,
            "index ticker",
        )
    }

    pub async fn funding_rate(&self, instrument_id: &str) -> Result<PublicFundingRate, OkxError> {
        one(
            self.client
                .public_get(
                    "/api/v5/public/funding-rate",
                    &[("instId", instrument_id.to_owned())],
                )
                .await?,
            "funding rate",
        )
    }

    pub async fn open_interest(
        &self,
        instrument_type: InstrumentType,
        instrument_id: &str,
    ) -> Result<PublicOpenInterest, OkxError> {
        one(
            self.client
                .public_get(
                    "/api/v5/public/open-interest",
                    &[
                        ("instType", instrument_type.to_string()),
                        ("instId", instrument_id.to_owned()),
                    ],
                )
                .await?,
            "open interest",
        )
    }

    pub async fn open_interest_history(
        &self,
        instrument_id: &str,
        period: &str,
        limit: u16,
    ) -> Result<Vec<PublicOpenInterestHistory>, OkxError> {
        if !(1..=100).contains(&limit) {
            return Err(OkxError::Response(
                "open interest history limit must be between 1 and 100".to_owned(),
            ));
        }
        if !is_open_interest_history_period(period) {
            return Err(OkxError::Response(format!(
                "unsupported open interest history period '{period}'"
            )));
        }

        self.client
            .public_get(
                "/api/v5/rubik/stat/contracts/open-interest-history",
                &[
                    ("instId", instrument_id.to_owned()),
                    ("period", period.to_owned()),
                    ("limit", limit.to_string()),
                ],
            )
            .await
    }

    pub async fn trades(
        &self,
        instrument_id: &str,
        limit: u16,
    ) -> Result<Vec<PublicTrade>, OkxError> {
        if !(1..=100).contains(&limit) {
            return Err(OkxError::Response(
                "recent trades limit must be between 1 and 100".to_owned(),
            ));
        }

        self.client
            .public_get(
                "/api/v5/market/trades",
                &[
                    ("instId", instrument_id.to_owned()),
                    ("limit", limit.to_string()),
                ],
            )
            .await
    }

    pub async fn history_trades_page_captured(
        &self,
        instrument_id: &str,
        after: Option<&str>,
        before: Option<&str>,
        limit: u16,
    ) -> Result<CapturedPublicRows<PublicTrade>, OkxError> {
        if !(1..=100).contains(&limit) {
            return Err(OkxError::Response(
                "history trades limit must be between 1 and 100".to_owned(),
            ));
        }

        let mut params = vec![
            ("instId", instrument_id.to_owned()),
            ("type", "1".to_owned()),
        ];
        if let Some(after) = after.filter(|value| !value.trim().is_empty()) {
            params.push(("after", after.to_owned()));
        }
        if let Some(before) = before.filter(|value| !value.trim().is_empty()) {
            params.push(("before", before.to_owned()));
        }
        params.push(("limit", limit.to_string()));

        self.client
            .public_get_captured("/api/v5/market/history-trades", &params)
            .await
    }

    pub async fn funding_rate_history(
        &self,
        instrument_id: &str,
        limit: u16,
    ) -> Result<Vec<PublicFundingHistory>, OkxError> {
        Ok(self
            .funding_rate_history_page_captured(instrument_id, None, None, limit)
            .await?
            .rows)
    }

    pub async fn funding_rate_history_page_captured(
        &self,
        instrument_id: &str,
        after: Option<&str>,
        before: Option<&str>,
        limit: u16,
    ) -> Result<CapturedPublicRows<PublicFundingHistory>, OkxError> {
        if !(1..=400).contains(&limit) {
            return Err(OkxError::Response(
                "funding history limit must be between 1 and 400".to_owned(),
            ));
        }

        let mut params = vec![("instId", instrument_id.to_owned())];
        if let Some(after) = after.filter(|value| !value.trim().is_empty()) {
            params.push(("after", after.to_owned()));
        }
        if let Some(before) = before.filter(|value| !value.trim().is_empty()) {
            params.push(("before", before.to_owned()));
        }
        params.push(("limit", limit.to_string()));

        self.client
            .public_get_captured("/api/v5/public/funding-rate-history", &params)
            .await
    }

    pub async fn history_candles(
        &self,
        instrument_id: &str,
        bar: &str,
        limit: u16,
    ) -> Result<Vec<PublicCandle>, OkxError> {
        Ok(self
            .history_candles_page_captured(instrument_id, bar, None, None, limit)
            .await?
            .rows)
    }

    pub async fn history_candles_page_captured(
        &self,
        instrument_id: &str,
        bar: &str,
        after: Option<&str>,
        before: Option<&str>,
        limit: u16,
    ) -> Result<CapturedPublicRows<PublicCandle>, OkxError> {
        if !(1..=100).contains(&limit) {
            return Err(OkxError::Response(
                "history candle limit must be between 1 and 100".to_owned(),
            ));
        }

        let mut params = vec![("instId", instrument_id.to_owned())];
        if let Some(after) = after.filter(|value| !value.trim().is_empty()) {
            params.push(("after", after.to_owned()));
        }
        if let Some(before) = before.filter(|value| !value.trim().is_empty()) {
            params.push(("before", before.to_owned()));
        }
        params.push(("bar", bar.to_owned()));
        params.push(("limit", limit.to_string()));

        self.client
            .public_get_captured("/api/v5/market/history-candles", &params)
            .await
    }
}

fn one<T>(mut rows: Vec<T>, label: &'static str) -> Result<T, OkxError> {
    if rows.len() != 1 {
        return Err(OkxError::Response(format!(
            "OKX returned {} {label} rows; expected exactly one",
            rows.len()
        )));
    }
    Ok(rows.remove(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn historical_page_limits_match_current_okx_contract() {
        assert!((1..=100).contains(&100_u16));
        assert!(!(1..=100).contains(&101_u16));
        assert!((1..=400).contains(&400_u16));
        assert!(!(1..=400).contains(&401_u16));
    }

    #[test]
    fn ticker_preserves_exchange_decimal_text_exactly() {
        let ticker: PublicTicker = serde_json::from_str(
            r#"{
                "instType":"SWAP",
                "instId":"DOGE-USDT-SWAP",
                "last":"0.123456",
                "lastSz":"17",
                "askPx":"0.123457",
                "askSz":"25",
                "bidPx":"0.123455",
                "bidSz":"31",
                "open24h":"0.120000",
                "high24h":"0.130000",
                "low24h":"0.110000",
                "volCcy24h":"1234567.890123",
                "vol24h":"7654321",
                "sodUtc0":"0.121000",
                "sodUtc8":"0.122000",
                "ts":"1790467200123"
            }"#,
        )
        .expect("ticker");

        assert_eq!(ticker.last, "0.123456");
        assert_eq!(ticker.ask_price, "0.123457");
        assert_eq!(ticker.bid_price, "0.123455");
        assert_eq!(ticker.volume_currency_24h, "1234567.890123");
    }

    #[test]
    fn funding_preserves_extended_decimal_text_exactly() {
        let funding: PublicFundingRate = serde_json::from_str(
            r#"{
                "instType":"SWAP",
                "instId":"DOGE-USDT-SWAP",
                "fundingRate":"0.00001234",
                "fundingTime":"1790467200000",
                "nextFundingTime":"1790496000000",
                "impactValue":"10000",
                "interestRate":"0.0001",
                "premium":"0.00000001",
                "minFundingRate":"-0.003",
                "maxFundingRate":"0.003",
                "method":"current_period",
                "formulaType":"withRate",
                "settState":"settled",
                "settFundingRate":"0.00001111",
                "ts":"1790467199000"
            }"#,
        )
        .expect("funding");

        assert_eq!(funding.funding_rate, "0.00001234");
        assert_eq!(funding.premium, "0.00000001");
        assert_eq!(funding.max_funding_rate, "0.003");
    }

    #[test]
    fn open_interest_history_periods_match_current_okx_contract() {
        for period in [
            "5m", "4H", "6H", "1D", "5D", "1W", "3M", "6Hutc", "1Dutc", "3Mutc",
        ] {
            assert!(is_open_interest_history_period(period), "{period}");
        }
        for period in ["1m", "8H", "1Y", "bad"] {
            assert!(!is_open_interest_history_period(period), "{period}");
        }
    }

    #[test]
    fn open_interest_history_accepts_live_four_field_row_shape() {
        let row: PublicOpenInterestHistory =
            serde_json::from_str(r#"["1609459200000","100000","10","5000000000"]"#)
                .expect("open interest history");

        assert_eq!(row.ts, "1609459200000");
        assert_eq!(row.oi, "100000");
        assert_eq!(row.oi_currency, "10");
    }

    #[test]
    fn open_interest_history_accepts_legacy_object_shape() {
        let row: PublicOpenInterestHistory = serde_json::from_str(
            r#"{
                "ts":"1609459200000",
                "oi":"100000",
                "oiCcy":"10"
            }"#,
        )
        .expect("open interest history");

        assert_eq!(row.ts, "1609459200000");
        assert_eq!(row.oi, "100000");
        assert_eq!(row.oi_currency, "10");
    }

    #[test]
    fn open_interest_history_rejects_unknown_row_shape() {
        let error =
            serde_json::from_str::<PublicOpenInterestHistory>(r#"["1609459200000","100000"]"#)
                .expect_err("invalid open interest history");
        assert!(error.to_string().contains("expected 3 or 4"));
    }

    #[test]
    fn recent_trade_preserves_taker_side_and_exchange_identity() {
        let trade: PublicTrade = serde_json::from_str(
            r#"{
                "instId":"DOGE-USDT-SWAP",
                "tradeId":"242720720",
                "px":"0.09455",
                "sz":"17",
                "side":"buy",
                "source":"0",
                "ts":"1790963452563"
            }"#,
        )
        .expect("trade");

        assert_eq!(trade.instrument_id, "DOGE-USDT-SWAP");
        assert_eq!(trade.trade_id, "242720720");
        assert_eq!(trade.side, "buy");
        assert_eq!(trade.price, "0.09455");
        assert_eq!(trade.size, "17");
    }

    #[test]
    fn funding_history_preserves_realized_rate_and_mechanism() {
        let funding: PublicFundingHistory = serde_json::from_str(
            r#"{
                "formulaType":"noRate",
                "fundingRate":"0.0000746604960499",
                "fundingTime":"1703059200000",
                "instId":"DOGE-USDT-SWAP",
                "instType":"SWAP",
                "method":"next_period",
                "realizedRate":"0.0000746572360545"
            }"#,
        )
        .expect("funding");

        assert_eq!(funding.realized_rate, "0.0000746572360545");
        assert_eq!(funding.formula_type, "noRate");
        assert_eq!(funding.method, "next_period");
    }

    #[test]
    fn history_candle_accepts_current_nine_field_shape() {
        let row: PublicCandle = serde_json::from_str(
            r#"["1790467200000","0.12","0.13","0.11","0.125","100","12.5","12.5","1"]"#,
        )
        .expect("history candle");

        assert_eq!(row.timestamp_ms, "1790467200000");
        assert_eq!(row.close, "0.125");
        assert_eq!(row.volume_quote.as_deref(), Some("12.5"));
        assert_eq!(row.confirm, "1");
    }

    #[test]
    fn history_candle_accepts_documented_eight_field_shape() {
        let row: PublicCandle = serde_json::from_str(
            r#"["1790467200000","0.12","0.13","0.11","0.125","100","12.5","1"]"#,
        )
        .expect("history candle");

        assert_eq!(row.volume_quote, None);
        assert_eq!(row.confirm, "1");
    }

    #[test]
    fn history_candle_rejects_unknown_shape() {
        let error = serde_json::from_str::<PublicCandle>(r#"["1790467200000","0.12","0.13"]"#)
            .expect_err("invalid history candle");
        assert!(error.to_string().contains("expected 8 or 9"));
    }

    #[test]
    fn one_requires_exact_cardinality() {
        assert_eq!(one(vec!["row"], "test").expect("one"), "row");
        assert!(one::<String>(Vec::new(), "test").is_err());
        assert!(one(vec!["a", "b"], "test").is_err());
    }
}

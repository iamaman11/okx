use serde::{Deserialize, Serialize};

use crate::{client::OkxPublicClient, error::OkxError, instrument::InstrumentType};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Ticker {
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
    #[serde(default)]
    pub ts: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct MarkPrice {
    #[serde(rename = "instType", default)]
    pub instrument_type: String,
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(rename = "instFamily", default)]
    pub instrument_family: String,
    #[serde(rename = "markPx", default)]
    pub mark_price: String,
    #[serde(default)]
    pub ts: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct IndexTicker {
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(rename = "idxPx", default)]
    pub index_price: String,
    #[serde(rename = "high24h", default)]
    pub high_24h: String,
    #[serde(rename = "low24h", default)]
    pub low_24h: String,
    #[serde(rename = "open24h", default)]
    pub open_24h: String,
    #[serde(default)]
    pub ts: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct FundingRate {
    #[serde(rename = "instType", default)]
    pub instrument_type: String,
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(default)]
    pub method: String,
    #[serde(rename = "formulaType", default)]
    pub formula_type: String,
    #[serde(rename = "fundingRate", default)]
    pub funding_rate: String,
    #[serde(rename = "fundingTime", default)]
    pub funding_time: String,
    #[serde(rename = "nextFundingTime", default)]
    pub next_funding_time: String,
    #[serde(rename = "minFundingRate", default)]
    pub min_funding_rate: String,
    #[serde(rename = "maxFundingRate", default)]
    pub max_funding_rate: String,
    #[serde(rename = "interestRate", default)]
    pub interest_rate: String,
    #[serde(default)]
    pub premium: String,
    #[serde(default)]
    pub ts: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct OpenInterest {
    #[serde(rename = "instType", default)]
    pub instrument_type: String,
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(default)]
    pub oi: String,
    #[serde(rename = "oiCcy", default)]
    pub open_interest_currency: String,
    #[serde(rename = "oiUsd", default)]
    pub open_interest_usd: String,
    #[serde(default)]
    pub ts: String,
}

#[derive(Clone)]
pub struct MarketDataApi {
    client: OkxPublicClient,
}

impl MarketDataApi {
    pub fn new(client: OkxPublicClient) -> Self {
        Self { client }
    }

    pub async fn ticker(&self, instrument_id: &str) -> Result<Ticker, OkxError> {
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

    pub async fn mark_price(
        &self,
        instrument_type: InstrumentType,
        instrument_id: &str,
    ) -> Result<MarkPrice, OkxError> {
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

    pub async fn index_ticker(&self, index_id: &str) -> Result<IndexTicker, OkxError> {
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

    pub async fn funding_rate(&self, instrument_id: &str) -> Result<FundingRate, OkxError> {
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
    ) -> Result<OpenInterest, OkxError> {
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
}

fn one<T>(mut values: Vec<T>, resource: &'static str) -> Result<T, OkxError> {
    if values.len() != 1 {
        return Err(OkxError::Config(format!(
            "OKX returned {} {resource} rows; expected exactly one",
            values.len()
        )));
    }
    Ok(values.remove(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_wire_types_preserve_exact_exchange_decimal_text() {
        let ticker: Ticker = serde_json::from_str(
            r#"{
                "instType":"SWAP",
                "instId":"DOGE-USDT-SWAP",
                "last":"0.123456",
                "lastSz":"12.34",
                "askPx":"0.123457",
                "askSz":"100.01",
                "bidPx":"0.123455",
                "bidSz":"99.99",
                "open24h":"0.120000",
                "high24h":"0.130000",
                "low24h":"0.110000",
                "volCcy24h":"123456.789",
                "vol24h":"987654.321",
                "ts":"1790460000123"
            }"#,
        )
        .expect("ticker");

        let funding: FundingRate = serde_json::from_str(
            r#"{
                "instType":"SWAP",
                "instId":"DOGE-USDT-SWAP",
                "method":"current_period",
                "formulaType":"withRate",
                "fundingRate":"0.000123456789",
                "fundingTime":"1790467200000",
                "nextFundingTime":"1790496000000",
                "minFundingRate":"-0.00375",
                "maxFundingRate":"0.00375",
                "interestRate":"0.0001",
                "premium":"0.000023456789",
                "ts":"1790460000456"
            }"#,
        )
        .expect("funding");

        assert_eq!(ticker.last, "0.123456");
        assert_eq!(ticker.bid_price, "0.123455");
        assert_eq!(funding.funding_rate, "0.000123456789");
        assert_eq!(funding.min_funding_rate, "-0.00375");
    }
}

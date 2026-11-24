//! Narrow public-data source capability supplied by the authenticated host.
use okx_ipc_contracts::research_ipc::*;
use rust_decimal::Decimal;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{future::Future, pin::Pin, sync::Arc};
pub type SourceFuture<'a> =
    Pin<Box<dyn Future<Output = Result<PublicSourceReply, String>> + Send + 'a>>;
pub trait PublicSourceProvider: Send + Sync {
    fn request(&self, request: PublicSourceRequest) -> SourceFuture<'_>;
}
#[derive(Clone)]
pub struct PublicSourceClient(Arc<dyn PublicSourceProvider>);
impl PublicSourceClient {
    pub fn new(provider: Arc<dyn PublicSourceProvider>) -> Self {
        Self(provider)
    }
    async fn request(&self, request: PublicSourceRequest) -> Result<PublicSourceReply, String> {
        crate::source_io::request_started(&request);
        let result = self.0.request(request.clone()).await;
        crate::source_io::request_finished(&request, &result);
        result
    }

    /// Research-owned normalization of the host's finite public GET catalog.
    /// New computations over these rows require no Agent or Engine code changes.
    pub async fn recent_trades(
        &self,
        instrument: &str,
        limit: u16,
    ) -> Result<ResearchTradePage, String> {
        self.trade_page("recent_trades", instrument, None, None, limit)
            .await
    }

    pub async fn history_trades(
        &self,
        instrument: &str,
        after: Option<&str>,
        before: Option<&str>,
        limit: u16,
    ) -> Result<ResearchTradePage, String> {
        self.trade_page("history_trades", instrument, after, before, limit)
            .await
    }

    async fn trade_page(
        &self,
        route: &'static str,
        instrument: &str,
        after: Option<&str>,
        before: Option<&str>,
        limit: u16,
    ) -> Result<ResearchTradePage, String> {
        let request = PublicSourceRequest::CatalogPage {
            route: route.into(),
            instrument: instrument.into(),
            after: after.map(str::to_owned),
            before: before.map(str::to_owned),
            limit,
        };
        if !request.valid() {
            return Err("INVALID_RESEARCH_SOURCE_REQUEST".into());
        }
        let PublicSourceReply::CatalogPage(capture) = self.request(request).await? else {
            return Err("RESEARCH_SOURCE_KIND_MISMATCH".into());
        };
        if capture.route != route
            || capture.rows.len() > usize::from(limit)
            || capture.raw_body.is_empty()
            || capture.raw_body.len() > MAX_FRAME_BYTES / 8
            || capture
                .acquired_at_ms
                .parse::<u64>()
                .ok()
                .filter(|ts| *ts > 0)
                .is_none()
            || capture.http_delivery.transport_send_invocations != 1
            || !(200..300).contains(&capture.http_delivery.response_status)
            || capture.http_delivery.response_body_bytes != capture.raw_body.len() as u64
        {
            return Err("RESEARCH_TRADE_SOURCE_RECEIPT_INVALID".into());
        }
        let raw: serde_json::Value = serde_json::from_slice(&capture.raw_body)
            .map_err(|_| "RESEARCH_TRADE_SOURCE_BODY_MISMATCH")?;
        if raw.get("code").and_then(serde_json::Value::as_str) != Some("0")
            || raw.get("data").and_then(serde_json::Value::as_array) != Some(&capture.rows)
        {
            return Err("RESEARCH_TRADE_SOURCE_BODY_MISMATCH".into());
        }
        let mut trades = Vec::with_capacity(capture.rows.len());
        for value in capture.rows {
            let row: WireTrade =
                serde_json::from_value(value).map_err(|_| "RESEARCH_TRADE_ROW_INVALID")?;
            if row.instrument != instrument
                || row.trade_id.is_empty()
                || !row.trade_id.bytes().all(|b| b.is_ascii_digit())
            {
                return Err("RESEARCH_TRADE_ROW_INVALID".into());
            }
            let timestamp_ms = row
                .timestamp_ms
                .parse::<u64>()
                .map_err(|_| "RESEARCH_TRADE_ROW_INVALID")?;
            let price =
                Decimal::from_str_exact(&row.price).map_err(|_| "RESEARCH_TRADE_ROW_INVALID")?;
            let size =
                Decimal::from_str_exact(&row.size).map_err(|_| "RESEARCH_TRADE_ROW_INVALID")?;
            if timestamp_ms == 0 || price <= Decimal::ZERO || size <= Decimal::ZERO {
                return Err("RESEARCH_TRADE_ROW_INVALID".into());
            }
            trades.push(ResearchTrade {
                instrument: row.instrument,
                trade_id: row.trade_id,
                timestamp_ms,
                price,
                size,
                side: row.side,
            });
        }
        Ok(ResearchTradePage {
            route,
            acquired_at_ms: capture.acquired_at_ms,
            raw_sha256: format!("{:x}", Sha256::digest(&capture.raw_body)),
            http_delivery: capture.http_delivery,
            trades,
        })
    }
    pub async fn research_candles_page(
        &self,
        instrument: &str,
        bar: &str,
        after: Option<&str>,
        before: Option<&str>,
        limit: u16,
    ) -> Result<PublicCandleCapture, String> {
        self.candles(instrument, bar, after, before, limit, false)
            .await
    }
    pub async fn analytics_candles_page(
        &self,
        instrument: &str,
        bar: &str,
        after: Option<&str>,
        before: Option<&str>,
        limit: u16,
    ) -> Result<PublicCandleCapture, String> {
        self.candles(instrument, bar, after, before, limit, true)
            .await
    }
    async fn candles(
        &self,
        instrument: &str,
        bar: &str,
        after: Option<&str>,
        before: Option<&str>,
        limit: u16,
        queued: bool,
    ) -> Result<PublicCandleCapture, String> {
        match self
            .request(PublicSourceRequest::Candles {
                instrument: instrument.into(),
                bar: bar.into(),
                after: after.map(str::to_owned),
                before: before.map(str::to_owned),
                limit,
                queued,
            })
            .await?
        {
            PublicSourceReply::Candles(value) => Ok(value),
            _ => Err("RESEARCH_SOURCE_KIND_MISMATCH".into()),
        }
    }
    pub async fn research_funding_page(
        &self,
        instrument: &str,
        after: Option<&str>,
        before: Option<&str>,
        limit: u16,
    ) -> Result<PublicFundingCapture, String> {
        match self
            .request(PublicSourceRequest::Funding {
                instrument: instrument.into(),
                after: after.map(str::to_owned),
                before: before.map(str::to_owned),
                limit,
            })
            .await?
        {
            PublicSourceReply::Funding(value) => Ok(value),
            _ => Err("RESEARCH_SOURCE_KIND_MISMATCH".into()),
        }
    }
    pub async fn research_current_reference(
        &self,
        instrument: &str,
    ) -> Result<PublicReferenceCapture, String> {
        match self
            .request(PublicSourceRequest::Reference {
                instrument: instrument.into(),
            })
            .await?
        {
            PublicSourceReply::Reference(value) => Ok(value),
            _ => Err("RESEARCH_SOURCE_KIND_MISMATCH".into()),
        }
    }
    pub async fn analytics_ticker(&self, instrument: &str) -> Result<ResearchTicker, String> {
        match self
            .request(PublicSourceRequest::Ticker {
                instrument: instrument.into(),
            })
            .await?
        {
            PublicSourceReply::Ticker(value) => Ok(value),
            _ => Err("RESEARCH_SOURCE_KIND_MISMATCH".into()),
        }
    }
    pub async fn analytics_oi(
        &self,
        instrument: &str,
        period: &str,
    ) -> Result<Vec<ResearchOpenInterest>, String> {
        match self
            .request(PublicSourceRequest::OpenInterest {
                instrument: instrument.into(),
                period: period.into(),
            })
            .await?
        {
            PublicSourceReply::OpenInterest(value) => Ok(value),
            _ => Err("RESEARCH_SOURCE_KIND_MISMATCH".into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResearchTrade {
    pub instrument: String,
    pub trade_id: String,
    pub timestamp_ms: u64,
    pub price: Decimal,
    pub size: Decimal,
    pub side: TradeSide,
}

#[derive(Debug, Clone)]
pub struct ResearchTradePage {
    pub route: &'static str,
    pub acquired_at_ms: String,
    pub raw_sha256: String,
    pub http_delivery: PublicSourceHttpDelivery,
    pub trades: Vec<ResearchTrade>,
}

#[derive(Deserialize)]
struct WireTrade {
    #[serde(rename = "instId")]
    instrument: String,
    #[serde(rename = "tradeId")]
    trade_id: String,
    #[serde(rename = "ts")]
    timestamp_ms: String,
    #[serde(rename = "px")]
    price: String,
    #[serde(rename = "sz")]
    size: String,
    side: TradeSide,
}

#[cfg(test)]
mod source_observation_tests {
    use super::*;

    struct Captured;
    impl PublicSourceProvider for Captured {
        fn request(&self, _: PublicSourceRequest) -> SourceFuture<'_> {
            Box::pin(async {
                Ok(PublicSourceReply::Candles(PublicCandleCapture {
                    raw_body: vec![0, 1, 2],
                    rows: Vec::new(),
                    acquired_at_ms: "1".into(),
                    http_delivery: None,
                }))
            })
        }
    }

    #[tokio::test]
    async fn proxy_observation_includes_real_adapter_call() {
        let client = PublicSourceClient::new(Arc::new(Captured));
        let meter = Arc::new(crate::source_io::SourceIoMeter::default());
        let capture = crate::source_io::observe(meter.clone(), async {
            client
                .research_candles_page("BTC-USDT-SWAP", "1m", None, None, 100)
                .await
        })
        .await
        .unwrap();
        assert_eq!(capture.raw_body.len(), 3);
        let observed = meter.snapshot();
        assert_eq!(
            observed["source_calls"]["candles"]["proxy_requests_started"],
            1
        );
        assert_eq!(observed["source_calls"]["candles"]["proxy_replies"], 1);
        assert_eq!(
            observed["source_calls"]["candles"]["captured_response_body_bytes"],
            3
        );
    }
}

#[cfg(test)]
mod catalog_adapter_tests {
    use super::*;
    use serde_json::{Value, json};
    use std::sync::Mutex;

    struct Catalog {
        calls: Mutex<Vec<PublicSourceRequest>>,
        route: &'static str,
        rows: Vec<Value>,
    }

    impl PublicSourceProvider for Catalog {
        fn request(&self, request: PublicSourceRequest) -> SourceFuture<'_> {
            self.calls.lock().unwrap().push(request);
            Box::pin(async move {
                let raw = serde_json::to_vec(&json!({"code":"0","data":self.rows})).unwrap();
                Ok(PublicSourceReply::CatalogPage(PublicCatalogCapture {
                    route: self.route.into(),
                    raw_body: raw.clone(),
                    rows: self.rows.clone(),
                    acquired_at_ms: "123456789".into(),
                    http_delivery: PublicSourceHttpDelivery {
                        transport_send_invocations: 1,
                        response_status: 200,
                        response_body_bytes: raw.len() as u64,
                    },
                }))
            })
        }
    }

    fn row() -> Value {
        json!({
            "instId": "BTC-USDT-SWAP", "tradeId": "123", "ts": "123456",
            "px": "100.125", "sz": "0.001", "side": "buy", "source": "0",
        })
    }

    #[tokio::test]
    async fn new_public_trade_adapter_normalizes_with_replay_provenance_and_source_diagnostics() {
        let provider = Arc::new(Catalog {
            calls: Mutex::new(Vec::new()),
            route: "history_trades",
            rows: vec![row()],
        });
        let client = PublicSourceClient::new(provider.clone());
        let meter = Arc::new(crate::source_io::SourceIoMeter::default());
        let page = crate::source_io::observe(meter.clone(), async {
            client
                .history_trades("BTC-USDT-SWAP", Some("98765"), None, 50)
                .await
        })
        .await
        .unwrap();
        assert_eq!(page.route, "history_trades");
        assert_eq!(page.acquired_at_ms, "123456789");
        assert_eq!(page.trades.len(), 1);
        assert_eq!(page.trades[0].trade_id, "123");
        assert_eq!(page.trades[0].timestamp_ms, 123456);
        assert_eq!(page.trades[0].side, TradeSide::Buy);
        assert_eq!(page.trades[0].price.to_string(), "100.125");
        assert_eq!(page.trades[0].size.to_string(), "0.001");
        assert_eq!(
            page.raw_sha256,
            format!(
                "{:x}",
                Sha256::digest(
                    serde_json::to_vec(&json!({"code":"0","data":provider.rows})).unwrap()
                )
            )
        );
        assert_eq!(page.http_delivery.transport_send_invocations, 1);
        assert_eq!(
            meter.snapshot()["source_calls"]["catalog_page"]["proxy_replies"],
            1
        );
        assert_eq!(
            meter.snapshot()["source_calls"]["catalog_page"]["host_transport_receipts"],
            1
        );
        assert!(matches!(
            provider.calls.lock().unwrap().as_slice(),
            [PublicSourceRequest::CatalogPage { route, after, limit, .. }]
                if route == "history_trades" && after.as_deref() == Some("98765") && *limit == 50
        ));
    }

    #[tokio::test]
    async fn source_adapter_rejects_invalid_params_before_ipc_and_dishonest_rows() {
        let provider = Arc::new(Catalog {
            calls: Mutex::new(Vec::new()),
            route: "recent_trades",
            rows: vec![row()],
        });
        let client = PublicSourceClient::new(provider.clone());
        assert_eq!(
            client
                .recent_trades("https://evil.invalid", 10)
                .await
                .unwrap_err(),
            "INVALID_RESEARCH_SOURCE_REQUEST"
        );
        assert_eq!(
            client
                .history_trades("BTC-USDT-SWAP", Some("1&route=evil"), None, 10)
                .await
                .unwrap_err(),
            "INVALID_RESEARCH_SOURCE_REQUEST"
        );
        assert_eq!(provider.calls.lock().unwrap().len(), 0);
        let parsed = client.recent_trades("BTC-USDT-SWAP", 100).await.unwrap();
        assert_eq!(parsed.trades.len(), 1);

        for (field, invalid) in [
            ("instId", json!("ETH-USDT-SWAP")),
            ("tradeId", json!("")),
            ("ts", json!("NaN")),
            ("px", json!("NaN")),
            ("px", json!("0")),
            ("sz", json!("-1")),
            ("side", json!("unknown")),
        ] {
            let mut bad = row();
            bad[field] = invalid;
            let faulty = PublicSourceClient::new(Arc::new(Catalog {
                calls: Mutex::new(Vec::new()),
                route: "recent_trades",
                rows: vec![bad],
            }));
            assert_eq!(
                faulty
                    .recent_trades("BTC-USDT-SWAP", 100)
                    .await
                    .unwrap_err(),
                "RESEARCH_TRADE_ROW_INVALID"
            );
        }
        let mismatched = PublicSourceClient::new(Arc::new(Catalog {
            calls: Mutex::new(Vec::new()),
            route: "history_trades",
            rows: vec![row()],
        }));
        assert_eq!(
            mismatched
                .recent_trades("BTC-USDT-SWAP", 100)
                .await
                .unwrap_err(),
            "RESEARCH_TRADE_SOURCE_RECEIPT_INVALID"
        );
    }
}

//! Bounded public closed-candle input for live strategy decisions.
use crate::market_bootstrap::MarketBootstrapper;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct LiveCandleSource {
    bootstrap: MarketBootstrapper,
    permits: Arc<Semaphore>,
}
impl LiveCandleSource {
    pub fn new(bootstrap: MarketBootstrapper) -> Self {
        Self {
            bootstrap,
            permits: Arc::new(Semaphore::new(8)),
        }
    }
    /// Close timestamps are the stable decision keys. Only confirmed bars in
    /// the current closed window are returned; source normalization is shared
    /// with all other public captures and retains the host's rate budget.
    pub async fn load_closed_closes(
        &self,
        instrument: &str,
        timeframe_ms: u64,
        limit: usize,
    ) -> Result<Vec<(u64, f64)>, String> {
        if limit == 0 || limit > 10_000 {
            return Err("LIVE_WARMUP_LIMIT:1..10000".into());
        }
        let bar = match timeframe_ms {
            60000 => "1m",
            180000 => "3m",
            300000 => "5m",
            900000 => "15m",
            1800000 => "30m",
            3600000 => "1H",
            14400000 => "4H",
            86400000 => "1Dutc",
            _ => return Err("UNSUPPORTED_TIMEFRAME".into()),
        };
        let now = jiff::Timestamp::now().as_millisecond().max(0) as u64;
        let end = now / timeframe_ms * timeframe_ms;
        let start = end.saturating_sub((limit as u64).saturating_mul(timeframe_ms));
        if end <= start || (end - start) / timeframe_ms > 1_000_000 {
            return Err("LIMIT_BARS:1000000".into());
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            let _permit = self.permits.acquire().await.map_err(|e| e.to_string())?;
            let mut cursor = end.to_string();
            let mut rows = BTreeMap::new();
            loop {
                let page = self
                    .bootstrap
                    .research_candles_page(instrument, bar, Some(&cursor), None, 100)
                    .await
                    .map_err(|e| e.to_string())?;
                if page.rows.is_empty() {
                    break;
                }
                let mut oldest = u64::MAX;
                for row in page.rows {
                    let t = row.open_time_ms.parse::<u64>().map_err(|e| e.to_string())?;
                    oldest = oldest.min(t);
                    if t >= start && t < end && row.confirmed {
                        let number = |v: &str| -> Result<f64, String> {
                            let n = v.parse::<f64>().map_err(|e| e.to_string())?;
                            if !n.is_finite() {
                                return Err("NONFINITE_SOURCE".into());
                            }
                            Ok(n)
                        };
                        // Preserve the old source's full OHLCV validation even
                        // though the strategy currently consumes only closes.
                        number(&row.open)?;
                        number(&row.high)?;
                        number(&row.low)?;
                        let close = number(&row.close)?;
                        number(&row.volume)?;
                        rows.insert(t, close);
                    }
                }
                if oldest <= start || oldest >= cursor.parse().unwrap_or(u64::MAX) {
                    break;
                }
                cursor = oldest.to_string();
            }
            if rows.is_empty() {
                return Err("CANDLE_RANGE_UNAVAILABLE".into());
            }
            Ok(rows
                .into_iter()
                .map(|(time, close)| (time + timeframe_ms, close))
                .collect())
        })
        .await
        .map_err(|_| "LIVE_SOURCE_DEADLINE".to_owned())?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_api::{
        HttpRequest, HttpSend, HttpTransport, OkxEnvironment, OkxPublicClient, RateBudget,
        RawHttpResponse, Region,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Recorded {
        body: Vec<u8>,
        calls: AtomicUsize,
    }
    impl HttpTransport for Recorded {
        fn send(&self, request: HttpRequest, _: std::time::Instant) -> HttpSend<'_> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            assert_eq!(request.method().as_str(), "GET");
            assert_eq!(request.url().path(), "/api/v5/market/history-candles");
            assert_eq!(
                request
                    .url()
                    .query_pairs()
                    .find(|(k, _)| k == "bar")
                    .unwrap()
                    .1,
                "1m"
            );
            Box::pin(async {
                Ok(RawHttpResponse {
                    status: okx_api::HttpStatusCode::OK,
                    headers: Default::default(),
                    body: self.body.clone(),
                })
            })
        }
    }
    fn source(rows: serde_json::Value) -> (LiveCandleSource, Arc<Recorded>) {
        let transport = Arc::new(Recorded {
            body: serde_json::to_vec(&serde_json::json!({"code":"0","msg":"","data":rows}))
                .unwrap(),
            calls: AtomicUsize::new(0),
        });
        let client = OkxPublicClient::with_http_transport(
            OkxEnvironment::new(Region::Global, false),
            RateBudget::new(),
            transport.clone(),
        )
        .unwrap();
        (
            LiveCandleSource::new(MarketBootstrapper::new(client)),
            transport,
        )
    }
    fn candle(t: u64, close: &str, confirmed: &str) -> serde_json::Value {
        serde_json::json!([
            t.to_string(),
            "10",
            "11",
            "9",
            close,
            "3",
            "3",
            "3",
            confirmed
        ])
    }
    #[tokio::test]
    async fn live_source_returns_only_confirmed_closed_rows_and_uses_public_capture() {
        let end = jiff::Timestamp::now().as_millisecond().max(0) as u64 / 60000 * 60000;
        let (source, transport) = source(serde_json::json!([
            candle(end - 60000, "10", "0"),
            candle(end - 120000, "10", "1")
        ]));
        let rows = source
            .load_closed_closes("BTC-USDT", 60000, 2)
            .await
            .unwrap();
        assert_eq!(rows, vec![(end - 60000, 10.0)]);
        assert_eq!(transport.calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            source
                .load_closed_closes("BTC-USDT", 60000, 2)
                .await
                .unwrap(),
            rows
        );
        assert_eq!(transport.calls.load(Ordering::Relaxed), 2);
    }
    #[tokio::test]
    async fn live_source_rejects_invalid_limits_timeframes_and_duplicate_source_rows() {
        let end = jiff::Timestamp::now().as_millisecond().max(0) as u64 / 60000 * 60000;
        let (source, transport) = source(serde_json::json!([
            candle(end - 60000, "10", "1"),
            candle(end - 60000, "10", "1")
        ]));
        assert_eq!(
            source
                .load_closed_closes("BTC-USDT", 60000, 0)
                .await
                .unwrap_err(),
            "LIVE_WARMUP_LIMIT:1..10000"
        );
        assert_eq!(
            source
                .load_closed_closes("BTC-USDT", 123, 1)
                .await
                .unwrap_err(),
            "UNSUPPORTED_TIMEFRAME"
        );
        assert_eq!(transport.calls.load(Ordering::Relaxed), 0);
        assert!(
            source
                .load_closed_closes("BTC-USDT", 60000, 1)
                .await
                .unwrap_err()
                .contains("duplicate candle timestamp")
        );
        assert_eq!(transport.calls.load(Ordering::Relaxed), 1);
    }
}

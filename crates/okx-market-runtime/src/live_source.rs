//! Bounded public closed-candle input for live strategy decisions.
use crate::market_bootstrap::MarketBootstrapper;
use okx_ipc_contracts::trading::StrategyTimeframe;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

const MAX_CACHED_SERIES: usize = 64;
type SeriesCache = Arc<AsyncMutex<Option<CachedCloses>>>;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SeriesKey {
    instrument: String,
    timeframe_ms: u64,
    limit: usize,
}

struct CachedCloses {
    boundary_ms: u64,
    rows: Vec<(u64, f64)>,
}

#[derive(Clone)]
pub struct LiveCandleSource {
    bootstrap: MarketBootstrapper,
    permits: Arc<Semaphore>,
    cache: Arc<Mutex<BTreeMap<SeriesKey, SeriesCache>>>,
}
impl LiveCandleSource {
    pub fn new(bootstrap: MarketBootstrapper) -> Self {
        Self {
            bootstrap,
            permits: Arc::new(Semaphore::new(8)),
            cache: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn cache_slot(&self, key: SeriesKey) -> Result<SeriesCache, String> {
        let mut cache = self.cache.lock().map_err(|_| "LIVE_CACHE_UNAVAILABLE")?;
        if !cache.contains_key(&key) && cache.len() >= MAX_CACHED_SERIES {
            cache.clear();
        }
        Ok(cache
            .entry(key)
            .or_insert_with(|| Arc::new(AsyncMutex::new(None)))
            .clone())
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
        let now = jiff::Timestamp::now().as_millisecond().max(0) as u64;
        self.load_closed_closes_at(instrument, timeframe_ms, limit, now)
            .await
    }

    async fn load_closed_closes_at(
        &self,
        instrument: &str,
        timeframe_ms: u64,
        limit: usize,
        now: u64,
    ) -> Result<Vec<(u64, f64)>, String> {
        if limit == 0 || limit > 10_000 {
            return Err("LIVE_WARMUP_LIMIT:1..10000".into());
        }
        let bar = match StrategyTimeframe::from_millis(timeframe_ms).ok_or("UNSUPPORTED_TIMEFRAME")? {
            StrategyTimeframe::Minute1 => "1m",
            StrategyTimeframe::Minute3 => "3m",
            StrategyTimeframe::Minute5 => "5m",
            StrategyTimeframe::Minute15 => "15m",
            StrategyTimeframe::Minute30 => "30m",
            StrategyTimeframe::Hour1 => "1H",
            StrategyTimeframe::Hour4 => "4H",
            StrategyTimeframe::Day1Utc => "1Dutc",
        };
        let end = now / timeframe_ms * timeframe_ms;
        let start = end.saturating_sub((limit as u64).saturating_mul(timeframe_ms));
        if end <= start || (end - start) / timeframe_ms > 1_000_000 {
            return Err("LIMIT_BARS:1000000".into());
        }

        // An owned async lock for each series merges simultaneous requests to
        // the same closed bar while unrelated instruments remain concurrent.
        // Clearing the bounded index merely causes a fresh source read.
        let key = SeriesKey {
            instrument: instrument.to_owned(),
            timeframe_ms,
            limit,
        };
        let slot = self.cache_slot(key)?;
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut cached = slot.lock().await;
            if let Some(existing) = cached.as_ref()
                && existing.boundary_ms == end
            {
                return Ok(existing.rows.clone());
            }
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
            let closes: Vec<_> = rows
                .into_iter()
                .map(|(time, close)| (time + timeframe_ms, close))
                .collect();
            // Never cache missing or partially observed history: a later
            // source retry might supply the omitted confirmed close.
            *cached = if closes.len() == limit && closes.last().is_some_and(|(t, _)| *t == end) {
                Some(CachedCloses {
                    boundary_ms: end,
                    rows: closes.clone(),
                })
            } else {
                None
            };
            Ok(closes)
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
            assert!(!request.headers().contains_key("x-simulated-trading"));
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
    async fn complete_closed_bar_is_read_once_per_boundary_even_for_concurrent_consumers() {
        let end = jiff::Timestamp::now().as_millisecond().max(0) as u64 / 60000 * 60000;
        let (source, transport) = source(serde_json::json!([
            candle(end - 60000, "11", "1"),
            candle(end - 120000, "10", "1")
        ]));
        let now = end + 1_000;
        let (first, second) = tokio::join!(
            source.load_closed_closes_at("BTC-USDT", 60000, 2, now),
            source.load_closed_closes_at("BTC-USDT", 60000, 2, now),
        );
        assert_eq!(first.unwrap(), second.unwrap());
        assert_eq!(transport.calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            source
                .load_closed_closes_at("BTC-USDT", 60000, 2, now)
                .await
                .unwrap()
                .last()
                .unwrap()
                .0,
            end
        );
        assert_eq!(transport.calls.load(Ordering::Relaxed), 1);
        source
            .load_closed_closes_at("BTC-USDT", 60000, 2, now + 60000)
            .await
            .unwrap();
        assert_eq!(transport.calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn incomplete_closed_bar_is_not_cached() {
        let end = jiff::Timestamp::now().as_millisecond().max(0) as u64 / 60000 * 60000;
        let (source, transport) = source(serde_json::json!([
            candle(end - 60000, "10", "0"),
            candle(end - 120000, "10", "1")
        ]));
        let now = end + 1_000;
        for _ in 0..2 {
            assert_eq!(
                source
                    .load_closed_closes_at("BTC-USDT", 60000, 2, now)
                    .await
                    .unwrap(),
                vec![(end - 60000, 10.0)]
            );
        }
        assert_eq!(transport.calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn cached_series_index_remains_bounded() {
        let (source, _) = source(serde_json::json!([]));
        for index in 0..MAX_CACHED_SERIES + 1 {
            source
                .cache_slot(SeriesKey {
                    instrument: format!("BTC-USDT-{index}"),
                    timeframe_ms: 60000,
                    limit: 1,
                })
                .unwrap();
        }
        assert!(source.cache.lock().unwrap().len() <= MAX_CACHED_SERIES);
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

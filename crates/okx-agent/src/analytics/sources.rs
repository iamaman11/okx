use super::cache::{SourceCache, SourceKey};
use crate::market_bootstrap::MarketBootstrapper;
use okx_research::strategy_registry::Bar;
use std::sync::Arc;
#[derive(Clone)]
pub struct Sources {
    pub excluded: Arc<std::sync::Mutex<std::collections::BTreeMap<SourceKey, usize>>>,
    pub cache: SourceCache,
    pub bootstrap: MarketBootstrapper,
}
pub fn timeframe(ms: u64) -> Result<&'static str, String> {
    match ms {
        60000 => Ok("1m"),
        180000 => Ok("3m"),
        300000 => Ok("5m"),
        900000 => Ok("15m"),
        1800000 => Ok("30m"),
        3600000 => Ok("1H"),
        14400000 => Ok("4H"),
        86400000 => Ok("1Dutc"),
        _ => Err("UNSUPPORTED_TIMEFRAME".into()),
    }
}
impl Sources {
    pub async fn candles(
        &self,
        symbol: &str,
        ms: u64,
        start: u64,
        end: u64,
        queued: bool,
    ) -> Result<Arc<Vec<Bar>>, String> {
        let bar = timeframe(ms)?;
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let end = end.min(now / ms * ms);
        if end <= start || (end - start) / ms > 1_000_000 {
            return Err("LIMIT_BARS:1000000".into());
        }
        let key = SourceKey {
            source: "candles".into(),
            instrument: symbol.into(),
            timeframe_ms: ms,
            start_ms: start,
            end_ms: end,
            closed_only: true,
            schema: 1,
        };
        let metadata_key = key.clone();
        self.cache
            .load(key, || async {
                let mut excluded = 0usize;
                let mut rows = std::collections::BTreeMap::new();
                let mut cursor = end.to_string();
                loop {
                    let page = if queued {
                        self.bootstrap
                            .analytics_candles_page(symbol, bar, Some(&cursor), None, 100)
                            .await
                    } else {
                        self.bootstrap
                            .research_candles_page(symbol, bar, Some(&cursor), None, 100)
                            .await
                    }
                    .map_err(|e| e.to_string())?;
                    if page.rows.is_empty() {
                        break;
                    }
                    let mut oldest = u64::MAX;
                    for r in page.rows {
                        if !r.confirmed {
                            excluded += 1;
                        }
                        let t = r.open_time_ms.parse::<u64>().map_err(|e| e.to_string())?;
                        oldest = oldest.min(t);
                        if t >= start && t < end && r.confirmed {
                            let num = |v: &str| -> Result<f64, String> {
                                let n = v.parse::<f64>().map_err(|e| e.to_string())?;
                                if !n.is_finite() {
                                    return Err("NONFINITE_SOURCE".into());
                                }
                                Ok(n)
                            };
                            rows.insert(
                                t,
                                Bar {
                                    time: t,
                                    open: num(&r.open)?,
                                    high: num(&r.high)?,
                                    low: num(&r.low)?,
                                    close: num(&r.close)?,
                                    volume: num(&r.volume)?,
                                    funding_rate: None,
                                },
                            );
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
                {
                    let mut metadata = self.excluded.lock().unwrap();
                    while metadata.len() >= 1000 {
                        let k = metadata.keys().next().cloned().unwrap();
                        metadata.remove(&k);
                    }
                    metadata.insert(metadata_key, excluded);
                }
                Ok(rows.into_values().collect())
            })
            .await
    }
}
impl Sources {
    pub async fn funding(
        &self,
        symbol: &str,
        start: u64,
        end: u64,
    ) -> Result<Vec<(u64, f64)>, String> {
        let _p = self.cache.permit().await?;
        let mut cursor = end.to_string();
        let mut out = std::collections::BTreeMap::new();
        loop {
            let page = self
                .bootstrap
                .research_funding_page(symbol, Some(&cursor), None, 100)
                .await
                .map_err(|e| e.to_string())?;
            if page.rows.is_empty() {
                break;
            }
            let mut oldest = u64::MAX;
            for row in page.rows {
                let time = row
                    .funding_time_ms
                    .parse::<u64>()
                    .map_err(|e| e.to_string())?;
                oldest = oldest.min(time);
                if time >= start && time < end {
                    let rate = row
                        .realized_rate
                        .as_deref()
                        .unwrap_or(&row.funding_rate)
                        .parse::<f64>()
                        .map_err(|e| e.to_string())?;
                    if !rate.is_finite() {
                        return Err("INVALID_FUNDING".into());
                    }
                    out.insert(time, rate);
                }
            }
            if oldest <= start || oldest >= cursor.parse().unwrap_or(u64::MAX) {
                break;
            }
            cursor = oldest.to_string();
        }
        if out.is_empty() {
            return Err("FUNDING_RANGE_UNAVAILABLE".into());
        }
        Ok(out.into_iter().collect())
    }
}

#[cfg(test)]
mod extension_tests {
    use super::*;
    use okx_api::{HttpSend, HttpTransport, RawHttpResponse};
    use okx_protocol::analytics::{
        EmaParameters, FundingMode, MarketAction, MarketRequest, ResearchCosts, ResearchDataMode,
        ResearchRequest, ResearchResultView,
    };
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct RecordedCandles {
        calls: AtomicUsize,
        writes: AtomicUsize,
        body: Vec<u8>,
    }
    impl HttpTransport for RecordedCandles {
        fn send(&self, request: okx_api::HttpRequest, _: std::time::Instant) -> HttpSend<'_> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if request.method() != reqwest::Method::GET {
                self.writes.fetch_add(1, Ordering::Relaxed);
            }
            assert_eq!(request.method(), reqwest::Method::GET);
            assert_eq!(request.url().path(), "/api/v5/market/history-candles");
            // Sources::candles sends end_ms as `after`, no `before`; the
            // API builder appends bar and limit without adding other fields.
            assert_eq!(
                request.url().query_pairs().collect::<Vec<_>>(),
                vec![
                    ("instId".into(), "BTC-USDT".into()),
                    ("after".into(), "1200000".into()),
                    ("bar".into(), "1m".into()),
                    ("limit".into(), "100".into()),
                ]
            );
            assert!(!request.headers().contains_key("x-simulated-trading"));
            Box::pin(async {
                Ok(RawHttpResponse {
                    status: okx_api::HttpStatusCode::OK,
                    headers: Default::default(),
                    body: self.body.clone(),
                })
            })
        }
    }

    fn raw_candle(index: u64, confirm: &str) -> serde_json::Value {
        let close = (index + 1) as f64;
        json!([
            (index * 60000).to_string(),
            close.to_string(),
            (close + 0.5).to_string(),
            (close - 0.5).to_string(),
            close.to_string(),
            "42",
            "42",
            "42",
            confirm
        ])
    }

    #[tokio::test]
    async fn c20_duplicate_raw_source_is_rejected_without_cache_or_job_effects() {
        let mut rows = (0..20)
            .rev()
            .map(|index| raw_candle(index, "1"))
            .collect::<Vec<_>>();
        rows.push(raw_candle(2, "1"));
        let transport = Arc::new(RecordedCandles {
            calls: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
            body: serde_json::to_vec(&json!({"code":"0","msg":"","data":rows})).unwrap(),
        });
        let (root, mut service) = super::super::artifact_job_fixtures::service();
        service.sources.bootstrap = MarketBootstrapper::new(
            okx_api::OkxPublicClient::with_http_transport(
                okx_api::OkxEnvironment::new(okx_api::Region::Global, false),
                okx_api::RateBudget::new(),
                transport.clone(),
            )
            .unwrap(),
        );
        assert_eq!(
            service
                .sources
                .candles("BTC-USDT", 60000, 0, 1200000, false)
                .await
                .unwrap_err(),
            "market history normalization error: history contains duplicate candle timestamp '120000'"
        );
        assert!(service.sources.excluded.lock().unwrap().is_empty());
        assert!(service.jobs.snapshot().is_empty());
        let key = super::super::cache::SourceKey {
            source: "candles".into(),
            instrument: "BTC-USDT".into(),
            timeframe_ms: 60000,
            start_ms: 0,
            end_ms: 1200000,
            closed_only: true,
            schema: 1,
        };
        // A successful cache entry would bypass this predetermined error probe.
        assert_eq!(
            service
                .sources
                .cache
                .load(key, || async { Err("INVALID_PAGE_NOT_CACHED".into()) })
                .await
                .unwrap_err(),
            "INVALID_PAGE_NOT_CACHED"
        );
        assert_eq!(transport.calls.load(Ordering::Relaxed), 1);
        assert_eq!(transport.writes.load(Ordering::Relaxed), 0);
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn c20_in_window_unconfirmed_candle_blocks_validation_and_replay_without_result() {
        let rows = (0..20)
            .rev()
            .map(|index| {
                if index == 10 {
                    let mut row = raw_candle(index, "0");
                    row[1] = json!("999");
                    row[2] = json!("1000");
                    row[3] = json!("998");
                    row[4] = json!("999");
                    row
                } else {
                    raw_candle(index, "1")
                }
            })
            .collect::<Vec<_>>();
        let transport = Arc::new(RecordedCandles {
            calls: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
            body: serde_json::to_vec(&json!({"code":"0","msg":"","data":rows})).unwrap(),
        });
        let (root, mut service) = super::super::artifact_job_fixtures::service();
        service.sources.bootstrap = MarketBootstrapper::new(
            okx_api::OkxPublicClient::with_http_transport(
                okx_api::OkxEnvironment::new(okx_api::Region::Global, false),
                okx_api::RateBudget::new(),
                transport.clone(),
            )
            .unwrap(),
        );
        let mut wire = json!({"action":"validate","instruments":["BTC-USDT"],"timeframe_ms":60000,
            "start_ms":0,"end_ms":1200000,"range_mode":"strict","strategy_id":"ema_momentum_confirm_v1",
            "parameters":{"fast":1,"slow":2},"costs":{"maker_fee_bps":0,"taker_fee_bps":0,"slippage_bps":0,"funding_mode":"zero_assumption"},
            "data_mode":"counterfactual"});
        let validation = service
            .research(serde_json::from_value(wire.clone()).unwrap())
            .await
            .unwrap();
        assert_eq!(validation["status"], "blocked");
        assert_eq!(validation["job_created"], false);
        assert_eq!(validation["source_coverage"][0]["observed_bars"], 19);
        assert_eq!(
            validation["source_coverage"][0]["reason_code"],
            "CANDLE_RANGE_INCOMPLETE_OR_GAPPED"
        );
        // Partial observations remain cached honestly, never a complete result.
        let confirmed = service
            .sources
            .candles("BTC-USDT", 60000, 0, 1200000, false)
            .await
            .unwrap();
        assert_eq!(confirmed.len(), 19);
        assert!(
            confirmed
                .iter()
                .all(|bar| bar.time != 600000 && bar.close != 999.)
        );
        wire["action"] = json!("backtest");
        wire["idempotency_key"] = json!("c20-unconfirmed-gap");
        wire.as_object_mut().unwrap().remove("range_mode");
        assert_eq!(
            service
                .execute_one(serde_json::from_value(wire).unwrap(), Default::default())
                .await
                .unwrap_err(),
            "CANDLE_INTERIOR_GAP"
        );
        assert!(service.jobs.snapshot().is_empty());
        assert!(!service.artifacts.evidence_dir().exists());
        assert_eq!(transport.calls.load(Ordering::Relaxed), 1);
        assert_eq!(transport.writes.load(Ordering::Relaxed), 0);
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn c20_actual_sources_normalize_fake_http_then_run_registered_composition_and_mean() {
        let mut rows = (0..20)
            .rev()
            .map(|index| raw_candle(index, "1"))
            .collect::<Vec<_>>();
        // Fixed valid profile: unique reversed timestamps plus a distinct
        // unconfirmed edge candle, never a second row for an existing timestamp.
        let mut unclosed = raw_candle(20, "0");
        unclosed[1] = json!("999");
        unclosed[2] = json!("1000");
        unclosed[3] = json!("998");
        unclosed[4] = json!("999");
        rows.push(unclosed);
        let transport = Arc::new(RecordedCandles {
            calls: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
            body: serde_json::to_vec(&json!({"code":"0","msg":"","data":rows})).unwrap(),
        });
        let (root, mut service) = super::super::artifact_job_fixtures::service();
        service.sources.bootstrap = MarketBootstrapper::new(
            okx_api::OkxPublicClient::with_http_transport(
                okx_api::OkxEnvironment::new(okx_api::Region::Global, false),
                okx_api::RateBudget::new(),
                transport.clone(),
            )
            .unwrap(),
        );
        // This exercises the existing real delivery decoder and Sources boundary,
        // rather than inserting normalized bars directly into the source cache.
        let bars = service
            .sources
            .candles("BTC-USDT", 60000, 0, 1200000, false)
            .await
            .unwrap();
        assert_eq!(bars.len(), 20);
        for (index, bar) in bars.iter().enumerate() {
            assert_eq!(bar.time, index as u64 * 60000);
            assert_eq!(bar.close, (index + 1) as f64);
            assert_eq!(bar.volume, 42.);
            assert_eq!(bar.funding_rate, None);
        }
        assert_eq!(
            *service
                .sources
                .excluded
                .lock()
                .unwrap()
                .values()
                .next()
                .unwrap(),
            1
        );
        let market = service
            .market(
                MarketRequest {
                    action: MarketAction::Screen,
                    instruments: vec!["BTC-USDT".into()],
                    universe: None,
                    timeframe_ms: 60000,
                    start_ms: 0,
                    end_ms: 1200000,
                    fields: vec!["rolling_mean".into()],
                    parameters: Default::default(),
                    filters: vec![],
                    sort: None,
                    limit: 20,
                    cursor: None,
                },
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(market["rows"][0]["metrics"]["rolling_mean"], 13.5);
        assert_eq!(market["rows"][0]["units"]["rolling_mean"], "price");
        let mut request = ResearchRequest::Backtest {
            idempotency_key: "c20-registered-composition".into(),
            instruments: vec!["BTC-USDT".into()],
            timeframe_ms: 60000,
            start_ms: 0,
            end_ms: 1200000,
            strategy_id: "ema_momentum_confirm_v1".into(),
            parameters: EmaParameters { fast: 1, slow: 2 },
            costs: ResearchCosts {
                maker_fee_bps: 0.,
                taker_fee_bps: 0.,
                slippage_bps: 0.,
                funding_mode: FundingMode::ZeroAssumption,
            },
            data_mode: ResearchDataMode::Counterfactual,
            seed: 7,
        };
        let queued = service.research(request.clone()).await.unwrap();
        let job = queued["id"].as_str().unwrap().to_owned();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let status = service.jobs.get(&job).unwrap();
                match status.state {
                    super::super::jobs::State::Completed => break,
                    super::super::jobs::State::Failed => {
                        panic!("composition job failed: {:?}", status.error)
                    }
                    _ => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
                }
            }
        })
        .await
        .unwrap();
        let result = service
            .research(ResearchRequest::Result {
                job_id: job,
                cursor: None,
                limit: 20,
                view: ResearchResultView::Summary,
            })
            .await
            .unwrap();
        assert_eq!(result["metadata"]["summary"]["pnl_exact"], "17");
        let experiment: serde_json::Value = service
            .artifacts
            .read_evidence_json(result["metadata"]["experiment_artifact"].as_str().unwrap())
            .unwrap();
        assert_eq!(experiment["spec"]["strategy_id"], "ema_momentum_confirm_v1");
        assert_eq!(experiment["spec"]["parameters"], json!({"fast":1,"slow":2}));
        let before = service.jobs.snapshot().len();
        if let ResearchRequest::Backtest { strategy_id, .. } = &mut request {
            *strategy_id = "unregistered_source_code".into();
        }
        assert_eq!(
            service.research(request).await.unwrap_err(),
            "INVALID_RESEARCH_REQUEST"
        );
        assert_eq!(service.jobs.snapshot().len(), before);
        assert_eq!(transport.calls.load(Ordering::Relaxed), 1);
        assert_eq!(transport.writes.load(Ordering::Relaxed), 0);
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }
}

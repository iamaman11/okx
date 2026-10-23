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
                    let page = self
                        .bootstrap
                        .research_candles_page(symbol, bar, Some(&cursor), None, 100)
                        .await
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

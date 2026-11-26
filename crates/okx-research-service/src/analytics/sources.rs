use super::cache::{SourceCache, SourceKey};
use crate::source_api::{PublicSourceClient, ResearchTradePage};
use okx_research::strategy_registry::Bar;
use std::sync::Arc;
#[derive(Clone)]
pub struct Sources {
    pub excluded: Arc<std::sync::Mutex<std::collections::BTreeMap<SourceKey, usize>>>,
    pub cache: SourceCache,
    pub bootstrap: PublicSourceClient,
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

// OKX funding history distinguishes a predicted fundingRate from the
// realizedRate actually charged at settlement. Substituting the forecast
// would silently change the meaning of a historical funding_sum.
fn realized_funding_rate(value: Option<&str>) -> Result<f64, String> {
    let raw = value
        .filter(|value| !value.trim().is_empty())
        .ok_or("FUNDING_REALIZED_RATE_UNAVAILABLE")?;
    let rate = raw
        .parse::<f64>()
        .map_err(|_| "FUNDING_REALIZED_RATE_INVALID")?;
    if !rate.is_finite() {
        return Err("FUNDING_REALIZED_RATE_INVALID".into());
    }
    Ok(rate)
}

impl Sources {
    /// Typed public trade source with Research-owned normalization and evidence.
    /// The only network capability is the host's pre-registered public GET route.
    pub async fn recent_trades(
        &self,
        symbol: &str,
        limit: u16,
    ) -> Result<ResearchTradePage, String> {
        self.bootstrap.recent_trades(symbol, limit).await
    }

    pub async fn history_trades(
        &self,
        symbol: &str,
        after: Option<&str>,
        before: Option<&str>,
        limit: u16,
    ) -> Result<ResearchTradePage, String> {
        self.bootstrap
            .history_trades(symbol, after, before, limit)
            .await
    }

    pub async fn candles(
        &self,
        symbol: &str,
        ms: u64,
        start: u64,
        end: u64,
        queued: bool,
    ) -> Result<Arc<Vec<Bar>>, String> {
        let bar = timeframe(ms)?;
        let now = jiff::Timestamp::now().as_millisecond().max(0) as u64;
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
                    let rate = realized_funding_rate(row.realized_rate.as_deref())?;
                    // API pages may overlap; identical events are harmless,
                    // conflicting realized rates cannot be silently overwritten.
                    match out.entry(time) {
                        std::collections::btree_map::Entry::Vacant(slot) => {
                            slot.insert(rate);
                        }
                        std::collections::btree_map::Entry::Occupied(slot)
                            if *slot.get() != rate =>
                        {
                            return Err("FUNDING_CONFLICTING_EVENT_TIMESTAMP".into());
                        }
                        _ => {}
                    }
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
#[path = "tests/sources_extensions.rs"]
mod extension_tests;

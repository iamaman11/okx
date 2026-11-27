//! Research-owned, public-only OKX market source. No Engine, keys, private
//! endpoints, arbitrary URLs or venue mutations are reachable from this API.
use crate::source_api::{PublicSourceProvider, SourceFuture};
use okx_research_protocol::research_ipc::{
    FundingHistoryEvent, HistoryCandle, MAX_FRAME_BYTES, PublicCandleCapture, PublicCatalogCapture,
    PublicFundingCapture, PublicReferenceCapture, PublicReferenceInstrument,
    PublicSourceHttpDelivery, PublicSourceReply, PublicSourceRequest, ResearchOpenInterest,
    ResearchTicker,
};
use rust_decimal::Decimal;
use serde::{Deserialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const PUBLIC_CANDLES_PATH: &str = "/api/v5/market/history-candles";
const PUBLIC_ORIGIN: &str = "https://www.okx.com";
const MAX_PUBLIC_BYTES: usize = MAX_FRAME_BYTES / 8;
// The complete SWAP/FUTURES reference registry is larger than an ordinary
// source page. Keep its independent bound below the 4 MiB IPC frame, without
// relaxing candle/trade/funding or single-instrument capture limits.
pub(crate) const MAX_PUBLIC_INSTRUMENTS_BYTES: usize = MAX_FRAME_BYTES / 2;

fn public_body_limit(path: &str, parameters: &[(&str, String)]) -> usize {
    if path == "/api/v5/public/instruments"
        && parameters
            .iter()
            .any(|(name, value)| *name == "instType" && matches!(value.as_str(), "SWAP" | "FUTURES"))
        && !parameters.iter().any(|(name, _)| *name == "instId")
    {
        MAX_PUBLIC_INSTRUMENTS_BYTES
    } else {
        MAX_PUBLIC_BYTES
    }
}

pub(crate) type GetFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + 'a>>;

/// Narrow injection point for recorded public HTTP responses in offline tests.
/// The production implementation is GET-only on a compile-time fixed origin.
pub(crate) trait PublicGet: Send + Sync {
    fn get(&self, path: &'static str, parameters: Vec<(&'static str, String)>) -> GetFuture<'_>;
}

struct BoundedHttpsGet {
    client: reqwest::Client,
    permits: tokio::sync::Semaphore,
}

impl PublicGet for BoundedHttpsGet {
    fn get(&self, path: &'static str, parameters: Vec<(&'static str, String)>) -> GetFuture<'_> {
        Box::pin(async move {
            let max_body_bytes = public_body_limit(path, &parameters);
            let _permit = tokio::time::timeout(Duration::from_secs(5), self.permits.acquire())
                .await
                .map_err(|_| "OKX rate/backpressure defer: standalone request capacity")?
                .map_err(|_| "RESEARCH_PUBLIC_TRANSPORT_FAILED")?;
            let mut response = self
                .client
                .get(format!("{PUBLIC_ORIGIN}{path}"))
                .query(&parameters)
                .send()
                .await
                .map_err(|_| "RESEARCH_PUBLIC_TRANSPORT_FAILED".to_owned())?;
            if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                return Err("OKX rate/backpressure defer: HTTP 429".into());
            }
            if response.status() != reqwest::StatusCode::OK {
                return Err("RESEARCH_PUBLIC_HTTP_FAILED".into());
            }
            if response
                .content_length()
                .is_some_and(|size| size > max_body_bytes as u64)
            {
                return Err("RESEARCH_PUBLIC_BODY_TOO_LARGE".into());
            }
            let mut raw = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| "RESEARCH_PUBLIC_TRANSPORT_FAILED".to_owned())?
            {
                if chunk.len() > max_body_bytes.saturating_sub(raw.len()) {
                    return Err("RESEARCH_PUBLIC_BODY_TOO_LARGE".into());
                }
                raw.extend_from_slice(&chunk);
            }
            Ok(raw)
        })
    }
}

/// Standalone public market capability for Research. It is intentionally
/// independent of `MarketBootstrapper` and the installed trading Agent.
pub struct IndependentOkxPublicSource {
    get: Arc<dyn PublicGet>,
    pub(crate) public_market_ws: crate::public_ws::PublicMarketWs,
    #[cfg(test)]
    market_ws_test_endpoint: Option<String>,
}

impl IndependentOkxPublicSource {
    pub fn new() -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| "RESEARCH_PUBLIC_CLIENT_INIT_FAILED".to_owned())?;
        Ok(Self {
            get: Arc::new(BoundedHttpsGet {
                client,
                permits: tokio::sync::Semaphore::new(4),
            }),
            public_market_ws: Default::default(),
            #[cfg(test)]
            market_ws_test_endpoint: Some(crate::public_ws::PUBLIC_URL.to_owned()),
        })
    }

    pub(crate) async fn get_bytes(
        &self,
        path: &'static str,
        query: Vec<(&'static str, String)>,
    ) -> Result<Vec<u8>, String> {
        self.get.get(path, query).await
    }

    pub(crate) fn public_get(&self) -> &Arc<dyn PublicGet> {
        &self.get
    }

    pub(crate) fn market_ws_endpoint(&self) -> Option<&str> {
        #[cfg(test)]
        {
            self.market_ws_test_endpoint.as_deref()
        }
        #[cfg(not(test))]
        {
            Some(crate::public_ws::PUBLIC_URL)
        }
    }

    #[cfg(test)]
    pub(crate) fn with_get(get: Arc<dyn PublicGet>) -> Self {
        Self {
            get,
            public_market_ws: Default::default(),
            market_ws_test_endpoint: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_market_ws_endpoint(mut self, endpoint: String) -> Self {
        self.market_ws_test_endpoint = Some(endpoint);
        self
    }
}

impl PublicSourceProvider for IndependentOkxPublicSource {
    fn request(&self, request: PublicSourceRequest) -> SourceFuture<'_> {
        Box::pin(async move {
            if !request.valid() {
                return Err("INVALID_RESEARCH_SOURCE_REQUEST".into());
            }
            match request {
                PublicSourceRequest::Candles {
                    instrument,
                    bar,
                    after,
                    before,
                    limit,
                    ..
                } => {
                    let mut parameters = page_parameters(instrument, after, before, limit);
                    parameters.insert(1, ("bar", bar));
                    let raw = self.get.get(PUBLIC_CANDLES_PATH, parameters).await?;
                    let rows = parse_candles(&raw, usize::from(limit))?;
                    let http_delivery = Some(delivery(&raw));
                    Ok(PublicSourceReply::Candles(PublicCandleCapture {
                        raw_body: raw,
                        rows,
                        acquired_at_ms: acquired_at()?,
                        http_delivery,
                    }))
                }
                PublicSourceRequest::CatalogPage {
                    route,
                    instrument,
                    after,
                    before,
                    limit,
                } => {
                    let mut parameters = page_parameters(instrument, after, before, limit);
                    let path = match route.as_str() {
                        "recent_trades" => "/api/v5/market/trades",
                        "history_trades" => {
                            parameters.push(("type", "1".into()));
                            "/api/v5/market/history-trades"
                        }
                        _ => return Err("INVALID_RESEARCH_SOURCE_REQUEST".into()),
                    };
                    let raw = self.get.get(path, parameters).await?;
                    let rows = rows(&raw, usize::from(limit), "RESEARCH_PUBLIC_TRADES_INVALID")?;
                    let http_delivery = delivery(&raw);
                    Ok(PublicSourceReply::CatalogPage(PublicCatalogCapture {
                        route,
                        raw_body: raw,
                        rows,
                        acquired_at_ms: acquired_at()?,
                        http_delivery,
                    }))
                }
                PublicSourceRequest::Ticker { instrument } => {
                    let raw = self
                        .get
                        .get(
                            "/api/v5/market/ticker",
                            vec![("instId", instrument.clone())],
                        )
                        .await?;
                    let row: TickerRow = one(&raw, "RESEARCH_PUBLIC_TICKER_INVALID")?;
                    if row.instrument != instrument {
                        return Err("RESEARCH_PUBLIC_TICKER_INVALID".into());
                    }
                    Ok(PublicSourceReply::Ticker(row.ticker))
                }
                PublicSourceRequest::Reference { instrument } => {
                    if !instrument.ends_with("-SWAP") {
                        return Err("RESEARCH_PUBLIC_REFERENCE_REQUIRES_SWAP".into());
                    }
                    let params = vec![("instType", "SWAP".into()), ("instId", instrument.clone())];
                    let raw = self.get.get("/api/v5/public/instruments", params).await?;
                    let row: ReferenceRow = one(&raw, "RESEARCH_PUBLIC_REFERENCE_INVALID")?;
                    if row.inst_id != instrument || row.inst_type != "SWAP" || row.state != "live" {
                        return Err("RESEARCH_PUBLIC_REFERENCE_INVALID".into());
                    }
                    let http_delivery = Some(delivery(&raw));
                    let reference_generation = format!("sha256:{:x}", Sha256::digest(&raw));
                    Ok(PublicSourceReply::Reference(PublicReferenceCapture {
                        raw_body: raw,
                        instrument: PublicReferenceInstrument {
                            contract_type: row.ct_type,
                            contract_value: row.ct_val,
                            contract_value_currency: row.ct_val_ccy,
                            settle_currency: row.settle_ccy,
                        },
                        reference_generation,
                        acquired_at_ms: acquired_at()?,
                        http_delivery,
                    }))
                }
                PublicSourceRequest::Funding {
                    instrument,
                    after,
                    before,
                    limit,
                } => {
                    let params = page_parameters(instrument.clone(), after, before, limit);
                    let raw = self
                        .get
                        .get("/api/v5/public/funding-rate-history", params)
                        .await?;
                    let rows = funding_rows(&raw, &instrument, usize::from(limit))?;
                    let http_delivery = Some(delivery(&raw));
                    Ok(PublicSourceReply::Funding(PublicFundingCapture {
                        raw_body: raw,
                        rows,
                        acquired_at_ms: acquired_at()?,
                        http_delivery,
                    }))
                }
                PublicSourceRequest::OpenInterest { instrument, period } => {
                    let params = vec![
                        ("instId", instrument),
                        ("period", period),
                        ("limit", "100".into()),
                    ];
                    let raw = self
                        .get
                        .get("/api/v5/rubik/stat/contracts/open-interest-history", params)
                        .await?;
                    let error = "RESEARCH_PUBLIC_OPEN_INTEREST_INVALID";
                    let rows = rows::<OpenInterestRow>(&raw, 100, error)?;
                    let mapped = rows
                        .into_iter()
                        .map(OpenInterestRow::normalize)
                        .collect::<Result<_, _>>()?;
                    Ok(PublicSourceReply::OpenInterest(mapped))
                }
            }
        })
    }
}

fn page_parameters(
    instrument: String,
    after: Option<String>,
    before: Option<String>,
    limit: u16,
) -> Vec<(&'static str, String)> {
    let mut parameters = vec![("instId", instrument)];
    if let Some(cursor) = after.filter(|value| !value.is_empty()) {
        parameters.push(("after", cursor));
    }
    if let Some(cursor) = before.filter(|value| !value.is_empty()) {
        parameters.push(("before", cursor));
    }
    parameters.push(("limit", limit.to_string()));
    parameters
}

fn acquired_at() -> Result<String, String> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "RESEARCH_PUBLIC_CLOCK_INVALID")?
        .as_millis()
        .max(1)
        .to_string())
}

fn delivery(raw: &[u8]) -> PublicSourceHttpDelivery {
    PublicSourceHttpDelivery {
        transport_send_invocations: 1,
        response_status: 200,
        response_body_bytes: raw.len() as u64,
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope<T> {
    code: String,
    #[serde(default)]
    msg: String,
    data: Vec<T>,
}

pub(crate) fn rows<T: DeserializeOwned>(
    raw: &[u8],
    max_rows: usize,
    error: &'static str,
) -> Result<Vec<T>, String> {
    rows_bounded(raw, max_rows, MAX_PUBLIC_BYTES, error)
}

pub(crate) fn rows_bounded<T: DeserializeOwned>(
    raw: &[u8],
    max_rows: usize,
    max_body_bytes: usize,
    error: &'static str,
) -> Result<Vec<T>, String> {
    if raw.is_empty() || raw.len() > max_body_bytes {
        return Err("RESEARCH_PUBLIC_BODY_INVALID".into());
    }
    let envelope: Envelope<T> = serde_json::from_slice(raw).map_err(|_| error)?;
    if envelope.code != "0" || !envelope.msg.is_empty() || envelope.data.len() > max_rows {
        return Err(error.into());
    }
    Ok(envelope.data)
}

pub(crate) fn one<T: DeserializeOwned>(raw: &[u8], error: &'static str) -> Result<T, String> {
    rows(raw, 1, error)?.pop().ok_or_else(|| error.into())
}

#[derive(Deserialize)]
struct TickerRow {
    #[serde(rename = "instId")]
    instrument: String,
    #[serde(flatten)]
    ticker: ResearchTicker,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReferenceRow {
    inst_id: String,
    inst_type: String,
    state: String,
    ct_type: Option<String>,
    ct_val: Option<String>,
    ct_val_ccy: Option<String>,
    settle_ccy: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingRow {
    inst_id: String,
    funding_time: String,
    funding_rate: String,
    realized_rate: Option<String>,
    formula_type: Option<String>,
    method: Option<String>,
}

fn funding_rows(
    raw: &[u8],
    instrument: &str,
    limit: usize,
) -> Result<Vec<FundingHistoryEvent>, String> {
    let error = "RESEARCH_PUBLIC_FUNDING_INVALID";
    let mut events = std::collections::BTreeMap::new();
    for row in rows::<FundingRow>(raw, limit, error)? {
        let time = row.funding_time.parse::<u64>().map_err(|_| error)?;
        if row.inst_id != instrument || time == 0 || row.funding_rate.trim().is_empty() {
            return Err(error.into());
        }
        if events
            .insert(
                time,
                FundingHistoryEvent {
                    funding_time_ms: row.funding_time,
                    funding_rate: row.funding_rate,
                    realized_rate: row.realized_rate.filter(|v| !v.trim().is_empty()),
                    formula_type: row.formula_type.filter(|v| !v.trim().is_empty()),
                    method: row.method.filter(|v| !v.trim().is_empty()),
                },
            )
            .is_some()
        {
            return Err(error.into());
        }
    }
    Ok(events.into_values().collect())
}

#[derive(Deserialize)]
#[serde(untagged)]
enum OpenInterestRow {
    Object {
        ts: String,
        oi: String,
        #[serde(rename = "oiCcy")]
        currency: String,
    },
    Array(Vec<String>),
}

impl OpenInterestRow {
    fn normalize(self) -> Result<ResearchOpenInterest, String> {
        match self {
            Self::Object { ts, oi, currency } => Ok(ResearchOpenInterest {
                ts,
                oi,
                oi_currency: currency,
            }),
            Self::Array(row) => match row.as_slice() {
                [ts, oi, currency] | [ts, oi, currency, _] => Ok(ResearchOpenInterest {
                    ts: ts.clone(),
                    oi: oi.clone(),
                    oi_currency: currency.clone(),
                }),
                _ => Err("RESEARCH_PUBLIC_OPEN_INTEREST_INVALID".into()),
            },
        }
    }
}

fn parse_candles(raw: &[u8], max_rows: usize) -> Result<Vec<HistoryCandle>, String> {
    let rows = rows::<Vec<String>>(raw, max_rows, "RESEARCH_PUBLIC_CANDLES_INVALID")?;
    let mut times = BTreeSet::new();
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        if row.len() != 8 && row.len() != 9 {
            return Err("RESEARCH_PUBLIC_CANDLES_INVALID".into());
        }
        let time = row[0]
            .parse::<u64>()
            .map_err(|_| "RESEARCH_PUBLIC_CANDLES_INVALID")?;
        if time == 0 || !times.insert(time) {
            return Err("RESEARCH_PUBLIC_CANDLES_INVALID".into());
        }
        let decimal = |field: &str| -> Result<Decimal, String> {
            Decimal::from_str_exact(field).map_err(|_| "RESEARCH_PUBLIC_CANDLES_INVALID".into())
        };
        let (open, high, low, close) = (
            decimal(&row[1])?,
            decimal(&row[2])?,
            decimal(&row[3])?,
            decimal(&row[4])?,
        );
        if low <= Decimal::ZERO
            || low > high
            || open < low
            || open > high
            || close < low
            || close > high
            || decimal(&row[5])? < Decimal::ZERO
            || decimal(&row[6])? < Decimal::ZERO
            || (row.len() == 9 && decimal(&row[7])? < Decimal::ZERO)
        {
            return Err("RESEARCH_PUBLIC_CANDLES_INVALID".into());
        }
        let confirmed = match row.last().map(String::as_str) {
            Some("0") => false,
            Some("1") => true,
            _ => return Err("RESEARCH_PUBLIC_CANDLES_INVALID".into()),
        };
        output.push(HistoryCandle {
            open_time_ms: row[0].clone(),
            open: row[1].clone(),
            high: row[2].clone(),
            low: row[3].clone(),
            close: row[4].clone(),
            volume: row[5].clone(),
            volume_currency: row[6].clone(),
            volume_quote: (row.len() == 9).then(|| row[7].clone()),
            confirmed,
        });
    }
    output.sort_by_key(|row| row.open_time_ms.parse::<u64>().unwrap_or_default());
    Ok(output)
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use crate::source_api::PublicSourceClient;
    use std::sync::Mutex;

    pub(crate) type RecordedRoute = (&'static str, Vec<(&'static str, String)>);

    pub(crate) struct RecordedGet {
        pages: Vec<Vec<u8>>,
        pub(crate) routes: Mutex<Vec<RecordedRoute>>,
    }

    impl RecordedGet {
        pub(crate) fn new(pages: Vec<Vec<u8>>) -> Arc<Self> {
            Arc::new(Self {
                pages,
                routes: Mutex::new(Vec::new()),
            })
        }

        pub(crate) fn client(self: &Arc<Self>) -> PublicSourceClient {
            PublicSourceClient::new(Arc::new(IndependentOkxPublicSource::with_get(self.clone())))
        }
    }

    impl PublicGet for RecordedGet {
        fn get(
            &self,
            path: &'static str,
            parameters: Vec<(&'static str, String)>,
        ) -> GetFuture<'_> {
            let mut routes = self.routes.lock().unwrap();
            let page = self
                .pages
                .get(routes.len())
                .unwrap_or_else(|| panic!("unexpected public GET to {path}"))
                .clone();
            routes.push((path, parameters));
            Box::pin(async move { Ok(page) })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{test_support::RecordedGet, *};

    #[test]
    fn full_reference_get_has_separate_bounded_budget() {
        let full = [("instType", "SWAP".to_owned())];
        assert_eq!(
            public_body_limit("/api/v5/public/instruments", &full),
            MAX_PUBLIC_INSTRUMENTS_BYTES
        );
        assert_eq!(
            public_body_limit(
                "/api/v5/public/instruments",
                &[("instType", "FUTURES".to_owned())]
            ),
            MAX_PUBLIC_INSTRUMENTS_BYTES
        );
        assert_eq!(
            public_body_limit(
                "/api/v5/public/instruments",
                &[
                    ("instType", "SWAP".to_owned()),
                    ("instId", "BTC-USDT-SWAP".to_owned()),
                ]
            ),
            MAX_PUBLIC_BYTES
        );
        assert_eq!(
            public_body_limit(PUBLIC_CANDLES_PATH, &full),
            MAX_PUBLIC_BYTES
        );
        assert!(MAX_PUBLIC_INSTRUMENTS_BYTES < MAX_FRAME_BYTES);
    }

    const RECORDED: &str = r#"{"code":"0","msg":"","data":[["1700000060000","100","120","90","110","2","200","220","1"],["1700000000000","100","110","95","105","1","100","1"]]}"#;

    #[tokio::test]
    async fn public_candles_do_not_require_engine_or_private_client() {
        let transport = RecordedGet::new(vec![RECORDED.as_bytes().to_vec()]);
        let client = transport.client();
        let result = client
            .research_candles_page("BTC-USDT-SWAP", "1m", None, None, 100)
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].open_time_ms, "1700000000000");
        assert!(result.rows.iter().all(|row| row.confirmed));
        assert_eq!(result.http_delivery.unwrap().transport_send_invocations, 1);
        let requests = transport.routes.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, PUBLIC_CANDLES_PATH);
        assert_eq!(requests[0].1[0], ("instId", "BTC-USDT-SWAP".into()));
    }

    #[tokio::test]
    async fn market_request_replays_recorded_public_candles_without_engine() {
        use crate::analytics::AnalyticsService;
        use serde_json::json;

        let raw = RECORDED
            .replace("1700000000000", "1699999980000")
            .replace("1700000060000", "1700000040000")
            .into_bytes();
        let transport = RecordedGet::new(vec![raw]);
        let root = std::env::temp_dir().join(format!(
            "okx-independent-market-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let service = AnalyticsService::new(root.clone(), transport.client()).unwrap();
        let request = serde_json::from_value(json!({
            "action": "screen",
            "instruments": ["BTC-USDT-SWAP"],
            "fields": ["returns"],
            "timeframe_ms": 60000,
            "start_ms": 1699999980000u64,
            "end_ms": 1700000100000u64
        }))
        .unwrap();
        let first = service.market(request, None, None).await.unwrap();
        let metric = first["rows"][0]["metrics"]["returns"].as_f64().unwrap();
        assert!((metric - (110.0 / 105.0 - 1.0)).abs() < 1e-9);
        assert_eq!(transport.routes.lock().unwrap().len(), 1);
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn public_source_rejects_untrusted_requests_without_network() {
        let transport = RecordedGet::new(Vec::new());
        let source = IndependentOkxPublicSource::with_get(transport.clone());
        assert_eq!(
            source
                .request(PublicSourceRequest::Candles {
                    instrument: "../BTC".into(),
                    bar: "1m".into(),
                    after: None,
                    before: None,
                    limit: 2,
                    queued: false,
                })
                .await
                .unwrap_err(),
            "INVALID_RESEARCH_SOURCE_REQUEST"
        );
        assert_eq!(
            source
                .request(PublicSourceRequest::Reference {
                    instrument: "BTC-USDT".into(),
                })
                .await
                .unwrap_err(),
            "RESEARCH_PUBLIC_REFERENCE_REQUIRES_SWAP"
        );
        assert!(transport.routes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn standalone_routes_preserve_public_source_units_and_provenance() {
        use serde_json::json;
        let symbol = "BTC-USDT-SWAP";
        let request_cases = [
            (
                PublicSourceRequest::CatalogPage {
                    route: "history_trades".into(),
                    instrument: symbol.into(),
                    after: Some("123".into()),
                    before: None,
                    limit: 2,
                },
                "/api/v5/market/history-trades",
                json!({"instId":symbol,"tradeId":"123"}),
                "catalog_page",
                "/data/route",
                "history_trades",
            ),
            (
                PublicSourceRequest::Reference {
                    instrument: symbol.into(),
                },
                "/api/v5/public/instruments",
                json!({"instType":"SWAP","instId":symbol,"state":"live","ctType":"linear",
                    "ctVal":"0.01","ctValCcy":"BTC","settleCcy":"USDT"}),
                "reference",
                "/data/instrument/contract_value_currency",
                "BTC",
            ),
            (
                PublicSourceRequest::Funding {
                    instrument: symbol.into(),
                    after: None,
                    before: None,
                    limit: 1,
                },
                "/api/v5/public/funding-rate-history",
                json!({"instId":symbol,"fundingTime":"123","fundingRate":"0.0001",
                    "realizedRate":"-0.0002"}),
                "funding",
                "/data/rows/0/realized_rate",
                "-0.0002",
            ),
            (
                PublicSourceRequest::Ticker {
                    instrument: symbol.into(),
                },
                "/api/v5/market/ticker",
                json!({"instId":symbol,"bidPx":"99","askPx":"101"}),
                "ticker",
                "/data/bid_price",
                "99",
            ),
            (
                PublicSourceRequest::OpenInterest {
                    instrument: symbol.into(),
                    period: "5m".into(),
                },
                "/api/v5/rubik/stat/contracts/open-interest-history",
                json!(["123", "99", "0.99", "100.0"]),
                "open_interest",
                "/data/0/oi_currency",
                "0.99",
            ),
        ];
        for (request, path, row, variant, field, expected) in request_cases {
            let raw = serde_json::to_vec(&json!({"code":"0","msg":"","data":[row]})).unwrap();
            let transport = RecordedGet::new(vec![raw]);
            let source = IndependentOkxPublicSource::with_get(transport.clone());
            let response = serde_json::to_value(source.request(request).await.unwrap()).unwrap();
            assert_eq!(response["type"], variant);
            assert_eq!(
                response.pointer(field).and_then(|value| value.as_str()),
                Some(expected)
            );
            assert_eq!(transport.routes.lock().unwrap()[0].0, path);
        }
    }

    #[test]
    fn malformed_exchange_rows_are_fail_closed() {
        assert_eq!(
            parse_candles(RECORDED.as_bytes(), 1).unwrap_err(),
            "RESEARCH_PUBLIC_CANDLES_INVALID"
        );
        for raw in [
            r#"{"code":"1","data":[]}"#,
            r#"{"code":"0","data":[["1","100","90","95","110","1","1","1"]]}"#,
            r#"{"code":"0","data":[["1","100","120","90","110","1","1","2"]]}"#,
            r#"{"code":"0","data":[["1","100","120","90","110","1","1","1"],["1","100","120","90","110","1","1","1"]]}"#,
            r#"{"code":"0","data":[["bad","100","120","90","110","1","1","1"]]}"#,
        ] {
            assert!(parse_candles(raw.as_bytes(), 100).is_err());
        }
    }
}

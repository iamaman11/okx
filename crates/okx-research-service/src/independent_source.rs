//! Research-owned, public-only OKX candle source. No Engine, keys, private
//! endpoints, arbitrary URLs or venue mutations are reachable from this API.
use crate::source_api::{PublicSourceProvider, SourceFuture};
use okx_ipc_contracts::research_ipc::{
    HistoryCandle, MAX_FRAME_BYTES, PublicCandleCapture, PublicSourceHttpDelivery,
    PublicSourceReply, PublicSourceRequest,
};
use rust_decimal::Decimal;
use serde::Deserialize;
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

type GetFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + 'a>>;

/// Narrow injection point for recorded public HTTP responses in offline tests.
/// The production implementation is GET-only on a compile-time fixed origin.
trait PublicGet: Send + Sync {
    fn get(&self, path: &'static str, parameters: Vec<(&'static str, String)>) -> GetFuture<'_>;
}

struct BoundedHttpsGet(reqwest::Client);

impl PublicGet for BoundedHttpsGet {
    fn get(&self, path: &'static str, parameters: Vec<(&'static str, String)>) -> GetFuture<'_> {
        Box::pin(async move {
            let mut response = self
                .0
                .get(format!("{PUBLIC_ORIGIN}{path}"))
                .query(&parameters)
                .send()
                .await
                .map_err(|_| "RESEARCH_PUBLIC_TRANSPORT_FAILED".to_owned())?;
            if response.status() != reqwest::StatusCode::OK {
                return Err("RESEARCH_PUBLIC_HTTP_FAILED".into());
            }
            if response.content_length().is_some_and(|size| size > MAX_PUBLIC_BYTES as u64) {
                return Err("RESEARCH_PUBLIC_BODY_TOO_LARGE".into());
            }
            let mut raw = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| "RESEARCH_PUBLIC_TRANSPORT_FAILED".to_owned())?
            {
                if chunk.len() > MAX_PUBLIC_BYTES.saturating_sub(raw.len()) {
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
}

impl IndependentOkxPublicSource {
    pub fn new() -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| "RESEARCH_PUBLIC_CLIENT_INIT_FAILED".to_owned())?;
        Ok(Self {
            get: Arc::new(BoundedHttpsGet(client)),
        })
    }

    #[cfg(test)]
    fn with_get(get: Arc<dyn PublicGet>) -> Self {
        Self { get }
    }
}

impl PublicSourceProvider for IndependentOkxPublicSource {
    fn request(&self, request: PublicSourceRequest) -> SourceFuture<'_> {
        Box::pin(async move {
            if !request.valid() {
                return Err("INVALID_RESEARCH_SOURCE_REQUEST".into());
            }
            let PublicSourceRequest::Candles {
                instrument,
                bar,
                after,
                before,
                limit,
                queued: _,
            } = request
            else {
                return Err("RESEARCH_PUBLIC_OPERATION_UNSUPPORTED".into());
            };
            let mut parameters = vec![("instId", instrument), ("bar", bar)];
            if let Some(cursor) = after {
                parameters.push(("after", cursor));
            }
            if let Some(cursor) = before {
                parameters.push(("before", cursor));
            }
            parameters.push(("limit", limit.to_string()));
            let raw = self.get.get(PUBLIC_CANDLES_PATH, parameters).await?;
            let rows = parse_candles(&raw, usize::from(limit))?;
            let acquired_at_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| "RESEARCH_PUBLIC_CLOCK_INVALID".to_owned())?
                .as_millis()
                .max(1)
                .to_string();
            let response_body_bytes = raw.len() as u64;
            Ok(PublicSourceReply::Candles(PublicCandleCapture {
                raw_body: raw,
                rows,
                acquired_at_ms,
                http_delivery: Some(PublicSourceHttpDelivery {
                    transport_send_invocations: 1,
                    response_status: 200,
                    response_body_bytes,
                }),
            }))
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CandleEnvelope {
    code: String,
    #[serde(default)]
    msg: String,
    data: Vec<Vec<String>>,
}

fn parse_candles(raw: &[u8], max_rows: usize) -> Result<Vec<HistoryCandle>, String> {
    if raw.is_empty() || raw.len() > MAX_PUBLIC_BYTES {
        return Err("RESEARCH_PUBLIC_BODY_INVALID".into());
    }
    let envelope: CandleEnvelope =
        serde_json::from_slice(raw).map_err(|_| "RESEARCH_PUBLIC_CANDLES_INVALID")?;
    if envelope.code != "0" || !envelope.msg.is_empty() || envelope.data.len() > max_rows {
        return Err("RESEARCH_PUBLIC_CANDLES_INVALID".into());
    }
    let mut times = BTreeSet::new();
    let mut output = Vec::with_capacity(envelope.data.len());
    for row in envelope.data {
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
            Decimal::from_str_exact(field)
                .map_err(|_| "RESEARCH_PUBLIC_CANDLES_INVALID".into())
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
mod tests {
    use super::*;
    use crate::source_api::PublicSourceClient;
    use std::sync::Mutex;

    const RECORDED: &str = r#"{"code":"0","msg":"","data":[["1700000060000","100","120","90","110","2","200","220","1"],["1700000000000","100","110","95","105","1","100","1"]]}"#;

    struct RecordedGet {
        raw: Vec<u8>,
        routes: Mutex<Vec<(String, Vec<(&'static str, String)>)>>,
    }
    impl PublicGet for RecordedGet {
        fn get(&self, path: &'static str, parameters: Vec<(&'static str, String)>) -> GetFuture<'_> {
            self.routes.lock().unwrap().push((path.into(), parameters));
            Box::pin(async { Ok(self.raw.clone()) })
        }
    }

    #[tokio::test]
    async fn public_candles_do_not_require_engine_or_private_client() {
        let transport = Arc::new(RecordedGet {
            raw: RECORDED.as_bytes().to_vec(),
            routes: Mutex::new(Vec::new()),
        });
        let client = PublicSourceClient::new(Arc::new(IndependentOkxPublicSource::with_get(
            transport.clone(),
        )));
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
    async fn public_source_rejects_untrusted_requests_without_network() {
        let transport = Arc::new(RecordedGet {
            raw: RECORDED.as_bytes().to_vec(),
            routes: Mutex::new(Vec::new()),
        });
        let source = IndependentOkxPublicSource::with_get(transport.clone());
        assert_eq!(
            source.request(PublicSourceRequest::Candles {
                instrument: "../BTC".into(),
                bar: "1m".into(),
                after: None,
                before: None,
                limit: 2,
                queued: false,
            }).await.unwrap_err(),
            "INVALID_RESEARCH_SOURCE_REQUEST"
        );
        assert_eq!(
            source.request(PublicSourceRequest::Ticker {
                instrument: "BTC-USDT-SWAP".into(),
            }).await.unwrap_err(),
            "RESEARCH_PUBLIC_OPERATION_UNSUPPORTED"
        );
        assert!(transport.routes.lock().unwrap().is_empty());
    }

    #[test]
    fn malformed_exchange_rows_are_fail_closed() {
        assert_eq!(parse_candles(RECORDED.as_bytes(), 1).unwrap_err(), "RESEARCH_PUBLIC_CANDLES_INVALID");
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

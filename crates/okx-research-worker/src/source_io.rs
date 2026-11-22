//! Per-execution observations at the Research public-source capability boundary.
//! These are observed IPC calls, not independently verified venue HTTP sends.
use okx_protocol::research_ipc::{PublicSourceReply, PublicSourceRequest};
use serde::Serialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, future::Future, sync::{Arc, Mutex}};

tokio::task_local! {
    static ACTIVE: Arc<SourceIoMeter>;
}

#[derive(Default, Serialize)]
struct SourceCounts {
    proxy_requests_started: u64,
    proxy_replies: u64,
    proxy_errors: u64,
    captured_response_body_bytes: u64,
    replies_without_raw_body: u64,
}

#[derive(Default)]
struct State {
    by_source: BTreeMap<&'static str, SourceCounts>,
    candle_cache_hits: u64,
}

#[derive(Default)]
pub(crate) struct SourceIoMeter(Mutex<State>);

impl SourceIoMeter {
    pub(crate) fn snapshot(&self) -> Value {
        let state = self.0.lock().unwrap();
        json!({
            "schema": "okx.research.source-io/v1",
            "scope": "initial_market_job_execution",
            "boundary": "worker_public_source_ipc",
            "source_calls": state.by_source,
            "candle_cache_hits": state.candle_cache_hits,
            "venue_http_requests": null,
            "network_bytes_transferred": null,
            "notes": "Counts are worker-observed provider calls and raw captured response body sizes; they do not measure host HTTP retries, TCP bytes, or OS disk I/O. Ticker and OI replies have no raw body in IPC."
        })
    }
}

pub(crate) async fn observe<T>(meter: Arc<SourceIoMeter>, future: impl Future<Output = T>) -> T {
    ACTIVE.scope(meter, future).await
}

fn kind(request: &PublicSourceRequest) -> &'static str {
    match request {
        PublicSourceRequest::Candles { .. } => "candles",
        PublicSourceRequest::Funding { .. } => "funding",
        PublicSourceRequest::Reference { .. } => "reference",
        PublicSourceRequest::Ticker { .. } => "ticker",
        PublicSourceRequest::OpenInterest { .. } => "open_interest",
    }
}

pub(crate) fn request_started(request: &PublicSourceRequest) {
    let _ = ACTIVE.try_with(|meter| {
        meter.0.lock().unwrap().by_source.entry(kind(request))
            .or_default().proxy_requests_started += 1;
    });
}

pub(crate) fn request_finished(request: &PublicSourceRequest, result: &Result<PublicSourceReply, String>) {
    let _ = ACTIVE.try_with(|meter| {
        let mut state = meter.0.lock().unwrap();
        let counts = state.by_source.entry(kind(request)).or_default();
        match result {
            Err(_) => counts.proxy_errors += 1,
            Ok(reply) => {
                counts.proxy_replies += 1;
                match reply {
                    PublicSourceReply::Candles(value) => counts.captured_response_body_bytes += value.raw_body.len() as u64,
                    PublicSourceReply::Funding(value) => counts.captured_response_body_bytes += value.raw_body.len() as u64,
                    PublicSourceReply::Reference(value) => counts.captured_response_body_bytes += value.raw_body.len() as u64,
                    PublicSourceReply::Ticker(_) | PublicSourceReply::OpenInterest(_) => counts.replies_without_raw_body += 1,
                }
            }
        }
    });
}

pub(crate) fn candle_cache_hit() {
    let _ = ACTIVE.try_with(|meter| meter.0.lock().unwrap().candle_cache_hits += 1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn job_scope_isolated_and_captured_bytes_not_network_bytes() {
        let a = Arc::new(SourceIoMeter::default());
        let b = Arc::new(SourceIoMeter::default());
        let req = PublicSourceRequest::Candles {
            instrument: "BTC-USDT-SWAP".into(), bar: "1m".into(), after: None,
            before: None, limit: 100, queued: false,
        };
        let captured = PublicSourceReply::Candles(okx_protocol::research_ipc::PublicCandleCapture {
            raw_body: vec![1, 2, 3, 4], rows: vec![], acquired_at_ms: "1".into(),
        });
        let first = observe(a.clone(), async {
            request_started(&req);
            tokio::task::yield_now().await;
            request_finished(&req, &Ok(captured));
            candle_cache_hit();
        });
        let second = observe(b.clone(), async {
            request_started(&req);
            tokio::task::yield_now().await;
            request_finished(&req, &Err("deferred".into()));
        });
        tokio::join!(first, second);
        let first = a.snapshot();
        let second = b.snapshot();
        assert_eq!(first["source_calls"]["candles"]["proxy_replies"], 1);
        assert_eq!(first["source_calls"]["candles"]["captured_response_body_bytes"], 4);
        assert_eq!(first["candle_cache_hits"], 1);
        assert!(first["venue_http_requests"].is_null());
        assert_eq!(second["source_calls"]["candles"]["proxy_errors"], 1);
        assert_eq!(second["source_calls"]["candles"]["captured_response_body_bytes"], 0);
        assert_eq!(second["candle_cache_hits"], 0);
    }
}

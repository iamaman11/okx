//! Per-execution observations at the Research public-source capability boundary.
//! IPC calls and optional host transport receipts; neither measures venue wire HTTP.
use okx_protocol::research_ipc::{
    PublicSourceHttpDelivery, PublicSourceReply, PublicSourceRequest,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
};

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
    host_transport_receipts: u64,
    host_transport_receipts_missing: u64,
    /// Null if a completed capture lacks a receipt or a source call failed.
    host_transport_send_invocations: Option<u64>,
    host_transport_response_body_bytes: Option<u64>,
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
            "os_physical_disk_reads": null,
            "notes": "Host receipts count explicit HttpTransport::send invocations on successful captured public replies only; not reqwest redirects, failed/ambiguous sends, venue on-wire HTTP, TCP bytes or OS disk I/O. Missing receipts from an older Agent remain unmeasured, never zero. Ticker and OI replies have no raw body or transport receipt in IPC."
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
        meter
            .0
            .lock()
            .unwrap()
            .by_source
            .entry(kind(request))
            .or_default()
            .proxy_requests_started += 1;
    });
}

pub(crate) fn request_finished(
    request: &PublicSourceRequest,
    result: &Result<PublicSourceReply, String>,
) {
    let _ = ACTIVE.try_with(|meter| {
        let mut state = meter.0.lock().unwrap();
        let counts = state.by_source.entry(kind(request)).or_default();
        match result {
            Err(_) => {
                counts.proxy_errors += 1;
                counts.host_transport_send_invocations = None;
                counts.host_transport_response_body_bytes = None;
            }
            Ok(reply) => {
                counts.proxy_replies += 1;
                match reply {
                    PublicSourceReply::Candles(value) => {
                        counts.captured_response_body_bytes += value.raw_body.len() as u64;
                        account_delivery(
                            counts,
                            value.http_delivery.as_ref(),
                            value.raw_body.len(),
                        );
                    }
                    PublicSourceReply::Funding(value) => {
                        counts.captured_response_body_bytes += value.raw_body.len() as u64;
                        account_delivery(
                            counts,
                            value.http_delivery.as_ref(),
                            value.raw_body.len(),
                        );
                    }
                    PublicSourceReply::Reference(value) => {
                        counts.captured_response_body_bytes += value.raw_body.len() as u64;
                        account_delivery(
                            counts,
                            value.http_delivery.as_ref(),
                            value.raw_body.len(),
                        );
                    }
                    PublicSourceReply::Ticker(_) | PublicSourceReply::OpenInterest(_) => {
                        counts.replies_without_raw_body += 1
                    }
                }
            }
        }
    });
}

fn account_delivery(
    counts: &mut SourceCounts,
    receipt: Option<&PublicSourceHttpDelivery>,
    captured_bytes: usize,
) {
    match receipt {
        Some(receipt)
            if receipt.transport_send_invocations > 0
                && (200..300).contains(&receipt.response_status)
                && receipt.response_body_bytes == captured_bytes as u64 =>
        {
            counts.host_transport_receipts += 1;
            if counts.host_transport_receipts_missing == 0 && counts.proxy_errors == 0 {
                counts.host_transport_send_invocations = Some(
                    counts
                        .host_transport_send_invocations
                        .unwrap_or(0)
                        .saturating_add(receipt.transport_send_invocations),
                );
                counts.host_transport_response_body_bytes = Some(
                    counts
                        .host_transport_response_body_bytes
                        .unwrap_or(0)
                        .saturating_add(receipt.response_body_bytes),
                );
            }
        }
        _ => {
            counts.host_transport_receipts_missing += 1;
            counts.host_transport_send_invocations = None;
            counts.host_transport_response_body_bytes = None;
        }
    }
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
            instrument: "BTC-USDT-SWAP".into(),
            bar: "1m".into(),
            after: None,
            before: None,
            limit: 100,
            queued: false,
        };
        let captured =
            PublicSourceReply::Candles(okx_protocol::research_ipc::PublicCandleCapture {
                raw_body: vec![1, 2, 3, 4],
                rows: vec![],
                acquired_at_ms: "1".into(),
                http_delivery: None,
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
        assert_eq!(
            first["source_calls"]["candles"]["captured_response_body_bytes"],
            4
        );
        assert_eq!(first["candle_cache_hits"], 1);
        assert_eq!(
            first["source_calls"]["candles"]["host_transport_receipts_missing"],
            1
        );
        assert!(first["source_calls"]["candles"]["host_transport_send_invocations"].is_null());
        assert!(first["venue_http_requests"].is_null());
        assert_eq!(second["source_calls"]["candles"]["proxy_errors"], 1);
        assert_eq!(
            second["source_calls"]["candles"]["captured_response_body_bytes"],
            0
        );
        assert_eq!(second["candle_cache_hits"], 0);
    }

    #[tokio::test]
    async fn host_receipts_are_only_counted_when_present_and_consistent() {
        let meter = Arc::new(SourceIoMeter::default());
        let req = PublicSourceRequest::Candles {
            instrument: "BTC-USDT-SWAP".into(),
            bar: "1m".into(),
            after: None,
            before: None,
            limit: 100,
            queued: false,
        };
        observe(meter.clone(), async {
            for receipt in [
                Some(PublicSourceHttpDelivery {
                    transport_send_invocations: 1,
                    response_status: 200,
                    response_body_bytes: 3,
                }),
                None,
                Some(PublicSourceHttpDelivery {
                    transport_send_invocations: 1,
                    response_status: 200,
                    response_body_bytes: 99,
                }),
            ] {
                request_started(&req);
                request_finished(
                    &req,
                    &Ok(PublicSourceReply::Candles(
                        okx_protocol::research_ipc::PublicCandleCapture {
                            raw_body: vec![1, 2, 3],
                            rows: vec![],
                            acquired_at_ms: "1".into(),
                            http_delivery: receipt,
                        },
                    )),
                );
            }
        })
        .await;
        let counts = &meter.snapshot()["source_calls"]["candles"];
        assert_eq!(counts["proxy_replies"], 3);
        assert_eq!(counts["host_transport_receipts"], 1);
        assert_eq!(counts["host_transport_receipts_missing"], 2);
        assert!(counts["host_transport_send_invocations"].is_null());
        assert!(counts["host_transport_response_body_bytes"].is_null());
        assert!(meter.snapshot()["venue_http_requests"].is_null());
    }

    #[tokio::test]
    async fn complete_receipts_produce_measured_transport_totals() {
        let meter = Arc::new(SourceIoMeter::default());
        let req = PublicSourceRequest::Candles {
            instrument: "BTC-USDT-SWAP".into(),
            bar: "1m".into(),
            after: None,
            before: None,
            limit: 100,
            queued: false,
        };
        observe(meter.clone(), async {
            for _ in 0..2 {
                request_started(&req);
                request_finished(
                    &req,
                    &Ok(PublicSourceReply::Candles(
                        okx_protocol::research_ipc::PublicCandleCapture {
                            raw_body: vec![1, 2, 3],
                            rows: vec![],
                            acquired_at_ms: "1".into(),
                            http_delivery: Some(PublicSourceHttpDelivery {
                                transport_send_invocations: 1,
                                response_status: 200,
                                response_body_bytes: 3,
                            }),
                        },
                    )),
                );
            }
        })
        .await;
        let counts = &meter.snapshot()["source_calls"]["candles"];
        assert_eq!(counts["host_transport_receipts"], 2);
        assert_eq!(counts["host_transport_receipts_missing"], 0);
        assert_eq!(counts["host_transport_send_invocations"], 2);
        assert_eq!(counts["host_transport_response_body_bytes"], 6);
        assert!(meter.snapshot()["venue_http_requests"].is_null());
    }
}

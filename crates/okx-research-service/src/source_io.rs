//! Per-execution observations at the Research public-source capability boundary.
//! IPC calls and optional host transport receipts; neither measures venue wire HTTP.
use okx_research_protocol::research_ipc::{
    PublicSourceHttpDelivery, PublicSourceReply, PublicSourceRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
};

tokio::task_local! {
    static ACTIVE: Arc<SourceIoMeter>;
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct SourceCounts {
    proxy_requests_started: u64,
    proxy_replies: u64,
    proxy_errors: u64,
    /// The source future was dropped before returning a Result. This does
    /// not establish whether a host HTTP send was attempted.
    #[serde(skip_serializing_if = "is_zero")]
    proxy_requests_aborted: u64,
    captured_response_body_bytes: u64,
    replies_without_raw_body: u64,
    host_transport_receipts: u64,
    host_transport_receipts_missing: u64,
    /// Null if a receipt is absent, a call fails, or a future is dropped.
    host_transport_send_invocations: Option<u64>,
    host_transport_response_body_bytes: Option<u64>,
}

/// Only bounded, aggregate counters. This is safe to persist with a Research
/// job: it contains no URL, parameters, credential, body or raw failure text.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JobSourceObservation {
    scope: String,
    source_calls: BTreeMap<String, SourceCounts>,
    candle_cache_hits: u64,
    /// Wire HTTP cannot be proven from Worker source invocation counters.
    venue_http_requests: Option<u64>,
}

impl JobSourceObservation {
    pub(crate) fn valid(&self) -> bool {
        self.scope == "research_job_execution"
            && self.venue_http_requests.is_none()
            && self.source_calls.len() <= 6
            && self.source_calls.iter().all(|(name, count)| {
                matches!(
                    name.as_str(),
                    "catalog_page"
                        | "candles"
                        | "funding"
                        | "reference"
                        | "ticker"
                        | "open_interest"
                ) && count
                    .proxy_replies
                    .saturating_add(count.proxy_errors)
                    .saturating_add(count.proxy_requests_aborted)
                    <= count.proxy_requests_started
                    && count
                        .host_transport_receipts
                        .saturating_add(count.host_transport_receipts_missing)
                        <= count.proxy_replies
                    && count.replies_without_raw_body <= count.proxy_replies
                    && (count.host_transport_send_invocations.is_some()
                        == count.host_transport_response_body_bytes.is_some())
                    && (count.host_transport_receipts_missing == 0
                        && count.proxy_errors == 0
                        && count.proxy_requests_aborted == 0
                        || count.host_transport_send_invocations.is_none())
            })
    }
}

#[derive(Default)]
struct State {
    by_source: BTreeMap<&'static str, SourceCounts>,
    candle_cache_hits: u64,
}

#[derive(Default)]
pub(crate) struct SourceIoMeter(Mutex<State>);

impl SourceIoMeter {
    pub(crate) fn job_observation(&self) -> Option<JobSourceObservation> {
        // Telemetry failure must never panic the job or forge a clean source
        // history. A poisoned meter remains unobserved.
        let state = self.0.lock().ok()?;
        Some(JobSourceObservation {
            scope: "research_job_execution".into(),
            source_calls: state
                .by_source
                .iter()
                .map(|(key, value)| ((*key).to_owned(), value.clone()))
                .collect(),
            candle_cache_hits: state.candle_cache_hits,
            venue_http_requests: None,
        })
    }

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
            "notes": "Host receipts count explicit HttpTransport::send invocations on successful captured public replies only; not reqwest redirects, failed/ambiguous sends, venue on-wire HTTP, TCP bytes or OS disk I/O. Missing receipts from an older Agent and dropped source futures remain unmeasured, never zero. proxy_requests_aborted means dropped before Result, not that no send occurred. Ticker and OI replies have no raw body or transport receipt in IPC."
        })
    }
}

pub(crate) async fn observe<T>(meter: Arc<SourceIoMeter>, future: impl Future<Output = T>) -> T {
    ACTIVE.scope(meter, future).await
}

// These are observations, not financial totals. A long-running or corrupted
// telemetry counter must never wrap to zero or panic in the Research worker.
fn add_count(counter: &mut u64, amount: u64) {
    *counter = counter.saturating_add(amount);
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

fn kind(request: &PublicSourceRequest) -> &'static str {
    match request {
        PublicSourceRequest::CatalogPage { .. } => "catalog_page",
        PublicSourceRequest::Candles { .. } => "candles",
        PublicSourceRequest::Funding { .. } => "funding",
        PublicSourceRequest::Reference { .. } => "reference",
        PublicSourceRequest::Ticker { .. } => "ticker",
        PublicSourceRequest::OpenInterest { .. } => "open_interest",
    }
}

/// Tracks a single public-source request even when its await is cancelled.
/// The meter Arc must be captured at start: the Tokio task-local may no
/// longer exist by the time this observation guard is dropped.
#[must_use = "a source attempt must be completed or dropped to record its outcome"]
pub(crate) struct SourceAttempt {
    meter: Option<Arc<SourceIoMeter>>,
    source: &'static str,
    finished: bool,
}

pub(crate) fn request_started(request: &PublicSourceRequest) -> SourceAttempt {
    let source = kind(request);
    let meter = ACTIVE.try_with(Arc::clone).ok();
    if let Some(meter) = &meter {
        let mut state = meter.0.lock().unwrap();
        let counts = state.by_source.entry(source).or_default();
        add_count(&mut counts.proxy_requests_started, 1);
    }
    SourceAttempt {
        meter,
        source,
        finished: false,
    }
}

impl SourceAttempt {
    pub(crate) fn finished(mut self, result: &Result<PublicSourceReply, String>) {
        if let Some(meter) = &self.meter {
            let mut state = meter.0.lock().unwrap();
            let counts = state.by_source.entry(self.source).or_default();
            match result {
                Err(_) => {
                    add_count(&mut counts.proxy_errors, 1);
                    counts.host_transport_send_invocations = None;
                    counts.host_transport_response_body_bytes = None;
                }
                Ok(reply) => {
                    add_count(&mut counts.proxy_replies, 1);
                    match reply {
                        PublicSourceReply::CatalogPage(value) => {
                            add_count(
                                &mut counts.captured_response_body_bytes,
                                value.raw_body.len() as u64,
                            );
                            account_delivery(
                                counts,
                                Some(&value.http_delivery),
                                value.raw_body.len(),
                            );
                        }
                        PublicSourceReply::Candles(value) => {
                            add_count(
                                &mut counts.captured_response_body_bytes,
                                value.raw_body.len() as u64,
                            );
                            account_delivery(
                                counts,
                                value.http_delivery.as_ref(),
                                value.raw_body.len(),
                            );
                        }
                        PublicSourceReply::Funding(value) => {
                            add_count(
                                &mut counts.captured_response_body_bytes,
                                value.raw_body.len() as u64,
                            );
                            account_delivery(
                                counts,
                                value.http_delivery.as_ref(),
                                value.raw_body.len(),
                            );
                        }
                        PublicSourceReply::Reference(value) => {
                            add_count(
                                &mut counts.captured_response_body_bytes,
                                value.raw_body.len() as u64,
                            );
                            account_delivery(
                                counts,
                                value.http_delivery.as_ref(),
                                value.raw_body.len(),
                            );
                        }
                        PublicSourceReply::Ticker(_) | PublicSourceReply::OpenInterest(_) => {
                            add_count(&mut counts.replies_without_raw_body, 1);
                        }
                    }
                }
            }
        }
        self.finished = true;
    }
}

impl Drop for SourceAttempt {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Some(meter) = &self.meter {
            // A cancelled request may already have sent bytes. Losing the
            // reply leaves earlier receipt totals incomplete, never zero.
            // Do not introduce a second panic on a poisoned telemetry lock.
            if let Ok(mut state) = meter.0.lock() {
                let counts = state.by_source.entry(self.source).or_default();
                add_count(&mut counts.proxy_requests_aborted, 1);
                counts.host_transport_send_invocations = None;
                counts.host_transport_response_body_bytes = None;
            }
        }
    }
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
            add_count(&mut counts.host_transport_receipts, 1);
            if counts.host_transport_receipts_missing == 0
                && counts.proxy_errors == 0
                && counts.proxy_requests_aborted == 0
            {
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
            add_count(&mut counts.host_transport_receipts_missing, 1);
            counts.host_transport_send_invocations = None;
            counts.host_transport_response_body_bytes = None;
        }
    }
}

pub(crate) fn candle_cache_hit() {
    let _ = ACTIVE.try_with(|meter| {
        let mut state = meter.0.lock().unwrap();
        add_count(&mut state.candle_cache_hits, 1);
    });
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
            PublicSourceReply::Candles(okx_research_protocol::research_ipc::PublicCandleCapture {
                raw_body: vec![1, 2, 3, 4],
                rows: vec![],
                acquired_at_ms: "1".into(),
                http_delivery: None,
            });
        let first = observe(a.clone(), async {
            let attempt = request_started(&req);
            tokio::task::yield_now().await;
            attempt.finished(&Ok(captured));
            candle_cache_hit();
        });
        let second = observe(b.clone(), async {
            let attempt = request_started(&req);
            tokio::task::yield_now().await;
            attempt.finished(&Err("deferred".into()));
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
        assert!(first["source_calls"]["candles"]["proxy_requests_aborted"].is_null());
        assert!(first["venue_http_requests"].is_null());
        assert_eq!(second["source_calls"]["candles"]["proxy_errors"], 1);
        assert!(second["source_calls"]["candles"]["proxy_requests_aborted"].is_null());
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
                request_started(&req).finished(&Ok(PublicSourceReply::Candles(
                    okx_research_protocol::research_ipc::PublicCandleCapture {
                        raw_body: vec![1, 2, 3],
                        rows: vec![],
                        acquired_at_ms: "1".into(),
                        http_delivery: receipt,
                    },
                )));
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
                request_started(&req).finished(&Ok(PublicSourceReply::Candles(
                    okx_research_protocol::research_ipc::PublicCandleCapture {
                        raw_body: vec![1, 2, 3],
                        rows: vec![],
                        acquired_at_ms: "1".into(),
                        http_delivery: Some(PublicSourceHttpDelivery {
                            transport_send_invocations: 1,
                            response_status: 200,
                            response_body_bytes: 3,
                        }),
                    },
                )));
            }
        })
        .await;
        let counts = &meter.snapshot()["source_calls"]["candles"];
        assert_eq!(counts["host_transport_receipts"], 2);
        assert_eq!(counts["host_transport_receipts_missing"], 0);
        assert_eq!(counts["host_transport_send_invocations"], 2);
        assert_eq!(counts["host_transport_response_body_bytes"], 6);
        assert!(counts["proxy_requests_aborted"].is_null());
        assert!(meter.snapshot()["venue_http_requests"].is_null());
    }

    #[tokio::test]
    async fn dropped_source_future_revokes_transport_totals_and_keeps_unknown_send() {
        let meter = Arc::new(SourceIoMeter::default());
        let req = PublicSourceRequest::Candles {
            instrument: "BTC-USDT-SWAP".into(),
            bar: "1m".into(),
            after: None,
            before: None,
            limit: 100,
            queued: false,
        };
        let captured = || {
            Ok(PublicSourceReply::Candles(
                okx_research_protocol::research_ipc::PublicCandleCapture {
                    raw_body: vec![1, 2, 3],
                    rows: vec![],
                    acquired_at_ms: "1".into(),
                    http_delivery: Some(PublicSourceHttpDelivery {
                        transport_send_invocations: 1,
                        response_status: 200,
                        response_body_bytes: 3,
                    }),
                },
            ))
        };
        let aborted = observe(meter.clone(), async {
            request_started(&req).finished(&captured());
            // This is the guard held across an unfinished host-source await.
            // The caller drops the future after the task-local scope ends.
            request_started(&req)
        })
        .await;
        assert_eq!(
            meter.snapshot()["source_calls"]["candles"]["host_transport_send_invocations"],
            1
        );
        drop(aborted);
        observe(meter.clone(), async {
            // A later successful receipt must never resurrect a complete sum
            // after a cancelled/ambiguous source request.
            request_started(&req).finished(&captured());
        })
        .await;
        let snapshot = meter.snapshot();
        let counts = &snapshot["source_calls"]["candles"];
        assert_eq!(counts["proxy_requests_started"], 3);
        assert_eq!(counts["proxy_replies"], 2);
        assert_eq!(counts["proxy_errors"], 0);
        assert_eq!(counts["proxy_requests_aborted"], 1);
        assert_eq!(counts["host_transport_receipts"], 2);
        assert_eq!(counts["host_transport_receipts_missing"], 0);
        assert!(counts["host_transport_send_invocations"].is_null());
        assert!(counts["host_transport_response_body_bytes"].is_null());
        assert!(snapshot["network_bytes_transferred"].is_null());
    }

    #[tokio::test]
    async fn counters_saturate_at_u64_max_without_corrupting_source_diagnostics() {
        let meter = Arc::new(SourceIoMeter::default());
        {
            let mut state = meter.0.lock().unwrap();
            state.candle_cache_hits = u64::MAX;
            state.by_source.insert(
                "candles",
                SourceCounts {
                    proxy_requests_started: u64::MAX,
                    proxy_replies: u64::MAX,
                    proxy_errors: u64::MAX,
                    proxy_requests_aborted: u64::MAX,
                    captured_response_body_bytes: u64::MAX,
                    host_transport_receipts: u64::MAX,
                    host_transport_receipts_missing: u64::MAX,
                    ..Default::default()
                },
            );
        }
        let req = PublicSourceRequest::Candles {
            instrument: "BTC-USDT-SWAP".into(),
            bar: "1m".into(),
            after: None,
            before: None,
            limit: 100,
            queued: false,
        };
        observe(meter.clone(), async {
            request_started(&req).finished(&Ok(PublicSourceReply::Candles(
                okx_research_protocol::research_ipc::PublicCandleCapture {
                    raw_body: vec![1],
                    rows: vec![],
                    acquired_at_ms: "1".into(),
                    http_delivery: None,
                },
            )));
            request_started(&req).finished(&Err("failed".into()));
            drop(request_started(&req));
            candle_cache_hit();
        })
        .await;
        let snapshot = meter.snapshot();
        let counts = &snapshot["source_calls"]["candles"];
        for name in [
            "proxy_requests_started",
            "proxy_replies",
            "proxy_errors",
            "proxy_requests_aborted",
            "captured_response_body_bytes",
            "host_transport_receipts",
            "host_transport_receipts_missing",
        ] {
            assert_eq!(counts[name], u64::MAX, "counter wrapped: {name}");
        }
        assert_eq!(snapshot["candle_cache_hits"], u64::MAX);
        assert!(counts["host_transport_send_invocations"].is_null());
    }
}

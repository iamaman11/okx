//! Local research IPC data only; no financial commands or credentials.
use crate::analytics::{MarketRequest, ResearchRequest};
pub use okx_kernel::observation_model::{FundingHistoryEvent, HistoryCandle};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PROTOCOL_VERSION: u32 = 1;
pub const STORAGE_VERSION: u32 = 1;
pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_IN_FLIGHT: usize = 32;
pub const QUEUE_CAPACITY: usize = 64;
pub const REQUEST_TIMEOUT_MS: u64 = 10_000;
/// Independent admission budgets inside the Agent and its child Worker.
/// A flood of read-only observations cannot consume the emergency STOP/CANCEL lane.
pub const CRITICAL_IN_FLIGHT: usize = 2;
pub const OBSERVATION_IN_FLIGHT: usize = 4;

macro_rules! id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);
        impl $name {
            pub fn new(value: String) -> Result<Self, &'static str> {
                if value.is_empty()
                    || value.len() > 128
                    || !value
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_-:.".contains(&b))
                {
                    return Err("INVALID_RESEARCH_IDENTITY");
                }
                Ok(Self(value))
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                Self::new(String::deserialize(d)?).map_err(serde::de::Error::custom)
            }
        }
    };
}
id!(RequestId);
id!(WorkerSession);
id!(ResearchScope);
id!(JobId);
id!(InvocationId);
/// Public research keys accept any nonempty UTF-8 string up to 128 bytes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct IdempotencyKey(String);
impl IdempotencyKey {
    pub fn new(value: String) -> Result<Self, &'static str> {
        if value.is_empty() || value.len() > 128 {
            return Err("INVALID_RESEARCH_IDENTITY");
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl<'de> Deserialize<'de> for IdempotencyKey {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResearchCommand {
    Research {
        request: ResearchRequest,
    },
    Market {
        request: MarketRequest,
        universe_page: Option<Value>,
        resolved_instruments: Option<Vec<String>>,
    },
    Capabilities,
    Drain,
    Explain {
        job_id: JobId,
    },
    Shutdown,
}

/// Internal scheduling metadata, not a new IPC wire field or public MCP action.
/// Both endpoints derive the same lane from the existing typed command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResearchLane {
    Critical,
    Observation,
    Work,
}
impl ResearchCommand {
    pub fn lane(&self) -> ResearchLane {
        match self {
            Self::Shutdown
            | Self::Drain
            | Self::Research {
                request: ResearchRequest::Cancel { .. },
            } => ResearchLane::Critical,
            Self::Explain { .. }
            | Self::Capabilities
            | Self::Research {
                request: ResearchRequest::Status { .. } | ResearchRequest::Capabilities {},
            } => ResearchLane::Observation,
            _ => ResearchLane::Work,
        }
    }
}

#[cfg(test)]
mod priority_lane_tests {
    use super::*;

    #[test]
    fn t11_t12_control_plane_commands_have_typed_independent_budgets() {
        assert_eq!(CRITICAL_IN_FLIGHT, 2);
        assert_eq!(OBSERVATION_IN_FLIGHT, 4);
        assert_eq!(MAX_IN_FLIGHT, 32);
        let research = |request| ResearchCommand::Research { request };
        assert_eq!(ResearchCommand::Drain.lane(), ResearchLane::Critical);
        assert_eq!(ResearchCommand::Shutdown.lane(), ResearchLane::Critical);
        assert_eq!(
            research(ResearchRequest::Cancel {
                job_id: "job".into(),
                idempotency_key: "cancel".into(),
            })
            .lane(),
            ResearchLane::Critical
        );
        assert_eq!(
            ResearchCommand::Explain {
                job_id: JobId::new("job".into()).unwrap(),
            }
            .lane(),
            ResearchLane::Observation
        );
        assert_eq!(
            ResearchCommand::Capabilities.lane(),
            ResearchLane::Observation
        );
        assert_eq!(
            research(ResearchRequest::Status {
                job_id: "job".into()
            })
            .lane(),
            ResearchLane::Observation
        );
        assert_eq!(
            research(ResearchRequest::Capabilities {}).lane(),
            ResearchLane::Observation
        );
        assert_eq!(
            research(ResearchRequest::Economics {
                cases: vec![],
                data_mode: crate::analytics::ResearchDataMode::Counterfactual,
            })
            .lane(),
            ResearchLane::Work
        );
        assert_eq!(
            research(ResearchRequest::Benchmark {
                idempotency_key: "benchmark".into(),
            })
            .lane(),
            ResearchLane::Work
        );
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HostFrame {
    Hello {
        protocol: u32,
        storage: u32,
        session: WorkerSession,
        scope: ResearchScope,
    },
    Request {
        id: RequestId,
        session: WorkerSession,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        invocation: Option<InvocationId>,
        command: Box<ResearchCommand>,
    },
    SourceReply {
        id: RequestId,
        session: WorkerSession,
        result: Result<PublicSourceReply, String>,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkerFrame {
    Hello {
        protocol: u32,
        storage: u32,
        session: WorkerSession,
        scope: ResearchScope,
        producer_sha256: String,
    },
    Response {
        id: RequestId,
        session: WorkerSession,
        result: Result<WorkerReply, String>,
    },
    SourceRequest {
        id: RequestId,
        session: WorkerSession,
        request: PublicSourceRequest,
    },
}

/// Control outcomes are typed; product projections stay bounded JSON at the wire boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum WorkerReply {
    Product(Value),
    Draining(DrainStatus),
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrainStatus {
    pub active_jobs: u64,
}

impl WorkerReply {
    pub fn product(self) -> Result<Value, String> {
        match self {
            Self::Product(value) => Ok(value),
            _ => Err("WORKER_REPLY_KIND_MISMATCH".into()),
        }
    }
    pub fn drain(self) -> Result<DrainStatus, String> {
        match self {
            Self::Draining(value) => Ok(value),
            _ => Err("WORKER_REPLY_KIND_MISMATCH".into()),
        }
    }
    pub fn shutdown(self) -> Result<(), String> {
        match self {
            Self::Shutdown => Ok(()),
            _ => Err("WORKER_REPLY_KIND_MISMATCH".into()),
        }
    }
}

#[cfg(test)]
mod reply_contract_tests {
    use super::*;

    #[test]
    fn control_replies_have_explicit_kinds_and_product_cannot_authorize_drain() {
        let drain = WorkerReply::Draining(DrainStatus { active_jobs: 2 });
        let bytes = serde_json::to_vec(&drain).unwrap();
        assert_eq!(
            serde_json::from_slice::<WorkerReply>(&bytes)
                .unwrap()
                .drain()
                .unwrap()
                .active_jobs,
            2
        );
        assert!(
            WorkerReply::Product(serde_json::json!({"active_jobs": 0}))
                .drain()
                .is_err()
        );
        assert!(WorkerReply::Shutdown.product().is_err());
        assert!(
            WorkerReply::Draining(DrainStatus { active_jobs: 0 })
                .shutdown()
                .is_err()
        );
        for malformed in [
            r#"{"type":"draining","data":{"active_jobs":-1}}"#,
            r#"{"type":"draining","data":{"active_jobs":0,"ready":true}}"#,
            r#"{"type":"unknown"}"#,
        ] {
            assert!(serde_json::from_str::<WorkerReply>(malformed).is_err());
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum PublicSourceRequest {
    Candles {
        instrument: String,
        bar: String,
        after: Option<String>,
        before: Option<String>,
        limit: u16,
        queued: bool,
    },
    Funding {
        instrument: String,
        after: Option<String>,
        before: Option<String>,
        limit: u16,
    },
    Reference {
        instrument: String,
    },
    Ticker {
        instrument: String,
    },
    OpenInterest {
        instrument: String,
        period: String,
    },
}
impl PublicSourceRequest {
    pub fn valid(&self) -> bool {
        let symbol = |s: &str| {
            !s.is_empty()
                && s.len() <= 64
                && s.bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'-')
        };
        let cursor = |s: &Option<String>| {
            s.as_ref()
                .is_none_or(|s| s.len() <= 32 && s.bytes().all(|b| b.is_ascii_digit()))
        };
        match self {
            Self::Candles {
                instrument,
                bar,
                after,
                before,
                limit,
                ..
            } => {
                symbol(instrument)
                    && !bar.is_empty()
                    && bar.len() <= 16
                    && bar.bytes().all(|b| b.is_ascii_alphanumeric())
                    && cursor(after)
                    && cursor(before)
                    && (1..=100).contains(limit)
            }
            Self::Funding {
                instrument,
                after,
                before,
                limit,
            } => symbol(instrument) && cursor(after) && cursor(before) && (1..=100).contains(limit),
            Self::Reference { instrument } | Self::Ticker { instrument } => symbol(instrument),
            Self::OpenInterest { instrument, period } => {
                symbol(instrument)
                    && !period.is_empty()
                    && period.len() <= 16
                    && period.bytes().all(|b| b.is_ascii_alphanumeric())
            }
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicSourceHttpDelivery {
    /// Explicit host HttpTransport::send calls, not redirects or wire-level sends.
    pub transport_send_invocations: u64,
    pub response_status: u16,
    /// Body bytes returned by the host transport, not total network traffic.
    pub response_body_bytes: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicCandleCapture {
    pub raw_body: Vec<u8>,
    pub rows: Vec<HistoryCandle>,
    pub acquired_at_ms: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_delivery: Option<PublicSourceHttpDelivery>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicFundingCapture {
    pub raw_body: Vec<u8>,
    pub rows: Vec<FundingHistoryEvent>,
    pub acquired_at_ms: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_delivery: Option<PublicSourceHttpDelivery>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicReferenceInstrument {
    pub contract_type: Option<String>,
    pub contract_value: Option<String>,
    /// Unit of ctVal. Without this, ctVal cannot be converted to base assets.
    /// Optional on the wire so an older native peer can still be decoded;
    /// consumers must reject its absence before using contract economics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract_value_currency: Option<String>,
    /// Settlement unit of realized PnL and funding. Older IPC peers omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settle_currency: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicReferenceCapture {
    pub raw_body: Vec<u8>,
    pub instrument: PublicReferenceInstrument,
    pub reference_generation: String,
    pub acquired_at_ms: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_delivery: Option<PublicSourceHttpDelivery>,
}
#[cfg(test)]
mod t10_http_delivery_contract_tests {
    use super::*;

    #[test]
    fn legacy_candle_reply_is_unmeasured_and_new_receipt_is_additive() {
        let old_reply = serde_json::json!({
            "type": "candles",
            "data": {"raw_body": [1, 2, 3], "rows": [], "acquired_at_ms": "1"}
        });
        let legacy: PublicSourceReply = serde_json::from_value(old_reply.clone()).unwrap();
        let PublicSourceReply::Candles(capture) = legacy else {
            panic!("expected candles");
        };
        assert!(capture.http_delivery.is_none());
        assert_eq!(
            serde_json::to_value(PublicSourceReply::Candles(capture)).unwrap(),
            old_reply
        );

        let mut new_reply = old_reply.clone();
        new_reply["data"]["http_delivery"] = serde_json::json!({
            "transport_send_invocations": 1,
            "response_status": 200,
            "response_body_bytes": 3
        });
        let modern: PublicSourceReply = serde_json::from_value(new_reply.clone()).unwrap();
        let PublicSourceReply::Candles(capture) = modern else {
            panic!("expected candles");
        };
        assert_eq!(capture.http_delivery.unwrap().response_body_bytes, 3);
        assert_eq!(
            serde_json::to_value(new_reply).unwrap()["data"]["raw_body"],
            old_reply["data"]["raw_body"]
        );
    }
}
#[cfg(test)]
mod reference_unit_contract_tests {
    use super::*;

    #[test]
    fn t02_reference_ipc_preserves_contract_unit_and_decodes_older_peer() {
        let capture = PublicReferenceCapture {
            raw_body: vec![1, 2, 3],
            instrument: PublicReferenceInstrument {
                contract_type: Some("linear".into()),
                contract_value: Some("0.01".into()),
                contract_value_currency: Some("BTC".into()),
                settle_currency: Some("USDT".into()),
            },
            reference_generation: "sha256:example".into(),
            acquired_at_ms: "123456789".into(),
            http_delivery: None,
        };
        let wire = serde_json::to_value(&capture).unwrap();
        assert_eq!(wire["instrument"]["contract_value_currency"], "BTC");
        assert_eq!(wire["instrument"]["settle_currency"], "USDT");
        let roundtrip: PublicReferenceCapture = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(
            roundtrip.instrument.contract_value_currency.as_deref(),
            Some("BTC")
        );
        assert_eq!(
            roundtrip.instrument.settle_currency.as_deref(),
            Some("USDT")
        );
        let mut old_peer = wire;
        old_peer["instrument"]
            .as_object_mut()
            .unwrap()
            .remove("contract_value_currency");
        old_peer["instrument"]
            .as_object_mut()
            .unwrap()
            .remove("settle_currency");
        let legacy: PublicReferenceCapture = serde_json::from_value(old_peer).unwrap();
        assert_eq!(legacy.instrument.contract_value_currency, None);
        assert_eq!(legacy.instrument.settle_currency, None);
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchTicker {
    #[serde(alias = "bidPx")]
    pub bid_price: String,
    #[serde(alias = "askPx")]
    pub ask_price: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchOpenInterest {
    pub oi: String,
    pub oi_currency: String,
    pub ts: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum PublicSourceReply {
    Candles(PublicCandleCapture),
    Funding(PublicFundingCapture),
    Reference(PublicReferenceCapture),
    Ticker(ResearchTicker),
    OpenInterest(Vec<ResearchOpenInterest>),
}
/// Preserve the existing public source backpressure classification across IPC.
pub fn is_retryable_source_defer(error: &str) -> bool {
    error.contains("OKX rate/backpressure defer:")
}

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
    },
    Response {
        id: RequestId,
        session: WorkerSession,
        result: Result<Value, String>,
    },
    SourceRequest {
        id: RequestId,
        session: WorkerSession,
        request: PublicSourceRequest,
    },
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
pub struct PublicCandleCapture {
    pub raw_body: Vec<u8>,
    pub rows: Vec<HistoryCandle>,
    pub acquired_at_ms: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicFundingCapture {
    pub raw_body: Vec<u8>,
    pub rows: Vec<FundingHistoryEvent>,
    pub acquired_at_ms: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicReferenceInstrument {
    pub contract_type: Option<String>,
    pub contract_value: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicReferenceCapture {
    pub raw_body: Vec<u8>,
    pub instrument: PublicReferenceInstrument,
    pub reference_generation: String,
    pub acquired_at_ms: String,
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

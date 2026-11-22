//! Local research IPC data only; no financial commands or credentials.
use crate::analytics::{MarketRequest, ResearchRequest};
use crate::graph_wire::GraphEnvelope;
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
id!(GraphSessionId);
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
    /// Internal read-only signal calculation. No finance or order authority.
    EvaluateSignal {
        request: SignalRequest,
    },
    /// Isolated, bounded, read-only incremental graph session.
    GraphSignal {
        request: GraphSessionRequest,
    },
    /// Read the durable Research signal journal after a verified consumer cursor.
    /// This never advances graph state and cannot publish an order intent.
    ReplayGraphPublications {
        request: GraphPublicationRequest,
    },
    /// Start or stop a Research-owned public-candle feed, not a trading loop.
    GraphFeed {
        request: GraphFeedRequest,
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
    /// Unsolicited read-only Research publication. This is never a trade
    /// instruction and does not replace the correlated command reply.
    SignalPublished {
        session: WorkerSession,
        publication: GraphSignalPublication,
    },
    /// A feed has stopped; consumers must fail closed, not infer Hold/Flat.
    GraphFeedFault {
        session: WorkerSession,
        fault: GraphFeedFault,
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
    Signal(SignalDecision),
    GraphSignal(GraphSessionReply),
    GraphPublications(GraphPublicationPage),
    GraphFeed(GraphFeedReply),
    Draining(DrainStatus),
    Shutdown,
}

/// Bounded fully-closed history supplied by Engine. The Worker cannot fetch
/// private account state or issue financial commands through this request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalRequest {
    pub strategy_id: String,
    pub parameters: crate::analytics::EmaParameters,
    pub closes: Vec<ClosedPrice>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClosedPrice {
    pub time_ms: u64,
    pub close: f64,
}

/// An exact closed-bar observation; an absent close is unknown, never zero.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphClosedBar {
    pub time_ms: u64,
    pub close: Option<f64>,
}

impl GraphClosedBar {
    pub fn valid(self) -> bool {
        self.time_ms > 0
            && self.time_ms <= i64::MAX as u64
            && self
                .close
                .is_none_or(|value| value.is_finite() && value > 0.0)
    }
}

/// Stable wire contract. Operator semantics, units and graph compilation are
/// deliberately NOT owned by Protocol or Agent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphSignalDefinition {
    pub version: String,
    pub graph: GraphEnvelope,
    pub signal_step: String,
    pub target_if_true: i8,
    pub target_if_false: i8,
    pub missing: GraphMissingPolicy,
}

impl GraphSignalDefinition {
    /// Wire admission only. Mathematical operator IDs, versions, and units
    /// remain exclusively owned by the Research compiler.
    pub fn structurally_valid(&self) -> bool {
        self.version == "okx.graph-signal/v1"
            && self.graph.structurally_valid()
            && self.graph.outputs.contains(&self.signal_step)
            && (-1..=1).contains(&self.target_if_true)
            && (-1..=1).contains(&self.target_if_false)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphMissingPolicy {
    Hold,
    Flat,
}

/// Restore is a one-time prefix replay, then Advance sends exactly one new
/// candle per call. No order controls or financial state cross this boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum GraphSessionRequest {
    Restore {
        session_id: GraphSessionId,
        definition: Box<GraphSignalDefinition>,
        capacity: u64,
        history: Vec<GraphClosedBar>,
    },
    /// One-time Research-owned public-candle prefix. It does not start a feed,
    /// publish a trading intent, or grant financial execution authority.
    Bootstrap {
        session_id: GraphSessionId,
        definition: Box<GraphSignalDefinition>,
        capacity: u64,
        instrument: String,
        timeframe_ms: u64,
        history_bars: u16,
    },
    Advance {
        session_id: GraphSessionId,
        graph_sha256: String,
        expected_bars: u64,
        bar: GraphClosedBar,
    },
    Release {
        session_id: GraphSessionId,
        graph_sha256: String,
    },
}

impl GraphSessionRequest {
    pub fn valid(&self) -> bool {
        fn digest_valid(hash: &str) -> bool {
            hash.len() == 64
                && hash
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        }
        match self {
            Self::Restore {
                definition,
                capacity,
                history,
                ..
            } => {
                definition.structurally_valid()
                    && (1..=10_000).contains(&history.len())
                    && *capacity >= history.len() as u64
                    && *capacity <= 100_000
                    && history.iter().all(|bar| bar.valid())
                    && history
                        .windows(2)
                        .all(|pair| pair[0].time_ms < pair[1].time_ms)
            }
            Self::Bootstrap {
                definition,
                capacity,
                instrument,
                timeframe_ms,
                history_bars,
                ..
            } => {
                definition.structurally_valid()
                    && (2..=256).contains(history_bars)
                    && *capacity > u64::from(*history_bars)
                    && *capacity <= 100_000
                    && graph_feed_instrument_valid(instrument)
                    && graph_feed_timeframe_valid(*timeframe_ms)
            }
            Self::Advance {
                graph_sha256,
                expected_bars,
                bar,
                ..
            } => digest_valid(graph_sha256) && *expected_bars > 0 && bar.valid(),
            Self::Release { graph_sha256, .. } => digest_valid(graph_sha256),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphPosition {
    Flat,
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphSignalIntent {
    Hold,
    SetTarget(GraphPosition),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum GraphSessionReply {
    Evaluated {
        schema: String,
        session_id: GraphSessionId,
        graph_sha256: String,
        last_closed_ms: u64,
        observed_bars: u64,
        intent: GraphSignalIntent,
    },
    Released {
        session_id: GraphSessionId,
    },
}

impl GraphSessionReply {
    pub const SCHEMA: &'static str = "okx.research.graph-signal/v1";

    pub fn matches_request(&self, request: &GraphSessionRequest) -> bool {
        match (self, request) {
            (
                Self::Evaluated {
                    schema,
                    session_id: response_id,
                    graph_sha256,
                    last_closed_ms,
                    observed_bars,
                    ..
                },
                GraphSessionRequest::Restore {
                    session_id,
                    history,
                    ..
                },
            ) => {
                schema == Self::SCHEMA
                    && response_id == session_id
                    && graph_sha256.len() == 64
                    && *last_closed_ms == history.last().map_or(0, |bar| bar.time_ms)
                    && *observed_bars == history.len() as u64
            }
            (
                Self::Evaluated {
                    schema,
                    session_id: response_id,
                    graph_sha256,
                    last_closed_ms,
                    observed_bars,
                    ..
                },
                GraphSessionRequest::Bootstrap {
                    session_id,
                    timeframe_ms,
                    history_bars,
                    ..
                },
            ) => {
                request.valid()
                    && schema == Self::SCHEMA
                    && response_id == session_id
                    && graph_sha256.len() == 64
                    && graph_sha256
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                    && *last_closed_ms > 0
                    && *last_closed_ms % timeframe_ms == 0
                    && *observed_bars == u64::from(*history_bars)
            }
            (
                Self::Evaluated {
                    schema,
                    session_id: response_id,
                    graph_sha256: response_sha,
                    last_closed_ms,
                    observed_bars,
                    ..
                },
                GraphSessionRequest::Advance {
                    session_id,
                    graph_sha256,
                    expected_bars,
                    bar,
                },
            ) => {
                schema == Self::SCHEMA
                    && response_id == session_id
                    && response_sha == graph_sha256
                    && *last_closed_ms == bar.time_ms
                    && *observed_bars == *expected_bars
            }
            (
                Self::Released {
                    session_id: response_id,
                },
                GraphSessionRequest::Release { session_id, .. },
            ) => response_id == session_id,
            _ => false,
        }
    }
}

/// Immutable closed-bar intent published after a NEW graph advancement.
/// A replay/restoration never publishes an old decision as a new event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphSignalPublication {
    pub schema: String,
    pub session_id: GraphSessionId,
    pub graph_sha256: String,
    pub closed_ms: u64,
    pub sequence: u64,
    pub intent: GraphSignalIntent,
}

impl GraphSignalPublication {
    pub fn from_decision(reply: &GraphSessionReply) -> Option<Self> {
        let GraphSessionReply::Evaluated {
            schema,
            session_id,
            graph_sha256,
            last_closed_ms,
            observed_bars,
            intent,
        } = reply
        else {
            return None;
        };
        Some(Self {
            schema: schema.clone(),
            session_id: session_id.clone(),
            graph_sha256: graph_sha256.clone(),
            closed_ms: *last_closed_ms,
            sequence: *observed_bars,
            intent: *intent,
        })
    }

    pub fn valid(&self) -> bool {
        self.schema == GraphSessionReply::SCHEMA
            && self.graph_sha256.len() == 64
            && self
                .graph_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            && self.closed_ms > 0
            && self.closed_ms <= i64::MAX as u64
            && (1..=100_000).contains(&self.sequence)
    }
}

/// The consumer supplies its last *durably accepted* sequence. A replay starts
/// strictly after it; skipped or conflicting events are hard failures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphPublicationRequest {
    pub session_id: GraphSessionId,
    pub graph_sha256: String,
    pub after_sequence: u64,
    pub limit: u16,
}

impl GraphPublicationRequest {
    pub const MAX_PAGE: u16 = 64;

    pub fn valid(&self) -> bool {
        (1..=Self::MAX_PAGE).contains(&self.limit)
            && self.after_sequence <= 100_000
            && self.graph_sha256.len() == 64
            && self
                .graph_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphPublicationPage {
    pub session_id: GraphSessionId,
    pub graph_sha256: String,
    pub after_sequence: u64,
    pub publications: Vec<GraphSignalPublication>,
    pub has_more: bool,
}

impl GraphPublicationPage {
    pub fn matches_request(&self, request: &GraphPublicationRequest) -> bool {
        request.valid()
            && self.session_id == request.session_id
            && self.graph_sha256 == request.graph_sha256
            && self.after_sequence == request.after_sequence
            && self.publications.len() <= request.limit as usize
            && (!self.has_more || self.publications.len() == request.limit as usize)
            && self
                .publications
                .iter()
                .enumerate()
                .all(|(index, publication)| {
                    publication.valid()
                        && publication.session_id == request.session_id
                        && publication.graph_sha256 == request.graph_sha256
                        && publication.sequence == request.after_sequence + 1 + index as u64
                })
    }
}

/// The feed is owned by Research after a one-time session Restore. It uses
/// only the authenticated public-candle source capability. No account/risk
/// state, order command, or private credential is accepted on this route.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum GraphFeedRequest {
    Start {
        session_id: GraphSessionId,
        graph_sha256: String,
        instrument: String,
        timeframe_ms: u64,
        poll_every_ms: u64,
    },
    Stop {
        session_id: GraphSessionId,
        graph_sha256: String,
    },
}

fn graph_feed_instrument_valid(instrument: &str) -> bool {
    (3..=40).contains(&instrument.len())
        && instrument
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'-')
}

fn graph_feed_timeframe_valid(timeframe_ms: u64) -> bool {
    matches!(
        timeframe_ms,
        60_000 | 180_000 | 300_000 | 900_000 | 1_800_000 | 3_600_000 | 14_400_000 | 86_400_000
    )
}

impl GraphFeedRequest {
    pub fn valid(&self) -> bool {
        let hash = match self {
            Self::Start { graph_sha256, .. } | Self::Stop { graph_sha256, .. } => graph_sha256,
        };
        if hash.len() != 64
            || !hash
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return false;
        }
        match self {
            Self::Start {
                instrument,
                timeframe_ms,
                poll_every_ms,
                ..
            } => {
                graph_feed_instrument_valid(instrument)
                    && graph_feed_timeframe_valid(*timeframe_ms)
                    && (1_000..=300_000).contains(poll_every_ms)
            }
            Self::Stop { .. } => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum GraphFeedReply {
    Started {
        session_id: GraphSessionId,
        graph_sha256: String,
        last_closed_ms: u64,
    },
    Stopped {
        session_id: GraphSessionId,
        graph_sha256: String,
    },
}

impl GraphFeedReply {
    pub fn matches_request(&self, request: &GraphFeedRequest) -> bool {
        match (self, request) {
            (
                Self::Started {
                    session_id: a,
                    graph_sha256: h,
                    last_closed_ms,
                },
                GraphFeedRequest::Start {
                    session_id: b,
                    graph_sha256,
                    timeframe_ms,
                    ..
                },
            ) => {
                request.valid()
                    && a == b
                    && h == graph_sha256
                    && *last_closed_ms > 0
                    && *last_closed_ms % timeframe_ms == 0
            }
            (
                Self::Stopped {
                    session_id: a,
                    graph_sha256: h,
                },
                GraphFeedRequest::Stop {
                    session_id: b,
                    graph_sha256,
                },
            ) => request.valid() && a == b && h == graph_sha256,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphFeedFaultReason {
    PublicSourceUnavailable,
    MissingClosedBar,
    SessionUnavailable,
    PublicationUnavailable,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphFeedFault {
    pub session_id: GraphSessionId,
    pub graph_sha256: String,
    pub reason: GraphFeedFaultReason,
}

impl GraphFeedFault {
    pub fn valid(&self) -> bool {
        self.graph_sha256.len() == 64
            && self
                .graph_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }
}

/// A consumer must start from its explicitly restored cursor; it may not
/// accept an arbitrary initial signal or skip a closed bar after lag/restart.
/// A repeated identical publication is harmless, a changed replay is not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphPublicationCursor {
    session_id: GraphSessionId,
    graph_sha256: String,
    closed_ms: u64,
    sequence: u64,
    intent: GraphSignalIntent,
}

impl GraphPublicationCursor {
    /// Rebuild only from an Engine-owned, already committed SQL checkpoint.
    /// A caller must first verify it against the active strategy binding.
    pub fn from_checkpoint(
        session_id: GraphSessionId,
        graph_sha256: String,
        closed_ms: u64,
        sequence: u64,
        intent: GraphSignalIntent,
    ) -> Result<Self, &'static str> {
        let event = GraphSignalPublication {
            schema: GraphSessionReply::SCHEMA.into(),
            session_id,
            graph_sha256,
            closed_ms,
            sequence,
            intent,
        };
        if !event.valid() {
            return Err("GRAPH_CURSOR_CHECKPOINT_INVALID");
        }
        Ok(Self {
            session_id: event.session_id,
            graph_sha256: event.graph_sha256,
            closed_ms: event.closed_ms,
            sequence: event.sequence,
            intent: event.intent,
        })
    }

    pub fn from_restored(reply: &GraphSessionReply) -> Result<Self, &'static str> {
        let Some(event) = GraphSignalPublication::from_decision(reply) else {
            return Err("GRAPH_CURSOR_RESTORE_REQUIRED");
        };
        if !event.valid() {
            return Err("GRAPH_CURSOR_RESTORE_INVALID");
        }
        Ok(Self {
            session_id: event.session_id,
            graph_sha256: event.graph_sha256,
            closed_ms: event.closed_ms,
            sequence: event.sequence,
            intent: event.intent,
        })
    }

    /// True only for the next never-before-seen bar. No state mutation on
    /// missing/reordered/forged events. The actual risk decision is separate.
    pub fn accept(&mut self, event: &GraphSignalPublication) -> Result<bool, &'static str> {
        if !event.valid()
            || event.session_id != self.session_id
            || event.graph_sha256 != self.graph_sha256
        {
            return Err("GRAPH_PUBLICATION_IDENTITY_MISMATCH");
        }
        if event.sequence == self.sequence {
            return if event.closed_ms == self.closed_ms && event.intent == self.intent {
                Ok(false)
            } else {
                Err("GRAPH_PUBLICATION_CONFLICTING_REPLAY")
            };
        }
        if self.sequence.checked_add(1) != Some(event.sequence) {
            return Err("GRAPH_PUBLICATION_SEQUENCE_GAP");
        }
        if event.closed_ms <= self.closed_ms {
            return Err("GRAPH_PUBLICATION_NOT_MONOTONIC");
        }
        self.sequence = event.sequence;
        self.closed_ms = event.closed_ms;
        self.intent = event.intent;
        Ok(true)
    }

    pub fn last_sequence(&self) -> u64 {
        self.sequence
    }

    pub fn last_closed_ms(&self) -> u64 {
        self.closed_ms
    }

    pub fn last_intent(&self) -> GraphSignalIntent {
        self.intent
    }

    pub fn matches_binding(&self, session_id: &GraphSessionId, graph_sha256: &str) -> bool {
        &self.session_id == session_id && self.graph_sha256 == graph_sha256
    }
}

#[cfg(test)]
mod graph_signal_contract_tests {
    use super::*;
    use serde_json::json;

    fn sample() -> GraphSessionRequest {
        serde_json::from_value(json!({
            "type":"restore", "session_id":"instance:btc", "capacity":100,
            "definition":{
                "version":"okx.graph-signal/v1",
                "graph":{"schema":"okx.graph/v1","steps":[
                    {"id":"return","op":{"id":"returns_series","version":1},"inputs":[{"source":"close"}]},
                    {"id":"positive","op":{"id":"greater_than","version":1},
                     "inputs":[{"step":"return"}],"params":{"threshold":0.0}}
                ],"outputs":["positive"]},
                "signal_step":"positive", "target_if_true":1,"target_if_false":0,
                "missing":"hold"
            },
            "history":[{"time_ms":60000,"close":100.0},{"time_ms":120000,"close":110.0}]
        }))
        .unwrap()
    }

    #[test]
    fn graph_public_bootstrap_has_strict_bounds_and_typed_restore_reply() {
        let GraphSessionRequest::Restore {
            session_id,
            definition,
            ..
        } = sample()
        else {
            unreachable!()
        };
        let good = GraphSessionRequest::Bootstrap {
            session_id: session_id.clone(),
            definition,
            capacity: 10_000,
            instrument: "BTC-USDT-SWAP".into(),
            timeframe_ms: 60_000,
            history_bars: 64,
        };
        assert!(good.valid());
        let wire = serde_json::to_value(&good).unwrap();
        assert_eq!(wire["type"], "bootstrap");
        assert_eq!(
            serde_json::from_value::<GraphSessionRequest>(wire).unwrap(),
            good
        );
        let restored = GraphSessionReply::Evaluated {
            schema: GraphSessionReply::SCHEMA.into(),
            session_id,
            graph_sha256: "a".repeat(64),
            last_closed_ms: 100 * 60_000,
            observed_bars: 64,
            intent: GraphSignalIntent::Hold,
        };
        assert!(restored.matches_request(&good));
        let mut bad = good.clone();
        if let GraphSessionRequest::Bootstrap { history_bars, .. } = &mut bad {
            *history_bars = 257;
        }
        assert!(!bad.valid());
        bad = good.clone();
        if let GraphSessionRequest::Bootstrap { instrument, .. } = &mut bad {
            *instrument = "private/account".into();
        }
        assert!(!bad.valid());
        bad = good.clone();
        if let GraphSessionRequest::Bootstrap { capacity, .. } = &mut bad {
            *capacity = 64;
        }
        assert!(!bad.valid());
        bad = good.clone();
        if let GraphSessionRequest::Bootstrap { timeframe_ms, .. } = &mut bad {
            *timeframe_ms = 1_000;
        }
        assert!(!bad.valid());
        let mut forged = restored.clone();
        if let GraphSessionReply::Evaluated { observed_bars, .. } = &mut forged {
            *observed_bars -= 1;
        }
        assert!(!forged.matches_request(&good));
    }

    #[test]
    fn typed_graph_session_admission_limits_history_versions_and_replays() {
        let good = sample();
        assert!(good.valid());
        let roundtrip: GraphSessionRequest =
            serde_json::from_value(serde_json::to_value(&good).unwrap()).unwrap();
        assert_eq!(good, roundtrip);

        let mut out_of_order = good.clone();
        let GraphSessionRequest::Restore { history, .. } = &mut out_of_order else {
            unreachable!()
        };
        history[1].time_ms = 60000;
        assert!(!out_of_order.valid());

        let mut forged_graph = good.clone();
        let GraphSessionRequest::Restore { definition, .. } = &mut forged_graph else {
            unreachable!()
        };
        definition.graph.schema = "future-contract".into();
        assert!(!forged_graph.valid());

        let mut too_many = good.clone();
        let GraphSessionRequest::Restore { capacity, .. } = &mut too_many else {
            unreachable!()
        };
        *capacity = u64::MAX;
        assert!(!too_many.valid());

        let mut unknown = serde_json::to_value(good).unwrap();
        unknown["unexpected"] = json!("not-a-financial-command");
        assert!(serde_json::from_value::<GraphSessionRequest>(unknown).is_err());
    }

    #[test]
    fn flat_is_an_explicit_target_and_reply_must_match_admitted_sequence() {
        let restore = sample();
        let GraphSessionRequest::Restore {
            session_id,
            history,
            ..
        } = &restore
        else {
            unreachable!()
        };
        let response = GraphSessionReply::Evaluated {
            schema: GraphSessionReply::SCHEMA.into(),
            session_id: session_id.clone(),
            graph_sha256: "a".repeat(64),
            last_closed_ms: history[1].time_ms,
            observed_bars: 2,
            intent: GraphSignalIntent::SetTarget(GraphPosition::Flat),
        };
        assert!(response.matches_request(&restore));
        let advance = GraphSessionRequest::Advance {
            session_id: session_id.clone(),
            graph_sha256: "a".repeat(64),
            expected_bars: 3,
            bar: GraphClosedBar {
                time_ms: 180000,
                close: None,
            },
        };
        assert!(!response.matches_request(&advance));
        assert_eq!(
            serde_json::to_value(GraphSignalIntent::SetTarget(GraphPosition::Flat)).unwrap(),
            json!({"set_target":"flat"})
        );
        assert_eq!(
            serde_json::to_value(GraphSignalIntent::Hold).unwrap(),
            "hold"
        );
    }

    #[test]
    fn publication_requires_restored_cursor_and_rejects_gaps_or_conflicting_replay() {
        let restore = GraphSessionReply::Evaluated {
            schema: GraphSessionReply::SCHEMA.into(),
            session_id: GraphSessionId::new("session:btc".into()).unwrap(),
            graph_sha256: "a".repeat(64),
            last_closed_ms: 100,
            observed_bars: 2,
            intent: GraphSignalIntent::Hold,
        };
        let mut cursor = GraphPublicationCursor::from_restored(&restore).unwrap();
        let mut publication = GraphSignalPublication::from_decision(&restore).unwrap();
        assert!(publication.valid());
        assert!(!cursor.accept(&publication).unwrap());
        publication.sequence = 4;
        publication.closed_ms = 200;
        assert_eq!(
            cursor.accept(&publication).unwrap_err(),
            "GRAPH_PUBLICATION_SEQUENCE_GAP"
        );
        assert_eq!(cursor.last_sequence(), 2);
        publication.sequence = 3;
        publication.closed_ms = 100;
        assert_eq!(
            cursor.accept(&publication).unwrap_err(),
            "GRAPH_PUBLICATION_NOT_MONOTONIC"
        );
        publication.closed_ms = 200;
        publication.intent = GraphSignalIntent::SetTarget(GraphPosition::Flat);
        assert!(cursor.accept(&publication).unwrap());
        assert!(!cursor.accept(&publication).unwrap());
        publication.intent = GraphSignalIntent::Hold;
        assert_eq!(
            cursor.accept(&publication).unwrap_err(),
            "GRAPH_PUBLICATION_CONFLICTING_REPLAY"
        );
        publication.sequence = 4;
        publication.closed_ms = 300;
        publication.graph_sha256 = "b".repeat(64);
        assert_eq!(
            cursor.accept(&publication).unwrap_err(),
            "GRAPH_PUBLICATION_IDENTITY_MISMATCH"
        );
        assert_eq!(cursor.last_sequence(), 3);
    }

    #[test]
    fn replay_page_is_bounded_and_rejects_forged_or_skipped_publications() {
        let request = GraphPublicationRequest {
            session_id: GraphSessionId::new("session:btc".into()).unwrap(),
            graph_sha256: "b".repeat(64),
            after_sequence: 2,
            limit: 1,
        };
        let publication = GraphSignalPublication {
            schema: GraphSessionReply::SCHEMA.into(),
            session_id: request.session_id.clone(),
            graph_sha256: request.graph_sha256.clone(),
            closed_ms: 180_000,
            sequence: 3,
            intent: GraphSignalIntent::Hold,
        };
        let page = GraphPublicationPage {
            session_id: request.session_id.clone(),
            graph_sha256: request.graph_sha256.clone(),
            after_sequence: 2,
            publications: vec![publication.clone()],
            has_more: true,
        };
        assert!(page.matches_request(&request));
        let mut forged = page.clone();
        forged.publications[0].sequence = 4;
        assert!(!forged.matches_request(&request));
        forged = page.clone();
        forged.publications[0].graph_sha256 = "a".repeat(64);
        assert!(!forged.matches_request(&request));
        forged = page.clone();
        forged.publications.push(publication);
        assert!(!forged.matches_request(&request));
        let wire = serde_json::to_value(ResearchCommand::ReplayGraphPublications {
            request: request.clone(),
        })
        .unwrap();
        assert_eq!(wire["type"], "replay_graph_publications");
        assert!(serde_json::from_value::<ResearchCommand>(wire).is_ok());
        assert!(
            !GraphPublicationRequest {
                limit: 65,
                ..request
            }
            .valid()
        );
    }

    #[test]
    fn graph_feed_is_public_only_typed_bounded_and_explicitly_faults() {
        let start = GraphFeedRequest::Start {
            session_id: GraphSessionId::new("feed:btc".into()).unwrap(),
            graph_sha256: "a".repeat(64),
            instrument: "BTC-USDT".into(),
            timeframe_ms: 14_400_000,
            poll_every_ms: 1_000,
        };
        assert!(start.valid());
        let GraphFeedRequest::Start {
            session_id,
            graph_sha256,
            timeframe_ms,
            ..
        } = start.clone()
        else {
            unreachable!()
        };
        assert!(
            GraphFeedReply::Started {
                session_id: session_id.clone(),
                graph_sha256: graph_sha256.clone(),
                last_closed_ms: 2 * timeframe_ms,
            }
            .matches_request(&start)
        );
        let mut forged = start.clone();
        if let GraphFeedRequest::Start { poll_every_ms, .. } = &mut forged {
            *poll_every_ms = 10;
        }
        assert!(!forged.valid());
        forged = start.clone();
        if let GraphFeedRequest::Start { instrument, .. } = &mut forged {
            *instrument = "account/private".into();
        }
        assert!(!forged.valid());
        forged = start.clone();
        if let GraphFeedRequest::Start { timeframe_ms, .. } = &mut forged {
            *timeframe_ms = 7_000;
        }
        assert!(!forged.valid());
        let wire = serde_json::to_value(ResearchCommand::GraphFeed { request: start }).unwrap();
        assert_eq!(wire["type"], "graph_feed");
        assert!(serde_json::from_value::<ResearchCommand>(wire).is_ok());
        assert!(
            GraphFeedFault {
                session_id,
                graph_sha256,
                reason: GraphFeedFaultReason::MissingClosedBar,
            }
            .valid()
        );
    }
}

impl SignalRequest {
    pub fn valid(&self) -> bool {
        !self.closes.is_empty()
            && self.closes.len() <= 10_000
            && self.parameters.validate().is_ok()
            && !self.strategy_id.is_empty()
            && self.strategy_id.len() <= 128
            && self
                .closes
                .iter()
                .all(|bar| bar.close.is_finite() && bar.close > 0.0 && bar.time_ms > 0)
            && self
                .closes
                .windows(2)
                .all(|pair| pair[0].time_ms < pair[1].time_ms)
    }
}

/// A computation result, never an authorization for trade execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalPosition {
    Long,
    Short,
    Flat,
    Warmup,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalDecision {
    pub schema: String,
    pub last_closed_ms: u64,
    pub observed_bars: usize,
    pub position: SignalPosition,
}

impl SignalDecision {
    pub const SCHEMA: &'static str = "okx.research.signal/v1";

    pub fn matches_request(&self, request: &SignalRequest) -> bool {
        self.schema == Self::SCHEMA
            && self.last_closed_ms == request.closes.last().map_or(0, |bar| bar.time_ms)
            && self.observed_bars == request.closes.len()
    }
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
    pub fn signal(self) -> Result<SignalDecision, String> {
        match self {
            Self::Signal(decision) => Ok(decision),
            _ => Err("WORKER_REPLY_KIND_MISMATCH".into()),
        }
    }
    pub fn graph_signal(self) -> Result<GraphSessionReply, String> {
        match self {
            Self::GraphSignal(reply) => Ok(reply),
            _ => Err("WORKER_REPLY_KIND_MISMATCH".into()),
        }
    }
    pub fn graph_publications(self) -> Result<GraphPublicationPage, String> {
        match self {
            Self::GraphPublications(page) => Ok(page),
            _ => Err("WORKER_REPLY_KIND_MISMATCH".into()),
        }
    }
    pub fn graph_feed(self) -> Result<GraphFeedReply, String> {
        match self {
            Self::GraphFeed(reply) => Ok(reply),
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

    #[test]
    fn signal_ipc_rejects_noncausal_inputs_and_has_no_financial_commands() {
        let request = SignalRequest {
            strategy_id: "ema_cross_v1".into(),
            parameters: crate::analytics::EmaParameters { fast: 2, slow: 4 },
            closes: (1..=4)
                .map(|time_ms| ClosedPrice {
                    time_ms,
                    close: 100.0,
                })
                .collect(),
        };
        assert!(request.valid());
        let outcome = SignalDecision {
            schema: SignalDecision::SCHEMA.into(),
            last_closed_ms: 4,
            observed_bars: 4,
            position: SignalPosition::Flat,
        };
        assert!(outcome.matches_request(&request));
        let wire = serde_json::to_vec(&WorkerReply::Signal(outcome)).unwrap();
        let decoded = serde_json::from_slice::<WorkerReply>(&wire)
            .unwrap()
            .signal()
            .unwrap();
        assert_eq!(decoded.position, SignalPosition::Flat);
        let mut invalid = request.clone();
        invalid.closes[1].time_ms = invalid.closes[0].time_ms;
        assert!(!invalid.valid());
        invalid = request;
        invalid.closes[0].close = f64::INFINITY;
        assert!(!invalid.valid());
        assert!(
            WorkerReply::Product(serde_json::json!({"position":"long"}))
                .signal()
                .is_err()
        );
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

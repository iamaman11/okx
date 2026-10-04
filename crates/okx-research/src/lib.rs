mod replay;

pub use replay::*;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use okx_observation::{
    FundingHistoryEvent, HistoryCandle, InstrumentSpec, MarketTrade, MarketTradeSide,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const DATASET_MANIFEST_SCHEMA_V1: &str = "okx.research.dataset-manifest/v1";
pub const CHUNK_MANIFEST_SCHEMA_V1: &str = "okx.research.chunk-manifest/v1";
pub const SOURCE_CAPTURE_SCHEMA_V1: &str = "okx.research.source-capture/v1";
pub const REFERENCE_WINDOW_SCHEMA_V1: &str = "okx.research.reference-window/v1";
pub const CHECKPOINT_SCHEMA_V1: &str = "okx.research.checkpoint/v1";
pub const RESEARCH_CANDLE_SCHEMA_V1: &str = "okx.research.candle/v1";
pub const RESEARCH_FUNDING_SCHEMA_V1: &str = "okx.research.funding-event/v1";
pub const RESEARCH_TRADE_SCHEMA_V1: &str = "okx.research.trade-event/v1";
pub const RESEARCH_REFERENCE_SCHEMA_V1: &str = "okx.research.instrument-reference/v1";
pub const BUILD_SOURCE_TREE: &str = env!("OKX_SOURCE_TREE");
const EVIDENCE_DIR: &str = "evidence";
const SOURCE_CACHE_DIR: &str = "source-cache";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchTier {
    TierA,
    TierB,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchSourceKind {
    Candle,
    Funding,
    Reference,
    TierBProbe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReferenceCoverageStatus {
    Complete,
    InsufficientReferenceHistory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StorageClass {
    EvidenceStore,
    SourceCache,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchRange {
    pub begin_ms: String,
    pub end_ms: String,
}

impl ResearchRange {
    pub fn new(
        begin_ms: impl Into<String>,
        end_ms: impl Into<String>,
    ) -> Result<Self, ResearchError> {
        let range = Self {
            begin_ms: begin_ms.into(),
            end_ms: end_ms.into(),
        };
        let begin = parse_ms("begin_ms", &range.begin_ms)?;
        let end = parse_ms("end_ms", &range.end_ms)?;
        if begin >= end {
            return Err(ResearchError::InvalidRange {
                begin_ms: range.begin_ms,
                end_ms: range.end_ms,
            });
        }
        Ok(range)
    }

    pub fn begin(&self) -> Result<u64, ResearchError> {
        parse_ms("begin_ms", &self.begin_ms)
    }

    pub fn end(&self) -> Result<u64, ResearchError> {
        parse_ms("end_ms", &self.end_ms)
    }

    pub fn validate(&self) -> Result<(), ResearchError> {
        let begin = self.begin()?;
        let end = self.end()?;
        if begin >= end {
            return Err(ResearchError::InvalidRange {
                begin_ms: self.begin_ms.clone(),
                end_ms: self.end_ms.clone(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceRequest {
    pub provider: String,
    pub resource: String,
    pub instrument_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bar: Option<String>,
    pub range: ResearchRange,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
}

impl SourceRequest {
    pub fn validate(&self) -> Result<(), ResearchError> {
        required("provider", &self.provider)?;
        required("resource", &self.resource)?;
        required("instrument_id", &self.instrument_id)?;
        self.range.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkManifest {
    pub schema: String,
    pub chunk_id: String,
    pub capture_id: String,
    pub kind: ResearchSourceKind,
    pub source: SourceRequest,
    pub acquired_at_ms: String,
    pub raw_sha256: String,
    pub raw_size_bytes: u64,
    pub normalized_sha256: String,
    pub normalized_row_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_event_time_ms: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub newest_event_time_ms: Option<String>,
    pub parser_version: String,
    pub normalization_version: String,
    pub source_tree: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedChunk<T> {
    pub manifest: ChunkManifest,
    pub rows: Vec<T>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchCandle {
    pub schema: String,
    pub open_time_ms: String,
    pub available_time_ms: String,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub volume_currency: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_quote: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchFundingEvent {
    pub schema: String,
    pub funding_time_ms: String,
    pub available_time_ms: String,
    pub funding_rate: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub realized_rate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub formula_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchTradeEvent {
    pub schema: String,
    pub trade_id: String,
    pub event_time_ms: String,
    pub available_time_ms: String,
    pub price: String,
    pub size_contracts: String,
    pub side: MarketTradeSide,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchInstrumentReference {
    pub schema: String,
    pub instrument_id: String,
    pub instrument_type: String,
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settle_currency: Option<String>,
    pub tick_size: String,
    pub lot_size: String,
    pub min_size: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_limit_size: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_market_size: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract_value_currency: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_leverage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub list_time_ms: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiry_time_ms: Option<String>,
}

impl From<&InstrumentSpec> for ResearchInstrumentReference {
    fn from(value: &InstrumentSpec) -> Self {
        Self {
            schema: RESEARCH_REFERENCE_SCHEMA_V1.to_owned(),
            instrument_id: value.instrument_id.clone(),
            instrument_type: value.instrument_type.to_string(),
            state: value.state.clone(),
            settle_currency: value.settle_currency.clone(),
            tick_size: value.tick_size.clone(),
            lot_size: value.lot_size.clone(),
            min_size: value.min_size.clone(),
            max_limit_size: value.max_limit_size.clone(),
            max_market_size: value.max_market_size.clone(),
            contract_type: value.contract_type.clone(),
            contract_value: value.contract_value.clone(),
            contract_value_currency: value.contract_value_currency.clone(),
            max_leverage: value.max_leverage.clone(),
            list_time_ms: value.list_time_ms.clone(),
            expiry_time_ms: value.expiry_time_ms.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceCoverageWindow {
    pub schema: String,
    pub instrument_id: String,
    pub observed_from_ms: String,
    pub observed_through_ms: String,
    pub available_from_ms: String,
    pub reference_generation: String,
    pub reference_hash: String,
}

impl ReferenceCoverageWindow {
    pub fn covers(
        &self,
        instrument_id: &str,
        range: &ResearchRange,
    ) -> Result<bool, ResearchError> {
        if self.instrument_id != instrument_id {
            return Ok(false);
        }
        let observed_from = parse_ms("observed_from_ms", &self.observed_from_ms)?;
        let observed_through = parse_ms("observed_through_ms", &self.observed_through_ms)?;
        let available_from = parse_ms("available_from_ms", &self.available_from_ms)?;
        if observed_from > observed_through || available_from > observed_through {
            return Err(ResearchError::InvalidRange {
                begin_ms: self.observed_from_ms.clone(),
                end_ms: self.observed_through_ms.clone(),
            });
        }
        range.validate()?;
        let begin = range.begin()?;
        let end = range.end()?;
        Ok(observed_from <= begin && available_from <= begin && observed_through >= end)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataGap {
    pub begin_ms: String,
    pub end_ms: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetManifest {
    pub schema: String,
    pub dataset_id: String,
    pub tier: ResearchTier,
    pub instrument_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bar: Option<String>,
    pub range: ResearchRange,
    pub reference_coverage: ReferenceCoverageStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reference_window: Option<ReferenceCoverageWindow>,
    pub chunk_ids: Vec<String>,
    pub gaps: Vec<DataGap>,
    pub parser_version: String,
    pub normalization_version: String,
    pub source_tree: String,
    pub created_at_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct DatasetIdentity<'a> {
    schema: &'static str,
    tier: ResearchTier,
    instrument_id: &'a str,
    bar: &'a Option<String>,
    range: &'a ResearchRange,
    reference_coverage: ReferenceCoverageStatus,
    reference_window: &'a Option<ReferenceCoverageWindow>,
    chunk_ids: &'a [String],
    gaps: &'a [DataGap],
    parser_version: &'a str,
    normalization_version: &'a str,
    source_tree: &'a str,
}

impl DatasetManifest {
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        tier: ResearchTier,
        instrument_id: impl Into<String>,
        bar: Option<String>,
        range: ResearchRange,
        reference_window: Option<ReferenceCoverageWindow>,
        chunks: &[ChunkManifest],
        mut gaps: Vec<DataGap>,
        parser_version: impl Into<String>,
        normalization_version: impl Into<String>,
        source_tree: impl Into<String>,
        created_at_ms: impl Into<String>,
    ) -> Result<Self, ResearchError> {
        let instrument_id = instrument_id.into();
        required("instrument_id", &instrument_id)?;
        let parser_version = parser_version.into();
        let normalization_version = normalization_version.into();
        let source_tree = source_tree.into();
        let created_at_ms = created_at_ms.into();
        required("parser_version", &parser_version)?;
        required("normalization_version", &normalization_version)?;
        required("source_tree", &source_tree)?;
        parse_ms("created_at_ms", &created_at_ms)?;

        let mut chunk_ids = chunks
            .iter()
            .map(|chunk| {
                if chunk.source.instrument_id != instrument_id {
                    return Err(ResearchError::InstrumentMismatch {
                        expected: instrument_id.clone(),
                        actual: chunk.source.instrument_id.clone(),
                    });
                }
                Ok(chunk.chunk_id.clone())
            })
            .collect::<Result<Vec<_>, _>>()?;
        chunk_ids.sort();
        if chunk_ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ResearchError::DuplicateChunk);
        }

        gaps.sort_by(|left, right| {
            left.begin_ms
                .cmp(&right.begin_ms)
                .then_with(|| left.end_ms.cmp(&right.end_ms))
                .then_with(|| left.reason.cmp(&right.reason))
        });
        for gap in &gaps {
            validate_gap(gap, &range)?;
        }

        let reference_coverage = match &reference_window {
            Some(window) if window.covers(&instrument_id, &range)? => {
                ReferenceCoverageStatus::Complete
            }
            _ => ReferenceCoverageStatus::InsufficientReferenceHistory,
        };
        let dataset_id = canonical_sha256(&DatasetIdentity {
            schema: DATASET_MANIFEST_SCHEMA_V1,
            tier,
            instrument_id: &instrument_id,
            bar: &bar,
            range: &range,
            reference_coverage,
            reference_window: &reference_window,
            chunk_ids: &chunk_ids,
            gaps: &gaps,
            parser_version: &parser_version,
            normalization_version: &normalization_version,
            source_tree: &source_tree,
        })?;

        Ok(Self {
            schema: DATASET_MANIFEST_SCHEMA_V1.to_owned(),
            dataset_id,
            tier,
            instrument_id,
            bar,
            range,
            reference_coverage,
            reference_window,
            chunk_ids,
            gaps,
            parser_version,
            normalization_version,
            source_tree,
            created_at_ms,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchCheckpoint {
    pub schema: String,
    pub checkpoint_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_checkpoint_id: Option<String>,
    pub completed_chunk_ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remaining_cursor: Option<String>,
    pub source_tree: String,
    pub created_at_ms: String,
}

#[derive(Debug, Serialize)]
struct CheckpointIdentity<'a> {
    schema: &'static str,
    parent_checkpoint_id: &'a Option<String>,
    completed_chunk_ids: &'a [String],
    remaining_cursor: &'a Option<String>,
    source_tree: &'a str,
}

impl ResearchCheckpoint {
    pub fn build(
        parent_checkpoint_id: Option<String>,
        mut completed_chunk_ids: Vec<String>,
        remaining_cursor: Option<String>,
        source_tree: impl Into<String>,
        created_at_ms: impl Into<String>,
    ) -> Result<Self, ResearchError> {
        completed_chunk_ids.sort();
        completed_chunk_ids.dedup();
        let source_tree = source_tree.into();
        let created_at_ms = created_at_ms.into();
        required("source_tree", &source_tree)?;
        parse_ms("created_at_ms", &created_at_ms)?;
        let checkpoint_id = canonical_sha256(&CheckpointIdentity {
            schema: CHECKPOINT_SCHEMA_V1,
            parent_checkpoint_id: &parent_checkpoint_id,
            completed_chunk_ids: &completed_chunk_ids,
            remaining_cursor: &remaining_cursor,
            source_tree: &source_tree,
        })?;
        Ok(Self {
            schema: CHECKPOINT_SCHEMA_V1.to_owned(),
            checkpoint_id,
            parent_checkpoint_id,
            completed_chunk_ids,
            remaining_cursor,
            source_tree,
            created_at_ms,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageBudget {
    pub source_cache_max_bytes: u64,
    pub free_space_floor_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageUsage {
    pub source_cache_bytes: u64,
    pub observed_free_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageAdmission {
    Allowed,
    EvictUnpinnedSourceCache { bytes: u64 },
}

pub fn admit_storage(
    class: StorageClass,
    incoming_bytes: u64,
    budget: StorageBudget,
    usage: StorageUsage,
) -> Result<StorageAdmission, ResearchError> {
    let required_free = incoming_bytes
        .checked_add(budget.free_space_floor_bytes)
        .ok_or(ResearchError::StorageBudgetExceeded)?;
    if usage.observed_free_bytes < required_free {
        return Err(ResearchError::StorageBudgetExceeded);
    }
    if matches!(class, StorageClass::EvidenceStore) {
        return Ok(StorageAdmission::Allowed);
    }
    let projected = usage
        .source_cache_bytes
        .checked_add(incoming_bytes)
        .ok_or(ResearchError::StorageBudgetExceeded)?;
    if projected <= budget.source_cache_max_bytes {
        Ok(StorageAdmission::Allowed)
    } else {
        Ok(StorageAdmission::EvictUnpinnedSourceCache {
            bytes: projected - budget.source_cache_max_bytes,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveLimits {
    pub max_compressed_bytes: u64,
    pub max_decompressed_bytes: u64,
    pub max_rows: u64,
    pub max_decompression_ratio: u64,
}

pub fn validate_archive_budget(
    compressed_bytes: u64,
    decompressed_bytes: u64,
    rows: u64,
    limits: ArchiveLimits,
) -> Result<(), ResearchError> {
    if compressed_bytes == 0
        || compressed_bytes > limits.max_compressed_bytes
        || decompressed_bytes > limits.max_decompressed_bytes
        || rows > limits.max_rows
    {
        return Err(ResearchError::ArchiveBudgetExceeded);
    }
    let max_by_ratio = compressed_bytes
        .checked_mul(limits.max_decompression_ratio)
        .ok_or(ResearchError::ArchiveBudgetExceeded)?;
    if decompressed_bytes > max_by_ratio {
        return Err(ResearchError::ArchiveBudgetExceeded);
    }
    Ok(())
}

pub fn validate_archive_member_path(path: &str) -> Result<(), ResearchError> {
    let trimmed = path.trim();
    if trimmed.is_empty()
        || trimmed.starts_with('/')
        || trimmed.starts_with('\\')
        || trimmed.contains('\\')
        || trimmed.contains(':')
        || trimmed
            .split('/')
            .any(|part| matches!(part, "" | "." | ".."))
    {
        return Err(ResearchError::UnsafeArchivePath(path.to_owned()));
    }
    Ok(())
}

pub fn validate_source_host(
    host: &str,
    allowed_hosts: &BTreeSet<String>,
) -> Result<(), ResearchError> {
    if allowed_hosts.contains(host) {
        Ok(())
    } else {
        Err(ResearchError::SourceHostNotAllowed(host.to_owned()))
    }
}

#[derive(Debug, Error)]
pub enum ResearchError {
    #[error("missing required research field '{0}'")]
    MissingField(&'static str),

    #[error("invalid millisecond timestamp for '{field}': '{value}'")]
    InvalidTimestamp { field: &'static str, value: String },

    #[error("invalid research range: begin={begin_ms}, end={end_ms}")]
    InvalidRange { begin_ms: String, end_ms: String },

    #[error("instrument mismatch: expected '{expected}', got '{actual}'")]
    InstrumentMismatch { expected: String, actual: String },

    #[error("duplicate event timestamp '{0}'")]
    DuplicateTimestamp(String),

    #[error("dataset contains duplicate chunk identity")]
    DuplicateChunk,

    #[error("research trade history contains duplicate trade id '{0}'")]
    DuplicateTradeId(String),

    #[error("unconfirmed candle '{0}' is not admissible research history")]
    UnconfirmedCandle(String),

    #[error("gap lies outside the requested dataset range")]
    GapOutsideRange,

    #[error("storage budget exceeded")]
    StorageBudgetExceeded,

    #[error("archive budget exceeded")]
    ArchiveBudgetExceeded,

    #[error("unsafe archive member path '{0}'")]
    UnsafeArchivePath(String),

    #[error("historical source host is not allowlisted: '{0}'")]
    SourceHostNotAllowed(String),

    #[error("research artifact I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("research artifact id is invalid")]
    InvalidArtifactId,

    #[error("research artifact content does not match its content-addressed identity")]
    ArtifactIdentityMismatch,

    #[error("analysis error: {0}")]
    Analysis(#[from] okx_analysis::AnalysisError),

    #[error("replay dataset/spec identity mismatch")]
    ReplayDatasetMismatch,

    #[error("replay event ordering is not strictly chronological")]
    ReplayEventOrdering,

    #[error("replay causal ordering was violated")]
    ReplayCausalityViolation,

    #[error("replay bar '{0}' is not supported by the current deterministic kernel")]
    ReplayUnsupportedBar(String),

    #[error("invalid replay execution model field '{0}'")]
    ReplayInvalidExecutionModel(String),

    #[error("invalid replay decimal field '{field}': '{value}'")]
    ReplayInvalidDecimal { field: &'static str, value: String },

    #[error("missing required replay field '{0}'")]
    ReplayMissingField(&'static str),

    #[error("failed to serialize canonical research evidence: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
pub struct ResearchArtifactStore {
    root: PathBuf,
}

impl ResearchArtifactStore {
    pub fn at(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn evidence_dir(&self) -> PathBuf {
        self.root.join(EVIDENCE_DIR)
    }

    pub fn source_cache_dir(&self) -> PathBuf {
        self.root.join(SOURCE_CACHE_DIR)
    }

    pub fn publish_evidence<T: Serialize>(
        &self,
        value: &T,
    ) -> Result<(String, PathBuf), ResearchError> {
        let bytes = serde_json::to_vec(value)?;
        let artifact_id = sha256_bytes(&bytes);
        let path = self
            .evidence_dir()
            .join(format!("{}.json", sha256_hex(&artifact_id)?));
        publish_atomic_verified(&path, &bytes)?;
        Ok((artifact_id, path))
    }

    pub fn publish_source_bytes(
        &self,
        raw_sha256: &str,
        bytes: &[u8],
    ) -> Result<PathBuf, ResearchError> {
        validate_sha256_id(raw_sha256)?;
        if sha256_bytes(bytes) != raw_sha256 {
            return Err(ResearchError::ArtifactIdentityMismatch);
        }
        let path = self
            .source_cache_dir()
            .join(format!("{}.bin", sha256_hex(raw_sha256)?));
        publish_atomic_verified(&path, bytes)?;
        Ok(path)
    }

    pub fn read_evidence(&self, artifact_id: &str) -> Result<Vec<u8>, ResearchError> {
        validate_sha256_id(artifact_id)?;
        let path = self
            .evidence_dir()
            .join(format!("{}.json", sha256_hex(artifact_id)?));
        let bytes = fs::read(path)?;
        if sha256_bytes(&bytes) != artifact_id {
            return Err(ResearchError::ArtifactIdentityMismatch);
        }
        Ok(bytes)
    }

    pub fn read_source_bytes(&self, raw_sha256: &str) -> Result<Vec<u8>, ResearchError> {
        validate_sha256_id(raw_sha256)?;
        let path = self
            .source_cache_dir()
            .join(format!("{}.bin", sha256_hex(raw_sha256)?));
        let bytes = fs::read(path)?;
        if sha256_bytes(&bytes) != raw_sha256 {
            return Err(ResearchError::ArtifactIdentityMismatch);
        }
        Ok(bytes)
    }
}

pub fn detect_fixed_interval_gaps(
    event_times_ms: &[String],
    expected_interval_ms: u64,
) -> Result<Vec<DataGap>, ResearchError> {
    if expected_interval_ms == 0 {
        return Err(ResearchError::InvalidRange {
            begin_ms: "expected_interval_ms=0".to_owned(),
            end_ms: "expected_interval_ms=0".to_owned(),
        });
    }
    let mut times = event_times_ms
        .iter()
        .map(|value| parse_ms("event_time_ms", value))
        .collect::<Result<Vec<_>, _>>()?;
    times.sort_unstable();
    if times.windows(2).any(|pair| pair[0] == pair[1]) {
        let duplicate = times
            .windows(2)
            .find(|pair| pair[0] == pair[1])
            .expect("duplicate exists")[0];
        return Err(ResearchError::DuplicateTimestamp(duplicate.to_string()));
    }

    let mut gaps = Vec::new();
    for pair in times.windows(2) {
        let expected_next = pair[0].checked_add(expected_interval_ms).ok_or_else(|| {
            ResearchError::InvalidTimestamp {
                field: "expected_next_event_time_ms",
                value: pair[0].to_string(),
            }
        })?;
        if pair[1] > expected_next {
            gaps.push(DataGap {
                begin_ms: expected_next.to_string(),
                end_ms: pair[1].to_string(),
                reason: "missing_fixed_interval_events".to_owned(),
            });
        }
    }
    Ok(gaps)
}

fn validate_sha256_id(value: &str) -> Result<(), ResearchError> {
    sha256_hex(value).map(|_| ())
}

fn sha256_hex(value: &str) -> Result<&str, ResearchError> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(ResearchError::InvalidArtifactId);
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(ResearchError::InvalidArtifactId);
    }
    Ok(hex)
}

fn publish_atomic_verified(path: &Path, bytes: &[u8]) -> Result<(), ResearchError> {
    if path.exists() {
        let existing = fs::read(path)?;
        if existing == bytes {
            return Ok(());
        }
        return Err(ResearchError::ArtifactIdentityMismatch);
    }

    let parent = path.parent().ok_or(ResearchError::InvalidArtifactId)?;
    fs::create_dir_all(parent)?;
    let temp = path.with_extension(format!("tmp-{}", std::process::id()));
    if temp.exists() {
        fs::remove_file(&temp)?;
    }
    let mut file = File::create(&temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);

    match fs::rename(&temp, path) {
        Ok(()) => Ok(()),
        Err(_error) if path.exists() => {
            let _ = fs::remove_file(&temp);
            let existing = fs::read(path)?;
            if existing == bytes {
                Ok(())
            } else {
                Err(ResearchError::ArtifactIdentityMismatch)
            }
        }
        Err(error) => {
            let _ = fs::remove_file(&temp);
            Err(error.into())
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn build_candle_chunk(
    source: SourceRequest,
    acquired_at_ms: impl Into<String>,
    raw_body: &[u8],
    rows: &[HistoryCandle],
    bar_ms: u64,
    parser_version: impl Into<String>,
    normalization_version: impl Into<String>,
    source_tree: impl Into<String>,
) -> Result<NormalizedChunk<ResearchCandle>, ResearchError> {
    source.validate()?;
    let acquired_at_ms = acquired_at_ms.into();
    parse_ms("acquired_at_ms", &acquired_at_ms)?;
    if bar_ms == 0 {
        return Err(ResearchError::InvalidRange {
            begin_ms: "bar_ms=0".to_owned(),
            end_ms: "bar_ms=0".to_owned(),
        });
    }

    let mut normalized = Vec::with_capacity(rows.len());
    let mut timestamps = BTreeSet::new();
    for row in rows {
        let open = parse_ms("candle.open_time_ms", &row.open_time_ms)?;
        if !timestamps.insert(open) {
            return Err(ResearchError::DuplicateTimestamp(row.open_time_ms.clone()));
        }
        if !row.confirmed {
            return Err(ResearchError::UnconfirmedCandle(row.open_time_ms.clone()));
        }
        let available =
            open.checked_add(bar_ms)
                .ok_or_else(|| ResearchError::InvalidTimestamp {
                    field: "candle.available_time_ms",
                    value: row.open_time_ms.clone(),
                })?;
        normalized.push(ResearchCandle {
            schema: RESEARCH_CANDLE_SCHEMA_V1.to_owned(),
            open_time_ms: open.to_string(),
            available_time_ms: available.to_string(),
            open: required_owned("open", &row.open)?,
            high: required_owned("high", &row.high)?,
            low: required_owned("low", &row.low)?,
            close: required_owned("close", &row.close)?,
            volume: required_owned("volume", &row.volume)?,
            volume_currency: required_owned("volume_currency", &row.volume_currency)?,
            volume_quote: row.volume_quote.clone(),
        });
    }
    normalized.sort_by_key(|row| row.open_time_ms.parse::<u64>().unwrap_or_default());

    build_normalized_chunk(
        ChunkBuildInput {
            kind: ResearchSourceKind::Candle,
            source,
            acquired_at_ms,
            raw_body,
            parser_version: parser_version.into(),
            normalization_version: normalization_version.into(),
            source_tree: source_tree.into(),
        },
        normalized,
        |row: &ResearchCandle| &row.open_time_ms,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn build_funding_chunk(
    source: SourceRequest,
    acquired_at_ms: impl Into<String>,
    raw_body: &[u8],
    rows: &[FundingHistoryEvent],
    parser_version: impl Into<String>,
    normalization_version: impl Into<String>,
    source_tree: impl Into<String>,
) -> Result<NormalizedChunk<ResearchFundingEvent>, ResearchError> {
    source.validate()?;
    let acquired_at_ms = acquired_at_ms.into();
    parse_ms("acquired_at_ms", &acquired_at_ms)?;

    let mut normalized = Vec::with_capacity(rows.len());
    let mut timestamps = BTreeSet::new();
    for row in rows {
        let time = parse_ms("funding_time_ms", &row.funding_time_ms)?;
        if !timestamps.insert(time) {
            return Err(ResearchError::DuplicateTimestamp(
                row.funding_time_ms.clone(),
            ));
        }
        normalized.push(ResearchFundingEvent {
            schema: RESEARCH_FUNDING_SCHEMA_V1.to_owned(),
            funding_time_ms: time.to_string(),
            available_time_ms: time.to_string(),
            funding_rate: required_owned("funding_rate", &row.funding_rate)?,
            realized_rate: row.realized_rate.clone(),
            formula_type: row.formula_type.clone(),
            method: row.method.clone(),
        });
    }
    normalized.sort_by_key(|row| row.funding_time_ms.parse::<u64>().unwrap_or_default());

    build_normalized_chunk(
        ChunkBuildInput {
            kind: ResearchSourceKind::Funding,
            source,
            acquired_at_ms,
            raw_body,
            parser_version: parser_version.into(),
            normalization_version: normalization_version.into(),
            source_tree: source_tree.into(),
        },
        normalized,
        |row: &ResearchFundingEvent| &row.funding_time_ms,
    )
}

pub fn build_tier_b_trade_chunk(
    source: SourceRequest,
    acquired_at_ms: impl Into<String>,
    raw_body: &[u8],
    rows: &[MarketTrade],
    parser_version: impl Into<String>,
    normalization_version: impl Into<String>,
    source_tree: impl Into<String>,
) -> Result<NormalizedChunk<ResearchTradeEvent>, ResearchError> {
    source.validate()?;
    let acquired_at_ms = acquired_at_ms.into();
    parse_ms("acquired_at_ms", &acquired_at_ms)?;

    let mut trade_ids = BTreeSet::new();
    let mut normalized = Vec::with_capacity(rows.len());
    for row in rows {
        if !trade_ids.insert(row.trade_id.clone()) {
            return Err(ResearchError::DuplicateTradeId(row.trade_id.clone()));
        }
        let event_time = parse_ms("trade.exchange_timestamp_ms", &row.exchange_timestamp_ms)?;
        normalized.push(ResearchTradeEvent {
            schema: RESEARCH_TRADE_SCHEMA_V1.to_owned(),
            trade_id: required_owned("trade_id", &row.trade_id)?,
            event_time_ms: event_time.to_string(),
            available_time_ms: event_time.to_string(),
            price: required_owned("price", &row.price)?,
            size_contracts: required_owned("size_contracts", &row.size_contracts)?,
            side: row.side,
            source: row.source.clone(),
        });
    }
    normalized.sort_by(|left, right| {
        left.event_time_ms
            .parse::<u64>()
            .unwrap_or_default()
            .cmp(&right.event_time_ms.parse::<u64>().unwrap_or_default())
            .then_with(|| left.trade_id.cmp(&right.trade_id))
    });

    build_normalized_chunk(
        ChunkBuildInput {
            kind: ResearchSourceKind::TierBProbe,
            source,
            acquired_at_ms,
            raw_body,
            parser_version: parser_version.into(),
            normalization_version: normalization_version.into(),
            source_tree: source_tree.into(),
        },
        normalized,
        |row: &ResearchTradeEvent| &row.event_time_ms,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn build_reference_chunk(
    source: SourceRequest,
    acquired_at_ms: impl Into<String>,
    raw_body: &[u8],
    instrument: &InstrumentSpec,
    observed_from_ms: impl Into<String>,
    observed_through_ms: impl Into<String>,
    available_from_ms: impl Into<String>,
    reference_generation: impl Into<String>,
    parser_version: impl Into<String>,
    normalization_version: impl Into<String>,
    source_tree: impl Into<String>,
) -> Result<
    (
        NormalizedChunk<ResearchInstrumentReference>,
        ReferenceCoverageWindow,
    ),
    ResearchError,
> {
    source.validate()?;
    if source.instrument_id != instrument.instrument_id {
        return Err(ResearchError::InstrumentMismatch {
            expected: source.instrument_id,
            actual: instrument.instrument_id.clone(),
        });
    }
    let acquired_at_ms = acquired_at_ms.into();
    parse_ms("acquired_at_ms", &acquired_at_ms)?;
    let observed_from_ms = observed_from_ms.into();
    let observed_through_ms = observed_through_ms.into();
    let available_from_ms = available_from_ms.into();
    let observed_from = parse_ms("observed_from_ms", &observed_from_ms)?;
    let observed_through = parse_ms("observed_through_ms", &observed_through_ms)?;
    let available_from = parse_ms("available_from_ms", &available_from_ms)?;
    if observed_from > observed_through || available_from > observed_through {
        return Err(ResearchError::InvalidRange {
            begin_ms: observed_from_ms,
            end_ms: observed_through_ms,
        });
    }
    let reference_generation = reference_generation.into();
    required("reference_generation", &reference_generation)?;

    let row = ResearchInstrumentReference::from(instrument);
    let rows = vec![row];
    let chunk = build_normalized_chunk(
        ChunkBuildInput {
            kind: ResearchSourceKind::Reference,
            source,
            acquired_at_ms,
            raw_body,
            parser_version: parser_version.into(),
            normalization_version: normalization_version.into(),
            source_tree: source_tree.into(),
        },
        rows,
        |_row: &ResearchInstrumentReference| "",
    )?;
    let window = ReferenceCoverageWindow {
        schema: REFERENCE_WINDOW_SCHEMA_V1.to_owned(),
        instrument_id: instrument.instrument_id.clone(),
        observed_from_ms,
        observed_through_ms,
        available_from_ms,
        reference_generation,
        reference_hash: chunk.manifest.normalized_sha256.clone(),
    };
    Ok((chunk, window))
}

struct ChunkBuildInput<'a> {
    kind: ResearchSourceKind,
    source: SourceRequest,
    acquired_at_ms: String,
    raw_body: &'a [u8],
    parser_version: String,
    normalization_version: String,
    source_tree: String,
}

fn build_normalized_chunk<T, F>(
    input: ChunkBuildInput<'_>,
    rows: Vec<T>,
    event_time: F,
) -> Result<NormalizedChunk<T>, ResearchError>
where
    T: Serialize,
    F: Fn(&T) -> &str,
{
    let ChunkBuildInput {
        kind,
        source,
        acquired_at_ms,
        raw_body,
        parser_version,
        normalization_version,
        source_tree,
    } = input;
    required("parser_version", &parser_version)?;
    required("normalization_version", &normalization_version)?;
    required("source_tree", &source_tree)?;

    let raw_sha256 = sha256_bytes(raw_body);
    let normalized_sha256 = canonical_sha256(&rows)?;
    let raw_size_bytes =
        u64::try_from(raw_body.len()).map_err(|_| ResearchError::ArchiveBudgetExceeded)?;
    let normalized_row_count =
        u64::try_from(rows.len()).map_err(|_| ResearchError::ArchiveBudgetExceeded)?;

    let mut times = rows
        .iter()
        .map(event_time)
        .filter(|value| !value.is_empty())
        .map(|value| parse_ms("event_time_ms", value))
        .collect::<Result<Vec<_>, _>>()?;
    times.sort_unstable();

    #[derive(Serialize)]
    struct SourceCaptureIdentity<'a> {
        schema: &'static str,
        kind: ResearchSourceKind,
        source: &'a SourceRequest,
        raw_sha256: &'a str,
        raw_size_bytes: u64,
    }

    #[derive(Serialize)]
    struct ChunkIdentity<'a> {
        schema: &'static str,
        kind: ResearchSourceKind,
        source: &'a SourceRequest,
        normalized_sha256: &'a str,
        normalized_row_count: u64,
        parser_version: &'a str,
        normalization_version: &'a str,
        source_tree: &'a str,
    }

    let capture_id = canonical_sha256(&SourceCaptureIdentity {
        schema: SOURCE_CAPTURE_SCHEMA_V1,
        kind,
        source: &source,
        raw_sha256: &raw_sha256,
        raw_size_bytes,
    })?;
    let chunk_id = canonical_sha256(&ChunkIdentity {
        schema: CHUNK_MANIFEST_SCHEMA_V1,
        kind,
        source: &source,
        normalized_sha256: &normalized_sha256,
        normalized_row_count,
        parser_version: &parser_version,
        normalization_version: &normalization_version,
        source_tree: &source_tree,
    })?;

    Ok(NormalizedChunk {
        manifest: ChunkManifest {
            schema: CHUNK_MANIFEST_SCHEMA_V1.to_owned(),
            chunk_id,
            capture_id,
            kind,
            source,
            acquired_at_ms,
            raw_sha256,
            raw_size_bytes,
            normalized_sha256,
            normalized_row_count,
            oldest_event_time_ms: times.first().map(u64::to_string),
            newest_event_time_ms: times.last().map(u64::to_string),
            parser_version,
            normalization_version,
            source_tree,
        },
        rows,
    })
}

pub fn sha256_bytes(value: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(value))
}

pub fn canonical_sha256<T: Serialize>(value: &T) -> Result<String, ResearchError> {
    Ok(sha256_bytes(&serde_json::to_vec(value)?))
}

fn validate_gap(gap: &DataGap, range: &ResearchRange) -> Result<(), ResearchError> {
    let begin = parse_ms("gap.begin_ms", &gap.begin_ms)?;
    let end = parse_ms("gap.end_ms", &gap.end_ms)?;
    if begin >= end || begin < range.begin()? || end > range.end()? {
        return Err(ResearchError::GapOutsideRange);
    }
    required("gap.reason", &gap.reason)?;
    Ok(())
}

fn parse_ms(field: &'static str, value: &str) -> Result<u64, ResearchError> {
    value
        .parse::<u64>()
        .map_err(|_| ResearchError::InvalidTimestamp {
            field,
            value: value.to_owned(),
        })
}

fn required(field: &'static str, value: &str) -> Result<(), ResearchError> {
    if value.trim().is_empty() {
        Err(ResearchError::MissingField(field))
    } else {
        Ok(())
    }
}

fn required_owned(field: &'static str, value: &str) -> Result<String, ResearchError> {
    required(field, value)?;
    Ok(value.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_observation::{FundingRequirement, InstrumentType};

    const TREE: &str = "0123456789abcdef";
    const PARSER: &str = "okx-history-parser/v1";
    const NORMALIZER: &str = "okx-research-normalizer/v1";

    fn range() -> ResearchRange {
        ResearchRange::new("1700000000000", "1700007200000").expect("range")
    }

    fn source(kind: &str) -> SourceRequest {
        SourceRequest {
            provider: "okx".to_owned(),
            resource: kind.to_owned(),
            instrument_id: "BTC-USDT-SWAP".to_owned(),
            bar: Some("1H".to_owned()),
            range: range(),
            parameters: BTreeMap::new(),
        }
    }

    fn candles() -> Vec<HistoryCandle> {
        vec![
            HistoryCandle {
                open_time_ms: "1700000000000".to_owned(),
                open: "100".to_owned(),
                high: "110".to_owned(),
                low: "90".to_owned(),
                close: "105".to_owned(),
                volume: "10".to_owned(),
                volume_currency: "1".to_owned(),
                volume_quote: Some("1000".to_owned()),
                confirmed: true,
            },
            HistoryCandle {
                open_time_ms: "1700003600000".to_owned(),
                open: "105".to_owned(),
                high: "111".to_owned(),
                low: "101".to_owned(),
                close: "108".to_owned(),
                volume: "12".to_owned(),
                volume_currency: "1.2".to_owned(),
                volume_quote: Some("1260".to_owned()),
                confirmed: true,
            },
        ]
    }

    fn trades() -> Vec<MarketTrade> {
        vec![
            MarketTrade {
                trade_id: "1001".to_owned(),
                price: "100.1".to_owned(),
                size_contracts: "2".to_owned(),
                side: MarketTradeSide::Buy,
                source: Some("0".to_owned()),
                exchange_timestamp_ms: "1700000000100".to_owned(),
            },
            MarketTrade {
                trade_id: "1002".to_owned(),
                price: "100.2".to_owned(),
                size_contracts: "1".to_owned(),
                side: MarketTradeSide::Sell,
                source: Some("0".to_owned()),
                exchange_timestamp_ms: "1700000000200".to_owned(),
            },
        ]
    }

    fn funding() -> Vec<FundingHistoryEvent> {
        vec![
            FundingHistoryEvent {
                funding_time_ms: "1700000000000".to_owned(),
                funding_rate: "0.0001".to_owned(),
                realized_rate: Some("0.00009".to_owned()),
                formula_type: Some("withRate".to_owned()),
                method: Some("current_period".to_owned()),
            },
            FundingHistoryEvent {
                funding_time_ms: "1700003600000".to_owned(),
                funding_rate: "-0.0002".to_owned(),
                realized_rate: Some("-0.00019".to_owned()),
                formula_type: Some("withRate".to_owned()),
                method: Some("current_period".to_owned()),
            },
        ]
    }

    fn instrument() -> InstrumentSpec {
        InstrumentSpec {
            instrument_id: "BTC-USDT-SWAP".to_owned(),
            instrument_type: InstrumentType::Swap,
            instrument_family: Some("BTC-USDT".to_owned()),
            underlying: Some("BTC-USDT".to_owned()),
            state: "live".to_owned(),
            rule_type: Some("normal".to_owned()),
            funding_requirement: FundingRequirement::Required,
            base_currency: None,
            quote_currency: None,
            settle_currency: Some("USDT".to_owned()),
            tick_size: "0.1".to_owned(),
            lot_size: "0.01".to_owned(),
            min_size: "0.01".to_owned(),
            max_limit_size: Some("10000".to_owned()),
            max_market_size: Some("1000".to_owned()),
            max_limit_amount: None,
            max_market_amount: None,
            contract_type: Some("linear".to_owned()),
            contract_value: Some("0.01".to_owned()),
            contract_value_currency: Some("BTC".to_owned()),
            fee_group_id: Some("4".to_owned()),
            max_leverage: Some("100".to_owned()),
            list_time_ms: Some("1600000000000".to_owned()),
            expiry_time_ms: None,
            initial_price_limit_pct: None,
            floating_price_limit_pct: None,
            maximum_price_limit_pct: None,
            upcoming_rule_changes: Vec::new(),
        }
    }

    #[test]
    fn candle_chunk_identity_ignores_acquisition_time_but_binds_raw_and_normalized_bytes() {
        let raw = br#"{"code":"0","data":[["1700003600000","105","111","101","108","12","1.2","1260","1"],["1700000000000","100","110","90","105","10","1","1000","1"]]}"#;
        let a = build_candle_chunk(
            source("history-candles"),
            "1700010000000",
            raw,
            &candles(),
            3_600_000,
            PARSER,
            NORMALIZER,
            TREE,
        )
        .expect("chunk");
        let b = build_candle_chunk(
            source("history-candles"),
            "1700020000000",
            raw,
            &candles(),
            3_600_000,
            PARSER,
            NORMALIZER,
            TREE,
        )
        .expect("chunk");

        assert_eq!(a.manifest.chunk_id, b.manifest.chunk_id);
        assert_eq!(a.manifest.raw_sha256, b.manifest.raw_sha256);
        assert_eq!(a.manifest.normalized_sha256, b.manifest.normalized_sha256);
        assert_ne!(a.manifest.acquired_at_ms, b.manifest.acquired_at_ms);
        assert_eq!(a.rows[0].available_time_ms, "1700003600000");
        assert_eq!(a.rows[1].available_time_ms, "1700007200000");
    }

    #[test]
    fn changed_raw_bytes_change_chunk_identity_even_when_normalized_rows_match() {
        let a = build_candle_chunk(
            source("history-candles"),
            "1700010000000",
            b"raw-a",
            &candles(),
            3_600_000,
            PARSER,
            NORMALIZER,
            TREE,
        )
        .expect("a");
        let b = build_candle_chunk(
            source("history-candles"),
            "1700010000000",
            b"raw-b",
            &candles(),
            3_600_000,
            PARSER,
            NORMALIZER,
            TREE,
        )
        .expect("b");

        assert_ne!(a.manifest.raw_sha256, b.manifest.raw_sha256);
        assert_ne!(a.manifest.capture_id, b.manifest.capture_id);
        assert_eq!(a.manifest.chunk_id, b.manifest.chunk_id);
        assert_eq!(a.manifest.normalized_sha256, b.manifest.normalized_sha256);
    }

    #[test]
    fn tier_b_trade_chunk_is_reference_free_and_causal() {
        let mut trade_source = source("/api/v5/market/history-trades");
        trade_source.bar = None;
        let chunk = build_tier_b_trade_chunk(
            trade_source,
            "1700010000000",
            br#"{"code":"0","data":[{"tradeId":"1002"},{"tradeId":"1001"}]}"#,
            &trades(),
            PARSER,
            NORMALIZER,
            TREE,
        )
        .expect("tier b trade chunk");

        assert_eq!(chunk.manifest.kind, ResearchSourceKind::TierBProbe);
        assert_eq!(chunk.rows.len(), 2);
        assert_eq!(chunk.rows[0].trade_id, "1001");
        assert_eq!(chunk.rows[0].available_time_ms, chunk.rows[0].event_time_ms);
        assert_eq!(
            chunk.manifest.oldest_event_time_ms.as_deref(),
            Some("1700000000100")
        );
        assert_eq!(
            chunk.manifest.newest_event_time_ms.as_deref(),
            Some("1700000000200")
        );

        let mut duplicate = trades();
        duplicate[1].trade_id = duplicate[0].trade_id.clone();
        assert!(matches!(
            build_tier_b_trade_chunk(
                source("/api/v5/market/history-trades"),
                "1700010000000",
                b"x",
                &duplicate,
                PARSER,
                NORMALIZER,
                TREE,
            ),
            Err(ResearchError::DuplicateTradeId(_))
        ));
    }

    #[test]
    fn candle_builder_fails_closed_on_unconfirmed_or_duplicate_rows() {
        let mut unconfirmed = candles();
        unconfirmed[0].confirmed = false;
        assert!(matches!(
            build_candle_chunk(
                source("history-candles"),
                "1700010000000",
                b"x",
                &unconfirmed,
                3_600_000,
                PARSER,
                NORMALIZER,
                TREE,
            ),
            Err(ResearchError::UnconfirmedCandle(_))
        ));

        let mut duplicate = candles();
        duplicate[1].open_time_ms = duplicate[0].open_time_ms.clone();
        assert!(matches!(
            build_candle_chunk(
                source("history-candles"),
                "1700010000000",
                b"x",
                &duplicate,
                3_600_000,
                PARSER,
                NORMALIZER,
                TREE,
            ),
            Err(ResearchError::DuplicateTimestamp(_))
        ));
    }

    #[test]
    fn funding_events_are_sorted_and_preserve_realized_mechanism() {
        let mut rows = funding();
        rows.reverse();
        let chunk = build_funding_chunk(
            source("funding-rate-history"),
            "1700010000000",
            b"funding-raw",
            &rows,
            PARSER,
            NORMALIZER,
            TREE,
        )
        .expect("funding");

        assert_eq!(chunk.rows[0].funding_time_ms, "1700000000000");
        assert_eq!(chunk.rows[0].available_time_ms, "1700000000000");
        assert_eq!(chunk.rows[0].realized_rate.as_deref(), Some("0.00009"));
        assert_eq!(chunk.rows[0].method.as_deref(), Some("current_period"));
    }

    #[test]
    fn current_reference_snapshot_is_not_retroactively_valid_historical_coverage() {
        let (chunk, window) = build_reference_chunk(
            source("public-instruments"),
            "1700010000000",
            b"reference-raw",
            &instrument(),
            "1700010000000",
            "1700010000000",
            "1700010000000",
            "sha256:reference",
            PARSER,
            NORMALIZER,
            TREE,
        )
        .expect("reference");

        let candles = build_candle_chunk(
            source("history-candles"),
            "1700010000000",
            b"candles",
            &candles(),
            3_600_000,
            PARSER,
            NORMALIZER,
            TREE,
        )
        .expect("candles");
        let manifest = DatasetManifest::build(
            ResearchTier::TierA,
            "BTC-USDT-SWAP",
            Some("1H".to_owned()),
            range(),
            Some(window),
            &[chunk.manifest, candles.manifest],
            Vec::new(),
            PARSER,
            NORMALIZER,
            TREE,
            "1700011000000",
        )
        .expect("manifest");

        assert_eq!(
            manifest.reference_coverage,
            ReferenceCoverageStatus::InsufficientReferenceHistory
        );
    }

    #[test]
    fn observed_reference_window_must_cover_full_requested_range() {
        let (reference, window) = build_reference_chunk(
            source("public-instruments"),
            "1700000000000",
            b"reference-raw",
            &instrument(),
            "1699990000000",
            "1700007200000",
            "1699990000000",
            "sha256:reference",
            PARSER,
            NORMALIZER,
            TREE,
        )
        .expect("reference");
        let candles = build_candle_chunk(
            source("history-candles"),
            "1700010000000",
            b"candles",
            &candles(),
            3_600_000,
            PARSER,
            NORMALIZER,
            TREE,
        )
        .expect("candles");
        let manifest = DatasetManifest::build(
            ResearchTier::TierA,
            "BTC-USDT-SWAP",
            Some("1H".to_owned()),
            range(),
            Some(window),
            &[reference.manifest, candles.manifest],
            Vec::new(),
            PARSER,
            NORMALIZER,
            TREE,
            "1700011000000",
        )
        .expect("manifest");

        assert_eq!(
            manifest.reference_coverage,
            ReferenceCoverageStatus::Complete
        );
    }

    #[test]
    fn dataset_identity_is_independent_of_manifest_creation_time_and_input_chunk_order() {
        let candle = build_candle_chunk(
            source("history-candles"),
            "1700010000000",
            b"candles",
            &candles(),
            3_600_000,
            PARSER,
            NORMALIZER,
            TREE,
        )
        .expect("candles");
        let funding = build_funding_chunk(
            source("funding-rate-history"),
            "1700010000000",
            b"funding",
            &funding(),
            PARSER,
            NORMALIZER,
            TREE,
        )
        .expect("funding");

        let first = DatasetManifest::build(
            ResearchTier::TierA,
            "BTC-USDT-SWAP",
            Some("1H".to_owned()),
            range(),
            None,
            &[candle.manifest.clone(), funding.manifest.clone()],
            Vec::new(),
            PARSER,
            NORMALIZER,
            TREE,
            "1700011000000",
        )
        .expect("first");
        let second = DatasetManifest::build(
            ResearchTier::TierA,
            "BTC-USDT-SWAP",
            Some("1H".to_owned()),
            range(),
            None,
            &[funding.manifest, candle.manifest],
            Vec::new(),
            PARSER,
            NORMALIZER,
            TREE,
            "1700012000000",
        )
        .expect("second");

        assert_eq!(first.dataset_id, second.dataset_id);
    }

    #[test]
    fn checkpoint_identity_is_idempotent_and_parent_linked() {
        let first = ResearchCheckpoint::build(
            None,
            vec!["chunk-b".to_owned(), "chunk-a".to_owned()],
            Some("cursor-2".to_owned()),
            TREE,
            "1700011000000",
        )
        .expect("first");
        let retry = ResearchCheckpoint::build(
            None,
            vec![
                "chunk-a".to_owned(),
                "chunk-b".to_owned(),
                "chunk-a".to_owned(),
            ],
            Some("cursor-2".to_owned()),
            TREE,
            "1700012000000",
        )
        .expect("retry");
        assert_eq!(first.checkpoint_id, retry.checkpoint_id);

        let next = ResearchCheckpoint::build(
            Some(first.checkpoint_id.clone()),
            vec![
                "chunk-a".to_owned(),
                "chunk-b".to_owned(),
                "chunk-c".to_owned(),
            ],
            None,
            TREE,
            "1700013000000",
        )
        .expect("next");
        assert_eq!(
            next.parent_checkpoint_id.as_deref(),
            Some(first.checkpoint_id.as_str())
        );
        assert_ne!(next.checkpoint_id, first.checkpoint_id);
    }

    #[test]
    fn storage_budget_preserves_evidence_and_bounds_source_cache() {
        let budget = StorageBudget {
            source_cache_max_bytes: 100,
            free_space_floor_bytes: 50,
        };
        let usage = StorageUsage {
            source_cache_bytes: 90,
            observed_free_bytes: 1_000,
        };
        assert_eq!(
            admit_storage(StorageClass::EvidenceStore, 20, budget, usage).expect("evidence"),
            StorageAdmission::Allowed
        );
        assert_eq!(
            admit_storage(StorageClass::SourceCache, 20, budget, usage).expect("cache"),
            StorageAdmission::EvictUnpinnedSourceCache { bytes: 10 }
        );

        let low_disk = StorageUsage {
            source_cache_bytes: 0,
            observed_free_bytes: 60,
        };
        assert!(matches!(
            admit_storage(StorageClass::EvidenceStore, 20, budget, low_disk),
            Err(ResearchError::StorageBudgetExceeded)
        ));
    }

    #[test]
    fn archive_budget_and_member_path_fail_closed() {
        let limits = ArchiveLimits {
            max_compressed_bytes: 100,
            max_decompressed_bytes: 1_000,
            max_rows: 100,
            max_decompression_ratio: 10,
        };
        validate_archive_budget(50, 400, 80, limits).expect("valid archive");
        assert!(matches!(
            validate_archive_budget(
                50,
                600,
                80,
                ArchiveLimits {
                    max_decompression_ratio: 10,
                    ..limits
                }
            ),
            Err(ResearchError::ArchiveBudgetExceeded)
        ));
        assert!(validate_archive_member_path("BTC-USDT-SWAP/2026-10.csv").is_ok());
        for path in [
            "../secret",
            "/absolute/file",
            r"C:\\windows\\file",
            "a/../b",
        ] {
            assert!(matches!(
                validate_archive_member_path(path),
                Err(ResearchError::UnsafeArchivePath(_))
            ));
        }
    }

    #[test]
    fn historical_source_host_is_explicitly_allowlisted() {
        let allowed = BTreeSet::from(["www.okx.com".to_owned(), "openapi.okx.com".to_owned()]);
        validate_source_host("www.okx.com", &allowed).expect("allowlisted");
        assert!(matches!(
            validate_source_host("example.com", &allowed),
            Err(ResearchError::SourceHostNotAllowed(host)) if host == "example.com"
        ));
    }

    #[test]
    fn gaps_are_bound_to_requested_range_and_affect_dataset_identity() {
        let candle = build_candle_chunk(
            source("history-candles"),
            "1700010000000",
            b"candles",
            &candles(),
            3_600_000,
            PARSER,
            NORMALIZER,
            TREE,
        )
        .expect("candles");
        let clean = DatasetManifest::build(
            ResearchTier::TierA,
            "BTC-USDT-SWAP",
            Some("1H".to_owned()),
            range(),
            None,
            std::slice::from_ref(&candle.manifest),
            Vec::new(),
            PARSER,
            NORMALIZER,
            TREE,
            "1700011000000",
        )
        .expect("clean");
        let gapped = DatasetManifest::build(
            ResearchTier::TierA,
            "BTC-USDT-SWAP",
            Some("1H".to_owned()),
            range(),
            None,
            &[candle.manifest],
            vec![DataGap {
                begin_ms: "1700003600000".to_owned(),
                end_ms: "1700007200000".to_owned(),
                reason: "source_missing".to_owned(),
            }],
            PARSER,
            NORMALIZER,
            TREE,
            "1700011000000",
        )
        .expect("gapped");

        assert_ne!(clean.dataset_id, gapped.dataset_id);
    }

    #[test]
    fn fixed_interval_gap_detection_is_deterministic() {
        let gaps = detect_fixed_interval_gaps(
            &[
                "1700000000000".to_owned(),
                "1700003600000".to_owned(),
                "1700010800000".to_owned(),
            ],
            3_600_000,
        )
        .expect("gaps");
        assert_eq!(
            gaps,
            vec![DataGap {
                begin_ms: "1700007200000".to_owned(),
                end_ms: "1700010800000".to_owned(),
                reason: "missing_fixed_interval_events".to_owned(),
            }]
        );
    }

    #[test]
    fn content_addressed_store_is_idempotent_and_detects_corruption() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(1);

        let suffix = NEXT.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("okx-research-test-{}-{suffix}", std::process::id()));
        let store = ResearchArtifactStore::at(&root);

        let payload = serde_json::json!({"schema":"test/v1","value":"stable"});
        let bytes = serde_json::to_vec(&payload).expect("serialize");
        let id = sha256_bytes(&bytes);

        let (first_id, first) = store.publish_evidence(&payload).expect("publish");
        let (second_id, second) = store.publish_evidence(&payload).expect("retry");
        assert_eq!(first_id, id);
        assert_eq!(second_id, id);
        assert_eq!(first, second);
        assert_eq!(store.read_evidence(&id).expect("read"), bytes);

        fs::write(&first, b"corrupt").expect("corrupt");
        assert!(matches!(
            store.read_evidence(&id),
            Err(ResearchError::ArtifactIdentityMismatch)
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn source_cache_rejects_identity_mismatch() {
        let root =
            std::env::temp_dir().join(format!("okx-research-source-test-{}", std::process::id()));
        let store = ResearchArtifactStore::at(&root);
        let bytes = b"raw-okx-response";
        let id = sha256_bytes(bytes);
        store
            .publish_source_bytes(&id, bytes)
            .expect("publish source");
        assert_eq!(store.read_source_bytes(&id).expect("read"), bytes);
        assert!(matches!(
            store.publish_source_bytes(&id, b"different"),
            Err(ResearchError::ArtifactIdentityMismatch)
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn deserialized_invalid_ranges_fail_validation() {
        let invalid: ResearchRange = serde_json::from_value(serde_json::json!({
            "begin_ms": "200",
            "end_ms": "100"
        }))
        .expect("deserialize");
        assert!(matches!(
            invalid.validate(),
            Err(ResearchError::InvalidRange { .. })
        ));

        let source = SourceRequest {
            provider: "okx".to_owned(),
            resource: "history".to_owned(),
            instrument_id: "BTC-USDT-SWAP".to_owned(),
            bar: Some("1H".to_owned()),
            range: invalid,
            parameters: BTreeMap::new(),
        };
        assert!(matches!(
            source.validate(),
            Err(ResearchError::InvalidRange { .. })
        ));
    }
}

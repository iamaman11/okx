//! Strict Control protocol and lifecycle states, free of runtime side effects.

use std::{
    fmt,
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

/// A type-level owner; four owners do not imply four operating-system processes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComponentId {
    Engine,
    Research,
    Control,
    Edge,
}

/// The only locally managed deployable owners. Cloudflare Edge is released remotely.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalComponent {
    Engine,
    Research,
    Control,
}

impl From<LocalComponent> for ComponentId {
    fn from(component: LocalComponent) -> Self {
        match component {
            LocalComponent::Engine => Self::Engine,
            LocalComponent::Research => Self::Research,
            LocalComponent::Control => Self::Control,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OperationId(NonZeroU64);

impl OperationId {
    pub fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WriterEpoch(NonZeroU64);

impl WriterEpoch {
    pub fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }
}

// JSON numbers above 2^53 lose precision in JavaScript. IDs/epochs use a
// canonical decimal string over the Control <-> Edge transport boundary.
fn parse_nonzero_decimal<E: serde::de::Error>(value: &str) -> Result<NonZeroU64, E> {
    if value.starts_with('0') || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(E::custom(
            "identifier must be a canonical unsigned decimal string",
        ));
    }
    let parsed = value
        .parse::<u64>()
        .map_err(|_| E::custom("identifier exceeds the u64 range"))?;
    NonZeroU64::new(parsed).ok_or_else(|| E::custom("identifier must be nonzero"))
}

impl Serialize for OperationId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.get().to_string())
    }
}

impl<'de> Deserialize<'de> for OperationId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        parse_nonzero_decimal::<D::Error>(&value).map(Self)
    }
}

impl Serialize for WriterEpoch {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.get().to_string())
    }
}

impl<'de> Deserialize<'de> for WriterEpoch {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        parse_nonzero_decimal::<D::Error>(&value).map(Self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProtocolVersion(NonZeroU16);

impl ProtocolVersion {
    pub fn new(value: u16) -> Option<Self> {
        NonZeroU16::new(value).map(Self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
pub struct ComponentVersion {
    major: u16,
    minor: u16,
    patch: u16,
}

impl ComponentVersion {
    pub fn new(major: u16, minor: u16, patch: u16) -> Option<Self> {
        (major != 0 || minor != 0 || patch != 0).then_some(Self {
            major,
            minor,
            patch,
        })
    }

    pub const fn tuple(self) -> (u16, u16, u16) {
        (self.major, self.minor, self.patch)
    }
}

impl<'de> Deserialize<'de> for ComponentVersion {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireVersion {
            major: u16,
            minor: u16,
            patch: u16,
        }

        let wire = WireVersion::deserialize(deserializer)?;
        Self::new(wire.major, wire.minor, wire.patch)
            .ok_or_else(|| D::Error::custom("component version must not be 0.0.0"))
    }
}

/// A claimed SHA-256; digest equality is not a proof of source provenance.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Sha256Digest([u8; 32]);

impl TryFrom<&str> for Sha256Digest {
    type Error = DigestError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(DigestError);
        }
        let mut bytes = [0_u8; 32];
        for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
            bytes[index] = (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]);
        }
        Ok(Self(bytes))
    }
}

const fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => unreachable!(),
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl Serialize for Sha256Digest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::try_from(value.as_str()).map_err(D::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DigestError;

impl fmt::Display for DigestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("digest must contain exactly 64 lowercase hexadecimal characters")
    }
}

impl std::error::Error for DigestError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalPlatform {
    WindowsX64Msvc,
    LinuxX64Gnu,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactClaim {
    component: LocalComponent,
    version: ComponentVersion,
    protocol: ProtocolVersion,
    target: LocalPlatform,
    sha256: Sha256Digest,
}

impl ArtifactClaim {
    pub fn new(
        component: LocalComponent,
        version: ComponentVersion,
        protocol: ProtocolVersion,
        target: LocalPlatform,
        sha256: Sha256Digest,
    ) -> Self {
        Self {
            component,
            version,
            protocol,
            target,
            sha256,
        }
    }

    pub const fn component(&self) -> LocalComponent {
        self.component
    }

    pub const fn digest(&self) -> Sha256Digest {
        self.sha256
    }
}

/// Commands are intents, not proof of authorization or a license to stop Engine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlCommand {
    Status {
        component: ComponentId,
    },
    Doctor {},
    Start {
        component: LocalComponent,
        expected_version: ComponentVersion,
    },
    StopResearch {},
    Stage {
        candidate: ArtifactClaim,
    },
    Activate {
        component: LocalComponent,
        expected_current: Sha256Digest,
        candidate: Sha256Digest,
    },
    Rollback {
        component: LocalComponent,
        expected_current: Sha256Digest,
    },
    /// Explicit request for externally authorized maintenance and writer fencing.
    RequestEngineDrain {
        expected_writer: WriterEpoch,
    },
}

/// Every command carries a required, nonzero operation id for correlation and
/// idempotency. Authorization is checked by the future controller boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlRequest {
    pub operation_id: OperationId,
    pub command: ControlCommand,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticCode {
    InvalidRequest,
    Unauthorized,
    IncompatibleVersion,
    ArtifactHashMismatch,
    ArtifactProvenanceUnproven,
    ComponentUnavailable,
    WriterFencingUnproven,
    LifecycleConflict,
    ReadinessDeadline,
    RollbackUnavailable,
    OutcomeUnknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureStage {
    Validate,
    Authorize,
    VerifyArtifact,
    FenceWriter,
    Stage,
    Switch,
    Readiness,
    Rollback,
    Inspect,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SafeAction {
    None,
    RecheckStatus,
    InspectLocalEvidence,
    RequestAuthorization,
    ReconcileOwner,
    RestorePreviousArtifact,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceCompleteness {
    Complete,
    Partial,
    Unavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Health {
    Ready,
    Degraded,
    Stopped,
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ComponentStatus {
    component: ComponentId,
    health: Health,
    evidence_age_ms: Option<u64>,
    evidence: EvidenceCompleteness,
}

impl ComponentStatus {
    pub fn new(
        component: ComponentId,
        health: Health,
        evidence_age_ms: Option<u64>,
        evidence: EvidenceCompleteness,
    ) -> Option<Self> {
        if health == Health::Ready
            && (evidence != EvidenceCompleteness::Complete || evidence_age_ms.is_none())
        {
            return None;
        }
        Some(Self {
            component,
            health,
            evidence_age_ms,
            evidence,
        })
    }

    pub const fn component(&self) -> ComponentId {
        self.component
    }

    pub const fn health(&self) -> Health {
        self.health
    }
}

impl<'de> Deserialize<'de> for ComponentStatus {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireStatus {
            component: ComponentId,
            health: Health,
            evidence_age_ms: Option<u64>,
            evidence: EvidenceCompleteness,
        }

        let status = WireStatus::deserialize(deserializer)?;
        Self::new(
            status.component,
            status.health,
            status.evidence_age_ms,
            status.evidence,
        )
        .ok_or_else(|| D::Error::custom("ready requires complete and dated evidence"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "receipt", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlReceipt {
    Status {
        status: ComponentStatus,
    },
    Staged {
        component: ComponentId,
        digest: Sha256Digest,
    },
    Activated {
        component: ComponentId,
        digest: Sha256Digest,
    },
    RolledBack {
        component: ComponentId,
        digest: Sha256Digest,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlFailure {
    pub owner: ComponentId,
    pub code: DiagnosticCode,
    pub stage: FailureStage,
    pub evidence_age_ms: Option<u64>,
    pub next_action: SafeAction,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlOutcome {
    Completed {
        operation_id: OperationId,
        receipt: ControlReceipt,
    },
    Rejected {
        operation_id: OperationId,
        failure: ControlFailure,
    },
    Unknown {
        operation_id: OperationId,
        component: ComponentId,
        stage: FailureStage,
        next_action: SafeAction,
    },
}

/// Only these transitions may be recorded as proven install progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstallPhase {
    Staged,
    PreflightPassed,
    Activating,
    AwaitingReadiness,
    Active,
    RollingBack,
    Restored,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InstallEvent {
    PreflightVerified,
    BeginActivation,
    Launched,
    Ready,
    BeginRollback,
    Restored,
    LostOutcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidTransition {
    pub from: InstallPhase,
    pub event: InstallEvent,
}

impl InstallPhase {
    pub const fn transition(self, event: InstallEvent) -> Result<Self, InvalidTransition> {
        use InstallEvent as E;
        use InstallPhase as S;

        match (self, event) {
            (S::Staged, E::PreflightVerified) => Ok(S::PreflightPassed),
            (S::PreflightPassed, E::BeginActivation) => Ok(S::Activating),
            (S::Activating, E::Launched) => Ok(S::AwaitingReadiness),
            (S::AwaitingReadiness, E::Ready) => Ok(S::Active),
            (S::Activating | S::AwaitingReadiness | S::Active, E::BeginRollback) => {
                Ok(S::RollingBack)
            }
            (S::RollingBack, E::Restored) => Ok(S::Restored),
            (_, E::LostOutcome) => Ok(S::Unknown),
            _ => Err(InvalidTransition { from: self, event }),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessIdentity {
    pub pid: NonZeroU32,
    pub started_at_ms: u64,
    pub artifact: Sha256Digest,
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEX: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn digest_is_lowercase_exact_and_roundtrips_as_string() {
        let digest = Sha256Digest::try_from(HEX).unwrap();
        assert_eq!(digest.to_string(), HEX);
        assert_eq!(
            serde_json::to_string(&digest).unwrap(),
            format!("\"{HEX}\"")
        );
        assert_eq!(
            serde_json::from_str::<Sha256Digest>(&format!("\"{HEX}\"")).unwrap(),
            digest
        );
        assert!(Sha256Digest::try_from(&HEX.to_uppercase()[..]).is_err());
        assert!(Sha256Digest::try_from("abcd").is_err());
        assert!(serde_json::from_str::<Sha256Digest>("\"abcd\"").is_err());
    }

    #[test]
    fn version_is_validated_even_on_deserialization() {
        // Rust and Edge components legitimately use 0.x versions before 1.0.
        assert!(ComponentVersion::new(0, 1, 2).is_some());
        assert!(ComponentVersion::new(0, 0, 0).is_none());
        assert!(
            serde_json::from_str::<ComponentVersion>(r#"{"major":0,"minor":0,"patch":0}"#).is_err()
        );
        let version = ComponentVersion::new(1, 2, 3).unwrap();
        assert_eq!(
            serde_json::from_str::<ComponentVersion>(&serde_json::to_string(&version).unwrap())
                .unwrap(),
            version
        );
        assert!(
            serde_json::from_str::<ComponentVersion>(
                r#"{"major":1,"minor":0,"patch":0,"unexpected":1}"#
            )
            .is_err()
        );
    }

    #[test]
    fn commands_cannot_target_cloud_edge_as_local_process() {
        assert!(
            serde_json::from_str::<ControlCommand>(
                r#"{"command":"start","component":"edge","expected_version":{"major":1,"minor":0,"patch":0}}"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<ControlCommand>(r#"{"command":"doctor","unexpected":true}"#)
                .is_err()
        );
        assert!(
            serde_json::from_str::<ControlCommand>(
                r#"{"command":"stop_research","engine_drain":true}"#
            )
            .is_err()
        );
        let drain = ControlCommand::RequestEngineDrain {
            expected_writer: WriterEpoch::new(9).unwrap(),
        };
        assert_eq!(
            serde_json::from_str::<ControlCommand>(&serde_json::to_string(&drain).unwrap())
                .unwrap(),
            drain
        );
        assert!(OperationId::new(0).is_none());
        assert!(ProtocolVersion::new(0).is_none());
        assert!(
            serde_json::from_str::<ControlRequest>(r#"{"command":{"command":"doctor"}}"#).is_err()
        );
        assert!(
            serde_json::from_str::<ControlRequest>(
                r#"{"operation_id":0,"command":{"command":"doctor"}}"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<ControlRequest>(
                r#"{"operation_id":"0","command":{"command":"doctor"}}"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<ControlRequest>(
                r#"{"operation_id":"03","command":{"command":"doctor"}}"#
            )
            .is_err()
        );
        let local = ControlRequest {
            operation_id: OperationId::new(3).unwrap(),
            command: ControlCommand::Doctor {},
        };
        assert_eq!(
            serde_json::from_str::<ControlRequest>(&serde_json::to_string(&local).unwrap())
                .unwrap(),
            local
        );
        let digest = Sha256Digest::try_from(HEX).unwrap();
        let local_artifact = ArtifactClaim::new(
            LocalComponent::Research,
            ComponentVersion::new(1, 0, 0).unwrap(),
            ProtocolVersion::new(1).unwrap(),
            LocalPlatform::WindowsX64Msvc,
            digest,
        );
        assert!(
            serde_json::from_str::<ArtifactClaim>(&serde_json::to_string(&local_artifact).unwrap())
                .is_ok()
        );
        let rejected_edge = serde_json::to_string(&local_artifact)
            .unwrap()
            .replace("research", "edge");
        assert!(serde_json::from_str::<ArtifactClaim>(&rejected_edge).is_err());
        let above_js_safe_integer = OperationId::new(9_007_199_254_740_993).unwrap();
        let decimal_wire = serde_json::to_string(&above_js_safe_integer).unwrap();
        assert_eq!(decimal_wire, "\"9007199254740993\"");
        assert_eq!(
            serde_json::from_str::<OperationId>(&decimal_wire).unwrap(),
            above_js_safe_integer
        );
    }

    #[test]
    fn unknown_is_not_completed_or_rejected() {
        let unknown = ControlOutcome::Unknown {
            operation_id: OperationId::new(1).unwrap(),
            component: ComponentId::Engine,
            stage: FailureStage::Switch,
            next_action: SafeAction::ReconcileOwner,
        };
        let serialized = serde_json::to_string(&unknown).unwrap();
        assert!(serialized.contains("\"result\":\"unknown\""));
        assert_eq!(
            serde_json::from_str::<ControlOutcome>(&serialized).unwrap(),
            unknown
        );
    }

    #[test]
    fn ready_requires_complete_current_evidence() {
        assert!(
            ComponentStatus::new(
                ComponentId::Engine,
                Health::Ready,
                None,
                EvidenceCompleteness::Complete
            )
            .is_none()
        );
        assert!(
            ComponentStatus::new(
                ComponentId::Engine,
                Health::Ready,
                Some(0),
                EvidenceCompleteness::Partial
            )
            .is_none()
        );
        let ready = ComponentStatus::new(
            ComponentId::Engine,
            Health::Ready,
            Some(42),
            EvidenceCompleteness::Complete,
        )
        .unwrap();
        assert_eq!(ready.health(), Health::Ready);
        assert_eq!(
            serde_json::from_str::<ComponentStatus>(&serde_json::to_string(&ready).unwrap())
                .unwrap(),
            ready
        );
        assert!(
            serde_json::from_str::<ComponentStatus>(
                r#"{"component":"engine","health":"ready","evidence_age_ms":null,"evidence":"complete"}"#
            )
            .is_err()
        );
    }

    #[test]
    fn only_proven_install_transitions_are_allowed() {
        use InstallEvent as E;
        use InstallPhase as S;

        assert_eq!(
            S::Staged.transition(E::PreflightVerified).unwrap(),
            S::PreflightPassed
        );
        assert_eq!(
            S::PreflightPassed.transition(E::BeginActivation).unwrap(),
            S::Activating
        );
        assert_eq!(
            S::Activating.transition(E::Launched).unwrap(),
            S::AwaitingReadiness
        );
        assert!(S::Activating.transition(E::Ready).is_err());
        assert!(S::Staged.transition(E::BeginRollback).is_err());
        assert_eq!(
            S::AwaitingReadiness.transition(E::LostOutcome).unwrap(),
            S::Unknown
        );
        assert!(S::Unknown.transition(E::Ready).is_err());
    }
}

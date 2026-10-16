use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{HostControlError, HostControlResult};

pub const DESIRED_STATE_SCHEMA_V1: &str = "okx.host.desired/v1";
pub const DESIRED_STATE_SCHEMA_V2: &str = "okx.host.desired/v2";
pub const DESIRED_STATE_SCHEMA_V3: &str = "okx.host.desired/v3";
// The original CONTROL desired.json is the ONLY durable lifecycle owner.
// Transition evidence is a field here, not a second file or state machine.
const SCHEMA: &str = "okx.host.profile-transition/v1";
const MAX_WITNESS_LIFETIME_MS: u64 = 60_000;
const MAX_SOURCE_OBSERVATION_AGE_MS: u64 = 10_000;
const DEFAULT_DESIRED_PATH: &str = r"C:\okx-control\desired.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AgentDesired {
    Running,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AgentProfile {
    #[default]
    Production,
    DemoAcceptance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentDesiredState {
    pub agent: AgentDesired,
    pub profile: AgentProfile,
}

impl AgentDesiredState {
    pub const fn stopped() -> Self {
        Self {
            agent: AgentDesired::Stopped,
            profile: AgentProfile::Production,
        }
    }

    /// Keep the last committed exchange environment even while the agent is stopped.
    /// Dropping it to Production would let stop + restart bypass profile handoff.
    pub const fn stopped_on(profile: AgentProfile) -> Self {
        Self {
            agent: AgentDesired::Stopped,
            profile,
        }
    }

    pub const fn production_running() -> Self {
        Self {
            agent: AgentDesired::Running,
            profile: AgentProfile::Production,
        }
    }

    pub const fn demo_acceptance_running() -> Self {
        Self {
            agent: AgentDesired::Running,
            profile: AgentProfile::DemoAcceptance,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct DesiredHeader {
    schema: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DesiredFileV1 {
    schema: String,
    agent: AgentDesired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DesiredFileV2 {
    schema: String,
    agent: AgentDesired,
    profile: AgentProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DesiredFileV3 {
    schema: String,
    agent: AgentDesired,
    profile: AgentProfile,
    transition: Option<TransitionRecord>,
}

impl DesiredFileV3 {
    fn initial(state: AgentDesiredState) -> Self {
        Self {
            schema: DESIRED_STATE_SCHEMA_V3.to_owned(),
            agent: state.agent,
            profile: state.profile,
            transition: None,
        }
    }

    fn validate(&self) -> HostControlResult<()> {
        if self.schema != DESIRED_STATE_SCHEMA_V3 {
            return Err(HostControlError::DesiredStateCorrupt);
        }
        if let Some(transition) = &self.transition {
            transition.validate()?;
            if transition.phase == TransitionPhase::SourceFrozen
                && (self.profile != transition.witness.source
                    || self.agent != AgentDesired::Running)
            {
                return Err(HostControlError::ProfileTransitionInvalid);
            }
        }
        Ok(())
    }

    fn desired(&self) -> AgentDesiredState {
        AgentDesiredState {
            agent: self.agent,
            profile: self.profile,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DesiredStateStore {
    path: PathBuf,
}

impl DesiredStateStore {
    pub fn canonical() -> Self {
        Self {
            path: PathBuf::from(DEFAULT_DESIRED_PATH),
        }
    }

    #[cfg(test)]
    pub(crate) fn at(path: PathBuf) -> Self {
        Self { path }
    }

    fn load_full(&self) -> HostControlResult<DesiredFileV3> {
        if !self.path.exists() {
            return Ok(DesiredFileV3::initial(AgentDesiredState::stopped()));
        }

        let bytes = fs::read(&self.path)?;
        let header: DesiredHeader =
            serde_json::from_slice(&bytes).map_err(|_| HostControlError::DesiredStateCorrupt)?;
        let complete = match header.schema.as_str() {
            DESIRED_STATE_SCHEMA_V1 => {
                let old: DesiredFileV1 = serde_json::from_slice(&bytes)
                    .map_err(|_| HostControlError::DesiredStateCorrupt)?;
                DesiredFileV3::initial(AgentDesiredState {
                    agent: old.agent,
                    profile: AgentProfile::Production,
                })
            }
            DESIRED_STATE_SCHEMA_V2 => {
                let old: DesiredFileV2 = serde_json::from_slice(&bytes)
                    .map_err(|_| HostControlError::DesiredStateCorrupt)?;
                DesiredFileV3::initial(AgentDesiredState {
                    agent: old.agent,
                    profile: old.profile,
                })
            }
            DESIRED_STATE_SCHEMA_V3 => serde_json::from_slice(&bytes)
                .map_err(|_| HostControlError::DesiredStateCorrupt)?,
            _ => return Err(HostControlError::DesiredStateCorrupt),
        };
        complete.validate()?;
        Ok(complete)
    }

    fn persist_full(&self, complete: &DesiredFileV3) -> HostControlResult<()> {
        complete.validate()?;
        let parent = self
            .path
            .parent()
            .ok_or(HostControlError::DesiredStateCorrupt)?;
        fs::create_dir_all(parent)?;
        let temp = temp_path(&self.path);
        let mut file = File::create(&temp)?;
        file.write_all(&serde_json::to_vec_pretty(complete)?)?;
        file.sync_all()?;
        drop(file);
        atomic_replace(&temp, &self.path)
    }

    pub fn load(&self) -> HostControlResult<AgentDesiredState> {
        Ok(self.load_full()?.desired())
    }

    /// A normal desired-state write cannot drop a pending transition.
    /// Every change of profile while drained belongs to the same atomic owner.
    pub fn save(&self, state: AgentDesiredState) -> HostControlResult<()> {
        if self.load_full()?.transition.is_some() {
            return Err(HostControlError::ProfileTransitionPending);
        }
        self.persist_full(&DesiredFileV3::initial(state))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub(crate) fn temp_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".tmp");
    PathBuf::from(value)
}

#[cfg(windows)]
pub(crate) fn atomic_replace(source: &Path, destination: &Path) -> HostControlResult<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    fn wide(path: &Path) -> Vec<u16> {
        path.as_os_str().encode_wide().chain(Some(0)).collect()
    }

    let source_w = wide(source);
    let destination_w = wide(destination);
    let ok = unsafe {
        MoveFileExW(
            source_w.as_ptr(),
            destination_w.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };

    if ok == 0 {
        Err(std::io::Error::last_os_error().into())
    } else {
        Ok(())
    }
}

#[cfg(not(windows))]
pub(crate) fn atomic_replace(source: &Path, destination: &Path) -> HostControlResult<()> {
    if destination.exists() {
        fs::remove_file(destination)?;
    }
    fs::rename(source, destination)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("okx-desired-{label}-{}", std::process::id()))
    }

    #[test]
    fn missing_state_fails_closed_to_stopped_production() {
        let root = test_root("missing");
        let _ = fs::remove_dir_all(&root);
        let store = DesiredStateStore::at(root.join("desired.json"));

        assert_eq!(store.load().expect("load"), AgentDesiredState::stopped());
    }

    #[test]
    fn v1_running_state_migrates_to_production_profile() {
        let root = test_root("v1");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("root");
        let path = root.join("desired.json");
        fs::write(
            &path,
            br#"{
  "schema": "okx.host.desired/v1",
  "agent": "RUNNING"
}"#,
        )
        .expect("write v1");
        let store = DesiredStateStore::at(path);

        assert_eq!(
            store.load().expect("load v1"),
            AgentDesiredState::production_running()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn stopped_demo_retains_its_identity_across_durable_restart() {
        let root = test_root("stopped-demo");
        let _ = fs::remove_dir_all(&root);
        let store = DesiredStateStore::at(root.join("desired.json"));
        let stopped = AgentDesiredState::stopped_on(AgentProfile::DemoAcceptance);
        store.save(stopped).expect("save stopped Demo");
        assert_eq!(store.load().expect("reload stopped Demo"), stopped);
        assert_ne!(stopped.profile, AgentProfile::Production);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn v2_demo_profile_round_trips_without_second_state_owner() {
        let root = test_root("v2-demo");
        let _ = fs::remove_dir_all(&root);
        let store = DesiredStateStore::at(root.join("desired.json"));

        store
            .save(AgentDesiredState::demo_acceptance_running())
            .expect("save demo");
        assert_eq!(
            store.load().expect("load demo"),
            AgentDesiredState::demo_acceptance_running()
        );

        store
            .save(AgentDesiredState::production_running())
            .expect("save production");
        assert_eq!(
            store.load().expect("load production"),
            AgentDesiredState::production_running()
        );

        let _ = fs::remove_dir_all(root);
    }
}


#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenSourceWitness {
    pub sequence: u64,
    pub source: AgentProfile,
    pub destination: AgentProfile,
    pub source_pid: u32,
    pub launch_lease_sha256: String,
    pub account_uid_fingerprint: String,
    pub private_ws_generation: u64,
    pub ledger_manifest_sha256: String,
    pub source_tree: String,
    pub agent_sha256: String,
    pub controller_sha256: String,
    pub source_observed_at_ms: u64,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
}

/// Runtime facts that the existing CONTROL must obtain independently from
/// its JOB-owned child and authenticated agent readback. Never copy these
/// fields from a caller-provided witness without checking the native owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundSourceIdentity {
    pub source: AgentProfile,
    pub source_pid: u32,
    pub launch_lease_sha256: String,
    pub account_uid_fingerprint: String,
    pub private_ws_generation: u64,
    pub ledger_manifest_sha256: String,
    pub source_tree: String,
    pub agent_sha256: String,
    pub controller_sha256: String,
}

impl FrozenSourceWitness {
    fn matches_bound_source(&self, bound: &BoundSourceIdentity) -> bool {
        self.source == bound.source
            && self.source_pid == bound.source_pid
            && self.launch_lease_sha256 == bound.launch_lease_sha256
            && self.account_uid_fingerprint == bound.account_uid_fingerprint
            && self.private_ws_generation == bound.private_ws_generation
            && self.ledger_manifest_sha256 == bound.ledger_manifest_sha256
            && self.source_tree == bound.source_tree
            && self.agent_sha256 == bound.agent_sha256
            && self.controller_sha256 == bound.controller_sha256
    }

    fn validate(&self) -> HostControlResult<()> {
        if self.sequence == 0
            || self.source == self.destination
            || self.source_pid == 0
            || self.private_ws_generation == 0
            || !is_lower_hex(&self.launch_lease_sha256, 64)
            || !is_lower_hex(&self.account_uid_fingerprint, 64)
            || !is_lower_hex(&self.ledger_manifest_sha256, 64)
            || !is_lower_hex(&self.source_tree, 40)
            || !is_lower_hex(&self.agent_sha256, 64)
            || !is_lower_hex(&self.controller_sha256, 64)
            || self.source_observed_at_ms == 0
            || self.source_observed_at_ms > self.issued_at_ms
            || self.issued_at_ms.saturating_sub(self.source_observed_at_ms)
                > MAX_SOURCE_OBSERVATION_AGE_MS
            || self.expires_at_ms <= self.issued_at_ms
            || self.expires_at_ms - self.issued_at_ms > MAX_WITNESS_LIFETIME_MS
        {
            return Err(HostControlError::ProfileTransitionInvalid);
        }
        Ok(())
    }
}

fn is_lower_hex(text: &str, expected_length: usize) -> bool {
    text.len() == expected_length
        && text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum TransitionPhase {
    SourceFrozen,
    Consumed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransitionRecord {
    schema: String,
    phase: TransitionPhase,
    witness: FrozenSourceWitness,
    consumed_at_ms: Option<u64>,
    checksum_sha256: String,
}

impl TransitionRecord {
    fn checksum(&self) -> HostControlResult<String> {
        let payload = serde_json::to_vec(&(
            &self.schema,
            &self.phase,
            &self.witness,
            &self.consumed_at_ms,
        ))?;
        Ok(format!("{:x}", Sha256::digest(payload)))
    }

    fn seal(&mut self) -> HostControlResult<()> {
        self.checksum_sha256 = self.checksum()?;
        self.validate()
    }

    fn validate(&self) -> HostControlResult<()> {
        self.witness.validate()?;
        let phase_coherent = match self.phase {
            TransitionPhase::SourceFrozen => self.consumed_at_ms.is_none(),
            TransitionPhase::Consumed => self.consumed_at_ms.is_some_and(|now| {
                now >= self.witness.issued_at_ms && now <= self.witness.expires_at_ms
            }),
        };
        if self.schema != SCHEMA || !phase_coherent || self.checksum_sha256 != self.checksum()? {
            return Err(HostControlError::ProfileTransitionInvalid);
        }
        Ok(())
    }
}


impl DesiredStateStore {
    /// The native CONTROL may eventually stage only an authenticated and
    /// independently checked drained-owner witness. No remote request can
    /// currently invoke this method: there is still NO operational switch.
    pub fn stage_frozen(&self, witness: FrozenSourceWitness, now_ms: u64) -> HostControlResult<()> {
        witness.validate()?;
        if now_ms < witness.issued_at_ms || now_ms > witness.expires_at_ms {
            return Err(HostControlError::ProfileTransitionInvalid);
        }
        let mut complete = self.load_full()?;
        if complete.transition.is_some() {
            return Err(HostControlError::ProfileTransitionPending);
        }
        if complete.agent != AgentDesired::Running || complete.profile != witness.source {
            return Err(HostControlError::ProfileTransitionInvalid);
        }
        let mut record = TransitionRecord {
            schema: SCHEMA.to_owned(),
            phase: TransitionPhase::SourceFrozen,
            witness,
            consumed_at_ms: None,
            checksum_sha256: String::new(),
        };
        record.seal()?;
        complete.transition = Some(record);
        self.persist_full(&complete)
    }

    /// A one-time witness consumption updates desired.json IN PLACE, not a
    /// second journal. No production transition is authorized by this API.
    pub fn consume_once(&self, bound: &BoundSourceIdentity, now_ms: u64) -> HostControlResult<()> {
        let mut complete = self.load_full()?;
        let record = complete
            .transition
            .as_mut()
            .ok_or(HostControlError::ProfileTransitionInvalid)?;
        if record.phase != TransitionPhase::SourceFrozen
            || !record.witness.matches_bound_source(bound)
            || now_ms < record.witness.issued_at_ms
            || now_ms > record.witness.expires_at_ms
        {
            return Err(HostControlError::ProfileTransitionReplay);
        }
        record.phase = TransitionPhase::Consumed;
        record.consumed_at_ms = Some(now_ms);
        record.seal()?;
        self.persist_full(&complete)
    }

    pub fn is_pending(&self) -> HostControlResult<bool> {
        Ok(self.load_full()?.transition.is_some())
    }

    pub fn status_value(&self) -> Value {
        match self.load_full() {
            Ok(complete) => match complete.transition {
                None => json!({
                    "state": "RUNNING_OR_STOPPED",
                    "phase": "NONE",
                    "handoff_admission": "DENIED_NO_CONTROL_BOUND_ONE_TIME_FENCE",
                    "durable_owner": "desired.json"
                }),
                Some(record) => json!({
                    "state": if record.phase == TransitionPhase::SourceFrozen { "SWITCHING" } else { "BLOCKED" },
                    "phase": record.phase,
                    "sequence": record.witness.sequence,
                    "source": record.witness.source,
                    "destination": record.witness.destination,
                    "handoff_admission": "DENIED_TRANSITION_NOT_FINALIZED",
                    "durable_owner": "desired.json"
                }),
            },
            Err(_) => json!({
                "state": "CORRUPT",
                "handoff_admission": "DENIED_TRANSITION_INTEGRITY_UNPROVEN",
                "durable_owner": "desired.json"
            }),
        }
    }
}

#[cfg(test)]
mod single_owner_transition_tests {
    use super::*;

    fn path(label: &str) -> PathBuf {
        std::env::temp_dir()
            .join(format!("okx-single-lifecycle-{label}-{}", std::process::id()))
            .join("desired.json")
    }

    fn witness() -> FrozenSourceWitness {
        FrozenSourceWitness {
            sequence: 1,
            source: AgentProfile::DemoAcceptance,
            destination: AgentProfile::Production,
            source_pid: 321,
            launch_lease_sha256: "a".repeat(64),
            account_uid_fingerprint: "b".repeat(64),
            private_ws_generation: 7,
            ledger_manifest_sha256: "c".repeat(64),
            source_tree: "d".repeat(40),
            agent_sha256: "e".repeat(64),
            controller_sha256: "f".repeat(64),
            source_observed_at_ms: 10_000,
            issued_at_ms: 10_500,
            expires_at_ms: 40_500,
        }
    }

    fn bound(w: &FrozenSourceWitness) -> BoundSourceIdentity {
        BoundSourceIdentity {
            source: w.source,
            source_pid: w.source_pid,
            launch_lease_sha256: w.launch_lease_sha256.clone(),
            account_uid_fingerprint: w.account_uid_fingerprint.clone(),
            private_ws_generation: w.private_ws_generation,
            ledger_manifest_sha256: w.ledger_manifest_sha256.clone(),
            source_tree: w.source_tree.clone(),
            agent_sha256: w.agent_sha256.clone(),
            controller_sha256: w.controller_sha256.clone(),
        }
    }

    #[test]
    fn single_atomic_owner_rejects_replay_and_normal_state_write_after_restart() {
        let path = path("consume");
        let _ = fs::remove_file(&path);
        let store = DesiredStateStore::at(path.clone());
        store.save(AgentDesiredState::demo_acceptance_running()).unwrap();
        let ticket = witness();
        store.stage_frozen(ticket.clone(), 11_000).unwrap();
        assert!(store.is_pending().unwrap());
        assert_eq!(store.load().unwrap(), AgentDesiredState::demo_acceptance_running());
        assert!(matches!(
            store.save(AgentDesiredState::production_running()),
            Err(HostControlError::ProfileTransitionPending)
        ));
        assert!(matches!(
            store.stage_frozen(ticket.clone(), 11_001),
            Err(HostControlError::ProfileTransitionPending)
        ));
        let owner = bound(&ticket);
        for wrong in [
            {
                let mut b = owner.clone();
                b.source_pid += 1;
                b
            },
            {
                let mut b = owner.clone();
                b.private_ws_generation += 1;
                b
            },
            {
                let mut b = owner.clone();
                b.ledger_manifest_sha256 = "0".repeat(64);
                b
            },
            {
                let mut b = owner.clone();
                b.account_uid_fingerprint = "0".repeat(64);
                b
            },
        ] {
            assert!(matches!(
                store.consume_once(&wrong, 11_900),
                Err(HostControlError::ProfileTransitionReplay)
            ));
        }
        store.consume_once(&owner, 12_000).unwrap();
        let reopened = DesiredStateStore::at(path.clone());
        assert!(reopened.is_pending().unwrap());
        assert_eq!(reopened.status_value()["state"], "BLOCKED");
        assert!(matches!(
            reopened.consume_once(&owner, 12_001),
            Err(HostControlError::ProfileTransitionReplay)
        ));
        assert!(matches!(
            reopened.save(AgentDesiredState::production_running()),
            Err(HostControlError::ProfileTransitionPending)
        ));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn invalid_or_corrupt_transition_is_never_interpreted_as_absent() {
        let path = path("corrupt");
        let _ = fs::remove_file(&path);
        let store = DesiredStateStore::at(path.clone());
        store.save(AgentDesiredState::demo_acceptance_running()).unwrap();
        let mut wrong = witness();
        wrong.source_pid = 0;
        assert!(store.stage_frozen(wrong, 11_000).is_err());
        store.stage_frozen(witness(), 11_000).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        let idx = bytes.iter().position(|b| *b == b'd').unwrap();
        bytes[idx] = b'0';
        fs::write(&path, bytes).unwrap();
        assert!(matches!(store.is_pending(), Err(HostControlError::ProfileTransitionInvalid)));
        assert_eq!(store.status_value()["state"], "CORRUPT");
        assert!(store.save(AgentDesiredState::production_running()).is_err());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn clean_legacy_v2_state_migrates_to_v3_without_changing_profile() {
        let path = path("migrate");
        let _ = fs::remove_file(&path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, br#"{"schema":"okx.host.desired/v2","agent":"RUNNING","profile":"demo_acceptance"}"#).unwrap();
        let store = DesiredStateStore::at(path.clone());
        assert_eq!(store.load().unwrap(), AgentDesiredState::demo_acceptance_running());
        store.save(store.load().unwrap()).unwrap();
        let value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(value["schema"], DESIRED_STATE_SCHEMA_V3);
        assert_eq!(value["profile"], "demo_acceptance");
        assert!(value["transition"].is_null());
        let _ = fs::remove_file(path);
    }
}

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
    /// ReadOnly is a durable execution right, NOT implied by the account profile.
    /// Legacy v3 files default to the previous mode, but a verified transition
    /// persists ReadOnly until an independently authorized explicit rearm.
    #[serde(default)]
    read_only: bool,
    transition: Option<TransitionRecord>,
    /// Written BEFORE requesting the child drain. A crash cannot silently
    /// restart source mutations while its exchange POST outcome is unknown.
    #[serde(default)]
    switch_hold: Option<SourceDrainHold>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceDrainHold {
    source: AgentProfile,
    target: AgentProfile,
    source_pid: u32,
}

impl SourceDrainHold {
    fn valid(&self) -> bool {
        self.source != self.target && self.source_pid > 0
    }
}

impl DesiredFileV3 {
    fn initial(state: AgentDesiredState) -> Self {
        Self {
            schema: DESIRED_STATE_SCHEMA_V3.to_owned(),
            agent: state.agent,
            profile: state.profile,
            read_only: false,
            transition: None,
            switch_hold: None,
        }
    }

    fn validate(&self) -> HostControlResult<()> {
        if self.schema != DESIRED_STATE_SCHEMA_V3 {
            return Err(HostControlError::DesiredStateCorrupt);
        }
        if let Some(hold) = &self.switch_hold
            && (!hold.valid()
                || self.transition.is_some()
                || self.agent != AgentDesired::Running
                || self.profile != hold.source)
        {
            return Err(HostControlError::ProfileTransitionInvalid);
        }
        if let Some(transition) = &self.transition {
            transition.validate()?;
            if self.profile != transition.witness.source || self.agent != AgentDesired::Running {
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
            DESIRED_STATE_SCHEMA_V3 => {
                serde_json::from_slice(&bytes).map_err(|_| HostControlError::DesiredStateCorrupt)?
            }
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
        let prior = self.load_full()?;
        if prior.transition.is_some() || prior.switch_hold.is_some() {
            return Err(HostControlError::ProfileTransitionPending);
        }
        let mut next = DesiredFileV3::initial(state);
        // Same-profile stop/restart MUST NOT silently restore Demo mutation
        // acceptance after a verified ReadOnly selection.
        if prior.profile == state.profile {
            next.read_only = prior.read_only;
        }
        self.persist_full(&next)
    }

    /// Read the persisted execution right before EVERY ordinary child launch.
    /// Pending transition target launch is authorized separately by CONTROL.
    pub fn read_only_for(&self, profile: AgentProfile) -> HostControlResult<bool> {
        let state = self.load_full()?;
        if state.profile != profile || state.transition.is_some() || state.switch_hold.is_some() {
            return Err(HostControlError::ProfileTransitionPending);
        }
        Ok(state.read_only)
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
    SourceExited,
}

/// Physical destination facts MUST be independently observed by the existing
/// Job-owning CONTROL after source exit, never obtained from the switch caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedReadOnlyDestination {
    pub profile: AgentProfile,
    pub pid: u32,
    pub account_uid_fingerprint: String,
    pub private_ws_generation: u64,
    pub account_snapshot_received_ms: u64,
    pub agent_sha256: String,
    pub controller_sha256: String,
    pub has_trade_authority: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransitionRecord {
    schema: String,
    phase: TransitionPhase,
    witness: FrozenSourceWitness,
    consumed_at_ms: Option<u64>,
    source_exited_at_ms: Option<u64>,
    checksum_sha256: String,
}

impl TransitionRecord {
    fn checksum(&self) -> HostControlResult<String> {
        let payload = serde_json::to_vec(&(
            &self.schema,
            &self.phase,
            &self.witness,
            &self.consumed_at_ms,
            &self.source_exited_at_ms,
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
            TransitionPhase::SourceFrozen => {
                self.consumed_at_ms.is_none() && self.source_exited_at_ms.is_none()
            }
            TransitionPhase::Consumed => {
                self.source_exited_at_ms.is_none()
                    && self.consumed_at_ms.is_some_and(|now| {
                        now >= self.witness.issued_at_ms && now <= self.witness.expires_at_ms
                    })
            }
            TransitionPhase::SourceExited => self.consumed_at_ms.is_some_and(|consumed| {
                consumed >= self.witness.issued_at_ms
                    && consumed <= self.witness.expires_at_ms
                    && self.source_exited_at_ms.is_some_and(|exited| {
                        exited >= consumed && exited <= self.witness.expires_at_ms
                    })
            }),
        };
        if self.schema != SCHEMA || !phase_coherent || self.checksum_sha256 != self.checksum()? {
            return Err(HostControlError::ProfileTransitionInvalid);
        }
        Ok(())
    }
}

impl DesiredStateStore {
    /// One minimal durable hold BEFORE the first drain request or possible
    /// in-flight POST ambiguity. No new service, file or mutable engine.
    /// Automatic reconcile/restart and release install are fenced while held.
    pub fn begin_source_drain_hold(
        &self,
        source: AgentProfile,
        target: AgentProfile,
        source_pid: u32,
    ) -> HostControlResult<()> {
        let mut state = self.load_full()?;
        if state.transition.is_some()
            || state.switch_hold.is_some()
            || state.profile != source
            || state.agent != AgentDesired::Running
        {
            return Err(HostControlError::ProfileTransitionPending);
        }
        let hold = SourceDrainHold {
            source,
            target,
            source_pid,
        };
        if !hold.valid() {
            return Err(HostControlError::ProfileTransitionInvalid);
        }
        state.switch_hold = Some(hold);
        self.persist_full(&state)
    }

    pub fn permits_owned_source_drain(
        &self,
        source: AgentProfile,
        source_pid: u32,
    ) -> HostControlResult<bool> {
        let state = self.load_full()?;
        Ok(state.transition.is_none()
            && state
                .switch_hold
                .as_ref()
                .is_some_and(|hold| hold.source == source && hold.source_pid == source_pid))
    }

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
        if complete.switch_hold.as_ref().is_some_and(|hold| {
            hold.source != witness.source
                || hold.target != witness.destination
                || hold.source_pid != witness.source_pid
        }) {
            return Err(HostControlError::ProfileTransitionInvalid);
        }
        if complete.agent != AgentDesired::Running || complete.profile != witness.source {
            return Err(HostControlError::ProfileTransitionInvalid);
        }
        let mut record = TransitionRecord {
            schema: SCHEMA.to_owned(),
            phase: TransitionPhase::SourceFrozen,
            witness,
            consumed_at_ms: None,
            source_exited_at_ms: None,
            checksum_sha256: String::new(),
        };
        record.seal()?;
        complete.switch_hold = None;
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

    /// Call ONLY after the exclusive Windows Job owner has observed exact
    /// source PID termination and waited for its process handle. No direct
    /// remote operation is connected to this transition.
    pub fn mark_source_exited(
        &self,
        bound: &BoundSourceIdentity,
        source_pid: u32,
        now_ms: u64,
    ) -> HostControlResult<()> {
        let mut state = self.load_full()?;
        let record = state
            .transition
            .as_mut()
            .ok_or(HostControlError::ProfileTransitionInvalid)?;
        if record.phase != TransitionPhase::Consumed
            || record.witness.source_pid != source_pid
            || !record.witness.matches_bound_source(bound)
            || now_ms < record.consumed_at_ms.unwrap_or(u64::MAX)
            || now_ms > record.witness.expires_at_ms
        {
            return Err(HostControlError::ProfileTransitionReplay);
        }
        record.phase = TransitionPhase::SourceExited;
        record.source_exited_at_ms = Some(now_ms);
        record.seal()?;
        self.persist_full(&state)
    }

    /// Atomic finalization after an independently verified ReadOnly target.
    /// CONTROL must observe the new target PID/UID/WS/rights natively and
    /// ensure source Job exit before invoking. Not a remotely callable path.
    pub fn complete_verified_read_only(
        &self,
        source: &BoundSourceIdentity,
        target: &VerifiedReadOnlyDestination,
        now_ms: u64,
    ) -> HostControlResult<()> {
        let mut state = self.load_full()?;
        let record = state
            .transition
            .as_ref()
            .ok_or(HostControlError::ProfileTransitionInvalid)?;
        if record.phase != TransitionPhase::SourceExited
            || !record.witness.matches_bound_source(source)
            || record.witness.destination != target.profile
            || target.pid == 0
            || target.pid == source.source_pid
            || !is_lower_hex(&target.account_uid_fingerprint, 64)
            || target.account_uid_fingerprint == source.account_uid_fingerprint
            || target.private_ws_generation == 0
            || target.has_trade_authority
            || target.agent_sha256 != source.agent_sha256
            || target.controller_sha256 != source.controller_sha256
            || target.account_snapshot_received_ms == 0
            || now_ms < target.account_snapshot_received_ms
            || now_ms - target.account_snapshot_received_ms > MAX_SOURCE_OBSERVATION_AGE_MS
            || now_ms < record.source_exited_at_ms.unwrap_or(u64::MAX)
            || now_ms > record.witness.expires_at_ms
        {
            return Err(HostControlError::ProfileTransitionInvalid);
        }
        state.profile = target.profile;
        state.agent = AgentDesired::Running;
        state.read_only = true;
        state.transition = None;
        self.persist_full(&state)
    }

    /// Recovery after confirmed source exit AND verified empty Windows Job.
    /// Roll back only to STOPPED on the source profile: never auto-rearm
    /// trading, reset STOP, migrate ledger, or resurrect a different account.
    pub fn recover_stopped_after_empty_job(
        &self,
        source: &BoundSourceIdentity,
        source_pid: u32,
    ) -> HostControlResult<()> {
        let mut state = self.load_full()?;
        let record = state
            .transition
            .as_ref()
            .ok_or(HostControlError::ProfileTransitionInvalid)?;
        if record.phase != TransitionPhase::SourceExited
            || record.witness.source_pid != source_pid
            || !record.witness.matches_bound_source(source)
        {
            return Err(HostControlError::ProfileTransitionInvalid);
        }
        state.agent = AgentDesired::Stopped;
        state.profile = source.source;
        state.transition = None;
        self.persist_full(&state)
    }

    /// Only the source-exited durable phase can temporarily permit one
    /// ReadOnly target spawn under the same existing Windows Job.
    pub fn permits_read_only_target_spawn(&self, target: AgentProfile) -> HostControlResult<bool> {
        let state = self.load_full()?;
        Ok(state.transition.as_ref().is_some_and(|transition| {
            transition.phase == TransitionPhase::SourceExited
                && transition.witness.destination == target
                && state.profile == transition.witness.source
        }))
    }

    /// Explicit operator recovery after the old Job closed on CONTROL crash.
    /// CONTROL must verify its current Job is empty and no child exists.
    /// Never use an elapsed ticket as authority to auto-start an agent.
    pub fn recover_stopped_explicit(&self) -> HostControlResult<AgentDesiredState> {
        let mut state = self.load_full()?;
        let source = if let Some(record) = &state.transition {
            record.witness.source
        } else if let Some(hold) = &state.switch_hold {
            hold.source
        } else {
            return Err(HostControlError::ProfileTransitionInvalid);
        };
        if state.profile != source {
            return Err(HostControlError::ProfileTransitionInvalid);
        }
        state.profile = source;
        state.agent = AgentDesired::Stopped;
        state.transition = None;
        state.switch_hold = None;
        self.persist_full(&state)?;
        Ok(state.desired())
    }

    pub fn is_pending(&self) -> HostControlResult<bool> {
        let state = self.load_full()?;
        Ok(state.transition.is_some() || state.switch_hold.is_some())
    }

    pub fn status_value(&self) -> Value {
        match self.load_full() {
            Ok(complete) => match complete.transition {
                None if complete.switch_hold.is_some() => json!({
                    "state": "SWITCHING",
                    "phase": "DRAIN_REQUESTED",
                    "handoff_admission": "DENIED_PRE_DRAIN_OR_UNCERTAIN_POST",
                    "recovery": "VERIFY_EMPTY_OWNED_JOB_THEN_RECOVER_STOPPED",
                    "durable_owner": "desired.json"
                }),
                None => json!({
                    "state": if complete.agent == AgentDesired::Running { "RUNNING" } else { "STOPPED" },
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
                    "recovery": if record.phase == TransitionPhase::SourceExited {
                        "VERIFY_EMPTY_OWNED_JOB_THEN_RECOVER_STOPPED_OR_VERIFY_READONLY_TARGET"
                    } else {
                        "DO_NOT_RESPAWN_SOURCE_OR_TARGET"
                    },
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
            .join(format!(
                "okx-single-lifecycle-{label}-{}",
                std::process::id()
            ))
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
        store
            .save(AgentDesiredState::demo_acceptance_running())
            .unwrap();
        let ticket = witness();
        store.stage_frozen(ticket.clone(), 11_000).unwrap();
        assert!(store.is_pending().unwrap());
        assert_eq!(
            store.load().unwrap(),
            AgentDesiredState::demo_acceptance_running()
        );
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
    fn verified_readonly_finish_is_atomic_and_preserves_target_identity() {
        let path = path("target-verified");
        let _ = fs::remove_file(&path);
        let store = DesiredStateStore::at(path.clone());
        store
            .save(AgentDesiredState::demo_acceptance_running())
            .unwrap();
        let ticket = witness();
        let source = bound(&ticket);
        store.stage_frozen(ticket, 11_000).unwrap();
        assert!(store.mark_source_exited(&source, 321, 11_100).is_err());
        store.consume_once(&source, 11_150).unwrap();
        assert!(store.mark_source_exited(&source, 999, 11_200).is_err());
        store.mark_source_exited(&source, 321, 11_200).unwrap();
        assert_eq!(store.status_value()["state"], "BLOCKED");
        assert!(store.mark_source_exited(&source, 321, 11_300).is_err());

        let target = VerifiedReadOnlyDestination {
            profile: AgentProfile::Production,
            pid: 555,
            account_uid_fingerprint: "9".repeat(64),
            private_ws_generation: 9,
            account_snapshot_received_ms: 11_500,
            agent_sha256: source.agent_sha256.clone(),
            controller_sha256: source.controller_sha256.clone(),
            has_trade_authority: false,
        };
        let mut wrong = target.clone();
        wrong.has_trade_authority = true;
        assert!(
            store
                .complete_verified_read_only(&source, &wrong, 11_600)
                .is_err()
        );
        wrong = target.clone();
        wrong.account_uid_fingerprint = source.account_uid_fingerprint.clone();
        assert!(
            store
                .complete_verified_read_only(&source, &wrong, 11_600)
                .is_err()
        );
        wrong = target.clone();
        wrong.agent_sha256 = "0".repeat(64);
        assert!(
            store
                .complete_verified_read_only(&source, &wrong, 11_600)
                .is_err()
        );
        wrong = target.clone();
        wrong.private_ws_generation = 0;
        assert!(
            store
                .complete_verified_read_only(&source, &wrong, 11_600)
                .is_err()
        );
        wrong = target.clone();
        wrong.pid = 321;
        assert!(
            store
                .complete_verified_read_only(&source, &wrong, 11_600)
                .is_err()
        );
        wrong = target.clone();
        wrong.account_snapshot_received_ms = 1;
        assert!(
            store
                .complete_verified_read_only(&source, &wrong, 11_600)
                .is_err()
        );

        store
            .complete_verified_read_only(&source, &target, 11_600)
            .unwrap();
        let recovered = DesiredStateStore::at(path.clone());
        assert_eq!(
            recovered.load().unwrap(),
            AgentDesiredState::production_running()
        );
        assert!(!recovered.is_pending().unwrap());
        assert!(recovered.read_only_for(AgentProfile::Production).unwrap());
        recovered.save(AgentDesiredState::production_running()).unwrap();
        assert!(recovered.read_only_for(AgentProfile::Production).unwrap());
        assert!(
            recovered
                .complete_verified_read_only(&source, &target, 11_601)
                .is_err()
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn reverse_selection_persists_demo_read_only_across_cold_restart_and_normal_start() {
        let path = path("demo-readonly-restart");
        let _ = fs::remove_file(&path);
        let store = DesiredStateStore::at(path.clone());
        store.save(AgentDesiredState::production_running()).unwrap();
        let mut ticket = witness();
        ticket.source = AgentProfile::Production;
        ticket.destination = AgentProfile::DemoAcceptance;
        let bound = bound(&ticket);
        store.stage_frozen(ticket, 11_000).unwrap();
        store.consume_once(&bound, 11_100).unwrap();
        store.mark_source_exited(&bound, 321, 11_200).unwrap();
        store.complete_verified_read_only(
            &bound,
            &VerifiedReadOnlyDestination {
                profile: AgentProfile::DemoAcceptance,
                pid: 555,
                account_uid_fingerprint: "9".repeat(64),
                private_ws_generation: 9,
                account_snapshot_received_ms: 11_500,
                agent_sha256: bound.agent_sha256.clone(),
                controller_sha256: bound.controller_sha256.clone(),
                has_trade_authority: false,
            },
            11_600,
        ).unwrap();
        let cold = DesiredStateStore::at(path.clone());
        assert!(cold.read_only_for(AgentProfile::DemoAcceptance).unwrap());
        cold.save(AgentDesiredState::stopped_on(AgentProfile::DemoAcceptance)).unwrap();
        cold.save(AgentDesiredState::demo_acceptance_running()).unwrap();
        assert!(DesiredStateStore::at(path.clone())
            .read_only_for(AgentProfile::DemoAcceptance).unwrap());
        assert!(cold.read_only_for(AgentProfile::Production).is_err());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn post_exit_recovery_stops_same_source_and_does_not_auto_start_live() {
        let path = path("recovery");
        let _ = fs::remove_file(&path);
        let store = DesiredStateStore::at(path.clone());
        store
            .save(AgentDesiredState::demo_acceptance_running())
            .unwrap();
        let ticket = witness();
        let source = bound(&ticket);
        store.stage_frozen(ticket, 11_000).unwrap();
        assert!(store.recover_stopped_after_empty_job(&source, 321).is_err());
        store.consume_once(&source, 11_100).unwrap();
        assert!(store.recover_stopped_after_empty_job(&source, 321).is_err());
        store.mark_source_exited(&source, 321, 11_200).unwrap();
        assert!(store.recover_stopped_after_empty_job(&source, 999).is_err());
        store.recover_stopped_after_empty_job(&source, 321).unwrap();
        let restarted_control = DesiredStateStore::at(path.clone());
        assert_eq!(
            restarted_control.load().unwrap(),
            AgentDesiredState::stopped_on(AgentProfile::DemoAcceptance)
        );
        assert!(!restarted_control.is_pending().unwrap());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn durable_pre_drain_hold_prevents_auto_rearm_on_crash_or_uncertain_ack() {
        let path = path("pre-drain-hold");
        let _ = fs::remove_file(&path);
        let store = DesiredStateStore::at(path.clone());
        store
            .save(AgentDesiredState::demo_acceptance_running())
            .unwrap();
        store
            .begin_source_drain_hold(AgentProfile::DemoAcceptance, AgentProfile::Production, 321)
            .unwrap();
        assert!(store.is_pending().unwrap());
        assert_eq!(store.status_value()["phase"], "DRAIN_REQUESTED");
        assert!(
            store
                .permits_owned_source_drain(AgentProfile::DemoAcceptance, 321)
                .unwrap()
        );
        assert!(
            !store
                .permits_owned_source_drain(AgentProfile::DemoAcceptance, 999)
                .unwrap()
        );
        assert!(
            !store
                .permits_owned_source_drain(AgentProfile::Production, 321)
                .unwrap()
        );
        assert!(
            store
                .save(AgentDesiredState::demo_acceptance_running())
                .is_err()
        );
        assert!(store.save(AgentDesiredState::production_running()).is_err());
        let reopened = DesiredStateStore::at(path.clone());
        assert!(reopened.is_pending().unwrap());
        assert_eq!(reopened.status_value()["state"], "SWITCHING");
        let mut incorrect = witness();
        incorrect.source_pid = 999;
        assert!(reopened.stage_frozen(incorrect, 11_000).is_err());
        reopened.stage_frozen(witness(), 11_000).unwrap();
        assert!(reopened.is_pending().unwrap());
        assert_eq!(reopened.status_value()["phase"], "SOURCE_FROZEN");
        assert!(
            !reopened
                .permits_owned_source_drain(AgentProfile::DemoAcceptance, 321)
                .unwrap()
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn explicit_empty_job_recovery_never_resurrects_venue_or_target() {
        let path = path("explicit-stop");
        let _ = fs::remove_file(&path);
        let store = DesiredStateStore::at(path.clone());
        store
            .save(AgentDesiredState::demo_acceptance_running())
            .unwrap();
        let witness = witness();
        assert!(store.recover_stopped_explicit().is_err());
        store.stage_frozen(witness.clone(), 11_000).unwrap();
        assert!(
            !store
                .permits_read_only_target_spawn(AgentProfile::Production)
                .unwrap()
        );
        assert_eq!(
            store.recover_stopped_explicit().unwrap(),
            AgentDesiredState::stopped_on(AgentProfile::DemoAcceptance)
        );
        assert!(!store.is_pending().unwrap());
        assert_eq!(
            DesiredStateStore::at(path.clone()).load().unwrap(),
            AgentDesiredState::stopped_on(AgentProfile::DemoAcceptance)
        );
        assert!(store.recover_stopped_explicit().is_err());
        store
            .save(AgentDesiredState::demo_acceptance_running())
            .unwrap();
        let bound = bound(&witness);
        store.stage_frozen(witness, 11_000).unwrap();
        store.consume_once(&bound, 11_100).unwrap();
        store
            .mark_source_exited(&bound, bound.source_pid, 11_200)
            .unwrap();
        assert!(
            store
                .permits_read_only_target_spawn(AgentProfile::Production)
                .unwrap()
        );
        assert!(
            !store
                .permits_read_only_target_spawn(AgentProfile::DemoAcceptance)
                .unwrap()
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn invalid_or_corrupt_transition_is_never_interpreted_as_absent() {
        let path = path("corrupt");
        let _ = fs::remove_file(&path);
        let store = DesiredStateStore::at(path.clone());
        store
            .save(AgentDesiredState::demo_acceptance_running())
            .unwrap();
        let mut wrong = witness();
        wrong.source_pid = 0;
        assert!(store.stage_frozen(wrong, 11_000).is_err());
        store.stage_frozen(witness(), 11_000).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        let needle = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let idx = bytes
            .windows(needle.len())
            .position(|window| window == needle)
            .unwrap();
        bytes[idx] = b'0';
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            store.is_pending(),
            Err(HostControlError::ProfileTransitionInvalid)
        ));
        assert_eq!(store.status_value()["state"], "CORRUPT");
        assert!(store.save(AgentDesiredState::production_running()).is_err());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn clean_legacy_v2_state_migrates_to_v3_without_changing_profile() {
        let path = path("migrate");
        let _ = fs::remove_file(&path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            br#"{"schema":"okx.host.desired/v2","agent":"RUNNING","profile":"demo_acceptance"}"#,
        )
        .unwrap();
        let store = DesiredStateStore::at(path.clone());
        assert_eq!(
            store.load().unwrap(),
            AgentDesiredState::demo_acceptance_running()
        );
        store.save(store.load().unwrap()).unwrap();
        let value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(value["schema"], DESIRED_STATE_SCHEMA_V3);
        assert_eq!(value["profile"], "demo_acceptance");
        assert!(value["transition"].is_null());
        let _ = fs::remove_file(path);
    }
}

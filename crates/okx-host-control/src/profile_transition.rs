//! Crash-safe CONTROL-owned profile transition journal.
//!
//! This journal is a *fence*, never an exchange or account attestation. Only a
//! future authenticated, already-frozen native source agent may mint the input
//! witness. There is intentionally NO remote command to create/consume it yet.
//! A pending or corrupt journal prevents automatic lifecycle transitions.

use std::{fs::{self, File}, io::Write, path::PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{
    HostControlError, HostControlResult,
    desired::{AgentProfile, atomic_replace, temp_path},
};

const SCHEMA: &str = "okx.host.profile-transition/v1";
const CANONICAL_PATH: &str = r"C:\okx-control\profile-transition.json";
const MAX_WITNESS_LIFETIME_MS: u64 = 60_000;
const MAX_SOURCE_OBSERVATION_AGE_MS: u64 = 10_000;

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

impl FrozenSourceWitness {
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
        && text.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
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
        if self.schema != SCHEMA
            || !phase_coherent
            || self.checksum_sha256 != self.checksum()?
        {
            return Err(HostControlError::ProfileTransitionInvalid);
        }
        Ok(())
    }
}

/// The same existing Windows CONTROL owns this atomic file, just as it owns
/// desired.json. A process restart must not turn a consumed source ticket back
/// into a fresh profile switch.
pub struct ProfileTransitionStore {
    path: PathBuf,
}

impl ProfileTransitionStore {
    pub fn canonical() -> Self {
        Self { path: PathBuf::from(CANONICAL_PATH) }
    }

    #[cfg(test)]
    pub(crate) fn at(path: PathBuf) -> Self {
        Self { path }
    }

    fn load(&self) -> HostControlResult<Option<TransitionRecord>> {
        if !self.path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&self.path)?;
        let record: TransitionRecord = serde_json::from_slice(&bytes)
            .map_err(|_| HostControlError::ProfileTransitionInvalid)?;
        record.validate()?;
        Ok(Some(record))
    }

    fn persist(&self, record: &TransitionRecord) -> HostControlResult<()> {
        record.validate()?;
        let parent = self.path.parent().ok_or(HostControlError::ProfileTransitionInvalid)?;
        fs::create_dir_all(parent)?;
        let temp = temp_path(&self.path);
        let mut file = File::create(&temp)?;
        file.write_all(&serde_json::to_vec_pretty(record)?)?;
        file.sync_all()?;
        drop(file);
        atomic_replace(&temp, &self.path)
    }

    /// A frozen native source witness is a prerequisite, not an authorization.
    /// No existing CONTROL request can invoke this method.
    pub fn stage_frozen(&self, witness: FrozenSourceWitness, now_ms: u64)
        -> HostControlResult<()>
    {
        witness.validate()?;
        if now_ms < witness.issued_at_ms || now_ms > witness.expires_at_ms {
            return Err(HostControlError::ProfileTransitionInvalid);
        }
        // There is intentionally no "clear pending" or "retry" operation:
        // an unknown/crashed transition cannot roll back to implicit authority.
        if self.load()?.is_some() {
            return Err(HostControlError::ProfileTransitionPending);
        }
        let mut record = TransitionRecord {
            schema: SCHEMA.to_owned(),
            phase: TransitionPhase::SourceFrozen,
            witness,
            consumed_at_ms: None,
            checksum_sha256: String::new(),
        };
        record.seal()?;
        self.persist(&record)
    }

    /// Durable single-use consumption. A second use, including after process
    /// restart or an uncertain response, is ALWAYS denied.
    pub fn consume_once(&self, expected_lease_sha256: &str, now_ms: u64)
        -> HostControlResult<()>
    {
        let Some(mut record) = self.load()? else {
            return Err(HostControlError::ProfileTransitionInvalid);
        };
        if record.phase != TransitionPhase::SourceFrozen
            || record.witness.launch_lease_sha256 != expected_lease_sha256
            || now_ms < record.witness.issued_at_ms
            || now_ms > record.witness.expires_at_ms
        {
            return Err(HostControlError::ProfileTransitionReplay);
        }
        record.phase = TransitionPhase::Consumed;
        record.consumed_at_ms = Some(now_ms);
        record.seal()?;
        self.persist(&record)
    }

    pub fn is_pending(&self) -> HostControlResult<bool> {
        Ok(self.load()?.is_some())
    }

    pub fn status_value(&self) -> Value {
        match self.load() {
            Ok(None) => json!({
                "schema": SCHEMA,
                "state": "ABSENT",
                "handoff_admission": "DENIED_NO_CONTROL_BOUND_ONE_TIME_FENCE"
            }),
            Ok(Some(record)) => json!({
                "schema": SCHEMA,
                "state": record.phase,
                "sequence": record.witness.sequence,
                "source": record.witness.source,
                "destination": record.witness.destination,
                "handoff_admission": "DENIED_TRANSITION_NOT_FINALIZED"
            }),
            Err(_) => json!({
                "schema": SCHEMA,
                "state": "CORRUPT",
                "handoff_admission": "DENIED_TRANSITION_INTEGRITY_UNPROVEN"
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "okx-profile-fence-{label}-{}", std::process::id()
        )).join("profile-transition.json")
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

    #[test]
    fn frozen_ticket_persists_one_use_and_replay_denial_across_restart() {
        let path = temp("single");
        let _ = fs::remove_file(&path);
        let store = ProfileTransitionStore::at(path.clone());
        let ticket = witness();
        assert!(!store.is_pending().unwrap());
        store.stage_frozen(ticket.clone(), 11_000).unwrap();
        assert!(store.is_pending().unwrap());
        assert!(matches!(store.stage_frozen(ticket.clone(), 11_001),
            Err(HostControlError::ProfileTransitionPending)));
        assert!(matches!(store.consume_once(&"0".repeat(64), 11_002),
            Err(HostControlError::ProfileTransitionReplay)));
        store.consume_once(&ticket.launch_lease_sha256, 12_000).unwrap();

        // The next controller process sees the consumed proof, not a fresh
        // opportunity to re-use or implicitly swap profiles.
        let reopened = ProfileTransitionStore::at(path.clone());
        assert!(reopened.is_pending().unwrap());
        assert!(matches!(reopened.consume_once(&ticket.launch_lease_sha256, 12_001),
            Err(HostControlError::ProfileTransitionReplay)));
        assert_eq!(reopened.status_value()["handoff_admission"],
            "DENIED_TRANSITION_NOT_FINALIZED");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn rejects_wrong_uid_release_pid_generation_age_and_corruption() {
        let path = temp("negative");
        let _ = fs::remove_file(&path);
        let store = ProfileTransitionStore::at(path.clone());
        let mut invalid = witness();
        invalid.source = invalid.destination;
        assert!(store.stage_frozen(invalid, 11_000).is_err());
        let mut invalid = witness();
        invalid.source_pid = 0;
        assert!(store.stage_frozen(invalid, 11_000).is_err());
        let mut invalid = witness();
        invalid.private_ws_generation = 0;
        assert!(store.stage_frozen(invalid, 11_000).is_err());
        let mut invalid = witness();
        invalid.account_uid_fingerprint.clear();
        assert!(store.stage_frozen(invalid, 11_000).is_err());
        let mut invalid = witness();
        invalid.source_tree = "wrong".to_owned();
        assert!(store.stage_frozen(invalid, 11_000).is_err());
        let mut invalid = witness();
        invalid.issued_at_ms = invalid.source_observed_at_ms + 20_000;
        assert!(store.stage_frozen(invalid, 21_000).is_err());
        assert!(store.stage_frozen(witness(), 41_000).is_err());
        store.stage_frozen(witness(), 11_000).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        let idx = bytes.iter().position(|b| *b == b'd').unwrap();
        bytes[idx] = b'0';
        fs::write(&path, bytes).unwrap();
        assert!(matches!(store.is_pending(),
            Err(HostControlError::ProfileTransitionInvalid)));
        assert_eq!(store.status_value()["state"], "CORRUPT");
        let _ = fs::remove_file(path);
    }
}

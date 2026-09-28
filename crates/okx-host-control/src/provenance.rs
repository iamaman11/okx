use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use okx_github::REPOSITORY_ID;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{HostControlError, HostControlResult};

pub const INSTALLED_AGENT_PROVENANCE_SCHEMA_V1: &str =
    "okx.host-control.installed-agent/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledAgentProvenance {
    pub schema: String,
    pub repository_id: u64,
    pub run_id: u64,
    pub artifact_id: u64,
    pub source_head_sha: String,
    pub source_tree: String,
    pub rust_version: String,
    pub agent_sha256: String,
}

impl InstalledAgentProvenance {
    pub fn verified(
        run_id: u64,
        artifact_id: u64,
        source_head_sha: String,
        source_tree: String,
        rust_version: String,
        agent_sha256: String,
    ) -> Self {
        Self {
            schema: INSTALLED_AGENT_PROVENANCE_SCHEMA_V1.to_owned(),
            repository_id: REPOSITORY_ID,
            run_id,
            artifact_id,
            source_head_sha,
            source_tree,
            rust_version,
            agent_sha256,
        }
    }

    fn valid(&self) -> bool {
        self.schema == INSTALLED_AGENT_PROVENANCE_SCHEMA_V1
            && self.repository_id == REPOSITORY_ID
            && self.run_id != 0
            && self.artifact_id != 0
            && is_lower_hex(&self.source_head_sha, 40)
            && is_lower_hex(&self.source_tree, 40)
            && is_lower_hex(&self.agent_sha256, 64)
            && !self.rust_version.trim().is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct InstalledAgentProvenanceStore {
    path: PathBuf,
}

impl InstalledAgentProvenanceStore {
    pub fn canonical() -> Self {
        Self::at(PathBuf::from(r"C:\okx-runtime\installed-agent.json"))
    }

    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn save(&self, provenance: &InstalledAgentProvenance) -> HostControlResult<()> {
        if !provenance.valid() {
            return Err(HostControlError::ArtifactVerification(
                "installed agent provenance is invalid",
            ));
        }

        let parent = self.path.parent().ok_or(HostControlError::ArtifactVerification(
            "installed agent provenance path has no parent",
        ))?;
        fs::create_dir_all(parent)?;

        let payload = serde_json::to_vec_pretty(provenance)?;
        let temp = temp_path(&self.path);
        let mut file = File::create(&temp)?;
        file.write_all(&payload)?;
        file.sync_all()?;
        drop(file);

        atomic_replace(&temp, &self.path)
    }

    pub fn status_value(&self, binary_path: &Path) -> Value {
        let binary_present = binary_path.is_file();
        let binary_sha256 = binary_present
            .then(|| sha256_file(binary_path))
            .transpose();

        let record = match self.load() {
            Ok(value) => value,
            Err(error) => {
                return json!({
                    "state": "UNKNOWN",
                    "reason": "PROVENANCE_INVALID",
                    "binary_path": binary_path,
                    "binary_present": binary_present,
                    "binary_sha256": binary_sha256.ok().flatten(),
                    "provenance_path": &self.path,
                    "provenance": Value::Null,
                    "diagnostic": error.to_string()
                });
            }
        };

        let current_sha = match binary_sha256 {
            Ok(value) => value,
            Err(error) => {
                return json!({
                    "state": "UNKNOWN",
                    "reason": "BINARY_HASH_UNAVAILABLE",
                    "binary_path": binary_path,
                    "binary_present": binary_present,
                    "binary_sha256": Value::Null,
                    "provenance_path": &self.path,
                    "provenance": record,
                    "diagnostic": error.to_string()
                });
            }
        };

        let Some(provenance) = record else {
            return json!({
                "state": "UNKNOWN",
                "reason": "PROVENANCE_MISSING",
                "binary_path": binary_path,
                "binary_present": binary_present,
                "binary_sha256": current_sha,
                "provenance_path": &self.path,
                "provenance": Value::Null
            });
        };

        if !binary_present {
            return json!({
                "state": "MISMATCH",
                "reason": "BINARY_MISSING",
                "binary_path": binary_path,
                "binary_present": false,
                "binary_sha256": Value::Null,
                "provenance_path": &self.path,
                "provenance": provenance
            });
        }

        let state = if current_sha.as_deref() == Some(provenance.agent_sha256.as_str()) {
            "VERIFIED"
        } else {
            "MISMATCH"
        };
        let reason = if state == "VERIFIED" {
            "HASH_MATCH"
        } else {
            "BINARY_HASH_MISMATCH"
        };

        json!({
            "state": state,
            "reason": reason,
            "binary_path": binary_path,
            "binary_present": true,
            "binary_sha256": current_sha,
            "provenance_path": &self.path,
            "provenance": provenance
        })
    }

    fn load(&self) -> HostControlResult<Option<InstalledAgentProvenance>> {
        if !self.path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&self.path)?;
        let provenance: InstalledAgentProvenance = serde_json::from_slice(&bytes)?;
        if !provenance.valid() {
            return Err(HostControlError::ArtifactVerification(
                "installed agent provenance identity is invalid",
            ));
        }
        Ok(Some(provenance))
    }
}

fn sha256_file(path: &Path) -> HostControlResult<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex_digest(&digest.finalize()))
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .concat()
}

fn is_lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn temp_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".tmp");
    PathBuf::from(value)
}

#[cfg(windows)]
fn atomic_replace(source: &Path, destination: &Path) -> HostControlResult<()> {
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
fn atomic_replace(source: &Path, destination: &Path) -> HostControlResult<()> {
    if destination.exists() {
        fs::remove_file(destination)?;
    }
    fs::rename(source, destination)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "okx-installed-provenance-{name}-{}",
            std::process::id()
        ))
    }

    fn fixture(hash: String) -> InstalledAgentProvenance {
        InstalledAgentProvenance::verified(
            42,
            84,
            "a".repeat(40),
            "b".repeat(40),
            "rustc 1.95.0".to_owned(),
            hash,
        )
    }

    #[test]
    fn legacy_binary_without_record_is_unknown() {
        let root = temp_root("unknown");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("root");
        let binary = root.join("okx-agent.exe");
        fs::write(&binary, b"agent").expect("binary");
        let store = InstalledAgentProvenanceStore::at(root.join("installed-agent.json"));

        let status = store.status_value(&binary);
        assert_eq!(status["state"], "UNKNOWN");
        assert_eq!(status["reason"], "PROVENANCE_MISSING");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn persisted_record_survives_store_reopen_and_verifies_binary() {
        let root = temp_root("verified");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("root");
        let binary = root.join("okx-agent.exe");
        fs::write(&binary, b"agent").expect("binary");
        let hash = sha256_file(&binary).expect("hash");
        let path = root.join("installed-agent.json");

        InstalledAgentProvenanceStore::at(&path)
            .save(&fixture(hash.clone()))
            .expect("save");
        let reopened = InstalledAgentProvenanceStore::at(&path);
        let status = reopened.status_value(&binary);

        assert_eq!(status["state"], "VERIFIED");
        assert_eq!(status["reason"], "HASH_MATCH");
        assert_eq!(status["binary_sha256"], hash);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn changed_binary_is_mismatch() {
        let root = temp_root("mismatch");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("root");
        let binary = root.join("okx-agent.exe");
        fs::write(&binary, b"agent-v1").expect("binary");
        let path = root.join("installed-agent.json");
        let hash = sha256_file(&binary).expect("hash");
        let store = InstalledAgentProvenanceStore::at(&path);
        store.save(&fixture(hash)).expect("save");

        fs::write(&binary, b"agent-v2").expect("mutate");
        let status = store.status_value(&binary);

        assert_eq!(status["state"], "MISMATCH");
        assert_eq!(status["reason"], "BINARY_HASH_MISMATCH");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn status_contains_no_secret_material() {
        let root = temp_root("public");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("root");
        let binary = root.join("okx-agent.exe");
        fs::write(&binary, b"agent").expect("binary");
        let hash = sha256_file(&binary).expect("hash");
        let store = InstalledAgentProvenanceStore::at(root.join("installed-agent.json"));
        store.save(&fixture(hash)).expect("save");

        let json = serde_json::to_string(&store.status_value(&binary)).expect("json");
        assert!(!json.contains("token"));
        assert!(!json.contains("secret"));
        assert!(!json.contains("private"));

        let _ = fs::remove_dir_all(root);
    }
}

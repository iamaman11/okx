//! Immutable content-addressed research artifacts and causal strategy replay.
pub mod strategy_registry;
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};
#[derive(Debug, thiserror::Error)]
pub enum ResearchError {
    #[error("research storage: {0}")]
    Io(#[from] std::io::Error),
    #[error("research serialization: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("invalid artifact identity")]
    InvalidArtifactId,
    #[error("artifact content does not match identity")]
    ArtifactIdentityMismatch,
}
const EVIDENCE_DIR: &str = "evidence";
const SOURCE_CACHE_DIR: &str = "source-cache";
pub fn sha256_bytes(value: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(value))
}
pub fn canonical_sha256<T: Serialize>(value: &T) -> Result<String, ResearchError> {
    Ok(sha256_bytes(&serde_json::to_vec(value)?))
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

    pub fn read_evidence_json<T: DeserializeOwned>(
        &self,
        artifact_id: &str,
    ) -> Result<T, ResearchError> {
        let bytes = self.read_evidence(artifact_id)?;
        Ok(serde_json::from_slice(&bytes)?)
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
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let temp = path.with_extension(format!(
        "tmp-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
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

impl ResearchArtifactStore {
    /// Delete only a caller-selected, validated content address after retention reachability checks.
    pub fn remove_evidence(&self, id: &str) -> Result<(), ResearchError> {
        let path = self
            .evidence_dir()
            .join(format!("{}.json", sha256_hex(id)?));
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn immutable_artifact_detects_corruption_and_path_escape() {
        let root = std::env::temp_dir().join(format!("okx-artifact-{}", std::process::id()));
        let store = ResearchArtifactStore::at(root);
        let (id, path) = store.publish_evidence(&vec![1, 2, 3]).unwrap();
        assert_eq!(
            store.read_evidence_json::<Vec<i32>>(&id).unwrap(),
            vec![1, 2, 3]
        );
        assert!(store.read_evidence("../escape").is_err());
        std::fs::write(&path, b"corrupt").unwrap();
        assert!(store.read_evidence(&id).is_err());
        std::fs::remove_file(path).unwrap();
    }
}

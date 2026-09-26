use std::{
    collections::BTreeMap,
    io::{Cursor, Read},
};

use okx_github::{GitHubClient, REPOSITORY_ID};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use zip::ZipArchive;

use crate::{HostControlError, HostControlResult, executor::HostExecutor};

const BUNDLE_SCHEMA_V1: &str = "okx.windows.bundle/v1";
const MAX_MANIFEST_BYTES: u64 = 64 * 1024;
const MAX_AGENT_BYTES: u64 = 128 * 1024 * 1024;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundleManifest {
    schema: String,
    repository_id: u64,
    source_head_sha: String,
    source_tree: String,
    rust_version: String,
    files: BTreeMap<String, BundleFile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundleFile {
    sha256: String,
}

pub async fn deploy_agent(
    github: &GitHubClient,
    executor: &mut HostExecutor,
    run_id: u64,
    artifact_id: u64,
    expected_source_tree: &str,
) -> HostControlResult<Value> {
    let run = github.workflow_run(run_id).await?;
    if run.name != "CI"
        || run.event != "pull_request"
        || run.status != "completed"
        || run.conclusion.as_deref() != Some("success")
    {
        return Err(HostControlError::ArtifactVerification(
            "workflow run is not an accepted successful PR CI run",
        ));
    }

    let artifact = github.workflow_artifact(run_id, artifact_id).await?;
    if artifact.expired {
        return Err(HostControlError::ArtifactVerification(
            "workflow artifact is expired",
        ));
    }

    let expected_name = format!("okx-windows-bundle-{}", run.head_sha);
    if artifact.name != expected_name {
        return Err(HostControlError::ArtifactVerification(
            "workflow artifact name does not bind to run head",
        ));
    }

    let zip_bytes = github.download_artifact_zip(artifact_id).await?;
    let (manifest, agent_bytes) = parse_bundle(&zip_bytes)?;

    if manifest.schema != BUNDLE_SCHEMA_V1
        || manifest.repository_id != REPOSITORY_ID
        || manifest.source_head_sha != run.head_sha
        || manifest.source_tree != expected_source_tree
    {
        return Err(HostControlError::ArtifactVerification(
            "bundle manifest identity mismatch",
        ));
    }

    let origin_main_tree = executor.fetch_origin_main_tree()?;
    if origin_main_tree != manifest.source_tree {
        return Err(HostControlError::ArtifactSourceTreeMismatch);
    }

    let declared_agent_hash = manifest
        .files
        .get("okx-agent.exe")
        .ok_or(HostControlError::ArtifactVerification(
            "manifest does not contain okx-agent.exe",
        ))?
        .sha256
        .as_str();

    let actual_agent_hash = sha256_hex(&agent_bytes);
    if actual_agent_hash != declared_agent_hash {
        return Err(HostControlError::ArtifactHashMismatch);
    }

    executor.install_verified_agent(&agent_bytes)?;

    Ok(json!({
        "run_id": run_id,
        "artifact_id": artifact_id,
        "source_head_sha": manifest.source_head_sha,
        "source_tree": manifest.source_tree,
        "rust_version": manifest.rust_version,
        "agent_sha256": actual_agent_hash,
        "installed": true
    }))
}

fn parse_bundle(zip_bytes: &[u8]) -> HostControlResult<(BundleManifest, Vec<u8>)> {
    let reader = Cursor::new(zip_bytes);
    let mut archive = ZipArchive::new(reader)?;

    let manifest_bytes = read_entry(&mut archive, "manifest.json", MAX_MANIFEST_BYTES)?;
    let manifest: BundleManifest = serde_json::from_slice(&manifest_bytes)?;

    let agent_bytes = read_entry(&mut archive, "okx-agent.exe", MAX_AGENT_BYTES)?;
    Ok((manifest, agent_bytes))
}

fn read_entry(
    archive: &mut ZipArchive<Cursor<&[u8]>>,
    name: &str,
    max_bytes: u64,
) -> HostControlResult<Vec<u8>> {
    let mut entry = archive.by_name(name)?;
    if entry.size() > max_bytes {
        return Err(HostControlError::ArtifactVerification(
            "bundle entry exceeds size limit",
        ));
    }

    let mut bytes = Vec::with_capacity(entry.size() as usize);
    entry.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .concat()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_is_lowercase_hex() {
        let value = sha256_hex(b"okx");
        assert_eq!(value.len(), 64);
        assert!(
            value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
    }
}

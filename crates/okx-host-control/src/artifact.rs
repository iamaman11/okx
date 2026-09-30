use std::{
    collections::BTreeMap,
    io::{Cursor, Read},
};

use okx_github::{GitHubClient, REPOSITORY_ID};
use okx_host_launcher::ControllerVersion;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use zip::{ZipArchive, result::ZipError};

use crate::{
    HostControlError, HostControlResult,
    controller_update::{self, ControllerUpdateCandidate},
    executor::HostExecutor,
    provenance::{InstalledAgentProvenance, InstalledAgentProvenanceStore},
    root_migration,
};

const BUNDLE_SCHEMA_V1: &str = "okx.windows.bundle/v1";
const MAX_MANIFEST_BYTES: u64 = 64 * 1024;
const MAX_AGENT_BYTES: u64 = 128 * 1024 * 1024;
const MAX_CONTROLLER_BYTES: u64 = 128 * 1024 * 1024;
const MAX_LAUNCHER_BYTES: u64 = 32 * 1024 * 1024;

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

struct VerifiedBundle {
    manifest: BundleManifest,
    agent_bytes: Vec<u8>,
    controller_bytes: Vec<u8>,
    launcher_bytes: Option<Vec<u8>>,
}

pub async fn deploy_agent(
    github: &GitHubClient,
    executor: &mut HostExecutor,
    run_id: u64,
    artifact_id: u64,
    expected_source_tree: &str,
) -> HostControlResult<Value> {
    let bundle =
        verified_bundle(github, executor, run_id, artifact_id, expected_source_tree).await?;

    let declared_agent_hash = declared_hash(&bundle.manifest, "okx-agent.exe")?;
    let actual_agent_hash = sha256_hex(&bundle.agent_bytes);
    if actual_agent_hash != declared_agent_hash {
        return Err(HostControlError::ArtifactHashMismatch);
    }

    let provenance = InstalledAgentProvenance::verified(
        run_id,
        artifact_id,
        bundle.manifest.source_head_sha.clone(),
        bundle.manifest.source_tree.clone(),
        bundle.manifest.rust_version.clone(),
        actual_agent_hash.clone(),
    );

    executor.install_verified_agent(&bundle.agent_bytes)?;
    InstalledAgentProvenanceStore::canonical().save(&provenance)?;

    Ok(json!({
        "run_id": provenance.run_id,
        "artifact_id": provenance.artifact_id,
        "source_head_sha": provenance.source_head_sha,
        "source_tree": provenance.source_tree,
        "rust_version": provenance.rust_version,
        "agent_sha256": provenance.agent_sha256,
        "installed": true,
        "provenance_persisted": true
    }))
}

pub async fn install_launcher_root(
    github: &GitHubClient,
    executor: &HostExecutor,
    request_id: &str,
    run_id: u64,
    artifact_id: u64,
    expected_source_tree: &str,
) -> HostControlResult<Value> {
    let bundle =
        verified_bundle(github, executor, run_id, artifact_id, expected_source_tree).await?;

    let launcher_bytes =
        bundle
            .launcher_bytes
            .as_deref()
            .ok_or(HostControlError::ArtifactVerification(
                "bundle is missing okx-host-launcher.exe",
            ))?;
    let declared_launcher_hash = declared_hash(&bundle.manifest, "okx-host-launcher.exe")?;
    let actual_launcher_hash = sha256_hex(launcher_bytes);
    if actual_launcher_hash != declared_launcher_hash {
        return Err(HostControlError::ArtifactHashMismatch);
    }

    let declared_controller_hash = declared_hash(&bundle.manifest, "okx-host-control.exe")?;
    let actual_controller_hash = sha256_hex(&bundle.controller_bytes);
    if actual_controller_hash != declared_controller_hash {
        return Err(HostControlError::ArtifactHashMismatch);
    }

    let current_exe = std::env::current_exe()?;
    if okx_host_launcher::sha256_file(&current_exe)? != actual_controller_hash {
        return Err(HostControlError::ArtifactVerification(
            "launcher-root migration requires the running controller to match the accepted bundle",
        ));
    }

    let active = ControllerVersion {
        repository_id: REPOSITORY_ID,
        run_id,
        artifact_id,
        source_head_sha: bundle.manifest.source_head_sha.clone(),
        source_tree: bundle.manifest.source_tree.clone(),
        rust_version: bundle.manifest.rust_version.clone(),
        controller_sha256: actual_controller_hash.clone(),
    };
    let root = okx_host_launcher::install_root(
        launcher_bytes,
        &actual_launcher_hash,
        active,
        &bundle.controller_bytes,
    )?;
    let migration =
        root_migration::prepare(request_id, &actual_controller_hash, &actual_launcher_hash)?;

    Ok(json!({
        "root": root,
        "root_migration": migration,
        "scheduler_changed": false,
        "migration": "ONE_TIME_IMMUTABLE_LAUNCHER_ROOT_POST_EXIT"
    }))
}

pub async fn upgrade_launcher_root(
    github: &GitHubClient,
    executor: &HostExecutor,
    run_id: u64,
    artifact_id: u64,
    expected_source_tree: &str,
    expected_current_launcher_sha256: &str,
) -> HostControlResult<Value> {
    let bundle =
        verified_bundle(github, executor, run_id, artifact_id, expected_source_tree).await?;

    let launcher_bytes =
        bundle
            .launcher_bytes
            .as_deref()
            .ok_or(HostControlError::ArtifactVerification(
                "bundle is missing okx-host-launcher.exe",
            ))?;
    let declared_launcher_hash = declared_hash(&bundle.manifest, "okx-host-launcher.exe")?;
    let actual_launcher_hash = sha256_hex(launcher_bytes);
    if actual_launcher_hash != declared_launcher_hash {
        return Err(HostControlError::ArtifactHashMismatch);
    }

    let root = okx_host_launcher::upgrade_launcher_root(
        launcher_bytes,
        &actual_launcher_hash,
        expected_current_launcher_sha256,
    )?;

    Ok(json!({
        "run_id": run_id,
        "artifact_id": artifact_id,
        "source_head_sha": bundle.manifest.source_head_sha,
        "source_tree": bundle.manifest.source_tree,
        "launcher_sha256": actual_launcher_hash,
        "root": root
    }))
}

pub async fn stage_controller_update(
    github: &GitHubClient,
    executor: &HostExecutor,
    run_id: u64,
    artifact_id: u64,
    expected_source_tree: &str,
) -> HostControlResult<Value> {
    let bundle =
        verified_bundle(github, executor, run_id, artifact_id, expected_source_tree).await?;

    let declared_controller_hash = declared_hash(&bundle.manifest, "okx-host-control.exe")?;
    let actual_controller_hash = sha256_hex(&bundle.controller_bytes);
    if actual_controller_hash != declared_controller_hash {
        return Err(HostControlError::ArtifactHashMismatch);
    }

    controller_update::stage(
        ControllerUpdateCandidate {
            run_id,
            artifact_id,
            source_head_sha: bundle.manifest.source_head_sha,
            source_tree: bundle.manifest.source_tree,
            rust_version: bundle.manifest.rust_version,
            controller_sha256: actual_controller_hash,
        },
        &bundle.controller_bytes,
    )
}

async fn verified_bundle(
    github: &GitHubClient,
    executor: &HostExecutor,
    run_id: u64,
    artifact_id: u64,
    expected_source_tree: &str,
) -> HostControlResult<VerifiedBundle> {
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
    let bundle = parse_bundle(&zip_bytes)?;

    if bundle.manifest.schema != BUNDLE_SCHEMA_V1
        || bundle.manifest.repository_id != REPOSITORY_ID
        || bundle.manifest.source_head_sha != run.head_sha
        || bundle.manifest.source_tree != expected_source_tree
    {
        return Err(HostControlError::ArtifactVerification(
            "bundle manifest identity mismatch",
        ));
    }

    let origin_main_tree = executor.fetch_origin_main_tree()?;
    if origin_main_tree != bundle.manifest.source_tree {
        return Err(HostControlError::ArtifactSourceTreeMismatch);
    }

    Ok(bundle)
}

fn declared_hash<'a>(
    manifest: &'a BundleManifest,
    file: &'static str,
) -> HostControlResult<&'a str> {
    manifest
        .files
        .get(file)
        .map(|value| value.sha256.as_str())
        .ok_or(HostControlError::ArtifactVerification(
            "bundle manifest is missing a required binary",
        ))
}

fn parse_bundle(zip_bytes: &[u8]) -> HostControlResult<VerifiedBundle> {
    let reader = Cursor::new(zip_bytes);
    let mut archive = ZipArchive::new(reader)?;

    let manifest_bytes = read_entry(&mut archive, "manifest.json", MAX_MANIFEST_BYTES)?;
    let manifest: BundleManifest = serde_json::from_slice(&manifest_bytes)?;
    let agent_bytes = read_entry(&mut archive, "okx-agent.exe", MAX_AGENT_BYTES)?;
    let controller_bytes = read_entry(&mut archive, "okx-host-control.exe", MAX_CONTROLLER_BYTES)?;
    let launcher_bytes =
        read_optional_entry(&mut archive, "okx-host-launcher.exe", MAX_LAUNCHER_BYTES)?;

    Ok(VerifiedBundle {
        manifest,
        agent_bytes,
        controller_bytes,
        launcher_bytes,
    })
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

fn read_optional_entry(
    archive: &mut ZipArchive<Cursor<&[u8]>>,
    name: &str,
    max_bytes: u64,
) -> HostControlResult<Option<Vec<u8>>> {
    let mut entry = match archive.by_name(name) {
        Ok(entry) => entry,
        Err(ZipError::FileNotFound) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if entry.size() > max_bytes {
        return Err(HostControlError::ArtifactVerification(
            "bundle entry exceeds size limit",
        ));
    }
    let mut bytes = Vec::with_capacity(entry.size() as usize);
    entry.read_to_end(&mut bytes)?;
    Ok(Some(bytes))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
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

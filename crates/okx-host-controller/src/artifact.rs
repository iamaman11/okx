use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::{Cursor, Read, Write},
    path::Path,
};

use okx_github::{GitHubClient, REPOSITORY_ID};
use okx_host_launcher::ControllerVersion;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use zip::{ZipArchive, result::ZipError};

use crate::{
    HostControlError, HostControlResult,
    controller_update::{self, ControllerUpdateCandidate},
    lifecycle_executor::HostExecutor,
    provenance::{InstalledAgentProvenance, InstalledAgentProvenanceStore},
    root_migration,
};

mod research_component;
pub(crate) use research_component::{VerifiedResearchImage, resolve_verified_installed_research};
pub use research_component::{deploy_research, rollback_research};
use research_component::{parse_bundle_compatible, parse_persisted_manifest};

const BUNDLE_SCHEMA_V1: &str = "okx.windows.bundle/v1";
const MAX_MANIFEST_BYTES: u64 = 64 * 1024;
const MAX_AGENT_BYTES: u64 = 128 * 1024 * 1024;
const MAX_CONTROLLER_BYTES: u64 = 128 * 1024 * 1024;
const MAX_LAUNCHER_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct BundleManifest {
    schema: String,
    repository_id: u64,
    source_head_sha: String,
    source_tree: String,
    rust_version: String,
    files: BTreeMap<String, BundleFile>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
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
    let bundle = verified_bundle(github, run_id, artifact_id, expected_source_tree).await?;

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
    _executor: &HostExecutor,
    request_id: &str,
    run_id: u64,
    artifact_id: u64,
    expected_source_tree: &str,
) -> HostControlResult<Value> {
    let bundle = verified_bundle(github, run_id, artifact_id, expected_source_tree).await?;

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
    // A single accepted ReleaseSet must attest all three binaries before
    // publishing even a local manifest for offline inspection.
    if sha256_hex(&bundle.agent_bytes) != declared_hash(&bundle.manifest, "okx-agent.exe")? {
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
    // A failed conflict must not leave a partially installed Launcher root.
    // This check is read-only; it precedes every install_root filesystem write.
    preflight_verified_manifest(&bundle.manifest)?;
    require_current_entrypoint(&actual_controller_hash)?;
    let root = okx_host_launcher::install_root(
        launcher_bytes,
        &actual_launcher_hash,
        active,
        &bundle.controller_bytes,
    )?;
    // This manifest comes only from verified_bundle's accepted-main GitHub
    // attestation. Exact retry is allowed; a different release cannot
    // silently replace provenance during the one-time launcher installation.
    persist_verified_manifest(&bundle.manifest)?;
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
    _executor: &HostExecutor,
    run_id: u64,
    artifact_id: u64,
    expected_source_tree: &str,
    expected_current_launcher_sha256: &str,
) -> HostControlResult<Value> {
    let bundle = verified_bundle(github, run_id, artifact_id, expected_source_tree).await?;

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
            "launcher-root upgrade requires the running controller from the same accepted bundle",
        ));
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
    _executor: &HostExecutor,
    run_id: u64,
    artifact_id: u64,
    expected_source_tree: &str,
) -> HostControlResult<Value> {
    let bundle = verified_bundle(github, run_id, artifact_id, expected_source_tree).await?;

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
    let bundle = parse_bundle_compatible(&zip_bytes)?;

    if bundle.manifest.schema != BUNDLE_SCHEMA_V1
        || bundle.manifest.repository_id != REPOSITORY_ID
        || bundle.manifest.source_head_sha != run.head_sha
        || bundle.manifest.source_tree != expected_source_tree
    {
        return Err(HostControlError::ArtifactVerification(
            "bundle manifest identity mismatch",
        ));
    }

    // The archive must have fresh, self-origin Native components (enforced by
    // parse_bundle_compatible) and its tested head must be present on main.
    // Later, *isolated* Research edits may advance main without invalidating
    // the still-accepted Native base. All other changes fail closed and need
    // another full Windows/B1 artifact. Never attribute borrowed binaries to
    // the Research composition's own PR run.
    if !github
        .native_base_on_research_only_main(&run.head_sha, &bundle.manifest.source_tree)
        .await?
    {
        return Err(HostControlError::ArtifactSourceTreeMismatch);
    }

    Ok(bundle)
}

fn require_current_entrypoint(expected_controller_sha256: &str) -> HostControlResult<()> {
    // The running executable may be a downloaded new controller while the
    // installed entrypoint still belongs to an older ReleaseSet. Do not start
    // a migration that cannot complete its last immutable replacement step.
    let entrypoint = Path::new(okx_host_launcher::ENTRYPOINT_PATH);
    if !entrypoint.is_file()
        || okx_host_launcher::sha256_file(entrypoint)? != expected_controller_sha256
    {
        return Err(HostControlError::ArtifactVerification(
            "legacy installed CONTROL needs a verified controller handoff before Launcher migration",
        ));
    }
    Ok(())
}

fn preflight_verified_manifest(manifest: &BundleManifest) -> HostControlResult<()> {
    let control = Path::new(okx_host_launcher::CONTROL_ROOT);
    match fs::symlink_metadata(control) {
        Ok(metadata) if !metadata.is_dir() || metadata.file_type().is_symlink() => {
            return Err(HostControlError::ArtifactVerification(
                "accepted release control root is not a plain directory",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
        _ => {}
    }
    let path = control.join("manifest.json");
    match fs::symlink_metadata(&path) {
        Ok(metadata) => {
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(HostControlError::ArtifactVerification(
                    "accepted release manifest is not a plain file",
                ));
            }
            let old = parse_persisted_manifest(&fs::read(path)?)?;
            if old != *manifest {
                return Err(HostControlError::ArtifactVerification(
                    "accepted release manifest conflicts with existing installation",
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn persist_verified_manifest(manifest: &BundleManifest) -> HostControlResult<()> {
    let control = Path::new(okx_host_launcher::CONTROL_ROOT);
    let metadata = fs::symlink_metadata(control)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(HostControlError::ArtifactVerification(
            "accepted release control root is not a plain directory",
        ));
    }
    let path = control.join("manifest.json");
    match fs::symlink_metadata(&path) {
        Ok(metadata) => {
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(HostControlError::ArtifactVerification(
                    "accepted release manifest is not a plain file",
                ));
            }
            let old = parse_persisted_manifest(&fs::read(path)?)?;
            if old != *manifest {
                return Err(HostControlError::ArtifactVerification(
                    "accepted release manifest conflicts with existing installation",
                ));
            }
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let bytes = serde_json::to_vec_pretty(manifest)?;
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
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
    let mut archive = validated_archive(zip_bytes)?;

    let manifest_bytes = read_entry(&mut archive, "manifest.json", MAX_MANIFEST_BYTES)?;
    let manifest: BundleManifest = serde_json::from_slice(&manifest_bytes)?;
    // v1 predates Research as a separately verified component. Reject both
    // names so a legacy image cannot be smuggled through a renamed reader.
    if archive.by_name("okx-research-service.exe").is_ok()
        || archive.by_name("okx-research-worker.exe").is_ok()
    {
        return Err(HostControlError::ArtifactVerification(
            "v1 bundle cannot carry Research worker",
        ));
    }
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

// Reject ambiguous directory/local-header views before selecting any entry.
fn validated_archive(bytes: &[u8]) -> HostControlResult<ZipArchive<Cursor<&[u8]>>> {
    let mut archive = ZipArchive::new(Cursor::new(bytes))?;
    archive.validate_header_consistency()?;
    Ok(archive)
}

fn read_bounded(
    mut entry: impl Read,
    declared_size: u64,
    max_bytes: u64,
) -> HostControlResult<Vec<u8>> {
    if declared_size > max_bytes {
        return Err(HostControlError::ArtifactVerification(
            "bundle entry exceeds size limit",
        ));
    }
    let mut bytes = Vec::with_capacity(declared_size as usize);
    entry.by_ref().take(max_bytes + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        return Err(HostControlError::ArtifactVerification(
            "bundle entry exceeds size limit",
        ));
    }
    if bytes.len() as u64 != declared_size {
        return Err(HostControlError::ArtifactVerification(
            "bundle entry size differs from header",
        ));
    }
    Ok(bytes)
}

fn read_entry(
    archive: &mut ZipArchive<Cursor<&[u8]>>,
    name: &str,
    max_bytes: u64,
) -> HostControlResult<Vec<u8>> {
    let entry = archive.by_name(name)?;
    let size = entry.size();
    read_bounded(entry, size, max_bytes)
}

fn read_optional_entry(
    archive: &mut ZipArchive<Cursor<&[u8]>>,
    name: &str,
    max_bytes: u64,
) -> HostControlResult<Option<Vec<u8>>> {
    let entry = match archive.by_name(name) {
        Ok(entry) => entry,
        Err(ZipError::FileNotFound) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let size = entry.size();
    read_bounded(entry, size, max_bytes).map(Some)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn archive(entries: &[(&str, &[u8])], method: zip::CompressionMethod) -> Vec<u8> {
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default().compression_method(method);
        for (name, bytes) in entries {
            writer.start_file(*name, options).unwrap();
            writer.write_all(bytes).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    #[test]
    fn valid_v1_stored_and_deflated_and_optional_launcher() {
        let manifest = BundleManifest {
            schema: BUNDLE_SCHEMA_V1.into(),
            repository_id: REPOSITORY_ID,
            source_head_sha: "a".repeat(40),
            source_tree: "b".repeat(40),
            rust_version: "rustc test".into(),
            files: BTreeMap::new(),
        };
        let json = serde_json::to_vec(&manifest).unwrap();
        for method in [
            zip::CompressionMethod::Stored,
            zip::CompressionMethod::Deflated,
        ] {
            let bytes = archive(
                &[
                    ("manifest.json", &json),
                    ("okx-agent.exe", b"agent"),
                    ("okx-host-control.exe", b"control"),
                ],
                method,
            );
            let bundle = parse_bundle_compatible(&bytes).unwrap();
            assert_eq!(bundle.agent_bytes, b"agent");
            assert_eq!(bundle.controller_bytes, b"control");
            assert!(bundle.launcher_bytes.is_none());
        }
    }

    #[test]
    fn rejects_inconsistent_duplicate_and_truncated_archive() {
        let original = archive(
            &[("manifest.json", b"one"), ("manifest.jsox", b"two")],
            zip::CompressionMethod::Stored,
        );
        let mut mismatch = original.clone();
        // Change only the first local header name; central name stays intact.
        let local = mismatch
            .windows(13)
            .position(|v| v == b"manifest.json")
            .unwrap();
        mismatch[local + 12] = b'x';
        assert!(validated_archive(&mismatch).is_err());
        let mut duplicate = original.clone();
        // Rename second entry in both headers to create duplicate central names.
        let positions: Vec<_> = duplicate
            .windows(13)
            .enumerate()
            .filter_map(|(i, v)| (v == b"manifest.jsox").then_some(i))
            .collect();
        assert_eq!(positions.len(), 2);
        for pos in positions {
            duplicate[pos + 12] = b'n';
        }
        assert!(validated_archive(&duplicate).is_err());
        assert!(validated_archive(&original[..original.len() - 8]).is_err());
        let mut corrupt = original.clone();
        let payload = corrupt.windows(3).position(|v| v == b"one").unwrap();
        corrupt[payload] ^= 1;
        let mut archive = validated_archive(&corrupt).unwrap();
        assert!(read_entry(&mut archive, "manifest.json", 32).is_err());
    }

    #[test]
    fn accepts_stream_data_descriptor_and_zip64_entry() {
        let mut stream = zip::ZipWriter::new_stream(Vec::new());
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        stream.start_file("entry", options).unwrap();
        stream.write_all(b"streamed").unwrap();
        let bytes = stream.finish().unwrap().into_inner();
        // General purpose bit 3 marks a data descriptor in the local header.
        assert_ne!(u16::from_le_bytes([bytes[6], bytes[7]]) & 8, 0);
        let mut archive = validated_archive(&bytes).unwrap();
        assert_eq!(read_entry(&mut archive, "entry", 64).unwrap(), b"streamed");

        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored)
            .large_file(true);
        writer.start_file("entry", options).unwrap();
        writer.write_all(b"zip64").unwrap();
        let bytes = writer.finish().unwrap().into_inner();
        // The local size fields signal that ZIP64 extra metadata is present.
        assert_eq!(&bytes[18..22], &u32::MAX.to_le_bytes());
        let mut archive = validated_archive(&bytes).unwrap();
        assert_eq!(read_entry(&mut archive, "entry", 64).unwrap(), b"zip64");
    }

    #[test]
    fn rejects_declared_and_actual_size_above_limit() {
        let bytes = archive(&[("entry", b"12345")], zip::CompressionMethod::Deflated);
        let mut archive = validated_archive(&bytes).unwrap();
        assert!(read_entry(&mut archive, "entry", 4).is_err());
        // Independently exercise the streaming bound if a reader yields more
        // than its advertised size, without relying on ZIP parser behavior.
        assert!(read_bounded(Cursor::new(b"12345"), 1, 4).is_err());
        assert!(read_bounded(Cursor::new(b"123"), 1, 4).is_err());
        assert!(read_bounded(Cursor::new(b"123"), 4, 4).is_err());
    }

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

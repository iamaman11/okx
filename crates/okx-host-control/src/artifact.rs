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
    executor::HostExecutor,
    provenance::{InstalledAgentProvenance, InstalledAgentProvenanceStore},
    root_migration,
};

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

    // Keep the accepted-main policy, but resolve immutable source identity
    // through GitHub instead of requiring Git/source on the installed host.
    let tested_tree = github.repository_commit_tree(&run.head_sha).await?;
    let main_tree = github.repository_commit_tree("main").await?;
    if tested_tree != bundle.manifest.source_tree || main_tree != bundle.manifest.source_tree {
        return Err(HostControlError::ArtifactSourceTreeMismatch);
    }

    Ok(bundle)
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
            let old: BundleManifest = serde_json::from_slice(&fs::read(path)?)?;
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
    // Run ONLY on a brand-new GitHub-hosted Windows runner after the real
    // CONTROL bootstrap and exact bundle download. This exercises existing
    // production owners; it is not a GitHub accepted-main attestation and
    // cannot run against a user's installed Windows host.
    #[cfg(windows)]
    #[test]
    #[ignore = "CI-only destructive fixture rooted at pristine C:\\okx"]
    fn ci_native_release_install_uses_real_agent_and_launcher_owners() {
        use std::{path::PathBuf, process::Command};

        assert_eq!(std::env::var("GITHUB_ACTIONS").as_deref(), Ok("true"));
        assert_eq!(std::env::var("CI").as_deref(), Ok("true"));
        let bundle_root =
            PathBuf::from(std::env::var("OKX_CI_BUNDLE_ROOT").expect("explicit CI bundle root"));
        let head = std::env::var("OKX_CI_EXPECTED_HEAD").expect("CI source head");
        let tree = std::env::var("OKX_CI_EXPECTED_TREE").expect("CI source tree");
        let run_id: u64 = std::env::var("GITHUB_RUN_ID")
            .expect("GitHub workflow run")
            .parse()
            .expect("numeric workflow run");
        let artifact_id: u64 = std::env::var("OKX_CI_ARTIFACT_ID")
            .expect("real uploaded bundle artifact ID")
            .parse()
            .expect("numeric artifact ID");
        assert!(run_id > 0 && artifact_id > 0);
        let manifest_bytes = fs::read(bundle_root.join("manifest.json")).expect("manifest");
        let manifest: BundleManifest =
            serde_json::from_slice(&manifest_bytes).expect("typed bundle manifest");
        assert_eq!(manifest.schema, BUNDLE_SCHEMA_V1);
        assert_eq!(manifest.repository_id, REPOSITORY_ID);
        assert_eq!(manifest.source_head_sha, head);
        assert_eq!(manifest.source_tree, tree);

        let agent_bytes = fs::read(bundle_root.join("okx-agent.exe")).expect("Agent");
        let control_bytes = fs::read(bundle_root.join("okx-host-control.exe")).expect("CONTROL");
        let launcher_bytes = fs::read(bundle_root.join("okx-host-launcher.exe")).expect("Launcher");
        for (name, binary) in [
            ("okx-agent.exe", &agent_bytes),
            ("okx-host-control.exe", &control_bytes),
            ("okx-host-launcher.exe", &launcher_bytes),
        ] {
            assert_eq!(
                sha256_hex(binary),
                declared_hash(&manifest, name).expect("declared hash"),
                "{name} must match this exact CI bundle"
            );
        }

        let control_root = Path::new(okx_host_launcher::CONTROL_ROOT);
        let runtime_root = Path::new(okx_host_launcher::RUNTIME_ROOT);
        let entrypoint = control_root.join("okx-host-control.exe");
        assert!(
            control_root.is_dir(),
            "preceding native CONTROL install required"
        );
        assert!(!runtime_root.exists(), "never adopt an unknown runtime");
        assert_eq!(
            okx_host_launcher::sha256_file(&entrypoint).expect("native entrypoint"),
            sha256_hex(&control_bytes)
        );
        let control_entries = fs::read_dir(control_root)
            .expect("control root")
            .map(|entry| entry.expect("entry").file_name())
            .collect::<Vec<_>>();
        assert_eq!(
            control_entries,
            vec![std::ffi::OsString::from("okx-host-control.exe")],
            "only the native CONTROL bootstrap may precede this test"
        );

        let controller_hash = sha256_hex(&control_bytes);
        let launcher_hash = sha256_hex(&launcher_bytes);
        let active = ControllerVersion {
            repository_id: REPOSITORY_ID,
            run_id,
            artifact_id,
            source_head_sha: head.clone(),
            source_tree: tree.clone(),
            rust_version: manifest.rust_version.clone(),
            controller_sha256: controller_hash.clone(),
        };

        let mut corrupt_launcher = launcher_bytes.clone();
        corrupt_launcher[0] ^= 1;
        assert!(
            okx_host_launcher::install_root(
                &corrupt_launcher,
                &launcher_hash,
                active.clone(),
                &control_bytes
            )
            .is_err(),
            "invalid launcher must fail before creating the root"
        );

        let installed = okx_host_launcher::install_root(
            &launcher_bytes,
            &launcher_hash,
            active.clone(),
            &control_bytes,
        )
        .expect("real Launcher root installation");
        assert_eq!(installed["installed"], true);
        let retried = okx_host_launcher::install_root(
            &launcher_bytes,
            &launcher_hash,
            active,
            &control_bytes,
        )
        .expect("exact Launcher root retry");
        assert_eq!(retried["installed"], true);
        let migrated =
            okx_host_launcher::install_launcher_entrypoint(&launcher_hash, &controller_hash)
                .expect("native launcher entrypoint");
        assert_eq!(migrated["installed"], true);
        let entrypoint_retry =
            okx_host_launcher::install_launcher_entrypoint(&launcher_hash, &controller_hash)
                .expect("exact entrypoint retry");
        assert_eq!(entrypoint_retry["disposition"], "EXISTING");
        let launcher_status = okx_host_launcher::status_value();
        assert_eq!(launcher_status["active"]["state"], "READY");
        assert_eq!(
            launcher_status["active"]["value"]["active"]["source_tree"],
            tree
        );
        assert_eq!(launcher_status["entrypoint_is_launcher"], true);

        let mut executor = HostExecutor::canonical().expect("existing native owner");
        executor
            .install_verified_agent(&agent_bytes)
            .expect("real Agent deployment owner");
        let agent_path = runtime_root.join("okx-agent.exe");
        assert_eq!(
            okx_host_launcher::sha256_file(&agent_path).expect("installed Agent hash"),
            sha256_hex(&agent_bytes)
        );
        InstalledAgentProvenanceStore::canonical()
            .save(&InstalledAgentProvenance::verified(
                run_id,
                artifact_id,
                head,
                tree.clone(),
                manifest.rust_version.clone(),
                sha256_hex(&agent_bytes),
            ))
            .expect("persist actual CI identity, NOT accepted-main attestation");
        persist_verified_manifest(&manifest).expect("matching local release manifest");
        persist_verified_manifest(&manifest).expect("exact manifest retry");

        let inspect = || {
            let output = Command::new(bundle_root.join("okx-host-control.exe"))
                .arg("inspect")
                .output()
                .expect("offline CONTROL inspect");
            assert!(output.status.success(), "inspect must stay offline");
            serde_json::from_slice::<Value>(&output.stdout).expect("typed inspection")
        };
        let before = inspect();
        assert_eq!(before["state"], "UNINITIALIZED_LOCAL_IDENTITY");
        assert_eq!(
            before["local_release_integrity"],
            "LOCAL_HASHES_MATCH_NOT_REMOTE_ATTESTED"
        );
        assert_eq!(before["central_risk"], "UNKNOWN");
        assert_eq!(before["trading_permission"], "NEW_RISK_BLOCKED");

        let initialize = || {
            let output = Command::new(&agent_path)
                .args([
                    "--root",
                    okx_host_launcher::RUNTIME_ROOT,
                    "initialize-installation",
                    "--installation-id",
                    "ci-clean-installation-01",
                    "--expected-release",
                    tree.as_str(),
                    "--operation-id",
                    "ci-clean-install-operation-01",
                ])
                .output()
                .expect("native Agent initialization");
            assert!(
                output.status.success(),
                "native Agent refused fixture genesis: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            serde_json::from_slice::<Value>(&output.stdout).expect("native Agent receipt")
        };
        let genesis = initialize();
        assert_eq!(genesis["status"], "completed");
        assert_eq!(genesis["release"], tree);
        let catalog = runtime_root.join("account-catalog.pb");
        let catalog_before = fs::read(&catalog).expect("durable catalog");
        assert_eq!(initialize(), genesis, "genesis exact retry");
        assert_eq!(fs::read(&catalog).unwrap(), catalog_before);
        let after = inspect();
        assert_eq!(after["state"], "IDENTITY_FILE_PRESENT_NOT_VERIFIED");
        assert_eq!(after["central_risk"], "UNKNOWN");
        assert_eq!(after["trading_permission"], "NEW_RISK_BLOCKED");
        assert_eq!(after["safe_start"], "READ_ONLY_INSPECTION_ONLY");

        // Real deploy owner must make an identical release a pure no-op:
        // otherwise a retry can disturb a live binary or discard recovery.
        executor
            .install_verified_agent(&agent_bytes)
            .expect("same Agent is an inert retry");
        let previous = runtime_root.join("okx-agent.exe.previous");
        assert!(!previous.exists(), "exact retry must not create a backup");

        // CI-CRASH pre-commit boundary: power loss after moving current to
        // previous, before staged replacement becomes active. These three
        // files are byte sentinels, not a claim to have verified SQL semantics.
        let sql_root = runtime_root.join("accounts").join("ci-crash-boundary");
        fs::create_dir_all(&sql_root).expect("isolated financial fixture");
        let sentinels = [
            ("account.sqlite", b"ci-sql-original".as_slice()),
            ("account.sqlite-wal", b"ci-wal-original".as_slice()),
            ("account.sqlite-shm", b"ci-shm-original".as_slice()),
        ];
        for (file, data) in sentinels {
            fs::write(sql_root.join(file), data).expect("SQL family sentinel");
        }
        let interrupted_staging = runtime_root.join("okx-agent.exe.new");
        fs::write(&interrupted_staging, &agent_bytes).expect("durable candidate fixture");
        fs::rename(&agent_path, &previous).expect("interrupted move to previous");
        assert!(!agent_path.exists());
        assert_eq!(
            okx_host_launcher::sha256_file(&previous).expect("last-known-good Agent"),
            sha256_hex(&agent_bytes)
        );
        let broken = inspect();
        assert_eq!(broken["state"], "BLOCKED_RELEASE_FILES_MISSING");
        assert_eq!(broken["trading_permission"], "NEW_RISK_BLOCKED");

        executor
            .install_verified_agent(&agent_bytes)
            .expect("recover interrupted native Agent install");
        assert_eq!(
            okx_host_launcher::sha256_file(&agent_path).expect("recovered Agent"),
            sha256_hex(&agent_bytes)
        );
        assert_eq!(
            okx_host_launcher::sha256_file(&previous).expect("preserved backup"),
            sha256_hex(&agent_bytes),
            "recovery must retain previous even when active Agent was missing"
        );
        assert_eq!(
            fs::read(&catalog).expect("catalog retained"),
            catalog_before
        );
        for (file, data) in sentinels {
            assert_eq!(
                fs::read(sql_root.join(file)).expect("SQL family retained"),
                data,
                "{file} changed during Agent recovery"
            );
        }
        let recovered = inspect();
        assert_eq!(recovered["state"], "IDENTITY_FILE_PRESENT_NOT_VERIFIED");
        assert_eq!(recovered["central_risk"], "UNKNOWN");
        assert_eq!(recovered["trading_permission"], "NEW_RISK_BLOCKED");

        println!(
            "CI_NATIVE_RELEASE_INSTALL_V1:{}",
            serde_json::json!({
                "source_tree": tree,
                "agent": "INSTALLED_NATIVE_OWNER",
                "launcher": "INSTALLED_NATIVE_OWNER",
                "controller_entrypoint": "IMMUTABLE_LAUNCHER",
                "exact_retry": "PASS",
                "identity_genesis": "PASS",
                "crash_after_agent_backup_before_rename": "RECOVERED",
                "previous_agent_backup": "PRESERVED",
                "sql_wal_shm_byte_sentinels": "UNCHANGED_NOT_SEMANTIC_SQL_PROOF",
                "risk": "UNKNOWN_BLOCKED",
                "trading": "NOT_STARTED",
                "trust": "CI_ARTIFACT_ONLY_NOT_ACCEPTED_MAIN"
            })
        );
    }
}

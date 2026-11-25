use super::*;
use std::{path::PathBuf, time::Instant};

// A composition has its own accepted source identity. Each component records
// its immutable origin separately, allowing previously accepted bytes to be
// reused without claiming they were built by the composition run.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ComponentManifest {
    sha256: String,
    size: u64,
    source_head_sha: String,
    source_tree: String,
    run_id: u64,
    artifact_id: u64,
    protocol_version: u32,
    storage_version: u32,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BundleManifestV2 {
    schema: String,
    repository_id: u64,
    source_head_sha: String,
    source_tree: String,
    rust_version: String,
    components: BTreeMap<String, ComponentManifest>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct InstalledResearch {
    schema: String,
    repository_id: u64,
    run_id: u64,
    artifact_id: u64,
    source_head_sha: String,
    source_tree: String,
    worker_sha256: String,
    protocol_version: u32,
    storage_version: u32,
}

/// Immutable, freshly checked Research image. The caller must not derive an
/// executable pathname from an unverified JSON pointer.
#[derive(Debug, Clone)]
pub(crate) struct VerifiedResearchImage {
    pub binary: PathBuf,
    pub sha256: String,
}

const COMPONENT_NAMES: [&str; 4] = [
    "okx-agent.exe",
    "okx-host-control.exe",
    "okx-host-launcher.exe",
    "okx-research-service.exe",
];
const MAX_COMPONENT_BYTES: u64 = 128 * 1024 * 1024;
const MAX_RESEARCH_POINTER_BYTES: u64 = 64 * 1024;
const CURRENT_RESEARCH_POINTER: &str = "installed-research.json";
const ROLLBACK_RESEARCH_POINTER: &str = "rollback-research.json";

fn valid_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn component_valid(c: &ComponentManifest) -> bool {
    valid_hex(&c.sha256, 64)
        && valid_hex(&c.source_head_sha, 40)
        && valid_hex(&c.source_tree, 40)
        && c.size > 0
        && c.size <= MAX_COMPONENT_BYTES
        && ((c.run_id == 0 && c.artifact_id == 0) || (c.run_id != 0 && c.artifact_id != 0))
        && c.protocol_version == 1
        && c.storage_version == 1
}

async fn accepted_archive(
    github: &GitHubClient,
    run_id: u64,
    artifact_id: u64,
) -> HostControlResult<(String, Vec<u8>)> {
    let run = github.workflow_run(run_id).await?;
    if run.name != "CI"
        || run.event != "pull_request"
        || run.status != "completed"
        || run.conclusion.as_deref() != Some("success")
    {
        return Err(HostControlError::ArtifactVerification(
            "component origin is not successful PR CI",
        ));
    }
    let artifact = github.workflow_artifact(run_id, artifact_id).await?;
    if artifact.expired || artifact.name != format!("okx-windows-bundle-{}", run.head_sha) {
        return Err(HostControlError::ArtifactVerification(
            "component origin artifact identity mismatch",
        ));
    }
    Ok((
        run.head_sha,
        github.download_artifact_zip(artifact_id).await?,
    ))
}

fn validate_component_bytes(c: &ComponentManifest, bytes: &[u8]) -> HostControlResult<()> {
    if !component_valid(c) || bytes.len() as u64 != c.size || sha256_hex(bytes) != c.sha256 {
        return Err(HostControlError::ArtifactVerification(
            "invalid component identity, compatibility or bytes",
        ));
    }
    Ok(())
}

async fn verify_component_origin(
    github: &GitHubClient,
    name: &str,
    c: &ComponentManifest,
) -> HostControlResult<()> {
    let (head, bytes) = accepted_archive(github, c.run_id, c.artifact_id).await?;
    if head != c.source_head_sha || github.repository_commit_tree(&head).await? != c.source_tree {
        return Err(HostControlError::ArtifactSourceTreeMismatch);
    }
    let mut archive = validated_archive(bytes.as_slice())?;
    let manifest_bytes = read_entry(&mut archive, "manifest.json", MAX_MANIFEST_BYTES)?;
    let value: Value = serde_json::from_slice(&manifest_bytes)?;
    match value.get("schema").and_then(Value::as_str) {
        Some("okx.windows.bundle/v1") if name != "okx-research-service.exe" => {
            let m: BundleManifest = serde_json::from_value(value)?;
            if m.repository_id != REPOSITORY_ID
                || m.source_head_sha != head
                || m.source_tree != c.source_tree
                || declared_hash(
                    &m,
                    match name {
                        "okx-agent.exe" => "okx-agent.exe",
                        "okx-host-control.exe" => "okx-host-control.exe",
                        _ => "okx-host-launcher.exe",
                    },
                )? != c.sha256
            {
                return Err(HostControlError::ArtifactVerification(
                    "legacy origin manifest mismatch",
                ));
            }
        }
        Some("okx.windows.bundle/v2") => {
            let m: BundleManifestV2 = serde_json::from_value(value)?;
            if m.repository_id != REPOSITORY_ID
                || m.source_head_sha != head
                || m.source_tree != c.source_tree
                || !m.components.get(name).is_some_and(|origin| {
                    let mut resolved = origin.clone();
                    if resolved.run_id == 0 && resolved.artifact_id == 0 {
                        resolved.run_id = c.run_id;
                        resolved.artifact_id = c.artifact_id;
                    }
                    resolved == *c
                })
            {
                return Err(HostControlError::ArtifactVerification(
                    "component origin manifest mismatch",
                ));
            }
        }
        _ => {
            return Err(HostControlError::ArtifactVerification(
                "unsupported component origin",
            ));
        }
    }
    let origin_bytes = read_entry(&mut archive, name, MAX_COMPONENT_BYTES)?;
    validate_component_bytes(c, &origin_bytes)
}

pub async fn deploy_research(
    github: &GitHubClient,
    run_id: u64,
    artifact_id: u64,
    expected_source_tree: &str,
) -> HostControlResult<Value> {
    let started = Instant::now();
    let (head, bytes) = accepted_archive(github, run_id, artifact_id).await?;
    let archive_ready = Instant::now();
    let mut archive = validated_archive(bytes.as_slice())?;
    let manifest: BundleManifestV2 = serde_json::from_slice(&read_entry(
        &mut archive,
        "manifest.json",
        MAX_MANIFEST_BYTES,
    )?)?;
    if manifest.schema != "okx.windows.bundle/v2"
        || manifest.repository_id != REPOSITORY_ID
        || manifest.source_head_sha != head
        || manifest.source_tree != expected_source_tree
        || github.repository_commit_tree(&head).await? != manifest.source_tree
        || github.repository_commit_tree("main").await? != manifest.source_tree
        || manifest.components.len() != COMPONENT_NAMES.len()
        || archive.len() != COMPONENT_NAMES.len() + 2
    {
        return Err(HostControlError::ArtifactVerification(
            "composition identity mismatch",
        ));
    }
    let mut worker = None;
    for name in COMPONENT_NAMES {
        let c = manifest
            .components
            .get(name)
            .ok_or(HostControlError::ArtifactVerification(
                "composition component missing",
            ))?;
        let component_bytes = read_entry(&mut archive, name, MAX_COMPONENT_BYTES)?;
        validate_component_bytes(c, &component_bytes)?;
        let mut resolved = c.clone();
        if c.run_id == 0 && c.artifact_id == 0 {
            if c.source_head_sha != head || c.source_tree != manifest.source_tree {
                return Err(HostControlError::ArtifactVerification(
                    "self-origin identity mismatch",
                ));
            }
            resolved.run_id = run_id;
            resolved.artifact_id = artifact_id;
        } else {
            verify_component_origin(github, name, c).await?;
        }
        if name == "okx-research-service.exe" {
            worker = Some((resolved, component_bytes));
        }
    }
    // A Research-only release cannot silently change the accepted financial
    // runtime composition. Bind the reused Agent and Controller to their
    // installed provenance and verify the Launcher bytes as well.
    let resolve_origin = |name: &str| {
        let mut component = manifest.components.get(name).unwrap().clone();
        if component.run_id == 0 && component.artifact_id == 0 {
            component.run_id = run_id;
            component.artifact_id = artifact_id;
        }
        component
    };
    let agent = resolve_origin("okx-agent.exe");
    let installed = InstalledAgentProvenanceStore::canonical().load()?.ok_or(
        HostControlError::ArtifactVerification("installed agent provenance missing"),
    )?;
    if agent.sha256 != installed.agent_sha256
        || agent.run_id != installed.run_id
        || agent.artifact_id != installed.artifact_id
        || agent.source_head_sha != installed.source_head_sha
        || agent.source_tree != installed.source_tree
        || okx_host_launcher::sha256_file(
            &Path::new(okx_host_launcher::RUNTIME_ROOT).join("okx-agent.exe"),
        )? != agent.sha256
    {
        return Err(HostControlError::ArtifactVerification(
            "installed agent composition mismatch",
        ));
    }
    let controller = resolve_origin("okx-host-control.exe");
    let active: okx_host_launcher::ActiveController =
        serde_json::from_slice(&fs::read(okx_host_launcher::ACTIVE_PATH)?)?;
    active.active.validate()?;
    if active.schema != "okx.host-launcher.active/v1"
        || active.repository_id != REPOSITORY_ID
        || active.active.controller_sha256 != controller.sha256
        || active.active.run_id != controller.run_id
        || active.active.artifact_id != controller.artifact_id
        || active.active.source_head_sha != controller.source_head_sha
        || active.active.source_tree != controller.source_tree
        || okx_host_launcher::sha256_file(&std::env::current_exe()?)? != controller.sha256
        || okx_host_launcher::sha256_file(Path::new(okx_host_launcher::LAUNCHER_PATH))?
            != manifest
                .components
                .get("okx-host-launcher.exe")
                .unwrap()
                .sha256
    {
        return Err(HostControlError::ArtifactVerification(
            "installed control composition mismatch",
        ));
    }
    let (c, worker) = worker.ok_or(HostControlError::ArtifactVerification(
        "research component missing",
    ))?;
    let record = InstalledResearch {
        schema: "okx.host-control.installed-research/v1".into(),
        repository_id: REPOSITORY_ID,
        run_id: c.run_id,
        artifact_id: c.artifact_id,
        source_head_sha: c.source_head_sha,
        source_tree: c.source_tree,
        worker_sha256: c.sha256,
        protocol_version: c.protocol_version,
        storage_version: c.storage_version,
    };
    let verified = Instant::now();
    stage_research(Path::new(okx_host_launcher::RUNTIME_ROOT), &record, &worker)?;
    let staged = Instant::now();
    Ok(json!({
        "installed": true,
        "provenance": record,
        "agent_lifecycle_changed": false,
        "activation": "requires_engine_observation",
        "timing_ms": {
            "fetch_accepted_archive": (archive_ready - started).as_millis(),
            "verify_composition_and_origins": (verified - archive_ready).as_millis(),
            "stage_and_publish_pointer": (staged - verified).as_millis(),
            "control_total": (staged - started).as_millis()
        }
    }))
}

/// Restore only the preceding image that CONTROL itself saved while deploying
/// the current one. Do not require the historical composition to equal today's
/// main tree; both on-disk images and the requested transition are hash-fenced.
pub fn rollback_research(
    expected_current_worker_sha256: &str,
    expected_rollback_worker_sha256: &str,
) -> HostControlResult<Value> {
    rollback_research_at(
        Path::new(okx_host_launcher::RUNTIME_ROOT),
        expected_current_worker_sha256,
        expected_rollback_worker_sha256,
    )
}

fn rollback_research_at(
    root: &Path,
    expected_current_worker_sha256: &str,
    expected_rollback_worker_sha256: &str,
) -> HostControlResult<Value> {
    let started = Instant::now();
    validate_existing_ancestors(root)?;
    existing_plain_directory(root)?;
    let current = read_research_pointer(root, CURRENT_RESEARCH_POINTER)?.ok_or(
        HostControlError::ArtifactVerification("current research provenance missing"),
    )?;
    let previous = read_research_pointer(root, ROLLBACK_RESEARCH_POINTER)?.ok_or(
        HostControlError::ArtifactVerification("research rollback image missing"),
    )?;
    if current.worker_sha256 != expected_current_worker_sha256
        || previous.worker_sha256 != expected_rollback_worker_sha256
        || current.worker_sha256 == previous.worker_sha256
    {
        return Err(HostControlError::ArtifactVerification(
            "research rollback identity mismatch",
        ));
    }
    verify_staged_research(root, &current)?;
    verify_staged_research(root, &previous)?;
    let verified = Instant::now();
    // One atomic pointer switch. Preserve the saved rollback image so a
    // repeated or interrupted request cannot accidentally roll forward.
    write_research_pointer(root, CURRENT_RESEARCH_POINTER, &previous)?;
    let restored = Instant::now();
    Ok(json!({
        "rolled_back": true,
        "provenance": previous,
        "agent_lifecycle_changed": false,
        "activation": "requires_engine_observation",
        "timing_ms": {
            "verify_current_and_rollback": (verified - started).as_millis(),
            "publish_pointer": (restored - verified).as_millis(),
            "control_total": (restored - started).as_millis()
        }
    }))
}

fn is_reparse(metadata: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    }
    #[cfg(not(windows))]
    {
        metadata.file_type().is_symlink()
    }
}

fn validate_existing_ancestors(root: &Path) -> HostControlResult<()> {
    for ancestor in root.ancestors().skip(1) {
        let metadata = fs::symlink_metadata(ancestor)?;
        if !metadata.is_dir() || is_reparse(&metadata) {
            return Err(HostControlError::ArtifactVerification(
                "research stage ancestor is not a plain directory",
            ));
        }
    }
    Ok(())
}

fn plain_directory(path: &Path) -> HostControlResult<()> {
    match fs::symlink_metadata(path) {
        Ok(m) if m.is_dir() && !is_reparse(&m) => Ok(()),
        Ok(_) => Err(HostControlError::ArtifactVerification(
            "research stage path is not a plain directory",
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path)?;
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

fn existing_plain_directory(path: &Path) -> HostControlResult<()> {
    let m = fs::symlink_metadata(path)?;
    if !m.is_dir() || is_reparse(&m) {
        return Err(HostControlError::ArtifactVerification(
            "research image path is not a plain directory",
        ));
    }
    Ok(())
}

fn validate_research_record(record: &InstalledResearch) -> HostControlResult<()> {
    if record.schema != "okx.host-control.installed-research/v1"
        || record.repository_id != REPOSITORY_ID
        || record.run_id == 0
        || record.artifact_id == 0
        || !valid_hex(&record.source_head_sha, 40)
        || !valid_hex(&record.source_tree, 40)
        || !valid_hex(&record.worker_sha256, 64)
        || record.protocol_version != 1
        || record.storage_version != 1
    {
        return Err(HostControlError::ArtifactVerification(
            "invalid saved research provenance",
        ));
    }
    Ok(())
}

fn read_research_pointer(root: &Path, name: &str) -> HostControlResult<Option<InstalledResearch>> {
    let path = root.join(name);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(m) if m.is_file() && !is_reparse(&m) => m,
        Ok(_) => {
            return Err(HostControlError::ArtifactVerification(
                "research pointer is not a plain file",
            ));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if metadata.len() == 0 || metadata.len() > MAX_RESEARCH_POINTER_BYTES {
        return Err(HostControlError::ArtifactVerification(
            "research pointer size invalid",
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(0x0020_0000); // FILE_FLAG_OPEN_REPARSE_POINT
    }
    let file = options.open(&path)?;
    let opened = file.metadata()?;
    if !opened.is_file() || is_reparse(&opened) || opened.len() > MAX_RESEARCH_POINTER_BYTES {
        return Err(HostControlError::ArtifactVerification(
            "research pointer changed during read",
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_RESEARCH_POINTER_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_RESEARCH_POINTER_BYTES {
        return Err(HostControlError::ArtifactVerification(
            "research pointer size invalid",
        ));
    }
    let record: InstalledResearch = serde_json::from_slice(&bytes)?;
    validate_research_record(&record)?;
    Ok(Some(record))
}

fn verify_staged_research(root: &Path, record: &InstalledResearch) -> HostControlResult<()> {
    validate_research_record(record)?;
    existing_plain_directory(root)?;
    let research = root.join("research");
    existing_plain_directory(&research)?;
    let versions = research.join("versions");
    existing_plain_directory(&versions)?;
    let version = versions.join(&record.worker_sha256);
    existing_plain_directory(&version)?;
    let service = version.join("okx-research-service.exe");
    let binary = match fs::symlink_metadata(&service) {
        Ok(_) => service,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            version.join("okx-research-worker.exe")
        }
        Err(e) => return Err(e.into()),
    };
    let metadata = fs::symlink_metadata(&binary)?;
    if !metadata.is_file()
        || is_reparse(&metadata)
        || metadata.len() == 0
        || metadata.len() > MAX_COMPONENT_BYTES
        || okx_host_launcher::sha256_file(&binary)? != record.worker_sha256
    {
        return Err(HostControlError::ArtifactHashMismatch);
    }
    Ok(())
}

/// Read the controller-installed pointer with the same size, reparse-point,
/// provenance and binary SHA checks used for deployment/rollback. This is the
/// only capability the independent process supervisor needs from staging.
pub(crate) fn resolve_verified_installed_research(
    root: &Path,
) -> HostControlResult<VerifiedResearchImage> {
    validate_existing_ancestors(root)?;
    existing_plain_directory(root)?;
    let installed = read_research_pointer(root, CURRENT_RESEARCH_POINTER)?.ok_or(
        HostControlError::ArtifactVerification("installed research pointer missing"),
    )?;
    verify_staged_research(root, &installed)?;
    let version = root
        .join("research/versions")
        .join(&installed.worker_sha256);
    let preferred = version.join("okx-research-service.exe");
    let binary = match fs::symlink_metadata(&preferred) {
        Ok(_) => preferred,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            version.join("okx-research-worker.exe")
        }
        Err(error) => return Err(error.into()),
    };
    Ok(VerifiedResearchImage {
        binary,
        sha256: installed.worker_sha256,
    })
}

fn write_research_pointer(
    root: &Path,
    name: &str,
    record: &InstalledResearch,
) -> HostControlResult<()> {
    validate_research_record(record)?;
    let _ = read_research_pointer(root, name)?;
    let pointer = root.join(name);
    let temp = write_stage_file(root, &serde_json::to_vec_pretty(record)?)?;
    let result = crate::provenance::atomic_replace(&temp, &pointer);
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn write_stage_file(directory: &Path, bytes: &[u8]) -> HostControlResult<std::path::PathBuf> {
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).map_err(|e| std::io::Error::other(e.to_string()))?;
    let path = directory.join(format!(
        ".research-stage-{:032x}.tmp",
        u128::from_le_bytes(nonce)
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        file.write_all(bytes)?;
        file.sync_all()
    })();
    if let Err(error) = result {
        let _ = fs::remove_file(&path);
        return Err(error.into());
    }
    Ok(path)
}

fn stage_research(root: &Path, record: &InstalledResearch, bytes: &[u8]) -> HostControlResult<()> {
    validate_research_record(record)?;
    if sha256_hex(bytes) != record.worker_sha256 {
        return Err(HostControlError::ArtifactHashMismatch);
    }
    // Never traverse an existing link or publish a pointer until the complete
    // immutable version has been persisted and checked.
    validate_existing_ancestors(root)?;
    plain_directory(root)?;
    let research = root.join("research");
    plain_directory(&research)?;
    let versions = research.join("versions");
    plain_directory(&versions)?;
    let version = versions.join(&record.worker_sha256);
    plain_directory(&version)?;
    let binary = version.join("okx-research-service.exe");
    match fs::symlink_metadata(&binary) {
        Ok(m) if m.is_file() && !is_reparse(&m) => {
            if sha256_hex(&fs::read(&binary)?) != record.worker_sha256 {
                return Err(HostControlError::ArtifactHashMismatch);
            }
        }
        Ok(_) => {
            return Err(HostControlError::ArtifactVerification(
                "research binary is not a plain file",
            ));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Interrupted writes leave only an unpublished unique staging file.
            let temp = write_stage_file(&version, bytes)?;
            if let Err(error) = fs::rename(&temp, &binary) {
                let _ = fs::remove_file(&temp);
                if !fs::read(&binary).is_ok_and(|value| sha256_hex(&value) == record.worker_sha256)
                {
                    return Err(error.into());
                }
            }
        }
        Err(e) => return Err(e.into()),
    }
    let current = read_research_pointer(root, CURRENT_RESEARCH_POINTER)?;
    let previous = read_research_pointer(root, ROLLBACK_RESEARCH_POINTER)?;
    if let Some(old) = &current {
        verify_staged_research(root, old)?;
        if old == record {
            return Ok(());
        }
        if old.worker_sha256 == record.worker_sha256 {
            return Err(HostControlError::ArtifactVerification(
                "same research bytes with different provenance",
            ));
        }
        write_research_pointer(root, ROLLBACK_RESEARCH_POINTER, old)?;
    } else if previous.is_some() {
        return Err(HostControlError::ArtifactVerification(
            "orphaned research rollback pointer",
        ));
    }
    write_research_pointer(root, CURRENT_RESEARCH_POINTER, record)
}

pub(super) fn parse_persisted_manifest(bytes: &[u8]) -> HostControlResult<BundleManifest> {
    let value: Value = serde_json::from_slice(bytes)?;
    if value.get("schema").and_then(Value::as_str) == Some(BUNDLE_SCHEMA_V1) {
        return Ok(serde_json::from_value(value)?);
    }
    let m: BundleManifestV2 = serde_json::from_value(value)?;
    if m.schema != "okx.windows.bundle/v2" || m.components.len() != 4 {
        return Err(HostControlError::ArtifactVerification(
            "invalid persisted composition",
        ));
    }
    let mut files = BTreeMap::new();
    for name in COMPONENT_NAMES {
        let c = m
            .components
            .get(name)
            .ok_or(HostControlError::ArtifactVerification(
                "missing persisted component",
            ))?;
        if !component_valid(c)
            || c.run_id != 0
            || c.artifact_id != 0
            || c.source_head_sha != m.source_head_sha
            || c.source_tree != m.source_tree
        {
            return Err(HostControlError::ArtifactVerification(
                "invalid persisted component",
            ));
        }
        files.insert(
            name.to_owned(),
            BundleFile {
                sha256: c.sha256.clone(),
            },
        );
    }
    Ok(BundleManifest {
        schema: BUNDLE_SCHEMA_V1.into(),
        repository_id: m.repository_id,
        source_head_sha: m.source_head_sha,
        source_tree: m.source_tree,
        rust_version: m.rust_version,
        files,
    })
}

pub(super) fn parse_bundle_compatible(zip_bytes: &[u8]) -> HostControlResult<VerifiedBundle> {
    let mut archive = validated_archive(zip_bytes)?;
    let raw = read_entry(&mut archive, "manifest.json", MAX_MANIFEST_BYTES)?;
    let value: Value = serde_json::from_slice(&raw)?;
    if value.get("schema").and_then(Value::as_str) == Some(BUNDLE_SCHEMA_V1) {
        return parse_bundle(zip_bytes);
    }
    let m: BundleManifestV2 = serde_json::from_value(value)?;
    if m.schema != "okx.windows.bundle/v2"
        || m.repository_id != REPOSITORY_ID
        || m.rust_version.trim().is_empty()
        || m.components.len() != 4
        || archive.len() != 6
    {
        return Err(HostControlError::ArtifactVerification(
            "invalid v2 release composition",
        ));
    }
    // Legacy deploy operations accept only a complete fresh ReleaseSet. A
    // mixed-origin Research composition is usable solely by DeployResearch.
    let mut files = BTreeMap::new();
    let mut contents = BTreeMap::new();
    for name in COMPONENT_NAMES {
        let c = m
            .components
            .get(name)
            .ok_or(HostControlError::ArtifactVerification("missing component"))?;
        if c.run_id != 0
            || c.artifact_id != 0
            || c.source_head_sha != m.source_head_sha
            || c.source_tree != m.source_tree
        {
            return Err(HostControlError::ArtifactVerification(
                "legacy install requires fresh components",
            ));
        }
        let bytes = read_entry(&mut archive, name, MAX_COMPONENT_BYTES)?;
        validate_component_bytes(c, &bytes)?;
        files.insert(
            name.to_owned(),
            BundleFile {
                sha256: c.sha256.clone(),
            },
        );
        contents.insert(name, bytes);
    }
    let _ = read_entry(&mut archive, "Cargo.lock", 4 * 1024 * 1024)?;
    Ok(VerifiedBundle {
        manifest: BundleManifest {
            schema: BUNDLE_SCHEMA_V1.to_owned(),
            repository_id: m.repository_id,
            source_head_sha: m.source_head_sha,
            source_tree: m.source_tree,
            rust_version: m.rust_version,
            files,
        },
        agent_bytes: contents.remove("okx-agent.exe").unwrap(),
        controller_bytes: contents.remove("okx-host-control.exe").unwrap(),
        launcher_bytes: contents.remove("okx-host-launcher.exe"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(windows)]
    #[test]
    fn research_stage_rejects_junction_ancestor_before_writing() {
        let root = std::env::temp_dir().join(format!(
            "research-junction-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&root).unwrap();
        let target = root.join("target");
        fs::create_dir(&target).unwrap();
        let junction = root.join("junction");
        let status = std::process::Command::new("cmd.exe")
            .args(["/C", "mklink", "/J"])
            .arg(&junction)
            .arg(&target)
            .status()
            .unwrap();
        assert!(status.success(), "create junction fixture");
        let bytes = b"worker";
        let record = InstalledResearch {
            schema: "okx.host-control.installed-research/v1".into(),
            repository_id: REPOSITORY_ID,
            run_id: 1,
            artifact_id: 2,
            source_head_sha: "a".repeat(40),
            source_tree: "b".repeat(40),
            worker_sha256: sha256_hex(bytes),
            protocol_version: 1,
            storage_version: 1,
        };
        assert!(stage_research(&junction.join("runtime"), &record, bytes).is_err());
        assert!(!target.join("runtime").exists());
        fs::remove_dir(&junction).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn research_staging_publishes_only_verified_version_and_preserves_agent_state() {
        let root = std::env::temp_dir().join(format!(
            "research-stage-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&root).unwrap();
        fs::write(root.join("okx-agent.exe"), b"agent").unwrap();
        fs::write(root.join("installed-agent.json"), b"agent provenance").unwrap();
        fs::write(root.join("desired-state"), b"unchanged").unwrap();
        let bytes = b"research worker";
        let record = InstalledResearch {
            schema: "okx.host-control.installed-research/v1".into(),
            repository_id: REPOSITORY_ID,
            run_id: 1,
            artifact_id: 2,
            source_head_sha: "a".repeat(40),
            source_tree: "b".repeat(40),
            worker_sha256: sha256_hex(bytes),
            protocol_version: 1,
            storage_version: 1,
        };
        assert!(stage_research(&root, &record, b"tampered").is_err());
        assert!(!root.join("installed-research.json").exists());
        let version = root.join("research/versions").join(&record.worker_sha256);
        fs::create_dir_all(&version).unwrap();
        // Simulated crash residue is unpublished and cannot block an exact retry.
        fs::write(version.join(".research-stage-interrupted.tmp"), b"partial").unwrap();
        fs::write(
            root.join(format!(".installed-research-{}.tmp", std::process::id())),
            b"partial",
        )
        .unwrap();
        stage_research(&root, &record, bytes).unwrap();
        stage_research(&root, &record, bytes).unwrap();
        assert_eq!(
            fs::read(
                root.join("research/versions")
                    .join(&record.worker_sha256)
                    .join("okx-research-service.exe")
            )
            .unwrap(),
            bytes
        );
        assert_eq!(fs::read(root.join("okx-agent.exe")).unwrap(), b"agent");
        assert_eq!(
            fs::read(root.join("installed-agent.json")).unwrap(),
            b"agent provenance"
        );
        assert_eq!(fs::read(root.join("desired-state")).unwrap(), b"unchanged");
        let pointer: Value =
            serde_json::from_slice(&fs::read(root.join("installed-research.json")).unwrap())
                .unwrap();
        assert_eq!(pointer["worker_sha256"], record.worker_sha256);
        assert!(!root.join(ROLLBACK_RESEARCH_POINTER).exists());

        let mut next = record.clone();
        next.worker_sha256 = sha256_hex(b"research worker v2");
        next.run_id = 3;
        next.artifact_id = 4;
        next.source_head_sha = "c".repeat(40);
        stage_research(&root, &next, b"research worker v2").unwrap();
        assert_eq!(
            read_research_pointer(&root, ROLLBACK_RESEARCH_POINTER).unwrap(),
            Some(record.clone())
        );
        // An exact redeploy cannot erase the preceding safe rollback slot.
        stage_research(&root, &next, b"research worker v2").unwrap();
        assert_eq!(
            read_research_pointer(&root, ROLLBACK_RESEARCH_POINTER).unwrap(),
            Some(record.clone())
        );
        assert!(rollback_research_at(&root, &"0".repeat(64), &record.worker_sha256).is_err());
        assert_eq!(
            read_research_pointer(&root, CURRENT_RESEARCH_POINTER).unwrap(),
            Some(next.clone())
        );
        let old_binary = root
            .join("research/versions")
            .join(&record.worker_sha256)
            .join("okx-research-service.exe");
        fs::write(&old_binary, b"tampered").unwrap();
        assert!(rollback_research_at(&root, &next.worker_sha256, &record.worker_sha256).is_err());
        assert_eq!(
            read_research_pointer(&root, CURRENT_RESEARCH_POINTER).unwrap(),
            Some(next.clone())
        );
        fs::write(old_binary, bytes).unwrap();
        let receipt =
            rollback_research_at(&root, &next.worker_sha256, &record.worker_sha256).unwrap();
        assert_eq!(receipt["agent_lifecycle_changed"], false);
        assert_eq!(receipt["activation"], "requires_engine_observation");
        assert!(receipt["timing_ms"]["control_total"].is_u64());
        assert_eq!(receipt["provenance"]["worker_sha256"], record.worker_sha256);
        assert_eq!(
            read_research_pointer(&root, CURRENT_RESEARCH_POINTER).unwrap(),
            Some(record.clone())
        );
        assert!(rollback_research_at(&root, &next.worker_sha256, &record.worker_sha256).is_err());
        assert_eq!(fs::read(root.join("okx-agent.exe")).unwrap(), b"agent");
        assert_eq!(fs::read(root.join("desired-state")).unwrap(), b"unchanged");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rollback_fails_closed_for_corrupt_or_missing_previous_provenance() {
        let root = std::env::temp_dir().join(format!(
            "research-rollback-negative-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&root).unwrap();
        let original = InstalledResearch {
            schema: "okx.host-control.installed-research/v1".into(),
            repository_id: REPOSITORY_ID,
            run_id: 1,
            artifact_id: 2,
            source_head_sha: "a".repeat(40),
            source_tree: "b".repeat(40),
            worker_sha256: sha256_hex(b"old"),
            protocol_version: 1,
            storage_version: 1,
        };
        let mut next = original.clone();
        next.worker_sha256 = sha256_hex(b"new");
        next.run_id = 3;
        stage_research(&root, &original, b"old").unwrap();
        assert!(rollback_research_at(&root, &next.worker_sha256, &original.worker_sha256).is_err());
        stage_research(&root, &next, b"new").unwrap();
        fs::write(root.join(ROLLBACK_RESEARCH_POINTER), b"{}").unwrap();
        assert!(rollback_research_at(&root, &next.worker_sha256, &original.worker_sha256).is_err());
        assert!(stage_research(&root, &original, b"old").is_err());
        assert_eq!(
            read_research_pointer(&root, CURRENT_RESEARCH_POINTER).unwrap(),
            Some(next)
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn component_validation_rejects_identity_and_compatibility_mismatches() {
        let bytes = b"worker";
        let mut c = ComponentManifest {
            sha256: sha256_hex(bytes),
            size: bytes.len() as u64,
            source_head_sha: "a".repeat(40),
            source_tree: "b".repeat(40),
            run_id: 1,
            artifact_id: 2,
            protocol_version: 1,
            storage_version: 1,
        };
        assert!(validate_component_bytes(&c, bytes).is_ok());
        c.protocol_version = 2;
        assert!(validate_component_bytes(&c, bytes).is_err());
        c.protocol_version = 1;
        c.storage_version = 2;
        assert!(validate_component_bytes(&c, bytes).is_err());
        c.storage_version = 1;
        c.source_tree = "../invalid".into();
        assert!(validate_component_bytes(&c, bytes).is_err());
    }

    #[test]
    fn fresh_v2_bundle_requires_all_component_bytes_and_compatibility() {
        fn bundle(corrupt: bool, method: zip::CompressionMethod) -> Vec<u8> {
            let mut components = BTreeMap::new();
            for name in COMPONENT_NAMES {
                let bytes = name.as_bytes();
                components.insert(
                    name.to_owned(),
                    ComponentManifest {
                        sha256: sha256_hex(bytes),
                        size: bytes.len() as u64,
                        source_head_sha: "a".repeat(40),
                        source_tree: "b".repeat(40),
                        run_id: 0,
                        artifact_id: 0,
                        protocol_version: 1,
                        storage_version: 1,
                    },
                );
            }
            let manifest = BundleManifestV2 {
                schema: "okx.windows.bundle/v2".into(),
                repository_id: REPOSITORY_ID,
                source_head_sha: "a".repeat(40),
                source_tree: "b".repeat(40),
                rust_version: "rustc test".into(),
                components,
            };
            let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
            let options = zip::write::SimpleFileOptions::default().compression_method(method);
            zip.start_file("manifest.json", options).unwrap();
            zip.write_all(&serde_json::to_vec(&manifest).unwrap())
                .unwrap();
            zip.start_file("Cargo.lock", options).unwrap();
            zip.write_all(b"lock").unwrap();
            for name in COMPONENT_NAMES {
                zip.start_file(name, options).unwrap();
                zip.write_all(if corrupt && name == "okx-research-service.exe" {
                    b"bad"
                } else {
                    name.as_bytes()
                })
                .unwrap();
            }
            zip.finish().unwrap().into_inner()
        }
        for method in [
            zip::CompressionMethod::Stored,
            zip::CompressionMethod::Deflated,
        ] {
            assert!(parse_bundle_compatible(&bundle(false, method)).is_ok());
            assert!(parse_bundle_compatible(&bundle(true, method)).is_err());
        }
    }

    #[test]
    fn legacy_bundle_cannot_carry_research_worker() {
        let manifest = BundleManifest {
            schema: BUNDLE_SCHEMA_V1.into(),
            repository_id: REPOSITORY_ID,
            source_head_sha: "a".repeat(40),
            source_tree: "b".repeat(40),
            rust_version: "rustc test".into(),
            files: BTreeMap::new(),
        };
        for name in ["okx-research-service.exe", "okx-research-worker.exe"] {
            let cursor = Cursor::new(Vec::new());
            let mut zip = zip::ZipWriter::new(cursor);
            let options = zip::write::SimpleFileOptions::default();
            zip.start_file("manifest.json", options).unwrap();
            zip.write_all(&serde_json::to_vec(&manifest).unwrap())
                .unwrap();
            zip.start_file(name, options).unwrap();
            zip.write_all(b"worker").unwrap();
            let bytes = zip.finish().unwrap().into_inner();
            assert!(matches!(
                parse_bundle(&bytes),
                Err(HostControlError::ArtifactVerification(_))
            ));
        }
    }
}

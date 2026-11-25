use super::*;
use crate::research_supervisor::ResearchSupervisor;
use okx_github::{WorkflowArtifact, WorkflowJob, WorkflowRun};
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

/// Independent Research release. No Engine, SQL or Launcher payload may be
/// present in this archive or referenced by its manifest.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ResearchComponentManifestV1 {
    schema: String,
    repository_id: u64,
    source_head_sha: String,
    source_tree: String,
    rust_version: String,
    cargo_lock_sha256: String,
    worker: ResearchComponentWorkerV1,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ResearchComponentWorkerV1 {
    sha256: String,
    size: u64,
    protocol_version: u32,
    storage_version: u32,
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

#[derive(Debug, Clone)]
struct StagedResearchTransition {
    target: InstalledResearch,
    current: Option<InstalledResearch>,
    rollback: Option<InstalledResearch>,
}

impl StagedResearchTransition {
    fn previous_sha(&self) -> Option<&str> {
        self.current
            .as_ref()
            .map(|record| record.worker_sha256.as_str())
    }
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
const MAX_RESEARCH_LOCK_BYTES: u64 = 4 * 1024 * 1024;
const CURRENT_RESEARCH_POINTER: &str = "installed-research.json";
const ROLLBACK_RESEARCH_POINTER: &str = "rollback-research.json";
const RESEARCH_COMPONENT_SCHEMA_V1: &str = "okx.windows.research-component/v1";

/// The parent CONTROL singleton is scoped to the interactive Windows session.
/// Independent sessions therefore need an installation-scoped kernel guard
/// over the entire pointer transition (including failed-activation rollback).
/// The name contains a path fingerprint, never the installation path itself.
#[cfg(windows)]
struct ResearchPointerGuard {
    handle: windows_sys::Win32::Foundation::HANDLE,
    // Win32 mutex ownership belongs to the acquiring *thread*, not merely
    // the HANDLE or task. Reject accidental transfer to another thread.
    _not_send: std::marker::PhantomData<std::rc::Rc<()>>,
}

#[cfg(windows)]
impl ResearchPointerGuard {
    fn acquire(root: &Path) -> HostControlResult<Self> {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::{
            Foundation::{CloseHandle, WAIT_ABANDONED, WAIT_OBJECT_0},
            System::Threading::{CreateMutexW, ReleaseMutex, WaitForSingleObject},
        };

        let fingerprint = sha256_hex(root.to_string_lossy().to_lowercase().as_bytes());
        let name = format!(
            r"Global\iamaman11-okx-research-pointer-{}",
            &fingerprint[..32]
        );
        let wide: Vec<u16> = std::ffi::OsStr::new(&name)
            .encode_wide()
            .chain(Some(0))
            .collect();
        // The current Windows token's default DACL owns the mutex. Reusing a
        // cross-session object requires the same rights; access denied, held
        // and abandoned states all fail closed without touching the pointer.
        let handle = unsafe { CreateMutexW(std::ptr::null(), 0, wide.as_ptr()) };
        if handle.is_null() {
            return Err(HostControlError::ResearchPointerLockUnavailable);
        }
        let wait = unsafe { WaitForSingleObject(handle, 0) };
        if wait != WAIT_OBJECT_0 {
            if wait == WAIT_ABANDONED {
                unsafe { ReleaseMutex(handle) };
            }
            unsafe { CloseHandle(handle) };
            return Err(HostControlError::ResearchPointerLockUnavailable);
        }
        Ok(Self {
            handle,
            _not_send: std::marker::PhantomData,
        })
    }
}

#[cfg(windows)]
impl Drop for ResearchPointerGuard {
    fn drop(&mut self) {
        use windows_sys::Win32::{Foundation::CloseHandle, System::Threading::ReleaseMutex};
        unsafe {
            ReleaseMutex(self.handle);
            CloseHandle(self.handle);
        }
    }
}

#[cfg(not(windows))]
struct ResearchPointerGuard;

#[cfg(not(windows))]
impl ResearchPointerGuard {
    fn acquire(_root: &Path) -> HostControlResult<Self> {
        // Production Research supervision is Windows-only; this fallback only
        // allows cross-platform unit tests to exercise pointer compensation.
        Ok(Self)
    }
}

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

fn validate_component_bytes(c: &ComponentManifest, bytes: &[u8]) -> HostControlResult<()> {
    if !component_valid(c) || bytes.len() as u64 != c.size || sha256_hex(bytes) != c.sha256 {
        return Err(HostControlError::ArtifactVerification(
            "invalid component identity, compatibility or bytes",
        ));
    }
    Ok(())
}

/// An upload-artifact archive has exactly these three root files. In particular,
/// it is not a bundle and cannot borrow an Engine, CONTROL, or Launcher image.
fn parse_research_component_archive(
    bytes: &[u8],
    expected_head: &str,
    expected_tree: &str,
    run_id: u64,
    artifact_id: u64,
) -> HostControlResult<(InstalledResearch, Vec<u8>)> {
    if !valid_hex(expected_head, 40)
        || !valid_hex(expected_tree, 40)
        || run_id == 0
        || artifact_id == 0
    {
        return Err(HostControlError::ArtifactVerification(
            "invalid standalone research release identity",
        ));
    }
    let mut archive = validated_archive(bytes)?;
    if archive.len() != 3 {
        return Err(HostControlError::ArtifactVerification(
            "research release must contain exactly three files",
        ));
    }
    let mut names = std::collections::BTreeSet::new();
    for index in 0..archive.len() {
        let entry = archive.by_index(index)?;
        let name = entry.name()?;
        let name = name.as_ref();
        if !matches!(
            name,
            "manifest.json" | "Cargo.lock" | "okx-research-service.exe"
        ) || entry.is_dir()
            || entry.unix_mode().is_some_and(|mode| {
                let file_type = mode & 0o170000;
                file_type != 0 && file_type != 0o100000
            })
            || !names.insert(name.to_owned())
        {
            return Err(HostControlError::ArtifactVerification(
                "research release has an unexpected, duplicate or linked entry",
            ));
        }
    }
    if names.len() != 3 {
        return Err(HostControlError::ArtifactVerification(
            "research release is incomplete",
        ));
    }

    let manifest: ResearchComponentManifestV1 = serde_json::from_slice(&read_entry(
        &mut archive,
        "manifest.json",
        MAX_MANIFEST_BYTES,
    )?)?;
    if manifest.schema != RESEARCH_COMPONENT_SCHEMA_V1
        || manifest.repository_id != REPOSITORY_ID
        || manifest.source_head_sha != expected_head
        || manifest.source_tree != expected_tree
        || manifest.rust_version.trim().is_empty()
        || manifest.rust_version.len() > 128
        || !valid_hex(&manifest.cargo_lock_sha256, 64)
        || !valid_hex(&manifest.worker.sha256, 64)
        || manifest.worker.size == 0
        || manifest.worker.size > MAX_COMPONENT_BYTES
        || manifest.worker.protocol_version != 1
        || manifest.worker.storage_version != 1
    {
        return Err(HostControlError::ArtifactVerification(
            "standalone research manifest identity or compatibility mismatch",
        ));
    }
    let lock = read_entry(&mut archive, "Cargo.lock", MAX_RESEARCH_LOCK_BYTES)?;
    if lock.is_empty() || sha256_hex(&lock) != manifest.cargo_lock_sha256 {
        return Err(HostControlError::ArtifactHashMismatch);
    }
    let worker = read_entry(
        &mut archive,
        "okx-research-service.exe",
        MAX_COMPONENT_BYTES,
    )?;
    if worker.len() < 1024
        || !worker.starts_with(b"MZ")
        || worker.len() as u64 != manifest.worker.size
        || sha256_hex(&worker) != manifest.worker.sha256
    {
        return Err(HostControlError::ArtifactHashMismatch);
    }
    let record = InstalledResearch {
        schema: "okx.host-control.installed-research/v1".to_owned(),
        repository_id: REPOSITORY_ID,
        run_id,
        artifact_id,
        source_head_sha: manifest.source_head_sha,
        source_tree: manifest.source_tree,
        worker_sha256: manifest.worker.sha256,
        protocol_version: manifest.worker.protocol_version,
        storage_version: manifest.worker.storage_version,
    };
    validate_research_record(&record)?;
    Ok((record, worker))
}

/// This is a CONTROL trust anchor only: it examines its own active record and
/// executable, never Engine files or the Launcher payload in another release.
fn verify_active_control_identity_at(
    active_path: &Path,
    running_exe: &Path,
) -> HostControlResult<()> {
    let metadata = fs::symlink_metadata(active_path)?;
    if !metadata.is_file()
        || is_reparse(&metadata)
        || metadata.len() == 0
        || metadata.len() > MAX_RESEARCH_POINTER_BYTES
    {
        return Err(HostControlError::ArtifactVerification(
            "active controller provenance is not a plain bounded file",
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(0x0020_0000); // FILE_FLAG_OPEN_REPARSE_POINT
    }
    let file = options.open(active_path)?;
    let opened = file.metadata()?;
    if !opened.is_file()
        || is_reparse(&opened)
        || opened.len() == 0
        || opened.len() > MAX_RESEARCH_POINTER_BYTES
    {
        return Err(HostControlError::ArtifactVerification(
            "active controller provenance changed during read",
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_RESEARCH_POINTER_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_RESEARCH_POINTER_BYTES {
        return Err(HostControlError::ArtifactVerification(
            "active controller provenance exceeds size limit",
        ));
    }
    let active: okx_host_launcher::ActiveController = serde_json::from_slice(&bytes)?;
    active.active.validate()?;
    if active.schema != "okx.host-launcher.active/v1"
        || active.repository_id != REPOSITORY_ID
        || okx_host_launcher::sha256_file(running_exe)? != active.active.controller_sha256
    {
        return Err(HostControlError::ArtifactVerification(
            "active controller executable identity mismatch",
        ));
    }
    Ok(())
}

fn verify_active_control_identity() -> HostControlResult<()> {
    verify_active_control_identity_at(
        Path::new(okx_host_launcher::ACTIVE_PATH),
        &std::env::current_exe()?,
    )
}

fn is_canonical_ci_workflow_path(path: &str) -> bool {
    // Live workflow-run REST data uses the plain path; documented examples
    // may include an @ref suffix. Bind the exact canonical filename in either
    // representation without accepting a broader workflow-path prefix.
    if path == ".github/workflows/ci.yml" {
        return true;
    }
    let Some(reference) = path.strip_prefix(".github/workflows/ci.yml@") else {
        return false;
    };
    !reference.is_empty()
        && reference.len() <= 256
        && !reference.contains("..")
        && !reference.contains("//")
        && reference
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_/.".contains(&byte))
}

/// A green workflow by itself can be a Draft run whose required Ready checks
/// never executed. Bind the accepted component to *this attempt's* completed
/// required jobs, not another attempt or a similarly named Draft artifact.
fn validate_research_release_jobs(
    run: &WorkflowRun,
    artifact: &WorkflowArtifact,
    jobs: &[WorkflowJob],
) -> HostControlResult<()> {
    const READY_JOBS: [&str; 7] = [
        "classify",
        "architecture-contracts",
        "dependency-audit",
        "cloudflare-mcp-contracts",
        "research-service-tests-windows",
        "linux-core",
        "windows-native",
    ];
    if run.name != "CI"
        || !is_canonical_ci_workflow_path(&run.path)
        || run.event != "pull_request"
        || run.status != "completed"
        || run.conclusion.as_deref() != Some("success")
        // GitHub's artifact API does not attest the producing attempt. A
        // successful rerun could otherwise reuse an artifact from an earlier
        // failed attempt of the same run. Only first attempts are accepted.
        || run.run_attempt != 1
        || !valid_hex(&run.head_sha, 40)
        || artifact.expired
        || artifact.name != format!("okx-research-component-{}", run.head_sha)
        || artifact
            .digest
            .as_deref()
            .is_none_or(|digest| !digest.starts_with("sha256:") || !valid_hex(&digest[7..], 64))
        || artifact.workflow_run.as_ref().is_none_or(|source| {
            source.id != run.id
                || source.repository_id != REPOSITORY_ID
                || source.head_repository_id != REPOSITORY_ID
                || source.head_sha != run.head_sha
        })
    {
        return Err(HostControlError::ArtifactVerification(
            "research release provenance is not an accepted Ready PR CI artifact",
        ));
    }
    // The API method rejects duplicate/truncated lists. The local verification
    // also detects duplicate names to keep direct mock/fixture calls fail-closed.
    let mut seen = std::collections::BTreeSet::new();
    for job in jobs {
        if !seen.insert(job.name.as_str()) || job.run_id != run.id || job.head_sha != run.head_sha {
            return Err(HostControlError::ArtifactVerification(
                "research release job identity mismatch",
            ));
        }
    }
    for name in READY_JOBS {
        if !jobs.iter().any(|job| {
            job.name == name
                && job.status == "completed"
                && job.conclusion.as_deref() == Some("success")
        }) {
            return Err(HostControlError::ArtifactVerification(
                "research release missing successful Ready CI gate",
            ));
        }
    }
    // All Native Engine jobs are deliberately skipped for a Research-only
    // release. The separate Research owner job alone supplies this image.
    for name in [
        "trading-engine-acceptance-windows",
        "trading-engine-build-windows",
        "host-controller-launcher-checks",
    ] {
        if jobs
            .iter()
            .any(|job| job.name == name && job.conclusion.as_deref() != Some("skipped"))
        {
            return Err(HostControlError::ArtifactVerification(
                "research-only release unexpectedly executed Native Engine CI",
            ));
        }
    }
    Ok(())
}

fn verify_research_zip_digest(artifact: &WorkflowArtifact, bytes: &[u8]) -> HostControlResult<()> {
    if artifact.digest.as_deref() != Some(format!("sha256:{}", sha256_hex(bytes)).as_str()) {
        return Err(HostControlError::ArtifactHashMismatch);
    }
    Ok(())
}

pub async fn deploy_research(
    github: &GitHubClient,
    supervisor: &mut ResearchSupervisor,
    run_id: u64,
    artifact_id: u64,
    expected_source_tree: &str,
) -> HostControlResult<Value> {
    let started = Instant::now();
    let run = github.workflow_run(run_id).await?;
    let artifact = github.workflow_artifact(run_id, artifact_id).await?;
    if run.name != "CI"
        || run.event != "pull_request"
        || run.status != "completed"
        || run.conclusion.as_deref() != Some("success")
        || artifact.expired
        || !valid_hex(&run.head_sha, 40)
    {
        return Err(HostControlError::ArtifactVerification(
            "research component is not from accepted PR CI",
        ));
    }
    // DeployResearch has exactly one accepted release format. Historical v2
    // bundles can still install native components via their own commands, but
    // can never bypass Ready checks or inspect Engine through Research deploy.
    if artifact.name != format!("okx-research-component-{}", run.head_sha) {
        return Err(HostControlError::ArtifactVerification(
            "standalone research artifact name does not bind to PR head",
        ));
    }
    // Only this exact attempt's Ready jobs can authorize a Research-only
    // installation. A Draft CI conclusion is insufficient.
    let jobs = github
        .workflow_run_attempt_jobs(run_id, run.run_attempt)
        .await?;
    validate_research_release_jobs(&run, &artifact, &jobs)?;
    if !valid_hex(expected_source_tree, 40)
        || github.repository_commit_tree(&run.head_sha).await? != expected_source_tree
        || github.repository_commit_tree("main").await? != expected_source_tree
    {
        return Err(HostControlError::ArtifactSourceTreeMismatch);
    }
    let zip_bytes = github.download_artifact_zip(artifact_id).await?;
    verify_research_zip_digest(&artifact, &zip_bytes)?;
    let archive_ready = Instant::now();
    let (record, worker) = parse_research_component_archive(
        &zip_bytes,
        &run.head_sha,
        expected_source_tree,
        run_id,
        artifact_id,
    )?;
    verify_active_control_identity()?;
    let verified = Instant::now();
    let (status, staged) = activate_verified_research_image(supervisor, &record, &worker)?;
    Ok(json!({
        "installed": true,
        "provenance": record,
        "agent_lifecycle_changed": false,
        "engine_installation": "not_observed",
        "activation": "requires_research_edge_attestation",
        "research_process": status,
        "timing_ms": {
            "fetch_accepted_archive": (archive_ready - started).as_millis(),
            "verify_research_component": (verified - archive_ready).as_millis(),
            "stage_and_publish_pointer": (staged - verified).as_millis(),
            "control_total": (staged - started).as_millis()
        }
    }))
}

/// Important: the Win32 mutex is thread-affine. Keep this complete critical
/// section inside a synchronous function, including all compensating writes;
/// no Tokio `.await` may be introduced while the guard exists. The async
/// caller finishes all GitHub verification before entering this function.
fn activate_verified_research_image(
    supervisor: &mut ResearchSupervisor,
    record: &InstalledResearch,
    worker: &[u8],
) -> HostControlResult<(Value, Instant)> {
    let root = Path::new(okx_host_launcher::RUNTIME_ROOT);
    let _pointer_guard = ResearchPointerGuard::acquire(root)?;
    let staged = prepare_research_transition(root, record, worker)?;
    let expected_generation = supervisor.process_generation();
    let status = supervisor.transition_image(
        staged.previous_sha(),
        expected_generation,
        &record.worker_sha256,
        || commit_research_transition(root, &staged),
        || restore_research_transition(root, &staged),
    )?;
    Ok((status, Instant::now()))
}

/// Restore only the preceding image that CONTROL itself saved while deploying
/// the current one. Do not require the historical composition to equal today's
/// main tree; both on-disk images and the requested transition are hash-fenced.
pub fn rollback_research(
    supervisor: &mut ResearchSupervisor,
    expected_current_worker_sha256: &str,
    expected_rollback_worker_sha256: &str,
) -> HostControlResult<Value> {
    rollback_research_with_supervisor(
        Path::new(okx_host_launcher::RUNTIME_ROOT),
        supervisor,
        expected_current_worker_sha256,
        expected_rollback_worker_sha256,
    )
}

fn rollback_research_with_supervisor(
    root: &Path,
    supervisor: &mut ResearchSupervisor,
    expected_current_worker_sha256: &str,
    expected_rollback_worker_sha256: &str,
) -> HostControlResult<Value> {
    let started = Instant::now();
    let _pointer_guard = ResearchPointerGuard::acquire(root)?;
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
        return Err(HostControlError::ResearchIdentityMismatch);
    }
    verify_staged_research(root, &current)?;
    verify_staged_research(root, &previous)?;
    let verified = Instant::now();
    let expected_generation = supervisor.process_generation();
    let status = supervisor.transition_image(
        Some(&current.worker_sha256),
        expected_generation,
        &previous.worker_sha256,
        || {
            if read_research_pointer(root, CURRENT_RESEARCH_POINTER)?.as_ref() != Some(&current) {
                return Err(HostControlError::ResearchIdentityMismatch);
            }
            write_research_pointer(root, CURRENT_RESEARCH_POINTER, &previous)
        },
        || restore_rollback_pointer_if_expected(root, &previous, &current),
    )?;
    let restored = Instant::now();
    Ok(json!({
        "rolled_back": true,
        "provenance": previous,
        "agent_lifecycle_changed": false,
        "activation": "requires_research_edge_attestation",
        "research_process": status,
        "timing_ms": {
            "verify_current_and_rollback": (verified - started).as_millis(),
            "publish_pointer_and_supervise": (restored - verified).as_millis(),
            "control_total": (restored - started).as_millis()
        }
    }))
}

/// A failed process activation may only undo its *own* pointer publication.
/// Another CONTROL session may have advanced the installed image meanwhile;
/// an unqualified write here would silently roll that third image back.
fn restore_rollback_pointer_if_expected(
    root: &Path,
    attempted: &InstalledResearch,
    original: &InstalledResearch,
) -> HostControlResult<()> {
    match read_research_pointer(root, CURRENT_RESEARCH_POINTER)? {
        Some(observed) if observed == *attempted => {
            write_research_pointer(root, CURRENT_RESEARCH_POINTER, original)
        }
        Some(observed) if observed == *original => Ok(()),
        _ => Err(HostControlError::ResearchRollbackUnconfirmed),
    }
}

#[cfg(test)]
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

fn prepare_research_transition(
    root: &Path,
    record: &InstalledResearch,
    bytes: &[u8],
) -> HostControlResult<StagedResearchTransition> {
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
    let rollback = read_research_pointer(root, ROLLBACK_RESEARCH_POINTER)?;
    if let Some(old) = &current {
        verify_staged_research(root, old)?;
        if old.worker_sha256 == record.worker_sha256 && old != record {
            return Err(HostControlError::ArtifactVerification(
                "same research bytes with different provenance",
            ));
        }
    } else if rollback.is_some() {
        return Err(HostControlError::ArtifactVerification(
            "orphaned research rollback pointer",
        ));
    }
    if let Some(saved) = &rollback {
        verify_staged_research(root, saved)?;
    }
    Ok(StagedResearchTransition {
        target: record.clone(),
        current,
        rollback,
    })
}

fn commit_research_transition(
    root: &Path,
    staged: &StagedResearchTransition,
) -> HostControlResult<()> {
    if read_research_pointer(root, CURRENT_RESEARCH_POINTER)? != staged.current
        || read_research_pointer(root, ROLLBACK_RESEARCH_POINTER)? != staged.rollback
    {
        return Err(HostControlError::ResearchIdentityMismatch);
    }
    verify_staged_research(root, &staged.target)?;
    if staged.current.as_ref() == Some(&staged.target) {
        return Ok(());
    }
    if let Some(old) = &staged.current {
        write_research_pointer(root, ROLLBACK_RESEARCH_POINTER, old)?;
    }
    write_research_pointer(root, CURRENT_RESEARCH_POINTER, &staged.target)
}

/// Compensate a failed activation only while the pointer still matches the
/// exact snapshot or the expected candidate. Never overwrite a third image.
fn restore_research_transition(
    root: &Path,
    staged: &StagedResearchTransition,
) -> HostControlResult<()> {
    let current = read_research_pointer(root, CURRENT_RESEARCH_POINTER)?;
    if current.as_ref() != Some(&staged.target) && current != staged.current {
        return Err(HostControlError::ResearchRollbackUnconfirmed);
    }
    let rollback = read_research_pointer(root, ROLLBACK_RESEARCH_POINTER)?;
    if rollback != staged.rollback && rollback != staged.current {
        return Err(HostControlError::ResearchRollbackUnconfirmed);
    }
    match &staged.current {
        Some(old) => write_research_pointer(root, CURRENT_RESEARCH_POINTER, old)?,
        None if current.is_some() => fs::remove_file(root.join(CURRENT_RESEARCH_POINTER))?,
        None => {}
    }
    match &staged.rollback {
        Some(old) => write_research_pointer(root, ROLLBACK_RESEARCH_POINTER, old)?,
        None if rollback.is_some() => fs::remove_file(root.join(ROLLBACK_RESEARCH_POINTER))?,
        None => {}
    }
    Ok(())
}

// Existing test fixtures exercise the original staged-pointer contract without
// running a Host. Production deployment uses the same two phases under the
// independent Research process supervisor.
#[cfg(test)]
fn stage_research(root: &Path, record: &InstalledResearch, bytes: &[u8]) -> HostControlResult<()> {
    let staged = prepare_research_transition(root, record, bytes)?;
    commit_research_transition(root, &staged)
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
    // Native Agent/Launcher installation still recognizes a complete v2
    // release with all fresh, self-origin components. Mixed-origin v2 archives
    // are never accepted by DeployResearch (standalone v1 only).
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

    use crate::research_supervisor::ResearchSupervisor;

    fn fixture_pe_image() -> Vec<u8> {
        let mut bytes = vec![0u8; 1024];
        bytes[..2].copy_from_slice(b"MZ");
        bytes
    }

    fn standalone_fixture_zip(
        mut manifest: Value,
        extra: &[(&str, &[u8])],
        omit: Option<&str>,
    ) -> Vec<u8> {
        if manifest.is_null() {
            manifest = json!({
                "schema": RESEARCH_COMPONENT_SCHEMA_V1,
                "repository_id": REPOSITORY_ID,
                "source_head_sha": "a".repeat(40),
                "source_tree": "b".repeat(40),
                "rust_version": "rustc 1.100.0",
                "cargo_lock_sha256": sha256_hex(b"lock bytes"),
                "worker": {
                    "sha256": sha256_hex(&fixture_pe_image()),
                    "size": fixture_pe_image().len(),
                    "protocol_version": 1,
                    "storage_version": 1
                }
            });
        }
        let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default();
        for (name, bytes) in [
            ("manifest.json", serde_json::to_vec(&manifest).unwrap()),
            ("Cargo.lock", b"lock bytes".to_vec()),
            ("okx-research-service.exe", fixture_pe_image()),
        ] {
            if omit != Some(name) {
                writer.start_file(name, options).unwrap();
                writer.write_all(&bytes).unwrap();
            }
        }
        for (name, bytes) in extra {
            writer.start_file(*name, options).unwrap();
            writer.write_all(bytes).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    fn standalone_fixture() -> (Value, Vec<u8>) {
        let bytes = standalone_fixture_zip(Value::Null, &[], None);
        let mut archive = validated_archive(&bytes).unwrap();
        let manifest: Value = serde_json::from_slice(
            &read_entry(&mut archive, "manifest.json", MAX_MANIFEST_BYTES).unwrap(),
        )
        .unwrap();
        (manifest, bytes)
    }

    fn parse_standalone(bytes: &[u8]) -> HostControlResult<(InstalledResearch, Vec<u8>)> {
        parse_research_component_archive(bytes, &"a".repeat(40), &"b".repeat(40), 11, 12)
    }

    fn duplicate_named_entry_zip(manifest: Value) -> Vec<u8> {
        // The current ZipWriter refuses duplicate names at creation. Emulate
        // a hostile raw ZIP by replacing a same-length filename in both the
        // local header and central directory after a valid archive is built.
        let mut bytes = standalone_fixture_zip(manifest, &[("other.file", b"duplicate")], None);
        let mut renamed = 0;
        for index in 0..bytes.len().saturating_sub(b"other.file".len()) {
            if &bytes[index..index + b"other.file".len()] == b"other.file" {
                bytes[index..index + b"other.file".len()].copy_from_slice(b"Cargo.lock");
                renamed += 1;
            }
        }
        assert_eq!(renamed, 2);
        bytes
    }

    fn symlink_entry_zip(manifest: Value) -> Vec<u8> {
        let mut bytes = standalone_fixture_zip(manifest, &[], None);
        let start = bytes
            .windows(4)
            .position(|bytes| bytes == b"PK\x01\x02")
            .expect("central directory entry");
        // ZIP central-directory creator OS byte and external Unix mode.
        bytes[start + 5] = 3;
        bytes[start + 38..start + 42].copy_from_slice(&(0o120777_u32 << 16).to_le_bytes());
        bytes
    }

    fn ready_jobs() -> (WorkflowRun, WorkflowArtifact, Vec<WorkflowJob>) {
        let run = WorkflowRun {
            id: 11,
            name: "CI".into(),
            run_attempt: 1,
            path: ".github/workflows/ci.yml@refs/pull/581/merge".into(),
            event: "pull_request".into(),
            status: "completed".into(),
            conclusion: Some("success".into()),
            head_sha: "a".repeat(40),
        };
        let artifact = WorkflowArtifact {
            id: 12,
            name: format!("okx-research-component-{}", run.head_sha),
            expired: false,
            digest: Some(format!("sha256:{}", "d".repeat(64))),
            workflow_run: Some(okx_github::WorkflowArtifactRun {
                id: run.id,
                repository_id: REPOSITORY_ID,
                head_repository_id: REPOSITORY_ID,
                head_sha: run.head_sha.clone(),
            }),
        };
        let jobs = [
            "classify",
            "architecture-contracts",
            "dependency-audit",
            "cloudflare-mcp-contracts",
            "research-service-tests-windows",
            "linux-core",
            "windows-native",
        ]
        .into_iter()
        .map(|name| WorkflowJob {
            run_id: run.id,
            name: name.into(),
            status: "completed".into(),
            conclusion: Some("success".into()),
            head_sha: run.head_sha.clone(),
        })
        .collect();
        (run, artifact, jobs)
    }

    #[test]
    fn independent_research_component_strict_three_file_release() {
        let (manifest, zip) = standalone_fixture();
        let (record, worker) = parse_standalone(&zip).unwrap();
        assert_eq!(record.run_id, 11);
        assert_eq!(record.artifact_id, 12);
        assert_eq!(worker, fixture_pe_image());
        assert_eq!(record.worker_sha256, sha256_hex(&worker));
        let bad = [
            standalone_fixture_zip(manifest.clone(), &[("okx-agent.exe", b"not allowed")], None),
            standalone_fixture_zip(manifest.clone(), &[("other/secret", b"extra")], None),
            standalone_fixture_zip(manifest.clone(), &[], Some("Cargo.lock")),
            standalone_fixture_zip(manifest.clone(), &[], Some("okx-research-service.exe")),
            duplicate_named_entry_zip(manifest.clone()),
            symlink_entry_zip(manifest.clone()),
        ];
        for candidate in &bad {
            assert!(parse_standalone(candidate).is_err());
        }
        for key in [
            "source_tree",
            "source_head_sha",
            "schema",
            "cargo_lock_sha256",
        ] {
            let mut changed = manifest.clone();
            changed[key] = json!("invalid");
            assert!(
                parse_standalone(&standalone_fixture_zip(changed, &[], None)).is_err(),
                "accepted invalid {key}"
            );
        }
        let mut changed = manifest.clone();
        changed["worker"]["sha256"] = json!("0".repeat(64));
        assert!(parse_standalone(&standalone_fixture_zip(changed, &[], None)).is_err());
        let mut changed = manifest.clone();
        changed["worker"]["size"] = json!(42);
        assert!(parse_standalone(&standalone_fixture_zip(changed, &[], None)).is_err());
        let mut changed = manifest.clone();
        changed["worker"]["protocol_version"] = json!(2);
        assert!(parse_standalone(&standalone_fixture_zip(changed, &[], None)).is_err());
        let mut changed = manifest;
        changed["unexpected_engine"] = json!(true);
        assert!(parse_standalone(&standalone_fixture_zip(changed, &[], None)).is_err());
    }

    #[test]
    fn research_component_requires_this_attempts_real_ready_gates() {
        let (run, artifact, jobs) = ready_jobs();
        validate_research_release_jobs(&run, &artifact, &jobs).unwrap();
        let (_, zip) = standalone_fixture();
        let mut with_real_digest = artifact.clone();
        with_real_digest.digest = Some(format!("sha256:{}", sha256_hex(&zip)));
        verify_research_zip_digest(&with_real_digest, &zip).unwrap();
        let mut tampered = zip;
        tampered[0] ^= 1;
        assert!(verify_research_zip_digest(&with_real_digest, &tampered).is_err());
        assert!(is_canonical_ci_workflow_path(".github/workflows/ci.yml"));
        assert!(is_canonical_ci_workflow_path(
            ".github/workflows/ci.yml@main"
        ));
        let mut plain = run.clone();
        plain.path = ".github/workflows/ci.yml".into();
        validate_research_release_jobs(&plain, &artifact, &jobs).unwrap();
        for path in [
            ".github/workflows/ci.yml@",
            ".github/workflows/ci.yml/evil",
            ".github/workflows/Ci.yml",
            ".github/workflows/ci.yml@refs//heads/main",
            ".github/workflows/ci-test.yml@main",
            ".github/workflows/ci.yml@../../evil",
            ".github/workflows/ci.yml@refs/pull/581/merge?attacker=1",
        ] {
            let mut bad = run.clone();
            bad.path = path.into();
            assert!(validate_research_release_jobs(&bad, &artifact, &jobs).is_err());
        }
        for name in [
            "classify",
            "architecture-contracts",
            "dependency-audit",
            "cloudflare-mcp-contracts",
            "research-service-tests-windows",
            "linux-core",
            "windows-native",
        ] {
            let missing: Vec<_> = jobs
                .iter()
                .filter(|job| job.name != name)
                .cloned()
                .collect();
            assert!(
                validate_research_release_jobs(&run, &artifact, &missing).is_err(),
                "Draft/skipped run accepted without {name}"
            );
        }
        let mut previous_attempt = run.clone();
        previous_attempt.run_attempt = 2;
        assert!(validate_research_release_jobs(&previous_attempt, &artifact, &jobs).is_err());
        previous_attempt.run_attempt = 0;
        assert!(validate_research_release_jobs(&previous_attempt, &artifact, &jobs).is_err());
        let mut bad_artifact = artifact.clone();
        bad_artifact.digest = None;
        assert!(validate_research_release_jobs(&run, &bad_artifact, &jobs).is_err());
        bad_artifact.digest = Some("sha256:wrong".into());
        assert!(validate_research_release_jobs(&run, &bad_artifact, &jobs).is_err());
        bad_artifact = artifact.clone();
        bad_artifact.name = format!("okx-windows-bundle-{}", run.head_sha);
        assert!(validate_research_release_jobs(&run, &bad_artifact, &jobs).is_err());
        bad_artifact = artifact.clone();
        bad_artifact.workflow_run.as_mut().unwrap().id = run.id + 1;
        assert!(validate_research_release_jobs(&run, &bad_artifact, &jobs).is_err());
        bad_artifact = artifact.clone();
        bad_artifact
            .workflow_run
            .as_mut()
            .unwrap()
            .head_repository_id = REPOSITORY_ID + 1;
        assert!(validate_research_release_jobs(&run, &bad_artifact, &jobs).is_err());
        let mut wrong_run = jobs.clone();
        wrong_run[0].run_id += 1;
        assert!(validate_research_release_jobs(&run, &artifact, &wrong_run).is_err());
        let mut wrong_head = jobs.clone();
        wrong_head[0].head_sha = "c".repeat(40);
        assert!(validate_research_release_jobs(&run, &artifact, &wrong_head).is_err());
        let mut duplicate = jobs.clone();
        duplicate.push(jobs[0].clone());
        assert!(validate_research_release_jobs(&run, &artifact, &duplicate).is_err());
        let mut skipped = jobs;
        skipped[0].conclusion = Some("skipped".into());
        assert!(validate_research_release_jobs(&run, &artifact, &skipped).is_err());
    }

    #[test]
    fn standalone_research_stages_and_rolls_back_with_engine_entirely_absent() {
        let root = std::env::temp_dir().join(format!(
            "research-no-engine-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let runtime = root.join("runtime");
        let control = root.join("control");
        fs::create_dir_all(&runtime).unwrap();
        fs::create_dir_all(&control).unwrap();
        let (_, zip) = standalone_fixture();
        let (first, worker) = parse_standalone(&zip).unwrap();
        let mut supervisor = ResearchSupervisor::at(control, runtime.clone()).unwrap();
        let staged = prepare_research_transition(&runtime, &first, &worker).unwrap();
        supervisor
            .transition_image(
                None,
                0,
                &first.worker_sha256,
                || commit_research_transition(&runtime, &staged),
                || restore_research_transition(&runtime, &staged),
            )
            .unwrap();
        let next_bytes = b"standalone research update";
        let mut next = fixture_record(next_bytes, 'c');
        next.run_id = 13;
        next.artifact_id = 14;
        let prepared = prepare_research_transition(&runtime, &next, next_bytes).unwrap();
        let generation = supervisor.process_generation();
        supervisor
            .transition_image(
                Some(&first.worker_sha256),
                generation,
                &next.worker_sha256,
                || commit_research_transition(&runtime, &prepared),
                || restore_research_transition(&runtime, &prepared),
            )
            .unwrap();
        let receipt = rollback_research_with_supervisor(
            &runtime,
            &mut supervisor,
            &next.worker_sha256,
            &first.worker_sha256,
        )
        .unwrap();
        assert_eq!(receipt["rolled_back"], true);
        assert_eq!(receipt["provenance"]["worker_sha256"], first.worker_sha256);
        assert!(!runtime.join("okx-agent.exe").exists());
        assert!(!runtime.join("installed-agent.json").exists());
        assert!(!runtime.join("desired-state.json").exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn active_controller_self_sha_is_checked_without_engine_files() {
        let root = std::env::temp_dir().join(format!(
            "research-control-self-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let binary = root.join("okx-host-control.exe");
        fs::write(&binary, b"test control image").unwrap();
        let active = root.join("active-controller.json");
        let data = okx_host_launcher::ActiveController {
            schema: "okx.host-launcher.active/v1".into(),
            repository_id: REPOSITORY_ID,
            active: okx_host_launcher::ControllerVersion {
                repository_id: REPOSITORY_ID,
                run_id: 10,
                artifact_id: 20,
                source_head_sha: "a".repeat(40),
                source_tree: "b".repeat(40),
                rust_version: "rustc 1.100.0".into(),
                controller_sha256: sha256_hex(b"test control image"),
            },
            previous: None,
        };
        fs::write(&active, serde_json::to_vec(&data).unwrap()).unwrap();
        verify_active_control_identity_at(&active, &binary).unwrap();
        fs::write(&binary, b"tampered control").unwrap();
        assert!(verify_active_control_identity_at(&active, &binary).is_err());
        fs::write(&binary, b"test control image").unwrap();
        let mut forged = data;
        forged.active.source_head_sha = "invalid".into();
        fs::write(&active, serde_json::to_vec(&forged).unwrap()).unwrap();
        assert!(verify_active_control_identity_at(&active, &binary).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn active_controller_refuses_symlinked_record() {
        use std::os::unix::fs::symlink;
        let root = std::env::temp_dir().join(format!(
            "research-control-link-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let target = root.join("real.json");
        fs::write(&target, b"{}").unwrap();
        let linked = root.join("active-controller.json");
        symlink(&target, &linked).unwrap();
        assert!(verify_active_control_identity_at(&linked, &target).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn research_pointer_global_mutex_serializes_two_control_sessions() {
        let root = std::env::temp_dir().join(format!(
            "research-global-mutex-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let first = ResearchPointerGuard::acquire(&root).unwrap();
        let competitor_root = root.clone();
        assert_eq!(
            std::thread::spawn(move || {
                ResearchPointerGuard::acquire(&competitor_root)
                    .err()
                    .unwrap()
                    .code()
            })
            .join()
            .unwrap(),
            "RESEARCH_POINTER_LOCK_UNAVAILABLE"
        );
        drop(first);
        assert!(ResearchPointerGuard::acquire(&root).is_ok());
    }

    fn fixture_record(bytes: &[u8], marker: char) -> InstalledResearch {
        InstalledResearch {
            schema: "okx.host-control.installed-research/v1".into(),
            repository_id: REPOSITORY_ID,
            run_id: 1,
            artifact_id: 2,
            source_head_sha: marker.to_string().repeat(40),
            source_tree: "b".repeat(40),
            worker_sha256: sha256_hex(bytes),
            protocol_version: 1,
            storage_version: 1,
        }
    }

    #[test]
    fn research_transition_is_staged_before_promotion_and_fenced_by_generation() {
        let root = std::env::temp_dir().join(format!(
            "research-atomic-stage-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let runtime = root.join("runtime");
        let control = root.join("control");
        fs::create_dir_all(&runtime).unwrap();
        fs::create_dir_all(&control).unwrap();
        let old = fixture_record(b"old research", 'a');
        stage_research(&runtime, &old, b"old research").unwrap();
        let new = fixture_record(b"new research", 'c');
        let prepared = prepare_research_transition(&runtime, &new, b"new research").unwrap();
        assert_eq!(
            read_research_pointer(&runtime, CURRENT_RESEARCH_POINTER).unwrap(),
            Some(old.clone()),
            "staging an immutable image must not activate it"
        );
        let mut owner = ResearchSupervisor::at(control, runtime.clone()).unwrap();
        assert_eq!(
            owner
                .transition_image(
                    prepared.previous_sha(),
                    1,
                    &new.worker_sha256,
                    || commit_research_transition(&runtime, &prepared),
                    || restore_research_transition(&runtime, &prepared),
                )
                .unwrap_err()
                .code(),
            "RESEARCH_IDENTITY_MISMATCH"
        );
        assert_eq!(
            read_research_pointer(&runtime, CURRENT_RESEARCH_POINTER).unwrap(),
            Some(old)
        );
        let status = owner
            .transition_image(
                prepared.previous_sha(),
                0,
                &new.worker_sha256,
                || commit_research_transition(&runtime, &prepared),
                || restore_research_transition(&runtime, &prepared),
            )
            .unwrap();
        assert_eq!(status["phase"], "STOPPED");
        assert_eq!(
            read_research_pointer(&runtime, CURRENT_RESEARCH_POINTER).unwrap(),
            Some(new)
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_research_promotion_compensates_both_pointers_without_engine_mutation() {
        let root = std::env::temp_dir().join(format!(
            "research-promotion-fault-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let runtime = root.join("runtime");
        let control = root.join("control");
        fs::create_dir_all(&runtime).unwrap();
        fs::create_dir_all(&control).unwrap();
        fs::write(runtime.join("okx-agent.exe"), b"untouched").unwrap();
        let old = fixture_record(b"previous version", 'a');
        stage_research(&runtime, &old, b"previous version").unwrap();
        let new = fixture_record(b"candidate version", 'c');
        let prepared = prepare_research_transition(&runtime, &new, b"candidate version").unwrap();
        let mut owner = ResearchSupervisor::at(control, runtime.clone()).unwrap();
        let error = owner
            .transition_image(
                prepared.previous_sha(),
                0,
                &new.worker_sha256,
                || {
                    commit_research_transition(&runtime, &prepared)?;
                    Err(HostControlError::ArtifactVerification(
                        "injected activation failure",
                    ))
                },
                || restore_research_transition(&runtime, &prepared),
            )
            .unwrap_err();
        assert_eq!(error.code(), "RESEARCH_ACTIVATION_ROLLED_BACK");
        assert_eq!(
            read_research_pointer(&runtime, CURRENT_RESEARCH_POINTER).unwrap(),
            Some(old)
        );
        assert!(
            read_research_pointer(&runtime, ROLLBACK_RESEARCH_POINTER)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            fs::read(runtime.join("okx-agent.exe")).unwrap(),
            b"untouched"
        );
        assert_eq!(owner.status().unwrap()["last_error"], error.code());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn controlled_research_rollback_restores_prior_hash_with_no_process_spawn() {
        let root = std::env::temp_dir().join(format!(
            "research-controlled-rollback-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let runtime = root.join("runtime");
        let control = root.join("control");
        fs::create_dir_all(&runtime).unwrap();
        fs::create_dir_all(&control).unwrap();
        let old = fixture_record(b"rollback target", 'a');
        let current = fixture_record(b"running candidate", 'c');
        stage_research(&runtime, &old, b"rollback target").unwrap();
        stage_research(&runtime, &current, b"running candidate").unwrap();
        let mut owner = ResearchSupervisor::at(control, runtime.clone()).unwrap();
        let receipt = rollback_research_with_supervisor(
            &runtime,
            &mut owner,
            &current.worker_sha256,
            &old.worker_sha256,
        )
        .unwrap();
        assert_eq!(receipt["rolled_back"], true);
        assert_eq!(receipt["research_process"]["phase"], "STOPPED");
        assert_eq!(receipt["activation"], "requires_research_edge_attestation");
        assert_eq!(
            read_research_pointer(&runtime, CURRENT_RESEARCH_POINTER).unwrap(),
            Some(old)
        );
        assert_eq!(
            rollback_research_with_supervisor(
                &runtime,
                &mut owner,
                &current.worker_sha256,
                &"0".repeat(64)
            )
            .unwrap_err()
            .code(),
            "RESEARCH_IDENTITY_MISMATCH"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_rollback_compensation_never_overwrites_a_third_image() {
        let root = std::env::temp_dir().join(format!(
            "research-rollback-cas-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let old = fixture_record(b"old", 'a');
        let current = fixture_record(b"current", 'c');
        let third = fixture_record(b"third", 'd');
        stage_research(&root, &old, b"old").unwrap();
        stage_research(&root, &current, b"current").unwrap();

        // The failed rollback published the old image. Reverting that exact
        // write is permitted and restores the original current image.
        write_research_pointer(&root, CURRENT_RESEARCH_POINTER, &old).unwrap();
        restore_rollback_pointer_if_expected(&root, &old, &current).unwrap();
        assert_eq!(
            read_research_pointer(&root, CURRENT_RESEARCH_POINTER).unwrap(),
            Some(current.clone())
        );

        // Another owner has advanced the pointer after the first session's
        // rollback commit. Its candidate must never be overwritten by the
        // first session's failure compensation.
        stage_research(&root, &third, b"third").unwrap();
        assert_eq!(
            restore_rollback_pointer_if_expected(&root, &old, &current)
                .unwrap_err()
                .code(),
            "RESEARCH_ROLLBACK_UNCONFIRMED"
        );
        assert_eq!(
            read_research_pointer(&root, CURRENT_RESEARCH_POINTER).unwrap(),
            Some(third)
        );
        fs::remove_dir_all(root).unwrap();
    }

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

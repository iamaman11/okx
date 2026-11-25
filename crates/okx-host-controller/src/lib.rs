pub mod account_catalog;
pub mod artifact;
pub mod auth;
pub mod autostart;
pub mod background_process;
mod control_ack;
pub mod controller_update;
pub mod desired;
pub mod job;
pub mod legacy_bootstrap;
pub mod lifecycle_executor;
pub mod provenance;
pub mod research_supervisor;
pub mod root_migration;
pub mod runtime;
pub mod single_instance;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum HostControlError {
    #[error("protocol error: {0}")]
    Protocol(#[from] okx_ipc_contracts::ProtocolError),

    #[error("GitHub transport error: {0}")]
    Github(#[from] okx_github::GitHubError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("launcher error: {0}")]
    Launcher(#[from] okx_host_launcher::LauncherError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("local scheduler time error: {0}")]
    ScheduleTime(#[from] jiff::Error),

    #[error("native secret store error: {0}")]
    SecretStore(String),

    #[error("GitHub control token was not found")]
    GithubTokenNotFound,

    #[error("GitHub control token is invalid")]
    InvalidGithubToken,

    #[error("native Windows host control is unavailable on this platform")]
    UnsupportedPlatform,

    #[error("poll interval must be between 1 and 60 seconds")]
    InvalidPollInterval,

    #[error("optional development repository C:\\okx\\workspace was not found")]
    RepositoryMissing,

    #[error("origin does not identify iamaman11/okx")]
    OriginMismatch,

    #[error("canonical repository has local changes")]
    RepositoryDirty,

    #[error("canonical repository is not on main")]
    BranchMismatch,

    #[error("canonical main is not equal to origin/main; run sync first")]
    MainNotSynced,

    #[error("required command failed: {0}")]
    CommandFailed(&'static str),

    #[error("okx-agent.exe is not built")]
    AgentBinaryMissing,

    #[error("okx-trading-engine is running; stop it before mutating the workspace")]
    AgentRunning,

    #[error("host controller install source is invalid")]
    InvalidInstallSource,

    #[error("artifact archive error: {0}")]
    ArtifactArchive(#[from] zip::result::ZipError),

    #[error("artifact verification failed: {0}")]
    ArtifactVerification(&'static str),

    #[error("artifact source tree does not match accepted origin/main")]
    ArtifactSourceTreeMismatch,

    #[error("artifact binary SHA-256 mismatch")]
    ArtifactHashMismatch,

    #[error("artifact deployment path is not valid for this execution layer")]
    InvalidExecutionPath,

    #[error("desired lifecycle state is corrupt or unsupported")]
    DesiredStateCorrupt,

    #[error("independent Research desired state is corrupt or unsupported")]
    ResearchStateCorrupt,

    #[error("independent Research active/installed image or generation does not match request")]
    ResearchIdentityMismatch,

    #[error("independent Research process generation overflow")]
    ResearchGenerationExhausted,

    #[error(
        "legacy Engine and standalone Research require exclusive process ownership during cutover"
    )]
    ResearchTransitionRequiresExclusiveProcess,

    #[error("existing Research catalog requires its expected scope fingerprint")]
    ResearchCatalogScopeRequired,

    #[error("expected existing Research catalog is missing")]
    ResearchCatalogMissing,

    #[error("Research catalog roots or selected bindings conflict")]
    ResearchCatalogConflict,

    #[error("account metadata catalog is corrupt or invalid")]
    AccountCatalogInvalid,

    #[error("account metadata identifier or UID is already registered differently")]
    AccountCatalogConflict,

    #[error("native account metadata catalog has reached its bounded capacity")]
    AccountCatalogFull,

    #[error("agent process could not be assigned to the controller job object: {0}")]
    JobAssignment(String),

    #[error("another host-controller instance is already running")]
    ControllerAlreadyRunning,

    #[error("Windows interactive account identity is unavailable")]
    WindowsIdentityUnavailable,

    #[error("Windows autostart task is missing or does not match the fixed policy")]
    AutostartPolicyInvalid,

    #[error("verified controller update is not staged")]
    ControllerUpdateNotStaged,

    #[error("controller update state is invalid")]
    ControllerUpdateStateInvalid,

    #[error("staged or installed controller SHA-256 mismatch")]
    ControllerUpdateHashMismatch,

    #[error("another controller update is already staged")]
    ControllerUpdateConflict,

    #[error("controller update handoff has no durable CONTROL terminal PASS")]
    ControllerUpdateTerminalAckMissing,

    #[error("controller update activator process could not be launched")]
    ControllerUpdateActivatorLaunch,

    #[error("legacy controller bootstrap is disabled after launcher-root installation")]
    LegacyBootstrapDisabled,

    #[error("controller activation failure was deliberately injected for acceptance")]
    AcceptanceActivationFailureInjected,

    #[error("CONTROL result budget invariant failed")]
    ControlResponseBudgetInvariant,

    #[error(
        "native storage operation outcome is uncertain; inspect the durable owner, never replay blindly"
    )]
    NativeStorageOutcomeUnknown,

    #[error("secure random source failed: {0}")]
    Random(String),
}

impl HostControlError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Protocol(_) => "PROTOCOL_ERROR",
            Self::Github(_) => "GITHUB_ERROR",
            Self::Json(_) => "JSON_ERROR",
            Self::Launcher(error) => controller_update::launcher_error_code(error),
            Self::Io(_) => "IO_ERROR",
            Self::ScheduleTime(_) => "SCHEDULE_TIME_INVALID",
            Self::AccountCatalogInvalid => "ACCOUNT_CATALOG_INVALID",
            Self::AccountCatalogConflict => "ACCOUNT_CATALOG_CONFLICT",
            Self::AccountCatalogFull => "ACCOUNT_CATALOG_FULL",
            Self::SecretStore(_) => "SECRET_STORE_ERROR",
            Self::GithubTokenNotFound => "GITHUB_TOKEN_NOT_FOUND",
            Self::InvalidGithubToken => "INVALID_GITHUB_TOKEN",
            Self::UnsupportedPlatform => "UNSUPPORTED_PLATFORM",
            Self::InvalidPollInterval => "INVALID_POLL_INTERVAL",
            Self::RepositoryMissing => "REPOSITORY_MISSING",
            Self::OriginMismatch => "ORIGIN_MISMATCH",
            Self::RepositoryDirty => "REPOSITORY_DIRTY",
            Self::BranchMismatch => "BRANCH_MISMATCH",
            Self::MainNotSynced => "MAIN_NOT_SYNCED",
            Self::CommandFailed(_) => "COMMAND_FAILED",
            Self::AgentBinaryMissing => "AGENT_BINARY_MISSING",
            Self::AgentRunning => "AGENT_RUNNING",
            Self::InvalidInstallSource => "INVALID_INSTALL_SOURCE",
            Self::ArtifactArchive(_) => "ARTIFACT_ARCHIVE_ERROR",
            Self::ArtifactVerification(_) => "ARTIFACT_VERIFICATION_FAILED",
            Self::ArtifactSourceTreeMismatch => "ARTIFACT_SOURCE_TREE_MISMATCH",
            Self::ArtifactHashMismatch => "ARTIFACT_HASH_MISMATCH",
            Self::InvalidExecutionPath => "INVALID_EXECUTION_PATH",
            Self::DesiredStateCorrupt => "DESIRED_STATE_CORRUPT",
            Self::ResearchStateCorrupt => "RESEARCH_STATE_CORRUPT",
            Self::ResearchIdentityMismatch => "RESEARCH_IDENTITY_MISMATCH",
            Self::ResearchGenerationExhausted => "RESEARCH_GENERATION_EXHAUSTED",
            Self::ResearchTransitionRequiresExclusiveProcess => {
                "RESEARCH_TRANSITION_REQUIRES_EXCLUSIVE_PROCESS"
            }
            Self::ResearchCatalogScopeRequired => "RESEARCH_CATALOG_SCOPE_REQUIRED",
            Self::ResearchCatalogMissing => "RESEARCH_CATALOG_MISSING",
            Self::ResearchCatalogConflict => "RESEARCH_CATALOG_CONFLICT",
            Self::JobAssignment(_) => "JOB_ASSIGNMENT_FAILED",
            Self::ControllerAlreadyRunning => "CONTROLLER_ALREADY_RUNNING",
            Self::WindowsIdentityUnavailable => "WINDOWS_IDENTITY_UNAVAILABLE",
            Self::AutostartPolicyInvalid => "AUTOSTART_POLICY_INVALID",
            Self::ControllerUpdateNotStaged => "CONTROLLER_UPDATE_NOT_STAGED",
            Self::ControllerUpdateStateInvalid => "CONTROLLER_UPDATE_STATE_INVALID",
            Self::ControllerUpdateHashMismatch => "CONTROLLER_UPDATE_HASH_MISMATCH",
            Self::ControllerUpdateConflict => "CONTROLLER_UPDATE_CONFLICT",
            Self::ControllerUpdateTerminalAckMissing => "CONTROLLER_UPDATE_TERMINAL_ACK_MISSING",
            Self::ControllerUpdateActivatorLaunch => "CONTROLLER_UPDATE_ACTIVATOR_LAUNCH_FAILED",
            Self::LegacyBootstrapDisabled => "LEGACY_BOOTSTRAP_DISABLED",
            Self::AcceptanceActivationFailureInjected => "ACCEPTANCE_ACTIVATION_FAILURE_INJECTED",
            Self::ControlResponseBudgetInvariant => "CONTROL_RESPONSE_BUDGET_INVARIANT",
            Self::NativeStorageOutcomeUnknown => "NATIVE_STORAGE_OUTCOME_UNKNOWN",
            Self::Random(_) => "RANDOM_SOURCE_FAILED",
        }
    }
}

pub type HostControlResult<T> = Result<T, HostControlError>;

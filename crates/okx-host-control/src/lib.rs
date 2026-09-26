pub mod artifact;
pub mod auth;
pub mod executor;
pub mod runtime;
pub mod recovery;
pub mod service;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum HostControlError {
    #[error("protocol error: {0}")]
    Protocol(#[from] okx_protocol::ProtocolError),

    #[error("GitHub transport error: {0}")]
    Github(#[from] okx_github::GitHubError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("native secret store error: {0}")]
    SecretStore(String),

    #[error("machine secret store error: {0}")]
    MachineSecret(#[from] okx_windows_secrets::SecretStoreError),

    #[error("GitHub control token was not found")]
    GithubTokenNotFound,

    #[error("GitHub control token is invalid")]
    InvalidGithubToken,

    #[error("native Windows host control is unavailable on this platform")]
    UnsupportedPlatform,

    #[error("poll interval must be between 1 and 60 seconds")]
    InvalidPollInterval,

    #[error("canonical repository C:\\okx was not found")]
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

    #[error("okx-agent is running; stop it before mutating the workspace")]
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

    #[error("Windows service error: {0}")]
    WindowsService(String),

    #[error("controller crash recovery probe requires SCM service mode")]
    RecoveryProbeRequiresService,

    #[error("controller crash recovery marker belongs to another request")]
    RecoveryMarkerConflict,
}

impl HostControlError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Protocol(_) => "PROTOCOL_ERROR",
            Self::Github(_) => "GITHUB_ERROR",
            Self::Json(_) => "JSON_ERROR",
            Self::Io(_) => "IO_ERROR",
            Self::SecretStore(_) => "SECRET_STORE_ERROR",
            Self::MachineSecret(_) => "MACHINE_SECRET_ERROR",
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
            Self::WindowsService(_) => "WINDOWS_SERVICE_ERROR",
            Self::RecoveryProbeRequiresService => "RECOVERY_PROBE_REQUIRES_SERVICE",
            Self::RecoveryMarkerConflict => "RECOVERY_MARKER_CONFLICT",
        }
    }
}

pub type HostControlResult<T> = Result<T, HostControlError>;

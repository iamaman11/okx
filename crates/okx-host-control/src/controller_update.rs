use okx_github::{GitHubClient, OWNER_USER_ID};
use okx_host_launcher::{ControllerVersion, LauncherError};
use okx_protocol::{
    HostControlOperation, HostControlResult as ProtocolControlResult, HostControlStatus,
};
use serde_json::{Value, json};

use crate::{HostControlError, HostControlResult};

#[derive(Debug, Clone)]
pub struct ControllerUpdateCandidate {
    pub run_id: u64,
    pub artifact_id: u64,
    pub source_head_sha: String,
    pub source_tree: String,
    pub rust_version: String,
    pub controller_sha256: String,
}

impl ControllerUpdateCandidate {
    fn into_version(self) -> ControllerVersion {
        ControllerVersion {
            repository_id: okx_host_launcher::REPOSITORY_ID,
            run_id: self.run_id,
            artifact_id: self.artifact_id,
            source_head_sha: self.source_head_sha,
            source_tree: self.source_tree,
            rust_version: self.rust_version,
            controller_sha256: self.controller_sha256,
        }
    }
}

pub fn stage(candidate: ControllerUpdateCandidate, bytes: &[u8]) -> HostControlResult<Value> {
    Ok(okx_host_launcher::stage_update(
        candidate.into_version(),
        bytes,
    )?)
}

pub fn prepare_handoff(request_id: &str) -> HostControlResult<Value> {
    Ok(okx_host_launcher::prepare_activation(
        request_id,
        std::process::id(),
    )?)
}

pub fn mark_terminal_ack(request_id: &str) -> HostControlResult<Value> {
    Ok(okx_host_launcher::mark_terminal_ack(request_id)?)
}

pub async fn recover_terminal_ack(github: &GitHubClient) -> HostControlResult<bool> {
    let Some(pending) = okx_host_launcher::pending_activation()? else {
        return Ok(false);
    };
    if pending.terminal_ack {
        return Ok(true);
    }

    let comments = github.issue_comments(12).await?;
    if comments
        .iter()
        .any(|comment| durable_ack_body(comment.user_id, &comment.body, &pending.request_id))
    {
        okx_host_launcher::mark_terminal_ack(&pending.request_id)?;
        return Ok(true);
    }
    Ok(false)
}

pub fn abort() -> HostControlResult<Value> {
    Ok(okx_host_launcher::abort_update()?)
}

pub fn status_value() -> Value {
    okx_host_launcher::status_value()
}

fn durable_ack_body(user_id: u64, body: &str, request_id: &str) -> bool {
    if user_id != OWNER_USER_ID {
        return false;
    }
    let Ok(result) = serde_json::from_str::<ProtocolControlResult>(body) else {
        return false;
    };
    result.request_id == request_id
        && result.status == HostControlStatus::Pass
        && result.operation == HostControlOperation::HandoffControllerUpdate
        && result.validate().is_ok()
}

pub fn launcher_error_code(error: &LauncherError) -> &'static str {
    match error {
        LauncherError::RootNotInstalled => "LAUNCHER_ROOT_NOT_INSTALLED",
        LauncherError::RootConflict => "LAUNCHER_ROOT_CONFLICT",
        LauncherError::HashMismatch => "CONTROLLER_UPDATE_HASH_MISMATCH",
        LauncherError::ActivationConflict => "CONTROLLER_UPDATE_CONFLICT",
        LauncherError::NotStaged => "CONTROLLER_UPDATE_NOT_STAGED",
        LauncherError::InvalidReadiness | LauncherError::InvalidState => {
            "CONTROLLER_UPDATE_STATE_INVALID"
        }
        LauncherError::AlreadyRunning => "LAUNCHER_ALREADY_RUNNING",
        LauncherError::UnsupportedPlatform => "UNSUPPORTED_PLATFORM",
        LauncherError::ControllerLaunch => "CONTROLLER_LAUNCH_FAILED",
        LauncherError::Io(_) => "IO_ERROR",
        LauncherError::Json(_) => "JSON_ERROR",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_ack_requires_owner_pass_and_exact_handoff_operation() {
        let request_id = "ctl_controller_update_0123456789";
        let pass = ProtocolControlResult {
            schema: okx_protocol::HOST_CONTROL_RESULT_SCHEMA_V1.to_owned(),
            request_id: request_id.to_owned(),
            operation: HostControlOperation::HandoffControllerUpdate,
            status: HostControlStatus::Pass,
            observed_at: "2026-09-29T12:00:00.000Z".to_owned(),
            details: Some(json!({"handoff_prepared": true})),
            failure: None,
        };
        let pass_json = serde_json::to_string(&pass).expect("serialize");
        assert!(durable_ack_body(OWNER_USER_ID, &pass_json, request_id));
        assert!(!durable_ack_body(OWNER_USER_ID + 1, &pass_json, request_id));

        let mut wrong_operation = pass.clone();
        wrong_operation.operation = HostControlOperation::Status;
        let wrong_json = serde_json::to_string(&wrong_operation).expect("serialize");
        assert!(!durable_ack_body(OWNER_USER_ID, &wrong_json, request_id));

        let mut failed = pass;
        failed.status = HostControlStatus::Fail;
        let failed_json = serde_json::to_string(&failed).expect("serialize");
        assert!(!durable_ack_body(OWNER_USER_ID, &failed_json, request_id));
    }

    #[test]
    fn normal_update_module_has_no_self_overwrite_path() {
        let source = include_str!("controller_update.rs");
        assert!(!source.contains("okx-host-control.exe.new"));
        assert!(!source.contains("fs::rename(canonical"));
        assert!(!source.contains("schtasks"));
        assert!(!source.contains("Command::new"));
    }
}

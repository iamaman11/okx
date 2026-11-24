//! Versioned CONTROL mailbox messages. Native Engine and Research never import this crate.
use okx_ipc_contracts::{
    ProtocolError, accounts, validate_request_id, validate_sha256, validate_source_tree,
};
use serde::{Deserialize, Serialize};

pub const HOST_CONTROL_REQUEST_SCHEMA_V1: &str = "okx.windows.control/v1";
pub const HOST_CONTROL_RESULT_SCHEMA_V1: &str = "okx.windows.control.result/v1";
pub const HOST_RESEARCH_RUNTIME_STATUS_SCHEMA_V1: &str = "okx.host-control.research-runtime/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostControlRequest {
    pub schema: String,
    pub request_id: String,
    pub operation: HostControlOperation,
}

impl HostControlRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema != HOST_CONTROL_REQUEST_SCHEMA_V1 {
            return Err(ProtocolError::UnsupportedSchema(self.schema.clone()));
        }
        validate_host_control_request_id(&self.request_id)?;
        self.operation.validate()
    }
}

/// CONTROL owns installation and process lifecycle. Access and limits belong to governance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HostControlOperation {
    Status,
    AccountCatalog,
    Sync,
    BuildAgent,
    DeployAgent {
        run_id: u64,
        artifact_id: u64,
        expected_source_tree: String,
    },
    DeployResearch {
        run_id: u64,
        artifact_id: u64,
        expected_source_tree: String,
    },
    RollbackResearch {
        expected_current_worker_sha256: String,
        expected_rollback_worker_sha256: String,
    },
    InstallLauncherRoot {
        run_id: u64,
        artifact_id: u64,
        expected_source_tree: String,
    },
    UpgradeLauncherRoot {
        run_id: u64,
        artifact_id: u64,
        expected_source_tree: String,
        expected_current_launcher_sha256: String,
    },
    StageControllerUpdate {
        run_id: u64,
        artifact_id: u64,
        expected_source_tree: String,
    },
    HandoffControllerUpdate,
    AbortControllerUpdate,
    ControllerUpdateStatus,
    WorkspaceStatus,
    TestWorkspace,
    InitAgentIdentity,
    AgentIdentity,
    BootstrapAgentGithubToken,
    ProvisionCloudflareRuntimeToken,
    /// Ask CONTROL to generate a distinct Research credential. Neither its
    /// plaintext bearer nor a caller-supplied token is present in this wire.
    ProvisionResearchRuntimeToken,
    StartAgent,
    StopAgent,
    RestartAgent,
    /// Activate only the exact verified Research image currently selected by
    /// CONTROL. The scoped data root and research credential are host-owned.
    StartResearch {
        expected_installed_worker_sha256: String,
    },
    /// Stop only the observed Research process generation. A stale status
    /// cannot accidentally stop a newly started process or the Trading Engine.
    StopResearch {
        expected_active_worker_sha256: String,
        expected_process_generation: u64,
    },
    /// Read-only snapshot of CONTROL's independent Research process owner.
    StatusResearch,
    InitializeInstallation {
        installation_id: String,
        expected_release: String,
        operation_id: String,
    },
    InitializeAccount {
        account_key: String,
        installation_id: String,
        expected_release: String,
        operation_id: String,
    },
    BackupAccount {
        account_key: String,
    },
    RestoreAccount {
        account_key: String,
        snapshot_id: String,
        expected_release: String,
        operation_id: String,
    },
    InstallAutostart,
    AutostartStatus,
    HandoffToAutostart,
    AcceptanceKillAgent,
    AcceptanceCrashController,
    AcceptanceFailNextControllerActivation,
    TransportStatus,
}
impl HostControlOperation {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::InitializeInstallation {
                installation_id,
                expected_release,
                operation_id,
            }
            | Self::InitializeAccount {
                installation_id,
                expected_release,
                operation_id,
                ..
            } => {
                if !accounts::valid_installation(installation_id)
                    || !(8..=128).contains(&operation_id.len())
                    || !operation_id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                {
                    return Err(ProtocolError::InvalidRequestId);
                }
                validate_source_tree(expected_release)?;
                if let Self::InitializeAccount { account_key, .. } = self {
                    validate_account_wire(account_key)?;
                }
                Ok(())
            }
            Self::BackupAccount { account_key } => validate_account_wire(account_key),
            Self::RestoreAccount {
                account_key,
                snapshot_id,
                expected_release,
                operation_id,
            } => {
                validate_account_wire(account_key)?;
                validate_source_tree(expected_release)?;
                if snapshot_id.is_empty()
                    || snapshot_id.len() > 128
                    || !snapshot_id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                    || !(8..=128).contains(&operation_id.len())
                {
                    Err(ProtocolError::InvalidRequestId)
                } else {
                    Ok(())
                }
            }
            Self::DeployAgent {
                run_id,
                artifact_id,
                expected_source_tree,
            }
            | Self::DeployResearch {
                run_id,
                artifact_id,
                expected_source_tree,
            }
            | Self::InstallLauncherRoot {
                run_id,
                artifact_id,
                expected_source_tree,
            }
            | Self::StageControllerUpdate {
                run_id,
                artifact_id,
                expected_source_tree,
            }
            | Self::UpgradeLauncherRoot {
                run_id,
                artifact_id,
                expected_source_tree,
                ..
            } => {
                if *run_id == 0 || *artifact_id == 0 {
                    return Err(ProtocolError::InvalidRequestId);
                }
                validate_source_tree(expected_source_tree)?;
                if let Self::UpgradeLauncherRoot {
                    expected_current_launcher_sha256,
                    ..
                } = self
                {
                    validate_sha256(expected_current_launcher_sha256)?;
                }
                Ok(())
            }
            Self::RollbackResearch {
                expected_current_worker_sha256,
                expected_rollback_worker_sha256,
            } => {
                validate_sha256(expected_current_worker_sha256)?;
                validate_sha256(expected_rollback_worker_sha256)?;
                if expected_current_worker_sha256 == expected_rollback_worker_sha256 {
                    return Err(ProtocolError::InvalidRequestId);
                }
                Ok(())
            }
            Self::StartResearch {
                expected_installed_worker_sha256,
            } => validate_sha256(expected_installed_worker_sha256),
            Self::StopResearch {
                expected_active_worker_sha256,
                expected_process_generation,
            } => {
                validate_sha256(expected_active_worker_sha256)?;
                if *expected_process_generation == 0 {
                    Err(ProtocolError::InvalidRequestId)
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }
}
fn validate_account_wire(value: &str) -> Result<(), ProtocolError> {
    let p = value.split(':').collect::<Vec<_>>();
    if p.len() != 3 || p[0] != "okx" || !matches!(p[1], "real" | "demo") {
        return Err(ProtocolError::InvalidRequestId);
    }
    validate_sha256(p[2])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HostControlStatus {
    Pass,
    Fail,
}

/// Process observation only, never evidence of a successful Edge handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchRuntimePhase {
    Stopped,
    Starting,
    RunningUnverified,
    RestartWait,
    InvalidProvenance,
}

/// Research's catalog is not the legacy Engine-owned analytics database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchCatalogMode {
    HostIndependent,
}

/// Readback from the CONTROL-owned Research supervisor. The caller compares
/// process generation and exact image SHA before issuing a stop. No arbitrary
/// on-disk path, account identity or credential crosses this boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchRuntimeStatus {
    pub schema: String,
    pub desired_running: bool,
    pub process_generation: u64,
    pub pid: Option<u32>,
    pub active_worker_sha256: Option<String>,
    pub installed_worker_sha256: Option<String>,
    pub phase: ResearchRuntimePhase,
    pub catalog_mode: ResearchCatalogMode,
    pub retry_in_ms: Option<u64>,
    pub last_error: Option<String>,
}

impl ResearchRuntimeStatus {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema != HOST_RESEARCH_RUNTIME_STATUS_SCHEMA_V1 {
            return Err(ProtocolError::UnsupportedSchema(self.schema.clone()));
        }
        if let Some(value) = &self.active_worker_sha256 {
            validate_sha256(value)?;
        }
        if let Some(value) = &self.installed_worker_sha256 {
            validate_sha256(value)?;
        }
        if self.pid == Some(0)
            || (self.pid.is_some()
                && (self.process_generation == 0 || self.active_worker_sha256.is_none()))
            || (self.phase == ResearchRuntimePhase::RunningUnverified && self.pid.is_none())
            || self.retry_in_ms.is_some_and(|ms| ms > 600_000)
            || self.last_error.as_ref().is_some_and(|code| {
                code.is_empty()
                    || code.len() > 128
                    || !code
                        .bytes()
                        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
            })
        {
            return Err(ProtocolError::InvalidRequestId);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostControlFailure {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostControlResult {
    pub schema: String,
    pub request_id: String,
    pub operation: HostControlOperation,
    pub status: HostControlStatus,
    pub observed_at: String,
    pub details: Option<serde_json::Value>,
    pub failure: Option<HostControlFailure>,
}

impl HostControlResult {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema != HOST_CONTROL_RESULT_SCHEMA_V1 {
            return Err(ProtocolError::UnsupportedSchema(self.schema.clone()));
        }
        validate_host_control_request_id(&self.request_id)
    }
}

fn validate_host_control_request_id(value: &str) -> Result<(), ProtocolError> {
    if value.starts_with("ctl_") && validate_request_id(value).is_ok() {
        Ok(())
    } else {
        Err(ProtocolError::InvalidHostControlRequestId)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_wire_schemas_and_negative_inputs_are_unchanged() {
        let request = HostControlRequest {
            schema: HOST_CONTROL_REQUEST_SCHEMA_V1.into(),
            request_id: "ctl_controller_update_0123456789".into(),
            operation: HostControlOperation::Status,
        };
        assert!(request.validate().is_ok());
        let wire = serde_json::to_value(&request).unwrap();
        assert_eq!(
            wire,
            serde_json::json!({
                "schema":"okx.windows.control/v1",
                "request_id":"ctl_controller_update_0123456789",
                "operation":{"type":"status"}
            })
        );
        let restored: HostControlRequest = serde_json::from_value(wire.clone()).unwrap();
        assert!(restored.validate().is_ok());
        for (field, value) in [
            ("schema", serde_json::json!("okx.windows.control/v2")),
            ("request_id", serde_json::json!("agent_wrong_owner")),
        ] {
            let mut changed = wire.clone();
            changed[field] = value;
            assert!(
                serde_json::from_value::<HostControlRequest>(changed)
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
        let mut unknown = wire;
        unknown["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<HostControlRequest>(unknown).is_err());

        let result = HostControlResult {
            schema: HOST_CONTROL_RESULT_SCHEMA_V1.into(),
            request_id: request.request_id.clone(),
            operation: HostControlOperation::Status,
            status: HostControlStatus::Pass,
            observed_at: "2026-11-24T00:00:00Z".into(),
            details: None,
            failure: None,
        };
        assert!(result.validate().is_ok());
        let wire = serde_json::to_value(result).unwrap();
        assert_eq!(wire["schema"], "okx.windows.control.result/v1");
        assert_eq!(wire["status"], "PASS");
        assert_eq!(wire["operation"]["type"], "status");
        let mut unknown = wire.clone();
        unknown["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<HostControlResult>(unknown).is_err());
        let mut invalid = wire;
        invalid["request_id"] = serde_json::json!("ctl_bad!");
        assert!(
            serde_json::from_value::<HostControlResult>(invalid)
                .unwrap()
                .validate()
                .is_err()
        );
    }

    #[test]
    fn typed_research_rollback_requires_two_distinct_exact_hashes() {
        let command = serde_json::json!({
            "type": "rollback_research",
            "expected_current_worker_sha256": "a".repeat(64),
            "expected_rollback_worker_sha256": "b".repeat(64)
        });
        let parsed: HostControlOperation = serde_json::from_value(command.clone()).unwrap();
        assert!(parsed.validate().is_ok());
        assert_eq!(serde_json::to_value(parsed).unwrap(), command);
        let mut same = command.clone();
        same["expected_rollback_worker_sha256"] = serde_json::json!("a".repeat(64));
        assert!(
            serde_json::from_value::<HostControlOperation>(same)
                .unwrap()
                .validate()
                .is_err()
        );
        let mut malformed = command.clone();
        malformed["expected_current_worker_sha256"] = serde_json::json!("../bad");
        assert!(
            serde_json::from_value::<HostControlOperation>(malformed)
                .unwrap()
                .validate()
                .is_err()
        );
        let mut extra = command;
        extra["run_id"] = serde_json::json!(1);
        assert!(serde_json::from_value::<HostControlOperation>(extra).is_err());
    }

    #[test]
    fn standalone_research_lifecycle_requires_exact_image_and_process_generation() {
        let hash = "a".repeat(64);
        let start = serde_json::json!({
            "type": "start_research",
            "expected_installed_worker_sha256": hash,
        });
        let operation: HostControlOperation = serde_json::from_value(start.clone()).unwrap();
        assert!(operation.validate().is_ok());
        assert_eq!(serde_json::to_value(operation).unwrap(), start);

        let stop = serde_json::json!({
            "type": "stop_research",
            "expected_active_worker_sha256": hash,
            "expected_process_generation": 7,
        });
        let operation: HostControlOperation = serde_json::from_value(stop.clone()).unwrap();
        assert!(operation.validate().is_ok());
        assert_eq!(serde_json::to_value(operation).unwrap(), stop);

        let read: HostControlOperation = serde_json::from_value(serde_json::json!({
            "type": "status_research"
        }))
        .unwrap();
        assert_eq!(read, HostControlOperation::StatusResearch);
        assert!(read.validate().is_ok());

        for malformed in [
            serde_json::json!({"type":"start_research", "expected_installed_worker_sha256":"a"}),
            serde_json::json!({"type":"stop_research", "expected_active_worker_sha256":hash, "expected_process_generation":0}),
            serde_json::json!({"type":"stop_research", "expected_active_worker_sha256":"x", "expected_process_generation":8}),
        ] {
            let parsed: HostControlOperation = serde_json::from_value(malformed).unwrap();
            assert!(parsed.validate().is_err());
        }

        // No arbitrary path, account, environment variables, secret or venue
        // identity may be injected through a CONTROL lifecycle command.
        for extra in [
            "root",
            "bearer_token",
            "account_key",
            "host_scope",
            "agent_pid",
        ] {
            let mut changed = start.clone();
            changed[extra] = serde_json::json!("untrusted");
            assert!(serde_json::from_value::<HostControlOperation>(changed).is_err());
        }
        let mut invalid_status = serde_json::json!({"type":"status_research"});
        invalid_status["expected_process_generation"] = serde_json::json!(1);
        assert!(serde_json::from_value::<HostControlOperation>(invalid_status).is_err());
    }

    #[test]
    fn standalone_research_status_is_typed_and_does_not_claim_edge_online() {
        let status = ResearchRuntimeStatus {
            schema: HOST_RESEARCH_RUNTIME_STATUS_SCHEMA_V1.into(),
            desired_running: true,
            process_generation: 8,
            pid: Some(1158),
            active_worker_sha256: Some("a".repeat(64)),
            installed_worker_sha256: Some("b".repeat(64)),
            phase: ResearchRuntimePhase::RunningUnverified,
            catalog_mode: ResearchCatalogMode::HostIndependent,
            retry_in_ms: None,
            last_error: None,
        };
        status.validate().unwrap();
        let wire = serde_json::to_value(&status).unwrap();
        assert_eq!(wire["schema"], HOST_RESEARCH_RUNTIME_STATUS_SCHEMA_V1);
        assert_eq!(wire["phase"], "RUNNING_UNVERIFIED");
        assert_eq!(wire["catalog_mode"], "HOST_INDEPENDENT");
        assert_eq!(wire["process_generation"], 8);
        assert_eq!(wire["pid"], 1158);
        assert!(wire.get("edge_online").is_none());
        assert!(wire.get("bearer_token").is_none());
        assert!(wire.get("research_root").is_none());
        assert_eq!(
            serde_json::from_value::<ResearchRuntimeStatus>(wire.clone()).unwrap(),
            status
        );

        let mut unknown = wire.clone();
        unknown["status"] = serde_json::json!("HEALTHY");
        assert!(serde_json::from_value::<ResearchRuntimeStatus>(unknown).is_err());
        let mut invalid = wire.clone();
        invalid["process_generation"] = serde_json::json!(0);
        assert!(serde_json::from_value::<ResearchRuntimeStatus>(invalid)
            .unwrap()
            .validate()
            .is_err());
        let mut invalid = wire.clone();
        invalid["active_worker_sha256"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<ResearchRuntimeStatus>(invalid)
            .unwrap()
            .validate()
            .is_err());
        let mut invalid = wire.clone();
        invalid["last_error"] = serde_json::json!("secret appears here!");
        assert!(serde_json::from_value::<ResearchRuntimeStatus>(invalid)
            .unwrap()
            .validate()
            .is_err());
        let mut invalid = wire;
        invalid["catalog_mode"] = serde_json::json!("ENGINE_OWNED");
        assert!(serde_json::from_value::<ResearchRuntimeStatus>(invalid).is_err());
    }

    #[test]
    fn research_token_provision_command_has_no_bearer_fields() {
        let command = serde_json::json!({"type":"provision_research_runtime_token"});
        let parsed: HostControlOperation = serde_json::from_value(command.clone()).unwrap();
        assert_eq!(parsed, HostControlOperation::ProvisionResearchRuntimeToken);
        assert!(parsed.validate().is_ok());
        assert_eq!(serde_json::to_value(parsed).unwrap(), command);
        for field in ["token", "bearer", "credential", "token_sha256", "account_key"] {
            let mut changed = command.clone();
            changed[field] = serde_json::json!("a-secret-plaintext");
            assert!(serde_json::from_value::<HostControlOperation>(changed).is_err());
        }
    }
}

//! One parser and validator for durable owner-issued CONTROL acknowledgements.
use okx_github::OWNER_USER_ID;
use okx_protocol::{HostControlOperation, HostControlResult, HostControlStatus};

#[derive(Clone, Copy)]
pub(super) enum AckOperation {
    HandoffControllerUpdate,
    InstallLauncherRoot,
}

pub(super) fn matches_durable_ack(
    user_id: u64,
    body: &str,
    request_id: &str,
    expected: AckOperation,
) -> bool {
    if user_id != OWNER_USER_ID {
        return false;
    }
    let Ok(result) = serde_json::from_str::<HostControlResult>(body) else {
        return false;
    };
    let operation_matches = matches!(
        (&expected, &result.operation),
        (
            AckOperation::HandoffControllerUpdate,
            HostControlOperation::HandoffControllerUpdate
        ) | (
            AckOperation::InstallLauncherRoot,
            HostControlOperation::InstallLauncherRoot { .. }
        )
    );
    result.request_id == request_id
        && result.status == HostControlStatus::Pass
        && operation_matches
        && result.validate().is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handoff_result() -> HostControlResult {
        HostControlResult {
            schema: okx_protocol::HOST_CONTROL_RESULT_SCHEMA_V1.to_owned(),
            request_id: "ctl_controller_update_0123456789".to_owned(),
            operation: HostControlOperation::HandoffControllerUpdate,
            status: HostControlStatus::Pass,
            observed_at: "2026-09-29T12:00:00.000Z".to_owned(),
            details: Some(serde_json::json!({"handoff_prepared": true})),
            failure: None,
        }
    }

    #[test]
    fn durable_ack_rejects_wrong_owner_request_operation_status_and_schema() {
        let request = "ctl_controller_update_0123456789";
        let valid = handoff_result();
        let body = serde_json::to_string(&valid).unwrap();
        assert!(matches_durable_ack(
            OWNER_USER_ID,
            &body,
            request,
            AckOperation::HandoffControllerUpdate
        ));
        assert!(!matches_durable_ack(
            OWNER_USER_ID + 1,
            &body,
            request,
            AckOperation::HandoffControllerUpdate
        ));
        assert!(!matches_durable_ack(
            OWNER_USER_ID,
            &body,
            "ctl_controller_update_wrong",
            AckOperation::HandoffControllerUpdate
        ));
        assert!(!matches_durable_ack(
            OWNER_USER_ID,
            &body,
            request,
            AckOperation::InstallLauncherRoot
        ));

        let mut failed = valid.clone();
        failed.status = HostControlStatus::Fail;
        assert!(!matches_durable_ack(
            OWNER_USER_ID,
            &serde_json::to_string(&failed).unwrap(),
            request,
            AckOperation::HandoffControllerUpdate
        ));

        let mut wrong_schema = valid;
        wrong_schema.schema = "wrong/schema".to_owned();
        assert!(!matches_durable_ack(
            OWNER_USER_ID,
            &serde_json::to_string(&wrong_schema).unwrap(),
            request,
            AckOperation::HandoffControllerUpdate
        ));
        assert!(!matches_durable_ack(
            OWNER_USER_ID,
            "not json",
            request,
            AckOperation::HandoffControllerUpdate
        ));
    }

    #[test]
    fn durable_ack_accepts_only_typed_launcher_root_operation() {
        let request = "ctl_root_migration_012345";
        let mut result = handoff_result();
        result.request_id = request.to_owned();
        result.operation = HostControlOperation::InstallLauncherRoot {
            run_id: 1,
            artifact_id: 2,
            expected_source_tree: "a".repeat(40),
        };
        let body = serde_json::to_string(&result).unwrap();
        assert!(matches_durable_ack(
            OWNER_USER_ID,
            &body,
            request,
            AckOperation::InstallLauncherRoot
        ));
    }
}

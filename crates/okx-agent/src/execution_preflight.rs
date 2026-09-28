use okx_api::{AccountConfig, OkxEnvironment, account_uid_fingerprint};
use okx_observation::{ACCOUNT_SNAPSHOT_SCHEMA_V2, AccountSnapshot};
use serde::Serialize;

use crate::AgentResult;

pub const EXECUTOR_CREDENTIAL_PREFLIGHT_SCHEMA_V1: &str = "okx.executor-credential-preflight/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutorCredentialPreflight {
    pub schema: &'static str,
    pub accepted: bool,
    pub observer_read_only: bool,
    pub observer_private_ws_converged: bool,
    pub executor_read_permission: bool,
    pub executor_trade_permission: bool,
    pub executor_withdraw_permission: bool,
    pub executor_ip_bound: bool,
    pub account_identity_match: bool,
    pub account_uid_fingerprint: String,
    pub futures_mode: bool,
    pub long_short_mode: bool,
    pub subaccount: bool,
    pub production_environment: bool,
}

pub fn evaluate_executor_preflight_against_snapshot(
    environment: OkxEnvironment,
    observer: &AccountSnapshot,
    executor: &AccountConfig,
) -> ExecutorCredentialPreflight {
    let observer_read_only = observer.api_key_permissions == ["read_only"];
    let observer_private_ws_converged =
        observer.schema == ACCOUNT_SNAPSHOT_SCHEMA_V2 && observer.private_ws_connected;
    let executor_permissions = permissions(&executor.perm);
    evaluate_common(
        environment,
        observer_read_only,
        observer_private_ws_converged,
        &observer.account_uid_fingerprint,
        &observer.account_level,
        &observer.position_mode,
        &executor_permissions,
        executor,
    )
}

pub fn evaluate_executor_preflight(
    environment: OkxEnvironment,
    observer: &AccountConfig,
    executor: &AccountConfig,
) -> ExecutorCredentialPreflight {
    let observer_permissions = permissions(&observer.perm);
    let observer_read_only = observer_permissions == ["read_only"];
    let observer_fingerprint = account_uid_fingerprint(&observer.uid);
    let executor_permissions = permissions(&executor.perm);

    evaluate_common(
        environment,
        observer_read_only,
        true,
        &observer_fingerprint,
        &observer.account_level,
        &observer.position_mode,
        &executor_permissions,
        executor,
    )
}

#[allow(clippy::too_many_arguments)]
fn evaluate_common(
    environment: OkxEnvironment,
    observer_read_only: bool,
    observer_private_ws_converged: bool,
    observer_fingerprint: &str,
    observer_account_level: &str,
    observer_position_mode: &str,
    executor_permissions: &[String],
    executor: &AccountConfig,
) -> ExecutorCredentialPreflight {
    let executor_read_permission = executor_permissions.iter().any(|value| value == "read_only");
    let executor_trade_permission = executor_permissions.iter().any(|value| value == "trade");
    let executor_withdraw_permission = executor_permissions.iter().any(|value| value == "withdraw");
    let executor_permissions_exact =
        executor_permissions.len() == 2 && executor_read_permission && executor_trade_permission;
    let executor_ip_bound = !executor.ip.trim().is_empty();

    let executor_fingerprint = account_uid_fingerprint(&executor.uid);
    let account_identity_match = !observer_fingerprint.is_empty()
        && !executor.uid.is_empty()
        && observer_fingerprint == executor_fingerprint;

    let futures_mode = observer_account_level == "2" && executor.account_level == "2";
    let long_short_mode =
        observer_position_mode == "long_short_mode" && executor.position_mode == "long_short_mode";
    let subaccount = !executor.uid.is_empty()
        && !executor.main_uid.is_empty()
        && executor.uid != executor.main_uid;
    let production_environment = !environment.demo;

    let accepted = observer_read_only
        && observer_private_ws_converged
        && executor_permissions_exact
        && !executor_withdraw_permission
        && executor_ip_bound
        && account_identity_match
        && futures_mode
        && long_short_mode
        && subaccount
        && production_environment;

    ExecutorCredentialPreflight {
        schema: EXECUTOR_CREDENTIAL_PREFLIGHT_SCHEMA_V1,
        accepted,
        observer_read_only,
        observer_private_ws_converged,
        executor_read_permission,
        executor_trade_permission,
        executor_withdraw_permission,
        executor_ip_bound,
        account_identity_match,
        account_uid_fingerprint: executor_fingerprint,
        futures_mode,
        long_short_mode,
        subaccount,
        production_environment,
    }
}

fn permissions(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use okx_api::Region;

    use super::*;

    fn config(uid: &str, main_uid: &str, perm: &str, ip: &str) -> AccountConfig {
        AccountConfig {
            account_level: "2".to_owned(),
            position_mode: "long_short_mode".to_owned(),
            uid: uid.to_owned(),
            main_uid: main_uid.to_owned(),
            account_type: "1".to_owned(),
            account_stp_mode: "cancel_maker".to_owned(),
            auto_loan: false,
            greeks_type: "PA".to_owned(),
            fee_type: "0".to_owned(),
            label: String::new(),
            ip: ip.to_owned(),
            perm: perm.to_owned(),
        }
    }

    fn environment() -> OkxEnvironment {
        OkxEnvironment::new(Region::Global, false)
    }

    #[test]
    fn accepted_requires_separate_read_only_observer_and_ip_bound_trade_executor() {
        let observer = config("sub-uid", "main-uid", "read_only", "");
        let executor = config("sub-uid", "main-uid", "read_only,trade", "203.0.113.10");

        let evidence = evaluate_executor_preflight(environment(), &observer, &executor);

        assert!(evidence.accepted);
        assert!(evidence.observer_read_only);
        assert!(evidence.observer_private_ws_converged);
        assert!(evidence.executor_read_permission);
        assert!(evidence.executor_trade_permission);
        assert!(!evidence.executor_withdraw_permission);
        assert!(evidence.executor_ip_bound);
        assert!(evidence.account_identity_match);
        assert!(evidence.futures_mode);
        assert!(evidence.long_short_mode);
        assert!(evidence.subaccount);
        assert!(evidence.production_environment);
    }

    #[test]
    fn snapshot_preflight_requires_private_ws_convergence_and_exact_observer_identity() {
        use okx_observation::{
            ACCOUNT_CONVERGED_SOURCE_V2, AccountBalanceState, M4_REST_WS_CONVERGED_REASON,
        };

        let mut observer = AccountSnapshot {
            schema: ACCOUNT_SNAPSHOT_SCHEMA_V2.to_owned(),
            source: ACCOUNT_CONVERGED_SOURCE_V2.to_owned(),
            source_received_at: "2026-09-28T19:00:00Z".to_owned(),
            account_generation: "sha256:account".to_owned(),
            quality_reason: M4_REST_WS_CONVERGED_REASON.to_owned(),
            private_ws_connected: true,
            private_ws_generation: Some(1),
            private_ws_connection_fingerprint: Some("fingerprint".to_owned()),
            private_ws_last_inbound_ms: Some(1),
            private_ws_events_applied: Some(1),
            account_level: "2".to_owned(),
            position_mode: "long_short_mode".to_owned(),
            account_type: "1".to_owned(),
            account_uid_fingerprint: account_uid_fingerprint("sub-uid"),
            api_key_permissions: vec!["read_only".to_owned()],
            balance: AccountBalanceState {
                total_equity_usd: "1".to_owned(),
                adjusted_equity_usd: None,
                isolated_equity_usd: None,
                initial_margin_requirement_usd: None,
                maintenance_margin_requirement_usd: None,
                margin_ratio: None,
                notional_usd: None,
                update_time_ms: None,
                details: Vec::new(),
            },
            positions: Vec::new(),
            pending_orders: Vec::new(),
        };
        let executor = config("sub-uid", "main-uid", "read_only,trade", "203.0.113.10");

        assert!(
            evaluate_executor_preflight_against_snapshot(environment(), &observer, &executor)
                .accepted
        );

        observer.private_ws_connected = false;
        let evidence =
            evaluate_executor_preflight_against_snapshot(environment(), &observer, &executor);
        assert!(!evidence.accepted);
        assert!(!evidence.observer_private_ws_converged);
    }

    #[test]
    fn withdraw_permission_fails_closed() {
        let observer = config("sub-uid", "main-uid", "read_only", "");
        let executor = config(
            "sub-uid",
            "main-uid",
            "read_only,trade,withdraw",
            "203.0.113.10",
        );

        let evidence = evaluate_executor_preflight(environment(), &observer, &executor);
        assert!(!evidence.accepted);
        assert!(evidence.executor_withdraw_permission);
    }

    #[test]
    fn missing_ip_or_trade_permission_fails_closed() {
        let observer = config("sub-uid", "main-uid", "read_only", "");
        let no_ip = config("sub-uid", "main-uid", "read_only,trade", "");
        let no_trade = config("sub-uid", "main-uid", "read_only", "203.0.113.10");

        assert!(!evaluate_executor_preflight(environment(), &observer, &no_ip).accepted);
        assert!(!evaluate_executor_preflight(environment(), &observer, &no_trade).accepted);
    }

    #[test]
    fn account_or_mode_mismatch_fails_closed() {
        let observer = config("sub-uid", "main-uid", "read_only", "");
        let mut executor = config(
            "other-sub-uid",
            "main-uid",
            "read_only,trade",
            "203.0.113.10",
        );
        assert!(!evaluate_executor_preflight(environment(), &observer, &executor).accepted);

        executor.uid = "sub-uid".to_owned();
        executor.account_level = "3".to_owned();
        assert!(!evaluate_executor_preflight(environment(), &observer, &executor).accepted);

        executor.account_level = "2".to_owned();
        executor.position_mode = "net_mode".to_owned();
        assert!(!evaluate_executor_preflight(environment(), &observer, &executor).accepted);
    }

    #[test]
    fn demo_environment_is_not_accepted_for_production_preflight() {
        let observer = config("sub-uid", "main-uid", "read_only", "");
        let executor = config("sub-uid", "main-uid", "read_only,trade", "203.0.113.10");
        let demo = OkxEnvironment::new(Region::Global, true);

        let evidence = evaluate_executor_preflight(demo, &observer, &executor);
        assert!(!evidence.accepted);
        assert!(!evidence.production_environment);
    }
}

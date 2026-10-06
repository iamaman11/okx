use okx_api::{
    AccountConfig, AccountRateLimitEvidence, ClockEvidenceSnapshot, OkxEnvironment,
    RateBudgetSnapshot, account_uid_fingerprint,
};
use okx_observation::{ACCOUNT_SNAPSHOT_SCHEMA_V2, AccountSnapshot};
use serde::Serialize;

pub const EXECUTOR_CREDENTIAL_PREFLIGHT_SCHEMA_V1: &str = "okx.executor-credential-preflight/v1";
pub const EXECUTOR_PREFLIGHT_SCHEMA_V2: &str = "okx.executor-preflight/v2";
pub const EXECUTOR_PREFLIGHT_SCHEMA_V3: &str = "okx.executor-preflight/v3";

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutorPreflightSnapshot {
    pub schema: &'static str,
    pub accepted: bool,
    pub credential: ExecutorCredentialPreflight,
    pub clock: ClockEvidenceSnapshot,
    pub account_rate_limit: AccountRateLimitEvidence,
    pub rate_budget: RateBudgetSnapshot,
}

impl ExecutorPreflightSnapshot {
    pub fn new(
        credential: ExecutorCredentialPreflight,
        clock: ClockEvidenceSnapshot,
        account_rate_limit: AccountRateLimitEvidence,
        rate_budget: RateBudgetSnapshot,
    ) -> Self {
        Self {
            schema: EXECUTOR_PREFLIGHT_SCHEMA_V3,
            accepted: credential.accepted
                && clock.accepted
                && account_rate_limit.current_orders_per_2s > 0,
            credential,
            clock,
            account_rate_limit,
            rate_budget,
        }
    }
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
        false,
    )
}

pub fn evaluate_demo_executor_preflight_against_snapshot(
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
        true,
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
        false,
    )
}

pub fn evaluate_demo_executor_preflight(
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
        true,
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
    require_demo_environment: bool,
) -> ExecutorCredentialPreflight {
    let executor_read_permission = executor_permissions
        .iter()
        .any(|value| value == "read_only");
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
    let environment_matches_authority = environment.demo == require_demo_environment;

    let accepted = observer_read_only
        && observer_private_ws_converged
        && executor_permissions_exact
        && !executor_withdraw_permission
        && account_identity_match
        && futures_mode
        && long_short_mode
        && subaccount
        && environment_matches_authority;

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
    fn combined_preflight_requires_both_credential_and_clock_acceptance() {
        let observer = config("sub-uid", "main-uid", "read_only", "");
        let executor = config("sub-uid", "main-uid", "read_only,trade", "");
        let credential = evaluate_executor_preflight(environment(), &observer, &executor);
        let clock = ClockEvidenceSnapshot {
            server_time_ms: 1_790_000_000_000,
            local_midpoint_ms: 1_790_000_000_001,
            offset_ms: -1,
            round_trip_ms: 10,
            age_ms: 0,
            max_abs_offset_ms: 5_000,
            max_round_trip_ms: 2_000,
            max_age_ms: 2_000,
            accepted: true,
        };

        let account_rate_limit = AccountRateLimitEvidence {
            schema: okx_api::ACCOUNT_RATE_LIMIT_EVIDENCE_SCHEMA_V1,
            current_orders_per_2s: 1000,
            next_orders_per_2s: None,
            fill_ratio: None,
            main_fill_ratio: None,
            updated_at_ms: 1_790_000_000_000,
        };
        let rate_budget = okx_api::RateBudget::new().snapshot();

        let accepted = ExecutorPreflightSnapshot::new(
            credential.clone(),
            clock.clone(),
            account_rate_limit.clone(),
            rate_budget.clone(),
        );
        assert!(accepted.accepted);
        assert_eq!(accepted.schema, EXECUTOR_PREFLIGHT_SCHEMA_V3);

        let rejected_clock = ExecutorPreflightSnapshot::new(
            credential,
            ClockEvidenceSnapshot {
                accepted: false,
                ..clock
            },
            account_rate_limit,
            rate_budget,
        );
        assert!(!rejected_clock.accepted);
    }

    #[test]
    fn accepted_does_not_require_ip_binding() {
        let observer = config("sub-uid", "main-uid", "read_only", "");
        let executor = config("sub-uid", "main-uid", "read_only,trade", "");

        let evidence = evaluate_executor_preflight(environment(), &observer, &executor);

        assert!(evidence.accepted);
        assert!(evidence.observer_read_only);
        assert!(evidence.observer_private_ws_converged);
        assert!(evidence.executor_read_permission);
        assert!(evidence.executor_trade_permission);
        assert!(!evidence.executor_withdraw_permission);
        assert!(!evidence.executor_ip_bound);
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
    fn missing_trade_permission_fails_closed_but_missing_ip_does_not() {
        let observer = config("sub-uid", "main-uid", "read_only", "");
        let no_ip = config("sub-uid", "main-uid", "read_only,trade", "");
        let no_trade = config("sub-uid", "main-uid", "read_only", "");

        let no_ip_evidence = evaluate_executor_preflight(environment(), &observer, &no_ip);
        assert!(no_ip_evidence.accepted);
        assert!(!no_ip_evidence.executor_ip_bound);

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
    fn demo_acceptance_preflight_requires_demo_and_preserves_production_rejection() {
        let observer = config("sub-uid", "main-uid", "read_only", "");
        let executor = config(
            "sub-uid",
            "main-uid",
            "read_only,trade",
            "203.0.113.10",
        );
        let demo = OkxEnvironment::new(Region::Global, true);
        let production = OkxEnvironment::new(Region::Global, false);

        let accepted = evaluate_demo_executor_preflight(demo, &observer, &executor);
        assert!(accepted.accepted);
        assert!(
            !accepted.production_environment,
            "production_environment is raw evidence, not the Demo authority decision"
        );

        let rejected =
            evaluate_demo_executor_preflight(production, &observer, &executor);
        assert!(!rejected.accepted);
        assert!(rejected.production_environment);

        assert!(
            !evaluate_executor_preflight(demo, &observer, &executor).accepted,
            "the existing production preflight must stay fail-closed in Demo"
        );
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

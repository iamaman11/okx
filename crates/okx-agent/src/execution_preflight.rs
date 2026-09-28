use okx_api::{
    AccountApi, AccountConfig, Credentials, OkxEnvironment, OkxRestClient, account_uid_fingerprint,
};
use serde::Serialize;

use crate::AgentResult;

pub const EXECUTOR_CREDENTIAL_PREFLIGHT_SCHEMA_V1: &str =
    "okx.executor-credential-preflight/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutorCredentialPreflight {
    pub schema: &'static str,
    pub accepted: bool,
    pub observer_read_only: bool,
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

pub async fn probe_executor_credentials(
    environment: OkxEnvironment,
    observer_credentials: Credentials,
    executor_credentials: Credentials,
) -> AgentResult<ExecutorCredentialPreflight> {
    let observer = AccountApi::new(OkxRestClient::new(environment, observer_credentials)?)
        .config()
        .await?;
    let executor = AccountApi::new(OkxRestClient::new(environment, executor_credentials)?)
        .config()
        .await?;

    Ok(evaluate_executor_preflight(environment, &observer, &executor))
}

pub fn evaluate_executor_preflight(
    environment: OkxEnvironment,
    observer: &AccountConfig,
    executor: &AccountConfig,
) -> ExecutorCredentialPreflight {
    let observer_permissions = permissions(&observer.perm);
    let executor_permissions = permissions(&executor.perm);

    let observer_read_only = observer_permissions.iter().any(|value| value == "read_only")
        && !observer_permissions.iter().any(|value| value == "trade")
        && !observer_permissions.iter().any(|value| value == "withdraw");
    let executor_read_permission = executor_permissions.iter().any(|value| value == "read_only");
    let executor_trade_permission = executor_permissions.iter().any(|value| value == "trade");
    let executor_withdraw_permission =
        executor_permissions.iter().any(|value| value == "withdraw");
    let executor_ip_bound = !executor.ip.trim().is_empty();

    let observer_fingerprint = account_uid_fingerprint(&observer.uid);
    let executor_fingerprint = account_uid_fingerprint(&executor.uid);
    let account_identity_match =
        !observer.uid.is_empty() && !executor.uid.is_empty() && observer_fingerprint == executor_fingerprint;

    let futures_mode = executor.account_level == "2";
    let long_short_mode = executor.position_mode == "long_short_mode";
    let subaccount = !executor.uid.is_empty()
        && !executor.main_uid.is_empty()
        && executor.uid != executor.main_uid;
    let production_environment = !environment.demo;

    let accepted = observer_read_only
        && executor_read_permission
        && executor_trade_permission
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

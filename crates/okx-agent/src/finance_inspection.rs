//! #510: one strictly read-only native view of an independently supplied,
//! verified stopped finance import. This is NOT TradeService/Risk authority.
//! A missing or mismatched source is an error, never an empty budget.
use crate::{AgentError, AgentResult};
use okx_execution::account_worker::UnifiedFinanceHandle;
use serde_json::{Value, json};
use std::{path::Path, time::Duration};

#[derive(Debug, Clone)]
pub struct StoppedFinanceInspectionRequest {
    pub installation_id: String,
    pub expected_accounts: Vec<String>,
    pub expected_campaigns: Vec<String>,
    pub campaign: String,
    pub account: Option<String>,
    pub reservation_key: Option<String>,
    pub plan_sha256: Option<String>,
}

impl StoppedFinanceInspectionRequest {
    fn validate(&self) -> AgentResult<()> {
        if self.installation_id.is_empty()
            || self.expected_accounts.is_empty()
            || self.expected_campaigns.is_empty()
            || !self.expected_campaigns.contains(&self.campaign)
            || self
                .account
                .as_ref()
                .is_some_and(|account| !self.expected_accounts.contains(account))
            || self.reservation_key.is_some() != self.plan_sha256.is_some()
        {
            return Err(AgentError::FinancialCaptureTask(
                "FINANCE_INSPECTION_SCOPE_INVALID".into(),
            ));
        }
        Ok(())
    }
}

/// The only new Agent entrypoint for an offline/stopped import: no exchange
/// HTTP, old Cloudflare financial RPC, AccountRuntimeRegistry or POST grant.
/// Underlying original RiskBook/AccountStore readbacks share the existing one
/// SQL owner; the typed DTO merely exposes source values without recalculation.
pub async fn inspect(root: &Path, request: StoppedFinanceInspectionRequest) -> AgentResult<Value> {
    request.validate()?;
    let owner = UnifiedFinanceHandle::open_stopped_import(
        root.to_path_buf(),
        request.installation_id.clone(),
        request.expected_accounts,
        request.expected_campaigns,
    )
    .await
    .map_err(finance_error)?;
    let result = async {
        let inventory = owner.inventory().await.map_err(finance_error)?;
        if inventory.state != "OFFLINE_NOT_AUTHORIZED" {
            return Err(AgentError::FinancialCaptureTask(
                "FINANCE_INSPECTION_NOT_STOPPED".into(),
            ));
        }
        let campaign = owner
            .campaign_summary(request.campaign.clone())
            .await
            .map_err(finance_error)?;
        let account = match request.account {
            Some(account) => Some(
                owner
                    .stopped_account_summary(request.campaign.clone(), account)
                    .await
                    .map_err(finance_error)?,
            ),
            None => None,
        };
        let reservation = match (request.reservation_key, request.plan_sha256) {
            (Some(key), Some(plan)) => owner
                .stopped_inspect_reservation(request.campaign, key, plan)
                .await
                .map_err(finance_error)?,
            _ => None,
        };
        // None means no matching durable reservation, NOT safe to POST.
        Ok(json!({
            "schema": "okx.finance-stopped-inspection/v1",
            "authority": "OFFLINE_NOT_AUTHORIZED",
            "trade_authorized": false,
            "inventory": inventory,
            "campaign": campaign,
            "account": account,
            "reservation": reservation,
            "reservation_missing_is_safe_to_post": false
        }))
    }
    .await;
    // Do not leave an installation-wide exclusive finance lock behind after
    // a diagnostic invocation. A drop is asynchronous to its native worker.
    let mut terminated = owner.termination_signal();
    drop(owner);
    let released = tokio::time::timeout(Duration::from_secs(5), async {
        while !*terminated.borrow() {
            terminated.changed().await.map_err(|_| {
                AgentError::FinancialCaptureTask("FINANCE_INSPECTION_SHUTDOWN_LOST".into())
            })?;
        }
        Ok::<(), AgentError>(())
    })
    .await
    .map_err(|_| AgentError::FinancialCaptureTask("FINANCE_INSPECTION_SHUTDOWN_TIMEOUT".into()))?;
    released?;
    result
}

fn finance_error(error: okx_execution::StoreError) -> AgentError {
    AgentError::FinancialCaptureTask(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> StoppedFinanceInspectionRequest {
        StoppedFinanceInspectionRequest {
            installation_id: "installation-verified-test".into(),
            expected_accounts: vec![format!("okx:real:{}", "a".repeat(64))],
            expected_campaigns: vec![format!("sha256:{}", "b".repeat(64))],
            campaign: format!("sha256:{}", "b".repeat(64)),
            account: None,
            reservation_key: None,
            plan_sha256: None,
        }
    }

    #[test]
    fn external_scope_is_not_widened_or_guessed() {
        let mut r = request();
        r.campaign = format!("sha256:{}", "c".repeat(64));
        assert!(r.validate().is_err());
        let mut r = request();
        r.account = Some(format!("okx:real:{}", "d".repeat(64)));
        assert!(r.validate().is_err());
        let mut r = request();
        r.plan_sha256 = Some("e".repeat(64));
        assert!(r.validate().is_err());
        let mut r = request();
        r.expected_accounts.clear();
        assert!(r.validate().is_err());
    }

    #[tokio::test]
    async fn no_import_never_creates_financial_database_or_grants_risk() {
        let missing = std::env::temp_dir()
            .join(format!("okx-510-no-finance-import-{}", std::process::id()))
            .join("nonexistent");
        let err = inspect(&missing, request()).await.unwrap_err();
        assert!(err.to_string().contains("UNIFIED_FINANCE_IMPORT_NOT_FOUND"));
        assert!(!missing.join("finance").join("finance.sqlite").exists());
    }
}

//! Native one-command orchestrator. SQL commits precede every exchange mutation.
use crate::{
    accounts::{AccountResources, AccountRuntimeRegistry},
    risk_bridge::NativeRiskClient,
};
use chrono::Utc;
use okx_execution::{
    AccountStoreHandle, CentralReceipt, Decision, IntentPlan, IntentState, NewIntent, StoredIntent,
    StrategyInstance,
};
use okx_protocol::{
    account_governance::{AccountControl, CentralGovernanceSnapshot},
    risk_authority::*,
    trading::*,
};
use okx_runtime::PublicWsHandle;
use rust_decimal::{Decimal, RoundingStrategy};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;
mod execute;
mod plan;
mod reconcile;
mod recovery;
mod strategies;
pub struct TradeService {
    accounts: Arc<AccountRuntimeRegistry>,
    public_ws: PublicWsHandle,
    risk: NativeRiskClient,
    governance: RwLock<Option<CentralGovernanceSnapshot>>,
    analytics: RwLock<Option<crate::analytics::AnalyticsService>>,
}
fn now() -> i64 {
    Utc::now().timestamp_millis()
}
fn digest(v: &impl serde::Serialize) -> Result<String, String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(v).map_err(err)?)
    ))
}
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}
fn dec(v: &str) -> Result<Decimal, String> {
    Decimal::from_str(v).map_err(|_| "INVALID_EXACT_DECIMAL".into())
}
fn exact(v: Decimal) -> String {
    v.normalize().to_string()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
fn completed(data: Value) -> Value {
    json!({"status":"completed","as_of_ms":now(),"data":data,"quality":{"state":"not_evaluated"},"warnings":[]})
}
fn uncertain(intent: &str, reason: &str) -> Value {
    json!({"status":"unknown","intent_id":intent,"reason_code":reason,"scope":"intent","retryable":false,"next_action":"trade.status","as_of_ms":now()})
}
impl TradeService {
    pub fn new(
        accounts: Arc<AccountRuntimeRegistry>,
        public_ws: PublicWsHandle,
        risk: NativeRiskClient,
    ) -> Self {
        Self {
            accounts,
            public_ws,
            risk,
            governance: RwLock::new(None),
            analytics: RwLock::new(None),
        }
    }
    pub async fn set_analytics(&self, analytics: crate::analytics::AnalyticsService) {
        *self.analytics.write().await = Some(analytics);
    }
    pub async fn update_governance(
        &self,
        snapshot: &CentralGovernanceSnapshot,
    ) -> Result<(), String> {
        if !snapshot.valid() || snapshot.state.installation_id != self.accounts.installation_id() {
            return Err("INSTALLATION_POLICY_MISMATCH".into());
        }
        let mut latest = self.governance.write().await;
        if latest
            .as_ref()
            .is_some_and(|v| v.state.revision > snapshot.state.revision)
        {
            return Err("POLICY_REVISION_CONFLICT".into());
        }
        if latest.as_ref().is_some_and(|v| {
            v.state.revision == snapshot.state.revision && v.state != snapshot.state
        }) {
            return Err("POLICY_REVISION_CONFLICT".into());
        }
        *latest = Some(snapshot.clone());
        drop(latest);

        // A policy frame is the authoritative update.  Persist the campaign
        // ceiling for every currently connected account before returning the
        // frame acknowledgement, so a restart or read-only diagnostic cannot
        // retain an older ceiling until somebody submits a trade.
        let limit = dec(&snapshot.state.campaign_loss_limit_usd)?;
        for account in &snapshot.state.accounts {
            let Some(resources) = self.accounts.resolve(&account.account_key).await else {
                continue;
            };
            let revision = snapshot.state.revision as i64;
            let paused = !account.access_enabled();
            let campaign = snapshot.state.campaign_id.clone();
            resources
                .execution
                .store
                .call(deadline(), move |store| {
                    store.apply_authority_policy(revision, paused, &campaign, limit)
                })
                .await
                .map_err(err)?;
        }
        Ok(())
    }
    pub async fn diagnostic_access(&self, key: &str) -> Value {
        let latest = self.governance.read().await;
        let Some(snapshot) = latest.as_ref() else {
            return json!({"state":"not_evaluated","reason_code":"AUTHORITY_UNAVAILABLE"});
        };
        match snapshot
            .state
            .accounts
            .iter()
            .find(|a| a.account_key == key || a.id == key)
        {
            Some(a) => {
                json!({"state":"configured","enabled":a.enabled,"explicit_pause":a.explicit_pause,"policy_revision":a.policy_revision,"revision":snapshot.state.revision,"limits":a.limits,"issued_at_ms":snapshot.issued_at_ms})
            }
            None => json!({"state":"not_evaluated","reason_code":"ACCOUNT_ACCESS_MISSING"}),
        }
    }
    pub async fn risk_summary(&self) -> Result<Value, String> {
        let snapshot = self
            .governance
            .read()
            .await
            .clone()
            .ok_or("AUTHORITY_UNAVAILABLE")?;
        let response = self
            .risk
            .request(self.risk_command(
                &snapshot,
                format!("summary_{}", now()),
                RiskCommandAction::Summary {},
            ))
            .await
            .map_err(str::to_owned)?;
        if response.status != RiskDecisionStatus::Observed {
            return Err(response
                .reason
                .unwrap_or("CENTRAL_BUDGET_UNAVAILABLE".into()));
        }
        serde_json::to_value(response.budget.ok_or("CENTRAL_BUDGET_MISSING")?).map_err(err)
    }
    async fn resources(&self, account: &str) -> Result<Arc<AccountResources>, String> {
        self.accounts
            .resolve(account)
            .await
            .ok_or_else(|| "ACCOUNT_NOT_CONNECTED".into())
    }
    async fn authority(
        &self,
        r: &AccountResources,
        supplied: Option<&CentralGovernanceSnapshot>,
    ) -> Result<(CentralGovernanceSnapshot, AccountControl), String> {
        if let Some(s) = supplied {
            self.update_governance(s).await?;
        }
        let state = self
            .governance
            .read()
            .await
            .clone()
            .ok_or("AUTHORITY_UNAVAILABLE")?;
        let account = state
            .state
            .accounts
            .iter()
            .find(|a| a.account_key == r.configuration.account_key.wire())
            .cloned()
            .ok_or("ACCOUNT_ACCESS_MISSING")?;
        let revision = state.state.revision as i64;
        let paused = !account.access_enabled();
        let campaign = state.state.campaign_id.clone();
        let limit = dec(&state.state.campaign_loss_limit_usd)?;
        r.execution
            .store
            .call(deadline(), move |s| {
                s.apply_authority_policy(revision, paused, &campaign, limit)
            })
            .await
            .map_err(err)?;
        Ok((state, account))
    }
    pub async fn trade(
        &self,
        account: &str,
        request: &TradeRequest,
        governance: Option<&CentralGovernanceSnapshot>,
    ) -> Result<Value, String> {
        if !request.valid() {
            return Err("INVALID_TRADE_REQUEST".into());
        }
        let r = self.resources(account).await?;
        match request {
            TradeRequest::Status { reference } => self.status_intent(&r, reference).await,
            TradeRequest::Cancel {
                idempotency_key,
                reference,
            } => self.cancel(&r, idempotency_key, reference, request).await,
            TradeRequest::Close {
                idempotency_key,
                reference,
                expires_in_ms,
            } => {
                self.close(&r, idempotency_key, reference, *expires_in_ms, governance)
                    .await
            }
            TradeRequest::Preview {
                operation,
                strategy_instance,
                instrument,
                side,
                sizing,
                order,
                protection,
            } => {
                let (a, c) = self.authority(&r, governance).await?;
                let plan = self
                    .plan(
                        &r,
                        &c,
                        *operation,
                        strategy_instance,
                        instrument,
                        *side,
                        sizing,
                        order,
                        protection.as_ref(),
                        Instant::now() + Duration::from_secs(10),
                    )
                    .await?;
                Ok(completed(
                    json!({"plan":plan,"access":c.access_enabled(),"policy_revision":a.state.revision,"admission":"preview_only"}),
                ))
            }
            TradeRequest::Execute {
                operation: TradeOperation::Replace,
                ..
            } => self.replace(&r, request, governance).await,
            TradeRequest::Execute { .. } => self.execute(&r, request, governance).await,
        }
    }
}

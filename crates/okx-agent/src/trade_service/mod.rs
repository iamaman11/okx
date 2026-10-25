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
        // Publish the new in-process authority only after all connected SQL
        // account owners have durably applied it. On failure, retain the old
        // authority; stale SQL revisions reject attempts to reuse it.
        // Holding the write guard also prevents readers from observing a
        // partially applied multi-account policy.
        
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
        *latest = Some(snapshot.clone());
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
                json!({"state":"configured","enabled":a.enabled,"explicit_pause":a.explicit_pause,"policy_revision":a.policy_revision,"revision":snapshot.state.revision,
                    "limits":a.limits,"issued_at_ms":snapshot.issued_at_ms,
                    "session_generation":snapshot.session_generation})
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
    fn sql_policy_receipt(
        policy: &(i64, bool, String, Decimal, Decimal),
        governance: Option<&CentralGovernanceSnapshot>,
        account_key: &str,
    ) -> Value {
        let matched = governance.is_some_and(|snapshot| {
            let Some(account) = snapshot
                .state
                .accounts
                .iter()
                .find(|account| account.account_key == account_key)
            else {
                return false;
            };
            snapshot.valid()
                && i64::try_from(snapshot.state.revision).ok() == Some(policy.0)
                && policy.1 == !account.access_enabled()
                && policy.2 == snapshot.state.campaign_id
                && dec(&snapshot.state.campaign_loss_limit_usd)
                    .is_ok_and(|value| policy.3 == value)
        });
        json!({
            "schema":"okx.native-policy-sql-receipt/v1",
            "source":"account_sql",
            "revision":policy.0,
            "paused":policy.1,
            "campaign_id":policy.2,
            "campaign_limit_usd":exact(policy.3),
            "matches_governance":matched,
            "state":if matched {"applied"} else {"mismatch"},
        })
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
            TradeRequest::Capabilities {} => {
                let access = self
                    .diagnostic_access(&r.configuration.account_key.wire())
                    .await;
                let snapshot = r.latest_snapshot.read().await.clone();
                let fresh = snapshot
                    .as_ref()
                    .and_then(|v| chrono::DateTime::parse_from_rfc3339(&v.source_received_at).ok())
                    .is_some_and(|t| {
                        crate::config::evidence_fresh(
                            t.timestamp_millis(),
                            now(),
                            crate::config::ACCOUNT_MAX_AGE_MS,
                        )
                    });
                // A live capabilities readback is independently sourced from
                // the native account-local SQLite owner. It proves the current
                // governance revision was physically applied; the cached Worker
                // snapshot alone is insufficient evidence.
                let sql = r
                    .execution
                    .store
                    .call(deadline(), |store| store.policy())
                    .await;
                let governance = self.governance.read().await;
                let sql_receipt = match sql {
                    Ok(policy) => Self::sql_policy_receipt(
                        &policy,
                        governance.as_ref(),
                        &r.configuration.account_key.wire(),
                    ),
                    Err(_) => json!({
                        "schema":"okx.native-policy-sql-receipt/v1",
                        "source":"account_sql",
                        "state":"unavailable",
                        "reason_code":"NATIVE_SQL_POLICY_UNAVAILABLE",
                        "matches_governance":false
                    }),
                };
                let policy_applied = sql_receipt["matches_governance"] == true;
                drop(governance);
                Ok(
                    json!({"schema":"okx.trade.capabilities/v1","account":r.configuration.account_key.wire(),
                    "order_types":["limit","market","post_only","ioc","fok"],
                    "operations":["open","add","reduce","close","replace"],
                    "protection":["stop_price","stop_distance_bps"],
                    "account_level":snapshot.as_ref().map(|s|s.account_level.as_str()),
                    "position_mode":snapshot.as_ref().map(|s|s.position_mode.as_str()),
                    "api_key_permissions":snapshot.as_ref().map(|s|&s.api_key_permissions),
                    "account_source_received_at":snapshot.as_ref().map(|s|s.source_received_at.as_str()),
                    "account_source_fresh":fresh,"governance":access,
                    "native_sql_policy":sql_receipt,
                    "quality":if fresh&&policy_applied{"complete"}else{"partial"},
                    "ready_for_post":false,
                    "reason_code":if !policy_applied {"NATIVE_SQL_POLICY_NOT_APPLIED"} else {"VENUE_PER_INSTRUMENT_ADMISSION_REQUIRES_TRADE_PLAN"}}),
                )
            }
            TradeRequest::Preflight {} => {
                let access = self
                    .diagnostic_access(&r.configuration.account_key.wire())
                    .await;
                let identity = r.source_identity_ready().await;
                let snapshot = r.latest_snapshot.read().await.clone();
                let fresh = snapshot
                    .as_ref()
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s.source_received_at).ok())
                    .is_some_and(|t| {
                        crate::config::evidence_fresh(
                            t.timestamp_millis(),
                            now(),
                            crate::config::ACCOUNT_MAX_AGE_MS,
                        )
                    });
                let (financial, stops) = r
                    .execution
                    .store
                    .call(deadline(), |s| {
                        Ok((s.financial_risk(now(), None)?, s.stops()?.len()))
                    })
                    .await
                    .map_err(err)?;
                let budget = self.risk_summary().await;
                let mut blockers = Vec::new();
                if access["state"] != "configured"
                    || access["enabled"] != true
                    || access["explicit_pause"] == true
                {
                    blockers.push("ACCOUNT_ACCESS_UNAVAILABLE");
                }
                if !identity || !fresh {
                    blockers.push("WAITING_ACCOUNT_EVIDENCE");
                }
                if !financial.nav_complete {
                    blockers.push("NAV_SOURCE_INCOMPLETE");
                }
                if stops > 0 {
                    blockers.push("ACCOUNT_STOP");
                }
                if budget.is_err() {
                    blockers.push("CENTRAL_BUDGET_UNAVAILABLE");
                }
                Ok(json!({"schema":"okx.trade.preflight/v1",
                    "account":r.configuration.account_key.wire(),
                    "state":if blockers.is_empty(){"not_evaluated"}else{"blocked"},
                    "ready_for_new_risk":false,
                    "reason_code":if blockers.is_empty(){"FULL_ADMISSION_REQUIRES_EXACT_TRADE_PLAN"}else{"READINESS_BLOCKERS"},
                    "blockers":blockers,"account_source_fresh":fresh,
                    "identity_verified":identity,"financial_risk":financial,
                    "stops":stops,"central_budget":budget.ok(),
                    "side_effects":"none","quality":"partial"}))
            }
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


#[cfg(test)]
mod governance_sql_proof_tests {
    use super::*;
    use okx_protocol::account_governance::CentralGovernanceSnapshot;

    fn fixture() -> CentralGovernanceSnapshot {
        serde_json::from_value(json!({
            "state":{
                "schema":"okx.governance/v3","revision":2,
                "installation_id":"installation_test",
                "campaign_id":format!("sha256:{}", "a".repeat(64)),
                "campaign_loss_limit_usd":"10",
                "max_total_modelled_loss_usdt":"5",
                "accounts":[{
                    "id":"primary","label":"Primary","venue":"okx",
                    "environment":"real","uid_fingerprint":"b".repeat(64),
                    "identity_verified":true,"enabled":true,"explicit_pause":false,
                    "policy_revision":1,
                    "account_key":format!("okx:real:{}", "b".repeat(64)),
                    "limits":{
                        "max_modelled_loss_usdt":"5","max_order_loss_usdt":"2",
                        "max_notional_usdt":"100","max_leverage":"20",
                        "max_daily_realized_loss_usdt":"8","max_drawdown_ratio":"0.2",
                        "allowed_instruments":["BTC-USDT-SWAP"]
                    },"strategies":[]
                }]
            },
            "issued_at_ms":1792910000000_u64,"session_generation":673_u64
        })).unwrap()
    }
    #[test]
    fn physical_policy_sql_receipt_matches_account_revision_and_fails_closed() {
        let governance = fixture();
        assert!(governance.valid());
        let key = format!("okx:real:{}", "b".repeat(64));
        let original = (
            2_i64,false,format!("sha256:{}", "a".repeat(64)),
            Decimal::from(10),Decimal::ZERO
        );
        let receipt = TradeService::sql_policy_receipt(&original,Some(&governance),&key);
        assert_eq!(receipt["state"],"applied");
        assert_eq!(receipt["revision"],2);
        assert_eq!(receipt["matches_governance"],true);
        for failed in [
            (1_i64,false,original.2.clone(),Decimal::from(10),Decimal::ZERO),
            (2_i64,true,original.2.clone(),Decimal::from(10),Decimal::ZERO),
            (2_i64,false,original.2.clone(),Decimal::from(20),Decimal::ZERO),
            (2_i64,false,"sha256:wrong".into(),Decimal::from(10),Decimal::ZERO),
        ] {
            assert_eq!(
                TradeService::sql_policy_receipt(&failed,Some(&governance),&key)["matches_governance"],
                false
            );
        }
        assert_eq!(
            TradeService::sql_policy_receipt(&original,Some(&governance),"wrong")["matches_governance"],
            false
        );
        assert_eq!(
            TradeService::sql_policy_receipt(&original,None,&key)["matches_governance"],
            false
        );
    }
}

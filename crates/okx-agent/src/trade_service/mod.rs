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
    collections::HashMap,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock};
mod entry_budget;
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
    /// Non-authoritative polling schedule only; SQLite and RiskBook retain
    /// all decision, reservation, order, and financial state.
    recovery_schedule: Mutex<RecoverySchedule>,
}

#[derive(Default)]
struct RecoverySchedule {
    pages: HashMap<String, usize>,
    attempts: HashMap<(String, String), RecoveryAttempt>,
}
struct RecoveryAttempt {
    count: u8,
    due: Instant,
    incident_emitted: bool,
}
const RECOVERY_DELAYS_SECONDS: [u64; 6] = [1, 2, 4, 8, 16, 30];
// One maintenance turn reads at most 16 native intents per account.
// The SQL API itself supports bounded pages up to 100, without full scans.
const RECOVERY_BATCH: usize = 16;
impl RecoverySchedule {
    fn page(&self, account: &str) -> usize {
        *self.pages.get(account).unwrap_or(&0)
    }

    fn advance_page(&mut self, account: &str, offset: usize, len: usize) {
        self.pages.insert(
            account.to_owned(),
            if len == RECOVERY_BATCH && offset < 100_000 - RECOVERY_BATCH {
                offset + RECOVERY_BATCH
            } else {
                0
            },
        );
    }

    fn due(&mut self, account: &str, intent: &str, clock: Instant) -> bool {
        let key = (account.to_owned(), intent.to_owned());
        // Scheduling metadata is bounded; a pruned entry only causes an
        // earlier safe readback, never another risk grant or venue POST.
        if self.attempts.len() >= 16_384 && !self.attempts.contains_key(&key) {
            self.attempts.retain(|_, value| value.due > clock);
            if self.attempts.len() >= 16_384 {
                self.attempts.clear();
            }
        }
        let attempt = self.attempts.entry(key).or_insert(RecoveryAttempt {
            count: 0,
            due: clock,
            incident_emitted: false,
        });
        if clock < attempt.due {
            return false;
        }
        let seconds = RECOVERY_DELAYS_SECONDS[usize::from(attempt.count.min(5))];
        // Stable bounded jitter distributes same-second account recovery
        // across workers without requiring a new RNG or a persistence layer.
        let key_digest = Sha256::digest(format!("{account}:{intent}").as_bytes());
        let jitter_ms = u64::from(key_digest[0]) % 251;
        attempt.due = clock + Duration::from_millis(seconds * 1_000 + jitter_ms);
        attempt.count = attempt.count.saturating_add(1);
        true
    }

    fn finish(&mut self, account: &str, intent: &str) {
        self.attempts
            .remove(&(account.to_owned(), intent.to_owned()));
    }

    fn overdue_incident(
        &mut self,
        account: &str,
        intent: &str,
        deadline_ms: i64,
        now_ms: i64,
    ) -> bool {
        if now_ms.saturating_sub(deadline_ms) < 30_000 {
            return false;
        }
        let Some(attempt) = self
            .attempts
            .get_mut(&(account.to_owned(), intent.to_owned()))
        else {
            return false;
        };
        if attempt.incident_emitted {
            return false;
        }
        attempt.incident_emitted = true;
        true
    }
}
#[cfg(test)]
mod recovery_schedule_tests {
    use super::*;
    #[test]
    fn bounded_exponential_backoff_and_one_incident() {
        let mut r = RecoverySchedule::default();
        let start = Instant::now();
        assert!(r.due("account", "intent", start));
        assert!(!r.due("account", "intent", start));
        let mut tick = start;
        for seconds in RECOVERY_DELAYS_SECONDS {
            tick += Duration::from_millis(seconds * 1_000 + 251);
            assert!(r.due("account", "intent", tick));
        }
        assert!(!r.overdue_incident("account", "intent", 50_000, 79_999));
        assert!(r.overdue_incident("account", "intent", 50_000, 80_000));
        assert!(!r.overdue_incident("account", "intent", 50_000, 90_000));
        r.advance_page("account", 0, RECOVERY_BATCH);
        assert_eq!(r.page("account"), RECOVERY_BATCH);
        r.advance_page("account", RECOVERY_BATCH, 3);
        assert_eq!(r.page("account"), 0);
        r.finish("account", "intent");
        assert!(r.due("account", "intent", tick));
    }
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
            recovery_schedule: Mutex::new(RecoverySchedule::default()),
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
    /// A preview observes an already-applied governance revision; only the
    /// policy delivery and execution owners may write policy to account SQL.
    async fn preview_authority(
        &self,
        r: &AccountResources,
        supplied: Option<&CentralGovernanceSnapshot>,
    ) -> Result<(CentralGovernanceSnapshot, AccountControl), String> {
        let state = self
            .governance
            .read()
            .await
            .clone()
            .ok_or("AUTHORITY_UNAVAILABLE")?;
        if supplied.is_some_and(|proof| !proof.valid() || proof.state != state.state) {
            return Err("PREVIEW_AUTHORITY_NOT_APPLIED".into());
        }
        let account = state
            .state
            .accounts
            .iter()
            .find(|a| a.account_key == r.configuration.account_key.wire())
            .cloned()
            .ok_or("ACCOUNT_ACCESS_MISSING")?;
        let policy = r
            .execution
            .store
            .call(deadline(), |store| store.policy())
            .await
            .map_err(err)?;
        if Self::sql_policy_receipt(&policy, Some(&state), &r.configuration.account_key.wire())["matches_governance"]
            != true
        {
            return Err("NATIVE_SQL_POLICY_NOT_APPLIED".into());
        }
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
                && policy.1 != account.access_enabled()
                && policy.2 == snapshot.state.campaign_id
                && dec(&snapshot.state.campaign_loss_limit_usd).is_ok_and(|value| policy.3 == value)
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
                // Query the actual Executor key, not the Observer-only account snapshot.
                // Never return key material, passphrase, or exchange UID.
                let executor = match r.execution.account.config().await {
                    Ok(config) => {
                        let permissions = config
                            .perm
                            .split(',')
                            .map(str::trim)
                            .filter(|p| !p.is_empty())
                            .collect::<Vec<_>>();
                        let read = permissions.contains(&"read_only");
                        let trade = permissions.contains(&"trade");
                        let withdraw = permissions.contains(&"withdraw");
                        let uid_match = okx_api::account_uid_fingerprint(&config.uid)
                            == r.configuration.account_key.verified_uid;
                        json!({"state":if permissions.len()==2 && read && trade && !withdraw && uid_match
                                  {"verified"}else{"blocked"},
                               "source":"executor_okx_account_config","as_of_ms":now(),
                               "read":read,"trade":trade,"withdraw":withdraw,
                               "uid_match":uid_match})
                    }
                    Err(_) => json!({"state":"unavailable","source":"executor_okx_account_config",
                                     "reason_code":"EXECUTOR_CONFIG_UNAVAILABLE"}),
                };
                let executor_verified = executor["state"] == "verified";
                Ok(
                    json!({"schema":"okx.trade.capabilities/v1","account":r.configuration.account_key.wire(),
                    "order_types":["limit","market","post_only","ioc","fok"],
                    "operations":["open","add","reduce","close","replace"],
                    "protection":["stop_price","stop_distance_bps"],
                    "account_level":snapshot.as_ref().map(|s|s.account_level.as_str()),
                    "position_mode":snapshot.as_ref().map(|s|s.position_mode.as_str()),
                    "api_key_permissions":snapshot.as_ref().map(|s|&s.api_key_permissions),
                    "observer":{"source":"observer_account_snapshot",
                                "permissions":snapshot.as_ref().map(|s|&s.api_key_permissions),
                                "source_received_at":snapshot.as_ref().map(|s|s.source_received_at.as_str())},
                    "executor":executor,
                    "account_source_received_at":snapshot.as_ref().map(|s|s.source_received_at.as_str()),
                    "account_source_fresh":fresh,"governance":access,
                    "native_sql_policy":sql_receipt,
                    "quality":if fresh&&policy_applied&&executor_verified{"complete"}else{"partial"},
                    "admission_state":"not_evaluated",
                    "ready_for_post":false,
                    "reason_code":if !policy_applied {"NATIVE_SQL_POLICY_NOT_APPLIED"}
                      else if !executor_verified {"EXECUTOR_PERMISSION_NOT_VERIFIED"}
                      else {"VENUE_PER_INSTRUMENT_ADMISSION_REQUIRES_TRADE_PLAN"}}),
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
                let (a, c) = self.preview_authority(&r, governance).await?;
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
                        false,
                    )
                    .await?;
                // A manual trade uses account capital. For a separately funded
                // strategy, a preview exposes missing capital without granting
                // permission to post. Neither path is an execution admission.
                let independent_capital =
                    plan::requires_independent_strategy_nav(strategy_instance);
                let binding = strategy_instance.clone();
                let instance_nav_complete = if independent_capital {
                    r.execution
                        .store
                        .call(deadline(), move |store| {
                            if store.instances()?.iter().any(|i| i.instance_id == binding) {
                                Ok(Some(
                                    store.financial_risk(now(), Some(&binding))?.nav_complete,
                                ))
                            } else {
                                Ok(None)
                            }
                        })
                        .await
                        .map_err(err)?
                } else {
                    None
                };
                let instance_nav_blocked = instance_nav_complete == Some(false);
                // Preview uses the same price-bounded, fee-inclusive cost model as
                // native admission; do not silently suppress missing fee evidence.
                let costs = if matches!(
                    operation,
                    TradeOperation::Open | TradeOperation::Add | TradeOperation::Replace
                ) {
                    let rules = self
                        .public_ws
                        .instrument_rules(instrument)
                        .await
                        .ok_or("INSTRUMENT_RULES_UNAVAILABLE")?;
                    let fees = r.observer.fee_schedule(&rules).await.map_err(err)?;
                    let source = r.observer.snapshot().await.map_err(err)?;
                    let conversion = Self::usd_conversion(&source)?;
                    let (entry, fee_rate) = entry_budget::verified_known_entry_quote(
                        &plan,
                        instrument,
                        &rules,
                        &fees,
                        &conversion,
                        &source.account_uid_fingerprint,
                        &r.configuration.account_key.verified_uid,
                        now(),
                    )?;
                    let fx = dec(&conversion.usd_per_usdt)?;
                    let entry_ceiling = entry_budget::enforced_entry_ceiling(
                        true,
                        c.environment == okx_protocol::account_governance::AccountEnvironment::Real,
                        c.limits.max_entry_usd.as_deref().map(dec).transpose()?,
                        order.max_entry_usd.as_deref().map(dec).transpose()?,
                    )?;
                    // Known entry fee/price is NOT an all-in cost proof. Report
                    // exact remaining capacity for still-unproven immediate
                    // costs, rather than treating them as zero.
                    let unproven_headroom =
                        entry_budget::unproven_entry_cost_headroom(entry.total_usd, entry_ceiling)?;
                    let proof = entry_budget::EntryCostProof::source_valued_candidate(
                        entry.clone(),
                        &conversion,
                        now(),
                    )?;
                    let all_in_upper = proof.proven_upper_usd()?;
                    let missing = proof.missing_reasons();
                    json!({"price_bound_usdt":exact(plan.limit_price),
                           "notional_usdt":exact(entry.notional_usdt),
                           "entry_fee_usdt":exact(entry.fee_usdt),
                           "entry_total_usdt":exact(entry.total_usdt),
                           "entry_total_usd":exact(entry.total_usd),
                           "usd_per_usdt":exact(fx),
                           "admission_fx_snapshot_state":"PENDING_INDEPENDENT_COST_SOURCES",
                           "entry_total_usd_basis":"QUOTED_FX_PREVIEW_NOT_FROZEN_ADMISSION",
                           "fee_rate":exact(fee_rate),
                           "fee_model_currency":"USDT",
                           "actual_fee_settlement_currency":null,
                           "actual_fee_settlement_currency_state":"UNPROVEN",
                           "fee_rounding":"CEIL_0.000001",
                           "entry_cap_usd":entry_ceiling.map(exact),
                           "account_entry_cap_usd":c.limits.max_entry_usd,
                           "price_enforcement":"VENUE_LIMIT_OR_IOC_FOK",
                           "remaining_cap_for_unproven_costs_usd":unproven_headroom.map(exact),
                           "all_in_entry_cost_proven":all_in_upper.is_some(),
                           "entry_budget_upper_reporting":all_in_upper.map(exact),
                           "entry_cost_proof_schema":"okx.entry-cost-proof/v1",
                           "entry_cost_reason_codes":missing,
                           "other_costs":"UNAVAILABLE_NOT_INCLUDED"})
                } else {
                    json!(null)
                };
                // This is a fresh exchange-side census, not a reservation or
                // authorization to POST. Any gap is reported, never guessed flat.
                let census = if r.configuration.account_key.environment
                    == okx_protocol::accounts::Environment::Real
                {
                    match self.census(&a).await {
                        Ok(proof) => json!({"state":"verified","source":"native_risk_census",
                                            "generated_at_ms":proof.generated_at_ms,
                                            "complete":proof.complete}),
                        Err(reason) => json!({"state":"blocked","reason_code":reason}),
                    }
                } else {
                    json!({"state":"not_evaluated"})
                };
                // The Observer's ordinary positions/orders are distinct from
                // pending algos, open UNKNOWN intents and central reservations.
                // Never equate zero ordinary rows with a proved flat account.
                let exposure = r.observer.snapshot().await.map_err(err)?;
                let exposure_fresh =
                    chrono::DateTime::parse_from_rfc3339(&exposure.source_received_at)
                        .ok()
                        .is_some_and(|t| {
                            crate::config::evidence_fresh(
                                t.timestamp_millis(),
                                now(),
                                crate::config::ACCOUNT_MAX_AGE_MS,
                            )
                        })
                        && exposure.account_uid_fingerprint
                            == r.configuration.account_key.verified_uid;
                let visible_positions = plan::visible_nonzero_position_count(
                    exposure.positions.iter().map(|p| p.position.as_str()),
                )?;
                let account_exposure = json!({
                    "source":"observer_account_snapshot",
                    "state":if exposure_fresh {"partial"} else {"stale"},
                    "source_received_at":exposure.source_received_at,
                    "nonzero_positions":visible_positions,
                    "pending_ordinary_orders":exposure.pending_orders.len(),
                    "pending_algo_orders":null,
                    "unknown_intents":null,
                    "flat_verified":false,
                    "reason_code":"ALGO_INTENTS_AND_RESERVATIONS_REQUIRE_INDEPENDENT_READBACK"
                });
                Ok(completed(
                    json!({"plan":plan,"entry_costs":costs,"access":c.access_enabled(),"policy_revision":a.state.revision,
                    "admission":"preview_only","side_effects":"none","census":census,
                    "account_exposure":account_exposure,
                    "financial_admission":{
                        "state":if instance_nav_blocked {"blocked"} else {"not_evaluated"},
                        "reason_code":if instance_nav_blocked {Some("INSTANCE_NAV_INCOMPLETE")} else {None},
                        "ready_for_post":false,
                        "instance_nav_complete":instance_nav_complete,
                        "capital_scope":if independent_capital {"strategy"} else {"account"}
                    }}),
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
        }))
        .unwrap()
    }
    #[test]
    fn physical_policy_sql_receipt_matches_account_revision_and_fails_closed() {
        let governance = fixture();
        assert!(governance.valid());
        let key = format!("okx:real:{}", "b".repeat(64));
        let original = (
            2_i64,
            false,
            format!("sha256:{}", "a".repeat(64)),
            Decimal::from(10),
            Decimal::ZERO,
        );
        let receipt = TradeService::sql_policy_receipt(&original, Some(&governance), &key);
        assert_eq!(receipt["state"], "applied");
        assert_eq!(receipt["revision"], 2);
        assert_eq!(receipt["matches_governance"], true);
        for failed in [
            (
                1_i64,
                false,
                original.2.clone(),
                Decimal::from(10),
                Decimal::ZERO,
            ),
            (
                2_i64,
                true,
                original.2.clone(),
                Decimal::from(10),
                Decimal::ZERO,
            ),
            (
                2_i64,
                false,
                original.2.clone(),
                Decimal::from(20),
                Decimal::ZERO,
            ),
            (
                2_i64,
                false,
                "sha256:wrong".into(),
                Decimal::from(10),
                Decimal::ZERO,
            ),
        ] {
            assert_eq!(
                TradeService::sql_policy_receipt(&failed, Some(&governance), &key)["matches_governance"],
                false
            );
        }
        assert_eq!(
            TradeService::sql_policy_receipt(&original, Some(&governance), "wrong")["matches_governance"],
            false
        );
        assert_eq!(
            TradeService::sql_policy_receipt(&original, None, &key)["matches_governance"],
            false
        );
    }
}

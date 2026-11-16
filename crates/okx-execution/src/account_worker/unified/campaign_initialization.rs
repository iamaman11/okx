//! Full authorized policy enters the existing writer; no opening/source grant.
use super::UnifiedFinanceHandle;
use crate::{AccountStore, StoreError, StoreResult};
use okx_kernel::risk::money::RiskAmount;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeCampaignStrategyPolicy {
    pub strategy_id: String,
    pub limit_usdt: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeCampaignAccountPolicy {
    pub account_key: String,
    pub paused: bool,
    pub limit_usdt: String,
    pub order_limit_usdt: String,
    pub entry_limit_usd: String,
    pub strategies: Vec<NativeCampaignStrategyPolicy>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeCampaignPolicy {
    pub campaign_id: String,
    pub policy_revision: u64,
    pub generation: u64,
    pub global_limit_usdt: String,
    pub loss_limit_usd: String,
    pub entry_limit_usd: String,
    pub accounts: Vec<NativeCampaignAccountPolicy>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeCampaignPolicyReceipt {
    pub policy_revision: u64,
    pub book_sequence: u64,
    pub replayed: bool,
}

impl NativeCampaignPolicy {
    pub(crate) fn normalize(&mut self) -> StoreResult<()> {
        let bad = || StoreError::Invalid("NATIVE_CAMPAIGN_POLICY_INVALID");
        let money = |v: &mut String| -> StoreResult<RiskAmount> {
            let amount = RiskAmount::parse_positive(v).map_err(|_| bad())?;
            *v = amount.canonical().map_err(|_| bad())?;
            Ok(amount)
        };
        let hex = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        if !self.campaign_id.strip_prefix("sha256:").is_some_and(hex)
            || self.policy_revision == 0
            || self.policy_revision > 9_007_199_254_740_991
            || self.generation == 0
            || self.generation > 9_007_199_254_740_991
            || self.accounts.is_empty()
            || self.accounts.len() > 32
            || !self
                .accounts
                .windows(2)
                .all(|w| w[0].account_key < w[1].account_key)
        {
            return Err(bad());
        }
        money(&mut self.global_limit_usdt)?;
        money(&mut self.loss_limit_usd)?;
        money(&mut self.entry_limit_usd)?;
        for a in &mut self.accounts {
            if !a.account_key.strip_prefix("okx:real:").is_some_and(hex)
                || a.strategies.is_empty()
                || a.strategies.len() > 33
                || !a
                    .strategies
                    .windows(2)
                    .all(|w| w[0].strategy_id < w[1].strategy_id)
            {
                return Err(bad());
            }
            let cap = money(&mut a.limit_usdt)?;
            let order = money(&mut a.order_limit_usdt)?;
            money(&mut a.entry_limit_usd)?;
            if order > cap {
                return Err(bad());
            }
            for st in &mut a.strategies {
                if !(2..=64).contains(&st.strategy_id.len())
                    || !st.strategy_id.as_bytes()[0].is_ascii_alphabetic()
                    || !st
                        .strategy_id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                    || money(&mut st.limit_usdt)? > cap
                {
                    return Err(bad());
                }
            }
            if !a
                .strategies
                .iter()
                .any(|s| s.strategy_id == "manual" && s.limit_usdt == a.limit_usdt)
            {
                return Err(bad());
            }
        }
        Ok(())
    }
    pub(crate) fn digest(&self) -> StoreResult<String> {
        Ok(format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(self)
                    .map_err(|_| StoreError::Invalid("NATIVE_CAMPAIGN_POLICY_ENCODING"))?
            )
        ))
    }
}
impl UnifiedFinanceHandle {
    pub async fn apply_campaign_policy(
        &self,
        deadline: Instant,
        mut policy: NativeCampaignPolicy,
    ) -> StoreResult<NativeCampaignPolicyReceipt> {
        policy.normalize()?;
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            AccountStore::inspect_finance_installation(db, &installation)?;
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            // A transport reconnect is not a new financial epoch. Bind the
            // canonical replay identity to the original durable campaign.
            let generation: Option<i64> = tx
                .query_row(
                    "SELECT generation FROM risk_meta WHERE installation_id=?1 AND campaign_id=?2",
                    rusqlite::params![installation, policy.campaign_id],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(generation) = generation {
                policy.generation = u64::try_from(generation)
                    .ok()
                    .filter(|g| *g > 0)
                    .ok_or(StoreError::Invalid("NATIVE_CAMPAIGN_GENERATION_INVALID"))?;
            }
            let result =
                AccountStore::apply_native_campaign_policy_sql(&tx, &installation, &policy)?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(result)
        })
        .await
    }
}
#[cfg(test)]
mod tests;

//! Clean-install persistent account access shared with Cloudflare.
//! Configuration does NOT constitute a native Real exchange-post permit.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const GOVERNANCE_SCHEMA: &str = "okx.governance/v3";
const SCALE: u128 = 1_000_000_000;

fn units(text: &str) -> Option<u128> {
    if text.is_empty() {
        return None;
    }
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    if whole.is_empty()
        || whole.len() > 15
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || (whole.len() > 1 && whole.starts_with('0'))
        || text.ends_with('.')
        || fraction.len() > 9
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let integer = whole.parse::<u128>().ok()?;
    let fraction_units = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<u128>().ok()? * 10_u128.pow(9 - fraction.len() as u32)
    };
    integer.checked_mul(SCALE)?.checked_add(fraction_units)
}

fn positive(text: &str) -> Option<u128> {
    units(text).filter(|value| *value > 0)
}

fn optional_positive(value: &Option<String>) -> bool {
    value.as_ref().is_none_or(|text| positive(text).is_some())
}

fn ratio(value: &Option<String>) -> bool {
    value
        .as_ref()
        .is_none_or(|text| units(text).is_some_and(|number| number <= SCALE))
}

fn lower_sha256(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn token(text: &str) -> bool {
    (2..=64).contains(&text.len())
        && text
            .as_bytes()
            .first()
            .is_some_and(|byte| byte.is_ascii_alphabetic())
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountLimits {
    pub max_modelled_loss_usdt: String,
    pub max_order_loss_usdt: String,
    pub max_notional_usdt: Option<String>,
    pub max_leverage: Option<String>,
    pub max_daily_realized_loss_usdt: Option<String>,
    pub max_drawdown_ratio: Option<String>,
    pub allowed_instruments: Vec<String>,
}

impl AccountLimits {
    pub fn valid(&self) -> bool {
        let (Some(account), Some(order)) = (
            positive(&self.max_modelled_loss_usdt),
            positive(&self.max_order_loss_usdt),
        ) else {
            return false;
        };
        account >= order
            && optional_positive(&self.max_notional_usdt)
            && optional_positive(&self.max_leverage)
            && optional_positive(&self.max_daily_realized_loss_usdt)
            && ratio(&self.max_drawdown_ratio)
            && self.allowed_instruments.len() <= 32
            && self.allowed_instruments.iter().all(|instrument| {
                (3..=64).contains(&instrument.len())
                    && instrument.bytes().all(|byte| {
                        byte.is_ascii_uppercase()
                            || byte.is_ascii_digit()
                            || byte == b'-'
                            || byte == b'_'
                    })
            })
            && self
                .allowed_instruments
                .iter()
                .collect::<BTreeSet<_>>()
                .len()
                == self.allowed_instruments.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyLimits {
    pub id: String,
    pub enabled: bool,
    pub allocated_capital_usdt: String,
    pub max_modelled_loss_usdt: String,
    pub max_daily_realized_loss_usdt: Option<String>,
    pub max_drawdown_ratio: Option<String>,
}

impl StrategyLimits {
    pub fn valid(&self) -> bool {
        token(&self.id)
            && positive(&self.allocated_capital_usdt).is_some_and(|capital| {
                positive(&self.max_modelled_loss_usdt).is_some_and(|risk| risk <= capital)
            })
            && optional_positive(&self.max_daily_realized_loss_usdt)
            && ratio(&self.max_drawdown_ratio)
            && (!self.enabled
                || (self.max_daily_realized_loss_usdt.is_some()
                    && self.max_drawdown_ratio.is_some()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AccountEnvironment {
    Real,
    Demo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountControl {
    pub id: String,
    pub label: String,
    pub venue: String,
    pub environment: AccountEnvironment,
    pub uid_fingerprint: String,
    pub identity_verified: bool,
    pub enabled: bool,
    pub explicit_pause: bool,
    pub policy_revision: u64,
    pub account_key: String,
    pub limits: AccountLimits,
    pub strategies: Vec<StrategyLimits>,
}

impl AccountControl {
    pub fn access_enabled(&self) -> bool {
        self.identity_verified && self.enabled && !self.explicit_pause
    }
    pub fn valid(&self) -> bool {
        let venue_valid = (2..=32).contains(&self.venue.len())
            && self.venue.starts_with(|c: char| c.is_ascii_lowercase())
            && self.venue.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
            });
        if !token(&self.id)
            || self.label.len() > 80
            || !venue_valid
            || !lower_sha256(&self.uid_fingerprint)
            || (self.enabled && !self.identity_verified)
            || self.policy_revision == 0
            || self.account_key
                != format!(
                    "{}:{}:{}",
                    self.venue,
                    match self.environment {
                        AccountEnvironment::Real => "real",
                        AccountEnvironment::Demo => "demo",
                    },
                    self.uid_fingerprint
                )
            || !self.limits.valid()
            || self.strategies.len() > 32
        {
            return false;
        }
        if self.enabled
            && self.environment == AccountEnvironment::Real
            && (self.limits.max_notional_usdt.is_none()
                || self.limits.max_leverage.is_none()
                || self.limits.max_daily_realized_loss_usdt.is_none()
                || self.limits.max_drawdown_ratio.is_none()
                || self.limits.allowed_instruments.is_empty())
        {
            return false;
        }
        let mut ids = BTreeSet::new();
        let mut total = 0_u128;
        for strategy in &self.strategies {
            if !strategy.valid() || !ids.insert(&strategy.id) {
                return false;
            }
            if strategy.enabled {
                let Some(value) = positive(&strategy.max_modelled_loss_usdt) else {
                    return false;
                };
                let Some(added) = total.checked_add(value) else {
                    return false;
                };
                total = added;
            }
        }
        positive(&self.limits.max_modelled_loss_usdt).is_some_and(|cap| total <= cap)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountGovernance {
    pub schema: String,
    pub revision: u64,
    pub installation_id: String,
    pub campaign_id: String,
    pub campaign_loss_limit_usd: String,
    pub max_total_modelled_loss_usdt: String,
    pub accounts: Vec<AccountControl>,
}

impl AccountGovernance {
    pub fn valid(&self) -> bool {
        if self.schema != GOVERNANCE_SCHEMA
            || !token(&self.installation_id)
            || !self.campaign_id.starts_with("sha256:")
            || !lower_sha256(&self.campaign_id[7..])
            || positive(&self.campaign_loss_limit_usd).is_none()
            || positive(&self.max_total_modelled_loss_usdt).is_none()
            || self.accounts.len() > 32
        {
            return false;
        }
        let mut ids = BTreeSet::new();
        let mut identities = BTreeSet::new();
        for account in &self.accounts {
            if !account.valid()
                || account.policy_revision > self.revision
                || !ids.insert(&account.id)
            {
                return false;
            }
            if !identities.insert((
                &account.venue,
                account.environment as u8,
                &account.uid_fingerprint,
            )) {
                return false;
            }
        }
        true
    }

    pub fn has_real_access(&self) -> bool {
        self.accounts
            .iter()
            .any(|a| a.environment == AccountEnvironment::Real && a.access_enabled())
    }

    /// Never make native POST admission from configuration alone. This must
    /// be replaced by fresh authenticated reservation AND venue risk proof.
    pub const fn native_new_risk_admitted(&self) -> bool {
        false
    }
}

/// A short-lived snapshot forwarded ONLY by the authenticated RuntimeSession.
/// It is a delivery/UID scope proof, not a reservation or POST permit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CentralGovernanceSnapshot {
    pub state: AccountGovernance,
    pub issued_at_ms: u64,
    pub session_generation: u64,
    /// Worker request-scoped ping challenge; validated and consumed by the
    /// authenticated native transport using its own monotonic clock.
    /// Optional only for decoding older read-only snapshots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_challenge: Option<String>,
}
impl CentralGovernanceSnapshot {
    pub fn valid(&self) -> bool {
        self.state.valid() && self.issued_at_ms > 0 && self.session_generation > 0
    }
    /// UID delivery checked after native transport consumed the exact,
    /// non-replayable Worker ping nonce within its own monotonic TTL.
    /// This scope remains only a delivery grant; risk reservation is separate.
    pub fn authorizes_challenge_bound_delivery(
        &self,
        uid: &str,
        generation: u64,
        challenge_verified: bool,
    ) -> bool {
        challenge_verified
            && self.valid()
            && self.delivery_challenge.is_some()
            && self.session_generation == generation
            && self.state.revision > 0
            && self.state.accounts.iter().any(|a| {
                a.venue == "okx"
                    && a.environment == AccountEnvironment::Real
                    && a.access_enabled()
                    && a.identity_verified
                    && a.uid_fingerprint == uid
            })
    }

    pub fn authorizes_delivery(&self, uid: &str, now_ms: u64, generation: u64) -> bool {
        self.valid()
            && self.session_generation == generation
            && self.issued_at_ms <= now_ms
            && now_ms.saturating_sub(self.issued_at_ms) <= 15_000
            && self.state.revision > 0
            && self.state.accounts.iter().any(|a| {
                a.venue == "okx"
                    && a.environment == AccountEnvironment::Real
                    && a.access_enabled()
                    && a.identity_verified
                    && a.uid_fingerprint == uid
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn clean_governance_does_not_grant_post() {
        let state = AccountGovernance {
            schema: GOVERNANCE_SCHEMA.into(),
            revision: 0,
            installation_id: "installation_test".into(),
            campaign_id: format!("sha256:{}", "a".repeat(64)),
            campaign_loss_limit_usd: "20".into(),
            max_total_modelled_loss_usdt: "5".into(),
            accounts: vec![],
        };
        assert!(state.valid());
        assert!(!state.has_real_access());
        assert!(!state.native_new_risk_admitted());
        let mut wire = serde_json::to_value(&state).unwrap();
        wire["trading_requested"] = true.into();
        assert!(serde_json::from_value::<AccountGovernance>(wire).is_err());
    }
}

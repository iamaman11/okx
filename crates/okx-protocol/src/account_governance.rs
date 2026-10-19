//! Typed account-governance v2 contract shared with Cloudflare.
//! Configuration does NOT constitute a native Real exchange-post permit.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const GOVERNANCE_SCHEMA_V2: &str = "okx.governance/v2";
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AuthorizationStatus {
    Unverified,
    Active,
    Revoked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorAuthorizationV2 {
    pub id: String,
    pub status: AuthorizationStatus,
    pub ceiling_usdt: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountLimitsV2 {
    pub max_modelled_loss_usdt: String,
    pub max_order_loss_usdt: String,
    pub max_notional_usdt: Option<String>,
    pub max_leverage: Option<String>,
    pub max_daily_realized_loss_usdt: Option<String>,
    pub max_drawdown_ratio: Option<String>,
    pub allowed_instruments: Vec<String>,
}

impl AccountLimitsV2 {
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
pub struct StrategyLimitsV2 {
    pub id: String,
    pub enabled: bool,
    pub allocated_capital_usdt: String,
    pub max_modelled_loss_usdt: String,
    pub max_daily_realized_loss_usdt: Option<String>,
    pub max_drawdown_ratio: Option<String>,
}

impl StrategyLimitsV2 {
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
pub enum AccountEnvironmentV2 {
    Real,
    Demo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountControlV2 {
    pub id: String,
    pub label: String,
    pub venue: String,
    pub environment: AccountEnvironmentV2,
    pub uid_fingerprint: String,
    pub identity_verified: bool,
    pub enabled: bool,
    pub limits: AccountLimitsV2,
    pub strategies: Vec<StrategyLimitsV2>,
}

impl AccountControlV2 {
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
            || !self.limits.valid()
            || self.strategies.len() > 32
        {
            return false;
        }
        if self.enabled
            && self.environment == AccountEnvironmentV2::Real
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
pub struct AccountGovernanceV2 {
    pub schema: String,
    pub revision: u64,
    pub trading_requested: bool,
    pub operator_authorization: OperatorAuthorizationV2,
    pub max_total_modelled_loss_usdt: String,
    pub accounts: Vec<AccountControlV2>,
}

impl AccountGovernanceV2 {
    pub fn valid(&self) -> bool {
        let Some(ceiling) = positive(&self.operator_authorization.ceiling_usdt) else {
            return false;
        };
        let Some(global) = positive(&self.max_total_modelled_loss_usdt) else {
            return false;
        };
        if self.schema != GOVERNANCE_SCHEMA_V2
            || (self.trading_requested && self.revision == 0)
            || !self.operator_authorization.id.starts_with("sha256:")
            || !lower_sha256(&self.operator_authorization.id[7..])
            || global > ceiling
            || self.accounts.len() > 32
        {
            return false;
        }
        let mut ids = BTreeSet::new();
        let mut identities = BTreeSet::new();
        for account in &self.accounts {
            if !account.valid() || !ids.insert(&account.id) {
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
        !self.trading_requested
            || (self.operator_authorization.status == AuthorizationStatus::Active
                && self.accounts.iter().any(|account| {
                    account.enabled && account.environment == AccountEnvironmentV2::Real
                }))
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
pub struct CentralGovernanceSnapshotV2 {
    pub state: AccountGovernanceV2,
    pub issued_at_ms: u64,
    pub session_generation: u64,
    /// Worker request-scoped ping challenge; validated and consumed by the
    /// authenticated native transport using its own monotonic clock.
    /// Optional only for decoding older read-only snapshots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_challenge: Option<String>,
}
impl CentralGovernanceSnapshotV2 {
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
            && self.state.trading_requested
            && self.state.operator_authorization.status == AuthorizationStatus::Active
            && self.state.accounts.iter().any(|a| {
                a.venue == "okx"
                    && a.environment == AccountEnvironmentV2::Real
                    && a.enabled
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
            && self.state.trading_requested
            && self.state.operator_authorization.status == AuthorizationStatus::Active
            && self.state.accounts.iter().any(|a| {
                a.venue == "okx"
                    && a.environment == AccountEnvironmentV2::Real
                    && a.enabled
                    && a.identity_verified
                    && a.uid_fingerprint == uid
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn governance_v2_is_typed_and_does_not_grant_live() {
        let state = AccountGovernanceV2 {
            schema: GOVERNANCE_SCHEMA_V2.to_owned(),
            revision: 0,
            trading_requested: false,
            operator_authorization: OperatorAuthorizationV2 {
                id: format!("sha256:{}", "0".repeat(64)),
                status: AuthorizationStatus::Unverified,
                ceiling_usdt: "10".to_owned(),
            },
            max_total_modelled_loss_usdt: "2.75".to_owned(),
            accounts: Vec::new(),
        };
        assert!(state.valid());
        assert!(!state.native_new_risk_admitted());
        let roundtrip = serde_json::to_string(&state).unwrap();
        let decoded: AccountGovernanceV2 = serde_json::from_str(&roundtrip).unwrap();
        assert_eq!(decoded, state);
        let mut altered = state;
        altered.max_total_modelled_loss_usdt = "10.000000001".to_owned();
        assert!(!altered.valid());
        altered.max_total_modelled_loss_usdt = "1e1".to_owned();
        assert!(!altered.valid());
        altered.max_total_modelled_loss_usdt = "1".to_owned();
        altered.trading_requested = true;
        assert!(!altered.valid());
        let scope = CentralGovernanceSnapshotV2 {
            state: altered,
            issued_at_ms: 1000,
            session_generation: 42,
            delivery_challenge: None,
        };
        assert!(!scope.authorizes_delivery(&"a".repeat(64), 2000, 42));
        assert!(!scope.authorizes_delivery(&"a".repeat(64), 20_000, 42));
    }
}

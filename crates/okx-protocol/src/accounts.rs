//! Clean-install account access contract. No network, storage or financial arithmetic.
use serde::{Deserialize, Serialize};

pub const PRODUCT_CONTRACT: &str = "okx.product/v1";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountKey {
    pub venue: String,
    pub environment: Environment,
    pub verified_uid: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Environment {
    Real,
    Demo,
}

impl AccountKey {
    pub fn valid(&self) -> bool {
        self.venue == "okx"
            && self.verified_uid.len() == 64
            && self
                .verified_uid
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradingAccess<L> {
    pub account_key: AccountKey,
    pub enabled: bool,
    pub policy_revision: u64,
    pub limits: L,
    pub explicit_pause: bool,
}

impl<L> TradingAccess<L> {
    /// Access is persistent user intent; this is never a POST admission decision.
    pub fn permits_new_risk(&self) -> bool {
        self.account_key.valid() && self.policy_revision > 0 && self.enabled && !self.explicit_pause
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Readiness {
    NotEvaluated,
    Ready,
    Waiting,
    Blocked,
}

impl AccountKey {
    pub fn wire(&self) -> String {
        format!(
            "{}:{}:{}",
            self.venue,
            match self.environment {
                Environment::Real => "real",
                Environment::Demo => "demo",
            },
            self.verified_uid
        )
    }
}

pub fn valid_reference(value: &str) -> bool {
    (2..=64).contains(&value.len())
        && value.as_bytes()[0].is_ascii_alphabetic()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiedAccountConfig {
    pub account_key: AccountKey,
    pub alias: String,
    pub credential_ref: String,
    pub connect_operation_id: String,
}

impl VerifiedAccountConfig {
    pub fn valid(&self) -> bool {
        self.account_key.valid()
            && valid_reference(&self.alias)
            && valid_reference(&self.credential_ref)
            && (8..=128).contains(&self.connect_operation_id.len())
            && self
                .connect_operation_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountCatalog {
    pub schema: String,
    pub installation_id: String,
    pub accounts: Vec<VerifiedAccountConfig>,
}

impl AccountCatalog {
    pub const SCHEMA: &str = "okx.verified-account-catalog/v1";
    pub fn valid(&self) -> bool {
        use std::collections::BTreeSet;
        let mut aliases = BTreeSet::new();
        let mut keys = BTreeSet::new();
        let mut refs = BTreeSet::new();
        self.schema == Self::SCHEMA
            && valid_reference(&self.installation_id)
            && self.accounts.len() <= 32
            && self.accounts.iter().all(|a| {
                a.valid()
                    && aliases.insert(&a.alias)
                    && keys.insert(&a.account_key)
                    && refs.insert(&a.credential_ref)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn access_survives_restart() {
        let access = TradingAccess {
            account_key: AccountKey {
                venue: "okx".into(),
                environment: Environment::Real,
                verified_uid: "a".repeat(64),
            },
            enabled: true,
            policy_revision: 1,
            limits: (),
            explicit_pause: false,
        };
        let restored: TradingAccess<()> =
            serde_json::from_slice(&serde_json::to_vec(&access).unwrap()).unwrap();
        assert_eq!(access, restored);
        assert!(restored.permits_new_risk());
    }
}

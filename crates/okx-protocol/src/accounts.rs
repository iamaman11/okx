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

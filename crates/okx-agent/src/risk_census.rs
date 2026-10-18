//! One native source-complete account cohort. Never infer portfolio flat
//! from only the selected account. Only Rust/venue may construct evidence.
use okx_observation::AccountLedgerFacts;
use okx_protocol::risk_authority::{NativeRiskCensus, RiskAccountOccupancy};
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CensusFailure {
    RegistryUnverified,
    MissingOrDuplicateAccount,
    StaleSource,
    IncompleteHistory,
    PrivateStreamGap,
    UnknownMutation,
    UnpricedExposure,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountRiskEvidence {
    pub account_key: String,
    pub source_generation: String,
    pub received_at_ms: u64,
    pub account_inventory_complete: bool,
    pub orders_algos_complete: bool,
    pub financial_history_complete: bool,
    pub private_stream_continuous: bool,
    pub unknown_mutations_reconciled: bool,
    /// None means unknown or potentially unbounded manual/owned risk.
    pub all_exposure_modelled_loss_usdt: Option<String>,
}
#[derive(Debug, Clone)]
pub struct PortfolioRiskSources {
    /// Sorted exact identities from authenticated central governance.
    pub registered_real_accounts: Vec<String>,
    pub registry_revision: u64,
    pub registry_verified: bool,
    pub observed: Vec<AccountRiskEvidence>,
    pub now_ms: u64,
}
fn account_key_valid(key: &str) -> bool {
    let Some(uid) = key.strip_prefix("okx:real:") else {
        return false;
    };
    uid.len() == 64
        && uid
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn loss_valid(value: &str) -> bool {
    let Ok(n) = Decimal::from_str(value) else {
        return false;
    };
    n >= Decimal::ZERO
        && n.scale() <= 9
        && value.len() <= 25
        && !value.contains(['e', 'E', '_', '+', '-'])
}
impl PortfolioRiskSources {
    pub fn census(&self) -> Result<NativeRiskCensus, CensusFailure> {
        use CensusFailure as E;
        if !self.registry_verified || self.registry_revision == 0 {
            return Err(E::RegistryUnverified);
        }
        let keys = &self.registered_real_accounts;
        if keys.is_empty()
            || keys.len() > 32
            || keys.iter().any(|k| !account_key_valid(k))
            || keys.windows(2).any(|pair| pair[0] >= pair[1])
            || self.observed.len() != keys.len()
        {
            return Err(E::MissingOrDuplicateAccount);
        }
        let mut seen = BTreeSet::new();
        let mut occupied = Vec::with_capacity(keys.len());
        for source in &self.observed {
            if !keys.contains(&source.account_key)
                || !seen.insert(&source.account_key)
                || source.source_generation.is_empty()
            {
                return Err(E::MissingOrDuplicateAccount);
            }
            if source.received_at_ms == 0
                || source.received_at_ms > self.now_ms
                || self.now_ms - source.received_at_ms > 5_000
            {
                return Err(E::StaleSource);
            }
            if !source.account_inventory_complete
                || !source.orders_algos_complete
                || !source.financial_history_complete
            {
                return Err(E::IncompleteHistory);
            }
            if !source.private_stream_continuous {
                return Err(E::PrivateStreamGap);
            }
            if !source.unknown_mutations_reconciled {
                return Err(E::UnknownMutation);
            }
            let amount = source
                .all_exposure_modelled_loss_usdt
                .as_deref()
                .ok_or(E::UnpricedExposure)?;
            if !loss_valid(amount) {
                return Err(E::UnpricedExposure);
            }
            occupied.push(RiskAccountOccupancy {
                account_key: source.account_key.clone(),
                occupied_usdt: Decimal::from_str(amount)
                    .map_err(|_| E::UnpricedExposure)?
                    .normalize()
                    .to_string(),
            });
        }
        occupied.sort_by(|a, b| a.account_key.cmp(&b.account_key));
        let bytes = serde_json::to_vec(&(
            "okx.native-risk-source/v1",
            self.registry_revision,
            keys,
            &self.observed,
        ))
        .map_err(|_| E::IncompleteHistory)?;
        let digest = Sha256::digest(bytes);
        let census = NativeRiskCensus {
            account_keys: keys.clone(),
            occupied,
            generated_at_ms: self.now_ms,
            source_digest: format!("{digest:x}"),
            complete: true,
            native_account_evidence_complete: true,
            native_order_evidence_complete: true,
            native_ledger_evidence_complete: true,
        };
        if !census.valid() {
            return Err(E::UnpricedExposure);
        }
        Ok(census)
    }
    /// The currently installed observer only attests one account and cannot
    /// grant global Real risk regardless of observed flatness.
    pub fn selected_account_is_insufficient(facts: &AccountLedgerFacts) -> bool {
        !facts.summary.authority.multi_account_inventory_complete
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn key(v: char) -> String {
        format!("okx:real:{}", v.to_string().repeat(64))
    }
    fn row(v: char) -> AccountRiskEvidence {
        AccountRiskEvidence {
            account_key: key(v),
            source_generation: "generation_verified".into(),
            received_at_ms: 1_000,
            account_inventory_complete: true,
            orders_algos_complete: true,
            financial_history_complete: true,
            private_stream_continuous: true,
            unknown_mutations_reconciled: true,
            all_exposure_modelled_loss_usdt: Some(if v == 'a' { "0" } else { "2.25" }.into()),
        }
    }
    fn cohort() -> PortfolioRiskSources {
        PortfolioRiskSources {
            registered_real_accounts: vec![key('a'), key('b')],
            registry_revision: 4,
            registry_verified: true,
            observed: vec![row('a'), row('b')],
            now_ms: 1_000,
        }
    }
    #[test]
    fn two_accounts_and_manual_exposure_are_counted() {
        let c = cohort().census().unwrap();
        assert!(c.valid());
        assert_eq!(c.occupied[0].occupied_usdt, "0");
        assert_eq!(c.occupied[1].occupied_usdt, "2.25");
    }
    #[test]
    fn missing_duplicate_unpriced_stale_and_unknown_never_become_zero() {
        let mut c = cohort();
        c.observed.pop();
        assert_eq!(
            c.census().unwrap_err(),
            CensusFailure::MissingOrDuplicateAccount
        );
        c = cohort();
        c.observed[1] = c.observed[0].clone();
        assert_eq!(
            c.census().unwrap_err(),
            CensusFailure::MissingOrDuplicateAccount
        );
        c = cohort();
        c.observed[1].all_exposure_modelled_loss_usdt = None;
        assert_eq!(c.census().unwrap_err(), CensusFailure::UnpricedExposure);
        c = cohort();
        c.observed[1].unknown_mutations_reconciled = false;
        assert_eq!(c.census().unwrap_err(), CensusFailure::UnknownMutation);
        c = cohort();
        c.observed[1].orders_algos_complete = false;
        assert_eq!(c.census().unwrap_err(), CensusFailure::IncompleteHistory);
        c = cohort();
        c.observed[1].private_stream_continuous = false;
        assert_eq!(c.census().unwrap_err(), CensusFailure::PrivateStreamGap);
        c = cohort();
        c.now_ms = 6_001;
        assert_eq!(c.census().unwrap_err(), CensusFailure::StaleSource);
        c = cohort();
        c.registry_verified = false;
        assert_eq!(c.census().unwrap_err(), CensusFailure::RegistryUnverified);
    }
}

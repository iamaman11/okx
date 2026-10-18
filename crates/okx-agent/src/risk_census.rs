//! One native source-complete account cohort. Never infer portfolio flat
//! from only the selected account. Only Rust/venue may construct evidence.
use okx_observation::AccountLedgerFacts;
use okx_protocol::account_governance::{
    AccountEnvironmentV2, AuthorizationStatus, CentralGovernanceSnapshotV2,
};
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
    /// Only the authoritative v2 constructor may establish this cohort.
    /// One selected authenticated account cannot self-certify the other UIDs.
    registered_real_accounts: Vec<String>,
    registry_revision: u64,
    registry_verified: bool,
    observed: Vec<AccountRiskEvidence>,
    now_ms: u64,
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
    /// Bind all registered Real accounts (including disabled ones) to the
    /// authenticated DO governance snapshot. Missing/unverified account
    /// sources must fail later at census(), never silently disappear.
    pub fn from_governance(
        scope: &CentralGovernanceSnapshotV2,
        observed: Vec<AccountRiskEvidence>,
        now_ms: u64,
    ) -> Result<Self, CensusFailure> {
        if !scope.valid()
            || !scope.state.trading_requested
            || scope.state.operator_authorization.status != AuthorizationStatus::Active
            || scope.state.revision == 0
            || scope.issued_at_ms > now_ms
            || now_ms.saturating_sub(scope.issued_at_ms) > 15_000
        {
            return Err(CensusFailure::RegistryUnverified);
        }
        let mut keys = Vec::new();
        for account in &scope.state.accounts {
            if account.environment != AccountEnvironmentV2::Real {
                continue;
            }
            // Cross-venue aggregation needs an equally source-proven reader
            // for each venue; never omit unsupported Real accounts.
            if account.venue != "okx" || !account.identity_verified {
                return Err(CensusFailure::RegistryUnverified);
            }
            keys.push(format!(
                "{}:real:{}",
                account.venue, account.uid_fingerprint
            ));
        }
        keys.sort();
        if keys.is_empty() || keys.len() > 32 || keys.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(CensusFailure::MissingOrDuplicateAccount);
        }
        Ok(Self {
            registered_real_accounts: keys,
            registry_revision: scope.state.revision,
            registry_verified: true,
            observed,
            now_ms,
        })
    }

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
        let source_read_floor_ms = self
            .observed
            .iter()
            .map(|row| row.received_at_ms)
            .min()
            .ok_or(E::MissingOrDuplicateAccount)?;
        let canonical_content = serde_json::to_vec(&(
            "okx.native-risk-content/v1",
            self.registry_revision,
            keys,
            &occupied,
        ))
        .map_err(|_| E::IncompleteHistory)?;
        let content_digest = format!("{:x}", Sha256::digest(canonical_content));
        let mut canonical_sources = self.observed.iter().collect::<Vec<_>>();
        canonical_sources.sort_by(|a, b| a.account_key.cmp(&b.account_key));
        let bytes = serde_json::to_vec(&(
            "okx.native-risk-source/v1",
            self.registry_revision,
            keys,
            canonical_sources,
        ))
        .map_err(|_| E::IncompleteHistory)?;
        let digest = Sha256::digest(bytes);
        let census = NativeRiskCensus {
            account_keys: keys.clone(),
            occupied,
            generated_at_ms: self.now_ms,
            source_digest: format!("{digest:x}"),
            content_digest,
            source_read_floor_ms,
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
    fn governance() -> CentralGovernanceSnapshotV2 {
        use okx_protocol::account_governance::{
            AccountControlV2, AccountGovernanceV2, AccountLimitsV2, GOVERNANCE_SCHEMA_V2,
            OperatorAuthorizationV2,
        };
        let account = |id: &str, uid: char, enabled: bool| AccountControlV2 {
            id: id.to_owned(),
            label: id.to_owned(),
            venue: "okx".to_owned(),
            environment: AccountEnvironmentV2::Real,
            uid_fingerprint: uid.to_string().repeat(64),
            identity_verified: true,
            enabled,
            limits: AccountLimitsV2 {
                max_modelled_loss_usdt: "10".into(),
                max_order_loss_usdt: "5".into(),
                max_notional_usdt: Some("100".into()),
                max_leverage: Some("3".into()),
                max_daily_realized_loss_usdt: Some("2".into()),
                max_drawdown_ratio: Some("0.1".into()),
                allowed_instruments: vec!["BTC-USDT-SWAP".into()],
            },
            strategies: Vec::new(),
        };
        CentralGovernanceSnapshotV2 {
            state: AccountGovernanceV2 {
                schema: GOVERNANCE_SCHEMA_V2.into(),
                revision: 4,
                trading_requested: true,
                operator_authorization: OperatorAuthorizationV2 {
                    id: format!("sha256:{}", "c".repeat(64)),
                    status: AuthorizationStatus::Active,
                    ceiling_usdt: "10".into(),
                },
                max_total_modelled_loss_usdt: "10".into(),
                accounts: vec![account("acct_a", 'a', true), account("acct_b", 'b', false)],
            },
            issued_at_ms: 1_000,
            session_generation: 77,
        }
    }

    #[test]
    fn central_scope_cannot_drop_disabled_or_unverified_real_accounts() {
        let g = governance();
        let source = PortfolioRiskSources::from_governance(&g, vec![row('a'), row('b')], 1_000)
            .expect("native cohort");
        assert_eq!(
            source.census().unwrap().account_keys,
            vec![key('a'), key('b')]
        );
        let incomplete = PortfolioRiskSources::from_governance(&g, vec![row('a')], 1_000).unwrap();
        assert_eq!(
            incomplete.census().unwrap_err(),
            CensusFailure::MissingOrDuplicateAccount
        );

        let mut bad = governance();
        bad.state.accounts[1].identity_verified = false;
        assert_eq!(
            PortfolioRiskSources::from_governance(&bad, vec![row('a'), row('b')], 1_000)
                .unwrap_err(),
            CensusFailure::RegistryUnverified
        );
        let mut other_venue = governance();
        other_venue.state.accounts[1].venue = "other".into();
        assert_eq!(
            PortfolioRiskSources::from_governance(&other_venue, vec![row('a'), row('b')], 1_000)
                .unwrap_err(),
            CensusFailure::RegistryUnverified
        );
        assert_eq!(
            PortfolioRiskSources::from_governance(&g, vec![row('a'), row('b')], 16_001)
                .unwrap_err(),
            CensusFailure::RegistryUnverified
        );
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
        let mut reordered = cohort();
        reordered.observed.reverse();
        assert_eq!(reordered.census().unwrap().source_digest, c.source_digest);
        let mut fresh = cohort();
        fresh.now_ms = 1_001;
        for row in &mut fresh.observed {
            row.received_at_ms = 1_001;
        }
        let later = fresh.census().unwrap();
        assert_eq!(later.content_digest, c.content_digest);
        assert_ne!(later.source_digest, c.source_digest);
        assert!(later.source_read_floor_ms > c.source_read_floor_ms);
        // Only advancing the envelope timestamp cannot fabricate a new read.
        let mut old_rows = cohort();
        old_rows.now_ms = 1_001;
        let envelope_only = old_rows.census().unwrap();
        assert_eq!(envelope_only.source_digest, c.source_digest);
        assert_eq!(envelope_only.source_read_floor_ms, c.source_read_floor_ms);
        // Refreshing one of two registered accounts is also insufficient.
        old_rows.observed[0].received_at_ms = 1_001;
        assert_eq!(
            old_rows.census().unwrap().source_read_floor_ms,
            c.source_read_floor_ms
        );
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

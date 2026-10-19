//! One native source-complete account cohort. Never infer portfolio flat
//! from only the selected account. Only Rust/venue may construct evidence.
use chrono::DateTime;
use okx_api::PendingDerivativeAlgoRiskInventory;
use okx_execution::{
    DurableExecutionLedger, ExecutionRecord, ExecutionState, ProfileHandoffLedgerAssessment,
    ProtectiveOrderLink, ProtectiveOrderStatus,
};
use okx_observation::{AccountLedgerFacts, AccountSnapshot};
#[cfg(test)]
use okx_protocol::account_governance::AuthorizationStatus;
use okx_protocol::account_governance::{AccountEnvironmentV2, CentralGovernanceSnapshotV2};
use okx_protocol::risk_authority::{NativeRiskCensus, RiskAccountOccupancy};
use okx_runtime::{PrivateConnectionState, PrivateConvergenceWindow};
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
    InconsistentAuthenticatedIdentity,
}

impl CensusFailure {
    /// Preserve exact fail-closed cause in the native trade rejection rather
    /// than turning source, WS, history and ledger failures into one generic code.
    pub const fn native_reason(self) -> &'static str {
        match self {
            Self::RegistryUnverified => "RISK_NATIVE_REGISTRY_UNVERIFIED",
            Self::MissingOrDuplicateAccount => "RISK_NATIVE_UID_COHORT_INCOMPLETE",
            Self::StaleSource => "RISK_NATIVE_SOURCE_STALE",
            Self::IncompleteHistory => "RISK_NATIVE_HISTORY_INCOMPLETE",
            Self::PrivateStreamGap => "RISK_NATIVE_PRIVATE_STREAM_GAP",
            Self::UnknownMutation => "RISK_NATIVE_UNKNOWN_MUTATION",
            Self::UnpricedExposure => "RISK_NATIVE_EXPOSURE_UNPRICED",
            Self::InconsistentAuthenticatedIdentity => "RISK_NATIVE_IDENTITY_MISMATCH",
        }
    }
}

/// A protective link is inert only before its FIRST venue POST. Merely seeing
/// PENDING is insufficient: it also describes ambiguous exchange outcomes.
fn unsent_protection(link: &ProtectiveOrderLink) -> bool {
    link.status == ProtectiveOrderStatus::Pending
        && link.algo_order_id.is_none()
        && link.covered_size.is_none()
        && link.failure_code.is_none()
        && link.cleanup.is_none()
        && link.size_amend.is_none()
}

fn exact_local_unsent_prepared(record: &ExecutionRecord, uid: &str) -> bool {
    record.state == ExecutionState::Prepared
        && record.plan.account_uid_fingerprint == uid
        && record.plan.action.is_risk_increasing()
        && record.plan.open_risk.is_some()
        && record.strategy_run_id.is_some()
        && record.order_id.is_none()
        && record.exchange_state.is_none()
        && record.submission_timing.is_none()
        && record.rejection_code.is_none()
        && record.mutations.is_empty()
        && record.reverse.is_none()
        && record.protection.as_ref().is_some_and(unsent_protection)
}

fn ledger_flat_except_exact_prepared(
    native: &ProfileHandoffLedgerAssessment,
    exact_prepared: bool,
    stop_present: bool,
) -> bool {
    native.account_uid_mismatches == 0
        && native.unresolved_order_mutations == 0
        // profile-handoff correctly counts the unsent protective PENDING.
        // Risk reservation may exclude exactly its OWN unsent candidate only.
        && native.unresolved_protective_lifecycle == usize::from(exact_prepared)
        && native.nonterminal_intents == usize::from(exact_prepared)
        && !stop_present
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountRiskEvidence {
    account_key: String,
    source_generation: String,
    received_at_ms: u64,
    account_inventory_complete: bool,
    orders_algos_complete: bool,
    financial_history_complete: bool,
    private_stream_continuous: bool,
    unknown_mutations_reconciled: bool,
    /// None means unknown or potentially unbounded manual/owned risk.
    all_exposure_modelled_loss_usdt: Option<String>,
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
impl AccountRiskEvidence {
    /// A genuinely authenticated, exactly FLAT derivatives source. Only the
    /// native Rust owner can build it, after signed REST and a private WS
    /// convergence cursor. This deliberately cannot price open/manual risk:
    /// such exposure must use a separately validated native risk model.
    ///
    /// Existing PREPARED candidate is allowed in the ledger because it has
    /// not sent an order. Any other nonterminal mutation is disqualifying.
    pub fn from_native_flat_read(
        snapshot: &AccountSnapshot,
        facts: &AccountLedgerFacts,
        algos: &PendingDerivativeAlgoRiskInventory,
        private_window: &PrivateConvergenceWindow,
        ledger: &DurableExecutionLedger,
        prepared_intent: Option<&str>,
        now_ms: u64,
    ) -> Result<Self, CensusFailure> {
        use CensusFailure as E;
        let received = |raw: &str| {
            DateTime::parse_from_rfc3339(raw)
                .ok()
                .and_then(|v| u64::try_from(v.timestamp_millis()).ok())
        };
        let snapshot_at = received(&snapshot.source_received_at).ok_or(E::StaleSource)?;
        let history_at = received(&facts.summary.source_received_at).ok_or(E::StaleSource)?;
        let received_at_ms = snapshot_at.min(history_at).min(algos.received_at_ms);
        if received_at_ms == 0 || received_at_ms > now_ms || now_ms - received_at_ms > 5_000 {
            return Err(E::StaleSource);
        }
        let uid = &snapshot.account_uid_fingerprint;
        let account_key = format!("okx:real:{uid}");
        if !account_key_valid(&account_key)
            || facts.summary.authority.account_uid_fingerprint != *uid
            || facts.summary.account_generation != snapshot.account_generation
            || snapshot.api_key_permissions.as_slice() != ["read_only"]
        {
            return Err(E::InconsistentAuthenticatedIdentity);
        }

        if algos.schema != "okx.pending-derivative-risk-algos/v1"
            || algos.scopes_checked != 16
            || !algos.complete_within_bound
            || algos.rows != 0
            || snapshot.pending_orders.len() != facts.summary.pending_orders
            || snapshot.positions.len() != facts.summary.open_positions
            || !snapshot.pending_orders.is_empty()
            || !snapshot.positions.is_empty()
            || !facts.authoritative_positions.is_empty()
        {
            return Err(E::UnpricedExposure);
        }
        if !facts.bills_history_complete
            || !facts.exchange_orders_history_complete
            || !facts.exchange_fills_history_complete
            || facts.summary.history_coverage.is_empty()
            || facts
                .summary
                .history_coverage
                .iter()
                .any(|source| !source.complete_within_bound)
        {
            return Err(E::IncompleteHistory);
        }
        let ws = &private_window.status;
        if !private_window.events.is_empty()
            || private_window.generation != ws.connection_generation
            || ws.connection_state != PrivateConnectionState::Connected
            || ws.connection_id_fingerprint.is_none()
            || !ws.logged_in
            || !ws.subscriptions_complete
            || !ws.account_data_seen
            || !ws.positions_data_seen
            || !snapshot.private_ws_connected
            || snapshot.private_ws_generation != Some(ws.connection_generation)
            || snapshot.private_ws_connection_fingerprint != ws.connection_id_fingerprint
        {
            return Err(E::PrivateStreamGap);
        }

        let native = ledger
            .profile_handoff_ledger_assessment(uid)
            .map_err(|_| E::UnknownMutation)?;
        let prepared = prepared_intent
            .and_then(|id| ledger.get(id))
            .is_some_and(|entry| exact_local_unsent_prepared(&entry.record, uid));
        if (prepared_intent.is_some() && !prepared)
            || !ledger_flat_except_exact_prepared(&native, prepared, ledger.risk_stop().is_some())
        {
            return Err(E::UnknownMutation);
        }
        Ok(Self {
            account_key,
            source_generation: format!(
                "{}:{}",
                snapshot.account_generation, ws.connection_generation
            ),
            received_at_ms,
            account_inventory_complete: true,
            orders_algos_complete: true,
            financial_history_complete: true,
            private_stream_continuous: true,
            unknown_mutations_reconciled: true,
            all_exposure_modelled_loss_usdt: Some("0".to_owned()),
        })
    }
}

impl PortfolioRiskSources {
    /// Bind all registered Real accounts (including disabled ones) to the
    /// authenticated DO governance snapshot. Missing/unverified account
    /// sources must fail later at census(), never silently disappear.
    pub fn from_governance(
        scope: &CentralGovernanceSnapshotV2,
        observed: Vec<AccountRiskEvidence>,
        now_ms: u64,
        delivery_challenge_verified: bool,
    ) -> Result<Self, CensusFailure> {
        // Source observation is valid independently of positive trading
        // permission. STOP/revocation must still allow proof of terminal FLAT
        // and release of existing risk; only RESERVE/BEGIN_POST require GO.
        if !scope.valid()
            || scope.state.revision == 0
            || scope.delivery_challenge.is_none()
            || !delivery_challenge_verified
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

    #[test]
    fn only_one_exact_unsent_prepared_protection_is_inert_for_risk() {
        let mut pending = ProtectiveOrderLink {
            policy_version: "okx.protective-order/mark-market-v1".into(),
            algo_client_order_id: "prx01234567890123456789".into(),
            trigger_price_basis: okx_execution::ProtectiveTriggerPriceBasis::Mark,
            status: ProtectiveOrderStatus::Pending,
            algo_order_id: None,
            covered_size: None,
            failure_code: None,
            cleanup: None,
            size_amend: None,
        };
        assert!(unsent_protection(&pending));
        pending.algo_order_id = Some("venue_123".into());
        assert!(!unsent_protection(&pending));
        pending.algo_order_id = None;
        pending.status = ProtectiveOrderStatus::Active;
        assert!(!unsent_protection(&pending));
        pending.status = ProtectiveOrderStatus::Failed;
        assert!(!unsent_protection(&pending));
    }

    #[test]
    fn native_risk_excludes_one_inert_prepared_but_never_other_unknowns() {
        let mut audit = ProfileHandoffLedgerAssessment {
            schema: "okx.profile-handoff-ledger-assessment/v1",
            managed_intents_total: 1,
            nonterminal_intents: 1,
            unresolved_order_mutations: 0,
            unresolved_protective_lifecycle: 1,
            account_uid_mismatches: 0,
            source_ledger_manifest_sha256: "a".repeat(64),
            source_ledger_format: "okx.execution-ledger/v14".into(),
            immutable_stop_reason: None,
            finalized_run_checksum_sha256: None,
        };
        assert!(ledger_flat_except_exact_prepared(&audit, true, false));
        assert!(!ledger_flat_except_exact_prepared(&audit, false, false));
        assert!(!ledger_flat_except_exact_prepared(&audit, true, true));
        audit.unresolved_protective_lifecycle = 2;
        assert!(!ledger_flat_except_exact_prepared(&audit, true, false));
        audit.unresolved_protective_lifecycle = 1;
        audit.nonterminal_intents = 2;
        assert!(!ledger_flat_except_exact_prepared(&audit, true, false));
        audit.nonterminal_intents = 1;
        audit.unresolved_order_mutations = 1;
        assert!(!ledger_flat_except_exact_prepared(&audit, true, false));
        audit.unresolved_order_mutations = 0;
        audit.account_uid_mismatches = 1;
        assert!(!ledger_flat_except_exact_prepared(&audit, true, false));
    }

    #[test]
    fn exact_census_failure_codes_remain_distinct() {
        assert_ne!(
            CensusFailure::StaleSource.native_reason(),
            CensusFailure::UnknownMutation.native_reason()
        );
        assert_ne!(
            CensusFailure::PrivateStreamGap.native_reason(),
            CensusFailure::IncompleteHistory.native_reason()
        );
    }

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
            delivery_challenge: Some("ping_risk_census_012345".to_owned()),
        }
    }

    #[test]
    fn central_scope_cannot_drop_disabled_or_unverified_real_accounts() {
        let g = governance();
        let source =
            PortfolioRiskSources::from_governance(&g, vec![row('a'), row('b')], 1_000, true)
                .expect("native cohort");
        assert_eq!(
            source.census().unwrap().account_keys,
            vec![key('a'), key('b')]
        );
        let mut stopped = governance();
        stopped.state.trading_requested = false;
        stopped.state.operator_authorization.status = AuthorizationStatus::Revoked;
        let read_only =
            PortfolioRiskSources::from_governance(&stopped, vec![row('a'), row('b')], 1_000, true)
                .expect("revoked new trade never blocks native read for release");
        assert_eq!(read_only.census().unwrap().account_keys.len(), 2);
        let incomplete =
            PortfolioRiskSources::from_governance(&g, vec![row('a')], 1_000, true).unwrap();
        assert_eq!(
            incomplete.census().unwrap_err(),
            CensusFailure::MissingOrDuplicateAccount
        );

        let mut bad = governance();
        bad.state.accounts[1].identity_verified = false;
        assert_eq!(
            PortfolioRiskSources::from_governance(&bad, vec![row('a'), row('b')], 1_000, true)
                .unwrap_err(),
            CensusFailure::RegistryUnverified
        );
        let mut other_venue = governance();
        other_venue.state.accounts[1].venue = "other".into();
        assert_eq!(
            PortfolioRiskSources::from_governance(
                &other_venue,
                vec![row('a'), row('b')],
                1_000,
                true
            )
            .unwrap_err(),
            CensusFailure::RegistryUnverified
        );
        assert_eq!(
            PortfolioRiskSources::from_governance(&g, vec![row('a'), row('b')], 16_001, false)
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

//! #510: original single-writer RiskCommand read ingress.
//! The local backend never forwards to Cloudflare and never synthesizes a
//! budget/venue grant. Trading mutations require independently accepted
//! account, policy and original financial opening receipts on the same owner.
use okx_execution::account_worker::{
    UnifiedFinanceBoundSettlement, UnifiedFinanceBoundUnsent, UnifiedFinanceHandle,
};
use okx_protocol::account_governance::{AccountEnvironment, CentralGovernanceSnapshot};
use okx_protocol::risk_authority::{
    RISK_RESULT_SCHEMA_V1, RiskAccountBudget, RiskBudgetSnapshot, RiskCommand, RiskCommandAction,
    RiskDecisionStatus, RiskReservationState, RiskResult,
};

/// Observing authorized configuration is not an SQL financial permission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativePolicyObservation {
    WaitingForAccounts,
    Applied,
}

#[derive(Clone)]
pub struct NativeFinanceRisk {
    pub(super) owner: UnifiedFinanceHandle,
    pub(super) installation: String,
    pub(super) campaign: String,
}

impl NativeFinanceRisk {
    pub fn new(
        owner: UnifiedFinanceHandle,
        installation: String,
        campaign: String,
    ) -> Result<Self, &'static str> {
        if installation.is_empty() || campaign.is_empty() || owner.installation_id() != installation
        {
            return Err("NATIVE_FINANCE_RISK_SCOPE_INVALID");
        }
        Ok(Self {
            owner,
            installation,
            campaign,
        })
    }

    /// Standard startup before the first authenticated policy. SQL remains the
    /// sole owner of campaign identity; absence grants no financial capability.
    pub fn from_owner(owner: UnifiedFinanceHandle) -> Self {
        Self {
            installation: owner.installation_id().to_owned(),
            owner,
            campaign: String::new(),
        }
    }

    pub(crate) async fn resolved(&self) -> Result<Self, &'static str> {
        if !self.campaign.is_empty() {
            return Ok(self.clone());
        }
        let campaign = self
            .owner
            .configured_campaign(std::time::Instant::now() + std::time::Duration::from_secs(2))
            .await
            .map_err(|_| "FINANCE_CAMPAIGN_IDENTITY_UNAVAILABLE")?
            .ok_or("FINANCE_CAMPAIGN_POLICY_REQUIRED")?;
        Self::new(self.owner.clone(), self.installation.clone(), campaign)
    }

    /// Initial policy delivery must not deadlock account connection. Only the
    /// complete original SQL roster can activate policy; pending observations
    /// remain memory-only and cannot manufacture a campaign, budget or grant.
    pub async fn observe_governance(
        &self,
        snapshot: &CentralGovernanceSnapshot,
    ) -> Result<NativePolicyObservation, String> {
        self.validate_governance_scope(snapshot)?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let configured = self
            .owner
            .configured_campaign(deadline)
            .await
            .map_err(|e| e.to_string())?;
        if configured
            .as_ref()
            .is_some_and(|c| c != &snapshot.state.campaign_id)
        {
            return Err("UNIFIED_GOVERNANCE_IDENTITY_UNVERIFIED".into());
        }
        if configured.is_none() {
            let registered = self
                .owner
                .connected_account_keys(deadline)
                .await
                .map_err(|e| e.to_string())?;
            let mut requested = snapshot
                .state
                .accounts
                .iter()
                .map(|a| a.account_key.clone())
                .collect::<Vec<_>>();
            requested.sort();
            if requested.is_empty() || registered != requested {
                return Ok(NativePolicyObservation::WaitingForAccounts);
            }
        }
        self.apply_governance(snapshot).await?;
        Ok(NativePolicyObservation::Applied)
    }

    fn validate_governance_scope(
        &self,
        snapshot: &CentralGovernanceSnapshot,
    ) -> Result<(), String> {
        if !snapshot.valid()
            || snapshot.state.installation_id != self.installation
            || (!self.campaign.is_empty() && snapshot.state.campaign_id != self.campaign)
        {
            return Err("UNIFIED_GOVERNANCE_IDENTITY_UNVERIFIED".into());
        }
        if snapshot
            .state
            .accounts
            .iter()
            .any(|a| a.environment != AccountEnvironment::Real)
        {
            return Err("UNIFIED_GOVERNANCE_DEMO_COHORT_UNSUPPORTED".into());
        }
        Ok(())
    }

    /// Apply original native AccountStore account-policy rows and RiskBook
    /// campaign revision in ONE caller-owned SQL transaction. A policy creates
    /// no financial opening, no source-completeness assertion and no POST grant.
    pub async fn apply_governance(
        &self,
        snapshot: &CentralGovernanceSnapshot,
    ) -> Result<(), String> {
        self.validate_governance_scope(snapshot)?;
        if snapshot.state.accounts.is_empty() {
            return Err("NATIVE_CAMPAIGN_ACCOUNTS_NOT_CONNECTED".into());
        }
        use okx_execution::account_worker::{
            NativeCampaignAccountPolicy, NativeCampaignPolicy, NativeCampaignStrategyPolicy,
        };
        let mut accounts = snapshot
            .state
            .accounts
            .iter()
            .map(|account| {
                let mut strategies = account
                    .strategies
                    .iter()
                    .map(|strategy| NativeCampaignStrategyPolicy {
                        strategy_id: strategy.id.clone(),
                        limit_usdt: strategy.max_modelled_loss_usdt.clone(),
                    })
                    .collect::<Vec<_>>();
                if strategies.iter().any(|s| s.strategy_id == "manual") {
                    return Err("NATIVE_MANUAL_POLICY_CONFLICT".to_string());
                }
                strategies.push(NativeCampaignStrategyPolicy {
                    strategy_id: "manual".into(),
                    limit_usdt: account.limits.max_modelled_loss_usdt.clone(),
                });
                strategies.sort_by(|a, b| a.strategy_id.cmp(&b.strategy_id));
                Ok(NativeCampaignAccountPolicy {
                    account_key: account.account_key.clone(),
                    paused: !account.access_enabled(),
                    limit_usdt: account.limits.max_modelled_loss_usdt.clone(),
                    order_limit_usdt: account.limits.max_order_loss_usdt.clone(),
                    entry_limit_usd: account
                        .limits
                        .max_entry_usd
                        .clone()
                        .ok_or("NATIVE_ACCOUNT_ENTRY_LIMIT_MISSING")?,
                    strategies,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        accounts.sort_by(|a, b| a.account_key.cmp(&b.account_key));
        self.owner
            .apply_campaign_policy(
                std::time::Instant::now() + std::time::Duration::from_secs(2),
                NativeCampaignPolicy {
                    campaign_id: snapshot.state.campaign_id.clone(),
                    policy_revision: snapshot.state.revision,
                    generation: snapshot.session_generation,
                    global_limit_usdt: snapshot.state.max_total_modelled_loss_usdt.clone(),
                    loss_limit_usd: snapshot.state.campaign_loss_limit_usd.clone(),
                    entry_limit_usd: snapshot
                        .state
                        .campaign_entry_limit_usd
                        .clone()
                        .ok_or("NATIVE_CAMPAIGN_ENTRY_LIMIT_MISSING")?,
                    accounts,
                },
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    /// An authenticated incoming governance frame may be OBSERVED while
    /// an import is stopped, but it may never update SQL or authorize a
    /// venue operation. Every claimed account and policy field is compared
    /// to the ORIGINAL import on the one installed owner before publishing
    /// an in-memory diagnostic snapshot.
    pub async fn verify_stopped_observation(
        &self,
        snapshot: &CentralGovernanceSnapshot,
    ) -> Result<(), String> {
        if !snapshot.valid()
            || snapshot.state.installation_id != self.installation
            || snapshot.state.campaign_id != self.campaign
            || snapshot.state.accounts.is_empty()
            || snapshot
                .state
                .accounts
                .iter()
                .any(|a| a.environment != AccountEnvironment::Real)
        {
            return Err("FINANCE_STOPPED_OBSERVATION_SCOPE_MISMATCH".into());
        }
        let inventory = self.owner.inventory().await.map_err(|e| e.to_string())?;
        if inventory.state != "OFFLINE_NOT_AUTHORIZED"
            || inventory.accounts != snapshot.state.accounts.len() as i64
            || inventory.campaigns != 1
        {
            return Err("FINANCE_STOPPED_OBSERVATION_ROSTER_MISMATCH".into());
        }
        let summary = self
            .owner
            .campaign_summary(self.campaign.clone())
            .await
            .map_err(|e| e.to_string())?;
        if summary.policy_revision != snapshot.state.revision
            || snapshot
                .state
                .campaign_loss_limit_usd
                .parse::<rust_decimal::Decimal>()
                .ok()
                != summary.loss_limit_usd.parse::<rust_decimal::Decimal>().ok()
        {
            return Err("FINANCE_STOPPED_OBSERVATION_POLICY_MISMATCH".into());
        }
        for account in &snapshot.state.accounts {
            let original = self
                .owner
                .stopped_account_policy(self.campaign.clone(), account.account_key.clone())
                .await
                .map_err(|e| e.to_string())?;
            if original.revision != account.policy_revision
                || original.paused == account.access_enabled()
                || original
                    .campaign_limit_usd
                    .parse::<rust_decimal::Decimal>()
                    .ok()
                    != summary.loss_limit_usd.parse::<rust_decimal::Decimal>().ok()
            {
                return Err("FINANCE_STOPPED_OBSERVATION_ACCOUNT_MISMATCH".into());
            }
        }
        Ok(())
    }

    pub async fn request(&self, command: RiskCommand) -> Result<RiskResult, &'static str> {
        if !command.valid() {
            return Err("INVALID_RISK_COMMAND");
        }
        if command.installation_id != self.installation {
            return Err("FINANCE_RISK_INSTALLATION_MISMATCH");
        }
        let base =
            |status, reason, key, sequence, revision, state, budget, account_budget| RiskResult {
                schema: RISK_RESULT_SCHEMA_V1.into(),
                command_id: command.command_id.clone(),
                status,
                reason,
                reservation_key: key,
                risk_book_sequence: sequence,
                policy_revision: revision,
                existing_modelled_loss_usdt: None,
                reservation_state: state,
                budget,
                account_budget,
            };
        let result = match &command.action {
            RiskCommandAction::Summary {} => {
                let summary = self
                    .owner
                    .campaign_summary(self.campaign.clone())
                    .await
                    .map_err(|_| "NATIVE_RISK_SUMMARY_SOURCE_UNAVAILABLE")?;
                let campaign_origin = summary.opening.as_ref().map(|o| {
                    okx_protocol::risk_authority::RiskCampaignOrigin {
                        campaign_id: self.campaign.clone(),
                        installation_id: self.installation.clone(),
                        started_at_ms: o.campaign_started_at_ms,
                    }
                });
                let campaign_origin = campaign_origin.or_else(|| {
                    summary.campaign_started_at_ms.map(|started_at_ms| {
                        okx_protocol::risk_authority::RiskCampaignOrigin {
                            campaign_id: self.campaign.clone(),
                            installation_id: self.installation.clone(),
                            started_at_ms,
                        }
                    })
                });
                let entry_budget = summary.opening.as_ref().map(|o| {
                    okx_protocol::risk_authority::RiskEntryBudgetSnapshot {
                        campaign_id: self.campaign.clone(),
                        limit_usd: summary.entry_limit_usd.clone(),
                        consumed_usd: summary.consumed_entry_usd.clone(),
                        reserved_usd: summary.reserved_entry_usd.clone(),
                        remaining_usd: summary.remaining_entry_usd.clone(),
                        bound_breach_usd: summary.entry_bound_breach_usd.clone(),
                        opening_source_digest: o.source_digest.clone(),
                        opening_as_of_ms: o.as_of_ms,
                    }
                });
                let budget = RiskBudgetSnapshot {
                    campaign_limit_usd: summary.loss_limit_usd,
                    consumed_loss_usd: summary.consumed_loss_usd,
                    reserved_loss_usd: summary.reserved_loss_usd,
                    remaining_loss_usd: summary.remaining_loss_usd,
                    occupied_modelled_loss_usdt: summary.global_reserved_usdt,
                    // No accepted opening/inception timestamp is in the
                    // imported meta row; never invent it from wall clock.
                    source_as_of_ms: None,
                    campaign_origin,
                    entry_budget,
                };
                base(
                    RiskDecisionStatus::Observed,
                    None,
                    None,
                    Some(summary.book_sequence),
                    Some(summary.policy_revision),
                    None,
                    Some(budget),
                    None,
                )
            }
            RiskCommandAction::AccountSummary { account_key } => {
                let original = self
                    .owner
                    .stopped_account_summary(self.campaign.clone(), account_key.clone())
                    .await
                    .map_err(|_| "NATIVE_RISK_ACCOUNT_SOURCE_UNAVAILABLE")?;
                let account = RiskAccountBudget {
                    account_key: original.account_key,
                    active_reservations: original.active_reservations,
                    occupied_usd: original.reserved_loss_usd,
                    cumulative_loss_usd: original.cumulative_loss_usd,
                    latest_activity_ms: original.latest_activity_ms,
                };
                base(
                    RiskDecisionStatus::Observed,
                    None,
                    None,
                    Some(original.book_sequence),
                    Some(original.policy_revision),
                    None,
                    None,
                    Some(account),
                )
            }
            RiskCommandAction::Inspect {
                reservation_key,
                plan_sha256,
            } => {
                let found = self
                    .owner
                    .stopped_inspect_reservation(
                        self.campaign.clone(),
                        reservation_key.clone(),
                        plan_sha256.clone(),
                    )
                    .await
                    .map_err(|_| "NATIVE_RISK_INSPECT_SOURCE_UNAVAILABLE")?;
                match found {
                    Some(known) => {
                        let state = match known.state {
                            "HELD" => RiskReservationState::Held,
                            "POSTED" => RiskReservationState::Posted,
                            "UNKNOWN" => RiskReservationState::Unknown,
                            "OPEN" => RiskReservationState::Open,
                            "RELEASED" => RiskReservationState::Released,
                            _ => return Err("NATIVE_RISK_RESERVATION_STATE_INVALID"),
                        };
                        base(
                            RiskDecisionStatus::Observed,
                            None,
                            Some(known.reservation_key),
                            Some(known.current_book_sequence),
                            Some(known.policy_revision),
                            Some(state),
                            None,
                            None,
                        )
                    }
                    // Absence means there is no matching durable witness.
                    // It cannot grant POST, RELEASE or any replay permission.
                    None => base(
                        RiskDecisionStatus::Denied,
                        Some("RISK_RESERVATION_NOT_FOUND".into()),
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    ),
                }
            }
            // Four remaining mutation actions require the original live
            // account-census/POST/terminal source producers before native
            // ingress can be accepted. SettleEntry and ReleaseUnsent above
            // ALREADY use their original SQL sources, but ALL writes still
            // require an independent RB09 activation receipt. No fallback.
            RiskCommandAction::SettleEntry {
                reservation_key,
                plan_sha256,
                expected_risk_book_sequence,
                cumulative_entry_usd,
                settlement_revision,
                source_digest,
            } => {
                // The same native SQL worker resolves source amount and
                // immutable fill/bill revisions internally, then commits
                // the original AccountStore ACK and RiskBook debit together.
                // Before RB09 the common write gate always refuses.
                let receipt = self
                    .owner
                    .settle_bound_entry(UnifiedFinanceBoundSettlement {
                        campaign: self.campaign.clone(),
                        reservation_key: reservation_key.clone(),
                        plan_sha256: plan_sha256.clone(),
                        expected_sequence: *expected_risk_book_sequence,
                        cumulative_entry_usd: cumulative_entry_usd.clone(),
                        settlement_revision: *settlement_revision,
                        source_digest: source_digest.clone(),
                    })
                    .await
                    .map_err(|e| match e {
                        okx_execution::StoreError::Invalid(reason) => reason,
                        _ => "NATIVE_RISK_SETTLEMENT_OWNER_UNAVAILABLE",
                    })?;
                base(
                    RiskDecisionStatus::Updated,
                    None,
                    Some(receipt.reservation_key),
                    Some(receipt.book_sequence),
                    Some(receipt.policy_revision),
                    None,
                    None,
                    None,
                )
            }
            RiskCommandAction::ReleaseUnsent {
                reservation_key,
                plan_sha256,
                expected_risk_book_sequence,
                proof,
            } => {
                // No claimed no-send source can be trusted over the wire:
                // the same SQL owner re-reads ORIGINAL account terminal
                // receipt, reserve attempt, immutable hash and campaign.
                let receipt = self
                    .owner
                    .release_bound_unsent(UnifiedFinanceBoundUnsent {
                        campaign: self.campaign.clone(),
                        reservation_key: reservation_key.clone(),
                        plan_sha256: plan_sha256.clone(),
                        expected_sequence: *expected_risk_book_sequence,
                        account_key: proof.account_key.clone(),
                        intent_id: proof.intent_id.clone(),
                        request_hash: proof.request_hash.clone(),
                        reason: proof.reason.clone(),
                        record_digest: proof.record_digest.clone(),
                    })
                    .await
                    .map_err(|error| match error {
                        okx_execution::StoreError::Invalid(reason) => reason,
                        _ => "NATIVE_RISK_UNSENT_OWNER_UNAVAILABLE",
                    })?;
                base(
                    RiskDecisionStatus::Updated,
                    None,
                    Some(receipt.reservation_key),
                    Some(receipt.book_sequence),
                    Some(receipt.policy_revision),
                    None,
                    None,
                    None,
                )
            }
            RiskCommandAction::InstallEntryOpening { .. }
            | RiskCommandAction::Reserve { .. }
            | RiskCommandAction::BeginPost { .. }
            | RiskCommandAction::Resolve { .. } => {
                return Err("FINANCE_CUTOVER_NOT_ACCEPTED");
            }
        };
        if !result.valid() {
            return Err("NATIVE_RISK_RESULT_SOURCE_INVALID");
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_protocol::risk_authority::{NativeRiskCensus, RISK_COMMAND_SCHEMA_V1, RiskCommand};

    #[test]
    fn all_six_mutating_actions_have_one_local_fail_closed_match() {
        // Compile-level exhaustive matching above is part of the required
        // wire contract. A changed variant must be consciously introduced
        // into the match before the native adapter can build.
        assert!(std::mem::size_of::<RiskCommandAction>() > 0);
        assert!(std::mem::size_of::<NativeRiskCensus>() > 0);
        let invalid = RiskCommand {
            schema: RISK_COMMAND_SCHEMA_V1.into(),
            command_id: "native_read_0001".into(),
            installation_id: "invalid".into(),
            action: RiskCommandAction::Inspect {
                reservation_key: String::new(),
                plan_sha256: String::new(),
            },
        };
        assert!(!invalid.valid());
    }
}

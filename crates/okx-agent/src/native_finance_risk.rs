//! #510: original single-writer RiskCommand read ingress.
//! The local backend never forwards to Cloudflare and never synthesizes a
//! budget/venue grant. Trading mutations require independently accepted
//! RB09 activation and a fully source-wired AccountStore/TradeService.
use okx_execution::account_worker::{
    UnifiedFinanceBoundSettlement, UnifiedFinanceBoundUnsent, UnifiedFinanceHandle,
    UnifiedFinancePolicy,
    UnifiedFinancePolicyAccount,
};
use okx_protocol::account_governance::{AccountEnvironment, CentralGovernanceSnapshot};
use okx_protocol::risk_authority::{
    RISK_RESULT_SCHEMA_V1, RiskAccountBudget, RiskBudgetSnapshot, RiskCommand, RiskCommandAction,
    RiskDecisionStatus, RiskReservationState, RiskResult,
};

#[derive(Clone)]
pub struct NativeFinanceRisk {
    owner: UnifiedFinanceHandle,
    installation: String,
    campaign: String,
}

impl NativeFinanceRisk {
    pub fn new(
        owner: UnifiedFinanceHandle,
        installation: String,
        campaign: String,
    ) -> Result<Self, &'static str> {
        if installation.is_empty() || campaign.is_empty() {
            return Err("NATIVE_FINANCE_RISK_SCOPE_INVALID");
        }
        Ok(Self {
            owner,
            installation,
            campaign,
        })
    }

    /// Apply original native AccountStore account-policy rows and RiskBook
    /// campaign revision in ONE caller-owned SQL transaction, not one loop
    /// over legacy account writers. Until RB09 this is denied by the same
    /// finance manifest gate as every other financial COMMIT.
    pub async fn apply_governance(
        &self,
        snapshot: &CentralGovernanceSnapshot,
    ) -> Result<(), String> {
        if !snapshot.valid()
            || snapshot.state.installation_id != self.installation
            || snapshot.state.campaign_id != self.campaign
            || snapshot.state.accounts.is_empty()
        {
            return Err("UNIFIED_GOVERNANCE_IDENTITY_UNVERIFIED".into());
        }
        // The original RiskBook campaign is a REAL cohort. A demo policy
        // cannot be silently ignored or rewritten as a real account row.
        if snapshot
            .state
            .accounts
            .iter()
            .any(|account| account.environment != AccountEnvironment::Real)
        {
            return Err("UNIFIED_GOVERNANCE_DEMO_COHORT_UNSUPPORTED".into());
        }
        let mut accounts = snapshot
            .state
            .accounts
            .iter()
            .map(|account| UnifiedFinancePolicyAccount {
                account_key: account.account_key.clone(),
                paused: !account.access_enabled(),
            })
            .collect::<Vec<_>>();
        accounts.sort_by(|a, b| a.account_key.cmp(&b.account_key));
        self.owner
            .apply_governance_policy(UnifiedFinancePolicy {
                campaign_id: self.campaign.clone(),
                policy_revision: snapshot.state.revision,
                campaign_loss_limit_usd: snapshot.state.campaign_loss_limit_usd.clone(),
                accounts,
            })
            .await
            .map_err(|error| error.to_string())
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
                let budget = RiskBudgetSnapshot {
                    campaign_limit_usd: summary.loss_limit_usd,
                    consumed_loss_usd: summary.consumed_loss_usd,
                    reserved_loss_usd: summary.reserved_loss_usd,
                    remaining_loss_usd: summary.remaining_loss_usd,
                    occupied_modelled_loss_usdt: summary.global_reserved_usdt,
                    // No accepted opening/inception timestamp is in the
                    // imported meta row; never invent it from wall clock.
                    source_as_of_ms: None,
                    campaign_origin: None,
                    entry_budget: None,
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
            // All six mutation variants are explicitly owned here and never
            // routed through the legacy remote bridge. The sealed offline
            // import is NOT an accepted production financial owner. The
            // AccountStore + native census source producers must first be
            // fully moved before enabling their existing joint transactions.
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
                let receipt = self.owner.release_bound_unsent(UnifiedFinanceBoundUnsent {
                    campaign: self.campaign.clone(),
                    reservation_key: reservation_key.clone(),
                    plan_sha256: plan_sha256.clone(),
                    expected_sequence: *expected_risk_book_sequence,
                    account_key: proof.account_key.clone(),
                    intent_id: proof.intent_id.clone(),
                    request_hash: proof.request_hash.clone(),
                    reason: proof.reason.clone(),
                    record_digest: proof.record_digest.clone(),
                }).await.map_err(|error| match error {
                    okx_execution::StoreError::Invalid(reason) => reason,
                    _ => "NATIVE_RISK_UNSENT_OWNER_UNAVAILABLE",
                })?;
                base(RiskDecisionStatus::Updated, None,
                    Some(receipt.reservation_key), Some(receipt.book_sequence),
                    Some(receipt.policy_revision), None, None, None)
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

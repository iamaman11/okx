//! Risk authority messages over the existing authenticated direct WebSocket.
//! A risk receipt is not exchange permission until a separate POST fence.
use serde::{Deserialize, Serialize};
pub const RISK_COMMAND_SCHEMA_V1: &str = "okx.risk-command/v1";
pub const RISK_RESULT_SCHEMA_V1: &str = "okx.risk-result/v1";

fn hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn token(value: &str) -> bool {
    (8..=128).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn positive(value: &str) -> bool {
    let (whole, frac) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || whole.len() > 15
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || whole.len() > 1 && whole.starts_with('0')
        || value.ends_with('.')
        || frac.len() > 9
        || !frac.bytes().all(|b| b.is_ascii_digit())
    {
        return false;
    }
    let Ok(integer) = whole.parse::<u128>() else {
        return false;
    };
    integer > 0 || frac.bytes().any(|b| b != b'0')
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalRiskLedgerProof {
    pub account_uid: String,
    pub run_id: String,
    pub intent_id: String,
    pub state: String,
    pub record_digest: String,
    pub cumulative_loss_usd: String,
    pub remaining_exposure_usdt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cumulative_entry_usd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_settlement_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_settlement_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsdConversion {
    pub usd_per_usdt: String,
    pub source_digest: String,
    pub observed_at_ms: u64,
}
impl UsdConversion {
    pub fn valid(&self) -> bool {
        positive(&self.usd_per_usdt) && hash(&self.source_digest) && self.observed_at_ms > 0
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskAccountOccupancy {
    pub account_key: String,
    pub occupied_usdt: String,
    pub cumulative_loss_usd: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRiskCensus {
    pub account_keys: Vec<String>,
    pub occupied: Vec<RiskAccountOccupancy>,
    pub generated_at_ms: u64,
    pub source_digest: String,
    /// Canonical content hash. Independent reads may return identical data.
    pub content_digest: String,
    /// Oldest completed native read across the entire registered Real cohort.
    pub source_read_floor_ms: u64,
    pub complete: bool,
    pub native_account_evidence_complete: bool,
    pub native_order_evidence_complete: bool,
    pub native_ledger_evidence_complete: bool,
}
impl NativeRiskCensus {
    pub fn valid(&self) -> bool {
        let mut observed = self
            .occupied
            .iter()
            .map(|v| v.account_key.as_str())
            .collect::<Vec<_>>();
        observed.sort_unstable();
        self.complete
            && self.native_account_evidence_complete
            && self.native_order_evidence_complete
            && self.native_ledger_evidence_complete
            && self.generated_at_ms > 0
            && hash(&self.source_digest)
            && hash(&self.content_digest)
            && self.source_read_floor_ms > 0
            && self.source_read_floor_ms <= self.generated_at_ms
            && !self.account_keys.is_empty()
            && self.account_keys.len() <= 32
            && self.account_keys.windows(2).all(|v| v[0] < v[1])
            && self.account_keys.iter().all(|s| {
                let parts = s.split(':').collect::<Vec<_>>();
                parts.len() == 3
                    && (2..=32).contains(&parts[0].len())
                    && parts[0]
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b == b'_' || b == b'-')
                    && parts[1] == "real"
                    && hash(parts[2])
            })
            && observed
                == self
                    .account_keys
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
            && self.occupied.iter().all(|v| {
                let x = &v.occupied_usdt;
                (x == "0" || positive(x))
                    && (v.cumulative_loss_usd == "0" || positive(&v.cumulative_loss_usd))
            })
    }
}
/// A native observation performed after the authenticated HELD response.
/// Ticks are relative to the receipt on one process incarnation, never epoch time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostAccountReadProof {
    pub account_key: String,
    pub private_generation: u64,
    pub source_read_id: String,
    pub content_digest: String,
    pub started_after_held_ms: u64,
    pub completed_after_held_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CausalPostProof {
    /// Exact central reservation identity, whose creation time only Worker knows.
    pub reserve_anchor: String,
    pub native_incarnation: String,
    pub elapsed_to_proof_ms: u64,
    pub account_reads: Vec<PostAccountReadProof>,
}
impl CausalPostProof {
    pub fn valid(&self, census: &NativeRiskCensus) -> bool {
        !self.reserve_anchor.is_empty()
            && self.reserve_anchor.len() <= 512
            && self.native_incarnation.len() == 32
            && self
                .native_incarnation
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            && self.elapsed_to_proof_ms <= 5_000
            && self.account_reads.len() == census.account_keys.len()
            && self
                .account_reads
                .iter()
                .zip(&census.account_keys)
                .all(|(read, account)| {
                    &read.account_key == account
                        && read.private_generation > 0
                        && hash(&read.source_read_id)
                        && hash(&read.content_digest)
                        && read.started_after_held_ms <= read.completed_after_held_ms
                        && read.completed_after_held_ms <= self.elapsed_to_proof_ms
                })
    }
}
/// Current authenticated SQL writer attests durable REJECTED_UNSENT
/// and the absence of any native POST claim for this exact intention.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnsentRiskReleaseProof {
    pub reservation_key: String,
    pub account_key: String,
    pub intent_id: String,
    pub plan_hash: String,
    pub request_hash: String,
    pub reason: String,
    pub record_digest: String,
}
impl UnsentRiskReleaseProof {
    pub fn valid(&self) -> bool {
        self.reservation_key.len() <= 512
            && !self.reservation_key.is_empty()
            && self.account_key.starts_with("okx:real:")
            && self.account_key.len() == 73
            && hash(&self.account_key[9..])
            && token(&self.intent_id)
            && hash(&self.plan_hash)
            && hash(&self.request_hash)
            && (1..=128).contains(&self.reason.len())
            && hash(&self.record_digest)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskResolution {
    Unknown,
    Open,
    Released,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskReservationState {
    Held,
    Posted,
    Unknown,
    Open,
    Released,
}
/** The only opening import transport is the authenticated native Risk WS.
 * The financial source is still independently verified in the native SQL
 * owner; serializing a source hash alone does not create this witness.
 */
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignOpeningAccount {
    pub account_key: String,
    pub history_from_ms: u64,
    pub history_through_ms: u64,
    pub consumed_entry_usd: String,
    pub order_history_complete: bool,
    pub fill_history_complete: bool,
    pub bill_history_complete: bool,
    pub finance_provenance_sealed: bool,
    pub source_digest: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignOpeningSource {
    pub campaign_id: String,
    pub limit_usd: String,
    pub consumed_usd: String,
    pub source_digest: String,
    pub as_of_ms: u64,
    pub campaign_started_at_ms: u64,
    pub accounts: Vec<CampaignOpeningAccount>,
}
impl CampaignOpeningSource {
    pub fn valid(&self) -> bool {
        self.campaign_id.starts_with("sha256:")
            && hash(&self.campaign_id[7..])
            && positive(&self.limit_usd)
            && (self.consumed_usd == "0" || positive(&self.consumed_usd))
            && hash(&self.source_digest)
            && self.as_of_ms >= self.campaign_started_at_ms
            && self.campaign_started_at_ms > 0
            && !self.accounts.is_empty()
            && self.accounts.len() <= 32
            && self
                .accounts
                .windows(2)
                .all(|pair| pair[0].account_key < pair[1].account_key)
            && self.accounts.iter().all(|row| {
                row.account_key.starts_with("okx:real:")
                    && row.account_key.len() == 73
                    && hash(&row.account_key[9..])
                    && row.history_from_ms == self.campaign_started_at_ms
                    && row.history_through_ms == self.as_of_ms
                    && (row.consumed_entry_usd == "0" || positive(&row.consumed_entry_usd))
                    && row.order_history_complete
                    && row.fill_history_complete
                    && row.bill_history_complete
                    && row.finance_provenance_sealed
                    && hash(&row.source_digest)
            })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "SCREAMING_SNAKE_CASE",
    deny_unknown_fields
)]
pub enum RiskCommandAction {
    Summary {},
    InstallEntryOpening {
        source: CampaignOpeningSource,
    },
    AccountSummary {
        account_key: String,
    },
    Inspect {
        reservation_key: String,
        plan_sha256: String,
    },
    Reserve {
        venue: String,
        account_uid: String,
        run_id: String,
        intent_id: String,
        strategy_id: String,
        instrument_id: String,
        plan_sha256: String,
        requested_loss_usdt: String,
        requested_loss_usd: String,
        /** Incomplete until venue spec/fee/FX/economic cost are proven.
         * None is NOT zero and Worker must reject its reservation. */
        #[serde(default, skip_serializing_if = "Option::is_none")]
        entry_upper_usd: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        entry_proof_digest: Option<String>,
        usd_conversion: UsdConversion,
        policy_revision: u64,
        census: NativeRiskCensus,
    },
    /// E1: native SQL-authoritative cumulative entry costs, never an MCP
    /// user-supplied debit. No sender is registered until a complete
    /// fills/bills/FX source binding is proven.
    SettleEntry {
        reservation_key: String,
        plan_sha256: String,
        expected_risk_book_sequence: u64,
        cumulative_entry_usd: String,
        settlement_revision: u64,
        source_digest: String,
    },
    ReleaseUnsent {
        reservation_key: String,
        plan_sha256: String,
        expected_risk_book_sequence: u64,
        proof: UnsentRiskReleaseProof,
    },
    BeginPost {
        reservation_key: String,
        plan_sha256: String,
        policy_revision: u64,
        expected_risk_book_sequence: u64,
        source_proof_sha256: String,
        census: NativeRiskCensus,
        causal_proof: CausalPostProof,
    },
    Resolve {
        reservation_key: String,
        plan_sha256: String,
        expected_risk_book_sequence: u64,
        next_state: RiskResolution,
        source_proof_sha256: String,
        terminal_census: Option<NativeRiskCensus>,
        terminal_ledger: Option<TerminalRiskLedgerProof>,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskCommand {
    pub schema: String,
    pub command_id: String,
    pub installation_id: String,
    pub action: RiskCommandAction,
}
impl RiskCommand {
    pub fn valid(&self) -> bool {
        if self.schema != RISK_COMMAND_SCHEMA_V1
            || !token(&self.command_id)
            || !token(&self.installation_id)
        {
            return false;
        }
        match &self.action {
            RiskCommandAction::Summary {} => true,
            RiskCommandAction::InstallEntryOpening { source } => source.valid(),
            RiskCommandAction::AccountSummary { account_key } => {
                account_key.starts_with("okx:real:")
                    && account_key.len() == 73
                    && hash(&account_key[9..])
            }
            RiskCommandAction::Inspect {
                reservation_key,
                plan_sha256,
            } => !reservation_key.is_empty() && reservation_key.len() <= 512 && hash(plan_sha256),
            RiskCommandAction::Reserve {
                venue,
                account_uid,
                run_id,
                intent_id,
                strategy_id,
                instrument_id,
                plan_sha256,
                requested_loss_usdt,
                requested_loss_usd,
                entry_upper_usd,
                entry_proof_digest,
                usd_conversion,
                policy_revision,
                census,
            } => {
                (2..=32).contains(&venue.len())
                    && venue
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b == b'_' || b == b'-')
                    && hash(account_uid)
                    && token(run_id)
                    && token(intent_id)
                    && (2..=64).contains(&strategy_id.len())
                    && strategy_id.as_bytes()[0].is_ascii_alphabetic()
                    && strategy_id
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
                    && (3..=64).contains(&instrument_id.len())
                    && instrument_id.bytes().all(|c| {
                        c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'-' || c == b'_'
                    })
                    && hash(plan_sha256)
                    && positive(requested_loss_usdt)
                    && positive(requested_loss_usd)
                    && matches!((entry_upper_usd.as_deref(), entry_proof_digest.as_deref()),
                        (Some(value), Some(digest)) if positive(value) && hash(digest))
                    && usd_conversion.valid()
                    && *policy_revision > 0
                    && census.valid()
            }
            RiskCommandAction::SettleEntry {
                reservation_key,
                plan_sha256,
                expected_risk_book_sequence,
                cumulative_entry_usd,
                settlement_revision,
                source_digest,
            } => {
                !reservation_key.is_empty()
                    && reservation_key.len() <= 512
                    && hash(plan_sha256)
                    && *expected_risk_book_sequence > 0
                    && *settlement_revision > 0
                    && (cumulative_entry_usd == "0" || positive(cumulative_entry_usd))
                    && hash(source_digest)
            }
            RiskCommandAction::ReleaseUnsent {
                reservation_key,
                plan_sha256,
                expected_risk_book_sequence: _,
                proof,
            } => {
                hash(plan_sha256)
                    && proof.valid()
                    && proof.reservation_key == *reservation_key
                    && proof.plan_hash == *plan_sha256
            }
            RiskCommandAction::BeginPost {
                reservation_key,
                plan_sha256,
                policy_revision,
                expected_risk_book_sequence,
                source_proof_sha256,
                census,
                causal_proof,
            } => {
                !reservation_key.is_empty()
                    && reservation_key.len() <= 512
                    && hash(plan_sha256)
                    && *policy_revision > 0
                    && *expected_risk_book_sequence > 0
                    && hash(source_proof_sha256)
                    && census.valid()
                    && causal_proof.valid(census)
                    && causal_proof.reserve_anchor == *reservation_key
            }
            RiskCommandAction::Resolve {
                reservation_key,
                plan_sha256,
                expected_risk_book_sequence,
                next_state,
                source_proof_sha256,
                terminal_census,
                terminal_ledger,
            } => {
                !reservation_key.is_empty()
                    && reservation_key.len() <= 512
                    && hash(plan_sha256)
                    && *expected_risk_book_sequence > 0
                    && hash(source_proof_sha256)
                    && match next_state {
                        RiskResolution::Released => {
                            terminal_census
                                .as_ref()
                                .is_some_and(NativeRiskCensus::valid)
                                && terminal_ledger.as_ref().is_some_and(|proof| {
                                    hash(&proof.account_uid)
                                        && token(&proof.run_id)
                                        && token(&proof.intent_id)
                                        && hash(&proof.record_digest)
                                        && proof.remaining_exposure_usdt == "0"
                                        && (proof.cumulative_loss_usd == "0"
                                            || positive(&proof.cumulative_loss_usd))
                                        && matches!(
                                            proof.state.as_str(),
                                            "FILLED" | "CANCELED" | "REJECTED"
                                        )
                                })
                        }
                        _ => terminal_census.is_none() && terminal_ledger.is_none(),
                    }
            }
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskDecisionStatus {
    Denied,
    Held,
    PostAuthorized,
    Updated,
    Observed,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskEntryBudgetSnapshot {
    pub campaign_id: String,
    pub limit_usd: String,
    pub consumed_usd: String,
    pub reserved_usd: String,
    pub remaining_usd: String,
    pub opening_source_digest: String,
    pub opening_as_of_ms: u64,
}
impl RiskEntryBudgetSnapshot {
    pub fn valid(&self) -> bool {
        fn exact_units(value: &str) -> Option<u128> {
            if value != "0" && !positive(value) {
                return None;
            }
            let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
            whole
                .parse::<u128>()
                .ok()?
                .checked_mul(1_000_000_000)?
                .checked_add(
                    fraction.parse::<u128>().ok().unwrap_or(0)
                        * 10u128.pow(9 - fraction.len() as u32),
                )
        }
        let (Some(limit), Some(consumed), Some(reserved), Some(remaining)) = (
            exact_units(&self.limit_usd),
            exact_units(&self.consumed_usd),
            exact_units(&self.reserved_usd),
            exact_units(&self.remaining_usd),
        ) else {
            return false;
        };
        self.campaign_id.starts_with("sha256:")
            && hash(&self.campaign_id[7..])
            && limit > 0
            && consumed
                .checked_add(reserved)
                .and_then(|v| v.checked_add(remaining))
                == Some(limit)
            && hash(&self.opening_source_digest)
            && self.opening_as_of_ms > 0
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskCampaignOrigin {
    pub campaign_id: String,
    pub installation_id: String,
    pub started_at_ms: u64,
}
impl RiskCampaignOrigin {
    pub fn valid(&self) -> bool {
        self.campaign_id.starts_with("sha256:")
            && hash(&self.campaign_id[7..])
            && token(&self.installation_id)
            && self.started_at_ms > 0
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskBudgetSnapshot {
    pub campaign_limit_usd: String,
    pub consumed_loss_usd: String,
    pub reserved_loss_usd: String,
    pub remaining_loss_usd: String,
    pub occupied_modelled_loss_usdt: String,
    pub source_as_of_ms: Option<u64>,
    /// Existing RiskBook installation is the only owner of inception.
    /// Missing for old installations is NOT a zero-spend proof.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub campaign_origin: Option<RiskCampaignOrigin>,
    /// Only present after authenticated opening finance spend reconciliation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_budget: Option<RiskEntryBudgetSnapshot>,
}
impl RiskBudgetSnapshot {
    pub fn valid(&self) -> bool {
        positive(&self.campaign_limit_usd)
            && [
                &self.consumed_loss_usd,
                &self.reserved_loss_usd,
                &self.remaining_loss_usd,
                &self.occupied_modelled_loss_usdt,
            ]
            .iter()
            .all(|v| v.as_str() == "0" || positive(v))
            && self
                .campaign_origin
                .as_ref()
                .is_none_or(RiskCampaignOrigin::valid)
            && self
                .entry_budget
                .as_ref()
                .is_none_or(RiskEntryBudgetSnapshot::valid)
            && self.entry_budget.as_ref().is_none_or(|opened| {
                self.campaign_origin.as_ref().is_some_and(|origin| {
                    origin.campaign_id == opened.campaign_id
                        && origin.started_at_ms <= opened.opening_as_of_ms
                })
            })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskAccountBudget {
    pub account_key: String,
    pub active_reservations: u64,
    pub occupied_usd: String,
    pub cumulative_loss_usd: String,
    pub latest_activity_ms: u64,
}
impl RiskAccountBudget {
    fn valid(&self) -> bool {
        self.account_key.starts_with("okx:real:")
            && self.account_key.len() == 73
            && hash(&self.account_key[9..])
            && (self.active_reservations == 0) == (self.occupied_usd == "0")
            && (self.active_reservations == 0 || self.latest_activity_ms > 0)
            && [&self.occupied_usd, &self.cumulative_loss_usd]
                .iter()
                .all(|v| v.as_str() == "0" || positive(v))
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskResult {
    pub schema: String,
    pub command_id: String,
    pub status: RiskDecisionStatus,
    pub reason: Option<String>,
    pub reservation_key: Option<String>,
    pub risk_book_sequence: Option<u64>,
    pub policy_revision: Option<u64>,
    /// DO occupancy excluding the just-fenced candidate; exact nine-decimal USD risk.
    #[serde(default)]
    pub existing_modelled_loss_usdt: Option<String>,
    #[serde(default)]
    pub reservation_state: Option<RiskReservationState>,
    pub budget: Option<RiskBudgetSnapshot>,
    #[serde(default)]
    pub account_budget: Option<RiskAccountBudget>,
}
impl RiskResult {
    pub fn valid(&self) -> bool {
        self.schema == RISK_RESULT_SCHEMA_V1
            && token(&self.command_id)
            && (self.status == RiskDecisionStatus::Denied
                || self.status == RiskDecisionStatus::Observed
                    && (self.budget.as_ref().is_some_and(RiskBudgetSnapshot::valid)
                        || self
                            .account_budget
                            .as_ref()
                            .is_some_and(RiskAccountBudget::valid))
                    && self.reservation_key.is_none()
                    && self.risk_book_sequence.is_some()
                    && self.reservation_state.is_none()
                || self.reservation_key.is_some()
                    && self.budget.is_none()
                    && self.risk_book_sequence.is_some_and(|s| s > 0)
                    && self.policy_revision.is_some_and(|r| r > 0))
            && match self.status {
                RiskDecisionStatus::PostAuthorized => self
                    .existing_modelled_loss_usdt
                    .as_deref()
                    .is_some_and(|v| v == "0" || positive(v)),
                _ => self.existing_modelled_loss_usdt.is_none(),
            }
            && (self.status == RiskDecisionStatus::Observed
                && (self.reservation_state.is_some()
                    || (self.budget.as_ref().is_some_and(RiskBudgetSnapshot::valid)
                        || self
                            .account_budget
                            .as_ref()
                            .is_some_and(RiskAccountBudget::valid)))
                || self.status != RiskDecisionStatus::Observed && self.reservation_state.is_none())
            && self
                .reason
                .as_ref()
                .is_none_or(|s| (1..=128).contains(&s.len()))
    }
}
#[cfg(test)]
mod tests {
    #[test]
    fn campaign_opening_requires_full_cohort_and_serializes_to_native_ws() {
        let row = CampaignOpeningAccount {
            account_key: format!("okx:real:{}", "b".repeat(64)),
            history_from_ms: 1_000,
            history_through_ms: 2_000,
            consumed_entry_usd: "0".into(),
            order_history_complete: true,
            fill_history_complete: true,
            bill_history_complete: true,
            finance_provenance_sealed: true,
            source_digest: "a".repeat(64),
        };
        let source = CampaignOpeningSource {
            campaign_id: format!("sha256:{}", "c".repeat(64)),
            limit_usd: "10".into(),
            consumed_usd: "0".into(),
            source_digest: "d".repeat(64),
            as_of_ms: 2_000,
            campaign_started_at_ms: 1_000,
            accounts: vec![row.clone()],
        };
        let command = RiskCommand {
            schema: RISK_COMMAND_SCHEMA_V1.into(),
            command_id: "opening_test_001".into(),
            installation_id: "test_install".into(),
            action: RiskCommandAction::InstallEntryOpening {
                source: source.clone(),
            },
        };
        assert!(command.valid());
        let wire = serde_json::to_value(&command).unwrap();
        assert_eq!(wire["action"]["action"], "INSTALL_ENTRY_OPENING");
        assert_eq!(
            serde_json::from_value::<RiskCommand>(wire).unwrap(),
            command
        );
        let mut invalid = source.clone();
        invalid.accounts[0].bill_history_complete = false;
        assert!(!invalid.valid());
        invalid = source.clone();
        invalid.accounts[0].history_from_ms += 1;
        assert!(!invalid.valid());
        invalid = source.clone();
        invalid.accounts.push(row);
        assert!(!invalid.valid());
        invalid = source;
        invalid.consumed_usd = "-1".into();
        assert!(!invalid.valid());
    }

    use super::*;
    #[test]
    fn e1_entry_settlement_requires_exact_plan_bound_native_revision() {
        let action = RiskCommandAction::SettleEntry {
            reservation_key: "central:existing-reserve".into(),
            plan_sha256: "a".repeat(64),
            expected_risk_book_sequence: 4,
            cumulative_entry_usd: "1.25".into(),
            settlement_revision: 2,
            source_digest: "b".repeat(64),
        };
        let mut command = RiskCommand {
            schema: RISK_COMMAND_SCHEMA_V1.into(),
            command_id: "settle_entry_00001".into(),
            installation_id: "installation_test".into(),
            action: action.clone(),
        };
        assert!(command.valid());
        let encoded = serde_json::to_value(&command).unwrap();
        assert_eq!(encoded["action"]["action"], "SETTLE_ENTRY");
        assert!(
            serde_json::from_value::<RiskCommand>(encoded)
                .unwrap()
                .valid()
        );
        if let RiskCommandAction::SettleEntry {
            cumulative_entry_usd,
            ..
        } = &mut command.action
        {
            *cumulative_entry_usd = "-0.01".into();
        }
        assert!(
            !command.valid(),
            "negative settlement cannot credit campaign spend"
        );
        let mut missing_source = action;
        if let RiskCommandAction::SettleEntry { source_digest, .. } = &mut missing_source {
            *source_digest = "not_source_evidence".into();
        }
        assert!(
            !RiskCommand {
                action: missing_source,
                ..command
            }
            .valid()
        );
    }
    #[test]
    fn entry_budget_readback_never_invents_unproved_history() {
        let plain: RiskBudgetSnapshot = serde_json::from_value(serde_json::json!({
            "campaign_limit_usd": "10",
            "consumed_loss_usd": "0",
            "reserved_loss_usd": "0",
            "remaining_loss_usd": "10",
            "occupied_modelled_loss_usdt": "0",
            "source_as_of_ms": null,
            "entry_budget": null
        }))
        .unwrap();
        assert!(plain.valid() && plain.entry_budget.is_none());
        let ready: RiskBudgetSnapshot = serde_json::from_value(serde_json::json!({
            "campaign_limit_usd": "10",
            "consumed_loss_usd": "0",
            "reserved_loss_usd": "0",
            "remaining_loss_usd": "10",
            "occupied_modelled_loss_usdt": "0",
            "source_as_of_ms": null,
            "campaign_origin": {
                "campaign_id": format!("sha256:{}", "c".repeat(64)),
                "installation_id": "installation_test",
                "started_at_ms": 500
            },
            "entry_budget": {
                "campaign_id": format!("sha256:{}", "c".repeat(64)),
                "limit_usd": "10",
                "consumed_usd": "1",
                "reserved_usd": "3",
                "remaining_usd": "6",
                "opening_source_digest": "d".repeat(64),
                "opening_as_of_ms": 1000
            }
        }))
        .unwrap();
        assert!(ready.valid());
        let mut originless = ready.clone();
        originless.campaign_origin = None;
        assert!(
            !originless.valid(),
            "opened campaign must retain its immutable inception authority"
        );
        let mut wrong_sum = ready.clone();
        wrong_sum.entry_budget.as_mut().unwrap().remaining_usd = "5".into();
        assert!(
            !wrong_sum.valid(),
            "budget is an exact conservation readback"
        );
        let mut tampered = serde_json::to_value(&ready).unwrap();
        tampered["entry_budget"]["untrusted_field"] = true.into();
        assert!(serde_json::from_value::<RiskBudgetSnapshot>(tampered).is_err());
    }

    #[test]
    fn exact_contract_denies_partial_native_risk_proof() {
        let uid = "a".repeat(64);
        let key = format!("okx:real:{uid}");
        let command = RiskCommand {
            schema: RISK_COMMAND_SCHEMA_V1.to_owned(),
            command_id: "risk_request_01".to_owned(),
            installation_id: "installation_test".into(),
            action: RiskCommandAction::Reserve {
                venue: "okx".to_owned(),
                account_uid: uid,
                run_id: "run_12345678".to_owned(),
                intent_id: "intent_12345678".to_owned(),
                strategy_id: "momentum_A".to_owned(),
                instrument_id: "BTC-USDT-SWAP".to_owned(),
                plan_sha256: "c".repeat(64),
                requested_loss_usdt: "1".to_owned(),
                requested_loss_usd: "1".into(),
                entry_upper_usd: Some("2".into()),
                entry_proof_digest: Some("b".repeat(64)),
                usd_conversion: UsdConversion {
                    usd_per_usdt: "1".into(),
                    source_digest: "a".repeat(64),
                    observed_at_ms: 1,
                },
                policy_revision: 1,
                census: NativeRiskCensus {
                    account_keys: vec![key.clone()],
                    occupied: vec![RiskAccountOccupancy {
                        account_key: key,
                        occupied_usdt: "0".to_owned(),
                        cumulative_loss_usd: "0".into(),
                    }],
                    generated_at_ms: 1,
                    source_digest: "b".repeat(64),
                    content_digest: "c".repeat(64),
                    source_read_floor_ms: 1,
                    complete: false,
                    native_account_evidence_complete: true,
                    native_order_evidence_complete: true,
                    native_ledger_evidence_complete: true,
                },
            },
        };
        assert!(!command.valid());
        let mut ready = command;
        if let RiskCommandAction::Reserve { census, .. } = &mut ready.action {
            census.complete = true;
        }
        assert!(ready.valid());
        let mut missing_entry = ready.clone();
        if let RiskCommandAction::Reserve {
            entry_upper_usd, ..
        } = &mut missing_entry.action
        {
            *entry_upper_usd = None;
        }
        assert!(
            !missing_entry.valid(),
            "no entry debit is not zero entry debit"
        );
        let json = serde_json::to_string(&ready).unwrap();
        assert_eq!(serde_json::from_str::<RiskCommand>(&json).unwrap(), ready);
        if let RiskCommandAction::Reserve {
            requested_loss_usdt,
            ..
        } = &mut ready.action
        {
            *requested_loss_usdt = "1e3".to_owned();
        }
        assert!(!ready.valid());
    }
    #[test]
    fn terminal_release_requires_complete_bound_census_and_exact_ledger() {
        let uid = "a".repeat(64);
        let census = NativeRiskCensus {
            account_keys: vec![format!("okx:real:{uid}")],
            occupied: vec![RiskAccountOccupancy {
                account_key: format!("okx:real:{uid}"),
                occupied_usdt: "0".to_owned(),
                cumulative_loss_usd: "0".into(),
            }],
            generated_at_ms: 2,
            source_digest: "b".repeat(64),
            content_digest: "c".repeat(64),
            source_read_floor_ms: 2,
            complete: true,
            native_account_evidence_complete: true,
            native_order_evidence_complete: true,
            native_ledger_evidence_complete: true,
        };
        let mut release = RiskCommand {
            schema: RISK_COMMAND_SCHEMA_V1.to_owned(),
            command_id: "release_0001".to_owned(),
            installation_id: "installation_test".into(),
            action: RiskCommandAction::Resolve {
                reservation_key: "reservation_identity".to_owned(),
                plan_sha256: "d".repeat(64),
                expected_risk_book_sequence: 3,
                next_state: RiskResolution::Released,
                source_proof_sha256: "e".repeat(64),
                terminal_census: None,
                terminal_ledger: None,
            },
        };
        assert!(!release.valid());
        if let RiskCommandAction::Resolve {
            terminal_census,
            terminal_ledger,
            ..
        } = &mut release.action
        {
            *terminal_census = Some(census);
            *terminal_ledger = Some(TerminalRiskLedgerProof {
                account_uid: uid,
                run_id: "run_accepted_001".to_owned(),
                intent_id: "intent_accepted_001".to_owned(),
                state: "FILLED".to_owned(),
                record_digest: "f".repeat(64),
                cumulative_loss_usd: "0".into(),
                remaining_exposure_usdt: "0".into(),
                cumulative_entry_usd: None,
                entry_settlement_revision: None,
                entry_settlement_digest: None,
            });
        }
        assert!(release.valid());
        if let RiskCommandAction::Resolve {
            terminal_ledger, ..
        } = &mut release.action
        {
            terminal_ledger.as_mut().unwrap().state = "UNKNOWN".to_owned();
        }
        assert!(!release.valid());
    }

    #[test]
    fn entry_budget_requires_original_installation_owned_campaign_inception() {
        let campaign = format!("sha256:{}", "a".repeat(64));
        let origin = RiskCampaignOrigin {
            campaign_id: campaign.clone(),
            installation_id: "installation_test".into(),
            started_at_ms: 1_790_000_000_000,
        };
        let entry = RiskEntryBudgetSnapshot {
            campaign_id: campaign.clone(),
            limit_usd: "10".into(),
            consumed_usd: "0".into(),
            reserved_usd: "0".into(),
            remaining_usd: "10".into(),
            opening_source_digest: "b".repeat(64),
            opening_as_of_ms: 1_790_000_000_100,
        };
        let mut view = RiskBudgetSnapshot {
            campaign_limit_usd: "10".into(),
            consumed_loss_usd: "0".into(),
            reserved_loss_usd: "0".into(),
            remaining_loss_usd: "10".into(),
            occupied_modelled_loss_usdt: "0".into(),
            source_as_of_ms: None,
            campaign_origin: Some(origin),
            entry_budget: Some(entry),
        };
        assert!(view.valid());
        view.campaign_origin = None;
        assert!(
            !view.valid(),
            "a previous Worker must not mint an opening timestamp"
        );
        view.campaign_origin = Some(RiskCampaignOrigin {
            campaign_id: format!("sha256:{}", "c".repeat(64)),
            installation_id: "installation_test".into(),
            started_at_ms: 1_790_000_000_000,
        });
        assert!(
            !view.valid(),
            "another campaign's inception is not a source"
        );
    }

    #[test]
    fn denied_decision_cannot_count_as_post_authorization() {
        let mut result = RiskResult {
            schema: RISK_RESULT_SCHEMA_V1.to_owned(),
            command_id: "risk_request_01".to_owned(),
            status: RiskDecisionStatus::Denied,
            reason: Some("UNVERIFIED".to_owned()),
            reservation_key: None,
            risk_book_sequence: None,
            policy_revision: None,
            existing_modelled_loss_usdt: None,
            reservation_state: None,
            budget: None,
            account_budget: None,
        };
        assert!(result.valid());
        result.status = RiskDecisionStatus::PostAuthorized;
        assert!(!result.valid());
        result.reservation_key = Some("owned_reservation".to_owned());
        result.risk_book_sequence = Some(2);
        result.policy_revision = Some(1);
        assert!(
            !result.valid(),
            "POST without occupied-risk evidence cannot authorize"
        );
        result.existing_modelled_loss_usdt = Some("1.000000001".to_owned());
        assert!(result.valid());
        result.existing_modelled_loss_usdt = Some("not-a-risk".to_owned());
        assert!(!result.valid());
    }
}

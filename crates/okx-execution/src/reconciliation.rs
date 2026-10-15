use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use okx_observation::{
    AccountLedgerFacts, AccountPositionState, ExchangeBillIdentity, ExchangeFillIdentity,
    ExchangeOrderIdentity,
};
use rust_decimal::Decimal;
use serde::Serialize;
use thiserror::Error;

use crate::{
    DurableExecutionLedger, ExecutionAction, ExecutionState, PositionSide, ProtectiveOrderStatus,
    StrategyCapitalReservation,
};

pub const ACCOUNT_LEDGER_RECONCILIATION_SCHEMA_V1: &str = "okx.account-ledger-reconciliation/v1";
const MAX_POSITION_ATTRIBUTION_ROWS: usize = 20;
const MAX_ORIGIN_COHORT_ROWS: usize = 8;
const MAX_ARCHIVE_PROOF_ROWS: usize = 16;
pub const STRATEGY_ATTRIBUTION_SCHEMA_V1: &str = "okx.strategy-exchange-attribution/v1";
pub const STRATEGY_NAV_ASSESSMENT_SCHEMA_V1: &str = "okx.strategy-nav-assessment/v1";

/// Observed *linked* exchange components; not a strategy NAV or account-equity proxy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LinkedCurrencyAmount {
    pub currency: String,
    pub amount: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StrategyExchangeAttribution {
    pub schema: &'static str,
    pub status: &'static str,
    pub strategy_nav_available: bool,
    pub managed_intents: usize,
    pub lineage_bound_intents: usize,
    pub exact_orders: usize,
    pub exact_fills: usize,
    pub exact_trade_bills: usize,
    pub duplicate_fill_or_bill_claims: usize,
    pub unmatched_lineage_fills: usize,
    pub missing_exact_bill_links: usize,
    pub fee_bill_mismatches: usize,
    pub fill_pnl_bill_mismatches: usize,
    pub unallocated_funding_bills: usize,
    pub unallocated_funding_totals: Vec<LinkedCurrencyAmount>,
    pub unlinked_nonfunding_bills: usize,
    pub orders_history_complete: bool,
    pub fills_history_complete: bool,
    /// Different decision-origin/mandate cohorts are NOT a single strategy NAV.
    pub origin_cohorts_total: usize,
    pub origin_cohorts_truncated: bool,
    pub origin_cohorts: Vec<AttributionOriginCohort>,
    /// ONLY independently corroborated fill/bill amounts, not all fill fees.
    pub fills_fee_totals: Vec<LinkedCurrencyAmount>,
    pub linked_trade_bill_pnl_totals: Vec<LinkedCurrencyAmount>,
    pub incomplete_reasons: Vec<&'static str>,
}

/// Grouping by immutable execution provenance and declared mandate only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AttributionOriginCohort {
    pub origin_schema: String,
    pub origin_version: String,
    pub mandate_version: Option<String>,
    pub declared_capital_usd: Option<String>,
    pub lineage_bound_intents: usize,
    pub exact_trade_bills: usize,
    pub verified_fees: Vec<LinkedCurrencyAmount>,
    pub verified_realized_pnl: Vec<LinkedCurrencyAmount>,
}

#[derive(Default)]
struct AttributionCohortAccumulator {
    intents: usize,
    exact_trade_bills: usize,
    fees: BTreeMap<String, Decimal>,
    pnl: BTreeMap<String, Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StrategyNavAssessment {
    pub schema: &'static str,
    pub status: &'static str,
    pub usable_for_risk: bool,
    pub allocation_id: String,
    pub allocated_usdt: String,
    pub exact_trade_bills: usize,
    pub verified_realized_usdt: String,
    pub verified_fees_usdt: String,
    pub nav_usdt: Option<String>,
    pub reasons: Vec<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PositionAttributionDiagnostic {
    pub instrument_id: String,
    pub position_side: String,
    pub authoritative_exchange_position: String,
    pub managed_fill_delta_in_bounded_history: String,
    pub unattributed_external_or_outside_bounded_history_residual: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountLedgerReconciliation {
    pub schema: &'static str,
    pub managed_intents: usize,
    pub managed_exchange_orders_observed: usize,
    pub managed_exchange_fills_observed: usize,
    pub unattributed_external_or_exchange_system_orders: usize,
    pub unattributed_external_or_exchange_system_fills: usize,
    pub managed_intents_with_exchange_match: usize,
    pub managed_intents_unresolved_in_bounded_exchange_evidence: usize,
    pub unexpected_exchange_orders_for_non_submitted_intents: usize,
    pub unexpected_exchange_fills_for_non_submitted_intents: usize,
    pub identity_mismatches: usize,
    pub position_attribution_total: usize,
    pub position_attribution_residual_count: usize,
    pub position_attribution_truncated: bool,
    pub position_attribution: Vec<PositionAttributionDiagnostic>,
    pub position_attribution_unavailable_events: usize,
    pub consistent: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy_attribution: Option<StrategyExchangeAttribution>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy_nav: Option<StrategyNavAssessment>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exchange_evidence: Option<crate::ExchangeEvidenceSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub financial_period: Option<crate::StrategyFinancialPeriod>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy_nav_watermark: Option<crate::StrategyNavPoint>,
    pub strategy_nav_checkpoint_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy_risk_stop: Option<crate::ExecutionRiskStop>,
    /// Immutable operator-authorized STOP epoch archive from this same ledger.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub strategy_stop_rearm_proofs: Vec<crate::StrategyStopRearm>,
    /// Immutable past result; independent of current source-window age.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finalized_run: Option<crate::StrategyFinalization>,
    pub archived_runs_count: usize,
    /// Immutable archived period hashes and financial totals (maximum 16).
    pub archived_run_proofs: Vec<crate::ArchivedStrategyProof>,
    pub archived_run_proofs_truncated: bool,
    pub unverified_runs_count: usize,
    pub unverified_run_proofs_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unverified_run: Option<crate::StrategyUnverifiedClosure>,
    pub unverified_run_proofs: Vec<crate::UnverifiedStrategyProof>,
    pub archived_run_slots_remaining: usize,
    pub source_checkpoint_deferred: bool,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AccountLedgerReconciliationError {
    #[error("multiple exchange orders resolve to managed client order id '{client_order_id}'")]
    DuplicateManagedExchangeOrder { client_order_id: String },

    #[error("position attribution decimal '{field}' is invalid: '{value}'")]
    InvalidDecimal { field: &'static str, value: String },
}

pub const MANAGED_EXECUTION_INVENTORY_SCHEMA_V1: &str = "okx.managed-execution-inventory/v1";
pub const MAX_MANAGED_EXECUTION_INVENTORY_ROWS: usize = 16;

/// Account-scoped durable identities only. No risk policy, private credentials or
/// mutable plan content are included; the existing account_summary transport
/// remains the sole read-only observation path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ManagedExecutionIdentity {
    pub intent_id: String,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub account_uid_fingerprint: String,
    pub instrument_id: String,
    pub action: ExecutionAction,
    pub position_side: PositionSide,
    pub state: ExecutionState,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub protective_algo_client_id: Option<String>,
    pub protective_algo_order_id: Option<String>,
    pub protective_state: Option<ProtectiveOrderStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ManagedExecutionInventory {
    pub schema: &'static str,
    pub total: usize,
    pub shown: usize,
    pub complete_within_bound: bool,
    pub rows: Vec<ManagedExecutionIdentity>,
}

/// Bounded deterministic inventory for resolving pre-existing Demo intent IDs.
/// Truncation is explicit and cannot be used as a proof of absence.
pub fn managed_execution_inventory(ledger: &DurableExecutionLedger) -> ManagedExecutionInventory {
    let mut entries = ledger.entries().collect::<Vec<_>>();
    let total = entries.len();
    entries.sort_by(|a, b| {
        b.updated_at_ms
            .cmp(&a.updated_at_ms)
            .then_with(|| a.record.plan.intent_id.cmp(&b.record.plan.intent_id))
    });
    let rows = entries
        .into_iter()
        .take(MAX_MANAGED_EXECUTION_INVENTORY_ROWS)
        .map(|entry| {
            let plan = &entry.record.plan;
            let protection = entry.record.protection.as_ref();
            ManagedExecutionIdentity {
                intent_id: plan.intent_id.clone(),
                client_order_id: plan.client_order_id.clone(),
                exchange_order_id: entry.record.order_id.clone(),
                account_uid_fingerprint: plan.account_uid_fingerprint.clone(),
                instrument_id: plan.instrument_id.clone(),
                action: plan.action,
                position_side: plan.position_side,
                state: entry.record.state,
                created_at_ms: entry.created_at_ms,
                updated_at_ms: entry.updated_at_ms,
                protective_algo_client_id: protection
                    .map(|value| value.algo_client_order_id.clone()),
                protective_algo_order_id: protection.and_then(|value| value.algo_order_id.clone()),
                protective_state: protection.map(|value| value.status),
            }
        })
        .collect::<Vec<_>>();
    ManagedExecutionInventory {
        schema: MANAGED_EXECUTION_INVENTORY_SCHEMA_V1,
        total,
        shown: rows.len(),
        complete_within_bound: total <= MAX_MANAGED_EXECUTION_INVENTORY_ROWS,
        rows,
    }
}

pub fn reconcile_account_ledger(
    ledger: &DurableExecutionLedger,
    facts: &AccountLedgerFacts,
) -> Result<AccountLedgerReconciliation, AccountLedgerReconciliationError> {
    let mut result = reconcile_exchange_evidence(
        ledger,
        &facts.authoritative_positions,
        &facts.exchange_orders,
        &facts.exchange_fills,
    )?;
    result.strategy_nav = ledger
        .capital_reservation()
        .map(|capital| assess_flat_strategy_nav(capital, ledger, facts))
        .transpose()?;
    result.exchange_evidence = ledger.exchange_evidence_summary();
    result.strategy_nav_watermark = ledger.strategy_nav_watermark().cloned();
    result.strategy_nav_checkpoint_count = ledger.strategy_nav_point_count();
    result.strategy_risk_stop = ledger.risk_stop().cloned();
    result.strategy_stop_rearm_proofs = ledger.stop_rearm_proofs();
    result.financial_period = ledger.financial_period(facts, false);
    result.finalized_run = ledger.finalized_run().cloned();
    result.archived_runs_count = ledger.archived_runs_count();
    result.unverified_runs_count = ledger.archived_unverified_count();
    let archived = ledger.archived_strategy_proofs();
    result.archived_run_proofs_truncated = archived.len() > MAX_ARCHIVE_PROOF_ROWS;
    result.archived_run_proofs = archived
        .into_iter()
        .rev()
        .take(MAX_ARCHIVE_PROOF_ROWS)
        .collect::<Vec<_>>();
    result.archived_run_proofs.reverse();
    result.unverified_run = ledger.unverified_run().cloned();
    let unverified = ledger.archived_unverified_proofs();
    result.unverified_run_proofs_truncated = unverified.len() > MAX_ARCHIVE_PROOF_ROWS;
    result.unverified_run_proofs = unverified
        .into_iter()
        .rev()
        .take(MAX_ARCHIVE_PROOF_ROWS)
        .collect::<Vec<_>>();
    result.unverified_run_proofs.reverse();
    result.archived_run_slots_remaining =
        crate::MAX_ARCHIVED_STRATEGY_RUNS.saturating_sub(ledger.archived_storage_count());
    result.strategy_attribution = Some(attribute_strategy_exchange_evidence(
        ledger,
        &facts.exchange_orders,
        &facts.exchange_fills,
        &facts.exchange_bills,
        facts.bills_history_complete,
        facts.exchange_orders_history_complete,
        facts.exchange_fills_history_complete,
    )?);
    Ok(result)
}

fn reconcile_exchange_evidence(
    ledger: &DurableExecutionLedger,
    authoritative_positions: &[AccountPositionState],
    exchange_orders: &[ExchangeOrderIdentity],
    exchange_fills: &[ExchangeFillIdentity],
) -> Result<AccountLedgerReconciliation, AccountLedgerReconciliationError> {
    let entries = ledger.entries().collect::<Vec<_>>();
    let managed_clients = entries
        .iter()
        .map(|entry| entry.record.plan.client_order_id.as_str())
        .collect::<BTreeSet<_>>();
    let managed_order_ids = entries
        .iter()
        .filter_map(|entry| entry.record.order_id.as_deref())
        .collect::<BTreeSet<_>>();

    let managed_exchange_orders_observed = exchange_orders
        .iter()
        .filter(|order| managed_clients.contains(order.client_order_id.as_str()))
        .count();
    let unattributed_orders = exchange_orders.len() - managed_exchange_orders_observed;

    let managed_exchange_fills_observed = exchange_fills
        .iter()
        .filter(|fill| {
            managed_clients.contains(fill.client_order_id.as_str())
                || fill
                    .order_id
                    .as_deref()
                    .is_some_and(|order_id| managed_order_ids.contains(order_id))
        })
        .count();
    let unattributed_fills = exchange_fills.len() - managed_exchange_fills_observed;

    let mut by_client = BTreeMap::<&str, Vec<_>>::new();
    for order in exchange_orders {
        if !order.client_order_id.trim().is_empty() {
            by_client
                .entry(order.client_order_id.as_str())
                .or_default()
                .push(order);
        }
    }

    let mut matched = 0usize;
    let mut unresolved = 0usize;
    let mut unexpected_orders = 0usize;
    let mut unexpected_fills = 0usize;
    let mut identity_mismatches = 0usize;

    for entry in entries {
        let plan = &entry.record.plan;
        let matches = by_client
            .get(plan.client_order_id.as_str())
            .cloned()
            .unwrap_or_default();
        if matches.len() > 1 {
            return Err(
                AccountLedgerReconciliationError::DuplicateManagedExchangeOrder {
                    client_order_id: plan.client_order_id.clone(),
                },
            );
        }

        let observed = matches.first().copied();
        if let Some(order) = observed {
            if order.instrument_id != plan.instrument_id {
                identity_mismatches += 1;
                continue;
            }
            if let Some(expected_order_id) = entry.record.order_id.as_deref()
                && order.order_id != expected_order_id
            {
                identity_mismatches += 1;
                continue;
            }
        }

        let fill_observed = exchange_fills.iter().any(|fill| {
            fill.client_order_id == plan.client_order_id
                || entry
                    .record
                    .order_id
                    .as_deref()
                    .is_some_and(|order_id| fill.order_id.as_deref() == Some(order_id))
        });

        match entry.record.state {
            ExecutionState::Prepared | ExecutionState::Rejected => {
                if observed.is_some() {
                    unexpected_orders += 1;
                }
                if fill_observed {
                    unexpected_fills += 1;
                }
            }
            ExecutionState::Submitting
            | ExecutionState::Acknowledged
            | ExecutionState::UnknownSubmission
            | ExecutionState::Live
            | ExecutionState::PartiallyFilled
            | ExecutionState::Filled
            | ExecutionState::Canceled => {
                if observed.is_some() {
                    matched += 1;
                } else {
                    unresolved += 1;
                }
            }
        }
    }

    let (position_attribution, position_attribution_unavailable_events) =
        reconcile_position_attribution(
            authoritative_positions,
            exchange_fills,
            &managed_clients,
            &managed_order_ids,
        )?;

    let position_attribution_total = position_attribution.len();
    let position_attribution_residual_count = position_attribution
        .iter()
        .filter(|item| item.unattributed_external_or_outside_bounded_history_residual != "0")
        .count();
    let position_attribution_truncated = position_attribution_total > MAX_POSITION_ATTRIBUTION_ROWS;
    let position_attribution = position_attribution
        .into_iter()
        .take(MAX_POSITION_ATTRIBUTION_ROWS)
        .collect();

    Ok(AccountLedgerReconciliation {
        schema: ACCOUNT_LEDGER_RECONCILIATION_SCHEMA_V1,
        managed_intents: managed_clients.len(),
        managed_exchange_orders_observed,
        managed_exchange_fills_observed,
        unattributed_external_or_exchange_system_orders: unattributed_orders,
        unattributed_external_or_exchange_system_fills: unattributed_fills,
        managed_intents_with_exchange_match: matched,
        managed_intents_unresolved_in_bounded_exchange_evidence: unresolved,
        unexpected_exchange_orders_for_non_submitted_intents: unexpected_orders,
        unexpected_exchange_fills_for_non_submitted_intents: unexpected_fills,
        identity_mismatches,
        position_attribution_total,
        position_attribution_residual_count,
        position_attribution_truncated,
        position_attribution,
        position_attribution_unavailable_events,
        consistent: unexpected_orders == 0 && unexpected_fills == 0 && identity_mismatches == 0,
        strategy_attribution: None,
        strategy_nav: None,
        exchange_evidence: None,
        financial_period: None,
        strategy_nav_watermark: None,
        strategy_nav_checkpoint_count: 0,
        strategy_risk_stop: None,
        strategy_stop_rearm_proofs: Vec::new(),
        finalized_run: None,
        archived_runs_count: 0,
        archived_run_proofs: Vec::new(),
        archived_run_proofs_truncated: false,
        unverified_runs_count: 0,
        unverified_run_proofs_truncated: false,
        unverified_run: None,
        unverified_run_proofs: Vec::new(),
        archived_run_slots_remaining: crate::MAX_ARCHIVED_STRATEGY_RUNS,
        source_checkpoint_deferred: false,
    })
}

/// Strict read-only provenance projection. BillId from the *fill* and
/// billId from account bills MUST agree; matching only instrument/tradeId
/// is insufficient proof. No inference of shared account capital/ownership.
fn attribute_strategy_exchange_evidence(
    ledger: &DurableExecutionLedger,
    orders: &[ExchangeOrderIdentity],
    fills: &[ExchangeFillIdentity],
    bills: &[ExchangeBillIdentity],
    bills_complete: bool,
    orders_complete: bool,
    fills_complete: bool,
) -> Result<StrategyExchangeAttribution, AccountLedgerReconciliationError> {
    let mut lineage_bound_intents = 0usize;
    let mut exact_orders = 0usize;
    let mut exact_fills = 0usize;
    let mut exact_trade_bills = 0usize;
    let mut duplicate_claims = 0usize;
    let mut unmatched_lineage_fills = 0usize;
    let mut missing_exact_bill_links = 0usize;
    let mut fee_bill_mismatches = 0usize;
    let mut fill_pnl_bill_mismatches = 0usize;
    let mut seen_fills = BTreeSet::new();
    let mut seen_bills = BTreeSet::new();
    let mut fees = BTreeMap::<String, Decimal>::new();
    let mut pnl = BTreeMap::<String, Decimal>::new();
    let mut funding = BTreeMap::<String, Decimal>::new();
    // No source-evidence ID can itself prove a separate capital account.
    // Cohorts are evidence partitions, never allocation or NAV authority.
    let mut cohorts = BTreeMap::<
        (String, String, Option<String>, Option<String>, String),
        AttributionCohortAccumulator,
    >::new();

    for entry in ledger.entries() {
        let Some(lineage) = entry.record.lineage.as_ref() else {
            continue;
        };
        lineage_bound_intents += 1;
        let plan = &entry.record.plan;
        let key = (
            lineage.origin_schema.clone(),
            lineage.origin_version.clone(),
            plan.risk_binding
                .as_ref()
                .map(|risk| risk.mandate.version.clone()),
            plan.risk_binding
                .as_ref()
                .map(|risk| risk.mandate.capital_base_usd.clone()),
            plan.account_uid_fingerprint.clone(),
        );
        cohorts.entry(key.clone()).or_default().intents += 1;
        let Some(order_id) = entry.record.order_id.as_deref() else {
            continue;
        };
        if !orders.iter().any(|order| {
            order.instrument_id == plan.instrument_id
                && order.client_order_id == plan.client_order_id
                && order.order_id == order_id
        }) {
            continue;
        }
        exact_orders += 1;

        // A fill matching only one of the two independently bound order IDs,
        // or having the wrong position side, must not be silently discarded.
        for fill in fills.iter().filter(|fill| {
            fill.client_order_id == plan.client_order_id
                || fill.order_id.as_deref() == Some(order_id)
        }) {
            if fill.instrument_id != plan.instrument_id
                || fill.client_order_id != plan.client_order_id
                || fill.order_id.as_deref() != Some(order_id)
                || fill.position_side != plan.position_side.as_str()
                || fill.side != plan.side.as_str()
            {
                unmatched_lineage_fills += 1;
                continue;
            }
            let fill_key = (&fill.instrument_id, &fill.trade_id);
            if !seen_fills.insert(fill_key) {
                duplicate_claims += 1;
                continue;
            }
            exact_fills += 1;

            let Some(bill_id) = fill.bill_id.as_deref() else {
                missing_exact_bill_links += 1;
                continue;
            };
            let linked = bills
                .iter()
                .filter(|bill| bill.bill_id == bill_id)
                .collect::<Vec<_>>();
            if linked.len() > 1 {
                duplicate_claims += linked.len() - 1;
            }
            let Some(bill) = linked.first() else {
                missing_exact_bill_links += 1;
                continue;
            };
            if !seen_bills.insert(&bill.bill_id) {
                duplicate_claims += 1;
                continue;
            }
            // Funding and unrelated bills are never allowed to inherit a
            // fill's strategy identity just because billId happens to match.
            if matches!(bill.sub_type.as_str(), "173" | "174")
                || bill.instrument_id != fill.instrument_id
                || bill.trade_id.as_deref() != Some(fill.trade_id.as_str())
                || bill.order_id.as_deref().is_some_and(|id| id != order_id)
                || bill
                    .client_order_id
                    .as_deref()
                    .is_some_and(|id| id != plan.client_order_id)
            {
                unmatched_lineage_fills += 1;
                continue;
            }

            let (Some(fill_fee), Some(fill_currency), Some(bill_fee)) = (
                fill.fee.as_deref(),
                fill.fee_currency.as_deref(),
                bill.fee.as_deref(),
            ) else {
                fee_bill_mismatches += 1;
                continue;
            };
            if fill_currency != bill.currency
                || fill_currency.trim().is_empty()
                || decimal("attribution.fill_fee", fill_fee)?
                    != decimal("attribution.bill_fee", bill_fee)?
            {
                fee_bill_mismatches += 1;
                continue;
            }

            let (Some(fill_pnl), Some(bill_pnl)) = (fill.fill_pnl.as_deref(), bill.pnl.as_deref())
            else {
                fill_pnl_bill_mismatches += 1;
                continue;
            };
            let fill_realized = decimal("attribution.fill_pnl", fill_pnl)?;
            let bill_realized = decimal("attribution.bill_pnl", bill_pnl)?;
            if fill_realized != bill_realized {
                fill_pnl_bill_mismatches += 1;
                continue;
            }

            // Only source-complete, independently cross-checked financial
            // events enter either total; bill fee is NOT added to fill fee.
            exact_trade_bills += 1;
            *fees.entry(fill_currency.to_owned()).or_default() +=
                decimal("attribution.verified_fee", fill_fee)?;
            *pnl.entry(bill.currency.clone()).or_default() += bill_realized;
            let cohort = cohorts.entry(key.clone()).or_default();
            cohort.exact_trade_bills += 1;
            *cohort.fees.entry(fill_currency.to_owned()).or_default() +=
                decimal("attribution.verified_fee", fill_fee)?;
            *cohort.pnl.entry(bill.currency.clone()).or_default() += bill_realized;
        }
    }

    let mut unallocated_funding_bills = 0usize;
    let mut unlinked_nonfunding_bills = 0usize;
    for bill in bills {
        if matches!(bill.sub_type.as_str(), "173" | "174") {
            unallocated_funding_bills += 1;
            if let (Some(value), false) = (bill.pnl.as_deref(), bill.currency.trim().is_empty()) {
                *funding.entry(bill.currency.clone()).or_default() +=
                    decimal("attribution.unallocated_funding", value)?;
            }
        } else if !seen_bills.contains(&bill.bill_id) {
            unlinked_nonfunding_bills += 1;
        }
    }

    let managed_intents = ledger.len();
    let mut reasons = Vec::new();
    if lineage_bound_intents < managed_intents {
        reasons.push("historical_intents_missing_immutable_lineage");
    }
    if !bills_complete {
        reasons.push("bills_history_incomplete");
    }
    if !orders_complete {
        reasons.push("orders_history_incomplete");
    }
    if !fills_complete {
        reasons.push("fills_history_incomplete");
    }
    if unmatched_lineage_fills > 0 || duplicate_claims > 0 {
        reasons.push("fill_bill_provenance_incomplete");
    }
    if missing_exact_bill_links > 0 {
        reasons.push("native_fill_bill_id_missing_or_bill_unobserved");
    }
    if fee_bill_mismatches > 0 {
        reasons.push("bill_fee_or_currency_not_reconciled");
    }
    if fill_pnl_bill_mismatches > 0 {
        reasons.push("fill_realized_pnl_not_reconciled_with_bill");
    }
    if unallocated_funding_bills > 0 {
        // OKX funding bills have no stable strategy/position-side identity.
        // Instrument and timestamp alone do not establish an owner.
        reasons.push("funding_not_allocated_to_exact_strategy_position");
    }
    if unlinked_nonfunding_bills > 0 {
        reasons.push("account_bill_activity_outside_exact_managed_fill_links");
    }
    // Origin/version alone cannot prove that a capital pool is exclusive.
    // Even a single cohort still lacks strategy capital and funding ownership.
    reasons.push("strategy_capital_cashflows_and_mark_to_market_unattested");
    let origin_cohorts_total = cohorts.len();
    if origin_cohorts_total > 1 {
        reasons.push("multiple_origin_cohorts_not_one_strategy");
    }
    let origin_cohorts = cohorts
        .into_iter()
        .take(MAX_ORIGIN_COHORT_ROWS)
        .map(
            |(
                (origin_schema, origin_version, mandate_version, declared_capital_usd, _uid),
                value,
            )| {
                AttributionOriginCohort {
                    origin_schema,
                    origin_version,
                    mandate_version,
                    declared_capital_usd,
                    lineage_bound_intents: value.intents,
                    exact_trade_bills: value.exact_trade_bills,
                    verified_fees: linked_amounts(value.fees),
                    verified_realized_pnl: linked_amounts(value.pnl),
                }
            },
        )
        .collect::<Vec<_>>();
    if origin_cohorts_total > origin_cohorts.len() {
        reasons.push("origin_cohorts_truncated");
    }
    Ok(StrategyExchangeAttribution {
        schema: STRATEGY_ATTRIBUTION_SCHEMA_V1,
        status: "INCOMPLETE",
        strategy_nav_available: false,
        managed_intents,
        lineage_bound_intents,
        exact_orders,
        exact_fills,
        exact_trade_bills,
        duplicate_fill_or_bill_claims: duplicate_claims,
        unmatched_lineage_fills,
        missing_exact_bill_links,
        fee_bill_mismatches,
        fill_pnl_bill_mismatches,
        unallocated_funding_bills,
        unallocated_funding_totals: linked_amounts(funding),
        unlinked_nonfunding_bills,
        orders_history_complete: orders_complete,
        fills_history_complete: fills_complete,
        origin_cohorts_total,
        origin_cohorts_truncated: origin_cohorts_total > origin_cohorts.len(),
        origin_cohorts,
        fills_fee_totals: linked_amounts(fees),
        linked_trade_bill_pnl_totals: linked_amounts(pnl),
        incomplete_reasons: reasons,
    })
}

/// Changes in run-owned lots must be causally possible. A CLOSE or REDUCE
/// cannot establish ownership by itself; it may only consume a previously
/// observed run-owned OPEN/ADD/HEDGE fill in the same instrument/Hedge side.
/// Neither order timestamp proximity nor shared account equity creates ownership.
fn apply_run_lot_fill(
    outstanding: &mut BTreeMap<(String, String), Decimal>,
    instrument: &str,
    position_side: &str,
    side: &str,
    action: ExecutionAction,
    quantity: Decimal,
) -> Result<(), &'static str> {
    if quantity <= Decimal::ZERO {
        return Err("nav_invalid_size");
    }
    let increasing = match (position_side, side) {
        ("long", "buy") | ("short", "sell") => true,
        ("long", "sell") | ("short", "buy") => false,
        _ => return Err("nav_invalid_position_side"),
    };
    if increasing != action.is_risk_increasing() {
        return Err("nav_fill_action_side_mismatch");
    }
    let position = outstanding
        .entry((instrument.to_owned(), position_side.to_owned()))
        .or_default();
    if !increasing && *position < quantity {
        return Err("nav_exit_exceeds_run_owned_lot");
    }
    if increasing {
        *position += quantity;
    } else {
        *position -= quantity;
    }
    Ok(())
}

/// New-period, closed-position proof only. Any outside account event or
/// incomplete venue history invalidates the NAV; open mark-to-market is NOT
/// inferred from shared account equity.
fn assess_flat_strategy_nav(
    capital: &StrategyCapitalReservation,
    ledger: &DurableExecutionLedger,
    facts: &AccountLedgerFacts,
) -> Result<StrategyNavAssessment, AccountLedgerReconciliationError> {
    let mut reasons = Vec::new();
    let mut pnl = Decimal::ZERO;
    let mut fee = Decimal::ZERO;
    let mut exact = BTreeSet::new();
    let mut seen_trades = BTreeSet::new();
    let mut size = BTreeMap::<(String, String), Decimal>::new();
    if facts.summary.authority.account_uid_fingerprint != capital.account_uid_fingerprint {
        reasons.push("nav_account_uid_changed");
    }
    if !facts.bills_history_complete
        || !facts.exchange_orders_history_complete
        || !facts.exchange_fills_history_complete
    {
        reasons.push("nav_bounded_history_incomplete");
    }
    let now = facts
        .summary
        .current_account_as_of_ms
        .as_deref()
        .and_then(|v| v.parse::<u64>().ok());
    // Canceled/unfilled order history is documented for only two hours.
    if !now.is_some_and(|t| t >= capital.started_at_ms && t - capital.started_at_ms <= 7_200_000) {
        reasons.push("nav_epoch_outside_order_history_window");
    }
    // Keep strategy NAV on the same independently attested Futures-mode
    // currency boundary as BEGIN, first-risk admission and FINALIZED.
    // Native USDT equity is still mandatory; foreign balances never enter NAV.
    if !facts.usdt_margin_excludes_other_currencies() || facts.trading_usdt_equity.is_none() {
        reasons.push("nav_native_usdt_equity_unavailable");
    }
    if facts.summary.pending_orders > 0
        || facts
            .authoritative_positions
            .iter()
            .any(|p| !Decimal::from_str(&p.position).is_ok_and(|q| q == Decimal::ZERO))
    {
        reasons.push("nav_open_position_or_pending_order");
    }
    let entries = ledger
        .entries()
        .filter(|e| e.created_at_ms > capital.started_at_ms)
        .collect::<Vec<_>>();
    if entries.iter().any(|e| {
        !e.record.state.is_terminal()
            || e.record.plan.account_uid_fingerprint != capital.account_uid_fingerprint
            || (e.record.plan.action.is_risk_increasing()
                && (e.record.strategy_run_id.as_deref() != Some(capital.allocation_id.as_str())
                    || e.record.lineage.as_ref().is_none_or(|l| {
                        l.origin_schema != capital.origin_schema
                            || l.origin_version != capital.origin_version
                    })
                    || e.record
                        .plan
                        .risk_binding
                        .as_ref()
                        .is_none_or(|b| b.mandate.version != capital.mandate_version)))
            || (e.record.plan.action.is_risk_reducing() && e.record.strategy_run_id.is_some())
    }) {
        reasons.push("nav_unowned_or_pending_intent");
    }
    for order in facts.exchange_orders.iter().filter(|o| {
        o.update_time_ms
            .parse::<u64>()
            .is_ok_and(|t| t >= capital.started_at_ms)
    }) {
        if !entries.iter().any(|e| {
            e.record.order_id.as_deref() == Some(&order.order_id)
                && e.record.plan.client_order_id == order.client_order_id
                && e.record.plan.instrument_id == order.instrument_id
        }) {
            reasons.push("nav_unowned_exchange_order");
        }
    }
    let mut ordered_fills = facts
        .exchange_fills
        .iter()
        .filter(|f| f.fill_time_ms >= capital.started_at_ms)
        .collect::<Vec<_>>();
    ordered_fills.sort_by(|a, b| {
        a.fill_time_ms
            .cmp(&b.fill_time_ms)
            .then_with(|| a.instrument_id.cmp(&b.instrument_id))
            .then_with(|| a.trade_id.cmp(&b.trade_id))
    });
    // OKX millisecond timestamps alone do not establish the ordering of
    // opposite executions at the same instant. Do not invent a close-after-open
    // sequence merely from lexicographic trade IDs.
    for pair in ordered_fills.windows(2) {
        if pair[0].fill_time_ms == pair[1].fill_time_ms
            && pair[0].instrument_id == pair[1].instrument_id
            && pair[0].position_side == pair[1].position_side
            && pair[0].side != pair[1].side
        {
            reasons.push("nav_fill_sequence_ambiguous");
        }
    }
    for fill in ordered_fills {
        let owners = entries
            .iter()
            .filter(|e| {
                e.record.order_id.as_deref() == fill.order_id.as_deref()
                    && e.record.order_id.is_some()
                    && e.record.plan.client_order_id == fill.client_order_id
                    && e.record.plan.instrument_id == fill.instrument_id
                    && e.record.plan.side.as_str() == fill.side
                    && e.record.plan.position_side.as_str() == fill.position_side
                    && facts.exchange_orders.iter().any(|o| {
                        Some(o.order_id.as_str()) == fill.order_id.as_deref()
                            && o.client_order_id == fill.client_order_id
                            && o.instrument_id == fill.instrument_id
                    })
            })
            .collect::<Vec<_>>();
        if owners.len() != 1 || !seen_trades.insert((&fill.instrument_id, &fill.trade_id)) {
            reasons.push("nav_unowned_or_duplicate_fill");
            continue;
        }
        let owner = owners[0];
        if owner.record.plan.action.is_risk_increasing()
            && owner.record.strategy_run_id.as_deref() != Some(capital.allocation_id.as_str())
        {
            reasons.push("nav_risk_fill_not_owned_by_run");
            continue;
        }
        let qty = decimal("nav.fill_size", &fill.fill_size)?;
        if let Err(reason) = apply_run_lot_fill(
            &mut size,
            &fill.instrument_id,
            &fill.position_side,
            &fill.side,
            owner.record.plan.action,
            qty,
        ) {
            reasons.push(reason);
            continue;
        }
        let bills = facts
            .exchange_bills
            .iter()
            .filter(|b| {
                fill.bill_id.as_deref() == Some(b.bill_id.as_str())
                    && b.instrument_id == fill.instrument_id
                    && b.trade_id.as_deref() == Some(fill.trade_id.as_str())
                    && b.order_id
                        .as_deref()
                        .is_none_or(|id| Some(id) == fill.order_id.as_deref())
                    && b.client_order_id
                        .as_deref()
                        .is_none_or(|id| id == fill.client_order_id)
            })
            .collect::<Vec<_>>();
        if bills.len() != 1
            || bills[0].timestamp_ms < capital.started_at_ms
            || !exact.insert(&bills[0].bill_id)
        {
            reasons.push("nav_missing_or_duplicate_native_bill");
            continue;
        }
        let bill = bills[0];
        if bill.currency != "USDT"
            || fill.fee_currency.as_deref() != Some("USDT")
            || matches!(bill.sub_type.as_str(), "173" | "174")
            || fill.fee.is_none()
            || bill.fee.is_none()
            || fill.fill_pnl.is_none()
            || bill.pnl.is_none()
        {
            reasons.push("nav_incomplete_or_non_usdt_bill");
            continue;
        }
        let a = decimal("nav.fill_fee", fill.fee.as_deref().unwrap_or_default())?;
        let b = decimal("nav.bill_fee", bill.fee.as_deref().unwrap_or_default())?;
        let c = decimal("nav.fill_pnl", fill.fill_pnl.as_deref().unwrap_or_default())?;
        let d = decimal("nav.bill_pnl", bill.pnl.as_deref().unwrap_or_default())?;
        if a != b || c != d {
            reasons.push("nav_bill_financial_mismatch");
            continue;
        }
        fee += a;
        pnl += c;
    }
    if size.values().any(|v| *v != Decimal::ZERO) {
        reasons.push("nav_fill_position_not_flat");
    }
    // Includes cash transfers and funding: none may inherit a strategy
    // owner merely from the instrument or event timestamp.
    if facts
        .exchange_bills
        .iter()
        .any(|b| b.timestamp_ms >= capital.started_at_ms && !exact.contains(&b.bill_id))
    {
        reasons.push("nav_unowned_account_bill_or_funding");
    }
    let allocated = decimal("nav.allocated_usdt", &capital.allocated_usdt)?;
    let nav = allocated + pnl + fee;
    if let Some(current) = facts.trading_usdt_equity.as_deref()
        && decimal("nav.current_usdt_equity", current)?
            - decimal(
                "nav.opening_usdt_equity",
                &capital.opening_account_usdt_equity,
            )?
            != pnl + fee
    {
        reasons.push("nav_shared_account_equity_residual");
    }
    reasons.sort_unstable();
    reasons.dedup();
    let complete = reasons.is_empty();
    Ok(StrategyNavAssessment {
        schema: STRATEGY_NAV_ASSESSMENT_SCHEMA_V1,
        status: if complete {
            "FLAT_PERIOD_RECONCILED"
        } else {
            "INCOMPLETE"
        },
        usable_for_risk: false,
        allocation_id: capital.allocation_id.clone(),
        allocated_usdt: allocated.normalize().to_string(),
        exact_trade_bills: exact.len(),
        verified_realized_usdt: pnl.normalize().to_string(),
        verified_fees_usdt: fee.normalize().to_string(),
        nav_usdt: complete.then(|| nav.normalize().to_string()),
        reasons,
    })
}

fn linked_amounts(amounts: BTreeMap<String, Decimal>) -> Vec<LinkedCurrencyAmount> {
    amounts
        .into_iter()
        .map(|(currency, amount)| LinkedCurrencyAmount {
            currency,
            amount: amount.normalize().to_string(),
        })
        .collect()
}

fn reconcile_position_attribution(
    authoritative_positions: &[AccountPositionState],
    exchange_fills: &[ExchangeFillIdentity],
    managed_clients: &BTreeSet<&str>,
    managed_order_ids: &BTreeSet<&str>,
) -> Result<(Vec<PositionAttributionDiagnostic>, usize), AccountLedgerReconciliationError> {
    let mut authoritative = BTreeMap::<(String, String), Decimal>::new();
    let mut managed = BTreeMap::<(String, String), Decimal>::new();
    let mut unavailable = 0usize;

    for position in authoritative_positions {
        if !matches!(position.position_side.as_str(), "long" | "short") {
            unavailable += 1;
            continue;
        }
        let value = decimal("account.position", &position.position)?;
        authoritative.insert(
            (
                position.instrument_id.clone(),
                position.position_side.clone(),
            ),
            value,
        );
    }

    for fill in exchange_fills {
        let is_managed = managed_clients.contains(fill.client_order_id.as_str())
            || fill
                .order_id
                .as_deref()
                .is_some_and(|order_id| managed_order_ids.contains(order_id));
        if !is_managed {
            continue;
        }
        if !matches!(fill.position_side.as_str(), "long" | "short") {
            unavailable += 1;
            continue;
        }
        let size = decimal("fill.fillSz", &fill.fill_size)?;
        if size <= Decimal::ZERO {
            return Err(AccountLedgerReconciliationError::InvalidDecimal {
                field: "fill.fillSz",
                value: fill.fill_size.clone(),
            });
        }
        let signed = match (fill.position_side.as_str(), fill.side.as_str()) {
            ("long", "buy") | ("short", "sell") => size,
            ("long", "sell") | ("short", "buy") => -size,
            _ => {
                unavailable += 1;
                continue;
            }
        };
        *managed
            .entry((fill.instrument_id.clone(), fill.position_side.clone()))
            .or_default() += signed;
    }

    let keys = authoritative
        .keys()
        .chain(managed.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let diagnostics = keys
        .into_iter()
        .map(|(instrument_id, position_side)| {
            let exchange = authoritative
                .get(&(instrument_id.clone(), position_side.clone()))
                .copied()
                .unwrap_or(Decimal::ZERO);
            let managed_delta = managed
                .get(&(instrument_id.clone(), position_side.clone()))
                .copied()
                .unwrap_or(Decimal::ZERO);
            PositionAttributionDiagnostic {
                instrument_id,
                position_side,
                authoritative_exchange_position: exchange.normalize().to_string(),
                managed_fill_delta_in_bounded_history: managed_delta.normalize().to_string(),
                unattributed_external_or_outside_bounded_history_residual: (exchange
                    - managed_delta)
                    .normalize()
                    .to_string(),
            }
        })
        .collect();

    Ok((diagnostics, unavailable))
}

fn decimal(field: &'static str, value: &str) -> Result<Decimal, AccountLedgerReconciliationError> {
    Decimal::from_str(value.trim()).map_err(|_| AccountLedgerReconciliationError::InvalidDecimal {
        field,
        value: value.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use okx_observation::{ExchangeFillIdentity, ExchangeOrderIdentity};

    use super::*;
    use crate::{
        ExecutionAction, ExecutionLedgerStore, ExecutionPlan, OrderSide, OrderType, PositionSide,
        PrepareDisposition, TradeMode, derive_client_order_id,
    };

    fn test_lineage() -> crate::ExecutionLineageBinding {
        crate::ExecutionLineageBinding {
            schema: crate::EXECUTION_LINEAGE_SCHEMA_V1.to_owned(),
            origin_evidence_id: format!("sha256:{}", "a".repeat(64)),
            origin_schema: "okx.strategy-decision/v1".to_owned(),
            origin_version: "frozen-strategy-1".to_owned(),
            authority_evidence_id: None,
            decision_reference: crate::ExecutionDecisionReference {
                decision_time_ms: 100,
                price: "0.1".to_owned(),
                price_basis: crate::TcaReferencePriceBasis::DecisionPrice,
                price_policy_version: "v1".to_owned(),
            },
            tca_mechanics: None,
        }
    }

    #[test]
    fn exact_trade_and_bill_lineage_counts_fee_once_and_refuses_unallocated_funding_nav() {
        let p = path("strategy-attribution-exact");
        let _ = std::fs::remove_file(&p);
        let mut ledger =
            DurableExecutionLedger::open(ExecutionLedgerStore::at(&p), 100).expect("ledger");
        let plan = plan("intent_attribution_exact_20261012a");
        ledger
            .prepare_with_lineage(plan.clone(), Some(test_lineage()), 101)
            .expect("lineage bound");
        ledger
            .begin_submission(&plan.intent_id, 102)
            .expect("first POST reservation");
        ledger
            .acknowledge(&plan.intent_id, "ord-exact", 103)
            .expect("venue ACK");
        let order = ExchangeOrderIdentity {
            instrument_type: "SWAP".to_owned(),
            instrument_id: plan.instrument_id.clone(),
            order_id: "ord-exact".to_owned(),
            client_order_id: plan.client_order_id.clone(),
            state: "filled".to_owned(),
            size: "1".to_owned(),
            accumulated_fill_size: "0".to_owned(),
            update_time_ms: "1791828000000".to_owned(),
        };
        let fill = ExchangeFillIdentity {
            instrument_type: "SWAP".to_owned(),
            instrument_id: plan.instrument_id.clone(),
            order_id: Some("ord-exact".to_owned()),
            client_order_id: plan.client_order_id.clone(),
            trade_id: "trade-exact".to_owned(),
            bill_id: Some("bill-exact".to_owned()),
            fill_pnl: Some("0.13".to_owned()),
            side: "buy".to_owned(),
            position_side: "long".to_owned(),
            fill_price: "0.1".to_owned(),
            fill_size: "1".to_owned(),
            fee: Some("-0.02".to_owned()),
            fee_currency: Some("USDT".to_owned()),
            execution_type: Some("T".to_owned()),
            fill_time_ms: 1791828000000,
        };
        let bill = ExchangeBillIdentity {
            bill_id: "bill-exact".to_owned(),
            instrument_id: plan.instrument_id.clone(),
            order_id: Some("ord-exact".to_owned()),
            trade_id: Some("trade-exact".to_owned()),
            client_order_id: Some(plan.client_order_id.clone()),
            currency: "USDT".to_owned(),
            sub_type: "1".to_owned(),
            pnl: Some("0.13".to_owned()),
            fee: Some("-0.02".to_owned()),
            timestamp_ms: 1791828000000,
        };
        let funding = ExchangeBillIdentity {
            bill_id: "bill-funding".to_owned(),
            instrument_id: plan.instrument_id.clone(),
            order_id: None,
            trade_id: None,
            client_order_id: None,
            currency: "USDT".to_owned(),
            sub_type: "173".to_owned(),
            pnl: Some("-0.01".to_owned()),
            fee: None,
            timestamp_ms: 1791828000100,
        };
        let report = attribute_strategy_exchange_evidence(
            &ledger,
            &[order],
            &[fill],
            &[bill.clone(), bill, funding],
            true,
            true,
            true,
        )
        .expect("strict read-only attribution");
        assert_eq!(report.lineage_bound_intents, 1);
        assert_eq!(report.exact_orders, 1);
        assert_eq!(report.exact_fills, 1);
        assert_eq!(
            report.exact_trade_bills, 1,
            "same billId must never count twice"
        );
        assert_eq!(report.duplicate_fill_or_bill_claims, 1);
        assert_eq!(report.unallocated_funding_bills, 1);
        assert_eq!(report.unallocated_funding_totals[0].amount, "-0.01");
        assert_eq!(
            report.fills_fee_totals[0].amount, "-0.02",
            "bill fee is not added again"
        );
        assert_eq!(report.linked_trade_bill_pnl_totals[0].amount, "0.13");
        assert_eq!(report.status, "INCOMPLETE");
        assert!(!report.strategy_nav_available);
        assert!(
            report
                .incomplete_reasons
                .contains(&"strategy_capital_cashflows_and_mark_to_market_unattested")
        );
        let _ = std::fs::remove_file(p);
    }

    fn linked_accounting_pair(
        plan: &ExecutionPlan,
        trade_id: &str,
        bill_id: &str,
        fee: &str,
        currency: &str,
        realized_pnl: &str,
    ) -> (ExchangeFillIdentity, ExchangeBillIdentity) {
        let fill = ExchangeFillIdentity {
            instrument_type: "SWAP".to_owned(),
            instrument_id: plan.instrument_id.clone(),
            order_id: Some("ord-exact".to_owned()),
            client_order_id: plan.client_order_id.clone(),
            trade_id: trade_id.to_owned(),
            bill_id: Some(bill_id.to_owned()),
            fill_pnl: Some(realized_pnl.to_owned()),
            side: "buy".to_owned(),
            position_side: "long".to_owned(),
            fill_price: "0.1".to_owned(),
            fill_size: "0.5".to_owned(),
            fee: Some(fee.to_owned()),
            fee_currency: Some(currency.to_owned()),
            execution_type: Some("M".to_owned()),
            fill_time_ms: 1791828000000,
        };
        let bill = ExchangeBillIdentity {
            bill_id: bill_id.to_owned(),
            instrument_id: plan.instrument_id.clone(),
            order_id: Some("ord-exact".to_owned()),
            trade_id: Some(trade_id.to_owned()),
            client_order_id: Some(plan.client_order_id.clone()),
            currency: currency.to_owned(),
            sub_type: "3".to_owned(),
            pnl: Some(realized_pnl.to_owned()),
            fee: Some(fee.to_owned()),
            timestamp_ms: 1791828000000,
        };
        (fill, bill)
    }

    fn acknowledged_test_ledger(name: &str) -> (DurableExecutionLedger, ExecutionPlan, PathBuf) {
        let p = path(name);
        let _ = std::fs::remove_file(&p);
        let mut ledger =
            DurableExecutionLedger::open(ExecutionLedgerStore::at(&p), 100).expect("ledger");
        let plan = plan("intent_attribution_corroboration_20261012a");
        ledger
            .prepare_with_lineage(plan.clone(), Some(test_lineage()), 101)
            .expect("bind strategy evidence");
        ledger
            .begin_submission(&plan.intent_id, 102)
            .expect("reserve venue send");
        ledger
            .acknowledge(&plan.intent_id, "ord-exact", 103)
            .expect("exact venue order ACK");
        (ledger, plan, p)
    }

    #[test]
    fn mismatched_native_bill_id_fee_currency_and_pnl_never_claim_reconciled_costs() {
        let (ledger, plan, p) = acknowledged_test_ledger("native-bill-mismatches");
        let order = ExchangeOrderIdentity {
            instrument_type: "SWAP".to_owned(),
            instrument_id: plan.instrument_id.clone(),
            order_id: "ord-exact".to_owned(),
            client_order_id: plan.client_order_id.clone(),
            state: "filled".to_owned(),
            size: "1".to_owned(),
            accumulated_fill_size: "0".to_owned(),
            update_time_ms: "1791828000000".to_owned(),
        };
        let (fill, bill) =
            linked_accounting_pair(&plan, "trade-1", "bill-1", "-0.02", "USDT", "0.13");
        let analyze = |fill: ExchangeFillIdentity, bill: ExchangeBillIdentity| {
            attribute_strategy_exchange_evidence(
                &ledger,
                std::slice::from_ref(&order),
                &[fill],
                &[bill],
                true,
                true,
                true,
            )
            .expect("typed read-only bill validation")
        };
        let ok = analyze(fill.clone(), bill.clone());
        assert_eq!(ok.exact_trade_bills, 1);
        assert_eq!(ok.fills_fee_totals[0].amount, "-0.02");
        assert_eq!(ok.linked_trade_bill_pnl_totals[0].amount, "0.13");
        assert_eq!(ok.fee_bill_mismatches, 0);
        assert_eq!(ok.fill_pnl_bill_mismatches, 0);

        let mut altered_fee = bill.clone();
        altered_fee.fee = Some("-0.03".to_owned());
        let bad = analyze(fill.clone(), altered_fee);
        assert_eq!(bad.exact_trade_bills, 0);
        assert_eq!(bad.fee_bill_mismatches, 1);
        assert!(bad.fills_fee_totals.is_empty());
        assert!(
            bad.incomplete_reasons
                .contains(&"bill_fee_or_currency_not_reconciled")
        );

        let mut altered_currency = bill.clone();
        altered_currency.currency = "USDC".to_owned();
        assert_eq!(
            analyze(fill.clone(), altered_currency).fee_bill_mismatches,
            1
        );

        let mut altered_pnl = bill.clone();
        altered_pnl.pnl = Some("0.14".to_owned());
        let bad = analyze(fill.clone(), altered_pnl);
        assert_eq!(bad.exact_trade_bills, 0);
        assert_eq!(bad.fill_pnl_bill_mismatches, 1);
        assert!(bad.linked_trade_bill_pnl_totals.is_empty());

        let mut wrong_bill_id = fill.clone();
        wrong_bill_id.bill_id = Some("another-bill".to_owned());
        let bad = analyze(wrong_bill_id, bill.clone());
        assert_eq!(bad.missing_exact_bill_links, 1);
        assert_eq!(bad.exact_trade_bills, 0);

        let mut no_bill_id = fill.clone();
        no_bill_id.bill_id = None;
        assert_eq!(
            analyze(no_bill_id, bill.clone()).missing_exact_bill_links,
            1
        );

        let mut wrong_order = bill.clone();
        wrong_order.order_id = Some("external-order".to_owned());
        let bad = analyze(fill.clone(), wrong_order);
        assert_eq!(bad.unmatched_lineage_fills, 1);
        assert_eq!(bad.exact_trade_bills, 0);
        assert!(bad.fills_fee_totals.is_empty());

        let mut wrong_trade = bill.clone();
        wrong_trade.trade_id = Some("unrelated-fill".to_owned());
        assert_eq!(
            analyze(fill.clone(), wrong_trade).unmatched_lineage_fills,
            1
        );

        let missing_history = attribute_strategy_exchange_evidence(
            &ledger,
            &[order],
            &[fill],
            &[bill],
            false,
            false,
            false,
        )
        .expect("missing history is an incomplete witness");
        assert!(
            missing_history
                .incomplete_reasons
                .contains(&"bills_history_incomplete")
        );
        assert_eq!(missing_history.status, "INCOMPLETE");
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn partial_fills_non_settle_currency_rebate_and_external_bill_do_not_mix() {
        let (ledger, plan, p) = acknowledged_test_ledger("native-partial-fees");
        let order = ExchangeOrderIdentity {
            instrument_type: "SWAP".to_owned(),
            instrument_id: plan.instrument_id.clone(),
            order_id: "ord-exact".to_owned(),
            client_order_id: plan.client_order_id.clone(),
            state: "filled".to_owned(),
            size: "1".to_owned(),
            accumulated_fill_size: "0".to_owned(),
            update_time_ms: "1791828000000".to_owned(),
        };
        let (a, ab) = linked_accounting_pair(&plan, "trade-a", "bill-a", "-0.01", "USDT", "0.13");
        let (b, bb) = linked_accounting_pair(&plan, "trade-b", "bill-b", "0.004", "USDC", "-0.03");
        let mut external = bb.clone();
        external.bill_id = "manual-unmanaged-bill".to_owned();
        external.trade_id = Some("external-trade".to_owned());
        let r = attribute_strategy_exchange_evidence(
            &ledger,
            &[order],
            &[a, b],
            &[ab, bb, external],
            true,
            true,
            true,
        )
        .expect("partial accounting");
        assert_eq!(r.exact_fills, 2);
        assert_eq!(r.exact_trade_bills, 2);
        assert_eq!(r.unlinked_nonfunding_bills, 1);
        assert_eq!(r.fills_fee_totals.len(), 2);
        assert_eq!(r.fills_fee_totals[0].currency, "USDC");
        assert_eq!(r.fills_fee_totals[0].amount, "0.004");
        assert_eq!(r.fills_fee_totals[1].amount, "-0.01");
        assert_eq!(r.linked_trade_bill_pnl_totals[0].amount, "-0.03");
        assert_eq!(r.linked_trade_bill_pnl_totals[1].amount, "0.13");
        assert!(
            r.incomplete_reasons
                .contains(&"account_bill_activity_outside_exact_managed_fill_links")
        );
        assert!(!r.strategy_nav_available);
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn different_immutable_origin_versions_and_incomplete_venue_history_cannot_form_one_nav() {
        let p = path("distinct-provenance-cohorts");
        let _ = std::fs::remove_file(&p);
        let mut ledger =
            DurableExecutionLedger::open(ExecutionLedgerStore::at(&p), 100).expect("ledger");
        let first = plan("intent_origin_family_one_20261012");
        let mut second = plan("intent_origin_family_two_20261012");
        // Two independently owned instruments; the test must not bypass the
        // existing one-active-intent-per-instrument executor invariant.
        second.instrument_id = "BTC-USDT-SWAP".to_owned();
        let origin1 = test_lineage();
        let mut origin2 = test_lineage();
        origin2.origin_version = "independent-origin-v2".to_owned();

        for (plan, lineage, order_id) in [
            (&first, origin1, "ord-exact"),
            (&second, origin2, "ord-other"),
        ] {
            ledger
                .prepare_with_lineage(plan.clone(), Some(lineage), 101)
                .expect("immutably bound intent");
            ledger
                .begin_submission(&plan.intent_id, 102)
                .expect("durable send reservation");
            ledger
                .acknowledge(&plan.intent_id, order_id, 103)
                .expect("exact exchange ACK");
        }

        let (fill1, bill1) = linked_accounting_pair(
            &first,
            "trade-cohort-one",
            "bill-one",
            "-0.02",
            "USDT",
            "0.11",
        );
        let (mut fill2, mut bill2) = linked_accounting_pair(
            &second,
            "trade-cohort-two",
            "bill-two",
            "-0.03",
            "USDT",
            "0.22",
        );
        fill2.order_id = Some("ord-other".to_owned());
        bill2.order_id = Some("ord-other".to_owned());
        let orders = [
            ExchangeOrderIdentity {
                instrument_type: "SWAP".to_owned(),
                instrument_id: first.instrument_id.clone(),
                order_id: "ord-exact".to_owned(),
                client_order_id: first.client_order_id.clone(),
                state: "filled".to_owned(),
                size: "1".to_owned(),
                accumulated_fill_size: "0".to_owned(),
                update_time_ms: "1791828000000".to_owned(),
            },
            ExchangeOrderIdentity {
                instrument_type: "SWAP".to_owned(),
                instrument_id: second.instrument_id.clone(),
                order_id: "ord-other".to_owned(),
                client_order_id: second.client_order_id.clone(),
                state: "filled".to_owned(),
                size: "1".to_owned(),
                accumulated_fill_size: "0".to_owned(),
                update_time_ms: "1791828000000".to_owned(),
            },
        ];
        let report = attribute_strategy_exchange_evidence(
            &ledger,
            &orders,
            &[fill1, fill2],
            &[bill1, bill2],
            true,
            false,
            false,
        )
        .expect("account source partition");
        assert_eq!(report.exact_trade_bills, 2);
        assert_eq!(report.origin_cohorts_total, 2);
        assert!(!report.origin_cohorts_truncated);
        let independent = report
            .origin_cohorts
            .iter()
            .find(|cohort| cohort.origin_version == "independent-origin-v2")
            .expect("second origin");
        let original = report
            .origin_cohorts
            .iter()
            .find(|cohort| cohort.origin_version == "frozen-strategy-1")
            .expect("first origin");
        assert_eq!(original.verified_realized_pnl[0].amount, "0.11");
        assert_eq!(independent.verified_realized_pnl[0].amount, "0.22");
        assert_eq!(original.verified_fees[0].amount, "-0.02");
        assert_eq!(independent.verified_fees[0].amount, "-0.03");
        assert_eq!(report.linked_trade_bill_pnl_totals[0].amount, "0.33");
        assert!(!report.orders_history_complete);
        assert!(!report.fills_history_complete);
        assert!(
            report
                .incomplete_reasons
                .contains(&"orders_history_incomplete")
        );
        assert!(
            report
                .incomplete_reasons
                .contains(&"fills_history_incomplete")
        );
        assert!(
            report
                .incomplete_reasons
                .contains(&"multiple_origin_cohorts_not_one_strategy")
        );
        assert_eq!(report.status, "INCOMPLETE");
        assert!(!report.strategy_nav_available);
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn run_lot_causality_refuses_unowned_exit_and_double_reduction() {
        let mut size = BTreeMap::new();
        let d = |v: &str| Decimal::from_str(v).expect("test decimal");
        assert_eq!(
            apply_run_lot_fill(
                &mut size,
                "BTC-USDT-SWAP",
                "long",
                "sell",
                ExecutionAction::Close,
                d("1")
            ),
            Err("nav_exit_exceeds_run_owned_lot")
        );
        assert!(
            size.get(&("BTC-USDT-SWAP".to_owned(), "long".to_owned()))
                .is_some_and(|v| v.is_zero())
        );
        assert_eq!(
            apply_run_lot_fill(
                &mut size,
                "BTC-USDT-SWAP",
                "long",
                "buy",
                ExecutionAction::Open,
                d("2")
            ),
            Ok(())
        );
        assert_eq!(
            apply_run_lot_fill(
                &mut size,
                "BTC-USDT-SWAP",
                "long",
                "sell",
                ExecutionAction::Reduce,
                d("1.25")
            ),
            Ok(())
        );
        assert_eq!(
            apply_run_lot_fill(
                &mut size,
                "BTC-USDT-SWAP",
                "long",
                "sell",
                ExecutionAction::Close,
                d("1")
            ),
            Err("nav_exit_exceeds_run_owned_lot")
        );
        assert_eq!(
            size.get(&("BTC-USDT-SWAP".to_owned(), "long".to_owned())),
            Some(&d("0.75"))
        );
        assert_eq!(
            apply_run_lot_fill(
                &mut size,
                "BTC-USDT-SWAP",
                "long",
                "sell",
                ExecutionAction::Close,
                d("0.75")
            ),
            Ok(())
        );
        assert_eq!(
            size.get(&("BTC-USDT-SWAP".to_owned(), "long".to_owned())),
            Some(&Decimal::ZERO)
        );
        assert_eq!(
            apply_run_lot_fill(
                &mut size,
                "BTC-USDT-SWAP",
                "short",
                "buy",
                ExecutionAction::Close,
                d("1")
            ),
            Err("nav_exit_exceeds_run_owned_lot")
        );
    }

    #[test]
    fn run_lot_causality_refuses_hedge_side_mismatch_and_invalid_size() {
        let mut size = BTreeMap::new();
        let d = |v: &str| Decimal::from_str(v).expect("test decimal");
        assert_eq!(
            apply_run_lot_fill(
                &mut size,
                "BTC-USDT-SWAP",
                "short",
                "buy",
                ExecutionAction::Hedge,
                d("1")
            ),
            Err("nav_fill_action_side_mismatch")
        );
        assert_eq!(
            apply_run_lot_fill(
                &mut size,
                "BTC-USDT-SWAP",
                "short",
                "sell",
                ExecutionAction::Hedge,
                d("1")
            ),
            Ok(())
        );
        assert_eq!(
            apply_run_lot_fill(
                &mut size,
                "BTC-USDT-SWAP",
                "short",
                "buy",
                ExecutionAction::Close,
                d("0")
            ),
            Err("nav_invalid_size")
        );
        assert_eq!(
            apply_run_lot_fill(
                &mut size,
                "BTC-USDT-SWAP",
                "short",
                "buy",
                ExecutionAction::Close,
                d("1")
            ),
            Ok(())
        );
        assert_eq!(
            apply_run_lot_fill(
                &mut size,
                "BTC-USDT-SWAP",
                "short",
                "buy",
                ExecutionAction::Open,
                d("1")
            ),
            Err("nav_fill_action_side_mismatch")
        );
        assert_eq!(
            apply_run_lot_fill(
                &mut size,
                "BTC-USDT-SWAP",
                "net",
                "buy",
                ExecutionAction::Open,
                d("1")
            ),
            Err("nav_invalid_position_side")
        );
    }

    #[test]
    fn old_fills_without_original_lineage_are_not_retroactively_attributed() {
        let p = path("strategy-attribution-legacy");
        let _ = std::fs::remove_file(&p);
        let mut ledger =
            DurableExecutionLedger::open(ExecutionLedgerStore::at(&p), 100).expect("ledger");
        let plan = plan("intent_legacy_no_lineage_20261012");
        ledger.prepare(plan.clone(), 101).expect("prepared");
        let fill = ExchangeFillIdentity {
            instrument_type: "SWAP".to_owned(),
            instrument_id: plan.instrument_id,
            order_id: Some("old-order".to_owned()),
            client_order_id: plan.client_order_id,
            trade_id: "old-trade".to_owned(),
            bill_id: None,
            fill_pnl: None,
            side: "buy".to_owned(),
            position_side: "long".to_owned(),
            fill_price: "0.1".to_owned(),
            fill_size: "1".to_owned(),
            fee: Some("-0.5".to_owned()),
            fee_currency: Some("USDT".to_owned()),
            execution_type: Some("M".to_owned()),
            fill_time_ms: 1791828000000,
        };
        let report =
            attribute_strategy_exchange_evidence(&ledger, &[], &[fill], &[], false, false, false)
                .expect("legacy evidence remains incomplete");
        assert_eq!(report.lineage_bound_intents, 0);
        assert_eq!(report.exact_fills, 0);
        assert!(report.fills_fee_totals.is_empty());
        assert!(
            report
                .incomplete_reasons
                .contains(&"historical_intents_missing_immutable_lineage")
        );
        assert!(
            report
                .incomplete_reasons
                .contains(&"bills_history_incomplete")
        );
        let _ = std::fs::remove_file(p);
    }

    fn path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "okx-ledger-reconciliation-{name}-{}",
            std::process::id()
        ))
    }

    fn plan(intent_id: &str) -> ExecutionPlan {
        ExecutionPlan {
            schema: "okx.execution-plan/v1".to_owned(),
            intent_id: intent_id.to_owned(),
            client_order_id: derive_client_order_id(intent_id),
            reference_generation: "sha256:reference".to_owned(),
            account_generation: "sha256:account".to_owned(),
            account_uid_fingerprint: "uid-fingerprint".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            trade_mode: TradeMode::Cross,
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            action: ExecutionAction::Open,
            order_type: OrderType::Limit,
            size: "1".to_owned(),
            price: "0.1".to_owned(),
            open_risk: None,
            risk_binding: None,
        }
    }

    #[test]
    fn managed_intent_inventory_exposes_exact_ids_without_mutation_and_has_stable_order() {
        let p = path("identity-inventory");
        let _ = std::fs::remove_file(&p);
        let mut ledger =
            DurableExecutionLedger::open(ExecutionLedgerStore::at(&p), 100).expect("ledger");
        for (suffix, time) in [("older", 101), ("newer", 102)] {
            let mut plan = plan(&format!("intent_inventory_{suffix}_01234567"));
            plan.instrument_id = format!("TOKEN{time}-USDT-SWAP");
            ledger.prepare(plan, time).expect("prepare");
        }
        let inventory = managed_execution_inventory(&ledger);
        assert_eq!(inventory.schema, MANAGED_EXECUTION_INVENTORY_SCHEMA_V1);
        assert_eq!(inventory.total, 2);
        assert_eq!(inventory.shown, 2);
        assert!(inventory.complete_within_bound);
        assert!(inventory.rows[0].intent_id.contains("newer"));
        assert!(inventory.rows[1].intent_id.contains("older"));
        assert_eq!(inventory.rows[0].state, ExecutionState::Prepared);
        assert_eq!(inventory.rows[0].exchange_order_id, None);
        assert_eq!(inventory.rows[0].action, ExecutionAction::Open);
        assert_eq!(inventory.rows[0].account_uid_fingerprint, "uid-fingerprint");
        assert_eq!(
            inventory.rows[0].client_order_id,
            derive_client_order_id(&inventory.rows[0].intent_id)
        );
        assert_eq!(
            managed_execution_inventory(&ledger),
            inventory,
            "read-only repeat must not mutate ledger"
        );
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn managed_intent_inventory_never_claims_complete_when_bounded() {
        let p = path("identity-bounded");
        let _ = std::fs::remove_file(&p);
        let mut ledger =
            DurableExecutionLedger::open(ExecutionLedgerStore::at(&p), 100).expect("ledger");
        for n in 0..(MAX_MANAGED_EXECUTION_INVENTORY_ROWS + 1) {
            let mut plan = plan(&format!("intent_inventory_many_{n:04}_01234567"));
            plan.instrument_id = format!("TOKEN{n}-USDT-SWAP");
            ledger.prepare(plan, (n + 100) as u64).expect("prepare");
        }
        let inventory = managed_execution_inventory(&ledger);
        assert_eq!(inventory.total, MAX_MANAGED_EXECUTION_INVENTORY_ROWS + 1);
        assert_eq!(inventory.shown, MAX_MANAGED_EXECUTION_INVENTORY_ROWS);
        assert!(!inventory.complete_within_bound);
        assert!(inventory.rows[0].intent_id.contains("0016"));
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn prepared_intent_cannot_have_exchange_order_without_inconsistency() {
        let p = path("prepared");
        let store = ExecutionLedgerStore::at(&p);
        let mut ledger = DurableExecutionLedger::open(store, 100).expect("ledger");
        let plan = plan("intent_reconcile_0123456789");
        assert!(matches!(
            ledger.prepare(plan.clone(), 101).expect("prepare"),
            PrepareDisposition::Created(_)
        ));

        let orders = vec![ExchangeOrderIdentity {
            instrument_type: "SWAP".to_owned(),
            instrument_id: plan.instrument_id,
            order_id: "ord-1".to_owned(),
            client_order_id: plan.client_order_id,
            state: "live".to_owned(),
            size: "1".to_owned(),
            accumulated_fill_size: "0".to_owned(),
            update_time_ms: "1790884800000".to_owned(),
        }];
        let result = reconcile_exchange_evidence(&ledger, &[], &orders, &[]).expect("reconcile");
        assert!(!result.consistent);
        assert_eq!(
            result.unexpected_exchange_orders_for_non_submitted_intents,
            1
        );
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn managed_fill_delta_leaves_unattributed_position_residual() {
        let positions = vec![AccountPositionState {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            position: "2".to_owned(),
            position_side: "long".to_owned(),
            margin_mode: "cross".to_owned(),
            average_price: Some("0.1".to_owned()),
            mark_price: Some("0.11".to_owned()),
            liquidation_price: None,
            unrealized_pnl: Some("0.02".to_owned()),
            unrealized_pnl_ratio: None,
            leverage: Some("5".to_owned()),
            margin: None,
            initial_margin_requirement: None,
            maintenance_margin_requirement: None,
            margin_ratio: None,
            notional_usd: None,
            margin_currency: Some("USDT".to_owned()),
            creation_time_ms: Some("1790884700000".to_owned()),
            update_time_ms: Some("1790884800000".to_owned()),
        }];
        let fills = vec![ExchangeFillIdentity {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            order_id: Some("ord-1".to_owned()),
            client_order_id: "managed-client".to_owned(),
            trade_id: "trade-managed-1".to_owned(),
            bill_id: None,
            fill_pnl: None,
            side: "buy".to_owned(),
            position_side: "long".to_owned(),
            fill_price: "0.1".to_owned(),
            fill_size: "1".to_owned(),
            fee: Some("-0.001".to_owned()),
            fee_currency: Some("USDT".to_owned()),
            execution_type: Some("T".to_owned()),
            fill_time_ms: 1_790_884_800_000,
        }];
        let managed_clients = BTreeSet::from(["managed-client"]);
        let managed_order_ids = BTreeSet::from(["ord-1"]);

        let (diagnostics, unavailable) = reconcile_position_attribution(
            &positions,
            &fills,
            &managed_clients,
            &managed_order_ids,
        )
        .expect("position attribution");

        assert_eq!(unavailable, 0);
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].authoritative_exchange_position, "2");
        assert_eq!(diagnostics[0].managed_fill_delta_in_bounded_history, "1");
        assert_eq!(
            diagnostics[0].unattributed_external_or_outside_bounded_history_residual,
            "1"
        );
    }

    #[test]
    fn external_activity_is_not_attributed_to_managed_intent() {
        let p = path("external");
        let store = ExecutionLedgerStore::at(&p);
        let ledger = DurableExecutionLedger::open(store, 100).expect("ledger");
        let orders = vec![ExchangeOrderIdentity {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            order_id: "manual-1".to_owned(),
            client_order_id: String::new(),
            state: "filled".to_owned(),
            size: "1".to_owned(),
            accumulated_fill_size: "0".to_owned(),
            update_time_ms: "1790884800000".to_owned(),
        }];
        let fills = vec![ExchangeFillIdentity {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            order_id: Some("manual-1".to_owned()),
            client_order_id: String::new(),
            trade_id: "trade-1".to_owned(),
            bill_id: None,
            fill_pnl: None,
            side: "buy".to_owned(),
            position_side: "long".to_owned(),
            fill_price: "0.1".to_owned(),
            fill_size: "1".to_owned(),
            fee: Some("-0.001".to_owned()),
            fee_currency: Some("USDT".to_owned()),
            execution_type: Some("T".to_owned()),
            fill_time_ms: 1_790_884_800_000,
        }];
        let result = reconcile_exchange_evidence(&ledger, &[], &orders, &fills).expect("reconcile");
        assert!(result.consistent);
        assert_eq!(result.managed_intents, 0);
        assert_eq!(result.unattributed_external_or_exchange_system_orders, 1);
        assert_eq!(result.unattributed_external_or_exchange_system_fills, 1);
        let _ = std::fs::remove_file(p);
    }
}

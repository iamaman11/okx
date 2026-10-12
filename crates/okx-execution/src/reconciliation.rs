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
};

pub const ACCOUNT_LEDGER_RECONCILIATION_SCHEMA_V1: &str = "okx.account-ledger-reconciliation/v1";
const MAX_POSITION_ATTRIBUTION_ROWS: usize = 20;
pub const STRATEGY_ATTRIBUTION_SCHEMA_V1: &str = "okx.strategy-exchange-attribution/v1";

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
    /// ONLY independently corroborated fill/bill amounts, not all fill fees.
    pub fills_fee_totals: Vec<LinkedCurrencyAmount>,
    pub linked_trade_bill_pnl_totals: Vec<LinkedCurrencyAmount>,
    pub incomplete_reasons: Vec<&'static str>,
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
    result.strategy_attribution = Some(attribute_strategy_exchange_evidence(
        ledger,
        &facts.exchange_orders,
        &facts.exchange_fills,
        &facts.exchange_bills,
        facts.bills_history_complete,
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

    for entry in ledger.entries() {
        let Some(_lineage) = entry.record.lineage.as_ref() else {
            continue;
        };
        lineage_bound_intents += 1;
        let plan = &entry.record.plan;
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
    reasons.push("strategy_capital_cashflows_and_mark_to_market_unattested");
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
        fills_fee_totals: linked_amounts(fees),
        linked_trade_bill_pnl_totals: linked_amounts(pnl),
        incomplete_reasons: reasons,
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

        let missing_history =
            attribute_strategy_exchange_evidence(&ledger, &[order], &[fill], &[bill], false)
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
        let report = attribute_strategy_exchange_evidence(&ledger, &[], &[fill], &[], false)
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

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use okx_observation::{
    AccountLedgerFacts, AccountPositionState, ExchangeFillIdentity, ExchangeOrderIdentity,
};
use rust_decimal::Decimal;
use serde::Serialize;
use thiserror::Error;

use crate::{DurableExecutionLedger, ExecutionState};

pub const ACCOUNT_LEDGER_RECONCILIATION_SCHEMA_V1: &str = "okx.account-ledger-reconciliation/v1";
const MAX_POSITION_ATTRIBUTION_ROWS: usize = 20;

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
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AccountLedgerReconciliationError {
    #[error("multiple exchange orders resolve to managed client order id '{client_order_id}'")]
    DuplicateManagedExchangeOrder { client_order_id: String },

    #[error("position attribution decimal '{field}' is invalid: '{value}'")]
    InvalidDecimal { field: &'static str, value: String },
}

pub fn reconcile_account_ledger(
    ledger: &DurableExecutionLedger,
    facts: &AccountLedgerFacts,
) -> Result<AccountLedgerReconciliation, AccountLedgerReconciliationError> {
    reconcile_exchange_evidence(
        ledger,
        &facts.authoritative_positions,
        &facts.exchange_orders,
        &facts.exchange_fills,
    )
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
    })
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
            side: "buy".to_owned(),
            position_side: "long".to_owned(),
            fill_size: "1".to_owned(),
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
            side: "buy".to_owned(),
            position_side: "long".to_owned(),
            fill_size: "1".to_owned(),
        }];
        let result = reconcile_exchange_evidence(&ledger, &[], &orders, &fills).expect("reconcile");
        assert!(result.consistent);
        assert_eq!(result.managed_intents, 0);
        assert_eq!(result.unattributed_external_or_exchange_system_orders, 1);
        assert_eq!(result.unattributed_external_or_exchange_system_fills, 1);
        let _ = std::fs::remove_file(p);
    }
}

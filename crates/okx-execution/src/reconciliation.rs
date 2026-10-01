use std::collections::{BTreeMap, BTreeSet};

use okx_observation::AccountLedgerFacts;
use serde::Serialize;
use thiserror::Error;

use crate::{DurableExecutionLedger, ExecutionState};

pub const ACCOUNT_LEDGER_RECONCILIATION_SCHEMA_V1: &str =
    "okx.account-ledger-reconciliation/v1";

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
    pub identity_mismatches: usize,
    pub consistent: bool,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AccountLedgerReconciliationError {
    #[error(
        "multiple exchange orders resolve to managed client order id '{client_order_id}'"
    )]
    DuplicateManagedExchangeOrder { client_order_id: String },
}

pub fn reconcile_account_ledger(
    ledger: &DurableExecutionLedger,
    facts: &AccountLedgerFacts,
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

    let managed_exchange_orders_observed = facts
        .exchange_orders
        .iter()
        .filter(|order| managed_clients.contains(order.client_order_id.as_str()))
        .count();
    let unattributed_orders = facts.exchange_orders.len() - managed_exchange_orders_observed;

    let managed_exchange_fills_observed = facts
        .exchange_fills
        .iter()
        .filter(|fill| {
            managed_clients.contains(fill.client_order_id.as_str())
                || fill
                    .order_id
                    .as_deref()
                    .is_some_and(|order_id| managed_order_ids.contains(order_id))
        })
        .count();
    let unattributed_fills = facts.exchange_fills.len() - managed_exchange_fills_observed;

    let mut by_client = BTreeMap::<&str, Vec<_>>::new();
    for order in &facts.exchange_orders {
        if !order.client_order_id.trim().is_empty() {
            by_client
                .entry(order.client_order_id.as_str())
                .or_default()
                .push(order);
        }
    }

    let mut matched = 0usize;
    let mut unresolved = 0usize;
    let mut unexpected = 0usize;
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

        match entry.record.state {
            ExecutionState::Prepared | ExecutionState::Rejected => {
                if observed.is_some() {
                    unexpected += 1;
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

    Ok(AccountLedgerReconciliation {
        schema: ACCOUNT_LEDGER_RECONCILIATION_SCHEMA_V1,
        managed_intents: managed_clients.len(),
        managed_exchange_orders_observed,
        managed_exchange_fills_observed,
        unattributed_external_or_exchange_system_orders: unattributed_orders,
        unattributed_external_or_exchange_system_fills: unattributed_fills,
        managed_intents_with_exchange_match: matched,
        managed_intents_unresolved_in_bounded_exchange_evidence: unresolved,
        unexpected_exchange_orders_for_non_submitted_intents: unexpected,
        identity_mismatches,
        consistent: unexpected == 0 && identity_mismatches == 0,
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use okx_execution_test_support::*;
    use okx_observation::{
        AccountAuthorityEvidence, AccountLedgerSummary, CurrencyAggregate, ExchangeFillIdentity,
        ExchangeOrderIdentity,
    };

    use super::*;
    use crate::{
        ExecutionLedgerStore, ExecutionPlan, OrderSide, OrderType, PositionSide, PrepareDisposition,
        TradeMode, derive_client_order_id,
    };

    mod okx_execution_test_support {
        use okx_observation::{AccountLedgerFacts, AccountLedgerSummary};
        use std::collections::BTreeMap;

        pub fn facts(
            summary: AccountLedgerSummary,
            exchange_orders: Vec<okx_observation::ExchangeOrderIdentity>,
            exchange_fills: Vec<okx_observation::ExchangeFillIdentity>,
        ) -> AccountLedgerFacts {
            AccountLedgerFacts::for_test(summary, exchange_orders, exchange_fills, BTreeMap::new())
        }
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
            action: crate::ExecutionAction::Open,
            order_type: OrderType::Limit,
            size: "1".to_owned(),
            price: "0.1".to_owned(),
            open_risk: None,
        }
    }

    fn summary() -> AccountLedgerSummary {
        AccountLedgerSummary {
            schema: "okx.account-ledger-summary/v1",
            source_received_at: "2026-10-01T20:00:00Z".to_owned(),
            account_generation: "sha256:account".to_owned(),
            authority: AccountAuthorityEvidence {
                scope: "authenticated_account_only",
                account_type: "1".to_owned(),
                is_subaccount: true,
                account_uid_fingerprint: "uid-fingerprint".to_owned(),
                main_account_uid_fingerprint: Some("main".to_owned()),
                api_key_permissions: vec!["read_only".to_owned()],
                multi_account_inventory_complete: false,
            },
            total_equity_usd: "100".to_owned(),
            open_positions: 0,
            pending_orders: 0,
            current_unrealized_pnl: Vec::<CurrencyAggregate>::new(),
            history_coverage: Vec::new(),
            realized_pnl_basis: "positions",
            realized_pnl: Vec::new(),
            trade_fee_basis: "fills",
            trade_fees: Vec::new(),
            funding_basis: "bills",
            funding: Vec::new(),
            position_pnl_identity_rows_checked: 0,
            fill_order_links_checked: 0,
            fill_order_links_unresolved_due_to_truncation: 0,
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

        let client_order_id = plan.client_order_id.clone();
        let facts = facts(
            summary(),
            vec![ExchangeOrderIdentity {
                instrument_type: "SWAP".to_owned(),
                instrument_id: plan.instrument_id,
                order_id: "ord-1".to_owned(),
                client_order_id,
                state: "live".to_owned(),
                update_time_ms: "1790884800000".to_owned(),
            }],
            Vec::new(),
        );
        let result = reconcile_account_ledger(&ledger, &facts).expect("reconcile");
        assert!(!result.consistent);
        assert_eq!(
            result.unexpected_exchange_orders_for_non_submitted_intents,
            1
        );
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn external_activity_is_not_attributed_to_managed_intent() {
        let p = path("external");
        let store = ExecutionLedgerStore::at(&p);
        let ledger = DurableExecutionLedger::open(store, 100).expect("ledger");
        let facts = facts(
            summary(),
            vec![ExchangeOrderIdentity {
                instrument_type: "SWAP".to_owned(),
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                order_id: "manual-1".to_owned(),
                client_order_id: String::new(),
                state: "filled".to_owned(),
                update_time_ms: "1790884800000".to_owned(),
            }],
            vec![ExchangeFillIdentity {
                instrument_type: "SWAP".to_owned(),
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                order_id: Some("manual-1".to_owned()),
                client_order_id: String::new(),
                trade_id: "trade-1".to_owned(),
            }],
        );
        let result = reconcile_account_ledger(&ledger, &facts).expect("reconcile");
        assert!(result.consistent);
        assert_eq!(result.managed_intents, 0);
        assert_eq!(
            result.unattributed_external_or_exchange_system_orders,
            1
        );
        assert_eq!(
            result.unattributed_external_or_exchange_system_fills,
            1
        );
        let _ = std::fs::remove_file(p);
    }
}

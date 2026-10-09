use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use okx_api::{
    AccountBill, AccountConfig, BoundedHistory, FillHistory, FundingBalance, HistoricalOrder,
    PositionHistory, account_uid_fingerprint,
};
use rust_decimal::Decimal;
use serde::Serialize;
use thiserror::Error;

use crate::AccountSnapshot;

pub const ACCOUNT_LEDGER_SUMMARY_SCHEMA_V1: &str = "okx.account-ledger-summary/v1";
pub const ACCOUNT_LEDGER_HISTORY_WINDOW: &str = "last_3_months";
const FUNDING_EXPENSE_SUBTYPE: &str = "173";
const FUNDING_INCOME_SUBTYPE: &str = "174";
const MAX_HISTORY_INSTRUMENT_SAMPLES: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountAuthorityEvidence {
    pub scope: &'static str,
    pub account_type: String,
    pub is_subaccount: bool,
    pub account_uid_fingerprint: String,
    pub main_account_uid_fingerprint: Option<String>,
    pub api_key_permissions: Vec<String>,
    pub multi_account_inventory_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountHistoryCoverage {
    pub resource: String,
    pub documented_window: &'static str,
    pub instrument_count: usize,
    pub sample_instruments: Vec<String>,
    pub rows: usize,
    pub pages: usize,
    pub complete_within_bound: bool,
    pub newest_event_time_ms: Option<String>,
    pub oldest_event_time_ms: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FundingBalanceEvidence {
    pub currency: String,
    pub balance: String,
    pub available_balance: String,
    pub frozen_balance: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CurrencyAggregate {
    pub currency: String,
    pub amount: String,
    pub events: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountLedgerSummary {
    pub schema: &'static str,
    pub source_received_at: String,
    pub current_account_as_of_ms: Option<String>,
    pub account_generation: String,
    pub authority: AccountAuthorityEvidence,
    pub total_equity_usd: String,
    pub trading_equity_detail_usd_sum: String,
    pub trading_equity_residual_usd: String,
    pub funding_balances: Vec<FundingBalanceEvidence>,
    pub open_positions: usize,
    pub pending_orders: usize,
    pub current_unrealized_pnl: Vec<CurrencyAggregate>,
    pub history_coverage: Vec<AccountHistoryCoverage>,
    pub realized_pnl_basis: &'static str,
    pub realized_pnl: Vec<CurrencyAggregate>,
    pub daily_realized_pnl_utc_basis: &'static str,
    pub daily_realized_pnl_utc_day_start_ms: Option<String>,
    pub daily_realized_pnl_utc_day_end_ms: Option<String>,
    pub daily_realized_pnl_utc: Vec<CurrencyAggregate>,
    pub trade_fee_basis: &'static str,
    pub trade_fees: Vec<CurrencyAggregate>,
    pub funding_basis: &'static str,
    pub funding: Vec<CurrencyAggregate>,
    pub position_pnl_identity_rows_checked: usize,
    pub fill_order_links_checked: usize,
    pub fill_order_links_unresolved_due_to_truncation: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeOrderIdentity {
    pub instrument_type: String,
    pub instrument_id: String,
    pub order_id: String,
    pub client_order_id: String,
    pub state: String,
    pub update_time_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionFillEvidence {
    pub fills: Vec<ExchangeFillIdentity>,
    pub pages: usize,
    pub complete_within_bound: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeFillIdentity {
    pub instrument_type: String,
    pub instrument_id: String,
    pub order_id: Option<String>,
    pub client_order_id: String,
    pub trade_id: String,
    pub side: String,
    pub position_side: String,
    pub fill_price: String,
    pub fill_size: String,
    pub fee: Option<String>,
    pub fee_currency: Option<String>,
    pub execution_type: Option<String>,
    pub fill_time_ms: u64,
}

#[derive(Debug, Clone)]
pub struct AccountLedgerFacts {
    pub summary: AccountLedgerSummary,
    pub authoritative_positions: Vec<crate::AccountPositionState>,
    pub exchange_orders: Vec<ExchangeOrderIdentity>,
    pub exchange_fills: Vec<ExchangeFillIdentity>,
    order_history_complete: BTreeMap<String, bool>,
}

impl AccountLedgerFacts {
    pub fn order_history_complete(&self, instrument_type: &str) -> bool {
        self.order_history_complete
            .get(instrument_type)
            .copied()
            .unwrap_or(false)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_okx(
        source_received_at: impl Into<String>,
        snapshot: &AccountSnapshot,
        config: AccountConfig,
        funding_balances: Vec<FundingBalance>,
        position_histories: Vec<(String, BoundedHistory<PositionHistory>)>,
        order_histories: Vec<(String, BoundedHistory<HistoricalOrder>)>,
        fill_histories: Vec<(String, BoundedHistory<FillHistory>)>,
        bills: BoundedHistory<AccountBill>,
    ) -> Result<Self, AccountLedgerError> {
        let source_received_at = source_received_at.into();
        if source_received_at.trim().is_empty() {
            return Err(AccountLedgerError::EmptySourceTimestamp);
        }

        let uid = required("config.uid", &config.uid)?;
        if account_uid_fingerprint(uid) != snapshot.account_uid_fingerprint {
            return Err(AccountLedgerError::AuthorityMismatch);
        }
        let main_uid = optional(&config.main_uid);
        let is_subaccount = main_uid.is_some_and(|main| main != uid);
        let authority = AccountAuthorityEvidence {
            scope: "authenticated_account_only",
            account_type: snapshot.account_type.clone(),
            is_subaccount,
            account_uid_fingerprint: snapshot.account_uid_fingerprint.clone(),
            main_account_uid_fingerprint: main_uid.map(account_uid_fingerprint),
            api_key_permissions: snapshot.api_key_permissions.clone(),
            multi_account_inventory_complete: false,
        };

        let total_equity = decimal_required("balance.totalEq", &snapshot.balance.total_equity_usd)?;
        let detail_equity_sum =
            snapshot
                .balance
                .details
                .iter()
                .try_fold(Decimal::ZERO, |total, detail| {
                    let Some(eq_usd) = detail.equity_usd.as_deref() else {
                        return Ok::<Decimal, AccountLedgerError>(total);
                    };
                    if eq_usd.trim().is_empty() {
                        return Ok(total);
                    }
                    Ok(total + decimal_required("balance.details.eqUsd", eq_usd)?)
                })?;
        let total_equity_usd = total_equity.normalize().to_string();
        let trading_equity_detail_usd_sum = detail_equity_sum.normalize().to_string();
        let trading_equity_residual_usd =
            (total_equity - detail_equity_sum).normalize().to_string();
        let funding_balances = normalize_funding_balances(funding_balances)?;
        let current_account_as_of_ms = current_account_as_of(snapshot)?;
        let current_unrealized_pnl = aggregate_current_unrealized(snapshot)?;
        let utc_day_bounds = current_account_as_of_ms
            .as_deref()
            .map(|value| timestamp_required("account.current_as_of", value))
            .transpose()?
            .map(|as_of| {
                const UTC_DAY_MS: u64 = 86_400_000;
                let start = (as_of / UTC_DAY_MS) * UTC_DAY_MS;
                (start, start + UTC_DAY_MS)
            });

        let mut coverage = Vec::new();
        let mut realized = BTreeMap::<String, Aggregate>::new();
        let mut daily_realized = BTreeMap::<String, Aggregate>::new();
        let mut pnl_rows_checked = 0usize;
        let mut seen_position_rows = BTreeSet::new();

        for (expected_type, history) in &position_histories {
            coverage.push(coverage_for(
                &format!("positions_history:{expected_type}"),
                history,
                |row| row.update_time_ms.as_str(),
                |row| row.instrument_id.as_str(),
            )?);
            for row in &history.rows {
                require_expected_type(expected_type, &row.instrument_type)?;
                let position_id = required("positions_history.posId", &row.position_id)?;
                let event_time =
                    timestamp_required("positions_history.uTime", &row.update_time_ms)?;
                let identity = format!("{}:{}:{}", row.instrument_type, position_id, event_time);
                if !seen_position_rows.insert(identity.clone()) {
                    return Err(AccountLedgerError::DuplicateIdentity(identity));
                }

                let realized_value =
                    decimal_required("positions_history.realizedPnl", &row.realized_pnl)?;
                let pnl = decimal_or_zero("positions_history.pnl", &row.pnl)?;
                let fee = decimal_or_zero("positions_history.fee", &row.fee)?;
                let funding = decimal_or_zero("positions_history.fundingFee", &row.funding_fee)?;
                let liquidation =
                    decimal_or_zero("positions_history.liqPenalty", &row.liquidation_penalty)?;
                let settled = decimal_or_zero("positions_history.settledPnl", &row.settled_pnl)?;
                let components = pnl + fee + funding + liquidation + settled;
                if realized_value != components {
                    return Err(AccountLedgerError::PositionPnlIdentityMismatch {
                        position_id: position_id.to_owned(),
                        realized: realized_value.normalize().to_string(),
                        components: components.normalize().to_string(),
                    });
                }
                pnl_rows_checked += 1;

                if !realized_value.is_zero() {
                    let currency = required("positions_history.ccy", &row.ccy)?;
                    add_aggregate(&mut realized, currency, realized_value);
                    if let Some((day_start, day_end)) = utc_day_bounds
                        && event_time >= day_start
                        && event_time < day_end
                    {
                        add_aggregate(&mut daily_realized, currency, realized_value);
                    }
                }
            }
        }

        let mut exchange_orders = Vec::new();
        let mut order_ids = BTreeSet::new();
        let mut order_history_complete = BTreeMap::new();
        for pending in &snapshot.pending_orders {
            if !order_ids.insert(pending.order_id.clone()) {
                return Err(AccountLedgerError::DuplicateIdentity(format!(
                    "order:{}",
                    pending.order_id
                )));
            }
            exchange_orders.push(ExchangeOrderIdentity {
                instrument_type: pending.instrument_type.clone(),
                instrument_id: pending.instrument_id.clone(),
                order_id: pending.order_id.clone(),
                client_order_id: pending.client_order_id.clone().unwrap_or_default(),
                state: pending.state.clone(),
                update_time_ms: pending.update_time_ms.clone(),
            });
        }

        for (expected_type, history) in &order_histories {
            order_history_complete.insert(expected_type.clone(), history.complete);
            coverage.push(coverage_for(
                &format!("orders_history:{expected_type}"),
                history,
                |row| row.update_time_ms.as_str(),
                |row| row.instrument_id.as_str(),
            )?);
            for row in &history.rows {
                require_expected_type(expected_type, &row.instrument_type)?;
                let order_id = required("orders_history.ordId", &row.order_id)?;
                timestamp_required("orders_history.uTime", &row.update_time_ms)?;
                if !order_ids.insert(order_id.to_owned()) {
                    return Err(AccountLedgerError::DuplicateIdentity(format!(
                        "order:{order_id}"
                    )));
                }
                exchange_orders.push(ExchangeOrderIdentity {
                    instrument_type: row.instrument_type.clone(),
                    instrument_id: required("orders_history.instId", &row.instrument_id)?
                        .to_owned(),
                    order_id: order_id.to_owned(),
                    client_order_id: row.client_order_id.clone(),
                    state: required("orders_history.state", &row.state)?.to_owned(),
                    update_time_ms: row.update_time_ms.clone(),
                });
            }
        }

        let mut fees = BTreeMap::<String, Aggregate>::new();
        let mut exchange_fills = Vec::new();
        let mut fill_ids = BTreeSet::new();
        let mut fill_order_links_checked = 0usize;
        let mut unresolved_due_to_truncation = 0usize;

        for (expected_type, history) in &fill_histories {
            coverage.push(coverage_for(
                &format!("fills_history:{expected_type}"),
                history,
                |row| row.fill_time_ms.as_str(),
                |row| row.instrument_id.as_str(),
            )?);
            for row in &history.rows {
                let fill = normalize_fill_identity(expected_type, row)?;
                let identity = format!("{}:{}", fill.instrument_id, fill.trade_id);
                if !fill_ids.insert(identity.clone()) {
                    return Err(AccountLedgerError::DuplicateIdentity(format!(
                        "fill:{identity}"
                    )));
                }

                if let (Some(fee), Some(currency)) =
                    (fill.fee.as_deref(), fill.fee_currency.as_deref())
                {
                    add_aggregate(
                        &mut fees,
                        currency,
                        decimal_required("fills_history.fee", fee)?,
                    );
                }

                if let Some(order_id) = fill.order_id.as_deref() {
                    fill_order_links_checked += 1;
                    if !order_ids.contains(order_id) {
                        if order_history_complete
                            .get(expected_type)
                            .copied()
                            .unwrap_or(false)
                        {
                            return Err(AccountLedgerError::FillOrderMissing {
                                instrument_type: expected_type.clone(),
                                order_id: order_id.to_owned(),
                                trade_id: fill.trade_id.clone(),
                            });
                        }
                        unresolved_due_to_truncation += 1;
                    }
                }

                exchange_fills.push(fill);
            }
        }

        coverage.push(coverage_for(
            "bills_history",
            &bills,
            |row| row.timestamp_ms.as_str(),
            |row| row.instrument_id.as_str(),
        )?);
        let mut funding = BTreeMap::<String, Aggregate>::new();
        let mut bill_ids = BTreeSet::new();
        for bill in &bills.rows {
            let bill_id = required("bills_history.billId", &bill.bill_id)?;
            timestamp_required("bills_history.ts", &bill.timestamp_ms)?;
            if !bill_ids.insert(bill_id.to_owned()) {
                return Err(AccountLedgerError::DuplicateIdentity(format!(
                    "bill:{bill_id}"
                )));
            }
            if matches!(
                bill.bill_sub_type.as_str(),
                FUNDING_EXPENSE_SUBTYPE | FUNDING_INCOME_SUBTYPE
            ) {
                let amount = decimal_required("bills_history.pnl", &bill.pnl)?;
                let currency = required("bills_history.ccy", &bill.ccy)?;
                add_aggregate(&mut funding, currency, amount);
            }
        }

        Ok(Self {
            authoritative_positions: snapshot.positions.clone(),
            summary: AccountLedgerSummary {
                schema: ACCOUNT_LEDGER_SUMMARY_SCHEMA_V1,
                source_received_at,
                current_account_as_of_ms,
                account_generation: snapshot.account_generation.clone(),
                authority,
                total_equity_usd,
                trading_equity_detail_usd_sum,
                trading_equity_residual_usd,
                funding_balances,
                open_positions: snapshot.positions.len(),
                pending_orders: snapshot.pending_orders.len(),
                current_unrealized_pnl,
                history_coverage: coverage,
                realized_pnl_basis: "positions-history.realizedPnl; exact OKX identity checked per row",
                realized_pnl: finish_aggregates(realized),
                daily_realized_pnl_utc_basis: "positions-history.realizedPnl filtered by uTime into [UTC day start, next UTC day); rebuilt from exchange history after restart",
                daily_realized_pnl_utc_day_start_ms: utc_day_bounds
                    .map(|(start, _)| start.to_string()),
                daily_realized_pnl_utc_day_end_ms: utc_day_bounds.map(|(_, end)| end.to_string()),
                daily_realized_pnl_utc: finish_aggregates(daily_realized),
                trade_fee_basis: "fills-history.fee; deduplicated by instId+tradeId",
                trade_fees: finish_aggregates(fees),
                funding_basis: "bills-archive subType=173/174 pnl; deduplicated by billId",
                funding: finish_aggregates(funding),
                position_pnl_identity_rows_checked: pnl_rows_checked,
                fill_order_links_checked,
                fill_order_links_unresolved_due_to_truncation: unresolved_due_to_truncation,
            },
            exchange_orders,
            exchange_fills,
            order_history_complete,
        })
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AccountLedgerError {
    #[error("account ledger source receive timestamp is empty")]
    EmptySourceTimestamp,

    #[error("account authority does not match the coherent account snapshot")]
    AuthorityMismatch,

    #[error("account ledger field '{field}' is missing")]
    MissingField { field: &'static str },

    #[error("account ledger timestamp '{field}' is invalid: '{value}'")]
    InvalidTimestamp { field: &'static str, value: String },

    #[error("account ledger decimal '{field}' is invalid: '{value}'")]
    InvalidDecimal { field: &'static str, value: String },

    #[error("account ledger row instrument type '{actual}' does not match requested '{expected}'")]
    InstrumentTypeMismatch { expected: String, actual: String },

    #[error("duplicate exchange history identity '{0}'")]
    DuplicateIdentity(String),

    #[error(
        "execution fill identity field '{field}' mismatch: expected '{expected}', actual '{actual}'"
    )]
    ExecutionFillIdentityMismatch {
        field: &'static str,
        expected: String,
        actual: String,
    },

    #[error(
        "position '{position_id}' realized PnL '{realized}' does not reconcile to components '{components}'"
    )]
    PositionPnlIdentityMismatch {
        position_id: String,
        realized: String,
        components: String,
    },

    #[error(
        "fill '{trade_id}' for {instrument_type} references order '{order_id}' absent from complete order history"
    )]
    FillOrderMissing {
        instrument_type: String,
        order_id: String,
        trade_id: String,
    },
}

#[derive(Default)]
struct Aggregate {
    amount: Decimal,
    events: usize,
}

pub fn normalize_execution_fill_history(
    expected_type: &str,
    expected_instrument_id: &str,
    expected_order_id: &str,
    expected_client_order_id: &str,
    history: &BoundedHistory<FillHistory>,
) -> Result<ExecutionFillEvidence, AccountLedgerError> {
    let mut seen = BTreeSet::new();
    let mut fills = Vec::with_capacity(history.rows.len());
    for row in &history.rows {
        let fill = normalize_fill_identity(expected_type, row)?;
        require_execution_fill_identity("instId", expected_instrument_id, &fill.instrument_id)?;
        require_execution_fill_identity(
            "ordId",
            expected_order_id,
            fill.order_id.as_deref().unwrap_or_default(),
        )?;
        require_execution_fill_identity(
            "clOrdId",
            expected_client_order_id,
            &fill.client_order_id,
        )?;

        let identity = format!("{}:{}", fill.instrument_id, fill.trade_id);
        if !seen.insert(identity.clone()) {
            return Err(AccountLedgerError::DuplicateIdentity(format!(
                "fill:{identity}"
            )));
        }
        fills.push(fill);
    }

    Ok(ExecutionFillEvidence {
        fills,
        pages: history.pages,
        complete_within_bound: history.complete,
    })
}

fn require_execution_fill_identity(
    field: &'static str,
    expected: &str,
    actual: &str,
) -> Result<(), AccountLedgerError> {
    if !expected.trim().is_empty() && expected == actual {
        Ok(())
    } else {
        Err(AccountLedgerError::ExecutionFillIdentityMismatch {
            field,
            expected: expected.to_owned(),
            actual: actual.to_owned(),
        })
    }
}

fn normalize_fill_identity(
    expected_type: &str,
    row: &FillHistory,
) -> Result<ExchangeFillIdentity, AccountLedgerError> {
    require_expected_type(expected_type, &row.instrument_type)?;
    let instrument_id = required("fills_history.instId", &row.instrument_id)?;
    let fill_time = timestamp_required("fills_history.fillTime", &row.fill_time_ms)?;
    let trade_id = required("fills_history.tradeId", &row.trade_id)?;
    let (fee, fee_currency) = if row.fee.trim().is_empty() {
        (None, None)
    } else {
        let fee = decimal_required("fills_history.fee", &row.fee)?;
        let currency = required("fills_history.feeCcy", &row.fee_currency)?;
        (Some(fee.normalize().to_string()), Some(currency.to_owned()))
    };
    let fill_price = decimal_required("fills_history.fillPx", &row.fill_price)?;
    if fill_price <= Decimal::ZERO {
        return Err(AccountLedgerError::InvalidDecimal {
            field: "fills_history.fillPx",
            value: row.fill_price.clone(),
        });
    }
    let fill_size = decimal_required("fills_history.fillSz", &row.fill_size)?;
    if fill_size <= Decimal::ZERO {
        return Err(AccountLedgerError::InvalidDecimal {
            field: "fills_history.fillSz",
            value: row.fill_size.clone(),
        });
    }

    Ok(ExchangeFillIdentity {
        instrument_type: row.instrument_type.clone(),
        instrument_id: instrument_id.to_owned(),
        order_id: optional(&row.order_id).map(str::to_owned),
        client_order_id: row.client_order_id.clone(),
        trade_id: trade_id.to_owned(),
        side: required("fills_history.side", &row.side)?.to_owned(),
        position_side: required("fills_history.posSide", &row.position_side)?.to_owned(),
        fill_price: fill_price.normalize().to_string(),
        fill_size: fill_size.normalize().to_string(),
        fee,
        fee_currency,
        execution_type: optional(&row.execution_type).map(str::to_owned),
        fill_time_ms: fill_time,
    })
}

fn current_account_as_of(snapshot: &AccountSnapshot) -> Result<Option<String>, AccountLedgerError> {
    let mut latest = None::<u64>;
    let mut admit = |field: &'static str, value: Option<&str>| -> Result<(), AccountLedgerError> {
        let Some(value) = value.filter(|value| !value.trim().is_empty()) else {
            return Ok(());
        };
        let timestamp = timestamp_required(field, value)?;
        latest = Some(latest.map_or(timestamp, |current| current.max(timestamp)));
        Ok(())
    };

    admit("balance.uTime", snapshot.balance.update_time_ms.as_deref())?;
    for detail in &snapshot.balance.details {
        admit("balance.details.uTime", detail.update_time_ms.as_deref())?;
    }
    for position in &snapshot.positions {
        admit("position.uTime", position.update_time_ms.as_deref())?;
    }
    for order in &snapshot.pending_orders {
        admit("order.uTime", Some(order.update_time_ms.as_str()))?;
    }

    Ok(latest.map(|value| value.to_string()))
}

fn normalize_funding_balances(
    rows: Vec<FundingBalance>,
) -> Result<Vec<FundingBalanceEvidence>, AccountLedgerError> {
    let mut seen = BTreeSet::new();
    let mut normalized = Vec::with_capacity(rows.len());
    for row in rows {
        let currency = required("funding_balance.ccy", &row.ccy)?;
        if !seen.insert(currency.to_owned()) {
            return Err(AccountLedgerError::DuplicateIdentity(format!(
                "funding_balance:{currency}"
            )));
        }
        let balance = decimal_required("funding_balance.bal", &row.bal)?;
        let available = decimal_required("funding_balance.availBal", &row.available_balance)?;
        let frozen = decimal_required("funding_balance.frozenBal", &row.frozen_balance)?;
        if balance < Decimal::ZERO || available < Decimal::ZERO || frozen < Decimal::ZERO {
            return Err(AccountLedgerError::InvalidDecimal {
                field: "funding_balance",
                value: format!(
                    "bal={},availBal={},frozenBal={}",
                    row.bal, row.available_balance, row.frozen_balance
                ),
            });
        }
        normalized.push(FundingBalanceEvidence {
            currency: currency.to_owned(),
            balance: balance.normalize().to_string(),
            available_balance: available.normalize().to_string(),
            frozen_balance: frozen.normalize().to_string(),
        });
    }
    normalized.sort_by(|a, b| a.currency.cmp(&b.currency));
    Ok(normalized)
}

fn aggregate_current_unrealized(
    snapshot: &AccountSnapshot,
) -> Result<Vec<CurrencyAggregate>, AccountLedgerError> {
    let mut values = BTreeMap::<String, Aggregate>::new();
    for detail in &snapshot.balance.details {
        let Some(text) = detail.unrealized_pnl.as_deref() else {
            continue;
        };
        if text.trim().is_empty() {
            continue;
        }
        let amount = decimal_required("balance.details.upl", text)?;
        if amount.is_zero() {
            continue;
        }
        add_aggregate(&mut values, &detail.currency, amount);
    }
    Ok(finish_aggregates(values))
}

fn coverage_for<T>(
    resource: &str,
    history: &BoundedHistory<T>,
    event_time: fn(&T) -> &str,
    instrument_id: fn(&T) -> &str,
) -> Result<AccountHistoryCoverage, AccountLedgerError> {
    let mut newest = None::<u64>;
    let mut oldest = None::<u64>;
    let mut instruments = BTreeSet::new();
    for row in &history.rows {
        let timestamp = timestamp_required("history.event_time", event_time(row))?;
        newest = Some(newest.map_or(timestamp, |current| current.max(timestamp)));
        oldest = Some(oldest.map_or(timestamp, |current| current.min(timestamp)));
        let instrument = instrument_id(row).trim();
        if !instrument.is_empty() {
            instruments.insert(instrument.to_owned());
        }
    }

    let instrument_count = instruments.len();
    let sample_instruments = instruments
        .into_iter()
        .take(MAX_HISTORY_INSTRUMENT_SAMPLES)
        .collect();

    Ok(AccountHistoryCoverage {
        resource: resource.to_owned(),
        documented_window: if resource.starts_with("orders_history:") {
            "recent_7_days_plus_archive_3_months;unfilled_canceled_2_hours"
        } else {
            ACCOUNT_LEDGER_HISTORY_WINDOW
        },
        instrument_count,
        sample_instruments,
        rows: history.rows.len(),
        pages: history.pages,
        complete_within_bound: history.complete,
        newest_event_time_ms: newest.map(|value| value.to_string()),
        oldest_event_time_ms: oldest.map(|value| value.to_string()),
    })
}

fn add_aggregate(values: &mut BTreeMap<String, Aggregate>, currency: &str, amount: Decimal) {
    let entry = values.entry(currency.to_owned()).or_default();
    entry.amount += amount;
    entry.events += 1;
}

fn finish_aggregates(values: BTreeMap<String, Aggregate>) -> Vec<CurrencyAggregate> {
    values
        .into_iter()
        .map(|(currency, aggregate)| CurrencyAggregate {
            currency,
            amount: aggregate.amount.normalize().to_string(),
            events: aggregate.events,
        })
        .collect()
}

fn required<'a>(field: &'static str, value: &'a str) -> Result<&'a str, AccountLedgerError> {
    let value = value.trim();
    if value.is_empty() {
        Err(AccountLedgerError::MissingField { field })
    } else {
        Ok(value)
    }
}

fn optional(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

fn timestamp_required(field: &'static str, value: &str) -> Result<u64, AccountLedgerError> {
    value
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| AccountLedgerError::InvalidTimestamp {
            field,
            value: value.to_owned(),
        })
}

fn decimal_required(field: &'static str, value: &str) -> Result<Decimal, AccountLedgerError> {
    let value = value.trim();
    if value.is_empty() || value.len() > 64 {
        return Err(AccountLedgerError::InvalidDecimal {
            field,
            value: value.to_owned(),
        });
    }
    Decimal::from_str(value).map_err(|_| AccountLedgerError::InvalidDecimal {
        field,
        value: value.to_owned(),
    })
}

fn decimal_or_zero(field: &'static str, value: &str) -> Result<Decimal, AccountLedgerError> {
    if value.trim().is_empty() {
        Ok(Decimal::ZERO)
    } else {
        decimal_required(field, value)
    }
}

fn require_expected_type(expected: &str, actual: &str) -> Result<(), AccountLedgerError> {
    if actual == expected {
        Ok(())
    } else {
        Err(AccountLedgerError::InstrumentTypeMismatch {
            expected: expected.to_owned(),
            actual: actual.to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AccountBalanceDetail, AccountBalanceState, AccountPositionState, PendingOrderState,
    };

    fn snapshot() -> AccountSnapshot {
        AccountSnapshot {
            schema: "okx.account-snapshot/v2".to_owned(),
            source: "test".to_owned(),
            source_received_at: "2026-10-01T20:00:00.000Z".to_owned(),
            account_generation: "sha256:account".to_owned(),
            quality_reason: "test".to_owned(),
            private_ws_connected: true,
            private_ws_generation: Some(2),
            private_ws_connection_fingerprint: Some("fp".to_owned()),
            private_ws_last_inbound_ms: Some(1_790_884_800_000),
            private_ws_events_applied: Some(1),
            account_level: "2".to_owned(),
            position_mode: "long_short_mode".to_owned(),
            account_type: "1".to_owned(),
            account_uid_fingerprint: account_uid_fingerprint("sub-uid"),
            api_key_permissions: vec!["read_only".to_owned()],
            balance: AccountBalanceState {
                total_equity_usd: "100.5".to_owned(),
                adjusted_equity_usd: None,
                isolated_equity_usd: None,
                initial_margin_requirement_usd: None,
                maintenance_margin_requirement_usd: None,
                margin_ratio: None,
                notional_usd: None,
                update_time_ms: Some("1790884800000".to_owned()),
                details: vec![AccountBalanceDetail {
                    currency: "USDT".to_owned(),
                    equity: "100.5".to_owned(),
                    cash_balance: None,
                    available_equity: None,
                    available_balance: None,
                    frozen_balance: None,
                    equity_usd: Some("100.5".to_owned()),
                    unrealized_pnl: Some("1.5".to_owned()),
                    update_time_ms: Some("1790884800000".to_owned()),
                }],
            },
            positions: Vec::<AccountPositionState>::new(),
            pending_orders: vec![PendingOrderState {
                order_id: "live-1".to_owned(),
                client_order_id: Some("manual".to_owned()),
                instrument_type: "SWAP".to_owned(),
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                side: "buy".to_owned(),
                position_side: Some("long".to_owned()),
                trade_mode: "cross".to_owned(),
                order_type: "limit".to_owned(),
                price: Some("0.1".to_owned()),
                size: "1".to_owned(),
                accumulated_fill_size: "0".to_owned(),
                average_fill_price: None,
                state: "live".to_owned(),
                reduce_only: Some(false),
                creation_time_ms: "1790884700000".to_owned(),
                update_time_ms: "1790884800000".to_owned(),
            }],
        }
    }

    fn config() -> AccountConfig {
        AccountConfig {
            account_level: "2".to_owned(),
            position_mode: "long_short_mode".to_owned(),
            uid: "sub-uid".to_owned(),
            main_uid: "main-uid".to_owned(),
            account_type: "1".to_owned(),
            account_stp_mode: "cancel_maker".to_owned(),
            auto_loan: false,
            greeks_type: String::new(),
            fee_type: String::new(),
            label: String::new(),
            ip: String::new(),
            perm: "read_only".to_owned(),
        }
    }

    fn position_history(realized: &str) -> PositionHistory {
        PositionHistory {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            margin_mode: "cross".to_owned(),
            position_id: "pos-1".to_owned(),
            position_side: "long".to_owned(),
            direction: "close".to_owned(),
            open_average_price: "0.09".to_owned(),
            close_average_price: "0.10".to_owned(),
            max_open_position: "10".to_owned(),
            total_closed_position: "10".to_owned(),
            realized_pnl: realized.to_owned(),
            settled_pnl: "0".to_owned(),
            pnl: "1.2".to_owned(),
            fee: "-0.1".to_owned(),
            funding_fee: "-0.05".to_owned(),
            liquidation_penalty: "0".to_owned(),
            pnl_ratio: "0.1".to_owned(),
            lever: "5".to_owned(),
            ccy: "USDT".to_owned(),
            close_type: "2".to_owned(),
            creation_time_ms: "1790880000000".to_owned(),
            update_time_ms: "1790884800000".to_owned(),
        }
    }

    fn order_history() -> HistoricalOrder {
        HistoricalOrder {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            order_id: "ord-1".to_owned(),
            client_order_id: "okx-managed".to_owned(),
            tag: String::new(),
            side: "sell".to_owned(),
            position_side: "long".to_owned(),
            trade_mode: "cross".to_owned(),
            order_type: "limit".to_owned(),
            px: "0.10".to_owned(),
            sz: "10".to_owned(),
            accumulated_fill_size: "10".to_owned(),
            average_fill_price: "0.10".to_owned(),
            state: "filled".to_owned(),
            category: "normal".to_owned(),
            source: String::new(),
            pnl: "1.2".to_owned(),
            fee: "-0.1".to_owned(),
            fee_currency: "USDT".to_owned(),
            trade_id: "trade-1".to_owned(),
            creation_time_ms: "1790884700000".to_owned(),
            update_time_ms: "1790884800000".to_owned(),
        }
    }

    fn fill_history() -> FillHistory {
        FillHistory {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            trade_id: "trade-1".to_owned(),
            order_id: "ord-1".to_owned(),
            client_order_id: "okx-managed".to_owned(),
            bill_id: "bill-fill-1".to_owned(),
            sub_type: "5".to_owned(),
            side: "sell".to_owned(),
            position_side: "long".to_owned(),
            fill_price: "0.10".to_owned(),
            fill_size: "10".to_owned(),
            fill_pnl: "1.2".to_owned(),
            fee: "-0.1".to_owned(),
            fee_currency: "USDT".to_owned(),
            execution_type: "T".to_owned(),
            timestamp_ms: "1790884800001".to_owned(),
            fill_time_ms: "1790884800000".to_owned(),
        }
    }

    fn funding_bill() -> AccountBill {
        AccountBill {
            bill_id: "bill-funding-1".to_owned(),
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            order_id: String::new(),
            trade_id: String::new(),
            ccy: "USDT".to_owned(),
            bill_type: "8".to_owned(),
            bill_sub_type: FUNDING_EXPENSE_SUBTYPE.to_owned(),
            balance_change: "-0.05".to_owned(),
            position_balance_change: String::new(),
            bal: "100".to_owned(),
            position_balance: String::new(),
            sz: "10".to_owned(),
            px: "0.095".to_owned(),
            pnl: "-0.05".to_owned(),
            fee: String::new(),
            margin_mode: "cross".to_owned(),
            execution_type: String::new(),
            client_order_id: String::new(),
            fill_time_ms: String::new(),
            timestamp_ms: "1790884800000".to_owned(),
        }
    }

    fn bounded<T>(rows: Vec<T>) -> BoundedHistory<T> {
        BoundedHistory {
            rows,
            pages: 1,
            complete: true,
        }
    }

    #[test]
    fn exact_execution_fill_history_preserves_bound_and_rejects_identity_drift() {
        let history = BoundedHistory {
            rows: vec![fill_history()],
            pages: 1,
            complete: false,
        };
        let evidence = normalize_execution_fill_history(
            "SWAP",
            "DOGE-USDT-SWAP",
            "ord-1",
            "okx-managed",
            &history,
        )
        .expect("exact fill evidence");
        assert_eq!(evidence.fills.len(), 1);
        assert_eq!(evidence.pages, 1);
        assert!(!evidence.complete_within_bound);

        let mismatch = normalize_execution_fill_history(
            "SWAP",
            "DOGE-USDT-SWAP",
            "ord-1",
            "different-client",
            &history,
        );
        assert!(matches!(
            mismatch,
            Err(AccountLedgerError::ExecutionFillIdentityMismatch {
                field: "clOrdId",
                ..
            })
        ));
    }

    #[test]
    fn normalizes_account_ledger_without_double_counting_sources() {
        let facts = AccountLedgerFacts::from_okx(
            "2026-10-01T20:00:01.000Z",
            &snapshot(),
            config(),
            vec![FundingBalance {
                ccy: "USDT".to_owned(),
                bal: "5".to_owned(),
                frozen_balance: "1".to_owned(),
                available_balance: "4".to_owned(),
            }],
            vec![
                ("SWAP".to_owned(), bounded(vec![position_history("1.05")])),
                ("FUTURES".to_owned(), bounded(Vec::new())),
            ],
            vec![
                ("SWAP".to_owned(), bounded(vec![order_history()])),
                ("FUTURES".to_owned(), bounded(Vec::new())),
            ],
            vec![
                ("SWAP".to_owned(), bounded(vec![fill_history()])),
                ("FUTURES".to_owned(), bounded(Vec::new())),
            ],
            bounded(vec![funding_bill()]),
        )
        .expect("ledger facts");

        assert!(facts.summary.authority.is_subaccount);
        assert!(!facts.summary.authority.multi_account_inventory_complete);
        assert_eq!(facts.summary.total_equity_usd, "100.5");
        assert_eq!(facts.summary.trading_equity_detail_usd_sum, "100.5");
        assert_eq!(facts.summary.trading_equity_residual_usd, "0");
        assert_eq!(facts.summary.funding_balances[0].balance, "5");
        assert_eq!(facts.summary.current_unrealized_pnl[0].amount, "1.5");
        assert_eq!(facts.summary.realized_pnl[0].amount, "1.05");
        assert_eq!(facts.summary.daily_realized_pnl_utc[0].amount, "1.05");
        assert_eq!(
            facts.summary.daily_realized_pnl_utc_day_start_ms.as_deref(),
            Some("1790812800000")
        );
        assert_eq!(facts.summary.trade_fees[0].amount, "-0.1");
        assert_eq!(facts.summary.funding[0].amount, "-0.05");
        assert_eq!(facts.summary.position_pnl_identity_rows_checked, 1);
        assert_eq!(facts.summary.fill_order_links_checked, 1);
        assert_eq!(
            facts.summary.fill_order_links_unresolved_due_to_truncation,
            0
        );
    }

    #[test]
    fn rejects_position_pnl_identity_residual() {
        let result = AccountLedgerFacts::from_okx(
            "2026-10-01T20:00:01.000Z",
            &snapshot(),
            config(),
            vec![FundingBalance {
                ccy: "USDT".to_owned(),
                bal: "5".to_owned(),
                frozen_balance: "1".to_owned(),
                available_balance: "4".to_owned(),
            }],
            vec![
                ("SWAP".to_owned(), bounded(vec![position_history("9")])),
                ("FUTURES".to_owned(), bounded(Vec::new())),
            ],
            vec![
                ("SWAP".to_owned(), bounded(vec![order_history()])),
                ("FUTURES".to_owned(), bounded(Vec::new())),
            ],
            vec![
                ("SWAP".to_owned(), bounded(vec![fill_history()])),
                ("FUTURES".to_owned(), bounded(Vec::new())),
            ],
            bounded(vec![funding_bill()]),
        );

        assert!(matches!(
            result,
            Err(AccountLedgerError::PositionPnlIdentityMismatch { .. })
        ));
    }

    #[test]
    fn zero_account_is_explicit_and_valid() {
        let mut zero = snapshot();
        zero.balance.total_equity_usd = "0".to_owned();
        zero.balance.details.clear();
        zero.positions.clear();
        zero.pending_orders.clear();

        let facts = AccountLedgerFacts::from_okx(
            "2026-10-01T20:00:01.000Z",
            &zero,
            config(),
            Vec::new(),
            vec![
                ("SWAP".to_owned(), bounded(Vec::new())),
                ("FUTURES".to_owned(), bounded(Vec::new())),
            ],
            vec![
                ("SWAP".to_owned(), bounded(Vec::new())),
                ("FUTURES".to_owned(), bounded(Vec::new())),
            ],
            vec![
                ("SWAP".to_owned(), bounded(Vec::new())),
                ("FUTURES".to_owned(), bounded(Vec::new())),
            ],
            bounded(Vec::new()),
        )
        .expect("zero account");

        assert_eq!(facts.summary.total_equity_usd, "0");
        assert_eq!(facts.summary.trading_equity_detail_usd_sum, "0");
        assert_eq!(facts.summary.trading_equity_residual_usd, "0");
        assert!(facts.summary.funding_balances.is_empty());
        assert_eq!(facts.summary.open_positions, 0);
        assert_eq!(facts.summary.pending_orders, 0);
        assert!(facts.summary.realized_pnl.is_empty());
        assert!(facts.summary.daily_realized_pnl_utc.is_empty());
        assert!(facts.summary.trade_fees.is_empty());
        assert!(facts.summary.funding.is_empty());
    }

    #[test]
    fn duplicate_fill_identity_fails_closed() {
        let fill = fill_history();
        let result = AccountLedgerFacts::from_okx(
            "2026-10-01T20:00:01.000Z",
            &snapshot(),
            config(),
            Vec::new(),
            vec![
                ("SWAP".to_owned(), bounded(vec![position_history("1.05")])),
                ("FUTURES".to_owned(), bounded(Vec::new())),
            ],
            vec![
                ("SWAP".to_owned(), bounded(vec![order_history()])),
                ("FUTURES".to_owned(), bounded(Vec::new())),
            ],
            vec![
                ("SWAP".to_owned(), bounded(vec![fill.clone(), fill])),
                ("FUTURES".to_owned(), bounded(Vec::new())),
            ],
            bounded(vec![funding_bill()]),
        );

        assert!(matches!(
            result,
            Err(AccountLedgerError::DuplicateIdentity(identity))
                if identity.starts_with("fill:")
        ));
    }

    #[test]
    fn incomplete_order_history_marks_fill_link_unresolved_instead_of_inventing_mismatch() {
        let mut fill = fill_history();
        fill.order_id = "older-order".to_owned();
        let facts = AccountLedgerFacts::from_okx(
            "2026-10-01T20:00:01.000Z",
            &snapshot(),
            config(),
            vec![FundingBalance {
                ccy: "USDT".to_owned(),
                bal: "5".to_owned(),
                frozen_balance: "1".to_owned(),
                available_balance: "4".to_owned(),
            }],
            vec![
                ("SWAP".to_owned(), bounded(vec![position_history("1.05")])),
                ("FUTURES".to_owned(), bounded(Vec::new())),
            ],
            vec![
                (
                    "SWAP".to_owned(),
                    BoundedHistory {
                        rows: vec![order_history()],
                        pages: 1,
                        complete: false,
                    },
                ),
                ("FUTURES".to_owned(), bounded(Vec::new())),
            ],
            vec![
                ("SWAP".to_owned(), bounded(vec![fill])),
                ("FUTURES".to_owned(), bounded(Vec::new())),
            ],
            bounded(vec![funding_bill()]),
        )
        .expect("bounded evidence");

        assert_eq!(
            facts.summary.fill_order_links_unresolved_due_to_truncation,
            1
        );
    }
}

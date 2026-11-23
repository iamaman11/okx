//! Parsed bill facts; classifications and ownership stay with the SQL owner.
use super::*;
#[derive(Debug, Clone)]
pub struct CapturedBill {
    pub bill_id: String,
    pub client_order_id: String,
    pub order_id: String,
    pub instrument_id: String,
    pub trade_id: String,
    pub bill_type: String,
    pub bill_sub_type: String,
    pub currency: String,
    pub time: i64,
    pub fee: Option<Decimal>,
    pub pnl: Option<Decimal>,
    pub balance_change: Option<Decimal>,
}
#[derive(Debug, Clone)]
pub struct CapturedBillPage {
    pub cursor: SourceCursor,
    pub rows: Vec<CapturedBill>,
}
pub fn captured_bill_pnl(sub_type: &str, value: Option<Decimal>) -> StoreResult<Decimal> {
    if matches!(sub_type, "173" | "174") {
        value.ok_or(StoreError::Invalid("FUNDING_AMOUNT_SOURCE_UNAVAILABLE"))
    } else {
        Ok(value.unwrap_or(Decimal::ZERO))
    }
}
pub fn captured_bill_residual_component(
    bill_type: &str,
    pnl: Option<Decimal>,
    fee: Option<Decimal>,
) -> StoreResult<Option<&'static str>> {
    if bill_type == "7" {
        if pnl.unwrap_or(Decimal::ZERO) != Decimal::ZERO
            || fee.unwrap_or(Decimal::ZERO) != Decimal::ZERO
        {
            return Err(StoreError::Invalid("INTEREST_BILL_AMBIGUOUS"));
        }
        Ok(Some("INTEREST"))
    } else if pnl.is_none() && fee.is_none() {
        Ok(Some("UNATTRIBUTED"))
    } else {
        Ok(None)
    }
}
impl AccountStore {
    pub fn ingest_captured_bills(
        &mut self,
        input: &CapturedBillPage,
        fx: &UsdConversionEvidence,
    ) -> StoreResult<()> {
        let page = Self::bind_captured_bills_sql(&self.connection, None, input)?;
        self.ingest_page_with_usd(&page, fx)
    }
    pub(crate) fn bind_captured_bills_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        input: &CapturedBillPage,
    ) -> StoreResult<IngestPage> {
        if input.rows.len() > 100 {
            return Err(StoreError::Invalid("BILL_HISTORY_PAGE_TOO_LARGE"));
        }
        if input.cursor.source != "bills" || input.cursor.stream_partition != "account" {
            return Err(StoreError::Invalid("SOURCE_CAPTURE_CURSOR_MISMATCH"));
        }
        let initialized_at = Self::source_capture_state_sql(db, scope)?.initialized_at;
        let mut page = IngestPage {
            cursor: Some(input.cursor.clone()),
            ..Default::default()
        };
        for b in &input.rows {
            let time = b.time;
            if time < initialized_at {
                continue;
            }
            let key = format!("bill:{}", b.bill_id);
            if !b.order_id.is_empty() && b.order_id != "0" && !b.instrument_id.is_empty() {
                page.bill_order_sources.push(BillOrderSource {
                    bill_id: b.bill_id.clone(),
                    instrument_id: b.instrument_id.clone(),
                    exchange_order_id: b.order_id.clone(),
                    trade_id: b.trade_id.clone(),
                });
            }
            let owner = Self::source_order_owner_sql(db, scope, &b.client_order_id, &b.order_id)?;
            let fee = b.fee.unwrap_or(Decimal::ZERO);
            if fee < Decimal::ZERO
                && let Some(i) = owner.as_ref().filter(|i| i.risk_increase)
                && i.exchange_order_id.as_deref() == Some(b.order_id.as_str())
                && i.instrument_id == b.instrument_id
            {
                if b.currency != "USDT" {
                    return Err(StoreError::Invalid("ENTRY_FEE_CURRENCY_UNPROVEN"));
                }
                page.entry_order_fee_links.push(EntryOrderFeeLink {
                    bill_id: b.bill_id.clone(),
                    intent_id: i.intent_id.clone(),
                    exchange_order_id: b.order_id.clone(),
                    instrument_id: b.instrument_id.clone(),
                    bill_at: time,
                    fee_usdt: fee,
                });
            }
            let instance = owner.map(|i| i.plan.strategy_instance);
            let amount = b.balance_change.unwrap_or(Decimal::ZERO);
            let residual_component = captured_bill_residual_component(&b.bill_type, b.pnl, b.fee)?;
            page.bills.push(Bill {
                bill_id: b.bill_id.clone(),
                kind: format!("{}:{}", b.bill_type, b.bill_sub_type),
                time,
                amount,
                currency: b.currency.clone(),
                economic_event_key: key.clone(),
            });
            let transfer = b.bill_type == "1" && matches!(b.bill_sub_type.as_str(), "11" | "12");
            if transfer {
                if !b.instrument_id.is_empty() || !b.order_id.is_empty() && b.order_id != "0" {
                    return Err(StoreError::Invalid("CASH_FLOW_LINKAGE_INVALID"));
                }
                page.cash_flows.push(CashFlow {
                    event_id: key.clone(),
                    kind: if amount >= Decimal::ZERO {
                        "TRANSFER_IN"
                    } else {
                        "TRANSFER_OUT"
                    }
                    .into(),
                    currency: b.currency.clone(),
                    amount,
                    time,
                    source_ref: key.clone(),
                });
            }
            for (component, value) in [
                ("FEE", b.fee.unwrap_or(Decimal::ZERO)),
                (
                    if matches!(b.bill_sub_type.as_str(), "173" | "174") {
                        "FUNDING"
                    } else {
                        "REALIZED_PNL"
                    },
                    captured_bill_pnl(&b.bill_sub_type, b.pnl)?,
                ),
            ] {
                if value == Decimal::ZERO {
                    continue;
                }
                if component == "FUNDING" {
                    page.funding_events.push(FundingEvent {
                        event_id: b.bill_id.clone(),
                        instrument_id: b.instrument_id.clone(),
                        time,
                        amount: value,
                        currency: b.currency.clone(),
                    });
                    continue;
                }
                page.finance.push(FinanceEntry {
                    entry_id: format!("{key}:{component}"),
                    economic_event_key: key.clone(),
                    component: component.into(),
                    allocation_leg: "account".into(),
                    amount: value,
                    currency: b.currency.clone(),
                    instance_id: instance.clone(),
                    source_ref: key.clone(),
                    time,
                    reverses_entry_id: None,
                });
            }
            if !transfer
                && amount != Decimal::ZERO
                && let Some(component) = residual_component
            {
                page.finance.push(FinanceEntry {
                    entry_id: format!("{key}:unattributed"),
                    economic_event_key: key.clone(),
                    component: component.into(),
                    allocation_leg: "account".into(),
                    amount,
                    currency: b.currency.clone(),
                    instance_id: None,
                    source_ref: key,
                    time,
                    reverses_entry_id: None,
                });
            }
        }
        Ok(page)
    }
}

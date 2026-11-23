//! Original USD normalization, with scoped idempotent source reads.
use super::super::*;
impl AccountStore {
    pub(crate) fn normalize_source_page_usd_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        page: &IngestPage,
        fx: &UsdConversionEvidence,
    ) -> StoreResult<IngestPage> {
        use rusqlite::{params_from_iter, types::Value};
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
        }
        if fx.source_digest.is_empty() || fx.observed_at <= 0 {
            return Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE"));
        }
        let values = |mut original: Vec<Value>| {
            if let Some((installation, account)) = scope {
                original.push(installation.to_owned().into());
                original.push(account.to_owned().into());
            }
            original
        };
        let mut normalized = page.clone();
        for flow in &page.cash_flows {
            if flow.kind == "OPENING" {
                continue;
            }
            let exists:bool=db.query_row(match scope {None=>"SELECT EXISTS(SELECT 1 FROM finance_entries WHERE economic_event_key=?1 AND component='EXTERNAL_FLOW_USD' AND allocation_leg='account')",Some(_)=>"SELECT EXISTS(SELECT 1 FROM finance_entries WHERE economic_event_key=?1 AND component='EXTERNAL_FLOW_USD' AND allocation_leg='account' AND installation_id=?2 AND account_key=?3)"},params_from_iter(values(vec![flow.event_id.clone().into()])),|r|r.get(0))?;
            if exists {
                continue;
            }
            let rate = if flow.currency == "USD" {
                Decimal::ONE
            } else {
                match fx.usd_per_currency.get(&flow.currency).copied() {
                    Some(v) => v,
                    None => {
                        return Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE"));
                    }
                }
            };
            if rate <= Decimal::ZERO {
                return Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE"));
            }
            let amount = flow
                .amount
                .checked_mul(rate)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            normalized.finance.push(FinanceEntry {
                entry_id: format!("{}:usd-flow", flow.event_id),
                economic_event_key: flow.event_id.clone(),
                component: "EXTERNAL_FLOW_USD".into(),
                allocation_leg: "account".into(),
                amount,
                currency: "USD".into(),
                instance_id: None,
                source_ref: format!(
                    "{}|fx:{}|rate:{}|at:{}",
                    flow.source_ref,
                    fx.source_digest,
                    exact(rate),
                    fx.observed_at
                ),
                time: flow.time,
                reverses_entry_id: None,
            });
        }
        for e in &page.finance {
            if e.amount >= Decimal::ZERO
                || !matches!(
                    e.component.as_str(),
                    "FEE" | "REALIZED_PNL" | "FUNDING" | "INTEREST" | "LIQUIDATION_COST"
                )
            {
                continue;
            }
            let leg = format!("{}:{}", e.component, e.allocation_leg);
            let exists:bool=db.query_row(match scope {None=>"SELECT EXISTS(SELECT 1 FROM finance_entries WHERE economic_event_key=?1 AND component='CAMPAIGN_LOSS_USD' AND allocation_leg=?2)",Some(_)=>"SELECT EXISTS(SELECT 1 FROM finance_entries WHERE economic_event_key=?1 AND component='CAMPAIGN_LOSS_USD' AND allocation_leg=?2 AND installation_id=?3 AND account_key=?4)"},params_from_iter(values(vec![e.economic_event_key.clone().into(),leg.clone().into()])),|r|r.get(0))?;
            if exists {
                continue;
            }
            let rate = if e.currency == "USD" {
                Decimal::ONE
            } else {
                match fx.usd_per_currency.get(&e.currency).copied() {
                    Some(v) => v,
                    None => {
                        return Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE"));
                    }
                }
            };
            if rate <= Decimal::ZERO {
                return Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE"));
            }
            let loss = (-e.amount)
                .checked_mul(rate)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?
                .round_dp_with_strategy(9, rust_decimal::RoundingStrategy::AwayFromZero);
            normalized.finance.push(FinanceEntry {
                entry_id: format!("{}:usd-loss:{leg}", e.economic_event_key),
                economic_event_key: e.economic_event_key.clone(),
                component: "CAMPAIGN_LOSS_USD".into(),
                allocation_leg: leg,
                amount: loss,
                currency: "USD".into(),
                instance_id: e.instance_id.clone(),
                source_ref: format!(
                    "{}|fx:{}|rate:{}|at:{}",
                    e.source_ref,
                    fx.source_digest,
                    exact(rate),
                    fx.observed_at
                ),
                time: e.time,
                reverses_entry_id: None,
            });
        }
        Ok(normalized)
    }
}

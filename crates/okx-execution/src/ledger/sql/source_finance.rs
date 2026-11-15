//! Financial source verification shared by account and installation storage.
use super::super::*;
type StrategyReversalSource = (String, String, String, Option<String>, Option<String>);

impl AccountStore {
    pub(crate) fn verify_strategy_finance_source_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        e: &FinanceEntry,
    ) -> StoreResult<()> {
        use rusqlite::{params_from_iter, types::Value};
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
        }
        // Only values are extended. Every scoped query below has explicit scope
        // predicates; this is not a SQL rewriter or a second source projection.
        let values = |mut original: Vec<Value>| {
            if let Some((installation, account)) = scope {
                original.push(installation.to_owned().into());
                original.push(account.to_owned().into());
            }
            original
        };
        if e.instance_id.as_deref().is_none_or(|id| id == "manual")
            || matches!(
                e.component.as_str(),
                "INTERNAL_CAPITAL_USD" | "CAMPAIGN_LOSS_USD"
            )
        {
            return Ok(());
        }
        if !matches!(
            e.component.as_str(),
            "FEE" | "FUNDING" | "REALIZED_PNL" | "LIQUIDATION_COST"
        ) {
            return Err(StoreError::Invalid("STRATEGY_NAV_COMPONENT_UNKNOWN"));
        }
        if let Some(reversed_id) = e.reverses_entry_id.as_deref() {
            // A correction derives its provenance from the immutable original.
            // The write path also checks reversal uniqueness before insert.
            let original: Option<StrategyReversalSource> = db
            .query_row(
                match scope {
                    None => "SELECT component,amount,currency,instance_id,reverses_entry_id FROM finance_entries WHERE entry_id=?1",
                    Some(_) => "SELECT component,amount,currency,instance_id,reverses_entry_id FROM finance_entries WHERE entry_id=?1 AND installation_id=?2 AND account_key=?3",
                },
                params_from_iter(values(vec![reversed_id.to_owned().into()])) ,
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .optional()?;
            if !original.is_some_and(|(component, amount, currency, owner, chained)| {
                component == e.component
                    && currency == e.currency
                    && owner == e.instance_id
                    && chained.is_none()
                    && decimal(amount).ok().and_then(|n| n.checked_add(e.amount))
                        == Some(Decimal::ZERO)
            }) {
                return Err(StoreError::Invalid(
                    "STRATEGY_NAV_ECONOMIC_SOURCE_UNVERIFIED",
                ));
            }
            // Matching the original does not authenticate the correction.
            // Its own new bill/fill must also exist in this account ledger.
        }
        let found = if let Some(id) = e.source_ref.strip_prefix("fill:") {
            let fill: Option<(String, String, String, String, String)> = db
            .query_row(
                match scope {
                    None => "SELECT instrument_id,trade_id,exchange_order_id,fee,currency FROM fills WHERE fill_id=?1",
                    Some(_) => "SELECT instrument_id,trade_id,exchange_order_id,fee,currency FROM fills WHERE fill_id=?1 AND installation_id=?2 AND account_key=?3",
                },
                params_from_iter(values(vec![id.to_owned().into()])),
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .optional()?;
            if let Some((instrument, trade, order, fee, fee_currency)) = fill {
                let canonical = format!("fill:{instrument}:{trade}:{order}");
                if e.component == "FEE" && (e.currency != fee_currency || e.amount != decimal(fee)?)
                {
                    return Err(StoreError::Invalid("STRATEGY_NAV_FEE_SOURCE_MISMATCH"));
                }
                // The venue fill already proves its fee. Its bill ID may be
                // known before the bill-history page arrives, so waiting for a
                // separate bill row would deadlock fills-first source capture.
                // One fill source cannot claim two different economic fee keys.
                let canonical_or_bill = e.economic_event_key == canonical
                    || e.economic_event_key
                        .strip_prefix("bill:")
                        .is_some_and(|bill_id| !bill_id.is_empty());
                if canonical_or_bill && e.component == "FEE" {
                    let reused: bool = db.query_row(
                    match scope {
                        None => "SELECT EXISTS(SELECT 1 FROM finance_entries WHERE source_ref=?1 AND component='FEE' AND economic_event_key<>?2 AND reverses_entry_id IS NULL)",
                        Some(_) => "SELECT EXISTS(SELECT 1 FROM finance_entries WHERE source_ref=?1 AND component='FEE' AND economic_event_key<>?2 AND reverses_entry_id IS NULL AND installation_id=?3 AND account_key=?4)",
                    },
                    params_from_iter(values(vec![e.source_ref.clone().into(), e.economic_event_key.clone().into()])) ,
                    |row| row.get(0),
                )?;
                    if reused {
                        return Err(StoreError::Invalid("FINANCE_FILL_SOURCE_REUSED"));
                    }
                }
                canonical_or_bill
            } else {
                false
            }
        } else if let Some(id) = e.source_ref.strip_prefix("bill:") {
            e.economic_event_key == e.source_ref
            && db.query_row(
                match scope {
                    None => "SELECT EXISTS(SELECT 1 FROM bills WHERE bill_id=?1)",
                    Some(_) => "SELECT EXISTS(SELECT 1 FROM bills WHERE bill_id=?1 AND installation_id=?2 AND account_key=?3)",
                },
                params_from_iter(values(vec![id.to_owned().into()])),
                |r| r.get::<_, bool>(0),
            )?
        } else {
            false
        };
        if !found {
            return Err(StoreError::Invalid(
                "STRATEGY_NAV_ECONOMIC_SOURCE_UNVERIFIED",
            ));
        }
        Ok(())
    }
}

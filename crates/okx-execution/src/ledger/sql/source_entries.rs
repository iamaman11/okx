//! Original financial events, reversals and exact totals in the caller transaction.
use super::super::*;
struct ReversalOriginal {
    component: String,
    allocation_leg: String,
    amount: String,
    currency: String,
    instance_id: Option<String>,
    reverses_entry_id: Option<String>,
}
impl AccountStore {
    pub(crate) fn record_finance_entries_sql(
        tx: &rusqlite::Transaction<'_>,
        account_scope: Option<(&str, &str)>,
        entries: &[FinanceEntry],
    ) -> StoreResult<Decimal> {
        use rusqlite::{params_from_iter, types::Value};
        if let Some((installation, account)) = account_scope {
            Self::verify_source_account_sql(tx, installation, account)?;
        }
        let values = |mut original: Vec<Value>| {
            if let Some((installation, account)) = account_scope {
                original.push(installation.to_owned().into());
                original.push(account.to_owned().into());
            }
            original
        };
        let mut added_loss = Decimal::ZERO;
        for e in entries {
            if e.component == "INTERNAL_CAPITAL_USD" {
                return Err(StoreError::Invalid(
                    "CAPITAL_WRITE_REQUIRES_AUTHORIZED_OPERATION",
                ));
            }
            if e.currency.is_empty() {
                return Err(StoreError::Invalid("CURRENCY_REQUIRED"));
            }
            // Proof is checked against raw fills/bills already inserted by
            // this same transaction, before a delta reaches durable totals.
            Self::verify_strategy_finance_source_sql(tx, account_scope, e)?;
            // Reversals are economic facts, not an arbitrary positive credit.
            // Validate the original in this very transaction before changing
            // any totals. A missing/out-of-order original is retryable without
            // advancing a source cursor; no fictitious profit is recognized.
            if let Some(target) = e.reverses_entry_id.as_deref() {
                // Preserve the canonical-event contract for conflicting
                // replays: changing an already recorded reversal pointer is
                // a CANONICAL_ECONOMIC_EVENT_CONFLICT, not an orphan correction.
                let canonical: Option<Option<String>> = tx
                    .query_row(
                        match account_scope { None => "SELECT reverses_entry_id FROM finance_entries WHERE economic_event_key=?1 AND component=?2 AND allocation_leg=?3", Some(_) => "SELECT reverses_entry_id FROM finance_entries WHERE economic_event_key=?1 AND component=?2 AND allocation_leg=?3 AND installation_id=?4 AND account_key=?5" },
 params_from_iter(values(vec![e.economic_event_key.clone().into(),e.component.clone().into(),e.allocation_leg.clone().into()])),
                        |r| r.get(0),
                    )
                    .optional()?;
                if canonical.is_some_and(|recorded| recorded.as_deref() != Some(target)) {
                    return Err(StoreError::Invalid("CANONICAL_ECONOMIC_EVENT_CONFLICT"));
                }
                let original: Option<ReversalOriginal> = tx
                    .query_row(
                        match account_scope { None => "SELECT component,allocation_leg,amount,currency,instance_id,reverses_entry_id FROM finance_entries WHERE entry_id=?1", Some(_) => "SELECT component,allocation_leg,amount,currency,instance_id,reverses_entry_id FROM finance_entries WHERE entry_id=?1 AND installation_id=?2 AND account_key=?3" },
 params_from_iter(values(vec![target.to_owned().into()])),
                        |r| {
                            Ok(ReversalOriginal {
                                component: r.get(0)?,
                                allocation_leg: r.get(1)?,
                                amount: r.get(2)?,
                                currency: r.get(3)?,
                                instance_id: r.get(4)?,
                                reverses_entry_id: r.get(5)?,
                            })
                        },
                    )
                    .optional()?;
                let Some(original) = original else {
                    return Err(StoreError::Invalid("FINANCE_REVERSAL_ORIGINAL_MISSING"));
                };
                if original.reverses_entry_id.is_some()
                    || original.component != e.component
                    || original.allocation_leg != e.allocation_leg
                    || original.currency != e.currency
                    || original.instance_id != e.instance_id
                    || decimal(original.amount)?.checked_add(e.amount) != Some(Decimal::ZERO)
                {
                    return Err(StoreError::Invalid("FINANCE_REVERSAL_MISMATCH"));
                }
                // Single AccountStore owner serializes writers. Prevent a
                // second economic event reversing the same original; canonical
                // source-link replays of the *same* event remain idempotent.
                let previous: Option<String> = tx
                    .query_row(
                        match account_scope { None => "SELECT economic_event_key FROM finance_entries WHERE reverses_entry_id=?1 LIMIT 1", Some(_) => "SELECT economic_event_key FROM finance_entries WHERE reverses_entry_id=?1 AND installation_id=?2 AND account_key=?3 LIMIT 1" },
 params_from_iter(values(vec![target.to_owned().into()])),
                        |r| r.get(0),
                    )
                    .optional()?;
                if previous.is_some_and(|key| key != e.economic_event_key) {
                    return Err(StoreError::Invalid("FINANCE_REVERSAL_ALREADY_APPLIED"));
                }
            }
            let added=tx.execute(match account_scope { None => "INSERT INTO finance_entries VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(economic_event_key,component,allocation_leg) DO NOTHING", Some(_) => "INSERT INTO finance_entries VALUES(?11,?12,?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(installation_id,account_key,economic_event_key,component,allocation_leg) DO NOTHING" },
 params_from_iter(values(vec![e.entry_id.clone().into(),e.economic_event_key.clone().into(),e.component.clone().into(),e.allocation_leg.clone().into(),exact(e.amount).clone().into(),e.currency.clone().into(),e.instance_id.clone().into(),e.source_ref.clone().into(),e.time.clone().into(),e.reverses_entry_id.clone().into()])))?;
            if added == 1
                && let Some(instance) = &e.instance_id
            {
                let old:Option<String>=tx.query_row(match account_scope { None => "SELECT amount FROM instance_finance_totals WHERE instance_id=?1 AND currency=?2 AND component=?3", Some(_) => "SELECT amount FROM instance_finance_totals WHERE instance_id=?1 AND currency=?2 AND component=?3 AND installation_id=?4 AND account_key=?5" },
 params_from_iter(values(vec![instance.clone().into(),e.currency.clone().into(),e.component.clone().into()])),|r|r.get(0)).optional()?;
                let total = old
                    .map(decimal)
                    .transpose()?
                    .unwrap_or(Decimal::ZERO)
                    .checked_add(e.amount)
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
                tx.execute(match account_scope { None => "INSERT INTO instance_finance_totals VALUES(?1,?2,?3,?4) ON CONFLICT(instance_id,currency,component) DO UPDATE SET amount=excluded.amount", Some(_) => "INSERT INTO instance_finance_totals VALUES(?5,?6,?1,?2,?3,?4) ON CONFLICT(installation_id,account_key,instance_id,currency,component) DO UPDATE SET amount=excluded.amount" },
 params_from_iter(values(vec![instance.clone().into(),e.currency.clone().into(),e.component.clone().into(),exact(total).clone().into()])))?;
            }
            if added == 1 {
                let prior: Option<String> = tx
                    .query_row(
                        match account_scope { None => "SELECT amount FROM finance_totals WHERE currency=?1 AND component=?2", Some(_) => "SELECT amount FROM finance_totals WHERE currency=?1 AND component=?2 AND installation_id=?3 AND account_key=?4" },
 params_from_iter(values(vec![e.currency.clone().into(),e.component.clone().into()])),
                        |r| r.get(0),
                    )
                    .optional()?;
                let total = prior
                    .map(decimal)
                    .transpose()?
                    .unwrap_or(Decimal::ZERO)
                    .checked_add(e.amount)
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
                tx.execute(match account_scope { None => "INSERT INTO finance_totals VALUES(?1,?2,?3) ON CONFLICT(currency,component) DO UPDATE SET amount=excluded.amount", Some(_) => "INSERT INTO finance_totals VALUES(?4,?5,?1,?2,?3) ON CONFLICT(installation_id,account_key,currency,component) DO UPDATE SET amount=excluded.amount" },
 params_from_iter(values(vec![e.currency.clone().into(),e.component.clone().into(),exact(total).clone().into()])))?;
            }
            let (entry_id, amount, currency, instance, time, reversal, source): (
                String,
                String,
                String,
                Option<String>,
                i64,
                Option<String>,
                String,
            ) = tx.query_row(
                match account_scope { None => "SELECT entry_id,amount,currency,instance_id,time,reverses_entry_id,source_ref FROM finance_entries WHERE economic_event_key=?1 AND component=?2 AND allocation_leg=?3", Some(_) => "SELECT entry_id,amount,currency,instance_id,time,reverses_entry_id,source_ref FROM finance_entries WHERE economic_event_key=?1 AND component=?2 AND allocation_leg=?3 AND installation_id=?4 AND account_key=?5" },
 params_from_iter(values(vec![e.economic_event_key.clone().into(),e.component.clone().into(),e.allocation_leg.clone().into()])),
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?)),
            )?;
            if amount != exact(e.amount)
                || currency != e.currency
                || instance != e.instance_id
                || reversal != e.reverses_entry_id
            {
                return Err(StoreError::Invalid("CANONICAL_ECONOMIC_EVENT_CONFLICT"));
            }
            // The original source fixes the canonical event timestamp.
            // A later fill/bill source may have a different posting time:
            // its proof links to the same economic fee, not a second charge.
            if time != e.time && source == e.source_ref {
                return Err(StoreError::Invalid("CANONICAL_ECONOMIC_EVENT_CONFLICT"));
            }
            tx.execute(
                match account_scope { None => "INSERT INTO finance_source_links VALUES(?1,?2) ON CONFLICT DO NOTHING", Some(_) => "INSERT INTO finance_source_links VALUES(?3,?4,?1,?2) ON CONFLICT DO NOTHING" },
 params_from_iter(values(vec![entry_id.clone().into(),e.source_ref.clone().into()])),
            )?;
            // Campaign accounting is conservative and never replenished by profits.
            // USD conversion must be supplied as its own attested component.
            if added == 1 && e.component == "CAMPAIGN_LOSS_USD" {
                if e.currency != "USD" || e.amount < Decimal::ZERO {
                    return Err(StoreError::Invalid("CAMPAIGN_USD_CONVERSION_REQUIRED"));
                }
                added_loss = added_loss
                    .checked_add(e.amount)
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
                let mut scopes = vec!["account".to_string()];
                if let Some(instance) = &e.instance_id {
                    scopes.push(format!("strategy:{instance}"));
                }
                for risk_scope in scopes {
                    let day = crate::risk::utc_day(e.time);
                    let prior: Option<String> = tx
                        .query_row(
                            match account_scope { None => "SELECT loss_usd FROM daily_losses WHERE scope_id=?1 AND utc_day=?2", Some(_) => "SELECT loss_usd FROM daily_losses WHERE scope_id=?1 AND utc_day=?2 AND installation_id=?3 AND account_key=?4" },
 params_from_iter(values(vec![risk_scope.clone().into(),day.clone().into()])),
                            |r| r.get(0),
                        )
                        .optional()?;
                    let total = prior
                        .map(decimal)
                        .transpose()?
                        .unwrap_or(Decimal::ZERO)
                        .checked_add(e.amount)
                        .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
                    tx.execute(match account_scope { None => "INSERT INTO daily_losses VALUES(?1,?2,?3) ON CONFLICT(scope_id,utc_day) DO UPDATE SET loss_usd=excluded.loss_usd", Some(_) => "INSERT INTO daily_losses VALUES(?4,?5,?1,?2,?3) ON CONFLICT(installation_id,account_key,scope_id,utc_day) DO UPDATE SET loss_usd=excluded.loss_usd" },
 params_from_iter(values(vec![risk_scope.clone().into(),day.clone().into(),exact(total).clone().into()])))?;
                }
            }
        }
        Ok(added_loss)
    }
}

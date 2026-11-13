//! Verifies that cached capacities match exact, durable primary obligations.
//! Called in a SQLite transaction before readback or authorization; no independent writer.
use super::{BookMeta, Result, RiskAmount, RiskStoreError, amount};
use std::collections::BTreeMap;

// The risk_entries rows carry the primary remaining obligations. Cached totals
// never authorize a grant unless they agree with those exact-decimal records.
// This version only implements HELD; other nonterminal states fail closed
// until their transition and remaining-balance semantics are implemented.
// RELEASED rows are protected by SQLite CHECK and excluded from this scan.
pub(super) fn verify_reservation_aggregates(
    tx: &rusqlite::Transaction<'_>,
    meta: &BookMeta,
) -> Result<()> {
    let mut accounts: BTreeMap<String, RiskAmount> = BTreeMap::new();
    let mut strategies: BTreeMap<(String, String), RiskAmount> = BTreeMap::new();
    let mut global = RiskAmount::zero();
    let mut loss = RiskAmount::zero();
    let mut entry = RiskAmount::zero();

    {
        let mut stmt = tx.prepare(
            "SELECT account_key,strategy_id,state,loss_usdt,loss_usd,entry_upper_usd,
                    reserved_usdt,reserved_loss_usd,reserved_entry_usd
             FROM risk_entries WHERE state <> 'RELEASED'",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
            ))
        })?;
        for row in rows {
            let (
                account,
                strategy,
                state,
                original_usdt,
                original_loss,
                original_entry,
                remaining_usdt,
                remaining_loss,
                remaining_entry,
            ) = row?;
            let remaining_usdt = amount(remaining_usdt)?;
            let remaining_loss = amount(remaining_loss)?;
            let remaining_entry = amount(remaining_entry)?;
            if state != "HELD" {
                return Err(RiskStoreError::Corrupt("RESERVATION_STATE_UNIMPLEMENTED"));
            }
            if remaining_usdt != amount(original_usdt)?
                || remaining_loss != amount(original_loss)?
                || remaining_entry != amount(original_entry)?
            {
                return Err(RiskStoreError::Corrupt("RESERVATION_AGGREGATES"));
            }
            global = global.checked_add(&remaining_usdt)?;
            loss = loss.checked_add(&remaining_loss)?;
            entry = entry.checked_add(&remaining_entry)?;
            let previous = accounts
                .entry(account.clone())
                .or_insert_with(RiskAmount::zero);
            *previous = previous.checked_add(&remaining_usdt)?;
            let previous = strategies
                .entry((account, strategy))
                .or_insert_with(RiskAmount::zero);
            *previous = previous.checked_add(&remaining_usdt)?;
        }
    }
    if global != meta.global_reserved || loss != meta.loss_reserved || entry != meta.entry_reserved
    {
        return Err(RiskStoreError::Corrupt("RESERVATION_AGGREGATES"));
    }
    {
        let mut stmt = tx.prepare("SELECT account_key,reserved_usdt FROM risk_account_occupied")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        for row in rows {
            let (account, stored) = row?;
            let expected = accounts.remove(&account).unwrap_or_else(RiskAmount::zero);
            if amount(stored)? != expected {
                return Err(RiskStoreError::Corrupt("RESERVATION_AGGREGATES"));
            }
        }
    }
    if !accounts.is_empty() {
        return Err(RiskStoreError::Corrupt("RESERVATION_AGGREGATES"));
    }
    {
        let mut stmt =
            tx.prepare("SELECT account_key,strategy_id,reserved_usdt FROM risk_strategy_occupied")?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?;
        for row in rows {
            let (account, strategy, stored) = row?;
            let expected = strategies
                .remove(&(account, strategy))
                .unwrap_or_else(RiskAmount::zero);
            if amount(stored)? != expected {
                return Err(RiskStoreError::Corrupt("RESERVATION_AGGREGATES"));
            }
        }
    }
    if !strategies.is_empty() {
        return Err(RiskStoreError::Corrupt("RESERVATION_AGGREGATES"));
    }
    // Loss high-water marks are the primary per-account facts in this v2 book.
    // Their complete sum must equal the cached campaign consumed-loss total.
    let mut cumulative = RiskAmount::zero();
    let mut policy_count = 0_u64;
    {
        let mut stmt = tx.prepare(
            "SELECT p.account_key, l.cumulative_loss_usd
             FROM risk_policy_account p
             LEFT JOIN risk_loss_source l USING(account_key)",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
        })?;
        for row in rows {
            let (_, observed) = row?;
            let observed = observed.ok_or(RiskStoreError::Corrupt("LOSS_SOURCE_AGGREGATES"))?;
            cumulative = cumulative.checked_add(&amount(observed)?)?;
            policy_count += 1;
        }
    }
    let source_count: i64 =
        tx.query_row("SELECT COUNT(*) FROM risk_loss_source", [], |r| r.get(0))?;
    let source_count = u64::try_from(source_count)
        .map_err(|_| RiskStoreError::Corrupt("LOSS_SOURCE_AGGREGATES"))?;
    if cumulative != meta.loss_consumed || source_count != policy_count {
        return Err(RiskStoreError::Corrupt("LOSS_SOURCE_AGGREGATES"));
    }
    Ok(())
}

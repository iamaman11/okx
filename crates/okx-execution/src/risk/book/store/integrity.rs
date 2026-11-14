//! Verifies that cached capacities match exact, durable primary obligations.
//! Called in a SQLite transaction before readback or authorization; no independent writer.
use super::{BookMeta, Result, RiskAmount, RiskStoreError, amount};
use std::collections::BTreeMap;
use rusqlite::params_from_iter;

// The risk_entries rows carry the primary remaining obligations. Cached totals
// never authorize a grant unless they agree with those exact-decimal records.
// HELD and POSTED both retain their original obligations. A committed
// POSTED record includes immutable causal evidence and never mints another
// grant. UNKNOWN/OPEN remain fail-closed until their balance rules exist.
// RELEASED rows are protected by SQLite CHECK and excluded from this scan.
pub(super) fn verify_reservation_aggregates(
    tx: &rusqlite::Transaction<'_>,
    meta: &BookMeta,
) -> Result<()> {
    verify_reservation_aggregates_for_scope(tx, meta, None)
}

/// One original RiskBook mathematical owner for both legacy single-campaign
/// v5 SQL and the scoped v6 multi-campaign finance.sqlite.
pub(super) fn verify_reservation_aggregates_for_scope(
    tx: &rusqlite::Transaction<'_>,
    meta: &BookMeta,
    scope: Option<(&str, &str)>,
) -> Result<()> {
    let scoped_args = scope.map(|(installation, campaign)| vec![installation, campaign])
        .unwrap_or_default();
    let mut accounts: BTreeMap<String, RiskAmount> = BTreeMap::new();
    let mut strategies: BTreeMap<(String, String), RiskAmount> = BTreeMap::new();
    let mut global = RiskAmount::zero();
    let mut loss = RiskAmount::zero();
    let mut entry = RiskAmount::zero();

    {
        let query = if scope.is_some() {
            "SELECT account_key,strategy_id,state,loss_usdt,loss_usd,entry_upper_usd,
                    reserved_usdt,reserved_loss_usd,reserved_entry_usd,
                    book_sequence,post_sequence,post_proof_digest,post_source_digest,
                    post_authorized_at_ms,held_at_ms,post_existing_occupied_usdt,
                    entry_settled_usd,entry_settlement_revision,entry_settlement_digest,
                    entry_bound_breach_usd
             FROM risk_entries
             WHERE installation_id=?1 AND campaign_id=?2 AND state <> 'RELEASED'"
        } else {
            "SELECT account_key,strategy_id,state,loss_usdt,loss_usd,entry_upper_usd,
                    reserved_usdt,reserved_loss_usd,reserved_entry_usd,
                    book_sequence,post_sequence,post_proof_digest,post_source_digest,
                    post_authorized_at_ms,held_at_ms,post_existing_occupied_usdt,
                    entry_settled_usd,entry_settlement_revision,entry_settlement_digest,
                    entry_bound_breach_usd
             FROM risk_entries WHERE state <> 'RELEASED'"
        };
        let mut stmt = tx.prepare(query)?;
        let rows = stmt.query_map(params_from_iter(scoped_args.iter()), |row| {
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
                row.get::<_, i64>(9)?,
                row.get::<_, i64>(10)?,
                row.get::<_, String>(11)?,
                row.get::<_, String>(12)?,
                row.get::<_, i64>(13)?,
                row.get::<_, i64>(14)?,
                row.get::<_, String>(15)?,
                row.get::<_, String>(16)?,
                row.get::<_, i64>(17)?,
                row.get::<_, String>(18)?,
                row.get::<_, String>(19)?,
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
                original_sequence,
                post_sequence,
                post_proof_digest,
                post_source_digest,
                post_time,
                held_time,
                post_existing,
                cumulative_entry,
                settlement_revision,
                settlement_digest,
                entry_breach,
            ) = row?;
            let remaining_usdt = amount(remaining_usdt)?;
            let remaining_loss = amount(remaining_loss)?;
            let remaining_entry = amount(remaining_entry)?;
            // A durable POSTED observation must survive OS kill and reopen
            // without losing any originally charged reserve. An incomplete
            // transition is corrupt, never an alternative route to POST.
            match state.as_str() {
                "HELD"
                    if post_sequence == 0
                        && post_time == 0
                        && post_proof_digest.is_empty()
                        && post_source_digest.is_empty()
                        && post_existing == "0" => {}
                "POSTED"
                    if original_sequence > 0
                        && post_sequence > original_sequence
                        && u64::try_from(post_sequence)
                            .ok()
                            .is_some_and(|v| v <= meta.sequence)
                        && post_time >= held_time
                        && super::valid_digest(&post_proof_digest)
                        && super::valid_digest(&post_source_digest)
                        && amount(post_existing).is_ok() => {}
                "HELD" | "POSTED" => {
                    return Err(RiskStoreError::Corrupt("POST_FENCE_INTEGRITY"));
                }
                _ => return Err(RiskStoreError::Corrupt("RESERVATION_STATE_UNIMPLEMENTED")),
            }
            let settled = amount(cumulative_entry)?;
            let ceiling = amount(original_entry)?;
            let expected_entry = if settled < ceiling {
                ceiling.checked_sub(&settled)?
            } else {
                RiskAmount::zero()
            };
            let expected_breach = if settled > ceiling {
                settled.checked_sub(&ceiling)?
            } else {
                RiskAmount::zero()
            };
            let settlement_consistent = if settlement_revision == 0 {
                settled.is_zero() && settlement_digest.is_empty()
            } else {
                settlement_revision > 0
                    && super::valid_digest(&settlement_digest)
                    && !settled.is_zero()
                    && state == "POSTED"
            };
            if !settlement_consistent
                || amount(entry_breach)? != expected_breach
                || remaining_usdt != amount(original_usdt)?
                || remaining_loss != amount(original_loss)?
                || remaining_entry != expected_entry
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
        let query = if scope.is_some() {
            "SELECT account_key,reserved_usdt FROM risk_account_occupied
             WHERE installation_id=?1 AND campaign_id=?2"
        } else {
            "SELECT account_key,reserved_usdt FROM risk_account_occupied"
        };
        let mut stmt = tx.prepare(query)?;
        let rows = stmt.query_map(params_from_iter(scoped_args.iter()), |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?;
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
        let query = if scope.is_some() {
            "SELECT account_key,strategy_id,reserved_usdt FROM risk_strategy_occupied
             WHERE installation_id=?1 AND campaign_id=?2"
        } else {
            "SELECT account_key,strategy_id,reserved_usdt FROM risk_strategy_occupied"
        };
        let mut stmt = tx.prepare(query)?;
        let rows = stmt.query_map(params_from_iter(scoped_args.iter()), |r| {
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
    // A negative SQL fence and an actual reservation for the same address
    // must never coexist, even if an out-of-band writer damaged the DB.
    let contradictory_query = if scope.is_some() {
        "SELECT COUNT(*) FROM risk_no_reserve f
         JOIN risk_entries e
           ON e.installation_id=f.installation_id AND e.campaign_id=f.campaign_id
          AND e.reservation_key=f.reservation_key
         WHERE f.installation_id=?1 AND f.campaign_id=?2"
    } else {
        "SELECT COUNT(*) FROM risk_no_reserve f
         JOIN risk_entries e USING(reservation_key)"
    };
    let contradictory: i64 = tx.query_row(
        contradictory_query,
        params_from_iter(scoped_args.iter()),
        |r| r.get(0),
    )?;
    if contradictory != 0 {
        return Err(RiskStoreError::Corrupt("NO_RESERVE_FENCE_CONFLICT"));
    }
    // Original SQL settlement revisions are the primary consumed-entry facts.
    // The opening amount is explicitly separate: the cache may not be
    // increased or reduced without a corresponding durable native receipt.
    let mut consumed_from_receipts = meta.entry_opening_consumed.clone();
    {
        let query = if scope.is_some() {
            "SELECT entry_settled_usd FROM risk_entries
             WHERE installation_id=?1 AND campaign_id=?2
               AND entry_settlement_revision > 0"
        } else {
            "SELECT entry_settled_usd FROM risk_entries
             WHERE entry_settlement_revision > 0"
        };
        let mut stmt = tx.prepare(query)?;
        let rows = stmt.query_map(params_from_iter(scoped_args.iter()), |r| r.get::<_, String>(0))?;
        for row in rows {
            consumed_from_receipts = consumed_from_receipts.checked_add(&amount(row?)?)?;
        }
    }
    if consumed_from_receipts != meta.entry_consumed {
        return Err(RiskStoreError::Corrupt("ENTRY_CONSUMED_AGGREGATES"));
    }
    // Loss high-water marks are the primary per-account facts in this v5 book.
    // Their complete sum must equal the cached campaign consumed-loss total.
    let mut cumulative = RiskAmount::zero();
    let mut policy_count = 0_u64;
    {
        let query = if scope.is_some() {
            "SELECT p.account_key, l.cumulative_loss_usd
             FROM risk_policy_account p
             LEFT JOIN risk_loss_source l
               ON l.installation_id=p.installation_id AND l.campaign_id=p.campaign_id
              AND l.account_key=p.account_key
             WHERE p.installation_id=?1 AND p.campaign_id=?2"
        } else {
            "SELECT p.account_key, l.cumulative_loss_usd
             FROM risk_policy_account p
             LEFT JOIN risk_loss_source l USING(account_key)"
        };
        let mut stmt = tx.prepare(query)?;
        let rows = stmt.query_map(params_from_iter(scoped_args.iter()), |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
        })?;
        for row in rows {
            let (_, observed) = row?;
            let observed = observed.ok_or(RiskStoreError::Corrupt("LOSS_SOURCE_AGGREGATES"))?;
            cumulative = cumulative.checked_add(&amount(observed)?)?;
            policy_count += 1;
        }
    }
    let source_query = if scope.is_some() {
        "SELECT COUNT(*) FROM risk_loss_source WHERE installation_id=?1 AND campaign_id=?2"
    } else {
        "SELECT COUNT(*) FROM risk_loss_source"
    };
    let source_count: i64 = tx.query_row(
        source_query,
        params_from_iter(scoped_args.iter()),
        |r| r.get(0),
    )?;
    let source_count = u64::try_from(source_count)
        .map_err(|_| RiskStoreError::Corrupt("LOSS_SOURCE_AGGREGATES"))?;
    if cumulative != meta.loss_consumed || source_count != policy_count {
        return Err(RiskStoreError::Corrupt("LOSS_SOURCE_AGGREGATES"));
    }
    Ok(())
}

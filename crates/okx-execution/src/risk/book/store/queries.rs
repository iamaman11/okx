//! Read-only RiskBook observations within the existing SQLite owner.
//! No absent row means "safe to send": no-reserve fencing is a separate
//! durable operation, not a conclusion that an INSPECT caller can invent.
use super::{BookMeta, Result, RiskStoreError, amount, reserve_scope_values, valid_digest};
use okx_kernel::risk::{model::RiskEntryState, money::RiskAmount};
use rusqlite::{OptionalExtension, Transaction, params_from_iter, types::Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AccountReadback {
    pub account_key: String,
    pub policy_revision: u64,
    pub book_sequence: u64,
    pub active_reservations: u64,
    pub reserved_loss_usdt: String,
    pub reserved_loss_usd: String,
    pub cumulative_loss_usd: String,
    pub latest_activity_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReservationReadback {
    pub reservation_key: String,
    pub plan_sha256: String,
    pub state: RiskEntryState,
    pub reservation_sequence: u64,
    pub current_book_sequence: u64,
    pub policy_revision: u64,
}

pub(super) fn account_readback(
    tx: &Transaction<'_>,
    meta: &BookMeta,
    account_key: &str,
    scope: Option<(&str, &str)>,
) -> Result<AccountReadback> {
    if !account_key
        .strip_prefix("okx:real:")
        .is_some_and(valid_digest)
    {
        return Err(RiskStoreError::Invalid("ACCOUNT_IDENTITY"));
    }
    // The same original RiskBook exact-money projection now accepts the
    // installation/campaign namespace of v6, never joining another
    // campaign's policy, occupation or loss by account_key alone.
    let values = reserve_scope_values(vec![Value::Text(account_key.to_owned())], scope);
    let policy_sql = if scope.is_some() {
        "SELECT 1 FROM risk_policy_account WHERE account_key=?1
         AND installation_id=?2 AND campaign_id=?3"
    } else {
        "SELECT 1 FROM risk_policy_account WHERE account_key=?1"
    };
    let registered = tx
        .query_row(policy_sql, params_from_iter(values.iter()), |row| {
            row.get::<_, i64>(0)
        })
        .optional()?
        .is_some();
    if !registered {
        return Err(RiskStoreError::Invalid("ACCOUNT_NOT_ACTIVE"));
    }
    let aggregate_sql = if scope.is_some() {
        "SELECT o.reserved_usdt,l.cumulative_loss_usd
         FROM risk_policy_account p
         JOIN risk_account_occupied o
           ON o.account_key=p.account_key
          AND o.installation_id=p.installation_id
          AND o.campaign_id=p.campaign_id
         JOIN risk_loss_source l
           ON l.account_key=p.account_key
          AND l.installation_id=p.installation_id
          AND l.campaign_id=p.campaign_id
         WHERE p.account_key=?1
           AND p.installation_id=?2 AND p.campaign_id=?3"
    } else {
        "SELECT o.reserved_usdt,l.cumulative_loss_usd
         FROM risk_policy_account p
         JOIN risk_account_occupied o USING(account_key)
         JOIN risk_loss_source l USING(account_key)
         WHERE p.account_key=?1"
    };
    let (reserved, cumulative): (String, String) = tx
        .query_row(aggregate_sql, params_from_iter(values.iter()), |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()?
        .ok_or(RiskStoreError::Corrupt("MISSING_ACCOUNT_AGGREGATE"))?;

    let mut count = 0_u64;
    let mut usd = RiskAmount::zero();
    {
        let entries_sql = if scope.is_some() {
            "SELECT loss_usd FROM risk_entries WHERE account_key=?1
             AND installation_id=?2 AND campaign_id=?3 AND state <> 'RELEASED'"
        } else {
            "SELECT loss_usd FROM risk_entries
             WHERE account_key=?1 AND state <> 'RELEASED'"
        };
        let mut stmt = tx.prepare(entries_sql)?;
        let rows = stmt.query_map(params_from_iter(values.iter()), |r| r.get::<_, String>(0))?;
        for row in rows {
            usd = usd.checked_add(&amount(row?)?)?;
            count = count
                .checked_add(1)
                .ok_or(RiskStoreError::Corrupt("ACTIVE_RESERVATION_COUNT"))?;
        }
    }
    // Original RiskBook receipt time, NOT process clock or a fabricated
    // freshness indicator. Active reservations require a durable positive
    // held_at_ms to satisfy the native account-budget wire contract.
    let activity_sql = if scope.is_some() {
        "SELECT COALESCE(MAX(held_at_ms),0) FROM risk_entries
         WHERE account_key=?1 AND installation_id=?2 AND campaign_id=?3"
    } else {
        "SELECT COALESCE(MAX(held_at_ms),0) FROM risk_entries WHERE account_key=?1"
    };
    let activity: i64 = tx.query_row(
        activity_sql,
        params_from_iter(values.iter()),
        |row| row.get(0),
    )?;
    let latest_activity_ms = u64::try_from(activity)
        .map_err(|_| RiskStoreError::Corrupt("RISK_ACTIVITY_TIME_INVALID"))?;
    if count > 0 && latest_activity_ms == 0 {
        return Err(RiskStoreError::Corrupt("RISK_ACTIVE_ACTIVITY_MISSING"));
    }
    Ok(AccountReadback {
        account_key: account_key.to_owned(),
        policy_revision: meta.policy_revision,
        book_sequence: meta.sequence,
        active_reservations: count,
        reserved_loss_usdt: amount(reserved)?.canonical()?,
        reserved_loss_usd: usd.canonical()?,
        cumulative_loss_usd: amount(cumulative)?.canonical()?,
        latest_activity_ms,
    })
}

pub(super) fn inspect(
    tx: &Transaction<'_>,
    meta: &BookMeta,
    reservation_key: &str,
    plan_sha256: &str,
    scope: Option<(&str, &str)>,
) -> Result<Option<ReservationReadback>> {
    if reservation_key.is_empty() || reservation_key.len() > 512 || !valid_digest(plan_sha256) {
        return Err(RiskStoreError::Invalid("INSPECTION_IDENTITY"));
    }
    let values = reserve_scope_values(vec![Value::Text(reservation_key.to_owned())], scope);
    let entries_sql = if scope.is_some() {
        "SELECT plan_sha256,state,book_sequence,policy_revision
         FROM risk_entries WHERE reservation_key=?1
           AND installation_id=?2 AND campaign_id=?3"
    } else {
        "SELECT plan_sha256,state,book_sequence,policy_revision
         FROM risk_entries WHERE reservation_key=?1"
    };
    let row: Option<(String, String, i64, i64)> = tx
        .query_row(entries_sql, params_from_iter(values.iter()), |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        })
        .optional()?;
    let Some((stored_plan, state, entry_seq, revision)) = row else {
        // A committed negative fence is an explicit terminal observation.
        // Without either durable row, absence is UNKNOWN, not no-send proof.
        let fence_sql = if scope.is_some() {
            "SELECT plan_sha256,book_sequence,policy_revision
             FROM risk_no_reserve WHERE reservation_key=?1
               AND installation_id=?2 AND campaign_id=?3"
        } else {
            "SELECT plan_sha256,book_sequence,policy_revision
             FROM risk_no_reserve WHERE reservation_key=?1"
        };
        let fence: Option<(String, i64, i64)> = tx
            .query_row(fence_sql, params_from_iter(values.iter()), |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .optional()?;
        let Some((stored_plan, sequence, revision)) = fence else {
            return Ok(None);
        };
        if stored_plan != plan_sha256 {
            return Ok(None);
        }
        let reservation_sequence =
            u64::try_from(sequence).map_err(|_| RiskStoreError::Corrupt("NO_RESERVE_SEQUENCE"))?;
        let policy_revision =
            u64::try_from(revision).map_err(|_| RiskStoreError::Corrupt("NO_RESERVE_POLICY"))?;
        if reservation_sequence == 0
            || reservation_sequence > meta.sequence
            || policy_revision == 0
            || policy_revision > meta.policy_revision
        {
            return Err(RiskStoreError::Corrupt("NO_RESERVE_REVISION"));
        }
        return Ok(Some(ReservationReadback {
            reservation_key: reservation_key.to_owned(),
            plan_sha256: stored_plan,
            state: RiskEntryState::Released,
            reservation_sequence,
            current_book_sequence: meta.sequence,
            policy_revision,
        }));
    };
    // Match the exact original immutable plan, never disclose a different one.
    if stored_plan != plan_sha256 {
        return Ok(None);
    }
    let state = match state.as_str() {
        "HELD" => RiskEntryState::Held,
        "POSTED" => RiskEntryState::Posted,
        "UNKNOWN" => RiskEntryState::Unknown,
        "OPEN" => RiskEntryState::Open,
        "RELEASED" => RiskEntryState::Released,
        _ => return Err(RiskStoreError::Corrupt("RESERVATION_STATE")),
    };
    let reservation_sequence =
        u64::try_from(entry_seq).map_err(|_| RiskStoreError::Corrupt("RESERVATION_SEQUENCE"))?;
    let policy_revision =
        u64::try_from(revision).map_err(|_| RiskStoreError::Corrupt("RESERVATION_POLICY"))?;
    if reservation_sequence == 0
        || reservation_sequence > meta.sequence
        || policy_revision == 0
        || policy_revision > meta.policy_revision
    {
        return Err(RiskStoreError::Corrupt("RESERVATION_REVISION"));
    }
    Ok(Some(ReservationReadback {
        reservation_key: reservation_key.to_owned(),
        plan_sha256: stored_plan,
        state,
        reservation_sequence,
        current_book_sequence: meta.sequence,
        policy_revision,
    }))
}

//! Read-only RiskBook observations within the existing SQLite owner.
//! No absent row means "safe to send": no-reserve fencing is a separate
//! durable operation, not a conclusion that an INSPECT caller can invent.
use super::{BookMeta, Result, RiskStoreError, amount, valid_digest};
use okx_kernel::risk::{model::RiskEntryState, money::RiskAmount};
use rusqlite::{OptionalExtension, Transaction, params};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AccountReadback {
    pub account_key: String,
    pub policy_revision: u64,
    pub book_sequence: u64,
    pub active_reservations: u64,
    pub reserved_loss_usdt: String,
    pub reserved_loss_usd: String,
    pub cumulative_loss_usd: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ReservationReadback {
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
) -> Result<AccountReadback> {
    if !account_key
        .strip_prefix("okx:real:")
        .is_some_and(valid_digest)
    {
        return Err(RiskStoreError::Invalid("ACCOUNT_IDENTITY"));
    }
    let registered = tx
        .query_row(
            "SELECT 1 FROM risk_policy_account WHERE account_key=?1",
            params![account_key],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .is_some();
    if !registered {
        return Err(RiskStoreError::Invalid("ACCOUNT_NOT_ACTIVE"));
    }
    let (reserved, cumulative): (String, String) = tx
        .query_row(
            "SELECT o.reserved_usdt,l.cumulative_loss_usd
             FROM risk_policy_account p
             JOIN risk_account_occupied o USING(account_key)
             JOIN risk_loss_source l USING(account_key)
             WHERE p.account_key=?1",
            params![account_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .ok_or(RiskStoreError::Corrupt("MISSING_ACCOUNT_AGGREGATE"))?;

    let mut count = 0_u64;
    let mut usd = RiskAmount::zero();
    {
        let mut stmt = tx.prepare(
            "SELECT loss_usd FROM risk_entries
             WHERE account_key=?1 AND state <> 'RELEASED'",
        )?;
        let rows = stmt.query_map(params![account_key], |r| r.get::<_, String>(0))?;
        for row in rows {
            usd = usd.checked_add(&amount(row?)?)?;
            count = count
                .checked_add(1)
                .ok_or(RiskStoreError::Corrupt("ACTIVE_RESERVATION_COUNT"))?;
        }
    }
    Ok(AccountReadback {
        account_key: account_key.to_owned(),
        policy_revision: meta.policy_revision,
        book_sequence: meta.sequence,
        active_reservations: count,
        reserved_loss_usdt: amount(reserved)?.canonical()?,
        reserved_loss_usd: usd.canonical()?,
        cumulative_loss_usd: amount(cumulative)?.canonical()?,
    })
}

pub(super) fn inspect(
    tx: &Transaction<'_>,
    meta: &BookMeta,
    reservation_key: &str,
    plan_sha256: &str,
) -> Result<Option<ReservationReadback>> {
    if reservation_key.is_empty()
        || reservation_key.len() > 512
        || !valid_digest(plan_sha256)
    {
        return Err(RiskStoreError::Invalid("INSPECTION_IDENTITY"));
    }
    let row: Option<(String, String, i64, i64)> = tx
        .query_row(
            "SELECT plan_sha256,state,book_sequence,policy_revision
             FROM risk_entries WHERE reservation_key=?1",
            params![reservation_key],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;
    let Some((stored_plan, state, entry_seq, revision)) = row else {
        return Ok(None);
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
    let reservation_sequence = u64::try_from(entry_seq)
        .map_err(|_| RiskStoreError::Corrupt("RESERVATION_SEQUENCE"))?;
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

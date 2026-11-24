//! Campaign entry-opening installation on the SAME original Rust SQL owner.
//! The protocol source is only an address: every account's immutable money,
//! capture readiness and source digest are independently read from AccountStore.
//! No caller-selected zero, second budget owner or alternate campaign schema.
use super::{
    LEGACY_MAX_SAFE_INTEGER, Result, RiskBookStore, RiskStoreError, VerifiedCensus, amount,
    integrity, read_meta_for_scope, valid_digest,
};
use okx_domain_core::risk::money::RiskAmount;
/// Pure typed native source passed by the authenticated agent adapter.
/// The external protocol DTO is converted at the agent boundary; execution
/// never depends on protocol, WS, Cloudflare or the exchange transport.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NativeOpeningAccount {
    pub account_key: String,
    pub history_from_ms: u64,
    pub history_through_ms: u64,
    pub consumed_entry_usd: String,
    pub order_history_complete: bool,
    pub fill_history_complete: bool,
    pub bill_history_complete: bool,
    pub finance_provenance_sealed: bool,
    pub source_digest: String,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NativeOpeningSource {
    pub campaign_id: String,
    pub limit_usd: String,
    pub consumed_usd: String,
    pub source_digest: String,
    pub as_of_ms: u64,
    pub campaign_started_at_ms: u64,
    pub accounts: Vec<NativeOpeningAccount>,
}
impl NativeOpeningSource {
    pub(in crate::risk::book) fn canonical_digest(&self) -> Result<String> {
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(
                "okx.campaign-entry-opening/v1",
                &self.campaign_id,
                &self.limit_usd,
                &self.consumed_usd,
                self.campaign_started_at_ms,
                self.as_of_ms,
                &self.accounts,
            ))?)
        ))
    }

    pub(crate) fn valid(&self) -> bool {
        self.campaign_id
            .strip_prefix("sha256:")
            .is_some_and(valid_digest)
            && valid_digest(&self.source_digest)
            && RiskAmount::parse_positive(&self.limit_usd).is_ok()
            && RiskAmount::parse(&self.consumed_usd).is_ok()
            && self.campaign_started_at_ms > 0
            && self.as_of_ms > self.campaign_started_at_ms
            && !self.accounts.is_empty()
            && self.accounts.len() <= 32
            && self
                .accounts
                .windows(2)
                .all(|pair| pair[0].account_key < pair[1].account_key)
            && self.accounts.iter().all(|row| {
                row.account_key
                    .strip_prefix("okx:real:")
                    .is_some_and(valid_digest)
                    && row.history_from_ms == self.campaign_started_at_ms
                    && row.history_through_ms == self.as_of_ms
                    && RiskAmount::parse(&row.consumed_entry_usd).is_ok()
                    && row.order_history_complete
                    && row.fill_history_complete
                    && row.bill_history_complete
                    && row.finance_provenance_sealed
                    && valid_digest(&row.source_digest)
            })
    }
}

use rusqlite::{OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EntryOpeningReceipt {
    pub source_digest: String,
    pub book_sequence: u64,
    pub replayed: bool,
}
fn source_error(error: crate::StoreError) -> RiskStoreError {
    match error {
        crate::StoreError::Invalid(reason) => RiskStoreError::Invalid(reason),
        crate::StoreError::Sql(error) => RiskStoreError::Sql(error),
        crate::StoreError::Io(error) => RiskStoreError::Io(error),
    }
}
impl RiskBookStore {
    /// The original AccountStore per-account financial opening and original
    /// RiskBook campaign aggregate share ONE transaction. A full offline
    /// imported campaign with existing economic obligations is never reset.
    pub(in crate::risk::book) fn install_unified_entry_opening_from_account_sql_transaction(
        tx: &Transaction<'_>,
        installation: &str,
        source: &NativeOpeningSource,
        census: &VerifiedCensus,
        now_ms: i64,
    ) -> Result<EntryOpeningReceipt> {
        if installation.is_empty()
            || !source.valid()
            || now_ms <= 0
            || now_ms as u64 > LEGACY_MAX_SAFE_INTEGER
            || source.campaign_started_at_ms == 0
            || source.as_of_ms <= source.campaign_started_at_ms
            || source.as_of_ms > now_ms as u64
            || now_ms
                - i64::try_from(source.as_of_ms)
                    .map_err(|_| RiskStoreError::Invalid("OPENING_TIME_RANGE"))?
                > 5_000
        {
            return Err(RiskStoreError::Invalid("UNIFIED_OPENING_IDENTITY_INVALID"));
        }
        let expected = source.canonical_digest()?;
        if source.source_digest != expected {
            return Err(RiskStoreError::Invalid(
                "UNIFIED_OPENING_SOURCE_DIGEST_CHANGED",
            ));
        }
        let scope = Some((installation, source.campaign_id.as_str()));
        let mut meta = read_meta_for_scope(tx, scope)?;
        integrity::verify_reservation_aggregates_for_scope(tx, &meta, scope)?;
        if !census.complete
            || census.generation != meta.generation
            || census.generated_at_ms <= 0
            || census.read_floor_ms <= 0
            || census.read_floor_ms > census.generated_at_ms
            || census.generated_at_ms > now_ms
            || now_ms - census.generated_at_ms > 5_000
            || census.read_floor_ms
                < i64::try_from(source.as_of_ms).map_err(|_| RiskStoreError::CensusUnproven)?
            || !valid_digest(&census.source_digest)
        {
            return Err(RiskStoreError::CensusUnproven);
        }
        let limit = RiskAmount::parse_positive(&source.limit_usd)?;
        if limit != meta.entry_limit {
            return Err(RiskStoreError::PolicyConflict);
        }
        let accounts = {
            let mut q = tx.prepare(
                "SELECT account_key FROM risk_policy_account
                 WHERE installation_id=?1 AND campaign_id=?2 ORDER BY account_key",
            )?;
            q.query_map(params![installation, &source.campaign_id], |r| {
                r.get::<_, String>(0)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
        };
        if accounts.is_empty()
            || accounts.len() > 32
            || accounts.len() != source.accounts.len()
            || accounts
                .iter()
                .ne(source.accounts.iter().map(|row| &row.account_key))
            || accounts.iter().ne(census.occupied.keys())
            || accounts.iter().ne(census.cumulative_loss_usd.keys())
            || census.occupied.values().any(|occupied| !occupied.is_zero())
        {
            return Err(RiskStoreError::CensusUnproven);
        }
        let marker = format!("risk-entry-opening-installed:{}", source.campaign_id);
        let already: Option<(String, String, String, i64)> = tx
            .query_row(
                "SELECT kind,request_hash,result_ref,committed_at
             FROM operation_receipts WHERE installation_id=?1 AND account_key=?2
                AND operation_id=?3",
                params![installation, &accounts[0], &marker],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        if let Some((kind, hash, money, at)) = already {
            if kind != "CAMPAIGN_OPENING_INSTALLED"
                || hash != source.source_digest
                || at
                    != i64::try_from(source.as_of_ms).map_err(|_| RiskStoreError::StaleSequence)?
                || amount(money)? != amount(source.consumed_usd.clone())?
                || meta.entry_opening_consumed != amount(source.consumed_usd.clone())?
            {
                return Err(RiskStoreError::IdempotencyConflict);
            }
            return Ok(EntryOpeningReceipt {
                source_digest: source.source_digest.clone(),
                book_sequence: meta.sequence,
                replayed: true,
            });
        }
        // An imported historical campaign with nonzero entry obligations
        // must be reconciled by RB09, not overwritten by this new opening.
        if meta.sequence != 0
            || !meta.entry_consumed.is_zero()
            || !meta.entry_opening_consumed.is_zero()
            || !meta.global_reserved.is_zero()
            || !meta.loss_reserved.is_zero()
            || !meta.entry_reserved.is_zero()
        {
            return Err(RiskStoreError::Invalid(
                "OPENING_EXISTING_FINANCE_OBLIGATIONS",
            ));
        }
        let mut sum = RiskAmount::zero();
        for row in &source.accounts {
            if row.history_from_ms != source.campaign_started_at_ms
                || row.history_through_ms != source.as_of_ms
                || !row.order_history_complete
                || !row.fill_history_complete
                || !row.bill_history_complete
                || !row.finance_provenance_sealed
            {
                return Err(RiskStoreError::EntrySqlUnproven);
            }
            let witnessed = RiskAmount::parse(&row.consumed_entry_usd)?;
            let decimal = row
                .consumed_entry_usd
                .parse::<rust_decimal::Decimal>()
                .map_err(|_| RiskStoreError::Invalid("OPENING_ACCOUNT_MONEY_INVALID"))?;
            crate::AccountStore::verify_unified_campaign_entry_source_sql(
                tx,
                installation,
                &row.account_key,
                &source.campaign_id,
                i64::try_from(row.history_from_ms).map_err(|_| RiskStoreError::EntrySqlUnproven)?,
                i64::try_from(row.history_through_ms)
                    .map_err(|_| RiskStoreError::EntrySqlUnproven)?,
                decimal,
                &row.source_digest,
            )
            .map_err(source_error)?;
            sum = sum.checked_add(&witnessed)?;
        }
        if sum != amount(source.consumed_usd.clone())? || sum > meta.entry_limit {
            return Err(RiskStoreError::Invalid("OPENING_BUDGET_SOURCE_MISMATCH"));
        }
        // The original full-cohort loss high-water remains monotonic; do not
        // invent a separate loss ledger or silently forgive historical loss.
        super::apply_census_loss_for_scope(tx, &mut meta, census, scope)?;
        let next = meta
            .sequence
            .checked_add(1)
            .filter(|value| *value <= LEGACY_MAX_SAFE_INTEGER)
            .ok_or(RiskStoreError::Corrupt("SEQUENCE_OVERFLOW"))?;
        let changed = tx.execute(
            "UPDATE risk_meta SET entry_opening_consumed_usd=?1,
                entry_consumed_usd=?1,book_sequence=?2
             WHERE installation_id=?3 AND campaign_id=?4 AND singleton=1
               AND book_sequence=?5 AND generation=?6
               AND entry_opening_consumed_usd='0' AND entry_consumed_usd='0'",
            params![
                sum.canonical()?,
                i64::try_from(next).map_err(|_| RiskStoreError::StaleSequence)?,
                installation,
                &source.campaign_id,
                i64::try_from(meta.sequence).map_err(|_| RiskStoreError::StaleSequence)?,
                i64::try_from(meta.generation).map_err(|_| RiskStoreError::StaleGeneration)?
            ],
        )?;
        if changed != 1 {
            return Err(RiskStoreError::StaleSequence);
        }
        tx.execute(
            "INSERT INTO operation_receipts(
                installation_id,account_key,operation_id,kind,
                request_hash,result_ref,committed_at)
             VALUES(?1,?2,?3,'CAMPAIGN_OPENING_INSTALLED',?4,?5,?6)",
            params![
                installation,
                &accounts[0],
                &marker,
                &source.source_digest,
                sum.canonical()?,
                i64::try_from(source.as_of_ms).map_err(|_| RiskStoreError::StaleSequence)?
            ],
        )?;
        tx.execute("INSERT INTO operation_receipts(installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at) VALUES(?1,?2,?3,'CAMPAIGN_OPENING_SOURCE',?4,?5,?6)",params![installation,&accounts[0],format!("risk-entry-opening-source:{}",source.campaign_id),source.source_digest,serde_json::to_string(source)?,i64::try_from(source.as_of_ms).map_err(|_|RiskStoreError::Invalid("OPENING_TIME_RANGE"))?])?;
        let verified = read_meta_for_scope(tx, scope)?;
        integrity::verify_reservation_aggregates_for_scope(tx, &verified, scope)?;
        Ok(EntryOpeningReceipt {
            source_digest: source.source_digest.clone(),
            book_sequence: verified.sequence,
            replayed: false,
        })
    }
}

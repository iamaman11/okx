//! One campaign's SQLite transaction owner, no account ledger duplication.
//! Only trusted native evidence may enter reserve; this WIP exposes no
//! production bootstrap, venue POST authorization or legacy transfer.
use okx_kernel::risk::{
    model::{CampaignCapacity, RiskProposal, ScopeCapacity},
    money::{MoneyError, RiskAmount},
    rules::{RiskRuleError, evaluate_campaign_capacity, evaluate_scope_capacity},
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    path::Path,
    time::Duration,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub(super) enum RiskStoreError {
    #[error("RISK_STORAGE_UNAVAILABLE: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("RISK_STORAGE_UNAVAILABLE: {0}")]
    Io(#[from] std::io::Error),
    #[error("RISK_ENCODE_FAILED: {0}")]
    Encode(#[from] serde_json::Error),
    #[error("{0}")]
    Money(#[from] MoneyError),
    #[error("{0}")]
    Denied(#[from] RiskRuleError),
    #[error("RISK_BOOK_UNINITIALIZED")]
    Uninitialized,
    #[error("RISK_OWNER_QUEUE_FULL")]
    QueueFull,
    #[error("RISK_OWNER_STOPPED")]
    WorkerStopped,
    #[error("RISK_OUTCOME_UNKNOWN_DO_NOT_REPLAY")]
    OutcomeUnknown,
    #[error("RISK_WRITER_ALREADY_RUNNING")]
    WriterBusy,
    #[error("RISK_BOOK_IDENTITY_MISMATCH")]
    IdentityMismatch,
    #[error("POLICY_SNAPSHOT_NOT_AUTHORITATIVE")]
    PolicyConflict,
    #[error("STALE_RUNTIME_GENERATION")]
    StaleGeneration,
    #[error("COMPLETE_ACCOUNT_CENSUS_UNPROVEN")]
    CensusUnproven,
    #[error("IDEMPOTENCY_CONFLICT")]
    IdempotencyConflict,
    #[error("INTENT_ALREADY_POSTED_OR_RESOLVED")]
    AlreadyResolved,
    #[error("RISK_BOOK_MISSING_OR_CORRUPT: {0}")]
    Corrupt(&'static str),
    #[error("INVALID_RISK_INPUT: {0}")]
    Invalid(&'static str),
}
type Result<T> = std::result::Result<T, RiskStoreError>;

#[derive(Debug, Clone)]
pub(super) struct AccountPolicy {
    pub account_key: String,
    pub account_limit: RiskAmount,
    pub order_limit: RiskAmount,
    pub strategy_id: String,
    pub strategy_limit: RiskAmount,
}
#[derive(Debug, Clone)]
pub(super) struct CampaignOpening {
    pub installation_id: String,
    pub campaign_id: String,
    pub policy_revision: u64,
    pub generation: u64,
    pub global_limit: RiskAmount,
    pub loss_limit: RiskAmount,
    pub entry_limit: RiskAmount,
    pub accounts: Vec<AccountPolicy>,
}
#[derive(Debug, Clone)]
pub(super) struct VerifiedCensus {
    pub complete: bool,
    pub generation: u64,
    pub source_digest: String,
    pub generated_at_ms: i64,
    pub read_floor_ms: i64,
    pub occupied: BTreeMap<String, RiskAmount>,
}
#[derive(Debug, Clone)]
pub(super) struct ReserveInput {
    pub venue: String,
    pub uid: String,
    pub run_id: String,
    pub intent_id: String,
    pub strategy_id: String,
    pub instrument_id: String,
    pub plan_sha256: String,
    pub entry_proof_digest: String,
    pub conversion_digest: String,
    pub conversion_observed_at_ms: i64,
    pub conversion_rate: RiskAmount,
    pub proposal: RiskProposal,
    pub policy_revision: u64,
    pub now_ms: i64,
}
impl ReserveInput {
    fn account_key(&self) -> String {
        format!("{}:real:{}", self.venue, self.uid)
    }
    pub(super) fn reservation_key(&self) -> String {
        // Same length-prefixed key as legacy TS riskKey; no delimiter collision.
        [
            self.venue.as_str(),
            "real",
            self.uid.as_str(),
            self.run_id.as_str(),
            self.intent_id.as_str(),
        ]
        .into_iter()
        .map(|value| format!("{}:{value}", value.len()))
        .collect::<String>()
    }
    fn digest(&self) -> Result<String> {
        let canonical = serde_json::to_vec(&(
            self.account_key(),
            &self.run_id,
            &self.intent_id,
            &self.strategy_id,
            &self.instrument_id,
            &self.plan_sha256,
            &self.entry_proof_digest,
            &self.conversion_digest,
            self.conversion_observed_at_ms,
            self.conversion_rate.canonical()?,
            self.proposal.loss_usdt.canonical()?,
            self.proposal.loss_usd.canonical()?,
            self.proposal.entry_upper_usd.canonical()?,
            self.policy_revision,
        ))?;
        Ok(format!("{:x}", Sha256::digest(canonical)))
    }
    fn validate(&self) -> Result<()> {
        if !valid_venue(&self.venue)
            || !valid_digest(&self.uid)
            || !valid_digest(&self.plan_sha256)
            || !valid_digest(&self.entry_proof_digest)
            || !valid_digest(&self.conversion_digest)
            || !valid_token(&self.run_id)
            || !valid_token(&self.intent_id)
            || !valid_token(&self.strategy_id)
            || !valid_instrument(&self.instrument_id)
            || self.policy_revision == 0
            || self.now_ms <= 0
            || self.conversion_observed_at_ms <= 0
            || self.conversion_observed_at_ms > self.now_ms
            || self.now_ms - self.conversion_observed_at_ms > 5_000
        {
            return Err(RiskStoreError::Invalid("RESERVATION_IDENTITY_OR_SOURCE"));
        }
        if self.proposal.loss_usdt.is_zero()
            || self.proposal.entry_upper_usd.is_zero()
            || self.conversion_rate.is_zero()
            || self
                .proposal
                .loss_usdt
                .checked_mul_ceil(&self.conversion_rate)?
                != self.proposal.loss_usd
        {
            return Err(RiskStoreError::Invalid("USD_CONVERSION_UNPROVEN"));
        }
        Ok(())
    }
}
fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
fn valid_venue(value: &str) -> bool {
    (2..=32).contains(&value.len())
        && value.bytes().next().is_some_and(|b| b.is_ascii_lowercase())
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
}
fn valid_token(value: &str) -> bool {
    (8..=128).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn valid_instrument(value: &str) -> bool {
    (3..=64).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HeldReceipt {
    pub reservation_key: String,
    pub book_sequence: u64,
    pub replayed: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BookReadback {
    pub book_sequence: u64,
    pub policy_revision: u64,
    pub global_reserved_usdt: String,
    pub reserved_loss_usd: String,
    pub reserved_entry_usd: String,
}

struct BookMeta {
    policy_revision: u64,
    generation: u64,
    sequence: u64,
    global_limit: RiskAmount,
    global_reserved: RiskAmount,
    loss_limit: RiskAmount,
    loss_consumed: RiskAmount,
    loss_reserved: RiskAmount,
    entry_limit: RiskAmount,
    entry_consumed: RiskAmount,
    entry_reserved: RiskAmount,
    entry_breach: RiskAmount,
}
fn read_meta(tx: &rusqlite::Transaction<'_>) -> Result<BookMeta> {
    let row: (
        i64,
        i64,
        i64,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
    ) = tx.query_row(
        "SELECT policy_revision,generation,book_sequence,global_limit_usdt,global_reserved_usdt,
        loss_limit_usd,loss_consumed_usd,loss_reserved_usd,entry_limit_usd,
        entry_consumed_usd,entry_reserved_usd,entry_bound_breach_usd
        FROM risk_meta WHERE singleton=1",
        [],
        |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
                r.get(8)?,
                r.get(9)?,
                r.get(10)?,
                r.get(11)?,
            ))
        },
    )?;
    Ok(BookMeta {
        policy_revision: u64::try_from(row.0)
            .map_err(|_| RiskStoreError::Corrupt("POLICY_REVISION"))?,
        generation: u64::try_from(row.1).map_err(|_| RiskStoreError::Corrupt("GENERATION"))?,
        sequence: u64::try_from(row.2).map_err(|_| RiskStoreError::Corrupt("SEQUENCE"))?,
        global_limit: RiskAmount::parse(&row.3)?,
        global_reserved: RiskAmount::parse(&row.4)?,
        loss_limit: RiskAmount::parse(&row.5)?,
        loss_consumed: RiskAmount::parse(&row.6)?,
        loss_reserved: RiskAmount::parse(&row.7)?,
        entry_limit: RiskAmount::parse(&row.8)?,
        entry_consumed: RiskAmount::parse(&row.9)?,
        entry_reserved: RiskAmount::parse(&row.10)?,
        entry_breach: RiskAmount::parse(&row.11)?,
    })
}
fn amount(text: String) -> Result<RiskAmount> {
    Ok(RiskAmount::parse(&text)?)
}

pub(super) struct RiskBookStore {
    connection: Connection,
    _lease: File,
}
impl RiskBookStore {
    pub(super) fn open_existing(path: &Path, installation: &str, campaign: &str) -> Result<Self> {
        if !path.is_file() {
            return Err(RiskStoreError::Uninitialized);
        }
        let canonical = path.canonicalize()?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(format!("{}.writer.lock", canonical.display()))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Err(RiskStoreError::WriterBusy),
            Err(std::fs::TryLockError::Error(error)) => return Err(RiskStoreError::Io(error)),
        }
        let connection =
            Connection::open_with_flags(&canonical, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        connection.busy_timeout(Duration::from_millis(250))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        let version: i64 = connection.pragma_query_value(None, "user_version", |r| r.get(0))?;
        let journal: String = connection.pragma_query_value(None, "journal_mode", |r| r.get(0))?;
        if version != 1 || journal != "wal" {
            return Err(RiskStoreError::Corrupt("SCHEMA_OR_JOURNAL"));
        }
        let identity: (String, String, i64) = connection.query_row(
            "SELECT installation_id,campaign_id,legacy_fenced FROM risk_meta WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        if identity.0 != installation || identity.1 != campaign {
            return Err(RiskStoreError::IdentityMismatch);
        }
        if identity.2 != 1 {
            return Err(RiskStoreError::Corrupt("LEGACY_FENCE_NOT_PROVEN"));
        }
        Ok(Self {
            connection,
            _lease: lock,
        })
    }
    pub(super) fn readback(&mut self) -> Result<BookReadback> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)?;
        let meta = read_meta(&tx)?;
        tx.commit()?;
        Ok(BookReadback {
            book_sequence: meta.sequence,
            policy_revision: meta.policy_revision,
            global_reserved_usdt: meta.global_reserved.canonical()?,
            reserved_loss_usd: meta.loss_reserved.canonical()?,
            reserved_entry_usd: meta.entry_reserved.canonical()?,
        })
    }
    /// The source proof type is private to this module pending validated native
    /// evidence constructor and production wiring. A caller cannot mint grants.
    pub(super) fn reserve(
        &mut self,
        input: &ReserveInput,
        census: &VerifiedCensus,
    ) -> Result<HeldReceipt> {
        input.validate()?;
        if !census.complete
            || census.generation == 0
            || !valid_digest(&census.source_digest)
            || input.now_ms < census.generated_at_ms
            || census.generated_at_ms <= 0
            || census.read_floor_ms <= 0
            || census.read_floor_ms > census.generated_at_ms
            || input.now_ms - census.generated_at_ms > 5_000
        {
            return Err(RiskStoreError::CensusUnproven);
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let meta = read_meta(&tx)?;
        if meta.policy_revision != input.policy_revision {
            return Err(RiskStoreError::PolicyConflict);
        }
        if meta.generation != census.generation {
            return Err(RiskStoreError::StaleGeneration);
        }
        let mut policies = BTreeMap::new();
        {
            let mut stmt=tx.prepare("SELECT account_key,limit_usdt,order_limit_usdt FROM risk_policy_account ORDER BY account_key")?;
            let iter = stmt.query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?;
            for item in iter {
                let (account, limit, order) = item?;
                policies.insert(account, (amount(limit)?, amount(order)?));
            }
        }
        if policies.is_empty() || policies.keys().ne(census.occupied.keys()) {
            return Err(RiskStoreError::CensusUnproven);
        }
        let account_key = input.account_key();
        let (account_limit, order_limit) = policies
            .get(&account_key)
            .ok_or(RiskStoreError::Invalid("ACCOUNT_NOT_ACTIVE"))?;
        let strategy_limit:String=tx.query_row(
            "SELECT limit_usdt FROM risk_policy_strategy WHERE account_key=?1 AND strategy_id=?2",
            params![&account_key,&input.strategy_id],|r|r.get(0))
            .optional()?.ok_or(RiskStoreError::Invalid("STRATEGY_NOT_ENABLED_FOR_ACCOUNT"))?;
        let reserved_account: String = tx.query_row(
            "SELECT reserved_usdt FROM risk_account_occupied WHERE account_key=?1",
            params![&account_key],
            |r| r.get(0),
        )?;
        let reserved_strategy:String=tx.query_row(
            "SELECT reserved_usdt FROM risk_strategy_occupied WHERE account_key=?1 AND strategy_id=?2",
            params![&account_key,&input.strategy_id],|r|r.get(0))?;
        let key = input.reservation_key();
        let digest = input.digest()?;
        let prior: Option<(String, String)> = tx
            .query_row(
                "SELECT request_digest,state FROM risk_entries WHERE reservation_key=?1",
                params![&key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if let Some((existing, state)) = prior {
            if existing != digest {
                return Err(RiskStoreError::IdempotencyConflict);
            }
            if state != "HELD" {
                return Err(RiskStoreError::AlreadyResolved);
            }
            return Ok(HeldReceipt {
                reservation_key: key,
                book_sequence: meta.sequence,
                replayed: true,
            });
        }
        let mut source_total = RiskAmount::zero();
        for occupied in census.occupied.values() {
            source_total = source_total.checked_add(occupied)?;
        }
        let global_occupied = source_total.checked_add(&meta.global_reserved)?;
        let account_occupied =
            census.occupied[&account_key].checked_add(&amount(reserved_account.clone())?)?;
        let account_reserved = amount(reserved_account)?;
        let strategy_reserved = amount(reserved_strategy)?;
        let strategy_limit = amount(strategy_limit)?;
        evaluate_campaign_capacity(
            &CampaignCapacity {
                loss_limit_usd: meta.loss_limit.clone(),
                loss_consumed_usd: meta.loss_consumed.clone(),
                loss_reserved_usd: meta.loss_reserved.clone(),
                entry_limit_usd: meta.entry_limit.clone(),
                entry_consumed_usd: meta.entry_consumed.clone(),
                entry_reserved_usd: meta.entry_reserved.clone(),
                entry_bound_breach_usd: meta.entry_breach.clone(),
            },
            &input.proposal,
        )?;
        evaluate_scope_capacity(
            &ScopeCapacity {
                global_limit_usdt: meta.global_limit,
                global_occupied_usdt: global_occupied,
                account_limit_usdt: account_limit.clone(),
                account_occupied_usdt: account_occupied,
                strategy_limit_usdt: strategy_limit,
                strategy_occupied_usdt: strategy_reserved.clone(),
                order_limit_usdt: order_limit.clone(),
            },
            &input.proposal,
        )?;
        let next_sequence = meta
            .sequence
            .checked_add(1)
            .ok_or(RiskStoreError::Corrupt("SEQUENCE_OVERFLOW"))?;
        let next_sequence_i64 = i64::try_from(next_sequence)
            .map_err(|_| RiskStoreError::Corrupt("SEQUENCE_OVERFLOW"))?;
        let new_global = meta
            .global_reserved
            .checked_add(&input.proposal.loss_usdt)?
            .canonical()?;
        let new_loss = meta
            .loss_reserved
            .checked_add(&input.proposal.loss_usd)?
            .canonical()?;
        let new_entry = meta
            .entry_reserved
            .checked_add(&input.proposal.entry_upper_usd)?
            .canonical()?;
        let new_account = account_reserved
            .checked_add(&input.proposal.loss_usdt)?
            .canonical()?;
        let new_strategy = strategy_reserved
            .checked_add(&input.proposal.loss_usdt)?
            .canonical()?;
        let policy_i64 =
            i64::try_from(input.policy_revision).map_err(|_| RiskStoreError::PolicyConflict)?;
        tx.execute("INSERT INTO risk_entries(
            reservation_key,account_key,strategy_id,intent_id,plan_sha256,request_digest,
            policy_revision,state,loss_usdt,loss_usd,entry_upper_usd,entry_proof_digest,
            source_digest,book_sequence) VALUES (?1,?2,?3,?4,?5,?6,?7,'HELD',?8,?9,?10,?11,?12,?13)",
            params![&key,&account_key,&input.strategy_id,&input.intent_id,&input.plan_sha256,digest,
                policy_i64,input.proposal.loss_usdt.canonical()?,input.proposal.loss_usd.canonical()?,
                input.proposal.entry_upper_usd.canonical()?,&input.entry_proof_digest,
                &census.source_digest,next_sequence_i64])?;
        tx.execute(
            "UPDATE risk_account_occupied SET reserved_usdt=?1 WHERE account_key=?2",
            params![new_account, &account_key],
        )?;
        tx.execute("UPDATE risk_strategy_occupied SET reserved_usdt=?1 WHERE account_key=?2 AND strategy_id=?3",
            params![new_strategy,&account_key,&input.strategy_id])?;
        let changed=tx.execute(
            "UPDATE risk_meta SET book_sequence=?1,global_reserved_usdt=?2,loss_reserved_usd=?3,entry_reserved_usd=?4
             WHERE singleton=1 AND book_sequence=?5 AND policy_revision=?6",
            params![next_sequence_i64,new_global,new_loss,new_entry,
                i64::try_from(meta.sequence).map_err(|_|RiskStoreError::Corrupt("SEQUENCE_OVERFLOW"))?,
                policy_i64])?;
        if changed != 1 {
            return Err(RiskStoreError::PolicyConflict);
        }
        tx.commit()?;
        Ok(HeldReceipt {
            reservation_key: key,
            book_sequence: next_sequence,
            replayed: false,
        })
    }

    #[cfg(test)]
    pub(super) fn initialize_fixture(path: &Path, opening: &CampaignOpening) -> Result<()> {
        if path.exists() {
            return Err(RiskStoreError::Invalid("EXISTING_RISK_BOOK"));
        }
        if opening.accounts.len() < 2 || opening.policy_revision == 0 || opening.generation == 0 {
            return Err(RiskStoreError::Invalid("FIXTURE_COHORT"));
        }
        // Test-only; never a production path to mint a fresh risk budget over
        // an unknown legacy owner. Production bootstrap requires physical fence.
        let mut conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.execute_batch(include_str!("schema.sql"))?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO risk_meta VALUES(1,?1,?2,?3,?4,0,1,?5,'0',?6,'0','0',?7,'0','0','0')",
            params![
                opening.campaign_id,
                opening.installation_id,
                i64::try_from(opening.policy_revision)
                    .map_err(|_| RiskStoreError::PolicyConflict)?,
                i64::try_from(opening.generation).map_err(|_| RiskStoreError::StaleGeneration)?,
                opening.global_limit.canonical()?,
                opening.loss_limit.canonical()?,
                opening.entry_limit.canonical()?
            ],
        )?;
        for account in &opening.accounts {
            tx.execute(
                "INSERT INTO risk_account_occupied VALUES (?1,'0')",
                params![account.account_key],
            )?;
            tx.execute(
                "INSERT INTO risk_policy_account VALUES(?1,?2,?3)",
                params![
                    account.account_key,
                    account.account_limit.canonical()?,
                    account.order_limit.canonical()?
                ],
            )?;
            tx.execute(
                "INSERT INTO risk_policy_strategy VALUES(?1,?2,?3)",
                params![
                    account.account_key,
                    account.strategy_id,
                    account.strategy_limit.canonical()?
                ],
            )?;
            tx.execute(
                "INSERT INTO risk_strategy_occupied VALUES(?1,?2,'0')",
                params![account.account_key, account.strategy_id],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
}

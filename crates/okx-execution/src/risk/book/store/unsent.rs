//! The campaign-side half of REJECTED_UNSENT -> permanent no-reserve fence.
//! The input is NOT a proof constructor. The authenticated account SQL owner
//! must verify and construct the receipt before production callers can use it.
//! No absence, timeout, caller flag or source digest alone is evidence of no send.
use super::{
    LEGACY_MAX_SAFE_INTEGER, Result, RiskBookStore, RiskStoreError, amount, read_meta,
    valid_digest, valid_token, verify_reservation_aggregates,
};
use crate::account_store::UnsentSqlReceipt;
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::risk::book) struct UnsentSqlProof {
    pub reservation_key: String,
    pub account_key: String,
    pub intent_id: String,
    pub plan_sha256: String,
    pub request_hash: String,
    pub reason: String,
    pub record_digest: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::risk::book) struct UnsentReceipt {
    pub reservation_key: String,
    pub book_sequence: u64,
    pub replayed: bool,
}

fn valid_no_reserve_address(proof: &UnsentSqlProof) -> bool {
    let Some(uid) = proof.account_key.strip_prefix("okx:real:") else {
        return false;
    };
    if !valid_digest(uid) || !valid_token(&proof.intent_id) {
        return false;
    }
    let prefix = ["okx", "real", uid]
        .into_iter()
        .map(|part| format!("{}:{part}", part.len()))
        .collect::<String>();
    let suffix = format!("{}:{}", proof.intent_id.len(), proof.intent_id);
    let Some(run) = proof
        .reservation_key
        .strip_prefix(&prefix)
        .and_then(|rest| rest.strip_suffix(&suffix))
    else {
        return false;
    };
    let Some((length, value)) = run.split_once(':') else {
        return false;
    };
    !length.starts_with('0')
        && length.len() <= 3
        && length.parse::<usize>().ok() == Some(value.len())
        && valid_token(value)
}

impl UnsentSqlProof {
    /// The receipt is read from the existing durable native AccountStore SQL
    /// owner, never decoded from a caller's proof flags or remote JSON.
    pub(in crate::risk::book) fn from_account_receipt(receipt: UnsentSqlReceipt) -> Result<Self> {
        let encoded = serde_json::to_vec(&receipt)?;
        let proof = Self {
            reservation_key: receipt.reservation_key,
            account_key: receipt.account_key,
            intent_id: receipt.intent_id,
            plan_sha256: receipt.plan_hash,
            request_hash: receipt.request_hash,
            reason: receipt.reason,
            record_digest: format!("{:x}", Sha256::digest(encoded)),
        };
        proof.validate()?;
        Ok(proof)
    }

    fn validate(&self) -> Result<()> {
        if self.reservation_key.len() > 512
            || !valid_no_reserve_address(self)
            || !valid_digest(&self.plan_sha256)
            || !valid_digest(&self.request_hash)
            || !valid_digest(&self.record_digest)
            || !(1..=128).contains(&self.reason.len())
        {
            return Err(RiskStoreError::Invalid("UNSENT_SQL_PROOF_INVALID"));
        }
        Ok(())
    }
}
fn next_sequence(current: u64) -> Result<i64> {
    let next = current
        .checked_add(1)
        .filter(|v| *v <= LEGACY_MAX_SAFE_INTEGER)
        .ok_or(RiskStoreError::Corrupt("SEQUENCE_OVERFLOW"))?;
    i64::try_from(next).map_err(|_| RiskStoreError::Corrupt("SEQUENCE_OVERFLOW"))
}

impl RiskBookStore {
    /// Internal SQL mutation. Cannot be wired to an arbitrary network proof:
    /// the native AccountStore must first verify its own durable no-send CAS.
    pub(in crate::risk::book) fn release_unsent(
        &mut self,
        proof: &UnsentSqlProof,
        expected_sequence: u64,
        generation: u64,
    ) -> Result<UnsentReceipt> {
        proof.validate()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let meta = read_meta(&tx)?;
        verify_reservation_aggregates(&tx, &meta)?;
        if generation != meta.generation {
            return Err(RiskStoreError::StaleGeneration);
        }
        if expected_sequence != meta.sequence {
            return Err(RiskStoreError::StaleSequence);
        }
        let existing: Option<(String, String, String, String, String, String)> = tx
            .query_row(
                "SELECT account_key,intent_id,plan_sha256,request_hash,record_digest,reason
                 FROM risk_no_reserve WHERE reservation_key=?1",
                params![&proof.reservation_key],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                    ))
                },
            )
            .optional()?;
        if let Some(row) = existing {
            if row
                != (
                    proof.account_key.clone(),
                    proof.intent_id.clone(),
                    proof.plan_sha256.clone(),
                    proof.request_hash.clone(),
                    proof.record_digest.clone(),
                    proof.reason.clone(),
                )
            {
                return Err(RiskStoreError::IdempotencyConflict);
            }
            return Ok(UnsentReceipt {
                reservation_key: proof.reservation_key.clone(),
                book_sequence: meta.sequence,
                replayed: true,
            });
        }
        let entry: Option<(String, String, String, String, String, String, String, String, String, String)> = tx
            .query_row(
                "SELECT account_key,intent_id,plan_sha256,state,strategy_id,
                        reserved_usdt,reserved_loss_usd,release_proof_digest,release_request_hash,release_reason
                 FROM risk_entries WHERE reservation_key=?1",
                params![&proof.reservation_key],
                |r| Ok((
                    r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?,
                    r.get(5)?, r.get(6)?, r.get(7)?, r.get(8)?, r.get(9)?
                )),
            )
            .optional()?;
        let new_sequence = next_sequence(meta.sequence)?;
        let new_sequence_u64 = u64::try_from(new_sequence)
            .map_err(|_| RiskStoreError::Corrupt("SEQUENCE_OVERFLOW"))?;
        if let Some((
            account,
            intent,
            plan,
            state,
            strategy,
            remaining_usdt,
            remaining_loss,
            previous_proof,
            previous_request,
            previous_reason,
        )) = entry
        {
            if account != proof.account_key
                || intent != proof.intent_id
                || plan != proof.plan_sha256
            {
                return Err(RiskStoreError::IdempotencyConflict);
            }
            if state == "RELEASED" {
                if previous_proof != proof.record_digest
                    || previous_request != proof.request_hash
                    || previous_reason != proof.reason
                {
                    return Err(RiskStoreError::IdempotencyConflict);
                }
                return Ok(UnsentReceipt {
                    reservation_key: proof.reservation_key.clone(),
                    book_sequence: meta.sequence,
                    replayed: true,
                });
            }
            // A POSTED fence is not a venue no-send witness. The existing
            // AccountStore source-binding for that state must be proven first.
            if state != "HELD" {
                return Err(RiskStoreError::AlreadyResolved);
            }
            let held: String = tx.query_row(
                "SELECT reserved_entry_usd FROM risk_entries WHERE reservation_key=?1",
                params![&proof.reservation_key],
                |r| r.get(0),
            )?;
            let reserved_usdt = amount(remaining_usdt)?;
            let reserved_loss = amount(remaining_loss)?;
            let reserved_entry = amount(held)?;
            let account_total: String = tx.query_row(
                "SELECT reserved_usdt FROM risk_account_occupied WHERE account_key=?1",
                params![&account],
                |r| r.get(0),
            )?;
            let strategy_total: String = tx.query_row(
                "SELECT reserved_usdt FROM risk_strategy_occupied
                 WHERE account_key=?1 AND strategy_id=?2",
                params![&account, &strategy],
                |r| r.get(0),
            )?;
            let global = meta
                .global_reserved
                .checked_sub(&reserved_usdt)?
                .canonical()?;
            let loss = meta
                .loss_reserved
                .checked_sub(&reserved_loss)?
                .canonical()?;
            let entry = meta
                .entry_reserved
                .checked_sub(&reserved_entry)?
                .canonical()?;
            let account_remaining = amount(account_total)?
                .checked_sub(&reserved_usdt)?
                .canonical()?;
            let strategy_remaining = amount(strategy_total)?
                .checked_sub(&reserved_usdt)?
                .canonical()?;
            let updated = tx.execute(
                "UPDATE risk_entries SET state='RELEASED',
                       reserved_usdt='0',reserved_loss_usd='0',reserved_entry_usd='0',
                       release_proof_digest=?2,release_request_hash=?3,release_reason=?4
                 WHERE reservation_key=?1 AND state='HELD'",
                params![
                    &proof.reservation_key,
                    &proof.record_digest,
                    &proof.request_hash,
                    &proof.reason
                ],
            )?;
            if updated != 1 {
                return Err(RiskStoreError::StaleSequence);
            }
            tx.execute(
                "UPDATE risk_account_occupied SET reserved_usdt=?1 WHERE account_key=?2",
                params![account_remaining, &account],
            )?;
            tx.execute(
                "UPDATE risk_strategy_occupied SET reserved_usdt=?1
                 WHERE account_key=?2 AND strategy_id=?3",
                params![strategy_remaining, &account, &strategy],
            )?;
            tx.execute(
                "UPDATE risk_meta SET book_sequence=?1,
                       global_reserved_usdt=?2,loss_reserved_usd=?3,entry_reserved_usd=?4
                 WHERE singleton=1",
                params![new_sequence, global, loss, entry],
            )?;
        } else {
            let account: Option<i64> = tx
                .query_row(
                    "SELECT 1 FROM risk_policy_account WHERE account_key=?1",
                    params![&proof.account_key],
                    |r| r.get(0),
                )
                .optional()?;
            if account.is_none() {
                return Err(RiskStoreError::Invalid("ACCOUNT_NOT_ACTIVE"));
            }
            tx.execute(
                "INSERT INTO risk_no_reserve(
                   reservation_key,account_key,intent_id,plan_sha256,
                   request_hash,record_digest,reason,policy_revision,book_sequence)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                params![
                    &proof.reservation_key,
                    &proof.account_key,
                    &proof.intent_id,
                    &proof.plan_sha256,
                    &proof.request_hash,
                    &proof.record_digest,
                    &proof.reason,
                    i64::try_from(meta.policy_revision)
                        .map_err(|_| RiskStoreError::PolicyConflict)?,
                    new_sequence,
                ],
            )?;
            tx.execute(
                "UPDATE risk_meta SET book_sequence=?1 WHERE singleton=1",
                params![new_sequence],
            )?;
        }
        tx.commit()?;
        Ok(UnsentReceipt {
            reservation_key: proof.reservation_key.clone(),
            book_sequence: new_sequence_u64,
            replayed: false,
        })
    }
}

//! The campaign-side half of REJECTED_UNSENT -> permanent no-reserve fence.
//! The input is NOT a proof constructor. The authenticated account SQL owner
//! must verify and construct the receipt before production callers can use it.
//! No absence, timeout, caller flag or source digest alone is evidence of no send.
use super::{
    LEGACY_MAX_SAFE_INTEGER, Result, RiskBookStore, RiskStoreError, amount, integrity,
    read_meta_for_scope, reserve_scope_values, valid_digest, valid_token,
};
use crate::account_store::UnsentSqlReceipt;
use rusqlite::{
    OptionalExtension, Transaction, TransactionBehavior, params_from_iter, types::Value,
};
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
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result =
            Self::release_unsent_sql_transaction(&tx, None, proof, expected_sequence, generation)?;
        tx.commit()?;
        Ok(result)
    }

    /// V6 joint no-send operation: obtain the ORIGINAL AccountStore
    /// REJECTED_UNSENT/attempt receipt from this same SQLite transaction,
    /// then apply the ORIGINAL RiskBook no-reserve/release rule. There is
    /// exactly one caller-owned COMMIT, no caller-minted source flag and
    /// no separate account/campaign connection.
    pub(in crate::risk::book) fn release_unified_unsent_from_account_sql_transaction(
        tx: &Transaction<'_>,
        installation: &str,
        campaign: &str,
        account_key: &str,
        intent_id: &str,
        expected_sequence: u64,
        generation: u64,
    ) -> Result<UnsentReceipt> {
        if installation.is_empty() || campaign.is_empty()
            || account_key.is_empty() || intent_id.is_empty()
        {
            return Err(RiskStoreError::Invalid("UNIFIED_UNSENT_SCOPE_INVALID"));
        }
        // The source account must belong to THIS campaign, not merely be
        // present in the same installation (A/B may share intent IDs).
        let source_campaign: Option<String> = tx.query_row(
            "SELECT campaign_id FROM account_policy
             WHERE installation_id=?1 AND account_key=?2 AND singleton=1",
            rusqlite::params![installation, account_key],
            |row| row.get(0),
        ).optional()?;
        if source_campaign.as_deref() != Some(campaign) {
            return Err(RiskStoreError::UnsentSqlUnproven);
        }
        let original = crate::account_store::AccountStore::unsent_release_receipt_sql(
            tx, Some(installation), account_key, intent_id,
        )
        .map_err(|error| match error {
            crate::StoreError::Invalid(reason) => RiskStoreError::Invalid(reason),
            crate::StoreError::Sql(error) => RiskStoreError::Sql(error),
            crate::StoreError::Io(error) => RiskStoreError::Io(error),
        })?
        .ok_or(RiskStoreError::UnsentSqlUnproven)?;
        let verified = UnsentSqlProof::from_account_receipt(original)?;
        if verified.account_key != account_key || verified.intent_id != intent_id {
            return Err(RiskStoreError::UnsentSqlUnproven);
        }
        Self::release_unsent_sql_transaction(
            tx, Some((installation, campaign)), &verified, expected_sequence, generation,
        )
    }

    /// Exact ORIGINAL native no-send settlement, shared with installation-v6.
    /// The caller has to supply an independently verified original AccountStore
    /// rejection receipt. No raw user/Cloudflare proof is an authority.
    /// This function never commits or opens another SQLite writer.
    pub(in crate::risk::book) fn release_unsent_sql_transaction(
        tx: &Transaction<'_>,
        scope: Option<(&str, &str)>,
        proof: &UnsentSqlProof,
        expected_sequence: u64,
        generation: u64,
    ) -> Result<UnsentReceipt> {
        proof.validate()?;
        let meta = read_meta_for_scope(tx, scope)?;
        integrity::verify_reservation_aggregates_for_scope(tx, &meta, scope)?;
        if generation != meta.generation {
            return Err(RiskStoreError::StaleGeneration);
        }
        // A completed, matching operation may be replayed with its ORIGINAL
        // expected sequence after a committed reply was lost. Only a NEW
        // mutation is subject to current campaign-sequence CAS.
        let existing_sql = if scope.is_some() {
            "SELECT account_key,intent_id,plan_sha256,request_hash,record_digest,reason
             FROM risk_no_reserve WHERE reservation_key=?1
               AND installation_id=?2 AND campaign_id=?3"
        } else {
            "SELECT account_key,intent_id,plan_sha256,request_hash,record_digest,reason
             FROM risk_no_reserve WHERE reservation_key=?1"
        };
        let entry_address =
            reserve_scope_values(vec![Value::Text(proof.reservation_key.clone())], scope);
        let existing: Option<(String, String, String, String, String, String)> = tx
            .query_row(existing_sql, params_from_iter(entry_address.iter()), |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                ))
            })
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
        let risk_entry_sql = if scope.is_some() {
            "SELECT account_key,intent_id,plan_sha256,state,strategy_id,
                    reserved_usdt,reserved_loss_usd,release_proof_digest,release_request_hash,release_reason
             FROM risk_entries WHERE reservation_key=?1
               AND installation_id=?2 AND campaign_id=?3"
        } else {
            "SELECT account_key,intent_id,plan_sha256,state,strategy_id,
                    reserved_usdt,reserved_loss_usd,release_proof_digest,release_request_hash,release_reason
             FROM risk_entries WHERE reservation_key=?1"
        };
        let entry: Option<(
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
        )> = tx
            .query_row(
                risk_entry_sql,
                params_from_iter(entry_address.iter()),
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
                    ))
                },
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
            // A completed release can replay after a different campaign
            // sequence was committed; new mutations still use exact CAS.
            if expected_sequence != meta.sequence {
                return Err(RiskStoreError::StaleSequence);
            }
            // A POSTED fence is not a venue no-send witness. The existing
            // AccountStore source-binding for that state must be proven first.
            if state != "HELD" {
                return Err(RiskStoreError::AlreadyResolved);
            }
            let held_sql = if scope.is_some() {
                "SELECT reserved_entry_usd FROM risk_entries WHERE reservation_key=?1
                 AND installation_id=?2 AND campaign_id=?3"
            } else {
                "SELECT reserved_entry_usd FROM risk_entries WHERE reservation_key=?1"
            };
            let held: String =
                tx.query_row(held_sql, params_from_iter(entry_address.iter()), |r| {
                    r.get(0)
                })?;
            let reserved_usdt = amount(remaining_usdt)?;
            let reserved_loss = amount(remaining_loss)?;
            let reserved_entry = amount(held)?;
            let account_sql = if scope.is_some() {
                "SELECT reserved_usdt FROM risk_account_occupied WHERE account_key=?1
                 AND installation_id=?2 AND campaign_id=?3"
            } else {
                "SELECT reserved_usdt FROM risk_account_occupied WHERE account_key=?1"
            };
            let account_address = reserve_scope_values(vec![Value::Text(account.clone())], scope);
            let account_total: String =
                tx.query_row(account_sql, params_from_iter(account_address.iter()), |r| {
                    r.get(0)
                })?;
            let strategy_sql = if scope.is_some() {
                "SELECT reserved_usdt FROM risk_strategy_occupied
                 WHERE account_key=?1 AND strategy_id=?2
                   AND installation_id=?3 AND campaign_id=?4"
            } else {
                "SELECT reserved_usdt FROM risk_strategy_occupied
                 WHERE account_key=?1 AND strategy_id=?2"
            };
            let strategy_address = reserve_scope_values(
                vec![Value::Text(account.clone()), Value::Text(strategy.clone())],
                scope,
            );
            let strategy_total: String = tx.query_row(
                strategy_sql,
                params_from_iter(strategy_address.iter()),
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
            let update_entry_sql = if scope.is_some() {
                "UPDATE risk_entries SET state='RELEASED',
                       reserved_usdt='0',reserved_loss_usd='0',reserved_entry_usd='0',
                       release_proof_digest=?2,release_request_hash=?3,release_reason=?4
                 WHERE reservation_key=?1 AND state='HELD'
                   AND installation_id=?5 AND campaign_id=?6"
            } else {
                "UPDATE risk_entries SET state='RELEASED',
                       reserved_usdt='0',reserved_loss_usd='0',reserved_entry_usd='0',
                       release_proof_digest=?2,release_request_hash=?3,release_reason=?4
                 WHERE reservation_key=?1 AND state='HELD'"
            };
            let update_entry_args = reserve_scope_values(
                vec![
                    Value::Text(proof.reservation_key.clone()),
                    Value::Text(proof.record_digest.clone()),
                    Value::Text(proof.request_hash.clone()),
                    Value::Text(proof.reason.clone()),
                ],
                scope,
            );
            let updated =
                tx.execute(update_entry_sql, params_from_iter(update_entry_args.iter()))?;
            if updated != 1 {
                return Err(RiskStoreError::StaleSequence);
            }
            let account_update_sql = if scope.is_some() {
                "UPDATE risk_account_occupied SET reserved_usdt=?1 WHERE account_key=?2
                 AND installation_id=?3 AND campaign_id=?4"
            } else {
                "UPDATE risk_account_occupied SET reserved_usdt=?1 WHERE account_key=?2"
            };
            let account_update_args = reserve_scope_values(
                vec![Value::Text(account_remaining), Value::Text(account.clone())],
                scope,
            );
            let account_updates = tx.execute(
                account_update_sql,
                params_from_iter(account_update_args.iter()),
            )?;
            let strategy_update_sql = if scope.is_some() {
                "UPDATE risk_strategy_occupied SET reserved_usdt=?1
                 WHERE account_key=?2 AND strategy_id=?3
                   AND installation_id=?4 AND campaign_id=?5"
            } else {
                "UPDATE risk_strategy_occupied SET reserved_usdt=?1
                 WHERE account_key=?2 AND strategy_id=?3"
            };
            let strategy_update_args = reserve_scope_values(
                vec![
                    Value::Text(strategy_remaining),
                    Value::Text(account),
                    Value::Text(strategy),
                ],
                scope,
            );
            let strategy_updates = tx.execute(
                strategy_update_sql,
                params_from_iter(strategy_update_args.iter()),
            )?;
            if scope.is_some() && (account_updates != 1 || strategy_updates != 1) {
                return Err(RiskStoreError::Corrupt("UNIFIED_UNSENT_OCCUPANCY_MISSING"));
            }
            let meta_update_sql = if scope.is_some() {
                "UPDATE risk_meta SET book_sequence=?1,
                       global_reserved_usdt=?2,loss_reserved_usd=?3,entry_reserved_usd=?4
                 WHERE singleton=1 AND book_sequence=?5 AND generation=?6
                   AND installation_id=?7 AND campaign_id=?8"
            } else {
                "UPDATE risk_meta SET book_sequence=?1,
                       global_reserved_usdt=?2,loss_reserved_usd=?3,entry_reserved_usd=?4
                 WHERE singleton=1"
            };
            let mut meta_update_args = vec![
                Value::Integer(new_sequence),
                Value::Text(global),
                Value::Text(loss),
                Value::Text(entry),
            ];
            if scope.is_some() {
                meta_update_args.extend([
                    Value::Integer(
                        i64::try_from(meta.sequence).map_err(|_| RiskStoreError::StaleSequence)?,
                    ),
                    Value::Integer(
                        i64::try_from(meta.generation)
                            .map_err(|_| RiskStoreError::StaleGeneration)?,
                    ),
                ]);
            }
            let meta_update_args = reserve_scope_values(meta_update_args, scope);
            let meta_updates =
                tx.execute(meta_update_sql, params_from_iter(meta_update_args.iter()))?;
            if scope.is_some() && meta_updates != 1 {
                return Err(RiskStoreError::StaleSequence);
            }
        } else {
            if expected_sequence != meta.sequence {
                return Err(RiskStoreError::StaleSequence);
            }
            let account_sql = if scope.is_some() {
                "SELECT 1 FROM risk_policy_account WHERE account_key=?1
                 AND installation_id=?2 AND campaign_id=?3"
            } else {
                "SELECT 1 FROM risk_policy_account WHERE account_key=?1"
            };
            let account_args =
                reserve_scope_values(vec![Value::Text(proof.account_key.clone())], scope);
            let account: Option<i64> = tx
                .query_row(account_sql, params_from_iter(account_args.iter()), |r| {
                    r.get(0)
                })
                .optional()?;
            if account.is_none() {
                return Err(RiskStoreError::Invalid("ACCOUNT_NOT_ACTIVE"));
            }
            let insert_sql = if scope.is_some() {
                "INSERT INTO risk_no_reserve(
                   reservation_key,account_key,intent_id,plan_sha256,
                   request_hash,record_digest,reason,policy_revision,book_sequence,
                   installation_id,campaign_id)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)"
            } else {
                "INSERT INTO risk_no_reserve(
                   reservation_key,account_key,intent_id,plan_sha256,
                   request_hash,record_digest,reason,policy_revision,book_sequence)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)"
            };
            let insert_args = reserve_scope_values(
                vec![
                    Value::Text(proof.reservation_key.clone()),
                    Value::Text(proof.account_key.clone()),
                    Value::Text(proof.intent_id.clone()),
                    Value::Text(proof.plan_sha256.clone()),
                    Value::Text(proof.request_hash.clone()),
                    Value::Text(proof.record_digest.clone()),
                    Value::Text(proof.reason.clone()),
                    Value::Integer(
                        i64::try_from(meta.policy_revision)
                            .map_err(|_| RiskStoreError::PolicyConflict)?,
                    ),
                    Value::Integer(new_sequence),
                ],
                scope,
            );
            tx.execute(insert_sql, params_from_iter(insert_args.iter()))?;
            let meta_update_sql = if scope.is_some() {
                "UPDATE risk_meta SET book_sequence=?1 WHERE singleton=1
                 AND book_sequence=?2 AND generation=?3
                 AND installation_id=?4 AND campaign_id=?5"
            } else {
                "UPDATE risk_meta SET book_sequence=?1 WHERE singleton=1"
            };
            let mut meta_update_args = vec![Value::Integer(new_sequence)];
            if scope.is_some() {
                meta_update_args.extend([
                    Value::Integer(
                        i64::try_from(meta.sequence).map_err(|_| RiskStoreError::StaleSequence)?,
                    ),
                    Value::Integer(
                        i64::try_from(meta.generation)
                            .map_err(|_| RiskStoreError::StaleGeneration)?,
                    ),
                ]);
            }
            let meta_update_args = reserve_scope_values(meta_update_args, scope);
            let updated = tx.execute(meta_update_sql, params_from_iter(meta_update_args.iter()))?;
            if scope.is_some() && updated != 1 {
                return Err(RiskStoreError::StaleSequence);
            }
        }
        Ok(UnsentReceipt {
            reservation_key: proof.reservation_key.clone(),
            book_sequence: new_sequence_u64,
            replayed: false,
        })
    }
}

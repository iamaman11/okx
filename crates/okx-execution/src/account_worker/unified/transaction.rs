//! One atomic AccountStore/RiskBook command; no independent queue or storage owner.
use super::{UnifiedFinanceWrite, UnifiedFinanceWriteReceipt};
use super::{original_bound_entry_settlement_sql, original_bound_unsent_release_sql};
use crate::{AccountStore, StoreError, StoreResult};
/// The original SQL owners share this one transaction; no secondary
/// AccountStore/RiskBook handle, independent COMMIT or RPC can participate.
/// The only caller that may commit this change is the installation SQL FIFO.
/// For offline tests this very same function runs inside a ROLLBACK.
pub(crate) fn apply_original_unified_finance_transaction(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    command: UnifiedFinanceWrite,
) -> StoreResult<UnifiedFinanceWriteReceipt> {
    // Enforce the complete command's atomicity even if a future caller
    // mishandles Err and attempts to COMMIT its surrounding transaction.
    // This nested savepoint is NOT a second database writer or COMMIT.
    // Normal Risk::Denied is an Ok receipt and preserves any monotonic
    // source-loss high-water observed before denial.
    tx.execute_batch("SAVEPOINT okx_finance_command")?;
    match apply_original_unified_finance_inner(tx, installation, command) {
        Ok(receipt) => {
            tx.execute_batch("RELEASE SAVEPOINT okx_finance_command")?;
            Ok(receipt)
        }
        Err(error) => {
            tx.execute_batch(
                "ROLLBACK TO SAVEPOINT okx_finance_command;
                 RELEASE SAVEPOINT okx_finance_command",
            )?;
            Err(error)
        }
    }
}

/// The original AccountStore source is authoritative. A supplied proof
/// merely names an existing no-send record; an absent, foreign or altered
/// terminal receipt is NEVER proof that the venue POST did not happen.
/// The ORIGINAL RiskBook computes the exact same canonical receipt SHA-256.
fn apply_original_unified_finance_inner(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    command: UnifiedFinanceWrite,
) -> StoreResult<UnifiedFinanceWriteReceipt> {
    use crate::risk;
    let risk_operation = |outcome: Result<risk::UnifiedRiskDecision, &'static str>| {
        outcome
            .map(UnifiedFinanceWriteReceipt::Risk)
            .map_err(StoreError::Invalid)
    };
    match command {
        UnifiedFinanceWrite::Prepare {
            account,
            decision,
            intent,
        } => {
            let inserted = AccountStore::prepare_sql_transaction(
                tx,
                Some((installation, &account)),
                &decision,
                &intent,
            )?;
            let saved = AccountStore::find_intent_sql(
                tx,
                Some((installation, &account)),
                "idempotency_key",
                &intent.idempotency_key,
            )?
            .ok_or(StoreError::Invalid("UNIFIED_PREPARED_INTENT_MISSING"))?;
            if saved.request_hash != intent.request_hash {
                return Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"));
            }
            Ok(UnifiedFinanceWriteReceipt::Prepared {
                inserted,
                intent_id: saved.intent_id,
            })
        }
        UnifiedFinanceWrite::ApplyPolicy { policy } => {
            AccountStore::apply_unified_campaign_policy_sql_transaction(tx, installation, &policy)?;
            Ok(UnifiedFinanceWriteReceipt::PolicyApplied)
        }
        UnifiedFinanceWrite::InstallEntryOpening {
            source,
            census,
            now_ms,
        } => risk_operation(risk::install_entry_opening_unified_native(
            tx,
            installation,
            &source,
            &census,
            now_ms,
        )),
        UnifiedFinanceWrite::Reserve {
            campaign,
            input,
            census,
            receipt,
        } => risk_operation(risk::reserve_unified_native(
            tx,
            installation,
            &campaign,
            &input,
            &census,
            &receipt,
        )),
        UnifiedFinanceWrite::BeginPost { campaign, input } => risk_operation(
            risk::begin_post_unified_native(tx, installation, &campaign, &input),
        ),
        UnifiedFinanceWrite::SettleEntry {
            campaign,
            account,
            intent,
            expected_sequence,
            generation,
            source,
        } => risk_operation(risk::settle_entry_unified_native(
            tx,
            installation,
            &campaign,
            &account,
            &intent,
            expected_sequence,
            generation,
            source,
        )),
        UnifiedFinanceWrite::SettleBoundEntry { request } => {
            let receipt = original_bound_entry_settlement_sql(tx, installation, &request)?;
            Ok(UnifiedFinanceWriteReceipt::BoundEntrySettled(receipt))
        }
        UnifiedFinanceWrite::ReleaseBoundUnsent { request } => {
            let receipt = original_bound_unsent_release_sql(tx, installation, &request)?;
            Ok(UnifiedFinanceWriteReceipt::BoundUnsentReleased(receipt))
        }
        UnifiedFinanceWrite::ResolveTerminal { campaign, input } => risk_operation(
            risk::resolve_terminal_unified_native(tx, installation, &campaign, &input),
        ),
        UnifiedFinanceWrite::ReleaseUnsent {
            campaign,
            account,
            intent,
            expected_sequence,
            generation,
        } => risk_operation(risk::release_unsent_unified_native(
            tx,
            installation,
            &campaign,
            &account,
            &intent,
            expected_sequence,
            generation,
        )),
        UnifiedFinanceWrite::RecoverPosting { account, now } => {
            // RiskBook and AccountStore retain their original POST fences and
            // reserved capacity, but change to UNKNOWN in ONE SQL commit.
            // Any missing foreign campaign receipt rejects the whole call.
            risk::recover_posting_unified_native(tx, installation, &account)
                .map_err(StoreError::Invalid)?;
            let changed = AccountStore::recover_posting_sql_transaction(
                tx,
                Some((installation, &account)),
                now,
            )?;
            Ok(UnifiedFinanceWriteReceipt::Recovered { changed })
        }
        UnifiedFinanceWrite::RecoverInstallation { now } => {
            // The exact installed roster comes from the original account_meta
            // primary identities in this SAME financial SQLite snapshot.
            // AccountStore's own installation-wide recovery performs the
            // original receipt checks; failure rolls back the entire caller
            // transaction including all RiskBook UNKNOWN transitions.
            let accounts = {
                let mut query = tx.prepare(
                    "SELECT account_key FROM account_meta
                     WHERE installation_id=?1 ORDER BY account_key",
                )?;
                query
                    .query_map([installation], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            };
            if accounts.is_empty() {
                return Err(StoreError::Invalid("UNIFIED_RECOVERY_ACCOUNT_ROSTER_EMPTY"));
            }
            for account in accounts {
                risk::recover_posting_unified_native(tx, installation, &account)
                    .map_err(StoreError::Invalid)?;
            }
            let changed =
                AccountStore::recover_installation_posting_sql_transaction(tx, installation, now)?;
            Ok(UnifiedFinanceWriteReceipt::Recovered { changed })
        }
        UnifiedFinanceWrite::RecordOutcome {
            account,
            intent,
            state,
            exchange_order_id,
            evidence,
            now,
        } => {
            AccountStore::record_outcome_sql_transaction(
                tx,
                Some((installation, &account)),
                &intent,
                state,
                exchange_order_id.as_deref(),
                &evidence,
                now,
            )?;
            risk::reconcile_outcome_unified_native(tx, installation, &account, &intent)
                .map_err(StoreError::Invalid)?;
            Ok(UnifiedFinanceWriteReceipt::OutcomeRecorded)
        }
    }
}

/// The stopped source manifest is deliberately immutable: a valid import
/// digest is NOT evidence that the old Cloudflare financial endpoint was
/// server-disabled and all outstanding obligations were inventoried.
/// Production financial COMMIT stays disabled until RB09 supplies a new,
/// separately verified local activation receipt. This code NEVER promotes
/// OFFLINE_NOT_AUTHORIZED by itself and never calls Cloudflare on a hot path.
pub(super) fn require_verified_live_finance_authority(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
) -> StoreResult<()> {
    let state: String = tx.query_row(
        "SELECT state FROM finance_import_manifest
         WHERE singleton=1 AND installation_id=?1",
        [installation],
        |row| row.get(0),
    )?;
    if state == "OFFLINE_NOT_AUTHORIZED" {
        return Err(StoreError::Invalid("FINANCE_CUTOVER_NOT_ACCEPTED"));
    }
    Err(StoreError::Invalid("FINANCE_AUTHORITY_STATE_UNVERIFIED"))
}

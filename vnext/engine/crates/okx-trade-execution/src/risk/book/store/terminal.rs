//! Source-bound terminal Resolve on the ORIGINAL installation SQLite writer.
//! This is the final capital refund, never an alternative entry debit or a
//! venue order sender. A complete fresh native census is still required.
//! No Cloudflare grant, ledger replica, independent COMMIT or extra SQL owner.
/// Named row decoded by the explicit SELECT below; column order stays unchanged.
type TerminalReservationRow = (
    String,
    String,
    String,
    String,
    i64,
    String,
    String,
    String,
    String,
    i64,
    String,
    String,
    String,
    String,
    String,
    String,
);

/// Named row decoded by the explicit SELECT below; column order stays unchanged.
type TerminalIntentBindingRow = (
    String,
    String,
    String,
    i64,
    i64,
    String,
    String,
    String,
    String,
);

use super::{
    LEGACY_MAX_SAFE_INTEGER, Result, RiskBookStore, RiskStoreError, VerifiedCensus, amount,
    apply_census_loss_for_scope, integrity, read_meta_for_scope, valid_digest,
};
use okx_domain_core::risk::money::RiskAmount;
use rusqlite::{OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
pub(crate) struct TerminalResolveInput {
    pub account: String,
    pub intent: String,
    pub reservation: String,
    pub plan_sha256: String,
    pub generation: u64,
    pub expected_sequence: u64,
    pub now_ms: i64,
    pub census: VerifiedCensus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TerminalResolveReceipt {
    pub reservation_key: String,
    pub book_sequence: u64,
    pub replayed: bool,
}

fn sql_source(error: crate::StoreError) -> RiskStoreError {
    match error {
        crate::StoreError::Sql(error) => RiskStoreError::Sql(error),
        crate::StoreError::Io(error) => RiskStoreError::Io(error),
        crate::StoreError::Invalid(reason) => RiskStoreError::Invalid(reason),
    }
}

// Preserve the first failed temporal boundary instead of collapsing every
// cause into COMPLETE_ACCOUNT_CENSUS_UNPROVEN. These are the original guards.
fn terminal_census_time_error(
    read_floor: i64,
    generated: i64,
    post_at: i64,
    settled_through: i64,
    now: i64,
) -> Option<&'static str> {
    if generated <= 0 || now <= 0 || now as u64 > LEGACY_MAX_SAFE_INTEGER {
        Some("TERMINAL_CENSUS_TIME_INVALID")
    } else if read_floor <= post_at {
        Some("TERMINAL_CENSUS_NOT_AFTER_POST")
    } else if read_floor < settled_through {
        Some("TERMINAL_CENSUS_PRECEDES_SETTLEMENT_SOURCE")
    } else if read_floor > generated {
        Some("TERMINAL_CENSUS_FLOOR_AFTER_GENERATION")
    } else if generated > now {
        Some("TERMINAL_CENSUS_GENERATED_IN_FUTURE")
    } else if now - generated > 5_000 {
        Some("TERMINAL_CENSUS_EXPIRED")
    } else {
        None
    }
}

impl RiskBookStore {
    /// A scoped native terminal release is accepted ONLY after the original
    /// AccountStore terminal, immutable entry-spend revision and its ACK are
    /// already durable in this SAME SQLite. All reservations and AccountStore
    /// release receipts change together in the caller's one COMMIT.
    pub(in crate::risk::book) fn resolve_unified_terminal_from_account_sql_transaction(
        tx: &Transaction<'_>,
        installation: &str,
        campaign: &str,
        input: &TerminalResolveInput,
    ) -> Result<TerminalResolveReceipt> {
        if installation.is_empty()
            || campaign.is_empty()
            || !input
                .account
                .strip_prefix("okx:real:")
                .is_some_and(valid_digest)
            || input.intent.is_empty()
            || input.reservation.is_empty()
            || input.reservation.len() > 512
            || !valid_digest(&input.plan_sha256)
            || input.generation == 0
            || input.generation > LEGACY_MAX_SAFE_INTEGER
            || input.expected_sequence == 0
            || input.expected_sequence > LEGACY_MAX_SAFE_INTEGER
        {
            return Err(RiskStoreError::Invalid("TERMINAL_RESOLVE_SCOPE_INVALID"));
        }
        let scope = Some((installation, campaign));
        let mut meta = read_meta_for_scope(tx, scope)?;
        integrity::verify_reservation_aggregates_for_scope(tx, &meta, scope)?;
        if meta.generation != input.generation {
            return Err(RiskStoreError::StaleGeneration);
        }
        let original: Option<TerminalIntentBindingRow> = tx
            .query_row(
                "SELECT i.plan_hash,i.state,i.request_hash,i.risk_increase,
                    COALESCE(i.begin_post_at,0),i.instrument_id,
                    r.state,r.receipt_hash,p.campaign_id
             FROM intents i
             JOIN risk_receipts r ON r.installation_id=i.installation_id
                 AND r.account_key=i.account_key AND r.intent_id=i.intent_id
             JOIN account_policy p ON p.installation_id=i.installation_id
                 AND p.account_key=i.account_key AND p.singleton=1
             WHERE i.installation_id=?1 AND i.account_key=?2 AND i.intent_id=?3
               AND r.reservation_key=?4",
                params![
                    installation,
                    &input.account,
                    &input.intent,
                    &input.reservation
                ],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            plan,
            intent_state,
            request_hash,
            risk_increase,
            begin_post_at,
            instrument,
            receipt_state,
            _receipt_hash,
            assigned,
        )) = original
        else {
            return Err(RiskStoreError::EntrySqlUnproven);
        };
        if assigned != campaign
            || plan != input.plan_sha256
            || risk_increase != 1
            || begin_post_at <= 0
            || !matches!(intent_state.as_str(), "FILLED" | "CANCELLED")
        {
            return Err(RiskStoreError::Invalid(
                "TERMINAL_RESOLVE_ORIGINAL_INTENT_UNPROVEN",
            ));
        }
        let entry: Option<TerminalReservationRow> = tx
            .query_row(
                "SELECT account_key,intent_id,plan_sha256,state,post_authorized_at_ms,
                    strategy_id,reserved_usdt,reserved_loss_usd,reserved_entry_usd,
                    entry_settlement_revision,entry_settlement_digest,
                    entry_settled_usd,release_proof_digest,release_request_hash,
                    release_reason,entry_upper_usd
             FROM risk_entries
             WHERE installation_id=?1 AND campaign_id=?2 AND reservation_key=?3",
                params![installation, campaign, &input.reservation],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                        row.get(10)?,
                        row.get(11)?,
                        row.get(12)?,
                        row.get(13)?,
                        row.get(14)?,
                        row.get(15)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            entry_account,
            entry_intent,
            entry_plan,
            state,
            post_at,
            strategy,
            reserved_usdt,
            reserved_loss,
            reserved_entry,
            settlement_revision,
            settlement_digest,
            settled,
            release_digest,
            release_hash,
            release_reason,
            _upper,
        )) = entry
        else {
            return Err(RiskStoreError::EntrySqlUnproven);
        };
        if entry_account != input.account
            || entry_intent != input.intent
            || entry_plan != plan
            || post_at <= 0
            || post_at < begin_post_at
        {
            return Err(RiskStoreError::Invalid(
                "TERMINAL_RESOLVE_RISK_IDENTITY_UNPROVEN",
            ));
        }
        // An already released booking may replay after a lost ACK and after
        // subsequent late-fee revisions; no new census or CAS is authorized.
        let release_id = format!("central-release:{}", input.reservation);
        if state == "RELEASED" {
            let old: Option<(String, String, String)> = tx
                .query_row(
                    "SELECT kind,request_hash,result_ref FROM operation_receipts
                 WHERE installation_id=?1 AND account_key=?2 AND operation_id=?3",
                    params![installation, &input.account, &release_id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            if receipt_state == "RELEASED"
                && matches!(old,Some((ref kind,ref hash,ref digest))
                    if kind == "CENTRAL_RELEASE" && hash == &request_hash
                        && digest == &release_digest && valid_digest(digest))
                && release_hash == request_hash
                && release_reason == "terminal-flat-census"
            {
                return Ok(TerminalResolveReceipt {
                    reservation_key: input.reservation.clone(),
                    book_sequence: meta.sequence,
                    replayed: true,
                });
            }
            return Err(RiskStoreError::IdempotencyConflict);
        }
        if state != "OPEN"
            || !matches!(receipt_state.as_str(), "OPEN" | "TERMINAL_PENDING_CENTRAL")
            || input.expected_sequence != meta.sequence
        {
            return Err(RiskStoreError::AlreadyResolved);
        }
        // Read the ORIGINAL immutable AccountStore terminal expense source,
        // which has already validated fills/bills, FX and its own revision.
        let source = crate::AccountStore::final_entry_settlement_receipt_sql(
            tx,
            installation,
            &input.account,
            &input.intent,
            false,
        )
        .map_err(sql_source)?
        .ok_or(RiskStoreError::EntrySqlUnproven)?;
        if source.plan_hash != plan
            || source.reservation_key != input.reservation
            || source.settlement_revision
                != u64::try_from(settlement_revision)
                    .map_err(|_| RiskStoreError::Corrupt("TERMINAL_SETTLEMENT_REVISION"))?
            || source.source_digest != settlement_digest
            || amount(settled.clone())?
                != RiskAmount::parse(&source.cumulative_entry_usd.normalize().to_string())?
        {
            return Err(RiskStoreError::Invalid(
                "TERMINAL_ENTRY_DEBIT_NOT_ACKNOWLEDGED",
            ));
        }
        let settled_id = format!(
            "entry-terminal-settled:{}:r{}",
            input.intent, source.settlement_revision
        );
        let acknowledged: Option<(String, String, String)> = tx
            .query_row(
                "SELECT kind,request_hash,result_ref FROM operation_receipts
             WHERE installation_id=?1 AND account_key=?2 AND operation_id=?3",
                params![installation, &input.account, &settled_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        if !matches!(acknowledged,Some((ref kind,ref hash,ref digest))
                if kind == "ENTRY_TERMINAL_SETTLED"
                    && hash == &source.plan_hash && digest == &source.source_digest)
        {
            return Err(RiskStoreError::Invalid(
                "TERMINAL_ENTRY_DEBIT_NOT_ACKNOWLEDGED",
            ));
        }
        let terminal_id = format!("terminal:{}", input.intent);
        let terminal: Option<(String, String, String)> = tx
            .query_row(
                "SELECT kind,request_hash,result_ref FROM operation_receipts
             WHERE installation_id=?1 AND account_key=?2 AND operation_id=?3",
                params![installation, &input.account, &terminal_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let Some((terminal_kind, terminal_hash, terminal_evidence)) = terminal else {
            return Err(RiskStoreError::EntrySqlUnproven);
        };
        if terminal_kind != "TERMINAL"
            || terminal_hash != request_hash
            || terminal_evidence.is_empty()
        {
            return Err(RiskStoreError::Invalid("TERMINAL_VENUE_OUTCOME_UNPROVEN"));
        }
        // Original local lots and the fresh native full-cohort venue census
        // must BOTH report this account flat before capital can be refunded.
        let local_lots: i64 = tx.query_row(
            "SELECT COUNT(*) FROM position_lots
             WHERE installation_id=?1 AND account_key=?2
               AND instrument_id=?3 AND remaining_qty!='0'",
            params![installation, &input.account, &instrument],
            |r| r.get(0),
        )?;
        if local_lots != 0 {
            return Err(RiskStoreError::Invalid("TERMINAL_LOCAL_EXPOSURE_REMAINS"));
        }
        let census = &input.census;
        if let Some(reason) = terminal_census_time_error(
            census.read_floor_ms,
            census.generated_at_ms,
            post_at,
            source.source_covered_through_ms,
            input.now_ms,
        ) {
            return Err(RiskStoreError::Invalid(reason));
        }
        if !census.complete
            || census.generation != meta.generation
            || !valid_digest(&census.source_digest)
            || census
                .occupied
                .get(&input.account)
                .is_none_or(|v| !v.is_zero())
        {
            return Err(RiskStoreError::CensusUnproven);
        }
        let keys = {
            let mut stmt = tx.prepare(
                "SELECT account_key FROM risk_policy_account
                 WHERE installation_id=?1 AND campaign_id=?2 ORDER BY account_key",
            )?;
            stmt.query_map(params![installation, campaign], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        if keys.is_empty()
            || keys.iter().ne(census.occupied.keys())
            || keys.iter().ne(census.cumulative_loss_usd.keys())
        {
            return Err(RiskStoreError::CensusUnproven);
        }
        // A terminal no-fill still has revision 1 with zero consumed entry.
        // An unverified newer late bill remains pending; this operation never
        // refunds entry capacity until original cumulative debit is accepted.
        let release_digest = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(
                "okx.native-terminal-release/v1",
                installation,
                campaign,
                &input.account,
                &input.intent,
                &input.reservation,
                &plan,
                &source.source_digest,
                &terminal_evidence,
                &census.source_digest,
            ))?)
        );
        // The original risk algorithm owns monotonic per-account loss watermarks
        // and campaign consumed loss. A later error rolls this back with the
        // remainder of the same finance transaction.
        apply_census_loss_for_scope(tx, &mut meta, census, scope)?;
        let reserved_usdt = amount(reserved_usdt)?;
        let reserved_loss = amount(reserved_loss)?;
        let reserved_entry = amount(reserved_entry)?;
        let account_total: String = tx.query_row(
            "SELECT reserved_usdt FROM risk_account_occupied
             WHERE installation_id=?1 AND campaign_id=?2 AND account_key=?3",
            params![installation, campaign, &input.account],
            |r| r.get(0),
        )?;
        let strategy_total: String = tx.query_row(
            "SELECT reserved_usdt FROM risk_strategy_occupied
             WHERE installation_id=?1 AND campaign_id=?2
               AND account_key=?3 AND strategy_id=?4",
            params![installation, campaign, &input.account, &strategy],
            |r| r.get(0),
        )?;
        let next = meta
            .sequence
            .checked_add(1)
            .filter(|v| *v <= LEGACY_MAX_SAFE_INTEGER)
            .ok_or(RiskStoreError::Corrupt("SEQUENCE_OVERFLOW"))?;
        let next_i64 =
            i64::try_from(next).map_err(|_| RiskStoreError::Corrupt("SEQUENCE_OVERFLOW"))?;
        let changed = tx.execute(
            "UPDATE risk_entries SET state='RELEASED',
                reserved_usdt='0',reserved_loss_usd='0',reserved_entry_usd='0',
                release_proof_digest=?1,release_request_hash=?2,
                release_reason='terminal-flat-census'
             WHERE installation_id=?3 AND campaign_id=?4
               AND reservation_key=?5 AND account_key=?6 AND intent_id=?7
               AND state='OPEN' AND entry_settlement_revision=?8
               AND entry_settlement_digest=?9",
            params![
                &release_digest,
                &request_hash,
                installation,
                campaign,
                &input.reservation,
                &input.account,
                &input.intent,
                settlement_revision,
                &settlement_digest
            ],
        )?;
        if changed != 1 {
            return Err(RiskStoreError::StaleSequence);
        }
        let changed = tx.execute(
            "UPDATE risk_meta SET book_sequence=?1,global_reserved_usdt=?2,
                loss_reserved_usd=?3,entry_reserved_usd=?4
             WHERE installation_id=?5 AND campaign_id=?6 AND singleton=1
               AND book_sequence=?7 AND generation=?8",
            params![
                next_i64,
                meta.global_reserved
                    .checked_sub(&reserved_usdt)?
                    .canonical()?,
                meta.loss_reserved
                    .checked_sub(&reserved_loss)?
                    .canonical()?,
                meta.entry_reserved
                    .checked_sub(&reserved_entry)?
                    .canonical()?,
                installation,
                campaign,
                i64::try_from(meta.sequence).map_err(|_| RiskStoreError::StaleSequence)?,
                i64::try_from(meta.generation).map_err(|_| RiskStoreError::StaleGeneration)?
            ],
        )?;
        if changed != 1 {
            return Err(RiskStoreError::StaleSequence);
        }
        let changed = tx.execute(
            "UPDATE risk_account_occupied SET reserved_usdt=?1
             WHERE installation_id=?2 AND campaign_id=?3 AND account_key=?4",
            params![
                amount(account_total)?
                    .checked_sub(&reserved_usdt)?
                    .canonical()?,
                installation,
                campaign,
                &input.account
            ],
        )?;
        if changed != 1 {
            return Err(RiskStoreError::Corrupt("TERMINAL_ACCOUNT_OCCUPANCY"));
        }
        let changed = tx.execute(
            "UPDATE risk_strategy_occupied SET reserved_usdt=?1
             WHERE installation_id=?2 AND campaign_id=?3
               AND account_key=?4 AND strategy_id=?5",
            params![
                amount(strategy_total)?
                    .checked_sub(&reserved_usdt)?
                    .canonical()?,
                installation,
                campaign,
                &input.account,
                &strategy
            ],
        )?;
        if changed != 1 {
            return Err(RiskStoreError::Corrupt("TERMINAL_STRATEGY_OCCUPANCY"));
        }
        let changed = tx.execute(
            "UPDATE risk_receipts SET state='RELEASED'
             WHERE installation_id=?1 AND account_key=?2
               AND intent_id=?3 AND reservation_key=?4
               AND state IN('OPEN','TERMINAL_PENDING_CENTRAL')",
            params![
                installation,
                &input.account,
                &input.intent,
                &input.reservation
            ],
        )?;
        if changed != 1 {
            return Err(RiskStoreError::StaleSequence);
        }
        tx.execute(
            "INSERT INTO operation_receipts(
               installation_id,account_key,operation_id,kind,
               request_hash,result_ref,committed_at)
             VALUES(?1,?2,?3,'CENTRAL_RELEASE',?4,?5,?6)
             ON CONFLICT(installation_id,account_key,operation_id) DO NOTHING",
            params![
                installation,
                &input.account,
                &release_id,
                &request_hash,
                &release_digest,
                input.now_ms
            ],
        )?;
        let observed: (String, String, String) = tx.query_row(
            "SELECT kind,request_hash,result_ref FROM operation_receipts
             WHERE installation_id=?1 AND account_key=?2 AND operation_id=?3",
            params![installation, &input.account, &release_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        if observed != ("CENTRAL_RELEASE".into(), request_hash, release_digest) {
            return Err(RiskStoreError::IdempotencyConflict);
        }
        let accepted = read_meta_for_scope(tx, scope)?;
        integrity::verify_reservation_aggregates_for_scope(tx, &accepted, scope)?;
        Ok(TerminalResolveReceipt {
            reservation_key: input.reservation.clone(),
            book_sequence: accepted.sequence,
            replayed: false,
        })
    }
}

#[cfg(test)]
mod time_tests {
    use super::terminal_census_time_error as check;

    #[test]
    fn risk_book_contract_terminal_census_reports_exact_time_boundary() {
        assert_eq!(check(101, 102, 100, 101, 103), None);
        assert_eq!(
            check(100, 102, 100, 101, 103),
            Some("TERMINAL_CENSUS_NOT_AFTER_POST")
        );
        assert_eq!(
            check(101, 102, 100, 102, 103),
            Some("TERMINAL_CENSUS_PRECEDES_SETTLEMENT_SOURCE")
        );
        assert_eq!(
            check(103, 102, 100, 101, 104),
            Some("TERMINAL_CENSUS_FLOOR_AFTER_GENERATION")
        );
        assert_eq!(
            check(101, 104, 100, 101, 103),
            Some("TERMINAL_CENSUS_GENERATED_IN_FUTURE")
        );
        assert_eq!(
            check(101, 102, 100, 101, 5103),
            Some("TERMINAL_CENSUS_EXPIRED")
        );
        assert_eq!(check(101, 102, 100, 101, 5102), None);
        assert_eq!(
            check(101, 102, 100, 101, 0),
            Some("TERMINAL_CENSUS_TIME_INVALID")
        );
    }
}

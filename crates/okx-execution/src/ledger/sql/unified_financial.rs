//! #510 r4: one-file account-source → campaign-budget transaction slice.
//! The existing financial SQL owner supplies the Transaction. There is NO
//! second Connection, writer, account ACK, venue client or bootstrap here.
//! This first slice cannot be production-enabled until the full v2 account
//! schema is scoped/imported and original source producers are wired.
use crate::{StoreError, StoreResult};
use okx_kernel::risk::money::RiskAmount;
use rusqlite::{OptionalExtension, Transaction, params};

pub(super) const CORE_SCHEMA: &str = include_str!("finance-v1.sql");
const MAX_SAFE_SEQUENCE: i64 = 9_007_199_254_740_991;

#[derive(Debug, Clone)]
pub(in crate::account_store) struct EntryRef<'a> {
    pub installation: &'a str,
    pub campaign: &'a str,
    pub reservation: &'a str,
    pub account: &'a str,
    pub intent: &'a str,
    pub source_revision: i64,
    pub expected_sequence: i64,
    pub generation: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::account_store) struct EntryCommit {
    pub book_sequence: i64,
    pub cumulative_entry_usd: String,
    pub replayed: bool,
}

fn money(raw: &str) -> StoreResult<RiskAmount> {
    RiskAmount::parse(raw).map_err(|_| StoreError::Invalid("UNIFIED_MONEY_INVALID"))
}
fn canonical(value: &RiskAmount) -> StoreResult<String> {
    value
        .canonical()
        .map_err(|_| StoreError::Invalid("UNIFIED_MONEY_RANGE"))
}
fn digest_ok(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
fn checked_add(a: &RiskAmount, b: &RiskAmount) -> StoreResult<RiskAmount> {
    a.checked_add(b)
        .map_err(|_| StoreError::Invalid("UNIFIED_MONEY_RANGE"))
}
fn checked_sub(a: &RiskAmount, b: &RiskAmount) -> StoreResult<RiskAmount> {
    a.checked_sub(b)
        .map_err(|_| StoreError::Invalid("UNIFIED_AMOUNT_REGRESSION"))
}
fn remainder(upper: &RiskAmount, used: &RiskAmount) -> StoreResult<RiskAmount> {
    if upper > used {
        checked_sub(upper, used)
    } else {
        Ok(RiskAmount::zero())
    }
}
fn overage(value: &RiskAmount, limit: &RiskAmount) -> StoreResult<RiskAmount> {
    if value > limit {
        checked_sub(value, limit)
    } else {
        Ok(RiskAmount::zero())
    }
}

/// Scope-keyed primary rows are checked before a mutable campaign cache may
/// grant headroom. This is an O(active entries) proof for the initial slice;
/// production performance/index evidence remains a checkpoint, not assumed.
fn verify_campaign(
    tx: &Transaction<'_>,
    scope: &EntryRef<'_>,
    opening: &RiskAmount,
    consumed: &RiskAmount,
    reserved: &RiskAmount,
) -> StoreResult<()> {
    let mut source_total = opening.clone();
    let mut active_reserved = RiskAmount::zero();
    let mut stmt = tx.prepare(
        "SELECT reservation_key,state,entry_settled_usd,reserved_entry_usd,entry_upper_usd,
                settlement_revision,source_digest
         FROM finance_risk_entries
         WHERE installation_id=?1 AND campaign_id=?2",
    )?;
    let rows = stmt.query_map(params![scope.installation, scope.campaign], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, String>(4)?,
            r.get::<_, i64>(5)?,
            r.get::<_, String>(6)?,
        ))
    })?;
    for row in rows {
        let (reservation_key, state, used, remaining, upper, revision, digest) = row?;
        let used = money(&used)?;
        let remaining = money(&remaining)?;
        let upper = money(&upper)?;
        if revision == 0 {
            if !used.is_zero() || !digest.is_empty() {
                return Err(StoreError::Invalid("UNIFIED_PRIMARY_REVISION_CORRUPT"));
            }
        } else if revision < 0 || revision > MAX_SAFE_SEQUENCE || !digest_ok(&digest) {
            return Err(StoreError::Invalid("UNIFIED_PRIMARY_REVISION_CORRUPT"));
        }
        if revision > 0 {
            // Original immutable SQL receipt must back every materialized
            // budget charge, not only a queried replay.
            let receipt: Option<(String, String, i64)> = tx
                .query_row(
                    "SELECT source_digest,cumulative_entry_usd,book_sequence
                 FROM finance_entry_receipts
                 WHERE installation_id=?1 AND campaign_id=?2
                   AND reservation_key=?3 AND settlement_revision=?4",
                    params![
                        scope.installation,
                        scope.campaign,
                        reservation_key,
                        revision
                    ],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            match receipt {
                Some((record_digest, record_usd, book_seq))
                    if record_digest == digest
                        && money(&record_usd)? == used
                        && book_seq > 0
                        && book_seq <= MAX_SAFE_SEQUENCE => {}
                _ => return Err(StoreError::Invalid("UNIFIED_PRIMARY_RECEIPT_CORRUPT")),
            }
        }
        if state != "RELEASED" {
            if remaining != remainder(&upper, &used)? {
                return Err(StoreError::Invalid("UNIFIED_PRIMARY_RESERVED_CORRUPT"));
            }
            active_reserved = checked_add(&active_reserved, &remaining)?;
        }
        source_total = checked_add(&source_total, &used)?;
    }
    if &source_total != consumed || &active_reserved != reserved {
        return Err(StoreError::Invalid("UNIFIED_CAMPAIGN_AGGREGATES_CORRUPT"));
    }
    Ok(())
}

/// This function accepts no caller-supplied money, fill, fee or FX values.
/// It reads the original immutable source revision, intent, fill and bill
/// from the SAME SQLite snapshot as the campaign CAS. Existing source
/// capture must populate these primary rows before this can be used live.
pub(in crate::account_store) fn settle_entry(
    tx: &Transaction<'_>,
    scope: &EntryRef<'_>,
) -> StoreResult<EntryCommit> {
    if [
        scope.installation,
        scope.campaign,
        scope.reservation,
        scope.account,
        scope.intent,
    ]
    .iter()
    .any(|s| s.is_empty())
        || scope.source_revision <= 0
        || scope.source_revision > MAX_SAFE_SEQUENCE
        || scope.generation <= 0
        || scope.expected_sequence < 0
        || scope.expected_sequence > MAX_SAFE_SEQUENCE
    {
        return Err(StoreError::Invalid("UNIFIED_SCOPE_INVALID"));
    }

    let campaign: Option<(i64, i64, i64, String, String, String, String, String)> = tx
        .query_row(
            "SELECT policy_revision,generation,book_sequence,entry_limit_usd,
                    entry_consumed_usd,entry_opening_consumed_usd,entry_reserved_usd,entry_breach_usd
             FROM finance_campaigns WHERE installation_id=?1 AND campaign_id=?2",
            params![scope.installation, scope.campaign],
            |r| Ok((
                r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?,
                r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?,
            )),
        )
        .optional()?;
    let Some((policy, generation, sequence, limit, consumed, opening, reserved, breach)) = campaign
    else {
        return Err(StoreError::Invalid("UNIFIED_CAMPAIGN_UNAVAILABLE"));
    };
    if policy <= 0 || generation != scope.generation {
        return Err(StoreError::Invalid("UNIFIED_CAMPAIGN_STALE"));
    }
    let limit = money(&limit)?;
    let consumed = money(&consumed)?;
    let opening = money(&opening)?;
    let reserved = money(&reserved)?;
    let prior_breach = money(&breach)?;
    verify_campaign(tx, scope, &opening, &consumed, &reserved)?;

    let row: Option<(String, String, String, String, String, String, i64, String)> = tx
        .query_row(
            "SELECT account_key,intent_id,plan_sha256,state,entry_upper_usd,
                    entry_settled_usd,settlement_revision,source_digest
             FROM finance_risk_entries
             WHERE installation_id=?1 AND campaign_id=?2 AND reservation_key=?3",
            params![scope.installation, scope.campaign, scope.reservation],
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
                ))
            },
        )
        .optional()?;
    let Some((account, intent, plan, state, upper, previous, old_rev, old_digest)) = row else {
        return Err(StoreError::Invalid("UNIFIED_RESERVATION_MISSING"));
    };
    if account != scope.account || intent != scope.intent {
        return Err(StoreError::Invalid("UNIFIED_ACCOUNT_OR_INTENT_MISMATCH"));
    }
    if state != "POSTED" || old_rev < 0 {
        return Err(StoreError::Invalid("UNIFIED_ENTRY_NOT_POSTED"));
    }
    let intent_identity: (String, String) = tx.query_row(
        "SELECT plan_sha256,exchange_order_id FROM finance_intents
         WHERE installation_id=?1 AND account_key=?2 AND intent_id=?3",
        params![scope.installation, scope.account, scope.intent],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let (intent_plan, intent_exchange) = intent_identity;
    if intent_plan != plan || !digest_ok(&plan) {
        return Err(StoreError::Invalid("UNIFIED_ORIGINAL_PLAN_CONFLICT"));
    }

    // All joins are scoped by BOTH installation and source account. Even
    // identical venue order/fill/bill IDs on a different account cannot
    // become a source for this risk reservation.
    let source: Option<(String, String, String, String, String)> = tx
        .query_row(
            "SELECT s.source_digest,s.cumulative_entry_usd,b.source_digest,s.original_receipt_json,
                    f.exchange_order_id
             FROM finance_entry_sources s
             JOIN finance_fills f
               ON f.installation_id=s.installation_id AND f.account_key=s.account_key
              AND f.fill_id=s.fill_id AND f.intent_id=s.intent_id
             JOIN finance_bills b
               ON b.installation_id=s.installation_id AND b.account_key=s.account_key
              AND b.bill_id=s.bill_id AND b.fill_id=s.fill_id
             WHERE s.installation_id=?1 AND s.account_key=?2
               AND s.intent_id=?3 AND s.settlement_revision=?4",
            params![
                scope.installation,
                scope.account,
                scope.intent,
                scope.source_revision
            ],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .optional()?;
    let Some((source_digest, source_amount, bill_digest, original_json, fill_order)) = source else {
        return Err(StoreError::Invalid("UNIFIED_ORIGINAL_SOURCE_MISSING"));
    };
    if !digest_ok(&source_digest)
        || !digest_ok(&bill_digest)
        || fill_order != intent_exchange
    {
        return Err(StoreError::Invalid("UNIFIED_SOURCE_DIGEST_INVALID"));
    }
    let cumulative = money(&source_amount)?;
    // Legacy-imported rows must carry the byte-exact original AccountStore
    // proof. Synthetic scope/OS tests use only test-seeded source rows; the
    // complete production schema must require this native provenance.
    if !original_json.is_empty() {
        let original: crate::EntrySettlementEvidence = serde_json::from_str(&original_json)
            .map_err(|_| StoreError::Invalid("UNIFIED_ORIGINAL_JSON_CORRUPT"))?;
        let original_amount = money(&original.cumulative_entry_usd.normalize().to_string())?;
        if original.schema != "okx.entry-partial-settlement/v1"
            || original.account_key != scope.account
            || original.intent_id != scope.intent
            || original.plan_hash != plan
            || original.reservation_key != scope.reservation
            || original.venue_order_id != intent_exchange
            || original.source_digest != source_digest
            || bill_digest != source_digest
            || i64::try_from(original.settlement_revision).ok() != Some(scope.source_revision)
            || original_amount != cumulative
        {
            return Err(StoreError::Invalid("UNIFIED_ORIGINAL_RECEIPT_CONFLICT"));
        }
    }
    if cumulative.is_zero() {
        return Err(StoreError::Invalid("UNIFIED_SOURCE_EMPTY"));
    }
    let previous = money(&previous)?;
    let upper = money(&upper)?;
    if scope.source_revision == old_rev {
        let previous_receipt: Option<(String, String, i64)> = tx
            .query_row(
                "SELECT source_digest,cumulative_entry_usd,book_sequence
                 FROM finance_entry_receipts
                 WHERE installation_id=?1 AND campaign_id=?2 AND reservation_key=?3
                   AND settlement_revision=?4",
                params![
                    scope.installation,
                    scope.campaign,
                    scope.reservation,
                    scope.source_revision
                ],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        return match previous_receipt {
            Some((digest, amount, receipt_seq))
                if source_digest == old_digest
                    && digest == source_digest
                    && money(&amount)? == cumulative
                    && previous == cumulative
                    && receipt_seq > 0
                    && receipt_seq <= sequence
                    && scope.expected_sequence <= sequence =>
            {
                Ok(EntryCommit {
                    book_sequence: sequence,
                    cumulative_entry_usd: canonical(&cumulative)?,
                    replayed: true,
                })
            }
            _ => Err(StoreError::Invalid("UNIFIED_RECEIPT_REPLAY_CONFLICT")),
        };
    }
    if sequence != scope.expected_sequence {
        return Err(StoreError::Invalid("UNIFIED_STALE_SEQUENCE"));
    }
    if old_rev >= MAX_SAFE_SEQUENCE
        || scope.source_revision != old_rev + 1
        || cumulative < previous
        || source_digest == old_digest
    {
        return Err(StoreError::Invalid("UNIFIED_SETTLEMENT_REGRESSION"));
    }
    if sequence >= MAX_SAFE_SEQUENCE {
        return Err(StoreError::Invalid("UNIFIED_SEQUENCE_OVERFLOW"));
    }

    let previously_reserved = remainder(&upper, &previous)?;
    let next_reserved = remainder(&upper, &cumulative)?;
    let old_reservation: String = tx.query_row(
        "SELECT reserved_entry_usd FROM finance_risk_entries
         WHERE installation_id=?1 AND campaign_id=?2 AND reservation_key=?3",
        params![scope.installation, scope.campaign, scope.reservation],
        |r| r.get(0),
    )?;
    if money(&old_reservation)? != previously_reserved {
        return Err(StoreError::Invalid("UNIFIED_PRIMARY_RESERVED_CORRUPT"));
    }
    let delta = checked_sub(&cumulative, &previous)?;
    let consumed_after = checked_add(&consumed, &delta)?;
    let reserved_after = checked_sub(
        &reserved,
        &checked_sub(&previously_reserved, &next_reserved)?,
    )?;
    let breached_by_total = if RiskAmount::sum_exceeds(&[&consumed_after, &reserved_after], &limit)
    {
        overage(&checked_add(&consumed_after, &reserved_after)?, &limit)?
    } else {
        RiskAmount::zero()
    };
    let breached_by_entry = overage(&cumulative, &upper)?;
    let breach_after = [&prior_breach, &breached_by_total, &breached_by_entry]
        .into_iter()
        .max()
        .expect("nonempty loss limit candidates")
        .clone();
    let next_sequence = sequence + 1;
    if tx.execute(
        "UPDATE finance_risk_entries
         SET entry_settled_usd=?1,reserved_entry_usd=?2,settlement_revision=?3,source_digest=?4
         WHERE installation_id=?5 AND campaign_id=?6 AND reservation_key=?7
           AND settlement_revision=?8 AND state='POSTED'",
        params![
            canonical(&cumulative)?,
            canonical(&next_reserved)?,
            scope.source_revision,
            &source_digest,
            scope.installation,
            scope.campaign,
            scope.reservation,
            old_rev,
        ],
    )? != 1
    {
        return Err(StoreError::Invalid("UNIFIED_RESERVATION_CAS_CONFLICT"));
    }
    if tx.execute(
        "UPDATE finance_campaigns
         SET book_sequence=?1,entry_consumed_usd=?2,entry_reserved_usd=?3,entry_breach_usd=?4
         WHERE installation_id=?5 AND campaign_id=?6 AND generation=?7 AND book_sequence=?8",
        params![
            next_sequence,
            canonical(&consumed_after)?,
            canonical(&reserved_after)?,
            canonical(&breach_after)?,
            scope.installation,
            scope.campaign,
            scope.generation,
            scope.expected_sequence,
        ],
    )? != 1
    {
        return Err(StoreError::Invalid("UNIFIED_CAMPAIGN_CAS_CONFLICT"));
    }
    tx.execute(
        "INSERT INTO finance_entry_receipts
         (installation_id,campaign_id,reservation_key,account_key,intent_id,
          settlement_revision,source_digest,cumulative_entry_usd,book_sequence)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
        params![
            scope.installation,
            scope.campaign,
            scope.reservation,
            scope.account,
            scope.intent,
            scope.source_revision,
            source_digest,
            canonical(&cumulative)?,
            next_sequence,
        ],
    )?;
    Ok(EntryCommit {
        book_sequence: next_sequence,
        cumulative_entry_usd: canonical(&cumulative)?,
        replayed: false,
    })
}

/// A frozen, original AccountStore receipt and its one-to-one source links.
/// This is import evidence, not an alternative producer of monetary amounts.
/// Multi-fill/late histories require the complete future normalized importer.
#[derive(Debug, Clone)]
pub(in crate::account_store) struct LegacyPartialSource {
    receipt: crate::EntrySettlementEvidence,
    installation: String,
    fill_id: String,
    trade_id: String,
    instrument_id: String,
    bill_id: String,
}

/// Read the SAME AccountStore authority already computing fills, fees and FX.
/// Reject unverifiable links instead of rebuilding finance calculations.
pub(in crate::account_store) fn inspect_legacy_partial_source(
    account: &mut crate::AccountStore,
    intent_id: &str,
) -> StoreResult<LegacyPartialSource> {
    let receipt = account
        .partial_entry_settlement_receipt(intent_id)?
        .ok_or(StoreError::Invalid("UNIFIED_ORIGINAL_RECEIPT_MISSING"))?;
    // Re-evaluate the original immutable captured evidence using its real
    // existing owner; a cached head alone is not an authenticated producer.
    let verified = account.bind_partial_entry_settlement(
        intent_id,
        receipt.venue_update_ms,
        receipt.venue_filled_quantity,
        receipt.source_covered_through_ms,
    )?;
    if verified != receipt || receipt.account_key != account.meta.account_key {
        return Err(StoreError::Invalid("UNIFIED_ORIGINAL_SOURCE_CHANGED"));
    }
    let mut stmt = account.connection.prepare(
        "SELECT f.fill_id,f.trade_id,f.instrument_id,b.bill_id
         FROM fills f
         JOIN finance_source_links lfill ON lfill.source_ref='fill:'||f.fill_id
         JOIN finance_entries e ON e.entry_id=lfill.entry_id
                               AND e.component='FEE' AND e.allocation_leg='account'
         JOIN finance_source_links lbill ON lbill.entry_id=e.entry_id
         JOIN bills b ON lbill.source_ref='bill:'||b.bill_id
         WHERE f.intent_id=?1 AND f.exchange_order_id=?2
           AND f.time<=?3 AND b.time<=?3
         ORDER BY f.fill_id,b.bill_id LIMIT 2",
    )?;
    let mut rows = stmt.query(params![
        intent_id,
        &receipt.venue_order_id,
        receipt.source_covered_through_ms
    ])?;
    let Some(first) = rows.next()? else {
        return Err(StoreError::Invalid("UNIFIED_LINKED_FILL_BILL_MISSING"));
    };
    let (fill_id, trade_id, instrument_id, bill_id): (String, String, String, String) =
        (first.get(0)?, first.get(1)?, first.get(2)?, first.get(3)?);
    if rows.next()?.is_some() {
        return Err(StoreError::Invalid(
            "UNIFIED_MULTI_SOURCE_IMPORT_NOT_SUPPORTED",
        ));
    }
    Ok(LegacyPartialSource {
        receipt,
        installation: account.meta.installation_id.clone(),
        fill_id,
        trade_id,
        instrument_id,
        bill_id,
    })
}

/// Import one already-authenticated original source under the FINAL new
/// account/campaign scope into the destination's single SQLite transaction.
/// The historical account writer must be fenced before a real migration;
/// this function does not create/free budget or transfer the ledger itself.
pub(in crate::account_store) fn import_legacy_partial_source(
    tx: &Transaction<'_>,
    source: &LegacyPartialSource,
) -> StoreResult<()> {
    let proof = &source.receipt;
    if source.installation.is_empty()
        || proof.account_key.is_empty()
        || proof.intent_id.is_empty()
        || proof.reservation_key.is_empty()
        || proof.venue_order_id.is_empty()
        || source.fill_id.is_empty()
        || source.bill_id.is_empty()
        || source.trade_id.is_empty()
        || source.instrument_id.is_empty()
        || proof.schema != "okx.entry-partial-settlement/v1"
        || proof.settlement_revision != 1
        || !digest_ok(&proof.source_digest)
        || !digest_ok(&proof.plan_hash)
    {
        return Err(StoreError::Invalid("UNIFIED_LEGACY_IMPORT_INVALID"));
    }
    let original_amount = money(&proof.cumulative_entry_usd.normalize().to_string())?;
    if original_amount.is_zero() {
        return Err(StoreError::Invalid("UNIFIED_LEGACY_IMPORT_AMOUNT_INVALID"));
    }
    let revision = i64::try_from(proof.settlement_revision)
        .map_err(|_| StoreError::Invalid("UNIFIED_LEGACY_IMPORT_REVISION"))?;
    // Admission into an already-fenced, known installation with an existing
    // account; never bootstrap a campaign from an arbitrary external snapshot.
    let account_exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM finance_accounts
         WHERE installation_id=?1 AND account_key=?2)",
        params![&source.installation, &proof.account_key],
        |r| r.get(0),
    )?;
    if !account_exists {
        return Err(StoreError::Invalid("UNIFIED_LEGACY_ACCOUNT_NOT_ADMITTED"));
    }
    let original_json = serde_json::to_string(proof)
        .map_err(|_| StoreError::Invalid("UNIFIED_LEGACY_IMPORT_SERIALIZATION"))?;
    tx.execute(
        "INSERT INTO finance_intents(
           installation_id,account_key,intent_id,plan_sha256,exchange_order_id)
         VALUES(?1,?2,?3,?4,?5) ON CONFLICT DO NOTHING",
        params![
            &source.installation,
            &proof.account_key,
            &proof.intent_id,
            &proof.plan_hash,
            &proof.venue_order_id,
        ],
    )?;
    tx.execute(
        "INSERT INTO finance_fills(
           installation_id,account_key,fill_id,intent_id,exchange_order_id,
           trade_id,instrument_id)
         VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT DO NOTHING",
        params![
            &source.installation,
            &proof.account_key,
            &source.fill_id,
            &proof.intent_id,
            &proof.venue_order_id,
            &source.trade_id,
            &source.instrument_id,
        ],
    )?;
    tx.execute(
        "INSERT INTO finance_bills(
           installation_id,account_key,bill_id,fill_id,source_digest)
         VALUES(?1,?2,?3,?4,?5) ON CONFLICT DO NOTHING",
        params![
            &source.installation,
            &proof.account_key,
            &source.bill_id,
            &source.fill_id,
            &proof.source_digest,
        ],
    )?;
    tx.execute(
        "INSERT INTO finance_entry_sources(
           installation_id,account_key,intent_id,settlement_revision,
           fill_id,bill_id,source_digest,cumulative_entry_usd,original_receipt_json)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)
         ON CONFLICT DO NOTHING",
        params![
            &source.installation,
            &proof.account_key,
            &proof.intent_id,
            revision,
            &source.fill_id,
            &source.bill_id,
            &proof.source_digest,
            canonical(&original_amount)?,
            &original_json,
        ],
    )?;
    // ON CONFLICT DO NOTHING is safe only when the FULL imported source
    // identity is byte-exact on replay. Reject foreign/partial prior imports.
    let saved: (
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
    ) = tx.query_row(
        "SELECT i.plan_sha256,i.exchange_order_id,f.trade_id,f.instrument_id,
                f.fill_id,b.bill_id,b.source_digest,s.source_digest,s.cumulative_entry_usd,s.original_receipt_json
         FROM finance_intents i
         JOIN finance_entry_sources s ON s.installation_id=i.installation_id
           AND s.account_key=i.account_key AND s.intent_id=i.intent_id
         JOIN finance_fills f ON f.installation_id=s.installation_id
           AND f.account_key=s.account_key AND f.fill_id=s.fill_id
           AND f.intent_id=s.intent_id
         JOIN finance_bills b ON b.installation_id=s.installation_id
           AND b.account_key=s.account_key AND b.bill_id=s.bill_id AND b.fill_id=s.fill_id
         WHERE i.installation_id=?1 AND i.account_key=?2 AND i.intent_id=?3
           AND s.settlement_revision=?4",
        params![
            &source.installation,
            &proof.account_key,
            &proof.intent_id,
            revision
        ],
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
    )?;
    if saved
        != (
            proof.plan_hash.clone(),
            proof.venue_order_id.clone(),
            source.trade_id.clone(),
            source.instrument_id.clone(),
            source.fill_id.clone(),
            source.bill_id.clone(),
            proof.source_digest.clone(),
            proof.source_digest.clone(),
            canonical(&original_amount)?,
            original_json,
        )
    {
        return Err(StoreError::Invalid("UNIFIED_LEGACY_IMPORT_CONFLICT"));
    }
    Ok(())
}

#[cfg(test)]
#[path = "unified_financial_tests.rs"]
mod tests;

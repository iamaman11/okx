//! Original campaign deltas rules on a scoped, caller-owned SQL snapshot.
use super::*;
impl CampaignCapture<'_> {
    pub(in crate::account_store::sql) fn apply_captured_campaign_deltas(
        &self,
        campaign_id: &str,
        from_ms: i64,
        through_ms: i64,
        receipts: &[(String, String, Decimal, u64, i64)],
        witnessed_total: Decimal,
        opening_proof: Option<&str>,
    ) -> StoreResult<()> {
        let tx = self.connection;
        let prior: Option<(i64, i64, String)> = tx
            .query_row(
                self.sql(
                    "SELECT from_ms,covered_through_ms,consumed_usd
             FROM campaign_entry_totals WHERE campaign_id=?1",
                    campaign_read!(
                        "SELECT from_ms,covered_through_ms,consumed_usd
             FROM campaign_entry_totals WHERE campaign_id=?3"
                    ),
                ),
                self.params(params![campaign_id]),
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let mut consumed = if let Some((first, cut, total)) = prior {
            if first != from_ms || cut > through_ms {
                return Err(StoreError::Invalid("CAMPAIGN_APPLIED_FRONTIER_REGRESSION"));
            }
            decimal(total)?
        } else {
            Decimal::ZERO
        };
        for (intent_id, digest, amount, revision, venue_updated_at) in receipts {
            if intent_id.is_empty()
                || digest.len() != 64
                || !digest.bytes().all(|b| b.is_ascii_hexdigit())
                || *amount < Decimal::ZERO
                || *venue_updated_at < from_ms
                || *venue_updated_at > through_ms
            {
                return Err(StoreError::Invalid("CAMPAIGN_APPLIED_EVENT_INVALID"));
            }
            let revision = i64::try_from(*revision)
                .map_err(|_| StoreError::Invalid("CAMPAIGN_APPLIED_REVISION_INVALID"))?;
            let earlier: Option<(i64, String, String)> = tx
                .query_row(
                    self.sql(
                        "SELECT settlement_revision,cumulative_usd,source_digest
                 FROM campaign_entry_applied
                 WHERE campaign_id=?1 AND intent_id=?2
                 ORDER BY settlement_revision DESC LIMIT 1",
                        campaign_read!(
                            "SELECT settlement_revision,cumulative_usd,source_digest
                 FROM campaign_entry_applied
                 WHERE campaign_id=?3 AND intent_id=?4
                 ORDER BY settlement_revision DESC LIMIT 1"
                        ),
                    ),
                    self.params(params![campaign_id, intent_id]),
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            let prior_money = if let Some((prior_revision, prior_amount, prior_digest)) = earlier {
                let prior_amount = decimal(prior_amount)?;
                if prior_revision > revision {
                    // A reread may contain an OLD already-applied immutable
                    // revision. It is not a new purchase or a regression if,
                    // and only if, its own append-only historical row matches
                    // BOTH the exact money and source hash.
                    let historical: Option<(String, String)> = tx
                        .query_row(
                            self.sql(
                                "SELECT cumulative_usd,source_digest
                           FROM campaign_entry_applied
                          WHERE campaign_id=?1 AND intent_id=?2
                            AND settlement_revision=?3",
                                campaign_read!(
                                    "SELECT cumulative_usd,source_digest
                           FROM campaign_entry_applied
                          WHERE campaign_id=?3 AND intent_id=?4
                            AND settlement_revision=?5"
                                ),
                            ),
                            self.params(params![campaign_id, intent_id, revision]),
                            |row| Ok((row.get(0)?, row.get(1)?)),
                        )
                        .optional()?;
                    if historical.as_ref().is_some_and(|(money, hash)| {
                        decimal(money.clone()).ok() == Some(*amount) && hash == digest
                    }) {
                        continue;
                    }
                    return Err(StoreError::Invalid("CAMPAIGN_APPLIED_MONOTONICITY_BREACH"));
                }
                if prior_amount > *amount {
                    return Err(StoreError::Invalid("CAMPAIGN_APPLIED_MONOTONICITY_BREACH"));
                }
                if prior_revision == revision {
                    if prior_amount != *amount || prior_digest != *digest {
                        return Err(StoreError::Invalid("CAMPAIGN_APPLIED_SOURCE_CONFLICT"));
                    }
                    continue;
                }
                prior_amount
            } else {
                Decimal::ZERO
            };
            let delta = amount
                .checked_sub(prior_money)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            consumed = consumed
                .checked_add(delta)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            tx.execute(
                self.sql(
                    "INSERT INTO campaign_entry_applied
                 VALUES(?1,?2,?3,?4,?5,?6)",
                    "INSERT INTO campaign_entry_applied
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                ),
                self.params(params![
                    campaign_id,
                    intent_id,
                    revision,
                    exact(*amount),
                    digest,
                    venue_updated_at
                ]),
            )?;
        }
        if consumed != witnessed_total {
            return Err(StoreError::Invalid("CAMPAIGN_APPLIED_TOTAL_DIVERGENCE"));
        }
        tx.execute(
            self.sql(
                "INSERT INTO campaign_entry_totals
             VALUES(?1,?2,?3,?4)
             ON CONFLICT(campaign_id) DO UPDATE SET
                 covered_through_ms=excluded.covered_through_ms,
                 consumed_usd=excluded.consumed_usd",
                "INSERT INTO campaign_entry_totals
             VALUES(?1,?2,?3,?4,?5,?6)
             ON CONFLICT(installation_id,account_key,campaign_id) DO UPDATE SET
                 covered_through_ms=excluded.covered_through_ms,
                 consumed_usd=excluded.consumed_usd",
            ),
            self.params(params![campaign_id, from_ms, through_ms, exact(consumed)]),
        )?;
        if let Some(proof) = opening_proof {
            if proof.len() != 64 || !proof.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(StoreError::Invalid("CAMPAIGN_OPENING_PROOF_INVALID"));
            }
            tx.execute(
                self.sql("INSERT INTO campaign_entry_window_proofs
                 VALUES(?1,?2,?3,?4)
                 ON CONFLICT(campaign_id,covered_through_ms) DO NOTHING", "INSERT INTO campaign_entry_window_proofs
                 VALUES(?1,?2,?3,?4,?5,?6)
                 ON CONFLICT(installation_id,account_key,campaign_id,covered_through_ms) DO NOTHING"),
                self.params(params![campaign_id, through_ms, exact(consumed), proof]),
            )?;
            let saved: (String, String) = tx.query_row(
                self.sql(
                    "SELECT consumed_usd,source_digest FROM campaign_entry_window_proofs
                 WHERE campaign_id=?1 AND covered_through_ms=?2",
                    campaign_read!(
                        "SELECT consumed_usd,source_digest FROM campaign_entry_window_proofs
                 WHERE campaign_id=?3 AND covered_through_ms=?4"
                    ),
                ),
                self.params(params![campaign_id, through_ms]),
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            if saved != (exact(consumed), proof.to_owned()) {
                return Err(StoreError::Invalid("CAMPAIGN_OPENING_PROOF_CONFLICT"));
            }
            let key = format!("campaign_entry_opening:{campaign_id}:{proof}");
            tx.execute(
                self.sql("INSERT INTO operation_receipts
                 (operation_id,kind,request_hash,result_ref,committed_at)
                 VALUES(?1,'CAMPAIGN_ENTRY_OPENING_SEALED',?2,?2,?3)
                 ON CONFLICT(operation_id) DO NOTHING", "INSERT INTO operation_receipts
                 (installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at)
                 VALUES(?1,?2,?3,'CAMPAIGN_ENTRY_OPENING_SEALED',?4,?4,?5)
                 ON CONFLICT(installation_id,account_key,operation_id) DO NOTHING"),
                self.params(params![key, proof, through_ms]),
            )?;
            let receipt: (String, String, String, i64) = tx.query_row(
                self.sql(
                    "SELECT kind,request_hash,result_ref,committed_at
                 FROM operation_receipts WHERE operation_id=?1",
                    campaign_read!(
                        "SELECT kind,request_hash,result_ref,committed_at
                 FROM operation_receipts WHERE operation_id=?3"
                    ),
                ),
                self.params(params![key]),
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?;
            if receipt
                != (
                    "CAMPAIGN_ENTRY_OPENING_SEALED".into(),
                    proof.to_owned(),
                    proof.to_owned(),
                    through_ms,
                )
            {
                return Err(StoreError::Invalid("CAMPAIGN_OPENING_RECEIPT_CONFLICT"));
            }
        }
        Ok(())
    }
}

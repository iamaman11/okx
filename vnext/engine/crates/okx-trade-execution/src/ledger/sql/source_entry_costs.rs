//! Original fill/bill/fee evidence with compile-time namespace-scoped reads.
use super::super::*;
macro_rules! entry_read {
 ($query:literal)=>{concat!("WITH
 intents AS (SELECT rowid AS rowid,* FROM main.intents WHERE installation_id=?1 AND account_key=?2),
 intent_plans AS (SELECT rowid AS rowid,* FROM main.intent_plans WHERE installation_id=?1 AND account_key=?2),
 risk_receipts AS (SELECT rowid AS rowid,* FROM main.risk_receipts WHERE installation_id=?1 AND account_key=?2),
 fills AS (SELECT rowid AS rowid,* FROM main.fills WHERE installation_id=?1 AND account_key=?2),
 bills AS (SELECT rowid AS rowid,* FROM main.bills WHERE installation_id=?1 AND account_key=?2),
 bill_order_sources AS (SELECT rowid AS rowid,* FROM main.bill_order_sources WHERE installation_id=?1 AND account_key=?2),
 finance_entries AS (SELECT rowid AS rowid,* FROM main.finance_entries WHERE installation_id=?1 AND account_key=?2),
 finance_source_links AS (SELECT rowid AS rowid,* FROM main.finance_source_links WHERE installation_id=?1 AND account_key=?2),
 swap_fill_sources AS (SELECT rowid AS rowid,* FROM main.swap_fill_sources WHERE installation_id=?1 AND account_key=?2),
 operation_receipts AS (SELECT rowid AS rowid,* FROM main.operation_receipts WHERE installation_id=?1 AND account_key=?2) ", $query)};
}
pub(super) use entry_read;
pub(super) fn entry_source_values(
    scope: Option<(&str, &str)>,
    mut values: Vec<rusqlite::types::Value>,
) -> Vec<rusqlite::types::Value> {
    if let Some((i, a)) = scope {
        values.insert(0, a.to_owned().into());
        values.insert(0, i.to_owned().into());
    }
    values
}
impl AccountStore {
    pub(crate) fn intent_fill_cost_evidence_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        intent_id: &str,
        through: i64,
    ) -> StoreResult<IntentFillCostEvidence> {
        use sha2::{Digest, Sha256};
        let meta = Self::entry_settlement_identity_sql(db, scope)?;
        if through < meta.initialized_at {
            return Err(StoreError::Invalid("ENTRY_FILL_WINDOW_INVALID"));
        }
        let cursor = Self::entry_settlement_cursor_sql(db, scope, "fills", "SWAP")?
            .ok_or(StoreError::Invalid("ENTRY_FILL_COVERAGE_UNPROVEN"))?;
        if !cursor.cursor.is_empty()
            || !cursor.coverage_gaps.is_empty()
            || cursor.generation <= 0
            || cursor.watermark < through
        {
            return Err(StoreError::Invalid("ENTRY_FILL_COVERAGE_UNPROVEN"));
        }
        let (
            plan_hash,
            instrument,
            contract_value,
            inverse,
            currency,
            increase,
            side,
            planned_quantity,
            bound_price,
        ): (
            String,
            String,
            String,
            bool,
            String,
            bool,
            String,
            String,
            String,
        ) = db
            .query_row(
                match scope {
                    None => {
                        "SELECT i.plan_hash,i.instrument_id,p.contract_value,p.inverse,
                        p.settlement_currency,i.risk_increase,p.side,p.quantity,p.limit_price
                 FROM intents i JOIN intent_plans p ON p.intent_id=i.intent_id
                 WHERE i.intent_id=?1"
                    }
                    Some(_) => entry_read!(
                        "SELECT i.plan_hash,i.instrument_id,p.contract_value,p.inverse,
                        p.settlement_currency,i.risk_increase,p.side,p.quantity,p.limit_price
                 FROM intents i JOIN intent_plans p ON p.intent_id=i.intent_id
                 WHERE i.intent_id=?3"
                    ),
                },
                rusqlite::params_from_iter(entry_source_values(
                    scope,
                    vec![intent_id.to_owned().into()],
                )),
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
                    ))
                },
            )
            .optional()?
            .ok_or(StoreError::Invalid("ENTRY_FILL_INTENT_NOT_FOUND"))?;
        // Addressable PK-backed read of the only authorized SQL order and
        // central receipt, separate from the immutable contract mechanics.
        let (exchange_order_id, begin_post_at, reservation_key): (
            Option<String>,
            Option<i64>,
            Option<String>,
        ) = db.query_row(
            match scope {
                None => {
                    "SELECT i.exchange_order_id,i.begin_post_at,r.reservation_key
                 FROM intents i LEFT JOIN risk_receipts r
                  ON r.intent_id=i.intent_id AND r.plan_hash=i.plan_hash
                   AND r.account_key=?2 AND r.installation_id=?3
                 WHERE i.intent_id=?1"
                }
                Some(_) => entry_read!(
                    "SELECT i.exchange_order_id,i.begin_post_at,r.reservation_key
                 FROM intents i LEFT JOIN risk_receipts r
                  ON r.intent_id=i.intent_id AND r.plan_hash=i.plan_hash
                   AND r.account_key=?4 AND r.installation_id=?5
                 WHERE i.intent_id=?3"
                ),
            },
            rusqlite::params_from_iter(entry_source_values(
                scope,
                vec![
                    intent_id.to_owned().into(),
                    meta.account_key.clone().into(),
                    meta.installation_id.clone().into(),
                ],
            )),
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        if !increase || inverse || !instrument.ends_with("-USDT-SWAP") || currency != "USDT" {
            return Err(StoreError::Invalid("ENTRY_FILL_INSTRUMENT_UNSUPPORTED"));
        }
        // A fill row's intent_id is attribution, not proof that the exchange
        // actually accepted *this* order. Resolve the durable POST fence,
        // central reservation and exact order ID before monetary evaluation.
        let (Some(exchange_order_id), Some(posted_at), Some(reservation_key)) =
            (exchange_order_id, begin_post_at, reservation_key)
        else {
            return Err(StoreError::Invalid("ENTRY_FILL_ORDER_UNVERIFIED"));
        };
        if exchange_order_id.is_empty() || reservation_key.is_empty() || posted_at <= 0 {
            return Err(StoreError::Invalid("ENTRY_FILL_ORDER_UNVERIFIED"));
        }
        let ct = decimal(contract_value.clone())?;
        let planned_qty = decimal(planned_quantity.clone())?;
        let price_bound = decimal(bound_price.clone())?;
        if !matches!(side.as_str(), "buy" | "sell")
            || planned_qty <= Decimal::ZERO
            || price_bound <= Decimal::ZERO
        {
            return Err(StoreError::Invalid("ENTRY_FILL_PLAN_INVALID"));
        }
        if ct <= Decimal::ZERO {
            return Err(StoreError::Invalid("ENTRY_FILL_CONTRACT_INVALID"));
        }
        // A cursor watermark or private generation can advance after an
        // immutable venue event. Check source coverage above on EVERY read,
        // but hash only the chosen covered-through cut-off and economic facts.
        // Otherwise a lost Worker ACK changes digest on ordinary recapture.
        let mut hasher = Sha256::new();
        let header = serde_json::to_vec(&(
            "okx.intent-fill-cost-evidence/v1",
            meta.account_key.as_str(),
            meta.installation_id.as_str(),
            intent_id,
            plan_hash.as_str(),
            instrument.as_str(),
            contract_value.as_str(),
            side.as_str(),
            planned_quantity.as_str(),
            bound_price.as_str(),
            exchange_order_id.as_str(),
            reservation_key.as_str(),
            posted_at,
            through,
        ))
        .map_err(|_| StoreError::Invalid("ENTRY_FILL_DIGEST_INVALID"))?;
        hasher.update(&header);
        let mut statement = db.prepare(
            match scope {None=>"SELECT fill_id,instrument_id,trade_id,exchange_order_id,time,qty,price,fee,currency,side
             FROM fills WHERE intent_id=?1 AND time<=?2 ORDER BY fill_id LIMIT 10001",Some(_)=>entry_read!("SELECT fill_id,instrument_id,trade_id,exchange_order_id,time,qty,price,fee,currency,side
             FROM fills WHERE intent_id=?3 AND time<=?4 ORDER BY fill_id LIMIT 10001")},
        )?;
        let mut rows = statement.query(rusqlite::params_from_iter(entry_source_values(
            scope,
            vec![intent_id.to_owned().into(), through.into()],
        )))?;
        let mut count = 0u64;
        let mut notional = Decimal::ZERO;
        let mut fees = Decimal::ZERO;
        let mut filled_qty = Decimal::ZERO;
        while let Some(row) = rows.next()? {
            count += 1;
            if count > 10000 {
                return Err(StoreError::Invalid("ENTRY_FILL_HISTORY_TOO_LARGE"));
            }
            let source: (
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
            ) = (
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
            );
            if source.1 != instrument
                || source.4 < meta.initialized_at
                || source.4 < posted_at
                || source.8 != "USDT"
                || source.9 != side
                || source.0.is_empty()
                || source.2.is_empty()
                || source.3.is_empty()
            {
                return Err(StoreError::Invalid("ENTRY_FILL_SOURCE_INVALID"));
            }
            if source.3 != exchange_order_id {
                return Err(StoreError::Invalid("ENTRY_FILL_EXCHANGE_ORDER_MISMATCH"));
            }
            let qty = decimal(source.5.clone())?;
            let price = decimal(source.6.clone())?;
            let fee = decimal(source.7.clone())?;
            if qty <= Decimal::ZERO || price <= Decimal::ZERO {
                return Err(StoreError::Invalid("ENTRY_FILL_SOURCE_INVALID"));
            }
            // A fill attributed to an intent is not enough: prove that it
            // could have resulted from the exact persisted bounded order.
            if (side == "buy" && price > price_bound) || (side == "sell" && price < price_bound) {
                return Err(StoreError::Invalid("ENTRY_FILL_PRICE_BOUND_VIOLATED"));
            }
            filled_qty = filled_qty
                .checked_add(qty)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            if filled_qty > planned_qty {
                return Err(StoreError::Invalid("ENTRY_FILL_QUANTITY_EXCEEDED"));
            }
            let gross = qty
                .checked_mul(ct)
                .and_then(|v| v.checked_mul(price))
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            notional = notional
                .checked_add(gross)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            fees = fees
                .checked_add(super::finance::entry_fee_debit(fee))
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            // Length-delimited immutable venue row; independent of ingestion order.
            let bytes = serde_json::to_vec(&source)
                .map_err(|_| StoreError::Invalid("ENTRY_FILL_DIGEST_INVALID"))?;
            hasher.update((bytes.len() as u64).to_le_bytes());
            hasher.update(bytes);
        }
        if count == 0 {
            return Err(StoreError::Invalid("ENTRY_FILL_EVIDENCE_EMPTY"));
        }
        Ok(IntentFillCostEvidence {
            intent_id: intent_id.to_owned(),
            plan_hash,
            exchange_order_id,
            reservation_key,
            fill_count: count,
            filled_quantity: filled_qty,
            covered_through: through,
            gross_notional_usdt: notional,
            conservative_fee_usdt: fees,
            source_digest: format!("{:x}", hasher.finalize()),
        })
    }
    pub(crate) fn intent_additional_entry_fee_evidence_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
        through: i64,
    ) -> StoreResult<(Decimal, String)> {
        use sha2::{Digest, Sha256};
        Self::entry_settlement_identity_sql(db, scope)?;
        let covered = Self::entry_settlement_cursor_sql(db, scope, "bills", "account")?
            .ok_or(StoreError::Invalid("ENTRY_EXTRA_BILLS_UNCOVERED"))?;
        if covered.watermark < through
            || !covered.cursor.is_empty()
            || !covered.coverage_gaps.is_empty()
            || covered.generation <= 0
        {
            return Err(StoreError::Invalid("ENTRY_EXTRA_BILLS_UNCOVERED"));
        }
        let intent = Self::find_intent_sql(db, scope, "intent_id", id)?
            .ok_or(StoreError::Invalid("ENTRY_BILL_INTENT_MISSING"))?;
        let prefix = format!("entry-bill:{id}:");
        let bound = format!("{prefix}~");
        let mut q = db.prepare(match scope {
            None => {
                "SELECT operation_id,kind,request_hash,result_ref,committed_at
             FROM operation_receipts WHERE operation_id>=?1 AND operation_id<?2
             ORDER BY operation_id LIMIT 10001"
            }
            Some(_) => entry_read!(
                "SELECT operation_id,kind,request_hash,result_ref,committed_at
             FROM operation_receipts WHERE operation_id>=?3 AND operation_id<?4
             ORDER BY operation_id LIMIT 10001"
            ),
        })?;
        let mut rows = q.query(rusqlite::params_from_iter(entry_source_values(
            scope,
            vec![prefix.into(), bound.into()],
        )))?;
        let mut extras = Decimal::ZERO;
        let mut digest = Sha256::new();
        digest.update(b"okx.intent-extra-entry-fee/v1");
        digest.update(id.as_bytes());
        digest.update(through.to_le_bytes());
        let mut count = 0_u64;
        while let Some(row) = rows.next()? {
            count += 1;
            if count > 10000 {
                return Err(StoreError::Invalid("ENTRY_EXTRA_BILLS_TOO_MANY"));
            }
            let (key, kind, hash, encoded, at): (String, String, String, String, i64) = (
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            );
            let link: EntryOrderFeeLink = serde_json::from_str(&encoded)
                .map_err(|_| StoreError::Invalid("ENTRY_EXTRA_BILL_RECEIPT_CORRUPT"))?;
            if kind != "ENTRY_ORDER_FEE_PROVEN"
                || key != format!("entry-bill:{id}:{}", link.bill_id)
                || hash != intent.plan_hash
                || link.intent_id != id
                || link.instrument_id != intent.instrument_id
                || Some(link.exchange_order_id.as_str()) != intent.exchange_order_id.as_deref()
                || link.bill_at != at
                || link.fee_usdt >= Decimal::ZERO
            {
                return Err(StoreError::Invalid("ENTRY_EXTRA_BILL_IDENTITY_MISMATCH"));
            }
            if at > through {
                continue;
            }
            let source: Option<(i64, String, String, String, bool)> = db
                .query_row(
                    match scope {
                        None => {
                            "SELECT b.time,b.currency,e.amount,e.entry_id,
                    EXISTS(SELECT 1 FROM finance_source_links lf
                        JOIN fills f ON lf.source_ref='fill:'||f.fill_id
                        WHERE lf.entry_id=e.entry_id AND f.intent_id=?2)
                 FROM bills b JOIN finance_entries e
                    ON e.economic_event_key=b.economic_event_key
                       AND e.component='FEE' AND e.allocation_leg='account'
                 JOIN finance_source_links lb ON lb.entry_id=e.entry_id
                       AND lb.source_ref='bill:'||b.bill_id
                 WHERE b.bill_id=?1"
                        }
                        Some(_) => entry_read!(
                            "SELECT b.time,b.currency,e.amount,e.entry_id,
                    EXISTS(SELECT 1 FROM finance_source_links lf
                        JOIN fills f ON lf.source_ref='fill:'||f.fill_id
                        WHERE lf.entry_id=e.entry_id AND f.intent_id=?4)
                 FROM bills b JOIN finance_entries e
                    ON e.economic_event_key=b.economic_event_key
                       AND e.component='FEE' AND e.allocation_leg='account'
                 JOIN finance_source_links lb ON lb.entry_id=e.entry_id
                       AND lb.source_ref='bill:'||b.bill_id
                 WHERE b.bill_id=?3"
                        ),
                    },
                    rusqlite::params_from_iter(entry_source_values(
                        scope,
                        vec![link.bill_id.clone().into(), id.to_owned().into()],
                    )),
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .optional()?;
            let (bill_at, ccy, charge, event_id, first_fee) =
                source.ok_or(StoreError::Invalid("ENTRY_EXTRA_BILL_SOURCE_UNVERIFIED"))?;
            if bill_at != link.bill_at || ccy != "USDT" || decimal(charge.clone())? != link.fee_usdt
            {
                return Err(StoreError::Invalid("ENTRY_EXTRA_BILL_AMOUNT_CHANGED"));
            }
            if !first_fee {
                extras = extras
                    .checked_add(super::finance::entry_fee_debit(link.fee_usdt))
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
                let bytes = serde_json::to_vec(&(link, bill_at, ccy, charge, event_id))
                    .map_err(|_| StoreError::Invalid("ENTRY_EXTRA_BILL_DIGEST_FAILED"))?;
                digest.update((bytes.len() as u64).to_le_bytes());
                digest.update(bytes);
            }
        }
        Ok((
            extras,
            if extras > Decimal::ZERO {
                format!("{:x}", digest.finalize())
            } else {
                String::new()
            },
        ))
    }
    pub(crate) fn intent_fee_bill_evidence_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        intent_id: &str,
        through: i64,
    ) -> StoreResult<IntentFeeBillEvidence> {
        use sha2::{Digest, Sha256};
        let meta = Self::entry_settlement_identity_sql(db, scope)?;
        #[derive(serde::Serialize)]
        struct FeeBillSource {
            entry_id: String,
            economic_event_key: String,
            amount: String,
            currency: String,
            allocation_leg: String,
            finance_time: i64,
            reversal: Option<String>,
            bill_id: Option<String>,
            bill_time: Option<i64>,
            bill_kind: Option<String>,
            bill_currency: Option<String>,
            bill_linked: bool,
            // Preserve the canonical digest of legacy authenticated
            // fee receipts that predate raw bill order provenance capture.
            #[serde(skip_serializing_if = "Option::is_none")]
            bill_order_id: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            bill_instrument_id: Option<String>,
        }
        let fill = Self::intent_fill_cost_evidence_sql(db, scope, intent_id, through)?;
        let intent_instrument = Self::find_intent_sql(db, scope, "intent_id", intent_id)?
            .ok_or(StoreError::Invalid("ENTRY_BILL_INTENT_MISSING"))?
            .instrument_id;
        if !Self::finance_proof_sealed_sql(db, scope)? {
            return Err(StoreError::Invalid("ENTRY_FINANCE_PROVENANCE_UNSEALED"));
        }
        let bills = Self::entry_settlement_cursor_sql(db, scope, "bills", "account")?
            .ok_or(StoreError::Invalid("ENTRY_BILLS_COVERAGE_UNPROVEN"))?;
        if !bills.cursor.is_empty()
            || !bills.coverage_gaps.is_empty()
            || bills.generation <= 0
            || bills.watermark < through
        {
            return Err(StoreError::Invalid("ENTRY_BILLS_COVERAGE_UNPROVEN"));
        }
        // A later source generation/watermark cannot change the source
        // digest of the same fixed through-window and immutable fee bills.
        let mut hasher = Sha256::new();
        hasher.update(
            serde_json::to_vec(&(
                "okx.intent-fee-bill-evidence/v1",
                meta.account_key.as_str(),
                meta.installation_id.as_str(),
                intent_id,
                fill.source_digest.as_str(),
                through,
            ))
            .map_err(|_| StoreError::Invalid("ENTRY_FEE_BILL_DIGEST_INVALID"))?,
        );
        let mut fee_rows = db.prepare(match scope {
            None => {
                "SELECT fill_id,fee,currency,time FROM fills
             WHERE intent_id=?1 AND time<=?2 ORDER BY fill_id LIMIT 10001"
            }
            Some(_) => entry_read!(
                "SELECT fill_id,fee,currency,time FROM fills
             WHERE intent_id=?3 AND time<=?4 ORDER BY fill_id LIMIT 10001"
            ),
        })?;
        // Source-ref first index makes each immutable fee lookup addressable.
        // The canonical finance entry may have first arrived from bill history
        // or from fills. Its source_links must include BOTH original sources.
        let mut linked = db.prepare(match scope {
            None => {
                "SELECT e.entry_id,e.economic_event_key,e.amount,e.currency,
                    e.allocation_leg,e.time,e.reverses_entry_id,b.bill_id,b.time,b.kind,b.currency,
                    EXISTS(SELECT 1 FROM finance_source_links lb
                      WHERE lb.entry_id=e.entry_id
                        AND lb.source_ref='bill:'||b.bill_id),
                    origin.exchange_order_id,origin.instrument_id
             FROM finance_source_links fl
             JOIN finance_entries e ON e.entry_id=fl.entry_id AND e.component='FEE'
             LEFT JOIN bills b ON b.economic_event_key=e.economic_event_key
                              AND e.economic_event_key='bill:'||b.bill_id
             LEFT JOIN bill_order_sources origin ON origin.bill_id=b.bill_id
             WHERE fl.source_ref=?1 ORDER BY e.entry_id LIMIT 2"
            }
            Some(_) => entry_read!(
                "SELECT e.entry_id,e.economic_event_key,e.amount,e.currency,
                    e.allocation_leg,e.time,e.reverses_entry_id,b.bill_id,b.time,b.kind,b.currency,
                    EXISTS(SELECT 1 FROM finance_source_links lb
                      WHERE lb.entry_id=e.entry_id
                        AND lb.source_ref='bill:'||b.bill_id),
                    origin.exchange_order_id,origin.instrument_id
             FROM finance_source_links fl
             JOIN finance_entries e ON e.entry_id=fl.entry_id AND e.component='FEE'
             LEFT JOIN bills b ON b.economic_event_key=e.economic_event_key
                              AND e.economic_event_key='bill:'||b.bill_id
             LEFT JOIN bill_order_sources origin ON origin.bill_id=b.bill_id
             WHERE fl.source_ref=?3 ORDER BY e.entry_id LIMIT 2"
            ),
        })?;
        let mut scanned = fee_rows.query(rusqlite::params_from_iter(entry_source_values(
            scope,
            vec![intent_id.to_owned().into(), through.into()],
        )))?;
        let mut count = 0_u64;
        let mut total = Decimal::ZERO;
        while let Some(row) = scanned.next()? {
            count += 1;
            if count > 10000 {
                return Err(StoreError::Invalid("ENTRY_FILL_HISTORY_TOO_LARGE"));
            }
            let id: String = row.get(0)?;
            let fee: String = row.get(1)?;
            let currency: String = row.get(2)?;
            let fill_time: i64 = row.get(3)?;
            let fee = decimal(fee)?;
            if fee == Decimal::ZERO {
                // No finance fee event exists for an actual zero-fee fill.
                // A zero amount in fills alone does NOT prove the final bill.
                return Err(StoreError::Invalid("ENTRY_ZERO_FEE_BILL_UNPROVEN"));
            }
            let fee_source = format!("fill:{id}");
            let mut rows = linked.query(rusqlite::params_from_iter(entry_source_values(
                scope,
                vec![fee_source.clone().into()],
            )))?;
            let Some(source) = rows.next()? else {
                return Err(StoreError::Invalid("ENTRY_FEE_BILL_LINK_MISSING"));
            };
            let proof = FeeBillSource {
                entry_id: source.get(0)?,
                economic_event_key: source.get(1)?,
                amount: source.get(2)?,
                currency: source.get(3)?,
                allocation_leg: source.get(4)?,
                finance_time: source.get(5)?,
                reversal: source.get(6)?,
                bill_id: source.get(7)?,
                bill_time: source.get(8)?,
                bill_kind: source.get(9)?,
                bill_currency: source.get(10)?,
                bill_linked: source.get(11)?,
                bill_order_id: source.get(12)?,
                bill_instrument_id: source.get(13)?,
            };
            if rows.next()?.is_some() {
                return Err(StoreError::Invalid("ENTRY_FEE_BILL_LINK_AMBIGUOUS"));
            }
            // A fill without billId can acquire a fee link from bill-history
            // later. If another matching bill arrives AFTER the first link,
            // checking just finance_source_links would falsely retain success.
            // Use the original per-account SQL facts, not a second ledger.
            let late_swap: bool = db.query_row(
                match scope {
                    None => {
                        "SELECT EXISTS(SELECT 1 FROM swap_fill_sources
                 WHERE fill_id=?1 AND bill_id='')"
                    }
                    Some(_) => entry_read!(
                        "SELECT EXISTS(SELECT 1 FROM swap_fill_sources
                 WHERE fill_id=?3 AND bill_id='')"
                    ),
                },
                rusqlite::params_from_iter(entry_source_values(scope, vec![id.clone().into()])),
                |r| r.get(0),
            )?;
            if late_swap {
                let mut candidates = db.prepare(match scope {
                    None => {
                        "SELECT b.bill_id,e.amount,e.currency,b.time
                     FROM fills f
                     JOIN bill_order_sources o
                       ON o.instrument_id=f.instrument_id
                      AND o.exchange_order_id=f.exchange_order_id
                      AND o.trade_id=f.trade_id
                     JOIN bills b ON b.bill_id=o.bill_id
                     JOIN finance_entries e ON e.economic_event_key=b.economic_event_key
                       AND e.component='FEE' AND e.allocation_leg='account'
                     WHERE f.fill_id=?1 AND b.time<=?2
                     ORDER BY b.bill_id LIMIT 65"
                    }
                    Some(_) => entry_read!(
                        "SELECT b.bill_id,e.amount,e.currency,b.time
                     FROM fills f
                     JOIN bill_order_sources o
                       ON o.instrument_id=f.instrument_id
                      AND o.exchange_order_id=f.exchange_order_id
                      AND o.trade_id=f.trade_id
                     JOIN bills b ON b.bill_id=o.bill_id
                     JOIN finance_entries e ON e.economic_event_key=b.economic_event_key
                       AND e.component='FEE' AND e.allocation_leg='account'
                     WHERE f.fill_id=?3 AND b.time<=?4
                     ORDER BY b.bill_id LIMIT 65"
                    ),
                })?;
                let mut bill_rows = candidates.query(rusqlite::params_from_iter(
                    entry_source_values(scope, vec![id.clone().into(), through.into()]),
                ))?;
                let mut scanned = 0_u32;
                let mut matched: Option<String> = None;
                while let Some(bill_row) = bill_rows.next()? {
                    scanned += 1;
                    if scanned > 64 {
                        return Err(StoreError::Invalid("LATE_SWAP_FEE_BILLS_UNBOUNDED"));
                    }
                    let bill_amount: String = bill_row.get(1)?;
                    let bill_currency: String = bill_row.get(2)?;
                    let bill_time: i64 = bill_row.get(3)?;
                    if decimal(bill_amount)? != fee
                        || bill_currency != currency
                        || bill_time < fill_time
                    {
                        continue;
                    }
                    if matched.is_some() {
                        return Err(StoreError::Invalid("ENTRY_FEE_BILL_LINK_AMBIGUOUS"));
                    }
                    matched = Some(bill_row.get(0)?);
                }
                if matched.as_deref() != proof.bill_id.as_deref() {
                    return Err(StoreError::Invalid("ENTRY_FEE_BILL_SOURCE_MISMATCH"));
                }
            }
            // A linked fee still cannot settle an entry if the bill
            // predates the fill, the finance event is outside the
            // claimed covered window, or the link belongs to a non-account
            // allocation. Do not equate a source-link match with money proof.
            if proof.reversal.is_some()
                || proof.allocation_leg != "account"
                || fill_time <= 0
                || proof.finance_time < fill_time
                || proof.finance_time > through
                || !proof.economic_event_key.starts_with("bill:")
                || proof.bill_id.as_deref() != proof.economic_event_key.strip_prefix("bill:")
                || proof
                    .bill_time
                    .is_none_or(|t| t < fill_time || t > through || t > bills.watermark)
                || proof.bill_kind.as_deref().is_none_or(|kind| {
                    kind.is_empty()
                        || kind.starts_with("7:")
                        || kind == "1:11"
                        || kind == "1:12"
                        || kind.ends_with(":173")
                        || kind.ends_with(":174")
                })
                || proof.bill_currency.as_deref() != Some(currency.as_str())
                || !proof.bill_linked
                || proof
                    .bill_order_id
                    .as_deref()
                    .is_some_and(|v| v != fill.exchange_order_id.as_str())
                || proof
                    .bill_instrument_id
                    .as_deref()
                    .is_some_and(|v| v != intent_instrument.as_str())
                || proof.currency != currency
                || decimal(proof.amount.clone())? != fee
            {
                return Err(StoreError::Invalid("ENTRY_FEE_BILL_SOURCE_MISMATCH"));
            }
            total = total
                .checked_add(super::finance::entry_fee_debit(fee))
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            let data = serde_json::to_vec(&(id, fee_source, currency, fill_time, proof))
                .map_err(|_| StoreError::Invalid("ENTRY_FEE_BILL_DIGEST_INVALID"))?;
            hasher.update((data.len() as u64).to_le_bytes());
            hasher.update(data);
        }
        if count != fill.fill_count || count == 0 || total != fill.conservative_fee_usdt {
            return Err(StoreError::Invalid("ENTRY_FEE_BILL_TOTAL_MISMATCH"));
        }
        Ok(IntentFeeBillEvidence {
            fill,
            linked_bill_count: count,
            bills_covered_through: bills.watermark,
            conservative_verified_fee_usdt: total,
            source_digest: format!("{:x}", hasher.finalize()),
        })
    }
}

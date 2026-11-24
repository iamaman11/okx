//! Original SWAP source labels and bounded late-fee links.
use super::super::*;
impl AccountStore {
    pub(crate) fn record_swap_sources_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        sources: &[SwapFillSource],
    ) -> StoreResult<()> {
        use rusqlite::{params_from_iter, types::Value};
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(tx, installation, account)?;
        }
        let values = |mut original: Vec<Value>| {
            if let Some((installation, account)) = scope {
                original.push(installation.to_owned().into());
                original.push(account.to_owned().into());
            }
            original
        };
        // SWAP source labels and the page frontier commit atomically.
        // Conflicting replays never advance the source cursor.
        for source in sources {
            let found: Option<(String, String)> = tx
                .query_row(
                    match scope { None => "SELECT instrument_id,side FROM fills WHERE fill_id=?1", Some(_) => "SELECT instrument_id,side FROM fills WHERE fill_id=?1 AND installation_id=?2 AND account_key=?3" },
 params_from_iter(values(vec![source.fill_id.clone().into()])),
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            let (instrument, side) =
                found.ok_or(StoreError::Invalid("SWAP_FILL_SOURCE_WITHOUT_FILL"))?;
            if !instrument.ends_with("-SWAP")
                || source.direction
                    != swap_fill_direction(&source.sub_type, &side, &source.position_side)?
            {
                return Err(StoreError::Invalid("SWAP_FILL_SOURCE_INVALID"));
            }
            tx.execute(
                match scope {
                    None => {
                        "INSERT INTO swap_fill_sources VALUES(?1,?2,?3,?4,?5)
                 ON CONFLICT(fill_id) DO NOTHING"
                    }
                    Some(_) => {
                        "INSERT INTO swap_fill_sources VALUES(?6,?7,?1,?2,?3,?4,?5)
                 ON CONFLICT(installation_id,account_key,fill_id) DO NOTHING"
                    }
                },
                params_from_iter(values(vec![
                    source.fill_id.clone().into(),
                    source.bill_id.clone().into(),
                    source.sub_type.clone().into(),
                    source.position_side.clone().into(),
                    source.direction.clone().into(),
                ])),
            )?;
            let old: (String, String, String, String) = tx.query_row(
                match scope {
                    None => {
                        "SELECT bill_id,sub_type,position_side,direction
                 FROM swap_fill_sources WHERE fill_id=?1"
                    }
                    Some(_) => {
                        "SELECT bill_id,sub_type,position_side,direction
                 FROM swap_fill_sources WHERE fill_id=?1 AND installation_id=?2 AND account_key=?3"
                    }
                },
                params_from_iter(values(vec![source.fill_id.clone().into()])),
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?;
            if old
                != (
                    source.bill_id.clone(),
                    source.sub_type.clone(),
                    source.position_side.clone(),
                    source.direction.clone(),
                )
            {
                return Err(StoreError::Invalid("IMMUTABLE_SWAP_FILL_SOURCE_CONFLICT"));
            }
        }
        Ok(())
    }
    // Late SWAP fills may not carry billId. Resolve this only from the existing
    // immutable fill and bill-order source records of this ONE account SQL owner.
    // An ambiguous or mismatched source stays unlinked (and cannot settle).
    pub(crate) fn link_late_swap_fee_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        fill_id: &str,
    ) -> StoreResult<()> {
        use rusqlite::{params_from_iter, types::Value};
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(tx, installation, account)?;
        }
        let values = |mut original: Vec<Value>| {
            if let Some((installation, account)) = scope {
                original.push(installation.to_owned().into());
                original.push(account.to_owned().into());
            }
            original
        };

        let raw: Option<(String, String, String, String, String, i64, String)> = tx
        .query_row(
            match scope { None => "SELECT f.instrument_id,f.trade_id,f.exchange_order_id,f.fee,
                    f.currency,f.time,s.bill_id
             FROM fills f JOIN swap_fill_sources s ON s.fill_id=f.fill_id
             WHERE f.fill_id=?1", Some(_) => "SELECT f.instrument_id,f.trade_id,f.exchange_order_id,f.fee,
                    f.currency,f.time,s.bill_id
             FROM fills f JOIN swap_fill_sources s ON s.fill_id=f.fill_id AND s.installation_id=f.installation_id AND s.account_key=f.account_key
             WHERE f.fill_id=?1 AND f.installation_id=?2 AND f.account_key=?3" },
 params_from_iter(values(vec![fill_id.to_owned().into()])),
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                ))
            },
        )
        .optional()?;
        let Some((instrument, trade, order, raw_fee, raw_currency, fill_time, fill_bill_id)) = raw
        else {
            return Err(StoreError::Invalid("LATE_SWAP_FEE_FILL_SOURCE_MISSING"));
        };
        if !fill_bill_id.is_empty() || trade.is_empty() || order.is_empty() {
            return Ok(());
        }
        let raw_fee = decimal(raw_fee)?;
        let mut q = tx.prepare(
        match scope {None=>"SELECT e.entry_id,e.amount,e.currency,b.time
         FROM bill_order_sources origin
         JOIN bills b ON b.bill_id=origin.bill_id
         JOIN finance_entries e ON e.economic_event_key=b.economic_event_key
             AND e.component='FEE' AND e.allocation_leg='account'
         WHERE origin.instrument_id=?1 AND origin.exchange_order_id=?2
           AND origin.trade_id=?3
         ORDER BY b.bill_id LIMIT 65",Some(_)=>"SELECT e.entry_id,e.amount,e.currency,b.time
         FROM bill_order_sources origin
         JOIN bills b ON b.bill_id=origin.bill_id AND b.installation_id=origin.installation_id AND b.account_key=origin.account_key
         JOIN finance_entries e ON e.economic_event_key=b.economic_event_key AND e.installation_id=b.installation_id AND e.account_key=b.account_key
             AND e.component='FEE' AND e.allocation_leg='account'
         WHERE origin.instrument_id=?1 AND origin.exchange_order_id=?2
           AND origin.trade_id=?3
         AND origin.installation_id=?4 AND origin.account_key=?5 ORDER BY b.bill_id LIMIT 65"},
    )?;
        let mut rows = q.query(params_from_iter(values(vec![
            instrument.into(),
            order.into(),
            trade.into(),
        ])))?;
        let mut matched: Option<String> = None;
        let mut candidates = 0;
        while let Some(row) = rows.next()? {
            candidates += 1;
            if candidates > 64 {
                return Err(StoreError::Invalid("LATE_SWAP_FEE_BILLS_UNBOUNDED"));
            }
            let entry_id: String = row.get(0)?;
            let fee: String = row.get(1)?;
            let currency: String = row.get(2)?;
            let bill_time: i64 = row.get(3)?;
            if decimal(fee)? != raw_fee || currency != raw_currency || bill_time < fill_time {
                continue;
            }
            if matched.is_some() {
                // Two matching invoices cannot each acquire this fill's fee.
                return Ok(());
            }
            matched = Some(entry_id);
        }
        let Some(entry_id) = matched else {
            return Ok(());
        };
        let fill_source = format!("fill:{fill_id}");
        let conflicting: bool = tx.query_row(
        match scope { None => "SELECT EXISTS(SELECT 1 FROM finance_source_links l
         JOIN finance_entries e ON e.entry_id=l.entry_id
         WHERE l.source_ref=?1 AND e.component='FEE'
           AND l.entry_id<>?2 AND e.reverses_entry_id IS NULL)", Some(_) => "SELECT EXISTS(SELECT 1 FROM finance_source_links l
         JOIN finance_entries e ON e.entry_id=l.entry_id AND e.installation_id=l.installation_id AND e.account_key=l.account_key
         WHERE l.source_ref=?1 AND e.component='FEE'
           AND l.entry_id<>?2 AND e.reverses_entry_id IS NULL AND l.installation_id=?3 AND l.account_key=?4)" },
 params_from_iter(values(vec![fill_source.clone().into(),entry_id.clone().into()])),
        |r| r.get(0),
    )?;
        if conflicting {
            return Err(StoreError::Invalid("LATE_SWAP_FEE_SOURCE_ALREADY_BOOKED"));
        }
        tx.execute(
        match scope { None => "INSERT INTO finance_source_links(entry_id,source_ref) VALUES(?1,?2)
         ON CONFLICT DO NOTHING", Some(_) => "INSERT INTO finance_source_links(entry_id,source_ref,installation_id,account_key) VALUES(?1,?2,?3,?4)
         ON CONFLICT DO NOTHING" },
 params_from_iter(values(vec![entry_id.clone().into(),fill_source.clone().into()])),
    )?;
        Ok(())
    }

    pub(crate) fn link_page_swap_fees_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        page: &IngestPage,
    ) -> StoreResult<()> {
        use rusqlite::{params_from_iter, types::Value};
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(tx, installation, account)?;
        }
        let values = |mut original: Vec<Value>| {
            if let Some((installation, account)) = scope {
                original.push(installation.to_owned().into());
                original.push(account.to_owned().into());
            }
            original
        };
        // Bounded delta linking: a missing fill-side billId is recovered
        // from the immutable authenticated bill->order->trade identity.
        // No historical scans, duplicate ledger or alternate financial owner.
        for source in &page.swap_fill_sources {
            if source.bill_id.is_empty() {
                Self::link_late_swap_fee_sql(tx, scope, &source.fill_id)?;
            }
        }
        for source in &page.bill_order_sources {
            if source.trade_id.is_empty() {
                continue;
            }
            let fill_id: Option<String> = tx
                .query_row(
                    match scope { None => "SELECT f.fill_id FROM fills f
                     JOIN swap_fill_sources s ON s.fill_id=f.fill_id
                     WHERE f.instrument_id=?1 AND f.exchange_order_id=?2
                       AND f.trade_id=?3 AND s.bill_id=''", Some(_) => "SELECT f.fill_id FROM fills f
                     JOIN swap_fill_sources s ON s.fill_id=f.fill_id AND s.installation_id=f.installation_id AND s.account_key=f.account_key
                     WHERE f.instrument_id=?1 AND f.exchange_order_id=?2
                       AND f.trade_id=?3 AND s.bill_id='' AND f.installation_id=?4 AND f.account_key=?5" },
 params_from_iter(values(vec![source.instrument_id.clone().into(),source.exchange_order_id.clone().into(),source.trade_id.clone().into()])),
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(fill_id) = fill_id {
                Self::link_late_swap_fee_sql(tx, scope, &fill_id)?;
            }
        }
        Ok(())
    }
}

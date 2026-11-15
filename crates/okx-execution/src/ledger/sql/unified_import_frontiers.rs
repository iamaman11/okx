//! Translate mutable scan frontiers; immutable source facts/receipts stay unchanged.
use super::*;
use std::collections::BTreeMap;
const REFERENCES: [(&str, &str, &str); 5] = [
    ("fills", "fill_id", "reviewed_fill_rowid"),
    ("bills", "bill_id", "reviewed_bill_rowid"),
    (
        "operation_receipts",
        "operation_id",
        "reviewed_receipt_rowid",
    ),
    ("finance_entries", "entry_id", "reviewed_finance_rowid"),
    ("cash_flows", "event_id", "reviewed_flow_rowid"),
];
pub(super) fn references_rowid(table: &str) -> bool {
    REFERENCES.iter().any(|(name, _, _)| *name == table)
}
fn translated(map: &BTreeMap<i64, i64>, previous: i64) -> StoreResult<i64> {
    let highest = map.last_key_value().map_or(0, |(old, _)| *old);
    if previous < 0 || previous > highest {
        return Err(StoreError::Invalid("UNIFIED_IMPORT_FRONTIER_OUT_OF_RANGE"));
    }
    Ok(map
        .range(..=previous)
        .next_back()
        .map_or(0, |(_, new)| *new))
}
pub(super) fn rebase(
    source: &Connection,
    tx: &Transaction<'_>,
    installation: &str,
    account: &str,
) -> StoreResult<()> {
    for (table, key, column) in REFERENCES {
        let mut mapping = BTreeMap::new();
        let mut original = source.prepare(&format!(
            "SELECT rowid,{} FROM {} ORDER BY rowid",
            quote(key),
            quote(table)
        ))?;
        let mut rows = original.query([])?;
        let mut last_target = 0;
        while let Some(row) = rows.next()? {
            let old: i64 = row.get(0)?;
            let id: String = row.get(1)?;
            let new: i64 = tx.query_row(
                &format!(
                    "SELECT rowid FROM {} WHERE installation_id=?1 AND account_key=?2 AND {}=?3",
                    quote(table),
                    quote(key)
                ),
                params![installation, account, id],
                |r| r.get(0),
            )?;
            if old <= 0 || new <= last_target {
                return Err(StoreError::Invalid("UNIFIED_IMPORT_FRONTIER_ORDER_INVALID"));
            }
            mapping.insert(old, new);
            last_target = new;
        }
        let query = format!(
            "SELECT campaign_id,{} FROM campaign_entry_readiness WHERE installation_id=?1 AND account_key=?2",
            quote(column)
        );
        let readiness: Vec<(Option<String>, i64)> = tx
            .prepare(&query)?
            .query_map(params![installation, account], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?
            .collect::<Result<_, _>>()?;
        for (campaign, previous) in readiness {
            let next = translated(&mapping, previous)?;
            tx.execute(&format!("UPDATE campaign_entry_readiness SET {}=?1 WHERE installation_id=?2 AND account_key=?3 AND campaign_id IS ?4",quote(column)),params![next,installation,account,campaign])?;
        }
        if table == "finance_entries" {
            let previous:Option<i64>=source.query_row("SELECT watermark FROM source_cursors WHERE source='finance_provenance' AND stream_partition='account'",[],|r|r.get(0)).optional()?;
            if let Some(previous) = previous {
                let next = translated(&mapping, previous)?;
                tx.execute("UPDATE source_cursors SET watermark=?1 WHERE installation_id=?2 AND account_key=?3 AND source='finance_provenance' AND stream_partition='account'",params![next,installation,account])?;
            }
        }
    }
    Ok(())
}

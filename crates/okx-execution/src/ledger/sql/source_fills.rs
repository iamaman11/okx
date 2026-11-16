//! Immutable raw fills and pending projection, shared by legacy and unified storage.
//! This operation never advances a coverage cursor or grants settlement/release.
use super::super::*;
type StoredFillRow = (
    String,
    Option<String>,
    i64,
    String,
    String,
    String,
    String,
    String,
);
impl AccountStore {
    pub(crate) fn record_source_fills_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        fills: &[Fill],
    ) -> StoreResult<()> {
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(tx, installation, account)?;
        }
        for f in fills {
            if f.qty <= Decimal::ZERO || f.price <= Decimal::ZERO || f.currency.is_empty() {
                return Err(StoreError::Invalid("INVALID_FILL"));
            }
            f.qty
                .checked_mul(f.price)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            let values = params![
                f.fill_id,
                f.instrument_id,
                f.trade_id,
                f.exchange_order_id,
                f.intent_id,
                f.time,
                exact(f.qty),
                exact(f.price),
                exact(f.fee),
                f.currency,
                f.side
            ];
            let inserted = match scope {
                None => tx.execute(
                    "INSERT INTO fills VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
                     ON CONFLICT(instrument_id,trade_id,exchange_order_id) DO NOTHING", values)?,
                Some((installation, account)) => tx.execute(
                    "INSERT INTO fills VALUES(?12,?13,?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
                     ON CONFLICT(installation_id,account_key,instrument_id,trade_id,exchange_order_id) DO NOTHING",
                    params![f.fill_id,f.instrument_id,f.trade_id,f.exchange_order_id,
                        f.intent_id,f.time,exact(f.qty),exact(f.price),exact(f.fee),f.currency,f.side,installation,account])?,
            };
            if inserted == 1 {
                match scope {
                    None => tx.execute(
                        "INSERT INTO fill_projection_pending VALUES(?1,?2)",
                        params![f.fill_id, f.side],
                    )?,
                    Some((installation, account)) => tx.execute(
                        "INSERT INTO fill_projection_pending VALUES(?1,?2,?3,?4)",
                        params![installation, account, f.fill_id, f.side],
                    )?,
                };
            }
            let old: (String, Option<String>, i64, String, String, String, String, String) = match scope {
                None => tx.query_row(
                    "SELECT fill_id,intent_id,time,side,qty,price,fee,currency FROM fills
                     WHERE instrument_id=?1 AND trade_id=?2 AND exchange_order_id=?3",
                    params![f.instrument_id,f.trade_id,f.exchange_order_id], Self::source_fill_row)?,
                Some((installation, account)) => tx.query_row(
                    "SELECT fill_id,intent_id,time,side,qty,price,fee,currency FROM fills
                     WHERE installation_id=?1 AND account_key=?2 AND instrument_id=?3 AND trade_id=?4 AND exchange_order_id=?5",
                    params![installation,account,f.instrument_id,f.trade_id,f.exchange_order_id],Self::source_fill_row)?,
            };
            if old
                != (
                    f.fill_id.clone(),
                    f.intent_id.clone(),
                    f.time,
                    f.side.clone(),
                    exact(f.qty),
                    exact(f.price),
                    exact(f.fee),
                    f.currency.clone(),
                )
            {
                return Err(StoreError::Invalid("IMMUTABLE_FILL_CONFLICT"));
            }
        }
        Ok(())
    }

    fn source_fill_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredFillRow> {
        Ok((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
            row.get(7)?,
        ))
    }
}

//! Original immutable bills and venue-order links. No debit, coverage or release.
use super::super::*;
type BillIdentity = (String, String, String, i64, String);
impl AccountStore {
    pub(crate) fn record_source_bills_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        bills: &[Bill],
        order_sources: &[BillOrderSource],
    ) -> StoreResult<()> {
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(tx, installation, account)?;
        }
        for bill in bills {
            let old: BillIdentity = match scope {
                None => {
                    tx.execute("INSERT INTO bills VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(bill_id) DO NOTHING",
                        params![bill.bill_id,bill.kind,bill.time,exact(bill.amount),bill.currency,bill.economic_event_key])?;
                    tx.query_row("SELECT kind,amount,currency,time,economic_event_key FROM bills WHERE bill_id=?1",
                        [&bill.bill_id],Self::source_bill_row)?
                }
                Some((installation, account)) => {
                    tx.execute(
                        "INSERT INTO bills VALUES(?1,?2,?3,?4,?5,?6,?7,?8)
                        ON CONFLICT(installation_id,account_key,bill_id) DO NOTHING",
                        params![
                            installation,
                            account,
                            bill.bill_id,
                            bill.kind,
                            bill.time,
                            exact(bill.amount),
                            bill.currency,
                            bill.economic_event_key
                        ],
                    )?;
                    tx.query_row(
                        "SELECT kind,amount,currency,time,economic_event_key FROM bills
                        WHERE installation_id=?1 AND account_key=?2 AND bill_id=?3",
                        params![installation, account, bill.bill_id],
                        Self::source_bill_row,
                    )?
                }
            };
            if old
                != (
                    bill.kind.clone(),
                    exact(bill.amount),
                    bill.currency.clone(),
                    bill.time,
                    bill.economic_event_key.clone(),
                )
            {
                return Err(StoreError::Invalid("IMMUTABLE_BILL_CONFLICT"));
            }
        }
        for source in order_sources {
            if source.bill_id.is_empty()
                || source.exchange_order_id.is_empty()
                || source.exchange_order_id == "0"
                || source.instrument_id.is_empty()
            {
                return Err(StoreError::Invalid("BILL_ORDER_SOURCE_INVALID"));
            }
            let exists: bool = match scope {
                None => tx.query_row("SELECT EXISTS(SELECT 1 FROM bills WHERE bill_id=?1)",[&source.bill_id],|r|r.get(0))?,
                Some((installation, account)) => tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM bills WHERE installation_id=?1 AND account_key=?2 AND bill_id=?3)",
                    params![installation,account,source.bill_id],|r|r.get(0))?,
            };
            if !exists {
                return Err(StoreError::Invalid("BILL_ORDER_SOURCE_WITHOUT_BILL"));
            }
            let old: (String, String, String) = match scope {
                None => {
                    tx.execute("INSERT INTO bill_order_sources VALUES(?1,?2,?3,?4) ON CONFLICT(bill_id) DO NOTHING",
                        params![source.bill_id,source.instrument_id,source.exchange_order_id,source.trade_id])?;
                    tx.query_row("SELECT instrument_id,exchange_order_id,trade_id FROM bill_order_sources WHERE bill_id=?1",
                        [&source.bill_id],Self::source_bill_order_row)?
                }
                Some((installation, account)) => {
                    tx.execute(
                        "INSERT INTO bill_order_sources VALUES(?1,?2,?3,?4,?5,?6)
                        ON CONFLICT(installation_id,account_key,bill_id) DO NOTHING",
                        params![
                            installation,
                            account,
                            source.bill_id,
                            source.instrument_id,
                            source.exchange_order_id,
                            source.trade_id
                        ],
                    )?;
                    tx.query_row(
                        "SELECT instrument_id,exchange_order_id,trade_id FROM bill_order_sources
                        WHERE installation_id=?1 AND account_key=?2 AND bill_id=?3",
                        params![installation, account, source.bill_id],
                        Self::source_bill_order_row,
                    )?
                }
            };
            if old
                != (
                    source.instrument_id.clone(),
                    source.exchange_order_id.clone(),
                    source.trade_id.clone(),
                )
            {
                return Err(StoreError::Invalid("IMMUTABLE_BILL_ORDER_SOURCE_CONFLICT"));
            }
        }
        Ok(())
    }
    fn source_bill_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<BillIdentity> {
        Ok((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
        ))
    }
    fn source_bill_order_row(
        row: &rusqlite::Row<'_>,
    ) -> rusqlite::Result<(String, String, String)> {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    }
}

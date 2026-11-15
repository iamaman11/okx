//! Original pending funding and durable attribution acknowledgements.
use super::super::*;
impl AccountStore {
    pub(crate) fn record_funding_sources_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        events: &[FundingEvent],
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
        for event in events {
            let completed: bool = tx.query_row(
                match scope {None=>"SELECT EXISTS(SELECT 1 FROM operation_receipts WHERE operation_id=?1)",Some(_)=>"SELECT EXISTS(SELECT 1 FROM operation_receipts WHERE operation_id=?1 AND installation_id=?2 AND account_key=?3)"}, params_from_iter(values(vec![format!("funding:{}",event.event_id).into()])),
                |r| r.get(0),
            )?;
            if !completed {
                if let Some((installation, account)) = scope {
                    let found:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM bills WHERE installation_id=?1 AND account_key=?2 AND bill_id=?3)",params![installation,account,event.event_id],|r|r.get(0))?;
                    if !found {
                        return Err(StoreError::Invalid("FUNDING_SOURCE_BILL_MISSING"));
                    }
                }
                tx.execute(match scope {None=>"INSERT INTO funding_projection_pending VALUES(?1,?2,?3,?4,?5) ON CONFLICT(event_id) DO NOTHING",Some(_)=>"INSERT INTO funding_projection_pending VALUES(?6,?7,?1,?2,?3,?4,?5) ON CONFLICT(installation_id,account_key,event_id) DO NOTHING"}, params_from_iter(values(vec![event.event_id.clone().into(),event.instrument_id.clone().into(),event.time.clone().into(),exact(event.amount).clone().into(),event.currency.clone().into()])))?;
            }
        }
        Ok(())
    }
    pub(crate) fn acknowledge_funding_sources_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        ids: &[String],
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
        for id in ids {
            tx.execute(
                match scope {None=>"DELETE FROM funding_projection_pending WHERE event_id=?1",Some(_)=>"DELETE FROM funding_projection_pending WHERE event_id=?1 AND installation_id=?2 AND account_key=?3"}, params_from_iter(values(vec![id.clone().into()])),
            )?;
            tx.execute(match scope {None=>"INSERT INTO operation_receipts VALUES(?1,'FUNDING_ATTRIBUTED',?2,'COMMITTED',?3) ON CONFLICT(operation_id) DO NOTHING",Some(_)=>"INSERT INTO operation_receipts VALUES(?4,?5,?1,'FUNDING_ATTRIBUTED',?2,'COMMITTED',?3) ON CONFLICT(installation_id,account_key,operation_id) DO NOTHING"}, params_from_iter(values(vec![format!("funding:{id}").clone().into(),id.clone().into(),wall_time_ms().clone().into()])))?;
        }
        Ok(())
    }
}

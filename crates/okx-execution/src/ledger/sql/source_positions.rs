//! Original lot ownership and projection ACKs in the financial transaction.
use super::super::*;
impl AccountStore {
    pub(crate) fn record_position_lots_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        lots: &[StoredLot],
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
        for l in lots {
            if l.remaining_qty < Decimal::ZERO
                || l.cost < Decimal::ZERO
                || !matches!(l.side.as_str(), "long" | "short")
            {
                return Err(StoreError::Invalid("INVALID_POSITION_LOT"));
            }
            if let Some(intent) = &l.intent_id {
                let owner:(Option<String>,String)=tx.query_row(match scope {None=>"SELECT instance_id,instrument_id FROM intents JOIN decisions USING(decision_id) WHERE intent_id=?1",Some(_)=>"SELECT d.instance_id,i.instrument_id FROM intents i JOIN decisions d ON d.installation_id=i.installation_id AND d.account_key=i.account_key AND d.decision_id=i.decision_id WHERE i.intent_id=?1 AND i.installation_id=?2 AND i.account_key=?3"}, params_from_iter(values(vec![intent.clone().into()])),|r|Ok((r.get(0)?,r.get(1)?)))?;
                if owner != (l.instance_id.clone(), l.instrument_id.clone()) {
                    return Err(StoreError::Invalid("LOT_OWNER_MISMATCH"));
                }
            } else if l.instance_id.is_some() {
                return Err(StoreError::Invalid("EXTERNAL_FILL_MUST_BE_UNATTRIBUTED"));
            }
            let changed=tx.execute(match scope {None=>"INSERT INTO position_lots VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT(lot_id) DO UPDATE SET remaining_qty=excluded.remaining_qty,cost=excluded.cost WHERE instance_id IS excluded.instance_id AND instrument_id=excluded.instrument_id AND side=excluded.side AND intent_id IS excluded.intent_id AND fill_id IS excluded.fill_id",Some(_)=>"INSERT INTO position_lots VALUES(?10,?11,?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT(installation_id,account_key,lot_id) DO UPDATE SET remaining_qty=excluded.remaining_qty,cost=excluded.cost WHERE instance_id IS excluded.instance_id AND instrument_id=excluded.instrument_id AND side=excluded.side AND intent_id IS excluded.intent_id AND fill_id IS excluded.fill_id"}, params_from_iter(values(vec![l.lot_id.clone().into(),l.instance_id.clone().into(),l.intent_id.clone().into(),l.fill_id.clone().into(),l.instrument_id.clone().into(),l.side.clone().into(),exact(l.remaining_qty).clone().into(),exact(l.cost).clone().into(),l.opened_at.clone().into()])))?;
            if changed != 1 {
                return Err(StoreError::Invalid("LOT_OWNER_MISMATCH"));
            }
        }
        Ok(())
    }
    pub(crate) fn acknowledge_projected_fills_sql(
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
            tx.execute(match scope {None=>"DELETE FROM fill_projection_pending WHERE fill_id=?1",Some(_)=>"DELETE FROM fill_projection_pending WHERE fill_id=?1 AND installation_id=?2 AND account_key=?3"}, params_from_iter(values(vec![id.clone().into()])))?;
        }
        Ok(())
    }
}

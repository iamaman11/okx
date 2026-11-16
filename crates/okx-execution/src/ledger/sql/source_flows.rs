//! Original immutable external cash flows and strategy allocations.
use super::super::*;
impl AccountStore {
    pub(crate) fn record_source_flows_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        flows: &[CashFlow],
        allocations: &[Allocation],
    ) -> StoreResult<bool> {
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
        let mut cash_flows_changed = false;
        for f in flows {
            if f.currency.is_empty() || f.source_ref.is_empty() {
                return Err(StoreError::Invalid("INVALID_CASH_FLOW"));
            }
            cash_flows_changed |= tx.execute(
                match scope {None=>"INSERT INTO cash_flows VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(event_id) DO NOTHING",Some(_)=>"INSERT INTO cash_flows VALUES(?7,?8,?1,?2,?3,?4,?5,?6) ON CONFLICT(installation_id,account_key,event_id) DO NOTHING"}, params_from_iter(values(vec![f.event_id.clone().into(),f.kind.clone().into(),f.currency.clone().into(),exact(f.amount).clone().into(),f.time.into(),f.source_ref.clone().into()])),
            )? > 0;
            let old: (String, String, String, i64, String) = tx.query_row(
                match scope {None=>"SELECT kind,currency,amount,time,source_ref FROM cash_flows WHERE event_id=?1",Some(_)=>"SELECT kind,currency,amount,time,source_ref FROM cash_flows WHERE event_id=?1 AND installation_id=?2 AND account_key=?3"}, params_from_iter(values(vec![f.event_id.clone().into()])),
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )?;
            // A replay cannot silently relocate a deposit or replace its
            // original provenance while reusing the same event id.
            if old
                != (
                    f.kind.clone(),
                    f.currency.clone(),
                    exact(f.amount),
                    f.time,
                    f.source_ref.clone(),
                )
            {
                return Err(StoreError::Invalid("IMMUTABLE_CASH_FLOW_CONFLICT"));
            }
        }
        for a in allocations {
            tx.execute(
                match scope {None=>"INSERT INTO allocations VALUES(?1,?2,?3,?4,?5) ON CONFLICT(event_id) DO NOTHING",Some(_)=>"INSERT INTO allocations VALUES(?6,?7,?1,?2,?3,?4,?5) ON CONFLICT(installation_id,account_key,event_id) DO NOTHING"}, params_from_iter(values(vec![a.event_id.clone().into(),a.instance_id.clone().into(),exact(a.delta).clone().into(),a.currency.clone().into(),a.time.into()])),
            )?;
            let old: (String, String, String, i64) = tx.query_row(
                match scope {None=>"SELECT instance_id,delta,currency,time FROM allocations WHERE event_id=?1",Some(_)=>"SELECT instance_id,delta,currency,time FROM allocations WHERE event_id=?1 AND installation_id=?2 AND account_key=?3"}, params_from_iter(values(vec![a.event_id.clone().into()])),
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?;
            if old
                != (
                    a.instance_id.clone(),
                    exact(a.delta),
                    a.currency.clone(),
                    a.time,
                )
            {
                return Err(StoreError::Invalid("IMMUTABLE_ALLOCATION_CONFLICT"));
            }
        }
        Ok(cash_flows_changed)
    }
}

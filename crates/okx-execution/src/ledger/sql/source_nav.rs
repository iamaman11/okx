//! Original immutable NAV points and account-scoped high-water updates.
use super::super::*;
impl AccountStore {
    pub(crate) fn record_nav_points_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        points: &[NavPoint],
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
        for n in points {
            if n.scope_id.starts_with("strategy:")
                && n.source_version.starts_with("strategy-reconciled-v1")
            {
                return Err(StoreError::Invalid("STRATEGY_NAV_REQUIRES_RECONCILIATION"));
            }
            tx.execute(
                match scope {
                    None => "INSERT INTO nav_scopes VALUES(?1,?2) ON CONFLICT DO NOTHING",
                    Some(_) => "INSERT INTO nav_scopes VALUES(?3,?4,?1,?2) ON CONFLICT DO NOTHING",
                },
                params_from_iter(values(vec![
                    n.scope_id.clone().into(),
                    n.currency.clone().into(),
                ])),
            )?;
            if n.mark_source.is_empty() {
                return Err(StoreError::Invalid("NAV_MARK_SOURCE_REQUIRED"));
            }
            tx.execute(match scope {None=>"INSERT INTO nav_points VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT(scope_id,time,source_version) DO NOTHING",Some(_)=>"INSERT INTO nav_points VALUES(?10,?11,?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT(installation_id,account_key,scope_id,time,source_version) DO NOTHING"}, params_from_iter(values(vec![n.scope_id.clone().into(),n.time.clone().into(),n.source_version.clone().into(),exact(n.equity).clone().into(),exact(n.pnl).clone().into(),exact(n.flows).clone().into(),n.currency.clone().into(),n.coverage.clone().into(),n.mark_source.clone().into()])))?;
            // A replay may not change a previously accepted mark while
            // updating its derived high-water or advancing its source cursor.
            // A legitimate correction needs a distinct immutable source version.
            let saved: (String, String, String, String, String, String) = tx.query_row(
                match scope {None=>"SELECT equity,pnl,flows,currency,coverage,mark_source FROM nav_points WHERE scope_id=?1 AND time=?2 AND source_version=?3",Some(_)=>"SELECT equity,pnl,flows,currency,coverage,mark_source FROM nav_points WHERE scope_id=?1 AND time=?2 AND source_version=?3 AND installation_id=?4 AND account_key=?5"}, params_from_iter(values(vec![n.scope_id.clone().into(),n.time.clone().into(),n.source_version.clone().into()])),
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
            )?;
            if saved
                != (
                    exact(n.equity),
                    exact(n.pnl),
                    exact(n.flows),
                    n.currency.clone(),
                    n.coverage.clone(),
                    n.mark_source.clone(),
                )
            {
                return Err(StoreError::Invalid("IMMUTABLE_NAV_SOURCE_CONFLICT"));
            }
            if n.currency == "USD" {
                let adjusted = n
                    .equity
                    .checked_sub(n.flows)
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
                let prior: Option<(String, i64)> = tx
                    .query_row(
                        match scope {None=>"SELECT highwater,time FROM nav_watermarks WHERE scope_id=?1",Some(_)=>"SELECT highwater,time FROM nav_watermarks WHERE scope_id=?1 AND installation_id=?2 AND account_key=?3"}, params_from_iter(values(vec![n.scope_id.clone().into()])),
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .optional()?;
                if prior.as_ref().is_none_or(|(_, time)| n.time >= *time) {
                    let peak = prior
                        .map(|(v, _)| decimal(v))
                        .transpose()?
                        .unwrap_or(adjusted)
                        .max(adjusted);
                    tx.execute(match scope {None=>"INSERT INTO nav_watermarks VALUES(?1,?2,?3,?4,?5) ON CONFLICT(scope_id) DO UPDATE SET highwater=excluded.highwater,adjusted_equity=excluded.adjusted_equity,time=excluded.time,coverage=excluded.coverage",Some(_)=>"INSERT INTO nav_watermarks VALUES(?6,?7,?1,?2,?3,?4,?5) ON CONFLICT(installation_id,account_key,scope_id) DO UPDATE SET highwater=excluded.highwater,adjusted_equity=excluded.adjusted_equity,time=excluded.time,coverage=excluded.coverage"}, params_from_iter(values(vec![n.scope_id.clone().into(),exact(peak).clone().into(),exact(adjusted).clone().into(),n.time.clone().into(),n.coverage.clone().into()])))?;
                }
            }
        }
        Ok(())
    }
}

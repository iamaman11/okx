//! Immutable valuation revisions and NAV receipts in the caller transaction.
use super::super::*;
impl AccountStore {
    pub(crate) fn strategy_valuation_revision_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        through: i64,
    ) -> StoreResult<String> {
        let mut values = vec![rusqlite::types::Value::from(through)];
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
            values.push(installation.to_owned().into());
            values.push(account.to_owned().into());
        }
        let (finance, fills, bills, lots, flows, nav): (i64, i64, i64, i64, i64, i64) =
            db.query_row(
                match scope {None=>"SELECT
                    (SELECT COALESCE(MAX(rowid),0) FROM finance_entries WHERE time<=?1),
                    (SELECT COALESCE(MAX(rowid),0) FROM fills WHERE time<=?1),
                    (SELECT COALESCE(MAX(rowid),0) FROM bills WHERE time<=?1),
                    (SELECT COALESCE(MAX(rowid),0) FROM position_lots WHERE opened_at<=?1),
                    (SELECT COALESCE(MAX(rowid),0) FROM cash_flows WHERE time<=?1),
                    (SELECT COALESCE(MAX(rowid),0) FROM nav_points
                     WHERE scope_id='account' AND currency='USD' AND time<=?1)",Some(_)=>"SELECT
                    (SELECT COALESCE(MAX(rowid),0) FROM finance_entries WHERE time<=?1 AND installation_id=?2 AND account_key=?3),
                    (SELECT COALESCE(MAX(rowid),0) FROM fills WHERE time<=?1 AND installation_id=?2 AND account_key=?3),
                    (SELECT COALESCE(MAX(rowid),0) FROM bills WHERE time<=?1 AND installation_id=?2 AND account_key=?3),
                    (SELECT COALESCE(MAX(rowid),0) FROM position_lots WHERE opened_at<=?1 AND installation_id=?2 AND account_key=?3),
                    (SELECT COALESCE(MAX(rowid),0) FROM cash_flows WHERE time<=?1 AND installation_id=?2 AND account_key=?3),
                    (SELECT COALESCE(MAX(rowid),0) FROM nav_points
                     WHERE scope_id='account' AND currency='USD' AND time<=?1 AND installation_id=?2 AND account_key=?3)"},
                rusqlite::params_from_iter(values),
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                    ))
                },
            )?;
        Ok(format!(
            "f{finance}:x{fills}:b{bills}:p{lots}:c{flows}:n{nav}"
        ))
    }
    pub(crate) fn commit_reconciled_strategy_nav_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        evidence: &StrategyValuationEvidence,
        cursor: Option<&SourceCursor>,
    ) -> StoreResult<StrategyValuationReconciliation> {
        use rusqlite::{params_from_iter, types::Value};
        let values = |mut original: Vec<Value>| {
            if let Some((installation, account)) = scope {
                original.push(installation.to_owned().into());
                original.push(account.to_owned().into());
            }
            original
        };
        let summary = Self::reconcile_strategy_valuation_sql(tx, scope, evidence)?;
        if summary.strategies.is_empty() && cursor.is_none() {
            return Ok(summary);
        }
        let revision = if summary.strategies.is_empty() {
            String::new()
        } else {
            Self::strategy_valuation_revision_sql(tx, scope, evidence.as_of)?
        };
        let (initialized_at,opening_source):(i64,String)=match scope {
            None=>tx.query_row("SELECT initialized_at,opening_source FROM account_meta WHERE singleton=1",[],|r|Ok((r.get(0)?,r.get(1)?)))?,
            Some((installation,account))=>tx.query_row("SELECT initialized_at,opening_source FROM account_meta WHERE singleton=1 AND installation_id=?1 AND account_key=?2",params![installation,account],|r|Ok((r.get(0)?,r.get(1)?)))?
        };
        for row in &summary.strategies {
            let scope_id = format!("strategy:{}", row.instance_id);
            let points = [
                (
                    initialized_at,
                    Decimal::ZERO,
                    Decimal::ZERO,
                    Decimal::ZERO,
                    opening_source.as_str(),
                    String::from("opening"),
                ),
                (
                    evidence.as_of,
                    row.equity_usd,
                    row.realized_usd
                        .checked_add(row.unrealized_usd)
                        .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
                    row.capital_usd,
                    evidence.account_nav_source.as_str(),
                    format!("revision:{revision}"),
                ),
            ];
            for (time, equity, pnl, flows, source, version) in points {
                // Same as-of can be revalued after an out-of-order fill, bill
                // or account correction. Keep both revisions immutable;
                // readback accepts only the currently verified source revision.
                let source_version = format!("strategy-reconciled-v1:{version}");
                let receipt_id = format!("strategy-nav:{scope_id}:{time}:{source_version}");
                let previous:Option<(String,String,String)>=tx.query_row(
                    match scope {None=>"SELECT kind,request_hash,result_ref FROM operation_receipts WHERE operation_id=?1",Some(_)=>"SELECT kind,request_hash,result_ref FROM operation_receipts WHERE operation_id=?1 AND installation_id=?2 AND account_key=?3"},
                    params_from_iter(values(vec![receipt_id.clone().into()])),|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))
                ).optional()?;
                if previous.is_some_and(|(kind, hash, refid)| {
                    kind != "STRATEGY_NAV_RECONCILED" || hash != version || refid != source
                }) {
                    return Err(StoreError::Invalid("STRATEGY_NAV_WITNESS_CONFLICT"));
                }
                tx.execute(
                    match scope {None=>"INSERT INTO operation_receipts VALUES(?1,'STRATEGY_NAV_RECONCILED',?2,?3,?4) ON CONFLICT(operation_id) DO NOTHING",Some(_)=>"INSERT INTO operation_receipts VALUES(?5,?6,?1,'STRATEGY_NAV_RECONCILED',?2,?3,?4) ON CONFLICT(installation_id,account_key,operation_id) DO NOTHING"},
                    params_from_iter(values(vec![receipt_id.clone().into(),version.clone().into(),source.to_owned().into(),time.into()]))
                )?;
                tx.execute(
                    match scope {None=>"INSERT INTO nav_points VALUES(?1,?2,?3,?4,?5,?6,'USD','COMPLETE',?7) ON CONFLICT(scope_id,time,source_version) DO NOTHING",Some(_)=>"INSERT INTO nav_points VALUES(?8,?9,?1,?2,?3,?4,?5,?6,'USD','COMPLETE',?7) ON CONFLICT(installation_id,account_key,scope_id,time,source_version) DO NOTHING"},
                    params_from_iter(values(vec![scope_id.clone().into(),time.into(),source_version.clone().into(),exact(equity).into(),exact(pnl).into(),exact(flows).into(),source.to_owned().into()]))
                )?;
                let actual:(String,String,String,String)=tx.query_row(
                    match scope {None=>"SELECT equity,pnl,flows,mark_source FROM nav_points WHERE scope_id=?1 AND time=?2 AND source_version=?3",Some(_)=>"SELECT equity,pnl,flows,mark_source FROM nav_points WHERE scope_id=?1 AND time=?2 AND source_version=?3 AND installation_id=?4 AND account_key=?5"},
                    params_from_iter(values(vec![scope_id.clone().into(),time.into(),source_version.clone().into()])),|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))
                )?;
                if actual != (exact(equity), exact(pnl), exact(flows), source.to_owned()) {
                    return Err(StoreError::Invalid("STRATEGY_NAV_WITNESS_CONFLICT"));
                }
            }
            tx.execute(
                match scope {
                    None => "INSERT INTO nav_scopes VALUES(?1,'USD') ON CONFLICT DO NOTHING",
                    Some(_) => {
                        "INSERT INTO nav_scopes VALUES(?2,?3,?1,'USD') ON CONFLICT DO NOTHING"
                    }
                },
                params_from_iter(values(vec![scope_id.clone().into()])),
            )?;
            // No strategy high-water or risk admission from this read model:
            // strategy drawdown and authorization need independent acceptance.
        }
        if let Some(cursor) = cursor {
            Self::upsert_source_cursor_sql(tx, scope, cursor)?;
        }
        Ok(summary)
    }
}

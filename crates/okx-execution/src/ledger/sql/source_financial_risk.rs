//! Committed financial risk read model; never an independent admission grant.
use super::super::*;
impl AccountStore {
    pub(crate) fn financial_risk_sql(
        db: &Connection,
        account_scope: Option<(&str, &str)>,
        now: i64,
        instance: Option<&str>,
    ) -> StoreResult<FinancialRisk> {
        use rusqlite::{params_from_iter, types::Value};
        if let Some((installation, account)) = account_scope {
            Self::verify_source_account_sql(db, installation, account)?;
        }
        let values = |mut original: Vec<Value>| {
            if let Some((installation, account)) = account_scope {
                original.push(installation.to_owned().into());
                original.push(account.to_owned().into());
            }
            original
        };
        let scope = instance
            .map(|s| format!("strategy:{s}"))
            .unwrap_or_else(|| "account".into());
        let daily: Option<String> = db
            .query_row(
                match account_scope {None=>"SELECT loss_usd FROM daily_losses WHERE scope_id=?1 AND utc_day=?2",Some(_)=>"SELECT loss_usd FROM daily_losses WHERE scope_id=?1 AND utc_day=?2 AND installation_id=?3 AND account_key=?4"},
                params_from_iter(values(vec![scope.clone().into(),crate::risk::utc_day(now).into()])),
                |r| r.get(0),
            )
            .optional()?;
        let nav:Option<(String,String,i64,String)>=db.query_row(match account_scope {None=>"SELECT highwater,adjusted_equity,time,coverage FROM nav_watermarks WHERE scope_id=?1",Some(_)=>"SELECT highwater,adjusted_equity,time,coverage FROM nav_watermarks WHERE scope_id=?1 AND installation_id=?2 AND account_key=?3"},params_from_iter(values(vec![scope.clone().into()])),|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
        let (drawdown, asof, complete) = if let Some((peak, equity, time, coverage)) = nav {
            let peak = decimal(peak)?;
            let equity = decimal(equity)?;
            let dd = if peak > Decimal::ZERO {
                peak.checked_sub(equity)
                    .and_then(|v| v.max(Decimal::ZERO).checked_div(peak))
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?
            } else {
                Decimal::ZERO
            };
            (
                dd,
                time,
                coverage == "COMPLETE" && instance.is_none() && now >= time && now - time <= 5000,
            )
        } else {
            (Decimal::ZERO, 0, false)
        };
        Ok(FinancialRisk {
            daily_loss_usd: daily.map(decimal).transpose()?.unwrap_or(Decimal::ZERO),
            drawdown_ratio: drawdown,
            nav_complete: complete,
            source_as_of: asof,
        })
    }
}

//! Original cash-flow and opening-equity reads within the caller's SQL snapshot.
use super::super::*;
impl AccountStore {
    pub(crate) fn cash_flow_totals_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        observe: impl FnOnce(AccountSqlStatementStat),
    ) -> StoreResult<std::collections::BTreeMap<(String, String), Decimal>> {
        let mut values: Vec<rusqlite::types::Value> = Vec::new();
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
            values.push(installation.to_owned().into());
            values.push(account.to_owned().into());
        }
        let mut statement = db
            .prepare(match scope {None=>"SELECT currency,kind,amount FROM cash_flows ORDER BY time,event_id",Some(_)=>"SELECT currency,kind,amount FROM cash_flows WHERE installation_id=?1 AND account_key=?2 ORDER BY time,event_id"})?;
        let started = std::time::Instant::now();
        let mut count = 0_u64;
        let result: StoreResult<std::collections::BTreeMap<(String, String), Decimal>> = (|| {
            let mut rows = statement.query(rusqlite::params_from_iter(values))?;
            let mut totals = std::collections::BTreeMap::new();
            while let Some(row) = rows.next()? {
                let key = (row.get::<_, String>(0)?, row.get::<_, String>(1)?);
                let old = *totals.get(&key).unwrap_or(&Decimal::ZERO);
                let amount = decimal(row.get(2)?)?;
                totals.insert(
                    key,
                    old.checked_add(amount)
                        .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
                );
                count += 1;
            }
            Ok(totals)
        })(
        );
        observe(AccountSqlStatementStat {
            statement_id: "cash_flow_totals_select",
            elapsed_us: u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
            rows: count,
            vm_steps: statement
                .get_status(rusqlite::StatementStatus::VmStep)
                .max(0) as u64,
            fullscan_steps: statement
                .get_status(rusqlite::StatementStatus::FullscanStep)
                .max(0) as u64,
            sorts: statement.get_status(rusqlite::StatementStatus::Sort).max(0) as u64,
            succeeded: result.is_ok(),
        });
        result
    }
    pub(crate) fn opening_equity_usd_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
    ) -> StoreResult<Decimal> {
        let value=match scope {
            None=>db.query_row("SELECT n.equity FROM nav_points n JOIN account_meta a ON n.time=a.initialized_at WHERE n.scope_id='account' AND n.source_version='opening' AND a.singleton=1",[],|r|r.get(0))?,
            Some((installation,account))=> {Self::verify_source_account_sql(db,installation,account)?;db.query_row("SELECT n.equity FROM nav_points n JOIN account_meta a ON n.installation_id=a.installation_id AND n.account_key=a.account_key AND n.time=a.initialized_at WHERE n.scope_id='account' AND n.source_version='opening' AND a.singleton=1 AND a.installation_id=?1 AND a.account_key=?2",params![installation,account],|r|r.get(0))?}
        };
        decimal(value)
    }
    pub(crate) fn usd_component_total_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        component: &str,
    ) -> StoreResult<Decimal> {
        let value: Option<String> = match scope {
            None => db
                .query_row(
                    "SELECT amount FROM finance_totals WHERE currency='USD' AND component=?1",
                    [component],
                    |r| r.get(0),
                )
                .optional()?,
            Some((installation, account)) => {
                Self::verify_source_account_sql(db, installation, account)?;
                db.query_row("SELECT amount FROM finance_totals WHERE currency='USD' AND component=?1 AND installation_id=?2 AND account_key=?3",params![component,installation,account],|r|r.get(0)).optional()?
            }
        };
        value
            .map(decimal)
            .transpose()
            .map(|v| v.unwrap_or(Decimal::ZERO))
    }
}

//! Original bounded finance folds for valuation; all account scopes remain explicit.
use super::super::*;
impl AccountStore {
    pub(crate) fn current_strategy_finance_totals_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
    ) -> StoreResult<FinanceAttribution> {
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
        }
        let mut stmt = db.prepare(
            match scope {None=>"SELECT instance_id,currency,component,amount FROM instance_finance_totals ORDER BY instance_id,currency,component LIMIT 4097",Some(_)=>"SELECT instance_id,currency,component,amount FROM instance_finance_totals WHERE installation_id=?1 AND account_key=?2 ORDER BY instance_id,currency,component LIMIT 4097"},
        )?;
        let mut rows = stmt.query(rusqlite::params_from_iter(
            scope.into_iter().flat_map(|(i, a)| [i, a]),
        ))?;
        let mut totals = std::collections::BTreeMap::new();
        while let Some(r) = rows.next()? {
            let owner: String = r.get(0)?;
            let currency: String = r.get(1)?;
            let component: String = r.get(2)?;
            let amount = decimal(r.get(3)?)?;
            if totals.len() >= 4096 {
                return Err(StoreError::Invalid("FINANCE_ATTRIBUTION_GROUP_LIMIT"));
            }
            totals.insert((Some(owner), currency, component), amount);
        }
        Ok(totals)
    }
    pub(crate) fn finance_attribution_filtered_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        from: i64,
        through: i64,
        instances: &[String],
        currencies: &[String],
    ) -> StoreResult<FinanceAttribution> {
        let (filter, mut args) = super::finance::finance_filter(
            instances,
            currencies,
            if scope.is_some() { 4 } else { 2 },
        )?;
        let mut values = vec![from.into(), through.into()];
        let account_filter = if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
            values.push(installation.to_owned().into());
            values.push(account.to_owned().into());
            " AND installation_id=?3 AND account_key=?4"
        } else {
            ""
        };
        values.append(&mut args);
        let query = format!(
            "SELECT instance_id,currency,component,amount FROM finance_entries WHERE time>=?1 AND time<=?2{account_filter}{filter} ORDER BY time,entry_id"
        );
        let mut s = db.prepare(&query)?;
        let mut rows = s.query(rusqlite::params_from_iter(values))?;
        let mut totals = std::collections::BTreeMap::new();
        while let Some(row) = rows.next()? {
            let key = (
                row.get::<_, Option<String>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            );
            let amount = decimal(row.get(3)?)?;
            let old = *totals.get(&key).unwrap_or(&Decimal::ZERO);
            totals.insert(
                key,
                old.checked_add(amount)
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
            );
            if totals.len() > 4096 {
                return Err(StoreError::Invalid("FINANCE_ATTRIBUTION_GROUP_LIMIT"));
            }
        }
        Ok(totals)
    }
}

//! Bounded canonical finance history and filters, shared by both storage layouts.
use super::super::*;
impl AccountStore {
    #[allow(clippy::too_many_arguments)] // Existing bounded SQL pagination contract.
    pub(crate) fn finance_history_filtered_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        from: i64,
        through: i64,
        after_time: i64,
        after_id: &str,
        limit: usize,
        instances: &[String],
        currencies: &[String],
    ) -> StoreResult<Vec<FinanceEntry>> {
        let (filter, mut args) = super::finance::finance_filter(
            instances,
            currencies,
            if scope.is_some() { 7 } else { 5 },
        )?;
        let mut values = vec![
            from.into(),
            through.into(),
            after_time.into(),
            after_id.to_string().into(),
            (limit.min(1000) as i64).into(),
        ];
        let account_filter = if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
            values.push(installation.to_owned().into());
            values.push(account.to_owned().into());
            " AND installation_id=?6 AND account_key=?7"
        } else {
            ""
        };
        values.append(&mut args);
        let query = format!(
            "SELECT entry_id,economic_event_key,component,allocation_leg,amount,currency,instance_id,source_ref,time,reverses_entry_id FROM finance_entries WHERE time>=?1 AND time<=?2 AND (time,entry_id)>(?3,?4){account_filter}{filter} ORDER BY time,entry_id LIMIT ?5"
        );
        let mut s = db.prepare(&query)?;
        s.query_map(rusqlite::params_from_iter(values), |r| {
            Ok((
                FinanceEntry {
                    entry_id: r.get(0)?,
                    economic_event_key: r.get(1)?,
                    component: r.get(2)?,
                    allocation_leg: r.get(3)?,
                    amount: Decimal::ZERO,
                    currency: r.get(5)?,
                    instance_id: r.get(6)?,
                    source_ref: r.get(7)?,
                    time: r.get(8)?,
                    reverses_entry_id: r.get(9)?,
                },
                r.get::<_, String>(4)?,
            ))
        })?
        .map(|v| {
            let (mut e, a) = v?;
            e.amount = decimal(a)?;
            Ok(e)
        })
        .collect()
    }
}

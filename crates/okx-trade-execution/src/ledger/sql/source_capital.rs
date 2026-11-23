//! Account-backed principal in the caller's SQL snapshot; never independent NAV.
use super::super::*;
impl AccountStore {
    pub(crate) fn latest_capital_assignment_time_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
    ) -> StoreResult<Option<i64>> {
        Ok(match scope {
            None => db.query_row(
                "SELECT MAX(time) FROM finance_entries WHERE component='INTERNAL_CAPITAL_USD'",
                [],
                |r| r.get(0),
            )?,
            Some((installation, account)) => {
                Self::verify_source_account_sql(db, installation, account)?;
                db.query_row("SELECT MAX(time) FROM finance_entries WHERE component='INTERNAL_CAPITAL_USD' AND installation_id=?1 AND account_key=?2",params![installation,account],|r|r.get(0))?
            }
        })
    }
    pub(crate) fn capital_state_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        now: i64,
        latest_assignment: Option<i64>,
    ) -> StoreResult<AccountCapitalState> {
        use rusqlite::{params_from_iter, types::Value};
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
        }
        let values = |mut original: Vec<Value>| {
            if let Some((installation, account)) = scope {
                original.push(installation.to_owned().into());
                original.push(account.to_owned().into());
            }
            original
        };
        let (nav,nav_time,coverage,source):(String,i64,String,String)=db.query_row(
            match scope {None=>"SELECT equity,time,coverage,mark_source FROM nav_points WHERE scope_id='account' AND currency='USD' AND time<=?1 ORDER BY time DESC,source_version DESC LIMIT 1",Some(_)=>"SELECT equity,time,coverage,mark_source FROM nav_points WHERE scope_id='account' AND currency='USD' AND time<=?1 AND installation_id=?2 AND account_key=?3 ORDER BY time DESC,source_version DESC LIMIT 1"},
            params_from_iter(values(vec![now.into()])),|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))
        ).optional()?.ok_or(StoreError::Invalid("ACCOUNT_NAV_SOURCE_MISSING"))?;
        let recovering: i64 = db.query_row(
            match scope {None=>"SELECT requires_reconcile FROM storage_recovery WHERE singleton=1",Some(_)=>"SELECT requires_reconcile FROM storage_recovery WHERE singleton=1 AND installation_id=?1 AND account_key=?2"},
            params_from_iter(values(vec![])),
            |r| r.get(0),
        )?;
        if coverage != "COMPLETE" || now < nav_time || now - nav_time > 5000 || recovering != 0 {
            return Err(StoreError::Invalid(
                "ACCOUNT_CAPITAL_SOURCE_INCOMPLETE_OR_STALE",
            ));
        }
        let account_nav = decimal(nav)?;
        if account_nav < Decimal::ZERO {
            return Err(StoreError::Invalid("ACCOUNT_NAV_NEGATIVE"));
        }
        // instance_finance_totals is a CURRENT projection, not an as-of
        // balance. A later allocation must never appear as historical money.
        // Use the fast projection at the latest point; only historical reads
        // replay the already-immutable, source-backed assignment legs.
        // Immutable finance history belongs on disk, but its latest
        // assignment timestamp need not be scanned at every NAV cycle.
        let historical = latest_assignment.is_some_and(|last| now < last);
        let sql = if historical {
            match scope {
                None => {
                    "SELECT instance_id,amount FROM finance_entries
             WHERE component='INTERNAL_CAPITAL_USD' AND currency='USD'
               AND instance_id IS NOT NULL AND time<=?1
             ORDER BY time,entry_id"
                }
                Some(_) => {
                    "SELECT instance_id,amount FROM finance_entries
             WHERE component='INTERNAL_CAPITAL_USD' AND currency='USD'
               AND installation_id=?2 AND account_key=?3 AND instance_id IS NOT NULL AND time<=?1
             ORDER BY time,entry_id"
                }
            }
        } else {
            match scope {
                None => {
                    "SELECT instance_id,amount FROM instance_finance_totals
             WHERE currency='USD' AND component='INTERNAL_CAPITAL_USD'
             ORDER BY instance_id"
                }
                Some(_) => {
                    "SELECT instance_id,amount FROM instance_finance_totals
             WHERE currency='USD' AND component='INTERNAL_CAPITAL_USD'
             AND installation_id=?1 AND account_key=?2 ORDER BY instance_id"
                }
            }
        };
        let mut query = db.prepare(sql)?;
        let mut rows = if historical {
            query.query(params_from_iter(values(vec![now.into()])))?
        } else {
            query.query(params_from_iter(values(vec![])))?
        };
        let mut principals = std::collections::BTreeMap::new();
        let mut assigned = Decimal::ZERO;
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            let amount = decimal(row.get(1)?)?;
            let next = principals
                .get(&id)
                .copied()
                .unwrap_or(Decimal::ZERO)
                .checked_add(amount)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            if id == "manual" || next < Decimal::ZERO {
                return Err(StoreError::Invalid("INTERNAL_CAPITAL_CORRUPT"));
            }
            principals.insert(id, next);
            assigned = assigned
                .checked_add(amount)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            if principals.len() > 64 {
                return Err(StoreError::Invalid("CAPITAL_SCOPE_LIMIT"));
            }
        }
        let residual = account_nav
            .checked_sub(assigned)
            .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
        // The residual includes economic returns until complete per-owner
        // valuation exists. It must never be presented as trading PnL.
        // Known venue losses can reduce equity below previously assigned
        // principal. Report the deficit, but allow safe deallocation to repair it.
        let under_collateralized = residual < Decimal::ZERO;
        let total:String=db.query_row(
            match scope {None=>"SELECT amount FROM finance_totals WHERE currency='USD' AND component='INTERNAL_CAPITAL_USD'",Some(_)=>"SELECT amount FROM finance_totals WHERE currency='USD' AND component='INTERNAL_CAPITAL_USD' AND installation_id=?1 AND account_key=?2"},
            params_from_iter(values(vec![])),|r|r.get(0)
        ).optional()?.unwrap_or_else(||"0".into());
        if decimal(total)? != Decimal::ZERO {
            return Err(StoreError::Invalid("INTERNAL_CAPITAL_NOT_CONSERVED"));
        }
        Ok(AccountCapitalState {
            account_nav_usd: account_nav,
            assigned_usd: assigned,
            unattributed_residual_usd: residual,
            strategy_principal_usd: principals,
            nav_source_as_of: nav_time,
            nav_source_ref: source,
            coverage: if under_collateralized {
                "PARTIAL"
            } else {
                "COMPLETE"
            }
            .into(),
            reason_code: if under_collateralized {
                Some("ACCOUNT_CAPITAL_OVERASSIGNED".into())
            } else {
                None
            },
        })
    }
}

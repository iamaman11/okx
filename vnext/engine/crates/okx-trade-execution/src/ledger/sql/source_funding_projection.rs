//! Original funding allocation from account-scoped historical fill exposure.
use super::super::*;
pub(crate) struct FundingProjection {
    pub(crate) page: IngestPage,
    pub(crate) unknown: bool,
}
impl AccountStore {
    pub(crate) fn pending_funding_sources_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        limit: usize,
    ) -> StoreResult<Vec<FundingEvent>> {
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
        }
        let query = match scope {
            None => {
                "SELECT event_id,instrument_id,time,amount,currency FROM funding_projection_pending ORDER BY time,event_id LIMIT ?1"
            }
            Some(_) => {
                "SELECT event_id,instrument_id,time,amount,currency FROM funding_projection_pending WHERE installation_id=?2 AND account_key=?3 ORDER BY time,event_id LIMIT ?1"
            }
        };
        let mut values = vec![rusqlite::types::Value::from(limit.min(100) as i64)];
        if let Some((installation, account)) = scope {
            values.push(installation.to_owned().into());
            values.push(account.to_owned().into());
        }
        let mut statement = db.prepare(query)?;
        let mut rows = statement.query(rusqlite::params_from_iter(values))?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            result.push(FundingEvent {
                event_id: row.get(0)?,
                instrument_id: row.get(1)?,
                time: row.get(2)?,
                amount: decimal(row.get(3)?)?,
                currency: row.get(4)?,
            });
        }
        Ok(result)
    }
    pub(crate) fn verify_funding_capture_coverage_sql(
        db: &Connection,
        installation: &str,
        account: &str,
        through: i64,
    ) -> StoreResult<()> {
        let state = Self::source_capture_state_sql(db, Some((installation, account)))?;
        let cursors = [
            state.cursors.spot,
            state.cursors.margin,
            state.cursors.swap,
            state.cursors.futures,
            state.cursors.options,
            state.cursors.bills,
        ];
        let mut generation = None;
        for cursor in cursors {
            let cursor = cursor.ok_or(StoreError::Invalid("FUNDING_SOURCE_HISTORY_INCOMPLETE"))?;
            if cursor.watermark < through
                || !cursor.coverage_gaps.is_empty()
                || generation.is_some_and(|g| g != cursor.generation)
            {
                return Err(StoreError::Invalid("FUNDING_SOURCE_HISTORY_INCOMPLETE"));
            }
            generation = Some(cursor.generation);
        }
        Ok(())
    }
    pub(crate) fn project_funding_source_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        event: &FundingEvent,
    ) -> StoreResult<FundingProjection> {
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
        }
        let mut exposures = std::collections::BTreeMap::<String, Decimal>::new();
        {
            let mut q=db.prepare(match scope {None=>"SELECT d.instance_id,p.operation,p.position_side,f.side,f.qty FROM fills f LEFT JOIN intents i USING(intent_id) LEFT JOIN decisions d USING(decision_id) LEFT JOIN intent_plans p USING(intent_id) WHERE f.instrument_id=?1 AND f.time<=?2 ORDER BY f.time,f.fill_id",Some(_)=>"SELECT d.instance_id,p.operation,p.position_side,f.side,f.qty FROM fills f LEFT JOIN intents i ON i.installation_id=f.installation_id AND i.account_key=f.account_key AND i.intent_id=f.intent_id LEFT JOIN decisions d ON d.installation_id=i.installation_id AND d.account_key=i.account_key AND d.decision_id=i.decision_id LEFT JOIN intent_plans p ON p.installation_id=i.installation_id AND p.account_key=i.account_key AND p.intent_id=i.intent_id WHERE f.instrument_id=?1 AND f.time<=?2 AND f.installation_id=?3 AND f.account_key=?4 ORDER BY f.time,f.fill_id"})?;
            let mut values = vec![
                rusqlite::types::Value::from(event.instrument_id.clone()),
                event.time.into(),
            ];
            if let Some((installation, account)) = scope {
                values.push(installation.to_owned().into());
                values.push(account.to_owned().into());
            }
            let mut rows = q.query(rusqlite::params_from_iter(values))?;
            while let Some(r) = rows.next()? {
                let owner = r
                    .get::<_, Option<String>>(0)?
                    .unwrap_or_else(|| "unattributed".into());
                let operation = r.get::<_, Option<String>>(1)?;
                let position = r.get::<_, Option<String>>(2)?;
                let side: String = r.get(3)?;
                let q = decimal(r.get(4)?)?;
                let sign = match position.as_deref() {
                    Some("long") => {
                        if operation
                            .as_deref()
                            .is_some_and(|o| matches!(o, "close" | "reduce"))
                        {
                            -Decimal::ONE
                        } else {
                            Decimal::ONE
                        }
                    }
                    Some("short") => {
                        if operation
                            .as_deref()
                            .is_some_and(|o| matches!(o, "close" | "reduce"))
                        {
                            Decimal::ONE
                        } else {
                            -Decimal::ONE
                        }
                    }
                    _ => {
                        if side == "buy" {
                            Decimal::ONE
                        } else {
                            -Decimal::ONE
                        }
                    }
                };
                let old = *exposures.get(&owner).unwrap_or(&Decimal::ZERO);
                exposures.insert(
                    owner,
                    old.checked_add(
                        q.checked_mul(sign)
                            .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
                    )
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
                );
            }
        }
        let exposure = exposures
            .into_iter()
            .filter(|(_, q)| *q != Decimal::ZERO)
            .collect::<Vec<_>>();
        let allocations = crate::finance::allocate_funding(event.amount, &exposure, 18)?;
        let unknown = allocations
            .iter()
            .any(|(owner, a)| owner == "unattributed" && *a != Decimal::ZERO);
        let key = format!("bill:{}", event.event_id);
        let mut page = IngestPage::default();
        for (owner, amount) in allocations {
            if amount == Decimal::ZERO && owner != "rounding_residual" {
                continue;
            }
            let instance = if owner == "unattributed" || owner == "rounding_residual" {
                None
            } else {
                Some(owner.clone())
            };
            page.finance.push(FinanceEntry {
                entry_id: format!("{key}:funding:{owner}"),
                economic_event_key: key.clone(),
                component: "FUNDING".into(),
                allocation_leg: owner,
                amount,
                currency: event.currency.clone(),
                instance_id: instance,
                source_ref: key.clone(),
                time: event.time,
                reverses_entry_id: None,
            });
        }
        page.attributed_funding_ids.push(event.event_id.clone());
        Ok(FundingProjection { page, unknown })
    }
}

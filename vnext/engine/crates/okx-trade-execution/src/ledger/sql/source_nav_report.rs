//! Source-bound account and strategy NAV windows, read from one SQL snapshot.
use super::super::*;
impl AccountStore {
    pub(crate) fn nav_summary_sql(
        db: &Connection,
        account_scope: Option<(&str, &str)>,
        from: i64,
        through: i64,
        instances: &[String],
        currencies: &[String],
    ) -> StoreResult<Vec<NavWindow>> {
        use rusqlite::{params_from_iter, types::Value};
        let metadata = Self::source_capture_state_sql(db, account_scope)?;
        let values = |mut original: Vec<Value>| {
            if let Some((installation, account)) = account_scope {
                original.push(installation.to_owned().into());
                original.push(account.to_owned().into());
            }
            original
        };
        if from > through || instances.len() > 32 || currencies.len() > 32 {
            return Err(StoreError::Invalid("NAV_RANGE_FILTER_INVALID"));
        }
        let scopes = {
            let mut q = db
                .prepare(match account_scope {None=>"SELECT scope_id,currency FROM nav_scopes UNION SELECT 'strategy:' || instance_id, 'USD' FROM strategy_instances ORDER BY scope_id,currency",Some(_)=>"SELECT scope_id,currency FROM nav_scopes WHERE installation_id=?1 AND account_key=?2 UNION SELECT 'strategy:' || instance_id, 'USD' FROM strategy_instances WHERE installation_id=?1 AND account_key=?2 ORDER BY scope_id,currency"})?;
            q.query_map(params_from_iter(values(vec![])), |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .filter_map(|v| match v {
                Ok((scope, currency))
                    if (instances.is_empty()
                        || instances.iter().any(|i| scope == format!("strategy:{i}")))
                        && (currencies.is_empty() || currencies.contains(&currency)) =>
                {
                    Some(Ok((scope, currency)))
                }
                Err(e) => Some(Err(e)),
                _ => None,
            })
            .take(65)
            .collect::<Result<Vec<_>, _>>()?
        };
        if scopes.len() > 64 {
            return Err(StoreError::Invalid("NAV_SCOPE_LIMIT"));
        }
        let mut result = Vec::new();
        // Only ordinary strategies need a source-revision witness. Account
        // NAV uses its own source, while manual has no separate strategy NAV.
        // For an ordinary strategy, both endpoints still retain independent
        // immutable as-of revisions: later facts cannot validate old marks.
        let requires_strategy_revision = scopes
            .iter()
            .any(|(scope, _)| scope.starts_with("strategy:") && scope != "strategy:manual");
        let (opening_revision, revision) = if requires_strategy_revision {
            (
                Self::strategy_valuation_revision_sql(db, account_scope, from).map_err(|e| {
                    e.with_interruption_stage("ACCOUNT_SQL_NAV_OPENING_REVISION_INTERRUPTED")
                })?,
                Self::strategy_valuation_revision_sql(db, account_scope, through).map_err(|e| {
                    e.with_interruption_stage("ACCOUNT_SQL_NAV_CLOSING_REVISION_INTERRUPTED")
                })?,
            )
        } else {
            (String::new(), String::new())
        };
        for (scope, currency) in scopes {
            // Only native reconciliation receipts authorize strategy NAV rows.
            // Arbitrary strategy nav_points, including old COMPLETE claims, are
            // ignored. A new financial event invalidates a prior revision.
            let manual = scope == "strategy:manual";
            let strategy = scope.starts_with("strategy:") && !manual;
            let endpoint = |time: i64,
                            source_revision: &str|
             -> StoreResult<Option<(Decimal, Decimal, i64, String)>> {
                let row: Option<(String, String, i64, String)> = if strategy {
                    db.query_row(
                        match account_scope {None=>"SELECT n.equity,n.flows,n.time,n.coverage FROM nav_points n
                         JOIN operation_receipts o ON o.operation_id='strategy-nav:' || n.scope_id || ':' || n.time || ':' || n.source_version
                         WHERE n.scope_id=?1 AND n.time<=?2
                           AND n.source_version IN ('strategy-reconciled-v1:opening',?3)
                           AND o.kind='STRATEGY_NAV_RECONCILED' AND o.result_ref=n.mark_source
                           AND o.request_hash=substr(n.source_version,length('strategy-reconciled-v1:')+1)
                         ORDER BY n.time DESC, n.source_version DESC LIMIT 1",Some(_)=>"SELECT n.equity,n.flows,n.time,n.coverage FROM nav_points n
                         JOIN operation_receipts o ON o.installation_id=n.installation_id AND o.account_key=n.account_key AND o.operation_id='strategy-nav:' || n.scope_id || ':' || n.time || ':' || n.source_version
                         WHERE n.installation_id=?4 AND n.account_key=?5 AND n.scope_id=?1 AND n.time<=?2
                           AND n.source_version IN ('strategy-reconciled-v1:opening',?3)
                           AND o.kind='STRATEGY_NAV_RECONCILED' AND o.result_ref=n.mark_source
                           AND o.request_hash=substr(n.source_version,length('strategy-reconciled-v1:')+1)
                         ORDER BY n.time DESC, n.source_version DESC LIMIT 1"},
                        params_from_iter(values(vec![scope.clone().into(),time.into(),format!("strategy-reconciled-v1:revision:{source_revision}").into()])),
                        |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))
                    ).optional()?
                } else {
                    db.query_row(
                        match account_scope {None=>"SELECT equity,flows,time,coverage FROM nav_points WHERE scope_id=?1 AND time<=?2 ORDER BY time DESC,source_version DESC LIMIT 1",Some(_)=>"SELECT equity,flows,time,coverage FROM nav_points WHERE installation_id=?3 AND account_key=?4 AND scope_id=?1 AND time<=?2 ORDER BY time DESC,source_version DESC LIMIT 1"},
                        params_from_iter(values(vec![scope.clone().into(),time.into()])),|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))
                    ).optional()?
                };
                row.map(|(e, f, t, c)| Ok((decimal(e)?, decimal(f)?, t, c)))
                    .transpose()
            };
            let capital_known=!manual && (!scope.starts_with("strategy:") ||
                db.query_row(
                    match account_scope {None=>"SELECT EXISTS(SELECT 1 FROM nav_points n JOIN operation_receipts o ON
                     o.operation_id='strategy-nav:' || n.scope_id || ':' || n.time || ':' || n.source_version
                     WHERE n.scope_id=?1 AND n.time<=?2 AND n.source_version=?3
                       AND o.kind='STRATEGY_NAV_RECONCILED'
                       AND o.request_hash=substr(n.source_version,length('strategy-reconciled-v1:')+1)
                       AND o.result_ref=n.mark_source)",Some(_)=>"SELECT EXISTS(SELECT 1 FROM nav_points n JOIN operation_receipts o ON
                     o.installation_id=n.installation_id AND o.account_key=n.account_key AND o.operation_id='strategy-nav:' || n.scope_id || ':' || n.time || ':' || n.source_version
                     WHERE n.installation_id=?4 AND n.account_key=?5 AND n.scope_id=?1 AND n.time<=?2 AND n.source_version=?3
                       AND o.kind='STRATEGY_NAV_RECONCILED'
                       AND o.request_hash=substr(n.source_version,length('strategy-reconciled-v1:')+1)
                       AND o.result_ref=n.mark_source)"},
                    params_from_iter(values(vec![scope.clone().into(),through.into(),format!("strategy-reconciled-v1:revision:{revision}").into()])),
                    |r|r.get::<_,bool>(0)
                )?);
            let opening = if capital_known {
                endpoint(from, &opening_revision)?
            } else {
                None
            };
            let current = if capital_known {
                endpoint(through, &revision)?
            } else {
                None
            };
            let flows = opening
                .as_ref()
                .zip(current.as_ref())
                .map(|(o, c)| {
                    c.1.checked_sub(o.1)
                        .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))
                })
                .transpose()?;
            let pnl = opening
                .as_ref()
                .zip(current.as_ref())
                .zip(flows)
                .map(|((o, c), f)| {
                    c.0.checked_sub(o.0)
                        .and_then(|v| v.checked_sub(f))
                        .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))
                })
                .transpose()?;
            let valuation_gap = if strategy {
                let latest:Option<(i64,String)>=db.query_row(
                    match account_scope {None=>"SELECT watermark,coverage_gaps FROM source_cursors WHERE source='strategy_valuation' AND stream_partition='account'",Some(_)=>"SELECT watermark,coverage_gaps FROM source_cursors WHERE installation_id=?1 AND account_key=?2 AND source='strategy_valuation' AND stream_partition='account'"},
                    params_from_iter(values(vec![])),|r|Ok((r.get(0)?,r.get(1)?))
                ).optional()?;
                latest.is_some_and(|(t, g)| {
                    !g.is_empty() && t <= through && current.as_ref().is_none_or(|p| t >= p.2)
                })
            } else {
                false
            };
            // A newer account snapshot supersedes an old strategy witness
            // even when there is no fill, bill or finance entry (e.g. a pure
            // mark-to-market move). Historical reads are checked as-of through.
            let account_source_matches = if strategy {
                let account_latest:Option<(i64,String)>=db.query_row(
                    match account_scope {None=>"SELECT time,mark_source FROM nav_points WHERE scope_id='account'
                     AND currency='USD' AND time<=?1 ORDER BY time DESC,source_version DESC LIMIT 1",Some(_)=>"SELECT time,mark_source FROM nav_points WHERE installation_id=?2 AND account_key=?3 AND scope_id='account'
                     AND currency='USD' AND time<=?1 ORDER BY time DESC,source_version DESC LIMIT 1"},
                    params_from_iter(values(vec![through.into()])),|r|Ok((r.get(0)?,r.get(1)?))
                ).optional()?;
                let strategy_source: Option<String> = if let Some((_, _, time, _)) =
                    current.as_ref()
                {
                    db
                            .query_row(
                                match account_scope {None=>"SELECT mark_source FROM nav_points WHERE scope_id=?1 AND time=?2
                         AND source_version=?3",Some(_)=>"SELECT mark_source FROM nav_points WHERE installation_id=?4 AND account_key=?5 AND scope_id=?1 AND time=?2
                         AND source_version=?3"},
                                params_from_iter(values(vec![scope.clone().into(),(*time).into(),format!("strategy-reconciled-v1:revision:{revision}").into()])),
                                |r| r.get(0),
                            )
                            .optional()?
                } else {
                    None
                };
                account_latest
                    .zip(strategy_source)
                    .is_some_and(|((time, source), witness)| {
                        current
                            .as_ref()
                            .is_some_and(|row| row.2 == time && source == witness)
                    })
            } else {
                true
            };
            let complete = account_source_matches
                && !valuation_gap
                && opening
                    .as_ref()
                    .is_some_and(|v| v.3 == "COMPLETE" && from - v.2 <= 5000)
                && current
                    .as_ref()
                    .is_some_and(|v| v.3 == "COMPLETE" && through - v.2 <= 5000);
            result.push(NavWindow {
                scope_id: scope,
                currency,
                opening_equity: opening.as_ref().map(|v| v.0),
                current_equity: current.as_ref().map(|v| v.0),
                flows,
                pnl,
                opening_as_of: opening.as_ref().map(|v| v.2),
                current_as_of: current.as_ref().map(|v| v.2),
                coverage: if manual {
                    "NOT_APPLICABLE"
                } else if from < metadata.initialized_at {
                    "UNAVAILABLE_BEFORE_T0"
                } else if complete && capital_known {
                    "COMPLETE"
                } else {
                    "PARTIAL"
                }
                .into(),
            });
        }
        Ok(result)
    }
}

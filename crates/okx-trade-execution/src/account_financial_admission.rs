//! Account/strategy financial admission against one original SQL snapshot.
use super::*;
#[derive(Debug, Clone)]
pub struct FinancialAdmissionRequest {
    pub daily_loss_limit_usd: Decimal,
    pub max_drawdown_ratio: Decimal,
    pub candidate_loss_usd: Decimal,
    pub strategy_instance: String,
    pub record_risk_state: bool,
}
/// Incomplete strategy NAV permits only a read-only preview.
pub fn instance_nav_gate(
    nav_complete: bool,
    record_risk_state: bool,
) -> Result<bool, &'static str> {
    if nav_complete {
        Ok(true)
    } else if record_risk_state {
        Err("INSTANCE_NAV_INCOMPLETE")
    } else {
        Ok(false)
    }
}

impl AccountStore {
    pub(crate) fn active_stops_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
    ) -> StoreResult<Vec<(String, String, String, i64)>> {
        let mut values = Vec::<rusqlite::types::Value>::new();
        let sql = if let Some((i, a)) = scope {
            Self::verify_source_account_sql(db, i, a)?;
            values.push(i.to_owned().into());
            values.push(a.to_owned().into());
            "SELECT stop_id,scope,reason,latched_at FROM risk_stops WHERE resolution IS NULL AND installation_id=?1 AND account_key=?2 ORDER BY latched_at,stop_id"
        } else {
            "SELECT stop_id,scope,reason,latched_at FROM risk_stops WHERE resolution IS NULL ORDER BY latched_at,stop_id"
        };
        let mut query = db.prepare(sql)?;
        Ok(query
            .query_map(rusqlite::params_from_iter(values), |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })?
            .collect::<Result<_, _>>()?)
    }
    pub fn check_financial_admission(
        &mut self,
        request: &FinancialAdmissionRequest,
        now: i64,
    ) -> StoreResult<Option<String>> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = Self::check_financial_admission_sql(&tx, None, request, now)?;
        tx.commit()?;
        Ok(result)
    }
    pub(crate) fn check_financial_admission_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        request: &FinancialAdmissionRequest,
        now: i64,
    ) -> StoreResult<Option<String>> {
        if request.daily_loss_limit_usd <= Decimal::ZERO
            || request.max_drawdown_ratio <= Decimal::ZERO
            || request.candidate_loss_usd < Decimal::ZERO
            || request.strategy_instance.is_empty()
        {
            return Err(StoreError::Invalid("FINANCIAL_ADMISSION_LIMIT_INVALID"));
        }
        let risk = Self::financial_risk_sql(db, scope, now, None)?;
        if !risk.nav_complete
            || risk.source_as_of <= 0
            || now < risk.source_as_of
            || now - risk.source_as_of > 5000
        {
            return Err(StoreError::Invalid("FINANCIAL_SOURCE_INCOMPLETE_OR_STALE"));
        }
        // UTC derives from the same owner time as risk/source freshness.
        let utc_day: String = db.query_row(
            "SELECT strftime('%Y-%m-%d',?1 / 1000,'unixepoch')",
            [now],
            |r| r.get(0),
        )?;
        if request.record_risk_state {
            for (id, _, reason, _) in Self::active_stops_sql(db, scope)? {
                if reason == "DAILY_LOSS_LIMIT" && !id.ends_with(&utc_day) {
                    Self::resolve_source_stop_sql(db, scope, &id, "fresh_UTC_window_rollover")?;
                }
            }
        }
        if risk.daily_loss_usd >= request.daily_loss_limit_usd {
            if request.record_risk_state {
                Self::latch_stop_sql(
                    db,
                    scope,
                    &format!("daily:{utc_day}"),
                    "account",
                    "DAILY_LOSS_LIMIT",
                    now,
                )?;
            }
            return Ok(Some("DAILY_LOSS_LIMIT".into()));
        }
        if risk
            .daily_loss_usd
            .checked_add(request.candidate_loss_usd)
            .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?
            > request.daily_loss_limit_usd
        {
            return Ok(Some("DAILY_LOSS_HEADROOM_INSUFFICIENT".into()));
        }
        if risk.drawdown_ratio >= request.max_drawdown_ratio {
            if request.record_risk_state {
                Self::latch_stop_sql(db, scope, "drawdown", "account", "DRAWDOWN_LIMIT", now)?;
            }
            return Ok(Some("DRAWDOWN_LIMIT".into()));
        }
        let instance = &request.strategy_instance;
        if instance != "manual" && Self::instance_sql(db, scope, instance)?.is_some() {
            let own = Self::financial_risk_sql(db, scope, now, Some(instance))?;
            // Preserve original instance_nav_gate: preview can quote with
            // incomplete strategy NAV; actual admission must fail closed.
            let nav_valid = instance_nav_gate(own.nav_complete, request.record_risk_state)
                .map_err(StoreError::Invalid)?;
            if nav_valid
                && (own.daily_loss_usd >= request.daily_loss_limit_usd
                    || own.drawdown_ratio >= request.max_drawdown_ratio)
            {
                if request.record_risk_state {
                    Self::latch_stop_sql(
                        db,
                        scope,
                        &format!("instance:{instance}:risk"),
                        instance,
                        "INSTANCE_LOSS_LIMIT",
                        now,
                    )?;
                }
                return Ok(Some("INSTANCE_LOSS_LIMIT".into()));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const NOW: i64 = 86_400_000;
    fn fixture() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(include_str!("../schema/finance-unified-v1.sql"))
            .unwrap();
        for account in ["A", "B"] {
            db.execute(
                "INSERT INTO account_meta VALUES(1,?1,'installation',2,1,'test','init','test')",
                [account],
            )
            .unwrap();
            db.execute(
                "INSERT INTO account_policy VALUES('installation',?1,1,1,0,'campaign','20','0')",
                [account],
            )
            .unwrap();
            db.execute("INSERT INTO nav_watermarks VALUES('installation',?1,'account','100','100',?2,'COMPLETE')",params![account,NOW]).unwrap();
        }
        db
    }
    fn request(record: bool) -> FinancialAdmissionRequest {
        FinancialAdmissionRequest {
            daily_loss_limit_usd: Decimal::from(10),
            max_drawdown_ratio: Decimal::new(2, 1),
            candidate_loss_usd: Decimal::ONE,
            strategy_instance: "manual".into(),
            record_risk_state: record,
        }
    }
    fn scope() -> Option<(&'static str, &'static str)> {
        Some(("installation", "A"))
    }

    #[test]
    fn financial_admission_preview_retains_every_stop_and_never_latches() {
        let db = fixture();
        AccountStore::latch_stop_sql(
            &db,
            scope(),
            "daily:1970-01-01",
            "account",
            "DAILY_LOSS_LIMIT",
            1,
        )
        .unwrap();
        db.execute(
            "INSERT INTO daily_losses VALUES('installation','A','account',1,'10')",
            [],
        )
        .unwrap();
        let changes = db.total_changes();
        assert_eq!(
            AccountStore::check_financial_admission_sql(&db, scope(), &request(false), NOW)
                .unwrap()
                .as_deref(),
            Some("DAILY_LOSS_LIMIT")
        );
        assert_eq!(db.total_changes(), changes);
        assert_eq!(
            AccountStore::active_stops_sql(&db, scope()).unwrap().len(),
            1
        );
    }

    #[test]
    fn financial_admission_rollover_is_daily_only_and_account_scoped() {
        let db = fixture();
        for (account, id, reason) in [
            ("A", "daily:1970-01-01", "DAILY_LOSS_LIMIT"),
            ("A", "explicit", "OPERATOR_STOP"),
            ("B", "daily:1970-01-01", "DAILY_LOSS_LIMIT"),
        ] {
            AccountStore::latch_stop_sql(
                &db,
                Some(("installation", account)),
                id,
                "account",
                reason,
                1,
            )
            .unwrap();
        }
        db.execute(
            "INSERT INTO daily_losses VALUES('installation','A','account',1,'10')",
            [],
        )
        .unwrap();
        assert_eq!(
            AccountStore::check_financial_admission_sql(&db, scope(), &request(true), NOW)
                .unwrap()
                .as_deref(),
            Some("DAILY_LOSS_LIMIT")
        );
        let active = AccountStore::active_stops_sql(&db, scope()).unwrap();
        assert_eq!(active.len(), 2);
        assert!(active.iter().any(|s| s.0 == "explicit"));
        assert!(active.iter().any(|s| s.0 == "daily:1970-01-02"));
        assert_eq!(
            AccountStore::active_stops_sql(&db, Some(("installation", "B"))).unwrap()[0].0,
            "daily:1970-01-01"
        );
    }

    #[test]
    fn financial_admission_checks_fresh_nav_headroom_and_drawdown() {
        let db = fixture();
        db.execute(
            "UPDATE nav_watermarks SET time=?1 WHERE account_key='A'",
            [NOW - 5001],
        )
        .unwrap();
        assert!(matches!(
            AccountStore::check_financial_admission_sql(&db, scope(), &request(true), NOW),
            Err(StoreError::Invalid("FINANCIAL_SOURCE_INCOMPLETE_OR_STALE"))
        ));
        db.execute(
            "UPDATE nav_watermarks SET time=?1 WHERE account_key='A'",
            [NOW],
        )
        .unwrap();
        db.execute(
            "INSERT INTO daily_losses VALUES('installation','A','account',1,'9.5')",
            [],
        )
        .unwrap();
        assert_eq!(
            AccountStore::check_financial_admission_sql(&db, scope(), &request(true), NOW)
                .unwrap()
                .as_deref(),
            Some("DAILY_LOSS_HEADROOM_INSUFFICIENT")
        );
        db.execute("DELETE FROM daily_losses", []).unwrap();
        db.execute(
            "UPDATE nav_watermarks SET adjusted_equity='80' WHERE account_key='A'",
            [],
        )
        .unwrap();
        assert_eq!(
            AccountStore::check_financial_admission_sql(&db, scope(), &request(true), NOW)
                .unwrap()
                .as_deref(),
            Some("DRAWDOWN_LIMIT")
        );
    }

    #[test]
    fn financial_admission_named_strategy_requires_original_nav_but_preview_remains_readonly() {
        let mut db = fixture();
        let tx = db.transaction().unwrap();
        let instance = StrategyInstance {
            instance_id: "strategy-a".into(),
            definition_id: "strategy".into(),
            definition_version: "1".into(),
            parameters: "{}".into(),
            enabled: false,
            policy_revision: 1,
            budget: Decimal::from(10),
            last_decision_key: None,
        };
        AccountStore::mutate_instance_sql(
            &tx,
            scope(),
            "create-a",
            "a",
            "create",
            &instance,
            None,
            1,
        )
        .unwrap();
        let mut proposed = request(true);
        proposed.strategy_instance = instance.instance_id;
        assert!(matches!(
            AccountStore::check_financial_admission_sql(&tx, scope(), &proposed, NOW),
            Err(StoreError::Invalid("INSTANCE_NAV_INCOMPLETE"))
        ));
        proposed.record_risk_state = false;
        let changes = tx.total_changes();
        assert!(
            AccountStore::check_financial_admission_sql(&tx, scope(), &proposed, NOW)
                .unwrap()
                .is_none()
        );
        assert_eq!(changes, tx.total_changes());
    }
}

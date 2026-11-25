//! Original strategy projections and decision journal on one account-scoped snapshot.
use super::super::*;
impl AccountStore {
    pub(crate) fn verify_strategy_activation_sql(
        db: &Connection,
        scope: (&str, &str),
        instance_id: &str,
        activation_revision: i64,
    ) -> StoreResult<()> {
        let current = Self::instance_sql(db, Some(scope), instance_id)?
            .ok_or(StoreError::Invalid("STRATEGY_INSTANCE_NOT_FOUND"))?;
        if !current.enabled {
            return Err(StoreError::Invalid("STRATEGY_STOPPED"));
        }
        if current.policy_revision != activation_revision {
            return Err(StoreError::Invalid("STRATEGY_ACTIVATION_CHANGED"));
        }
        Ok(())
    }

    pub(crate) fn bind_strategy_send_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: (&str, &str),
        intent: &StoredIntent,
        activation_revision: i64,
        now: i64,
    ) -> StoreResult<()> {
        let operation = format!("strategy-send:{}", intent.intent_id);
        Self::insert_strategy_receipt_sql(
            tx,
            Some(scope),
            &operation,
            "STRATEGY_SEND_ACTIVATION",
            &intent.request_hash,
            &activation_revision.to_string(),
            now,
        )
    }

    pub(crate) fn verify_strategy_send_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: (&str, &str),
        intent_id: &str,
    ) -> StoreResult<()> {
        let operation = format!("strategy-send:{intent_id}");
        let Some(receipt) = Self::operation_sql(tx, Some(scope), &operation)? else {
            return Ok(());
        };
        let (instance, hash): (String, String) = tx.query_row(
            "SELECT p.strategy_instance,i.request_hash FROM intent_plans p
             JOIN intents i USING(installation_id,account_key,intent_id)
             WHERE p.installation_id=?1 AND p.account_key=?2 AND p.intent_id=?3",
            params![scope.0, scope.1, intent_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if receipt.kind != "STRATEGY_SEND_ACTIVATION" || receipt.request_hash != hash {
            return Err(StoreError::Invalid("STRATEGY_SEND_ACTIVATION_CORRUPT"));
        }
        let activation_revision = receipt
            .result_ref
            .parse()
            .map_err(|_| StoreError::Invalid("STRATEGY_SEND_ACTIVATION_CORRUPT"))?;
        Self::verify_strategy_activation_sql(tx, scope, &instance, activation_revision)
    }

    pub(crate) fn strategy_status_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
        now: i64,
    ) -> StoreResult<serde_json::Value> {
        let instance = Self::instance_sql(db, scope, id)?
            .ok_or(StoreError::Invalid("STRATEGY_INSTANCE_NOT_FOUND"))?;
        let mut values = vec![rusqlite::types::Value::from(id.to_owned())];
        if let Some((i, a)) = scope {
            values.push(i.to_owned().into());
            values.push(a.to_owned().into());
        }
        let mut q = db.prepare(match scope {
            None => "SELECT lot_id,intent_id,fill_id,instrument_id,side,remaining_qty,cost,opened_at FROM position_lots WHERE instance_id=?1 AND remaining_qty<>'0' ORDER BY instrument_id,side,opened_at,lot_id LIMIT 51",
            Some(_) => "SELECT lot_id,intent_id,fill_id,instrument_id,side,remaining_qty,cost,opened_at FROM position_lots WHERE instance_id=?1 AND installation_id=?2 AND account_key=?3 AND remaining_qty<>'0' ORDER BY instrument_id,side,opened_at,lot_id LIMIT 51",
        })?;
        let mut positions = q
            .query_map(rusqlite::params_from_iter(values), |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, String>(6)?,
                    r.get::<_, i64>(7)?,
                ))
            })?
            .map(|row| {
                let (lot_id, intent_id, fill_id, instrument_id, side, qty, cost, opened_at) = row?;
                Ok(StoredLot {
                    lot_id,
                    instance_id: Some(id.into()),
                    intent_id,
                    fill_id,
                    instrument_id,
                    side,
                    remaining_qty: decimal(qty)?,
                    cost: decimal(cost)?,
                    opened_at,
                })
            })
            .collect::<StoreResult<Vec<_>>>()?;
        let partial = positions.len() > 50;
        positions.truncate(50);
        let initialized_at = Self::source_capture_state_sql(db, scope)?.initialized_at;
        let mut result = serde_json::json!({"instance":instance,"definition":Self::strategy_definition_sql(db,scope,id)?,"positions":positions,"positions_partial":partial,"limits":{"allocated_budget_usd":instance.budget,"policy_revision":instance.policy_revision},"nav":Self::nav_summary_sql(db,scope,initialized_at,now,&[id.into()],&["USD".into()])?,"capital_scope":if id=="manual" {"account"} else {"strategy"},"risk":Self::financial_risk_sql(db,scope,now,if id=="manual" {None} else {Some(id)})?});
        if let Some((_, account)) = scope.filter(|_| id != "manual") {
            result["account"] = serde_json::json!(account);
            result["configuration_digest"] =
                serde_json::json!(instance.configuration_digest(account)?);
        }
        Ok(result)
    }
    fn latest_strategy_claim_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        base: &str,
    ) -> StoreResult<Option<(String, String, i64, i64)>> {
        let mut values = vec![rusqlite::types::Value::from(base.to_owned())];
        if let Some((i, a)) = scope {
            values.push(i.to_owned().into());
            values.push(a.to_owned().into());
        }
        Ok(db.query_row(match scope {
            None=>"SELECT decision_id,input_digest,policy_revision,rowid FROM decisions WHERE decision_id=?1 OR (decision_id >= (?1 || ':retry:') AND decision_id < (?1 || ':retry;')) ORDER BY rowid DESC LIMIT 1",
            Some(_)=>"SELECT decision_id,input_digest,policy_revision,rowid FROM decisions WHERE installation_id=?2 AND account_key=?3 AND (decision_id=?1 OR (decision_id >= (?1 || ':retry:') AND decision_id < (?1 || ':retry;'))) ORDER BY rowid DESC LIMIT 1",
        },rusqlite::params_from_iter(values),|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?)
    }
    fn strategy_has_intent_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        key: &str,
    ) -> StoreResult<bool> {
        Ok(match scope {
            None=>db.query_row("SELECT EXISTS(SELECT 1 FROM intents WHERE idempotency_key=?1)",[key],|r|r.get(0))?,
            Some((i,a))=>db.query_row("SELECT EXISTS(SELECT 1 FROM intents WHERE installation_id=?1 AND account_key=?2 AND idempotency_key=?3)",params![i,a,key],|r|r.get(0))?,
        })
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "Keep SQL scope, original proof and CAS fields explicit at this bounded boundary."
    )]
    fn insert_strategy_decision_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        id: &str,
        instance: &str,
        digest: &str,
        revision: i64,
        now: i64,
        reason: &str,
    ) -> StoreResult<()> {
        match scope {
            None => tx.execute(
                "INSERT INTO decisions VALUES(?1,?2,?3,?4,?5,?6)",
                params![id, instance, now, digest, revision, reason],
            )?,
            Some((i, a)) => tx.execute(
                "INSERT INTO decisions VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![i, a, id, instance, now, digest, revision, reason],
            )?,
        };
        Ok(())
    }
    fn insert_strategy_receipt_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        id: &str,
        kind: &str,
        digest: &str,
        result: &str,
        now: i64,
    ) -> StoreResult<()> {
        match scope {
            None => tx.execute(
                "INSERT INTO operation_receipts VALUES(?1,?2,?3,?4,?5)",
                params![id, kind, digest, result, now],
            )?,
            Some((i, a)) => tx.execute(
                "INSERT INTO operation_receipts VALUES(?1,?2,?3,?4,?5,?6,?7)",
                params![i, a, id, kind, digest, result, now],
            )?,
        };
        Ok(())
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "Keep SQL scope, original proof and CAS fields explicit at this bounded boundary."
    )]
    pub(crate) fn claim_strategy_decision_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        instance_id: &str,
        decision_key: &str,
        input_digest: &str,
        policy_revision: i64,
        expected_version: Option<&str>,
        now: i64,
    ) -> StoreResult<Option<String>> {
        if policy_revision < 1 {
            return Err(StoreError::Invalid("STRATEGY_POLICY_REVISION_INVALID"));
        }
        let version = Self::strategy_definition_sql(tx, scope, instance_id)?.version;
        if expected_version.is_some_and(|v| v != version) {
            return Err(StoreError::Invalid("STRATEGY_VERSION_CHANGED"));
        }
        let instance = Self::instance_sql(tx, scope, instance_id)?
            .ok_or(StoreError::Invalid("STRATEGY_INSTANCE_NOT_FOUND"))?;
        if !instance.enabled {
            return Err(StoreError::Invalid("STRATEGY_STOPPED"));
        }
        let base = format!("strategy:{instance_id}:{version}:{decision_key}");
        if let Some((prior_id, prior_digest, prior_revision, prior_row)) =
            Self::latest_strategy_claim_sql(tx, scope, &base)?
        {
            if prior_digest != input_digest {
                return Err(StoreError::Invalid("STRATEGY_DECISION_INPUT_CONFLICT"));
            }
            let terminal =
                Self::operation_sql(tx, scope, &format!("strategy-no-trade:{prior_id}"))?.is_some();
            if Self::strategy_has_intent_sql(tx, scope, decision_key)? || terminal {
                return Ok(None);
            }
            if prior_revision == policy_revision {
                return Ok(Some(prior_id));
            }
            if policy_revision < prior_revision {
                return Err(StoreError::Invalid("STRATEGY_POLICY_REVISION_REGRESSION"));
            }
            let successor = format!("{base}:retry:{prior_row}:policy:{policy_revision}");
            Self::insert_strategy_decision_sql(
                tx,
                scope,
                &successor,
                instance_id,
                input_digest,
                policy_revision,
                now,
                "strategy-bar/policy-retry-v1",
            )?;
            Self::insert_strategy_receipt_sql(
                tx,
                scope,
                &format!("strategy-policy-retry:{prior_id}"),
                "strategy_policy_retry",
                input_digest,
                &successor,
                now,
            )?;
            return Ok(Some(successor));
        }
        Self::insert_strategy_decision_sql(
            tx,
            scope,
            &base,
            instance_id,
            input_digest,
            policy_revision,
            now,
            "strategy-bar/v1",
        )?;
        Ok(Some(base))
    }
    pub(crate) fn finish_strategy_no_trade_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        instance_id: &str,
        decision_key: &str,
        expected_version: Option<&str>,
        now: i64,
    ) -> StoreResult<()> {
        let version = Self::strategy_definition_sql(tx, scope, instance_id)?.version;
        if expected_version.is_some_and(|v| v != version) {
            return Err(StoreError::Invalid("STRATEGY_VERSION_CHANGED"));
        }
        let base = format!("strategy:{instance_id}:{version}:{decision_key}");
        let (id, digest, _, _) = Self::latest_strategy_claim_sql(tx, scope, &base)?
            .ok_or(StoreError::Invalid("STRATEGY_CLAIM_MISSING"))?;
        if Self::strategy_has_intent_sql(tx, scope, decision_key)? {
            return Err(StoreError::Invalid("STRATEGY_ALREADY_HAS_INTENT"));
        }
        let operation = format!("strategy-no-trade:{id}");
        if Self::operation_sql(tx, scope, &operation)?.is_none() {
            Self::insert_strategy_receipt_sql(
                tx,
                scope,
                &operation,
                "strategy_no_trade",
                &digest,
                "NO_TRADE",
                now,
            )?;
        }
        match scope {
            None=>tx.execute("UPDATE strategy_instances SET last_decision_key=?2 WHERE instance_id=?1",params![instance_id,decision_key])?,
            Some((i,a))=>tx.execute("UPDATE strategy_instances SET last_decision_key=?2 WHERE instance_id=?1 AND installation_id=?3 AND account_key=?4",params![instance_id,decision_key,i,a])?,
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn database() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(include_str!("../../../schema/finance-unified-v1.sql"))
            .unwrap();
        db.pragma_update(None, "foreign_keys", "ON").unwrap();
        for account in ["strategy-A", "strategy-B"] {
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
        }
        db
    }
    fn create(tx: &rusqlite::Transaction<'_>, account: &str) -> StrategyInstance {
        let instance = StrategyInstance {
            instance_id: "same-instance".into(),
            definition_id: "no_trade".into(),
            definition_version: "1".into(),
            parameters: "{}".into(),
            enabled: true,
            policy_revision: 1,
            budget: Decimal::ONE,
            last_decision_key: None,
        };
        let receipt = AccountStore::mutate_instance_sql(
            tx,
            Some(("installation", account)),
            "same-create",
            "create-hash",
            "create",
            &instance,
            None,
            2,
        )
        .unwrap();
        serde_json::from_str(&receipt.result_ref).unwrap()
    }
    #[test]
    fn native_strategy_contract_mutation_replay_conflict_revision_and_status_are_scoped() {
        let mut db = database();
        let tx = db.transaction().unwrap();
        let a = create(&tx, "strategy-A");
        let b = create(&tx, "strategy-B");
        assert_eq!(a.definition_version, b.definition_version);
        let scope = Some(("installation", "strategy-A"));
        let changes = tx.total_changes();
        let replay = AccountStore::mutate_instance_sql(
            &tx,
            scope,
            "same-create",
            "create-hash",
            "create",
            &a,
            None,
            3,
        )
        .unwrap();
        assert_eq!(tx.total_changes(), changes);
        assert_eq!(
            serde_json::from_str::<StrategyInstance>(&replay.result_ref)
                .unwrap()
                .definition_version,
            a.definition_version
        );
        assert!(matches!(
            AccountStore::mutate_instance_sql(
                &tx,
                scope,
                "same-create",
                "different",
                "create",
                &a,
                None,
                3
            ),
            Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"))
        ));
        let mut updated = a.clone();
        updated.parameters = "{\"fast\":3}".into();
        updated.policy_revision = 2;
        let changes = tx.total_changes();
        assert!(matches!(
            AccountStore::mutate_instance_sql(
                &tx,
                scope,
                "update",
                "update-hash",
                "update",
                &updated,
                Some(1),
                4,
            ),
            Err(StoreError::Invalid("STRATEGY_UPDATE_REQUIRES_STOPPED"))
        ));
        assert_eq!(tx.total_changes(), changes);
        assert!(
            AccountStore::operation_sql(&tx, scope, "update")
                .unwrap()
                .is_none()
        );

        let mut stopped = a.clone();
        stopped.policy_revision = 2;
        let receipt = AccountStore::mutate_instance_sql(
            &tx,
            scope,
            "stop",
            "stop-hash",
            "stop",
            &stopped,
            Some(1),
            5,
        )
        .unwrap();
        let stopped: StrategyInstance = serde_json::from_str(&receipt.result_ref).unwrap();
        assert!(!stopped.enabled);
        assert_eq!(stopped.definition_version, a.definition_version);

        updated.enabled = stopped.enabled;
        updated.policy_revision = 3;
        let receipt = AccountStore::mutate_instance_sql(
            &tx,
            scope,
            "update",
            "update-hash",
            "update",
            &updated,
            Some(stopped.policy_revision),
            6,
        )
        .unwrap();
        let updated: StrategyInstance = serde_json::from_str(&receipt.result_ref).unwrap();
        assert_ne!(updated.definition_version, a.definition_version);
        assert!(matches!(
            AccountStore::mutate_instance_sql(
                &tx,
                scope,
                "stale-update",
                "hash",
                "update",
                &updated,
                Some(stopped.policy_revision),
                7
            ),
            Err(StoreError::Invalid("INSTANCE_REVISION_CONFLICT"))
        ));
        for index in 0..51 {
            tx.execute("INSERT INTO position_lots VALUES('installation','strategy-A',?1,'same-instance',NULL,NULL,'BTC-USDT-SWAP','long','1','100',5)",[format!("lot-{index:03}")]).unwrap();
        }
        tx.execute("INSERT INTO position_lots VALUES('installation','strategy-B','lot-000','same-instance',NULL,NULL,'ETH-USDT-SWAP','short','2','200',5)",[]).unwrap();
        let status = AccountStore::strategy_status_sql(&tx, scope, &a.instance_id, 6).unwrap();
        assert_eq!(status["instance"]["enabled"], false);
        assert_eq!(status["positions"].as_array().unwrap().len(), 50);
        assert_eq!(status["positions_partial"], true);
        assert!(
            status["positions"]
                .as_array()
                .unwrap()
                .iter()
                .all(|v| v["instrument_id"] == "BTC-USDT-SWAP")
        );
        assert_eq!(status["instance"]["policy_revision"], 3);
        assert_eq!(status["definition"]["version"], updated.definition_version);
        let other = AccountStore::strategy_status_sql(
            &tx,
            Some(("installation", "strategy-B")),
            &a.instance_id,
            6,
        )
        .unwrap();
        assert_eq!(other["instance"]["enabled"], true);
        assert_eq!(other["positions"].as_array().unwrap().len(), 1);
        assert_eq!(other["positions_partial"], false);
        assert_eq!(other["positions"][0]["instrument_id"], "ETH-USDT-SWAP");
        assert_eq!(other["instance"]["policy_revision"], 1);
        assert_eq!(other["definition"]["version"], b.definition_version);
        tx.commit().unwrap();
    }
    #[test]
    fn native_strategy_contract_claim_retry_and_no_trade_are_exact_and_scoped() {
        let mut db = database();
        let tx = db.transaction().unwrap();
        let a = create(&tx, "strategy-A");
        create(&tx, "strategy-B");
        let scope = Some(("installation", "strategy-A"));
        let other = Some(("installation", "strategy-B"));
        let claim = |tx: &rusqlite::Transaction<'_>, scope, rev, digest| {
            AccountStore::claim_strategy_decision_sql(
                tx,
                scope,
                "same-instance",
                "same-bar",
                digest,
                rev,
                Some(&a.definition_version),
                7,
            )
        };
        let first = claim(&tx, scope, 1, "inputs").unwrap().unwrap();
        assert_eq!(claim(&tx, scope, 1, "inputs").unwrap(), Some(first.clone()));
        assert!(matches!(
            claim(&tx, scope, 1, "different"),
            Err(StoreError::Invalid("STRATEGY_DECISION_INPUT_CONFLICT"))
        ));
        let second = claim(&tx, scope, 2, "inputs").unwrap().unwrap();
        assert_ne!(first, second);
        assert_eq!(
            AccountStore::operation_sql(&tx, scope, &format!("strategy-policy-retry:{first}"))
                .unwrap()
                .unwrap()
                .result_ref,
            second
        );
        assert!(matches!(
            claim(&tx, scope, 1, "inputs"),
            Err(StoreError::Invalid("STRATEGY_POLICY_REVISION_REGRESSION"))
        ));
        assert!(matches!(
            AccountStore::claim_strategy_decision_sql(
                &tx,
                scope,
                "same-instance",
                "same-bar",
                "inputs",
                2,
                Some("stale-version"),
                7
            ),
            Err(StoreError::Invalid("STRATEGY_VERSION_CHANGED"))
        ));
        // Identical account-local instance, bar and decision IDs remain independent.
        assert_eq!(claim(&tx, other, 1, "inputs").unwrap(), Some(first));
        AccountStore::finish_strategy_no_trade_sql(
            &tx,
            scope,
            "same-instance",
            "same-bar",
            Some(&a.definition_version),
            8,
        )
        .unwrap();
        let receipt =
            AccountStore::operation_sql(&tx, scope, &format!("strategy-no-trade:{second}"))
                .unwrap()
                .unwrap();
        assert_eq!(receipt.kind, "strategy_no_trade");
        assert_eq!(receipt.request_hash, "inputs");
        assert_eq!(receipt.result_ref, "NO_TRADE");
        AccountStore::finish_strategy_no_trade_sql(
            &tx,
            scope,
            "same-instance",
            "same-bar",
            Some(&a.definition_version),
            9,
        )
        .unwrap();
        assert_eq!(
            AccountStore::operation_sql(&tx, scope, &format!("strategy-no-trade:{second}"))
                .unwrap()
                .unwrap()
                .committed_at,
            8
        );
        assert!(claim(&tx, scope, 3, "inputs").unwrap().is_none());
        assert!(claim(&tx, other, 1, "inputs").unwrap().is_some());
        assert!(
            AccountStore::operation_sql(&tx, other, &format!("strategy-no-trade:{second}"))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            AccountStore::instance_sql(&tx, scope, "same-instance")
                .unwrap()
                .unwrap()
                .last_decision_key
                .as_deref(),
            Some("same-bar")
        );
        assert_eq!(
            AccountStore::instance_sql(&tx, other, "same-instance")
                .unwrap()
                .unwrap()
                .last_decision_key,
            None
        );
        tx.commit().unwrap();
    }
    #[test]
    fn native_strategy_contract_existing_intent_blocks_reclaim_and_no_trade_only_in_its_account() {
        let mut db = database();
        let tx = db.transaction().unwrap();
        let a = create(&tx, "strategy-A");
        create(&tx, "strategy-B");
        let scope = Some(("installation", "strategy-A"));
        let other = Some(("installation", "strategy-B"));
        let first = AccountStore::claim_strategy_decision_sql(
            &tx,
            scope,
            "same-instance",
            "same-bar",
            "inputs",
            1,
            Some(&a.definition_version),
            7,
        )
        .unwrap()
        .unwrap();
        tx.execute("INSERT INTO intents(installation_id,account_key,intent_id,idempotency_key,request_hash,decision_id,state,revision,client_order_id,deadline,plan_hash,policy_revision,risk_increase,instrument_id,created_at,updated_at) VALUES('installation','strategy-A','same-intent','same-bar','hash',?1,'PREPARED',1,'client',100,'plan',1,1,'BTC-USDT-SWAP',7,7)",[first]).unwrap();
        assert!(
            AccountStore::claim_strategy_decision_sql(
                &tx,
                scope,
                "same-instance",
                "same-bar",
                "inputs",
                2,
                Some(&a.definition_version),
                8
            )
            .unwrap()
            .is_none()
        );
        assert!(matches!(
            AccountStore::finish_strategy_no_trade_sql(
                &tx,
                scope,
                "same-instance",
                "same-bar",
                Some(&a.definition_version),
                8
            ),
            Err(StoreError::Invalid("STRATEGY_ALREADY_HAS_INTENT"))
        ));
        assert!(
            AccountStore::claim_strategy_decision_sql(
                &tx,
                other,
                "same-instance",
                "same-bar",
                "inputs",
                1,
                Some(&a.definition_version),
                8
            )
            .unwrap()
            .is_some()
        );
        AccountStore::finish_strategy_no_trade_sql(
            &tx,
            other,
            "same-instance",
            "same-bar",
            Some(&a.definition_version),
            9,
        )
        .unwrap();
        tx.commit().unwrap();
    }
}

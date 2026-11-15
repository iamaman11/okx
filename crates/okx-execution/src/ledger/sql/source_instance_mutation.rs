//! Same original strategy mutation and immutable definition rules, scoped SQL.
use super::super::*;
impl AccountStore {
    pub(crate) fn instances_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
    ) -> StoreResult<Vec<StrategyInstance>> {
        if let Some((i, a)) = scope {
            Self::verify_source_account_sql(db, i, a)?;
        }
        let (sql, values) = match scope {
            Some((i, a)) => (
                "SELECT instance_id FROM strategy_instances WHERE installation_id=?1 AND account_key=?2 ORDER BY instance_id",
                vec![
                    rusqlite::types::Value::from(i.to_owned()),
                    a.to_owned().into(),
                ],
            ),
            None => (
                "SELECT instance_id FROM strategy_instances ORDER BY instance_id",
                vec![],
            ),
        };
        let mut query = db.prepare(sql)?;
        let ids = query
            .query_map(rusqlite::params_from_iter(values), |r| {
                r.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| {
                Self::instance_sql(db, scope, &id)?.ok_or(StoreError::Invalid("INSTANCE_NOT_FOUND"))
            })
            .collect()
    }
    pub(crate) fn strategy_definition_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
    ) -> StoreResult<StrategyDefinitionVersion> {
        if let Some((i, a)) = scope {
            Self::verify_source_account_sql(db, i, a)?;
        }
        let sql = match scope {
            Some(_) => {
                "SELECT v.definition_id,v.version,v.parameters,v.parameters_digest,v.created_at FROM strategy_instances i JOIN strategy_definition_versions v ON v.installation_id=i.installation_id AND v.account_key=i.account_key AND v.definition_id=i.definition_id AND v.version=i.definition_version AND v.parameters_digest=i.parameters_digest WHERE i.instance_id=?1 AND i.installation_id=?2 AND i.account_key=?3"
            }
            None => {
                "SELECT v.definition_id,v.version,v.parameters,v.parameters_digest,v.created_at FROM strategy_instances i JOIN strategy_definition_versions v ON v.definition_id=i.definition_id AND v.version=i.definition_version AND v.parameters_digest=i.parameters_digest WHERE i.instance_id=?1"
            }
        };
        let mut values = vec![rusqlite::types::Value::from(id.to_owned())];
        if let Some((i, a)) = scope {
            values.push(i.to_owned().into());
            values.push(a.to_owned().into());
        }
        Ok(db.query_row(sql, rusqlite::params_from_iter(values), |r| {
            Ok(StrategyDefinitionVersion {
                definition_id: r.get(0)?,
                version: r.get(1)?,
                parameters: r.get(2)?,
                parameters_digest: r.get(3)?,
                created_at: r.get(4)?,
            })
        })?)
    }
    pub(crate) fn persist_definition_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        definition: &StrategyDefinitionVersion,
    ) -> StoreResult<()> {
        let saved: (String, String) = match scope {
            Some((i, a)) => {
                Self::verify_source_account_sql(tx, i, a)?;
                tx.execute("INSERT INTO strategy_definition_versions VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(installation_id,account_key,definition_id,version) DO NOTHING",params![i,a,definition.definition_id,definition.version,definition.parameters,definition.parameters_digest,definition.created_at])?;
                tx.query_row("SELECT parameters,parameters_digest FROM strategy_definition_versions WHERE installation_id=?1 AND account_key=?2 AND definition_id=?3 AND version=?4",params![i,a,definition.definition_id,definition.version],|r|Ok((r.get(0)?,r.get(1)?)))?
            }
            None => {
                tx.execute("INSERT INTO strategy_definition_versions VALUES(?1,?2,?3,?4,?5) ON CONFLICT(definition_id,version) DO NOTHING",params![definition.definition_id,definition.version,definition.parameters,definition.parameters_digest,definition.created_at])?;
                tx.query_row("SELECT parameters,parameters_digest FROM strategy_definition_versions WHERE definition_id=?1 AND version=?2",params![definition.definition_id,definition.version],|r|Ok((r.get(0)?,r.get(1)?)))?
            }
        };
        if saved
            != (
                definition.parameters.clone(),
                definition.parameters_digest.clone(),
            )
        {
            return Err(StoreError::Invalid(
                "IMMUTABLE_STRATEGY_DEFINITION_CONFLICT",
            ));
        }
        Ok(())
    }
    pub(crate) fn mutate_instance_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        operation_id: &str,
        request_hash: &str,
        action: &str,
        instance: &StrategyInstance,
        expected_revision: Option<i64>,
        now: i64,
    ) -> StoreResult<OperationReceipt> {
        let kind = format!("STRATEGY_{action}");
        if let Some(old) = Self::operation_sql(tx, scope, operation_id)? {
            if old.request_hash != request_hash || old.kind != kind {
                return Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"));
            }
            return Ok(old);
        }
        if !matches!(action, "create" | "update" | "start" | "stop")
            || instance.budget < Decimal::ZERO
        {
            return Err(StoreError::Invalid("INVALID_INSTANCE_MUTATION"));
        }
        let prior = Self::instance_sql(tx, scope, &instance.instance_id)?;
        if action == "create" && prior.is_some() {
            return Err(StoreError::Invalid("INSTANCE_ALREADY_EXISTS"));
        }
        if action != "create" && prior.is_none() {
            return Err(StoreError::Invalid("INSTANCE_NOT_FOUND"));
        }
        if prior.as_ref().map(|i| i.policy_revision) != expected_revision {
            return Err(StoreError::Invalid("INSTANCE_REVISION_CONFLICT"));
        }
        let next_revision = prior
            .as_ref()
            .map(|i| {
                i.policy_revision
                    .checked_add(1)
                    .ok_or(StoreError::Invalid("INSTANCE_REVISION_OVERFLOW"))
            })
            .transpose()?
            .unwrap_or(1);
        if instance.policy_revision != next_revision {
            return Err(StoreError::Invalid("INSTANCE_REVISION_CONFLICT"));
        }
        let mut committed = if matches!(action, "start" | "stop") {
            prior
                .clone()
                .ok_or(StoreError::Invalid("INSTANCE_NOT_FOUND"))?
        } else {
            instance.clone()
        };
        committed.policy_revision = next_revision;
        committed.last_decision_key = prior.as_ref().and_then(|i| i.last_decision_key.clone());
        committed.enabled = match action {
            "start" => true,
            "stop" => false,
            _ => committed.enabled,
        };
        let prior_definition = prior
            .as_ref()
            .map(|_| Self::strategy_definition_sql(tx, scope, &instance.instance_id))
            .transpose()?;
        let mut candidate = super::definitions::canonical_definition(&committed, 1, now)?;
        let changed = prior_definition.as_ref().is_none_or(|d| {
            d.definition_id != candidate.definition_id
                || d.parameters_digest != candidate.parameters_digest
        });
        let definition = if changed {
            let revision = if let Some(old) = &prior_definition {
                old.version
                    .split('-')
                    .next()
                    .and_then(|v| v.parse::<u64>().ok())
                    .and_then(|v| v.checked_add(1))
                    .ok_or(StoreError::Invalid("DEFINITION_VERSION_OVERFLOW"))?
            } else {
                instance
                    .definition_version
                    .split('-')
                    .next()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .ok_or(StoreError::Invalid("DEFINITION_VERSION_INVALID"))?
            };
            candidate = super::definitions::canonical_definition(&committed, revision, now)?;
            candidate
        } else {
            prior_definition.ok_or(StoreError::Invalid("DEFINITION_VERSION_MISSING"))?
        };
        committed.definition_id = definition.definition_id.clone();
        committed.definition_version = definition.version.clone();
        committed.parameters = definition.parameters.clone();
        Self::persist_definition_sql(tx, scope, &definition)?;
        let mut values = vec![
            rusqlite::types::Value::from(committed.instance_id.clone()),
            committed.definition_id.clone().into(),
            committed.definition_version.clone().into(),
            committed.parameters.clone().into(),
            i64::from(committed.enabled).into(),
            committed.policy_revision.into(),
            exact(committed.budget).into(),
            committed.last_decision_key.clone().into(),
            definition.parameters_digest.clone().into(),
        ];
        let changed = if action == "create" {
            let sql = if let Some((i, a)) = scope {
                values.push(i.to_owned().into());
                values.push(a.to_owned().into());
                "INSERT INTO strategy_instances(installation_id,account_key,instance_id,definition_id,definition_version,parameters,enabled,policy_revision,budget,last_decision_key,parameters_digest) VALUES(?10,?11,?1,?2,?3,?4,?5,?6,?7,?8,?9)"
            } else {
                "INSERT INTO strategy_instances VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)"
            };
            tx.execute(sql, rusqlite::params_from_iter(values))?
        } else {
            // Update preserves the original last decision key; it is not caller authority.
            values.remove(7);
            values.push(expected_revision.into());
            let sql = if let Some((i, a)) = scope {
                values.push(i.to_owned().into());
                values.push(a.to_owned().into());
                "UPDATE strategy_instances SET definition_id=?2,definition_version=?3,parameters=?4,enabled=?5,policy_revision=?6,budget=?7,parameters_digest=?8 WHERE instance_id=?1 AND policy_revision=?9 AND installation_id=?10 AND account_key=?11"
            } else {
                "UPDATE strategy_instances SET definition_id=?2,definition_version=?3,parameters=?4,enabled=?5,policy_revision=?6,budget=?7,parameters_digest=?8 WHERE instance_id=?1 AND policy_revision=?9"
            };
            tx.execute(sql, rusqlite::params_from_iter(values))?
        };
        if changed != 1 {
            return Err(StoreError::Invalid("INSTANCE_REVISION_CONFLICT"));
        }
        if matches!(action, "create" | "update") {
            let delta = committed
                .budget
                .checked_sub(prior.as_ref().map_or(Decimal::ZERO, |i| i.budget))
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            if delta != Decimal::ZERO {
                match scope {
                    Some((i, a)) => {
                        tx.execute(
                            "INSERT INTO allocations VALUES(?1,?2,?3,?4,?5,'USD',?6)",
                            params![
                                i,
                                a,
                                format!("allocation:{operation_id}"),
                                committed.instance_id,
                                exact(delta),
                                now
                            ],
                        )?;
                    }
                    None => {
                        tx.execute(
                            "INSERT INTO allocations VALUES(?1,?2,?3,'USD',?4)",
                            params![
                                format!("allocation:{operation_id}"),
                                committed.instance_id,
                                exact(delta),
                                now
                            ],
                        )?;
                    }
                }
            }
        }
        if action == "create" {
            let nav_scope = format!("strategy:{}", committed.instance_id);
            match scope {
                Some((i, a)) => {
                    tx.execute(
                        "INSERT INTO nav_scopes VALUES(?1,?2,?3,'USD')",
                        params![i, a, nav_scope],
                    )?;
                }
                None => {
                    tx.execute("INSERT INTO nav_scopes VALUES(?1,'USD')", [nav_scope])?;
                }
            }
        }
        let result = serde_json::to_string(&committed)
            .map_err(|_| StoreError::Invalid("INSTANCE_SERIALIZATION_FAILED"))?;
        match scope {
            Some((i, a)) => {
                tx.execute(
                    "INSERT INTO operation_receipts VALUES(?1,?2,?3,?4,?5,?6,?7)",
                    params![i, a, operation_id, kind, request_hash, result, now],
                )?;
            }
            None => {
                tx.execute(
                    "INSERT INTO operation_receipts VALUES(?1,?2,?3,?4,?5)",
                    params![operation_id, kind, request_hash, result, now],
                )?;
            }
        }
        Self::operation_sql(tx, scope, operation_id)?
            .ok_or(StoreError::Invalid("OPERATION_MISSING"))
    }
    pub(crate) fn ensure_manual_instance_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        now: i64,
    ) -> StoreResult<StrategyDefinitionVersion> {
        if Self::instance_sql(tx, scope, "manual")?.is_none() {
            let budget = Self::account_policy_sql(tx, scope)?.campaign_limit;
            let instance = super::definitions::manual_instance(budget);
            Self::mutate_instance_sql(
                tx,
                scope,
                "native-manual-instance",
                "native-manual-instance/v1",
                "create",
                &instance,
                None,
                now,
            )?;
        }
        Self::strategy_definition_sql(tx, scope, "manual")
    }
    pub(crate) fn plan_definition_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        instance: &str,
        record_risk_state: bool,
        now: i64,
    ) -> StoreResult<StrategyDefinitionVersion> {
        if instance == "manual" {
            if record_risk_state {
                return Self::ensure_manual_instance_sql(tx, scope, now);
            }
            if Self::instance_sql(tx, scope, instance)?.is_none() {
                let budget = Self::account_policy_sql(tx, scope)?.campaign_limit;
                return super::definitions::canonical_definition(
                    &super::definitions::manual_instance(budget),
                    1,
                    0,
                );
            }
        }
        Self::strategy_definition_sql(tx, scope, instance)
    }
}

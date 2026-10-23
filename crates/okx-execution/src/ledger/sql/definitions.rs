use super::super::*;
use sha2::{Digest, Sha256};

pub(super) fn canonical_definition(
    instance: &StrategyInstance,
    revision: u64,
    now: i64,
) -> StoreResult<StrategyDefinitionVersion> {
    let parsed: serde_json::Value = serde_json::from_str(&instance.parameters)
        .map_err(|_| StoreError::Invalid("STRATEGY_PARAMETERS_INVALID"))?;
    let parameters = serde_json::to_string(&parsed)
        .map_err(|_| StoreError::Invalid("STRATEGY_PARAMETERS_INVALID"))?;
    let digest = format!("{:x}", Sha256::digest(parameters.as_bytes()));
    Ok(StrategyDefinitionVersion {
        definition_id: instance.definition_id.clone(),
        version: format!("{revision}-{digest}"),
        parameters,
        parameters_digest: digest,
        created_at: now,
    })
}
pub(super) fn persist_definition(
    tx: &rusqlite::Transaction<'_>,
    definition: &StrategyDefinitionVersion,
) -> StoreResult<()> {
    tx.execute("INSERT INTO strategy_definition_versions VALUES(?1,?2,?3,?4,?5) ON CONFLICT(definition_id,version) DO NOTHING",params![definition.definition_id,definition.version,definition.parameters,definition.parameters_digest,definition.created_at])?;
    let saved:(String,String)=tx.query_row("SELECT parameters,parameters_digest FROM strategy_definition_versions WHERE definition_id=?1 AND version=?2",params![definition.definition_id,definition.version],|r|Ok((r.get(0)?,r.get(1)?)))?;
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

impl AccountStore {
    pub fn instance(&self, id: &str) -> StoreResult<Option<StrategyInstance>> {
        let row=self.connection.query_row("SELECT instance_id,definition_id,definition_version,parameters,enabled,policy_revision,budget,last_decision_key FROM strategy_instances WHERE instance_id=?1",[id],|r|Ok((StrategyInstance{instance_id:r.get(0)?,definition_id:r.get(1)?,definition_version:r.get(2)?,parameters:r.get(3)?,enabled:r.get(4)?,policy_revision:r.get(5)?,budget:Decimal::ZERO,last_decision_key:r.get(7)?},r.get::<_,String>(6)?))).optional()?;
        row.map(|(mut instance, budget)| {
            instance.budget = decimal(budget)?;
            Ok(instance)
        })
        .transpose()
    }
    pub fn strategy_definition(&self, instance_id: &str) -> StoreResult<StrategyDefinitionVersion> {
        Ok(self.connection.query_row("SELECT v.definition_id,v.version,v.parameters,v.parameters_digest,v.created_at FROM strategy_instances i JOIN strategy_definition_versions v ON v.definition_id=i.definition_id AND v.version=i.definition_version AND v.parameters_digest=i.parameters_digest WHERE i.instance_id=?1",[instance_id],|r|Ok(StrategyDefinitionVersion{definition_id:r.get(0)?,version:r.get(1)?,parameters:r.get(2)?,parameters_digest:r.get(3)?,created_at:r.get(4)?}))?)
    }
    pub fn definition_version(
        &self,
        id: &str,
        version: &str,
    ) -> StoreResult<Option<StrategyDefinitionVersion>> {
        Ok(self.connection.query_row("SELECT parameters,parameters_digest,created_at FROM strategy_definition_versions WHERE definition_id=?1 AND version=?2",params![id,version],|r|Ok(StrategyDefinitionVersion{definition_id:id.into(),version:version.into(),parameters:r.get(0)?,parameters_digest:r.get(1)?,created_at:r.get(2)?})).optional()?)
    }
    pub fn ensure_manual_instance(&mut self, now: i64) -> StoreResult<StrategyDefinitionVersion> {
        if self.instance("manual")?.is_none() {
            let instance = StrategyInstance {
                instance_id: "manual".into(),
                definition_id: "manual".into(),
                definition_version: "1".into(),
                parameters: "{\"kind\":\"manual\"}".into(),
                enabled: false,
                policy_revision: 1,
                budget: self.policy()?.3,
                last_decision_key: None,
            };
            self.mutate_instance(
                "native-manual-instance",
                "native-manual-instance/v1",
                "create",
                &instance,
                None,
                now,
            )?;
        }
        self.strategy_definition("manual")
    }
}

impl AccountStore {
    pub fn strategy_status(&self, id: &str, now: i64) -> StoreResult<serde_json::Value> {
        let instance = self
            .instance(id)?
            .ok_or(StoreError::Invalid("STRATEGY_INSTANCE_NOT_FOUND"))?;
        let mut q=self.connection.prepare("SELECT lot_id,intent_id,fill_id,instrument_id,side,remaining_qty,cost,opened_at FROM position_lots WHERE instance_id=?1 AND remaining_qty<>'0' ORDER BY instrument_id,side,opened_at,lot_id LIMIT 51")?;
        let mut positions = q
            .query_map([id], |r| {
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
        Ok(
            serde_json::json!({"instance":instance,"definition":self.strategy_definition(id)?,"positions":positions,"positions_partial":partial,"limits":{"allocated_budget_usd":instance.budget,"policy_revision":instance.policy_revision},"nav":self.nav_summary(self.meta.initialized_at,now,&[id.into()],&["USD".into()])?,"risk":self.financial_risk(now,Some(id))?}),
        )
    }
}

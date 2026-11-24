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

pub(super) fn manual_instance(budget: Decimal) -> StrategyInstance {
    StrategyInstance {
        instance_id: "manual".into(),
        definition_id: "manual".into(),
        definition_version: "1".into(),
        parameters: "{\"kind\":\"manual\"}".into(),
        enabled: false,
        policy_revision: 1,
        budget,
        last_decision_key: None,
    }
}

impl AccountStore {
    pub fn instance(&self, id: &str) -> StoreResult<Option<StrategyInstance>> {
        Self::instance_sql(&self.connection, None, id)
    }
    pub fn strategy_definition(&self, instance_id: &str) -> StoreResult<StrategyDefinitionVersion> {
        Self::strategy_definition_sql(&self.connection, None, instance_id)
    }
    pub fn definition_version(
        &self,
        id: &str,
        version: &str,
    ) -> StoreResult<Option<StrategyDefinitionVersion>> {
        Ok(self.connection.query_row("SELECT parameters,parameters_digest,created_at FROM strategy_definition_versions WHERE definition_id=?1 AND version=?2",params![id,version],|r|Ok(StrategyDefinitionVersion{definition_id:id.into(),version:version.into(),parameters:r.get(0)?,parameters_digest:r.get(1)?,created_at:r.get(2)?})).optional()?)
    }
    /// Construct exactly the same immutable definition without a SQL mutation.
    pub fn manual_definition_preview(&self) -> StoreResult<StrategyDefinitionVersion> {
        if self.instance("manual")?.is_some() {
            self.strategy_definition("manual")
        } else {
            canonical_definition(&manual_instance(self.policy()?.3), 1, 0)
        }
    }
    pub fn ensure_manual_instance(&mut self, now: i64) -> StoreResult<StrategyDefinitionVersion> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let definition = Self::ensure_manual_instance_sql(&tx, None, now)?;
        tx.commit()?;
        Ok(definition)
    }
}

impl AccountStore {
    pub fn strategy_status(&self, id: &str, now: i64) -> StoreResult<serde_json::Value> {
        Self::strategy_status_sql(&self.connection, None, id, now)
    }
}

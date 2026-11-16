//! Original scoped strategy/receipt decoders; no SQL handles escape the owner.
use super::super::*;
impl AccountStore {
    pub(crate) fn operation_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
    ) -> StoreResult<Option<OperationReceipt>> {
        let mut values = vec![rusqlite::types::Value::from(id.to_owned())];
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
            values.push(installation.to_owned().into());
            values.push(account.to_owned().into());
        }
        Ok(db.query_row(match scope {None=>"SELECT kind,request_hash,result_ref,committed_at FROM operation_receipts WHERE operation_id=?1",Some(_)=>"SELECT kind,request_hash,result_ref,committed_at FROM operation_receipts WHERE operation_id=?1 AND installation_id=?2 AND account_key=?3"},rusqlite::params_from_iter(values),|r|Ok(OperationReceipt{operation_id:id.into(),kind:r.get(0)?,request_hash:r.get(1)?,result_ref:r.get(2)?,committed_at:r.get(3)?})).optional()?)
    }
    pub(crate) fn instance_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
    ) -> StoreResult<Option<StrategyInstance>> {
        let mut values = vec![rusqlite::types::Value::from(id.to_owned())];
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
            values.push(installation.to_owned().into());
            values.push(account.to_owned().into());
        }
        let row=db.query_row(match scope {None=>"SELECT instance_id,definition_id,definition_version,parameters,enabled,policy_revision,budget,last_decision_key FROM strategy_instances WHERE instance_id=?1",Some(_)=>"SELECT instance_id,definition_id,definition_version,parameters,enabled,policy_revision,budget,last_decision_key FROM strategy_instances WHERE instance_id=?1 AND installation_id=?2 AND account_key=?3"},rusqlite::params_from_iter(values),|r|Ok((StrategyInstance{instance_id:r.get(0)?,definition_id:r.get(1)?,definition_version:r.get(2)?,parameters:r.get(3)?,enabled:r.get(4)?,policy_revision:r.get(5)?,budget:Decimal::ZERO,last_decision_key:r.get(7)?},r.get::<_,String>(6)?))).optional()?;
        row.map(|(mut instance, budget)| {
            instance.budget = decimal(budget)?;
            Ok(instance)
        })
        .transpose()
    }
}

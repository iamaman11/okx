use super::super::*;
impl AccountStore {
    pub fn claim_strategy_decision(&mut self,instance_id:&str,decision_key:&str,input_digest:&str,policy_revision:i64,now:i64)->StoreResult<bool>{let id=format!("strategy:{instance_id}:{decision_key}");let tx=self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;let prior:Option<String>=tx.query_row("SELECT input_digest FROM decisions WHERE decision_id=?1",[&id],|r|r.get(0)).optional()?;if let Some(prior)=prior{if prior!=input_digest{return Err(StoreError::Invalid("STRATEGY_DECISION_INPUT_CONFLICT"));}return Ok(false);}let enabled:bool=tx.query_row("SELECT enabled FROM strategy_instances WHERE instance_id=?1",[instance_id],|r|r.get(0))?;if !enabled{return Err(StoreError::Invalid("STRATEGY_STOPPED"));}tx.execute("INSERT INTO decisions VALUES(?1,?2,?3,?4,?5,'strategy-bar/v1')",params![id,instance_id,now,input_digest,policy_revision])?;tx.execute("UPDATE strategy_instances SET last_decision_key=?2 WHERE instance_id=?1",params![instance_id,decision_key])?;tx.commit()?;Ok(true)}
    pub fn account_summary(&self) -> StoreResult<serde_json::Value> {
        let mut s=self.connection.prepare("SELECT state,count(*) FROM intents WHERE state IN('PREPARED','RESERVED','POSTING','UNKNOWN','ACKNOWLEDGED','PARTIALLY_FILLED') GROUP BY state")?;
        let active = s
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?
            .collect::<Result<std::collections::BTreeMap<_, _>, _>>()?;
        let mut s=self.connection.prepare("SELECT currency,component,amount FROM finance_totals ORDER BY currency,component LIMIT 21")?;
        let totals=s.query_map([],|r|Ok(serde_json::json!({"currency":r.get::<_,String>(0)?,"component":r.get::<_,String>(1)?,"amount":r.get::<_,String>(2)?})))?.collect::<Result<Vec<_>,_>>()?;
        let truncated = totals.len() > 20;
        let policy = self.policy()?;
        let (db, wal) = self.storage_bytes()?;
        Ok(
            serde_json::json!({"schema":"okx.account-summary/v2","t0":self.meta.initialized_at,"active":active,"policy_revision":policy.0,"paused":policy.1,"campaign":{"id":policy.2,"limit_usd":exact(policy.3),"loss_usd":exact(policy.4)},"money":totals.into_iter().take(20).collect::<Vec<_>>(),"money_partial":truncated,"storage":{"database_bytes":db,"wal_bytes":wal,"health_error":self.storage_health(),"recovery_required":self.storage_recovery_required()?},"sqlite_version":rusqlite::version()}),
        )
    }
    pub fn mutate_instance(
        &mut self,
        operation_id: &str,
        request_hash: &str,
        action: &str,
        instance: &StrategyInstance,
        expected_revision: Option<i64>,
        now: i64,
    ) -> StoreResult<OperationReceipt> {
        let kind = format!("STRATEGY_{action}");
        if let Some(old) = self.operation(operation_id)? {
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
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current: Option<i64> = tx
            .query_row(
                "SELECT policy_revision FROM strategy_instances WHERE instance_id=?1",
                [&instance.instance_id],
                |r| r.get(0),
            )
            .optional()?;
        if action == "create" && current.is_some() {
            return Err(StoreError::Invalid("INSTANCE_ALREADY_EXISTS"));
        }
        if action != "create" && current.is_none() {
            return Err(StoreError::Invalid("INSTANCE_NOT_FOUND"));
        }
        if current.is_some() && expected_revision != current {
            return Err(StoreError::Invalid("INSTANCE_REVISION_CONFLICT"));
        }
        let enabled = match action {
            "start" => true,
            "stop" => false,
            _ => instance.enabled,
        };
        if matches!(action, "start" | "stop") {
            tx.execute(
                "UPDATE strategy_instances SET enabled=?2 WHERE instance_id=?1",
                params![instance.instance_id, enabled],
            )?;
        } else {
            tx.execute("INSERT INTO strategy_instances VALUES(?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(instance_id) DO UPDATE SET definition_id=excluded.definition_id,definition_version=excluded.definition_version,parameters=excluded.parameters,enabled=excluded.enabled,policy_revision=excluded.policy_revision,budget=excluded.budget",params![instance.instance_id,instance.definition_id,instance.definition_version,instance.parameters,enabled,instance.policy_revision,exact(instance.budget),instance.last_decision_key])?;
        }
        let result=serde_json::to_string(&serde_json::json!({"instance_id":instance.instance_id,"enabled":enabled,"policy_revision":instance.policy_revision})).map_err(|_|StoreError::Invalid("INSTANCE_SERIALIZATION_FAILED"))?;
        tx.execute(
            "INSERT INTO operation_receipts VALUES(?1,?2,?3,?4,?5)",
            params![operation_id, kind, request_hash, result, now],
        )?;
        tx.commit()?;
        self.operation(operation_id)?
            .ok_or(StoreError::Invalid("OPERATION_MISSING"))
    }
    pub fn apply_authority_policy(
        &mut self,
        revision: i64,
        paused: bool,
        campaign_id: &str,
        limit: Decimal,
    ) -> StoreResult<()> {
        if revision < 0 || campaign_id.is_empty() || limit < Decimal::ZERO {
            return Err(StoreError::Invalid("INVALID_AUTHORITY_POLICY"));
        }
        let (old, old_paused, old_id, old_limit, _) = self.policy()?;
        if revision < old {
            return Err(StoreError::Invalid("POLICY_REVISION_CONFLICT"));
        }
        if revision == old && old_id != "unconfigured" {
            if old_paused != paused || old_id != campaign_id || old_limit != limit {
                return Err(StoreError::Invalid("POLICY_REVISION_CONFLICT"));
            }
            return Ok(());
        }
        if old_id != "unconfigured" && old_id != campaign_id {
            return Err(StoreError::Invalid("CAMPAIGN_ID_IMMUTABLE"));
        }
        self.connection.execute(
            "UPDATE account_policy SET revision=?1,paused=?2,campaign_id=?3,campaign_limit=?4",
            params![revision, paused, campaign_id, exact(limit)],
        )?;
        Ok(())
    }
    pub fn policy(&self) -> StoreResult<(i64, bool, String, Decimal, Decimal)> {
        let (rev, paused, id, limit, loss): (i64, bool, String, String, String) = self
            .connection
            .query_row(
            "SELECT revision,paused,campaign_id,campaign_limit,campaign_loss FROM account_policy",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )?;
        Ok((rev, paused, id, decimal(limit)?, decimal(loss)?))
    }
    pub fn put_instance(&mut self, instance: &StrategyInstance) -> StoreResult<()> {
        if instance.budget < Decimal::ZERO || instance.instance_id.is_empty() {
            return Err(StoreError::Invalid("INVALID_INSTANCE"));
        }
        self.connection.execute("INSERT INTO strategy_instances VALUES(?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(instance_id) DO UPDATE SET definition_id=excluded.definition_id,definition_version=excluded.definition_version,parameters=excluded.parameters,enabled=excluded.enabled,policy_revision=excluded.policy_revision,budget=excluded.budget",params![instance.instance_id,instance.definition_id,instance.definition_version,instance.parameters,instance.enabled,instance.policy_revision,exact(instance.budget),instance.last_decision_key])?;
        Ok(())
    }
    pub fn instances(&self) -> StoreResult<Vec<StrategyInstance>> {
        let mut s=self.connection.prepare("SELECT instance_id,definition_id,definition_version,parameters,enabled,policy_revision,budget,last_decision_key FROM strategy_instances ORDER BY instance_id")?;
        s.query_map([], |r| {
            Ok((
                StrategyInstance {
                    instance_id: r.get(0)?,
                    definition_id: r.get(1)?,
                    definition_version: r.get(2)?,
                    parameters: r.get(3)?,
                    enabled: r.get(4)?,
                    policy_revision: r.get(5)?,
                    budget: Decimal::ZERO,
                    last_decision_key: r.get(7)?,
                },
                r.get::<_, String>(6)?,
            ))
        })?
        .map(|x| {
            let (mut i, b) = x?;
            i.budget = decimal(b)?;
            Ok(i)
        })
        .collect()
    }
    pub fn latch_stop(&mut self, id: &str, scope: &str, reason: &str, now: i64) -> StoreResult<()> {
        self.connection.execute(
            "INSERT INTO risk_stops VALUES(?1,?2,?3,?4,NULL) ON CONFLICT(stop_id) DO NOTHING",
            params![id, scope, reason, now],
        )?;
        Ok(())
    }
    pub fn resolve_stop(&mut self, id: &str, evidence: &str) -> StoreResult<()> {
        if evidence.is_empty() {
            return Err(StoreError::Invalid("STOP_RESOLUTION_EVIDENCE_REQUIRED"));
        }
        self.connection.execute(
            "UPDATE risk_stops SET resolution=?2 WHERE stop_id=?1 AND resolution IS NULL",
            params![id, evidence],
        )?;
        Ok(())
    }
    pub fn stops(&self) -> StoreResult<Vec<(String, String, String, i64)>> {
        let mut s=self.connection.prepare("SELECT stop_id,scope,reason,latched_at FROM risk_stops WHERE resolution IS NULL ORDER BY latched_at,stop_id")?;
        Ok(
            s.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?
                .collect::<Result<_, _>>()?,
        )
    }
    // Exactly one transaction covers source deduplication, canonical money and watermark.
    pub fn checkpoint(&self) -> StoreResult<(i64, i64, i64)> {
        Ok(self
            .connection
            .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })?)
    }
    pub fn storage_bytes(&self) -> StoreResult<(u64, u64)> {
        Ok((
            std::fs::metadata(&self.path)?.len(),
            std::fs::metadata(format!("{}-wal", self.path.display()))
                .map(|m| m.len())
                .unwrap_or(0),
        ))
    }
    pub fn backup(&self, destination: impl AsRef<Path>) -> StoreResult<()> {
        if destination.as_ref().exists() {
            return Err(StoreError::Invalid("BACKUP_ALREADY_EXISTS"));
        }
        let mut target = Connection::open(destination)?;
        {
            let b = rusqlite::backup::Backup::new(&self.connection, &mut target)?;
            b.run_to_completion(128, Duration::from_millis(1), None)?;
        }
        target.execute("UPDATE storage_recovery SET requires_reconcile=1,backup_at=?1,reconciled_evidence=NULL",[wall_time_ms()])?;
        target.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
        Ok(())
    }
    pub fn mark_storage_reconciled(
        &mut self,
        observed_at: i64,
        exchange_evidence: &str,
        central_evidence: &str,
    ) -> StoreResult<()> {
        if exchange_evidence.is_empty() || central_evidence.is_empty() {
            return Err(StoreError::Invalid("STORAGE_RECOVERY_INCOMPLETE"));
        }
        let n=self.connection.execute("UPDATE storage_recovery SET requires_reconcile=0,reconciled_evidence=?2 WHERE backup_at IS NULL OR backup_at<=?1",params![observed_at,format!("{exchange_evidence}|{central_evidence}")])?;
        if n != 1 {
            return Err(StoreError::Invalid("STORAGE_RECOVERY_INCOMPLETE"));
        }
        Ok(())
    }
    pub fn storage_recovery_required(&self) -> StoreResult<bool> {
        Ok(self.connection.query_row(
            "SELECT requires_reconcile FROM storage_recovery",
            [],
            |r| r.get(0),
        )?)
    }
    pub fn storage_health(&self) -> Option<&str> {
        self.maintenance_error.as_deref()
    }
    pub(crate) fn set_maintenance_error(&mut self, error: String) {
        self.maintenance_error = Some(error);
    }
    pub(crate) fn maintenance(&mut self, backup_due: bool) -> StoreResult<()> {
        let (_, wal) = self.storage_bytes()?;
        let page_size: i64 = self
            .connection
            .pragma_query_value(None, "page_size", |r| r.get(0))?;
        if wal >= (page_size as u64).saturating_add(24).saturating_mul(1000) {
            self.checkpoint()?;
        }
        if !backup_due {
            return Ok(());
        }
        let directory = self
            .path
            .parent()
            .ok_or(StoreError::Invalid("BACKUP_DIRECTORY_MISSING"))?
            .join("backups");
        std::fs::create_dir_all(&directory)?;
        let now = wall_time_ms();
        let target = directory.join(format!("account-{now}.db"));
        self.backup(&target)?;
        use sha2::{Digest, Sha256};
        use std::io::Read;
        let mut file = std::fs::File::open(&target)?;
        let mut hash = Sha256::new();
        let mut buffer = [0u8; 65536];
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hash.update(&buffer[..n]);
        }
        let manifest = serde_json::json!({"schema_version":2,"account_key":self.meta.account_key,"installation_id":self.meta.installation_id,"time":now,"sha256":format!("{:x}",hash.finalize())});
        std::fs::write(
            target.with_extension("manifest.json"),
            serde_json::to_vec(&manifest)
                .map_err(|_| StoreError::Invalid("BACKUP_MANIFEST_FAILED"))?,
        )?;
        let mut backups = std::fs::read_dir(&directory)?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| {
                p.extension().is_some_and(|e| e == "db")
                    && p.file_name()
                        .is_some_and(|n| n.to_string_lossy().starts_with("account-"))
            })
            .collect::<Vec<_>>();
        backups.sort();
        let remove = backups.len().saturating_sub(4);
        for old in backups.into_iter().take(remove) {
            std::fs::remove_file(old.with_extension("manifest.json"))?;
            std::fs::remove_file(old)?;
        }
        self.maintenance_error = None;
        Ok(())
    }
}

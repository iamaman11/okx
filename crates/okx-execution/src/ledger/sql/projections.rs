use super::super::*;
impl AccountStore {
    pub fn recovery_snapshot(&self, now: i64) -> StoreResult<RecoverySnapshot> {
        Self::recovery_snapshot_sql(&self.connection, None, now)
    }
    // The immutable decision table is the sole strategy claim journal. A retry
    // after a policy revision must append another decision, never update one.
    // The primary-key range is bounded by this exact strategy/version/bar.
    fn latest_strategy_claim(
        tx: &rusqlite::Transaction<'_>,
        base: &str,
    ) -> StoreResult<Option<(String, String, i64, i64)>> {
        Ok(tx
            .query_row(
                "SELECT decision_id,input_digest,policy_revision,rowid FROM decisions
                 WHERE decision_id=?1 OR
                 (decision_id >= (?1 || ':retry:') AND decision_id < (?1 || ':retry;'))
                 ORDER BY rowid DESC LIMIT 1",
                [base],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?)
    }
    pub fn claim_strategy_decision(
        &mut self,
        instance_id: &str,
        decision_key: &str,
        input_digest: &str,
        policy_revision: i64,
        now: i64,
    ) -> StoreResult<bool> {
        Ok(self
            .claim_strategy_decision_with_id(
                instance_id,
                decision_key,
                input_digest,
                policy_revision,
                now,
            )?
            .is_some())
    }
    pub fn claim_strategy_decision_with_id(
        &mut self,
        instance_id: &str,
        decision_key: &str,
        input_digest: &str,
        policy_revision: i64,
        now: i64,
    ) -> StoreResult<Option<String>> {
        if policy_revision < 1 {
            return Err(StoreError::Invalid("STRATEGY_POLICY_REVISION_INVALID"));
        }
        let version = self.strategy_definition(instance_id)?.version;
        let base = format!("strategy:{instance_id}:{version}:{decision_key}");
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let enabled: bool = tx.query_row(
            "SELECT enabled FROM strategy_instances WHERE instance_id=?1",
            [instance_id],
            |r| r.get(0),
        )?;
        if !enabled {
            return Err(StoreError::Invalid("STRATEGY_STOPPED"));
        }
        if let Some((prior_id, prior_digest, prior_revision, prior_row)) =
            Self::latest_strategy_claim(&tx, &base)?
        {
            if prior_digest != input_digest {
                return Err(StoreError::Invalid("STRATEGY_DECISION_INPUT_CONFLICT"));
            }
            // Idempotency is per bar, across every policy retry. Once any
            // intent exists there is no second autonomous exchange POST.
            let has_intent: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM intents WHERE idempotency_key=?1)",
                [decision_key],
                |r| r.get(0),
            )?;
            let terminal: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM operation_receipts WHERE operation_id=?1)",
                [format!("strategy-no-trade:{prior_id}")],
                |r| r.get(0),
            )?;
            if has_intent || terminal {
                return Ok(None);
            }
            if prior_revision == policy_revision {
                return Ok(Some(prior_id));
            }
            if policy_revision < prior_revision {
                return Err(StoreError::Invalid("STRATEGY_POLICY_REVISION_REGRESSION"));
            }
            let successor = format!("{base}:retry:{prior_row}:policy:{policy_revision}");
            tx.execute(
                "INSERT INTO decisions VALUES(?1,?2,?3,?4,?5,'strategy-bar/policy-retry-v1')",
                params![successor, instance_id, now, input_digest, policy_revision],
            )?;
            // Immutable predecessor -> successor lineage is committed together
            // with the new decision; an interrupted claim cannot half-advance.
            tx.execute(
                "INSERT INTO operation_receipts(operation_id,kind,request_hash,result_ref,committed_at)
                 VALUES(?1,'strategy_policy_retry',?2,?3,?4)",
                params![format!("strategy-policy-retry:{prior_id}"), input_digest, successor, now],
            )?;
            tx.commit()?;
            return Ok(Some(successor));
        }
        tx.execute(
            "INSERT INTO decisions VALUES(?1,?2,?3,?4,?5,'strategy-bar/v1')",
            params![base, instance_id, now, input_digest, policy_revision],
        )?;
        // A pending bar is not terminal until its first intent or NO_TRADE.
        tx.commit()?;
        Ok(Some(base))
    }
    pub fn finish_strategy_no_trade(
        &mut self,
        instance_id: &str,
        decision_key: &str,
        now: i64,
    ) -> StoreResult<()> {
        let version = self.strategy_definition(instance_id)?.version;
        let base = format!("strategy:{instance_id}:{version}:{decision_key}");
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (id, digest, _, _) = Self::latest_strategy_claim(&tx, &base)?
            .ok_or(StoreError::Invalid("STRATEGY_CLAIM_MISSING"))?;
        let has_intent: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM intents WHERE idempotency_key=?1)",
            [decision_key],
            |r| r.get(0),
        )?;
        if has_intent {
            return Err(StoreError::Invalid("STRATEGY_ALREADY_HAS_INTENT"));
        }
        tx.execute(
            "INSERT INTO operation_receipts(operation_id,kind,request_hash,result_ref,committed_at)
             VALUES(?1,'strategy_no_trade',?2,'NO_TRADE',?3)
             ON CONFLICT(operation_id) DO NOTHING",
            params![format!("strategy-no-trade:{id}"), digest, now],
        )?;
        tx.execute(
            "UPDATE strategy_instances SET last_decision_key=?2 WHERE instance_id=?1",
            params![instance_id, decision_key],
        )?;
        tx.commit()?;
        Ok(())
    }
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
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let receipt = Self::mutate_instance_sql(
            &tx,
            None,
            operation_id,
            request_hash,
            action,
            instance,
            expected_revision,
            now,
        )?;
        tx.commit()?;
        Ok(receipt)
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
        self.account_policy()
            .map(AccountPolicySnapshot::legacy_tuple)
    }
    pub fn put_instance(&mut self, instance: &StrategyInstance) -> StoreResult<()> {
        let prior = self.instance(&instance.instance_id)?;
        let mut target = instance.clone();
        target.policy_revision = prior
            .as_ref()
            .map(|i| {
                i.policy_revision
                    .checked_add(1)
                    .ok_or(StoreError::Invalid("INSTANCE_REVISION_OVERFLOW"))
            })
            .transpose()?
            .unwrap_or(1);
        use sha2::{Digest, Sha256};
        let hash = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&target)
                    .map_err(|_| StoreError::Invalid("INSTANCE_SERIALIZATION_FAILED"))?
            )
        );
        self.mutate_instance(
            &format!("native-put:{}:{hash}", instance.instance_id),
            &hash,
            if prior.is_some() { "update" } else { "create" },
            &target,
            prior.map(|i| i.policy_revision),
            wall_time_ms(),
        )?;
        Ok(())
    }
    pub fn instances(&self) -> StoreResult<Vec<StrategyInstance>> {
        Self::instances_sql(&self.connection, None)
    }
    pub fn latch_stop(&mut self, id: &str, scope: &str, reason: &str, now: i64) -> StoreResult<()> {
        Self::latch_stop_sql(&self.connection, None, id, scope, reason, now)
    }
    pub(crate) fn latch_stop_sql(
        db: &Connection,
        account_scope: Option<(&str, &str)>,
        id: &str,
        scope: &str,
        reason: &str,
        now: i64,
    ) -> StoreResult<()> {
        match account_scope {
            None => db.execute(
                "INSERT INTO risk_stops VALUES(?1,?2,?3,?4,NULL) ON CONFLICT(stop_id) DO NOTHING",
                params![id, scope, reason, now],
            )?,
            Some((installation, account)) => {
                Self::verify_source_account_sql(db, installation, account)?;
                db.execute("INSERT INTO risk_stops VALUES(?1,?2,?3,?4,?5,?6,NULL) ON CONFLICT(installation_id,account_key,stop_id) DO NOTHING",params![installation,account,id,scope,reason,now])?
            }
        };
        Ok(())
    }
    pub fn resolve_stop(&mut self, id: &str, evidence: &str) -> StoreResult<()> {
        Self::resolve_source_stop_sql(&self.connection, None, id, evidence)
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
        Self::mark_storage_reconciled_sql(
            &self.connection,
            None,
            observed_at,
            exchange_evidence,
            central_evidence,
        )
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

    pub(crate) fn clear_maintenance_error(&mut self) {
        self.maintenance_error = None;
    }
    /// Existing single SQL owner remains responsible for all financial writes
    /// and WAL checkpoints. Copying a 150MB+ backup never runs in its FIFO.
    /// Observe the single existing PASSIVE checkpoint within its owner turn.
    /// Keep error and busy separate; do not silently claim full completion.
    pub(crate) fn maintenance_observed(
        &mut self,
        observe: impl FnOnce(WalMaintenanceObservation),
    ) -> StoreResult<()> {
        let mut measured = WalMaintenanceObservation {
            wal_before_bytes: None,
            wal_after_bytes: None,
            checkpoint_mode: "PASSIVE",
            checkpoint_attempted: false,
            checkpoint_busy: None,
            log_frames: None,
            checkpointed_frames: None,
            checkpoint_elapsed_us: None,
        };
        let result: StoreResult<()> = (|| {
            let (_, wal) = self.storage_bytes()?;
            measured.wal_before_bytes = Some(wal);
            let page_size: i64 = self
                .connection
                .pragma_query_value(None, "page_size", |r| r.get(0))?;
            if wal >= (page_size as u64).saturating_add(24).saturating_mul(1000) {
                measured.checkpoint_attempted = true;
                let started = std::time::Instant::now();
                let checkpoint = self.checkpoint();
                measured.checkpoint_elapsed_us =
                    Some(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
                if let Ok((busy, log, checked)) = &checkpoint {
                    measured.checkpoint_busy = Some(*busy);
                    measured.log_frames = Some(*log);
                    measured.checkpointed_frames = Some(*checked);
                }
                checkpoint?;
            }
            Ok(())
        })();
        measured.wal_after_bytes = self.storage_bytes().ok().map(|(_, wal)| wal);
        observe(measured);
        result
    }

    pub(crate) fn backup_identity(&self) -> (PathBuf, AccountMeta) {
        (self.path.clone(), self.meta.clone())
    }

    /// A short-lived *read-only* SQLite snapshot reader. The sole account
    /// writer and its original connection are never borrowed by this backup.
    /// The destination is a separate immutable recovery artifact, not a second
    /// ledger or trading authority. The 15-minute cadence stays in the owner.
    pub(crate) fn periodic_backup_from_reader(
        source_path: PathBuf,
        meta: AccountMeta,
    ) -> StoreResult<()> {
        let directory = source_path
            .parent()
            .ok_or(StoreError::Invalid("BACKUP_DIRECTORY_MISSING"))?
            .join("backups");
        std::fs::create_dir_all(&directory)?;
        let now = wall_time_ms();
        let target = directory.join(format!("account-{now}.db"));
        let staged = directory.join(format!("account-{now}.pending"));
        let manifest_path = target.with_extension("manifest.json");
        let staged_manifest = directory.join(format!("account-{now}.manifest-pending"));
        if target.exists() || staged.exists() || manifest_path.exists() || staged_manifest.exists()
        {
            return Err(StoreError::Invalid("BACKUP_ALREADY_EXISTS"));
        }
        let result = (|| -> StoreResult<()> {
            let source =
                Connection::open_with_flags(&source_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            source.busy_timeout(Duration::from_millis(250))?;
            let mut destination = Connection::open(&staged)?;
            destination.pragma_update(None, "synchronous", "FULL")?;
            {
                let backup = rusqlite::backup::Backup::new(&source, &mut destination)?;
                // A separate read-only connection plus a paged backup can restart
                // from page zero on EVERY commit by the live WAL writer.
                // A NAV cycle commits every ~2s: large copies may never finish.
                // One SQLite backup_step(-1) pins a consistent read snapshot
                // for the complete copy without taking ownership of the writer.
                // Under WAL, the source read lock permits concurrent writers.
                // rusqlite::Backup::run_to_completion requires positive
                // pages_per_step; call SQLite backup_step(-1) directly.
                // Fail closed on a busy or incomplete snapshot.
                match backup.step(-1)? {
                    rusqlite::backup::StepResult::Done => {}
                    rusqlite::backup::StepResult::Busy | rusqlite::backup::StepResult::Locked => {
                        return Err(StoreError::Invalid("BACKUP_SOURCE_BUSY"));
                    }
                    rusqlite::backup::StepResult::More => {
                        return Err(StoreError::Invalid("BACKUP_SNAPSHOT_INCOMPLETE"));
                    }
                    _ => return Err(StoreError::Invalid("BACKUP_STEP_UNKNOWN")),
                }
            }
            // As with AccountStore::backup, every restored copy requires
            // reconciliation against the venue and central risk authority.
            destination.execute(
                "UPDATE storage_recovery SET requires_reconcile=1,backup_at=?1,reconciled_evidence=NULL",
                [now],
            )?;
            destination.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
            let integrity: String =
                destination.query_row("PRAGMA quick_check", [], |r| r.get(0))?;
            if integrity != "ok" {
                return Err(StoreError::Invalid("BACKUP_INTEGRITY_FAILED"));
            }
            drop(destination);
            drop(source);

            use sha2::{Digest, Sha256};
            use std::io::Read;
            let mut file = std::fs::File::open(&staged)?;
            let mut hash = Sha256::new();
            let mut buffer = [0u8; 65536];
            loop {
                let n = file.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                hash.update(&buffer[..n]);
            }
            let manifest = serde_json::json!({
                "schema_version": 2,
                "account_key": meta.account_key,
                "installation_id": meta.installation_id,
                "time": now,
                "sha256": format!("{:x}", hash.finalize())
            });
            let mut manifest_file = std::fs::File::create(&staged_manifest)?;
            use std::io::Write;
            manifest_file.write_all(
                &serde_json::to_vec(&manifest)
                    .map_err(|_| StoreError::Invalid("BACKUP_MANIFEST_FAILED"))?,
            )?;
            manifest_file.sync_all()?;
            // Publish only a complete, integrity-checked, hashed snapshot.
            // The manifest precedes the DB so an interrupted transfer can
            // never expose an incomplete *.db as a restorable backup.
            std::fs::rename(&staged_manifest, &manifest_path)?;
            std::fs::rename(&staged, &target)?;

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
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&staged);
            let _ = std::fs::remove_file(&staged_manifest);
            // A manifest is never evidence without its corresponding DB.
            if !target.exists() {
                let _ = std::fs::remove_file(&manifest_path);
            }
        }
        result
    }
}

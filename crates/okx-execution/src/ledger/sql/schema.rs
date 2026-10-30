use super::super::*;
impl AccountStore {
    pub fn initialize_or_inspect_exact(
        path: impl AsRef<Path>,
        meta: AccountMeta,
        snapshot: &OpeningSnapshot,
    ) -> StoreResult<Self> {
        if path.as_ref().exists() {
            let store = Self::open_existing(path, &meta.account_key, &meta.installation_id)?;
            if store.meta.initialize_operation_id != meta.initialize_operation_id
                || store.meta.release_id != meta.release_id
            {
                return Err(StoreError::Invalid("INITIALIZE_OPERATION_CONFLICT"));
            }
            return Ok(store);
        }
        Self::initialize_flat(path, meta, snapshot)
    }
    pub fn initialize_flat(
        path: impl AsRef<Path>,
        meta: AccountMeta,
        snapshot: &OpeningSnapshot,
    ) -> StoreResult<Self> {
        if snapshot.positions != 0
            || snapshot.orders != 0
            || snapshot.conditional_orders != 0
            || snapshot.liabilities != Decimal::ZERO
        {
            return Err(StoreError::Invalid("ACCOUNT_NOT_FLAT"));
        }
        if snapshot.source_ref != meta.opening_source
            || snapshot.observed_at != meta.initialized_at
            || snapshot.balances.is_empty()
        {
            return Err(StoreError::Invalid("OPENING_EVIDENCE_MISMATCH"));
        }
        Self::initialize_inner(path, meta, snapshot)
    }
    fn initialize_inner(
        path: impl AsRef<Path>,
        meta: AccountMeta,
        snapshot: &OpeningSnapshot,
    ) -> StoreResult<Self> {
        if path.as_ref().exists() {
            return Err(StoreError::Invalid("ACCOUNT_ALREADY_INITIALIZED"));
        }
        if meta.account_key.is_empty()
            || meta.installation_id.is_empty()
            || meta.opening_source.is_empty()
            || meta.initialized_at <= 0
            || meta.initialize_operation_id.is_empty()
            || meta.release_id.is_empty()
        {
            return Err(StoreError::Invalid("INVALID_ACCOUNT_META"));
        }
        let lease = Self::writer_lease(path.as_ref())?;
        let _new_file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path.as_ref())?;
        let mut c = Connection::open(path.as_ref())?;
        Self::configure(&c)?;
        c.pragma_update(None, "journal_mode", "WAL")?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(include_str!("../../../schema/account-v2.sql"))?;
        tx.execute(
            "INSERT INTO account_meta VALUES(1,?1,?2,2,?3,?4,?5,?6)",
            params![
                meta.account_key,
                meta.installation_id,
                meta.initialized_at,
                meta.opening_source,
                meta.initialize_operation_id,
                meta.release_id
            ],
        )?;
        tx.execute(
            "INSERT INTO account_policy VALUES(1,0,0,'unconfigured','0','0')",
            [],
        )?;
        for (currency, amount) in &snapshot.balances {
            if currency.is_empty() || *amount < Decimal::ZERO {
                return Err(StoreError::Invalid("INVALID_OPENING_BALANCE"));
            }
            tx.execute(
                "INSERT INTO cash_flows VALUES(?1,'OPENING',?2,?3,?4,?5)",
                params![
                    format!("opening:{currency}"),
                    currency,
                    exact(*amount),
                    snapshot.observed_at,
                    snapshot.source_ref
                ],
            )?;
        }
        tx.execute(
            "INSERT INTO source_cursors VALUES('opening','account',?1,?2,0,'')",
            params![snapshot.source_ref, snapshot.observed_at],
        )?;
        tx.execute(
            "INSERT INTO operation_receipts VALUES(?1,'INITIALIZE',?2,?3,?4)",
            params![
                meta.initialize_operation_id,
                format!(
                    "{}|{}|{}",
                    meta.account_key, meta.installation_id, meta.release_id
                ),
                meta.opening_source,
                meta.initialized_at
            ],
        )?;
        tx.execute("INSERT INTO storage_recovery VALUES(1,0,NULL,NULL)", [])?;
        tx.execute(
            "INSERT INTO nav_points VALUES('account',?1,'opening',?2,'0','0','USD','COMPLETE',?3)",
            params![
                meta.initialized_at,
                exact(snapshot.opening_equity_usd),
                snapshot.source_ref
            ],
        )?;
        tx.execute(
            "INSERT INTO nav_watermarks VALUES('account',?1,?1,?2,'COMPLETE')",
            params![exact(snapshot.opening_equity_usd), meta.initialized_at],
        )?;
        tx.execute("INSERT INTO nav_scopes VALUES('account','USD')", [])?;
        for (currency, amount) in &snapshot.balances {
            let scope = format!("account:{currency}");
            tx.execute(
                "INSERT INTO nav_points VALUES(?1,?2,'opening',?3,'0','0',?4,'COMPLETE',?5)",
                params![
                    scope,
                    meta.initialized_at,
                    exact(*amount),
                    currency,
                    snapshot.source_ref
                ],
            )?;
            tx.execute(
                "INSERT INTO nav_scopes VALUES(?1,?2)",
                params![scope, currency],
            )?;
        }
        tx.commit()?;
        Ok(Self {
            connection: c,
            path: path.as_ref().into(),
            meta,
            _lease: lease,
            maintenance_error: None,
            cash_flow_totals_cache: std::cell::RefCell::new(None),
            latest_capital_assignment_time: std::cell::RefCell::new(None),
        })
    }
    pub fn open_existing(
        path: impl AsRef<Path>,
        account_key: &str,
        installation_id: &str,
    ) -> StoreResult<Self> {
        if path.as_ref().with_extension("restore.pending").exists() {
            return Err(StoreError::Invalid("STORAGE_RECOVERY_INCOMPLETE"));
        }
        let lease = Self::writer_lease(path.as_ref())?;
        let mut c = Connection::open_with_flags(path.as_ref(), OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        Self::configure(&c)?;
        let version: i64 = c.pragma_query_value(None, "user_version", |r| r.get(0))?;
        let journal: String = c.pragma_query_value(None, "journal_mode", |r| r.get(0))?;
        if version != 2 || journal != "wal" {
            return Err(StoreError::Invalid("UNSUPPORTED_ACCOUNT_STORAGE"));
        }
        let meta=c.query_row("SELECT account_key,installation_id,initialized_at,opening_source,initialize_operation_id,release_id FROM account_meta WHERE singleton=1",[],|r|Ok(AccountMeta{account_key:r.get(0)?,installation_id:r.get(1)?,initialized_at:r.get(2)?,opening_source:r.get(3)?,initialize_operation_id:r.get(4)?,release_id:r.get(5)?}))?;
        if meta.account_key != account_key || meta.installation_id != installation_id {
            return Err(StoreError::Invalid("ACCOUNT_IDENTITY_MISMATCH"));
        }
        // Existing v2 databases need the same derived lookup as clean T0 databases.
        // This is an idempotent index only: no financial facts or schema version change.
        c.execute_batch("CREATE INDEX IF NOT EXISTS idx_nav_scope_version_time ON nav_points(scope_id,source_version,time); CREATE INDEX IF NOT EXISTS idx_finance_source_component ON finance_entries(source_ref,component)")?;
        // Existing v2 ledger: establish an immutable-event provenance fence
        // once on open, never during the two-second NAV hot path. An invalid
        // old economic row leaves strategy valuation NOT_PROVEN, but must not
        // prevent account inspection, reconciliation or reducing risk.
        if let Err(error) = Self::audit_existing_finance_provenance(&mut c) {
            if matches!(error, StoreError::Invalid(_)) {
                eprintln!("ACCOUNT_FINANCE_PROVENANCE_V1 outcome=unverified");
            } else {
                return Err(error);
            }
        }
        Ok(Self {
            connection: c,
            path: path.as_ref().into(),
            meta,
            _lease: lease,
            maintenance_error: None,
            cash_flow_totals_cache: std::cell::RefCell::new(None),
            latest_capital_assignment_time: std::cell::RefCell::new(None),
        })
    }
    pub(super) fn writer_lease(path: &Path) -> StoreResult<std::fs::File> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(format!("{}.writer.lock", path.display()))?;
        file.try_lock()
            .map_err(|_| StoreError::Invalid("ACCOUNT_WRITER_ALREADY_RUNNING"))?;
        Ok(file)
    }
    pub(super) fn configure(c: &Connection) -> StoreResult<()> {
        c.busy_timeout(Duration::from_millis(250))?;
        c.pragma_update(None, "foreign_keys", "ON")?;
        c.pragma_update(None, "synchronous", "FULL")?;
        // The single AccountStore owner already runs a bounded-cadence
        // PASSIVE checkpoint (and mandatory durable backups). SQLite's
        // default 1000-page auto-checkpoint otherwise runs inside COMMIT,
        // injecting unpredictable disk I/O into time-critical NAV writes.
        // Keep FULL transaction durability; do not add a second checkpointer.
        c.pragma_update(None, "wal_autocheckpoint", 0)?;
        if rusqlite::version_number() < 3_051_003 {
            return Err(StoreError::Invalid("SQLITE_WAL_RESET_FIX_REQUIRED"));
        }
        Ok(())
    }
    pub fn meta(&self) -> &AccountMeta {
        &self.meta
    }
    pub fn sqlite_runtime(&self) -> StoreResult<(String, String)> {
        Ok((
            rusqlite::version().into(),
            self.connection
                .query_row("SELECT sqlite_source_id()", [], |r| r.get(0))?,
        ))
    }
    pub fn integrity_check(&self) -> StoreResult<()> {
        let s: String = self
            .connection
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
        if s != "ok" {
            return Err(StoreError::Invalid("ACCOUNT_STORAGE_CORRUPT"));
        }
        Ok(())
    }
}

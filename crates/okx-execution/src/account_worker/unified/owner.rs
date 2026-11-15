//! Sole SQLite connection, bounded queue and shutdown lifetime.
#[cfg(test)]
use super::require_offline_finance_trial;
use super::{
    UnifiedFinanceBoundSettlement, UnifiedFinanceBoundSettlementReceipt, UnifiedFinanceBoundUnsent,
    UnifiedFinanceBoundUnsentReceipt, UnifiedFinanceHandle, UnifiedFinancePolicy,
    UnifiedFinanceWrite, UnifiedFinanceWriteReceipt,
};
use super::{apply_original_unified_finance_transaction, require_verified_live_finance_authority};
use crate::finance_owner_gate::{self, AuthorityModeError, AuthorityModeLease};
use crate::{AccountStore, StoreError, StoreResult};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        mpsc::{SyncSender, TrySendError, sync_channel},
    },
    time::{Duration, Instant},
};
use tokio::sync::{oneshot, watch};
type FinanceJob = Box<dyn FnOnce(&mut rusqlite::Connection) + Send + 'static>;

pub(super) struct FinanceWorkerOwner {
    sender: SyncSender<FinanceJob>,
    terminated: watch::Receiver<bool>,
}

impl UnifiedFinanceHandle {
    pub async fn open_stopped_import(
        runtime_root: PathBuf,
        installation: String,
        accounts: Vec<String>,
        campaigns: Vec<String>,
    ) -> StoreResult<Self> {
        if !runtime_root.is_absolute()
            || installation.is_empty()
            || accounts.is_empty()
            || campaigns.is_empty()
        {
            return Err(StoreError::Invalid("UNIFIED_FINANCE_OWNER_SCOPE_INVALID"));
        }
        // This exact path is the only future installation finance authority.
        // No auto-create or re-initialization: all original source tables and
        // immutable manifest must already exist and verify in the same file.
        let path = runtime_root.join("finance").join("finance.sqlite");
        if !path.is_file() {
            return Err(StoreError::Invalid("UNIFIED_FINANCE_IMPORT_NOT_FOUND"));
        }
        let path = path.canonicalize()?;
        let runtime_root = runtime_root.canonicalize()?;
        let (sender, receiver) = sync_channel::<FinanceJob>(128);
        let (ready_tx, ready_rx) = oneshot::channel();
        let (terminated_tx, terminated_rx) = watch::channel(false);
        let startup_installation = installation.clone();
        std::thread::Builder::new()
            .name("okx-finance-sql".into())
            .spawn(move || {
                let startup = (|| -> StoreResult<(rusqlite::Connection, std::fs::File, AuthorityModeLease)> {
                    use std::fs::OpenOptions;
                    // Exclusive across ALL legacy accounts/campaigns, not just
                    // other openers of this particular finance.sqlite file.
                    let mode_lease = finance_owner_gate::unified_mode_for(&runtime_root)
                        .map_err(|error| match error {
                            AuthorityModeError::Io(error) => StoreError::Io(error),
                            AuthorityModeError::Busy => {
                                StoreError::Invalid("UNIFIED_FINANCE_WRITER_BUSY")
                            }
                            AuthorityModeError::FinancePresent => {
                                StoreError::Invalid("UNIFIED_FINANCE_MODE_INVALID")
                            }
                        })?;
                    let lease = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .truncate(false)
                        .open(format!("{}.writer.lock", path.display()))?;
                    match lease.try_lock() {
                        Ok(()) => {}
                        Err(std::fs::TryLockError::WouldBlock) => {
                            return Err(StoreError::Invalid("UNIFIED_FINANCE_WRITER_BUSY"));
                        }
                        Err(std::fs::TryLockError::Error(error)) => {
                            return Err(StoreError::Io(error));
                        }
                    }
                    let db = rusqlite::Connection::open_with_flags(
                        &path,
                        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
                    )?;
                    db.busy_timeout(Duration::from_millis(250))?;
                    db.pragma_update(None, "foreign_keys", "ON")?;
                    // Must not silently alter WAL mode or repair an unknown
                    // source. The original finance importer proves schema,
                    // 47 tables, identity, digest, integrity and all FKs.
                    AccountStore::inspect_offline_finance_import(
                        &db,
                        &startup_installation,
                        &accounts.iter().map(String::as_str).collect::<Vec<_>>(),
                        &campaigns.iter().map(String::as_str).collect::<Vec<_>>(),
                    )?;
                    Ok((db, lease, mode_lease))
                })();
                let (mut db, lease, mode_lease) = match startup {
                    Ok(resources) => resources,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                        return;
                    }
                };
                let _ = ready_tx.send(Ok(()));
                while let Ok(job) = receiver.recv() {
                    job(&mut db);
                }
                // Publish shutdown only AFTER the sole writer, WAL handles
                // and exclusive cross-process lease have been released.
                drop(db);
                let _ = lease.unlock();
                drop(lease);
                drop(mode_lease);
                let _ = terminated_tx.send(true);
            })?;
        ready_rx
            .await
            .map_err(|_| StoreError::Invalid("UNIFIED_FINANCE_WORKER_STOPPED"))??;
        Ok(Self {
            owner: Arc::new(FinanceWorkerOwner {
                sender,
                terminated: terminated_rx,
            }),
            installation,
        })
    }

    pub fn termination_signal(&self) -> watch::Receiver<bool> {
        self.owner.terminated.clone()
    }

    // One FIFO and a single Connection for all future typed account/campaign
    // calls. Raw SQL jobs remain PRIVATE to this module; callers cannot
    // bypass the original ledger/RiskBook business owners via this API.
    pub(super) async fn submit<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut rusqlite::Connection) -> StoreResult<T> + Send + 'static,
    ) -> StoreResult<T> {
        self.submit_before(None, operation).await
    }

    /// The deadline applies only BEFORE execution begins. Once the FIFO accepts
    /// a financial job, dropping its caller never aborts a possible COMMIT.
    /// A vanished worker/reply after enqueue is UNKNOWN, not a safe retry.
    pub(super) async fn submit_before<T: Send + 'static>(
        &self,
        start_deadline: Option<Instant>,
        operation: impl FnOnce(&mut rusqlite::Connection) -> StoreResult<T> + Send + 'static,
    ) -> StoreResult<T> {
        if start_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(StoreError::Invalid("UNIFIED_FINANCE_QUEUE_DEADLINE"));
        }
        let (tx, rx) = oneshot::channel();
        let job: FinanceJob = Box::new(move |db| {
            if start_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                let _ = tx.send(Err(StoreError::Invalid("UNIFIED_FINANCE_QUEUE_DEADLINE")));
                return;
            }
            let _ = tx.send(operation(db));
        });
        self.owner
            .sender
            .try_send(job)
            .map_err(|error| match error {
                TrySendError::Full(_) => StoreError::Invalid("UNIFIED_FINANCE_QUEUE_FULL"),
                TrySendError::Disconnected(_) => {
                    StoreError::Invalid("UNIFIED_FINANCE_WORKER_STOPPED")
                }
            })?;
        // Accepted is not completed. On cancellation the worker still runs.
        // If the one-shot is lost, only a durable identity readback may resolve it.
        rx.await
            .map_err(|_| StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY"))?
    }

    /// This is the ONLY future production financial COMMIT entrypoint.
    /// It uses the very same original AccountStore/RiskBook transaction
    /// functions as the offline physical tests. In the current schema the
    /// sealed import is strictly OFFLINE_NOT_AUTHORIZED, so this method
    /// fails BEFORE applying any financial rule or source mutation.
    /// RB09 must be accepted before a real activation receipt exists.
    pub(crate) async fn apply_financial_write(
        &self,
        command: UnifiedFinanceWrite,
    ) -> StoreResult<UnifiedFinanceWriteReceipt> {
        let installation = self.installation.clone();
        // A queued mutation is safe to reject BEFORE the SQL owner starts it.
        // After start, neither a dropped caller nor a late reply can cancel the
        // transaction or authorize a blind retry of a financial/venue command.
        self.submit_before(Some(Instant::now() + Duration::from_secs(10)), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_verified_live_finance_authority(&tx, &installation)?;
            let result = apply_original_unified_finance_transaction(&tx, &installation, command)?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(result)
        })
        .await
    }

    pub async fn ingest_captured_bills(
        &self,
        account: String,
        input: crate::CapturedBillPage,
        conversion: crate::UsdConversionEvidence,
    ) -> StoreResult<()> {
        match self
            .apply_financial_write(UnifiedFinanceWrite::IngestCapturedBills {
                account,
                input,
                conversion,
            })
            .await?
        {
            UnifiedFinanceWriteReceipt::SourcePageIngested => Ok(()),
            UnifiedFinanceWriteReceipt::SourcePageRejected { reason } => {
                Err(StoreError::Invalid(reason))
            }
            _ => Err(StoreError::Invalid(
                "UNIFIED_SOURCE_PAGE_RECEIPT_UNEXPECTED",
            )),
        }
    }

    /// Bind source ownership, normalize USD and commit the page in the same
    /// transaction. No source attribution can race a separate SQL readback.
    pub async fn ingest_captured_fills(
        &self,
        account: String,
        input: crate::CapturedFillPage,
        conversion: crate::UsdConversionEvidence,
    ) -> StoreResult<()> {
        match self
            .apply_financial_write(UnifiedFinanceWrite::IngestCapturedFills {
                account,
                input,
                conversion,
            })
            .await?
        {
            UnifiedFinanceWriteReceipt::SourcePageIngested => Ok(()),
            UnifiedFinanceWriteReceipt::SourcePageRejected { reason } => {
                Err(StoreError::Invalid(reason))
            }
            _ => Err(StoreError::Invalid(
                "UNIFIED_SOURCE_PAGE_RECEIPT_UNEXPECTED",
            )),
        }
    }

    /// Same original USD conversion and atomic page ingestion as the source
    /// collector. A missing rate commits only a stop and returns its reason.
    pub async fn ingest_source_page_with_usd(
        &self,
        account: String,
        page: crate::IngestPage,
        conversion: crate::UsdConversionEvidence,
    ) -> StoreResult<()> {
        match self
            .apply_financial_write(UnifiedFinanceWrite::IngestSourcePageUsd {
                account,
                page,
                conversion,
            })
            .await?
        {
            UnifiedFinanceWriteReceipt::SourcePageIngested => Ok(()),
            UnifiedFinanceWriteReceipt::SourcePageRejected { reason } => {
                Err(StoreError::Invalid(reason))
            }
            _ => Err(StoreError::Invalid(
                "UNIFIED_SOURCE_PAGE_RECEIPT_UNEXPECTED",
            )),
        }
    }

    /// Commit all financial-history page components and its cursor in one
    /// transaction. Authenticity belongs to the source producer; activation
    /// remains independently required before this owner can commit.
    pub async fn ingest_source_page(
        &self,
        account: String,
        page: crate::IngestPage,
    ) -> StoreResult<()> {
        match self
            .apply_financial_write(UnifiedFinanceWrite::IngestSourcePage { account, page })
            .await?
        {
            UnifiedFinanceWriteReceipt::SourcePageIngested => Ok(()),
            _ => Err(StoreError::Invalid(
                "UNIFIED_SOURCE_PAGE_RECEIPT_UNEXPECTED",
            )),
        }
    }

    /// Store original immutable bills and their venue-order links atomically.
    /// These raw facts alone cannot advance coverage or debit/release risk.
    pub async fn record_source_bills(
        &self,
        account: String,
        bills: Vec<crate::Bill>,
        order_sources: Vec<crate::BillOrderSource>,
    ) -> StoreResult<()> {
        match self
            .apply_financial_write(UnifiedFinanceWrite::RecordSourceBills {
                account,
                bills,
                order_sources,
            })
            .await?
        {
            UnifiedFinanceWriteReceipt::SourceBillsRecorded => Ok(()),
            _ => Err(StoreError::Invalid(
                "UNIFIED_SOURCE_BILL_RECEIPT_UNEXPECTED",
            )),
        }
    }

    /// Store raw authenticated-source fills and pending projection atomically.
    /// Coverage is advanced only by complete page ingestion, never here.
    pub async fn record_source_fills(
        &self,
        account: String,
        fills: Vec<crate::Fill>,
    ) -> StoreResult<()> {
        match self
            .apply_financial_write(UnifiedFinanceWrite::RecordSourceFills { account, fills })
            .await?
        {
            UnifiedFinanceWriteReceipt::SourceFillsRecorded => Ok(()),
            _ => Err(StoreError::Invalid(
                "UNIFIED_SOURCE_FILL_RECEIPT_UNEXPECTED",
            )),
        }
    }

    /// The original AccountStore account-policies and RiskBook revision are
    /// one local transaction. No per-account owner, second COMMIT or Cloudflare
    /// finance write. Before accepted RB09 this remains strictly fail-closed.
    pub async fn apply_governance_policy(&self, policy: UnifiedFinancePolicy) -> StoreResult<()> {
        match self
            .apply_financial_write(UnifiedFinanceWrite::ApplyPolicy { policy })
            .await?
        {
            UnifiedFinanceWriteReceipt::PolicyApplied => Ok(()),
            _ => Err(StoreError::Invalid("UNIFIED_POLICY_RECEIPT_UNEXPECTED")),
        }
    }

    /// The ONLY native SettleEntry public ingress. The wire supplies a
    /// matching address/revision, not money authority. No durable mutation
    /// may occur before RB09 verifies the old owner stopped and creates an
    /// accepted native activation receipt. No secondary SQL or Cloudflare RPC.
    pub async fn settle_bound_entry(
        &self,
        request: UnifiedFinanceBoundSettlement,
    ) -> StoreResult<UnifiedFinanceBoundSettlementReceipt> {
        match self
            .apply_financial_write(UnifiedFinanceWrite::SettleBoundEntry { request })
            .await?
        {
            UnifiedFinanceWriteReceipt::BoundEntrySettled(receipt) => Ok(receipt),
            _ => Err(StoreError::Invalid("UNIFIED_SETTLEMENT_RECEIPT_UNEXPECTED")),
        }
    }

    /// Source-bound native release. No-send is proven by the ORIGINAL
    /// AccountStore terminal operation and reserve-attempt receipt in this
    /// writer, never by caller absence, flags, a timeout or foreign ACK.
    pub async fn release_bound_unsent(
        &self,
        request: UnifiedFinanceBoundUnsent,
    ) -> StoreResult<UnifiedFinanceBoundUnsentReceipt> {
        match self
            .apply_financial_write(UnifiedFinanceWrite::ReleaseBoundUnsent { request })
            .await?
        {
            UnifiedFinanceWriteReceipt::BoundUnsentReleased(receipt) => Ok(receipt),
            _ => Err(StoreError::Invalid("UNIFIED_UNSENT_RECEIPT_UNEXPECTED")),
        }
    }

    /// The SAME real Rust domain dispatch for direct contract witnesses.
    /// Only the immutable stopped import may be used; the caller never
    /// commits a staged grant, ACK, reserve, loss or release.
    #[cfg(test)]
    pub(crate) async fn trial_financial_write(
        &self,
        command: UnifiedFinanceWrite,
    ) -> StoreResult<UnifiedFinanceWriteReceipt> {
        let installation = self.installation.clone();
        self.submit(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_offline_finance_trial(&tx, &installation)?;
            let result = apply_original_unified_finance_transaction(&tx, &installation, command);
            tx.rollback()?;
            result
        })
        .await
    }

    /// Read-only installation-scoped diagnostic. No finance mutation API is
    /// exported while the import marker says OFFLINE_NOT_AUTHORIZED.
    /// TEST ONLY: exercise original durable POSTING -> UNKNOWN recovery
    /// through the same bounded worker. The source import stays immutable:
    /// every staged receipt/intent change is rolled back on this endpoint.
    #[cfg(test)]
    pub(crate) async fn trial_original_recover_posting(
        &self,
        account: String,
        now: i64,
    ) -> StoreResult<usize> {
        match self
            .trial_financial_write(UnifiedFinanceWrite::RecoverPosting { account, now })
            .await?
        {
            UnifiedFinanceWriteReceipt::Recovered { changed } => Ok(changed),
            _ => Err(StoreError::Invalid("UNIFIED_RECOVERY_RECEIPT_UNEXPECTED")),
        }
    }

    /// TEST ONLY: exercise installation-wide native UNKNOWN recovery on
    /// the existing single SQL FIFO; any account failure rolls back all
    /// staged accounts, and the immutable offline import remains unchanged.
    #[cfg(test)]
    pub(crate) async fn trial_original_recover_installation(&self, now: i64) -> StoreResult<usize> {
        match self
            .trial_financial_write(UnifiedFinanceWrite::RecoverInstallation { now })
            .await?
        {
            UnifiedFinanceWriteReceipt::Recovered { changed } => Ok(changed),
            _ => Err(StoreError::Invalid("UNIFIED_RECOVERY_RECEIPT_UNEXPECTED")),
        }
    }

    /// TEST ONLY: check the original venue readback/ACK/terminal source
    /// transition on the same installation owner without persisting a
    /// synthetic exchange response to the immutable import.
    #[cfg(test)]
    pub(crate) async fn trial_original_record_outcome(
        &self,
        account: String,
        intent: String,
        state: crate::IntentState,
        exchange_order_id: Option<String>,
        evidence: String,
        now: i64,
    ) -> StoreResult<()> {
        match self
            .trial_financial_write(UnifiedFinanceWrite::RecordOutcome {
                account,
                intent,
                state,
                exchange_order_id,
                evidence,
                now,
            })
            .await?
        {
            UnifiedFinanceWriteReceipt::OutcomeRecorded => Ok(()),
            _ => Err(StoreError::Invalid("UNIFIED_OUTCOME_RECEIPT_UNEXPECTED")),
        }
    }

    /// TEST ONLY: the same ORIGINAL AccountStore PREPARE transaction on the
    /// installation's one bounded writer. The offline source manifest is
    /// immutable and ALL staged decision/intent/plan writes roll back.
    #[cfg(test)]
    pub(crate) async fn trial_original_prepare(
        &self,
        account: String,
        decision: crate::Decision,
        intent: crate::NewIntent,
    ) -> StoreResult<bool> {
        match self
            .trial_financial_write(UnifiedFinanceWrite::Prepare {
                account,
                decision,
                intent,
            })
            .await?
        {
            UnifiedFinanceWriteReceipt::Prepared { inserted, .. } => Ok(inserted),
            _ => Err(StoreError::Invalid("UNIFIED_PREPARE_RECEIPT_UNEXPECTED")),
        }
    }

    /// TEST ONLY: full ORIGINAL account+RiskBook Reserve under this exact
    /// installation's bounded SQL owner. The immutable offline import must
    /// remain byte-for-byte recoverable: ALWAYS rollback even after HELD.
    /// This is not a staging grant, production Reserve, or cutover switch.
    #[cfg(test)]
    pub(crate) async fn trial_original_reserve(
        &self,
        campaign: String,
        input: crate::risk::ReserveInput,
        census: crate::risk::VerifiedCensus,
        receipt: crate::CentralReceipt,
    ) -> StoreResult<&'static str> {
        match self
            .trial_financial_write(UnifiedFinanceWrite::Reserve {
                campaign,
                input,
                census,
                receipt,
            })
            .await
            .map_err(|_| StoreError::Invalid("UNIFIED_ORIGINAL_RESERVE_TRIAL_REJECTED"))?
        {
            UnifiedFinanceWriteReceipt::Risk(crate::risk::UnifiedRiskDecision::Reserved {
                ..
            }) => Ok("HELD_UNCOMMITTED"),
            UnifiedFinanceWriteReceipt::Risk(crate::risk::UnifiedRiskDecision::Denied {
                ..
            }) => Ok("REJECTED_UNCOMMITTED"),
            _ => Err(StoreError::Invalid("UNIFIED_RESERVE_RECEIPT_UNEXPECTED")),
        }
    }

    /// TEST ONLY: both the ORIGINAL AccountStore no-send source and the
    /// RiskBook no-reserve mutation are checked on the same installation
    /// SQL worker/transaction. The offline import is ALWAYS rolled back,
    /// even after an internally accepted tombstone or HELD release.
    #[cfg(test)]
    pub(crate) async fn trial_original_unsent_from_account(
        &self,
        campaign: String,
        account: String,
        intent: String,
        expected_sequence: u64,
        generation: u64,
    ) -> StoreResult<(u64, String, bool)> {
        match self
            .trial_financial_write(UnifiedFinanceWrite::ReleaseUnsent {
                campaign,
                account,
                intent,
                expected_sequence,
                generation,
            })
            .await
            .map_err(|_| StoreError::Invalid("UNIFIED_UNSENT_SOURCE_OR_RISK_DENIED"))?
        {
            UnifiedFinanceWriteReceipt::Risk(
                crate::risk::UnifiedRiskDecision::UnsentReleased {
                    reservation_key,
                    book_sequence,
                    replayed,
                },
            ) => Ok((book_sequence, reservation_key, replayed)),
            _ => Err(StoreError::Invalid("UNIFIED_UNSENT_RECEIPT_UNEXPECTED")),
        }
    }

    /// TEST ONLY: the original AccountStore entry-partial source is read
    /// and settled in the SAME owned installation SQL transaction. All
    /// mutations are rolled back, since offline import is not authorized.
    #[cfg(test)]
    pub(crate) async fn trial_original_partial_from_account(
        &self,
        campaign: String,
        account: String,
        intent: String,
        expected_sequence: u64,
        generation: u64,
    ) -> StoreResult<(u64, String, bool)> {
        match self
            .trial_financial_write(UnifiedFinanceWrite::SettleEntry {
                campaign,
                account,
                intent,
                expected_sequence,
                generation,
                source: crate::risk::EntrySettlementSource::Partial,
            })
            .await
            .map_err(|_| StoreError::Invalid("UNIFIED_PARTIAL_SOURCE_OR_RISK_DENIED"))?
        {
            UnifiedFinanceWriteReceipt::Risk(crate::risk::UnifiedRiskDecision::EntrySettled {
                book_sequence,
                cumulative_entry_usd,
                replayed,
                ..
            }) => Ok((book_sequence, cumulative_entry_usd, replayed)),
            _ => Err(StoreError::Invalid("UNIFIED_PARTIAL_RECEIPT_UNEXPECTED")),
        }
    }

    /// The original RiskBook Inspect returns None for missing/foreign
    /// reservation or wrong plan. Absence is UNKNOWN, NOT permission to POST.
    pub(crate) async fn inspect_reservation(
        &self,
        campaign: String,
        reservation_key: String,
        plan_sha256: String,
    ) -> StoreResult<Option<crate::risk::ReservationReadback>> {
        if campaign.is_empty() {
            return Err(StoreError::Invalid("UNIFIED_FINANCE_CAMPAIGN_EMPTY"));
        }
        let installation = self.installation.clone();
        self.submit(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let readback = crate::risk::read_unified_campaign_reservation(
                &tx,
                &installation,
                &campaign,
                &reservation_key,
                &plan_sha256,
            )
            .map_err(StoreError::Invalid)?;
            tx.commit()?;
            Ok(readback)
        })
        .await
    }
}

#[cfg(test)]
mod unified_finance_queue_contract_tests {
    use super::*;

    fn offline_owner(sender: SyncSender<FinanceJob>) -> UnifiedFinanceHandle {
        let (_, terminated) = watch::channel(false);
        UnifiedFinanceHandle {
            owner: Arc::new(FinanceWorkerOwner { sender, terminated }),
            installation: "queue-fixture".into(),
        }
    }

    #[tokio::test]
    async fn expired_before_enqueue_never_enters_writer() {
        let (sender, receiver) = sync_channel::<FinanceJob>(1);
        let owner = offline_owner(sender);
        let error = owner
            .submit_before(
                Some(Instant::now() - Duration::from_millis(1)),
                |_db| -> StoreResult<()> { panic!("expired financial command must not execute") },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            StoreError::Invalid("UNIFIED_FINANCE_QUEUE_DEADLINE")
        ));
        assert!(matches!(
            receiver.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn full_and_disconnected_queue_are_definite_pre_acceptance_failures() {
        let (sender, receiver) = sync_channel::<FinanceJob>(1);
        let owner = offline_owner(sender.clone());
        assert!(sender.try_send(Box::new(|_| {})).is_ok());
        assert!(matches!(
            owner
                .submit(|_| -> StoreResult<()> { panic!("queue is full") })
                .await,
            Err(StoreError::Invalid("UNIFIED_FINANCE_QUEUE_FULL"))
        ));
        drop(receiver);
        assert!(matches!(
            owner
                .submit(|_| -> StoreResult<()> { panic!("worker is gone") })
                .await,
            Err(StoreError::Invalid("UNIFIED_FINANCE_WORKER_STOPPED"))
        ));
    }

    #[tokio::test]
    async fn accepted_then_lost_sql_reply_is_unknown_not_a_retry_permission() {
        let (sender, receiver) = sync_channel::<FinanceJob>(1);
        let worker = std::thread::spawn(move || {
            let accepted = receiver.recv().expect("queued job");
            // Emulate worker termination after admission but before a result.
            // A caller cannot infer whether a prior COMMIT took place.
            drop(accepted);
        });
        let owner = offline_owner(sender);
        let error = owner
            .submit(|_| -> StoreResult<()> { Ok(()) })
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
        ));
        worker.join().unwrap();
    }

    #[tokio::test]
    async fn one_sql_fifo_preserves_interleaved_account_order() {
        let (sender, receiver) = sync_channel::<FinanceJob>(2);
        let worker = std::thread::spawn(move || {
            let mut db = rusqlite::Connection::open_in_memory().unwrap();
            db.execute_batch("CREATE TABLE observed(account TEXT NOT NULL)")
                .unwrap();
            while let Ok(job) = receiver.recv() {
                job(&mut db);
            }
            db.query_row(
                "SELECT group_concat(account, ',') FROM (SELECT account FROM observed ORDER BY rowid)",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
        });
        let owner = offline_owner(sender);
        for account in ["account-a", "account-b", "account-a"] {
            owner
                .submit(move |db| {
                    db.execute("INSERT INTO observed(account) VALUES(?1)", [account])?;
                    Ok(())
                })
                .await
                .unwrap();
        }
        drop(owner);
        assert_eq!(worker.join().unwrap(), "account-a,account-b,account-a");
    }
}

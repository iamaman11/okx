//! Selection of the existing financial owner during Engine startup.
use crate::{
    AgentError, accounts::AccountRuntimeRegistry, finance_dispatch::FinanceDispatch,
    market_bootstrap::MarketBootstrapper, native_finance_risk::NativeFinanceRisk,
    risk_bridge::NativeRiskClient, trade_service::TradeService,
};
use okx_api::RateBudget;
use okx_trade_execution::{StoreError, account_worker::UnifiedFinanceHandle};
use okx_market_runtime::public::PublicWsHandle;
use std::{path::Path, sync::Arc};

/// The cause remains available for diagnostics; only the stable code crosses dispatch.
#[derive(Debug)]
pub struct FinanceStartupFailure {
    code: &'static str,
    cause: FinanceStartupCause,
}
#[derive(Debug)]
pub enum FinanceStartupCause {
    Metadata(std::io::Error),
    UnsafeFileType,
    Owner(AgentError),
    RiskScope(&'static str),
}
impl FinanceStartupFailure {
    pub fn code(&self) -> &'static str {
        self.code
    }
    pub fn cause(&self) -> &FinanceStartupCause {
        &self.cause
    }
    fn owner(error: AgentError) -> Self {
        let code = match &error {
            AgentError::FinancialCaptureTask(s) => match s.as_str() {
                "FINANCE_EXPECTED_CAMPAIGN_REQUIRED" => "FINANCE_EXPECTED_CAMPAIGN_REQUIRED",
                "FINANCE_STOPPED_CATALOG_COHORT_INVALID" => {
                    "FINANCE_STOPPED_CATALOG_COHORT_INVALID"
                }
                "FINANCE_STOPPED_MANIFEST_INVALID" => "FINANCE_STOPPED_MANIFEST_INVALID",
                "FINANCE_CAMPAIGN_IDENTITY_MISMATCH" => "FINANCE_CAMPAIGN_IDENTITY_MISMATCH",
                "ACCOUNT_CATALOG_INVALID" => "ACCOUNT_CATALOG_INVALID",
                _ => "FINANCE_AUTHORITY_UNAVAILABLE",
            },
            AgentError::Store(StoreError::Invalid(code)) => *code,
            AgentError::Store(StoreError::Sql(rusqlite::Error::SqliteFailure(e, _))) => {
                match e.code {
                    rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked => {
                        "FINANCE_STORAGE_BUSY"
                    }
                    rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase => {
                        "FINANCE_STORAGE_CORRUPT"
                    }
                    _ => "FINANCE_AUTHORITY_UNAVAILABLE",
                }
            }
            _ => "FINANCE_AUTHORITY_UNAVAILABLE",
        };
        Self {
            code,
            cause: FinanceStartupCause::Owner(error),
        }
    }
}

pub enum FinanceSelection {
    Missing,
    Failed(FinanceStartupFailure),
    Opened {
        accounts: Arc<AccountRuntimeRegistry>,
        owner: UnifiedFinanceHandle,
        stopped: bool,
        campaign: Option<String>,
    },
}

/// Select exactly the same owner used in production, without creating finance storage.
pub async fn select(
    root: &Path,
    budget: RateBudget,
    stopped: bool,
    campaign: Option<String>,
) -> FinanceSelection {
    match std::fs::symlink_metadata(root.join("finance").join("finance.sqlite")) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => FinanceSelection::Missing,
        Err(e) => FinanceSelection::Failed(FinanceStartupFailure {
            code: "FINANCE_AUTHORITY_UNAVAILABLE",
            cause: FinanceStartupCause::Metadata(e),
        }),
        Ok(m) if !m.is_file() || m.file_type().is_symlink() => {
            FinanceSelection::Failed(FinanceStartupFailure {
                code: "FINANCE_AUTHORITY_INVALID",
                cause: FinanceStartupCause::UnsafeFileType,
            })
        }
        Ok(_) => {
            let expected = campaign.as_deref().unwrap_or("");
            let result = if stopped {
                AccountRuntimeRegistry::open_stopped_finance(root, budget, expected).await
            } else {
                AccountRuntimeRegistry::open_native_finance(root, budget, expected).await
            };
            match result {
                Ok((accounts, owner)) => FinanceSelection::Opened {
                    accounts: Arc::new(accounts),
                    owner,
                    stopped,
                    campaign,
                },
                Err(error) => FinanceSelection::Failed(FinanceStartupFailure::owner(error)),
            }
        }
    }
}
impl FinanceSelection {
    pub async fn finish(
        self,
        public_ws: PublicWsHandle,
        market: MarketBootstrapper,
    ) -> FinanceDispatch {
        match self {
            Self::Missing => FinanceDispatch::missing(),
            Self::Failed(reason) => FinanceDispatch::failed(reason),
            Self::Opened {
                accounts,
                owner,
                stopped,
                campaign,
            } => {
                let backend = match campaign {
                    Some(c) => {
                        NativeFinanceRisk::new(owner, accounts.installation_id().to_owned(), c)
                    }
                    None => Ok(NativeFinanceRisk::from_owner(owner)),
                };
                match backend {
                    Ok(backend) => {
                        let risk = if stopped {
                            NativeRiskClient::local_stopped(backend)
                        } else {
                            NativeRiskClient::local(backend)
                        };
                        let trade = Arc::new(TradeService::new(accounts.clone(), public_ws, risk));
                        trade
                            .set_live_source(okx_market_runtime::live_source::LiveCandleSource::new(
                                market,
                            ))
                            .await;
                        if stopped {
                            FinanceDispatch::stopped(accounts, trade)
                        } else {
                            FinanceDispatch::ready(accounts, trade)
                        }
                    }
                    Err(code) => {
                        accounts.shutdown().await;
                        FinanceDispatch::failed(FinanceStartupFailure {
                            code,
                            cause: FinanceStartupCause::RiskScope(code),
                        })
                    }
                }
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use okx_trade_execution::NewFinanceInstallation;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    fn root() -> std::path::PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "finance-selection-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        root
    }
    async fn select_code(root: &Path) -> &'static str {
        match select(root, RateBudget::new(), false, None).await {
            FinanceSelection::Missing => "FINANCE_INSTALLATION_NOT_INITIALIZED",
            FinanceSelection::Failed(reason) => {
                let code = reason.code();
                let dispatch = FinanceDispatch::failed(reason);
                assert_eq!(dispatch.accounts().err().as_deref(), Some(code));
                assert_eq!(dispatch.trade().err().as_deref(), Some(code));
                assert_eq!(dispatch.startup_failure().unwrap().code(), code);
                code
            }
            FinanceSelection::Opened { .. } => panic!("unexpected financial owner"),
        }
    }
    async fn close(owner: UnifiedFinanceHandle) {
        let mut signal = owner.termination_signal();
        drop(owner);
        if !*signal.borrow() {
            tokio::time::timeout(Duration::from_secs(3), signal.changed())
                .await
                .unwrap()
                .unwrap();
        }
    }
    async fn initialized(root: &Path) -> UnifiedFinanceHandle {
        UnifiedFinanceHandle::initialize_new(
            root.to_path_buf(),
            NewFinanceInstallation {
                installation_id: "installation_test".into(),
                initialize_request_id: "initialize_test_001".into(),
                source_tree: "a".repeat(40),
            },
        )
        .await
        .unwrap()
    }
    #[tokio::test]
    async fn missing_and_corrupt_finance_use_real_selection() {
        let root = root();
        assert_eq!(
            select_code(&root).await,
            "FINANCE_INSTALLATION_NOT_INITIALIZED"
        );
        std::fs::create_dir(root.join("finance")).unwrap();
        std::fs::write(root.join("finance/finance.sqlite"), b"corrupt").unwrap();
        okx_ipc_contracts::account_catalog_proto::initialize(&root, "installation_test").unwrap();
        assert_eq!(select_code(&root).await, "FINANCE_FILE_FORMAT_UNKNOWN");
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn foreign_and_busy_owner_use_real_selection() {
        let root = root();
        let owner = initialized(&root).await;
        okx_ipc_contracts::account_catalog_proto::initialize(&root, "installation_test").unwrap();
        assert_eq!(select_code(&root).await, "UNIFIED_FINANCE_WRITER_BUSY");
        close(owner).await;
        std::fs::remove_file(root.join(okx_ipc_contracts::account_catalog_proto::CATALOG_FILE)).unwrap();
        okx_ipc_contracts::account_catalog_proto::initialize(&root, "installation_other").unwrap();
        assert_eq!(
            select_code(&root).await,
            "FINANCE_INSTALLATION_IDENTITY_MISMATCH"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn campaign_mismatch_uses_real_owner_policy() {
        use okx_trade_execution::account_worker::{
            NativeCampaignAccountPolicy, NativeCampaignPolicy, NativeCampaignStrategyPolicy,
        };
        use okx_trade_execution::{AccountMeta, OpeningSnapshot};
        use rust_decimal::Decimal;
        use std::time::Instant;
        let root = root();
        let owner = initialized(&root).await;
        let key = format!("okx:real:{}", "a".repeat(64));
        let opening = OpeningSnapshot {
            opening_equity_usd: Decimal::from(100),
            source_ref: "a".repeat(64),
            observed_at: 100,
            positions: 0,
            orders: 0,
            conditional_orders: 0,
            liabilities: Decimal::ZERO,
            balances: vec![("USDT".into(), Decimal::from(100))],
        };
        let deadline = || Instant::now() + Duration::from_secs(2);
        owner
            .initialize_account(
                deadline(),
                AccountMeta {
                    account_key: key.clone(),
                    installation_id: "installation_test".into(),
                    initialized_at: 100,
                    opening_source: opening.source_ref.clone(),
                    initialize_operation_id: "connect_native_001".into(),
                    release_id: "a".repeat(40),
                },
                opening,
                "d".repeat(64),
            )
            .await
            .unwrap();
        owner
            .apply_campaign_policy(
                deadline(),
                NativeCampaignPolicy {
                    campaign_id: format!("sha256:{}", "c".repeat(64)),
                    policy_revision: 1,
                    generation: 1,
                    global_limit_usdt: "10".into(),
                    loss_limit_usd: "10".into(),
                    entry_limit_usd: "20".into(),
                    accounts: vec![NativeCampaignAccountPolicy {
                        account_key: key,
                        paused: false,
                        limit_usdt: "5".into(),
                        order_limit_usdt: "2".into(),
                        entry_limit_usd: "10".into(),
                        strategies: vec![NativeCampaignStrategyPolicy {
                            strategy_id: "manual".into(),
                            limit_usdt: "5".into(),
                        }],
                    }],
                },
            )
            .await
            .unwrap();
        close(owner).await;
        okx_ipc_contracts::account_catalog_proto::initialize(&root, "installation_test").unwrap();
        let result = select(
            &root,
            RateBudget::new(),
            false,
            Some(format!("sha256:{}", "d".repeat(64))),
        )
        .await;
        assert!(
            matches!(result, FinanceSelection::Failed(ref f) if f.code() == "FINANCE_CAMPAIGN_IDENTITY_MISMATCH")
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}

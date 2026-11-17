use crate::{accounts::AccountRuntimeRegistry, trade_service::TradeService};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinanceReadiness {
    Ready,
    Stopped,
    Missing,
    Invalid,
}

#[derive(Clone)]
pub struct FinanceDispatch {
    readiness: FinanceReadiness,
    owner: Option<(Arc<AccountRuntimeRegistry>, Arc<TradeService>)>,
    failure: Option<Arc<crate::finance_startup::FinanceStartupFailure>>,
}
impl FinanceDispatch {
    pub fn ready(accounts: Arc<AccountRuntimeRegistry>, trade: Arc<TradeService>) -> Self {
        Self {
            readiness: FinanceReadiness::Ready,
            owner: Some((accounts, trade)),
            failure: None,
        }
    }
    pub fn stopped(accounts: Arc<AccountRuntimeRegistry>, trade: Arc<TradeService>) -> Self {
        Self {
            readiness: FinanceReadiness::Stopped,
            owner: Some((accounts, trade)),
            failure: None,
        }
    }
    pub fn missing() -> Self {
        Self {
            readiness: FinanceReadiness::Missing,
            owner: None,
            failure: None,
        }
    }
    pub fn invalid() -> Self {
        Self {
            readiness: FinanceReadiness::Invalid,
            owner: None,
            failure: None,
        }
    }
    pub fn failed(failure: crate::finance_startup::FinanceStartupFailure) -> Self {
        Self {
            readiness: FinanceReadiness::Invalid,
            owner: None,
            failure: Some(Arc::new(failure)),
        }
    }
    pub fn startup_failure(&self) -> Option<&crate::finance_startup::FinanceStartupFailure> {
        self.failure.as_deref()
    }
    pub fn readiness(&self) -> FinanceReadiness {
        self.readiness
    }
    fn unavailable(&self) -> String {
        if let Some(failure) = &self.failure {
            return failure.code().into();
        }
        match self.readiness {
            FinanceReadiness::Missing => "FINANCE_INSTALLATION_NOT_INITIALIZED",
            FinanceReadiness::Invalid => "FINANCE_AUTHORITY_INVALID",
            FinanceReadiness::Stopped => "FINANCE_STOPPED",
            FinanceReadiness::Ready => "FINANCE_AUTHORITY_UNAVAILABLE",
        }
        .into()
    }
    pub fn accounts(&self) -> Result<&Arc<AccountRuntimeRegistry>, String> {
        self.owner
            .as_ref()
            .map(|v| &v.0)
            .ok_or_else(|| self.unavailable())
    }
    pub fn trade(&self) -> Result<&Arc<TradeService>, String> {
        self.owner
            .as_ref()
            .map(|v| &v.1)
            .ok_or_else(|| self.unavailable())
    }
    pub async fn shutdown(&self) {
        if let Some((accounts, _)) = &self.owner {
            accounts.shutdown().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_finance_never_supplies_financial_authority() {
        let missing = FinanceDispatch::missing();
        assert_eq!(missing.readiness(), FinanceReadiness::Missing);
        assert!(missing.accounts().is_err());
        assert!(missing.trade().is_err());
        let invalid = FinanceDispatch::invalid();
        assert_eq!(invalid.readiness(), FinanceReadiness::Invalid);
        assert!(invalid.accounts().is_err());
        assert!(invalid.trade().is_err());
    }
}

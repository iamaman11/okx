//! Account-local native adapters. AccountStoreHandle is the sole SQL owner;
//! this object owns no parallel ledger, access grant or financial epoch.
use crate::AgentResult;
use okx_api::{
    AccountApi, AccountHistoryApi, Credentials, OkxEnvironment, OkxRestClient, RateBudget, TradeApi,
};
use okx_execution::AccountStoreHandle;

pub struct ExecutionRuntime {
    pub store: AccountStoreHandle,
    pub trade: TradeApi,
    pub history: AccountHistoryApi,
    pub account: AccountApi,
    pub clock: OkxRestClient,
    pub environment: OkxEnvironment,
    uid: String,
}
impl ExecutionRuntime {
    pub fn new(
        store: AccountStoreHandle,
        environment: OkxEnvironment,
        credentials: Credentials,
        uid: String,
        budget: RateBudget,
    ) -> AgentResult<Self> {
        let rest = OkxRestClient::with_rate_budget(environment, credentials, budget)?;
        Ok(Self {
            store,
            trade: TradeApi::new(rest.clone()),
            history: AccountHistoryApi::new(rest.clone()),
            account: AccountApi::new(rest.clone()),
            clock: rest,
            environment,
            uid,
        })
    }
    pub async fn verified_uid_matches(&self, uid: &str) -> AgentResult<bool> {
        let config = self.account.config().await?;
        Ok(okx_api::account_uid_fingerprint(&config.uid) == uid && self.uid == uid)
    }
    pub fn uid(&self) -> &str {
        &self.uid
    }
}

//! Read-only view of the Engine's sole verified account catalog. CONTROL never edits aliases or credentials.
use crate::{HostControlError, HostControlResult};
use okx_protocol::{account_catalog_proto,accounts::{AccountCatalog, VerifiedAccountConfig}};
use serde_json::{Value, json};
use std::path::PathBuf;
pub struct AccountCatalogStore {
    root: PathBuf,
}
impl AccountCatalogStore {
    pub fn canonical() -> Self {
        Self {
            root: PathBuf::from(r"C:\okx-runtime"),
        }
    }
    pub fn load(&self) -> HostControlResult<AccountCatalog> {
        Ok(account_catalog_proto::load(&self.root)?)
    }
    pub fn status(&self) -> HostControlResult<Value> {
        let c = self.load()?;
        Ok(
            json!({"schema":c.schema,"installation_id":c.installation_id,"accounts":c.accounts,"as_of_ms":chrono::Utc::now().timestamp_millis()}),
        )
    }
    pub fn status_summary(&self) -> HostControlResult<Value> {
        let c = self.load()?;
        Ok(
            json!({"schema":c.schema,"installation_id":c.installation_id,"accounts_total":c.accounts.len()}),
        )
    }
    pub fn require_account(
        &self,
        key: &str,
        installation: &str,
    ) -> HostControlResult<VerifiedAccountConfig> {
        let c = self.load()?;
        if c.installation_id != installation {
            return Err(HostControlError::AccountCatalogConflict);
        }
        c.accounts
            .into_iter()
            .find(|a| a.account_key.wire() == key)
            .ok_or(HostControlError::AccountCatalogInvalid)
    }
}

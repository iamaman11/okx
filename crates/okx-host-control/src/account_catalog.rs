//! Read-only view of the Engine's sole verified account catalog. CONTROL never edits aliases or credentials.
use crate::{HostControlError, HostControlResult};
use okx_protocol::accounts::{AccountCatalog, VerifiedAccountConfig};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{fs, path::PathBuf};
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
        let installation: Value =
            serde_json::from_slice(&fs::read(self.root.join("installation.json"))?)?;
        if installation["schema"] != "okx.installation/v1" {
            return Err(HostControlError::AccountCatalogInvalid);
        }
        let installation_id = installation["installation_id"]
            .as_str()
            .ok_or(HostControlError::AccountCatalogInvalid)?
            .to_owned();
        let mut accounts = Vec::new();
        for entry in fs::read_dir(self.root.join("account-catalog"))? {
            let entry = entry?;
            if entry.path().extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            if entry.metadata()?.len() > 8192 {
                return Err(HostControlError::AccountCatalogInvalid);
            }
            let config: VerifiedAccountConfig = serde_json::from_slice(&fs::read(entry.path())?)?;
            if entry.file_name().to_string_lossy()
                != format!(
                    "{:x}.json",
                    Sha256::digest(config.account_key.wire().as_bytes())
                )
            {
                return Err(HostControlError::AccountCatalogInvalid);
            }
            accounts.push(config);
            if accounts.len() > 32 {
                return Err(HostControlError::AccountCatalogFull);
            }
        }
        let catalog = AccountCatalog {
            schema: AccountCatalog::SCHEMA.into(),
            installation_id,
            accounts,
        };
        if !catalog.valid() {
            return Err(HostControlError::AccountCatalogInvalid);
        }
        Ok(catalog)
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

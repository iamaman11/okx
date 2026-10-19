use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use okx_protocol::{ACCOUNT_CATALOG_ENTRY_SCHEMA_V1, AccountCatalogEntry, TradingVenue};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    HostControlError, HostControlResult,
    desired::{atomic_replace, temp_path},
};

pub const ACCOUNT_CATALOG_SCHEMA_V1: &str = "okx.account-catalog/v1";
const CANONICAL_PATH: &str = r"C:\okx-control\accounts.json";
const MAX_CATALOG_ENTRIES: usize = 32;
const MAX_CATALOG_BYTES: u64 = 16_384;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AccountCatalogFile {
    schema: String,
    entries: Vec<AccountCatalogEntry>,
}

impl AccountCatalogFile {
    fn empty() -> Self {
        Self {
            schema: ACCOUNT_CATALOG_SCHEMA_V1.to_owned(),
            entries: Vec::new(),
        }
    }

    fn validate(&self) -> HostControlResult<()> {
        if self.schema != ACCOUNT_CATALOG_SCHEMA_V1 || self.entries.len() > MAX_CATALOG_ENTRIES {
            return Err(HostControlError::AccountCatalogInvalid);
        }
        let mut ids = BTreeSet::new();
        let mut uids = BTreeSet::new();
        for entry in &self.entries {
            entry
                .validate()
                .map_err(|_| HostControlError::AccountCatalogInvalid)?;
            if !ids.insert(entry.account_id.as_str())
                || !uids.insert(entry.expected_uid_fingerprint.as_str())
            {
                return Err(HostControlError::AccountCatalogInvalid);
            }
        }
        Ok(())
    }

    fn public_value(&self) -> Value {
        json!({
            "schema": ACCOUNT_CATALOG_SCHEMA_V1,
            "status": "METADATA_ONLY",
            "entries_total": self.entries.len(),
            "selectable": false,
            "entries": self.entries.iter().map(|e| json!({
                "schema": ACCOUNT_CATALOG_ENTRY_SCHEMA_V1,
                "venue": e.venue.as_str(),
                "account_id": e.account_id,
                "label": e.label,
                "environment": e.environment,
                "expected_uid_fingerprint": e.expected_uid_fingerprint,
                // A deterministic GitHub Environment name, not a second secret owner.
                "observer_github_environment": format!(
                    "{}-{}-global-observer",
                    e.venue.as_str(),
                    e.account_id
                ),
                "identity": "UNVERIFIED",
                "credentials": "NOT_ATTESTED",
                "selectable": false,
                "trading_authorized": false
            })).collect::<Vec<_>>()
        })
    }
}

/// An implemented native exchange/account slot. Additional venues fail closed
/// until a verified adapter and credential binding exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountRoute {
    Real,
    Demo,
}

/// Static onboarding metadata is not a second lifecycle journal.
/// Only existing CONTROL may mutate it; desired.json remains the sole Job owner.
#[derive(Debug, Clone)]
pub struct AccountCatalogStore {
    path: PathBuf,
}

impl AccountCatalogStore {
    pub fn canonical() -> Self {
        Self {
            path: PathBuf::from(CANONICAL_PATH),
        }
    }

    #[cfg(test)]
    fn at(path: PathBuf) -> Self {
        Self { path }
    }

    fn load(&self) -> HostControlResult<AccountCatalogFile> {
        // Only ENOENT means an empty catalog. Access denied, reparse or
        // metadata I/O failure must never masquerade as no registered accounts.
        let length = match fs::metadata(&self.path) {
            Ok(meta) => meta.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(AccountCatalogFile::empty());
            }
            Err(error) => return Err(error.into()),
        };
        if length > MAX_CATALOG_BYTES {
            return Err(HostControlError::AccountCatalogInvalid);
        }
        let content = fs::read(&self.path)?;
        let file: AccountCatalogFile = serde_json::from_slice(&content)
            .map_err(|_| HostControlError::AccountCatalogInvalid)?;
        file.validate()?;
        Ok(file)
    }

    pub fn status(&self) -> HostControlResult<Value> {
        let mut status = self.load()?.public_value();
        // The two existing exchange-bound slots share the same account selector.
        // Selectable is only a command shape: each live handoff independently
        // proves installed credentials, UID, flat/UNKNOWN and venue sources.
        status["selectable_account_ids"] = json!([
            {"venue":"okx","account_id":"real","identity":"VERIFY_ON_SELECTION"},
            {"venue":"okx","account_id":"demo","identity":"VERIFY_ON_SELECTION"}
        ]);
        status["implemented_venue_adapters"] = json!(["okx"]);
        Ok(status)
    }

    /// The single native account router. A metadata-only named account (or an
    /// unimplemented venue adapter) is never promoted to a running profile.
    pub fn resolve(&self, venue: TradingVenue, account_id: &str) -> HostControlResult<AccountRoute> {
        let catalog = self.load()?;
        if catalog
            .entries
            .iter()
            .any(|e| e.venue == venue && e.account_id == account_id)
        {
            return Err(HostControlError::ProfileHandoffProofRequired);
        }
        match (venue, account_id) {
            (TradingVenue::Okx, "real") => Ok(AccountRoute::Real),
            (TradingVenue::Okx, "demo") => Ok(AccountRoute::Demo),
            _ => Err(HostControlError::ProfileHandoffProofRequired),
        }
    }

    /// Ordinary CONTROL status stays small even at full catalog capacity.
    /// The explicit account_catalog command owns the bounded full list.
    pub fn status_summary(&self) -> HostControlResult<Value> {
        let catalog = self.load()?;
        Ok(json!({
            "schema": ACCOUNT_CATALOG_SCHEMA_V1,
            "status": "METADATA_ONLY",
            "entries_total": catalog.entries.len(),
            "selectable": false
        }))
    }

    pub fn register(&self, entry: AccountCatalogEntry) -> HostControlResult<Value> {
        entry
            .validate()
            .map_err(|_| HostControlError::AccountCatalogInvalid)?;
        let mut file = self.load()?;
        if let Some(existing) = file
            .entries
            .iter()
            .find(|e| e.account_id == entry.account_id)
        {
            if existing == &entry {
                return Ok(
                    json!({"status":"ALREADY_REGISTERED","account_id":entry.account_id,
                    "identity":"UNVERIFIED","selectable":false,"trading_authorized":false}),
                );
            }
            return Err(HostControlError::AccountCatalogConflict);
        }
        if file
            .entries
            .iter()
            .any(|e| e.expected_uid_fingerprint == entry.expected_uid_fingerprint)
        {
            return Err(HostControlError::AccountCatalogConflict);
        }
        if file.entries.len() >= MAX_CATALOG_ENTRIES {
            return Err(HostControlError::AccountCatalogFull);
        }
        file.entries.push(entry.clone());
        file.entries.sort_by(|a, b| a.account_id.cmp(&b.account_id));
        file.validate()?;
        self.persist(&file)?;
        Ok(
            json!({"status":"REGISTERED_METADATA_ONLY","account_id":entry.account_id,
            "identity":"UNVERIFIED","selectable":false,"trading_authorized":false}),
        )
    }

    fn persist(&self, file: &AccountCatalogFile) -> HostControlResult<()> {
        file.validate()?;
        let content = serde_json::to_vec(file)?;
        if content.len() as u64 > MAX_CATALOG_BYTES {
            return Err(HostControlError::AccountCatalogFull);
        }
        let parent: &Path = self
            .path
            .parent()
            .ok_or(HostControlError::AccountCatalogInvalid)?;
        fs::create_dir_all(parent)?;
        let tmp = temp_path(&self.path);
        let mut writer = File::create(&tmp)?;
        writer.write_all(&content)?;
        writer.sync_all()?;
        drop(writer);
        atomic_replace(&tmp, &self.path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_protocol::AccountCatalogEnvironment;

    fn entry(id: &str, uid: char) -> AccountCatalogEntry {
        AccountCatalogEntry {
            schema: ACCOUNT_CATALOG_ENTRY_SCHEMA_V1.to_owned(),
            venue: TradingVenue::Okx,
            account_id: id.to_owned(),
            label: format!("Readonly {id}"),
            environment: AccountCatalogEnvironment::Production,
            expected_uid_fingerprint: uid.to_string().repeat(64),
        }
    }

    fn at(name: &str) -> AccountCatalogStore {
        AccountCatalogStore::at(std::env::temp_dir().join(format!(
            "okx-account-catalog-{name}-{}.json",
            std::process::id()
        )))
    }

    #[test]
    fn named_registration_is_safe_idempotent_and_never_selectable() {
        let store = at("same");
        let _ = fs::remove_file(&store.path);
        let registered = store.register(entry("sub-a", 'a')).unwrap();
        assert_eq!(registered["status"], "REGISTERED_METADATA_ONLY");
        assert_eq!(
            store.register(entry("sub-a", 'a')).unwrap()["status"],
            "ALREADY_REGISTERED"
        );
        let status = store.status().unwrap();
        assert_eq!(status["entries_total"], 1);
        assert_eq!(status["entries"][0]["identity"], "UNVERIFIED");
        assert_eq!(status["entries"][0]["venue"], "okx");
        assert_eq!(
            status["entries"][0]["observer_github_environment"],
            "okx-sub-a-global-observer"
        );
        assert_eq!(status["entries"][0]["selectable"], false);
        assert_eq!(status["entries"][0]["trading_authorized"], false);
        assert!(matches!(
            store.register(entry("sub-a", 'b')),
            Err(HostControlError::AccountCatalogConflict)
        ));
        assert!(matches!(
            store.register(entry("sub-b", 'a')),
            Err(HostControlError::AccountCatalogConflict)
        ));
        assert_eq!(store.status().unwrap()["entries_total"], 1);
        let _ = fs::remove_file(&store.path);
    }

    #[test]
    fn one_router_rejects_unimplemented_venues_and_unverified_accounts() {
        let store = at("routes");
        let _ = fs::remove_file(&store.path);
        assert_eq!(
            store.resolve(TradingVenue::Okx, "real").unwrap(),
            AccountRoute::Real
        );
        assert_eq!(
            store.resolve(TradingVenue::Okx, "demo").unwrap(),
            AccountRoute::Demo
        );
        assert!(store.resolve(TradingVenue::Bybit, "real").is_err());
        assert!(store.resolve(TradingVenue::Okx, "unknown").is_err());
        store.register(entry("sub-a", 'a')).unwrap();
        assert!(store.resolve(TradingVenue::Okx, "sub-a").is_err());
        let status = store.status().unwrap();
        assert_eq!(status["implemented_venue_adapters"][0], "okx");
        assert_eq!(status["selectable_account_ids"][0]["account_id"], "real");
        let _ = fs::remove_file(&store.path);
    }

    #[test]
    fn corrupted_catalog_fails_closed_and_is_not_reset_on_write() {
        let store = at("invalid");
        let _ = fs::remove_file(&store.path);
        fs::write(&store.path, b"{\"schema\":\"unexpected\",\"entries\":[]}").unwrap();
        assert!(matches!(
            store.status(),
            Err(HostControlError::AccountCatalogInvalid)
        ));
        assert!(matches!(
            store.register(entry("sub-a", 'a')),
            Err(HostControlError::AccountCatalogInvalid)
        ));
        assert!(
            String::from_utf8(fs::read(&store.path).unwrap())
                .unwrap()
                .contains("unexpected")
        );
        let _ = fs::remove_file(&store.path);
    }

    #[test]
    fn catalog_is_bounded_and_never_stores_secrets() {
        let store = at("bound");
        let _ = fs::remove_file(&store.path);
        for i in 0..MAX_CATALOG_ENTRIES {
            let uid = format!("{:064x}", i + 1);
            let mut item = entry(&format!("sub-{i}"), 'a');
            item.expected_uid_fingerprint = uid;
            store.register(item).unwrap();
        }
        assert!(matches!(
            store.register(entry("extra", 'f')),
            Err(HostControlError::AccountCatalogFull)
        ));
        assert_eq!(
            store.status().unwrap()["entries_total"],
            MAX_CATALOG_ENTRIES
        );
        // The wire budget is 16 KiB including the CONTROL result envelope.
        // Leave at least 1 KiB headroom in the maximum metadata payload.
        let result_bytes = serde_json::to_vec(&store.status().unwrap()).unwrap();
        assert!(result_bytes.len() <= 15 * 1024);
        let content = String::from_utf8(fs::read(&store.path).unwrap()).unwrap();
        assert!(!content.contains("api_key"));
        assert!(!content.contains("secret_key"));
        assert!(!content.contains("passphrase"));
        let _ = fs::remove_file(&store.path);
    }
}

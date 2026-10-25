//! Canonical typed owner for the installed account catalog.
//! Runtime uses one Protobuf snapshot; account finances and policy remain in SQL.
use crate::accounts::{AccountCatalog, AccountKey, Environment, VerifiedAccountConfig};
use prost::Message;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

pub const CATALOG_FILE: &str = "account-catalog.pb";
const VERSION: u32 = 1;
const MAX_BYTES: usize = 32 * 1024;
#[derive(Clone, PartialEq, Message)]
struct CatalogWire {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(string, tag = "2")]
    installation_id: String,
    #[prost(message, repeated, tag = "3")]
    accounts: Vec<AccountWire>,
}
#[derive(Clone, PartialEq, Message)]
struct AccountWire {
    #[prost(string, tag = "1")]
    venue: String,
    #[prost(uint32, tag = "2")]
    environment: u32,
    #[prost(string, tag = "3")]
    uid: String,
    #[prost(string, tag = "4")]
    alias: String,
    #[prost(string, tag = "5")]
    credential_ref: String,
    #[prost(string, tag = "6")]
    connect_operation_id: String,
    #[prost(string, tag = "7")]
    configuration_digest: String,
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "ACCOUNT_CATALOG_INVALID")
}
fn to_wire(a: &VerifiedAccountConfig) -> AccountWire {
    AccountWire {
        venue: a.account_key.venue.clone(),
        environment: match a.account_key.environment {
            Environment::Real => 1,
            Environment::Demo => 2,
        },
        uid: a.account_key.verified_uid.clone(),
        alias: a.alias.clone(),
        credential_ref: a.credential_ref.clone(),
        connect_operation_id: a.connect_operation_id.clone(),
        configuration_digest: a.configuration_digest.clone(),
    }
}
pub fn encode(c: &AccountCatalog) -> io::Result<Vec<u8>> {
    if !c.valid() {
        return Err(invalid());
    }
    let bytes = CatalogWire {
        version: VERSION,
        installation_id: c.installation_id.clone(),
        accounts: c.accounts.iter().map(to_wire).collect(),
    }
    .encode_to_vec();
    if bytes.len() > MAX_BYTES {
        return Err(invalid());
    }
    Ok(bytes)
}
pub fn decode(bytes: &[u8]) -> io::Result<AccountCatalog> {
    if bytes.is_empty() || bytes.len() > MAX_BYTES {
        return Err(invalid());
    }
    let w = CatalogWire::decode(bytes).map_err(|_| invalid())?;
    if w.version != VERSION || w.encode_to_vec() != bytes {
        return Err(invalid());
    }
    let accounts = w
        .accounts
        .into_iter()
        .map(|a| {
            let environment = match a.environment {
                1 => Environment::Real,
                2 => Environment::Demo,
                _ => return Err(invalid()),
            };
            Ok(VerifiedAccountConfig {
                account_key: AccountKey {
                    venue: a.venue,
                    environment,
                    verified_uid: a.uid,
                },
                alias: a.alias,
                credential_ref: a.credential_ref,
                connect_operation_id: a.connect_operation_id,
                configuration_digest: a.configuration_digest,
            })
        })
        .collect::<io::Result<Vec<_>>>()?;
    let c = AccountCatalog {
        schema: AccountCatalog::SCHEMA.into(),
        installation_id: w.installation_id,
        accounts,
    };
    if !c.valid() {
        return Err(invalid());
    }
    Ok(c)
}
pub fn load(root: &Path) -> io::Result<AccountCatalog> {
    decode(&fs::read(root.join(CATALOG_FILE))?)
}
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);
fn stage(root: &Path, bytes: &[u8]) -> io::Result<PathBuf> {
    fs::create_dir_all(root)?;
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp = root.join(format!(".catalog-{}-{counter}.tmp", std::process::id()));
    let mut f = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(tmp)
}
pub fn initialize(root: &Path, installation_id: &str) -> io::Result<()> {
    let c = AccountCatalog {
        schema: AccountCatalog::SCHEMA.into(),
        installation_id: installation_id.into(),
        accounts: vec![],
    };
    let bytes = encode(&c)?;
    if root.join(CATALOG_FILE).exists() || root.join("installation.json").exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "INSTALLATION_ALREADY_EXISTS",
        ));
    }
    let tmp = stage(root, &bytes)?;
    let result = fs::hard_link(&tmp, root.join(CATALOG_FILE));
    let _ = fs::remove_file(tmp);
    result
}
pub fn append(root: &Path, config: &VerifiedAccountConfig) -> io::Result<()> {
    let mut c = load(root)?;
    c.accounts.push(config.clone());
    let tmp = stage(root, &encode(&c)?)?;
    // Engine serializes writers. Same-volume rename replaces one canonical snapshot.
    let result = fs::rename(&tmp, root.join(CATALOG_FILE));
    if result.is_err() {
        let _ = fs::remove_file(tmp);
    }
    result
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyInstallation {
    schema: String,
    installation_id: String,
}
/// One-time identity cutover under the Engine lock. Never used if Protobuf
/// exists (including a corrupt one). No finance DB, campaigns or SQL is reset.
pub fn promote_existing_once(
    root: &Path,
    verify_account_sql: impl FnOnce(&AccountCatalog) -> io::Result<()>,
) -> io::Result<AccountCatalog> {
    if root.join(CATALOG_FILE).exists() {
        return load(root);
    }
    let old = root.join("installation.json");
    let bytes = fs::read(&old)?;
    if bytes.len() > 8192 {
        return Err(invalid());
    }
    let installation: LegacyInstallation = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if installation.schema != "okx.installation/v1" {
        return Err(invalid());
    }
    let dir = root.join("account-catalog");
    let mut accounts = vec![];
    for row in fs::read_dir(&dir)? {
        let row = row?;
        if row.path().extension().and_then(|s| s.to_str()) != Some("json") {
            return Err(invalid());
        }
        let bytes = fs::read(row.path())?;
        if bytes.len() > 8192 {
            return Err(invalid());
        }
        let cfg: VerifiedAccountConfig = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        let id = format!("{:x}", Sha256::digest(cfg.account_key.wire().as_bytes()));
        if row.file_name() != std::ffi::OsStr::new(&format!("{id}.json")) {
            return Err(invalid());
        }
        if !root
            .join("accounts")
            .join(&id)
            .join("account.sqlite")
            .is_file()
        {
            return Err(invalid());
        }
        accounts.push(cfg);
    }
    accounts.sort_by(|a, b| a.account_key.cmp(&b.account_key));
    let expected = AccountCatalog {
        schema: AccountCatalog::SCHEMA.into(),
        installation_id: installation.installation_id,
        accounts,
    };
    // Validate each account's actual SQL identity before publishing one
    // canonical catalog; a matching filename alone is insufficient.
    verify_account_sql(&expected)?;
    let tmp = stage(root, &encode(&expected)?)?;
    let publish = fs::hard_link(&tmp, root.join(CATALOG_FILE));
    let _ = fs::remove_file(tmp);
    if let Err(e) = publish
        && e.kind() != io::ErrorKind::AlreadyExists
    {
        return Err(e);
    }
    let loaded = load(root)?;
    if loaded != expected {
        return Err(invalid());
    }
    // Once published, all readers use only Protobuf; legacy copies are retired.
    for row in fs::read_dir(&dir)? {
        fs::remove_file(row?.path())?;
    }
    fs::remove_dir(&dir)?;
    fs::remove_file(old)?;
    Ok(loaded)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn sample() -> AccountCatalog {
        AccountCatalog {
            schema: AccountCatalog::SCHEMA.into(),
            installation_id: "installation-test".into(),
            accounts: vec![VerifiedAccountConfig {
                account_key: AccountKey {
                    venue: "okx".into(),
                    environment: Environment::Real,
                    verified_uid: "a".repeat(64),
                },
                alias: "account_one".into(),
                credential_ref: "credential_one".into(),
                connect_operation_id: "connect_1234".into(),
                configuration_digest: "b".repeat(64),
            }],
        }
    }
    #[test]
    fn wire_roundtrip_rejects_corruption_and_duplicate() {
        let c = sample();
        let data = encode(&c).unwrap();
        assert_eq!(decode(&data).unwrap(), c);
        let mut extra = data.clone();
        extra.extend([32, 1]);
        assert!(decode(&extra).is_err());
        assert!(decode(br#"{"installation_id":"installation-test"}"#).is_err());
        let mut dup = c.clone();
        dup.accounts.push(c.accounts[0].clone());
        assert!(encode(&dup).is_err());
    }
    #[test]
    fn one_time_cutover_is_verified_atomic_and_retains_sql() {
        let root = std::env::temp_dir().join(format!("okx-catalog-cutover-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let catalog = sample();
        let config = &catalog.accounts[0];
        let key_hash = format!("{:x}", Sha256::digest(config.account_key.wire().as_bytes()));
        let account_dir = root.join("accounts").join(&key_hash);
        let old_dir = root.join("account-catalog");
        fs::create_dir_all(&account_dir).unwrap();
        fs::create_dir_all(&old_dir).unwrap();
        let db = account_dir.join("account.sqlite");
        fs::write(&db, b"SQL-SENTINEL-NOT-REWRITTEN").unwrap();
        fs::write(
            root.join("installation.json"),
            r#"{"schema":"okx.installation/v1","installation_id":"installation-test"}"#,
        )
        .unwrap();
        fs::write(
            old_dir.join(format!("{key_hash}.json")),
            serde_json::to_vec(config).unwrap(),
        )
        .unwrap();
        // A failed SQL-identity validation cannot publish new canonical state.
        assert!(promote_existing_once(&root, |_| Err(invalid())).is_err());
        assert!(!root.join(CATALOG_FILE).exists());
        assert!(root.join("installation.json").exists());
        let promoted = promote_existing_once(&root, |c| {
            assert_eq!(c, &catalog);
            Ok(())
        })
        .unwrap();
        assert_eq!(promoted, catalog);
        assert_eq!(load(&root).unwrap(), catalog);
        assert_eq!(fs::read(db).unwrap(), b"SQL-SENTINEL-NOT-REWRITTEN");
        assert!(!root.join("installation.json").exists());
        assert!(!old_dir.exists());
        // Canonical protobuf must win; no second legacy read or conversion.
        assert_eq!(
            promote_existing_once(&root, |_| Err(invalid())).unwrap(),
            catalog
        );
        fs::remove_dir_all(&root).unwrap();
    }
    #[test]
    fn catalog_publication_preserves_identity_and_fails_closed() {
        let root = std::env::temp_dir().join(format!("okx-catalog-proto-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        initialize(&root, "installation-test").unwrap();
        assert!(initialize(&root, "installation-other").is_err());
        let c = sample();
        append(&root, &c.accounts[0]).unwrap();
        assert_eq!(load(&root).unwrap(), c);
        assert!(append(&root, &c.accounts[0]).is_err());
        fs::write(root.join(CATALOG_FILE), b"corrupted").unwrap();
        assert!(promote_existing_once(&root, |_| Ok(())).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}

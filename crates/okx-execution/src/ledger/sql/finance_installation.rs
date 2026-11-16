//! Explicit creation and read-only reopening of the single native financial file.
//! Empty local storage never proves an empty venue account or grants a POST.
use crate::{AccountStore, StoreError, StoreResult};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

const DOMAIN_SCHEMA: &str = include_str!("../../../schema/finance-unified-v1.sql");
// SQLite's standard header field identifies this format BEFORE SQLite opens
// an existing file/WAL. It is not identity, completeness or trade authority.
pub(crate) const FINANCE_APPLICATION_ID: i64 = 0x4f4b5846;
const INSTALLATION_SCHEMA: &str = "
DROP TABLE finance_import_manifest;
CREATE TABLE finance_installation(
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 installation_id TEXT NOT NULL CHECK(length(installation_id) BETWEEN 1 AND 128),
 initialize_request_id TEXT NOT NULL CHECK(length(initialize_request_id) BETWEEN 8 AND 128),
 request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64),
 source_tree TEXT NOT NULL CHECK(length(source_tree)=40),
 created_at_ms INTEGER NOT NULL CHECK(created_at_ms>0),
 schema_version INTEGER NOT NULL CHECK(schema_version=8)
) STRICT;
CREATE TRIGGER immutable_finance_installation_update BEFORE UPDATE ON finance_installation
 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_FINANCE_INSTALLATION'); END;
CREATE TRIGGER immutable_finance_installation_delete BEFORE DELETE ON finance_installation
 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_FINANCE_INSTALLATION'); END;
CREATE TRIGGER immutable_entry_fee_owner_update BEFORE UPDATE ON operation_receipts
 WHEN OLD.kind IN ('ENTRY_ORDER_FEE_PROVEN','ENTRY_ORDER_FEE_INDEX')
 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_ENTRY_FEE_OWNER'); END;
CREATE TRIGGER immutable_entry_fee_owner_delete BEFORE DELETE ON operation_receipts
 WHEN OLD.kind IN ('ENTRY_ORDER_FEE_PROVEN','ENTRY_ORDER_FEE_INDEX')
 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_ENTRY_FEE_OWNER'); END;
CREATE TRIGGER immutable_entry_late_update BEFORE UPDATE ON operation_receipts
 WHEN OLD.kind IN ('ENTRY_LATE_SETTLEMENT_PROVEN','ENTRY_LATE_SETTLED')
 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_LATE_ENTRY'); END;
CREATE TRIGGER immutable_entry_late_delete BEFORE DELETE ON operation_receipts
 WHEN OLD.kind IN ('ENTRY_LATE_SETTLEMENT_PROVEN','ENTRY_LATE_SETTLED')
 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_LATE_ENTRY'); END;
PRAGMA user_version=8;
";
// Exact canonical tables, columns, constraints, indexes and triggers. Whitespace
// is normalized for LF/CRLF portability; changes require an intentional version.
const EXPECTED_SCHEMA_SHA256: &str =
    "dab72aee8c7d48c7f6372ddcfcde717cf610224f33ee6226ffbb881dd8e8ef06";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewFinanceInstallation {
    pub installation_id: String,
    pub initialize_request_id: String,
    pub source_tree: String,
}
impl NewFinanceInstallation {
    pub(crate) fn validate(&self) -> StoreResult<()> {
        let token = |value: &str, min| {
            (min..=128).contains(&value.len())
                && value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        };
        if !token(&self.installation_id, 1)
            || !token(&self.initialize_request_id, 8)
            || self.source_tree.len() != 40
            || !self
                .source_tree
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(StoreError::Invalid(
                "FINANCE_INITIALIZATION_REQUEST_INVALID",
            ));
        }
        Ok(())
    }
    pub(crate) fn request_sha256(&self) -> StoreResult<String> {
        self.validate()?;
        let bytes = serde_json::to_vec(&(
            "okx.finance-installation/v1",
            &self.installation_id,
            &self.initialize_request_id,
            &self.source_tree,
        ))
        .map_err(|_| StoreError::Invalid("FINANCE_INITIALIZATION_ENCODING_INVALID"))?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FinanceInstallationReceipt {
    pub installation_id: String,
    pub initialize_request_id: String,
    pub request_sha256: String,
    pub source_tree: String,
    pub created_at_ms: i64,
    pub schema_version: u32,
}

fn schema_digest(db: &Connection) -> StoreResult<String> {
    let mut digest = Sha256::new();
    let mut stmt = db.prepare(
        "SELECT type,name,tbl_name,sql FROM sqlite_master
        WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
        ))
    })?;
    for row in rows {
        let (kind, name, table, sql) = row?;
        let normalized = sql.split_whitespace().collect::<Vec<_>>().join(" ");
        for value in [&kind, &name, &table, &normalized] {
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value.as_bytes());
        }
    }
    Ok(format!("{:x}", digest.finalize()))
}
fn durability(db: &Connection) -> StoreResult<()> {
    let journal: String = db.pragma_query_value(None, "journal_mode", |r| r.get(0))?;
    let sync: i64 = db.pragma_query_value(None, "synchronous", |r| r.get(0))?;
    let fk: i64 = db.pragma_query_value(None, "foreign_keys", |r| r.get(0))?;
    if journal != "wal" || sync != 2 || fk != 1 {
        return Err(StoreError::Invalid("FINANCE_SQLITE_DURABILITY_INVALID"));
    }
    Ok(())
}
impl AccountStore {
    /// Caller already holds the sole SQL writer and cross-process lease.
    /// No initial account, campaign, balance, policy or credentials are inferred.
    pub(crate) fn initialize_finance_installation(
        db: &mut Connection,
        request: &NewFinanceInstallation,
    ) -> StoreResult<FinanceInstallationReceipt> {
        request.validate()?;
        durability(db)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let objects: i64 = tx.query_row(
            "SELECT COUNT(*) FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%'",
            [],
            |r| r.get(0),
        )?;
        let version: i64 = tx.pragma_query_value(None, "user_version", |r| r.get(0))?;
        if objects != 0 || version != 0 {
            return Err(StoreError::Invalid(
                "FINANCE_INITIALIZATION_TARGET_NOT_EMPTY",
            ));
        }
        tx.execute_batch(DOMAIN_SCHEMA)?;
        // The canonical domain DDL is shared with source-contract tests, but
        // a new installation contains no import manifest or legacy authority.
        tx.execute_batch(INSTALLATION_SCHEMA)?;
        tx.pragma_update(None, "application_id", FINANCE_APPLICATION_ID)?;
        let created_at_ms = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| StoreError::Invalid("FINANCE_INITIALIZATION_CLOCK_INVALID"))?
                .as_millis(),
        )
        .map_err(|_| StoreError::Invalid("FINANCE_INITIALIZATION_CLOCK_INVALID"))?;
        tx.execute(
            "INSERT INTO finance_installation VALUES(1,?1,?2,?3,?4,?5,8)",
            params![
                request.installation_id,
                request.initialize_request_id,
                request.request_sha256()?,
                request.source_tree,
                created_at_ms
            ],
        )?;
        if schema_digest(&tx)? != EXPECTED_SCHEMA_SHA256 {
            return Err(StoreError::Invalid("FINANCE_INSTALLATION_SCHEMA_MISMATCH"));
        }
        tx.commit()?;
        Self::inspect_finance_installation(db, &request.installation_id)
    }

    /// Current schema and economic invariants, not equality with initial rows.
    /// A valid creation receipt itself does NOT prove account/trading readiness.
    pub(crate) fn inspect_finance_installation(
        db: &Connection,
        installation: &str,
    ) -> StoreResult<FinanceInstallationReceipt> {
        durability(db)?;
        let tx = db.unchecked_transaction()?;
        let version: i64 = tx.pragma_query_value(None, "user_version", |r| r.get(0))?;
        let application_id: i64 = tx.pragma_query_value(None, "application_id", |r| r.get(0))?;
        let count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            [],
            |r| r.get(0),
        )?;
        if application_id != FINANCE_APPLICATION_ID
            || version != 8
            || count != 48
            || schema_digest(&tx)? != EXPECTED_SCHEMA_SHA256
        {
            return Err(StoreError::Invalid("FINANCE_INSTALLATION_SCHEMA_MISMATCH"));
        }
        let receipt = tx
            .query_row(
                "SELECT installation_id,initialize_request_id,request_sha256,
            source_tree,created_at_ms,schema_version FROM finance_installation WHERE singleton=1",
                [],
                |r| {
                    Ok(FinanceInstallationReceipt {
                        installation_id: r.get(0)?,
                        initialize_request_id: r.get(1)?,
                        request_sha256: r.get(2)?,
                        source_tree: r.get(3)?,
                        created_at_ms: r.get(4)?,
                        schema_version: r.get(5)?,
                    })
                },
            )
            .optional()?
            .ok_or(StoreError::Invalid("FINANCE_INSTALLATION_RECEIPT_MISSING"))?;
        let original = NewFinanceInstallation {
            installation_id: receipt.installation_id.clone(),
            initialize_request_id: receipt.initialize_request_id.clone(),
            source_tree: receipt.source_tree.clone(),
        };
        if receipt.installation_id != installation
            || receipt.schema_version != 8
            || receipt.created_at_ms <= 0
            || original.request_sha256()? != receipt.request_sha256
        {
            return Err(StoreError::Invalid(
                "FINANCE_INSTALLATION_IDENTITY_MISMATCH",
            ));
        }
        let names: Vec<String> = tx
            .prepare(
                "SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            )?
            .query_map([], |r| r.get(0))?
            .collect::<Result<_, _>>()?;
        for table in names {
            // The exact schema fingerprint above makes identifiers trusted DDL.
            let foreign: i64 = tx.query_row(
                &format!(
                    "SELECT COUNT(*) FROM \"{table}\"
                WHERE installation_id <> ?1"
                ),
                [installation],
                |r| r.get(0),
            )?;
            if foreign != 0 {
                return Err(StoreError::Invalid("FINANCE_INSTALLATION_FOREIGN_SCOPE"));
            }
        }
        let integrity: String = tx.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
        if integrity != "ok" {
            return Err(StoreError::Invalid("FINANCE_INSTALLATION_INTEGRITY_FAILED"));
        }
        if tx
            .prepare("PRAGMA foreign_key_check")?
            .query([])?
            .next()?
            .is_some()
        {
            return Err(StoreError::Invalid(
                "FINANCE_INSTALLATION_FOREIGN_KEY_FAILED",
            ));
        }
        let campaigns: Vec<String> = tx
            .prepare("SELECT campaign_id FROM risk_meta WHERE installation_id=?1")?
            .query_map([installation], |r| r.get(0))?
            .collect::<Result<_, _>>()?;
        for campaign in campaigns {
            crate::risk::verify_unified_import_campaign(&tx, installation, &campaign)
                .map_err(StoreError::Invalid)?;
        }
        tx.commit()?;
        Ok(receipt)
    }
}

#[cfg(test)]
#[path = "finance_installation_tests.rs"]
mod tests;

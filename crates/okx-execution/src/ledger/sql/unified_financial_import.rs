//! #510 r4: PRIVATE offline, lossless source rows into the one finance SQLite.
//! This moves the ORIGINAL 39 AccountStore tables, not a shadow fee/ledger
//! projection. Only an authorized stopped/fenced legacy owner may supply the
//! immutable source snapshots. No production import or trade entrypoint uses
//! this helper until the campaign import and cross-host authority cutover.
use crate::{StoreError, StoreResult};
use rusqlite::{
    Connection, OptionalExtension, Transaction, TransactionBehavior, params, params_from_iter,
    types::Value,
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, path::Path};

/// Same 29 canonical account-v2 tables plus 10 source-owned derived tables.
/// Compare against the physical old source on EVERY import: new unlisted
/// obligations must fail closed instead of being lost during normalization.
const ORIGINAL_ACCOUNT_TABLES: [&str; 39] = [
    "account_meta",
    "strategy_definition_versions",
    "strategy_instances",
    "decisions",
    "intents",
    "intent_plans",
    "protection_receipts",
    "orders",
    "fills",
    "fill_projection_pending",
    "funding_projection_pending",
    "bills",
    "cash_flows",
    "allocations",
    "position_lots",
    "finance_entries",
    "finance_totals",
    "instance_finance_totals",
    "finance_source_links",
    "nav_points",
    "nav_watermarks",
    "nav_scopes",
    "daily_losses",
    "risk_stops",
    "source_cursors",
    "risk_receipts",
    "operation_receipts",
    "account_policy",
    "storage_recovery",
    "campaign_source_epoch",
    "campaign_entry_readiness",
    "campaign_late_head_epoch",
    "campaign_receipt_events",
    "campaign_receipt_cursors",
    "campaign_entry_applied",
    "campaign_entry_totals",
    "campaign_entry_window_proofs",
    "bill_order_sources",
    "swap_fill_sources",
];

const ORIGINAL_CAMPAIGN_TABLES: [&str; 8] = [
    "risk_meta",
    "risk_account_occupied",
    "risk_policy_account",
    "risk_loss_source",
    "risk_policy_strategy",
    "risk_strategy_occupied",
    "risk_entries",
    "risk_no_reserve",
];

/// Expected source IDs from an OUT-OF-BAND inventory. These names do not
/// authenticate themselves or constitute distributed owner fencing.
#[derive(Debug)]
pub(in crate::account_store) struct ExpectedFinanceInventory<'a> {
    pub installation: &'a str,
    pub accounts: &'a [&'a str],
    pub campaigns: &'a [&'a str],
}

#[derive(Debug, PartialEq, Eq)]
pub(in crate::account_store) struct OriginalFinanceImport {
    pub accounts: usize,
    pub campaigns: usize,
    pub tables_per_account: usize,
    pub tables_per_campaign: usize,
    pub account_rows: u64,
    pub campaign_rows: u64,
    pub inventory_sha256: String,
    pub content_sha256: String,
}

#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
pub(in crate::account_store) struct OriginalAccountImport {
    pub accounts: usize,
    pub tables_per_account: usize,
    pub imported_rows: u64,
}

fn inventory_ids(ids: &[&str]) -> StoreResult<BTreeSet<String>> {
    if ids.is_empty() || ids.iter().any(|id| id.is_empty()) {
        return Err(StoreError::Invalid("UNIFIED_IMPORT_INVENTORY_EMPTY"));
    }
    let unique: BTreeSet<String> = ids.iter().map(|id| (*id).to_owned()).collect();
    if unique.len() != ids.len() {
        return Err(StoreError::Invalid("UNIFIED_IMPORT_INVENTORY_DUPLICATE"));
    }
    Ok(unique)
}

fn frame(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
}

fn hash_inventory(
    installation: &str,
    accounts: &BTreeSet<String>,
    campaigns: &BTreeSet<String>,
) -> String {
    let mut hash = Sha256::new();
    frame(&mut hash, b"okx-finance-source-inventory-r4-v1");
    frame(&mut hash, installation.as_bytes());
    for (kind, names) in [(b'A', accounts), (b'C', campaigns)] {
        hash.update([kind]);
        hash.update((names.len() as u64).to_be_bytes());
        for name in names {
            frame(&mut hash, name.as_bytes());
        }
    }
    format!("{:x}", hash.finalize())
}

fn hash_value(hash: &mut Sha256, value: Value) {
    match value {
        Value::Null => hash.update([0]),
        Value::Integer(v) => {
            hash.update([1]);
            hash.update(v.to_be_bytes());
        }
        Value::Real(v) => {
            hash.update([2]);
            hash.update(v.to_bits().to_be_bytes());
        }
        Value::Text(v) => {
            hash.update([3]);
            frame(hash, v.as_bytes());
        }
        Value::Blob(v) => {
            hash.update([4]);
            frame(hash, &v);
        }
    }
}

/// Canonical logical-row hash across ALL original target tables, each scope
/// and every SQLite value (NULL, integer, REAL, text or blob). Does not
/// hash physical SQLite pages or WAL checkpoints and remains stable on reopen.
/// Uses the 47 ORIGINAL tables; the extra offline marker never hashes itself.
fn target_content_digest(
    db: &Connection,
    installation: &str,
    accounts: &BTreeSet<String>,
    campaigns: &BTreeSet<String>,
) -> StoreResult<String> {
    let mut hash = Sha256::new();
    frame(&mut hash, b"okx-finance-content-r4-v1");
    frame(&mut hash, installation.as_bytes());
    for (kind, names, scope_column, tables) in [
        (b'A', accounts, "account_key", &ORIGINAL_ACCOUNT_TABLES[..]),
        (
            b'C',
            campaigns,
            "campaign_id",
            &ORIGINAL_CAMPAIGN_TABLES[..],
        ),
    ] {
        for name in names {
            hash.update([kind]);
            frame(&mut hash, name.as_bytes());
            for table in tables {
                frame(&mut hash, table.as_bytes());
                let cols = columns(db, table)?;
                hash.update((cols.len() as u64).to_be_bytes());
                for col in &cols {
                    frame(&mut hash, col.as_bytes());
                }
                let projected = cols.iter().map(|c| quote(c)).collect::<Vec<_>>().join(",");
                // Order by the COMPLETE row, rather than rowid, to ignore
                // SQLite page allocation and retain duplicate rows.
                let query = format!(
                    "SELECT {projected} FROM {} WHERE installation_id=?1 AND {}=?2
                     ORDER BY {projected}",
                    quote(table),
                    quote(scope_column)
                );
                let mut stmt = db.prepare(&query)?;
                let mut rows = stmt.query(params![installation, name])?;
                let mut row_count = 0_u64;
                while let Some(row) = rows.next()? {
                    hash.update([0x52]);
                    for index in 0..cols.len() {
                        hash_value(&mut hash, row.get::<_, Value>(index)?);
                    }
                    row_count = row_count
                        .checked_add(1)
                        .ok_or(StoreError::Invalid("UNIFIED_IMPORT_ROW_COUNT_OVERFLOW"))?;
                }
                hash.update(row_count.to_be_bytes());
            }
        }
    }
    Ok(format!("{:x}", hash.finalize()))
}

/// Includes every original account/campaign table, and refuses extra
/// unrecognized installed scopes or extra target rows in other installations.
fn target_row_totals(
    db: &Connection,
    installation: &str,
    accounts: &BTreeSet<String>,
    campaigns: &BTreeSet<String>,
) -> StoreResult<(u64, u64)> {
    let mut totals = [0_u64, 0_u64];
    for (index, names, scope, tables) in [
        (
            0_usize,
            accounts,
            "account_key",
            &ORIGINAL_ACCOUNT_TABLES[..],
        ),
        (
            1_usize,
            campaigns,
            "campaign_id",
            &ORIGINAL_CAMPAIGN_TABLES[..],
        ),
    ] {
        for &table in tables {
            let total: i64 =
                db.query_row(&format!("SELECT COUNT(*) FROM {}", quote(table)), [], |r| {
                    r.get(0)
                })?;
            let scoped: i64 = db.query_row(
                &format!(
                    "SELECT COUNT(*) FROM {} WHERE installation_id=?1",
                    quote(table)
                ),
                [installation],
                |r| r.get(0),
            )?;
            if total != scoped {
                return Err(StoreError::Invalid("UNIFIED_IMPORT_FOREIGN_INSTALLATION"));
            }
            let mut names_total = 0_i64;
            for name in names {
                let count: i64 = db.query_row(
                    &format!(
                        "SELECT COUNT(*) FROM {} WHERE installation_id=?1 AND {}=?2",
                        quote(table),
                        quote(scope)
                    ),
                    params![installation, name],
                    |r| r.get(0),
                )?;
                names_total = names_total
                    .checked_add(count)
                    .ok_or(StoreError::Invalid("UNIFIED_IMPORT_ROW_COUNT_OVERFLOW"))?;
            }
            if total != names_total {
                return Err(StoreError::Invalid("UNIFIED_IMPORT_EXTRA_SCOPE"));
            }
            totals[index] = totals[index]
                .checked_add(
                    u64::try_from(total)
                        .map_err(|_| StoreError::Invalid("UNIFIED_IMPORT_ROW_COUNT_OVERFLOW"))?,
                )
                .ok_or(StoreError::Invalid("UNIFIED_IMPORT_ROW_COUNT_OVERFLOW"))?;
        }
    }
    Ok((totals[0], totals[1]))
}

fn columns(db: &Connection, name: &str) -> StoreResult<Vec<String>> {
    let mut statement = db.prepare(&format!("PRAGMA table_info(\"{name}\")"))?;
    let result = statement
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if result.is_empty() {
        return Err(StoreError::Invalid("UNIFIED_LEGACY_TABLE_MISSING"));
    }
    Ok(result)
}
fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
fn count_scoped(
    tx: &Transaction<'_>,
    table: &str,
    installation: &str,
    scope_column: &str,
    scope: &str,
) -> StoreResult<i64> {
    Ok(tx.query_row(
        &format!(
            "SELECT COUNT(*) FROM {} WHERE installation_id=?1 AND {}=?2",
            quote(table),
            quote(scope_column)
        ),
        params![installation, scope],
        |r| r.get(0),
    )?)
}
fn verify_original_source(source: &Connection, installation: &str) -> StoreResult<String> {
    let version: i64 = source.pragma_query_value(None, "user_version", |r| r.get(0))?;
    if version != 2 {
        return Err(StoreError::Invalid("UNIFIED_LEGACY_VERSION_INVALID"));
    }
    let check: String = source.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    if check != "ok" {
        return Err(StoreError::Invalid("UNIFIED_LEGACY_INTEGRITY_FAILED"));
    }
    let mut fk = source.prepare("PRAGMA foreign_key_check")?;
    if fk.query([])?.next()?.is_some() {
        return Err(StoreError::Invalid("UNIFIED_LEGACY_FOREIGN_KEY_FAILED"));
    }
    let names: BTreeSet<String> = {
        let mut stmt = source.prepare(
            "SELECT name FROM sqlite_master
             WHERE type='table' AND name NOT LIKE 'sqlite_%'",
        )?;
        stmt.query_map([], |r| r.get(0))?
            .collect::<Result<BTreeSet<_>, _>>()?
    };
    if names
        != ORIGINAL_ACCOUNT_TABLES
            .into_iter()
            .map(str::to_owned)
            .collect()
    {
        return Err(StoreError::Invalid("UNIFIED_LEGACY_TABLE_CENSUS_CHANGED"));
    }
    let (account, owned_installation): (String, String) = source.query_row(
        "SELECT account_key,installation_id FROM account_meta WHERE singleton=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if account.is_empty() || owned_installation != installation {
        return Err(StoreError::Invalid("UNIFIED_LEGACY_SOURCE_IDENTITY"));
    }
    Ok(account)
}

fn verify_original_campaign(source: &Connection, installation: &str) -> StoreResult<String> {
    let version: i64 = source.pragma_query_value(None, "user_version", |r| r.get(0))?;
    if version != 5 {
        return Err(StoreError::Invalid(
            "UNIFIED_LEGACY_CAMPAIGN_VERSION_INVALID",
        ));
    }
    let check: String = source.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    if check != "ok" {
        return Err(StoreError::Invalid(
            "UNIFIED_LEGACY_CAMPAIGN_INTEGRITY_FAILED",
        ));
    }
    let mut fk = source.prepare("PRAGMA foreign_key_check")?;
    if fk.query([])?.next()?.is_some() {
        return Err(StoreError::Invalid(
            "UNIFIED_LEGACY_CAMPAIGN_FOREIGN_KEY_FAILED",
        ));
    }
    let names: BTreeSet<String> = {
        let mut stmt = source.prepare(
            "SELECT name FROM sqlite_master
             WHERE type='table' AND name NOT LIKE 'sqlite_%'",
        )?;
        stmt.query_map([], |r| r.get(0))?
            .collect::<Result<BTreeSet<_>, _>>()?
    };
    if names
        != ORIGINAL_CAMPAIGN_TABLES
            .into_iter()
            .map(str::to_owned)
            .collect()
    {
        return Err(StoreError::Invalid(
            "UNIFIED_LEGACY_CAMPAIGN_TABLE_CENSUS_CHANGED",
        ));
    }
    let (campaign, owned_installation, fenced): (String, String, i64) = source.query_row(
        "SELECT campaign_id,installation_id,legacy_fenced FROM risk_meta WHERE singleton=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    if campaign.is_empty() || owned_installation != installation || fenced != 1 {
        return Err(StoreError::Invalid(
            "UNIFIED_LEGACY_CAMPAIGN_IDENTITY_OR_FENCE",
        ));
    }
    // Reuse the ORIGINAL RiskBook invariant checker, not duplicate budget
    // arithmetic in the migration layer or trust mutable cached totals.
    crate::risk::verify_legacy_import_source(source).map_err(StoreError::Invalid)?;
    // A historical SQLite flag is NOT server-side Cloudflare authority revocation.
    // The authorized cutover must independently prove that remote mutation is fenced.
    Ok(campaign)
}

/// Copy exact original SQLite column values, adding only the missing
/// installation/account or installation/campaign namespace columns.
/// SQL foreign keys, immutable monetary values and legacy IDs are preserved.
fn copy_one_scope(
    source: &Connection,
    tx: &Transaction<'_>,
    installation: &str,
    scope_column: &str,
    scope: &str,
    tables: &[&str],
) -> StoreResult<u64> {
    let mut imported = 0u64;
    for &table in tables {
        let source_cols = columns(source, table)?;
        let mut target_cols = source_cols.clone();
        if !target_cols.iter().any(|c| c == "installation_id") {
            target_cols.insert(0, "installation_id".into());
        }
        if !target_cols.iter().any(|c| c == scope_column) {
            target_cols.insert(1, scope_column.into());
        }
        let expected_target = columns(tx, table)?;
        if target_cols.len() != expected_target.len()
            || !expected_target.iter().all(|c| target_cols.contains(c))
        {
            return Err(StoreError::Invalid("UNIFIED_LEGACY_SCHEMA_NOT_LOSSLESS"));
        }
        let query = format!(
            "SELECT {} FROM {}",
            source_cols
                .iter()
                .map(|c| quote(c))
                .collect::<Vec<_>>()
                .join(","),
            quote(table),
        );
        let write = format!(
            "INSERT INTO {}({}) VALUES({})",
            quote(table),
            target_cols
                .iter()
                .map(|c| quote(c))
                .collect::<Vec<_>>()
                .join(","),
            (0..target_cols.len())
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(","),
        );
        let mut reader = source.prepare(&query)?;
        let mut rows = reader.query([])?;
        let mut statement = tx.prepare(&write)?;
        let mut n = 0i64;
        while let Some(row) = rows.next()? {
            let native_values = (0..source_cols.len())
                .map(|i| row.get::<_, Value>(i))
                .collect::<Result<Vec<_>, _>>()?;
            for (column, value) in source_cols.iter().zip(&native_values) {
                if column == "installation_id" && value != &Value::Text(installation.to_owned()) {
                    return Err(StoreError::Invalid(
                        "UNIFIED_LEGACY_ROW_INSTALLATION_MISMATCH",
                    ));
                }
                if column == scope_column && value != &Value::Text(scope.to_owned()) {
                    return Err(StoreError::Invalid("UNIFIED_LEGACY_ROW_SCOPE_MISMATCH"));
                }
            }
            let mut values = Vec::with_capacity(target_cols.len());
            for col in &target_cols {
                if let Some(i) = source_cols.iter().position(|c| c == col) {
                    values.push(native_values[i].clone());
                } else if col == "installation_id" {
                    values.push(Value::Text(installation.to_owned()));
                } else if col == scope_column {
                    values.push(Value::Text(scope.to_owned()));
                } else {
                    return Err(StoreError::Invalid("UNIFIED_LEGACY_COLUMN_UNMAPPED"));
                }
            }
            if statement.execute(params_from_iter(values.iter()))? != 1 {
                return Err(StoreError::Invalid("UNIFIED_LEGACY_ROW_NOT_IMPORTED"));
            }
            n += 1;
        }
        let actual: i64 =
            source.query_row(&format!("SELECT COUNT(*) FROM {}", quote(table)), [], |r| {
                r.get(0)
            })?;
        if n != actual || count_scoped(tx, table, installation, scope_column, scope)? != actual {
            return Err(StoreError::Invalid("UNIFIED_LEGACY_ROW_PARITY_FAILED"));
        }
        imported = imported
            .checked_add(
                u64::try_from(n)
                    .map_err(|_| StoreError::Invalid("UNIFIED_LEGACY_ROW_COUNT_OVERFLOW"))?,
            )
            .ok_or(StoreError::Invalid("UNIFIED_LEGACY_ROW_COUNT_OVERFLOW"))?;
    }
    Ok(imported)
}

/// PRIVATE: copies all source account and campaign rows under ONE
/// BEGIN IMMEDIATE/COMMIT. Caller must supply a complete authenticated
/// inventory of stopped accounts and campaigns; OS file leases are local
/// safety only, not proof of Cloudflare fencing or source completeness.
/// An empty campaign set is permitted ONLY for a historical account-only test.
fn import_sources(
    target: &mut Connection,
    installation: &str,
    accounts: &[&Connection],
    campaigns: &[&Connection],
    expected: Option<&ExpectedFinanceInventory<'_>>,
) -> StoreResult<OriginalFinanceImport> {
    if installation.is_empty() || accounts.is_empty() {
        return Err(StoreError::Invalid("UNIFIED_IMPORT_EMPTY_SCOPE"));
    }
    let journal: String = target.pragma_query_value(None, "journal_mode", |r| r.get(0))?;
    let fk: i64 = target.pragma_query_value(None, "foreign_keys", |r| r.get(0))?;
    let sync: i64 = target.pragma_query_value(None, "synchronous", |r| r.get(0))?;
    if journal != "wal" || fk != 1 || sync != 2 {
        return Err(StoreError::Invalid(
            "UNIFIED_IMPORT_SQLITE_DURABILITY_INVALID",
        ));
    }
    let target_path: String = target.query_row(
        "SELECT file FROM pragma_database_list WHERE name='main'",
        [],
        |r| r.get(0),
    )?;
    if target_path.is_empty() {
        return Err(StoreError::Invalid("UNIFIED_IMPORT_TARGET_PATH_INVALID"));
    }
    let target_canonical = Path::new(&target_path).canonicalize()?;
    // Acquire all original OS locks in canonical path order so a concurrent
    // native account/campaign writer cannot change a source during import.
    let mut paths = Vec::new();
    for source in accounts.iter().chain(campaigns.iter()) {
        let path: String = source.query_row(
            "SELECT file FROM pragma_database_list WHERE name='main'",
            [],
            |r| r.get(0),
        )?;
        if path.is_empty() {
            return Err(StoreError::Invalid("UNIFIED_IMPORT_SOURCE_PATH_INVALID"));
        }
        let canonical = Path::new(&path).canonicalize()?;
        if canonical == target_canonical {
            return Err(StoreError::Invalid("UNIFIED_IMPORT_SOURCE_PATH_INVALID"));
        }
        paths.push(canonical);
    }
    paths.sort();
    if paths.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(StoreError::Invalid("UNIFIED_DUPLICATE_SOURCE_PATH"));
    }
    let mut _leases = Vec::new();
    for path in &paths {
        // Both old native owners use <canonical SQLite path>.writer.lock.
        _leases.push(crate::AccountStore::writer_lease(path)?);
    }

    let mut account_identities = BTreeSet::new();
    for source in accounts {
        if !account_identities.insert(verify_original_source(source, installation)?) {
            return Err(StoreError::Invalid("UNIFIED_DUPLICATE_ACCOUNT_SOURCE"));
        }
    }
    let mut campaign_identities = BTreeSet::new();
    for source in campaigns {
        if !campaign_identities.insert(verify_original_campaign(source, installation)?) {
            return Err(StoreError::Invalid("UNIFIED_DUPLICATE_CAMPAIGN_SOURCE"));
        }
    }
    // A caller-supplied inventory is an exact source-set contract, NOT a
    // trusted export or proof of the old Cloudflare owner being revoked.
    // Compare its complete canonical identities BEFORE touching target SQL.
    if let Some(inventory) = expected {
        let allowed_accounts = inventory_ids(inventory.accounts)?;
        let allowed_campaigns = inventory_ids(inventory.campaigns)?;
        if inventory.installation != installation
            || account_identities != allowed_accounts
            || campaign_identities != allowed_campaigns
        {
            return Err(StoreError::Invalid("UNIFIED_IMPORT_INVENTORY_MISMATCH"));
        }
    }
    let existing: i64 = target.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table'
         AND name NOT LIKE 'sqlite_%'",
        [],
        |r| r.get(0),
    )?;
    if existing != 0 {
        return Err(StoreError::Invalid("UNIFIED_IMPORT_TARGET_NOT_EMPTY"));
    }
    let complete = include_str!("../../../schema/finance-unified-v1.sql");
    const MARKER: &str =
        "-- Original immutable-event rules and scoped source-invalidation triggers.";
    let (tables_and_indexes, trigger_and_version) = complete.split_once(MARKER).ok_or(
        StoreError::Invalid("UNIFIED_SCHEMA_TRIGGER_BOUNDARY_MISSING"),
    )?;
    let tx = target.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute_batch(tables_and_indexes)?;
    tx.pragma_update(None, "defer_foreign_keys", 1)?;
    let mut account_rows = 0u64;
    for source in accounts {
        let account = verify_original_source(source, installation)?;
        account_rows = account_rows
            .checked_add(copy_one_scope(
                source,
                &tx,
                installation,
                "account_key",
                &account,
                &ORIGINAL_ACCOUNT_TABLES,
            )?)
            .ok_or(StoreError::Invalid("UNIFIED_LEGACY_ROW_COUNT_OVERFLOW"))?;
    }
    let mut campaign_rows = 0u64;
    for source in campaigns {
        let campaign = verify_original_campaign(source, installation)?;
        campaign_rows = campaign_rows
            .checked_add(copy_one_scope(
                source,
                &tx,
                installation,
                "campaign_id",
                &campaign,
                &ORIGINAL_CAMPAIGN_TABLES,
            )?)
            .ok_or(StoreError::Invalid("UNIFIED_LEGACY_ROW_COUNT_OVERFLOW"))?;
    }
    // Original event logs must precede trigger installation to avoid replaying
    // immutable operation_receipts as newly emitted source events.
    tx.execute_batch(&format!("{MARKER}{trigger_and_version}"))?;
    let count: i64 = tx.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table'
         AND name NOT LIKE 'sqlite_%'",
        [],
        |r| r.get(0),
    )?;
    if count != 48 {
        return Err(StoreError::Invalid("UNIFIED_COMPLETE_TABLE_CENSUS_INVALID"));
    }
    let (recorded_account_rows, recorded_campaign_rows) =
        target_row_totals(&tx, installation, &account_identities, &campaign_identities)?;
    if (recorded_account_rows, recorded_campaign_rows) != (account_rows, campaign_rows) {
        return Err(StoreError::Invalid("UNIFIED_IMPORT_ROW_PARITY_FAILED"));
    }
    let inventory_sha256 = hash_inventory(installation, &account_identities, &campaign_identities);
    let content_sha256 =
        target_content_digest(&tx, installation, &account_identities, &campaign_identities)?;
    if expected.is_some() {
        // The immutable marker is part of this SAME data transaction.
        // OFFLINE_NOT_AUTHORIZED explicitly cannot grant any finance capability.
        tx.execute(
            "INSERT INTO finance_import_manifest
             (singleton,installation_id,inventory_sha256,content_sha256,
              account_count,campaign_count,account_rows,campaign_rows,state)
             VALUES(1,?1,?2,?3,?4,?5,?6,?7,'OFFLINE_NOT_AUTHORIZED')",
            params![
                installation,
                &inventory_sha256,
                &content_sha256,
                i64::try_from(account_identities.len())
                    .map_err(|_| StoreError::Invalid("UNIFIED_IMPORT_ROW_COUNT_OVERFLOW"))?,
                i64::try_from(campaign_identities.len())
                    .map_err(|_| StoreError::Invalid("UNIFIED_IMPORT_ROW_COUNT_OVERFLOW"))?,
                i64::try_from(account_rows)
                    .map_err(|_| StoreError::Invalid("UNIFIED_IMPORT_ROW_COUNT_OVERFLOW"))?,
                i64::try_from(campaign_rows)
                    .map_err(|_| StoreError::Invalid("UNIFIED_IMPORT_ROW_COUNT_OVERFLOW"))?,
            ],
        )?;
    }
    let mut fk_check = tx.prepare("PRAGMA foreign_key_check")?;
    if fk_check.query([])?.next()?.is_some() {
        return Err(StoreError::Invalid("UNIFIED_IMPORT_FOREIGN_KEY_FAILED"));
    }
    drop(fk_check);
    tx.commit()?;
    Ok(OriginalFinanceImport {
        accounts: accounts.len(),
        campaigns: campaigns.len(),
        tables_per_account: ORIGINAL_ACCOUNT_TABLES.len(),
        tables_per_campaign: ORIGINAL_CAMPAIGN_TABLES.len(),
        account_rows,
        campaign_rows,
        inventory_sha256,
        content_sha256,
    })
}

/// Offline migration witness only. Not wired into active account/RiskBook
/// owners until original authority is physically fenced and inventoried.
pub(in crate::account_store) fn import_stopped_finance_into_empty(
    target: &mut Connection,
    expected: &ExpectedFinanceInventory<'_>,
    accounts: &[&Connection],
    campaigns: &[&Connection],
) -> StoreResult<OriginalFinanceImport> {
    if campaigns.is_empty() || expected.campaigns.is_empty() {
        return Err(StoreError::Invalid("UNIFIED_CAMPAIGN_INVENTORY_UNPROVEN"));
    }
    import_sources(
        target,
        expected.installation,
        accounts,
        campaigns,
        Some(expected),
    )
}

/// Read-only reopen/recovery witness: the caller can distinguish a COMMIT
/// whose reply was lost from a rolled-back, partial or mismatched import.
/// This never turns the offline marker into a live trading authority.
pub(in crate::account_store) fn inspect_stopped_finance_import(
    db: &Connection,
    expected: &ExpectedFinanceInventory<'_>,
) -> StoreResult<OriginalFinanceImport> {
    let accounts = inventory_ids(expected.accounts)?;
    let campaigns = inventory_ids(expected.campaigns)?;
    if expected.installation.is_empty() {
        return Err(StoreError::Invalid("UNIFIED_IMPORT_INVENTORY_EMPTY"));
    }
    let tx = db.unchecked_transaction()?;
    let version: i64 = tx.pragma_query_value(None, "user_version", |r| r.get(0))?;
    if version != 6 {
        return Err(StoreError::Invalid("UNIFIED_IMPORT_TARGET_VERSION_INVALID"));
    }
    let count: i64 = tx.query_row(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type='table' AND name NOT LIKE 'sqlite_%'",
        [],
        |r| r.get(0),
    )?;
    if count != 48 {
        return Err(StoreError::Invalid("UNIFIED_COMPLETE_TABLE_CENSUS_INVALID"));
    }
    let found_accounts: BTreeSet<String> = {
        let mut stmt =
            tx.prepare("SELECT account_key FROM account_meta WHERE installation_id=?1")?;
        stmt.query_map([expected.installation], |r| r.get(0))?
            .collect::<Result<_, _>>()?
    };
    let found_campaigns: BTreeSet<String> = {
        let mut stmt = tx.prepare("SELECT campaign_id FROM risk_meta WHERE installation_id=?1")?;
        stmt.query_map([expected.installation], |r| r.get(0))?
            .collect::<Result<_, _>>()?
    };
    if found_accounts != accounts || found_campaigns != campaigns {
        return Err(StoreError::Invalid("UNIFIED_IMPORT_INVENTORY_MISMATCH"));
    }
    let marker: Option<(String, String, String, String, i64, i64, i64, i64)> = tx
        .query_row(
            "SELECT installation_id,inventory_sha256,content_sha256,state,
                    account_count,campaign_count,account_rows,campaign_rows
             FROM finance_import_manifest WHERE singleton=1",
            [],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                ))
            },
        )
        .optional()?;
    let (
        installation,
        inventory_sha256,
        content_sha256,
        state,
        account_count,
        campaign_count,
        account_rows,
        campaign_rows,
    ) = marker.ok_or(StoreError::Invalid("UNIFIED_IMPORT_COMMIT_MARKER_MISSING"))?;
    if installation != expected.installation
        || state != "OFFLINE_NOT_AUTHORIZED"
        || account_count
            != i64::try_from(accounts.len())
                .map_err(|_| StoreError::Invalid("UNIFIED_IMPORT_ROW_COUNT_OVERFLOW"))?
        || campaign_count
            != i64::try_from(campaigns.len())
                .map_err(|_| StoreError::Invalid("UNIFIED_IMPORT_ROW_COUNT_OVERFLOW"))?
        || hash_inventory(expected.installation, &accounts, &campaigns) != inventory_sha256
    {
        return Err(StoreError::Invalid("UNIFIED_IMPORT_COMMIT_MARKER_CONFLICT"));
    }
    let (actual_account_rows, actual_campaign_rows) =
        target_row_totals(&tx, expected.installation, &accounts, &campaigns)?;
    if i64::try_from(actual_account_rows)
        .map_err(|_| StoreError::Invalid("UNIFIED_IMPORT_ROW_COUNT_OVERFLOW"))?
        != account_rows
        || i64::try_from(actual_campaign_rows)
            .map_err(|_| StoreError::Invalid("UNIFIED_IMPORT_ROW_COUNT_OVERFLOW"))?
            != campaign_rows
    {
        return Err(StoreError::Invalid("UNIFIED_IMPORT_COMMIT_MARKER_CONFLICT"));
    }
    let actual_digest = target_content_digest(&tx, expected.installation, &accounts, &campaigns)?;
    if actual_digest != content_sha256 {
        return Err(StoreError::Invalid(
            "UNIFIED_IMPORT_CONTENT_DIGEST_MISMATCH",
        ));
    }
    let integrity: String = tx.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    if integrity != "ok" {
        return Err(StoreError::Invalid("UNIFIED_IMPORT_INTEGRITY_FAILED"));
    }
    let mut fk_check = tx.prepare("PRAGMA foreign_key_check")?;
    if fk_check.query([])?.next()?.is_some() {
        return Err(StoreError::Invalid("UNIFIED_IMPORT_FOREIGN_KEY_FAILED"));
    }
    drop(fk_check);
    tx.commit()?;
    Ok(OriginalFinanceImport {
        accounts: accounts.len(),
        campaigns: campaigns.len(),
        tables_per_account: ORIGINAL_ACCOUNT_TABLES.len(),
        tables_per_campaign: ORIGINAL_CAMPAIGN_TABLES.len(),
        account_rows: actual_account_rows,
        campaign_rows: actual_campaign_rows,
        inventory_sha256,
        content_sha256,
    })
}

/// Historical account-only direct test, not a cutover API. It deliberately
/// does not import campaign obligations and must never activate the target.
#[cfg(test)]
pub(in crate::account_store) fn import_stopped_accounts_into_empty_finance(
    target: &mut Connection,
    installation: &str,
    accounts: &[&Connection],
) -> StoreResult<OriginalAccountImport> {
    let result = import_sources(target, installation, accounts, &[], None)?;
    Ok(OriginalAccountImport {
        accounts: result.accounts,
        tables_per_account: result.tables_per_account,
        imported_rows: result.account_rows,
    })
}

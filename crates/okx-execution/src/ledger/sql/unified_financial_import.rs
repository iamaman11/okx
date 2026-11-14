//! #510 r4: PRIVATE offline, lossless source rows into the one finance SQLite.
//! This moves the ORIGINAL 39 AccountStore tables, not a shadow fee/ledger
//! projection. Only an authorized stopped/fenced legacy owner may supply the
//! immutable source snapshots. No production import or trade entrypoint uses
//! this helper until the campaign import and cross-host authority cutover.
use crate::{StoreError, StoreResult};
use rusqlite::{
    Connection, Transaction, TransactionBehavior, params, params_from_iter, types::Value,
};
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

#[derive(Debug, PartialEq, Eq)]
pub(in crate::account_store) struct OriginalFinanceImport {
    pub accounts: usize,
    pub campaigns: usize,
    pub tables_per_account: usize,
    pub tables_per_campaign: usize,
    pub account_rows: u64,
    pub campaign_rows: u64,
}

#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
pub(in crate::account_store) struct OriginalAccountImport {
    pub accounts: usize,
    pub tables_per_account: usize,
    pub imported_rows: u64,
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
    if count != 47 {
        return Err(StoreError::Invalid("UNIFIED_COMPLETE_TABLE_CENSUS_INVALID"));
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
    })
}

/// Offline migration witness only. Not wired into active account/RiskBook
/// owners until original authority is physically fenced and inventoried.
pub(in crate::account_store) fn import_stopped_finance_into_empty(
    target: &mut Connection,
    installation: &str,
    accounts: &[&Connection],
    campaigns: &[&Connection],
) -> StoreResult<OriginalFinanceImport> {
    if campaigns.is_empty() {
        return Err(StoreError::Invalid("UNIFIED_CAMPAIGN_INVENTORY_UNPROVEN"));
    }
    import_sources(target, installation, accounts, campaigns)
}

/// Historical account-only direct test, not a cutover API. It deliberately
/// does not import campaign obligations and must never activate the target.
#[cfg(test)]
pub(in crate::account_store) fn import_stopped_accounts_into_empty_finance(
    target: &mut Connection,
    installation: &str,
    accounts: &[&Connection],
) -> StoreResult<OriginalAccountImport> {
    let result = import_sources(target, installation, accounts, &[])?;
    Ok(OriginalAccountImport {
        accounts: result.accounts,
        tables_per_account: result.tables_per_account,
        imported_rows: result.account_rows,
    })
}

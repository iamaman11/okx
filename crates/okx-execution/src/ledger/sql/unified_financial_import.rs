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
    account: &str,
) -> StoreResult<i64> {
    Ok(tx.query_row(
        &format!(
            "SELECT COUNT(*) FROM {} WHERE installation_id=?1 AND account_key=?2",
            quote(table),
        ),
        params![installation, account],
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

fn copy_one_account(
    source: &Connection,
    tx: &Transaction<'_>,
    installation: &str,
    account: &str,
) -> StoreResult<u64> {
    let mut imported = 0u64;
    for table in ORIGINAL_ACCOUNT_TABLES {
        let source_cols = columns(source, table)?;
        let mut target_cols = source_cols.clone();
        if !target_cols.iter().any(|c| c == "installation_id") {
            target_cols.insert(0, "installation_id".into());
        }
        if !target_cols.iter().any(|c| c == "account_key") {
            target_cols.insert(1, "account_key".into());
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
                if column == "account_key" && value != &Value::Text(account.to_owned()) {
                    return Err(StoreError::Invalid("UNIFIED_LEGACY_ROW_ACCOUNT_MISMATCH"));
                }
            }
            let mut values = Vec::with_capacity(target_cols.len());
            for col in &target_cols {
                if let Some(i) = source_cols.iter().position(|c| c == col) {
                    values.push(native_values[i].clone());
                } else if col == "installation_id" {
                    values.push(Value::Text(installation.to_owned()));
                } else if col == "account_key" {
                    values.push(Value::Text(account.to_owned()));
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
        if n != actual || count_scoped(tx, table, installation, account)? != actual {
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

/// One owner transaction imports a stopped installation's exact original
/// account SQL table rows. It does NOT import the 8 campaign authority tables
/// or revoke Cloudflare. Until those are implemented, never activate this DB.
/// Sources must be authenticated, stopped and writer-fenced by the caller;
/// a read-only SQLite connection alone does not prove distributed fencing.
pub(in crate::account_store) fn import_stopped_accounts_into_empty_finance(
    target: &mut Connection,
    installation: &str,
    sources: &[&Connection],
) -> StoreResult<OriginalAccountImport> {
    if installation.is_empty() || sources.is_empty() {
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
    // Local exclusive OS leases MUST cover the entire copy/verification
    // transaction. A read-only SQLite handle alone does not prohibit the
    // original AccountStore writer from changing WAL under our feet.
    let target_path: String = target.query_row(
        "SELECT file FROM pragma_database_list WHERE name='main'",
        [],
        |r| r.get(0),
    )?;
    let mut original_paths = Vec::new();
    for source in sources {
        let path: String = source.query_row(
            "SELECT file FROM pragma_database_list WHERE name='main'",
            [],
            |r| r.get(0),
        )?;
        if path.is_empty() || path == target_path {
            return Err(StoreError::Invalid("UNIFIED_IMPORT_SOURCE_PATH_INVALID"));
        }
        original_paths.push(path);
    }
    original_paths.sort();
    if original_paths.windows(2).any(|paths| paths[0] == paths[1]) {
        return Err(StoreError::Invalid("UNIFIED_DUPLICATE_ACCOUNT_SOURCE"));
    }
    let mut _source_leases = Vec::new();
    for path in &original_paths {
        _source_leases.push(crate::AccountStore::writer_lease(Path::new(path))?);
    }

    let mut identities = BTreeSet::new();
    for source in sources {
        let account = verify_original_source(source, installation)?;
        if !identities.insert(account) {
            return Err(StoreError::Invalid("UNIFIED_DUPLICATE_ACCOUNT_SOURCE"));
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
    let mut total = 0u64;
    for source in sources {
        let account = verify_original_source(source, installation)?;
        let rows = copy_one_account(source, &tx, installation, &account)?;
        total = total
            .checked_add(rows)
            .ok_or(StoreError::Invalid("UNIFIED_LEGACY_ROW_COUNT_OVERFLOW"))?;
    }
    // Original event logs are copied with their source IDs untouched BEFORE
    // triggers are installed. Otherwise INSERT into operation_receipts would
    // regenerate the same event and double the source proof.
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
    Ok(OriginalAccountImport {
        accounts: sources.len(),
        tables_per_account: ORIGINAL_ACCOUNT_TABLES.len(),
        imported_rows: total,
    })
}

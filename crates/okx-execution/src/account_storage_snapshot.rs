//! Bounded diagnostic copies read by the existing account SQL owner only.
//! These values never authorize an operation and cannot mutate financial state.
use super::*;

#[derive(Clone, Debug, Serialize)]
pub struct AccountStoragePolicy {
    pub revision: i64,
    pub explicit_pause: bool,
    pub campaign_id: String,
    pub campaign_limit_usd: Decimal,
    pub campaign_loss_usd: Decimal,
}

#[derive(Clone, Debug, Serialize)]
pub struct AccountStorageStop {
    pub id: String,
    pub scope: String,
    pub reason_code: String,
    pub latched_at_ms: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct AccountStorageCoverage {
    pub source: &'static str,
    pub partition: &'static str,
    pub watermark: Option<i64>,
    pub generation: Option<i64>,
    pub gaps: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AccountStorageSnapshot {
    pub account: String,
    pub observed_at_ms: i64,
    /// Connection-local SQLite change counter, reset on reopen. This can include
    /// rolled-back writes; it is an invalidation hint, not a commit receipt.
    pub sql_change_sequence: u64,
    pub policy: AccountStoragePolicy,
    /// Exact only when the complete snapshot query successfully finishes.
    pub stop_count: u64,
    pub stops: Vec<AccountStorageStop>,
    pub sqlite_version: String,
    pub sqlite_source_id: String,
    pub database_bytes: u64,
    pub wal_bytes: u64,
    pub maintenance_error: Option<String>,
    pub source_coverage: Vec<AccountStorageCoverage>,
}

pub(super) fn safe_reason(reason: &str) -> String {
    if !reason.is_empty()
        && reason.len() <= 96
        && reason
            .bytes()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'_')
    {
        reason.into()
    } else {
        "ACCOUNT_STORAGE_UNAVAILABLE".into()
    }
}

impl AccountStore {
    pub(crate) fn diagnostic_change_sequence(&self) -> u64 {
        self.connection.total_changes()
    }

    /// Called inside the existing owner's diagnostic deadline. No ledger scan,
    /// writer, persistent table, clock-triggered job or trading proof is added.
    pub(crate) fn diagnostic_storage_snapshot(
        &self,
        previous: Option<&AccountStorageSnapshot>,
    ) -> StoreResult<AccountStorageSnapshot> {
        let policy = self.policy()?;
        let stop_count: i64 = self.connection.query_row(
            "SELECT COUNT(*) FROM risk_stops WHERE resolution IS NULL",
            [],
            |r| r.get(0),
        )?;
        let stop_count = u64::try_from(stop_count)
            .map_err(|_| StoreError::Invalid("STORAGE_STOP_COUNT_INVALID"))?;
        let stops = {
            let mut statement = self.connection.prepare(
                "SELECT stop_id,scope,reason,latched_at FROM risk_stops WHERE resolution IS NULL ORDER BY latched_at,stop_id LIMIT 10")?;
            statement
                .query_map([], |r| {
                    Ok(AccountStorageStop {
                        id: r.get(0)?,
                        scope: r.get(1)?,
                        reason_code: safe_reason(&r.get::<_, String>(2)?),
                        latched_at_ms: r.get(3)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let (sqlite_version, sqlite_source_id) = match previous {
            Some(snapshot) => (
                snapshot.sqlite_version.clone(),
                snapshot.sqlite_source_id.clone(),
            ),
            None => self.sqlite_runtime()?,
        };
        let (database_bytes, wal_bytes) = self.storage_bytes()?;
        let mut source_coverage = Vec::with_capacity(5);
        for (source, partition) in [
            ("account_snapshot", "account"),
            ("fills", "SPOT"),
            ("fills", "SWAP"),
            ("fills", "FUTURES"),
            ("bills", "account"),
        ] {
            let cursor = self.cursor(source, partition)?;
            source_coverage.push(AccountStorageCoverage {
                source,
                partition,
                watermark: cursor.as_ref().map(|c| c.watermark),
                generation: cursor.as_ref().map(|c| c.generation),
                gaps: cursor.map(|c| c.coverage_gaps.chars().take(256).collect()),
            });
        }
        Ok(AccountStorageSnapshot {
            account: self.meta.account_key.clone(),
            observed_at_ms: wall_time_ms(),
            sql_change_sequence: self.diagnostic_change_sequence(),
            policy: AccountStoragePolicy {
                revision: policy.0,
                explicit_pause: policy.1,
                campaign_id: policy.2,
                campaign_limit_usd: policy.3,
                campaign_loss_usd: policy.4,
            },
            stop_count,
            stops,
            sqlite_version,
            sqlite_source_id,
            database_bytes,
            wal_bytes,
            maintenance_error: self.storage_health().map(safe_reason),
            source_coverage,
        })
    }
}

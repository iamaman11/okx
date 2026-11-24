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

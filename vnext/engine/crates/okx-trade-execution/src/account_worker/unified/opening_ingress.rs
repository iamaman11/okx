//! Reported producer observations are bound to the ORIGINAL SQL cohort/generation.
//! These DTOs are data, not an activation, mutation or venue-send capability.
use super::{UnifiedFinanceHandle, UnifiedFinanceWrite, UnifiedFinanceWriteReceipt};
use crate::{StoreError, StoreResult};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct ReportedOpeningCensus {
    pub source_digest: String,
    pub generated_at_ms: i64,
    pub read_floor_ms: i64,
    pub occupied: BTreeMap<String, String>,
    pub cumulative_loss_usd: BTreeMap<String, String>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportedOpeningReceipt {
    pub source_digest: String,
    pub book_sequence: u64,
}
impl UnifiedFinanceHandle {
    pub async fn install_reported_opening(
        &self,
        source: crate::risk::NativeOpeningSource,
        census: ReportedOpeningCensus,
    ) -> StoreResult<ReportedOpeningReceipt> {
        match self
            .apply_financial_write(UnifiedFinanceWrite::InstallReportedOpening { source, census })
            .await?
        {
            UnifiedFinanceWriteReceipt::Risk(
                crate::risk::UnifiedRiskDecision::EntryOpeningInstalled {
                    source_digest,
                    book_sequence,
                    ..
                },
            ) => Ok(ReportedOpeningReceipt {
                source_digest,
                book_sequence,
            }),
            _ => Err(StoreError::Invalid("UNIFIED_OPENING_RECEIPT_UNEXPECTED")),
        }
    }
}

/// Only the existing FIFO's transaction may call this binder. The public
/// dispatch supplies owner execution time, never a caller-selected timestamp.
pub(crate) fn install_reported_opening_sql(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    source: &crate::risk::NativeOpeningSource,
    reported: &ReportedOpeningCensus,
    now_ms: i64,
) -> StoreResult<UnifiedFinanceWriteReceipt> {
    use okx_engine_risk::risk::money::RiskAmount;
    let digest = &reported.source_digest;
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v))
        || reported.generated_at_ms <= 0
        || reported.read_floor_ms <= 0
        || reported.read_floor_ms > reported.generated_at_ms
        || reported.generated_at_ms > now_ms
        || now_ms - reported.generated_at_ms > 5000
    {
        return Err(StoreError::Invalid("UNIFIED_OPENING_CENSUS_INVALID"));
    }
    let generation:i64=tx.query_row("SELECT generation FROM risk_meta WHERE installation_id=?1 AND campaign_id=?2 AND singleton=1",rusqlite::params![installation,source.campaign_id],|r|r.get(0))?;
    let generation = u64::try_from(generation)
        .ok()
        .filter(|g| *g > 0)
        .ok_or(StoreError::Invalid("UNIFIED_OPENING_GENERATION_INVALID"))?;
    let mut query=tx.prepare("SELECT account_key FROM risk_policy_account WHERE installation_id=?1 AND campaign_id=?2 ORDER BY account_key")?;
    let accounts = query
        .query_map(rusqlite::params![installation, source.campaign_id], |r| {
            r.get::<_, String>(0)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if accounts.is_empty()
        || accounts.len() > 32
        || accounts.iter().ne(reported.occupied.keys())
        || accounts.iter().ne(reported.cumulative_loss_usd.keys())
    {
        return Err(StoreError::Invalid(
            "UNIFIED_OPENING_CENSUS_COHORT_MISMATCH",
        ));
    }
    let parse = |values: &BTreeMap<String, String>| -> StoreResult<BTreeMap<String, RiskAmount>> {
        values
            .iter()
            .map(|(key, value)| {
                Ok((
                    key.clone(),
                    RiskAmount::parse(value)
                        .map_err(|_| StoreError::Invalid("UNIFIED_OPENING_CENSUS_MONEY_INVALID"))?,
                ))
            })
            .collect()
    };
    let census = crate::risk::VerifiedCensus {
        complete: true,
        generation,
        source_digest: reported.source_digest.clone(),
        generated_at_ms: reported.generated_at_ms,
        read_floor_ms: reported.read_floor_ms,
        occupied: parse(&reported.occupied)?,
        cumulative_loss_usd: parse(&reported.cumulative_loss_usd)?,
    };
    crate::risk::install_entry_opening_unified_native(tx, installation, source, &census, now_ms)
        .map(UnifiedFinanceWriteReceipt::Risk)
        .map_err(StoreError::Invalid)
}

pub(super) fn owner_now_ms() -> StoreResult<i64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|v| i64::try_from(v.as_millis()).ok())
        .ok_or(StoreError::Invalid("UNIFIED_OPENING_CLOCK_UNAVAILABLE"))
}

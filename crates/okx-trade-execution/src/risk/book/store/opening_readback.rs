//! Immutable opening provenance, not current venue freshness or a send grant.
use super::{NativeOpeningSource, Result, RiskStoreError, amount};
use rusqlite::{OptionalExtension, Transaction, params};
pub(in crate::risk::book) fn read_unified_campaign_opening(
    tx: &Transaction<'_>,
    installation: &str,
    campaign: &str,
) -> Result<Option<NativeOpeningSource>> {
    let account:Option<String>=tx.query_row("SELECT MIN(account_key) FROM risk_policy_account WHERE installation_id=?1 AND campaign_id=?2",params![installation,campaign],|r|r.get(0))?;
    let account = account.ok_or(RiskStoreError::Invalid("OPENING_ACCOUNT_ROSTER_MISSING"))?;
    let stored:Option<(String,String,i64)>=tx.query_row("SELECT request_hash,result_ref,committed_at FROM operation_receipts WHERE installation_id=?1 AND account_key=?2 AND operation_id=?3 AND kind='CAMPAIGN_OPENING_SOURCE'",params![installation,account,format!("risk-entry-opening-source:{campaign}")],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    let Some((digest, json, at)) = stored else {
        return Ok(None);
    };
    let source: NativeOpeningSource = serde_json::from_str(&json)?;
    if !source.valid()
        || source.campaign_id != campaign
        || source.source_digest != digest
        || source.canonical_digest()? != digest
        || i64::try_from(source.as_of_ms).ok() != Some(at)
        || source
            .accounts
            .first()
            .is_none_or(|a| a.account_key != account)
    {
        return Err(RiskStoreError::Invalid("OPENING_PROVENANCE_CONFLICT"));
    }
    let original:Option<(String,String,i64)>=tx.query_row("SELECT request_hash,result_ref,committed_at FROM operation_receipts WHERE installation_id=?1 AND account_key=?2 AND operation_id=?3 AND kind='CAMPAIGN_OPENING_INSTALLED'",params![installation,account,format!("risk-entry-opening-installed:{campaign}")],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    let Some((hash, money, time)) = original else {
        return Err(RiskStoreError::Invalid("OPENING_INSTALL_RECEIPT_MISSING"));
    };
    if hash != digest || time != at || amount(money)? != amount(source.consumed_usd.clone())? {
        return Err(RiskStoreError::Invalid("OPENING_PROVENANCE_CONFLICT"));
    }
    Ok(Some(source))
}

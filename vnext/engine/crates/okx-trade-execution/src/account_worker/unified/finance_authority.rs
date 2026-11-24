//! SQL authority: source collection, preparation, then proven execution.
use crate::{StoreError, StoreResult};

/// Execution requires the original policy, account connection and financial
/// opening receipts in the same SQLite snapshot. Source collection and baseline
/// preparation cannot grant execution. No Cloudflare financial authority applies.
pub(super) fn require_verified_live_finance_authority(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
) -> StoreResult<()> {
    if require_native_campaign_policy(tx, installation)? {
        let campaigns: Vec<String> = tx
            .prepare("SELECT campaign_id FROM risk_meta WHERE installation_id=?1")?
            .query_map([installation], |r| r.get(0))?
            .collect::<Result<_, _>>()?;
        for campaign in campaigns {
            let opening = crate::risk::read_unified_campaign_opening(tx, installation, &campaign)
                .map_err(StoreError::Invalid)?
                .ok_or(StoreError::Invalid("FINANCE_SOURCE_ACCEPTANCE_REQUIRED"))?;
            let roster:Vec<String>=tx.prepare("SELECT account_key FROM risk_policy_account WHERE installation_id=?1 AND campaign_id=?2 ORDER BY account_key")?
                .query_map(rusqlite::params![installation,campaign],|r|r.get(0))?.collect::<Result<_,_>>()?;
            if roster
                .iter()
                .ne(opening.accounts.iter().map(|a| &a.account_key))
            {
                return Err(StoreError::Invalid("FINANCE_OPENING_COHORT_MISMATCH"));
            }
        }
        return Ok(());
    }
    let state: String = tx.query_row(
        "SELECT state FROM finance_import_manifest
         WHERE singleton=1 AND installation_id=?1",
        [installation],
        |row| row.get(0),
    )?;
    if state == "OFFLINE_NOT_AUTHORIZED" {
        return Err(StoreError::Invalid("FINANCE_CUTOVER_NOT_ACCEPTED"));
    }
    Err(StoreError::Invalid("FINANCE_AUTHORITY_STATE_UNVERIFIED"))
}

/// Native policy permits preparation and proving the initial money window;
/// only the separate original opening receipt can permit financial execution.
/// False denotes a historical fixture, whose unchanged offline guard applies.
pub(super) fn require_native_campaign_policy(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
) -> StoreResult<bool> {
    let fresh:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='finance_installation')",[],|r|r.get(0))?;
    if !fresh {
        return Ok(false);
    }
    super::source_permission::require_source_account(tx, installation, None)?;
    let accounts: i64 = tx.query_row(
        "SELECT COUNT(*) FROM account_meta WHERE installation_id=?1",
        [installation],
        |r| r.get(0),
    )?;
    if accounts == 0 {
        return Err(StoreError::Invalid("FINANCE_ACCOUNT_BOOTSTRAP_REQUIRED"));
    }
    let campaigns: Vec<String> = tx
        .prepare("SELECT campaign_id FROM risk_meta WHERE installation_id=?1")?
        .query_map([installation], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    if campaigns.is_empty() {
        return Err(StoreError::Invalid("FINANCE_CAMPAIGN_POLICY_REQUIRED"));
    }
    for campaign in campaigns {
        let configured: i64 = tx.query_row(
            "SELECT COUNT(*) FROM risk_policy_account WHERE installation_id=?1 AND campaign_id=?2",
            rusqlite::params![installation, campaign],
            |r| r.get(0),
        )?;
        if configured != accounts {
            return Err(StoreError::Invalid("FINANCE_CAMPAIGN_COHORT_MISMATCH"));
        }
        crate::risk::verify_unified_import_campaign(tx, installation, &campaign)
            .map_err(StoreError::Invalid)?;
        let inconsistent:i64=tx.query_row("SELECT COUNT(*) FROM risk_policy_account r JOIN risk_meta m USING(installation_id,campaign_id)
            LEFT JOIN account_policy a ON a.installation_id=r.installation_id AND a.account_key=r.account_key AND a.singleton=1
            LEFT JOIN account_meta i ON i.installation_id=r.installation_id AND i.account_key=r.account_key AND i.singleton=1
            LEFT JOIN operation_receipts c ON c.installation_id=i.installation_id AND c.account_key=i.account_key
                AND c.operation_id='account-connect:'||i.initialize_operation_id AND c.kind='ACCOUNT_CONNECTED'
            WHERE r.installation_id=?1 AND r.campaign_id=?2 AND (a.revision IS NULL OR a.revision<>m.policy_revision
                OR a.campaign_id<>m.campaign_id OR c.operation_id IS NULL)",rusqlite::params![installation,campaign],|r|r.get(0))?;
        if inconsistent != 0 {
            return Err(StoreError::Invalid("FINANCE_CAMPAIGN_POLICY_UNVERIFIED"));
        }
    }
    Ok(true)
}

pub(super) fn require_preparation_authority(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
) -> StoreResult<()> {
    if require_native_campaign_policy(tx, installation)? {
        Ok(())
    } else {
        require_verified_live_finance_authority(tx, installation)
    }
}

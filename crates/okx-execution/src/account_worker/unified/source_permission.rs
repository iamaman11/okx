//! Observing registered accounts is not Reserve/BeginPost authority.
use super::{UnifiedFinanceWrite, require_verified_live_finance_authority};
use crate::{AccountStore, StoreError, StoreResult};

pub(super) fn require_source_account(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    account: Option<&str>,
) -> StoreResult<()> {
    let fresh: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='finance_installation')",
        [], |r|r.get(0),
    )?;
    if !fresh {
        return require_verified_live_finance_authority(tx, installation);
    }
    let valid: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM finance_installation WHERE singleton=1
         AND installation_id=?1 AND schema_version=8)",
        [installation],
        |r| r.get(0),
    )?;
    if !valid {
        return Err(StoreError::Invalid(
            "FINANCE_INSTALLATION_IDENTITY_MISMATCH",
        ));
    }
    if let Some(account) = account {
        AccountStore::verify_source_account_sql(tx, installation, account)?;
    }
    Ok(())
}

pub(super) fn require_command_permission(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    command: &UnifiedFinanceWrite,
) -> StoreResult<()> {
    use UnifiedFinanceWrite::*;
    match command {
        #[cfg(test)]
        InstallEntryOpening { .. } => {
            super::finance_authority::require_preparation_authority(tx, installation)
        }
        #[cfg(test)]
        Reserve { .. } | SettleEntry { .. } | ReleaseUnsent { .. } => {
            require_verified_live_finance_authority(tx, installation)
        }
        ProjectAccountNav { account, .. }
        | ProjectPendingFills { account, .. }
        | AttributePendingFunding { account, .. }
        | IngestCapturedBills { account, .. }
        | IngestCapturedFills { account, .. }
        | IngestSourcePageUsd { account, .. }
        | IngestSourcePage { account, .. }
        | RecordSourceBills { account, .. }
        | RecordSourceFills { account, .. }
        | RecoverPosting { account, .. }
        | RecordOutcome { account, .. } => require_source_account(tx, installation, Some(account)),
        RecoverInstallation { .. } => require_source_account(tx, installation, None),
        InstallReportedOpening { .. }
        | Prepare { .. }
        | AssignBoundCapital { .. }
        | AssignCapital { .. }
        | CommitStrategyNav { .. } => {
            super::finance_authority::require_preparation_authority(tx, installation)
        }
        ReserveNative { .. }
        | ApplyPolicy { .. }
        | BeginPost { .. }
        | SettleBoundEntry { .. }
        | ReleaseBoundUnsent { .. }
        | ResolveTerminal { .. } => require_verified_live_finance_authority(tx, installation),
    }
}

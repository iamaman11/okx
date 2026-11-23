//! Verified account source -> one durable opening. Never a trading grant.
use crate::{AgentError, AgentResult, account_bootstrap::AccountBootstrapper};
use okx_ipc_contracts::accounts::VerifiedAccountConfig;
use okx_trade_execution::{AccountMeta, account_worker::UnifiedFinanceHandle};
use std::time::{Duration, Instant};

fn error(reason: &str) -> AgentError {
    AgentError::FinancialCaptureTask(reason.into())
}

pub(crate) async fn original_connection(
    owner: &UnifiedFinanceHandle,
    configuration: &VerifiedAccountConfig,
    deadline: Instant,
) -> AgentResult<Option<AccountMeta>> {
    let account = configuration.account_key.wire();
    let Some(meta) = owner.account_metadata(deadline, account.clone()).await? else {
        return Ok(None);
    };
    if meta.initialize_operation_id != configuration.connect_operation_id
        || meta.release_id.is_empty()
    {
        return Err(error("ACCOUNT_CONNECTION_IDENTITY_CONFLICT"));
    }
    let receipt = owner
        .operation(
            deadline,
            account.clone(),
            format!("account-connect:{}", configuration.connect_operation_id),
        )
        .await?;
    if !matches!(receipt,Some(ref r) if r.kind=="ACCOUNT_CONNECTED"
        && r.request_hash==configuration.configuration_digest && r.result_ref==account)
    {
        return Err(error("ACCOUNT_CONNECTION_IDEMPOTENCY_CONFLICT"));
    }
    Ok(Some(meta))
}

/// UID/permissions are authenticated by the caller before entering this
/// operation. A lost SQL reply is resolved by ORIGINAL durable identity and
/// pinned configuration, never by reseeding the latest balance as an opening.
pub(crate) async fn ensure_account(
    owner: &UnifiedFinanceHandle,
    configuration: &VerifiedAccountConfig,
    observer: &AccountBootstrapper,
    verified_uid: &str,
    initialize: bool,
) -> AgentResult<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    if original_connection(owner, configuration, deadline)
        .await?
        .is_some()
    {
        return Ok(());
    }
    if !initialize {
        return Err(error("NATIVE_ACCOUNT_NOT_INITIALIZED"));
    }
    let opening = crate::account_opening::capture_opening(
        observer,
        verified_uid,
        tokio::time::Instant::now() + Duration::from_secs(10),
    )
    .await?;
    let meta = AccountMeta {
        account_key: configuration.account_key.wire(),
        installation_id: owner.installation_id().into(),
        initialized_at: opening.observed_at,
        opening_source: opening.source_ref.clone(),
        initialize_operation_id: configuration.connect_operation_id.clone(),
        release_id: crate::accounts::build_release().into(),
    };
    if let Err(failure) = owner
        .initialize_account(
            deadline,
            meta,
            opening,
            configuration.configuration_digest.clone(),
        )
        .await
    {
        // The same request may already have committed (including a lost ACK).
        // New facts do not overwrite its opening. Failed readback remains UNKNOWN.
        if original_connection(
            owner,
            configuration,
            Instant::now() + Duration::from_secs(2),
        )
        .await?
        .is_none()
        {
            return Err(failure.into());
        }
    }
    Ok(())
}

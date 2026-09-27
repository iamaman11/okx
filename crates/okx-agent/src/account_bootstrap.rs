use chrono::{SecondsFormat, Utc};
use okx_api::{AccountApi, OkxError, OkxRestClient};
use okx_observation::{AccountError, AccountSnapshot};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AccountBootstrapError {
    #[error("OKX observer API key is not strictly read-only")]
    PermissionRejected,

    #[error("OKX private API error: {0}")]
    Api(#[from] OkxError),

    #[error("account normalization error: {0}")]
    Normalize(#[from] AccountError),
}

#[derive(Clone)]
pub struct AccountBootstrapper {
    api: AccountApi,
}

impl AccountBootstrapper {
    pub fn new(client: OkxRestClient) -> Self {
        Self {
            api: AccountApi::new(client),
        }
    }

    pub async fn snapshot(&self) -> Result<AccountSnapshot, AccountBootstrapError> {
        let config = self.api.config().await?;
        let permissions = strict_read_only_permissions(&config.perm)?;

        let (balance, positions, pending_orders) = tokio::try_join!(
            self.api.balance(),
            self.api.positions(),
            self.api.pending_orders(),
        )?;
        let source_received_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);

        Ok(AccountSnapshot::from_rest_bootstrap(
            source_received_at,
            config,
            balance,
            positions,
            pending_orders,
            permissions,
        )?)
    }
}

fn strict_read_only_permissions(value: &str) -> Result<Vec<String>, AccountBootstrapError> {
    let permissions = value
        .split(',')
        .map(str::trim)
        .filter(|permission| !permission.is_empty())
        .collect::<Vec<_>>();

    if permissions.is_empty()
        || permissions.iter().any(|permission| *permission != "read_only")
        || permissions.len() != 1
    {
        return Err(AccountBootstrapError::PermissionRejected);
    }

    Ok(vec!["read_only".to_owned()])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observer_permission_must_be_exactly_read_only() {
        assert_eq!(
            strict_read_only_permissions("read_only").expect("read only"),
            vec!["read_only".to_owned()]
        );
        assert!(matches!(
            strict_read_only_permissions("read_only,trade"),
            Err(AccountBootstrapError::PermissionRejected)
        ));
        assert!(matches!(
            strict_read_only_permissions("withdraw"),
            Err(AccountBootstrapError::PermissionRejected)
        ));
        assert!(matches!(
            strict_read_only_permissions("read_only,unknown_future_permission"),
            Err(AccountBootstrapError::PermissionRejected)
        ));
        assert!(matches!(
            strict_read_only_permissions(""),
            Err(AccountBootstrapError::PermissionRejected)
        ));
    }
}

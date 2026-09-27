use chrono::{SecondsFormat, Utc};
use okx_api::{AccountApi, FeeRate, OkxError, OkxRestClient};
use okx_observation::{
    AccountError, AccountSnapshot, FeeScheduleError, FeeScheduleInput, FeeScheduleSnapshot,
    InstrumentRulesSnapshot,
};
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

#[derive(Debug, Error)]
pub enum FeeScheduleBootstrapError {
    #[error("OKX observer API key is not strictly read-only")]
    PermissionRejected,

    #[error("OKX private API error: {0}")]
    Api(#[from] OkxError),

    #[error("fee reference field '{0}' is unavailable")]
    ReferenceIncomplete(&'static str),

    #[error("fee response is inconsistent: {0}")]
    ResponseInconsistent(String),

    #[error("fee normalization error: {0}")]
    Normalize(#[from] FeeScheduleError),
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

    pub async fn fee_schedule(
        &self,
        rules: &InstrumentRulesSnapshot,
    ) -> Result<FeeScheduleSnapshot, FeeScheduleBootstrapError> {
        let config = self.api.config().await?;
        strict_read_only_permissions(&config.perm)
            .map_err(|_| FeeScheduleBootstrapError::PermissionRejected)?;

        let instrument = &rules.instrument;
        let family = instrument
            .instrument_family
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(FeeScheduleBootstrapError::ReferenceIncomplete("instFamily"))?;
        let expected_group = instrument
            .fee_group_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(FeeScheduleBootstrapError::ReferenceIncomplete("groupId"))?;

        let rows = self
            .api
            .fee_rates_for_family(instrument.instrument_type, family)
            .await?;
        let source_received_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        normalize_fee_schedule(
            &instrument.instrument_id,
            &rules.reference_generation,
            &instrument.instrument_type.to_string(),
            family,
            expected_group,
            source_received_at,
            rows,
        )
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

fn normalize_fee_schedule(
    instrument_id: &str,
    reference_generation: &str,
    expected_type: &str,
    family: &str,
    expected_group: &str,
    source_received_at: String,
    rows: Vec<FeeRate>,
) -> Result<FeeScheduleSnapshot, FeeScheduleBootstrapError> {
    let mut matching_rows = rows
        .into_iter()
        .filter(|row| row.instrument_type == expected_type)
        .collect::<Vec<_>>();
    if matching_rows.len() != 1 {
        return Err(FeeScheduleBootstrapError::ResponseInconsistent(format!(
            "expected one fee-rate row for {expected_type}/{family}, found {}",
            matching_rows.len()
        )));
    }
    let row = matching_rows.pop().expect("length checked");
    if row.timestamp_ms.trim().is_empty() {
        return Err(FeeScheduleBootstrapError::ResponseInconsistent(
            "fee-rate row is missing exchange timestamp".to_owned(),
        ));
    }

    let mut groups = row
        .fee_groups
        .into_iter()
        .filter(|group| group.group_id == expected_group)
        .collect::<Vec<_>>();
    if groups.len() != 1 {
        return Err(FeeScheduleBootstrapError::ResponseInconsistent(format!(
            "expected one fee group '{expected_group}', found {}",
            groups.len()
        )));
    }
    let group = groups.pop().expect("length checked");

    Ok(FeeScheduleSnapshot::from_input(FeeScheduleInput {
        instrument_id: instrument_id.to_owned(),
        reference_generation: reference_generation.to_owned(),
        source_received_at,
        exchange_timestamp_ms: row.timestamp_ms,
        level: row.level,
        maker_rate: group.maker,
        taker_rate: group.taker,
        exact_for_instrument: true,
    })?)
}

fn strict_read_only_permissions(value: &str) -> Result<Vec<String>, AccountBootstrapError> {
    let permissions = value
        .split(',')
        .map(str::trim)
        .filter(|permission| !permission.is_empty())
        .collect::<Vec<_>>();

    if permissions.is_empty()
        || permissions
            .iter()
            .any(|permission| *permission != "read_only")
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

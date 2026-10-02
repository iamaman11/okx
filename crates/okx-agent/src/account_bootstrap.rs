use chrono::{SecondsFormat, Utc};
use okx_api::{
    AccountApi, AccountHistoryApi, AccountPositionRiskSnapshot, AssetApi, FeeRate, InstrumentType,
    MarginMode, OkxError, OkxRestClient,
};
use okx_observation::{
    AccountError, AccountLedgerError, AccountLedgerFacts, AccountSnapshot, FeeScheduleError,
    FeeScheduleInput, FeeScheduleSnapshot, InstrumentRulesSnapshot, TradingCapabilitiesError,
    TradingCapabilitiesInput, TradingCapabilitiesSnapshot,
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
pub enum AccountLedgerBootstrapError {
    #[error("OKX observer API key is not strictly read-only")]
    PermissionRejected,

    #[error("OKX private API error: {0}")]
    Api(#[from] OkxError),

    #[error("account ledger normalization error: {0}")]
    Normalize(#[from] AccountLedgerError),
}

#[derive(Debug, Error)]
pub enum TradingCapabilitiesBootstrapError {
    #[error("OKX observer API key is not strictly read-only")]
    PermissionRejected,

    #[error("OKX private API error: {0}")]
    Api(#[from] OkxError),

    #[error("fee capability assembly error: {0}")]
    Fee(#[from] FeeScheduleBootstrapError),

    #[error("trading capability normalization error: {0}")]
    Normalize(#[from] TradingCapabilitiesError),
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
    history: AccountHistoryApi,
    asset: AssetApi,
}

impl AccountBootstrapper {
    pub fn new(client: OkxRestClient) -> Self {
        Self {
            api: AccountApi::new(client.clone()),
            history: AccountHistoryApi::new(client.clone()),
            asset: AssetApi::new(client),
        }
    }

    pub async fn fee_schedule(
        &self,
        rules: &InstrumentRulesSnapshot,
    ) -> Result<FeeScheduleSnapshot, FeeScheduleBootstrapError> {
        let config = self.api.config().await?;
        strict_read_only_permissions(&config.perm)
            .map_err(|_| FeeScheduleBootstrapError::PermissionRejected)?;
        self.fee_schedule_authorized(rules).await
    }

    async fn fee_schedule_authorized(
        &self,
        rules: &InstrumentRulesSnapshot,
    ) -> Result<FeeScheduleSnapshot, FeeScheduleBootstrapError> {
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

    pub async fn trading_capabilities(
        &self,
        rules: &InstrumentRulesSnapshot,
        margin_mode: MarginMode,
    ) -> Result<TradingCapabilitiesSnapshot, TradingCapabilitiesBootstrapError> {
        let config = self.api.config().await?;
        let permissions = strict_read_only_permissions(&config.perm)
            .map_err(|_| TradingCapabilitiesBootstrapError::PermissionRejected)?;

        let account_instruments = self
            .api
            .instruments(rules.instrument.instrument_type)
            .await?;
        let selected = account_instruments
            .into_iter()
            .find(|instrument| instrument.instrument_id == rules.instrument.instrument_id);

        let mut warnings = Vec::new();
        let (available_to_account, account_max_leverage, configured_leverage, fee_schedule) =
            if let Some(instrument) = selected {
                let configured_leverage = match self
                    .api
                    .leverage(&rules.instrument.instrument_id, margin_mode)
                    .await
                {
                    Ok(rows) => rows,
                    Err(error) => {
                        warnings.push(format!("configured leverage unavailable: {error}"));
                        Vec::new()
                    }
                };

                let fee = self.fee_schedule_authorized(rules).await?;
                (
                    true,
                    non_empty_owned(instrument.lever),
                    configured_leverage,
                    Some(fee),
                )
            } else {
                warnings.push(format!(
                    "instrument '{}' is not available to the authenticated account",
                    rules.instrument.instrument_id
                ));
                (false, None, Vec::new(), None)
            };

        let source_received_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        Ok(TradingCapabilitiesSnapshot::from_input(
            TradingCapabilitiesInput {
                source_received_at,
                config,
                api_key_permissions: permissions,
                rules: rules.clone(),
                available_to_account,
                account_max_leverage,
                requested_margin_mode: margin_mode.to_string(),
                configured_leverage,
                fee_schedule,
                warnings,
            },
        )?)
    }

    pub async fn account_position_risk_oracle(
        &self,
    ) -> Result<AccountPositionRiskSnapshot, AccountBootstrapError> {
        let config = self.api.config().await?;
        strict_read_only_permissions(&config.perm)?;
        Ok(self.api.account_position_risk().await?)
    }

    pub async fn ledger_facts(
        &self,
        snapshot: &AccountSnapshot,
    ) -> Result<AccountLedgerFacts, AccountLedgerBootstrapError> {
        let config = self.api.config().await?;
        strict_read_only_permissions(&config.perm)
            .map_err(|_| AccountLedgerBootstrapError::PermissionRejected)?;

        let funding_balances = self.asset.funding_balances().await?;
        let positions_swap = self.history.positions_history(InstrumentType::Swap).await?;
        let positions_futures = self
            .history
            .positions_history(InstrumentType::Futures)
            .await?;
        let orders_swap = self.history.orders_history(InstrumentType::Swap).await?;
        let orders_futures = self.history.orders_history(InstrumentType::Futures).await?;
        let fills_swap = self.history.fills_history(InstrumentType::Swap).await?;
        let fills_futures = self.history.fills_history(InstrumentType::Futures).await?;
        let bills = self.history.bills_history().await?;

        let source_received_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        Ok(AccountLedgerFacts::from_okx(
            source_received_at,
            snapshot,
            config,
            funding_balances,
            vec![
                ("SWAP".to_owned(), positions_swap),
                ("FUTURES".to_owned(), positions_futures),
            ],
            vec![
                ("SWAP".to_owned(), orders_swap),
                ("FUTURES".to_owned(), orders_futures),
            ],
            vec![
                ("SWAP".to_owned(), fills_swap),
                ("FUTURES".to_owned(), fills_futures),
            ],
            bills,
        )?)
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

fn non_empty_owned(value: String) -> Option<String> {
    (!value.trim().is_empty()).then_some(value)
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
    use okx_api::account::FeeGroup;

    fn fee_row(groups: Vec<FeeGroup>) -> FeeRate {
        FeeRate {
            level: "Lv1".to_owned(),
            timestamp_ms: "1790539200000".to_owned(),
            instrument_type: "SWAP".to_owned(),
            rule_type: "normal".to_owned(),
            maker: "-0.009".to_owned(),
            taker: "-0.009".to_owned(),
            maker_usdt: String::new(),
            taker_usdt: String::new(),
            fee_groups: groups,
        }
    }

    fn fee_group(id: &str, maker: &str, taker: &str) -> FeeGroup {
        FeeGroup {
            group_id: id.to_owned(),
            maker: maker.to_owned(),
            taker: taker.to_owned(),
            elp_maker: String::new(),
            rpi_maker: String::new(),
        }
    }

    #[test]
    fn exact_fee_schedule_uses_matching_fee_group_not_deprecated_top_level_rate() {
        let result = normalize_fee_schedule(
            "DOGE-USDT-SWAP",
            "sha256:reference",
            "SWAP",
            "DOGE-USDT",
            "4",
            "2026-09-27T20:00:00.000Z".to_owned(),
            vec![fee_row(vec![
                fee_group("4", "-0.0002", "-0.0005"),
                fee_group("5", "-0.0001", "-0.0004"),
            ])],
        )
        .expect("fee schedule");

        assert_eq!(result.instrument_id, "DOGE-USDT-SWAP");
        assert_eq!(result.reference_generation, "sha256:reference");
        assert_eq!(result.exchange_timestamp_ms, "1790539200000");
        assert_eq!(result.level, "Lv1");
        assert_eq!(result.maker_rate, "-0.0002");
        assert_eq!(result.taker_rate, "-0.0005");
        assert!(result.exact_for_instrument);
    }

    #[test]
    fn exact_fee_schedule_fails_when_group_mapping_is_missing_or_ambiguous() {
        let missing = normalize_fee_schedule(
            "DOGE-USDT-SWAP",
            "sha256:reference",
            "SWAP",
            "DOGE-USDT",
            "4",
            "2026-09-27T20:00:00.000Z".to_owned(),
            vec![fee_row(vec![fee_group("5", "-0.0001", "-0.0004")])],
        );
        assert!(matches!(
            missing,
            Err(FeeScheduleBootstrapError::ResponseInconsistent(_))
        ));

        let duplicate = normalize_fee_schedule(
            "DOGE-USDT-SWAP",
            "sha256:reference",
            "SWAP",
            "DOGE-USDT",
            "4",
            "2026-09-27T20:00:00.000Z".to_owned(),
            vec![fee_row(vec![
                fee_group("4", "-0.0002", "-0.0005"),
                fee_group("4", "-0.0003", "-0.0006"),
            ])],
        );
        assert!(matches!(
            duplicate,
            Err(FeeScheduleBootstrapError::ResponseInconsistent(_))
        ));
    }

    #[test]
    fn exact_fee_schedule_requires_one_typed_row_and_exchange_timestamp() {
        let wrong_type = normalize_fee_schedule(
            "DOGE-USDT-SWAP",
            "sha256:reference",
            "SWAP",
            "DOGE-USDT",
            "4",
            "2026-09-27T20:00:00.000Z".to_owned(),
            vec![FeeRate {
                instrument_type: "FUTURES".to_owned(),
                ..fee_row(vec![fee_group("4", "-0.0002", "-0.0005")])
            }],
        );
        assert!(matches!(
            wrong_type,
            Err(FeeScheduleBootstrapError::ResponseInconsistent(_))
        ));

        let missing_timestamp = normalize_fee_schedule(
            "DOGE-USDT-SWAP",
            "sha256:reference",
            "SWAP",
            "DOGE-USDT",
            "4",
            "2026-09-27T20:00:00.000Z".to_owned(),
            vec![FeeRate {
                timestamp_ms: String::new(),
                ..fee_row(vec![fee_group("4", "-0.0002", "-0.0005")])
            }],
        );
        assert!(matches!(
            missing_timestamp,
            Err(FeeScheduleBootstrapError::ResponseInconsistent(_))
        ));
    }

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

use chrono::{SecondsFormat, Utc};
use okx_api::{
    AccountApi, AccountConfig, AccountHistoryApi, AccountPositionRiskSnapshot, AssetApi, FeeRate,
    InstrumentType, LeverageInfo, MarginMode, OkxError, OkxRestClient,
    PendingDerivativeAlgoRiskInventory, PendingProtectiveAlgoInventory, PositionBuilderRequest,
    PositionBuilderSnapshot, TradeApi,
};
use okx_observation::{
    AccountError, AccountLedgerError, AccountLedgerFacts, AccountSnapshot, ExecutionFillEvidence,
    FeeScheduleError, FeeScheduleInput, FeeScheduleSnapshot, InstrumentRulesSnapshot,
    TradingCapabilitiesError, TradingCapabilitiesInput, TradingCapabilitiesSnapshot,
    normalize_execution_fill_history,
};
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::sync::Mutex;

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
pub enum ConfiguredLeverageBootstrapError {
    #[error("OKX observer API key is not strictly read-only")]
    PermissionRejected,

    #[error("OKX private API error: {0}")]
    Api(#[from] OkxError),

    #[error(
        "expected exactly one configured leverage row for {instrument}/{margin_mode}/{position_side}, found {found}"
    )]
    Ambiguous {
        instrument: String,
        margin_mode: String,
        position_side: String,
        found: usize,
    },

    #[error("configured leverage is empty")]
    Empty,
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

// The observer credential belongs to one account; never share its configuration across UIDs.
// The 2s bound matches the narrow account/config rate-limit window, not risk-source freshness.
const OBSERVER_CONFIG_MAX_REUSE_AGE: Duration = Duration::from_secs(2);

struct ObservedConfig {
    config: AccountConfig,
    received_at: chrono::DateTime<Utc>,
    sampled_at: Instant,
}

#[derive(Default)]
struct ObserverConfigCache {
    current: Mutex<Option<ObservedConfig>>,
}

impl ObserverConfigCache {
    /// Lock across the one authenticated read: concurrent callers share the same proof.
    /// Failed refreshes discard expired proof; no stale success/fallback is possible.
    async fn read<F, Fut>(
        &self,
        fetch: F,
    ) -> Result<(AccountConfig, chrono::DateTime<Utc>), OkxError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<AccountConfig, OkxError>>,
    {
        let mut current = self.current.lock().await;
        if let Some(proof) = current.as_ref()
            && proof.sampled_at.elapsed() < OBSERVER_CONFIG_MAX_REUSE_AGE
        {
            return Ok((proof.config.clone(), proof.received_at));
        }
        *current = None;
        let sampled_at = Instant::now();
        let received_at = Utc::now();
        let config = fetch().await?;
        *current = Some(ObservedConfig {
            config: config.clone(),
            received_at,
            sampled_at,
        });
        Ok((config, received_at))
    }
}

#[derive(Clone)]
pub struct AccountBootstrapper {
    api: AccountApi,
    config_cache: Arc<ObserverConfigCache>,
    history: AccountHistoryApi,
    asset: AssetApi,
    trade: TradeApi,
}

impl AccountBootstrapper {
    pub fn new(client: OkxRestClient) -> Self {
        Self {
            api: AccountApi::new(client.clone()),
            config_cache: Arc::new(ObserverConfigCache::default()),
            history: AccountHistoryApi::new(client.clone()),
            asset: AssetApi::new(client.clone()),
            trade: TradeApi::new(client),
        }
    }

    async fn observer_config(&self) -> Result<(AccountConfig, chrono::DateTime<Utc>), OkxError> {
        self.config_cache.read(|| self.api.config()).await
    }

    pub async fn fee_schedule(
        &self,
        rules: &InstrumentRulesSnapshot,
    ) -> Result<FeeScheduleSnapshot, FeeScheduleBootstrapError> {
        let (config, _) = self.observer_config().await?;
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
        let (config, _) = self.observer_config().await?;
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

    pub async fn configured_leverage(
        &self,
        instrument_id: &str,
        margin_mode: MarginMode,
        position_side: &str,
    ) -> Result<String, ConfiguredLeverageBootstrapError> {
        let (config, _) = self.observer_config().await?;
        strict_read_only_permissions(&config.perm)
            .map_err(|_| ConfiguredLeverageBootstrapError::PermissionRejected)?;

        let rows = self.api.leverage(instrument_id, margin_mode).await?;
        select_configured_leverage(
            &rows,
            instrument_id,
            &margin_mode.to_string(),
            position_side,
        )
    }

    pub async fn position_builder_oracle(
        &self,
        request: &PositionBuilderRequest,
    ) -> Result<PositionBuilderSnapshot, AccountBootstrapError> {
        let (config, _) = self.observer_config().await?;
        strict_read_only_permissions(&config.perm)?;
        Ok(self.api.position_builder(request).await?)
    }

    pub async fn account_position_risk_oracle(
        &self,
    ) -> Result<AccountPositionRiskSnapshot, AccountBootstrapError> {
        let (config, _) = self.observer_config().await?;
        strict_read_only_permissions(&config.perm)?;
        Ok(self.api.account_position_risk().await?)
    }

    pub async fn execution_fills(
        &self,
        instrument_type: InstrumentType,
        instrument_id: &str,
        order_id: &str,
        client_order_id: &str,
    ) -> Result<ExecutionFillEvidence, AccountLedgerBootstrapError> {
        let (config, _) = self.observer_config().await?;
        strict_read_only_permissions(&config.perm)
            .map_err(|_| AccountLedgerBootstrapError::PermissionRejected)?;
        let history = self
            .history
            .fills_history_for_order(instrument_type, instrument_id, order_id)
            .await?;
        Ok(normalize_execution_fill_history(
            &instrument_type.to_string(),
            instrument_id,
            order_id,
            client_order_id,
            &history,
        )?)
    }

    pub async fn pending_protective_algos(
        &self,
    ) -> Result<PendingProtectiveAlgoInventory, AccountLedgerBootstrapError> {
        let (config, _) = self.observer_config().await?;
        strict_read_only_permissions(&config.perm)
            .map_err(|_| AccountLedgerBootstrapError::PermissionRejected)?;
        Ok(self.trade.pending_protective_algos().await?)
    }

    /// Complete bounded derivatives algo enumeration for central Risk Census.
    /// It uses the same authenticated observer client and Read permission.
    /// A full page is marked incomplete; no retry, guessed zero or fallback.
    pub async fn pending_derivative_risk_algos(
        &self,
    ) -> Result<PendingDerivativeAlgoRiskInventory, AccountLedgerBootstrapError> {
        let (config, _) = self.observer_config().await?;
        strict_read_only_permissions(&config.perm)
            .map_err(|_| AccountLedgerBootstrapError::PermissionRejected)?;
        Ok(self.trade.pending_derivative_risk_algos().await?)
    }

    pub async fn ledger_facts(
        &self,
        snapshot: &AccountSnapshot,
    ) -> Result<AccountLedgerFacts, AccountLedgerBootstrapError> {
        let (config, config_received_at) = self.observer_config().await?;
        strict_read_only_permissions(&config.perm)
            .map_err(|_| AccountLedgerBootstrapError::PermissionRejected)?;

        let (
            funding_balances,
            positions_swap,
            positions_futures,
            orders_swap,
            orders_futures,
            fills_swap,
            fills_futures,
            bills,
        ) = tokio::try_join!(
            self.asset.funding_balances(),
            self.history.positions_history(InstrumentType::Swap),
            self.history.positions_history(InstrumentType::Futures),
            self.history.orders_history(InstrumentType::Swap),
            self.history.orders_history(InstrumentType::Futures),
            self.history.fills_history(InstrumentType::Swap),
            self.history.fills_history(InstrumentType::Futures),
            self.history.bills_history(),
        )?;

        let source_received_at = config_received_at.to_rfc3339_opts(SecondsFormat::Millis, true);
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
        let (config, config_received_at) = self.observer_config().await?;
        // Use the original authenticated config observation time, not the cache-hit time.
        // Balance/positions/orders are read afterward; no falsely fresh source receipt.
        let source_received_at = config_received_at.to_rfc3339_opts(SecondsFormat::Millis, true);
        let permissions = strict_read_only_permissions(&config.perm)?;

        let (balance, positions, pending_orders) = tokio::try_join!(
            self.api.balance(),
            self.api.positions(),
            self.api.pending_orders(),
        )?;
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

fn select_configured_leverage(
    rows: &[LeverageInfo],
    instrument_id: &str,
    margin_mode: &str,
    position_side: &str,
) -> Result<String, ConfiguredLeverageBootstrapError> {
    let matching = rows
        .iter()
        .filter(|row| {
            row.instrument_id == instrument_id
                && row.margin_mode.eq_ignore_ascii_case(margin_mode)
                && row.position_side.eq_ignore_ascii_case(position_side)
        })
        .collect::<Vec<_>>();
    if matching.len() != 1 {
        return Err(ConfiguredLeverageBootstrapError::Ambiguous {
            instrument: instrument_id.to_owned(),
            margin_mode: margin_mode.to_owned(),
            position_side: position_side.to_owned(),
            found: matching.len(),
        });
    }
    let leverage = matching[0].lever.trim();
    if leverage.is_empty() {
        return Err(ConfiguredLeverageBootstrapError::Empty);
    }
    Ok(leverage.to_owned())
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

    fn leverage_row(
        instrument_id: &str,
        margin_mode: &str,
        position_side: &str,
        leverage: &str,
    ) -> LeverageInfo {
        LeverageInfo {
            instrument_id: instrument_id.to_owned(),
            margin_mode: margin_mode.to_owned(),
            position_side: position_side.to_owned(),
            lever: leverage.to_owned(),
        }
    }

    #[test]
    fn configured_leverage_requires_exact_instrument_mode_and_position_side() {
        let rows = vec![
            leverage_row("DOGE-USDT-SWAP", "cross", "long", "5"),
            leverage_row("DOGE-USDT-SWAP", "cross", "short", "3"),
        ];
        assert_eq!(
            select_configured_leverage(&rows, "DOGE-USDT-SWAP", "cross", "long")
                .expect("long leverage"),
            "5"
        );
        assert_eq!(
            select_configured_leverage(&rows, "DOGE-USDT-SWAP", "cross", "short")
                .expect("short leverage"),
            "3"
        );
    }

    #[test]
    fn configured_leverage_missing_or_duplicate_side_fails_closed() {
        let missing = vec![leverage_row("DOGE-USDT-SWAP", "cross", "long", "5")];
        assert!(matches!(
            select_configured_leverage(&missing, "DOGE-USDT-SWAP", "cross", "short"),
            Err(ConfiguredLeverageBootstrapError::Ambiguous { found: 0, .. })
        ));

        let duplicate = vec![
            leverage_row("DOGE-USDT-SWAP", "cross", "long", "5"),
            leverage_row("DOGE-USDT-SWAP", "cross", "long", "10"),
        ];
        assert!(matches!(
            select_configured_leverage(&duplicate, "DOGE-USDT-SWAP", "cross", "long"),
            Err(ConfiguredLeverageBootstrapError::Ambiguous { found: 2, .. })
        ));

        let empty = vec![leverage_row("DOGE-USDT-SWAP", "cross", "long", "")];
        assert!(matches!(
            select_configured_leverage(&empty, "DOGE-USDT-SWAP", "cross", "long"),
            Err(ConfiguredLeverageBootstrapError::Empty)
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
    #[tokio::test]
    async fn observer_config_is_single_flight_per_account() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let cache = Arc::new(ObserverConfigCache::default());
        let fetches = Arc::new(AtomicUsize::new(0));
        let mut calls = Vec::new();
        for _ in 0..16 {
            let cache = cache.clone();
            let fetches = fetches.clone();
            calls.push(tokio::spawn(async move {
                cache
                    .read(|| async move {
                        fetches.fetch_add(1, Ordering::SeqCst);
                        tokio::task::yield_now().await;
                        Ok::<_, OkxError>(
                            serde_json::from_value::<AccountConfig>(
                                serde_json::json!({"uid":"observer-1","perm":"read_only"}),
                            )
                            .unwrap(),
                        )
                    })
                    .await
                    .expect("shared config")
            }));
        }
        let mut timestamps = Vec::new();
        for call in calls {
            let (config, received_at) = call.await.expect("join");
            assert_eq!(config.uid, "observer-1");
            timestamps.push(received_at);
        }
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        assert!(timestamps.iter().all(|t| *t == timestamps[0]));
    }

    #[tokio::test]
    async fn observer_config_expiry_refreshes_and_denies_stale_fallback() {
        let cache = ObserverConfigCache::default();
        cache
            .read(|| async {
                Ok::<_, OkxError>(
                    serde_json::from_value::<AccountConfig>(
                        serde_json::json!({"uid":"observer-1","perm":"read_only"}),
                    )
                    .unwrap(),
                )
            })
            .await
            .expect("initial config");
        // Expiry is deterministic; no wall-clock sleeps or loose timing assertions.
        {
            let mut current = cache.current.lock().await;
            current.as_mut().expect("entry").sampled_at = Instant::now() - Duration::from_secs(3);
        }
        assert!(
            cache
                .read(|| async {
                    Err::<AccountConfig, _>(OkxError::Config("authentication unavailable".into()))
                })
                .await
                .is_err()
        );
        assert!(cache.current.lock().await.is_none());
        let (changed, _) = cache
            .read(|| async {
                Ok::<_, OkxError>(
                    serde_json::from_value::<AccountConfig>(
                        serde_json::json!({"uid":"observer-1","perm":"trade"}),
                    )
                    .unwrap(),
                )
            })
            .await
            .expect("changed config");
        assert!(strict_read_only_permissions(&changed.perm).is_err());
    }
}

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{client::OkxRestClient, config::Region, error::OkxError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum InstrumentType {
    Swap,
    Futures,
}

impl fmt::Display for InstrumentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Swap => f.write_str("SWAP"),
            Self::Futures => f.write_str("FUTURES"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MarginMode {
    Cross,
    Isolated,
}

impl fmt::Display for MarginMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cross => f.write_str("cross"),
            Self::Isolated => f.write_str("isolated"),
        }
    }
}

impl std::str::FromStr for MarginMode {
    type Err = OkxError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "cross" => Ok(Self::Cross),
            "isolated" => Ok(Self::Isolated),
            other => Err(OkxError::Config(format!(
                "unsupported margin mode '{other}'; expected cross or isolated"
            ))),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AccountConfig {
    #[serde(rename = "acctLv", default)]
    pub account_level: String,
    #[serde(rename = "posMode", default)]
    pub position_mode: String,
    #[serde(rename = "uid", default)]
    pub uid: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Instrument {
    #[serde(rename = "instType", default)]
    pub instrument_type: String,
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(rename = "instFamily", default)]
    pub instrument_family: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub lever: String,
    #[serde(rename = "ctType", default)]
    pub contract_type: String,
    #[serde(rename = "groupId", default)]
    pub fee_group_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LeverageInfo {
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(rename = "mgnMode", default)]
    pub margin_mode: String,
    #[serde(rename = "posSide", default)]
    pub position_side: String,
    #[serde(default)]
    pub lever: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FeeGroup {
    #[serde(rename = "groupId", default)]
    pub group_id: String,
    #[serde(default)]
    pub maker: String,
    #[serde(default)]
    pub taker: String,
    #[serde(rename = "elpMaker", default)]
    pub elp_maker: String,
    #[serde(rename = "rpiMaker", default)]
    pub rpi_maker: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FeeRate {
    #[serde(default)]
    pub level: String,
    #[serde(rename = "instType", default)]
    pub instrument_type: String,
    #[serde(rename = "ruleType", default)]
    pub rule_type: String,
    #[serde(default)]
    pub maker: String,
    #[serde(default)]
    pub taker: String,
    #[serde(rename = "makerU", default)]
    pub maker_usdt: String,
    #[serde(rename = "takerU", default)]
    pub taker_usdt: String,
    #[serde(rename = "feeGroup", default)]
    pub fee_groups: Vec<FeeGroup>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BalanceSnapshot {
    #[serde(rename = "totalEq", default)]
    pub total_equity: String,
    #[serde(default)]
    pub details: Vec<BalanceDetail>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BalanceDetail {
    #[serde(default)]
    pub ccy: String,
    #[serde(rename = "eq", default)]
    pub equity: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Position {
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(default)]
    pub pos: String,
    #[serde(rename = "posSide", default)]
    pub position_side: String,
    #[serde(rename = "mgnMode", default)]
    pub margin_mode: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DerivativeAvailability {
    pub instrument_type: InstrumentType,
    pub instrument_count: usize,
    pub sample_instruments: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountCapabilities {
    pub authenticated: bool,
    pub region: Region,
    pub demo: bool,
    pub rest_base_url: &'static str,
    pub account_level: String,
    pub account_mode: String,
    pub position_mode: String,
    pub derivatives: Vec<DerivativeAvailability>,
    pub requested_instrument: String,
    pub requested_instrument_available: bool,
    pub requested_instrument_max_leverage: Option<String>,
    pub requested_fee_group_id: Option<String>,
    pub configured_leverage: Vec<LeverageInfo>,
    pub fee_rates: Vec<FeeRate>,
    pub non_zero_balance_currencies: usize,
    pub open_positions: usize,
    pub warnings: Vec<String>,
}

#[derive(Clone)]
pub struct AccountApi {
    client: OkxRestClient,
}

impl AccountApi {
    pub fn new(client: OkxRestClient) -> Self {
        Self { client }
    }

    pub async fn config(&self) -> Result<AccountConfig, OkxError> {
        self.client
            .private_get::<AccountConfig>("/api/v5/account/config", &[])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| OkxError::Config("OKX returned no account configuration".to_owned()))
    }

    pub async fn instruments(
        &self,
        instrument_type: InstrumentType,
    ) -> Result<Vec<Instrument>, OkxError> {
        self.client
            .private_get(
                "/api/v5/account/instruments",
                &[("instType", instrument_type.to_string())],
            )
            .await
    }

    pub async fn leverage(
        &self,
        instrument_id: &str,
        margin_mode: MarginMode,
    ) -> Result<Vec<LeverageInfo>, OkxError> {
        self.client
            .private_get(
                "/api/v5/account/leverage-info",
                &[
                    ("instId", instrument_id.to_owned()),
                    ("mgnMode", margin_mode.to_string()),
                ],
            )
            .await
    }

    pub async fn fee_rates(
        &self,
        instrument_type: InstrumentType,
        group_id: Option<&str>,
    ) -> Result<Vec<FeeRate>, OkxError> {
        let mut params = vec![("instType", instrument_type.to_string())];
        if let Some(group_id) = group_id.filter(|value| !value.is_empty()) {
            params.push(("groupId", group_id.to_owned()));
        }

        self.client
            .private_get("/api/v5/account/trade-fee", &params)
            .await
    }

    pub async fn balance(&self) -> Result<BalanceSnapshot, OkxError> {
        self.client
            .private_get::<BalanceSnapshot>("/api/v5/account/balance", &[])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| OkxError::Config("OKX returned no account balance snapshot".to_owned()))
    }

    pub async fn positions(&self) -> Result<Vec<Position>, OkxError> {
        self.client
            .private_get("/api/v5/account/positions", &[])
            .await
    }

    pub async fn probe_capabilities(
        &self,
        requested_instrument: &str,
        margin_mode: MarginMode,
    ) -> Result<AccountCapabilities, OkxError> {
        let environment = self.client.environment();
        let config = self.config().await?;

        let swap = self.instruments(InstrumentType::Swap).await?;
        let futures = self.instruments(InstrumentType::Futures).await?;
        let balance = self.balance().await?;
        let positions = self.positions().await?;

        let mut derivatives = Vec::new();
        if !swap.is_empty() {
            derivatives.push(availability(InstrumentType::Swap, &swap));
        }
        if !futures.is_empty() {
            derivatives.push(availability(InstrumentType::Futures, &futures));
        }

        let selected = swap
            .iter()
            .map(|instrument| (InstrumentType::Swap, instrument))
            .chain(
                futures
                    .iter()
                    .map(|instrument| (InstrumentType::Futures, instrument)),
            )
            .find(|(_, instrument)| instrument.instrument_id == requested_instrument);

        let mut warnings = Vec::new();
        let mut configured_leverage = Vec::new();
        let mut fee_rates = Vec::new();
        let mut requested_instrument_max_leverage = None;
        let mut requested_fee_group_id = None;

        if let Some((instrument_type, instrument)) = selected {
            requested_instrument_max_leverage =
                non_empty(&instrument.lever).map(ToOwned::to_owned);
            requested_fee_group_id =
                non_empty(&instrument.fee_group_id).map(ToOwned::to_owned);

            match self.leverage(requested_instrument, margin_mode).await {
                Ok(value) => configured_leverage = value,
                Err(error) => warnings.push(format!("leverage probe unavailable: {error}")),
            }

            match self
                .fee_rates(instrument_type, requested_fee_group_id.as_deref())
                .await
            {
                Ok(value) => fee_rates = value,
                Err(error) => warnings.push(format!("fee-rate probe unavailable: {error}")),
            }
        } else {
            warnings.push(format!(
                "requested instrument '{requested_instrument}' is not available to this account"
            ));
        }

        Ok(AccountCapabilities {
            authenticated: true,
            region: environment.region,
            demo: environment.demo,
            rest_base_url: environment.rest_base_url(),
            account_level: config.account_level.clone(),
            account_mode: account_mode_name(&config.account_level).to_owned(),
            position_mode: config.position_mode,
            derivatives,
            requested_instrument: requested_instrument.to_owned(),
            requested_instrument_available: selected.is_some(),
            requested_instrument_max_leverage,
            requested_fee_group_id,
            configured_leverage,
            fee_rates,
            non_zero_balance_currencies: balance.details.len(),
            open_positions: positions.len(),
            warnings,
        })
    }
}

fn availability(
    instrument_type: InstrumentType,
    instruments: &[Instrument],
) -> DerivativeAvailability {
    DerivativeAvailability {
        instrument_type,
        instrument_count: instruments.len(),
        sample_instruments: instruments
            .iter()
            .filter(|instrument| instrument.state == "live")
            .take(5)
            .map(|instrument| instrument.instrument_id.clone())
            .collect(),
    }
}

fn non_empty(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

fn account_mode_name(account_level: &str) -> &'static str {
    match account_level {
        "1" => "spot",
        "2" => "futures",
        "3" => "multi_currency_margin",
        "4" => "portfolio_margin",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::account_mode_name;

    #[test]
    fn maps_documented_account_levels() {
        assert_eq!(account_mode_name("1"), "spot");
        assert_eq!(account_mode_name("2"), "futures");
        assert_eq!(account_mode_name("3"), "multi_currency_margin");
        assert_eq!(account_mode_name("4"), "portfolio_margin");
        assert_eq!(account_mode_name(""), "unknown");
    }
}

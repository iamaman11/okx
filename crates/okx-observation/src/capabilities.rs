use okx_api::{
    account::{account_mode_name, account_type_name},
    AccountConfig, LeverageInfo,
};
use serde::Serialize;
use thiserror::Error;

use crate::{FeeScheduleSnapshot, InstrumentRulesSnapshot, InstrumentSpec};

pub const TRADING_CAPABILITIES_SCHEMA_V1: &str = "okx.trading-capabilities/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TradingAccountCapabilities {
    pub account_level_code: String,
    pub account_mode: String,
    pub position_mode: String,
    pub account_type_code: String,
    pub account_type: String,
    pub is_subaccount: bool,
    pub account_uid_fingerprint: String,
    pub api_key_permissions: Vec<String>,
    pub api_key_ip_bound: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TradingInstrumentCapabilities {
    pub available_to_account: bool,
    pub account_max_leverage: Option<String>,
    pub rules: InstrumentSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConfiguredLeverage {
    pub instrument_id: String,
    pub margin_mode: String,
    pub position_side: Option<String>,
    pub leverage: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TradingCapabilitiesSnapshot {
    pub schema: String,
    pub source_received_at: String,
    pub reference_generation: String,
    pub account: TradingAccountCapabilities,
    pub instrument: TradingInstrumentCapabilities,
    pub requested_margin_mode: String,
    pub configured_leverage: Vec<ConfiguredLeverage>,
    pub fee_schedule: Option<FeeScheduleSnapshot>,
    pub warnings: Vec<String>,
}

#[derive(Debug)]
pub struct TradingCapabilitiesInput {
    pub source_received_at: String,
    pub config: AccountConfig,
    pub api_key_permissions: Vec<String>,
    pub rules: InstrumentRulesSnapshot,
    pub available_to_account: bool,
    pub account_max_leverage: Option<String>,
    pub requested_margin_mode: String,
    pub configured_leverage: Vec<LeverageInfo>,
    pub fee_schedule: Option<FeeScheduleSnapshot>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TradingCapabilitiesError {
    #[error("trading capabilities source timestamp is empty")]
    EmptySourceTimestamp,

    #[error("account config is missing required field '{0}'")]
    MissingAccountConfig(&'static str),

    #[error("requested margin mode '{0}' is unsupported")]
    UnsupportedMarginMode(String),

    #[error("configured leverage row is missing required field '{0}'")]
    MissingLeverageField(&'static str),

    #[error("configured leverage row margin mode '{actual}' does not match requested '{expected}'")]
    LeverageMarginModeMismatch { expected: String, actual: String },

    #[error("fee schedule instrument '{actual}' does not match requested instrument '{expected}'")]
    FeeInstrumentMismatch { expected: String, actual: String },

    #[error("fee schedule is not exact for the requested instrument")]
    InexactFeeSchedule,
}

impl TradingCapabilitiesSnapshot {
    pub fn from_input(
        input: TradingCapabilitiesInput,
    ) -> Result<Self, TradingCapabilitiesError> {
        if input.source_received_at.trim().is_empty() {
            return Err(TradingCapabilitiesError::EmptySourceTimestamp);
        }

        let account_level_code = require_config("acctLv", input.config.account_level)?;
        let position_mode = require_config("posMode", input.config.position_mode)?;
        let account_type_code = require_config("type", input.config.account_type)?;
        let uid = require_config("uid", input.config.uid)?;

        let requested_margin_mode = input.requested_margin_mode.trim().to_ascii_lowercase();
        if !matches!(requested_margin_mode.as_str(), "cross" | "isolated") {
            return Err(TradingCapabilitiesError::UnsupportedMarginMode(
                input.requested_margin_mode,
            ));
        }

        let mut permissions = input
            .api_key_permissions
            .into_iter()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>();
        permissions.sort();
        permissions.dedup();

        let configured_leverage = input
            .configured_leverage
            .into_iter()
            .map(|row| normalize_leverage(row, &requested_margin_mode))
            .collect::<Result<Vec<_>, _>>()?;

        let requested_instrument = input.rules.instrument.instrument_id.clone();
        if let Some(fee) = input.fee_schedule.as_ref() {
            if fee.instrument_id != requested_instrument {
                return Err(TradingCapabilitiesError::FeeInstrumentMismatch {
                    expected: requested_instrument.clone(),
                    actual: fee.instrument_id.clone(),
                });
            }
            if !fee.exact_for_instrument {
                return Err(TradingCapabilitiesError::InexactFeeSchedule);
            }
        }

        let is_subaccount = !input.config.main_uid.trim().is_empty()
            && uid != input.config.main_uid;

        Ok(Self {
            schema: TRADING_CAPABILITIES_SCHEMA_V1.to_owned(),
            source_received_at: input.source_received_at,
            reference_generation: input.rules.reference_generation,
            account: TradingAccountCapabilities {
                account_level_code: account_level_code.clone(),
                account_mode: account_mode_name(&account_level_code).to_owned(),
                position_mode,
                account_type_code: account_type_code.clone(),
                account_type: account_type_name(&account_type_code).to_owned(),
                is_subaccount,
                account_uid_fingerprint: okx_api::account_uid_fingerprint(&uid),
                api_key_permissions: permissions,
                api_key_ip_bound: !input.config.ip.trim().is_empty(),
            },
            instrument: TradingInstrumentCapabilities {
                available_to_account: input.available_to_account,
                account_max_leverage: input
                    .account_max_leverage
                    .and_then(non_empty_owned),
                rules: input.rules.instrument,
            },
            requested_margin_mode,
            configured_leverage,
            fee_schedule: input.fee_schedule,
            warnings: input.warnings,
        })
    }
}

fn normalize_leverage(
    row: LeverageInfo,
    expected_margin_mode: &str,
) -> Result<ConfiguredLeverage, TradingCapabilitiesError> {
    let instrument_id = require_leverage("instId", row.instrument_id)?;
    let margin_mode = require_leverage("mgnMode", row.margin_mode)?.to_ascii_lowercase();
    if margin_mode != expected_margin_mode {
        return Err(TradingCapabilitiesError::LeverageMarginModeMismatch {
            expected: expected_margin_mode.to_owned(),
            actual: margin_mode,
        });
    }
    let leverage = require_leverage("lever", row.lever)?;

    Ok(ConfiguredLeverage {
        instrument_id,
        margin_mode,
        position_side: non_empty_owned(row.position_side),
        leverage,
    })
}

fn require_config(
    field: &'static str,
    value: String,
) -> Result<String, TradingCapabilitiesError> {
    if value.trim().is_empty() {
        Err(TradingCapabilitiesError::MissingAccountConfig(field))
    } else {
        Ok(value)
    }
}

fn require_leverage(
    field: &'static str,
    value: String,
) -> Result<String, TradingCapabilitiesError> {
    if value.trim().is_empty() {
        Err(TradingCapabilitiesError::MissingLeverageField(field))
    } else {
        Ok(value)
    }
}

fn non_empty_owned(value: String) -> Option<String> {
    (!value.trim().is_empty()).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_api::InstrumentType;
    use crate::{FundingRequirement, FeeScheduleInput, FeeScheduleSnapshot};

    fn rules() -> InstrumentRulesSnapshot {
        InstrumentRulesSnapshot {
            reference_generation: "sha256:reference".to_owned(),
            source_received_at: "2026-09-30T18:30:00.000Z".to_owned(),
            instrument: InstrumentSpec {
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                instrument_type: InstrumentType::Swap,
                instrument_family: Some("DOGE-USDT".to_owned()),
                underlying: Some("DOGE-USDT".to_owned()),
                state: "live".to_owned(),
                rule_type: Some("normal".to_owned()),
                funding_requirement: FundingRequirement::Required,
                base_currency: Some("DOGE".to_owned()),
                quote_currency: Some("USDT".to_owned()),
                settle_currency: Some("USDT".to_owned()),
                tick_size: "0.00001".to_owned(),
                lot_size: "0.01".to_owned(),
                min_size: "0.01".to_owned(),
                max_limit_size: None,
                max_market_size: None,
                max_limit_amount: None,
                max_market_amount: None,
                contract_type: Some("linear".to_owned()),
                contract_value: Some("1000".to_owned()),
                contract_value_currency: Some("DOGE".to_owned()),
                fee_group_id: Some("4".to_owned()),
                max_leverage: Some("50".to_owned()),
                list_time_ms: None,
                expiry_time_ms: None,
            },
        }
    }

    fn config() -> AccountConfig {
        AccountConfig {
            account_level: "2".to_owned(),
            position_mode: "long_short_mode".to_owned(),
            uid: "sub-uid".to_owned(),
            main_uid: "main-uid".to_owned(),
            account_type: "1".to_owned(),
            account_stp_mode: "cancel_maker".to_owned(),
            auto_loan: false,
            greeks_type: String::new(),
            fee_type: String::new(),
            label: "observer".to_owned(),
            ip: "203.0.113.10".to_owned(),
            perm: "read_only".to_owned(),
        }
    }

    fn fee() -> FeeScheduleSnapshot {
        FeeScheduleSnapshot::from_input(FeeScheduleInput {
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            reference_generation: "sha256:reference".to_owned(),
            source_received_at: "2026-09-30T18:30:01.000Z".to_owned(),
            exchange_timestamp_ms: "1790793001000".to_owned(),
            level: "Lv1".to_owned(),
            maker_rate: "-0.0002".to_owned(),
            taker_rate: "-0.0005".to_owned(),
            exact_for_instrument: true,
        })
        .expect("fee")
    }

    #[test]
    fn normalizes_account_instrument_leverage_and_exact_fee() {
        let snapshot = TradingCapabilitiesSnapshot::from_input(TradingCapabilitiesInput {
            source_received_at: "2026-09-30T18:30:02.000Z".to_owned(),
            config: config(),
            api_key_permissions: vec!["read_only".to_owned()],
            rules: rules(),
            available_to_account: true,
            account_max_leverage: Some("50".to_owned()),
            requested_margin_mode: "isolated".to_owned(),
            configured_leverage: vec![
                LeverageInfo {
                    instrument_id: "DOGE-USDT-SWAP".to_owned(),
                    margin_mode: "isolated".to_owned(),
                    position_side: "long".to_owned(),
                    lever: "10".to_owned(),
                },
                LeverageInfo {
                    instrument_id: "DOGE-USDT-SWAP".to_owned(),
                    margin_mode: "isolated".to_owned(),
                    position_side: "short".to_owned(),
                    lever: "8".to_owned(),
                },
            ],
            fee_schedule: Some(fee()),
            warnings: Vec::new(),
        })
        .expect("snapshot");

        assert_eq!(snapshot.schema, TRADING_CAPABILITIES_SCHEMA_V1);
        assert_eq!(snapshot.account.account_mode, "futures");
        assert_eq!(snapshot.account.position_mode, "long_short_mode");
        assert!(snapshot.account.is_subaccount);
        assert_eq!(snapshot.account.api_key_permissions, vec!["read_only"]);
        assert!(snapshot.account.api_key_ip_bound);
        assert!(snapshot.instrument.available_to_account);
        assert_eq!(snapshot.instrument.account_max_leverage.as_deref(), Some("50"));
        assert_eq!(snapshot.configured_leverage.len(), 2);
        assert_eq!(
            snapshot.configured_leverage[0].position_side.as_deref(),
            Some("long")
        );
        assert!(snapshot.fee_schedule.is_some());
    }

    #[test]
    fn rejects_wrong_leverage_scope() {
        let error = TradingCapabilitiesSnapshot::from_input(TradingCapabilitiesInput {
            source_received_at: "2026-09-30T18:30:02.000Z".to_owned(),
            config: config(),
            api_key_permissions: vec!["read_only".to_owned()],
            rules: rules(),
            available_to_account: true,
            account_max_leverage: Some("50".to_owned()),
            requested_margin_mode: "cross".to_owned(),
            configured_leverage: vec![LeverageInfo {
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                margin_mode: "isolated".to_owned(),
                position_side: "long".to_owned(),
                lever: "10".to_owned(),
            }],
            fee_schedule: Some(fee()),
            warnings: Vec::new(),
        })
        .expect_err("mismatch");

        assert!(matches!(
            error,
            TradingCapabilitiesError::LeverageMarginModeMismatch { .. }
        ));
    }
}

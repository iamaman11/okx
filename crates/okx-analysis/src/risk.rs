use okx_observation::{AccountPositionState, AccountSnapshot};
use rust_decimal::Decimal;
use serde::Serialize;

use super::{decimal, positive_decimal, AnalysisError, PositionDirection};

pub const ACCOUNT_RISK_ANALYSIS_SCHEMA_V1: &str = "okx.account-risk-analysis/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PositionRiskAnalysis {
    pub instrument_type: String,
    pub instrument_id: String,
    pub direction: PositionDirection,
    pub margin_mode: String,
    pub position_contracts: String,
    pub position_notional_usd: String,
    pub signed_notional_usd: String,
    pub gross_concentration_ratio: String,
    pub configured_leverage: Option<String>,
    pub initial_margin_requirement_usd: Option<String>,
    pub maintenance_margin_requirement_usd: Option<String>,
    pub exchange_margin_ratio: Option<String>,
    pub mark_price: Option<String>,
    pub estimated_liquidation_price: Option<String>,
    pub estimated_liquidation_distance_ratio: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountRiskAnalysis {
    pub schema: String,
    pub account_generation: String,
    pub account_source: String,
    pub account_quality_reason: String,
    pub account_level: String,
    pub position_mode: String,
    pub total_equity_usd: String,
    pub adjusted_equity_usd: Option<String>,
    pub exchange_gross_notional_usd: Option<String>,
    pub gross_position_notional_usd: String,
    pub directional_net_position_notional_usd: String,
    pub gross_exposure_to_equity_ratio: Option<String>,
    pub directional_net_exposure_to_equity_ratio: Option<String>,
    pub initial_margin_requirement_usd: Option<String>,
    pub maintenance_margin_requirement_usd: Option<String>,
    pub exchange_margin_ratio: Option<String>,
    pub source_position_record_count: usize,
    pub open_position_count: usize,
    pub pending_order_count: usize,
    pub positions: Vec<PositionRiskAnalysis>,
}

struct PositionWork {
    output: PositionRiskAnalysis,
    notional: Decimal,
}

pub fn analyze_account_risk(account: &AccountSnapshot) -> Result<AccountRiskAnalysis, AnalysisError> {
    match account.account_level.as_str() {
        "2" | "3" | "4" => {}
        other => return Err(AnalysisError::UnsupportedAccountMode(other.to_owned())),
    }
    match account.position_mode.as_str() {
        "net_mode" | "long_short_mode" => {}
        other => return Err(AnalysisError::UnsupportedPositionMode(other.to_owned())),
    }

    let total_equity = decimal("total_equity_usd", &account.balance.total_equity_usd)?;
    let adjusted_equity = optional_decimal(
        "adjusted_equity_usd",
        account.balance.adjusted_equity_usd.as_deref(),
    )?;
    let exchange_gross_notional = optional_non_negative_decimal(
        "exchange_gross_notional_usd",
        account.balance.notional_usd.as_deref(),
    )?;
    let account_imr = optional_non_negative_decimal(
        "initial_margin_requirement_usd",
        account.balance.initial_margin_requirement_usd.as_deref(),
    )?;
    let account_mmr = optional_non_negative_decimal(
        "maintenance_margin_requirement_usd",
        account.balance.maintenance_margin_requirement_usd.as_deref(),
    )?;
    let account_margin_ratio = optional_non_negative_decimal(
        "exchange_margin_ratio",
        account.balance.margin_ratio.as_deref(),
    )?;

    let mut gross = Decimal::ZERO;
    let mut net = Decimal::ZERO;
    let mut work = Vec::new();

    for position in &account.positions {
        let size = decimal("position_contracts", &position.position)?;
        if size == Decimal::ZERO {
            continue;
        }

        match position.instrument_type.as_str() {
            "SWAP" | "FUTURES" => {}
            other => {
                return Err(AnalysisError::UnsupportedPositionType {
                    instrument_id: position.instrument_id.clone(),
                    instrument_type: other.to_owned(),
                });
            }
        }

        let direction = direction_for(position, &account.position_mode, size)?;
        let notional_text = position
            .notional_usd
            .as_deref()
            .ok_or_else(|| AnalysisError::MissingPositionNotional(position.instrument_id.clone()))?;
        let notional = positive_decimal("position_notional_usd", notional_text)?;

        let signed_notional = match direction {
            PositionDirection::Long => notional,
            PositionDirection::Short => -notional,
        };
        gross += notional;
        net += signed_notional;

        let configured_leverage =
            optional_positive_decimal("configured_leverage", position.leverage.as_deref())?;
        let imr = optional_non_negative_decimal(
            "position_initial_margin_requirement_usd",
            position.initial_margin_requirement.as_deref(),
        )?;
        let mmr = optional_non_negative_decimal(
            "position_maintenance_margin_requirement_usd",
            position.maintenance_margin_requirement.as_deref(),
        )?;
        let margin_ratio = optional_non_negative_decimal(
            "position_exchange_margin_ratio",
            position.margin_ratio.as_deref(),
        )?;
        let mark_price = optional_positive_decimal("position_mark_price", position.mark_price.as_deref())?;
        let liquidation_price = optional_positive_decimal(
            "estimated_liquidation_price",
            position.liquidation_price.as_deref(),
        )?;
        let liquidation_distance =
            estimated_liquidation_distance(&position.instrument_id, direction, mark_price, liquidation_price)?;

        work.push(PositionWork {
            output: PositionRiskAnalysis {
                instrument_type: position.instrument_type.clone(),
                instrument_id: position.instrument_id.clone(),
                direction,
                margin_mode: position.margin_mode.clone(),
                position_contracts: size.normalize().to_string(),
                position_notional_usd: notional.normalize().to_string(),
                signed_notional_usd: signed_notional.normalize().to_string(),
                gross_concentration_ratio: String::new(),
                configured_leverage: normalized(configured_leverage),
                initial_margin_requirement_usd: normalized(imr),
                maintenance_margin_requirement_usd: normalized(mmr),
                exchange_margin_ratio: normalized(margin_ratio),
                mark_price: normalized(mark_price),
                estimated_liquidation_price: normalized(liquidation_price),
                estimated_liquidation_distance_ratio: normalized(liquidation_distance),
            },
            notional,
        });
    }

    for item in &mut work {
        item.output.gross_concentration_ratio = if gross > Decimal::ZERO {
            (item.notional / gross).normalize().to_string()
        } else {
            "0".to_owned()
        };
    }

    let gross_to_equity = (total_equity > Decimal::ZERO)
        .then(|| (gross / total_equity).normalize().to_string());
    let net_to_equity = (total_equity > Decimal::ZERO)
        .then(|| (net / total_equity).normalize().to_string());

    let positions = work.into_iter().map(|item| item.output).collect::<Vec<_>>();

    Ok(AccountRiskAnalysis {
        schema: ACCOUNT_RISK_ANALYSIS_SCHEMA_V1.to_owned(),
        account_generation: account.account_generation.clone(),
        account_source: account.source.clone(),
        account_quality_reason: account.quality_reason.clone(),
        account_level: account.account_level.clone(),
        position_mode: account.position_mode.clone(),
        total_equity_usd: total_equity.normalize().to_string(),
        adjusted_equity_usd: normalized(adjusted_equity),
        exchange_gross_notional_usd: normalized(exchange_gross_notional),
        gross_position_notional_usd: gross.normalize().to_string(),
        directional_net_position_notional_usd: net.normalize().to_string(),
        gross_exposure_to_equity_ratio: gross_to_equity,
        directional_net_exposure_to_equity_ratio: net_to_equity,
        initial_margin_requirement_usd: normalized(account_imr),
        maintenance_margin_requirement_usd: normalized(account_mmr),
        exchange_margin_ratio: normalized(account_margin_ratio),
        source_position_record_count: account.positions.len(),
        open_position_count: positions.len(),
        pending_order_count: account.pending_orders.len(),
        positions,
    })
}

fn direction_for(
    position: &AccountPositionState,
    account_position_mode: &str,
    size: Decimal,
) -> Result<PositionDirection, AnalysisError> {
    match (account_position_mode, position.position_side.as_str()) {
        ("net_mode", "net") => {
            if size > Decimal::ZERO {
                Ok(PositionDirection::Long)
            } else {
                Ok(PositionDirection::Short)
            }
        }
        ("long_short_mode", "long") if size > Decimal::ZERO => Ok(PositionDirection::Long),
        ("long_short_mode", "short") if size > Decimal::ZERO => Ok(PositionDirection::Short),
        ("long_short_mode", "long" | "short") => Err(AnalysisError::InconsistentPositionDirection {
            instrument_id: position.instrument_id.clone(),
            position_side: position.position_side.clone(),
            position: position.position.clone(),
        }),
        (_, side) => Err(AnalysisError::UnsupportedPositionSide {
            instrument_id: position.instrument_id.clone(),
            position_side: side.to_owned(),
        }),
    }
}

fn estimated_liquidation_distance(
    instrument_id: &str,
    direction: PositionDirection,
    mark_price: Option<Decimal>,
    liquidation_price: Option<Decimal>,
) -> Result<Option<Decimal>, AnalysisError> {
    let (Some(mark), Some(liquidation)) = (mark_price, liquidation_price) else {
        return Ok(None);
    };

    let adverse_distance = match direction {
        PositionDirection::Long => mark - liquidation,
        PositionDirection::Short => liquidation - mark,
    };
    if adverse_distance < Decimal::ZERO {
        return Err(AnalysisError::InconsistentLiquidationPrice(
            instrument_id.to_owned(),
        ));
    }

    Ok(Some(adverse_distance / mark))
}

fn optional_decimal(
    field: &'static str,
    value: Option<&str>,
) -> Result<Option<Decimal>, AnalysisError> {
    value.map(|value| decimal(field, value)).transpose()
}

fn optional_positive_decimal(
    field: &'static str,
    value: Option<&str>,
) -> Result<Option<Decimal>, AnalysisError> {
    value.map(|value| positive_decimal(field, value)).transpose()
}

fn optional_non_negative_decimal(
    field: &'static str,
    value: Option<&str>,
) -> Result<Option<Decimal>, AnalysisError> {
    value
        .map(|value| {
            let value = decimal(field, value)?;
            if value < Decimal::ZERO {
                return Err(AnalysisError::Negative(field));
            }
            Ok(value)
        })
        .transpose()
}

fn normalized(value: Option<Decimal>) -> Option<String> {
    value.map(|value| value.normalize().to_string())
}

#[cfg(test)]
mod tests {
    use okx_observation::{AccountBalanceState, AccountPositionState, AccountSnapshot};

    use super::*;

    fn account(position_mode: &str, positions: Vec<AccountPositionState>) -> AccountSnapshot {
        AccountSnapshot {
            schema: "okx.account-snapshot/v2".to_owned(),
            source: "okx_private_rest_plus_ws".to_owned(),
            source_received_at: "2026-09-27T20:00:00Z".to_owned(),
            account_generation: "sha256:test".to_owned(),
            quality_reason: "M4_PRIVATE_REST_WS_CONVERGED".to_owned(),
            private_ws_connected: true,
            private_ws_generation: Some(7),
            private_ws_connection_fingerprint: Some("fingerprint".to_owned()),
            private_ws_last_inbound_ms: Some(1),
            private_ws_events_applied: Some(1),
            account_level: "3".to_owned(),
            position_mode: position_mode.to_owned(),
            account_type: "0".to_owned(),
            account_uid_fingerprint: "uid".to_owned(),
            api_key_permissions: vec!["read_only".to_owned()],
            balance: AccountBalanceState {
                total_equity_usd: "500".to_owned(),
                adjusted_equity_usd: Some("480".to_owned()),
                isolated_equity_usd: None,
                initial_margin_requirement_usd: Some("100".to_owned()),
                maintenance_margin_requirement_usd: Some("50".to_owned()),
                margin_ratio: Some("9.6".to_owned()),
                notional_usd: Some("1000".to_owned()),
                update_time_ms: Some("1".to_owned()),
                details: Vec::new(),
            },
            positions,
            pending_orders: Vec::new(),
        }
    }

    fn position(
        instrument_id: &str,
        instrument_type: &str,
        side: &str,
        size: &str,
        notional: Option<&str>,
        mark: Option<&str>,
        liquidation: Option<&str>,
    ) -> AccountPositionState {
        AccountPositionState {
            instrument_type: instrument_type.to_owned(),
            instrument_id: instrument_id.to_owned(),
            position: size.to_owned(),
            position_side: side.to_owned(),
            margin_mode: "cross".to_owned(),
            average_price: Some("90".to_owned()),
            mark_price: mark.map(ToOwned::to_owned),
            liquidation_price: liquidation.map(ToOwned::to_owned),
            unrealized_pnl: Some("0".to_owned()),
            unrealized_pnl_ratio: Some("0".to_owned()),
            leverage: Some("5".to_owned()),
            margin: None,
            initial_margin_requirement: Some("10".to_owned()),
            maintenance_margin_requirement: Some("5".to_owned()),
            margin_ratio: Some("10".to_owned()),
            notional_usd: notional.map(ToOwned::to_owned),
            margin_currency: Some("USDT".to_owned()),
            creation_time_ms: Some("1".to_owned()),
            update_time_ms: Some("2".to_owned()),
        }
    }

    #[test]
    fn long_short_mode_aggregates_gross_net_concentration_and_liquidation_distance() {
        let snapshot = account(
            "long_short_mode",
            vec![
                position(
                    "BTC-USDT-SWAP",
                    "SWAP",
                    "long",
                    "2",
                    Some("600"),
                    Some("100"),
                    Some("80"),
                ),
                position(
                    "ETH-USDT-SWAP",
                    "SWAP",
                    "short",
                    "3",
                    Some("400"),
                    Some("100"),
                    Some("125"),
                ),
            ],
        );

        let risk = analyze_account_risk(&snapshot).expect("risk");

        assert_eq!(risk.gross_position_notional_usd, "1000");
        assert_eq!(risk.directional_net_position_notional_usd, "200");
        assert_eq!(risk.gross_exposure_to_equity_ratio.as_deref(), Some("2"));
        assert_eq!(
            risk.directional_net_exposure_to_equity_ratio.as_deref(),
            Some("0.4")
        );
        assert_eq!(risk.positions[0].gross_concentration_ratio, "0.6");
        assert_eq!(risk.positions[1].gross_concentration_ratio, "0.4");
        assert_eq!(
            risk.positions[0]
                .estimated_liquidation_distance_ratio
                .as_deref(),
            Some("0.2")
        );
        assert_eq!(
            risk.positions[1]
                .estimated_liquidation_distance_ratio
                .as_deref(),
            Some("0.25")
        );
    }

    #[test]
    fn net_mode_uses_position_sign_for_direction() {
        let snapshot = account(
            "net_mode",
            vec![position(
                "DOGE-USDT-SWAP",
                "SWAP",
                "net",
                "-4",
                Some("300"),
                Some("0.2"),
                Some("0.3"),
            )],
        );

        let risk = analyze_account_risk(&snapshot).expect("risk");

        assert_eq!(risk.positions[0].direction, PositionDirection::Short);
        assert_eq!(risk.directional_net_position_notional_usd, "-300");
        assert_eq!(
            risk.positions[0]
                .estimated_liquidation_distance_ratio
                .as_deref(),
            Some("0.5")
        );
    }

    #[test]
    fn unsupported_non_zero_position_type_fails_closed() {
        let snapshot = account(
            "net_mode",
            vec![position(
                "BTC-USD-OPTION",
                "OPTION",
                "net",
                "1",
                Some("100"),
                Some("10"),
                None,
            )],
        );

        assert!(matches!(
            analyze_account_risk(&snapshot),
            Err(AnalysisError::UnsupportedPositionType { .. })
        ));
    }

    #[test]
    fn missing_notional_for_non_zero_position_fails_closed() {
        let snapshot = account(
            "net_mode",
            vec![position(
                "DOGE-USDT-SWAP",
                "SWAP",
                "net",
                "1",
                None,
                Some("0.2"),
                None,
            )],
        );

        assert!(matches!(
            analyze_account_risk(&snapshot),
            Err(AnalysisError::MissingPositionNotional(_))
        ));
    }

    #[test]
    fn zero_size_funded_record_is_allowed_without_notional() {
        let snapshot = account(
            "net_mode",
            vec![position(
                "DOGE-USDT-SWAP",
                "SWAP",
                "net",
                "0",
                None,
                None,
                None,
            )],
        );

        let risk = analyze_account_risk(&snapshot).expect("risk");
        assert_eq!(risk.source_position_record_count, 1);
        assert_eq!(risk.open_position_count, 0);
        assert_eq!(risk.gross_position_notional_usd, "0");
        assert!(risk.positions.is_empty());
    }
}

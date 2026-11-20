//! Pure economic research oracle: typed units, settlement currencies and
//! independent portfolios; no venue/account access or durable job mutation.
use super::*;
use okx_protocol::analytics::{EconomicCase, EconomicProduct, EconomicSide};
use okx_research::economics::{
    InverseContract, LinearContract, QuoteAmount, SpotPair, group_quote_by_currency,
};
use rust_decimal::Decimal;
use std::str::FromStr;

fn exact_positive(input: &str) -> Result<Decimal, String> {
    let value = Decimal::from_str(input).map_err(|_| "INVALID_EXACT_AMOUNT")?;
    if value <= Decimal::ZERO {
        return Err("POSITION_OR_PRICE_NOT_POSITIVE".into());
    }
    Ok(value)
}

fn evaluated_case(case: &EconomicCase) -> Result<(QuoteAmount, QuoteAmount, Value), String> {
    let quantity = exact_positive(&case.quantity)?;
    let entry = exact_positive(&case.entry_price)?;
    let exit = exact_positive(&case.exit_price)?;
    let fees = QuoteAmount::parse_exact(&case.fees.currency, &case.fees.amount)?;
    let funding = QuoteAmount::parse_exact(&case.funding.currency, &case.funding.amount)?;
    if fees.value < Decimal::ZERO {
        return Err("INVALID_FEE".into());
    }
    let signed_quantity = if case.side == EconomicSide::Short {
        -quantity
    } else {
        quantity
    };
    let unit_and_contract = match case.product {
        EconomicProduct::Spot => {
            if case.side == EconomicSide::Short {
                return Err("SPOT_SHORT_REQUIRES_BORROW_MODEL".into());
            }
            if case.contract_type.is_some()
                || case.contract_value.is_some()
                || case.contract_value_currency.is_some()
                || case.settle_currency.is_some()
            {
                return Err("SPOT_CONTRACT_FIELDS_UNEXPECTED".into());
            }
            if funding.value != Decimal::ZERO {
                return Err("SPOT_FUNDING_UNSUPPORTED".into());
            }
            let pair = SpotPair::from_instrument(&case.instrument)?;
            if funding.currency != pair.quote_currency {
                return Err("CURRENCY_MISMATCH_FX_REQUIRED".into());
            }
            let pnl = pair.pnl(quantity, entry, exit, &fees)?;
            (
                pnl.gross_quote,
                pnl.net_quote,
                json!({"position_unit":"base_asset","base_asset":pair.base_asset,
                    "quote_currency":pair.quote_currency,"base_quantity_exact":quantity.normalize().to_string()}),
            )
        }
        EconomicProduct::LinearSwap => {
            let contract = LinearContract::from_reference(
                &case.instrument,
                case.contract_type.as_deref(),
                case.contract_value.as_deref(),
                case.contract_value_currency.as_deref(),
                case.settle_currency.as_deref(),
            )?;
            let pnl = contract.pnl_signed(signed_quantity, entry, exit, &fees, &funding)?;
            (
                pnl.gross_quote,
                pnl.net_quote,
                json!({"position_unit":"contracts","base_asset":contract.base_asset,
                    "quote_currency":contract.quote_currency,
                    "base_quantity_exact":pnl.base_quantity.normalize().to_string(),
                    "contract_value_exact":contract.base_per_contract.normalize().to_string(),
                    "contract_value_unit":"base_asset_per_contract"}),
            )
        }
        EconomicProduct::InverseSwap => {
            let contract = InverseContract::from_reference(
                &case.instrument,
                case.contract_type.as_deref(),
                case.contract_value.as_deref(),
                case.contract_value_currency.as_deref(),
                case.settle_currency.as_deref(),
            )?;
            let pnl = contract.pnl_signed(signed_quantity, entry, exit, &fees, &funding)?;
            (
                pnl.gross,
                pnl.net,
                json!({"position_unit":"contracts","base_asset":contract.base_asset,
                    "quote_currency":contract.quote_currency,
                    "contract_value_exact":contract.face_quote_per_contract.normalize().to_string(),
                    "contract_value_unit":"quote_currency_per_contract",
                    "settlement_unit":"base_asset"}),
            )
        }
        EconomicProduct::Option => return Err("UNSUPPORTED_OPTION_ECONOMICS_MODEL".into()),
    };
    Ok(unit_and_contract)
}

pub(super) fn evaluate(
    cases: Vec<EconomicCase>,
    data_mode: ResearchDataMode,
) -> Result<Value, String> {
    // A reproducible fingerprint is not proof of historical rules or prices.
    let fingerprint = okx_research::canonical_sha256(&(
        "okx.research.counterfactual-economics/v1",
        &cases,
        data_mode,
    ))
    .map_err(|e| e.to_string())?;
    let effects = json!({"public_reads":0,"research_writes":0,"governance_writes":0,
        "finance_writes":0,"venue_posts":0,"job_created":false});
    if data_mode == ResearchDataMode::HistoricalObserved {
        return Ok(
            json!({"schema":"okx.research.economics/v1","status":"blocked",
            "reason_code":"HISTORICAL_RULES_AND_FEES_UNAVAILABLE",
            "input_fingerprint":fingerprint,"rows":[],"quality":"unavailable",
            "historical_rules_verified":false,"effects":effects}),
        );
    }
    let mut totals = Vec::new();
    let mut rows = Vec::with_capacity(cases.len());
    for (index, case) in cases.iter().enumerate() {
        let outcome = evaluated_case(case);
        let row = match outcome {
            Ok((gross, net, details)) => {
                totals.push(net.clone());
                json!({"index":index,"instrument":case.instrument,"product":case.product,
                    "side":case.side,"state":"completed",
                    "quantity_exact":case.quantity,
                    "gross":{"currency":gross.currency,"amount_exact":gross.value.normalize().to_string()},
                    "net":{"currency":net.currency,"amount_exact":net.value.normalize().to_string()},
                    "units":details,"reference_time_basis":"caller_supplied_unverified",
                    "historical_rules_verified":false,"historical_fees_verified":false})
            }
            Err(reason) => json!({"index":index,"instrument":case.instrument,
                "product":case.product,"side":case.side,"state":"blocked",
                "reason_code":reason,"historical_rules_verified":false}),
        };
        rows.push(row);
    }
    let grouped = group_quote_by_currency(&totals)?;
    let completed = totals.len();
    Ok(
        json!({"schema":"okx.research.economics/v1","kind":"economic_scenario",
        "status":"completed","mode":"counterfactual",
        "input_fingerprint":fingerprint,"rows":rows,
        "summary":{"quality":if completed == cases.len(){"complete"}else{"partial"},
            "completed":completed,"blocked":cases.len()-completed,
            "totals_by_settlement_currency":grouped.iter().map(|group|json!({
                "currency":group.currency,"amount_exact":group.amount.normalize().to_string(),
                "contributors":group.contributors})).collect::<Vec<_>>(),
            "aggregation":"independent_positions_no_fx_no_joint_risk"},
        "model_basis":"caller_supplied_counterfactual_only",
        "historical_rules_verified":false,"historical_fees_verified":false,
        "portfolio_equity":null,"joint_portfolio_risk":null,"effects":effects}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counterfactual_case(product: &str) -> Value {
        match product {
            "spot" => json!({"instrument":"BTC-USDT","product":"spot", "side":"long",
                "quantity":"2","entry_price":"100","exit_price":"110",
                "fees":{"currency":"USDT","amount":"1"},
                "funding":{"currency":"USDT","amount":"0"}}),
            "linear_swap" => json!({"instrument":"BTC-USDT-SWAP", "product":"linear_swap",
                "side":"long","quantity":"3","entry_price":"100","exit_price":"110",
                "contract_type":"linear","contract_value":"0.1",
                "contract_value_currency":"BTC","settle_currency":"USDT",
                "fees":{"currency":"USDT","amount":"0"},
                "funding":{"currency":"USDT","amount":"0"}}),
            "inverse_swap" => json!({"instrument":"BTC-USD-SWAP", "product":"inverse_swap",
                "side":"long","quantity":"1","entry_price":"100","exit_price":"110",
                "contract_type":"inverse","contract_value":"100",
                "contract_value_currency":"USD","settle_currency":"BTC",
                "fees":{"currency":"BTC","amount":"0"},
                "funding":{"currency":"BTC","amount":"0"}}),
            "option" => json!({"instrument":"BTC-USD-260101-100-C","product":"option",
                "side":"long","quantity":"1","entry_price":"100","exit_price":"110",
                "fees":{"currency":"USD","amount":"0"},
                "funding":{"currency":"USD","amount":"0"}}),
            _ => panic!("unexpected product"),
        }
    }

    fn request(cases: Vec<Value>, mode: &str) -> ResearchRequest {
        serde_json::from_value(json!({"action":"economics","cases":cases,"data_mode":mode}))
            .unwrap()
    }

    #[test]
    fn t02_t09_public_economics_is_exact_per_product_and_keeps_currencies_separate() {
        let request = request(
            vec![
                counterfactual_case("spot"),
                counterfactual_case("linear_swap"),
                counterfactual_case("inverse_swap"),
                counterfactual_case("option"),
            ],
            "counterfactual",
        );
        assert!(request.valid());
        let cloned =
            serde_json::from_value::<ResearchRequest>(serde_json::to_value(&request).unwrap())
                .unwrap();
        assert_eq!(request, cloned);
        let ResearchRequest::Economics { cases, data_mode } = request else {
            panic!()
        };
        let result = evaluate(cases.clone(), data_mode).unwrap();
        assert_eq!(result["rows"][0]["units"]["position_unit"], "base_asset");
        assert_eq!(result["rows"][0]["net"]["amount_exact"], "19");
        assert_eq!(result["rows"][1]["units"]["position_unit"], "contracts");
        assert_eq!(result["rows"][1]["units"]["base_quantity_exact"], "0.3");
        assert_eq!(result["rows"][1]["gross"]["amount_exact"], "3");
        assert_eq!(
            result["rows"][2]["units"]["contract_value_unit"],
            "quote_currency_per_contract"
        );
        assert_eq!(result["rows"][2]["gross"]["currency"], "BTC");
        assert_eq!(
            result["rows"][2]["gross"]["amount_exact"],
            "0.0909090909090909090909090909"
        );
        assert_eq!(result["rows"][3]["state"], "blocked");
        assert_eq!(
            result["rows"][3]["reason_code"],
            "UNSUPPORTED_OPTION_ECONOMICS_MODEL"
        );
        assert_eq!(result["summary"]["quality"], "partial");
        assert_eq!(
            result["summary"]["totals_by_settlement_currency"][0]["currency"],
            "BTC"
        );
        assert_eq!(
            result["summary"]["totals_by_settlement_currency"][1]["currency"],
            "USDT"
        );
        assert_eq!(
            result["summary"]["totals_by_settlement_currency"][1]["amount_exact"],
            "22"
        );
        assert!(result["portfolio_equity"].is_null());
        assert_eq!(result["effects"]["public_reads"], 0);
        assert_eq!(result["effects"]["finance_writes"], 0);
        assert_eq!(result["effects"]["venue_posts"], 0);
        assert_eq!(result["historical_rules_verified"], false);
        assert_eq!(evaluate(cases.clone(), data_mode).unwrap(), result);
        let mut changed = cases;
        changed[0].exit_price = "111".into();
        assert_ne!(
            evaluate(changed, data_mode).unwrap()["input_fingerprint"],
            result["input_fingerprint"]
        );
    }

    #[test]
    fn t02_t09_mismatched_units_and_missing_models_do_not_contribute_to_totals() {
        let mut wrong_linear = counterfactual_case("linear_swap");
        wrong_linear["contract_value_currency"] = json!("USDT");
        let mut wrong_fee = counterfactual_case("inverse_swap");
        wrong_fee["fees"]["currency"] = json!("USD");
        let mut missing = counterfactual_case("inverse_swap");
        missing.as_object_mut().unwrap().remove("contract_value");
        let mut short_spot = counterfactual_case("spot");
        short_spot["side"] = json!("short");
        let mut funded_spot = counterfactual_case("spot");
        funded_spot["funding"]["amount"] = json!("0.01");
        let request = request(
            vec![wrong_linear, wrong_fee, missing, short_spot, funded_spot],
            "counterfactual",
        );
        let ResearchRequest::Economics { cases, data_mode } = request else {
            panic!()
        };
        let result = evaluate(cases, data_mode).unwrap();
        let reasons = result["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["reason_code"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            reasons,
            [
                "CONTRACT_UNIT_MISMATCH",
                "CURRENCY_MISMATCH_FX_REQUIRED",
                "CONTRACT_MULTIPLIER_MISSING",
                "SPOT_SHORT_REQUIRES_BORROW_MODEL",
                "SPOT_FUNDING_UNSUPPORTED"
            ]
        );
        assert_eq!(result["summary"]["completed"], 0);
        assert_eq!(
            result["summary"]["totals_by_settlement_currency"],
            json!([])
        );
    }

    #[test]
    fn t09_counterfactual_short_and_signed_funding_are_exact() {
        let mut short = counterfactual_case("linear_swap");
        short["side"] = json!("short");
        short["funding"]["amount"] = json!("-0.5");
        let ResearchRequest::Economics { cases, data_mode } =
            request(vec![short], "counterfactual")
        else {
            panic!()
        };
        let result = evaluate(cases, data_mode).unwrap();
        assert_eq!(result["rows"][0]["units"]["base_quantity_exact"], "-0.3");
        assert_eq!(result["rows"][0]["gross"]["amount_exact"], "-3");
        assert_eq!(result["rows"][0]["net"]["amount_exact"], "-2.5");
    }

    #[test]
    fn t09_historical_observed_never_infers_rules_from_caller_assumptions() {
        let ResearchRequest::Economics { cases, data_mode } = request(
            vec![counterfactual_case("linear_swap")],
            "historical_observed",
        ) else {
            panic!()
        };
        let response = evaluate(cases, data_mode).unwrap();
        assert_eq!(response["status"], "blocked");
        assert_eq!(
            response["reason_code"],
            "HISTORICAL_RULES_AND_FEES_UNAVAILABLE"
        );
        assert_eq!(response["effects"]["job_created"], false);
        assert_eq!(response["rows"], json!([]));
    }

    #[tokio::test]
    async fn t11_research_economics_route_creates_no_job_or_market_read() {
        let (root, service) = super::super::artifact_job_fixtures::service();
        let request = request(vec![counterfactual_case("linear_swap")], "counterfactual");
        let reply = service.research(request).await.unwrap();
        assert_eq!(reply["rows"][0]["state"], "completed");
        assert!(service.jobs.snapshot().is_empty());
        assert!(!service.artifacts.evidence_dir().exists());
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }
}

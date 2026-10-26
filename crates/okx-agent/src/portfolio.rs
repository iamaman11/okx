//! Bounded portfolio projections with a stable cross-account page cursor.
use crate::accounts::AccountRuntimeRegistry;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use okx_protocol::trading::{PortfolioAction, PortfolioRequest};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant},
};
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    query: String,
    time: i64,
    account: String,
    id: String,
}
fn history_after(key: &str, cursor: Option<&Cursor>, from: i64) -> (i64, String) {
    match cursor {
        Some(c) if key < c.account.as_str() => (c.time, String::from("\u{10ffff}")),
        Some(c) if key == c.account => (c.time, c.id.clone()),
        Some(c) => (c.time, String::new()),
        None => (from.saturating_sub(1), String::new()),
    }
}
fn hash(v: &impl Serialize) -> Result<String, String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(v).map_err(|e| e.to_string())?)
    ))
}
pub async fn portfolio(
    accounts: &Arc<AccountRuntimeRegistry>,
    request: &PortfolioRequest,
    trade: &crate::trade_service::TradeService,
) -> Result<Value, String> {
    let central_budget = trade.risk_summary().await;
    let from = i64::try_from(request.start_ms).map_err(|_| "RANGE_OVERFLOW")?;
    let through = i64::try_from(request.end_ms).map_err(|_| "RANGE_OVERFLOW")?;
    let mut keys = BTreeSet::new();
    let mut resolved = Vec::new();
    for account in &request.accounts {
        let r = accounts
            .resolve(account)
            .await
            .ok_or("ACCOUNT_UNAVAILABLE")?;
        let key = r.configuration.account_key.wire();
        if !keys.insert(key.clone()) {
            return Err("DUPLICATE_ACCOUNT".into());
        }
        resolved.push((key, r));
    }
    resolved.sort_by(|a, b| a.0.cmp(&b.0));
    let query = hash(&(
        keys,
        from,
        through,
        &request.strategies,
        &request.currencies,
        &request.action,
    ))?;
    let cursor = request
        .cursor
        .as_ref()
        .map(|s| {
            URL_SAFE_NO_PAD
                .decode(s)
                .map_err(|_| "INVALID_CURSOR")
                .and_then(|bytes| {
                    serde_json::from_slice::<Cursor>(&bytes).map_err(|_| "INVALID_CURSOR")
                })
        })
        .transpose()?;
    if cursor
        .as_ref()
        .is_some_and(|c| c.query != query || c.time < from || c.time > through)
    {
        return Err("CURSOR_QUERY_MISMATCH".into());
    }
    if request.action == PortfolioAction::History
        && cursor
            .as_ref()
            .is_some_and(|c| !resolved.iter().any(|(key, _)| key == &c.account))
    {
        return Err("CURSOR_QUERY_MISMATCH".into());
    }
    let mut rows = Vec::new();
    let mut totals: BTreeMap<(String, String, String), Decimal> = BTreeMap::new();
    let mut attribution: BTreeMap<(String, String, Option<String>, String, String), Decimal> =
        BTreeMap::new();
    let mut account_names = BTreeMap::new();
    let mut readiness = Vec::new();
    let mut navigation = Vec::new();
    for (key, r) in resolved {
        let environment = match r.configuration.account_key.environment {
            okx_protocol::accounts::Environment::Real => "real",
            okx_protocol::accounts::Environment::Demo => "demo",
        };
        let alias = r.configuration.alias.clone();
        account_names.insert(key.clone(), alias.clone());
        let instances = request.strategies.clone().unwrap_or_default();
        let currencies = request.currencies.clone().unwrap_or_default();
        let history = request.action == PortfolioAction::History;
        let limit = usize::from(request.limit).min(50) + 1;
        let (after_time, after_id) = history_after(&key, cursor.as_ref(), from);
        let page = r
            .execution
            .store
            .call(Instant::now() + Duration::from_secs(5), move |s| {
                let health = s.financial_risk(Utc::now().timestamp_millis(), None)?;
                if history {
                    Ok((
                        s.finance_history_filtered(
                            from,
                            through,
                            after_time,
                            &after_id,
                            limit,
                            &instances,
                            &currencies,
                        )?,
                        BTreeMap::new(),
                        health,
                        Vec::new(),
                        s.meta().initialized_at,
                    ))
                } else {
                    Ok((
                        Vec::new(),
                        s.finance_attribution_filtered(from, through, &instances, &currencies)?,
                        health,
                        s.nav_summary(from, through, &instances, &currencies)?,
                        s.meta().initialized_at,
                    ))
                }
            })
            .await
            .map_err(|e| e.to_string())?;
        for nav in page.3 {
            navigation.push(nav_projection(&key, &alias, nav, from, through, page.4));
        }
        readiness.push(json!({"account":key,"account_name":alias,"t0":page.4,"financial_risk":page.2}));
        for entry in page.0 {
            rows.push((entry.time,key.clone(),entry.entry_id.clone(),json!({"time":entry.time,"account":key,"account_name":alias,"component":entry.component,"amount":entry.amount.normalize().to_string(),"currency":entry.currency,"strategy_instance":entry.instance_id,"reference":entry.entry_id})));
        }
        accumulate_finance(&key, environment, page.1, &mut totals, &mut attribution)?;
    }
    if request.action == PortfolioAction::History {
        rows.sort_by(|a, b| (&a.0, &a.1, &a.2).cmp(&(&b.0, &b.1, &b.2)));
        let more = rows.len() > usize::from(request.limit).min(50);
        rows.truncate(usize::from(request.limit).min(50));
        let mut output = Vec::new();
        let mut last = None;
        let mut bytes = 0;
        let mut truncated = false;
        for (time, account, id, value) in rows {
            let size = serde_json::to_vec(&value).map_err(|e| e.to_string())?.len();
            if bytes + size > 7000 {
                truncated = true;
                break;
            }
            bytes += size;
            last = Some(Cursor {
                query: query.clone(),
                time,
                account,
                id,
            });
            output.push(value);
        }
        let next = if more || truncated {
            last.map(|c| serde_json::to_vec(&c).map(|v| URL_SAFE_NO_PAD.encode(v)))
                .transpose()
                .map_err(|e| e.to_string())?
        } else {
            None
        };
        Ok(
            json!({"rows":output,"next_cursor":next,"central_budget":central_budget.as_ref().ok(),"central_budget_error":central_budget.as_ref().err(),"as_of_ms":Utc::now().timestamp_millis()}),
        )
    } else {
        let mut all=totals.into_iter().map(|((environment,currency,component),amount)|json!({"kind":"component","environment":environment,"currency":currency,"component":component,"amount":amount.normalize().to_string()})).collect::<Vec<_>>();
        all.extend(attribution.into_iter().map(
            |((account, environment, instance, currency, component), amount)| {
                json!({
                    "kind":"account_component",
                    "account_name":account_names.get(&account).map(String::as_str),
                    "account":account,
                    "environment":environment,
                    "strategy_instance":instance,
                    "currency":currency,
                    "component":component,
                    "amount":amount.normalize().to_string(),
                    "view":"detail_of_environment_component",
                    "additive_with_rollup":false
                })
            },
        ));
        all.extend(navigation.iter().cloned());
        if cursor.as_ref().is_some_and(|c| c.account != "summary") {
            return Err("INVALID_CURSOR".into());
        }
        let last_id = cursor.as_ref().map(|c| c.id.as_str()).unwrap_or("");
        let mut ordered = all
            .into_iter()
            .map(|v| (summary_key(&v), v))
            .filter(|(key, _)| key.as_str() > last_id)
            .collect::<Vec<_>>();
        ordered.sort_by(|a, b| a.0.cmp(&b.0));
        let total = ordered.len();
        let mut output = Vec::new();
        let mut bytes = 0;
        let mut last = None;
        for (key, value) in ordered.into_iter().take(usize::from(request.limit).min(50)) {
            let size = serde_json::to_vec(&value).map_err(|e| e.to_string())?.len();
            if bytes + size > 5500 {
                break;
            }
            bytes += size;
            last = Some(key);
            output.push(value);
        }
        let next = if output.len() < total {
            last.map(|id| {
                serde_json::to_vec(&Cursor {
                    query,
                    time: from,
                    account: "summary".into(),
                    id,
                })
                .map(|v| URL_SAFE_NO_PAD.encode(v))
            })
            .transpose()
            .map_err(|e| e.to_string())?
        } else {
            None
        };
        let complete = summary_is_complete(
            &readiness,
            &navigation,
            central_budget.is_ok(),
            next.is_none(),
        );
        Ok(
            json!({"rows":output,"next_cursor":next,"pagination_complete":next.is_none(),"central_budget":central_budget.as_ref().ok(),"central_budget_error":central_budget.as_ref().err(),"account_count":readiness.len(),"quality":if complete{"complete"}else{"partial"},"diagnostics":summary_diagnostics(readiness, &navigation, central_budget.as_ref().err()),"as_of_ms":Utc::now().timestamp_millis()}),
        )
    }
}

type AccountFinance = BTreeMap<(Option<String>, String, String), Decimal>;
type EnvironmentTotals = BTreeMap<(String, String, String), Decimal>;
type AccountDetails = BTreeMap<(String, String, Option<String>, String, String), Decimal>;

/// A detail is a partition of account economic facts, not an additional
/// amount to add to the environment rollup or to NAV.
fn accumulate_finance(
    account: &str,
    environment: &str,
    scoped: AccountFinance,
    totals: &mut EnvironmentTotals,
    details: &mut AccountDetails,
) -> Result<(), String> {
    for ((instance, currency, component), amount) in scoped {
        let sum = totals
            .entry((environment.to_owned(), currency.clone(), component.clone()))
            .or_default();
        *sum = sum.checked_add(amount).ok_or("MONEY_OVERFLOW")?;
        details.insert(
            (
                account.to_owned(),
                environment.to_owned(),
                instance,
                currency,
                component,
            ),
            amount,
        );
    }
    Ok(())
}

fn nav_projection(
    account: &str,
    account_name: &str,
    nav: okx_execution::NavWindow,
    from: i64,
    through: i64,
    t0: i64,
) -> Value {
    let coverage_reason = match nav.coverage.as_str() {
        "COMPLETE" => Value::Null,
        "UNAVAILABLE_BEFORE_T0" => json!("UNAVAILABLE_BEFORE_T0"),
        "PARTIAL" if nav.scope_id.starts_with("strategy:") => {
            json!("STRATEGY_CAPITAL_PROVENANCE_MISSING")
        }
        "PARTIAL" => json!("NAV_SOURCE_INCOMPLETE"),
        _ => json!("NAV_COVERAGE_INVALID"),
    };
    json!({
        "kind":"nav",
        "account":account,
        "account_name":account_name,
        "pnl_interpretation":"equity_change_less_external_flows_includes_valuation_fx_not_necessarily_realized_trade_pnl",
        "requested_range":{"start_ms":from,"end_ms":through},
        "covered_range":{"start_ms":nav.opening_as_of,"end_ms":nav.current_as_of},
        "t0":t0,
        "coverage_reason":coverage_reason,
        "as_of_ms":nav.current_as_of,
        "window":nav
    })
}

fn summary_is_complete(
    readiness: &[Value],
    navigation: &[Value],
    central_budget_available: bool,
    pagination_complete: bool,
) -> bool {
    !navigation.is_empty()
        && central_budget_available
        && pagination_complete
        && readiness
            .iter()
            .all(|r| r["financial_risk"]["nav_complete"] == true)
        && navigation
            .iter()
            .all(|row| row["window"]["coverage"] == "COMPLETE")
}

fn summary_diagnostics(
    readiness: Vec<Value>,
    navigation: &[Value],
    central_budget_error: Option<&String>,
) -> Vec<Value> {
    let mut diagnostics = readiness
        .into_iter()
        .filter(|r| r["financial_risk"]["nav_complete"] != true)
        .take(4)
        .collect::<Vec<_>>();
    diagnostics.extend(
        navigation
            .iter()
            .filter(|row| row["window"]["coverage"] != "COMPLETE")
            .take(4)
            .map(|row| {
                json!({"account":row["account"],"coverage_reason":row["coverage_reason"],"requested_range":row["requested_range"],"covered_range":row["covered_range"],"t0":row["t0"],"as_of_ms":row["as_of_ms"]})
            }),
    );
    if central_budget_error.is_some() {
        diagnostics.push(json!({"central_budget":"UNAVAILABLE"}));
    }
    diagnostics.truncate(4);
    diagnostics
}
fn summary_key(value: &Value) -> String {
    if value["kind"] == "component" {
        format!(
            "component|{}|{}|{}",
            value["environment"].as_str().unwrap_or_default(),
            value["currency"].as_str().unwrap_or_default(),
            value["component"].as_str().unwrap_or_default()
        )
    } else if value["kind"] == "account_component" {
        format!(
            "account_component|{}|{}|{}|{}|{}",
            value["account"].as_str().unwrap_or_default(),
            value["environment"].as_str().unwrap_or_default(),
            value["strategy_instance"].as_str().unwrap_or(""),
            value["currency"].as_str().unwrap_or_default(),
            value["component"].as_str().unwrap_or_default()
        )
    } else {
        format!(
            "nav|{}|{}|{}",
            value["account"].as_str().unwrap_or_default(),
            value["window"]["scope_id"].as_str().unwrap_or_default(),
            value["window"]["currency"].as_str().unwrap_or_default()
        )
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use okx_execution::{AccountMeta, AccountStore, FinanceEntry, IngestPage, OpeningSnapshot};
    fn store(account: &str) -> AccountStore {
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path =
            std::env::temp_dir().join(format!("portfolio-{:x}.sqlite", u128::from_le_bytes(nonce)));
        let opening = OpeningSnapshot {
            opening_equity_usd: Decimal::from(100),
            source_ref: "flat".into(),
            observed_at: 1,
            positions: 0,
            orders: 0,
            conditional_orders: 0,
            liabilities: Decimal::ZERO,
            balances: vec![("USD".into(), Decimal::from(100))],
        };
        let meta = AccountMeta {
            account_key: account.into(),
            installation_id: "fixture-installation".into(),
            initialized_at: 1,
            opening_source: "flat".into(),
            initialize_operation_id: "fixture-operation".into(),
            release_id: "fixture".into(),
        };
        let mut store = AccountStore::initialize_flat(path, meta, &opening).unwrap();
        let mut page = IngestPage::default();
        for (time, id) in [(9, "before"), (10, "a"), (10, "b"), (11, "after")] {
            page.finance.push(FinanceEntry {
                entry_id: id.into(),
                economic_event_key: format!("{account}:{id}"),
                component: "FEE".into(),
                allocation_leg: "account".into(),
                amount: -Decimal::ONE,
                currency: "USD".into(),
                instance_id: None,
                source_ref: "source".into(),
                time,
                reverses_entry_id: None,
            });
        }
        store.ingest_page(&page).unwrap();
        store
    }
    fn fixture_scoped_finance(account: &mut AccountStore, amounts: [i64; 3]) -> AccountFinance {
        use okx_execution::StrategyInstance;
        for id in ["momentum", "mean_reversion"] {
            account
                .mutate_instance(
                    &format!("create-{id}"),
                    "definition-fixture",
                    "create",
                    &StrategyInstance {
                        instance_id: id.into(),
                        definition_id: id.into(),
                        definition_version: "1".into(),
                        parameters: "{}".into(),
                        enabled: false,
                        policy_revision: 1,
                        budget: Decimal::from(10),
                        last_decision_key: None,
                    },
                    None,
                    2,
                )
                .unwrap();
        }
        let mut page = IngestPage::default();
        for (n, (instance, amount)) in [
            (Some("momentum"), amounts[0]),
            (Some("mean_reversion"), amounts[1]),
            (None, amounts[2]),
        ]
        .into_iter()
        .enumerate()
        {
            page.finance.push(FinanceEntry {
                entry_id: format!("fact-{n}"),
                economic_event_key: format!("economic-{n}"),
                component: "REALIZED_PNL".into(),
                allocation_leg: instance.unwrap_or("unattributed").into(),
                amount: Decimal::from(amount),
                currency: "USD".into(),
                instance_id: instance.map(str::to_owned),
                source_ref: "verified-exchange-fixture".into(),
                time: 12,
                reverses_entry_id: None,
            });
        }
        account.ingest_page(&page).unwrap();
        // Replayed venue evidence is idempotent, not a second financial leg.
        account.ingest_page(&page).unwrap();
        account
            .finance_attribution_filtered(12, 12, &[], &["USD".into()])
            .unwrap()
    }

    #[test]
    fn two_accounts_two_strategies_conserve_real_demo_and_unattributed_money() {
        let mut real = store("okx:real:uid-a");
        let mut demo = store("okx:demo:uid-b");
        let mut totals = EnvironmentTotals::new();
        let mut details = AccountDetails::new();
        accumulate_finance(
            "okx:real:uid-a",
            "real",
            fixture_scoped_finance(&mut real, [10, 5, -2]),
            &mut totals,
            &mut details,
        )
        .unwrap();
        accumulate_finance(
            "okx:demo:uid-b",
            "demo",
            fixture_scoped_finance(&mut demo, [7, -1, 4]),
            &mut totals,
            &mut details,
        )
        .unwrap();
        assert_eq!(totals.len(), 2);
        assert_eq!(
            totals[&("real".into(), "USD".into(), "REALIZED_PNL".into())],
            Decimal::from(13)
        );
        assert_eq!(
            totals[&("demo".into(), "USD".into(), "REALIZED_PNL".into())],
            Decimal::from(10)
        );
        assert_eq!(details.len(), 6);
        for (env, expected) in [("real", Decimal::from(13)), ("demo", Decimal::from(10))] {
            let detail_sum: Decimal = details
                .iter()
                .filter(|((_, e, _, _, _), _)| e == env)
                .map(|(_, amount)| amount)
                .copied()
                .sum();
            assert_eq!(detail_sum, expected);
        }
        assert_eq!(
            details[&(
                "okx:real:uid-a".into(),
                "real".into(),
                None,
                "USD".into(),
                "REALIZED_PNL".into(),
            )],
            Decimal::from(-2)
        );
        // Risk budgets are not financial facts or additional capital.
        assert_eq!(
            real.instance("momentum").unwrap().unwrap().budget,
            Decimal::from(10)
        );
        assert_eq!(
            demo.instance("momentum").unwrap().unwrap().budget,
            Decimal::from(10)
        );
        assert_eq!(
            real.finance_totals_filtered(12, 12, &[], &[]).unwrap()
                [&("USD".into(), "REALIZED_PNL".into())],
            Decimal::from(13)
        );
        assert_eq!(
            demo.finance_totals_filtered(12, 12, &[], &[]).unwrap()
                [&("USD".into(), "REALIZED_PNL".into())],
            Decimal::from(10)
        );
    }

    #[test]
    fn cross_account_cursor_preserves_all_equal_timestamp_rows() {
        let accounts = vec![("A", store("A")), ("B", store("B")), ("C", store("C"))];
        let mut cursor = None;
        let mut all = Vec::new();
        for _ in 0..16 {
            let mut rows = Vec::new();
            for (key, store) in &accounts {
                let (t, id) = history_after(key, cursor.as_ref(), 1);
                for e in store
                    .finance_history_filtered(1, 20, t, &id, 3, &[], &[])
                    .unwrap()
                {
                    rows.push((e.time, key.to_string(), e.entry_id));
                }
            }
            rows.sort();
            rows.truncate(2);
            if rows.is_empty() {
                break;
            }
            let last = rows.last().unwrap();
            cursor = Some(Cursor {
                query: "query".into(),
                time: last.0,
                account: last.1.clone(),
                id: last.2.clone(),
            });
            all.extend(rows);
        }
        assert_eq!(all.len(), 12);
        assert_eq!(all.iter().collect::<BTreeSet<_>>().len(), 12);
        let same = all
            .iter()
            .filter(|r| r.0 == 10)
            .map(|r| format!("{}:{}", r.1, r.2))
            .collect::<Vec<_>>();
        assert_eq!(same, vec!["A:a", "A:b", "B:a", "B:b", "C:a", "C:b"]);
    }
    #[test]
    fn cross_account_resume_never_skips_later_accounts_at_boundary() {
        let c = Cursor {
            query: "q".into(),
            time: 10,
            account: "B".into(),
            id: "b".into(),
        };
        assert_eq!(history_after("A", Some(&c), 1), (10, "\u{10ffff}".into()));
        assert_eq!(history_after("B", Some(&c), 1), (10, "b".into()));
        assert_eq!(history_after("C", Some(&c), 1), (10, String::new()));
    }

    #[test]
    fn real_and_demo_components_have_distinct_summary_keys() {
        let real =
            json!({"kind":"component","environment":"real","currency":"USD","component":"FEE"});
        let demo =
            json!({"kind":"component","environment":"demo","currency":"USD","component":"FEE"});
        assert_ne!(summary_key(&real), summary_key(&demo));
    }

    #[test]
    fn unknown_strategy_capital_is_an_explicit_partial_reason() {
        let nav = okx_execution::NavWindow {
            scope_id: "strategy:manual".into(),
            currency: "USD".into(),
            opening_equity: None,
            current_equity: None,
            flows: None,
            pnl: None,
            opening_as_of: None,
            current_as_of: None,
            coverage: "PARTIAL".into(),
        };
        let row = nav_projection("okx:real:verified", "Acceptance", nav, 1, 10, 1);
        assert_eq!(
            row["coverage_reason"],
            "STRATEGY_CAPITAL_PROVENANCE_MISSING"
        );
        assert!(row["window"]["opening_equity"].is_null());
        assert!(row["window"]["pnl"].is_null());
    }
    #[test]
    fn portfolio_before_t0_is_partial() {
        let readiness = vec![json!({"financial_risk":{"nav_complete":true}})];
        let navigation = vec![json!({
            "kind":"nav",
            "account":"A",
            "window":{"coverage":"UNAVAILABLE_BEFORE_T0"}
        })];
        assert!(!summary_is_complete(&readiness, &navigation, true, true));
    }

    #[test]
    fn summary_requires_complete_requested_window_and_fresh_account_nav() {
        let readiness = vec![json!({"financial_risk":{"nav_complete":true}})];
        let navigation = vec![json!({
            "kind":"nav",
            "account":"A",
            "window":{"coverage":"COMPLETE"}
        })];
        assert!(summary_is_complete(&readiness, &navigation, true, true));
    }
}

pub fn scenario(r: &okx_protocol::trading::PortfolioScenarioRequest) -> Result<Value, String> {
    use rust_decimal::Decimal;
    if !r.valid() {
        return Err("INVALID_PORTFOLIO_SCENARIO".into());
    }
    let decimal = |v: &str| {
        v.parse::<Decimal>()
            .map_err(|_| "INVALID_DECIMAL".to_owned())
    };
    let equity = decimal(&r.equity_usd)?;
    let (mut gross, mut margin, mut move_pnl, mut liquidity) =
        (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);
    let mut vols = Vec::new();
    let mut rows = Vec::new();
    for p in &r.positions {
        let n = decimal(&p.notional_usd)?;
        let m = decimal(&p.margin_usd)?;
        let side: i64 = if p.side == okx_protocol::trading::PositionSide::Long {
            1
        } else {
            -1
        };
        let pnl = n
            .checked_mul(Decimal::from(side * i64::from(p.price_shock_bps)))
            .and_then(|x| x.checked_div(Decimal::from(10000)))
            .ok_or("SCENARIO_OVERFLOW")?;
        let impact = n
            .checked_mul(Decimal::from(p.liquidity_cost_bps))
            .and_then(|x| x.checked_div(Decimal::from(10000)))
            .ok_or("SCENARIO_OVERFLOW")?;
        let vol = n
            .checked_mul(Decimal::from(p.volatility_bps))
            .and_then(|x| x.checked_div(Decimal::from(10000)))
            .ok_or("SCENARIO_OVERFLOW")?;
        gross = gross.checked_add(n).ok_or("SCENARIO_OVERFLOW")?;
        margin = margin.checked_add(m).ok_or("SCENARIO_OVERFLOW")?;
        move_pnl = move_pnl.checked_add(pnl).ok_or("SCENARIO_OVERFLOW")?;
        liquidity = liquidity.checked_add(impact).ok_or("SCENARIO_OVERFLOW")?;
        vols.push(
            vol.to_string()
                .parse::<f64>()
                .map_err(|_| "SCENARIO_OVERFLOW")?,
        );
        rows.push(json!({"instrument":p.instrument,"side":p.side,"price_pnl_usd":pnl.normalize().to_string(),
    "liquidity_loss_usd":impact.normalize().to_string(),"source":"caller_supplied"}));
    }
    let size = vols.len();
    let mut correlation = vec![vec![0.0; size]; size];
    for (i, row) in correlation.iter_mut().enumerate() {
        row[i] = 1.0
    }
    for c in &r.correlations {
        correlation[c.left][c.right] = c.rho;
        correlation[c.right][c.left] = c.rho
    }
    let mut root = vec![vec![0.0_f64; size]; size];
    for i in 0..size {
        for j in 0..=i {
            let mut value = correlation[i][j];
            for (left, right) in root[i][..j].iter().zip(root[j][..j].iter()) {
                value -= left * right;
            }
            if i == j {
                if value < -1e-8 {
                    return Err("CORRELATION_NOT_PSD".into());
                }
                root[i][j] = value.max(0.0).sqrt()
            } else if root[j][j] > 1e-10 {
                root[i][j] = value / root[j][j]
            } else if value.abs() > 1e-8 {
                return Err("CORRELATION_NOT_PSD".into());
            }
        }
    }
    let mut variance = 0.0;
    for i in 0..size {
        for j in 0..size {
            variance += vols[i] * vols[j] * correlation[i][j]
        }
    }
    if !variance.is_finite() || variance < -1e-7 {
        return Err("CORRELATION_NOT_PSD".into());
    }
    let net = move_pnl.checked_sub(liquidity).ok_or("SCENARIO_OVERFLOW")?;
    let stressed = equity.checked_add(net).ok_or("SCENARIO_OVERFLOW")?;
    let loss = if net < Decimal::ZERO {
        -net
    } else {
        Decimal::ZERO
    };
    Ok(
        json!({"schema":"okx.portfolio.scenario/v1","quality":"assumed","execution":"virtual_read_only",
  "exchange_post":false,"venue_margin_verified":false,"provenance":"caller_supplied",
  "gross_notional_usd":gross.normalize().to_string(),"margin_usd":margin.normalize().to_string(),
  "stress_price_pnl_usd":move_pnl.normalize().to_string(),"liquidity_loss_usd":liquidity.normalize().to_string(),
  "net_stress_pnl_usd":net.normalize().to_string(),"stressed_equity_usd":stressed.normalize().to_string(),
  "drawdown_ratio":loss.checked_div(equity).ok_or("SCENARIO_OVERFLOW")?.normalize().to_string(),
  "gross_leverage_ratio":gross.checked_div(equity).ok_or("SCENARIO_OVERFLOW")?.normalize().to_string(),
  "correlated_volatility_usd":variance.max(0.0).sqrt(),"correlation_basis":"caller_supplied_psd",
  "positions":rows}),
    )
}
#[cfg(test)]
mod scenario_tests {
    use super::*;
    use okx_protocol::trading::{
        PortfolioScenarioRequest, PositionSide, ScenarioCorrelation, ScenarioPosition,
    };
    #[test]
    fn virtual_risk_has_no_exchange_side_effects_and_requires_psd() {
        let p = ScenarioPosition {
            instrument: "BTC-USDT-SWAP".into(),
            side: PositionSide::Long,
            notional_usd: "100".into(),
            margin_usd: "10".into(),
            price_shock_bps: -1000,
            volatility_bps: 200,
            liquidity_cost_bps: 50,
        };
        let mut r = PortfolioScenarioRequest {
            equity_usd: "50".into(),
            positions: vec![p.clone(), p],
            correlations: vec![ScenarioCorrelation {
                left: 0,
                right: 1,
                rho: 0.6,
            }],
        };
        let v = scenario(&r).unwrap();
        assert_eq!(v["stress_price_pnl_usd"], "-20");
        assert_eq!(v["exchange_post"], false);
        r.correlations[0].rho = 1.2;
        assert!(scenario(&r).is_err());
    }
}

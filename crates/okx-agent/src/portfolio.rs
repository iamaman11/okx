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
    let mut totals: BTreeMap<(String, String), Decimal> = BTreeMap::new();
    let mut readiness = Vec::new();
    let mut navigation = Vec::new();
    for (key, r) in resolved {
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
                    ))
                } else {
                    Ok((
                        Vec::new(),
                        s.finance_totals_filtered(from, through, &instances, &currencies)?,
                        health,
                        s.nav_summary(from, through, &instances, &currencies)?,
                    ))
                }
            })
            .await
            .map_err(|e| e.to_string())?;
        for nav in page.3 {
            navigation.push(json!({"kind":"nav","account":key,"window":nav}));
        }
        readiness.push(json!({"account":key,"financial_risk":page.2}));
        for entry in page.0 {
            rows.push((entry.time,key.clone(),entry.entry_id.clone(),json!({"time":entry.time,"account":key,"component":entry.component,"amount":entry.amount.normalize().to_string(),"currency":entry.currency,"strategy_instance":entry.instance_id,"reference":entry.entry_id})));
        }
        for (k, amount) in page.1 {
            let sum = totals.entry(k).or_default();
            *sum = sum.checked_add(amount).ok_or("MONEY_OVERFLOW")?;
        }
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
        let mut all=totals.into_iter().map(|((currency,component),amount)|json!({"kind":"component","currency":currency,"component":component,"amount":amount.normalize().to_string()})).collect::<Vec<_>>();
        all.extend(navigation);
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
        let complete = readiness
            .iter()
            .all(|r| r["financial_risk"]["nav_complete"] == true);
        Ok(
            json!({"rows":output,"next_cursor":next,"central_budget":central_budget.as_ref().ok(),"central_budget_error":central_budget.as_ref().err(),"account_count":readiness.len(),"quality":if complete{"complete"}else{"partial"},"diagnostics":readiness.into_iter().filter(|r|r["financial_risk"]["nav_complete"]!=true).take(4).collect::<Vec<_>>(),"as_of_ms":Utc::now().timestamp_millis()}),
        )
    }
}
fn summary_key(value: &Value) -> String {
    if value["kind"] == "component" {
        format!(
            "component|{}|{}",
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
}

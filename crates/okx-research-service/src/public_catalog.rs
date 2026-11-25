use crate::independent_source::{IndependentOkxPublicSource, PublicGet};
use okx_research_domain::observation_model::{
    FundingRequirement, InstrumentSpec, InstrumentType, UpcomingRuleChange,
};
use okx_research_protocol::{analytics::MarketRequest, edge::InstrumentTypeFilter};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

const MAX_CATALOG_AGE_MS: i64 = 5_000;

fn catalog_fresh(observed_ms: i64, now_ms: i64) -> bool {
    observed_ms > 0 && (0..=MAX_CATALOG_AGE_MS).contains(&now_ms.saturating_sub(observed_ms))
}

#[derive(Deserialize)]
struct Change {
    param: String,
    #[serde(rename = "newValue")]
    new_value: String,
    #[serde(rename = "effTime")]
    effective_time_ms: String,
}
#[derive(Deserialize)]
struct PublicInstrument {
    #[serde(rename = "instType")]
    kind: String,
    #[serde(rename = "instId", default)]
    id: String,
    state: String,
    #[serde(rename = "tickSz", default)]
    tick: String,
    #[serde(rename = "lotSz", default)]
    lot: String,
    #[serde(rename = "minSz", default)]
    min: String,
    #[serde(rename = "instFamily", default)]
    family: String,
    #[serde(rename = "uly", default)]
    underlying: String,
    #[serde(rename = "ruleType", default)]
    rule: String,
    #[serde(rename = "baseCcy", default)]
    base: String,
    #[serde(rename = "quoteCcy", default)]
    quote: String,
    #[serde(rename = "settleCcy", default)]
    settle: String,
    #[serde(rename = "ctValCcy", default)]
    contract_currency: String,
    #[serde(rename = "maxLmtSz", default)]
    max_limit: String,
    #[serde(rename = "maxMktSz", default)]
    max_market: String,
    #[serde(rename = "maxLmtAmt", default)]
    max_limit_amount: String,
    #[serde(rename = "maxMktAmt", default)]
    max_market_amount: String,
    #[serde(rename = "ctType", default)]
    contract_type: String,
    #[serde(rename = "ctVal", default)]
    contract_value: String,
    #[serde(rename = "groupId", default)]
    group: String,
    #[serde(default)]
    lever: String,
    #[serde(rename = "listTime", default)]
    list: String,
    #[serde(rename = "expTime", default)]
    expiry: String,
    #[serde(rename = "initPxLmtPct", default)]
    initial_limit: String,
    #[serde(rename = "floatPxLmtPct", default)]
    floating_limit: String,
    #[serde(rename = "maxPxLmtPct", default)]
    maximum_limit: String,
    #[serde(rename = "upcChg", default)]
    changes: Vec<Change>,
}
fn opt(s: String) -> Option<String> {
    if s.trim().is_empty() { None } else { Some(s) }
}
fn parse_catalog(swap: &[u8], futures: &[u8]) -> Result<(Vec<InstrumentSpec>, String), String> {
    let mut instruments = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for (bytes, expected) in [(swap, "SWAP"), (futures, "FUTURES")] {
        let rows: Vec<PublicInstrument> =
            crate::independent_source::rows(bytes, 20_000, "REFERENCE_SOURCE_INVALID")?;
        if rows.is_empty() {
            return Err("REFERENCE_SOURCE_INVALID".into());
        }
        for row in rows {
            if row.kind != expected {
                return Err("REFERENCE_SOURCE_INVALID".into());
            }
            if row.kind == "FUTURES"
                && row.state.trim() == "preopen"
                && row.id.trim().is_empty()
                && !row.family.trim().is_empty()
            {
                continue;
            }
            if row.id.trim().is_empty() || !seen.insert(row.id.clone()) {
                return Err("REFERENCE_SOURCE_INVALID".into());
            }
            if row.state.trim() == "preopen" {
                continue;
            }
            if [&row.state, &row.tick, &row.lot, &row.min]
                .iter()
                .any(|v| v.trim().is_empty())
            {
                return Err("REFERENCE_SOURCE_INVALID".into());
            }
            // Search and universe selection require these facts; accepting absent values would silently omit matches.
            if row.state == "live"
                && (row.settle.trim().is_empty() || row.contract_currency.trim().is_empty())
            {
                return Err("REFERENCE_SOURCE_INVALID".into());
            }
            let kind = if row.kind == "SWAP" {
                InstrumentType::Swap
            } else {
                InstrumentType::Futures
            };
            let funding_requirement = match (kind, row.rule.as_str()) {
                (InstrumentType::Swap, _) | (InstrumentType::Futures, "xperp" | "pre_market") => {
                    FundingRequirement::Required
                }
                (InstrumentType::Futures, "normal") => FundingRequirement::NotApplicable,
                _ => FundingRequirement::Unknown,
            };
            let mut changes = Vec::new();
            for c in row.changes {
                if !matches!(c.param.as_str(), "tickSz" | "minSz" | "maxMktSz")
                    || c.new_value.trim().is_empty()
                    || !c.effective_time_ms.parse::<u64>().is_ok_and(|n| n > 0)
                {
                    return Err("REFERENCE_SOURCE_INVALID".into());
                }
                changes.push(UpcomingRuleChange {
                    param: c.param,
                    new_value: c.new_value,
                    effective_time_ms: c.effective_time_ms,
                });
            }
            changes.sort_by(|a, b| {
                (&a.effective_time_ms, &a.param, &a.new_value).cmp(&(
                    &b.effective_time_ms,
                    &b.param,
                    &b.new_value,
                ))
            });
            let spec = InstrumentSpec {
                instrument_id: row.id,
                instrument_type: kind,
                instrument_family: opt(row.family),
                underlying: opt(row.underlying),
                state: row.state,
                rule_type: opt(row.rule),
                funding_requirement,
                base_currency: opt(row.base),
                quote_currency: opt(row.quote),
                settle_currency: opt(row.settle),
                tick_size: row.tick,
                lot_size: row.lot,
                min_size: row.min,
                max_limit_size: opt(row.max_limit),
                max_market_size: opt(row.max_market),
                max_limit_amount: opt(row.max_limit_amount),
                max_market_amount: opt(row.max_market_amount),
                contract_type: opt(row.contract_type),
                contract_value: opt(row.contract_value),
                contract_value_currency: opt(row.contract_currency),
                fee_group_id: opt(row.group),
                max_leverage: opt(row.lever),
                list_time_ms: opt(row.list),
                expiry_time_ms: opt(row.expiry),
                initial_price_limit_pct: opt(row.initial_limit),
                floating_price_limit_pct: opt(row.floating_limit),
                maximum_price_limit_pct: opt(row.maximum_limit),
                upcoming_rule_changes: changes,
            };
            instruments.insert(spec.instrument_id.clone(), spec);
        }
    }
    let instruments: Vec<InstrumentSpec> = instruments.into_values().collect();
    #[derive(Serialize)]
    struct GenerationInput<'a> {
        schema: &'static str,
        instruments: &'a [InstrumentSpec],
    }
    let encoded = serde_json::to_vec(&GenerationInput {
        schema: "okx.reference-registry/v1",
        instruments: &instruments,
    })
    .map_err(|_| "REFERENCE_SOURCE_INVALID")?;
    Ok((instruments, format!("sha256:{:x}", Sha256::digest(encoded))))
}
pub(crate) struct Catalog {
    pub(crate) instruments: Vec<InstrumentSpec>,
    pub(crate) generation: String,
    pub(crate) observed_at: jiff::Timestamp,
}

pub(crate) async fn catalog(source: &dyn PublicGet) -> Result<Catalog, String> {
    let observed_at = jiff::Timestamp::now();
    let (swap, futures) = tokio::try_join!(
        source.get(
            "/api/v5/public/instruments",
            vec![("instType", "SWAP".into())]
        ),
        source.get(
            "/api/v5/public/instruments",
            vec![("instType", "FUTURES".into())]
        ),
    )?;
    let (rows, generation) = parse_catalog(&swap, &futures)?;
    if !catalog_fresh(
        observed_at.as_millisecond(),
        jiff::Timestamp::now().as_millisecond(),
    ) {
        return Err("REFERENCE_SOURCE_STALE".into());
    }
    Ok(Catalog {
        instruments: rows,
        generation,
        observed_at,
    })
}
pub(crate) async fn find_instruments(
    source: &IndependentOkxPublicSource,
    asset: &str,
    settle_currency: Option<&str>,
    instrument_type: Option<InstrumentTypeFilter>,
) -> Result<Value, String> {
    let Catalog {
        instruments,
        generation,
        ..
    } = catalog(source.public_get().as_ref()).await?;
    let kind = instrument_type.map(|t| match t {
        InstrumentTypeFilter::Swap => InstrumentType::Swap,
        InstrumentTypeFilter::Futures => InstrumentType::Futures,
    });
    let instruments: Vec<InstrumentSpec> = instruments
        .into_iter()
        .filter(|v| {
            (v.base_currency.as_deref() == Some(asset)
                || v.contract_value_currency.as_deref() == Some(asset))
                && settle_currency.is_none_or(|s| v.settle_currency.as_deref() == Some(s))
                && kind.is_none_or(|k| v.instrument_type == k)
        })
        .take(50)
        .collect();
    Ok(json!({"instruments":instruments,"reference_generation":generation}))
}
pub(crate) async fn resolve_universe(
    source: &IndependentOkxPublicSource,
    request: &MarketRequest,
) -> Result<(Value, Vec<String>), String> {
    let Catalog {
        instruments,
        generation,
        observed_at,
    } = catalog(source.public_get().as_ref()).await?;
    resolve(
        &instruments,
        &generation,
        &format!("{observed_at:.3}"),
        request,
    )
}
fn resolve(
    rows: &[InstrumentSpec],
    generation: &str,
    received: &str,
    request: &MarketRequest,
) -> Result<(Value, Vec<String>), String> {
    let universe = request.universe.as_ref().ok_or("UNIVERSE_PAGE_EMPTY")?;
    if universe
        .reference_generation
        .as_deref()
        .is_some_and(|v| v != generation)
    {
        return Err("UNIVERSE_REFERENCE_GENERATION_CHANGED".into());
    }
    let mut names: Vec<String> = rows
        .iter()
        .filter(|v| {
            v.state == "live"
                && v.instrument_type.to_string() == universe.instrument_type
                && universe
                    .settle_currency
                    .as_deref()
                    .is_none_or(|s| v.settle_currency.as_deref() == Some(s))
        })
        .map(|v| v.instrument_id.clone())
        .collect();
    names.sort_unstable();
    names.dedup();
    let size = names.len();
    if universe.rank_globally && size > 512 {
        return Err("GLOBAL_RANK_UNIVERSE_LIMIT:512".into());
    }
    if !universe.rank_globally {
        if let Some(after) = &universe.after_instrument {
            names.retain(|n| n > after);
        }
        names.truncate(usize::from(universe.limit) + 1);
    }
    let more = names.len()
        > usize::from(if universe.rank_globally {
            request.limit
        } else {
            universe.limit
        });
    if !universe.rank_globally {
        names.truncate(usize::from(universe.limit));
    }
    if names.is_empty() {
        return Err("UNIVERSE_PAGE_EMPTY".into());
    }
    let (scope, count, next) = if universe.rank_globally {
        (
            "complete_venue_universe",
            names.len().min(usize::from(request.limit)),
            None,
        )
    } else {
        (
            "bounded_market_page",
            names.len(),
            names.last().filter(|_| more),
        )
    };
    let page = json!({
        "schema": "okx.market.universe-page/v1",
        "source": "venue_reference_registry",
        "reference_generation": generation,
        "reference_received_at": received,
        "global_ranking": universe.rank_globally,
        "scope": scope,
        "has_more": more,
        "next_after_instrument": next,
        "universe_size": size,
        "page_count": count,
    });
    Ok((page, names))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn payload(kind: &str, ids: &[&str]) -> Vec<u8> {
        serde_json::to_vec(&json!({"code":"0","data":ids.iter().map(|id| json!({"instType":kind,"instId":id,"state":"live","tickSz":"1","lotSz":"1","minSz":"1","settleCcy":"USDT","ctValCcy":"BTC"})).collect::<Vec<_>>() })).unwrap()
    }
    #[test]
    fn catalog_freshness_uses_earliest_request_start() {
        assert!(catalog_fresh(1, 5_001));
        assert!(!catalog_fresh(1, 5_002));
        assert!(!catalog_fresh(10, 9));
    }

    #[test]
    fn typed_catalog_is_stable_and_rejects_incomplete_facts() {
        let futures = payload("FUTURES", &["BTC-USDT-2"]);
        let (a, generation) = parse_catalog(
            &payload("SWAP", &["ETH-USDT-SWAP", "BTC-USDT-SWAP"]),
            &futures,
        )
        .unwrap();
        let (_, reordered) = parse_catalog(
            &payload("SWAP", &["BTC-USDT-SWAP", "ETH-USDT-SWAP"]),
            &futures,
        )
        .unwrap();
        assert_eq!(generation, reordered);
        assert_eq!(a[0].instrument_id, "BTC-USDT-2");
        assert_eq!(a[0].instrument_type, InstrumentType::Futures);
        assert_eq!(a[0].funding_requirement, FundingRequirement::Unknown);
        assert_eq!(a[1].instrument_type, InstrumentType::Swap);
        assert_eq!(a[1].funding_requirement, FundingRequirement::Required);
        let bad = br#"{"code":"0","data":[{"instType":"SWAP","instId":"X","state":"live","tickSz":"1","lotSz":"1","minSz":"1","settleCcy":"USDT"}]}"#;
        assert!(parse_catalog(bad, &futures).is_err());
    }
    #[test]
    fn universe_cursor_and_generation_are_enforced() {
        let (rows, generation) = parse_catalog(
            &payload("SWAP", &["C", "A", "B"]),
            &payload("FUTURES", &["F"]),
        )
        .unwrap();
        let mut request: MarketRequest = serde_json::from_value(json!({"action":"screen","universe":{"instrument_type":"SWAP","limit":1,"after_instrument":"A"},"fields":["returns"],"timeframe_ms":60000,"start_ms":0,"end_ms":6000000})).unwrap();
        let (page, names) = resolve(&rows, &generation, "now", &request).unwrap();
        assert_eq!(names, vec!["B"]);
        assert_eq!(page["next_after_instrument"], "B");
        assert_eq!(page["universe_size"], 3);
        request.universe.as_mut().unwrap().reference_generation = Some("old".into());
        assert_eq!(
            resolve(&rows, &generation, "now", &request).unwrap_err(),
            "UNIVERSE_REFERENCE_GENERATION_CHANGED"
        );
    }
}

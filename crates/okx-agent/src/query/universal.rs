use std::collections::BTreeMap;

use okx_analysis::{
    RETURN_24H_PCT_METRIC_ID, RETURN_24H_PCT_METRIC_VERSION_V1, RETURN_24H_PCT_UNIT, Return24hPct,
    analyze_return_24h_pct,
};
use okx_api::InstrumentType;
use okx_observation::{InstrumentSpec, MarketUniverseTicker, ReferenceRegistry};
use okx_protocol::{
    ANALYTICAL_QUERY_CATALOG_VERSION_V1, AnalyticalQueryPlan, QueryField, QueryInstrumentState,
    QueryMetric, QuerySortDirection,
};

use super::*;

pub const QUERY_CAPABILITIES_SCHEMA_V1: &str = "okx.query-capabilities/v1";
pub const QUERY_EVIDENCE_SCHEMA_V1: &str = "okx.query-evidence/v1";
pub const QUERY_CATALOG_STALE_CODE: &str = "QUERY_CATALOG_STALE";
pub const QUERY_EVALUATION_INCONSISTENT_CODE: &str = "QUERY_EVALUATION_INCONSISTENT";

const DIAGNOSTIC_SAMPLE_LIMIT: usize = 25;

#[derive(serde::Serialize)]
struct QueryCapabilitiesResult {
    schema: &'static str,
    catalog_version: &'static str,
    domains: Vec<&'static str>,
    fields: Vec<QueryFieldCapability>,
    metrics: Vec<QueryMetricCapability>,
    operators: Vec<&'static str>,
    hard_limits: QueryHardLimits,
}

#[derive(serde::Serialize)]
struct QueryFieldCapability {
    id: &'static str,
    kind: &'static str,
}

#[derive(serde::Serialize)]
struct QueryMetricCapability {
    id: &'static str,
    version: &'static str,
    unit: &'static str,
}

#[derive(serde::Serialize)]
struct QueryHardLimits {
    instrument_types_per_query: usize,
    projection_fields: usize,
    result_rows: u16,
    diagnostic_samples: usize,
    bulk_rest_requests: usize,
}

#[derive(Debug, serde::Serialize)]
struct QueryEvidence {
    schema: &'static str,
    catalog_version: &'static str,
    as_of: String,
    quality: DataQuality,
    reference_generation: String,
    reference_received_at: String,
    source: &'static str,
    coherence: QueryCoherence,
    universe_total: usize,
    rows_observed: usize,
    eligible_rows: usize,
    rows_returned: usize,
    missing_count: usize,
    missing: Vec<String>,
    missing_truncated: bool,
    excluded_count: usize,
    excluded: Vec<QueryExclusion>,
    excluded_truncated: bool,
    result_truncated: bool,
    columns: Vec<QueryField>,
    metric_versions: Vec<QueryMetricVersion>,
    rows: Vec<QueryEvidenceRow>,
}

#[derive(Debug, serde::Serialize)]
struct QueryCoherence {
    bulk_request_count: usize,
    exchange_timestamp_min_ms: Option<u64>,
    exchange_timestamp_max_ms: Option<u64>,
    exchange_timestamp_skew_ms: Option<u64>,
    full_universe_ws_subscription_used: bool,
}

#[derive(Debug, serde::Serialize)]
struct QueryMetricVersion {
    id: &'static str,
    version: &'static str,
    unit: &'static str,
}

#[derive(Debug, serde::Serialize)]
struct QueryExclusion {
    instrument_id: String,
    reason: String,
}

#[derive(Debug, serde::Serialize)]
struct QueryEvidenceRow {
    instrument_id: String,
    values: BTreeMap<&'static str, String>,
}

#[derive(Debug)]
struct Candidate {
    instrument: InstrumentSpec,
    ticker: MarketUniverseTicker,
    return_24h_pct: Option<Return24hPct>,
}

pub(super) async fn dispatch(
    request: &AgentRequest,
    context: ObservationQueryContext<'_>,
    generated_at: &str,
) -> AgentResult<AgentResponse> {
    match &request.operation {
        AgentOperation::QueryCapabilities => Ok(AgentResponse {
            schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
            request_id: request.request_id.clone(),
            status: AgentResponseStatus::Completed,
            generated_at: generated_at.to_owned(),
            quality: DataQuality::Fresh,
            result_schema: Some(QUERY_CAPABILITIES_SCHEMA_V1.to_owned()),
            result: Some(serde_json::to_value(capabilities())?),
            failure: None,
            warnings: Vec::new(),
        }),
        AgentOperation::Query { plan } => {
            if plan.catalog_version != ANALYTICAL_QUERY_CATALOG_VERSION_V1 {
                return Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Rejected,
                    QUERY_CATALOG_STALE_CODE,
                    format!(
                        "query catalog '{}' is not current; refresh query_capabilities",
                        plan.catalog_version
                    ),
                    false,
                ));
            }

            let reference = if let Some(public_ws) = context.public_ws {
                public_ws.reference_snapshot().await
            } else if let Some(reference) = context.standalone_reference {
                reference.clone()
            } else {
                return Ok(unavailable(request, generated_at));
            };
            let Some(market) = context.market_fallback else {
                return Ok(unavailable(request, generated_at));
            };

            let mut tickers = Vec::new();
            for requested in &plan.universe.instrument_types {
                let instrument_type = match requested {
                    InstrumentTypeFilter::Swap => InstrumentType::Swap,
                    InstrumentTypeFilter::Futures => InstrumentType::Futures,
                };
                match market.universe_tickers(instrument_type).await {
                    Ok(mut batch) => tickers.append(&mut batch),
                    Err(error) => return Ok(market_failure(request, generated_at, error)),
                }
            }

            match evaluate_market_query(&reference, tickers, plan, generated_at) {
                Ok(result) => Ok(AgentResponse {
                    schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                    request_id: request.request_id.clone(),
                    status: AgentResponseStatus::Completed,
                    generated_at: generated_at.to_owned(),
                    quality: result.quality,
                    result_schema: Some(QUERY_EVIDENCE_SCHEMA_V1.to_owned()),
                    result: Some(serde_json::to_value(result)?),
                    failure: None,
                    warnings: vec![
                        "whole-universe query uses bounded bulk REST snapshots and does not subscribe the full universe on WebSocket".to_owned(),
                    ],
                }),
                Err(message) => Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Failed,
                    QUERY_EVALUATION_INCONSISTENT_CODE,
                    message,
                    true,
                )),
            }
        }
        _ => unreachable!("universal query dispatcher received unsupported operation"),
    }
}

fn capabilities() -> QueryCapabilitiesResult {
    QueryCapabilitiesResult {
        schema: QUERY_CAPABILITIES_SCHEMA_V1,
        catalog_version: ANALYTICAL_QUERY_CATALOG_VERSION_V1,
        domains: vec!["market"],
        fields: vec![
            field("instrument_id", "reference"),
            field("instrument_type", "reference"),
            field("settle_currency", "reference"),
            field("state", "reference"),
            field("last", "ticker"),
            field("open_24h", "ticker"),
            field("volume_24h", "ticker"),
            field("volume_currency_24h", "ticker"),
            field("exchange_timestamp_ms", "ticker"),
            field("return_24h_pct", "derived"),
        ],
        metrics: vec![QueryMetricCapability {
            id: RETURN_24H_PCT_METRIC_ID,
            version: RETURN_24H_PCT_METRIC_VERSION_V1,
            unit: RETURN_24H_PCT_UNIT,
        }],
        operators: vec![
            "universe_filter",
            "projection",
            "stable_sort",
            "top_bottom_k",
        ],
        hard_limits: QueryHardLimits {
            instrument_types_per_query: 2,
            projection_fields: 10,
            result_rows: 25,
            diagnostic_samples: DIAGNOSTIC_SAMPLE_LIMIT,
            bulk_rest_requests: 2,
        },
    }
}

const fn field(id: &'static str, kind: &'static str) -> QueryFieldCapability {
    QueryFieldCapability { id, kind }
}

fn evaluate_market_query(
    reference: &ReferenceRegistry,
    tickers: Vec<MarketUniverseTicker>,
    plan: &AnalyticalQueryPlan,
    generated_at: &str,
) -> Result<QueryEvidence, String> {
    let mut ticker_by_id = BTreeMap::new();
    for ticker in tickers {
        let id = ticker.instrument_id.clone();
        if ticker_by_id.insert(id.clone(), ticker).is_some() {
            return Err(format!("duplicate bulk ticker row for '{id}'"));
        }
    }
    let rows_observed = ticker_by_id.len();

    let mut universe = Vec::new();
    for instrument in reference.iter() {
        if !requested_type(instrument.instrument_type, &plan.universe.instrument_types) {
            continue;
        }
        if let Some(settle) = plan.universe.settle_currency.as_deref()
            && instrument.settle_currency.as_deref() != Some(settle)
        {
            continue;
        }
        if matches!(plan.universe.state, Some(QueryInstrumentState::Live))
            && instrument.state != "live"
        {
            continue;
        }
        universe.push(instrument.clone());
    }
    let universe_total = universe.len();

    let mut missing = Vec::new();
    let mut missing_count = 0usize;
    let mut excluded = Vec::new();
    let mut excluded_count = 0usize;
    let mut candidates = Vec::new();

    for instrument in universe {
        let Some(ticker) = ticker_by_id.remove(&instrument.instrument_id) else {
            missing_count += 1;
            push_bounded(&mut missing, instrument.instrument_id.clone());
            continue;
        };
        if ticker.instrument_type != instrument.instrument_type {
            excluded_count += 1;
            push_exclusion(
                &mut excluded,
                &instrument.instrument_id,
                "instrument_type_mismatch",
            );
            continue;
        }
        if ticker.exchange_timestamp_ms.parse::<u64>().is_err() {
            excluded_count += 1;
            push_exclusion(
                &mut excluded,
                &instrument.instrument_id,
                "invalid_exchange_timestamp",
            );
            continue;
        }

        let return_24h_pct = if plan.metric == Some(QueryMetric::Return24hPct) {
            match (ticker.last.as_deref(), ticker.open_24h.as_deref()) {
                (Some(last), Some(open)) => match analyze_return_24h_pct(last, open) {
                    Ok(value) => Some(value),
                    Err(_) => {
                        excluded_count += 1;
                        push_exclusion(
                            &mut excluded,
                            &instrument.instrument_id,
                            "invalid_return_24h_input",
                        );
                        continue;
                    }
                },
                _ => {
                    excluded_count += 1;
                    push_exclusion(
                        &mut excluded,
                        &instrument.instrument_id,
                        "missing_return_24h_input",
                    );
                    continue;
                }
            }
        } else {
            None
        };

        if let Some(reason) = missing_selected_field(plan, &instrument, &ticker) {
            excluded_count += 1;
            push_exclusion(&mut excluded, &instrument.instrument_id, reason);
            continue;
        }

        candidates.push(Candidate {
            instrument,
            ticker,
            return_24h_pct,
        });
    }

    if let Some(sort) = &plan.sort {
        candidates.sort_by(|left, right| {
            let left_metric = left
                .return_24h_pct
                .as_ref()
                .expect("validated sort requires metric");
            let right_metric = right
                .return_24h_pct
                .as_ref()
                .expect("validated sort requires metric");
            let metric_cmp = match sort.direction {
                QuerySortDirection::Asc => left_metric.cmp(right_metric),
                QuerySortDirection::Desc => right_metric.cmp(left_metric),
            };
            metric_cmp.then_with(|| {
                left.instrument
                    .instrument_id
                    .cmp(&right.instrument.instrument_id)
            })
        });
    }

    let eligible_rows = candidates.len();
    let mut min_ts: Option<u64> = None;
    let mut max_ts: Option<u64> = None;
    for candidate in &candidates {
        let timestamp = candidate
            .ticker
            .exchange_timestamp_ms
            .parse::<u64>()
            .expect("validated timestamp");
        min_ts = Some(min_ts.map_or(timestamp, |current| current.min(timestamp)));
        max_ts = Some(max_ts.map_or(timestamp, |current| current.max(timestamp)));
    }

    let result_truncated = eligible_rows > usize::from(plan.limit);
    candidates.truncate(usize::from(plan.limit));

    let rows = candidates
        .into_iter()
        .map(|candidate| project_row(candidate, &plan.select))
        .collect::<Vec<_>>();

    let metric_versions = if plan.metric == Some(QueryMetric::Return24hPct) {
        vec![QueryMetricVersion {
            id: RETURN_24H_PCT_METRIC_ID,
            version: RETURN_24H_PCT_METRIC_VERSION_V1,
            unit: RETURN_24H_PCT_UNIT,
        }]
    } else {
        Vec::new()
    };

    Ok(QueryEvidence {
        schema: QUERY_EVIDENCE_SCHEMA_V1,
        catalog_version: ANALYTICAL_QUERY_CATALOG_VERSION_V1,
        as_of: generated_at.to_owned(),
        quality: DataQuality::Degraded,
        reference_generation: reference.generation().as_str().to_owned(),
        reference_received_at: reference.source_received_at().to_owned(),
        source: "public_rest_bulk_tickers",
        coherence: QueryCoherence {
            bulk_request_count: plan.universe.instrument_types.len(),
            exchange_timestamp_min_ms: min_ts,
            exchange_timestamp_max_ms: max_ts,
            exchange_timestamp_skew_ms: min_ts.zip(max_ts).map(|(min, max)| max - min),
            full_universe_ws_subscription_used: false,
        },
        universe_total,
        rows_observed,
        eligible_rows,
        rows_returned: rows.len(),
        missing_count,
        missing,
        missing_truncated: missing_count > DIAGNOSTIC_SAMPLE_LIMIT,
        excluded_count,
        excluded,
        excluded_truncated: excluded_count > DIAGNOSTIC_SAMPLE_LIMIT,
        result_truncated,
        columns: plan.select.clone(),
        metric_versions,
        rows,
    })
}

fn requested_type(kind: InstrumentType, requested: &[InstrumentTypeFilter]) -> bool {
    requested.iter().any(|value| {
        matches!(
            (kind, value),
            (InstrumentType::Swap, InstrumentTypeFilter::Swap)
                | (InstrumentType::Futures, InstrumentTypeFilter::Futures)
        )
    })
}

fn missing_selected_field(
    plan: &AnalyticalQueryPlan,
    instrument: &InstrumentSpec,
    ticker: &MarketUniverseTicker,
) -> Option<&'static str> {
    for field in &plan.select {
        let missing = match field {
            QueryField::SettleCurrency => instrument.settle_currency.is_none(),
            QueryField::Last => ticker.last.is_none(),
            QueryField::Open24h => ticker.open_24h.is_none(),
            QueryField::Volume24h => ticker.volume_24h.is_none(),
            QueryField::VolumeCurrency24h => ticker.volume_currency_24h.is_none(),
            _ => false,
        };
        if missing {
            return Some(match field {
                QueryField::SettleCurrency => "missing_settle_currency",
                QueryField::Last => "missing_last",
                QueryField::Open24h => "missing_open_24h",
                QueryField::Volume24h => "missing_volume_24h",
                QueryField::VolumeCurrency24h => "missing_volume_currency_24h",
                _ => unreachable!(),
            });
        }
    }
    None
}

fn project_row(candidate: Candidate, fields: &[QueryField]) -> QueryEvidenceRow {
    let mut values = BTreeMap::new();
    for field in fields {
        match field {
            QueryField::InstrumentId => {
                values.insert("instrument_id", candidate.instrument.instrument_id.clone());
            }
            QueryField::InstrumentType => {
                values.insert(
                    "instrument_type",
                    candidate.instrument.instrument_type.to_string(),
                );
            }
            QueryField::SettleCurrency => {
                values.insert(
                    "settle_currency",
                    candidate
                        .instrument
                        .settle_currency
                        .clone()
                        .expect("selected field validated"),
                );
            }
            QueryField::State => {
                values.insert("state", candidate.instrument.state.clone());
            }
            QueryField::Last => {
                values.insert(
                    "last",
                    candidate
                        .ticker
                        .last
                        .clone()
                        .expect("selected field validated"),
                );
            }
            QueryField::Open24h => {
                values.insert(
                    "open_24h",
                    candidate
                        .ticker
                        .open_24h
                        .clone()
                        .expect("selected field validated"),
                );
            }
            QueryField::Volume24h => {
                values.insert(
                    "volume_24h",
                    candidate
                        .ticker
                        .volume_24h
                        .clone()
                        .expect("selected field validated"),
                );
            }
            QueryField::VolumeCurrency24h => {
                values.insert(
                    "volume_currency_24h",
                    candidate
                        .ticker
                        .volume_currency_24h
                        .clone()
                        .expect("selected field validated"),
                );
            }
            QueryField::ExchangeTimestampMs => {
                values.insert(
                    "exchange_timestamp_ms",
                    candidate.ticker.exchange_timestamp_ms.clone(),
                );
            }
            QueryField::Return24hPct => {
                values.insert(
                    "return_24h_pct",
                    candidate
                        .return_24h_pct
                        .as_ref()
                        .expect("selected metric validated")
                        .value_text(),
                );
            }
        }
    }
    QueryEvidenceRow {
        instrument_id: candidate.instrument.instrument_id,
        values,
    }
}

fn push_bounded(values: &mut Vec<String>, value: String) {
    if values.len() < DIAGNOSTIC_SAMPLE_LIMIT {
        values.push(value);
    }
}

fn push_exclusion(values: &mut Vec<QueryExclusion>, instrument_id: &str, reason: &str) {
    if values.len() < DIAGNOSTIC_SAMPLE_LIMIT {
        values.push(QueryExclusion {
            instrument_id: instrument_id.to_owned(),
            reason: reason.to_owned(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_api::PublicInstrument;

    fn reference() -> ReferenceRegistry {
        ReferenceRegistry::from_public(
            "2026-10-02T10:00:00.000Z",
            vec![
                instrument("AAA-USDT-SWAP", "AAA", "USDT"),
                instrument("BBB-USDT-SWAP", "BBB", "USDT"),
                instrument("CCC-USDT-SWAP", "CCC", "USDT"),
                instrument("DDD-USDC-SWAP", "DDD", "USDC"),
            ],
        )
        .expect("reference")
    }

    fn instrument(id: &str, asset: &str, settle: &str) -> PublicInstrument {
        PublicInstrument {
            instrument_type: "SWAP".to_owned(),
            instrument_id: id.to_owned(),
            instrument_family: format!("{asset}-{settle}"),
            underlying: format!("{asset}-{settle}"),
            state: "live".to_owned(),
            rule_type: "normal".to_owned(),
            base_currency: String::new(),
            quote_currency: String::new(),
            settle_currency: settle.to_owned(),
            tick_size: "0.0001".to_owned(),
            lot_size: "1".to_owned(),
            min_size: "1".to_owned(),
            max_limit_size: "1000000".to_owned(),
            max_market_size: "100000".to_owned(),
            max_limit_amount: String::new(),
            max_market_amount: String::new(),
            contract_type: "linear".to_owned(),
            contract_value: "1".to_owned(),
            contract_value_currency: asset.to_owned(),
            fee_group_id: "4".to_owned(),
            lever: "50".to_owned(),
            list_time: "1700000000000".to_owned(),
            expiry_time: String::new(),
            initial_price_limit_pct: "0.05".to_owned(),
            floating_price_limit_pct: "0.03".to_owned(),
            maximum_price_limit_pct: "0.15".to_owned(),
            upcoming_parameter_changes: Vec::new(),
        }
    }

    fn ticker(id: &str, last: &str, open: &str, ts: &str) -> MarketUniverseTicker {
        MarketUniverseTicker {
            instrument_id: id.to_owned(),
            instrument_type: InstrumentType::Swap,
            last: Some(last.to_owned()),
            open_24h: Some(open.to_owned()),
            volume_24h: Some("1000".to_owned()),
            volume_currency_24h: Some("100".to_owned()),
            exchange_timestamp_ms: ts.to_owned(),
        }
    }

    fn plan(limit: u16, direction: QuerySortDirection) -> AnalyticalQueryPlan {
        AnalyticalQueryPlan {
            catalog_version: ANALYTICAL_QUERY_CATALOG_VERSION_V1.to_owned(),
            universe: okx_protocol::MarketQueryUniverse {
                instrument_types: vec![InstrumentTypeFilter::Swap],
                settle_currency: Some("USDT".to_owned()),
                state: Some(QueryInstrumentState::Live),
            },
            select: vec![
                QueryField::InstrumentId,
                QueryField::Last,
                QueryField::Return24hPct,
            ],
            metric: Some(QueryMetric::Return24hPct),
            sort: Some(okx_protocol::QuerySort {
                key: okx_protocol::QuerySortKey::Return24hPct,
                direction,
            }),
            limit,
        }
    }

    #[test]
    fn ranking_is_numeric_stable_and_ties_use_instrument_id() {
        let result = evaluate_market_query(
            &reference(),
            vec![
                ticker("BBB-USDT-SWAP", "110", "100", "1002"),
                ticker("AAA-USDT-SWAP", "110", "100", "1001"),
                ticker("CCC-USDT-SWAP", "102", "100", "1003"),
            ],
            &plan(25, QuerySortDirection::Desc),
            "2026-10-02T10:00:01.000Z",
        )
        .expect("result");

        assert_eq!(result.rows[0].instrument_id, "AAA-USDT-SWAP");
        assert_eq!(result.rows[1].instrument_id, "BBB-USDT-SWAP");
        assert_eq!(result.rows[2].instrument_id, "CCC-USDT-SWAP");
        assert_eq!(result.universe_total, 3);
        assert_eq!(result.coherence.bulk_request_count, 1);
        assert!(!result.coherence.full_universe_ws_subscription_used);
    }

    #[test]
    fn limit_reports_result_truncation() {
        let result = evaluate_market_query(
            &reference(),
            vec![
                ticker("AAA-USDT-SWAP", "101", "100", "1001"),
                ticker("BBB-USDT-SWAP", "102", "100", "1002"),
                ticker("CCC-USDT-SWAP", "103", "100", "1003"),
            ],
            &plan(2, QuerySortDirection::Desc),
            "2026-10-02T10:00:01.000Z",
        )
        .expect("result");
        assert_eq!(result.eligible_rows, 3);
        assert_eq!(result.rows_returned, 2);
        assert!(result.result_truncated);
        assert_eq!(result.coherence.exchange_timestamp_min_ms, Some(1001));
        assert_eq!(result.coherence.exchange_timestamp_max_ms, Some(1003));
        assert_eq!(result.coherence.exchange_timestamp_skew_ms, Some(2));
    }

    #[test]
    fn missing_and_invalid_rows_are_explicit() {
        let mut bad = ticker("BBB-USDT-SWAP", "0", "100", "1002");
        bad.last = None;
        let result = evaluate_market_query(
            &reference(),
            vec![ticker("AAA-USDT-SWAP", "101", "100", "1001"), bad],
            &plan(25, QuerySortDirection::Desc),
            "2026-10-02T10:00:01.000Z",
        )
        .expect("result");
        assert_eq!(result.missing_count, 1);
        assert_eq!(result.missing, vec!["CCC-USDT-SWAP"]);
        assert_eq!(result.excluded_count, 1);
        assert_eq!(result.excluded[0].instrument_id, "BBB-USDT-SWAP");
    }

    #[test]
    fn settle_filter_does_not_pull_unrelated_reference_rows_into_universe() {
        let result = evaluate_market_query(
            &reference(),
            vec![
                ticker("AAA-USDT-SWAP", "101", "100", "1001"),
                ticker("BBB-USDT-SWAP", "102", "100", "1002"),
                ticker("CCC-USDT-SWAP", "103", "100", "1003"),
                ticker("DDD-USDC-SWAP", "200", "100", "1004"),
            ],
            &plan(25, QuerySortDirection::Desc),
            "2026-10-02T10:00:01.000Z",
        )
        .expect("result");
        assert_eq!(result.universe_total, 3);
        assert!(
            result
                .rows
                .iter()
                .all(|row| row.instrument_id != "DDD-USDC-SWAP")
        );
    }
}

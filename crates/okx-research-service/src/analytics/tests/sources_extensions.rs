use super::super::artifact_job_fixtures::service_with_source;
use super::*;
use crate::independent_source::test_support::RecordedGet;
use okx_research_protocol::analytics::{
    EmaParameters, FundingMode, MarketAction, MarketRequest, ResearchCosts, ResearchDataMode,
    ResearchRequest, ResearchResultView,
};
use okx_research_protocol::edge::{ResearchCommand, ResearchRequestEnvelope};
use serde_json::json;

const CANDLE_INTERVAL_MS: u64 = 60_000;
const CANDLE_START_MS: u64 = CANDLE_INTERVAL_MS;
const CANDLE_END_MS: u64 = CANDLE_START_MS + 20 * CANDLE_INTERVAL_MS;
const CANDLE_GAP_MS: u64 = CANDLE_START_MS + 10 * CANDLE_INTERVAL_MS;

#[tokio::test]
async fn graph_oos_rejects_a_train_window_without_two_scored_bars_before_any_io() {
    let transport = RecordedGet::new(vec![]);
    let (root, service) = service_with_source(transport.client());
    let request: ResearchRequest = serde_json::from_value(json!({
        "action":"graph_backtest","idempotency_key":"graph-oos-insufficient-train",
        "instruments":["BTC-USDT"],"timeframe_ms":CANDLE_INTERVAL_MS,
        "start_ms":CANDLE_START_MS,"end_ms":CANDLE_END_MS,
        "graph_signal":{
            "version":"okx.graph-signal/v1",
            "graph":{"version":"okx.market.composition/v1","steps":[
                {"id":"mean","op":"rolling_mean","input":"close","period":19},
                {"id":"entry","op":"greater_than","input":"mean","threshold":0}
            ],"result_steps":["entry"]},
            "signal_step":"entry","target_if_true":1,"target_if_false":0,
            "missing":"hold"
        },
        "costs":{"maker_fee_bps":0,"taker_fee_bps":0,"slippage_bps":0,
            "funding_mode":"zero_assumption"},
        "data_mode":"counterfactual"
    }))
    .unwrap();
    assert!(request.valid());
    assert_eq!(
        service.research(request).await.unwrap_err(),
        "graph_oos.range:INSUFFICIENT_TRAIN_OR_HOLDOUT"
    );
    assert!(service.jobs.snapshot().is_empty());
    assert!(transport.routes.lock().unwrap().is_empty());
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn graph_source_port_rejects_invalid_instrument_before_job_or_market_io() {
    let transport = RecordedGet::new(vec![]);
    let (root, service) = service_with_source(transport.client());
    let request: ResearchRequest = serde_json::from_value(json!({
        "action":"graph_backtest","idempotency_key":"typed-source-invalid-instrument",
        "instruments":["btc-usdt"],"timeframe_ms":CANDLE_INTERVAL_MS,
        "start_ms":0,"end_ms":100 * CANDLE_INTERVAL_MS,
        "graph_signal":{
            "version":"okx.graph-signal/v1",
            "graph":{"version":"okx.market.composition/v1","steps":[
                {"id":"gain","op":"returns_series","input":"close"},
                {"id":"entry","op":"greater_than","input":"gain","threshold":0}
            ],"result_steps":["entry"]},
            "signal_step":"entry","target_if_true":1,"target_if_false":0,
            "missing":"hold"
        },
        "costs":{"maker_fee_bps":0,"taker_fee_bps":0,"slippage_bps":0,
            "funding_mode":"zero_assumption"},
        "data_mode":"counterfactual"
    }))
    .unwrap();
    assert!(request.valid());
    assert_eq!(
        service.research(request).await.unwrap_err(),
        "composition.bindings.instruments[0]:INVALID_INSTRUMENT"
    );
    assert!(service.jobs.snapshot().is_empty());
    assert!(transport.routes.lock().unwrap().is_empty());
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn unavailable_historical_mode_rejects_all_executions_before_sql_or_market_reads() {
    let transport = RecordedGet::new(vec![]);
    let (root, service) = service_with_source(transport.client());
    let backtest = json!({
        "action":"backtest", "idempotency_key":"historical-backtest-no-job",
        "instruments":["BTC-USDT"], "timeframe_ms":CANDLE_INTERVAL_MS,
        "start_ms":CANDLE_START_MS, "end_ms":CANDLE_END_MS,
        "strategy_id":"ema_cross_v1", "parameters":{"fast":2,"slow":3},
        "costs":{"maker_fee_bps":0,"taker_fee_bps":10,"slippage_bps":5,
            "funding_mode":"zero_assumption"},
        "data_mode":"historical_observed"
    });
    let mut grid = backtest.clone();
    grid["action"] = json!("grid");
    grid["idempotency_key"] = json!("historical-grid-no-job");
    grid.as_object_mut().unwrap().remove("parameters");
    grid["variants"] = json!([{"fast":2,"slow":3}]);
    let mut graph = backtest.clone();
    graph["action"] = json!("graph_backtest");
    graph["idempotency_key"] = json!("historical-graph-no-job");
    graph.as_object_mut().unwrap().remove("strategy_id");
    graph.as_object_mut().unwrap().remove("parameters");
    graph["graph_signal"] = json!({
        "version":"okx.graph-signal/v1",
        "graph":{"version":"okx.market.composition/v1","steps":[
            {"id":"gain","op":"returns_series","input":"close"},
            {"id":"entry","op":"greater_than","input":"gain","threshold":0}
        ],"result_steps":["entry"]},
        "signal_step":"entry","target_if_true":1,"target_if_false":0,
        "missing":"hold"
    });
    for value in [backtest, grid, graph] {
        let request: ResearchRequest = serde_json::from_value(value).unwrap();
        assert!(request.valid());
        assert_eq!(
            service.research(request).await.unwrap_err(),
            "HISTORICAL_RULES_AND_FEES_UNAVAILABLE"
        );
    }
    assert!(service.jobs.snapshot().is_empty());
    assert!(transport.routes.lock().unwrap().is_empty());
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn graph_preflight_rejects_unsupported_source_without_job_or_venue_get() {
    let transport = RecordedGet::new(vec![]);
    let (root, service) = service_with_source(transport.client());
    let request: ResearchRequest = serde_json::from_value(json!({
        "action":"graph_backtest", "idempotency_key":"graph-invalid-no-sources",
        "instruments":["BTC-USDT"],"timeframe_ms":CANDLE_INTERVAL_MS,
        "start_ms":CANDLE_START_MS,"end_ms":CANDLE_END_MS,
        "graph_signal":{
            "version":"okx.graph-signal/v1",
            "graph":{"version":"okx.market.composition/v1","steps":[
                {"id":"vol","op":"rolling_stddev","input":"volume","period":2},
                {"id":"signal","op":"greater_than","input":"vol","threshold":0}
            ],"result_steps":["signal"]},
            "signal_step":"signal","target_if_true":1,"target_if_false":0,
            "missing":"hold"
        },
        "costs":{"maker_fee_bps":0,"taker_fee_bps":10,"slippage_bps":5,
            "funding_mode":"zero_assumption"},
        "data_mode":"counterfactual"
    }))
    .unwrap();
    assert!(request.valid());
    assert_eq!(
        service.research(request).await.unwrap_err(),
        "composition.stream:SOURCE_NOT_SUPPORTED:volume"
    );
    assert!(service.jobs.snapshot().is_empty());
    assert!(transport.routes.lock().unwrap().is_empty());
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn graph_admission_identifies_invalid_field_at_edge_and_owner_without_side_effects() {
    let transport = RecordedGet::new(vec![]);
    let (root, service) = service_with_source(transport.client());
    let base = json!({
        "action":"graph_backtest", "idempotency_key":"typed-graph-admission",
        "instruments":["BTC-USDT"],"timeframe_ms":CANDLE_INTERVAL_MS,
        "start_ms":CANDLE_START_MS,"end_ms":CANDLE_END_MS,
        "graph_signal":{
            "version":"okx.graph-signal/v1",
            "graph":{"version":"okx.market.composition/v1","steps":[
                {"id":"gain","op":"returns_series","input":"close"},
                {"id":"signal","op":"greater_than","input":"gain","threshold":0.0}
            ],"result_steps":["signal"]},
            "signal_step":"signal","target_if_true":1,"target_if_false":0,
            "missing":"hold"
        },
        "costs":{"maker_fee_bps":0,"taker_fee_bps":10,"slippage_bps":5,
            "funding_mode":"zero_assumption"},
        "data_mode":"counterfactual"
    });
    for (pointer, bad, reason) in [
        (
            "/graph_signal/version",
            json!("bad"),
            "graph_signal.version:UNSUPPORTED",
        ),
        (
            "/graph_signal/graph/steps/0/input",
            json!("private/source"),
            "graph_signal.graph.steps[0].input:INVALID_REFERENCE",
        ),
        (
            "/graph_signal/graph/steps/1/op",
            json!("forward_return_label"),
            "graph_signal.graph.steps[1].op:FORWARD_LABEL_FORBIDDEN",
        ),
        (
            "/graph_signal/signal_step",
            json!("unused"),
            "graph_signal.signal_step:UNPROJECTED",
        ),
        (
            "/graph_signal/target_if_true",
            json!(2),
            "graph_signal.target_if_true:INVALID_POSITION",
        ),
        (
            "/costs/taker_fee_bps",
            json!(-1),
            "research.graph_backtest.costs.taker_fee_bps:OUT_OF_RANGE",
        ),
        (
            "/timeframe_ms",
            json!(1),
            "research.graph_backtest.timeframe_ms:UNSUPPORTED",
        ),
        (
            "/end_ms",
            json!(CANDLE_START_MS + 1_000_001 * CANDLE_INTERVAL_MS),
            "research.graph_backtest.range:RESOURCE_LIMIT",
        ),
        (
            "/instruments/0",
            json!(""),
            "research.graph_backtest.instruments[0]:INVALID_LENGTH",
        ),
    ] {
        let mut value = base.clone();
        *value.pointer_mut(pointer).unwrap() = bad;
        let envelope: ResearchRequestEnvelope = serde_json::from_value(json!({
            "schema":"okx.agent.request/v1","request_id":"typed_graph_request_001",
            "operation":{"type":"research","request":value.clone()}
        }))
        .unwrap();
        assert_eq!(
            ResearchCommand::try_from(envelope).err().unwrap(),
            reason,
            "{pointer}: Edge"
        );
        let request: ResearchRequest = serde_json::from_value(value).unwrap();
        assert!(!request.valid(), "{pointer}");
        assert_eq!(
            service.research(request).await.unwrap_err(),
            reason,
            "{pointer}: owner"
        );
    }
    assert!(service.jobs.snapshot().is_empty());
    assert!(transport.routes.lock().unwrap().is_empty());
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn graph_compiler_unit_rejection_precedes_job_and_source_access() {
    let transport = RecordedGet::new(vec![]);
    let (root, service) = service_with_source(transport.client());
    let request: ResearchRequest = serde_json::from_value(json!({
        "action":"graph_backtest", "idempotency_key":"graph-non-boolean-signal",
        "instruments":["BTC-USDT"],"timeframe_ms":CANDLE_INTERVAL_MS,
        "start_ms":CANDLE_START_MS,"end_ms":CANDLE_END_MS,
        "graph_signal":{
            "version":"okx.graph-signal/v1",
            "graph":{"version":"okx.market.composition/v1","steps":[
                {"id":"gain","op":"returns_series","input":"close"}
            ],"result_steps":["gain"]},
            "signal_step":"gain","target_if_true":1,"target_if_false":0,
            "missing":"hold"
        },
        "costs":{"maker_fee_bps":0,"taker_fee_bps":10,"slippage_bps":5,
            "funding_mode":"zero_assumption"},
        "data_mode":"counterfactual"
    }))
    .unwrap();
    assert!(request.valid());
    assert_eq!(
        service.research(request).await.unwrap_err(),
        "graph_signal:SIGNAL_MUST_BE_BOOLEAN_OR_EVENT"
    );
    assert!(service.jobs.snapshot().is_empty());
    assert!(transport.routes.lock().unwrap().is_empty());
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn t06_historical_funding_never_falls_back_to_predicted_rate() {
    assert_eq!(
        realized_funding_rate(None).unwrap_err(),
        "FUNDING_REALIZED_RATE_UNAVAILABLE"
    );
    assert_eq!(
        realized_funding_rate(Some("")).unwrap_err(),
        "FUNDING_REALIZED_RATE_UNAVAILABLE"
    );
    assert_eq!(
        realized_funding_rate(Some("NaN")).unwrap_err(),
        "FUNDING_REALIZED_RATE_INVALID"
    );
    assert_eq!(
        realized_funding_rate(Some("not-a-rate")).unwrap_err(),
        "FUNDING_REALIZED_RATE_INVALID"
    );
    assert!((realized_funding_rate(Some("0.00012")).unwrap() - 0.00012).abs() < 1e-15);
}

fn funding_page(time: &str, realized: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"code":"0","msg":"","data":[{
        "instId":"BTC-USDT-SWAP","fundingTime":time,
        "fundingRate":"0.09","realizedRate":realized
    }]}))
    .unwrap()
}

fn assert_funding_requests(transport: &RecordedGet, cursors: &[&str]) {
    let routes = transport.routes.lock().unwrap();
    assert_eq!(routes.len(), cursors.len());
    for ((path, parameters), cursor) in routes.iter().zip(cursors.iter().copied()) {
        assert_eq!(*path, "/api/v5/public/funding-rate-history");
        assert_eq!(
            *parameters,
            vec![
                ("instId", "BTC-USDT-SWAP".to_owned()),
                ("after", cursor.to_owned()),
                ("limit", "100".to_owned()),
            ]
        );
    }
}

#[tokio::test]
async fn t06_realized_funding_guard_is_enforced_through_public_source() {
    for (realized, expected) in [("0.02", Some(0.02)), ("", None)] {
        let transport = RecordedGet::new(vec![funding_page("20000", realized)]);
        let (root, service) = service_with_source(transport.client());
        let actual = service
            .sources
            .funding("BTC-USDT-SWAP", 20_000, 30_000)
            .await;
        match expected {
            Some(rate) => assert_eq!(actual.unwrap(), vec![(20_000, rate)]),
            None => assert_eq!(actual.unwrap_err(), "FUNDING_REALIZED_RATE_UNAVAILABLE"),
        }
        assert_funding_requests(&transport, &["30000"]);
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn t06_duplicate_funding_event_requires_consistent_realized_rate() {
    for (other_rate, expected) in [
        ("0.02", None),
        ("0.03", Some("FUNDING_CONFLICTING_EVENT_TIMESTAMP")),
    ] {
        let transport = RecordedGet::new(vec![
            funding_page("25000", "0.02"),
            funding_page("25000", other_rate),
        ]);
        let (root, service) = service_with_source(transport.client());
        let result = service
            .sources
            .funding("BTC-USDT-SWAP", 20_000, 30_000)
            .await;
        match expected {
            None => assert_eq!(result.unwrap(), vec![(25_000, 0.02)]),
            Some(error) => assert_eq!(result.unwrap_err(), error),
        }
        assert_funding_requests(&transport, &["30000", "25000"]);
        drop(service);
        std::fs::remove_dir_all(root).unwrap();
    }
}

fn recorded_candles(rows: Vec<serde_json::Value>) -> Arc<RecordedGet> {
    RecordedGet::new(vec![
        serde_json::to_vec(&json!({"code":"0","msg":"","data":rows})).unwrap(),
    ])
}

fn assert_candle_request(transport: &RecordedGet) {
    assert_eq!(
        *transport.routes.lock().unwrap(),
        vec![(
            "/api/v5/market/history-candles",
            vec![
                ("instId", "BTC-USDT".to_owned()),
                ("bar", "1m".to_owned()),
                ("after", CANDLE_END_MS.to_string()),
                ("limit", "100".to_owned()),
            ],
        )]
    );
}

#[tokio::test]
async fn p7_missing_closed_dataset_reports_owner_reason_without_creating_a_job() {
    // The recorded public endpoint returns an empty, successful page.
    // This is a real source-owner failure, not a fabricated job/error row.
    let transport = recorded_candles(vec![]);
    let (root, service) = service_with_source(transport.client());
    let request: ResearchRequest = serde_json::from_value(json!({
        "action":"validate", "instruments":["BTC-USDT"],
        "timeframe_ms":CANDLE_INTERVAL_MS,
        "start_ms":CANDLE_START_MS, "end_ms":CANDLE_END_MS,
        "range_mode":"strict", "strategy_id":"ema_momentum_confirm_v1",
        "parameters":{"fast":1,"slow":2},
        "costs":{"maker_fee_bps":0,"taker_fee_bps":0,"slippage_bps":0,
            "funding_mode":"zero_assumption"},
        "data_mode":"counterfactual"
    }))
    .unwrap();
    let response = service.research(request).await.unwrap();
    assert_eq!(response["schema"], "okx.research.validation/v1");
    assert_eq!(response["status"], "blocked");
    assert_eq!(response["quality"], "partial");
    assert_eq!(response["side_effects"], "none");
    assert_eq!(response["job_created"], false);
    assert_eq!(response["source_coverage"][0]["instrument"], "BTC-USDT");
    assert_eq!(response["source_coverage"][0]["coverage"], "unavailable");
    assert_eq!(
        response["source_coverage"][0]["reason_code"],
        "CANDLE_RANGE_UNAVAILABLE"
    );
    assert!(service.jobs.snapshot().is_empty());
    let unknown = service
        .jobs
        .diagnostic_by_invocation("req_no_job_for_missing_dataset");
    assert_eq!(unknown["state"], "NOT_OBSERVED");
    assert_eq!(unknown["reason_code"], "UNKNOWN_NOT_RETAINED");
    assert!(unknown.get("matched_request_id").is_none());
    assert_candle_request(&transport);
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

fn raw_candle(index: u64, confirm: &str) -> serde_json::Value {
    let close = (index + 1) as f64;
    json!([
        (CANDLE_START_MS + index * CANDLE_INTERVAL_MS).to_string(),
        close.to_string(),
        (close + 0.5).to_string(),
        (close - 0.5).to_string(),
        close.to_string(),
        "42",
        "42",
        "42",
        confirm
    ])
}

#[tokio::test]
async fn c20_duplicate_raw_source_is_rejected_without_cache_or_job_effects() {
    let mut rows = (0..20)
        .rev()
        .map(|index| raw_candle(index, "1"))
        .collect::<Vec<_>>();
    rows.push(raw_candle(2, "1"));
    let transport = recorded_candles(rows);
    let (root, service) = service_with_source(transport.client());
    assert_eq!(
        service
            .sources
            .candles(
                "BTC-USDT",
                CANDLE_INTERVAL_MS,
                CANDLE_START_MS,
                CANDLE_END_MS,
                false
            )
            .await
            .unwrap_err(),
        "RESEARCH_PUBLIC_CANDLES_INVALID"
    );
    assert!(service.sources.excluded.lock().unwrap().is_empty());
    assert!(service.jobs.snapshot().is_empty());
    let key = super::super::cache::SourceKey {
        source: "candles".into(),
        instrument: "BTC-USDT".into(),
        timeframe_ms: CANDLE_INTERVAL_MS,
        start_ms: CANDLE_START_MS,
        end_ms: CANDLE_END_MS,
        closed_only: true,
        schema: 1,
    };
    // A successful cache entry would bypass this predetermined error probe.
    assert_eq!(
        service
            .sources
            .cache
            .load(key, || async { Err("INVALID_PAGE_NOT_CACHED".into()) })
            .await
            .unwrap_err(),
        "INVALID_PAGE_NOT_CACHED"
    );
    assert_candle_request(&transport);
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn c20_in_window_unconfirmed_candle_blocks_validation_and_replay_without_result() {
    let rows = (0..20)
        .rev()
        .map(|index| {
            if index == 10 {
                let mut row = raw_candle(index, "0");
                row[1] = json!("999");
                row[2] = json!("1000");
                row[3] = json!("998");
                row[4] = json!("999");
                row
            } else {
                raw_candle(index, "1")
            }
        })
        .collect::<Vec<_>>();
    let transport = recorded_candles(rows);
    let (root, service) = service_with_source(transport.client());
    let mut wire = json!({"action":"validate","instruments":["BTC-USDT"],"timeframe_ms":CANDLE_INTERVAL_MS,
        "start_ms":CANDLE_START_MS,"end_ms":CANDLE_END_MS,"range_mode":"strict","strategy_id":"ema_momentum_confirm_v1",
        "parameters":{"fast":1,"slow":2},"costs":{"maker_fee_bps":0,"taker_fee_bps":0,"slippage_bps":0,"funding_mode":"zero_assumption"},
        "data_mode":"counterfactual"});
    let validation = service
        .research(serde_json::from_value(wire.clone()).unwrap())
        .await
        .unwrap();
    assert_eq!(validation["status"], "blocked");
    assert_eq!(validation["job_created"], false);
    assert_eq!(validation["source_coverage"][0]["observed_bars"], 19);
    assert_eq!(
        validation["source_coverage"][0]["reason_code"],
        "CANDLE_RANGE_INCOMPLETE_OR_GAPPED"
    );
    // Partial observations remain cached honestly, never a complete result.
    let confirmed = service
        .sources
        .candles(
            "BTC-USDT",
            CANDLE_INTERVAL_MS,
            CANDLE_START_MS,
            CANDLE_END_MS,
            false,
        )
        .await
        .unwrap();
    assert_eq!(confirmed.len(), 19);
    assert!(
        confirmed
            .iter()
            .all(|bar| bar.time != CANDLE_GAP_MS && bar.close != 999.)
    );
    // T04: the same recorded HTTP gap must remain visible in the public
    // market composition, never silently turning a missing bar into zero.
    let screen: MarketRequest = serde_json::from_value(json!({
        "action":"screen","instruments":["BTC-USDT"],"fields":["returns"],
        "timeframe_ms":CANDLE_INTERVAL_MS,"start_ms":CANDLE_START_MS,"end_ms":CANDLE_END_MS,"limit":20,
        "composition":{"version":"okx.market.composition/v1","steps":[
            {"id":"r1","op":"returns_series","input":"close"},
            {"id":"w1","op":"rolling_mean","input":"r1","period":2}
        ],"result_steps":["w1"]}
    }))
    .unwrap();
    let screen_response = service.market(screen, None, None).await.unwrap();
    let first = &screen_response["rows"][0];
    assert_eq!(first["quality"], "partial");
    assert_eq!(first["candle_coverage"]["expected_bars"], 20);
    assert_eq!(first["candle_coverage"]["observed_unique_bars"], 19);
    assert_eq!(first["candle_coverage"]["missing_bars"], 1);
    assert_eq!(first["candle_coverage"]["first_missing_ms"], CANDLE_GAP_MS);
    assert_eq!(first["unclosed_candles_excluded"], 1);
    assert_eq!(first["metrics"]["returns"], serde_json::Value::Null);
    assert_eq!(first["metrics"]["w1"], serde_json::Value::Null);
    assert_eq!(first["units"]["w1"], "ratio");
    assert!(
        first["gaps"]
            .as_array()
            .unwrap()
            .contains(&json!("CANDLE_INTERIOR_GAP"))
    );
    assert_candle_request(&transport);
    // A market query legitimately records one completed read-only job and
    // immutable evidence. A rejected backtest must add neither.
    let jobs_before_rejected_replay = service.jobs.snapshot().len();
    let evidence_before_rejected_replay = std::fs::read_dir(service.artifacts.evidence_dir())
        .unwrap()
        .count();
    assert!(jobs_before_rejected_replay > 0);
    assert!(evidence_before_rejected_replay > 0);

    wire["action"] = json!("backtest");
    wire["idempotency_key"] = json!("c20-unconfirmed-gap");
    wire.as_object_mut().unwrap().remove("range_mode");
    assert_eq!(
        service
            .execute_one(serde_json::from_value(wire).unwrap(), Default::default())
            .await
            .unwrap_err(),
        "CANDLE_INTERIOR_GAP"
    );
    assert_eq!(service.jobs.snapshot().len(), jobs_before_rejected_replay);
    assert_eq!(
        std::fs::read_dir(service.artifacts.evidence_dir())
            .unwrap()
            .count(),
        evidence_before_rejected_replay
    );
    assert_candle_request(&transport);
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn c20_actual_sources_normalize_fake_http_then_run_registered_composition_and_mean() {
    let mut rows = (0..20)
        .rev()
        .map(|index| raw_candle(index, "1"))
        .collect::<Vec<_>>();
    // Fixed valid profile: unique reversed timestamps plus a distinct
    // unconfirmed edge candle, never a second row for an existing timestamp.
    let mut unclosed = raw_candle(20, "0");
    unclosed[1] = json!("999");
    unclosed[2] = json!("1000");
    unclosed[3] = json!("998");
    unclosed[4] = json!("999");
    rows.push(unclosed);
    let transport = recorded_candles(rows);
    let (root, service) = service_with_source(transport.client());
    // This exercises the existing real delivery decoder and Sources boundary,
    // rather than inserting normalized bars directly into the source cache.
    let bars = service
        .sources
        .candles(
            "BTC-USDT",
            CANDLE_INTERVAL_MS,
            CANDLE_START_MS,
            CANDLE_END_MS,
            false,
        )
        .await
        .unwrap();
    assert_eq!(bars.len(), 20);
    for (index, bar) in bars.iter().enumerate() {
        assert_eq!(
            bar.time,
            CANDLE_START_MS + index as u64 * CANDLE_INTERVAL_MS
        );
        assert_eq!(bar.close, (index + 1) as f64);
        assert_eq!(bar.volume, 42.);
        assert_eq!(bar.funding_rate, None);
    }
    assert_eq!(
        *service
            .sources
            .excluded
            .lock()
            .unwrap()
            .values()
            .next()
            .unwrap(),
        1
    );
    let market = service
        .market(
            MarketRequest {
                action: MarketAction::Screen,
                instruments: vec!["BTC-USDT".into()],
                universe: None,
                timeframe_ms: CANDLE_INTERVAL_MS,
                start_ms: CANDLE_START_MS,
                end_ms: CANDLE_END_MS,
                fields: vec!["rolling_mean".into()],
                parameters: Default::default(),
                filters: vec![],
                sort: None,
                composition: Some(Box::new(okx_analysis::query_plan::CompositionRequest {
                    version: okx_analysis::query_plan::COMPOSITION_VERSION.into(),
                    steps: vec![
                        okx_analysis::query_plan::SeriesStep {
                            id: "r1".into(),
                            op: okx_analysis::query_plan::SeriesOp::ReturnsSeries,
                            input: "close".into(),
                            period: None,
                            threshold: None,
                            when: None,
                            other: None,
                        },
                        okx_analysis::query_plan::SeriesStep {
                            id: "w1".into(),
                            op: okx_analysis::query_plan::SeriesOp::RollingMean,
                            input: "r1".into(),
                            period: Some(2),
                            threshold: None,
                            when: None,
                            other: None,
                        },
                    ],
                    result_steps: vec!["w1".into()],
                })),
                limit: 20,
                cursor: None,
            },
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(market["rows"][0]["metrics"]["rolling_mean"], 13.5);
    assert_eq!(market["rows"][0]["units"]["rolling_mean"], "price");
    assert!(market["rows"][0]["metrics"]["w1"].is_number());
    assert_eq!(market["rows"][0]["units"]["w1"], "ratio");
    assert_eq!(market["rows"][0]["quality"], "complete");
    assert_eq!(market["rows"][0]["candle_coverage"]["expected_bars"], 20);
    assert_eq!(
        market["rows"][0]["candle_coverage"]["observed_unique_bars"],
        20
    );
    assert_eq!(market["rows"][0]["candle_coverage"]["missing_bars"], 0);
    assert_eq!(market["rows"][0]["unclosed_candles_excluded"], 1);
    assert_eq!(
        market["rows"][0]["source_integrity"]["quality"],
        "verified_normalized_sources"
    );
    assert_eq!(
        market["rows"][0]["source_integrity"]["verified_sources"][0]["source"],
        "candles"
    );
    assert_eq!(
        market["rows"][0]["source_integrity"]["replay_metric_reexecution_verified"],
        true
    );
    assert_eq!(
        market["rows"][0]["source_integrity"]["metric_reexecution"]["quality"],
        "verified_reexecution"
    );
    // T07/T15: a previously undeclared combination executes through the
    // public source decoder and durable market artifact path with the same
    // binary. The already captured candles are reused, with no venue POST.
    let boolean_query: MarketRequest = serde_json::from_value(json!({
        "action":"screen","instruments":["BTC-USDT"],"fields":["returns"],
        "timeframe_ms":CANDLE_INTERVAL_MS,"start_ms":CANDLE_START_MS,"end_ms":CANDLE_END_MS,"limit":20,
        "composition":{"version":"okx.market.composition/v1","steps":[
            {"id":"return","op":"returns_series","input":"close"},
            {"id":"positive","op":"greater_than","input":"return","threshold":0},
            {"id":"prior","op":"lag","input":"positive","period":1},
            {"id":"both","op":"and","input":"positive","other":"prior"},
            {"id":"either","op":"or","input":"positive","other":"prior"},
            {"id":"event","op":"rising_edge","input":"both"}
        ],"result_steps":["both","either","event"]}
    }))
    .unwrap();
    let boolean_result = service.market(boolean_query, None, None).await.unwrap();
    let boolean_row = &boolean_result["rows"][0];
    assert_eq!(boolean_row["metrics"]["both"], true);
    assert_eq!(boolean_row["metrics"]["either"], true);
    assert_eq!(boolean_row["metrics"]["event"], false);
    assert_eq!(boolean_row["units"]["both"], "boolean");
    assert_eq!(boolean_row["quality"], "complete");
    assert_candle_request(&transport);
    // T05/T15: ratio arithmetic is available through the same binary,
    // venue source decoder and immutable result route without new I/O.
    let arithmetic_query: MarketRequest = serde_json::from_value(json!({
        "action":"screen","instruments":["BTC-USDT"],"fields":["returns"],
        "timeframe_ms":CANDLE_INTERVAL_MS,"start_ms":CANDLE_START_MS,"end_ms":CANDLE_END_MS,"limit":20,
        "composition":{"version":"okx.market.composition/v1","steps":[
            {"id":"gain","op":"returns_series","input":"close"},
            {"id":"previous","op":"lag","input":"gain","period":1},
            {"id":"difference","op":"subtract_series","input":"gain","other":"previous"},
            {"id":"relative","op":"divide_series","input":"gain","other":"previous"}
        ],"result_steps":["difference","relative"]}
    }))
    .unwrap();
    let arithmetic = service.market(arithmetic_query, None, None).await.unwrap();
    let arithmetic_row = &arithmetic["rows"][0];
    assert!((arithmetic_row["metrics"]["difference"].as_f64().unwrap() + 1. / 342.).abs() < 1e-12);
    assert!((arithmetic_row["metrics"]["relative"].as_f64().unwrap() - 18. / 19.).abs() < 1e-12);
    assert_eq!(arithmetic_row["units"]["difference"], "ratio");
    assert_eq!(arithmetic_row["quality"], "complete");
    assert_candle_request(&transport);
    // T13: composition must disclose its candle dependency even when
    // the only legacy projection depends solely on request parameters.
    let composition_only_source: MarketRequest = serde_json::from_value(json!({
        "action":"screen","instruments":["BTC-USDT"],"fields":["position_size"],
        "timeframe_ms":CANDLE_INTERVAL_MS,"start_ms":CANDLE_START_MS,"end_ms":CANDLE_END_MS,"limit":20,
        "parameters":{"budget":100,"price":10,"stop_distance":0.1},
        "composition":{"version":"okx.market.composition/v1","steps":[
            {"id":"r1","op":"returns_series","input":"close"},
            {"id":"w1","op":"rolling_mean","input":"r1","period":2}
        ],"result_steps":["w1"]}
    }))
    .unwrap();
    let with_candles = service
        .market(composition_only_source, None, None)
        .await
        .unwrap();
    assert_eq!(with_candles["sources"], json!(["candles", "request"]));
    assert_eq!(
        with_candles["sources"],
        with_candles["query_plan"]["sources"]
    );
    assert_eq!(with_candles["rows"][0]["units"]["w1"], "ratio");
    assert!(with_candles["rows"][0]["metrics"]["w1"].is_number());
    let mut request = ResearchRequest::Backtest {
        idempotency_key: "c20-registered-composition".into(),
        instruments: vec!["BTC-USDT".into()],
        timeframe_ms: CANDLE_INTERVAL_MS,
        start_ms: CANDLE_START_MS,
        end_ms: CANDLE_END_MS,
        strategy_id: "ema_momentum_confirm_v1".into(),
        parameters: EmaParameters { fast: 1, slow: 2 }.into(),
        costs: ResearchCosts {
            maker_fee_bps: 0.,
            taker_fee_bps: 0.,
            slippage_bps: 0.,
            funding_mode: FundingMode::ZeroAssumption,
        },
        data_mode: ResearchDataMode::Counterfactual,
        seed: 7,
    };
    let queued = service.research(request.clone()).await.unwrap();
    let job = queued["id"].as_str().unwrap().to_owned();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let status = service.jobs.get(&job).unwrap();
            match status.state {
                super::super::jobs::State::Completed => break,
                super::super::jobs::State::Failed => {
                    panic!("composition job failed: {:?}", status.error)
                }
                _ => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .unwrap();
    let result = service
        .research(ResearchRequest::Result {
            job_id: job,
            cursor: None,
            limit: 20,
            view: ResearchResultView::Summary,
        })
        .await
        .unwrap();
    assert_eq!(result["metadata"]["summary"]["pnl_exact"], "17");
    let experiment: serde_json::Value = service
        .artifacts
        .read_evidence_json(result["metadata"]["experiment_artifact"].as_str().unwrap())
        .unwrap();
    assert_eq!(experiment["spec"]["strategy_id"], "ema_momentum_confirm_v1");
    assert_eq!(experiment["spec"]["parameters"], json!({"fast":1,"slow":2}));
    // The operator passes an existing causal market graph as data to the SAME
    // Research owner. No new StrategyKernel, finance owner or service exists.
    let graph_request: ResearchRequest = serde_json::from_value(json!({
        "action":"graph_backtest", "idempotency_key":"c20-graph-through-research-owner",
        "instruments":["BTC-USDT"],"timeframe_ms":CANDLE_INTERVAL_MS,
        "start_ms":CANDLE_START_MS,"end_ms":CANDLE_END_MS,
        "graph_signal":{
            "version":"okx.graph-signal/v1",
            "graph":{"version":"okx.market.composition/v1","steps":[
                {"id":"gain","op":"returns_series","input":"close"},
                {"id":"entry","op":"greater_than","input":"gain","threshold":0}
            ],"result_steps":["entry"]},
            "signal_step":"entry","target_if_true":1,"target_if_false":0,
            "missing":"hold"
        },
        "costs":{"maker_fee_bps":8,"taker_fee_bps":10,"slippage_bps":5,
            "funding_mode":"zero_assumption"},
        "data_mode":"counterfactual","seed":7
    }))
    .unwrap();
    assert!(graph_request.valid());
    let graph_job = service.research(graph_request).await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let state = service.jobs.get(&graph_job).unwrap();
            match state.state {
                super::super::jobs::State::Completed => break,
                super::super::jobs::State::Failed => panic!("graph job failed: {:?}", state.error),
                _ => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .unwrap();
    let graph_result = service
        .research(ResearchRequest::Result {
            job_id: graph_job.clone(),
            cursor: None,
            limit: 20,
            view: ResearchResultView::Summary,
        })
        .await
        .unwrap();
    assert!(
        graph_result["metadata"]["summary"]["trades"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(
        graph_result["metadata"]["graph_signal"]["signal_step"],
        "entry"
    );
    assert_eq!(
        graph_result["metadata"]["economic_audit"]["net_exact"],
        graph_result["metadata"]["summary"]["pnl_exact"]
    );
    let oos = &graph_result["metadata"]["graph_oos"];
    assert_eq!(oos["schema"], "okx.research.graph-oos/v1");
    assert_eq!(oos["policy"]["split_index"], 14);
    assert_eq!(oos["policy"]["train_score_start_index"], 2);
    assert_eq!(oos["policy"]["test_score_start_index"], 14);
    assert_eq!(oos["policy"]["selection"], "fixed_graph_no_model_fitting");
    assert_eq!(oos["selection"], "predeclared_graph_no_optimization");
    assert_eq!(
        oos["holdout"]["summary"],
        graph_result["metadata"]["summary"]
    );
    assert_eq!(
        oos["train"]["economic_audit"]["net_exact"],
        oos["train"]["summary"]["pnl_exact"]
    );
    let graph_experiment: serde_json::Value = service
        .artifacts
        .read_evidence_json(
            graph_result["metadata"]["experiment_artifact"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        graph_experiment["graph_spec"]["graph"]["signal_step"],
        "entry"
    );
    assert!(graph_experiment.get("spec").is_none());
    assert_eq!(graph_experiment["graph_oos_policy"], oos["policy"]);
    let exported = service
        .research(ResearchRequest::Result {
            job_id: graph_job,
            cursor: None,
            limit: 20,
            view: ResearchResultView::Export,
        })
        .await
        .unwrap();
    assert_eq!(
        exported["integrity"]["reexecution"]["quality"],
        "verified_reexecution"
    );
    assert_eq!(exported["graph_oos"], *oos);
    let manifest: serde_json::Value = service
        .artifacts
        .read_evidence_json(exported["leaf_id"].as_str().unwrap())
        .unwrap();
    let mut forged_train = manifest.clone();
    forged_train["graph_oos"]["train"]["result_hash"] = json!("forged");
    assert_eq!(
        super::super::result_reexecution::verify(
            &service.artifacts,
            &graph_experiment,
            &forged_train
        )
        .unwrap_err(),
        "EXPORT_REEXECUTION_MISMATCH"
    );
    let mut forged_split = manifest;
    forged_split["graph_oos"]["policy"]["split_index"] = json!(13);
    assert_eq!(
        super::super::result_reexecution::verify(
            &service.artifacts,
            &graph_experiment,
            &forged_split
        )
        .unwrap_err(),
        "EXPORT_REEXECUTION_MISMATCH"
    );
    let before = service.jobs.snapshot().len();
    if let ResearchRequest::Backtest { strategy_id, .. } = &mut request {
        *strategy_id = "unregistered_source_code".into();
    }
    assert_eq!(
        service.research(request).await.unwrap_err(),
        "INVALID_RESEARCH_REQUEST"
    );
    assert_eq!(service.jobs.snapshot().len(), before);
    assert_candle_request(&transport);
    drop(service);
    std::fs::remove_dir_all(root).unwrap();
}

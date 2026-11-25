use super::*;
use crate::independent_source::{GetFuture, PublicGet};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct Venue(AtomicUsize);

impl PublicGet for Venue {
    fn get(&self, path: &'static str, query: Vec<(&'static str, String)>) -> GetFuture<'_> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::Relaxed);
            let supplied = |key| {
                query
                    .iter()
                    .find(|(k, _)| *k == key)
                    .map(|(_, v)| v.as_str())
            };
            let inst = supplied("instId").unwrap_or("");
            let ts = jiff::Timestamp::now().as_millisecond().to_string();
            let spec = |instrument, kind| {
                json!({
                    "instId":instrument,"instType":kind,"state":"live","uly":"BTC-USDT",
                    "tickSz":"0.1","lotSz":"1","minSz":"1","ruleType":"normal",
                    "settleCcy":"USDT","ctValCcy":"BTC","ctVal":"1"
                })
            };
            let data = match path {
                "/api/v5/public/instruments" => match (supplied("instType"), inst) {
                    (Some("SWAP"), "") => {
                        vec![spec("BTC-USDT-SWAP", "SWAP"), spec("BTC-USDC-SWAP", "SWAP")]
                    }
                    (Some("FUTURES"), "") => vec![spec("BTC-USDT-271231", "FUTURES")],
                    (Some("SWAP"), "BTC-USDT-SWAP") => vec![spec(inst, "SWAP")],
                    _ => panic!("unexpected catalog parameters {query:?}"),
                },
                "/api/v5/market/books"
                    if inst == "BTC-USDT-SWAP" && supplied("sz") == Some("10") =>
                {
                    vec![
                        json!({"ts":ts,"seqId":17,"asks":[["101","2","0","1"]],"bids":[["99","2","0","1"]]}),
                    ]
                }
                "/api/v5/market/ticker" if matches!(inst, "BTC-USDT-SWAP" | "BTC-USDC-SWAP") => {
                    vec![
                        json!({"instId":inst,"ts":ts,"last":"100","lastSz":"1","bidPx":"99","bidSz":"2","askPx":"101","askSz":"2"}),
                    ]
                }
                "/api/v5/public/mark-price"
                    if inst == "BTC-USDT-SWAP" && supplied("instType") == Some("SWAP") =>
                {
                    vec![json!({"instId":inst,"markPx":"100","ts":ts})]
                }
                "/api/v5/market/history-candles"
                    if matches!(inst, "BTC-USDT-SWAP" | "BTC-USDC-SWAP") =>
                {
                    vec![
                        json!(["120000", "100", "111", "99", "110", "2", "200", "220", "1"]),
                        json!(["60000", "99", "101", "98", "100", "1", "99", "100", "1"]),
                    ]
                }
                "/api/v5/market/index-tickers" if inst == "BTC-USDT" => {
                    vec![json!({"instId":inst,"idxPx":"100","ts":ts})]
                }
                "/api/v5/public/open-interest"
                    if inst == "BTC-USDT-SWAP" && supplied("instType") == Some("SWAP") =>
                {
                    vec![json!({"instId":inst,"oi":"30","ts":ts})]
                }
                "/api/v5/public/funding-rate" if inst == "BTC-USDT-SWAP" => vec![
                    json!({"instId":inst,"fundingRate":"0.001","fundingTime":"1795550400000","nextFundingTime":"1795579200000","ts":ts}),
                ],
                other => panic!("unexpected public GET {other} {query:?}"),
            };
            Ok(serde_json::to_vec(&json!({"code":"0","msg":"","data":data})).unwrap())
        })
    }
}

async fn request(
    service: &AnalyticsService,
    public: &IndependentOkxPublicSource,
    operation: Value,
) -> Result<Value, String> {
    let frame = json!({
        "type":"request","schema":SCHEMA,"session_id":"fixture-research-session",
        "connection_generation":1,"request":{
            "schema":"okx.agent.request/v1","request_id":"fixture-market-request","operation":operation
        }
    });
    let Some(IncomingFrame::Request { request, .. }) =
        parse_frame(Message::Text(frame.to_string().into()))?
    else {
        panic!("expected Research request")
    };
    dispatch(service, public, ResearchCommand::try_from(*request)?).await
}

#[tokio::test]
async fn public_market_operations_work_from_research_owner_and_cursor_reads_persisted_job() {
    let root = tempfile::tempdir().unwrap();
    let venue = Arc::new(Venue::default());
    let public = Arc::new(IndependentOkxPublicSource::with_get(venue.clone()));
    let scope = ResearchScope::new("market-research-only".to_owned()).unwrap();
    let open = || {
        AnalyticsService::new_scoped(
            root.path().into(),
            PublicSourceClient::new(public.clone()),
            &scope,
        )
        .unwrap()
    };
    let service = open();
    let found = request(&service, &public, json!({
        "type":"find_instruments","asset":"BTC","settle_currency":"USDT","instrument_type":"SWAP"
    })).await.unwrap();
    assert_eq!(found["instruments"].as_array().unwrap().len(), 2);
    assert_eq!(found["instruments"][1]["instrument_id"], "BTC-USDT-SWAP");

    let overview = request(
        &service,
        &public,
        json!({
            "type":"market_overview","instrument":"BTC-USDT-SWAP"
        }),
    )
    .await
    .unwrap();
    assert_eq!(overview["source_mode"], "rest");
    assert_eq!(overview["quality"], "degraded");
    assert_eq!(overview["market"]["funding"]["rate"], "0.001");

    let microstructure = request(
        &service,
        &public,
        json!({
            "type":"market_microstructure","instrument":"BTC-USDT-SWAP","size_contracts":"1"
        }),
    )
    .await
    .unwrap();
    assert_eq!(microstructure["quality"], "degraded");
    assert_eq!(microstructure["spread_bps"], "200");
    assert_eq!(microstructure["sweep"]["buy_vwap"], "101");
    assert!(microstructure["book_sequence"].is_null());

    let operation = json!({
        "type":"market","request":{
            "action":"screen","universe":{"instrument_type":"SWAP","rank_globally":true},
            "fields":["returns"],"sort":{"field":"returns","descending":true},
            "timeframe_ms":60000,"start_ms":60000,"end_ms":180000,"limit":1
        }
    });
    let page = request(&service, &public, operation.clone()).await.unwrap();
    assert_eq!(
        page["universe_page"]["reference_generation"],
        found["reference_generation"]
    );
    assert_eq!(page["universe_page"]["universe_size"], 2);
    assert_eq!(page["rows"].as_array().unwrap().len(), 1);
    let mut next = operation;
    next["request"]["cursor"] = page["next_cursor"].clone();
    assert!(next["request"]["cursor"].is_string());

    drop(service);
    let reopened = open();
    let requests_before = venue.0.load(Ordering::Relaxed);
    let second = request(&reopened, &public, next.clone()).await.unwrap();
    assert_eq!(second["job_id"], page["job_id"]);
    assert_eq!(
        second["universe_page"]["reference_generation"],
        found["reference_generation"]
    );
    assert_ne!(
        second["rows"][0]["instrument"],
        page["rows"][0]["instrument"]
    );
    assert!(second["next_cursor"].is_null());
    next["request"]["universe"]["reference_generation"] = json!("another-source-generation");
    assert_eq!(
        request(&reopened, &public, next).await.unwrap_err(),
        "MARKET_CURSOR_REQUEST_MISMATCH"
    );
    assert_eq!(venue.0.load(Ordering::Relaxed), requests_before);
}

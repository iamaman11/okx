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

#[derive(Default)]
struct OfflineVenue(AtomicUsize);

impl PublicGet for OfflineVenue {
    fn get(&self, _path: &'static str, _query: Vec<(&'static str, String)>) -> GetFuture<'_> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::Relaxed);
            Err("RESEARCH_PUBLIC_TRANSPORT_FAILED".into())
        })
    }
}

#[derive(Default)]
struct ExpandedReferenceVenue(AtomicUsize);

impl PublicGet for ExpandedReferenceVenue {
    fn get(&self, path: &'static str, query: Vec<(&'static str, String)>) -> GetFuture<'_> {
        Box::pin(async move {
            assert_eq!(path, "/api/v5/public/instruments");
            let kind = query
                .iter()
                .find(|(name, _)| *name == "instType")
                .map(|(_, value)| value.as_str())
                .expect("required instType");
            let (prefix, count) = match kind {
                "SWAP" => ("A-SWAP", 60),
                "FUTURES" => ("Z-FUTURE", 55),
                other => panic!("unexpected reference type: {other}"),
            };
            self.0.fetch_add(1, Ordering::Relaxed);
            let data = (0..count)
                .map(|index| {
                    json!({
                        "instId":format!("{prefix}-{index:02}"), "instType":kind,
                        "state":"live", "baseCcy":"BTC", "ctValCcy":"BTC", "settleCcy":"USDT",
                        "tickSz":"0.1", "lotSz":"1", "minSz":"1", "ruleType":"normal"
                    })
                })
                .collect::<Vec<_>>();
            Ok(serde_json::to_vec(&json!({"code":"0","msg":"","data":data})).unwrap())
        })
    }
}

#[tokio::test]
async fn research_owner_instrument_type_filter_precedes_public_fifty_row_limit() {
    // A real standalone Research dispatch owns its two public reference GETs.
    // If limit=50 is applied before the FUTURES predicate, the 60 alphabetically
    // earlier SWAPs hide all 55 future instruments: a causal query regression.
    let root = tempfile::tempdir().unwrap();
    let venue = Arc::new(ExpandedReferenceVenue::default());
    let public = Arc::new(IndependentOkxPublicSource::with_get(venue.clone()));
    let scope = ResearchScope::new("futures-before-limit".into()).unwrap();
    let service = AnalyticsService::new_scoped(
        root.path().into(),
        PublicSourceClient::new(public.clone()),
        &scope,
    )
    .unwrap();
    let found = request(
        &service,
        &public,
        json!({
            "type":"find_instruments", "asset":"BTC", "settle_currency":"USDT",
            "instrument_type":"FUTURES"
        }),
    )
    .await
    .unwrap();
    let rows = found["instruments"].as_array().unwrap();
    assert_eq!(rows.len(), 50);
    assert!(rows.iter().all(|row| {
        row["instrument_id"]
            .as_str()
            .unwrap()
            .starts_with("Z-FUTURE-")
    }));
    assert_eq!(
        venue.0.load(Ordering::Relaxed),
        2,
        "one SWAP and one FUTURES public GET"
    );
    assert_eq!(
        request(
            &service,
            &public,
            json!({
                "type":"find_instruments", "asset":"", "settle_currency":null,
                "instrument_type":null
            })
        )
        .await
        .unwrap_err(),
        "INVALID_RESEARCH_REQUEST"
    );
    assert_eq!(
        venue.0.load(Ordering::Relaxed),
        2,
        "invalid search cannot touch source"
    );
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

#[test]
fn research_response_transport_quality_preserves_provenance_and_staleness() {
    for (label, expected) in [
        ("complete", DataQuality::Fresh),
        ("verified", DataQuality::Fresh),
        ("fresh", DataQuality::Fresh),
        ("verified_reexecution", DataQuality::Fresh),
        ("stale", DataQuality::Stale),
        ("partial", DataQuality::Degraded),
        ("missing", DataQuality::Degraded),
        ("degraded", DataQuality::Degraded),
        ("assumed", DataQuality::Degraded),
        ("unverified_legacy", DataQuality::Degraded),
        ("not_verified", DataQuality::Degraded),
        ("unavailable", DataQuality::Degraded),
        ("future_unsupported_quality", DataQuality::Degraded),
    ] {
        let body = json!({"quality":label,"preserved":"same-result"});
        let response = response_for("quality-check", Ok(body.clone()));
        assert_eq!(response.quality, expected, "{label}");
        assert_eq!(response.status, AgentResponseStatus::Completed);
        assert_eq!(response.result, Some(body));
    }
    for unexpected in [
        serde_json::Value::Null,
        json!(42),
        json!({"status":"ambiguous"}),
    ] {
        let body = json!({"quality":unexpected});
        let response = response_for("quality-check", Ok(body.clone()));
        assert_eq!(response.quality, DataQuality::Degraded);
        assert_eq!(response.result, Some(body));
    }
    assert_eq!(
        response_for("quality-check", Err("RESEARCH_SOURCE_FAILED".into())).quality,
        DataQuality::NotReady
    );
}

#[tokio::test]
async fn invalid_market_view_names_never_reserve_source_or_socket() {
    // These names pass the broader public wire envelope grammar but cannot be
    // queried as OKX venue instruments. Reject *before* the WSS session is
    // created, or repeated malformed requests can occupy both WSS slots.
    let root = tempfile::tempdir().unwrap();
    let venue = Arc::new(Venue::default());
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let public = Arc::new(
        IndependentOkxPublicSource::with_get(venue.clone())
            .with_market_ws_endpoint(format!("ws://{}", listener.local_addr().unwrap())),
    );
    let scope = ResearchScope::new("malformed-public-market-view".into()).unwrap();
    let service = AnalyticsService::new_scoped(
        root.path().into(),
        PublicSourceClient::new(public.clone()),
        &scope,
    )
    .unwrap();
    for instrument in [
        "BTC__USDT_SWAP",
        "BTC--USDT-SWAP",
        "-BTC-USDT-SWAP",
        "BTC-USDT-SWAP-",
    ] {
        for operation in [
            json!({"type":"market_overview","instrument":instrument}),
            json!({"type":"market_microstructure","instrument":instrument}),
        ] {
            let result = tokio::time::timeout(
                Duration::from_millis(200),
                request(&service, &public, operation),
            )
            .await
            .expect("a rejected public market request must have no source delay");
            assert_eq!(result.unwrap_err(), "INSTRUMENT_INVALID");
        }
    }
    assert_eq!(venue.0.load(Ordering::Relaxed), 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err(),
        "invalid market names must not open a Research public WSS"
    );
}

#[tokio::test]
async fn standalone_market_only_runtime_fails_closed_without_engine_or_public_source() {
    // Disposable Research catalog, a recorded failing GET, and a closed
    // loopback-only public WSS endpoint are the complete environment: no
    // Engine process, account DB, credentials or external network.
    let root = tempfile::tempdir().unwrap();
    let venue = Arc::new(OfflineVenue::default());
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let disconnected = listener.local_addr().unwrap();
    drop(listener);
    let public = Arc::new(
        IndependentOkxPublicSource::with_get(venue.clone())
            .with_market_ws_endpoint(format!("ws://{disconnected}")),
    );
    let scope = ResearchScope::new("market-offline-standalone".into()).unwrap();
    let service = AnalyticsService::new_scoped(
        root.path().into(),
        PublicSourceClient::new(public.clone()),
        &scope,
    )
    .unwrap();

    let capabilities = request(&service, &public, json!({"type":"market_capabilities"}))
        .await
        .unwrap();
    assert_eq!(capabilities["effects"]["venue_post"], false);
    assert_eq!(capabilities["effects"]["write_finance"], false);
    assert_eq!(venue.0.load(Ordering::Relaxed), 0);

    // A Research identity must never fall back to Engine for private or
    // account-bound actions, including when no public source is reachable.
    for operation in [
        json!({"type":"trade","request":{"action":"capabilities"}}),
        json!({"type":"strategies","request":{"action":"list"}}),
        json!({"type":"account_summary"}),
    ] {
        assert_eq!(
            request(&service, &public, operation).await.unwrap_err(),
            "RESEARCH_OPERATION_FORBIDDEN"
        );
    }
    assert_eq!(venue.0.load(Ordering::Relaxed), 0);

    // All three source-backed public routes must fail closed with their own
    // transport error, even if the independently owned WSS is unavailable.
    // Neither a cached empty view nor Engine fallback can replace evidence.
    for (name, operation) in [
        (
            "instruments",
            json!({"type":"find_instruments", "asset":"BTC",
                "settle_currency":"USDT", "instrument_type":"SWAP"}),
        ),
        (
            "snapshot",
            json!({"type":"market_overview", "instrument":"BTC-USDT-SWAP"}),
        ),
        (
            "microstructure",
            json!({"type":"market_microstructure", "instrument":"BTC-USDT-SWAP",
                "size_contracts":"1"}),
        ),
    ] {
        let reads_before = venue.0.load(Ordering::Relaxed);
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            request(&service, &public, operation),
        )
        .await
        .unwrap_or_else(|_| panic!("{name}: offline Research read hung"));
        assert_eq!(
            result.unwrap_err(),
            "RESEARCH_PUBLIC_TRANSPORT_FAILED",
            "{name}"
        );
        assert!(
            venue.0.load(Ordering::Relaxed) > reads_before,
            "{name}: failed Research read did not use its own public GET"
        );
    }
    // A source failure does not poison the purely local operator catalogue.
    let reads_after_failure = venue.0.load(Ordering::Relaxed);
    assert_eq!(
        request(&service, &public, json!({"type":"market_capabilities"}))
            .await
            .unwrap(),
        capabilities
    );
    assert_eq!(venue.0.load(Ordering::Relaxed), reads_after_failure);

    drop(service);
    let conn = rusqlite::Connection::open(root.path().join("research-catalog.sqlite")).unwrap();
    let jobs: i64 = conn
        .query_row("SELECT COUNT(*) FROM research_jobs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(jobs, 0);
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

    let diagnostic = |reference: Value| {
        json!({"type":"diagnostics","request":{
            "reference":reference,"view":"research","account":null,"cycle_id":null,"limit":null
        }})
    };
    let observed = request(&reopened, &public, diagnostic(page["job_id"].clone()))
        .await
        .unwrap();
    assert_eq!(observed["owner"], "research_job_catalog");
    assert_eq!(observed["state"], "OBSERVED");
    assert_eq!(observed["job"]["state"], "completed");
    assert_eq!(observed["read_only"], true);
    let response = response_for(
        "fixture-market-request",
        request(&reopened, &public, diagnostic(json!("deployment"))).await,
    );
    assert_eq!(response.quality, DataQuality::NotReady);
    assert_eq!(
        response.failure.unwrap().code,
        "RESEARCH_DEPLOYMENT_NOT_SUPERVISED"
    );
}

#[tokio::test]
async fn market_views_use_research_websocket_and_downgrade_a_gapped_book() {
    const INSTRUMENT: &str = "BTC-USDT-SWAP";
    let root = tempfile::tempdir().unwrap();
    let scope = ResearchScope::new("research-live-views".to_owned()).unwrap();
    let venue = Arc::new(Venue::default());
    let base = Arc::new(IndependentOkxPublicSource::with_get(venue.clone()));
    let service =
        AnalyticsService::new_scoped(root.path().into(), PublicSourceClient::new(base), &scope)
            .unwrap();

    for (operation, fault) in [
        (
            json!({"type":"market_overview","instrument":INSTRUMENT}),
            None,
        ),
        (
            json!({"type":"market_microstructure","instrument":INSTRUMENT,"size_contracts":"1"}),
            None,
        ),
        (
            json!({"type":"market_microstructure","instrument":INSTRUMENT,"size_contracts":"1"}),
            Some("gap"),
        ),
        (
            json!({"type":"market_microstructure","instrument":INSTRUMENT,"size_contracts":"1"}),
            Some("reference_handoff"),
        ),
    ] {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let public = IndependentOkxPublicSource::with_get(venue.clone())
            .with_market_ws_endpoint(format!("ws://{}", listener.local_addr().unwrap()));
        let before = venue.0.load(Ordering::Relaxed);
        let mock = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            public_ws::tests::serve_market_fixture(socket, fault).await;
        });
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            request(&service, &public, operation.clone()),
        )
        .await
        .expect("Research is expected to use its own venue socket");
        drop(public);
        tokio::time::timeout(Duration::from_secs(1), mock)
            .await
            .unwrap()
            .unwrap();
        let response = response_for("fixture-market-request", result);
        let actual = response.result.unwrap();
        if fault.is_some() {
            assert_eq!(response.quality, DataQuality::Degraded);
            assert_eq!(actual["quality"], "degraded");
            match fault {
                Some("gap") => {
                    assert_eq!(
                        actual["public_ws_quality"]["reason_code"],
                        "BOOK_SEQUENCE_INVALID"
                    );
                    assert_eq!(venue.0.load(Ordering::Relaxed) - before, 3);
                }
                Some("reference_handoff") => {
                    assert_eq!(
                        actual["public_ws_quality"]["reason_code"],
                        "RESEARCH_PUBLIC_WS_REFERENCE_CHANGED"
                    );
                }
                _ => panic!("unexpected fixture fault"),
            }
            assert!(actual["book_sequence"].is_null());
        } else {
            assert_eq!(response.quality, DataQuality::Fresh);
            assert_eq!(actual["quality"], "complete");
            assert_eq!(actual["generation"], 1);
            assert_eq!(actual["book_sequence"], 11);
            assert_eq!(venue.0.load(Ordering::Relaxed) - before, 2);
            if operation["type"] == "market_overview" {
                assert_eq!(actual["source_mode"], "websocket");
                assert_eq!(actual["market"]["funding"]["rate"], "0.001");
            } else {
                assert_eq!(actual["spread_bps"], "200");
                assert_eq!(actual["asks"][0]["size"], "3");
                assert_eq!(actual["sweep"]["buy_vwap"], "101");
            }
        }
    }
}

#[tokio::test]
async fn warming_venue_session_survives_a_completed_rest_request() {
    let root = tempfile::tempdir().unwrap();
    let scope = ResearchScope::new("research-live-views".to_owned()).unwrap();
    let venue = Arc::new(Venue::default());
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let public = Arc::new(
        IndependentOkxPublicSource::with_get(venue.clone())
            .with_market_ws_endpoint(format!("ws://{}", listener.local_addr().unwrap())),
    );
    let service = AnalyticsService::new_scoped(
        root.path().into(),
        PublicSourceClient::new(public.clone()),
        &scope,
    )
    .unwrap();
    let (release, allowed) = tokio::sync::oneshot::channel();
    let mock = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        public_ws::tests::serve_market_fixture_after(socket, None, Some(allowed)).await;
    });
    let operation = json!({"type":"market_microstructure","instrument":"BTC-USDT-SWAP"});
    let fallback = request(&service, &public, operation.clone()).await.unwrap();
    assert_eq!(fallback["quality"], "degraded");
    assert_eq!(
        fallback["public_ws_quality"]["reason_code"],
        "RESEARCH_PUBLIC_WS_WARMING"
    );
    assert_eq!(venue.0.load(Ordering::Relaxed), 3);

    release.send(()).unwrap();
    let warm = request(&service, &public, operation).await.unwrap();
    assert_eq!(warm["quality"], "complete");
    assert_eq!(warm["generation"], 1);
    assert_eq!(warm["book_sequence"], 11);
    assert_eq!(venue.0.load(Ordering::Relaxed), 3);

    drop(service);
    drop(public);
    tokio::time::timeout(Duration::from_secs(1), mock)
        .await
        .unwrap()
        .unwrap();
}

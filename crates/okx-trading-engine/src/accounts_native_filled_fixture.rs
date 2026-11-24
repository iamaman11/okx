//! Finite external observations for the original Native fill/Close owners.
use super::*;

pub(crate) const INSTRUMENT: &str = "BTC-USDT-SWAP";
pub(crate) const OPEN_ORDER: &str = "native-filled-open-001";
pub(super) const CLOSE_ORDER: &str = "native-filled-close-001";
pub(crate) const STOP: &str = "900000002";

/// Test-only phase observations for the four serialized financial lifecycle
/// witnesses. A typed identity fixes both the receipt filename and the exact
/// libtest witness; no caller-controlled path or account data is written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ClockFixture {
    FilledCloseReopen,
    StoppedClose,
    EmergencyClose,
    LateFeeRebate,
}

impl ClockFixture {
    const ALL: [Self; 4] = [
        Self::FilledCloseReopen,
        Self::StoppedClose,
        Self::EmergencyClose,
        Self::LateFeeRebate,
    ];

    const fn slug(self) -> &'static str {
        match self {
            Self::FilledCloseReopen => "filled_close_reopen",
            Self::StoppedClose => "stopped_close",
            Self::EmergencyClose => "emergency_close",
            Self::LateFeeRebate => "late_fee_rebate",
        }
    }

    const fn witness(self) -> &'static str {
        match self {
            Self::FilledCloseReopen => {
                "accounts::native_trade_contracts::native_filled_close::risk_book_contract_native_filled_close_settlement_release_and_cold_reopen"
            }
            Self::StoppedClose => {
                "accounts::native_trade_contracts::native_filled_close::risk_book_contract_native_stop_after_fill_blocks_open_but_closes_and_releases"
            }
            Self::EmergencyClose => {
                "accounts::native_trade_contracts::native_filled_close::risk_book_contract_native_failed_stop_emergency_close_and_verified_remediation"
            }
            Self::LateFeeRebate => {
                "accounts::native_trade_contracts::native_fee_correction::risk_book_contract_native_late_fee_rebate_and_bound_breach_use_immutable_revisions"
            }
        }
    }
}

pub(super) struct ClockPhaseTimings {
    fixture: ClockFixture,
    started: std::time::Instant,
    previous_ms: u64,
    phases: Vec<serde_json::Value>,
}

impl ClockPhaseTimings {
    pub(super) fn new(fixture: ClockFixture) -> Self {
        let started = std::time::Instant::now();
        Self {
            fixture,
            started,
            previous_ms: 0,
            phases: Vec::new(),
        }
    }

    pub(super) fn mark(&mut self, phase: &'static str) {
        let now = std::time::Instant::now();
        let cumulative_ms = u64::try_from(now.duration_since(self.started).as_millis())
            .expect("bounded finance phase timer");
        self.phases.push(serde_json::json!({
            "phase": phase,
            // Subtract the same integer epoch, so sum(wall_ms) always equals
            // the final cumulative_ms even for sub-millisecond phases.
            "wall_ms": cumulative_ms - self.previous_ms,
            "cumulative_ms": cumulative_ms,
        }));
        self.previous_ms = cumulative_ms;
    }

    pub(super) fn finish(self) {
        // Ordinary Cargo tests produce no telemetry side effects. The current
        // financial collector supplies BOTH variables for its private run dir;
        // a partial environment is an explicit failure, never a silent skip.
        let (dir, run_id) = match (
            std::env::var_os("OKX_NATIVE_ACCEPTANCE_WORKLOAD_DIR"),
            std::env::var_os("OKX_NATIVE_ACCEPTANCE_RUN_ID"),
        ) {
            (None, None) => return,
            (Some(dir), Some(run_id)) => (dir, run_id),
            _ => panic!("INCOMPLETE_FINANCIAL_PHASE_RECEIPT_CONTEXT"),
        };
        let run_id = run_id
            .to_str()
            .expect("financial acceptance run ID is UTF-8");
        assert!(!run_id.is_empty(), "EMPTY_FINANCIAL_PHASE_RECEIPT_RUN_ID");
        assert!(!self.phases.is_empty(), "EMPTY_FINANCIAL_PHASE_TIMINGS");
        let receipt = serde_json::json!({
            "schema": "okx.finance.clock-phase/v1",
            "run_id": run_id,
            "test": self.fixture.slug(),
            "witness": self.fixture.witness(),
            "phases": self.phases,
        });
        let bytes = serde_json::to_vec(&receipt).expect("SERIALIZE_FINANCIAL_PHASE_RECEIPT");
        assert!(bytes.len() < 16 * 1024, "FINANCIAL_PHASE_RECEIPT_TOO_LARGE");
        let path =
            std::path::Path::new(&dir).join(format!("finance-phase-{}.json", self.fixture.slug()));
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .expect("CREATE_UNIQUE_FINANCIAL_PHASE_RECEIPT");
        file.write_all(&bytes)
            .expect("WRITE_FINANCIAL_PHASE_RECEIPT");
        file.write_all(b"\n")
            .expect("FINALIZE_FINANCIAL_PHASE_RECEIPT");
    }
}

/// A finite scripted external clock publisher for Native contract tests that
/// may spend several seconds in the original SQL admission path on Windows.
/// Only the one not-yet-delivered raw venue response can be refreshed; the
/// production adapter still parses that response and enforces its unchanged
/// five-second mutation-clock bound. No response is created on demand.
pub(crate) async fn with_fresh_recorded_clock<F: std::future::Future>(
    feed: &RecordedAccountHttp,
    execution: F,
) -> F::Output {
    assert_eq!(feed.unconsumed_public_clock(), 1);
    let mut publisher = tokio::time::interval(Duration::from_millis(250));
    publisher.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut publisher_ticks = 0;
    tokio::pin!(execution);
    loop {
        tokio::select! {
            biased;
            _ = publisher.tick(), if publisher_ticks < 80 => {
                publisher_ticks += 1;
                let now = jiff::Timestamp::now().as_millisecond().to_string();
                // A delivered clock sample cannot be replaced or multiplied.
                let _ = feed.refresh_unconsumed_public_clock(
                    0,
                    envelope(json!([{"ts": now}])),
                );
            }
            outcome = &mut execution => return outcome,
        }
    }
}

/// Exercise the original production campaign-readiness refresh while a
/// different account is deliberately held at an external HTTP boundary.
/// `opened_market` stops bootstrap maintenance before installing finite market
/// transcripts; without this scoped continuation a slow CI executor makes
/// the real five-second SQL readiness fence expire during the test itself.
/// This never changes the clock, source generations, balances or safety rules.
pub(super) fn keep_captured_readiness_current(
    fixture: &NativeFixture,
) -> (watch::Sender<bool>, tokio::task::JoinHandle<()>) {
    let service = fixture.trade.clone();
    let authority = fixture.authority.clone();
    let (stop, mut stopped) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = stopped.changed() => {
                    if *stopped.borrow() { break; }
                }
                _ = tick.tick() => {
                    service
                        .install_captured_empty_campaign_opening(
                            &authority,
                            Instant::now() + Duration::from_secs(4),
                        )
                        .await
                        .expect("original captured campaign readiness remains verifiable");
                }
            }
        }
    });
    (stop, task)
}

pub(super) async fn stop_captured_readiness(
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
) {
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
}

pub(crate) async fn opened_market(
    fixture: &mut NativeFixture,
) -> (
    watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
    Arc<RecordedWsConnector>,
    okx_market_runtime::PublicWsHandle,
) {
    opened_market_with_public_client(fixture, None).await
}

pub(super) async fn opened_market_with_public_client(
    fixture: &mut NativeFixture,
    client: Option<okx_api::OkxPublicClient>,
) -> (
    watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
    Arc<RecordedWsConnector>,
    okx_market_runtime::PublicWsHandle,
) {
    let (stop, stopped) = watch::channel(false);
    let service = fixture.trade.clone();
    let maintenance = tokio::spawn(async move { service.run(stopped).await });
    let mut observations = tokio::time::interval(Duration::from_millis(25));
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            observations.tick().await;
            if fixture
                .owner
                .campaign_summary(fixture.campaign.clone())
                .await
                .unwrap()
                .opening
                .is_some()
            {
                break;
            }
        }
    })
    .await
    .expect("production maintenance opening before external fills");
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), maintenance)
        .await
        .unwrap()
        .unwrap();
    let (market, public, socket) = raw_market_with_public_client(client);
    let mut updates = public.subscribe_market_updates();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(async move {
        market.run(stopped).await.unwrap();
    });
    public.demand_instrument(INSTRUMENT).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if public
                .risk_fresh_snapshot(
                    INSTRUMENT,
                    jiff::Timestamp::now().as_millisecond() as u64,
                    2000,
                    180000,
                    jiff::Timestamp::now().to_string(),
                )
                .await
                .is_ok()
            {
                break;
            }
            updates.changed().await.unwrap();
        }
    })
    .await
    .expect("original raw public market parser");
    *fixture.market_boundary.market.lock().unwrap() = Some((socket.clone(), public.clone()));
    fixture.trade = Arc::new(TradeService::new(
        fixture.registry.clone(),
        public.clone(),
        NativeRiskClient::local(NativeFinanceRisk::from_owner(fixture.owner.clone())),
    ));
    fixture
        .trade
        .update_governance(&fixture.authority)
        .await
        .unwrap();
    (stop, task, socket, public)
}

pub(super) fn get(
    feed: &RecordedAccountHttp,
    actor: &str,
    path: &str,
    query: Vec<(&str, &str)>,
    data: Value,
) {
    feed.expect_get(RecordedGet {
        access_key: actor.into(),
        path: path.into(),
        required_query: query
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect(),
        raw_response: envelope(data),
    });
}

/// One independently timestamped balance and position snapshot. Repetition is
/// explicit at the caller: each item names a real production assessment.
pub(crate) fn snapshot(feed: &RecordedAccountHttp, equity: &str, quantity: &str) {
    let at = jiff::Timestamp::now().as_millisecond().to_string();
    get(
        feed,
        "fixture-observer-a",
        "/api/v5/account/balance",
        vec![],
        json!([{
        "totalEq":equity,"adjEq":equity,"uTime":at,"details":[{"ccy":"USDT","eq":equity,
        "liab":"0","availEq":equity,"availBal":equity,"eqUsd":equity,"uTime":at}]}]),
    );
    let positions = if quantity == "0" {
        json!([])
    } else {
        json!([{
        "instType":"SWAP","instId":INSTRUMENT,"posSide":"long","pos":quantity,
        "mgnMode":"cross","avgPx":"1000","markPx":"1000","uTime":at}])
    };
    get(
        feed,
        "fixture-observer-a",
        "/api/v5/account/positions",
        vec![],
        positions,
    );
}

/// Exactly one production monetary capture turn, five original partitions and
/// one independently signed bill page. Later turns may be empty incremental
/// windows; immutable prior fills and bills remain in the original SQL owner.
pub(crate) fn history(feed: &RecordedAccountHttp, execution: Option<(&str, &str, &str, i64)>) {
    history_amount(feed, execution, "0.1", "-0.0005");
}

pub(super) fn history_amount(
    feed: &RecordedAccountHttp,
    execution: Option<(&str, &str, &str, i64)>,
    size: &str,
    fee: &str,
) {
    history_amount_sources(feed, execution, size, fee, true);
}

/// A real fill page whose independent fee bill has not arrived yet.
pub(super) fn history_unbilled_amount(
    feed: &RecordedAccountHttp,
    execution: (&str, &str, &str, i64),
    size: &str,
    fee: &str,
) {
    history_amount_sources(feed, Some(execution), size, fee, false);
}

fn history_amount_sources(
    feed: &RecordedAccountHttp,
    execution: Option<(&str, &str, &str, i64)>,
    size: &str,
    fee: &str,
    bill_available: bool,
) {
    let (fills, bills) = match execution {
        Some((order, event, side, at)) => {
            let subtype = if side == "buy" { "3" } else { "5" };
            (
                json!([{"instType":"SWAP","instId":INSTRUMENT,
                "tradeId":format!("{event}-trade"),"ordId":order,"clOrdId":"__REQUEST_CL_ORD_ID__",
                "billId":format!("{event}-bill"),"subType":subtype,"side":side,"posSide":"long",
                "fillPx":"1000","fillSz":size,"fillPnl":"0","fee":fee,"feeCcy":"USDT",
                "ts":at.to_string(),"fillTime":at.to_string()}]),
                if bill_available {
                    json!([{"billId":format!("{event}-bill"),"instType":"SWAP","instId":INSTRUMENT,
                "ordId":order,"tradeId":format!("{event}-trade"),"clOrdId":"__REQUEST_CL_ORD_ID__",
                "ccy":"USDT","type":"2","subType":subtype,"balChg":fee,
                "fee":fee,"pnl":"0","ts":at.to_string()}])
                } else {
                    json!([])
                },
            )
        }
        None => (json!([]), json!([])),
    };
    for partition in ["SPOT", "MARGIN", "SWAP", "FUTURES", "OPTION"] {
        get(
            feed,
            "fixture-executor-a",
            "/api/v5/trade/fills-history",
            vec![("instType", partition)],
            if partition == "SWAP" {
                fills.clone()
            } else {
                json!([])
            },
        );
    }
    get(
        feed,
        "fixture-executor-a",
        "/api/v5/account/bills-archive",
        vec![],
        bills,
    );
}

pub(crate) fn order(feed: &RecordedAccountHttp, id: &str, side: &str, at: i64) {
    order_cumulative(feed, id, side, at, "filled", "0.1");
}

pub(super) fn order_cumulative(
    feed: &RecordedAccountHttp,
    id: &str,
    side: &str,
    at: i64,
    state: &str,
    cumulative_size: &str,
) {
    get(
        feed,
        "fixture-executor-a",
        "/api/v5/trade/order",
        vec![("instId", INSTRUMENT)],
        json!([{
        "instId":INSTRUMENT,"ordId":id,"clOrdId":"__REQUEST_CL_ORD_ID__","side":side,
        "posSide":"long","tdMode":"cross","ordType":if side == "buy" {"limit"} else {"ioc"},
        "px":if side == "buy" {"1000"} else {"999"},"sz":"0.1","accFillSz":cumulative_size,
        "avgPx":"1000","state":state,"cTime":at.to_string(),"uTime":at.to_string()}]),
    );
}

pub(crate) fn protection(feed: &RecordedAccountHttp, client: &str) {
    get(
        feed,
        "fixture-executor-a",
        "/api/v5/trade/order-algo",
        vec![("algoClOrdId", client)],
        json!([{
        "instId":INSTRUMENT,"algoId":STOP,"algoClOrdId":client,"state":"live","sz":"0.1",
        "side":"sell","posSide":"long","tdMode":"cross","ordType":"conditional",
        "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"}]),
    );
}

pub(crate) async fn capture(fixture: &NativeFixture) {
    crate::source_capture::capture_account_sources_measured(
        &fixture._account.observer,
        &fixture._account.private_ws,
        &fixture._account.execution,
    )
    .await
    .expect("original signed history ingestion, lot projection and account NAV");
    let (risk, stops) = fixture
        .owner
        .financial_readiness(
            fixture.config.account_key.wire(),
            None,
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert!(risk.nav_complete, "original NAV completeness: {risk:?}");
    assert_eq!(stops, 0, "no source or projection stop may be hidden");
}

pub(super) async fn close_market(
    socket: &RecordedWsConnector,
    public: &okx_market_runtime::PublicWsHandle,
) {
    let at = jiff::Timestamp::now().as_millisecond().to_string();
    let mut parsed = public.subscribe_market_updates();
    socket.send_public_frame(
        json!({"arg":{"channel":"tickers","instId":INSTRUMENT},"data":[{
        "instType":"SWAP","instId":INSTRUMENT,"last":"1000","lastSz":"1","askPx":"1000",
        "askSz":"10","bidPx":"999.9","bidSz":"10","ts":at}]})
        .to_string(),
    );
    socket.send_public_frame(
        json!({"arg":{"channel":"mark-price","instId":INSTRUMENT},"data":[{
        "instType":"SWAP","instId":INSTRUMENT,"markPx":"1000","ts":at}]})
        .to_string(),
    );
    socket.send_public_frame(
        json!({"arg":{"channel":"index-tickers","instId":"BTC-USDT"},"data":[{
        "instId":"BTC-USDT","idxPx":"1000","ts":at}]})
        .to_string(),
    );
    socket.send_public_frame(
        json!({"arg":{"channel":"books","instId":INSTRUMENT},"action":"update","data":[{
        "asks":[["1000","10","0","1"]],"bids":[["999.9","10","0","1"]],
        "seqId":13,"prevSeqId":12,"ts":at}]})
        .to_string(),
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            parsed.changed().await.unwrap();
            if public
                .risk_fresh_snapshot(
                    INSTRUMENT,
                    jiff::Timestamp::now().as_millisecond() as u64,
                    2000,
                    180000,
                    jiff::Timestamp::now().to_string(),
                )
                .await
                .is_ok()
            {
                break;
            }
        }
    })
    .await
    .expect("single named Close market observation");
}

pub(super) fn cleanup(feed: &RecordedAccountHttp, stop_client: &str) {
    let mut before = vec![];
    let mut after = vec![];
    for kind in ["SWAP", "FUTURES"] {
        for order_type in [
            "conditional",
            "oco",
            "trigger",
            "move_order_stop",
            "chase",
            "iceberg",
            "twap",
            "smart_iceberg",
        ] {
            let query = vec![
                ("instType".into(), kind.into()),
                ("ordType".into(), order_type.into()),
                ("limit".into(), "100".into()),
            ];
            let page = |data| RecordedGet {
                access_key: "fixture-observer-a".into(),
                path: "/api/v5/trade/orders-algo-pending".into(),
                required_query: query.clone(),
                raw_response: envelope(data),
            };
            before.push(page(if kind == "SWAP" && order_type == "conditional" {json!([{
                "instType":"SWAP","instId":INSTRUMENT,"algoId":STOP,"algoClOrdId":stop_client,
                "ordType":"conditional","state":"live","side":"sell","posSide":"long","tdMode":"cross",
                "sz":"0.1","slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"}])} else {json!([])}));
            after.push(page(json!([])));
        }
    }
    feed.expect_causal_algo_inventory(STOP, before, after);
    feed.expect_public_clock(envelope(
        json!([{"ts":jiff::Timestamp::now().as_millisecond().to_string()}]),
    ));
    feed.expect_post(RecordedPost {
        access_key: "fixture-executor-a".into(),
        path: "/api/v5/trade/cancel-algos".into(),
        expected_body: json!([{"instId":INSTRUMENT,"algoId":STOP}]),
        raw_response: envelope(json!([{"algoId":STOP,"sCode":"0"}])),
    });
}

#[cfg(test)]
mod clock_phase_tests {
    use super::*;

    #[test]
    fn clock_phase_receipts_have_distinct_fixed_safe_identities() {
        let mut slugs = std::collections::BTreeSet::new();
        let mut witnesses = std::collections::BTreeSet::new();
        for fixture in ClockFixture::ALL {
            let slug = fixture.slug();
            assert!(slug.bytes().all(|c| c.is_ascii_lowercase() || c == b'_'));
            assert!(slugs.insert(slug));
            assert!(witnesses.insert(fixture.witness()));
            assert!(
                fixture
                    .witness()
                    .starts_with("accounts::native_trade_contracts::")
            );
        }
        assert_eq!(slugs.len(), 4);
        assert_eq!(witnesses.len(), 4);
    }

    #[test]
    fn clock_phase_deltas_sum_to_the_same_monotonic_epoch() {
        let mut timings = ClockPhaseTimings::new(ClockFixture::FilledCloseReopen);
        for phase in ["fixture_setup", "market_open", "shutdown"] {
            timings.mark(phase);
        }
        let mut elapsed = 0_u64;
        for sample in &timings.phases {
            elapsed += sample["wall_ms"].as_u64().unwrap();
            assert_eq!(sample["cumulative_ms"].as_u64(), Some(elapsed));
        }
        assert_eq!(timings.phases.len(), 3);
    }
}

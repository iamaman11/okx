//! Finite raw venue inputs and fresh Native composition; no alternate ledger.
use super::*;

// Finite external market observations, causally delivered after each of the
// two Execute inventory scans. No periodic publisher or source freshness bypass.
pub(super) struct ExecuteMarketBoundary {
    pub(super) feed: Arc<RecordedAccountHttp>,
    pub(super) market: Mutex<Option<(Arc<RecordedWsConnector>, okx_runtime::PublicWsHandle)>>,
    pub(super) updates: AtomicUsize,
    // Hold one real planner before intent preparation. Other public callers
    // must return their actual no-send result, not consume spare HTTP pages.
    pub(super) planning_fee_gate: Mutex<Option<Arc<PlanningFeeGate>>>,
    pub(super) planning_fee_gate_actor: Mutex<Option<String>>,
    pub(super) planning_fee_skip: AtomicUsize,
    pub(super) market_updates_expected: AtomicUsize,
    market_delivery: tokio::sync::Mutex<()>,
    // A single corrupt raw ACK must reach the production identity verifier,
    // rather than becoming a transcript-guard transport error first.
    pub(super) foreign_create_ack: std::sync::atomic::AtomicBool,
}

#[derive(Default)]
pub(super) struct PlanningFeeGate {
    pub(super) entered: tokio::sync::Notify,
    pub(super) release: tokio::sync::Notify,
}
impl HttpTransport for ExecuteMarketBoundary {
    fn send(&self, request: HttpRequest, deadline: Instant) -> HttpSend<'_> {
        Box::pin(async move {
            let actor_matches = self
                .planning_fee_gate_actor
                .lock()
                .unwrap()
                .as_ref()
                .is_none_or(|actor| {
                    request
                        .headers()
                        .get("OK-ACCESS-KEY")
                        .and_then(|v| v.to_str().ok())
                        == Some(actor.as_str())
                });
            let skip = if actor_matches && request.url().path() == "/api/v5/account/trade-fee" {
                self.planning_fee_skip
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_sub(1))
                    .is_ok()
            } else {
                false
            };
            let planning_gate =
                if actor_matches && !skip && request.url().path() == "/api/v5/account/trade-fee" {
                    self.planning_fee_gate.lock().unwrap().take()
                } else {
                    None
                };
            if let Some(gate) = planning_gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            let last_inventory = request.url().path() == "/api/v5/trade/orders-algo-pending"
                && request
                    .url()
                    .query_pairs()
                    .any(|(k, v)| k == "instType" && v == "FUTURES")
                && request
                    .url()
                    .query_pairs()
                    .any(|(k, v)| k == "ordType" && v == "smart_iceberg");
            let create =
                request.method() == "POST" && request.url().path() == "/api/v5/trade/order";
            // Different accounts may complete inventory concurrently; raw
            // sequence frames still have one ordered physical delivery lane.
            let _delivery = if last_inventory {
                Some(self.market_delivery.lock().await)
            } else {
                None
            };
            let result = self.feed.send(request, deadline).await;
            // A real child process stops after the venue consumed Create,
            // before Execute can persist the transport outcome. Only this
            // precise test flag and original lost-ACK response arm the probe.
            if create
                && std::env::var("OKX_NATIVE_CRASH_CHILD").as_deref()
                    == Ok("POST_CREATE_PRE_RESULT")
                && matches!(&result, Err(okx_api::OkxError::Response(reason))
                    if reason == "TRANSCRIPT_POST_ACK_LOST_AFTER_SEND")
            {
                assert!(self.feed.lost_create_ack_was_consumed());
                assert_eq!(self.feed.outbound_posts.lock().unwrap().len(), 1);
                use std::io::Write;
                println!("OKX_NATIVE_OS_CRASH_POST_CREATE_PRE_RESULT");
                std::io::stdout().flush().unwrap();
                std::future::pending::<()>().await;
                unreachable!();
            }
            let mut response = result?;
            if create && self.foreign_create_ack.swap(false, Ordering::SeqCst) {
                let mut raw: Value = serde_json::from_slice(&response.body).unwrap();
                raw["data"][0]["clOrdId"] = json!("foreign-client-order");
                response.body = serde_json::to_vec(&raw).unwrap();
            }
            let market = if last_inventory {
                self.market.lock().unwrap().clone()
            } else {
                None
            };
            if let Some((socket, public)) = market {
                let index = self.updates.fetch_add(1, Ordering::SeqCst);
                assert!(
                    index < self.market_updates_expected.load(Ordering::SeqCst),
                    "only pre-reserve and post-held inventory observations are scripted"
                );
                let timestamp = chrono::Utc::now().timestamp_millis();
                let mut parsed = public.subscribe_market_updates();
                socket.send_public_frame(json!({"arg":{"channel":"tickers","instId":"BTC-USDT-SWAP"},
                    "data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","last":"1000","lastSz":"1",
                    "askPx":"1000","askSz":"10","bidPx":"999.9","bidSz":"10","ts":timestamp.to_string()}]}).to_string());
                socket.send_public_frame(json!({"arg":{"channel":"mark-price","instId":"BTC-USDT-SWAP"},
                    "data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","markPx":"1000","ts":timestamp.to_string()}]}).to_string());
                socket.send_public_frame(
                    json!({"arg":{"channel":"index-tickers","instId":"BTC-USDT"},
                    "data":[{"instId":"BTC-USDT","idxPx":"1000","ts":timestamp.to_string()}]})
                    .to_string(),
                );
                socket.send_public_frame(
                    json!({"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"update",
                    "data":[{"asks":[["1000","10","0","1"]],"bids":[["999.9","10","0","1"]],
                    "seqId":11+index,"prevSeqId":10+index,"ts":timestamp.to_string()}]})
                    .to_string(),
                );
                tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
                    loop {
                        parsed
                            .changed()
                            .await
                            .expect("market task must remain running");
                        if public
                            .risk_fresh_snapshot(
                                "BTC-USDT-SWAP",
                                chrono::Utc::now().timestamp_millis() as u64,
                                2000,
                                180000,
                                chrono::Utc::now().to_rfc3339(),
                            )
                            .await
                            .is_ok()
                        {
                            break;
                        }
                    }
                })
                .await
                .unwrap_or_else(|_| panic!("finite market observation not parsed: phase={index}"));
            }
            Ok(response)
        })
    }
}

pub(super) fn envelope(data: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({"code":"0","msg":"","data":data})).unwrap()
}

pub(super) fn record_fee(feed: &RecordedAccountHttp, observed_at: &str) {
    feed.expect_get(RecordedGet {
        access_key: "fixture-observer-a".into(),
        path: "/api/v5/account/trade-fee".into(),
        required_query: vec![
            ("instType".into(), "SWAP".into()),
            ("instFamily".into(), "BTC-USDT".into()),
        ],
        raw_response: envelope(json!([{"instType":"SWAP","level":"Lv1","ts":observed_at,
            "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]}])),
    });
}

pub(super) fn record_leverage(feed: &RecordedAccountHttp) {
    feed.expect_get(RecordedGet {
        access_key: "fixture-observer-a".into(),
        path: "/api/v5/account/leverage-info".into(),
        required_query: vec![
            ("instId".into(), "BTC-USDT-SWAP".into()),
            ("mgnMode".into(), "cross".into()),
        ],
        raw_response: envelope(json!([
            {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"long","lever":"2"},
            {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"short","lever":"2"}])),
    });
}

/// Finite original venue bytes. Only production WS parsing publishes prices.
pub(super) fn raw_market() -> (
    okx_runtime::PublicWsCoordinator,
    okx_runtime::PublicWsHandle,
    Arc<RecordedWsConnector>,
) {
    let socket = Arc::new(RecordedWsConnector::default());
    let timestamp = chrono::Utc::now().timestamp_millis();
    let instrument = json!({"instType":"SWAP","instId":"BTC-USDT-SWAP",
        "instFamily":"BTC-USDT","uly":"BTC-USDT","state":"live","ruleType":"normal",
        "settleCcy":"USDT","tickSz":"0.1","lotSz":"0.01","minSz":"0.01",
        "maxLmtSz":"1000","maxMktSz":"1000","ctType":"linear","ctVal":"0.01",
        "ctValCcy":"BTC","groupId":"4","lever":"20"});
    let reference = okx_observation::ReferenceRegistry::from_public(
        chrono::Utc::now().to_rfc3339(),
        vec![serde_json::from_value(instrument).unwrap()],
    )
    .unwrap();
    for (argument, data) in [
        (json!({"channel":"instruments","instType":"SWAP"}), None),
        (json!({"channel":"instruments","instType":"FUTURES"}), None),
        (
            json!({"channel":"tickers","instId":"BTC-USDT-SWAP"}),
            Some(
                json!({"instType":"SWAP","instId":"BTC-USDT-SWAP","last":"1000","lastSz":"1",
                "askPx":"1000","askSz":"10","bidPx":"999.9","bidSz":"10","ts":timestamp.to_string()}),
            ),
        ),
        (
            json!({"channel":"mark-price","instId":"BTC-USDT-SWAP"}),
            Some(
                json!({"instType":"SWAP","instId":"BTC-USDT-SWAP","markPx":"1000","ts":timestamp.to_string()}),
            ),
        ),
        (
            json!({"channel":"index-tickers","instId":"BTC-USDT"}),
            Some(json!({"instId":"BTC-USDT","idxPx":"1000","ts":timestamp.to_string()})),
        ),
        (
            json!({"channel":"funding-rate","instId":"BTC-USDT-SWAP"}),
            Some(
                json!({"instType":"SWAP","instId":"BTC-USDT-SWAP","fundingRate":"0.0001",
                "fundingTime":timestamp.to_string(),"nextFundingTime":(timestamp+28_800_000).to_string(),"ts":timestamp.to_string()}),
            ),
        ),
        (
            json!({"channel":"open-interest","instId":"BTC-USDT-SWAP"}),
            Some(
                json!({"instType":"SWAP","instId":"BTC-USDT-SWAP","oi":"1000","oiCcy":"10","oiUsd":"10000","ts":timestamp.to_string()}),
            ),
        ),
        (
            json!({"channel":"books","instId":"BTC-USDT-SWAP"}),
            Some(
                json!({"asks":[["1000","10","0","1"]],"bids":[["999.9","10","0","1"]],
                "seqId":10,"prevSeqId":-1,"ts":timestamp.to_string()}),
            ),
        ),
    ] {
        let ack = json!({"event":"subscribe","arg":argument,"connId":"native-recorded-market"})
            .to_string();
        let mut frames = vec![];
        if let Some(data) = data {
            let mut frame = json!({"arg":argument,"data":[data]});
            if argument["channel"] == "books" {
                frame["action"] = "snapshot".into();
            }
            frames.push(frame.to_string());
        }
        if argument["channel"] == "books" {
            frames.push(ack);
        } else {
            frames.insert(0, ack);
        }
        socket.expect_public_subscription(argument, frames);
    }
    let (coordinator, handle) = okx_runtime::PublicWsCoordinator::new_with_connector(
        OkxEnvironment::new(okx_api::Region::Global, false),
        reference,
        RateBudget::new(),
        socket.clone(),
    );
    (coordinator, handle, socket)
}

pub(super) struct NativeFixture {
    pub(super) root: PathBuf,
    pub(super) registry: Arc<AccountRuntimeRegistry>,
    pub(super) owner: UnifiedFinanceHandle,
    pub(super) _account: Arc<AccountResources>,
    pub(super) config: VerifiedAccountConfig,
    pub(super) feed: Arc<RecordedAccountHttp>,
    pub(super) market_boundary: Arc<ExecuteMarketBoundary>,
    pub(super) _market: okx_runtime::PublicWsCoordinator,
    pub(super) trade: Arc<TradeService>,
    pub(super) authority: CentralGovernanceSnapshot,
    pub(super) campaign: String,
}

pub(super) async fn fixture() -> NativeFixture {
    fixture_for_accounts(&["a"]).await
}

/// Connect the entire desired roster before strict policy application. This
/// exercises the same source/bootstrap path for every account, not SQL seeds.
pub(super) async fn fixture_for_accounts(labels: &[&str]) -> NativeFixture {
    assert_eq!(labels.first(), Some(&"a"));
    assert_eq!(
        labels
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        labels.len()
    );
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).unwrap();
    let root = if std::env::var("OKX_NATIVE_CRASH_CHILD").as_deref() == Ok("POST_CREATE_PRE_RESULT")
    {
        let root = PathBuf::from(std::env::var_os("OKX_NATIVE_CRASH_ROOT").unwrap());
        assert!(root.is_absolute());
        assert_eq!(
            root.parent().unwrap().canonicalize().unwrap(),
            std::env::temp_dir().canonicalize().unwrap(),
            "crash fixture must be a direct child of the OS temp directory"
        );
        assert!(
            root.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("okx-native-os-crash-")
        );
        assert!(
            matches!(std::fs::symlink_metadata(&root), Err(error)
            if error.kind() == std::io::ErrorKind::NotFound),
            "crash fixture must create a NEW installation"
        );
        root
    } else {
        std::env::temp_dir().join(format!(
            "okx-native-trade-{:032x}",
            u128::from_le_bytes(nonce)
        ))
    };
    let installation = "installation-native-trade";
    let campaign = format!("sha256:{}", "c".repeat(64));
    initialize_installation(&root, installation).unwrap();
    let initial_owner = UnifiedFinanceHandle::initialize_new(
        root.clone(),
        okx_execution::NewFinanceInstallation {
            installation_id: installation.into(),
            initialize_request_id: "initialize_native_trade_001".into(),
            source_tree: "a".repeat(40),
        },
    )
    .await
    .unwrap();
    let mut initial_closed = initial_owner.termination_signal();
    drop(initial_owner);
    tokio::time::timeout(Duration::from_secs(3), initial_closed.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();

    let budget = RateBudget::new();
    let (registry, owner) =
        AccountRuntimeRegistry::open_native_catalog(&root, budget.clone(), &campaign)
            .await
            .unwrap();
    let registry = Arc::new(registry);
    let feed = Arc::new(RecordedAccountHttp::default());
    let market_boundary = Arc::new(ExecuteMarketBoundary {
        feed: feed.clone(),
        market: Mutex::new(None),
        updates: AtomicUsize::new(0),
        planning_fee_gate: Mutex::new(None),
        planning_fee_gate_actor: Mutex::new(None),
        planning_fee_skip: AtomicUsize::new(0),
        market_updates_expected: AtomicUsize::new(2),
        market_delivery: tokio::sync::Mutex::new(()),
        foreign_create_ack: std::sync::atomic::AtomicBool::new(false),
    });
    let prepared = labels
        .iter()
        .map(|label| {
            prepared_with_socket(
                label,
                market_boundary.clone(),
                Arc::new(RecordedWsConnector::default()),
                budget.clone(),
            )
        })
        .collect::<Vec<_>>();
    let config = prepared[0].0.clone();
    let environment = OkxEnvironment::new(okx_api::Region::Global, false);
    let (market, public_ws) = okx_runtime::PublicWsCoordinator::new_with_connector(
        environment,
        okx_observation::ReferenceRegistry::from_public("native-admission", vec![]).unwrap(),
        budget,
        Arc::new(recorded_support::OfflineSocket),
    );
    assert!(matches!(
        NativeFinanceRisk::from_owner(owner.clone())
            .resolved()
            .await,
        Err("FINANCE_CAMPAIGN_POLICY_REQUIRED")
    ));
    let trade = Arc::new(TradeService::new(
        registry.clone(),
        public_ws,
        NativeRiskClient::local(NativeFinanceRisk::from_owner(owner.clone())),
    ));
    let mut authority: CentralGovernanceSnapshot = serde_json::from_value(serde_json::json!({
        "state":{"schema":"okx.governance/v3","revision":2,
            "installation_id":installation,"campaign_id":campaign,
            "campaign_loss_limit_usd":"10","campaign_entry_limit_usd":"10",
            "max_total_modelled_loss_usdt":"10",
            "accounts":[{"id":"account_a","label":"Recorded A","venue":"okx",
                "environment":"real","uid_fingerprint":config.account_key.verified_uid,
                "identity_verified":true,"enabled":true,"explicit_pause":false,
                "policy_revision":2,"account_key":config.account_key.wire(),"strategies":[],
                "limits":{"max_modelled_loss_usdt":"10","max_order_loss_usdt":"5","max_entry_usd":"10",
                    "max_notional_usdt":"1000","max_leverage":"5",
                    "max_daily_realized_loss_usdt":"10","max_drawdown_ratio":"0.5",
                    "allowed_instruments":["BTC-USDT-SWAP"]}}]},
        "issued_at_ms":chrono::Utc::now().timestamp_millis(),"session_generation":1
    }))
    .unwrap();
    let account_policy = authority.state.accounts[0].clone();
    authority.state.accounts = prepared
        .iter()
        .zip(labels)
        .map(|((config, _), label)| {
            let mut policy = account_policy.clone();
            policy.id = format!("account_{label}");
            policy.label = format!("Recorded {label}");
            policy.account_key = config.account_key.wire();
            policy.uid_fingerprint = config.account_key.verified_uid.clone();
            policy
        })
        .collect();
    assert!(authority.valid());
    // Same callback used by authenticated PolicySnapshot delivery: first an
    // empty initial configuration, then the desired roster BEFORE connection.
    // Neither observation may create financial authority or tear down transport.
    let mut initial = authority.clone();
    initial.state.revision = 1;
    initial.state.accounts.clear();
    assert!(initial.valid());
    let mut wrong_installation = authority.clone();
    wrong_installation.state.installation_id = "foreign-installation".into();
    assert!(trade.update_governance(&wrong_installation).await.is_err());
    let mut demo = authority.clone();
    demo.state.accounts[0].environment = okx_protocol::account_governance::AccountEnvironment::Demo;
    demo.state.accounts[0].account_key = demo.state.accounts[0]
        .account_key
        .replace(":real:", ":demo:");
    assert!(demo.valid());
    assert!(trade.update_governance(&demo).await.is_err());
    trade.update_governance(&initial).await.unwrap();
    trade.update_governance(&authority).await.unwrap();
    assert!(
        owner
            .connected_account_keys(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        owner
            .configured_campaign(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        NativeFinanceRisk::from_owner(owner.clone())
            .apply_governance(&authority)
            .await
            .is_err(),
        "observed policy is not the strict financial application"
    );
    assert!(
        owner
            .verify_live_authority(Instant::now() + Duration::from_secs(2))
            .await
            .is_err()
    );
    assert!(feed.outbound_posts.lock().unwrap().is_empty());
    for (connected_config, clients) in prepared {
        registry
            .connect_prepared(connected_config.clone(), clients)
            .await
            .unwrap();
        let account = registry.resolve(&connected_config.alias).await.unwrap();
        // Only production login, identity validation and capture publish readiness.
        // No fixture mutates verified_source_generation, SQL cursors or NAV.
        tokio::time::timeout(Duration::from_secs(5), async {
            while !account.source_identity_ready().await {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Native private source identity must be independently verified");
        crate::source_capture::capture_account_sources_measured(
            &account.observer,
            &account.private_ws,
            &account.execution,
        )
        .await
        .expect("original Native source capture must reach its original SQL owner");
        let frontier = account
            .execution
            .store
            .source_capture_state(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        assert_eq!(frontier.account_key, connected_config.account_key.wire());
        for cursor in [
            frontier.cursors.spot,
            frontier.cursors.margin,
            frontier.cursors.swap,
            frontier.cursors.futures,
            frontier.cursors.options,
            frontier.cursors.bills,
        ] {
            let cursor = cursor.expect("all six original monetary history frontiers persisted");
            assert!(cursor.generation > 0);
            assert!(cursor.coverage_gaps.is_empty());
        }
    }
    let account = registry.resolve(&config.alias).await.unwrap();

    trade.update_governance(&authority).await.unwrap();
    assert_eq!(
        owner
            .configured_campaign(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap(),
        Some(campaign.clone())
    );
    let bound = NativeFinanceRisk::from_owner(owner.clone())
        .resolved()
        .await
        .unwrap();
    assert_eq!(bound.campaign, campaign);
    let mut foreign = authority.clone();
    foreign.state.campaign_id = format!("sha256:{}", "d".repeat(64));
    assert!(
        NativeFinanceRisk::from_owner(owner.clone())
            .apply_governance(&foreign)
            .await
            .is_err()
    );
    NativeFixture {
        root,
        registry,
        owner,
        _account: account,
        config,
        feed,
        market_boundary,
        _market: market,
        trade,
        authority,
        campaign,
    }
}

pub(super) fn execute() -> TradeRequest {
    serde_json::from_value(serde_json::json!({
        "action":"execute","idempotency_key":"native_opening_required_001",
        "operation":"open","strategy_instance":"manual","instrument":"BTC-USDT-SWAP",
        "side":"long","sizing":{"contracts":"0.10"},
        "order":{"type":"limit","limit_price":"1000","max_entry_usd":"10",
            "max_slippage_bps":0},"protection":{"stop_price":"900"},"expires_in_ms":1000
    }))
    .unwrap()
}

impl NativeFixture {
    pub(super) async fn finish(self) {
        assert!(
            !self.root.join("accounts").exists(),
            "one original financial SQLite only"
        );
        self.registry.shutdown().await;
        let root = self.root.clone();
        let mut closed = self.owner.termination_signal();
        drop(self);
        tokio::time::timeout(Duration::from_secs(3), closed.wait_for(|v| *v))
            .await
            .unwrap()
            .unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

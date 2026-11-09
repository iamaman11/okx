//! Public routing tests: real owners, with delivery replaced only at HTTP/WS.
use super::*;
use crate::accounts::{initialize_installation, recorded_support};
use okx_api::{HttpSend, HttpTransport, OkxEnvironment, RateBudget};
use okx_protocol::{AGENT_REQUEST_SCHEMA_V1, InstrumentTypeFilter};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};

#[derive(Default)]
struct ForbiddenNetwork(AtomicUsize);
impl HttpTransport for ForbiddenNetwork {
    fn send(&self, _: okx_api::HttpRequest, _: Instant) -> HttpSend<'_> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Box::pin(async {
            Err(okx_api::OkxError::Response(
                "PUBLIC_TEST_NETWORK_FORBIDDEN".into(),
            ))
        })
    }
}

struct Fixture {
    root: PathBuf,
    context: DispatchContext,
    // Keep the actual command receiver alive without starting a network task.
    _coordinator: okx_runtime::PublicWsCoordinator,
    network: Arc<ForbiddenNetwork>,
    risk: tokio::sync::mpsc::Receiver<crate::risk_bridge::OutboundRiskCommand>,
}
impl Fixture {
    async fn new(reference: okx_observation::ReferenceRegistry) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "okx-query-dispatch-{}-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        initialize_installation(&root, "query-dispatch-installation").unwrap();
        let accounts = Arc::new(
            AccountRuntimeRegistry::open(&root, RateBudget::new())
                .await
                .unwrap(),
        );
        let environment = OkxEnvironment::new(okx_api::Region::Global, false);
        let network = Arc::new(ForbiddenNetwork::default());
        let market = crate::market_bootstrap::MarketBootstrapper::new(
            okx_api::OkxPublicClient::with_http_transport(
                environment,
                RateBudget::new(),
                network.clone(),
            )
            .unwrap(),
        );
        let analytics = AnalyticsService::new(root.join("research"), market.clone()).unwrap();
        let (coordinator, public_ws) = okx_runtime::PublicWsCoordinator::new_with_connector(
            environment,
            reference,
            RateBudget::new(),
            Arc::new(recorded_support::OfflineSocket),
        );
        let (tx, risk) = tokio::sync::mpsc::channel(4);
        let trade = Arc::new(TradeService::new(
            accounts.clone(),
            public_ws.clone(),
            crate::risk_bridge::NativeRiskClient::new(tx),
        ));
        Self {
            root,
            context: DispatchContext {
                accounts,
                analytics,
                public_ws,
                trade,
                market,
            },
            _coordinator: coordinator,
            network,
            risk,
        }
    }
    async fn request(&self, operation: AgentOperation, account: Option<&str>) -> AgentResponse {
        dispatch(
            &self.context,
            &AgentRequest {
                schema: AGENT_REQUEST_SCHEMA_V1.into(),
                request_id: "query-dispatch-request-001".into(),
                account: account.map(str::to_owned),
                operation,
            },
        )
        .await
    }
    async fn finish(mut self) {
        assert_eq!(
            self.network.0.load(Ordering::Relaxed),
            0,
            "read-only fixture must not fetch external data"
        );
        assert!(
            self.risk.try_recv().is_err(),
            "read-only query must not request a risk mutation"
        );
        self.context.accounts.shutdown().await;
        let root = self.root.clone();
        drop(self);
        std::fs::remove_dir_all(root).unwrap();
    }
}
fn empty_reference() -> okx_observation::ReferenceRegistry {
    okx_observation::ReferenceRegistry::from_public("fixture", vec![]).unwrap()
}
fn rejection(response: AgentResponse, code: &str) {
    assert_eq!(response.status, AgentResponseStatus::Rejected);
    assert_eq!(response.quality, DataQuality::NotReady);
    assert!(response.result.is_none());
    assert_eq!(response.failure.unwrap().code, code);
}

#[tokio::test]
async fn capabilities_and_mailbox_use_public_dispatch_without_external_effects() {
    let fixture = Fixture::new(empty_reference()).await;
    let capabilities = fixture
        .request(AgentOperation::MarketCapabilities, None)
        .await;
    assert_eq!(capabilities.request_id, "query-dispatch-request-001");
    assert_eq!(capabilities.status, AgentResponseStatus::Completed);
    assert_eq!(capabilities.quality, DataQuality::Fresh);
    let data = capabilities.result.unwrap();
    assert_eq!(data["schema"], "okx.market.capabilities/v1");
    assert_eq!(data["limits"]["max_page_rows"], 50);
    assert!(
        data["operators"]
            .as_array()
            .unwrap()
            .iter()
            .any(|op| op["name"] == "returns" && op["unit"] == "ratio")
    );
    let mailbox = fixture
        .request(AgentOperation::MailboxTelemetry, None)
        .await;
    assert_eq!(mailbox.status, AgentResponseStatus::Completed);
    assert_eq!(
        mailbox.result.unwrap(),
        json!({"contract":"okx.product/v1","installation_id":"query-dispatch-installation"})
    );
    fixture.finish().await;
}

#[tokio::test]
async fn instrument_search_routes_type_filter_before_public_limit() {
    let instrument = |id: String, kind: &str| {
        serde_json::from_value(json!({
            "instId":id,"instType":kind,"state":"live","baseCcy":"BTC","ctValCcy":"BTC",
            "settleCcy":"USDT","tickSz":"0.1","lotSz":"1","minSz":"1"
        }))
        .unwrap()
    };
    let mut instruments = (0..60)
        .map(|i| instrument(format!("A-SWAP-{i:02}"), "SWAP"))
        .collect::<Vec<_>>();
    instruments.extend((0..55).map(|i| instrument(format!("Z-FUTURE-{i:02}"), "FUTURES")));
    let fixture = Fixture::new(
        okx_observation::ReferenceRegistry::from_public("fixture", instruments).unwrap(),
    )
    .await;
    let response = fixture
        .request(
            AgentOperation::FindInstruments {
                asset: "BTC".into(),
                settle_currency: Some("USDT".into()),
                instrument_type: Some(InstrumentTypeFilter::Futures),
            },
            None,
        )
        .await;
    assert_eq!(response.status, AgentResponseStatus::Completed);
    let data = response.result.unwrap();
    let rows = data["instruments"].as_array().unwrap();
    assert_eq!(rows.len(), 50);
    assert!(rows.iter().all(|row| {
        row["instrument_id"]
            .as_str()
            .unwrap()
            .starts_with("Z-FUTURE-")
    }));
    rejection(
        fixture
            .request(
                AgentOperation::FindInstruments {
                    asset: "".into(),
                    settle_currency: None,
                    instrument_type: None,
                },
                None,
            )
            .await,
        "INVALID_COMMAND",
    );
    fixture.finish().await;
}

#[tokio::test]
async fn account_summary_checks_account_and_source_at_public_boundary() {
    let mut fixture = Fixture::new(empty_reference()).await;
    rejection(
        fixture.request(AgentOperation::AccountSummary, None).await,
        "EXPLICIT_ACCOUNT_REQUIRED",
    );
    rejection(
        fixture
            .request(AgentOperation::AccountSummary, Some("unknown-account"))
            .await,
        "ACCOUNT_UNAVAILABLE",
    );
    let feed = Arc::new(recorded_support::RecordedAccountHttp::default());
    let (config, clients) = recorded_support::prepared("a", feed, RateBudget::new());
    fixture
        .context
        .accounts
        .connect_prepared(config.clone(), clients)
        .await
        .unwrap();
    // Freeze the source producer before varying its published input. This tests
    // the public consumer, not the exchange observer's separate freshness proof.
    fixture.context.accounts.shutdown().await;
    let owner = fixture
        .context
        .accounts
        .resolve(&config.account_key.wire())
        .await
        .unwrap();
    let mut terminated = owner.execution.store.termination_signal();
    let original = owner.observer.snapshot().await.unwrap();
    *owner.latest_snapshot.write().await = None;
    rejection(
        fixture
            .request(AgentOperation::AccountSummary, Some(&config.alias))
            .await,
        "WAITING_ACCOUNT_EVIDENCE",
    );
    let mut snapshot = original.clone();
    snapshot.source_received_at = "2020-01-01T00:00:00Z".into();
    *owner.latest_snapshot.write().await = Some(snapshot.clone());
    rejection(
        fixture
            .request(AgentOperation::AccountSummary, Some(&config.alias))
            .await,
        "WAITING_ACCOUNT_EVIDENCE",
    );
    snapshot.source_received_at = "invalid-time".into();
    *owner.latest_snapshot.write().await = Some(snapshot.clone());
    rejection(
        fixture
            .request(AgentOperation::AccountSummary, Some(&config.alias))
            .await,
        "ACCOUNT_SOURCE_TIME_INVALID",
    );
    snapshot.source_received_at = Utc::now().to_rfc3339();
    *owner.latest_snapshot.write().await = Some(snapshot.clone());
    let response = fixture
        .request(AgentOperation::AccountSummary, Some(&config.alias))
        .await;
    assert_eq!(response.status, AgentResponseStatus::Completed);
    let result = response.result.unwrap();
    assert_eq!(result["account"], config.account_key.wire());
    assert_eq!(result["source_received_at"], snapshot.source_received_at);
    assert_eq!(
        result["total_equity_usd"],
        json!(original.balance.total_equity_usd)
    );
    assert_eq!(fixture.network.0.load(Ordering::Relaxed), 0);
    assert!(fixture.risk.try_recv().is_err());
    drop(owner);
    fixture.context.accounts.shutdown().await;
    let root = fixture.root.clone();
    drop(fixture);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !*terminated.borrow_and_update() {
            terminated.changed().await.unwrap();
        }
    })
    .await
    .expect("actual SQL owner must terminate before deleting its files");
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn overview_without_reference_rejects_unavailable_source_instead_of_empty_success() {
    let fixture = Fixture::new(empty_reference()).await;
    rejection(
        fixture
            .request(
                AgentOperation::MarketOverview {
                    instrument: "BTC-USDT-SWAP".into(),
                },
                None,
            )
            .await,
        "MARKET_SOURCE_UNAVAILABLE",
    );
    rejection(
        fixture
            .request(
                AgentOperation::MarketMicrostructure {
                    instrument: "BTC-USDT-SWAP".into(),
                    size_contracts: None,
                },
                None,
            )
            .await,
        "MICROSTRUCTURE_FRESH_BOOK_REQUIRED",
    );
    fixture.finish().await;
}

fn two_position_scenario() -> okx_protocol::trading::PortfolioScenarioRequest {
    use okx_protocol::trading::{
        PortfolioScenarioRequest, PositionSide, ScenarioCorrelation, ScenarioPosition,
    };
    PortfolioScenarioRequest {
        equity_usd: "100".into(),
        positions: vec![
            ScenarioPosition {
                instrument: "BTC-USDT-SWAP".into(),
                side: PositionSide::Long,
                notional_usd: "100".into(),
                margin_usd: "10".into(),
                price_shock_bps: -1000,
                volatility_bps: 300,
                liquidity_cost_bps: 100,
            },
            ScenarioPosition {
                instrument: "ETH-USDT-SWAP".into(),
                side: PositionSide::Short,
                notional_usd: "200".into(),
                margin_usd: "20".into(),
                price_shock_bps: 500,
                volatility_bps: 200,
                liquidity_cost_bps: 50,
            },
        ],
        correlations: vec![ScenarioCorrelation {
            left: 0,
            right: 1,
            rho: 0.0,
        }],
    }
}

// Compare existing files and their bytes as well as absence of new artifacts.
fn fixture_artifacts(root: &std::path::Path) -> Vec<(PathBuf, Vec<u8>)> {
    fn collect(root: &std::path::Path, dir: &std::path::Path, rows: &mut Vec<(PathBuf, Vec<u8>)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                collect(root, &path, rows);
            } else {
                rows.push((
                    path.strip_prefix(root).unwrap().to_owned(),
                    std::fs::read(&path).unwrap(),
                ));
            }
        }
    }
    let mut rows = Vec::new();
    collect(root, root, &mut rows);
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

#[tokio::test]
async fn portfolio_scenario_public_dispatch_returns_independent_virtual_risk_values() {
    let fixture = Fixture::new(empty_reference()).await;
    let before = fixture_artifacts(&fixture.root);
    let response = fixture
        .request(
            AgentOperation::PortfolioScenario {
                request: two_position_scenario(),
            },
            None,
        )
        .await;
    assert_eq!(response.request_id, "query-dispatch-request-001");
    assert_eq!(response.status, AgentResponseStatus::Completed);
    assert_eq!(response.quality, DataQuality::Fresh);
    assert!(response.failure.is_none());
    let data = response.result.unwrap();
    assert_eq!(data["schema"], "okx.portfolio.scenario/v1");
    assert_eq!(data["quality"], "assumed");
    assert_eq!(data["execution"], "virtual_read_only");
    assert_eq!(data["provenance"], "caller_supplied");
    assert_eq!(data["exchange_post"], false);
    assert_eq!(data["venue_margin_verified"], false);
    assert_eq!(data["gross_notional_usd"], "300");
    assert_eq!(data["margin_usd"], "30");
    assert_eq!(data["gross_leverage_ratio"], "3");
    assert_eq!(data["stress_price_pnl_usd"], "-20");
    assert_eq!(data["liquidity_loss_usd"], "2");
    assert_eq!(data["net_stress_pnl_usd"], "-22");
    assert_eq!(data["stressed_equity_usd"], "78");
    assert_eq!(data["drawdown_ratio"], "0.22");
    // Independent 3-4-5 case: uncorrelated dollar volatilities 3 and 4
    // have variance 25 and standard deviation 5. No VaR is exposed by this DTO.
    assert_eq!(data["correlated_volatility_usd"], 5.0);
    assert_eq!(data["correlation_basis"], "caller_supplied_psd");
    assert_eq!(
        data["positions"],
        json!([
            {"instrument":"BTC-USDT-SWAP","side":"long","price_pnl_usd":"-10","liquidity_loss_usd":"1","source":"caller_supplied"},
            {"instrument":"ETH-USDT-SWAP","side":"short","price_pnl_usd":"-10","liquidity_loss_usd":"1","source":"caller_supplied"}
        ])
    );
    assert_eq!(fixture_artifacts(&fixture.root), before);
    fixture.finish().await;
}

#[tokio::test]
async fn portfolio_scenario_public_dispatch_rejects_non_psd_and_invalid_parameters() {
    use okx_protocol::trading::ScenarioCorrelation;
    let fixture = Fixture::new(empty_reference()).await;
    let before = fixture_artifacts(&fixture.root);
    let mut non_psd = two_position_scenario();
    non_psd.positions.push(non_psd.positions[0].clone());
    // Pairwise-valid correlations cannot all coexist: the first two perfect
    // positive relationships require the third relationship to be positive.
    non_psd.correlations = vec![
        ScenarioCorrelation {
            left: 0,
            right: 1,
            rho: 1.0,
        },
        ScenarioCorrelation {
            left: 0,
            right: 2,
            rho: 1.0,
        },
        ScenarioCorrelation {
            left: 1,
            right: 2,
            rho: -1.0,
        },
    ];
    assert!(non_psd.valid());
    rejection(
        fixture
            .request(AgentOperation::PortfolioScenario { request: non_psd }, None)
            .await,
        "CORRELATION_NOT_PSD",
    );
    let mut invalid = two_position_scenario();
    invalid.positions[0].price_shock_bps = -10001;
    rejection(
        fixture
            .request(AgentOperation::PortfolioScenario { request: invalid }, None)
            .await,
        "INVALID_COMMAND",
    );
    let mut invalid = two_position_scenario();
    invalid.correlations[0].rho = 1.01;
    rejection(
        fixture
            .request(AgentOperation::PortfolioScenario { request: invalid }, None)
            .await,
        "INVALID_COMMAND",
    );
    assert_eq!(fixture_artifacts(&fixture.root), before);
    fixture.finish().await;
}

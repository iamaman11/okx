//! Public Strategies routing against the real registry and SQL owner. Recorded
//! HTTP/WS ports provide delivery only; no strategy or money owner is replaced.
use super::*;
use crate::accounts::{native_trade_contracts::fixture as native, recorded_support};
use okx_protocol::{AgentOperation, AgentRequest, AgentResponse, AgentResponseStatus, DataQuality};

const INSTANCE: &str = "ema_contract";

struct Fixture {
    native: native::NativeFixture,
    ctx: crate::query::DispatchContext,
    feed: Arc<recorded_support::RecordedAccountHttp>,
    _coordinator: okx_runtime::PublicWsCoordinator,
    owner: Arc<AccountResources>,
}

impl Fixture {
    async fn new() -> Self {
        let native = native::fixture().await;
        let owner = native._account.clone();
        let feed = native.feed.clone();
        let environment = okx_api::OkxEnvironment::new(okx_api::Region::Global, false);
        let budget = okx_api::RateBudget::new();
        let (coordinator, public_ws) = okx_runtime::PublicWsCoordinator::new_with_connector(
            environment,
            okx_observation::ReferenceRegistry::from_public("fixture", vec![]).unwrap(),
            budget.clone(),
            Arc::new(recorded_support::OfflineSocket),
        );
        let market = crate::market_bootstrap::MarketBootstrapper::new(
            okx_api::OkxPublicClient::with_http_transport(environment, budget, feed.clone())
                .unwrap(),
        );
        let ctx = crate::query::DispatchContext {
            research: crate::research_test_support::client(
                native.root.join("research"),
                market.clone(),
            )
            .unwrap(),
            public_ws,
            finance: crate::finance_dispatch::FinanceDispatch::ready(
                native.registry.clone(),
                native.trade.clone(),
            ),
            market,
        };
        Self {
            native,
            ctx,
            feed,
            _coordinator: coordinator,
            owner,
        }
    }

    async fn authority(&self, paused: bool) {
        let mut authority = self.native.authority.clone();
        authority.state.revision += 1;
        authority.state.accounts[0].policy_revision = authority.state.revision;
        authority.state.accounts[0].explicit_pause = paused;
        authority.issued_at_ms = jiff::Timestamp::now().as_millisecond() as _;
        self.ctx
            .finance
            .trade()
            .unwrap()
            .update_governance(&authority)
            .await
            .unwrap();
    }

    async fn operation(&self, operation: AgentOperation, account: Option<&str>) -> AgentResponse {
        crate::query::dispatch(
            &self.ctx,
            &AgentRequest {
                schema: okx_protocol::AGENT_REQUEST_SCHEMA_V1.into(),
                request_id: "strategy-contract-request".into(),
                account: account.map(str::to_owned),
                operation,
            },
        )
        .await
    }

    async fn request(&self, request: StrategyRequest) -> AgentResponse {
        self.operation(
            AgentOperation::Strategies { request },
            Some(&self.owner.configuration.alias),
        )
        .await
    }

    async fn finish(self) {
        let Self {
            native,
            ctx,
            owner,
            feed,
            _coordinator,
        } = self;
        drop(ctx);
        drop(owner);
        drop(feed);
        drop(_coordinator);
        native.finish().await;
    }
}

fn create(key: &str) -> StrategyRequest {
    StrategyRequest::Create {
        idempotency_key: key.into(),
        instance_id: INSTANCE.into(),
        definition_id: "ema_cross_v1".into(),
        version: 1,
        parameters: json!({"fast":10,"slow":20}),
        instruments: vec!["BTC-USDT-SWAP".into()],
        timeframe_ms: 60_000,
        budget_usd: "2".into(),
    }
}
fn update(key: &str, revision: u64) -> StrategyRequest {
    StrategyRequest::Update {
        idempotency_key: key.into(),
        instance_id: INSTANCE.into(),
        expected_revision: revision,
        parameters: json!({"fast":5,"slow":30}),
        budget_usd: "3".into(),
    }
}

fn graph_signal_parameters() -> Value {
    json!({
        "version":"okx.graph-signal/v1",
        "graph":{"schema":"okx.graph/v1","steps":[
            {"id":"return","op":{"id":"returns_series","version":1},"inputs":[{"source":"close"}]},
            {"id":"positive","op":{"id":"greater_than","version":1},
             "inputs":[{"step":"return"}],"params":{"threshold":0.0}}
        ],"outputs":["positive"]},
        "signal_step":"positive","target_if_true":1,"target_if_false":0,"missing":"hold"
    })
}

#[tokio::test]
async fn graph_strategy_is_durable_and_does_not_invoke_legacy_ema() {
    let fixture = Fixture::new().await;
    let _capture = fixture.owner.recorded_capture_gate.write().await;
    let before = money(&fixture).await;
    let posts = fixture.feed.outbound_posts.lock().unwrap().clone();
    let instance_id = "graph_contract";
    let graph = graph_signal_parameters();
    let create = StrategyRequest::Create {
        idempotency_key: "graph_create".into(),
        instance_id: instance_id.into(),
        definition_id: "graph_signal_v1".into(),
        version: 1,
        parameters: graph.clone(),
        instruments: vec!["BTC-USDT-SWAP".into()],
        timeframe_ms: 60_000,
        budget_usd: "2".into(),
    };
    let created = completed_data(fixture.request(create.clone()).await)["instance"].clone();
    assert_eq!(created["enabled"], false);
    assert_eq!(created["definition_id"], "graph_signal_v1");
    assert_eq!(
        completed_data(fixture.request(create).await)["instance"],
        created
    );
    let update = StrategyRequest::Update {
        idempotency_key: "graph_update".into(),
        instance_id: instance_id.into(),
        expected_revision: 1,
        parameters: graph.clone(),
        budget_usd: "2".into(),
    };
    assert_eq!(
        completed_data(fixture.request(update).await)["instance"]["policy_revision"],
        2
    );
    rejected(
        fixture
            .request(StrategyRequest::Update {
                idempotency_key: "graph_update_ema_wrong_type".into(),
                instance_id: instance_id.into(),
                expected_revision: 2,
                parameters: json!({"fast": 5, "slow": 10}),
                budget_usd: "2".into(),
            })
            .await,
        "STRATEGY_PARAMETERS_INVALID",
    );
    completed_data(
        fixture
            .request(StrategyRequest::Start {
                idempotency_key: "graph_start".into(),
                instance_id: instance_id.into(),
            })
            .await,
    );
    let status = completed_data(
        fixture
            .request(StrategyRequest::Status {
                instance_id: instance_id.into(),
            })
            .await,
    );
    assert_eq!(status["graph_execution"]["state"], "NOT_ACTIVATED");
    assert_eq!(
        status["graph_execution"]["reason_code"],
        "GRAPH_EXECUTION_NOT_CONNECTED"
    );
    let persisted = fixture
        .owner
        .execution
        .store
        .strategy_instances(deadline())
        .await
        .unwrap()
        .into_iter()
        .find(|instance| instance.instance_id == instance_id)
        .unwrap();
    assert!(persisted.enabled);
    let typed: okx_protocol::trading::GraphStrategyConfigurationV1 =
        serde_json::from_str(&persisted.parameters).unwrap();
    assert_eq!(typed.signal, serde_json::from_value(graph).unwrap());
    assert_eq!(
        fixture
            .native
            .trade
            .strategy_turn(&fixture.owner, &persisted)
            .await
            .unwrap_err(),
        "GRAPH_EXECUTION_NOT_CONNECTED"
    );
    let trading = json!({
        "signal": graph_signal_parameters(),
        "execution": {"mode":"trade","max_slippage_bps":5,"stop_distance_bps":50}
    });
    rejected(
        fixture
            .request(StrategyRequest::Update {
                idempotency_key: "graph_live_update_denied".into(),
                instance_id: instance_id.into(),
                expected_revision: persisted.policy_revision as u64,
                parameters: trading.clone(),
                budget_usd: "2".into(),
            })
            .await,
        "GRAPH_TRADE_REQUIRES_STOP",
    );
    let stopped = completed_data(
        fixture
            .request(StrategyRequest::Stop {
                idempotency_key: "graph_explicit_stop".into(),
                instance_id: instance_id.into(),
            })
            .await,
    );
    let updated = completed_data(
        fixture
            .request(StrategyRequest::Update {
                idempotency_key: "graph_explicit_trading_policy".into(),
                instance_id: instance_id.into(),
                expected_revision: stopped["instance"]["policy_revision"].as_u64().unwrap(),
                parameters: trading,
                budget_usd: "2".into(),
            })
            .await,
    );
    assert!(!updated["instance"]["enabled"].as_bool().unwrap());
    assert_eq!(
        serde_json::from_str::<okx_protocol::trading::GraphStrategyConfigurationV1>(
            updated["instance"]["parameters"].as_str().unwrap()
        )
        .unwrap()
        .execution,
        okx_protocol::trading::GraphExecutionPolicy::Trade {
            max_slippage_bps: 5,
            stop_distance_bps: 50,
        }
    );
    assert_eq!(money(&fixture).await, before);
    assert_eq!(*fixture.feed.outbound_posts.lock().unwrap(), posts);
    drop(_capture);
    fixture.finish().await;
}
fn completed_data(response: AgentResponse) -> Value {
    assert_eq!(response.request_id, "strategy-contract-request");
    assert_eq!(
        response.status,
        AgentResponseStatus::Completed,
        "{:?}",
        response.failure
    );
    assert!(response.failure.is_none());
    response.result.unwrap()["data"].clone()
}

#[tokio::test]
async fn graph_target_preview_reads_the_original_account_owner_without_any_order_post() {
    use okx_protocol::research_ipc::{
        GraphPosition, GraphPublicationCursor, GraphSessionId, GraphSessionReply,
        GraphSignalIntent, GraphSignalPublication,
    };
    let fixture = Fixture::new().await;
    let _capture = fixture.owner.recorded_capture_gate.write().await;
    completed_data(fixture.request(create("graph_preview_create")).await);
    completed_data(
        fixture
            .request(StrategyRequest::Start {
                idempotency_key: "graph_preview_start".into(),
                instance_id: INSTANCE.into(),
            })
            .await,
    );
    let instance = fixture
        .owner
        .execution
        .store
        .strategy_instances(deadline())
        .await
        .unwrap()
        .into_iter()
        .find(|v| v.instance_id == INSTANCE)
        .unwrap();
    let session_id = GraphSessionId::new("engine:graph-preview".into()).unwrap();
    let hash = "a".repeat(64);
    let binding = super::graph_target::GraphTargetBinding {
        account_key: fixture.owner.configuration.account_key.wire(),
        instance_id: INSTANCE.into(),
        definition_version: instance.definition_version.clone(),
        policy_revision: instance.policy_revision,
        session_id: session_id.clone(),
        graph_sha256: hash.clone(),
        instrument_id: "BTC-USDT-SWAP".into(),
        timeframe_ms: 60_000,
    };
    let confirmed = GraphPublicationCursor::from_restored(&GraphSessionReply::Evaluated {
        schema: GraphSessionReply::SCHEMA.into(),
        session_id: session_id.clone(),
        graph_sha256: hash.clone(),
        last_closed_ms: 120_000,
        observed_bars: 2,
        intent: GraphSignalIntent::Hold,
    })
    .unwrap();
    assert_eq!(
        fixture
            .native
            .trade
            .bind_graph_strategy(&fixture.owner.configuration.alias, &binding, &confirmed)
            .await
            .unwrap_err(),
        "GRAPH_TARGET_STRATEGY_MODE_MISMATCH",
        "a legacy EMA instance must never acquire a concurrent graph dispatcher"
    );
    let publication = GraphSignalPublication {
        schema: GraphSessionReply::SCHEMA.into(),
        session_id,
        graph_sha256: hash,
        closed_ms: 180_000,
        sequence: 3,
        intent: GraphSignalIntent::SetTarget(GraphPosition::Long),
    };
    let before = money(&fixture).await;
    let posts = fixture.feed.outbound_posts.lock().unwrap().clone();
    let preview = fixture
        .native
        .trade
        .preview_graph_publication(
            &fixture.owner.configuration.alias,
            &binding,
            &confirmed,
            &publication,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        preview.target,
        okx_execution::position_target::TargetPosition::Long
    );
    assert_eq!(
        preview.plan,
        okx_execution::position_target::TargetPlan::Open(
            okx_execution::position_target::ExposureSide::Long,
        )
    );
    assert_eq!(confirmed.last_sequence(), 2);
    assert_eq!(money(&fixture).await, before);
    assert_eq!(*fixture.feed.outbound_posts.lock().unwrap(), posts);
    let mut foreign = binding;
    foreign.account_key.push_str("-foreign");
    assert_eq!(
        fixture
            .native
            .trade
            .preview_graph_publication(
                &fixture.owner.configuration.alias,
                &foreign,
                &confirmed,
                &publication,
            )
            .await
            .unwrap_err(),
        "GRAPH_TARGET_ACCOUNT_MISMATCH"
    );
    drop(_capture);
    fixture.finish().await;
}

#[tokio::test]
async fn graph_research_journal_consumer_commits_idle_and_blocks_open_without_post() {
    use super::graph_consumer::{EngineGraphConsumer, GraphConsumeOutcome};
    use okx_protocol::research_ipc::{
        GraphClosedBar, GraphPosition, GraphPublicationCursor, GraphSessionId, GraphSessionReply,
        GraphSessionRequest, GraphSignalDefinition, GraphSignalIntent,
    };
    let fixture = Fixture::new().await;
    let _capture = fixture.owner.recorded_capture_gate.write().await;
    completed_data(
        fixture
            .request(StrategyRequest::Create {
                idempotency_key: "graph_consumer_create".into(),
                instance_id: INSTANCE.into(),
                definition_id: "graph_signal_v1".into(),
                version: 1,
                parameters: graph_signal_parameters(),
                instruments: vec!["BTC-USDT-SWAP".into()],
                timeframe_ms: 60_000,
                budget_usd: "2".into(),
            })
            .await,
    );
    completed_data(
        fixture
            .request(StrategyRequest::Start {
                idempotency_key: "graph_consumer_start".into(),
                instance_id: INSTANCE.into(),
            })
            .await,
    );
    let instance = fixture
        .owner
        .execution
        .store
        .strategy_instances(deadline())
        .await
        .unwrap()
        .into_iter()
        .find(|v| v.instance_id == INSTANCE)
        .unwrap();
    let session_id = GraphSessionId::new("engine:finance-journal-test".into()).unwrap();
    let definition: GraphSignalDefinition = serde_json::from_value(json!({
        "version":"okx.graph-signal/v1",
        "graph":{"schema":"okx.graph/v1","steps":[
            {"id":"return","op":{"id":"returns_series","version":1},"inputs":[{"source":"close"}]},
            {"id":"positive","op":{"id":"greater_than","version":1},
             "inputs":[{"step":"return"}],"params":{"threshold":0.0}}
        ],"outputs":["positive"]},
        "signal_step":"positive","target_if_true":1,"target_if_false":0,"missing":"hold"
    }))
    .unwrap();
    let restored = fixture
        .ctx
        .research
        .graph_signal(GraphSessionRequest::Restore {
            session_id: session_id.clone(),
            definition: Box::new(definition),
            capacity: 64,
            history: vec![
                GraphClosedBar {
                    time_ms: 60_000,
                    close: Some(100.),
                },
                GraphClosedBar {
                    time_ms: 120_000,
                    close: Some(110.),
                },
            ],
        })
        .await
        .unwrap();
    let GraphSessionReply::Evaluated { graph_sha256, .. } = &restored else {
        panic!("graph restore must yield an evaluated session");
    };
    let binding = super::graph_target::GraphTargetBinding {
        account_key: fixture.owner.configuration.account_key.wire(),
        instance_id: INSTANCE.into(),
        definition_version: instance.definition_version,
        policy_revision: instance.policy_revision,
        session_id: session_id.clone(),
        graph_sha256: graph_sha256.clone(),
        instrument_id: "BTC-USDT-SWAP".into(),
        timeframe_ms: 60_000,
    };
    let account = fixture.owner.configuration.alias.clone();
    let original = GraphPublicationCursor::from_restored(&restored).unwrap();
    let mut wrong_timeframe = binding.clone();
    wrong_timeframe.timeframe_ms = 300_000;
    assert_eq!(
        fixture
            .native
            .trade
            .bind_graph_strategy(&account, &wrong_timeframe, &original)
            .await
            .unwrap_err(),
        "GRAPH_TARGET_CONFIGURATION_MISMATCH"
    );
    fixture
        .native
        .trade
        .bind_graph_strategy(&account, &binding, &original)
        .await
        .unwrap();
    let mut consumer =
        EngineGraphConsumer::new(&fixture.ctx.research, account.clone(), binding.clone()).unwrap();
    let before = money(&fixture).await;
    let posts = fixture.feed.outbound_posts.lock().unwrap().clone();

    // Research computes on its side; Engine only reads the committed outbox.
    fixture
        .ctx
        .research
        .graph_signal(GraphSessionRequest::Advance {
            session_id: session_id.clone(),
            graph_sha256: graph_sha256.clone(),
            expected_bars: 3,
            bar: GraphClosedBar {
                time_ms: 180_000,
                close: Some(90.),
            },
        })
        .await
        .unwrap();
    assert!(matches!(
        consumer
            .next(&fixture.native.trade, &fixture.ctx.research)
            .await
            .unwrap(),
        GraphConsumeOutcome::Idle {
            accepted: 1,
            confirmed_sequence: 3,
            has_more: false
        }
    ));
    assert_eq!(
        fixture
            .native
            .trade
            .graph_confirmed_cursor(&account, &binding)
            .await
            .unwrap()
            .unwrap()
            .last_sequence(),
        3
    );
    fixture
        .native
        .trade
        .set_signal_research(fixture.ctx.research.clone())
        .await;
    assert_eq!(
        fixture
            .native
            .trade
            .graph_research_turn(&fixture.owner)
            .await
            .unwrap(),
        vec![GraphConsumeOutcome::Idle {
            accepted: 0,
            confirmed_sequence: 3,
            has_more: false
        }],
        "the ordinary Engine maintenance route rediscovers the SQL binding"
    );
    // A newly attached subscriber recovers from finance SQL without needing
    // any old broadcast event, then ignores unrelated Research sessions.
    let mut recovered =
        EngineGraphConsumer::new(&fixture.ctx.research, account.clone(), binding.clone()).unwrap();
    assert!(matches!(
        recovered
            .next(&fixture.native.trade, &fixture.ctx.research)
            .await
            .unwrap(),
        GraphConsumeOutcome::Idle {
            accepted: 0,
            confirmed_sequence: 3,
            has_more: false
        }
    ));
    let other_session = GraphSessionId::new("engine:unrelated-session".into()).unwrap();
    let other_definition: GraphSignalDefinition =
        serde_json::from_value(graph_signal_parameters()).unwrap();
    let other_restored = fixture
        .ctx
        .research
        .graph_signal(GraphSessionRequest::Restore {
            session_id: other_session.clone(),
            definition: Box::new(other_definition),
            capacity: 64,
            history: vec![
                GraphClosedBar {
                    time_ms: 60_000,
                    close: Some(100.),
                },
                GraphClosedBar {
                    time_ms: 120_000,
                    close: Some(110.),
                },
            ],
        })
        .await
        .unwrap();
    let GraphSessionReply::Evaluated {
        graph_sha256: other_hash,
        ..
    } = other_restored
    else {
        panic!("other session must be restored");
    };
    fixture
        .ctx
        .research
        .graph_signal(GraphSessionRequest::Advance {
            session_id: other_session,
            graph_sha256: other_hash,
            expected_bars: 3,
            bar: GraphClosedBar {
                time_ms: 180_000,
                close: Some(90.),
            },
        })
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            recovered.next(&fixture.native.trade, &fixture.ctx.research)
        )
        .await
        .expect("unrelated event must not block waiting for 64 notifications")
        .unwrap(),
        GraphConsumeOutcome::UnrelatedNotifications
    );
    fixture
        .ctx
        .research
        .graph_signal(GraphSessionRequest::Advance {
            session_id,
            graph_sha256: graph_sha256.clone(),
            expected_bars: 4,
            bar: GraphClosedBar {
                time_ms: 240_000,
                close: Some(100.),
            },
        })
        .await
        .unwrap();
    let outcome = recovered
        .next(&fixture.native.trade, &fixture.ctx.research)
        .await
        .unwrap();
    let GraphConsumeOutcome::Blocked { publication, plan } = outcome else {
        panic!("actionable Research publication may not be ACKed");
    };
    assert_eq!(publication.sequence, 4);
    assert_eq!(
        publication.intent,
        GraphSignalIntent::SetTarget(GraphPosition::Long)
    );
    assert_eq!(
        plan,
        okx_execution::position_target::TargetPlan::Open(
            okx_execution::position_target::ExposureSide::Long
        )
    );
    assert_eq!(
        fixture
            .native
            .trade
            .graph_research_turn(&fixture.owner)
            .await
            .unwrap(),
        vec![GraphConsumeOutcome::Blocked {
            publication: publication.clone(),
            plan: plan.clone(),
        }],
        "the maintenance route must neither ACK nor execute an actionable Open"
    );
    assert_eq!(
        fixture
            .native
            .trade
            .graph_confirmed_cursor(&account, &binding)
            .await
            .unwrap()
            .unwrap()
            .last_sequence(),
        3,
        "first actionable event remains unacknowledged after idle replay"
    );
    assert_eq!(money(&fixture).await, before);
    assert_eq!(*fixture.feed.outbound_posts.lock().unwrap(), posts);
    drop(_capture);
    fixture.finish().await;
}

#[tokio::test]
async fn graph_status_reads_finance_binding_without_claiming_live_execution() {
    use okx_protocol::research_ipc::{GraphClosedBar, GraphPublicationCursor, GraphSessionRequest};
    let fixture = Fixture::new().await;
    let _capture = fixture.owner.recorded_capture_gate.write().await;
    let account = fixture.owner.configuration.alias.clone();
    let before = money(&fixture).await;
    let posts = fixture.feed.outbound_posts.lock().unwrap().clone();
    let instance_id = "graph_status_bound";
    completed_data(
        fixture
            .request(StrategyRequest::Create {
                idempotency_key: "graph_status_create".into(),
                instance_id: instance_id.into(),
                definition_id: "graph_signal_v1".into(),
                version: 1,
                parameters: graph_signal_parameters(),
                instruments: vec!["BTC-USDT-SWAP".into()],
                timeframe_ms: 60_000,
                budget_usd: "2".into(),
            })
            .await,
    );
    completed_data(
        fixture
            .request(StrategyRequest::Start {
                idempotency_key: "graph_status_start".into(),
                instance_id: instance_id.into(),
            })
            .await,
    );
    let instance = fixture
        .owner
        .execution
        .store
        .strategy_instances(deadline())
        .await
        .unwrap()
        .into_iter()
        .find(|v| v.instance_id == instance_id)
        .unwrap();
    let configuration: GraphStrategyConfigurationV1 =
        serde_json::from_str(&instance.parameters).unwrap();
    let binding = super::graph_activation::activation_binding(
        &fixture.owner.configuration.account_key.wire(),
        &instance,
        &configuration,
        "BTC-USDT-SWAP",
    )
    .unwrap();
    let restored = fixture
        .ctx
        .research
        .graph_signal(GraphSessionRequest::Restore {
            session_id: binding.session_id.clone(),
            definition: Box::new(configuration.signal),
            capacity: 64,
            history: vec![
                GraphClosedBar {
                    time_ms: 60_000,
                    close: Some(100.0),
                },
                GraphClosedBar {
                    time_ms: 120_000,
                    close: Some(105.0),
                },
            ],
        })
        .await
        .unwrap();
    let original = GraphPublicationCursor::from_restored(&restored).unwrap();
    assert!(original.matches_binding(&binding.session_id, &binding.graph_sha256));
    fixture
        .native
        .trade
        .bind_graph_strategy(&account, &binding, &original)
        .await
        .unwrap();
    let status = completed_data(
        fixture
            .request(StrategyRequest::Status {
                instance_id: instance_id.into(),
            })
            .await,
    );
    assert_eq!(
        status["graph_execution"]["state"],
        "FINANCE_BOUND_EXECUTION_DISABLED"
    );
    assert_eq!(status["graph_execution"]["bound_instruments"], 1);
    assert_eq!(status["graph_execution"]["configured_instruments"], 1);
    assert_eq!(money(&fixture).await, before);
    assert_eq!(*fixture.feed.outbound_posts.lock().unwrap(), posts);
    drop(_capture);
    fixture.finish().await;
}

#[tokio::test]
async fn research_bootstrap_binds_and_starts_public_feed_without_order_post() {
    use crate::research_client::{ResearchClient, ResearchClientConfig, SourceHandler};
    use okx_protocol::research_ipc::{
        GraphFeedReply, GraphFeedRequest, PublicCandleCapture, PublicSourceReply,
        PublicSourceRequest, ResearchScope,
    };
    let fixture = Fixture::new().await;
    let _capture = fixture.owner.recorded_capture_gate.write().await;
    let before = money(&fixture).await;
    let posts = fixture.feed.outbound_posts.lock().unwrap().clone();
    let instance_id = "graph_bootstrap_contract";
    completed_data(
        fixture
            .request(StrategyRequest::Create {
                idempotency_key: "graph_bootstrap_create".into(),
                instance_id: instance_id.into(),
                definition_id: "graph_signal_v1".into(),
                version: 1,
                parameters: graph_signal_parameters(),
                instruments: vec!["BTC-USDT-SWAP".into()],
                timeframe_ms: 60_000,
                budget_usd: "2".into(),
            })
            .await,
    );
    completed_data(
        fixture
            .request(StrategyRequest::Start {
                idempotency_key: "graph_bootstrap_start".into(),
                instance_id: instance_id.into(),
            })
            .await,
    );
    let source: SourceHandler = Arc::new(|request| {
        Box::pin(async move {
            let PublicSourceRequest::Candles {
                instrument,
                bar,
                after,
                limit,
                queued,
                ..
            } = request
            else {
                return Err("GRAPH_TEST_PUBLIC_CANDLES_ONLY".into());
            };
            if instrument != "BTC-USDT-SWAP" || bar != "1m" || queued || limit != 100 {
                return Err("GRAPH_TEST_SOURCE_SCOPE_INVALID".into());
            }
            let end: u64 = after
                .ok_or("GRAPH_TEST_CURSOR_MISSING")?
                .parse()
                .map_err(|_| "GRAPH_TEST_CURSOR_INVALID")?;
            let rows = (1..=100u64)
                .map(|i| {
                    let time = end - i * 60_000;
                    okx_protocol::research_ipc::HistoryCandle {
                        open_time_ms: time.to_string(),
                        open: "100".into(),
                        high: "100".into(),
                        low: "100".into(),
                        close: "100".into(),
                        volume: "1".into(),
                        volume_currency: "1".into(),
                        volume_quote: None,
                        confirmed: true,
                    }
                })
                .collect();
            Ok(PublicSourceReply::Candles(PublicCandleCapture {
                raw_body: vec![],
                rows,
                acquired_at_ms: jiff::Timestamp::now().as_millisecond().to_string(),
            }))
        })
    });
    let executable = std::env::var_os("OKX_RESEARCH_SERVICE_TEST_EXE")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            let mut executable = std::env::current_exe().unwrap();
            executable.pop();
            if executable.ends_with("deps") {
                executable.pop();
            }
            executable.push(format!(
                "okx-research-service{}",
                std::env::consts::EXE_SUFFIX
            ));
            executable
        });
    let research = ResearchClient::new(
        ResearchClientConfig {
            executable,
            root: fixture.native.root.join("graph-bootstrap-research"),
            scope: ResearchScope::new(format!("graph-bootstrap-test-{}", std::process::id()))
                .unwrap(),
        },
        source,
    );
    fixture
        .native
        .trade
        .set_signal_research(research.clone())
        .await;
    let instance = fixture
        .owner
        .execution
        .store
        .strategy_instances(deadline())
        .await
        .unwrap()
        .into_iter()
        .find(|v| v.instance_id == instance_id)
        .unwrap();
    fixture
        .native
        .trade
        .graph_activation_turn(&fixture.owner, &[instance.clone()])
        .await
        .unwrap();
    let configuration: GraphStrategyConfigurationV1 =
        serde_json::from_str(&instance.parameters).unwrap();
    let binding = super::graph_activation::activation_binding(
        &fixture.owner.configuration.account_key.wire(),
        &instance,
        &configuration,
        "BTC-USDT-SWAP",
    )
    .unwrap();
    let cursor = fixture
        .native
        .trade
        .graph_confirmed_cursor(&fixture.owner.configuration.alias, &binding)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor.last_sequence(), 64);
    let reply = research
        .graph_feed(GraphFeedRequest::Start {
            session_id: binding.session_id,
            graph_sha256: binding.graph_sha256,
            instrument: "BTC-USDT-SWAP".into(),
            timeframe_ms: 60_000,
            poll_every_ms: 5_000,
        })
        .await
        .unwrap();
    assert!(matches!(reply, GraphFeedReply::Started { .. }));
    assert_eq!(money(&fixture).await, before);
    assert_eq!(*fixture.feed.outbound_posts.lock().unwrap(), posts);
    drop(_capture);
    fixture.finish().await;
}

#[tokio::test]
async fn explicit_graph_open_uses_original_paused_risk_gate_without_order_post() {
    use super::graph_consumer::GraphConsumeOutcome;
    use okx_protocol::research_ipc::{
        GraphClosedBar, GraphPublicationCursor, GraphSessionId, GraphSessionReply,
        GraphSessionRequest, GraphSignalDefinition,
    };
    let fixture = Fixture::new().await;
    let _capture = fixture.owner.recorded_capture_gate.write().await;
    let parameters = json!({
        "signal": graph_signal_parameters(),
        "execution": {"mode":"trade","max_slippage_bps":5,"stop_distance_bps":50}
    });
    completed_data(
        fixture
            .request(StrategyRequest::Create {
                idempotency_key: "graph_armed_create".into(),
                instance_id: "graph_armed".into(),
                definition_id: "graph_signal_v1".into(),
                version: 1,
                parameters,
                instruments: vec!["BTC-USDT-SWAP".into()],
                timeframe_ms: 60_000,
                budget_usd: "2".into(),
            })
            .await,
    );
    completed_data(
        fixture
            .request(StrategyRequest::Start {
                idempotency_key: "graph_armed_start".into(),
                instance_id: "graph_armed".into(),
            })
            .await,
    );
    let instance = fixture
        .owner
        .execution
        .store
        .strategy_instances(deadline())
        .await
        .unwrap()
        .into_iter()
        .find(|v| v.instance_id == "graph_armed")
        .unwrap();
    let definition: GraphSignalDefinition =
        serde_json::from_value(graph_signal_parameters()).unwrap();
    let session = GraphSessionId::new("engine:armed-smoke".into()).unwrap();
    let restored = fixture
        .ctx
        .research
        .graph_signal(GraphSessionRequest::Restore {
            session_id: session.clone(),
            definition: Box::new(definition),
            capacity: 64,
            history: vec![
                GraphClosedBar {
                    time_ms: 60_000,
                    close: Some(100.),
                },
                GraphClosedBar {
                    time_ms: 120_000,
                    close: Some(90.),
                },
            ],
        })
        .await
        .unwrap();
    let GraphSessionReply::Evaluated { graph_sha256, .. } = &restored else {
        panic!("Research must return an evaluated graph session");
    };
    let cursor = GraphPublicationCursor::from_restored(&restored).unwrap();
    let binding = super::graph_target::GraphTargetBinding {
        account_key: fixture.owner.configuration.account_key.wire(),
        instance_id: instance.instance_id,
        definition_version: instance.definition_version,
        policy_revision: instance.policy_revision,
        session_id: session.clone(),
        graph_sha256: graph_sha256.clone(),
        instrument_id: "BTC-USDT-SWAP".into(),
        timeframe_ms: 60_000,
    };
    fixture
        .native
        .trade
        .bind_graph_strategy(&fixture.owner.configuration.alias, &binding, &cursor)
        .await
        .unwrap();
    fixture
        .native
        .trade
        .set_signal_research(fixture.ctx.research.clone())
        .await;
    // Policy has been paused AFTER creation and durable binding. It is the
    // original Engine authority, not a fake Research-side risk decision.
    fixture.authority(true).await;
    let before = money(&fixture).await;
    let posts = fixture.feed.outbound_posts.lock().unwrap().clone();
    fixture
        .ctx
        .research
        .graph_signal(GraphSessionRequest::Advance {
            session_id: session,
            graph_sha256: binding.graph_sha256.clone(),
            expected_bars: 3,
            bar: GraphClosedBar {
                time_ms: 180_000,
                close: Some(95.),
            },
        })
        .await
        .unwrap();
    let first = fixture
        .native
        .trade
        .graph_research_turn(&fixture.owner)
        .await
        .unwrap();
    assert_eq!(
        first,
        vec![GraphConsumeOutcome::OpenAdmissionScheduled { sequence: 3 }]
    );
    // Joining the account-local permit also proves this spawned admission
    // finished; no 10-second blocked research task is treated as success.
    let _permit = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fixture.owner.graph_order_dispatch.clone().acquire_owned(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        fixture
            .native
            .trade
            .graph_confirmed_cursor(&fixture.owner.configuration.alias, &binding,)
            .await
            .unwrap()
            .unwrap()
            .last_sequence(),
        2,
        "paused RiskBook must not ACK an actionable graph event",
    );
    assert_eq!(money(&fixture).await, before);
    assert_eq!(*fixture.feed.outbound_posts.lock().unwrap(), posts);
    drop(_permit);
    drop(_capture);
    fixture.finish().await;
}

#[tokio::test]
async fn explicit_graph_open_posts_once_through_native_risk_and_durable_cursor() {
    use crate::accounts::recorded_support::RecordedPost;
    use crate::native_finance_risk::NativeFinanceRisk;
    use crate::risk_bridge::NativeRiskClient;
    use okx_protocol::research_ipc::{
        GraphClosedBar, GraphPublicationCursor, GraphSessionId, GraphSessionReply,
        GraphSessionRequest, GraphSignalDefinition,
    };
    use tokio::sync::watch;

    let mut fixture = Fixture::new().await;
    let parameters = json!({
        "signal": graph_signal_parameters(),
        "execution": {"mode":"trade","max_slippage_bps":5,"stop_distance_bps":50}
    });
    completed_data(
        fixture
            .request(StrategyRequest::Create {
                idempotency_key: "graph_native_create".into(),
                instance_id: "graph_native".into(),
                definition_id: "graph_signal_v1".into(),
                version: 1,
                parameters,
                instruments: vec!["BTC-USDT-SWAP".into()],
                timeframe_ms: 60_000,
                budget_usd: "2".into(),
            })
            .await,
    );
    completed_data(
        fixture
            .request(StrategyRequest::Start {
                idempotency_key: "graph_native_start".into(),
                instance_id: "graph_native".into(),
            })
            .await,
    );
    let instance = fixture
        .owner
        .execution
        .store
        .strategy_instances(deadline())
        .await
        .unwrap()
        .into_iter()
        .find(|instance| instance.instance_id == "graph_native")
        .unwrap();

    // Only production maintenance may install this campaign opening. A
    // synthetic opening or a bypassed RiskBook would invalidate the test.
    let (maintenance_stop, stopped) = watch::channel(false);
    let service = fixture.native.trade.clone();
    let maintenance = tokio::spawn(async move { service.run(stopped).await });
    let mut observations = tokio::time::interval(Duration::from_millis(25));
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            observations.tick().await;
            if fixture
                .owner
                .campaign_summary(fixture.native.campaign.clone())
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
    .expect("production maintenance installs the native campaign opening");
    maintenance_stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), maintenance)
        .await
        .unwrap()
        .unwrap();

    // Public reference, executable book and mark are parsed through the
    // original WS coordinator. No fake market snapshot enters the Engine.
    let (market, public, socket) = native::raw_market();
    let mut updates = public.subscribe_market_updates();
    let (market_stop, stopped) = watch::channel(false);
    let market_task = tokio::spawn(async move { market.run(stopped) });
    public.demand_instrument("BTC-USDT-SWAP").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if public
                .risk_fresh_snapshot(
                    "BTC-USDT-SWAP",
                    jiff::Timestamp::now().as_millisecond() as u64,
                    2_000,
                    180_000,
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
    .expect("recorded venue frames must enter the public market parser");
    fixture
        .native
        .attach_recorded_market(socket, public.clone());
    fixture.native.trade = Arc::new(TradeService::new(
        fixture.native.registry.clone(),
        public,
        NativeRiskClient::local(
            NativeFinanceRisk::new(
                fixture.native.owner.clone(),
                fixture.native.owner.installation_id().into(),
                fixture.native.campaign.clone(),
            )
            .unwrap(),
        ),
    ));
    fixture
        .native
        .trade
        .update_governance(&fixture.native.authority)
        .await
        .unwrap();
    fixture
        .native
        .trade
        .set_signal_research(fixture.ctx.research.clone())
        .await;

    let _capture = fixture.owner.recorded_capture_gate.write().await;
    let definition: GraphSignalDefinition =
        serde_json::from_value(graph_signal_parameters()).unwrap();
    let session = GraphSessionId::new("engine:positive-native-open".into()).unwrap();
    let restored = fixture
        .ctx
        .research
        .graph_signal(GraphSessionRequest::Restore {
            session_id: session.clone(),
            definition: Box::new(definition),
            capacity: 64,
            history: vec![
                GraphClosedBar {
                    time_ms: 60_000,
                    close: Some(100.),
                },
                GraphClosedBar {
                    time_ms: 120_000,
                    close: Some(90.),
                },
            ],
        })
        .await
        .unwrap();
    let GraphSessionReply::Evaluated { graph_sha256, .. } = &restored else {
        panic!("Research must evaluate the native graph session");
    };
    let binding = super::graph_target::GraphTargetBinding {
        account_key: fixture.owner.configuration.account_key.wire(),
        instance_id: instance.instance_id,
        definition_version: instance.definition_version,
        policy_revision: instance.policy_revision,
        session_id: session.clone(),
        graph_sha256: graph_sha256.clone(),
        instrument_id: "BTC-USDT-SWAP".into(),
        timeframe_ms: 60_000,
    };
    fixture
        .native
        .trade
        .bind_graph_strategy(
            &fixture.owner.configuration.alias,
            &binding,
            &GraphPublicationCursor::from_restored(&restored).unwrap(),
        )
        .await
        .unwrap();
    fixture
        .ctx
        .research
        .graph_signal(GraphSessionRequest::Advance {
            session_id: session,
            graph_sha256: binding.graph_sha256.clone(),
            expected_bars: 3,
            bar: GraphClosedBar {
                time_ms: 180_000,
                close: Some(95.),
            },
        })
        .await
        .unwrap();

    // Strict finite transport transcript: finance must consume exactly the
    // signed fee/leverage reads, then create one FOK with the bounded stop.
    let now = jiff::Timestamp::now().as_millisecond().to_string();
    fixture.native.feed.expect_public_clock(native::envelope(json!([{"ts":now}])));
    native::record_fee(&fixture.native.feed, &now);
    native::record_leverage(&fixture.native.feed);
    native::record_leverage(&fixture.native.feed);
    native::record_fee(&fixture.native.feed, &now);
    native::record_fee(&fixture.native.feed, &now);
    native::record_leverage(&fixture.native.feed);
    fixture.native.feed.expect_post(RecordedPost {
        access_key: "fixture-executor-a".into(),
        path: "/api/v5/trade/order".into(),
        expected_body: json!({
            "instId":"BTC-USDT-SWAP","tdMode":"cross",
            "clOrdId":"__REQUEST_CL_ORD_ID__","side":"buy","posSide":"long",
            "ordType":"fok","sz":"0.99","px":"1000.5",
            "attachAlgoOrds":[{
                "attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__",
                "tpTriggerPxType":"mark","slTriggerPx":"995.4",
                "slTriggerPxType":"mark","slOrdPx":"-1"
            }]
        }),
        raw_response: native::envelope(json!([{
            "ordId":"native-graph-open-001",
            "clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"
        }])),
    });

    let first = fixture
        .native
        .trade
        .graph_research_turn(&fixture.owner)
        .await
        .unwrap();
    assert_eq!(
        first,
        vec![super::graph_consumer::GraphConsumeOutcome::OpenAdmissionScheduled {
            sequence: 3
        }]
    );
    // Acquiring the same account-local permit proves the spawned Execute has
    // left its risk/venue path; a queued task alone is not evidence of POST.
    let permit = tokio::time::timeout(
        Duration::from_secs(12),
        fixture.owner.graph_order_dispatch.clone().acquire_owned(),
    )
    .await
    .expect("native graph Open admission must finish")
    .unwrap();
    assert_eq!(fixture.native.feed.outbound_posts.lock().unwrap().len(), 1);
    assert_eq!(fixture.native.feed.unconsumed_reads(), 0);
    assert_eq!(
        fixture
            .native
            .trade
            .graph_confirmed_cursor(&fixture.owner.configuration.alias, &binding)
            .await
            .unwrap()
            .unwrap()
            .last_sequence(),
        3,
        "the native SQL prepare must bind the graph cursor to the order"
    );
    assert!(
        fixture
            .owner
            .reconcilable_intents_page(
                fixture.native.config.account_key.wire(),
                100,
                0,
                deadline(),
            )
            .await
            .unwrap()
            .iter()
            .any(|intent| intent.plan.strategy_instance == "graph_native"),
        "the Graph order must survive in the original finance ledger"
    );
    let replay = fixture
        .native
        .trade
        .graph_research_turn(&fixture.owner)
        .await
        .unwrap();
    assert!(matches!(
        replay.as_slice(),
        [super::graph_consumer::GraphConsumeOutcome::Idle {
            confirmed_sequence: 3,
            ..
        }]
    ));
    assert_eq!(fixture.native.feed.outbound_posts.lock().unwrap().len(), 1);

    drop(permit);
    drop(_capture);
    market_stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), market_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    fixture.finish().await;
}

fn rejected(response: AgentResponse, code: &str) {
    assert_eq!(response.status, AgentResponseStatus::Rejected);
    assert_eq!(response.quality, DataQuality::NotReady);
    assert!(response.result.is_none());
    assert_eq!(response.failure.unwrap().code, code);
}
async fn money(fixture: &Fixture) -> Value {
    let owner = &fixture.native.owner;
    let account = fixture.native.config.account_key.wire();
    let report = owner
        .finance_report(
            account.clone(),
            okx_execution::FinanceReportQuery {
                from: i64::MIN,
                through: i64::MAX,
                instances: vec![],
                currencies: vec![],
                view: okx_execution::FinanceReportView::Summary,
            },
            deadline(),
        )
        .await
        .unwrap();
    json!({
        "finance_revision": report.revision,
        "finance": report.entries,
        "lots": owner.active_lots(account.clone(), "BTC-USDT-SWAP".into(), deadline()).await.unwrap(),
        "intents": owner.reconcilable_intents_page(account.clone(), 100, 0, deadline()).await.unwrap(),
        "fills": owner.source_cursor(account.clone(), "fills".into(), "SWAP".into()).await.unwrap(),
        "bills": owner.source_cursor(account, "bills".into(), "account".into()).await.unwrap(),
    })
}

#[tokio::test]
async fn public_strategy_lifecycle_replays_cas_and_read_models_preserve_money() {
    let fixture = Fixture::new().await;
    let capture = fixture.owner.recorded_capture_gate.write().await;
    let baseline = money(&fixture).await;
    let requests = fixture.feed.requests.lock().unwrap().clone();
    let posts = fixture.feed.outbound_posts.lock().unwrap().clone();
    let denied = fixture.feed.denied_routes.lock().unwrap().clone();
    let created =
        completed_data(fixture.request(create("create_contract")).await)["instance"].clone();
    assert_eq!(created["policy_revision"], 1);
    assert_eq!(created["enabled"], false);
    assert_eq!(
        completed_data(fixture.request(create("create_contract")).await)["instance"],
        created
    );
    let old_definition = fixture
        .native
        .owner
        .plan_definition(
            deadline(),
            fixture.native.config.account_key.wire(),
            INSTANCE.into(),
            false,
        )
        .await
        .unwrap();
    let mut conflicting = create("create_contract");
    if let StrategyRequest::Create { budget_usd, .. } = &mut conflicting {
        *budget_usd = "3".into();
    }
    rejected(fixture.request(conflicting).await, "IDEMPOTENCY_CONFLICT");
    let changed =
        completed_data(fixture.request(update("update_contract", 1)).await)["instance"].clone();
    assert_eq!(changed["policy_revision"], 2);
    assert_ne!(changed["definition_version"], created["definition_version"]);
    assert_eq!(
        completed_data(fixture.request(update("update_contract", 1)).await)["instance"],
        changed
    );
    rejected(
        fixture.request(update("stale_contract", 1)).await,
        "INSTANCE_REVISION_CONFLICT",
    );
    let new_definition = fixture
        .native
        .owner
        .plan_definition(
            deadline(),
            fixture.native.config.account_key.wire(),
            INSTANCE.into(),
            false,
        )
        .await
        .unwrap();
    assert_ne!(old_definition.version, new_definition.version);
    assert!(
        fixture
            .native
            .owner
            .operation(
                deadline(),
                fixture.native.config.account_key.wire(),
                "stale_contract".into()
            )
            .await
            .unwrap()
            .is_none()
    );
    let start = StrategyRequest::Start {
        idempotency_key: "start_contract".into(),
        instance_id: INSTANCE.into(),
    };
    let started = completed_data(fixture.request(start.clone()).await)["instance"].clone();
    assert_eq!(started["enabled"], true);
    assert_eq!(started["policy_revision"], 3);
    assert_eq!(
        completed_data(fixture.request(start).await)["instance"],
        started
    );
    let stop = StrategyRequest::Stop {
        idempotency_key: "stop_contract".into(),
        instance_id: INSTANCE.into(),
    };
    let stopped = completed_data(fixture.request(stop.clone()).await)["instance"].clone();
    assert_eq!(stopped["enabled"], false);
    assert_eq!(stopped["policy_revision"], 4);
    assert_eq!(
        completed_data(fixture.request(stop).await)["instance"],
        stopped
    );
    let list = completed_data(fixture.request(StrategyRequest::List {}).await);
    assert!(list["instances"].as_array().unwrap().contains(&stopped));
    let status = completed_data(
        fixture
            .request(StrategyRequest::Status {
                instance_id: INSTANCE.into(),
            })
            .await,
    );
    assert_eq!(status["instance"], stopped);
    assert_eq!(status["positions"], json!([]));
    rejected(
        fixture
            .operation(
                AgentOperation::Strategies {
                    request: create("no_account"),
                },
                None,
            )
            .await,
        "EXPLICIT_ACCOUNT_REQUIRED",
    );
    rejected(
        fixture
            .operation(
                AgentOperation::Strategies {
                    request: create("wrong_account"),
                },
                Some("unconnected"),
            )
            .await,
        "ACCOUNT_NOT_CONNECTED",
    );
    assert_eq!(money(&fixture).await, baseline);
    assert_eq!(*fixture.feed.requests.lock().unwrap(), requests);
    assert_eq!(*fixture.feed.outbound_posts.lock().unwrap(), posts);
    assert_eq!(*fixture.feed.denied_routes.lock().unwrap(), denied);
    drop(capture);
    fixture.finish().await;
}

#[tokio::test]
async fn public_strategy_capital_uses_authenticated_flat_opening_and_paused_authority() {
    let fixture = Fixture::new().await;
    let capture = fixture.owner.recorded_capture_gate.write().await;
    completed_data(fixture.request(create("capital_create")).await);
    // initialize_flat received the real observer's identity-bound, stable flat
    // opening; do not insert a COMPLETE NAV point or set a test coverage flag.
    let capital_before = fixture
        .native
        .owner
        .capital_state(fixture.native.config.account_key.wire(), now(), deadline())
        .await
        .unwrap();
    assert_eq!(capital_before.coverage, "COMPLETE");
    assert!(!capital_before.nav_source_ref.is_empty());
    let financial_before = money(&fixture).await;
    let requests = fixture.feed.requests.lock().unwrap().clone();
    let posts = fixture.feed.outbound_posts.lock().unwrap().clone();
    let assign = StrategyRequest::CapitalAssign {
        idempotency_key: "assign_contract".into(),
        instance_id: INSTANCE.into(),
        direction: CapitalDirection::Assign,
        amount_usd: "10".into(),
        expected_authority_revision: fixture.native.authority.state.revision,
    };
    let assigned = completed_data(fixture.request(assign.clone()).await);
    assert_eq!(assigned["capital"]["assigned_usd"], "10");
    assert_eq!(assigned["capital_coverage"], "COMPLETE");
    assert_eq!(
        assigned["capital"]["nav_source_ref"],
        capital_before.nav_source_ref
    );
    let after_assign = money(&fixture).await;
    assert_eq!(after_assign["lots"], financial_before["lots"]);
    assert_eq!(after_assign["intents"], financial_before["intents"]);
    let replay = completed_data(fixture.request(assign.clone()).await);
    assert_eq!(replay["receipt"], assigned["receipt"]);
    assert_eq!(money(&fixture).await, after_assign);
    let mut conflict = assign;
    if let StrategyRequest::CapitalAssign { amount_usd, .. } = &mut conflict {
        *amount_usd = "11".into();
    }
    rejected(fixture.request(conflict).await, "IDEMPOTENCY_CONFLICT");
    fixture.authority(true).await;
    let paused_before = money(&fixture).await;
    rejected(
        fixture
            .request(StrategyRequest::Start {
                idempotency_key: "paused_start".into(),
                instance_id: INSTANCE.into(),
            })
            .await,
        "ACCOUNT_NEW_RISK_PAUSED",
    );
    rejected(
        fixture
            .request(StrategyRequest::CapitalAssign {
                idempotency_key: "paused_assign".into(),
                instance_id: INSTANCE.into(),
                direction: CapitalDirection::Assign,
                amount_usd: "1".into(),
                expected_authority_revision: fixture.native.authority.state.revision + 1,
            })
            .await,
        "ACCOUNT_NEW_RISK_PAUSED",
    );
    rejected(
        fixture
            .request(StrategyRequest::CapitalAssign {
                idempotency_key: "stale_authority".into(),
                instance_id: INSTANCE.into(),
                direction: CapitalDirection::Assign,
                amount_usd: "1".into(),
                expected_authority_revision: fixture.native.authority.state.revision,
            })
            .await,
        "CAPITAL_AUTHORITY_REVISION_MISMATCH",
    );
    assert_eq!(money(&fixture).await, paused_before);
    for key in ["paused_start", "paused_assign", "stale_authority"] {
        assert!(
            fixture
                .native
                .owner
                .operation(
                    deadline(),
                    fixture.native.config.account_key.wire(),
                    key.into()
                )
                .await
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(*fixture.feed.requests.lock().unwrap(), requests);
    assert_eq!(*fixture.feed.outbound_posts.lock().unwrap(), posts);
    drop(capture);
    fixture.finish().await;
}

/// Bounded S2-05 proof: original intent creation is deliberately through the
/// SQL owner, not a claimed successful exchange admission. Public Update and
/// public Execute replay must nevertheless preserve that immutable identity.
/// A full post-ACK strategy-edit scenario belongs to the exchange transcript
/// harness; this test does not claim new-key public admission coverage.
#[tokio::test]
async fn public_strategy_update_keeps_sql_bound_original_execute_replay_immutable() {
    let fixture = Fixture::new().await;
    let capture = fixture.owner.recorded_capture_gate.write().await;
    completed_data(fixture.request(create("binding_create")).await);
    let command = TradeRequest::Execute {
        idempotency_key: "strategy_original_trade".into(),
        operation: TradeOperation::Open,
        strategy_instance: INSTANCE.into(),
        instrument: "BTC-USDT-SWAP".into(),
        side: PositionSide::Long,
        sizing: Sizing::Contracts {
            contracts: "1".into(),
        },
        order: OrderConstraints {
            order_type: "fok".into(),
            limit_price: Some("100".into()),
            reference: None,
            max_entry_usd: None,
            max_slippage_bps: 5,
        },
        protection: Some(Protection::Distance {
            stop_distance_bps: 100,
        }),
        expires_in_ms: 30_000,
    };
    use sha2::Digest;
    let original_hash = format!(
        "{:x}",
        sha2::Sha256::digest(
            command
                .canonical_semantic_json(&fixture.owner.configuration.account_key.wire())
                .unwrap()
                .as_bytes()
        )
    );
    let account_key = fixture.native.config.account_key.wire();
    let definition = fixture
        .native
        .owner
        .plan_definition(deadline(), account_key.clone(), INSTANCE.into(), false)
        .await
        .unwrap();
    let at = now();
    let revision = fixture.native.authority.state.revision as i64;
    let saved = fixture
        .native
        .owner
        .prepare_admission(
            deadline(),
            account_key.clone(),
            Decision {
                decision_id: "binding_decision".into(),
                instance_id: Some(INSTANCE.into()),
                decision_at: at,
                input_digest: "b".repeat(64),
                policy_revision: revision,
                code_version: "fixture".into(),
            },
            NewIntent {
                intent_id: "binding_intent".into(),
                idempotency_key: "strategy_original_trade".into(),
                request_hash: original_hash,
                client_order_id: "b".repeat(32),
                deadline: at + 30_000,
                plan_hash: "c".repeat(64),
                policy_revision: revision,
                risk_increase: true,
                instrument_id: "BTC-USDT-SWAP".into(),
                plan: IntentPlan {
                    strategy_definition_id: definition.definition_id,
                    strategy_definition_version: definition.version,
                    strategy_parameters_digest: definition.parameters_digest,
                    operation: "open".into(),
                    side: "buy".into(),
                    position_side: "long".into(),
                    quantity: Decimal::ONE,
                    limit_price: Decimal::from(100),
                    stop_price: Some(Decimal::from(99)),
                    strategy_instance: INSTANCE.into(),
                    planned_loss_usd: Decimal::ONE,
                    source_generation: 1,
                    private_source_generation: 1,
                    contract_value: Decimal::ONE,
                    inverse: false,
                    settlement_currency: "USDT".into(),
                },
            },
        )
        .await
        .unwrap();
    completed_data(fixture.request(update("binding_update", 1)).await);
    let before = money(&fixture).await;
    let requests = fixture.feed.requests.lock().unwrap().clone();
    let posts = fixture.feed.outbound_posts.lock().unwrap().clone();
    let replay = completed_data(
        fixture
            .operation(
                AgentOperation::Trade {
                    request: command.clone(),
                },
                Some(&fixture.owner.configuration.alias),
            )
            .await,
    );
    assert_eq!(replay["intent"], serde_json::to_value(&saved).unwrap());
    let old = fixture
        .native
        .owner
        .lookup_intent(
            account_key.clone(),
            okx_execution::TradeIntentLookup::IntentId("binding_intent".into()),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&old).unwrap(),
        serde_json::to_value(&saved).unwrap()
    );
    let current = fixture
        .native
        .owner
        .plan_definition(deadline(), account_key.clone(), INSTANCE.into(), false)
        .await
        .unwrap();
    assert_ne!(current.version, old.plan.strategy_definition_version);
    let fresh = NewIntent {
        intent_id: "binding_new_intent".into(),
        idempotency_key: "binding_new_key".into(),
        request_hash: "d".repeat(64),
        client_order_id: "d".repeat(32),
        deadline: now() + 30_000,
        plan_hash: "e".repeat(64),
        policy_revision: revision,
        risk_increase: true,
        instrument_id: old.instrument_id,
        plan: old.plan,
    };
    let result = fixture
        .native
        .owner
        .prepare_admission(
            deadline(),
            account_key.clone(),
            Decision {
                decision_id: "binding_new_decision".into(),
                instance_id: Some(INSTANCE.into()),
                decision_at: now(),
                input_digest: "e".repeat(64),
                policy_revision: revision,
                code_version: "fixture".into(),
            },
            fresh,
        )
        .await;
    assert_eq!(result.unwrap_err().to_string(), "STRATEGY_VERSION_CHANGED");
    assert!(
        fixture
            .native
            .owner
            .lookup_intent(
                account_key,
                okx_execution::TradeIntentLookup::IdempotencyKey("binding_new_key".into()),
                deadline()
            )
            .await
            .unwrap()
            .is_none()
    );
    let mut conflict = command;
    if let TradeRequest::Execute { sizing, .. } = &mut conflict {
        *sizing = Sizing::Contracts {
            contracts: "2".into(),
        };
    }
    rejected(
        fixture
            .operation(
                AgentOperation::Trade { request: conflict },
                Some(&fixture.owner.configuration.alias),
            )
            .await,
        "IDEMPOTENCY_CONFLICT",
    );
    assert_eq!(money(&fixture).await, before);
    assert_eq!(*fixture.feed.requests.lock().unwrap(), requests);
    assert_eq!(*fixture.feed.outbound_posts.lock().unwrap(), posts);
    drop(capture);
    fixture.finish().await;
}

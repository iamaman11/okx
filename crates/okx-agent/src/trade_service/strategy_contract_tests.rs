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
    completed_data(fixture.request(create("graph_consumer_create")).await);
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
    fixture
        .native
        .trade
        .bind_graph_strategy(&account, &binding, &original)
        .await
        .unwrap();
    let consumer =
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
            .replay_one_page(&fixture.native.trade, &fixture.ctx.research)
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
    let outcome = consumer
        .replay_one_page(&fixture.native.trade, &fixture.ctx.research)
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

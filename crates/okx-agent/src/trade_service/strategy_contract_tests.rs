//! Public Strategies routing against the real registry and SQL owner. Recorded
//! HTTP/WS ports provide delivery only; no strategy or money owner is replaced.
use super::*;
use crate::accounts::{initialize_installation, recorded_support};
use okx_protocol::{AgentOperation, AgentRequest, AgentResponse, AgentResponseStatus, DataQuality};

const INSTALLATION: &str = "strategy-public-contract";
const INSTANCE: &str = "ema_contract";

struct Fixture {
    root: std::path::PathBuf,
    ctx: crate::query::DispatchContext,
    feed: Arc<recorded_support::RecordedAccountHttp>,
    _coordinator: okx_runtime::PublicWsCoordinator,
    risk: tokio::sync::mpsc::Receiver<crate::risk_bridge::OutboundRiskCommand>,
    owner: Arc<AccountResources>,
}

impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "okx-strategy-public-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        initialize_installation(&root, INSTALLATION).unwrap();
        let budget = okx_api::RateBudget::new();
        let accounts = Arc::new(
            AccountRuntimeRegistry::open(&root, budget.clone())
                .await
                .unwrap(),
        );
        let feed = Arc::new(recorded_support::RecordedAccountHttp::default());
        let (config, clients) = recorded_support::prepared("a", feed.clone(), budget.clone());
        accounts
            .connect_prepared(config.clone(), clients)
            .await
            .unwrap();
        let owner = accounts.resolve(&config.alias).await.unwrap();
        let environment = okx_api::OkxEnvironment::new(okx_api::Region::Global, false);
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
        let (tx, risk) = tokio::sync::mpsc::channel(4);
        let trade = Arc::new(TradeService::new(
            accounts.clone(),
            public_ws.clone(),
            crate::risk_bridge::NativeRiskClient::new(tx),
        ));
        let ctx = crate::query::DispatchContext {
            accounts,
            analytics: crate::analytics::AnalyticsService::new(
                root.join("research"),
                market.clone(),
            )
            .unwrap(),
            public_ws,
            trade,
            market,
        };
        let result = Self {
            root,
            ctx,
            feed,
            _coordinator: coordinator,
            risk,
            owner,
        };
        result.authority(false).await;
        result
    }

    async fn authority(&self, paused: bool) {
        let config = &self.owner.configuration;
        let authority: CentralGovernanceSnapshot = serde_json::from_value(json!({
            "state":{"schema":"okx.governance/v3","revision":if paused {2} else {1},
                "installation_id":INSTALLATION,"campaign_id":format!("sha256:{}", "a".repeat(64)),
                "campaign_loss_limit_usd":"20","max_total_modelled_loss_usdt":"20",
                "accounts":[{"id":"account_a","label":"Fixture A","venue":"okx","environment":"real",
                    "uid_fingerprint":config.account_key.verified_uid,"identity_verified":true,
                    "enabled":true,"explicit_pause":paused,"policy_revision":if paused {2} else {1},
                    "account_key":config.account_key.wire(),"strategies":[],
                    "limits":{"max_modelled_loss_usdt":"20","max_order_loss_usdt":"10",
                        "max_notional_usdt":"1000","max_leverage":"5","max_daily_realized_loss_usdt":"20",
                        "max_drawdown_ratio":"0.5","allowed_instruments":["BTC-USDT-SWAP"]}}]},
            "issued_at_ms":1792910000000_u64,"session_generation":673
        })).unwrap();
        assert!(authority.valid());
        // Use the production authority owner to apply the same policy before
        // taking no-effect snapshots. Rejected commands may observe this policy.
        *self.ctx.trade.governance.write().await = Some(authority);
        self.ctx.trade.authority(&self.owner, None).await.unwrap();
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

    async fn finish(mut self) {
        assert!(matches!(
            self.risk.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        let mut terminated = self.owner.execution.store.termination_signal();
        self.ctx.accounts.shutdown().await;
        let root = self.root.clone();
        drop(self);
        tokio::time::timeout(Duration::from_secs(5), terminated.wait_for(|v| *v))
            .await
            .unwrap()
            .unwrap();
        std::fs::remove_dir_all(root).unwrap();
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
fn rejected(response: AgentResponse, code: &str) {
    assert_eq!(response.status, AgentResponseStatus::Rejected);
    assert_eq!(response.quality, DataQuality::NotReady);
    assert!(response.result.is_none());
    assert_eq!(response.failure.unwrap().code, code);
}
async fn money(store: &AccountStoreHandle) -> Value {
    store
        .call(deadline(), |s| {
            Ok(json!({
                "finance_revision":s.finance_revision()?,
                "finance":s.finance_totals(i64::MIN,i64::MAX,None)?.into_iter().collect::<Vec<_>>(),
                "lots":s.active_lots("BTC-USDT-SWAP")?,
                "intents":s.reconcilable_intents_page(100,0)?,
                "fills":s.cursor("fills","SWAP")?,"bills":s.cursor("bills","account")?
            }))
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn public_strategy_lifecycle_replays_cas_and_read_models_preserve_money() {
    let fixture = Fixture::new().await;
    let capture = fixture.owner.recorded_capture_gate.write().await;
    let baseline = money(&fixture.owner.execution.store).await;
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
        .owner
        .execution
        .store
        .call(deadline(), |s| s.strategy_definition(INSTANCE))
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
    let old_for_sql = old_definition.clone();
    fixture
        .owner
        .execution
        .store
        .call(deadline(), move |s| {
            assert_eq!(
                s.definition_version(&old_for_sql.definition_id, &old_for_sql.version)?
                    .unwrap(),
                old_for_sql
            );
            assert!(s.operation("stale_contract")?.is_none());
            Ok(())
        })
        .await
        .unwrap();
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
    assert_eq!(money(&fixture.owner.execution.store).await, baseline);
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
        .owner
        .execution
        .store
        .call(deadline(), |s| s.capital_state(now()))
        .await
        .unwrap();
    assert_eq!(capital_before.coverage, "COMPLETE");
    assert!(!capital_before.nav_source_ref.is_empty());
    let financial_before = money(&fixture.owner.execution.store).await;
    let requests = fixture.feed.requests.lock().unwrap().clone();
    let posts = fixture.feed.outbound_posts.lock().unwrap().clone();
    let assign = StrategyRequest::CapitalAssign {
        idempotency_key: "assign_contract".into(),
        instance_id: INSTANCE.into(),
        direction: CapitalDirection::Assign,
        amount_usd: "10".into(),
        expected_authority_revision: 1,
    };
    let assigned = completed_data(fixture.request(assign.clone()).await);
    assert_eq!(assigned["capital"]["assigned_usd"], "10");
    assert_eq!(assigned["capital_coverage"], "COMPLETE");
    assert_eq!(
        assigned["capital"]["nav_source_ref"],
        capital_before.nav_source_ref
    );
    let after_assign = money(&fixture.owner.execution.store).await;
    assert_eq!(after_assign["lots"], financial_before["lots"]);
    assert_eq!(after_assign["intents"], financial_before["intents"]);
    let replay = completed_data(fixture.request(assign.clone()).await);
    assert_eq!(replay["receipt"], assigned["receipt"]);
    assert_eq!(money(&fixture.owner.execution.store).await, after_assign);
    let mut conflict = assign;
    if let StrategyRequest::CapitalAssign { amount_usd, .. } = &mut conflict {
        *amount_usd = "11".into();
    }
    rejected(fixture.request(conflict).await, "IDEMPOTENCY_CONFLICT");
    fixture.authority(true).await;
    let paused_before = money(&fixture.owner.execution.store).await;
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
                expected_authority_revision: 2,
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
                expected_authority_revision: 1,
            })
            .await,
        "CAPITAL_AUTHORITY_REVISION_MISMATCH",
    );
    assert_eq!(money(&fixture.owner.execution.store).await, paused_before);
    fixture
        .owner
        .execution
        .store
        .call(deadline(), |s| {
            for key in ["paused_start", "paused_assign", "stale_authority"] {
                assert!(s.operation(key)?.is_none());
            }
            Ok(())
        })
        .await
        .unwrap();
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
    let saved = fixture
        .owner
        .execution
        .store
        .call(deadline(), move |s| {
            let definition = s.strategy_definition(INSTANCE)?;
            let at = now();
            s.prepare(
                &Decision {
                    decision_id: "binding_decision".into(),
                    instance_id: Some(INSTANCE.into()),
                    decision_at: at,
                    input_digest: "b".repeat(64),
                    policy_revision: 1,
                    code_version: "fixture".into(),
                },
                &NewIntent {
                    intent_id: "binding_intent".into(),
                    idempotency_key: "strategy_original_trade".into(),
                    request_hash: original_hash,
                    client_order_id: "b".repeat(32),
                    deadline: at + 30_000,
                    plan_hash: "c".repeat(64),
                    policy_revision: 1,
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
        })
        .await
        .unwrap();
    completed_data(fixture.request(update("binding_update", 1)).await);
    let before = money(&fixture.owner.execution.store).await;
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
    let saved_for_sql = saved.clone();
    fixture
        .owner
        .execution
        .store
        .call(deadline(), move |s| {
            let old = s.intent("binding_intent")?.unwrap();
            assert_eq!(
                serde_json::to_value(&old).unwrap(),
                serde_json::to_value(&saved_for_sql).unwrap()
            );
            assert_ne!(
                s.strategy_definition(INSTANCE)?.version,
                old.plan.strategy_definition_version
            );
            assert!(
                s.definition_version(
                    &old.plan.strategy_definition_id,
                    &old.plan.strategy_definition_version
                )?
                .is_some()
            );
            // A different SQL intent cannot bind the pre-edit definition. This is
            // the store's version gate, explicitly not public new-key admission.
            let fresh = NewIntent {
                intent_id: "binding_new_intent".into(),
                idempotency_key: "binding_new_key".into(),
                request_hash: "d".repeat(64),
                client_order_id: "d".repeat(32),
                deadline: now() + 30_000,
                plan_hash: "e".repeat(64),
                policy_revision: 1,
                risk_increase: true,
                instrument_id: old.instrument_id,
                plan: old.plan,
            };
            assert_eq!(
                s.prepare(
                    &Decision {
                        decision_id: "binding_new_decision".into(),
                        instance_id: Some(INSTANCE.into()),
                        decision_at: now(),
                        input_digest: "e".repeat(64),
                        policy_revision: 1,
                        code_version: "fixture".into()
                    },
                    &fresh
                )
                .unwrap_err()
                .to_string(),
                "STRATEGY_VERSION_CHANGED"
            );
            assert!(s.by_idempotency("binding_new_key")?.is_none());
            Ok(())
        })
        .await
        .unwrap();
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
    assert_eq!(money(&fixture.owner.execution.store).await, before);
    assert_eq!(*fixture.feed.requests.lock().unwrap(), requests);
    assert_eq!(*fixture.feed.outbound_posts.lock().unwrap(), posts);
    drop(capture);
    fixture.finish().await;
}

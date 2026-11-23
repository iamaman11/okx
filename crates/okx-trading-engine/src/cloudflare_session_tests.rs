//! Production Rust session with recorded frame delivery and the real Native owner.
use super::*;
use crate::accounts::{native_trade_contracts::fixture as native, recorded_support};
use futures_util::{Sink, Stream};
use okx_ipc_contracts::{AGENT_REQUEST_SCHEMA_V1, AgentRequest, trading::StrategyRequest};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::mpsc;

const SESSION: &str = "session_native_contract_001";
const GENERATION: u64 = 1;
struct RawFrames {
    inbound: mpsc::UnboundedReceiver<Result<Message, tokio_tungstenite::tungstenite::Error>>,
    outbound: mpsc::UnboundedSender<Message>,
}
impl Stream for RawFrames {
    type Item = Result<Message, tokio_tungstenite::tungstenite::Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inbound.poll_recv(cx)
    }
}
impl Sink<Message> for RawFrames {
    type Error = tokio_tungstenite::tungstenite::Error;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
        self.outbound
            .send(message)
            .map_err(|_| tokio_tungstenite::tungstenite::Error::ConnectionClosed)
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
fn wire(value: F) -> Message {
    Message::Text(serde_json::to_string(&value).unwrap().into())
}
struct Session {
    native: native::NativeFixture,
    input: mpsc::UnboundedSender<Result<Message, tokio_tungstenite::tungstenite::Error>>,
    output: mpsc::UnboundedReceiver<Message>,
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<AgentResult<()>>,
    research: crate::research_client::ResearchClient,
    _coordinator: okx_market_runtime::PublicWsCoordinator,
}
impl Session {
    async fn new() -> Self {
        Self::with_research_only(false).await
    }
    async fn with_research_only(research_only: bool) -> Self {
        let native = native::fixture().await;
        let environment = okx_api::OkxEnvironment::new(okx_api::Region::Global, false);
        let budget = okx_api::RateBudget::new();
        let (coordinator, public_ws) = okx_market_runtime::PublicWsCoordinator::new_with_connector(
            environment,
            okx_observation::ReferenceRegistry::from_public("session-test", vec![]).unwrap(),
            budget.clone(),
            Arc::new(recorded_support::OfflineSocket),
        );
        let market = crate::market_bootstrap::MarketBootstrapper::new(
            okx_api::OkxPublicClient::with_http_transport(environment, budget, native.feed.clone())
                .unwrap(),
        );
        let ctx = DispatchContext {
            research: crate::research_test_support::client(
                native.root.join("session-analytics"),
                market.clone(),
            )
            .unwrap(),
            public_ws,
            finance: if research_only {
                crate::finance_dispatch::FinanceDispatch::missing()
            } else {
                crate::finance_dispatch::FinanceDispatch::ready(
                    native.registry.clone(),
                    native.trade.clone(),
                )
            },
            market,
        };
        let research = ctx.research.clone();
        let (input, incoming) = mpsc::unbounded_channel();
        let (outgoing, output) = mpsc::unbounded_channel();
        let (stop, stopped) = watch::channel(false);
        let hello = if research_only {
            F::HelloResearch {
                schema: SCHEMA.into(),
                runtime_id: "windows-primary".into(),
                connection_id: "native_0123456789abcdef0123456789abcdef".into(),
                host_scope: "a".repeat(64),
                contract: okx_ipc_contracts::accounts::PRODUCT_CONTRACT.into(),
                runtime_profile: DirectRuntimeProfile::Engine,
                agent_pid: 100,
                agent_binary_sha256: "a".repeat(64),
                agent_incarnation: format!("ctl_{}", "b".repeat(32)),
            }
        } else {
            F::Hello {
                schema: SCHEMA.into(),
                runtime_id: "windows-primary".into(),
                connection_id: "native_0123456789abcdef0123456789abcdef".into(),
                installation_id: native.owner.installation_id().into(),
                contract: okx_ipc_contracts::accounts::PRODUCT_CONTRACT.into(),
                runtime_profile: DirectRuntimeProfile::Engine,
                agent_pid: 100,
                agent_binary_sha256: "a".repeat(64),
                agent_incarnation: format!("ctl_{}", "b".repeat(32)),
            }
        };
        let task = tokio::spawn(run_connected_session(
            RawFrames {
                inbound: incoming,
                outbound: outgoing,
            },
            hello,
            ctx,
            stopped,
        ));
        let mut session = Self {
            native,
            input,
            output,
            stop,
            task,
            research,
            _coordinator: coordinator,
        };
        let sent_hello = session.next().await;
        assert!(if research_only {
            matches!(sent_hello, F::HelloResearch { .. })
        } else {
            matches!(sent_hello, F::Hello { .. })
        });
        session.send(F::HelloAck {
            schema: SCHEMA.into(),
            session_id: SESSION.into(),
            connection_generation: GENERATION,
        });
        session
    }
    fn send(&self, frame: F) {
        self.input.send(Ok(wire(frame))).unwrap();
    }
    async fn next(&mut self) -> F {
        let message = tokio::time::timeout(Duration::from_secs(15), self.output.recv())
            .await
            .unwrap()
            .unwrap();
        serde_json::from_str(message.to_text().unwrap()).unwrap()
    }
    async fn end(mut self, expected: Option<&str>) {
        if expected.is_none() {
            self.stop.send(true).unwrap();
        }
        let result = tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .unwrap()
            .unwrap();
        match expected {
            Some(reason) => assert!(result.unwrap_err().to_string().contains(reason)),
            None => result.unwrap(),
        }
        if expected == Some("RESEARCH_OPERATION_FORBIDDEN") {
            while let Ok(message) = self.output.try_recv() {
                let frame: F = serde_json::from_str(message.to_text().unwrap()).unwrap();
                assert!(
                    !matches!(frame, F::DeliveryAck { .. } | F::Response { .. }),
                    "forbidden operation must not reach dispatch"
                );
            }
        }
        drop(self.input);
        drop(self.output);
        drop(self._coordinator);
        self.research.shutdown().await;
        self.native.finish().await;
    }
}

fn research_request() -> AgentRequest {
    AgentRequest {
        schema: AGENT_REQUEST_SCHEMA_V1.into(),
        request_id: "research_session_capabilities_001".into(),
        account: None,
        operation: AgentOperation::Research {
            request: okx_ipc_contracts::analytics::ResearchRequest::Capabilities {},
        },
    }
}

#[tokio::test]
async fn research_session_forwards_to_real_worker_without_finance() {
    let mut s = Session::with_research_only(true).await;
    s.send(F::Request {
        schema: SCHEMA.into(),
        session_id: SESSION.into(),
        connection_generation: GENERATION,
        request: research_request(),
        operator_governance: None,
    });
    let mut ack = false;
    loop {
        match s.next().await {
            F::DeliveryAck { request_id, .. } => {
                assert_eq!(request_id, "research_session_capabilities_001");
                ack = true;
            }
            F::Response { response, .. } => {
                assert!(ack);
                assert_eq!(
                    response.status,
                    okx_ipc_contracts::AgentResponseStatus::Completed
                );
                let data = response.result.unwrap();
                assert_eq!(data["schema"], "okx.research.capabilities/v1");
                assert!(!data.as_object().unwrap().is_empty());
                break;
            }
            F::Ping { .. } => {}
            other => panic!("unexpected frame {other:?}"),
        }
    }
    assert!(s.native.feed.outbound_posts.lock().unwrap().is_empty());
    s.end(None).await;
}

#[tokio::test]
async fn research_session_rejects_finance_and_governance_before_ack() {
    for governance in [false, true] {
        let s = Session::with_research_only(true).await;
        let request = if governance {
            research_request()
        } else {
            AgentRequest {
                schema: AGENT_REQUEST_SCHEMA_V1.into(),
                request_id: "research_session_forbidden_001".into(),
                account: None,
                operation: AgentOperation::Strategies {
                    request: StrategyRequest::List {},
                },
            }
        };
        let snapshot = if governance {
            Some(s.native.authority.clone())
        } else {
            None
        };
        s.send(F::Request {
            schema: SCHEMA.into(),
            session_id: SESSION.into(),
            connection_generation: GENERATION,
            request,
            operator_governance: snapshot,
        });
        s.end(Some("RESEARCH_OPERATION_FORBIDDEN")).await;
    }
    let s = Session::with_research_only(true).await;
    s.send(F::PolicySnapshot {
        schema: SCHEMA.into(),
        session_id: SESSION.into(),
        connection_generation: GENERATION,
        snapshot: s.native.authority.clone(),
    });
    s.end(Some("RESEARCH_POLICY_FORBIDDEN")).await;
    let s = Session::with_research_only(true).await;
    s.send(F::RiskControl {
        schema: SCHEMA.into(),
        session_id: SESSION.into(),
        connection_generation: GENERATION,
        command: okx_ipc_contracts::risk_authority::RiskCommand {
            schema: okx_ipc_contracts::risk_authority::RISK_COMMAND_SCHEMA_V1.into(),
            command_id: "research_risk_command_001".into(),
            installation_id: s.native.owner.installation_id().into(),
            action: okx_ipc_contracts::risk_authority::RiskCommandAction::Summary {},
        },
    });
    s.end(Some("UNEXPECTED_FRAME")).await;
}

#[test]
fn research_hello_protocol_rejects_mixed_fields_and_bad_scope() {
    let hello = F::HelloResearch {
        schema: SCHEMA.into(),
        runtime_id: "windows-primary".into(),
        connection_id: "native_0123456789abcdef0123456789abcdef".into(),
        host_scope: "a".repeat(64),
        contract: okx_ipc_contracts::accounts::PRODUCT_CONTRACT.into(),
        runtime_profile: DirectRuntimeProfile::Engine,
        agent_pid: 100,
        agent_binary_sha256: "b".repeat(64),
        agent_incarnation: format!("ctl_{}", "c".repeat(32)),
    };
    assert!(hello.validate().is_ok());
    let mut value = serde_json::to_value(&hello).unwrap();
    value["installation_id"] = "mixed_installation".into();
    assert!(serde_json::from_value::<F>(value).is_err());
    let mut value = serde_json::to_value(hello).unwrap();
    value["host_scope"] = "BAD".into();
    assert!(
        serde_json::from_value::<F>(value)
            .unwrap()
            .validate()
            .is_err()
    );
}

#[tokio::test]
async fn native_session_public_data_is_acked_and_dispatched() {
    let mut s = Session::new().await;
    let id = "native_session_data_001";
    s.send(F::Request {
        schema: SCHEMA.into(),
        session_id: SESSION.into(),
        connection_generation: GENERATION,
        request: AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.into(),
            request_id: id.into(),
            account: Some(s.native.config.alias.clone()),
            operation: AgentOperation::Strategies {
                request: StrategyRequest::List {},
            },
        },
        operator_governance: None,
    });
    let mut ack = false;
    let mut response = false;
    while !ack || !response {
        match s.next().await {
            F::DeliveryAck {
                session_id,
                connection_generation,
                request_id,
                ..
            } => {
                assert_eq!(
                    (
                        session_id.as_str(),
                        connection_generation,
                        request_id.as_str()
                    ),
                    (SESSION, GENERATION, id)
                );
                assert!(!ack);
                ack = true;
            }
            F::Response {
                session_id,
                connection_generation,
                response: result,
                ..
            } => {
                assert!(ack);
                assert!(!response);
                assert_eq!(
                    (
                        session_id.as_str(),
                        connection_generation,
                        result.request_id.as_str()
                    ),
                    (SESSION, GENERATION, id)
                );
                assert_eq!(
                    result.status,
                    okx_ipc_contracts::AgentResponseStatus::Completed
                );
                assert!(result.result.unwrap()["data"]["instances"].is_array());
                response = true;
            }
            F::Ping { .. } => {}
            other => panic!("unexpected frame {other:?}"),
        }
    }
    assert!(s.native.feed.outbound_posts.lock().unwrap().is_empty());
    s.end(None).await;
}

#[tokio::test]
async fn native_session_rejects_foreign_session_and_generation() {
    for (id, generation) in [
        ("session_foreign_contract_001", GENERATION),
        (SESSION, GENERATION + 1),
    ] {
        let s = Session::new().await;
        s.send(F::Pong {
            schema: SCHEMA.into(),
            session_id: id.into(),
            connection_generation: generation,
            nonce: "session_test_nonce_001".into(),
        });
        s.end(Some("SESSION_GENERATION_MISMATCH")).await;
    }
}

#[tokio::test]
async fn native_session_rejects_bound_retired_financial_result() {
    use okx_ipc_contracts::risk_authority::{
        RISK_RESULT_SCHEMA_V1, RiskDecisionStatus, RiskResult,
    };
    let s = Session::new().await;
    s.send(F::RiskControlResult {
        schema: SCHEMA.into(),
        session_id: SESSION.into(),
        connection_generation: GENERATION,
        result: RiskResult {
            schema: RISK_RESULT_SCHEMA_V1.into(),
            command_id: "retired_finance_result_001".into(),
            status: RiskDecisionStatus::Denied,
            reason: Some("NATIVE_FINANCE_AUTHORITY_REQUIRED".into()),
            reservation_key: None,
            risk_book_sequence: None,
            policy_revision: None,
            existing_modelled_loss_usdt: None,
            reservation_state: None,
            budget: None,
            account_budget: None,
        },
    });
    s.end(Some("RETIRED_FINANCIAL_FRAME")).await;
}

#[tokio::test]
async fn native_session_applies_valid_policy_and_rejects_foreign_installation() {
    let s = Session::new().await;
    let mut policy = s.native.authority.clone();
    policy.state.revision += 1;
    policy.state.accounts[0].policy_revision = policy.state.revision;
    policy.state.accounts[0].explicit_pause = true;
    policy.session_generation = GENERATION;
    s.send(F::PolicySnapshot {
        schema: SCHEMA.into(),
        session_id: SESSION.into(),
        connection_generation: GENERATION,
        snapshot: policy.clone(),
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let observed = s
                .native
                .trade
                .diagnostic_access(&s.native.config.account_key.wire())
                .await;
            if observed["revision"] == policy.state.revision && observed["explicit_pause"] == true {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("valid policy must reach the real Native governance owner");
    let create = StrategyRequest::Create {
        idempotency_key: "session_policy_create_001".into(),
        instance_id: "session-policy-instance".into(),
        definition_id: "ema_cross_v1".into(),
        version: 1,
        parameters: serde_json::json!({"fast":10,"slow":20}),
        instruments: vec!["BTC-USDT-SWAP".into()],
        timeframe_ms: 60_000,
        budget_usd: "2".into(),
    };
    s.native
        .trade
        .strategies(&s.native.config.alias, &create, None)
        .await
        .unwrap();
    let sql = s
        .native
        .owner
        .account_policy(
            s.native.config.account_key.wire(),
            std::time::Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert_eq!(sql.revision as u64, policy.state.revision);
    assert!(sql.paused);
    policy.state.installation_id = "foreign-installation".into();
    policy.state.revision += 1;
    policy.state.accounts[0].policy_revision = policy.state.revision;
    s.send(F::PolicySnapshot {
        schema: SCHEMA.into(),
        session_id: SESSION.into(),
        connection_generation: GENERATION,
        snapshot: policy,
    });
    s.end(Some("INVALID_POLICY_SNAPSHOT")).await;
}

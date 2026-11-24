use futures_util::{SinkExt, StreamExt};
use okx_research_protocol::{analytics::MarketRequest, research_ipc::*};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_BYTES)
        .new_codec()
}
async fn send(w: &mut FramedWrite<ChildStdin, LengthDelimitedCodec>, f: HostFrame) {
    w.send(bytes::Bytes::from(serde_json::to_vec(&f).unwrap()))
        .await
        .unwrap();
}
async fn receive(r: &mut FramedRead<ChildStdout, LengthDelimitedCodec>) -> WorkerFrame {
    let bytes = tokio::time::timeout(Duration::from_secs(10), r.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}
async fn start(
    root: &Path,
    session: &WorkerSession,
    scope: &ResearchScope,
) -> (
    Child,
    FramedWrite<ChildStdin, LengthDelimitedCodec>,
    FramedRead<ChildStdout, LengthDelimitedCodec>,
) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_okx-research-service"))
        .arg("--root")
        .arg(root)
        .arg("--scope")
        .arg(scope.as_str())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = FramedWrite::new(child.stdin.take().unwrap(), codec());
    let mut output = FramedRead::new(child.stdout.take().unwrap(), codec());
    send(
        &mut input,
        HostFrame::Hello {
            protocol: PROTOCOL_VERSION,
            storage: STORAGE_VERSION,
            session: session.clone(),
            scope: scope.clone(),
        },
    )
    .await;
    assert!(
        matches!(receive(&mut output).await, WorkerFrame::Hello { session: s, .. } if s == *session)
    );
    (child, input, output)
}
#[tokio::test]
async fn child_round_trip_source_and_eof_restart() {
    let root = std::env::temp_dir().join(format!(
        "okx-ipc-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let session = WorkerSession::new("test-session".into()).unwrap();
    let scope = ResearchScope::new("test-scope".into()).unwrap();
    let (mut child, mut input, mut output) = start(&root, &session, &scope).await;
    for (number, command) in [
        (1, ResearchCommand::Capabilities),
        (
            2,
            ResearchCommand::Explain {
                job_id: JobId::new("missing".into()).unwrap(),
            },
        ),
    ] {
        let id = RequestId::new(format!("h{number}")).unwrap();
        send(
            &mut input,
            HostFrame::Request {
                invocation: None,
                id: id.clone(),
                session: session.clone(),
                command: Box::new(command),
            },
        )
        .await;
        assert!(
            matches!(receive(&mut output).await, WorkerFrame::Response { id: got, result: Ok(_), .. } if got == id)
        );
    }
    let request: MarketRequest = serde_json::from_value(serde_json::json!({"action":"screen","instruments":["BTC-USDT"],"fields":["rolling_mean"],"timeframe_ms":60000,"start_ms":0,"end_ms":6000000})).unwrap();
    let id = RequestId::new("h3".into()).unwrap();
    send(
        &mut input,
        HostFrame::Request {
            invocation: None,
            id: id.clone(),
            session: session.clone(),
            command: Box::new(ResearchCommand::Market {
                request,
                universe_page: None,
                resolved_instruments: None,
            }),
        },
    )
    .await;
    let mut source_seen = false;
    let mut last_source = None;
    loop {
        match receive(&mut output).await {
            WorkerFrame::SourceRequest { id, session: s, .. } => {
                source_seen = true;
                last_source = Some((id.clone(), s.clone()));
                send(
                    &mut input,
                    HostFrame::SourceReply {
                        id,
                        session: s,
                        result: Err("TEST_SOURCE_UNAVAILABLE".into()),
                    },
                )
                .await;
            }
            WorkerFrame::Response { id: got, .. } if got == id => break,
            _ => panic!("unexpected frame"),
        }
    }
    assert!(source_seen);
    let (id, s) = last_source.unwrap();
    send(
        &mut input,
        HostFrame::SourceReply {
            id,
            session: s,
            result: Err("LATE".into()),
        },
    )
    .await;
    send(
        &mut input,
        HostFrame::Request {
            invocation: None,
            id: RequestId::new("h4".into()).unwrap(),
            session: session.clone(),
            command: Box::new(ResearchCommand::Capabilities),
        },
    )
    .await;
    assert!(matches!(
        receive(&mut output).await,
        WorkerFrame::Response { result: Ok(_), .. }
    ));
    drop(input);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    let (mut child, input, _) = start(&root, &session, &scope).await;
    drop(input);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn strategy_outbox_publishes_once_and_recovers_committed_proposal_after_restart() {
    let root = test_root("strategy-outbox");
    let session = WorkerSession::new("strategy-session".into()).unwrap();
    let scope = ResearchScope::new("strategy-scope".into()).unwrap();
    let now = jiff::Timestamp::now().as_millisecond() as u64;
    let request = StrategySignalInput {
        instance_id: SignalInstanceId::new("test-instance".into()).unwrap(),
        definition_id: SignalDefinitionId::new("ema_cross_v1".into()).unwrap(),
        definition_version: SignalDefinitionVersion::new("1".into()).unwrap(),
        instrument: SignalInstrumentId::new("BTC-USDT-SWAP".into()).unwrap(),
        timeframe_ms: 60_000,
        available_at_ms: now,
        prices: SignalRequest {
            strategy_id: "ema_cross_v1".into(),
            parameters: okx_research_protocol::analytics::EmaParameters { fast: 1, slow: 2 },
            closes: (0..4)
                .map(|i| ClosedPrice {
                    time_ms: now - (4 - i) * 60_000,
                    close: 100.0 + i as f64,
                })
                .collect(),
        },
    };
    async fn poll_proposal(
        input: &mut FramedWrite<ChildStdin, LengthDelimitedCodec>,
        output: &mut FramedRead<ChildStdout, LengthDelimitedCodec>,
        session: &WorkerSession,
        number: u64,
        request: &StrategySignalInput,
    ) -> Option<StrategySignalProposal> {
        let id = RequestId::new(format!("h{number}")).unwrap();
        send(
            input,
            HostFrame::Request {
                id: id.clone(),
                session: session.clone(),
                invocation: None,
                command: Box::new(ResearchCommand::ProposeStrategySignal {
                    request: request.clone(),
                }),
            },
        )
        .await;
        match receive(output).await {
            WorkerFrame::Response {
                id: actual,
                result: Ok(WorkerReply::StrategyProposal(value)),
                ..
            } => {
                assert_eq!(actual, id);
                value
            }
            other => panic!("unexpected outbox reply: {other:?}"),
        }
    }
    let (mut child, mut input, mut output) = start(&root, &session, &scope).await;
    assert!(
        poll_proposal(&mut input, &mut output, &session, 1, &request)
            .await
            .is_none()
    );
    let proposal = tokio::time::timeout(Duration::from_secs(10), async {
        for number in 2.. {
            if let Some(proposal) =
                poll_proposal(&mut input, &mut output, &session, number, &request).await
            {
                return proposal;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        unreachable!()
    })
    .await
    .unwrap();
    assert_ne!(proposal.position, SignalPosition::Warmup);
    assert_eq!(proposal.last_closed_ms, now - 60_000);
    drop(input);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );

    let (mut child, mut input, mut output) = start(&root, &session, &scope).await;
    assert_eq!(
        poll_proposal(&mut input, &mut output, &session, 1, &request).await,
        Some(proposal)
    );
    drop(input);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    std::fs::remove_dir_all(root).unwrap();
}

use okx_research_protocol::analytics::ResearchRequest;
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;
fn test_root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "okx-ipc-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn raw_start(
    root: &Path,
    scope: &ResearchScope,
) -> (
    Child,
    FramedWrite<ChildStdin, LengthDelimitedCodec>,
    FramedRead<ChildStdout, LengthDelimitedCodec>,
) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_okx-research-service"))
        .arg("--root")
        .arg(root)
        .arg("--scope")
        .arg(scope.as_str())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let input = FramedWrite::new(child.stdin.take().unwrap(), codec());
    let output = FramedRead::new(child.stdout.take().unwrap(), codec());
    (child, input, output)
}
async fn failed(mut child: Child) {
    assert!(
        !tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
}
fn hello(session: &WorkerSession, scope: &ResearchScope) -> HostFrame {
    HostFrame::Hello {
        protocol: PROTOCOL_VERSION,
        storage: STORAGE_VERSION,
        session: session.clone(),
        scope: scope.clone(),
    }
}
#[tokio::test]
async fn incompatible_handshake_cannot_create_root() {
    let scope = ResearchScope::new("alice".into()).unwrap();
    let other = ResearchScope::new("bob".into()).unwrap();
    let session = WorkerSession::new("session".into()).unwrap();
    for variant in 0..3 {
        let root = test_root("bad-hello");
        let (child, mut input, _) = raw_start(&root, &scope);
        let mut message = hello(&session, &scope);
        if let HostFrame::Hello {
            protocol,
            storage,
            scope,
            ..
        } = &mut message
        {
            match variant {
                0 => *protocol += 1,
                1 => *storage += 1,
                _ => *scope = other.clone(),
            }
        }
        send(&mut input, message).await;
        failed(child).await;
        assert!(!root.exists());
    }
}
#[tokio::test]
async fn cross_scope_restart_rejected() {
    let root = test_root("cross-scope");
    let alice = ResearchScope::new("alice".into()).unwrap();
    let bob = ResearchScope::new("bob".into()).unwrap();
    let session = WorkerSession::new("session".into()).unwrap();
    let (mut child, input, _) = start(&root, &session, &alice).await;
    drop(input);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    let (child, mut input, _) = raw_start(&root, &bob);
    send(&mut input, hello(&session, &bob)).await;
    failed(child).await;
    let (mut child, input, _) = start(&root, &session, &alice).await;
    drop(input);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn replay_and_wrong_session_terminate_worker() {
    for replay in [true, false] {
        let root = test_root("bad-request");
        let scope = ResearchScope::new("scope".into()).unwrap();
        let session = WorkerSession::new("session".into()).unwrap();
        let (child, mut input, mut output) = start(&root, &session, &scope).await;
        let id = RequestId::new("h1".into()).unwrap();
        send(
            &mut input,
            HostFrame::Request {
                invocation: None,
                id: id.clone(),
                session: session.clone(),
                command: Box::new(ResearchCommand::Capabilities),
            },
        )
        .await;
        assert!(
            matches!(receive(&mut output).await, WorkerFrame::Response { id: got, .. } if got == id)
        );
        send(
            &mut input,
            HostFrame::Request {
                invocation: None,
                id: if replay {
                    id
                } else {
                    RequestId::new("h2".into()).unwrap()
                },
                session: if replay {
                    session
                } else {
                    WorkerSession::new("old-session".into()).unwrap()
                },
                command: Box::new(ResearchCommand::Capabilities),
            },
        )
        .await;
        failed(child).await;
        std::fs::remove_dir_all(root).unwrap();
    }
}
#[tokio::test]
async fn oversized_and_truncated_wire_terminate_worker() {
    for oversized in [true, false] {
        let root = test_root("bad-frame");
        let scope = ResearchScope::new("scope".into()).unwrap();
        let session = WorkerSession::new("session".into()).unwrap();
        let (child, input, _) = start(&root, &session, &scope).await;
        let mut raw = input.into_inner();
        if oversized {
            raw.write_all(&((MAX_FRAME_BYTES as u32) + 1).to_be_bytes())
                .await
                .unwrap();
        } else {
            raw.write_all(&[0, 0, 0, 10, b'x']).await.unwrap();
        }
        drop(raw);
        failed(child).await;
        std::fs::remove_dir_all(root).unwrap();
    }
}
fn backtest(fast: u64) -> ResearchRequest {
    serde_json::from_value(serde_json::json!({"action":"backtest","idempotency_key":"lost-ack-key","instruments":["BTC-USDT"],"timeframe_ms":60000,"start_ms":0,"end_ms":1200000,"strategy_id":"ema_momentum_confirm_v1","parameters":{"fast":fast,"slow":5},"costs":{"maker_fee_bps":0,"taker_fee_bps":0,"slippage_bps":0,"funding_mode":"zero_assumption"},"data_mode":"counterfactual","seed":7})).unwrap()
}
#[tokio::test]
async fn lost_enqueue_ack_retry_preserves_job_and_rejects_conflict() {
    let root = test_root("lost-ack");
    let scope = ResearchScope::new("scope".into()).unwrap();
    let session = WorkerSession::new("session-1".into()).unwrap();
    let (mut child, mut input, mut output) = start(&root, &session, &scope).await;
    send(
        &mut input,
        HostFrame::Request {
            invocation: None,
            id: RequestId::new("h1".into()).unwrap(),
            session: session.clone(),
            command: Box::new(ResearchCommand::Research {
                request: backtest(1),
            }),
        },
    )
    .await;
    // Ignore any enqueue response. A source request proves the job was persisted
    // and began execution; deliberately lose the response before killing the worker.
    loop {
        if matches!(
            receive(&mut output).await,
            WorkerFrame::SourceRequest { .. }
        ) {
            break;
        }
    }
    child.kill().await.unwrap();
    let _ = child.wait().await.unwrap();
    drop(input);
    drop(output);
    let db = rusqlite::Connection::open(root.join("research-catalog.sqlite")).unwrap();
    let durable_id: String = db
        .query_row(
            "SELECT id FROM research_jobs WHERE idempotency_key='lost-ack-key'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    drop(db);
    let next = WorkerSession::new("session-2".into()).unwrap();
    let (mut child, mut input, mut output) = start(&root, &next, &scope).await;
    // The old request's response was lost, but its durable state is not a
    // completed result. Observe the actual restarted child before retrying.
    send(
        &mut input,
        HostFrame::Request {
            invocation: None,
            id: RequestId::new("h1".into()).unwrap(),
            session: next.clone(),
            command: Box::new(ResearchCommand::Research {
                request: ResearchRequest::Status {
                    job_id: durable_id.clone(),
                },
            }),
        },
    )
    .await;
    assert!(matches!(receive(&mut output).await,
            WorkerFrame::Response { result: Ok(WorkerReply::Product(value)), .. }
            if value["id"] == durable_id && value["state"] == "interrupted"
                && value["error"] == "process restarted before completion"));
    send(
        &mut input,
        HostFrame::Request {
            invocation: None,
            id: RequestId::new("h2".into()).unwrap(),
            session: next.clone(),
            command: Box::new(ResearchCommand::Research {
                request: ResearchRequest::Result {
                    job_id: durable_id.clone(),
                    cursor: None,
                    limit: 1,
                    view: okx_research_protocol::analytics::ResearchResultView::Summary,
                },
            }),
        },
    )
    .await;
    assert!(matches!(receive(&mut output).await,
        WorkerFrame::Response { result: Err(error), .. } if error == "RESULT_NOT_READY"));
    send(
        &mut input,
        HostFrame::Request {
            invocation: None,
            id: RequestId::new("h3".into()).unwrap(),
            session: next.clone(),
            command: Box::new(ResearchCommand::Research {
                request: backtest(1),
            }),
        },
    )
    .await;
    match receive(&mut output).await {
        WorkerFrame::Response {
            result: Ok(WorkerReply::Product(value)),
            ..
        } => {
            assert_eq!(value["id"].as_str(), Some(durable_id.as_str()));
            assert_eq!(value["state"], "interrupted");
            assert!(value["artifact"].is_null());
        }
        _ => panic!("missing retry response"),
    }
    send(
        &mut input,
        HostFrame::Request {
            invocation: None,
            id: RequestId::new("h4".into()).unwrap(),
            session: next,
            command: Box::new(ResearchCommand::Research {
                request: backtest(2),
            }),
        },
    )
    .await;
    assert!(
        matches!(receive(&mut output).await, WorkerFrame::Response { result: Err(error), .. } if error == "IDEMPOTENCY_CONFLICT")
    );
    drop(input);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn draining_preserves_active_job_and_rejects_new_admission() {
    let root = test_root("drain");
    let scope = ResearchScope::new("scope".into()).unwrap();
    let session = WorkerSession::new("session".into()).unwrap();
    let (mut child, mut input, mut output) = start(&root, &session, &scope).await;
    send(
        &mut input,
        HostFrame::Request {
            invocation: None,
            id: RequestId::new("h1".into()).unwrap(),
            session: session.clone(),
            command: Box::new(ResearchCommand::Research {
                request: backtest(1),
            }),
        },
    )
    .await;
    let mut source = None;
    let mut job = None;
    while source.is_none() || job.is_none() {
        match receive(&mut output).await {
            WorkerFrame::SourceRequest { id, session, .. } => source = Some((id, session)),
            WorkerFrame::Response {
                result: Ok(WorkerReply::Product(value)),
                ..
            } => job = value["id"].as_str().map(str::to_owned),
            _ => panic!("unexpected enqueue frame"),
        }
    }
    send(
        &mut input,
        HostFrame::Request {
            invocation: None,
            id: RequestId::new("h2".into()).unwrap(),
            session: session.clone(),
            command: Box::new(ResearchCommand::Drain),
        },
    )
    .await;
    assert!(
        matches!(receive(&mut output).await, WorkerFrame::Response { result: Ok(WorkerReply::Draining(v)), .. } if v.active_jobs == 1)
    );
    let mut next = backtest(1);
    if let ResearchRequest::Backtest {
        idempotency_key, ..
    } = &mut next
    {
        *idempotency_key = "new-key".into();
    }
    send(
        &mut input,
        HostFrame::Request {
            invocation: None,
            id: RequestId::new("h3".into()).unwrap(),
            session: session.clone(),
            command: Box::new(ResearchCommand::Research { request: next }),
        },
    )
    .await;
    assert!(
        matches!(receive(&mut output).await, WorkerFrame::Response { result: Err(error), .. } if error == "RESEARCH_UPDATING")
    );
    let (id, s) = source.unwrap();
    send(
        &mut input,
        HostFrame::SourceReply {
            id,
            session: s,
            result: Err("SOURCE_UNAVAILABLE".into()),
        },
    )
    .await;
    let wait = tokio::time::timeout(Duration::from_secs(10), async {
        for number in 4..10000 {
            let id = RequestId::new(format!("h{number}")).unwrap();
            send(
                &mut input,
                HostFrame::Request {
                    invocation: None,
                    id: id.clone(),
                    session: session.clone(),
                    command: Box::new(ResearchCommand::Drain),
                },
            )
            .await;
            loop {
                match receive(&mut output).await {
                    WorkerFrame::Response {
                        id: got,
                        result: Ok(WorkerReply::Draining(v)),
                        ..
                    } if got == id && v.active_jobs == 0 => return,
                    WorkerFrame::Response {
                        id: got,
                        result: Ok(_),
                        ..
                    } if got == id => {
                        tokio::task::yield_now().await;
                        break;
                    }
                    WorkerFrame::SourceRequest { id, session, .. } => {
                        send(
                            &mut input,
                            HostFrame::SourceReply {
                                id,
                                session,
                                result: Err("SOURCE_UNAVAILABLE".into()),
                            },
                        )
                        .await
                    }
                    _ => panic!("unexpected drain frame"),
                }
            }
        }
        panic!("job never drained")
    })
    .await;
    wait.unwrap();
    drop(input);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    std::fs::remove_dir_all(root).unwrap();
}

/// T11/T12: actual child process and actual framed IPC. Thirty-two public
/// validation requests wait on deliberately unanswered source RPCs. The host
/// still must be able to obtain status/capabilities/explain and cancel/drain.
/// No Agent engine, exchange HTTP, finance owner, or order is created here.
#[tokio::test]
async fn t11_t12_real_worker_observation_and_cancel_survive_saturated_work_lane() {
    use std::time::Instant;

    let root = test_root("t11-t12-lane-saturation");
    let scope = ResearchScope::new("isolation".into()).unwrap();
    let session = WorkerSession::new("isolation-session".into()).unwrap();
    let (mut child, mut input, mut output) = start(&root, &session, &scope).await;
    let validation: ResearchRequest = serde_json::from_value(serde_json::json!({
        "action":"validate","instruments":["BTC-USDT"],
        "timeframe_ms":60000,"start_ms":0,"end_ms":12000000,
        "strategy_id":"ema_cross_v1","parameters":{"fast":2,"slow":5},
        "costs":{"maker_fee_bps":0,"taker_fee_bps":0,"slippage_bps":0,
            "funding_mode":"zero_assumption"},
        "data_mode":"counterfactual"
    }))
    .unwrap();
    for number in 1..=MAX_IN_FLIGHT + 1 {
        send(
            &mut input,
            HostFrame::Request {
                invocation: None,
                id: RequestId::new(format!("h{number}")).unwrap(),
                session: session.clone(),
                command: Box::new(ResearchCommand::Research {
                    request: validation.clone(),
                }),
            },
        )
        .await;
    }

    let mut source_requests = 0usize;
    let overflow = RequestId::new(format!("h{}", MAX_IN_FLIGHT + 1)).unwrap();
    tokio::time::timeout(Duration::from_secs(4), async {
        let mut saw_overflow = false;
        // The control response can be written before the scheduled source RPCs.
        while !saw_overflow || source_requests == 0 {
            match receive(&mut output).await {
                WorkerFrame::SourceRequest { .. } => source_requests += 1,
                WorkerFrame::Response {
                    id,
                    result: Err(error),
                    ..
                } if id == overflow => {
                    assert_eq!(error, "RESEARCH_BUSY", "main lane did not saturate");
                    saw_overflow = true;
                }
                other => panic!("work completed while synthetic source was held: {other:?}"),
            }
        }
    })
    .await
    .expect("expected both a source RPC and a rejected overflow request");
    // Public status/cancel validate a full canonical SHA-shaped job ID before
    // querying SQLite. Use a well-formed but absent ID to exercise that path.
    let unknown_job = "f".repeat(64);

    let mut latencies_ms = Vec::new();
    for step in 0..100usize {
        let number = MAX_IN_FLIGHT + 2 + step;
        let id = RequestId::new(format!("h{number}")).unwrap();
        let command = match step % 3 {
            0 => ResearchCommand::Capabilities,
            1 => ResearchCommand::Explain {
                job_id: JobId::new(unknown_job.clone()).unwrap(),
            },
            _ => ResearchCommand::Research {
                request: ResearchRequest::Status {
                    job_id: unknown_job.clone(),
                },
            },
        };
        assert_eq!(command.lane(), ResearchLane::Observation);
        let began = Instant::now();
        send(
            &mut input,
            HostFrame::Request {
                invocation: None,
                id: id.clone(),
                session: session.clone(),
                command: Box::new(command),
            },
        )
        .await;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match receive(&mut output).await {
                    WorkerFrame::SourceRequest { .. } => source_requests += 1,
                    WorkerFrame::Response {
                        id: returned,
                        result,
                        ..
                    } if returned == id => {
                        if step % 3 == 2 {
                            assert!(matches!(result, Err(ref e) if e == "JOB_NOT_FOUND"));
                        } else {
                            assert!(matches!(result, Ok(WorkerReply::Product(_))));
                        }
                        break;
                    }
                    other => panic!("unrelated response in saturated IPC test: {other:?}"),
                }
            }
        })
        .await
        .expect("observation lane blocked by heavy work");
        latencies_ms.push(began.elapsed().as_secs_f64() * 1000.0);
    }

    // Emergency controls have their own two slots, independent of both the
    // saturated work lane and four read-only observation slots.
    for (number, command) in [
        ResearchCommand::Research {
            request: ResearchRequest::Cancel {
                job_id: unknown_job,
                idempotency_key: "cancel-under-load".into(),
            },
        },
        ResearchCommand::Drain,
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(command.lane(), ResearchLane::Critical);
        let id = RequestId::new(format!("h{}", MAX_IN_FLIGHT + 102 + number)).unwrap();
        let began = Instant::now();
        send(
            &mut input,
            HostFrame::Request {
                invocation: None,
                id: id.clone(),
                session: session.clone(),
                command: Box::new(command),
            },
        )
        .await;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match receive(&mut output).await {
                    WorkerFrame::SourceRequest { .. } => source_requests += 1,
                    WorkerFrame::Response {
                        id: got, result, ..
                    } if got == id => {
                        if number == 0 {
                            assert!(matches!(result, Err(ref error) if error == "JOB_NOT_FOUND"));
                        } else {
                            assert!(matches!(result, Ok(WorkerReply::Draining(_))));
                        }
                        break;
                    }
                    other => panic!("unexpected control reply: {other:?}"),
                }
            }
        })
        .await
        .expect("critical lane blocked");
        latencies_ms.push(began.elapsed().as_secs_f64() * 1000.0);
    }
    let mut sorted = latencies_ms.clone();
    sorted.sort_by(f64::total_cmp);
    assert!(
        sorted[96] <= 100.0,
        "T11/T12 warm IPC read-only/control p95 exceeded 100ms: {}",
        sorted[96]
    );
    println!(
        "T11_T12_IPC_ISOLATION_JSON:{}",
        serde_json::json!({
            "schema":"okx.518.t11-t12-ipc-isolation/v1",
            "work_requests_held":MAX_IN_FLIGHT,
            "work_lane_overflow":"RESEARCH_BUSY",
            "observations":100,"critical_commands":2,
            "warm_p95_ms":sorted[96],"warm_max_ms":sorted[101],
            "source_rpc_seen":source_requests,
            "source_policy":"no_reply_no_exchange_http",
            "engine_or_finance_under_load":"not_evaluated",
            "venue_posts":0,"finance_writes":0,
        })
    );

    send(
        &mut input,
        HostFrame::Request {
            invocation: None,
            id: RequestId::new(format!("h{}", MAX_IN_FLIGHT + 104)).unwrap(),
            session: session.clone(),
            command: Box::new(ResearchCommand::Shutdown),
        },
    )
    .await;
    assert!(matches!(
        receive(&mut output).await,
        WorkerFrame::Response {
            result: Ok(WorkerReply::Shutdown),
            ..
        }
    ));
    drop(input);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    std::fs::remove_dir_all(root).unwrap();
}

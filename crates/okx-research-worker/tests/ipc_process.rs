use futures_util::{SinkExt, StreamExt};
use okx_protocol::{analytics::MarketRequest, research_ipc::*};
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
    let mut child = Command::new(env!("CARGO_BIN_EXE_okx-research-worker"))
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

use okx_protocol::analytics::ResearchRequest;
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
    let mut child = Command::new(env!("CARGO_BIN_EXE_okx-research-worker"))
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
    send(
        &mut input,
        HostFrame::Request {
            invocation: None,
            id: RequestId::new("h1".into()).unwrap(),
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
        } => assert_eq!(value["id"].as_str(), Some(durable_id.as_str())),
        _ => panic!("missing retry response"),
    }
    send(
        &mut input,
        HostFrame::Request {
            invocation: None,
            id: RequestId::new("h2".into()).unwrap(),
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

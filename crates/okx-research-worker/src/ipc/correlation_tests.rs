use super::*;
use std::io::{self, Write};

#[derive(Clone, Default)]
struct Capture {
    bytes: Arc<std::sync::Mutex<Vec<u8>>>,
    terminal: Arc<tokio::sync::Notify>,
}
impl Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut output = self.bytes.lock().unwrap();
        let remaining = 65536usize.saturating_sub(output.len());
        output.extend_from_slice(&bytes[..bytes.len().min(remaining)]);
        if output
            .windows(b"research job ended".len())
            .any(|window| window == b"research job ended")
        {
            self.terminal.notify_one();
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
    type Writer = Capture;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

async fn send(
    w: &mut FramedWrite<tokio::io::WriteHalf<tokio::io::DuplexStream>, LengthDelimitedCodec>,
    frame: HostFrame,
) {
    w.send(bytes::Bytes::from(serde_json::to_vec(&frame).unwrap()))
        .await
        .unwrap();
}
async fn receive(
    r: &mut FramedRead<tokio::io::ReadHalf<tokio::io::DuplexStream>, LengthDelimitedCodec>,
) -> WorkerFrame {
    let bytes = tokio::time::timeout(Duration::from_secs(5), r.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn failed_backtest_correlates_real_ipc_job_and_safe_trace() {
    let capture = Capture::default();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(capture.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let root = std::env::temp_dir().join(format!(
        "okx-ipc-correlation-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    ));
    let scope = ResearchScope::new("trace-scope".into()).unwrap();
    let session = WorkerSession::new("trace-session".into()).unwrap();
    let invocation = InvocationId::new("trace-invocation".into()).unwrap();
    let (host, worker) = tokio::io::duplex(1024 * 1024);
    let (worker_read, worker_write) = tokio::io::split(worker);
    let task = tokio::spawn(serve(
        worker_read,
        worker_write,
        root.clone(),
        scope.clone(),
    ));
    let (host_read, host_write) = tokio::io::split(host);
    let mut writer = FramedWrite::new(host_write, codec());
    let mut reader = FramedRead::new(host_read, codec());
    send(
        &mut writer,
        HostFrame::Hello {
            protocol: PROTOCOL_VERSION,
            storage: STORAGE_VERSION,
            session: session.clone(),
            scope,
        },
    )
    .await;
    let binary = match receive(&mut reader).await {
        WorkerFrame::Hello {
            producer_sha256, ..
        } => producer_sha256,
        other => panic!("unexpected hello: {other:?}"),
    };
    let request = serde_json::from_value(serde_json::json!({"action":"backtest","idempotency_key":"secret-idempotency-value","instruments":["BTC-USDT"],"timeframe_ms":60000,"start_ms":0,"end_ms":1200000,"strategy_id":"ema_momentum_confirm_v1","parameters":{"fast":1,"slow":5},"costs":{"maker_fee_bps":0,"taker_fee_bps":0,"slippage_bps":0,"funding_mode":"zero_assumption"},"data_mode":"counterfactual","seed":7})).unwrap();
    send(
        &mut writer,
        HostFrame::Request {
            id: RequestId::new("h1".into()).unwrap(),
            session: session.clone(),
            invocation: Some(invocation.clone()),
            command: Box::new(ResearchCommand::Research { request }),
        },
    )
    .await;
    let mut job_id = None;
    let mut source_seen = false;
    while job_id.is_none() || !source_seen {
        match receive(&mut reader).await {
            WorkerFrame::Response {
                id,
                result: Ok(WorkerReply::Product(value)),
                ..
            } if id.as_str() == "h1" => job_id = Some(value["id"].as_str().unwrap().to_owned()),
            WorkerFrame::SourceRequest { id, session, .. } => {
                source_seen = true;
                send(
                    &mut writer,
                    HostFrame::SourceReply {
                        id,
                        session,
                        result: Err("SOURCE_INVALID".into()),
                    },
                )
                .await;
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }
    let job_id = job_id.unwrap();
    tokio::time::timeout(Duration::from_secs(5), capture.terminal.notified())
        .await
        .expect("missing terminal event");
    send(
        &mut writer,
        HostFrame::Request {
            id: RequestId::new("h2".into()).unwrap(),
            session: session.clone(),
            invocation: None,
            command: Box::new(ResearchCommand::Explain {
                job_id: JobId::new(job_id.clone()).unwrap(),
            }),
        },
    )
    .await;
    let failed = match receive(&mut reader).await {
        WorkerFrame::Response {
            result: Ok(WorkerReply::Product(value)),
            ..
        } => value,
        other => panic!("unexpected explain: {other:?}"),
    };
    assert_eq!(failed["job"]["state"], "failed");
    assert_eq!(failed["job"]["error_code"], "SOURCE_INVALID");
    assert_eq!(failed["job"]["origin_invocation"], invocation.as_str());
    assert_eq!(failed["job"]["producer"]["binary_sha256"], binary);
    let output = String::from_utf8(capture.bytes.lock().unwrap().clone()).unwrap();
    let terminal = output
        .lines()
        .find(|line| line.contains("stage=\"terminal\"") && line.contains(&job_id))
        .expect("missing correlated terminal event");
    for field in ["trace-invocation", "trace-session", "h1", "SOURCE_INVALID"] {
        assert!(terminal.contains(field), "missing {field} in {terminal}");
    }
    assert!(!output.contains("secret-idempotency-value"));
    send(
        &mut writer,
        HostFrame::Request {
            id: RequestId::new("h3".into()).unwrap(),
            session,
            invocation: None,
            command: Box::new(ResearchCommand::Shutdown),
        },
    )
    .await;
    assert!(matches!(
        receive(&mut reader).await,
        WorkerFrame::Response {
            result: Ok(WorkerReply::Shutdown),
            ..
        }
    ));
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("worker must stop after shutdown")
        .unwrap()
        .unwrap();
    drop(writer);
    drop(reader);
    std::fs::remove_dir_all(root).unwrap();
}

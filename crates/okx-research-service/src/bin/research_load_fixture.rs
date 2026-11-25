//! Non-shipping, independently owned T10 load generator. Finance tests speak
//! bounded JSONL to this fixture, never import Research's Rust protocol and
//! never create a second implementation of Research. All work executes in the
//! actual `okx-research-service` child over its typed production IPC.

#[cfg(not(debug_assertions))]
compile_error!("okx-research-load-fixture must never be built in release mode");

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use okx_research_protocol::{
    analytics::{EmaParameters, FundingMode, ResearchCosts, ResearchDataMode, ResearchRequest},
    research_ipc::{
        HistoryCandle, HostFrame, JobId, MAX_FRAME_BYTES, PROTOCOL_VERSION, PublicCandleCapture,
        PublicSourceReply, PublicSourceRequest, RequestId, ResearchCommand, ResearchScope,
        STORAGE_VERSION, WorkerFrame, WorkerReply, WorkerSession,
    },
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::{mpsc, oneshot},
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

const SCHEMA: &str = "okx.research-load-fixture/v1";
const MAX_JSONL_COMMAND: usize = 4096;
const MAX_JSONL_REPLY: usize = 32 * 1024;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureCommand {
    schema: String,
    id: u64,
    action: String,
    #[serde(default)]
    job_id: Option<String>,
}

enum DriverCommand {
    Call(
        ResearchCommand,
        oneshot::Sender<Result<WorkerReply, String>>,
    ),
    Disconnect(oneshot::Sender<Result<(), String>>),
}

#[derive(Default)]
struct SourceStats {
    successful_requests: std::sync::atomic::AtomicU64,
    returned_rows: std::sync::atomic::AtomicU64,
}

struct WorkerClient {
    commands: mpsc::Sender<DriverCommand>,
    task: tokio::task::JoinHandle<()>,
    pid: u32,
}

fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_BYTES)
        .new_codec()
}

fn synthetic_candles(
    request: PublicSourceRequest,
    stats: &SourceStats,
) -> Result<PublicSourceReply, String> {
    let PublicSourceRequest::Candles { after, .. } = request else {
        return Err("UNEXPECTED_SOURCE".into());
    };
    let cursor = after
        .as_deref()
        .ok_or("CURSOR_MISSING")?
        .parse::<u64>()
        .map_err(|_| "INVALID_SOURCE_CURSOR")?;
    let upper = (cursor / 60_000).min(60_000);
    let lower = upper.saturating_sub(100);
    let rows = (lower..upper)
        .rev()
        .map(|index| HistoryCandle {
            open_time_ms: (index * 60_000).to_string(),
            open: "100".into(),
            high: "102".into(),
            low: "99".into(),
            close: if index % 2 == 0 { "100" } else { "101" }.into(),
            volume: "1".into(),
            volume_currency: "1".into(),
            volume_quote: None,
            confirmed: true,
        })
        .collect::<Vec<_>>();
    stats
        .returned_rows
        .fetch_add(rows.len() as u64, std::sync::atomic::Ordering::Relaxed);
    stats
        .successful_requests
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(PublicSourceReply::Candles(PublicCandleCapture {
        rows,
        raw_body: b"synthetic closed candles".to_vec(),
        acquired_at_ms: "1".into(),
        http_delivery: None,
    }))
}

impl WorkerClient {
    async fn spawn(
        root: &PathBuf,
        worker: &PathBuf,
        stats: Arc<SourceStats>,
    ) -> Result<Self, String> {
        let scope = ResearchScope::new("engine-heavy".into()).map_err(str::to_owned)?;
        let session = WorkerSession::new(format!("load-fixture-{}", std::process::id()))
            .map_err(str::to_owned)?;
        let namespace = format!("{:x}", Sha256::digest(root.to_string_lossy().as_bytes()));
        let mut child = Command::new(worker)
            .arg("--root")
            .arg(root)
            .arg("--scope")
            .arg(scope.as_str())
            .env("OKX_RESEARCH_TEST_SINGLETON_NAMESPACE", namespace)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| "RESEARCH_CHILD_SPAWN_FAILED")?;
        let pid = child.id().ok_or("RESEARCH_CHILD_PID_MISSING")?;
        let mut input = FramedWrite::new(child.stdin.take().unwrap(), codec());
        let mut output = FramedRead::new(child.stdout.take().unwrap(), codec());
        input
            .send(Bytes::from(
                serde_json::to_vec(&HostFrame::Hello {
                    protocol: PROTOCOL_VERSION,
                    storage: STORAGE_VERSION,
                    session: session.clone(),
                    scope: scope.clone(),
                })
                .map_err(|_| "RESEARCH_FRAME_INVALID")?,
            ))
            .await
            .map_err(|_| "RESEARCH_HELLO_WRITE_FAILED")?;
        let hello = tokio::time::timeout(Duration::from_secs(10), output.next())
            .await
            .map_err(|_| "RESEARCH_HELLO_TIMEOUT")?
            .ok_or("RESEARCH_HELLO_EOF")?
            .map_err(|_| "RESEARCH_HELLO_FRAME_INVALID")?;
        if !matches!(
            serde_json::from_slice::<WorkerFrame>(&hello),
            Ok(WorkerFrame::Hello {
                protocol: PROTOCOL_VERSION,
                storage: STORAGE_VERSION,
                session: actual,
                scope: actual_scope,
                ..
            }) if actual == session && actual_scope == scope
        ) {
            return Err("RESEARCH_HELLO_MISMATCH".into());
        }
        let (commands, mut receiver) = mpsc::channel(32);
        let task = tokio::spawn(async move {
            let mut next = 0_u64;
            let mut pending =
                HashMap::<String, oneshot::Sender<Result<WorkerReply, String>>>::new();
            loop {
                tokio::select! {
                    command = receiver.recv() => {
                        match command {
                            Some(DriverCommand::Call(command, reply)) => {
                                next += 1;
                                let id = RequestId::new(format!("h{next}")).unwrap();
                                let wire = HostFrame::Request {
                                    invocation: None,
                                    id: id.clone(),
                                    session: session.clone(),
                                    command: Box::new(command),
                                };
                                let ok = match serde_json::to_vec(&wire) {
                                    Ok(bytes) => input.send(Bytes::from(bytes)).await.is_ok(),
                                    Err(_) => false,
                                };
                                if ok {
                                    pending.insert(id.as_str().to_owned(), reply);
                                } else {
                                    let _ = reply.send(Err("RESEARCH_IPC_WRITE_FAILED".into()));
                                    break;
                                }
                            }
                            Some(DriverCommand::Disconnect(reply)) => {
                                let result = match child.kill().await {
                                    Ok(()) => child.wait().await
                                        .map(|_| ())
                                        .map_err(|_| "RESEARCH_CHILD_REAP_FAILED".to_owned()),
                                    Err(_) => Err("RESEARCH_CHILD_KILL_FAILED".into()),
                                };
                                let _ = reply.send(result);
                                break;
                            }
                            None => break,
                        }
                    }
                    message = output.next() => {
                        let Some(Ok(bytes)) = message else { break };
                        match serde_json::from_slice::<WorkerFrame>(&bytes) {
                            Ok(WorkerFrame::SourceRequest { id, session: actual, request })
                                if actual == session => {
                                let result = synthetic_candles(request, &stats);
                                let wire = HostFrame::SourceReply {
                                    id, session: session.clone(), result,
                                };
                                let bytes = match serde_json::to_vec(&wire) {
                                    Ok(bytes) => bytes,
                                    Err(_) => break,
                                };
                                if input.send(Bytes::from(bytes)).await.is_err() {
                                    break;
                                }
                            }
                            Ok(WorkerFrame::Response { id, session: actual, result })
                                if actual == session => {
                                if let Some(reply) = pending.remove(id.as_str()) {
                                    let _ = reply.send(result);
                                } else { break }
                            }
                            _ => break,
                        }
                    }
                }
            }
            for (_, reply) in pending {
                let _ = reply.send(Err("RESEARCH_WORKER_DISCONNECTED".into()));
            }
            let _ = child.kill().await;
            let _ = child.wait().await;
        });
        Ok(Self {
            commands,
            task,
            pid,
        })
    }

    async fn call(&self, command: ResearchCommand) -> Result<WorkerReply, String> {
        let (sender, receiver) = oneshot::channel();
        self.commands
            .send(DriverCommand::Call(command, sender))
            .await
            .map_err(|_| "RESEARCH_WORKER_OFFLINE")?;
        tokio::time::timeout(Duration::from_secs(10), receiver)
            .await
            .map_err(|_| "RESEARCH_REPLY_TIMEOUT")?
            .map_err(|_| "RESEARCH_WORKER_OFFLINE".to_owned())?
    }

    async fn product(&self, command: ResearchCommand) -> Result<Value, String> {
        match self.call(command).await? {
            WorkerReply::Product(value) => Ok(value),
            _ => Err("RESEARCH_RESPONSE_KIND_INVALID".into()),
        }
    }

    async fn disconnect(self) -> Result<(), String> {
        let (sender, receiver) = oneshot::channel();
        self.commands
            .send(DriverCommand::Disconnect(sender))
            .await
            .map_err(|_| "RESEARCH_WORKER_OFFLINE")?;
        let result = tokio::time::timeout(Duration::from_secs(10), receiver)
            .await
            .map_err(|_| "RESEARCH_STOP_TIMEOUT")?
            .map_err(|_| "RESEARCH_WORKER_OFFLINE".to_owned())?;
        self.task.await.map_err(|_| "RESEARCH_DRIVER_JOIN_FAILED")?;
        result
    }
}

fn boundary_grid(key: &str, instruments: usize, bars: u64, variants: usize) -> ResearchRequest {
    ResearchRequest::Grid {
        idempotency_key: key.into(),
        instruments: (0..instruments).map(|i| format!("TEST-{i}")).collect(),
        timeframe_ms: 60_000,
        start_ms: 0,
        end_ms: 60_000 * bars,
        strategy_id: "ema_cross_v1".into(),
        variants: (0..variants)
            .map(|i| {
                EmaParameters {
                    fast: 2,
                    slow: 90 + i,
                }
                .into()
            })
            .collect(),
        costs: ResearchCosts {
            maker_fee_bps: 1.0,
            taker_fee_bps: 2.0,
            slippage_bps: 1.0,
            funding_mode: FundingMode::ZeroAssumption,
        },
        data_mode: ResearchDataMode::Counterfactual,
        seed: 7,
    }
}

fn heavy_grid(key: &str) -> ResearchRequest {
    ResearchRequest::Grid {
        idempotency_key: key.into(),
        instruments: vec!["BTC-USDT".into()],
        timeframe_ms: 60_000,
        start_ms: 0,
        end_ms: 60_000 * 60_000,
        strategy_id: "ema_cross_v1".into(),
        variants: (3..67)
            .map(|slow| EmaParameters { fast: 2, slow }.into())
            .collect(),
        costs: ResearchCosts {
            maker_fee_bps: 1.0,
            taker_fee_bps: 2.0,
            slippage_bps: 1.0,
            funding_mode: FundingMode::ZeroAssumption,
        },
        data_mode: ResearchDataMode::Counterfactual,
        seed: 7,
    }
}

struct Fixture {
    root: PathBuf,
    worker: PathBuf,
    client: Option<WorkerClient>,
    stats: Arc<SourceStats>,
    started: bool,
}

impl Fixture {
    async fn open(root: PathBuf, worker: PathBuf) -> Result<Self, String> {
        if !root.is_absolute() || !worker.is_absolute() || !worker.is_file() {
            return Err("INVALID_FIXTURE_PATH".into());
        }
        // On Windows the loader must never accidentally attach the shipping
        // singleton binary. The sole accepted worker is the independently
        // built, opt-in debug test executable with its isolated mutex feature.
        if cfg!(windows)
            && worker.file_name().and_then(|name| name.to_str())
                != Some("okx-research-service-test-isolated.exe")
        {
            return Err("RESEARCH_TEST_WORKER_REQUIRED".into());
        }
        if let Ok(metadata) = std::fs::symlink_metadata(&root) {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err("FIXTURE_ROOT_NOT_DISPOSABLE".into());
            }
        }
        std::fs::create_dir_all(&root).map_err(|_| "FIXTURE_ROOT_UNAVAILABLE")?;
        if std::fs::read_dir(&root)
            .map_err(|_| "FIXTURE_ROOT_UNAVAILABLE")?
            .next()
            .is_some()
        {
            return Err("FIXTURE_ROOT_NOT_EMPTY".into());
        }
        let stats = Arc::new(SourceStats::default());
        let client = WorkerClient::spawn(&root, &worker, stats.clone()).await?;
        Ok(Self {
            root,
            worker,
            client: Some(client),
            stats,
            started: false,
        })
    }

    async fn call(&self, command: ResearchCommand) -> Result<Value, String> {
        self.client
            .as_ref()
            .ok_or("RESEARCH_WORKER_OFFLINE")?
            .product(command)
            .await
    }

    async fn status(&self, job_id: &str) -> Result<Value, String> {
        self.call(ResearchCommand::Research {
            request: ResearchRequest::Status {
                job_id: job_id.into(),
            },
        })
        .await
    }

    async fn explain(&self, job_id: &str) -> Result<Value, String> {
        let job_id = JobId::new(job_id.to_owned()).map_err(str::to_owned)?;
        self.call(ResearchCommand::Explain { job_id }).await
    }

    fn counts(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering;
        (
            self.stats.successful_requests.load(Ordering::SeqCst),
            self.stats.returned_rows.load(Ordering::SeqCst),
        )
    }

    async fn start(&mut self) -> Result<Value, String> {
        if self.started {
            return Err("RESEARCH_LOAD_ALREADY_STARTED".into());
        }
        let (before_requests, before_rows) = self.counts();
        if before_requests != 0 || before_rows != 0 {
            return Err("RESEARCH_BOUNDARY_SOURCE_PREMATURE".into());
        }
        for request in [
            boundary_grid("t10-max-bars", 1, 1_000_000, 64),
            boundary_grid("t10-max-eight", 8, 125_000, 64),
        ] {
            if !request.valid() {
                return Err("RESEARCH_VALID_BOUNDARY_REJECTED".into());
            }
        }
        let mut rejected = serde_json::Map::new();
        for (label, request) in [
            (
                "one_million_plus_one_bars",
                boundary_grid("t10-over-bars", 1, 1_000_001, 64),
            ),
            (
                "nine_instruments",
                boundary_grid("t10-over-instruments", 9, 125_000, 64),
            ),
            (
                "sixty_five_variants",
                boundary_grid("t10-over-variants", 8, 125_000, 65),
            ),
        ] {
            if request.valid() {
                return Err("RESEARCH_INVALID_BOUNDARY_ADMITTED".into());
            }
            let error = self
                .client
                .as_ref()
                .unwrap()
                .call(ResearchCommand::Research { request })
                .await
                .err()
                .ok_or("RESEARCH_INVALID_BOUNDARY_EXECUTED")?;
            if error != "INVALID_RESEARCH_REQUEST" {
                return Err("RESEARCH_BOUNDARY_ERROR_DRIFT".into());
            }
            rejected.insert(label.into(), json!(error));
        }
        let (after_requests, after_rows) = self.counts();
        if (after_requests, after_rows) != (before_requests, before_rows) {
            return Err("RESEARCH_BOUNDARY_FETCHED_SOURCE".into());
        }
        let boundary_evidence = json!({
            "schema":"okx.518.t10-real-child-pre-source-boundaries/v1",
            "route":"research_owned_fixture_to_worker_ipc",
            "admission_only_not_executed":["one_million_bars_one_instrument_64","eight_instruments_one_million_bars_64"],
            "rejected_before_source":rejected,
            "source_requests_before":before_requests,
            "source_requests_after":after_requests,
            "returned_rows_before":before_rows,
            "returned_rows_after":after_rows,
            "full_scale_execution":"not_attempted","side_effects":"none"
        });
        let first_enqueue = self
            .call(ResearchCommand::Research {
                request: heavy_grid("heavy-one"),
            })
            .await?;
        let first = first_enqueue["id"]
            .as_str()
            .ok_or("RESEARCH_FIRST_JOB_ID_MISSING")?
            .to_owned();
        let second_enqueue = self
            .call(ResearchCommand::Research {
                request: heavy_grid("heavy-two"),
            })
            .await?;
        let second = second_enqueue["id"]
            .as_str()
            .ok_or("RESEARCH_SECOND_JOB_ID_MISSING")?
            .to_owned();
        if first == second {
            return Err("RESEARCH_GRID_IDENTITY_COLLISION".into());
        }
        let (first_status, second_status) = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let left = self.status(&first).await?;
                let right = self.status(&second).await?;
                let valid = |value: &Value| {
                    value["state"] == "running"
                        && value["candidate_progress"]["completed_candidates"]
                            .as_u64()
                            .is_some_and(|count| count > 0)
                };
                if valid(&left) && valid(&right) && self.counts().0 > 0 {
                    return Ok((left, right));
                }
                if [left["state"].as_str(), right["state"].as_str()]
                    .iter()
                    .any(|value| matches!(value, Some("completed" | "failed" | "cancelled")))
                {
                    return Err::<(Value, Value), String>(
                        "RESEARCH_LOAD_FINISHED_BEFORE_FINANCE".into(),
                    );
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| "RESEARCH_LOAD_START_TIMEOUT")??;
        self.started = true;
        Ok(json!({
            "first":first, "second":second,
            "first_status":first_status, "second_status":second_status,
            "worker_pid":self.client.as_ref().unwrap().pid,
            "research_jobs_actual":true,
            "boundary_evidence":boundary_evidence
        }))
    }

    async fn command(&mut self, action: &str, job_id: Option<&str>) -> Result<Value, String> {
        match action {
            "start" if job_id.is_none() => self.start().await,
            "status" => self.status(job_id.ok_or("JOB_ID_REQUIRED")?).await,
            "explain" => self.explain(job_id.ok_or("JOB_ID_REQUIRED")?).await,
            "disconnect" if job_id.is_none() => {
                let client = self.client.take().ok_or("RESEARCH_WORKER_OFFLINE")?;
                client.disconnect().await?;
                Ok(json!({"disconnected":true}))
            }
            "reopen_status" => {
                let job = job_id.ok_or("JOB_ID_REQUIRED")?.to_owned();
                if self.client.is_some() {
                    return Err("RESEARCH_WORKER_STILL_ONLINE".into());
                }
                self.client =
                    Some(WorkerClient::spawn(&self.root, &self.worker, self.stats.clone()).await?);
                self.status(&job).await
            }
            "stats" if job_id.is_none() => {
                let (successful_requests, returned_rows) = self.counts();
                Ok(json!({"successful_requests":successful_requests,"returned_rows":returned_rows}))
            }
            "stop" if job_id.is_none() => {
                if let Some(client) = self.client.take() {
                    client.disconnect().await?;
                }
                Ok(json!({"stopped":true}))
            }
            _ => Err("INVALID_FIXTURE_COMMAND".into()),
        }
    }
}

async fn read_bounded_line(
    input: &mut BufReader<tokio::io::Stdin>,
) -> Result<Option<Vec<u8>>, String> {
    let mut bytes = Vec::new();
    let mut one = [0_u8; 1];
    loop {
        let read = input
            .read(&mut one)
            .await
            .map_err(|_| "FIXTURE_STDIN_READ_FAILED")?;
        if read == 0 {
            if bytes.is_empty() {
                return Ok(None);
            }
            return Err("FIXTURE_COMMAND_TRUNCATED".into());
        }
        if one[0] == b'\n' {
            return Ok(Some(bytes));
        }
        if bytes.len() == MAX_JSONL_COMMAND {
            return Err("FIXTURE_COMMAND_TOO_LARGE".into());
        }
        bytes.push(one[0]);
    }
}

async fn write_json(value: &Value) -> Result<(), String> {
    let mut bytes = serde_json::to_vec(value).map_err(|_| "FIXTURE_RESPONSE_INVALID")?;
    if bytes.len() > MAX_JSONL_REPLY {
        return Err("FIXTURE_RESPONSE_TOO_LARGE".into());
    }
    bytes.push(b'\n');
    let mut output = tokio::io::stdout();
    output
        .write_all(&bytes)
        .await
        .map_err(|_| "FIXTURE_STDOUT_WRITE_FAILED")?;
    output
        .flush()
        .await
        .map_err(|_| "FIXTURE_STDOUT_FLUSH_FAILED".to_owned())
}

async fn run() -> Result<(), String> {
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref() != Some(std::ffi::OsStr::new("--root")) {
        return Err("FIXTURE_USAGE".into());
    }
    let root = PathBuf::from(args.next().ok_or("FIXTURE_ROOT_REQUIRED")?);
    if args.next().as_deref() != Some(std::ffi::OsStr::new("--worker")) {
        return Err("FIXTURE_USAGE".into());
    }
    let worker = PathBuf::from(args.next().ok_or("FIXTURE_WORKER_REQUIRED")?);
    if args.next().is_some() {
        return Err("FIXTURE_USAGE".into());
    }
    let mut fixture = Fixture::open(root, worker).await?;
    write_json(&json!({"schema":SCHEMA,"event":"ready"})).await?;
    let mut input = BufReader::new(tokio::io::stdin());
    while let Some(line) = read_bounded_line(&mut input).await? {
        let command: FixtureCommand =
            serde_json::from_slice(&line).map_err(|_| "INVALID_FIXTURE_COMMAND")?;
        if command.schema != SCHEMA || command.id == 0 {
            return Err("INVALID_FIXTURE_COMMAND".into());
        }
        let result = fixture
            .command(&command.action, command.job_id.as_deref())
            .await;
        let exit = command.action == "stop" && result.is_ok();
        let reply = match result {
            Ok(data) => json!({"schema":SCHEMA,"id":command.id,"ok":true,"data":data}),
            Err(error) => json!({"schema":SCHEMA,"id":command.id,"ok":false,"error":error}),
        };
        write_json(&reply).await?;
        if exit {
            break;
        }
    }
    if let Some(client) = fixture.client.take() {
        client.disconnect().await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

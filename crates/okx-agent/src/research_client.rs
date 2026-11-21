#[cfg(test)]
mod activation_tests;
mod installed;
#[cfg(test)]
mod measurement_tests;
use futures_util::{SinkExt, StreamExt};
use okx_protocol::{
    analytics::{MarketRequest, ResearchRequest},
    research_ipc::*,
};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::HashMap,
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    process::Command,
    sync::{Mutex, Notify, Semaphore, broadcast, mpsc, oneshot, watch},
    task::JoinSet,
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::Instrument;

tokio::task_local! { static HOST_INVOCATION: Option<InvocationId>; }

/// Correlate an authenticated product invocation without changing its durable key.
pub(crate) async fn with_invocation<F: Future>(request_id: &str, future: F) -> F::Output {
    let span = tracing::info_span!(
        "product_invocation",
        invocation_id = request_id,
        owner = "agent_dispatch"
    );
    HOST_INVOCATION
        .scope(
            InvocationId::new(request_id.to_owned()).ok(),
            future.instrument(span),
        )
        .await
}

pub type SourceHandler = Arc<
    dyn Fn(
            PublicSourceRequest,
        ) -> Pin<Box<dyn Future<Output = Result<PublicSourceReply, String>> + Send>>
        + Send
        + Sync,
>;
#[derive(Clone)]
pub struct ResearchClientConfig {
    pub executable: PathBuf,
    pub root: PathBuf,
    pub scope: ResearchScope,
}
#[derive(Debug, Clone, Default, Serialize)]
pub struct ResearchDeploymentStatus {
    pub installed: Option<String>,
    pub active: Option<String>,
    pub draining: bool,
}
#[derive(Clone)]
pub struct ResearchClient {
    config: ResearchClientConfig,
    installation_runtime: Option<PathBuf>,
    source: SourceHandler,
    connection: Arc<Mutex<Option<Arc<Connection>>>>,
    deployment: Arc<std::sync::Mutex<ResearchDeploymentStatus>>,
    publications: broadcast::Sender<GraphSignalEvent>,
}

/// Subscribers are read-only. On a lagged broadcast or ServiceUnavailable,
/// consumers must stop using the stream and restore a checked cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphSignalEvent {
    Published(GraphSignalPublication),
    /// A per-session public feed stopped; never reinterpret this as Hold.
    FeedUnavailable(GraphFeedFault),
    ServiceUnavailable,
}
type PendingReplies =
    Arc<std::sync::Mutex<HashMap<String, oneshot::Sender<Result<WorkerReply, String>>>>>;

struct Connection {
    tx: mpsc::Sender<HostFrame>,
    pending: PendingReplies,
    session: WorkerSession,
    executable: PathBuf,
    draining: AtomicBool,
    next: AtomicU64,
    send_order: Mutex<()>,
    live: AtomicBool,
    permits: Semaphore,
    control_permits: Semaphore,
    observation_permits: Semaphore,
    source_permits: Arc<Semaphore>,
    stop: Notify,
    closed: watch::Sender<bool>,
}
struct PendingReply {
    pending: PendingReplies,
    id: String,
}
impl Drop for PendingReply {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.remove(&self.id);
        }
    }
}
fn issued_host_id(id: &RequestId, next: u64) -> bool {
    id.as_str()
        .strip_prefix('h')
        .and_then(|s| s.parse::<u64>().ok())
        .is_some_and(|n| n > 0 && n < next && format!("h{n}") == id.as_str())
}
fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_BYTES)
        .new_codec()
}
fn unique() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}
fn stop_if_dispatched(c: Option<&Arc<Connection>>, dispatched: bool) {
    if dispatched && let Some(c) = c {
        c.stop.notify_one();
    }
}
fn version_name(path: &std::path::Path) -> Option<String> {
    path.parent()?.file_name()?.to_str().map(str::to_owned)
}
fn next_id(id: &RequestId, prefix: char, previous: &mut u64) -> bool {
    let Some(number) = id
        .as_str()
        .strip_prefix(prefix)
        .and_then(|n| n.parse::<u64>().ok())
    else {
        return false;
    };
    if number <= *previous || format!("{prefix}{number}") != id.as_str() {
        return false;
    }
    *previous = number;
    true
}
impl ResearchClient {
    pub fn new(config: ResearchClientConfig, source: SourceHandler) -> Self {
        let (publications, _) = broadcast::channel(64);
        Self {
            config,
            installation_runtime: None,
            source,
            connection: Arc::new(Mutex::new(None)),
            deployment: Arc::new(std::sync::Mutex::new(ResearchDeploymentStatus::default())),
            publications,
        }
    }
    /// Production factory. Pointer changes are checked on demand; images are hashed before launch.
    pub fn installed(
        runtime_root: PathBuf,
        research_root: PathBuf,
        scope: ResearchScope,
        bootstrap: crate::market_bootstrap::MarketBootstrapper,
    ) -> Self {
        let (publications, _) = broadcast::channel(64);
        let source: SourceHandler = Arc::new(move |request| {
            let bootstrap = bootstrap.clone();
            Box::pin(async move { bootstrap.research_source(request).await })
        });
        Self {
            config: ResearchClientConfig {
                executable: PathBuf::new(),
                root: research_root,
                scope,
            },
            installation_runtime: Some(runtime_root),
            source,
            connection: Arc::new(Mutex::new(None)),
            deployment: Arc::new(std::sync::Mutex::new(ResearchDeploymentStatus::default())),
            publications,
        }
    }

    /// Direct Research publications; no synchronous strategy calculation or
    /// financial execution on the subscriber path. `Lagged` means a hard gap.
    pub fn subscribe_graph_signals(&self) -> broadcast::Receiver<GraphSignalEvent> {
        self.publications.subscribe()
    }
    async fn connect(&self) -> Result<Arc<Connection>, String> {
        let mut slot = self.connection.lock().await;
        let installed = match &self.installation_runtime {
            Some(root) => match tokio::task::spawn_blocking({
                let root = root.clone();
                move || installed::candidate(&root)
            })
            .await
            .map_err(|_| "RESEARCH_INSTALL_CHECK_FAILED")?
            {
                Ok(path) => {
                    if let Ok(mut state) = self.deployment.lock() {
                        state.installed = version_name(&path);
                    }
                    Some(path)
                }
                Err(error) => {
                    if let Ok(mut state) = self.deployment.lock() {
                        state.installed = None;
                    }
                    if slot
                        .as_ref()
                        .is_some_and(|c| c.live.load(Ordering::Acquire))
                    {
                        None
                    } else {
                        return Err(error);
                    }
                }
            },
            None => None,
        };
        if let Some(c) = slot
            .as_ref()
            .filter(|c| c.live.load(Ordering::Acquire))
            .cloned()
        {
            if installed.as_ref().is_some_and(|path| *path != c.executable)
                || c.draining.load(Ordering::Acquire)
            {
                c.draining.store(true, Ordering::Release);
                if let Ok(mut state) = self.deployment.lock() {
                    state.draining = true;
                }
                let drain = Self::send(&c, ResearchCommand::Drain, None).await?;
                let active = drain.drain()?.active_jobs;
                if active != 0 || installed.is_none() {
                    return Ok(c);
                }
                // No old command may remain in flight when the old worker exits.
                // Drain already prevents any racing enqueue from committing.
                let _all = c
                    .permits
                    .acquire_many(MAX_IN_FLIGHT as u32)
                    .await
                    .map_err(|_| "WORKER_CLOSED")?;
                let _control = c
                    .control_permits
                    .acquire_many(CRITICAL_IN_FLIGHT as u32)
                    .await
                    .map_err(|_| "WORKER_CLOSED")?;
                let _observations = c
                    .observation_permits
                    .acquire_many(OBSERVATION_IN_FLIGHT as u32)
                    .await
                    .map_err(|_| "WORKER_CLOSED")?;
                let _sources = c
                    .source_permits
                    .acquire_many(MAX_IN_FLIGHT as u32)
                    .await
                    .map_err(|_| "WORKER_CLOSED")?;
                let mut done = c.closed.subscribe();
                Self::send(&c, ResearchCommand::Shutdown, None)
                    .await?
                    .shutdown()?;
                while !*done.borrow_and_update() {
                    done.changed().await.map_err(|_| "WORKER_DISCONNECTED")?;
                }
            } else {
                return Ok(c);
            }
        }
        if let Some(c) = slot.as_ref() {
            let mut done = c.closed.subscribe();
            while !*done.borrow_and_update() {
                done.changed().await.map_err(|_| "WORKER_DISCONNECTED")?;
            }
        }
        let session = WorkerSession::new(unique()).map_err(str::to_owned)?;
        // Re-read after shutdown: installation may have changed during drain.
        let executable = match &self.installation_runtime {
            Some(root) => {
                let root = root.clone();
                let path = tokio::task::spawn_blocking(move || installed::resolve(&root))
                    .await
                    .map_err(|_| "RESEARCH_INSTALL_CHECK_FAILED")??;
                if let Ok(mut state) = self.deployment.lock() {
                    state.installed = version_name(&path);
                }
                path
            }
            None => self.config.executable.clone(),
        };
        let mut command = Command::new(&executable);
        command
            .arg("--root")
            .arg(&self.config.root)
            .arg("--scope")
            .arg(self.config.scope.as_str())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true);
        // Research executes CPU-heavy grids in its own child process. On
        // Windows, keep it below the Normal-priority native Finance/Engine
        // process so that an overloaded host cannot starve bounded financial
        // reads. The Worker still owns its two heavy slots and safety limits.
        #[cfg(windows)]
        command.creation_flags(0x0000_4000); // BELOW_NORMAL_PRIORITY_CLASS
        let mut child = command.spawn().map_err(|_| "WORKER_START")?;
        let stdin = child.stdin.take().ok_or("WORKER_STDIN")?;
        let stdout = child.stdout.take().ok_or("WORKER_STDOUT")?;
        let mut writer = FramedWrite::new(stdin, codec());
        let mut reader = FramedRead::new(stdout, codec());
        let hello = HostFrame::Hello {
            protocol: PROTOCOL_VERSION,
            storage: STORAGE_VERSION,
            session: session.clone(),
            scope: self.config.scope.clone(),
        };
        writer
            .send(bytes::Bytes::from(
                serde_json::to_vec(&hello).map_err(|_| "WORKER_HELLO_ENCODE")?,
            ))
            .await
            .map_err(|_| "WORKER_HELLO_WRITE")?;
        let frame = reader
            .next()
            .await
            .ok_or("WORKER_EOF")?
            .map_err(|_| "WORKER_HELLO_READ")?;
        match serde_json::from_slice::<WorkerFrame>(&frame).map_err(|_| "WORKER_HELLO_DECODE")? {
            WorkerFrame::Hello {
                protocol: PROTOCOL_VERSION,
                storage: STORAGE_VERSION,
                session: s,
                scope,
                producer_sha256,
            } if s == session
                && scope == self.config.scope
                && producer_sha256.len() == 64
                && producer_sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                && (self.installation_runtime.is_none()
                    || version_name(&executable).as_ref() == Some(&producer_sha256)) => {}
            _ => return Err("WORKER_HELLO_MISMATCH".into()),
        }
        let (tx, mut rx) = mpsc::channel::<HostFrame>(QUEUE_CAPACITY);
        let (closed, _) = watch::channel(false);
        let c = Arc::new(Connection {
            tx,
            pending: Arc::new(std::sync::Mutex::new(HashMap::new())),
            session,
            executable: executable.clone(),
            draining: AtomicBool::new(false),
            next: AtomicU64::new(1),
            send_order: Mutex::new(()),
            live: AtomicBool::new(true),
            permits: Semaphore::new(MAX_IN_FLIGHT),
            control_permits: Semaphore::new(CRITICAL_IN_FLIGHT),
            observation_permits: Semaphore::new(OBSERVATION_IN_FLIGHT),
            source_permits: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            stop: Notify::new(),
            closed,
        });
        let driver = c.clone();
        let source = self.source.clone();
        let deployment = self.deployment.clone();
        let publications = self.publications.clone();
        tokio::spawn(async move {
            let mut writer_task = tokio::spawn(async move {
                while let Some(frame) = rx.recv().await {
                    let data = serde_json::to_vec(&frame).map_err(|_| ())?;
                    if data.len() > MAX_FRAME_BYTES {
                        return Err(());
                    }
                    tokio::time::timeout(
                        Duration::from_millis(REQUEST_TIMEOUT_MS),
                        writer.send(bytes::Bytes::from(data)),
                    )
                    .await
                    .map_err(|_| ())?
                    .map_err(|_| ())?;
                }
                Ok::<(), ()>(())
            });
            let mut sources = JoinSet::new();
            let source_permits = driver.source_permits.clone();
            let mut last_source = 0;
            let mut writer_done = false;
            loop {
                tokio::select! {
                    _ = driver.stop.notified() => break,
                    _ = &mut writer_task => { writer_done = true; break; },
                    Some(_) = sources.join_next(), if !sources.is_empty() => {},
                    incoming = reader.next() => match incoming {
                        Some(Ok(bytes)) => match serde_json::from_slice::<WorkerFrame>(&bytes) {
                            Ok(WorkerFrame::Response { id, session, result }) if session == driver.session => { let reply = driver.pending.lock().ok().and_then(|mut p| p.remove(id.as_str())); if let Some(reply) = reply { let _ = reply.send(result); } else if !issued_host_id(&id, driver.next.load(Ordering::Acquire)) { break; } },
                            Ok(WorkerFrame::SourceRequest { id, session, request }) if session == driver.session => { if !request.valid() || !next_id(&id, 's', &mut last_source) { break; } let permit = match source_permits.clone().try_acquire_owned() { Ok(p) => p, Err(_) => break }; let source = source.clone(); let tx = driver.tx.clone(); sources.spawn(async move { let _permit = permit; let result = source(request).await; let _ = tx.send(HostFrame::SourceReply { id, session, result }).await; }); },
                            Ok(WorkerFrame::SignalPublished { session, publication }) if session == driver.session => {
                                if !publication.valid() { break; }
                                // No subscribers means no trading action; a
                                // slow consumer sees Lagged, never silent skip.
                                let _ = publications.send(GraphSignalEvent::Published(publication));
                            },
                            Ok(WorkerFrame::GraphFeedFault { session, fault }) if session == driver.session => {
                                if !fault.valid() { break; }
                                let _ = publications.send(GraphSignalEvent::FeedUnavailable(fault));
                            },
                            _ => break,
                        }, _ => break,
                    },
                    _ = child.wait() => break,
                }
            }
            driver.live.store(false, Ordering::Release);
            let _ = publications.send(GraphSignalEvent::ServiceUnavailable);
            sources.abort_all();
            while sources.join_next().await.is_some() {}
            if !writer_done {
                writer_task.abort();
                let _ = writer_task.await;
            }
            if let Ok(mut pending) = driver.pending.lock() {
                for (_, reply) in pending.drain() {
                    let _ = reply.send(Err("WORKER_DISCONNECTED".into()));
                }
            }
            let _ = child.kill().await;
            let _ = child.wait().await;
            if let Ok(mut state) = deployment.lock()
                && state.active == version_name(&driver.executable)
            {
                state.active = None;
                state.draining = false;
            }
            driver.closed.send_replace(true);
        });
        if let Ok(mut state) = self.deployment.lock() {
            state.active = version_name(&executable);
            state.draining = false;
        }
        *slot = Some(c.clone());
        Ok(c)
    }
    async fn send(
        c: &Arc<Connection>,
        command: ResearchCommand,
        dispatched: Option<&AtomicBool>,
    ) -> Result<WorkerReply, String> {
        let _permit = if matches!(command, ResearchCommand::Shutdown) {
            None
        } else {
            Some(
                match command.lane() {
                    ResearchLane::Critical => c.control_permits.acquire().await,
                    ResearchLane::Observation => c.observation_permits.acquire().await,
                    ResearchLane::Work => c.permits.acquire().await,
                }
                .map_err(|_| "WORKER_CLOSED")?,
            )
        };
        if !c.live.load(Ordering::Acquire) {
            return Err("WORKER_DISCONNECTED".into());
        }
        let order = c.send_order.lock().await;
        let queue_slot = c.tx.reserve().await.map_err(|_| "WORKER_DISCONNECTED")?;
        let (rx, _guard) = {
            let mut pending = c.pending.lock().map_err(|_| "WORKER_CLOSED")?;
            let number = c.next.load(Ordering::Acquire);
            let next = number.checked_add(1).ok_or("WORKER_ID_EXHAUSTED")?;
            let id = RequestId::new(format!("h{number}")).map_err(str::to_owned)?;
            let (tx, rx) = oneshot::channel();
            pending.insert(id.as_str().to_owned(), tx);
            tracing::info!(
                owner = "research_client",
                stage = "dispatch",
                ipc_request_id = id.as_str(),
                session = c.session.as_str(),
                invocation_id = HOST_INVOCATION
                    .try_with(Clone::clone)
                    .ok()
                    .flatten()
                    .as_ref()
                    .map(InvocationId::as_str),
                "research request dispatched"
            );
            let guard = PendingReply {
                pending: c.pending.clone(),
                id: id.as_str().to_owned(),
            };
            c.next.store(next, Ordering::Release);
            queue_slot.send(HostFrame::Request {
                id,
                session: c.session.clone(),
                invocation: HOST_INVOCATION.try_with(Clone::clone).ok().flatten(),
                command: Box::new(command),
            });
            if let Some(dispatched) = dispatched {
                dispatched.store(true, Ordering::Release);
            }
            (rx, guard)
        };
        drop(order);
        rx.await.map_err(|_| "WORKER_DISCONNECTED")?
    }
    async fn call(&self, command: ResearchCommand, start: bool) -> Result<WorkerReply, String> {
        let mut used = None;
        let dispatched = AtomicBool::new(false);
        let diagnostic = matches!(command, ResearchCommand::Explain { .. });
        let result = tokio::time::timeout(
            Duration::from_millis(if diagnostic { 100 } else { REQUEST_TIMEOUT_MS }),
            async {
                let c = if start {
                    self.connect().await?
                } else {
                    self.connection
                        .lock()
                        .await
                        .as_ref()
                        .filter(|c| c.live.load(Ordering::Acquire))
                        .cloned()
                        .ok_or("WORKER_UNAVAILABLE")?
                };
                used = Some(c.clone());
                Self::send(&c, command, Some(&dispatched)).await
            },
        )
        .await;
        match result {
            Ok(result) => result,
            Err(_) => {
                if !diagnostic {
                    stop_if_dispatched(used.as_ref(), dispatched.load(Ordering::Acquire));
                }
                Err("WORKER_TIMEOUT".into())
            }
        }
    }
    /// Cached projection only; this does not read the pointer or start a worker.
    pub fn deployment_status(&self) -> ResearchDeploymentStatus {
        self.deployment
            .lock()
            .map(|s| s.clone())
            .unwrap_or_default()
    }
    pub async fn research(&self, request: ResearchRequest) -> Result<Value, String> {
        self.call(ResearchCommand::Research { request }, true)
            .await?
            .product()
    }
    /// Strictly read-only mathematical result from the independently installed Worker.
    pub async fn evaluate_signal(&self, request: SignalRequest) -> Result<SignalDecision, String> {
        if !request.valid() {
            return Err("INVALID_SIGNAL_REQUEST".into());
        }
        let decision = self
            .call(
                ResearchCommand::EvaluateSignal {
                    request: request.clone(),
                },
                true,
            )
            .await?
            .signal()?;
        if !decision.matches_request(&request) {
            return Err("SIGNAL_RESPONSE_MISMATCH".into());
        }
        Ok(decision)
    }
    /// Read-only versioned graph transport. The Agent does not resolve
    /// operators, allocate graph state, or transform intent into an order.
    pub async fn graph_signal(
        &self,
        request: GraphSessionRequest,
    ) -> Result<GraphSessionReply, String> {
        if !request.valid() {
            return Err("GRAPH_SESSION_REQUEST_INVALID".into());
        }
        let reply = self
            .call(
                ResearchCommand::GraphSignal {
                    request: request.clone(),
                },
                true,
            )
            .await?
            .graph_signal()?;
        if !reply.matches_request(&request) {
            return Err("GRAPH_SESSION_RESPONSE_MISMATCH".into());
        }
        Ok(reply)
    }
    /// Recover missed, already committed Research events. The caller must use
    /// a durably accepted cursor; receipt alone is never an order command.
    pub async fn replay_graph_publications(
        &self,
        request: GraphPublicationRequest,
    ) -> Result<GraphPublicationPage, String> {
        if !request.valid() {
            return Err("GRAPH_PUBLICATION_REQUEST_INVALID".into());
        }
        let page = self
            .call(
                ResearchCommand::ReplayGraphPublications {
                    request: request.clone(),
                },
                true,
            )
            .await?
            .graph_publications()?;
        if !page.matches_request(&request) {
            return Err("GRAPH_PUBLICATION_RESPONSE_MISMATCH".into());
        }
        Ok(page)
    }
    /// One-time public feed control: Research polls and publishes thereafter.
    /// No Engine per-bar request and no financial capability crosses IPC.
    pub async fn graph_feed(&self, request: GraphFeedRequest) -> Result<GraphFeedReply, String> {
        if !request.valid() {
            return Err("GRAPH_FEED_REQUEST_INVALID".into());
        }
        let reply = self.call(ResearchCommand::GraphFeed { request: request.clone() }, true)
            .await?.graph_feed()?;
        if !reply.matches_request(&request) {
            return Err("GRAPH_FEED_RESPONSE_MISMATCH".into());
        }
        Ok(reply)
    }
    pub async fn market(
        &self,
        request: MarketRequest,
        universe_page: Option<Value>,
        resolved_instruments: Option<Vec<String>>,
    ) -> Result<Value, String> {
        self.call(
            ResearchCommand::Market {
                request,
                universe_page,
                resolved_instruments,
            },
            true,
        )
        .await?
        .product()
    }
    pub async fn capabilities(&self) -> Result<Value, String> {
        self.call(ResearchCommand::Capabilities, true)
            .await?
            .product()
    }
    pub async fn diagnostic_job(&self, id: &str) -> Result<Value, String> {
        self.call(
            ResearchCommand::Explain {
                job_id: JobId::new(id.to_owned()).map_err(str::to_owned)?,
            },
            false,
        )
        .await?
        .product()
    }
    pub async fn shutdown(&self) {
        let c = self.connection.lock().await.as_ref().cloned();
        if let Some(c) = c {
            let mut closed = c.closed.subscribe();
            if c.live.load(Ordering::Acquire) {
                let _ = tokio::time::timeout(
                    Duration::from_millis(REQUEST_TIMEOUT_MS),
                    Self::send(&c, ResearchCommand::Shutdown, None),
                )
                .await;
                c.stop.notify_one();
            }
            let _ = tokio::time::timeout(Duration::from_millis(REQUEST_TIMEOUT_MS), async {
                while !*closed.borrow_and_update() {
                    if closed.changed().await.is_err() {
                        break;
                    }
                }
            })
            .await;
        }
    }
}

#[cfg(test)]
mod transport_tests {
    use super::*;
    #[tokio::test]
    async fn diagnostic_timeout_cleans_pending_without_stopping_worker() {
        let (tx, mut rx) = mpsc::channel(1);
        let (closed, _) = watch::channel(false);
        let c = Arc::new(Connection {
            tx,
            pending: Arc::new(std::sync::Mutex::new(HashMap::new())),
            session: WorkerSession::new("test".into()).unwrap(),
            executable: PathBuf::new(),
            draining: AtomicBool::new(false),
            next: AtomicU64::new(1),
            send_order: Mutex::new(()),
            live: AtomicBool::new(true),
            permits: Semaphore::new(MAX_IN_FLIGHT),
            control_permits: Semaphore::new(CRITICAL_IN_FLIGHT),
            observation_permits: Semaphore::new(OBSERVATION_IN_FLIGHT),
            source_permits: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            stop: Notify::new(),
            closed,
        });
        let source: SourceHandler = Arc::new(|_| Box::pin(async { Err("unused".into()) }));
        let client = ResearchClient::new(
            ResearchClientConfig {
                executable: PathBuf::new(),
                root: PathBuf::new(),
                scope: ResearchScope::new("test".into()).unwrap(),
            },
            source,
        );
        *client.connection.lock().await = Some(c.clone());
        let normal = c.permits.acquire_many(MAX_IN_FLIGHT as u32).await.unwrap();
        let task = tokio::spawn(async move { client.diagnostic_job("job").await });
        let frame = rx.recv().await.unwrap();
        let id = match frame {
            HostFrame::Request { id, command, .. }
                if matches!(*command, ResearchCommand::Explain { .. }) =>
            {
                id
            }
            _ => panic!("expected diagnostic"),
        };
        assert_eq!(task.await.unwrap().unwrap_err(), "WORKER_TIMEOUT");
        assert!(c.pending.lock().unwrap().is_empty());
        assert!(issued_host_id(&id, c.next.load(Ordering::Acquire)));
        assert!(!issued_host_id(
            &RequestId::new("h2".into()).unwrap(),
            c.next.load(Ordering::Acquire)
        ));
        assert!(c.live.load(Ordering::Acquire));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), c.stop.notified())
                .await
                .is_err()
        );
        drop(normal);
    }
    #[tokio::test]
    async fn timeout_before_handoff_does_not_stop_other_work() {
        let (tx, _rx) = mpsc::channel(1);
        let (closed, _) = watch::channel(false);
        let c = Arc::new(Connection {
            tx,
            pending: Arc::new(std::sync::Mutex::new(HashMap::new())),
            session: WorkerSession::new("test".into()).unwrap(),
            executable: PathBuf::new(),
            draining: AtomicBool::new(false),
            next: AtomicU64::new(1),
            send_order: Mutex::new(()),
            live: AtomicBool::new(true),
            permits: Semaphore::new(MAX_IN_FLIGHT),
            control_permits: Semaphore::new(CRITICAL_IN_FLIGHT),
            observation_permits: Semaphore::new(OBSERVATION_IN_FLIGHT),
            source_permits: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            stop: Notify::new(),
            closed,
        });
        let held = c.permits.acquire_many(MAX_IN_FLIGHT as u32).await.unwrap();
        let work = ResearchCommand::Research {
            request: ResearchRequest::Economics {
                cases: vec![],
                data_mode: okx_protocol::analytics::ResearchDataMode::Counterfactual,
            },
        };
        let sent = AtomicBool::new(false);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                ResearchClient::send(&c, work.clone(), Some(&sent))
            )
            .await
            .is_err()
        );
        stop_if_dispatched(Some(&c), sent.load(Ordering::Acquire));
        assert!(!sent.load(Ordering::Acquire));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), c.stop.notified())
                .await
                .is_err()
        );
        assert!(c.pending.lock().unwrap().is_empty());
        drop(held);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                ResearchClient::send(&c, work, Some(&sent))
            )
            .await
            .is_err()
        );
        assert!(sent.load(Ordering::Acquire));
        stop_if_dispatched(Some(&c), sent.load(Ordering::Acquire));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), c.stop.notified())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn t11_t12_cancel_is_dispatchable_while_work_and_observation_lanes_are_full() {
        let (tx, mut rx) = mpsc::channel(4);
        let (closed, _) = watch::channel(false);
        let c = Arc::new(Connection {
            tx,
            pending: Arc::new(std::sync::Mutex::new(HashMap::new())),
            session: WorkerSession::new("t11-lanes".into()).unwrap(),
            executable: PathBuf::new(),
            draining: AtomicBool::new(false),
            next: AtomicU64::new(1),
            send_order: Mutex::new(()),
            live: AtomicBool::new(true),
            permits: Semaphore::new(MAX_IN_FLIGHT),
            control_permits: Semaphore::new(CRITICAL_IN_FLIGHT),
            observation_permits: Semaphore::new(OBSERVATION_IN_FLIGHT),
            source_permits: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            stop: Notify::new(),
            closed,
        });
        let _work = c.permits.acquire_many(MAX_IN_FLIGHT as u32).await.unwrap();
        let observations = c
            .observation_permits
            .acquire_many(OBSERVATION_IN_FLIGHT as u32)
            .await
            .unwrap();

        let cancel = ResearchCommand::Research {
            request: ResearchRequest::Cancel {
                job_id: "job".into(),
                idempotency_key: "t11-cancel".into(),
            },
        };
        let owner = c.clone();
        let task = tokio::spawn(async move { ResearchClient::send(&owner, cancel, None).await });
        let frame = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let HostFrame::Request { id, command, .. } = frame else {
            panic!("missing cancel");
        };
        assert_eq!(command.lane(), ResearchLane::Critical);
        c.pending
            .lock()
            .unwrap()
            .remove(id.as_str())
            .unwrap()
            .send(Ok(WorkerReply::Product(
                serde_json::json!({"state":"cancelled"}),
            )))
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );

        let owner = c.clone();
        let observed = tokio::spawn(async move {
            ResearchClient::send(&owner, ResearchCommand::Capabilities, None).await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(30), rx.recv())
                .await
                .is_err()
        );
        drop(observations);
        let frame = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let HostFrame::Request { id, command, .. } = frame else {
            panic!("missing observation");
        };
        assert_eq!(command.lane(), ResearchLane::Observation);
        c.pending
            .lock()
            .unwrap()
            .remove(id.as_str())
            .unwrap()
            .send(Ok(WorkerReply::Product(
                serde_json::json!({"quality":"fresh"}),
            )))
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), observed)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
    }
}

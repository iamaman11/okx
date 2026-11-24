use crate::{
    analytics::AnalyticsService,
    source_api::{PublicSourceClient, PublicSourceProvider, SourceFuture},
};
use futures_util::{SinkExt, StreamExt};
use okx_research_protocol::research_ipc::*;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Mutex, Semaphore, mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::Instrument;

fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_BYTES)
        .new_codec()
}

/// The Research boundary owns the conversion from externally validated wire
/// fields to pure, typed portfolio mathematics. The Analysis crate has no
/// dependency on the Engine/Research IPC package or its crypto dependencies.
pub(crate) fn portfolio_scenario_wire(
    request: &okx_research_protocol::analytics::PortfolioScenarioRequest,
) -> Result<serde_json::Value, String> {
    use okx_analysis::portfolio_scenario::{
        PortfolioScenarioInput, ScenarioCorrelation, ScenarioPosition, ScenarioSide, evaluate,
    };
    use okx_research_protocol::analytics::ScenarioPositionSide;
    use rust_decimal::Decimal;

    if !request.valid() {
        return Err("INVALID_PORTFOLIO_SCENARIO".into());
    }
    let decimal = |value: &str| {
        value
            .parse::<Decimal>()
            .map_err(|_| "INVALID_DECIMAL".to_owned())
    };
    let positions = request
        .positions
        .iter()
        .map(|position| {
            Ok(ScenarioPosition {
                instrument: position.instrument.clone(),
                side: match position.side {
                    ScenarioPositionSide::Long => ScenarioSide::Long,
                    ScenarioPositionSide::Short => ScenarioSide::Short,
                },
                notional_usd: decimal(&position.notional_usd)?,
                margin_usd: decimal(&position.margin_usd)?,
                price_shock_bps: position.price_shock_bps,
                volatility_bps: position.volatility_bps,
                liquidity_cost_bps: position.liquidity_cost_bps,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let correlations = request
        .correlations
        .iter()
        .map(|pair| ScenarioCorrelation {
            left: pair.left,
            right: pair.right,
            rho: pair.rho,
        })
        .collect();
    let input = PortfolioScenarioInput::new(decimal(&request.equity_usd)?, positions, correlations)
        .map_err(|error| error.to_string())?;
    let result = evaluate(&input).map_err(|error| error.to_string())?;
    let rows = result
        .positions
        .iter()
        .map(|position| {
            serde_json::json!({
                "instrument": position.instrument,
                "side": match position.side { ScenarioSide::Long => "long", ScenarioSide::Short => "short" },
                "price_pnl_usd": position.price_pnl_usd.normalize().to_string(),
                "liquidity_loss_usd": position.liquidity_loss_usd.normalize().to_string(),
                "source": "caller_supplied",
            })
        })
        .collect::<Vec<_>>();
    Ok(serde_json::json!({
        "schema": "okx.portfolio.scenario/v1",
        "quality": "assumed",
        "execution": "virtual_read_only",
        "exchange_post": false,
        "venue_margin_verified": false,
        "provenance": "caller_supplied",
        "gross_notional_usd": result.gross_notional_usd.normalize().to_string(),
        "margin_usd": result.margin_usd.normalize().to_string(),
        "stress_price_pnl_usd": result.stress_price_pnl_usd.normalize().to_string(),
        "liquidity_loss_usd": result.liquidity_loss_usd.normalize().to_string(),
        "net_stress_pnl_usd": result.net_stress_pnl_usd.normalize().to_string(),
        "stressed_equity_usd": result.stressed_equity_usd.normalize().to_string(),
        "drawdown_ratio": result.drawdown_ratio.normalize().to_string(),
        "gross_leverage_ratio": result.gross_leverage_ratio.normalize().to_string(),
        "correlated_volatility_usd": result.correlated_volatility_usd,
        "correlation_basis": "caller_supplied_psd",
        "positions": rows,
    }))
}
type PendingSources =
    Arc<std::sync::Mutex<HashMap<String, oneshot::Sender<Result<PublicSourceReply, String>>>>>;

struct RemoteSource {
    tx: mpsc::Sender<(WorkerFrame, Option<oneshot::Sender<()>>)>,
    order: Mutex<()>,
    session: WorkerSession,
    next: Arc<AtomicU64>,
    pending: PendingSources,
    permits: Semaphore,
}
struct PendingSource {
    pending: PendingSources,
    id: String,
}
impl Drop for PendingSource {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.remove(&self.id);
        }
    }
}
fn issued_source_id(id: &RequestId, next: u64) -> bool {
    id.as_str()
        .strip_prefix('s')
        .and_then(|s| s.parse::<u64>().ok())
        .is_some_and(|n| n > 0 && n < next && format!("s{n}") == id.as_str())
}
fn accept_source_reply(
    pending: &std::sync::Mutex<HashMap<String, oneshot::Sender<Result<PublicSourceReply, String>>>>,
    next: u64,
    id: &RequestId,
    result: Result<PublicSourceReply, String>,
) -> Result<(), String> {
    let reply = pending
        .lock()
        .map_err(|_| "SOURCE_CLOSED")?
        .remove(id.as_str());
    if let Some(reply) = reply {
        let _ = reply.send(result);
        Ok(())
    } else if issued_source_id(id, next) {
        Ok(())
    } else {
        Err("UNKNOWN_SOURCE_REPLY".into())
    }
}
impl PublicSourceProvider for RemoteSource {
    fn request(&self, request: PublicSourceRequest) -> SourceFuture<'_> {
        Box::pin(async move {
            if !request.valid() {
                return Err("INVALID_PUBLIC_SOURCE_REQUEST".into());
            }
            tokio::time::timeout(Duration::from_millis(REQUEST_TIMEOUT_MS), async {
                let _permit = self.permits.acquire().await.map_err(|_| "SOURCE_CLOSED")?;
                let order = self.order.lock().await;
                let queue_slot = self.tx.reserve().await.map_err(|_| "SOURCE_CLOSED")?;
                let number = self.next.load(Ordering::Acquire);
                let next = number.checked_add(1).ok_or("SOURCE_ID_EXHAUSTED")?;
                let id = RequestId::new(format!("s{number}")).map_err(str::to_owned)?;
                let (tx, rx) = oneshot::channel();
                self.pending
                    .lock()
                    .map_err(|_| "SOURCE_CLOSED")?
                    .insert(id.as_str().to_owned(), tx);
                let _guard = PendingSource {
                    pending: self.pending.clone(),
                    id: id.as_str().to_owned(),
                };
                self.next.store(next, Ordering::Release);
                queue_slot.send((
                    WorkerFrame::SourceRequest {
                        id,
                        session: self.session.clone(),
                        request,
                    },
                    None,
                ));
                drop(order);
                rx.await.map_err(|_| "SOURCE_CLOSED".to_owned())?
            })
            .await
            .map_err(|_| "SOURCE_TIMEOUT".to_owned())?
        })
    }
}
/// Serve one parent connection. EOF, invalid frames and session violations terminate the worker.
pub async fn serve<R, W>(
    input: R,
    output: W,
    root: PathBuf,
    expected_scope: ResearchScope,
) -> Result<(), String>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    serve_with_source(input, output, root, expected_scope, None).await
}

/// Serve the same typed Research commands using the public-only local source.
pub async fn serve_independent<R, W>(
    input: R,
    output: W,
    root: PathBuf,
    expected_scope: ResearchScope,
) -> Result<(), String>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let provider = crate::independent_source::IndependentOkxPublicSource::new()?;
    let source = PublicSourceClient::new(Arc::new(provider));
    serve_with_source(input, output, root, expected_scope, Some(source)).await
}

async fn serve_with_source<R, W>(
    input: R,
    output: W,
    root: PathBuf,
    expected_scope: ResearchScope,
    source: Option<PublicSourceClient>,
) -> Result<(), String>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut reader = FramedRead::new(input, codec());
    let mut writer = FramedWrite::new(output, codec());
    let bytes = tokio::time::timeout(Duration::from_millis(REQUEST_TIMEOUT_MS), reader.next())
        .await
        .map_err(|_| "HELLO_TIMEOUT")?
        .ok_or("HELLO_EOF")?
        .map_err(|e| e.to_string())?;
    let session = match serde_json::from_slice::<HostFrame>(&bytes).map_err(|e| e.to_string())? {
        HostFrame::Hello {
            protocol: PROTOCOL_VERSION,
            storage: STORAGE_VERSION,
            session,
            scope,
        } if scope == expected_scope => session,
        _ => return Err("HELLO_MISMATCH".into()),
    };
    std::fs::create_dir_all(&root).map_err(|e| e.to_string())?;
    let (tx, mut rx) = mpsc::channel(QUEUE_CAPACITY);
    let pending = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let next_source = Arc::new(AtomicU64::new(1));
    let remote_source = source.is_none();
    let source = source.unwrap_or_else(|| {
        PublicSourceClient::new(Arc::new(RemoteSource {
            tx: tx.clone(),
            order: Mutex::new(()),
            session: session.clone(),
            next: next_source.clone(),
            pending: pending.clone(),
            permits: Semaphore::new(MAX_IN_FLIGHT),
        }))
    });
    let producer = tokio::task::spawn_blocking(crate::producer::Producer::current)
        .await
        .map_err(|_| "WORKER_PRODUCER_UNAVAILABLE")??;
    let service = AnalyticsService::new_scoped_with_producer(
        root,
        source,
        &expected_scope,
        producer.clone(),
    )?;
    let hello = WorkerFrame::Hello {
        protocol: PROTOCOL_VERSION,
        storage: STORAGE_VERSION,
        session: session.clone(),
        scope: expected_scope,
        producer_sha256: producer.binary_sha256().to_owned(),
    };
    tokio::time::timeout(
        Duration::from_millis(REQUEST_TIMEOUT_MS),
        writer.send(bytes::Bytes::from(
            serde_json::to_vec(&hello).map_err(|_| "HELLO_ENCODE")?,
        )),
    )
    .await
    .map_err(|_| "HELLO_WRITE_TIMEOUT")?
    .map_err(|_| "HELLO_WRITE_FAILED")?;
    let mut writers = JoinSet::new();
    writers.spawn(async move {
        while let Some((frame, ack)) = rx.recv().await {
            let bytes = serde_json::to_vec(&frame).map_err(|_| "ENCODE")?;
            if bytes.len() > MAX_FRAME_BYTES {
                return Err("FRAME_TOO_LARGE");
            }
            tokio::time::timeout(
                Duration::from_millis(REQUEST_TIMEOUT_MS),
                writer.send(bytes::Bytes::from(bytes)),
            )
            .await
            .map_err(|_| "WRITE_TIMEOUT")?
            .map_err(|_| "WRITE_FAILED")?;
            if let Some(ack) = ack {
                let _ = ack.send(());
            }
        }
        Ok::<(), &'static str>(())
    });
    let mut jobs = JoinSet::new();
    let permits = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
    let controls = Arc::new(Semaphore::new(CRITICAL_IN_FLIGHT));
    let observations = Arc::new(Semaphore::new(OBSERVATION_IN_FLIGHT));
    let mut last_request = 0u64;
    loop {
        tokio::select! {
            _ = writers.join_next() => return Err("WRITER_CLOSED".into()),
            incoming = reader.next() => match incoming {
                None => break,
                Some(Err(e)) => return Err(e.to_string()),
                Some(Ok(bytes)) => match serde_json::from_slice::<HostFrame>(&bytes).map_err(|e| e.to_string())? {
                    HostFrame::SourceReply { id, session: s, result } if remote_source && s == session => {
                        accept_source_reply(&pending, next_source.load(Ordering::Acquire), &id, result)?;
                    },
                    HostFrame::Request { id, session: s, invocation, command } if s == session => {
                        let command = *command;
                        let number = id.as_str().strip_prefix('h').and_then(|s| s.parse::<u64>().ok()).ok_or("INVALID_REQUEST_ID")?;
                        if number <= last_request || format!("h{number}") != id.as_str() { return Err("DUPLICATE_REQUEST".into()); }
                        last_request = number;
                        if matches!(command, ResearchCommand::Shutdown) { let response = WorkerFrame::Response { id, session: session.clone(), result: Ok(WorkerReply::Shutdown) }; let (ack, done) = oneshot::channel(); tokio::time::timeout(Duration::from_millis(REQUEST_TIMEOUT_MS), async { tx.send((response, Some(ack))).await.map_err(|_| "WRITER_CLOSED")?; done.await.map_err(|_| "WRITER_CLOSED") }).await.map_err(|_| "WRITE_TIMEOUT")??; break; }
                        let permit = match match command.lane() {
                            ResearchLane::Critical => controls.clone().try_acquire_owned(),
                            ResearchLane::Observation => observations.clone().try_acquire_owned(),
                            ResearchLane::Work => permits.clone().try_acquire_owned(),
                        } {
                            Ok(permit) => permit,
                            Err(_) => { tx.try_send((WorkerFrame::Response { id, session: session.clone(), result: Err("RESEARCH_BUSY".into()) }, None)).map_err(|_| "WRITER_CLOSED")?; continue; }
                        };
                        let service = service.clone(); let tx = tx.clone(); let session = session.clone();
                        let span = tracing::info_span!("research_command", owner="research_worker", ipc_request_id=id.as_str(), session=session.as_str(), invocation_id=invocation.as_ref().map(InvocationId::as_str));
                        jobs.spawn(crate::invocation::scope(invocation, async move {
                            let _permit = permit;
                            let result = match command {
                                ResearchCommand::Research { request } => service.research(request).await.map(WorkerReply::Product),
                                ResearchCommand::Market { request, universe_page, resolved_instruments } => service.market(request, universe_page, resolved_instruments).await.map(WorkerReply::Product),
                                ResearchCommand::EvaluateSignal { request } => service.evaluate_signal(request).await.map(WorkerReply::Signal),
                                ResearchCommand::ProposeStrategySignal { request } => service.stage_strategy_signal(request).await.map(WorkerReply::StrategyProposal),
                                ResearchCommand::PortfolioScenario { request } => portfolio_scenario_wire(&request).map(WorkerReply::Product),
                                ResearchCommand::Capabilities => Ok(WorkerReply::Product(service.market_capabilities())),
                                ResearchCommand::Drain => service.begin_drain().await.map(WorkerReply::Draining),
                                ResearchCommand::Explain { job_id } => Ok(WorkerReply::Product(service.diagnostic_job(job_id.as_str()))),
                                ResearchCommand::Shutdown => unreachable!(),
                            };
                            match &result {
                                Ok(WorkerReply::Product(value)) => tracing::info!(stage="reply", job_id=value.get("id").and_then(serde_json::Value::as_str), "research command completed"),
                                Ok(_) => tracing::info!(stage="control_reply", "research control completed"),
                                Err(_) => tracing::warn!(stage="reply", reason="RESEARCH_COMMAND_REJECTED", "research command rejected; inspect scoped job diagnostic"),
                            }
                            let _ = tx.send((WorkerFrame::Response { id, session, result }, None)).await;
                        }).instrument(span));
                    },
                    _ => return Err("INVALID_SESSION_OR_FRAME".into()),
                }
            },
            Some(_) = jobs.join_next(), if !jobs.is_empty() => {},
        }
    }
    jobs.abort_all();
    while jobs.join_next().await.is_some() {}
    writers.abort_all();
    while writers.join_next().await.is_some() {}
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_research_protocol::analytics::ResearchRequest;
    use okx_research_protocol::analytics::{
        PortfolioScenarioRequest, ScenarioCorrelation, ScenarioPosition, ScenarioPositionSide,
    };
    use tokio::io::AsyncWriteExt;

    fn scenario_position(side: ScenarioPositionSide) -> ScenarioPosition {
        ScenarioPosition {
            instrument: "BTC-USDT-SWAP".into(),
            side,
            notional_usd: "100".into(),
            margin_usd: "10".into(),
            price_shock_bps: -1_000,
            volatility_bps: 200,
            liquidity_cost_bps: 50,
        }
    }

    #[test]
    fn portfolio_scenario_wire_schema_and_decimal_values_remain_stable() {
        let request = PortfolioScenarioRequest {
            equity_usd: "50".into(),
            positions: vec![
                scenario_position(ScenarioPositionSide::Long),
                scenario_position(ScenarioPositionSide::Short),
            ],
            correlations: vec![ScenarioCorrelation {
                left: 0,
                right: 1,
                rho: 0.6,
            }],
        };
        let mut result = portfolio_scenario_wire(&request).unwrap();
        let volatility = result["correlated_volatility_usd"].as_f64().unwrap();
        assert!((volatility - 3.577708764).abs() < 1e-8);
        result
            .as_object_mut()
            .unwrap()
            .remove("correlated_volatility_usd");
        assert_eq!(
            result,
            serde_json::json!({
                "schema": "okx.portfolio.scenario/v1",
                "quality": "assumed",
                "execution": "virtual_read_only",
                "exchange_post": false,
                "venue_margin_verified": false,
                "provenance": "caller_supplied",
                "gross_notional_usd": "200",
                "margin_usd": "20",
                "stress_price_pnl_usd": "0",
                "liquidity_loss_usd": "1",
                "net_stress_pnl_usd": "-1",
                "stressed_equity_usd": "49",
                "drawdown_ratio": "0.02",
                "gross_leverage_ratio": "4",
                "correlation_basis": "caller_supplied_psd",
                "positions": [
                    {"instrument":"BTC-USDT-SWAP", "side":"long", "price_pnl_usd":"-10", "liquidity_loss_usd":"0.5", "source":"caller_supplied"},
                    {"instrument":"BTC-USDT-SWAP", "side":"short", "price_pnl_usd":"10", "liquidity_loss_usd":"0.5", "source":"caller_supplied"},
                ],
            })
        );
    }

    #[test]
    fn portfolio_scenario_wire_preserves_validation_and_psd_error_codes() {
        let mut request = PortfolioScenarioRequest {
            equity_usd: "50".into(),
            positions: vec![
                scenario_position(ScenarioPositionSide::Long),
                scenario_position(ScenarioPositionSide::Long),
            ],
            correlations: vec![ScenarioCorrelation {
                left: 0,
                right: 1,
                rho: 0.6,
            }],
        };
        request.correlations[0].rho = 1.2;
        assert_eq!(
            portfolio_scenario_wire(&request).unwrap_err(),
            "INVALID_PORTFOLIO_SCENARIO"
        );
        request.correlations[0].rho = 0.6;
        request.positions[0].notional_usd = "0".into();
        assert_eq!(
            portfolio_scenario_wire(&request).unwrap_err(),
            "INVALID_PORTFOLIO_SCENARIO"
        );
        request.positions[0].notional_usd = "100".into();
        request
            .positions
            .push(scenario_position(ScenarioPositionSide::Long));
        request.correlations = vec![
            ScenarioCorrelation {
                left: 0,
                right: 1,
                rho: 0.9,
            },
            ScenarioCorrelation {
                left: 0,
                right: 2,
                rho: 0.9,
            },
            ScenarioCorrelation {
                left: 1,
                right: 2,
                rho: -0.9,
            },
        ];
        assert_eq!(
            portfolio_scenario_wire(&request).unwrap_err(),
            "CORRELATION_NOT_PSD"
        );
    }

    #[tokio::test]
    async fn cancelled_source_late_reply_is_ignored_but_unissued_is_rejected() {
        let (tx, mut rx) = mpsc::channel(1);
        let pending = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let next = Arc::new(AtomicU64::new(1));
        let provider = Arc::new(RemoteSource {
            tx,
            order: Mutex::new(()),
            session: WorkerSession::new("session".into()).unwrap(),
            next: next.clone(),
            pending: pending.clone(),
            permits: Semaphore::new(1),
        });
        let source = provider.clone();
        let task = tokio::spawn(async move {
            source
                .request(PublicSourceRequest::Ticker {
                    instrument: "BTC-USDT".into(),
                })
                .await
        });
        let (frame, _) = rx.recv().await.unwrap();
        let id = match frame {
            WorkerFrame::SourceRequest { id, .. } => id,
            _ => panic!("missing source request"),
        };
        task.abort();
        let _ = task.await;
        assert!(pending.lock().unwrap().is_empty());
        assert!(
            accept_source_reply(
                &pending,
                next.load(Ordering::Acquire),
                &id,
                Err("late".into())
            )
            .is_ok()
        );
        let never = RequestId::new("s2".into()).unwrap();
        assert!(
            accept_source_reply(
                &pending,
                next.load(Ordering::Acquire),
                &never,
                Err("unexpected".into())
            )
            .is_err()
        );
    }
    #[tokio::test]
    async fn cancellation_has_reserved_capacity_when_jobs_and_observations_are_full() {
        let permits = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
        let observations = Arc::new(Semaphore::new(OBSERVATION_IN_FLIGHT));
        let controls = Arc::new(Semaphore::new(CRITICAL_IN_FLIGHT));
        let _full = permits
            .clone()
            .acquire_many_owned(MAX_IN_FLIGHT as u32)
            .await
            .unwrap();
        let _observed = observations
            .clone()
            .acquire_many_owned(OBSERVATION_IN_FLIGHT as u32)
            .await
            .unwrap();
        let cancel = ResearchCommand::Research {
            request: ResearchRequest::Cancel {
                job_id: "job".into(),
                idempotency_key: "cancel".into(),
            },
        };
        assert_eq!(cancel.lane(), ResearchLane::Critical);
        let _permit = controls.clone().try_acquire_owned().unwrap();
        assert!(permits.try_acquire().is_err());
        assert!(observations.try_acquire().is_err());
    }
    #[tokio::test]
    async fn split_and_coalesced_frames() {
        let (mut out, input) = tokio::io::duplex(128);
        let mut reader = FramedRead::new(input, codec());
        out.write_all(&[0, 0]).await.unwrap();
        out.write_all(&[0, 3, b'o', b'n', b'e', 0, 0, 0, 3, b't', b'w', b'o'])
            .await
            .unwrap();
        assert_eq!(&reader.next().await.unwrap().unwrap()[..], b"one");
        assert_eq!(&reader.next().await.unwrap().unwrap()[..], b"two");
    }
    #[tokio::test]
    async fn oversized_and_truncated_frames_fail() {
        let (mut out, input) = tokio::io::duplex(64);
        let mut reader = FramedRead::new(input, codec());
        out.write_all(&((MAX_FRAME_BYTES as u32) + 1).to_be_bytes())
            .await
            .unwrap();
        assert!(reader.next().await.unwrap().is_err());
        let (mut out, input) = tokio::io::duplex(64);
        let mut reader = FramedRead::new(input, codec());
        out.write_all(&[0, 0, 0, 3, b'x']).await.unwrap();
        drop(out);
        assert!(reader.next().await.unwrap().is_err());
    }
    #[tokio::test]
    async fn incompatible_hello_never_creates_root() {
        for wrong_scope in [false, true] {
            let root = std::env::temp_dir().join(format!(
                "okx-bad-hello-{}-{}-{}",
                std::process::id(),
                wrong_scope,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let (host, worker) = tokio::io::duplex(4096);
            let (output, _) = tokio::io::duplex(4096);
            let mut writer = FramedWrite::new(host, codec());
            let expected = ResearchScope::new("alice".into()).unwrap();
            let hello = HostFrame::Hello {
                protocol: if wrong_scope {
                    PROTOCOL_VERSION
                } else {
                    PROTOCOL_VERSION + 1
                },
                storage: STORAGE_VERSION,
                session: WorkerSession::new("session".into()).unwrap(),
                scope: ResearchScope::new(if wrong_scope { "bob" } else { "alice" }.into())
                    .unwrap(),
            };
            writer
                .send(bytes::Bytes::from(serde_json::to_vec(&hello).unwrap()))
                .await
                .unwrap();
            assert!(serve(worker, output, root.clone(), expected).await.is_err());
            assert!(!root.exists());
        }
    }
    #[test]
    fn typed_identity_rejects_invalid_wire_data() {
        assert!(serde_json::from_str::<WorkerSession>("\"bad scope\"").is_err());
        assert!(serde_json::from_str::<RequestId>("\"\"").is_err());
    }
}

#[cfg(test)]
mod correlation_tests;

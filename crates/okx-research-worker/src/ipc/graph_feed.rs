//! Research-owned public closed-candle ingestion. Tokio owns IO/timers, the
//! existing bounded CpuExecutor owns all graph math and SQLite commits.
use super::{AnalyticsService, WorkerFrame, WorkerSession};
use okx_protocol::research_ipc::{
    GraphClosedBar, GraphFeedFault, GraphFeedFaultReason, GraphFeedReply, GraphFeedRequest,
    GraphSessionId, GraphSignalPublication, REQUEST_TIMEOUT_MS,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{sync::{mpsc, Mutex}, task::JoinHandle, time::MissedTickBehavior};

const MAX_BARS_PER_POLL: u64 = 64;
type Writer = mpsc::Sender<(WorkerFrame, Option<tokio::sync::oneshot::Sender<()>>)>;

#[derive(Clone, PartialEq, Eq)]
struct FeedSpec {
    session_id: GraphSessionId,
    graph_sha256: String,
    instrument: String,
    timeframe_ms: u64,
    poll_every_ms: u64,
}

struct RunningFeed {
    spec: FeedSpec,
    task: JoinHandle<()>,
}

#[derive(Clone, Default)]
pub(super) struct GraphFeeds {
    running: Arc<Mutex<BTreeMap<GraphSessionId, RunningFeed>>>,
}

fn poll_window(last_closed_ms: u64, timeframe_ms: u64, now_ms: u64)
    -> Result<Option<(u64, u64)>, GraphFeedFaultReason>
{
    if timeframe_ms == 0 || last_closed_ms == 0 || last_closed_ms % timeframe_ms != 0 {
        return Err(GraphFeedFaultReason::SessionUnavailable);
    }
    let current_open = now_ms / timeframe_ms * timeframe_ms;
    if last_closed_ms >= current_open {
        return Err(GraphFeedFaultReason::SessionUnavailable);
    }
    let next = last_closed_ms.checked_add(timeframe_ms)
        .ok_or(GraphFeedFaultReason::SessionUnavailable)?;
    if next >= current_open {
        return Ok(None);
    }
    let upper = current_open.min(next.saturating_add(timeframe_ms * MAX_BARS_PER_POLL));
    Ok(Some((next, upper)))
}

async fn poll_once(
    service: &AnalyticsService,
    writer: &Writer,
    worker_session: &WorkerSession,
    spec: &FeedSpec,
) -> Result<(), GraphFeedFaultReason> {
    let cursor = service.graph_feed_cursor(spec.session_id.clone(), spec.graph_sha256.clone())
        .await.map_err(|_| GraphFeedFaultReason::SessionUnavailable)?;
    let now_ms = jiff::Timestamp::now().as_millisecond().max(0) as u64;
    let Some((next, upper)) = poll_window(cursor.last_closed_ms, spec.timeframe_ms, now_ms)? else {
        return Ok(());
    };
    let rows = match service.graph_feed_candles(&spec.instrument, spec.timeframe_ms, next, upper).await {
        Ok(rows) => rows,
        Err(error) if error == "CANDLE_RANGE_UNAVAILABLE" && upper - next < 2 * spec.timeframe_ms => return Ok(()),
        Err(error) if error == "CANDLE_RANGE_UNAVAILABLE" => return Err(GraphFeedFaultReason::MissingClosedBar),
        Err(_) => return Err(GraphFeedFaultReason::PublicSourceUnavailable),
    };
    let mut next_time = next;
    let mut sequence = cursor.observed_bars;
    for candle in rows.iter() {
        if candle.time != next_time || candle.time >= upper {
            return Err(GraphFeedFaultReason::MissingClosedBar);
        }
        let bar = GraphClosedBar { time_ms: candle.time, close: Some(candle.close) };
        if !bar.valid() {
            return Err(GraphFeedFaultReason::MissingClosedBar);
        }
        sequence = sequence.checked_add(1).ok_or(GraphFeedFaultReason::SessionUnavailable)?;
        let (reply, is_new) = service.advance_graph_feed(
            spec.session_id.clone(), spec.graph_sha256.clone(), sequence, bar,
        ).await.map_err(|_| GraphFeedFaultReason::SessionUnavailable)?;
        if is_new {
            let publication = GraphSignalPublication::from_decision(&reply)
                .ok_or(GraphFeedFaultReason::SessionUnavailable)?;
            tokio::time::timeout(
                Duration::from_millis(REQUEST_TIMEOUT_MS),
                writer.send((WorkerFrame::SignalPublished {
                    session: worker_session.clone(), publication,
                }, None)),
            ).await.map_err(|_| GraphFeedFaultReason::PublicationUnavailable)?
                .map_err(|_| GraphFeedFaultReason::PublicationUnavailable)?;
        }
        next_time = next_time.checked_add(spec.timeframe_ms)
            .ok_or(GraphFeedFaultReason::SessionUnavailable)?;
    }
    // A newly closed candle can take a moment to be confirmed at the venue.
    // Two or more missing closed bars are a real gap: never skip or synthesize.
    if upper.saturating_sub(next_time) >= 2 * spec.timeframe_ms {
        return Err(GraphFeedFaultReason::MissingClosedBar);
    }
    Ok(())
}

impl GraphFeeds {
    pub(super) async fn command(
        &self,
        service: AnalyticsService,
        writer: Writer,
        worker_session: WorkerSession,
        request: GraphFeedRequest,
    ) -> Result<GraphFeedReply, String> {
        if !request.valid() {
            return Err("GRAPH_FEED_REQUEST_INVALID".into());
        }
        match request {
            GraphFeedRequest::Start {
                session_id, graph_sha256, instrument, timeframe_ms, poll_every_ms,
            } => {
                let spec = FeedSpec { session_id: session_id.clone(), graph_sha256, instrument, timeframe_ms, poll_every_ms };
                let mut running = self.running.lock().await;
                if let Some(existing) = running.get(&session_id) {
                    if existing.spec != spec { return Err("GRAPH_FEED_ALREADY_ACTIVE".into()); }
                    if existing.task.is_finished() { return Err("GRAPH_FEED_RESTORE_REQUIRED".into()); }
                    let cursor = service.graph_feed_cursor(session_id.clone(), spec.graph_sha256.clone()).await?;
                    return Ok(GraphFeedReply::Started {
                        session_id, graph_sha256: spec.graph_sha256, last_closed_ms: cursor.last_closed_ms,
                    });
                }
                let cursor = service.claim_graph_feed(session_id.clone(), spec.graph_sha256.clone(), timeframe_ms).await?;
                let poll_spec = spec.clone();
                let task = tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(Duration::from_millis(poll_spec.poll_every_ms));
                    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
                    loop {
                        ticker.tick().await;
                        if let Err(reason) = poll_once(&service, &writer, &worker_session, &poll_spec).await {
                            let _ = service.release_graph_feed(
                                poll_spec.session_id.clone(), poll_spec.graph_sha256.clone(), true,
                            ).await;
                            let fault = GraphFeedFault {
                                session_id: poll_spec.session_id.clone(),
                                graph_sha256: poll_spec.graph_sha256.clone(), reason,
                            };
                            let _ = tokio::time::timeout(Duration::from_millis(REQUEST_TIMEOUT_MS),
                                writer.send((WorkerFrame::GraphFeedFault { session: worker_session, fault }, None))
                            ).await;
                            break;
                        }
                    }
                });
                running.insert(session_id.clone(), RunningFeed { spec: spec.clone(), task });
                Ok(GraphFeedReply::Started {
                    session_id, graph_sha256: spec.graph_sha256, last_closed_ms: cursor.last_closed_ms,
                })
            }
            GraphFeedRequest::Stop { session_id, graph_sha256 } => {
                let mut running = self.running.lock().await;
                if let Some(existing) = running.get(&session_id) {
                    if existing.spec.graph_sha256 != graph_sha256 {
                        return Err("GRAPH_SESSION_DEFINITION_MISMATCH".into());
                    }
                }
                if let Some(existing) = running.remove(&session_id) {
                    existing.task.abort();
                    let _ = existing.task.await;
                    service.release_graph_feed(session_id.clone(), graph_sha256.clone(), false).await?;
                    let _ = writer.send((WorkerFrame::GraphFeedFault {
                        session: worker_session,
                        fault: GraphFeedFault {
                            session_id: session_id.clone(), graph_sha256: graph_sha256.clone(),
                            reason: GraphFeedFaultReason::Stopped,
                        },
                    }, None)).await;
                }
                Ok(GraphFeedReply::Stopped { session_id, graph_sha256 })
            }
        }
    }

    pub(super) async fn shutdown(&self) {
        let mut running = self.running.lock().await;
        let tasks: Vec<_> = std::mem::take(&mut *running).into_values().map(|feed| feed.task).collect();
        drop(running);
        for task in &tasks { task.abort(); }
        for task in tasks { let _ = task.await; }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poll_window_obeys_closed_time_and_64_bar_cap() {
        const M: u64 = 60_000;
        assert_eq!(poll_window(2*M, M, 3*M+20_000).unwrap(), None);
        assert_eq!(poll_window(2*M, M, 4*M+20_000).unwrap(), Some((3*M, 4*M)));
        assert_eq!(poll_window(2*M, M, 100*M).unwrap(), Some((3*M, 67*M)));
        assert_eq!(poll_window(2*M+1, M, 10*M).unwrap_err(), GraphFeedFaultReason::SessionUnavailable);
        assert_eq!(poll_window(4*M, M, 3*M).unwrap_err(), GraphFeedFaultReason::SessionUnavailable);
    }
}

//! Bounded public venue sessions shared by both Research market views.
mod book;
mod context;
#[cfg(test)]
pub(crate) mod tests;

use self::{book::Book, context::Context};
use crate::{
    independent_source::{IndependentOkxPublicSource, PublicGet},
    public_catalog,
};
use futures_util::{SinkExt, StreamExt};
use okx_domain_core::observation_model::{
    BookLevel, FundingRequirement, InstrumentType, MarketSnapshot,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::watch,
    time::Instant,
};
use tokio_tungstenite::{
    WebSocketStream, connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

pub(crate) const PUBLIC_URL: &str = "wss://ws.okx.com/ws/v5/public";
const CONNECT_BUDGET: Duration = Duration::from_secs(5);
const REQUEST_WAIT: Duration = Duration::from_secs(2);
const IDLE_BUDGET: Duration = Duration::from_secs(120);
const HEARTBEAT_AFTER: Duration = Duration::from_secs(15);
const HEARTBEAT_WAIT: Duration = Duration::from_secs(10);
const MAX_SESSIONS: usize = 2;
const MIN_CONNECT_INTERVAL: Duration = Duration::from_millis(500);
const MAX_MESSAGE_BYTES: usize = 128 * 1024;
const REFERENCE_MAX_AGE_MS: u64 = 5_000;
const BOOK_MAX_AGE_MS: u64 = 2_000;

#[derive(Default)]
pub(crate) struct PublicMarketWs {
    state: Mutex<Sessions>,
}

#[derive(Default)]
struct Sessions {
    active: BTreeMap<String, Session>,
    last_connect: Option<Instant>,
    generation: u64,
}

#[derive(Clone)]
struct Session {
    demanded: watch::Sender<Instant>,
    latest: watch::Receiver<Status>,
}

#[derive(Clone)]
enum Status {
    Warming,
    Ready {
        snapshot: Arc<Snapshot>,
        published_ms: u64,
        valid_until_ms: u64,
    },
    Failed(&'static str),
}

impl PublicMarketWs {
    fn session(
        &self,
        endpoint: &str,
        source: &Arc<dyn PublicGet>,
        instrument: &str,
    ) -> Result<Session, &'static str> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "RESEARCH_PUBLIC_WS_UNAVAILABLE")?;
        state
            .active
            .retain(|_, session| !session.demanded.is_closed());
        if let Some(session) = state.active.get(instrument) {
            return Ok(session.clone());
        }
        let now = Instant::now();
        if state.active.len() == MAX_SESSIONS
            || state
                .last_connect
                .is_some_and(|previous| now.duration_since(previous) < MIN_CONNECT_INTERVAL)
        {
            return Err("RESEARCH_PUBLIC_WS_BUSY");
        }
        state.generation = state
            .generation
            .checked_add(1)
            .ok_or("RESEARCH_PUBLIC_WS_GENERATION_EXHAUSTED")?;
        state.last_connect = Some(now);
        let generation = state.generation;
        let (demanded, demand) = watch::channel(now);
        let (publish, latest) = watch::channel(Status::Warming);
        let session = Session { demanded, latest };
        state.active.insert(instrument.to_owned(), session.clone());
        let endpoint = endpoint.to_owned();
        let source = source.clone();
        let instrument = instrument.to_owned();
        tokio::spawn(async move {
            tokio::select! {
                _ = publish.closed() => {},
                result = run_session(&endpoint, source.as_ref(), &instrument, generation, demand, &publish) => {
                    publish.send_replace(Status::Failed(result.err().unwrap_or("RESEARCH_PUBLIC_WS_DISCONNECTED")));
                }
            }
        });
        Ok(session)
    }
}

pub(crate) struct Snapshot {
    pub(crate) market: MarketSnapshot,
    pub(crate) asks: Vec<BookLevel>,
    pub(crate) bids: Vec<BookLevel>,
    pub(crate) generation: u64,
    pub(crate) book_sequence: i64,
    pub(crate) book_exchange_timestamp_ms: String,
}

pub(super) struct Reference {
    instrument: String,
    kind: InstrumentType,
    underlying: String,
    generation: String,
    funding_required: bool,
    observed_ms: u64,
}

pub(crate) async fn capture(
    source: &IndependentOkxPublicSource,
    instrument: &str,
) -> Result<Arc<Snapshot>, &'static str> {
    let endpoint = source
        .market_ws_endpoint()
        .ok_or("RESEARCH_PUBLIC_WS_UNAVAILABLE")?;
    let session = source
        .public_market_ws
        .session(endpoint, source.public_get(), instrument)?;
    session.demanded.send_replace(Instant::now());
    wait_for_snapshot(session.latest).await
}

async fn load_reference(
    source: &dyn PublicGet,
    instrument: &str,
) -> Result<Reference, &'static str> {
    let catalog = tokio::time::timeout(CONNECT_BUDGET, public_catalog::catalog(source))
        .await
        .map_err(|_| "RESEARCH_PUBLIC_WS_REFERENCE_UNAVAILABLE")?
        .map_err(|_| "RESEARCH_PUBLIC_WS_REFERENCE_UNAVAILABLE")?;
    let spec = catalog
        .instruments
        .into_iter()
        .find(|spec| spec.instrument_id == instrument && spec.state == "live")
        .ok_or("RESEARCH_PUBLIC_WS_INSTRUMENT_UNAVAILABLE")?;
    let funding_required = match spec.funding_requirement {
        FundingRequirement::Required => true,
        FundingRequirement::NotApplicable => false,
        FundingRequirement::Unknown => return Err("RESEARCH_PUBLIC_WS_FUNDING_UNKNOWN"),
    };
    Ok(Reference {
        instrument: spec.instrument_id,
        kind: spec.instrument_type,
        underlying: spec
            .underlying
            .ok_or("RESEARCH_PUBLIC_WS_REFERENCE_INVALID")?,
        generation: catalog.generation,
        funding_required,
        observed_ms: u64::try_from(catalog.observed_at.as_millisecond())
            .ok()
            .filter(|&time| time > 0)
            .ok_or("RESEARCH_PUBLIC_WS_REFERENCE_INVALID")?,
    })
}

async fn wait_for_snapshot(
    mut latest: watch::Receiver<Status>,
) -> Result<Arc<Snapshot>, &'static str> {
    tokio::time::timeout(REQUEST_WAIT, async {
        loop {
            match latest.borrow_and_update().clone() {
                Status::Ready {
                    snapshot,
                    published_ms,
                    valid_until_ms,
                } => {
                    let now = now_ms()?;
                    if (published_ms..=valid_until_ms).contains(&now) {
                        return Ok(snapshot);
                    }
                }
                Status::Failed(reason) => return Err(reason),
                Status::Warming => {}
            }
            latest
                .changed()
                .await
                .map_err(|_| "RESEARCH_PUBLIC_WS_DISCONNECTED")?;
        }
    })
    .await
    .map_err(|_| "RESEARCH_PUBLIC_WS_WARMING")?
}

async fn run_session(
    endpoint: &str,
    source: &dyn PublicGet,
    instrument: &str,
    generation: u64,
    demand: watch::Receiver<Instant>,
    publish: &watch::Sender<Status>,
) -> Result<(), &'static str> {
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_MESSAGE_BYTES));
    let (mut socket, _) = tokio::time::timeout(
        CONNECT_BUDGET,
        connect_async_with_config(endpoint, Some(config), true),
    )
    .await
    .map_err(|_| "RESEARCH_PUBLIC_WS_CONNECT_TIMEOUT")?
    .map_err(|_| "RESEARCH_PUBLIC_WS_CONNECT_FAILED")?;
    let (reference, fence) = tokio::time::timeout(
        CONNECT_BUDGET,
        subscribe_reference(&mut socket, source, instrument),
    )
    .await
    .map_err(|_| "RESEARCH_PUBLIC_WS_REFERENCE_TIMEOUT")??;
    observe(socket, &reference, generation, demand, publish, fence).await
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
enum Channel {
    Instruments,
    Tickers,
    MarkPrice,
    IndexTickers,
    OpenInterest,
    Books,
    FundingRate,
}

impl Channel {
    fn name(self) -> &'static str {
        match self {
            Self::Instruments => "instruments",
            Self::Tickers => "tickers",
            Self::MarkPrice => "mark-price",
            Self::IndexTickers => "index-tickers",
            Self::OpenInterest => "open-interest",
            Self::Books => "books",
            Self::FundingRate => "funding-rate",
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
struct Subscription {
    channel: Channel,
    #[serde(skip_serializing_if = "Option::is_none")]
    inst_type: Option<InstrumentTypeName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    inst_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    inst_family: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "UPPERCASE")]
enum InstrumentTypeName {
    Swap,
    Futures,
}

fn baseline() -> BTreeSet<Subscription> {
    [InstrumentTypeName::Swap, InstrumentTypeName::Futures]
        .into_iter()
        .map(|kind| Subscription {
            channel: Channel::Instruments,
            inst_type: Some(kind),
            inst_id: None,
            inst_family: None,
        })
        .collect()
}

fn subscriptions(reference: &Reference) -> BTreeSet<Subscription> {
    let mut requested = BTreeSet::new();
    let mut channels = vec![
        Channel::Tickers,
        Channel::MarkPrice,
        Channel::IndexTickers,
        Channel::OpenInterest,
        Channel::Books,
    ];
    if reference.funding_required {
        channels.push(Channel::FundingRate);
    }
    for channel in channels {
        requested.insert(Subscription {
            channel,
            inst_type: None,
            inst_id: Some(if channel == Channel::IndexTickers {
                reference.underlying.clone()
            } else {
                reference.instrument.clone()
            }),
            inst_family: None,
        });
    }
    requested
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Frame {
    event: Option<String>,
    arg: Option<Subscription>,
    conn_id: Option<String>,
    action: Option<String>,
    data: Option<Vec<Value>>,
}

#[derive(Default)]
struct SubscriptionFence {
    acknowledged: BTreeSet<Subscription>,
    connection_id: Option<String>,
}

impl Frame {
    fn verify(
        &self,
        requested: &BTreeSet<Subscription>,
        fence: &mut SubscriptionFence,
    ) -> Result<(), &'static str> {
        match self.event.as_deref() {
            Some("error") => return Err("RESEARCH_PUBLIC_WS_SUBSCRIPTION_REJECTED"),
            Some("notice") | Some("unsubscribe") => return Err("RESEARCH_PUBLIC_WS_DISCONNECTED"),
            Some("subscribe") | None => {}
            Some(_) => return Err("RESEARCH_PUBLIC_WS_FRAME_INVALID"),
        }
        let arg = self
            .arg
            .as_ref()
            .ok_or("RESEARCH_PUBLIC_WS_FRAME_INVALID")?;
        if !requested.contains(arg) {
            return Err("RESEARCH_PUBLIC_WS_TARGET_MISMATCH");
        }
        if let Some(id) = self.conn_id.as_ref() {
            if id.is_empty()
                || id.len() > 128
                || fence
                    .connection_id
                    .as_ref()
                    .is_some_and(|previous| previous != id)
            {
                return Err("RESEARCH_PUBLIC_WS_ACK_INVALID");
            }
            fence.connection_id.get_or_insert_with(|| id.clone());
        }
        if self.event.is_some()
            && (self.conn_id.is_none() || self.data.is_some() || self.action.is_some())
        {
            return Err("RESEARCH_PUBLIC_WS_ACK_INVALID");
        }
        Ok(())
    }
}

fn frame_from_message(message: Message) -> Result<Option<Frame>, &'static str> {
    match message {
        Message::Text(text) if text == "pong" => Ok(None),
        Message::Text(text) if text.len() <= MAX_MESSAGE_BYTES => serde_json::from_str(&text)
            .map(Some)
            .map_err(|_| "RESEARCH_PUBLIC_WS_FRAME_INVALID"),
        Message::Ping(_) | Message::Pong(_) => Ok(None),
        Message::Close(_) => Err("RESEARCH_PUBLIC_WS_DISCONNECTED"),
        _ => Err("RESEARCH_PUBLIC_WS_FRAME_INVALID"),
    }
}

async fn subscribe<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut WebSocketStream<S>,
    args: &BTreeSet<Subscription>,
) -> Result<(), &'static str> {
    socket
        .send(Message::Text(
            json!({"op":"subscribe","args":args}).to_string().into(),
        ))
        .await
        .map_err(|_| "RESEARCH_PUBLIC_WS_SEND_FAILED")
}

async fn subscribe_reference<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut WebSocketStream<S>,
    source: &dyn PublicGet,
    instrument: &str,
) -> Result<(Reference, SubscriptionFence), &'static str> {
    let mut fence = acknowledge_baseline(socket).await?;
    let desired = baseline();
    // The reference update channels cover the entire catalog read. A pending
    // update wins over accepting the REST generation.
    let reference = load_reference(source, instrument);
    tokio::pin!(reference);
    loop {
        tokio::select! {
            biased;
            result = reference_frame(socket, &desired, &mut fence) => result?,
            result = &mut reference => return Ok((result?, fence)),
        }
    }
}

async fn acknowledge_baseline<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut WebSocketStream<S>,
) -> Result<SubscriptionFence, &'static str> {
    let desired = baseline();
    let mut fence = SubscriptionFence::default();
    subscribe(socket, &desired).await?;
    while fence.acknowledged != desired {
        reference_frame(socket, &desired, &mut fence).await?;
    }
    Ok(fence)
}

async fn reference_frame<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut WebSocketStream<S>,
    desired: &BTreeSet<Subscription>,
    fence: &mut SubscriptionFence,
) -> Result<(), &'static str> {
    let message = socket
        .next()
        .await
        .ok_or("RESEARCH_PUBLIC_WS_DISCONNECTED")?
        .map_err(|_| "RESEARCH_PUBLIC_WS_READ_FAILED")?;
    if let Some(frame) = frame_from_message(message)? {
        frame.verify(desired, fence)?;
        if frame.event.is_none() {
            return Err("RESEARCH_PUBLIC_WS_REFERENCE_CHANGED");
        }
        fence
            .acknowledged
            .insert(frame.arg.ok_or("RESEARCH_PUBLIC_WS_FRAME_INVALID")?);
    }
    Ok(())
}

fn now_ms() -> Result<u64, &'static str> {
    u64::try_from(jiff::Timestamp::now().as_millisecond())
        .map_err(|_| "RESEARCH_PUBLIC_WS_CLOCK_INVALID")
}

fn fresh(observed: u64, now: u64, max_age: u64) -> bool {
    observed > 0 && now.checked_sub(observed).is_some_and(|age| age <= max_age)
}

async fn observe<S: AsyncRead + AsyncWrite + Unpin>(
    mut socket: WebSocketStream<S>,
    reference: &Reference,
    generation: u64,
    mut demand: watch::Receiver<Instant>,
    publish: &watch::Sender<Status>,
    mut fence: SubscriptionFence,
) -> Result<(), &'static str> {
    let mut requested = subscriptions(reference);
    tokio::time::timeout(CONNECT_BUDGET, subscribe(&mut socket, &requested))
        .await
        .map_err(|_| "RESEARCH_PUBLIC_WS_ACK_TIMEOUT")??;
    requested.extend(baseline());

    let mut book = Book::default();
    let mut book_received_ms = 0;
    let mut context = Context::default();
    let ack_deadline = Instant::now() + CONNECT_BUDGET;
    let mut heartbeat_at = Instant::now() + HEARTBEAT_AFTER;
    let mut awaiting_pong = false;
    loop {
        let idle_at = *demand.borrow() + IDLE_BUDGET;
        let message = tokio::select! {
            _ = tokio::time::sleep_until(idle_at) => {
                if Instant::now() >= *demand.borrow() + IDLE_BUDGET {
                    return Err("RESEARCH_PUBLIC_WS_IDLE");
                }
                continue;
            }
            change = demand.changed() => {
                change.map_err(|_| "RESEARCH_PUBLIC_WS_IDLE")?;
                continue;
            }
            _ = tokio::time::sleep_until(ack_deadline), if fence.acknowledged != requested => {
                return Err("RESEARCH_PUBLIC_WS_ACK_TIMEOUT");
            }
            _ = tokio::time::sleep_until(heartbeat_at) => {
                if awaiting_pong {
                    return Err("RESEARCH_PUBLIC_WS_HEARTBEAT_TIMEOUT");
                }
                tokio::time::timeout(HEARTBEAT_WAIT, socket.send(Message::Text("ping".into())))
                    .await.map_err(|_| "RESEARCH_PUBLIC_WS_HEARTBEAT_TIMEOUT")?
                    .map_err(|_| "RESEARCH_PUBLIC_WS_SEND_FAILED")?;
                awaiting_pong = true;
                heartbeat_at = Instant::now() + HEARTBEAT_WAIT;
                continue;
            }
            next = socket.next() => next.ok_or("RESEARCH_PUBLIC_WS_DISCONNECTED")?
                .map_err(|_| "RESEARCH_PUBLIC_WS_READ_FAILED")?,
        };
        awaiting_pong = false;
        heartbeat_at = Instant::now() + HEARTBEAT_AFTER;
        let Some(frame) = frame_from_message(message)? else {
            continue;
        };
        frame.verify(&requested, &mut fence)?;
        let arg = frame.arg.ok_or("RESEARCH_PUBLIC_WS_FRAME_INVALID")?;
        let received_ms = now_ms()?;
        if frame.event.is_some() {
            if fence.acknowledged != requested {
                if !fresh(reference.observed_ms, received_ms, REFERENCE_MAX_AGE_MS) {
                    return Err("RESEARCH_PUBLIC_WS_REFERENCE_STALE");
                }
                fence.acknowledged.insert(arg);
            }
        } else {
            if arg.channel == Channel::Instruments {
                // An update can invalidate the full REST reference generation.
                return Err("RESEARCH_PUBLIC_WS_REFERENCE_CHANGED");
            }
            let mut data = frame.data.ok_or("RESEARCH_PUBLIC_WS_FRAME_INVALID")?;
            if data.len() != 1 {
                return Err("RESEARCH_PUBLIC_WS_FRAME_INVALID");
            }
            let row = data.pop().expect("checked one row");
            if arg.channel == Channel::Books {
                book.apply(
                    frame
                        .action
                        .as_deref()
                        .ok_or("RESEARCH_PUBLIC_WS_FRAME_INVALID")?,
                    row,
                )?;
                book_received_ms = received_ms;
            } else {
                if row.get("instId").and_then(Value::as_str) != arg.inst_id.as_deref() {
                    return Err("RESEARCH_PUBLIC_WS_TARGET_MISMATCH");
                }
                context.apply(arg.channel.name(), row, received_ms)?;
            }
        }

        let now = now_ms()?;
        if fence.acknowledged == requested
            && fresh(book_received_ms, now, BOOK_MAX_AGE_MS)
            && let Some(book) = book.snapshot()
            && let Ok(exchange_ms) = book.exchange_timestamp_ms.parse::<u64>()
            && fresh(exchange_ms, now, BOOK_MAX_AGE_MS)
            && let Some(market) = context.snapshot(reference, now)?
            && let Some(context_until) = context.valid_until_ms(reference)
        {
            let book_until = book_received_ms
                .min(exchange_ms)
                .saturating_add(BOOK_MAX_AGE_MS);
            publish.send_replace(Status::Ready {
                snapshot: Arc::new(Snapshot {
                    market,
                    asks: book.asks,
                    bids: book.bids,
                    generation,
                    book_sequence: book.sequence,
                    book_exchange_timestamp_ms: book.exchange_timestamp_ms,
                }),
                published_ms: now,
                valid_until_ms: book_until.min(context_until),
            });
        } else {
            publish.send_if_modified(|current| {
                if matches!(current, Status::Ready { .. }) {
                    *current = Status::Warming;
                    true
                } else {
                    false
                }
            });
        }
    }
}

#[cfg(test)]
async fn read_snapshot<S: AsyncRead + AsyncWrite + Unpin>(
    mut socket: WebSocketStream<S>,
    reference: &Reference,
    generation: u64,
) -> Result<Arc<Snapshot>, &'static str> {
    let (_demanded, demand) = watch::channel(Instant::now());
    let (publish, latest) = watch::channel(Status::Warming);
    tokio::select! {
        result = wait_for_snapshot(latest) => result,
        result = async {
            let fence = acknowledge_baseline(&mut socket).await?;
            observe(socket, reference, generation, demand, &publish, fence).await
        } => {
            Err(result.err().unwrap_or("RESEARCH_PUBLIC_WS_DISCONNECTED"))
        },
    }
}

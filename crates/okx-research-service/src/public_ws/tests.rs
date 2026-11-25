use super::*;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, protocol::Role},
};

async fn send<S: AsyncRead + AsyncWrite + Unpin>(socket: &mut WebSocketStream<S>, value: Value) {
    let _ = socket.send(Message::Text(value.to_string().into())).await;
}

/// Public protocol fixture usable by the Edge loopback tests as well as these unit tests.
pub(crate) async fn serve_market_fixture<S: AsyncRead + AsyncWrite + Unpin>(
    socket: WebSocketStream<S>,
    fault: Option<&str>,
) {
    serve_market_fixture_after(socket, fault, None).await;
}

pub(crate) async fn serve_market_fixture_after<S: AsyncRead + AsyncWrite + Unpin>(
    mut socket: WebSocketStream<S>,
    fault: Option<&str>,
    allow_market: Option<tokio::sync::oneshot::Receiver<()>>,
) {
    let message = socket.next().await.expect("subscription").expect("socket");
    let request: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    assert_eq!(request["op"], "subscribe");
    let first_args = request["args"].as_array().unwrap().clone();
    let baseline: Vec<Value> = first_args
        .iter()
        .filter(|arg| arg["channel"] == "instruments")
        .cloned()
        .collect();
    let args = if first_args.iter().all(|arg| arg["channel"] == "instruments") {
        for arg in &baseline {
            send(
                &mut socket,
                json!({"event":"subscribe", "arg":arg, "connId":"fixture"}),
            )
            .await;
        }
        if fault == Some("reference_handoff") {
            if let Some(arg) = baseline.first() {
                send(&mut socket, json!({"arg":arg,"data":[{"instId":"BTC-USDT-SWAP","instType":"SWAP","state":"suspend"}]})).await;
            }
            return;
        }
        let message = socket
            .next()
            .await
            .expect("market subscription")
            .expect("socket");
        let request: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
        assert_eq!(request["op"], "subscribe");
        request["args"].as_array().unwrap().clone()
    } else {
        first_args
    };
    if let Some(allow_market) = allow_market {
        let _ = allow_market.await;
    }
    let now = jiff::Timestamp::now().as_millisecond().to_string();
    // Keep the final ACK outstanding while all data and any book update arrive.
    for arg in args.iter().take(args.len() - 1) {
        send(
            &mut socket,
            json!({"event":"subscribe", "arg":arg, "connId":"fixture"}),
        )
        .await;
    }
    for arg in &args {
        let channel = arg["channel"].as_str().unwrap();
        let id = arg["instId"].as_str().unwrap_or("BTC-USDT-SWAP");
        let mut row = match channel {
            "tickers" => {
                json!({"instId":id,"last":"100","lastSz":"1","askPx":"101","askSz":"1","bidPx":"99","bidSz":"1","ts":now})
            }
            "mark-price" => json!({"instId":id,"markPx":"100","ts":now}),
            "index-tickers" => json!({"instId":id,"idxPx":"100","ts":now}),
            "open-interest" => json!({"instId":id,"oi":"10","ts":now}),
            "funding-rate" => {
                json!({"instId":id,"fundingRate":"0.001","fundingTime":"2000000000000","nextFundingTime":"2000001000000","ts":now})
            }
            "books" => {
                json!({"seqId":10,"prevSeqId":-1,"ts":now,"asks":[["101","2","0","1"]],"bids":[["99","2","0","1"]]})
            }
            _ => continue,
        };
        if fault == Some("wrong_row") && channel == "tickers" {
            row["instId"] = json!("ETH-USDT-SWAP");
        }
        let mut target = arg.clone();
        if fault == Some("wrong_instrument") && channel == "tickers" {
            target["instId"] = json!("ETH-USDT-SWAP");
        }
        let mut frame = json!({"arg":target,"data":[row]});
        if channel == "books" {
            frame["action"] = json!("snapshot");
        }
        send(&mut socket, frame).await;
        if channel == "books" {
            let previous = if fault == Some("gap") { 9 } else { 10 };
            send(&mut socket, json!({"arg":arg,"action":"update","data":[{"seqId":11,"prevSeqId":previous,"ts":now,"asks":[["101","3","0","1"]],"bids":[]}]})).await;
        }
    }
    if fault == Some("revocation")
        && let Some(arg) = baseline.first()
    {
        send(&mut socket, json!({"arg":arg,"data":[{"instId":"BTC-USDT-SWAP","instType":"SWAP","state":"suspend"}]})).await;
    }
    if fault != Some("missing_ack")
        && let Some(arg) = args.last()
    {
        send(
            &mut socket,
            json!({"event":"subscribe","arg":arg,"connId":"fixture"}),
        )
        .await;
    }
    if fault.is_none() {
        // Keep a healthy warm session alive until its peer closes.
        while let Some(Ok(message)) = socket.next().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    }
}

fn reference() -> Reference {
    Reference {
        instrument: "BTC-USDT-SWAP".into(),
        kind: okx_research_domain::observation_model::InstrumentType::Swap,
        underlying: "BTC-USDT".into(),
        generation: "ref".into(),
        funding_required: true,
        observed_ms: jiff::Timestamp::now().as_millisecond() as u64,
    }
}

async fn exercise_with_reference(
    fault: Option<&'static str>,
    reference: Reference,
) -> Result<Arc<Snapshot>, &'static str> {
    let (client, server) = tokio::io::duplex(65536);
    let client = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
    let server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
    let task = tokio::spawn(serve_market_fixture(server, fault));
    let result = read_snapshot(client, &reference, 42).await;
    task.await.unwrap();
    result
}

async fn exercise(fault: Option<&'static str>) -> Result<Arc<Snapshot>, &'static str> {
    exercise_with_reference(fault, reference()).await
}

#[tokio::test]
async fn full_context_late_ack_and_linked_update() {
    let snapshot = exercise(None).await.expect("complete snapshot");
    assert_eq!(snapshot.generation, 42);
    assert_eq!(snapshot.book_sequence, 11);
    assert_eq!(snapshot.asks[0].size, "3");
    assert_eq!(snapshot.market.instrument_id, "BTC-USDT-SWAP");
    assert!(snapshot.market.funding.is_some());
}

#[tokio::test]
async fn rejects_sequence_gap_and_wrong_instrument() {
    assert_eq!(
        exercise(Some("gap")).await.err(),
        Some("BOOK_SEQUENCE_INVALID")
    );
    assert_eq!(
        exercise(Some("wrong_instrument")).await.err(),
        Some("RESEARCH_PUBLIC_WS_TARGET_MISMATCH")
    );
    assert_eq!(
        exercise(Some("wrong_row")).await.err(),
        Some("RESEARCH_PUBLIC_WS_TARGET_MISMATCH")
    );
    assert!(exercise(Some("missing_ack")).await.is_err());
}

#[tokio::test]
async fn instrument_revocation_prevents_snapshot() {
    assert_eq!(
        exercise(Some("revocation")).await.err(),
        Some("RESEARCH_PUBLIC_WS_REFERENCE_CHANGED")
    );
}

#[tokio::test]
async fn funding_not_required_does_not_block_snapshot() {
    let mut reference = reference();
    reference.funding_required = false;
    let snapshot = exercise_with_reference(None, reference)
        .await
        .expect("complete snapshot");
    assert!(snapshot.market.funding.is_none());
}

use super::*;
use futures_util::Sink;
use serde_json::json;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::AsyncWriteExt as _;
use tokio_tungstenite::tungstenite::{Error as WsError, protocol::Role};

fn response_with_blob(size: usize) -> AgentResponse {
    response_for(
        "research_request_0123456789",
        Ok(json!({ "blob": "x".repeat(size) })),
    )
}

#[test]
fn stable_research_edge_session_resets_connection_backoff() {
    let mut failures = 0;
    for expected in [1, 3, 10, 30, 60, 60] {
        assert_eq!(
            reconnect_delay(&mut failures, Duration::from_secs(0)),
            Duration::from_secs(expected)
        );
    }
    assert_eq!(failures, 4);
    assert_eq!(
        reconnect_delay(&mut failures, Duration::from_secs(59)),
        Duration::from_secs(60)
    );
    assert_eq!(
        reconnect_delay(&mut failures, Duration::from_secs(60)),
        Duration::from_secs(1)
    );
    assert_eq!(
        reconnect_delay(&mut failures, Duration::from_secs(1)),
        Duration::from_secs(3)
    );
}

#[test]
fn research_edge_decoder_and_writer_use_the_same_finite_frame_budget() {
    let config = edge_websocket_config();
    assert_eq!(FRAME_LIMIT, 64 * 1024);
    assert_eq!(config.max_message_size, Some(FRAME_LIMIT));
    assert_eq!(config.max_frame_size, Some(FRAME_LIMIT));
    assert!(config.max_write_buffer_size >= config.write_buffer_size + FRAME_LIMIT);
    assert!(config.max_write_buffer_size < usize::MAX);

    let mut writer = BoundedFrameWriter::new();
    writer.write_all(&vec![b'x'; FRAME_LIMIT]).unwrap();
    assert!(writer.write_all(b"x").is_err());
    assert!(writer.oversized);
    assert_eq!(writer.bytes.len(), FRAME_LIMIT);
}

#[test]
fn bounded_result_encoding_preserves_small_responses_and_downgrades_huge_results() {
    let session = "session_abcdefghijklmnop";
    let id = "research_request_0123456789";
    let small = OutgoingFrame::Response {
        schema: SCHEMA.into(),
        session_id: session.into(),
        connection_generation: 1,
        response: response_with_blob(100),
    };
    assert_eq!(
        encode_frame(&small).unwrap(),
        serde_json::to_string(&small).unwrap()
    );
    let oversized = OutgoingFrame::Response {
        schema: SCHEMA.into(),
        session_id: session.into(),
        connection_generation: 1,
        response: response_with_blob(4 * FRAME_LIMIT),
    };
    assert_eq!(
        encode_frame(&oversized).unwrap_err(),
        "RESEARCH_EDGE_RESPONSE_OVERSIZED"
    );
    let bounded =
        encode_result_response(session, 1, id, response_with_blob(4 * FRAME_LIMIT)).unwrap();
    assert!(bounded.len() <= FRAME_LIMIT);
    let packet: serde_json::Value = serde_json::from_str(&bounded).unwrap();
    assert_eq!(packet["type"], "response");
    assert_eq!(packet["response"]["request_id"], id);
    assert_eq!(packet["response"]["status"], "failed");
    assert_eq!(
        packet["response"]["failure"]["code"],
        "RESEARCH_RESULT_TOO_LARGE"
    );
    assert!(packet["response"]["result"].is_null());
}

struct StalledSink;

impl Sink<Message> for StalledSink {
    type Error = std::io::Error;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }
    fn start_send(self: Pin<&mut Self>, _: Message) -> Result<(), Self::Error> {
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

struct FailedSink;

impl Sink<Message> for FailedSink {
    type Error = std::io::Error;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe)))
    }
    fn start_send(self: Pin<&mut Self>, _: Message) -> Result<(), Self::Error> {
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn permanently_blocked_and_broken_wss_sinks_fail_with_bounded_errors() {
    assert_eq!(
        send_encoded(&mut StalledSink, "{}".into(), Duration::from_millis(10))
            .await
            .unwrap_err(),
        "RESEARCH_EDGE_WRITE_TIMEOUT"
    );
    assert_eq!(
        send_encoded(&mut FailedSink, "{}".into(), EDGE_WRITE_DEADLINE)
            .await
            .unwrap_err(),
        "RESEARCH_EDGE_WRITE_FAILED"
    );
}

fn raw_server_text_frame(fin: bool, opcode: u8, body: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(body.len() + 10);
    frame.push((if fin { 0x80 } else { 0 }) | opcode);
    if body.len() <= 125 {
        frame.push(body.len() as u8);
    } else if body.len() <= u16::MAX as usize {
        frame.push(126);
        frame.extend_from_slice(&(body.len() as u16).to_be_bytes());
    } else {
        frame.push(127);
        frame.extend_from_slice(&(body.len() as u64).to_be_bytes());
    }
    frame.extend_from_slice(body);
    frame
}

async fn receive_raw_server_frames(raw: Vec<u8>) -> Result<Message, WsError> {
    let (reader, mut writer) = tokio::io::duplex(2 * FRAME_LIMIT + 128);
    let mut socket =
        WebSocketStream::from_raw_socket(reader, Role::Client, Some(edge_websocket_config())).await;
    writer.write_all(&raw).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), socket.next())
        .await
        .expect("finite decoder must complete")
        .expect("server sent a frame")
}

#[tokio::test]
async fn actual_websocket_decoder_rejects_oversized_single_and_fragmented_messages() {
    let at_limit = raw_server_text_frame(true, 0x1, &vec![b'x'; FRAME_LIMIT]);
    assert_eq!(
        receive_raw_server_frames(at_limit)
            .await
            .unwrap()
            .into_text()
            .unwrap()
            .len(),
        FRAME_LIMIT
    );

    let too_long = raw_server_text_frame(true, 0x1, &vec![b'x'; FRAME_LIMIT + 1]);
    assert!(matches!(
        receive_raw_server_frames(too_long).await,
        Err(WsError::Capacity(_))
    ));

    // Each individual fragment is below 64 KiB, but the aggregate must
    // still be rejected before allocating an oversized complete message.
    let mut fragmented = raw_server_text_frame(false, 0x1, &vec![b'x'; 40 * 1024]);
    fragmented.extend(raw_server_text_frame(true, 0x0, &vec![b'x'; 30 * 1024]));
    assert!(matches!(
        receive_raw_server_frames(fragmented).await,
        Err(WsError::Capacity(_))
    ));
}

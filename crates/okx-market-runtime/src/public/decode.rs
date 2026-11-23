use std::time::{SystemTime, UNIX_EPOCH};

use okx_observation::{BookLevelUpdate, OrderBookMessage};
use serde::Deserialize;

use super::PublicRuntimeError;

#[derive(Debug, Deserialize)]
pub(super) struct RawBookData {
    asks: Vec<[String; 4]>,
    bids: Vec<[String; 4]>,
    ts: String,
    #[serde(rename = "seqId")]
    seq_id: i64,
    #[serde(rename = "prevSeqId")]
    prev_seq_id: i64,
}

pub(super) fn decode_book(raw: RawBookData) -> OrderBookMessage {
    OrderBookMessage {
        asks: raw.asks.into_iter().map(decode_level).collect(),
        bids: raw.bids.into_iter().map(decode_level).collect(),
        exchange_timestamp_ms: raw.ts,
        seq_id: raw.seq_id,
        prev_seq_id: raw.prev_seq_id,
    }
}

fn decode_level(row: [String; 4]) -> BookLevelUpdate {
    let [price, size, _deprecated, order_count] = row;
    BookLevelUpdate {
        price,
        size,
        order_count: Some(order_count),
    }
}

pub(super) fn now_ms() -> Result<u64, PublicRuntimeError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PublicRuntimeError::ClockBeforeEpoch)?
        .as_millis();
    Ok(millis.min(u64::MAX as u128) as u64)
}

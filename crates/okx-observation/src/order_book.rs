use std::{collections::BTreeMap, str::FromStr};

use rust_decimal::Decimal;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookLevelUpdate {
    pub price: String,
    pub size: String,
    pub order_count: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBookMessage {
    pub asks: Vec<BookLevelUpdate>,
    pub bids: Vec<BookLevelUpdate>,
    pub exchange_timestamp_ms: String,
    pub seq_id: i64,
    pub prev_seq_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BookLevel {
    pub price: String,
    pub size: String,
    pub order_count: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderBookStatus {
    WaitingSnapshot,
    Contiguous,
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OrderBookSnapshot {
    pub generation: u64,
    pub status: OrderBookStatus,
    pub seq_id: Option<i64>,
    pub exchange_timestamp_ms: Option<String>,
    pub asks: Vec<BookLevel>,
    pub bids: Vec<BookLevel>,
}

#[derive(Debug)]
pub struct OrderBookState {
    generation: u64,
    status: OrderBookStatus,
    last_seq_id: Option<i64>,
    exchange_timestamp_ms: Option<String>,
    asks: BTreeMap<Decimal, BookLevel>,
    bids: BTreeMap<Decimal, BookLevel>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OrderBookError {
    #[error("order-book snapshot must have prevSeqId=-1, got {0}")]
    InvalidSnapshotPrevSeqId(i64),

    #[error("order-book update arrived before a contiguous snapshot")]
    NotContiguous,

    #[error("order-book update generation {actual} does not match current generation {expected}")]
    GenerationMismatch { expected: u64, actual: u64 },

    #[error("order-book sequence gap: expected prevSeqId={expected}, got {actual}")]
    SequenceGap { expected: i64, actual: i64 },

    #[error("order-book field '{field}' is invalid: '{value}'")]
    InvalidDecimal { field: &'static str, value: String },

    #[error("order-book size must not be negative: '{0}'")]
    NegativeSize(String),

    #[error("order-book price must be positive: '{0}'")]
    NonPositivePrice(String),
}

impl OrderBookState {
    pub fn waiting(generation: u64) -> Self {
        Self {
            generation,
            status: OrderBookStatus::WaitingSnapshot,
            last_seq_id: None,
            exchange_timestamp_ms: None,
            asks: BTreeMap::new(),
            bids: BTreeMap::new(),
        }
    }

    pub fn apply_snapshot(
        &mut self,
        generation: u64,
        message: OrderBookMessage,
    ) -> Result<(), OrderBookError> {
        if message.prev_seq_id != -1 {
            self.invalidate();
            return Err(OrderBookError::InvalidSnapshotPrevSeqId(
                message.prev_seq_id,
            ));
        }

        let normalized = levels_from_snapshot(message.asks)
            .and_then(|asks| levels_from_snapshot(message.bids).map(|bids| (asks, bids)));
        let (asks, bids) = match normalized {
            Ok(levels) => levels,
            Err(error) => {
                self.invalidate();
                return Err(error);
            }
        };

        self.generation = generation;
        self.status = OrderBookStatus::Contiguous;
        self.last_seq_id = Some(message.seq_id);
        self.exchange_timestamp_ms = Some(message.exchange_timestamp_ms);
        self.asks = asks;
        self.bids = bids;
        Ok(())
    }

    pub fn apply_update(
        &mut self,
        generation: u64,
        message: OrderBookMessage,
    ) -> Result<(), OrderBookError> {
        if generation != self.generation {
            self.invalidate();
            return Err(OrderBookError::GenerationMismatch {
                expected: self.generation,
                actual: generation,
            });
        }
        if self.status != OrderBookStatus::Contiguous {
            return Err(OrderBookError::NotContiguous);
        }

        let expected = self.last_seq_id.ok_or(OrderBookError::NotContiguous)?;
        if message.prev_seq_id != expected {
            self.invalidate();
            return Err(OrderBookError::SequenceGap {
                expected,
                actual: message.prev_seq_id,
            });
        }

        let mut next_asks = self.asks.clone();
        let mut next_bids = self.bids.clone();
        if let Err(error) = apply_levels(&mut next_asks, message.asks)
            .and_then(|()| apply_levels(&mut next_bids, message.bids))
        {
            self.invalidate();
            return Err(error);
        }

        self.asks = next_asks;
        self.bids = next_bids;
        self.last_seq_id = Some(message.seq_id);
        self.exchange_timestamp_ms = Some(message.exchange_timestamp_ms);
        Ok(())
    }

    pub fn invalidate(&mut self) {
        self.status = OrderBookStatus::Invalid;
        self.last_seq_id = None;
        self.exchange_timestamp_ms = None;
        self.asks.clear();
        self.bids.clear();
    }

    pub fn status(&self) -> OrderBookStatus {
        self.status
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn last_seq_id(&self) -> Option<i64> {
        self.last_seq_id
    }

    pub fn snapshot(&self) -> OrderBookSnapshot {
        OrderBookSnapshot {
            generation: self.generation,
            status: self.status,
            seq_id: self.last_seq_id,
            exchange_timestamp_ms: self.exchange_timestamp_ms.clone(),
            asks: self.asks.values().cloned().collect(),
            bids: self.bids.values().rev().cloned().collect(),
        }
    }
}

fn levels_from_snapshot(
    levels: Vec<BookLevelUpdate>,
) -> Result<BTreeMap<Decimal, BookLevel>, OrderBookError> {
    let mut result = BTreeMap::new();
    apply_levels(&mut result, levels)?;
    Ok(result)
}

fn apply_levels(
    side: &mut BTreeMap<Decimal, BookLevel>,
    levels: Vec<BookLevelUpdate>,
) -> Result<(), OrderBookError> {
    for update in levels {
        let price = decimal("price", &update.price)?;
        if price <= Decimal::ZERO {
            return Err(OrderBookError::NonPositivePrice(update.price));
        }
        let size = decimal("size", &update.size)?;
        if size < Decimal::ZERO {
            return Err(OrderBookError::NegativeSize(update.size));
        }

        if size == Decimal::ZERO {
            side.remove(&price);
        } else {
            side.insert(
                price,
                BookLevel {
                    price: update.price,
                    size: update.size,
                    order_count: update.order_count,
                },
            );
        }
    }
    Ok(())
}

fn decimal(field: &'static str, value: &str) -> Result<Decimal, OrderBookError> {
    Decimal::from_str(value).map_err(|_| OrderBookError::InvalidDecimal {
        field,
        value: value.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn level(price: &str, size: &str) -> BookLevelUpdate {
        BookLevelUpdate {
            price: price.to_owned(),
            size: size.to_owned(),
            order_count: Some("1".to_owned()),
        }
    }

    fn snapshot(seq_id: i64) -> OrderBookMessage {
        OrderBookMessage {
            asks: vec![level("0.130", "20"), level("0.125", "10")],
            bids: vec![level("0.120", "15"), level("0.115", "30")],
            exchange_timestamp_ms: "1790467200000".to_owned(),
            seq_id,
            prev_seq_id: -1,
        }
    }

    #[test]
    fn snapshot_is_sorted_by_decimal_price_not_string_order() {
        let mut state = OrderBookState::waiting(7);
        let mut data = snapshot(10);
        data.asks = vec![level("10", "1"), level("2", "1")];
        data.bids = vec![level("2", "1"), level("10", "1")];

        state.apply_snapshot(7, data).expect("snapshot");
        let published = state.snapshot();

        assert_eq!(published.asks[0].price, "2");
        assert_eq!(published.asks[1].price, "10");
        assert_eq!(published.bids[0].price, "10");
        assert_eq!(published.bids[1].price, "2");
    }

    #[test]
    fn malformed_replacement_snapshot_revokes_previous_continuity() {
        let mut state = OrderBookState::waiting(7);
        state.apply_snapshot(7, snapshot(10)).expect("snapshot");

        let mut malformed = snapshot(20);
        malformed.bids = vec![level("not-a-price", "1")];
        let error = state
            .apply_snapshot(7, malformed)
            .expect_err("malformed snapshot");

        assert!(matches!(error, OrderBookError::InvalidDecimal { .. }));
        let published = state.snapshot();
        assert_eq!(published.status, OrderBookStatus::Invalid);
        assert_eq!(published.seq_id, None);
        assert_eq!(published.exchange_timestamp_ms, None);
        assert!(published.asks.is_empty());
        assert!(published.bids.is_empty());
    }

    #[test]
    fn update_applies_replace_insert_and_zero_size_delete() {
        let mut state = OrderBookState::waiting(7);
        state.apply_snapshot(7, snapshot(10)).expect("snapshot");

        state
            .apply_update(
                7,
                OrderBookMessage {
                    asks: vec![level("0.125", "0"), level("0.126", "12")],
                    bids: vec![level("0.120", "18")],
                    exchange_timestamp_ms: "1790467200100".to_owned(),
                    seq_id: 11,
                    prev_seq_id: 10,
                },
            )
            .expect("update");

        let published = state.snapshot();
        assert_eq!(published.seq_id, Some(11));
        assert_eq!(published.asks[0].price, "0.126");
        assert_eq!(published.bids[0].size, "18");
    }

    #[test]
    fn linkage_gap_invalidates_book_immediately() {
        let mut state = OrderBookState::waiting(7);
        state.apply_snapshot(7, snapshot(10)).expect("snapshot");

        let error = state
            .apply_update(
                7,
                OrderBookMessage {
                    asks: vec![],
                    bids: vec![],
                    exchange_timestamp_ms: "1790467200100".to_owned(),
                    seq_id: 12,
                    prev_seq_id: 9,
                },
            )
            .expect_err("gap");

        assert_eq!(
            error,
            OrderBookError::SequenceGap {
                expected: 10,
                actual: 9
            }
        );
        assert_eq!(state.status(), OrderBookStatus::Invalid);
        assert!(state.snapshot().asks.is_empty());
        assert!(state.snapshot().bids.is_empty());
    }

    #[test]
    fn malformed_update_is_atomic_and_invalidates_book() {
        let mut state = OrderBookState::waiting(7);
        state.apply_snapshot(7, snapshot(10)).expect("snapshot");

        let error = state
            .apply_update(
                7,
                OrderBookMessage {
                    asks: vec![level("0.125", "0"), level("0.126", "12")],
                    bids: vec![level("not-a-price", "18")],
                    exchange_timestamp_ms: "1790467200100".to_owned(),
                    seq_id: 11,
                    prev_seq_id: 10,
                },
            )
            .expect_err("malformed update");

        assert!(matches!(error, OrderBookError::InvalidDecimal { .. }));
        let published = state.snapshot();
        assert_eq!(published.status, OrderBookStatus::Invalid);
        assert_eq!(published.exchange_timestamp_ms, None);
        assert!(published.asks.is_empty());
        assert!(published.bids.is_empty());
    }

    #[test]
    fn same_sequence_keepalive_and_sequence_reset_are_valid_when_linked() {
        let mut state = OrderBookState::waiting(7);
        state.apply_snapshot(7, snapshot(10)).expect("snapshot");

        state
            .apply_update(
                7,
                OrderBookMessage {
                    asks: vec![],
                    bids: vec![],
                    exchange_timestamp_ms: "1790467260000".to_owned(),
                    seq_id: 10,
                    prev_seq_id: 10,
                },
            )
            .expect("keepalive");

        state
            .apply_update(
                7,
                OrderBookMessage {
                    asks: vec![level("0.124", "2")],
                    bids: vec![],
                    exchange_timestamp_ms: "1790467260100".to_owned(),
                    seq_id: 3,
                    prev_seq_id: 10,
                },
            )
            .expect("documented sequence reset");

        assert_eq!(state.last_seq_id(), Some(3));
        assert_eq!(state.status(), OrderBookStatus::Contiguous);
    }

    #[test]
    fn reconnect_generation_requires_new_snapshot() {
        let mut state = OrderBookState::waiting(7);
        state.apply_snapshot(7, snapshot(10)).expect("snapshot");

        let error = state
            .apply_update(
                8,
                OrderBookMessage {
                    asks: vec![],
                    bids: vec![],
                    exchange_timestamp_ms: "1790467200100".to_owned(),
                    seq_id: 11,
                    prev_seq_id: 10,
                },
            )
            .expect_err("generation mismatch");

        assert_eq!(
            error,
            OrderBookError::GenerationMismatch {
                expected: 7,
                actual: 8
            }
        );
        assert_eq!(state.status(), OrderBookStatus::Invalid);

        state.apply_snapshot(8, snapshot(1)).expect("new snapshot");
        assert_eq!(state.generation(), 8);
        assert_eq!(state.status(), OrderBookStatus::Contiguous);
    }
}

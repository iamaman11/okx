use std::collections::{BTreeMap, BTreeSet};

use okx_domain_core::observation_model::BookLevel;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

const DEPTH: usize = 400;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct VenueBook {
    seq_id: i64,
    prev_seq_id: i64,
    ts: String,
    asks: Vec<[String; 4]>,
    bids: Vec<[String; 4]>,
}

#[derive(Default)]
pub(super) struct Book {
    asks: BTreeMap<Decimal, BookLevel>,
    bids: BTreeMap<Decimal, BookLevel>,
    sequence: Option<i64>,
    exchange_timestamp_ms: String,
}

pub(super) struct BookSnapshot {
    pub(super) asks: Vec<BookLevel>,
    pub(super) bids: Vec<BookLevel>,
    pub(super) sequence: i64,
    pub(super) exchange_timestamp_ms: String,
}

impl Book {
    pub(super) fn apply(&mut self, action: &str, row: Value) -> Result<(), &'static str> {
        let result = self.apply_inner(action, row);
        if result.is_err() {
            *self = Self::default();
        }
        result
    }

    fn apply_inner(&mut self, action: &str, row: Value) -> Result<(), &'static str> {
        let row: VenueBook = serde_json::from_value(row).map_err(|_| "BOOK_LEVEL_INVALID")?;
        let seq = row.seq_id;
        let prev = row.prev_seq_id;
        if seq < 0 {
            return Err("BOOK_SEQUENCE_INVALID");
        }
        if row.ts.is_empty()
            || !row.ts.bytes().all(|b| b.is_ascii_digit())
            || row.ts.parse::<u64>().is_err()
        {
            return Err("BOOK_TIMESTAMP_INVALID");
        }
        if row.asks.len() > DEPTH || row.bids.len() > DEPTH {
            return Err("BOOK_LEVEL_INVALID");
        }
        let (mut next_asks, mut next_bids) = match action {
            "snapshot" if prev == -1 => (BTreeMap::new(), BTreeMap::new()),
            "update" if self.sequence == Some(prev) => {
                if seq == prev && (!row.asks.is_empty() || !row.bids.is_empty()) {
                    return Err("BOOK_SEQUENCE_INVALID");
                }
                (
                    std::mem::take(&mut self.asks),
                    std::mem::take(&mut self.bids),
                )
            }
            _ => return Err("BOOK_SEQUENCE_INVALID"),
        };
        apply_side(&mut next_asks, &row.asks)?;
        apply_side(&mut next_bids, &row.bids)?;
        while next_asks.len() > DEPTH {
            next_asks.pop_last();
        }
        while next_bids.len() > DEPTH {
            next_bids.pop_first();
        }
        let best_ask = next_asks.first_key_value().ok_or("BOOK_EMPTY")?.0;
        let best_bid = next_bids.last_key_value().ok_or("BOOK_EMPTY")?.0;
        if best_bid >= best_ask {
            return Err("BOOK_CROSSED");
        }
        self.asks = next_asks;
        self.bids = next_bids;
        self.sequence = Some(seq);
        self.exchange_timestamp_ms = row.ts;
        Ok(())
    }

    pub(super) fn snapshot(&self) -> Option<BookSnapshot> {
        Some(BookSnapshot {
            asks: self.asks.values().take(10).cloned().collect(),
            bids: self.bids.values().rev().take(10).cloned().collect(),
            sequence: self.sequence?,
            exchange_timestamp_ms: self.exchange_timestamp_ms.clone(),
        })
    }
}

fn apply_side(
    side: &mut BTreeMap<Decimal, BookLevel>,
    rows: &[[String; 4]],
) -> Result<(), &'static str> {
    let mut seen = BTreeSet::new();
    for [price_text, size_text, reserved, count] in rows {
        let price = Decimal::from_str_exact(price_text).map_err(|_| "BOOK_LEVEL_INVALID")?;
        let size = Decimal::from_str_exact(size_text).map_err(|_| "BOOK_LEVEL_INVALID")?;
        let order_count = count.parse::<u64>().map_err(|_| "BOOK_LEVEL_INVALID")?;
        if price <= Decimal::ZERO
            || size < Decimal::ZERO
            || reserved != "0"
            || (size > Decimal::ZERO && order_count == 0)
            || !seen.insert(price)
        {
            return Err("BOOK_LEVEL_INVALID");
        }
        if size == Decimal::ZERO {
            side.remove(&price);
        } else {
            side.insert(
                price,
                BookLevel {
                    price: price_text.clone(),
                    size: size_text.clone(),
                    order_count: Some(count.clone()),
                },
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(seq: i64, prev: i64, asks: Value, bids: Value) -> Value {
        json!({"seqId":seq,"prevSeqId":prev,"ts":"1790467200000","asks":asks,"bids":bids})
    }
    fn initial() -> Value {
        row(
            10,
            -1,
            json!([["11", "1", "0", "1"], ["12", "2", "0", "2"]]),
            json!([["9", "1", "0", "1"], ["8", "2", "0", "2"]]),
        )
    }
    #[test]
    fn updates_delete_and_preserve_sorted_depth() {
        let mut book = Book::default();
        book.apply("snapshot", initial()).unwrap();
        book.apply(
            "update",
            row(
                11,
                10,
                json!([["11.0", "0", "0", "0"]]),
                json!([["9", "3", "0", "1"]]),
            ),
        )
        .unwrap();
        let snap = book.snapshot().unwrap();
        assert_eq!(snap.asks[0].price, "12");
        assert_eq!(snap.bids[0].size, "3");
        assert_eq!(snap.sequence, 11);
    }
    #[test]
    fn heartbeat_and_linked_reset() {
        let mut book = Book::default();
        book.apply("snapshot", initial()).unwrap();
        book.apply("update", row(10, 10, json!([]), json!([])))
            .unwrap();
        book.apply(
            "update",
            row(3, 10, json!([["10", "1", "0", "1"]]), json!([])),
        )
        .unwrap();
        assert_eq!(book.snapshot().unwrap().sequence, 3);
    }
    #[test]
    fn invalid_messages_revoke_continuity() {
        for bad in [
            row(12, 9, json!([]), json!([])),
            row(10, 10, json!([["11", "2", "0", "1"]]), json!([])),
            row(11, 10, json!([["bad", "1", "0", "1"]]), json!([])),
            row(
                11,
                10,
                json!([["11", "1", "0", "1"], ["11.0", "2", "0", "1"]]),
                json!([]),
            ),
            row(11, 10, json!([["9", "1", "0", "1"]]), json!([])),
            row(
                11,
                10,
                json!([["11", "0", "0", "0"], ["12", "0", "0", "0"]]),
                json!([]),
            ),
        ] {
            let mut book = Book::default();
            book.apply("snapshot", initial()).unwrap();
            assert!(book.apply("update", bad).is_err());
            assert!(book.snapshot().is_none());
            assert!(
                book.apply("update", row(11, 10, json!([]), json!([])))
                    .is_err()
            );
        }
        let mut book = Book::default();
        assert!(
            book.apply("snapshot", row(1, 0, json!([]), json!([])))
                .is_err()
        );
    }
    #[test]
    fn retains_levels_beyond_published_top_ten() {
        let mut book = Book::default();
        let asks: Vec<Value> = (11..=22)
            .map(|p| json!([p.to_string(), "1", "0", "1"]))
            .collect();
        book.apply(
            "snapshot",
            row(1, -1, json!(asks), json!([["9", "1", "0", "1"]])),
        )
        .unwrap();
        assert_eq!(book.snapshot().unwrap().asks.len(), 10);
        book.apply(
            "update",
            row(2, 1, json!([["11", "0", "0", "0"]]), json!([])),
        )
        .unwrap();
        assert_eq!(book.snapshot().unwrap().asks.last().unwrap().price, "21");
    }
}

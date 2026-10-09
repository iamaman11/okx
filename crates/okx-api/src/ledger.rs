use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{OkxError, OkxRestClient, instrument::InstrumentType};

const HISTORY_PAGE_LIMIT: usize = 100;
const HISTORY_MAX_PAGES: usize = 1;
const RECENT_ORDER_HISTORY_PATH: &str = "/api/v5/trade/orders-history";
const ARCHIVE_ORDER_HISTORY_PATH: &str = "/api/v5/trade/orders-history-archive";

#[derive(Debug, Clone)]
pub struct BoundedHistory<T> {
    pub rows: Vec<T>,
    pub pages: usize,
    pub complete: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PositionHistory {
    #[serde(rename = "instType", default)]
    pub instrument_type: String,
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(rename = "mgnMode", default)]
    pub margin_mode: String,
    #[serde(rename = "posId", default)]
    pub position_id: String,
    #[serde(rename = "posSide", default)]
    pub position_side: String,
    #[serde(default)]
    pub direction: String,
    #[serde(rename = "openAvgPx", default)]
    pub open_average_price: String,
    #[serde(rename = "closeAvgPx", default)]
    pub close_average_price: String,
    #[serde(rename = "openMaxPos", default)]
    pub max_open_position: String,
    #[serde(rename = "closeTotalPos", default)]
    pub total_closed_position: String,
    #[serde(rename = "realizedPnl", default)]
    pub realized_pnl: String,
    #[serde(rename = "settledPnl", default)]
    pub settled_pnl: String,
    #[serde(default)]
    pub pnl: String,
    #[serde(default)]
    pub fee: String,
    #[serde(rename = "fundingFee", default)]
    pub funding_fee: String,
    #[serde(rename = "liqPenalty", default)]
    pub liquidation_penalty: String,
    #[serde(rename = "pnlRatio", default)]
    pub pnl_ratio: String,
    #[serde(default)]
    pub lever: String,
    #[serde(default)]
    pub ccy: String,
    #[serde(rename = "type", default)]
    pub close_type: String,
    #[serde(rename = "cTime", default)]
    pub creation_time_ms: String,
    #[serde(rename = "uTime", default)]
    pub update_time_ms: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HistoricalOrder {
    #[serde(rename = "instType", default)]
    pub instrument_type: String,
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(rename = "ordId", default)]
    pub order_id: String,
    #[serde(rename = "clOrdId", default)]
    pub client_order_id: String,
    #[serde(default)]
    pub tag: String,
    #[serde(default)]
    pub side: String,
    #[serde(rename = "posSide", default)]
    pub position_side: String,
    #[serde(rename = "tdMode", default)]
    pub trade_mode: String,
    #[serde(rename = "ordType", default)]
    pub order_type: String,
    #[serde(default)]
    pub px: String,
    #[serde(default)]
    pub sz: String,
    #[serde(rename = "accFillSz", default)]
    pub accumulated_fill_size: String,
    #[serde(rename = "avgPx", default)]
    pub average_fill_price: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub category: String,
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub pnl: String,
    #[serde(default)]
    pub fee: String,
    #[serde(rename = "feeCcy", default)]
    pub fee_currency: String,
    #[serde(rename = "tradeId", default)]
    pub trade_id: String,
    #[serde(rename = "cTime", default)]
    pub creation_time_ms: String,
    #[serde(rename = "uTime", default)]
    pub update_time_ms: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FillHistory {
    #[serde(rename = "instType", default)]
    pub instrument_type: String,
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(rename = "tradeId", default)]
    pub trade_id: String,
    #[serde(rename = "ordId", default)]
    pub order_id: String,
    #[serde(rename = "clOrdId", default)]
    pub client_order_id: String,
    #[serde(rename = "billId", default)]
    pub bill_id: String,
    #[serde(rename = "subType", default)]
    pub sub_type: String,
    #[serde(default)]
    pub side: String,
    #[serde(rename = "posSide", default)]
    pub position_side: String,
    #[serde(rename = "fillPx", default)]
    pub fill_price: String,
    #[serde(rename = "fillSz", default)]
    pub fill_size: String,
    #[serde(rename = "fillPnl", default)]
    pub fill_pnl: String,
    #[serde(default)]
    pub fee: String,
    #[serde(rename = "feeCcy", default)]
    pub fee_currency: String,
    #[serde(rename = "execType", default)]
    pub execution_type: String,
    #[serde(rename = "ts", default)]
    pub timestamp_ms: String,
    #[serde(rename = "fillTime", default)]
    pub fill_time_ms: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AccountBill {
    #[serde(rename = "billId", default)]
    pub bill_id: String,
    #[serde(rename = "instType", default)]
    pub instrument_type: String,
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(rename = "ordId", default)]
    pub order_id: String,
    #[serde(rename = "tradeId", default)]
    pub trade_id: String,
    #[serde(default)]
    pub ccy: String,
    #[serde(rename = "type", default)]
    pub bill_type: String,
    #[serde(rename = "subType", default)]
    pub bill_sub_type: String,
    #[serde(rename = "balChg", default)]
    pub balance_change: String,
    #[serde(rename = "posBalChg", default)]
    pub position_balance_change: String,
    #[serde(default)]
    pub bal: String,
    #[serde(rename = "posBal", default)]
    pub position_balance: String,
    #[serde(default)]
    pub sz: String,
    #[serde(default)]
    pub px: String,
    #[serde(default)]
    pub pnl: String,
    #[serde(default)]
    pub fee: String,
    #[serde(rename = "mgnMode", default)]
    pub margin_mode: String,
    #[serde(rename = "execType", default)]
    pub execution_type: String,
    #[serde(rename = "clOrdId", default)]
    pub client_order_id: String,
    #[serde(rename = "fillTime", default)]
    pub fill_time_ms: String,
    #[serde(rename = "ts", default)]
    pub timestamp_ms: String,
}

#[derive(Clone)]
pub struct AccountHistoryApi {
    client: OkxRestClient,
}

impl AccountHistoryApi {
    pub fn new(client: OkxRestClient) -> Self {
        Self { client }
    }

    pub async fn positions_history(
        &self,
        instrument_type: InstrumentType,
    ) -> Result<BoundedHistory<PositionHistory>, OkxError> {
        self.bounded_history(
            "/api/v5/account/positions-history",
            vec![("instType", instrument_type.to_string())],
            |row: &PositionHistory| row.update_time_ms.as_str(),
        )
        .await
    }

    pub async fn orders_history(
        &self,
        instrument_type: InstrumentType,
    ) -> Result<BoundedHistory<HistoricalOrder>, OkxError> {
        // The archive alone can omit recently canceled, unfilled orders. Both
        // independently bounded reads must succeed; never treat missing data
        // from either endpoint as authoritative evidence of absence.
        let recent = self
            .bounded_history(
                RECENT_ORDER_HISTORY_PATH,
                vec![("instType", instrument_type.to_string())],
                |row: &HistoricalOrder| row.order_id.as_str(),
            )
            .await?;
        let archive = self
            .bounded_history(
                ARCHIVE_ORDER_HISTORY_PATH,
                vec![("instType", instrument_type.to_string())],
                |row: &HistoricalOrder| row.order_id.as_str(),
            )
            .await?;
        merge_order_histories(recent, archive)
    }

    pub async fn fills_history(
        &self,
        instrument_type: InstrumentType,
    ) -> Result<BoundedHistory<FillHistory>, OkxError> {
        self.bounded_history(
            "/api/v5/trade/fills-history",
            vec![("instType", instrument_type.to_string())],
            |row: &FillHistory| row.bill_id.as_str(),
        )
        .await
    }

    pub async fn fills_history_for_order(
        &self,
        instrument_type: InstrumentType,
        instrument_id: &str,
        order_id: &str,
    ) -> Result<BoundedHistory<FillHistory>, OkxError> {
        if instrument_id.trim().is_empty() {
            return Err(OkxError::Config(
                "fills-history instrument_id must be non-empty".to_owned(),
            ));
        }
        if order_id.trim().is_empty() {
            return Err(OkxError::Config(
                "fills-history order_id must be non-empty".to_owned(),
            ));
        }
        self.bounded_history(
            "/api/v5/trade/fills-history",
            vec![
                ("instType", instrument_type.to_string()),
                ("instId", instrument_id.to_owned()),
                ("ordId", order_id.to_owned()),
            ],
            |row: &FillHistory| row.bill_id.as_str(),
        )
        .await
    }

    pub async fn bills_history(&self) -> Result<BoundedHistory<AccountBill>, OkxError> {
        self.bounded_history(
            "/api/v5/account/bills-archive",
            Vec::new(),
            |row: &AccountBill| row.bill_id.as_str(),
        )
        .await
    }

    async fn bounded_history<T>(
        &self,
        path: &str,
        base_params: Vec<(&'static str, String)>,
        cursor: fn(&T) -> &str,
    ) -> Result<BoundedHistory<T>, OkxError>
    where
        T: DeserializeOwned,
    {
        let mut rows = Vec::new();
        let mut after: Option<String> = None;

        for page_index in 0..HISTORY_MAX_PAGES {
            let mut params = base_params.clone();
            params.push(("limit", HISTORY_PAGE_LIMIT.to_string()));
            if let Some(after) = after.as_ref() {
                params.push(("after", after.clone()));
            }

            let page: Vec<T> = self.client.private_get(path, &params).await?;
            let page_len = page.len();
            if page_len == 0 {
                return Ok(BoundedHistory {
                    rows,
                    pages: page_index + 1,
                    complete: true,
                });
            }

            let next_after = page
                .last()
                .map(cursor)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    OkxError::Response(format!(
                        "{path} page is missing a terminal pagination cursor"
                    ))
                })?
                .to_owned();

            if after.as_deref() == Some(next_after.as_str()) {
                return Err(OkxError::Response(format!(
                    "{path} pagination did not advance"
                )));
            }

            rows.extend(page);
            if page_len < HISTORY_PAGE_LIMIT {
                return Ok(BoundedHistory {
                    rows,
                    pages: page_index + 1,
                    complete: true,
                });
            }
            after = Some(next_after);
        }

        Ok(BoundedHistory {
            rows,
            pages: HISTORY_MAX_PAGES,
            complete: false,
        })
    }
}


fn merge_order_histories(
    recent: BoundedHistory<HistoricalOrder>,
    archive: BoundedHistory<HistoricalOrder>,
) -> Result<BoundedHistory<HistoricalOrder>, OkxError> {
    let pages = recent.pages + archive.pages;
    let complete = recent.complete && archive.complete;
    let mut orders = BTreeMap::<String, HistoricalOrder>::new();

    for (source, rows) in [("archive", archive.rows), ("recent", recent.rows)] {
        let mut source_ids = BTreeSet::new();
        for order in rows {
            let order_id = order.order_id.trim();
            if order_id.is_empty() {
                return Err(OkxError::Response(format!(
                    "{source} order history contains a missing ordId"
                )));
            }
            if !source_ids.insert(order_id.to_owned()) {
                return Err(OkxError::Response(format!(
                    "{source} order history repeats ordId {order_id}"
                )));
            }
            let timestamp = order
                .update_time_ms
                .parse::<u64>()
                .ok()
                .filter(|ts| *ts > 0)
                .ok_or_else(|| {
                    OkxError::Response(format!(
                        "{source} order history contains invalid uTime for ordId {order_id}"
                    ))
                })?;

            if let Some(previous) = orders.get(order_id) {
                // Overlapping venue windows must never silently join another
                // instrument or client-order identity to this order ID.
                if previous.instrument_type != order.instrument_type
                    || previous.instrument_id != order.instrument_id
                    || previous.client_order_id != order.client_order_id
                    || previous.side != order.side
                    || previous.position_side != order.position_side
                    || previous.trade_mode != order.trade_mode
                {
                    return Err(OkxError::Response(format!(
                        "recent/archive order identity mismatch for ordId {order_id}"
                    )));
                }
                let prior = previous.update_time_ms.parse::<u64>().map_err(|_| {
                    OkxError::Response(format!(
                        "archive order history contains invalid uTime for ordId {order_id}"
                    ))
                })?;
                if timestamp >= prior {
                    orders.insert(order_id.to_owned(), order);
                }
            } else {
                orders.insert(order_id.to_owned(), order);
            }
        }
    }

    // Complete means both endpoint pages were nontruncated, not that OKX
    // retains older or canceled-unfilled orders beyond documented windows.
    Ok(BoundedHistory {
        rows: orders.into_values().collect(),
        pages,
        complete,
    })
}

#[cfg(test)]
mod tests {
    use super::*;


    fn canceled_demo_order(id: &str, update_time_ms: &str) -> HistoricalOrder {
        serde_json::from_value(serde_json::json!({
            "instType": "SWAP",
            "instId": "BTC-USDT-SWAP",
            "ordId": id,
            "clOrdId": "okx1234567890",
            "side": "buy",
            "posSide": "long",
            "tdMode": "cross",
            "ordType": "post_only",
            "state": "canceled",
            "accFillSz": "0",
            "cTime": "1791579300000",
            "uTime": update_time_ms
        }))
        .expect("canceled Demo order fixture")
    }

    #[test]
    fn recent_history_covers_canceled_unfilled_order_missing_from_archive() {
        assert_eq!(RECENT_ORDER_HISTORY_PATH, "/api/v5/trade/orders-history");
        assert_eq!(ARCHIVE_ORDER_HISTORY_PATH, "/api/v5/trade/orders-history-archive");
        let recent = BoundedHistory {
            rows: vec![canceled_demo_order("ord-1", "1791579500000")],
            pages: 1,
            complete: true,
        };
        let archive = BoundedHistory {
            rows: vec![],
            pages: 1,
            complete: true,
        };
        let merged = merge_order_histories(recent, archive).expect("merge recent history");
        assert_eq!(merged.rows.len(), 1);
        assert_eq!(merged.rows[0].state, "canceled");
        assert_eq!(merged.rows[0].accumulated_fill_size, "0");
        assert_eq!(merged.pages, 2);
        assert!(merged.complete);
    }

    #[test]
    fn deduplicates_recent_and_archive_by_exact_order_id_preferring_newer_update() {
        let mut older = canceled_demo_order("ord-1", "1791579400000");
        older.state = "partially_filled".to_owned();
        let recent = BoundedHistory {
            rows: vec![canceled_demo_order("ord-1", "1791579500000")],
            pages: 1,
            complete: true,
        };
        let archive = BoundedHistory {
            rows: vec![older],
            pages: 1,
            complete: true,
        };
        let merged = merge_order_histories(recent, archive).expect("merge same order");
        assert_eq!(merged.rows.len(), 1);
        assert_eq!(merged.rows[0].state, "canceled");
    }

    #[test]
    fn conflicts_and_intra_source_duplicates_fail_closed() {
        let mut different_instrument = canceled_demo_order("ord-1", "1791579500000");
        different_instrument.instrument_id = "ETH-USDT-SWAP".to_owned();
        let error = merge_order_histories(
            BoundedHistory {
                rows: vec![different_instrument],
                pages: 1,
                complete: true,
            },
            BoundedHistory {
                rows: vec![canceled_demo_order("ord-1", "1791579400000")],
                pages: 1,
                complete: true,
            },
        )
        .expect_err("identity mismatch must reject");
        assert!(error.to_string().contains("identity mismatch"));

        let error = merge_order_histories(
            BoundedHistory {
                rows: vec![
                    canceled_demo_order("ord-1", "1791579500000"),
                    canceled_demo_order("ord-1", "1791579500000"),
                ],
                pages: 1,
                complete: true,
            },
            BoundedHistory {
                rows: vec![],
                pages: 1,
                complete: true,
            },
        )
        .expect_err("duplicate recent row must reject");
        assert!(error.to_string().contains("repeats ordId"));
    }

    #[test]
    fn truncation_or_unknown_timestamp_never_claims_complete_coverage() {
        let merged = merge_order_histories(
            BoundedHistory {
                rows: vec![canceled_demo_order("ord-1", "1791579500000")],
                pages: 1,
                complete: false,
            },
            BoundedHistory {
                rows: vec![],
                pages: 1,
                complete: true,
            },
        )
        .expect("bounded merge");
        assert!(!merged.complete);
        assert_eq!(merged.rows.len(), 1);

        let error = merge_order_histories(
            BoundedHistory {
                rows: vec![canceled_demo_order("ord-2", "")],
                pages: 1,
                complete: true,
            },
            BoundedHistory {
                rows: vec![],
                pages: 1,
                complete: true,
            },
        )
        .expect_err("invalid historical timestamp must reject");
        assert!(error.to_string().contains("invalid uTime"));
    }

    #[test]
    fn parses_fill_and_bill_event_time_fields() {
        let fill: FillHistory = serde_json::from_str(
            r#"{
                "instType":"SWAP",
                "instId":"DOGE-USDT-SWAP",
                "tradeId":"trade-1",
                "ordId":"ord-1",
                "billId":"bill-1",
                "fillPx":"0.1",
                "fillSz":"1",
                "fillPnl":"0.01",
                "fee":"-0.001",
                "feeCcy":"USDT",
                "ts":"1790884800001",
                "fillTime":"1790884800000"
            }"#,
        )
        .expect("fill");
        assert_eq!(fill.timestamp_ms, "1790884800001");
        assert_eq!(fill.fill_time_ms, "1790884800000");

        let bill: AccountBill = serde_json::from_str(
            r#"{
                "billId":"bill-2",
                "instType":"SWAP",
                "instId":"DOGE-USDT-SWAP",
                "subType":"173",
                "ccy":"USDT",
                "pnl":"-0.05",
                "posBalChg":"-0.05",
                "posBal":"1",
                "sz":"10",
                "px":"0.095",
                "execType":"",
                "clOrdId":"",
                "fillTime":"",
                "ts":"1790884800000"
            }"#,
        )
        .expect("bill");
        assert_eq!(bill.bill_sub_type, "173");
        assert_eq!(bill.position_balance_change, "-0.05");
        assert_eq!(bill.timestamp_ms, "1790884800000");
    }

    #[test]
    fn order_filtered_fill_history_remains_one_bounded_page() {
        assert_eq!(HISTORY_PAGE_LIMIT, 100);
        assert_eq!(HISTORY_MAX_PAGES, 1);
    }

    #[test]
    fn bounded_history_limit_is_one_page_of_one_hundred_rows() {
        assert_eq!(HISTORY_PAGE_LIMIT, 100);
        assert_eq!(HISTORY_MAX_PAGES, 1);
    }
}

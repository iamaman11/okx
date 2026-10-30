use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::{OkxError, OkxRestClient, instrument::InstrumentType};

const HISTORY_PAGE_LIMIT: usize = 100;
const HISTORY_MAX_PAGES: usize = 1;
// Campaign bootstrap is an explicit bounded operation, not a maintenance poll.
const CAMPAIGN_ORDER_MAX_PAGES_PER_SOURCE: usize = 256;
const CAMPAIGN_UNFILLED_CANCEL_RETENTION_MS: i64 = 2 * 60 * 60 * 1_000;
const CAMPAIGN_RETENTION_SAFETY_MS: i64 = 60 * 1_000;
const RECENT_ORDER_HISTORY_PATH: &str = "/api/v5/trade/orders-history";
const ARCHIVE_ORDER_HISTORY_PATH: &str = "/api/v5/trade/orders-history-archive";

#[derive(Debug, Clone)]
pub struct BoundedHistory<T> {
    pub rows: Vec<T>,
    pub pages: usize,
    pub complete: bool,
}

/// Source-bound result of TWO exhausted OKX order archives, not a
/// partial 100-row sample. A valid campaign start is supplied by the
/// existing authenticated installation authority, NEVER inferred here.
#[derive(Debug, Clone)]
pub struct CompleteCampaignOrderHistory {
    pub from_ms: i64,
    pub through_ms: i64,
    pub instrument_type: String,
    pub orders: Vec<HistoricalOrder>,
    pub source_digest: String,
}

/// Complete authenticated venue *facts* for a campaign account, not a
/// currency-converted entry expense. Only the native finance SQL owner,
/// with verified account/cohort, campaign inception and FX sources, can
/// turn this into an opening USD budget receipt.
#[derive(Debug, Clone)]
pub struct CompleteCampaignMoneyHistory {
    pub from_ms: i64,
    pub through_ms: i64,
    pub fills: Vec<FillHistory>,
    pub bills: Vec<AccountBill>,
    pub source_digest: String,
}

/// One fixed-cut-off account-wide venue witness. Native account identity
/// and original campaign inception MUST still be independently attested by
/// the authenticated finance owner before any Worker budget opening.
#[derive(Debug, Clone)]
pub struct CompleteCampaignAccountHistory {
    pub from_ms: i64,
    pub through_ms: i64,
    pub orders: Vec<HistoricalOrder>,
    pub fills: Vec<FillHistory>,
    pub bills: Vec<AccountBill>,
    pub source_digest: String,
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
    /// One immutable history window page for the native post-T0 cursor owner.
    pub async fn fills_history_page(
        &self,
        instrument_type: &str,
        begin: i64,
        end: i64,
        after: Option<&str>,
    ) -> Result<Vec<FillHistory>, OkxError> {
        if !matches!(instrument_type, "SPOT" | "MARGIN" | "SWAP" | "FUTURES" | "OPTION") {
            return Err(OkxError::Config("invalid history instrument type".into()));
        }
        let mut params = history_window_params(begin, end, after)?;
        params.push(("instType", instrument_type.into()));
        self.client
            .private_get("/api/v5/trade/fills-history", &params)
            .await
    }
    /// A complete opening campaign requires BOTH independent order sources:
    /// recent orders include canceled/unfilled cases which the archived
    /// endpoint may omit, while archived orders cover the longer history.
    /// Never merge their pagination cursors or infer a gap-free result from
    /// one bounded 100-row sample; each source must be exhausted separately.
    pub async fn orders_history_page(
        &self,
        instrument_type: &str,
        begin: i64,
        end: i64,
        after: Option<&str>,
        archive: bool,
    ) -> Result<Vec<HistoricalOrder>, OkxError> {
        if !matches!(
            instrument_type,
            "SPOT" | "MARGIN" | "SWAP" | "FUTURES" | "OPTION"
        ) {
            return Err(OkxError::Config("invalid history instrument type".into()));
        }
        let mut params = history_window_params(begin, end, after)?;
        params.push(("instType", instrument_type.into()));
        self.client
            .private_get(
                if archive {
                    ARCHIVE_ORDER_HISTORY_PATH
                } else {
                    RECENT_ORDER_HISTORY_PATH
                },
                &params,
            )
            .await
    }

    /// Complete venue evidence for ONE authenticated account and a single
    /// fixed cut-off. Each of five instrument classes and the account-wide
    /// bill stream must pass exhaustive pagination and cross-source joins.
    /// No permission to initialize RiskBook is issued by this read alone.
    pub async fn complete_campaign_account_history(
        &self,
        started_at_ms: i64,
        through_ms: i64,
        observed_at_ms: i64,
    ) -> Result<CompleteCampaignAccountHistory, OkxError> {
        verify_campaign_order_window(started_at_ms, through_ms, observed_at_ms)?;
        let mut orders = Vec::new();
        for instrument_type in ["SPOT","MARGIN","SWAP","FUTURES","OPTION"] {
            let proof = self
                .complete_campaign_orders(
                    instrument_type, started_at_ms, through_ms, observed_at_ms
                )
                .await?;
            orders.extend(proof.orders);
        }
        let money = self.complete_campaign_money_history(
            started_at_ms, through_ms, observed_at_ms
        ).await?;
        complete_campaign_account_witness(
            started_at_ms, through_ms, orders, money
        )
    }

    /// A campaign opening MUST NOT silently promote the archive's missing
    /// canceled-without-fills orders to a zero-debit assertion. OKX retains
    /// some incomplete canceled orders for only two hours. Use a fresh
    /// bounded window, exhaust recent and archive separately, then bind
    /// all returned economic identities into one content digest. Earlier
    /// campaigns need independent archival authority, not this API.
    pub async fn complete_campaign_orders(
        &self,
        instrument_type: &str,
        campaign_started_at_ms: i64,
        through_ms: i64,
        observed_at_ms: i64,
    ) -> Result<CompleteCampaignOrderHistory, OkxError> {
        verify_campaign_order_window(campaign_started_at_ms, through_ms, observed_at_ms)?;
        let recent = self
            .complete_order_source(instrument_type, campaign_started_at_ms, through_ms, false)
            .await?;
        let archive = self
            .complete_order_source(instrument_type, campaign_started_at_ms, through_ms, true)
            .await?;
        complete_campaign_orders_witness(
            instrument_type,
            campaign_started_at_ms,
            through_ms,
            recent,
            archive,
        )
    }

    async fn complete_order_source(
        &self,
        instrument_type: &str,
        begin: i64,
        end: i64,
        archive: bool,
    ) -> Result<BoundedHistory<HistoricalOrder>, OkxError> {
        let mut rows = Vec::new();
        let mut after: Option<String> = None;
        let mut seen_cursors = BTreeSet::new();
        for page in 0..CAMPAIGN_ORDER_MAX_PAGES_PER_SOURCE {
            let chunk = self
                .orders_history_page(instrument_type, begin, end, after.as_deref(), archive)
                .await?;
            let count = chunk.len();
            if count > HISTORY_PAGE_LIMIT {
                return Err(OkxError::Response(
                    "campaign order source returned an oversized page".into(),
                ));
            }
            let next = if count == HISTORY_PAGE_LIMIT {
                let last = chunk.last().ok_or_else(|| {
                    OkxError::Response("campaign order source missing last row".into())
                })?;
                if last.order_id.is_empty()
                    || !last.order_id.bytes().all(|b| b.is_ascii_digit())
                    || !seen_cursors.insert(last.order_id.clone())
                {
                    return Err(OkxError::Response(
                        "campaign order pagination stalled or invalid".into(),
                    ));
                }
                Some(last.order_id.clone())
            } else {
                None
            };
            rows.extend(chunk);
            if count < HISTORY_PAGE_LIMIT {
                return Ok(BoundedHistory {
                    rows,
                    pages: page + 1,
                    complete: true,
                });
            }
            after = next;
        }
        Err(OkxError::Response(
            "campaign order source exceeds bounded full-pagination capacity".into(),
        ))
    }

    /// On-demand venue source for E1 campaign opening. Every instrument
    /// partition and the full unfiltered bills stream are exhausted at the
    /// SAME fixed cut-off. Partial pagination and missing fee bills fail
    /// closed. This method DOES NOT assert a campaign inception, a USD FX
    /// bound, or that source contents alone prove initial spending.
    pub async fn complete_campaign_money_history(
        &self,
        started_at_ms: i64,
        through_ms: i64,
        observed_at_ms: i64,
    ) -> Result<CompleteCampaignMoneyHistory, OkxError> {
        verify_campaign_order_window(started_at_ms, through_ms, observed_at_ms)?;
        let mut fills = Vec::new();
        for instrument_type in ["SPOT", "MARGIN", "SWAP", "FUTURES", "OPTION"] {
            let mut after: Option<String> = None;
            let mut seen_cursors = BTreeSet::new();
            let mut complete = false;
            for _ in 0..CAMPAIGN_ORDER_MAX_PAGES_PER_SOURCE {
                let rows = self
                    .fills_history_page(instrument_type, started_at_ms, through_ms, after.as_deref())
                    .await?;
                let count = rows.len();
                if count > HISTORY_PAGE_LIMIT {
                    return Err(OkxError::Response("campaign fills page oversized".into()));
                }
                let next = if count == HISTORY_PAGE_LIMIT {
                    let id = rows.last().map(|fill| fill.bill_id.as_str()).unwrap_or("");
                    if !valid_campaign_cursor(id) || !seen_cursors.insert(id.to_owned()) {
                        return Err(OkxError::Response("campaign fills cursor invalid or stalled".into()));
                    }
                    Some(id.to_owned())
                } else {
                    None
                };
                fills.extend(rows);
                if count < HISTORY_PAGE_LIMIT {
                    complete = true;
                    break;
                }
                after = next;
            }
            if !complete {
                return Err(OkxError::Response("campaign fills pagination incomplete".into()));
            }
        }
        let mut bills = Vec::new();
        let mut after: Option<String> = None;
        let mut seen_cursors = BTreeSet::new();
        let mut complete = false;
        for _ in 0..CAMPAIGN_ORDER_MAX_PAGES_PER_SOURCE {
            let rows = self.bills_history_page(started_at_ms, through_ms, after.as_deref()).await?;
            let count = rows.len();
            if count > HISTORY_PAGE_LIMIT {
                return Err(OkxError::Response("campaign bills page oversized".into()));
            }
            let next = if count == HISTORY_PAGE_LIMIT {
                let id = rows.last().map(|bill| bill.bill_id.as_str()).unwrap_or("");
                if !valid_campaign_cursor(id) || !seen_cursors.insert(id.to_owned()) {
                    return Err(OkxError::Response("campaign bills cursor invalid or stalled".into()));
                }
                Some(id.to_owned())
            } else {
                None
            };
            bills.extend(rows);
            if count < HISTORY_PAGE_LIMIT {
                complete = true;
                break;
            }
            after = next;
        }
        if !complete {
            return Err(OkxError::Response("campaign bills pagination incomplete".into()));
        }
        complete_campaign_money_witness(started_at_ms, through_ms, fills, bills)
    }

    pub async fn bills_history_page(
        &self,
        begin: i64,
        end: i64,
        after: Option<&str>,
    ) -> Result<Vec<AccountBill>, OkxError> {
        self.client
            .private_get(
                "/api/v5/account/bills-archive",
                &history_window_params(begin, end, after)?,
            )
            .await
    }
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
fn history_window_params(
    begin: i64,
    end: i64,
    after: Option<&str>,
) -> Result<Vec<(&'static str, String)>, OkxError> {
    if begin < 0
        || end < begin
        || after
            .is_some_and(|s| s.is_empty() || s.len() > 64 || !s.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err(OkxError::Config("invalid history window cursor".into()));
    }
    let mut params = vec![
        ("begin", begin.to_string()),
        ("end", end.to_string()),
        ("limit", "100".into()),
    ];
    if let Some(after) = after {
        params.push(("after", after.into()));
    }
    Ok(params)
}

/// A venue history response contains no intrinsic campaign start. The
/// caller must provide the immutable campaign inception, and that start
/// must be within the weakest source's retention window. A minute of
/// boundary headroom rejects ambiguous oldest canceled orders.
fn verify_campaign_order_window(
    started_at_ms: i64,
    through_ms: i64,
    observed_at_ms: i64,
) -> Result<(), OkxError> {
    if started_at_ms <= 0
        || through_ms < started_at_ms
        || observed_at_ms < through_ms
        || observed_at_ms - through_ms > CAMPAIGN_RETENTION_SAFETY_MS
        || observed_at_ms - started_at_ms
            >= CAMPAIGN_UNFILLED_CANCEL_RETENTION_MS - CAMPAIGN_RETENTION_SAFETY_MS
    {
        return Err(OkxError::Config(
            "campaign order start or cut-off outside provable venue history".into(),
        ));
    }
    Ok(())
}

fn complete_campaign_orders_witness(
    instrument_type: &str,
    from_ms: i64,
    through_ms: i64,
    recent: BoundedHistory<HistoricalOrder>,
    archive: BoundedHistory<HistoricalOrder>,
) -> Result<CompleteCampaignOrderHistory, OkxError> {
    if !recent.complete || !archive.complete {
        return Err(OkxError::Response(
            "campaign order history pagination incomplete".into(),
        ));
    }
    let merged = merge_order_histories(recent, archive)?;
    if !merged.complete {
        return Err(OkxError::Response(
            "campaign order history pagination incomplete".into(),
        ));
    }
    for order in &merged.rows {
        if order.instrument_type != instrument_type
            || order.order_id.is_empty()
            || order
                .creation_time_ms
                .parse::<i64>()
                .ok()
                .is_none_or(|v| v < from_ms || v > through_ms)
            || order
                .update_time_ms
                .parse::<i64>()
                .ok()
                .is_none_or(|v| v < from_ms || v > through_ms)
        {
            return Err(OkxError::Response(
                "campaign order history scope or causal time invalid".into(),
            ));
        }
    }
    let canonical = serde_json::to_vec(&(
        "okx.campaign-orders-window/v1",
        instrument_type,
        from_ms,
        through_ms,
        &merged.rows,
    ))
    .map_err(|_| OkxError::Response("campaign order digest serialization failed".into()))?;
    Ok(CompleteCampaignOrderHistory {
        from_ms,
        through_ms,
        instrument_type: instrument_type.into(),
        orders: merged.rows,
        source_digest: format!("{:x}", Sha256::digest(canonical)),
    })
}

/// Exact joins of authenticated venue economic facts. Contract valuation,
/// historic FX and campaign start provenance are NOT asserted here.
fn complete_campaign_account_witness(
    from_ms: i64,
    through_ms: i64,
    mut orders: Vec<HistoricalOrder>,
    money: CompleteCampaignMoneyHistory,
) -> Result<CompleteCampaignAccountHistory, OkxError> {
    if from_ms <= 0
        || through_ms < from_ms
        || money.from_ms != from_ms
        || money.through_ms != through_ms
    {
        return Err(OkxError::Response("campaign account source cut-off mismatch".into()));
    }
    let mut seen = BTreeMap::new();
    for order in &orders {
        if order.order_id.is_empty() ||
            !matches!(order.instrument_type.as_str(),"SPOT"|"MARGIN"|"SWAP"|"FUTURES"|"OPTION") ||
            order.creation_time_ms.parse::<i64>().ok().is_none_or(|v|v<from_ms||v>through_ms) ||
            order.update_time_ms.parse::<i64>().ok().is_none_or(|v|v<from_ms||v>through_ms) ||
            seen.insert(order.order_id.as_str(),order).is_some()
        {
            return Err(OkxError::Response("campaign account order identity conflict".into()));
        }
    }
    let mut executed = BTreeMap::<&str, rust_decimal::Decimal>::new();
    for fill in &money.fills {
        let order = seen.get(fill.order_id.as_str())
            .ok_or_else(|| OkxError::Response("campaign fill has no proven venue order".into()))?;
        if order.instrument_id != fill.instrument_id ||
            order.instrument_type != fill.instrument_type
        {
            return Err(OkxError::Response("campaign fill order instrument mismatch".into()));
        }
        let size = fill.fill_size.parse::<rust_decimal::Decimal>()
            .map_err(|_|OkxError::Response("campaign fill quantity invalid".into()))?;
        let previous=executed.get(fill.order_id.as_str()).copied()
            .unwrap_or(rust_decimal::Decimal::ZERO);
        let new=previous.checked_add(size)
            .ok_or_else(||OkxError::Response("campaign fill quantity overflow".into()))?;
        executed.insert(fill.order_id.as_str(),new);
    }
    for order in &orders {
        let order_size=order.accumulated_fill_size.parse::<rust_decimal::Decimal>()
            .map_err(|_|OkxError::Response("campaign venue order filled size invalid".into()))?;
        if order_size<rust_decimal::Decimal::ZERO ||
            executed.get(order.order_id.as_str()).copied()
                .unwrap_or(rust_decimal::Decimal::ZERO)!=order_size
        {
            return Err(OkxError::Response("campaign venue order and fills disagree".into()));
        }
    }
    // Bills without an order identifier (e.g. funding or account transfers)
    // remain raw evidence; their economic meaning/currency must be classified
    // by the SQL finance owner, not silently interpreted as zero entry cost.
    orders.sort_by(|a,b|a.order_id.cmp(&b.order_id));
    let encoded=serde_json::to_vec(&(
        "okx.campaign-account-history/v1",
        from_ms, through_ms, &orders, &money.source_digest,
    )).map_err(|_|OkxError::Response("campaign account digest invalid".into()))?;
    Ok(CompleteCampaignAccountHistory {
        from_ms,through_ms,orders,fills:money.fills,bills:money.bills,
        source_digest:format!("{:x}",Sha256::digest(encoded)),
    })
}

fn valid_campaign_cursor(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|b| b.is_ascii_digit())
}

/// Pure owner-independent source consistency, not a finance authorization.
/// Hash source facts only after validating every native fill and fee bill.
fn complete_campaign_money_witness(
    from_ms: i64,
    through_ms: i64,
    mut fills: Vec<FillHistory>,
    mut bills: Vec<AccountBill>,
) -> Result<CompleteCampaignMoneyHistory, OkxError> {
    if from_ms <= 0 || through_ms < from_ms {
        return Err(OkxError::Response("campaign money window invalid".into()));
    }
    let mut fill_ids = BTreeSet::new();
    let mut bill_ids = BTreeSet::new();
    for fill in &fills {
        let ts = if fill.fill_time_ms.is_empty() {
            &fill.timestamp_ms
        } else {
            &fill.fill_time_ms
        };
        let when = ts.parse::<i64>().ok().filter(|v| *v >= from_ms && *v <= through_ms);
        if !matches!(
            fill.instrument_type.as_str(),
            "SPOT" | "MARGIN" | "SWAP" | "FUTURES" | "OPTION"
        ) || when.is_none()
            || fill.instrument_id.is_empty()
            || !valid_campaign_cursor(&fill.bill_id)
            || fill.trade_id.is_empty()
            || fill.order_id.is_empty()
            || fill.fee.is_empty()
            || fill.fee_currency.is_empty()
            || fill.fill_price.parse::<rust_decimal::Decimal>().ok().is_none_or(|v| v <= rust_decimal::Decimal::ZERO)
            || fill.fill_size.parse::<rust_decimal::Decimal>().ok().is_none_or(|v| v <= rust_decimal::Decimal::ZERO)
            || fill.fee.parse::<rust_decimal::Decimal>().is_err()
            || !fill_ids.insert((fill.instrument_id.clone(),fill.trade_id.clone(),fill.order_id.clone()))
        {
            return Err(OkxError::Response("campaign fill identity, currency or scope unproven".into()));
        }
    }
    for bill in &bills {
        if !valid_campaign_cursor(&bill.bill_id)
            || bill.timestamp_ms.parse::<i64>().ok()
                .is_none_or(|v| v < from_ms || v > through_ms)
            || bill.ccy.is_empty()
            || !bill_ids.insert(bill.bill_id.clone())
            || (!bill.fee.is_empty() && bill.fee.parse::<rust_decimal::Decimal>().is_err())
        {
            return Err(OkxError::Response("campaign bill identity, currency or scope unproven".into()));
        }
    }
    // The evidence can contain thousands of fills/bills: use indexed joins
    // rather than O(fills × bills) scans inside the single native reader.
    let billed: BTreeMap<&str, &AccountBill> = bills
        .iter()
        .map(|bill| (bill.bill_id.as_str(), bill))
        .collect();
    for fill in &fills {
        let bill = billed
            .get(fill.bill_id.as_str())
            .ok_or_else(|| OkxError::Response("campaign fill fee bill unavailable".into()))?;
        if bill.ccy != fill.fee_currency
            || !bill.order_id.is_empty() && bill.order_id != "0" && bill.order_id != fill.order_id
            || !bill.trade_id.is_empty() && bill.trade_id != "0" && bill.trade_id != fill.trade_id
            || bill.fee.parse::<rust_decimal::Decimal>().ok()
                != fill.fee.parse::<rust_decimal::Decimal>().ok()
        {
            return Err(OkxError::Response("campaign fill and fee bill disagree".into()));
        }
    }
    fills.sort_by(|a, b| (&a.instrument_id,&a.trade_id,&a.order_id)
        .cmp(&(&b.instrument_id,&b.trade_id,&b.order_id)));
    bills.sort_by(|a, b| a.bill_id.cmp(&b.bill_id));
    let bytes = serde_json::to_vec(&(
        "okx.campaign-money-window/v1",
        from_ms,
        through_ms,
        &fills,
        &bills,
    )).map_err(|_| OkxError::Response("campaign money digest invalid".into()))?;
    Ok(CompleteCampaignMoneyHistory {
        from_ms,
        through_ms,
        fills,
        bills,
        source_digest: format!("{:x}",Sha256::digest(bytes)),
    })
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
                // A merge is only a choice between different snapshots in
                // time, not permission to rewrite an economic event. Equal
                // uTime on independently authenticated endpoints must agree
                // on all fields that affect order accounting. Otherwise the
                // result depends on endpoint enumeration order.
                if timestamp == prior
                    && (previous.state != order.state
                        || previous.accumulated_fill_size != order.accumulated_fill_size
                        || previous.average_fill_price != order.average_fill_price
                        || previous.fee != order.fee
                        || previous.fee_currency != order.fee_currency
                        || previous.order_type != order.order_type
                        || previous.creation_time_ms != order.creation_time_ms)
                {
                    return Err(OkxError::Response(format!(
                        "recent/archive order economic conflict for ordId {order_id}"
                    )));
                }
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
        assert_eq!(
            ARCHIVE_ORDER_HISTORY_PATH,
            "/api/v5/trade/orders-history-archive"
        );
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
    fn campaign_money_window_binds_immutable_order_fill_to_original_fee_bill() {
        let begin = 1_800_000_000_000i64;
        let end = begin + 10_000;
        let fill: FillHistory = serde_json::from_value(serde_json::json!({
            "instType":"SWAP","instId":"BTC-USDT-SWAP","tradeId":"trade001",
            "ordId":"order001","billId":"123","fillPx":"100","fillSz":"1",
            "fee":"-0.05","feeCcy":"USDT","fillTime":begin+1_000
        })).unwrap();
        let bill: AccountBill = serde_json::from_value(serde_json::json!({
            "billId":"123","instType":"SWAP","instId":"BTC-USDT-SWAP",
            "ordId":"order001","tradeId":"trade001","ccy":"USDT",
            "fee":"-0.05","ts":begin+1_001
        })).unwrap();
        let source = complete_campaign_money_witness(begin,end,vec![fill.clone()],vec![bill.clone()]).unwrap();
        assert_eq!(source.fills.len(),1);
        assert_eq!(source.bills.len(),1);
        assert_eq!(source.source_digest.len(),64);
        assert_eq!(
            complete_campaign_money_witness(begin,end,vec![fill.clone()],vec![bill.clone()])
                .unwrap().source_digest,
            source.source_digest
        );
        assert!(complete_campaign_money_witness(begin,end,vec![fill.clone()],vec![]).is_err());
        let mut different_currency=bill.clone();
        different_currency.ccy="BTC".into();
        assert!(complete_campaign_money_witness(begin,end,vec![fill.clone()],vec![different_currency]).is_err());
        let mut altered_fee=bill.clone();
        altered_fee.fee="-0.06".into();
        assert!(complete_campaign_money_witness(begin,end,vec![fill.clone()],vec![altered_fee]).is_err());
        assert!(complete_campaign_money_witness(begin,end,vec![fill.clone(),fill.clone()],vec![bill.clone()]).is_err());
        let mut stale=fill;
        stale.fill_time_ms=(begin-1).to_string();
        assert!(complete_campaign_money_witness(begin,end,vec![stale],vec![bill]).is_err());
    }

    #[test]
    fn same_time_conflicting_archive_fills_cannot_mint_campaign_history() {
        let mut original = canceled_demo_order("123456", "1791579301000");
        original.accumulated_fill_size = "1".into();
        original.fee = "-0.01".into();
        let mut changed = original.clone();
        changed.accumulated_fill_size = "2".into();
        let source = BoundedHistory {
            rows: vec![original],
            pages: 1,
            complete: true,
        };
        let archive = BoundedHistory {
            rows: vec![changed],
            pages: 1,
            complete: true,
        };
        assert!(merge_order_histories(source, archive).is_err());
    }

    #[test]
    fn campaign_order_window_requires_provable_short_retention() {
        let now = 1_800_000_000_000_i64;
        assert!(verify_campaign_order_window(now - 60_000, now, now).is_ok());
        assert!(
            verify_campaign_order_window(now - CAMPAIGN_UNFILLED_CANCEL_RETENTION_MS, now, now)
                .is_err()
        );
        assert!(verify_campaign_order_window(now - 120_000, now, now - 1).is_err());
        assert!(verify_campaign_order_window(now - 120_000, now - 120_000, now).is_err());
        assert!(verify_campaign_order_window(0, now, now).is_err());
    }

    #[test]
    fn campaign_order_witness_requires_exhaustion_and_causal_identity() {
        let since = 1_800_000_000_000_i64;
        let through = since + 20_000;
        let order = HistoricalOrder {
            instrument_type: "SWAP".into(),
            instrument_id: "BTC-USDT-SWAP".into(),
            order_id: "123456".into(),
            client_order_id: "client".into(),
            side: "buy".into(),
            creation_time_ms: since.to_string(),
            update_time_ms: (since + 1_000).to_string(),
            ..canceled_demo_order("123456", &(since + 1_000).to_string())
        };
        let recent = BoundedHistory {
            rows: vec![order.clone()],
            pages: 1,
            complete: true,
        };
        let archive = BoundedHistory {
            rows: vec![],
            pages: 1,
            complete: true,
        };
        let proved = complete_campaign_orders_witness(
            "SWAP",
            since,
            through,
            recent.clone(),
            archive.clone(),
        )
        .unwrap();
        assert_eq!(proved.orders.len(), 1);
        assert_eq!(proved.source_digest.len(), 64);
        assert_eq!(proved.from_ms, since);
        assert_eq!(proved.through_ms, through);
        assert!(
            complete_campaign_orders_witness(
                "SWAP",
                since,
                through,
                BoundedHistory {
                    complete: false,
                    ..recent.clone()
                },
                archive.clone()
            )
            .is_err()
        );
        let mut early = order.clone();
        early.creation_time_ms = (since - 1).to_string();
        assert!(
            complete_campaign_orders_witness(
                "SWAP",
                since,
                through,
                BoundedHistory {
                    rows: vec![early],
                    pages: 1,
                    complete: true
                },
                archive.clone()
            )
            .is_err()
        );
        let mut conflicting = order;
        conflicting.side = "sell".into();
        assert!(
            complete_campaign_orders_witness(
                "SWAP",
                since,
                through,
                recent,
                BoundedHistory {
                    rows: vec![conflicting],
                    pages: 1,
                    complete: true
                }
            )
            .is_err()
        );
        assert!(
            complete_campaign_orders_witness("SWAP", since, through, archive.clone(), archive)
                .is_ok()
        );
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

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use crate::{
    DEFAULT_SUBACCOUNT_ORDER_LIMIT_PER_2S, MutationTiming, OkxRestClient, RateOperationClass,
    RateRequestPlan, client::ApiEnvelope, error::OkxError,
};

const PLACE_ORDER_PATH: &str = "/api/v5/trade/order";
const CANCEL_ORDER_PATH: &str = "/api/v5/trade/cancel-order";
const CANCEL_ALGO_PATH: &str = "/api/v5/trade/cancel-algos";
const AMEND_ORDER_PATH: &str = "/api/v5/trade/amend-order";
const ORDER_DETAILS_PATH: &str = "/api/v5/trade/order";
const ALGO_ORDER_DETAILS_PATH: &str = "/api/v5/trade/order-algo";
const ALGO_PENDING_PATH: &str = "/api/v5/trade/orders-algo-pending";
const PROTECTIVE_ALGO_PAGE_LIMIT: usize = 100;
const PROTECTIVE_ALGO_SAMPLE_LIMIT: usize = 8;
const ACCOUNT_RATE_LIMIT_PATH: &str = "/api/v5/trade/account-rate-limit";
pub const ACCOUNT_RATE_LIMIT_EVIDENCE_SCHEMA_V1: &str = "okx.account-rate-limit/v1";
pub const ACCOUNT_RATE_LIMIT_EVIDENCE_SCHEMA_V2: &str = "okx.account-rate-limit/v2";
pub const ACCOUNT_RATE_LIMIT_EVIDENCE_SCHEMA_V3: &str = "okx.account-rate-limit/v3";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApiTradeMode {
    Cross,
    Isolated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApiOrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApiPositionSide {
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiOrderType {
    Limit,
    PostOnly,
    Fok,
    Ioc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApiTriggerPriceType {
    Last,
    Index,
    Mark,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachedAlgoOrderRequest {
    #[serde(rename = "attachAlgoClOrdId")]
    pub client_order_id: String,
    #[serde(rename = "tpTriggerPx")]
    pub take_profit_trigger_price: String,
    #[serde(rename = "tpTriggerPxType")]
    pub take_profit_trigger_price_type: ApiTriggerPriceType,
    #[serde(rename = "tpOrdPx")]
    pub take_profit_order_price: String,
    #[serde(rename = "slTriggerPx")]
    pub stop_loss_trigger_price: String,
    #[serde(rename = "slTriggerPxType")]
    pub stop_loss_trigger_price_type: ApiTriggerPriceType,
    #[serde(rename = "slOrdPx")]
    pub stop_loss_order_price: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlaceOrderRequest {
    #[serde(rename = "instId")]
    pub instrument_id: String,
    #[serde(rename = "tdMode")]
    pub trade_mode: ApiTradeMode,
    #[serde(rename = "clOrdId")]
    pub client_order_id: String,
    pub side: ApiOrderSide,
    #[serde(rename = "posSide")]
    pub position_side: ApiPositionSide,
    #[serde(rename = "ordType")]
    pub order_type: ApiOrderType,
    #[serde(rename = "sz")]
    pub size: String,
    #[serde(rename = "px")]
    pub price: String,
    #[serde(
        rename = "attachAlgoOrds",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub attached_algo_orders: Vec<AttachedAlgoOrderRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelOrderRequest {
    #[serde(rename = "instId")]
    pub instrument_id: String,
    #[serde(rename = "clOrdId")]
    pub client_order_id: String,
}

/// Exchange accepts an array of cancellation entries; this typed operation
/// deliberately sends only one exact owned algoId, never bulk/cancel-all.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelAlgoOrderRequest {
    #[serde(rename = "instId")]
    pub instrument_id: String,
    #[serde(rename = "algoId")]
    pub algo_order_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CancelAlgoOrderAck {
    #[serde(rename = "algoId", default)]
    pub algo_order_id: String,
    #[serde(rename = "sCode", default)]
    pub status_code: String,
    #[serde(rename = "sMsg", default)]
    pub status_message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AmendOrderRequest {
    #[serde(rename = "instId")]
    pub instrument_id: String,
    #[serde(rename = "clOrdId")]
    pub client_order_id: String,
    #[serde(rename = "reqId")]
    pub request_id: String,
    #[serde(rename = "cxlOnFail")]
    pub cancel_on_fail: bool,
    #[serde(rename = "newSz", skip_serializing_if = "Option::is_none")]
    pub new_size: Option<String>,
    #[serde(rename = "newPx", skip_serializing_if = "Option::is_none")]
    pub new_price: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderOperationAck {
    #[serde(rename = "ordId", default)]
    pub order_id: String,
    #[serde(rename = "clOrdId", default)]
    pub client_order_id: String,
    #[serde(rename = "reqId", default)]
    pub request_id: String,
    #[serde(rename = "ts", default)]
    pub timestamp_ms: String,
    #[serde(rename = "sCode", default)]
    pub status_code: String,
    #[serde(rename = "sMsg", default)]
    pub status_message: String,
}

impl OrderOperationAck {
    pub fn accepted(&self) -> bool {
        self.status_code == "0"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountRateLimitSource {
    Exchange,
    DemoBaseFallback,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountRateLimitEvidence {
    pub schema: &'static str,
    pub source: AccountRateLimitSource,
    pub current_orders_per_2s: u32,
    pub next_orders_per_2s: Option<u32>,
    pub fill_ratio: Option<String>,
    pub main_fill_ratio: Option<String>,
    pub updated_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawAccountRateLimit {
    #[serde(rename = "accRateLimit")]
    current_orders_per_2s: String,
    #[serde(rename = "nextAccRateLimit", default)]
    next_orders_per_2s: String,
    #[serde(rename = "fillRatio", default)]
    fill_ratio: String,
    #[serde(rename = "mainFillRatio", default)]
    main_fill_ratio: String,
    #[serde(rename = "ts")]
    updated_at_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TradeResponse<T> {
    pub code: String,
    pub message: String,
    pub data: Vec<T>,
    pub in_time_us: String,
    pub out_time_us: String,
}

impl<T> TradeResponse<T> {
    pub fn top_level_success(&self) -> bool {
        self.code == "0"
    }
}

impl<T> From<ApiEnvelope<T>> for TradeResponse<T> {
    fn from(value: ApiEnvelope<T>) -> Self {
        Self {
            code: value.code,
            message: value.msg,
            data: value.data,
            in_time_us: value.in_time,
            out_time_us: value.out_time,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TradeOrderDetails {
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(rename = "ordId", default)]
    pub order_id: String,
    #[serde(rename = "clOrdId", default)]
    pub client_order_id: String,
    #[serde(default)]
    pub side: String,
    #[serde(rename = "posSide", default)]
    pub position_side: String,
    #[serde(rename = "tdMode", default)]
    pub trade_mode: String,
    #[serde(rename = "ordType", default)]
    pub order_type: String,
    #[serde(rename = "px", default)]
    pub price: String,
    #[serde(rename = "sz", default)]
    pub size: String,
    #[serde(rename = "accFillSz", default)]
    pub accumulated_fill_size: String,
    #[serde(rename = "avgPx", default)]
    pub average_fill_price: String,
    #[serde(default)]
    pub state: String,
    #[serde(rename = "cTime", default)]
    pub creation_time_ms: String,
    #[serde(rename = "uTime", default)]
    pub update_time_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TradeAlgoOrderDetails {
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(rename = "algoId", default)]
    pub algo_order_id: String,
    #[serde(rename = "algoClOrdId", default)]
    pub client_order_id: String,
    #[serde(default)]
    pub state: String,
    #[serde(rename = "tpTriggerPx", default)]
    pub take_profit_trigger_price: String,
    #[serde(rename = "tpTriggerPxType", default)]
    pub take_profit_trigger_price_type: String,
    #[serde(rename = "tpOrdPx", default)]
    pub take_profit_order_price: String,
    #[serde(rename = "slTriggerPx", default)]
    pub stop_loss_trigger_price: String,
    #[serde(rename = "slTriggerPxType", default)]
    pub stop_loss_trigger_price_type: String,
    #[serde(rename = "slOrdPx", default)]
    pub stop_loss_order_price: String,
    #[serde(rename = "failCode", default)]
    pub failure_code: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PendingAlgoOrderDetails {
    #[serde(rename = "instType", default)]
    pub instrument_type: String,
    #[serde(rename = "ordType", default)]
    pub order_type: String,
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(rename = "algoId", default)]
    pub algo_order_id: String,
    #[serde(rename = "algoClOrdId", default)]
    pub client_order_id: String,
    #[serde(default)]
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PendingProtectiveAlgoSample {
    pub instrument_id: String,
    pub algo_order_id: String,
    pub client_order_id: String,
    pub order_type: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PendingProtectiveAlgoInventory {
    pub schema: &'static str,
    pub scope: &'static str,
    pub order_types: &'static str,
    pub instrument_types: &'static str,
    pub rows: usize,
    pub complete_within_bound: bool,
    pub samples: Vec<PendingProtectiveAlgoSample>,
}

fn normalize_protective_algo_inventory(
    batches: Vec<(String, String, Vec<PendingAlgoOrderDetails>)>,
) -> Result<PendingProtectiveAlgoInventory, OkxError> {
    let mut ids = BTreeSet::<String>::new();
    let mut seen_scopes = BTreeSet::<(String, String)>::new();
    let mut total = 0_usize;
    let mut complete = true;
    let mut samples = Vec::new();
    for (instrument_type, order_type, rows) in batches {
        if !matches!(instrument_type.as_str(), "SWAP" | "FUTURES")
            || !matches!(order_type.as_str(), "conditional" | "oco")
            || !seen_scopes.insert((instrument_type.clone(), order_type.clone()))
        {
            return Err(OkxError::Response(
                "invalid or duplicate pending protective algo query scope".to_owned(),
            ));
        }
        if rows.len() > PROTECTIVE_ALGO_PAGE_LIMIT {
            return Err(OkxError::Response(
                "pending protective algo page exceeded hard bound".to_owned(),
            ));
        }
        complete &= rows.len() < PROTECTIVE_ALGO_PAGE_LIMIT;
        for item in rows {
            if item.instrument_type != instrument_type
                || item.order_type != order_type
                || item.algo_order_id.trim().is_empty()
                || item.instrument_id.trim().is_empty()
                || !matches!(item.state.as_str(), "live" | "pause")
                || !ids.insert(item.algo_order_id.clone())
            {
                return Err(OkxError::Response(
                    "pending protective algo has invalid exchange identity/type/state or duplicate algoId".to_owned(),
                ));
            }
            total += 1;
            if samples.len() < PROTECTIVE_ALGO_SAMPLE_LIMIT {
                samples.push(PendingProtectiveAlgoSample {
                    instrument_id: item.instrument_id,
                    algo_order_id: item.algo_order_id,
                    client_order_id: item.client_order_id,
                    order_type: item.order_type,
                    state: item.state,
                });
            }
        }
    }
    if seen_scopes.len() != 4 {
        return Err(OkxError::Response(
            "pending protective algo inventory lacks one or more required scopes".to_owned(),
        ));
    }
    Ok(PendingProtectiveAlgoInventory {
        schema: "okx.pending-protective-algo-inventory/v1",
        scope: "authenticated_account",
        order_types: "conditional,oco",
        instrument_types: "SWAP,FUTURES",
        rows: total,
        complete_within_bound: complete,
        samples,
    })
}

#[derive(Clone)]
pub struct TradeApi {
    client: OkxRestClient,
}

impl TradeApi {
    pub fn new(client: OkxRestClient) -> Self {
        Self { client }
    }

    /// Read-only, bounded account-wide inventory of Futures/SWAP attached
    /// conditional/OCO protection. Never claim account-wide ALL algo types.
    pub async fn pending_protective_algos(
        &self,
    ) -> Result<PendingProtectiveAlgoInventory, OkxError> {
        let mut batches = Vec::with_capacity(4);
        for instrument_type in ["SWAP", "FUTURES"] {
            for order_type in ["conditional", "oco"] {
                let rows: Vec<PendingAlgoOrderDetails> = self
                    .client
                    .private_get(
                        ALGO_PENDING_PATH,
                        &[
                            ("ordType", order_type.to_owned()),
                            ("instType", instrument_type.to_owned()),
                            ("limit", PROTECTIVE_ALGO_PAGE_LIMIT.to_string()),
                        ],
                    )
                    .await?;
                batches.push((instrument_type.to_owned(), order_type.to_owned(), rows));
            }
        }
        normalize_protective_algo_inventory(batches)
    }

    pub fn admit_place_order(
        &self,
        request: &PlaceOrderRequest,
    ) -> Result<RateRequestPlan, OkxError> {
        validate_place(request)?;
        let rate_plan = self.client.rate_budget().trade_rest_plan(
            RateOperationClass::PlaceOrder,
            &request.instrument_id,
            None,
        );
        self.client
            .rate_budget()
            .admit(&rate_plan)
            .map_err(|evidence| OkxError::RateLimited { evidence })?;
        Ok(rate_plan)
    }

    pub async fn place_order_after_admission(
        &self,
        request: &PlaceOrderRequest,
        timing: &MutationTiming,
        rate_plan: &RateRequestPlan,
    ) -> Result<TradeResponse<OrderOperationAck>, OkxError> {
        validate_place(request)?;
        Ok(self
            .client
            .private_post_after_admission(
                PLACE_ORDER_PATH,
                request,
                timing.request_timestamp(),
                Some(timing.exp_time_ms()),
                rate_plan,
            )
            .await?
            .into())
    }

    pub async fn place_order(
        &self,
        request: &PlaceOrderRequest,
        timing: &MutationTiming,
    ) -> Result<TradeResponse<OrderOperationAck>, OkxError> {
        let rate_plan = self.admit_place_order(request)?;
        self.place_order_after_admission(request, timing, &rate_plan)
            .await
    }

    pub fn admit_cancel_order(
        &self,
        request: &CancelOrderRequest,
    ) -> Result<RateRequestPlan, OkxError> {
        validate_instrument_id(&request.instrument_id)?;
        validate_client_id("clOrdId", &request.client_order_id)?;
        let rate_plan = self.client.rate_budget().trade_rest_plan(
            RateOperationClass::CancelOrder,
            &request.instrument_id,
            None,
        );
        self.client
            .rate_budget()
            .admit(&rate_plan)
            .map_err(|evidence| OkxError::RateLimited { evidence })?;
        Ok(rate_plan)
    }

    pub async fn cancel_order_after_admission(
        &self,
        request: &CancelOrderRequest,
        timing: &MutationTiming,
        rate_plan: &RateRequestPlan,
    ) -> Result<TradeResponse<OrderOperationAck>, OkxError> {
        validate_instrument_id(&request.instrument_id)?;
        validate_client_id("clOrdId", &request.client_order_id)?;
        Ok(self
            .client
            .private_post_after_admission(
                CANCEL_ORDER_PATH,
                request,
                timing.request_timestamp(),
                None,
                rate_plan,
            )
            .await?
            .into())
    }

    pub async fn cancel_order(
        &self,
        request: &CancelOrderRequest,
        timing: &MutationTiming,
    ) -> Result<TradeResponse<OrderOperationAck>, OkxError> {
        let rate_plan = self.admit_cancel_order(request)?;
        self.cancel_order_after_admission(request, timing, &rate_plan)
            .await
    }

    pub fn admit_cancel_algo_order(
        &self,
        request: &CancelAlgoOrderRequest,
    ) -> Result<RateRequestPlan, OkxError> {
        validate_instrument_id(&request.instrument_id)?;
        if request.algo_order_id.is_empty()
            || request.algo_order_id.len() > 64
            || !request.algo_order_id.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(OkxError::Config("algoId must be a numeric exchange order ID".to_owned()));
        }
        let plan = self.client.rate_budget().private_rest_plan(
            CANCEL_ALGO_PATH,
            &[("instId", request.instrument_id.clone())],
        );
        self.client
            .rate_budget()
            .admit(&plan)
            .map_err(|evidence| OkxError::RateLimited { evidence })?;
        Ok(plan)
    }

    pub async fn cancel_algo_order_after_admission(
        &self,
        request: &CancelAlgoOrderRequest,
        timing: &MutationTiming,
        plan: &RateRequestPlan,
    ) -> Result<TradeResponse<CancelAlgoOrderAck>, OkxError> {
        validate_instrument_id(&request.instrument_id)?;
        if request.algo_order_id.is_empty()
            || !request.algo_order_id.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(OkxError::Config("invalid exact algoId".to_owned()));
        }
        Ok(self
            .client
            .private_post_after_admission(
                CANCEL_ALGO_PATH,
                std::slice::from_ref(request),
                timing.request_timestamp(),
                None,
                plan,
            )
            .await?
            .into())
    }

    pub fn admit_amend_order(
        &self,
        request: &AmendOrderRequest,
    ) -> Result<RateRequestPlan, OkxError> {
        validate_amend(request)?;
        let rate_plan = self.client.rate_budget().trade_rest_plan(
            RateOperationClass::AmendOrder,
            &request.instrument_id,
            None,
        );
        self.client
            .rate_budget()
            .admit(&rate_plan)
            .map_err(|evidence| OkxError::RateLimited { evidence })?;
        Ok(rate_plan)
    }

    pub async fn amend_order_after_admission(
        &self,
        request: &AmendOrderRequest,
        timing: &MutationTiming,
        rate_plan: &RateRequestPlan,
    ) -> Result<TradeResponse<OrderOperationAck>, OkxError> {
        validate_amend(request)?;
        Ok(self
            .client
            .private_post_after_admission(
                AMEND_ORDER_PATH,
                request,
                timing.request_timestamp(),
                Some(timing.exp_time_ms()),
                rate_plan,
            )
            .await?
            .into())
    }

    pub async fn amend_order(
        &self,
        request: &AmendOrderRequest,
        timing: &MutationTiming,
    ) -> Result<TradeResponse<OrderOperationAck>, OkxError> {
        let rate_plan = self.admit_amend_order(request)?;
        self.amend_order_after_admission(request, timing, &rate_plan)
            .await
    }

    pub async fn account_rate_limit(&self) -> Result<AccountRateLimitEvidence, OkxError> {
        let rows: Vec<RawAccountRateLimit> = self
            .client
            .private_get(ACCOUNT_RATE_LIMIT_PATH, &[])
            .await?;
        let [row] = rows.as_slice() else {
            return Err(OkxError::Response(format!(
                "expected exactly one account-rate-limit row, found {}",
                rows.len()
            )));
        };

        let demo = self.client.environment().demo;
        let (current_orders_per_2s, source) =
            parse_current_account_rate_limit(demo, &row.current_orders_per_2s)?;
        let next_orders_per_2s = if demo
            && source == AccountRateLimitSource::DemoBaseFallback
            && row.next_orders_per_2s.trim() == "0"
        {
            None
        } else {
            parse_optional_positive_u32("nextAccRateLimit", &row.next_orders_per_2s)?
        };
        let updated_at_ms = parse_account_rate_limit_timestamp(source, &row.updated_at_ms)?;
        let fill_ratio = parse_optional_ratio("fillRatio", &row.fill_ratio)?;
        let main_fill_ratio = parse_optional_ratio("mainFillRatio", &row.main_fill_ratio)?;

        match (source, updated_at_ms) {
            (AccountRateLimitSource::Exchange, Some(exchange_updated_at_ms)) => {
                self.client.rate_budget().update_subaccount_rate_limit(
                    current_orders_per_2s,
                    next_orders_per_2s,
                    exchange_updated_at_ms,
                );
            }
            (AccountRateLimitSource::Exchange, None) => {
                return Err(OkxError::Response(
                    "exchange account-rate-limit timestamp is unavailable".to_owned(),
                ));
            }
            (AccountRateLimitSource::DemoBaseFallback, _) => {}
        }

        Ok(AccountRateLimitEvidence {
            schema: ACCOUNT_RATE_LIMIT_EVIDENCE_SCHEMA_V3,
            source,
            current_orders_per_2s,
            next_orders_per_2s,
            fill_ratio,
            main_fill_ratio,
            updated_at_ms,
        })
    }

    pub async fn order_by_client_id(
        &self,
        instrument_id: &str,
        client_order_id: &str,
    ) -> Result<TradeOrderDetails, OkxError> {
        validate_instrument_id(instrument_id)?;
        validate_client_id("clOrdId", client_order_id)?;

        let mut orders: Vec<TradeOrderDetails> = self
            .client
            .private_get(
                ORDER_DETAILS_PATH,
                &[
                    ("instId", instrument_id.to_owned()),
                    ("clOrdId", client_order_id.to_owned()),
                ],
            )
            .await?;

        if orders.len() != 1 {
            return Err(OkxError::Response(format!(
                "expected exactly one order detail, found {}",
                orders.len()
            )));
        }
        let order = orders.remove(0);
        if order.instrument_id != instrument_id || order.client_order_id != client_order_id {
            return Err(OkxError::Response(
                "order detail identity does not match requested instId/clOrdId".to_owned(),
            ));
        }
        Ok(order)
    }

    pub async fn algo_order_by_client_id(
        &self,
        client_order_id: &str,
    ) -> Result<TradeAlgoOrderDetails, OkxError> {
        validate_client_id("algoClOrdId", client_order_id)?;

        let mut orders: Vec<TradeAlgoOrderDetails> = self
            .client
            .private_get(
                ALGO_ORDER_DETAILS_PATH,
                &[("algoClOrdId", client_order_id.to_owned())],
            )
            .await?;

        if orders.len() != 1 {
            return Err(OkxError::Response(format!(
                "expected exactly one algo order detail, found {}",
                orders.len()
            )));
        }
        let order = orders.remove(0);
        if order.client_order_id != client_order_id {
            return Err(OkxError::Response(
                "algo order detail identity does not match requested algoClOrdId".to_owned(),
            ));
        }
        Ok(order)
    }
}

fn parse_current_account_rate_limit(
    demo: bool,
    value: &str,
) -> Result<(u32, AccountRateLimitSource), OkxError> {
    let raw = value.trim();
    if demo && matches!(raw, "" | "0") {
        return Ok((
            DEFAULT_SUBACCOUNT_ORDER_LIMIT_PER_2S,
            AccountRateLimitSource::DemoBaseFallback,
        ));
    }
    parse_positive_u32("accRateLimit", value).map(|value| (value, AccountRateLimitSource::Exchange))
}

fn parse_positive_u32(field: &str, value: &str) -> Result<u32, OkxError> {
    value
        .trim()
        .parse::<u32>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            let raw = value.trim();
            let bounded_raw = if raw.len() <= 64 && raw.is_ascii() {
                raw
            } else {
                "<non-ascii-or-oversized>"
            };
            OkxError::Response(format!(
                "{field} is not a positive integer (raw={bounded_raw:?})"
            ))
        })
}

fn parse_optional_positive_u32(field: &str, value: &str) -> Result<Option<u32>, OkxError> {
    if value.trim().is_empty() {
        Ok(None)
    } else {
        parse_positive_u32(field, value).map(Some)
    }
}

fn parse_account_rate_limit_timestamp(
    source: AccountRateLimitSource,
    value: &str,
) -> Result<Option<u64>, OkxError> {
    if source == AccountRateLimitSource::DemoBaseFallback && value.trim().is_empty() {
        Ok(None)
    } else {
        parse_positive_u64("account-rate-limit ts", value).map(Some)
    }
}

fn parse_positive_u64(field: &str, value: &str) -> Result<u64, OkxError> {
    value
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            let raw = value.trim();
            let bounded_raw = if raw.len() <= 64 && raw.is_ascii() {
                raw
            } else {
                "<non-ascii-or-oversized>"
            };
            OkxError::Response(format!(
                "{field} is not a positive integer (raw={bounded_raw:?})"
            ))
        })
}

fn parse_optional_ratio(field: &str, value: &str) -> Result<Option<String>, OkxError> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'.')
        || value.matches('.').count() > 1
    {
        return Err(OkxError::Response(format!(
            "{field} is not a decimal ratio"
        )));
    }
    Ok(Some(value.to_owned()))
}

fn validate_place(request: &PlaceOrderRequest) -> Result<(), OkxError> {
    validate_instrument_id(&request.instrument_id)?;
    validate_client_id("clOrdId", &request.client_order_id)?;
    validate_nonempty("sz", &request.size)?;
    validate_nonempty("px", &request.price)?;
    for attached in &request.attached_algo_orders {
        validate_client_id("attachAlgoClOrdId", &attached.client_order_id)?;
        validate_nonempty("tpTriggerPx", &attached.take_profit_trigger_price)?;
        validate_nonempty("tpOrdPx", &attached.take_profit_order_price)?;
        validate_nonempty("slTriggerPx", &attached.stop_loss_trigger_price)?;
        validate_nonempty("slOrdPx", &attached.stop_loss_order_price)?;
    }
    Ok(())
}

fn validate_amend(request: &AmendOrderRequest) -> Result<(), OkxError> {
    validate_instrument_id(&request.instrument_id)?;
    validate_client_id("clOrdId", &request.client_order_id)?;
    validate_client_id("reqId", &request.request_id)?;

    if request.new_size.is_none() && request.new_price.is_none() {
        return Err(OkxError::Config(
            "amend order requires newSz and/or newPx".to_owned(),
        ));
    }
    if let Some(size) = request.new_size.as_deref() {
        validate_nonempty("newSz", size)?;
    }
    if let Some(price) = request.new_price.as_deref() {
        validate_nonempty("newPx", price)?;
    }
    Ok(())
}

fn validate_instrument_id(value: &str) -> Result<(), OkxError> {
    if value.trim().is_empty() || value.len() > 128 || !value.is_ascii() {
        Err(OkxError::Config("instId is invalid".to_owned()))
    } else {
        Ok(())
    }
}

fn validate_client_id(field: &str, value: &str) -> Result<(), OkxError> {
    if value.is_empty()
        || value.len() > 32
        || !value.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        Err(OkxError::Config(format!(
            "{field} must be 1..=32 ASCII alphanumeric characters"
        )))
    } else {
        Ok(())
    }
}

fn validate_nonempty(field: &str, value: &str) -> Result<(), OkxError> {
    if value.trim().is_empty() || value.len() > 128 {
        Err(OkxError::Config(format!("{field} is invalid")))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending_algo(id: &str, kind: &str, inst_type: &str) -> PendingAlgoOrderDetails {
        serde_json::from_value(serde_json::json!({
            "algoId": id,
            "algoClOrdId": "okx1234567",
            "instType": inst_type,
            "instId": "BTC-USDT-SWAP",
            "ordType": kind,
            "state": "live"
        }))
        .expect("fixture")
    }

    fn all_protective_scopes(
        swap_conditional: Vec<PendingAlgoOrderDetails>,
        swap_oco: Vec<PendingAlgoOrderDetails>,
    ) -> Vec<(String, String, Vec<PendingAlgoOrderDetails>)> {
        vec![
            (
                "SWAP".to_owned(),
                "conditional".to_owned(),
                swap_conditional,
            ),
            ("SWAP".to_owned(), "oco".to_owned(), swap_oco),
            ("FUTURES".to_owned(), "conditional".to_owned(), vec![]),
            ("FUTURES".to_owned(), "oco".to_owned(), vec![]),
        ]
    }

    #[test]
    fn pending_protection_inventory_requires_all_four_exact_query_scopes() {
        assert_eq!(ALGO_PENDING_PATH, "/api/v5/trade/orders-algo-pending");
        let inv = normalize_protective_algo_inventory(all_protective_scopes(
            vec![pending_algo("111", "conditional", "SWAP")],
            vec![pending_algo("222", "oco", "SWAP")],
        ))
        .expect("every scope checked");
        assert_eq!(inv.rows, 2);
        assert_eq!(inv.samples.len(), 2);
        assert!(inv.complete_within_bound);
        let empty = normalize_protective_algo_inventory(all_protective_scopes(vec![], vec![]))
            .expect("explicitly queried all four scopes");
        assert_eq!(empty.rows, 0);
        assert!(empty.complete_within_bound);
        let missing = all_protective_scopes(vec![], vec![])
            .into_iter()
            .take(3)
            .collect();
        assert!(normalize_protective_algo_inventory(missing).is_err());
    }

    #[test]
    fn pending_algo_inventory_rejects_cross_type_and_duplicate_scope() {
        let bad = all_protective_scopes(vec![pending_algo("111", "oco", "SWAP")], vec![]);
        assert!(normalize_protective_algo_inventory(bad).is_err());
        let mut duplicate = all_protective_scopes(vec![], vec![]);
        duplicate.push(("SWAP".to_owned(), "conditional".to_owned(), vec![]));
        assert!(normalize_protective_algo_inventory(duplicate).is_err());
        let dup_algo = all_protective_scopes(
            vec![pending_algo("111", "conditional", "SWAP")],
            vec![pending_algo("111", "oco", "SWAP")],
        );
        assert!(normalize_protective_algo_inventory(dup_algo).is_err());
    }

    #[test]
    fn pending_algo_inventory_paused_orders_and_full_pages_are_not_empty() {
        let mut paused = pending_algo("111", "conditional", "SWAP");
        paused.state = "pause".to_owned();
        let inv = normalize_protective_algo_inventory(all_protective_scopes(vec![paused], vec![]))
            .expect("paused is still pending");
        assert_eq!(inv.rows, 1);
        let full = normalize_protective_algo_inventory(all_protective_scopes(
            (0..PROTECTIVE_ALGO_PAGE_LIMIT)
                .map(|n| pending_algo(&format!("{n}"), "conditional", "SWAP"))
                .collect(),
            vec![],
        ))
        .expect("page bound");
        assert_eq!(full.rows, PROTECTIVE_ALGO_PAGE_LIMIT);
        assert!(!full.complete_within_bound);
        assert_eq!(full.samples.len(), PROTECTIVE_ALGO_SAMPLE_LIMIT);
    }

    #[test]
    fn one_exact_algo_cancel_wire_contract_is_an_array_and_not_a_bulk_request() {
        let one = CancelAlgoOrderRequest {
            instrument_id: "BTC-USDT-SWAP".to_owned(),
            algo_order_id: "1234567890".to_owned(),
        };
        assert_eq!(serde_json::to_string(std::slice::from_ref(&one)).expect("JSON"),
            r#"[{"instId":"BTC-USDT-SWAP","algoId":"1234567890"}]"#);
        let ack: CancelAlgoOrderAck = serde_json::from_value(serde_json::json!({
            "algoId":"1234567890", "sCode":"0", "sMsg":""
        })).expect("ack");
        assert_eq!(ack.algo_order_id, one.algo_order_id);
        assert_eq!(ack.status_code, "0");
    }

    fn place() -> PlaceOrderRequest {
        PlaceOrderRequest {
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            trade_mode: ApiTradeMode::Cross,
            client_order_id: "okx01234567890123456789012345678".to_owned(),
            side: ApiOrderSide::Buy,
            position_side: ApiPositionSide::Long,
            order_type: ApiOrderType::Limit,
            size: "1.25".to_owned(),
            price: "0.10000".to_owned(),
            attached_algo_orders: Vec::new(),
        }
    }

    #[test]
    fn long_short_place_payload_is_exact_and_has_no_reduce_only_field() {
        let encoded = serde_json::to_value(place()).expect("serialize");
        assert_eq!(encoded["instId"], "DOGE-USDT-SWAP");
        assert_eq!(encoded["tdMode"], "cross");
        assert_eq!(encoded["side"], "buy");
        assert_eq!(encoded["posSide"], "long");
        assert_eq!(encoded["ordType"], "limit");
        assert_eq!(encoded["sz"], "1.25");
        assert_eq!(encoded["px"], "0.10000");
        assert!(encoded.get("reduceOnly").is_none());
    }

    #[test]
    fn attached_tp_sl_payload_is_exact_and_explicit() {
        let mut request = place();
        request.attached_algo_orders = vec![AttachedAlgoOrderRequest {
            client_order_id: "prx01234567890123456789012345678".to_owned(),
            take_profit_trigger_price: "0.12000".to_owned(),
            take_profit_trigger_price_type: ApiTriggerPriceType::Mark,
            take_profit_order_price: "-1".to_owned(),
            stop_loss_trigger_price: "0.09000".to_owned(),
            stop_loss_trigger_price_type: ApiTriggerPriceType::Mark,
            stop_loss_order_price: "-1".to_owned(),
        }];

        validate_place(&request).expect("valid attached protection");
        let encoded = serde_json::to_value(request).expect("serialize");
        let attached = &encoded["attachAlgoOrds"][0];
        assert_eq!(
            attached["attachAlgoClOrdId"],
            "prx01234567890123456789012345678"
        );
        assert_eq!(attached["tpTriggerPx"], "0.12000");
        assert_eq!(attached["tpTriggerPxType"], "mark");
        assert_eq!(attached["tpOrdPx"], "-1");
        assert_eq!(attached["slTriggerPx"], "0.09000");
        assert_eq!(attached["slTriggerPxType"], "mark");
        assert_eq!(attached["slOrdPx"], "-1");
    }

    #[test]
    fn algo_detail_preserves_protective_identity_and_terms() {
        let detail: TradeAlgoOrderDetails = serde_json::from_str(
            r#"{
                "instId":"DOGE-USDT-SWAP",
                "algoId":"123456",
                "algoClOrdId":"prx01234567890123456789012345678",
                "state":"effective",
                "tpTriggerPx":"0.12",
                "tpTriggerPxType":"mark",
                "tpOrdPx":"-1",
                "slTriggerPx":"0.09",
                "slTriggerPxType":"mark",
                "slOrdPx":"-1",
                "failCode":""
            }"#,
        )
        .expect("decode");
        assert_eq!(detail.instrument_id, "DOGE-USDT-SWAP");
        assert_eq!(detail.algo_order_id, "123456");
        assert_eq!(detail.client_order_id, "prx01234567890123456789012345678");
        assert_eq!(detail.state, "effective");
    }

    #[test]
    fn amend_omits_unchanged_fields_and_preserves_cancel_policy() {
        let request = AmendOrderRequest {
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            client_order_id: "okx01234567890123456789012345678".to_owned(),
            request_id: "amend000000000000000000000000001".to_owned(),
            cancel_on_fail: false,
            new_size: None,
            new_price: Some("0.10100".to_owned()),
        };
        validate_amend(&request).expect("valid");
        let encoded = serde_json::to_value(request).expect("serialize");
        assert!(encoded.get("newSz").is_none());
        assert_eq!(encoded["newPx"], "0.10100");
        assert_eq!(encoded["cxlOnFail"], false);
    }

    #[test]
    fn exchange_item_error_is_preserved_even_when_top_level_succeeds() {
        let envelope: ApiEnvelope<OrderOperationAck> = serde_json::from_str(
            r#"{
                "code":"0",
                "msg":"",
                "data":[{
                    "ordId":"",
                    "clOrdId":"okx01234567890123456789012345678",
                    "ts":"1790000000000",
                    "sCode":"51008",
                    "sMsg":"insufficient balance"
                }],
                "inTime":"1790000000000000",
                "outTime":"1790000000001000"
            }"#,
        )
        .expect("OKX envelope");
        let response: TradeResponse<OrderOperationAck> = envelope.into();

        assert!(response.top_level_success());
        assert_eq!(response.data.len(), 1);
        assert!(!response.data[0].accepted());
        assert_eq!(response.data[0].status_code, "51008");
    }

    #[test]
    fn client_ids_are_strictly_bounded_alphanumeric() {
        validate_client_id("clOrdId", "Abc123").expect("valid");
        assert!(validate_client_id("clOrdId", "bad_id").is_err());
        assert!(validate_client_id("clOrdId", "").is_err());
        assert!(validate_client_id("clOrdId", &"a".repeat(33)).is_err());
    }

    #[test]
    fn amend_requires_a_real_change() {
        let request = AmendOrderRequest {
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            client_order_id: "okx01234567890123456789012345678".to_owned(),
            request_id: "amend000000000000000000000000001".to_owned(),
            cancel_on_fail: false,
            new_size: None,
            new_price: None,
        };
        assert!(validate_amend(&request).is_err());
    }

    #[test]
    fn account_rate_limit_evidence_preserves_non_vip_empty_ratios() {
        let raw: RawAccountRateLimit = serde_json::from_str(
            r#"{
                "accRateLimit":"1000",
                "fillRatio":"",
                "mainFillRatio":"",
                "nextAccRateLimit":"",
                "ts":"1790884800000"
            }"#,
        )
        .expect("rate row");

        assert_eq!(
            parse_positive_u32("accRateLimit", &raw.current_orders_per_2s).expect("current"),
            1000
        );
        assert_eq!(
            parse_optional_positive_u32("nextAccRateLimit", &raw.next_orders_per_2s).expect("next"),
            None
        );
        assert_eq!(
            parse_optional_ratio("fillRatio", &raw.fill_ratio).expect("ratio"),
            None
        );
        assert_eq!(
            parse_positive_u64("ts", &raw.updated_at_ms).expect("timestamp"),
            1_790_884_800_000
        );
    }

    #[test]
    fn account_rate_limit_demo_sentinel_uses_explicit_base_fallback() {
        for raw in ["", "0"] {
            let (current, source) =
                parse_current_account_rate_limit(true, raw).expect("known Demo sentinel");
            assert_eq!(current, DEFAULT_SUBACCOUNT_ORDER_LIMIT_PER_2S);
            assert_eq!(source, AccountRateLimitSource::DemoBaseFallback);
        }

        let (current, source) =
            parse_current_account_rate_limit(true, "1500").expect("positive Demo limit");
        assert_eq!(current, 1500);
        assert_eq!(source, AccountRateLimitSource::Exchange);
    }

    #[test]
    fn account_rate_limit_production_and_malformed_evidence_remain_fail_closed() {
        assert!(parse_current_account_rate_limit(false, "").is_err());
        assert!(parse_current_account_rate_limit(false, "0").is_err());

        let malformed = parse_current_account_rate_limit(true, "abc")
            .expect_err("malformed Demo value must remain rejected")
            .to_string();
        assert!(malformed.contains("raw="));
        assert!(malformed.contains("abc"));

        let oversized = "x".repeat(65);
        let bounded = parse_current_account_rate_limit(true, &oversized)
            .expect_err("oversized Demo value must remain rejected")
            .to_string();
        assert!(bounded.contains("<non-ascii-or-oversized>"));
        assert!(!bounded.contains(&oversized));

        assert!(parse_optional_ratio("fillRatio", "1.2.3").is_err());
    }

    #[test]
    fn demo_base_rate_limit_allows_only_missing_exchange_timestamp() {
        assert_eq!(
            parse_account_rate_limit_timestamp(AccountRateLimitSource::DemoBaseFallback, "")
                .expect("Demo fallback timestamp"),
            None
        );
        assert_eq!(
            parse_account_rate_limit_timestamp(
                AccountRateLimitSource::DemoBaseFallback,
                "1790884800000",
            )
            .expect("positive timestamp"),
            Some(1_790_884_800_000)
        );
        assert!(parse_account_rate_limit_timestamp(AccountRateLimitSource::Exchange, "").is_err());
        assert!(
            parse_account_rate_limit_timestamp(AccountRateLimitSource::DemoBaseFallback, "0")
                .is_err()
        );
    }

    #[test]
    fn account_rate_limit_timestamp_error_preserves_bounded_raw_evidence() {
        let zero = parse_positive_u64("account-rate-limit ts", "0")
            .expect_err("zero timestamp must remain rejected")
            .to_string();
        assert!(zero.contains("raw="));
        assert!(zero.contains('0'));

        let malformed = parse_positive_u64("account-rate-limit ts", "abc")
            .expect_err("malformed timestamp must remain rejected")
            .to_string();
        assert!(malformed.contains("raw="));
        assert!(malformed.contains("abc"));

        let oversized = "x".repeat(65);
        let bounded = parse_positive_u64("account-rate-limit ts", &oversized)
            .expect_err("oversized timestamp must remain rejected")
            .to_string();
        assert!(bounded.contains("<non-ascii-or-oversized>"));
        assert!(!bounded.contains(&oversized));
    }
}

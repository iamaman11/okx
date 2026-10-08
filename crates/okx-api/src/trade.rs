use serde::{Deserialize, Serialize};

use crate::{
    MutationTiming, OkxRestClient, RateOperationClass, RateRequestPlan, client::ApiEnvelope,
    error::OkxError,
};

const PLACE_ORDER_PATH: &str = "/api/v5/trade/order";
const CANCEL_ORDER_PATH: &str = "/api/v5/trade/cancel-order";
const AMEND_ORDER_PATH: &str = "/api/v5/trade/amend-order";
const ORDER_DETAILS_PATH: &str = "/api/v5/trade/order";
const ALGO_ORDER_DETAILS_PATH: &str = "/api/v5/trade/order-algo";
const ACCOUNT_RATE_LIMIT_PATH: &str = "/api/v5/trade/account-rate-limit";
pub const ACCOUNT_RATE_LIMIT_EVIDENCE_SCHEMA_V1: &str = "okx.account-rate-limit/v1";

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountRateLimitEvidence {
    pub schema: &'static str,
    pub current_orders_per_2s: u32,
    pub next_orders_per_2s: Option<u32>,
    pub fill_ratio: Option<String>,
    pub main_fill_ratio: Option<String>,
    pub updated_at_ms: u64,
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

#[derive(Clone)]
pub struct TradeApi {
    client: OkxRestClient,
}

impl TradeApi {
    pub fn new(client: OkxRestClient) -> Self {
        Self { client }
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

        let current_orders_per_2s = parse_positive_u32("accRateLimit", &row.current_orders_per_2s)?;
        let next_orders_per_2s =
            parse_optional_positive_u32("nextAccRateLimit", &row.next_orders_per_2s)?;
        let updated_at_ms = parse_positive_u64("account-rate-limit ts", &row.updated_at_ms)?;
        let fill_ratio = parse_optional_ratio("fillRatio", &row.fill_ratio)?;
        let main_fill_ratio = parse_optional_ratio("mainFillRatio", &row.main_fill_ratio)?;

        self.client.rate_budget().update_subaccount_rate_limit(
            current_orders_per_2s,
            next_orders_per_2s,
            updated_at_ms,
        );

        Ok(AccountRateLimitEvidence {
            schema: ACCOUNT_RATE_LIMIT_EVIDENCE_SCHEMA_V1,
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

fn parse_positive_u64(field: &str, value: &str) -> Result<u64, OkxError> {
    value
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| OkxError::Response(format!("{field} is not a positive integer")))
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
    fn account_rate_limit_rejects_malformed_exchange_evidence_with_bounded_raw_value() {
        let zero = parse_positive_u32("accRateLimit", "0")
            .expect_err("zero must remain rejected")
            .to_string();
        assert!(zero.contains("raw=\\\"0\\\""));

        let malformed = parse_positive_u32("accRateLimit", "abc")
            .expect_err("malformed must remain rejected")
            .to_string();
        assert!(malformed.contains("raw=\\\"abc\\\""));

        let oversized = "x".repeat(65);
        let bounded = parse_positive_u32("accRateLimit", &oversized)
            .expect_err("oversized must remain rejected")
            .to_string();
        assert!(bounded.contains("<non-ascii-or-oversized>"));
        assert!(!bounded.contains(&oversized));

        assert!(parse_optional_ratio("fillRatio", "1.2.3").is_err());
    }
}

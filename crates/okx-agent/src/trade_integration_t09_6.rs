// T09.6 continuation: two lots owned by two separate entry intents must be
// closed through the one public TradeService, never by writing SQL positions
// or creating a private synthetic close engine.
struct T09_6CloseContext {
    trade: Arc<TradeService>,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    opening_intent_id: String,
    operation_key: String,
    quantity: &'static str,
    venue_order_id: &'static str,
    expected_post_count: usize,
}

async fn t09_6_close_owned_lot(input: T09_6CloseContext) -> serde_json::Value {
    use crate::accounts::recorded_support::{RecordedGet, RecordedPost};
    use okx_protocol::trading::TradeRequest;
    let T09_6CloseContext {
        trade, feed, opening_intent_id, operation_key, quantity,
        venue_order_id, expected_post_count,
    } = input;
    let at=chrono::Utc::now().timestamp_millis().to_string();
    let raw=|v|serde_json::to_vec(&serde_json::json!({
        "code":"0","msg":"","data":v
    })).expect("signed test HTTP envelope");
    feed.expect_public_clock(raw(serde_json::json!([{"ts":at}])));
    feed.expect_post(RecordedPost {
        access_key:"fixture-executor-a".into(),
        path:"/api/v5/trade/order".into(),
        expected_body:serde_json::json!({
            "instId":"BTC-USDT-SWAP","tdMode":"cross",
            "clOrdId":"__REQUEST_CL_ORD_ID__","side":"sell",
            "posSide":"long","ordType":"ioc","sz":quantity,"px":"999"
        }),
        raw_response:raw(serde_json::json!([{
            "ordId":venue_order_id,"clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"
        }])),
    });
    feed.expect_get(RecordedGet {
        access_key:"fixture-executor-a".into(),
        path:"/api/v5/trade/order".into(),
        required_query:vec![("instId".into(),"BTC-USDT-SWAP".into())],
        raw_response:raw(serde_json::json!([{
            "instId":"BTC-USDT-SWAP","ordId":venue_order_id,
            "clOrdId":"__REQUEST_CL_ORD_ID__","side":"sell","posSide":"long",
            "tdMode":"cross","ordType":"ioc","px":"999","sz":quantity,
            "accFillSz":"0","avgPx":"","state":"live","cTime":at,"uTime":at
        }])),
    });
    let close=trade.trade("fixture-a",&TradeRequest::Close {
        idempotency_key:operation_key,
        reference:opening_intent_id.clone(),
        expires_in_ms:30_000,
    },None).await.unwrap_or_else(|reason|panic!(
        "T09_6_PUBLIC_CLOSE_DENIED:source_intent={opening_intent_id}:quantity={quantity}:reason={reason}:posts={:?}",
        feed.outbound_posts.lock().unwrap().len()
    ));
    let intent=&close["data"]["intent"];
    assert_eq!(intent["exchange_order_id"],venue_order_id,
        "T09.6 requires exact authenticated venue order ACK");
    assert_eq!(intent["plan"]["quantity"],quantity,
        "T09.6 must reduce only the original own lot quantity");
    let sends=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(sends,expected_post_count,
        "T09.6 public Close must send exactly one create per separately owned lot");
    assert_eq!(feed.unconsumed_posts(),0,
        "T09.6 exact signed close response must have been consumed");
    serde_json::json!({
        "source_intent_id":opening_intent_id,
        "close_intent_id":intent["intent_id"],
        "venue_order_id":venue_order_id,
        "quantity":quantity,
        "post_count":sends,
        "status":close["status"],
        "financial_complete":close["data"]["financial_complete"],
    })
}
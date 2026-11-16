//! Real public REST parsing and original coordinator publication boundaries.
use super::*;
use crate::public::{PublicConnectionState, PublicWsCoordinator};
use okx_api::{
    HttpRequest, HttpSend, HttpStatusCode, HttpTransport, OkxEnvironment, OkxError,
    OkxPublicClient, PublicInstrument, RateBudget, RawHttpResponse,
};
use okx_ws::{LiveWsConnector, WsArg};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, atomic::AtomicUsize},
    time::Duration,
};

struct RawReference {
    pages: Mutex<VecDeque<(&'static str, Vec<u8>)>>,
    reads: AtomicUsize,
}
impl HttpTransport for RawReference {
    fn send(&self, request: HttpRequest, _deadline: Instant) -> HttpSend<'_> {
        Box::pin(async move {
            if request.method() != "GET"
                || request.url().path() != "/api/v5/public/instruments"
                || request.headers().get("OK-ACCESS-KEY").is_some()
            {
                return Err(OkxError::Response("REFERENCE_TEST_UNEXPECTED_ROUTE".into()));
            }
            let (kind, body) = self
                .pages
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| OkxError::Response("REFERENCE_TEST_EXHAUSTED".into()))?;
            assert_eq!(
                request.url().query_pairs().collect::<Vec<_>>(),
                vec![("instType".into(), kind.into())]
            );
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(RawHttpResponse {
                status: HttpStatusCode::OK,
                headers: Default::default(),
                body,
            })
        })
    }
}
fn row(tick: &str) -> Value {
    json!({"instType":"SWAP","instId":"BTC-USDT-SWAP","instFamily":"BTC-USDT",
        "uly":"BTC-USDT","state":"live","ruleType":"normal","settleCcy":"USDT",
        "tickSz":tick,"lotSz":"0.01","minSz":"0.01","ctType":"linear",
        "ctVal":"0.01","ctValCcy":"BTC","groupId":"4"})
}
fn envelope(rows: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({"code":"0","msg":"","data":rows})).unwrap()
}
fn setup(
    tick: &str,
    futures_response: Vec<u8>,
) -> (PublicWsCoordinator, PublicWsHandle, Arc<RawReference>) {
    let raw = Arc::new(RawReference {
        pages: Mutex::new(
            [
                ("SWAP", envelope(json!([row(tick)]))),
                ("FUTURES", futures_response),
            ]
            .into(),
        ),
        reads: AtomicUsize::new(0),
    });
    let environment = OkxEnvironment::new(okx_api::Region::Global, false);
    let rate = RateBudget::new();
    let client =
        OkxPublicClient::with_http_transport(environment, rate.clone(), raw.clone()).unwrap();
    let reference = ReferenceRegistry::from_public(
        (Utc::now() - chrono::Duration::seconds(10)).to_rfc3339(),
        vec![serde_json::from_value::<PublicInstrument>(row("0.1")).unwrap()],
    )
    .unwrap();
    let (coordinator, handle) = PublicWsCoordinator::new_with_connector_and_public_client(
        environment,
        reference,
        rate,
        Arc::new(LiveWsConnector),
        client,
    );
    (coordinator, handle, raw)
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}
async fn book(coordinator: &mut PublicWsCoordinator, handle: &PublicWsHandle) {
    handle.demand_instrument("BTC-USDT-SWAP").await.unwrap();
    let command = coordinator.commands.recv().await.unwrap();
    assert!(!coordinator.apply_command(command).await);
    let arg: WsArg =
        serde_json::from_value(json!({"channel":"books","instId":"BTC-USDT-SWAP"})).unwrap();
    coordinator
        .apply_data(
            0,
            arg,
            Some("snapshot".into()),
            vec![json!({
                "asks":[["1000","10","0","1"]],"bids":[["999.9","10","0","1"]],
                "seqId":10,"prevSeqId":-1,"ts":Utc::now().timestamp_millis().to_string()
            })],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn risk_book_contract_reference_unchanged_rest_preserves_books_and_fresh_reads_are_local() {
    let (mut coordinator, handle, raw) = setup("0.1", envelope(json!([])));
    book(&mut coordinator, &handle).await;
    let old = handle.instrument_rules("BTC-USDT-SWAP").await.unwrap();
    let refresh = handle.ensure_fresh_instrument_rules("BTC-USDT-SWAP", deadline());
    let owner = async {
        let command = coordinator.commands.recv().await.unwrap();
        assert!(!coordinator.apply_command(command).await);
    };
    let (result, ()) = tokio::join!(refresh, owner);
    let current = result.unwrap();
    assert_eq!(old.instrument, current.instrument);
    assert_eq!(old.reference_generation, current.reference_generation);
    assert_ne!(old.source_received_at, current.source_received_at);
    assert_eq!(
        handle.state.read().await.markets["BTC-USDT-SWAP"].order_book_seq_id(),
        Some(10)
    );
    assert_eq!(raw.reads.load(Ordering::SeqCst), 2);
    assert!(raw.pages.lock().unwrap().is_empty());
    let again = handle
        .ensure_fresh_instrument_rules("BTC-USDT-SWAP", deadline())
        .await
        .unwrap();
    assert_eq!(again, current);
    assert_eq!(
        raw.reads.load(Ordering::SeqCst),
        2,
        "fresh rules do not poll REST"
    );
}

#[tokio::test]
async fn risk_book_contract_reference_changed_rest_denies_plan_and_reconnects() {
    let (mut coordinator, handle, _) = setup("0.2", envelope(json!([])));
    let old = handle.reference_snapshot().await;
    let refresh = handle.ensure_fresh_instrument_rules("BTC-USDT-SWAP", deadline());
    let owner = async {
        let command = coordinator.commands.recv().await.unwrap();
        assert!(coordinator.apply_command(command).await);
    };
    let (result, ()) = tokio::join!(refresh, owner);
    assert!(matches!(
        result,
        Err(PublicRuntimeError::ReferenceRefresh(
            "REFERENCE_REFRESH_CONTENT_CHANGED"
        ))
    ));
    let state = handle.state.read().await;
    assert_ne!(old.generation(), state.reference.generation());
    assert_eq!(
        state.connection_state(),
        PublicConnectionState::Reconnecting
    );
}

#[tokio::test]
async fn risk_book_contract_reference_intervening_ws_event_fences_rest_even_when_content_is_unchanged()
 {
    for ws_tick in ["0.1", "0.3"] {
        let (mut coordinator, handle, _) = setup("0.1", envelope(json!([])));
        let refresh = handle.ensure_fresh_instrument_rules("BTC-USDT-SWAP", deadline());
        let owner = async {
            let command = coordinator.commands.recv().await.unwrap();
            let arg: WsArg =
                serde_json::from_value(json!({"channel":"instruments","instType":"SWAP"})).unwrap();
            coordinator
                .apply_data(0, arg, None, vec![row(ws_tick)])
                .await
                .unwrap();
            let ws_reference = handle.reference_snapshot().await;
            assert!(!coordinator.apply_command(command).await);
            let actual = handle.reference_snapshot().await;
            assert_eq!(
                actual.source_received_at(),
                ws_reference.source_received_at()
            );
            assert_eq!(actual.generation(), ws_reference.generation());
            assert_eq!(actual.get("BTC-USDT-SWAP").unwrap().tick_size, ws_tick);
        };
        let (result, ()) = tokio::join!(refresh, owner);
        assert!(matches!(
            result,
            Err(PublicRuntimeError::ReferenceRefresh(
                "REFERENCE_REFRESH_SOURCE_CHANGED"
            ))
        ));
    }
}

#[tokio::test]
async fn risk_book_contract_reference_failed_partition_or_deadline_preserves_original_source() {
    let failure =
        serde_json::to_vec(&json!({"code":"50000","msg":"source unavailable","data":[]})).unwrap();
    let (mut coordinator, handle, raw) = setup("0.2", failure);
    let old = handle.reference_snapshot().await;
    assert!(
        handle
            .ensure_fresh_instrument_rules("BTC-USDT-SWAP", Instant::now())
            .await
            .is_err()
    );
    assert_eq!(raw.reads.load(Ordering::SeqCst), 0);
    assert!(matches!(
        handle
            .ensure_fresh_instrument_rules("BTC-USDT-SWAP", deadline())
            .await,
        Err(PublicRuntimeError::ReferenceSource(_))
    ));
    assert_eq!(raw.reads.load(Ordering::SeqCst), 2);
    let current = handle.reference_snapshot().await;
    assert_eq!(old.generation(), current.generation());
    assert_eq!(old.source_received_at(), current.source_received_at());
    assert!(coordinator.commands.try_recv().is_err());
}

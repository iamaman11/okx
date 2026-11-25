//! The shipped Engine starts its actual tasks and drains the same Native
//! account registry. External Ctrl-C/stdin and installed CONTROL are separate
//! physical boundaries; this test supplies only the shutdown notification.
use super::*;
use crate::{
    market_bootstrap::MarketBootstrapper,
    query::DispatchContext,
    runtime::{EngineContext, run_engine_until},
};

#[tokio::test]
async fn risk_book_contract_native_engine_tasks_initialize_opening_and_drain_original_owner() {
    let mut fixture = fixture().await;
    let (market, public, _socket) = raw_market();
    let client = okx_api::OkxPublicClient::with_http_transport(
        okx_api::OkxEnvironment::new(okx_api::Region::Global, false),
        okx_api::RateBudget::new(),
        fixture.feed.clone(),
    )
    .unwrap();
    let bootstrap = MarketBootstrapper::new(client);
    fixture.trade = Arc::new(TradeService::new(
        fixture.registry.clone(),
        public.clone(),
        NativeRiskClient::local(NativeFinanceRisk::from_owner(fixture.owner.clone())),
    ));
    fixture
        .trade
        .update_governance(&fixture.authority)
        .await
        .unwrap();
    fixture
        .trade
        .set_live_source(okx_market_runtime::live_source::LiveCandleSource::new(
            bootstrap,
        ))
        .await;
    let dispatch = DispatchContext {
        finance: crate::finance_dispatch::FinanceDispatch::ready(
            fixture.registry.clone(),
            fixture.trade.clone(),
        ),
    };
    let ctx = EngineContext {
        dispatch,
        public: market,
        transport: None,
        mailbox: None,
        private_key: zeroize::Zeroizing::new([0; 32]),
        key_id: "isolated-native-engine".into(),
        poll_seconds: 1,
        native_child_control_stdio: false,
    };
    let owner = fixture.owner.clone();
    let campaign = fixture.campaign.clone();
    let installation = owner.installation_id().to_owned();
    // No test installs a financial opening: the real Engine maintenance task
    // must capture sources and satisfy its actual history cutoff itself.
    let until = async move {
        let mut checks = tokio::time::interval(Duration::from_millis(25));
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                checks.tick().await;
                let summary = owner.campaign_summary(campaign.clone()).await.unwrap();
                if summary.opening.is_some() {
                    assert_eq!(summary.reserved_entry_usd, "0");
                    assert_eq!(summary.consumed_entry_usd, "0");
                    assert_eq!(owner.installation_id(), installation);
                    break;
                }
            }
        })
        .await
        .expect("actual Engine maintenance must become ready before shutdown");
        None
    };
    tokio::time::timeout(Duration::from_secs(20), run_engine_until(ctx, until))
        .await
        .unwrap()
        .unwrap();
    assert!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap()
            .opening
            .is_some()
    );
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());
    assert!(!fixture.root.join("accounts").exists());
    fixture.finish().await;
}

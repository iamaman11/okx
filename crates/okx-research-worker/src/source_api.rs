//! Narrow public-data source capability supplied by the authenticated host.
use okx_protocol::research_ipc::*;
use std::{future::Future, pin::Pin, sync::Arc};
pub type SourceFuture<'a> =
    Pin<Box<dyn Future<Output = Result<PublicSourceReply, String>> + Send + 'a>>;
pub trait PublicSourceProvider: Send + Sync {
    fn request(&self, request: PublicSourceRequest) -> SourceFuture<'_>;
}
#[derive(Clone)]
pub struct PublicSourceClient(Arc<dyn PublicSourceProvider>);
impl PublicSourceClient {
    pub fn new(provider: Arc<dyn PublicSourceProvider>) -> Self {
        Self(provider)
    }
    async fn request(&self, request: PublicSourceRequest) -> Result<PublicSourceReply, String> {
        crate::source_io::request_started(&request);
        let result = self.0.request(request.clone()).await;
        crate::source_io::request_finished(&request, &result);
        result
    }
    pub async fn research_candles_page(
        &self,
        instrument: &str,
        bar: &str,
        after: Option<&str>,
        before: Option<&str>,
        limit: u16,
    ) -> Result<PublicCandleCapture, String> {
        self.candles(instrument, bar, after, before, limit, false)
            .await
    }
    pub async fn analytics_candles_page(
        &self,
        instrument: &str,
        bar: &str,
        after: Option<&str>,
        before: Option<&str>,
        limit: u16,
    ) -> Result<PublicCandleCapture, String> {
        self.candles(instrument, bar, after, before, limit, true)
            .await
    }
    async fn candles(
        &self,
        instrument: &str,
        bar: &str,
        after: Option<&str>,
        before: Option<&str>,
        limit: u16,
        queued: bool,
    ) -> Result<PublicCandleCapture, String> {
        match self
            .request(PublicSourceRequest::Candles {
                instrument: instrument.into(),
                bar: bar.into(),
                after: after.map(str::to_owned),
                before: before.map(str::to_owned),
                limit,
                queued,
            })
            .await?
        {
            PublicSourceReply::Candles(value) => Ok(value),
            _ => Err("RESEARCH_SOURCE_KIND_MISMATCH".into()),
        }
    }
    pub async fn research_funding_page(
        &self,
        instrument: &str,
        after: Option<&str>,
        before: Option<&str>,
        limit: u16,
    ) -> Result<PublicFundingCapture, String> {
        match self
            .request(PublicSourceRequest::Funding {
                instrument: instrument.into(),
                after: after.map(str::to_owned),
                before: before.map(str::to_owned),
                limit,
            })
            .await?
        {
            PublicSourceReply::Funding(value) => Ok(value),
            _ => Err("RESEARCH_SOURCE_KIND_MISMATCH".into()),
        }
    }
    pub async fn research_current_reference(
        &self,
        instrument: &str,
    ) -> Result<PublicReferenceCapture, String> {
        match self
            .request(PublicSourceRequest::Reference {
                instrument: instrument.into(),
            })
            .await?
        {
            PublicSourceReply::Reference(value) => Ok(value),
            _ => Err("RESEARCH_SOURCE_KIND_MISMATCH".into()),
        }
    }
    pub async fn analytics_ticker(&self, instrument: &str) -> Result<ResearchTicker, String> {
        match self
            .request(PublicSourceRequest::Ticker {
                instrument: instrument.into(),
            })
            .await?
        {
            PublicSourceReply::Ticker(value) => Ok(value),
            _ => Err("RESEARCH_SOURCE_KIND_MISMATCH".into()),
        }
    }
    pub async fn analytics_oi(
        &self,
        instrument: &str,
        period: &str,
    ) -> Result<Vec<ResearchOpenInterest>, String> {
        match self
            .request(PublicSourceRequest::OpenInterest {
                instrument: instrument.into(),
                period: period.into(),
            })
            .await?
        {
            PublicSourceReply::OpenInterest(value) => Ok(value),
            _ => Err("RESEARCH_SOURCE_KIND_MISMATCH".into()),
        }
    }
}

// Conformance tests use the same source adapter as the host. No network client
// or credential capability is compiled into the worker's production graph.
#[cfg(test)]
impl From<okx_runtime::market_bootstrap::MarketBootstrapper> for PublicSourceClient {
    fn from(value: okx_runtime::market_bootstrap::MarketBootstrapper) -> Self {
        struct Source(okx_runtime::market_bootstrap::MarketBootstrapper);
        impl PublicSourceProvider for Source {
            fn request(&self, request: PublicSourceRequest) -> SourceFuture<'_> {
                Box::pin(self.0.research_source(request))
            }
        }
        Self::new(Arc::new(Source(value)))
    }
}

#[cfg(test)]
mod source_observation_tests {
    use super::*;

    struct Captured;
    impl PublicSourceProvider for Captured {
        fn request(&self, _: PublicSourceRequest) -> SourceFuture<'_> {
            Box::pin(async {
                Ok(PublicSourceReply::Candles(PublicCandleCapture {
                    raw_body: vec![0, 1, 2],
                    rows: Vec::new(),
                    acquired_at_ms: "1".into(),
                    http_delivery: None,
                }))
            })
        }
    }

    #[tokio::test]
    async fn proxy_observation_includes_real_adapter_call() {
        let client = PublicSourceClient::new(Arc::new(Captured));
        let meter = Arc::new(crate::source_io::SourceIoMeter::default());
        let capture = crate::source_io::observe(meter.clone(), async {
            client
                .research_candles_page("BTC-USDT-SWAP", "1m", None, None, 100)
                .await
        })
        .await
        .unwrap();
        assert_eq!(capture.raw_body.len(), 3);
        let observed = meter.snapshot();
        assert_eq!(
            observed["source_calls"]["candles"]["proxy_requests_started"],
            1
        );
        assert_eq!(observed["source_calls"]["candles"]["proxy_replies"], 1);
        assert_eq!(
            observed["source_calls"]["candles"]["captured_response_body_bytes"],
            3
        );
    }
}

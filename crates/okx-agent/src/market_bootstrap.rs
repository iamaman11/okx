use chrono::{SecondsFormat, Utc};
use okx_api::{MarketDataApi, OkxPublicClient};
use okx_observation::{
    FundingRequirement, MarketBootstrap, MarketError, MarketSnapshot, ReferenceRegistry,
};
use thiserror::Error;

#[derive(Clone)]
pub struct MarketBootstrapper {
    api: MarketDataApi,
}

#[derive(Debug, Error)]
pub enum MarketBootstrapError {
    #[error("instrument '{0}' is not present in the reference registry")]
    ReferenceInstrumentNotFound(String),

    #[error("instrument '{0}' has no underlying/index id in reference data")]
    MissingUnderlying(String),

    #[error("public OKX market API error: {0}")]
    Api(#[from] okx_api::OkxError),

    #[error("market normalization error: {0}")]
    Normalize(#[from] MarketError),
}

impl MarketBootstrapper {
    pub fn new(client: OkxPublicClient) -> Self {
        Self {
            api: MarketDataApi::new(client),
        }
    }

    pub async fn snapshot(
        &self,
        reference: &ReferenceRegistry,
        instrument_id: &str,
    ) -> Result<MarketSnapshot, MarketBootstrapError> {
        let instrument = reference.get(instrument_id).ok_or_else(|| {
            MarketBootstrapError::ReferenceInstrumentNotFound(instrument_id.to_owned())
        })?;
        let instrument_type = instrument.instrument_type;
        let underlying = instrument
            .underlying
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| MarketBootstrapError::MissingUnderlying(instrument_id.to_owned()))?
            .to_owned();

        let (ticker, mark_price, index_ticker, open_interest) = tokio::try_join!(
            self.api.ticker(instrument_id),
            self.api.mark_price(instrument_type, instrument_id),
            self.api.index_ticker(&underlying),
            self.api.open_interest(instrument_type, instrument_id),
        )?;

        let funding_rate = match instrument.funding_requirement {
            FundingRequirement::Required => Some(self.api.funding_rate(instrument_id).await?),
            FundingRequirement::NotApplicable | FundingRequirement::Unknown => None,
        };

        let source_received_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        Ok(MarketSnapshot::from_bootstrap(
            reference,
            instrument_id,
            source_received_at,
            MarketBootstrap {
                ticker,
                mark_price,
                index_ticker,
                funding_rate,
                open_interest,
            },
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_api::PublicInstrument;

    fn reference() -> ReferenceRegistry {
        ReferenceRegistry::from_public(
            "2026-09-27T00:00:00.000Z",
            vec![PublicInstrument {
                instrument_type: "SWAP".to_owned(),
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                instrument_family: "DOGE-USDT".to_owned(),
                underlying: "DOGE-USDT".to_owned(),
                state: "live".to_owned(),
                rule_type: "normal".to_owned(),
                base_currency: String::new(),
                quote_currency: String::new(),
                settle_currency: "USDT".to_owned(),
                tick_size: "0.00001".to_owned(),
                lot_size: "0.01".to_owned(),
                min_size: "0.01".to_owned(),
                max_limit_size: "1000000".to_owned(),
                max_market_size: "100000".to_owned(),
                max_limit_amount: String::new(),
                max_market_amount: String::new(),
                contract_type: "linear".to_owned(),
                contract_value: "1000".to_owned(),
                contract_value_currency: "DOGE".to_owned(),
                fee_group_id: "4".to_owned(),
                lever: "50".to_owned(),
                list_time: "1700000000000".to_owned(),
                expiry_time: String::new(),
            }],
        )
        .expect("reference")
    }

    #[test]
    fn dependency_resolution_uses_reference_underlying() {
        let reference = reference();
        let instrument = reference.get("DOGE-USDT-SWAP").expect("instrument");

        assert_eq!(instrument.instrument_type, okx_api::InstrumentType::Swap);
        assert_eq!(instrument.funding_requirement, FundingRequirement::Required);
        assert_eq!(instrument.underlying.as_deref(), Some("DOGE-USDT"));
    }
}

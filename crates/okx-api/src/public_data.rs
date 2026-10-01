use serde::{Deserialize, Serialize};

use crate::{client::OkxPublicClient, error::OkxError, instrument::InstrumentType};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct UpcomingParameterChange {
    #[serde(default)]
    pub param: String,
    #[serde(rename = "newValue", default)]
    pub new_value: String,
    #[serde(rename = "effTime", default)]
    pub effective_time_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PublicPriceLimit {
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(rename = "buyLmt", default)]
    pub buy_limit: String,
    #[serde(rename = "sellLmt", default)]
    pub sell_limit: String,
    #[serde(rename = "ts", default)]
    pub timestamp_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SystemStatus {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub begin: String,
    #[serde(default)]
    pub end: String,
    #[serde(default)]
    pub href: String,
    #[serde(rename = "serviceType", default)]
    pub service_type: String,
    #[serde(default)]
    pub system: String,
    #[serde(rename = "scheDesc", default)]
    pub schedule_description: String,
    #[serde(rename = "maintType", default)]
    pub maintenance_type: String,
    #[serde(default)]
    pub env: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PublicInstrument {
    #[serde(rename = "instType", default)]
    pub instrument_type: String,
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(rename = "instFamily", default)]
    pub instrument_family: String,
    #[serde(rename = "uly", default)]
    pub underlying: String,
    #[serde(default)]
    pub state: String,
    #[serde(rename = "ruleType", default)]
    pub rule_type: String,
    #[serde(rename = "baseCcy", default)]
    pub base_currency: String,
    #[serde(rename = "quoteCcy", default)]
    pub quote_currency: String,
    #[serde(rename = "settleCcy", default)]
    pub settle_currency: String,
    #[serde(rename = "tickSz", default)]
    pub tick_size: String,
    #[serde(rename = "lotSz", default)]
    pub lot_size: String,
    #[serde(rename = "minSz", default)]
    pub min_size: String,
    #[serde(rename = "maxLmtSz", default)]
    pub max_limit_size: String,
    #[serde(rename = "maxMktSz", default)]
    pub max_market_size: String,
    #[serde(rename = "maxLmtAmt", default)]
    pub max_limit_amount: String,
    #[serde(rename = "maxMktAmt", default)]
    pub max_market_amount: String,
    #[serde(rename = "ctType", default)]
    pub contract_type: String,
    #[serde(rename = "ctVal", default)]
    pub contract_value: String,
    #[serde(rename = "ctValCcy", default)]
    pub contract_value_currency: String,
    #[serde(rename = "groupId", default)]
    pub fee_group_id: String,
    #[serde(default)]
    pub lever: String,
    #[serde(rename = "listTime", default)]
    pub list_time: String,
    #[serde(rename = "expTime", default)]
    pub expiry_time: String,
    #[serde(rename = "initPxLmtPct", default)]
    pub initial_price_limit_pct: String,
    #[serde(rename = "floatPxLmtPct", default)]
    pub floating_price_limit_pct: String,
    #[serde(rename = "maxPxLmtPct", default)]
    pub maximum_price_limit_pct: String,
    #[serde(rename = "upcChg", default)]
    pub upcoming_parameter_changes: Vec<UpcomingParameterChange>,
}

#[derive(Clone)]
pub struct PublicDataApi {
    client: OkxPublicClient,
}

impl PublicDataApi {
    pub fn new(client: OkxPublicClient) -> Self {
        Self { client }
    }

    pub async fn instruments(
        &self,
        instrument_type: InstrumentType,
        instrument_id: Option<&str>,
    ) -> Result<Vec<PublicInstrument>, OkxError> {
        let mut params = vec![("instType", instrument_type.to_string())];
        if let Some(instrument_id) = instrument_id.filter(|value| !value.trim().is_empty()) {
            params.push(("instId", instrument_id.to_owned()));
        }

        self.client
            .public_get("/api/v5/public/instruments", &params)
            .await
    }

    pub async fn derivative_instruments(&self) -> Result<Vec<PublicInstrument>, OkxError> {
        let mut instruments = self.instruments(InstrumentType::Swap, None).await?;
        instruments.extend(self.instruments(InstrumentType::Futures, None).await?);
        Ok(instruments)
    }

    pub async fn price_limit(&self, instrument_id: &str) -> Result<PublicPriceLimit, OkxError> {
        let rows = self
            .client
            .public_get::<PublicPriceLimit>(
                "/api/v5/public/price-limit",
                &[("instId", instrument_id.to_owned())],
            )
            .await?;
        let [row] = rows.as_slice() else {
            return Err(OkxError::Response(format!(
                "expected exactly one price-limit row for {instrument_id}, found {}",
                rows.len()
            )));
        };
        Ok(row.clone())
    }

    pub async fn system_status(&self, state: &str) -> Result<Vec<SystemStatus>, OkxError> {
        self.client
            .public_get(
                "/api/v5/system/status",
                &[("state", state.to_owned())],
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_instrument_preserves_exchange_decimal_text_exactly() {
        let instrument: PublicInstrument = serde_json::from_str(
            r#"{
                "instType":"SWAP",
                "instId":"DOGE-USDT-SWAP",
                "instFamily":"DOGE-USDT",
                "uly":"DOGE-USDT",
                "state":"live",
                "ruleType":"normal",
                "baseCcy":"",
                "quoteCcy":"",
                "settleCcy":"USDT",
                "tickSz":"0.00001",
                "lotSz":"0.01",
                "minSz":"0.01",
                "maxLmtSz":"1000000",
                "maxMktSz":"100000",
                "maxLmtAmt":"",
                "maxMktAmt":"",
                "ctType":"linear",
                "ctVal":"1000",
                "ctValCcy":"DOGE",
                "groupId":"4",
                "lever":"100",
                "listTime":"1700000000000",
                "expTime":"",
                "initPxLmtPct":"0.05",
                "floatPxLmtPct":"0.03",
                "maxPxLmtPct":"0.15",
                "upcChg":[{"param":"tickSz","newValue":"0.000001","effTime":"1790900000000"}]
            }"#,
        )
        .expect("instrument");

        assert_eq!(instrument.tick_size, "0.00001");
        assert_eq!(instrument.lot_size, "0.01");
        assert_eq!(instrument.contract_value, "1000");
        assert_eq!(instrument.initial_price_limit_pct, "0.05");
        assert_eq!(instrument.upcoming_parameter_changes.len(), 1);
        assert_eq!(instrument.upcoming_parameter_changes[0].param, "tickSz");
        assert_eq!(instrument.upcoming_parameter_changes[0].new_value, "0.000001");
    }
}

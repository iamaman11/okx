use std::collections::BTreeMap;

use okx_api::{InstrumentType, PublicInstrument};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const REFERENCE_REGISTRY_SCHEMA_V1: &str = "okx.reference-registry/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ReferenceGeneration(String);

impl ReferenceGeneration {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InstrumentSpec {
    pub instrument_id: String,
    pub instrument_type: InstrumentType,
    pub instrument_family: Option<String>,
    pub underlying: Option<String>,
    pub state: String,
    pub rule_type: Option<String>,
    pub base_currency: Option<String>,
    pub quote_currency: Option<String>,
    pub settle_currency: Option<String>,
    pub tick_size: String,
    pub lot_size: String,
    pub min_size: String,
    pub max_limit_size: Option<String>,
    pub max_market_size: Option<String>,
    pub max_limit_amount: Option<String>,
    pub max_market_amount: Option<String>,
    pub contract_type: Option<String>,
    pub contract_value: Option<String>,
    pub contract_value_currency: Option<String>,
    pub fee_group_id: Option<String>,
    pub max_leverage: Option<String>,
    pub list_time_ms: Option<String>,
    pub expiry_time_ms: Option<String>,
}

#[derive(Debug)]
pub struct ReferenceRegistry {
    source_received_at: String,
    generation: ReferenceGeneration,
    instruments: BTreeMap<String, InstrumentSpec>,
}

#[derive(Debug, Error)]
pub enum ReferenceError {
    #[error("reference source timestamp is empty")]
    EmptySourceTimestamp,

    #[error("unsupported instrument type '{0}'")]
    UnsupportedInstrumentType(String),

    #[error("instrument '{instrument_id}' is missing required field '{field}'")]
    MissingRequiredField {
        instrument_id: String,
        field: &'static str,
    },

    #[error("duplicate instrument '{0}' in reference snapshot")]
    DuplicateInstrument(String),

    #[error("failed to serialize normalized reference registry: {0}")]
    Serialization(#[from] serde_json::Error),
}

impl ReferenceRegistry {
    pub fn from_public(
        source_received_at: impl Into<String>,
        instruments: Vec<PublicInstrument>,
    ) -> Result<Self, ReferenceError> {
        let source_received_at = source_received_at.into();
        if source_received_at.trim().is_empty() {
            return Err(ReferenceError::EmptySourceTimestamp);
        }

        let mut normalized = BTreeMap::new();
        for instrument in instruments {
            let spec = InstrumentSpec::try_from(instrument)?;
            let instrument_id = spec.instrument_id.clone();
            if normalized.insert(instrument_id.clone(), spec).is_some() {
                return Err(ReferenceError::DuplicateInstrument(instrument_id));
            }
        }

        let generation = generation_for(&normalized)?;

        Ok(Self {
            source_received_at,
            generation,
            instruments: normalized,
        })
    }

    pub fn source_received_at(&self) -> &str {
        &self.source_received_at
    }

    pub fn generation(&self) -> &ReferenceGeneration {
        &self.generation
    }

    pub fn get(&self, instrument_id: &str) -> Option<&InstrumentSpec> {
        self.instruments.get(instrument_id)
    }

    pub fn len(&self) -> usize {
        self.instruments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.instruments.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &InstrumentSpec> {
        self.instruments.values()
    }
}

impl TryFrom<PublicInstrument> for InstrumentSpec {
    type Error = ReferenceError;

    fn try_from(value: PublicInstrument) -> Result<Self, Self::Error> {
        let instrument_id = require(&value.instrument_id, &value.instrument_id, "instId")?;
        let instrument_type = match value.instrument_type.as_str() {
            "SWAP" => InstrumentType::Swap,
            "FUTURES" => InstrumentType::Futures,
            other => return Err(ReferenceError::UnsupportedInstrumentType(other.to_owned())),
        };

        Ok(Self {
            instrument_id: instrument_id.to_owned(),
            instrument_type,
            instrument_family: optional(value.instrument_family),
            underlying: optional(value.underlying),
            state: require(&value.instrument_id, &value.state, "state")?.to_owned(),
            rule_type: optional(value.rule_type),
            base_currency: optional(value.base_currency),
            quote_currency: optional(value.quote_currency),
            settle_currency: optional(value.settle_currency),
            tick_size: require(&value.instrument_id, &value.tick_size, "tickSz")?.to_owned(),
            lot_size: require(&value.instrument_id, &value.lot_size, "lotSz")?.to_owned(),
            min_size: require(&value.instrument_id, &value.min_size, "minSz")?.to_owned(),
            max_limit_size: optional(value.max_limit_size),
            max_market_size: optional(value.max_market_size),
            max_limit_amount: optional(value.max_limit_amount),
            max_market_amount: optional(value.max_market_amount),
            contract_type: optional(value.contract_type),
            contract_value: optional(value.contract_value),
            contract_value_currency: optional(value.contract_value_currency),
            fee_group_id: optional(value.fee_group_id),
            max_leverage: optional(value.lever),
            list_time_ms: optional(value.list_time),
            expiry_time_ms: optional(value.expiry_time),
        })
    }
}

fn require<'a>(
    instrument_id: &str,
    value: &'a str,
    field: &'static str,
) -> Result<&'a str, ReferenceError> {
    if value.trim().is_empty() {
        Err(ReferenceError::MissingRequiredField {
            instrument_id: instrument_id.to_owned(),
            field,
        })
    } else {
        Ok(value)
    }
}

fn optional(value: String) -> Option<String> {
    (!value.trim().is_empty()).then_some(value)
}

fn generation_for(
    instruments: &BTreeMap<String, InstrumentSpec>,
) -> Result<ReferenceGeneration, ReferenceError> {
    #[derive(Serialize)]
    struct GenerationInput<'a> {
        schema: &'static str,
        instruments: Vec<&'a InstrumentSpec>,
    }

    let encoded = serde_json::to_vec(&GenerationInput {
        schema: REFERENCE_REGISTRY_SCHEMA_V1,
        instruments: instruments.values().collect(),
    })?;
    let digest = Sha256::digest(encoded);
    Ok(ReferenceGeneration(format!("sha256:{digest:x}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn swap(id: &str) -> PublicInstrument {
        PublicInstrument {
            instrument_type: "SWAP".to_owned(),
            instrument_id: id.to_owned(),
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
            lever: "100".to_owned(),
            list_time: "1700000000000".to_owned(),
            expiry_time: String::new(),
        }
    }

    #[test]
    fn lookup_preserves_exact_exchange_mechanics() {
        let registry = ReferenceRegistry::from_public(
            "2026-09-27T00:00:00.000Z",
            vec![swap("DOGE-USDT-SWAP")],
        )
        .expect("registry");

        let spec = registry.get("DOGE-USDT-SWAP").expect("instrument");
        assert_eq!(spec.tick_size, "0.00001");
        assert_eq!(spec.lot_size, "0.01");
        assert_eq!(spec.contract_value.as_deref(), Some("1000"));
        assert_eq!(spec.contract_value_currency.as_deref(), Some("DOGE"));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn generation_depends_on_content_not_input_order_or_receive_time() {
        let doge = swap("DOGE-USDT-SWAP");
        let mut btc = swap("BTC-USDT-SWAP");
        btc.instrument_family = "BTC-USDT".to_owned();
        btc.underlying = "BTC-USDT".to_owned();
        btc.contract_value_currency = "BTC".to_owned();

        let first = ReferenceRegistry::from_public(
            "2026-09-27T00:00:00.000Z",
            vec![doge.clone(), btc.clone()],
        )
        .expect("registry");
        let second = ReferenceRegistry::from_public(
            "2026-09-27T00:01:00.000Z",
            vec![btc, doge],
        )
        .expect("registry");

        assert_eq!(first.generation(), second.generation());
    }

    #[test]
    fn duplicate_instrument_fails_closed() {
        let error = ReferenceRegistry::from_public(
            "2026-09-27T00:00:00.000Z",
            vec![swap("DOGE-USDT-SWAP"), swap("DOGE-USDT-SWAP")],
        )
        .expect_err("duplicate must fail");

        assert!(matches!(error, ReferenceError::DuplicateInstrument(_)));
    }

    #[test]
    fn missing_required_mechanics_fail_closed() {
        let mut instrument = swap("DOGE-USDT-SWAP");
        instrument.tick_size.clear();

        let error = ReferenceRegistry::from_public(
            "2026-09-27T00:00:00.000Z",
            vec![instrument],
        )
        .expect_err("missing tick must fail");

        assert!(matches!(
            error,
            ReferenceError::MissingRequiredField {
                field: "tickSz",
                ..
            }
        ));
    }
}

use std::collections::{BTreeMap, BTreeSet};

use okx_api::{
    Instrument as AccountInstrument, InstrumentType, MaxOrderSize, PublicInstrument,
    PublicPriceLimit, SystemStatus,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const REFERENCE_REGISTRY_SCHEMA_V1: &str = "okx.reference-registry/v1";
pub const INSTRUMENT_RULES_SCHEMA_V1: &str = "okx.instrument-rules/v1";
pub const INSTRUMENT_SEARCH_SCHEMA_V1: &str = "okx.instrument-search/v1";
pub const VENUE_EXECUTION_EVIDENCE_SCHEMA_V1: &str = "okx.venue-execution-evidence/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ReferenceGeneration(String);

impl ReferenceGeneration {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FundingRequirement {
    Required,
    NotApplicable,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UpcomingRuleChange {
    pub param: String,
    pub new_value: String,
    pub effective_time_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InstrumentSpec {
    pub instrument_id: String,
    pub instrument_type: InstrumentType,
    pub instrument_family: Option<String>,
    pub underlying: Option<String>,
    pub state: String,
    pub rule_type: Option<String>,
    pub funding_requirement: FundingRequirement,
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
    pub initial_price_limit_pct: Option<String>,
    pub floating_price_limit_pct: Option<String>,
    pub maximum_price_limit_pct: Option<String>,
    pub upcoming_rule_changes: Vec<UpcomingRuleChange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountInstrumentExecutionLimits {
    pub state: String,
    pub max_limit_size: Option<String>,
    pub max_market_size: Option<String>,
    pub position_limit_amount_usd: Option<String>,
    pub position_limit_pct: Option<String>,
    pub platform_open_interest_limit_usd: Option<String>,
    pub platform_open_interest_limit_coin: Option<String>,
    pub long_position_remaining_quota_usd: Option<String>,
    pub short_position_remaining_quota_usd: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PriceLimitEvidence {
    pub instrument_id: String,
    pub buy_limit: String,
    pub sell_limit: String,
    pub exchange_timestamp_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MaxOrderSizeEvidence {
    pub instrument_id: String,
    pub max_buy: String,
    pub max_sell: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SystemStatusEvidence {
    pub id: String,
    pub state: String,
    pub service_type: String,
    pub system: String,
    pub maintenance_type: String,
    pub environment: String,
    pub begin_ms: String,
    pub end_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VenueExecutionEvidence {
    pub schema: &'static str,
    pub source_received_at: String,
    pub public_instrument: InstrumentSpec,
    pub account_instrument: AccountInstrumentExecutionLimits,
    pub price_limit: PriceLimitEvidence,
    pub max_order_size: Option<MaxOrderSizeEvidence>,
    pub ongoing_system_statuses: Vec<SystemStatusEvidence>,
}

impl VenueExecutionEvidence {
    pub fn from_okx(
        source_received_at: impl Into<String>,
        public_instrument: PublicInstrument,
        account_instrument: AccountInstrument,
        price_limit: PublicPriceLimit,
        max_order_size: Option<MaxOrderSize>,
        ongoing_system_statuses: Vec<SystemStatus>,
    ) -> Result<Self, ReferenceError> {
        let source_received_at = source_received_at.into();
        if source_received_at.trim().is_empty() {
            return Err(ReferenceError::EmptySourceTimestamp);
        }
        Ok(Self {
            schema: VENUE_EXECUTION_EVIDENCE_SCHEMA_V1,
            source_received_at,
            public_instrument: InstrumentSpec::try_from(public_instrument)?,
            account_instrument: AccountInstrumentExecutionLimits {
                state: account_instrument.state,
                max_limit_size: optional(account_instrument.max_limit_size),
                max_market_size: optional(account_instrument.max_market_size),
                position_limit_amount_usd: optional(account_instrument.position_limit_amount_usd),
                position_limit_pct: optional(account_instrument.position_limit_pct),
                platform_open_interest_limit_usd: optional(
                    account_instrument.platform_open_interest_limit_usd,
                ),
                platform_open_interest_limit_coin: optional(
                    account_instrument.platform_open_interest_limit_coin,
                ),
                long_position_remaining_quota_usd: optional(
                    account_instrument.long_position_remaining_quota_usd,
                ),
                short_position_remaining_quota_usd: optional(
                    account_instrument.short_position_remaining_quota_usd,
                ),
            },
            price_limit: PriceLimitEvidence {
                instrument_id: price_limit.instrument_id,
                buy_limit: price_limit.buy_limit,
                sell_limit: price_limit.sell_limit,
                exchange_timestamp_ms: price_limit.timestamp_ms,
            },
            max_order_size: max_order_size.map(|max_order_size| MaxOrderSizeEvidence {
                instrument_id: max_order_size.instrument_id,
                max_buy: max_order_size.max_buy,
                max_sell: max_order_size.max_sell,
            }),
            ongoing_system_statuses: ongoing_system_statuses
                .into_iter()
                .map(|status| SystemStatusEvidence {
                    id: status.id,
                    state: status.state,
                    service_type: status.service_type,
                    system: status.system,
                    maintenance_type: status.maintenance_type,
                    environment: status.env,
                    begin_ms: status.begin,
                    end_ms: status.end,
                })
                .collect(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InstrumentSearchSnapshot {
    pub reference_generation: String,
    pub source_received_at: String,
    pub asset: String,
    pub settle_currency: Option<String>,
    pub instrument_type: Option<InstrumentType>,
    pub instruments: Vec<InstrumentSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InstrumentRulesSnapshot {
    pub reference_generation: String,
    pub source_received_at: String,
    pub instrument: InstrumentSpec,
}

#[derive(Debug, Clone)]
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

    #[error("instrument '{instrument_id}' announced unsupported upcoming parameter '{param}'")]
    UnsupportedUpcomingParameter {
        instrument_id: String,
        param: String,
    },

    #[error("instrument '{instrument_id}' has malformed upcoming parameter change for '{param}'")]
    MalformedUpcomingParameter {
        instrument_id: String,
        param: String,
    },

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
        let mut seen = BTreeSet::new();
        for instrument in instruments {
            let instrument_id = require(
                &instrument.instrument_id,
                &instrument.instrument_id,
                "instId",
            )?
            .to_owned();
            if !seen.insert(instrument_id.clone()) {
                return Err(ReferenceError::DuplicateInstrument(instrument_id));
            }
            if is_preopen(&instrument) {
                continue;
            }

            let spec = InstrumentSpec::try_from(instrument)?;
            normalized.insert(instrument_id, spec);
        }

        let generation = generation_for(&normalized)?;

        Ok(Self {
            source_received_at,
            generation,
            instruments: normalized,
        })
    }

    pub fn apply_public_updates(
        &mut self,
        source_received_at: impl Into<String>,
        updates: Vec<PublicInstrument>,
    ) -> Result<bool, ReferenceError> {
        let source_received_at = source_received_at.into();
        if source_received_at.trim().is_empty() {
            return Err(ReferenceError::EmptySourceTimestamp);
        }

        let mut normalized_updates = BTreeMap::new();
        let mut preopen_ids = BTreeSet::new();
        let mut seen = BTreeSet::new();
        for update in updates {
            let instrument_id =
                require(&update.instrument_id, &update.instrument_id, "instId")?.to_owned();
            if !seen.insert(instrument_id.clone()) {
                return Err(ReferenceError::DuplicateInstrument(instrument_id));
            }
            if is_preopen(&update) {
                preopen_ids.insert(instrument_id);
                continue;
            }

            let spec = InstrumentSpec::try_from(update)?;
            normalized_updates.insert(instrument_id, spec);
        }

        let mut next = self.instruments.clone();
        for instrument_id in preopen_ids {
            next.remove(&instrument_id);
        }
        for (instrument_id, spec) in normalized_updates {
            next.insert(instrument_id, spec);
        }

        let changed = next != self.instruments;
        if changed {
            self.generation = generation_for(&next)?;
            self.instruments = next;
        }
        self.source_received_at = source_received_at;
        Ok(changed)
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

    pub fn instrument_rules(&self, instrument_id: &str) -> Option<InstrumentRulesSnapshot> {
        self.get(instrument_id)
            .cloned()
            .map(|instrument| InstrumentRulesSnapshot {
                reference_generation: self.generation.as_str().to_owned(),
                source_received_at: self.source_received_at.clone(),
                instrument,
            })
    }

    pub fn find_instruments(
        &self,
        asset: &str,
        settle_currency: Option<&str>,
        instrument_type: Option<InstrumentType>,
    ) -> InstrumentSearchSnapshot {
        let instruments = self
            .instruments
            .values()
            .filter(|instrument| {
                instrument.base_currency.as_deref() == Some(asset)
                    || instrument.contract_value_currency.as_deref() == Some(asset)
            })
            .filter(|instrument| {
                settle_currency
                    .is_none_or(|settle| instrument.settle_currency.as_deref() == Some(settle))
            })
            .filter(|instrument| {
                instrument_type.is_none_or(|expected| instrument.instrument_type == expected)
            })
            .cloned()
            .collect();

        InstrumentSearchSnapshot {
            reference_generation: self.generation.as_str().to_owned(),
            source_received_at: self.source_received_at.clone(),
            asset: asset.to_owned(),
            settle_currency: settle_currency.map(ToOwned::to_owned),
            instrument_type,
            instruments,
        }
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

        let rule_type = optional(value.rule_type);
        let funding_requirement = funding_requirement(instrument_type, rule_type.as_deref());
        let upcoming_rule_changes =
            normalize_upcoming_changes(instrument_id, value.upcoming_parameter_changes)?;

        Ok(Self {
            instrument_id: instrument_id.to_owned(),
            instrument_type,
            instrument_family: optional(value.instrument_family),
            underlying: optional(value.underlying),
            state: require(&value.instrument_id, &value.state, "state")?.to_owned(),
            rule_type,
            funding_requirement,
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
            initial_price_limit_pct: optional(value.initial_price_limit_pct),
            floating_price_limit_pct: optional(value.floating_price_limit_pct),
            maximum_price_limit_pct: optional(value.maximum_price_limit_pct),
            upcoming_rule_changes,
        })
    }
}

fn normalize_upcoming_changes(
    instrument_id: &str,
    changes: Vec<okx_api::UpcomingParameterChange>,
) -> Result<Vec<UpcomingRuleChange>, ReferenceError> {
    let mut normalized = Vec::with_capacity(changes.len());
    for change in changes {
        if !matches!(change.param.as_str(), "tickSz" | "minSz" | "maxMktSz") {
            return Err(ReferenceError::UnsupportedUpcomingParameter {
                instrument_id: instrument_id.to_owned(),
                param: change.param,
            });
        }
        if change.new_value.trim().is_empty()
            || change
                .effective_time_ms
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0)
                .is_none()
        {
            return Err(ReferenceError::MalformedUpcomingParameter {
                instrument_id: instrument_id.to_owned(),
                param: change.param,
            });
        }
        normalized.push(UpcomingRuleChange {
            param: change.param,
            new_value: change.new_value,
            effective_time_ms: change.effective_time_ms,
        });
    }
    normalized.sort_by(|left, right| {
        (
            left.effective_time_ms.as_str(),
            left.param.as_str(),
            left.new_value.as_str(),
        )
            .cmp(&(
                right.effective_time_ms.as_str(),
                right.param.as_str(),
                right.new_value.as_str(),
            ))
    });
    Ok(normalized)
}

fn is_preopen(instrument: &PublicInstrument) -> bool {
    instrument.state.trim() == "preopen"
}

fn funding_requirement(
    instrument_type: InstrumentType,
    rule_type: Option<&str>,
) -> FundingRequirement {
    match instrument_type {
        InstrumentType::Swap => FundingRequirement::Required,
        InstrumentType::Futures => match rule_type {
            Some("xperp" | "pre_market") => FundingRequirement::Required,
            Some("normal") => FundingRequirement::NotApplicable,
            _ => FundingRequirement::Unknown,
        },
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
            initial_price_limit_pct: "0.05".to_owned(),
            floating_price_limit_pct: "0.03".to_owned(),
            maximum_price_limit_pct: "0.15".to_owned(),
            upcoming_parameter_changes: Vec::new(),
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
        assert_eq!(spec.initial_price_limit_pct.as_deref(), Some("0.05"));
        assert_eq!(registry.len(), 1);

        let rules = registry
            .instrument_rules("DOGE-USDT-SWAP")
            .expect("instrument rules");
        assert_eq!(rules.reference_generation, registry.generation().as_str());
        assert_eq!(rules.source_received_at, registry.source_received_at());
        assert_eq!(rules.instrument.instrument_id, "DOGE-USDT-SWAP");
    }

    #[test]
    fn upcoming_rule_change_changes_reference_generation_before_effective_time() {
        let mut registry = ReferenceRegistry::from_public(
            "2026-10-01T19:00:00.000Z",
            vec![swap("DOGE-USDT-SWAP")],
        )
        .expect("registry");
        let before = registry.generation().as_str().to_owned();

        let mut changed = swap("DOGE-USDT-SWAP");
        changed
            .upcoming_parameter_changes
            .push(okx_api::UpcomingParameterChange {
                param: "tickSz".to_owned(),
                new_value: "0.000001".to_owned(),
                effective_time_ms: "1790900000000".to_owned(),
            });

        assert!(
            registry
                .apply_public_updates("2026-10-01T19:00:01.000Z", vec![changed])
                .expect("update")
        );
        assert_ne!(registry.generation().as_str(), before);
        let rule_change = &registry
            .get("DOGE-USDT-SWAP")
            .expect("instrument")
            .upcoming_rule_changes[0];
        assert_eq!(rule_change.param, "tickSz");
        assert_eq!(rule_change.new_value, "0.000001");
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
        let second = ReferenceRegistry::from_public("2026-09-27T00:01:00.000Z", vec![btc, doge])
            .expect("registry");

        assert_eq!(first.generation(), second.generation());
    }

    #[test]
    fn ws_style_reference_updates_are_atomic_and_generation_is_content_based() {
        let mut registry = ReferenceRegistry::from_public(
            "2026-09-27T00:00:00.000Z",
            vec![swap("DOGE-USDT-SWAP")],
        )
        .expect("registry");
        let initial_generation = registry.generation().as_str().to_owned();

        let mut changed = swap("DOGE-USDT-SWAP");
        changed.tick_size = "0.000001".to_owned();
        assert!(
            registry
                .apply_public_updates("2026-09-27T00:01:00.000Z", vec![changed.clone()])
                .expect("update")
        );
        assert_ne!(registry.generation().as_str(), initial_generation);
        assert_eq!(
            registry
                .get("DOGE-USDT-SWAP")
                .expect("instrument")
                .tick_size,
            "0.000001"
        );

        let changed_generation = registry.generation().as_str().to_owned();
        assert!(
            !registry
                .apply_public_updates("2026-09-27T00:02:00.000Z", vec![changed])
                .expect("same update")
        );
        assert_eq!(registry.generation().as_str(), changed_generation);
        assert_eq!(registry.source_received_at(), "2026-09-27T00:02:00.000Z");

        let mut invalid = swap("BROKEN-USDT-SWAP");
        invalid.tick_size.clear();
        let before = registry.generation().as_str().to_owned();
        assert!(
            registry
                .apply_public_updates(
                    "2026-09-27T00:03:00.000Z",
                    vec![swap("BTC-USDT-SWAP"), invalid],
                )
                .is_err()
        );
        assert_eq!(registry.generation().as_str(), before);
        assert!(registry.get("BTC-USDT-SWAP").is_none());
    }

    #[test]
    fn preopen_instrument_with_incomplete_mechanics_is_excluded_from_snapshot() {
        let mut preopen = swap("XDP-USDT-SWAP");
        preopen.state = "preopen".to_owned();
        preopen.tick_size.clear();
        preopen.lot_size.clear();
        preopen.min_size.clear();

        let registry = ReferenceRegistry::from_public(
            "2026-09-28T13:30:00.000Z",
            vec![swap("DOGE-USDT-SWAP"), preopen],
        )
        .expect("preopen must not block trade-ready reference bootstrap");

        assert_eq!(registry.len(), 1);
        assert!(registry.get("DOGE-USDT-SWAP").is_some());
        assert!(registry.get("XDP-USDT-SWAP").is_none());
    }

    #[test]
    fn preopen_update_removes_trade_ready_instrument_and_live_update_readds_it() {
        let mut xdp = swap("XDP-USDT-SWAP");
        xdp.instrument_family = "XDP-USDT".to_owned();
        xdp.underlying = "XDP-USDT".to_owned();
        xdp.contract_value_currency = "XDP".to_owned();

        let mut registry = ReferenceRegistry::from_public(
            "2026-09-28T13:00:00.000Z",
            vec![swap("DOGE-USDT-SWAP"), xdp.clone()],
        )
        .expect("registry");
        assert!(registry.get("XDP-USDT-SWAP").is_some());

        let mut preopen = xdp.clone();
        preopen.state = "preopen".to_owned();
        preopen.tick_size.clear();
        preopen.lot_size.clear();
        preopen.min_size.clear();

        assert!(
            registry
                .apply_public_updates("2026-09-28T13:30:00.000Z", vec![preopen])
                .expect("preopen update")
        );
        assert!(registry.get("XDP-USDT-SWAP").is_none());

        xdp.state = "live".to_owned();
        assert!(
            registry
                .apply_public_updates("2026-09-28T14:00:00.000Z", vec![xdp])
                .expect("live update")
        );
        assert!(registry.get("XDP-USDT-SWAP").is_some());
    }

    #[test]
    fn live_instrument_with_incomplete_mechanics_still_fails_closed() {
        let mut live = swap("XDP-USDT-SWAP");
        live.tick_size.clear();

        let error = ReferenceRegistry::from_public(
            "2026-09-28T14:00:00.000Z",
            vec![swap("DOGE-USDT-SWAP"), live],
        )
        .expect_err("live incomplete mechanics must fail closed");

        assert!(matches!(
            error,
            ReferenceError::MissingRequiredField {
                instrument_id,
                field: "tickSz"
            } if instrument_id == "XDP-USDT-SWAP"
        ));
    }

    #[test]
    fn instrument_search_uses_normalized_asset_and_settlement_fields() {
        let mut btc = swap("BTC-USDT-SWAP");
        btc.instrument_family = "BTC-USDT".to_owned();
        btc.underlying = "BTC-USDT".to_owned();
        btc.contract_value_currency = "BTC".to_owned();

        let registry = ReferenceRegistry::from_public(
            "2026-09-27T00:00:00.000Z",
            vec![swap("DOGE-USDT-SWAP"), btc],
        )
        .expect("registry");

        let result = registry.find_instruments("DOGE", Some("USDT"), Some(InstrumentType::Swap));
        assert_eq!(result.instruments.len(), 1);
        assert_eq!(result.instruments[0].instrument_id, "DOGE-USDT-SWAP");

        let none = registry.find_instruments("DOGE", Some("USDC"), None);
        assert!(none.instruments.is_empty());
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
    fn funding_semantics_are_reference_driven_and_fail_closed_for_unknown_rules() {
        let mut ordinary_future = swap("BTC-USDT-261225");
        ordinary_future.instrument_type = "FUTURES".to_owned();
        ordinary_future.rule_type = "normal".to_owned();

        let mut xperp = swap("BTC-USD_XPERP-310101");
        xperp.instrument_type = "FUTURES".to_owned();
        xperp.rule_type = "xperp".to_owned();

        let mut pre_market_xperp = swap("OPENAI-USD-XPERP-PRE");
        pre_market_xperp.instrument_type = "FUTURES".to_owned();
        pre_market_xperp.rule_type = "pre_market".to_owned();

        let mut unknown_future = swap("BTC-USDT-UNKNOWN");
        unknown_future.instrument_type = "FUTURES".to_owned();
        unknown_future.rule_type = "future_rule".to_owned();

        let registry = ReferenceRegistry::from_public(
            "2026-09-27T00:00:00.000Z",
            vec![
                swap("DOGE-USDT-SWAP"),
                ordinary_future,
                xperp,
                pre_market_xperp,
                unknown_future,
            ],
        )
        .expect("registry");

        assert_eq!(
            registry
                .get("DOGE-USDT-SWAP")
                .expect("swap")
                .funding_requirement,
            FundingRequirement::Required
        );
        assert_eq!(
            registry
                .get("BTC-USDT-261225")
                .expect("future")
                .funding_requirement,
            FundingRequirement::NotApplicable
        );
        assert_eq!(
            registry
                .get("BTC-USD_XPERP-310101")
                .expect("xperp")
                .funding_requirement,
            FundingRequirement::Required
        );
        assert_eq!(
            registry
                .get("OPENAI-USD-XPERP-PRE")
                .expect("pre-market xperp")
                .funding_requirement,
            FundingRequirement::Required
        );
        assert_eq!(
            registry
                .get("BTC-USDT-UNKNOWN")
                .expect("unknown future")
                .funding_requirement,
            FundingRequirement::Unknown
        );
    }

    #[test]
    fn missing_required_mechanics_fail_closed() {
        let mut instrument = swap("DOGE-USDT-SWAP");
        instrument.tick_size.clear();

        let error = ReferenceRegistry::from_public("2026-09-27T00:00:00.000Z", vec![instrument])
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

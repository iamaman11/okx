use std::collections::BTreeSet;

use okx_api::InstrumentType;
use okx_observation::{FundingRequirement, ReferenceRegistry};
use okx_ws::{PublicChannel, Subscription, WsArg};
use sha2::{Digest, Sha256};

use super::PublicRuntimeError;

pub(super) fn baseline_subscriptions() -> BTreeSet<Subscription> {
    [
        Subscription::instrument_type(PublicChannel::Instruments, InstrumentType::Swap.to_string()),
        Subscription::instrument_type(
            PublicChannel::Instruments,
            InstrumentType::Futures.to_string(),
        ),
    ]
    .into_iter()
    .collect()
}

pub(super) fn required_subscriptions(
    reference: &ReferenceRegistry,
    instrument_id: &str,
) -> Result<BTreeSet<Subscription>, PublicRuntimeError> {
    let instrument = reference
        .get(instrument_id)
        .ok_or_else(|| PublicRuntimeError::InstrumentNotFound(instrument_id.to_owned()))?;
    let underlying = instrument
        .underlying
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| PublicRuntimeError::MissingUnderlying(instrument_id.to_owned()))?;

    let mut subscriptions = BTreeSet::from([
        Subscription::instrument(PublicChannel::Tickers, instrument_id),
        Subscription::instrument(PublicChannel::MarkPrice, instrument_id),
        Subscription::instrument(PublicChannel::IndexTickers, underlying),
        Subscription::instrument(PublicChannel::OpenInterest, instrument_id),
        Subscription::instrument(PublicChannel::Books, instrument_id),
    ]);

    match instrument.funding_requirement {
        FundingRequirement::Required => {
            subscriptions.insert(Subscription::instrument(
                PublicChannel::FundingRate,
                instrument_id,
            ));
        }
        FundingRequirement::NotApplicable => {}
        FundingRequirement::Unknown => {
            return Err(PublicRuntimeError::UnknownFundingSemantics {
                instrument_id: instrument_id.to_owned(),
            });
        }
    }
    Ok(subscriptions)
}

pub(super) fn desired_subscriptions(
    reference: &ReferenceRegistry,
    demands: &BTreeSet<String>,
) -> BTreeSet<Subscription> {
    let mut desired = baseline_subscriptions();
    for instrument_id in demands {
        if let Ok(required) = required_subscriptions(reference, instrument_id) {
            desired.extend(required);
        }
    }
    desired
}

pub(super) fn subscription_from_arg(arg: &WsArg) -> Subscription {
    Subscription {
        channel: arg.channel,
        instrument_type: arg.instrument_type.clone(),
        instrument_family: arg.instrument_family.clone(),
        instrument_id: arg.instrument_id.clone(),
    }
}

pub(super) fn connection_fingerprint(connection_id: &str) -> String {
    let digest = Sha256::digest(connection_id.as_bytes());
    format!("{digest:x}")
}

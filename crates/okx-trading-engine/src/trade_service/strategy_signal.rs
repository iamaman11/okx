//! Admission of *untrusted* Research proposals to Engine's existing strategy
//! decision ledger. This module never sends orders and never grants risk
//! authority; the existing account authority, RiskBook, and SQL gates remain
//! mandatory before any venue operation.

use okx_ipc_contracts::{
    research_ipc::{SignalPosition, StrategySignalProposal},
    trading::StrategyConfigurationV1,
};
use okx_trade_execution::{StrategyInstance, account_worker::StrategyDecisionClaimCommand};
use serde::Serialize;
use sha2::{Digest, Sha256};

/// A *potential* decision claim; neither a trade command nor an authorization.
/// SQL still serializes duplicate claims and the finance path still checks
/// account governance and RiskBook before constructing a venue-send capability.
pub struct CheckedStrategySignal {
    claim: StrategyDecisionClaimCommand,
    position: SignalPosition,
}

/// Caller-side, independently verified identity and source data. The caller
/// must obtain these fields from Engine's registry/governance/data provider;
/// no field may be copied from the untrusted proposal without comparison.
pub struct TrustedStrategySignalContext<'a> {
    pub instance: &'a StrategyInstance,
    pub configuration: &'a StrategyConfigurationV1,
    pub authenticated_account_key: &'a str,
    pub instrument: &'a str,
    pub last_closed_ms: u64,
    pub source_digest: &'a str,
    pub policy_revision: i64,
    pub now_ms: i64,
}

impl CheckedStrategySignal {
    pub fn into_claim(self) -> (StrategyDecisionClaimCommand, SignalPosition) {
        (self.claim, self.position)
    }
}

/// One durable key preimage for both accepted Research proposals and the
/// existing capability-negotiated legacy path. Diverging these keys would
/// admit two SQL claims for the same strategy bar during a worker upgrade.
pub(super) fn scoped_decision_key(
    authenticated_account_key: &str,
    instance: &StrategyInstance,
    instrument: &str,
    timeframe_ms: u64,
    last_closed_ms: u64,
) -> Result<String, &'static str> {
    Ok(format!(
        "bar_{}",
        &digest(&(
            authenticated_account_key,
            &instance.instance_id,
            &instance.definition_id,
            &instance.definition_version,
            &instance.parameters,
            instrument,
            timeframe_ms,
            last_closed_ms,
        ))?[..40]
    ))
}

/// The input digest and last bar timestamp must be recomputed from a trusted
/// observed source, not taken on Research's word. Until a separately accepted
/// SQL migration, `source_digest` is precisely the legacy *input digest*:
/// SHA-256(JSON serialization of `(last_time_ms, &Vec<(time_ms, close_f64)>)`).
/// `last_closed_ms` is that bar's existing `time_ms` field, not necessarily the
/// wall-clock end of its candle. Account is supplied solely by Engine's
/// authenticated registry, never carried in the proposal.
pub fn check_strategy_signal(
    proposal: &StrategySignalProposal,
    context: &TrustedStrategySignalContext<'_>,
) -> Result<CheckedStrategySignal, &'static str> {
    let TrustedStrategySignalContext {
        instance,
        configuration,
        authenticated_account_key,
        instrument,
        last_closed_ms,
        source_digest,
        policy_revision,
        now_ms,
    } = *context;
    let now = u64::try_from(now_ms).map_err(|_| "STRATEGY_SIGNAL_TIME_INVALID")?;
    if !proposal.valid_at(now) {
        return Err("STRATEGY_SIGNAL_EXPIRED_OR_NONCAUSAL");
    }
    if !instance.enabled || authenticated_account_key.is_empty() || policy_revision < 1 {
        return Err("STRATEGY_SIGNAL_AUTHORITY_REQUIRED");
    }
    let installed_configuration =
        serde_json::from_str::<StrategyConfigurationV1>(&instance.parameters)
            .map_err(|_| "STRATEGY_SIGNAL_INSTALLED_CONFIG_INVALID")?;
    if !configuration.valid()
        || installed_configuration != *configuration
        || instance.instance_id != proposal.instance_id.as_str()
        || instance.definition_id != proposal.definition_id.as_str()
        || instance.definition_version != proposal.definition_version.as_str()
        || proposal.instrument.as_str() != instrument
        || !configuration
            .instruments
            .iter()
            .any(|configured| configured == instrument)
        || proposal.timeframe_ms != configuration.timeframe_ms
        || proposal.last_closed_ms != last_closed_ms
        || proposal.source_digest.as_str() != source_digest
    {
        return Err("STRATEGY_SIGNAL_IDENTITY_OR_SOURCE_MISMATCH");
    }
    // Preserve the existing decision-key preimage so previously persisted
    // claims remain idempotent across this admission-boundary refactor.
    let key = scoped_decision_key(
        authenticated_account_key,
        instance,
        instrument,
        configuration.timeframe_ms,
        last_closed_ms,
    )?;
    Ok(CheckedStrategySignal {
        claim: StrategyDecisionClaimCommand {
            instance_id: instance.instance_id.clone(),
            decision_key: key,
            input_digest: source_digest.to_owned(),
            policy_revision,
            expected_version: instance.definition_version.clone(),
            time: now_ms,
        },
        // Existing semantics: Flat means no *new entry*, not a request to
        // close every existing position. A change requires finance acceptance.
        position: proposal.position,
    })
}

fn digest(value: &impl Serialize) -> Result<String, &'static str> {
    let bytes = serde_json::to_vec(value).map_err(|_| "STRATEGY_SIGNAL_IDENTITY_ENCODE")?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_ipc_contracts::{
        research_ipc::{
            SignalDefinitionId, SignalDefinitionVersion, SignalInstanceId, SignalInstrumentId,
            SignalSourceDigest,
        },
        trading::StrategyParametersV1,
    };
    use rust_decimal::Decimal;

    const NOW: i64 = 125_000;

    fn instance() -> StrategyInstance {
        StrategyInstance {
            instance_id: "strategy-A".into(),
            definition_id: "ema_cross_v1".into(),
            definition_version: "4".into(),
            parameters: r#"{"parameters":{"fast":2,"slow":4},"instruments":["BTC-USDT-SWAP"],"timeframe_ms":60000}"#.into(),
            enabled: true,
            policy_revision: 1,
            budget: Decimal::ONE,
            last_decision_key: None,
        }
    }

    fn configuration() -> StrategyConfigurationV1 {
        StrategyConfigurationV1 {
            parameters: StrategyParametersV1 { fast: 2, slow: 4 },
            instruments: vec!["BTC-USDT-SWAP".into()],
            timeframe_ms: 60_000,
        }
    }

    fn proposal() -> StrategySignalProposal {
        StrategySignalProposal {
            schema: StrategySignalProposal::SCHEMA.into(),
            instance_id: SignalInstanceId::new("strategy-A".into()).unwrap(),
            definition_id: SignalDefinitionId::new("ema_cross_v1".into()).unwrap(),
            definition_version: SignalDefinitionVersion::new("4".into()).unwrap(),
            instrument: SignalInstrumentId::new("BTC-USDT-SWAP".into()).unwrap(),
            timeframe_ms: 60_000,
            last_closed_ms: 120_000,
            available_at_ms: 120_500,
            produced_at_ms: 121_000,
            expires_at_ms: 130_000,
            source_digest: SignalSourceDigest::new("a".repeat(64)).unwrap(),
            position: SignalPosition::Long,
        }
    }

    fn check(
        proposal: &StrategySignalProposal,
        instance: &StrategyInstance,
        configuration: &StrategyConfigurationV1,
    ) -> Result<CheckedStrategySignal, &'static str> {
        check_strategy_signal(
            proposal,
            &TrustedStrategySignalContext {
                instance,
                configuration,
                authenticated_account_key: "trusted-account-key",
                instrument: "BTC-USDT-SWAP",
                last_closed_ms: 120_000,
                source_digest: &"a".repeat(64),
                policy_revision: 2,
                now_ms: NOW,
            },
        )
    }

    #[test]
    fn same_proposal_produces_same_scoped_sql_claim_without_send_rights() {
        let accepted = check(&proposal(), &instance(), &configuration()).unwrap();
        let (first, position) = accepted.into_claim();
        let (retry, _) = check(&proposal(), &instance(), &configuration())
            .unwrap()
            .into_claim();
        assert_eq!(position, SignalPosition::Long);
        assert_eq!(first.decision_key, retry.decision_key);
        assert_eq!(first.input_digest, "a".repeat(64));
        assert_eq!(first.expected_version, "4");
        assert_eq!(first.policy_revision, 2);
        assert_ne!(first.decision_key, first.input_digest);
    }

    #[test]
    fn legacy_and_checked_proposal_share_exact_durable_decision_identity() {
        let installed = instance();
        let configured = configuration();
        let (verified, _) = check(&proposal(), &installed, &configured)
            .unwrap()
            .into_claim();
        let legacy_key = scoped_decision_key(
            "trusted-account-key",
            &installed,
            "BTC-USDT-SWAP",
            configured.timeframe_ms,
            120_000,
        )
        .unwrap();
        assert_eq!(verified.decision_key, legacy_key);
        // Golden key from the historical `(account, instance, definition,
        // version, installed JSON, instrument, timeframe, bar)` preimage.
        // Comparing both new paths alone would miss a simultaneous drift.
        assert_eq!(legacy_key, "bar_601fc2b5879d41104f179658981d7f7f3f2f3df0");
        assert_eq!(legacy_key.len(), 44);
        assert_ne!(
            legacy_key,
            scoped_decision_key(
                "different-account-key",
                &installed,
                "BTC-USDT-SWAP",
                configured.timeframe_ms,
                120_000,
            )
            .unwrap()
        );
    }

    #[test]
    fn historical_source_digest_matches_existing_live_strategy_preimage() {
        let bars = vec![(60_000_u64, 101.0_f64), (120_000_u64, 103.5_f64)];
        let legacy_digest = digest(&(120_000_u64, &bars)).unwrap();
        let mut recorded = proposal();
        recorded.source_digest = SignalSourceDigest::new(legacy_digest.clone()).unwrap();
        let installed = instance();
        let configured = configuration();
        let checked = check_strategy_signal(
            &recorded,
            &TrustedStrategySignalContext {
                instance: &installed,
                configuration: &configured,
                authenticated_account_key: "trusted-account-key",
                instrument: "BTC-USDT-SWAP",
                last_closed_ms: 120_000,
                source_digest: &legacy_digest,
                policy_revision: 2,
                now_ms: NOW,
            },
        )
        .unwrap();
        assert_eq!(checked.into_claim().0.input_digest, legacy_digest);
    }

    #[test]
    fn denies_untrusted_identity_source_time_and_disabled_strategy() {
        let current = instance();
        let configured = configuration();
        let mut wrong = proposal();
        wrong.definition_version = SignalDefinitionVersion::new("3".into()).unwrap();
        assert_eq!(
            check(&wrong, &current, &configured).err(),
            Some("STRATEGY_SIGNAL_IDENTITY_OR_SOURCE_MISMATCH")
        );
        wrong = proposal();
        wrong.instrument = SignalInstrumentId::new("ETH-USDT-SWAP".into()).unwrap();
        assert!(check(&wrong, &current, &configured).is_err());
        wrong = proposal();
        wrong.source_digest = SignalSourceDigest::new("b".repeat(64)).unwrap();
        assert!(check(&wrong, &current, &configured).is_err());
        wrong = proposal();
        wrong.produced_at_ms = (NOW as u64) + 1;
        assert_eq!(
            check(&wrong, &current, &configured).err(),
            Some("STRATEGY_SIGNAL_EXPIRED_OR_NONCAUSAL")
        );
        wrong = proposal();
        wrong.expires_at_ms = NOW as u64;
        assert!(check(&wrong, &current, &configured).is_err());
        wrong = proposal();
        wrong.last_closed_ms = 60_000;
        assert!(check(&wrong, &current, &configured).is_err());
        let mut stopped = current.clone();
        stopped.enabled = false;
        assert_eq!(
            check(&proposal(), &stopped, &configured).err(),
            Some("STRATEGY_SIGNAL_AUTHORITY_REQUIRED")
        );
        let mut context = TrustedStrategySignalContext {
            instance: &current,
            configuration: &configured,
            authenticated_account_key: "",
            instrument: "BTC-USDT-SWAP",
            last_closed_ms: 120_000,
            source_digest: &"a".repeat(64),
            policy_revision: 2,
            now_ms: NOW,
        };
        assert!(check_strategy_signal(&proposal(), &context).is_err());
        context.authenticated_account_key = "trusted-account-key";
        context.policy_revision = 0;
        assert!(check_strategy_signal(&proposal(), &context).is_err());
    }

    #[test]
    fn research_proposal_cannot_override_engine_installed_configuration() {
        let mut installed = instance();
        let current = configuration();
        // This is a valid alternate installed configuration; an untrusted
        // proposal cannot keep claiming the previous runtime configuration.
        installed.parameters =
            r#"{"parameters":{"fast":3,"slow":4},"instruments":["BTC-USDT-SWAP"],"timeframe_ms":60000}"#.into();
        assert_eq!(
            check(&proposal(), &installed, &current).err(),
            Some("STRATEGY_SIGNAL_IDENTITY_OR_SOURCE_MISMATCH")
        );
        installed = instance();
        installed.parameters = "invalid JSON".into();
        assert_eq!(
            check(&proposal(), &installed, &current).err(),
            Some("STRATEGY_SIGNAL_INSTALLED_CONFIG_INVALID")
        );
    }

    #[test]
    fn flat_keeps_legacy_no_new_entry_semantics() {
        let mut flat = proposal();
        flat.position = SignalPosition::Flat;
        let (_, position) = check(&flat, &instance(), &configuration())
            .unwrap()
            .into_claim();
        assert_eq!(position, SignalPosition::Flat);
        flat.position = SignalPosition::Warmup;
        assert!(check(&flat, &instance(), &configuration()).is_err());
    }
}

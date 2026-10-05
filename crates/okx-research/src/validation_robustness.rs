use std::collections::BTreeMap;

use okx_analysis::{
    ValidationRegimeStatistics, ValidationSampleStatistics, ValidationVolatilityRegime,
    analyze_validation_pnl_samples, analyze_validation_regime_pnl,
    classify_validation_volatility_regime,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    BUILD_SOURCE_TREE, PreHoldoutEvidence, ReplayMechanicsProvenance, ReplayStatus,
    ResearchArtifactStore, ResearchError, ResearchRange, ValidationEvidenceReadiness,
    ValidationSpec, VALIDATION_SPEC_SCHEMA_V2, build_baseline_experiment, canonical_sha256,
    derive_replay_range, replay_experiment,
};
use crate::validation_evidence::replay_result_view;

pub const VALIDATION_ROBUSTNESS_SCHEMA_V1: &str = "okx.research.validation-robustness/v1";
pub const VALIDATION_ROBUSTNESS_ALGORITHM_V1: &str =
    "okx.research.validation-robustness/2026-10-05.1";

const MIN_TRADES_PER_FOLD_V1: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalkForwardFoldEvidence {
    pub fold_index: u16,
    pub train_declared_range: ResearchRange,
    pub train_effective_range: ResearchRange,
    pub validation_range: ResearchRange,
    pub validation_dataset_id: String,
    pub validation_replay_dataset_artifact_id: String,
    pub experiment_id: String,
    pub experiment_result_artifact_id: String,
    pub replay_source_tree: String,
    pub trade_count: usize,
    pub net_pnl_quote: String,
    pub statistics: Option<ValidationSampleStatistics>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationRegimeEvidence {
    pub basis: String,
    pub threshold_source: String,
    pub threshold_abs_return: String,
    pub minimum_trades_per_regime: u16,
    pub regimes: Vec<ValidationRegimeStatistics>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationRobustnessEvidence {
    pub schema: String,
    pub robustness_id: String,
    pub algorithm_version: String,
    pub validation_spec_id: String,
    pub validation_spec_artifact_id: String,
    pub pre_holdout_evidence_id: String,
    pub pre_holdout_evidence_artifact_id: String,
    pub train_replay_dataset_artifact_id: String,
    pub validation_replay_dataset_artifact_id: String,
    pub validation_experiment_result_artifact_id: String,
    pub walk_forward_folds: Vec<WalkForwardFoldEvidence>,
    pub regime: ValidationRegimeEvidence,
    pub readiness: ValidationEvidenceReadiness,
    pub blockers: Vec<String>,
    pub final_oos_status: String,
    pub source_tree: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedValidationRobustness {
    pub evidence: ValidationRobustnessEvidence,
    pub evidence_artifact_id: String,
}

#[derive(Serialize)]
struct RobustnessIdentity<'a> {
    schema: &'static str,
    algorithm_version: &'static str,
    validation_spec_id: &'a str,
    validation_spec_artifact_id: &'a str,
    pre_holdout_evidence_id: &'a str,
    pre_holdout_evidence_artifact_id: &'a str,
    train_replay_dataset_artifact_id: &'a str,
    validation_replay_dataset_artifact_id: &'a str,
    validation_experiment_result_artifact_id: &'a str,
    walk_forward_folds: &'a [WalkForwardFoldEvidence],
    regime: &'a ValidationRegimeEvidence,
    readiness: ValidationEvidenceReadiness,
    blockers: &'a [String],
    final_oos_status: &'static str,
    source_tree: &'a str,
}

#[allow(clippy::too_many_arguments)]
pub fn prepare_validation_robustness(
    store: &ResearchArtifactStore,
    expected_instrument: &str,
    validation_spec_artifact_id: &str,
    pre_holdout_evidence_artifact_id: &str,
    train_replay_dataset_artifact_id: &str,
    validation_replay_dataset_artifact_id: &str,
    validation_experiment_result_artifact_id: &str,
) -> Result<PreparedValidationRobustness, ResearchError> {
    if BUILD_SOURCE_TREE == "UNAVAILABLE" {
        return Err(ResearchError::MissingField("validation_robustness.source_tree"));
    }

    let spec: ValidationSpec = store.read_evidence_json(validation_spec_artifact_id)?;
    if spec.schema != VALIDATION_SPEC_SCHEMA_V2 {
        return Err(ResearchError::ArtifactIdentityMismatch);
    }
    let walk_forward_plan = spec
        .walk_forward_plan
        .as_ref()
        .ok_or(ResearchError::MissingField("validation.walk_forward_plan"))?;
    let regime_plan = spec
        .regime_plan
        .as_ref()
        .ok_or(ResearchError::MissingField("validation.regime_plan"))?;
    let pre_holdout: PreHoldoutEvidence =
        store.read_evidence_json(pre_holdout_evidence_artifact_id)?;
    if pre_holdout.validation_spec_id != spec.validation_spec_id
        || pre_holdout.validation_spec_artifact_id != validation_spec_artifact_id
        || pre_holdout.train.replay_dataset_artifact_id != train_replay_dataset_artifact_id
        || pre_holdout.validation.replay_dataset_artifact_id
            != validation_replay_dataset_artifact_id
        || pre_holdout.validation.experiment_result_artifact_id
            != validation_experiment_result_artifact_id
        || pre_holdout.final_oos_status != "SEALED_UNCONSUMED"
    {
        return Err(ResearchError::ArtifactIdentityMismatch);
    }

    let train_dataset: crate::ReplayDatasetArtifact =
        store.read_evidence_json(train_replay_dataset_artifact_id)?;
    let validation_dataset: crate::ReplayDatasetArtifact =
        store.read_evidence_json(validation_replay_dataset_artifact_id)?;
    train_dataset.validate()?;
    validation_dataset.validate()?;
    if train_dataset.manifest.instrument_id != expected_instrument
        || validation_dataset.manifest.instrument_id != expected_instrument
    {
        return Err(ResearchError::InstrumentMismatch {
            expected: expected_instrument.to_owned(),
            actual: format!(
                "{}|{}",
                train_dataset.manifest.instrument_id, validation_dataset.manifest.instrument_id
            ),
        });
    }

    let validation_value: Value =
        store.read_evidence_json(validation_experiment_result_artifact_id)?;
    let validation_result = replay_result_view(&validation_value)?;
    if validation_result.dataset_id != validation_dataset.manifest.dataset_id
        || validation_result.candles_processed != validation_dataset.candles.len()
        || validation_result.hypothesis_id != pre_holdout.hypothesis_id
    {
        return Err(ResearchError::ReplayDatasetMismatch);
    }

    let mut fold_evidence = Vec::with_capacity(walk_forward_plan.folds.len());
    for fold in &walk_forward_plan.folds {
        if fold.train_effective_range.end()? >= fold.validation_range.begin()?
            || fold.train_declared_range.end()? != fold.validation_range.begin()?
        {
            return Err(ResearchError::ReplayCausalityViolation);
        }
        let fold_dataset = derive_replay_range(&train_dataset, &fold.validation_range)?;
        let (fold_dataset_artifact_id, _) = store.publish_evidence(&fold_dataset)?;
        let (_, experiment) = build_baseline_experiment(
            &fold_dataset,
            spec.strategy,
            ReplayMechanicsProvenance::DeclaredCounterfactual,
        )?;
        let result = replay_experiment(
            &fold_dataset.manifest,
            &fold_dataset.candles,
            &fold_dataset.funding,
            &experiment,
        )?;
        if result.status != ReplayStatus::Completed
            || result.hypothesis_id != pre_holdout.hypothesis_id
            || result.replay_source_tree != validation_result.replay_source_tree
        {
            return Err(ResearchError::ArtifactIdentityMismatch);
        }
        let (result_artifact_id, _) = store.publish_evidence(&result)?;
        let trade_pnl = result
            .trades
            .iter()
            .map(|trade| trade.net_pnl_quote.clone())
            .collect::<Vec<_>>();
        let statistics = if trade_pnl.len() >= 2 {
            Some(analyze_validation_pnl_samples(&trade_pnl)?)
        } else {
            None
        };
        fold_evidence.push(WalkForwardFoldEvidence {
            fold_index: fold.fold_index,
            train_declared_range: fold.train_declared_range.clone(),
            train_effective_range: fold.train_effective_range.clone(),
            validation_range: fold.validation_range.clone(),
            validation_dataset_id: fold_dataset.manifest.dataset_id,
            validation_replay_dataset_artifact_id: fold_dataset_artifact_id,
            experiment_id: result.experiment_id,
            experiment_result_artifact_id: result_artifact_id,
            replay_source_tree: result.replay_source_tree,
            trade_count: result.trade_count,
            net_pnl_quote: result.net_pnl_quote,
            statistics,
        });
    }
    fold_evidence.sort_by_key(|fold| fold.fold_index);
    if fold_evidence.len() != usize::from(walk_forward_plan.fold_count)
        || fold_evidence
            .iter()
            .enumerate()
            .any(|(index, fold)| usize::from(fold.fold_index) != index)
    {
        return Err(ResearchError::ArtifactIdentityMismatch);
    }

    let candle_by_available_time = validation_dataset
        .candles
        .iter()
        .map(|candle| (candle.available_time_ms.as_str(), candle))
        .collect::<BTreeMap<_, _>>();
    let mut regime_samples = Vec::with_capacity(validation_result.trade_count);
    for (signal_available_time_ms, net_pnl_quote) in validation_result
        .trade_signal_available_time_ms
        .iter()
        .zip(validation_result.trade_net_pnl.iter())
    {
        let candle = candle_by_available_time
            .get(signal_available_time_ms.as_str())
            .ok_or(ResearchError::ReplayDatasetMismatch)?;
        let regime = classify_validation_volatility_regime(
            &candle.open,
            &candle.close,
            &regime_plan.threshold_abs_return,
        )?;
        regime_samples.push((regime, net_pnl_quote.clone()));
    }
    let regimes = analyze_validation_regime_pnl(&regime_samples)?;
    if regimes.len() != 2
        || regimes[0].regime != ValidationVolatilityRegime::Low
        || regimes[1].regime != ValidationVolatilityRegime::High
    {
        return Err(ResearchError::ArtifactIdentityMismatch);
    }
    let regime = ValidationRegimeEvidence {
        basis: regime_plan.basis.clone(),
        threshold_source: regime_plan.threshold_source.clone(),
        threshold_abs_return: regime_plan.threshold_abs_return.clone(),
        minimum_trades_per_regime: regime_plan.minimum_trades_per_regime,
        regimes,
    };

    let mut blockers = pre_holdout
        .blockers
        .iter()
        .filter(|blocker| {
            blocker.as_str() != "WALK_FORWARD_FOLDS_BELOW_POLICY_MINIMUM"
                && blocker.as_str() != "REGIME_BREAKDOWN_MISSING"
        })
        .cloned()
        .collect::<Vec<_>>();
    if fold_evidence.len() < pre_holdout.policy.min_walk_forward_folds {
        blockers.push("WALK_FORWARD_FOLDS_BELOW_POLICY_MINIMUM".to_owned());
    }
    if fold_evidence
        .iter()
        .any(|fold| fold.trade_count < MIN_TRADES_PER_FOLD_V1)
    {
        blockers.push("WALK_FORWARD_FOLD_SAMPLE_BELOW_POLICY_MINIMUM".to_owned());
    }
    if regime
        .regimes
        .iter()
        .any(|bucket| bucket.trade_count < usize::from(regime.minimum_trades_per_regime))
    {
        blockers.push("REGIME_SAMPLE_BELOW_POLICY_MINIMUM".to_owned());
    }
    blockers.sort();
    blockers.dedup();
    let readiness = if blockers.is_empty() {
        ValidationEvidenceReadiness::ReadyForFinalOos
    } else {
        ValidationEvidenceReadiness::InsufficientEvidence
    };

    let robustness_id = canonical_sha256(&RobustnessIdentity {
        schema: VALIDATION_ROBUSTNESS_SCHEMA_V1,
        algorithm_version: VALIDATION_ROBUSTNESS_ALGORITHM_V1,
        validation_spec_id: &spec.validation_spec_id,
        validation_spec_artifact_id,
        pre_holdout_evidence_id: &pre_holdout.evidence_id,
        pre_holdout_evidence_artifact_id,
        train_replay_dataset_artifact_id,
        validation_replay_dataset_artifact_id,
        validation_experiment_result_artifact_id,
        walk_forward_folds: &fold_evidence,
        regime: &regime,
        readiness,
        blockers: &blockers,
        final_oos_status: "SEALED_UNCONSUMED",
        source_tree: BUILD_SOURCE_TREE,
    })?;
    let evidence = ValidationRobustnessEvidence {
        schema: VALIDATION_ROBUSTNESS_SCHEMA_V1.to_owned(),
        robustness_id,
        algorithm_version: VALIDATION_ROBUSTNESS_ALGORITHM_V1.to_owned(),
        validation_spec_id: spec.validation_spec_id,
        validation_spec_artifact_id: validation_spec_artifact_id.to_owned(),
        pre_holdout_evidence_id: pre_holdout.evidence_id,
        pre_holdout_evidence_artifact_id: pre_holdout_evidence_artifact_id.to_owned(),
        train_replay_dataset_artifact_id: train_replay_dataset_artifact_id.to_owned(),
        validation_replay_dataset_artifact_id: validation_replay_dataset_artifact_id.to_owned(),
        validation_experiment_result_artifact_id:
            validation_experiment_result_artifact_id.to_owned(),
        walk_forward_folds: fold_evidence,
        regime,
        readiness,
        blockers,
        final_oos_status: "SEALED_UNCONSUMED".to_owned(),
        source_tree: BUILD_SOURCE_TREE.to_owned(),
    };
    let (evidence_artifact_id, _) = store.publish_evidence(&evidence)?;

    Ok(PreparedValidationRobustness {
        evidence,
        evidence_artifact_id,
    })
}

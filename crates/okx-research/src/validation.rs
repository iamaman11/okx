use std::collections::BTreeSet;

use okx_analysis::{
    BaselineStrategyKind, StrategyParameterSurface, StrategyResearchMetadata,
    baseline_strategy_research_metadata, baseline_strategy_version, validation_median_absolute_return,
};
use serde::{Deserialize, Serialize};

use crate::{
    BUILD_SOURCE_TREE, ReplayDatasetArtifact, ResearchError, ResearchRange, canonical_sha256,
};

pub const VALIDATION_SPEC_SCHEMA_V1: &str = "okx.research.validation-spec/v1";
pub const VALIDATION_SPEC_SCHEMA_V2: &str = "okx.research.validation-spec/v2";
pub const VALIDATION_WALK_FORWARD_PLAN_VERSION_V1: &str =
    "okx.research.walk-forward-plan/2026-10-05.1";
pub const VALIDATION_REGIME_PLAN_VERSION_V1: &str = "okx.research.regime-plan/2026-10-05.1";
pub const RESEARCH_FAMILY_SCHEMA_V1: &str = "okx.research.family/v1";
pub const VALIDATION_SPLIT_SCHEMA_V1: &str = "okx.research.validation-split/v1";
pub const VALIDATION_EVIDENCE_POLICY_V1: &str = "okx.research.validation-evidence/2026-10-05.1";
pub const VALIDATION_PROMOTION_CRITERIA_V1: &str = "okx.research.promotion-criteria/2026-10-05.1";
pub const VALIDATION_PROMOTION_CRITERIA_V2: &str = "okx.research.promotion-criteria/2026-10-05.2";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ValidationPartitionRole {
    Train,
    Validation,
    FinalOos,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationPartition {
    pub role: ValidationPartitionRole,
    pub range: ResearchRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationWalkForwardFold {
    pub fold_index: u16,
    pub train_declared_range: ResearchRange,
    pub train_effective_range: ResearchRange,
    pub validation_range: ResearchRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationWalkForwardPlan {
    pub version: String,
    pub fold_count: u16,
    pub validation_candles_per_fold: u16,
    pub folds: Vec<ValidationWalkForwardFold>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationRegimePlan {
    pub version: String,
    pub basis: String,
    pub threshold_source: String,
    pub threshold_abs_return: String,
    pub minimum_trades_per_regime: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationSpec {
    pub schema: String,
    pub validation_spec_id: String,
    pub parent_replay_dataset_artifact_id: String,
    pub parent_dataset_id: String,
    pub strategy: BaselineStrategyKind,
    pub strategy_version: String,
    pub strategy_metadata: StrategyResearchMetadata,
    pub partitions: Vec<ValidationPartition>,
    pub purge_bars: u16,
    pub embargo_bars: u16,
    pub evidence_policy_version: String,
    pub promotion_criteria_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub walk_forward_plan: Option<ValidationWalkForwardPlan>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regime_plan: Option<ValidationRegimePlan>,
    pub validation_source_tree: String,
}

#[derive(Serialize)]
struct ValidationSpecIdentity<'a> {
    schema: &'static str,
    parent_replay_dataset_artifact_id: &'a str,
    parent_dataset_id: &'a str,
    strategy: BaselineStrategyKind,
    strategy_version: &'a str,
    strategy_metadata: &'a StrategyResearchMetadata,
    partitions: &'a [ValidationPartition],
    purge_bars: u16,
    embargo_bars: u16,
    evidence_policy_version: &'a str,
    promotion_criteria_version: &'a str,
    validation_source_tree: &'a str,
}

#[derive(Serialize)]
struct ValidationSpecIdentityV2<'a> {
    schema: &'static str,
    parent_replay_dataset_artifact_id: &'a str,
    parent_dataset_id: &'a str,
    strategy: BaselineStrategyKind,
    strategy_version: &'a str,
    strategy_metadata: &'a StrategyResearchMetadata,
    partitions: &'a [ValidationPartition],
    purge_bars: u16,
    embargo_bars: u16,
    evidence_policy_version: &'a str,
    promotion_criteria_version: &'a str,
    walk_forward_plan: &'a ValidationWalkForwardPlan,
    regime_plan: &'a ValidationRegimePlan,
    validation_source_tree: &'a str,
}

impl ValidationSpec {
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        parent_replay_dataset_artifact_id: impl Into<String>,
        parent_dataset_id: impl Into<String>,
        strategy: BaselineStrategyKind,
        strategy_version: impl Into<String>,
        train: ResearchRange,
        validation: ResearchRange,
        final_oos: ResearchRange,
        evidence_policy_version: impl Into<String>,
        promotion_criteria_version: impl Into<String>,
    ) -> Result<Self, ResearchError> {
        let parent_replay_dataset_artifact_id = parent_replay_dataset_artifact_id.into();
        let parent_dataset_id = parent_dataset_id.into();
        let strategy_version = strategy_version.into();
        let evidence_policy_version = evidence_policy_version.into();
        let promotion_criteria_version = promotion_criteria_version.into();

        require_sha256(
            "validation.parent_replay_dataset_artifact_id",
            &parent_replay_dataset_artifact_id,
        )?;
        require_sha256("validation.parent_dataset_id", &parent_dataset_id)?;
        require_nonempty("validation.strategy_version", &strategy_version)?;
        require_nonempty(
            "validation.evidence_policy_version",
            &evidence_policy_version,
        )?;
        require_nonempty(
            "validation.promotion_criteria_version",
            &promotion_criteria_version,
        )?;
        if BUILD_SOURCE_TREE == "UNAVAILABLE" {
            return Err(ResearchError::MissingField(
                "validation.validation_source_tree",
            ));
        }

        train.validate()?;
        validation.validate()?;
        final_oos.validate()?;
        if train.end()? != validation.begin()? || validation.end()? != final_oos.begin()? {
            return Err(ResearchError::InvalidRange {
                begin_ms: train.begin_ms,
                end_ms: final_oos.end_ms,
            });
        }

        let strategy_metadata = baseline_strategy_research_metadata(strategy);
        let purge_bars = strategy_metadata.forward_outcome_bars;
        let embargo_bars = 0;
        let partitions = vec![
            ValidationPartition {
                role: ValidationPartitionRole::Train,
                range: train,
            },
            ValidationPartition {
                role: ValidationPartitionRole::Validation,
                range: validation,
            },
            ValidationPartition {
                role: ValidationPartitionRole::FinalOos,
                range: final_oos,
            },
        ];

        let validation_spec_id = canonical_sha256(&ValidationSpecIdentity {
            schema: VALIDATION_SPEC_SCHEMA_V1,
            parent_replay_dataset_artifact_id: &parent_replay_dataset_artifact_id,
            parent_dataset_id: &parent_dataset_id,
            strategy,
            strategy_version: &strategy_version,
            strategy_metadata: &strategy_metadata,
            partitions: &partitions,
            purge_bars,
            embargo_bars,
            evidence_policy_version: &evidence_policy_version,
            promotion_criteria_version: &promotion_criteria_version,
            validation_source_tree: BUILD_SOURCE_TREE,
        })?;

        Ok(Self {
            schema: VALIDATION_SPEC_SCHEMA_V1.to_owned(),
            validation_spec_id,
            parent_replay_dataset_artifact_id,
            parent_dataset_id,
            strategy,
            strategy_version,
            strategy_metadata,
            partitions,
            purge_bars,
            embargo_bars,
            evidence_policy_version,
            promotion_criteria_version,
            walk_forward_plan: None,
            regime_plan: None,
            validation_source_tree: BUILD_SOURCE_TREE.to_owned(),
        })
    }

    pub fn final_oos(&self) -> &ResearchRange {
        &self.partitions[2].range
    }

    pub fn parameter_sensitivity_applicable(&self) -> bool {
        self.strategy_metadata.parameter_surface != StrategyParameterSurface::None
    }

    #[allow(clippy::too_many_arguments)]
    fn build_v2(
        parent_replay_dataset_artifact_id: impl Into<String>,
        parent_dataset_id: impl Into<String>,
        strategy: BaselineStrategyKind,
        strategy_version: impl Into<String>,
        train: ResearchRange,
        validation: ResearchRange,
        final_oos: ResearchRange,
        evidence_policy_version: impl Into<String>,
        promotion_criteria_version: impl Into<String>,
        walk_forward_plan: ValidationWalkForwardPlan,
        regime_plan: ValidationRegimePlan,
    ) -> Result<Self, ResearchError> {
        let parent_replay_dataset_artifact_id = parent_replay_dataset_artifact_id.into();
        let parent_dataset_id = parent_dataset_id.into();
        let strategy_version = strategy_version.into();
        let evidence_policy_version = evidence_policy_version.into();
        let promotion_criteria_version = promotion_criteria_version.into();

        require_sha256(
            "validation.parent_replay_dataset_artifact_id",
            &parent_replay_dataset_artifact_id,
        )?;
        require_sha256("validation.parent_dataset_id", &parent_dataset_id)?;
        require_nonempty("validation.strategy_version", &strategy_version)?;
        require_nonempty(
            "validation.evidence_policy_version",
            &evidence_policy_version,
        )?;
        require_nonempty(
            "validation.promotion_criteria_version",
            &promotion_criteria_version,
        )?;
        if BUILD_SOURCE_TREE == "UNAVAILABLE" {
            return Err(ResearchError::MissingField(
                "validation.validation_source_tree",
            ));
        }

        train.validate()?;
        validation.validate()?;
        final_oos.validate()?;
        let train_begin = train.begin()?;
        let train_end = train.end()?;
        if train_end != validation.begin()? || validation.end()? != final_oos.begin()? {
            return Err(ResearchError::InvalidRange {
                begin_ms: train.begin_ms,
                end_ms: final_oos.end_ms,
            });
        }
        if walk_forward_plan.version != VALIDATION_WALK_FORWARD_PLAN_VERSION_V1
            || walk_forward_plan.fold_count != 3
            || usize::from(walk_forward_plan.fold_count) != walk_forward_plan.folds.len()
            || walk_forward_plan.validation_candles_per_fold < 4
            || regime_plan.version != VALIDATION_REGIME_PLAN_VERSION_V1
            || regime_plan.basis != "ABSOLUTE_COMPLETED_BAR_OPEN_CLOSE_RETURN"
            || regime_plan.threshold_source != "TRAIN_MEDIAN"
            || regime_plan.minimum_trades_per_regime == 0
        {
            return Err(ResearchError::ArtifactIdentityMismatch);
        }
        let expected_fold_span_ms = u64::from(walk_forward_plan.validation_candles_per_fold)
            .checked_mul(3_600_000)
            .ok_or(ResearchError::ReplayDatasetMismatch)?;
        let mut previous_validation_end = None::<u64>;
        for (index, fold) in walk_forward_plan.folds.iter().enumerate() {
            fold.train_declared_range.validate()?;
            fold.train_effective_range.validate()?;
            fold.validation_range.validate()?;
            let declared_begin = fold.train_declared_range.begin()?;
            let declared_end = fold.train_declared_range.end()?;
            let effective_begin = fold.train_effective_range.begin()?;
            let effective_end = fold.train_effective_range.end()?;
            let validation_begin = fold.validation_range.begin()?;
            let validation_end = fold.validation_range.end()?;
            if usize::from(fold.fold_index) != index
                || declared_begin != train_begin
                || effective_begin != train_begin
                || effective_end > declared_end
                || declared_end != validation_begin
                || validation_end > train_end
                || validation_end
                    .checked_sub(validation_begin)
                    .ok_or(ResearchError::ReplayDatasetMismatch)?
                    != expected_fold_span_ms
                || previous_validation_end.is_some_and(|previous| previous != validation_begin)
            {
                return Err(ResearchError::ArtifactIdentityMismatch);
            }
            previous_validation_end = Some(validation_end);
        }

        let strategy_metadata = baseline_strategy_research_metadata(strategy);
        let purge_bars = strategy_metadata.forward_outcome_bars;
        let embargo_bars = 0;
        let partitions = vec![
            ValidationPartition {
                role: ValidationPartitionRole::Train,
                range: train,
            },
            ValidationPartition {
                role: ValidationPartitionRole::Validation,
                range: validation,
            },
            ValidationPartition {
                role: ValidationPartitionRole::FinalOos,
                range: final_oos,
            },
        ];
        let validation_spec_id = canonical_sha256(&ValidationSpecIdentityV2 {
            schema: VALIDATION_SPEC_SCHEMA_V2,
            parent_replay_dataset_artifact_id: &parent_replay_dataset_artifact_id,
            parent_dataset_id: &parent_dataset_id,
            strategy,
            strategy_version: &strategy_version,
            strategy_metadata: &strategy_metadata,
            partitions: &partitions,
            purge_bars,
            embargo_bars,
            evidence_policy_version: &evidence_policy_version,
            promotion_criteria_version: &promotion_criteria_version,
            walk_forward_plan: &walk_forward_plan,
            regime_plan: &regime_plan,
            validation_source_tree: BUILD_SOURCE_TREE,
        })?;

        Ok(Self {
            schema: VALIDATION_SPEC_SCHEMA_V2.to_owned(),
            validation_spec_id,
            parent_replay_dataset_artifact_id,
            parent_dataset_id,
            strategy,
            strategy_version,
            strategy_metadata,
            partitions,
            purge_bars,
            embargo_bars,
            evidence_policy_version,
            promotion_criteria_version,
            walk_forward_plan: Some(walk_forward_plan),
            regime_plan: Some(regime_plan),
            validation_source_tree: BUILD_SOURCE_TREE.to_owned(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedValidationSlice {
    pub role: ValidationPartitionRole,
    pub declared_range: ResearchRange,
    pub effective_range: ResearchRange,
    pub replay_dataset: ReplayDatasetArtifact,
}

pub fn build_validation_spec_from_counts(
    parent_replay_dataset_artifact_id: impl Into<String>,
    parent: &ReplayDatasetArtifact,
    strategy: BaselineStrategyKind,
    train_candle_count: u16,
    validation_candle_count: u16,
    final_oos_candle_count: u16,
) -> Result<ValidationSpec, ResearchError> {
    parent.validate()?;
    let parent_replay_dataset_artifact_id = parent_replay_dataset_artifact_id.into();
    require_sha256(
        "validation.parent_replay_dataset_artifact_id",
        &parent_replay_dataset_artifact_id,
    )?;
    if parent.manifest.bar.as_deref() != Some("1H") {
        return Err(ResearchError::ReplayUnsupportedBar(
            parent.manifest.bar.clone().unwrap_or_default(),
        ));
    }

    let counts = [
        usize::from(train_candle_count),
        usize::from(validation_candle_count),
        usize::from(final_oos_candle_count),
    ];
    if counts.contains(&0) || counts.iter().sum::<usize>() != parent.candles.len() {
        return Err(ResearchError::ReplayDatasetMismatch);
    }

    let train_end = counts[0];
    let validation_end = train_end + counts[1];
    let train = candle_slice_range(&parent.candles[..train_end])?;
    let validation = candle_slice_range(&parent.candles[train_end..validation_end])?;
    let final_oos = candle_slice_range(&parent.candles[validation_end..])?;

    if train.begin()? != parent.manifest.range.begin()?
        || final_oos.end()? != parent.manifest.range.end()?
    {
        return Err(ResearchError::ReplayDatasetMismatch);
    }

    let purge_bars =
        usize::from(baseline_strategy_research_metadata(strategy).forward_outcome_bars);
    let effective_train_count = train_end
        .checked_sub(purge_bars)
        .ok_or(ResearchError::ReplayDatasetMismatch)?;
    const FOLD_COUNT: usize = 3;
    const VALIDATION_CANDLES_PER_FOLD: usize = 24;
    let validation_span = FOLD_COUNT * VALIDATION_CANDLES_PER_FOLD;
    if effective_train_count <= validation_span + purge_bars + 3 {
        return Err(ResearchError::ReplayMissingField(
            "validation.walk_forward_train_history",
        ));
    }
    let initial_train_count = effective_train_count - validation_span;
    let mut folds = Vec::with_capacity(FOLD_COUNT);
    for fold_index in 0..FOLD_COUNT {
        let validation_begin = initial_train_count + fold_index * VALIDATION_CANDLES_PER_FOLD;
        let validation_end = validation_begin + VALIDATION_CANDLES_PER_FOLD;
        let train_effective_end = validation_begin
            .checked_sub(purge_bars)
            .ok_or(ResearchError::ReplayDatasetMismatch)?;
        folds.push(ValidationWalkForwardFold {
            fold_index: u16::try_from(fold_index)
                .map_err(|_| ResearchError::ReplayDatasetMismatch)?,
            train_declared_range: candle_slice_range(&parent.candles[..validation_begin])?,
            train_effective_range: candle_slice_range(&parent.candles[..train_effective_end])?,
            validation_range: candle_slice_range(
                &parent.candles[validation_begin..validation_end],
            )?,
        });
    }
    let walk_forward_plan = ValidationWalkForwardPlan {
        version: VALIDATION_WALK_FORWARD_PLAN_VERSION_V1.to_owned(),
        fold_count: u16::try_from(FOLD_COUNT).map_err(|_| ResearchError::ReplayDatasetMismatch)?,
        validation_candles_per_fold: u16::try_from(VALIDATION_CANDLES_PER_FOLD)
            .map_err(|_| ResearchError::ReplayDatasetMismatch)?,
        folds,
    };

    let regime_prices = parent.candles[..effective_train_count]
        .iter()
        .map(|candle| (candle.open.clone(), candle.close.clone()))
        .collect::<Vec<_>>();
    let regime_plan = ValidationRegimePlan {
        version: VALIDATION_REGIME_PLAN_VERSION_V1.to_owned(),
        basis: "ABSOLUTE_COMPLETED_BAR_OPEN_CLOSE_RETURN".to_owned(),
        threshold_source: "TRAIN_MEDIAN".to_owned(),
        threshold_abs_return: validation_median_absolute_return(&regime_prices)?,
        minimum_trades_per_regime: 5,
    };

    ValidationSpec::build_v2(
        parent_replay_dataset_artifact_id,
        parent.manifest.dataset_id.clone(),
        strategy,
        baseline_strategy_version(strategy),
        train,
        validation,
        final_oos,
        VALIDATION_EVIDENCE_POLICY_V1,
        VALIDATION_PROMOTION_CRITERIA_V2,
        walk_forward_plan,
        regime_plan,
    )
}

pub fn derive_validation_slice(
    parent: &ReplayDatasetArtifact,
    spec: &ValidationSpec,
    role: ValidationPartitionRole,
) -> Result<DerivedValidationSlice, ResearchError> {
    parent.validate()?;
    if spec.parent_dataset_id != parent.manifest.dataset_id
        || spec.validation_source_tree != BUILD_SOURCE_TREE
    {
        return Err(ResearchError::ReplayDatasetMismatch);
    }

    let partition = spec
        .partitions
        .iter()
        .find(|partition| partition.role == role)
        .ok_or(ResearchError::MissingField("validation.partition"))?;
    let begin = partition.range.begin()?;
    let end = partition.range.end()?;
    let mut candles = parent
        .candles
        .iter()
        .filter(|row| {
            row.open_time_ms
                .parse::<u64>()
                .ok()
                .is_some_and(|timestamp| timestamp >= begin && timestamp < end)
        })
        .cloned()
        .collect::<Vec<_>>();

    let embargo = if role == ValidationPartitionRole::Train {
        0usize
    } else {
        usize::from(spec.embargo_bars)
    };
    let purge = if role == ValidationPartitionRole::FinalOos {
        0usize
    } else {
        usize::from(spec.purge_bars)
    };
    if candles.len() <= embargo + purge {
        return Err(ResearchError::ReplayMissingField(
            "validation.effective_partition",
        ));
    }
    if embargo > 0 {
        candles.drain(0..embargo);
    }
    if purge > 0 {
        candles.truncate(candles.len() - purge);
    }

    let effective_range = candle_slice_range(&candles)?;
    let effective_begin = effective_range.begin()?;
    let effective_end = effective_range.end()?;
    let funding = parent
        .funding
        .iter()
        .filter(|event| {
            event
                .funding_time_ms
                .parse::<u64>()
                .ok()
                .is_some_and(|timestamp| timestamp >= effective_begin && timestamp < effective_end)
        })
        .cloned()
        .collect::<Vec<_>>();
    let manifest = parent.manifest.derive_slice(effective_range.clone())?;
    let replay_dataset =
        ReplayDatasetArtifact::build(manifest, candles, funding, parent.reference.clone())?;

    Ok(DerivedValidationSlice {
        role,
        declared_range: partition.range.clone(),
        effective_range,
        replay_dataset,
    })
}

pub fn derive_replay_range(
    parent: &ReplayDatasetArtifact,
    range: &ResearchRange,
) -> Result<ReplayDatasetArtifact, ResearchError> {
    parent.validate()?;
    range.validate()?;
    let begin = range.begin()?;
    let end = range.end()?;
    if begin < parent.manifest.range.begin()? || end > parent.manifest.range.end()? {
        return Err(ResearchError::ReplayDatasetMismatch);
    }
    let candles = parent
        .candles
        .iter()
        .filter(|row| {
            row.open_time_ms
                .parse::<u64>()
                .ok()
                .is_some_and(|timestamp| timestamp >= begin && timestamp < end)
        })
        .cloned()
        .collect::<Vec<_>>();
    if candles.is_empty() || candle_slice_range(&candles)? != *range {
        return Err(ResearchError::ReplayDatasetMismatch);
    }
    let funding = parent
        .funding
        .iter()
        .filter(|event| {
            event
                .funding_time_ms
                .parse::<u64>()
                .ok()
                .is_some_and(|timestamp| timestamp >= begin && timestamp < end)
        })
        .cloned()
        .collect::<Vec<_>>();
    let manifest = parent.manifest.derive_slice(range.clone())?;
    ReplayDatasetArtifact::build(manifest, candles, funding, parent.reference.clone())
}

fn candle_slice_range(candles: &[crate::ResearchCandle]) -> Result<ResearchRange, ResearchError> {
    let first = candles
        .first()
        .ok_or(ResearchError::ReplayMissingField("validation.candles"))?;
    let last = candles
        .last()
        .ok_or(ResearchError::ReplayMissingField("validation.candles"))?;
    ResearchRange::new(first.open_time_ms.clone(), last.available_time_ms.clone())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchTrialOutcome {
    Completed,
    Rejected,
    InsufficientData,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchTrialRef {
    pub trial_index: u32,
    pub experiment_result_artifact_id: String,
    pub outcome: ResearchTrialOutcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_experiment_id: Option<String>,
    #[serde(default)]
    pub changed_fields: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchFamily {
    pub schema: String,
    pub research_family_id: String,
    pub hypothesis_id: String,
    pub strategy: BaselineStrategyKind,
    pub strategy_version: String,
    pub trials: Vec<ResearchTrialRef>,
    pub source_tree: String,
}

#[derive(Serialize)]
struct ResearchFamilyIdentity<'a> {
    schema: &'static str,
    hypothesis_id: &'a str,
    strategy: BaselineStrategyKind,
    strategy_version: &'a str,
    trials: &'a [ResearchTrialRef],
    source_tree: &'a str,
}

impl ResearchFamily {
    pub fn build(
        hypothesis_id: impl Into<String>,
        strategy: BaselineStrategyKind,
        strategy_version: impl Into<String>,
        mut trials: Vec<ResearchTrialRef>,
    ) -> Result<Self, ResearchError> {
        let hypothesis_id = hypothesis_id.into();
        let strategy_version = strategy_version.into();
        require_sha256("family.hypothesis_id", &hypothesis_id)?;
        require_nonempty("family.strategy_version", &strategy_version)?;
        if BUILD_SOURCE_TREE == "UNAVAILABLE" {
            return Err(ResearchError::MissingField("family.source_tree"));
        }
        if trials.is_empty() {
            return Err(ResearchError::MissingField("family.trials"));
        }

        trials.sort_by_key(|trial| trial.trial_index);
        let mut indexes = BTreeSet::new();
        let mut artifacts = BTreeSet::new();
        for (position, trial) in trials.iter().enumerate() {
            if trial.trial_index != position as u32 {
                return Err(ResearchError::ArtifactIdentityMismatch);
            }
            require_sha256(
                "family.experiment_result_artifact_id",
                &trial.experiment_result_artifact_id,
            )?;
            if let Some(parent) = trial.parent_experiment_id.as_deref() {
                require_sha256("family.parent_experiment_id", parent)?;
            }
            if !indexes.insert(trial.trial_index)
                || !artifacts.insert(trial.experiment_result_artifact_id.clone())
            {
                return Err(ResearchError::DuplicateChunk);
            }
        }

        let research_family_id = canonical_sha256(&ResearchFamilyIdentity {
            schema: RESEARCH_FAMILY_SCHEMA_V1,
            hypothesis_id: &hypothesis_id,
            strategy,
            strategy_version: &strategy_version,
            trials: &trials,
            source_tree: BUILD_SOURCE_TREE,
        })?;

        Ok(Self {
            schema: RESEARCH_FAMILY_SCHEMA_V1.to_owned(),
            research_family_id,
            hypothesis_id,
            strategy,
            strategy_version,
            trials,
            source_tree: BUILD_SOURCE_TREE.to_owned(),
        })
    }
}

fn require_nonempty(field: &'static str, value: &str) -> Result<(), ResearchError> {
    if value.trim().is_empty() {
        Err(ResearchError::MissingField(field))
    } else {
        Ok(())
    }
}

fn require_sha256(field: &'static str, value: &str) -> Result<(), ResearchError> {
    let valid = value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    });
    if valid {
        Ok(())
    } else {
        Err(ResearchError::MissingField(field))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_analysis::BASELINE_STRATEGY_VERSION_V1;

    fn id(ch: char) -> String {
        format!("sha256:{}", ch.to_string().repeat(64))
    }

    const HOUR: u64 = 3_600_000;

    fn range(begin: u64, end: u64) -> ResearchRange {
        ResearchRange::new(begin.to_string(), end.to_string()).expect("range")
    }

    fn parent_dataset(candle_count: usize) -> ReplayDatasetArtifact {
        let candles = (0..candle_count)
            .map(|index| {
                let open = u64::try_from(index).expect("index") * HOUR;
                crate::ResearchCandle {
                    schema: crate::RESEARCH_CANDLE_SCHEMA_V1.to_owned(),
                    open_time_ms: open.to_string(),
                    available_time_ms: (open + HOUR).to_string(),
                    open: (100 + index).to_string(),
                    high: (101 + index).to_string(),
                    low: (99 + index).to_string(),
                    close: (100 + index).to_string(),
                    volume: "1".to_owned(),
                    volume_currency: "1".to_owned(),
                    volume_quote: Some("1".to_owned()),
                }
            })
            .collect::<Vec<_>>();
        ReplayDatasetArtifact::build(
            crate::DatasetManifest {
                schema: crate::DATASET_MANIFEST_SCHEMA_V1.to_owned(),
                dataset_id: id('b'),
                tier: crate::ResearchTier::TierA,
                instrument_id: "BTC-USDT-SWAP".to_owned(),
                bar: Some("1H".to_owned()),
                range: range(0, u64::try_from(candle_count).expect("count") * HOUR),
                reference_coverage: crate::ReferenceCoverageStatus::InsufficientReferenceHistory,
                reference_window: None,
                chunk_ids: vec![id('c')],
                gaps: Vec::new(),
                parser_version: "parser/v1".to_owned(),
                normalization_version: "normalizer/v1".to_owned(),
                source_tree: BUILD_SOURCE_TREE.to_owned(),
                created_at_ms: "1".to_owned(),
            },
            candles,
            Vec::new(),
            None,
        )
        .expect("parent dataset")
    }

    #[test]
    fn validation_spec_is_frozen_and_strategy_metadata_drives_purge() {
        let spec = ValidationSpec::build(
            id('a'),
            id('b'),
            BaselineStrategyKind::CloseMomentum,
            BASELINE_STRATEGY_VERSION_V1,
            range(0, 100),
            range(100, 200),
            range(200, 300),
            "adequacy/v1",
            "promotion/v1",
        )
        .expect("spec");

        assert_eq!(spec.purge_bars, 2);
        assert_eq!(spec.embargo_bars, 0);
        assert_eq!(spec.strategy_metadata.signal_lookback_bars, 1);
        assert!(!spec.parameter_sensitivity_applicable());
        assert_eq!(spec.final_oos(), &range(200, 300));

        let retry = ValidationSpec::build(
            id('a'),
            id('b'),
            BaselineStrategyKind::CloseMomentum,
            BASELINE_STRATEGY_VERSION_V1,
            range(0, 100),
            range(100, 200),
            range(200, 300),
            "adequacy/v1",
            "promotion/v1",
        )
        .expect("retry");
        assert_eq!(spec.validation_spec_id, retry.validation_spec_id);
    }

    #[test]
    fn validation_spec_rejects_overlapping_or_reordered_partitions() {
        assert!(
            ValidationSpec::build(
                id('a'),
                id('b'),
                BaselineStrategyKind::CloseMomentum,
                BASELINE_STRATEGY_VERSION_V1,
                range(0, 150),
                range(100, 200),
                range(200, 300),
                "adequacy/v1",
                "promotion/v1",
            )
            .is_err()
        );

        assert!(
            ValidationSpec::build(
                id('a'),
                id('b'),
                BaselineStrategyKind::CloseMomentum,
                BASELINE_STRATEGY_VERSION_V1,
                range(0, 100),
                range(200, 300),
                range(150, 200),
                "adequacy/v1",
                "promotion/v1",
            )
            .is_err()
        );
    }

    #[test]
    fn no_trade_requires_no_purge_or_parameter_sensitivity() {
        let spec = ValidationSpec::build(
            id('a'),
            id('b'),
            BaselineStrategyKind::NoTrade,
            BASELINE_STRATEGY_VERSION_V1,
            range(0, 100),
            range(100, 200),
            range(200, 300),
            "adequacy/v1",
            "promotion/v1",
        )
        .expect("spec");
        assert_eq!(spec.purge_bars, 0);
        assert!(!spec.parameter_sensitivity_applicable());
    }

    #[test]
    fn count_based_spec_and_slices_are_deterministic_and_apply_purge() {
        let parent = parent_dataset(240);
        let spec = build_validation_spec_from_counts(
            id('a'),
            &parent,
            BaselineStrategyKind::CloseMomentum,
            144,
            48,
            48,
        )
        .expect("spec");
        let retry = build_validation_spec_from_counts(
            id('a'),
            &parent,
            BaselineStrategyKind::CloseMomentum,
            144,
            48,
            48,
        )
        .expect("retry");
        assert_eq!(spec.validation_spec_id, retry.validation_spec_id);
        assert_eq!(spec.purge_bars, 2);
        assert_eq!(spec.schema, VALIDATION_SPEC_SCHEMA_V2);
        assert_eq!(spec.partitions[0].range, range(0, 144 * HOUR));
        assert_eq!(spec.partitions[1].range, range(144 * HOUR, 192 * HOUR));
        assert_eq!(spec.partitions[2].range, range(192 * HOUR, 240 * HOUR));
        let walk_forward = spec.walk_forward_plan.as_ref().expect("walk-forward plan");
        assert_eq!(walk_forward.fold_count, 3);
        assert_eq!(walk_forward.validation_candles_per_fold, 24);
        assert_eq!(
            walk_forward.folds[0].train_declared_range,
            range(0, 70 * HOUR)
        );
        assert_eq!(
            walk_forward.folds[0].train_effective_range,
            range(0, 68 * HOUR)
        );
        assert_eq!(
            walk_forward.folds[0].validation_range,
            range(70 * HOUR, 94 * HOUR)
        );
        assert_eq!(
            walk_forward.folds[2].validation_range,
            range(118 * HOUR, 142 * HOUR)
        );
        let regime = spec.regime_plan.as_ref().expect("regime plan");
        assert_eq!(regime.threshold_source, "TRAIN_MEDIAN");
        assert_eq!(regime.threshold_abs_return, "0");

        let train =
            derive_validation_slice(&parent, &spec, ValidationPartitionRole::Train).expect("train");
        let validation =
            derive_validation_slice(&parent, &spec, ValidationPartitionRole::Validation)
                .expect("validation");
        let final_oos = derive_validation_slice(&parent, &spec, ValidationPartitionRole::FinalOos)
            .expect("final oos");

        assert_eq!(train.replay_dataset.candles.len(), 142);
        assert_eq!(validation.replay_dataset.candles.len(), 46);
        assert_eq!(final_oos.replay_dataset.candles.len(), 48);
        assert_eq!(train.effective_range, range(0, 142 * HOUR));
        assert_eq!(validation.effective_range, range(144 * HOUR, 190 * HOUR));
        assert_eq!(final_oos.effective_range, range(192 * HOUR, 240 * HOUR));
    }

    #[test]
    fn final_oos_future_poison_cannot_change_train_slice() {
        let parent = parent_dataset(240);
        let spec = build_validation_spec_from_counts(
            id('a'),
            &parent,
            BaselineStrategyKind::CloseMomentum,
            144,
            48,
            48,
        )
        .expect("spec");
        let baseline = derive_validation_slice(&parent, &spec, ValidationPartitionRole::Train)
            .expect("baseline train");

        let mut poisoned = parent.clone();
        poisoned.candles[239].close = "999999999".to_owned();
        let poisoned_spec = build_validation_spec_from_counts(
            id('a'),
            &poisoned,
            BaselineStrategyKind::CloseMomentum,
            144,
            48,
            48,
        )
        .expect("poisoned spec");
        let poisoned_train =
            derive_validation_slice(&poisoned, &spec, ValidationPartitionRole::Train)
                .expect("poisoned train");

        assert_eq!(spec.validation_spec_id, poisoned_spec.validation_spec_id);
        assert_eq!(spec.walk_forward_plan, poisoned_spec.walk_forward_plan);
        assert_eq!(spec.regime_plan, poisoned_spec.regime_plan);
        assert_eq!(
            baseline.replay_dataset.candles,
            poisoned_train.replay_dataset.candles
        );
        assert_eq!(
            baseline.replay_dataset.manifest.dataset_id,
            poisoned_train.replay_dataset.manifest.dataset_id
        );
    }

    #[test]
    fn reordered_walk_forward_plan_fails_closed() {
        let parent = parent_dataset(240);
        let spec = build_validation_spec_from_counts(
            id('a'),
            &parent,
            BaselineStrategyKind::CloseMomentum,
            144,
            48,
            48,
        )
        .expect("spec");
        let mut walk_forward = spec.walk_forward_plan.clone().expect("walk-forward");
        walk_forward.folds.swap(0, 1);
        let regime = spec.regime_plan.clone().expect("regime");
        assert!(matches!(
            ValidationSpec::build_v2(
                id('a'),
                parent.manifest.dataset_id.clone(),
                BaselineStrategyKind::CloseMomentum,
                BASELINE_STRATEGY_VERSION_V1,
                spec.partitions[0].range.clone(),
                spec.partitions[1].range.clone(),
                spec.partitions[2].range.clone(),
                VALIDATION_EVIDENCE_POLICY_V1,
                VALIDATION_PROMOTION_CRITERIA_V1,
                walk_forward,
                regime,
            ),
            Err(ResearchError::ArtifactIdentityMismatch)
        ));
    }

    #[test]
    fn count_based_spec_requires_exact_parent_partitioning() {
        let parent = parent_dataset(240);
        assert!(matches!(
            build_validation_spec_from_counts(
                id('a'),
                &parent,
                BaselineStrategyKind::CloseMomentum,
                144,
                48,
                47,
            ),
            Err(ResearchError::ReplayDatasetMismatch)
        ));
    }

    #[test]
    fn research_family_identity_retains_negative_trials() {
        let positive = ResearchTrialRef {
            trial_index: 0,
            experiment_result_artifact_id: id('c'),
            outcome: ResearchTrialOutcome::Completed,
            parent_experiment_id: None,
            changed_fields: Vec::new(),
        };
        let negative = ResearchTrialRef {
            trial_index: 1,
            experiment_result_artifact_id: id('d'),
            outcome: ResearchTrialOutcome::Rejected,
            parent_experiment_id: Some(id('e')),
            changed_fields: vec!["execution.fee_rate".to_owned()],
        };
        let with_negative = ResearchFamily::build(
            id('f'),
            BaselineStrategyKind::CloseMomentum,
            BASELINE_STRATEGY_VERSION_V1,
            vec![positive.clone(), negative],
        )
        .expect("family");
        let without_negative = ResearchFamily::build(
            id('f'),
            BaselineStrategyKind::CloseMomentum,
            BASELINE_STRATEGY_VERSION_V1,
            vec![positive],
        )
        .expect("smaller family");

        assert_ne!(
            with_negative.research_family_id,
            without_negative.research_family_id
        );
        assert_eq!(with_negative.trials.len(), 2);
    }
}

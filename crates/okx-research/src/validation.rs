use std::collections::BTreeSet;

use okx_analysis::{
    BaselineStrategyKind, StrategyParameterSurface, StrategyResearchMetadata,
    baseline_strategy_research_metadata,
};
use serde::{Deserialize, Serialize};

use crate::{BUILD_SOURCE_TREE, ResearchError, ResearchRange, canonical_sha256};

pub const VALIDATION_SPEC_SCHEMA_V1: &str = "okx.research.validation-spec/v1";
pub const RESEARCH_FAMILY_SCHEMA_V1: &str = "okx.research.family/v1";

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
        if train.end()? > validation.begin()? || validation.end()? > final_oos.begin()? {
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
            validation_source_tree: BUILD_SOURCE_TREE.to_owned(),
        })
    }

    pub fn final_oos(&self) -> &ResearchRange {
        &self.partitions[2].range
    }

    pub fn parameter_sensitivity_applicable(&self) -> bool {
        self.strategy_metadata.parameter_surface != StrategyParameterSurface::None
    }
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

    fn range(begin: u64, end: u64) -> ResearchRange {
        ResearchRange::new(begin.to_string(), end.to_string()).expect("range")
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

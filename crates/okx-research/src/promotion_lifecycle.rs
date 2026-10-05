use okx_analysis::BaselineStrategyKind;
use serde::{Deserialize, Serialize};

use crate::{
    BUILD_SOURCE_TREE, PROMOTION_BUNDLE_SCHEMA_V1, PromotionBundle, ResearchArtifactStore,
    ResearchError, ValidationFinalDecision, canonical_sha256,
};

pub const PROMOTION_TRANSITION_SCHEMA_V1: &str = "okx.research.promotion-transition/v1";
pub const PROMOTION_TRANSITION_ALGORITHM_V1: &str =
    "okx.research.promotion-transition/2026-10-05.1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResearchPromotionState {
    Research,
    Backtested,
    Paper,
    Shadow,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromotionTransition {
    pub schema: String,
    pub transition_id: String,
    pub algorithm_version: String,
    pub promotion_bundle_id: String,
    pub promotion_bundle_artifact_id: String,
    pub hypothesis_id: String,
    pub strategy: BaselineStrategyKind,
    pub strategy_version: String,
    pub from: ResearchPromotionState,
    pub to: ResearchPromotionState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_transition_artifact_id: Option<String>,
    pub authorization_id: String,
    pub authorization_statement: String,
    pub promotion_source_tree: String,
    pub transition_source_tree: String,
}

#[derive(Serialize)]
struct PromotionTransitionIdentity<'a> {
    schema: &'static str,
    algorithm_version: &'static str,
    promotion_bundle_id: &'a str,
    promotion_bundle_artifact_id: &'a str,
    hypothesis_id: &'a str,
    strategy: BaselineStrategyKind,
    strategy_version: &'a str,
    from: ResearchPromotionState,
    to: ResearchPromotionState,
    previous_transition_artifact_id: &'a Option<String>,
    authorization_id: &'a str,
    authorization_statement: &'a str,
    promotion_source_tree: &'a str,
    transition_source_tree: &'a str,
}

impl PromotionTransition {
    pub fn validate(&self) -> Result<(), ResearchError> {
        if self.schema != PROMOTION_TRANSITION_SCHEMA_V1
            || self.algorithm_version != PROMOTION_TRANSITION_ALGORITHM_V1
        {
            return Err(ResearchError::ArtifactIdentityMismatch);
        }
        require_nonempty(
            "promotion_transition.authorization_id",
            &self.authorization_id,
        )?;
        require_nonempty(
            "promotion_transition.authorization_statement",
            &self.authorization_statement,
        )?;
        let expected = canonical_sha256(&PromotionTransitionIdentity {
            schema: PROMOTION_TRANSITION_SCHEMA_V1,
            algorithm_version: PROMOTION_TRANSITION_ALGORITHM_V1,
            promotion_bundle_id: &self.promotion_bundle_id,
            promotion_bundle_artifact_id: &self.promotion_bundle_artifact_id,
            hypothesis_id: &self.hypothesis_id,
            strategy: self.strategy,
            strategy_version: &self.strategy_version,
            from: self.from,
            to: self.to,
            previous_transition_artifact_id: &self.previous_transition_artifact_id,
            authorization_id: &self.authorization_id,
            authorization_statement: &self.authorization_statement,
            promotion_source_tree: &self.promotion_source_tree,
            transition_source_tree: &self.transition_source_tree,
        })?;
        if expected != self.transition_id {
            return Err(ResearchError::ArtifactIdentityMismatch);
        }
        Ok(())
    }
}

pub fn load_accepted_promotion_transition(
    store: &ResearchArtifactStore,
    transition_artifact_id: &str,
) -> Result<PromotionTransition, ResearchError> {
    let transition: PromotionTransition = store.read_evidence_json(transition_artifact_id)?;
    transition.validate()?;

    let bundle: PromotionBundle =
        store.read_evidence_json(&transition.promotion_bundle_artifact_id)?;
    if bundle.schema != PROMOTION_BUNDLE_SCHEMA_V1
        || bundle.decision != ValidationFinalDecision::Backtested
        || !bundle.decision_blockers.is_empty()
        || bundle.final_oos_status != "CONSUMED"
        || transition.promotion_bundle_id != bundle.promotion_bundle_id
        || transition.hypothesis_id != bundle.hypothesis_id
        || transition.strategy != bundle.strategy
        || transition.strategy_version != bundle.strategy_version
        || transition.promotion_source_tree != bundle.source_tree
    {
        return Err(ResearchError::ArtifactIdentityMismatch);
    }

    match transition.to {
        ResearchPromotionState::Paper => {
            if transition.from != ResearchPromotionState::Backtested
                || transition.previous_transition_artifact_id.is_some()
            {
                return Err(ResearchError::InvalidPromotionTransition(
                    "accepted PAPER lineage must be BACKTESTED->PAPER",
                ));
            }
        }
        ResearchPromotionState::Shadow => {
            if transition.from != ResearchPromotionState::Paper {
                return Err(ResearchError::InvalidPromotionTransition(
                    "accepted SHADOW lineage must be PAPER->SHADOW",
                ));
            }
            let Some(previous_artifact_id) =
                transition.previous_transition_artifact_id.as_deref()
            else {
                return Err(ResearchError::InvalidPromotionTransition(
                    "accepted SHADOW lineage requires PAPER transition",
                ));
            };
            let previous: PromotionTransition = store.read_evidence_json(previous_artifact_id)?;
            previous.validate()?;
            if previous.from != ResearchPromotionState::Backtested
                || previous.to != ResearchPromotionState::Paper
                || previous.previous_transition_artifact_id.is_some()
                || previous.promotion_bundle_artifact_id
                    != transition.promotion_bundle_artifact_id
                || previous.promotion_bundle_id != transition.promotion_bundle_id
                || previous.hypothesis_id != transition.hypothesis_id
                || previous.strategy != transition.strategy
                || previous.strategy_version != transition.strategy_version
            {
                return Err(ResearchError::ArtifactIdentityMismatch);
            }
        }
        ResearchPromotionState::Research | ResearchPromotionState::Backtested => {
            return Err(ResearchError::InvalidPromotionTransition(
                "live research requires PAPER or SHADOW promotion",
            ));
        }
    }

    Ok(transition)
}

pub fn authorize_promotion_transition(
    store: &ResearchArtifactStore,
    promotion_bundle_artifact_id: &str,
    previous_transition_artifact_id: Option<&str>,
    from: ResearchPromotionState,
    to: ResearchPromotionState,
    authorization_id: &str,
    authorization_statement: &str,
) -> Result<(PromotionTransition, String), ResearchError> {
    if BUILD_SOURCE_TREE == "UNAVAILABLE" {
        return Err(ResearchError::MissingField(
            "promotion_transition.source_tree",
        ));
    }
    require_nonempty("promotion_transition.authorization_id", authorization_id)?;
    require_nonempty(
        "promotion_transition.authorization_statement",
        authorization_statement,
    )?;

    let bundle: PromotionBundle = store.read_evidence_json(promotion_bundle_artifact_id)?;
    if bundle.schema != PROMOTION_BUNDLE_SCHEMA_V1
        || bundle.decision != ValidationFinalDecision::Backtested
        || !bundle.decision_blockers.is_empty()
        || bundle.final_oos_status != "CONSUMED"
    {
        return Err(ResearchError::InvalidPromotionTransition(
            "promotion requires an accepted BACKTESTED bundle",
        ));
    }

    let previous_transition_artifact_id = previous_transition_artifact_id.map(ToOwned::to_owned);
    match (from, to) {
        (ResearchPromotionState::Backtested, ResearchPromotionState::Paper) => {
            if previous_transition_artifact_id.is_some() {
                return Err(ResearchError::InvalidPromotionTransition(
                    "BACKTESTED->PAPER must start directly from the PromotionBundle",
                ));
            }
        }
        (ResearchPromotionState::Paper, ResearchPromotionState::Shadow) => {
            let Some(previous_artifact_id) = previous_transition_artifact_id.as_deref() else {
                return Err(ResearchError::InvalidPromotionTransition(
                    "PAPER->SHADOW requires the accepted PAPER transition artifact",
                ));
            };
            let previous: PromotionTransition = store.read_evidence_json(previous_artifact_id)?;
            previous.validate()?;
            if previous.to != ResearchPromotionState::Paper
                || previous.promotion_bundle_artifact_id != promotion_bundle_artifact_id
                || previous.promotion_bundle_id != bundle.promotion_bundle_id
                || previous.hypothesis_id != bundle.hypothesis_id
                || previous.strategy != bundle.strategy
                || previous.strategy_version != bundle.strategy_version
            {
                return Err(ResearchError::ArtifactIdentityMismatch);
            }
        }
        _ => {
            return Err(ResearchError::InvalidPromotionTransition(
                "only BACKTESTED->PAPER and PAPER->SHADOW are admitted",
            ));
        }
    }

    let transition_id = canonical_sha256(&PromotionTransitionIdentity {
        schema: PROMOTION_TRANSITION_SCHEMA_V1,
        algorithm_version: PROMOTION_TRANSITION_ALGORITHM_V1,
        promotion_bundle_id: &bundle.promotion_bundle_id,
        promotion_bundle_artifact_id,
        hypothesis_id: &bundle.hypothesis_id,
        strategy: bundle.strategy,
        strategy_version: &bundle.strategy_version,
        from,
        to,
        previous_transition_artifact_id: &previous_transition_artifact_id,
        authorization_id,
        authorization_statement,
        promotion_source_tree: &bundle.source_tree,
        transition_source_tree: BUILD_SOURCE_TREE,
    })?;

    let transition = PromotionTransition {
        schema: PROMOTION_TRANSITION_SCHEMA_V1.to_owned(),
        transition_id,
        algorithm_version: PROMOTION_TRANSITION_ALGORITHM_V1.to_owned(),
        promotion_bundle_id: bundle.promotion_bundle_id,
        promotion_bundle_artifact_id: promotion_bundle_artifact_id.to_owned(),
        hypothesis_id: bundle.hypothesis_id,
        strategy: bundle.strategy,
        strategy_version: bundle.strategy_version,
        from,
        to,
        previous_transition_artifact_id,
        authorization_id: authorization_id.to_owned(),
        authorization_statement: authorization_statement.to_owned(),
        promotion_source_tree: bundle.source_tree,
        transition_source_tree: BUILD_SOURCE_TREE.to_owned(),
    };
    transition.validate()?;
    let (artifact_id, _) = store.publish_evidence(&transition)?;
    Ok((transition, artifact_id))
}

fn require_nonempty(field: &'static str, value: &str) -> Result<(), ResearchError> {
    if value.trim().is_empty() {
        Err(ResearchError::MissingField(field))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use okx_analysis::{BaselineStrategyKind, ValidationCostStress, ValidationCostStressPoint};

    use super::*;
    use crate::{
        FinalOosEvidence, PROMOTION_BUNDLE_SCHEMA_V1, PromotionBundle, ValidationPromotionCriteria,
    };

    fn id(ch: char) -> String {
        format!("sha256:{}", ch.to_string().repeat(64))
    }

    fn backtested_bundle() -> PromotionBundle {
        PromotionBundle {
            schema: PROMOTION_BUNDLE_SCHEMA_V1.to_owned(),
            promotion_bundle_id: id('a'),
            algorithm_version: "promotion/v1".to_owned(),
            decision: ValidationFinalDecision::Backtested,
            decision_blockers: Vec::new(),
            promotion_criteria: ValidationPromotionCriteria {
                version: "criteria/v1".to_owned(),
                min_positive_walk_forward_folds: 2,
                require_positive_walk_forward_aggregate: true,
                validation_cost_stress_multiplier: "1.5".to_owned(),
                min_validation_stressed_net_pnl_exclusive: "0".to_owned(),
                min_final_oos_trades: 30,
                min_net_pnl_quote_exclusive: "0".to_owned(),
                min_profit_factor_exclusive: "1".to_owned(),
                min_mean_to_sample_stddev_ratio_exclusive: "0".to_owned(),
                require_cost_stress_monotonic: true,
            },
            validation_spec_id: id('b'),
            validation_spec_artifact_id: id('c'),
            research_family_id: id('d'),
            research_family_artifact_id: id('e'),
            hypothesis_id: id('f'),
            strategy: BaselineStrategyKind::TwoBarMomentum,
            strategy_version: "strategy/v1".to_owned(),
            robustness_id: id('1'),
            robustness_artifact_id: id('2'),
            consumption_intent_id: id('3'),
            consumption_intent_artifact_id: id('4'),
            final_oos: FinalOosEvidence {
                replay_dataset_artifact_id: id('5'),
                dataset_id: id('6'),
                experiment_spec_artifact_id: id('7'),
                experiment_result_artifact_id: id('8'),
                experiment_id: id('9'),
                replay_source_tree: "promotion-tree".to_owned(),
                status: "COMPLETED".to_owned(),
                evidence_class: "COUNTERFACTUAL_MECHANICS".to_owned(),
                candles_processed: 96,
                trade_count: 40,
                net_pnl_quote: "1".to_owned(),
                statistics: None,
                cost_stress: ValidationCostStress {
                    schema: "cost-stress/v1".to_owned(),
                    algorithm_version: "cost/v1".to_owned(),
                    points: vec![ValidationCostStressPoint {
                        trading_cost_multiplier: "1".to_owned(),
                        stressed_net_pnl_quote: "1".to_owned(),
                    }],
                    monotonic_nonincreasing: true,
                },
            },
            final_oos_status: "CONSUMED".to_owned(),
            invalidation_conditions: vec!["strategy semantic change".to_owned()],
            source_tree: "promotion-tree".to_owned(),
        }
    }

    fn store(tag: &str) -> (ResearchArtifactStore, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "okx-promotion-transition-{}-{tag}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        (ResearchArtifactStore::at(root.join("research")), root)
    }

    #[test]
    fn backtested_to_paper_then_paper_to_shadow_is_deterministic() {
        let (store, root) = store("deterministic");
        let (bundle_artifact_id, _) = store
            .publish_evidence(&backtested_bundle())
            .expect("publish bundle");

        let (paper, paper_artifact_id) = authorize_promotion_transition(
            &store,
            &bundle_artifact_id,
            None,
            ResearchPromotionState::Backtested,
            ResearchPromotionState::Paper,
            "operator-approval-1",
            "authorize bounded PAPER observation",
        )
        .expect("paper");
        let (paper_retry, paper_retry_artifact_id) = authorize_promotion_transition(
            &store,
            &bundle_artifact_id,
            None,
            ResearchPromotionState::Backtested,
            ResearchPromotionState::Paper,
            "operator-approval-1",
            "authorize bounded PAPER observation",
        )
        .expect("paper retry");
        assert_eq!(paper.transition_id, paper_retry.transition_id);
        assert_eq!(paper_artifact_id, paper_retry_artifact_id);

        let (shadow, _) = authorize_promotion_transition(
            &store,
            &bundle_artifact_id,
            Some(&paper_artifact_id),
            ResearchPromotionState::Paper,
            ResearchPromotionState::Shadow,
            "operator-approval-2",
            "authorize bounded SHADOW observation",
        )
        .expect("shadow");
        assert_eq!(
            shadow.previous_transition_artifact_id,
            Some(paper_artifact_id)
        );
        assert_eq!(shadow.to, ResearchPromotionState::Shadow);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn invalid_or_skipped_positive_transitions_fail_closed() {
        let (store, root) = store("invalid-transition");
        let (bundle_artifact_id, _) = store
            .publish_evidence(&backtested_bundle())
            .expect("publish bundle");

        for (from, to) in [
            (
                ResearchPromotionState::Research,
                ResearchPromotionState::Paper,
            ),
            (
                ResearchPromotionState::Backtested,
                ResearchPromotionState::Shadow,
            ),
            (ResearchPromotionState::Paper, ResearchPromotionState::Paper),
        ] {
            assert!(matches!(
                authorize_promotion_transition(
                    &store,
                    &bundle_artifact_id,
                    None,
                    from,
                    to,
                    "operator",
                    "authorize",
                ),
                Err(ResearchError::InvalidPromotionTransition(_))
            ));
        }
        assert!(matches!(
            authorize_promotion_transition(
                &store,
                &bundle_artifact_id,
                None,
                ResearchPromotionState::Backtested,
                ResearchPromotionState::Paper,
                "",
                "authorize",
            ),
            Err(ResearchError::MissingField(_))
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rejected_bundle_and_mismatched_paper_lineage_cannot_promote() {
        let (store, root) = store("lineage");
        let mut rejected = backtested_bundle();
        rejected.decision = ValidationFinalDecision::Reject;
        rejected.decision_blockers = vec!["failed".to_owned()];
        let (rejected_artifact_id, _) = store.publish_evidence(&rejected).expect("rejected");
        assert!(matches!(
            authorize_promotion_transition(
                &store,
                &rejected_artifact_id,
                None,
                ResearchPromotionState::Backtested,
                ResearchPromotionState::Paper,
                "operator",
                "authorize",
            ),
            Err(ResearchError::InvalidPromotionTransition(_))
        ));

        let (bundle_artifact_id, _) = store
            .publish_evidence(&backtested_bundle())
            .expect("bundle");
        let (_, paper_artifact_id) = authorize_promotion_transition(
            &store,
            &bundle_artifact_id,
            None,
            ResearchPromotionState::Backtested,
            ResearchPromotionState::Paper,
            "operator-1",
            "paper",
        )
        .expect("paper");

        let mut other = backtested_bundle();
        other.promotion_bundle_id = id('0');
        other.hypothesis_id = id('a');
        let (other_artifact_id, _) = store.publish_evidence(&other).expect("other");
        assert!(matches!(
            authorize_promotion_transition(
                &store,
                &other_artifact_id,
                Some(&paper_artifact_id),
                ResearchPromotionState::Paper,
                ResearchPromotionState::Shadow,
                "operator-2",
                "shadow",
            ),
            Err(ResearchError::ArtifactIdentityMismatch)
        ));

        let _ = fs::remove_dir_all(root);
    }
}

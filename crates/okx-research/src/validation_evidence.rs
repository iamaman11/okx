use std::str::FromStr;

use okx_analysis::{
    BaselineStrategyKind, ValidationCostStress, ValidationSampleStatistics,
    analyze_validation_cost_stress, analyze_validation_pnl_samples,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    BUILD_SOURCE_TREE, EXPERIMENT_RESULT_SCHEMA_V1, ReplayDatasetArtifact, ResearchArtifactStore,
    ResearchError, ResearchFamily, ResearchTrialOutcome, ResearchTrialRef, ValidationPartitionRole,
    ValidationSpec, VALIDATION_EVIDENCE_POLICY_V1, build_baseline_hypothesis, canonical_sha256,
};

pub const PRE_HOLDOUT_EVIDENCE_SCHEMA_V1: &str = "okx.research.pre-holdout-evidence/v1";
pub const PRE_HOLDOUT_EVIDENCE_ALGORITHM_V1: &str =
    "okx.research.pre-holdout-evidence/2026-10-05.1";
const ONE_HOUR_MS: u64 = 3_600_000;
const MIN_TRAIN_TRADES_V1: usize = 30;
const MIN_VALIDATION_TRADES_V1: usize = 30;
const MIN_WALK_FORWARD_FOLDS_V1: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ValidationEvidenceReadiness {
    ReadyForFinalOos,
    InsufficientEvidence,
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ValidationDiagnosticStatus {
    Applicable,
    NotApplicable,
    InsufficientEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationDiagnostic {
    pub name: String,
    pub status: ValidationDiagnosticStatus,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationEvidencePolicy {
    pub version: String,
    pub min_train_trades: usize,
    pub min_validation_trades: usize,
    pub min_walk_forward_folds: usize,
    pub require_regime_breakdown: bool,
    pub require_final_oos_for_backtested: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationPartitionEvidence {
    pub role: ValidationPartitionRole,
    pub replay_dataset_artifact_id: String,
    pub experiment_result_artifact_id: String,
    pub dataset_id: String,
    pub experiment_id: String,
    pub replay_source_tree: String,
    pub evidence_class: String,
    pub candles_processed: usize,
    pub trade_count: usize,
    pub net_pnl_quote: String,
    pub statistics: Option<ValidationSampleStatistics>,
    pub cost_stress: ValidationCostStress,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreHoldoutEvidence {
    pub schema: String,
    pub evidence_id: String,
    pub algorithm_version: String,
    pub validation_spec_id: String,
    pub validation_spec_artifact_id: String,
    pub research_family_id: String,
    pub research_family_artifact_id: String,
    pub hypothesis_id: String,
    pub instrument_id: String,
    pub strategy: BaselineStrategyKind,
    pub strategy_version: String,
    pub policy: ValidationEvidencePolicy,
    pub train: ValidationPartitionEvidence,
    pub validation: ValidationPartitionEvidence,
    pub walk_forward_fold_count: usize,
    pub diagnostics: Vec<ValidationDiagnostic>,
    pub final_oos_status: String,
    pub readiness: ValidationEvidenceReadiness,
    pub blockers: Vec<String>,
    pub source_tree: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedPreHoldoutEvidence {
    pub family: ResearchFamily,
    pub family_artifact_id: String,
    pub evidence: PreHoldoutEvidence,
    pub evidence_artifact_id: String,
}

#[derive(Serialize)]
struct PreHoldoutEvidenceIdentity<'a> {
    schema: &'static str,
    algorithm_version: &'static str,
    validation_spec_id: &'a str,
    validation_spec_artifact_id: &'a str,
    research_family_id: &'a str,
    research_family_artifact_id: &'a str,
    hypothesis_id: &'a str,
    instrument_id: &'a str,
    strategy: BaselineStrategyKind,
    strategy_version: &'a str,
    policy: &'a ValidationEvidencePolicy,
    train: &'a ValidationPartitionEvidence,
    validation: &'a ValidationPartitionEvidence,
    walk_forward_fold_count: usize,
    diagnostics: &'a [ValidationDiagnostic],
    final_oos_status: &'static str,
    readiness: ValidationEvidenceReadiness,
    blockers: &'a [String],
    source_tree: &'a str,
}

struct ReplayResultView {
    hypothesis_id: String,
    dataset_id: String,
    experiment_id: String,
    replay_source_tree: String,
    evidence_class: String,
    candles_processed: usize,
    trade_count: usize,
    gross_pnl_quote: String,
    trading_cost_quote: String,
    funding_cost_quote: String,
    net_pnl_quote: String,
    trade_net_pnl: Vec<String>,
    trade_gross_pnl: Vec<String>,
    trade_trading_cost: Vec<String>,
    trade_funding_cost: Vec<String>,
}

#[allow(clippy::too_many_arguments)]
pub fn prepare_pre_holdout_evidence(
    store: &ResearchArtifactStore,
    expected_instrument: &str,
    validation_spec_artifact_id: &str,
    train_replay_dataset_artifact_id: &str,
    train_experiment_result_artifact_id: &str,
    validation_replay_dataset_artifact_id: &str,
    validation_experiment_result_artifact_id: &str,
) -> Result<PreparedPreHoldoutEvidence, ResearchError> {
    if BUILD_SOURCE_TREE == "UNAVAILABLE" {
        return Err(ResearchError::MissingField("pre_holdout.source_tree"));
    }

    let spec: ValidationSpec = store.read_evidence_json(validation_spec_artifact_id)?;
    if spec.evidence_policy_version != VALIDATION_EVIDENCE_POLICY_V1 {
        return Err(ResearchError::ArtifactIdentityMismatch);
    }

    let train_dataset: ReplayDatasetArtifact =
        store.read_evidence_json(train_replay_dataset_artifact_id)?;
    let validation_dataset: ReplayDatasetArtifact =
        store.read_evidence_json(validation_replay_dataset_artifact_id)?;
    train_dataset.validate()?;
    validation_dataset.validate()?;
    if train_dataset.manifest.instrument_id != expected_instrument {
        return Err(ResearchError::InstrumentMismatch {
            expected: expected_instrument.to_owned(),
            actual: train_dataset.manifest.instrument_id.clone(),
        });
    }
    if validation_dataset.manifest.instrument_id != expected_instrument {
        return Err(ResearchError::InstrumentMismatch {
            expected: expected_instrument.to_owned(),
            actual: validation_dataset.manifest.instrument_id.clone(),
        });
    }

    validate_slice(&spec, ValidationPartitionRole::Train, &train_dataset)?;
    validate_slice(
        &spec,
        ValidationPartitionRole::Validation,
        &validation_dataset,
    )?;

    let train_value: Value = store.read_evidence_json(train_experiment_result_artifact_id)?;
    let validation_value: Value =
        store.read_evidence_json(validation_experiment_result_artifact_id)?;
    let train_result = replay_result_view(&train_value)?;
    let validation_result = replay_result_view(&validation_value)?;

    validate_result_against_dataset(&train_result, &train_dataset)?;
    validate_result_against_dataset(&validation_result, &validation_dataset)?;
    if train_result.hypothesis_id != validation_result.hypothesis_id
        || train_result.replay_source_tree != validation_result.replay_source_tree
    {
        return Err(ResearchError::ArtifactIdentityMismatch);
    }
    let expected_hypothesis = build_baseline_hypothesis(spec.strategy)?;
    if train_result.hypothesis_id != expected_hypothesis.hypothesis_id
        || spec.strategy_version != expected_hypothesis.strategy_version
    {
        return Err(ResearchError::ArtifactIdentityMismatch);
    }

    let family = ResearchFamily::build(
        train_result.hypothesis_id.clone(),
        spec.strategy,
        spec.strategy_version.clone(),
        vec![ResearchTrialRef {
            trial_index: 0,
            experiment_result_artifact_id: train_experiment_result_artifact_id.to_owned(),
            outcome: ResearchTrialOutcome::Completed,
            parent_experiment_id: None,
            changed_fields: Vec::new(),
        }],
    )?;
    let (family_artifact_id, _) = store.publish_evidence(&family)?;

    let train = partition_evidence(
        ValidationPartitionRole::Train,
        train_replay_dataset_artifact_id,
        train_experiment_result_artifact_id,
        &train_result,
    )?;
    let validation = partition_evidence(
        ValidationPartitionRole::Validation,
        validation_replay_dataset_artifact_id,
        validation_experiment_result_artifact_id,
        &validation_result,
    )?;

    let policy = ValidationEvidencePolicy {
        version: VALIDATION_EVIDENCE_POLICY_V1.to_owned(),
        min_train_trades: MIN_TRAIN_TRADES_V1,
        min_validation_trades: MIN_VALIDATION_TRADES_V1,
        min_walk_forward_folds: MIN_WALK_FORWARD_FOLDS_V1,
        require_regime_breakdown: true,
        require_final_oos_for_backtested: true,
    };
    let walk_forward_fold_count = 0usize;
    let mut blockers = Vec::new();
    if train.trade_count < policy.min_train_trades {
        blockers.push("TRAIN_TRADE_SAMPLE_BELOW_POLICY_MINIMUM".to_owned());
    }
    if validation.trade_count < policy.min_validation_trades {
        blockers.push("VALIDATION_TRADE_SAMPLE_BELOW_POLICY_MINIMUM".to_owned());
    }
    if walk_forward_fold_count < policy.min_walk_forward_folds {
        blockers.push("WALK_FORWARD_FOLDS_BELOW_POLICY_MINIMUM".to_owned());
    }
    if train.evidence_class != validation.evidence_class {
        blockers.push("TRAIN_VALIDATION_EVIDENCE_CLASS_MISMATCH".to_owned());
    }

    let mut diagnostics = vec![
        ValidationDiagnostic {
            name: "parameter_sensitivity".to_owned(),
            status: if spec.parameter_sensitivity_applicable() {
                ValidationDiagnosticStatus::InsufficientEvidence
            } else {
                ValidationDiagnosticStatus::NotApplicable
            },
            reason: if spec.parameter_sensitivity_applicable() {
                "strategy declares a parameter surface but sensitivity evidence is not yet bound"
                    .to_owned()
            } else {
                "baseline strategy declares no tunable parameter surface".to_owned()
            },
        },
        ValidationDiagnostic {
            name: "deflated_sharpe_ratio".to_owned(),
            status: ValidationDiagnosticStatus::NotApplicable,
            reason: "single-candidate research family has no selection multiplicity".to_owned(),
        },
        ValidationDiagnostic {
            name: "pbo_cscv".to_owned(),
            status: ValidationDiagnosticStatus::NotApplicable,
            reason: "single-candidate research family has no comparable selection family"
                .to_owned(),
        },
        ValidationDiagnostic {
            name: "walk_forward".to_owned(),
            status: ValidationDiagnosticStatus::InsufficientEvidence,
            reason: format!(
                "{walk_forward_fold_count} completed folds; policy requires at least {}",
                policy.min_walk_forward_folds
            ),
        },
        ValidationDiagnostic {
            name: "regime_breakdown".to_owned(),
            status: ValidationDiagnosticStatus::InsufficientEvidence,
            reason: "predeclared regime evidence is not yet bound".to_owned(),
        },
        ValidationDiagnostic {
            name: "capacity".to_owned(),
            status: ValidationDiagnosticStatus::NotApplicable,
            reason: "Tier-A baseline replay has no historical depth/capacity evidence".to_owned(),
        },
    ];
    diagnostics.sort_by(|left, right| left.name.cmp(&right.name));

    if policy.require_regime_breakdown {
        blockers.push("REGIME_BREAKDOWN_MISSING".to_owned());
    }
    if diagnostics.iter().any(|diagnostic| {
        diagnostic.status == ValidationDiagnosticStatus::InsufficientEvidence
            && diagnostic.name == "parameter_sensitivity"
    }) {
        blockers.push("APPLICABLE_PARAMETER_SENSITIVITY_MISSING".to_owned());
    }
    blockers.sort();
    blockers.dedup();

    let readiness = if blockers.is_empty() {
        ValidationEvidenceReadiness::ReadyForFinalOos
    } else {
        ValidationEvidenceReadiness::InsufficientEvidence
    };

    let evidence_id = canonical_sha256(&PreHoldoutEvidenceIdentity {
        schema: PRE_HOLDOUT_EVIDENCE_SCHEMA_V1,
        algorithm_version: PRE_HOLDOUT_EVIDENCE_ALGORITHM_V1,
        validation_spec_id: &spec.validation_spec_id,
        validation_spec_artifact_id,
        research_family_id: &family.research_family_id,
        research_family_artifact_id: &family_artifact_id,
        hypothesis_id: &train_result.hypothesis_id,
        instrument_id: &train_dataset.manifest.instrument_id,
        strategy: spec.strategy,
        strategy_version: &spec.strategy_version,
        policy: &policy,
        train: &train,
        validation: &validation,
        walk_forward_fold_count,
        diagnostics: &diagnostics,
        final_oos_status: "SEALED_UNCONSUMED",
        readiness,
        blockers: &blockers,
        source_tree: BUILD_SOURCE_TREE,
    })?;

    let evidence = PreHoldoutEvidence {
        schema: PRE_HOLDOUT_EVIDENCE_SCHEMA_V1.to_owned(),
        evidence_id,
        algorithm_version: PRE_HOLDOUT_EVIDENCE_ALGORITHM_V1.to_owned(),
        validation_spec_id: spec.validation_spec_id,
        validation_spec_artifact_id: validation_spec_artifact_id.to_owned(),
        research_family_id: family.research_family_id.clone(),
        research_family_artifact_id: family_artifact_id.clone(),
        hypothesis_id: train_result.hypothesis_id,
        instrument_id: train_dataset.manifest.instrument_id,
        strategy: spec.strategy,
        strategy_version: spec.strategy_version,
        policy,
        train,
        validation,
        walk_forward_fold_count,
        diagnostics,
        final_oos_status: "SEALED_UNCONSUMED".to_owned(),
        readiness,
        blockers,
        source_tree: BUILD_SOURCE_TREE.to_owned(),
    };
    let (evidence_artifact_id, _) = store.publish_evidence(&evidence)?;

    Ok(PreparedPreHoldoutEvidence {
        family,
        family_artifact_id,
        evidence,
        evidence_artifact_id,
    })
}

fn validate_slice(
    spec: &ValidationSpec,
    role: ValidationPartitionRole,
    dataset: &ReplayDatasetArtifact,
) -> Result<(), ResearchError> {
    if dataset.manifest.bar.as_deref() != Some("1H") {
        return Err(ResearchError::ReplayUnsupportedBar(
            dataset.manifest.bar.clone().unwrap_or_default(),
        ));
    }
    let partition = spec
        .partitions
        .iter()
        .find(|partition| partition.role == role)
        .ok_or(ResearchError::MissingField("validation.partition"))?;
    let declared_begin = partition.range.begin()?;
    let declared_end = partition.range.end()?;
    let embargo = if role == ValidationPartitionRole::Train {
        0u64
    } else {
        u64::from(spec.embargo_bars)
    };
    let purge = if role == ValidationPartitionRole::FinalOos {
        0u64
    } else {
        u64::from(spec.purge_bars)
    };
    let expected_begin = declared_begin
        .checked_add(embargo.saturating_mul(ONE_HOUR_MS))
        .ok_or(ResearchError::ReplayDatasetMismatch)?;
    let expected_end = declared_end
        .checked_sub(purge.saturating_mul(ONE_HOUR_MS))
        .ok_or(ResearchError::ReplayDatasetMismatch)?;
    if dataset.manifest.range.begin()? != expected_begin
        || dataset.manifest.range.end()? != expected_end
    {
        return Err(ResearchError::ReplayDatasetMismatch);
    }
    Ok(())
}

fn replay_result_view(value: &Value) -> Result<ReplayResultView, ResearchError> {
    if string_field(value, "schema")? != EXPERIMENT_RESULT_SCHEMA_V1
        || string_field(value, "status")? != "COMPLETED"
    {
        return Err(ResearchError::ArtifactIdentityMismatch);
    }
    let trades = value
        .get("trades")
        .and_then(Value::as_array)
        .ok_or(ResearchError::ReplayMissingField("experiment_result.trades"))?;
    let trade_count = usize_field(value, "trade_count")?;
    if trades.len() != trade_count {
        return Err(ResearchError::ArtifactIdentityMismatch);
    }

    let mut trade_net_pnl = Vec::with_capacity(trades.len());
    let mut trade_gross_pnl = Vec::with_capacity(trades.len());
    let mut trade_trading_cost = Vec::with_capacity(trades.len());
    let mut trade_funding_cost = Vec::with_capacity(trades.len());
    for trade in trades {
        trade_net_pnl.push(string_field(trade, "net_pnl_quote")?.to_owned());
        trade_gross_pnl.push(string_field(trade, "gross_pnl_quote")?.to_owned());
        trade_trading_cost.push(string_field(trade, "trading_cost_quote")?.to_owned());
        trade_funding_cost.push(string_field(trade, "funding_cost_quote")?.to_owned());
    }

    let result = ReplayResultView {
        hypothesis_id: string_field(value, "hypothesis_id")?.to_owned(),
        dataset_id: string_field(value, "dataset_id")?.to_owned(),
        experiment_id: string_field(value, "experiment_id")?.to_owned(),
        replay_source_tree: string_field(value, "replay_source_tree")?.to_owned(),
        evidence_class: string_field(value, "evidence_class")?.to_owned(),
        candles_processed: usize_field(value, "candles_processed")?,
        trade_count,
        gross_pnl_quote: string_field(value, "gross_pnl_quote")?.to_owned(),
        trading_cost_quote: string_field(value, "trading_cost_quote")?.to_owned(),
        funding_cost_quote: string_field(value, "funding_cost_quote")?.to_owned(),
        net_pnl_quote: string_field(value, "net_pnl_quote")?.to_owned(),
        trade_net_pnl,
        trade_gross_pnl,
        trade_trading_cost,
        trade_funding_cost,
    };
    validate_replay_totals(&result)?;
    Ok(result)
}

fn validate_result_against_dataset(
    result: &ReplayResultView,
    dataset: &ReplayDatasetArtifact,
) -> Result<(), ResearchError> {
    if result.dataset_id != dataset.manifest.dataset_id
        || result.candles_processed != dataset.candles.len()
    {
        return Err(ResearchError::ReplayDatasetMismatch);
    }
    Ok(())
}

fn validate_replay_totals(result: &ReplayResultView) -> Result<(), ResearchError> {
    let gross = sum_decimal("trade.gross_pnl_quote", &result.trade_gross_pnl)?;
    let trading = sum_decimal("trade.trading_cost_quote", &result.trade_trading_cost)?;
    let funding = sum_decimal("trade.funding_cost_quote", &result.trade_funding_cost)?;
    let net = sum_decimal("trade.net_pnl_quote", &result.trade_net_pnl)?;
    if gross != decimal("result.gross_pnl_quote", &result.gross_pnl_quote)?
        || trading != decimal("result.trading_cost_quote", &result.trading_cost_quote)?
        || funding != decimal("result.funding_cost_quote", &result.funding_cost_quote)?
        || net != decimal("result.net_pnl_quote", &result.net_pnl_quote)?
    {
        return Err(ResearchError::ArtifactIdentityMismatch);
    }
    Ok(())
}

fn partition_evidence(
    role: ValidationPartitionRole,
    replay_dataset_artifact_id: &str,
    experiment_result_artifact_id: &str,
    result: &ReplayResultView,
) -> Result<ValidationPartitionEvidence, ResearchError> {
    let statistics = if result.trade_net_pnl.len() >= 2 {
        Some(analyze_validation_pnl_samples(&result.trade_net_pnl)?)
    } else {
        None
    };
    let cost_stress = analyze_validation_cost_stress(
        &result.gross_pnl_quote,
        &result.trading_cost_quote,
        &result.funding_cost_quote,
    )?;
    if !cost_stress.monotonic_nonincreasing {
        return Err(ResearchError::ArtifactIdentityMismatch);
    }

    Ok(ValidationPartitionEvidence {
        role,
        replay_dataset_artifact_id: replay_dataset_artifact_id.to_owned(),
        experiment_result_artifact_id: experiment_result_artifact_id.to_owned(),
        dataset_id: result.dataset_id.clone(),
        experiment_id: result.experiment_id.clone(),
        replay_source_tree: result.replay_source_tree.clone(),
        evidence_class: result.evidence_class.clone(),
        candles_processed: result.candles_processed,
        trade_count: result.trade_count,
        net_pnl_quote: result.net_pnl_quote.clone(),
        statistics,
        cost_stress,
    })
}

fn string_field<'a>(value: &'a Value, field: &'static str) -> Result<&'a str, ResearchError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or(ResearchError::ReplayMissingField(field))
}

fn usize_field(value: &Value, field: &'static str) -> Result<usize, ResearchError> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .and_then(|raw| usize::try_from(raw).ok())
        .ok_or(ResearchError::ReplayMissingField(field))
}

fn decimal(field: &'static str, value: &str) -> Result<Decimal, ResearchError> {
    Decimal::from_str(value).map_err(|_| ResearchError::ReplayInvalidDecimal {
        field,
        value: value.to_owned(),
    })
}

fn sum_decimal(field: &'static str, values: &[String]) -> Result<Decimal, ResearchError> {
    values
        .iter()
        .try_fold(Decimal::ZERO, |sum, value| Ok(sum + decimal(field, value)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn result_json(dataset_id: &str, pnl: &[&str]) -> Value {
        let trades = pnl
            .iter()
            .map(|net| {
                json!({
                    "gross_pnl_quote": net,
                    "trading_cost_quote": "0",
                    "funding_cost_quote": "0",
                    "net_pnl_quote": net
                })
            })
            .collect::<Vec<_>>();
        let total = pnl
            .iter()
            .map(|value| Decimal::from_str(value).expect("decimal"))
            .sum::<Decimal>()
            .normalize()
            .to_string();
        json!({
            "schema": EXPERIMENT_RESULT_SCHEMA_V1,
            "experiment_id": format!("sha256:{}", "a".repeat(64)),
            "experiment_spec_id": format!("sha256:{}", "b".repeat(64)),
            "hypothesis_id": format!("sha256:{}", "c".repeat(64)),
            "dataset_id": dataset_id,
            "dataset_source_tree": "dataset-tree",
            "replay_source_tree": "replay-tree",
            "status": "COMPLETED",
            "evidence_class": "COUNTERFACTUAL_MECHANICS",
            "blocker": null,
            "signal_price_role": "COMPLETED_BAR_CLOSE",
            "execution_price_role": "NEXT_BAR_OPEN_THEN_FOLLOWING_BAR_OPEN",
            "candles_processed": 4,
            "decision_count": pnl.len(),
            "trade_count": pnl.len(),
            "rejected_candidate_count": 0,
            "gross_pnl_quote": total,
            "trading_cost_quote": "0",
            "funding_cost_quote": "0",
            "net_pnl_quote": total,
            "decisions": [],
            "trades": trades
        })
    }

    #[test]
    fn replay_result_view_reconciles_trade_totals() {
        let dataset_id = format!("sha256:{}", "d".repeat(64));
        let view = replay_result_view(&result_json(&dataset_id, &["1", "-0.5", "0.25"]))
            .expect("view");
        assert_eq!(view.dataset_id, dataset_id);
        assert_eq!(view.trade_count, 3);
        assert_eq!(view.net_pnl_quote, "0.75");
    }

    #[test]
    fn tampered_replay_totals_fail_closed() {
        let dataset_id = format!("sha256:{}", "d".repeat(64));
        let mut value = result_json(&dataset_id, &["1", "-0.5"]);
        value["net_pnl_quote"] = Value::String("999".to_owned());
        assert!(matches!(
            replay_result_view(&value),
            Err(ResearchError::ArtifactIdentityMismatch)
        ));
    }
}

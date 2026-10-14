//! Immutable financial closure identity for the existing execution ledger.
//! No separate archive owner: historical source events remain embedded in that ledger.
use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    DurableExchangeEvidence, ExecutionLedgerError, StrategyCapitalReservation,
    StrategyFinancialPeriod,
};

pub const STRATEGY_FINALIZATION_SCHEMA_V1: &str = "okx.strategy-finalization/v1";
pub const MAX_ARCHIVED_STRATEGY_RUNS: usize = 16;
pub const STRATEGY_NAV_POINT_SCHEMA_V1: &str = "okx.strategy-nav-point/v1";
pub const MAX_STRATEGY_NAV_POINTS: usize = 4096;

/// Bounded, append-only, hash-linked observations owned by the SAME
/// execution ledger. Sparse observations are not proof of intraday peak
/// coverage or permission to increase trading risk.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyNavPoint {
    pub schema: String,
    pub run_id: String,
    pub observed_at_ms: u64,
    pub account_as_of_ms: u64,
    pub evidence_sha256: String,
    pub nav_usdt: String,
    pub high_water_usdt: String,
    pub daily_gross_loss_usdt: String,
    pub utc_day_start_ms: u64,
    pub previous_sha256: String,
    pub checksum_sha256: String,
}

impl StrategyNavPoint {
    pub fn hash(&self) -> Result<String, ExecutionLedgerError> {
        let bytes = serde_json::to_vec(&(
            &self.schema, &self.run_id, self.observed_at_ms,
            self.account_as_of_ms, &self.evidence_sha256,
            &self.nav_usdt, &self.high_water_usdt,
            &self.daily_gross_loss_usdt, self.utc_day_start_ms,
            &self.previous_sha256,
        ))?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }

    pub fn validate(&self) -> Result<(), ExecutionLedgerError> {
        let nav = Decimal::from_str(&self.nav_usdt).ok();
        let peak = Decimal::from_str(&self.high_water_usdt).ok();
        let loss = Decimal::from_str(&self.daily_gross_loss_usdt).ok();
        if self.schema != STRATEGY_NAV_POINT_SCHEMA_V1
            || self.run_id.is_empty()
            || self.observed_at_ms == 0
            || self.account_as_of_ms == 0
            || self.observed_at_ms < self.account_as_of_ms
            || self.observed_at_ms - self.account_as_of_ms > 30_000
            || self.utc_day_start_ms != (self.account_as_of_ms / 86_400_000) * 86_400_000
            || self.evidence_sha256.len() != 64
            || !self.evidence_sha256.bytes().all(|b| b.is_ascii_hexdigit())
            || nav.is_none_or(|v| v <= Decimal::ZERO)
            || peak.zip(nav).is_none_or(|(peak, nav)| peak < nav)
            || loss.is_none_or(|v| v < Decimal::ZERO)
            || self.checksum_sha256 != self.hash()?
        {
            return Err(ExecutionLedgerError::Corrupt("invalid strategy NAV checkpoint"));
        }
        Ok(())
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyFinalization {
    pub schema: String,
    pub run_id: String,
    pub account_uid: String,
    pub finalized_at_ms: u64,
    pub evidence_sha256: String,
    pub nav_usdt: String,
    pub realized_usdt: String,
    pub fees_usdt: String,
    pub checksum_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchivedStrategyRun {
    pub reservation: StrategyCapitalReservation,
    pub evidence: DurableExchangeEvidence,
    pub finalization: StrategyFinalization,
}

pub const ARCHIVED_STRATEGY_PROOF_SCHEMA_V1: &str = "okx.archived-strategy-proof/v1";

/// Bounded read-only historical proof. Every field comes from the validated
/// immutable archive, not a fresh account projection or reconstructed fills.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArchivedStrategyProof {
    pub schema: &'static str,
    pub run_id: String,
    pub started_at_ms: u64,
    pub finalized_at_ms: u64,
    pub nav_usdt: String,
    pub realized_usdt: String,
    pub fees_usdt: String,
    pub finalization_sha256: String,
    pub evidence_sha256: String,
    pub source_events: usize,
    pub source_cursors: usize,
}

impl StrategyFinalization {
    pub fn new(
        run: &StrategyCapitalReservation,
        evidence: &DurableExchangeEvidence,
        report: &StrategyFinancialPeriod,
        finalized_at_ms: u64,
    ) -> Result<Self, ExecutionLedgerError> {
        if report.state != "FLAT_RECONCILED"
            || report.run_id != run.allocation_id
            || report.evidence_sha256 != evidence.checksum_sha256
            || !report.reasons.is_empty()
            || report.linked_bills != report.linked_fills
            || report.funding_usdt.is_some()
            || finalized_at_ms <= run.started_at_ms
        {
            return Err(ExecutionLedgerError::StrategyRunNotFinalizable);
        }
        let nav_usdt = report
            .nav_usdt
            .clone()
            .ok_or(ExecutionLedgerError::StrategyRunNotFinalizable)?;
        let mut value = Self {
            schema: STRATEGY_FINALIZATION_SCHEMA_V1.to_owned(),
            run_id: run.allocation_id.clone(),
            account_uid: run.account_uid_fingerprint.clone(),
            finalized_at_ms,
            evidence_sha256: evidence.checksum_sha256.clone(),
            nav_usdt,
            realized_usdt: report.realized_usdt.clone(),
            fees_usdt: report.fees_usdt.clone(),
            checksum_sha256: String::new(),
        };
        value.checksum_sha256 = value.hash()?;
        value.validate(run, evidence)?;
        Ok(value)
    }

    fn hash(&self) -> Result<String, ExecutionLedgerError> {
        let payload = serde_json::to_vec(&(
            &self.schema,
            &self.run_id,
            &self.account_uid,
            self.finalized_at_ms,
            &self.evidence_sha256,
            &self.nav_usdt,
            &self.realized_usdt,
            &self.fees_usdt,
        ))?;
        Ok(format!("{:x}", Sha256::digest(payload)))
    }

    pub fn validate(
        &self,
        run: &StrategyCapitalReservation,
        evidence: &DurableExchangeEvidence,
    ) -> Result<(), ExecutionLedgerError> {
        evidence.validate()?;
        if self.schema != STRATEGY_FINALIZATION_SCHEMA_V1
            || self.run_id != run.allocation_id
            || self.account_uid != run.account_uid_fingerprint
            || evidence.run_id != run.allocation_id
            || evidence.account_uid != run.account_uid_fingerprint
            || self.evidence_sha256 != evidence.checksum_sha256
            || self.finalized_at_ms <= run.started_at_ms
            || self.checksum_sha256 != self.hash()?
            || [&self.nav_usdt, &self.realized_usdt, &self.fees_usdt]
                .into_iter()
                .any(|v| Decimal::from_str(v).is_err())
        {
            return Err(ExecutionLedgerError::Corrupt(
                "strategy finalization is invalid or exchange evidence was rewritten",
            ));
        }
        Ok(())
    }
}

impl ArchivedStrategyRun {
    pub fn proof(&self) -> ArchivedStrategyProof {
        ArchivedStrategyProof {
            schema: ARCHIVED_STRATEGY_PROOF_SCHEMA_V1,
            run_id: self.reservation.allocation_id.clone(),
            started_at_ms: self.reservation.started_at_ms,
            finalized_at_ms: self.finalization.finalized_at_ms,
            nav_usdt: self.finalization.nav_usdt.clone(),
            realized_usdt: self.finalization.realized_usdt.clone(),
            fees_usdt: self.finalization.fees_usdt.clone(),
            finalization_sha256: self.finalization.checksum_sha256.clone(),
            evidence_sha256: self.evidence.checksum_sha256.clone(),
            source_events: self.evidence.events.len(),
            source_cursors: self.evidence.cursors.len(),
        }
    }

    pub fn validate(&self) -> Result<(), ExecutionLedgerError> {
        self.finalization
            .validate(&self.reservation, &self.evidence)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyPeriodHistory {
    pub finalized_run: Option<StrategyFinalization>,
    pub archived_runs: Vec<ArchivedStrategyRun>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nav_points: Vec<StrategyNavPoint>,
}

impl StrategyPeriodHistory {
    pub fn validate(
        &self,
        active: Option<&StrategyCapitalReservation>,
        evidence: Option<&DurableExchangeEvidence>,
    ) -> Result<(), ExecutionLedgerError> {
        if self.archived_runs.len() > MAX_ARCHIVED_STRATEGY_RUNS {
            return Err(ExecutionLedgerError::ExchangeEvidenceCapacity);
        }
        if self.nav_points.len() > MAX_STRATEGY_NAV_POINTS {
            return Err(ExecutionLedgerError::ExchangeEvidenceCapacity);
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut last_finalized = 0;
        for archived in &self.archived_runs {
            if archived.reservation.started_at_ms <= last_finalized {
                return Err(ExecutionLedgerError::Corrupt(
                    "overlapping archived run boundaries",
                ));
            }
            last_finalized = archived.finalization.finalized_at_ms;
            archived.validate()?;
            if !seen.insert(archived.reservation.allocation_id.as_str())
                || active.is_some_and(|a| {
                    a.account_uid_fingerprint != archived.reservation.account_uid_fingerprint
                })
            {
                return Err(ExecutionLedgerError::AccountScopeMismatch);
            }
        }
        if let Some(run) = active {
            if seen.contains(run.allocation_id.as_str()) {
                return Err(ExecutionLedgerError::CapitalReservationConflict);
            }
            if last_finalized > 0 && run.started_at_ms <= last_finalized {
                return Err(ExecutionLedgerError::Corrupt(
                    "next run predates prior financial close",
                ));
            }
        } else if evidence.is_some()
            || self.finalized_run.is_some()
            || !self.archived_runs.is_empty()
        {
            return Err(ExecutionLedgerError::Corrupt(
                "orphaned financial run state",
            ));
        }
        let mut previous: Option<&StrategyNavPoint> = None;
        for point in &self.nav_points {
            point.validate()?;
            let owner = active.filter(|r| r.allocation_id == point.run_id)
                .map(|r| (r.started_at_ms, None))
                .or_else(|| self.archived_runs.iter()
                    .find(|r| r.reservation.allocation_id == point.run_id)
                    .map(|r| (r.reservation.started_at_ms, Some(r.finalization.finalized_at_ms))));
            let Some((started_at_ms, ended_at_ms)) = owner else {
                return Err(ExecutionLedgerError::Corrupt("NAV checkpoint has no run owner"));
            };
            if point.account_as_of_ms < started_at_ms
                || ended_at_ms.is_some_and(|end| point.observed_at_ms > end)
                || previous.is_some_and(|p| point.observed_at_ms <= p.observed_at_ms)
                || point.previous_sha256 != previous.map_or("", |p| &p.checksum_sha256)
            {
                return Err(ExecutionLedgerError::Corrupt("invalid NAV checkpoint lineage"));
            }
            if let Some(prior) = previous.filter(|p| p.run_id == point.run_id) {
                let prior_peak = Decimal::from_str(&prior.high_water_usdt)
                    .map_err(|_| ExecutionLedgerError::Corrupt("invalid prior NAV peak"))?;
                let nav = Decimal::from_str(&point.nav_usdt)
                    .map_err(|_| ExecutionLedgerError::Corrupt("invalid checkpoint NAV"))?;
                if Decimal::from_str(&point.high_water_usdt).ok() != Some(prior_peak.max(nav)) {
                    return Err(ExecutionLedgerError::Corrupt("NAV peak rewound or invented"));
                }
            }
            previous = Some(point);
        }
        if let Some(finalized) = &self.finalized_run {
            let (Some(run), Some(source)) = (active, evidence) else {
                return Err(ExecutionLedgerError::Corrupt(
                    "finalized period without source events",
                ));
            };
            finalized.validate(run, source)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::STRATEGY_CAPITAL_RESERVATION_SCHEMA_V1;

    fn reservation() -> StrategyCapitalReservation {
        StrategyCapitalReservation {
            schema: STRATEGY_CAPITAL_RESERVATION_SCHEMA_V1.to_owned(),
            allocation_id: "run_finalization_demo_01234".to_owned(),
            account_uid_fingerprint: "uid-demo".to_owned(),
            origin_schema: "okx.research.live-decision/v1".to_owned(),
            origin_version: "strategy-v1".to_owned(),
            mandate_version: "demo-policy/v1".to_owned(),
            allocated_usdt: "100".to_owned(),
            opening_account_usdt_equity: "1000".to_owned(),
            account_generation: "generation".to_owned(),
            operator_authority_id: format!("sha256:{}", "a".repeat(64)),
            started_at_ms: 1000,
        }
    }

    #[test]
    fn closure_identity_survives_serde_and_rejects_rewritten_financials() {
        let run = reservation();
        let evidence = DurableExchangeEvidence::new(&run).unwrap();
        let report = StrategyFinancialPeriod {
            schema: crate::STRATEGY_FINANCIAL_PERIOD_SCHEMA_V1,
            run_id: run.allocation_id.clone(),
            state: "FLAT_RECONCILED",
            evidence_sha256: evidence.checksum_sha256.clone(),
            allocation_usdt: "100".into(),
            linked_fills: 0,
            linked_bills: 0,
            realized_usdt: "0".into(),
            fees_usdt: "0".into(),
            funding_usdt: None,
            unrealized_usdt: Some("0".into()),
            nav_usdt: Some("100".into()),
            positions: vec![],
            usable_for_new_risk: false,
            reasons: vec![],
        };
        let finalization = StrategyFinalization::new(&run, &evidence, &report, 2000).unwrap();
        let decoded: StrategyFinalization =
            serde_json::from_slice(&serde_json::to_vec(&finalization).unwrap()).unwrap();
        decoded.validate(&run, &evidence).unwrap();
        let archived = ArchivedStrategyRun {
            reservation: run.clone(),
            evidence: evidence.clone(),
            finalization: decoded.clone(),
        };
        let mut next = run.clone();
        next.allocation_id = "run_finalization_demo_next_01".to_owned();
        next.started_at_ms = 3000;
        let history = StrategyPeriodHistory {
            finalized_run: None,
            archived_runs: vec![archived.clone()],
            nav_points: Vec::new(),
        };
        history.validate(Some(&next), None).unwrap();
        let proof = history.archived_runs[0].proof();
        assert_eq!(proof.schema, ARCHIVED_STRATEGY_PROOF_SCHEMA_V1);
        assert_eq!(proof.run_id, run.allocation_id);
        assert_eq!(proof.finalization_sha256, decoded.checksum_sha256);
        assert_eq!(proof.evidence_sha256, evidence.checksum_sha256);
        assert_eq!(proof.nav_usdt, "100");
        assert_eq!(proof.source_events, 0);
        assert_eq!(proof.source_cursors, 0);
        let encoded = serde_json::to_value(&proof).unwrap();
        assert!(encoded.get("reservation").is_none());
        assert!(encoded.get("payload").is_none());
        let round_trip: StrategyPeriodHistory =
            serde_json::from_slice(&serde_json::to_vec(&history).unwrap()).unwrap();
        round_trip.validate(Some(&next), None).unwrap();
        assert!(matches!(
            history.validate(Some(&run), None),
            Err(ExecutionLedgerError::CapitalReservationConflict)
        ));
        let mut premature_next = next.clone();
        premature_next.started_at_ms = 2000;
        assert!(matches!(
            history.validate(Some(&premature_next), None),
            Err(ExecutionLedgerError::Corrupt(_))
        ));
        let duplicate_history = StrategyPeriodHistory {
            finalized_run: None,
            archived_runs: vec![archived.clone(), archived],
            nav_points: Vec::new(),
        };
        assert!(duplicate_history.validate(Some(&next), None).is_err());
        // Sixteen distinct, internally valid immutable periods are allowed.
        // The seventeenth must be refused without evicting earlier evidence.
        let mut accepted_archives = Vec::new();
        for i in 0..MAX_ARCHIVED_STRATEGY_RUNS {
            let mut distinct = run.clone();
            distinct.allocation_id = format!("run_archive_{i:04}");
            distinct.started_at_ms = 1000 + (i as u64) * 10_000;
            let source = DurableExchangeEvidence::new(&distinct).unwrap();
            let mut projection = report.clone();
            projection.run_id = distinct.allocation_id.clone();
            projection.evidence_sha256 = source.checksum_sha256.clone();
            let finalization = StrategyFinalization::new(
                &distinct,
                &source,
                &projection,
                distinct.started_at_ms + 1_000,
            )
            .unwrap();
            accepted_archives.push(ArchivedStrategyRun {
                reservation: distinct,
                evidence: source,
                finalization,
            });
        }
        let mut next_after_capacity = next.clone();
        next_after_capacity.started_at_ms =
            1000 + (MAX_ARCHIVED_STRATEGY_RUNS as u64) * 10_000 + 2_000;
        let bounded_history = StrategyPeriodHistory {
            finalized_run: None,
            archived_runs: accepted_archives,
            nav_points: Vec::new(),
        };
        bounded_history
            .validate(Some(&next_after_capacity), None)
            .unwrap();
        let original_history = serde_json::to_vec(&bounded_history).unwrap();
        let mut over_capacity = bounded_history.clone();
        over_capacity
            .archived_runs
            .push(over_capacity.archived_runs.last().unwrap().clone());
        assert!(matches!(
            over_capacity.validate(Some(&next_after_capacity), None),
            Err(ExecutionLedgerError::ExchangeEvidenceCapacity)
        ));
        assert_eq!(
            serde_json::to_vec(&bounded_history).unwrap(),
            original_history
        );

        let mut tampered = decoded;
        tampered.nav_usdt = "100000".into();
        assert!(matches!(
            tampered.validate(&run, &evidence),
            Err(ExecutionLedgerError::Corrupt(_))
        ));
    }

    #[test]
    fn incomplete_or_unallocated_funding_cannot_be_finalized() {
        let run = reservation();
        let evidence = DurableExchangeEvidence::new(&run).unwrap();
        let base = StrategyFinancialPeriod {
            schema: crate::STRATEGY_FINANCIAL_PERIOD_SCHEMA_V1,
            run_id: run.allocation_id.clone(),
            state: "INCOMPLETE",
            evidence_sha256: evidence.checksum_sha256.clone(),
            allocation_usdt: "100".into(),
            linked_fills: 0,
            linked_bills: 0,
            realized_usdt: "0".into(),
            fees_usdt: "0".into(),
            funding_usdt: None,
            unrealized_usdt: None,
            nav_usdt: None,
            positions: vec![],
            usable_for_new_risk: false,
            reasons: vec!["funding_unallocated"],
        };
        assert!(matches!(
            StrategyFinalization::new(&run, &evidence, &base, 2000),
            Err(ExecutionLedgerError::StrategyRunNotFinalizable)
        ));
    }
}

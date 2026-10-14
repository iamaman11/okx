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
    pub source_cursor_key: String,
    pub source_cursor_sha256: String,
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
            &self.schema,
            &self.run_id,
            self.observed_at_ms,
            self.account_as_of_ms,
            &self.evidence_sha256,
            &self.source_cursor_key,
            &self.source_cursor_sha256,
            &self.nav_usdt,
            &self.high_water_usdt,
            &self.daily_gross_loss_usdt,
            self.utc_day_start_ms,
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
            || self.observed_at_ms.saturating_add(2_000) < self.account_as_of_ms
            || self.observed_at_ms.abs_diff(self.account_as_of_ms) > 30_000
            || self.utc_day_start_ms != (self.account_as_of_ms / 86_400_000) * 86_400_000
            || self.evidence_sha256.len() != 64
            || !self.evidence_sha256.bytes().all(|b| b.is_ascii_hexdigit())
            || self.source_cursor_key.trim().is_empty()
            || self.source_cursor_sha256.len() != 64
            || !self.source_cursor_sha256.bytes().all(|b| b.is_ascii_hexdigit())
            || nav.is_none()
            || peak.zip(nav).is_none_or(|(peak, nav)| peak < nav)
            || loss.is_none_or(|v| v < Decimal::ZERO)
            || self.checksum_sha256 != self.hash()?
        {
            return Err(ExecutionLedgerError::Corrupt(
                "invalid strategy NAV checkpoint",
            ));
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
            let owner = active
                .filter(|r| r.allocation_id == point.run_id)
                .and_then(|r| {
                    evidence.map(|source| (r.started_at_ms, None, r.allocated_usdt.as_str(), source))
                })
                .or_else(|| {
                    self.archived_runs
                        .iter()
                        .find(|r| r.reservation.allocation_id == point.run_id)
                        .map(|r| {
                            (
                                r.reservation.started_at_ms,
                                Some(r.finalization.finalized_at_ms),
                                r.reservation.allocated_usdt.as_str(),
                                &r.evidence,
                            )
                        })
                });
            let Some((started_at_ms, ended_at_ms, allocation_usdt, source)) = owner else {
                return Err(ExecutionLedgerError::Corrupt(
                    "NAV checkpoint has no run owner",
                ));
            };
            let cursor_proven = source.cursors.get(&point.source_cursor_key).is_some_and(|c| {
                c.order_complete && c.fill_complete && c.bill_complete
                    && c.account_as_of_ms <= point.account_as_of_ms
                    && point.account_as_of_ms - c.account_as_of_ms
                        <= crate::exchange_evidence::MAX_SOURCE_COVERAGE_GAP_MS
                    && serde_json::to_vec(c).ok().is_some_and(|data| {
                        format!("{:x}", Sha256::digest(data)) == point.source_cursor_sha256
                    })
            });
            if point.account_as_of_ms < started_at_ms
                || ended_at_ms.is_some_and(|end| point.observed_at_ms > end)
                || previous.is_some_and(|p| {
                    point.observed_at_ms <= p.observed_at_ms
                        || point.account_as_of_ms < p.account_as_of_ms
                })
                || point.previous_sha256 != previous.map_or("", |p| &p.checksum_sha256)
                || !cursor_proven
            {
                return Err(ExecutionLedgerError::Corrupt(
                    "invalid NAV checkpoint lineage",
                ));
            }
            if let Some(prior) = previous.filter(|p| p.run_id == point.run_id) {
                if point.utc_day_start_ms == prior.utc_day_start_ms
                    && Decimal::from_str(&point.daily_gross_loss_usdt).ok()
                        < Decimal::from_str(&prior.daily_gross_loss_usdt).ok()
                {
                    return Err(ExecutionLedgerError::Corrupt("strategy daily loss rewound"));
                }
                let prior_peak = Decimal::from_str(&prior.high_water_usdt)
                    .map_err(|_| ExecutionLedgerError::Corrupt("invalid prior NAV peak"))?;
                let nav = Decimal::from_str(&point.nav_usdt)
                    .map_err(|_| ExecutionLedgerError::Corrupt("invalid checkpoint NAV"))?;
                if Decimal::from_str(&point.high_water_usdt).ok() != Some(prior_peak.max(nav)) {
                    return Err(ExecutionLedgerError::Corrupt(
                        "NAV peak rewound or invented",
                    ));
                }
            } else {
                let initial = Decimal::from_str(allocation_usdt)
                    .map_err(|_| ExecutionLedgerError::Corrupt("invalid NAV allocation"))?;
                let nav = Decimal::from_str(&point.nav_usdt)
                    .map_err(|_| ExecutionLedgerError::Corrupt("invalid initial NAV"))?;
                if Decimal::from_str(&point.high_water_usdt).ok() != Some(initial.max(nav)) {
                    return Err(ExecutionLedgerError::Corrupt("invalid first NAV peak"));
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

    fn attested_source(run: &StrategyCapitalReservation) -> DurableExchangeEvidence {
        let mut source = DurableExchangeEvidence::new(run).unwrap();
        source.cursors.insert(
            "source:1".to_owned(),
            crate::exchange_evidence::ExchangeCoverageCursor {
                source: "OKX_REST_ACCOUNT_HISTORY".to_owned(),
                source_received_at: "2026-10-14T00:00:00Z".to_owned(),
                generation: "source-test-generation".to_owned(),
                account_as_of_ms: 1500,
                order_complete: true,
                fill_complete: true,
                bill_complete: true,
                covered_ranges: serde_json::json!({}),
            },
        );
        seal_source_for_test(&mut source);
        source.validate().unwrap();
        source
    }

    fn seal_source_for_test(source: &mut DurableExchangeEvidence) {
        let bytes = serde_json::to_vec(&(
            &source.schema, &source.account_uid, &source.run_id,
            &source.events, &source.cursors,
        )).unwrap();
        source.checksum_sha256 = format!("{:x}", Sha256::digest(bytes));
    }

    fn nav_point(
        run: &StrategyCapitalReservation,
        source: &DurableExchangeEvidence,
        at_ms: u64,
        nav: &str,
        peak: &str,
        previous: &str,
    ) -> StrategyNavPoint {
        let as_of = at_ms.saturating_sub(200);
        let (cursor_key, cursor) = source.cursors.iter()
            .filter(|(_, c)| c.account_as_of_ms <= as_of)
            .max_by_key(|(_, c)| c.account_as_of_ms)
            .unwrap();
        let mut point = StrategyNavPoint {
            schema: STRATEGY_NAV_POINT_SCHEMA_V1.to_owned(),
            run_id: run.allocation_id.clone(),
            observed_at_ms: at_ms,
            account_as_of_ms: as_of,
            evidence_sha256: source.checksum_sha256.clone(),
            source_cursor_key: cursor_key.clone(),
            source_cursor_sha256: format!("{:x}", Sha256::digest(serde_json::to_vec(cursor).unwrap())),
            nav_usdt: nav.to_owned(),
            high_water_usdt: peak.to_owned(),
            daily_gross_loss_usdt: "0".to_owned(),
            utc_day_start_ms: at_ms.saturating_sub(200) / 86_400_000 * 86_400_000,
            previous_sha256: previous.to_owned(),
            checksum_sha256: String::new(),
        };
        point.checksum_sha256 = point.hash().unwrap();
        point
    }

    #[test]
    fn nav_checkpoint_allows_terminal_negative_nav_and_utc_day_loss_reset() {
        let mut run = reservation();
        run.started_at_ms = 86_390_000;
        let mut source = attested_source(&run);
        let original = source.cursors.remove("source:1").unwrap();
        let mut day_one = original.clone();
        day_one.account_as_of_ms = 86_390_000;
        source.cursors.insert("day:one".to_owned(), day_one);
        let mut day_two = original;
        day_two.account_as_of_ms = 86_400_100;
        source.cursors.insert("day:two".to_owned(), day_two);
        seal_source_for_test(&mut source);
        source.validate().unwrap();

        let mut first = nav_point(&run, &source, 86_399_900, "95", "100", "");
        first.daily_gross_loss_usdt = "5".to_owned();
        first.checksum_sha256 = first.hash().unwrap();
        let mut second =
            nav_point(&run, &source, 86_400_500, "-1", "100", &first.checksum_sha256);
        second.daily_gross_loss_usdt = "0".to_owned();
        second.checksum_sha256 = second.hash().unwrap();
        let history = StrategyPeriodHistory {
            nav_points: vec![first, second],
            ..StrategyPeriodHistory::default()
        };
        history.validate(Some(&run), Some(&source)).unwrap();
        assert_eq!(history.nav_points[1].utc_day_start_ms, 86_400_000);
        assert_ne!(
            history.nav_points[0].source_cursor_sha256,
            history.nav_points[1].source_cursor_sha256,
        );
        let mut bad = history.clone();
        bad.nav_points[1].account_as_of_ms = 86_399_700;
        bad.nav_points[1].checksum_sha256 = bad.nav_points[1].hash().unwrap();
        assert!(bad.validate(Some(&run), Some(&source)).is_err());

        let mut missing_source = source.clone();
        missing_source.cursors.remove("day:one");
        seal_source_for_test(&mut missing_source);
        assert!(history.validate(Some(&run), Some(&missing_source)).is_err());
    }

    #[test]
    fn nav_high_water_chain_is_durable_bounded_and_fails_closed_on_tampering() {
        let run = reservation();
        let source = attested_source(&run);
        let first = nav_point(&run, &source, 2000, "100", "100", "");
        let second = nav_point(&run, &source, 3000, "103", "103", &first.checksum_sha256);
        let third = nav_point(&run, &source, 4000, "98", "103", &second.checksum_sha256);
        let history = StrategyPeriodHistory {
            nav_points: vec![first, second, third],
            ..StrategyPeriodHistory::default()
        };
        history.validate(Some(&run), Some(&source)).unwrap();
        let bytes = serde_json::to_vec(&history).unwrap();
        let restored: StrategyPeriodHistory = serde_json::from_slice(&bytes).unwrap();
        restored.validate(Some(&run), Some(&source)).unwrap();
        assert_eq!(restored.nav_points[2].high_water_usdt, "103");
        let mut bad = restored.clone();
        bad.nav_points[1].daily_gross_loss_usdt = "2".to_owned();
        bad.nav_points[1].checksum_sha256 = bad.nav_points[1].hash().unwrap();
        bad.nav_points[2].previous_sha256 = bad.nav_points[1].checksum_sha256.clone();
        bad.nav_points[2].checksum_sha256 = bad.nav_points[2].hash().unwrap();
        assert!(bad.validate(Some(&run), Some(&source)).is_err(), "daily loss cannot go backward");
        bad = restored.clone();
        bad.nav_points[1].account_as_of_ms = 1600;
        bad.nav_points[1].checksum_sha256 = bad.nav_points[1].hash().unwrap();
        assert!(bad.validate(Some(&run), Some(&source)).is_err(), "invalid UTC stamp");
        bad = restored.clone();
        bad.nav_points[1].source_cursor_sha256 = "f".repeat(64);
        bad.nav_points[1].checksum_sha256 = bad.nav_points[1].hash().unwrap();
        assert!(bad.validate(Some(&run), Some(&source)).is_err(), "cursor substitution rejected");
        let mut bad = restored.clone();
        bad.nav_points[1].nav_usdt = "300".to_owned();
        assert!(bad.validate(Some(&run), Some(&source)).is_err());
        bad = restored.clone();
        bad.nav_points[2].high_water_usdt = "98".to_owned();
        bad.nav_points[2].checksum_sha256 = bad.nav_points[2].hash().unwrap();
        assert!(bad.validate(Some(&run), Some(&source)).is_err());
        bad = restored.clone();
        bad.nav_points[2].previous_sha256 = "foreign".to_owned();
        bad.nav_points[2].checksum_sha256 = bad.nav_points[2].hash().unwrap();
        assert!(bad.validate(Some(&run), Some(&source)).is_err());
        bad = restored.clone();
        bad.nav_points[1].account_as_of_ms = 0;
        assert!(bad.validate(Some(&run), Some(&source)).is_err());
        bad = restored;
        let last = bad.nav_points[2].clone();
        bad.nav_points.resize(MAX_STRATEGY_NAV_POINTS + 1, last);
        assert!(bad.validate(Some(&run), Some(&source)).is_err());
        assert!(!bytes.is_empty());
    }

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

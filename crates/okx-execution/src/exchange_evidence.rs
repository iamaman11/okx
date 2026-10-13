use std::collections::BTreeMap;

use okx_observation::AccountLedgerFacts;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{ExecutionLedgerEntry, ExecutionLedgerError, StrategyCapitalReservation};

pub const EXCHANGE_EVIDENCE_SCHEMA_V1: &str = "okx.exchange-evidence/v1";
const MAX_EVENTS: usize = 40_000;
const MAX_CURSORS: usize = 4_096;
const MAX_BYTES: usize = 24 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredExchangeEvent {
    pub payload: Value,
    pub source_intent_id: Option<String>,
    pub strategy_run_id: Option<String>,
    pub allocation: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExchangeCoverageCursor {
    pub source: String,
    pub source_received_at: String,
    pub generation: String,
    pub account_as_of_ms: u64,
    pub order_complete: bool,
    pub fill_complete: bool,
    pub bill_complete: bool,
    pub covered_ranges: Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableExchangeEvidence {
    pub schema: String,
    pub account_uid: String,
    pub run_id: String,
    pub events: BTreeMap<String, StoredExchangeEvent>,
    pub cursors: BTreeMap<String, ExchangeCoverageCursor>,
    pub checksum_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ExchangeEvidenceSummary {
    pub schema: &'static str,
    pub run_id: String,
    pub events: usize,
    pub cursors: usize,
    pub checksum_sha256: String,
    pub latest_source_complete: bool,
}

impl DurableExchangeEvidence {
    pub fn new(run: &StrategyCapitalReservation) -> Result<Self, ExecutionLedgerError> {
        let mut value = Self {
            schema: EXCHANGE_EVIDENCE_SCHEMA_V1.to_owned(),
            account_uid: run.account_uid_fingerprint.clone(),
            run_id: run.allocation_id.clone(),
            events: BTreeMap::new(),
            cursors: BTreeMap::new(),
            checksum_sha256: String::new(),
        };
        value.seal()?;
        Ok(value)
    }

    fn hash(&self) -> Result<String, ExecutionLedgerError> {
        let bytes = serde_json::to_vec(&(
            &self.schema,
            &self.account_uid,
            &self.run_id,
            &self.events,
            &self.cursors,
        ))?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }

    pub fn validate(&self) -> Result<(), ExecutionLedgerError> {
        if self.schema != EXCHANGE_EVIDENCE_SCHEMA_V1
            || self.account_uid.trim().is_empty()
            || self.run_id.trim().is_empty()
            || self.checksum_sha256 != self.hash()?
            || self.events.len() > MAX_EVENTS
            || self.cursors.len() > MAX_CURSORS
            || serde_json::to_vec(self)?.len() > MAX_BYTES
            || self.events.values().any(|e| {
                !matches!(e.allocation.as_str(), "RUN_OWNED" | "UNALLOCATED")
                    || (e.allocation == "RUN_OWNED"
                        && (e.strategy_run_id.as_deref() != Some(self.run_id.as_str())
                            || e.source_intent_id.is_none()))
                    || (e.allocation == "UNALLOCATED" && e.strategy_run_id.is_some())
            })
        {
            return Err(ExecutionLedgerError::Corrupt(
                "invalid durable exchange evidence",
            ));
        }
        Ok(())
    }

    fn seal(&mut self) -> Result<(), ExecutionLedgerError> {
        self.checksum_sha256 = self.hash()?;
        self.validate()
    }

    fn add(&mut self, key: String, value: StoredExchangeEvent) -> Result<(), ExecutionLedgerError> {
        if let Some(old) = self.events.get(&key) {
            return if old == &value {
                Ok(())
            } else {
                Err(ExecutionLedgerError::ExchangeEvidenceConflict)
            };
        }
        if self.events.len() >= MAX_EVENTS {
            return Err(ExecutionLedgerError::ExchangeEvidenceCapacity);
        }
        self.events.insert(key, value);
        Ok(())
    }

    /// Append-only source evidence. Absence from a short venue window never deletes old events.
    /// Only the existing ledger owner can call this and atomically persist the result.
    pub fn capture(
        &mut self,
        facts: &AccountLedgerFacts,
        entries: &[ExecutionLedgerEntry],
        run: &StrategyCapitalReservation,
    ) -> Result<(), ExecutionLedgerError> {
        self.validate()?;
        if self.run_id != run.allocation_id
            || self.account_uid != run.account_uid_fingerprint
            || self.account_uid != facts.summary.authority.account_uid_fingerprint
        {
            return Err(ExecutionLedgerError::AccountScopeMismatch);
        }
        let now = facts
            .summary
            .current_account_as_of_ms
            .as_deref()
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or(ExecutionLedgerError::Corrupt("missing exchange as-of"))?;
        if now < run.started_at_ms {
            return Err(ExecutionLedgerError::Corrupt("exchange as-of before run"));
        }
        let owner =
            |ord: Option<&str>, cl: &str, inst: &str, side: Option<&str>, pos: Option<&str>| {
                let matches = entries
                    .iter()
                    .filter(|e| {
                        ord.is_some()
                            && e.record.order_id.as_deref() == ord
                            && e.record.plan.client_order_id == cl
                            && e.record.plan.instrument_id == inst
                            && side.is_none_or(|s| e.record.plan.side.as_str() == s)
                            && pos.is_none_or(|s| e.record.plan.position_side.as_str() == s)
                    })
                    .collect::<Vec<_>>();
                if matches.len() > 1 {
                    Err(ExecutionLedgerError::ExchangeEvidenceConflict)
                } else {
                    Ok(matches.first().copied())
                }
            };
        let item = |value: Value, e: Option<&ExecutionLedgerEntry>, funding: bool| {
            let id = if funding {
                None
            } else {
                e.and_then(|x| x.record.strategy_run_id.clone())
            };
            StoredExchangeEvent {
                payload: value,
                source_intent_id: e.map(|x| x.record.plan.intent_id.clone()),
                allocation: if id.is_some() {
                    "RUN_OWNED"
                } else {
                    "UNALLOCATED"
                }
                .to_owned(),
                strategy_run_id: id,
            }
        };
        for o in &facts.exchange_orders {
            let time = o
                .update_time_ms
                .parse::<u64>()
                .map_err(|_| ExecutionLedgerError::Corrupt("invalid order timestamp"))?;
            if time < run.started_at_ms {
                continue;
            }
            let e = owner(
                Some(&o.order_id),
                &o.client_order_id,
                &o.instrument_id,
                None,
                None,
            )?;
            self.add(format!(
                "order:{}:{}:{}", o.instrument_type, o.order_id, o.update_time_ms
            ), item(json!({
                "source":"OKX_REST_ORDER","instType":o.instrument_type,"instId":o.instrument_id,
                "ordId":o.order_id,"clOrdId":o.client_order_id,"state":o.state,"uTime":o.update_time_ms
            }), e, false))?;
        }
        for f in &facts.exchange_fills {
            if f.fill_time_ms < run.started_at_ms {
                continue;
            }
            let e = owner(
                f.order_id.as_deref(),
                &f.client_order_id,
                &f.instrument_id,
                Some(&f.side),
                Some(&f.position_side),
            )?;
            self.add(format!("fill:{}:{}:{}", f.instrument_type, f.instrument_id, f.trade_id),
                item(json!({
                    "source":"OKX_REST_FILL","instType":f.instrument_type,"instId":f.instrument_id,
                    "ordId":f.order_id,"clOrdId":f.client_order_id,"tradeId":f.trade_id,
                    "billId":f.bill_id,"side":f.side,"posSide":f.position_side,
                    "fillPx":f.fill_price,"fillSz":f.fill_size,"fee":f.fee,
                    "feeCcy":f.fee_currency,"fillPnl":f.fill_pnl,"execType":f.execution_type,
                    "ts":f.fill_time_ms
                }), e, false))?;
        }
        for b in &facts.exchange_bills {
            if b.timestamp_ms < run.started_at_ms {
                continue;
            }
            let funding = matches!(b.sub_type.as_str(), "173" | "174");
            let fills = facts
                .exchange_fills
                .iter()
                .filter(|f| {
                    f.bill_id.as_deref() == Some(b.bill_id.as_str())
                        && f.instrument_id == b.instrument_id
                        && b.trade_id.as_deref() == Some(f.trade_id.as_str())
                        && b.order_id
                            .as_deref()
                            .is_none_or(|id| f.order_id.as_deref() == Some(id))
                        && b.client_order_id
                            .as_deref()
                            .is_none_or(|id| f.client_order_id == id)
                })
                .collect::<Vec<_>>();
            if fills.len() > 1 {
                return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
            }
            let e = if funding {
                None
            } else if let Some(f) = fills.first() {
                owner(
                    f.order_id.as_deref(),
                    &f.client_order_id,
                    &f.instrument_id,
                    Some(&f.side),
                    Some(&f.position_side),
                )?
            } else {
                None
            };
            self.add(format!("bill:{}", b.bill_id), item(json!({
                "source":"OKX_REST_BILL","billId":b.bill_id,"instId":b.instrument_id,
                "ordId":b.order_id,"clOrdId":b.client_order_id,"tradeId":b.trade_id,
                "ccy":b.currency,"subType":b.sub_type,"fee":b.fee,"pnl":b.pnl,"ts":b.timestamp_ms
            }), e, funding))?;
        }
        let key = format!(
            "{}:{}",
            facts.summary.account_generation, facts.summary.source_received_at
        );
        let cursor = ExchangeCoverageCursor {
            source: "OKX_REST_ACCOUNT_HISTORY".to_owned(),
            source_received_at: facts.summary.source_received_at.clone(),
            generation: facts.summary.account_generation.clone(),
            account_as_of_ms: now,
            order_complete: facts.exchange_orders_history_complete,
            fill_complete: facts.exchange_fills_history_complete,
            bill_complete: facts.bills_history_complete,
            covered_ranges: serde_json::to_value(&facts.summary.history_coverage)?,
        };
        if let Some(previous) = self.cursors.get(&key) {
            if previous != &cursor {
                return Err(ExecutionLedgerError::ExchangeEvidenceConflict);
            }
        } else {
            if self.cursors.len() >= MAX_CURSORS {
                return Err(ExecutionLedgerError::ExchangeEvidenceCapacity);
            }
            self.cursors.insert(key, cursor);
        }
        self.seal()
    }

    pub fn summary(&self) -> ExchangeEvidenceSummary {
        let latest = self.cursors.values().max_by_key(|v| v.account_as_of_ms);
        ExchangeEvidenceSummary {
            schema: EXCHANGE_EVIDENCE_SCHEMA_V1,
            run_id: self.run_id.clone(),
            events: self.events.len(),
            cursors: self.cursors.len(),
            checksum_sha256: self.checksum_sha256.clone(),
            latest_source_complete: latest
                .is_some_and(|v| v.order_complete && v.fill_complete && v.bill_complete),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn duplicate_is_idempotent_conflict_and_corruption_fail_closed() {
        let mut evidence = DurableExchangeEvidence {
            schema: EXCHANGE_EVIDENCE_SCHEMA_V1.to_owned(),
            account_uid: "uid".into(),
            run_id: "run_1".into(),
            events: BTreeMap::new(),
            cursors: BTreeMap::new(),
            checksum_sha256: String::new(),
        };
        evidence.seal().unwrap();
        let original = StoredExchangeEvent {
            payload: json!({"tradeId":"9","fee":"-0.01"}),
            source_intent_id: None,
            strategy_run_id: None,
            allocation: "UNALLOCATED".into(),
        };
        evidence
            .add("fill:SWAP:BTC:9".into(), original.clone())
            .unwrap();
        evidence
            .add("fill:SWAP:BTC:9".into(), original.clone())
            .unwrap();
        assert_eq!(evidence.events.len(), 1);
        let mut other = original;
        other.payload["fee"] = json!("-0.02");
        assert!(matches!(
            evidence.add("fill:SWAP:BTC:9".into(), other),
            Err(ExecutionLedgerError::ExchangeEvidenceConflict)
        ));
        evidence.seal().unwrap();
        let serialized = serde_json::to_vec(&evidence).unwrap();
        let mut reopened: DurableExchangeEvidence = serde_json::from_slice(&serialized).unwrap();
        reopened.validate().unwrap();
        reopened.events.get_mut("fill:SWAP:BTC:9").unwrap().payload["fee"] = json!("100");
        assert!(matches!(
            reopened.validate(),
            Err(ExecutionLedgerError::Corrupt(_))
        ));
    }
}

//! One coherent account census snapshot. Observations do not grant a send or release.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub struct AccountCensusRequest {
    pub positions: Vec<(String, String)>,
    pub client_order_ids: Vec<String>,
    pub terminal_peer_release_intent: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone)]
pub struct AccountPositionCensus {
    pub instrument: String,
    pub direction: String,
    pub lots: Vec<StoredLot>,
    pub owned_proofs: Vec<(String, Option<ProtectionReceipt>, Option<CentralReceipt>)>,
}

/// Raw terminal-peer inputs, deliberately not a verified settlement/release proof.
/// Replaying the original fill/bill/fee validator remains mandatory before release.
#[derive(Debug, Clone)]
pub struct TerminalPeerSources {
    pub intent_id: String,
    pub receipt: Option<OperationReceipt>,
    pub late_pending: bool,
    pub fills: Option<SourceCursor>,
    pub bills: Option<SourceCursor>,
    pub lots: Vec<StoredLot>,
}

#[derive(Debug, Clone)]
pub struct AccountCensusSnapshot {
    pub observed_at_ms: i64,
    pub positions: Vec<AccountPositionCensus>,
    pub pending_orders: Vec<Option<StoredIntent>>,
    pub intents: Vec<(StoredIntent, Option<CentralReceipt>)>,
    pub pending_central: Vec<String>,
    pub terminal_peer_sources: Vec<TerminalPeerSources>,
    /// Original immutable fill/bill/fee sources replayed in this very snapshot.
    /// These peers still require the caller's independently signed flat venue census.
    pub verified_terminal_peers: BTreeSet<String>,
    pub financial_risk: FinancialRisk,
    pub policy: AccountPolicySnapshot,
}

impl AccountStore {
    pub(crate) fn active_intents_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        limit: usize,
    ) -> StoreResult<Vec<StoredIntent>> {
        if let Some((i, a)) = scope {
            Self::verify_source_account_sql(db, i, a)?;
        }
        let (sql, args) = match scope {
            None => (
                "SELECT intent_id FROM intents WHERE state IN('PREPARED','RESERVED','POSTING','UNKNOWN','ACKNOWLEDGED','PARTIALLY_FILLED') ORDER BY state,updated_at,intent_id LIMIT ?1",
                vec![rusqlite::types::Value::from(limit.min(1000) as i64)],
            ),
            Some((i, a)) => (
                "SELECT intent_id FROM intents WHERE state IN('PREPARED','RESERVED','POSTING','UNKNOWN','ACKNOWLEDGED','PARTIALLY_FILLED') AND installation_id=?2 AND account_key=?3 ORDER BY state,updated_at,intent_id LIMIT ?1",
                vec![
                    (limit.min(1000) as i64).into(),
                    i.to_owned().into(),
                    a.to_owned().into(),
                ],
            ),
        };
        let mut q = db.prepare(sql)?;
        let ids = q
            .query_map(rusqlite::params_from_iter(args), |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Self::census_load_intents(db, scope, ids)
    }

    pub(crate) fn reconcilable_intents_page_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        limit: usize,
        offset: usize,
        now: i64,
    ) -> StoreResult<Vec<StoredIntent>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        if let Some((i, a)) = scope {
            Self::verify_source_account_sql(db, i, a)?;
        }
        // The CTEs scope EVERY original union branch and correlated join.
        // Explicit main.* references prevent accidental recursive CTE lookup.
        let prefix = if scope.is_some() {
            "WITH intents AS (SELECT * FROM main.intents WHERE installation_id=?4 AND account_key=?5), risk_receipts AS (SELECT * FROM main.risk_receipts WHERE installation_id=?4 AND account_key=?5), operation_receipts AS (SELECT * FROM main.operation_receipts WHERE installation_id=?4 AND account_key=?5) "
        } else {
            ""
        };
        let original = "SELECT intent_id FROM (
            SELECT intent_id,CASE WHEN state='UNSENT_PENDING_CENTRAL' THEN 0 ELSE 3 END AS priority
            FROM risk_receipts
            WHERE state IN('HELD','POSTED','UNKNOWN','OPEN','TERMINAL_PENDING_CENTRAL','UNSENT_PENDING_CENTRAL','LATE_SETTLEMENT_PENDING')
            UNION ALL
            SELECT i.intent_id,1 AS priority FROM intents i
            JOIN operation_receipts o ON o.operation_id='risk-reserve:'||i.intent_id
            WHERE o.kind='RISK_RESERVE_ATTEMPT' AND o.request_hash=i.request_hash
              AND i.state IN('PREPARED','REJECTED')
              AND NOT EXISTS(SELECT 1 FROM risk_receipts r WHERE r.intent_id=i.intent_id)
              AND NOT EXISTS(SELECT 1 FROM operation_receipts ack
                WHERE ack.operation_id='central-release:'||o.result_ref AND ack.kind='CENTRAL_RELEASE')
            UNION ALL
            SELECT i.intent_id,2 AS priority FROM intents i
            WHERE i.state='PREPARED' AND i.deadline<=?2
              AND NOT EXISTS(SELECT 1 FROM risk_receipts r WHERE r.intent_id=i.intent_id)
              AND NOT EXISTS(SELECT 1 FROM operation_receipts a WHERE a.operation_id='risk-reserve:'||i.intent_id)
            ) GROUP BY intent_id ORDER BY MIN(priority),intent_id LIMIT ?1 OFFSET ?3";
        let mut args = vec![
            rusqlite::types::Value::from(limit.min(100) as i64),
            now.into(),
            (offset.min(100_000) as i64).into(),
        ];
        if let Some((i, a)) = scope {
            args.push(i.to_owned().into());
            args.push(a.to_owned().into());
        }
        let mut q = db.prepare(&format!("{prefix}{original}"))?;
        let ids = q
            .query_map(rusqlite::params_from_iter(args), |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Self::census_load_intents(db, scope, ids)
    }

    fn census_load_intents(
        db: &Connection,
        scope: Option<(&str, &str)>,
        ids: Vec<String>,
    ) -> StoreResult<Vec<StoredIntent>> {
        ids.into_iter()
            .map(|id| {
                Self::lookup_trade_intent_sql(db, scope, &TradeIntentLookup::IntentId(id))?
                    .ok_or(StoreError::Invalid("INTENT_MISSING"))
            })
            .collect()
    }

    pub(crate) fn risk_census_intents_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        limit: usize,
        now: i64,
    ) -> StoreResult<(Vec<StoredIntent>, Vec<String>)> {
        if limit == 0 || limit > 100 {
            return Err(StoreError::Invalid("COHORT_CENSUS_LIMIT_INVALID"));
        }
        let live = Self::active_intents_sql(db, scope, limit)?;
        let pending = Self::reconcilable_intents_page_sql(db, scope, limit, 0, now)?;
        let mut all = BTreeMap::new();
        for intent in &live {
            all.insert(intent.intent_id.clone(), intent.clone());
        }
        let mut unresolved = std::collections::BTreeSet::new();
        for intent in pending {
            unresolved.insert(intent.intent_id.clone());
            all.insert(intent.intent_id.clone(), intent);
        }
        if live.len() >= limit || unresolved.len() >= limit || all.len() >= limit {
            return Err(StoreError::Invalid("COHORT_LOCAL_INTENTS_TRUNCATED"));
        }
        Ok((
            all.into_values().collect(),
            unresolved.into_iter().collect(),
        ))
    }

    pub(crate) fn census_snapshot_sql(
        db: &rusqlite::Transaction<'_>,
        installation: &str,
        account: &str,
        request: AccountCensusRequest,
        now: i64,
    ) -> StoreResult<AccountCensusSnapshot> {
        Self::verify_source_account_sql(db, installation, account)?;
        if request.positions.len() > 100 || request.client_order_ids.len() > 100 {
            return Err(StoreError::Invalid("COHORT_CENSUS_REQUEST_TOO_LARGE"));
        }
        let scope = Some((installation, account));
        let positions = request
            .positions
            .into_iter()
            .map(|(instrument, direction)| {
                let lots = Self::active_lots_sql(db, scope, &instrument)?;
                let owned_proofs = lots
                    .iter()
                    .filter(|lot| lot.side == direction)
                    .map(|lot| {
                        let id = lot
                            .intent_id
                            .as_ref()
                            .ok_or(StoreError::Invalid("UNATTRIBUTED_POSITION_LOT"))?;
                        Ok((
                            id.clone(),
                            Self::protection_sql(db, scope, id)?,
                            Self::reservation_receipt_sql(
                                db,
                                scope,
                                id,
                                CentralReceiptView::ActiveExposure,
                            )?,
                        ))
                    })
                    .collect::<StoreResult<Vec<_>>>()?;
                Ok(AccountPositionCensus {
                    instrument,
                    direction,
                    lots,
                    owned_proofs,
                })
            })
            .collect::<StoreResult<Vec<_>>>()?;
        let pending_orders = request
            .client_order_ids
            .into_iter()
            .map(|key| {
                Self::lookup_trade_intent_sql(db, scope, &TradeIntentLookup::ClientOrderId(key))
            })
            .collect::<StoreResult<Vec<_>>>()?;
        let (intents, pending_central) =
            Self::risk_census_intents_sql(db, scope, request.limit, now)?;
        let mut terminal_peer_sources = Vec::new();
        let mut verified_terminal_peers = BTreeSet::new();
        if let Some(releasing) = request.terminal_peer_release_intent {
            for intent in &intents {
                if !intent.risk_increase
                    || intent.state != IntentState::Filled
                    || !intent.instrument_id.ends_with("-USDT-SWAP")
                    || releasing == intent.intent_id
                    || pending_central.binary_search(&intent.intent_id).is_err()
                {
                    continue;
                }
                let late_pending: bool = db.query_row("SELECT state='LATE_SETTLEMENT_PENDING' FROM risk_receipts WHERE installation_id=?1 AND account_key=?2 AND intent_id=?3",params![installation,account,intent.intent_id],|r|r.get(0)).optional()?.unwrap_or(false);
                let sources = TerminalPeerSources {
                    intent_id: intent.intent_id.clone(),
                    receipt: Self::operation_sql(
                        db,
                        scope,
                        &format!("entry-settlement:{}", intent.intent_id),
                    )?,
                    late_pending,
                    fills: Self::scoped_source_cursor_sql(
                        db,
                        installation,
                        account,
                        "fills",
                        "SWAP",
                    )?,
                    bills: Self::scoped_source_cursor_sql(
                        db,
                        installation,
                        account,
                        "bills",
                        "account",
                    )?,
                    lots: Self::active_lots_sql(db, scope, &intent.instrument_id)?,
                };
                if Self::verify_terminal_peer_sources_sql(db, scope, intent, &sources)? {
                    verified_terminal_peers.insert(intent.intent_id.clone());
                }
                terminal_peer_sources.push(sources);
            }
        }
        let intents = intents
            .into_iter()
            .map(|intent| {
                let receipt = Self::reservation_receipt_sql(
                    db,
                    scope,
                    &intent.intent_id,
                    CentralReceiptView::ActiveExposure,
                )?;
                Ok((intent, receipt))
            })
            .collect::<StoreResult<Vec<_>>>()?;
        Ok(AccountCensusSnapshot {
            observed_at_ms: now,
            positions,
            pending_orders,
            intents,
            pending_central,
            terminal_peer_sources,
            verified_terminal_peers,
            financial_risk: Self::financial_risk_sql(db, scope, now, None)?,
            policy: Self::account_policy_sql(db, scope)?,
        })
    }

    fn verify_terminal_peer_sources_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        intent: &StoredIntent,
        sources: &TerminalPeerSources,
    ) -> StoreResult<bool> {
        let Some(saved) = sources.receipt.as_ref().filter(|saved| {
            saved.kind == "ENTRY_SETTLEMENT_PROVEN" && saved.request_hash == intent.plan_hash
        }) else {
            return Ok(false);
        };
        let proof: EntrySettlementEvidence = serde_json::from_str(&saved.result_ref)
            .map_err(|_| StoreError::Invalid("COHORT_TERMINAL_PEER_PROOF_CORRUPT"))?;
        if proof.schema != "okx.entry-terminal-settlement/v1"
            || proof.intent_id != intent.intent_id
            || proof.plan_hash != intent.plan_hash
            || sources.late_pending
        {
            return Ok(false);
        }
        let (Some(fills), Some(bills)) = (&sources.fills, &sources.bills) else {
            return Ok(false);
        };
        if fills.generation <= 0
            || bills.generation <= 0
            || !fills.cursor.is_empty()
            || !bills.cursor.is_empty()
            || !fills.coverage_gaps.is_empty()
            || !bills.coverage_gaps.is_empty()
        {
            return Ok(false);
        }
        let frontier = fills.watermark.min(bills.watermark);
        if frontier < proof.venue_update_ms
            || frontier < proof.source_covered_through_ms
            || sources.lots.iter().any(|lot| {
                lot.intent_id.as_deref() == Some(intent.intent_id.as_str())
                    && lot.remaining_qty > Decimal::ZERO
            })
        {
            return Ok(false);
        }
        // An existing terminal witness is required above. Re-run the complete
        // original immutable source validator; a cached receipt is not proof.
        let changes = tx.total_changes();
        let original = Self::bind_terminal_entry_settlement_sql(
            tx,
            scope,
            &intent.intent_id,
            proof.venue_update_ms,
            proof.venue_filled_quantity,
            frontier,
        )?;
        if tx.total_changes() != changes {
            return Err(StoreError::Invalid("COHORT_TERMINAL_PEER_REPLAY_WROTE_SQL"));
        }
        if original != proof {
            return Err(StoreError::Invalid("COHORT_TERMINAL_PEER_SOURCE_CHANGED"));
        }
        Ok(true)
    }
}

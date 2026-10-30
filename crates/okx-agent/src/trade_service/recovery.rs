use super::*;

// Conservative bound for OKX's short-lived canceled/unfilled order
// history. An expired archive cannot establish absence of lost POSTs.
fn verify_recovery_order_history_window(backup: i64, observed: i64) -> Result<(), String> {
    if backup <= 0 || observed < backup || observed - backup >= 119 * 60_000 {
        return Err("STORAGE_RECOVERY_ORDER_HISTORY_NOT_PROVEN".into());
    }
    Ok(())
}

#[cfg(test)]
mod recovery_history_tests {
    use super::verify_recovery_order_history_window;

    #[test]
    fn crash_history_window_never_certifies_expired_or_future_backup() {
        let observed = 1_800_000_000_000;
        assert!(verify_recovery_order_history_window(observed - 60_000, observed).is_ok());
        assert!(verify_recovery_order_history_window(observed - 119 * 60_000, observed).is_err());
        assert!(
            verify_recovery_order_history_window(observed - 89 * 86_400_000, observed).is_err()
        );
        assert!(verify_recovery_order_history_window(observed + 1, observed).is_err());
        assert!(verify_recovery_order_history_window(0, observed).is_err());
    }
}

fn recovery_matches(
    local: &okx_execution::RecoverySnapshot,
    central: &RiskAccountBudget,
) -> Result<(), String> {
    if !local.coverage_complete
        || local.unresolved_intents
        || central.active_reservations != local.active_receipts.len() as u64
        || dec(&central.occupied_usd)?
            != local
                .active_receipts
                .iter()
                .map(|r| r.requested_loss_usd)
                .sum::<Decimal>()
        || dec(&central.cumulative_loss_usd)? > local.cumulative_loss_usd
    {
        return Err("STORAGE_RECOVERY_INCOMPLETE".into());
    }
    Ok(())
}
type ProtectionRecoveryCheck = (String, String, Option<Decimal>, String, Decimal);
async fn fresh_recovery_protection(
    trade: okx_api::TradeApi,
    (client, instrument, stop, side, owned): ProtectionRecoveryCheck,
) -> Result<(String, i64), String> {
    let value = trade.algo_order_by_client_id(&client).await.map_err(err)?;
    let received = now();
    if value.instrument_id != instrument
        || value.client_order_id != client
        || value.state != "live"
        || !value.failure_code.is_empty()
        || value.stop_loss_trigger_price_type != "mark"
        || Some(dec(&value.stop_loss_trigger_price)?) != stop
        || dec(&value.size)? < owned
        || value.side == side
    {
        return Err("STORAGE_RECOVERY_PROTECTION_UNVERIFIED".into());
    }
    Ok((digest(&value)?, received))
}
fn recovery_positions(rows: &[okx_observation::AccountPositionState]) -> Result<String, String> {
    let mut identity = Vec::new();
    for p in rows {
        let qty = dec(&p.position)?;
        if qty != Decimal::ZERO {
            identity.push((
                &p.instrument_id,
                &p.position_side,
                &p.margin_mode,
                exact(qty),
                p.average_price.clone(),
            ));
        }
    }
    identity.sort();
    digest(&identity)
}
fn recovery_fresh(observed: i64, received: i64) -> Result<(), String> {
    if !crate::config::evidence_fresh(received, observed, crate::config::ACCOUNT_MAX_AGE_MS) {
        return Err("STORAGE_RECOVERY_SOURCE_STALE".into());
    }
    Ok(())
}
fn recovery_generation(expected: u64, actual: u64, ready: bool) -> Result<(), String> {
    if expected == 0 || expected != actual || !ready {
        return Err("STORAGE_RECOVERY_GENERATION_CHANGED".into());
    }
    Ok(())
}
fn commit_recovery(
    store: &mut okx_execution::AccountStore,
    local: &okx_execution::RecoverySnapshot,
    central: &RiskAccountBudget,
    observed: i64,
    exchange: &str,
    evidence: &str,
) -> okx_execution::StoreResult<()> {
    recovery_fresh(now(), observed)
        .map_err(|_| okx_execution::StoreError::Invalid("STORAGE_RECOVERY_SOURCE_STALE"))?;
    let current = store.recovery_snapshot(now())?;
    recovery_matches(&current, central)
        .map_err(|_| okx_execution::StoreError::Invalid("STORAGE_RECOVERY_INCOMPLETE"))?;
    if digest(&current.active_receipts)
        .map_err(|_| okx_execution::StoreError::Invalid("STORAGE_RECOVERY_INCOMPLETE"))?
        != digest(&local.active_receipts)
            .map_err(|_| okx_execution::StoreError::Invalid("STORAGE_RECOVERY_INCOMPLETE"))?
    {
        return Err(okx_execution::StoreError::Invalid(
            "STORAGE_RECOVERY_INCOMPLETE",
        ));
    }
    // Clearing this storage gate never authorizes POST: the normal fresh
    // all-account central reserve still charges any newer observed loss first.
    store.mark_storage_reconciled(observed, exchange, evidence)
}
impl TradeService {
    async fn recovery_central(
        &self,
        state: &CentralGovernanceSnapshot,
        account: &str,
    ) -> Result<RiskAccountBudget, String> {
        let result = self
            .risk
            .request(self.risk_command(
                state,
                format!("recovery_{}", now()),
                RiskCommandAction::AccountSummary {
                    account_key: account.into(),
                },
            ))
            .await
            .map_err(str::to_owned)?;
        if result.status != RiskDecisionStatus::Observed {
            return Err("STORAGE_RECOVERY_CENTRAL_UNAVAILABLE".into());
        }
        let result = result
            .account_budget
            .ok_or("STORAGE_RECOVERY_CENTRAL_MISSING")?;
        if result.account_key != account {
            return Err("STORAGE_RECOVERY_ACCOUNT_MISMATCH".into());
        }
        Ok(result)
    }
    pub(super) async fn reconcile_restored(&self, r: &Arc<AccountResources>) -> Result<(), String> {
        tokio::time::timeout(Duration::from_secs(60), self.reconcile_restored_bounded(r))
            .await
            .map_err(|_| "STORAGE_RECOVERY_AUDIT_TIMEOUT".to_owned())?
    }
    async fn reconcile_restored_bounded(&self, r: &Arc<AccountResources>) -> Result<(), String> {
        let local = r
            .execution
            .store
            .call(deadline(), |s| s.recovery_snapshot(now()))
            .await
            .map_err(err)?;
        if !local.required {
            return Ok(());
        }
        let backup = local.backup_at.ok_or("STORAGE_RECOVERY_BACKUP_MISSING")?;
        let started = now();
        // A post-crash audit cannot claim coverage from the last 100
        // completed orders: OKX also drops some canceled/unfilled orders
        // after two hours. Full recent+archive history must be exhaustively
        // paginated since the backup. Older recovery needs an independent
        // retained source and stays blocked instead of treating missing
        // orders as evidence of absence.
        verify_recovery_order_history_window(backup, started)?;
        if !r.source_identity_ready().await {
            return Err("STORAGE_RECOVERY_SOURCE_UNVERIFIED".into());
        }
        let generation = r.private_ws.status().await.connection_generation;
        let (state, _) = self.authority(r, None).await?;
        let mut history_proofs = Vec::new();
        let mut witnessed_orders = std::collections::BTreeSet::new();
        // Recovery has to account for every instrument class allowed by
        // OKX, not only swaps/futures. Otherwise an external SPOT/MARGIN
        // order can be missing while a recovery falsely claims completeness.
        for kind in ["SPOT", "MARGIN", "SWAP", "FUTURES", "OPTION"] {
            let history = r
                .execution
                .history
                .complete_campaign_orders(
                    kind,
                    backup,
                    started,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await
                .map_err(|_| "STORAGE_RECOVERY_ORDER_HISTORY_NOT_PROVEN")?;
            if history.from_ms != backup || history.through_ms != started {
                return Err("STORAGE_RECOVERY_ORDER_HISTORY_SCOPE_MISMATCH".into());
            }
            history_proofs.push(history.source_digest);
            for order in history.orders {
                witnessed_orders.insert(order.order_id.clone());
                let updated = order
                    .update_time_ms
                    .parse::<i64>()
                    .map_err(|_| "STORAGE_RECOVERY_ORDER_TIME_INVALID")?;
                if updated < backup {
                    continue;
                }
                let client = order.client_order_id.clone();
                let known = r
                    .execution
                    .store
                    .call(deadline(), move |s| s.by_client_order_id(&client))
                    .await
                    .map_err(err)?
                    .ok_or("STORAGE_RECOVERY_LOST_ORDER_FACT")?;
                if known.instrument_id != order.instrument_id
                    || known
                        .exchange_order_id
                        .as_ref()
                        .is_some_and(|id| id != &order.order_id)
                {
                    return Err("STORAGE_RECOVERY_ORDER_IDENTITY_MISMATCH".into());
                }
            }
        }
        // A complete order list does not prove the money side. Native venue
        // fills and account-wide fee bills must independently cover the same
        // backup→current window; otherwise the crash witness stays blocked.
        // The result is source evidence, not a synthetic USD opening budget.
        let money = r
            .execution
            .history
            .complete_campaign_money_history(
                backup,
                started,
                chrono::Utc::now().timestamp_millis(),
            )
            .await
            .map_err(|_| "STORAGE_RECOVERY_MONEY_HISTORY_NOT_PROVEN")?;
        if money.from_ms != backup || money.through_ms != started {
            return Err("STORAGE_RECOVERY_MONEY_HISTORY_SCOPE_MISMATCH".into());
        }
        if money
            .fills
            .iter()
            .any(|fill| !witnessed_orders.contains(&fill.order_id))
        {
            return Err("STORAGE_RECOVERY_FILL_ORDER_LINK_UNPROVEN".into());
        }
        history_proofs.push(money.source_digest);
        let snapshot = crate::source_capture::capture_account_sources(
            &r.observer,
            &r.private_ws,
            &r.execution,
        )
        .await
        .map_err(err)?;
        let algos = r
            .observer
            .pending_derivative_risk_algos()
            .await
            .map_err(err)?;
        if !algos.complete_within_bound || snapshot.account_uid_fingerprint != r.execution.uid() {
            return Err("STORAGE_RECOVERY_SOURCE_INCOMPLETE".into());
        }
        let mut managed = std::collections::BTreeSet::new();
        for position in &snapshot.positions {
            let Some((direction, qty)) = super::reconcile::verified_venue_position_direction(
                &position.position_side,
                &position.position,
            )?
            else {
                continue;
            };
            let instrument = position.instrument_id.clone();
            let lots = r
                .execution
                .store
                .call(deadline(), move |s| s.active_lots(&instrument))
                .await
                .map_err(err)?;
            let owned = lots
                .iter()
                .filter(|l| l.side == direction)
                .collect::<Vec<_>>();
            let total = owned.iter().try_fold(Decimal::ZERO, |total, lot| {
                if lot.remaining_qty <= Decimal::ZERO {
                    return Err("STORAGE_RECOVERY_LOT_QUANTITY_INVALID".to_owned());
                }
                total
                    .checked_add(lot.remaining_qty)
                    .ok_or("STORAGE_RECOVERY_LOT_QUANTITY_OVERFLOW".to_owned())
            })?;
            if total != qty {
                return Err("STORAGE_RECOVERY_UNATTRIBUTED_POSITION".into());
            }
            let mut seen_intents = std::collections::BTreeSet::new();
            for lot in &owned {
                let id = lot
                    .intent_id
                    .clone()
                    .ok_or("STORAGE_RECOVERY_LOT_OWNER_MISSING")?;
                if !seen_intents.insert(id.clone()) {
                    continue;
                }
                let owned_intent_qty = owned
                    .iter()
                    .filter(|entry| entry.intent_id.as_deref() == Some(id.as_str()))
                    .try_fold(Decimal::ZERO, |total, entry| {
                        if entry.remaining_qty <= Decimal::ZERO {
                            return Err("STORAGE_RECOVERY_LOT_QUANTITY_INVALID".to_owned());
                        }
                        total
                            .checked_add(entry.remaining_qty)
                            .ok_or("STORAGE_RECOVERY_LOT_QUANTITY_OVERFLOW".to_owned())
                    })?;
                let (protection, intent) = r
                    .execution
                    .store
                    .call(deadline(), move |s| {
                        Ok((s.protection(&id)?, s.intent(&id)?))
                    })
                    .await
                    .map_err(err)?;
                let protection = protection.ok_or("STORAGE_RECOVERY_PROTECTION_MISSING")?;
                let intent = intent.ok_or("STORAGE_RECOVERY_PROTECTION_INTENT_MISSING")?;
                if protection.quantity < owned_intent_qty || intent.plan.position_side != lot.side {
                    return Err("STORAGE_RECOVERY_PROTECTION_INCOMPLETE".into());
                }
                // Inventory proves only that an algo ID exists. It cannot
                // attest a live trigger, correct side or current quantity.
                // Use the same exact venue-source gate as trade.status.
                let client = intent
                    .client_order_id
                    .get(..31)
                    .map(|v| format!("s{v}"))
                    .ok_or("STORAGE_RECOVERY_PROTECTION_CLIENT_INVALID")?;
                let algo = r
                    .execution
                    .trade
                    .algo_order_by_client_id(&client)
                    .await
                    .map_err(|_| "STORAGE_RECOVERY_PROTECTION_READBACK_UNAVAILABLE")?;
                super::reconcile::verified_live_protection(&intent, owned_intent_qty, &algo)
                    .map_err(|_| "STORAGE_RECOVERY_PROTECTION_LIVE_UNVERIFIED")?;
                if algo.algo_order_id != protection.algo_id {
                    return Err("STORAGE_RECOVERY_PROTECTION_IDENTITY_MISMATCH".into());
                }
                managed.insert(protection.algo_id);
            }
        }
        if managed
            != algos
                .samples
                .iter()
                .map(|a| a.algo_order_id.clone())
                .collect()
        {
            return Err("STORAGE_RECOVERY_UNATTRIBUTED_ALGO".into());
        }
        for pending in &snapshot.pending_orders {
            let client = pending
                .client_order_id
                .clone()
                .ok_or("STORAGE_RECOVERY_UNATTRIBUTED_ORDER")?;
            if r.execution
                .store
                .call(deadline(), move |s| s.by_client_order_id(&client))
                .await
                .map_err(err)?
                .is_none()
            {
                return Err("STORAGE_RECOVERY_UNATTRIBUTED_ORDER".into());
            }
        }
        recovery_generation(
            generation,
            r.private_ws.status().await.connection_generation,
            r.source_identity_ready().await,
        )?;
        r.execution
            .store
            .call(deadline(), move |s| {
                s.ingest_page(&okx_execution::IngestPage {
                    cursor: Some(okx_execution::SourceCursor {
                        source: "recovery_audit".into(),
                        stream_partition: "account".into(),
                        cursor: String::new(),
                        watermark: started,
                        generation: generation as i64,
                        coverage_gaps: String::new(),
                    }),
                    ..Default::default()
                })
            })
            .await
            .map_err(err)?;
        let local = r
            .execution
            .store
            .call(deadline(), |s| s.recovery_snapshot(now()))
            .await
            .map_err(err)?;
        let central = self
            .recovery_central(&state, &r.configuration.account_key.wire())
            .await?;
        let mut protection_checks = Vec::new();
        for receipt in &local.active_receipts {
            let id = receipt.intent_id.clone();
            let intent = r
                .execution
                .store
                .call(deadline(), move |s| s.intent(&id))
                .await
                .map_err(err)?
                .ok_or("STORAGE_RECOVERY_INTENT_MISSING")?;
            let instrument = intent.instrument_id.clone();
            let lots = r
                .execution
                .store
                .call(deadline(), move |s| s.active_lots(&instrument))
                .await
                .map_err(err)?;
            let owned = lots
                .iter()
                .filter(|l| l.intent_id.as_deref() == Some(&receipt.intent_id))
                .map(|l| l.remaining_qty)
                .sum::<Decimal>();
            let actual = snapshot
                .positions
                .iter()
                .filter(|p| {
                    p.instrument_id == intent.instrument_id
                        && p.position_side == intent.plan.position_side
                })
                .map(|p| dec(&p.position).map(|q| q.abs()))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .sum::<Decimal>();
            if owned <= Decimal::ZERO || actual < owned || intent.client_order_id.len() < 31 {
                return Err("STORAGE_RECOVERY_LOT_COVERAGE_MISSING".into());
            }
            let client = format!("s{}", &intent.client_order_id[..31]);
            protection_checks.push((
                client,
                intent.instrument_id.clone(),
                intent.plan.stop_price,
                intent.plan.side.clone(),
                owned,
            ));
            let result = self
                .risk
                .request(self.risk_command(
                    &state,
                    format!("recover_{}", receipt.intent_id),
                    RiskCommandAction::Inspect {
                        reservation_key: receipt.reservation_key.clone(),
                        plan_sha256: receipt.plan_hash.clone(),
                    },
                ))
                .await
                .map_err(str::to_owned)?;
            if result.status != RiskDecisionStatus::Observed
                || result.reservation_key.as_deref() != Some(&receipt.reservation_key)
                || result.reservation_state != Some(RiskReservationState::Open)
            {
                return Err("STORAGE_RECOVERY_RESERVATION_UNRESOLVED".into());
            }
        }
        use futures_util::{StreamExt, TryStreamExt, stream};
        // A single bounded refresh follows the slower historical audit. Each
        // response retains its real receipt time; audit start is not freshness.
        let protection_calls = protection_checks
            .into_iter()
            .map(|check| fresh_recovery_protection(r.execution.trade.clone(), check))
            .collect::<Vec<_>>();
        let protections = stream::iter(protection_calls)
            .buffer_unordered(4)
            .try_collect::<Vec<_>>();
        let (fresh_snapshot, fresh_algos, (after, central_received), protections) = tokio::try_join!(
            async { r.observer.snapshot().await.map_err(err) },
            async {
                r.observer
                    .pending_derivative_risk_algos()
                    .await
                    .map_err(err)
            },
            async {
                let value = self
                    .recovery_central(&state, &r.configuration.account_key.wire())
                    .await?;
                Ok::<_, String>((value, now()))
            },
            protections,
        )?;
        let observed = now();
        let account_received =
            chrono::DateTime::parse_from_rfc3339(&fresh_snapshot.source_received_at)
                .map_err(|_| "STORAGE_RECOVERY_ACCOUNT_TIME_INVALID")?
                .timestamp_millis();
        recovery_fresh(observed, account_received)?;
        recovery_fresh(
            observed,
            i64::try_from(fresh_algos.received_at_ms)
                .map_err(|_| "STORAGE_RECOVERY_ALGO_TIME_INVALID")?,
        )?;
        recovery_fresh(observed, central_received)?;
        for (_, received) in &protections {
            recovery_fresh(observed, *received)?;
        }
        recovery_generation(
            generation,
            r.private_ws.status().await.connection_generation,
            r.source_identity_ready().await,
        )?;
        if after != central
            || !fresh_algos.complete_within_bound
            || fresh_snapshot.account_uid_fingerprint != r.execution.uid()
            || recovery_positions(&snapshot.positions)?
                != recovery_positions(&fresh_snapshot.positions)?
            || digest(&snapshot.pending_orders)? != digest(&fresh_snapshot.pending_orders)?
            || digest(&algos.samples)? != digest(&fresh_algos.samples)?
        {
            return Err("STORAGE_RECOVERY_PROOF_CHANGED".into());
        }
        r.execution
            .store
            .call(deadline(), move |s| {
                for source in ["account_snapshot", "orders"] {
                    s.ingest_page(&okx_execution::IngestPage {
                        cursor: Some(okx_execution::SourceCursor {
                            source: source.into(),
                            stream_partition: "account".into(),
                            cursor: String::new(),
                            watermark: account_received,
                            generation: generation as i64,
                            coverage_gaps: String::new(),
                        }),
                        ..Default::default()
                    })?;
                }
                Ok(())
            })
            .await
            .map_err(err)?;
        let proof_floor = protections
            .iter()
            .map(|(_, received)| *received)
            .chain([
                account_received,
                fresh_algos.received_at_ms as i64,
                central_received,
            ])
            .min()
            .ok_or("STORAGE_RECOVERY_SOURCE_MISSING")?;
        let local = r
            .execution
            .store
            .call(deadline(), |s| s.recovery_snapshot(now()))
            .await
            .map_err(err)?;
        recovery_matches(&local, &central)?;
        let exchange = digest(&(
            fresh_snapshot,
            fresh_algos,
            history_proofs,
            generation,
            protections,
        ))?;
        let evidence = digest(
            &json!({"central":central,"storage_gate_only":true,"post_requires_fresh_all_account_reserve":true,"local_cumulative_loss_usd":exact(local.cumulative_loss_usd)}),
        )?;
        r.execution
            .store
            .call(deadline(), move |s| {
                if s.cursor("account_snapshot", "account")?
                    .is_none_or(|c| c.generation != generation as i64)
                {
                    return Err(okx_execution::StoreError::Invalid(
                        "STORAGE_RECOVERY_GENERATION_CHANGED",
                    ));
                }
                commit_recovery(s, &local, &central, proof_floor, &exchange, &evidence)
            })
            .await
            .map_err(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn restored() -> okx_execution::AccountStore {
        let directory = std::env::temp_dir().join(format!(
            "okx-recovery-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let key = format!("okx:real:{}", "a".repeat(64));
        let meta = okx_execution::AccountMeta {
            initialize_operation_id: "initialize_recovery".into(),
            release_id: "release".into(),
            account_key: key.clone(),
            installation_id: "installation".into(),
            initialized_at: 1,
            opening_source: "authenticated".into(),
        };
        let opening = okx_execution::OpeningSnapshot {
            opening_equity_usd: Decimal::from(100),
            source_ref: "authenticated".into(),
            observed_at: 1,
            positions: 0,
            orders: 0,
            conditional_orders: 0,
            liabilities: Decimal::ZERO,
            balances: vec![("USDT".into(), Decimal::from(100))],
        };
        let source = okx_execution::AccountStore::initialize_flat(
            directory.join("source.db"),
            meta,
            &opening,
        )
        .unwrap();
        source.backup(directory.join("restored.db")).unwrap();
        let mut store = okx_execution::AccountStore::open_existing(
            directory.join("restored.db"),
            &key,
            "installation",
        )
        .unwrap();
        let at = now();
        for (source, part) in [
            ("fills", "SPOT"),
            ("fills", "SWAP"),
            ("fills", "FUTURES"),
            ("fills", "OPTION"),
            ("bills", "account"),
            ("orders", "account"),
            ("account_snapshot", "account"),
            ("recovery_audit", "account"),
        ] {
            store
                .ingest_page(&okx_execution::IngestPage {
                    cursor: Some(okx_execution::SourceCursor {
                        source: source.into(),
                        stream_partition: part.into(),
                        cursor: String::new(),
                        watermark: at,
                        generation: 1,
                        coverage_gaps: String::new(),
                    }),
                    ..Default::default()
                })
                .unwrap();
        }
        store
    }
    fn flat() -> RiskAccountBudget {
        RiskAccountBudget {
            account_key: format!("okx:real:{}", "a".repeat(64)),
            active_reservations: 0,
            occupied_usd: "0".into(),
            cumulative_loss_usd: "0".into(),
            latest_activity_ms: 0,
        }
    }
    #[test]
    fn restored_flat_clears() {
        let mut store = restored();
        let local = store.recovery_snapshot(now()).unwrap();
        assert!(local.required);
        commit_recovery(
            &mut store,
            &local,
            &flat(),
            now(),
            "fresh-exchange",
            "addressed-central",
        )
        .unwrap();
        assert!(!store.storage_recovery_required().unwrap());
    }
    #[test]
    fn unknown_central_never_clears() {
        let mut store = restored();
        let local = store.recovery_snapshot(now()).unwrap();
        let mut central = flat();
        central.active_reservations = 1;
        central.occupied_usd = "1".into();
        assert!(commit_recovery(&mut store, &local, &central, now(), "fresh", "central").is_err());
        assert!(store.storage_recovery_required().unwrap());
    }
    #[test]
    fn fresh_generation_required() {
        assert!(recovery_generation(2, 2, true).is_ok());
        assert!(recovery_generation(2, 3, true).is_err());
        assert!(recovery_generation(2, 2, false).is_err());
        assert!(recovery_generation(0, 0, true).is_err());
        let mut store = restored();
        store
            .ingest_page(&okx_execution::IngestPage {
                cursor: Some(okx_execution::SourceCursor {
                    source: "orders".into(),
                    stream_partition: "account".into(),
                    cursor: String::new(),
                    watermark: now(),
                    generation: 2,
                    coverage_gaps: String::new(),
                }),
                ..Default::default()
            })
            .unwrap();
        let local = store.recovery_snapshot(now()).unwrap();
        assert!(
            commit_recovery(
                &mut store,
                &local,
                &flat(),
                now(),
                "mixed-generation",
                "central"
            )
            .is_err()
        );
        assert!(store.storage_recovery_required().unwrap());
    }
    #[test]
    fn partial_source_never_clears() {
        let mut store = restored();
        store
            .ingest_page(&okx_execution::IngestPage {
                cursor: Some(okx_execution::SourceCursor {
                    source: "orders".into(),
                    stream_partition: "account".into(),
                    cursor: "more".into(),
                    watermark: now(),
                    generation: 1,
                    coverage_gaps: String::new(),
                }),
                ..Default::default()
            })
            .unwrap();
        let local = store.recovery_snapshot(now()).unwrap();
        assert!(commit_recovery(&mut store, &local, &flat(), now(), "fresh", "central").is_err());
        assert!(store.storage_recovery_required().unwrap());
    }
    #[test]
    fn newer_local_loss_recovers_without_resetting_central() {
        let mut store = restored();
        store
            .ingest_page_with_usd(
                &okx_execution::IngestPage {
                    finance: vec![okx_execution::FinanceEntry {
                        entry_id: "recovery-fee".into(),
                        economic_event_key: "recovery-trade-fee".into(),
                        component: "FEE".into(),
                        allocation_leg: "account".into(),
                        amount: -Decimal::ONE,
                        currency: "USD".into(),
                        instance_id: None,
                        source_ref: "venue-bill".into(),
                        time: now(),
                        reverses_entry_id: None,
                    }],
                    ..Default::default()
                },
                &okx_execution::UsdConversionEvidence {
                    observed_at: now(),
                    source_digest: "attested-usd".into(),
                    usd_per_currency: std::collections::BTreeMap::from([(
                        "USD".into(),
                        Decimal::ONE,
                    )]),
                },
            )
            .unwrap();
        let local = store.recovery_snapshot(now()).unwrap();
        assert_eq!(local.cumulative_loss_usd, Decimal::ONE);
        commit_recovery(
            &mut store,
            &local,
            &flat(),
            now(),
            "fresh-exchange",
            "central-with-earlier-loss",
        )
        .unwrap();
        assert!(!store.storage_recovery_required().unwrap());
        assert_eq!(store.policy().unwrap().4, Decimal::ONE);
        let mut central = flat();
        central.cumulative_loss_usd = "2".into();
        assert!(recovery_matches(&local, &central).is_err());
    }
    #[test]
    fn slow_historical_audit_accepts_fresh_final_receipts() {
        let audit_started = 1000;
        let observed = audit_started + 20_000;
        for receipt in [observed - 1000, observed - 4000, observed] {
            assert!(recovery_fresh(observed, receipt).is_ok());
        }
        assert!(recovery_fresh(observed, audit_started).is_err());
        assert!(recovery_fresh(observed, observed - 5001).is_err());
        assert!(recovery_fresh(observed, observed + 1).is_err());
        assert!(recovery_fresh(observed, 0).is_err());
        let mut store = restored();
        let local = store.recovery_snapshot(now()).unwrap();
        assert!(
            commit_recovery(
                &mut store,
                &local,
                &flat(),
                now() - 5001,
                "expired-final-receipt",
                "central"
            )
            .is_err()
        );
        assert!(store.storage_recovery_required().unwrap());
    }
    #[test]
    fn fresh_position_identity_ignores_market_mark_changes() {
        let p:okx_observation::AccountPositionState=serde_json::from_value(json!({"instrument_type":"SWAP","instrument_id":"BTC-USDT-SWAP","position":"1","position_side":"long","margin_mode":"isolated","average_price":"100","mark_price":"101"})).unwrap();
        let mut fresh = p.clone();
        fresh.mark_price = Some("102".into());
        fresh.unrealized_pnl = Some("2".into());
        assert_eq!(
            recovery_positions(std::slice::from_ref(&p)).unwrap(),
            recovery_positions(&[fresh.clone()]).unwrap()
        );
        fresh.position = "2".into();
        assert_ne!(
            recovery_positions(&[p]).unwrap(),
            recovery_positions(&[fresh]).unwrap()
        );
    }
}

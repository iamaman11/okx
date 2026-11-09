//! Terminal reservation settlement and release for a reconciled intent.
use super::reconcile::{verified_order_readback, verified_terminal_order_update_time};
use super::{AccountResources, StoredIntent, TradeService, deadline, digest, err, exact};
use okx_execution::IntentState;
use okx_protocol::risk_authority::{
    RiskCommandAction, RiskDecisionStatus, RiskReservationState, RiskResolution,
    TerminalRiskLedgerProof,
};
use rust_decimal::Decimal;

impl TradeService {
    pub(super) async fn release_terminal(
        &self,
        r: &AccountResources,
        i: &StoredIntent,
        order: Option<&okx_api::TradeOrderDetails>,
        venue_flat_verified: bool,
    ) -> Result<(), String> {
        if r.configuration.account_key.environment != okx_protocol::accounts::Environment::Real {
            return Ok(());
        }
        let state = self
            .governance
            .read()
            .await
            .clone()
            .ok_or("AUTHORITY_UNAVAILABLE")?;
        let id = i.intent_id.clone();
        let receipt = r
            .execution
            .store
            .call(deadline(), move |s| s.central_receipt(&id))
            .await
            .map_err(err)?;
        let Some(receipt) = receipt else {
            // Rejected PRE-RESERVE / centrally DENIED intents are handled by
            // their independent SQL witnesses before terminal reconciliation.
            // For any risk-increasing terminal intent with a possible POST,
            // absent local central receipt does not prove released capacity.
            return if i.risk_increase {
                Err("TERMINAL_CENTRAL_RESERVATION_UNVERIFIED".into())
            } else {
                Ok(())
            };
        };
        let local_reduction = receipt.reservation_key.starts_with("local:");
        let instrument = i.instrument_id.clone();
        let lots = r
            .execution
            .store
            .call(deadline(), move |s| s.active_lots(&instrument))
            .await
            .map_err(err)?;
        if lots.iter().any(|l| {
            (l.intent_id.as_deref() == Some(&i.intent_id) || local_reduction)
                && l.remaining_qty > Decimal::ZERO
        }) {
            return Err("INTENT_EXPOSURE_REMAINS".into());
        }
        let partition = if i.instrument_id.ends_with("-SWAP") {
            "SWAP"
        } else {
            "FUTURES"
        };
        let complete = r
            .execution
            .store
            .call(deadline(), move |s| s.cursor("fills", partition))
            .await
            .map_err(err)?
            .ok_or("FILL_COVERAGE_MISSING")?;
        let venue_updated_at = verified_terminal_order_update_time(i, order)?;
        if complete.generation <= 0
            || !complete.cursor.is_empty()
            || !complete.coverage_gaps.is_empty()
            || complete.watermark < venue_updated_at
        {
            return Err("FILL_COVERAGE_INCOMPLETE".into());
        }
        if local_reduction {
            let bills = r
                .execution
                .store
                .call(deadline(), |s| s.cursor("bills", "account"))
                .await
                .map_err(err)?
                .ok_or("BILLS_COVERAGE_MISSING")?;
            if bills.generation <= 0
                || !bills.cursor.is_empty()
                || !bills.coverage_gaps.is_empty()
                || bills.watermark < venue_updated_at
            {
                return Err("FINANCIAL_COVERAGE_INCOMPLETE".into());
            }
            // Reduce-only has NO central RiskBook grant. Its SQL receipt must
            // nevertheless be completed after its own terminal venue/fill/bill
            // readback. Leaving it TERMINAL_PENDING_CENTRAL causes the next
            // full cohort census to block the opening intent's legitimate
            // central RELEASE forever.
            if i.risk_increase || !venue_flat_verified {
                return Err("LOCAL_CLOSE_FLAT_POSITION_UNPROVEN".into());
            }
            let venue = order.ok_or("LOCAL_CLOSE_ORDER_READBACK_MISSING")?;
            if !matches!(venue.state.as_str(), "filled" | "canceled" | "mmp_canceled") {
                return Err("LOCAL_CLOSE_VENUE_NOT_TERMINAL".into());
            }
            let filled = verified_order_readback(i, venue)?;
            if filled <= Decimal::ZERO || filled != i.plan.quantity {
                return Err("LOCAL_CLOSE_FILL_QUANTITY_UNPROVEN".into());
            }
            let id = i.intent_id.clone();
            let order_id = venue.order_id.clone();
            r.execution
                .store
                .call(deadline(), move |s| {
                    s.acknowledge_local_terminal_close(&id, &order_id, filled, venue_updated_at)
                })
                .await
                .map_err(err)?;
            return Ok(());
        }
        let observed = self
            .risk
            .request(self.risk_command(
                &state,
                format!("inspect_{}", &i.client_order_id),
                RiskCommandAction::Inspect {
                    reservation_key: receipt.reservation_key.clone(),
                    plan_sha256: i.plan_hash.clone(),
                },
            ))
            .await
            .map_err(str::to_owned)?;
        if observed.reservation_state == Some(RiskReservationState::Released) {
            // A late bill may already be captured but not yet acknowledged
            // by central SETTLE_ENTRY. Never overwrite that pending marker
            // with the original terminal release acknowledgement.
            let id = i.intent_id.clone();
            if r.execution
                .store
                .call(deadline(), move |s| s.entry_late_pending(&id))
                .await
                .map_err(err)?
            {
                return Err("LATE_ENTRY_SETTLEMENT_PENDING".into());
            }
            let k = receipt.reservation_key.clone();
            let e = digest(&observed)?;
            r.execution
                .store
                .call(deadline(), move |s| s.acknowledge_central_release(&k, &e))
                .await
                .map_err(err)?;
            return Ok(());
        }
        let (through, loss) = r
            .execution
            .store
            .call(deadline(), |s| {
                Ok((
                    s.cursor("bills", "account")?
                        .ok_or(okx_execution::StoreError::Invalid("BILLS_COVERAGE_MISSING"))?,
                    s.policy()?.4,
                ))
            })
            .await
            .map_err(err)?;
        if through.generation <= 0
            || !through.cursor.is_empty()
            || !through.coverage_gaps.is_empty()
            || through.watermark < venue_updated_at
        {
            return Err("FINANCIAL_COVERAGE_INCOMPLETE".into());
        }
        // The account-wide fills and bills cursors can advance independently
        // because their native SQL page ingests are separate atomic writes.
        // A newer bill watermark is NOT itself proof that fills have advanced
        // equally far. Use their common proven frontier, exactly as the
        // incremental SETTLE_ENTRY path already does, without downgrading
        // either source's completeness or the venue uTime requirement.
        let financial_through = complete.watermark.min(through.watermark);
        if financial_through < venue_updated_at {
            return Err("FINANCIAL_COVERAGE_INCOMPLETE".into());
        }
        // A terminal POST may free entry capacity only after the independent
        // venue order and all immutable native fill/fee bills agree. The
        // SQL owner commits the exact final cumulative proof BEFORE any WSS
        // SETTLE_ENTRY. A lost central reply reuses that same receipt.
        let venue_order = order.ok_or("TERMINAL_ENTRY_VENUE_ORDER_UNAVAILABLE")?;
        if !matches!(
            venue_order.state.as_str(),
            "filled" | "canceled" | "mmp_canceled"
        ) {
            return Err("TERMINAL_ENTRY_VENUE_STATE_UNVERIFIED".into());
        }
        let filled = verified_order_readback(i, venue_order)?;
        // A truly canceled zero-fill order uses the same terminal USD
        // settlement/release path but requires separate SQL no-money proof.
        // Any missing stream, unmatched bill, or existing partial fill denies.
        if filled == Decimal::ZERO && i.state != IntentState::Cancelled {
            return Err("TERMINAL_ZERO_FILL_STATE_UNVERIFIED".into());
        }
        let id = i.intent_id.clone();
        let settlement = r
            .execution
            .store
            .call(deadline(), move |s| {
                s.bind_terminal_entry_settlement(&id, venue_updated_at, filled, financial_through)
            })
            .await
            .map_err(err)?;
        if settlement.reservation_key != receipt.reservation_key
            || settlement.plan_hash != i.plan_hash
            || settlement.venue_order_id != venue_order.order_id
        {
            return Err("TERMINAL_ENTRY_SETTLEMENT_IDENTITY_MISMATCH".into());
        }
        let settled = self
            .risk
            .request(self.risk_command(
                &state,
                format!("settle_{}", i.client_order_id),
                RiskCommandAction::SettleEntry {
                    reservation_key: settlement.reservation_key.clone(),
                    plan_sha256: settlement.plan_hash.clone(),
                    expected_risk_book_sequence:
                        observed.risk_book_sequence.ok_or("RISK_SEQUENCE_MISSING")?,
                    cumulative_entry_usd: exact(settlement.cumulative_entry_usd),
                    settlement_revision: settlement.settlement_revision,
                    source_digest: settlement.source_digest.clone(),
                },
            ))
            .await
            .map_err(str::to_owned)?;
        if settled.status != RiskDecisionStatus::Updated {
            return Err(settled
                .reason
                .unwrap_or("CENTRAL_ENTRY_SETTLEMENT_PENDING".into()));
        }
        let settlement_sequence = settled
            .risk_book_sequence
            .ok_or("ENTRY_SETTLEMENT_SEQUENCE_MISSING")?;
        // The central SETTLE_ENTRY is now confirmed, but SQL must retain
        // the terminal intent's pending reservation until this RELEASE is
        // durably acknowledged. Construct a fresh full-cohort census with
        // exactly this settled reservation scoped for terminal resolution.
        // Every other terminal-pending / unknown intent still denies release.
        let account_key = r.configuration.account_key.wire();
        let census = self
            .census_for_terminal_release(
                &state,
                &account_key,
                &i.intent_id,
                &receipt.reservation_key,
                &i.plan_hash,
            )
            .await?;
        let proof = TerminalRiskLedgerProof {
            account_uid: r.configuration.account_key.verified_uid.clone(),
            run_id: "continuous_engine".into(),
            intent_id: i.intent_id.clone(),
            state: match i.state {
                IntentState::Rejected => "REJECTED",
                IntentState::Cancelled => "CANCELED",
                _ => "FILLED",
            }
            .into(),
            record_digest: digest(&(i, order, loss, through))?,
            cumulative_loss_usd: exact(loss),
            remaining_exposure_usdt: "0".into(),
            // The previous SETTLE_ENTRY committed the entire observed
            // amount. Terminal RELEASE must not create another revision or
            // claim a new debit; Worker compares these exact immutable
            // SQL-backed fields to the existing settled RiskBook entry.
            cumulative_entry_usd: Some(exact(settlement.cumulative_entry_usd)),
            entry_settlement_revision: Some(settlement.settlement_revision),
            entry_settlement_digest: Some(settlement.source_digest.clone()),
        };
        let command = self.risk_command(
            &state,
            format!("release_{}", &i.client_order_id),
            RiskCommandAction::Resolve {
                reservation_key: receipt.reservation_key.clone(),
                plan_sha256: i.plan_hash.clone(),
                expected_risk_book_sequence: settlement_sequence,
                next_state: RiskResolution::Released,
                source_proof_sha256: digest(&proof)?,
                terminal_census: Some(census),
                terminal_ledger: Some(proof),
            },
        );
        let reservation = receipt.reservation_key.clone();
        let result = self.risk.request(command).await.map_err(str::to_owned)?;
        if result.status != RiskDecisionStatus::Updated {
            return Err(result.reason.unwrap_or("CENTRAL_RELEASE_DENIED".into()));
        }
        let e = digest(&result)?;
        r.execution
            .store
            .call(deadline(), move |s| {
                s.acknowledge_central_release(&reservation, &e)
            })
            .await
            .map_err(err)?;
        Ok(())
    }
}

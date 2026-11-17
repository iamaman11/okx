use super::census::*;
use super::*;
use okx_api::{
    ApiOrderSide, ApiOrderType, ApiPositionSide, ApiTradeMode, ApiTriggerPriceType,
    AttachedAlgoOrderRequest, PlaceOrderRequest,
};
/// Immutable provenance for checking the fresh post-HELD FX rate against the
/// exact native account that owns the central reservation.
// A legacy SQL intent stores its original RAW request hash but not the
// semantic DTO. Exact raw replay remains read-only. A new spelling cannot
// retroactively reinterpret a persisted legacy receipt as equivalent.
fn existing_request_hash_matches(stored: &str, semantic: &str, historical_raw: &str) -> bool {
    stored == semantic || stored == historical_raw
}

// A positive *persisted Worker* bound breach is already proof that the
// campaign cannot accept new entry risk. It can refuse a new command before
// the slower Native source-readiness check; it never authorizes entry, skips
// the Worker summary, or treats a missing/malformed balance as zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CampaignOpeningDecision {
    VerifyNativeReadiness,
    RefuseBoundBreach,
}

fn campaign_opening_bound_breach_decision(
    cumulative_breach_usd: &str,
) -> Result<CampaignOpeningDecision, String> {
    let breach = dec(cumulative_breach_usd)?;
    if breach < Decimal::ZERO {
        return Err("ENTRY_BUDGET_BOUND_BREACH_INVALID".into());
    }
    Ok(if breach > Decimal::ZERO {
        CampaignOpeningDecision::RefuseBoundBreach
    } else {
        CampaignOpeningDecision::VerifyNativeReadiness
    })
}

#[cfg(test)]
#[test]
fn t10_10_worker_bound_breach_precedes_native_readiness_only_for_refusal() {
    assert_eq!(
        campaign_opening_bound_breach_decision("0").unwrap(),
        CampaignOpeningDecision::VerifyNativeReadiness
    );
    assert_eq!(
        campaign_opening_bound_breach_decision("0.0001").unwrap(),
        CampaignOpeningDecision::RefuseBoundBreach
    );
    assert_eq!(
        campaign_opening_bound_breach_decision("4.0021").unwrap(),
        CampaignOpeningDecision::RefuseBoundBreach
    );
    assert_eq!(
        campaign_opening_bound_breach_decision("-0.0001").unwrap_err(),
        "ENTRY_BUDGET_BOUND_BREACH_INVALID"
    );
    assert!(campaign_opening_bound_breach_decision("unknown").is_err());
}

// A syntactically valid Worker response does not by itself grant authority
// for this intent. Bind the actual receipt to this reservation and policy.
fn risk_receipt_bound(
    receipt: &RiskResult,
    status: RiskDecisionStatus,
    reservation_key: &str,
    policy_revision: u64,
) -> bool {
    receipt.valid()
        && receipt.status == status
        && receipt.reservation_key.as_deref() == Some(reservation_key)
        && receipt.policy_revision == Some(policy_revision)
        && receipt
            .risk_book_sequence
            .is_some_and(|sequence| sequence > 0)
}

fn post_receipt_bound(
    receipt: &RiskResult,
    reservation_key: &str,
    policy_revision: u64,
    expected_sequence: u64,
) -> bool {
    risk_receipt_bound(
        receipt,
        RiskDecisionStatus::PostAuthorized,
        reservation_key,
        policy_revision,
    ) && receipt
        .risk_book_sequence
        .is_some_and(|sequence| sequence > expected_sequence)
}

// A venue reply after POST is authoritative only for the exact submitted
// client order. Malformed, empty or differently addressed replies remain
// UNKNOWN until independent venue readback; none is a proved rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlaceAckVerdict {
    Accepted,
    ExplicitRejected,
    Unknown,
}

fn place_ack_verdict(
    reply: &okx_api::TradeResponse<okx_api::OrderOperationAck>,
    client_order_id: &str,
) -> PlaceAckVerdict {
    // A single-order failure can use the all-failed envelope (code=1).
    // It is never acceptance, but an exact bound nonzero sCode can still
    // prove rejection. Arbitrary envelope errors remain unknown.
    if !matches!(reply.code.as_str(), "0" | "1") || reply.data.len() != 1 {
        return PlaceAckVerdict::Unknown;
    }
    let ack = &reply.data[0];
    if ack.client_order_id != client_order_id {
        return PlaceAckVerdict::Unknown;
    }
    if ack.accepted() {
        if !reply.top_level_success() || ack.order_id.is_empty() {
            PlaceAckVerdict::Unknown
        } else {
            PlaceAckVerdict::Accepted
        }
    } else if ack.status_code.is_empty() {
        PlaceAckVerdict::Unknown
    } else {
        PlaceAckVerdict::ExplicitRejected
    }
}

struct PostFxFence<'a> {
    owner: &'a AccountResources,
    conversion: &'a UsdConversion,
    order_type: &'a str,
}

/// One selected-account production source assessment, captured strictly after
/// the central HELD receipt. The same authenticated bytes serve the immutable
/// entry/margin assessment, the selected-account census and the final mode
/// check. CAS retry constructs another witness; no process-wide or stale cache.
pub(super) struct PostHeldSelected {
    pub(super) account_key: String,
    pub(super) snapshot: okx_observation::AccountSnapshot,
    pub(super) executor_config: okx_api::AccountConfig,
    pub(super) private_generation: u64,
    pub(super) started_after_held_ms: u64,
    pub(super) completed_after_held_ms: u64,
}

// A more recent SQL fill cannot be judged against an older signed
// venue-position observation. This is only SOURCE PENDING, never proof of
// attribution or permission to increase risk.
const POSITION_SOURCE_BEFORE_SQL_FILL: &str = "COHORT_POSITION_SOURCE_BEFORE_SQL_FILL";

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ManagedProtectionCensusError {
    SourceBeforeLedgerFill,
    PendingPeerPost,
    Denied(String),
}

// New risk must still fail the complete census. For an ALREADY owned
// position with its own independently checked live stop, a different
// SQL intent in POSTING/UNKNOWN is an unresolved outcome, not proof
// that this owned stop failed. Never turn that uncertainty into an
// unverified emergency reduction. The positive protection test and
// all other denial reasons remain unchanged.
pub(super) fn classify_managed_protection_census_error(
    reason: String,
) -> ManagedProtectionCensusError {
    match reason.as_str() {
        POSITION_SOURCE_BEFORE_SQL_FILL => ManagedProtectionCensusError::SourceBeforeLedgerFill,
        "COHORT_UNRESOLVED_POST_OUTCOME" => ManagedProtectionCensusError::PendingPeerPost,
        _ => ManagedProtectionCensusError::Denied(reason),
    }
}

#[cfg(test)]
#[test]
fn unresolved_peer_outcome_is_not_verified_protection_failure() {
    assert_eq!(
        classify_managed_protection_census_error("COHORT_UNRESOLVED_POST_OUTCOME".into()),
        ManagedProtectionCensusError::PendingPeerPost
    );
    // A direct missing stop, a fresh unowned position and an unrelated
    // conditional order are still denied, not accepted as a managed census.
    for code in [
        "PROTECTION_UNVERIFIED",
        "UNATTRIBUTED_EXCHANGE_POSITION",
        "UNATTRIBUTED_CONDITIONAL_ORDER",
    ] {
        assert_eq!(
            classify_managed_protection_census_error(code.into()),
            ManagedProtectionCensusError::Denied(code.into())
        );
    }
}

pub(super) fn position_mismatch_reason(
    venue_update_ms: Option<&str>,
    newest_sql_lot_opened_ms: Option<i64>,
) -> &'static str {
    let venue_time = venue_update_ms
        .and_then(|timestamp| timestamp.parse::<i64>().ok())
        .filter(|timestamp| *timestamp > 0);
    if venue_time
        .zip(newest_sql_lot_opened_ms)
        .is_some_and(|(venue, sql)| sql > venue)
    {
        POSITION_SOURCE_BEFORE_SQL_FILL
    } else {
        "UNATTRIBUTED_EXCHANGE_POSITION"
    }
}

#[cfg(test)]
#[test]
fn managed_census_stale_position_source_requires_fresh_event() {
    // Signed 0.30 position predates an independently committed 0.20 Add
    // fill. No emergency POST is justified by the old position snapshot.
    assert_eq!(
        position_mismatch_reason(Some("1000"), Some(2000)),
        POSITION_SOURCE_BEFORE_SQL_FILL
    );
    // An independently newer 0.70 foreign venue observation is a true
    // unowned-risk denial, not permission to wait for a matching SQL lot.
    assert_eq!(
        position_mismatch_reason(Some("3000"), Some(2000)),
        "UNATTRIBUTED_EXCHANGE_POSITION"
    );
    for missing in [None, Some(""), Some("0"), Some("invalid")] {
        assert_eq!(
            position_mismatch_reason(missing, Some(2000)),
            "UNATTRIBUTED_EXCHANGE_POSITION"
        );
    }
    assert_eq!(
        position_mismatch_reason(Some("1000"), None),
        "UNATTRIBUTED_EXCHANGE_POSITION"
    );
}

pub(super) fn verify_post_held_selected(
    witness: &PostHeldSelected,
    account_key: &str,
    verified_uid: &str,
    generation: u64,
    now_ms: i64,
) -> Result<(), String> {
    if witness.account_key != account_key
        || witness.snapshot.account_uid_fingerprint != verified_uid
        || okx_api::account_uid_fingerprint(&witness.executor_config.uid) != verified_uid
    {
        return Err("POST_HELD_ACCOUNT_IDENTITY_CHANGED".into());
    }
    if generation == 0 || witness.private_generation != generation {
        return Err("POST_HELD_SOURCE_GENERATION_CHANGED".into());
    }
    if witness.completed_after_held_ms < witness.started_after_held_ms {
        return Err("POST_HELD_CAUSAL_TIME_INVALID".into());
    }
    let received = chrono::DateTime::parse_from_rfc3339(&witness.snapshot.source_received_at)
        .map_err(|_| "POST_HELD_SOURCE_TIME_INVALID")?
        .timestamp_millis();
    if received > now_ms
        || !crate::config::evidence_fresh(received, now_ms, crate::config::ACCOUNT_MAX_AGE_MS)
    {
        return Err("POST_HELD_ACCOUNT_EVIDENCE_STALE".into());
    }
    if witness.executor_config.position_mode != witness.snapshot.position_mode
        || !matches!(
            witness.snapshot.position_mode.as_str(),
            "net_mode" | "long_short_mode"
        )
    {
        return Err("POST_ACCOUNT_POSITION_MODE_UNVERIFIED".into());
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn post_held_witness_rejects_identity_generation_mode_and_stale_sources() {
    use serde_json::json;
    let uid = "recorded-uid";
    let fingerprint = okx_api::account_uid_fingerprint(uid);
    let snapshot: okx_observation::AccountSnapshot = serde_json::from_value(json!({
        "schema": "okx.account-snapshot/v2",
        "source": "okx_private_rest_bootstrap",
        "source_received_at": Utc::now().to_rfc3339(),
        "account_generation": "test",
        "quality_reason": "test",
        "private_ws_connected": true,
        "account_level": "2",
        "position_mode": "net_mode",
        "account_type": "1",
        "account_uid_fingerprint": fingerprint,
        "api_key_permissions": ["read_only"],
        "balance": {
            "total_equity_usd": "100",
            "adjusted_equity_usd": null,
            "isolated_equity_usd": null,
            "initial_margin_requirement_usd": null,
            "maintenance_margin_requirement_usd": null,
            "margin_ratio": null,
            "notional_usd": null,
            "update_time_ms": null,
            "details": []
        },
        "positions": [],
        "pending_orders": []
    }))
    .unwrap();
    let config: okx_api::AccountConfig =
        serde_json::from_value(json!({"uid":uid,"posMode":"net_mode"})).unwrap();
    let mut proof = PostHeldSelected {
        account_key: "okx:real:test".into(),
        snapshot,
        executor_config: config,
        private_generation: 9,
        started_after_held_ms: 1,
        completed_after_held_ms: 2,
    };
    let check = |proof: &PostHeldSelected| {
        verify_post_held_selected(proof, "okx:real:test", &fingerprint, 9, now())
    };
    assert!(check(&proof).is_ok());
    assert_eq!(
        verify_post_held_selected(&proof, "okx:real:other", &fingerprint, 9, now()),
        Err("POST_HELD_ACCOUNT_IDENTITY_CHANGED".into())
    );
    assert_eq!(
        verify_post_held_selected(&proof, "okx:real:test", &fingerprint, 10, now()),
        Err("POST_HELD_SOURCE_GENERATION_CHANGED".into())
    );
    proof.executor_config.uid = "different".into();
    assert_eq!(
        check(&proof),
        Err("POST_HELD_ACCOUNT_IDENTITY_CHANGED".into())
    );
    proof.executor_config.uid = uid.into();
    proof.executor_config.position_mode = "long_short_mode".into();
    assert_eq!(
        check(&proof),
        Err("POST_ACCOUNT_POSITION_MODE_UNVERIFIED".into())
    );
    proof.executor_config.position_mode = "net_mode".into();
    proof.started_after_held_ms = 4;
    assert_eq!(check(&proof), Err("POST_HELD_CAUSAL_TIME_INVALID".into()));
    proof.started_after_held_ms = 1;
    proof.snapshot.source_received_at = (Utc::now() - chrono::Duration::minutes(10)).to_rfc3339();
    assert_eq!(
        check(&proof),
        Err("POST_HELD_ACCOUNT_EVIDENCE_STALE".into())
    );
}

impl TradeService {
    /// Native, completely classified USD-denominated spot opening spend.
    /// An opening buy consumes gross USD notional and its fee. Exit fees
    /// remain cashflow/risk costs, not new entry turnover.
    /// No other instrument, conversion, bill type or currency is guessed.
    /// Completeness and fill↔order↔bill identity must already be established
    /// by AccountHistoryApi's authenticated campaign witness.
    #[cfg(test)]
    fn historical_usd_spot_entry(
        history: &okx_api::CompleteCampaignAccountHistory,
    ) -> Result<Decimal, String> {
        use rust_decimal::RoundingStrategy;
        let orders = history
            .orders
            .iter()
            .map(|o| (o.order_id.as_str(), o))
            .collect::<std::collections::BTreeMap<_, _>>();
        let bills = history
            .bills
            .iter()
            .map(|b| (b.bill_id.as_str(), b))
            .collect::<std::collections::BTreeMap<_, _>>();
        if orders.len() != history.orders.len() || bills.len() != history.bills.len() {
            return Err("ENTRY_OPENING_DUPLICATE_VENUE_ID".into());
        }
        let mut observed_bills = std::collections::BTreeSet::new();
        let mut total = Decimal::ZERO;
        for fill in &history.fills {
            let order = orders
                .get(fill.order_id.as_str())
                .ok_or("ENTRY_OPENING_ORDER_UNPROVEN")?;
            let bill = bills
                .get(fill.bill_id.as_str())
                .ok_or("ENTRY_OPENING_BILL_UNPROVEN")?;
            if fill.instrument_type != "SPOT"
                || !fill.instrument_id.ends_with("-USD")
                || fill.fee_currency != "USD"
                || bill.ccy != "USD"
                || bill.bill_type != "2"
                || bill.instrument_id != fill.instrument_id
                || bill.order_id != fill.order_id
                || bill.trade_id != fill.trade_id
                || order.instrument_type != "SPOT"
                || order.instrument_id != fill.instrument_id
                || !matches!(fill.side.as_str(), "buy" | "sell")
                || order.side != fill.side
                || bill.fee != fill.fee
                || !observed_bills.insert(fill.bill_id.as_str())
            {
                return Err("ENTRY_OPENING_UNSUPPORTED_OR_UNCLASSIFIED_MONEY".into());
            }
            let qty = dec(&fill.fill_size)?;
            let price = dec(&fill.fill_price)?;
            let fee = dec(&fill.fee)?;
            if qty <= Decimal::ZERO || price <= Decimal::ZERO {
                return Err("ENTRY_OPENING_INVALID_FILL".into());
            }
            // Only a negative venue fee is a debit; a positive rebate is
            // neither an entry cost nor a refund of already consumed budget.
            // Sell proceeds never refund entry.
            let debit = if fill.side == "buy" {
                qty.checked_mul(price).ok_or("MONEY_OVERFLOW")?
            } else {
                Decimal::ZERO
            };
            let fee_debit = if fill.side == "buy" && fee < Decimal::ZERO {
                -fee
            } else {
                Decimal::ZERO
            };
            let debit = debit.checked_add(fee_debit).ok_or("MONEY_OVERFLOW")?;
            total = total.checked_add(debit).ok_or("MONEY_OVERFLOW")?;
        }
        if observed_bills.len() != history.bills.len() {
            return Err("ENTRY_OPENING_STANDALONE_BILL_UNCLASSIFIED".into());
        }
        Ok(total.round_dp_with_strategy(9, RoundingStrategy::ToPositiveInfinity))
    }

    /// Opening may be installed once for the entire authenticated cohort.
    /// Unknown venue events never imply zero. Non-USD historical instruments
    /// remain unsupported until a historical contract+FX source exists.
    /// No historical network pagination is permitted on the POST command
    /// path. Background capture owns source cursors; only the SQL owner can
    /// certify an absence of *unaccounted* campaign events in a fresh window.
    async fn ensure_campaign_opening(
        &self,
        state: &CentralGovernanceSnapshot,
        until: Instant,
    ) -> Result<CampaignOpeningDecision, String> {
        let summary = self
            .risk
            .request(self.risk_command(
                state,
                format!("opening_status_{}", now()),
                RiskCommandAction::Summary {},
            ))
            .await
            .map_err(str::to_owned)?;
        if summary.status != RiskDecisionStatus::Observed {
            return Err("ENTRY_OPENING_CENTRAL_SUMMARY_UNAVAILABLE".into());
        }
        let budget = summary
            .budget
            .ok_or("ENTRY_OPENING_CENTRAL_BUDGET_MISSING")?;
        let entry = budget
            .entry_budget
            .ok_or("BASELINE_RECONCILIATION_REQUIRED")?;
        let origin = budget
            .campaign_origin
            .ok_or("ENTRY_CAMPAIGN_ORIGIN_UNPROVEN")?;
        if origin.installation_id != self.accounts.installation_id()
            || origin.campaign_id != entry.campaign_id
            || origin.campaign_id != state.state.campaign_id
            || origin.started_at_ms > entry.opening_as_of_ms
        {
            return Err("ENTRY_CAMPAIGN_ORIGIN_MISMATCH".into());
        }
        // Validate provenance FIRST. This is a one-way fail-closed denial
        // from authenticated Worker state, not permission to bypass SQL
        // freshness on any campaign that can still admit new risk.
        if campaign_opening_bound_breach_decision(&entry.bound_breach_usd)?
            == CampaignOpeningDecision::RefuseBoundBreach
        {
            return Ok(CampaignOpeningDecision::RefuseBoundBreach);
        }
        let from =
            i64::try_from(origin.started_at_ms).map_err(|_| "ENTRY_OPENING_CUTOFF_INVALID")?;
        let campaign = origin.campaign_id;
        let through = now().saturating_sub(2_000);
        if through <= from {
            return Err("ENTRY_OPENING_CAPTURE_PENDING".into());
        }
        for account in
            state.state.accounts.iter().filter(|a| {
                a.environment == okx_protocol::account_governance::AccountEnvironment::Real
            })
        {
            if Instant::now() >= until {
                return Err("COMMAND_EXPIRED".into());
            }
            let resource = self.resources(&account.account_key).await?;
            if !resource.source_identity_ready().await {
                return Err("ENTRY_OPENING_IDENTITY_UNVERIFIED".into());
            }
            let campaign = campaign.clone();
            resource
                .execution
                .store
                .verify_campaign_entry_readiness(until, campaign, from, through)
                .await
                .map_err(err)?;
        }
        Ok(CampaignOpeningDecision::VerifyNativeReadiness)
    }

    /// The existing maintenance owner installs one complete cohort opening,
    /// including independently settled historical native entry USD. External
    /// or unsettled history remains pending, never inferred as zero.
    pub(super) async fn install_captured_empty_campaign_opening(
        &self,
        state: &CentralGovernanceSnapshot,
        until: Instant,
    ) -> Result<(), String> {
        let summary = self
            .risk
            .request(self.risk_command(
                state,
                format!("opening_summary_{}", now()),
                RiskCommandAction::Summary {},
            ))
            .await
            .map_err(str::to_owned)?;
        if summary.status != RiskDecisionStatus::Observed {
            return Err("ENTRY_OPENING_CENTRAL_SUMMARY_UNAVAILABLE".into());
        }
        let budget = summary
            .budget
            .ok_or("ENTRY_OPENING_CENTRAL_BUDGET_MISSING")?;
        if let Some(entry) = budget.entry_budget {
            let origin = budget
                .campaign_origin
                .ok_or("ENTRY_CAMPAIGN_ORIGIN_UNPROVEN")?;
            if origin.installation_id != self.accounts.installation_id()
                || origin.campaign_id != entry.campaign_id
                || origin.campaign_id != state.state.campaign_id
                || origin.started_at_ms > entry.opening_as_of_ms
            {
                return Err("ENTRY_CAMPAIGN_ORIGIN_MISMATCH".into());
            }
            let from =
                i64::try_from(origin.started_at_ms).map_err(|_| "ENTRY_CAMPAIGN_ORIGIN_INVALID")?;
            let through = now().saturating_sub(2_000);
            for account in state.state.accounts.iter().filter(|a| {
                a.environment == okx_protocol::account_governance::AccountEnvironment::Real
            }) {
                if Instant::now() >= until {
                    return Err("COMMAND_EXPIRED".into());
                }
                let resource = self.resources(&account.account_key).await?;
                if !resource.source_identity_ready().await {
                    return Err("ENTRY_OPENING_IDENTITY_UNVERIFIED".into());
                }
                let campaign = origin.campaign_id.clone();
                resource
                    .execution
                    .store
                    .refresh_campaign_entry_readiness(until, campaign, from, through)
                    .await
                    .map_err(err)?;
            }
            return Ok(());
        }
        let origin = budget
            .campaign_origin
            .ok_or("ENTRY_CAMPAIGN_ORIGIN_UNPROVEN")?;
        if origin.installation_id != self.accounts.installation_id()
            || origin.campaign_id != state.state.campaign_id
        {
            return Err("ENTRY_CAMPAIGN_ORIGIN_MISMATCH".into());
        }
        let from =
            i64::try_from(origin.started_at_ms).map_err(|_| "ENTRY_CAMPAIGN_ORIGIN_INVALID")?;
        let mut rows = Vec::new();
        let cohort = state
            .state
            .accounts
            .iter()
            .filter(|account| {
                account.environment == okx_protocol::account_governance::AccountEnvironment::Real
            })
            .collect::<Vec<_>>();
        if cohort.is_empty() || cohort.len() > 32 {
            return Err("ENTRY_OPENING_COHORT_INVALID".into());
        }
        if !self.accounts.failures().await.is_empty() {
            return Err("ENTRY_OPENING_COHORT_UNAVAILABLE".into());
        }
        let through = now().saturating_sub(2_000);
        for account in cohort {
            if Instant::now() >= until {
                return Err("COMMAND_EXPIRED".into());
            }
            let resource = self.resources(&account.account_key).await?;
            if !resource.source_identity_ready().await {
                return Err("ENTRY_OPENING_IDENTITY_UNVERIFIED".into());
            }
            let campaign = origin.campaign_id.clone();
            let (spent, source_digest) = resource
                .execution
                .store
                .seal_captured_campaign_baseline(until, campaign, from, through)
                .await
                .map_err(err)?;
            rows.push(CampaignOpeningAccount {
                account_key: account.account_key.clone(),
                history_from_ms: origin.started_at_ms,
                history_through_ms: u64::try_from(through)
                    .map_err(|_| "ENTRY_OPENING_TIME_INVALID")?,
                consumed_entry_usd: exact(spent),
                order_history_complete: true,
                fill_history_complete: true,
                bill_history_complete: true,
                finance_provenance_sealed: true,
                source_digest,
            });
        }
        rows.sort_by(|a, b| a.account_key.cmp(&b.account_key));
        let consumed = rows.iter().try_fold(Decimal::ZERO, |sum, account| {
            sum.checked_add(dec(&account.consumed_entry_usd)?)
                .ok_or_else(|| "MONEY_OVERFLOW".to_owned())
        })?;
        let limit_usd = state
            .state
            .campaign_entry_limit_usd
            .clone()
            .ok_or("ENTRY_CAMPAIGN_LIMIT_MISSING")?;
        let mut source = CampaignOpeningSource {
            campaign_id: origin.campaign_id,
            limit_usd,
            consumed_usd: exact(consumed),
            source_digest: String::new(),
            as_of_ms: u64::try_from(through).map_err(|_| "ENTRY_OPENING_TIME_INVALID")?,
            campaign_started_at_ms: origin.started_at_ms,
            accounts: rows,
        };
        source.source_digest = digest(&(
            "okx.campaign-entry-opening/v1",
            &source.campaign_id,
            &source.limit_usd,
            &source.consumed_usd,
            source.campaign_started_at_ms,
            source.as_of_ms,
            &source.accounts,
        ))?;
        if !source.valid() || Instant::now() >= until {
            return Err("ENTRY_OPENING_SOURCE_INVALID".into());
        }
        // Completed monetary source pages alone do not prove absence of
        // live pending orders, unprotected positions or UNKNOWN obligations.
        // The same authenticated whole-cohort census used for RESERVE must
        // pass before an opening can be installed.
        let packet = self.produce_opening(state, source.clone()).await?;
        let result = self
            .risk
            .install_produced_opening(packet)
            .await
            .map_err(str::to_owned)?;
        if result.status != RiskDecisionStatus::Observed
            || result
                .budget
                .as_ref()
                .and_then(|budget| budget.entry_budget.as_ref())
                .is_none_or(|opened| opened.opening_source_digest != source.source_digest)
        {
            return Err(result
                .reason
                .unwrap_or("ENTRY_OPENING_INSTALL_UNPROVEN".into()));
        }
        Ok(())
    }

    pub(super) async fn execute(
        &self,
        r: &Arc<AccountResources>,
        request: &TradeRequest,
        supplied: Option<&CentralGovernanceSnapshot>,
    ) -> Result<Value, String> {
        self.execute_bound(r, request, supplied, None).await
    }
    pub(super) async fn execute_bound(
        &self,
        r: &Arc<AccountResources>,
        request: &TradeRequest,
        supplied: Option<&CentralGovernanceSnapshot>,
        bound_hash: Option<String>,
    ) -> Result<Value, String> {
        self.execute_inner(r, request, supplied, bound_hash, None, None)
            .await
    }
    pub(super) async fn execute_for_definition(
        &self,
        r: &Arc<AccountResources>,
        request: &TradeRequest,
        supplied: Option<&CentralGovernanceSnapshot>,
        version: String,
        decision_digest: String,
        claimed_decision_id: String,
    ) -> Result<Value, String> {
        self.execute_inner(
            r,
            request,
            supplied,
            None,
            Some(version),
            Some((decision_digest, claimed_decision_id)),
        )
        .await
    }
    async fn execute_inner(
        &self,
        r: &Arc<AccountResources>,
        request: &TradeRequest,
        supplied: Option<&CentralGovernanceSnapshot>,
        bound_hash: Option<String>,
        expected_definition: Option<String>,
        claimed_decision: Option<(String, String)>,
    ) -> Result<Value, String> {
        let TradeRequest::Execute {
            idempotency_key,
            operation,
            strategy_instance,
            instrument,
            side,
            sizing,
            order,
            protection,
            expires_in_ms,
        } = request
        else {
            return Err("EXECUTE_REQUIRED".into());
        };
        let started = Instant::now();
        self.verify_runtime_finance_route(r)?;
        let until = started + Duration::from_millis(*expires_in_ms);
        // New risk still requires the exact per-account mutation semaphore.
        // Check an existing semantic identity from SQL before demanding
        // mutation capacity. However, status_intent is NOT read-only: it can
        // reconcile fills, risk and protective orders. If the permit is busy,
        // return an identity-bound UNKNOWN/no-resubmit instead of launching
        // overlapping financial reconciliation. This preserves the original
        // single-owner mutation fence under 100 same-key concurrent calls.
        let capacity = r.mutation_capacity.try_acquire().ok();
        let account_key = r.configuration.account_key.wire();
        // Keep the historical raw hash solely for read-only replay of
        // already persisted intents. Never rewrite their IDs, plans or hashes.
        // The old SQL record has only request_hash (not the complete original
        // semantic DTO); a different spelling of a legacy amount must NOT be
        // guessed equivalent or permitted to create another venue POST.
        let historical_raw_hash = digest(&(account_key.clone(), request))?;
        let body_hash = match bound_hash {
            Some(bound) => bound,
            None => {
                let preimage = request
                    .canonical_semantic_json(&account_key)
                    .map_err(|cause| cause.code().to_owned())?;
                format!("{:x}", Sha256::digest(preimage.as_bytes()))
            }
        };
        let key = idempotency_key.clone();
        let prior = r
            .execution
            .store
            .lookup_intent(until, okx_execution::TradeIntentLookup::IdempotencyKey(key))
            .await
            .map_err(err)?;
        if let Some(i) = prior {
            if !existing_request_hash_matches(&i.request_hash, &body_hash, &historical_raw_hash) {
                return Err("IDEMPOTENCY_CONFLICT".into());
            }
            // A concurrent mutation may be legitimately in progress. The
            // original durable intent remains the ONLY identity; do not
            // pretend a current exchange or financial state was read.
            if capacity.is_none() {
                return Ok(uncertain(
                    &i.intent_id,
                    "ACCOUNT_MUTATION_BUSY_STATUS_NOT_EVALUATED_NO_RESUBMIT",
                ));
            }
            drop(capacity);
            // A repeated Execute is not an instruction to run 100 concurrent
            // independent finance/venue Status reconciliations. Keep exactly
            // one replay readback in flight for this account, and return the
            // persisted intent identity without authorizing a re-POST to all
            // other callers. Explicit trade.status remains fully available
            // to verify partial fills, protection and terminal finance.
            let _replay_owner = match r.execute_replay_reconciliation.try_acquire() {
                Ok(owner) => owner,
                Err(_) => {
                    return Ok(uncertain(
                        &i.intent_id,
                        "ACCOUNT_MUTATION_BUSY_STATUS_NOT_EVALUATED_NO_RESUBMIT",
                    ));
                }
            };
            return self.status_intent(r, &i.intent_id, None).await;
        }
        // If this key is NEW, an unavailable semaphore remains a strict
        // no-send rejection. A concurrent task that created the intent
        // after our read cannot be double-posted: only the holder may
        // prepare/reserve/BEGIN_POST, and other calls must replay on retry.
        let _capacity = capacity.ok_or("ACCOUNT_MUTATION_BUSY")?;
        // 100 cold same-key Execute requests must not all evaluate separate
        // new-risk plans and contend on one native SQL/financial source.
        // A dedicated per-account *risk-increase* lane admits one planner;
        // safety cancel/close and other account owners never acquire it.
        // A loser is definitively local/no-send and can check the durable
        // identity on a later public Status/replay call.
        let _new_risk_admission = if matches!(
            operation,
            TradeOperation::Open | TradeOperation::Add | TradeOperation::Replace
        ) {
            Some(
                r.new_risk_admission
                    .try_acquire()
                    .map_err(|_| "ACCOUNT_MUTATION_BUSY")?,
            )
        } else {
            None
        };
        let (state, control) = self.authority(r, supplied).await?;
        let increase = matches!(
            operation,
            TradeOperation::Open | TradeOperation::Add | TradeOperation::Replace
        );
        // S2-05: only a NEW request reaches this point. A real Worker
        // policy pause must deny new risk before a potentially stale
        // campaign-source read; an accepted old key was already resolved
        // above without re-authorizing, re-reserving or re-sending it.
        // The existing plan also enforces this gate (defense in depth).
        if increase && !control.access_enabled() {
            return Err("ACCOUNT_NEW_RISK_PAUSED".into());
        }
        if increase
            && r.configuration.account_key.environment == okx_protocol::accounts::Environment::Real
            && self.ensure_campaign_opening(&state, until).await?
                == CampaignOpeningDecision::RefuseBoundBreach
        {
            // The authenticated Worker has already committed a positive
            // cumulative campaign overrun. There is no SQL PREPARE,
            // central RESERVE or venue POST for this new intent.
            // A stale Native capture cannot make that overrun safe.
            return Ok(completed(json!({
                "state":"REJECTED",
                "rejection_origin":"CENTRAL",
                "send_state":"NOT_STARTED",
                "primary_reason":"BUDGET_BOUND_BREACH",
                "risk_release_state":"NOT_RESERVED",
            })));
        }
        let plan = loop {
            match self
                .plan(
                    r,
                    &control,
                    *operation,
                    strategy_instance,
                    instrument,
                    *side,
                    sizing,
                    order,
                    protection.as_ref(),
                    until,
                    true,
                )
                .await
            {
                Ok(plan) => break plan,
                Err(reason)
                    if Instant::now() + Duration::from_millis(100) < until
                        && [
                            "ACCOUNT_SOURCE_GENERATION_UNVERIFIED",
                            "ACCOUNT_EVIDENCE_EXPIRED_DURING_PLAN",
                            "FINANCIAL_SOURCE_INCOMPLETE_OR_STALE",
                            "PRIVATE_SOURCE_GENERATION_CHANGED",
                            "PUBLIC_SOURCE_GENERATION_CHANGED",
                        ]
                        .iter()
                        .any(|code| reason.contains(code)) =>
                {
                    let remaining = until.saturating_duration_since(Instant::now());
                    let _ = tokio::time::timeout(
                        remaining,
                        crate::source_capture::capture_account_sources(
                            &r.observer,
                            &r.private_ws,
                            &r.execution,
                        ),
                    )
                    .await;
                }
                Err(reason) => return Err(reason),
            }
        };
        let id = format!("intent_{}", &body_hash[..32]);
        let client_id = body_hash[..32].to_owned();
        let decision = Decision {
            decision_id: claimed_decision
                .as_ref()
                .map(|(_, id)| id.clone())
                .unwrap_or_else(|| format!("decision_{}", &body_hash[..32])),
            instance_id: Some(strategy_instance.clone()),
            decision_at: now(),
            input_digest: match claimed_decision {
                Some((digest, _)) => digest,
                None => digest(&(body_hash.clone(), &plan))?,
            },
            policy_revision: state.state.revision as i64,
            code_version: crate::accounts::build_release().into(),
        };
        let intent = NewIntent {
            plan: plan.clone(),
            intent_id: id.clone(),
            idempotency_key: idempotency_key.clone(),
            request_hash: body_hash,
            client_order_id: client_id.clone(),
            deadline: now() + until.saturating_duration_since(Instant::now()).as_millis() as i64,
            plan_hash: digest(&plan)?,
            policy_revision: state.state.revision as i64,
            risk_increase: increase,
            instrument_id: instrument.clone(),
        };
        if expected_definition
            .as_ref()
            .is_some_and(|version| version != &intent.plan.strategy_definition_version)
        {
            return Err("STRATEGY_VERSION_CHANGED".into());
        }

        let prepared = {
            {
                let (owner, account) = r.execution.store.scope();

                owner
                    .prepare_admission(until, account.to_owned(), decision, intent)
                    .await
                    .map_err(err)?
            }
        };
        if Instant::now() >= until {
            return Err("COMMAND_EXPIRED".into());
        }
        let mut central = None;
        let mut post_held_selected = None;

        let native_send_claim;
        if increase
            && r.configuration.account_key.environment == okx_protocol::accounts::Environment::Real
        {
            // E1 staging: persist exactly what the live venue price, fee and
            // conversion sources can prove before requesting central risk.
            // The receipt says UNPROVEN; it cannot authorize the exchange.
            let rules = self
                .public_ws
                .instrument_rules(instrument)
                .await
                .ok_or("ENTRY_INSTRUMENT_RULES_UNAVAILABLE")?;
            // Native SQL PREPARED already exists. Transient LOCAL/NO-SEND
            // private-GET admission must wait inside this ORIGINAL Execute,
            // not escape as Err to a caller which might replay the mutation
            // under the same idempotency key. A sent GET, exchange error or
            // expired command still fails closed; no POST retry is introduced.
            let quote_snapshot = r.observer.snapshot_until(until).await.map_err(err)?;
            let fx = Self::usd_conversion(&quote_snapshot)?;
            let account = r
                .execution
                .account
                .config_wait(until.saturating_duration_since(Instant::now()))
                .await
                .map_err(err)?;
            let leverage = dec(&r
                .observer
                .configured_leverage_until(
                    instrument,
                    okx_api::MarginMode::Cross,
                    if plan.position_side == "long" {
                        "long"
                    } else {
                        "short"
                    },
                    until,
                )
                .await
                .map_err(err)?)?;
            // Read the independently signed fee LAST: the private snapshot,
            // account config and leverage can be locally rate-admitted for
            // seconds. Their wait must not age the fee before assessment.
            // The original caller deadline and exchange timestamp gate still
            // apply; a truly stale venue tariff continues to fail closed.
            let fees = r
                .observer
                .fee_schedule_until(&rules, until)
                .await
                .map_err(err)?;
            let (entry_cost_proof, rate) = entry_budget::assess_order(
                &plan,
                entry_budget::EntryAssessmentSources {
                    instrument,
                    order_type: &order.order_type,
                    rules: &rules,
                    fees: &fees,
                    account: &account,
                    snapshot: &quote_snapshot,
                    conversion: &fx,
                    leverage,
                    verified_uid: &r.configuration.account_key.verified_uid,
                    observed_at_ms: now(),
                },
            )?;
            let quote = entry_cost_proof.quote.clone();
            let ceiling = entry_budget::enforced_entry_ceiling(
                true,
                true,
                control
                    .limits
                    .max_entry_usd
                    .as_deref()
                    .map(dec)
                    .transpose()?,
                order.max_entry_usd.as_deref().map(dec).transpose()?,
            )?
            .ok_or("ENTRY_ACCOUNT_CAP_MISSING")?;
            if quote.total_usd > ceiling {
                return Err("ENTRY_QUOTE_EXCEEDS_ACCOUNT_CAP".into());
            }
            if Instant::now() >= until {
                return Err("COMMAND_EXPIRED".into());
            }
            let fee_received_at = chrono::DateTime::parse_from_rfc3339(&fees.source_received_at)
                .map_err(err)?
                .timestamp_millis();
            let entry_receipt = okx_execution::EntryQuoteEvidence {
                schema: "okx.entry-known-quote/v1".into(),
                account_key: r.configuration.account_key.wire(),
                installation_id: self.accounts.installation_id().into(),
                intent_id: prepared.intent_id.clone(),
                plan_hash: prepared.plan_hash.clone(),
                policy_revision: prepared.policy_revision,
                instrument_id: instrument.clone(),
                quantity: plan.quantity,
                limit_price: plan.limit_price,
                contract_value: plan.contract_value,
                fee_rate: rate,
                maker_rate: fees.maker_rate.clone(),
                taker_rate: fees.taker_rate.clone(),
                fee_level: fees.level.clone(),
                fee_exchange_at: fees
                    .exchange_timestamp_ms
                    .parse::<i64>()
                    .map_err(|_| "ENTRY_FEE_EXCHANGE_TIME_INVALID")?,
                usd_per_usdt: dec(&fx.usd_per_usdt)?,
                notional_usdt: quote.notional_usdt,
                fee_usdt: quote.fee_usdt,
                total_usdt: quote.total_usdt,
                total_usd: quote.total_usd,
                fee_generation: fees.fee_generation.clone(),
                reference_generation: rules.reference_generation.clone(),
                fee_received_at,
                conversion_digest: fx.source_digest.clone(),
                conversion_received_at: fx.observed_at_ms as i64,
                observed_at: now(),
            };
            // The source-backed assessment above is the only bridge from
            // authenticated venue/account sources to SQL and RiskBook.
            // No separate sizing/fee formula or unsupported dimension is
            // promoted into POST authority here.
            let verified_entry =
                entry_budget::reserve_entry_evidence(&entry_cost_proof, &entry_receipt, ceiling)?;
            let cost_components = entry_cost_proof.verified_fx_and_entry_fees()?;
            if verified_entry.is_some() != cost_components.is_some() {
                return Err("ENTRY_PROOF_COMPONENTS_UNVERIFIED".into());
            }
            // The native source attestation and its Worker RESERVE upper
            // must survive a crash together with the same immutable quote.
            // No admission operation receipt exists for an Unknown cost.
            let entry_admission = verified_entry
                .as_ref()
                .zip(cost_components)
                .map(|((upper, source_digest), (fx_upper, fixed_upper))| {
                    let serialized = serde_json::to_vec(&entry_receipt)
                        .map_err(|_| "ENTRY_ADMISSION_QUOTE_SERIALIZATION_FAILED")?;
                    Ok::<okx_execution::EntryAdmissionEvidence, String>(
                        okx_execution::EntryAdmissionEvidence {
                            schema: "okx.entry-admission/v2".into(),
                            account_key: entry_receipt.account_key.clone(),
                            installation_id: entry_receipt.installation_id.clone(),
                            intent_id: entry_receipt.intent_id.clone(),
                            plan_hash: entry_receipt.plan_hash.clone(),
                            quote_digest: format!("{:x}", Sha256::digest(serialized)),
                            admission_usd_per_usdt: fx_upper,
                            other_entry_costs_upper_usd: fixed_upper,
                            entry_upper_usd: *upper,
                            entry_proof_digest: source_digest.clone(),
                            observed_at: entry_receipt.observed_at,
                        },
                    )
                })
                .transpose()?;
            r.execution
                .store
                .bind_entry_evidence(until, entry_receipt, entry_admission)
                .await
                .map_err(err)?;
            // An absent all-in proof is a native decision, not a reason to
            // create a fictitious central RESERVE attempt and send a WSS
            // request with null money. Finish the SQL intent as unsent,
            // preserving the immutable known quote for trader inspection.
            if verified_entry.is_none() {
                let rejected = prepared.intent_id.clone();
                r.execution
                    .store
                    .reject_unsent(until, rejected, "ENTRY_ALL_IN_COST_UNPROVEN".into())
                    .await
                    .map_err(err)?;
                return Err("ENTRY_ALL_IN_COST_UNPROVEN".into());
            }
            // A census source can be refused by the REAL local REST
            // limiter after SQL PREPARE, but before ANY Worker RESERVE.
            // Leaving that intent PREPARED forces the public caller to
            // guess whether it can safely issue another request. Complete
            // the immutable SQL no-send terminal proof on the same owner:
            // no exchange POST, no central risk/release or fabricated zero
            // exposure. A failed local SQL commit stays UNKNOWN/no-resubmit.
            let census = match self.census_until(&state, until).await {
                Ok(census) => census,
                Err(reason) => {
                    let pre_reserve_id = prepared.intent_id.clone();
                    let rejected = pre_reserve_id.clone();
                    let denied_reason = reason.clone();
                    let rejection = async {
                        r.execution
                            .store
                            .reject_unsent(deadline(), rejected.clone(), denied_reason)
                            .await?;
                        if !r
                            .execution
                            .store
                            .unsent_before_reserve(deadline(), rejected)
                            .await?
                        {
                            return Err(okx_execution::StoreError::Invalid(
                                "PRE_RESERVE_SQL_NO_SEND_PROOF_MISSING",
                            ));
                        }
                        Ok(())
                    }
                    .await;
                    if rejection.is_err() {
                        return Ok(uncertain(
                            &pre_reserve_id,
                            "PRE_RESERVE_LOCAL_REJECTION_UNCONFIRMED_NO_RESUBMIT",
                        ));
                    }
                    return Ok(completed(json!({
                        "intent_id":pre_reserve_id,
                        "state":"REJECTED",
                        "rejection_origin":"LOCAL",
                        "send_state":"NOT_STARTED",
                        "primary_reason":reason,
                        "risk_release_state":"NOT_RESERVED",
                    })));
                }
            };
            let snapshot = r.observer.snapshot_until(until).await.map_err(err)?;
            let conversion = Self::usd_conversion(&snapshot)?;
            let usdt = (plan.planned_loss_usd / dec(&conversion.usd_per_usdt)?)
                .round_dp_with_strategy(9, RoundingStrategy::ToPositiveInfinity);
            let usd = (usdt * dec(&conversion.usd_per_usdt)?)
                .round_dp_with_strategy(9, RoundingStrategy::ToPositiveInfinity);
            let reserve = self.risk_command(
                &state,
                format!("reserve_{}", client_id),
                RiskCommandAction::Reserve {
                    venue: "okx".into(),
                    account_uid: r.configuration.account_key.verified_uid.clone(),
                    run_id: "continuous_engine".into(),
                    intent_id: id.clone(),
                    strategy_id: strategy_instance.clone(),
                    instrument_id: instrument.clone(),
                    plan_sha256: prepared.plan_hash.clone(),
                    requested_loss_usdt: exact(usdt),
                    requested_loss_usd: exact(usd),
                    // Only the exact SQL-quote-bound ALL-IN proof may
                    // authorize the independent entry-spend budget. An
                    // Unknown dimension never substitutes zero or a quote.
                    entry_upper_usd: verified_entry.as_ref().map(|(upper, _)| exact(*upper)),
                    entry_proof_digest: verified_entry
                        .as_ref()
                        .map(|(_, proof_digest)| proof_digest.clone()),
                    usd_conversion: conversion.clone(),
                    policy_revision: state.state.revision,
                    census,
                },
            );
            let key = self.risk_reservation_key(r, &prepared);
            let intent_id = prepared.intent_id.clone();
            let reserve_key = key.clone();
            r.execution
                .store
                .mark_reserve_attempt(until, intent_id, reserve_key)
                .await
                .map_err(err)?;

            let reserved = {
                let packet = ProducedReserve::from_production(
                    reserve,
                    native_process_incarnation()?.to_owned(),
                    plan.planned_loss_usd,
                )?;
                self.risk
                    .reserve_produced(packet)
                    .await
                    .map_err(|reason| (reason.to_owned(), false))
                    .and_then(|(result, receipt)| {
                        if result.status == RiskDecisionStatus::Held {
                            Ok((result, receipt))
                        } else {
                            let denied = result.status == RiskDecisionStatus::Denied;
                            Err((
                                result.reason.unwrap_or_else(|| {
                                    "NATIVE_RESERVE_DENIED_OUTCOME_REQUIRES_READBACK".into()
                                }),
                                denied,
                            ))
                        }
                    })
            };
            let (held, native_receipt) = match reserved {
                Ok(held) => held,
                Err((reason, true)) => {
                    // This is the actual local owner's explicit Denied reply,
                    // not a timeout or lost reply. Its scoped transaction also
                    // refuses rejection if any original reservation exists.

                    let (owner, account) = r.execution.store.scope();
                    if owner
                        .reject_denied_reserve(
                            deadline(),
                            account.to_owned(),
                            prepared.intent_id.clone(),
                            reason.clone(),
                        )
                        .await
                        .is_err()
                    {
                        return Ok(uncertain(
                            &prepared.intent_id,
                            "NATIVE_RESERVE_DENIAL_COMMIT_UNCONFIRMED_NO_RESUBMIT",
                        ));
                    }
                    return Ok(completed(json!({
                        "intent_id":prepared.intent_id,"state":"REJECTED",
                        "rejection_origin":"LOCAL","send_state":"NOT_STARTED",
                        "primary_reason":reason,"risk_release_state":"NOT_RESERVED"
                    })));
                }
                Err((reason, _)) => {
                    // An enqueued native transaction may have committed before
                    // its reply was lost. Never reject/release it through the
                    // legacy account writer or obtain another send grant.
                    return Ok(uncertain(&prepared.intent_id, &reason));
                }
            };
            let held_received = Instant::now();
            let receipt = native_receipt.ok_or("NATIVE_RESERVE_DURABLE_RECEIPT_MISSING")?;
            if receipt.intent_id != prepared.intent_id
                || receipt.plan_hash != prepared.plan_hash
                || receipt.account_key != r.configuration.account_key.wire()
            {
                return Err("NATIVE_RESERVE_DURABLE_RECEIPT_MISMATCH".into());
            }
            if Instant::now() >= until {
                return Err("COMMAND_EXPIRED".into());
            }
            match self
                .fence_inspect_retry(
                    &state,
                    &held,
                    &prepared,
                    until,
                    held_received,
                    PostFxFence {
                        owner: r,
                        conversion: &conversion,
                        order_type: &order.order_type,
                    },
                )
                .await
            {
                Ok((v, witness, claim)) => {
                    central = Some(v);
                    post_held_selected = Some(witness);
                    native_send_claim = claim;
                }
                Err(reason) => {
                    // The sole local owner can prove no BeginPost and atomically
                    // retire the original HELD obligation. A queued/committed
                    // BeginPost wins the same writer order and makes rejection
                    // fail; lost replies must then remain UNKNOWN, never resend.
                    {
                        {
                            let (owner, account) = r.execution.store.scope();

                            let resolution = owner
                                .reject_and_release_unsent(
                                    deadline(),
                                    account.into(),
                                    prepared.intent_id.clone(),
                                    prepared.plan_hash.clone(),
                                    reason.clone(),
                                )
                                .await;
                            if resolution.is_ok() {
                                return Ok(completed(json!({
                                    "intent_id":prepared.intent_id,"state":"REJECTED",
                                    "rejection_origin":"LOCAL","send_state":"NOT_STARTED",
                                    "primary_reason":reason,"risk_release_state":"RELEASED"
                                })));
                            }
                            let mut result = uncertain(&prepared.intent_id, &reason);
                            result["unsent_resolution"] = json!({
                                "state":"NOT_PROVEN",
                                "reason_code":err(resolution.unwrap_err()),
                                "send_permission":"NOT_GRANTED"
                            });
                            return Ok(result);
                        }
                    }
                }
            }
        } else {
            // Demo and native-confirmed reductions use the same durable marker; no Real global capacity is minted.
            let receipt = CentralReceipt {
                reservation_key: format!("local:{}", id),
                account_key: r.configuration.account_key.wire(),
                installation_id: self.accounts.installation_id().into(),
                instance_id: Some(strategy_instance.clone()),
                intent_id: id.clone(),
                plan_hash: prepared.plan_hash.clone(),
                policy_revision: state.state.revision as i64,
                receipt_hash: digest(&prepared)?,
                requested_loss_usd: plan.planned_loss_usd,
                conversion_digest: digest(&plan)?,
            };
            {
                {
                    let (owner, account) = r.execution.store.scope();

                    owner
                        .reserve_local_reduction(until, account.to_owned(), receipt)
                        .await
                        .map_err(err)?;
                    native_send_claim = owner
                        .claim_local_reduction_send(
                            until,
                            account.to_owned(),
                            prepared.intent_id.clone(),
                            prepared.plan_hash.clone(),
                            prepared.policy_revision,
                        )
                        .await
                        .map_err(err)?;
                    if native_send_claim.is_none() {
                        return Ok(uncertain(
                            &prepared.intent_id,
                            "NATIVE_REDUCTION_POST_ALREADY_CLAIMED_NO_RESEND",
                        ));
                    }
                }
            }
        }
        let revision = self
            .governance
            .read()
            .await
            .as_ref()
            .ok_or("AUTHORITY_UNAVAILABLE")?
            .state
            .revision;
        if revision != state.state.revision {
            return Err("POLICY_CHANGED".into());
        }
        let market = self
            .public_ws
            .risk_fresh_snapshot(
                instrument,
                now() as u64,
                crate::config::QUOTE_MAX_AGE_MS,
                crate::config::DERIVATIVE_CONTEXT_MAX_AGE_MS,
                Utc::now().to_rfc3339(),
            )
            .await
            .map_err(err)?;
        validate_generation(plan.source_generation, market.generation as i64)?;
        if increase
            && (!r.source_identity_ready().await
                || r.private_ws.status().await.connection_generation as i64
                    != plan.private_source_generation)
        {
            return Err("ACCOUNT_SOURCE_GENERATION_UNVERIFIED".into());
        }
        if Instant::now() >= until {
            return Err("COMMAND_EXPIRED".into());
        }
        let clock = r.execution.clock.clock_evidence().await.map_err(err)?;
        let timing = clock
            .mutation_timing(
                until
                    .saturating_duration_since(Instant::now())
                    .as_millis()
                    .min(crate::config::ACCOUNT_MAX_AGE_MS as u128) as u64,
            )
            .map_err(err)?;
        let attached = plan
            .stop_price
            .map(|stop| AttachedAlgoOrderRequest {
                client_order_id: format!("s{}", &client_id[..31]),
                take_profit_trigger_price: String::new(),
                take_profit_trigger_price_type: ApiTriggerPriceType::Mark,
                take_profit_order_price: String::new(),
                stop_loss_trigger_price: exact(stop),
                stop_loss_trigger_price_type: ApiTriggerPriceType::Mark,
                stop_loss_order_price: "-1".into(),
            })
            .into_iter()
            .collect();
        // Real risk increases already obtained fresh Executor config and
        // observer snapshot after HELD. Reusing exactly the selected witness
        // removes two redundant private GET sequences; no pre-HELD snapshot
        // or cache may cross the distributed reservation boundary.
        let (post_account, post_snapshot) = if increase
            && r.configuration.account_key.environment == okx_protocol::accounts::Environment::Real
        {
            let witness = post_held_selected
                .as_ref()
                .ok_or("POST_HELD_SELECTED_WITNESS_MISSING")?;
            verify_post_held_selected(
                witness,
                &r.configuration.account_key.wire(),
                &r.configuration.account_key.verified_uid,
                r.private_ws.status().await.connection_generation,
                now(),
            )?;
            if Instant::now() >= until {
                return Err("COMMAND_EXPIRED_BEFORE_SEND".into());
            }
            (witness.executor_config.clone(), witness.snapshot.clone())
        } else {
            // Reduction has no central HELD and remains independently
            // fail-closed on fresh Executor and Observer signed responses.
            let config = r
                .execution
                .account
                .config_wait(until.saturating_duration_since(Instant::now()))
                .await
                .map_err(err)?;
            let snapshot = r.observer.snapshot_until(until).await.map_err(err)?;
            (config, snapshot)
        };
        if post_account.position_mode != post_snapshot.position_mode
            || post_snapshot.account_uid_fingerprint != r.configuration.account_key.verified_uid
            || !matches!(
                post_snapshot.position_mode.as_str(),
                "net_mode" | "long_short_mode"
            )
        {
            return Err("POST_ACCOUNT_POSITION_MODE_UNVERIFIED".into());
        }
        let net_mode = post_snapshot.position_mode == "net_mode";
        let venue = PlaceOrderRequest {
            instrument_id: instrument.clone(),
            trade_mode: ApiTradeMode::Cross,
            client_order_id: client_id,
            side: if plan.side == "buy" {
                ApiOrderSide::Buy
            } else {
                ApiOrderSide::Sell
            },
            position_side: if net_mode {
                ApiPositionSide::Net
            } else if *side == PositionSide::Long {
                ApiPositionSide::Long
            } else {
                ApiPositionSide::Short
            },
            reduce_only: if net_mode && !increase {
                Some(true)
            } else {
                None
            },
            order_type: match order.order_type.as_str() {
                "limit" => ApiOrderType::Limit,
                "post_only" => ApiOrderType::PostOnly,
                "ioc" | "market" => ApiOrderType::Ioc,
                _ => ApiOrderType::Fok,
            },
            size: exact(plan.quantity),
            price: exact(plan.limit_price),
            attached_algo_orders: if increase { attached } else { Vec::new() },
        };
        let rate = r.execution.trade.admit_place_order(&venue).map_err(err)?;
        let send = || async {
            if self
                .governance
                .read()
                .await
                .as_ref()
                .is_none_or(|v| v.state.revision != prepared.policy_revision as u64)
            {
                return Err("POLICY_CHANGED_BEFORE_SEND".into());
            }
            if Instant::now() >= until {
                return Err("COMMAND_EXPIRED_BEFORE_SEND".into());
            }
            r.execution
                .trade
                .place_order_after_admission(&venue, &timing, &rate)
                .await
                .map_err(err)
        };
        let result = if let Some(claim) = native_send_claim {
            let owner = self.risk.finance_owner();
            submission::post_committed_native(
                &owner,
                self.accounts.installation_id(),
                &r.configuration.account_key.wire(),
                &prepared,
                claim,
                until,
                send,
            )
            .await?
        } else {
            {
                return Err("NATIVE_SEND_CLAIM_MISSING_NO_POST".into());
            }
        };

        drop(_new_risk_admission);
        drop(_capacity);
        match result {
            Some(response) => match place_ack_verdict(&response, &venue.client_order_id) {
                PlaceAckVerdict::Accepted => {
                    let order_id = response.data[0].order_id.clone();
                    self.record_execute_outcome(
                        r,
                        okx_execution::IntentOutcome {
                            intent_id: id.clone(),
                            state: IntentState::Acknowledged,
                            exchange_order_id: Some(order_id),
                            evidence: "exchange-ack".into(),
                            observed_at: now(),
                        },
                    )
                    .await?;
                    self.status_intent(r, &id, None).await
                }
                PlaceAckVerdict::ExplicitRejected => {
                    self.record_execute_outcome(
                        r,
                        okx_execution::IntentOutcome {
                            intent_id: id.clone(),
                            state: IntentState::Rejected,
                            exchange_order_id: None,
                            evidence: "exchange-explicit-rejection".into(),
                            observed_at: now(),
                        },
                    )
                    .await?;
                    Ok(completed(
                        // code=0 describes the envelope, not this rejected
                        // order. Preserve its exact bound sCode for callers.
                        json!({"intent_id":id,"state":"REJECTED","exchange_code":response.data[0].status_code,"central_reservation":central}),
                    ))
                }
                PlaceAckVerdict::Unknown => {
                    self.record_execute_outcome(
                        r,
                        okx_execution::IntentOutcome {
                            intent_id: id.clone(),
                            state: IntentState::Unknown,
                            exchange_order_id: None,
                            evidence: "exchange-ack-unverified".into(),
                            observed_at: now(),
                        },
                    )
                    .await?;
                    Ok(uncertain(&id, "EXCHANGE_ACK_UNVERIFIED_NO_REPOST"))
                }
            },
            None => Ok(uncertain(&id, "EXCHANGE_POST_OUTCOME_UNKNOWN")),
        }
    }
    async fn record_execute_outcome(
        &self,
        resource: &AccountResources,
        outcome: okx_execution::IntentOutcome,
    ) -> Result<(), String> {
        // The required route check already binds account and RiskBook owner
        // to the same installation, without a second optional owner probe.
        self.verify_runtime_finance_route(resource)?;
        resource
            .execution
            .store
            .record_intent_outcome(deadline(), outcome)
            .await
            .map_err(err)
    }
    pub(super) fn risk_command(
        &self,
        state: &CentralGovernanceSnapshot,
        id: String,
        action: RiskCommandAction,
    ) -> RiskCommand {
        build_risk_command(state, id, action)
    }

    fn risk_reservation_key(&self, r: &AccountResources, intent: &StoredIntent) -> String {
        [
            "okx",
            "real",
            r.configuration.account_key.verified_uid.as_str(),
            "continuous_engine",
            intent.intent_id.as_str(),
        ]
        .iter()
        .map(|x| format!("{}:{}", x.len(), x))
        .collect::<String>()
    }

    async fn revalidate_held_entry_quote(
        &self,
        r: &AccountResources,
        intent: &StoredIntent,
        order_type: &str,
        until: Instant,
        held_received: Instant,
    ) -> Result<PostHeldSelected, String> {
        // Begin before the first request, never synthesize an after-HELD
        // timestamp from the completion of an earlier cached observation.
        let started_after_held_ms = held_received.elapsed().as_millis() as u64;
        let generation = r.private_ws.status().await.connection_generation;
        if generation == 0 || !r.source_identity_ready().await {
            return Err("POST_HELD_SOURCE_GENERATION_UNVERIFIED".into());
        }
        let source = r
            .execution
            .store
            .reserve_source(until, intent.intent_id.clone())
            .await
            .map_err(err)?;
        let quote = source.quote;
        let admission = source.admission;
        let rules = self
            .public_ws
            .ensure_fresh_instrument_rules(&intent.instrument_id, until)
            .await
            .map_err(err)?;
        plan::verify_entry_rules_match(&rules, &intent.instrument_id, &intent.plan)?;
        let ct = dec(rules
            .instrument
            .contract_value
            .as_deref()
            .ok_or("POST_ENTRY_CONTRACT_VALUE_MISSING")?)?;
        // Snapshot first: its forced authenticated config read populates
        // the same per-account observer cache for the immediately following
        // fee source. Otherwise fee_schedule_until reads config, then the
        // required fresh snapshot invalidates it and reads config again.
        let snap = r
            .observer
            .snapshot_after_held_until(until)
            .await
            .map_err(err)?;
        let fee = r
            .observer
            .fee_schedule_until(&rules, until)
            .await
            .map_err(err)?;
        if snap.account_uid_fingerprint != r.configuration.account_key.verified_uid {
            return Err("POST_ENTRY_ACCOUNT_CHANGED".into());
        }
        let fx = Self::usd_conversion(&snap)?;
        verify_post_held_entry_sources(&quote, &fee, &rules.reference_generation, ct, &fx, now())?;
        // Do not rely on the PRE-RESERVE account funds, borrowing or mode:
        // they can change while the distributed reservation is HELD.
        // Refuse the only possible POST unless the SAME production assessment
        // still proves the ORIGINAL immutable USD upper and margin capacity.
        let config = r
            .execution
            .account
            .config_wait(until.saturating_duration_since(Instant::now()))
            .await
            .map_err(err)?;
        let leverage = dec(&r
            .observer
            .configured_leverage_until(
                &intent.instrument_id,
                okx_api::MarginMode::Cross,
                if intent.plan.position_side == "long" {
                    "long"
                } else {
                    "short"
                },
                until,
            )
            .await
            .map_err(err)?)?;
        let (reassessed, _) = entry_budget::assess_order(
            &intent.plan,
            entry_budget::EntryAssessmentSources {
                instrument: &intent.instrument_id,
                order_type,
                rules: &rules,
                fees: &fee,
                account: &config,
                snapshot: &snap,
                conversion: &fx,
                leverage,
                verified_uid: &r.configuration.account_key.verified_uid,
                observed_at_ms: now(),
            },
        )?;
        reassessed.verify_held_entry_admission(&quote, &admission)?;
        if Instant::now() >= until {
            return Err("COMMAND_EXPIRED".into());
        }
        let witness = PostHeldSelected {
            account_key: r.configuration.account_key.wire(),
            snapshot: snap,
            executor_config: config,
            private_generation: generation,
            started_after_held_ms,
            completed_after_held_ms: held_received.elapsed().as_millis() as u64,
        };
        verify_post_held_selected(
            &witness,
            &r.configuration.account_key.wire(),
            &r.configuration.account_key.verified_uid,
            r.private_ws.status().await.connection_generation,
            now(),
        )?;
        Ok(witness)
    }

    async fn fence_inspect_retry(
        &self,
        state: &CentralGovernanceSnapshot,
        held: &RiskResult,
        intent: &StoredIntent,
        until: Instant,
        held_received: Instant,
        fx: PostFxFence<'_>,
    ) -> Result<
        (
            RiskResult,
            PostHeldSelected,
            Option<okx_execution::CommittedVenueSend>,
        ),
        String,
    > {
        let key = held
            .reservation_key
            .clone()
            .ok_or("RESERVATION_KEY_MISSING")?;
        let mut sequence = held
            .risk_book_sequence
            .ok_or("RESERVATION_SEQUENCE_MISSING")?;
        // One original attempt plus at most two bounded CAS retries.
        // Every attempt shares the original after-HELD five-second window.
        for _ in 0..3 {
            if Instant::now() >= until {
                return Err("COMMAND_EXPIRED".into());
            }
            // A fresh census and a valid FX fence do not revalidate tariff or
            // contract mechanics. Bind the same immutable SQL quote against
            // actual post-HELD source data on EACH CAS attempt before POST.
            let selected = self
                .revalidate_held_entry_quote(fx.owner, intent, fx.order_type, until, held_received)
                .await?;
            let (census, causal_proof) = self
                .census_after_held(
                    state,
                    held_received,
                    until,
                    &key,
                    &fx.conversion.usd_per_usdt,
                    &selected,
                )
                .await?;
            let command = self.risk_command(
                state,
                format!("fence_{}", intent.client_order_id),
                RiskCommandAction::BeginPost {
                    reservation_key: key.clone(),
                    plan_sha256: intent.plan_hash.clone(),
                    policy_revision: state.state.revision,
                    expected_risk_book_sequence: sequence,
                    source_proof_sha256: digest(&census)?,
                    census,
                    causal_proof,
                },
            );

            let (response, claim) = {
                let packet = ProducedPost::from_production(command)?;
                let (result, claim) = self
                    .risk
                    .post_produced(packet)
                    .await
                    .map_err(str::to_owned)?;
                (Ok::<RiskResult, &'static str>(result), claim)
            };
            match response {
                Ok(v) if v.status == RiskDecisionStatus::PostAuthorized => {
                    if !post_receipt_bound(&v, &key, state.state.revision, sequence) {
                        return Err("POST_FENCE_RECEIPT_BINDING_MISMATCH".into());
                    }

                    if claim.is_none() {
                        return Err("NATIVE_POST_CLAIM_MISSING_NO_POST".into());
                    }
                    return Ok((v, selected, claim));
                }
                Ok(v) if v.reason.as_deref() != Some("STALE_RISK_BOOK_SEQUENCE") => {
                    return Err(v.reason.unwrap_or("CENTRAL_POST_FENCE_DENIED".into()));
                }
                _ => {
                    let inspected = self
                        .risk
                        .request(self.risk_command(
                            state,
                            format!("inspect_fence_{}", intent.client_order_id),
                            RiskCommandAction::Inspect {
                                reservation_key: key.clone(),
                                plan_sha256: intent.plan_hash.clone(),
                            },
                        ))
                        .await
                        .map_err(str::to_owned)?;
                    if !risk_receipt_bound(
                        &inspected,
                        RiskDecisionStatus::Observed,
                        &key,
                        state.state.revision,
                    ) || inspected.reservation_state != Some(RiskReservationState::Held)
                    {
                        return Err("POST_FENCE_OUTCOME_UNKNOWN_NO_POST".into());
                    }
                    sequence = inspected
                        .risk_book_sequence
                        .ok_or("RESERVATION_SEQUENCE_MISSING")?;
                }
            }
        }
        Err("CENTRAL_RISK_CAS_BUSY_NO_POST".into())
    }
}
pub(super) fn native_process_incarnation() -> Result<&'static str, String> {
    static INSTANCE: std::sync::OnceLock<Result<String, String>> = std::sync::OnceLock::new();
    INSTANCE
        .get_or_init(|| {
            let mut bytes = [0u8; 16];
            getrandom::fill(&mut bytes).map_err(err)?;
            Ok(bytes.iter().map(|b| format!("{b:02x}")).collect::<String>())
        })
        .as_ref()
        .map(String::as_str)
        .map_err(Clone::clone)
}
fn validate_generation(expected: i64, observed: i64) -> Result<(), String> {
    if expected <= 0 || expected != observed {
        Err("SOURCE_GENERATION_CHANGED".into())
    } else {
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn central_binding_fixture(status: RiskDecisionStatus) -> RiskResult {
        RiskResult {
            schema: RISK_RESULT_SCHEMA_V1.into(),
            command_id: "risk_receipt_fixture".into(),
            status,
            reason: None,
            reservation_key: Some("reservation_expected".into()),
            risk_book_sequence: Some(7),
            policy_revision: Some(3),
            existing_modelled_loss_usdt: if status == RiskDecisionStatus::PostAuthorized {
                Some("0".into())
            } else {
                None
            },
            reservation_state: if status == RiskDecisionStatus::Observed {
                Some(RiskReservationState::Held)
            } else {
                None
            },
            budget: None,
            account_budget: None,
        }
    }

    #[test]
    fn t10_native_risk_receipt_cannot_authorize_another_reservation_or_policy() {
        let expected_key = "reservation_expected";
        let held = central_binding_fixture(RiskDecisionStatus::Held);
        assert!(risk_receipt_bound(
            &held,
            RiskDecisionStatus::Held,
            expected_key,
            3,
        ));
        let observed = central_binding_fixture(RiskDecisionStatus::Observed);
        assert!(risk_receipt_bound(
            &observed,
            RiskDecisionStatus::Observed,
            expected_key,
            3,
        ));
        for status in [RiskDecisionStatus::Held, RiskDecisionStatus::Observed] {
            let original = central_binding_fixture(status);
            assert!(original.valid());
            let mut wrong_reservation = original.clone();
            wrong_reservation.reservation_key = Some("reservation_other".into());
            assert!(wrong_reservation.valid());
            assert!(!risk_receipt_bound(
                &wrong_reservation,
                status,
                expected_key,
                3,
            ));
            let mut wrong_revision = original.clone();
            wrong_revision.policy_revision = Some(4);
            assert!(wrong_revision.valid());
            assert!(!risk_receipt_bound(
                &wrong_revision,
                status,
                expected_key,
                3,
            ));
        }
    }

    #[test]
    fn t10_native_post_fence_requires_matching_identity_revision_and_advanced_cas() {
        let original = central_binding_fixture(RiskDecisionStatus::PostAuthorized);
        assert!(original.valid());
        assert!(post_receipt_bound(&original, "reservation_expected", 3, 6));
        assert!(!post_receipt_bound(&original, "reservation_other", 3, 6));
        assert!(!post_receipt_bound(&original, "reservation_expected", 4, 6));
        assert!(!post_receipt_bound(&original, "reservation_expected", 3, 7));
        assert!(!post_receipt_bound(&original, "reservation_expected", 3, 8));
        let mut prior_sequence = original.clone();
        prior_sequence.risk_book_sequence = Some(6);
        assert!(prior_sequence.valid());
        assert!(!post_receipt_bound(
            &prior_sequence,
            "reservation_expected",
            3,
            6,
        ));
        let mut malformed = original;
        malformed.existing_modelled_loss_usdt = None;
        assert!(!malformed.valid());
        assert!(!post_receipt_bound(
            &malformed,
            "reservation_expected",
            3,
            6,
        ));
    }

    fn venue_ack_fixture(
        code: &str,
        client_id: &str,
        status_code: &str,
        order_id: &str,
    ) -> okx_api::TradeResponse<okx_api::OrderOperationAck> {
        okx_api::TradeResponse {
            code: code.into(),
            message: String::new(),
            data: vec![okx_api::OrderOperationAck {
                order_id: order_id.into(),
                client_order_id: client_id.into(),
                request_id: String::new(),
                timestamp_ms: String::new(),
                status_code: status_code.into(),
                status_message: String::new(),
            }],
            in_time_us: String::new(),
            out_time_us: String::new(),
        }
    }

    #[test]
    fn t10_venue_post_ack_requires_exact_order_binding_and_unambiguous_result() {
        let client = "client_order_expected";
        let accepted = venue_ack_fixture("0", client, "0", "venue_order_1");
        assert_eq!(
            place_ack_verdict(&accepted, client),
            PlaceAckVerdict::Accepted,
        );
        let denied = venue_ack_fixture("0", client, "51000", "");
        assert_eq!(
            place_ack_verdict(&denied, client),
            PlaceAckVerdict::ExplicitRejected,
        );
        assert_eq!(
            place_ack_verdict(&venue_ack_fixture("1", client, "51008", ""), client),
            PlaceAckVerdict::ExplicitRejected,
        );
        let ambiguous = [
            venue_ack_fixture("1", client, "0", "venue_order_1"),
            venue_ack_fixture("1", "different_client", "51008", ""),
            venue_ack_fixture("0", "different_client", "0", "venue_order_1"),
            venue_ack_fixture("0", client, "0", ""),
            venue_ack_fixture("0", client, "", ""),
            venue_ack_fixture("51000", client, "51000", ""),
            venue_ack_fixture("0", "different_client", "51000", ""),
        ];
        for reply in ambiguous {
            assert_eq!(place_ack_verdict(&reply, client), PlaceAckVerdict::Unknown);
        }
        let mut missing_row = accepted.clone();
        missing_row.data.clear();
        assert_eq!(
            place_ack_verdict(&missing_row, client),
            PlaceAckVerdict::Unknown,
        );
        let mut duplicate_row = accepted;
        duplicate_row.data.push(duplicate_row.data[0].clone());
        assert_eq!(
            place_ack_verdict(&duplicate_row, client),
            PlaceAckVerdict::Unknown,
        );
    }

    #[test]
    fn e1_positive_historical_usd_spot_is_classified_without_usdt_fx_guess() {
        let order: okx_api::HistoricalOrder = serde_json::from_value(serde_json::json!({
            "instType":"SPOT","instId":"BTC-USD","ordId":"123","side":"buy"
        }))
        .unwrap();
        let fill: okx_api::FillHistory = serde_json::from_value(serde_json::json!({
            "instType":"SPOT","instId":"BTC-USD","tradeId":"11","ordId":"123",
            "billId":"111","side":"buy","fillPx":"100","fillSz":"0.05",
            "fee":"-0.01","feeCcy":"USD"
        }))
        .unwrap();
        let bill: okx_api::AccountBill = serde_json::from_value(serde_json::json!({
            "billId":"111","instType":"SPOT","instId":"BTC-USD","ordId":"123",
            "tradeId":"11","ccy":"USD","fee":"-0.01","type":"2"
        }))
        .unwrap();
        let mut history = okx_api::CompleteCampaignAccountHistory {
            from_ms: 1,
            through_ms: 100,
            orders: vec![order],
            fills: vec![fill],
            bills: vec![bill],
            source_digest: "f".repeat(64),
        };
        assert_eq!(
            TradeService::historical_usd_spot_entry(&history).unwrap(),
            dec("5.01").unwrap()
        );
        history.fills[0].fee = "0.03".into();
        history.bills[0].fee = "0.03".into();
        assert_eq!(
            TradeService::historical_usd_spot_entry(&history).unwrap(),
            dec("5").unwrap(),
            "rebate neither increases nor refunds campaign entry spend"
        );
        history.fills[0].side = "sell".into();
        history.orders[0].side = "sell".into();
        history.fills[0].fee = "-0.1".into();
        history.bills[0].fee = "-0.1".into();
        assert_eq!(
            TradeService::historical_usd_spot_entry(&history).unwrap(),
            Decimal::ZERO,
            "closing notional/fee must not be charged as an opening"
        );
        history.fills[0].side = "buy".into();
        history.orders[0].side = "buy".into();
        history.fills[0].fee = "0.03".into();
        history.bills[0].fee = "0.03".into();
        history.fills[0].instrument_type = "SWAP".into();
        assert!(
            TradeService::historical_usd_spot_entry(&history).is_err(),
            "SWAP cannot be priced without historical contract unit"
        );
        history.fills[0].instrument_type = "SPOT".into();
        history.fills[0].fee_currency = "USDT".into();
        assert!(
            TradeService::historical_usd_spot_entry(&history).is_err(),
            "no observed USD/USDT rate may be promoted into a guaranteed source"
        );
        history.fills[0].fee_currency = "USD".into();
        history.bills.push(
            serde_json::from_value(serde_json::json!({
                "billId":"112","instType":"SPOT","instId":"BTC-USD",
                "ccy":"USD","fee":"-1","type":"7"
            }))
            .unwrap(),
        );
        assert!(
            TradeService::historical_usd_spot_entry(&history).is_err(),
            "standalone bills and borrow costs cannot be treated as zero"
        );
    }

    #[test]
    fn e1_empty_history_is_zero_but_unclassified_bill_never_is() {
        let mut history = okx_api::CompleteCampaignAccountHistory {
            from_ms: 1,
            through_ms: 100,
            orders: vec![],
            fills: vec![],
            bills: vec![],
            source_digest: "f".repeat(64),
        };
        assert_eq!(
            TradeService::historical_usd_spot_entry(&history).unwrap(),
            Decimal::ZERO
        );
        history.bills.push(
            serde_json::from_value(serde_json::json!({
                "billId":"999","ccy":"USD","fee":"-1","type":"2"
            }))
            .unwrap(),
        );
        assert_eq!(
            TradeService::historical_usd_spot_entry(&history),
            Err("ENTRY_OPENING_STANDALONE_BILL_UNCLASSIFIED".into())
        );
    }

    #[test]
    fn e1_held_entry_sources_reject_tampering_and_stale_tariffs() {
        use okx_observation::{FeeScheduleInput, FeeScheduleSnapshot};
        let stamp = "2026-10-30T00:00:00Z";
        let t = chrono::DateTime::parse_from_rfc3339(stamp)
            .unwrap()
            .timestamp_millis();
        let fee = FeeScheduleSnapshot::from_input(FeeScheduleInput {
            instrument_id: "BTC-USDT-SWAP".into(),
            reference_generation: "reference-1".into(),
            source_received_at: stamp.into(),
            exchange_timestamp_ms: t.to_string(),
            level: "Lv1".into(),
            maker_rate: "-0.0002".into(),
            taker_rate: "0.0005".into(),
            exact_for_instrument: true,
        })
        .unwrap();
        let mut quote = okx_execution::EntryQuoteEvidence {
            schema: "okx.entry-known-quote/v1".into(),
            account_key: "okx:real:test".into(),
            installation_id: "test".into(),
            intent_id: "intent".into(),
            plan_hash: "a".repeat(64),
            policy_revision: 1,
            instrument_id: "BTC-USDT-SWAP".into(),
            quantity: dec("1").unwrap(),
            limit_price: dec("100").unwrap(),
            contract_value: dec("1").unwrap(),
            fee_rate: dec("0.0005").unwrap(),
            maker_rate: fee.maker_rate.clone(),
            taker_rate: fee.taker_rate.clone(),
            fee_level: fee.level.clone(),
            fee_exchange_at: t,
            usd_per_usdt: Decimal::ONE,
            notional_usdt: dec("100").unwrap(),
            fee_usdt: dec("0.05").unwrap(),
            total_usdt: dec("100.05").unwrap(),
            total_usd: dec("100.05").unwrap(),
            fee_generation: fee.fee_generation.clone(),
            reference_generation: fee.reference_generation.clone(),
            fee_received_at: t,
            conversion_digest: "b".repeat(64),
            conversion_received_at: t,
            observed_at: t,
        };
        let fx = UsdConversion {
            usd_per_usdt: "1".into(),
            source_digest: "b".repeat(64),
            observed_at_ms: t as u64,
        };
        let known = entry_budget::quote(
            quote.quantity,
            quote.contract_value,
            quote.limit_price,
            quote.fee_rate,
            quote.usd_per_usdt,
        )
        .unwrap();
        // A post-HELD source check also needs fresh own collateral and
        // borrowing evidence: fee/spec/FX parity alone never grants POST.
        let mut held =
            entry_budget::EntryCostProof::source_valued_candidate(known.clone(), &fx, t + 1)
                .unwrap();
        let source_digest = "c".repeat(64);
        held.extra_immediate_fees = entry_budget::EntryCostEvidence::NotApplicable {
            basis: "test-fee-source",
            source_digest: source_digest.clone(),
        };
        held.borrowing = entry_budget::EntryCostEvidence::NotApplicable {
            basis: "test-no-borrow",
            source_digest: source_digest.clone(),
        };
        held.collateral_or_margin = entry_budget::EntryCostEvidence::Proven {
            upper_usd: known.total_usd,
            basis: "test-margin-source",
            source_digest: source_digest.clone(),
        };
        held.available_collateral = entry_budget::EntryAvailableCollateral::Proven {
            available_lower_usd: dec("200").unwrap(),
            basis: "test-own-collateral",
            source_digest,
        };
        let mut admission = okx_execution::EntryAdmissionEvidence {
            schema: "okx.entry-admission/v2".into(),
            account_key: quote.account_key.clone(),
            installation_id: quote.installation_id.clone(),
            intent_id: quote.intent_id.clone(),
            plan_hash: quote.plan_hash.clone(),
            quote_digest: "d".repeat(64),
            admission_usd_per_usdt: Decimal::ONE,
            other_entry_costs_upper_usd: Decimal::ZERO,
            entry_upper_usd: known.total_usd,
            entry_proof_digest: "e".repeat(64),
            observed_at: t,
        };
        assert!(held.verify_held_entry_admission(&quote, &admission).is_ok());
        admission.entry_upper_usd += Decimal::ONE;
        assert_eq!(
            held.verify_held_entry_admission(&quote, &admission),
            Err("POST_ENTRY_ADMISSION_COST_CHANGED".into()),
        );
        admission.entry_upper_usd = known.total_usd;
        held.available_collateral = entry_budget::EntryAvailableCollateral::Proven {
            available_lower_usd: Decimal::ONE,
            basis: "test-now-insufficient",
            source_digest: "f".repeat(64),
        };
        assert_eq!(
            held.verify_held_entry_admission(&quote, &admission),
            Err("ENTRY_COLLATERAL_INSUFFICIENT".into()),
        );
        let unproven = entry_budget::EntryCostProof::unproven_linear_quote(known.clone());
        assert_eq!(
            entry_budget::reserve_entry_evidence(&unproven, &quote, dec("101").unwrap()),
            Ok(None),
            "a correctly priced SQL quote cannot invent margin, fees, borrowing or bounded FX"
        );
        let mut tampered = unproven;
        tampered.quote.total_usd += Decimal::ONE;
        assert_eq!(
            entry_budget::reserve_entry_evidence(&tampered, &quote, dec("101").unwrap()),
            Err("ENTRY_RESERVE_QUOTE_BINDING_MISMATCH".into())
        );
        assert!(
            verify_post_held_entry_sources(&quote, &fee, "reference-1", Decimal::ONE, &fx, t + 1)
                .is_ok()
        );
        assert_eq!(
            verify_post_held_entry_sources(&quote, &fee, "reference-2", Decimal::ONE, &fx, t + 1),
            Err("ENTRY_FEE_SOURCE_SCOPE_MISMATCH".into())
        );
        assert_eq!(
            verify_post_held_entry_sources(
                &quote,
                &fee,
                "reference-1",
                dec("2").unwrap(),
                &fx,
                t + 1
            ),
            Err("POST_ENTRY_FEE_OR_SPEC_CHANGED".into())
        );
        quote.fee_generation = "tampered".into();
        assert!(
            verify_post_held_entry_sources(&quote, &fee, "reference-1", Decimal::ONE, &fx, t + 1)
                .is_err()
        );
        quote.fee_generation = fee.fee_generation.clone();
        assert_eq!(
            verify_post_held_entry_sources(
                &quote,
                &fee,
                "reference-1",
                Decimal::ONE,
                &fx,
                t + crate::config::ACCOUNT_MAX_AGE_MS + 1
            ),
            Err("ENTRY_FEE_SOURCE_STALE".into())
        );
        let invalid_provenance = UsdConversion {
            source_digest: "invalid".into(),
            ..fx.clone()
        };
        assert_eq!(
            verify_post_held_entry_sources(
                &quote,
                &fee,
                "reference-1",
                Decimal::ONE,
                &invalid_provenance,
                t + 1
            ),
            Err("POST_ENTRY_FX_SOURCE_INVALID".into())
        );
        let refreshed_same_rate = UsdConversion {
            source_digest: "c".repeat(64),
            ..fx.clone()
        };
        assert!(
            verify_post_held_entry_sources(
                &quote,
                &fee,
                "reference-1",
                Decimal::ONE,
                &refreshed_same_rate,
                t + 1
            )
            .is_ok()
        );
        let modified_fx = UsdConversion {
            usd_per_usdt: "1.01".into(),
            ..fx
        };
        assert_eq!(
            verify_post_held_entry_sources(
                &quote,
                &fee,
                "reference-1",
                Decimal::ONE,
                &modified_fx,
                t + 1
            ),
            Err("POST_ENTRY_FX_CHANGED".into())
        );
    }

    #[test]
    fn k4_post_held_fx_same_rate_or_changed_rate() {
        assert!(verify_post_held_fx("0.999000000", "0.999").is_ok());
        assert_eq!(
            verify_post_held_fx("0.999", "0.998").unwrap_err(),
            "POST_FX_SOURCE_CHANGED_REQUIRES_NEW_RESERVE"
        );
        assert_eq!(
            verify_post_held_fx("0", "1").unwrap_err(),
            "POST_FX_SOURCE_INVALID"
        );
        assert!(verify_post_held_fx("1", "invalid").is_err());
    }
    #[test]
    fn e2_algo_census_source_age_is_part_of_cohort_proof() {
        let now_ms = 1_798_758_000_000_i64;
        assert_eq!(verified_algo_source_time(now_ms as u64, now_ms), Ok(now_ms));
        assert_eq!(
            verified_algo_source_time(0, now_ms),
            Err("COHORT_ALGO_SOURCE_STALE".into())
        );
        assert_eq!(
            verified_algo_source_time((now_ms + 1) as u64, now_ms),
            Err("COHORT_ALGO_SOURCE_STALE".into())
        );
        assert_eq!(
            verified_algo_source_time(
                (now_ms - crate::config::ACCOUNT_MAX_AGE_MS - 1) as u64,
                now_ms,
            ),
            Err("COHORT_ALGO_SOURCE_STALE".into())
        );
        assert_eq!(
            verified_algo_source_time(u64::MAX, now_ms),
            Err("COHORT_ALGO_SOURCE_TIME_INVALID".into())
        );
    }

    #[test]
    fn e2_census_zero_external_risk_requires_exact_managed_coverage() {
        assert_eq!(
            certified_managed_external_exposure(["0", "0.000"].into_iter(), 0, 0, 0, 0, 0),
            Ok("0")
        );
        assert_eq!(
            certified_managed_external_exposure(["0", "-0.01"].into_iter(), 0, 0, 0, 0, 0),
            Err("COHORT_UNMANAGED_EXTERNAL_EXPOSURE".into())
        );
        assert_eq!(
            certified_managed_external_exposure(["0"].into_iter(), 0, 1, 0, 0, 0),
            Err("COHORT_UNMANAGED_EXTERNAL_EXPOSURE".into())
        );
        assert_eq!(
            certified_managed_external_exposure(["0"].into_iter(), 0, 0, 0, 1, 0),
            Err("COHORT_UNMANAGED_EXTERNAL_EXPOSURE".into())
        );
        assert!(
            certified_managed_external_exposure(["unknown"].into_iter(), 0, 0, 0, 0, 0).is_err()
        );
        assert_eq!(
            certified_managed_external_exposure(std::iter::empty::<&str>(), 0, 0, 0, 0, 0,),
            Ok("0")
        );
    }

    #[test]
    fn e2_census_canonical_digest_rejects_missing_duplicate_and_order_variation() {
        let keys = vec!["a".to_owned(), "b".to_owned()];
        let rows = vec![
            ("a".to_owned(), "proof_a".to_owned()),
            ("b".to_owned(), "proof_b".to_owned()),
        ];
        assert_eq!(
            canonical_census_content(rows.clone(), &keys),
            canonical_census_content(rows.iter().rev().cloned().collect(), &keys)
        );
        assert!(canonical_census_content(vec![rows[0].clone()], &keys).is_err());
        assert!(canonical_census_content(vec![rows[0].clone(), rows[0].clone()], &keys).is_err());
    }

    #[test]
    fn e2_active_real_intents_have_complete_bounded_central_coverage() {
        let plan = IntentPlan {
            side: "buy".into(),
            position_side: "long".into(),
            quantity: dec("1").unwrap(),
            limit_price: dec("100").unwrap(),
            ..IntentPlan::default()
        };
        let mut intent = StoredIntent {
            plan,
            intent_id: "intent_123".into(),
            idempotency_key: "key_123".into(),
            request_hash: "a".repeat(64),
            client_order_id: "client_123".into(),
            state: IntentState::Prepared,
            revision: 1,
            deadline: 123,
            plan_hash: "b".repeat(64),
            policy_revision: 3,
            risk_increase: true,
            instrument_id: "BTC-USDT-SWAP".into(),
            exchange_order_id: None,
            incident: None,
        };
        let account = format!("okx:real:{}", "a".repeat(64));
        let receipt = CentralReceipt {
            reservation_key: "central:receipt_123".into(),
            account_key: account.clone(),
            installation_id: "install".into(),
            instance_id: Some("manual".into()),
            intent_id: intent.intent_id.clone(),
            plan_hash: intent.plan_hash.clone(),
            policy_revision: 3,
            receipt_hash: "c".repeat(64),
            requested_loss_usd: dec("1").unwrap(),
            conversion_digest: "d".repeat(64),
        };
        assert!(verify_local_intent_coverage(&[(intent.clone(), None)], &[], &account).is_ok());
        intent.state = IntentState::Reserved;
        assert_eq!(
            verify_local_intent_coverage(&[(intent.clone(), None)], &[], &account),
            Err("COHORT_CENTRAL_RESERVATION_MISSING".into())
        );
        assert!(
            verify_local_intent_coverage(&[(intent.clone(), Some(receipt.clone()))], &[], &account)
                .is_ok()
        );
        let mut wrong = receipt.clone();
        wrong.account_key = "other".into();
        assert_eq!(
            verify_local_intent_coverage(&[(intent.clone(), Some(wrong))], &[], &account),
            Err("COHORT_CENTRAL_RESERVATION_MISMATCH".into())
        );
        intent.state = IntentState::Posting;
        let posting =
            verify_local_intent_coverage(&[(intent.clone(), Some(receipt.clone()))], &[], &account)
                .unwrap_err();
        assert_eq!(posting, "COHORT_UNRESOLVED_POST_OUTCOME");
        assert_eq!(
            classify_managed_protection_census_error(posting),
            ManagedProtectionCensusError::PendingPeerPost,
            "SQL POSTING is an inconclusive peer event, never evidence of a lost stop"
        );
        intent.state = IntentState::Unknown;
        assert!(
            verify_local_intent_coverage(&[(intent.clone(), Some(receipt.clone()))], &[], &account)
                .is_err()
        );
        intent.state = IntentState::Acknowledged;
        assert_eq!(
            verify_local_intent_coverage(&[(intent.clone(), Some(receipt.clone()))], &[], &account),
            Err("COHORT_EXCHANGE_ORDER_ID_UNPROVEN".into())
        );
        intent.exchange_order_id = Some("venue_abc".into());
        assert!(
            verify_local_intent_coverage(&[(intent.clone(), Some(receipt.clone()))], &[], &account)
                .is_ok()
        );
        let unresolved = vec![intent.intent_id.clone()];
        assert!(
            verify_local_intent_coverage(
                &[(intent.clone(), Some(receipt.clone()))],
                &unresolved,
                &account
            )
            .is_ok()
        );
        let mut terminal = intent.clone();
        terminal.state = IntentState::Filled;
        assert_eq!(
            verify_local_intent_coverage(
                &[(terminal.clone(), Some(receipt.clone()))],
                &unresolved,
                &account
            ),
            Err("COHORT_TERMINAL_RISK_RELEASE_PENDING".into())
        );
        let confirmed_live = std::collections::BTreeSet::from([terminal.intent_id.clone()]);
        assert!(
            verify_local_intent_coverage_with_managed(
                &[(terminal.clone(), Some(receipt.clone()))],
                &unresolved,
                &account,
                None,
                &confirmed_live,
                &std::collections::BTreeSet::new(),
            )
            .is_ok(),
            "FILLED with signed live lots/protection and matched central risk is managed"
        );
        assert_eq!(
            verify_local_intent_coverage_with_managed(
                &[(terminal.clone(), None)],
                &unresolved,
                &account,
                None,
                &confirmed_live,
                &std::collections::BTreeSet::new(),
            ),
            Err("COHORT_TERMINAL_RISK_RELEASE_PENDING".into()),
            "missing central reservation must not become managed"
        );
        let mut cancelled = terminal.clone();
        cancelled.state = IntentState::Cancelled;
        assert_eq!(
            verify_local_intent_coverage_with_managed(
                &[(cancelled, Some(receipt.clone()))],
                &unresolved,
                &account,
                None,
                &confirmed_live,
                &std::collections::BTreeSet::new(),
            ),
            Err("COHORT_TERMINAL_RISK_RELEASE_PENDING".into()),
            "CANCELLED cannot acquire the FILLED live-position exception"
        );
        let mut invalid = receipt.clone();
        invalid.plan_hash = "different".into();
        assert_eq!(
            verify_local_intent_coverage_with_managed(
                &[(terminal.clone(), Some(invalid))],
                &unresolved,
                &account,
                None,
                &confirmed_live,
                &std::collections::BTreeSet::new(),
            ),
            Err("COHORT_TERMINAL_RISK_RELEASE_PENDING".into()),
            "mismatched central plan is never managed"
        );
        let scope = TerminalReleaseCensusScope {
            account_key: &account,
            intent_id: &terminal.intent_id,
            reservation_key: &receipt.reservation_key,
            plan_hash: &terminal.plan_hash,
        };
        // The pending terminal receipt is allowed ONLY in its own release
        // census; ordinary admission/readiness still denies it.
        assert!(
            verify_local_intent_coverage_scoped(
                &[(terminal.clone(), Some(receipt.clone()))],
                &unresolved,
                &account,
                Some(scope),
            )
            .is_ok()
        );
        assert_eq!(
            verify_local_intent_coverage_scoped(
                &[(terminal.clone(), None)],
                &unresolved,
                &account,
                Some(scope),
            ),
            Err("COHORT_TERMINAL_RELEASE_RECEIPT_MISMATCH".into())
        );
        // A terminal peer is never a blanket flat-position exception: the
        // production caller may mark it only after replay-verifying its own
        // immutable account fill/bill proof and zero remaining native lots.
        let mut other = terminal.clone();
        other.intent_id = format!("{}_other", terminal.intent_id);
        let mut other_receipt = receipt.clone();
        other_receipt.intent_id = other.intent_id.clone();
        other_receipt.reservation_key = format!("{}_other", receipt.reservation_key);
        let both = vec![
            (terminal.clone(), Some(receipt.clone())),
            (other.clone(), Some(other_receipt.clone())),
        ];
        let mut pending_both = vec![terminal.intent_id.clone(), other.intent_id.clone()];
        pending_both.sort();
        assert_eq!(
            verify_local_intent_coverage_scoped(&both, &pending_both, &account, Some(scope)),
            Err("COHORT_OTHER_TERMINAL_RISK_RELEASE_PENDING".into()),
            "an unproven peer must still deny a scoped terminal RELEASE"
        );
        let empty = std::collections::BTreeSet::new();
        let validated_peer = std::collections::BTreeSet::from([other.intent_id.clone()]);
        assert!(
            verify_local_intent_coverage_with_managed(
                &both,
                &pending_both,
                &account,
                Some(scope),
                &empty,
                &validated_peer,
            )
            .is_ok(),
            "a separately source-verified peer may remain HELD while only the selected intent releases"
        );
        let mut wrong_peer = other_receipt.clone();
        wrong_peer.account_key = "different-account".into();
        assert_eq!(
            verify_local_intent_coverage_with_managed(
                &[
                    (terminal.clone(), Some(receipt.clone())),
                    (other.clone(), Some(wrong_peer))
                ],
                &pending_both,
                &account,
                Some(scope),
                &empty,
                &validated_peer,
            ),
            Err("COHORT_OTHER_TERMINAL_RISK_RELEASE_PENDING".into()),
            "a cross-account or mismatched peer receipt cannot receive the flat peer exception"
        );
        assert_eq!(
            verify_local_intent_coverage_with_managed(
                &both,
                &pending_both,
                &account,
                None,
                &empty,
                &validated_peer,
            ),
            Err("COHORT_TERMINAL_RISK_RELEASE_PENDING".into()),
            "normal admission/readiness must never use terminal peer evidence"
        );
        for wrong in [
            TerminalReleaseCensusScope {
                account_key: "wrong",
                ..scope
            },
            TerminalReleaseCensusScope {
                reservation_key: "wrong",
                ..scope
            },
            TerminalReleaseCensusScope {
                plan_hash: "wrong",
                ..scope
            },
        ] {
            assert_eq!(
                verify_local_intent_coverage_scoped(
                    &[(terminal.clone(), Some(receipt.clone()))],
                    &unresolved,
                    &account,
                    Some(wrong),
                ),
                Err("COHORT_TERMINAL_RELEASE_RECEIPT_MISMATCH".into())
            );
        }
        assert_eq!(
            verify_local_intent_coverage_scoped(
                &[(terminal.clone(), Some(receipt.clone()))],
                &unresolved,
                &account,
                Some(TerminalReleaseCensusScope {
                    intent_id: "wrong",
                    ..scope
                }),
            ),
            Err("COHORT_OTHER_TERMINAL_RISK_RELEASE_PENDING".into())
        );
        assert_eq!(
            verify_local_intent_coverage_scoped(
                &[(intent.clone(), Some(receipt.clone()))],
                &unresolved,
                &account,
                Some(scope),
            ),
            Err("COHORT_TERMINAL_RELEASE_SCOPE_UNPROVEN".into())
        );
        // Even with a correctly scoped pending release, a second unrelated
        // terminal obligation cannot be hidden in the cohort proof.
        let mut other_terminal = terminal.clone();
        other_terminal.intent_id = "intent_999".into();
        let mut other_receipt = receipt.clone();
        other_receipt.intent_id = other_terminal.intent_id.clone();
        assert_eq!(
            verify_local_intent_coverage_scoped(
                &[
                    (terminal.clone(), Some(receipt.clone())),
                    (other_terminal.clone(), Some(other_receipt)),
                ],
                &[terminal.intent_id.clone(), other_terminal.intent_id.clone()],
                &account,
                Some(scope),
            ),
            Err("COHORT_OTHER_TERMINAL_RISK_RELEASE_PENDING".into())
        );
        let mut attempted = intent.clone();
        attempted.state = IntentState::Prepared;
        attempted.exchange_order_id = None;
        assert_eq!(
            verify_local_intent_coverage(&[(attempted, None)], &unresolved, &account),
            Err("COHORT_UNRESOLVED_RESERVE_ATTEMPT".into())
        );
        assert_eq!(
            verify_local_intent_coverage(
                &[
                    (intent.clone(), Some(receipt.clone())),
                    (intent.clone(), Some(receipt.clone()))
                ],
                &[],
                &account
            ),
            Err("COHORT_LOCAL_INTENT_CORRUPT".into())
        );
        let many = vec![(intent, Some(receipt)); 100];
        assert_eq!(
            verify_local_intent_coverage(&many, &[], &account),
            Err("COHORT_LOCAL_INTENTS_TRUNCATED".into())
        );
    }

    #[test]
    fn e2_pending_venue_order_cannot_steal_matching_client_id() {
        let plan = IntentPlan {
            side: "buy".into(),
            position_side: "long".into(),
            quantity: dec("2").unwrap(),
            limit_price: dec("100").unwrap(),
            ..IntentPlan::default()
        };
        let intent = StoredIntent {
            plan,
            intent_id: "intent_test".into(),
            idempotency_key: "test_id".into(),
            request_hash: "a".repeat(64),
            client_order_id: "client_test".into(),
            state: IntentState::Acknowledged,
            revision: 1,
            deadline: 123,
            plan_hash: "b".repeat(64),
            policy_revision: 1,
            risk_increase: true,
            instrument_id: "BTC-USDT-SWAP".into(),
            exchange_order_id: Some("exchange_test".into()),
            incident: None,
        };
        let order = okx_observation::PendingOrderState {
            order_id: "exchange_test".into(),
            client_order_id: Some("client_test".into()),
            instrument_type: "SWAP".into(),
            instrument_id: "BTC-USDT-SWAP".into(),
            side: "buy".into(),
            position_side: Some("long".into()),
            trade_mode: "cross".into(),
            order_type: "limit".into(),
            price: Some("99".into()),
            size: "2".into(),
            accumulated_fill_size: "0.5".into(),
            average_fill_price: None,
            state: "partially_filled".into(),
            reduce_only: Some(false),
            creation_time_ms: "1".into(),
            update_time_ms: "2".into(),
        };
        assert!(verify_pending_order_binding(&order, &intent).is_ok());
        let mut foreign = order.clone();
        foreign.order_id = "foreign_exchange_order".into();
        assert_eq!(
            verify_pending_order_binding(&foreign, &intent),
            Err("PENDING_ORDER_INTENT_MISMATCH".into())
        );
        let mut changed = order.clone();
        changed.side = "sell".into();
        assert!(verify_pending_order_binding(&changed, &intent).is_err());
        changed = order.clone();
        changed.size = "3".into();
        assert_eq!(
            verify_pending_order_binding(&changed, &intent),
            Err("PENDING_ORDER_QUANTITY_MISMATCH".into())
        );
        changed = order.clone();
        changed.accumulated_fill_size = "2.1".into();
        assert!(verify_pending_order_binding(&changed, &intent).is_err());
        changed = order.clone();
        changed.price = Some("101".into());
        assert_eq!(
            verify_pending_order_binding(&changed, &intent),
            Err("PENDING_ORDER_PRICE_BOUND_MISMATCH".into())
        );
        changed = order.clone();
        changed.price = None;
        assert_eq!(
            verify_pending_order_binding(&changed, &intent),
            Err("PENDING_ORDER_PRICE_UNPROVEN".into())
        );
        let mut terminal = intent;
        terminal.state = IntentState::Cancelled;
        assert!(verify_pending_order_binding(&order, &terminal).is_err());
    }

    #[test]
    fn e2_managed_exposure_is_external_zero_only_after_exact_ownership_proof() {
        assert_eq!(
            certified_managed_external_exposure(["0", "0.5", "-1"].into_iter(), 2, 1, 1, 2, 2),
            Ok("0")
        );
        assert_eq!(
            certified_managed_external_exposure(["0"].into_iter(), 0, 0, 0, 0, 0),
            Ok("0")
        );
        for (positions, managed, orders, checked, algos, protected) in [
            (vec!["1"], 0, 0, 0, 0, 0),
            (vec!["0"], 0, 1, 0, 0, 0),
            (vec!["0"], 0, 0, 0, 1, 0),
        ] {
            assert_eq!(
                certified_managed_external_exposure(
                    positions, managed, orders, checked, algos, protected
                ),
                Err("COHORT_UNMANAGED_EXTERNAL_EXPOSURE".into())
            );
        }
        assert!(certified_managed_external_exposure(["NaN"], 0, 0, 0, 0, 0).is_err());
    }

    #[test]
    fn e2_census_uses_oldest_verified_account_and_financial_source() {
        let received = "2026-10-30T00:00:00Z";
        let t = chrono::DateTime::parse_from_rfc3339(received)
            .unwrap()
            .timestamp_millis();
        assert_eq!(
            census_source_floor(received, "uid", "uid", true, t - 120, t + 300),
            Ok(t - 120)
        );
        assert_eq!(
            census_source_floor(received, "uid", "other", true, t, t),
            Err("COHORT_IDENTITY_MISMATCH".into())
        );
        assert_eq!(
            census_source_floor("invalid", "uid", "uid", true, t, t),
            Err("COHORT_ACCOUNT_SOURCE_TIME_INVALID".into())
        );
        assert_eq!(
            census_source_floor(received, "uid", "uid", true, t, t - 1),
            Err("COHORT_ACCOUNT_SOURCE_STALE".into())
        );
        assert_eq!(
            census_source_floor(
                received,
                "uid",
                "uid",
                true,
                t,
                t + crate::config::ACCOUNT_MAX_AGE_MS + 1,
            ),
            Err("COHORT_ACCOUNT_SOURCE_STALE".into())
        );
        assert_eq!(
            census_source_floor(received, "uid", "uid", false, t, t),
            Err("COHORT_FINANCIAL_SOURCE_INCOMPLETE".into())
        );
        assert_eq!(
            census_source_floor(
                received,
                "uid",
                "uid",
                true,
                t - crate::config::ACCOUNT_MAX_AGE_MS - 1,
                t,
            ),
            Err("COHORT_FINANCIAL_SOURCE_STALE".into())
        );
        assert_eq!(
            census_source_floor(received, "uid", "uid", true, t + 1, t),
            Err("COHORT_FINANCIAL_SOURCE_STALE".into())
        );
    }

    fn s2_08_legacy_request() -> TradeRequest {
        let request: TradeRequest = serde_json::from_str(
            r#"{"action":"execute","idempotency_key":"fixture_command_01","operation":"open","strategy_instance":"manual","instrument":"BTC-USDT-SWAP","side":"long","sizing":{"contracts":"1.00"},"order":{"type":"limit","limit_price":"100.00","max_entry_usd":"10.00","max_slippage_bps":0},"protection":{"stop_price":"99.00"},"expires_in_ms":30000}"#,
        )
        .unwrap();
        assert!(request.valid());
        request
    }

    #[test]
    fn stale_generation_rejects_before_financial_or_venue_action() {
        assert_eq!(
            validate_generation(2, 1),
            Err("SOURCE_GENERATION_CHANGED".into())
        );
        assert_eq!(validate_generation(2, 2), Ok(()));
    }
    #[test]
    fn s2_08_historical_raw_hash_requires_exact_original_request() {
        let original = s2_08_legacy_request();
        let account = format!("okx:real:{}", "a".repeat(64));
        let raw = digest(&(account.clone(), &original)).unwrap();
        let semantic = format!(
            "{:x}",
            Sha256::digest(
                original
                    .canonical_semantic_json(&account)
                    .unwrap()
                    .as_bytes()
            )
        );
        assert_ne!(raw, semantic);
        assert!(existing_request_hash_matches(&raw, &semantic, &raw));
        assert!(existing_request_hash_matches(&semantic, &semantic, &raw));
        let mut equivalent = original.clone();
        if let TradeRequest::Execute {
            sizing: Sizing::Contracts { contracts },
            order,
            protection: Some(Protection::Price { stop_price }),
            ..
        } = &mut equivalent
        {
            *contracts = "1.0".into();
            order.limit_price = Some("100".into());
            order.max_entry_usd = Some("10".into());
            *stop_price = "99".into();
        } else {
            panic!("historical request shape");
        }
        let equivalent_raw = digest(&(account.clone(), &equivalent)).unwrap();
        let equivalent_semantic = format!(
            "{:x}",
            Sha256::digest(
                equivalent
                    .canonical_semantic_json(&account)
                    .unwrap()
                    .as_bytes()
            )
        );
        assert_eq!(equivalent_semantic, semantic);
        assert_ne!(equivalent_raw, raw);
        assert!(
            !existing_request_hash_matches(&raw, &equivalent_semantic, &equivalent_raw),
            "an original raw receipt cannot infer decimal equivalence"
        );
    }
}

#[cfg(test)]
mod s2_decimal_contract_tests {
    use super::*;

    #[test]
    fn s2_02_shared_fixture_exact_rust_decimal_addition_and_overflow() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../contracts/trade-semantic-golden.json"
        ))
        .unwrap();
        let inputs = fixture["numeric"]["sum"].as_array().unwrap();
        let left = Decimal::from_str(inputs[0].as_str().unwrap()).unwrap();
        let right = Decimal::from_str(inputs[1].as_str().unwrap()).unwrap();
        let exact = left.checked_add(right).unwrap().normalize().to_string();
        assert_eq!(exact, inputs[2].as_str().unwrap());
        let maximum = Decimal::from_str("9999999999999999999999999999").unwrap();
        assert!(maximum.checked_mul(maximum).is_none(), "NUMERIC_RANGE");
    }
}

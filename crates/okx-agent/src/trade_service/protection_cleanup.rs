//! One source-backed, single-send protective cancellation owner.
//! Public Status and direct wire regressions enter through this same function.
use super::reconcile::verify_instrument_is_flat;
use super::*;
use crate::account_bootstrap::AccountBootstrapper;
use crate::diagnostics::{ProtectionOperationTrace, ProtectionTraceEvent};
use tokio::sync::Semaphore;

// A signed flat position is NOT permission to remove a stop while the
// original opening order is still live/partially filled: a late execution
// could create new exposure after cleanup. Require the independently signed
// terminal venue order state as well as flat exposure. Canceled orders may
// have partial fills, so they still need the flat-position witness.
fn flat_entry_protection_cleanup_allowed(
    risk_increase: bool,
    signed_exposed: bool,
    venue_order_state: &str,
) -> bool {
    risk_increase
        && !signed_exposed
        && matches!(venue_order_state, "filled" | "canceled" | "mmp_canceled")
}

#[derive(Clone, Copy)]
pub(super) struct CleanupOrderFacts<'a> {
    pub intent_id: &'a str,
    pub client_order_id: &'a str,
    pub instrument_id: &'a str,
    pub risk_increase: bool,
    pub signed_exposed: bool,
    pub venue_order_state: &'a str,
}

pub(super) struct CleanupDependencies<'a> {
    pub store: &'a AccountStoreHandle,
    pub observer: &'a AccountBootstrapper,
    pub trade: &'a okx_api::TradeApi,
    pub clock: &'a okx_api::OkxRestClient,
    pub mutation_capacity: &'a Semaphore,
    pub verified_uid: &'a str,
    pub trace: &'a ProtectionOperationTrace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanupDecision {
    Blocked,
    Start,
    Inspect,
    Confirmed,
}

#[derive(Debug, PartialEq, Eq)]
enum CleanupBlocker {
    Identity,
    MissingBegin,
    ReceiptConflict,
    WrongAccount,
    IncompleteSource,
    StaleSource,
    LiveAlgo,
    WrongAlgoInstrument,
    NonFlat(String),
}
impl CleanupBlocker {
    fn reason(&self) -> &str {
        match self {
            Self::Identity => "PROTECTION_CLEANUP_IDENTITY_MISMATCH",
            Self::MissingBegin => "PROTECTION_CLEANUP_BEGIN_MISSING",
            Self::ReceiptConflict => "PROTECTION_CLEANUP_RECEIPT_CONFLICT",
            Self::WrongAccount => "PROTECTION_CLEANUP_ACCOUNT_MISMATCH",
            Self::IncompleteSource => "PROTECTION_CANCEL_READBACK_INCOMPLETE",
            Self::StaleSource => "PROTECTION_CANCEL_READBACK_STALE",
            Self::LiveAlgo => "PROTECTION_CANCEL_READBACK_PENDING",
            Self::WrongAlgoInstrument => "PROTECTION_CLEANUP_ALGO_INSTRUMENT_MISMATCH",
            Self::NonFlat(code) => code,
        }
    }
}

fn decide_cleanup(
    facts: CleanupOrderFacts<'_>,
    prior: Option<&okx_execution::OperationReceipt>,
    hash: &str,
) -> Result<CleanupDecision, CleanupBlocker> {
    if !flat_entry_protection_cleanup_allowed(
        facts.risk_increase,
        facts.signed_exposed,
        facts.venue_order_state,
    ) {
        return Ok(CleanupDecision::Blocked);
    }
    match prior {
        None => Ok(CleanupDecision::Start),
        Some(receipt) if receipt.kind != "protection_cleanup" || receipt.request_hash != hash => {
            Err(CleanupBlocker::Identity)
        }
        Some(receipt) if receipt.result_ref == "POSTING" => Ok(CleanupDecision::Inspect),
        Some(receipt) if receipt.result_ref == "VERIFIED_CANCELED" => {
            Ok(CleanupDecision::Confirmed)
        }
        Some(_) => Err(CleanupBlocker::ReceiptConflict),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProtectionCleanupStep {
    SendOnce,
    Inspect,
    Confirmed,
}

// BEGIN and its receipt are the only authority to send. A repeated BEGIN is
// never permission to repeat a possibly accepted exchange cancellation.
async fn protection_cleanup_step(
    store: &AccountStoreHandle,
    key: &str,
    hash: &str,
) -> Result<ProtectionCleanupStep, String> {
    let key = key.to_owned();
    let hash = hash.to_owned();
    store
        .call(deadline(), move |s| {
            if s.begin_operation(&key, &hash, "protection_cleanup", now())? {
                return Ok(ProtectionCleanupStep::SendOnce);
            }
            let old = s
                .operation(&key)?
                .ok_or(okx_execution::StoreError::Invalid(
                    "PROTECTION_CLEANUP_BEGIN_MISSING",
                ))?;
            match old.result_ref.as_str() {
                "POSTING" => Ok(ProtectionCleanupStep::Inspect),
                "VERIFIED_CANCELED" => Ok(ProtectionCleanupStep::Confirmed),
                _ => Err(okx_execution::StoreError::Invalid(
                    "PROTECTION_CLEANUP_RECEIPT_CONFLICT",
                )),
            }
        })
        .await
        .map_err(err)
}

// The exact same authenticated cancellation and ACK verdict are exercised
// by the direct recorded-HTTP regression and by the live Status owner.
// An ambiguous response is UNKNOWN, never authorization to resend.
async fn send_protection_cleanup_after_admission(
    trade: &okx_api::TradeApi,
    request: &okx_api::CancelAlgoOrderRequest,
    timing: &okx_api::MutationTiming,
    rate: &okx_api::RateRequestPlan,
) -> Result<(), String> {
    let response = trade
        .cancel_algo_order_after_admission(request, timing, rate)
        .await
        .map_err(|_| "PROTECTION_CANCEL_OUTCOME_UNKNOWN")?;
    if !response.top_level_success()
        || response.data.len() != 1
        || response.data[0].status_code != "0"
        || response.data[0].algo_order_id != request.algo_order_id
    {
        return Err("PROTECTION_CANCEL_NOT_CONFIRMED".into());
    }
    Ok(())
}

// This is the same production SQL/observation decision used after the first
// ACK and after restart/lost ACK. The transport cannot assert its own outcome:
// it supplies an independently normalized, complete signed inventory and
// raw position readings. Only SQL may durably commit the verified transition.
// Only facts needed for this one cancellation's SQL receipt. The observer
// and exchange adapter remain the owners of authenticated I/O.
#[derive(Clone, Copy)]
struct ProtectionCleanupIdentity<'a> {
    key: &'a str,
    hash: &'a str,
    instrument_id: &'a str,
    algo_id: &'a str,
    expected_uid: &'a str,
}
struct ProtectionCleanupObservation<'a> {
    inventory: &'a okx_api::PendingDerivativeAlgoRiskInventory,
    uid: &'a str,
    position_received_at_ms: i64,
    positions: &'a [(String, String)],
}
// Constructible only by source validation, never by the recorded HTTP adapter.
#[derive(Debug)]
struct VerifiedCleanup {
    operation_key: String,
    request_hash: String,
}

fn evaluate_cleanup_readback(
    operation: &okx_execution::OperationReceipt,
    target: ProtectionCleanupIdentity<'_>,
    observed: ProtectionCleanupObservation<'_>,
) -> Result<VerifiedCleanup, CleanupBlocker> {
    if operation.request_hash != target.hash || operation.kind != "protection_cleanup" {
        return Err(CleanupBlocker::Identity);
    }
    if operation.result_ref != "POSTING" && operation.result_ref != "VERIFIED_CANCELED" {
        return Err(CleanupBlocker::ReceiptConflict);
    }
    if operation.result_ref == "VERIFIED_CANCELED" {
        return Ok(VerifiedCleanup {
            operation_key: target.key.to_owned(),
            request_hash: target.hash.to_owned(),
        });
    }
    if observed.uid != target.expected_uid {
        return Err(CleanupBlocker::WrongAccount);
    }
    if !observed.inventory.complete_within_bound || observed.inventory.scopes_checked != 16 {
        return Err(CleanupBlocker::IncompleteSource);
    }
    if observed.inventory.received_at_ms == 0
        || observed.inventory.received_at_ms < operation.committed_at.max(0) as u64
        || observed.position_received_at_ms < operation.committed_at
    {
        return Err(CleanupBlocker::StaleSource);
    }
    verify_instrument_is_flat(
        target.instrument_id,
        observed
            .positions
            .iter()
            .map(|(inst, qty)| (inst.as_str(), qty.as_str())),
    )
    .map_err(CleanupBlocker::NonFlat)?;
    if let Some(algo) = observed
        .inventory
        .samples
        .iter()
        .find(|a| a.algo_order_id == target.algo_id)
    {
        if algo.instrument_id != target.instrument_id {
            return Err(CleanupBlocker::WrongAlgoInstrument);
        }
        return Err(CleanupBlocker::LiveAlgo);
    }
    Ok(VerifiedCleanup {
        operation_key: target.key.to_owned(),
        request_hash: target.hash.to_owned(),
    })
}

async fn commit_cleanup_confirmation(
    store: &AccountStoreHandle,
    proof: VerifiedCleanup,
) -> Result<(), String> {
    store
        .call(deadline(), move |s| {
            s.operation_receipt(
                &proof.operation_key,
                &proof.request_hash,
                "protection_cleanup",
                "VERIFIED_CANCELED",
                now(),
            )
        })
        .await
        .map_err(err)?;
    Ok(())
}

async fn protection_cleanup_readback(
    store: &AccountStoreHandle,
    target: ProtectionCleanupIdentity<'_>,
    observed: ProtectionCleanupObservation<'_>,
) -> Result<(), String> {
    let op_key = target.key.to_owned();
    let operation = store
        .call(deadline(), move |s| s.operation(&op_key))
        .await
        .map_err(err)?
        .ok_or(CleanupBlocker::MissingBegin.reason())?;
    let proof = evaluate_cleanup_readback(&operation, target, observed)
        .map_err(|blocker| blocker.reason().to_owned())?;
    if operation.result_ref == "VERIFIED_CANCELED" {
        return Ok(());
    }
    commit_cleanup_confirmation(store, proof).await
}

// The same authenticated observer/SQL path is used by Status, after restart,
// and by the direct recorded-wire regression. The test supplies only venue
// bytes; neither the readback nor its durable decision is mocked.
async fn observe_protection_cleanup_sources(
    store: &AccountStoreHandle,
    observer: &crate::account_bootstrap::AccountBootstrapper,
    target: ProtectionCleanupIdentity<'_>,
    trace: Option<(&ProtectionOperationTrace, CleanupOrderFacts<'_>)>,
) -> Result<(), String> {
    let inventory = observer
        .pending_derivative_risk_algos()
        .await
        .map_err(err)?;
    let snapshot = observer
        .snapshot_after_held_until(deadline())
        .await
        .map_err(err)?;
    let source_at = chrono::DateTime::parse_from_rfc3339(&snapshot.source_received_at)
        .map_err(|_| "PROTECTION_CLEANUP_POSITION_TIME_INVALID")?
        .timestamp_millis();
    if let Some((trace, facts)) = trace {
        trace.record(
            target.key,
            facts.intent_id,
            facts.client_order_id,
            ProtectionTraceEvent::Sources {
                inventory_at_ms: inventory.received_at_ms,
                position_at_ms: source_at,
            },
        );
    }
    let positions = snapshot
        .positions
        .iter()
        .map(|p| (p.instrument_id.clone(), p.position.clone()))
        .collect::<Vec<_>>();
    protection_cleanup_readback(
        store,
        target,
        ProtectionCleanupObservation {
            inventory: &inventory,
            uid: &snapshot.account_uid_fingerprint,
            position_received_at_ms: source_at,
            positions: &positions,
        },
    )
    .await
}

fn trace_cleanup(
    deps: &CleanupDependencies<'_>,
    facts: CleanupOrderFacts<'_>,
    event: ProtectionTraceEvent,
) {
    deps.trace.record(
        &format!("cleanup_{}", facts.client_order_id),
        facts.intent_id,
        facts.client_order_id,
        event,
    );
}

async fn inspect_cleanup(
    deps: &CleanupDependencies<'_>,
    facts: CleanupOrderFacts<'_>,
    target: ProtectionCleanupIdentity<'_>,
) -> Result<(), String> {
    let result = observe_protection_cleanup_sources(
        deps.store,
        deps.observer,
        target,
        Some((deps.trace, facts)),
    )
    .await;
    match &result {
        Ok(()) => trace_cleanup(deps, facts, ProtectionTraceEvent::Verified),
        Err(reason) => trace_cleanup(deps, facts, ProtectionTraceEvent::Unproven(reason.clone())),
    }
    result
}

pub(super) async fn cleanup_flat_protection(
    deps: CleanupDependencies<'_>,
    facts: CleanupOrderFacts<'_>,
) -> Result<(), String> {
    if !flat_entry_protection_cleanup_allowed(
        facts.risk_increase,
        facts.signed_exposed,
        facts.venue_order_state,
    ) {
        trace_cleanup(&deps, facts, ProtectionTraceEvent::Blocked);
        return Ok(());
    }
    let _capacity = deps
        .mutation_capacity
        .try_acquire()
        .map_err(|_| "ACCOUNT_MUTATION_BUSY")?;
    let id = facts.intent_id.to_owned();
    let saved = deps
        .store
        .call(deadline(), move |s| s.protection(&id))
        .await
        .map_err(err)?;
    let Some(saved) = saved else {
        trace_cleanup(&deps, facts, ProtectionTraceEvent::NoProtection);
        return Ok(());
    };
    if saved.intent_id != facts.intent_id {
        return Err("PROTECTION_CLEANUP_IDENTITY_MISMATCH".into());
    }
    let key = format!("cleanup_{}", facts.client_order_id);
    let hash = digest(&(
        facts.intent_id.to_owned(),
        saved.algo_id.clone(),
        "flat_protection_cleanup",
    ))?;
    let expected_uid = deps.verified_uid;
    let target = ProtectionCleanupIdentity {
        key: &key,
        hash: &hash,
        instrument_id: facts.instrument_id,
        algo_id: &saved.algo_id,
        expected_uid,
    };

    // Inspect a previous durable BEGIN before examining the live stop or
    // obtaining a new send permit. The former path returned UNKNOWN here
    // forever even when the signed post-cancel inventory became complete.
    let op_key = key.clone();
    let prior = deps
        .store
        .call(deadline(), move |s| s.operation(&op_key))
        .await
        .map_err(err)?;
    match decide_cleanup(facts, prior.as_ref(), &hash)
        .map_err(|blocked| blocked.reason().to_owned())?
    {
        CleanupDecision::Blocked => {
            trace_cleanup(&deps, facts, ProtectionTraceEvent::Blocked);
            return Ok(());
        }
        CleanupDecision::Confirmed => {
            trace_cleanup(&deps, facts, ProtectionTraceEvent::Verified);
            return Ok(());
        }
        CleanupDecision::Inspect => {
            trace_cleanup(&deps, facts, ProtectionTraceEvent::Inspect);
            return inspect_cleanup(&deps, facts, target).await;
        }
        CleanupDecision::Start => {}
    }

    let inventory = deps
        .observer
        .pending_derivative_risk_algos()
        .await
        .map_err(err)?;
    if !inventory.complete_within_bound || inventory.scopes_checked != 16 {
        return Err("ALGO_INVENTORY_INCOMPLETE".into());
    }
    let live = inventory
        .samples
        .iter()
        .find(|a| a.algo_order_id == saved.algo_id);
    let Some(live) = live else {
        trace_cleanup(&deps, facts, ProtectionTraceEvent::NoLiveStop);
        return Ok(());
    };
    if live.instrument_id != facts.instrument_id {
        return Err("PROTECTION_CLEANUP_ALGO_INSTRUMENT_MISMATCH".into());
    }
    let snapshot = deps
        .observer
        .snapshot_until(deadline())
        .await
        .map_err(err)?;
    if snapshot.account_uid_fingerprint != expected_uid {
        return Err("PROTECTION_CLEANUP_ACCOUNT_MISMATCH".into());
    }
    verify_instrument_is_flat(
        facts.instrument_id,
        snapshot
            .positions
            .iter()
            .map(|p| (p.instrument_id.as_str(), p.position.as_str())),
    )?;
    // Preserve all existing pre-BEGIN clock/rate admission policies.
    let request = okx_api::CancelAlgoOrderRequest {
        instrument_id: facts.instrument_id.to_owned(),
        algo_order_id: saved.algo_id.clone(),
    };
    let timing = deps
        .clock
        .clock_evidence()
        .await
        .map_err(err)?
        .mutation_timing(5000)
        .map_err(err)?;
    let rate = deps.trade.admit_cancel_algo_order(&request).map_err(err)?;
    match protection_cleanup_step(deps.store, &key, &hash).await? {
        ProtectionCleanupStep::Confirmed => {
            trace_cleanup(&deps, facts, ProtectionTraceEvent::Verified);
            return Ok(());
        }
        ProtectionCleanupStep::Inspect => {
            // A concurrent caller committed BEGIN after our first read.
            // Do not consume a second send; take a NEW authenticated read.
            trace_cleanup(&deps, facts, ProtectionTraceEvent::Inspect);
            return inspect_cleanup(&deps, facts, target).await;
        }
        ProtectionCleanupStep::SendOnce => {
            trace_cleanup(&deps, facts, ProtectionTraceEvent::Begin);
        }
    }
    trace_cleanup(&deps, facts, ProtectionTraceEvent::AdapterInvoked);
    let outcome =
        send_protection_cleanup_after_admission(deps.trade, &request, &timing, &rate).await;
    if let Err(reason) = &outcome {
        trace_cleanup(&deps, facts, ProtectionTraceEvent::Unproven(reason.clone()));
    }
    outcome?;
    trace_cleanup(&deps, facts, ProtectionTraceEvent::Ack);
    inspect_cleanup(&deps, facts, target).await
}

#[cfg(test)]
mod decision_tests {
    use super::*;
    #[test]
    fn t09_6_live_or_partially_filled_entry_cannot_cancel_its_own_stop() {
        for state in ["live", "partially_filled"] {
            assert!(
                !flat_entry_protection_cleanup_allowed(true, false, state),
                "the opening order can still fill after a flat observation"
            );
        }
        for state in ["filled", "canceled", "mmp_canceled"] {
            assert!(flat_entry_protection_cleanup_allowed(true, false, state));
            assert!(!flat_entry_protection_cleanup_allowed(true, true, state));
            assert!(!flat_entry_protection_cleanup_allowed(false, false, state));
        }
        assert!(!flat_entry_protection_cleanup_allowed(
            true, false, "unknown"
        ));
    }

    #[test]
    fn pure_readback_rejects_foreign_stale_or_nonflat_facts() {
        let operation = okx_execution::OperationReceipt {
            operation_id: "pc-test".into(),
            kind: "protection_cleanup".into(),
            request_hash: "h".into(),
            result_ref: "POSTING".into(),
            committed_at: 10_000,
        };
        let target = ProtectionCleanupIdentity {
            key: "pc-test",
            hash: "h",
            instrument_id: "BTC-USDT-SWAP",
            algo_id: "stop-a",
            expected_uid: "uid-a",
        };
        let inventory = okx_api::PendingDerivativeAlgoRiskInventory {
            schema: "okx.pending-derivative-risk-algos/v1",
            instrument_types: "SWAP,FUTURES",
            order_types: "conditional,oco,trigger,move_order_stop,chase,iceberg,twap,smart_iceberg",
            scopes_checked: 16,
            rows: 0,
            complete_within_bound: true,
            received_at_ms: 10_001,
            samples: vec![],
        };
        let flat = vec![("BTC-USDT-SWAP".into(), "0".into())];
        let verified = evaluate_cleanup_readback(
            &operation,
            target,
            ProtectionCleanupObservation {
                inventory: &inventory,
                uid: "uid-a",
                position_received_at_ms: 10_001,
                positions: &flat,
            },
        )
        .unwrap();
        assert_eq!(verified.operation_key, "pc-test");
        assert_eq!(verified.request_hash, "h");
        assert_eq!(
            evaluate_cleanup_readback(
                &operation,
                target,
                ProtectionCleanupObservation {
                    inventory: &inventory,
                    uid: "uid-b",
                    position_received_at_ms: 10_001,
                    positions: &flat,
                },
            )
            .unwrap_err(),
            CleanupBlocker::WrongAccount
        );
        assert_eq!(
            evaluate_cleanup_readback(
                &operation,
                target,
                ProtectionCleanupObservation {
                    inventory: &inventory,
                    uid: "uid-a",
                    position_received_at_ms: 9_999,
                    positions: &flat,
                },
            )
            .unwrap_err(),
            CleanupBlocker::StaleSource
        );
        let nonflat = vec![("BTC-USDT-SWAP".into(), "0.1".into())];
        assert!(matches!(
            evaluate_cleanup_readback(
                &operation,
                target,
                ProtectionCleanupObservation {
                    inventory: &inventory,
                    uid: "uid-a",
                    position_received_at_ms: 10_001,
                    positions: &nonflat,
                },
            ),
            Err(CleanupBlocker::NonFlat(_))
        ));
    }
}
#[cfg(test)]
#[path = "protection_cleanup_direct_tests.rs"]
mod protection_cleanup_direct_tests;

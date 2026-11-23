//! Read-only opening observation shared by both account storage callers.
//! Source acquisition does not initialize SQL, choose policy or grant a POST.
use crate::{AgentError, AgentResult, account_bootstrap::AccountBootstrapper};
use okx_trade_execution::OpeningSnapshot;
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use std::str::FromStr;

fn invalid() -> AgentError {
    AgentError::FinancialCaptureTask("ACCOUNT_CATALOG_INVALID".into())
}

/// Preserve the original coherent two-snapshot/algo inventory observation.
/// Actual non-flat counts and liabilities are returned unchanged; the original
/// SQL initializer owns the fail-closed flat-account admission rule.
pub(crate) async fn capture_opening(
    observer: &AccountBootstrapper,
    verified_uid: &str,
    deadline: tokio::time::Instant,
) -> AgentResult<OpeningSnapshot> {
    let opening = loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(AgentError::FinancialCaptureTask(
                "INITIAL_SNAPSHOT_UNSTABLE".into(),
            ));
        }
        let first = observer.snapshot().await.map_err(|error| {
            AgentError::FinancialCaptureTask(
                crate::source_capture::snapshot_failure_code(&error).into(),
            )
        })?;
        let algos = observer
            .pending_derivative_risk_algos()
            .await
            .map_err(|_| {
                AgentError::FinancialCaptureTask("ACCOUNT_ALGO_SOURCE_UNAVAILABLE".into())
            })?;
        let last = observer.snapshot().await.map_err(|error| {
            AgentError::FinancialCaptureTask(
                crate::source_capture::snapshot_failure_code(&error).into(),
            )
        })?;
        if first.account_uid_fingerprint != verified_uid
            || last.account_uid_fingerprint != verified_uid
        {
            return Err(AgentError::FinancialCaptureTask(
                "ACCOUNT_IDENTITY_MISMATCH".into(),
            ));
        }
        if !first
            .balance
            .same_values_ignoring_update_times(&last.balance)
            || first.positions != last.positions
            || first.pending_orders != last.pending_orders
        {
            continue;
        }
        if !algos.complete_within_bound {
            return Err(AgentError::FinancialCaptureTask(
                "ACCOUNT_ALGO_COVERAGE_INCOMPLETE".into(),
            ));
        }
        let liabilities = last
            .balance
            .details
            .iter()
            .try_fold(Decimal::ZERO, |sum, d| {
                // OKX documents liab as not applicable to Futures mode (2).
                // Other account modes require an explicit observed debt field.
                let value = match d.liabilities.as_deref() {
                    Some(v) => Decimal::from_str(v).map_err(|_| invalid())?,
                    None if last.account_level == "2" => Decimal::ZERO,
                    None => return Err(invalid()),
                };
                sum.checked_add(value).ok_or_else(invalid)
            })?;
        let positions = last
            .positions
            .iter()
            .filter(|p| Decimal::from_str(&p.position) != Ok(Decimal::ZERO))
            .count();
        let balances = last
            .balance
            .details
            .iter()
            .map(|d| {
                Ok((
                    d.currency.clone(),
                    Decimal::from_str(&d.equity).map_err(|_| invalid())?,
                ))
            })
            .collect::<AgentResult<Vec<_>>>()?;
        let observed_at = jiff::Timestamp::from_str(&last.source_received_at)
            .map_err(|_| invalid())?
            .as_millisecond();
        let source_ref = format!("{:x}", Sha256::digest(serde_json::to_vec(&last)?));
        break OpeningSnapshot {
            opening_equity_usd: Decimal::from_str(&last.balance.total_equity_usd)
                .map_err(|_| invalid())?,
            source_ref,
            observed_at,
            positions,
            orders: last.pending_orders.len(),
            conditional_orders: algos.rows,
            liabilities,
            balances,
        };
    };
    Ok(opening)
}

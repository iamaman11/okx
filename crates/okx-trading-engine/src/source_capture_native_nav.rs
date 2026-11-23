//! Native capture composition; financial rules remain in the original SQL owner.
use super::*;
use okx_trade_execution::account_worker::UnifiedFinanceHandle;

// These are elapsed public-operation timings, including queue admission.
// They are NOT SQL owner/transaction timings; those remain unobserved here.
async fn measured<T>(
    trace: &Option<(Arc<crate::finance_trace::FinanceTrace>, u64)>,
    phase: &'static str,
    operation: impl std::future::Future<Output = StoreResult<T>>,
) -> StoreResult<T> {
    if let Some((store, id)) = trace {
        store.phase_started(*id, phase);
    }
    let started = Instant::now();
    let result = operation.await;
    if let Some((store, id)) = trace {
        store.phase_finished(
            *id,
            phase,
            u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
            result.is_ok(),
        );
    }
    result
}

pub(super) async fn reconcile(
    owner: &UnifiedFinanceHandle,
    account: &str,
    input: AccountNavSnapshot,
    valuation_positions: StoreResult<Vec<ValuationPosition>>,
    fx: UsdConversionEvidence,
    trace: &Option<(Arc<crate::finance_trace::FinanceTrace>, u64)>,
) -> StoreResult<()> {
    let complete = input.complete;
    let now = input.as_of;
    let generation = input.generation;
    let source = input.source_ref.clone();
    if complete {
        measured(
            trace,
            "fill_projection",
            owner.project_pending_fills(account.into(), 1000),
        )
        .await?;
        measured(
            trace,
            "funding_attribution",
            owner.attribute_pending_funding(account.into(), fx.clone(), 100),
        )
        .await?;
        measured(
            trace,
            "stop_resolution",
            owner.resolve_source_stop(
                deadline(),
                account.into(),
                "usd-conversion-unavailable".into(),
                fx.source_digest.clone(),
            ),
        )
        .await?;
    }
    // The owner's original ProjectAccountNav performs cashflow reads, page
    // construction and ingest in one transaction. Never split its internal
    // timing into invented CashFlows/Build/Commit measurements.
    measured(
        trace,
        "account_nav_projection",
        owner.project_account_nav(account.into(), input),
    )
    .await?;
    let strategy_cursor = SourceCursor {
        source: "strategy_valuation".into(),
        stream_partition: "account".into(),
        cursor: String::new(),
        watermark: now,
        generation,
        coverage_gaps: String::new(),
    };
    let mut committed = false;
    let mut no_strategy_capital = false;
    let strategy_result = if !complete {
        Err(StoreError::Invalid("FINANCIAL_BACKFILL_PENDING"))
    } else {
        match measured(
            trace,
            "capital",
            owner.capital_state(account.into(), now, deadline()),
        )
        .await
        {
            Ok(capital) if capital.strategy_principal_usd.is_empty() => {
                no_strategy_capital = true;
                Ok(())
            }
            Ok(_) => match valuation_positions {
                Ok(venue_positions) => {
                    // Preserve the legacy distinction: Invalid becomes a
                    // coverage gap; other storage errors abort this capture.
                    match measured(
                        trace,
                        "strategy_valuation",
                        owner.commit_reconciled_strategy_nav_with_cursor(
                            account.into(),
                            StrategyValuationEvidence {
                                account_key: account.into(),
                                account_nav_source: source,
                                as_of: now,
                                conversion: fx,
                                venue_positions,
                            },
                            strategy_cursor.clone(),
                        ),
                    )
                    .await
                    {
                        Ok(_) => {
                            committed = true;
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                }
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        }
    };
    let gap = match strategy_result {
        Ok(()) => String::new(),
        Err(StoreError::Invalid(reason)) => reason.into(),
        Err(e) => return Err(e),
    };
    if !committed {
        let previous = if no_strategy_capital && gap.is_empty() {
            measured(
                trace,
                "cursor_read",
                owner.source_cursor(
                    account.into(),
                    "strategy_valuation".into(),
                    "account".into(),
                ),
            )
            .await?
        } else {
            None
        };
        if strategy_cursor_refresh_needed(no_strategy_capital, &gap, previous.as_ref(), generation)
        {
            measured(
                trace,
                "cursor_commit",
                owner.ingest_source_page(
                    account.into(),
                    IngestPage {
                        cursor: Some(SourceCursor {
                            coverage_gaps: gap,
                            ..strategy_cursor
                        }),
                        ..Default::default()
                    },
                ),
            )
            .await?;
        }
    }
    Ok(())
}

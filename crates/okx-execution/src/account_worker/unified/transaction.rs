//! One atomic AccountStore/RiskBook command; no independent queue or storage owner.
use super::{UnifiedFinanceWrite, UnifiedFinanceWriteReceipt};
use super::{original_bound_entry_settlement_sql, original_bound_unsent_release_sql};
use crate::{AccountStore, StoreError, StoreResult};
/// The original SQL owners share this one transaction; no secondary
/// AccountStore/RiskBook handle, independent COMMIT or RPC can participate.
/// The only caller that may commit this change is the installation SQL FIFO.
/// For offline tests this very same function runs inside a ROLLBACK.
pub(crate) fn apply_original_unified_finance_transaction(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    command: UnifiedFinanceWrite,
) -> StoreResult<UnifiedFinanceWriteReceipt> {
    // Enforce the complete command's atomicity even if a future caller
    // mishandles Err and attempts to COMMIT its surrounding transaction.
    // This nested savepoint is NOT a second database writer or COMMIT.
    // Normal Risk::Denied is an Ok receipt and preserves any monotonic
    // source-loss high-water observed before denial.
    tx.execute_batch("SAVEPOINT okx_finance_command")?;
    match apply_original_unified_finance_inner(tx, installation, command) {
        Ok(receipt) => {
            tx.execute_batch("RELEASE SAVEPOINT okx_finance_command")?;
            Ok(receipt)
        }
        Err(error) => {
            tx.execute_batch(
                "ROLLBACK TO SAVEPOINT okx_finance_command;
                 RELEASE SAVEPOINT okx_finance_command",
            )?;
            Err(error)
        }
    }
}

/// The original AccountStore source is authoritative. A supplied proof
/// merely names an existing no-send record; an absent, foreign or altered
/// terminal receipt is NEVER proof that the venue POST did not happen.
/// The ORIGINAL RiskBook computes the exact same canonical receipt SHA-256.
fn apply_original_unified_finance_inner(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    command: UnifiedFinanceWrite,
) -> StoreResult<UnifiedFinanceWriteReceipt> {
    use crate::risk;
    let risk_operation = |outcome: Result<risk::UnifiedRiskDecision, &'static str>| {
        outcome
            .map(UnifiedFinanceWriteReceipt::Risk)
            .map_err(StoreError::Invalid)
    };
    match command {
        UnifiedFinanceWrite::AssignCapital { account, request } => {
            let receipt = AccountStore::commit_authorized_capital_assignment_sql(
                tx,
                Some((installation, &account)),
                &request,
            )?;
            Ok(UnifiedFinanceWriteReceipt::CapitalAssigned(receipt))
        }
        UnifiedFinanceWrite::CommitStrategyNav {
            account,
            evidence,
            cursor,
        } => {
            let scope = Some((installation, account.as_str()));
            let snapshot = AccountStore::scoped_source_cursor_sql(
                tx,
                installation,
                &account,
                "account_snapshot",
                "account",
            )?;
            if cursor.source != "strategy_valuation"
                || cursor.stream_partition != "account"
                || cursor.watermark != evidence.as_of
                || !cursor.coverage_gaps.is_empty()
                || !cursor.cursor.is_empty()
                || !snapshot.is_some_and(|s| s.generation == cursor.generation)
            {
                return Err(StoreError::Invalid("STRATEGY_NAV_CURSOR_INVALID"));
            }
            let summary = AccountStore::commit_reconciled_strategy_nav_sql(
                tx,
                scope,
                &evidence,
                Some(&cursor),
            )?;
            Ok(UnifiedFinanceWriteReceipt::StrategyNavCommitted(summary))
        }
        UnifiedFinanceWrite::ProjectAccountNav { account, snapshot } => {
            let scope = Some((installation, account.as_str()));
            let state = AccountStore::source_capture_state_sql(tx, scope)?;
            if snapshot.complete {
                for cursor in [
                    state.cursors.spot,
                    state.cursors.margin,
                    state.cursors.swap,
                    state.cursors.futures,
                    state.cursors.options,
                    state.cursors.bills,
                ] {
                    if !cursor.is_some_and(|c| {
                        c.generation == snapshot.generation
                            && c.watermark >= snapshot.as_of
                            && c.coverage_gaps.is_empty()
                    }) {
                        return Err(StoreError::Invalid("ACCOUNT_NAV_SOURCE_HISTORY_INCOMPLETE"));
                    }
                }
            }
            let flows = AccountStore::cash_flow_totals_sql(tx, scope, |_| {})?;
            let page = crate::build_account_nav_page(
                &snapshot,
                &flows,
                AccountStore::opening_equity_usd_sql(tx, scope)?,
                AccountStore::usd_component_total_sql(tx, scope, "EXTERNAL_FLOW_USD")?,
            )?;
            AccountStore::ingest_unified_source_page_sql(tx, installation, &account, &page)?;
            Ok(UnifiedFinanceWriteReceipt::SourcePageIngested)
        }
        UnifiedFinanceWrite::ProjectPendingFills { account, limit } => {
            let scope = Some((installation, account.as_str()));
            let fills = AccountStore::pending_fill_sources_sql(tx, scope, limit, |_| {})?;
            let count = fills.len();
            for fill in fills {
                let effect = AccountStore::pending_fill_effect_sql(tx, scope, &fill)?;
                let page =
                    AccountStore::project_attributed_fill_sql(tx, scope, fill, effect, None)?;
                AccountStore::ingest_unified_source_page_sql(tx, installation, &account, &page)?;
            }
            Ok(UnifiedFinanceWriteReceipt::FillsProjected { count })
        }
        UnifiedFinanceWrite::AttributePendingFunding {
            account,
            conversion,
            limit,
        } => {
            if conversion.source_digest.is_empty() || conversion.observed_at <= 0 {
                return Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE"));
            }
            let scope = Some((installation, account.as_str()));
            let events = AccountStore::pending_funding_sources_sql(tx, scope, limit)?;
            let mut prepared = Vec::with_capacity(events.len());
            for event in &events {
                AccountStore::verify_funding_capture_coverage_sql(
                    tx,
                    installation,
                    &account,
                    event.time,
                )?;
                let projection = AccountStore::project_funding_source_sql(tx, scope, event)?;
                match AccountStore::normalize_source_page_usd_sql(
                    tx,
                    scope,
                    &projection.page,
                    &conversion,
                ) {
                    Ok(page) => prepared.push((page, projection.unknown, event.time)),
                    Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE")) => {
                        AccountStore::latch_stop_sql(
                            tx,
                            scope,
                            "usd-conversion-unavailable",
                            "account",
                            "USD_CONVERSION_SOURCE_UNAVAILABLE",
                            conversion.observed_at,
                        )?;
                        return Ok(UnifiedFinanceWriteReceipt::SourcePageRejected {
                            reason: "USD_CONVERSION_SOURCE_UNAVAILABLE",
                        });
                    }
                    Err(error) => return Err(error),
                }
            }
            for (page, unknown, at) in prepared {
                AccountStore::ingest_unified_source_page_sql(tx, installation, &account, &page)?;
                if unknown {
                    AccountStore::latch_stop_sql(
                        tx,
                        scope,
                        "funding-attribution-incomplete",
                        "account",
                        "FUNDING_ATTRIBUTION_INCOMPLETE",
                        at,
                    )?;
                }
            }
            Ok(UnifiedFinanceWriteReceipt::FundingAttributed {
                count: events.len(),
            })
        }
        UnifiedFinanceWrite::IngestCapturedBills {
            account,
            input,
            conversion,
        } => {
            let page =
                AccountStore::bind_captured_bills_sql(tx, Some((installation, &account)), &input)?;
            apply_original_unified_finance_inner(
                tx,
                installation,
                UnifiedFinanceWrite::IngestSourcePageUsd {
                    account,
                    page,
                    conversion,
                },
            )
        }
        UnifiedFinanceWrite::IngestCapturedFills {
            account,
            input,
            conversion,
        } => {
            let page =
                AccountStore::bind_captured_fills_sql(tx, Some((installation, &account)), &input)?;
            apply_original_unified_finance_inner(
                tx,
                installation,
                UnifiedFinanceWrite::IngestSourcePageUsd {
                    account,
                    page,
                    conversion,
                },
            )
        }
        UnifiedFinanceWrite::IngestSourcePageUsd {
            account,
            page,
            conversion,
        } => {
            // Invalid attestation metadata does not create a stop witness.
            if conversion.source_digest.is_empty() || conversion.observed_at <= 0 {
                return Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE"));
            }
            let page = match AccountStore::normalize_source_page_usd_sql(
                tx,
                Some((installation, &account)),
                &page,
                &conversion,
            ) {
                Ok(page) => page,
                Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE")) => {
                    tx.execute("INSERT INTO risk_stops VALUES(?1,?2,'usd-conversion-unavailable','account','USD_CONVERSION_SOURCE_UNAVAILABLE',?3,NULL) ON CONFLICT(installation_id,account_key,stop_id) DO NOTHING",rusqlite::params![installation,account,conversion.observed_at])?;
                    // Commit the fail-closed stop, NOT any page facts/cursor.
                    return Ok(UnifiedFinanceWriteReceipt::SourcePageRejected {
                        reason: "USD_CONVERSION_SOURCE_UNAVAILABLE",
                    });
                }
                Err(error) => return Err(error),
            };
            AccountStore::ingest_unified_source_page_sql(tx, installation, &account, &page)?;
            Ok(UnifiedFinanceWriteReceipt::SourcePageIngested)
        }
        UnifiedFinanceWrite::IngestSourcePage { account, page } => {
            AccountStore::ingest_unified_source_page_sql(tx, installation, &account, &page)?;
            Ok(UnifiedFinanceWriteReceipt::SourcePageIngested)
        }
        UnifiedFinanceWrite::RecordSourceBills {
            account,
            bills,
            order_sources,
        } => {
            AccountStore::record_source_bills_sql(
                tx,
                Some((installation, &account)),
                &bills,
                &order_sources,
            )?;
            Ok(UnifiedFinanceWriteReceipt::SourceBillsRecorded)
        }
        UnifiedFinanceWrite::RecordSourceFills { account, fills } => {
            AccountStore::record_source_fills_sql(tx, Some((installation, &account)), &fills)?;
            Ok(UnifiedFinanceWriteReceipt::SourceFillsRecorded)
        }
        UnifiedFinanceWrite::Prepare {
            account,
            decision,
            intent,
        } => {
            let inserted = AccountStore::prepare_sql_transaction(
                tx,
                Some((installation, &account)),
                &decision,
                &intent,
            )?;
            let saved = AccountStore::find_intent_sql(
                tx,
                Some((installation, &account)),
                "idempotency_key",
                &intent.idempotency_key,
            )?
            .ok_or(StoreError::Invalid("UNIFIED_PREPARED_INTENT_MISSING"))?;
            if saved.request_hash != intent.request_hash {
                return Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"));
            }
            Ok(UnifiedFinanceWriteReceipt::Prepared {
                inserted,
                intent_id: saved.intent_id,
            })
        }
        UnifiedFinanceWrite::ApplyPolicy { policy } => {
            AccountStore::apply_unified_campaign_policy_sql_transaction(tx, installation, &policy)?;
            Ok(UnifiedFinanceWriteReceipt::PolicyApplied)
        }
        UnifiedFinanceWrite::InstallEntryOpening {
            source,
            census,
            now_ms,
        } => risk_operation(risk::install_entry_opening_unified_native(
            tx,
            installation,
            &source,
            &census,
            now_ms,
        )),
        UnifiedFinanceWrite::Reserve {
            campaign,
            input,
            census,
            receipt,
        } => risk_operation(risk::reserve_unified_native(
            tx,
            installation,
            &campaign,
            &input,
            &census,
            &receipt,
        )),
        UnifiedFinanceWrite::BeginPost { campaign, input } => risk_operation(
            risk::begin_post_unified_native(tx, installation, &campaign, &input),
        ),
        UnifiedFinanceWrite::SettleEntry {
            campaign,
            account,
            intent,
            expected_sequence,
            generation,
            source,
        } => risk_operation(risk::settle_entry_unified_native(
            tx,
            installation,
            &campaign,
            &account,
            &intent,
            expected_sequence,
            generation,
            source,
        )),
        UnifiedFinanceWrite::SettleBoundEntry { request } => {
            let receipt = original_bound_entry_settlement_sql(tx, installation, &request)?;
            Ok(UnifiedFinanceWriteReceipt::BoundEntrySettled(receipt))
        }
        UnifiedFinanceWrite::ReleaseBoundUnsent { request } => {
            let receipt = original_bound_unsent_release_sql(tx, installation, &request)?;
            Ok(UnifiedFinanceWriteReceipt::BoundUnsentReleased(receipt))
        }
        UnifiedFinanceWrite::ResolveTerminal { campaign, input } => risk_operation(
            risk::resolve_terminal_unified_native(tx, installation, &campaign, &input),
        ),
        UnifiedFinanceWrite::ReleaseUnsent {
            campaign,
            account,
            intent,
            expected_sequence,
            generation,
        } => risk_operation(risk::release_unsent_unified_native(
            tx,
            installation,
            &campaign,
            &account,
            &intent,
            expected_sequence,
            generation,
        )),
        UnifiedFinanceWrite::RecoverPosting { account, now } => {
            // RiskBook and AccountStore retain their original POST fences and
            // reserved capacity, but change to UNKNOWN in ONE SQL commit.
            // Any missing foreign campaign receipt rejects the whole call.
            risk::recover_posting_unified_native(tx, installation, &account)
                .map_err(StoreError::Invalid)?;
            let changed = AccountStore::recover_posting_sql_transaction(
                tx,
                Some((installation, &account)),
                now,
            )?;
            Ok(UnifiedFinanceWriteReceipt::Recovered { changed })
        }
        UnifiedFinanceWrite::RecoverInstallation { now } => {
            // The exact installed roster comes from the original account_meta
            // primary identities in this SAME financial SQLite snapshot.
            // AccountStore's own installation-wide recovery performs the
            // original receipt checks; failure rolls back the entire caller
            // transaction including all RiskBook UNKNOWN transitions.
            let accounts = {
                let mut query = tx.prepare(
                    "SELECT account_key FROM account_meta
                     WHERE installation_id=?1 ORDER BY account_key",
                )?;
                query
                    .query_map([installation], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            };
            if accounts.is_empty() {
                return Err(StoreError::Invalid("UNIFIED_RECOVERY_ACCOUNT_ROSTER_EMPTY"));
            }
            for account in accounts {
                risk::recover_posting_unified_native(tx, installation, &account)
                    .map_err(StoreError::Invalid)?;
            }
            let changed =
                AccountStore::recover_installation_posting_sql_transaction(tx, installation, now)?;
            Ok(UnifiedFinanceWriteReceipt::Recovered { changed })
        }
        UnifiedFinanceWrite::RecordOutcome {
            account,
            intent,
            state,
            exchange_order_id,
            evidence,
            now,
        } => {
            AccountStore::record_outcome_sql_transaction(
                tx,
                Some((installation, &account)),
                &intent,
                state,
                exchange_order_id.as_deref(),
                &evidence,
                now,
            )?;
            risk::reconcile_outcome_unified_native(tx, installation, &account, &intent)
                .map_err(StoreError::Invalid)?;
            Ok(UnifiedFinanceWriteReceipt::OutcomeRecorded)
        }
    }
}

/// The stopped source manifest is deliberately immutable: a valid import
/// digest is NOT evidence that the old Cloudflare financial endpoint was
/// server-disabled and all outstanding obligations were inventoried.
/// Production financial COMMIT stays disabled until RB09 supplies a new,
/// separately verified local activation receipt. This code NEVER promotes
/// OFFLINE_NOT_AUTHORIZED by itself and never calls Cloudflare on a hot path.
pub(super) fn require_verified_live_finance_authority(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
) -> StoreResult<()> {
    let state: String = tx.query_row(
        "SELECT state FROM finance_import_manifest
         WHERE singleton=1 AND installation_id=?1",
        [installation],
        |row| row.get(0),
    )?;
    if state == "OFFLINE_NOT_AUTHORIZED" {
        return Err(StoreError::Invalid("FINANCE_CUTOVER_NOT_ACCEPTED"));
    }
    Err(StoreError::Invalid("FINANCE_AUTHORITY_STATE_UNVERIFIED"))
}

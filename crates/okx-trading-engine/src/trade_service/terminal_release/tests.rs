//! Terminal release frontier guards on the original fresh Native owner.
use super::*;
use crate::accounts::native_trade_contracts::{
    filled_fixture::{
        INSTRUMENT, OPEN_ORDER, keep_captured_readiness_current, opened_market,
        stop_captured_readiness,
    },
    fixture::{envelope, execute, fixture, record_fee, record_leverage},
};
use crate::accounts::recorded_support::{RecordedGet, RecordedPost};
use serde_json::{Value, json};
use std::time::{Duration, Instant};

#[test]
fn terminal_coverage_requires_all_frontiers_without_refreshing_source() {
    let complete = SourceCursor {
        source: "fills".into(),
        stream_partition: "SWAP".into(),
        cursor: String::new(),
        watermark: 100,
        generation: 1,
        coverage_gaps: String::new(),
    };
    assert!(source_covers_terminal(&complete, 100));
    assert!(source_covers_terminal(&complete, 99));
    assert!(!source_covers_terminal(&complete, 101));
    assert!(!source_covers_terminal(
        &SourceCursor {
            generation: 0,
            ..complete.clone()
        },
        100
    ));
    assert!(!source_covers_terminal(
        &SourceCursor {
            cursor: "next-page".into(),
            ..complete.clone()
        },
        100
    ));
    assert!(!source_covers_terminal(
        &SourceCursor {
            coverage_gaps: "missing-page".into(),
            ..complete
        },
        100
    ));
}

#[tokio::test]
async fn missing_real_sql_reservation_denies_opening_but_not_reduction() {
    let f = fixture().await;
    let capture = f._account.recorded_capture_gate.write().await;
    let store = &f._account.execution.store;
    let before = financial_rows(&f.root);
    assert_eq!(
        terminal_receipt(store, "missing", true).await.unwrap_err(),
        "TERMINAL_CENTRAL_RESERVATION_UNVERIFIED"
    );
    assert!(
        terminal_receipt(store, "missing", false)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .central_receipt(deadline(), "missing".into())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        financial_rows(&f.root),
        before,
        "read gates cannot manufacture a grant"
    );
    assert!(f.feed.outbound_posts.lock().unwrap().is_empty());
    drop(capture);
    f.finish().await;
}

#[test]
fn typed_error_serializes_stage_identity_and_original_reason() {
    let failure = TerminalReleaseError {
        stage: TerminalReleaseStage::Census,
        reason_code: "COHORT_FINANCIAL_SOURCE_INCOMPLETE".into(),
        account: "okx:real:account-a".into(),
        intent_id: "intent-a".into(),
        intent_revision: 7,
        reservation_key: Some("reservation-a".into()),
    };
    let diagnostic = serde_json::to_value(failure).unwrap();
    assert_eq!(diagnostic["stage"], "census");
    assert_eq!(
        diagnostic["reason_code"],
        "COHORT_FINANCIAL_SOURCE_INCOMPLETE"
    );
    assert_eq!(diagnostic["account"], "okx:real:account-a");
    assert_eq!(diagnostic["intent_revision"], 7);
    assert_eq!(diagnostic["reservation_key"], "reservation-a");
}

#[tokio::test]
async fn stale_fill_coverage_blocks_release_before_worker() {
    stale_terminal_coverage_blocks_release(false).await;
}

#[tokio::test]
async fn stale_bill_coverage_blocks_settlement_after_inspect() {
    stale_terminal_coverage_blocks_release(true).await;
}

async fn stale_terminal_coverage_blocks_release(stale_bills: bool) {
    let mut f = fixture().await;
    let (market_stop, market_task, _socket, _public) = opened_market(&mut f).await;
    // Setup under contended Windows CI must not age out the independently
    // captured campaign opening before the test's FIRST order. Keep the
    // existing production refresh running only until that order is admitted;
    // the stale-fill/bill assertions below still run without a refresher.
    let (readiness_stop, readiness_task) = keep_captured_readiness_current(&f);
    let now = jiff::Timestamp::now().as_millisecond().to_string();
    f.feed.expect_current_public_clock();
    record_fee(&f.feed, &now);
    record_leverage(&f.feed);
    record_leverage(&f.feed);
    record_fee(&f.feed, &now);
    record_fee(&f.feed, &now);
    record_leverage(&f.feed);
    f.feed.expect_post(RecordedPost {
        access_key: "fixture-executor-a".into(),
        path: "/api/v5/trade/order".into(),
        expected_body: json!({"instId":INSTRUMENT,"tdMode":"cross",
            "clOrdId":"__REQUEST_CL_ORD_ID__","side":"buy","posSide":"long",
            "ordType":"limit","sz":"0.1","px":"1000",
            "attachAlgoOrds":[{"attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__",
                "tpTriggerPxType":"mark","slTriggerPx":"900",
                "slTriggerPxType":"mark","slOrdPx":"-1"}]}),
        raw_response: envelope(json!([{"ordId":OPEN_ORDER,
            "clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}])),
    });
    f.feed.arm_lost_next_create_ack();
    // Establish a verified fresh opening immediately before the first order;
    // starting the maintenance task alone is not an acknowledgement. The
    // independent source frontier may briefly lag its moving cutoff.
    let ready_until = Instant::now() + Duration::from_secs(8);
    loop {
        match f
            .trade
            .install_captured_empty_campaign_opening(&f.authority, ready_until)
            .await
        {
            Ok(()) => break,
            Err(reason)
                if matches!(
                    reason.as_str(),
                    "ENTRY_BASELINE_CAPTURE_PENDING"
                        | "FINANCE_CAMPAIGN_IDENTITY_UNAVAILABLE"
                        | "COMPLETE_ACCOUNT_CENSUS_UNPROVEN"
                ) && Instant::now() < ready_until =>
            {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(reason) => panic!("fresh captured campaign opening unavailable: {reason}"),
        }
    }
    let mut request = execute();
    if let okx_ipc_contracts::trading::TradeRequest::Execute { expires_in_ms, .. } = &mut request {
        *expires_in_ms = 10_000;
    }
    let sent = f
        .trade
        .trade(&f.config.alias, &request, Some(&f.authority))
        .await
        .unwrap();
    stop_captured_readiness(readiness_stop, readiness_task).await;
    assert_eq!(
        sent["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN",
        "{sent}"
    );
    let id = sent["intent_id"].as_str().unwrap().to_owned();
    assert_eq!(f.feed.outbound_posts.lock().unwrap().len(), 1);
    let resources = f._account.clone();
    let capture = resources.recorded_capture_gate.write().await;
    let store = &resources.execution.store;
    let account = f.config.account_key.wire();
    let fills = store
        .cursor(deadline(), "fills".into(), "SWAP".into())
        .await
        .unwrap()
        .unwrap();
    let bills = store
        .cursor(deadline(), "bills".into(), "account".into())
        .await
        .unwrap()
        .unwrap();
    // The independently observed venue update is newer than either captured
    // history frontier. No wall-clock wait or rewrite of an old source as fresh.
    let terminal_at = fills.watermark.max(bills.watermark) + 1;
    if stale_bills {
        // An ordinary typed source page advances fills alone. Bills remain the
        // original captured source, so the common financial frontier is short.
        f.owner
            .ingest_source_page(
                account.clone(),
                okx_trade_execution::IngestPage {
                    cursor: Some(SourceCursor {
                        watermark: terminal_at,
                        ..fills.clone()
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    let original = store
        .lookup_intent(
            deadline(),
            crate::execution_store::IntentLookup::new(
                okx_trade_execution::TradeIntentLookup::IntentId(id.clone()),
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    f.feed.expect_get(RecordedGet {
        access_key: "fixture-executor-a".into(),
        path: "/api/v5/trade/order".into(),
        required_query: vec![("instId".into(), INSTRUMENT.into())],
        raw_response: envelope(json!([{"instId":INSTRUMENT,"ordId":OPEN_ORDER,
            "clOrdId":"__REQUEST_CL_ORD_ID__","side":"buy","posSide":"long",
            "tdMode":"cross","ordType":"limit","px":"1000","sz":"0.1",
            "accFillSz":"0","avgPx":"","state":"canceled",
            "cTime":terminal_at.to_string(),"uTime":terminal_at.to_string()}])),
    });
    let (intent, order) = f
        .trade
        .observe_intent_order(&resources, original)
        .await
        .unwrap();
    let order = order.unwrap();
    assert_eq!(intent.state, IntentState::Cancelled);
    assert_eq!(
        verified_terminal_order_update_time(&intent, Some(&order)).unwrap(),
        terminal_at
    );
    let receipt = store
        .central_receipt(deadline(), id.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.account_key, account);
    assert_eq!(receipt.intent_id, id);
    assert_eq!(receipt.plan_hash, intent.plan_hash);
    let before = financial_rows(&f.root);
    let requests_before = f.feed.requests.lock().unwrap().clone();
    let posts_before = f.feed.outbound_posts.lock().unwrap().clone();
    let denied_before = f.feed.denied_routes.lock().unwrap().clone();
    let (stage, reason, required) = if stale_bills {
        (
            "bill_coverage",
            "FINANCIAL_COVERAGE_INCOMPLETE",
            "complete_bills_covering_terminal_time",
        )
    } else {
        (
            "fill_coverage",
            "FILL_COVERAGE_INCOMPLETE",
            "complete_fills_covering_terminal_time",
        )
    };
    for _ in 0..2 {
        let failure = f
            .trade
            .release_terminal(
                &resources,
                &intent,
                Some(&order),
                true,
                Some(crate::diagnostics::ProtectionCleanupInvocation {
                    request_id: "terminal-status-source",
                    origin: "agent_request.trade.status",
                }),
            )
            .await
            .unwrap_err();
        assert_eq!(
            failure.stage,
            if stale_bills {
                TerminalReleaseStage::BillCoverage
            } else {
                TerminalReleaseStage::FillCoverage
            }
        );
        assert_eq!(failure.reason_code, reason);
        assert_eq!(failure.account, account);
        assert_eq!(failure.intent_id, id);
        assert_eq!(failure.intent_revision, intent.revision);
        assert_eq!(
            failure.reservation_key.as_deref(),
            Some(receipt.reservation_key.as_str())
        );
        assert_eq!(financial_rows(&f.root), before);
        assert_eq!(*f.feed.requests.lock().unwrap(), requests_before);
        assert_eq!(*f.feed.outbound_posts.lock().unwrap(), posts_before);
    }
    let ctx = crate::query::DispatchContext {
        finance: crate::finance_dispatch::FinanceDispatch::ready(
            f.registry.clone(),
            f.trade.clone(),
        ),
    };
    // The old independent account SQL owner is gone. Occupied Native FIFO
    // timeout/reopen is covered by owner_diagnostic_busy_tests. Here the public
    // operation projection must remain local while its source locks are held.
    let source_snapshot = resources.latest_snapshot.write().await;
    let source_health = resources.source_capture_health.write().await;
    let sql_before = f.owner.sql_timing().observed_jobs;
    let operation_reference = format!("terminal_release:{id}");
    let request = okx_ipc_contracts::AgentRequest {
        schema: okx_ipc_contracts::AGENT_REQUEST_SCHEMA_V1.into(),
        request_id: "terminal-release-diagnostic".into(),
        account: Some(account.clone()),
        operation: okx_ipc_contracts::AgentOperation::Diagnostics {
            request: okx_ipc_contracts::trading::DiagnosticsRequest {
                account: Some(account.clone()),
                reference: Some(operation_reference.clone()),
                view: Some(okx_ipc_contracts::trading::DiagnosticsView::Operation),
                cycle_id: None,
                limit: None,
            },
        },
    };
    request.validate().unwrap();
    let response = tokio::time::timeout(
        Duration::from_millis(100),
        crate::query::dispatch(&ctx, &request),
    )
    .await
    .expect("terminal diagnostics cannot wait behind source owners")
    .result
    .unwrap();
    assert_eq!(response["family"], "terminal_release");
    assert_eq!(response["state"], "OBSERVED");
    assert_eq!(response["account"], account);
    assert_eq!(response["reference"], operation_reference);
    assert_eq!(response["read_only"], true);
    assert_eq!(response["central_budget"]["state"], "NOT_EVALUATED");
    let operation = &response["operation"];
    let snapshot = &operation["snapshot"];
    assert_eq!(snapshot["intent_id"], id);
    assert_eq!(snapshot["intent_revision"], intent.revision);
    assert_eq!(snapshot["reservation_key"], receipt.reservation_key);
    assert_eq!(snapshot["attempt"], 2);
    assert_eq!(snapshot["outcome"], "blocked");
    assert_eq!(snapshot["stage"], stage);
    assert_eq!(snapshot["reason_code"], reason);
    assert_eq!(snapshot["first_observed_blocker"]["stage"], stage);
    assert_eq!(snapshot["first_observed_blocker"]["reason_code"], reason);
    assert_eq!(snapshot["first_observed_blocker"]["attempt"], 1);
    assert_eq!(operation["required_evidence"], required);
    assert_eq!(operation["diagnostic_reference"], operation_reference);
    assert_eq!(operation["trace_health"]["durable_state"], "NOT_READ");
    assert_eq!(operation["trace_health"]["history_complete"], "UNKNOWN");
    assert_eq!(operation["trace_health"]["initiator"], "OBSERVED_CALLER");
    assert_eq!(
        snapshot["invocation"]["request_id"],
        "terminal-status-source"
    );
    assert_eq!(
        snapshot["first_observed_blocker"]["invocation"]["request_id"],
        "terminal-status-source"
    );
    assert_eq!(
        snapshot["facts"]["fills"]["watermark_ms"],
        if stale_bills {
            terminal_at
        } else {
            fills.watermark
        }
    );
    assert_eq!(snapshot["facts"]["fills"]["generation"], fills.generation);
    assert_eq!(snapshot["facts"]["fills"]["has_coverage_gaps"], false);
    assert_eq!(
        snapshot["facts"]["terminal_order_updated_at_ms"],
        terminal_at
    );
    if stale_bills {
        assert_eq!(snapshot["facts"]["bills"]["watermark_ms"], bills.watermark);
        assert!(snapshot["facts"]["worker_sequence"].as_u64().unwrap() > 0);
    } else {
        assert!(snapshot["facts"]["bills"].is_null());
        assert!(snapshot["facts"]["worker_sequence"].is_null());
    }
    assert_eq!(operation["trace_health"]["dropped_event_count"], 0);
    assert_eq!(operation["trace_health"]["evicted_operation_count"], 0);
    let mut unknown_request = request.clone();
    if let okx_ipc_contracts::AgentOperation::Diagnostics { request } =
        &mut unknown_request.operation
    {
        request.reference = Some("terminal_release:unknown-after-restart".into());
    }
    let unknown = tokio::time::timeout(
        Duration::from_millis(100),
        crate::query::dispatch(&ctx, &unknown_request),
    )
    .await
    .unwrap()
    .result
    .unwrap();
    assert_eq!(unknown["state"], "NOT_OBSERVED");
    assert_eq!(
        unknown["operation"]["trace_health"]["history_complete"],
        "UNKNOWN"
    );
    assert!(unknown["operation"]["snapshot"].is_null());
    assert_eq!(f.owner.sql_timing().observed_jobs, sql_before);
    assert_eq!(*f.feed.requests.lock().unwrap(), requests_before);
    assert_eq!(*f.feed.outbound_posts.lock().unwrap(), posts_before);
    assert_eq!(*f.feed.denied_routes.lock().unwrap(), denied_before);
    assert_eq!(financial_rows(&f.root), before);
    assert_eq!(f.feed.unconsumed_reads(), 0);
    assert_eq!(f.feed.unconsumed_posts(), 0);
    assert_eq!(f.feed.unconsumed_public_clock(), 0);
    drop(source_health);
    drop(source_snapshot);
    drop(ctx);
    drop(capture);
    drop(resources);
    market_stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), market_task)
        .await
        .unwrap()
        .unwrap();
    f.finish().await;
}

// Independent read-only equality oracle for all original financial rows.
// It cannot mutate SQL, compute a competing balance, or manufacture a receipt.
fn financial_rows(root: &std::path::Path) -> Value {
    let db = rusqlite::Connection::open_with_flags(
        root.join("finance").join("finance.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let names = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        .unwrap().query_map([], |r|r.get::<_,String>(0)).unwrap()
        .collect::<Result<Vec<_>,_>>().unwrap();
    let mut tables = std::collections::BTreeMap::new();
    for name in names {
        let quoted = name.replace('"', "\"\"");
        let mut stmt = db.prepare(&format!("SELECT * FROM \"{quoted}\"")).unwrap();
        let columns = stmt.column_count();
        let mut rows = stmt
            .query_map([], |r| {
                (0..columns)
                    .map(|i| r.get_ref(i).map(|v| format!("{v:?}")))
                    .collect::<Result<Vec<_>, _>>()
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        rows.sort();
        tables.insert(name, rows);
    }
    json!(tables)
}

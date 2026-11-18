//! One bounded full-Native public lifecycle. All finance comes from signed
//! source parsing and original production commands on the one SQLite owner.
use super::*;
use rust_decimal::Decimal;

use super::filled_fixture::*;

#[tokio::test]
async fn risk_book_contract_native_filled_close_settlement_release_and_cold_reopen() {
    filled_close(false, false).await;
}

#[tokio::test]
async fn risk_book_contract_native_stop_after_fill_blocks_open_but_closes_and_releases() {
    filled_close(true, false).await;
}

#[tokio::test]
async fn risk_book_contract_native_failed_stop_emergency_close_and_verified_remediation() {
    filled_close(false, true).await;
}

async fn filled_close(stop_after_fill: bool, emergency: bool) {
    let mut fixture = fixture().await;
    let (market_stop, market_task, socket, public) = opened_market(&mut fixture).await;
    let account = fixture._account.clone();
    let now = chrono::Utc::now().timestamp_millis().to_string();
    fixture
        .feed
        .expect_public_clock(envelope(json!([{"ts":now}])));
    record_fee(&fixture.feed, &now);
    record_leverage(&fixture.feed);
    record_leverage(&fixture.feed);
    record_fee(&fixture.feed, &now);
    record_fee(&fixture.feed, &now);
    record_leverage(&fixture.feed);
    fixture.feed.expect_post(RecordedPost {access_key:"fixture-executor-a".into(),path:"/api/v5/trade/order".into(),
        expected_body:json!({"instId":INSTRUMENT,"tdMode":"cross","clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"buy","posSide":"long","ordType":"limit","sz":"0.1","px":"1000",
            "attachAlgoOrds":[{"attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__","tpTriggerPxType":"mark",
                "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"}]}),
        raw_response:envelope(json!([{"ordId":OPEN_ORDER,"clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]))});
    fixture.feed.arm_lost_next_create_ack();
    let mut request = execute();
    if let TradeRequest::Execute { expires_in_ms, .. } = &mut request {
        *expires_in_ms = 10_000;
    }
    let first = fixture
        .trade
        .trade(&fixture.config.alias, &request, Some(&fixture.authority))
        .await
        .expect("original Native Execute before signed fill");
    assert_eq!(
        first["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN",
        "{first}"
    );
    let entry = first["intent_id"].as_str().unwrap().to_owned();
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), 1);
    assert_eq!(fixture.market_boundary.updates.load(Ordering::SeqCst), 2);
    // Admission retains ordinary periodic NAV capture. Only after the signed
    // POST do we serialize the timer against the finite fill event phases.
    // Status and explicit fill captures still call the original source owner.
    let timer_capture = account.recorded_capture_gate.write().await;
    // The two admission market observations are complete. Status inventories
    // do not publish prices. Close receives its own explicit external event.
    *fixture.market_boundary.market.lock().unwrap() = None;

    // Independent cashflow: 0.1 contracts * 0.01 BTC * 1000 = 1 USDT
    // notional; 0.0005 fee. Margin is not an equity debit.
    let opened_at = chrono::Utc::now().timestamp_millis();
    snapshot(&fixture.feed, "99.9995", "0.1");
    history(
        &fixture.feed,
        Some((OPEN_ORDER, "native-open-fill", "buy", opened_at)),
    );
    capture(&fixture).await;
    let lots = fixture
        .owner
        .active_lots(
            fixture.config.account_key.wire(),
            INSTRUMENT.into(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert_eq!(lots.len(), 1);
    assert_eq!(lots[0].intent_id.as_deref(), Some(entry.as_str()));
    assert_eq!(lots[0].remaining_qty, Decimal::new(1, 1));
    let original = fixture
        .owner
        .lookup_intent(
            fixture.config.account_key.wire(),
            okx_execution::TradeIntentLookup::Reference(entry.clone()),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap()
        .unwrap();
    let stop_client = format!("s{}", &original.client_order_id[..31]);
    order(&fixture.feed, OPEN_ORDER, "buy", opened_at);
    protection(&fixture.feed, &stop_client);
    // Named public Status: protection observation, then monetary capture.
    snapshot(&fixture.feed, "99.9995", "0.1");
    snapshot(&fixture.feed, "99.9995", "0.1");
    history(&fixture.feed, None);
    let filled = fixture
        .trade
        .trade(
            &fixture.config.alias,
            &TradeRequest::Status {
                reference: entry.clone(),
            },
            None,
        )
        .await
        .expect("Native filled Status and source-backed live protection");
    assert_eq!(filled["data"]["intent"]["state"], "FILLED", "{filled}");
    assert_eq!(
        filled["data"]["entry_fill_evidence"]["state"], "verified_native_fill_and_bill_fee_sources",
        "{filled}"
    );
    assert_eq!(
        filled["data"]["incremental_entry_settlement"]["state"], "verified_or_no_change",
        "{filled}"
    );
    assert_eq!(
        filled["data"]["central_release"]["reason_code"], "INTENT_EXPOSURE_REMAINS",
        "{filled}"
    );
    assert_eq!(
        fixture.feed.unconsumed_reads(),
        0,
        "all filled observations consumed"
    );
    let spent = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(spent.consumed_entry_usd, "1.0005");

    if stop_after_fill {
        let before_stop = fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap();
        fixture.authority.state.revision += 1;
        fixture.authority.state.accounts[0].policy_revision = fixture.authority.state.revision;
        fixture.authority.state.accounts[0].explicit_pause = true;
        fixture
            .trade
            .update_governance(&fixture.authority)
            .await
            .unwrap();
        let paused = fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap();
        assert_eq!(paused.consumed_entry_usd, before_stop.consumed_entry_usd);
        assert_eq!(paused.reserved_entry_usd, before_stop.reserved_entry_usd);
        assert_eq!(paused.reserved_loss_usd, before_stop.reserved_loss_usd);
        let post_count = fixture.feed.outbound_posts.lock().unwrap().len();
        let mut fresh_open = execute();
        if let TradeRequest::Execute {
            idempotency_key, ..
        } = &mut fresh_open
        {
            *idempotency_key = "native_new_open_after_stop".into();
        }
        let denied = fixture
            .trade
            .trade(&fixture.config.alias, &fresh_open, Some(&fixture.authority))
            .await
            .unwrap_err();
        assert_eq!(denied, "ACCOUNT_NEW_RISK_PAUSED");
        assert_eq!(
            fixture.feed.outbound_posts.lock().unwrap().len(),
            post_count
        );
        assert_eq!(
            fixture
                .owner
                .campaign_summary(fixture.campaign.clone())
                .await
                .unwrap(),
            paused,
            "denied new risk cannot consume or release the existing position budget"
        );
    }

    // Close is independently authorized from the actual SQL lot, fresh venue
    // exposure and its own public market observation. ACK loss cannot resend.
    close_market(&socket, &public).await;
    let now = chrono::Utc::now().timestamp_millis().to_string();
    fixture
        .feed
        .expect_public_clock(envelope(json!([{"ts":now}])));
    if emergency {
        order(&fixture.feed, OPEN_ORDER, "buy", opened_at);
        get(
            &fixture.feed,
            "fixture-executor-a",
            "/api/v5/trade/order-algo",
            vec![("algoClOrdId", &stop_client)],
            json!([{"instId":INSTRUMENT,"algoId":STOP,"algoClOrdId":stop_client,"state":"canceled",
                "sz":"0.1","side":"sell","posSide":"long","tdMode":"cross","ordType":"conditional",
                "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"}]),
        );
        snapshot(&fixture.feed, "99.9995", "0.1"); // original failed-stop Status
        order(&fixture.feed, OPEN_ORDER, "buy", opened_at); // independent emergency readback
    }
    snapshot(&fixture.feed, "99.9995", "0.1"); // plan
    snapshot(&fixture.feed, "99.9995", "0.1"); // final mode/position
    fixture.feed.expect_post(RecordedPost{access_key:"fixture-executor-a".into(),path:"/api/v5/trade/order".into(),
        expected_body:json!({"instId":INSTRUMENT,"tdMode":"cross","clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"sell","posSide":"long","ordType":"ioc","sz":"0.1","px":"999"}),
        raw_response:envelope(json!([{"ordId":CLOSE_ORDER,"clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]))});
    fixture.feed.arm_lost_next_create_ack();
    let close_request = TradeRequest::Close {
        idempotency_key: if emergency {
            format!("safety_close_{}", original.client_order_id)
        } else {
            "native_filled_close_001".into()
        },
        reference: entry.clone(),
        expires_in_ms: if emergency { 2_000 } else { 10_000 },
    };
    let close = if emergency {
        snapshot(&fixture.feed, "99.9995", "0.1"); // finance after the bounded protective action
        history(&fixture.feed, None);
        let result = fixture
            .trade
            .trade(
                &fixture.config.alias,
                &TradeRequest::Status {
                    reference: entry.clone(),
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            result["data"]["compensation"]["state"], "attempted",
            "{result}"
        );
        assert_eq!(
            result["data"]["compensation"]["action"]["sql_owned_quantity"], "0.1",
            "{result}"
        );
        assert_eq!(result["data"]["protection_incident"], true, "{result}");
        result["data"]["compensation"]["action"]["outcome"].clone()
    } else {
        fixture
            .trade
            .trade(&fixture.config.alias, &close_request, None)
            .await
            .expect("public Native Close from attributed lot")
    };
    assert_eq!(
        close["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN",
        "{close}"
    );
    let close_id = close["intent_id"].as_str().unwrap().to_owned();
    assert_eq!(
        fixture.feed.outbound_posts.lock().unwrap().len(),
        2,
        "one entry and one Close Create"
    );
    assert_eq!(
        fixture.feed.unconsumed_reads(),
        0,
        "Close admission consumed exact source"
    );

    // IOC sell at 999 may execute at the recorded better 1000. PnL is zero;
    // the second independently billed 0.0005 fee leaves equity 99.999.
    let closed_at = chrono::Utc::now().timestamp_millis();
    snapshot(&fixture.feed, "99.999", "0");
    history(
        &fixture.feed,
        Some((CLOSE_ORDER, "native-close-fill", "sell", closed_at)),
    );
    if emergency {
        crate::source_capture::capture_account_sources_measured(
            &account.observer,
            &account.private_ws,
            &account.execution,
        )
        .await
        .unwrap();
        let (risk, stops) = fixture
            .owner
            .financial_readiness(
                fixture.config.account_key.wire(),
                None,
                Instant::now() + Duration::from_secs(3),
            )
            .await
            .unwrap();
        assert!(risk.nav_complete);
        assert_eq!(
            stops, 1,
            "the protection incident must survive capture alone until verified remediation"
        );
    } else {
        capture(&fixture).await;
    }
    order(&fixture.feed, CLOSE_ORDER, "sell", closed_at);
    snapshot(&fixture.feed, "99.999", "0");
    snapshot(&fixture.feed, "99.999", "0");
    history(&fixture.feed, None);
    let closed = fixture
        .trade
        .trade(&fixture.config.alias, &close_request, None)
        .await
        .expect("same-key Close replay reads the original filled reduction");
    assert_eq!(closed["data"]["intent"]["intent_id"], close_id, "{closed}");
    assert_eq!(closed["data"]["intent"]["state"], "FILLED", "{closed}");
    assert_eq!(
        closed["data"]["central_release"]["state"], "verified",
        "{closed}"
    );
    assert_eq!(
        fixture.feed.outbound_posts.lock().unwrap().len(),
        2,
        "no second Close Create"
    );
    assert_eq!(fixture.feed.unconsumed_reads(), 0);

    // The original entry owns its protective stop. Its signed cancel ACK is
    // the only event switching all 16 inventory scopes to empty.
    cleanup(&fixture.feed, &stop_client);
    order(&fixture.feed, OPEN_ORDER, "buy", opened_at);
    snapshot(&fixture.feed, "99.999", "0"); // initial status
    snapshot(&fixture.feed, "99.999", "0"); // cleanup before BEGIN
    snapshot(&fixture.feed, "99.999", "0"); // cleanup after signed ACK
    snapshot(&fixture.feed, "99.999", "0"); // finance capture
    snapshot(&fixture.feed, "99.999", "0"); // full terminal census
    if emergency {
        snapshot(&fixture.feed, "99.999", "0"); // remediation bracket before algo census
        snapshot(&fixture.feed, "99.999", "0"); // remediation bracket after algo census
    }
    history(&fixture.feed, None);
    let released = fixture
        .trade
        .trade(
            &fixture.config.alias,
            &TradeRequest::Status {
                reference: entry.clone(),
            },
            None,
        )
        .await
        .expect("original entry terminal settlement and joint Resolve release");
    assert_eq!(
        released["data"]["central_release"]["state"], "verified",
        "{released}"
    );
    assert_eq!(released["data"]["financial_complete"], true, "{released}");
    if emergency {
        assert_eq!(
            released["data"]["protection_remediation"]["state"], "verified_flat_no_orphans",
            "{released}"
        );
        assert_eq!(released["data"]["protection_incident"], false, "{released}");
    }
    assert_eq!(
        fixture.feed.outbound_posts.lock().unwrap().len(),
        3,
        "two Creates and one exact stop cancel"
    );
    assert_eq!(fixture.feed.unconsumed_posts(), 0);
    assert_eq!(
        fixture.feed.unconsumed_reads(),
        0,
        "finite lifecycle transcript fully consumed"
    );
    assert!(fixture.feed.denied_routes.lock().unwrap().is_empty());
    let account_key = fixture.config.account_key.wire();
    assert!(
        fixture
            .owner
            .active_lots(
                account_key.clone(),
                INSTRUMENT.into(),
                Instant::now() + Duration::from_secs(3)
            )
            .await
            .unwrap()
            .is_empty()
    );
    let final_summary = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(
        final_summary.consumed_entry_usd, "1.0005",
        "Close cannot replenish spent entry capacity"
    );
    assert_eq!(final_summary.reserved_entry_usd, "0");
    assert_eq!(final_summary.reserved_loss_usd, "0");
    let final_account = fixture
        .owner
        .stopped_account_summary(fixture.campaign.clone(), account_key.clone())
        .await
        .unwrap();
    assert_eq!(final_account.active_reservations, 0);
    // Verify committed economics, not continuing freshness while this test
    // deliberately holds the periodic source producer stopped.
    let financial_snapshot = fixture
        .owner
        .account_diagnostic_snapshot(
            Instant::now() + Duration::from_secs(3),
            account_key.clone(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(financial_snapshot.nav_coverage.as_deref(), Some("COMPLETE"));
    let risk = financial_snapshot.financial;
    assert_eq!(
        risk.daily_loss_usd,
        Decimal::new(1, 3),
        "two signed fees, zero price PnL"
    );
    assert_eq!(financial_snapshot.stop_count, 0);

    let report_query = okx_execution::FinanceReportQuery {
        from: 0,
        through: chrono::Utc::now().timestamp_millis(),
        instances: vec![],
        currencies: vec![],
        view: okx_execution::FinanceReportView::Summary,
    };
    let report = fixture
        .owner
        .finance_report(
            account_key.clone(),
            report_query.clone(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    let account_nav = report
        .nav
        .iter()
        .find(|n| n.scope_id == "account" && n.currency == "USD")
        .expect("original account USD NAV");
    assert_eq!(
        account_nav.current_equity,
        Some(Decimal::new(99999, 3)),
        "100 minus two independently billed 0.0005 fees"
    );

    // Extend this same verified public release, rather than constructing a
    // synthetic RELEASED reservation or repeating the full lifecycle elsewhere.
    let correction_receipts = if !stop_after_fill && !emergency {
        super::native_post_release_correction::verify_post_release_correction(
            &fixture, &account, &entry, opened_at,
        )
        .await
    } else {
        Vec::new()
    };
    let final_summary = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    let final_account = fixture
        .owner
        .stopped_account_summary(fixture.campaign.clone(), account_key.clone())
        .await
        .unwrap();
    assert_eq!(
        final_summary.consumed_entry_usd,
        if stop_after_fill || emergency {
            "1.0005"
        } else {
            "2.0005"
        }
    );
    assert_eq!(
        final_summary.entry_bound_breach_usd,
        if stop_after_fill || emergency {
            "0"
        } else {
            "1"
        }
    );

    market_stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), market_task)
        .await
        .unwrap()
        .unwrap();
    fixture.registry.shutdown().await;
    drop(timer_capture);
    drop(account);
    let root = fixture.root.clone();
    let installation = fixture.owner.installation_id().to_owned();
    let campaign = fixture.campaign.clone();
    let mut stopped = fixture.owner.termination_signal();
    drop(original);
    drop(fixture);
    tokio::time::timeout(Duration::from_secs(3), stopped.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
    let owner = UnifiedFinanceHandle::open_native(root.clone(), installation)
        .await
        .unwrap();
    assert_eq!(
        owner.campaign_summary(campaign.clone()).await.unwrap(),
        final_summary
    );
    if !correction_receipts.is_empty() {
        super::native_post_release_correction::verify_reopened_correction(
            &owner,
            &campaign,
            &account_key,
            correction_receipts,
        )
        .await;
    }
    assert_eq!(
        owner
            .stopped_account_summary(campaign, account_key.clone())
            .await
            .unwrap(),
        final_account
    );
    assert!(
        owner
            .active_lots(
                account_key.clone(),
                INSTRUMENT.into(),
                Instant::now() + Duration::from_secs(3)
            )
            .await
            .unwrap()
            .is_empty()
    );
    let reopened_report = owner
        .finance_report(
            account_key.clone(),
            report_query,
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    let reopened_nav = reopened_report
        .nav
        .iter()
        .find(|n| n.scope_id == "account" && n.currency == "USD")
        .expect("reopened original account USD NAV");
    assert_eq!(reopened_nav.current_equity, Some(Decimal::new(99999, 3)));
    for reference in [entry, close_id] {
        let i = owner
            .lookup_intent(
                account_key.clone(),
                okx_execution::TradeIntentLookup::Reference(reference.clone()),
                Instant::now() + Duration::from_secs(3),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(i.state, okx_execution::IntentState::Filled);
        assert!(
            owner
                .reservation_receipt(
                    account_key.clone(),
                    reference,
                    okx_execution::CentralReceiptView::ActiveExposure,
                    Instant::now() + Duration::from_secs(3)
                )
                .await
                .unwrap()
                .is_none()
        );
    }
    let mut stopped = owner.termination_signal();
    drop(owner);
    tokio::time::timeout(Duration::from_secs(3), stopped.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
static COUNTER: AtomicU64 = AtomicU64::new(0);
#[test]
fn campaign_conversion_is_attested_deduplicated_and_profit_never_refills() {
    let mut s = store();
    let e = fee("fee", "fill-source");
    let fx = UsdConversionEvidence {
        observed_at: 4,
        source_digest: "authenticated-usdt-usd".into(),
        usd_per_currency: std::collections::BTreeMap::from([(
            "USDT".into(),
            Decimal::new(10001, 4),
        )]),
    };
    let page = IngestPage {
        finance: vec![e],
        ..Default::default()
    };
    s.ingest_page_with_usd(&page, &fx).unwrap();
    let loss = s.policy().unwrap().4;
    assert_eq!(loss, Decimal::new(250025, 6));
    let later = UsdConversionEvidence {
        observed_at: 5,
        source_digest: "different-rate".into(),
        usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::from(2))]),
    };
    s.ingest_page_with_usd(&page, &later).unwrap();
    assert_eq!(s.policy().unwrap().4, loss);
    let profit = FinanceEntry {
        entry_id: "profit".into(),
        economic_event_key: "profit".into(),
        component: "REALIZED_PNL".into(),
        amount: Decimal::from(20),
        ..fee("unused", "bill-source")
    };
    s.ingest_page_with_usd(
        &IngestPage {
            finance: vec![profit],
            ..Default::default()
        },
        &fx,
    )
    .unwrap();
    assert_eq!(s.policy().unwrap().4, loss);
    let rows = s.finance_history(1, 10, 0, "", 100).unwrap();
    assert!(
        rows.iter()
            .any(|e| e.component == "CAMPAIGN_LOSS_USD" && e.source_ref.contains("rate:1.0001"))
    );
}
#[test]
fn finance_filters_are_parameterized_and_exact() {
    let mut s = store();
    let mut e = fee("fee", "source");
    e.currency = "USD".into();
    s.ingest_page(&IngestPage {
        finance: vec![e],
        ..Default::default()
    })
    .unwrap();
    assert_eq!(
        s.finance_history_filtered(1, 10, 0, "", 10, &[], &["USD".into()])
            .unwrap()
            .len(),
        1
    );
    assert!(
        s.finance_history_filtered(1, 10, 0, "", 10, &[], &["USD' OR 1=1 --".into()])
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        s.finance_totals_filtered(1, 10, &[], &["USD".into()])
            .unwrap()[&("USD".into(), "FEE".into())],
        Decimal::new(-25, 2)
    );
    assert!(
        s.finance_history_filtered(1, 10, 0, "", 10, &[], &vec!["USD".into(); 33])
            .is_err()
    );
}
#[test]
fn pending_fill_projection_is_exactly_once() {
    let mut s = store();
    let f = Fill {
        side: "buy".into(),
        fill_id: "pending".into(),
        instrument_id: "BTC".into(),
        trade_id: "t".into(),
        exchange_order_id: "o".into(),
        intent_id: None,
        time: 3,
        qty: Decimal::ONE,
        price: Decimal::from(100),
        fee: Decimal::new(-1, 2),
        currency: "USDT".into(),
    };
    s.ingest_page(&IngestPage {
        fills: vec![f],
        finance: vec![fee("source-fee", "bill-source")],
        ..Default::default()
    })
    .unwrap();
    assert_eq!(s.project_pending_fills(100).unwrap(), 1);
    assert_eq!(s.project_pending_fills(100).unwrap(), 0);
    assert_eq!(s.active_lots("BTC").unwrap().len(), 1);
    assert_eq!(s.finance_history(1, 10, 0, "", 10).unwrap().len(), 1);
}
#[test]
fn backup_blocks_risk_until_exchange_and_central_reconcile() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    let backup = path();
    s.backup(&backup).unwrap();
    let mut s =
        AccountStore::open_existing(backup, &s.meta.account_key, &s.meta.installation_id).unwrap();
    assert!(s.begin_post("intent", 1, 4).is_err());
    assert!(s.mark_storage_reconciled(i64::MAX, "exchange", "").is_err());
    s.mark_storage_reconciled(i64::MAX, "exchange", "central")
        .unwrap();
    assert!(s.begin_post("intent", 1, 4).unwrap());
}
#[cfg(target_os = "linux")]
fn rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find(|l| l.starts_with("VmRSS:"))
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap()
}
#[cfg(target_os = "linux")]
#[test]
#[ignore = "explicit million-history performance workload"]
fn sqlite_million_history_bounded_rss() {
    let mut s = store();
    intent(&mut s);
    let tx = s.connection.transaction().unwrap();
    tx.execute_batch("WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000000) INSERT INTO intents(intent_id,idempotency_key,request_hash,decision_id,state,revision,client_order_id,deadline,plan_hash,policy_revision,risk_increase,instrument_id,created_at,updated_at) SELECT 'terminal-'||x,'key-'||x,'request-hash','decision','FILLED',4,'client-'||x,100,'plan',1,1,'BTC-USDT-SWAP',2,4 FROM n; INSERT INTO intent_plans SELECT intent_id,'open','buy','1','100','95','manual','5',1 FROM intents WHERE state='FILLED';").unwrap();
    tx.commit().unwrap();
    s.checkpoint().unwrap();
    let path = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let before = rss_kib();
    let start = std::time::Instant::now();
    let s = AccountStore::open_existing(&path, &meta.account_key, &meta.installation_id).unwrap();
    let active = s.active_intents(100).unwrap();
    let delta = rss_kib().saturating_sub(before);
    assert_eq!(active.len(), 1);
    assert!(delta <= 256 * 1024, "RSS increase {delta} KiB");
    let mut q=s.connection.prepare("EXPLAIN QUERY PLAN SELECT intent_id FROM intents WHERE state IN('PREPARED','RESERVED','POSTING','UNKNOWN','ACKNOWLEDGED','PARTIALLY_FILLED') ORDER BY state,updated_at,intent_id LIMIT 100").unwrap();
    let plans = q
        .query_map([], |r| r.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(plans.iter().any(|s| s.contains("idx_intents_active")));
    let cold_us = start.elapsed().as_micros();
    for _ in 0..5 {
        std::hint::black_box(s.account_summary().unwrap());
    }
    let mut measurements = Vec::new();
    let mut max_bytes = 0;
    for _ in 0..30 {
        let tick = std::time::Instant::now();
        let bytes = serde_json::to_vec(&s.account_summary().unwrap()).unwrap();
        measurements.push(tick.elapsed().as_micros() as u64);
        max_bytes = max_bytes.max(bytes.len());
    }
    measurements.sort();
    assert!(measurements[28] <= 200000);
    assert!(max_bytes <= 4096);
    use sha2::Digest;
    let fixture_hash = format!(
        "{:x}",
        sha2::Sha256::digest(
            concat!(
                include_str!("../schema/account-v2.sql"),
                "1000000 terminal intents+plans;1active"
            )
            .as_bytes()
        )
    );
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with("model name"))
        .unwrap_or("unknown")
        .to_owned();
    let evidence = serde_json::json!({"test":"sqlite_million_history_bounded_rss","result":"PASS","os":std::env::consts::OS,"cpu":cpu,"sqlite_version":rusqlite::version(),"sqlite_source_id":s.sqlite_runtime().unwrap().1,"terminal_intents":1000000,"active_intents":active.len(),"warmup_runs":5,"measured_runs":30,"p50_us":measurements[14],"p95_us":measurements[28],"max_us":measurements[29],"response_bytes_max":max_bytes,"rss_delta_kib":delta,"cold_active_us":cold_us,"query_plan":plans,"fixture_sha256":fixture_hash});
    let output = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/verification/398-sql-million.json");
    std::fs::create_dir_all(output.parent().unwrap()).unwrap();
    std::fs::write(output, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    println!("{}", evidence);
    println!(
        "million_history terminal_count=1000000 active_count={} rss_delta_kib={delta} cold_active_elapsed_us={} sqlite={} index={plans:?}",
        active.len(),
        start.elapsed().as_micros(),
        rusqlite::version()
    );
    drop(q);
    drop(s);
    std::fs::remove_file(path).unwrap();
}
fn path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "okx-t0-{}-{}.db",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}
fn store() -> AccountStore {
    let p = path();
    let m = AccountMeta {
        initialize_operation_id: "connect-op".into(),
        release_id: "release".into(),
        account_key: "okx:real:verified".into(),
        installation_id: "installation".into(),
        initialized_at: 1,
        opening_source: "authenticated-flat".into(),
    };
    let s = OpeningSnapshot {
        source_ref: m.opening_source.clone(),
        observed_at: 1,
        positions: 0,
        orders: 0,
        conditional_orders: 0,
        liabilities: Decimal::ZERO,
        balances: vec![("USDT".into(), Decimal::from(100))],
    };
    let mut a = AccountStore::initialize_flat(p, m, &s).unwrap();
    a.apply_authority_policy(1, false, "campaign-20", Decimal::from(20))
        .unwrap();
    a
}
fn intent(s: &mut AccountStore) -> StoredIntent {
    let d = Decision {
        decision_id: "decision".into(),
        instance_id: None,
        decision_at: 2,
        input_digest: "inputs".into(),
        policy_revision: 1,
        code_version: "test".into(),
    };
    let i = NewIntent {
        plan: IntentPlan {
            operation: "open".into(),
            side: "buy".into(),
            quantity: Decimal::ONE,
            limit_price: Decimal::from(100),
            stop_price: Some(Decimal::from(95)),
            strategy_instance: "manual".into(),
            planned_loss_usd: Decimal::from(5),
            source_generation: 1,
        },
        intent_id: "intent".into(),
        idempotency_key: "request".into(),
        request_hash: "body".into(),
        client_order_id: "stableclient".into(),
        deadline: 100,
        plan_hash: "plan".into(),
        policy_revision: 1,
        risk_increase: true,
        instrument_id: "BTC-USDT-SWAP".into(),
    };
    s.prepare(&d, &i).unwrap()
}
fn reserve(s: &mut AccountStore) {
    s.reserve(
        &CentralReceipt {
            reservation_key: "central-key".into(),
            account_key: s.meta.account_key.clone(),
            installation_id: s.meta.installation_id.clone(),
            instance_id: None,
            intent_id: "intent".into(),
            plan_hash: "plan".into(),
            policy_revision: 1,
            receipt_hash: "central-hash".into(),
            requested_loss_usd: Decimal::from(5),
            conversion_digest: "usd-conversion".into(),
        },
        3,
    )
    .unwrap();
}
fn fee(id: &str, source: &str) -> FinanceEntry {
    FinanceEntry {
        entry_id: id.into(),
        economic_event_key: "fill:BTC:trade:order".into(),
        component: "FEE".into(),
        allocation_leg: "account".into(),
        amount: Decimal::new(-25, 2),
        currency: "USDT".into(),
        instance_id: None,
        source_ref: source.into(),
        time: 4,
        reverses_entry_id: None,
    }
}
#[test]
fn sqlite_runtime_version_supported() {
    let s = store();
    let (v, source) = s.sqlite_runtime().unwrap();
    assert!(rusqlite::version_number() >= 3_051_003, "{v}");
    assert!(!source.is_empty());
    s.integrity_check().unwrap();
}
#[test]
fn sql_ingest_retry_exactly_once() {
    let mut s = store();
    let p = IngestPage {
        finance: vec![fee("fee", "fill")],
        ..Default::default()
    };
    s.ingest_page(&p).unwrap();
    s.ingest_page(&p).unwrap();
    let v = s.finance_history(1, 10, 0, "", 100).unwrap();
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].amount, Decimal::new(-25, 2));
}
#[test]
fn fee_fill_bill_counted_once() {
    let mut s = store();
    s.ingest_page(&IngestPage {
        finance: vec![fee("fill-fee", "fill:trade")],
        ..Default::default()
    })
    .unwrap();
    s.ingest_page(&IngestPage {
        finance: vec![fee("bill-fee", "bill:bill1")],
        ..Default::default()
    })
    .unwrap();
    assert_eq!(s.finance_history(1, 10, 0, "", 100).unwrap().len(), 1);
    let count: i64 = s
        .connection
        .query_row("SELECT count(*) FROM finance_source_links", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(count, 2);
}
#[test]
fn source_page_and_cursor_atomic() {
    let mut s = store();
    let cursor = SourceCursor {
        source: "fills".into(),
        stream_partition: "BTC".into(),
        cursor: "next".into(),
        watermark: 10,
        generation: 1,
        coverage_gaps: "".into(),
    };
    let bad = FinanceEntry {
        currency: "".into(),
        ..fee("bad", "source")
    };
    assert!(
        s.ingest_page(&IngestPage {
            finance: vec![fee("good", "source"), bad],
            cursor: Some(cursor.clone()),
            ..Default::default()
        })
        .is_err()
    );
    assert!(s.cursor("fills", "BTC").unwrap().is_none());
    assert!(s.finance_history(1, 20, 0, "", 10).unwrap().is_empty());
    s.ingest_page(&IngestPage {
        finance: vec![fee("good", "source")],
        cursor: Some(cursor),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(s.cursor("fills", "BTC").unwrap().unwrap().watermark, 10);
}
#[test]
fn crash_before_after_begin_post() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    let p = s.path.clone();
    let m = s.meta.clone();
    drop(s);
    let mut s = AccountStore::open_existing(&p, &m.account_key, &m.installation_id).unwrap();
    assert_eq!(
        s.intent("intent").unwrap().unwrap().state,
        IntentState::Reserved
    );
    assert!(s.begin_post("intent", 1, 4).unwrap());
    drop(s);
    let mut s = AccountStore::open_existing(&p, &m.account_key, &m.installation_id).unwrap();
    assert_eq!(s.recover_posting(5).unwrap(), 1);
    assert_eq!(
        s.intent("intent").unwrap().unwrap().state,
        IntentState::Unknown
    );
    assert!(!s.begin_post("intent", 1, 6).unwrap());
}
#[test]
fn duplicate_execute_one_post() {
    let mut s = store();
    let first = intent(&mut s);
    let second = intent(&mut s);
    assert_eq!(first.intent_id, second.intent_id);
    reserve(&mut s);
    reserve(&mut s);
    let mut posts = 0;
    for _ in 0..3 {
        if s.begin_post("intent", 1, 4).unwrap() {
            posts += 1;
        }
    }
    assert_eq!(posts, 1);
}
#[test]
fn stop_after_reserve_before_post() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    s.latch_stop("stop", "account", "USER_PAUSE", 4).unwrap();
    assert!(s.begin_post("intent", 1, 5).is_err());
    assert_eq!(
        s.intent("intent").unwrap().unwrap().state,
        IntentState::Reserved
    );
}
#[test]
fn lost_response_after_post_returns_unknown() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    assert!(s.begin_post("intent", 1, 4).unwrap());
    s.record_outcome("intent", IntentState::Unknown, None, "timeout", 5)
        .unwrap();
    assert!(!s.begin_post("intent", 1, 6).unwrap());
    let state: String = s
        .connection
        .query_row("SELECT state FROM risk_receipts", [], |r| r.get(0))
        .unwrap();
    assert_eq!(state, "UNKNOWN");
    s.record_outcome(
        "intent",
        IntentState::Filled,
        Some("order"),
        "private-readback",
        7,
    )
    .unwrap();
    assert_eq!(
        s.intent("intent").unwrap().unwrap().state,
        IntentState::Filled
    );
}
#[test]
fn expired_command_never_executes_later() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    assert!(s.begin_post("intent", 1, 100).is_err());
    assert!(s.begin_post("intent", 1, 101).is_err());
}
#[test]
fn corrupt_or_missing_db_never_reinitialized() {
    let missing = path();
    assert!(AccountStore::open_existing(&missing, "account", "installation").is_err());
    assert!(!missing.exists());
    let corrupt = path();
    std::fs::write(&corrupt, b"broken sqlite").unwrap();
    assert!(AccountStore::open_existing(&corrupt, "account", "installation").is_err());
    assert_eq!(std::fs::read(corrupt).unwrap(), b"broken sqlite");
}
#[test]
fn sql_money_overflow_rejected() {
    let mut s = store();
    let f = Fill {
        side: "buy".into(),
        fill_id: "fill".into(),
        instrument_id: "BTC".into(),
        trade_id: "trade".into(),
        exchange_order_id: "order".into(),
        intent_id: None,
        time: 3,
        qty: Decimal::MAX,
        price: Decimal::from(2),
        fee: Decimal::ZERO,
        currency: "USDT".into(),
    };
    assert!(
        s.ingest_page(&IngestPage {
            fills: vec![f],
            ..Default::default()
        })
        .is_err()
    );
}
#[test]
fn history_growth_does_not_expand_hot_read() {
    let mut s = store();
    intent(&mut s);
    let query = "EXPLAIN QUERY PLAN SELECT intent_id FROM intents WHERE state IN('PREPARED','RESERVED','POSTING','UNKNOWN','ACKNOWLEDGED','PARTIALLY_FILLED') ORDER BY state,updated_at,intent_id LIMIT 100";
    let mut q = s.connection.prepare(query).unwrap();
    let plans = q
        .query_map([], |r| r.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        plans.iter().any(|v| v.contains("idx_intents_active")),
        "{plans:?}"
    );
    assert_eq!(s.active_intents(100).unwrap().len(), 1);
}
#[test]
fn campaign_budget_survives_day_restart_and_profit() {
    let mut s = store();
    let e = FinanceEntry {
        entry_id: "loss".into(),
        economic_event_key: "closed-order-1".into(),
        component: "CAMPAIGN_LOSS_USD".into(),
        allocation_leg: "account".into(),
        amount: Decimal::from(3),
        currency: "USD".into(),
        instance_id: None,
        source_ref: "attested:conversion".into(),
        time: 5,
        reverses_entry_id: None,
    };
    s.ingest_page(&IngestPage {
        finance: vec![e.clone()],
        ..Default::default()
    })
    .unwrap();
    s.ingest_page(&IngestPage {
        finance: vec![e],
        ..Default::default()
    })
    .unwrap();
    s.ingest_page(&IngestPage {
        finance: vec![FinanceEntry {
            entry_id: "profit".into(),
            economic_event_key: "profit".into(),
            component: "REALIZED_PNL".into(),
            allocation_leg: "account".into(),
            amount: Decimal::from(50),
            currency: "USD".into(),
            instance_id: None,
            source_ref: "trade2".into(),
            time: 86_400_000,
            reverses_entry_id: None,
        }],
        ..Default::default()
    })
    .unwrap();
    let p = s.path.clone();
    let m = s.meta.clone();
    drop(s);
    let s = AccountStore::open_existing(p, &m.account_key, &m.installation_id).unwrap();
    assert_eq!(s.policy().unwrap().4, Decimal::from(3));
}
#[test]
fn new_format_backup_restore_reconciles() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    s.begin_post("intent", 1, 4).unwrap();
    let backup = path();
    s.backup(&backup).unwrap();
    let mut restored =
        AccountStore::open_existing(&backup, &s.meta.account_key, &s.meta.installation_id).unwrap();
    assert_eq!(restored.recover_posting(5).unwrap(), 1);
    assert!(!restored.begin_post("intent", 1, 6).unwrap());
    restored
        .record_outcome(
            "intent",
            IntentState::Cancelled,
            Some("order"),
            "exchange+central-readback",
            7,
        )
        .unwrap();
    assert_eq!(
        restored.intent("intent").unwrap().unwrap().state,
        IntentState::Cancelled
    );
}
#[test]
fn disk_full_before_post() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    s.connection
        .pragma_update(None, "query_only", "ON")
        .unwrap();
    assert!(s.begin_post("intent", 1, 4).is_err());
    s.connection
        .pragma_update(None, "query_only", "OFF")
        .unwrap();
    assert_eq!(
        s.intent("intent").unwrap().unwrap().state,
        IntentState::Reserved
    );
}
#[test]
fn storage_failure_after_post() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    s.begin_post("intent", 1, 4).unwrap();
    s.connection
        .pragma_update(None, "query_only", "ON")
        .unwrap();
    assert!(
        s.record_outcome("intent", IntentState::Filled, Some("order"), "ack", 5)
            .is_err()
    );
    s.connection
        .pragma_update(None, "query_only", "OFF")
        .unwrap();
    assert_eq!(
        s.intent("intent").unwrap().unwrap().state,
        IntentState::Posting
    );
    assert!(!s.begin_post("intent", 1, 6).unwrap());
}
#[test]
fn daily_roll_does_not_clear_stop() {
    let mut s = store();
    s.latch_stop("drawdown", "account", "LATCHED_DRAWDOWN", 1)
        .unwrap();
    let p = s.path.clone();
    let m = s.meta.clone();
    drop(s);
    let mut s = AccountStore::open_existing(p, &m.account_key, &m.installation_id).unwrap();
    assert_eq!(s.recover_posting(86_400_001).unwrap(), 0);
    assert_eq!(s.stops().unwrap()[0].2, "LATCHED_DRAWDOWN");
}
#[test]
fn external_fill_unattributed_not_zero_risk() {
    let mut s = store();
    let fill = Fill {
        side: "buy".into(),
        fill_id: "external".into(),
        instrument_id: "BTC".into(),
        trade_id: "t".into(),
        exchange_order_id: "o".into(),
        intent_id: None,
        time: 3,
        qty: Decimal::ONE,
        price: Decimal::from(100),
        fee: Decimal::new(-1, 2),
        currency: "USDT".into(),
    };
    s.ingest_attributed_fill(
        fill.clone(),
        FillEffect::Open {
            side: "long".into(),
        },
        None,
    )
    .unwrap();
    s.ingest_attributed_fill(
        fill,
        FillEffect::Open {
            side: "long".into(),
        },
        None,
    )
    .unwrap();
    let lots = s.active_lots("BTC").unwrap();
    assert_eq!(lots.len(), 1);
    assert!(lots[0].instance_id.is_none());
    assert_eq!(lots[0].remaining_qty, Decimal::ONE);
    assert_eq!(
        s.finance_totals(1, 10, None).unwrap()[&("USDT".into(), "FEE".into())],
        Decimal::new(-1, 2)
    );
}
#[test]
fn partial_fill_protection_tracks_size() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    s.begin_post("intent", 1, 4).unwrap();
    let p = ProtectionReceipt {
        intent_id: "intent".into(),
        algo_id: "algo".into(),
        side: "sell".into(),
        quantity: Decimal::new(4, 1),
        trigger_price: Decimal::from(95),
        trigger_basis: "mark".into(),
        verified_at: 5,
        source_digest: "authenticated".into(),
    };
    s.verify_protection(&p).unwrap();
    assert_eq!(
        s.protection("intent").unwrap().unwrap().quantity,
        Decimal::new(4, 1)
    );
    let wrong = ProtectionReceipt {
        side: "buy".into(),
        ..p.clone()
    };
    assert!(s.verify_protection(&wrong).is_err());
    s.verify_protection(&ProtectionReceipt {
        quantity: Decimal::ONE,
        ..p
    })
    .unwrap();
    assert_eq!(
        s.protection("intent").unwrap().unwrap().quantity,
        Decimal::ONE
    );
}
#[test]
fn policy_change_invalidates_unsent_plan() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    s.apply_authority_policy(2, false, "campaign-20", Decimal::from(20))
        .unwrap();
    assert!(s.begin_post("intent", 1, 4).is_err());
    assert_eq!(
        s.intent("intent").unwrap().unwrap().state,
        IntentState::Reserved
    );
}
#[test]
fn initialize_retry_is_exact_and_never_reinitializes() {
    let s = store();
    let p = s.path.clone();
    let m = s.meta.clone();
    drop(s);
    let fresh = OpeningSnapshot {
        source_ref: "changed".into(),
        observed_at: 100,
        positions: 1,
        orders: 0,
        conditional_orders: 0,
        liabilities: Decimal::ZERO,
        balances: vec![("USDT".into(), Decimal::from(999))],
    };
    let s = AccountStore::initialize_or_inspect_exact(&p, m.clone(), &fresh).unwrap();
    assert_eq!(s.meta.initialized_at, 1);
    assert_eq!(s.policy().unwrap().2, "campaign-20");
    drop(s);
    let bad = AccountMeta {
        initialize_operation_id: "different-op".into(),
        ..m
    };
    assert!(AccountStore::initialize_or_inspect_exact(&p, bad, &fresh).is_err());
}
#[test]
fn duplicate_cancel_never_posts_twice() {
    let mut s = store();
    assert!(s.begin_operation("cancel", "request", "CANCEL", 1).unwrap());
    assert!(!s.begin_operation("cancel", "request", "CANCEL", 2).unwrap());
    assert!(
        s.begin_operation("cancel", "different", "CANCEL", 2)
            .is_err()
    );
    s.operation_receipt("cancel", "request", "CANCEL", "CANCELLED", 3)
        .unwrap();
    assert!(!s.begin_operation("cancel", "request", "CANCEL", 4).unwrap());
}
#[tokio::test]
async fn writer_queue_backpressure_and_account_isolation() {
    let s = store();
    let p = s.path.clone();
    let m = s.meta.clone();
    drop(s);
    let h = crate::AccountStoreHandle::open_existing(
        p.clone(),
        m.account_key.clone(),
        m.installation_id.clone(),
    )
    .await
    .unwrap();
    assert!(AccountStore::open_existing(p, &m.account_key, &m.installation_id).is_err());
    let (block_tx, block_rx) = std::sync::mpsc::channel();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let first = h.clone();
    let task = tokio::spawn(async move {
        first
            .call(
                std::time::Instant::now() + Duration::from_secs(5),
                move |_| {
                    let _ = ready_tx.send(());
                    block_rx.recv().unwrap();
                    Ok(())
                },
            )
            .await
    });
    ready_rx.await.unwrap();
    let mut jobs = Vec::new();
    for _ in 0..129 {
        let h = h.clone();
        jobs.push(tokio::spawn(async move {
            h.call(std::time::Instant::now() + Duration::from_secs(5), |_| {
                Ok(())
            })
            .await
        }));
    }
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    let mut other = store();
    intent(&mut other);
    assert_eq!(other.active_intents(10).unwrap().len(), 1);
    block_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
    let mut rejected = 0;
    for job in jobs {
        if matches!(
            job.await.unwrap(),
            Err(StoreError::Invalid("ACCOUNT_STORAGE_BACKPRESSURE"))
        ) {
            rejected += 1;
        }
    }
    assert!(rejected > 0);
}

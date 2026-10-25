use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
static COUNTER: AtomicU64 = AtomicU64::new(0);
#[test]
fn sql_deadline_handler_is_removed_after_operation_error() {
    let mut s = store();
    let result: StoreResult<()> =
        s.run_before_deadline(std::time::Instant::now() + Duration::from_millis(1), |_| {
            std::thread::sleep(Duration::from_millis(3));
            Err(StoreError::Invalid("fixture-operation-failure"))
        });
    assert!(matches!(
        result,
        Err(StoreError::Invalid("fixture-operation-failure"))
    ));
    let total:i64=s.connection.query_row("WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000) SELECT sum(x) FROM n",[],|r|r.get(0)).unwrap();
    assert_eq!(total, 50005000);
}
#[tokio::test]
async fn long_reader_is_interrupted_at_deadline_and_worker_recovers() {
    let s = store();
    let p = s.path.clone();
    let m = s.meta.clone();
    drop(s);
    let h = crate::AccountStoreHandle::open_existing(p, m.account_key, m.installation_id)
        .await
        .unwrap();
    let result=h.call(std::time::Instant::now()+Duration::from_millis(20),|s|{let _:i64=s.connection.query_row("WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT sum(x) FROM n",[],|r|r.get(0))?;Ok(())}).await;
    assert!(matches!(
        result,
        Err(StoreError::Invalid("ACCOUNT_STORAGE_DEADLINE"))
    ));
    assert_eq!(
        h.call(std::time::Instant::now() + Duration::from_secs(2), |s| Ok(
            s.meta.initialized_at
        ))
        .await
        .unwrap(),
        1
    );
}
#[test]
fn persisted_source_generation_survives_restart_without_losing_cursors() {
    let mut s = store();
    assert_eq!(s.source_generation_base().unwrap(), 0);
    let cursor = |generation| IngestPage {
        cursor: Some(SourceCursor {
            source: "fills".into(),
            stream_partition: "SWAP".into(),
            cursor: String::new(),
            watermark: 20,
            generation,
            coverage_gaps: String::new(),
        }),
        ..Default::default()
    };
    s.ingest_page(&cursor(5)).unwrap();
    let path = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let mut recovered =
        AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
    assert_eq!(recovered.source_generation_base().unwrap(), 5);
    assert!(matches!(
        recovered.ingest_page(&cursor(1)),
        Err(StoreError::Invalid("SOURCE_CURSOR_REGRESSION"))
    ));
    recovered.ingest_page(&cursor(6)).unwrap();
    assert_eq!(
        recovered
            .cursor("fills", "SWAP")
            .unwrap()
            .unwrap()
            .generation,
        6
    );
    assert_eq!(recovered.source_generation_base().unwrap(), 6);
}
#[test]
fn missing_fx_stops_risk_and_does_not_commit_zero_loss_or_cursor() {
    let mut s = store();
    let fx = UsdConversionEvidence {
        observed_at: 5,
        source_digest: "source".into(),
        usd_per_currency: Default::default(),
    };
    let page = IngestPage {
        finance: vec![fee("fee", "source")],
        cursor: Some(SourceCursor {
            source: "bills".into(),
            stream_partition: "account".into(),
            cursor: "done".into(),
            watermark: 5,
            generation: 1,
            coverage_gaps: "".into(),
        }),
        ..Default::default()
    };
    assert!(s.ingest_page_with_usd(&page, &fx).is_err());
    assert_eq!(s.stops().unwrap()[0].2, "USD_CONVERSION_SOURCE_UNAVAILABLE");
    assert!(s.cursor("bills", "account").unwrap().is_none());
    assert!(s.finance_history(1, 10, 0, "", 100).unwrap().is_empty());
}
#[test]
fn allocation_and_bar_claim_survive_retry_and_restart() {
    let mut s = store();
    owned_fill(&mut s, "a", "a-order", 100, "long", 3);
    assert!(
        s.claim_strategy_decision("a", "bar-100", "inputs", 1, 5)
            .unwrap()
    );
    assert!(
        s.claim_strategy_decision("a", "bar-100", "inputs", 1, 5)
            .unwrap()
    );
    s.finish_strategy_no_trade("a", "bar-100", 5).unwrap();
    assert!(
        !s.claim_strategy_decision("a", "bar-100", "inputs", 1, 5)
            .unwrap()
    );
    let mut a = s.instances().unwrap().remove(0);
    a.budget = Decimal::from(15);
    a.policy_revision = 2;
    s.mutate_instance("allocation-update", "newbody", "update", &a, Some(1), 6)
        .unwrap();
    s.mutate_instance("allocation-update", "newbody", "update", &a, Some(1), 6)
        .unwrap();
    let amounts = s
        .connection
        .prepare("SELECT delta FROM allocations WHERE instance_id='a' ORDER BY time,event_id")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(amounts, vec!["10", "5"]);
    let p = s.path.clone();
    let m = s.meta.clone();
    drop(s);
    let mut s = AccountStore::open_existing(p, &m.account_key, &m.installation_id).unwrap();
    // A budget-only revision retains the same immutable strategy
    // definition; the terminal bar must not run a second time.
    assert!(
        !s.claim_strategy_decision("a", "bar-100", "inputs", 2, 7)
            .unwrap()
    );
    assert_eq!(
        s.instances().unwrap()[0].last_decision_key.as_deref(),
        Some("bar-100")
    );
}
#[test]
fn nav_window_excludes_deposit_and_reports_pre_t0_unavailable() {
    let mut s = store();
    let fx = UsdConversionEvidence {
        observed_at: 5,
        source_digest: "fx".into(),
        usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::ONE)]),
    };
    s.ingest_page_with_usd(
        &IngestPage {
            cash_flows: vec![CashFlow {
                event_id: "deposit".into(),
                kind: "DEPOSIT".into(),
                currency: "USDT".into(),
                amount: Decimal::from(50),
                time: 5,
                source_ref: "exchange".into(),
            }],
            ..Default::default()
        },
        &fx,
    )
    .unwrap();
    assert_eq!(
        s.usd_component_total("EXTERNAL_FLOW_USD").unwrap(),
        Decimal::from(50)
    );
    s.ingest_page(&IngestPage {
        nav: vec![NavPoint {
            scope_id: "account".into(),
            time: 6,
            source_version: "source".into(),
            equity: Decimal::from(155),
            pnl: Decimal::from(5),
            flows: Decimal::from(50),
            currency: "USD".into(),
            coverage: "COMPLETE".into(),
            mark_source: "source".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    let v = s.nav_summary(1, 6, &[], &["USD".into()]).unwrap();
    assert_eq!(v[0].flows, Some(Decimal::from(50)));
    assert_eq!(v[0].pnl, Some(Decimal::from(5)));
    assert_eq!(
        s.financial_risk(6, None).unwrap().drawdown_ratio,
        Decimal::ZERO
    );
    let before = s.nav_summary(0, 6, &[], &["USD".into()]).unwrap();
    assert!(before[0].opening_equity.is_none());
    assert!(before[0].pnl.is_none());
    assert_eq!(before[0].coverage, "UNAVAILABLE_BEFORE_T0");
}
#[test]
fn funding_opposite_instances_conserves_account_and_keeps_residual_explicit() {
    let mut s = store();
    owned_fill(&mut s, "a", "a1", 100, "long", 3);
    owned_fill(&mut s, "a", "a2", 100, "long", 4);
    owned_fill(&mut s, "b", "b1", 100, "short", 5);
    let fx = UsdConversionEvidence {
        observed_at: 10,
        source_digest: "fx".into(),
        usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::ONE)]),
    };
    s.ingest_page(&IngestPage {
        bills: vec![Bill {
            bill_id: "f".into(),
            kind: "FUNDING".into(),
            time: 9,
            amount: -Decimal::ONE,
            currency: "USDT".into(),
            economic_event_key: "bill:f".into(),
        }],
        funding_events: vec![FundingEvent {
            event_id: "f".into(),
            instrument_id: "BTC".into(),
            time: 9,
            amount: -Decimal::ONE,
            currency: "USDT".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    s.attribute_pending_funding(&fx, 10).unwrap();
    let f = s
        .finance_history(1, 10, 0, "", 100)
        .unwrap()
        .into_iter()
        .filter(|e| e.component == "FUNDING")
        .collect::<Vec<_>>();
    assert_eq!(f.iter().map(|e| e.amount).sum::<Decimal>(), -Decimal::ONE);
    assert_eq!(
        f.iter()
            .find(|e| e.instance_id.as_deref() == Some("a"))
            .unwrap()
            .amount,
        Decimal::from(-2)
    );
    assert_eq!(
        f.iter()
            .find(|e| e.instance_id.as_deref() == Some("b"))
            .unwrap()
            .amount,
        Decimal::ONE
    );
}
fn owned_fill(s: &mut AccountStore, instance: &str, id: &str, price: i64, side: &str, time: i64) {
    let i = StrategyInstance {
        instance_id: instance.into(),
        definition_id: "fixture".into(),
        definition_version: "1".into(),
        parameters: "{}".into(),
        enabled: true,
        policy_revision: 1,
        budget: Decimal::from(10),
        last_decision_key: None,
    };
    s.mutate_instance(&format!("create:{instance}"), "hash", "create", &i, None, 2)
        .unwrap();
    let definition = s.strategy_definition(instance).unwrap();
    let plan = IntentPlan {
        strategy_definition_id: definition.definition_id,
        strategy_definition_version: definition.version,
        strategy_parameters_digest: definition.parameters_digest,
        operation: "open".into(),
        side: if side == "long" { "buy" } else { "sell" }.into(),
        position_side: side.into(),
        quantity: Decimal::ONE,
        limit_price: Decimal::from(price),
        stop_price: None,
        strategy_instance: instance.into(),
        planned_loss_usd: Decimal::ONE,
        source_generation: 1,
        private_source_generation: 1,
        contract_value: Decimal::new(1, 2),
        inverse: false,
        settlement_currency: "USDT".into(),
    };
    let decision = Decision {
        decision_id: format!("d:{id}"),
        instance_id: Some(instance.into()),
        decision_at: 2,
        input_digest: "input".into(),
        policy_revision: 1,
        code_version: "fixture".into(),
    };
    let intent = NewIntent {
        plan,
        intent_id: id.into(),
        idempotency_key: id.into(),
        request_hash: "body".into(),
        client_order_id: id.into(),
        deadline: 1000,
        plan_hash: "plan".into(),
        policy_revision: 1,
        risk_increase: true,
        instrument_id: "BTC".into(),
    };
    s.prepare(&decision, &intent).unwrap();
    s.ingest_attributed_fill(
        Fill {
            side: if side == "long" { "buy" } else { "sell" }.into(),
            fill_id: format!("f:{id}"),
            instrument_id: "BTC".into(),
            trade_id: id.into(),
            exchange_order_id: format!("o:{id}"),
            intent_id: Some(id.into()),
            time,
            qty: Decimal::ONE,
            price: Decimal::from(price),
            fee: Decimal::ZERO,
            currency: "USDT".into(),
        },
        FillEffect::Open { side: side.into() },
        None,
    )
    .unwrap();
}
#[test]
fn instance_nav_uses_each_lot_cost_and_contract_value() {
    let mut s = store();
    owned_fill(&mut s, "a", "a-order", 100, "long", 3);
    owned_fill(&mut s, "b", "b-order", 200, "long", 4);
    let fx = UsdConversionEvidence {
        observed_at: 5,
        source_digest: "usd-mark".into(),
        usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::ONE)]),
    };
    s.record_instance_nav(
        &[VenueMark {
            instrument_id: "BTC".into(),
            price: Decimal::from(300),
            source_ref: "mark".into(),
        }],
        &fx,
        5,
    )
    .unwrap();
    let a: String = s
        .connection
        .query_row(
            "SELECT equity FROM nav_points WHERE scope_id='strategy:a' AND time=5",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let b: String = s
        .connection
        .query_row(
            "SELECT equity FROM nav_points WHERE scope_id='strategy:b' AND time=5",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(a, "12");
    assert_eq!(b, "11");
    s.record_instance_nav(
        &[VenueMark {
            instrument_id: "BTC".into(),
            price: Decimal::from(100),
            source_ref: "mark".into(),
        }],
        &fx,
        6,
    )
    .unwrap();
    let risk = s.financial_risk(6, Some("b")).unwrap();
    assert!(risk.nav_complete);
    assert!(risk.drawdown_ratio > Decimal::ZERO);
}
#[test]
fn funding_at_event_respects_opposite_instances_and_later_fills() {
    let mut s = store();
    owned_fill(&mut s, "a", "a-order", 100, "long", 3);
    owned_fill(&mut s, "b", "b-order", 100, "short", 20);
    let fx = UsdConversionEvidence {
        observed_at: 30,
        source_digest: "fx".into(),
        usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::ONE)]),
    };
    s.ingest_page(&IngestPage {
        bills: vec![Bill {
            bill_id: "funding".into(),
            kind: "FUNDING".into(),
            time: 10,
            amount: -Decimal::ONE,
            currency: "USDT".into(),
            economic_event_key: "bill:funding".into(),
        }],
        funding_events: vec![FundingEvent {
            event_id: "funding".into(),
            instrument_id: "BTC".into(),
            time: 10,
            amount: -Decimal::ONE,
            currency: "USDT".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    assert_eq!(s.attribute_pending_funding(&fx, 100).unwrap(), 1);
    assert_eq!(s.attribute_pending_funding(&fx, 100).unwrap(), 0);
    let rows = s.finance_history(1, 30, 0, "", 100).unwrap();
    let funding = rows
        .iter()
        .filter(|e| e.component == "FUNDING")
        .collect::<Vec<_>>();
    assert_eq!(
        funding.iter().filter(|e| e.amount != Decimal::ZERO).count(),
        1
    );
    assert_eq!(funding[0].instance_id.as_deref(), Some("a"));
    assert_eq!(s.policy().unwrap().4, Decimal::ONE);
}
#[test]
fn utc_daily_risk_rolls_without_replenishing_campaign_or_clearing_stop() {
    let mut s = store();
    let fx = UsdConversionEvidence {
        observed_at: 5,
        source_digest: "fx".into(),
        usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::ONE)]),
    };
    s.ingest_page_with_usd(
        &IngestPage {
            finance: vec![fee("fee", "source")],
            ..Default::default()
        },
        &fx,
    )
    .unwrap();
    assert_eq!(
        s.financial_risk(5, None).unwrap().daily_loss_usd,
        Decimal::new(25, 2)
    );
    s.latch_stop("drawdown", "account", "DRAWDOWN", 5).unwrap();
    assert_eq!(
        s.financial_risk(86_400_005, None).unwrap().daily_loss_usd,
        Decimal::ZERO
    );
    assert_eq!(s.policy().unwrap().4, Decimal::new(25, 2));
    assert_eq!(s.stops().unwrap().len(), 1);
}
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
    tx.execute_batch("WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000000) INSERT INTO intents(intent_id,idempotency_key,request_hash,decision_id,state,revision,client_order_id,deadline,plan_hash,policy_revision,risk_increase,instrument_id,created_at,updated_at) SELECT 'terminal-'||x,'key-'||x,'request-hash','decision','FILLED',4,'client-'||x,100,'plan',1,1,'BTC-USDT-SWAP',2,4 FROM n; INSERT INTO intent_plans SELECT intent_id,'open','buy','1','100','95','manual','5',1,'long',1,'1',0,'USDT',i.definition_id,i.definition_version,i.parameters_digest FROM intents CROSS JOIN strategy_instances i WHERE state='FILLED' AND i.instance_id='manual';").unwrap();
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
        opening_equity_usd: Decimal::from(100),
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
    a.ensure_manual_instance(1).unwrap();
    a
}
fn intent(s: &mut AccountStore) -> StoredIntent {
    let definition = s.ensure_manual_instance(1).unwrap();
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
            strategy_definition_id: definition.definition_id,
            strategy_definition_version: definition.version,
            strategy_parameters_digest: definition.parameters_digest,
            contract_value: Decimal::ONE,
            inverse: false,
            settlement_currency: "USDT".into(),
            position_side: "long".into(),
            private_source_generation: 1,
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
    s.connection.execute_batch("CREATE TABLE fault_padding(bytes BLOB) STRICT; CREATE TEMP TRIGGER inject_full BEFORE UPDATE OF state ON intents WHEN NEW.state='POSTING' BEGIN INSERT INTO fault_padding VALUES(zeroblob(1000000)); END;").unwrap();
    let pages: i64 = s
        .connection
        .pragma_query_value(None, "page_count", |r| r.get(0))
        .unwrap();
    s.connection
        .pragma_update(None, "max_page_count", pages)
        .unwrap();
    let error = s.begin_post("intent", 1, 4).unwrap_err();
    assert!(
        matches!(error,StoreError::Sql(rusqlite::Error::SqliteFailure(e,_))if e.code==rusqlite::ErrorCode::DiskFull)
    );
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
        opening_equity_usd: Decimal::from(100),
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

#[tokio::test]
async fn two_account_100_parallel_sql_readonly_isolation() {
    // Two distinct account keys and actor-owned databases. No exchange transport.
    let first = store();
    let second_meta = AccountMeta {
        account_key: "okx:demo:fixture-account-b".into(),
        installation_id: "fixture-installation-b".into(),
        initialize_operation_id: "fixture-connect-b".into(),
        ..first.meta.clone()
    };
    let opening = OpeningSnapshot {
        opening_equity_usd: Decimal::from(100),
        source_ref: second_meta.opening_source.clone(),
        observed_at: 1,
        positions: 0,
        orders: 0,
        conditional_orders: 0,
        liabilities: Decimal::ZERO,
        balances: vec![("USDT".into(), Decimal::from(100))],
    };
    let second = AccountStore::initialize_flat(path(), second_meta, &opening).unwrap();
    let a_key = first.meta.account_key.clone();
    let b_key = second.meta.account_key.clone();
    assert_ne!(a_key, b_key);
    let (a_path, a_meta) = (first.path.clone(), first.meta.clone());
    let (b_path, b_meta) = (second.path.clone(), second.meta.clone());
    drop(first);
    drop(second);
    let a = crate::AccountStoreHandle::open_existing(
        a_path,
        a_meta.account_key,
        a_meta.installation_id,
    )
    .await
    .unwrap();
    let b = crate::AccountStoreHandle::open_existing(
        b_path,
        b_meta.account_key,
        b_meta.installation_id,
    )
    .await
    .unwrap();
    let start = std::time::Instant::now();
    let mut tasks = Vec::new();
    for index in 0..100 {
        let handle = if index % 2 == 0 { a.clone() } else { b.clone() };
        tasks.push(tokio::spawn(async move {
            let issued = std::time::Instant::now();
            let (key, size) = handle
                .call(std::time::Instant::now() + Duration::from_secs(15), |s| {
                    let summary = s.account_summary()?;
                    let bytes = serde_json::to_vec(&summary).unwrap().len();
                    Ok((s.meta.account_key.clone(), bytes))
                })
                .await
                .unwrap();
            (index, key, size, issued.elapsed().as_micros() as u64)
        }));
    }
    let mut latencies = Vec::new();
    let mut max_bytes = 0;
    for task in tasks {
        let (index, key, bytes, latency) = task.await.unwrap();
        assert_eq!(
            key.as_str(),
            if index % 2 == 0 {
                a_key.as_str()
            } else {
                b_key.as_str()
            }
        );
        assert!(bytes <= 12_288);
        max_bytes = max_bytes.max(bytes);
        latencies.push(latency);
    }
    latencies.sort_unstable();
    println!(
        "NATIVE_SQL_100_PARALLEL_JSON:{}",
        serde_json::json!({
            "type": "native_sql_actors_not_network",
            "os": std::env::consts::OS,
            "accounts": 2,
            "distinct_account_keys": true,
            "operations": 100,
            "real_exchange_posts": 0,
            "p50_us": latencies[49],
            "p95_us": latencies[94],
            "max_us": latencies[99],
            "elapsed_ms": start.elapsed().as_millis(),
            "response_bytes_max": max_bytes,
            "network_p95_measured": false
        })
    );
}

#[test]
fn immutable_strategy_versions_bind_old_intent_and_cas_start_stop() {
    let mut s = store();
    let old = intent(&mut s);
    reserve(&mut s);
    let mut instance = s.instance("manual").unwrap().unwrap();
    let original = s.strategy_definition("manual").unwrap();
    instance.parameters = "{\"kind\":\"manual\",\"threshold\":2}".into();
    instance.policy_revision = 2;
    let receipt = s
        .mutate_instance(
            "update-version",
            "hash-update",
            "update",
            &instance,
            Some(1),
            4,
        )
        .unwrap();
    let updated: StrategyInstance = serde_json::from_str(&receipt.result_ref).unwrap();
    assert_ne!(updated.definition_version, original.version);
    assert_eq!(
        s.intent(&old.intent_id)
            .unwrap()
            .unwrap()
            .plan
            .strategy_definition_version,
        original.version
    );
    assert_eq!(
        s.definition_version(&original.definition_id, &original.version)
            .unwrap()
            .unwrap(),
        original
    );
    assert!(matches!(
        s.begin_post(&old.intent_id, 1, 5),
        Err(StoreError::Invalid("STRATEGY_VERSION_CHANGED"))
    ));
    assert!(
        s.connection
            .execute(
                "UPDATE strategy_definition_versions SET parameters='{}'",
                []
            )
            .is_err()
    );
    let mut started = updated.clone();
    started.policy_revision = 3;
    started.enabled = true;
    let start = s
        .mutate_instance("start-version", "hash-start", "start", &started, Some(2), 6)
        .unwrap();
    assert_eq!(s.instance("manual").unwrap().unwrap().policy_revision, 3);
    assert!(
        s.mutate_instance(
            "stale-update",
            "hash-stale",
            "update",
            &instance,
            Some(2),
            7
        )
        .is_err()
    );
    assert_eq!(
        s.mutate_instance("start-version", "hash-start", "start", &started, Some(2), 7)
            .unwrap()
            .result_ref,
        start.result_ref
    );
    assert!(
        s.claim_strategy_decision("manual", "same-bar", "input", 1, 7)
            .unwrap()
    );
    let mut changed = s.instance("manual").unwrap().unwrap();
    changed.parameters = "{\"kind\":\"manual\",\"threshold\":3}".into();
    changed.policy_revision = 4;
    s.mutate_instance(
        "update-second",
        "hash-second",
        "update",
        &changed,
        Some(3),
        8,
    )
    .unwrap();
    assert!(
        s.claim_strategy_decision("manual", "same-bar", "input", 1, 9)
            .unwrap()
    );
    let mut stopped = s.instance("manual").unwrap().unwrap();
    stopped.policy_revision = 5;
    s.mutate_instance("stop-version", "hash-stop", "stop", &stopped, Some(4), 10)
        .unwrap();
    assert_eq!(s.instance("manual").unwrap().unwrap().policy_revision, 5);
    assert!(!s.instance("manual").unwrap().unwrap().enabled);
    let status = s.strategy_status("manual", 10).unwrap();
    assert_eq!(status["limits"]["policy_revision"], 5);
    assert!(status["positions"].as_array().unwrap().is_empty());
}

#[test]
fn strategy_status_bounds_owned_positions_and_uses_instance_index() {
    let s = store();
    for n in 0..51 {
        s.connection.execute("INSERT INTO position_lots VALUES(?1,'manual',NULL,NULL,'BTC-USDT-SWAP','long','1','100',?2)",params![format!("lot-{n:03}"),n]).unwrap();
    }
    let status = s.strategy_status("manual", 60).unwrap();
    assert_eq!(status["positions"].as_array().unwrap().len(), 50);
    assert_eq!(status["positions_partial"], true);
    assert_eq!(status["positions"][0]["instance_id"], "manual");
    let plan:String=s.connection.query_row("EXPLAIN QUERY PLAN SELECT lot_id FROM position_lots WHERE instance_id='manual' AND remaining_qty<>'0' ORDER BY instrument_id,side,opened_at,lot_id LIMIT 51",[],|r|r.get(3)).unwrap();
    assert!(plan.contains("idx_lots_instance_active"));
}

#[test]
fn recovery_history_age_is_separate_from_current_evidence_freshness() {
    let mut s = store();
    let now = 20000;
    for (source, part) in [
        ("fills", "SPOT"),
        ("fills", "SWAP"),
        ("fills", "FUTURES"),
        ("fills", "OPTION"),
        ("bills", "account"),
        ("recovery_audit", "account"),
        ("orders", "account"),
        ("account_snapshot", "account"),
    ] {
        let watermark = if matches!(source, "orders" | "account_snapshot") {
            now
        } else {
            10000
        };
        s.ingest_page(&IngestPage {
            cursor: Some(SourceCursor {
                source: source.into(),
                stream_partition: part.into(),
                cursor: String::new(),
                watermark,
                generation: 1,
                coverage_gaps: String::new(),
            }),
            ..Default::default()
        })
        .unwrap();
    }
    assert!(s.recovery_snapshot(now).unwrap().coverage_complete);
    s.connection
        .execute(
            "UPDATE source_cursors SET watermark=9999 WHERE source='bills'",
            [],
        )
        .unwrap();
    assert!(!s.recovery_snapshot(now).unwrap().coverage_complete);
    s.connection
        .execute(
            "UPDATE source_cursors SET watermark=10000,generation=2 WHERE source='bills'",
            [],
        )
        .unwrap();
    assert!(!s.recovery_snapshot(now).unwrap().coverage_complete);
    s.connection
        .execute(
            "UPDATE source_cursors SET generation=1,coverage_gaps='missing' WHERE source='bills'",
            [],
        )
        .unwrap();
    assert!(!s.recovery_snapshot(now).unwrap().coverage_complete);
    s.connection
        .execute(
            "UPDATE source_cursors SET coverage_gaps='' WHERE source='bills'",
            [],
        )
        .unwrap();
    assert!(s.recovery_snapshot(now).unwrap().coverage_complete);
    assert!(!s.recovery_snapshot(now + 5001).unwrap().coverage_complete);
}

#[test]
fn strategy_crash_pending_decision_to_intent() {
    let mut s = store();
    owned_fill(&mut s, "a", "fixture-filled", 100, "long", 3);
    let version = s.strategy_definition("a").unwrap();
    assert!(
        s.claim_strategy_decision("a", "bar-next", "inputs-next", 1, 7)
            .unwrap()
    );
    // Simulate a process crash before the plan and intent were committed.
    let p = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let mut s = AccountStore::open_existing(p, &meta.account_key, &meta.installation_id).unwrap();
    assert!(
        s.claim_strategy_decision("a", "bar-next", "inputs-next", 1, 8)
            .unwrap()
    );
    assert!(
        s.instances()
            .unwrap()
            .iter()
            .find(|i| i.instance_id == "a")
            .unwrap()
            .last_decision_key
            .as_deref()
            != Some("bar-next")
    );
    let plan = IntentPlan {
        strategy_definition_id: version.definition_id.clone(),
        strategy_definition_version: version.version.clone(),
        strategy_parameters_digest: version.parameters_digest.clone(),
        operation: "open".into(),
        side: "buy".into(),
        position_side: "long".into(),
        quantity: Decimal::ONE,
        limit_price: Decimal::from(100),
        stop_price: Some(Decimal::from(90)),
        strategy_instance: "a".into(),
        planned_loss_usd: Decimal::ONE,
        source_generation: 1,
        private_source_generation: 1,
        contract_value: Decimal::ONE,
        inverse: false,
        settlement_currency: "USDT".into(),
    };
    let decision = Decision {
        decision_id: format!("strategy:a:{}:bar-next", version.version),
        instance_id: Some("a".into()),
        decision_at: 9,
        input_digest: "inputs-next".into(),
        policy_revision: 1,
        code_version: "strategy-bar/v1".into(),
    };
    let intent = NewIntent {
        plan,
        intent_id: "f07-next".into(),
        idempotency_key: "bar-next".into(),
        request_hash: "f07-body".into(),
        client_order_id: "f07-client".into(),
        deadline: 1000,
        plan_hash: "f07-plan".into(),
        policy_revision: 1,
        risk_increase: true,
        instrument_id: "BTC".into(),
    };
    s.prepare(&decision, &intent).unwrap();
    let decisions: i64 = s
        .connection
        .query_row(
            "SELECT count(*) FROM decisions WHERE decision_id=?1",
            [&decision.decision_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(decisions, 1);
    let bound: String = s
        .connection
        .query_row(
            "SELECT decision_id FROM intents WHERE idempotency_key='bar-next'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(bound, decision.decision_id);
    assert!(
        !s.claim_strategy_decision("a", "bar-next", "inputs-next", 1, 10)
            .unwrap()
    );
    assert_eq!(
        s.instances()
            .unwrap()
            .iter()
            .find(|i| i.instance_id == "a")
            .unwrap()
            .last_decision_key
            .as_deref(),
        Some("bar-next")
    );
}

#[test]
fn strategy_policy_retry_keeps_immutable_lineage_and_one_intent() {
    let mut s = store();
    owned_fill(&mut s, "a", "fixture-filled", 100, "long", 3);
    let definition = s.strategy_definition("a").unwrap();
    let base = format!("strategy:a:{}:bar-revision", definition.version);
    assert_eq!(
        s.claim_strategy_decision_with_id("a", "bar-revision", "inputs", 1, 7)
            .unwrap(),
        Some(base.clone())
    );
    let p = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let mut s = AccountStore::open_existing(p, &meta.account_key, &meta.installation_id).unwrap();
    // Revision changes after durable claim but before any intent or POST.
    let updated = s
        .claim_strategy_decision_with_id("a", "bar-revision", "inputs", 2, 8)
        .unwrap()
        .unwrap();
    assert_ne!(updated, base);
    assert!(updated.contains(":policy:2"));
    assert_eq!(
        s.claim_strategy_decision_with_id("a", "bar-revision", "inputs", 2, 9)
            .unwrap(),
        Some(updated.clone())
    );
    let old: (String, i64) = s
        .connection
        .query_row(
            "SELECT input_digest,policy_revision FROM decisions WHERE decision_id=?1",
            [&base],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(old, ("inputs".into(), 1));
    assert!(
        s.connection
            .execute(
                "UPDATE decisions SET policy_revision=2 WHERE decision_id=?1",
                [&base],
            )
            .is_err()
    );
    let successor: String = s.connection.query_row(
        "SELECT result_ref FROM operation_receipts WHERE operation_id=?1 AND kind='strategy_policy_retry'",
        [format!("strategy-policy-retry:{base}")],
        |r| r.get(0),
    ).unwrap();
    assert_eq!(successor, updated);
    // A third central revision appends again, never mutating either ancestor.
    let final_id = s
        .claim_strategy_decision_with_id("a", "bar-revision", "inputs", 3, 10)
        .unwrap()
        .unwrap();
    assert_ne!(final_id, updated);
    let plan = IntentPlan {
        strategy_definition_id: definition.definition_id,
        strategy_definition_version: definition.version,
        strategy_parameters_digest: definition.parameters_digest,
        operation: "open".into(),
        side: "buy".into(),
        position_side: "long".into(),
        quantity: Decimal::ONE,
        limit_price: Decimal::from(100),
        stop_price: Some(Decimal::from(90)),
        strategy_instance: "a".into(),
        planned_loss_usd: Decimal::ONE,
        source_generation: 1,
        private_source_generation: 1,
        contract_value: Decimal::ONE,
        inverse: false,
        settlement_currency: "USDT".into(),
    };
    let decision = Decision {
        decision_id: final_id.clone(),
        instance_id: Some("a".into()),
        decision_at: 11,
        input_digest: "inputs".into(),
        policy_revision: 3,
        code_version: "strategy-bar/policy-retry-v1".into(),
    };
    let intent = NewIntent {
        plan,
        intent_id: "f07-revision".into(),
        idempotency_key: "bar-revision".into(),
        request_hash: "f07-revision-body".into(),
        client_order_id: "f07-revision-client".into(),
        deadline: 1000,
        plan_hash: "f07-revision-plan".into(),
        policy_revision: 3,
        risk_increase: true,
        instrument_id: "BTC".into(),
    };
    s.prepare(&decision, &intent).unwrap();
    let count: i64 = s
        .connection
        .query_row(
            "SELECT count(*) FROM intents WHERE idempotency_key='bar-revision'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
    assert!(
        !s.claim_strategy_decision("a", "bar-revision", "inputs", 4, 12)
            .unwrap()
    );
    assert_eq!(
        s.connection
            .query_row(
                "SELECT decision_id FROM intents WHERE idempotency_key='bar-revision'",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap(),
        final_id
    );
    // The SQL owner has prepared exactly one intent and no authorized POST.
    assert!(
        s.connection
            .query_row(
                "SELECT begin_post_at FROM intents WHERE idempotency_key='bar-revision'",
                [],
                |r| r.get::<_, Option<i64>>(0),
            )
            .unwrap()
            .is_none()
    );
    let decisions: i64 = s
        .connection
        .query_row(
            "SELECT count(*) FROM decisions WHERE decision_id=?1 OR
         (decision_id >= (?1 || ':retry:') AND decision_id < (?1 || ':retry;'))",
            [&base],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(decisions, 3);
}

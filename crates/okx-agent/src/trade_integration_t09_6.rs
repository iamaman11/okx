// T09.6 continuation: two lots owned by two separate entry intents must be
// closed through the one public TradeService, never by writing SQL positions
// or creating a private synthetic close engine.
struct T09_6CloseContext {
    trade: Arc<TradeService>,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    opening_intent_id: String,
    operation_key: String,
    quantity: &'static str,
    venue_order_id: &'static str,
    expected_post_count: usize,
}

async fn t09_6_close_owned_lot(input: T09_6CloseContext) -> serde_json::Value {
    use crate::accounts::recorded_support::{RecordedGet, RecordedPost};
    use okx_protocol::trading::TradeRequest;
    let T09_6CloseContext {
        trade, feed, opening_intent_id, operation_key, quantity,
        venue_order_id, expected_post_count,
    } = input;
    let at=chrono::Utc::now().timestamp_millis().to_string();
    let raw=|v|serde_json::to_vec(&serde_json::json!({
        "code":"0","msg":"","data":v
    })).expect("signed test HTTP envelope");
    feed.advance_recorded_public_clock(raw(serde_json::json!([{"ts":at}])));
    feed.expect_post(RecordedPost {
        access_key:"fixture-executor-a".into(),
        path:"/api/v5/trade/order".into(),
        expected_body:serde_json::json!({
            "instId":"BTC-USDT-SWAP","tdMode":"cross",
            "clOrdId":"__REQUEST_CL_ORD_ID__","side":"sell",
            "posSide":"long","ordType":"ioc","sz":quantity,"px":"999"
        }),
        raw_response:raw(serde_json::json!([{
            "ordId":venue_order_id,"clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"
        }])),
    });
    feed.expect_get(RecordedGet {
        access_key:"fixture-executor-a".into(),
        path:"/api/v5/trade/order".into(),
        required_query:vec![("instId".into(),"BTC-USDT-SWAP".into())],
        raw_response:raw(serde_json::json!([{
            "instId":"BTC-USDT-SWAP","ordId":venue_order_id,
            "clOrdId":"__REQUEST_CL_ORD_ID__","side":"sell","posSide":"long",
            "tdMode":"cross","ordType":"ioc","px":"999","sz":quantity,
            "accFillSz":"0","avgPx":"","state":"live","cTime":at,"uTime":at
        }])),
    });
    let close=trade.trade("fixture-a",&TradeRequest::Close {
        idempotency_key:operation_key,
        reference:opening_intent_id.clone(),
        expires_in_ms:30_000,
    },None).await.unwrap_or_else(|reason|panic!(
        "T09_6_PUBLIC_CLOSE_DENIED:source_intent={opening_intent_id}:quantity={quantity}:reason={reason}:posts={:?}",
        feed.outbound_posts.lock().unwrap().len()
    ));
    let intent=&close["data"]["intent"];
    assert_eq!(intent["exchange_order_id"],venue_order_id,
        "T09.6 requires exact authenticated venue order ACK");
    assert_eq!(intent["plan"]["quantity"],quantity,
        "T09.6 must reduce only the original own lot quantity");
    let sends=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(sends,expected_post_count,
        "T09.6 public Close must send exactly one create per separately owned lot");
    assert_eq!(feed.unconsumed_posts(),0,
        "T09.6 exact signed close response must have been consumed");
    serde_json::json!({
        "source_intent_id":opening_intent_id,
        "close_intent_id":intent["intent_id"],
        "venue_order_id":venue_order_id,
        "quantity":quantity,
        "post_count":sends,
        "client_order_id":intent["client_order_id"],
        "close_at_ms":at,
        "status":close["status"],
        "financial_complete":close["data"]["financial_complete"],
    })
}
// The only simulated material is immutable raw OKX HTTP history and signed
// portfolio snapshots. Native capture owns inserts, cursors and PnL; no
// fixture is allowed to write a SQL money fact or Worker risk directly.
struct T09_6TerminalSources {
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    first_open_at_ms: String,
    first_close_at_ms: String,
    second_first_at_ms: i64,
    second_last_at_ms: i64,
    add_fill_at_ms: i64,
    closes: Vec<serde_json::Value>,
}
fn t09_6_publish_terminal_sources(input: T09_6TerminalSources) -> i64 {
    use crate::accounts::recorded_support::RecordedGet;
    let T09_6TerminalSources {
        feed, first_open_at_ms, first_close_at_ms, second_first_at_ms,
        second_last_at_ms, add_fill_at_ms, closes,
    }=input;
    assert_eq!(closes.len(),2,"T09.6 requires two public Close ACKs");
    let ms=|v:&serde_json::Value|v["close_at_ms"].as_str()
        .expect("T09.6 close timestamp").parse::<i64>()
        .expect("T09.6 valid close timestamp");
    let final_at=ms(&closes[0]).max(ms(&closes[1]));
    let wrap=|v|serde_json::to_vec(&serde_json::json!({
        "code":"0","msg":"","data":v
    })).expect("T09.6 serializable recorded venue bytes");
    let mut fills=Vec::new();
    let mut bills=Vec::new();
    let first_open=first_open_at_ms.parse::<i64>().expect("old opening timestamp");
    let first_close=first_close_at_ms.parse::<i64>().expect("old Close timestamp");
    // Immutable old events retain their original timestamp and bill identity.
    // Both new IOC limits at 999 receive the BETTER valid fill price 1000.
    for (event,ord_id,side,subtype,sz,fee,ts) in [
        ("t09-entry-001","recorded-t09-opening-001","buy","3","0.1","-0.0005",first_open),
        ("t09-first-close-001","recorded-t09-close-001","sell","5","0.1","-0.0005",first_close),
        ("t09-second-001","recorded-t09-second-001","buy","3","0.12","-0.0006",second_first_at_ms),
        ("t09-second-002","recorded-t09-second-001","buy","3","0.18","-0.0009",second_last_at_ms),
        ("t09-managed-add-001","recorded-t09-add-001","buy","3","0.2","-0.001",add_fill_at_ms),
        ("t09-managed-close-030","recorded-t09-close-030","sell","5","0.3","-0.0015",ms(&closes[0])),
        ("t09-managed-close-020","recorded-t09-close-020","sell","5","0.2","-0.001",ms(&closes[1])),
    ] {
        let (trade_id,bill_id)=match event {
            // Preserve precise identities already committed in accepted T09.1.
            "t09-entry-001"=>("t09-entry-trade-001".to_owned(),"t09-entry-bill-001".to_owned()),
            "t09-first-close-001"=>("t09-close-trade-001".to_owned(),"t09-close-bill-001".to_owned()),
            name=>(format!("{name}-trade"),format!("{name}-bill")),
        };
        fills.push(serde_json::json!({
            "instType":"SWAP","instId":"BTC-USDT-SWAP","tradeId":trade_id,
            "ordId":ord_id,"clOrdId":"__REQUEST_CL_ORD_ID__","billId":bill_id,
            "subType":subtype,"side":side,"posSide":"long",
            "fillPx":"1000","fillSz":sz,"fillPnl":"0","fee":fee,"feeCcy":"USDT",
            "ts":ts.to_string(),"fillTime":ts.to_string()
        }));
        bills.push(serde_json::json!({
            "billId":bill_id,"instType":"SWAP","instId":"BTC-USDT-SWAP",
            "ordId":ord_id,"tradeId":trade_id,"clOrdId":"__REQUEST_CL_ORD_ID__",
            "ccy":"USDT","type":"2","subType":subtype,
            "balChg":fee,"fee":fee,"pnl":"0","ts":ts.to_string()
        }));
    }
    let mut pages=Vec::new();
    for _ in 0..40 {
        for partition in ["SPOT","MARGIN","SWAP","FUTURES","OPTION"] {
            pages.push(RecordedGet {
                access_key:"fixture-executor-a".into(),
                path:"/api/v5/trade/fills-history".into(),
                required_query:vec![("instType".into(),partition.into())],
                raw_response:if partition=="SWAP" {wrap(serde_json::json!(fills))}
                    else {wrap(serde_json::json!([]))}
            });
        }
    }
    feed.advance_recorded_route("fixture-executor-a","/api/v5/trade/fills-history",pages);
    feed.advance_recorded_route("fixture-executor-a","/api/v5/account/bills-archive",
        (0..40).map(|_|RecordedGet {
            access_key:"fixture-executor-a".into(),
            path:"/api/v5/account/bills-archive".into(),
            required_query:vec![],
            raw_response:wrap(serde_json::json!(bills)),
        }).collect()
    );
    feed.advance_recorded_route("fixture-observer-a","/api/v5/account/positions",
        (0..80).map(|_|RecordedGet {
            access_key:"fixture-observer-a".into(),
            path:"/api/v5/account/positions".into(),
            required_query:vec![],
            raw_response:wrap(serde_json::json!([])),
        }).collect()
    );
    let mut orders=Vec::new();
    for _ in 0..24 {
        for (order,sz,side,order_type,px,at) in [
            ("recorded-t09-second-001","0.3","buy","limit","1000",second_last_at_ms),
            ("recorded-t09-add-001","0.2","buy","limit","1000",add_fill_at_ms),
            ("recorded-t09-close-030","0.3","sell","ioc","999",ms(&closes[0])),
            ("recorded-t09-close-020","0.2","sell","ioc","999",ms(&closes[1])),
        ] {
            orders.push(RecordedGet {
                access_key:"fixture-executor-a".into(),
                path:"/api/v5/trade/order".into(),
                required_query:vec![("instId".into(),"BTC-USDT-SWAP".into())],
                raw_response:wrap(serde_json::json!([{
                    "instId":"BTC-USDT-SWAP","ordId":order,
                    "clOrdId":"__REQUEST_CL_ORD_ID__","side":side,
                    "posSide":"long","tdMode":"cross","ordType":order_type,
                    "px":px,"sz":sz,"accFillSz":sz,"avgPx":"1000",
                    "state":"filled","cTime":at.to_string(),"uTime":at.to_string()
                }])),
            });
        }
    }
    feed.advance_recorded_route("fixture-executor-a","/api/v5/trade/order",orders);
    final_at
}
// A signed two-snapshot inventory is delivered around exactly one native
// cancel-algos POST. Stage 1 still has BOTH real protective orders, stage 2
// has only the other live stop; after the second cancel, stage 2 is empty.
// No order is canceled by the fixture, only production TradeService can send.
fn t09_6_cleanup_recorded_transport(
    feed: &crate::accounts::recorded_support::RecordedAccountHttp,
    cancel_id: &str,
    other_stop_live: bool,
    second_client_id: &str,
    add_client_id: &str,
) {
    use crate::accounts::recorded_support::{RecordedGet,RecordedPost};
    assert!(second_client_id.len() >= 31 && add_client_id.len() >= 31,
        "T09.6 must use durable native client order identities");
    let wrap=|v|serde_json::to_vec(&serde_json::json!({
        "code":"0","msg":"","data":v
    })).expect("T09.6 signed algo fixture bytes");
    let mut pages=Vec::new();
    // Pre-cancel, post-cancel verification, then independently signed
    // post-cancel census reads for the terminal central RELEASE. Keep finite
    // immutable pages: missing unexpected scans still fail closed.
    for phase in 0..4 {
        for inst_type in ["SWAP","FUTURES"] {
            for kind in [
                "conditional","oco","trigger","move_order_stop",
                "chase","iceberg","twap","smart_iceberg"
            ] {
                let mut present=Vec::new();
                if inst_type=="SWAP" && kind=="conditional" {
                    // An algoId without the attached client-order identity,
                    // signed mechanics and exact quantity is UNATTRIBUTED,
                    // never evidence of an owned managed stop or safe RELEASE.
                    for (algo, client_id, sz) in [
                        ("900000002",second_client_id,"0.3"),
                        ("900000003",add_client_id,"0.2"),
                    ] {
                        if (phase == 0 && algo == cancel_id)
                            || (other_stop_live && algo != cancel_id)
                        {
                            present.push(serde_json::json!({
                                "instType":"SWAP","instId":"BTC-USDT-SWAP",
                                "ordType":"conditional","algoId":algo,
                                "algoClOrdId":format!("s{}",&client_id[..31]),
                                "state":"live","side":"sell","posSide":"long",
                                "sz":sz,"tdMode":"cross","slTriggerPx":"900",
                                "slTriggerPxType":"mark","slOrdPx":"-1",
                            }));
                        }
                    }
                }
                pages.push(RecordedGet {
                    access_key:"fixture-observer-a".into(),
                    path:"/api/v5/trade/orders-algo-pending".into(),
                    required_query:vec![
                        ("instType".into(),inst_type.into()),
                        ("ordType".into(),kind.into()),
                        ("limit".into(),"100".into()),
                    ],
                    raw_response:wrap(serde_json::json!(present)),
                });
            }
        }
    }
    feed.advance_recorded_route("fixture-observer-a",
        "/api/v5/trade/orders-algo-pending",pages);
    feed.advance_recorded_public_clock(wrap(serde_json::json!([{"ts":chrono::Utc::now().timestamp_millis().to_string()}])));
    feed.expect_post(RecordedPost {
        access_key:"fixture-executor-a".into(),
        path:"/api/v5/trade/cancel-algos".into(),
        expected_body:serde_json::json!([{
            "instId":"BTC-USDT-SWAP","algoId":cancel_id
        }]),
        raw_response:wrap(serde_json::json!([{
            "algoId":cancel_id,"sCode":"0"
        }])),
    });
}

fn t09_6_publish_final_empty_algos(
    feed: &crate::accounts::recorded_support::RecordedAccountHttp
) {
    use crate::accounts::recorded_support::RecordedGet;
    let empty=serde_json::to_vec(&serde_json::json!({
        "code":"0","msg":"","data":[]
    })).expect("T09.6 empty venue inventory raw JSON");
    let mut pages=Vec::new();
    // Every public Status independently scans all 16 signed derivative
    // scopes, as does the final orphan check. These are finite raw GETs,
    // not a second account owner or a risk-census shortcut.
    for _ in 0..12 {
        for inst in ["SWAP","FUTURES"] {
            for kind in [
                "conditional","oco","trigger","move_order_stop",
                "chase","iceberg","twap","smart_iceberg"
            ] {
                pages.push(RecordedGet {
                    access_key:"fixture-observer-a".into(),
                    path:"/api/v5/trade/orders-algo-pending".into(),
                    required_query:vec![
                        ("instType".into(),inst.into()),("ordType".into(),kind.into()),
                        ("limit".into(),"100".into()),
                    ],
                    raw_response:empty.clone(),
                });
            }
        }
    }
    feed.advance_recorded_route("fixture-observer-a",
        "/api/v5/trade/orders-algo-pending",pages);
}
// Repeat a public read-only Status only when the shared production
// RateBudget proves a LOCAL, NO-SEND private GET defer. The same captured
// financial sources and the same release owner remain authoritative.
async fn t09_6_status_after_local_read_defer(
    trade: &Arc<TradeService>,
    rate_budget: &RateBudget,
    feed: &Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    intent: &str,
    allowed_new_cancel_posts: usize,
) -> serde_json::Value {
    use okx_protocol::trading::TradeRequest;
    assert!(allowed_new_cancel_posts <= 1, "one protective cancel per Status");
    let until = std::time::Instant::now() + Duration::from_secs(15);
    let posts_before = feed.outbound_posts.lock().unwrap().len();
    loop {
        let result = tokio::time::timeout_at(
            until.into(),
            trade.trade(
                "fixture-a",
                &TradeRequest::Status { reference: intent.to_owned() },
                None,
            ),
        )
        .await
        .expect("T09.6 bounded source-backed status");
        let posts_now = feed.outbound_posts.lock().unwrap().len();
        assert!(
            posts_now >= posts_before && posts_now <= posts_before + allowed_new_cancel_posts,
            "T09.6 Status must not create or repeat an unrelated venue POST: before={posts_before} now={posts_now} allowed_cancel={allowed_new_cancel_posts}"
        );
        let local_reason = match &result {
            Ok(status) => status["data"]["financial_error"]
                .as_str()
                .and_then(|e| e.strip_prefix("OKX API error: ")),
            Err(reason) => Some(reason.as_str()),
        };
        let remaining = until.saturating_duration_since(std::time::Instant::now());
        if let Some(delay) = local_reason.and_then(|reason| {
            recorded_preview_local_defer(reason, rate_budget, remaining)
        }) {
            tokio::time::timeout_at(until.into(), tokio::time::sleep(delay))
                .await
                .expect("T09.6 authorized read defer expired");
            continue;
        }
        let status = result.unwrap_or_else(|reason| {
            panic!("T09_6_STATUS_NONLOCAL_ERROR:{intent}:{reason}")
        });
        assert_eq!(posts_now, posts_before + allowed_new_cancel_posts,
            "T09.6 confirmed exactly one cancel POST when cleanup is due, or zero for close readback");
        return status;
    }
}

// A typed read-only test context keeps one owner for each financial
// boundary and avoids an expanding eight-argument terminal helper.
struct T09_6FinalizeContext<'a> {
    accounts: &'a Arc<AccountRuntimeRegistry>,
    trade: &'a Arc<TradeService>,
    risk: &'a NativeRiskClient,
    rate_budget: &'a RateBudget,
    feed: &'a Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    second_entry_id: &'a str,
    add_entry_id: &'a str,
    closes: &'a [serde_json::Value],
}
async fn t09_6_source_backed_close_release(
    ctx: T09_6FinalizeContext<'_>,
) -> serde_json::Value {
    let T09_6FinalizeContext {
        accounts, trade, risk, rate_budget, feed, second_entry_id,
        add_entry_id, closes,
    } = ctx;
    let mut close_results=Vec::new();    for close in closes {
        let intent=close["close_intent_id"].as_str()
            .expect("T09.6 real close intent");
        let status=t09_6_status_after_local_read_defer(
            trade,rate_budget,feed,intent,0
        ).await;
        assert_eq!(status["data"]["intent"]["state"],"FILLED",
            "T09.6 close must be terminal from signed venue GET");
        assert_eq!(status["data"]["central_release"]["state"],"verified",
            "T09.6 reducing Close must discharge its native terminal receipt; financial_error={}, financial_complete={}, release={}, protection_incident={}",
            status["data"]["financial_error"],status["data"]["financial_complete"],
            status["data"]["central_release"],status["data"]["protection_incident"]);
        close_results.push(serde_json::json!({
            "intent_id":intent,"state":status["data"]["intent"]["state"],
            "release":status["data"]["central_release"],
            "financial_complete":status["data"]["financial_complete"],
        }));
    }
    let owner=accounts.resolve("fixture-a").await.expect("T09.6 Native SQL owner");
    let second_sql_id = second_entry_id.to_owned();
    let add_sql_id = add_entry_id.to_owned();
    let (second_client_id,add_client_id)=owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        move |s| {
            let second=s.intent(&second_sql_id)?
                .ok_or(okx_execution::StoreError::Invalid("T09_6_SECOND_INTENT_MISSING"))?;
            let add=s.intent(&add_sql_id)?
                .ok_or(okx_execution::StoreError::Invalid("T09_6_ADD_INTENT_MISSING"))?;
            Ok((second.client_order_id,add.client_order_id))
        }
    ).await.expect("T09.6 stop attribution from persisted account owner");
    let mut entry_results=Vec::new();
    for (intent,cancel_algo,other_stop_live) in [
        (second_entry_id,"900000002",true),
        (add_entry_id,"900000003",false),
    ] {
        t09_6_cleanup_recorded_transport(
            feed,cancel_algo,other_stop_live,&second_client_id,&add_client_id
        );
        let status=t09_6_status_after_local_read_defer(
            trade,rate_budget,feed,intent,1
        ).await;
        assert_eq!(status["data"]["intent"]["state"],"FILLED",
            "T09.6 original entry must remain FILLED after close");
        // A full-cohort census must not RELEASE the first flat entry
        // while the other attached conditional stop is still live.
        // The second stop is cancelled through the same public Status path,
        // never by a fixture-owned mutation or a risk-policy exemption.
        if other_stop_live {
            assert_eq!(status["data"]["central_release"]["state"],"pending",
                "T09.6 first stop cancel must not release risk while another stop is live");
            assert_eq!(
                status["data"]["central_release"]["reason_code"],
                "UNATTRIBUTED_CONDITIONAL_ORDER",
                "T09.6 the independent cohort census must block the orphan live stop"
            );
        }
        assert_eq!(status["data"]["protection_incident"],false,
            "T09.6 post-close risk must not hide a protection incident");
        entry_results.push(serde_json::json!({
            "intent_id":intent,
            "cancel_phase_release":status["data"]["central_release"],
            "financial_complete":status["data"]["financial_complete"],
        }));
    }
    // Only after BOTH real cancel-algos ACKs may a fresh signed all-scope
    // empty inventory authorize either central RELEASE. The first Status
    // need not be repeated as a mutation: it was a one-shot cancel.
    t09_6_publish_final_empty_algos(feed);
    for entry in &mut entry_results {
        let intent=entry["intent_id"].as_str().expect("T09.6 persisted entry id");
        let status=t09_6_status_after_local_read_defer(
            trade,rate_budget,feed,intent,0
        ).await;
        assert_eq!(status["data"]["intent"]["state"],"FILLED",
            "T09.6 the original order remains terminal after both stop cancels");
        assert_eq!(status["data"]["central_release"]["state"],"verified",
            "T09.6 release after BOTH stop cancels must be verified: intent={}, release={}, incremental={}, entry_fill={}, financial_error={}, remaining={}, protection={}",
            intent,status["data"]["central_release"],status["data"]["incremental_entry_settlement"],
            status["data"]["entry_fill_evidence"],status["data"]["financial_error"],
            status["data"]["remaining_exposure"],status["data"]["protection"]);
        assert_eq!(status["data"]["protection_incident"],false,
            "T09.6 final release must not hide a protection incident");
        entry["release"]=status["data"]["central_release"].clone();
        entry["financial_complete"]=status["data"]["financial_complete"].clone();
    }
    let owner=accounts.resolve("fixture-a").await.expect("T09.6 native account");
    let final_algos=owner.observer.pending_derivative_risk_algos().await
        .expect("T09.6 final signed derivative inventory");
    assert!(final_algos.complete_within_bound
            && final_algos.samples.is_empty(),
        "T09.6 no orphan algo risk may remain after both cancel ACKs");
    let (lots, risk_census)=owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(5),
        |s| {
            let lots=s.active_lots("BTC-USDT-SWAP")?;
            let (_,intents)=s.risk_census_intents(100)?;
            Ok((lots,intents))
        },
    ).await.expect("T09.6 independent native SQL readback");
    assert!(lots.iter().all(|lot|lot.remaining_qty.is_zero()),
        "T09.6 no source-backed lots may remain");
    assert!(risk_census.is_empty(),
        "T09.6 central/local intent census must contain no unresolved risk");
    // Independently read immutable finance entries from the same SQL owner.
    // A signed close ACK and a flat lot census do not prove exit fees.
    // This is test-only evidence: neither the fixture nor Worker owns money.
    let (fee_sources, entry_fee, exit_fee) = owner.execution.store.call(
        std::time::Instant::now() + Duration::from_secs(5),
        |s| {
            let finance = s.finance_history(0, i64::MAX, 0, "", 200)?;
            let expected = [
                ("bill:t09-entry-bill-001", "entry"),
                ("bill:t09-second-001-bill", "entry"),
                ("bill:t09-second-002-bill", "entry"),
                ("bill:t09-managed-add-001-bill", "entry"),
                ("bill:t09-close-bill-001", "exit"),
                ("bill:t09-managed-close-030-bill", "exit"),
                ("bill:t09-managed-close-020-bill", "exit"),
            ];
            let mut sources = Vec::new();
            let mut entry_fee = Decimal::ZERO;
            let mut exit_fee = Decimal::ZERO;
            for (key, kind) in expected {
                let rows: Vec<_> = finance.iter().filter(|e| {
                    e.economic_event_key == key
                        && e.component == "FEE"
                        && e.currency == "USDT"
                        && e.reverses_entry_id.is_none()
                }).collect();
                let total = rows.iter().fold(Decimal::ZERO, |sum, row| sum + row.amount);
                if kind == "entry" {
                    entry_fee += total;
                } else {
                    exit_fee += total;
                }
                sources.push(serde_json::json!({
                    "economic_event_key": key,
                    "kind": kind,
                    "amount_usdt": total.normalize().to_string(),
                    "legs": rows.len(),
                }));
            }
            Ok((sources, entry_fee, exit_fee))
        },
    ).await.expect("T09.6 immutable fee entries from native SQL owner");
    for (index, expected) in [
        "-0.0005", "-0.0006", "-0.0009", "-0.001",
        "-0.0005", "-0.0015", "-0.001",
    ].iter().enumerate() {
        assert_eq!(fee_sources[index]["amount_usdt"], *expected,
            "T09.6 exact signed bill debit must appear once in economic money");
        assert_eq!(fee_sources[index]["legs"], 1,
            "T09.6 each economic fee bill must have exactly one immutable account debit");
    }
    assert_eq!(entry_fee.normalize().to_string(), "-0.003",
        "T09.6 independently summed entry fees");
    assert_eq!(exit_fee.normalize().to_string(), "-0.003",
        "T09.6 independently summed exit fees");
    let mut command=summary("risk_t09_6_after_all_closes");
    command.installation_id="installation_test".into();
    let observed=risk.request(command).await.expect("T09.6 real Worker risk");
    let b=observed.budget.as_ref().expect("T09.6 Worker Budget");
    let eb=b.entry_budget.as_ref().expect("T09.6 Worker entry budget");
    assert_eq!(eb.consumed_usd,"6.003",
        "T09.6 close must never replenish spent campaign entry capacity");
    assert_eq!(eb.reserved_usd,"0","T09.6 no outstanding entry reserve");
    assert_eq!(b.reserved_loss_usd,"0","T09.6 no outstanding central potential loss");
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),9,
        "T09.6 requires two create and two protection cancel POSTs in addition to prior five");
    serde_json::json!({
        "closes":close_results,"entries":entry_results,
        "remaining_lots":lots.iter().filter(|lot|!lot.remaining_qty.is_zero()).count(),
        "unresolved_census":risk_census.len(),
        "orphan_algos":final_algos.samples.len(),
        "fee_bill_evidence": fee_sources,
        "entry_fees_usdt": entry_fee.normalize().to_string(),
        "exit_fees_usdt": exit_fee.normalize().to_string(),
        "worker_budget":observed.budget,
        "worker_risk_book_sequence":observed.risk_book_sequence,
        "outbound_posts":feed.outbound_posts.lock().unwrap().len(),
    })
}


// T09.8: after fully source-proven flat T09.6, the same production
// Execute path must be denied by the persisted campaign RiskBook BEFORE
// any new venue POST. 0.40 * 0.01 * 1000 + 0.05% fee = 4.002 USD,
// which exceeds the authentic remaining 10 - 6.003 = 3.997 USD.
async fn t09_8_excess_entry_denied(
    accounts: &Arc<AccountRuntimeRegistry>,
    trade: &Arc<TradeService>,
    risk: &NativeRiskClient,
    feed: &Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
) -> serde_json::Value {
    use crate::accounts::recorded_support::RecordedGet;
    use okx_protocol::trading::{
        OrderConstraints, PositionSide, Protection, Sizing, TradeOperation, TradeRequest,
    };
    let mut before_command=summary("risk_t09_8_before_excess");
    before_command.installation_id="installation_test".into();
    let before=risk.request(before_command).await.expect("T09.8 Worker budget before");
    let budget=before.budget.as_ref().expect("T09.8 risk budget");
    let entry=budget.entry_budget.as_ref().expect("T09.8 entry limit");
    assert_eq!(entry.consumed_usd,"6.003");
    assert_eq!(entry.reserved_usd,"0");
    assert_eq!(entry.limit_usd,"10");
    let available=Decimal::from_str_exact(&entry.limit_usd).unwrap()
        -Decimal::from_str_exact(&entry.consumed_usd).unwrap()
        -Decimal::from_str_exact(&entry.reserved_usd).unwrap();
    assert_eq!(available.normalize().to_string(),"3.997");
    let before_posts=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(before_posts,9,"T09.8 starts strictly after two flat closes and cancels");
    // T09.6 has just added real source-backed fills, bills and terminal
    // settlement receipts to the existing Native SQL owners. The original
    // production maintenance loop must review that changed campaign epoch
    // before a new risk-increasing Execute can produce a budget denial.
    // This is read-only waiting for the SAME two SQL owners, not a test
    // shortcut to mark coverage complete or loosen its 5s freshness gate.
    let origin=before.budget.as_ref().expect("T09.8 campaign budget")
        .campaign_origin.as_ref().expect("T09.8 persisted campaign origin");
    let from=i64::try_from(origin.started_at_ms)
        .expect("T09.8 campaign origin fits source time");
    let campaign=entry.campaign_id.clone();
    let mut last_readiness=String::from("NOT_OBSERVED");
    // Production maintenance has a bounded 1/2/4/8/16/30s backoff
    // after unready financial pages. A 20s test-side observation window
    // can expire before the *next* legal reconciliation attempt. Give
    // that owner one maximum backoff cycle without changing any source TTL,
    // admission deadline or economic authority.
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let through=chrono::Utc::now().timestamp_millis()-2_000;
            let mut both_ready=true;
            for alias in ["fixture-a","fixture-b"] {
                let owner=accounts.resolve(alias).await
                    .expect("T09.8 existing SQL account owner");
                let id=campaign.clone();
                match owner.execution.store.call(
                    std::time::Instant::now()+Duration::from_secs(2),
                    move |s|s.verify_campaign_entry_readiness(&id,from,through)
                ).await {
                    Ok(())=>{},
                    Err(reason)=>{
                        last_readiness=format!("{alias}: {reason}");
                        both_ready=false;
                        break;
                    }
                }
            }
            if both_ready {break;}
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await.unwrap_or_else(|_|panic!(
        "T09_8_SOURCE_READINESS_NOT_RECONCILED:{last_readiness}"
    ));
    // Provide newly recorded, authenticated venue replies only at the
    // existing REST boundary; no exchange emulator, alternate risk owner
    // or unauthorized order outcome is fabricated.
    let at=chrono::Utc::now().timestamp_millis().to_string();
    let raw=|v|serde_json::to_vec(&serde_json::json!({
        "code":"0","msg":"","data":v
    })).expect("T09.8 recorded signed HTTP envelope");
    feed.advance_recorded_route(
        "fixture-observer-a","/api/v5/account/trade-fee",
        (0..12).map(|_|RecordedGet {
            access_key:"fixture-observer-a".into(),
            path:"/api/v5/account/trade-fee".into(),
            required_query:vec![
                ("instType".into(),"SWAP".into()),
                ("instFamily".into(),"BTC-USDT".into()),
            ],
            raw_response:raw(serde_json::json!([{
                "instType":"SWAP","level":"Lv1","ts":at,
                "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
            }])),
        }).collect(),
    );
    feed.advance_recorded_route(
        "fixture-observer-a","/api/v5/account/leverage-info",
        (0..12).map(|_|RecordedGet {
            access_key:"fixture-observer-a".into(),
            path:"/api/v5/account/leverage-info".into(),
            required_query:vec![
                ("instId".into(),"BTC-USDT-SWAP".into()),
                ("mgnMode".into(),"cross".into()),
            ],
            raw_response:raw(serde_json::json!([
                {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"long","lever":"2"},
                {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"short","lever":"2"}
            ])),
        }).collect(),
    );
    feed.advance_recorded_public_clock(raw(serde_json::json!([{"ts":at}])));
    for _ in 0..12 {
        feed.expect_public_clock(raw(serde_json::json!([{"ts":at}])));
    }
    recorded_flat_derivative_inventory_after_close(feed);
    let request=TradeRequest::Execute {
        idempotency_key:"t09_excess_campaign_budget_denied_0001".into(),
        operation:TradeOperation::Open,
        strategy_instance:"manual".into(),
        instrument:"BTC-USDT-SWAP".into(),
        side:PositionSide::Long,
        sizing:Sizing::Contracts{contracts:"0.40".into()},
        order:OrderConstraints {
            order_type:"limit".into(),limit_price:Some("1000".into()),
            reference:None,max_entry_usd:Some("10".into()),
            max_slippage_bps:0,
        },
        protection:Some(Protection::Price{stop_price:"900".into()}),
        expires_in_ms:30_000,
    };
    assert!(request.valid());
    let denied=trade.trade("fixture-a",&request,None).await
        .expect("T09.8 must return a structured CENTRAL rejection receipt");
    assert_eq!(denied["data"]["state"],"REJECTED",
        "T09.8 public Execute must refuse the venue create");
    assert_eq!(denied["data"]["rejection_origin"],"CENTRAL");
    assert_eq!(denied["data"]["send_state"],"NOT_STARTED");
    assert_eq!(denied["data"]["primary_reason"],"CAMPAIGN_ENTRY_BUDGET_EXHAUSTED",
        "T09.8 must prove central budget rejection, not stale input or local rate denial");
    let after_posts=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(after_posts,before_posts,
        "T09.8 Worker denial cannot send a new venue create");
    assert_eq!(feed.unconsumed_posts(),0,
        "T09.8 cannot append a fake successful exchange outcome");
    let owner=accounts.resolve("fixture-a").await.expect("T09.8 account A");
    let (state,held,quote_upper,admission_upper)=owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        |s| {
            let denied=s.by_idempotency("t09_excess_campaign_budget_denied_0001")?
                .ok_or(okx_execution::StoreError::Invalid("T09_8_INTENT_MISSING"))?;
            let held=s.active_central_receipt(&denied.intent_id)?.is_some();
            let quote=s.entry_quote(&denied.intent_id)?
                .ok_or(okx_execution::StoreError::Invalid("T09_8_NATIVE_QUOTE_MISSING"))?;
            let admission=s.entry_admission(&denied.intent_id)?
                .ok_or(okx_execution::StoreError::Invalid("T09_8_NATIVE_COST_PROOF_MISSING"))?;
            Ok((denied.state.as_str().to_owned(),held,quote.total_usd,admission.entry_upper_usd))
        },
    ).await.expect("T09.8 original native owner SQL after refusal");
    assert_eq!(state,"REJECTED");
    assert_eq!(quote_upper.normalize().to_string(),"4.002",
        "T09.8 independently persisted source-bound fee-inclusive SQL quote");
    assert_eq!(admission_upper.normalize().to_string(),"4.002",
        "T09.8 Worker RESERVE upper must match immutable native proof");
    assert!(admission_upper>available,"T09.8 exact financial over-budget witness");
    assert!(!held,"T09.8 cannot leave an active Worker reserve");
    let mut after_command=summary("risk_t09_8_after_excess");
    after_command.installation_id="installation_test".into();
    let after=risk.request(after_command).await.expect("T09.8 Worker after rejection");
    assert_eq!(after.budget,before.budget,
        "T09.8 over-budget denial must not spend, hold, release or reset any money");
    serde_json::json!({
        "denial_reason":denied["data"]["primary_reason"],
        "denial_origin":denied["data"]["rejection_origin"],
        "send_state":denied["data"]["send_state"],
        "requested_entry_upper_usd":admission_upper.normalize().to_string(),
        "sql_entry_quote_usd":quote_upper.normalize().to_string(),
        "remaining_entry_budget_usd":available.normalize().to_string(),
        "campaign_consumed_usd":entry.consumed_usd,
        "sql_intent_state":state,"sql_risk_held":held,
        "outbound_posts_before":before_posts,"outbound_posts_after":after_posts,
        "new_exchange_posts":after_posts-before_posts,
        "risk_book_sequence_after":after.risk_book_sequence,
        "worker_budget_after":after.budget,
    })
}

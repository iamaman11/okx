
    // T09.4: a distinct, provenance-bound Add order is admitted ONLY through
    // the existing TradeService and a fully sourced managed-position census.
    // All venue responses are immutable raw transport inputs, not SQL writes.
    struct T09_4Context {
        accounts: Arc<AccountRuntimeRegistry>,
        trade: Arc<TradeService>,
        risk: NativeRiskClient,
        feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
        prior_client_id: String,
        campaign_id: String,
        native_origin: i64,
        first_fill_at_ms: i64,
        terminal_fill_at_ms: i64,
        need_new_market: tokio::sync::oneshot::Sender<()>,
        fresh_market_ready: tokio::sync::oneshot::Receiver<()>,
    }


    struct T09_4FillContext {
        accounts: Arc<AccountRuntimeRegistry>,
        trade: Arc<TradeService>,
        risk: NativeRiskClient,
        feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
        intent_id: String,
        client_id: String,
        old_client_id: String,
        first_fill_at_ms: i64,
        terminal_fill_at_ms: i64,
    }

    // The only injected material is the venue's raw signed GET history,
    // position, protective algo and order details. The same production
    // account capture, SQL lot owner, TradeService and Worker settle them.
    async fn t09_4_fill_phase(input: T09_4FillContext) -> serde_json::Value {
        use crate::accounts::recorded_support::RecordedGet;
        let T09_4FillContext {
            accounts, trade, risk, feed, intent_id, client_id,
            old_client_id, first_fill_at_ms, terminal_fill_at_ms,
        } = input;
        let envelope=|v|serde_json::to_vec(&serde_json::json!({
            "code":"0","msg":"","data":v
        })).unwrap();
        let filled_at=chrono::Utc::now().timestamp_millis();
        let old_stop=format!("s{}",&old_client_id[..31]);
        let new_stop=format!("s{}",&client_id[..31]);
        let mut fills=Vec::new();
        let mut bills=Vec::new();
        for (event,order,size,charge,ts) in [
            ("t09-second-001","recorded-t09-second-001","0.12","-0.0006",first_fill_at_ms),
            ("t09-second-002","recorded-t09-second-001","0.18","-0.0009",terminal_fill_at_ms),
            ("t09-managed-add-001","recorded-t09-add-001","0.2","-0.001",filled_at),
        ] {
            fills.push(serde_json::json!({
                "instType":"SWAP","instId":"BTC-USDT-SWAP",
                "tradeId":format!("{event}-trade"),"ordId":order,
                "clOrdId":"__REQUEST_CL_ORD_ID__",
                "billId":format!("{event}-bill"),
                "subType":"3","side":"buy","posSide":"long",
                "fillPx":"1000","fillSz":size,"fillPnl":"0",
                "fee":charge,"feeCcy":"USDT",
                "ts":ts.to_string(),"fillTime":ts.to_string()
            }));
            bills.push(serde_json::json!({
                "billId":format!("{event}-bill"),"instType":"SWAP",
                "instId":"BTC-USDT-SWAP","ordId":order,
                "tradeId":format!("{event}-trade"),
                "clOrdId":"__REQUEST_CL_ORD_ID__",
                "ccy":"USDT","type":"2","subType":"3",
                "balChg":charge,"fee":charge,"pnl":"0",
                "ts":ts.to_string()
            }));
        }
        let mut history=Vec::new();
        for _ in 0..40 {
            for part in ["SPOT","MARGIN","SWAP","FUTURES","OPTION"] {
                history.push(RecordedGet {
                    access_key:"fixture-executor-a".into(),
                    path:"/api/v5/trade/fills-history".into(),
                    required_query:vec![("instType".into(),part.into())],
                    raw_response:if part=="SWAP" {
                        envelope(serde_json::json!(fills))
                    } else {envelope(serde_json::json!([]))},
                });
            }
        }
        feed.advance_recorded_route(
            "fixture-executor-a","/api/v5/trade/fills-history",history
        );
        feed.advance_recorded_route(
            "fixture-executor-a","/api/v5/account/bills-archive",
            (0..40).map(|_|RecordedGet {
                access_key:"fixture-executor-a".into(),
                path:"/api/v5/account/bills-archive".into(),
                required_query:vec![],
                raw_response:envelope(serde_json::json!(bills)),
            }).collect()
        );
        let positions=envelope(serde_json::json!([{
            "instType":"SWAP","instId":"BTC-USDT-SWAP",
            "posSide":"long","pos":"0.5","mgnMode":"cross",
            "avgPx":"1000","markPx":"1000","uTime":filled_at.to_string()
        }]));
        feed.advance_recorded_route(
            "fixture-observer-a","/api/v5/account/positions",
            (0..64).map(|_|RecordedGet {
                access_key:"fixture-observer-a".into(),
                path:"/api/v5/account/positions".into(),
                required_query:vec![],raw_response:positions.clone(),
            }).collect()
        );
        let mut signed_orders=Vec::new();
        for _ in 0..24 {
            for (order,quantity,at,client) in [
                ("recorded-t09-second-001","0.3",terminal_fill_at_ms,&old_client_id),
                ("recorded-t09-add-001","0.2",filled_at,&client_id)
            ] {
                signed_orders.push(RecordedGet {
                    access_key:"fixture-executor-a".into(),
                    path:"/api/v5/trade/order".into(),
                    required_query:vec![
                        ("instId".into(),"BTC-USDT-SWAP".into()),
                        ("clOrdId".into(),client.clone()),
                    ],
                    raw_response:envelope(serde_json::json!([{
                        "instId":"BTC-USDT-SWAP","ordId":order,
                        "clOrdId":"__REQUEST_CL_ORD_ID__",
                        "side":"buy","posSide":"long","tdMode":"cross",
                        "ordType":"limit","px":"1000","sz":quantity,
                        "accFillSz":quantity,"avgPx":"1000","state":"filled",
                        "cTime":at.to_string(),"uTime":at.to_string()
                    }])),
                });
            }
        }
        feed.advance_recorded_route(
            "fixture-executor-a","/api/v5/trade/order",signed_orders
        );
        let mut signed_algos=Vec::new();
        for _ in 0..16 {
            for (stop,algo,quantity) in [
                (&old_stop,"900000002","0.3"),
                (&new_stop,"900000003","0.2"),
            ] {
                signed_algos.push(RecordedGet {
                    access_key:"fixture-executor-a".into(),
                    path:"/api/v5/trade/order-algo".into(),
                    required_query:vec![("algoClOrdId".into(),stop.clone())],
                    raw_response:envelope(serde_json::json!([{
                        "instId":"BTC-USDT-SWAP","algoId":algo,
                        "algoClOrdId":stop,"state":"live","sz":quantity,
                        "side":"sell","posSide":"long","tdMode":"cross",
                        "ordType":"conditional","slTriggerPx":"900",
                        "slTriggerPxType":"mark","slOrdPx":"-1"
                    }])),
                });
            }
        }
        feed.advance_recorded_route(
            "fixture-executor-a","/api/v5/trade/order-algo",signed_algos
        );
        let mut pending=Vec::new();
        for _ in 0..12 {
            for inst in ["SWAP","FUTURES"] {
                for kind in [
                    "conditional","oco","trigger","move_order_stop",
                    "chase","iceberg","twap","smart_iceberg"
                ] {
                    let list=if inst=="SWAP" && kind=="conditional" {
                        serde_json::json!([
                            {
                                "instType":"SWAP","instId":"BTC-USDT-SWAP",
                                "ordType":"conditional","algoId":"900000002",
                                "algoClOrdId":old_stop,"state":"live",
                                "side":"sell","posSide":"long","sz":"0.3",
                                "tdMode":"cross","slTriggerPx":"900",
                                "slTriggerPxType":"mark","slOrdPx":"-1"
                            },
                            {
                                "instType":"SWAP","instId":"BTC-USDT-SWAP",
                                "ordType":"conditional","algoId":"900000003",
                                "algoClOrdId":new_stop,"state":"live",
                                "side":"sell","posSide":"long","sz":"0.2",
                                "tdMode":"cross","slTriggerPx":"900",
                                "slTriggerPxType":"mark","slOrdPx":"-1"
                            }
                        ])
                    } else {serde_json::json!([])};
                    pending.push(RecordedGet {
                        access_key:"fixture-observer-a".into(),
                        path:"/api/v5/trade/orders-algo-pending".into(),
                        required_query:vec![
                            ("instType".into(),inst.into()),
                            ("ordType".into(),kind.into()),
                            ("limit".into(),"100".into())
                        ],
                        raw_response:envelope(list),
                    });
                }
            }
        }
        feed.advance_recorded_route(
            "fixture-observer-a","/api/v5/trade/orders-algo-pending",pending
        );
        let owner=accounts.resolve("fixture-a").await.unwrap();
        let mut latest=String::from("NOT_OBSERVED");
        let sql=tokio::time::timeout(Duration::from_secs(16),async {
            loop {
                let id=intent_id.clone();
                let observed=owner.execution.store.call(
                    std::time::Instant::now()+Duration::from_secs(3),
                    move |s| {
                        let f=s.cursor("fills","SWAP")?
                            .ok_or(okx_execution::StoreError::Invalid("T09_4_FILL_CURSOR_MISSING"))?;
                        let b=s.cursor("bills","account")?
                            .ok_or(okx_execution::StoreError::Invalid("T09_4_BILL_CURSOR_MISSING"))?;
                        Ok((
                            s.intent_fill_cost_evidence(&id,f.watermark),
                            s.intent_fee_bill_evidence(&id,b.watermark),
                            f.watermark,b.watermark,f.coverage_gaps,b.coverage_gaps
                        ))
                    }
                ).await;
                match observed {
                    Ok((Ok(f),Ok(b),fw,bw,fg,bg)) => {
                        latest=format!("fill={f:?} bill={b:?} fw={fw} bw={bw}");
                        if f.filled_quantity==Decimal::new(2,1)
                            && f.fill_count==1
                            && b.linked_bill_count==1
                            && b.conservative_verified_fee_usdt==Decimal::new(1,3)
                            && fw>=filled_at && bw>=filled_at
                            && fg.is_empty() && bg.is_empty()
                        {
                            break serde_json::json!({
                                "fill_count":f.fill_count,
                                "quantity":f.filled_quantity.normalize().to_string(),
                                "fee_usdt":b.conservative_verified_fee_usdt.normalize().to_string(),
                                "bill_count":b.linked_bill_count,
                                "linked_bill_source_digest":b.source_digest,
                                "fill_watermark":fw,"bill_watermark":bw
                            });
                        }
                    }
                    o => latest=format!("capture={o:?}"),
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.unwrap_or_else(|_|panic!("T09_4_CAPTURE_UNPROVEN:{latest}"));
        let status=trade.trade(
            "fixture-a",
            &okx_protocol::trading::TradeRequest::Status {
                reference:intent_id.clone()
            },None
        ).await.expect("T09.4 production TradeService status after raw fill");
        assert_eq!(status["data"]["intent"]["state"],"FILLED",
            "T09_4_SOURCE_BACKED_STATUS_NOT_FILLED:{status}");
        assert_ne!(status["data"]["protection_incident"],true,
            "T09.4 must never pass a safety protection incident");
        let mut command=summary("risk_t09_4_after_linked_fill");
        command.installation_id="installation_test".into();
        let worker=risk.request(command).await.unwrap();
        let budget=worker.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
        assert_eq!(budget.consumed_usd,"6.003",
            "Native settled Add must debit exactly 2.001 additional USD");
        assert_eq!(budget.reserved_usd,"0",
            "Worker must retain no unspent Add reservation after full fill");
        assert_eq!(feed.outbound_posts.lock().unwrap().len(),5,
            "fill and bill capture can NEVER submit another venue POST");
        serde_json::json!({
            "add_fill_at_ms":filled_at,
            "intent_id":intent_id,
            "filled":sql,
            "trade_status":status,
            "worker_budget":worker.budget,
            "risk_book_sequence":worker.risk_book_sequence,
        })
    }

    async fn t09_4_managed_add(context: T09_4Context) -> serde_json::Value {
        let T09_4Context {
            accounts,
            trade,
            risk,
            feed,
            prior_client_id,
            campaign_id,
            native_origin,
            first_fill_at_ms,
            terminal_fill_at_ms,
            need_new_market,
            fresh_market_ready,
        } = context;
        use crate::accounts::recorded_support::{RecordedGet, RecordedPost};
        use okx_protocol::trading::{
            OrderConstraints, PositionSide, Protection, Sizing, TradeOperation, TradeRequest,
        };
        // The sole existing financial owner must commit both source frontiers
        // before an increase. Never infer freshness from the T09.3 status
        // response or add an independent importer.

        // Repeated signed history observations may contain the SAME immutable
        // fill/bill IDs. Their true event times never change; the existing
        // financial owner may capture them again to certify a fresh frontier.
        let raw_history=|v|serde_json::to_vec(&serde_json::json!({
            "code":"0","msg":"","data":v
        })).unwrap();
        let mut known_fills=Vec::new();
        let mut known_bills=Vec::new();
        for (idx,size,fee,event_ms) in [
            (1,"0.12","-0.0006",first_fill_at_ms),
            (2,"0.18","-0.0009",terminal_fill_at_ms)
        ] {
            let event=format!("t09-second-{idx:03}");
            known_fills.push(serde_json::json!({
                "instType":"SWAP","instId":"BTC-USDT-SWAP",
                "tradeId":format!("{event}-trade"),
                "ordId":"recorded-t09-second-001",
                "clOrdId":"__REQUEST_CL_ORD_ID__",
                "billId":format!("{event}-bill"),
                "subType":"3","side":"buy","posSide":"long",
                "fillPx":"1000","fillSz":size,"fillPnl":"0",
                "fee":fee,"feeCcy":"USDT",
                "ts":event_ms.to_string(),"fillTime":event_ms.to_string()
            }));
            known_bills.push(serde_json::json!({
                "billId":format!("{event}-bill"),
                "instType":"SWAP","instId":"BTC-USDT-SWAP",
                "ordId":"recorded-t09-second-001",
                "tradeId":format!("{event}-trade"),
                "clOrdId":"__REQUEST_CL_ORD_ID__",
                "ccy":"USDT","type":"2","subType":"3",
                "balChg":fee,"fee":fee,"pnl":"0","ts":event_ms.to_string()
            }));
        }
        let mut pages=Vec::new();
        for _ in 0..48 {
            for part in ["SPOT","MARGIN","SWAP","FUTURES","OPTION"] {
                pages.push(RecordedGet{
                    access_key:"fixture-executor-a".into(),
                    path:"/api/v5/trade/fills-history".into(),
                    required_query:vec![("instType".into(),part.into())],
                    raw_response:if part=="SWAP" {
                        raw_history(serde_json::json!(known_fills))
                    } else {raw_history(serde_json::json!([]))}
                });
            }
        }
        feed.advance_recorded_route("fixture-executor-a","/api/v5/trade/fills-history",pages);
        feed.advance_recorded_route("fixture-executor-a","/api/v5/account/bills-archive",
            (0..48).map(|_|RecordedGet{
                access_key:"fixture-executor-a".into(),
                path:"/api/v5/account/bills-archive".into(),
                required_query:vec![],
                raw_response:raw_history(serde_json::json!(known_bills))
            }).collect()
        );
        // The existing Native finance owner must have fresh signed positions
        // WHILE refreshing campaign source cursors, not only after it is ready.
        // It continues to observe the exact 0.30 filled, protected exposure.
        let wrap = |v|serde_json::to_vec(&serde_json::json!({
            "code":"0","msg":"","data":v
        })).unwrap();
        let ts=chrono::Utc::now().timestamp_millis().to_string();
        let source_position=wrap(serde_json::json!([{
            "instType":"SWAP","instId":"BTC-USDT-SWAP","posSide":"long",
            "pos":"0.3","mgnMode":"cross","avgPx":"1000","markPx":"1000","uTime":ts
        }]));
        feed.advance_recorded_route(
            "fixture-observer-a","/api/v5/account/positions",
            (0..64).map(|_|RecordedGet {
                access_key:"fixture-observer-a".into(),
                path:"/api/v5/account/positions".into(),
                required_query:vec![],raw_response:source_position.clone(),
            }).collect()
        );
        // Finance maintenance still observes the previously FILLED opening
        // until its complete source frontier is committed. Serve that exact
        // old venue order during readiness; only after the frontier is proven
        // may the recorded transport switch to the distinct Add order ID.
        let previous_order = wrap(serde_json::json!([{
            "instId":"BTC-USDT-SWAP","ordId":"recorded-t09-second-001",
            "clOrdId":"__REQUEST_CL_ORD_ID__","side":"buy",
            "posSide":"long","tdMode":"cross","ordType":"limit",
            "px":"1000","sz":"0.3","accFillSz":"0.3","avgPx":"1000",
            "state":"filled","cTime":terminal_fill_at_ms.to_string(),
            "uTime":terminal_fill_at_ms.to_string()
        }]));
        feed.advance_recorded_route(
            "fixture-executor-a","/api/v5/trade/order",
            (0..48).map(|_| RecordedGet {
                access_key:"fixture-executor-a".into(),
                path:"/api/v5/trade/order".into(),
                required_query:vec![("instId".into(),"BTC-USDT-SWAP".into())],
                raw_response:previous_order.clone(),
            }).collect()
        );
        let mut last_readiness=String::from("NOT_OBSERVED");
        tokio::time::timeout(Duration::from_secs(16),async {
            loop {
                let through=chrono::Utc::now().timestamp_millis()-2_000;
                let mut ready=true;
                for alias in ["fixture-a","fixture-b"] {
                    let owner=accounts.resolve(alias).await.unwrap();
                    let campaign=campaign_id.clone();
                    match owner.execution.store.call(
                        std::time::Instant::now()+Duration::from_secs(3),
                        move |s|s.verify_campaign_entry_readiness(&campaign,native_origin,through)
                    ).await {
                        Ok(())=>{},
                        Err(reason)=>{
                            last_readiness=format!("{alias}:{reason}");
                            ready=false;break;
                        }
                    }
                }
                if ready {break}
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.unwrap_or_else(|_|panic!(
            "T09_4_SOURCE_FRONTIER_INCOMPLETE:{last_readiness}; strict_source_denials={:?}",
            feed.denied_routes.lock().unwrap().iter().rev().take(5).collect::<Vec<_>>()
        ));

        // The producer must publish NEW signed public quotes only AFTER
        // the slower financial frontier is proven, before a single Execute.
        need_new_market.send(()).expect("T09.4 market request channel");
        fresh_market_ready.await.expect("T09.4 recorded market refresh missing");
        let signed_now=chrono::Utc::now().timestamp_millis().to_string();
        // 16 independently queried derivative conditional scopes, all
        // returned by signed Observer GET. Only the one previously owned,
        // live stop is present; an unknown algo would block Add.
        let old_stop=format!("s{}",&prior_client_id[..31]);
        let mut pages=Vec::new();
        for _ in 0..7 {
            for inst in ["SWAP","FUTURES"] {
                for kind in [
                    "conditional","oco","trigger","move_order_stop","chase",
                    "iceberg","twap","smart_iceberg"
                ] {
                    let data=if inst=="SWAP" && kind=="conditional" {
                        serde_json::json!([{
                            "instType":"SWAP","instId":"BTC-USDT-SWAP","ordType":"conditional",
                            "algoId":"900000002","algoClOrdId":old_stop,
                            "state":"live","side":"sell","posSide":"long","sz":"0.3",
                            "tdMode":"cross","slTriggerPx":"900",
                            "slTriggerPxType":"mark","slOrdPx":"-1"
                        }])
                    } else {serde_json::json!([])};
                    pages.push(RecordedGet {
                        access_key:"fixture-observer-a".into(),
                        path:"/api/v5/trade/orders-algo-pending".into(),
                        required_query:vec![
                            ("instType".into(),inst.into()),
                            ("ordType".into(),kind.into()),
                            ("limit".into(),"100".into())
                        ],
                        raw_response:wrap(data),
                    });
                }
            }
        }
        feed.advance_recorded_route(
            "fixture-observer-a","/api/v5/trade/orders-algo-pending",pages
        );
        // The actual exchange fee generation and leverage are reread for
        // this new order; the T09.2 admission proof cannot be reused.
        let mut refreshed_fee_pages=Vec::new();
        for _ in 0..8 {
            refreshed_fee_pages.push(RecordedGet {
                access_key:"fixture-observer-a".into(),
                path:"/api/v5/account/trade-fee".into(),
                required_query:vec![
                    ("instType".into(),"SWAP".into()),
                    ("instFamily".into(),"BTC-USDT".into()),
                ],
                raw_response:wrap(serde_json::json!([{
                    "instType":"SWAP","level":"Lv1","ts":signed_now,
                    "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
                }])),
            });
            feed.expect_get(RecordedGet {
                access_key:"fixture-observer-a".into(),
                path:"/api/v5/account/leverage-info".into(),
                required_query:vec![
                    ("instId".into(),"BTC-USDT-SWAP".into()),
                    ("mgnMode".into(),"cross".into()),
                ],
                raw_response:wrap(serde_json::json!([
                    {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"long","lever":"2"},
                    {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"short","lever":"2"}
                ])),
            });
        }
        // A fresh exchange fee generation supersedes unused older signed
        // pages from T09.2. The production GET must never consume stale
        // leftover fixture bytes ahead of this new admission.
        feed.advance_recorded_route(
            "fixture-observer-a","/api/v5/account/trade-fee",refreshed_fee_pages
        );
        feed.advance_recorded_public_clock(wrap(serde_json::json!([{"ts":signed_now}])));
        // The same production Add admission reads exchange time before POST
        // and again after the central reserve. Provide finite recorded venue
        // replies, never a simulated wall-clock or live HTTP fallback.
        for _ in 0..8 {
            feed.expect_public_clock(wrap(serde_json::json!([{"ts":signed_now}])));
        }
        feed.expect_post(RecordedPost {
            access_key:"fixture-executor-a".into(),
            path:"/api/v5/trade/order".into(),
            expected_body:serde_json::json!({
                "instId":"BTC-USDT-SWAP","tdMode":"cross",
                "clOrdId":"__REQUEST_CL_ORD_ID__",
                "side":"buy","posSide":"long","ordType":"limit",
                "sz":"0.2","px":"1000",
                "attachAlgoOrds":[{
                    "attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__",
                    "tpTriggerPxType":"mark","slTriggerPx":"900",
                    "slTriggerPxType":"mark","slOrdPx":"-1"
                }]
            }),
            raw_response:br#"{"code":"0","msg":"","data":[{"ordId":"recorded-t09-add-001","clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]}"#.to_vec(),
        });
        feed.advance_recorded_route(
            "fixture-executor-a","/api/v5/trade/order",
            (0..4).map(|_|RecordedGet {
                access_key:"fixture-executor-a".into(),
                path:"/api/v5/trade/order".into(),
                required_query:vec![("instId".into(),"BTC-USDT-SWAP".into())],
                raw_response:wrap(serde_json::json!([{
                    "instId":"BTC-USDT-SWAP","ordId":"recorded-t09-add-001",
                    "clOrdId":"__REQUEST_CL_ORD_ID__","side":"buy",
                    "posSide":"long","tdMode":"cross","ordType":"limit",
                    "px":"1000","sz":"0.2","accFillSz":"0","avgPx":"",
                    "state":"live","cTime":signed_now,"uTime":signed_now
                }])),
            }).collect(),
        );
        let request=TradeRequest::Execute {
            idempotency_key:"t09_real_public_add_0001".into(),
            operation:TradeOperation::Add,
            strategy_instance:"manual".into(),instrument:"BTC-USDT-SWAP".into(),
            side:PositionSide::Long,sizing:Sizing::Contracts{contracts:"0.20".into()},
            order:OrderConstraints{
                order_type:"limit".into(),limit_price:Some("1000".into()),
                reference:None,max_entry_usd:Some("10".into()),
                max_slippage_bps:0,
            },
            protection:Some(Protection::Price{stop_price:"900".into()}),
            expires_in_ms:30_000,
        };
        let mut before=summary("risk_t09_4_before_managed_add");
        before.installation_id="installation_test".into();
        let observed=risk.request(before).await.unwrap();
        let start_budget=observed.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
        assert_eq!(start_budget.consumed_usd,"4.002");
        assert_eq!(start_budget.reserved_usd,"0");
        assert!(request.valid());
        let response=trade.trade("fixture-a",&request,None).await
            .unwrap_or_else(|reason|panic!("T09_4_MANAGED_ADD_DENIED:{reason}"));
        let id=response["data"]["intent"]["intent_id"].as_str()
            .expect("T09.4 Native Add intent ID").to_owned();
        let owner=accounts.resolve("fixture-a").await.unwrap();
        let id_for_sql=id.clone();
        let (intent,reserve,post)=owner.execution.store.call(
            std::time::Instant::now()+Duration::from_secs(3),
            move |s|Ok((
                s.intent(&id_for_sql)?.ok_or(okx_execution::StoreError::Invalid("T09_4_SQL_ADD_MISSING"))?,
                s.central_receipt(&id_for_sql)?.ok_or(okx_execution::StoreError::Invalid("T09_4_WORKER_RESERVE_MISSING"))?,
                s.post_fence_evidence(&id_for_sql)?,
            ))
        ).await.unwrap();
        assert_eq!(intent.exchange_order_id.as_deref(),Some("recorded-t09-add-001"));
        assert_eq!(intent.plan.quantity,Decimal::new(2,1));
        assert!(post.0>0);
        assert_eq!(feed.outbound_posts.lock().unwrap().len(),5);
        let mut after=summary("risk_t09_4_add_ack_unsettled");
        after.installation_id="installation_test".into();
        let worker=risk.request(after).await.unwrap();
        println!("T09_4_NATIVE_ADD_ACK_V1:{}",serde_json::json!({
            "intent_id":id,"account_uid":owner.configuration.account_key.verified_uid,
            "sql_begin_post_at_ms":post.0,"central_receipt_hash":reserve.receipt_hash,
            "venue_order_id":intent.exchange_order_id,
            "worker_budget":worker.budget,"financial_complete":response["data"]["financial_complete"],
            "post_count":feed.outbound_posts.lock().unwrap().len()
        }));
        let settlement=t09_4_fill_phase(T09_4FillContext {
            accounts:accounts.clone(),trade:trade.clone(),risk:risk.clone(),
            feed:feed.clone(),intent_id:id.clone(),
            client_id:intent.client_order_id.clone(),
            old_client_id:prior_client_id.clone(),
            first_fill_at_ms,terminal_fill_at_ms,
        }).await;
        println!("T09_4_NATIVE_ADD_SETTLEMENT_V1:{settlement}");
        serde_json::json!({
            "intent_id":id,"client_order_id":intent.client_order_id,
            "central_receipt_hash":reserve.receipt_hash,
            "sql_begin_post_at_ms":post.0,
            "venue_order_id":"recorded-t09-add-001",
            "account_uid":owner.configuration.account_key.verified_uid,
            "settlement":settlement
        })
    }

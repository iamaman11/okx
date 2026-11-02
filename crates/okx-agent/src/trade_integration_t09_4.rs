
    // T09.4: a distinct, provenance-bound Add order is admitted ONLY through
    // the existing TradeService and a fully sourced managed-position census.
    // All venue responses are immutable raw transport inputs, not SQL writes.
    async fn t09_4_managed_add(
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
    ) -> serde_json::Value {
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
        serde_json::json!({"intent_id":id,"client_order_id":intent.client_order_id})
    }

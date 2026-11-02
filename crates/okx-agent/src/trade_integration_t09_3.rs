
    // T09.3: deterministic raw signed transport records, never an extra ledger.
    // Each phase is processed by the existing account capture/SQL owners.
    async fn t09_3_phase(
        accounts: Arc<AccountRuntimeRegistry>,
        trade: Arc<TradeService>,
        feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
        id: String,
        cl_id: String,
        first_ts: Option<i64>,
    ) -> (serde_json::Value, i64) {
        use crate::accounts::recorded_support::RecordedGet;
        let raw = |v| serde_json::to_vec(&serde_json::json!({"code":"0","msg":"","data":v})).unwrap();
        let now_ms=chrono::Utc::now().timestamp_millis();
        let terminal=first_ts.is_some();
        let first=first_ts.unwrap_or(now_ms);
        let (qty, fee, fills_count)=if terminal {
            (Decimal::new(30,2),Decimal::new(15,4),2usize)
        } else {(Decimal::new(12,2),Decimal::new(6,4),1usize)};
        let mut fills=Vec::new();
        let mut bills=Vec::new();
        for (n,size,charge,ts) in [
            (1,"0.12","-0.0006",first),
            (2,"0.18","-0.0009",now_ms),
        ].into_iter().take(fills_count) {
            let event=format!("t09-second-{n:03}");
            fills.push(serde_json::json!({
                "instType":"SWAP","instId":"BTC-USDT-SWAP",
                "tradeId":format!("{event}-trade"),"ordId":"recorded-t09-second-001",
                "clOrdId":"__REQUEST_CL_ORD_ID__","billId":format!("{event}-bill"),
                "subType":"3","side":"buy","posSide":"long",
                "fillPx":"1000","fillSz":size,"fillPnl":"0",
                "fee":charge,"feeCcy":"USDT",
                "ts":ts.to_string(),"fillTime":ts.to_string()
            }));
            bills.push(serde_json::json!({
                "billId":format!("{event}-bill"),"instType":"SWAP",
                "instId":"BTC-USDT-SWAP","ordId":"recorded-t09-second-001",
                "tradeId":format!("{event}-trade"),"clOrdId":"__REQUEST_CL_ORD_ID__",
                "ccy":"USDT","type":"2","subType":"3",
                "balChg":charge,"fee":charge,"pnl":"0","ts":ts.to_string()
            }));
        }
        let mut history=Vec::new();
        for _ in 0..12 {
            for part in ["SPOT","MARGIN","SWAP","FUTURES","OPTION"] {
                history.push(RecordedGet {
                    access_key:"fixture-executor-a".into(),
                    path:"/api/v5/trade/fills-history".into(),
                    required_query:vec![("instType".into(),part.into())],
                    raw_response:if part=="SWAP" {raw(serde_json::json!(fills))}
                        else {raw(serde_json::json!([]))},
                });
            }
        }
        feed.advance_recorded_route("fixture-executor-a","/api/v5/trade/fills-history",history);
        feed.advance_recorded_route("fixture-executor-a","/api/v5/account/bills-archive",
            (0..12).map(|_| RecordedGet{
                access_key:"fixture-executor-a".into(),
                path:"/api/v5/account/bills-archive".into(),
                required_query:vec![],raw_response:raw(serde_json::json!(bills)),
            }).collect());
        let positions=raw(serde_json::json!([{
            "instType":"SWAP","instId":"BTC-USDT-SWAP",
            "posSide":"long","pos":qty.to_string(),"mgnMode":"cross",
            "avgPx":"1000","markPx":"1000","uTime":now_ms.to_string()
        }]));
        feed.advance_recorded_route("fixture-observer-a","/api/v5/account/positions",
            (0..24).map(|_| RecordedGet{
                access_key:"fixture-observer-a".into(),
                path:"/api/v5/account/positions".into(),
                required_query:vec![],raw_response:positions.clone(),
            }).collect());
        let order=raw(serde_json::json!([{
            "instId":"BTC-USDT-SWAP","ordId":"recorded-t09-second-001",
            "clOrdId":"__REQUEST_CL_ORD_ID__","side":"buy","posSide":"long",
            "tdMode":"cross","ordType":"limit","px":"1000","sz":"0.3",
            "accFillSz":qty.to_string(),"avgPx":"1000",
            "state":if terminal {"filled"} else {"partially_filled"},
            "cTime":now_ms.to_string(),"uTime":now_ms.to_string()
        }]));
        feed.advance_recorded_route("fixture-executor-a","/api/v5/trade/order",
            (0..5).map(|_| RecordedGet{
                access_key:"fixture-executor-a".into(),path:"/api/v5/trade/order".into(),
                required_query:vec![("instId".into(),"BTC-USDT-SWAP".into())],
                raw_response:order.clone(),
            }).collect());
        let stop_id=format!("s{}",&cl_id[..31]);
        feed.advance_recorded_route("fixture-executor-a","/api/v5/trade/order-algo",
            (0..5).map(|_| RecordedGet{
                access_key:"fixture-executor-a".into(),path:"/api/v5/trade/order-algo".into(),
                required_query:vec![("algoClOrdId".into(),stop_id.clone())],
                raw_response:raw(serde_json::json!([{
                    "instId":"BTC-USDT-SWAP","algoId":"900000002",
                    "algoClOrdId":stop_id,"state":"live","sz":qty.to_string(),
                    "side":"sell","posSide":"long","tdMode":"cross",
                    "ordType":"conditional","slTriggerPx":"900",
                    "slTriggerPxType":"mark","slOrdPx":"-1"
                }])),
            }).collect());
        let owner=accounts.resolve("fixture-a").await.unwrap();
        let mut last=String::new();
        let sql=tokio::time::timeout(Duration::from_secs(16),async {
            loop {
                let intent=id.clone();
                let observed=owner.execution.store.call(
                    std::time::Instant::now()+Duration::from_secs(3),move |s| {
                        let f=s.cursor("fills","SWAP")?
                            .ok_or(okx_execution::StoreError::Invalid("T09_3_FILL_CURSOR_MISSING"))?;
                        let b=s.cursor("bills","account")?
                            .ok_or(okx_execution::StoreError::Invalid("T09_3_BILL_CURSOR_MISSING"))?;
                        Ok((s.intent_fill_cost_evidence(&intent,f.watermark),
                            s.intent_fee_bill_evidence(&intent,b.watermark),
                            f.watermark,b.watermark,f.coverage_gaps,b.coverage_gaps))
                    }).await;
                match observed {
                    Ok((Ok(f),Ok(b),fw,bw,fg,bg)) => {
                        last=format!("fill={f:?} bill={b:?} fw={fw} bw={bw}");
                        if f.filled_quantity==qty && f.fill_count==fills_count as u64
                            && b.linked_bill_count==fills_count as u64
                            && b.conservative_verified_fee_usdt==fee
                            && fw>=now_ms && bw>=now_ms && fg.is_empty() && bg.is_empty() {
                            break serde_json::json!({
                                "fill_count":f.fill_count,"bill_count":b.linked_bill_count,
                                "quantity":f.filled_quantity.normalize().to_string(),
                                "notional_usdt":f.gross_notional_usdt.normalize().to_string(),
                                "fee_usdt":b.conservative_verified_fee_usdt.normalize().to_string(),
                                "source_digest":b.source_digest,
                                "fill_watermark":fw,"bill_watermark":bw
                            });
                        }
                    }
                    other => last=format!("readback={other:?}"),
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.unwrap_or_else(|_|panic!("T09_3_CAPTURE_UNPROVEN terminal={terminal}: {last}"));
        let status=trade.trade("fixture-a",&okx_protocol::trading::TradeRequest::Status{reference:id},None)
            .await.expect("T09.3 real TradeService status");
        if !terminal {
            assert_ne!(status["data"]["financial_complete"],true,
                "partial fill must never claim fully settled order");
        }
        (serde_json::json!({
            "phase":if terminal {"terminal"} else {"partial"},
            "sql":sql,"status":status["status"],
            "intent_state":status["data"]["intent"]["state"],
            "financial_complete":status["data"]["financial_complete"],
            "protection":status["data"]["protection"],
        }),first)
    }

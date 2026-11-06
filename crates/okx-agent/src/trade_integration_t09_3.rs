
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
        // Recorded finite *external* balance observations for each financial
        // frontier. The baseline implicit GET boundary remains capped at
        // exactly 64: never raise it or rewind its consumed count. A newly
        // published fill/bill frontier also has its own explicitly signed
        // account balance pages. These bytes only enter the ordinary
        // authenticated observer parser; no SQL or Worker finance is written.
        let raw_balance = raw(serde_json::json!([{
            "totalEq":"100","adjEq":"100","uTime":now_ms.to_string(),
            "details":[{
                "ccy":"USDT","eq":"100","liab":"0",
                "availEq":"100","availBal":"100","eqUsd":"100",
                "uTime":now_ms.to_string()
            }]
        }]));
        let balance_pages = (0..if terminal { 12 } else { 8 })
            .map(|_| RecordedGet {
                access_key: "fixture-observer-a".into(),
                path: "/api/v5/account/balance".into(),
                required_query: vec![],
                raw_response: raw_balance.clone(),
            })
            .collect::<Vec<_>>();
        if terminal {
            let previous = feed.advance_recorded_route(
                "fixture-observer-a",
                "/api/v5/account/balance",
                balance_pages,
            );
            eprintln!(
                "T09_3_BALANCE_FRONTIER_V1 phase=terminal retired={previous} new_pages=12"
            );
        } else {
            for page in balance_pages {
                feed.expect_get(page);
            }
            eprintln!("T09_3_BALANCE_FRONTIER_V1 phase=partial new_pages=8");
        }
        // Test-only finite-source accounting: stage boundaries, not signed
        // credentials, bodies, exchange responses or unrestricted request logs.
        let trace_source_reads = |stage: &str| {
            let requests = feed.requests.lock().unwrap();
            let count = |route: &str| requests.iter()
                .filter(|entry| entry.starts_with(route)).count();
            eprintln!(
                "T09_3_SOURCE_READS_V1 stage={stage} terminal={terminal} observer_balance={} observer_positions={} observer_orders={} observer_algos={} executor_fills={} executor_bills={} total={}",
                count("fixture-observer-a:/api/v5/account/balance?"),
                count("fixture-observer-a:/api/v5/account/positions?"),
                count("fixture-observer-a:/api/v5/trade/orders-pending?"),
                count("fixture-observer-a:/api/v5/trade/orders-algo-pending?"),
                count("fixture-executor-a:/api/v5/trade/fills-history?"),
                count("fixture-executor-a:/api/v5/account/bills-archive?"),
                requests.len(),
            );
        };
        trace_source_reads("phase_start");
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
        // Standalone T09.3 observes this terminal venue order AFTER the
        // independent signed fill/bill phase. Its pending-order inventory is
        // therefore newly empty. Publish a finite, query-bound external GET
        // transcript for that distinct terminal phase, rather than silently
        // increasing/rewinding the original 64-request baseline. Extended
        // T09.4/T09.6 paths own subsequent live-Add source transitions.
        let standalone=std::env::var("OKX_T09_4_MANAGED_ADD_PROBE").as_deref()!=Ok("1")
            && std::env::var("OKX_T09_6_CLOSE_PROBE").as_deref()!=Ok("1");
        if terminal && standalone {
            for _ in 0..8 {
                feed.expect_get(RecordedGet {
                    access_key:"fixture-observer-a".into(),
                    path:"/api/v5/trade/orders-pending".into(),
                    required_query:vec![("limit".into(),"100".into())],
                    raw_response:raw(serde_json::json!([])),
                });
            }
            eprintln!("T09_3_TERMINAL_PENDING_ORDERS_SOURCE_V1 phase=terminal new_pages=8 signed_order=FILLED");
        }
        let stop_id=format!("s{}",&cl_id[..31]);
        // T10-08 has already witnessed a real 51008-canceled stop. A new
        // `live` GET with the SAME algoId would fabricate remediation. Only
        // the stand-alone T09.3 path publishes the original live stop.
        let t10_08_terminal = terminal
            && std::env::var("OKX_T10_08_PROTECTION_FAILURE_PROBE").as_deref() == Ok("1");
        if !t10_08_terminal {
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
        }
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
        trace_source_reads("before_status");
        if t10_08_terminal {
            // The original entry has two independently ingested signed fill
            // and bill events. Do NOT issue a status yet: it can authorize an
            // emergency Close before this case has observed its own signed
            // venue POST/ACK and registered a matching finite exchange source.
            // This receipt is SQL-only and explicitly NOT a public status.
            let sql_id=id.clone();
            let durable=owner.execution.store.call(
                std::time::Instant::now()+Duration::from_secs(3),
                move |s| {
                    let intent=s.intent(&sql_id)?.ok_or(
                        okx_execution::StoreError::Invalid("T10_08_SQL_INTENT_MISSING")
                    )?;
                    Ok(intent.state.as_str().to_owned())
                }
            ).await.expect("T10-08 terminal source owner SQL state");
            assert_eq!(durable,"FILLED","late signed fill must be durable before Close");
            return (serde_json::json!({
                "phase":"terminal",
                "raw_history_event_at_ms":now_ms,
                "source_read_counts":{"stage":"before_recovery_status"},
                "sql":sql,
                "intent_state":durable,
                "status":"not_observed",
                "entry_fill_evidence_state":"not_evaluated",
                "financial_complete":false,
                "protection":null,
                "public_status_deferred_until_signed_close_sources":true
            }),first);
        }
        let status_result=trade.trade("fixture-a",&okx_protocol::trading::TradeRequest::Status{reference:id},None)
            .await;
        trace_source_reads("after_status");
        let source_read_counts = {
            let requests = feed.requests.lock().unwrap();
            let count = |route: &str| {
                requests.iter().filter(|entry| entry.starts_with(route)).count()
            };
            serde_json::json!({
                "observer_balance":count("fixture-observer-a:/api/v5/account/balance?"),
                "observer_positions":count("fixture-observer-a:/api/v5/account/positions?"),
                "observer_orders":count("fixture-observer-a:/api/v5/trade/orders-pending?"),
                "observer_algos":count("fixture-observer-a:/api/v5/trade/orders-algo-pending?"),
                "executor_fills":count("fixture-executor-a:/api/v5/trade/fills-history?"),
                "executor_bills":count("fixture-executor-a:/api/v5/account/bills-archive?"),
                "all_recorded_gets":requests.len()
            })
        };
        let status=status_result.expect("T09.3 real TradeService status");
        if !terminal {
            assert_eq!(status["data"]["intent"]["state"],"PARTIALLY_FILLED",
                "a covered partial bill never proves the remaining quantity executed");
        }
        // SQL above independently proved exact fills and linked fees.
        // A fresh status GET may lack a NEW financial capture. Its safe
        // result is not_evaluated with financial_complete=false, never a
        // fabricated completed settlement.
        let entry_state=&status["data"]["entry_fill_evidence"]["state"];
        if entry_state=="not_evaluated" {
            assert_eq!(status["data"]["financial_complete"],false);
            assert!(status["data"]["financial_error"].is_string());
        } else {
            assert_eq!(entry_state,"verified_native_fill_and_bill_fee_sources");
        }
        (serde_json::json!({
            "phase":if terminal {"terminal"} else {"partial"},
            "raw_history_event_at_ms":now_ms,
            "source_read_counts":source_read_counts,
            "sql":sql,"status":status["status"],
            "intent_state":status["data"]["intent"]["state"],
            "entry_fill_evidence_state":status["data"]["entry_fill_evidence"]["state"],
            "financial_complete":status["data"]["financial_complete"],
            "protection":status["data"]["protection"],
        }),first)
    }

    #[tokio::test]
    async fn t09_3_terminal_pending_orders_source_is_one_use_and_fails_closed() {
        use crate::accounts::recorded_support::{RecordedAccountHttp, RecordedGet};
        use okx_api::{AccountApi, Credentials, OkxRestClient};
        let feed=Arc::new(RecordedAccountHttp::default());
        feed.expect_get(RecordedGet {
            access_key:"fixture-observer-a".into(),
            path:"/api/v5/trade/orders-pending".into(),
            required_query:vec![("limit".into(),"100".into())],
            raw_response:br#"{"code":"0","msg":"","data":[]}"#.to_vec(),
        });
        let client=OkxRestClient::with_http_transport(
            OkxEnvironment::new(okx_api::Region::Global,false),
            Credentials::new("fixture-observer-a".into(),"observer-secret".into(),"pass".into()).unwrap(),
            RateBudget::new(),feed.clone(),
        ).unwrap();
        let api=AccountApi::new(client);
        assert!(api.pending_orders().await.unwrap().is_empty(),
            "one matching signed terminal inventory is valid");
        assert_eq!(feed.unconsumed_reads(),0);
        let exhausted=api.pending_orders().await.unwrap_err();
        assert!(exhausted.to_string().contains("TRANSCRIPT_SOURCE_PAGE_UNEXPECTED_OR_EXHAUSTED"),
            "the signed empty terminal source is one-use; no default empty fallback: {exhausted}");
    }

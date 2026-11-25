//! Historical T06–T10 product semantics executed against the actual Research
//! child rather than a test-only Trading Engine ResearchClient implementation.
//! This is deliberately a helper of `ipc_process`: Windows still runs exactly
//! ten process tests, including the OS-wide singleton/Job exclusion case.

use super::{expected_worker_sha256, receive, send, start, test_root};
use okx_research_protocol::{
    analytics::{MarketRequest, PortfolioScenarioRequest, ResearchRequest},
    research_ipc::{
        FundingHistoryEvent, HistoryCandle, HostFrame, InvocationId, PublicCandleCapture,
        PublicFundingCapture, PublicSourceReply, PublicSourceRequest, RequestId, ResearchCommand,
        ResearchOpenInterest, ResearchScope, WorkerFrame, WorkerReply, WorkerSession,
    },
};
use serde_json::{Value, json};
use std::{path::PathBuf, time::Duration};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

pub(super) type Source = Box<dyn FnMut(PublicSourceRequest) -> Result<PublicSourceReply, String>>;

pub(super) struct RealWorker {
    root: PathBuf,
    child: Child,
    input: FramedWrite<ChildStdin, LengthDelimitedCodec>,
    output: FramedRead<ChildStdout, LengthDelimitedCodec>,
    session: WorkerSession,
    scope: ResearchScope,
    next: u64,
    source: Source,
    source_reads: usize,
}

impl RealWorker {
    pub(super) async fn open(label: &str, source: Source) -> Self {
        let root = test_root(label);
        let session = WorkerSession::new(format!("{label}-session")).unwrap();
        let scope = ResearchScope::new(format!("{label}-scope")).unwrap();
        let (child, input, output) = start(&root, &session, &scope).await;
        Self {
            root,
            child,
            input,
            output,
            session,
            scope,
            next: 0,
            source,
            source_reads: 0,
        }
    }

    pub(super) async fn call(&mut self, command: ResearchCommand) -> Result<WorkerReply, String> {
        self.call_with_invocation(command, None).await
    }

    pub(super) async fn call_with_invocation(
        &mut self,
        command: ResearchCommand,
        invocation: Option<InvocationId>,
    ) -> Result<WorkerReply, String> {
        self.next += 1;
        let id = RequestId::new(format!("h{}", self.next)).unwrap();
        send(
            &mut self.input,
            HostFrame::Request {
                invocation,
                id: id.clone(),
                session: self.session.clone(),
                command: Box::new(command),
            },
        )
        .await;
        loop {
            match receive(&mut self.output).await {
                WorkerFrame::SourceRequest {
                    id: source_id,
                    session,
                    request,
                } => {
                    self.source_reads += 1;
                    let result = (self.source)(request);
                    send(
                        &mut self.input,
                        HostFrame::SourceReply {
                            id: source_id,
                            session,
                            result,
                        },
                    )
                    .await;
                }
                WorkerFrame::Response {
                    id: returned,
                    result,
                    ..
                } if returned == id => return result,
                other => panic!("unexpected Research owner IPC frame: {other:?}"),
            }
        }
    }

    pub(super) async fn research(&mut self, request: ResearchRequest) -> Result<Value, String> {
        match self.call(ResearchCommand::Research { request }).await? {
            WorkerReply::Product(value) => Ok(value),
            other => panic!("Research request returned non-product: {other:?}"),
        }
    }

    pub(super) async fn market(&mut self, request: MarketRequest) -> Result<Value, String> {
        match self
            .call(ResearchCommand::Market {
                request,
                universe_page: None,
                resolved_instruments: None,
            })
            .await?
        {
            WorkerReply::Product(value) => Ok(value),
            other => panic!("Market request returned non-product: {other:?}"),
        }
    }

    pub(super) async fn terminal(&mut self, job: &str) -> Value {
        tokio::time::timeout(Duration::from_secs(45), async {
            loop {
                let status = self
                    .research(ResearchRequest::Status {
                        job_id: job.to_owned(),
                    })
                    .await
                    .unwrap();
                if matches!(
                    status["state"].as_str(),
                    Some("completed" | "failed" | "cancelled" | "interrupted")
                ) {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("Research owner child did not complete")
    }

    pub(super) fn source_reads(&self) -> usize {
        self.source_reads
    }

    pub(super) fn child_pid(&self) -> u32 {
        self.child.id().expect("live Research child PID")
    }

    pub(super) fn root(&self) -> &std::path::Path {
        &self.root
    }

    pub(super) async fn close(self) {
        let Self {
            root,
            mut child,
            input,
            output,
            ..
        } = self;
        drop(input);
        drop(output);
        let exit = tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(
            exit.success(),
            "Research owner child failed to close: {exit}"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    pub(super) async fn restart(self) -> Self {
        let Self {
            root,
            mut child,
            input,
            output,
            scope,
            source,
            source_reads,
            ..
        } = self;
        drop(input);
        drop(output);
        assert!(
            tokio::time::timeout(Duration::from_secs(10), child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
        let session = WorkerSession::new("research-owner-reopened-session".into()).unwrap();
        let (child, input, output) = start(&root, &session, &scope).await;
        Self {
            root,
            child,
            input,
            output,
            session,
            scope,
            next: 0,
            source,
            source_reads,
        }
    }
}

pub(super) fn candle_source(request: PublicSourceRequest) -> Result<PublicSourceReply, String> {
    let PublicSourceRequest::Candles { after, .. } = request else {
        return Err("UNEXPECTED_SOURCE".into());
    };
    let cursor = after.as_deref().unwrap_or("0").parse::<u64>().unwrap();
    let rows = (0..200_u64)
        .rev()
        .filter(|index| index * 60_000 < cursor)
        .take(100)
        .map(|index| HistoryCandle {
            open_time_ms: (index * 60_000).to_string(),
            open: (100 + index).to_string(),
            high: (102 + index).to_string(),
            low: (99 + index).to_string(),
            close: (101 + index).to_string(),
            volume: "1".into(),
            volume_currency: "1".into(),
            volume_quote: None,
            confirmed: true,
        })
        .collect();
    Ok(PublicSourceReply::Candles(PublicCandleCapture {
        raw_body: b"research-owner-closed-candles".to_vec(),
        rows,
        acquired_at_ms: "9000000".into(),
        http_delivery: None,
    }))
}

fn historical_source(request: PublicSourceRequest) -> Result<PublicSourceReply, String> {
    match request {
        PublicSourceRequest::Candles { .. } => candle_source(request),
        PublicSourceRequest::OpenInterest { instrument, .. } => {
            // T06: an off-grid observation may not be silently shifted onto
            // the previous close. The second instrument has a 1ms mismatch.
            let shifted = instrument == "ETH-USDT-SWAP";
            Ok(PublicSourceReply::OpenInterest(
                (0..100_u64)
                    .map(|index| ResearchOpenInterest {
                        ts: (index * 60_000 + u64::from(shifted && index == 42)).to_string(),
                        oi: (100 + index).to_string(),
                        oi_currency: (100 + index).to_string(),
                    })
                    .collect(),
            ))
        }
        PublicSourceRequest::Funding { after, .. } => {
            let cursor = after.as_deref().unwrap_or("0").parse::<u64>().unwrap();
            let rows = [60_000_u64, 99 * 60_000]
                .into_iter()
                .filter(|value| *value < cursor)
                .map(|value| FundingHistoryEvent {
                    funding_time_ms: value.to_string(),
                    funding_rate: "0.002".into(),
                    realized_rate: Some("0.001".into()),
                    formula_type: None,
                    method: None,
                })
                .collect();
            Ok(PublicSourceReply::Funding(PublicFundingCapture {
                raw_body: b"research-owner-realized-funding".to_vec(),
                rows,
                acquired_at_ms: "9000000".into(),
                http_delivery: None,
            }))
        }
        _ => Err("UNEXPECTED_SOURCE".into()),
    }
}

pub(super) async fn complete_market(worker: &mut RealWorker, request: MarketRequest) -> Vec<Value> {
    let first = worker.market(request.clone()).await.unwrap();
    let mut response = first.clone();
    if response["rows"].is_null() {
        let job = response["id"].as_str().expect("persisted market job id");
        assert_eq!(worker.terminal(job).await["state"], "completed");
        response = worker.market(request.clone()).await.unwrap();
    }
    let before_pages = worker.source_reads;
    let mut rows = Vec::new();
    loop {
        rows.extend(
            response["rows"]
                .as_array()
                .expect("materialized market rows")
                .iter()
                .cloned(),
        );
        let Some(cursor) = response["next_cursor"].as_str() else {
            break;
        };
        let mut next = request.clone();
        next.cursor = Some(cursor.to_owned());
        response = worker.market(next).await.unwrap();
    }
    assert_eq!(
        worker.source_reads, before_pages,
        "immutable Research market result pagination must not refetch public sources"
    );
    rows
}

async fn causal_market_source_boundary() {
    let mut worker = RealWorker::open("t06-research-causal", Box::new(historical_source)).await;
    let request: MarketRequest = serde_json::from_value(json!({
        "action":"screen","instruments":["BTC-USDT-SWAP"],"fields":["returns"],
        "timeframe_ms":60000,"start_ms":0,"end_ms":6000000,"limit":1,
        "composition":{"version":"okx.market.composition/v1","steps":[
            {"id":"price_change","op":"returns_series","input":"close"},
            {"id":"price_signal","op":"greater_than","input":"price_change","threshold":0},
            {"id":"oi_change","op":"returns_series","input":"open_interest"},
            {"id":"oi_signal","op":"greater_than","input":"oi_change","threshold":0},
            {"id":"funding_observed","op":"rolling_mean","input":"funding_rate","period":1},
            {"id":"funding_signal","op":"greater_than","input":"funding_rate","threshold":0}
        ],"result_steps":["price_signal","oi_change","oi_signal","funding_observed","funding_signal"]}
    }))
    .unwrap();
    assert!(request.valid());
    let rows = complete_market(&mut worker, request).await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row["metrics"]["price_signal"], true);
    assert_eq!(
        row["composition_quality"]["price_signal"]["status"],
        "available_at_closed_bar"
    );
    assert!(row["metrics"]["oi_change"].is_number());
    assert_eq!(
        row["composition_quality"]["oi_change"]["status"],
        "retrospective_publication_unverified"
    );
    assert!((row["metrics"]["funding_observed"].as_f64().unwrap() - 0.001).abs() < 1e-12);
    assert_eq!(
        row["composition_quality"]["funding_observed"]["status"],
        "retrospective_publication_unverified"
    );
    for denied in ["oi_signal", "funding_signal"] {
        assert!(
            row["metrics"][denied].is_null(),
            "future source became causal: {denied}"
        );
        assert_eq!(
            row["composition_quality"][denied]["issues"][0]["reason"],
            "CAUSAL_PUBLICATION_TIME_UNVERIFIED"
        );
    }
    assert_eq!(
        row["source_integrity"]["quality"],
        "verified_normalized_sources"
    );
    assert_eq!(
        row["source_integrity"]["historical_publication_time_verified"],
        false
    );
    assert_eq!(
        row["source_integrity"]["metric_reexecution"]["quality"],
        "verified_reexecution"
    );
    assert!(worker.source_reads >= 3);
    worker.close().await;
}

async fn off_grid_oi_must_remain_instrument_scoped() {
    let mut worker = RealWorker::open("t06-research-oi-scope", Box::new(historical_source)).await;
    let request: MarketRequest = serde_json::from_value(json!({
        "action":"screen", "instruments":["BTC-USDT-SWAP","ETH-USDT-SWAP"],
        "fields":["returns"],"timeframe_ms":60000,"start_ms":0,"end_ms":6000000,"limit":2,
        "composition":{"version":"okx.market.composition/v1","steps":[
            {"id":"price_return","op":"returns_series","input":"close"},
            {"id":"price_signal","op":"greater_than","input":"price_return","threshold":0},
            {"id":"oi_return","op":"returns_series","input":"open_interest"},
            {"id":"oi_signal","op":"greater_than","input":"oi_return","threshold":0}
        ],"result_steps":["price_signal","oi_return","oi_signal"]}
    }))
    .unwrap();
    let rows = complete_market(&mut worker, request).await;
    assert_eq!(rows.len(), 2);
    let row = |instrument| {
        rows.iter()
            .find(|row| row["instrument"] == instrument)
            .expect("instrument-specific Research evidence missing")
    };
    let aligned = row("BTC-USDT-SWAP");
    let shifted = row("ETH-USDT-SWAP");
    assert_eq!(aligned["source_alignment"]["aligned_observations"], 100);
    assert_eq!(aligned["source_alignment"]["oi_off_grid_events"], 0);
    assert!(aligned["metrics"]["oi_return"].is_number());
    assert!(shifted["metrics"]["oi_return"].is_null());
    assert_eq!(shifted["source_alignment"]["aligned_observations"], 99);
    assert_eq!(shifted["source_alignment"]["oi_off_grid_events"], 1);
    assert_eq!(
        shifted["source_alignment"]["oi_first_off_grid_ms"],
        42 * 60_000 + 1
    );
    assert_eq!(shifted["oi_coverage"]["first_missing_ms"], 42 * 60_000);
    assert_eq!(
        shifted["composition_quality"]["oi_return"]["issues"][0]["reason"],
        "OI_RANGE_INCOMPLETE_OR_GAPPED"
    );
    assert_ne!(
        aligned["source_captures"]["open_interest"]["capture_hash"],
        shifted["source_captures"]["open_interest"]["capture_hash"]
    );
    assert_eq!(
        worker.source_reads, 4,
        "two instrument price and OI captures"
    );
    worker.close().await;
}

pub(super) async fn causal_economics_and_portfolio_are_research_only() {
    causal_market_source_boundary().await;
    off_grid_oi_must_remain_instrument_scoped().await;
    let mut worker = RealWorker::open("t06-t10-research-economics", Box::new(candle_source)).await;
    let cases = json!([{
        "instrument":"BTC-USDT-SWAP","product":"linear_swap","side":"long",
        "quantity":"3","entry_price":"100","exit_price":"110",
        "contract_type":"linear","contract_value":"0.1",
        "contract_value_currency":"BTC","settle_currency":"USDT",
        "fees":{"currency":"USDT","amount":"0"},
        "funding":{"currency":"USDT","amount":"0"}
    },{
        "instrument":"ETH-USD","product":"spot","side":"long",
        "quantity":"1","entry_price":"20","exit_price":"22",
        "fees":{"currency":"USD","amount":"0"},
        "funding":{"currency":"USD","amount":"0"}
    }]);
    let counterfactual = serde_json::from_value(json!({
        "action":"economics","cases":cases,"data_mode":"counterfactual"
    }))
    .unwrap();
    let before = worker.source_reads;
    let economy = worker.research(counterfactual).await.unwrap();
    assert_eq!(economy["rows"][0]["net"]["amount_exact"], "3");
    assert_eq!(economy["rows"][0]["net"]["currency"], "USDT");
    assert_eq!(economy["rows"][1]["net"]["amount_exact"], "2");
    assert_eq!(economy["rows"][1]["net"]["currency"], "USD");
    assert!(economy["portfolio_equity"].is_null());
    assert!(economy["joint_portfolio_risk"].is_null());
    assert_eq!(
        economy["summary"]["aggregation"],
        "independent_positions_no_fx_no_joint_risk"
    );
    assert_eq!(economy["effects"]["venue_posts"], 0);
    assert_eq!(economy["effects"]["finance_writes"], 0);
    let historical = serde_json::from_value(json!({
        "action":"economics","cases":cases,"data_mode":"historical_observed"
    }))
    .unwrap();
    let denied = worker.research(historical).await.unwrap();
    assert_eq!(denied["status"], "blocked");
    assert_eq!(
        denied["reason_code"],
        "HISTORICAL_RULES_AND_FEES_UNAVAILABLE"
    );
    assert_eq!(denied["effects"]["job_created"], false);
    assert_eq!(
        worker.source_reads, before,
        "economics must never fetch a source"
    );

    let request: PortfolioScenarioRequest = serde_json::from_value(json!({
        "equity_usd":"100",
        "positions":[
            {"instrument":"BTC-USDT-SWAP","side":"long","notional_usd":"100","margin_usd":"10",
             "price_shock_bps":-1000,"volatility_bps":300,"liquidity_cost_bps":100},
            {"instrument":"ETH-USDT-SWAP","side":"short","notional_usd":"200","margin_usd":"20",
             "price_shock_bps":500,"volatility_bps":200,"liquidity_cost_bps":50}
        ],
        "correlations":[{"left":0,"right":1,"rho":0.0}]
    }))
    .unwrap();
    assert!(request.valid());
    let values = match worker
        .call(ResearchCommand::PortfolioScenario {
            request: request.clone(),
        })
        .await
        .unwrap()
    {
        WorkerReply::Product(value) => value,
        other => panic!("unexpected portfolio reply: {other:?}"),
    };
    for (key, expected) in [
        ("schema", "okx.portfolio.scenario/v1"),
        ("quality", "assumed"),
        ("execution", "virtual_read_only"),
        ("gross_notional_usd", "300"),
        ("margin_usd", "30"),
        ("net_stress_pnl_usd", "-22"),
        ("stressed_equity_usd", "78"),
        ("drawdown_ratio", "0.22"),
    ] {
        assert_eq!(values[key], expected, "portfolio field {key}");
    }
    assert_eq!(values["correlated_volatility_usd"], 5.0);
    assert_eq!(values["exchange_post"], false);
    assert_eq!(values["venue_margin_verified"], false);
    let mut non_psd = request.clone();
    non_psd.positions.push(non_psd.positions[0].clone());
    non_psd.correlations = serde_json::from_value(json!([
        {"left":0,"right":1,"rho":1.0},
        {"left":0,"right":2,"rho":1.0},
        {"left":1,"right":2,"rho":-1.0}
    ]))
    .unwrap();
    assert!(non_psd.valid());
    assert_eq!(
        worker
            .call(ResearchCommand::PortfolioScenario { request: non_psd })
            .await
            .unwrap_err(),
        "CORRELATION_NOT_PSD"
    );
    for invalid in [
        {
            let mut invalid = request.clone();
            invalid.positions[0].price_shock_bps = -10001;
            invalid
        },
        {
            let mut invalid = request;
            invalid.correlations[0].rho = 1.01;
            invalid
        },
    ] {
        assert_eq!(
            worker
                .call(ResearchCommand::PortfolioScenario { request: invalid })
                .await
                .unwrap_err(),
            "INVALID_COMMAND"
        );
    }
    assert_eq!(worker.source_reads, before);
    worker.close().await;
}

pub(super) async fn cancel_and_durable_decision_replay_are_research_only() {
    exact_candidate_progress_and_read_only_explain_reopen().await;
    // Real worker scheduling and persisted decisions are exercised in this
    // process fixture rather than through the Engine's retired client shim.
    let mut worker = RealWorker::open("t06-t10-research-decision", Box::new(candle_source)).await;
    let request: ResearchRequest = serde_json::from_value(json!({
        "action":"backtest", "idempotency_key":"owner-decision-replay",
        "instruments":["BTC-USDT"], "timeframe_ms":60000,
        "start_ms":0, "end_ms":12000000, "strategy_id":"ema_cross_v1",
        "parameters":{"fast":2,"slow":3},
        "costs":{"maker_fee_bps":1,"taker_fee_bps":2,
                 "slippage_bps":1,"funding_mode":"zero_assumption"},
        "data_mode":"counterfactual", "seed":7
    }))
    .unwrap();
    let queued = worker.research(request.clone()).await.unwrap();
    let job_id = queued["id"].as_str().unwrap().to_owned();
    let completed = worker.terminal(&job_id).await;
    assert_eq!(completed["state"], "completed", "{completed}");
    let result_id = completed["artifact"].as_str().unwrap().to_owned();
    let before = worker.source_reads;
    let record: ResearchRequest = serde_json::from_value(json!({
        "action":"record_decision", "job_id":job_id, "result_id":result_id,
        "expected_decision_id":null, "name":"Experiment A",
        "hypothesis":"Independent evidence", "status":"deferred", "reason":"Await review"
    }))
    .unwrap();
    let first = worker.research(record.clone()).await.unwrap();
    let replay = worker.research(record).await.unwrap();
    assert_eq!(first, replay);
    assert_eq!(first["trading_authority"], false);
    assert_eq!(
        worker.source_reads, before,
        "decisions cannot refetch sources"
    );
    let decision_id = first["decision_id"].as_str().unwrap().to_owned();
    let read = worker
        .research(ResearchRequest::Decision {
            job_id: job_id.clone(),
            decision_id: Some(decision_id.clone()),
        })
        .await
        .unwrap();
    assert_eq!(read, first);
    let other = {
        let mut other = request;
        if let ResearchRequest::Backtest {
            idempotency_key,
            seed,
            ..
        } = &mut other
        {
            *idempotency_key = "owner-foreign-result".into();
            *seed = 8;
        }
        other
    };
    let foreign = worker.research(other).await.unwrap();
    let foreign_job = foreign["id"].as_str().unwrap().to_owned();
    assert_eq!(worker.terminal(&foreign_job).await["state"], "completed");
    assert!(
        worker
            .research(ResearchRequest::Decision {
                job_id: foreign_job,
                decision_id: Some(decision_id),
            })
            .await
            .is_err()
    );
    assert!(worker.source_reads >= before);
    worker = worker.restart().await;
    let reopened = worker
        .research(ResearchRequest::Decision {
            job_id,
            decision_id: Some(first["decision_id"].as_str().unwrap().to_owned()),
        })
        .await
        .unwrap();
    assert_eq!(reopened, first, "decision must survive process restart");
    worker.close().await;
    cancelled_job_rejects_late_source_after_restart().await;
}

async fn exact_candidate_progress_and_read_only_explain_reopen() {
    let mut worker = RealWorker::open("t06-research-128-candidates", Box::new(candle_source)).await;
    let grid = ResearchRequest::Grid {
        idempotency_key: "owner-128-candidate-progress".into(),
        instruments: vec!["BTC-USDT".into(), "ETH-USDT".into()],
        timeframe_ms: 60_000,
        start_ms: 0,
        end_ms: 12_000_000,
        strategy_id: "ema_cross_v1".into(),
        variants: (3..67)
            .map(|slow| okx_research_protocol::analytics::EmaParameters { fast: 2, slow }.into())
            .collect(),
        costs: serde_json::from_value(json!({
            "maker_fee_bps":1,"taker_fee_bps":2,"slippage_bps":1,
            "funding_mode":"zero_assumption"
        }))
        .unwrap(),
        data_mode: okx_research_protocol::analytics::ResearchDataMode::Counterfactual,
        seed: 7,
    };
    assert!(grid.valid());
    let invocation = InvocationId::new("query-dispatch-request-001".into()).unwrap();
    let queued = match worker
        .call_with_invocation(
            ResearchCommand::Research { request: grid },
            Some(invocation.clone()),
        )
        .await
        .unwrap()
    {
        WorkerReply::Product(value) => value,
        other => panic!("wrong Research enqueue reply: {other:?}"),
    };
    let job = queued["id"]
        .as_str()
        .expect("durable grid job id")
        .to_owned();
    let status = worker.terminal(&job).await;
    assert_eq!(status["state"], "completed", "{status}");
    assert_eq!(status["origin_invocation"], invocation.as_str());
    assert_eq!(
        status["producer"]["binary_sha256"],
        expected_worker_sha256()
    );
    assert_eq!(status["candidate_progress"]["completed_candidates"], 128);
    assert_eq!(status["candidate_progress"]["total_candidates"], 128);
    assert_eq!(status["candidate_progress"]["completed_work_units"], 386);
    assert_eq!(status["candidate_progress"]["total_work_units"], 386);
    assert_eq!(status["progress"], 100);
    let summary = worker
        .research(ResearchRequest::Result {
            job_id: job.clone(),
            cursor: None,
            limit: 1,
            view: okx_research_protocol::analytics::ResearchResultView::Summary,
        })
        .await
        .unwrap();
    assert_eq!(summary["metadata"]["job_id"], job);
    assert_eq!(
        summary["metadata"]["producer"]["binary_sha256"],
        expected_worker_sha256(),
        "result metadata must bind the same actual Worker executable"
    );
    let before = worker.source_reads();
    for _ in 0..2 {
        let product = match worker
            .call(ResearchCommand::Explain {
                job_id: okx_research_protocol::research_ipc::JobId::new(job.clone()).unwrap(),
            })
            .await
            .unwrap()
        {
            WorkerReply::Product(value) => value,
            other => panic!("Research Explain reply was not a product: {other:?}"),
        };
        assert_eq!(product["job"]["state"], "completed");
        assert_eq!(product["job"]["origin_invocation"], invocation.as_str());
        assert_eq!(
            product["job"]["producer"]["binary_sha256"],
            expected_worker_sha256()
        );
    }
    assert_eq!(
        worker.source_reads(),
        before,
        "Explain must not acquire market sources"
    );
    worker = worker.restart().await;
    // The previous child was joined before this handshake. A new process is
    // certain, but bare Windows PIDs may legally be recycled immediately.
    assert!(worker.child_pid() > 0);
    let reopened = worker.terminal(&job).await;
    assert_eq!(reopened["origin_invocation"], invocation.as_str());
    assert_eq!(
        reopened["producer"]["binary_sha256"],
        expected_worker_sha256()
    );
    assert_eq!(reopened["candidate_progress"]["completed_candidates"], 128);
    assert_eq!(reopened["candidate_progress"]["completed_work_units"], 386);
    assert_eq!(
        worker.source_reads(),
        before,
        "durable replay must not refetch sources"
    );
    worker.close().await;
}

async fn cancelled_job_rejects_late_source_after_restart() {
    let mut worker = RealWorker::open("t06-t10-research-cancel", Box::new(candle_source)).await;
    let grid: ResearchRequest = serde_json::from_value(json!({
        "action":"grid", "idempotency_key":"owner-cancel-grid",
        "instruments":["BTC-USDT"], "timeframe_ms":60000,
        "start_ms":0, "end_ms":12000000, "strategy_id":"ema_cross_v1",
        "variants":[{"fast":2,"slow":5},{"fast":3,"slow":8}],
        "costs":{"maker_fee_bps":1,"taker_fee_bps":2,
                 "slippage_bps":1,"funding_mode":"zero_assumption"},
        "data_mode":"counterfactual", "seed":7
    }))
    .unwrap();
    let enqueue_id = RequestId::new("h1".into()).unwrap();
    send(
        &mut worker.input,
        HostFrame::Request {
            invocation: None,
            id: enqueue_id.clone(),
            session: worker.session.clone(),
            command: Box::new(ResearchCommand::Research { request: grid }),
        },
    )
    .await;
    let (mut queued_job, mut held_source) = (None, None);
    tokio::time::timeout(Duration::from_secs(10), async {
        while queued_job.is_none() || held_source.is_none() {
            match receive(&mut worker.output).await {
                WorkerFrame::Response {
                    id,
                    result: Ok(WorkerReply::Product(value)),
                    ..
                } if id == enqueue_id => queued_job = value["id"].as_str().map(str::to_owned),
                WorkerFrame::SourceRequest { id, session, .. } => {
                    if held_source.is_none() {
                        held_source = Some((id, session));
                    }
                }
                other => panic!("unexpected held Research source frame: {other:?}"),
            }
        }
    })
    .await
    .expect("real child must queue and request public source");
    worker.next = 2;
    let job_id = queued_job.unwrap();
    let cancel_id = RequestId::new("h2".into()).unwrap();
    send(
        &mut worker.input,
        HostFrame::Request {
            invocation: None,
            id: cancel_id.clone(),
            session: worker.session.clone(),
            command: Box::new(ResearchCommand::Research {
                request: ResearchRequest::Cancel {
                    job_id: job_id.clone(),
                    idempotency_key: "owner-cancel-once".into(),
                },
            }),
        },
    )
    .await;
    loop {
        match receive(&mut worker.output).await {
            WorkerFrame::Response {
                id,
                result: Ok(WorkerReply::Product(value)),
                ..
            } if id == cancel_id => {
                assert_eq!(value["state"], "cancelled");
                break;
            }
            WorkerFrame::SourceRequest { .. } => {}
            other => panic!("unexpected cancellation response: {other:?}"),
        }
    }
    let (source_id, session) = held_source.unwrap();
    send(
        &mut worker.input,
        HostFrame::SourceReply {
            id: source_id,
            session,
            result: Err("FIXTURE_LATE_SOURCE_FENCED".into()),
        },
    )
    .await;
    assert_eq!(worker.terminal(&job_id).await["state"], "cancelled");
    worker = worker.restart().await;
    assert_eq!(worker.terminal(&job_id).await["state"], "cancelled");
    worker.close().await;
}

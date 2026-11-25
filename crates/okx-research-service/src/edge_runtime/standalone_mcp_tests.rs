use super::*;
use crate::analytics::jobs::{Jobs, State};
use okx_research::ResearchArtifactStore;
use okx_research_protocol::analytics::{
    ResearchDecisionStatus, ResearchRequest, ResearchResultView,
};
use serde_json::{Value, json};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};

async fn call(client: &reqwest::Client, url: &str, tool: &str, args: Value) -> Value {
    let reply = tokio::time::timeout(
        Duration::from_secs(10),
        client
            .post(url)
            .header("content-type", "application/json")
            .body(
                json!({
                    "jsonrpc":"2.0", "id":1, "method":"tools/call",
                    "params":{"name":tool,"arguments":args}
                })
                .to_string(),
            )
            .send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(reply.status().is_success());
    let wire: Value = serde_json::from_str(&reply.text().await.unwrap()).unwrap();
    assert!(wire.get("error").is_none(), "MCP error: {wire}");
    wire["result"]["structuredContent"].clone()
}

#[tokio::test]
#[ignore = "requires Node, installed Miniflare dependencies, and local loopback"]
async fn standalone_catalog_reopens_through_real_mcp_do_websocket() {
    let root = tempfile::tempdir().unwrap();
    let scope = ResearchScope::new("standalone-mcp-fixture".into()).unwrap();
    let producer = Producer::new("c".repeat(64), None, None).unwrap();
    let public = Arc::new(IndependentOkxPublicSource::new().unwrap());
    let source = PublicSourceClient::new(public.clone());
    let store = ResearchArtifactStore::at(root.path().to_path_buf());
    let dataset = store.publish_evidence(&json!({"chunks":[]})).unwrap().0;
    let experiment = store
        .publish_evidence(&json!({
            "schema":"okx.research.experiment-inputs/v1", "dataset_artifact":dataset,
            "code_version":"decision-fixture", "spec":{"seed":7,"costs":{"taker_fee_bps":2}},
            "variants":[{"fast":2,"slow":5}]
        }))
        .unwrap()
        .0;
    let result = store.publish_evidence(&json!({
        "trade_pages":[],"total_trades":0,"equity_curve_pages":[],"total_equity_points":0,
        "dataset_artifact":dataset,"experiment_artifact":experiment,"code_version":"decision-fixture"
    })).unwrap().0;
    let job = "a".repeat(64);
    let jobs = Jobs::open_scoped(root.path().join("research-catalog.sqlite"), &scope).unwrap();
    jobs.enqueue("standalone-fixture".into(), job.clone())
        .unwrap();
    jobs.update(&job, State::Completed, Some(result.clone()), None)
        .unwrap();
    drop(jobs);
    let old = AnalyticsService::new_scoped_with_producer(
        root.path().to_path_buf(),
        source.clone(),
        &scope,
        producer.clone(),
    )
    .unwrap();
    let record = ResearchRequest::RecordDecision {
        job_id: job.clone(),
        result_id: result.clone(),
        expected_decision_id: None,
        name: "Experiment A".into(),
        hypothesis: "Original hypothesis".into(),
        status: ResearchDecisionStatus::Deferred,
        reason: "Await independent evidence".into(),
    };
    let expected_decision = old.research(record.clone()).await.unwrap();
    let status_request = ResearchRequest::Status {
        job_id: job.clone(),
    };
    let expected_status = old.research(status_request).await.unwrap();
    let result_request = ResearchRequest::Result {
        job_id: job.clone(),
        cursor: None,
        limit: 10,
        view: ResearchResultView::Summary,
    };
    let expected_result = old.research(result_request).await.unwrap();
    let result_bytes = store.read_evidence(&result).unwrap();
    let decision_id = expected_decision["decision_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let decision_bytes = store.read_evidence(&decision_id).unwrap();
    drop(old);
    let service = AnalyticsService::new_existing_scoped_with_producer(
        root.path().to_path_buf(),
        source,
        &scope,
        producer.clone(),
    )
    .unwrap();

    let mcp = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../cloudflare/okx-mcp");
    let mut host = tokio::process::Command::new("node")
        .arg("scripts/t10-mcp-load-host.mjs")
        .arg("--research-only")
        .current_dir(mcp)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(host.stdout.take().unwrap());
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(30), stdout.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    let ready: Value = serde_json::from_str(
        line.strip_prefix("T10_MCP_LOAD_HOST_READY:")
            .unwrap_or_else(|| panic!("expected local MCP host readiness, got {line:?}")),
    )
    .unwrap();
    assert_eq!(ready["schema"], "okx.518.t10-mcp-load-host/v1");
    let http = ready["http_url"].as_str().unwrap();
    let ws = ready["ws_url"].as_str().unwrap();
    assert!(http.starts_with("http://127.0.0.1:"));
    assert!(ws.starts_with("ws://127.0.0.1:"));
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let offline = call(
        &client,
        http,
        "research",
        json!({"action":"status","job_id":job}),
    )
    .await;
    assert_eq!(
        offline["reason_code"], "RESEARCH_RUNTIME_OFFLINE",
        "{offline}"
    );
    let config = EdgeRuntimeConfig {
        url: ws.into(),
        runtime_id: "windows-primary".into(),
        host_scope: "a".repeat(64),
        incarnation: format!("ctl_{}", "b".repeat(32)),
        token: Zeroizing::new("integration-only-research-ephemeral-token".into()),
    };
    let mut request = ws.into_client_request().unwrap();
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer integration-only-research-ephemeral-token"),
    );
    let (socket, _) = tokio::time::timeout(Duration::from_secs(10), connect_async(request))
        .await
        .unwrap()
        .unwrap();
    let task = tokio::spawn(async move {
        run_session(socket, config.hello(&producer), &service, &public).await
    });
    let status_args = json!({"action":"status","job_id":job});
    let status = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let value = call(&client, http, "research", status_args.clone()).await;
            if value["status"] == "completed" {
                break value;
            }
            assert_eq!(
                value["reason_code"], "RESEARCH_RUNTIME_OFFLINE",
                "Research became reachable but could not read the catalog: {value}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(status["data"], expected_status);
    assert_eq!(
        call(&client, http, "research", status_args).await["data"],
        expected_status
    );
    let view = call(
        &client,
        http,
        "research",
        json!({"action":"result","job_id":job,"limit":10,"view":"summary"}),
    )
    .await;
    assert_eq!(view["data"], expected_result, "{view}");
    let read = call(
        &client,
        http,
        "research",
        json!({"action":"decision","job_id":job}),
    )
    .await;
    assert_eq!(read["data"], expected_decision, "{read}");
    let replay = call(
        &client,
        http,
        "research",
        json!({"action":"record_decision","job_id":job,
        "result_id":result,"name":"Experiment A","hypothesis":"Original hypothesis",
        "status":"deferred","reason":"Await independent evidence"}),
    )
    .await;
    assert_eq!(replay["data"], expected_decision, "{replay}");
    assert_eq!(store.read_evidence(&result).unwrap(), result_bytes);
    assert_eq!(store.read_evidence(&decision_id).unwrap(), decision_bytes);
    let trade = call(
        &client,
        http,
        "trade.capabilities",
        json!({"account":"independent-fixture"}),
    )
    .await;
    assert_eq!(
        trade["reason_code"], "RUNTIME_OFFLINE",
        "Research must not provide trade: {trade}"
    );
    task.abort();
    let _ = task.await;
    let offline = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let value = call(
                &client,
                http,
                "research",
                json!({"action":"status","job_id":job}),
            )
            .await;
            if value["reason_code"] == "RESEARCH_RUNTIME_OFFLINE" {
                break value;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    assert_ne!(offline["status"], "completed");
    drop(host.stdin.take());
    assert!(
        tokio::time::timeout(Duration::from_secs(10), host.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
}

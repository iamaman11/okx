use super::*;
use okx_ipc_contracts::{
    analytics::{EmaParameters, FundingMode, ResearchCosts, ResearchDataMode},
    research_ipc::{HistoryCandle, PublicCandleCapture},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{fs, io::Write, sync::atomic::AtomicUsize};

fn stage(runtime: &std::path::Path, executable: &std::path::Path, overlay: bool) -> String {
    let mut bytes = fs::read(executable).expect("read real worker executable");
    if overlay {
        bytes.extend_from_slice(b"okx activation test image overlay v2");
    }
    let hash = format!("{:x}", Sha256::digest(&bytes));
    let dir = runtime.join("research").join("versions").join(&hash);
    fs::create_dir_all(&dir).unwrap();
    let image = dir.join("okx-research-service.exe");
    fs::copy(executable, &image).unwrap();
    if overlay {
        fs::OpenOptions::new()
            .append(true)
            .open(&image)
            .unwrap()
            .write_all(b"okx activation test image overlay v2")
            .unwrap();
    }
    hash
}

fn pointer(runtime: &std::path::Path, hash: &str) {
    let record = json!({"schema":"okx.host-control.installed-research/v1", "repository_id":1_388_071_566u64,
        "run_id":1, "artifact_id":2, "source_head_sha":"a".repeat(40), "source_tree":"b".repeat(40),
        "worker_sha256":hash, "protocol_version":PROTOCOL_VERSION, "storage_version":STORAGE_VERSION});
    fs::write(runtime.join("installed-research.json"), record.to_string()).unwrap();
}

fn job(key: &str) -> ResearchRequest {
    ResearchRequest::Backtest {
        idempotency_key: key.into(),
        instruments: vec!["BTC-USDT".into()],
        timeframe_ms: 60_000,
        start_ms: 0,
        end_ms: 12_000_000,
        strategy_id: "ema_cross_v1".into(),
        parameters: EmaParameters { fast: 2, slow: 3 }.into(),
        costs: ResearchCosts {
            maker_fee_bps: 1.0,
            taker_fee_bps: 2.0,
            slippage_bps: 1.0,
            funding_mode: FundingMode::ZeroAssumption,
        },
        data_mode: ResearchDataMode::Counterfactual,
        seed: 7,
    }
}

#[tokio::test]
async fn installed_worker_drains_and_replays_across_actual_child_handoff() {
    let executable = PathBuf::from(
        std::env::var_os("OKX_RESEARCH_SERVICE_TEST_EXE")
            .expect("set OKX_RESEARCH_SERVICE_TEST_EXE to the built worker"),
    );
    assert!(executable.is_file());
    let root = std::env::temp_dir().join(format!("okx-installed-activation-{}", unique()));
    let runtime = root.join("runtime");
    let catalog = root.join("catalog");
    fs::create_dir_all(&runtime).unwrap();
    let v1 = stage(&runtime, &executable, false);
    let v2 = stage(&runtime, &executable, true);
    assert_ne!(v1, v2);
    pointer(&runtime, &v1);
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let source: SourceHandler = Arc::new({
        let entered = entered.clone();
        let release = release.clone();
        let calls = calls.clone();
        move |request| {
            let entered = entered.clone();
            let release = release.clone();
            let calls = calls.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                let PublicSourceRequest::Candles { after, .. } = request else {
                    return Err("UNEXPECTED_SOURCE".into());
                };
                entered.notify_one();
                release.notified().await;
                let cursor = after.as_deref().unwrap_or("0").parse::<u64>().unwrap();
                let rows = (0..200u64)
                    .rev()
                    .filter(|i| i * 60_000 < cursor)
                    .take(100)
                    .map(|i| HistoryCandle {
                        open_time_ms: (i * 60_000).to_string(),
                        open: (100 + i).to_string(),
                        high: (102 + i).to_string(),
                        low: (99 + i).to_string(),
                        close: (101 + i).to_string(),
                        volume: "1".into(),
                        volume_currency: "1".into(),
                        volume_quote: None,
                        confirmed: true,
                    })
                    .collect();
                Ok(PublicSourceReply::Candles(PublicCandleCapture {
                    raw_body: b"fixture".to_vec(),
                    rows,
                    acquired_at_ms: "1".into(),
                    http_delivery: None,
                }))
            })
        }
    });
    let scope = ResearchScope::new("activation-scope".into()).unwrap();
    let mut client = ResearchClient::new(
        ResearchClientConfig {
            executable: PathBuf::new(),
            root: catalog.clone(),
            scope: scope.clone(),
        },
        source,
    );
    client.installation_runtime = Some(runtime.clone());
    client.capabilities().await.unwrap();
    let initial = client.deployment_status();
    assert_eq!(initial.active.as_deref(), Some(v1.as_str()));
    assert!(initial.active_pid.is_some());
    assert!(initial.handshake_verified_at_ms.is_some());
    let first_session = client
        .connection
        .lock()
        .await
        .as_ref()
        .unwrap()
        .session
        .clone();
    let request = job("activation-idempotency");
    let accepted = client.research(request.clone()).await.unwrap();
    let id = accepted["id"].as_str().unwrap().to_owned();
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("source request");
    fs::write(runtime.join("installed-research.json"), b"{}").unwrap();
    assert_eq!(
        client
            .research(ResearchRequest::Status { job_id: id.clone() })
            .await
            .unwrap()["id"],
        id
    );
    assert_eq!(
        client.deployment_status().active.as_deref(),
        Some(v1.as_str())
    );
    pointer(&runtime, &v2);
    client.capabilities().await.unwrap();
    let status = client.deployment_status();
    assert_eq!(status.installed.as_deref(), Some(v2.as_str()));
    assert_eq!(status.active.as_deref(), Some(v1.as_str()));
    assert_eq!(status.active_pid, initial.active_pid);
    assert!(status.draining);
    assert_eq!(
        client.research(job("new-during-drain")).await.unwrap_err(),
        "RESEARCH_UPDATING"
    );
    assert_eq!(client.research(request.clone()).await.unwrap()["id"], id);
    let cancelled = client
        .research(ResearchRequest::Cancel {
            job_id: id.clone(),
            idempotency_key: "activation-cancel".into(),
        })
        .await
        .unwrap();
    assert_eq!(cancelled["state"], "cancelled");
    release.notify_waiters();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            client.capabilities().await.unwrap();
            if client.deployment_status().active.as_deref() == Some(v2.as_str()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("worker handoff");
    let second_session = client
        .connection
        .lock()
        .await
        .as_ref()
        .unwrap()
        .session
        .clone();
    assert_ne!(first_session, second_session);
    assert!(client.deployment_status().active_pid.is_some());
    let replay = client.research(request).await.unwrap();
    assert_eq!(replay["id"], id);
    assert_eq!(replay["state"], "cancelled");
    assert_eq!(client.config.scope, scope);
    let inactive_image = runtime
        .join("research")
        .join("versions")
        .join(&v1)
        .join("okx-research-service.exe");
    fs::OpenOptions::new()
        .append(true)
        .open(inactive_image)
        .unwrap()
        .write_all(b"tampered")
        .unwrap();
    pointer(&runtime, &v1);
    let working_pid = client.deployment_status().active_pid;
    client.capabilities().await.unwrap();
    let refused = client.deployment_status();
    assert_eq!(refused.active.as_deref(), Some(v2.as_str()));
    assert_eq!(refused.active_pid, working_pid);
    assert_eq!(
        refused.installed_check_error.as_deref(),
        Some("RESEARCH_INSTALL_HASH_MISMATCH")
    );
    assert!(!refused.draining);
    client.shutdown().await;
    let stopped = client.deployment_status();
    assert!(stopped.active_pid.is_none());
    assert!(stopped.handshake_verified_at_ms.is_none());
    assert_eq!(
        client.capabilities().await.unwrap_err(),
        "RESEARCH_INSTALL_HASH_MISMATCH"
    );
    let db = rusqlite::Connection::open(catalog.join("research-catalog.sqlite")).unwrap();
    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM research_jobs", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
    let stored_scope: String = db
        .query_row(
            "SELECT value FROM research_catalog_metadata WHERE name='scope'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stored_scope, scope.as_str());
    drop(db);
    drop(client);
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn changed_pointer_after_drain_recovers_verified_previous_child() {
    let executable = PathBuf::from(
        std::env::var_os("OKX_RESEARCH_SERVICE_TEST_EXE")
            .expect("set OKX_RESEARCH_SERVICE_TEST_EXE to the built worker"),
    );
    let root = std::env::temp_dir().join(format!("okx-aborted-handoff-{}", unique()));
    let runtime = root.join("runtime");
    fs::create_dir_all(&runtime).unwrap();
    let original = stage(&runtime, &executable, false);
    let update = stage(&runtime, &executable, true);
    pointer(&runtime, &original);
    let source: SourceHandler =
        Arc::new(|_| Box::pin(async { Err("TEST_PUBLIC_SOURCE_NOT_AVAILABLE".into()) }));
    let mut client = ResearchClient::new(
        ResearchClientConfig {
            executable: PathBuf::new(),
            root: root.join("catalog"),
            scope: ResearchScope::new("aborted-handoff".into()).unwrap(),
        },
        source,
    );
    client.installation_runtime = Some(runtime.clone());
    client.capabilities().await.unwrap();
    let before = client.deployment_status();
    assert_eq!(before.active.as_deref(), Some(original.as_str()));
    let previous_session = client
        .connection
        .lock()
        .await
        .as_ref()
        .unwrap()
        .session
        .clone();

    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    client.after_drain_barrier = Some((reached.clone(), release.clone()));
    pointer(&runtime, &update);
    let switching = tokio::spawn({
        let client = client.clone();
        async move { client.capabilities().await }
    });
    tokio::time::timeout(Duration::from_secs(10), reached.notified())
        .await
        .expect("worker reached the after-Drain verification barrier");
    fs::write(runtime.join("installed-research.json"), b"{}").unwrap();
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(10), switching)
        .await
        .expect("verified previous image restarted")
        .unwrap()
        .unwrap();

    let recovered = client.deployment_status();
    assert_eq!(recovered.active.as_deref(), Some(original.as_str()));
    assert!(recovered.active_pid.is_some());
    let recovered_session = client
        .connection
        .lock()
        .await
        .as_ref()
        .unwrap()
        .session
        .clone();
    assert_ne!(recovered_session, previous_session);
    assert!(!recovered.draining);
    assert_eq!(
        recovered.installed_check_error.as_deref(),
        Some("RESEARCH_INSTALL_INVALID_POINTER")
    );
    assert!(
        recovered.handshake_verified_at_ms.unwrap() >= before.handshake_verified_at_ms.unwrap()
    );
    let admitted = client.research(job("after-aborted-handoff")).await.unwrap();
    assert!(
        admitted
            .get("id")
            .and_then(serde_json::Value::as_str)
            .is_some()
    );
    client.shutdown().await;
    drop(client);
    fs::remove_dir_all(root).unwrap();
}

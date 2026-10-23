//! Engine lifecycle: one process, one shutdown signal, account-local owners.
pub use crate::source_capture::capture_account_sources;
use crate::{
    AgentResult,
    cloudflare_transport::{CloudflareTransportConfig, run_cloudflare_transport},
    github_mailbox::{GitHubMailboxClient, MailboxQueryRuntimeContext},
    query::DispatchContext,
    risk_bridge::OutboundRiskCommand,
};
use okx_protocol::{
    NATIVE_CHILD_CONTROL_MAX_BYTES, NATIVE_CHILD_CONTROL_PREFIX, NATIVE_CHILD_CONTROL_SCHEMA_V1,
    NativeChildReply, NativeChildRequest,
};
use okx_runtime::PublicWsCoordinator;
use std::{
    io::{BufRead, Read},
    time::Duration,
};
use tokio::sync::{mpsc, watch};
pub struct EngineContext {
    pub dispatch: DispatchContext,
    pub public: PublicWsCoordinator,
    pub transport: Option<CloudflareTransportConfig>,
    pub risk: mpsc::Receiver<OutboundRiskCommand>,
    pub mailbox: Option<GitHubMailboxClient>,
    pub private_key: zeroize::Zeroizing<[u8; 32]>,
    pub key_id: String,
    pub poll_seconds: u64,
    pub native_child_control_stdio: bool,
}
fn control_reader(tx: mpsc::Sender<NativeChildRequest>) {
    let input = std::io::stdin();
    let mut input = input.lock();
    loop {
        let mut bytes = Vec::new();
        let result = (&mut input)
            .take((NATIVE_CHILD_CONTROL_MAX_BYTES + 1) as u64)
            .read_until(b'\n', &mut bytes);
        if !matches!(result,Ok(n)if n>0&&n<=NATIVE_CHILD_CONTROL_MAX_BYTES)
            || bytes.last() != Some(&b'\n')
        {
            break;
        }
        let Ok(request) = serde_json::from_slice::<NativeChildRequest>(&bytes) else {
            break;
        };
        if !request.valid() || tx.blocking_send(request).is_err() {
            break;
        }
    }
}
pub async fn run_engine(ctx: EngineContext) -> AgentResult<()> {
    let EngineContext {
        dispatch,
        public,
        transport,
        risk,
        mailbox,
        private_key,
        key_id,
        poll_seconds,
        native_child_control_stdio,
    } = ctx;
    let (tx, shutdown) = watch::channel(false);
    let mut tasks = tokio::task::JoinSet::new();
    let trade = dispatch.trade.clone();
    let signal = shutdown.clone();
    tasks.spawn(async move {
        trade.run(signal).await;
    });
    let d = dispatch.clone();
    let mut recovery_shutdown = shutdown.clone();
    tasks.spawn(async move{let mut interval=tokio::time::interval(Duration::from_secs(30));loop{tokio::select!{_=recovery_shutdown.changed()=>break,_=interval.tick()=>d.accounts.recover_unavailable().await}}});
    let signal = shutdown.clone();
    tasks.spawn(async move {
        let _ = public.run(signal).await;
    });
    if let Some(config) = transport {
        let signal = shutdown.clone();
        let d = dispatch.clone();
        tasks.spawn(async move {
            let _ = run_cloudflare_transport(&config, d, risk, signal).await;
        });
    }
    if let Some(mailbox) = mailbox {
        let mut signal = shutdown.clone();
        let d = dispatch.clone();
        tasks.spawn(async move{let mut interval=tokio::time::interval(Duration::from_secs(poll_seconds.clamp(1,60)));interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);loop{tokio::select!{_=signal.changed()=>break,_=interval.tick()=>{if mailbox.process_pending(MailboxQueryRuntimeContext{expected_key_id:&key_id,agent_private_key:&private_key,dispatch:&d}).await.is_err(){eprintln!("mailbox temporarily unavailable");}}}}});
    }
    let (control_tx, mut control) = mpsc::channel(1);
    if native_child_control_stdio {
        std::thread::spawn(move || control_reader(control_tx));
    }
    let request = tokio::select! {_=tokio::signal::ctrl_c()=>None,request=control.recv(),if native_child_control_stdio=>request};
    let _ = tx.send(true);
    let drained = tokio::time::timeout(Duration::from_secs(9), async {
        while tasks.join_next().await.is_some() {}
        dispatch.accounts.shutdown().await;
    })
    .await
    .is_ok();
    tasks.abort_all();
    if let Some(request) = request {
        let reply = NativeChildReply {
            schema: NATIVE_CHILD_CONTROL_SCHEMA_V1.into(),
            request_id: request.request_id,
            outcome: if drained {
                "DRAINED"
            } else {
                "RECOVERY_REQUIRED"
            }
            .into(),
        };
        eprintln!(
            "{}{}",
            NATIVE_CHILD_CONTROL_PREFIX,
            serde_json::to_string(&reply)?
        );
    }
    Ok(())
}

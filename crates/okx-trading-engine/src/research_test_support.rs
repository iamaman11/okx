//! Historical Research child-process fixtures for parity and finance load witnesses.
use crate::{
    market_bootstrap::MarketBootstrapper,
    research_client::{ResearchClient, ResearchClientConfig, SourceHandler},
};
use okx_ipc_contracts::research_ipc::ResearchScope;
use std::{path::PathBuf, sync::Arc};

pub(crate) fn client(root: PathBuf, market: MarketBootstrapper) -> Result<ResearchClient, String> {
    let executable = std::env::var_os("OKX_RESEARCH_SERVICE_TEST_EXE")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let mut path = std::env::current_exe().expect("test executable");
            path.pop();
            if path.ends_with("deps") {
                path.pop();
            }
            path.join(format!(
                "okx-research-service{}",
                std::env::consts::EXE_SUFFIX
            ))
        });
    let source: SourceHandler = Arc::new(move |request| {
        let market = market.clone();
        Box::pin(async move { market.research_source(request).await })
    });
    let scope =
        ResearchScope::new(format!("test-host-{}", std::process::id())).map_err(str::to_owned)?;
    Ok(ResearchClient::new(
        ResearchClientConfig {
            executable,
            root,
            scope,
        },
        source,
    ))
}

mod legacy_market;

/// Exercise the former Rust child contract for historical parity.
pub(crate) async fn dispatch(
    public_ws: &okx_market_runtime::PublicWsHandle,
    market: &MarketBootstrapper,
    research: &ResearchClient,
    request: &okx_ipc_contracts::AgentRequest,
) -> okx_ipc_contracts::AgentResponse {
    use okx_ipc_contracts::AgentOperation;
    use okx_ipc_contracts::trading::DiagnosticsView;

    let result = async {
        request.validate().map_err(|_| "INVALID_COMMAND")?;
        match &request.operation {
            AgentOperation::Research { request } => research.research(request.clone()).await,
            AgentOperation::PortfolioScenario { request } => {
                research.portfolio_scenario(request.clone()).await
            }
            AgentOperation::Diagnostics {
                request: diagnostic,
            } if diagnostic.view == Some(DiagnosticsView::Research) => {
                diagnostics(research, diagnostic, request.account.as_deref()).await
            }
            operation => legacy_market::execute(public_ws, market, research, operation).await,
        }
    };
    let result = crate::research_client::with_invocation(&request.request_id, result).await;
    crate::query::response(&request.request_id, result)
}

async fn diagnostics(
    research: &ResearchClient,
    request: &okx_ipc_contracts::trading::DiagnosticsRequest,
    account: Option<&str>,
) -> Result<serde_json::Value, String> {
    use serde_json::json;
    if request.cycle_id.is_some() || request.limit.is_some() {
        return Err("INVALID_DIAGNOSTICS_VIEW".into());
    }
    // Research belongs to the authenticated installation, not an account.
    if request.account.is_some() || account.is_some() {
        return Err("RESEARCH_DIAGNOSTICS_ACCOUNT_NOT_APPLICABLE".into());
    }
    let id = request
        .reference
        .as_deref()
        .ok_or("JOB_REFERENCE_REQUIRED")?;
    if id == "deployment" {
        return Ok(json!({
            "schema": "okx.research.deployment/v1",
            "engine_pid": std::process::id(),
            "observed_at_ms": jiff::Timestamp::now().as_millisecond(),
            "research": research.deployment_status(),
            "evidence": "cached_engine_observation",
            "read_only": true
        }));
    }
    if id.len() != 64
        || !id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("INVALID_JOB_REFERENCE".into());
    }
    research.diagnostic_job(id).await
}

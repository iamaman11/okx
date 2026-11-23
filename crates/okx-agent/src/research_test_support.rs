//! Test wiring only; production resolves a verified installed component.
use crate::{
    market_bootstrap::MarketBootstrapper,
    research_client::{ResearchClient, ResearchClientConfig, SourceHandler},
};
use okx_protocol::research_ipc::ResearchScope;
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

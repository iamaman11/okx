use okx_research_protocol::{
    analytics::MarketRequest,
    research_ipc::{MAX_FRAME_BYTES, ResearchScope},
};
use std::path::PathBuf;
use tokio::io::AsyncReadExt;
#[tokio::main]
async fn main() {
    if tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .try_init()
        .is_err()
    {
        eprintln!("OKX Research Service: TRACING_SUBSCRIBER_INIT_FAILED");
        std::process::exit(1);
    }
    match run().await {
        Ok(()) => std::process::exit(0),
        Err(error) => {
            eprintln!("OKX Research Service: {error}");
            std::process::exit(exit_code(&error));
        }
    }
}

/// Startup failures are observable by CONTROL without reading stderr or
/// sharing Research credentials. A live WSS authentication failure does not
/// exit this process and must not be misreported as a local credential error.
fn exit_code(error: &str) -> i32 {
    match error {
        "RESEARCH_CATALOG_MISSING" => 70,
        "RESEARCH_SCOPE_MISMATCH" => 71,
        "RESEARCH_CATALOG_SCOPE_UNBOUND" => 72,
        "RESEARCH_ROOT_LOCKED" => 73,
        "RESEARCH_WORKER_ALREADY_ACTIVE" => 74,
        "RESEARCH_EDGE_CREDENTIAL_NOT_FOUND"
        | "RESEARCH_EDGE_CREDENTIAL_READ_FAILED"
        | "RESEARCH_EDGE_CREDENTIAL_STORE_UNAVAILABLE"
        | "RESEARCH_STARTUP_BEARER_UNAVAILABLE" => 75,
        "RESEARCH_EDGE_ORIGIN_UNTRUSTED"
        | "INVALID_RESEARCH_EDGE_WSS_URL"
        | "INVALID_RESEARCH_BEARER"
        | "RESEARCH_EDGE_TOKEN_INVALID"
        | "RESEARCH_STARTUP_ENVELOPE_INVALID"
        | "RESEARCH_STARTUP_BEARER_INVALID"
        | "RESEARCH_SANDBOX_ATTESTATION_INVALID"
        | "RESEARCH_SANDBOX_CREDMAN_EXPOSED"
        | "RESEARCH_SANDBOX_FINANCE_FILE_EXPOSED"
        | "RESEARCH_EDGE_PROVISIONER_DISABLED" => 76,
        _ => 1,
    }
}
async fn run() -> Result<(), String> {
    let all_args = std::env::args_os().skip(1).collect::<Vec<_>>();
    // Credential ownership belongs exclusively to CONTROL. Reject the old
    // privileged Worker entry point before touching stdin, a catalog, a Job,
    // or Credential Manager, including when the flag appears after arguments.
    if all_args.iter().any(|arg| arg == "--set-edge-token") {
        return Err("RESEARCH_EDGE_PROVISIONER_DISABLED".into());
    }
    if all_args
        .first()
        .is_some_and(|arg| arg == "--public-candles")
    {
        return run_public_candles(&all_args).await;
    }
    let mut args = all_args.into_iter();
    let mut root = None;
    let mut scope = None;
    let mut independent_source = false;
    let mut market_request = false;
    let mut edge_runtime = false;
    let mut existing_catalog = false;
    while let Some(arg) = args.next() {
        if arg == "--root" && root.is_none() {
            root = Some(PathBuf::from(args.next().ok_or("MISSING_ROOT")?));
        } else if arg == "--scope" && scope.is_none() {
            scope = Some(
                ResearchScope::new(
                    args.next()
                        .ok_or("MISSING_SCOPE")?
                        .into_string()
                        .map_err(|_| "INVALID_SCOPE")?,
                )
                .map_err(str::to_owned)?,
            );
        } else if arg == "--independent-source" && !independent_source {
            independent_source = true;
        } else if arg == "--market-request" && !market_request {
            market_request = true;
        } else if arg == "--existing-catalog" && !existing_catalog {
            existing_catalog = true;
        } else if arg == "--edge-runtime" && !edge_runtime {
            edge_runtime = true;
        } else {
            return Err("INVALID_ARGUMENT".into());
        }
    }
    if existing_catalog && !edge_runtime {
        return Err("INVALID_ARGUMENT".into());
    }
    if edge_runtime && (independent_source || market_request || root.is_none() || scope.is_none()) {
        return Err("INVALID_ARGUMENT".into());
    }
    // Scoped to this Research process. Do not run the Worker without its OS
    // memory and CPU fences on Windows; Finance/Engine retain their own owners.
    let _memory_guard = okx_research_service::process_memory::JobMemoryGuard::enforce()?;
    #[cfg(windows)]
    tracing::info!(
        commit_limit_bytes = okx_research_service::process_memory::limit_bytes(),
        cpu_rate_basis_points =
            okx_research_service::process_memory::RESEARCH_CPU_RATE_BASIS_POINTS,
        isolation = "nested_windows_job_object",
        "research worker OS memory and CPU fences verified"
    );
    let startup_token = if edge_runtime {
        // CONTROL resumes us only after sandbox verification and host Job
        // assignment. Before CONTROL sends any bearer, the CHILD checks that
        // its own sandbox cannot read disposable CredMan/file canaries.
        // Only a nonce and denial booleans are emitted on inherited stdout.
        #[cfg(windows)]
        okx_research_service::edge_credentials::attest_startup_sandbox(
            root.as_deref().ok_or("MISSING_ROOT")?,
        )?;
        // CONTROL may send one bounded JSON bearer only after it has verified
        // the readiness attestation. EOF is required before proceeding.
        Some(okx_research_service::edge_runtime::await_supervisor_startup().await?)
    } else {
        None
    };
    let root = root.ok_or("MISSING_ROOT")?;
    let scope = scope.ok_or("MISSING_SCOPE")?;
    if edge_runtime {
        return okx_research_service::edge_runtime::run(
            root,
            scope,
            existing_catalog,
            startup_token.ok_or("RESEARCH_STARTUP_BEARER_UNAVAILABLE")?,
        )
        .await;
    }
    if market_request {
        if independent_source {
            return Err("INVALID_ARGUMENT".into());
        }
        return run_market_request(root, scope).await;
    }
    let input = tokio::io::stdin();
    let output = tokio::io::stdout();
    if independent_source {
        okx_research_service::ipc::serve_independent(input, output, root, scope).await
    } else {
        okx_research_service::ipc::serve(input, output, root, scope).await
    }
}

/// Read one MarketRequest as JSON from stdin and write the Research result.
async fn run_market_request(root: PathBuf, scope: ResearchScope) -> Result<(), String> {
    let mut bytes = Vec::new();
    tokio::io::stdin()
        .take(MAX_FRAME_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| "MARKET_REQUEST_READ_FAILED")?;
    if bytes.len() > MAX_FRAME_BYTES {
        return Err("MARKET_REQUEST_TOO_LARGE".into());
    }
    let request: MarketRequest =
        serde_json::from_slice(&bytes).map_err(|_| "INVALID_MARKET_REQUEST")?;
    if !request.valid() {
        return Err("INVALID_MARKET_REQUEST".into());
    }
    if request.universe.is_some() {
        return Err("MARKET_REQUEST_REQUIRES_EXPLICIT_INSTRUMENTS".into());
    }
    std::fs::create_dir_all(&root).map_err(|_| "RESEARCH_ROOT_CREATE_FAILED")?;
    let source = okx_research_service::source_api::PublicSourceClient::new(std::sync::Arc::new(
        okx_research_service::independent_source::IndependentOkxPublicSource::new()?,
    ));
    let producer = okx_research_service::producer::Producer::current()?;
    let service = okx_research_service::analytics::AnalyticsService::new_scoped_with_producer(
        root, source, &scope, producer,
    )?;
    let result = service.market(request, None, None).await?;
    println!("{result}");
    Ok(())
}

/// A bounded standalone public-data proof path. The Research EXE can fetch
/// public candle rows with an explicit `confirmed` flag when Agent/Engine are
/// stopped; computations must separately reject unclosed bars. This read-only
/// path does not enable strategies or install a new production transport.
async fn run_public_candles(args: &[std::ffi::OsString]) -> Result<(), String> {
    if args.len() != 3 {
        return Err("PUBLIC_CANDLES_USAGE".into());
    }
    let instrument = args[1].to_str().ok_or("INVALID_INSTRUMENT")?;
    let bar = args[2].to_str().ok_or("INVALID_TIMEFRAME")?;
    let _memory_guard = okx_research_service::process_memory::JobMemoryGuard::enforce()?;
    let source = okx_research_service::source_api::PublicSourceClient::new(std::sync::Arc::new(
        okx_research_service::independent_source::IndependentOkxPublicSource::new()?,
    ));
    let capture = source
        .research_candles_page(instrument, bar, None, None, 100)
        .await?;
    let encoded =
        serde_json::to_string(&capture).map_err(|_| "PUBLIC_CANDLES_SERIALIZATION_FAILED")?;
    println!("{encoded}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::exit_code;

    #[test]
    fn supervisor_startup_exit_codes_are_typed_and_credential_safe() {
        for (reason, expected) in [
            ("RESEARCH_CATALOG_MISSING", 70),
            ("RESEARCH_SCOPE_MISMATCH", 71),
            ("RESEARCH_CATALOG_SCOPE_UNBOUND", 72),
            ("RESEARCH_ROOT_LOCKED", 73),
            ("RESEARCH_WORKER_ALREADY_ACTIVE", 74),
            ("RESEARCH_EDGE_CREDENTIAL_NOT_FOUND", 75),
            ("RESEARCH_EDGE_CREDENTIAL_READ_FAILED", 75),
            ("RESEARCH_EDGE_CREDENTIAL_STORE_UNAVAILABLE", 75),
            ("RESEARCH_STARTUP_BEARER_UNAVAILABLE", 75),
            ("RESEARCH_EDGE_ORIGIN_UNTRUSTED", 76),
            ("INVALID_RESEARCH_EDGE_WSS_URL", 76),
            ("INVALID_RESEARCH_BEARER", 76),
            ("RESEARCH_EDGE_TOKEN_INVALID", 76),
            ("RESEARCH_STARTUP_ENVELOPE_INVALID", 76),
            ("RESEARCH_STARTUP_BEARER_INVALID", 76),
            ("RESEARCH_SANDBOX_ATTESTATION_INVALID", 76),
            ("RESEARCH_SANDBOX_CREDMAN_EXPOSED", 76),
            ("RESEARCH_SANDBOX_FINANCE_FILE_EXPOSED", 76),
            ("RESEARCH_EDGE_PROVISIONER_DISABLED", 76),
        ] {
            assert_eq!(exit_code(reason), expected, "{reason}");
        }
        // Remote Edge errors are retried and cannot be inferred from a
        // credential-only exit code. Unknown failure remains generic.
        assert_eq!(exit_code("RESEARCH_HELLO_ACK_REQUIRED"), 1);
        assert_eq!(exit_code("RESEARCH_PUBLIC_TRANSPORT_FAILED"), 1);
    }
}

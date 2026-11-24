use okx_ipc_contracts::research_ipc::ResearchScope;
use std::path::PathBuf;
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
            std::process::exit(1);
        }
    }
}
async fn run() -> Result<(), String> {
    let all_args = std::env::args_os().skip(1).collect::<Vec<_>>();
    if all_args
        .first()
        .is_some_and(|arg| arg == "--public-candles")
    {
        return run_public_candles(&all_args).await;
    }
    let mut args = all_args.into_iter();
    let mut root = None;
    let mut scope = None;
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
        } else {
            return Err("INVALID_ARGUMENT".into());
        }
    }
    // Scoped to this Research process. Do not run the Worker without its OS
    // memory fence on Windows; native Finance/Engine retains its own owner.
    let _memory_guard = okx_research_service::process_memory::JobMemoryGuard::enforce()?;
    #[cfg(windows)]
    tracing::info!(
        commit_limit_bytes = okx_research_service::process_memory::limit_bytes(),
        isolation = "nested_windows_job_object",
        "research worker OS commit fence verified"
    );
    okx_research_service::ipc::serve(
        tokio::io::stdin(),
        tokio::io::stdout(),
        root.ok_or("MISSING_ROOT")?,
        scope.ok_or("MISSING_SCOPE")?,
    )
    .await
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

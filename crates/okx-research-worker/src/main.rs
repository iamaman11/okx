use okx_protocol::research_ipc::ResearchScope;
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
        eprintln!("research worker: TRACING_SUBSCRIBER_INIT_FAILED");
        std::process::exit(1);
    }
    match run().await {
        Ok(()) => std::process::exit(0),
        Err(error) => {
            eprintln!("research worker: {error}");
            std::process::exit(1);
        }
    }
}
async fn run() -> Result<(), String> {
    let mut args = std::env::args_os().skip(1);
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
    let _memory_guard = okx_research_worker::process_memory::JobMemoryGuard::enforce()?;
    #[cfg(windows)]
    tracing::info!(
        commit_limit_bytes = okx_research_worker::process_memory::limit_bytes(),
        isolation = "nested_windows_job_object",
        "research worker OS commit fence verified"
    );
    okx_research_worker::ipc::serve(
        tokio::io::stdin(),
        tokio::io::stdout(),
        root.ok_or("MISSING_ROOT")?,
        scope.ok_or("MISSING_SCOPE")?,
    )
    .await
}

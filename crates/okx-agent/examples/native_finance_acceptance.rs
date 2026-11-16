//! Fresh contract-witness collector. Partial coverage never becomes full acceptance.
//! Run with --known for existing witnesses, --list for the manifest, or no flags
//! for full acceptance (blocked while any mandatory contract remains missing).
#[path = "native_finance_acceptance/manifest.rs"]
mod manifest;
#[path = "native_finance_acceptance/process.rs"]
mod process;
#[path = "native_finance_acceptance/repository_guards.rs"]
mod repository_guards;
#[path = "native_finance_acceptance/verify.rs"]
mod verify;
use manifest::CONTRACTS;
use process::{Output, command};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    path::Path,
    time::{Duration, Instant},
};
use tokio::process::Command;
use verify::{
    absent_umbrellas, artifacts, collector_sources_match, incomplete, manifest_json, selected,
    verify_output,
};

const PACKAGES: [&str; 5] = [
    "okx-kernel",
    "okx-execution",
    "okx-agent",
    "okx-protocol",
    "okx-runtime",
];
type Witnesses = BTreeSet<(String, String)>;
type Result<T> = std::result::Result<T, &'static str>;
async fn git(repo: &Path, args: &[&str]) -> Result<String> {
    let mut cmd = Command::new("git");
    cmd.current_dir(repo).args(args);
    let out = command(cmd, Duration::from_secs(30)).await?;
    if !out.success {
        return Err("GIT_COMMAND_FAILED");
    }
    String::from_utf8(out.stdout)
        .map(|v| v.trim().to_owned())
        .map_err(|_| "GIT_OUTPUT_INVALID")
}
async fn snapshot(repo: &Path) -> Result<(String, String, bool)> {
    Ok((
        git(repo, &["rev-parse", "HEAD"]).await?,
        git(repo, &["rev-parse", "HEAD^{tree}"]).await?,
        git(
            repo,
            &[
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
                "--ignore-submodules=none",
            ],
        )
        .await?
        .is_empty(),
    ))
}
fn failure(
    report: &mut Value,
    stage: &str,
    package: Option<&str>,
    code: &str,
    output: Option<&Output>,
) {
    let tail = |bytes: &[u8]| {
        String::from_utf8_lossy(&bytes[bytes.len().saturating_sub(8192)..]).into_owned()
    };
    report["failure"] = json!({"stage":stage,"package":package,"code":code,
        "exit_code":output.and_then(|o| o.exit_code),"child_failure":output.and_then(|o| o.failure),
        "child_failures":output.map(|o|&o.failures),
        "stdout_tail":output.map(|o|tail(&o.stdout)),"stderr_tail":output.map(|o|tail(&o.stderr))});
}
async fn execute(
    repo: &Path,
    baseline: &(String, String, bool),
    wanted: &Witnesses,
    report: &mut Value,
) -> Result<Witnesses> {
    if !baseline.2 {
        return Err("GIT_TREE_DIRTY");
    }
    if wanted.is_empty() {
        return Err("NO_SELECTED_WITNESSES");
    }
    let build_start = Instant::now();
    let mut cmd = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
    cmd.current_dir(repo).args([
        "test",
        "--locked",
        "--no-run",
        "--message-format=json",
        "--lib",
    ]);
    for package in PACKAGES {
        cmd.args(["-p", package]);
    }
    let built = command(cmd, Duration::from_secs(1800)).await;
    report["build_ms"] = json!(build_start.elapsed().as_millis());
    match &built {
        Ok(out) if !out.success => failure(
            report,
            "build",
            None,
            out.failure.unwrap_or("BUILD_FAILED"),
            Some(out),
        ),
        Err(code) => failure(report, "build", None, code, None),
        _ => {}
    }
    if snapshot(repo).await? != *baseline {
        return Err("GIT_TREE_CHANGED");
    }
    let built = built?;
    if !built.success {
        return Err(built.failure.unwrap_or("BUILD_FAILED"));
    }
    let binaries = artifacts(repo, &built.stdout).map_err(|code| {
        failure(
            report,
            "build_artifact_verification",
            None,
            code,
            Some(&built),
        );
        code
    })?;
    let mut observed = Witnesses::new();
    let runtime_start = Instant::now();
    let execution: Result<()> = async {
        for package in PACKAGES {
            let names: BTreeSet<String> = wanted
                .iter()
                .filter(|(p, _)| p == package)
                .map(|(_, n)| n.clone())
                .collect();
            if names.is_empty() {
                continue;
            }
            if snapshot(repo).await? != *baseline {
                return Err("GIT_TREE_CHANGED");
            }
            let mut cmd = Command::new(&binaries[package]);
            cmd.current_dir(repo)
                .args([
                    "--exact",
                    "--format",
                    "pretty",
                    "--color",
                    "never",
                    "--test-threads",
                    "4",
                ])
                .args(&names);
            cmd.env(
                "OKX_NATIVE_ACCEPTANCE_RUN_ID",
                report["run_id"].as_str().unwrap(),
            );
            let out = command(cmd, Duration::from_secs(180)).await;
            match &out {
                Ok(out) if !out.success => failure(
                    report,
                    "libtest",
                    Some(package),
                    out.failure.unwrap_or("LIBTEST_PROCESS_FAILED"),
                    Some(out),
                ),
                Err(code) => failure(report, "libtest", Some(package), code, None),
                _ => {}
            }
            if snapshot(repo).await? != *baseline {
                return Err("GIT_TREE_CHANGED");
            }
            let out = out?;
            if !out.success {
                return Err(out.failure.unwrap_or("LIBTEST_PROCESS_FAILED"));
            }
            let verified = verify_output(&out.stdout, &names).map_err(|code| {
                failure(
                    report,
                    "libtest_output_verification",
                    Some(package),
                    code,
                    Some(&out),
                );
                code
            })?;
            for name in verified {
                observed.insert((package.to_owned(), name));
            }
        }
        if wanted.contains(&("repository".into(), repository_guards::WITNESS.into())) {
            if snapshot(repo).await? != *baseline {
                return Err("GIT_TREE_CHANGED");
            }
            repository_guards::run(repo, report).await?;
            if snapshot(repo).await? != *baseline {
                return Err("GIT_TREE_CHANGED");
            }
            observed.insert(("repository".into(), repository_guards::WITNESS.into()));
        }
        if wanted.contains(&(
            "repository".into(),
            repository_guards::TRANSPORT_WITNESS.into(),
        )) {
            if snapshot(repo).await? != *baseline {
                return Err("GIT_TREE_CHANGED");
            }
            repository_guards::run_transport(repo, report).await?;
            if snapshot(repo).await? != *baseline {
                return Err("GIT_TREE_CHANGED");
            }
            observed.insert((
                "repository".into(),
                repository_guards::TRANSPORT_WITNESS.into(),
            ));
        }
        if observed != *wanted {
            return Err("SELECTED_WITNESS_MISSING");
        }
        Ok(())
    }
    .await;
    report["runtime_setup_to_teardown_ms"] = json!(runtime_start.elapsed().as_millis());
    report["observed_witnesses"] = json!(
        observed
            .iter()
            .map(|(p, n)| json!({"package":p,"test":n,"status":"ok"}))
            .collect::<Vec<_>>()
    );
    execution?;
    Ok(observed)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mode = match args.as_slice() {
        [] => "full",
        [arg] if arg == "--known" => "known",
        [arg] if arg == "--list" => "list",
        _ => {
            eprintln!("ARGUMENTS_INVALID");
            std::process::exit(1);
        }
    };
    if mode == "list" {
        println!("{}", json!({"contracts":manifest_json()}));
        return;
    }
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let mut nonce = [0_u8; 16];
    if getrandom::fill(&mut nonce).is_err() {
        eprintln!("RUN_ID_UNAVAILABLE");
        std::process::exit(1);
    }
    let run_id: String = nonce.iter().map(|b| format!("{b:02x}")).collect();
    let mut report = json!({"schema":"okx.native-finance-acceptance/v1", "run_id":run_id,"mode":mode,
        "head":null,"tree":null,"git_clean":false,"build_ms":null,"runtime_setup_to_teardown_ms":null,
        "observed_witnesses":[],"repository_guards":[],"contracts":[],"overall":"FAIL","timeout_guards_seconds":{"build":1800,"per_package_libtest":180},"test_threads":4});
    let result: Result<Witnesses> = async {
        collector_sources_match(repo)?;
        report["collector_source_binding"] = json!("VERIFIED");
        let baseline = snapshot(repo).await?;
        report["head"] = json!(baseline.0);
        report["tree"] = json!(baseline.1);
        report["git_clean"] = json!(baseline.2);
        let wanted = selected()?;
        if mode == "full" && incomplete() {
            report["overall"] = json!("BLOCKED");
            return Ok(Witnesses::new());
        }
        let observed = execute(repo, &baseline, &wanted, &mut report).await?;
        report["overall"] = json!(if mode == "full" {
            "FULL_PASS"
        } else {
            "PARTIAL"
        });
        Ok(observed)
    }
    .await;
    let observed = result.as_ref().ok();
    let blocked = report["overall"] == "BLOCKED";
    let mut contracts: Vec<Value> = CONTRACTS.iter().map(|c| {
        let missing = c.missing.or(if c.witnesses.is_empty() { Some("NO_WITNESS") } else { None });
        let passed = c.witnesses.iter().all(|w| observed.is_some_and(|o| o.contains(&(w.package.to_owned(), w.test.to_owned()))));
        json!({"id":c.id,"status":if missing.is_some(){"MISSING"}else if blocked{"NOT_RUN"}else if passed{"PASS"}else{"FAIL"},"missing":missing,
            "reason":if blocked && missing.is_none(){Some("FULL_MODE_BLOCKED_BY_MISSING_CONTRACTS")}else{missing}})
    }).collect();
    contracts.extend(
        absent_umbrellas()
            .into_iter()
            .map(|id| json!({"id":id,"status":"MISSING","missing":"RB_ID_NOT_IN_MANIFEST"})),
    );
    report["contracts"] = json!(contracts);
    if let Err(code) = result {
        report["error"] = json!(code);
        eprintln!("{code}");
    }
    println!("{report}");
    if report["overall"] == "BLOCKED" {
        std::process::exit(2);
    }
    if report["overall"] == "FAIL" {
        std::process::exit(1);
    }
}

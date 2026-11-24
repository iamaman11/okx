//! Fresh contract-witness collector. Partial coverage never becomes full acceptance.
//! Run with --known for existing witnesses, --list for the manifest, or no flags
//! for full acceptance (blocked while any mandatory contract remains missing).
mod budgets;
mod manifest;
mod phase_timing;
mod process;
mod reporting;
mod repository_guards;
mod resources;
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
    "okx-domain-core",
    "okx-trade-execution",
    "okx-trading-engine",
    "okx-ipc-contracts",
    "okx-market-runtime",
];
// Keep the ordinary finance workload concurrent as in the measured baseline.
// Real-clock fixtures can expire under unrelated concurrent OS/SQL work;
// keep those witnesses in a separate child with bounded concurrency.
const FINANCIAL_WITNESS_TEST_THREADS: u64 = 4;
const CLOCK_SENSITIVE_TEST_THREADS: u64 = 2;
const CLOCK_SENSITIVE_AGENT_WITNESSES: [&str; 7] = [
    "accounts::native_trade_contracts::native_fee_correction::risk_book_contract_native_late_fee_rebate_and_bound_breach_use_immutable_revisions",
    "accounts::native_trade_contracts::native_filled_close::risk_book_contract_native_failed_stop_emergency_close_and_verified_remediation",
    "accounts::native_trade_contracts::native_filled_close::risk_book_contract_native_filled_close_settlement_release_and_cold_reopen",
    "accounts::native_trade_contracts::native_filled_close::risk_book_contract_native_stop_after_fill_blocks_open_but_closes_and_releases",
    "accounts::native_trade_contracts::native_execute_safety::risk_book_contract_native_execute_bound_rejection_preserves_code_and_reserve",
    "accounts::native_trade_contracts::native_execute_safety::risk_book_contract_native_execute_foreign_rejection_remains_unknown",
    "accounts::native_trade_contracts::native_multi_close::risk_book_contract_native_two_closes_use_remaining_lot_and_retire_partial_terminal",
];
struct LibtestGroup {
    label: &'static str,
    threads: u64,
    names: BTreeSet<String>,
}
fn libtest_groups(package: &str, names: &BTreeSet<String>) -> Result<Vec<LibtestGroup>> {
    if package != "okx-trading-engine" {
        return Ok(vec![LibtestGroup {
            label: "all",
            threads: FINANCIAL_WITNESS_TEST_THREADS,
            names: names.clone(),
        }]);
    }
    if !CLOCK_SENSITIVE_AGENT_WITNESSES
        .iter()
        .all(|name| names.contains(*name))
    {
        return Err("CLOCK_SENSITIVE_WITNESS_MISSING");
    }
    let clock_sensitive: BTreeSet<String> = CLOCK_SENSITIVE_AGENT_WITNESSES
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    let parallel = names.difference(&clock_sensitive).cloned().collect();
    Ok(vec![
        LibtestGroup {
            label: "parallel",
            threads: FINANCIAL_WITNESS_TEST_THREADS,
            names: parallel,
        },
        LibtestGroup {
            label: "clock_sensitive",
            threads: CLOCK_SENSITIVE_TEST_THREADS,
            names: clock_sensitive,
        },
    ])
}
type Witnesses = BTreeSet<(String, String)>;
type Result<T> = std::result::Result<T, &'static str>;
fn timing_distribution(mut times: Vec<u64>) -> Value {
    times.sort_unstable();
    json!({"sample_count":times.len(),
        "method":"empirical nearest rank; no population tail guarantee",
        "p50_ms":(times.len()>=3).then(||times[(times.len()*50).div_ceil(100)-1]),
        "p95_ms":(times.len()>=20).then(||times[(times.len()*95).div_ceil(100)-1]),
        "p95_minimum_samples":20,"max_ms":times.last(),"build_excluded":true})
}
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
    enforce_resources: bool,
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
    let binaries = artifacts(repo, &built.stdout).inspect_err(|code| {
        failure(
            report,
            "build_artifact_verification",
            None,
            code,
            Some(&built),
        );
    })?;
    let mut observed = Witnesses::new();
    let runtime_start = Instant::now();
    let measurements = budgets::Measurements::new(report["run_id"].as_str().unwrap())?;
    // Repository guards are read-only and already independently gated by CI.
    // Run them on the same clean source tree while financial witnesses execute,
    // retaining their individual exit status, outputs and resource receipts.
    // Never overlap the two financial libtest groups with each other: the
    // real-clock and SQLite acceptance witnesses still run in isolation.
    let mut guard_task =
        if wanted.contains(&("repository".into(), repository_guards::WITNESS.into())) {
            let repo = repo.to_owned();
            Some(tokio::spawn(async move {
                let mut receipt = json!({"stages":[], "repository_guards":[], "failure":null});
                let result = repository_guards::run(&repo, &mut receipt).await;
                (result, receipt)
            }))
        } else {
            None
        };
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
            for group in libtest_groups(package, &names)? {
                if group.names.is_empty() {
                    return Err("EMPTY_LIBTEST_GROUP");
                }
                if snapshot(repo).await? != *baseline {
                    return Err("GIT_TREE_CHANGED");
                }
                let mut cmd = Command::new(&binaries[package]);
                let thread_count = group.threads.to_string();
                cmd.current_dir(repo)
                    .args([
                        "--exact",
                        "--format",
                        "pretty",
                        "--color",
                        "never",
                        "--test-threads",
                        &thread_count,
                    ])
                    .args(&group.names);
                cmd.env(
                    "OKX_NATIVE_ACCEPTANCE_RUN_ID",
                    report["run_id"].as_str().unwrap(),
                );
                cmd.env("OKX_NATIVE_ACCEPTANCE_WORKLOAD_DIR", measurements.path());
                let stage_started = Instant::now();
                let out = command(cmd, Duration::from_secs(180)).await;
                report["stages"].as_array_mut().unwrap().push(json!({
                    "stage":"libtest", "package":package, "group":group.label,
                    "test_threads":group.threads,"witness_count":group.names.len(),
                    "wall_ms":stage_started.elapsed().as_millis(),
                    "success":out.as_ref().is_ok_and(|o|o.success),
                    "direct_process_resources":out.as_ref().ok().map(|o|&o.resources)
                }));
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
                if let Ok(output) = &out
                    && !output.success
                {
                    reporting::record_failures(report, package, &output.stdout, &group.names);
                }
                if snapshot(repo).await? != *baseline {
                    return Err("GIT_TREE_CHANGED");
                }
                let out = out?;
                if !out.success {
                    return Err(out.failure.unwrap_or("LIBTEST_PROCESS_FAILED"));
                }
                let verified = verify_output(&out.stdout, &group.names).inspect_err(|code| {
                    failure(
                        report,
                        "libtest_output_verification",
                        Some(package),
                        code,
                        Some(&out),
                    );
                })?;
                if group.label == "clock_sensitive" {
                    // Collect fixed-name diagnostic receipts from the isolated
                    // clock-sensitive group only after the complete libtest proof;
                    // they never replace functional or resource witnesses.
                    let phases = phase_timing::collect(
                        measurements.path(),
                        report["run_id"].as_str().unwrap(),
                    )
                    .inspect_err(|code| {
                        failure(
                            report,
                            "clock_sensitive_phase_timing",
                            Some(package),
                            code,
                            Some(&out),
                        );
                    })?;
                    report["clock_sensitive_phase_timings"] = json!(phases);
                }
                for name in verified {
                    observed.insert((package.to_owned(), name));
                }
                reporting::record_verified(report, &observed);
            }
        }
        if let Some(task) = guard_task.take() {
            let (result, receipt) = task.await.map_err(|_| "REPOSITORY_GUARD_TASK_FAILED")?;
            if snapshot(repo).await? != *baseline {
                return Err("GIT_TREE_CHANGED");
            }
            let guard_stages = receipt["stages"]
                .as_array()
                .ok_or("RESOURCE_STAGES_MISSING")?;
            report["stages"]
                .as_array_mut()
                .unwrap()
                .extend(guard_stages.iter().cloned());
            report["repository_guards"] = receipt["repository_guards"].clone();
            if !receipt["failure"].is_null() {
                report["failure"] = receipt["failure"].clone();
            }
            result?;
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
        let core: Witnesses = wanted
            .iter()
            .filter(|(_, n)| n != budgets::WITNESS)
            .cloned()
            .collect();
        if observed != core {
            return Err("SELECTED_WITNESS_MISSING");
        }
        Ok(())
    }
    .await;
    if execution.is_err()
        && let Some(task) = guard_task.take()
    {
        task.abort();
        let _ = task.await;
    }
    let workload = if execution.is_ok() {
        measurements.finish(report["run_id"].as_str().unwrap())
    } else {
        Err("FUNCTIONAL_WITNESS_FAILED")
    };
    report["runtime_setup_to_teardown_ms"] = json!(runtime_start.elapsed().as_millis());
    // Preserve successful repository guards too when a later external stage fails.
    reporting::record_verified(report, &observed);
    execution?;
    report["owner_workload"] = workload?;
    if budgets::verify(report, &observed)? {
        observed.insert(("repository".into(), budgets::WITNESS.into()));
    } else if enforce_resources {
        return Err("FULL_RESOURCE_COUNTERS_UNAVAILABLE");
    }
    report["observed_witnesses"] = json!(
        observed
            .iter()
            .map(|(p, n)| json!({"package":p,"test":n,"status":"ok"}))
            .collect::<Vec<_>>()
    );
    Ok(observed)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (mode, samples) = match args.as_slice() {
        [] => ("full", 1),
        [arg] if arg == "--known" => ("known", 1),
        [arg] if arg == "--list" => ("list", 1),
        [arg, flag, count]
            if arg == "--known"
                && flag == "--samples"
                && count.parse::<usize>().is_ok_and(|n| (2..=100).contains(&n)) =>
        {
            ("known", count.parse::<usize>().unwrap())
        }
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
        "observed_witnesses":[],"repository_guards":[],"stages":[],"clock_sensitive_phase_timings":[],"contracts":[],"overall":"FAIL","timeout_guards_seconds":{"build":1800,"per_libtest_group":180},"test_threads":FINANCIAL_WITNESS_TEST_THREADS,"clock_sensitive_test_threads":CLOCK_SENSITIVE_TEST_THREADS,
        "test_schedule":format!("parallel_{FINANCIAL_WITNESS_TEST_THREADS}_then_clock_sensitive_{CLOCK_SENSITIVE_TEST_THREADS}")});
    let result: Result<Witnesses> = async {
        collector_sources_match(repo)?;
        report["collector_source_binding"] = json!("VERIFIED");
        let baseline = snapshot(repo).await?;
        report["head"] = json!(baseline.0);
        report["tree"] = json!(baseline.1);
        report["git_clean"] = json!(baseline.2);
        let mut compiler=Command::new("rustc");
        compiler.arg("--version");
        let compiler=command(compiler,Duration::from_secs(30)).await?;
        if !compiler.success { return Err("TOOLCHAIN_IDENTITY_UNAVAILABLE"); }
        report["environment"]=json!({"os":std::env::consts::OS,"arch":std::env::consts::ARCH,
            "logical_processors":std::thread::available_parallelism().ok().map(|n|n.get()),
            "processor_identifier":std::env::var("PROCESSOR_IDENTIFIER").ok(),
            "toolchain":String::from_utf8_lossy(&compiler.stdout).trim(),
            "profile":"Cargo test debug","test_threads":FINANCIAL_WITNESS_TEST_THREADS,
            "clock_sensitive_test_threads":CLOCK_SENSITIVE_TEST_THREADS});
        let wanted = selected()?;
        if mode == "full" && incomplete() {
            report["overall"] = json!("BLOCKED");
            return Ok(Witnesses::new());
        }
        let mut observed = Witnesses::new();
        let mut measurements = Vec::new();
        let root_run_id = report["run_id"].as_str().unwrap().to_owned();
        for sample in 0..samples {
            report["run_id"] = json!(format!("{root_run_id}_{sample}"));
            report["stages"] = json!([]);
            report["clock_sensitive_phase_timings"] = json!([]);
            report["observed_witnesses"] = json!([]);
            report["failed_witnesses"] = json!([]);
            report["failure"] = Value::Null;
            report["repository_guards"] = json!([]);
            report["resource_budgets"] = Value::Null;
            report["owner_workload"] = Value::Null;
            observed = execute(repo, &baseline, &wanted, &mut report, mode=="full").await?;
            measurements.push(json!({"sample":sample,"run_id":report["run_id"],
                "head":baseline.0,"tree":baseline.1,"build_ms":report["build_ms"],
                "runtime_setup_to_teardown_ms":report["runtime_setup_to_teardown_ms"],
                "stages":report["stages"],"clock_sensitive_phase_timings":report["clock_sensitive_phase_timings"],
                "owner_workload":report["owner_workload"],
                "resource_budgets":report["resource_budgets"],"executed_witnesses":observed.len()}));
            report["samples"] = json!(measurements);
        }
        let times: Vec<u64> = measurements
            .iter()
            .map(|m| m["runtime_setup_to_teardown_ms"].as_u64().unwrap())
            .collect();
        report["timing_distribution"] = timing_distribution(times);
        report["overall"] = json!(if mode == "full" {
            "FULL_PASS"
        } else {
            "PARTIAL"
        });
        Ok(observed)
    }
    .await;
    let mut contracts = reporting::contracts(&report);
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

#[cfg(test)]
mod timing_tests {
    use super::*;
    #[test]
    fn timing_requires_samples_and_uses_nearest_rank() {
        assert!(timing_distribution(vec![])["max_ms"].is_null());
        assert!(timing_distribution(vec![42])["p95_ms"].is_null());
        assert!(timing_distribution(vec![42])["p50_ms"].is_null());
        assert_eq!(timing_distribution(vec![3, 1, 2])["p50_ms"], 2);
        let r = timing_distribution((1..=20).rev().collect());
        assert_eq!(r["p50_ms"], 10);
        assert_eq!(r["p95_ms"], 19);
        assert_eq!(r["max_ms"], 20);
    }
    #[test]
    fn clock_sensitive_partition_is_exact_and_never_drops_a_witness() {
        let mut names: BTreeSet<String> = CLOCK_SENSITIVE_AGENT_WITNESSES
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        names.insert("another-financial-witness".into());
        let groups = libtest_groups("okx-trading-engine", &names).unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!((groups[0].label, groups[0].threads), ("parallel", 4));
        assert_eq!(
            (groups[1].label, groups[1].threads),
            ("clock_sensitive", CLOCK_SENSITIVE_TEST_THREADS)
        );
        assert!(groups[0].names.is_disjoint(&groups[1].names));
        assert_eq!(
            groups[0]
                .names
                .union(&groups[1].names)
                .cloned()
                .collect::<BTreeSet<_>>(),
            names
        );
        names.remove(CLOCK_SENSITIVE_AGENT_WITNESSES[0]);
        assert!(matches!(
            libtest_groups("okx-trading-engine", &names),
            Err("CLOCK_SENSITIVE_WITNESS_MISSING")
        ));
        let agent_names: BTreeSet<String> = selected()
            .unwrap()
            .into_iter()
            .filter(|(package, _)| package == "okx-trading-engine")
            .map(|(_, name)| name)
            .collect();
        let actual = libtest_groups("okx-trading-engine", &agent_names).unwrap();
        assert_eq!(
            actual[0].names.len() + actual[1].names.len(),
            agent_names.len()
        );
        assert_eq!(actual[1].names.len(), CLOCK_SENSITIVE_AGENT_WITNESSES.len());
    }
}

//! Existing static repository checks, bound to the same clean source snapshot.
//! This adds no trading implementation and no replacement architecture checker.
use super::{Result, command, failure};
use serde_json::{Value, json};
use std::{
    path::Path,
    time::{Duration, Instant},
};
use tokio::process::Command;

pub(super) const WITNESS: &str = "architecture_and_research_negative_guards";
pub(super) const TRANSPORT_WITNESS: &str =
    "retained_workerd_auth_session_transport_no_financial_owner";

pub(super) async fn run_transport(repo: &Path, report: &mut Value) -> Result<()> {
    let mut cmd = Command::new("node");
    cmd.current_dir(repo.join("cloudflare/okx-mcp"))
        .arg("scripts/test-native-finance-retirement-do.mjs");
    let started = Instant::now();
    let result = command(cmd, Duration::from_secs(120)).await;
    report["stages"].as_array_mut().unwrap().push(json!({
        "stage":"retained_transport", "wall_ms":started.elapsed().as_millis(),
        "success":result.as_ref().is_ok_and(|o|o.success),
        "direct_process_resources":result.as_ref().ok().map(|o|&o.resources)
    }));
    let output = result?;
    let marker = "actual_workerd_native_finance_retirement_transport PASS";
    if !output.success
        || !String::from_utf8_lossy(&output.stdout)
            .lines()
            .any(|line| line.starts_with(marker))
    {
        failure(
            report,
            "retained_transport",
            Some(TRANSPORT_WITNESS),
            "RETAINED_TRANSPORT_FAILED",
            Some(&output),
        );
        return Err("RETAINED_TRANSPORT_FAILED");
    }
    report["repository_guards"].as_array_mut().unwrap().push(json!({"command":["node","scripts/test-native-finance-retirement-do.mjs"],"marker":marker,"status":"PASS"}));
    Ok(())
}

pub(super) async fn run(repo: &Path, report: &mut Value) -> Result<()> {
    let python = if cfg!(windows) { "python" } else { "python3" };
    for (arguments, marker) in [
        (
            vec!["scripts/check_architecture.py"],
            "ARCHITECTURE_DEPENDENCY_GUARD=PASS",
        ),
        (
            vec!["scripts/check_architecture.py", "--self-test"],
            "ARCHITECTURE_SELF_TEST=PASS",
        ),
        (
            vec!["scripts/check_ledger_sql.py"],
            "ACCOUNT_SQLITE_SCHEMA_V2=PASS",
        ),
    ] {
        let mut cmd = Command::new(python);
        cmd.current_dir(repo).args(&arguments);
        let started = Instant::now();
        let result = command(cmd, Duration::from_secs(30)).await;
        report["stages"].as_array_mut().unwrap().push(json!({
            "stage":"repository_guard", "command":arguments,
            "wall_ms":started.elapsed().as_millis(),
            "success":result.as_ref().is_ok_and(|o|o.success),
            "direct_process_resources":result.as_ref().ok().map(|o|&o.resources)
        }));
        let output = result?;
        if !output.success
            || !String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line == marker)
        {
            failure(
                report,
                "repository_guard",
                Some(marker),
                "REPOSITORY_GUARD_FAILED",
                Some(&output),
            );
            return Err("REPOSITORY_GUARD_FAILED");
        }
        report["repository_guards"]
            .as_array_mut()
            .unwrap()
            .push(json!({"command":arguments,"marker":marker,"status":"PASS"}));
    }
    Ok(())
}

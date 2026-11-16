//! Existing static repository checks, bound to the same clean source snapshot.
//! This adds no trading implementation and no replacement architecture checker.
use super::{Result, command, failure};
use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use tokio::process::Command;

pub(super) const WITNESS: &str = "architecture_and_research_negative_guards";

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
        let output = command(cmd, Duration::from_secs(30)).await?;
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

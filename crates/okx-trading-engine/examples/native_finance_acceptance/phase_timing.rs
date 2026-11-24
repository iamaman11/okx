//! Clock-sensitive fixture phase observations. These are diagnostics, never
//! substitutes for successful financial witnesses or measured resource stages.
//! Each fixture writes one fixed-name sidecar, avoiding changes to libtest's
//! strictly verified stdout protocol or to its single-threaded execution.
use super::Result;
use serde_json::Value;
use std::{fs, path::Path};

const MAX_RECEIPT_BYTES: u64 = 16 * 1024;
const CLOSE_PHASES: [&str; 9] = [
    "fixture_setup",
    "market_open",
    "entry_execute",
    "entry_settled",
    "policy_gate",
    "close_execute",
    "close_settled",
    "terminal_release",
    "cold_reopen",
];
const FEE_PHASES: [&str; 9] = [
    "fixture_setup",
    "market_open",
    "entry_execute",
    "initial_settlement",
    "late_debit",
    "replay",
    "rebate",
    "risk_gate",
    "shutdown",
];

struct Fixture {
    slug: &'static str,
    witness: &'static str,
    phases: &'static [&'static str],
}

const FIXTURES: [Fixture; 4] = [
    Fixture {
        slug: "filled_close_reopen",
        witness: "accounts::native_trade_contracts::native_filled_close::risk_book_contract_native_filled_close_settlement_release_and_cold_reopen",
        phases: &CLOSE_PHASES,
    },
    Fixture {
        slug: "stopped_close",
        witness: "accounts::native_trade_contracts::native_filled_close::risk_book_contract_native_stop_after_fill_blocks_open_but_closes_and_releases",
        phases: &CLOSE_PHASES,
    },
    Fixture {
        slug: "emergency_close",
        witness: "accounts::native_trade_contracts::native_filled_close::risk_book_contract_native_failed_stop_emergency_close_and_verified_remediation",
        phases: &CLOSE_PHASES,
    },
    Fixture {
        slug: "late_fee_rebate",
        witness: "accounts::native_trade_contracts::native_fee_correction::risk_book_contract_native_late_fee_rebate_and_bound_breach_use_immutable_revisions",
        phases: &FEE_PHASES,
    },
];

fn name(slug: &str) -> String {
    format!("finance-phase-{slug}.json")
}

fn validate(receipt: &Value, run_id: &str, fixture: &Fixture) -> Result<()> {
    if receipt.as_object().is_none_or(|object| object.len() != 5) {
        return Err("CLOCK_PHASE_RECEIPT_FIELDS_INVALID");
    }
    if receipt["schema"] != "okx.finance.clock-phase/v1"
        || receipt["run_id"] != run_id
        || receipt["test"] != fixture.slug
        || receipt["witness"] != fixture.witness
    {
        return Err("CLOCK_PHASE_IDENTITY_MISMATCH");
    }
    let phases = receipt["phases"]
        .as_array()
        .ok_or("CLOCK_PHASE_OBSERVATIONS_MISSING")?;
    if phases.len() != fixture.phases.len() {
        return Err("CLOCK_PHASE_OBSERVATIONS_INCOMPLETE");
    }
    let mut elapsed = 0_u64;
    for (phase, expected) in phases.iter().zip(fixture.phases) {
        if phase.as_object().is_none_or(|object| object.len() != 3) {
            return Err("CLOCK_PHASE_FIELDS_INVALID");
        }
        if phase["phase"] != *expected {
            return Err("CLOCK_PHASE_ORDER_MISMATCH");
        }
        elapsed = elapsed
            .checked_add(
                phase["wall_ms"]
                    .as_u64()
                    .ok_or("CLOCK_PHASE_DURATION_INVALID")?,
            )
            .ok_or("CLOCK_PHASE_DURATION_OVERFLOW")?;
        if phase["cumulative_ms"].as_u64() != Some(elapsed) {
            return Err("CLOCK_PHASE_DURATION_INCONSISTENT");
        }
    }
    Ok(())
}

/// Read and remove only the four expected same-run files. The owning workload
/// directory must be empty after its separate owner-workload receipt is read.
pub(super) fn collect(dir: &Path, run_id: &str) -> Result<Vec<Value>> {
    let mut receipts = Vec::with_capacity(FIXTURES.len());
    for fixture in &FIXTURES {
        let path = dir.join(name(fixture.slug));
        let metadata = fs::symlink_metadata(&path).map_err(|_| "CLOCK_PHASE_RECEIPT_MISSING")?;
        if !metadata.file_type().is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() == 0
            || metadata.len() > MAX_RECEIPT_BYTES
        {
            return Err("CLOCK_PHASE_RECEIPT_FILE_INVALID");
        }
        let bytes = fs::read(&path).map_err(|_| "CLOCK_PHASE_RECEIPT_READ_FAILED")?;
        if bytes.len() as u64 != metadata.len() {
            return Err("CLOCK_PHASE_RECEIPT_CHANGED");
        }
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|_| "CLOCK_PHASE_RECEIPT_JSON_INVALID")?;
        validate(&value, run_id, fixture)?;
        fs::remove_file(&path).map_err(|_| "CLOCK_PHASE_RECEIPT_CLEANUP_FAILED")?;
        receipts.push(value);
    }
    Ok(receipts)
}

/// Bounded recovery cleanup, also on failed or timed-out child processes.
/// Never recurse, traverse arbitrary names, or remove the shared temp parent.
pub(super) fn cleanup(dir: &Path) {
    for fixture in &FIXTURES {
        let _ = fs::remove_file(dir.join(name(fixture.slug)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CLOCK_SENSITIVE_AGENT_WITNESSES;
    use serde_json::json;
    use std::collections::BTreeSet;

    fn sample(fixture: &Fixture) -> Value {
        json!({"schema":"okx.finance.clock-phase/v1", "run_id":"abc_0",
            "test":fixture.slug,"witness":fixture.witness,
            "phases":fixture.phases.iter().enumerate().map(|(index, phase)|
                json!({"phase":phase,"wall_ms":index as u64,
                    "cumulative_ms":(index*(index+1)/2) as u64})
            ).collect::<Vec<_>>()})
    }

    #[test]
    fn all_four_receipts_bind_exact_clock_witnesses_and_ordered_phases() {
        let fixture_witnesses = FIXTURES.iter().map(|f| f.witness).collect::<BTreeSet<_>>();
        assert_eq!(fixture_witnesses.len(), 4);
        assert_eq!(
            fixture_witnesses,
            CLOCK_SENSITIVE_AGENT_WITNESSES.into_iter().collect::<BTreeSet<_>>()
        );
        for fixture in &FIXTURES {
            validate(&sample(fixture), "abc_0", fixture).unwrap();
        }
    }

    #[test]
    fn missing_foreign_duplicate_out_of_order_and_bad_durations_are_not_evidence() {
        let fixture = &FIXTURES[0];
        for (path, bad) in [
            ("run_id", json!("previous_run")),
            ("test", json!("other_fixture")),
            ("witness", json!("unrelated::witness")),
            ("schema", json!("old")),
            ("phases", json!([])),
        ] {
            let mut receipt = sample(fixture);
            receipt[path] = bad;
            assert!(validate(&receipt, "abc_0", fixture).is_err(), "{path}");
        }
        let mut wrong_phase = sample(fixture);
        wrong_phase["phases"][3]["phase"] = json!("fixture_setup");
        assert!(validate(&wrong_phase, "abc_0", fixture).is_err());
        let mut extra = sample(fixture);
        extra["secret"] = json!("never echo untrusted fixture data");
        assert!(validate(&extra, "abc_0", fixture).is_err());
        let mut extra_phase = sample(fixture);
        extra_phase["phases"][0]["secret"] = json!("private");
        assert!(validate(&extra_phase, "abc_0", fixture).is_err());
        let mut negative = sample(fixture);
        negative["phases"][0]["wall_ms"] = json!(-1);
        assert!(validate(&negative, "abc_0", fixture).is_err());
        let mut inconsistent = sample(fixture);
        inconsistent["phases"][3]["cumulative_ms"] = json!(999);
        assert!(validate(&inconsistent, "abc_0", fixture).is_err());
        let mut overflow = sample(fixture);
        overflow["phases"][0]["wall_ms"] = json!(u64::MAX);
        overflow["phases"][0]["cumulative_ms"] = json!(u64::MAX);
        assert!(validate(&overflow, "abc_0", fixture).is_err());
        assert!(validate(&sample(fixture), "other_run", fixture).is_err());
    }

    #[test]
    fn complete_receipts_are_consumed_exactly_once_and_missing_files_fail() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "okx-clock-phase-negative-control-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&dir).unwrap();
        assert_eq!(collect(&dir, "abc_0"), Err("CLOCK_PHASE_RECEIPT_MISSING"));
        let first = dir.join(name(FIXTURES[0].slug));
        fs::write(&first, vec![b'x'; MAX_RECEIPT_BYTES as usize + 1]).unwrap();
        assert_eq!(
            collect(&dir, "abc_0"),
            Err("CLOCK_PHASE_RECEIPT_FILE_INVALID")
        );
        fs::write(&first, b"not-json").unwrap();
        assert_eq!(
            collect(&dir, "abc_0"),
            Err("CLOCK_PHASE_RECEIPT_JSON_INVALID")
        );
        let mut foreign = sample(&FIXTURES[0]);
        foreign["run_id"] = json!("foreign_run");
        fs::write(&first, foreign.to_string()).unwrap();
        assert_eq!(
            collect(&dir, "abc_0"),
            Err("CLOCK_PHASE_IDENTITY_MISMATCH")
        );
        for fixture in &FIXTURES {
            fs::write(dir.join(name(fixture.slug)), sample(fixture).to_string()).unwrap();
        }
        assert_eq!(collect(&dir, "abc_0").unwrap().len(), 4);
        assert!(fs::read_dir(&dir).unwrap().next().is_none());
        assert_eq!(collect(&dir, "abc_0"), Err("CLOCK_PHASE_RECEIPT_MISSING"));
        fs::remove_dir(&dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn symlink_sidecar_is_rejected_without_following_target() {
        use std::os::unix::fs::symlink;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "okx-clock-phase-symlink-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&dir).unwrap();
        let other = dir.join("unrelated.json");
        fs::write(&other, sample(&FIXTURES[0]).to_string()).unwrap();
        let link = dir.join(name(FIXTURES[0].slug));
        symlink(&other, &link).unwrap();
        assert_eq!(
            collect(&dir, "abc_0"),
            Err("CLOCK_PHASE_RECEIPT_FILE_INVALID")
        );
        fs::remove_file(&link).unwrap();
        assert!(other.is_file());
        fs::remove_file(&other).unwrap();
        fs::remove_dir(&dir).unwrap();
    }
}

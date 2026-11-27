//! #510 debug CODE acceptance envelope. No production limit or money rule.
//! All functional and physical witnesses run fresh; only timing is compared to
//! a historical baseline. A platform without counters cannot claim this witness.
use super::{Result, Witnesses};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::PathBuf;

pub(super) const WITNESS: &str = "measured_native_workload_and_resource_budgets";
const MIB: u64 = 1024 * 1024;
pub(super) struct Measurements(PathBuf);
impl Measurements {
    pub fn new(run_id: &str) -> Result<Self> {
        if !run_id.bytes().all(|b| b.is_ascii_hexdigit() || b == b'_') {
            return Err("MEASUREMENT_RUN_ID_INVALID");
        }
        let directory = std::env::temp_dir().join(format!("okx-acceptance-measurements-{run_id}"));
        std::fs::create_dir(&directory).map_err(|_| "MEASUREMENT_DIRECTORY_CREATE_FAILED")?;
        Ok(Self(directory))
    }
    pub fn path(&self) -> &std::path::Path {
        &self.0
    }
    pub fn finish(&self, run_id: &str) -> Result<Value> {
        let path = self.0.join("owner-workload.json");
        let meta = std::fs::symlink_metadata(&path).map_err(|_| "WORKLOAD_MEASUREMENT_MISSING")?;
        if !meta.is_file() || meta.file_type().is_symlink() || meta.len() > 64 * 1024 {
            return Err("WORKLOAD_MEASUREMENT_INVALID_FILE");
        }
        let value: Value = serde_json::from_slice(
            &std::fs::read(&path).map_err(|_| "WORKLOAD_MEASUREMENT_READ_FAILED")?,
        )
        .map_err(|_| "WORKLOAD_MEASUREMENT_INVALID_JSON")?;
        validate_workload(&value, run_id)?;
        std::fs::remove_file(path).map_err(|_| "MEASUREMENT_TEARDOWN_FAILED")?;
        std::fs::remove_dir(&self.0).map_err(|_| "MEASUREMENT_TEARDOWN_FAILED")?;
        Ok(value)
    }
}
impl Drop for Measurements {
    fn drop(&mut self) {
        // Only fixed files in a directory we created; no recursive traversal.
        let _ = std::fs::remove_file(self.0.join("owner-workload.json"));
        super::phase_timing::cleanup(&self.0);
        let _ = std::fs::remove_dir(&self.0);
    }
}
fn number(value: &Value, key: &str) -> Result<u64> {
    value[key]
        .as_u64()
        .ok_or("RESOURCE_MEASUREMENT_MISSING_OR_INVALID")
}
pub(super) fn validate_workload(v: &Value, run_id: &str) -> Result<()> {
    if v["schema"] != "okx.native-owner-workload/v1"
        || v["run_id"] != run_id
        || number(v, "operations")? != 64
        || number(v, "concurrency")? != 16
        || number(v, "replays")? != 64
        || number(v, "ledger_legs")? != 128
        || number(v, "sql_jobs")? != 65
    {
        return Err("WORKLOAD_IDENTITY_OR_COUNT_MISMATCH");
    }
    let latency = v["latency_us"]
        .as_array()
        .ok_or("WORKLOAD_LATENCY_MISSING")?;
    let values: Vec<u64> = latency
        .iter()
        .map(|v| v.as_u64().ok_or("WORKLOAD_LATENCY_INVALID"))
        .collect::<Result<_>>()?;
    if values.len() != 64
        || !values.windows(2).all(|w| w[0] <= w[1])
        || number(v, "p50_us")? != values[31]
        || number(v, "p95_us")? != values[60]
        || number(v, "max_us")? != values[63]
    {
        return Err("WORKLOAD_PERCENTILE_OR_COUNT_INVALID");
    }
    if values[63] >= 5_000_000
        || number(v, "max_queue_wait_ms")? >= 5_000
        || number(v, "max_sql_owner_ms")? >= 5_000
        || number(v, "setup_through_teardown_us")? >= 30_000_000
        || number(v, "storage_bytes")? > 32 * MIB
    {
        return Err("WORKLOAD_BUDGET_EXCEEDED");
    }
    Ok(())
}
pub(super) fn verify(report: &mut Value, observed: &Witnesses) -> Result<bool> {
    // One group per package, an extra isolated Engine group, three guards and transport.
    let required_stages = super::PACKAGES.len() + 1 + 3 + 1;
    let stages = report["stages"]
        .as_array()
        .ok_or("RESOURCE_STAGES_MISSING")?;
    if stages.len() != required_stages || stages.iter().any(|s| s["success"] != true) {
        return Err("RESOURCE_STAGES_INCOMPLETE");
    }
    let mut identities = BTreeSet::new();
    for stage in stages {
        let identity = match stage["stage"].as_str() {
            Some("libtest") => match (
                stage["package"].as_str(),
                stage["group"].as_str(),
                stage["test_threads"].as_u64(),
            ) {
                (Some("okx-trading-engine"), Some("parallel"), Some(4)) => {
                    "lib:okx-trading-engine:parallel".into()
                }
                (
                    Some("okx-trading-engine"),
                    Some("clock_sensitive"),
                    Some(super::CLOCK_SENSITIVE_TEST_THREADS),
                ) => "lib:okx-trading-engine:clock_sensitive".into(),
                (Some(p), Some("all"), Some(4))
                    if super::PACKAGES.contains(&p) && p != "okx-trading-engine" =>
                {
                    format!("lib:{p}:all")
                }
                _ => return Err("RESOURCE_STAGE_IDENTITY_INVALID"),
            },
            Some("retained_transport") => "transport".into(),
            Some("repository_guard")
                if [
                    json!(["scripts/check_architecture.py"]),
                    json!(["scripts/check_architecture.py", "--self-test"]),
                    json!(["scripts/check_ledger_sql.py"]),
                ]
                .contains(&stage["command"]) =>
            {
                format!("guard:{}", stage["command"])
            }
            _ => return Err("RESOURCE_STAGE_IDENTITY_INVALID"),
        };
        if !identities.insert(identity) {
            return Err("RESOURCE_STAGE_DUPLICATE");
        }
    }
    let mut cpu = 0_u64;
    let mut peak = 0_u64;
    let mut reads = 0_u64;
    let mut writes = 0_u64;
    let mut available = true;
    for stage in stages {
        let r = &stage["direct_process_resources"];
        if r["status"] != "MEASURED" {
            available = false;
            continue;
        }
        if r["scope"] != "direct_process_only" || r["descendants_included"] != false {
            return Err("RESOURCE_SCOPE_INVALID");
        }
        cpu = cpu
            .checked_add(number(r, "cpu_100ns")?)
            .ok_or("RESOURCE_OVERFLOW")?;
        peak = peak.max(number(r, "peak_working_set_bytes_observed")?);
        reads = reads
            .checked_add(number(r, "read_bytes")?)
            .ok_or("RESOURCE_OVERFLOW")?;
        writes = writes
            .checked_add(number(r, "write_bytes")?)
            .ok_or("RESOURCE_OVERFLOW")?;
    }
    // The two non-clock libtest children now overlap with one (sequential)
    // repository guard. A maximum of individual process peaks is no longer
    // an upper bound on simultaneous direct working-set consumption. The sum
    // of both child peaks plus the LARGEST guard peak is conservative: the
    // peaks may occur at different times, but a real simultaneous total can
    // never exceed this direct-process bound. Descendants remain excluded.
    let concurrent_peak_upper_bound = if available {
        let group_peak = |package: &str, group: &str| -> Result<u64> {
            let stage = stages
                .iter()
                .find(|s| s["stage"] == "libtest" && s["package"] == package && s["group"] == group)
                .ok_or("RESOURCE_OVERLAP_GROUP_MISSING")?;
            number(
                &stage["direct_process_resources"],
                "peak_working_set_bytes_observed",
            )
        };
        let guard_peak = stages
            .iter()
            .filter(|s| s["stage"] == "repository_guard")
            .map(|stage| {
                number(
                    &stage["direct_process_resources"],
                    "peak_working_set_bytes_observed",
                )
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .max()
            .ok_or("RESOURCE_OVERLAP_GUARD_MISSING")?;
        Some(
            group_peak("okx-trade-execution", "all")?
                .checked_add(group_peak("okx-trading-engine", "parallel")?)
                .and_then(|sum| sum.checked_add(guard_peak))
                .ok_or("RESOURCE_OVERFLOW")?,
        )
    } else {
        None
    };
    let wall = number(report, "runtime_setup_to_teardown_ms")?;
    // The prior same-workload local baseline is 81.798/82.765/89.091s on
    // Windows x86_64, Intel family6 model60,8 logical CPUs, debug threads4.
    //180s is a portable CI watchdog (~2x observed max), not a speedup claim.
    let workload_identity = observed
        .iter()
        .map(|(p, n)| format!("{p}\t{n}"))
        .collect::<Vec<_>>()
        .join("\n");
    let workload_hash = format!("{:x}", Sha256::digest(workload_identity.as_bytes()));
    // Historical 4-thread baseline ran Agent in one process. Splitting that
    // process changes timing methodology; do not claim same-workload speedup.
    let comparable = report["test_schedule"] == "uniform_4"
        && std::env::consts::OS == "windows"
        && std::env::var("PROCESSOR_IDENTIFIER").unwrap_or_default()
            == "Intel64 Family 6 Model 60 Stepping 3, GenuineIntel"
        && std::thread::available_parallelism().is_ok_and(|n| n.get() == 8)
        && workload_hash == "076f617c6aaf220a3b4a2506a28a05ce1d17d020363b68f46541d09610af6467";
    report["resource_budgets"] = json!({"status":if available{"VERIFIED"}else{"UNAVAILABLE"},
        "workload":"current exact production witnesses plus 64 transfers,16 concurrent,64 replays",
        "executed_core_witnesses":observed.len(),"workload_sha256":workload_hash,
        "observed":{"wall_ms":wall,"direct_cpu_100ns":available.then_some(cpu),
            "max_direct_peak_bytes":available.then_some(peak),
            "overlap_direct_peak_upper_bound_bytes":concurrent_peak_upper_bound,
            "direct_read_bytes":available.then_some(reads),"direct_write_bytes":available.then_some(writes)},
        "limits":{"wall_ms":180000,"direct_cpu_100ns":900000000,
            "max_direct_peak_bytes":512*MIB,"overlap_direct_peak_upper_bound_bytes":512*MIB,
            "direct_read_bytes":512*MIB,"direct_write_bytes":512*MIB},
        "scope":"direct processes only; excludes descendants; IO includes pipes, not disk-only",
        "baseline":{"head":"d35565e06f478bb83ef2699bee90cabeafd4c1d1",
            "wall_samples_ms":[89091,81798,82765],"same_hardware_class":comparable,
            "regression_ceiling_ms":if comparable{Some(111364)}else{None},
            "basis":"25% above observed max for identical test schedule and hardware; no cross-schedule speedup claim"},
        "percentiles":"64 operation latencies; whole-run p95 requires a separate sufficient sample"});
    let mut violations = Vec::new();
    for (field, actual, limit) in [
        ("wall_ms", wall, 180_000),
        ("direct_cpu_100ns", cpu, 900_000_000),
        ("max_direct_peak_bytes", peak, 512 * MIB),
        ("direct_read_bytes", reads, 512 * MIB),
        ("direct_write_bytes", writes, 512 * MIB),
    ] {
        if actual > limit {
            violations.push(json!({"field":field,"actual":actual,"limit":limit}));
        }
    }
    if let Some(actual) = concurrent_peak_upper_bound
        && actual > 512 * MIB
    {
        violations.push(json!({"field":"overlap_direct_peak_upper_bound_bytes",
            "actual":actual,"limit":512*MIB}));
    }
    if comparable && wall > 111_364 {
        violations.push(json!({"field":"comparable_wall_ms","actual":wall,"limit":111364}));
    }
    if !violations.is_empty() {
        report["resource_budgets"]["status"] = json!("FAILED");
        report["budget_violations"] = json!(violations);
        return Err("ACCEPTANCE_RESOURCE_BUDGET_EXCEEDED");
    }
    Ok(available)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample() -> Value {
        json!({"schema":"okx.native-owner-workload/v1","run_id":"test",
        "operations":64,"concurrency":16,"replays":64,"ledger_legs":128,"sql_jobs":65,
        "latency_us":(1..=64).collect::<Vec<u64>>(),"p50_us":32,"p95_us":61,"max_us":64,
        "max_queue_wait_ms":1,"max_sql_owner_ms":1,"storage_bytes":4096,
        "setup_through_teardown_us":1000})
    }
    #[test]
    fn missing_foreign_incorrect_percentile_and_exceeded_budgets_fail() {
        assert!(validate_workload(&sample(), "test").is_ok());
        for (key, value) in [
            ("run_id", json!("foreign")),
            ("operations", json!(0)),
            ("latency_us", json!([])),
            ("p95_us", json!(64)),
            ("max_queue_wait_ms", json!(5000)),
            ("storage_bytes", json!(33 * MIB)),
            ("setup_through_teardown_us", json!(30000000)),
        ] {
            let mut s = sample();
            s[key] = value;
            assert!(validate_workload(&s, "test").is_err(), "{key}");
        }
        assert!(validate_workload(&json!({}), "test").is_err());
        assert!(verify(&mut json!({}), &Witnesses::new()).is_err());
    }
    fn measured_report() -> Value {
        let mut stages: Vec<Value> = super::super::PACKAGES
            .iter()
            .map(|p| {
                json!({"stage":"libtest","package":p,
                "group":if *p=="okx-trading-engine"{"parallel"}else{"all"},"test_threads":4})
            })
            .collect();
        stages.push(json!({"stage":"libtest","package":"okx-trading-engine",
            "group":"clock_sensitive","test_threads":super::super::CLOCK_SENSITIVE_TEST_THREADS}));
        stages.push(json!({"stage":"retained_transport"}));
        for command in [
            json!(["scripts/check_architecture.py"]),
            json!(["scripts/check_architecture.py", "--self-test"]),
            json!(["scripts/check_ledger_sql.py"]),
        ] {
            stages.push(json!({"stage":"repository_guard","command":command}));
        }
        for stage in &mut stages {
            stage["success"] = json!(true);
            stage["direct_process_resources"] = json!({"status":"MEASURED",
                "scope":"direct_process_only","descendants_included":false,
                "cpu_100ns":1,"read_bytes":1,"write_bytes":1,"peak_working_set_bytes_observed":1});
        }
        json!({"stages":stages,"runtime_setup_to_teardown_ms":1})
    }
    #[test]
    fn metrics_missing_duplicate_foreign_and_excessive_resources_cannot_pass() {
        assert!(verify(&mut measured_report(), &Witnesses::new()).unwrap());
        let mut duplicate = measured_report();
        duplicate["stages"][1] = duplicate["stages"][0].clone();
        assert!(verify(&mut duplicate, &Witnesses::new()).is_err());
        let mut unisolated = measured_report();
        let isolated_index = unisolated["stages"]
            .as_array()
            .unwrap()
            .iter()
            .position(|stage| stage["group"] == "clock_sensitive")
            .unwrap();
        unisolated["stages"][isolated_index]["test_threads"] = json!(4);
        assert!(verify(&mut unisolated, &Witnesses::new()).is_err());
        let mut wrong_group = measured_report();
        wrong_group["stages"][isolated_index]["group"] = json!("parallel");
        assert!(verify(&mut wrong_group, &Witnesses::new()).is_err());
        let mut absent = measured_report();
        absent["stages"]
            .as_array_mut()
            .unwrap()
            .remove(isolated_index);
        assert!(verify(&mut absent, &Witnesses::new()).is_err());
        let mut foreign = measured_report();
        foreign["stages"][0]["package"] = json!("foreign");
        assert!(verify(&mut foreign, &Witnesses::new()).is_err());
        let mut missing = measured_report();
        missing["stages"][0]["direct_process_resources"] = Value::Null;
        assert!(!verify(&mut missing, &Witnesses::new()).unwrap());
        assert!(missing["resource_budgets"]["observed"]["direct_cpu_100ns"].is_null());
        let mut simultaneous = measured_report();
        simultaneous["stages"][0]["direct_process_resources"]["peak_working_set_bytes_observed"] =
            json!(300 * MIB);
        simultaneous["stages"][1]["direct_process_resources"]["peak_working_set_bytes_observed"] =
            json!(300 * MIB);
        // Neither child breaches 512 MiB alone. Their possible 600 MiB
        // overlap must fail, including in a full successful witness run.
        assert!(verify(&mut simultaneous, &Witnesses::new()).is_err());
        assert_eq!(
            simultaneous["budget_violations"][0]["field"],
            "overlap_direct_peak_upper_bound_bytes"
        );
        assert_eq!(
            simultaneous["resource_budgets"]["observed"]["max_direct_peak_bytes"],
            300 * MIB
        );
        for field in [
            "cpu_100ns",
            "read_bytes",
            "write_bytes",
            "peak_working_set_bytes_observed",
        ] {
            let mut exceeded = measured_report();
            exceeded["stages"][0]["direct_process_resources"][field] = json!(u64::MAX);
            assert!(verify(&mut exceeded, &Witnesses::new()).is_err(), "{field}");
        }
        let mut slow = measured_report();
        slow["runtime_setup_to_teardown_ms"] = json!(180001);
        assert!(verify(&mut slow, &Witnesses::new()).is_err());
        assert_eq!(slow["budget_violations"][0]["field"], "wall_ms");
    }
}

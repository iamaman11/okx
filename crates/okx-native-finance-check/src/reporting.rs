//! Failure reporting only: observations here can never grant full acceptance.
use super::{CONTRACTS, Witnesses};
use serde_json::{Value, json};
use std::collections::BTreeSet;

pub(super) fn record_verified(report: &mut Value, verified: &Witnesses) {
    report["observed_witnesses"] = json!(
        verified
            .iter()
            .map(|(package, test)| json!({"package":package,"test":test,"status":"ok"}))
            .collect::<Vec<_>>()
    );
}

pub(super) fn record_failures(
    report: &mut Value,
    package: &str,
    bytes: &[u8],
    expected: &BTreeSet<String>,
) {
    let rows = report
        .as_object_mut()
        .unwrap()
        .entry("failed_witnesses")
        .or_insert_with(|| json!([]));
    let Ok(text) = std::str::from_utf8(bytes) else {
        return;
    };
    let failed: BTreeSet<_> = text
        .lines()
        .filter_map(|line| {
            let (name, status) = line.strip_prefix("test ")?.rsplit_once(" ... ")?;
            (status == "FAILED" && expected.contains(name)).then_some(name)
        })
        .collect();
    for test in failed {
        let cause = failure_cause(text, test);
        rows.as_array_mut()
            .unwrap()
            .push(json!({"package":package,"test":test,
            "cause_code":cause,
            "quality":"OBSERVED_LIBTEST_FAILURE_NOT_SUCCESS_EVIDENCE"}));
    }
}

/// Classify a libtest panic without copying untrusted test output (which can
/// contain account data or credentials) into public CI annotations.
fn failure_cause(output: &str, test: &str) -> &'static str {
    let heading = format!("---- {test} stdout ----");
    let Some((_, after)) = output.split_once(&heading) else {
        return "LIBTEST_FAILED";
    };
    let section = after.split_once("\n---- ").map_or(after, |(part, _)| part);
    if section.contains("unsafe mutation clock evidence") {
        "CLOCK_EVIDENCE_REJECTED"
    } else if section.contains("Elapsed(())") {
        "TEST_READINESS_TIMEOUT"
    } else if section.contains("panicked at") {
        "TEST_PANIC"
    } else {
        "LIBTEST_FAILED"
    }
}

/// The only diagnostic strings allowed on stderr. A failed-witness row is
/// selected from the manifest by `record_failures`; this second validation
/// prevents output-control sequences or user payloads from reaching CI logs.
pub(super) fn diagnostic_lines(report: &Value) -> Vec<String> {
    const MAX_LINES: usize = 12;
    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
    for failed in report["failed_witnesses"].as_array().into_iter().flatten() {
        let (Some(package), Some(test), Some(cause)) = (
            failed["package"].as_str(),
            failed["test"].as_str(),
            failed["cause_code"].as_str(),
        ) else {
            continue;
        };
        let safe = |name: &str, max: usize| {
            !name.is_empty()
                && name.len() <= max
                && name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b':' | b'.' | b'-'))
        };
        if !safe(package, 64)
            || !safe(test, 256)
            || !matches!(
                cause,
                "LIBTEST_FAILED"
                    | "TEST_PANIC"
                    | "CLOCK_EVIDENCE_REJECTED"
                    | "TEST_READINESS_TIMEOUT"
            )
        {
            continue;
        }
        if seen.insert((package, test)) {
            rows.push(format!(
                "NATIVE_FINANCE_WITNESS_FAILED package={package} test={test} cause={cause}"
            ));
        }
        if rows.len() == MAX_LINES {
            break;
        }
    }
    rows
}

pub(super) fn contracts(report: &Value) -> Vec<Value> {
    let pairs = |key: &str| -> Witnesses {
        report[key]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|v| {
                Some((
                    v["package"].as_str()?.to_owned(),
                    v["test"].as_str()?.to_owned(),
                ))
            })
            .collect()
    };
    let verified = pairs("observed_witnesses");
    let mut failed = pairs("failed_witnesses");
    match report["failure"]["stage"].as_str() {
        Some("retained_transport") => {
            failed.insert((
                "repository".into(),
                super::repository_guards::TRANSPORT_WITNESS.into(),
            ));
        }
        Some("repository_guard") => {
            failed.insert((
                "repository".into(),
                super::repository_guards::WITNESS.into(),
            ));
        }
        _ => {}
    }
    let started: BTreeSet<_> = report["stages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|s| s["package"].as_str())
        .collect();
    CONTRACTS
        .iter()
        .map(|c| {
            let missing = c.missing.or(if c.witnesses.is_empty() {
                Some("NO_WITNESS")
            } else if c
                .witnesses
                .iter()
                .any(|w| w.test == super::budgets::WITNESS)
                && report["resource_budgets"]["status"] != "VERIFIED"
            {
                Some("RESOURCE_MEASUREMENTS_NOT_VERIFIED")
            } else {
                None
            });
            let (status, reason) = if let Some(missing) = missing {
                ("MISSING", Some(missing))
            } else if report["overall"] == "BLOCKED" {
                ("NOT_RUN", Some("FULL_MODE_BLOCKED_BY_MISSING_CONTRACTS"))
            } else if c
                .witnesses
                .iter()
                .all(|w| verified.contains(&(w.package.into(), w.test.into())))
            {
                ("PASS", None)
            } else if c
                .witnesses
                .iter()
                .any(|w| failed.contains(&(w.package.into(), w.test.into())))
            {
                ("FAIL", Some("WITNESS_TEST_FAILED"))
            } else if c.witnesses.iter().any(|w| started.contains(w.package)) {
                (
                    "NOT_PROVEN",
                    Some("PACKAGE_DID_NOT_PRODUCE_COMPLETE_VERIFIED_EVIDENCE"),
                )
            } else {
                ("NOT_RUN", Some("WITNESS_NOT_EXECUTED"))
            };
            json!({"id":c.id,"status":status,"missing":missing,"reason":reason})
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn external_failure_is_not_unstarted_and_does_not_erase_guard_proof() {
        let mut report = json!({"overall":"FAIL","failure":{"stage":"retained_transport"}});
        record_verified(
            &mut report,
            &Witnesses::from([(
                "repository".into(),
                super::super::repository_guards::WITNESS.into(),
            )]),
        );
        let rows = contracts(&report);
        for c in CONTRACTS.iter().filter(|c| {
            c.witnesses
                .iter()
                .any(|w| w.test == super::super::repository_guards::TRANSPORT_WITNESS)
        }) {
            assert_eq!(
                rows.iter().find(|r| r["id"] == c.id).unwrap()["status"],
                "FAIL"
            );
        }
        let guard = rows
            .iter()
            .find(|r| r["id"] == "RB13.layer_visibility_and_negative_architecture_guards")
            .unwrap();
        assert_eq!(guard["status"], "PASS");
        assert_eq!(report["overall"], "FAIL");
    }
    #[test]
    fn package_failure_preserves_prior_proof_and_does_not_invent_other_failures() {
        let golden = CONTRACTS
            .iter()
            .find(|c| c.id == "RB02.independent_cumulative_settlement_goldens")
            .unwrap();
        let public = CONTRACTS
            .iter()
            .find(|c| c.id == "RB02.second_instrument_public_spec_permission_and_precision")
            .unwrap();
        let failed = public
            .witnesses
            .iter()
            .find(|w| w.test.contains("native_second_instrument"))
            .unwrap();
        let mut report = json!({"overall":"FAIL","stages":[{"package":"okx-domain-core"},{"package":"okx-trading-engine"}]});
        let verified = golden
            .witnesses
            .iter()
            .map(|w| (w.package.into(), w.test.into()))
            .collect();
        record_verified(&mut report, &verified);
        let expected = BTreeSet::from([failed.test.to_owned()]);
        record_failures(
            &mut report,
            failed.package,
            format!(
                "test {} ... FAILED\ntest foreign::fake ... FAILED\n",
                failed.test
            )
            .as_bytes(),
            &expected,
        );
        let rows = contracts(&report);
        let status = |id: &str| rows.iter().find(|r| r["id"] == id).unwrap()["status"].clone();
        assert_eq!(status(golden.id), "PASS");
        assert_eq!(status(public.id), "FAIL");
        assert!(rows.iter().any(|r| r["status"] == "NOT_RUN"));
        assert!(rows.iter().any(|r| r["status"] == "NOT_PROVEN"));
        assert_eq!(report["failed_witnesses"].as_array().unwrap().len(), 1);
        assert_eq!(report["overall"], "FAIL");
    }

    #[test]
    fn failed_manifest_witnesses_receive_safe_finite_causes() {
        let mut report = json!({"overall":"FAIL"});
        let first = "accounts::native_trade_contracts::native_observation::native_stop_blocks_open";
        let second = "accounts::native_trade_contracts::native_partial_entry::risk_book_contract_native_partial";
        let expected = BTreeSet::from([first.to_string(), second.to_string()]);
        let output = format!(
            "test {first} ... FAILED\ntest foreign::not_selected ... FAILED\ntest {second} ... FAILED\n\
             ---- {first} stdout ----\nthread 'x' panicked at 'unsafe mutation clock evidence: secret=TOKEN'\n\
             ---- {second} stdout ----\nthread 'x' panicked at 'Elapsed(()) secret=PASSWORD'\n"
        );
        record_failures(
            &mut report,
            "okx-trading-engine",
            output.as_bytes(),
            &expected,
        );
        let witnesses = report["failed_witnesses"].as_array().unwrap();
        assert_eq!(witnesses.len(), 2);
        assert_eq!(witnesses[0]["test"], first);
        assert_eq!(witnesses[0]["cause_code"], "CLOCK_EVIDENCE_REJECTED");
        assert_eq!(witnesses[1]["test"], second);
        assert_eq!(witnesses[1]["cause_code"], "TEST_READINESS_TIMEOUT");
        let lines = diagnostic_lines(&report);
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("cause=CLOCK_EVIDENCE_REJECTED"));
        assert!(lines[1].contains("cause=TEST_READINESS_TIMEOUT"));
        assert!(!lines.join("\n").contains("TOKEN"));
        assert!(!lines.join("\n").contains("PASSWORD"));
    }

    #[test]
    fn unknown_panic_and_missing_section_fall_back_to_finite_codes() {
        let first = "test::with_panic";
        let second = "test::without_output";
        let expected = BTreeSet::from([first.to_string(), second.to_string()]);
        let mut report = json!({"overall":"FAIL"});
        let output = format!(
            "test {first} ... FAILED\ntest {second} ... FAILED\n\
             ---- {first} stdout ----\nthread 'test' panicked at arbitrary untrusted payload\n"
        );
        record_failures(
            &mut report,
            "okx-trading-engine",
            output.as_bytes(),
            &expected,
        );
        assert_eq!(report["failed_witnesses"][0]["cause_code"], "TEST_PANIC");
        assert_eq!(
            report["failed_witnesses"][1]["cause_code"],
            "LIBTEST_FAILED"
        );
    }

    #[test]
    fn diagnostics_reject_injection_unknown_causes_and_duplicates() {
        let report = json!({"failed_witnesses":[
            {"package":"okx-trading-engine","test":"valid::witness","cause_code":"TEST_PANIC"},
            {"package":"okx-trading-engine","test":"valid::witness","cause_code":"TEST_PANIC"},
            {"package":"okx-trading-engine","test":"evil\n::warning::injection","cause_code":"TEST_PANIC"},
            {"package":"okx-trading-engine","test":"valid::other","cause_code":"SECRET=PASSWORD"},
            {"package":"okx-trading-engine\u{001b}[0m","test":"valid::other","cause_code":"TEST_PANIC"},
            {"package":"okx-trading-engine","test":"valid::other","cause_code":"CLOCK_EVIDENCE_REJECTED"}
        ]});
        assert_eq!(
            diagnostic_lines(&report),
            vec![
                "NATIVE_FINANCE_WITNESS_FAILED package=okx-trading-engine test=valid::witness cause=TEST_PANIC",
                "NATIVE_FINANCE_WITNESS_FAILED package=okx-trading-engine test=valid::other cause=CLOCK_EVIDENCE_REJECTED",
            ]
        );
        assert!(diagnostic_lines(&json!({"failed_witnesses":{"not":"an array"}})).is_empty());
    }

    #[test]
    fn diagnostic_output_is_bounded_to_twelve_rows() {
        let report = json!({"failed_witnesses": (0..30).map(|i| json!({
            "package":"okx-trading-engine",
            "test":format!("valid::witness_{i}"),
            "cause_code":"LIBTEST_FAILED"
        })).collect::<Vec<_>>()});
        let lines = diagnostic_lines(&report);
        assert_eq!(lines.len(), 12);
        assert!(lines[0].contains("valid::witness_0"));
        assert!(lines[11].contains("valid::witness_11"));
    }
}

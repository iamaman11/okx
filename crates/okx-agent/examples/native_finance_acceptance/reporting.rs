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
        rows.as_array_mut()
            .unwrap()
            .push(json!({"package":package,"test":test,
            "quality":"OBSERVED_LIBTEST_FAILURE_NOT_SUCCESS_EVIDENCE"}));
    }
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
        let mut report =
            json!({"overall":"FAIL","stages":[{"package":"okx-kernel"},{"package":"okx-agent"}]});
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
}

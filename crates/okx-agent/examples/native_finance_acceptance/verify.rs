//! Exact current-build artifact binding and fail-closed libtest verification.
use super::{CONTRACTS, PACKAGES, Result, Witnesses};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

// An old manually copied collector must not certify a newer source tree with
// its embedded older manifest. Cargo-run is the supported entrypoint; this
// additionally binds every part of that collector to the inspected checkout.
fn check_source_bytes(actual: &[u8], embedded: &[u8]) -> Result<()> {
    if actual != embedded {
        return Err("COLLECTOR_SOURCE_BUILD_MISMATCH");
    }
    Ok(())
}
pub(super) fn collector_sources_match(repo: &Path) -> Result<()> {
    for (relative, embedded) in [
        (
            "native_finance_acceptance.rs",
            include_bytes!("../native_finance_acceptance.rs").as_slice(),
        ),
        (
            "native_finance_acceptance/manifest.rs",
            include_bytes!("manifest.rs").as_slice(),
        ),
        (
            "native_finance_acceptance/verify.rs",
            include_bytes!("verify.rs").as_slice(),
        ),
        (
            "native_finance_acceptance/process.rs",
            include_bytes!("process.rs").as_slice(),
        ),
        (
            "native_finance_acceptance/repository_guards.rs",
            include_bytes!("repository_guards.rs").as_slice(),
        ),
    ] {
        let path = repo.join("crates/okx-agent/examples").join(relative);
        let actual = std::fs::read(path).map_err(|_| "COLLECTOR_SOURCE_UNAVAILABLE")?;
        check_source_bytes(&actual, embedded)?;
    }
    Ok(())
}
pub(super) fn artifacts(repo: &Path, bytes: &[u8]) -> Result<BTreeMap<String, PathBuf>> {
    let mut binaries = BTreeMap::new();
    for line in bytes.split(|b| *b == b'\n').filter(|line| !line.is_empty()) {
        let v: Value = serde_json::from_slice(line).map_err(|_| "BUILD_JSON_INVALID")?;
        if v["reason"] != "compiler-artifact"
            || v["profile"]["test"] != true
            || v["executable"].is_null()
        {
            continue;
        }
        let Some(package) = PACKAGES
            .iter()
            .find(|p| v["target"]["name"] == p.replace('-', "_"))
        else {
            continue;
        };
        if !v["target"]["kind"]
            .as_array()
            .is_some_and(|k| k.iter().any(|v| v == "lib"))
        {
            continue;
        }
        let actual = Path::new(
            v["manifest_path"]
                .as_str()
                .ok_or("BUILD_MANIFEST_MISSING")?,
        )
        .canonicalize()
        .map_err(|_| "BUILD_MANIFEST_INVALID")?;
        let expected = repo
            .join("crates")
            .join(package)
            .join("Cargo.toml")
            .canonicalize()
            .map_err(|_| "BUILD_MANIFEST_INVALID")?;
        if actual != expected {
            return Err("BUILD_PACKAGE_SCOPE_MISMATCH");
        }
        let binary = PathBuf::from(v["executable"].as_str().ok_or("BUILD_EXECUTABLE_INVALID")?);
        if !binary.is_absolute() || !binary.is_file() {
            return Err("BUILD_EXECUTABLE_INVALID");
        }
        if binaries.insert(package.to_string(), binary).is_some() {
            return Err("BUILD_DUPLICATE_EXECUTABLE");
        }
    }
    if binaries.len() != PACKAGES.len() {
        return Err("BUILD_EXECUTABLE_MISSING");
    }
    Ok(binaries)
}

pub(super) fn verify_output(bytes: &[u8], expected: &BTreeSet<String>) -> Result<BTreeSet<String>> {
    if expected.is_empty() {
        return Err("NO_SELECTED_WITNESSES");
    }
    let text = std::str::from_utf8(bytes).map_err(|_| "LIBTEST_OUTPUT_INVALID")?;
    let mut observed = BTreeSet::new();
    let mut summary = 0;
    let mut running = 0;
    for line in text.lines() {
        if let Some(count) = line
            .strip_prefix("running ")
            .and_then(|v| v.strip_suffix(" tests").or_else(|| v.strip_suffix(" test")))
        {
            if count.parse::<usize>().ok() != Some(expected.len()) {
                return Err("LIBTEST_RUN_COUNT_MISMATCH");
            }
            running += 1;
        } else if let Some(rest) = line.strip_prefix("test result: ") {
            let wanted = format!(
                "ok. {} passed; 0 failed; 0 ignored; 0 measured; ",
                expected.len()
            );
            let remainder = rest.strip_prefix(&wanted).ok_or("LIBTEST_SUMMARY_FAILED")?;
            let (filtered, elapsed) = remainder
                .split_once(" filtered out; finished in ")
                .ok_or("LIBTEST_SUMMARY_INVALID")?;
            if filtered.parse::<usize>().is_err()
                || elapsed
                    .strip_suffix('s')
                    .and_then(|v| v.parse::<f64>().ok())
                    .is_none_or(|v| !v.is_finite() || v < 0.0)
            {
                return Err("LIBTEST_SUMMARY_INVALID");
            }
            summary += 1;
        } else if let Some(rest) = line.strip_prefix("test ") {
            let (name, status) = rest.split_once(" ... ").ok_or("LIBTEST_LINE_INVALID")?;
            if !expected.contains(name) {
                return Err("LIBTEST_FOREIGN_WITNESS");
            }
            if status != "ok" {
                return Err("LIBTEST_WITNESS_NOT_OK");
            }
            if !observed.insert(name.to_owned()) {
                return Err("LIBTEST_DUPLICATE_WITNESS");
            }
        } else if !line.trim().is_empty() {
            return Err("LIBTEST_UNEXPECTED_OUTPUT");
        }
    }
    if running != 1 || summary != 1 || observed != *expected {
        return Err("LIBTEST_WITNESS_MISSING");
    }
    Ok(observed)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn collector_rejects_changed_manifest_instead_of_reporting_new_head() {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap();
        collector_sources_match(repo).unwrap();
        let original = include_bytes!("manifest.rs");
        assert_eq!(check_source_bytes(original, original), Ok(()));
        assert_eq!(
            check_source_bytes(b"old or substituted manifest", original),
            Err("COLLECTOR_SOURCE_BUILD_MISMATCH")
        );
    }
    fn expected() -> BTreeSet<String> {
        BTreeSet::from(["scope::a".into(), "scope::b".into()])
    }
    fn output() -> String {
        "running 2 tests\ntest scope::a ... ok\ntest scope::b ... ok\n\ntest result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 8 filtered out; finished in 0.01s\n".into()
    }
    #[test]
    fn exact_complete_output_is_accepted() {
        assert_eq!(
            verify_output(output().as_bytes(), &expected()).unwrap(),
            expected()
        );
    }
    #[test]
    fn missing_duplicate_foreign_ignored_failed_and_zero_output_are_rejected() {
        for text in [
            String::new(),
            output().replace("test scope::b ... ok\n", ""),
            output().replace("scope::b", "scope::a"),
            output().replace("scope::b", "foreign::b"),
            output().replace("scope::b ... ok", "scope::b ... ignored"),
            output().replace("scope::b ... ok", "scope::b ... FAILED"),
            output().replace("running 2 tests", "running 0 tests"),
            output().replace("0 ignored", "1 ignored"),
            output().replace("0.01s", "invalids"),
            output() + "unexpected child output\n",
        ] {
            assert!(
                verify_output(text.as_bytes(), &expected()).is_err(),
                "accepted: {text}"
            );
        }
        assert!(verify_output(output().as_bytes(), &BTreeSet::new()).is_err());
    }
    #[test]
    fn absent_malformed_and_duplicate_build_artifacts_are_rejected() {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap();
        assert!(artifacts(repo, b"").is_err());
        assert!(artifacts(repo, b"old success text\n").is_err());
        let mut artifact = json!({"reason":"compiler-artifact", "profile":{"test":true},
            "executable":std::env::current_exe().unwrap(), "manifest_path":repo.join("crates/okx-kernel/Cargo.toml"),
            "target":{"name":"okx_kernel","kind":["lib"]}});
        assert!(matches!(
            artifacts(repo, format!("{artifact}\n{artifact}\n").as_bytes()),
            Err("BUILD_DUPLICATE_EXECUTABLE")
        ));
        artifact["manifest_path"] = json!(repo.join("crates").join("okx-agent").join("Cargo.toml"));
        assert!(matches!(
            artifacts(repo, artifact.to_string().as_bytes()),
            Err("BUILD_PACKAGE_SCOPE_MISMATCH")
        ));
    }
}

pub(super) fn manifest_json() -> Vec<Value> {
    CONTRACTS.iter().map(|c| json!({"id":c.id,"missing":c.missing,
        "witnesses":c.witnesses.iter().map(|w| json!({"package":w.package,"test":w.test})).collect::<Vec<_>>()})).collect()
}
pub(super) fn selected() -> Result<Witnesses> {
    validate_ids(&CONTRACTS.iter().map(|c| c.id).collect::<Vec<_>>())?;
    let mut witnesses = BTreeSet::new();
    for c in CONTRACTS {
        for w in c.witnesses {
            if (!PACKAGES.contains(&w.package)
                && !(w.package == "repository"
                    && [
                        super::repository_guards::WITNESS,
                        super::repository_guards::TRANSPORT_WITNESS,
                    ]
                    .contains(&w.test)))
                || w.test.is_empty()
                || !w
                    .test
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b':')
            {
                return Err("MANIFEST_WITNESS_INVALID");
            }
            witnesses.insert((w.package.to_owned(), w.test.to_owned()));
        }
    }
    Ok(witnesses)
}
fn validate_ids(ids: &[&str]) -> Result<()> {
    let mut unique = BTreeSet::new();
    for id in ids {
        if !unique.insert(id) {
            return Err("MANIFEST_DUPLICATE_CONTRACT");
        }
        let (umbrella, atom) = id.split_once('.').ok_or("MANIFEST_CONTRACT_ID_INVALID")?;
        if !(1..=16).any(|n| umbrella == format!("RB{n:02}"))
            || atom.is_empty()
            || !atom
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        {
            return Err("MANIFEST_CONTRACT_ID_INVALID");
        }
    }
    Ok(())
}
pub(super) fn absent_umbrellas() -> Vec<String> {
    absent_in(&CONTRACTS.iter().map(|c| c.id).collect::<Vec<_>>())
}
fn absent_in(ids: &[&str]) -> Vec<String> {
    (1..=16)
        .map(|n| format!("RB{n:02}"))
        .filter(|id| !ids.iter().any(|c| c.split('.').next() == Some(id.as_str())))
        .collect()
}
pub(super) fn incomplete() -> bool {
    !absent_umbrellas().is_empty()
        || CONTRACTS.is_empty()
        || CONTRACTS
            .iter()
            .any(|c| c.missing.is_some() || c.witnesses.is_empty())
}

#[cfg(test)]
mod manifest_tests {
    use super::*;
    #[test]
    fn invalid_duplicate_or_missing_umbrella_cannot_complete_manifest() {
        for ids in [
            &["RB00.a"][..],
            &["RB17.a"],
            &["RB1.a"],
            &["RB01."],
            &["RB01.a", "RB01.a"],
        ] {
            assert!(validate_ids(ids).is_err());
        }
        let complete: Vec<String> = (1..=16).map(|n| format!("RB{n:02}.a")).collect();
        let ids: Vec<&str> = complete.iter().map(String::as_str).collect();
        assert!(validate_ids(&ids).is_ok());
        assert!(absent_in(&ids).is_empty());
        assert_eq!(absent_in(&ids[..15]), vec!["RB16"]);
    }
}

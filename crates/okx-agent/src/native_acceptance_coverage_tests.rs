//! Pure checks for a future aggregation of actual Rust runtime results.
//! No runner or production-property mapping is installed here. The fixtures
//! below test this verifier only; they are not RB acceptance or a 26-case run.
use std::collections::{BTreeMap, BTreeSet};

const REQUIRED_PROPERTIES: [&str; 16] = [
    "RB01", "RB02", "RB03", "RB04", "RB05", "RB06", "RB07", "RB08", "RB09", "RB10", "RB11", "RB12",
    "RB13", "RB14", "RB15", "RB16",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scope {
    Full,
    Subset,
}

#[derive(Clone, Copy, Debug)]
struct RequiredProperty<'a> {
    id: &'a str,
    test_symbol: &'a str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Status {
    Passed,
    Failed,
    Skipped,
    Ignored,
    ChildFailed,
    TimedOut,
    TeardownIncomplete,
}

#[derive(Clone, Copy, Debug)]
struct RuntimeOutcome<'a> {
    property: &'a str,
    test_symbol: &'a str,
    run_id: &'a str,
    source_sha: &'a str,
    status: Status,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Coverage {
    FullManifestComplete,
    SubsetComplete,
}

#[derive(Debug, PartialEq, Eq)]
enum Incomplete {
    InvalidContext,
    InvalidManifest,
    ZeroResults,
    DuplicateResult,
    UnexpectedProperty,
    ForeignTest,
    ForeignRun,
    ForeignSha,
    NonPassingResult,
    MissingResult,
}

/// `required` must be bound to reviewed runtime test symbols by the eventual
/// caller. Source text, old test counts and historical artifacts are not input.
/// An RB property can require several tests: each (property, symbol) pair is
/// mandatory. Full coverage additionally requires every RB ID in the manifest.
fn verify(
    scope: Scope,
    required: &[RequiredProperty<'_>],
    run_id: &str,
    source_sha: &str,
    outcomes: &[RuntimeOutcome<'_>],
) -> Result<Coverage, Incomplete> {
    if run_id.is_empty()
        || source_sha.len() != 40
        || !source_sha
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Incomplete::InvalidContext);
    }
    let mut expected = BTreeSet::new();
    let mut properties = BTreeSet::new();
    for case in required {
        if !REQUIRED_PROPERTIES.contains(&case.id)
            || case.test_symbol.is_empty()
            || !expected.insert((case.id, case.test_symbol))
        {
            return Err(Incomplete::InvalidManifest);
        }
        properties.insert(case.id);
    }
    if expected.is_empty()
        || (scope == Scope::Full
            && properties != REQUIRED_PROPERTIES.into_iter().collect::<BTreeSet<_>>())
    {
        return Err(Incomplete::InvalidManifest);
    }
    if outcomes.is_empty() {
        return Err(Incomplete::ZeroResults);
    }
    let mut observed = BTreeMap::new();
    for outcome in outcomes {
        if !properties.contains(outcome.property) {
            return Err(Incomplete::UnexpectedProperty);
        }
        let key = (outcome.property, outcome.test_symbol);
        if !expected.contains(&key) {
            return Err(Incomplete::ForeignTest);
        }
        if observed.insert(key, outcome.status).is_some() {
            return Err(Incomplete::DuplicateResult);
        }
        if outcome.run_id != run_id {
            return Err(Incomplete::ForeignRun);
        }
        if outcome.source_sha != source_sha {
            return Err(Incomplete::ForeignSha);
        }
        if outcome.status != Status::Passed {
            return Err(Incomplete::NonPassingResult);
        }
    }
    if observed.len() != expected.len() {
        return Err(Incomplete::MissingResult);
    }
    Ok(match scope {
        Scope::Full => Coverage::FullManifestComplete,
        Scope::Subset => Coverage::SubsetComplete,
    })
}

const SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const RUN: &str = "synthetic-verifier-self-test";

fn fixture() -> (Vec<RequiredProperty<'static>>, Vec<RuntimeOutcome<'static>>) {
    // Synthetic verifier input only. No executed financial claim is made.
    let required: Vec<_> = REQUIRED_PROPERTIES
        .into_iter()
        .map(|id| RequiredProperty {
            id,
            test_symbol: "coverage_self_test::synthetic_result",
        })
        .collect();
    let outcomes = required
        .iter()
        .map(|case| RuntimeOutcome {
            property: case.id,
            test_symbol: case.test_symbol,
            run_id: RUN,
            source_sha: SHA,
            status: Status::Passed,
        })
        .collect();
    (required, outcomes)
}

#[test]
fn coverage_verifier_accepts_complete_runtime_result_set_and_labels_subset() {
    let (required, outcomes) = fixture();
    assert_eq!(
        verify(Scope::Full, &required, RUN, SHA, &outcomes),
        Ok(Coverage::FullManifestComplete)
    );
    assert_eq!(
        verify(Scope::Subset, &required[..1], RUN, SHA, &outcomes[..1]),
        Ok(Coverage::SubsetComplete)
    );
    assert_eq!(
        verify(Scope::Full, &required[..13], RUN, SHA, &outcomes[..13]),
        Err(Incomplete::InvalidManifest)
    );
}

#[test]
fn coverage_verifier_rejects_missing_zero_duplicate_and_foreign_results() {
    let (required, outcomes) = fixture();
    assert_eq!(
        verify(Scope::Full, &required, RUN, SHA, &[]),
        Err(Incomplete::ZeroResults)
    );
    assert_eq!(
        verify(Scope::Full, &required, RUN, SHA, &outcomes[..15]),
        Err(Incomplete::MissingResult)
    );
    let mut duplicate = outcomes.clone();
    duplicate.push(outcomes[0]);
    assert_eq!(
        verify(Scope::Full, &required, RUN, SHA, &duplicate),
        Err(Incomplete::DuplicateResult)
    );
    for (field, error) in [
        ("property", Incomplete::UnexpectedProperty),
        ("symbol", Incomplete::ForeignTest),
        ("run", Incomplete::ForeignRun),
        ("sha", Incomplete::ForeignSha),
    ] {
        let mut altered = outcomes.clone();
        match field {
            "property" => altered[0].property = "RB99",
            "symbol" => altered[0].test_symbol = "old_fixture::historical_pass",
            "run" => altered[0].run_id = "previous-run",
            "sha" => altered[0].source_sha = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            _ => unreachable!(),
        }
        assert_eq!(
            verify(Scope::Full, &required, RUN, SHA, &altered),
            Err(error)
        );
    }
}

#[test]
fn coverage_verifier_rejects_skips_child_failure_timeout_and_incomplete_teardown() {
    let (required, outcomes) = fixture();
    for status in [
        Status::Failed,
        Status::Skipped,
        Status::Ignored,
        Status::ChildFailed,
        Status::TimedOut,
        Status::TeardownIncomplete,
    ] {
        let mut altered = outcomes.clone();
        altered[0].status = status;
        assert_eq!(
            verify(Scope::Full, &required, RUN, SHA, &altered),
            Err(Incomplete::NonPassingResult)
        );
    }
}

#[test]
fn coverage_verifier_requires_each_symbol_for_a_property_and_valid_manifest_context() {
    let (mut required, outcomes) = fixture();
    required.push(RequiredProperty {
        id: "RB08",
        test_symbol: "coverage_self_test::physical_child",
    });
    assert_eq!(
        verify(Scope::Full, &required, RUN, SHA, &outcomes),
        Err(Incomplete::MissingResult)
    );
    required.pop();
    required.push(required[0]);
    assert_eq!(
        verify(Scope::Full, &required, RUN, SHA, &outcomes),
        Err(Incomplete::InvalidManifest)
    );
    assert_eq!(
        verify(Scope::Full, &[], RUN, SHA, &outcomes),
        Err(Incomplete::InvalidManifest)
    );
    assert_eq!(
        verify(Scope::Full, &required, "", SHA, &outcomes),
        Err(Incomplete::InvalidContext)
    );
    assert_eq!(
        verify(Scope::Full, &required, RUN, "HEAD", &outcomes),
        Err(Incomplete::InvalidContext)
    );
}

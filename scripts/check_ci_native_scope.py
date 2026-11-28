#!/usr/bin/env python3
"""Fail-closed CI impact classification from the complete GitHub changed-file list.

The classifier decides which component changed. Research release reuse requires
separate Cargo dependency-closure and immutable-artifact proof.
CONTROL/Launcher Rust source changes have a separate Windows host/consumer
gate. Unknown paths, workflow/dependency edits and malformed/truncated API
data always require the full native financial gate.
"""

import argparse
import json
import os
from pathlib import Path
import re
import sys


_PART = re.compile(r"^[A-Za-z0-9_.-]+$")
# The core is linked only by Research/Analysis; the architecture DAG gate
# rejects any transitive Native Agent dependency before this fast path is used.
_RESEARCH_DIRS = (
    "crates/okx-research/", "crates/okx-research-service/",
    "crates/okx-strategy-core/", "crates/okx-analysis/",
)
_RESEARCH_PROTECTED_SOURCES = (
    "crates/okx-research-protocol/", "crates/okx-research-domain/",
)
_ENGINE_DIRS = ("crates/okx-trading-engine/", "crates/okx-trade-execution/")
_CLOUDFLARE_DIR = "cloudflare/okx-mcp/"
_HOST_CONTROL_TEST = "crates/okx-host-controller/tests/"
_HOST_LAUNCHER_TEST = "crates/okx-host-launcher/tests/"
_HOST_PROTOCOL_TEST = "crates/okx-host-protocol/tests/"
# CONTROL's attestor is a Host-only binary owner, but its frame protocol is
# consumed by the Edge Durable Object. Host-only edits still need the actual
# Cloudflare MCP/DO/WSS contract tests, without a financial Engine rebuild.
_EDGE_WIRE_SHARED_HOST_SOURCE = "crates/okx-host-controller/src/attestor.rs"
# Host-only release is permitted for source files whose runtime authority is
# demonstrably isolated from Engine installation, desired state and lifecycle.
# An entire crate-level src/ prefix is NOT such a proof: runtime.rs, artifact.rs,
# desired.rs, autostart.rs and entrypoints can install/restart Engine. Unknown or
# newly introduced Host source defaults to Full until explicitly reviewed.
_ISOLATED_HOST_CONTROL_SOURCE = {
    _EDGE_WIRE_SHARED_HOST_SOURCE,
}
_ISOLATED_HOST_LAUNCHER_SOURCE = {
    "crates/okx-host-launcher/src/logging.rs",
    "crates/okx-host-launcher/src/stdio_capture.rs",
}
_RESEARCH_MANIFESTS = {
    "crates/okx-research/Cargo.toml",
    "crates/okx-analysis/Cargo.toml",
    "crates/okx-research-service/Cargo.toml",
    "crates/okx-strategy-core/Cargo.toml",
    "crates/okx-research-protocol/Cargo.toml",
    "crates/okx-research-domain/Cargo.toml",
}
_HOST_MANIFESTS = {
    "crates/okx-host-controller/Cargo.toml",
    "crates/okx-host-launcher/Cargo.toml",
}
DRAFT_CHECKS = frozenset({
    "kernel", "strategy", "analysis", "research", "worker", "worker-ipc",
    "wire", "ipc", "mcp", "host", "expanded",
})
# Research starts separately, but the Windows resource policy and process
# entrypoint can still affect the measured Finance-vs-Research load boundary.
_ENGINE_RESEARCH_RUNTIME_POLICY = {
    "crates/okx-research-service/src/process_memory.rs",
    "crates/okx-research-service/src/main.rs",
}
# These Host implementation files own the trading Engine process/Job as well
# as Research. Host-only CI would not exercise the Engine financial lifecycle
# after an edit here. Keep them in the native matrix until that shared owner
# is physically split into independently testable modules.
_ENGINE_HOST_LIFECYCLE = {
    "crates/okx-host-controller/src/job.rs",
    "crates/okx-host-controller/src/lifecycle_executor.rs",
    "crates/okx-host-controller/src/runtime.rs",
    "crates/okx-host-controller/src/artifact.rs",
    "crates/okx-host-controller/src/desired.rs",
    "crates/okx-host-controller/src/autostart.rs",
    "crates/okx-host-controller/src/controller_update.rs",
    "crates/okx-host-controller/src/legacy_bootstrap.rs",
    "crates/okx-host-controller/src/root_migration.rs",
    "crates/okx-host-controller/src/main.rs",
}


def category(path):
    if not isinstance(path, str) or not 1 <= len(path) <= 512 or "\\" in path:
        return "full"
    parts = path.split("/")
    if not all(part not in ("", ".", "..") and _PART.fullmatch(part) for part in parts):
        return "full"
    if path == "README.md" or (path.startswith("docs/") and path.endswith(".md")):
        return "docs"
    if path.startswith(_CLOUDFLARE_DIR):
        return "cloudflare"
    if path in _ENGINE_RESEARCH_RUNTIME_POLICY or path in _ENGINE_HOST_LIFECYCLE:
        return "full"
    # An explicit source allowlist protects the Engine lifecycle boundary.
    # Host test-only files cannot alter a shipping executable; however all
    # Host protocol implementation changes must still use Full until every
    # financial/Engine consumer of the wire schema has been proved absent.
    if path in _ISOLATED_HOST_CONTROL_SOURCE or (
        path.endswith(".rs") and path.startswith((_HOST_CONTROL_TEST, _HOST_PROTOCOL_TEST))
    ):
        return "host-controller"
    if path in _ISOLATED_HOST_LAUNCHER_SOURCE or (
        path.endswith(".rs") and path.startswith(_HOST_LAUNCHER_TEST)
    ):
        return "host-launcher"
    # A scoped Research change must be Rust source or an integration test.
    # Arbitrary configs, SQL fixtures, generated code and build inputs can
    # affect the installed product; they must not bypass the Native gate.
    if _research_source(path):
        return "research"
    # These new Research crates are still included by an Engine dev-only
    # compatibility test. They become Research-only *only after* the exact
    # before/after Windows test closure proves the dependency removed.
    if _research_protected_source(path):
        return "full"
    # Engine/Execution are both owned by the single financial writer. Their
    # source-only changes still require every financial property; only the
    # *shipping* executable may be narrowed after accepted-base proof.
    if path.endswith(".rs") and any(path.startswith(root + owner)
                                   for root in _ENGINE_DIRS for owner in ("src/", "tests/")):
        return "engine"
    return "full"


def verified_entries(pages):
    """Validate the complete GitHub paginated diff before using any scope."""
    if not isinstance(pages, list) or not 1 <= len(pages) <= 30:
        return None
    files = []
    for page in pages:
        if not isinstance(page, list) or len(page) > 100:
            return None
        files.extend(page)
    if not 1 <= len(files) <= 500:
        return None
    paths = set()
    for item in files:
        if not isinstance(item, dict):
            return None
        path = item.get("filename")
        if (
            not isinstance(path, str)
            or not 1 <= len(path) <= 512 or "\\" in path
            or not all(_PART.fullmatch(p) and p not in ("", ".", "..") for p in path.split("/"))
            or item.get("status") not in {"added", "modified", "removed"}
            or "previous_filename" in item
            or path in paths
        ):
            return None
        paths.add(path)
    return files


def classify(pages):
    """Classify complete GH paginated changed-file pages; fail closed."""
    files = verified_entries(pages)
    if files is None:
        return "full"
    return _scope({category(item["filename"]) for item in files})


def _scope(categories):
    if "full" in categories:
        return "full"
    if "engine" in categories:
        return "engine" if categories <= {"engine", "docs", "cloudflare"} else "full"
    if "research" in categories:
        if categories & {"host-controller", "host-launcher"}:
            return "research-host"
        return "research"
    if categories & {"host-controller", "host-launcher"}:
        # CONTROL consumes Launcher: either change needs both owner and
        # consumer checks; the distinct scope remains visible in CI evidence.
        return "host-controller" if "host-controller" in categories else "host-launcher"
    if "cloudflare" in categories:
        return "cloudflare"
    return "docs"


def _research_source(path):
    return path.endswith(".rs") and any(
        path.startswith(root + owner) for root in _RESEARCH_DIRS
        for owner in ("src/", "tests/")
    )


def _research_protected_source(path):
    return path.endswith(".rs") and any(
        path.startswith(root + owner) for root in _RESEARCH_PROTECTED_SOURCES
        for owner in ("src/", "tests/")
    )


def draft_checks(path, kind):
    """Map the classified owner to its short Draft checks."""
    if kind == "docs":
        return set()
    if kind == "cloudflare":
        return {"mcp", "wire", "worker"}
    if path == _EDGE_WIRE_SHARED_HOST_SOURCE:
        # The draft producer must be checked against its Edge decoder, too.
        return {"host", "mcp"}
    if kind in {"host-controller", "host-launcher"}:
        return {"host"}
    if kind == "research":
        if path.startswith("crates/okx-strategy-core/src/"):
            return {"strategy", "research", "worker", "analysis"}
        if path.startswith("crates/okx-analysis/src/"):
            return {"analysis", "kernel"}
        if path.startswith("crates/okx-research/src/"):
            return {"research", "worker"}
        if path.startswith(("crates/okx-research-service/src/ipc/", "crates/okx-research-service/tests/")) or (
            path == "crates/okx-research-service/src/ipc.rs"
        ):
            return {"worker", "worker-ipc"}
        if path.startswith("crates/okx-research-service/src/"):
            return {"worker"}
        if path.startswith(("crates/okx-research-protocol/", "crates/okx-research-domain/")):
            return {"wire", "strategy", "research", "worker"}
    if kind == "engine":
        return {"expanded"}
    if path == "crates/okx-research-service/src/main.rs":
        return {"ipc", "worker"}
    if path == "crates/okx-research-service/src/process_memory.rs":
        return {"worker"}
    if path == "crates/okx-research-protocol/src/strategy_contract.rs":
        return {"wire", "ipc", "worker", "mcp", "strategy", "research", "analysis"}
    if path.startswith(("crates/okx-ipc-contracts/src/", "crates/okx-research-protocol/src/")) and path.endswith(".rs"):
        return {"wire", "ipc", "worker", "mcp"}
    return {"expanded"}


def select_draft(status, path):
    return draft_checks(path, category(path)) if status in {"added", "modified"} else {"expanded"}


def github_plan(pages, *, allow_dependency_candidate):
    files = verified_entries(pages)
    scope, candidate, edge, checks = "full", "none", False, {"expanded"}
    if files is not None:
        owners = [(entry, category(entry["filename"])) for entry in files]
        scope = _scope({kind for _, kind in owners})
        candidate = dependency_candidate(pages) if allow_dependency_candidate else "none"
        edge = any(_needs_edge_witness(entry["filename"]) for entry, _ in owners)
        checks = set()
        for entry, kind in owners:
            checks.update(draft_checks(entry["filename"], kind)
                          if entry["status"] != "removed" else {"expanded"})
        if "expanded" in checks:
            checks = {"expanded"}
    return {
        "scope": scope,
        "manifest_candidate": candidate,
        "edge_changed": str(edge).lower(),
        "draft_checks": ",".join(sorted(checks)),
        "draft_mcp": str(bool(checks & {"mcp", "expanded"})).lower(),
        "diff_verified": str(files is not None).lower(),
    }


def _needs_edge_witness(path):
    return category(path) == "cloudflare" or path == _EDGE_WIRE_SHARED_HOST_SOURCE


def edge_changed(pages):
    # Historical CI output name: this is the *required Edge test* signal,
    # including changes to the native CONTROL producer of the shared wire.
    files = verified_entries(pages)
    return files is not None and any(_needs_edge_witness(entry["filename"]) for entry in files)


def dependency_candidate(pages):
    """Tentative owner only; NEVER authorizes skipping Native by itself.

    The resolved Windows Cargo closure must separately prove this candidate.
    A missing manifest, changed CI/build script, or mixed owner is full.
    """
    files = verified_entries(pages)
    if files is None:
        return "none"
    manifests = [entry["filename"] for entry in files
                 if entry["filename"] in _RESEARCH_MANIFESTS | _HOST_MANIFESTS]
    protected_sources = [entry["filename"] for entry in files
                         if _research_protected_source(entry["filename"])]
    if (not manifests and not protected_sources) or any(
        entry["status"] != "modified" for entry in files
        if entry["filename"] in _RESEARCH_MANIFESTS | _HOST_MANIFESTS | {"Cargo.lock"}
        or _research_protected_source(entry["filename"])
    ):
        return "none"
    kinds = set()
    for entry in files:
        path = entry["filename"]
        if path == "Cargo.lock":
            continue
        if path in _RESEARCH_MANIFESTS or category(path) == "research" or _research_protected_source(path):
            kinds.add("research")
        elif path in _HOST_MANIFESTS or category(path) in {"host-controller", "host-launcher"}:
            kinds.add("host")
        elif category(path) not in {"docs", "cloudflare"}:
            return "none"
    if kinds == {"research"}:
        return "research"
    if kinds == {"host"}:
        return "host-controller" if any(path.startswith("crates/okx-host-controller/") for path in manifests +
                                   [entry["filename"] for entry in files]) else "host-launcher"
    return "none"


def _self_test() -> None:
    # Post-merge reuse must skip Research compilation, not just full Native.
    repository = Path(__file__).resolve().parents[1]
    workflow = (repository / ".github/workflows/ci.yml").read_text()
    # Named Ready witnesses must exist in actual test sources. Checking only
    # workflow strings would allow a harmless Rust rename to turn a 10-minute
    # Windows matrix into an avoidable missing-witness failure.
    source_witnesses = {
        "crates/okx-host-controller/tests/research_process_independence.rs": (
            "one_process_job_can_be_terminated_without_touching_the_other",
            "nested_job_assignment_waits_for_parent_ready_receipt",
            "research_control_root_is_independent_and_start_requires_verified_installation",
            "corrupted_and_hash_mismatched_research_image_never_launch",
            "standalone_research_status_is_typed_and_does_not_claim_edge_online",
            "standalone_research_lifecycle_requires_exact_image_and_process_generation",
        ),
        "crates/okx-host-controller/src/research_supervisor.rs": (
            "research_state_is_strict_and_independent_of_engine_state",
        ),
        "crates/okx-host-protocol/src/lib.rs": (
            "typed_research_rollback_requires_two_distinct_exact_hashes",
            "standalone_research_lifecycle_requires_exact_image_and_process_generation",
            "standalone_research_status_is_typed_and_does_not_claim_edge_online",
            "research_token_provision_command_has_no_bearer_fields",
        ),
        "crates/okx-bridge-mcp/src/lib.rs": (
            "control_bridge_denies_research_mutations_before_network",
        ),
        "crates/okx-research-service/src/edge_credentials.rs": (
            "research_token_validation_is_bounded_and_ascii_only",
            "research_native_target_is_distinct_from_engine",
            "mock_store_never_exposes_research_bearer_to_engine_target",
        ),
        "crates/okx-research-service/src/edge_runtime.rs": (
            "research_starts_only_after_finite_supervisor_job_receipt",
            "installed_research_bearer_is_pinned_to_control_edge_origin",
        ),
    }
    for source_file, names in source_witnesses.items():
        source = (repository / source_file).read_text(encoding="utf-8")
        for name in names:
            assert re.search(r"(?m)^\s*(?:async\s+)?fn\s+" + re.escape(name) + r"\s*\(", source), (
                f"CI_NAMED_RESEARCH_WITNESS_NOT_IN_SOURCE:{source_file}:{name}"
            )
    # The Actions shell guard must accept exactly the names produced by this
    # classifier. A stale host-control spelling once rejected host-controller
    # source changes before their dedicated short Windows job could start.
    expected_scopes = {"docs", "cloudflare", "engine", "research", "research-host", "host-controller", "host-launcher", "full"}
    selector = workflow.split("\n  component_impact:\n", 1)[1].split("\n  draft-component-tests:\n", 1)[0]
    scope_guards = re.findall(r'case "\$scope" in ([a-z|-]+)\)', selector)
    assert len(scope_guards) == 1, "CI_SCOPE_GUARD_MISSING_OR_AMBIGUOUS"
    assert set(scope_guards[0].split("|")) == expected_scopes, "CI_SCOPE_GUARD_DRIFT"
    assert "component_scope: ${{ steps.classify.outputs.component_scope || steps.proven_scope.outputs.scope }}" in workflow
    assert "ACCEPTED_MAIN_COMPONENT_SCOPE=research" in selector
    assert "steps.file_scope.outputs.manifest_candidate" in selector
    assert 'research_only: ${{ steps.classify.outputs.research_only }}' in workflow
    assert 'scope="${{ steps.proven_scope.outputs.scope }}"' in workflow
    assert "--filter-platform x86_64-pc-windows-msvc" in workflow
    protected = workflow.split("\n  required_ci:\n", 1)[1].split("\n  trading_engine_build_windows:\n", 1)[0]
    assert "needs: [component_impact, dependency-audit, architecture_contracts, rust_tests_linux, cloudflare_mcp_contracts, host_component_release_windows, host_controller_launcher_checks, trading_engine_acceptance_windows, trading_engine_isolation_windows, trading_engine_regressions_windows, trading_engine_build_windows, research_ipc_windows, research_clippy_windows, research_service_tests_windows, research_component_release_windows]" in protected
    assert "check_result '${{ needs.research_ipc_windows.result }}' success research-ipc-windows" in protected
    assert protected.count("check_result '${{ needs.research_ipc_windows.result }}' skipped research-ipc-windows") == 4
    for owner in ("trading_engine_acceptance_windows", "trading_engine_isolation_windows",
                  "trading_engine_regressions_windows", "trading_engine_build_windows"):
        assert f"check_result '${{{{ needs.{owner}.result }}}}' success" in protected, f"CI_FINANCE_GATE_MISSING:{owner}"
        assert protected.count(f"check_result '${{{{ needs.{owner}.result }}}}' skipped") == 2, f"CI_SCOPE_EXCLUSION_MISSING:{owner}"
    isolation = workflow.split("\n  trading_engine_isolation_windows:\n", 1)[1].split("\n  trading_engine_regressions_windows:\n", 1)[0]
    finance = workflow.split("\n  trading_engine_acceptance_windows:\n", 1)[1].split("\n  trading_engine_isolation_windows:\n", 1)[0]
    assert "name: engine-isolation-windows" in isolation, "ENGINE_ISOLATION_OWNER_NAME_REQUIRED"
    assert "native_required != 'false'" in isolation, "FULL_ENGINE_ISOLATION_MUST_RUN"
    for witness in ("STANDALONE_RESEARCH_MCP_WITNESS_NOT_RUN",
                    "PROTECTION_CLEANUP_EXACT_SEVEN_TESTS_MISSING",
                    "PROCESS_TREE_NOT_MEASURED", "MCP_NATIVE_LOAD_RECEIPT_INVALID"):
        assert witness in isolation, f"ISOLATION_WITNESS_MISSING:{witness}"
    assert "Mandatory current Rust financial property acceptance" in finance, "FINANCE_COLLECTOR_MUST_REMAIN"
    assert "MCP DO WSS Native finance under isolated Research load" not in finance, "FINANCE_RUNNER_MUST_ISOLATE_CLOCKS"
    assert "['dependency-audit'].result }}\" success dependency-audit" in protected
    host_release = workflow.split("\n  host_component_release_windows:\n", 1)[1].split("\n  host_controller_launcher_checks:\n", 1)[0]
    host_contracts = workflow.split("\n  host_controller_launcher_checks:\n", 1)[1].split("\n  trading_engine_acceptance_windows:\n", 1)[0]
    for block in (host_release, host_contracts):
        for scope in ("host-controller", "host-launcher", "research-host"):
            assert f"needs.component_impact.outputs.component_scope == '{scope}'" in block, f"HOST_SCOPE_NOT_IN_BOTH:{scope}"
        assert "cargo build --locked --release -p okx-trading-engine" not in block, "HOST_MUST_NOT_RECOMPILE_ENGINE"
    assert "name: host-windows" in host_release, "HOST_RELEASE_OWNER_NAME_REQUIRED"
    assert "name: host-contracts-windows" in host_contracts, "HOST_CONTRACTS_OWNER_NAME_REQUIRED"
    assert "cargo build --locked --release -p okx-host-launcher -p okx-host-controller" in host_release
    assert "cargo build --locked --release" not in host_contracts, "HOST_TESTS_MUST_NOT_BUILD_RELEASE"
    assert "cargo test --locked" not in host_release and "cargo clippy --locked" not in host_release, "HOST_PRODUCER_ISOLATION_REQUIRED"
    assert "shared-key: trading-engine-release-windows" in host_release, "HOST_RELEASE_TRUSTED_CACHE_REQUIRED"
    assert "save-if: false" in host_release and "save-if: false" in host_contracts, "HOST_PR_CACHE_WRITE_FORBIDDEN"
    assert "installation_allowed = $false" in host_release, "HOST_COMPONENT_MUST_REMAIN_NONINSTALLABLE"
    for witness in (
        "one_process_job_can_be_terminated_without_touching_the_other",
        "nested_job_assignment_waits_for_parent_ready_receipt",
        "research_control_root_is_independent_and_start_requires_verified_installation",
        "corrupted_and_hash_mismatched_research_image_never_launch",
        "research_state_is_strict_and_independent_of_engine_state",
        "control_reconciles_research_when_engine_image_is_incompatible",
        "independent_engine_runtime_receipt_is_strict",
        "typed_research_rollback_requires_two_distinct_exact_hashes",
        "standalone_research_lifecycle_requires_exact_image_and_process_generation",
        "standalone_research_status_is_typed_and_does_not_claim_edge_online",
        "research_token_provision_command_has_no_bearer_fields",
        "control_bridge_denies_research_mutations_before_network",
    ):
        assert witness in host_contracts, f"HOST_CONTRACT_WITNESS_NOT_RUN:{witness}"
    for owner, display in (
        ("host_component_release_windows", "host-windows"),
        ("host_controller_launcher_checks", "host-contracts-windows"),
    ):
        result = f"needs.{owner}.result"
        assert protected.count(f"check_result '${{{{ {result} }}}}' success {display}") == 2, f"CI_HOST_SUCCESS_BRANCHES_MISSING:{owner}"
        assert protected.count(f"check_result '${{{{ {result} }}}}' skipped {display}") == 3, f"CI_HOST_SKIP_BRANCHES_MISSING:{owner}"
    research_ipc = workflow.split("\n  research_ipc_windows:\n", 1)[1].split("\n  research_clippy_windows:\n", 1)[0]
    research_clippy = workflow.split("\n  research_clippy_windows:\n", 1)[1].split("\n  research_service_tests_windows:\n", 1)[0]
    research = workflow.split("\n  research_service_tests_windows:\n", 1)[1].split("\n  research_component_release_windows:\n", 1)[0]
    research_release = workflow.split("\n  research_component_release_windows:\n", 1)[1].split("\n  required_ci:\n", 1)[0]
    assert "if: github.event_name == 'pull_request' && needs.component_impact.outputs.build_required == 'true' && (needs.component_impact.outputs.research_only == 'true' || needs.component_impact.outputs.component_scope == 'research-host')" in research
    assert "if: github.event_name == 'pull_request' && needs.component_impact.outputs.build_required == 'true' && needs.component_impact.outputs.research_only == 'true'" in research_ipc
    assert "RESEARCH_IPC_NOT_EXACT_PR_HEAD" in research_ipc
    assert "cargo test --locked -p okx-research-service --test ipc_process -- --nocapture --test-threads 1" in research_ipc
    assert "RESEARCH_IPC_ALL_TEN_TESTS_REQUIRED" in research_ipc
    assert "Research owner live loopback MCP DO WSS without Engine" in research_ipc
    assert "RESEARCH_ONLY_REAL_MCP_DO_WSS_WITNESS_NOT_EXECUTED" in research_ipc
    assert "if ('${{ needs.component_impact.outputs.component_scope }}' -eq 'research-host')" in research
    assert "RESEARCH_IPC_ALL_TEN_TESTS_REQUIRED" in research
    assert "if: needs.component_impact.outputs.component_scope == 'research-host'" in research
    assert "cargo build --locked --release -p okx-research-service --bin okx-research-service" in research_release
    assert "name: research-component-windows" in research_release
    assert "needs.component_impact.outputs.research_only == 'true'" in research_release
    assert "schema = 'okx.windows.research-component/v3'" in research_release
    assert "name: okx-research-component-${{ github.event.pull_request.head.sha }}" in research_release
    assert "check_result '${{ needs.research_component_release_windows.result }}' success research-component-windows" in protected
    assert "needs.research_component_release_windows.outputs.research_component_sha256" in protected
    for witness in (
        "research_token_validation_is_bounded_and_ascii_only",
        "research_native_target_is_distinct_from_engine",
        "mock_store_never_exposes_research_bearer_to_engine_target",
        "research_starts_only_after_finite_supervisor_job_receipt",
        "installed_research_bearer_is_pinned_to_control_edge_origin",
    ):
        assert witness in research, f"RESEARCH_WINDOWS_SECRET_WITNESS_REQUIRED:{witness}"
    assert "cargo fmt --all -- --check" in research, "SCOPED_RESEARCH_MUST_VERIFY_RUST_FORMATTING"
    # The separate Research-only Clippy and exact-name Ready-v3 owner tests,
    # plus component release, restore the same read-only Cargo cache into
    # their own isolated Windows target directories. Mixed Research+Host
    # retains Clippy and provenance on the single exact research-windows job.
    assert "Swatinem/rust-cache@6323deb102c322ba6fcbdcafc7e3dddab59af2b6" in research
    assert "Swatinem/rust-cache@6323deb102c322ba6fcbdcafc7e3dddab59af2b6" in research_clippy
    assert "Swatinem/rust-cache@6323deb102c322ba6fcbdcafc7e3dddab59af2b6" in research_release
    assert research.count("Join-Path $env:GITHUB_WORKSPACE 'target'") == 2
    assert "name: research-windows" in research
    assert "matrix.owner_check" not in research and "    strategy:" not in research
    assert "name: research-clippy-windows" in research_clippy
    assert "RESEARCH_CLIPPY_NOT_EXACT_PR_HEAD" in research_clippy
    assert "needs.component_impact.outputs.research_only == 'true'" in research_clippy
    assert "if: needs.component_impact.outputs.component_scope == 'research-host'" in research
    assert "check_result '${{ needs.research_clippy_windows.result }}' success research-clippy-windows" in protected
    assert "research_clippy_windows, research_service_tests_windows" in protected
    assert research_clippy.count("Join-Path $env:GITHUB_WORKSPACE 'target'") == 1
    assert research_release.count("Join-Path $env:GITHUB_WORKSPACE 'target'") == 1

    def files(*paths):
        return [[{"filename": path, "status": "modified"} for path in paths]]

    cases = {
        "docs": [files("docs/acceptance/518/t01-t15-evidence-matrix.md"), files("README.md", "docs/arch.md")],
        "cloudflare": [files("cloudflare/okx-mcp/src/runtime_session.ts"), files("cloudflare/okx-mcp/src/mcp.test.ts", "docs/okx.md")],
        "research": [files("crates/okx-research/src/strategy_registry.rs"), files("crates/okx-analysis/src/lib.rs"), files("crates/okx-research-service/src/analytics/market.rs", "cloudflare/okx-mcp/src/mcp.ts", "docs/a.md"), files("crates/okx-strategy-core/src/operators.rs"), files("crates/okx-strategy-core/src/time_series.rs"), files("crates/okx-strategy-core/src/query_plan.rs", "docs/a.md"), files("crates/okx-research-service/src/edge_runtime.rs"), files("crates/okx-research-service/tests/edge_wss.rs")],
        "engine": [files("crates/okx-trading-engine/src/query.rs"), files("crates/okx-trade-execution/src/risk/mod.rs", "README.md"), files("crates/okx-trading-engine/src/trade_service/strategies.rs", "cloudflare/okx-mcp/src/mcp.ts")],
        "host-controller": [files("crates/okx-host-controller/src/attestor.rs"), files("crates/okx-host-controller/tests/research_supervisor.rs"), files("crates/okx-host-protocol/tests/research_wire.rs"), files("crates/okx-host-controller/src/attestor.rs", "cloudflare/okx-mcp/src/mcp.ts"), files("crates/okx-host-controller/src/attestor.rs", "crates/okx-host-launcher/src/logging.rs", "docs/a.md")],
        "host-launcher": [files("crates/okx-host-launcher/src/logging.rs"), files("crates/okx-host-launcher/src/stdio_capture.rs"), files("crates/okx-host-launcher/tests/launcher_process.rs"), files("crates/okx-host-launcher/src/logging.rs", "README.md")],
        "research-host": [files("crates/okx-host-launcher/src/logging.rs", "crates/okx-research-service/src/analytics/market.rs"), files("crates/okx-host-controller/src/attestor.rs", "crates/okx-research-service/tests/edge_wss.rs"), files("crates/okx-host-controller/src/attestor.rs", "crates/okx-research-service/src/edge_runtime.rs"), files("crates/okx-host-protocol/tests/research_wire.rs", "crates/okx-research-service/src/edge_credentials.rs")],
    }
    for wanted, selections in cases.items():
        assert all(classify(pages) == wanted for pages in selections)
    host_and_edge = files("crates/okx-host-controller/src/attestor.rs", "cloudflare/okx-mcp/src/mcp.ts")
    assert classify(host_and_edge) == "host-controller"
    assert classify(files("crates/okx-host-protocol/tests/research_wire.rs", "cloudflare/okx-mcp/src/mcp.ts")) == "host-controller"
    assert edge_changed(host_and_edge)
    attestor_only = files(_EDGE_WIRE_SHARED_HOST_SOURCE, "README.md")
    assert classify(attestor_only) == "host-controller"
    assert edge_changed(attestor_only)
    attestor_plan = github_plan(attestor_only, allow_dependency_candidate=True)
    assert attestor_plan["scope"] == "host-controller"
    assert attestor_plan["edge_changed"] == "true"
    assert attestor_plan["manifest_candidate"] == "none"
    assert attestor_plan["draft_checks"] == "host,mcp"
    assert attestor_plan["draft_mcp"] == "true"
    assert github_plan(files("crates/okx-host-controller/tests/research_supervisor.rs"),
                       allow_dependency_candidate=False)["draft_checks"] == "host"
    assert not edge_changed(files("crates/okx-host-controller/tests/research_supervisor.rs", "README.md"))
    assert not edge_changed(files("crates/okx-host-launcher/src/logging.rs", "README.md"))
    assert not edge_changed(files("crates/okx-trading-engine/src/query.rs", "README.md"))
    # No broad `src/*.rs` Host skip: a CONTROL or Launcher implementation
    # can deploy/restart Engine or affect its desired/installed state without
    # depending on the trading-engine Cargo crate. The classifier must fail
    # closed for all files not explicitly isolated above, including new ones.
    for lifecycle in (
        "crates/okx-host-controller/src/job.rs",
        "crates/okx-host-controller/src/lifecycle_executor.rs",
        "crates/okx-host-controller/src/runtime.rs",
        "crates/okx-host-controller/src/artifact.rs",
        "crates/okx-host-controller/src/desired.rs",
        "crates/okx-host-controller/src/autostart.rs",
        "crates/okx-host-controller/src/controller_update.rs",
        "crates/okx-host-controller/src/legacy_bootstrap.rs",
        "crates/okx-host-controller/src/root_migration.rs",
        "crates/okx-host-controller/src/main.rs",
        "crates/okx-host-controller/src/future_new_engine_lifecycle.rs",
        "crates/okx-host-controller/src/research_supervisor.rs",
        "crates/okx-host-protocol/src/lib.rs",
        "crates/okx-host-protocol/src/research.rs",
        "crates/okx-host-launcher/src/main.rs",
        "crates/okx-host-launcher/src/lib.rs",
        "crates/okx-host-launcher/src/future_new_launcher.rs",
    ):
        assert classify(files(lifecycle)) == "full", f"HOST_ENGINE_LIFECYCLE_SKIP:{lifecycle}"
        assert classify(files(lifecycle, "cloudflare/okx-mcp/src/mcp.ts")) == "full", (
            f"HOST_ENGINE_LIFECYCLE_EDGE_SKIP:{lifecycle}"
        )
        assert classify(files(lifecycle, "crates/okx-research-service/src/edge_runtime.rs")) == "full", (
            f"HOST_ENGINE_LIFECYCLE_RESEARCH_SKIP:{lifecycle}"
        )
    unsafe = [
        [], [[]], [[{}]],
        files("crates/okx-trading-engine/src/query.rs", "crates/okx-research-protocol/src/analytics.rs"),
        files("crates/okx-research-protocol/src/analytics.rs"),
        files("crates/okx-research-domain/src/numeric.rs"),
        files("crates/okx-trade-execution/src/risk/mod.rs", "crates/okx-host-controller/src/research_supervisor.rs"),
        files("crates/okx-research-service/src/process_memory.rs"),
        files("crates/okx-research-service/src/main.rs"),
        files("crates/okx-research-service/src/process_memory.rs", "crates/okx-host-controller/src/research_supervisor.rs"),
        files("crates/okx-host-controller/src/job.rs"),
        files("crates/okx-host-controller/src/lifecycle_executor.rs"),
        files("crates/okx-host-controller/src/lifecycle_executor.rs", "crates/okx-research-service/src/edge_runtime.rs"),
        files("crates/okx-ipc-contracts/src/lib.rs"),
        files("crates/okx-host-controller/Cargo.toml"),
        files("crates/okx-host-protocol/Cargo.toml"),
        files("crates/okx-host-launcher/Cargo.toml"),
        files("crates/okx-host-controller/build.rs"),
        files("crates/okx-host-controller/assets/config.json"),
        files("crates/okx-host-controller/src/research_supervisor.json"),
        files("crates/okx-host-controller/tests/research_supervisor.json"),
        files("crates/okx-host-protocol/tests/research_wire_fixture.json"),
        files("crates/okx-host-controller/src/research_supervisor.rs", "crates/okx-ipc-contracts/src/lib.rs"),
        files("crates/okx-host-controller/src/migration.json"),
        files("crates/okx-host-launcher/src/main.rs", "crates/okx-trading-engine/src/main.rs"),
        files("crates/okx-host-controller/src/main.rs", "crates/okx-ipc-contracts/src/lib.rs"),
        files("Cargo.toml"), files("Cargo.lock"),
        files("crates/okx-research-service/Cargo.toml"),
        files("crates/okx-strategy-core/Cargo.toml"),
        files("crates/okx-research-service/build.rs"),
        files("crates/okx-research-service/config/edge_wss.json"),
        files("crates/okx-research-service/src/embedded_config.json"),
        files("crates/okx-research-service/src/edge_runtime.rs", "crates/okx-ipc-contracts/src/lib.rs"),
        files("crates/okx-analysis/data/series.parquet"),
        files("crates/okx-strategy-core/README.md"),
        files("crates/okx-research-service/tests/edge_wss.rs", "crates/okx-host-controller/Cargo.toml"),
        files(".github/workflows/ci.yml"),
        files("scripts/prepare-research-release.ps1"),
        files("scripts/check_ci_native_scope.py"),
        files("docs/a.yml"), files("crates/okx-trading-engine/src/readme.md"),
        files("docs/../crates/okx-trading-engine/README.md"), files("docs//a.md"),
        files("cloudflare/okx-mcp/../okx-trading-engine/src/a.ts"),
        files("docs/a.md", "crates/okx-trading-engine/build.rs"),
        files("cloudflare/okx-mcp/src/mcp.ts", ".github/workflows/ci.yml"),
        [[{"filename": "docs/a.md", "status": "renamed", "previous_filename": "crates/okx-trading-engine/a.md"}]],
        [[{"filename": "docs/a.md", "status": "modified"}] * 2],
        [[{"filename": "docs/a.md", "status": "modified"}] * 501],
        [{"filename": "docs/a.md", "status": "modified"}],
        [[{"filename": "docs/a.md", "status": "modified"}] * 101],
        [[{"filename": [], "status": "modified"}]],
    ]
    assert all(classify(pages) == "full" for pages in unsafe)
    for wanted, selection in (
        ("research", files("crates/okx-research-service/Cargo.toml", "Cargo.lock")),
        ("research", files("crates/okx-research-domain/Cargo.toml", "Cargo.lock")),
        ("research", files("crates/okx-research-protocol/Cargo.toml", "Cargo.lock")),
        ("research", files("crates/okx-research-protocol/src/analytics.rs")),
        ("research", files("crates/okx-research-domain/src/numeric.rs")),
        ("research", files("crates/okx-research-domain/src/numeric.rs", "crates/okx-research-service/src/analytics/market.rs")),
        ("research", files("crates/okx-analysis/Cargo.toml", "Cargo.lock")),
        ("research", files("crates/okx-strategy-core/Cargo.toml", "crates/okx-research/src/lib.rs")),
        ("host-controller", files("crates/okx-host-controller/Cargo.toml", "Cargo.lock")),
        ("host-controller", files("crates/okx-host-controller/Cargo.toml", "crates/okx-host-launcher/Cargo.toml")),
        ("host-launcher", files("crates/okx-host-launcher/Cargo.toml", "Cargo.lock")),
        ("host-launcher", files("crates/okx-host-launcher/Cargo.toml", "cloudflare/okx-mcp/src/mcp.ts")),
    ):
        assert classify(selection) == "full"  # never grant scope before Cargo proof
        assert dependency_candidate(selection) == wanted
    # A Research wire/domain source edit is *not* statically owner-only while
    # Engine can still consume that crate through a dev/test dependency. The
    # GitHub plan may request a Windows Cargo closure proof, but its initial
    # scope must remain full until that independent proof succeeds.
    for protected in (
        "crates/okx-research-protocol/src/analytics.rs",
        "crates/okx-research-domain/src/numeric.rs",
    ):
        modified = files(protected)
        assert classify(modified) == "full"
        scoped = github_plan(modified, allow_dependency_candidate=True)
        assert scoped["scope"] == "full"
        assert scoped["manifest_candidate"] == "research"
        assert scoped["diff_verified"] == "true"
        assert github_plan(modified, allow_dependency_candidate=False)["manifest_candidate"] == "none"
        with_edge = files(protected, "cloudflare/okx-mcp/src/runtime_session.ts")
        assert github_plan(with_edge, allow_dependency_candidate=True)["edge_changed"] == "true"
        assert github_plan(with_edge, allow_dependency_candidate=True)["manifest_candidate"] == "research"
        for status in ("added", "removed", "renamed"):
            mutation = [[{"filename": protected, "status": status}]]
            if status == "renamed":
                mutation[0][0]["previous_filename"] = "crates/okx-trading-engine/src/research_client.rs"
            assert classify(mutation) == "full"
            assert dependency_candidate(mutation) == "none"
        # A forged modified status with retained rename provenance is also
        # unverified, as is an apparent add+remove crossing the owner boundary.
        forged = [[{"filename": protected, "status": "modified",
                    "previous_filename": "crates/okx-trading-engine/src/research_client.rs"}]]
        assert classify(forged) == "full"
        assert dependency_candidate(forged) == "none"
        cross_owner = [[{"filename": protected, "status": "added"},
                        {"filename": "crates/okx-trading-engine/src/research_client.rs",
                         "status": "removed"}]]
        assert classify(cross_owner) == "full"
        assert dependency_candidate(cross_owner) == "none"
    for selection in (
        files("Cargo.lock"), files("Cargo.toml", "crates/okx-research-service/Cargo.toml"),
        files("crates/okx-ipc-contracts/Cargo.toml", "Cargo.lock"),
        files("crates/okx-host-launcher/Cargo.toml", "crates/okx-research-service/Cargo.toml"),
        files("crates/okx-research-service/Cargo.toml", ".github/workflows/ci.yml"),
        files("crates/okx-research-service/Cargo.toml", "crates/okx-research-service/config/edge_wss.json"),
        [[{"filename": "crates/okx-research-service/Cargo.toml", "status": "removed"}]],
        [[{"filename": "crates/okx-research-service/Cargo.toml", "status": "renamed", "previous_filename": "x"}]],
    ):
        assert dependency_candidate(selection) == "none"
    assert classify([[{"filename": "docs/obsolete.md", "status": "removed"}]]) == "docs"
    print("CI_COMPONENT_SCOPE_NEGATIVE_CONTROLS=PASS")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    output = parser.add_mutually_exclusive_group()
    output.add_argument("--dependency-candidate", action="store_true")
    output.add_argument("--edge-changed", action="store_true")
    output.add_argument("--github-plan", action="store_true")
    parser.add_argument("--allow-dependency-candidate", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        _self_test()
        return 0
    try:
        pages = json.load(sys.stdin)
    except (ValueError, UnicodeError):
        if not args.github_plan:
            return 1
        pages = None
    if args.github_plan:
        plan = github_plan(pages, allow_dependency_candidate=args.allow_dependency_candidate)
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output_file:
            for name, value in plan.items():
                print(f"{name}={value}", file=output_file)
    elif args.edge_changed:
        print("true" if edge_changed(pages) else "false")
    else:
        print(dependency_candidate(pages) if args.dependency_candidate else classify(pages))
    return 0


if __name__ == "__main__":
    sys.exit(main())

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
from pathlib import Path
import re
import sys


_PART = re.compile(r"^[A-Za-z0-9_.-]+$")
# The core is linked only by Research/Analysis; the architecture DAG gate
# rejects any transitive Native Agent dependency before this fast path is used.
_RESEARCH_DIRS = ("crates/okx-research/", "crates/okx-research-service/", "crates/okx-strategy-core/", "crates/okx-analysis/")
_CLOUDFLARE_DIR = "cloudflare/okx-mcp/"
_HOST_CONTROL_SRC = "crates/okx-host-controller/src/"
_HOST_LAUNCHER_SRC = "crates/okx-host-launcher/src/"
_HOST_PROTOCOL_SRC = "crates/okx-host-protocol/src/"
_RESEARCH_MANIFESTS = {
    "crates/okx-research/Cargo.toml",
    "crates/okx-analysis/Cargo.toml",
    "crates/okx-research-service/Cargo.toml",
    "crates/okx-strategy-core/Cargo.toml",
}
_HOST_MANIFESTS = {
    "crates/okx-host-controller/Cargo.toml",
    "crates/okx-host-launcher/Cargo.toml",
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
    # Only Rust implementation files may use the host-only gate. Changes to
    # manifests, assets, scripts, protocols or packaging still require full CI.
    if path.startswith((_HOST_CONTROL_SRC, _HOST_PROTOCOL_SRC)) and path.endswith(".rs"):
        return "host-controller"
    if path.startswith(_HOST_LAUNCHER_SRC) and path.endswith(".rs"):
        return "host-launcher"
    # A scoped Research change must be Rust source or an integration test.
    # Arbitrary configs, SQL fixtures, generated code and build inputs can
    # affect the installed product; they must not bypass the Native gate.
    if _research_source(path):
        return "research"
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
    categories = set()
    for item in files:
        path = item["filename"]
        kind = category(path)
        if kind == "full":
            return "full"
        categories.add(kind)
    if "research" in categories:
        if categories & {"host-controller", "host-launcher"}:
            if any(category(path) == "research" and not _research_source(path) for path in
                   (entry["filename"] for entry in files)):
                return "full"
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


def edge_changed(pages):
    files = verified_entries(pages)
    return files is not None and any(category(entry["filename"]) == "cloudflare" for entry in files)


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
    if not manifests or any(entry["status"] != "modified" for entry in files
                            if entry["filename"] in _RESEARCH_MANIFESTS | _HOST_MANIFESTS | {"Cargo.lock"}):
        return "none"
    kinds = set()
    for entry in files:
        path = entry["filename"]
        if path == "Cargo.lock":
            continue
        if path in _RESEARCH_MANIFESTS or category(path) == "research":
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
    workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml").read_text()
    # The Actions shell guard must accept exactly the names produced by this
    # classifier. A stale host-control spelling once rejected host-controller
    # source changes before their dedicated short Windows job could start.
    expected_scopes = {"docs", "cloudflare", "research", "research-host", "host-controller", "host-launcher", "full"}
    scope_guards = re.findall(r'case "\$scope" in ([a-z|-]+)\)', workflow)
    assert len(scope_guards) == 2, "CI_SCOPE_GUARD_MISSING_OR_AMBIGUOUS"
    for entry in scope_guards:  # raw file scope and final Cargo-proven scope
        guard_scopes = entry.split("|")
        assert len(guard_scopes) == len(set(guard_scopes))
        assert set(guard_scopes) == expected_scopes, "CI_SCOPE_GUARD_DRIFT"
    assert "component_scope: ${{ steps.proven_scope.outputs.scope }}" in workflow
    assert 'research_only: ${{ steps.classify.outputs.research_only }}' in workflow
    assert 'scope="${{ steps.proven_scope.outputs.scope }}"' in workflow
    assert "--filter-platform x86_64-pc-windows-msvc" in workflow
    windows = workflow.split("\n  required_windows:\n", 1)[1].split("\n  trading_engine_build_windows:\n", 1)[0]
    assert "needs: [component_impact, trading_engine_acceptance_windows, trading_engine_regressions_windows, trading_engine_build_windows," in windows
    assert 'test "${{ needs.trading_engine_acceptance_windows.result }}" = "success"' in windows
    assert 'test "${{ needs.trading_engine_regressions_windows.result }}" = "success"' in windows
    assert windows.count('test "${{ needs.trading_engine_regressions_windows.result }}" = "skipped"') == 5
    assert 'test "${{ needs.trading_engine_build_windows.result }}" = "success"' in windows
    host = workflow.split("\n  host_controller_launcher_checks:\n", 1)[1].split("\n  trading_engine_acceptance_windows:\n", 1)[0]
    assert "needs.component_impact.outputs.component_scope == 'host-controller'" in host
    assert "needs.component_impact.outputs.component_scope == 'host-launcher'" in host
    assert "cargo build --locked --release -p okx-host-launcher -p okx-host-controller" in host
    assert "cargo build --locked --release -p okx-trading-engine" not in host
    research = workflow.split("\n  research_service_tests_windows:\n", 1)[1].split("\n  required_windows:\n", 1)[0]
    assert "if: needs.component_impact.outputs.build_required == 'true' && (needs.component_impact.outputs.research_only == 'true' || needs.component_impact.outputs.component_scope == 'research-host')" in research
    assert "cargo build --locked --release -p okx-research-service --bin okx-research-service" in research
    assert "cargo fmt --all -- --check" in research, "SCOPED_RESEARCH_MUST_VERIFY_RUST_FORMATTING"
    # Reuse the same established Cargo cache action and one target directory.
    assert "Swatinem/rust-cache@6323deb102c322ba6fcbdcafc7e3dddab59af2b6" in research
    assert research.count("Join-Path $env:GITHUB_WORKSPACE 'target'") == 2

    def files(*paths):
        return [[{"filename": path, "status": "modified"} for path in paths]]

    cases = {
        "docs": [files("docs/acceptance/518/t01-t15-evidence-matrix.md"), files("README.md", "docs/arch.md")],
        "cloudflare": [files("cloudflare/okx-mcp/src/runtime_session.ts"), files("cloudflare/okx-mcp/src/mcp.test.ts", "docs/okx.md")],
        "research": [files("crates/okx-research/src/strategy_registry.rs"), files("crates/okx-analysis/src/lib.rs"), files("crates/okx-research-service/src/analytics/market.rs", "cloudflare/okx-mcp/src/mcp.ts", "docs/a.md"), files("crates/okx-strategy-core/src/operators.rs"), files("crates/okx-strategy-core/src/time_series.rs"), files("crates/okx-strategy-core/src/query_plan.rs", "docs/a.md"), files("crates/okx-research-service/src/edge_runtime.rs"), files("crates/okx-research-service/tests/edge_wss.rs")],
        "host-controller": [files("crates/okx-host-controller/src/main.rs"), files("crates/okx-host-protocol/src/lib.rs"), files("crates/okx-host-controller/src/activation/tests.rs", "crates/okx-host-launcher/src/main.rs", "docs/a.md")],
        "host-launcher": [files("crates/okx-host-launcher/src/main.rs"), files("crates/okx-host-launcher/src/verify.rs", "README.md")],
        "research-host": [files("crates/okx-host-launcher/src/main.rs", "crates/okx-research-service/src/analytics/market.rs"), files("crates/okx-host-controller/src/lifecycle.rs", "crates/okx-research-service/tests/edge_wss.rs")],
    }
    for wanted, selections in cases.items():
        assert all(classify(pages) == wanted for pages in selections)
    host_and_edge = files("crates/okx-host-controller/src/main.rs", "cloudflare/okx-mcp/src/mcp.ts")
    assert classify(host_and_edge) == "host-controller"
    assert classify(files("crates/okx-host-protocol/src/lib.rs", "cloudflare/okx-mcp/src/mcp.ts")) == "host-controller"
    assert edge_changed(host_and_edge)
    assert not edge_changed(files("crates/okx-host-controller/src/main.rs", "README.md"))
    unsafe = [
        [], [[]], [[{}]],
        files("crates/okx-research-domain/src/lib.rs"),
        files("crates/okx-ipc-contracts/src/lib.rs"),
        files("crates/okx-trading-engine/src/trade_service/execute.rs"),
        files("crates/okx-trade-execution/src/lib.rs"),
        files("crates/okx-host-controller/Cargo.toml"),
        files("crates/okx-host-protocol/Cargo.toml"),
        files("crates/okx-host-launcher/Cargo.toml"),
        files("crates/okx-host-controller/build.rs"),
        files("crates/okx-host-controller/assets/config.json"),
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
        files("docs/a.md", "crates/okx-research-domain/src/lib.rs"),
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
        ("research", files("crates/okx-analysis/Cargo.toml", "Cargo.lock")),
        ("research", files("crates/okx-strategy-core/Cargo.toml", "crates/okx-research/src/lib.rs")),
        ("host-controller", files("crates/okx-host-controller/Cargo.toml", "Cargo.lock")),
        ("host-controller", files("crates/okx-host-controller/Cargo.toml", "crates/okx-host-launcher/Cargo.toml")),
        ("host-launcher", files("crates/okx-host-launcher/Cargo.toml", "Cargo.lock")),
        ("host-launcher", files("crates/okx-host-launcher/Cargo.toml", "cloudflare/okx-mcp/src/mcp.ts")),
    ):
        assert classify(selection) == "full"  # never grant scope before Cargo proof
        assert dependency_candidate(selection) == wanted
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
    args = parser.parse_args()
    if args.self_test:
        _self_test()
        return 0
    try:
        pages = json.load(sys.stdin)
    except (ValueError, UnicodeError):
        return 1
    if args.edge_changed:
        print("true" if edge_changed(pages) else "false")
    else:
        print(dependency_candidate(pages) if args.dependency_candidate else classify(pages))
    return 0


if __name__ == "__main__":
    sys.exit(main())

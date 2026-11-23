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
_RESEARCH_DIRS = ("crates/okx-research/", "crates/okx-research-service/", "crates/okx-strategy-core/")
_CLOUDFLARE_DIR = "cloudflare/okx-mcp/"
_HOST_CONTROL_SRC = "crates/okx-host-controller/src/"
_HOST_LAUNCHER_SRC = "crates/okx-host-launcher/src/"


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
    if path.startswith(_HOST_CONTROL_SRC) and path.endswith(".rs"):
        return "host-controller"
    if path.startswith(_HOST_LAUNCHER_SRC) and path.endswith(".rs"):
        return "host-launcher"
    if path.startswith(_RESEARCH_DIRS):
        if path.endswith("/Cargo.toml") or path.endswith("/build.rs"):
            return "full"
        return "research"
    return "full"


def classify(pages):
    """Classify complete GH paginated changed-file pages; fail closed."""
    if not isinstance(pages, list) or not 1 <= len(pages) <= 30:
        return "full"
    files = []
    for page in pages:
        if not isinstance(page, list) or len(page) > 100:
            return "full"
        files.extend(page)
    if not 1 <= len(files) <= 500:
        return "full"
    paths = set()
    categories = set()
    for item in files:
        if not isinstance(item, dict):
            return "full"
        path = item.get("filename")
        if (
            not isinstance(path, str)
            or
            item.get("status") not in {"added", "modified", "removed"}
            or "previous_filename" in item
            or path in paths
        ):
            return "full"
        paths.add(path)
        kind = category(path)
        if kind == "full":
            return "full"
        categories.add(kind)
    if "research" in categories:
        if categories & {"host-controller", "host-launcher"}:
            return "full"
        return "research"
    if categories & {"host-controller", "host-launcher"}:
        if "cloudflare" in categories:
            return "full"
        # CONTROL consumes Launcher: either change needs both owner and
        # consumer checks; the distinct scope remains visible in CI evidence.
        return "host-controller" if "host-controller" in categories else "host-launcher"
    if "cloudflare" in categories:
        return "cloudflare"
    return "docs"


def _self_test() -> None:
    # Post-merge reuse must skip Research compilation, not just full Native.
    workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml").read_text()
    research = workflow.split("\n  research_service_windows:\n", 1)[1].split("\n  required_windows:\n", 1)[0]
    assert "if: needs.component_impact.outputs.build_required == 'true' && needs.component_impact.outputs.research_only == 'true'" in research
    # Reuse the same established Cargo cache action and one target directory.
    assert "Swatinem/rust-cache@6323deb102c322ba6fcbdcafc7e3dddab59af2b6" in research
    assert research.count("Join-Path $env:GITHUB_WORKSPACE 'target'") == 2

    def files(*paths):
        return [[{"filename": path, "status": "modified"} for path in paths]]

    cases = {
        "docs": [files("docs/acceptance/518/t01-t15-evidence-matrix.md"), files("README.md", "docs/arch.md")],
        "cloudflare": [files("cloudflare/okx-mcp/src/runtime_session.ts"), files("cloudflare/okx-mcp/src/mcp.test.ts", "docs/okx.md")],
        "research": [files("crates/okx-research/src/strategy_registry.rs"), files("crates/okx-research-service/src/analytics/market.rs", "cloudflare/okx-mcp/src/mcp.ts", "docs/a.md"), files("crates/okx-strategy-core/src/operators.rs"), files("crates/okx-strategy-core/src/query_plan.rs", "docs/a.md")],
        "host-controller": [files("crates/okx-host-controller/src/main.rs"), files("crates/okx-host-controller/src/activation/tests.rs", "crates/okx-host-launcher/src/main.rs", "docs/a.md")],
        "host-launcher": [files("crates/okx-host-launcher/src/main.rs"), files("crates/okx-host-launcher/src/verify.rs", "README.md")],
    }
    for wanted, selections in cases.items():
        assert all(classify(pages) == wanted for pages in selections)
    unsafe = [
        [], [[]], [[{}]],
        files("crates/okx-domain-core/src/query_plan.rs"),
        files("crates/okx-ipc-contracts/src/lib.rs"),
        files("crates/okx-trading-engine/src/trade_service/execute.rs"),
        files("crates/okx-trade-execution/src/lib.rs"),
        files("crates/okx-host-controller/Cargo.toml"),
        files("crates/okx-host-launcher/Cargo.toml"),
        files("crates/okx-host-controller/build.rs"),
        files("crates/okx-host-controller/assets/config.json"),
        files("crates/okx-host-controller/src/migration.json"),
        files("crates/okx-host-launcher/src/main.rs", "crates/okx-trading-engine/src/main.rs"),
        files("crates/okx-host-controller/src/main.rs", "crates/okx-ipc-contracts/src/lib.rs"),
        files("crates/okx-host-controller/src/main.rs", "cloudflare/okx-mcp/src/mcp.ts"),
        files("crates/okx-host-launcher/src/main.rs", "crates/okx-research-service/src/analytics/market.rs"),
        files("Cargo.toml"), files("Cargo.lock"),
        files("crates/okx-research-service/Cargo.toml"),
        files("crates/okx-strategy-core/Cargo.toml"),
        files("crates/okx-research-service/build.rs"),
        files(".github/workflows/ci.yml"),
        files("scripts/prepare-research-release.ps1"),
        files("scripts/check_ci_native_scope.py"),
        files("docs/a.yml"), files("crates/okx-trading-engine/src/readme.md"),
        files("docs/../crates/okx-trading-engine/README.md"), files("docs//a.md"),
        files("cloudflare/okx-mcp/../okx-trading-engine/src/a.ts"),
        files("docs/a.md", "crates/okx-domain-core/src/query_plan.rs"),
        files("cloudflare/okx-mcp/src/mcp.ts", ".github/workflows/ci.yml"),
        [[{"filename": "docs/a.md", "status": "renamed", "previous_filename": "crates/okx-trading-engine/a.md"}]],
        [[{"filename": "docs/a.md", "status": "modified"}] * 2],
        [[{"filename": "docs/a.md", "status": "modified"}] * 501],
        [{"filename": "docs/a.md", "status": "modified"}],
        [[{"filename": "docs/a.md", "status": "modified"}] * 101],
        [[{"filename": [], "status": "modified"}]],
    ]
    assert all(classify(pages) == "full" for pages in unsafe)
    assert classify([[{"filename": "docs/obsolete.md", "status": "removed"}]]) == "docs"
    print("CI_COMPONENT_SCOPE_NEGATIVE_CONTROLS=PASS")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        _self_test()
        return 0
    try:
        pages = json.load(sys.stdin)
    except (ValueError, UnicodeError):
        return 1
    print(classify(pages))
    return 0


if __name__ == "__main__":
    sys.exit(main())

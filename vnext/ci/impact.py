#!/usr/bin/env python3
"""Fail-closed owner impact plan for the single vNext workflow.

Only the four deployable owners are build units. ``boundaries`` is always
checked, and cross-owner contracts expand to their known consumers. This
planner deliberately does not invent a finance skip based on a file extension
or a previous successful run. A future artifact-reuse feature must prove an
identical owner tree and applicable finance witness before omitting a build.

PRs use merge-base(base, branch-head)..branch-head (the *entire* PR change);
pushes use before..after. Comparing only the last PR commit is unsafe.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
from typing import Iterable


OWNERS = ("engine", "research", "control", "edge")
OWNER_ROOTS = tuple(f"vnext/{owner}/" for owner in OWNERS)
SHA = re.compile(r"[0-9a-fA-F]{40}(?:[0-9a-fA-F]{24})?\Z")

# Public producer/consumer boundaries whose changes require more than their
# owning application's isolated compilation. Keep this list precise: broad
# Rust/TypeScript *internal* edits must not rebuild an unrelated owner.
CONTRACTS: dict[str, tuple[str, ...]] = {
    "vnext/edge/contracts/trade-semantic-golden.json": ("engine", "edge"),
    "vnext/edge/contracts/research-operator-baseline.json": ("research", "edge"),
    "vnext/edge/contracts/tools.schema.json": OWNERS,
    "vnext/edge/src/shared.ts": OWNERS,
    "vnext/engine/crates/okx-trading-engine/src/lib.rs": (
        "engine", "research", "edge"
    ),
    "vnext/control/crates/okx-host-controller/src/model.rs": (
        "control", "edge"
    ),
}

# These are repository-owned, non-deployable input changes. They still run
# mandatory ownership/symbol/S0 checks, but no Rust/Worker owner build.
BOUNDARY_ONLY_EXACT = {
    "vnext/README.md",
    "vnext/BOUNDARIES.md",
    "vnext/legacy-files.tsv",
    "vnext/split-symbols.tsv",
    "vnext/check_boundaries.py",
    "vnext/test_check_boundaries.py",
    "vnext/check_split_symbols.py",
    "vnext/test_check_split_symbols.py",
}
BOUNDARY_ONLY_PREFIXES = ("vnext/s0/",)
OWNER_PRIVATE_CONTRACTS = {
    # Edge-owned test data, not a producer-consumer public schema.
    "vnext/edge/contracts/action-fixtures.json",
}


def _run_git(repo: Path, args: list[str]) -> str:
    try:
        command = subprocess.run(
            ["git", *args], cwd=repo, capture_output=True, check=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError(f"cannot prove git diff: {' '.join(args[:2])}") from exc
    return command.stdout.decode("utf-8", errors="surrogateescape").strip()


def changed_paths(repo: Path, *, base: str, head: str, mode: str) -> tuple[list[str], str]:
    """Return all changed paths; fail rather than silently assume no impact."""
    if mode not in {"pr", "push"}:
        raise ValueError("mode must be pr or push")
    if not SHA.fullmatch(base) or not SHA.fullmatch(head):
        raise ValueError("base/head must be concrete full Git object IDs")
    for commit in (base, head):
        if len(commit) == 40 and commit == "0" * 40:
            raise ValueError("zero before-SHA cannot prove affected owners")
        _run_git(repo, ["rev-parse", "--verify", f"{commit}^{{commit}}"])
    start = (
        _run_git(repo, ["merge-base", base, head]) if mode == "pr" else base
    )
    # No rename detection means a move appears as both a deletion and an
    # addition. That is the safe owner-coverage interpretation.
    try:
        result = subprocess.run(
            ["git", "diff", "--no-renames", "--name-only", "-z", start, head],
            cwd=repo, capture_output=True, check=True, timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError("cannot enumerate changed Git paths") from exc
    paths = [os.fsdecode(raw) for raw in result.stdout.split(b"\0") if raw]
    return sorted(set(paths)), start


def plan_for_paths(paths: Iterable[str]) -> dict[str, object]:
    owners: set[str] = set()
    evidence: dict[str, str] = {}
    unknown: list[str] = []
    finance = False
    contracts = False
    for path in sorted(set(paths)):
        # Normalized Git paths never contain traversal; reject unusual inputs
        # to avoid letting the planner interpret a foreign source as docs.
        posix = PurePosixPath(path)
        if not path or posix.is_absolute() or ".." in posix.parts or "\\" in path:
            unknown.append(path)
            evidence[path] = "invalid changed path"
            continue
        if path in CONTRACTS:
            affected = CONTRACTS[path]
            owners.update(affected)
            contracts = True
            finance |= "engine" in affected
            evidence[path] = "contract: " + ",".join(affected)
            continue
        if path.startswith(".github/workflows/"):
            # Changing an owner acceptance workflow affects the proof that
            # owner remained safe. A unified workflow can affect all four.
            owners.update(OWNERS)
            finance = True
            evidence[path] = "workflow validation: all owners"
            continue
        if path.startswith("vnext/ci/"):
            owners.update(OWNERS)
            finance = True
            evidence[path] = "impact planner: all owners"
            continue
        if path.startswith("vnext/integration/"):
            owners.update(OWNERS)
            finance = True
            contracts = True
            evidence[path] = "integration: all consumers"
            continue
        if path in BOUNDARY_ONLY_EXACT or path.startswith(BOUNDARY_ONLY_PREFIXES):
            evidence[path] = "repository boundary/S0 only"
            continue
        root = next((owner for owner in OWNERS if path.startswith(f"vnext/{owner}/")), None)
        if root is not None:
            if path == f"vnext/{root}/README.md":
                evidence[path] = f"{root} documentation only"
                continue
            if (
                (path.startswith(f"vnext/{root}/contracts/") or "/contracts/" in path)
                and path not in OWNER_PRIVATE_CONTRACTS
            ):
                unknown.append(path)
                evidence[path] = "unclassified cross-owner contract: fail closed"
                continue
            owners.add(root)
            finance |= root == "engine"
            evidence[path] = f"{root} implementation/configuration"
            continue
        unknown.append(path)
        evidence[path] = "unclassified: fail closed"

    if unknown:
        owners.update(OWNERS)
        finance = True
        contracts = True

    # An empty diff cannot satisfy a claimed application change. It runs only
    # boundary checks, never falsely reports a changed owner as tested.
    booleans = {owner: owner in owners for owner in OWNERS}
    return {
        "schema": "okx-vnext-impact/v1",
        "changed_paths": sorted(evidence),
        "owners": booleans,
        "boundaries": True,
        "finance": finance,
        "contracts": contracts,
        "planner_ok": not unknown,
        "unknown_paths": unknown,
        "reasons": evidence,
        "artifact_reuse_authorized": False,
    }


def write_plan(report: dict[str, object], *, github_output: Path, json_out: Path) -> None:
    json_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    outputs = dict(report["owners"])
    for key in ("boundaries", "finance", "contracts", "planner_ok"):
        outputs[key] = report[key]
    github_output.parent.mkdir(parents=True, exist_ok=True)
    with github_output.open("a", encoding="utf-8") as handle:
        for name, value in outputs.items():
            handle.write(f"{name}={'true' if value else 'false'}\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--mode", choices=("pr", "push"), required=True)
    parser.add_argument("--repository", type=Path, default=Path("."))
    parser.add_argument("--github-output", type=Path, required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        paths, actual_base = changed_paths(
            args.repository.resolve(strict=True), base=args.base,
            head=args.head, mode=args.mode,
        )
        report = plan_for_paths(paths)
        report.update({
            "mode": args.mode,
            "base_sha": args.base,
            "comparison_base_sha": actual_base,
            "head_sha": args.head,
        })
        write_plan(report, github_output=args.github_output, json_out=args.json_out)
    except (OSError, ValueError) as exc:
        print(f"vNext impact REFUSED: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, sort_keys=True))
    # All owners still run on an unclassified diff, but the required final gate
    # must reject `planner_ok=false`; no green 'skip' is allowed.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

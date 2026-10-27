#!/usr/bin/env python3
"""Small executable guard for the accepted local-crate dependency DAG."""

import argparse
import json
import subprocess
import sys

ALLOWED = {
    "okx-api": set(),
    "okx-ws": {"okx-api"},
    "okx-observation": {"okx-api"},
    "okx-runtime": {"okx-api", "okx-observation", "okx-ws"},
    "okx-analysis": {"okx-observation"},
    "okx-research": {"okx-analysis", "okx-observation"},
    "okx-execution": {"okx-api", "okx-analysis", "okx-observation"},
    "okx-protocol": set(),
    "okx-github": set(),
    "okx-cli": {"okx-api"},
    "okx-host-launcher": set(),
    "okx-host-control": {"okx-github", "okx-host-launcher", "okx-protocol"},
    "okx-bridge-mcp": {"okx-github", "okx-protocol"},
    "okx-agent": {
        "okx-analysis",
        "okx-api",
        "okx-execution",
        "okx-github",
        "okx-observation",
        "okx-protocol",
        "okx-research",
        "okx-runtime",
    },
}


def validate(graph: dict[str, set[str]]) -> list[str]:
    errors: list[str] = []
    expected = set(ALLOWED)
    actual = set(graph)

    for missing in sorted(expected - actual):
        errors.append(f"missing expected workspace crate: {missing}")
    for extra in sorted(actual - expected):
        errors.append(f"new workspace crate requires architecture admission: {extra}")

    for crate in sorted(expected & actual):
        for dep in sorted(graph[crate] - ALLOWED[crate]):
            errors.append(f"forbidden local dependency: {crate} -> {dep}")
    return errors


def load_graph() -> dict[str, set[str]]:
    raw = subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--locked", "--format-version", "1"],
        text=True,
    )
    metadata = json.loads(raw)
    workspace_ids = set(metadata["workspace_members"])
    packages = {
        package["id"]: package
        for package in metadata["packages"]
        if package["id"] in workspace_ids
    }
    workspace_names = {package["name"] for package in packages.values()}

    return {
        package["name"]: {
            dependency["name"]
            for dependency in package["dependencies"]
            if dependency["name"] in workspace_names
        }
        for package in packages.values()
    }


# K10 target pure crates must be free of network/storage/runtime dependencies
# even through workspace-local transitive edges. Report separately while existing
# K10 migration is unfinished; the ordinary CI DAG check must stay honest.
K10_TARGET_PURE = ("okx-analysis", "okx-research")
K10_FORBIDDEN = {
    "okx-api", "okx-ws", "okx-runtime", "okx-github",
    "reqwest", "tokio", "rusqlite", "hyper", "axum", "tonic", "sqlx",
}


def pure_dependency_violations(
    graph: dict[str, set[str]],
    targets: tuple[str, ...] = K10_TARGET_PURE,
) -> list[str]:
    """Find the shortest actual dependency path to forbidden adapters/runtime."""
    from collections import deque

    errors = []
    for target in targets:
        if target not in graph:
            errors.append(f"K10 missing pure crate: {target}")
            continue
        visited = {target}
        queue = deque([(target, [target])])
        while queue:
            owner, path = queue.popleft()
            for dep in sorted(graph.get(owner, ())):
                next_path = path + [dep]
                if dep in K10_FORBIDDEN:
                    errors.append(f"K10 forbidden transitive dependency: {' -> '.join(next_path)}")
                    continue
                if dep in graph and dep not in visited:
                    visited.add(dep)
                    queue.append((dep, next_path))
    return errors


def load_complete_workspace_graph() -> dict[str, set[str]]:
    # --no-deps still includes every workspace manifest's direct external
    # requirements. Follow workspace edges recursively to catch adapter leaks.
    raw = subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--locked", "--format-version", "1"],
        text=True,
    )
    metadata = json.loads(raw)
    workspace_ids = set(metadata["workspace_members"])
    return {
        package["name"]: {dep["name"] for dep in package["dependencies"]}
        for package in metadata["packages"]
        if package["id"] in workspace_ids
    }


def self_test() -> int:
    accepted = {name: set(dependencies) for name, dependencies in ALLOWED.items()}
    if validate(accepted):
        print("self-test failed: accepted graph was rejected", file=sys.stderr)
        return 1

    # Removing an old dependency reduces coupling and is always permitted.
    simplified = {name: set(dependencies) for name, dependencies in ALLOWED.items()}
    simplified["okx-agent"].remove("okx-research")
    if validate(simplified):
        print("self-test failed: a removed dependency was rejected", file=sys.stderr)
        return 1

    forbidden = {name: set(dependencies) for name, dependencies in ALLOWED.items()}
    forbidden["okx-observation"].add("okx-analysis")
    errors = validate(forbidden)
    expected = "forbidden local dependency: okx-observation -> okx-analysis"
    if expected not in errors:
        print("self-test failed: forbidden reverse edge was not rejected", file=sys.stderr)
        return 1

    fixture = {
        "okx-analysis": {"okx-observation"},
        "okx-observation": {"okx-api"},
        "okx-api": {"reqwest", "tokio"},
        "okx-research": {"okx-analysis"},
    }
    violations = pure_dependency_violations(fixture)
    must_find = "K10 forbidden transitive dependency: okx-analysis -> okx-observation -> okx-api"
    if must_find not in violations:
        print("self-test failed: K10 transitive adapter leak passed", file=sys.stderr)
        return 1
    clean = {"okx-analysis": {"rust_decimal"}, "okx-research": {"okx-analysis"}}
    if pure_dependency_violations(clean):
        print("self-test failed: clean K10 pure graph rejected", file=sys.stderr)
        return 1

    print("ARCHITECTURE_SELF_TEST=PASS")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--k10-purity", action="store_true", help="strict transitive pure-crate acceptance; currently a blocking target")
    args = parser.parse_args()

    if args.self_test:
        return self_test()

    if args.k10_purity:
        errors = pure_dependency_violations(load_complete_workspace_graph())
        if errors:
            for error in errors:
                print(error, file=sys.stderr)
            return 1
        print("K10_TRANSITIVE_PURITY=PASS")
        return 0

    errors = validate(load_graph())
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1

    print("ARCHITECTURE_DEPENDENCY_GUARD=PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

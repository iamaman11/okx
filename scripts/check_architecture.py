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
    "okx-research": {"okx-observation"},
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
        for dep in sorted(ALLOWED[crate] - graph[crate]):
            errors.append(
                "accepted dependency disappeared; update architecture intentionally "
                f"if removal is valid: {crate} -> {dep}"
            )
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


def self_test() -> int:
    accepted = {name: set(dependencies) for name, dependencies in ALLOWED.items()}
    if validate(accepted):
        print("self-test failed: accepted graph was rejected", file=sys.stderr)
        return 1

    forbidden = {name: set(dependencies) for name, dependencies in ALLOWED.items()}
    forbidden["okx-observation"].add("okx-analysis")
    errors = validate(forbidden)
    expected = "forbidden local dependency: okx-observation -> okx-analysis"
    if expected not in errors:
        print("self-test failed: forbidden reverse edge was not rejected", file=sys.stderr)
        return 1

    print("ARCHITECTURE_SELF_TEST=PASS")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        return self_test()

    errors = validate(load_graph())
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1

    print("ARCHITECTURE_DEPENDENCY_GUARD=PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

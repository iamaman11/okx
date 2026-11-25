#!/usr/bin/env python3
"""Small executable guard for the accepted local-crate dependency DAG."""

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys

ALLOWED = {
    "okx-api": {"okx-engine-domain"},
    "okx-ws": {"okx-api"},
    "okx-observation": {"okx-api", "okx-engine-domain"},
    "okx-market-runtime": {"okx-api", "okx-observation", "okx-ipc-contracts", "okx-ws"},
    "okx-engine-domain": set(),
    "okx-research-domain": set(),
    "okx-strategy-core": {"okx-research-protocol"},
    "okx-analysis": {"okx-research-domain", "okx-strategy-core"},
    "okx-research": {"okx-analysis"},
    "okx-trade-execution": set(),
    # The financial wire must never import Research-owned definitions. A shared
    # protocol would cause a Research-only edit to compile Trading Engine.
    "okx-ipc-contracts": {"okx-engine-domain"},
    "okx-research-protocol": {"okx-research-domain"},
    "okx-github": set(),
    "okx-native-finance-check": set(),
    "okx-cli": {"okx-api"},
    "okx-host-launcher": set(),
    "okx-host-protocol": {"okx-ipc-contracts"},
    "okx-host-controller": {"okx-github", "okx-host-launcher", "okx-host-protocol", "okx-ipc-contracts"},
    "okx-bridge-mcp": {"okx-github", "okx-host-protocol", "okx-ipc-contracts"},
    "okx-research-service": {"okx-analysis", "okx-research-domain", "okx-research-protocol", "okx-research", "okx-strategy-core"},
    "okx-trading-engine": {
        "okx-api",
        "okx-trade-execution",
        "okx-github",
        "okx-observation",
        "okx-ipc-contracts",
        "okx-market-runtime",
    },
}

# Temporary S1 debt: native Engine T06–T10 child-process witnesses have not
# all been migrated to an owner-independent harness. Never confuse this
# test-only exception with a permitted normal/build dependency or S1 PASS.
TEMPORARY_ENGINE_TEST_DEBT = {"okx-trading-engine": {"okx-research-protocol"}}


# Scoped supplemental source guard: the Cargo DAG cannot see std::fs/network/
# wall-clock use. This does not replace Rust visibility/compile-fail checks.
RISK_PURE_FORBIDDEN = re.compile(
    r"\b(?:tokio|rusqlite|std::(?:fs|net|env|thread|time)|SystemTime|Instant|unsafe)\b"
)


def pure_risk_source_violations(sources: dict[str, str]) -> list[str]:
    errors = []
    for path, source in sorted(sources.items()):
        for lineno, line in enumerate(source.splitlines(), 1):
            # Comments alone do not create a runtime dependency.
            code = line.split("//", 1)[0].strip()
            if code and RISK_PURE_FORBIDDEN.search(code):
                errors.append(f"pure RiskBook forbidden symbol: {path}:{lineno}")
    return errors


def load_risk_sources() -> dict[str, str]:
    folder = Path(__file__).resolve().parents[1] / "crates/okx-trade-execution/src/risk"
    source_files = ("model.rs", "money.rs", "rules.rs", "settlement.rs", "money_property_tests.rs")
    return {
        name: (folder / name).read_text(encoding="utf-8") if (folder / name).is_file()
        else "use std::fs; // required pure risk source missing"
        for name in source_files
    }


def validate(graph: dict[str, set[str]], dev_graph: dict[str, set[str]] | None = None) -> list[str]:
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
    if dev_graph is not None:
        for crate in sorted(expected & set(dev_graph)):
            for dep in sorted(dev_graph[crate] - ALLOWED[crate]):
                if dep not in TEMPORARY_ENGINE_TEST_DEBT.get(crate, set()):
                    errors.append(f"forbidden test dependency: {crate} -> {dep}")
    return errors


def load_metadata() -> dict:
    # One locked Cargo resolution is enough for both direct workspace edges
    # and transitive production boundaries. The former previously incurred an
    # extra --no-deps Cargo subprocess on every full architecture check.
    raw = subprocess.check_output(
        ["cargo", "metadata", "--locked", "--format-version", "1"],
        text=True,
    )
    return json.loads(raw)


def load_graph(metadata: dict) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    workspace_ids = set(metadata["workspace_members"])
    packages = {
        package["id"]: package
        for package in metadata["packages"]
        if package["id"] in workspace_ids
    }
    workspace_names = {package["name"] for package in packages.values()}

    def exceptional_optional_test_edge(package: dict, dependency: dict) -> bool:
        # One narrowly identified migration fixture. Do not silently ignore
        # arbitrary optional dependencies in an accepted Cargo graph.
        return (
            package["name"] == "okx-ipc-contracts"
            and dependency["name"] == "okx-research-protocol"
            and dependency.get("optional", False)
            and "dep:okx-research-protocol" in package.get("features", {}).get("legacy-research-test", [])
            and "legacy-research-test" not in package.get("features", {}).get("default", [])
        )

    def edges(dev: bool) -> dict[str, set[str]]:
        return {
            package["name"]: {
                dependency["name"] for dependency in package["dependencies"]
                if dependency["name"] in workspace_names
                # The exact migration feature must not be mistaken for an
                # unconditional dependency. All other optional edges retain
                # normal direct-graph admission rules.
                and not exceptional_optional_test_edge(package, dependency)
                and (dependency["kind"] == "dev") == dev
            }
            for package in packages.values()
        }
    return edges(False), edges(True)



# K10 pure crates must be free of network/storage/runtime dependencies through
# the complete resolved production graph, including external package edges.
K10_TARGET_PURE = ("okx-engine-domain", "okx-research-domain", "okx-research-protocol", "okx-strategy-core", "okx-analysis", "okx-research")
K10_ENFORCED_PURE = K10_TARGET_PURE
K10_FORBIDDEN = {
    "okx-api", "okx-ws", "okx-market-runtime", "okx-github",
    "reqwest", "tokio", "rusqlite", "hyper", "axum", "tonic", "sqlx",
}


def pure_dependency_violations(
    graph: dict[str, set[str]],
    targets: tuple[str, ...] = K10_TARGET_PURE,
    package_names: dict[str, str] | None = None,
) -> list[str]:
    """Find the shortest actual dependency path to forbidden adapters/runtime."""
    from collections import deque

    names = package_names or {package: package for package in graph}
    errors = []
    for target in targets:
        roots = [package for package in graph if names.get(package, package) == target]
        if not roots:
            errors.append(f"K10 missing pure crate: {target}")
            continue
        visited = set(roots)
        queue = deque((root, [root]) for root in roots)
        while queue:
            owner, path = queue.popleft()
            for dep in sorted(graph.get(owner, ())):
                next_path = path + [dep]
                if names.get(dep, dep) in K10_FORBIDDEN:
                    readable_path = " -> ".join(names.get(node, node) for node in next_path)
                    errors.append(f"K10 forbidden transitive dependency: {readable_path}")
                    continue
                if dep in graph and dep not in visited:
                    visited.add(dep)
                    queue.append((dep, next_path))
    return errors


BOUNDARY_FORBIDDEN = {
    "okx-trading-engine": {
        "okx-research", "okx-research-service", "okx-strategy-core", "okx-analysis",
        "okx-research-protocol", "okx-research-domain",
    },
    "okx-research-service": {"okx-engine-domain", "okx-trading-engine", "okx-trade-execution", "okx-api", "okx-market-runtime", "okx-host-controller", "okx-bridge-mcp", "okx-ipc-contracts"},
    # Host lifecycle processes cannot acquire financial/market capabilities
    # through a direct or transitive production dependency. CONTROL may use
    # the Launcher and versioned protocol; neither may import Engine/SQL.
    "okx-host-controller": {"okx-trading-engine", "okx-trade-execution", "okx-api", "okx-market-runtime", "okx-observation", "okx-research", "okx-research-service"},
    "okx-host-launcher": {"okx-host-controller", "okx-trading-engine", "okx-trade-execution", "okx-api", "okx-market-runtime", "okx-observation", "okx-research", "okx-research-service"},
    "okx-host-protocol": {"okx-trading-engine", "okx-trade-execution", "okx-api", "okx-market-runtime", "okx-observation", "okx-research", "okx-research-service"},
}

# Independent build/test routes require both component closures to stay separate.
# Build and dev edges can cause a dependency to compile even without a runtime import.
COMPONENT_FORBIDDEN = {
    "okx-trading-engine": BOUNDARY_FORBIDDEN["okx-trading-engine"] | {
        "okx-host-controller", "okx-host-launcher", "okx-host-protocol", "okx-bridge-mcp",
    },
    "okx-research-service": BOUNDARY_FORBIDDEN["okx-research-service"] | {
        "okx-observation", "okx-ws", "okx-host-launcher", "okx-host-protocol",
    },
}


def component_scope_dependency_violations(metadata: dict, *, include_dev: bool = True) -> list[str]:
    """Check the complete test graph, including dev-activated optional features.

    Cargo metadata resolves optional features across dev and normal consumers;
    filtering dep_kinds alone is NOT proof of the production-only root closure.
    See cargo_component_tree_violations for the normal/build authority.
    """
    from collections import deque

    names = {package["id"]: package["name"] for package in metadata["packages"]}
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    members = set(metadata["workspace_members"])
    errors = []
    for component, forbidden in COMPONENT_FORBIDDEN.items():
        roots = [pkg for pkg in members if names.get(pkg) == component]
        if len(roots) != 1:
            errors.append(f"CI scope cannot establish unique Cargo root: {component}")
            continue
        visited = set(roots)
        queue = deque((root, [root]) for root in roots)
        while queue:
            owner, path = queue.popleft()
            node = nodes.get(owner)
            if node is None:
                errors.append(f"CI scope missing resolved Cargo node: {names.get(owner, owner)}")
                continue
            for dep in node["deps"]:
                if not include_dev and not any(
                    kind["kind"] in (None, "build") for kind in dep.get("dep_kinds", [])
                ):
                    continue
                next_node = dep["pkg"]
                next_path = path + [next_node]
                if next_node not in names or not dep.get("dep_kinds"):
                    errors.append(f"CI scope unresolved Cargo dependency: {names.get(owner, owner)}")
                    continue
                if next_node in members and names[next_node] in forbidden:
                    readable = " -> ".join(names[item] for item in next_path)
                    errors.append(f"CI forbidden component compile/test dependency: {readable}")
                    continue
                if next_node not in visited:
                    visited.add(next_node)
                    queue.append((next_node, next_path))
    return errors


def cargo_tree_scope_violations(output: str, root: str, forbidden: set[str]) -> list[str]:
    """Parse one root-scoped `cargo tree --prefix depth --format {p}` result."""
    errors = []
    path: list[str] = []
    seen_root = False
    for line in output.splitlines():
        match = re.fullmatch(r"(\d+)([a-zA-Z0-9_-]+) v\S+.*", line)
        if match is None:
            errors.append(f"Cargo tree unexpectedly formatted dependency: {line[:140]}")
            continue
        depth, package = int(match.group(1)), match.group(2)
        if depth > len(path):
            errors.append(f"Cargo tree skipped dependency depth: {line[:140]}")
            continue
        path = path[:depth] + [package]
        if not seen_root:
            if depth != 0 or package != root:
                errors.append(f"Cargo tree did not begin at root {root}")
            seen_root = True
        elif depth == 0:
            errors.append(f"Cargo tree unexpectedly added root {package}")
        if package in forbidden and depth > 0:
            errors.append("forbidden normal/build component dependency: " + " -> ".join(path))
    if not seen_root:
        errors.append(f"Cargo tree returned no root: {root}")
    return errors


def cargo_component_tree_violations() -> list[str]:
    """The actual Windows normal/build closure, without dev feature unification."""
    errors = []
    roots = sorted(set(BOUNDARY_FORBIDDEN) | set(COMPONENT_FORBIDDEN))
    for root in roots:
        forbidden = BOUNDARY_FORBIDDEN.get(root, set()) | COMPONENT_FORBIDDEN.get(root, set())
        try:
            output = subprocess.check_output(
                ["cargo", "tree", "--locked", "-p", root, "--target",
                 "x86_64-pc-windows-msvc", "--edges", "normal,build",
                 "--prefix", "depth", "--format", "{p}"],
                text=True,
                stderr=subprocess.PIPE,
            )
        except (OSError, subprocess.CalledProcessError) as error:
            errors.append(f"Cargo normal/build closure unavailable for {root}: {error}")
            continue
        errors.extend(cargo_tree_scope_violations(output, root, forbidden))
    return errors


def production_boundary_violations(graph: dict[str, set[str]], names: dict[str, str]) -> list[str]:
    """Check transitive normal edges by package ID, never test dependencies."""
    from collections import deque
    errors = []
    for target, forbidden in BOUNDARY_FORBIDDEN.items():
        roots = [node for node in graph if names.get(node) == target]
        if not roots:
            errors.append(f"boundary missing crate: {target}")
            continue
        visited = set(roots)
        queue = deque((root, [root]) for root in roots)
        while queue:
            node, path = queue.popleft()
            for dep in sorted(graph.get(node, ())):
                next_path = path + [dep]
                if names.get(dep) in forbidden:
                    errors.append("forbidden production boundary: " + " -> ".join(names.get(n, n) for n in next_path))
                if dep not in visited:
                    visited.add(dep)
                    queue.append((dep, next_path))
    return errors


def load_complete_workspace_graph(metadata: dict) -> tuple[dict[str, set[str]], dict[str, str]]:
    # Exact package IDs keep different versions/features distinct. Only normal
    # dependencies enter the shipped graph; build tools and test-only helpers
    # do not turn unrelated workspace test infrastructure into runtime edges.
    names = {package["id"]: package["name"] for package in metadata["packages"]}
    graph = {
        node["id"]: {
            dependency["pkg"]
            for dependency in node["deps"]
            if any(kind["kind"] is None for kind in dependency["dep_kinds"])
        }
        for node in metadata["resolve"]["nodes"]
    }
    return graph, names


def self_test() -> int:
    metadata = {
        "workspace_members": ["research-id", "engine-id", "analysis-id", "ipc-id", "api-id"],
        "packages": [
            {"id": "research-id", "name": "okx-research", "dependencies": [
                {"name": "okx-analysis", "kind": None},
                {"name": "okx-api", "kind": "dev"},
            ]},
            {"id": "engine-id", "name": "okx-trading-engine", "dependencies": [
                {"name": "okx-ipc-contracts", "kind": None},
            ]},
            {"id": "analysis-id", "name": "okx-analysis", "dependencies": []},
            {"id": "ipc-id", "name": "okx-ipc-contracts", "dependencies": []},
            {"id": "api-id", "name": "okx-api", "dependencies": []},
        ],
        "resolve": {"nodes": [
            {"id": "research-id", "deps": [
                {"pkg": "analysis-id", "dep_kinds": [{"kind": None}]},
                {"pkg": "api-id", "dep_kinds": [{"kind": "dev"}]},
            ]},
            {"id": "engine-id", "deps": [
                {"pkg": "ipc-id", "dep_kinds": [{"kind": None}]},
            ]},
        ]},
    }
    direct, dev = load_graph(metadata)
    resolved, names = load_complete_workspace_graph(metadata)
    if (direct != {"okx-research": {"okx-analysis"},
                   "okx-trading-engine": {"okx-ipc-contracts"},
                   "okx-analysis": set(), "okx-ipc-contracts": set(), "okx-api": set()}
        or dev != {"okx-research": {"okx-api"}, "okx-trading-engine": set(),
                   "okx-analysis": set(), "okx-ipc-contracts": set(), "okx-api": set()}
        or resolved != {"research-id": {"analysis-id"}, "engine-id": {"ipc-id"}}
        or names["research-id"] != "okx-research"):
        print("self-test failed: shared Cargo metadata graph boundaries", file=sys.stderr)
        return 1

    metadata["resolve"]["nodes"].extend([
        {"id": "ipc-id", "deps": []},
        {"id": "analysis-id", "deps": []},
        {"id": "api-id", "deps": []},
    ])

    metadata["workspace_members"].extend(["worker-id", "research-protocol-id", "domain-id"])
    metadata["packages"].extend([
        {"id": "worker-id", "name": "okx-research-service"},
        {"id": "research-protocol-id", "name": "okx-research-protocol"},
        {"id": "domain-id", "name": "okx-engine-domain"},
    ])
    metadata["resolve"]["nodes"].extend([
        {"id": "worker-id", "deps": [{"pkg": "research-protocol-id", "dep_kinds": [{"kind": None}]}]},
        {"id": "research-protocol-id", "deps": []},
        {"id": "domain-id", "deps": []},
    ])

    if component_scope_dependency_violations(metadata):
        print("self-test failed: clean Engine scoped-owner closure rejected", file=sys.stderr)
        return 1
    # Historical AgentOperation routed Research inside okx-ipc-contracts.
    # Assert that a normal, build, or dev edge anywhere in Engine's resolved
    # closure cannot silently reintroduce Research's protocol or domain.
    for forbidden_id, forbidden_name in (
        ("research-protocol-id", "okx-research-protocol"),
        ("research-domain-id", "okx-research-domain"),
    ):
        for kind in (None, "build", "dev"):
            leaked = json.loads(json.dumps(metadata))
            if forbidden_id == "research-domain-id":
                leaked["workspace_members"].append("research-domain-id")
                leaked["packages"].append({"id": "research-domain-id", "name": forbidden_name})
                leaked["resolve"]["nodes"].append({"id": "research-domain-id", "deps": []})
            next(node for node in leaked["resolve"]["nodes"] if node["id"] == "ipc-id")["deps"].append({
                "pkg": forbidden_id, "dep_kinds": [{"kind": kind}],
            })
            expected_path = f"okx-trading-engine -> okx-ipc-contracts -> {forbidden_name}"
            if not any(expected_path in error for error in component_scope_dependency_violations(leaked)):
                print(f"self-test failed: Engine indirect {kind} {forbidden_name} dependency accepted", file=sys.stderr)
                return 1
            in_production = any(
                expected_path in error
                for error in component_scope_dependency_violations(leaked, include_dev=False)
            )
            if in_production != (kind != "dev"):
                print(f"self-test failed: Engine {kind} {forbidden_name} production/dev classification", file=sys.stderr)
                return 1
            if kind is None:
                graph, names = load_complete_workspace_graph(leaked)
                if not any(expected_path in error for error in production_boundary_violations(graph, names)):
                    print(f"self-test failed: Engine production {forbidden_name} dependency accepted", file=sys.stderr)
                    return 1
    for kind in (None, "dev", "build"):
        dirty_scope = json.loads(json.dumps(metadata))
        dirty_scope["resolve"]["nodes"][1]["deps"].append({
            "pkg": "research-id", "dep_kinds": [{"kind": kind}],
        })
        if not any("okx-trading-engine -> okx-research" in error for error in component_scope_dependency_violations(dirty_scope)):
            print(f"self-test failed: Engine {kind} Research dependency accepted", file=sys.stderr)
            return 1
        for forbidden_id, forbidden_name in (("api-id", "okx-api"), ("ipc-id", "okx-ipc-contracts"), ("domain-id", "okx-engine-domain")):
            reverse_scope = json.loads(json.dumps(metadata))
            next(node for node in reverse_scope["resolve"]["nodes"] if node["id"] == "research-protocol-id")["deps"].append({
                "pkg": forbidden_id, "dep_kinds": [{"kind": kind}],
            })
            if not any(f"okx-research-service -> okx-research-protocol -> {forbidden_name}" in error
                       for error in component_scope_dependency_violations(reverse_scope)):
                print(f"self-test failed: transitive Research {kind} {forbidden_name} dependency accepted", file=sys.stderr)
                return 1
    host_scope = json.loads(json.dumps(metadata))
    for package in host_scope["packages"]:
        if package["id"] == "research-id":
            package["name"] = "okx-host-protocol"
    host_scope["resolve"]["nodes"][1]["deps"].append({
        "pkg": "research-id", "dep_kinds": [{"kind": "dev"}],
    })
    if not component_scope_dependency_violations(host_scope):
        print("self-test failed: Engine Host protocol test dependency accepted", file=sys.stderr)
        return 1
    dirty_transitive_scope = json.loads(json.dumps(metadata))
    next(node for node in dirty_transitive_scope["resolve"]["nodes"] if node["id"] == "ipc-id")["deps"].append({
        "pkg": "analysis-id", "dep_kinds": [{"kind": "dev"}],
    })
    if not any("ipc-contracts -> okx-analysis" in error for error in component_scope_dependency_violations(dirty_transitive_scope)):
        print("self-test failed: transitive Engine test dependency accepted", file=sys.stderr)
        return 1
    truncated_scope = json.loads(json.dumps(metadata))
    truncated_scope["resolve"]["nodes"] = [node for node in truncated_scope["resolve"]["nodes"] if node["id"] != "engine-id"]
    if not component_scope_dependency_violations(truncated_scope):
        print("self-test failed: missing Engine Cargo resolution accepted", file=sys.stderr)
        return 1

    accepted = {name: set(dependencies) for name, dependencies in ALLOWED.items()}
    if validate(accepted):
        print("self-test failed: accepted graph was rejected", file=sys.stderr)
        return 1

    # Removing an old dependency reduces coupling and is always permitted.
    simplified = {name: set(dependencies) for name, dependencies in ALLOWED.items()}
    simplified["okx-trading-engine"].remove("okx-api")
    if validate(simplified):
        print("self-test failed: a removed dependency was rejected", file=sys.stderr)
        return 1

    reverse = {name: set(dependencies) for name, dependencies in ALLOWED.items()}
    reverse["okx-engine-domain"].add("okx-trade-execution")
    if "forbidden local dependency: okx-engine-domain -> okx-trade-execution" not in validate(reverse):
        print("self-test failed: reverse risk domain dependency passed", file=sys.stderr)
        return 1

    research_escape = {name: set(dependencies) for name, dependencies in ALLOWED.items()}
    research_escape["okx-research"].add("okx-trade-execution")
    if "forbidden local dependency: okx-research -> okx-trade-execution" not in validate(research_escape):
        print("self-test failed: research gained write capability", file=sys.stderr)
        return 1

    wire_leak = {name: set(dependencies) for name, dependencies in ALLOWED.items()}
    wire_leak["okx-analysis"].add("okx-ipc-contracts")
    if "forbidden local dependency: okx-analysis -> okx-ipc-contracts" not in validate(wire_leak):
        print("self-test failed: analysis gained an IPC dependency", file=sys.stderr)
        return 1
    legacy_wire = {name: set(dependencies) for name, dependencies in ALLOWED.items()}
    legacy_wire["okx-ipc-contracts"].add("okx-research-protocol")
    if "forbidden local dependency: okx-ipc-contracts -> okx-research-protocol" not in validate(legacy_wire):
        print("self-test failed: shared Engine IPC gained Research protocol", file=sys.stderr)
        return 1
    temporary_test_wire = {name: set() for name in ALLOWED}
    temporary_test_wire["okx-trading-engine"].add("okx-research-protocol")
    if validate(accepted, temporary_test_wire):
        print("self-test failed: test-only Engine Research witness debt rejected before migration", file=sys.stderr)
        return 1
    clean_tree = "0okx-trading-engine v0.1.0\n1okx-ipc-contracts v0.1.0\n2okx-engine-domain v0.1.0 (*)\n"
    if cargo_tree_scope_violations(clean_tree, "okx-trading-engine", BOUNDARY_FORBIDDEN["okx-trading-engine"]):
        print("self-test failed: clean production Engine Cargo tree rejected", file=sys.stderr)
        return 1
    for name in ("okx-research-protocol", "okx-research-domain"):
        dirty_tree = clean_tree + f"2{name} v0.1.0\n"
        if not any(name in err for err in cargo_tree_scope_violations(dirty_tree, "okx-trading-engine", BOUNDARY_FORBIDDEN["okx-trading-engine"])):
            print(f"self-test failed: {name} production Cargo tree leak accepted", file=sys.stderr)
            return 1

    dev = {name: set() for name in ALLOWED}
    dev["okx-research-service"].update({"okx-api", "okx-market-runtime"})
    if not validate(accepted, dev):
        print("self-test failed: worker fixture dependencies accepted", file=sys.stderr)
        return 1
    for dep in ("okx-api", "okx-market-runtime", "okx-trading-engine", "okx-trade-execution", "okx-host-controller"):
        dirty = {name: set(deps) for name, deps in accepted.items()}
        dirty["okx-research-service"].add(dep)
        if not validate(dirty):
            print(f"self-test failed: worker normal edge {dep} accepted", file=sys.stderr)
            return 1
    for dep in ("okx-research", "okx-research-service", "okx-strategy-core", "okx-analysis"):
        dirty = {name: set(deps) for name, deps in accepted.items()}
        dirty["okx-trading-engine"].add(dep)
        if not validate(dirty):
            print(f"self-test failed: agent edge {dep} accepted", file=sys.stderr)
            return 1
    transitive = {"worker": {"helper"}, "helper": {"api"}, "api": set(), "agent": set(), "control": set(), "launcher": set(), "host_protocol": set()}
    transitive_names = {"worker": "okx-research-service", "helper": "helper", "api": "okx-api", "agent": "okx-trading-engine", "control": "okx-host-controller", "launcher": "okx-host-launcher", "host_protocol": "okx-host-protocol"}
    if not production_boundary_violations(transitive, transitive_names):
        print("self-test failed: transitive worker API leak passed", file=sys.stderr)
        return 1
    transitive["worker"] = set()
    if production_boundary_violations(transitive, transitive_names):
        print("self-test failed: clean production graph rejected", file=sys.stderr)
        return 1
    transitive["agent"] = {"helper"}
    transitive_names["api"] = "okx-research"
    if not production_boundary_violations(transitive, transitive_names):
        print("self-test failed: transitive Agent research leak passed", file=sys.stderr)
        return 1
    transitive_names["api"] = "okx-strategy-core"
    if not production_boundary_violations(transitive, transitive_names):
        print("self-test failed: transitive Agent mathematical core leak passed", file=sys.stderr)
        return 1
    transitive["agent"] = set()
    transitive["control"] = {"helper"}
    transitive_names["api"] = "okx-trade-execution"
    if not production_boundary_violations(transitive, transitive_names):
        print("self-test failed: transitive CONTROL -> financial SQL leak passed", file=sys.stderr)
        return 1
    transitive["control"] = set()
    transitive["launcher"] = {"control"}
    if not production_boundary_violations(transitive, transitive_names):
        print("self-test failed: Launcher -> CONTROL reverse dependency passed", file=sys.stderr)
        return 1
    transitive["launcher"] = set()
    transitive["host_protocol"] = {"helper"}
    if not production_boundary_violations(transitive, transitive_names):
        print("self-test failed: Host protocol acquired financial dependency", file=sys.stderr)
        return 1

    pure_source = {"money.rs": "use std::fmt;\nfn one() {}"}
    dirty_source = {"money.rs": "use std::fs;\nfn bad(){std::fs::write(\"x\", b\"x\");}"}
    if pure_risk_source_violations(pure_source) or not pure_risk_source_violations(dirty_source):
        print("self-test failed: pure RiskBook source guard", file=sys.stderr)
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
    clean = {"okx-engine-domain": {"serde"}, "okx-research-domain": set(), "okx-research-protocol": {"okx-research-domain"}, "okx-strategy-core": {"okx-research-protocol"}, "okx-analysis": {"rust_decimal"}, "okx-research": {"okx-analysis"}}
    if pure_dependency_violations(clean):
        print("self-test failed: clean K10 pure graph rejected", file=sys.stderr)
        return 1

    dirty_kernel = {"okx-engine-domain": {"okx-api"}, "okx-api": {"reqwest"}}
    if not pure_dependency_violations(dirty_kernel, ("okx-engine-domain",)):
        print("self-test failed: new kernel network violation went unnoticed", file=sys.stderr)
        return 1

    # An external helper may hide a runtime dependency. Preserve exact IDs so
    # an unrelated version of that helper cannot overwrite the leaking path.
    external = {"kernel-id": {"helper-v1"}, "helper-v1": {"tokio-id"}, "helper-v2": set(), "tokio-id": set()}
    external_names = {"kernel-id": "okx-engine-domain", "helper-v1": "helper", "helper-v2": "helper", "tokio-id": "tokio"}
    if not pure_dependency_violations(external, ("okx-engine-domain",), external_names):
        print("self-test failed: external package runtime leak passed", file=sys.stderr)
        return 1

    print("ARCHITECTURE_SELF_TEST=PASS")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--k10-purity", action="store_true", help="check the complete resolved production graph for pure crates")
    parser.add_argument(
        "--strict-component-test-boundary", action="store_true",
        help="require Engine and Research build/test closures to be independent including dev (final S1)",
    )
    args = parser.parse_args()

    if args.self_test:
        return self_test()

    metadata = load_metadata()
    if args.k10_purity:
        graph, names = load_complete_workspace_graph(metadata)
        errors = pure_dependency_violations(graph, package_names=names)
        errors.extend(pure_risk_source_violations(load_risk_sources()))
        if errors:
            for error in errors:
                print(error, file=sys.stderr)
            return 1
        print("K10_TRANSITIVE_PURITY=PASS")
        return 0

    normal, dev = load_graph(metadata)
    errors = validate(normal, dev)
    errors.extend(pure_risk_source_violations(load_risk_sources()))
    graph, names = load_complete_workspace_graph(metadata)
    errors.extend(pure_dependency_violations(graph, K10_ENFORCED_PURE, names))
    # `cargo metadata` unifies optional Research test features across the
    # workspace; treating those as Engine production deps is a false failure.
    # Root-scoped cargo tree normal/build is the authoritative release graph.
    production_scope_errors = cargo_component_tree_violations()
    errors.extend(production_scope_errors)
    complete_scope_errors = component_scope_dependency_violations(metadata, include_dev=True)
    test_debt = sorted(set(complete_scope_errors))
    if args.strict_component_test_boundary:
        errors.extend(test_debt)
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1

    if test_debt:
        print("ARCHITECTURE_TEST_BOUNDARY=DEBT_UNRESOLVED", file=sys.stderr)
        for debt in test_debt:
            print(f"S1_DEV_DEBT: {debt}", file=sys.stderr)
    else:
        print("ARCHITECTURE_TEST_BOUNDARY=PASS")
    print("ARCHITECTURE_DEPENDENCY_GUARD=PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

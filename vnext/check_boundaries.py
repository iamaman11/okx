#!/usr/bin/env python3
"""Validate and list file ownership in the vNext integration branch."""

import argparse
import csv
import json
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
import subprocess
import sys
import tomllib


ROOTS = {"engine", "research", "control", "edge"}
OWNERS = ROOTS | {"integration", "repository"}
WORKFLOWS = {
    f".github/workflows/vnext-{owner}.yml": owner for owner in ROOTS
} | {
    ".github/workflows/vnext-boundaries.yml": "repository",
    ".github/workflows/vnext-ci.yml": "repository",
}
REPOSITORY_FILES = {
    "vnext/README.md",
    "vnext/BOUNDARIES.md",
    "vnext/legacy-files.tsv",
    "vnext/check_boundaries.py",
    "vnext/test_check_boundaries.py",
    "vnext/split-symbols.tsv",
    "vnext/check_split_symbols.py",
    "vnext/test_check_split_symbols.py",
    "vnext/s0/README.md",
    "vnext/s0/measure.py",
    "vnext/s0/test_measure.py",
    "vnext/s0/engine_topology_ab.py",
    "vnext/s0/test_engine_topology_ab.py",
    "vnext/s0/engine_ab_local_sample.json",
    "vnext/s0/research_topology.py",
    "vnext/s0/research_oracle.py",
    "vnext/s0/research_diff.py",
    "vnext/s0/test_research_topology_ab.py",
    "vnext/s0/research_ab_local_sample.json",
    "vnext/s0/functional_coverage.md",
    "vnext/s0/topology_decision.md",
    "vnext/ci/impact.py",
    "vnext/ci/test_impact.py",
}
RUST_SOURCE_PATH = re.compile(
    r'\binclude(?:_str|_bytes)?!\s*\(\s*"([^"]+)"|#\s*\[\s*path\s*=\s*"([^"]+)"'
)
JS_SOURCE_PATH = re.compile(
    r'''(?:\b(?:from|import)\s*|\b(?:import|require|URL)\(\s*)["'](\.{1,2}/[^"']+)["']'''
)

JS_WORKING_PATH = re.compile(
    r'''\b(?:resolve|readFile|readFileSync)\(\s*["'](\.{1,2}/[^"']+)["']'''
)
TS_REFERENCE_PATH = re.compile(
    r'''///\s*<reference\s+path\s*=\s*["']([^"']+)["']'''
)


def root_for(path):
    parts = PurePosixPath(path).parts
    if len(parts) >= 3 and parts[0] == "vnext" and parts[1] in ROOTS | {"integration"}:
        return parts[1]
    return None


def fixed_owner(path):
    if path in REPOSITORY_FILES:
        return "repository"
    return WORKFLOWS.get(path) or root_for(path)


def inventory(path):
    rows = {}
    direct_targets = {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if reader.fieldnames != ["path", "owner", "action", "destination"]:
            raise ValueError("legacy-files.tsv has invalid columns")
        for row in reader:
            file, owner, action, destination = (row[key] for key in reader.fieldnames)
            if (
                file in rows
                or fixed_owner(file)
                or file.startswith(".github/workflows/")
                or owner not in OWNERS
            ):
                raise ValueError(f"invalid or duplicate ownership: {file}")
            targets = destination.split(";")
            valid = {
                "archive": destination == "-",
                "retire": destination == "-",
                "keep": owner == "repository" and destination == file,
                "move": len(targets) == 1 and root_for(destination) == owner,
                "oracle": len(targets) == 1 and root_for(destination) == owner,
                "split": len(set(targets)) >= 2 and all(root_for(target) for target in targets),
            }
            if not valid.get(action) or any(
                ".." in PurePosixPath(target).parts for target in targets
            ):
                raise ValueError(f"invalid migration target: {file}: {destination}")
            if action in {"move", "oracle"}:
                if destination in direct_targets:
                    raise ValueError(
                        f"two files claim {destination}: {direct_targets[destination]}, {file}"
                    )
                direct_targets[destination] = file
            rows[file] = owner, action, destination
    return rows


def file_ownership(tracked, legacy):
    missing = {path for path in tracked if not fixed_owner(path)} - legacy.keys()
    outdated = legacy.keys() - tracked
    if missing or outdated:
        errors = [f"file has no owner: {path}" for path in sorted(missing)]
        errors.extend(f"remove stale legacy row: {path}" for path in sorted(outdated))
        raise ValueError("\n".join(errors))
    return {
        path: legacy[path] if path in legacy else (fixed_owner(path), "active", path)
        for path in sorted(tracked)
    }


def cargo_paths(value):
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"path", "build", "workspace"} and isinstance(item, str):
                yield item
            elif key in {"members", "default-members", "exclude"} and isinstance(item, list):
                yield from item
            else:
                yield from cargo_paths(item)
    elif isinstance(value, list):
        for item in value:
            yield from cargo_paths(item)


def npm_paths(value):
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"workspaces", "packages"} and isinstance(item, list):
                yield from item
            else:
                yield from npm_paths(item)
    elif isinstance(value, list):
        for item in value:
            yield from npm_paths(item)
    elif isinstance(value, str) and value.startswith(("file:", "link:")):
        yield value.split(":", 1)[1]


def tsconfig_paths(value):
    """Static local paths, including aliases that have no consumer yet."""
    if not isinstance(value, dict):
        return
    for key in ("extends", "include", "exclude", "files", "references"):
        item = value.get(key)
        if isinstance(item, str):
            if (key != "extends" or item.startswith((".", "/", "\\"))
                    or PureWindowsPath(item).drive):
                yield item
        elif isinstance(item, list):
            for part in item:
                if isinstance(part, str):
                    yield part
                elif isinstance(part, dict) and isinstance(part.get("path"), str):
                    yield part["path"]
    compiler = value.get("compilerOptions", {})
    if not isinstance(compiler, dict):
        return
    for key in ("baseUrl", "rootDir", "outDir", "rootDirs", "typeRoots"):
        item = compiler.get(key)
        if isinstance(item, str):
            yield item
        elif isinstance(item, list):
            yield from (part for part in item if isinstance(part, str))
    paths = compiler.get("paths", {})
    base_url = compiler.get("baseUrl", ".")
    if not isinstance(base_url, str):
        base_url = "."
    if isinstance(paths, dict):
        for targets in paths.values():
            if isinstance(targets, list):
                yield from (
                    (part if PureWindowsPath(part).anchor else f"{base_url}/{part}")
                    for part in targets
                    if isinstance(part, str)
                )


def local_references(path):
    if path.name == "Cargo.toml":
        yield from cargo_paths(tomllib.loads(path.read_text(encoding="utf-8")))
    elif path.name in {"package.json", "package-lock.json"}:
        yield from npm_paths(json.loads(path.read_text(encoding="utf-8")))
    elif path.name.startswith("tsconfig") and path.name.endswith(".json"):
        yield from tsconfig_paths(json.loads(path.read_text(encoding="utf-8")))
    elif path.suffix == ".rs":
        for match in RUST_SOURCE_PATH.finditer(path.read_text(encoding="utf-8")):
            yield match[1] or match[2]
    elif path.suffix in {".ts", ".tsx", ".js", ".mjs", ".cjs"}:
        source = path.read_text(encoding="utf-8")
        yield from JS_SOURCE_PATH.findall(source)
        yield from TS_REFERENCE_PATH.findall(source)


def check_references(repo, ownership):
    for file, (owner, action, _) in ownership.items():
        if action != "active" or root_for(file) is None:
            continue
        path = repo / file
        boundary = repo / "vnext" / owner
        if not path.resolve().is_relative_to(boundary):
            raise ValueError(f"{file}: file resolves outside {boundary.relative_to(repo)}")
        references = [(path.parent, ref) for ref in local_references(path)]
        if path.suffix in {".ts", ".tsx", ".js", ".mjs", ".cjs"}:
            working_paths = JS_WORKING_PATH.findall(path.read_text(encoding="utf-8"))
            references.extend((boundary, ref) for ref in working_paths)
        for base, ref in references:
            # Linux CI must also reject Windows drive, UNC and backslash
            # traversal paths before a Windows build can resolve them.
            windows_path = PureWindowsPath(ref)
            # Preserve any traversal *after* a tsconfig glob as well.
            # Stripping at the first '*' would silently miss `src/*/../../../other`.
            normalized = ref.replace("\\", "/").replace("*", "_glob_")
            if windows_path.drive or windows_path.root or not (
                base / normalized
            ).resolve().is_relative_to(boundary):
                raise ValueError(f"{file}: local reference escapes {owner}: {ref}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="print one row per tracked file")
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    tracked = set(
        subprocess.check_output(["git", "ls-files", "-z"], cwd=repo)
        .decode("utf-8")
        .rstrip("\0")
        .split("\0")
    )
    try:
        legacy = inventory(repo / "vnext/legacy-files.tsv")
        ownership = file_ownership(tracked, legacy)
        check_references(repo, ownership)
    except (ValueError, OSError) as error:
        print(error, file=sys.stderr)
        return 1
    if args.list:
        print("path\towner\taction\tdestination")
        for file, row in ownership.items():
            print("\t".join((file, *row)))
    else:
        print(f"Ownership checked: {len(ownership)} tracked files, {len(legacy)} legacy rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

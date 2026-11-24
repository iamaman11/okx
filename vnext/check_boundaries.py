#!/usr/bin/env python3
"""Validate and list file ownership in the vNext integration branch."""

import argparse
import csv
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tomllib


ROOTS = {"engine", "research", "control", "edge"}
OWNERS = ROOTS | {"integration", "repository"}
WORKFLOWS = {
    f".github/workflows/vnext-{owner}.yml": owner for owner in ROOTS
} | {".github/workflows/vnext-boundaries.yml": "repository"}
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


def local_references(path):
    if path.name == "Cargo.toml":
        yield from cargo_paths(tomllib.loads(path.read_text(encoding="utf-8")))
    elif path.name in {"package.json", "package-lock.json"}:
        yield from npm_paths(json.loads(path.read_text(encoding="utf-8")))
    elif path.suffix == ".rs":
        for match in RUST_SOURCE_PATH.finditer(path.read_text(encoding="utf-8")):
            yield match[1] or match[2]
    elif path.suffix in {".ts", ".js", ".mjs", ".cjs"}:
        yield from JS_SOURCE_PATH.findall(path.read_text(encoding="utf-8"))


def check_references(repo, ownership):
    for file, (owner, action, _) in ownership.items():
        if action != "active" or root_for(file) is None:
            continue
        path = repo / file
        boundary = repo / "vnext" / owner
        if not path.resolve().is_relative_to(boundary):
            raise ValueError(f"{file}: file resolves outside {boundary.relative_to(repo)}")
        references = [(path.parent, ref) for ref in local_references(path)]
        if path.suffix in {".ts", ".js", ".mjs", ".cjs"}:
            working_paths = JS_WORKING_PATH.findall(path.read_text(encoding="utf-8"))
            references.extend((boundary, ref) for ref in working_paths)
        for base, ref in references:
            if not (base / ref).resolve().is_relative_to(boundary):
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

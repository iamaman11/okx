#!/usr/bin/env python3
"""Reproducible, non-authoritative local measurements for OKX vNext S0.

This measures explicitly selected owner commands, not GitHub Ready CI. In
particular, a fast sample here is never evidence that #569's 120 s gate passed.
"""

from __future__ import annotations

import argparse
import hashlib
from itertools import chain
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any, Iterable


IGNORED_DIRECTORIES = {".git", "target", "node_modules", ".venv", "dist"}
TEST_RESULT = re.compile(
    r"test result: (?:ok|FAILED)\.\s+(\d+) passed;\s+(\d+) failed;"
    r"\s+(\d+) ignored;\s+(\d+) measured;\s+(\d+) filtered out"
)
RUSTC_RUN = re.compile(r"\bRunning\b.*(?:[/\\]|\b)rustc(?:\s|[`'\"])")


def source_fingerprint(workspace: Path) -> tuple[str, int]:
    """Fingerprint tracked-like inputs, not build products or temporary logs."""
    digest = hashlib.sha256()
    count = 0
    for base, dirs, files in os.walk(workspace):
        dirs[:] = sorted(name for name in dirs if name not in IGNORED_DIRECTORIES)
        for filename in sorted(files):
            path = Path(base) / filename
            if path.is_symlink():
                # A symlink must not pull in a sibling application's sources.
                target = os.readlink(path)
                content = b"SYMLINK:" + target.encode("utf-8", errors="surrogateescape")
            elif path.is_file():
                content = path.read_bytes()
            else:
                continue
            relative = path.relative_to(workspace).as_posix().encode()
            digest.update(len(relative).to_bytes(8, "big"))
            digest.update(relative)
            digest.update(len(content).to_bytes(8, "big"))
            digest.update(content)
            count += 1
    return digest.hexdigest(), count


def capture_version(command: list[str]) -> str | None:
    if shutil.which(command[0]) is None:
        return None
    try:
        completed = subprocess.run(
            command, check=True, capture_output=True, text=True, timeout=10
        )
        return completed.stdout.strip() or completed.stderr.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def git_ident(repo: Path | None) -> dict[str, Any]:
    if repo is None:
        return {"head": None, "tree": None, "dirty": None}
    values: dict[str, Any] = {}
    for field, command in (
        ("head", ["git", "rev-parse", "HEAD"]),
        ("tree", ["git", "rev-parse", "HEAD^{tree}"]),
        ("dirty", ["git", "status", "--porcelain", "--untracked-files=no"]),
    ):
        try:
            result = subprocess.run(command, cwd=repo, check=True,
                                    capture_output=True, text=True, timeout=10)
            values[field] = (bool(result.stdout.strip()) if field == "dirty"
                             else result.stdout.strip())
        except (OSError, subprocess.SubprocessError):
            values[field] = None
    return values


def environment(repo: Path | None) -> dict[str, Any]:
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "python": platform.python_version(),
        "rustc": capture_version(["rustc", "--version", "--verbose"]),
        "cargo": capture_version(["cargo", "--version"]),
        "node": capture_version(["node", "--version"]),
        "npm": capture_version(["npm", "--version"]),
        "git": git_ident(repo),
        # An explicit allowlist: never serialize credentials or all process env.
        "build_inputs": {
            name: os.environ[name] for name in (
                "RUSTUP_TOOLCHAIN", "RUSTFLAGS", "RUSTDOCFLAGS",
                "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_TARGET",
                "CARGO_TARGET_DIR", "CARGO_INCREMENTAL", "CARGO_BUILD_JOBS",
                "RUSTC_WORKSPACE_WRAPPER", "CC", "CXX",
                "CI", "RUNNER_OS", "ImageVersion",
            ) if name in os.environ
        },
    }


def allowed_command(raw: str, owner: str) -> list[str]:
    argv = shlex.split(raw)
    if not argv:
        raise ValueError("empty command")
    if owner == "edge":
        if argv[0] == "npm" and len(argv) >= 2 and argv[1] in {"ci", "run", "test"}:
            return argv
        if argv[0] == "node" and len(argv) >= 2 and argv[1].endswith(".mjs"):
            return argv
        raise ValueError("Edge supports only explicit npm ci/run/test and node *.mjs")
    if argv[0] != "cargo" or len(argv) < 2 or argv[1] not in {
        "fmt", "check", "clippy", "test", "build"
    }:
        raise ValueError("Rust owner supports cargo fmt/check/clippy/test/build")
    # `cargo fmt` has its own output and does not accept JSON cargo messages.
    if argv[1] in {"check", "clippy", "test", "build"} and not any(
        x.startswith("--message-format") for x in argv
    ):
        # Cargo args must precede `--` (test harness arguments).
        separator = argv.index("--") if "--" in argv else len(argv)
        argv.insert(separator, "--message-format=json")
    return argv


def parse_cargo_output(lines: Iterable[str]) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "cargo_artifacts": 0,
        "nonfresh_artifacts": 0,
        "fresh_artifacts": 0,
        "executable_artifacts": 0,
        "compiler_warnings": 0,
        "build_scripts": 0,
        "tests_passed": 0,
        "tests_failed": 0,
        "tests_ignored": 0,
        # These are *not* claims of exact rustc/linker invocations.
        "rustc_verbose_invocations": None,
        "linker_invocations_observed": None,
    }
    for line in lines:
        match = TEST_RESULT.search(line)
        if match:
            passed, failed, ignored, _, _ = map(int, match.groups())
            stats["tests_passed"] += passed
            stats["tests_failed"] += failed
            stats["tests_ignored"] += ignored
        if RUSTC_RUN.search(line):
            stats["rustc_verbose_invocations"] = (
                (stats["rustc_verbose_invocations"] or 0) + 1
            )
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        reason = event.get("reason")
        if reason == "compiler-artifact":
            stats["cargo_artifacts"] += 1
            stats["fresh_artifacts" if event.get("fresh") else "nonfresh_artifacts"] += 1
            if event.get("executable"):
                stats["executable_artifacts"] += 1
        elif reason == "build-script-executed":
            stats["build_scripts"] += 1
        elif reason == "compiler-message":
            if (event.get("message") or {}).get("level") == "warning":
                stats["compiler_warnings"] += 1
    return stats


def cargo_closure(workspace: Path, owner: str) -> dict[str, Any] | None:
    if owner == "edge" or shutil.which("cargo") is None:
        return None
    try:
        result = subprocess.run(
            ["cargo", "metadata", "--locked", "--format-version=1"],
            cwd=workspace, check=True, capture_output=True, text=True, timeout=120,
        )
        metadata = json.loads(result.stdout)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
        return {"error": "cargo metadata unavailable or failed"}
    packages = {p["id"]: p for p in metadata["packages"]}
    members = metadata.get("workspace_members", [])
    return {
        "workspace_members": sorted(packages[m]["name"] for m in members),
        "resolved_packages": len((metadata.get("resolve") or {}).get("nodes", [])),
        "member_count": len(members),
    }


def measure_one(workspace: Path, argv: list[str]) -> dict[str, Any]:
    start = time.monotonic_ns()
    with tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as out:
        with tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as err:
            try:
                result = subprocess.run(argv, cwd=workspace, stdout=out, stderr=err,
                                        check=False)
                exit_code = result.returncode
            except OSError:
                exit_code = 127
            elapsed = (time.monotonic_ns() - start) // 1_000_000
            out.seek(0)
            err.seek(0)
            stats = parse_cargo_output(chain(out, err))
    return {"command": argv, "elapsed_ms": elapsed, "exit_code": exit_code,
            "observations": stats}


def run(args: argparse.Namespace) -> int:
    workspace = args.workspace.resolve(strict=True)
    if not workspace.is_dir():
        raise ValueError("workspace must be an existing directory")
    if not args.commands:
        raise ValueError("at least one --command is required")
    commands = [allowed_command(c, args.owner) for c in args.commands]
    digest, count = source_fingerprint(workspace)
    start = time.monotonic_ns()
    sample = {
        "schema": "okx-s0-local-measure/v1",
        "name": args.name,
        "owner": args.owner,
        "workspace_source_sha256": digest,
        "source_file_count": count,
        "environment": environment(args.repository),
        "metadata": cargo_closure(workspace, args.owner),
        "commands": commands,
        "phases": [],
        "ready_ci_gate_proof": False,
        "scope": "local sequential measurements, not mandatory Ready CI",
    }
    for argv in commands:
        observation = measure_one(workspace, argv)
        sample["phases"].append(observation)
        if observation["exit_code"] != 0:
            break
    sample["total_elapsed_ms"] = (time.monotonic_ns() - start) // 1_000_000
    sample["all_commands_passed"] = (
        len(sample["phases"]) == len(commands)
        and all(p["exit_code"] == 0 for p in sample["phases"])
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(sample, indent=2, sort_keys=True) + "\n",
                           encoding="utf-8")
    print(f"{args.name}: {sample['total_elapsed_ms']} ms; "
          f"success={sample['all_commands_passed']}; {args.output}")
    return 0 if sample["all_commands_passed"] else 1


def compare_records(baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    problems = []
    for key in ("schema", "owner", "commands"):
        if baseline.get(key) != candidate.get(key):
            problems.append(f"different {key}")
    for key in ("platform", "machine", "rustc", "cargo", "node", "npm", "cpu_count", "build_inputs"):
        if (baseline.get("environment") or {}).get(key) != (
            candidate.get("environment") or {}
        ).get(key):
            problems.append(f"different environment.{key}")
    if baseline.get("workspace_source_sha256") == candidate.get("workspace_source_sha256"):
        problems.append("source fingerprints identical: no meaningful source diff proven")
    if not baseline.get("all_commands_passed") or not candidate.get("all_commands_passed"):
        problems.append("one or more commands failed")
    before = baseline.get("phases", [])
    after = candidate.get("phases", [])
    phases = []
    for i in range(min(len(before), len(after))):
        phases.append({
            "command": before[i].get("command"),
            "baseline_ms": before[i].get("elapsed_ms"),
            "candidate_ms": after[i].get("elapsed_ms"),
            "delta_ms": after[i].get("elapsed_ms", 0) - before[i].get("elapsed_ms", 0),
            "baseline_cargo_nonfresh_artifacts": before[i].get("observations", {}).get("nonfresh_artifacts"),
            "candidate_cargo_nonfresh_artifacts": after[i].get("observations", {}).get("nonfresh_artifacts"),
        })
    return {
        "schema": "okx-s0-local-compare/v1",
        "comparable": not problems,
        "invalid_reasons": problems,
        "baseline_name": baseline.get("name"),
        "candidate_name": candidate.get("name"),
        "total_delta_ms": candidate.get("total_elapsed_ms", 0) - baseline.get("total_elapsed_ms", 0),
        "phases": phases,
        "ready_ci_gate_proof": False,
    }


def compare(args: argparse.Namespace) -> int:
    a = json.loads(args.baseline.read_text(encoding="utf-8"))
    b = json.loads(args.candidate.read_text(encoding="utf-8"))
    output = compare_records(a, b)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n",
                               encoding="utf-8")
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0 if output["comparable"] else 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)
    run_cmd = subparsers.add_parser("run", help="measure owner commands sequentially")
    run_cmd.add_argument("--workspace", type=Path, required=True)
    run_cmd.add_argument("--owner", choices=("engine", "research", "control", "edge"), required=True)
    run_cmd.add_argument("--name", required=True)
    run_cmd.add_argument("--output", type=Path, required=True)
    run_cmd.add_argument("--repository", type=Path, help="Git checkout for provenance")
    run_cmd.add_argument("--command", dest="commands", action="append", required=True)
    run_cmd.set_defaults(function=run)
    compare_cmd = subparsers.add_parser("compare", help="compare two recorded samples")
    compare_cmd.add_argument("--baseline", type=Path, required=True)
    compare_cmd.add_argument("--candidate", type=Path, required=True)
    compare_cmd.add_argument("--output", type=Path)
    compare_cmd.set_defaults(function=compare)
    args = parser.parse_args(argv)
    try:
        return args.function(args)
    except (ValueError, OSError, json.JSONDecodeError) as error:
        print(f"measurement error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())

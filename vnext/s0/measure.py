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
RUNNING_COMMAND = re.compile(r"^\s*Running `(?P<command>.+)`\s*$")
# Match a standalone executable token, not `RUSTC=/path/rustc` (an env var
# present even when Cargo is launching a build script rather than rustc).
RUSTC_RUN = re.compile(r"(?:^|\s)['\"]?(?:[^=\s'\"]+[/\\])?rustc(?:\.exe)?['\"]?(?=\s|$)")
RUSTDOC_RUN = re.compile(r"(?:^|\s)['\"]?(?:[^=\s'\"]+[/\\])?rustdoc(?:\.exe)?['\"]?(?=\s|$)")
BUILD_INPUTS = (
    "RUSTUP_TOOLCHAIN", "RUSTFLAGS", "RUSTDOCFLAGS",
    "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_TARGET",
    "CARGO_TARGET_DIR", "CARGO_INCREMENTAL", "CARGO_BUILD_JOBS",
    "RUSTC", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "RUSTDOC",
    "CC", "CXX", "CI", "RUNNER_OS", "ImageVersion",
)


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
        # Explicit allowlist of *digests*, not raw environment values. Build flags
        # and tool paths occasionally contain sensitive host-specific material.
        "build_inputs": {
            name: hashlib.sha256(os.fsencode(os.environ[name])).hexdigest()
            for name in BUILD_INPUTS if name in os.environ
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
    if argv[1] in {"check", "clippy", "test", "build"}:
        if any(flag in argv for flag in ("-q", "--quiet")):
            raise ValueError("Cargo quiet output hides mandatory compiler evidence")
        # Cargo's JSON artifact events are not rustc process counts. `-vv`
        # additionally logs Cargo-started rustc/rustdoc invocations. It does
        # not reveal the linker's subprocesses inside rustc.
        if not any(flag.startswith("-vv") for flag in argv):
            argv.insert(2, "-vv")
    # `cargo fmt` has its own output and does not accept JSON cargo messages.
    if argv[1] in {"check", "clippy", "test", "build"} and not any(
        x.startswith("--message-format") for x in argv
    ):
        # Cargo args must precede `--` (test harness arguments).
        separator = argv.index("--") if "--" in argv else len(argv)
        argv.insert(separator, "--message-format=json")
    return argv


def parse_cargo_output(lines: Iterable[str], *, verbose_evidence: bool = False) -> dict[str, Any]:
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
        "rustc_verbose_invocations": 0 if verbose_evidence else None,
        "rustdoc_verbose_invocations": 0 if verbose_evidence else None,
        "linker_invocations_observed": None,
        "linker_observation": "unknown: rustc does not report subprocess linker events",
    }
    for line in lines:
        match = TEST_RESULT.search(line)
        if match:
            passed, failed, ignored, _, _ = map(int, match.groups())
            stats["tests_passed"] += passed
            stats["tests_failed"] += failed
            stats["tests_ignored"] += ignored
        invocation = RUNNING_COMMAND.match(line)
        if invocation:
            command = invocation.group("command")
            if RUSTC_RUN.search(command):
                stats["rustc_verbose_invocations"] = (
                    (stats["rustc_verbose_invocations"] or 0) + 1
                )
            elif RUSTDOC_RUN.search(command):
                stats["rustdoc_verbose_invocations"] = (
                    (stats["rustdoc_verbose_invocations"] or 0) + 1
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
            stats = parse_cargo_output(
                chain(out, err),
                verbose_evidence=(argv[0] == "cargo" and any(
                    flag in argv for flag in ("-vv", "--verbose")
                )),
            )
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
    if (baseline.get("schema") != "okx-s0-local-measure/v1"
            or candidate.get("schema") != "okx-s0-local-measure/v1"):
        problems.append("unsupported measurement schema")
    if baseline.get("owner") not in ("engine", "research", "control", "edge"):
        problems.append("unsupported owner")
    for key in ("schema", "owner", "commands"):
        if baseline.get(key) != candidate.get(key):
            problems.append(f"different {key}")
    for key in ("platform", "machine", "rustc", "cargo", "node", "npm", "cpu_count", "build_inputs"):
        before_env = baseline.get("environment")
        after_env = candidate.get("environment")
        if not isinstance(before_env, dict) or not isinstance(after_env, dict):
            problems.append("invalid environment")
            break
        if before_env.get(key) != after_env.get(key):
            problems.append(f"different environment.{key}")
    env = baseline.get("environment")
    env = env if isinstance(env, dict) else {}
    required_tools = ("node", "npm") if baseline.get("owner") == "edge" else ("cargo", "rustc")
    for key in ("platform", "machine", *required_tools):
        if not env.get(key):
            problems.append(f"missing environment.{key}")
    if baseline.get("workspace_source_sha256") == candidate.get("workspace_source_sha256"):
        problems.append("source fingerprints identical: no meaningful source diff proven")
    if not baseline.get("all_commands_passed") or not candidate.get("all_commands_passed"):
        problems.append("one or more commands failed")
    before = baseline.get("phases", [])
    after = candidate.get("phases", [])
    if not isinstance(before, list) or not isinstance(after, list):
        problems.append("missing phase arrays")
        before, after = [], []
    commands = baseline.get("commands", [])
    if not isinstance(commands, list):
        problems.append("invalid command array")
        commands = []
    if len(before) != len(after) or len(before) != len(commands):
        problems.append("phase count does not match command count")
    phases = []
    for i in range(min(len(before), len(after))):
        if not isinstance(before[i], dict) or not isinstance(after[i], dict):
            problems.append(f"invalid phase object {i}")
            continue
        if (before[i].get("command") != after[i].get("command")
                or i >= len(commands) or before[i].get("command") != commands[i]):
            problems.append(f"different phase command {i}")
        if before[i].get("exit_code") != 0 or after[i].get("exit_code") != 0:
            problems.append(f"failed phase {i}")
        before_ms = before[i].get("elapsed_ms")
        after_ms = after[i].get("elapsed_ms")
        if (not isinstance(before_ms, int) or before_ms < 0
                or not isinstance(after_ms, int) or after_ms < 0):
            problems.append(f"invalid duration in phase {i}")
            continue
        before_observations = before[i].get("observations")
        after_observations = after[i].get("observations")
        if (not isinstance(before_observations, dict)
                or not isinstance(after_observations, dict)):
            problems.append(f"invalid observations in phase {i}")
            continue
        phases.append({
            "command": before[i].get("command"),
            "baseline_ms": before_ms,
            "candidate_ms": after_ms,
            "delta_ms": after_ms - before_ms,
            "baseline_cargo_nonfresh_artifacts": before_observations.get("nonfresh_artifacts"),
            "candidate_cargo_nonfresh_artifacts": after_observations.get("nonfresh_artifacts"),
            "baseline_rustc_invocations": before_observations.get("rustc_verbose_invocations"),
            "candidate_rustc_invocations": after_observations.get("rustc_verbose_invocations"),
            "baseline_linker_invocations": before_observations.get("linker_invocations_observed"),
            "candidate_linker_invocations": after_observations.get("linker_invocations_observed"),
        })
    before_total = baseline.get("total_elapsed_ms")
    after_total = candidate.get("total_elapsed_ms")
    if (not isinstance(before_total, int) or before_total < 0
            or not isinstance(after_total, int) or after_total < 0):
        problems.append("invalid total duration")
        total_delta = None
    else:
        total_delta = after_total - before_total
    return {
        "schema": "okx-s0-local-compare/v1",
        "comparable": not problems,
        "invalid_reasons": problems,
        "baseline_name": baseline.get("name"),
        "candidate_name": candidate.get("name"),
        "total_delta_ms": total_delta,
        "phases": phases,
        "ready_ci_gate_proof": False,
        "semantic_equivalence_proven": False,
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

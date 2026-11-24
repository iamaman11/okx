#!/usr/bin/env python3
"""S0: compare two *real* layouts of the existing Engine/SQLite code.

Only the temporary copies are transformed. Nothing in the source checkout or
the installed financial SQLite store is modified. The venue is not contacted:
the existing sql_handoff test stops at durable PREPARED.

The alternative is deliberately small: compact embeds the typed command module
in the existing finance crate; split leaves it in its own crate. The entire
financial SQL/RiskBook implementation is unchanged in both variants.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time

from measure import parse_cargo_output, source_fingerprint


PROBE = r'''

// S0 source-change probe: ambiguous identifiers must not become trusted.
#[cfg(test)]
mod s0_topology_probe {
    #[test]
    fn rejects_ambiguous_terminal_segment() {
        let ambiguous = r#"{"kind":"manual","account":"okx:demo:account1","instrument":"BTC-USDT-SWAP","operation":"s0-probe.","target":{"kind":"flat"},"observed_at_ms":100,"expires_at_ms":200}"#;
        assert!(super::validate_wire_command(ambiguous).is_err());
    }
}
'''


def cargo_binary() -> str:
    executable = shutil.which("cargo")
    if executable is None:
        candidate = Path.home() / ".cargo" / "bin" / "cargo"
        if candidate.is_file():
            executable = str(candidate)
    if executable is None:
        raise RuntimeError("cargo is required; use the pinned Rust 1.100.0 toolchain")
    return executable


def copy_owner(source: Path, destination: Path) -> None:
    if source.name != "engine" or not (source / "Cargo.lock").is_file():
        raise ValueError("source must be the Engine owner workspace with its lockfile")
    shutil.copytree(
        source,
        destination,
        ignore=shutil.ignore_patterns("target", ".git", "node_modules", ".venv"),
    )


def make_compact(workspace: Path, cargo: str) -> None:
    """Keep the real financial owner, embed only its thin command boundary."""
    command_package = workspace / "crates" / "okx-trading-engine"
    finance_package = workspace / "crates" / "okx-trade-execution"
    root_manifest = workspace / "Cargo.toml"
    manifest = root_manifest.read_text(encoding="utf-8")
    needle = ', "crates/okx-trading-engine"'
    if manifest.count(needle) != 1:
        raise ValueError("unexpected Engine workspace membership")
    root_manifest.write_text(manifest.replace(needle, ""), encoding="utf-8")

    finance_lib = finance_package / "src" / "lib.rs"
    finance_lib.write_text(
        finance_lib.read_text(encoding="utf-8")
        + '\n#[path = "trading_engine.rs"]\npub mod trading_engine;\n',
        encoding="utf-8",
    )
    shutil.copy2(command_package / "src" / "lib.rs", finance_package / "src" / "trading_engine.rs")
    sql_test = (command_package / "tests" / "sql_handoff.rs").read_text(encoding="utf-8")
    needle = "use okx_trading_engine::{"
    if sql_test.count(needle) != 1:
        raise ValueError("SQL handoff has changed; cannot translate its public import")
    tests = finance_package / "tests"
    tests.mkdir(exist_ok=True)
    (tests / "sql_handoff.rs").write_text(
        sql_test.replace(needle, "use okx_trade_execution::trading_engine::{"),
        encoding="utf-8",
    )
    shutil.rmtree(command_package)
    # This variant has a different workspace member list. Regenerate the lockfile
    # from the already pinned dependency versions in an isolated, offline copy.
    result = subprocess.run(
        [cargo, "generate-lockfile", "--offline"], cwd=workspace,
        capture_output=True, text=True, check=False,
    )
    if result.returncode:
        raise RuntimeError(f"compact Cargo.lock: {result.stderr[-2000:]}")


def typed_boundary_path(workspace: Path, variant: str) -> Path:
    if variant == "compact":
        return workspace / "crates/okx-trade-execution/src/trading_engine.rs"
    return workspace / "crates/okx-trading-engine/src/lib.rs"


def apply_identical_source_diff(workspace: Path, variant: str) -> None:
    source = typed_boundary_path(workspace, variant)
    content = source.read_text(encoding="utf-8")
    if "s0_topology_probe" in content or "pub fn validate_wire_command" not in content:
        raise ValueError("unexpected typed command source before probe")
    # A genuine production behavior change, rather than merely compiling a new
    # test: reject a trailing segment separator in critical identifiers. This
    # source change stays in this disposable experiment, not the application.
    old = "if raw.is_empty()\n        || raw.len() > max_len"
    new = "if raw.is_empty()\n        || raw.ends_with('.')\n        || raw.len() > max_len"
    if content.count(old) != 1:
        raise ValueError("typed identifier implementation changed")
    source.write_text(content.replace(old, new) + PROBE, encoding="utf-8")


def measure_cargo(cargo: str, workspace: Path, args: list[str]) -> dict:
    start = time.perf_counter_ns()
    result = subprocess.run(
        [cargo, *args, "--message-format=json", "-vv"],
        cwd=workspace, capture_output=True, text=True, check=False,
    )
    elapsed_ms = (time.perf_counter_ns() - start) // 1_000_000
    observations = parse_cargo_output(
        [*result.stdout.splitlines(keepends=True), *result.stderr.splitlines(keepends=True)],
        verbose_evidence=True,
    )
    observations["passed_test_names"] = re.findall(
        r"(?:^|\n)test\s+([^\n]+?)\s+\.\.\.\s+ok", result.stdout + result.stderr
    )
    # The real SQLite oracle must have run, not merely been compiled.
    observations["sqlite_handoff_executed"] = bool(
        re.search(r"typed_manual_target_reaches_original_sql_writer_and_survives_reopen_without_send\s+\.\.\.\s+ok", result.stdout + result.stderr)
    )
    observations["topology_probe_executed"] = bool(
        re.search(r"rejects_ambiguous_terminal_segment\s+\.\.\.\s+ok", result.stdout + result.stderr)
    )
    if result.returncode:
        raise RuntimeError(
            f"Cargo failed ({' '.join(args)}), exit={result.returncode}: "
            + result.stderr[-4000:] + result.stdout[-2000:]
        )
    return {
        "command": ["cargo", *args, "--message-format=json", "-vv"],
        "elapsed_ms": elapsed_ms,
        "observations": observations,
    }


def run_variant(cargo: str, workspace: Path, name: str) -> dict:
    workspace_hash_before, files_before = source_fingerprint(workspace)
    # The full source and test harnesses are compiled, but execute only the
    # selected real financial oracles: large unrelated finance tests can fill
    # /tmp when both topology experiments overlap on one runner.
    handoff_name = "typed_manual_target_reaches_original_sql_writer_and_survives_reopen_without_send"
    lost_reply_names = [
        "risk_book_contract_fresh_owner_reserve_commit_lost_reply_exact_readback_once_send",
        "risk_book_contract_fresh_owner_begin_post_commit_lost_reply_no_second_claim_or_send",
    ]
    cold_test = measure_cargo(cargo, workspace, ["test", "--locked", "--workspace", handoff_name])
    if not cold_test["observations"]["sqlite_handoff_executed"]:
        raise RuntimeError(f"{name}: durable SQLite oracle was not observed")
    fake_venue_witnesses = []
    for witness in lost_reply_names:
        fake_venue_witnesses.append(
            measure_cargo(cargo, workspace, ["test", "--locked", "-p", "okx-trade-execution", witness])
        )
        if not any(
            witness in test
            for test in fake_venue_witnesses[-1]["observations"]["passed_test_names"]
        ):
            raise RuntimeError(f"{name}: missing fake venue witness {witness}")
    cold_release = measure_cargo(cargo, workspace, ["build", "--locked", "--release", "--workspace"])
    apply_identical_source_diff(workspace, name)
    workspace_hash_after, files_after = source_fingerprint(workspace)
    edited_test = measure_cargo(cargo, workspace, ["test", "--locked", "--workspace", handoff_name])
    edited_probe = measure_cargo(cargo, workspace, ["test", "--locked", "--workspace", "rejects_ambiguous_terminal_segment"])
    edited_release = measure_cargo(cargo, workspace, ["build", "--locked", "--release", "--workspace"])
    if not edited_test["observations"]["sqlite_handoff_executed"]:
        raise RuntimeError(f"{name}: durable SQLite oracle disappeared after source diff")
    if not edited_probe["observations"]["topology_probe_executed"]:
        raise RuntimeError(f"{name}: source diff probe was not executed")
    return {
        "name": name,
        "workspace_members": 2 if name == "compact" else 3,
        "source_before": workspace_hash_before,
        "source_after": workspace_hash_after,
        "source_file_counts": [files_before, files_after],
        "cold_test": cold_test,
        "fake_venue_lost_ack_witnesses": fake_venue_witnesses,
        "cold_release": cold_release,
        "edited_test": edited_test,
        "edited_probe": edited_probe,
        "edited_release": edited_release,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", type=Path, required=True, help="vnext/engine")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--temp-root", type=Path, help="parent for isolated temporary workspaces")
    options = parser.parse_args()
    cargo = cargo_binary()
    source = options.engine.resolve(strict=True)
    # Both variants are fresh, self-contained owner copies. They share only the
    # global registry download cache; no sibling project source/target or build.
    with tempfile.TemporaryDirectory(prefix="okx-s0-engine-topology-", dir=options.temp_root) as directory:
        root = Path(directory)
        split = root / "split" / "engine"
        compact = root / "compact" / "engine"
        split.parent.mkdir()
        compact.parent.mkdir()
        copy_owner(source, split)
        copy_owner(source, compact)
        make_compact(compact, cargo)
        variants = [run_variant(cargo, compact, "compact"), run_variant(cargo, split, "split")]
    report = {
        "schema": "okx-vnext-s0-engine-topology/v1",
        "comparability": "same Rust toolchain/host/financial source; isolated target directories",
        "source_diff": "identical production change to identifier validation + its new test",
        "durable_scenario": "actual AccountStore SQLite/WAL PREPARED/idempotent/reopen + existing fake venue once-send/lost-ACK witnesses",
        "variants": variants,
        "limitations": [
            "This is a local topology experiment, not the protected Ready CI gate.",
            "Fake-send/lost-ACK SQL witnesses run separately from typed PREPARE handoff; no single end-to-end typed command to venue is proven.",
            "No actual HTTP exchange emulator, real trading authority or full venue reconciliation is exercised.",
            "Cargo verbose rustc commands are measured; actual linker invocations are not separately instrumented.",
            "Only a representative typed-command production diff is measured; finance-SQL edits and feature growth are not measured.",
            "Separate runner-queue/provenance/artifact, Windows and warm p50/p95 proofs are required by #569.",
        ],
        "ready_ci_gate_proof": False,
    }
    options.output.parent.mkdir(parents=True, exist_ok=True)
    options.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Engine topology A/B written to {options.output}")
    for variant in variants:
        print(
            variant["name"],
            "edit test ms", variant["edited_test"]["elapsed_ms"],
            "edit rustc", variant["edited_test"]["observations"]["rustc_verbose_invocations"],
            "edit release ms", variant["edited_release"]["elapsed_ms"],
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Run the identical causal numerical Research oracle on both S0 topologies.

Pass a directory exported by research_topology.py --output-dir. This tests the
real Rust replay executable, including missing/late/duplicate source inputs.
It neither fetches network data nor places trades.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys


VALID = {
    "timeframe_ms": 60_000,
    "graph": {
        "version": "okx.market.composition/v1",
        "steps": [{"id": "average", "op": "rolling_mean", "input": "close", "period": 2}],
        "result_steps": ["average"],
    },
    "bars": [
        {"timestamp_ms": index * 60_000, "available_at_ms": (index + 1) * 60_000,
         "close": float(index * 3)}
        for index in range(1, 7)
    ],
}
EXPERIMENTS = {
    "A": (2, [None, 4.5, 7.5, 10.5, 13.5, 16.5]),
    "B": (3, [None, None, 6.0, 9.0, 12.0, 15.0]),
    "C": (6, [None, None, None, None, None, 10.5]),
}


def experiment(name: str) -> dict:
    period, _ = EXPERIMENTS[name]
    request = copy.deepcopy(VALID)
    request["graph"]["steps"][0]["period"] = period
    return request


def source_digest(payload: dict) -> str:
    # The *recorded* source alone has a stable digest, independent of the plan.
    canonical = json.dumps(payload["bars"], sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def expected_series(values: list) -> list[dict]:
    return [
        {"timestamp_ms": bar["timestamp_ms"], "available_at_ms": bar["available_at_ms"],
         "values": {"average": [value, "price"]}}
        for bar, value in zip(VALID["bars"], values, strict=True)
    ]


def command(workspace: Path, payload: dict) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["cargo", "run", "--locked", "--quiet", "--bin", "okx-research-replay"],
        input=json.dumps(payload, separators=(",", ":")),
        text=True,
        capture_output=True,
        cwd=workspace,
        timeout=180,
        check=False,
    )


def run(workspace_root: Path) -> dict[str, object]:
    if shutil.which("cargo") is None:
        raise ValueError("Cargo must be on PATH")
    results: dict[str, object] = {}
    for topology, members in (("compact", 1), ("split", 2)):
        workspace = (workspace_root / topology).resolve(strict=True)
        metadata = subprocess.run(
            ["cargo", "metadata", "--locked", "--format-version=1", "--no-deps"],
            cwd=workspace, text=True, capture_output=True, check=True, timeout=30,
        )
        observed = len(json.loads(metadata.stdout)["workspace_members"])
        if observed != members:
            raise AssertionError(f"{topology}: expected {members} Cargo members, got {observed}")

        numerical: dict[str, float] = {}
        digest = source_digest(VALID)
        for name, (_, values) in EXPERIMENTS.items():
            payload = experiment(name)
            if source_digest(payload) != digest:
                raise AssertionError(f"{topology}: experiment {name} changed source bytes")
            passed = command(workspace, payload)
            repeated = command(workspace, payload)
            if passed.returncode or repeated.returncode:
                raise AssertionError(f"{topology}/{name}: valid replay failed: {passed.stderr[-1500:]}")
            if passed.stdout != repeated.stdout:
                raise AssertionError(f"{topology}/{name}: replay is not deterministic")
            output = json.loads(passed.stdout)
            if output != expected_series(values):
                raise AssertionError(f"{topology}/{name}: numerical oracle mismatch: {output!r}")
            numerical[name] = values[-1]
        if len(set(numerical.values())) != 3:
            raise AssertionError(f"{topology}: hypotheses are not distinguishable")

        invalid_cases: dict[str, dict] = {}
        early = copy.deepcopy(VALID)
        early["bars"][0]["available_at_ms"] -= 1
        invalid_cases["future_leak"] = early
        duplicate = copy.deepcopy(VALID)
        duplicate["bars"][1]["timestamp_ms"] = 60_000
        invalid_cases["duplicate_bar"] = duplicate
        gap = copy.deepcopy(VALID)
        gap["bars"][1]["timestamp_ms"] = 180_000
        gap["bars"][1]["available_at_ms"] = 240_000
        invalid_cases["missing_bar"] = gap
        forged = copy.deepcopy(VALID)
        forged["unverified_source"] = True
        invalid_cases["unknown_wire_field"] = forged
        for name, payload in invalid_cases.items():
            rejected = command(workspace, payload)
            if rejected.returncode == 0:
                raise AssertionError(f"{topology}: incorrectly accepted {name}")
            if rejected.stdout.strip():
                raise AssertionError(f"{topology}: {name} leaked partial replay results")

        results[topology] = {
            "cargo_members": members,
            "positive": len(EXPERIMENTS),
            "numerical_oracles": numerical,
            "immutable_recorded_source_sha256": digest,
            "same_executable_for_all_plans": True,
            "negative": list(invalid_cases),
            "source": "recorded, closed 60s bars",
        }
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace-root", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = run(args.workspace_root)
    except (AssertionError, ValueError, OSError, subprocess.SubprocessError) as error:
        print(f"Research S0 oracle FAIL: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()

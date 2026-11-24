#!/usr/bin/env python3
"""Apply one realistic Research-only source change to warm S0 A/B exports.

Prepare compact and split directories first with research_topology.py. Measure
the baseline in each directory, run this tool, and repeat the SAME Cargo
commands while retaining their own target caches. The two probes represent
independent source-change classes and MUST use separate export directories.

They are scoped benchmark experiments, not accepted product changes:
* kernel-only lowers the query-plan complexity budget (validation policy);
* adapter-only lowers the public HTTP response size cap (ingestion policy).

The exact original source checksums pin these probes to vNext #575. A changed
source revision MUST cause a refusal until the experiment is reviewed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys


PROBES = {
    "kernel-only": {
        "source": "crates/okx-strategy-core/src/query_plan.rs",
        "compact": "src/query_plan.rs",
        "split": "crates/okx-strategy-core/src/query_plan.rs",
        "original_sha256": "18043bea605f6a4738e2b70364ce595bd229fdd5f065e17cd132e173b64f6ac2",
        "before": "const MAX_COMPOSITION_WORK_UNITS: u64 = 64_000_000;",
        "after": "const MAX_COMPOSITION_WORK_UNITS: u64 = 48_000_000;",
    },
    "adapter-only": {
        "source": "src/adapters/okx_candles.rs",
        "compact": "src/adapters/okx_candles.rs",
        "split": "src/adapters/okx_candles.rs",
        "original_sha256": "b09df4188075d112aa3dbca172dcefbd8759cfa05aa9016bf3ecc56adf3c5c18",
        "before": "const MAX_BODY: usize = 128 * 1024;",
        "after": "const MAX_BODY: usize = 96 * 1024;",
    },
}


def checksum(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def apply_probe(source: Path, root: Path, probe: str) -> dict:
    if probe not in PROBES:
        raise ValueError("unknown source class")
    config = PROBES[probe]
    source = source.resolve(strict=True)
    root = root.resolve(strict=True)
    if root.is_relative_to(source) or source.is_relative_to(root):
        raise ValueError("benchmark exports must be outside the canonical Research source")
    manifest = root / "research-diff.json"
    if manifest.exists():
        raise ValueError("one source class per export; use a fresh A/B directory")
    original = (source / config["source"]).read_bytes()
    if checksum(original) != config["original_sha256"]:
        raise ValueError("unrecognized Research source revision: review source diff before measuring")
    before = config["before"].encode()
    after = config["after"].encode()
    if before == after or original.count(before) != 1:
        raise ValueError("probe anchor changed")

    # Fully validate BOTH variants before changing either, so a stale or
    # partially modified comparison never becomes a misleading measurement.
    planned: list[tuple[Path, bytes, bytes]] = []
    for topology in ("compact", "split"):
        path = root / topology / config[topology]
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(root) or resolved.is_relative_to(source):
            raise ValueError(f"{topology} benchmark path escapes the export root")
        current = path.read_bytes()
        if current != original:
            raise ValueError(f"{topology} source does not match pinned baseline")
        updated = current.replace(before, after)
        if updated == current:
            raise ValueError("probe produced no source change")
        planned.append((path, current, updated))

    report = {
        "schema": "okx-s0-research-source-diff/v1",
        "source_class": probe,
        "source_path": config["source"],
        "source_sha256": checksum(original),
        "benchmark_probe_not_production_change": True,
        "variants": [],
    }
    for path, current, updated in planned:
        path.write_bytes(updated)
        report["variants"].append({
            "path": path.relative_to(root).as_posix(),
            "before_sha256": checksum(current),
            "after_sha256": checksum(updated),
        })
    manifest.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--workspace-root", type=Path, required=True)
    parser.add_argument("--probe", choices=tuple(PROBES), required=True)
    args = parser.parse_args()
    try:
        report = apply_probe(args.source, args.workspace_root, args.probe)
    except (OSError, ValueError) as error:
        print(f"Research S0 diff REFUSED: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()

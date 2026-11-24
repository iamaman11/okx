#!/usr/bin/env python3
"""Audit semantic ownership of legacy files whose migration action is 'split'.

This is an inventory check, not a claim that the migration has been completed.
Use --require-complete only for the eventual S0 acceptance gate.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent
LEGACY = ROOT / "legacy-files.tsv"
DECISIONS = ROOT / "split-symbols.tsv"
HEADER = (
    "source",
    "symbol",
    "owner",
    "destination",
    "state_writer",
    "oracle",
    "last_legacy_consumer",
    "status",
)
OWNERS = {"engine", "research", "control", "edge"}
STATUS = {"planned", "pending", "migrated"}


@dataclass(frozen=True)
class Audit:
    expected: int
    covered: int
    rows: int
    pending: int
    planned: int
    migrated: int
    problems: tuple[str, ...]

    @property
    def complete(self) -> bool:
        return not self.problems and self.expected == self.covered and self.rows == self.migrated


def read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file, delimiter="\t"))


def audit(legacy: list[dict[str, str]], decisions: list[dict[str, str]]) -> Audit:
    problems: list[str] = []
    sources = {row["path"] for row in legacy if row.get("action") == "split"}
    if len(sources) != sum(row.get("action") == "split" for row in legacy):
        problems.append("duplicate split source in legacy inventory")
    seen: set[tuple[str, str]] = set()
    covered: set[str] = set()
    counts = {"planned": 0, "pending": 0, "migrated": 0}
    for number, row in enumerate(decisions, start=2):
        source = row.get("source") or ""
        symbol = row.get("symbol") or ""
        owner = row.get("owner") or ""
        destination = row.get("destination") or ""
        state = row.get("state_writer") or ""
        oracle = row.get("oracle") or ""
        consumer = row.get("last_legacy_consumer") or ""
        status = row.get("status") or ""
        label = f"line {number} ({source}::{symbol})"
        if source not in sources:
            problems.append(f"{label}: missing or non-split source")
        else:
            covered.add(source)
        if not symbol or symbol == "*":
            problems.append(f"{label}: symbol must be specific, not a whole-file wildcard")
        pair = (source, symbol)
        if pair in seen:
            problems.append(f"{label}: symbol assigned more than once")
        seen.add(pair)
        if owner not in OWNERS:
            problems.append(f"{label}: invalid owner {owner!r}")
        if not destination.startswith(f"vnext/{owner}/") or ".." in Path(destination).parts:
            problems.append(f"{label}: destination must remain in owner's root")
        if state not in OWNERS | {"none", "unresolved"}:
            problems.append(f"{label}: invalid state writer")
        if not oracle or not consumer:
            problems.append(f"{label}: oracle and last consumer must be explicit")
        if status not in STATUS:
            problems.append(f"{label}: unknown status")
        else:
            counts[status] += 1
        if status == "migrated" and (
            state == "unresolved"
            or consumer == "UNVERIFIED"
            or symbol.startswith("AUDIT:")
        ):
            problems.append(f"{label}: cannot claim migrated with unresolved ownership/consumer")
    missing = sorted(sources - covered)
    if missing:
        problems.append(f"{len(missing)} split paths are not inventoried: {', '.join(missing)}")
    return Audit(
        expected=len(sources),
        covered=len(covered),
        rows=len(decisions),
        pending=counts["pending"],
        planned=counts["planned"],
        migrated=counts["migrated"],
        problems=tuple(problems),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--require-complete",
        action="store_true",
        help="fail if any symbol split is not migrated and verified",
    )
    args = parser.parse_args()
    with DECISIONS.open("r", encoding="utf-8") as file:
        reader = csv.DictReader(file, delimiter="\t")
        if tuple(reader.fieldnames or ()) != HEADER:
            print(f"FAIL: expected TSV columns {HEADER}", file=sys.stderr)
            return 1
        decisions = list(reader)
    result = audit(read_tsv(LEGACY), decisions)
    print(
        f"split paths={result.covered}/{result.expected}; "
        f"symbols={result.rows}; migrated={result.migrated}; "
        f"planned={result.planned}; pending={result.pending}"
    )
    if result.problems:
        for problem in result.problems:
            print(f"FAIL: {problem}", file=sys.stderr)
        return 1
    if not result.complete:
        print("IN_PROGRESS: coverage inventoried, semantic migration NOT_PROVEN")
        return 2 if args.require_complete else 0
    print("PASS: all mapped symbols migrated with verified ownership")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

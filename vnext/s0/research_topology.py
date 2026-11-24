#!/usr/bin/env python3
"""Export two genuinely different, isolated Research Cargo compilation graphs.

`split` preserves the production Research binary + pure strategy-core package.
`compact` embeds those exact same strategy-core source files in the binary
crate, without rewriting their algorithms. Neither export is a deployment.

Use the same source revision and replay fixture for both exports. To measure a
real functional change, repeat the exports on a *different* source revision,
then compare before/after invalidation in each topology. The script does not
invent a benchmark change or silently modify financial semantics.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import shutil


CORE = "okx-strategy-core"
CORE_ROOT = Path("crates") / CORE
CORE_DEP = 'okx-strategy-core = { path = "crates/okx-strategy-core" }'
MEMBERS = 'members = ["crates/okx-strategy-core"]'


def export(source: Path, destination: Path, topology: str) -> None:
    source = source.resolve(strict=True)
    destination = destination.resolve(strict=False)
    if topology not in {"compact", "split"}:
        raise ValueError("topology must be compact or split")
    if not (source / "Cargo.lock").is_file() or not (source / CORE_ROOT / "src/lib.rs").is_file():
        raise ValueError("source is not a complete Research workspace")
    if destination == source or source.is_relative_to(destination) or destination.is_relative_to(source):
        raise ValueError("exports must live outside source")
    if destination.exists():
        raise FileExistsError(f"refusing to overwrite {destination}")

    # No source from an Engine, Control or Edge sibling is copied or read.
    shutil.copytree(source, destination, ignore=shutil.ignore_patterns("target", ".git", ".venv", "node_modules"))
    if topology == "split":
        return

    root = destination / "src"
    core = destination / CORE_ROOT
    manifest = (destination / "Cargo.toml").read_text(encoding="utf-8")
    if manifest.count(CORE_DEP) != 1 or manifest.count(MEMBERS) != 1:
        raise ValueError("Research manifest changed: compact export needs review")
    manifest = manifest.replace(CORE_DEP + "\n", "").replace(MEMBERS, "members = []")
    (destination / "Cargo.toml").write_text(manifest, encoding="utf-8")
    for item in (core / "src").glob("*.rs"):
        if item.name == "lib.rs":
            continue
        if (root / item.name).exists():
            raise ValueError(f"module name collides: {item.name}")
        shutil.copy2(item, root / item.name)
    lib = (core / "src/lib.rs").read_text(encoding="utf-8")
    modules = "\n".join(line for line in lib.splitlines() if line.startswith("pub mod "))
    if not modules or "pub mod query_plan;" not in modules:
        raise ValueError("unrecognized core module declarations")
    main = root / "main.rs"
    main.write_text(
        "#![forbid(unsafe_code)]\nextern crate self as okx_strategy_core;\n"
        + modules + "\n\n" + main.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    # Compact export really has one Cargo package. Keep all pinned third-party
    # dependency versions byte-for-byte, dropping only the removed local crate.
    lockfile = destination / "Cargo.lock"
    lock = lockfile.read_text(encoding="utf-8")
    if lock.count(' "okx-strategy-core",\n') != 1:
        raise ValueError("Research lockfile changed: local dependency missing")
    lock = lock.replace(' "okx-strategy-core",\n', "")
    lock, removed = re.subn(
        r'(?m)^\[\[package\]\]\nname = "okx-strategy-core"\n.*?(?=^\[\[package\]\]|\Z)',
        "",
        lock,
        flags=re.DOTALL,
    )
    if removed != 1:
        raise ValueError("Research lockfile changed: local package missing")
    lockfile.write_text(lock, encoding="utf-8")
    shutil.rmtree(core)
    (destination / "crates").rmdir()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    output_group = parser.add_mutually_exclusive_group(required=True)
    output_group.add_argument("--output", type=Path)
    output_group.add_argument("--output-dir", type=Path)
    parser.add_argument("--topology", choices=("compact", "split"))
    args = parser.parse_args()
    if args.output_dir:
        if args.topology:
            parser.error("--output-dir exports both topologies; omit --topology")
        if args.output_dir.exists():
            parser.error("--output-dir must not exist")
        args.output_dir.mkdir(parents=True)
        for topology in ("compact", "split"):
            output = args.output_dir / topology
            export(args.source, output, topology)
            print(f"{topology}: {output.resolve()}")
    else:
        if args.topology is None:
            parser.error("--topology required with --output")
        export(args.source, args.output, args.topology)
        print(f"{args.topology}: {args.output.resolve()}")


if __name__ == "__main__":
    main()

"""Fast structural checks; expensive Rust A/B is an explicit opt-in run."""

import tempfile
import unittest
from pathlib import Path

from research_diff import apply_probe, checksum
from research_topology import export


SOURCE = Path(__file__).resolve().parents[1] / "research"


class ResearchTopologyTests(unittest.TestCase):
    def test_compact_preserves_exact_numeric_source_and_removes_second_package(self):
        with tempfile.TemporaryDirectory(prefix="research-topology-") as temp:
            output = Path(temp) / "compact"
            export(SOURCE, output, "compact")
            self.assertFalse((output / "crates/okx-strategy-core").exists())
            self.assertEqual(
                (SOURCE / "crates/okx-strategy-core/src/query_plan.rs").read_bytes(),
                (output / "src/query_plan.rs").read_bytes(),
            )
            main = (output / "src/main.rs").read_text(encoding="utf-8")
            self.assertIn("extern crate self as okx_strategy_core;", main)
            self.assertIn("pub mod query_plan;", main)
            manifest = (output / "Cargo.toml").read_text(encoding="utf-8")
            self.assertNotIn("okx-strategy-core =", manifest)
            self.assertIn("members = []", manifest)
            lock = (output / "Cargo.lock").read_text(encoding="utf-8")
            self.assertNotIn('name = "okx-strategy-core"', lock)
            self.assertNotIn(' "okx-strategy-core",', lock)
            self.assertEqual(
                (SOURCE / "tests/replay.rs").read_bytes(),
                (output / "tests/replay.rs").read_bytes(),
            )

    def test_split_preserves_owner_source_without_sibling_dependencies(self):
        with tempfile.TemporaryDirectory(prefix="research-topology-") as temp:
            output = Path(temp) / "split"
            export(SOURCE, output, "split")
            for relative in ("Cargo.toml", "Cargo.lock", "src/main.rs",
                             "crates/okx-strategy-core/src/query_plan.rs"):
                self.assertEqual((SOURCE / relative).read_bytes(),
                                 (output / relative).read_bytes())
            self.assertFalse((output / "engine").exists())
            self.assertFalse((output / "control").exists())
            self.assertFalse((output / "edge").exists())

    def test_cannot_overwrite_or_export_into_source_tree(self):
        with tempfile.TemporaryDirectory(prefix="research-topology-") as temp:
            existing = Path(temp)
            with self.assertRaises(FileExistsError):
                export(SOURCE, existing, "split")
            with self.assertRaises(ValueError):
                export(SOURCE, SOURCE / "illegal-output", "compact")
            with self.assertRaises(ValueError):
                export(SOURCE, existing / "invalid", "unknown")

    def test_realistic_kernel_and_adapter_probes_are_separate_and_pinned(self):
        for probe in ("kernel-only", "adapter-only"):
            with self.subTest(probe=probe), tempfile.TemporaryDirectory(prefix="research-topology-") as temp:
                root = Path(temp)
                for topology in ("compact", "split"):
                    export(SOURCE, root / topology, topology)
                report = apply_probe(SOURCE, root, probe)
                self.assertEqual(report["source_class"], probe)
                self.assertTrue(report["benchmark_probe_not_production_change"])
                self.assertEqual(len(report["variants"]), 2)
                for variant in report["variants"]:
                    path = root / variant["path"]
                    self.assertEqual(checksum(path.read_bytes()), variant["after_sha256"])
                    self.assertNotEqual(variant["after_sha256"], variant["before_sha256"])
                with self.assertRaises(ValueError):
                    apply_probe(SOURCE, root, probe)

    def test_probe_refuses_modified_source_and_modified_variant(self):
        with tempfile.TemporaryDirectory(prefix="research-topology-") as temp:
            root = Path(temp)
            for topology in ("compact", "split"):
                export(SOURCE, root / topology, topology)
            modified = root / "compact/src/query_plan.rs"
            modified.write_bytes(modified.read_bytes() + b"\n")
            with self.assertRaisesRegex(ValueError, "does not match pinned baseline"):
                apply_probe(SOURCE, root, "kernel-only")
            self.assertFalse((root / "research-diff.json").exists())
            # Every variant must remain untouched on a failed preflight.
            self.assertEqual(
                (root / "split/crates/okx-strategy-core/src/query_plan.rs").read_bytes(),
                (SOURCE / "crates/okx-strategy-core/src/query_plan.rs").read_bytes(),
            )

    def test_probe_refuses_unknown_source_revision(self):
        with (tempfile.TemporaryDirectory(prefix="research-topology-") as temp,
              tempfile.TemporaryDirectory(prefix="research-stale-source-") as source_temp):
            root = Path(temp)
            for topology in ("compact", "split"):
                export(SOURCE, root / topology, topology)
            source = Path(source_temp) / "changed-source"
            relative = Path("crates/okx-strategy-core/src/query_plan.rs")
            (source / relative).parent.mkdir(parents=True)
            (source / relative).write_bytes((SOURCE / relative).read_bytes() + b"\n")
            with self.assertRaisesRegex(ValueError, "unrecognized Research source revision"):
                apply_probe(source, root, "kernel-only")
            self.assertFalse((root / "research-diff.json").exists())

    def test_probe_refuses_canonical_source_and_symlink_escape(self):
        with tempfile.TemporaryDirectory(prefix="research-topology-") as temp:
            root = Path(temp)
            with self.assertRaisesRegex(ValueError, "outside the canonical Research source"):
                apply_probe(SOURCE, SOURCE, "kernel-only")
            for topology in ("compact", "split"):
                export(SOURCE, root / topology, topology)
            candidate = root / "compact/src/query_plan.rs"
            candidate.unlink()
            candidate.symlink_to(SOURCE / "crates/okx-strategy-core/src/query_plan.rs")
            with self.assertRaisesRegex(ValueError, "escapes the export root"):
                apply_probe(SOURCE, root, "kernel-only")


if __name__ == "__main__":
    unittest.main()

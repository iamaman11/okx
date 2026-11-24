import tempfile
import unittest
from pathlib import Path

from check_boundaries import check_references, file_ownership, inventory


class Boundaries(unittest.TestCase):
    def test_legacy_files_require_an_explicit_owner(self):
        with self.assertRaisesRegex(ValueError, "file has no owner: crates/unknown.rs"):
            file_ownership({"crates/unknown.rs"}, {})

    def test_removed_sources_cannot_leave_stale_rows(self):
        with self.assertRaisesRegex(ValueError, "remove stale legacy row: crates/removed.rs"):
            file_ownership(set(), {"crates/removed.rs": ("engine", "retire", "-")})

    def test_cargo_dev_and_build_dependencies_cannot_leave_the_owner(self):
        for section in ("dev-dependencies", "build-dependencies", "target.'cfg(windows)'.dependencies"):
            with self.subTest(section=section):
                self.check_escape(
                    "vnext/research/Cargo.toml",
                    f'[{section}]\nengine = {{ path = "../engine" }}\n',
                    "research",
                )

    def test_edge_cannot_read_rust_src_as_a_fixture(self):
        self.check_escape(
            "vnext/edge/scripts/test.mjs",
            'readFileSync(resolve("../research/src/lib.rs"));',
            "edge",
        )

    def test_edge_relative_import_cannot_escape_the_owner(self):
        self.check_escape(
            "vnext/edge/src/test.ts",
            'import "../../research/module.js";',
            "edge",
        )

    def test_npm_file_dependencies_cannot_leave_the_owner(self):
        self.check_escape(
            "vnext/edge/package.json",
            '{"dependencies":{"engine":"file:../engine"}}',
            "edge",
        )

    def test_two_legacy_files_cannot_claim_the_same_move_target(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy-files.tsv"
            path.write_text(
                "path\towner\taction\tdestination\n"
                "first.rs\tengine\tmove\tvnext/engine/reserve.rs\n"
                "second.rs\tengine\tmove\tvnext/engine/reserve.rs\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "two files claim"):
                inventory(path)

    def check_escape(self, filename, source, owner):
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            path = repo / filename
            path.parent.mkdir(parents=True)
            path.write_text(source, encoding="utf-8")
            with self.assertRaisesRegex(ValueError, f"local reference escapes {owner}"):
                check_references(repo, {filename: (owner, "active", filename)})


if __name__ == "__main__":
    unittest.main()

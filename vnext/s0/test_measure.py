"""No-toolchain tests for the S0 local measurement utility."""

from pathlib import Path
import tempfile
import unittest

from measure import allowed_command, compare_records, measure_one, parse_cargo_output, source_fingerprint


class FingerprintTests(unittest.TestCase):
    def test_ignores_target_and_detects_source_change(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "src").mkdir()
            (root / "target").mkdir()
            (root / "src" / "lib.rs").write_text("pub fn value() -> u8 { 1 }\n")
            original, count = source_fingerprint(root)
            self.assertEqual(count, 1)
            (root / "target" / "foo.o").write_bytes(b"different built artifact")
            self.assertEqual(original, source_fingerprint(root)[0])
            (root / "src" / "lib.rs").write_text("pub fn value() -> u8 { 2 }\n")
            self.assertNotEqual(original, source_fingerprint(root)[0])


class CommandTests(unittest.TestCase):
    def test_test_harness_separator_is_preserved(self):
        self.assertEqual(
            allowed_command("cargo test --locked --workspace -- --test-threads 2", "engine"),
            ["cargo", "test", "--locked", "--workspace", "--message-format=json",
             "--", "--test-threads", "2"],
        )

    def test_disallows_shell_and_cargo_clean(self):
        with self.assertRaises(ValueError):
            allowed_command("sh -c 'rm -rf .'", "engine")
        with self.assertRaises(ValueError):
            allowed_command("cargo clean", "engine")
        with self.assertRaises(ValueError):
            allowed_command("npm install", "edge")


class ObservationTests(unittest.TestCase):
    def test_cargo_artifacts_not_automatically_rustc_invocations(self):
        events = [
            '{"reason":"compiler-artifact","fresh":false,"executable":"/tmp/test"}\n',
            '{"reason":"compiler-artifact","fresh":true,"executable":null}\n',
            '{"reason":"compiler-message","message":{"level":"warning"}}\n',
            'test result: ok. 5 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out\n',
        ]
        stats = parse_cargo_output(events)
        self.assertEqual(stats["cargo_artifacts"], 2)
        self.assertEqual(stats["nonfresh_artifacts"], 1)
        self.assertEqual(stats["fresh_artifacts"], 1)
        self.assertEqual(stats["executable_artifacts"], 1)
        self.assertEqual(stats["compiler_warnings"], 1)
        self.assertEqual(stats["tests_passed"], 5)
        self.assertEqual(stats["tests_ignored"], 2)
        self.assertIsNone(stats["rustc_verbose_invocations"])
        self.assertIsNone(stats["linker_invocations_observed"])

    def test_comparison_needs_changed_source_and_identical_environment(self):
        def sample(digest):
            return {
                "schema": "okx-s0-local-measure/v1", "name": digest,
                "owner": "engine", "commands": [["cargo", "test"]],
                "workspace_source_sha256": digest, "all_commands_passed": True,
                "environment": {"machine": "x86_64", "rustc": "rustc 1.100"},
                "total_elapsed_ms": 300,
                "phases": [{"elapsed_ms": 300, "command": ["cargo", "test"],
                            "observations": {"nonfresh_artifacts": 3}}],
            }
        self.assertFalse(compare_records(sample("A"), sample("A"))["comparable"])
        result = compare_records(sample("A"), sample("B"))
        self.assertTrue(result["comparable"])
        self.assertFalse(result["ready_ci_gate_proof"])
        self.assertEqual(result["phases"][0]["delta_ms"], 0)
        other = sample("B")
        other["environment"]["rustc"] = "rustc 1.101"
        self.assertFalse(compare_records(sample("A"), other)["comparable"])

    def test_failed_executable_uses_numeric_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            obs = measure_one(Path(directory), ["command-does-not-exist-s0"])
            self.assertEqual(obs["exit_code"], 127)


if __name__ == "__main__":
    unittest.main()

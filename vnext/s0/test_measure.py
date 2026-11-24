"""No-toolchain tests for the S0 local measurement utility."""

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from measure import (
    allowed_command, compare_records, environment, measure_one,
    parse_cargo_output, source_fingerprint,
)


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
            ["cargo", "test", "-vv", "--locked", "--workspace", "--message-format=json",
             "--", "--test-threads", "2"],
        )

    def test_disallows_shell_and_cargo_clean(self):
        with self.assertRaises(ValueError):
            allowed_command("sh -c 'rm -rf .'", "engine")
        with self.assertRaises(ValueError):
            allowed_command("cargo clean", "engine")
        with self.assertRaises(ValueError):
            allowed_command("npm install", "edge")
        with self.assertRaises(ValueError):
            allowed_command("cargo test --locked -q", "engine")

    def test_very_verbose_evidence_added_once(self):
        cmd = allowed_command("cargo build -vv --locked --release", "engine")
        self.assertEqual(cmd.count("-vv"), 1)
        self.assertEqual(cmd.count("--message-format=json"), 1)

    def test_build_affecting_values_are_not_disclosed(self):
        with patch.dict(os.environ, {"RUSTFLAGS": "-C link-arg=SECRET-TOKEN"}):
            info = environment(None)
        self.assertNotIn("SECRET-TOKEN", str(info))
        self.assertEqual(len(info["build_inputs"]["RUSTFLAGS"]), 64)


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
        self.assertIsNone(stats["rustdoc_verbose_invocations"])
        self.assertIsNone(stats["linker_invocations_observed"])

    def test_rustc_invocations_counted_only_from_cargo_verbose_commands(self):
        events = [
            '     Running `CARGO=/tmp/cargo /tmp/bin/rustc --crate-name a`\n',
            '     Running `CARGO=/tmp/cargo C:\\rust\\bin\\rustc.exe --crate-name b`\n',
            '     Running `CARGO=/tmp/cargo /tmp/bin/rustdoc --crate-name c`\n',
            '     Running `RUSTC=/tmp/bin/rustc /tmp/target/build-script-build`\n',
            '{"reason":"compiler-artifact","fresh":false,"executable":"/tmp/a"}\n',
            '     Fresh other-package v0.1.0\n',
        ]
        stats = parse_cargo_output(events, verbose_evidence=True)
        self.assertEqual(stats["rustc_verbose_invocations"], 2)
        self.assertEqual(stats["rustdoc_verbose_invocations"], 1)
        self.assertEqual(stats["cargo_artifacts"], 1)
        self.assertEqual(stats["nonfresh_artifacts"], 1)
        self.assertIsNone(stats["linker_invocations_observed"])

    def test_fully_fresh_verbose_build_reports_zero_invocations(self):
        stats = parse_cargo_output(['       Fresh serde v1.0\n'], verbose_evidence=True)
        self.assertEqual(stats["rustc_verbose_invocations"], 0)
        self.assertEqual(stats["rustdoc_verbose_invocations"], 0)

    def test_comparison_needs_changed_source_and_identical_environment(self):
        def sample(digest):
            return {
                "schema": "okx-s0-local-measure/v1", "name": digest,
                "owner": "engine", "commands": [["cargo", "test"]],
                "workspace_source_sha256": digest, "all_commands_passed": True,
                "environment": {"platform": "Linux", "machine": "x86_64",
                                "cargo": "cargo 1.100", "rustc": "rustc 1.100"},
                "total_elapsed_ms": 300,
                "phases": [{"elapsed_ms": 300, "exit_code": 0,
                            "command": ["cargo", "test"],
                            "observations": {"nonfresh_artifacts": 3,
                                             "rustc_verbose_invocations": 1}}],
            }
        self.assertFalse(compare_records(sample("A"), sample("A"))["comparable"])
        result = compare_records(sample("A"), sample("B"))
        self.assertTrue(result["comparable"])
        self.assertFalse(result["ready_ci_gate_proof"])
        self.assertEqual(result["phases"][0]["delta_ms"], 0)
        self.assertEqual(result["phases"][0]["baseline_rustc_invocations"], 1)
        self.assertFalse(result["semantic_equivalence_proven"])
        other = sample("B")
        other["environment"]["rustc"] = "rustc 1.101"
        self.assertFalse(compare_records(sample("A"), other)["comparable"])

    def test_comparison_refuses_missing_or_forged_phase(self):
        basic = {
            "schema": "okx-s0-local-measure/v1", "name": "test", "owner": "research",
            "commands": [["cargo", "test"]],
            "workspace_source_sha256": "aaa", "all_commands_passed": True,
            "environment": {"platform": "Linux", "machine": "x86_64",
                            "cargo": "cargo 1.100", "rustc": "rustc 1.100"},
            "total_elapsed_ms": 3,
            "phases": [{"elapsed_ms": 3, "exit_code": 0,
                        "command": ["cargo", "test"], "observations": {}}],
        }
        changed = {**basic, "workspace_source_sha256": "bbb"}
        self.assertTrue(compare_records(basic, changed)["comparable"])
        self.assertFalse(compare_records(basic, {**changed, "phases": []})["comparable"])
        failed = {**changed, "phases": [{**changed["phases"][0], "exit_code": 1}]}
        self.assertFalse(compare_records(basic, failed)["comparable"])
        wrong_cmd = {**changed, "phases": [{**changed["phases"][0],
                                            "command": ["cargo", "build"]}]}
        self.assertFalse(compare_records(basic, wrong_cmd)["comparable"])
        bad_time = {**changed, "phases": [{**changed["phases"][0], "elapsed_ms": -5}]}
        self.assertFalse(compare_records(basic, bad_time)["comparable"])
        bad_total = {**changed, "total_elapsed_ms": "unknown"}
        self.assertFalse(compare_records(basic, bad_total)["comparable"])
        bad_schema = {**changed, "schema": "okx-s0-local-measure/v2"}
        self.assertFalse(compare_records(basic, bad_schema)["comparable"])
        bad_observations = {**changed, "phases": [{**changed["phases"][0],
                                                   "observations": None}]}
        self.assertFalse(compare_records(basic, bad_observations)["comparable"])
        bad_environment = {**changed, "environment": None}
        self.assertFalse(compare_records(basic, bad_environment)["comparable"])

    def test_failed_executable_uses_numeric_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            obs = measure_one(Path(directory), ["command-does-not-exist-s0"])
            self.assertEqual(obs["exit_code"], 127)


if __name__ == "__main__":
    unittest.main()

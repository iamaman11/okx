import unittest

from check_p0 import check_counts, select


class FeedbackTests(unittest.TestCase):
    def test_kernel_and_boundaries(self):
        self.assertEqual(select("M", "crates/okx-kernel/src/operators.rs"), {"kernel"})
        self.assertEqual(select("M", "crates/okx-research-worker/src/analytics/jobs.rs"), {"worker"})
        self.assertEqual(select("M", "crates/okx-protocol/src/research_ipc.rs"), {"wire", "worker", "ipc", "mcp"})
        self.assertFalse(select("M", "docs/testing.md"))

    def test_expand_uncertain_impact(self):
        for path in ("Cargo.lock", "crates/okx-kernel/Cargo.toml", "rust-toolchain.toml",
                     "crates/okx-research-worker/build.rs", ".github/workflows/ci.yml", "unknown"):
            self.assertEqual(select("M", path), {"expanded"})
        self.assertEqual(select("D", "crates/okx-kernel/src/query_plan.rs"), {"expanded"})

    def test_missing_or_filtered_out_test_is_a_failure(self):
        self.assertEqual(check_counts("test one ... ok\ntest two ... ignored", ["one"]), 1)
        for text in ("running 0 tests", "test two ... ok", "test one ... ignored"):
            with self.assertRaises(ValueError):
                check_counts(text, ["one"])


if __name__ == "__main__":
    unittest.main()

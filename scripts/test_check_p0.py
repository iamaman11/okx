import unittest

from check_p0 import check_counts, measured_t10, select


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

    def test_t10_requires_real_samples_and_measured_process_memory(self):
        import json

        value = {"two_heavy_stop": {
            "os": "linux", "build": "debug", "logical_cpus": 4,
            "measurements": 30, "p95_ms": 2.0, "cancel_p95_ms": 3.0,
            "cancel_max_ms": 4.0, "stop_samples_ms": [2.0] * 30,
            "cancel_samples_ms": [3.0] * 30,
            "process_memory": {"scope": "whole_process_including_test_harness",
                "measurement_source": "linux_procfs_process_rss_and_hwm",
                "peak_rss_after_bytes": 123456, "reservation_not_rss_limit": True},
        }}
        output = "ANALYTICS_PERF_JSON:" + json.dumps(value)
        self.assertEqual(measured_t10(output)["measurements"], 30)
        for mutation in (lambda x: x.update(measurements=29),
                         lambda x: x.update(cancel_max_ms=1001),
                         lambda x: x["process_memory"].update(peak_rss_after_bytes=None)):
            bad = json.loads(json.dumps(value))
            mutation(bad["two_heavy_stop"])
            with self.assertRaises(ValueError):
                measured_t10("ANALYTICS_PERF_JSON:" + json.dumps(bad))
        with self.assertRaises(ValueError):
            measured_t10("test result: ok. 1 passed")


if __name__ == "__main__":
    unittest.main()

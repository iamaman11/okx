import unittest

from check_p0 import check_counts, measured_native_under_research, measured_t10, select


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

    def test_native_under_heavy_research_requires_full_finance_and_ipc_samples(self):
        import json

        def timing():
            return {"samples": 100, "p50_ms": 1.0, "p95_ms": 1.0,
                    "max_ms": 1.0, "raw_ms": [1.0] * 100, "slo_p95_ms": 100.0}

        def phase(name, explain):
            return {"schema": "okx.518.t11-t12-native-concurrent-isolation/v1",
                    "phase": name, "native_finance_read_only": timing(),
                    "research_explain": timing() if explain else None,
                    "native_installation_identity_preserved": True,
                    "financial_owner": "native_sqlite_original_owner",
                    "synthetic_public_source": True, "trading_requests_sent": 0,
                    "reserved_entry_usd": "0", "consumed_entry_usd": "0"}

        value = {"schema": "okx.518.t11-t12-native-isolation-campaign/v1",
                 "native_engine": "run_engine_until_real_tasks",
                 "research": "separate_child_process_two_real_grids",
                 "mcp_transport": "not_evaluated_in_native_test",
                 "exchange_trade_post": False,
                 "under_load": phase("two_heavy_grids_running", True),
                 "without_worker": phase("worker_disconnected", False)}

        def record(data):
            return "T11_T12_NATIVE_ISOLATION_JSON:" + json.dumps(data)

        self.assertEqual(measured_native_under_research(record(value))["under_load"]["phase"],
                         "two_heavy_grids_running")
        for path, changed in (
            (("under_load", "research_explain", "samples"), 99),
            (("under_load", "research_explain", "p95_ms"), 101.0),
            (("under_load", "native_finance_read_only", "raw_ms"), [float("nan")] * 100),
            (("without_worker", "research_explain"), timing()),
            (("under_load", "trading_requests_sent"), 1),
            (("mcp_transport",), "fully_measured"),
        ):
            bad = json.loads(json.dumps(value))
            target = bad
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = changed
            with self.assertRaises(ValueError):
                measured_native_under_research(record(bad))
        with self.assertRaises(ValueError):
            measured_native_under_research("test result: ok. 1 passed")


if __name__ == "__main__":
    unittest.main()

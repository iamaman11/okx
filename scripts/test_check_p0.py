import unittest
from unittest.mock import patch
from pathlib import Path
import re

from check_ci_native_scope import classify as ready_classify
from check_p0 import (check_counts, local_component, measured_mcp_native_under_research,
                      measured_native_under_research, measured_t10, execute, select)


class FeedbackTests(unittest.TestCase):
    def test_local_component_repeats_only_the_requested_package(self):
        with (patch("check_p0.git", side_effect=[b"abc\n", b" M changed\n"]),
              patch("check_p0.report") as report,
              patch("check_p0.tests") as run_tests,
              patch("check_p0.time.monotonic", side_effect=[0, 10, 10, 12, 13, 17])):
            local_component("strategy", 3, False)
        self.assertEqual([call.args for call in run_tests.call_args_list],
                         [("okx-strategy-core",)] * 3)
        self.assertIn("dirty=true", report.call_args_list[0].args[0])
        self.assertIn("median 3.00s", report.call_args_list[-1].args[0])

    def test_local_plan_prints_exact_cargo_invocation(self):
        with (patch("check_p0.git", side_effect=[b"abc\n", b""]),
              patch("check_p0.report") as report):
            local_component("engine", 1, True)
        planned = report.call_args_list[0].args[0]
        self.assertIn("--message-format=json-render-diagnostics", planned)
        self.assertIn("-- --test-threads 2 --exact", planned)
        self.assertIn("strategy_risk_budget_not_notional", planned)

    def test_ready_windows_runs_math_owner_tests_not_just_worker(self):
        workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml").read_text()
        owner_commands = {
            "research_service_tests_windows": [
                "cargo test --locked -p okx-strategy-core -p okx-research -p okx-analysis",
                "cargo test --locked -p okx-research-service --lib --bins -- --test-threads 2",
                "cargo test --locked -p okx-research-service --test causal_release_smoke",
                "cargo test --locked -p okx-research-service --test ipc_process -- --nocapture --test-threads 1",
                "cargo test --locked -p okx-research-service --doc",
            ],
            "trading_engine_regressions_windows": [
                "cargo test --locked -p okx-strategy-core -p okx-research",
            ],
        }
        for job, commands in owner_commands.items():
            match = re.search(rf"(?ms)^  {re.escape(job)}:\n(.*?)(?=^  [a-z_-]+:|\Z)", workflow)
            self.assertIsNotNone(match, f"missing mandatory Windows job {job}")
            for command in commands:
                self.assertIn(command, match.group(1),
                              f"{job} must execute owner tests, not just compile them")
        scoped = re.search(r"(?ms)^  research_service_tests_windows:\n(.*?)(?=^  [a-z_-]+:|\Z)",
                           workflow).group(1)
        self.assertNotIn("cargo test --locked -p okx-strategy-core -p okx-research -p okx-research-service",
                         scoped, "do not run IPC tests with unbounded default parallelism")
        acceptance = re.search(
            r"(?ms)^  trading_engine_acceptance_windows:\n(.*?)(?=^  [a-z_-]+:|\Z)", workflow
        )
        self.assertIsNotNone(acceptance)
        self.assertIn("cargo run --locked -p okx-native-finance-check", acceptance.group(1))

    @staticmethod
    def source_witness():
        return {"source_reads": {
            "kind": "actual_research_ipc_public_candles_requests_to_synthetic_handler",
            "successful_requests": 120, "returned_candle_rows": 12000,
            "venue_http_reads": 0, "source_storage_page_reads": "not_measured",
            "observation_scope": "request_and_rows_counted_in_native_source_handler",
        }, "phase_wall_ms_excluding_build": 1453.0}

    @staticmethod
    def boundary_witness():
        return {"pre_source_boundaries": {
            "schema": "okx.518.t10-real-child-pre-source-boundaries/v1",
            "route": "real_agent_to_separate_research_worker_ipc",
            "admission_only_not_executed": ["one_million_bars_one_instrument_64",
                                            "eight_instruments_one_million_bars_64"],
            "rejected_before_source": {
                "one_million_plus_one_bars": "INVALID_RESEARCH_REQUEST",
                "nine_instruments": "INVALID_RESEARCH_REQUEST",
                "sixty_five_variants": "INVALID_RESEARCH_REQUEST",
            }, "source_requests_before": 0, "source_requests_after": 0,
            "returned_rows_before": 0, "returned_rows_after": 0,
            "full_scale_execution": "not_attempted", "side_effects": "none"}}

    def test_kernel_and_boundaries(self):
        self.assertEqual(select("M", "crates/okx-research-domain/src/lib.rs"), {"expanded"})
        self.assertEqual(select("M", "crates/okx-strategy-core/src/time_series.rs"),
                         {"strategy", "research", "worker", "analysis"})
        self.assertEqual(select("M", "crates/okx-strategy-core/src/operators.rs"), {"strategy", "research", "worker", "analysis"})
        self.assertEqual(select("M", "crates/okx-research-protocol/src/strategy_contract.rs"),
                         {"wire", "ipc", "worker", "mcp", "strategy", "research", "analysis"})
        self.assertEqual(select("M", "crates/okx-research-service/src/analytics/jobs.rs"), {"worker"})
        self.assertEqual(select("M", "crates/okx-research-protocol/src/research_ipc.rs"), {"wire", "worker", "ipc", "mcp"})
        self.assertFalse(select("M", "docs/testing.md"))

    def test_worker_internal_changes_never_select_agent(self):
        for path in ("source_io.rs", "source_api.rs", "producer.rs", "invocation.rs", "process_memory.rs"):
            source = f"crates/okx-research-service/src/{path}"
            self.assertEqual(select("M", source), {"worker"})
            with (patch("check_p0.tests", return_value="") as run_tests,
                  patch("check_p0.measured_t10", return_value={}),
                  patch("check_p0.report_t10")):
                execute(select("M", source))
            packages = [call.args[0] for call in run_tests.call_args_list]
            self.assertEqual(set(packages), {"okx-research-service"})

    def test_shared_ipc_and_worker_transport_select_both_sides(self):
        for path in ("crates/okx-research-protocol/src/research_ipc.rs",
                     "crates/okx-research-service/src/ipc.rs",
                     "crates/okx-research-service/src/ipc/correlation_tests.rs",
                     "crates/okx-research-service/src/main.rs"):
            self.assertIn("ipc", select("M", path))
            self.assertIn("worker", select("M", path))
            with (patch("check_p0.tests", return_value="") as run_tests,
                  patch("check_p0.measured_t10", return_value={}),
                  patch("check_p0.report_t10"),
                  patch("check_p0.measured_native_under_research", return_value={}),
                  patch("check_p0.report_native_under_research"),
                  patch("check_p0.subprocess.check_output", return_value='{"target_directory":"/tmp/no-exec"}'),
                  patch.dict("check_p0.os.environ")):
                execute(select("M", path) - {"mcp"})
            packages = [call.args[0] for call in run_tests.call_args_list]
            self.assertIn("okx-research-service", packages)
            self.assertIn("okx-trading-engine", packages)

    def test_host_source_uses_owner_and_consumer_not_expanded(self):
        for path in (
            "crates/okx-host-controller/src/main.rs",
            "crates/okx-host-controller/src/lifecycle/activation.rs",
            "crates/okx-host-protocol/src/lib.rs",
            "crates/okx-host-launcher/src/main.rs",
            "crates/okx-host-launcher/src/stdio_capture.rs",
        ):
            self.assertEqual(select("M", path), {"host"})
            self.assertEqual(select("A", path), {"host"})
            self.assertEqual(select("D", path), {"expanded"})
        for path in (
            "crates/okx-host-controller/Cargo.toml",
            "crates/okx-host-launcher/build.rs",
            "crates/okx-host-launcher/assets/manifest.json",
            "crates/okx-trading-engine/src/trade_service/mod.rs",
            "crates/okx-ipc-contracts/src/lib.rs",
        ):
            self.assertNotEqual(select("M", path), {"host"})

    def test_host_protocol_feedback_runs_bridge_without_engine_or_finance(self):
        with patch("check_p0.tests", return_value="") as run_tests:
            execute(select("M", "crates/okx-host-protocol/src/lib.rs"))
        packages = [call.args[0] for call in run_tests.call_args_list]
        self.assertEqual(packages, [
            "okx-host-protocol", "okx-host-launcher", "okx-host-controller", "okx-bridge-mcp",
        ])

    def test_expand_uncertain_impact(self):
        for path in ("Cargo.lock", "crates/okx-research-domain/Cargo.toml", "rust-toolchain.toml",
                     "crates/okx-research-service/build.rs", ".github/workflows/ci.yml", "unknown"):
            self.assertEqual(select("M", path), {"expanded"})
        self.assertEqual(select("D", "crates/okx-research-domain/src/lib.rs"), {"expanded"})

    def test_one_strategy_primitive_selects_owner_and_ready_research(self):
        def ready_scope(path):
            return ready_classify([[{"filename": path, "status": "modified"}]])

        for primitive in ("operators.rs", "query_plan.rs", "stream.rs", "time_series.rs"):
            path = f"crates/okx-strategy-core/src/{primitive}"
            self.assertEqual(ready_scope(path), "research")
            draft_scope = select("M", path)
            self.assertEqual(draft_scope, {"strategy", "analysis", "research", "worker"})
            with (patch("check_p0.tests", return_value="") as run_tests,
                  patch("check_p0.measured_t10", return_value={}),
                  patch("check_p0.report_t10")):
                execute(draft_scope)
            packages = [call.args[0] for call in run_tests.call_args_list]
            self.assertIn("okx-strategy-core", packages)
            self.assertIn("okx-analysis", packages)
            self.assertIn("okx-research", packages)
            self.assertIn("okx-research-service", packages)
            self.assertNotIn("okx-trading-engine", packages)
            self.assertNotIn("okx-trade-execution", packages)

    def test_finance_and_dependency_changes_keep_full_native(self):
        for path in ("crates/okx-trading-engine/src/trade_service/execute.rs",
                     "crates/okx-trade-execution/src/lib.rs",
                     "crates/okx-strategy-core/Cargo.toml", "Cargo.lock"):
            self.assertEqual(ready_classify([[{"filename": path, "status": "modified"}]]), "full")
            self.assertEqual(select("M", path), {"expanded"})

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
                    **self.source_witness(),
                    **self.boundary_witness(),
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
            (("under_load", "source_reads", "successful_requests"), 0),
            (("under_load", "source_reads", "source_storage_page_reads"), 100),
            (("under_load", "pre_source_boundaries", "returned_rows_after"), 1),
            (("without_worker", "phase_wall_ms_excluding_build"), 0),
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

    @patch("check_p0.sys.platform", "linux")
    def test_t10_mcp_requires_actual_end_to_end_native_finance_samples(self):
        import json

        sample = {"samples":100,"p50_ms":2.0,"p95_ms":2.0,"max_ms":2.0,
                  "raw_ms":[2.0]*100,"slo_p95_ms":100.0}
        phase = {"phase":"two_heavy_grids_running",
                 "native_installation_identity_preserved":True,
                 "financial_owner":"native_sqlite_original_owner",
                 "trading_requests_sent":0,"reserved_entry_usd":"0",
                 "consumed_entry_usd":"0","mcp_native_finance_read_only":sample,
                 **self.source_witness(), **self.boundary_witness()}
        tree = {"status":"MEASURED_SAMPLED", "scope":"collector_and_current_descendants",
                "descendants_included":True,"max_active_descendants":3,
                "samples":30,"peak_sampled_sum_rss_bytes":1234567,
                "source":"linux_procfs_task_children_and_status",
                "budget_status":"NOT_PROVEN","rss_hard_limit_enforced":False,
                "observed_process_io":{
                    "read_char_bytes":10000, "read_syscalls":100,
                    "storage_read_bytes":4096, "sampled_processes":4,
                    "source_attribution":"not_attributed"}}
        value = {"schema":"okx.518.t10-mcp-do-wss-native-finance/v1",
                 "mcp_transport":"real_loopback_http_miniflare_sqlite_do_websocket_native",
                 "oauth":"test_scope_only_not_evaluated",
                 "production_network":"not_evaluated",
                 "research":"two_cpu_grids_in_separate_native_worker_child",
                 "financial_owner":"existing_native_sqlite","exchange_trade_post":False,
                 "process_tree":tree,
                 "under_load":phase,
                 "worker_disconnected":{**phase,"phase":"worker_disconnected"}}

        def record(data):
            return "T10_MCP_NATIVE_LOAD_JSON:"+json.dumps(data)

        self.assertEqual(measured_mcp_native_under_research(record(value))["under_load"]["phase"],
                         "two_heavy_grids_running")
        for change in (
            lambda x: x["under_load"]["mcp_native_finance_read_only"].update(samples=99),
            lambda x: x["under_load"]["mcp_native_finance_read_only"].update(p95_ms=101.0),
            lambda x: x["worker_disconnected"].update(trading_requests_sent=1),
            lambda x: x["under_load"]["source_reads"].update(successful_requests=0),
            lambda x: x["under_load"].update(phase_wall_ms_excluding_build=0),
            lambda x: x["under_load"]["pre_source_boundaries"]["rejected_before_source"].update(nine_instruments="accepted"),
            lambda x: x["process_tree"].update(budget_status="PASS"),
            lambda x: x["process_tree"].update(max_active_descendants=0),
            lambda x: x["process_tree"]["observed_process_io"].update(source_attribution="confirmed_venue_reads"),
            lambda x: x.update(production_network="accepted"),
            lambda x: x.update(oauth="production_oauth"),
        ):
            bad = json.loads(json.dumps(value))
            change(bad)
            with self.assertRaises(ValueError):
                measured_mcp_native_under_research(record(bad))
        with self.assertRaises(ValueError):
            measured_mcp_native_under_research("not a proof")


if __name__ == "__main__":
    unittest.main()

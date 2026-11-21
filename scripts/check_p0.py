"""Select and run Draft feedback on the checked-out tree; see docs/testing.md."""
import argparse
import json
import math
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
MCP = ROOT / "cloudflare/okx-mcp"
PLAN_TEST = "query_plan::tests::k7_invalid_operator_bounds_or_parameter_fail_closed"
GAP_TEST = "time_series::tests::c13_missing_internal_candle_not_healed_by_duplicate_or_unclosed"
WORKER_TESTS = (
    "analytics::planner::tests::market_plan_rejects_unknown_operator_before_any_source_read",
    "analytics::market::tests::interior_gap_never_imputed",
    "analytics::market::tests::c20_rolling_mean_reuses_existing_market_pipeline_without_new_routing",
)
SCOPES = {"kernel", "analysis", "research", "worker", "wire", "ipc", "mcp", "expanded"}


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT)


def changes(base):
    common = git("merge-base", base, "HEAD").decode().strip()
    fields = git("diff", "--name-status", "-z", "--no-renames", common, "--").split(b"\0")[:-1]
    paths = [(s.decode(), os.fsdecode(p)) for s, p in zip(fields[::2], fields[1::2])]
    paths.extend(("A", os.fsdecode(p)) for p in
                 git("ls-files", "--others", "--exclude-standard", "-z").split(b"\0") if p)
    return paths


def select(status, path):
    if status not in {"A", "M"}:
        return {"expanded"}
    if path == "README.md" or (path.startswith("docs/") and path.endswith(".md")):
        return set()
    if path.startswith("crates/okx-strategy-core/src/") and path.endswith(".rs"):
        return {"research", "worker", "analysis"}
    if path == "crates/okx-kernel/src/time_series.rs":
        return {"kernel"}
    if path.startswith("crates/okx-analysis/src/") and path.endswith(".rs"):
        return {"analysis", "kernel"}
    if path.startswith("crates/okx-research/src/") and path.endswith(".rs"):
        return {"research", "worker"}
    if path.startswith("crates/okx-research-worker/src/analytics/") and path.endswith(".rs"):
        return {"worker"}
    if path.startswith("crates/okx-research-worker/") and path.endswith(".rs") and not path.endswith("/build.rs"):
        return {"ipc", "worker"}
    if path.startswith("crates/okx-protocol/src/") and path.endswith(".rs"):
        return {"wire", "ipc", "worker", "mcp"}
    if path.startswith(("cloudflare/okx-mcp/src/", "cloudflare/okx-mcp/contracts/")):
        return {"mcp", "wire", "worker"}
    return {"expanded"}


def report(message):
    print(message, flush=True)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as summary:
            summary.write(message + "\n\n")


def check_counts(output, required):
    passed = set(re.findall(r"^test (\S+) \.\.\. ok$", output, re.M))
    missing = set(required) - passed
    if not passed or missing:
        raise ValueError(f"missing executed tests: {sorted(missing)}; passed={len(passed)}")
    return len(passed)


def tests(package, target=("--lib",), required=(), exact=False, show_output=False, extra_env=None):
    cmd = ["cargo", "test", "--locked", "-p", package, *target, "--message-format=json-render-diagnostics"]
    cmd.extend(["--", "--test-threads", "2"])
    if exact:
        cmd.extend(["--exact", *required])
    if show_output:
        cmd.append("--show-output")
    print(shlex.join(cmd), flush=True)
    started = time.monotonic()
    run = subprocess.run(cmd, cwd=ROOT, stdout=subprocess.PIPE, text=True,
                         env={**os.environ, **extra_env} if extra_env else None)
    rebuilt = set()
    output = []
    for line in run.stdout.splitlines():
        if line.startswith('{"reason":'):
            item = json.loads(line)
            if (item["reason"] == "compiler-artifact" and not item["fresh"]
                    and Path(item["target"]["src_path"]).is_relative_to(ROOT / "crates")):
                rebuilt.add(item["target"]["name"])
        else:
            output.append(line)
    print("\n".join(output), flush=True)
    run.check_returncode()
    count = check_counts("\n".join(output), required)
    report(f"{package} {' '.join(target)}: {count} tests passed in {time.monotonic()-started:.2f}s "
           f"(Cargo + tests). Recompiled workspace targets: {', '.join(sorted(rebuilt)) or 'none'}.")
    return "\n".join(output)


def measured_t10(output):
    records = re.findall(r"ANALYTICS_PERF_JSON:(\{[^\n]+\})", output)
    if len(records) != 1:
        raise ValueError(f"T10 requires exactly one observed performance record; got {len(records)}")
    value = json.loads(records[0])["two_heavy_stop"]
    samples = value.get("stop_samples_ms", [])
    cancellation = value.get("cancel_samples_ms", [])
    memory = value.get("process_memory", {})
    if (value.get("measurements") != 30 or len(samples) != 30 or len(cancellation) != 30
            or value.get("p95_ms", float("inf")) > 100
            or value.get("cancel_max_ms", float("inf")) > 1000
            or not isinstance(memory.get("peak_rss_after_bytes"), int)
            or memory.get("scope") != "whole_process_including_test_harness"
            or memory.get("reservation_not_rss_limit") is not True):
        raise ValueError("T10 incomplete samples, budget, or observed process-memory evidence")
    return value


def report_t10(value):
    memory = value["process_memory"]
    report(f"T10 observed two-heavy Grid fixture: OS={value['os']}, build={value['build']}, "
           f"logical CPUs={value['logical_cpus']}; 30 measured after 5 warmups; "
           f"stop p95={value['p95_ms']:.3f} ms, "
           f"cancel p95={value['cancel_p95_ms']:.3f} ms, "
           f"cancel max={value['cancel_max_ms']:.3f} ms; "
           f"whole-test-process peak RSS={memory['peak_rss_after_bytes']} bytes "
           f"({memory['measurement_source']}). "
           "This does NOT measure Finance/MCP under load or enforce a hard RSS ceiling.")


def check_source_witness(phase):
    source = phase.get("source_reads", {})
    requests = source.get("successful_requests")
    rows = source.get("returned_candle_rows")
    elapsed = phase.get("phase_wall_ms_excluding_build")
    if (source.get("kind") != "actual_research_ipc_public_candles_requests_to_synthetic_handler"
            or type(requests) is not int or requests <= 0
            or type(rows) is not int or rows < requests
            or source.get("venue_http_reads") != 0
            or source.get("source_storage_page_reads") != "not_measured"
            or source.get("observation_scope") != "request_and_rows_counted_in_native_source_handler"
            or not isinstance(elapsed, (int, float)) or not math.isfinite(elapsed)
            or elapsed <= 0):
        raise ValueError("T10 missing actual typed synthetic-source reads or build-excluded phase timing")


def check_real_child_boundaries(phase):
    witness = phase.get("pre_source_boundaries", {})
    expected = {"one_million_plus_one_bars", "nine_instruments", "sixty_five_variants"}
    if (witness.get("schema") != "okx.518.t10-real-child-pre-source-boundaries/v1"
            or witness.get("route") != "real_agent_to_separate_research_worker_ipc"
            or witness.get("admission_only_not_executed") != [
                "one_million_bars_one_instrument_64", "eight_instruments_one_million_bars_64"]
            or witness.get("rejected_before_source") != {name: "INVALID_RESEARCH_REQUEST"
                                                         for name in expected}
            or witness.get("source_requests_before") != 0
            or witness.get("source_requests_after") != 0
            or witness.get("returned_rows_before") != 0
            or witness.get("returned_rows_after") != 0
            or witness.get("full_scale_execution") != "not_attempted"
            or witness.get("side_effects") != "none"):
        raise ValueError("T10 real Worker IPC size boundaries did not reject before source I/O")


def measured_native_under_research(output):
    records = re.findall(r"T11_T12_NATIVE_ISOLATION_JSON:(\{[^\n]+\})", output)
    if len(records) != 1:
        raise ValueError(f"Native-under-Research requires one proof record; got {len(records)}")
    value = json.loads(records[0])
    if (value.get("schema") != "okx.518.t11-t12-native-isolation-campaign/v1"
            or value.get("native_engine") != "run_engine_until_real_tasks"
            or value.get("research") != "separate_child_process_two_real_grids"
            or value.get("mcp_transport") != "not_evaluated_in_native_test"
            or value.get("exchange_trade_post") is not False):
        raise ValueError("Native-under-Research proof has unexpected scope or financial side effects")
    for phase_name, expected in (("under_load", "two_heavy_grids_running"),
                                 ("without_worker", "worker_disconnected")):
        phase = value.get(phase_name, {})
        if (phase.get("schema") != "okx.518.t11-t12-native-concurrent-isolation/v1"
                or phase.get("phase") != expected
                or phase.get("native_installation_identity_preserved") is not True
                or phase.get("financial_owner") != "native_sqlite_original_owner"
                or phase.get("synthetic_public_source") is not True
                or phase.get("trading_requests_sent") != 0
                or phase.get("reserved_entry_usd") != "0"
                or phase.get("consumed_entry_usd") != "0"):
            raise ValueError(f"Native-under-Research invalid {phase_name} phase")
        check_source_witness(phase)
        check_real_child_boundaries(phase)
        for lane in ("native_finance_read_only", "research_explain"):
            proof = phase.get(lane)
            if phase_name == "without_worker" and lane == "research_explain":
                if proof is not None:
                    raise ValueError("Disconnected Worker must not claim an Explain measurement")
                continue
            if not isinstance(proof, dict):
                raise ValueError(f"Missing {phase_name}.{lane} measurement")
            samples = proof.get("raw_ms", [])
            if (proof.get("samples") != 100 or len(samples) != 100
                    or proof.get("slo_p95_ms") != 100.0
                    or not all(isinstance(v, (int, float)) and not isinstance(v, bool)
                               and math.isfinite(v) and v >= 0 for v in samples)):
                raise ValueError(f"Invalid {phase_name}.{lane} raw timing samples")
            ordered = sorted(samples)
            if (proof.get("p50_ms") != ordered[49]
                    or proof.get("p95_ms") != ordered[94]
                    or proof.get("max_ms") != ordered[99]
                    or ordered[94] > 100):
                raise ValueError(f"Invalid or over-budget {phase_name}.{lane} latency evidence")
    return value


def report_native_under_research(value):
    for name in ("under_load", "without_worker"):
        phase = value[name]
        finance = phase["native_finance_read_only"]
        source = phase["source_reads"]
        report(f"T10/T11/T12 observed Native Finance with {phase['phase']}: 100 read-only "
               f"SQLite owner samples; p95={finance['p95_ms']:.3f} ms, "
               f"max={finance['max_ms']:.3f} ms. Native installation identity preserved, "
               f"zero reservations/consumption/trade POSTs. "
               f"Real synthetic-source IPC requests={source['successful_requests']}, "
               f"returned candle rows={source['returned_candle_rows']}, "
               f"phase wall excluding build={phase['phase_wall_ms_excluding_build']:.1f} ms.")
        if name == "under_load":
            explain = phase["research_explain"]
            report(f"T10 observed child Research Explain alongside Finance and two running "
                   f"Grid jobs: 100 IPC samples; p95={explain['p95_ms']:.3f} ms, "
                   f"max={explain['max_ms']:.3f} ms. "
                   "MCP transport NOT measured; no full integrated T10 SLO claim.")


def measured_mcp_native_under_research(output):
    records = re.findall(r"T10_MCP_NATIVE_LOAD_JSON:(\{[^\n]+\})", output)
    if len(records) != 1:
        raise ValueError(f"T10 requires one MCP->DO->WSS->Native record; got {len(records)}")
    value = json.loads(records[0])
    if (value.get("schema") != "okx.518.t10-mcp-do-wss-native-finance/v1"
            or value.get("mcp_transport") != "real_loopback_http_miniflare_sqlite_do_websocket_native"
            or value.get("oauth") != "test_scope_only_not_evaluated"
            or value.get("production_network") != "not_evaluated"
            or value.get("research") != "two_cpu_grids_in_separate_native_worker_child"
            or value.get("financial_owner") != "existing_native_sqlite"
            or value.get("exchange_trade_post") is not False):
        raise ValueError("T10 MCP proof scope, financial owner or no-POST flag invalid")
    for phase_key, expected in (("under_load", "two_heavy_grids_running"),
                                ("worker_disconnected", "worker_disconnected")):
        phase = value.get(phase_key, {})
        if (phase.get("phase") != expected
                or phase.get("native_installation_identity_preserved") is not True
                or phase.get("financial_owner") != "native_sqlite_original_owner"
                or phase.get("trading_requests_sent") != 0
                or phase.get("reserved_entry_usd") != "0"
                or phase.get("consumed_entry_usd") != "0"):
            raise ValueError(f"T10 invalid finance identity/side effects in {phase_key}")
        check_source_witness(phase)
        check_real_child_boundaries(phase)
        sample = phase.get("mcp_native_finance_read_only")
        if not isinstance(sample, dict):
            raise ValueError(f"T10 missing {phase_key} real MCP native latency")
        raw = sample.get("raw_ms", [])
        if (sample.get("samples") != 100 or len(raw) != 100
                or sample.get("slo_p95_ms") != 100.0
                or not all(isinstance(v, (int, float)) and not isinstance(v, bool)
                               and math.isfinite(v) and v >= 0 for v in raw)):
            raise ValueError("T10 MCP raw sample set incomplete or invalid")
        ordered = sorted(raw)
        if (sample.get("p50_ms") != ordered[49]
                or sample.get("p95_ms") != ordered[94]
                or sample.get("max_ms") != ordered[99]
                or ordered[94] > 100.0):
            raise ValueError("T10 MCP p95 exceeded SLO or statistics not from raw data")
    memory = value.get("process_tree", {})
    if sys.platform == "linux":
        io = memory.get("observed_process_io", {})
        if (memory.get("status") != "MEASURED_SAMPLED"
                or memory.get("scope") != "collector_and_current_descendants"
                or memory.get("descendants_included") is not True
                or memory.get("max_active_descendants", 0) < 2
                or memory.get("samples", 0) < 2
                or memory.get("peak_sampled_sum_rss_bytes", 0) <= 0
                or memory.get("source") != "linux_procfs_task_children_and_status"
                or memory.get("budget_status") != "NOT_PROVEN"
                or memory.get("rss_hard_limit_enforced") is not False
                or not isinstance(io, dict)
                or type(io.get("read_char_bytes")) is not int or io["read_char_bytes"] <= 0
                or type(io.get("read_syscalls")) is not int or io["read_syscalls"] <= 0
                or type(io.get("storage_read_bytes")) is not int or io["storage_read_bytes"] < 0
                or io.get("sampled_processes", 0) < 3
                or io.get("source_attribution") != "not_attributed"):
            raise ValueError("T10 Linux MCP/Worker/Agent process tree RSS and OS I/O not sampled")
    return value


def report_mcp_native_under_research(value):
    memory = value["process_tree"]
    for name in ("under_load", "worker_disconnected"):
        phase = value[name]
        proof = phase["mcp_native_finance_read_only"]
        source = phase["source_reads"]
        report(f"T10 observed {name} genuine loopback MCP HTTP -> Miniflare SQLite DO -> "
               f"authenticated WSS -> native Finance: 100 calls, p95={proof['p95_ms']:.3f} ms, "
               f"max={proof['max_ms']:.3f} ms, no trade POST; "
               f"synthetic source IPC reads={source['successful_requests']} "
               f"({source['returned_candle_rows']} candle rows), "
               f"phase wall excluding build={phase['phase_wall_ms_excluding_build']:.1f} ms. "
               "Fixture OAuth replaced with test scope; production network, "
               "physical Windows and overall memory budget NOT evaluated.")
        report("T10 real Agent->Research Worker IPC admission boundaries: "
               "1,000,000 bars / 8 instruments / 64 variants protocol-admitted only; "
               "1,000,001 bars / 9 instruments / 65 variants each rejected before "
               "source requests or job creation; full-scale execution not attempted.")
    if memory.get("status") == "MEASURED_SAMPLED":
        report(f"T10 observed Linux MCP+Native+Research sampled process tree: "
               f"{memory['samples']} x 20ms /proc observations; max active descendants="
               f"{memory['max_active_descendants']}; max summed current RSS="
               f"{memory['peak_sampled_sum_rss_bytes']} bytes (shared pages double-counted; "
               "between-sample spikes may be missed). No hard RSS budget claim.")
        io = memory["observed_process_io"]
        report(f"T10 Linux sampled process-tree I/O (not source-attributed): "
               f"read syscalls={io['read_syscalls']}, read-character bytes="
               f"{io['read_char_bytes']}, storage-backed read bytes="
               f"{io['storage_read_bytes']}; terminated short-lived children may be missed.")
    else:
        report(f"T10 process-tree resource witness: {memory.get('status', 'NOT_PROVEN')}; "
               "not a physical memory acceptance.")


def execute(scopes):
    if "expanded" in scopes:
        # Check the local platform and changed build inputs. Ready covers Windows and the release.
        subprocess.run(["cargo", "fmt", "--all", "--", "--check"], cwd=ROOT, check=True)
        subprocess.run(["cargo", "check", "--locked", "--workspace", "--all-targets"], cwd=ROOT, check=True)
        subprocess.run([sys.executable, "scripts/check_architecture.py"], cwd=ROOT, check=True)
        scopes = SCOPES
    if "kernel" in scopes:
        tests("okx-kernel", required=(GAP_TEST,))
    if "research" in scopes or "analysis" in scopes or "kernel" in scopes:
        tests("okx-strategy-core", required=(PLAN_TEST,))
    if "analysis" in scopes:
        tests("okx-analysis")
    if "research" in scopes:
        tests("okx-research")
    if "wire" in scopes:
        tests("okx-protocol")
    if "worker" in scopes:
        tests("okx-research-worker", required=WORKER_TESTS)
        perf = tests("okx-research-worker", required=(
            "analytics::executor::heavy_fixtures::two_heavy_research_jobs_ingestion_stop_and_cancel_latency",
        ), exact=True, show_output=True)
        report_t10(measured_t10(perf))
    elif "kernel" in scopes:
        tests("okx-research-worker", required=WORKER_TESTS, exact=True)
    if "mcp" in scopes:
        # The exact Agent IPC test below spawns a separate, actual Miniflare
        # process. Install its existing locked dependencies before that test.
        subprocess.run(["npm", "ci", "--ignore-scripts", "--no-audit", "--no-fund"], cwd=MCP, check=True)
    if "ipc" in scopes:
        tests("okx-research-worker", ("--test", "ipc_process"))
        metadata = subprocess.check_output(["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"],
                                           cwd=ROOT, text=True)
        suffix = ".exe" if os.name == "nt" else ""
        worker = Path(json.loads(metadata)["target_directory"]) / "debug" / ("okx-research-service" + suffix)
        os.environ["OKX_RESEARCH_SERVICE_TEST_EXE"] = str(worker)
        tests("okx-agent", required=("research_dispatch_tests::real_worker_decision_replay_and_scope_are_durable_without_source_reads",), exact=True)
        tests("okx-agent", required=("research_dispatch_tests::real_worker_live_signal_ipc_is_causal_and_has_no_source_or_finance_access",), exact=True)
        tests("okx-agent", required=("research_dispatch_tests::real_agent_worker_ipc_graph_stream_is_incremental_idempotent_and_read_only",), exact=True)
        tests("okx-agent", required=("research_dispatch_tests::t06_t09_real_worker_causality_and_economics_are_isolated",), exact=True)
        tests("okx-agent", required=("research_dispatch_tests::t06_real_worker_cross_instrument_oi_alignment_is_strict_and_isolated",), exact=True)
        tests("okx-agent", required=("research_dispatch_tests::t07_t08_real_worker_intraday_event_and_holdout_stay_separate",), exact=True)
        tests("okx-agent", required=("research_client::transport_tests::t11_t12_cancel_is_dispatchable_while_work_and_observation_lanes_are_full",), exact=True)
        native = tests("okx-agent", required=("accounts::native_trade_contracts::native_engine_research::engine_maintenance_and_stop_remain_independent_of_heavy_research_and_disconnect",), exact=True, show_output=True)
        report_native_under_research(measured_native_under_research(native))
        if "mcp" in scopes:
            mcp_native = tests("okx-agent", required=(
                "accounts::native_trade_contracts::native_engine_research::mcp_http_durable_object_wss_native_finance_stays_readable_under_heavy_research",
            ), exact=True, show_output=True,
                extra_env={"OKX_CONTROL_AGENT_INCAR": "ctl_" + "a" * 32})
            report_mcp_native_under_research(measured_mcp_native_under_research(mcp_native))
    if "mcp" in scopes:
        artifacts = [MCP / "contracts" / name for name in ("tools.schema.json", "action-fixtures.json")]
        before = [path.read_bytes() for path in artifacts]
        subprocess.run(["npm", "run", "check"], cwd=MCP, check=True)
        if before != [path.read_bytes() for path in artifacts]:
            raise ValueError("generated public MCP contracts differ from the input tree")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="origin/main", help="compare working tree with merge base")
    parser.add_argument("--plan", action="store_true", help="print selection without building")
    parser.add_argument("--expanded", action="store_true", help="also check broader semantic impact")
    args = parser.parse_args()
    scopes = {"expanded"} if args.expanded else set()
    for status, path in changes(args.base):
        selected = select(status, path)
        print(f"{status} {path!r}: {', '.join(sorted(selected)) or 'documentation'}", flush=True)
        scopes.update(selected)
    report(f"P0 on {git('rev-parse', 'HEAD').decode().strip()} vs {args.base}: "
           f"{', '.join(sorted(scopes)) or 'no code changes'}.")
    if args.plan and os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
            out.write(f"code={'true' if scopes else 'false'}\n")
    if not args.plan:
        execute(scopes)


if __name__ == "__main__":
    main()

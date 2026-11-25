"""Select and run Draft feedback on the checked-out tree; see docs/testing.md."""
import argparse
import json
import math
import os
from pathlib import Path
import re
import shlex
import shutil
import statistics
import subprocess
import sys
import time

from check_ci_native_scope import DRAFT_CHECKS, select_draft

ROOT = Path(__file__).resolve().parents[1]
MCP = ROOT / "cloudflare/okx-mcp"
PLAN_TEST = "query_plan::tests::k7_invalid_operator_bounds_or_parameter_fail_closed"
GAP_TEST = "time_series::tests::c13_missing_internal_candle_not_healed_by_duplicate_or_unclosed"
WORKER_TESTS = (
    "analytics::planner::tests::market_plan_rejects_unknown_operator_before_any_source_read",
    "analytics::market::tests::interior_gap_never_imputed",
    "analytics::market::tests::c20_rolling_mean_reuses_existing_market_pipeline_without_new_routing",
    "edge_runtime::market_tests::public_market_operations_work_from_research_owner_and_cursor_reads_persisted_job",
)
STANDALONE_MCP_TEST = (
    "edge_runtime::standalone_mcp_tests::standalone_catalog_reopens_through_real_mcp_do_websocket",
)
# These are the actual, independently owned Research process tests. The first
# three invoke the retained T06–T10 helpers inside their bodies. The two new
# privileged-provisioner denials run on both platforms; the negative startup
# test replaces the Windows-only Job test on Linux. Every case must execute
# in the *same* exact 12-case Research IPC target.
RESEARCH_IPC_PORTABLE = (
    "child_round_trip_source_and_eof_restart",
    "strategy_outbox_publishes_once_and_recovers_committed_proposal_after_restart",
    "incompatible_handshake_cannot_create_root",
    "cross_scope_restart_rejected",
    "replay_and_wrong_session_terminate_worker",
    "oversized_and_truncated_wire_terminate_worker",
    "lost_enqueue_ack_retry_preserves_job_and_rejects_conflict",
    "draining_preserves_active_job_and_rejects_new_admission",
    "t11_t12_real_worker_observation_and_cancel_survive_saturated_work_lane",
    "research_worker_has_no_privileged_credential_store_api_or_dependency",
    "privileged_edge_token_cli_is_disabled_before_stdin_or_catalog_on_all_platforms",
)
RESEARCH_IPC_WINDOWS = "legacy_and_standalone_workers_cannot_claim_separate_cpu_quotas"
RESEARCH_IPC_LINUX = "edge_runtime_process_rejects_missing_and_malformed_supervisor_bearers_without_catalog"
RESEARCH_IPC_HELPERS = (
    ('child_round_trip_source_and_eof_restart',
     'research_legacy_witnesses', 'causal_economics_and_portfolio_are_research_only'),
    ('strategy_outbox_publishes_once_and_recovers_committed_proposal_after_restart',
     'research_legacy_witnesses', 'cancel_and_durable_decision_replay_are_research_only'),
    ('cross_scope_restart_rejected',
     'research_extra_witnesses', 't07_t08_market_and_catalog_witnesses'),
)
LOCAL_COMPONENTS = {
    "strategy": "okx-strategy-core",
    "analysis": "okx-analysis",
    "research": "okx-research",
    "worker": "okx-research-service",
    "ipc": "okx-ipc-contracts",
    "engine": "okx-trading-engine",
    "execution": "okx-trade-execution",
    "controller": "okx-host-controller",
    "launcher": "okx-host-launcher",
    "host-protocol": "okx-host-protocol",
}
ENGINE_FEEDBACK = (
    "trade_service::strategies::pending_strategy_turn_does_not_delay_a_second_recovery_turn",
    "trade_service::strategies::budget_contract_tests::strategy_risk_budget_not_notional",
)


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT)


def changes(base):
    common = git("merge-base", base, "HEAD").decode().strip()
    fields = git("diff", "--name-status", "-z", "--no-renames", common, "--").split(b"\0")[:-1]
    paths = [(s.decode(), os.fsdecode(p)) for s, p in zip(fields[::2], fields[1::2])]
    paths.extend(("A", os.fsdecode(p)) for p in
                 git("ls-files", "--others", "--exclude-standard", "-z").split(b"\0") if p)
    return paths


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


def check_research_ipc_wiring(source=None):
    if source is None:
        source = (ROOT / "crates/okx-research-service/tests/ipc_process.rs").read_text(encoding="utf-8")
    # Ignore comments so a description of the witness cannot replace its
    # executable call. A change to these entry points needs an explicit review.
    executable = "\n".join(line.split("//", 1)[0] for line in source.splitlines())
    if executable.count("#[tokio::test]") != 12 or executable.count("#[test]") != 1:
        raise ValueError("Research IPC exact twelve process tests were altered")
    names = RESEARCH_IPC_PORTABLE + (RESEARCH_IPC_WINDOWS, RESEARCH_IPC_LINUX)
    for name in names:
        decorator = "#[test]" if name == "research_worker_has_no_privileged_credential_store_api_or_dependency" else "#[tokio::test]"
        body = rf'(?m)^{re.escape(decorator)}\s*\n(?:async\s+)?fn\s+{re.escape(name)}\s*\('
        if len(re.findall(body, executable)) != 1:
            raise ValueError(f"Research IPC required executable test missing/duplicated: {name}")
    for platform, name in (("windows", RESEARCH_IPC_WINDOWS),
                           ("not(windows)", RESEARCH_IPC_LINUX)):
        guarded = rf'(?m)^#\[cfg\({re.escape(platform)}\)\]\s*\n#\[tokio::test\]\s*\nasync\s+fn\s+{re.escape(name)}\s*\('
        if not re.search(guarded, executable):
            raise ValueError(f"Research IPC platform guard missing: {name}")
    for test_name, module, witness in RESEARCH_IPC_HELPERS:
        if not re.search(rf'\#\[path\s*=\s*"support/{module}\.rs"\]\s*mod\s+{module}\s*;', executable):
            raise ValueError(f"Research IPC owner helper not imported: {module}")
        test_body = re.search(
            rf'(?ms)^async\s+fn\s+{test_name}\s*\(\s*\)\s*\{{(.*?)(?=^#\[tokio::test\]|\Z)',
            executable)
        if not test_body or not re.search(
                rf'\b{module}::{witness}\s*\(\s*\)\s*\.await\s*;', test_body.group(1)):
            raise ValueError(f"Research IPC owner witness not executed by {test_name}: {witness}")
        helper = ROOT / f"crates/okx-research-service/tests/support/{module}.rs"
        if not helper.is_file() or not re.search(rf'\basync\s+fn\s+{witness}\s*\(', helper.read_text(encoding="utf-8")):
            raise ValueError(f"Research IPC owner witness implementation missing: {witness}")


def check_research_ipc_result(output, windows=None):
    if windows is None:
        windows = os.name == "nt"
    expected = 12
    # No ignored/filtered/replaced test may satisfy either platform's
    # twelve-case protected Research IPC roster.
    result = re.findall(r"^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; "
                        r"(\d+) measured; (\d+) filtered out;", output, re.M)
    if len(result) != 1 or result[0] != (str(expected), "0", "0", "0", "0"):
        raise ValueError(f"Research IPC process roster is not exactly {expected}/{expected}")


def check_financial_manifest_counts(manifest):
    # The separate finance collector still owns 62 distinct contracts, 104
    # crate witnesses and 3 repository checks. This draft migration must not
    # silently drop a financial property to fund a Research test relocation.
    if not isinstance(manifest, dict):
        raise ValueError("financial witness manifest missing")
    contracts = manifest.get("contracts")
    witnesses = manifest.get("witnesses")
    repository = manifest.get("repository_witnesses")
    if (not isinstance(contracts, list) or len(contracts) != 62
            or not all(isinstance(value, str) and value for value in contracts)
            or len(set(contracts)) != 62
            or not isinstance(witnesses, list) or len(witnesses) != 104
            or not isinstance(repository, list) or len(repository) != 3):
        raise ValueError("financial property roster must retain 62 contracts and 107 witnesses")
    identities = []
    for entry in witnesses + repository:
        if (not isinstance(entry, dict) or not isinstance(entry.get("package"), str)
                or not isinstance(entry.get("test"), str)
                or not entry["package"] or not entry["test"]):
            raise ValueError("financial witness identity is invalid")
        identities.append((entry["package"], entry["test"]))
    repository_names = {
        "architecture_and_research_negative_guards",
        "measured_native_workload_and_resource_budgets",
        "retained_workerd_auth_session_transport_no_financial_owner",
    }
    if (len(set(identities)) != 107
            or {entry["test"] for entry in repository} != repository_names
            or any(entry["package"] != "repository" for entry in repository)):
        raise ValueError("financial witness roster is duplicate or missing repository gates")


def self_test():
    check_research_ipc_wiring()
    source = (ROOT / "crates/okx-research-service/tests/ipc_process.rs").read_text(encoding="utf-8")
    for name in RESEARCH_IPC_PORTABLE + (RESEARCH_IPC_WINDOWS, RESEARCH_IPC_LINUX):
        changed = re.sub(rf'\bfn\s+{re.escape(name)}\s*\(', f'fn removed_{name}(', source, count=1)
        if changed == source:
            raise ValueError(f"missing Research IPC test declaration: {name}")
        try:
            check_research_ipc_wiring(changed)
        except ValueError:
            pass
        else:
            raise ValueError(f"removed Research IPC test was accepted: {name}")
    for _, module, witness in RESEARCH_IPC_HELPERS:
        changed = source.replace(f"{module}::{witness}().await;", "", 1)
        if changed == source:
            raise ValueError(f"missing executable Research witness call: {witness}")
        try:
            check_research_ipc_wiring(changed)
        except ValueError:
            pass
        else:
            raise ValueError(f"removed Research witness was accepted: {witness}")
    for windows in (False, True):
        names = RESEARCH_IPC_PORTABLE + ((RESEARCH_IPC_WINDOWS,) if windows else (RESEARCH_IPC_LINUX,))
        rows = "\n".join(f"test {name} ... ok" for name in names)
        result = f"test result: ok. {len(names)} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;"
        check_counts(rows, names)
        check_research_ipc_result(rows + "\n" + result, windows=windows)
        for bad in (
            rows.replace(f"test {names[0]} ... ok", f"test {names[0]} ... ignored") + "\n" + result,
            rows + "\n" + result.replace("0 ignored", "1 ignored"),
            rows + "\n" + result.replace(f"{len(names)} passed", f"{len(names)-1} passed"),
            rows + "\n" + result + "\n" + result,
        ):
            try:
                check_counts(bad, names)
                check_research_ipc_result(bad, windows=windows)
            except ValueError:
                pass
            else:
                raise ValueError("missing/skipped/duplicate Research process evidence accepted")
    synthetic_t10 = {
        "source_reads": {
            "kind": "actual_research_ipc_public_candles_requests_to_synthetic_handler",
            "successful_requests": 10, "returned_candle_rows": 1000,
            "venue_http_reads": 0, "source_storage_page_reads": "not_measured",
            "observation_scope": "request_and_rows_counted_in_research_owned_fixture_source_handler",
        },
        "phase_wall_ms_excluding_build": 1000.0,
        "pre_source_boundaries": {
            "schema": "okx.518.t10-real-child-pre-source-boundaries/v1",
            "route": "research_owned_fixture_to_worker_ipc",
            "admission_only_not_executed": [
                "one_million_bars_one_instrument_64", "eight_instruments_one_million_bars_64"],
            "rejected_before_source": {
                "one_million_plus_one_bars": "INVALID_RESEARCH_REQUEST",
                "nine_instruments": "INVALID_RESEARCH_REQUEST",
                "sixty_five_variants": "INVALID_RESEARCH_REQUEST",
            },
            "source_requests_before": 0, "source_requests_after": 0,
            "returned_rows_before": 0, "returned_rows_after": 0,
            "full_scale_execution": "not_attempted", "side_effects": "none",
        },
    }
    check_source_witness(synthetic_t10)
    check_real_child_boundaries(synthetic_t10)
    for key, legacy in (
        ("route", "real_agent_to_separate_research_worker_ipc"),
        ("source_requests_after", 1),
        ("returned_rows_after", 1),
    ):
        phase = json.loads(json.dumps(synthetic_t10))
        phase["pre_source_boundaries"][key] = legacy
        try:
            check_real_child_boundaries(phase)
        except ValueError:
            pass
        else:
            raise ValueError(f"unverified T10 Research boundary accepted: {key}")
    phase = json.loads(json.dumps(synthetic_t10))
    phase["source_reads"]["observation_scope"] = "request_and_rows_counted_in_native_source_handler"
    try:
        check_source_witness(phase)
    except ValueError:
        pass
    else:
        raise ValueError("retired Agent-owned source receipt accepted as Research-owned")
    manifest = json.loads(subprocess.check_output(
        ["node", "scripts/ci-finance-witness-manifest.cjs"], cwd=ROOT, text=True))
    check_financial_manifest_counts(manifest)
    for field in ("contracts", "witnesses", "repository_witnesses"):
        changed = {**manifest, field: manifest[field][:-1]}
        try:
            check_financial_manifest_counts(changed)
        except ValueError:
            pass
        else:
            raise ValueError(f"reduced financial {field} roster accepted")
    print("P0_RESEARCH_OWNER_IPC_AND_FINANCE_NEGATIVE_CONTROLS=PASS")


def test_command(package, target=("--lib",), required=(), exact=False, show_output=False,
                 ignored=False, test_threads=2):
    cmd = ["cargo", "test", "--locked", "-p", package, *target, "--message-format=json-render-diagnostics"]
    cmd.extend(["--", "--test-threads", str(test_threads)])
    if exact:
        cmd.extend(["--exact", *required])
    if show_output:
        cmd.append("--show-output")
    if ignored:
        cmd.append("--ignored")
    return cmd


def tests(package, target=("--lib",), required=(), exact=False, show_output=False,
          extra_env=None, ignored=False, test_threads=2):
    if package == "okx-research-service" and target == ("--test", "ipc_process"):
        check_research_ipc_wiring()
        if test_threads != 1:
            raise ValueError("Research IPC real children must execute serially")
    cmd = test_command(package, target, required, exact, show_output, ignored, test_threads)
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
    if package == "okx-research-service" and target == ("--test", "ipc_process"):
        check_research_ipc_result("\n".join(output))
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
            or source.get("observation_scope") != "request_and_rows_counted_in_research_owned_fixture_source_handler"
            or not isinstance(elapsed, (int, float)) or not math.isfinite(elapsed)
            or elapsed <= 0):
        raise ValueError("T10 missing actual typed synthetic-source reads or build-excluded phase timing")


def check_real_child_boundaries(phase):
    witness = phase.get("pre_source_boundaries", {})
    expected = {"one_million_plus_one_bars", "nine_instruments", "sixty_five_variants"}
    if (witness.get("schema") != "okx.518.t10-real-child-pre-source-boundaries/v1"
            or witness.get("route") != "research_owned_fixture_to_worker_ipc"
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
        report("T10 real Research-owned fixture->Worker IPC admission boundaries: "
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
    if scopes & {"expanded", "mcp"}:
        # Fast, dependency-free public product contract guard. Run before
        # Cargo/Node setup so an unsafe Edge or Engine tool/schema change
        # fails immediately even in a narrowly selected Draft check.
        subprocess.run([sys.executable, "scripts/check_product_contract.py", "--self-test"], cwd=ROOT, check=True)
        subprocess.run([sys.executable, "scripts/check_product_contract.py"], cwd=ROOT, check=True)
    if "expanded" in scopes:
        # Check the local platform and changed build inputs. Ready covers Windows and the release.
        subprocess.run(["cargo", "check", "--locked", "--workspace", "--all-targets"], cwd=ROOT, check=True)
        subprocess.run([sys.executable, "scripts/check_architecture.py"], cwd=ROOT, check=True)
        scopes = DRAFT_CHECKS
    if "host" in scopes:
        # Host-only changes run both executables and their typed protocol;
        # the MCP bridge consumes these results without a trading Engine.
        tests("okx-host-protocol", ())
        tests("okx-host-launcher", ())
        tests("okx-host-controller", ())
        tests("okx-bridge-mcp", ())
    if "kernel" in scopes:
        tests("okx-research-domain")
    if "strategy" in scopes or "analysis" in scopes or "research" in scopes or "kernel" in scopes:
        tests("okx-strategy-core", required=(PLAN_TEST, GAP_TEST))
    if "analysis" in scopes:
        tests("okx-analysis")
    if "research" in scopes:
        tests("okx-research")
    if "wire" in scopes:
        tests("okx-ipc-contracts")
        tests("okx-research-protocol")
    if "worker" in scopes:
        tests("okx-research-service", required=WORKER_TESTS)
        perf = tests("okx-research-service", required=(
            "analytics::cpu_executor::heavy_fixtures::two_heavy_research_jobs_ingestion_stop_and_cancel_latency",
        ), exact=True, show_output=True)
        report_t10(measured_t10(perf))
    elif "kernel" in scopes:
        tests("okx-research-service", required=WORKER_TESTS, exact=True)
    if "mcp" in scopes:
        # The exact Agent IPC test below spawns a separate, actual Miniflare
        # process. Install its existing locked dependencies before that test.
        subprocess.run(["npm", "ci", "--ignore-scripts", "--no-audit", "--no-fund"], cwd=MCP, check=True)
        if "worker" in scopes:
            tests("okx-research-service", required=STANDALONE_MCP_TEST, exact=True, ignored=True)
    if scopes & {"ipc", "worker-ipc"}:
        required = RESEARCH_IPC_PORTABLE + ((RESEARCH_IPC_WINDOWS,) if os.name == "nt" else ())
        tests("okx-research-service", ("--test", "ipc_process"),
              required=required, test_threads=1)
    if "worker-ipc" in scopes:
        tests("okx-research-service", ("--test", "causal_release_smoke"))
    if "ipc" in scopes:
        # Binary renaming and the owner split must preserve the installed
        # three-file Research release and rollback with Engine entirely absent.
        # These are CONTROL-owned install proofs, not retired Engine test shims.
        tests("okx-host-controller", required=(
            "artifact::research_component::tests::standalone_research_stages_and_rolls_back_with_engine_entirely_absent",
        ), exact=True)
        tests("okx-host-controller", required=(
            "artifact::research_component::tests::research_staging_publishes_only_verified_version_and_preserves_agent_state",
        ), exact=True)
        tests("okx-host-controller", required=(
            "artifact::research_component::tests::legacy_bundle_cannot_carry_research_worker",
        ), exact=True)
        metadata = subprocess.check_output(["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"],
                                           cwd=ROOT, text=True)
        suffix = ".exe" if os.name == "nt" else ""
        worker = Path(json.loads(metadata)["target_directory"]) / "debug" / ("okx-research-service" + suffix)
        fixture = worker.with_name("okx-research-load-fixture" + suffix)
        # T10 financial process contracts still execute the real Worker under
        # two heavy CPU Grids. Research owns this debug-only fixture binary
        # and its private typed protocol; Engine drives only bounded JSONL.
        # The shipping Worker and the accepted Research ZIP remain unchanged.
        subprocess.run(["cargo", "build", "--locked", "-p", "okx-research-service",
                        "--bin", "okx-research-load-fixture", "--features", "test-fixture"],
                       cwd=ROOT, check=True)
        subprocess.run(["cargo", "build", "--locked", "-p", "okx-research-service",
                        "--bin", "okx-research-service", "--features", "test-isolated-singleton"],
                       cwd=ROOT, check=True)
        isolated_worker = worker.with_name("okx-research-service-test-isolated" + suffix)
        shutil.copy2(worker, isolated_worker)
        os.environ["OKX_RESEARCH_SERVICE_TEST_EXE"] = str(isolated_worker)
        os.environ["OKX_RESEARCH_LOAD_FIXTURE_EXE"] = str(fixture)
        native = tests("okx-trading-engine", required=("accounts::native_trade_contracts::native_engine_research::engine_maintenance_and_stop_remain_independent_of_heavy_research_and_disconnect",), exact=True, show_output=True)
        report_native_under_research(measured_native_under_research(native))
        if "mcp" in scopes:
            mcp_native = tests("okx-trading-engine", required=(
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


def local_component(component, repeat, plan):
    package = LOCAL_COMPONENTS.get(component)
    command = shlex.join(test_command(package, required=ENGINE_FEEDBACK,
                                      exact=True)) if component == "engine" else (
        shlex.join(test_command(package)) if package else
        "npm run check (cwd=cloudflare/okx-mcp)")
    checkout = git("rev-parse", "HEAD").decode().strip()
    dirty = bool(git("status", "--porcelain"))
    report(f"Local component check: {component}, HEAD={checkout}, dirty={str(dirty).lower()}, "
           f"repetitions={repeat}, command={command}. Scope: code tests.")
    if plan:
        return
    samples = []
    for _ in range(repeat):
        started = time.monotonic()
        if component == "engine":
            tests(package, required=ENGINE_FEEDBACK, exact=True)
        elif package:
            tests(package)
        else:
            subprocess.run(["npm", "run", "check"], cwd=MCP, check=True)
        samples.append(time.monotonic() - started)
    report(f"{component} first observed run: {samples[0]:.2f}s (initial cache state unspecified).")
    if repeat > 1:
        report(f"{component} follow-up runs: {', '.join(f'{s:.2f}s' for s in samples[1:])}; "
               f"median {statistics.median(samples[1:]):.2f}s including build and tests.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="origin/main", help="compare working tree with merge base")
    parser.add_argument("--plan", action="store_true", help="print selection without building")
    parser.add_argument("--expanded", action="store_true", help="also check broader semantic impact")
    parser.add_argument("--self-test", action="store_true",
                        help="check Research owner IPC and financial roster negative controls without Cargo")
    parser.add_argument("--checks", help="execute the comma-separated checks emitted by the CI classifier")
    parser.add_argument("--component", choices=sorted({*LOCAL_COMPONENTS, "mcp"}),
                        help="test one component locally")
    parser.add_argument("--repeat", type=int, default=1,
                        help="repeat the local component check in the same build directory")
    args = parser.parse_args()
    if args.self_test:
        if args.checks is not None or args.component or args.plan or args.expanded or args.repeat != 1:
            parser.error("--self-test cannot be combined with execution or plan options")
        self_test()
        return
    if args.checks is not None:
        scopes = set(args.checks.split(",")) if args.checks else set()
        if not scopes <= DRAFT_CHECKS or args.component or args.plan or args.expanded or args.repeat != 1:
            parser.error("--checks requires an unmodified CI check list")
        execute(scopes)
        return
    if args.component:
        if args.expanded or args.repeat < 1:
            parser.error("--component requires a positive --repeat and excludes --expanded")
        local_component(args.component, args.repeat, args.plan)
        return
    if args.repeat != 1:
        parser.error("--repeat requires --component")
    scopes = {"expanded"} if args.expanded else set()
    for status, path in changes(args.base):
        selected = select_draft({"A": "added", "M": "modified"}.get(status, status), path)
        print(f"{status} {path!r}: {', '.join(sorted(selected)) or 'documentation'}", flush=True)
        scopes.update(selected)
    report(f"P0 on {git('rev-parse', 'HEAD').decode().strip()} vs {args.base}: "
           f"{', '.join(sorted(scopes)) or 'no code changes'}.")
    if not args.plan:
        execute(scopes)


if __name__ == "__main__":
    main()

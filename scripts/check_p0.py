"""Select and run Draft feedback on the checked-out tree; see docs/testing.md."""
import argparse
import json
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
    if path in {"crates/okx-kernel/src/" + f for f in ("operators.rs", "query_plan.rs", "time_series.rs")}:
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


def tests(package, target=("--lib",), required=(), exact=False, show_output=False):
    cmd = ["cargo", "test", "--locked", "-p", package, *target, "--message-format=json-render-diagnostics"]
    cmd.extend(["--", "--test-threads", "2"])
    if exact:
        cmd.extend(["--exact", *required])
    if show_output:
        cmd.append("--show-output")
    print(shlex.join(cmd), flush=True)
    started = time.monotonic()
    run = subprocess.run(cmd, cwd=ROOT, stdout=subprocess.PIPE, text=True)
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


def execute(scopes):
    if "expanded" in scopes:
        # Check the local platform and changed build inputs. Ready covers Windows and the release.
        subprocess.run(["cargo", "check", "--locked", "--workspace", "--all-targets"], cwd=ROOT, check=True)
        subprocess.run([sys.executable, "scripts/check_architecture.py"], cwd=ROOT, check=True)
        scopes = SCOPES
    if "kernel" in scopes:
        tests("okx-kernel", required=(PLAN_TEST, GAP_TEST))
    if "analysis" in scopes:
        tests("okx-analysis")
    if "research" in scopes:
        tests("okx-research")
    if "wire" in scopes:
        tests("okx-protocol")
    if "worker" in scopes:
        tests("okx-research-worker", required=WORKER_TESTS)
    elif "kernel" in scopes:
        tests("okx-research-worker", required=WORKER_TESTS, exact=True)
    if "ipc" in scopes:
        tests("okx-research-worker", ("--test", "ipc_process"))
        metadata = subprocess.check_output(["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"],
                                           cwd=ROOT, text=True)
        suffix = ".exe" if os.name == "nt" else ""
        worker = Path(json.loads(metadata)["target_directory"]) / "debug" / ("okx-research-worker" + suffix)
        os.environ["OKX_RESEARCH_WORKER_TEST_EXE"] = str(worker)
        tests("okx-agent", required=("research_dispatch_tests::real_worker_decision_replay_and_scope_are_durable_without_source_reads",), exact=True)
        tests("okx-agent", required=("research_dispatch_tests::t06_t09_real_worker_causality_and_economics_are_isolated",), exact=True)
        tests("okx-agent", required=("research_client::transport_tests::t11_t12_cancel_is_dispatchable_while_work_and_observation_lanes_are_full",), exact=True)
        tests("okx-agent", required=("accounts::native_trade_contracts::native_engine_research::engine_maintenance_and_stop_remain_independent_of_heavy_research_and_disconnect",), exact=True, show_output=True)
    if "mcp" in scopes:
        subprocess.run(["npm", "ci", "--ignore-scripts", "--no-audit", "--no-fund"], cwd=MCP, check=True)
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

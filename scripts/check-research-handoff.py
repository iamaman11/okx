"""Check CONTROL and Engine readbacks for a Research-only update or rollback."""
import argparse
import json
import re
from datetime import datetime
from pathlib import Path


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def control_receipt(receipt, operations):
    require(receipt["schema"] == "okx.windows.control.result/v1"
            and receipt["status"] == "PASS" and receipt["failure"] is None
            and receipt["operation"]["type"] in operations,
            "expected a successful CONTROL receipt for " + " or ".join(operations))
    return receipt["details"]


def sha256(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def observation(control, result):
    details = control_receipt(control, ("status",))
    installed = details["installed_agent"]
    engine_pid = details["agent_pid"]
    engine_sha = installed["binary_sha256"]
    incarnation = details["agent_incarnation"]
    require(details["agent_owned_running"] is True and details["release_parity"]["state"] == "MATCHED"
            and installed["state"] == "VERIFIED" and sha256(engine_sha)
            and isinstance(incarnation, str) and re.fullmatch(r"ctl_[0-9a-f]{32}", incarnation)
            and type(engine_pid) is int and engine_pid > 0, "CONTROL does not verify the running Engine")

    # Accept the data field from the documented MCP diagnostic. A failed tool
    # call must not be interpreted as a deployment observation.
    require(result["status"] == "completed", "Research deployment diagnostic failed")
    deployment = result["data"]
    require(deployment["schema"] == "okx.research.deployment/v1"
            and deployment["engine_pid"] == engine_pid and deployment["read_only"] is True,
            "Research diagnostic and CONTROL do not refer to the same Engine")
    observed_at_ms = deployment["observed_at_ms"]
    require(type(observed_at_ms) is int and observed_at_ms > 0,
            "Research diagnostic has no native Engine capture timestamp")
    research = deployment["research"]
    require(sha256(research["active"]) and research["installed"] == research["active"]
            and research["installed_check_error"] is None and research["draining"] is False
            and type(research["active_pid"]) is int and research["active_pid"] > 0
            and type(research["handshake_verified_at_ms"]) is int
            and research["handshake_verified_at_ms"] > 0,
            "Research has no matching installed and handshake-verified active image")
    require(research["handshake_verified_at_ms"] <= observed_at_ms,
            "Research handshake is newer than its diagnostic observation")
    return (engine_pid, engine_sha, incarnation), research, observed_at_ms


def verify(before_control, before_research, change, after_control, after_research):
    before_engine, before_worker, before_observed_ms = observation(before_control, before_research)
    after_engine, after_worker, after_observed_ms = observation(after_control, after_research)
    receipt = control_receipt(change, ("deploy_research", "rollback_research"))
    operation = change["operation"]
    kind = operation["type"]
    timeline = [datetime.fromisoformat(result["observed_at"]) for result in
                (before_control, change, after_control)]
    require(all(time.tzinfo is not None for time in timeline)
            and timeline[0] < timeline[1] < timeline[2],
            "CONTROL readbacks must bracket the requested change")
    control_ms = [int(moment.timestamp() * 1_000) for moment in timeline]
    require(control_ms[0] <= before_observed_ms < control_ms[1]
            and control_ms[2] <= after_observed_ms,
            "Research readbacks must bracket CONTROL change on the same Engine clock")
    require(receipt["agent_lifecycle_changed"] is False and before_engine == after_engine,
            "Engine continuity was not proven")
    require(receipt["provenance"]["worker_sha256"] == after_worker["active"]
            and before_worker["active"] != after_worker["active"]
            and after_worker["handshake_verified_at_ms"] > before_worker["handshake_verified_at_ms"]
            and after_worker["handshake_verified_at_ms"] > before_observed_ms,
            "the selected Research image did not complete a new handshake")
    if kind == "rollback_research":
        require(receipt["rolled_back"] is True
                and operation["expected_current_worker_sha256"] == before_worker["active"]
                and operation["expected_rollback_worker_sha256"] == after_worker["active"],
                "rollback did not restore the named previous image")
    else:
        require(receipt["installed"] is True, "CONTROL did not stage the selected Research image")

    return {
        "schema": "okx.research.handoff-check/v1",
        "status": "MATCHED",
        "operation": kind,
        "engine_pid": after_engine[0],
        "engine_sha256": after_engine[1],
        "research_before_sha256": before_worker["active"],
        "research_after_sha256": after_worker["active"],
        "research_after_pid": after_worker["active_pid"],
        "before_research_observed_at_ms": before_observed_ms,
        "after_research_observed_at_ms": after_observed_ms,
        "control_timing_ms": receipt.get("timing_ms"),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("before_control", type=Path)
    parser.add_argument("before_research", type=Path)
    parser.add_argument("change", type=Path)
    parser.add_argument("after_control", type=Path)
    parser.add_argument("after_research", type=Path)
    args = parser.parse_args()
    try:
        receipts = [json.loads(path.read_text(encoding="utf-8-sig")) for path in vars(args).values()]
        report = verify(*receipts)
    except (KeyError, ValueError, TypeError, OSError) as error:
        parser.exit(1, f"Research handoff could not be verified: {error}\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone


CHECK = Path(__file__).with_name("check-research-handoff.py")
WINDOW_START_MS = int(datetime(2026, 11, 24, 10, tzinfo=timezone.utc).timestamp() * 1_000)


def control(operation, second, details):
    return {
        "schema": "okx.windows.control.result/v1",
        "status": "PASS",
        "failure": None,
        "operation": operation,
        "observed_at": f"2026-11-24T10:00:{second:02d}Z",
        "details": details,
    }


def status(second, incarnation="ctl_" + "1" * 32):
    return control({"type": "status"}, second, {
        "agent_pid": 42,
        "agent_owned_running": True,
        "agent_incarnation": incarnation,
        "release_parity": {"state": "MATCHED"},
        "installed_agent": {"state": "VERIFIED", "binary_sha256": "c" * 64},
    })


def research(sha, pid, handshake, observed=3_500):
    return {"status": "completed", "data": {
        "schema": "okx.research.deployment/v1",
        "engine_pid": 42,
        "observed_at_ms": WINDOW_START_MS + observed,
        "read_only": True,
        "research": {"active": sha, "installed": sha, "active_pid": pid,
                     "handshake_verified_at_ms": WINDOW_START_MS + handshake, "draining": False,
                     "installed_check_error": None},
    }}


class ResearchHandoff(unittest.TestCase):
    def check(self, *values):
        with tempfile.TemporaryDirectory() as scratch:
            paths = [Path(scratch) / f"receipt-{index}.json" for index in range(len(values))]
            for path, value in zip(paths, values):
                path.write_text(json.dumps(value), encoding="utf-8")
            return subprocess.run([sys.executable, CHECK, *paths], capture_output=True, text=True)

    def test_update_then_rollback_with_same_engine(self):
        old, new = "a" * 64, "b" * 64
        deploy = control({"type": "deploy_research"}, 2, {
            "installed": True, "agent_lifecycle_changed": False,
            "provenance": {"worker_sha256": new}, "timing_ms": {"control_total": 200}})
        first = research(old, 100, 1_000, observed=1_500)
        updated = research(new, 101, 2_000)
        result = self.check(status(1), first, deploy, status(3), updated)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["research_after_sha256"], new)
        rollback = control({"type": "rollback_research",
                            "expected_current_worker_sha256": new,
                            "expected_rollback_worker_sha256": old}, 4, {
            "rolled_back": True, "agent_lifecycle_changed": False,
            "provenance": {"worker_sha256": old}})
        result = self.check(status(3), updated, rollback, status(5),
                            research(old, 102, 4_000, observed=5_500))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["research_after_sha256"], old)

    def test_ack_without_activation_and_reused_engine_pid_are_rejected(self):
        old, new = "a" * 64, "b" * 64
        deploy = control({"type": "deploy_research"}, 2, {
            "installed": True, "agent_lifecycle_changed": False,
            "provenance": {"worker_sha256": new}})
        first = research(old, 100, 1_000, observed=1_500)
        stale = self.check(status(1), first, deploy, status(3), research(old, 100, 1_000))
        self.assertNotEqual(stale.returncode, 0)
        self.assertIn("new handshake", stale.stderr)
        restarted = self.check(status(1), first, deploy, status(3, "ctl_" + "2" * 32),
                               research(new, 101, 2_000))
        self.assertNotEqual(restarted.returncode, 0)
        self.assertIn("Engine continuity", restarted.stderr)
        older = self.check(status(1), research(old, 100, 100, observed=1_500), deploy, status(3),
                           research(new, 101, 200))
        self.assertNotEqual(older.returncode, 0)
        self.assertIn("new handshake", older.stderr)

    def test_stale_or_future_research_capture_cannot_certify_handoff(self):
        old, new = "a" * 64, "b" * 64
        deploy = control({"type": "deploy_research"}, 2, {
            "installed": True, "agent_lifecycle_changed": False,
            "provenance": {"worker_sha256": new}})
        before = research(old, 100, 1_000, observed=1_500)
        # The newer child appears before CONTROL's mutation. A stale cached
        # readback must not be accepted as proof of this operation.
        stale = self.check(status(1), before, deploy, status(3),
                           research(new, 101, 1_800, observed=1_900))
        self.assertNotEqual(stale.returncode, 0)
        self.assertIn("readbacks must bracket", stale.stderr)
        future = self.check(status(1), before, deploy, status(3),
                            research(new, 101, 4_000, observed=3_500))
        self.assertNotEqual(future.returncode, 0)
        self.assertIn("newer than its diagnostic", future.stderr)
        late_before = self.check(status(1), research(old, 100, 1_000, observed=2_500),
                                 deploy, status(3), research(new, 101, 2_700))
        self.assertNotEqual(late_before.returncode, 0)
        self.assertIn("readbacks must bracket", late_before.stderr)


if __name__ == "__main__":
    unittest.main()

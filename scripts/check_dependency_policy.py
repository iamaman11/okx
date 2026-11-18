#!/usr/bin/env python3
"""Check bounded maintenance-only cargo-deny exceptions against the resolved graph."""
import argparse
import datetime as dt
import json
from pathlib import Path
import re
import sys
import tempfile
import tomllib
import unittest

ROOT = Path(__file__).resolve().parent.parent


def fail(message):
    raise ValueError(message)


def check(policy, config, graph, advisory_db, today):
    exceptions = policy.get("workspace", {}).get("metadata", {}).get("dependency-policy", {}).get("maintenance-exceptions", [])
    ignores = config.get("advisories", {}).get("ignore", [])
    if not isinstance(exceptions, list) or not isinstance(ignores, list):
        fail("exceptions and advisory ignores must be lists")
    records = {}
    for item in exceptions:
        if not isinstance(item, dict) or not isinstance(item.get("id"), str) or item["id"] in records:
            fail("invalid or duplicate maintenance exception")
        records[item["id"]] = item
    ignored = {}
    for item in ignores:
        if not isinstance(item, dict) or set(item) != {"id", "reason"} or not isinstance(item["id"], str) or item["id"] in ignored or not isinstance(item["reason"], str) or not item["reason"].strip():
            fail("ignore must have a unique id and nonempty reason")
        ignored[item["id"]] = item
    if set(records) != set(ignored):
        fail("advisory ignores and scoped maintenance exceptions differ")
    packages = graph.get("packages")
    nodes = (graph.get("resolve") or {}).get("nodes")
    if not isinstance(packages, list) or not isinstance(nodes, list):
        fail("cargo metadata must include packages and resolve.nodes")
    by_id = {pkg["id"]: pkg for pkg in packages}
    if len(by_id) != len(packages):
        fail("duplicate package IDs in graph")
    node_ids = {node["id"] for node in nodes}
    if node_ids != set(by_id):
        fail("cargo metadata graph is incomplete")
    outcomes = []
    for advisory_id, item in records.items():
        if not re.fullmatch(r"RUSTSEC-\d{4}-\d{4,}", advisory_id):
            fail("invalid advisory ID")
        package, version, owner = (item.get(k) for k in ("package", "version", "owner"))
        if not all(isinstance(x, str) and x.strip() for x in (package, version, owner)):
            fail(f"{advisory_id}: package, version and owner are required")
        try:
            expiry = dt.date.fromisoformat(item["expires"])
        except (KeyError, ValueError, TypeError):
            fail(f"{advisory_id}: invalid expiry")
        if today >= expiry:
            fail(f"{advisory_id}: exception expired {expiry}")
        consumers = item.get("consumers")
        if not isinstance(consumers, list) or not consumers or any(not isinstance(x, str) or not x.strip() for x in consumers) or len(consumers) != len(set(consumers)):
            fail(f"{advisory_id}: invalid consumer scope")
        matches = [p for p in packages if p["name"] == package]
        if len(matches) != 1 or matches[0]["version"] != version:
            fail(f"{advisory_id}: resolved package/version differs from scope")
        target = matches[0]["id"]
        actual = set()
        for node in nodes:
            deps = node.get("deps")
            if not isinstance(deps, list):
                fail("cargo metadata node missing deps")
            if any(dep.get("pkg") == target for dep in deps):
                consumer = by_id[node["id"]]
                actual.add(f"{consumer['name']}@{consumer['version']}")
        if actual != set(consumers):
            fail(f"{advisory_id}: direct consumers differ: actual={sorted(actual)} expected={sorted(consumers)}")
        files = list(Path(advisory_db).rglob(f"{advisory_id}.md"))
        if len(files) != 1:
            fail(f"{advisory_id}: advisory missing or duplicated")
        content = files[0].read_text(encoding="utf-8")
        match = re.match(r"\A\s*```toml\s*\n(.*?)\n```", content, re.DOTALL)
        if not match:
            fail(f"{advisory_id}: malformed advisory metadata")
        metadata = tomllib.loads(match.group(1))
        advisory = metadata.get("advisory", {})
        if advisory.get("id") != advisory_id or advisory.get("package") != package or advisory.get("informational") != "unmaintained" or metadata.get("versions", {}).get("patched") != []:
            fail(f"{advisory_id}: advisory is not an unpatched maintenance-only advisory")
        outcomes.append(f"{advisory_id} {package}@{version}, expires {expiry}, consumers {', '.join(sorted(actual))}")
    return outcomes


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name)
        self.advisory = self.db / "RUSTSEC-2026-0543.md"
        self.advisory.write_text('```toml\n[advisory]\nid="RUSTSEC-2026-0543"\npackage="chrono"\ninformational="unmaintained"\n[versions]\npatched=[]\n```\n', encoding="utf-8")
        self.item = {"id": "RUSTSEC-2026-0543", "package": "chrono", "version": "0.4.45", "expires": "2026-12-18", "owner": "maintainer", "consumers": ["app@1.0.0"]}
        self.policy = {"workspace": {"metadata": {"dependency-policy": {"maintenance-exceptions": [self.item]}}}}
        self.config = {"advisories": {"ignore": [{"id": self.item["id"], "reason": "maintenance"}]}}
        self.graph = {"packages": [{"id": "chrono-id", "name": "chrono", "version": "0.4.45"}, {"id": "app-id", "name": "app", "version": "1.0.0"}], "resolve": {"nodes": [{"id": "chrono-id", "deps": []}, {"id": "app-id", "deps": [{"pkg": "chrono-id"}]}]}}
        self.today = dt.date(2026, 11, 18)

    def run_check(self):
        return check(self.policy, self.config, self.graph, self.db, self.today)

    def test_valid(self):
        self.assertEqual(len(self.run_check()), 1)

    def test_expiry(self):
        self.today = dt.date(2026, 12, 18)
        with self.assertRaisesRegex(ValueError, "expired"):
            self.run_check()

    def test_new_consumer(self):
        self.graph["packages"].append({"id": "new-id", "name": "new", "version": "1.0.0"})
        self.graph["resolve"]["nodes"].append({"id": "new-id", "deps": [{"pkg": "chrono-id"}]})
        with self.assertRaisesRegex(ValueError, "consumers differ"):
            self.run_check()

    def test_version_change(self):
        self.graph["packages"][0]["version"] = "0.4.46"
        with self.assertRaisesRegex(ValueError, "package/version"):
            self.run_check()

    def test_reclassified_advisory(self):
        self.advisory.write_text(self.advisory.read_text().replace('informational="unmaintained"', 'informational="unsound"'))
        with self.assertRaisesRegex(ValueError, "maintenance-only"):
            self.run_check()

    def test_unscoped_ignore(self):
        self.config["advisories"]["ignore"].append({"id": "RUSTSEC-2026-9999", "reason": "unscoped"})
        with self.assertRaisesRegex(ValueError, "differ"):
            self.run_check()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--graph", type=Path)
    parser.add_argument("--advisory-db", type=Path)
    parser.add_argument("--config", type=Path, default=ROOT / "deny.toml")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return 0 if unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(PolicyTests)).wasSuccessful() else 1
    if not args.graph or not args.advisory_db or not args.advisory_db.is_dir():
        parser.error("--graph and an existing --advisory-db directory are required")
    try:
        with (ROOT / "Cargo.toml").open("rb") as f:
            policy = tomllib.load(f)
        with args.config.open("rb") as f:
            config = tomllib.load(f)
        graph = json.loads(args.graph.read_text(encoding="utf-8"))
        outcomes = check(policy, config, graph, args.advisory_db, dt.datetime.now(dt.timezone.utc).date())
    except (ValueError, KeyError, TypeError, OSError) as error:
        print(f"dependency policy FAIL: {error}", file=sys.stderr)
        return 1
    print("dependency policy PASS: " + ("; ".join(outcomes) if outcomes else "no advisory ignores"))
    return 0


if __name__ == "__main__":
    sys.exit(main())

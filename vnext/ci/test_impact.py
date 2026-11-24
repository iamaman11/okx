"""Fast negative controls for the vNext owner impact decision."""

from pathlib import Path
import json
import subprocess
import tempfile
import unittest

from impact import changed_paths, plan_for_paths, write_plan


def git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True,
    )
    return completed.stdout.strip()


class OwnerImpact(unittest.TestCase):
    def test_research_only_never_rebuilds_engine_or_finance(self):
        report = plan_for_paths(["vnext/research/src/application/replay.rs"])
        self.assertEqual(report["owners"], {
            "engine": False, "research": True, "control": False, "edge": False,
        })
        self.assertFalse(report["finance"])
        self.assertTrue(report["planner_ok"])
        self.assertTrue(report["boundaries"])

    def test_financial_sql_and_riskbook_require_engine_finance(self):
        for path in (
            "vnext/engine/crates/okx-trade-execution/schema/finance-unified-v1.sql",
            "vnext/engine/crates/okx-engine-risk/src/risk/model.rs",
            "vnext/engine/Cargo.lock",
        ):
            with self.subTest(path=path):
                report = plan_for_paths([path])
                self.assertTrue(report["owners"]["engine"])
                self.assertTrue(report["finance"])
                self.assertFalse(report["artifact_reuse_authorized"])

    def test_edge_only_source_does_not_force_windows_or_finance(self):
        report = plan_for_paths(["vnext/edge/src/runtime_session.test.ts"])
        self.assertEqual(
            [owner for owner, selected in report["owners"].items() if selected],
            ["edge"],
        )
        self.assertFalse(report["finance"])

    def test_contract_changes_include_both_sides_and_finance_if_trading(self):
        cases = (
            ("vnext/edge/contracts/trade-semantic-golden.json", {"engine", "edge"}, True),
            ("vnext/edge/contracts/research-operator-baseline.json", {"research", "edge"}, False),
            ("vnext/control/crates/okx-host-controller/src/model.rs", {"control", "edge"}, False),
            ("vnext/engine/crates/okx-trading-engine/src/lib.rs", {"engine", "research", "edge"}, True),
            ("vnext/edge/contracts/tools.schema.json", {"engine", "research", "control", "edge"}, True),
        )
        for path, expected, finance in cases:
            with self.subTest(path=path):
                report = plan_for_paths([path])
                observed = {owner for owner, enabled in report["owners"].items() if enabled}
                self.assertEqual(observed, expected)
                self.assertEqual(report["finance"], finance)
                self.assertTrue(report["contracts"])

    def test_s0_and_repository_docs_require_only_boundaries(self):
        paths = ["vnext/s0/measure.py", "vnext/README.md", "vnext/edge/README.md"]
        report = plan_for_paths(paths)
        self.assertTrue(report["planner_ok"])
        self.assertTrue(report["boundaries"])
        self.assertFalse(any(report["owners"].values()))

    def test_nested_readme_may_be_codegen_input_and_must_build_owner(self):
        report = plan_for_paths(["vnext/engine/crates/okx-trade-execution/README.md"])
        self.assertTrue(report["owners"]["engine"])
        self.assertTrue(report["finance"])

    def test_new_unclassified_contract_fails_closed(self):
        for path in (
            "vnext/edge/contracts/new-schema.json",
            "vnext/research/contracts/strategy-signal.json",
            "vnext/engine/crates/okx-trading-engine/contracts/trade-wire.json",
        ):
            with self.subTest(path=path):
                report = plan_for_paths([path])
                self.assertTrue(all(report["owners"].values()))
                self.assertFalse(report["planner_ok"])
                self.assertTrue(report["finance"])
                self.assertTrue(report["contracts"])
        private = plan_for_paths(["vnext/edge/contracts/action-fixtures.json"])
        self.assertTrue(private["planner_ok"])
        self.assertTrue(private["owners"]["edge"])
        self.assertFalse(private["finance"])

    def test_ci_planner_and_workflow_changes_trigger_every_owner(self):
        for path in ("vnext/ci/test_impact.py", ".github/workflows/vnext-ci.yml"):
            with self.subTest(path=path):
                report = plan_for_paths([path])
                self.assertTrue(all(report["owners"].values()))
                self.assertTrue(report["finance"])

    def test_unknown_paths_fail_closed_without_green_skip(self):
        for path in (
            "crates/okx-trading-engine/src/lib.rs",
            "vnext/mystery/src/lib.rs",
            "../vnext/research/src/main.rs",
            "vnext\\research\\src\\main.rs",
        ):
            with self.subTest(path=path):
                report = plan_for_paths([path])
                self.assertFalse(report["planner_ok"])
                self.assertTrue(all(report["owners"].values()))
                self.assertTrue(report["finance"])
                self.assertEqual(report["unknown_paths"], [path])

    def test_file_deletions_remain_in_pr_and_push_diff(self):
        with tempfile.TemporaryDirectory(prefix="okx-impact-") as directory:
            repo = Path(directory)
            git(repo, "init", "-q")
            git(repo, "config", "user.email", "ci@example.invalid")
            git(repo, "config", "user.name", "CI Fixture")
            file = repo / "vnext/research/src/lib.rs"
            file.parent.mkdir(parents=True)
            file.write_text("fn a() {}\n", encoding="utf-8")
            git(repo, "add", ".")
            git(repo, "commit", "-qm", "base")
            base = git(repo, "rev-parse", "HEAD")
            file.unlink()
            git(repo, "add", "-u")
            git(repo, "commit", "-qm", "delete Research")
            head = git(repo, "rev-parse", "HEAD")
            for mode in ("pr", "push"):
                with self.subTest(mode=mode):
                    paths, effective_base = changed_paths(repo, base=base, head=head, mode=mode)
                    self.assertEqual(paths, ["vnext/research/src/lib.rs"])
                    self.assertEqual(effective_base, base)

    def test_pr_uses_entire_branch_diff_not_only_latest_commit(self):
        with tempfile.TemporaryDirectory(prefix="okx-impact-") as directory:
            repo = Path(directory)
            git(repo, "init", "-q")
            git(repo, "config", "user.email", "ci@example.invalid")
            git(repo, "config", "user.name", "CI Fixture")
            (repo / "README.md").write_text("baseline\n", encoding="utf-8")
            git(repo, "add", ".")
            git(repo, "commit", "-qm", "base")
            base = git(repo, "rev-parse", "HEAD")
            for owner in ("engine", "research"):
                path = repo / f"vnext/{owner}/src/lib.rs"
                path.parent.mkdir(parents=True)
                path.write_text("// real change\n", encoding="utf-8")
                git(repo, "add", ".")
                git(repo, "commit", "-qm", f"{owner} change")
                if owner == "engine":
                    after_engine = git(repo, "rev-parse", "HEAD")
            head = git(repo, "rev-parse", "HEAD")
            pr_paths, _ = changed_paths(repo, base=base, head=head, mode="pr")
            push_paths, _ = changed_paths(repo, base=after_engine, head=head, mode="push")
            self.assertEqual(pr_paths, [
                "vnext/engine/src/lib.rs", "vnext/research/src/lib.rs",
            ])
            self.assertEqual(push_paths, ["vnext/research/src/lib.rs"])
            self.assertTrue(plan_for_paths(pr_paths)["owners"]["engine"])

    def test_rejects_missing_git_base_instead_of_skipping_everything(self):
        with tempfile.TemporaryDirectory(prefix="okx-impact-") as directory:
            with self.assertRaisesRegex(ValueError, "concrete full Git object IDs"):
                changed_paths(Path(directory), base="HEAD~1", head="HEAD", mode="pr")

    def test_outputs_are_literal_booleans_and_json_has_provenance_fields(self):
        with tempfile.TemporaryDirectory(prefix="okx-impact-") as directory:
            root = Path(directory)
            report = plan_for_paths(["vnext/research/src/main.rs"])
            output = root / "out"
            json_file = root / "impact.json"
            write_plan(report, github_output=output, json_out=json_file)
            values = dict(line.split("=", 1) for line in output.read_text().splitlines())
            self.assertEqual(values["engine"], "false")
            self.assertEqual(values["research"], "true")
            self.assertEqual(values["planner_ok"], "true")
            self.assertEqual(json.loads(json_file.read_text())["schema"], "okx-vnext-impact/v1")


if __name__ == "__main__":
    unittest.main()

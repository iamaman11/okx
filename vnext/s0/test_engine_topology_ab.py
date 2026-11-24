"""Fast structural controls for Engine S0 A/B; no Cargo build is executed."""

from pathlib import Path
import tempfile
import unittest

from engine_topology_ab import apply_identical_source_diff, cargo_binary, copy_owner, make_compact


ENGINE = Path(__file__).resolve().parents[1] / "engine"


class EngineTopologyTests(unittest.TestCase):
    def test_compact_moves_only_thin_command_boundary_and_sql_oracle(self):
        with tempfile.TemporaryDirectory(prefix="okx-s0-engine-unit-") as root:
            owner = Path(root) / "engine"
            copy_owner(ENGINE, owner)
            original_finance_lib = (owner / "crates/okx-trade-execution/src/lib.rs").read_text()
            make_compact(owner, cargo_binary())
            self.assertNotIn("okx-trading-engine", (owner / "Cargo.toml").read_text())
            self.assertFalse((owner / "crates/okx-trading-engine").exists())
            self.assertTrue((owner / "crates/okx-trade-execution/src/trading_engine.rs").is_file())
            finance_lib = (owner / "crates/okx-trade-execution/src/lib.rs").read_text()
            self.assertTrue(finance_lib.startswith(original_finance_lib))
            self.assertIn('pub mod trading_engine;', finance_lib)
            sql_test = (owner / "crates/okx-trade-execution/tests/sql_handoff.rs").read_text()
            self.assertIn("use okx_trade_execution::trading_engine::{", sql_test)
            self.assertIn("AccountStore::open_existing", sql_test)

    def test_same_production_validation_change_applies_to_both_layouts(self):
        with tempfile.TemporaryDirectory(prefix="okx-s0-engine-unit-") as root:
            owner = Path(root) / "engine"
            copy_owner(ENGINE, owner)
            apply_identical_source_diff(owner, "split")
            source = (owner / "crates/okx-trading-engine/src/lib.rs").read_text()
            self.assertIn("raw.ends_with('.')", source)
            self.assertIn("rejects_ambiguous_terminal_segment", source)

    def test_wrong_owner_rejected_without_mutation(self):
        with tempfile.TemporaryDirectory(prefix="okx-s0-engine-unit-") as root:
            with self.assertRaises(ValueError):
                copy_owner(Path(root), Path(root) / "new")


if __name__ == "__main__":
    unittest.main()

"""Fast structural controls for Engine S0 A/B; no Cargo build is executed."""

from pathlib import Path
import tempfile
import unittest

from engine_topology_ab import (
    apply_financial_source_diff,
    apply_identical_source_diff,
    copy_owner,
    finance_test_path,
    make_compact,
    stage_finance_negative_oracle,
)


ENGINE = Path(__file__).resolve().parents[1] / "engine"


class EngineTopologyTests(unittest.TestCase):
    def test_compact_moves_only_thin_command_boundary_and_sql_oracle(self):
        with tempfile.TemporaryDirectory(prefix="okx-s0-engine-unit-") as root:
            owner = Path(root) / "engine"
            copy_owner(ENGINE, owner)
            original_finance_lib = (owner / "crates/okx-trade-execution/src/lib.rs").read_text()
            make_compact(owner)
            self.assertNotIn("okx-trading-engine", (owner / "Cargo.toml").read_text())
            self.assertFalse((owner / "crates/okx-trading-engine").exists())
            self.assertTrue((owner / "crates/okx-trade-execution/src/trading_engine.rs").is_file())
            finance_lib = (owner / "crates/okx-trade-execution/src/lib.rs").read_text()
            self.assertTrue(finance_lib.startswith(original_finance_lib))
            self.assertIn('pub mod trading_engine;', finance_lib)
            self.assertNotIn('name = "okx-trading-engine"', (owner / "Cargo.lock").read_text())
            sql_test = (owner / "crates/okx-trade-execution/tests/sql_handoff.rs").read_text()
            self.assertIn("use okx_trade_execution::trading_engine::{", sql_test)
            self.assertIn("AccountStore::open_existing", sql_test)
            self.assertIn("typed_manual_target_sqlite_lost_ack_reopen_and_no_second_simulated_post", sql_test)
            self.assertIn("S0-only: this preconstructed receipt is NOT a RiskBook authorization", sql_test)

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

    def test_finance_only_probe_is_identical_and_never_mutates_canonical_engine(self):
        finance = ENGINE / "crates/okx-trade-execution/src/ledger/sql/intents.rs"
        original = finance.read_bytes()
        for topology in ("split", "compact"):
            with self.subTest(topology=topology), tempfile.TemporaryDirectory(prefix="okx-s0-finance-") as root:
                owner = Path(root) / "engine"
                copy_owner(ENGINE, owner)
                if topology == "compact":
                    make_compact(owner)
                stage_finance_negative_oracle(owner, topology)
                test = finance_test_path(owner, topology).read_text(encoding="utf-8")
                self.assertIn("S0_FINANCE_RED_EXPECTED", test)
                self.assertIn("invalid.idempotency_key.push('.')", test)
                self.assertEqual((owner / "crates/okx-trade-execution/src/ledger/sql/intents.rs").read_bytes(), original)
                apply_financial_source_diff(owner)
                edited = (owner / "crates/okx-trade-execution/src/ledger/sql/intents.rs").read_text(encoding="utf-8")
                self.assertIn('Err(StoreError::Invalid("INVALID_IDEMPOTENCY_KEY"))', edited)
                self.assertNotEqual(edited.encode(), original)
                with self.assertRaisesRegex(ValueError, "anchor has changed"):
                    apply_financial_source_diff(owner)
        self.assertEqual(finance.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()

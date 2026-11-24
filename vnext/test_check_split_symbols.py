"""Negative controls for the split-symbol inventory and eventual S0 gate."""

import unittest

from check_split_symbols import audit


SOURCE = "crates/okx-api/src/client.rs"
LEGACY = [{"path": SOURCE, "action": "split"}]


def record(**changes):
    row = {
        "source": SOURCE,
        "symbol": "OkxRestClient",
        "owner": "engine",
        "destination": "vnext/engine/adapters/okx_client.rs",
        "state_writer": "engine",
        "oracle": "T01",
        "last_legacy_consumer": "UNVERIFIED",
        "status": "planned",
    }
    row.update(changes)
    return row


class SplitSymbolsTest(unittest.TestCase):
    def test_planned_inventory_is_not_semantic_completion(self):
        result = audit(LEGACY, [record()])
        self.assertFalse(result.problems)
        self.assertEqual(result.covered, 1)
        self.assertFalse(result.complete)

    def test_missing_legacy_split_fails(self):
        result = audit(LEGACY, [])
        self.assertTrue(any("not inventoried" in issue for issue in result.problems))

    def test_same_symbol_cannot_have_two_owners(self):
        result = audit(
            LEGACY,
            [record(), record(owner="research", destination="vnext/research/adapters/okx.rs")],
        )
        self.assertTrue(any("assigned more than once" in issue for issue in result.problems))

    def test_cross_owner_source_path_is_rejected(self):
        result = audit(LEGACY, [record(destination="vnext/research/client.rs")])
        self.assertTrue(any("owner's root" in issue for issue in result.problems))

    def test_unverified_consumer_cannot_be_migrated(self):
        result = audit(LEGACY, [record(status="migrated")])
        self.assertTrue(any("cannot claim migrated" in issue for issue in result.problems))

    def test_confirmed_migration_requires_explicit_owner_and_consumer(self):
        result = audit(LEGACY, [record(status="migrated", last_legacy_consumer="retired")])
        self.assertTrue(result.complete)


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""Standard-library schema conformance, no DB server or external packages."""
import pathlib
import sqlite3
from decimal import Decimal
import tempfile

DDL = pathlib.Path(__file__).resolve().parents[1] / "crates/okx-execution/schema/account-ledger-v1.sql"

def main():
    with tempfile.TemporaryDirectory() as root:
        dbpath = pathlib.Path(root) / "account.sqlite"
        a = sqlite3.connect(dbpath, timeout=1.0, isolation_level=None)
        b = sqlite3.connect(dbpath, timeout=1.0, isolation_level=None)
        try:
            assert a.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
            a.execute("PRAGMA synchronous=FULL")
            a.execute("PRAGMA foreign_keys=ON")
            b.execute("PRAGMA foreign_keys=ON")
            a.executescript(DDL.read_text(encoding="utf-8"))
            assert a.execute("PRAGMA user_version").fetchone()[0] == 1
            a.execute("""INSERT INTO account_identity VALUES
                (1,'okx','real','account-a','uid-a',1,1)""")
            a.execute("""INSERT INTO evidence_objects
                (evidence_sha256,evidence_kind,codec,relative_path,size_bytes)
                VALUES ('sha-source','order_history','raw_bytes','a.bin',1),
                       ('sha-decision','decision','binary','b.bin',2)""")
            a.execute("""INSERT INTO source_receipts
                VALUES ('receipt-a','orders','SWAP',1,2,3,'COMPLETE','sha-source')""")
            a.execute("""INSERT INTO capital_epochs VALUES
                ('epoch-a','strategy-a','manual','okx.manual/v1','1',3,
                 'receipt-a','USDT','100','ACTIVE',NULL,NULL)""")
            a.execute("""INSERT INTO decisions
                (decision_id,epoch_id,decision_kind,decision_at_ms,instrument_id,
                 source_evidence_sha256,decision_evidence_sha256,quality)
                VALUES ('decision-a','epoch-a','TRADE',4,'BTC-USDT-SWAP',
                        'sha-source','sha-decision','FRESH')""")
            a.execute("""INSERT INTO intents
                VALUES ('intent-a','epoch-a','decision-a','BTC-USDT-SWAP',
                        'OPEN','sha-plan','PREPARED',5)""")
            a.execute("""INSERT INTO domain_events
                (event_id,event_type,event_version,subject_kind,subject_id,
                 effective_at_ms,recorded_at_ms,event_sha256)
                VALUES ('post-a','BEGIN_POST',1,'INTENT','intent-a',6,6,'hash-a')""")
            a.execute("""INSERT INTO post_attempts
                VALUES ('intent-a','client-a','post-a',6,'UNKNOWN')""")
            a.execute("""INSERT INTO fills
                (instrument_id,trade_id,intent_id,exchange_time_ms,side,
                 position_side,price_decimal,contracts_decimal,source_evidence_sha256)
                VALUES ('BTC-USDT-SWAP','trade-a','intent-a',7,'BUY','LONG',
                        '12345.6789','0.01','sha-source')""")
            a.execute("""INSERT INTO bills VALUES
                ('bill-a','BTC-USDT-SWAP','trade-a',7,'USDT','FEE',
                '-0.1','sha-source')""")
            a.execute("""INSERT INTO bill_attribution VALUES
                ('bill-a','epoch-a','-0.04','sha-source')""")
            assert b.execute("SELECT COUNT(*) FROM fills").fetchone()[0] == 1
            assert a.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            assert a.execute("PRAGMA foreign_key_check").fetchall() == []
            def must_reject(sql):
                try:
                    a.execute(sql)
                except sqlite3.IntegrityError:
                    return
                raise AssertionError("Expected constraint failure: " + sql)
            must_reject("""UPDATE domain_events SET event_type='FAKE' WHERE event_id='post-a'""")
            must_reject("""DELETE FROM domain_events WHERE event_id='post-a'""")
            must_reject("""INSERT INTO post_attempts VALUES
                ('intent-a','client-b','post-a',7,'POSTED')""")
            must_reject("""INSERT INTO bill_attribution VALUES
                ('bill-a','epoch-a','-0.04','sha-source')""")
            must_reject("""INSERT INTO intents VALUES
                ('broken','nonexistent',NULL,'BTC','OPEN','sha','PREPARED',5)""")
            a.execute("""INSERT INTO capital_epochs VALUES
                ('epoch-b','strategy-a','manual','okx.manual/v1','1',5,
                 'receipt-a','USDT','100','ACTIVE',NULL,NULL)""")
            a.execute("""INSERT INTO bill_attribution VALUES
                ('bill-a','epoch-b','-0.06','sha-source')""")
            portions = [Decimal(r[0]) for r in a.execute(
                "SELECT allocated_decimal FROM bill_attribution WHERE bill_id='bill-a'")]
            assert sum(portions) == Decimal('-0.1')
            assert a.execute(
                "SELECT COUNT(*) FROM capital_epochs WHERE strategy_id='strategy-a'").fetchone()[0] == 2
            must_reject("""UPDATE fills SET price_decimal='999' WHERE trade_id='trade-a'""")
            must_reject("""UPDATE bills SET delta_decimal='0' WHERE bill_id='bill-a'""")
            must_reject("""INSERT INTO domain_events
                (event_id,event_type,event_version,subject_kind,subject_id,
                 effective_at_ms,recorded_at_ms,event_sha256)
                VALUES ('bad-chain','OBSERVE',1,'ACCOUNT','account-a',8,9,'hash-bad')""")
            a.execute("""INSERT INTO source_gaps VALUES
                ('gap-a','orders','SWAP',7,30000000,'EXECUTION_ONLY','HISTORY_WINDOW',NULL,NULL)""")
            assert a.execute("SELECT materiality FROM source_gaps").fetchone()[0] == "EXECUTION_ONLY"
            # WAL readers remain available while another connection has a write txn.
            b.execute("BEGIN")
            b.execute("SELECT COUNT(*) FROM domain_events").fetchone()
            a.execute("BEGIN IMMEDIATE")
            a.execute("""INSERT INTO domain_events
                (event_id,event_type,event_version,subject_kind,subject_id,
                 effective_at_ms,recorded_at_ms,previous_event_sha256,event_sha256)
                VALUES ('post-b','OBSERVE',1,'ACCOUNT','account-a',8,9,'hash-a','hash-b')""")
            a.execute("COMMIT")
            b.execute("ROLLBACK")
            assert b.execute("SELECT COUNT(*) FROM domain_events").fetchone()[0] == 2
        finally:
            b.close()
            a.close()
    print("ACCOUNT_SQLITE_SCHEMA_V1=PASS")

if __name__ == "__main__":
    main()

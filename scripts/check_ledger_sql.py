#!/usr/bin/env python3
"""Exercise the canonical T0 DDL, constraints, atomicity and hot query plans."""
import pathlib
import sqlite3
import tempfile
from decimal import Decimal

DDL = pathlib.Path(__file__).resolve().parents[1] / "crates/okx-execution/schema/account-v2.sql"


def main():
    with tempfile.TemporaryDirectory() as root:
        path = pathlib.Path(root) / "account.sqlite"
        a = sqlite3.connect(path, isolation_level=None)
        b = sqlite3.connect(path, isolation_level=None)
        try:
            assert a.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
            a.execute("PRAGMA synchronous=FULL")
            for connection in (a, b):
                connection.execute("PRAGMA foreign_keys=ON")
            a.executescript(DDL.read_text(encoding="utf-8"))
            assert a.execute("PRAGMA user_version").fetchone()[0] == 2
            tables = a.execute("PRAGMA table_list").fetchall()
            assert all(row[5] == 1 for row in tables if not row[1].startswith("sqlite_"))
            a.execute("INSERT INTO account_meta VALUES(1,'okx:real:uid','installation',2,1,'flat-source','initialize-op','release')")
            a.execute("INSERT INTO strategy_instances VALUES('a','ema','1','{}',1,1,'100',NULL)")
            a.execute("INSERT INTO strategy_instances VALUES('b','ema','1','{}',1,1,'100',NULL)")
            a.execute("INSERT INTO decisions VALUES('decision','a',2,'input-digest',1,'code')")
            intent = "INSERT INTO intents(intent_id,idempotency_key,request_hash,decision_id,state,revision,client_order_id,deadline,plan_hash,policy_revision,risk_increase,instrument_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            values = ('intent', 'request', 'hash', 'decision', 'PREPARED', 0, 'client', 100, 'plan', 1, 1, 'BTC', 2, 2)
            a.execute(intent, values)
            a.execute("INSERT INTO intent_plans VALUES('intent','open','buy','1','100','95','a','5',1)")
            a.execute("INSERT INTO fills VALUES('fill','BTC','trade','order','intent',3,'1','100','-0.1','USDT','buy')")
            a.execute("INSERT INTO bills VALUES('bill','FEE',3,'-0.1','USDT','trade-event')")
            a.execute("INSERT INTO finance_entries VALUES('fee-a','trade-event','FEE','a','-0.04','USDT','a','fill',3,NULL)")
            a.execute("INSERT INTO finance_entries VALUES('fee-b','trade-event','FEE','b','-0.06','USDT','b','bill',3,NULL)")
            portions = [Decimal(row[0]) for row in a.execute("SELECT amount FROM finance_entries")]
            assert sum(portions) == Decimal('-0.1')

            def reject(sql, params=()):
                try:
                    a.execute(sql, params)
                except sqlite3.IntegrityError:
                    return
                raise AssertionError("expected constraint failure: " + sql)

            reject(intent, ('duplicate',) + values[1:])  # stable idempotency/client IDs
            reject(intent, ('bad', 'bad-key', 'hash', 'missing', 'PREPARED', 0, 'bad-client', 100, 'plan', 1, 1, 'BTC', 2, 2))
            reject("INSERT INTO finance_entries VALUES('duplicate-fee','trade-event','FEE','a','-0.04','USDT','a','bill',3,NULL)")
            reject("INSERT INTO finance_entries VALUES('bad-fk','different','FEE','a','1','USDT','missing','bill',3,NULL)")
            reject("UPDATE account_meta SET account_key='other'")
            reject("UPDATE intent_plans SET quantity='99'")
            reject("UPDATE fills SET price='99'")
            reject("DELETE FROM bills")
            reject("UPDATE finance_entries SET amount='0'")
            reject("INSERT INTO bills VALUES('bill2','FEE','not-time','-0.1','USDT','event')")
            for query, index in [
                ("SELECT intent_id FROM intents WHERE state IN('PREPARED','RESERVED','POSTING','UNKNOWN','ACKNOWLEDGED','PARTIALLY_FILLED') ORDER BY state,updated_at,intent_id LIMIT 100", "idx_intents_active"),
                ("SELECT entry_id FROM finance_entries WHERE instance_id='a' AND time>=1 AND time<=100 ORDER BY time,entry_id LIMIT 100", "idx_finance_instance"),
                ("SELECT lot_id FROM position_lots WHERE instrument_id='BTC' AND side='long' AND instance_id='a'", "idx_lots_active"),
            ]:
                plan = a.execute("EXPLAIN QUERY PLAN " + query).fetchall()
                assert any(index in row[3] for row in plan), plan
            # Failure rolls back both facts and source watermark.
            a.execute("BEGIN IMMEDIATE")
            a.execute("INSERT INTO source_cursors VALUES('fills','BTC','next',10,1,'')")
            a.execute("INSERT INTO bills VALUES('uncommitted','FEE',4,'1','USDT','event')")
            a.execute("ROLLBACK")
            assert a.execute("SELECT count(*) FROM source_cursors").fetchone()[0] == 0
            assert a.execute("SELECT count(*) FROM bills WHERE bill_id='uncommitted'").fetchone()[0] == 0
            # WAL readers keep a consistent snapshot while the writer commits.
            b.execute("BEGIN")
            assert b.execute("SELECT count(*) FROM bills").fetchone()[0] == 1
            a.execute("INSERT INTO bills VALUES('new','FEE',4,'1','USDT','event')")
            assert b.execute("SELECT count(*) FROM bills").fetchone()[0] == 1
            b.execute("ROLLBACK")
            assert b.execute("SELECT count(*) FROM bills").fetchone()[0] == 2
            assert a.execute("PRAGMA integrity_check").fetchone()[0] == 'ok'
            assert not a.execute("PRAGMA foreign_key_check").fetchall()
        finally:
            b.close()
            a.close()
    print("ACCOUNT_SQLITE_SCHEMA_V2=PASS")


if __name__ == '__main__':
    main()


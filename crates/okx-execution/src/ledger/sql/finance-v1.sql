-- #510 r4: first physical single-DB transaction slice. This is NOT the
-- full account-v2 migration; until its exact source/identity cutover, it is
-- an unactivated private schema and grants no production trading capability.
-- All keys are scoped. Do not use ATTACH or independent account/risk COMMITs.
CREATE TABLE finance_installations (
 installation_id TEXT PRIMARY KEY CHECK(length(installation_id)>0),
 owner_epoch INTEGER NOT NULL CHECK(owner_epoch>0)
) STRICT;
CREATE TABLE finance_accounts (
 installation_id TEXT NOT NULL,
 account_key TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key),
 FOREIGN KEY(installation_id) REFERENCES finance_installations(installation_id)
) STRICT;
CREATE TABLE finance_campaigns (
 installation_id TEXT NOT NULL,
 campaign_id TEXT NOT NULL,
 policy_revision INTEGER NOT NULL CHECK(policy_revision>0),
 generation INTEGER NOT NULL CHECK(generation>0),
 book_sequence INTEGER NOT NULL CHECK(book_sequence>=0),
 entry_limit_usd TEXT NOT NULL,
 entry_consumed_usd TEXT NOT NULL DEFAULT '0',
 entry_opening_consumed_usd TEXT NOT NULL DEFAULT '0',
 entry_reserved_usd TEXT NOT NULL DEFAULT '0',
 entry_breach_usd TEXT NOT NULL DEFAULT '0',
 PRIMARY KEY(installation_id,campaign_id),
 FOREIGN KEY(installation_id) REFERENCES finance_installations(installation_id)
) STRICT;
CREATE TABLE finance_campaign_accounts (
 installation_id TEXT NOT NULL,
 campaign_id TEXT NOT NULL,
 account_key TEXT NOT NULL,
 PRIMARY KEY(installation_id,campaign_id,account_key),
 FOREIGN KEY(installation_id,campaign_id)
   REFERENCES finance_campaigns(installation_id,campaign_id),
 FOREIGN KEY(installation_id,account_key)
   REFERENCES finance_accounts(installation_id,account_key)
) STRICT;
CREATE TABLE finance_intents (
 installation_id TEXT NOT NULL,
 account_key TEXT NOT NULL,
 intent_id TEXT NOT NULL,
 plan_sha256 TEXT NOT NULL CHECK(length(plan_sha256)=64),
 exchange_order_id TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,intent_id),
 UNIQUE(installation_id,account_key,exchange_order_id),
 FOREIGN KEY(installation_id,account_key)
   REFERENCES finance_accounts(installation_id,account_key)
) STRICT;
CREATE TABLE finance_fills (
 installation_id TEXT NOT NULL,
 account_key TEXT NOT NULL,
 fill_id TEXT NOT NULL,
 intent_id TEXT NOT NULL,
 exchange_order_id TEXT NOT NULL,
 trade_id TEXT NOT NULL,
 instrument_id TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,fill_id),
 UNIQUE(installation_id,account_key,fill_id,intent_id),
 UNIQUE(installation_id,account_key,instrument_id,trade_id,exchange_order_id),
 FOREIGN KEY(installation_id,account_key,intent_id)
   REFERENCES finance_intents(installation_id,account_key,intent_id)
) STRICT;
CREATE TABLE finance_bills (
 installation_id TEXT NOT NULL,
 account_key TEXT NOT NULL,
 bill_id TEXT NOT NULL,
 fill_id TEXT NOT NULL,
 source_digest TEXT NOT NULL CHECK(length(source_digest)=64),
 PRIMARY KEY(installation_id,account_key,bill_id),
 UNIQUE(installation_id,account_key,bill_id,fill_id),
 FOREIGN KEY(installation_id,account_key,fill_id)
   REFERENCES finance_fills(installation_id,account_key,fill_id)
) STRICT;
CREATE TABLE finance_entry_sources (
 installation_id TEXT NOT NULL,
 account_key TEXT NOT NULL,
 intent_id TEXT NOT NULL,
 settlement_revision INTEGER NOT NULL CHECK(settlement_revision>0),
 fill_id TEXT NOT NULL,
 bill_id TEXT NOT NULL,
 source_digest TEXT NOT NULL CHECK(length(source_digest)=64),
 cumulative_entry_usd TEXT NOT NULL,
 -- Preserves original verified AccountStore receipt bytes during legacy import.
 -- Synthetic first-slice fixtures do not possess an original receipt.
 original_receipt_json TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(installation_id,account_key,intent_id,settlement_revision),
 FOREIGN KEY(installation_id,account_key,intent_id)
   REFERENCES finance_intents(installation_id,account_key,intent_id),
 FOREIGN KEY(installation_id,account_key,fill_id,intent_id)
   REFERENCES finance_fills(installation_id,account_key,fill_id,intent_id),
 FOREIGN KEY(installation_id,account_key,bill_id,fill_id)
   REFERENCES finance_bills(installation_id,account_key,bill_id,fill_id)
) STRICT;
CREATE TABLE finance_risk_entries (
 installation_id TEXT NOT NULL,
 campaign_id TEXT NOT NULL,
 reservation_key TEXT NOT NULL,
 account_key TEXT NOT NULL,
 intent_id TEXT NOT NULL,
 plan_sha256 TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('HELD','POSTED','UNKNOWN','OPEN','RELEASED')),
 entry_upper_usd TEXT NOT NULL,
 entry_settled_usd TEXT NOT NULL DEFAULT '0',
 reserved_entry_usd TEXT NOT NULL,
 settlement_revision INTEGER NOT NULL DEFAULT 0 CHECK(settlement_revision>=0),
 source_digest TEXT NOT NULL DEFAULT '',
 CHECK(state <> 'RELEASED' OR reserved_entry_usd='0'),
 CHECK(state <> 'HELD' OR (entry_settled_usd='0' AND settlement_revision=0)),
 PRIMARY KEY(installation_id,campaign_id,reservation_key),
 FOREIGN KEY(installation_id,campaign_id,account_key)
   REFERENCES finance_campaign_accounts(installation_id,campaign_id,account_key),
 FOREIGN KEY(installation_id,account_key,intent_id)
   REFERENCES finance_intents(installation_id,account_key,intent_id),
 UNIQUE(installation_id,campaign_id,account_key,intent_id)
) STRICT;
CREATE TABLE finance_entry_receipts (
 installation_id TEXT NOT NULL,
 campaign_id TEXT NOT NULL,
 reservation_key TEXT NOT NULL,
 account_key TEXT NOT NULL,
 intent_id TEXT NOT NULL,
 settlement_revision INTEGER NOT NULL CHECK(settlement_revision>0),
 source_digest TEXT NOT NULL CHECK(length(source_digest)=64),
 cumulative_entry_usd TEXT NOT NULL,
 book_sequence INTEGER NOT NULL CHECK(book_sequence>0),
 PRIMARY KEY(installation_id,campaign_id,reservation_key,settlement_revision),
 FOREIGN KEY(installation_id,campaign_id,reservation_key)
   REFERENCES finance_risk_entries(installation_id,campaign_id,reservation_key),
 FOREIGN KEY(installation_id,account_key,intent_id,settlement_revision)
   REFERENCES finance_entry_sources(installation_id,account_key,intent_id,settlement_revision)
) STRICT;
CREATE TRIGGER finance_immutable_fill_update BEFORE UPDATE ON finance_fills
 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_FILL'); END;
CREATE TRIGGER finance_immutable_fill_delete BEFORE DELETE ON finance_fills
 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_FILL'); END;
CREATE TRIGGER finance_immutable_bill_update BEFORE UPDATE ON finance_bills
 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_BILL'); END;
CREATE TRIGGER finance_immutable_bill_delete BEFORE DELETE ON finance_bills
 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_BILL'); END;
CREATE TRIGGER finance_immutable_source_update BEFORE UPDATE ON finance_entry_sources
 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_ENTRY_SOURCE'); END;
CREATE TRIGGER finance_immutable_source_delete BEFORE DELETE ON finance_entry_sources
 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_ENTRY_SOURCE'); END;
CREATE TRIGGER finance_immutable_receipt_update BEFORE UPDATE ON finance_entry_receipts
 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_ENTRY_RECEIPT'); END;
CREATE TRIGGER finance_immutable_receipt_delete BEFORE DELETE ON finance_entry_receipts
 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_ENTRY_RECEIPT'); END;

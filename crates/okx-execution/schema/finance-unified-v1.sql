-- #510 r4.0 single-file canonical target from all 47 ORIGINAL finance tables.
-- 39 AccountStore + 8 RiskBook source tables; no parallel shadow ledger.
-- All former account/campaign-local PK/UNIQUE/FK are namespace-scoped.
-- This is a PRIVATE, NOT-ACTIVATED candidate. Do NOT upgrade live account
-- SQLite or grant finance authority before migration, triggers, writer cutover
-- and physically confirmed legacy fencing. Existing source schema remains.

CREATE TABLE account_meta(
 singleton INTEGER CHECK(singleton=1),
 account_key TEXT NOT NULL,
 installation_id TEXT NOT NULL,
 schema_version INTEGER NOT NULL CHECK(schema_version=2),
 initialized_at INTEGER NOT NULL,
 opening_source TEXT NOT NULL,
 initialize_operation_id TEXT NOT NULL,
 release_id TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,singleton),
 UNIQUE(installation_id,account_key,initialize_operation_id),
 UNIQUE(installation_id,account_key)
) STRICT;

CREATE TABLE strategy_definition_versions(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 definition_id TEXT NOT NULL,
 version TEXT NOT NULL,
 parameters TEXT NOT NULL,
 parameters_digest TEXT NOT NULL,
 created_at INTEGER NOT NULL,
 UNIQUE(installation_id,account_key,definition_id,version,parameters_digest),
 PRIMARY KEY(installation_id,account_key,definition_id,version),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE strategy_instances(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 instance_id TEXT,
 definition_id TEXT NOT NULL,
 definition_version TEXT NOT NULL,
 parameters TEXT NOT NULL,
 enabled INTEGER NOT NULL CHECK(enabled IN(0,1)),
 policy_revision INTEGER NOT NULL,
 budget TEXT NOT NULL,
 last_decision_key TEXT,
 parameters_digest TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,instance_id),
 FOREIGN KEY(installation_id,account_key,definition_id,definition_version,parameters_digest) REFERENCES strategy_definition_versions(installation_id,account_key,definition_id,version,parameters_digest),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE decisions(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 decision_id TEXT,
 instance_id TEXT,
 decision_at INTEGER NOT NULL,
 input_digest TEXT NOT NULL,
 policy_revision INTEGER NOT NULL,
 code_version TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,decision_id),
 FOREIGN KEY(installation_id,account_key,instance_id) REFERENCES strategy_instances(installation_id,account_key,instance_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE intents(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 intent_id TEXT,
 idempotency_key TEXT NOT NULL,
 request_hash TEXT NOT NULL,
 decision_id TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('PREPARED','RESERVED','POSTING','UNKNOWN','ACKNOWLEDGED','PARTIALLY_FILLED','FILLED','CANCELLED','REJECTED')),
 revision INTEGER NOT NULL,
 client_order_id TEXT NOT NULL,
 deadline INTEGER NOT NULL,
 plan_hash TEXT NOT NULL,
 policy_revision INTEGER NOT NULL,
 risk_increase INTEGER NOT NULL CHECK(risk_increase IN(0,1)),
 instrument_id TEXT NOT NULL,
 created_at INTEGER NOT NULL,
 updated_at INTEGER NOT NULL,
 begin_post_at INTEGER,
 exchange_order_id TEXT,
 incident TEXT,
 PRIMARY KEY(installation_id,account_key,intent_id),
 UNIQUE(installation_id,account_key,idempotency_key),
 UNIQUE(installation_id,account_key,client_order_id),
 UNIQUE(installation_id,account_key,intent_id,plan_hash),
 FOREIGN KEY(installation_id,account_key,decision_id) REFERENCES decisions(installation_id,account_key,decision_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE intent_plans(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 intent_id TEXT,
 operation TEXT NOT NULL,
 side TEXT NOT NULL,
 quantity TEXT NOT NULL,
 limit_price TEXT NOT NULL,
 stop_price TEXT,
 strategy_instance TEXT NOT NULL,
 planned_loss_usd TEXT NOT NULL,
 source_generation INTEGER NOT NULL,
 position_side TEXT NOT NULL CHECK(position_side IN('long','short')),
 private_source_generation INTEGER NOT NULL,
 contract_value TEXT NOT NULL,
 inverse INTEGER NOT NULL CHECK(inverse IN(0,1)),
 settlement_currency TEXT NOT NULL,
 strategy_definition_id TEXT NOT NULL,
 strategy_definition_version TEXT NOT NULL,
 strategy_parameters_digest TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,intent_id),
 FOREIGN KEY(installation_id,account_key,intent_id) REFERENCES intents(installation_id,account_key,intent_id),
 FOREIGN KEY(installation_id,account_key,strategy_definition_id,strategy_definition_version,strategy_parameters_digest) REFERENCES strategy_definition_versions(installation_id,account_key,definition_id,version,parameters_digest),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE protection_receipts(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 intent_id TEXT,
 algo_id TEXT NOT NULL,
 side TEXT NOT NULL,
 quantity TEXT NOT NULL,
 trigger_price TEXT NOT NULL,
 trigger_basis TEXT NOT NULL,
 verified_at INTEGER NOT NULL,
 source_digest TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,intent_id),
 UNIQUE(installation_id,account_key,algo_id),
 FOREIGN KEY(installation_id,account_key,intent_id) REFERENCES intents(installation_id,account_key,intent_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE orders(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 order_id TEXT,
 exchange_order_id TEXT,
 intent_id TEXT,
 client_order_id TEXT NOT NULL,
 protection_id TEXT,
 filled_qty TEXT NOT NULL,
 state TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,order_id),
 UNIQUE(installation_id,account_key,exchange_order_id),
 FOREIGN KEY(installation_id,account_key,intent_id) REFERENCES intents(installation_id,account_key,intent_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE fills(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 fill_id TEXT,
 instrument_id TEXT NOT NULL,
 trade_id TEXT NOT NULL,
 exchange_order_id TEXT NOT NULL,
 intent_id TEXT,
 time INTEGER NOT NULL,
 qty TEXT NOT NULL,
 price TEXT NOT NULL,
 fee TEXT NOT NULL,
 currency TEXT NOT NULL,
 side TEXT NOT NULL CHECK(side IN('buy','sell')),
 UNIQUE(installation_id,account_key,instrument_id,trade_id,exchange_order_id),
 PRIMARY KEY(installation_id,account_key,fill_id),
 FOREIGN KEY(installation_id,account_key,intent_id) REFERENCES intents(installation_id,account_key,intent_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE fill_projection_pending(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 fill_id TEXT,
 side TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,fill_id),
 FOREIGN KEY(installation_id,account_key,fill_id) REFERENCES fills(installation_id,account_key,fill_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE funding_projection_pending(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 event_id TEXT,
 instrument_id TEXT NOT NULL,
 time INTEGER NOT NULL,
 amount TEXT NOT NULL,
 currency TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,event_id),
 FOREIGN KEY(installation_id,account_key,event_id) REFERENCES bills(installation_id,account_key,bill_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE bills(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 bill_id TEXT,
 kind TEXT NOT NULL,
 time INTEGER NOT NULL,
 amount TEXT NOT NULL,
 currency TEXT NOT NULL,
 economic_event_key TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,bill_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE cash_flows(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 event_id TEXT,
 kind TEXT NOT NULL,
 currency TEXT NOT NULL,
 amount TEXT NOT NULL,
 time INTEGER NOT NULL,
 source_ref TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,event_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE allocations(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 event_id TEXT,
 instance_id TEXT NOT NULL,
 delta TEXT NOT NULL,
 currency TEXT NOT NULL,
 time INTEGER NOT NULL,
 PRIMARY KEY(installation_id,account_key,event_id),
 FOREIGN KEY(installation_id,account_key,instance_id) REFERENCES strategy_instances(installation_id,account_key,instance_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE position_lots(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 lot_id TEXT,
 instance_id TEXT,
 intent_id TEXT,
 fill_id TEXT,
 instrument_id TEXT NOT NULL,
 side TEXT NOT NULL,
 remaining_qty TEXT NOT NULL,
 cost TEXT NOT NULL,
 opened_at INTEGER NOT NULL,
 PRIMARY KEY(installation_id,account_key,lot_id),
 FOREIGN KEY(installation_id,account_key,instance_id) REFERENCES strategy_instances(installation_id,account_key,instance_id),
 FOREIGN KEY(installation_id,account_key,intent_id) REFERENCES intents(installation_id,account_key,intent_id),
 FOREIGN KEY(installation_id,account_key,fill_id) REFERENCES fills(installation_id,account_key,fill_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE finance_entries(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 entry_id TEXT,
 economic_event_key TEXT NOT NULL,
 component TEXT NOT NULL,
 allocation_leg TEXT NOT NULL,
 amount TEXT NOT NULL,
 currency TEXT NOT NULL,
 instance_id TEXT,
 source_ref TEXT NOT NULL,
 time INTEGER NOT NULL,
 reverses_entry_id TEXT,
 UNIQUE(installation_id,account_key,economic_event_key,component,allocation_leg),
 PRIMARY KEY(installation_id,account_key,entry_id),
 FOREIGN KEY(installation_id,account_key,instance_id) REFERENCES strategy_instances(installation_id,account_key,instance_id),
 FOREIGN KEY(installation_id,account_key,reverses_entry_id) REFERENCES finance_entries(installation_id,account_key,entry_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE finance_totals(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 currency TEXT NOT NULL,
 component TEXT NOT NULL,
 amount TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,currency,component),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE instance_finance_totals(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 instance_id TEXT NOT NULL,
 currency TEXT NOT NULL,
 component TEXT NOT NULL,
 amount TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,instance_id,currency,component),
 FOREIGN KEY(installation_id,account_key,instance_id) REFERENCES strategy_instances(installation_id,account_key,instance_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE finance_source_links(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 entry_id TEXT NOT NULL,
 source_ref TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,entry_id,source_ref),
 FOREIGN KEY(installation_id,account_key,entry_id) REFERENCES finance_entries(installation_id,account_key,entry_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE nav_points(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 scope_id TEXT NOT NULL,
 time INTEGER NOT NULL,
 source_version TEXT NOT NULL,
 equity TEXT NOT NULL,
 pnl TEXT NOT NULL,
 flows TEXT NOT NULL,
 currency TEXT NOT NULL,
 coverage TEXT NOT NULL,
 mark_source TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,scope_id,time,source_version),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE nav_watermarks(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 scope_id TEXT,
 highwater TEXT NOT NULL,
 adjusted_equity TEXT NOT NULL,
 time INTEGER NOT NULL,
 coverage TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,scope_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE nav_scopes(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 scope_id TEXT NOT NULL,
 currency TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,scope_id,currency),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE daily_losses(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 scope_id TEXT NOT NULL,
 utc_day INTEGER NOT NULL,
 loss_usd TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,scope_id,utc_day),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE risk_stops(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 stop_id TEXT,
 scope TEXT NOT NULL,
 reason TEXT NOT NULL,
 latched_at INTEGER NOT NULL,
 resolution TEXT,
 PRIMARY KEY(installation_id,account_key,stop_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE source_cursors(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 source TEXT NOT NULL,
 stream_partition TEXT NOT NULL,
 cursor TEXT NOT NULL,
 watermark INTEGER NOT NULL,
 generation INTEGER NOT NULL,
 coverage_gaps TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,source,stream_partition),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE risk_receipts(
 reservation_key TEXT,
 intent_id TEXT NOT NULL,
 account_key TEXT NOT NULL,
 installation_id TEXT NOT NULL,
 instance_id TEXT,
 plan_hash TEXT NOT NULL,
 policy_revision INTEGER NOT NULL,
 receipt_hash TEXT NOT NULL,
 requested_loss_usd TEXT NOT NULL,
 conversion_digest TEXT NOT NULL,
 state TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,reservation_key),
 UNIQUE(installation_id,account_key,intent_id),
 FOREIGN KEY(installation_id,account_key,intent_id) REFERENCES intents(installation_id,account_key,intent_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE operation_receipts(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 operation_id TEXT,
 kind TEXT NOT NULL,
 request_hash TEXT NOT NULL,
 result_ref TEXT NOT NULL,
 committed_at INTEGER NOT NULL,
 PRIMARY KEY(installation_id,account_key,operation_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE account_policy(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 singleton INTEGER CHECK(singleton=1),
 revision INTEGER NOT NULL,
 paused INTEGER NOT NULL CHECK(paused IN(0,1)),
 campaign_id TEXT NOT NULL,
 campaign_limit TEXT NOT NULL,
 campaign_loss TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,singleton),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE storage_recovery(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 singleton INTEGER CHECK(singleton=1),
 requires_reconcile INTEGER NOT NULL CHECK(requires_reconcile IN(0,1)),
 backup_at INTEGER,
 reconciled_evidence TEXT,
 PRIMARY KEY(installation_id,account_key,singleton),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE campaign_source_epoch(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 id INTEGER CHECK(id=1),
 revision INTEGER NOT NULL CHECK(revision>=0),
 PRIMARY KEY(installation_id,account_key,id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE campaign_entry_readiness(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 campaign_id TEXT,
 from_ms INTEGER NOT NULL,
 covered_through_ms INTEGER NOT NULL,
 source_revision INTEGER NOT NULL,
 source_generations TEXT NOT NULL,
 reviewed_fill_rowid INTEGER NOT NULL DEFAULT 0,
 reviewed_bill_rowid INTEGER NOT NULL DEFAULT 0,
 reviewed_receipt_rowid INTEGER NOT NULL DEFAULT 0,
 reviewed_finance_rowid INTEGER NOT NULL DEFAULT 0,
 reviewed_flow_rowid INTEGER NOT NULL DEFAULT 0,
 reviewed_late_epoch INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(installation_id,account_key,campaign_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE campaign_late_head_epoch(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 id INTEGER CHECK(id=1),
 revision INTEGER NOT NULL CHECK(revision>=0),
 PRIMARY KEY(installation_id,account_key,id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE campaign_receipt_events(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 event_seq INTEGER NOT NULL CHECK(event_seq>0),
 operation_id TEXT NOT NULL,
 kind TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,event_seq),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE campaign_receipt_cursors(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 campaign_id TEXT,
 reviewed_event_seq INTEGER NOT NULL CHECK(reviewed_event_seq>=0),
 PRIMARY KEY(installation_id,account_key,campaign_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE campaign_entry_applied(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 campaign_id TEXT NOT NULL,
 intent_id TEXT NOT NULL,
 settlement_revision INTEGER NOT NULL CHECK(settlement_revision>0),
 cumulative_usd TEXT NOT NULL,
 source_digest TEXT NOT NULL,
 venue_update_ms INTEGER NOT NULL,
 PRIMARY KEY(installation_id,account_key,campaign_id,intent_id,settlement_revision),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE campaign_entry_totals(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 campaign_id TEXT,
 from_ms INTEGER NOT NULL,
 covered_through_ms INTEGER NOT NULL,
 consumed_usd TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,campaign_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE campaign_entry_window_proofs(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 campaign_id TEXT NOT NULL,
 covered_through_ms INTEGER NOT NULL,
 consumed_usd TEXT NOT NULL,
 source_digest TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,campaign_id,covered_through_ms),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE bill_order_sources(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 bill_id TEXT,
 instrument_id TEXT NOT NULL,
 exchange_order_id TEXT NOT NULL,
 trade_id TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_key,bill_id),
 FOREIGN KEY(installation_id,account_key,bill_id) REFERENCES bills(installation_id,account_key,bill_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE swap_fill_sources(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 account_key TEXT NOT NULL CHECK(length(account_key)>0),
 fill_id TEXT,
 bill_id TEXT NOT NULL,
 sub_type TEXT NOT NULL,
 position_side TEXT NOT NULL,
 direction TEXT NOT NULL CHECK(direction IN('OPEN','CLOSE','NET_UNRESOLVED','UNCLASSIFIED')),
 PRIMARY KEY(installation_id,account_key,fill_id),
 FOREIGN KEY(installation_id,account_key,fill_id) REFERENCES fills(installation_id,account_key,fill_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE risk_meta(
 singleton INTEGER CHECK(singleton=1),
 campaign_id TEXT NOT NULL,
 installation_id TEXT NOT NULL,
 policy_revision INTEGER NOT NULL CHECK(policy_revision>0),
 generation INTEGER NOT NULL CHECK(generation>0),
 book_sequence INTEGER NOT NULL CHECK(book_sequence>=0),
 legacy_fenced INTEGER NOT NULL CHECK(legacy_fenced=1),
 global_limit_usdt TEXT NOT NULL,
 global_reserved_usdt TEXT NOT NULL,
 loss_limit_usd TEXT NOT NULL,
 loss_consumed_usd TEXT NOT NULL,
 loss_reserved_usd TEXT NOT NULL,
 entry_limit_usd TEXT NOT NULL,
 entry_consumed_usd TEXT NOT NULL,
 entry_opening_consumed_usd TEXT NOT NULL,
 entry_reserved_usd TEXT NOT NULL,
 entry_bound_breach_usd TEXT NOT NULL,
 PRIMARY KEY(installation_id,campaign_id,singleton),
 UNIQUE(installation_id,campaign_id)
) STRICT;

CREATE TABLE risk_policy_account(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 campaign_id TEXT NOT NULL CHECK(length(campaign_id)>0),
 account_key TEXT,
 limit_usdt TEXT NOT NULL,
 order_limit_usdt TEXT NOT NULL,
 entry_limit_usd TEXT NOT NULL,
 PRIMARY KEY(installation_id,campaign_id,account_key),
 FOREIGN KEY(installation_id,campaign_id,account_key) REFERENCES risk_account_occupied(installation_id,campaign_id,account_key) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(installation_id,campaign_id) REFERENCES risk_meta(installation_id,campaign_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE risk_account_occupied(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 campaign_id TEXT NOT NULL CHECK(length(campaign_id)>0),
 account_key TEXT,
 reserved_usdt TEXT NOT NULL,
 PRIMARY KEY(installation_id,campaign_id,account_key),
 FOREIGN KEY(installation_id,campaign_id) REFERENCES risk_meta(installation_id,campaign_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE risk_loss_source(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 campaign_id TEXT NOT NULL CHECK(length(campaign_id)>0),
 account_key TEXT,
 cumulative_loss_usd TEXT NOT NULL,
 source_digest TEXT NOT NULL,
 PRIMARY KEY(installation_id,campaign_id,account_key),
 FOREIGN KEY(installation_id,campaign_id,account_key) REFERENCES risk_policy_account(installation_id,campaign_id,account_key),
 FOREIGN KEY(installation_id,campaign_id) REFERENCES risk_meta(installation_id,campaign_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE risk_policy_strategy(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 campaign_id TEXT NOT NULL CHECK(length(campaign_id)>0),
 account_key TEXT NOT NULL,
 strategy_id TEXT NOT NULL,
 limit_usdt TEXT NOT NULL,
 PRIMARY KEY(installation_id,campaign_id,account_key,strategy_id),
 FOREIGN KEY(installation_id,campaign_id,account_key) REFERENCES risk_policy_account(installation_id,campaign_id,account_key),
 FOREIGN KEY(installation_id,campaign_id) REFERENCES risk_meta(installation_id,campaign_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE risk_strategy_occupied(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 campaign_id TEXT NOT NULL CHECK(length(campaign_id)>0),
 account_key TEXT NOT NULL,
 strategy_id TEXT NOT NULL,
 reserved_usdt TEXT NOT NULL,
 PRIMARY KEY(installation_id,campaign_id,account_key,strategy_id),
 FOREIGN KEY(installation_id,campaign_id,account_key,strategy_id) REFERENCES risk_policy_strategy(installation_id,campaign_id,account_key,strategy_id),
 FOREIGN KEY(installation_id,campaign_id) REFERENCES risk_meta(installation_id,campaign_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key)
) STRICT;

CREATE TABLE risk_entries(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 campaign_id TEXT NOT NULL CHECK(length(campaign_id)>0),
 reservation_key TEXT,
 account_key TEXT NOT NULL,
 strategy_id TEXT NOT NULL,
 intent_id TEXT NOT NULL,
 plan_sha256 TEXT NOT NULL,
 request_digest TEXT NOT NULL,
 policy_revision INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('HELD','POSTED','UNKNOWN','OPEN','RELEASED')),
 loss_usdt TEXT NOT NULL,
 loss_usd TEXT NOT NULL,
 entry_upper_usd TEXT NOT NULL,
 reserved_usdt TEXT NOT NULL,
 reserved_loss_usd TEXT NOT NULL,
 reserved_entry_usd TEXT NOT NULL,
 entry_settled_usd TEXT NOT NULL DEFAULT '0',
 entry_settlement_revision INTEGER NOT NULL DEFAULT 0 CHECK(entry_settlement_revision>=0),
 entry_settlement_digest TEXT NOT NULL DEFAULT '',
 entry_bound_breach_usd TEXT NOT NULL DEFAULT '0',
 entry_proof_digest TEXT NOT NULL,
 source_digest TEXT NOT NULL,
 book_sequence INTEGER NOT NULL CHECK(book_sequence>0),
 held_at_ms INTEGER NOT NULL CHECK(held_at_ms>0),
 reserve_read_floor_ms INTEGER NOT NULL CHECK(reserve_read_floor_ms>0),
 native_incarnation TEXT NOT NULL CHECK(length(native_incarnation)=32),
 post_authorized_at_ms INTEGER NOT NULL DEFAULT 0,
 post_sequence INTEGER NOT NULL DEFAULT 0,
 post_proof_digest TEXT NOT NULL DEFAULT '',
 post_source_digest TEXT NOT NULL DEFAULT '',
 post_existing_occupied_usdt TEXT NOT NULL DEFAULT '0',
 release_proof_digest TEXT NOT NULL DEFAULT '',
 release_request_hash TEXT NOT NULL DEFAULT '',
 release_reason TEXT NOT NULL DEFAULT '',
 CHECK(state <> 'HELD' OR
       (entry_settled_usd='0' AND entry_settlement_revision=0 AND entry_settlement_digest='')),
 CHECK(entry_settlement_revision=0 OR length(entry_settlement_digest)=64),
 CHECK(state <> 'HELD' OR
       (post_authorized_at_ms=0 AND post_sequence=0
        AND post_proof_digest='' AND post_source_digest=''
        AND post_existing_occupied_usdt='0')),
 CHECK(state = 'HELD' OR state = 'RELEASED' OR
       (post_authorized_at_ms>=held_at_ms AND post_sequence>book_sequence
        AND length(post_proof_digest)=64 AND length(post_source_digest)=64)),
 CHECK(state <> 'RELEASED' OR
       (reserved_usdt='0' AND reserved_loss_usd='0' AND reserved_entry_usd='0'
        AND length(release_proof_digest)=64 AND length(release_request_hash)=64
        AND length(release_reason) BETWEEN 1 AND 128)),
 PRIMARY KEY(installation_id,campaign_id,reservation_key),
 UNIQUE(installation_id,account_key,intent_id),
 FOREIGN KEY(installation_id,campaign_id,account_key) REFERENCES risk_policy_account(installation_id,campaign_id,account_key),
 FOREIGN KEY(installation_id,campaign_id) REFERENCES risk_meta(installation_id,campaign_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key),
 FOREIGN KEY(installation_id,account_key,intent_id,plan_sha256) REFERENCES intents(installation_id,account_key,intent_id,plan_hash)
) STRICT;

CREATE TABLE risk_no_reserve(
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 campaign_id TEXT NOT NULL CHECK(length(campaign_id)>0),
 reservation_key TEXT,
 account_key TEXT NOT NULL,
 intent_id TEXT NOT NULL,
 plan_sha256 TEXT NOT NULL,
 request_hash TEXT NOT NULL,
 record_digest TEXT NOT NULL,
 reason TEXT NOT NULL CHECK(length(reason) BETWEEN 1 AND 128),
 policy_revision INTEGER NOT NULL CHECK(policy_revision>0),
 book_sequence INTEGER NOT NULL CHECK(book_sequence>0),
 PRIMARY KEY(installation_id,campaign_id,reservation_key),
 FOREIGN KEY(installation_id,campaign_id,account_key) REFERENCES risk_policy_account(installation_id,campaign_id,account_key),
 FOREIGN KEY(installation_id,campaign_id) REFERENCES risk_meta(installation_id,campaign_id),
 FOREIGN KEY(installation_id,account_key) REFERENCES account_meta(installation_id,account_key),
 FOREIGN KEY(installation_id,account_key,intent_id,plan_sha256) REFERENCES intents(installation_id,account_key,intent_id,plan_hash)
) STRICT;

-- Original query acceleration, prefixed with the exact identity scope.
CREATE INDEX idx_intents_active ON intents(installation_id,account_key,state,updated_at,intent_id);
CREATE INDEX idx_orders_intent ON orders(installation_id,account_key,intent_id);
CREATE INDEX idx_orders_active ON orders(installation_id,account_key,state,order_id);
CREATE INDEX idx_fills_intent ON fills(installation_id,account_key,intent_id,fill_id);
CREATE INDEX idx_fills_time ON fills(installation_id,account_key,time,fill_id);
CREATE INDEX idx_fills_instrument_time ON fills(installation_id,account_key,instrument_id,time,fill_id);
CREATE INDEX idx_bills_time ON bills(installation_id,account_key,time,bill_id);
CREATE INDEX idx_allocations_instance ON allocations(installation_id,account_key,instance_id,time,event_id);
CREATE INDEX idx_lots_active ON position_lots(installation_id,account_key,instrument_id,side,instance_id,opened_at,lot_id) WHERE remaining_qty<>'0';
CREATE INDEX idx_finance_time ON finance_entries(installation_id,account_key,time,entry_id);
CREATE INDEX idx_finance_instance ON finance_entries(installation_id,account_key,instance_id,time,entry_id);
CREATE INDEX idx_finance_source_component ON finance_entries(installation_id,account_key,source_ref,component);
CREATE INDEX idx_finance_links_source ON finance_source_links(installation_id,account_key,source_ref,entry_id);
CREATE INDEX idx_nav_scope_version_time ON nav_points(installation_id,account_key,scope_id,source_version,time);
CREATE INDEX idx_stops_unresolved ON risk_stops(installation_id,account_key,scope,resolution);
CREATE INDEX idx_risk_receipts_active ON risk_receipts(installation_id,account_key,state,intent_id);
CREATE INDEX idx_lots_instance_active ON position_lots(installation_id,account_key,instance_id,instrument_id,side,opened_at,lot_id) WHERE remaining_qty<>'0';
CREATE INDEX idx_campaign_native_receipt_scan ON operation_receipts(installation_id,account_key,kind,committed_at,operation_id);
CREATE INDEX idx_bill_order_sources_order ON bill_order_sources(installation_id,account_key,exchange_order_id,bill_id);
CREATE INDEX idx_risk_entries_active_account ON risk_entries(installation_id,campaign_id,account_key,state);
CREATE INDEX idx_risk_entries_active_balance ON risk_entries(installation_id,campaign_id,state)
 WHERE state <> 'RELEASED';

-- One offline migration receipt. This is NOT a financial ledger or proof
-- that Cloudflare revoked the former authority; no runtime grants from it.
-- The 47 original financial tables remain unchanged in number.
CREATE TABLE finance_import_manifest(
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 installation_id TEXT NOT NULL CHECK(length(installation_id)>0),
 inventory_sha256 TEXT NOT NULL CHECK(length(inventory_sha256)=64),
 content_sha256 TEXT NOT NULL CHECK(length(content_sha256)=64),
 account_count INTEGER NOT NULL CHECK(account_count>0),
 campaign_count INTEGER NOT NULL CHECK(campaign_count>0),
 account_rows INTEGER NOT NULL CHECK(account_rows>=0),
 campaign_rows INTEGER NOT NULL CHECK(campaign_rows>=0),
 state TEXT NOT NULL CHECK(state='OFFLINE_NOT_AUTHORIZED')
) STRICT;

-- Original immutable-event rules and scoped source-invalidation triggers.
CREATE TRIGGER immutable_finance_import_manifest_update BEFORE UPDATE ON finance_import_manifest
 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_FINANCE_IMPORT'); END;
CREATE TRIGGER immutable_finance_import_manifest_delete BEFORE DELETE ON finance_import_manifest
 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_FINANCE_IMPORT'); END;

CREATE TRIGGER immutable_identity BEFORE UPDATE ON account_meta BEGIN SELECT RAISE(ABORT,'IMMUTABLE_ACCOUNT_IDENTITY'); END;
CREATE TRIGGER immutable_identity_delete BEFORE DELETE ON account_meta BEGIN SELECT RAISE(ABORT,'IMMUTABLE_ACCOUNT_IDENTITY'); END;
CREATE TRIGGER immutable_strategy_definition_update BEFORE UPDATE ON strategy_definition_versions BEGIN SELECT RAISE(ABORT,'IMMUTABLE_STRATEGY_DEFINITION'); END;
CREATE TRIGGER immutable_strategy_definition_delete BEFORE DELETE ON strategy_definition_versions BEGIN SELECT RAISE(ABORT,'IMMUTABLE_STRATEGY_DEFINITION'); END;
CREATE TRIGGER immutable_entry_quote_update BEFORE UPDATE ON operation_receipts WHEN OLD.kind='ENTRY_QUOTE_UNPROVEN' BEGIN SELECT RAISE(ABORT,'IMMUTABLE_ENTRY_QUOTE'); END;
CREATE TRIGGER immutable_entry_quote_delete BEFORE DELETE ON operation_receipts WHEN OLD.kind='ENTRY_QUOTE_UNPROVEN' BEGIN SELECT RAISE(ABORT,'IMMUTABLE_ENTRY_QUOTE'); END;
CREATE TRIGGER immutable_fill_update BEFORE UPDATE ON fills BEGIN SELECT RAISE(ABORT,'IMMUTABLE_FILL'); END;
CREATE TRIGGER immutable_fill_delete BEFORE DELETE ON fills BEGIN SELECT RAISE(ABORT,'IMMUTABLE_FILL'); END;
CREATE TRIGGER immutable_bill_update BEFORE UPDATE ON bills BEGIN SELECT RAISE(ABORT,'IMMUTABLE_BILL'); END;
CREATE TRIGGER immutable_bill_delete BEFORE DELETE ON bills BEGIN SELECT RAISE(ABORT,'IMMUTABLE_BILL'); END;
CREATE TRIGGER immutable_finance_update BEFORE UPDATE ON finance_entries BEGIN SELECT RAISE(ABORT,'IMMUTABLE_FINANCE_ENTRY'); END;
CREATE TRIGGER immutable_finance_delete BEFORE DELETE ON finance_entries BEGIN SELECT RAISE(ABORT,'IMMUTABLE_FINANCE_ENTRY'); END;
CREATE TRIGGER immutable_plan_update BEFORE UPDATE ON intent_plans BEGIN SELECT RAISE(ABORT,'IMMUTABLE_INTENT_PLAN'); END;
CREATE TRIGGER immutable_plan_delete BEFORE DELETE ON intent_plans BEGIN SELECT RAISE(ABORT,'IMMUTABLE_INTENT_PLAN'); END;
CREATE TRIGGER immutable_decision_update BEFORE UPDATE ON decisions BEGIN SELECT RAISE(ABORT,'IMMUTABLE_DECISION'); END;
CREATE TRIGGER immutable_decision_delete BEFORE DELETE ON decisions BEGIN SELECT RAISE(ABORT,'IMMUTABLE_DECISION'); END;
CREATE TRIGGER immutable_cashflow_update BEFORE UPDATE ON cash_flows BEGIN SELECT RAISE(ABORT,'IMMUTABLE_CASH_FLOW'); END;
CREATE TRIGGER immutable_cashflow_delete BEFORE DELETE ON cash_flows BEGIN SELECT RAISE(ABORT,'IMMUTABLE_CASH_FLOW'); END;
CREATE TRIGGER immutable_allocation_update BEFORE UPDATE ON allocations BEGIN SELECT RAISE(ABORT,'IMMUTABLE_ALLOCATION'); END;
CREATE TRIGGER immutable_allocation_delete BEFORE DELETE ON allocations BEGIN SELECT RAISE(ABORT,'IMMUTABLE_ALLOCATION'); END;
CREATE TRIGGER strategy_parameters_insert BEFORE INSERT ON strategy_instances WHEN NEW.parameters <> (SELECT parameters FROM strategy_definition_versions WHERE installation_id=NEW.installation_id AND account_key=NEW.account_key AND definition_id=NEW.definition_id AND version=NEW.definition_version) BEGIN SELECT RAISE(ABORT,'STRATEGY_PARAMETERS_VERSION_MISMATCH'); END;
CREATE TRIGGER strategy_parameters_update BEFORE UPDATE ON strategy_instances WHEN NEW.parameters <> (SELECT parameters FROM strategy_definition_versions WHERE installation_id=NEW.installation_id AND account_key=NEW.account_key AND definition_id=NEW.definition_id AND version=NEW.definition_version) BEGIN SELECT RAISE(ABORT,'STRATEGY_PARAMETERS_VERSION_MISMATCH'); END;
CREATE TRIGGER campaign_fill_invalidates_ready AFTER INSERT ON fills
BEGIN UPDATE campaign_source_epoch SET revision=revision+1 WHERE id=1 AND installation_id=NEW.installation_id AND account_key=NEW.account_key; END;
CREATE TRIGGER campaign_bill_invalidates_ready AFTER INSERT ON bills
BEGIN UPDATE campaign_source_epoch SET revision=revision+1 WHERE id=1 AND installation_id=NEW.installation_id AND account_key=NEW.account_key; END;
CREATE TRIGGER campaign_finance_invalidates_ready AFTER INSERT ON finance_entries
BEGIN UPDATE campaign_source_epoch SET revision=revision+1 WHERE id=1 AND installation_id=NEW.installation_id AND account_key=NEW.account_key; END;
CREATE TRIGGER campaign_flow_invalidates_ready AFTER INSERT ON cash_flows
BEGIN UPDATE campaign_source_epoch SET revision=revision+1 WHERE id=1 AND installation_id=NEW.installation_id AND account_key=NEW.account_key; END;
CREATE TRIGGER campaign_settlement_invalidates_ready AFTER INSERT ON operation_receipts
WHEN NEW.kind IN(
    'ENTRY_SETTLEMENT_PROVEN','ENTRY_LATE_HEAD',
    'ENTRY_LATE_SETTLEMENT_PROVEN','ENTRY_LATE_SETTLED',
    'ENTRY_PARTIAL_SETTLED',
    'ENTRY_BILL_ORDER_SOURCE','ENTRY_ORDER_FEE_INDEX')
BEGIN UPDATE campaign_source_epoch SET revision=revision+1 WHERE id=1 AND installation_id=NEW.installation_id AND account_key=NEW.account_key; END;
CREATE TRIGGER campaign_late_update_invalidates_ready AFTER UPDATE OF result_ref ON operation_receipts
WHEN NEW.kind='ENTRY_LATE_HEAD' AND NEW.result_ref!=OLD.result_ref
BEGIN
UPDATE campaign_source_epoch SET revision=revision+1 WHERE id=1 AND installation_id=NEW.installation_id AND account_key=NEW.account_key;
UPDATE campaign_late_head_epoch SET revision=revision+1 WHERE id=1 AND installation_id=NEW.installation_id AND account_key=NEW.account_key;
END;
CREATE TRIGGER campaign_receipt_event_insert AFTER INSERT ON operation_receipts
WHEN NEW.kind IN ('ENTRY_SETTLEMENT_PROVEN','ENTRY_LATE_HEAD',
                  'ENTRY_LATE_SETTLEMENT_PROVEN','ENTRY_LATE_SETTLED',
                  'ENTRY_PARTIAL_SETTLED')
BEGIN
    INSERT INTO campaign_receipt_events(installation_id,account_key,event_seq,operation_id,kind)
    VALUES(NEW.installation_id,NEW.account_key,
        (SELECT COALESCE(MAX(event_seq),0)+1 FROM campaign_receipt_events
         WHERE installation_id=NEW.installation_id AND account_key=NEW.account_key),
        NEW.operation_id,NEW.kind);
END;
CREATE TRIGGER campaign_receipt_event_head_change AFTER UPDATE OF result_ref ON operation_receipts
WHEN NEW.kind='ENTRY_LATE_HEAD' AND OLD.result_ref!=NEW.result_ref
BEGIN
    INSERT INTO campaign_receipt_events(installation_id,account_key,event_seq,operation_id,kind)
    VALUES(NEW.installation_id,NEW.account_key,
        (SELECT COALESCE(MAX(event_seq),0)+1 FROM campaign_receipt_events
         WHERE installation_id=NEW.installation_id AND account_key=NEW.account_key),
        NEW.operation_id,NEW.kind);
END;
CREATE TRIGGER immutable_campaign_entry_applied_update BEFORE UPDATE ON campaign_entry_applied
    BEGIN SELECT RAISE(ABORT,'IMMUTABLE_CAMPAIGN_ENTRY_APPLIED'); END;
CREATE TRIGGER immutable_campaign_entry_applied_delete BEFORE DELETE ON campaign_entry_applied
    BEGIN SELECT RAISE(ABORT,'IMMUTABLE_CAMPAIGN_ENTRY_APPLIED'); END;
CREATE TRIGGER immutable_campaign_entry_window_proof_update BEFORE UPDATE ON campaign_entry_window_proofs
    BEGIN SELECT RAISE(ABORT,'IMMUTABLE_CAMPAIGN_WINDOW_PROOF'); END;
CREATE TRIGGER immutable_campaign_entry_window_proof_delete BEFORE DELETE ON campaign_entry_window_proofs
    BEGIN SELECT RAISE(ABORT,'IMMUTABLE_CAMPAIGN_WINDOW_PROOF'); END;
CREATE TRIGGER immutable_campaign_opening_update BEFORE UPDATE ON operation_receipts
    WHEN OLD.kind='CAMPAIGN_ENTRY_OPENING_SEALED'
    BEGIN SELECT RAISE(ABORT,'IMMUTABLE_CAMPAIGN_ENTRY_OPENING'); END;
CREATE TRIGGER immutable_campaign_opening_delete BEFORE DELETE ON operation_receipts
    WHEN OLD.kind='CAMPAIGN_ENTRY_OPENING_SEALED'
    BEGIN SELECT RAISE(ABORT,'IMMUTABLE_CAMPAIGN_ENTRY_OPENING'); END;
CREATE TRIGGER immutable_bill_order_source_update BEFORE UPDATE ON bill_order_sources
    BEGIN SELECT RAISE(ABORT,'IMMUTABLE_BILL_ORDER_SOURCE'); END;
CREATE TRIGGER immutable_bill_order_source_delete BEFORE DELETE ON bill_order_sources
    BEGIN SELECT RAISE(ABORT,'IMMUTABLE_BILL_ORDER_SOURCE'); END;
CREATE TRIGGER immutable_swap_fill_source_update BEFORE UPDATE ON swap_fill_sources
    BEGIN SELECT RAISE(ABORT,'IMMUTABLE_SWAP_FILL_SOURCE'); END;
CREATE TRIGGER immutable_swap_fill_source_delete BEFORE DELETE ON swap_fill_sources
    BEGIN SELECT RAISE(ABORT,'IMMUTABLE_SWAP_FILL_SOURCE'); END;
CREATE TRIGGER immutable_entry_admission_update BEFORE UPDATE ON operation_receipts
    WHEN OLD.kind='ENTRY_ADMISSION_PROVEN' BEGIN SELECT RAISE(ABORT,'IMMUTABLE_ENTRY_ADMISSION'); END;
CREATE TRIGGER immutable_entry_admission_delete BEFORE DELETE ON operation_receipts
    WHEN OLD.kind='ENTRY_ADMISSION_PROVEN' BEGIN SELECT RAISE(ABORT,'IMMUTABLE_ENTRY_ADMISSION'); END;
CREATE TRIGGER immutable_entry_partial_update BEFORE UPDATE ON operation_receipts
    WHEN OLD.kind='ENTRY_PARTIAL_SETTLEMENT_PROVEN' BEGIN SELECT RAISE(ABORT,'IMMUTABLE_PARTIAL_ENTRY'); END;
CREATE TRIGGER immutable_entry_partial_delete BEFORE DELETE ON operation_receipts
    WHEN OLD.kind='ENTRY_PARTIAL_SETTLEMENT_PROVEN' BEGIN SELECT RAISE(ABORT,'IMMUTABLE_PARTIAL_ENTRY'); END;
CREATE TRIGGER immutable_entry_partial_ack_update BEFORE UPDATE ON operation_receipts
    WHEN OLD.kind='ENTRY_PARTIAL_SETTLED' BEGIN SELECT RAISE(ABORT,'IMMUTABLE_PARTIAL_ACK'); END;
CREATE TRIGGER immutable_entry_partial_ack_delete BEFORE DELETE ON operation_receipts
    WHEN OLD.kind='ENTRY_PARTIAL_SETTLED' BEGIN SELECT RAISE(ABORT,'IMMUTABLE_PARTIAL_ACK'); END;
CREATE TRIGGER immutable_entry_settlement_update BEFORE UPDATE ON operation_receipts
    WHEN OLD.kind='ENTRY_SETTLEMENT_PROVEN' BEGIN SELECT RAISE(ABORT,'IMMUTABLE_ENTRY_SETTLEMENT'); END;
CREATE TRIGGER immutable_entry_settlement_delete BEFORE DELETE ON operation_receipts
    WHEN OLD.kind='ENTRY_SETTLEMENT_PROVEN' BEGIN SELECT RAISE(ABORT,'IMMUTABLE_ENTRY_SETTLEMENT'); END;

-- One new file version covers original account v2 and campaign fixture v5.
PRAGMA user_version=6;

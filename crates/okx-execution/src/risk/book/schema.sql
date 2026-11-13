-- Testable native campaign store. The authenticated bootstrap / legacy transfer path
-- is deliberately NOT exposed until its server-side fencing is implemented.
CREATE TABLE risk_meta (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
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
 entry_reserved_usd TEXT NOT NULL,
 entry_bound_breach_usd TEXT NOT NULL
) STRICT;
CREATE TABLE risk_policy_account (
 account_key TEXT PRIMARY KEY,
 limit_usdt TEXT NOT NULL,
 order_limit_usdt TEXT NOT NULL,
 FOREIGN KEY(account_key) REFERENCES risk_account_occupied(account_key) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE TABLE risk_account_occupied (
 account_key TEXT PRIMARY KEY,
 reserved_usdt TEXT NOT NULL
) STRICT;
CREATE TABLE risk_loss_source (
 account_key TEXT PRIMARY KEY,
 cumulative_loss_usd TEXT NOT NULL,
 source_digest TEXT NOT NULL,
 FOREIGN KEY(account_key) REFERENCES risk_policy_account(account_key)
) STRICT;
CREATE TABLE risk_policy_strategy (
 account_key TEXT NOT NULL,
 strategy_id TEXT NOT NULL,
 limit_usdt TEXT NOT NULL,
 PRIMARY KEY(account_key,strategy_id),
 FOREIGN KEY(account_key) REFERENCES risk_policy_account(account_key)
) STRICT;
CREATE TABLE risk_strategy_occupied (
 account_key TEXT NOT NULL,
 strategy_id TEXT NOT NULL,
 reserved_usdt TEXT NOT NULL,
 PRIMARY KEY(account_key,strategy_id),
 FOREIGN KEY(account_key,strategy_id) REFERENCES risk_policy_strategy(account_key,strategy_id)
) STRICT;
CREATE TABLE risk_entries (
 reservation_key TEXT PRIMARY KEY,
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
 -- Primary per-reservation remaining obligations. Aggregates are only cached projections.
 reserved_usdt TEXT NOT NULL,
 reserved_loss_usd TEXT NOT NULL,
 reserved_entry_usd TEXT NOT NULL,
 entry_proof_digest TEXT NOT NULL,
 source_digest TEXT NOT NULL,
 book_sequence INTEGER NOT NULL CHECK(book_sequence>0),
 release_proof_digest TEXT NOT NULL DEFAULT '',
 release_request_hash TEXT NOT NULL DEFAULT '',
 release_reason TEXT NOT NULL DEFAULT '',
 CHECK(state <> 'RELEASED' OR
       (reserved_usdt='0' AND reserved_loss_usd='0' AND reserved_entry_usd='0'
        AND length(release_proof_digest)=64 AND length(release_request_hash)=64
        AND length(release_reason) BETWEEN 1 AND 128)),
 FOREIGN KEY(account_key) REFERENCES risk_policy_account(account_key)
) STRICT;
-- An authenticated account SQL no-send rejection may precede RESERVE.
-- Absence of an entry is never proof of a terminal/no-send outcome.
CREATE TABLE risk_no_reserve (
 reservation_key TEXT PRIMARY KEY,
 account_key TEXT NOT NULL,
 intent_id TEXT NOT NULL,
 plan_sha256 TEXT NOT NULL,
 request_hash TEXT NOT NULL,
 record_digest TEXT NOT NULL,
 reason TEXT NOT NULL CHECK(length(reason) BETWEEN 1 AND 128),
 policy_revision INTEGER NOT NULL CHECK(policy_revision>0),
 book_sequence INTEGER NOT NULL CHECK(book_sequence>0),
 FOREIGN KEY(account_key) REFERENCES risk_policy_account(account_key)
) STRICT;
CREATE INDEX idx_risk_entries_active_account ON risk_entries(account_key,state);
-- Only currently held obligations are audited per mutation, never full history.
CREATE INDEX idx_risk_entries_active_balance ON risk_entries(state)
 WHERE state <> 'RELEASED';
PRAGMA user_version=3;

-- OKX account-ledger schema v1.
-- One DB per natively verified (venue, environment, UID/account_id).
-- This schema is an INACTIVE migration target, NOT the current production store.
-- Connection owner MUST set/verify WAL, synchronous=FULL, foreign_keys=ON.
-- All monetary/price/quantity values are exact decimal TEXT validated by Rust;
-- NEVER store finance as SQLite REAL. No JSON columns or JSON-backed state.
CREATE TABLE IF NOT EXISTS account_identity (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    venue TEXT NOT NULL,
    environment TEXT NOT NULL,
    account_id TEXT NOT NULL,
    uid_fingerprint TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms > 0),
    schema_version INTEGER NOT NULL CHECK (schema_version IN (1,2)),
    UNIQUE(venue, environment, account_id, uid_fingerprint)
) STRICT;
-- The first authenticated native binding is permanent. A runtime must
-- never silently relabel a populated DB as a different account.
CREATE TRIGGER IF NOT EXISTS immutable_account_identity_update
BEFORE UPDATE ON account_identity BEGIN SELECT RAISE(ABORT, 'IMMUTABLE_ACCOUNT_IDENTITY'); END;
CREATE TRIGGER IF NOT EXISTS immutable_account_identity_delete
BEFORE DELETE ON account_identity BEGIN SELECT RAISE(ABORT, 'IMMUTABLE_ACCOUNT_IDENTITY'); END;

CREATE TABLE IF NOT EXISTS evidence_objects (
    evidence_sha256 TEXT PRIMARY KEY,
    evidence_kind TEXT NOT NULL,
    codec TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    native_source_time_ms INTEGER,
    UNIQUE(relative_path)
) STRICT;

-- Source facts are independently scoped; order-history gaps do not
-- automatically become financial cash-flow gaps.
CREATE TABLE IF NOT EXISTS source_receipts (
    receipt_sha256 TEXT PRIMARY KEY,
    stream_kind TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    interval_start_ms INTEGER NOT NULL,
    interval_end_ms INTEGER NOT NULL,
    received_at_ms INTEGER NOT NULL,
    completeness TEXT NOT NULL CHECK(completeness IN ('COMPLETE','PARTIAL','UNVERIFIED')),
    evidence_sha256 TEXT REFERENCES evidence_objects(evidence_sha256),
    CHECK(interval_start_ms >= 0 AND interval_end_ms >= interval_start_ms),
    CHECK(received_at_ms >= interval_end_ms)
) STRICT;
CREATE INDEX IF NOT EXISTS idx_receipts_stream ON source_receipts
    (stream_kind, scope_key, received_at_ms DESC);

CREATE TABLE IF NOT EXISTS source_gaps (
    gap_id TEXT PRIMARY KEY,
    stream_kind TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    from_ms INTEGER NOT NULL,
    through_ms INTEGER NOT NULL CHECK(through_ms > from_ms),
    materiality TEXT NOT NULL CHECK(materiality IN
        ('UNKNOWN','EXECUTION_ONLY','FINANCIAL','EXPOSURE')),
    reason_code TEXT NOT NULL,
    opening_evidence_sha256 TEXT REFERENCES evidence_objects(evidence_sha256),
    resolution_evidence_sha256 TEXT REFERENCES evidence_objects(evidence_sha256)
) STRICT;
CREATE INDEX IF NOT EXISTS idx_source_gaps ON source_gaps
    (stream_kind, scope_key, from_ms);

-- Append-only authoritative transitions. Projection rows below are
-- transactionally derived, NEVER independent write authorities.
CREATE TABLE IF NOT EXISTS domain_events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL CHECK(event_version > 0),
    subject_kind TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    effective_at_ms INTEGER NOT NULL CHECK(effective_at_ms > 0),
    recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms >= effective_at_ms),
    evidence_sha256 TEXT REFERENCES evidence_objects(evidence_sha256),
    previous_event_sha256 TEXT,
    event_sha256 TEXT NOT NULL UNIQUE
) STRICT;
CREATE INDEX IF NOT EXISTS idx_events_subject ON domain_events
    (subject_kind, subject_id, seq);
CREATE TRIGGER IF NOT EXISTS require_previous_event_hash BEFORE INSERT ON domain_events
WHEN (SELECT event_sha256 FROM domain_events ORDER BY seq DESC LIMIT 1)
     IS NOT NEW.previous_event_sha256
BEGIN SELECT RAISE(ABORT, 'BROKEN_EVENT_CHAIN'); END;
CREATE TRIGGER IF NOT EXISTS prevent_event_update
BEFORE UPDATE ON domain_events BEGIN SELECT RAISE(ABORT, 'IMMUTABLE_EVENT'); END;
CREATE TRIGGER IF NOT EXISTS prevent_event_delete
BEFORE DELETE ON domain_events BEGIN SELECT RAISE(ABORT, 'IMMUTABLE_EVENT'); END;

-- The account accounting timeline is continuous and does not reset.
-- Strategy/capital epochs are subdivisions, not owners of exchange balances.
CREATE TABLE IF NOT EXISTS capital_epochs (
    epoch_id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    epoch_kind TEXT NOT NULL, -- manual / algorithmic / other versioned Rust kind
    origin_schema TEXT NOT NULL,
    origin_version TEXT NOT NULL,
    opening_at_ms INTEGER NOT NULL CHECK(opening_at_ms > 0),
    opening_receipt_sha256 TEXT NOT NULL REFERENCES source_receipts(receipt_sha256),
    allocation_currency TEXT NOT NULL,
    allocation_decimal TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('ACTIVE','FINANCIALLY_CLOSED','UNVERIFIED_CLOSED')),
    closing_at_ms INTEGER,
    closing_evidence_sha256 TEXT REFERENCES evidence_objects(evidence_sha256),
    CHECK((state='ACTIVE' AND closing_at_ms IS NULL) OR
          (state<>'ACTIVE' AND closing_at_ms>=opening_at_ms))
) STRICT;
CREATE INDEX IF NOT EXISTS idx_epochs_strategy ON capital_epochs
    (strategy_id, opening_at_ms);
-- Concurrent independently funded epochs for one strategy are allowed.

CREATE TABLE IF NOT EXISTS decisions (
    decision_id TEXT PRIMARY KEY,
    epoch_id TEXT REFERENCES capital_epochs(epoch_id),
    research_hypothesis_id TEXT,
    decision_kind TEXT NOT NULL,
    decision_at_ms INTEGER NOT NULL CHECK(decision_at_ms > 0),
    instrument_id TEXT,
    source_evidence_sha256 TEXT NOT NULL REFERENCES evidence_objects(evidence_sha256),
    decision_evidence_sha256 TEXT NOT NULL REFERENCES evidence_objects(evidence_sha256),
    mandate_version TEXT,
    quality TEXT NOT NULL,
    rationale_ref TEXT
) STRICT;
CREATE INDEX IF NOT EXISTS idx_decision_hypothesis ON decisions
    (research_hypothesis_id, decision_at_ms);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    epoch_id TEXT REFERENCES capital_epochs(epoch_id),
    decision_id TEXT REFERENCES decisions(decision_id),
    instrument_id TEXT NOT NULL,
    requested_action TEXT NOT NULL,
    plan_sha256 TEXT NOT NULL,
    current_state TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL CHECK(created_at_ms > 0)
) STRICT;
CREATE INDEX IF NOT EXISTS idx_intents_epoch ON intents(epoch_id, created_at_ms);

-- One BEGIN_POST record for each intent. If acknowledgment is lost this
-- remains UNKNOWN; no second POST is permitted based on timeout alone.
CREATE TABLE IF NOT EXISTS post_attempts (
    intent_id TEXT PRIMARY KEY REFERENCES intents(intent_id),
    client_order_id TEXT NOT NULL UNIQUE,
    begin_post_event_id TEXT NOT NULL UNIQUE REFERENCES domain_events(event_id),
    begin_post_at_ms INTEGER NOT NULL CHECK(begin_post_at_ms > 0),
    delivery_outcome TEXT NOT NULL
) STRICT;

-- Exact IDs for amendments, cancellations, protection and other venue writes.
CREATE TABLE IF NOT EXISTS exchange_mutations (
    mutation_id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL REFERENCES intents(intent_id),
    mutation_kind TEXT NOT NULL,
    begin_event_id TEXT NOT NULL UNIQUE REFERENCES domain_events(event_id),
    created_at_ms INTEGER NOT NULL,
    delivery_outcome TEXT NOT NULL,
    native_request_key TEXT NOT NULL UNIQUE
) STRICT;
CREATE INDEX IF NOT EXISTS idx_mutations_intent ON exchange_mutations(intent_id, created_at_ms);

CREATE TABLE IF NOT EXISTS exchange_orders (
    venue_order_id TEXT PRIMARY KEY,
    intent_id TEXT REFERENCES intents(intent_id),
    instrument_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    status TEXT NOT NULL,
    last_exchange_update_ms INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS idx_orders_intent ON exchange_orders(intent_id);

CREATE TABLE IF NOT EXISTS fills (
    instrument_id TEXT NOT NULL,
    trade_id TEXT NOT NULL,
    venue_order_id TEXT,
    intent_id TEXT REFERENCES intents(intent_id),
    exchange_time_ms INTEGER NOT NULL,
    side TEXT NOT NULL,
    position_side TEXT NOT NULL,
    price_decimal TEXT NOT NULL,
    contracts_decimal TEXT NOT NULL,
    fee_decimal TEXT,
    fee_currency TEXT,
    source_evidence_sha256 TEXT NOT NULL REFERENCES evidence_objects(evidence_sha256),
    PRIMARY KEY(instrument_id, trade_id)
) STRICT;
CREATE INDEX IF NOT EXISTS idx_fills_intent ON fills(intent_id, exchange_time_ms);

CREATE TABLE IF NOT EXISTS bills (
    bill_id TEXT PRIMARY KEY,
    instrument_id TEXT,
    trade_id TEXT,
    exchange_time_ms INTEGER NOT NULL,
    currency TEXT NOT NULL,
    bill_kind TEXT NOT NULL,
    delta_decimal TEXT NOT NULL,
    source_evidence_sha256 TEXT NOT NULL REFERENCES evidence_objects(evidence_sha256)
) STRICT;
CREATE INDEX IF NOT EXISTS idx_bills_time ON bills(exchange_time_ms);

-- Claims link exact exchange cash flows to a strategy epoch.
-- Unclaimed bills remain account-level and MUST NOT silently enter strategy PnL.
CREATE TABLE IF NOT EXISTS bill_attribution (
    bill_id TEXT NOT NULL REFERENCES bills(bill_id),
    epoch_id TEXT NOT NULL REFERENCES capital_epochs(epoch_id),
    allocated_decimal TEXT NOT NULL,
    attribution_evidence_sha256 TEXT NOT NULL REFERENCES evidence_objects(evidence_sha256),
    PRIMARY KEY(bill_id, epoch_id)
) STRICT;
-- The native same-transaction Rust decimal validator must prove claimed sums.

CREATE TABLE IF NOT EXISTS capital_flows (
    flow_id TEXT PRIMARY KEY,
    epoch_id TEXT NOT NULL REFERENCES capital_epochs(epoch_id),
    flow_kind TEXT NOT NULL,
    currency TEXT NOT NULL,
    amount_decimal TEXT NOT NULL,
    occurred_at_ms INTEGER NOT NULL,
    source_evidence_sha256 TEXT NOT NULL REFERENCES evidence_objects(evidence_sha256)
) STRICT;

CREATE TABLE IF NOT EXISTS account_positions (
    position_key TEXT NOT NULL,
    received_at_ms INTEGER NOT NULL,
    contracts_decimal TEXT NOT NULL,
    mark_decimal TEXT,
    currency TEXT NOT NULL,
    source_evidence_sha256 TEXT NOT NULL REFERENCES evidence_objects(evidence_sha256),
    PRIMARY KEY(position_key, received_at_ms)
) STRICT;

CREATE TABLE IF NOT EXISTS nav_points (
    epoch_id TEXT NOT NULL REFERENCES capital_epochs(epoch_id),
    observed_at_ms INTEGER NOT NULL,
    nav_decimal TEXT,
    high_water_decimal TEXT,
    gross_daily_loss_decimal TEXT,
    currency TEXT NOT NULL,
    finance_completeness TEXT NOT NULL CHECK(finance_completeness IN
        ('COMPLETE','PARTIAL','UNVERIFIED')),
    source_receipt_sha256 TEXT NOT NULL REFERENCES source_receipts(receipt_sha256),
    PRIMARY KEY(epoch_id, observed_at_ms)
) STRICT;

-- Daily risk cannot be reset by closing/reopening a strategy epoch.
CREATE TABLE IF NOT EXISTS account_daily_risk (
    utc_day TEXT PRIMARY KEY,
    gross_loss_decimal TEXT NOT NULL,
    stop_latched INTEGER NOT NULL CHECK(stop_latched IN (0,1)),
    last_event_id TEXT NOT NULL REFERENCES domain_events(event_id)
) STRICT;
CREATE TABLE IF NOT EXISTS strategy_daily_risk (
    strategy_id TEXT NOT NULL,
    utc_day TEXT NOT NULL,
    gross_loss_decimal TEXT NOT NULL,
    stop_latched INTEGER NOT NULL CHECK(stop_latched IN (0,1)),
    last_event_id TEXT NOT NULL REFERENCES domain_events(event_id),
    PRIMARY KEY(strategy_id, utc_day)
) STRICT;

-- Index only; immutable binary/columnar research datasets remain external.
CREATE TABLE IF NOT EXISTS research_artifact_index (
    artifact_sha256 TEXT PRIMARY KEY REFERENCES evidence_objects(evidence_sha256),
    dataset_id TEXT NOT NULL,
    artifact_kind TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    instrument_id TEXT,
    min_exchange_time_ms INTEGER,
    max_exchange_time_ms INTEGER,
    source_receipt_sha256 TEXT REFERENCES source_receipts(receipt_sha256),
    CHECK(min_exchange_time_ms IS NULL OR max_exchange_time_ms >= min_exchange_time_ms)
) STRICT;
CREATE INDEX IF NOT EXISTS idx_research_dataset ON research_artifact_index
    (dataset_id, instrument_id, min_exchange_time_ms);
-- External Cloudflare Durable Object remains the ONLY global CAS RiskBook.
-- These are locally durable receipts, never an independent reserve authority.
CREATE TABLE IF NOT EXISTS risk_receipts (
    receipt_id TEXT PRIMARY KEY,
    intent_id TEXT REFERENCES intents(intent_id),
    riskbook_sequence INTEGER NOT NULL,
    riskbook_digest TEXT NOT NULL,
    state TEXT NOT NULL,
    event_id TEXT NOT NULL REFERENCES domain_events(event_id)
) STRICT;
-- Sealed source facts and attributed bills cannot be rewritten.
CREATE TRIGGER IF NOT EXISTS immutable_fills_update BEFORE UPDATE ON fills BEGIN SELECT RAISE(ABORT, 'IMMUTABLE_FILL'); END;
CREATE TRIGGER IF NOT EXISTS immutable_fills_delete BEFORE DELETE ON fills BEGIN SELECT RAISE(ABORT, 'IMMUTABLE_FILL'); END;
CREATE TRIGGER IF NOT EXISTS immutable_bills_update BEFORE UPDATE ON bills BEGIN SELECT RAISE(ABORT, 'IMMUTABLE_BILL'); END;
CREATE TRIGGER IF NOT EXISTS immutable_bills_delete BEFORE DELETE ON bills BEGIN SELECT RAISE(ABORT, 'IMMUTABLE_BILL'); END;
CREATE TRIGGER IF NOT EXISTS immutable_source_update BEFORE UPDATE ON source_receipts BEGIN SELECT RAISE(ABORT, 'IMMUTABLE_SOURCE'); END;
CREATE TRIGGER IF NOT EXISTS immutable_source_delete BEFORE DELETE ON source_receipts BEGIN SELECT RAISE(ABORT, 'IMMUTABLE_SOURCE'); END;
CREATE TRIGGER IF NOT EXISTS immutable_bill_claim_update BEFORE UPDATE ON bill_attribution BEGIN SELECT RAISE(ABORT, 'IMMUTABLE_ATTRIBUTION'); END;
CREATE TRIGGER IF NOT EXISTS immutable_bill_claim_delete BEFORE DELETE ON bill_attribution BEGIN SELECT RAISE(ABORT, 'IMMUTABLE_ATTRIBUTION'); END;
PRAGMA user_version=1;

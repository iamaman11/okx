# #510 r4 — source-derived SQL scope inventory and first physical single-file slice

**Date:** 14 November 2026. **Canonical architecture:** [Issue #510 r4.0](https://github.com/iamaman11/okx/issues/510). **Code:** [Draft PR #511](https://github.com/iamaman11/okx/pull/511). No production authorization, migration, Cloudflare cutover, Windows installation or trading follows from this first slice.

## Original SQL table inventory (47 tables; verified against GitHub)

All 47 original tables below must be inspected for their **existing SQL PK/UNIQUE/FK, source joins, immutable triggers, account/campaign lifecycle and every caller**. Listing them is not a migration.

| Owner / existing source file | Tables | Count |
|---|---|---:|
| `crates/okx-execution/schema/account-v2.sql` | `account_meta`, `strategy_definition_versions`, `strategy_instances`, `decisions`, `intents`, `intent_plans`, `protection_receipts`, `orders`, `fills`, `fill_projection_pending`, `funding_projection_pending`, `bills`, `cash_flows`, `allocations`, `position_lots`, `finance_entries`, `finance_totals`, `instance_finance_totals`, `finance_source_links`, `nav_points`, `nav_watermarks`, `nav_scopes`, `daily_losses`, `risk_stops`, `source_cursors`, `risk_receipts`, `operation_receipts`, `account_policy`, `storage_recovery` | 29 |
| `crates/okx-execution/src/ledger/sql/schema.rs::ACCOUNT_DERIVED_SCHEMA` | `campaign_source_epoch`, `campaign_entry_readiness`, `campaign_late_head_epoch`, `campaign_receipt_events`, `campaign_receipt_cursors`, `campaign_entry_applied`, `campaign_entry_totals`, `campaign_entry_window_proofs`, `bill_order_sources`, `swap_fill_sources` | 10 |
| `crates/okx-execution/src/risk/book/schema.sql` | `risk_meta`, `risk_policy_account`, `risk_account_occupied`, `risk_loss_source`, `risk_policy_strategy`, `risk_strategy_occupied`, `risk_entries`, `risk_no_reserve` | 8 |

### The specific original assumptions that must change

| Original construct | Actual current constraint/problem | Required r4 property |
|---|---|---|
| `account_meta`, `account_policy`, `storage_recovery` | One `singleton=1` per *separate account file*; initial schema `user_version=2` | Account-scoped identity/policy/recovery rows in the **one** installation SQLite; all reads/writes constrained by `installation_id + account_key` |
| `campaign_source_epoch`, `campaign_late_head_epoch` | Original account-local singleton invalidation | Scope invalidation by the account whose original source changed; campaign reads still validate the correct cohort |
| `intents`, `orders`, `fills`, `bills`, `finance_entries`, `operation_receipts` | Original account-local `intent_id/order_id/fill_id/bill_id/entry_id/operation_id` PK/UNIQUE with no `account_key` column | Identical raw venue and original request IDs on A/B coexist, with scoped PK/UNIQUE, all related FK and queries; original bytes never silently rewritten |
| `strategy_instances` and definition/decision/plan/stop/position/NAV tables | Account-local FK and/or scope string | Composite account-scoped FKs, indexes and source queries; strategy IDs do not accidentally share budgets |
| `campaign_entry_applied` / totals / receipts / windows | Account-local campaign IDs plus intent IDs | Original-source account identity remains part of each receipt and finance FK; campaign totals aggregate the right cohort once |
| `risk_meta` | One campaign per separate `risk-book.sqlite` with singleton row and independently incremented sequence | `(installation_id,campaign_id)` keyed campaign policy, epoch, sequence and consumed/reserved/late obligations in the same installation DB |
| `risk_policy_account`, `risk_loss_source`, `risk_entries`, `risk_no_reserve` | Account/reservation keys unique **within the campaign file** | Campaign+account membership and canonical reservation PK/FK; a different campaign cannot spend or read another budget |
| Account SQL and campaign SQL owners | Existing original `AccountStoreHandle` and independent `RiskBookHandle` each execute `BEGIN IMMEDIATE` on different files | One actual writer, one `Transaction` for linked source revision, risk budget and immutable receipt; no second account ACK |
| Reopen/backup/migration | `AccountStore::open_existing` requires account schema v2, RiskBook requires fixture schema v5 | New one-file schema version, one manifest/import marker and scope-total verification. No implicit upgrade of existing files before legacy transfer and account history are proven |

**Extra code audit required before production:** all SQL statements that currently filter only by `intent_id`, `fill_id`, `bill_id`, `reservation_key`, `operation_id`, `scope_id`, `campaign_id`; all on-insert invalidation triggers; source provenance, cross-account JOINs, per-account currency and NAV aggregation; exact owner lease, backup/restore and startup. A row-level account filter only on the API is insufficient if the SQL UNIQUE/FK remains global. No detached DB `ATTACH` workaround.

## First executable code slice — WHAT IT DOES

Files under the **existing** `okx-execution::account_store::sql` tree:

- `ledger/sql/finance-v1.sql`: an explicit private *10-table scoped subset* for installation, accounts, campaigns, memberships, original account intent/fill/bill/source revision, original campaign reserve and immutable entry receipts. Account+campaign composite PK/FK, immutable source and receipts. **Not the complete unified account-v2 schema.**
- `ledger/sql/unified_financial.rs`: `settle_entry(&rusqlite::Transaction, EntryRef)` neither owns a Connection nor opens a DB, runs a second writer, calls venue, accepts caller-supplied dollar amounts or commits. It reads the original source revision and native scoped fill/bill links, validates exact persisted campaign balance and entry identity, applies checked `okx-kernel::risk::money::RiskAmount` updates and writes the immutable receipt **inside the caller's ONE SQL transaction**. The existing SQL worker must ultimately own the Connection/BEGIN/COMMIT; **production integration of the existing authenticated source producer remains NOT_PROVEN**.
- `ledger/sql/unified_financial_tests.rs`: real disk SQLite/WAL with account A and B sharing the same raw `intent_id`, `fill_id`, `bill_id`, `trade_id`, `exchange_order_id` but distinct source/account scopes; campaign X aggregates both, campaign Y stays unchanged. Checks source/plan mismatch, read-only replay, stale revision, corrupt cache, SQLite concurrent-writer refusal, transaction rollback and real child OS kill **both before and after the single COMMIT**. All test sources are isolated fixtures; they are **NOT** authenticated venue producers.

**Code tested:** SHA `941b24129697cd1399687c0736019f8a247721f7`, [GitHub CI #43529137651](https://github.com/iamaman11/okx/actions/runs/43529137651) **SUCCESS**: `risk_book_contract` **49/49 okx-execution + 6/6 okx-kernel** and native original AccountStore source-proof **1/1** = **56 filtered cases**, including subprocess entrypoints checked by physical parent tests; architecture guard/negative self-tests and `cargo fmt` PASS. Execution after compilation on Linux runner: **0.44s + 0.00s + 0.11s**. Negative integrity test `risk_book_contract_single_finance_sqlite_rejects_forged_primary_receipt_projection` demonstrates that tampering a mutable reservation while its original immutable SQL receipt remains is rejected; released rows with a nonzero reserved amount fail SQL CHECK. This is not full RB01–16, Windows 26/26, Windows P95, complete schema migration or production acceptance.

## Exact next engineering slice — not optional

1. Normalize **all 29+10 original account SQL tables** in one verified versioned schema; preserve original `AccountStore` immutable rows/financial calculations and foreign keys, no shadow duplicate ledger. The 10-table subset is NOT an alternative production ledger. Prove same-venue IDs and negative cross-account joins on the full normalized schema.
2. Move the original account source producer, `RiskBook` reserve/settle/resolve and legacy account ACK into **one existing account SQL writer / one connection**; transaction spans source and campaign facts. No second runtime `RiskBookStore::open_existing`; delete previous two-DB ACK only after same tests pass the new real production path.
3. Implement safe snapshot/import all historical account and campaign rows+identity and Cloudflare legacy physical server-side fence, owner epoch, UNKNOWN/late fees; fail closed for incomplete/corrupted transfer. Do not initialize a blank budget over unknown legacy state.
4. Finish all 9 action callers, RB01–RB16 + T09/T10 and physical Windows P95/CI; no mixed READY/NO-GO claims.

**Remaining original-source limitation:** the test core’s sealed source/fill/bill rows are seeded as isolated fixtures, not yet written by the full original authenticated `AccountStore::bind_partial_entry_settlement` producer running on a normalized shared ledger. The source-linked SQL transaction does not accept caller dollars, but that alone does **not** establish authentic exchange provenance. Actual full ledger scope/import/producer cutover remains mandatory and unimplemented. No PR READY claim from this subset.

**One file does not make OKX API calls atomic.** Venue POST is outside SQL COMMIT; unresolved response preserves `UNKNOWN`, original order identity and no blind retry. Multiple hosts cannot independently write their own replica of the same campaign authority.

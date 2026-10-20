# OKX Architecture Contract v1 — one extensible trading product

**Status: ACCEPTED TARGET ARCHITECTURE (2026-10-20). Implementation: PARTIAL.**
This document is the canonical architectural decision for extending the existing
application. It is **not** proof that multi-venue/multi-account concurrency or
Production trading is currently accepted. Keep the existing release/risk gates.
`ARCHITECTURE.md` and Stage 5 documents preserve historical evidence but may
not override these boundaries. The current implementation and P0 trade
acceptance are tracked separately.

## 1. Non-negotiable product contract

- **One application, one Rust Engine, one Windows CONTROL lifecycle owner, one
  primary Cloudflare MCP transport.** No Demo-specific executor, second
  controller/agent, parallel risk oracle, shadow state database, scheduler or
  trading fallback through GitHub.
- Every operation uses a canonical `AccountRef { venue, account_id }`.
  `environment` (Real/Demo), authenticated UID, credentials, position mode,
  permissions, capabilities and limits are *verified account properties*, never
  user-supplied proof or separate API names.
- `account.select` changes **operator view only**; execution writes require an
  explicit, immutable AccountRef plus strategy/run/intent identity. Never infer
  the trading destination from a mutable global UI selector.
- Same typed query, prepare, submit, amend, cancel, close, protect, reconcile
  and reporting contracts for all accounts. Capability checks can reject a
  request; they must not create separate Demo and Real code paths.
- Strategy supplies a versioned decision, **never an exchange POST**. One
  source-bound native execution owner validates account identity, instrument,
  venue capabilities, reconciled state, centralized policy/RiskBook, clock,
  market quality, protection and STOP immediately before sending.
- On a lost/uncertain POST result: persist UNKNOWN, reconcile exact native
  order/fills/bills and protections, **never blindly replay the mutation**.
  A failed/lost `prepare_execution` response is separately classified as
  `EXECUTION_PREPARE_OUTCOME_UNKNOWN_READ_EXACT_INTENT`: it can leave a
  durable local PREPARED record but cannot have submitted an exchange order;
  check the exact intent before any next attempt. One ambiguous local
  preparation is not a reason to investigate phantom exchange fills.
- Store prices/quantities/money as checked decimal quantities in canonical
  native domains; timestamps are exchange-reconciled and provenance-bound.
  ChatGPT is not the risk or PnL calculator.
- No regressions for Stage 1/2/3 research, Stage 4 lifecycle and execution, or
  Stage 5 NAV/STOP/TCA. Old evidence remains read-only and immutable.

## 2. Layer dependencies and ownership

```text
  ChatGPT / CLI / operator interface
             | typed small use-case requests + compact result projection
  Application workflows / account routing / authorization
             | immutable AccountRef + StrategyRunRef + IntentId
  ---------------------------------------------------------------
  Market & Observation | Research & Strategy (pure decisions)
  Portfolio & Risk     | Execution & Reconciliation (single writer)
  Ledger & Finance     | Reporting & TCA (source exact)
  ---------------------------------------------------------------
       VenuePort / AccountPort / ClockPort / StoragePort
             | explicit capabilities, credentials and errors
       OKX adapter | future venue adapters (not currently installed)
  ---------------------------------------------------------------
  One Rust runtime + durable journals and native event/WS sources
  Existing Windows CONTROL (install, start/stop, recovery ONLY)
  Existing Cloudflare MCP (OAuth, bounded request transport,
       centralized configured governance and RiskBook CAS ONLY)
  GitHub (CI, release provenance, CONTROL channel, diagnostics;
       NEVER a trading fallback)
```

Rule: higher layers may request lower-domain operations only through typed
application/port contracts. Venue adapters never own strategy NAV, portfolio
limits or independent trading state. ChatGPT and MCP do not reconstruct fills,
risk or PnL. CONTROL cannot submit exchange trades.

Current crate alignment, avoiding a new service or wholesale rewrite:

| Responsibility | Existing home |
| --- | --- |
| Typed wire contracts and identities | `okx-protocol` |
| Exchange REST, clock, venue mutations | `okx-api` (adapter boundary to extract when a second venue is implemented) |
| WS and account observations | `okx-ws`, `okx-observation` |
| Deterministic portfolio/strategy calculations | `okx-analysis` |
| Immutable datasets/replay/walk-forward | `okx-research` |
| Order state, one mutation owner, immutable financial ledger | `okx-execution` |
| Account-keyed application workflows and diagnostics | `okx-agent` |
| Deployment/identity/lifecycle, NOT trading | `okx-host-control` and `okx-host-launcher` |
| Authenticated bounded ChatGPT data path | existing Cloudflare MCP |
| Operator entry points | `okx-cli` / MCP typed tools |

## 3. Account isolation and scalable execution

Target `AccountContext` is constructed by **native verified registration**:
`AccountRef`, `VenueAdapter`, authenticated `UID`, environment, selected
credential identity, venue modes/capabilities, rate budget, WS/source cursor,
account financial ledger namespace, per-account execution journal and risk
scope. Credentials remain in the existing secrets owner; the catalog stores
only non-secret identity metadata and verification status.

One **process** may own many independent AccountContexts, but each
`(venue, account_id, UID)` has exactly one mutation writer and crash-recoverable
ledger. The total risk authority aggregates account/strategy reservations;
account-level and global ceilings are configured centrally. A strategy has its
own run ID, allocation, NAV, drawdown and daily-loss stop. Shared/NET venue
positions require exact attribution or explicit INCOMPLETE/NO_NEW_RISK:
independent strategy balances must not be invented from net exchange exposure.

The UI may select a visible account, but write requests are explicitly
account-scoped. Cross-account credential/UID/ledger mismatch fails closed.
Event-driven WS/venue updates are multiplexed with bounded concurrency and
fair per-venue rate budgets in the existing runtime; no new polling services.

**Current limitation:** only OKX Real/Demo have implemented native routes, via
exclusive profile handoff. Metadata-only accounts are not yet selectable.
The enum mentioning Bybit is not a Bybit integration. PR #394 only introduces
the typed account/venue ingress and routing skeleton. The next integration
slice must replace global profile ownership with account-keyed native contexts
*without creating one process or daemon per account*.

## 4. Extension rules (one simple path each)

**New strategy:** implement a versioned pure decision in `okx-analysis` or
the existing research strategy interface; attach immutable data and OOS
evidence in `okx-research`; allocate strategy budget in central governance;
then submit a normal typed `ExecutionIntent`. Never edit an exchange POST
gateway or add a strategy-specific risk/ledger implementation.

**New account (any environment):** register `AccountRef`, verify credentials,
UID and venue capabilities with the existing native adapter; provision a
durable account-keyed ledger/risk namespace, read full source and STOP;
activate independent account context. Existing trading and research APIs
must not change. Registration by itself cannot grant trading.

**New exchange:** implement `VenuePort` (instrument/contracts/margin modes,
clock and rate limits; authenticated account, orders/positions/fills/bills,
protection/OCO, REST/WS continuity; placed/cancelled/UNKNOWN semantics),
publish capabilities and contract normalization, and pass the shared adapter
conformance tests. No new Strategy/Risk/Finance/ChatGPT tool or agent.
Unsupported operations return a typed capability failure, never silent
fallback or synthesized atomicity.

**New analytic/financial function:** add one domain calculation and expose
a bounded versioned projection/query field. Reuse immutable account/market
snapshots and source quality, not new root tools for each user question.
For every reported money value, include account/run scope and source proof.

## 5. Compact transport and context budget

Transport is only the primary authenticated MCP -> existing Worker ->
one outbound Windows connection -> typed Rust request/result. CONTROL handles
host lifecycle; GitHub diagnostic reads do not submit orders.

Three output depths **on the same underlying source**:

| View | Purpose | Output policy |
| --- | --- | --- |
| `compact` (default) | Trader/analyst answer | Decision, key numbers, account/strategy, quality/as-of, blocker, next valid action; goal <= 3.5 KB |
| `detail` (explicit) | Investigation, position attribution, analysis | Bounded projections/tables, evidence digests and scoped cursor; keep existing research target ~12 KB |
| `proof` (explicit) | CI/deployment/financial acceptance | Source/version/UID/digest and bounded specific proofs; never dump full ledger into chat |

Observed 2026-10-20 via existing MCP `account_summary(view=trader)`:
`packet_bytes=2890`, `full_account_result_bytes=15303`,
`account_snapshot_read_ms=783`, `ledger_history_read_ms=1115`,
`native_source_read_ms=1503`. These are *one native observation*, not an
end-to-end p95 or a throughput benchmark. Preserve the accepted direct frame
and timeout budgets (64 KiB, 20 seconds) unless independent load measurements
prove a change necessary. For large history/replay, use native bounded jobs and
immutable continuation checkpoints; return a small summary, not raw events.

Every result should carry: schema/version, account/run scope, source/as-of,
quality/FRESH or precise DEGRADED reason, limitations, immutable proof/cursor,
and a compact decision. Avoid repeated nested envelopes, repeated account
inventory in each row, raw stdout and redundant tool calls. Measure actual
payload bytes, end-to-end latency percentiles, error/UNKNOWN rate and
tool calls per successful workflow before claiming cost/performance gains.

## 6. Product roles and acceptance

One UX for five views:
- **Trader:** account selector, capital/NAV/positions, protected order
  preview, risk budget before submit, execution/reconciliation state, close.
- **Analyst:** market universe/rank/filter, liquid instruments and
  microstructure, comparison and attributable TCA.
- **Researcher:** data lineage, replay, walk-forward, sealed FINAL_OOS,
  PAPER/SHADOW and controlled strategy promotion.
- **Risk officer:** account and strategy budgets, portfolio exposure,
  day loss/drawdown/STOP, UNKNOWN, reconciliation and decision audit.
- **Operator/developer:** one status/release parity/diagnosis/repair surface,
  scoped source errors, provenance; deploy one matched ReleaseSet.

An addition is complete only when:
1. Its versioned contract and capability are available through existing tools.
2. Its own account/strategy/venue isolation, crash recovery and normal
   risk/finance attribution are proved with shared tests.
3. CI passes exactly on the committed tree; native deployed parity is checked
   when runtime binaries change.
4. Operator compact result explains success/failure and *the next safe action*.
5. No extra runtime owner/daemon/scheduler/profile-specific API appears.

### Next integration order — prevent P0 from depending on P2

**P0:** accept and install matched Windows artifacts, reconcile historical
financial admission, prove one Real OPEN/protection/CLOSE with full bills and
NAV; no external IP binding or account duplication. Native safety gates remain.

**P1:** enable repeated protected Real cycles with complete strategy NAV,
fees, funding, daily STOP, intent->fill->bill->TCA audit and a compact
operator experience.

**P2:** finish native account-keyed contexts and independent simultaneous
strategies/accounts; add later venue adapters via `VenuePort` conformance.
P0 is not gated on implementing P2.

### Evidence/status separation

The implementation can be `CODE_PASSED`, `MERGED`,
`INSTALLED_MATCHED`, `RUNTIME_VERIFIED`, and
`TRADE_CYCLE_ACCEPTED` **independently**. Never equate a green CI result,
a policy configuration, or a read-only account observation with live trading
admission. Current read-only audit can report `FLAT_RECONCILED` and current
NAV while the global historical financial audit is still `INCOMPLETE`.

## 7. Architecture governance, not an extra project

This document is the only normative modularity and expansion architecture.
The existing issue #160 and Stage 5 documents remain implementation/checkpoint
history, not competing domain designs. Updates should change this contract
only for a genuine cross-layer/ownership decision; ordinary features stay in
the owned existing module. No new ceremony, second roadmap or duplicate
architecture policy is needed.


## 8. Accepted account storage and accounting epochs (2026-10-20)

**Decision:** replace operational JSON snapshot persistence with **embedded SQLite
in WAL mode**. This section is a *target and migration invariant*, not a claim
that the installed Windows agent currently writes SQLite. The existing v14
Ledger remains the only production execution authority until verified cutover.

### Storage boundaries, designed not to change for new strategies or venues

- One SQLite database **per natively verified account** keyed by
  `(venue, environment, account_id, authenticated UID)`; a single bounded
  native writer per account, independent writers for distinct accounts.
  A UI selector never changes a write's immutable account destination.
  Multiple strategies/runs share that account's execution facts, not separate
  exchange ledgers. Global Cloudflare Durable Object governance/RiskBook stays
  one CAS authority; SQLite risk receipts are evidence only, NOT competing
  reservations.
- Append-only typed **domain events** and source-exact fills/bills are the
  durable account truth. Indexed projections (intents, order state, positions,
  NAV/TCA) are transactionally derived and rebuildable; there is no dual-write
  JSON/SQLite mode. Native contracts, not SQL column names, are external API.
- `BEGIN IMMEDIATE` only for bounded local transitions. Persist the one
  BEGIN_POST transaction before the exchange network call; after a lost ACK,
  UNKNOWN remains charged, native exact-ID reconciliation follows, **never
  a second POST**. Cloudflare CAS and exchange POST are **not** SQLite
  transactions and must retain the existing durable fencing protocol.
- Connection admission MUST verify `journal_mode=WAL`,
  `synchronous=FULL`, `foreign_keys=ON`, schema version and account UID.
  Reject a mismatched/corrupt DB; do not silently create another ledger,
  downgrade durability or replay mutations. Money, prices and quantities are
  checked Rust decimal types stored as exact canonical decimal TEXT, never
  SQLite FLOAT/REAL. Signed proofs use canonical byte hashes.
- Trade/decision metadata and account cash flows are relational and indexed;
  **large** history trades/order-book datasets belong in immutable,
  content-addressed **binary/columnar research artifacts** with SQLite
  catalog references; do not stuff market ticks into the order commit path.
  JSON remains at required OKX/GitHub/MCP wire boundaries and as immutable
  historical original source bytes when byte-for-byte evidence matters.
  Remove JSON from live internal persistence, not from external protocols.
- Schema migrations are additive/versioned where possible. No dangerous
  JSON-to-SQL dual writer: freeze old writer, verify v14 original/archives and
  account identity, import on a staging DB, compare **all** states/receipts/
  positions/fills/bills/NAV/UNKNOWN/STOP/cashflows, integrity_check, backup,
  atomically install the new owner, retain old immutable bytes, prohibit
  automatic fallback to old write paths. Crash tests at every cutover stage.

### Permanent financial timeline vs strategy capital epochs

1. **Account financial timeline never resets.** Native fills, bills, fees,
   funding, transfers, liabilities, independent position observations and
   exposure continuity belong to the account even without an active strategy.
2. **Capital epoch** = an attributed run of a strategy or ChatGPT/manual
   research mandate with start time, account identity, source-confirmed
   opening capital, flows, decision lineage, and closure. Closing/reopening an
   epoch must not reset account- or strategy-level daily loss, drawdown STOP,
   unresolved orders or risk exposure. Epoch IDs are not exchange POST keys.
3. **Coverage** is per native source and materiality, not one global clock:
   `ORDER_LIFECYCLE`, `FILLS`, `BILLS`, `CASHFLOWS`,
   `POSITIONS`, `VALUATION`, `PROTECTION` and `ACCOUNT_IDENTITY`.
   An order-history gap is **execution coverage unknown**; it is not by
   definition a financial cash-flow gap. Conversely current FLAT is not
   historical PnL proof. Every relevant source records covered interval,
   freshness, completeness, identity and digest.
4. A financial epoch is `FINANCIALLY_CLOSED` only with source-attributable
   flows/fills/fees/funding and NAV, or `UNVERIFIED_CLOSED` with original
   evidence and unknown PnL explicitly preserved. Unverified closure is
   **not** a loss of global risk memory, a reset, or a trading permit.
   A fresh epoch may start only with verified opening balance, exposure,
   positions, protection, STOP, source continuity and complete native
   cross-account risk census. Unknown actual losses remain conservatively
   charged to the account/strategy risk budget.
5. **Materiality, not a universal two-hour ritual.** The current two-hour
   order-history retention constraint stays attached to that source.
   Financial completeness is independently demonstrated by exhaustive
   authoritative fill/bill/transfer/position evidence. Where gaps cannot
   be proved immaterial, leave those dimensions UNVERIFIED. Avoid
   exception switches, fabricated zero PnL, blank database resets and
   rerouting a blocked quarantine through another API.
6. A ChatGPT-initiated trade is a normal `decision → intent → order →
   fill → bill → PnL/TCA` record. The decision must include hypothesis,
   source/market snapshot hash, rationales, assumptions, versions and
   attribution — not only the order ID. Pure strategies and manual
   research decisions use the **same** executor and ledger.

The proposed version-1 relational schema is
`crates/okx-execution/schema/account-ledger-v1.sql`. It is intentionally
**not activated** by current runtime. SQLite schema conformance executes in
the existing architecture CI job using Python's built-in sqlite3 solely as a
test, not as an application runtime/database owner.

### Acceptance before SQLite becomes execution authority

- Exact original v14 source, 100% inventory/byte hash and archived histories
  copied without semantic reinterpretation; verified account identity and
  source-bound intent collision checks. No old proof rewritten.
- Real/Demo isolated, account-keyed writer; no global mutation serialization;
  migration restarted after crash without creating a parallel active writer.
- Deterministic replay of event/projection consistency, 1/10/50/100 account
  scoped concurrent requests (not 100 simultaneous POSTs on one account),
  measured p95/p99, no duplicate POST on ACK loss/crash, STOP and daily
  loss conserved across epoch boundaries, backup/restore checked with WAL.
- Current Real P0 financial NO_GO is unchanged until native facts establish
  permission. A green schema test does **not** mean SQLite is installed or
  authorize Real trading.
